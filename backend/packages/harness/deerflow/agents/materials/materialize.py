"""物化 helper：统一 fetch / rehost / stage 三原语（cfgpu-docs/materials.md §4.5/§8, P6）。

所有把素材在「远程 url ↔ 本地副本 ↔ 我方 OSS object_key」之间搬动的操作必须收敛到这里，
每个原语先走 ``stable_ref→id`` 反查 + 双向地址索引查重（§8 R1–R4）再决定是否真正 IO——
散落的 fetch/upload 会绕过去重导致重复物化（双计费）。复用 ``oss/uploader.py``
（``rehost_url`` / ``upload_local_file``）+ ``oss/client.py``，本模块只加「查重收口 + 注册表
登记」一层。

三原语（按 §8 命名）：
- ``stage``（R1）：远程 url → 本地副本字节；目标文件已在 → 跳过 fetch。
- ``rehost_remote_url`` / ``rehost_local_file``（R2 + Capture 远程路径）：→ 我方 OSS object_key
  （oss_path）；按地址 / local_path 命中既有 → 跳过 upload。
- ``fetch_bytes``：底层网络取字节（被 ``stage`` 复用；带 timeout/size 上限）。

登记产出统一为 ``MaterializeOutcome``：``update`` 是 reducer 形态 ``{id: Material}``，命中既有
（``deduped=True``）时为 ``{}``。调用方 merge 进 ``state["materials"]``。
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import httpx

from deerflow.agents.materials.registry import build_reverse_index, classify_ref, register, stable_ref
from deerflow.agents.materials.types import Kind, Material, Origin, RefType
from deerflow.oss.uploader import get_oss_uploader

logger = logging.getLogger(__name__)

# fetch 上限：与 uploader.rehost_url 同口径（cfdream 临期媒体有界，卡死的 CDN 不得吊住一次 run）。
_FETCH_TIMEOUT_S = 60.0
_FETCH_MAX_BYTES = 256 * 1024 * 1024  # 256 MiB（视频安全上限）


@dataclass(frozen=True)
class FetchedBytes:
    data: bytes
    content_type: str | None


@dataclass(frozen=True)
class MaterializeOutcome:
    """一次物化结果。``update`` 为 reducer 形态 ``{id: Material}``；``deduped`` 时为 ``{}``。"""

    id: str
    update: dict[str, Material]
    ref_type: RefType
    ref: str
    stable: bool
    deduped: bool


# ── 查重原语（§8 R2/R3/R4）──────────────────────────────────────────────────────


def find_by_address(materials: dict[str, Material] | None, ref_type: RefType, ref: str) -> str | None:
    """按权威 ref + origin_url 双向索引反查既有 material id（R3/R4）。无则 None。"""
    return build_reverse_index(materials).get(stable_ref(ref_type, ref))


def find_by_local_path(materials: dict[str, Material] | None, local_path: str | None) -> str | None:
    """按 ``local_path`` 反查既有 material id（R2）。

    ``local_path`` 不进 ``build_reverse_index`` 主索引（那是地址级 stable_ref），故单列。
    """
    if not local_path or not materials:
        return None
    for mid, mat in materials.items():
        if mat.get("local_path") == local_path:
            return mid
    return None


def _deduped(materials: dict[str, Material], mid: str) -> MaterializeOutcome:
    m = materials[mid]
    return MaterializeOutcome(
        id=mid,
        update={},
        ref_type=m.get("ref_type", "global_url"),
        ref=m.get("ref", ""),
        stable=m.get("stable", True),
        deduped=True,
    )


# ── fetch（底层网络）─────────────────────────────────────────────────────────────


async def fetch_bytes(url: str) -> FetchedBytes:
    """取远程 url 的字节（async httpx，带 timeout + size 上限）。失败 raise。"""
    async with httpx.AsyncClient(follow_redirects=True, timeout=_FETCH_TIMEOUT_S) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.content
        content_type = (resp.headers.get("content-type") or "").split(";")[0].strip() or None
    if len(data) > _FETCH_MAX_BYTES:
        raise ValueError(f"fetch payload {len(data)} bytes exceeds {_FETCH_MAX_BYTES} ceiling")
    return FetchedBytes(data=data, content_type=content_type)


async def stage(url: str, physical_path: str) -> str:
    """R1：远程 url → 本地副本。目标文件已在 → **跳过 fetch**，直接返回路径（幂等）。

    纯文件级物化原语（不登记注册表——登记由调用方按需 attach ``local_path``）。返回写入的
    物理路径。
    """
    dest = Path(physical_path)
    if dest.exists() and dest.stat().st_size > 0:
        logger.debug("materialize.stage: %s already present — skip fetch", physical_path)
        return physical_path
    fetched = await fetch_bytes(url)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(fetched.data)
    logger.info("materialize.stage: staged %s → %s (%d bytes)", url, physical_path, len(fetched.data))
    return physical_path


# ── rehost（→ 我方 OSS object_key）───────────────────────────────────────────────


async def rehost_remote_url(
    materials: dict[str, Material],
    url: str,
    *,
    thread_id: str,
    kind: Kind,
    origin: Origin = "generate",
    caption: str | None = None,
    turn: int | None = None,
    display: bool | None = None,
) -> MaterializeOutcome:
    """Capture 远程路径：cfdream 临期 url → 我方 OSS object_key（oss_path）。

    查重 R3/R4：先按 ``classify_ref(url)`` 地址反查；命中既有 → 跳过 IO。我方对象（D4，url 已
    是 ``agent-artifacts/`` 我方域）→ 登记 oss_path 跳过 fetch。第三方临期 url → fetch+upload。
    **失败 raise**（调用方 ``_capture`` fail-open 兜底：置 ``stable=false`` 登记 global_url 续跑、
    不阻断 run，§I5）。
    """
    ref_type0, ref0 = classify_ref(url)
    hit = find_by_address(materials, ref_type0, ref0)
    if hit is not None:
        return _deduped(materials, hit)  # 幂等：task_wait 重放 / 同批重复 url 不二次 rehost

    if ref_type0 == "oss_path":
        # D4：已是我方对象 → 登记 oss_path，跳过 fetch
        mid, upd = register(materials, kind=kind, origin=origin, ref_type="oss_path", ref=ref0, caption=caption, turn=turn, display=display, stable=True)
        return MaterializeOutcome(id=mid, update=upd, ref_type="oss_path", ref=ref0, stable=True, deduped=False)

    uploader = get_oss_uploader()
    if uploader is None:
        raise RuntimeError("OSS uploader unavailable — cannot re-host")
    object_key = await uploader.rehost_url(url, thread_id)
    mid, upd = register(materials, kind=kind, origin=origin, ref_type="oss_path", ref=object_key, origin_url=url, caption=caption, turn=turn, display=display, stable=True)
    return MaterializeOutcome(id=mid, update=upd, ref_type="oss_path", ref=object_key, stable=True, deduped=False)


def register_remote_url(
    materials: dict[str, Material],
    url: str,
    *,
    kind: Kind,
    origin: Origin = "generate",
    caption: str | None = None,
    turn: int | None = None,
    display: bool | None = None,
) -> MaterializeOutcome:
    """register policy（§D12）：准入但**不落盘**——查重 + 登记原 ref（global_url 保持临期）。

    无网络。后续可经 §4.5 lifecycle 升级 oss_path（再走 ``rehost_*``）。
    """
    ref_type, ref = classify_ref(url)
    hit = find_by_address(materials, ref_type, ref)
    if hit is not None:
        return _deduped(materials, hit)
    mid, upd = register(materials, kind=kind, origin=origin, ref_type=ref_type, ref=ref, caption=caption, turn=turn, display=display, stable=True)
    return MaterializeOutcome(id=mid, update=upd, ref_type=ref_type, ref=ref, stable=True, deduped=False)


async def rehost_local_file(
    materials: dict[str, Material],
    virtual_path: str,
    physical_path: str,
    *,
    thread_id: str,
    kind: Kind,
    origin: Origin = "local",
    caption: str | None = None,
    turn: int | None = None,
    display: bool = True,
) -> MaterializeOutcome:
    """R2：本地文件 → 我方 OSS object_key（oss_path）。

    查重 R2：先按 ``local_path`` 反查；命中且已是 oss_path → 跳过 upload。命中但仍 global_url
    （曾 stage 到本地）→ upload 后 attach 升级 oss_path（R3 同素材，经 merge_materials 放行
    global_url→oss_path）。未命中 → 新建。
    """
    hit = find_by_local_path(materials, virtual_path)
    if hit is not None and materials[hit].get("ref_type") == "oss_path":
        return _deduped(materials, hit)

    uploader = get_oss_uploader()
    if uploader is None:
        raise RuntimeError("OSS uploader unavailable — cannot re-host local file")
    ref = await uploader.upload_local_file(virtual_path, physical_path, thread_id)
    # ref 可能是 presigned（presigned_url=true）或裸 object_key（false）→ 一律归一 oss_path
    _ref_type, object_key = classify_ref(ref)

    if hit is not None:
        # R3：升级既有 material 的 ref_type（global_url+local_path → oss_path），attach object_key
        upd: dict[str, Material] = {
            hit: {"id": hit, "kind": kind, "origin": origin, "ref_type": "oss_path", "ref": object_key, "local_path": virtual_path}
        }
        if display:
            upd[hit]["display"] = True
        return MaterializeOutcome(id=hit, update=upd, ref_type="oss_path", ref=object_key, stable=True, deduped=False)

    mid, new_upd = register(materials, kind=kind, origin=origin, ref_type="oss_path", ref=object_key, local_path=virtual_path, caption=caption, turn=turn, display=display, stable=True)
    return MaterializeOutcome(id=mid, update=new_upd, ref_type="oss_path", ref=object_key, stable=True, deduped=False)


# ── 本地素材生命周期：三原语 register / localize / stage（§4.8.2, D15/D16, P10）────────
#
# 注册表寻址写/读收敛为三个按 ref_type 分流的幂等原语：``register``（唯一创建者）、
# ``localize``⊥``stage_to_oss``（一对反向"确保到达"：确保本地副本 vs 确保远程持久）。
# 三者都先读 state、只补缺口，故幂等可重入（§8 去重契约）。注：此处的 ``stage_to_oss``
# 是 §4.8 语义的 "stage"（id→oss_path）；上方 ``stage(url, physical)`` 是其复用的文件级
# 下载积木（名字撞但层次不同，§4.8.3 命名注记）。


def register_local_file(
    materials: dict[str, Material],
    virtual_path: str,
    *,
    kind: Kind,
    origin: Origin = "local",
    caption: str | None = None,
    turn: int | None = None,
    display: bool | None = None,
) -> MaterializeOutcome:
    """register 原语·local 分支（§4.8.2）：本地文件**廉价登记**成 ``local`` 素材（**零网络**）。

    只有 local_path、无远程 ref（I13 退化端）；``stable=false``（无持久远程副本，§4.8.1）→ emit/display
    投影按 stable 过滤，未 stage 的本地素材不会被当交付物下行。``find_by_local_path`` 去重命中既有 →
    返旧 id 跳过。喂 cfgpu / 展示前须经 ``stage_to_oss`` 升级 oss_path（§4.8.4）。
    """
    hit = find_by_local_path(materials, virtual_path)
    if hit is not None:
        return _deduped(materials, hit)
    mid, upd = register(
        materials, kind=kind, origin=origin, ref_type="local", local_path=virtual_path,
        caption=caption, turn=turn, display=display, stable=False,
    )
    return MaterializeOutcome(id=mid, update=upd, ref_type="local", ref="", stable=False, deduped=False)


async def localize(
    materials: dict[str, Material],
    mid: str,
    *,
    to_physical: Callable[[str], str],
    dest_virtual: str,
    presign: Callable[[str], str],
) -> tuple[str, dict[str, Material]]:
    """localize 原语（§4.8.2）【确保本地可达】：``id → 本地虚拟路径``。返回 ``(local_path, update)``。

    幂等：``local_path`` 有且物理文件在 → 直接返回（``update={}``）；否则按 ref_type 下载到
    ``dest_virtual``（其物理由 ``to_physical`` 映射）并 attach local_path：
    ``oss_path``→presign→下载 / ``global_url``→直接下载 / ``asset_url``→拒绝（I4，M3 不可下载）/
    ``local`` 但文件丢失 → 悬空报错。返回的是**本地路径（非 url）**，I9 安全。
    """
    mat = materials.get(mid)
    if mat is None:
        raise KeyError(f"material {mid!r} not in registry")
    local_path = mat.get("local_path")
    if local_path and Path(to_physical(local_path)).is_file():
        return local_path, {}  # 幂等：本地副本已在
    ref_type = mat.get("ref_type")
    if ref_type == "asset_url":
        raise ValueError(f"asset_url material {mid!r} cannot be localized (I4：不可下载)")
    if ref_type == "local":
        raise FileNotFoundError(f"local material {mid!r} has no reachable file (local_path 悬空)")
    ref = mat.get("ref", "")
    if not ref:
        raise ValueError(f"material {mid!r} has no remote ref to localize")
    url = presign(ref) if ref_type == "oss_path" else ref
    await stage(url, to_physical(dest_virtual))  # 幂等下载（目标已在则跳过 fetch）
    return dest_virtual, {mid: {"id": mid, "local_path": dest_virtual}}  # type: ignore[dict-item]


async def stage_to_oss(
    materials: dict[str, Material],
    mid: str,
    *,
    thread_id: str,
    to_physical: Callable[[str], str],
    display: bool = False,
) -> MaterializeOutcome:
    """stage 原语（§4.8.2）【确保远程持久】：``id → oss_path``。升级 ref_type、保留 local_path。

    按 ref_type 分流：``oss_path``→已持久，直接返回（deduped）/ ``local``→upload local_path→object_key
    （复用 ``rehost_local_file`` R3 升级 local→oss_path，id 不变，§4.8.1）/ ``global_url``→fetch+upload→
    object_key / ``asset_url``→拒绝（I4）。``display`` 仅在 present 出口置 True（交付物）；auto-stage
    喂 cfgpu 时保持 False（输入非交付）。
    """
    mat = materials.get(mid)
    if mat is None:
        raise KeyError(f"material {mid!r} not in registry")
    ref_type = mat.get("ref_type")
    if ref_type == "oss_path":
        return _deduped(materials, mid)
    if ref_type == "asset_url":
        raise ValueError(f"asset_url material {mid!r} cannot be staged (I4)")
    kind = mat.get("kind", "image")
    caption = mat.get("caption")
    if ref_type == "local":
        local_path = mat.get("local_path")
        if not local_path:
            raise ValueError(f"local material {mid!r} has no local_path to stage")
        return await rehost_local_file(
            materials, local_path, to_physical(local_path), thread_id=thread_id,
            kind=kind, origin=mat.get("origin", "local"), caption=caption, display=display,
        )
    # global_url：fetch + upload → object_key，**就地升级既有 id**（不走 rehost_remote_url——它按
    # 地址反查会把素材自身的 global_url ref 当命中而 deduped，永不真上传）。origin_url=原 url 使 id
    # 派生不变（§B），merge 放行 global_url→oss_path。
    url = mat.get("ref", "")
    if not url:
        raise ValueError(f"global_url material {mid!r} has no ref to stage")
    uploader = get_oss_uploader()
    if uploader is None:
        raise RuntimeError("OSS uploader unavailable — cannot stage")
    _rt, object_key = classify_ref(await uploader.rehost_url(url, thread_id))
    upd: dict[str, Material] = {
        mid: {"id": mid, "kind": kind, "origin": mat.get("origin", "generate"), "ref_type": "oss_path", "ref": object_key, "origin_url": url}
    }
    if mat.get("local_path"):
        upd[mid]["local_path"] = mat["local_path"]
    if display:
        upd[mid]["display"] = True
    return MaterializeOutcome(id=mid, update=upd, ref_type="oss_path", ref=object_key, stable=True, deduped=False)
