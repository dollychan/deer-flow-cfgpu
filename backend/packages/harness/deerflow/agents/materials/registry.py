"""Materials registry operations (cfgpu-docs/materials.md §4.1/§4.7/§8, P1).

★公共原语★：纯函数操作层，无 IO、无网络。被上行登记（P1）、异步 Capture 去重（P3）、
物化查重（P6）、in-gate url 归一（P9）共用。返回 reducer 形态更新 ``{id: Material}``，
调用方 seed graph_input 或 emit ``Command(update={"materials": ...})``。

归一一致性：``classify_ref`` 上行与 in-gate **共用同一规则**，使 ``stable_ref`` 去重跨
两路一致——真·外部 cdn 链无 ``agent-artifacts/`` 前缀 → global_url；我方 presigned /
裸 object_key → oss_path（refine §1 M1：M1 只设想真外链，我方回链应归 oss_path 以利去重）。
"""

from __future__ import annotations

import hashlib
from urllib.parse import parse_qsl, unquote, urlencode, urlsplit

from deerflow.agents.materials.types import Kind, Material, Origin, RefType

# 我方 OSS 对象的 object_key 前缀（oss/uploader.py: ``agent-artifacts/{thread}/...``）。
# host 无关的可靠"我方对象"信号：presigned url 的 path 去前导斜杠后以此打头。
# 已知小限制：第三方 host 恰好用同前缀路径会被误判 oss_path（缺 OSS host 常量，暂以前缀为准）。
_OUR_OSS_PREFIX = "agent-artifacts/"


def is_our_object_key(key: str) -> bool:
    """裸 object_key 是否指向我方 OSS 对象（``agent-artifacts/`` 前缀）。

    ``classify_ref`` 对无 scheme 串一律判 oss_path（含第三方裸路径 / prose token），故
    出口签发（§4.3 MaterialResolve）须用本 helper 再判一道：只有我方对象才现签 presigned，
    非我方裸串不碰（避免把 prompt 里的 ``and/or`` 之类误当 object_key 去签）。
    """
    return key.lstrip("/").startswith(_OUR_OSS_PREFIX)


# 临期签名 / 凭证 query 参数（小写匹配）：这些**不参与素材身份**，归一时剥除，使同一对象
# 被重新签发（task_wait 重放 / 重新 presign）后仍得同一 stable_ref（幂等，不双计费）。
# 反过来，**非签名 query（如搜索引擎 CDN 的 ``?id=...`` 缩略图标识）必须保留**——否则像 bing/
# duckduckgo 图搜结果那样 path 恒为 ``/th``、身份全在 query 的 url 会被错误折叠成同一 id（衝突 +
# 漏注册）。故只剥已知签名前缀/名，其余原样留作身份。
_SIGNING_QUERY_PREFIXES = ("x-amz-", "x-oss-", "x-tos-", "x-obs-", "x-cos-", "x-goog-", "q-")
_SIGNING_QUERY_NAMES = frozenset(
    {
        "expires", "signature", "policy", "credential",
        "ossaccesskeyid", "awsaccesskeyid", "key-pair-id", "keyid",
        "security-token", "securitytoken",
    }
)


def _is_signing_param(name: str) -> bool:
    low = name.lower()
    return low in _SIGNING_QUERY_NAMES or low.startswith(_SIGNING_QUERY_PREFIXES)


def _identity_query(query: str) -> str:
    """剥去临期签名参数后，返回**确定性归一**的身份 query（按 key,value 排序）。无身份参数 → ""。"""
    kept = [(k, v) for k, v in parse_qsl(query, keep_blank_values=True) if not _is_signing_param(k)]
    if not kept:
        return ""
    kept.sort()
    return urlencode(kept)


def stable_ref(ref_type: RefType, ref: str) -> str:
    """归一键（§8 R4 反查索引主键）。带类型前缀避免 object_key 与 url path 撞键。

    - ``oss_path``：ref 即 object_key，原样。
    - ``global_url``：取 scheme+host+path + **身份 query**（剥临期签名参数后仍保留的 query，
      见 ``_identity_query``）——presign 签名不参与身份（幂等），但搜索引擎 CDN 的 ``?id=...``
      之类身份 query 必须保留，否则 path 相同的不同素材会撞键折叠。
    - ``asset_url``：asset ref 原样。
    """
    if ref_type == "oss_path":
        return f"oss:{ref}"
    if ref_type == "asset_url":
        return f"asset:{ref}"
    if ref_type == "local":
        # local 素材身份 = local_path（D15/§4.8）；调用方把 local_path 当 ref 传入求 id。
        return f"local:{ref}"
    parts = urlsplit(ref)
    base = f"url:{parts.scheme.lower()}://{parts.netloc.lower()}{unquote(parts.path)}"
    identity_query = _identity_query(parts.query)
    return f"{base}?{identity_query}" if identity_query else base


def classify_ref(raw: str) -> tuple[RefType, str]:
    """把一个上行 / in-gate 的 url-或-object_key 串判定 ``ref_type`` 并归一 ``ref``。

    - 无 scheme 的裸串 → 我方 object_key（M2 上行 oss path）→ ``oss_path``。
    - http(s) 且 path 以 ``agent-artifacts/`` 打头（我方对象）→ 剥 presigned → ``oss_path``(object_key)。
    - 其余 http(s) → 第三方 → ``global_url``（原 url 留作出口；身份由 ``stable_ref`` 去 query）。

    ``asset_url`` 不经此函数（非媒体、由专门入口带 scope 登记）。
    """
    parts = urlsplit(raw)
    if not parts.scheme:
        return "oss_path", raw.lstrip("/")
    path = unquote(parts.path).lstrip("/")
    if path.startswith(_OUR_OSS_PREFIX):
        return "oss_path", path
    return "global_url", raw


# material id 派生哈希长度（hex 字符数）。8 hex=32bit，单 thread 数百素材碰撞概率可忽略。
_ID_HASH_LEN = 8


def material_id(ref_type: RefType, ref: str, origin_url: str | None = None) -> str:
    """素材的**内容派生确定性 id**（cfgpu-docs/materials.md §B / D12）：``m_<sha1(identity)[:8]>``。

    身份 = 素材的**源地址**经 ``stable_ref`` 归一后的串：优先 ``origin_url``（rehost 前的原始外链）
    ——使 id 在 ``global_url→oss_path`` 升级、task_wait 重放、并行 capture 三种情形下**保持稳定**，
    无 ``origin_url`` 时退回 ``(ref_type, ref)``。

    为什么不再用顺序 ``mN``：顺序分配是「读 registry → max+1」的 read-modify-write，并行工具调用
    各读同一快照 → 撞号 → reducer attach 合并致素材丢失/计费错位（§B 决策）。内容派生 id 是**纯函数、
    零协调**：并行不同源 → 不同 id；并行同源 → 同 id（reducer 幂等合并），从根上消除竞态。
    """
    if origin_url:
        rt, r = classify_ref(origin_url)
        key = stable_ref(rt, r)
    else:
        key = stable_ref(ref_type, ref)
    return "m_" + hashlib.sha1(key.encode("utf-8")).hexdigest()[:_ID_HASH_LEN]


def build_reverse_index(materials: dict[str, Material] | None) -> dict[str, str]:
    """``stable_ref → id`` 反查索引（§8 R4）。

    挂权威 ``ref`` **以及** ``origin_url``（rehost 前的原始外链）——一个素材 rehost 后 ``ref``
    变为 object_key（oss_path），但同一外链再浮现（task_wait 重放 / 上行复用）时带的是原 url
    （global_url）；只挂 ``ref`` 会漏判致重复 rehost（双计费）。两个地址都指回同一 id 才幂等。
    """
    index: dict[str, str] = {}
    if not materials:
        return index
    for mid, mat in materials.items():
        ref_type = mat.get("ref_type")
        ref = mat.get("ref")
        if ref_type and ref:
            index[stable_ref(ref_type, ref)] = mid
        origin_url = mat.get("origin_url")
        if origin_url:
            o_type, o_ref = classify_ref(origin_url)
            index.setdefault(stable_ref(o_type, o_ref), mid)
    return index


def register(
    materials: dict[str, Material] | None,
    *,
    kind: Kind,
    origin: Origin,
    ref_type: RefType,
    ref: str = "",
    caption: str | None = None,
    turn: int | None = None,
    origin_url: str | None = None,
    local_path: str | None = None,
    scope: str | None = None,
    display: bool | None = None,
    stable: bool | None = None,
    size: int | None = None,
) -> tuple[str, dict[str, Material]]:
    """分配内容派生 id 并产出 reducer 形态更新 ``{id: Material}``。不查重（调用方按需先 resolve）。

    ``materials`` 参数保留作 registry 上下文（语义对齐），id 不再依赖它——``material_id`` 是纯函数，
    并行登记天然不撞号（§B）。

    ``ref_type=local``（D15/§4.8）：无远程 ``ref``，身份 = ``local_path``——id 由 local_path 派生，
    ``ref`` 字段省略（I13：local_path 是唯一到达点）。
    """
    identity_ref = local_path if (ref_type == "local" and local_path) else ref
    mid = material_id(ref_type, identity_ref or "", origin_url)
    mat: Material = {"id": mid, "kind": kind, "origin": origin, "ref_type": ref_type}
    if ref:
        mat["ref"] = ref  # local 态无远程 ref → 省略（I13）
    for key, value in (
        ("caption", caption),
        ("turn", turn),
        ("origin_url", origin_url),
        ("local_path", local_path),
        ("scope", scope),
        ("display", display),
        ("stable", stable),
        ("size", size),
    ):
        if value is not None:
            mat[key] = value  # type: ignore[literal-required]
    return mid, {mid: mat}


def project_display_refs(materials: dict[str, Material] | None) -> list[str]:
    """artifacts 投影（§4.6, P8/D8）：取 ``display=true`` 子集的**稳定 ref**，按 id 升序去重。

    交付物投影出口之一（非流式 result.final_state，由 consumer 注入）。返回 ``Material.ref``
    （oss_path→object_key / global_url→url），**绝不 presigned、绝不含 id**——wire `artifacts`
    一贯是稳定 ref 的 list。registry（dict）本体绝不下行（I8，consumer 显式 strip ``materials`` 键）。
    """
    if not materials:
        return []
    # 内容派生 id 非顺序 → 按 id 串排序求**确定性**（artifacts 顺序仅展示用，稳定即可）。
    ordered = sorted(materials.values(), key=lambda m: str(m.get("id", "")))
    refs: list[str] = []
    for mat in ordered:
        if not mat.get("display"):
            continue
        ref = mat.get("ref")
        if ref and ref not in refs:
            refs.append(ref)
    return refs


def project_artifact_items(materials: dict[str, Material] | None, ids: list[str]) -> list[dict]:
    """Live `artifact` 事件 items 投影（§4.6, D14）：给定一组 id，构造下行交付项。

    与 `project_display_refs`（非流式 final_state 投影，按 `display`）镜像，但供 **emit 端**
    （MessageStreamMiddleware）在 `visibility==artifact` 时调用——故**按 `stable` 过滤**（交付资格
    由 visibility 决定、在调用方门控；这里只挡未落盘的 unstable 项，I5）。保留 `ids` 顺序（=工具结果
    content 里 `materials:[...]` 的顺序＝产出序）。**绝不 presigned**：item.ref 是稳定 ref（oss_path
    object_key / global_url url），客户端按约定取。

    每个 item 带 `size`（字节数）：随物化写入 `Material.size`（rehost/upload 时算一次），此处原样
    带出；未落盘（第三方 global_url 仅 register、从未下载）的素材无 size → 该字段为 `None`。
    """
    if not materials:
        return []
    items: list[dict] = []
    seen: set[str] = set()
    for mid in ids:
        if mid in seen:
            continue
        mat = materials.get(mid)
        if mat is None or not mat.get("stable", True):
            continue
        ref = mat.get("ref")
        if not ref:
            continue
        seen.add(mid)
        items.append({"id": mid, "ref": ref, "kind": mat.get("kind"), "stable": True, "size": mat.get("size")})
    return items


def resolve_or_register(
    materials: dict[str, Material] | None,
    raw: str,
    *,
    kind: Kind,
    origin: Origin = "uplink",
    caption: str | None = None,
    turn: int | None = None,
) -> tuple[str, dict[str, Material]]:
    """in-gate 归一（§4.7）：raw=已知 id → 原样；raw=url/object_key → 反查去重命中既有 / 未命中新建。

    第三方 host 新建 ``global_url``（不下载不 rehost）；我方对象命中/新建 ``oss_path``。
    返回 ``(id, update)``；命中既有时 ``update={}``（无新建）。
    """
    materials = materials or {}
    if raw in materials:
        return raw, {}
    ref_type, ref = classify_ref(raw)
    hit = build_reverse_index(materials).get(stable_ref(ref_type, ref))
    if hit is not None:
        return hit, {}
    return register(materials, kind=kind, origin=origin, ref_type=ref_type, ref=ref, caption=caption, turn=turn)
