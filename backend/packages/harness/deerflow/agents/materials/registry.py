"""Materials registry operations (cfgpu-docs/materials.md §4.1/§4.7/§8, P1).

★公共原语★：纯函数操作层，无 IO、无网络。被上行登记（P1）、异步 Capture 去重（P3）、
物化查重（P6）、in-gate url 归一（P9）共用。返回 reducer 形态更新 ``{id: Material}``，
调用方 seed graph_input 或 emit ``Command(update={"materials": ...})``。

归一一致性：``classify_ref`` 上行与 in-gate **共用同一规则**，使 ``stable_ref`` 去重跨
两路一致——真·外部 cdn 链无 ``agent-artifacts/`` 前缀 → global_url；我方 presigned /
裸 object_key → oss_path（refine §1 M1：M1 只设想真外链，我方回链应归 oss_path 以利去重）。
"""

from __future__ import annotations

from urllib.parse import unquote, urlsplit

from deerflow.agents.materials.types import Kind, Material, Origin, RefType, new_material_id

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


def stable_ref(ref_type: RefType, ref: str) -> str:
    """归一键（§8 R4 反查索引主键）。带类型前缀避免 object_key 与 url path 撞键。

    - ``oss_path``：ref 即 object_key，原样。
    - ``global_url``：剥 query（presign 签名 / 临期参数不参与身份），取 scheme+host+path。
    - ``asset_url``：asset ref 原样。
    """
    if ref_type == "oss_path":
        return f"oss:{ref}"
    if ref_type == "asset_url":
        return f"asset:{ref}"
    parts = urlsplit(ref)
    return f"url:{parts.scheme.lower()}://{parts.netloc.lower()}{unquote(parts.path)}"


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


def build_reverse_index(materials: dict[str, Material] | None) -> dict[str, str]:
    """``stable_ref → id`` 反查索引（§8 R4）。挂权威 ``ref``。"""
    index: dict[str, str] = {}
    if not materials:
        return index
    for mid, mat in materials.items():
        ref_type = mat.get("ref_type")
        ref = mat.get("ref")
        if ref_type and ref:
            index[stable_ref(ref_type, ref)] = mid
    return index


def register(
    materials: dict[str, Material] | None,
    *,
    kind: Kind,
    origin: Origin,
    ref_type: RefType,
    ref: str,
    caption: str | None = None,
    turn: int | None = None,
    origin_url: str | None = None,
    local_path: str | None = None,
    scope: str | None = None,
    display: bool | None = None,
    stable: bool | None = None,
) -> tuple[str, dict[str, Material]]:
    """分配新 id 并产出 reducer 形态更新 ``{id: Material}``。不查重（调用方按需先 resolve）。"""
    mid = new_material_id(materials)
    mat: Material = {"id": mid, "kind": kind, "origin": origin, "ref_type": ref_type, "ref": ref}
    for key, value in (
        ("caption", caption),
        ("turn", turn),
        ("origin_url", origin_url),
        ("local_path", local_path),
        ("scope", scope),
        ("display", display),
        ("stable", stable),
    ):
        if value is not None:
            mat[key] = value  # type: ignore[literal-required]
    return mid, {mid: mat}


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
