"""Material registry types (cfgpu-docs/materials.md §2.2).

P0 只交付类型 + 顺序 id 生成。`merge_materials` reducer 与既有 reducer 同处
`thread_state.py`（保持 ThreadState reducer 集中的约定）。后续阶段（P1+）在本包内
加 registry/middleware/policy/materialize。
"""

from typing import Literal, NotRequired, TypedDict

# 唯一行为判别轴（§0/§3）；origin 仅溯源元数据，不决定行为。
RefType = Literal["global_url", "oss_path", "asset_url"]
Origin = Literal["uplink", "generate", "tool", "local"]
Kind = Literal["image", "video", "audio", "document", "asset"]


class Material(TypedDict):
    """素材注册表条目（§2.2）。必填 = id/kind/origin/ref_type/ref；其余惰性 attach。"""

    id: str  # 稳定短 id：台账/摘要/参数三处的锚点
    kind: Kind
    origin: Origin  # 溯源元数据(不决定行为)；uplink 含 path-B 文本粘贴 url
    ref_type: RefType  # 唯一行为判别轴；可迁移 global_url→oss_path(§4.5 lifecycle)
    ref: str  # 远程权威标识(url/object_key/asset ref)
    local_path: NotRequired[str | None]  # 本地副本(虚拟路径)，可空；与 ref 并存
    scope: NotRequired[str | None]  # 仅 asset_url：限定可用 model，如 "doubao-seedance-*"
    stable: NotRequired[bool]  # rehost 失败置 false（I5：emit 前校验、台账标注）
    display: NotRequired[bool]  # 交付物标记：present_* 置 true → 进 artifacts 投影(§4.6)
    caption: NotRequired[str | None]  # generate 用 prompt；上行用文件名/alt
    turn: NotRequired[int | None]  # 产生轮次，帮"第二张图"指代
    origin_url: NotRequired[str | None]  # 仅审计/回溯，不参与流转


def new_material_id(materials: dict[str, Material] | None) -> str:
    """顺序 id 生成：扫现有 ``mN`` keys 取 max+1，空表→``m1``。确定性、可测。"""
    if not materials:
        return "m1"
    max_n = 0
    for mid in materials:
        if mid.startswith("m") and mid[1:].isdigit():
            max_n = max(max_n, int(mid[1:]))
    return f"m{max_n + 1}"
