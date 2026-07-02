"""Material registry types (cfgpu-docs/materials.md §2.2).

P0 只交付类型 + 顺序 id 生成。`merge_materials` reducer 与既有 reducer 同处
`thread_state.py`（保持 ThreadState reducer 集中的约定）。后续阶段（P1+）在本包内
加 registry/middleware/policy/materialize。
"""

from typing import Literal, NotRequired, TypedDict

# 唯一行为判别轴（§0/§3）；origin 仅溯源元数据，不决定行为。
# `local`（D15/§4.8）= 纯本地态：只有 local_path、无远程 ref（bash 产物廉价登记拿 id 不上传）。
RefType = Literal["global_url", "oss_path", "asset_url", "local"]
Origin = Literal["uplink", "generate", "tool", "local"]
Kind = Literal["image", "video", "audio", "document", "asset"]


class Material(TypedDict):
    """素材注册表条目（§2.2）。必填 = id/kind/origin/ref_type；其余惰性 attach。

    **I13（至少一个到达点，D15/§4.8）**：每条 material 至少有一个到达点——远程 ``ref``
    （global_url/oss_path/asset_url）或本地 ``local_path``，二选一、可并存。``ref_type=local``
    是仅 local_path 的退化端，此时 ``ref`` 省略；``stage`` 升级成 oss_path 后 ref 才落定。
    """

    id: str  # 稳定短 id：台账/摘要/参数三处的锚点
    kind: Kind
    origin: Origin  # 溯源元数据(不决定行为)；uplink 含 path-B 文本粘贴 url
    ref_type: RefType  # 唯一行为判别轴；可迁移 global_url→oss_path / local→oss_path(§4.5/§4.8 lifecycle)
    ref: NotRequired[str]  # 远程权威标识(url/object_key/asset ref)；ref_type=local 时省略(I13/D15)
    local_path: NotRequired[str | None]  # 本地副本(虚拟路径)，可空；与 ref 并存；ref_type=local 时为唯一到达点
    scope: NotRequired[str | None]  # 仅 asset_url：限定可用 model，如 "doubao-seedance-*"
    stable: NotRequired[bool]  # 常规恒 true；rehost 失败 fail-open 置 false（§4.6）→ emit 端按此过滤，不作交付物
    display: NotRequired[bool]  # 交付物标记：present_* 置 true → 进 artifacts 投影(§4.6)
    caption: NotRequired[str | None]  # generate 用 prompt；上行用文件名/alt
    turn: NotRequired[int | None]  # 产生轮次，帮"第二张图"指代
    origin_url: NotRequired[str | None]  # 仅审计/回溯，不参与流转
    size: NotRequired[int | None]  # 文件字节数；rehost/upload 物化时算一次，供下行 artifact item 展示大小（无副本时缺省 None）


# 注：id 生成已改为**内容派生确定性 id**（``registry.material_id``，§B）——顺序 ``new_material_id``
# 是 read-modify-write，并行登记会撞号致素材丢失，已退役。types 只留纯数据结构，无 id 分配逻辑。
