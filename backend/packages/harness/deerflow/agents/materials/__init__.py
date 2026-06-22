"""Materials registry subsystem (cfgpu-docs/materials.md).

唯一全量 registry 进 checkpoint，对内只用 id 流转，ref_type 决定出口形态。
P0：类型与 id 生成。后续阶段在本包内加 registry / middleware / policy / materialize。
"""

from deerflow.agents.materials.registry import (
    build_reverse_index,
    classify_ref,
    is_our_object_key,
    register,
    resolve_or_register,
    stable_ref,
)
from deerflow.agents.materials.types import Kind, Material, Origin, RefType, new_material_id

__all__ = [
    "Kind",
    "Material",
    "Origin",
    "RefType",
    "new_material_id",
    "build_reverse_index",
    "classify_ref",
    "is_our_object_key",
    "register",
    "resolve_or_register",
    "stable_ref",
]
