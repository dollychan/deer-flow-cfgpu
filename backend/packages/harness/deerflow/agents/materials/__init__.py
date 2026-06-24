"""Materials registry subsystem (cfgpu-docs/materials.md).

唯一全量 registry 进 checkpoint，对内只用 id 流转，ref_type 决定出口形态。
id 为**内容派生确定性 id**（``material_id``，§B）：纯函数、零协调，并行登记不撞号。
"""

from deerflow.agents.materials.registry import (
    build_reverse_index,
    classify_ref,
    is_our_object_key,
    material_id,
    register,
    resolve_or_register,
    stable_ref,
)
from deerflow.agents.materials.types import Kind, Material, Origin, RefType

__all__ = [
    "Kind",
    "Material",
    "Origin",
    "RefType",
    "material_id",
    "build_reverse_index",
    "classify_ref",
    "is_our_object_key",
    "register",
    "resolve_or_register",
    "stable_ref",
]
