"""P0 — materials registry 地基（cfgpu-docs/materials.md §2.1, materials-impl-plan.md P0）。

覆盖 merge_materials reducer 的字段级 attach / ref_type 升级 / asset_url immutable /
None 与空 dict 处理，以及 material_id 内容派生确定性生成（§B）。
"""

import pytest

from deerflow.agents.materials.registry import material_id
from deerflow.agents.materials.types import Material
from deerflow.agents.thread_state import merge_materials


def _mat(mid: str, **over) -> Material:
    base: dict = {
        "id": mid,
        "kind": "image",
        "origin": "uplink",
        "ref_type": "global_url",
        "ref": f"https://cdn/{mid}.png",
    }
    base.update(over)
    return base  # type: ignore[return-value]


# --- 插入 / None 处理 ------------------------------------------------------


def test_insert_new_id():
    out = merge_materials({}, {"m1": _mat("m1")})
    assert out["m1"]["ref"] == "https://cdn/m1.png"


def test_existing_none_takes_new():
    out = merge_materials(None, {"m1": _mat("m1")})
    assert set(out) == {"m1"}


def test_new_none_preserves_existing():
    existing = {"m1": _mat("m1")}
    assert merge_materials(existing, None) == existing


def test_empty_new_is_noop_not_clear():
    """registry 永不清空：空 dict = no-op（区别于 viewed_images 的"空=清空"）。"""
    existing = {"m1": _mat("m1")}
    assert merge_materials(existing, {}) == existing


# --- 字段级 attach ----------------------------------------------------------


def test_attach_local_path_keeps_ref():
    existing = {"m1": _mat("m1")}
    out = merge_materials(existing, {"m1": {"id": "m1", "local_path": "/mnt/user-data/outputs/m1.png"}})  # type: ignore[dict-item]
    assert out["m1"]["ref"] == "https://cdn/m1.png"  # ref 不丢
    assert out["m1"]["local_path"] == "/mnt/user-data/outputs/m1.png"


def test_attach_does_not_clear_with_none():
    existing = {"m1": _mat("m1", caption="hero")}
    out = merge_materials(existing, {"m1": {"id": "m1", "caption": None, "display": True}})  # type: ignore[dict-item]
    assert out["m1"]["caption"] == "hero"  # None 不覆盖
    assert out["m1"]["display"] is True


def test_attach_false_values_apply():
    existing = {"m1": _mat("m1")}
    out = merge_materials(existing, {"m1": {"id": "m1", "stable": False, "display": False}})  # type: ignore[dict-item]
    assert out["m1"]["stable"] is False
    assert out["m1"]["display"] is False


def test_idempotent_same_write():
    existing = {"m1": _mat("m1")}
    out = merge_materials(existing, {"m1": _mat("m1")})
    assert out["m1"] == existing["m1"]


# --- ref_type 生命周期 ------------------------------------------------------


def test_upgrade_global_url_to_oss_path():
    existing = {"m1": _mat("m1", ref_type="global_url")}
    out = merge_materials(existing, {"m1": {"id": "m1", "ref_type": "oss_path", "ref": "agent-artifacts/t/m1.png"}})  # type: ignore[dict-item]
    assert out["m1"]["ref_type"] == "oss_path"
    assert out["m1"]["ref"] == "agent-artifacts/t/m1.png"  # ref 随升级改写


def test_illegal_downgrade_oss_to_global_raises():
    existing = {"m1": _mat("m1", ref_type="oss_path", ref="agent-artifacts/t/m1.png")}
    with pytest.raises(ValueError, match="illegal ref_type transition"):
        merge_materials(existing, {"m1": {"id": "m1", "ref_type": "global_url", "ref": "https://x/m1.png"}})  # type: ignore[dict-item]


# --- asset_url immutable -----------------------------------------------------


def test_asset_url_ref_type_immutable():
    existing = {"m5": _mat("m5", kind="asset", ref_type="asset_url", ref="seedance:abc", scope="doubao-seedance-*")}
    with pytest.raises(ValueError, match="immutable"):
        merge_materials(existing, {"m5": {"id": "m5", "ref_type": "oss_path", "ref": "agent-artifacts/t/x"}})  # type: ignore[dict-item]


def test_asset_url_ref_immutable():
    existing = {"m5": _mat("m5", kind="asset", ref_type="asset_url", ref="seedance:abc")}
    with pytest.raises(ValueError, match="immutable"):
        merge_materials(existing, {"m5": {"id": "m5", "ref": "seedance:DIFFERENT"}})  # type: ignore[dict-item]


def test_asset_url_other_fields_still_attach():
    """immutable 只锁 ref_type/ref；display/caption 等仍可 attach。"""
    existing = {"m5": _mat("m5", kind="asset", ref_type="asset_url", ref="seedance:abc")}
    out = merge_materials(existing, {"m5": {"id": "m5", "display": True, "caption": "专属素材"}})  # type: ignore[dict-item]
    assert out["m5"]["display"] is True
    assert out["m5"]["caption"] == "专属素材"
    assert out["m5"]["ref"] == "seedance:abc"


# --- material_id 内容派生确定性生成（§B：并行不撞号）-------------------------


def test_material_id_format_and_determinism():
    mid = material_id("global_url", "https://cdn/x.png")
    assert mid.startswith("m_") and len(mid) == 10  # m_ + 8 hex
    assert all(c in "0123456789abcdef" for c in mid[2:])
    # 纯函数：同源恒同 id（并行/重放天然幂等）
    assert material_id("global_url", "https://cdn/x.png") == mid


def test_material_id_distinct_sources_distinct_ids():
    assert material_id("global_url", "https://cdn/a.png") != material_id("global_url", "https://cdn/b.png")


def test_material_id_query_agnostic():
    """身份去 query（presign 签名不参与）→ 同对象不同签名映射同 id（去重一致）。"""
    a = material_id("global_url", "https://cdn/x.png?Expires=1&Signature=aa")
    b = material_id("global_url", "https://cdn/x.png?Expires=2&Signature=bb")
    assert a == b


def test_material_id_prefers_origin_url_for_lifecycle_stability():
    """rehost 后 ref 变 object_key，但 id 由 origin_url 派生 → 升级前后 id 不变（§4.5 lifecycle）。"""
    before = material_id("global_url", "https://cdn.cfgpu.com/temp.png")
    after = material_id("oss_path", "agent-artifacts/t1/images/temp.png", origin_url="https://cdn.cfgpu.com/temp.png")
    assert before == after
