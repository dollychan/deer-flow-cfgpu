"""P0 — materials registry 地基（cfgpu-docs/materials.md §2.1, materials-impl-plan.md P0）。

覆盖 merge_materials reducer 的字段级 attach / ref_type 升级 / asset_url immutable /
None 与空 dict 处理，以及 new_material_id 顺序生成。
"""

import pytest

from deerflow.agents.materials.types import Material, new_material_id
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


# --- new_material_id 顺序生成 ------------------------------------------------


def test_new_id_empty_is_m1():
    assert new_material_id(None) == "m1"
    assert new_material_id({}) == "m1"


def test_new_id_max_plus_one():
    materials = {"m1": _mat("m1"), "m3": _mat("m3")}
    assert new_material_id(materials) == "m4"  # 取 max+1（非 count+1），跳号也正确


def test_new_id_ignores_non_mn_keys():
    materials = {"m2": _mat("m2"), "weird": _mat("weird")}
    assert new_material_id(materials) == "m3"
