"""P1 — materials registry 公共原语（cfgpu-docs/materials.md §4.1/§4.7/§8）。

覆盖 stable_ref / classify_ref / build_reverse_index / register / resolve_or_register。
重点：上行与 in-gate 共用 classify_ref → stable_ref 去重一致；我方对象前缀判定；第三方不触网。
"""

from deerflow.agents.materials.registry import (
    build_reverse_index,
    classify_ref,
    material_id,
    project_display_refs,
    register,
    resolve_or_register,
    stable_ref,
)

_OUR_PRESIGNED = "https://bkt.oss-cn-x.aliyuncs.com/agent-artifacts/t1/images/hero.png?Expires=999&Signature=abc"
_OUR_KEY = "agent-artifacts/t1/images/hero.png"
_THIRD = "https://cdn.example.com/pics/cat.png"


def _mid_of(raw: str) -> str:
    """测试侧复算某源地址的内容派生 id（§B），免硬编码 mN。"""
    return material_id(*classify_ref(raw))


# --- classify_ref -----------------------------------------------------------


def test_classify_bare_object_key_is_oss_path():
    assert classify_ref(_OUR_KEY) == ("oss_path", _OUR_KEY)


def test_classify_leading_slash_stripped():
    assert classify_ref("/agent-artifacts/t1/x.png") == ("oss_path", "agent-artifacts/t1/x.png")


def test_classify_our_presigned_url_strips_to_object_key():
    """我方 presigned → 剥签名 → oss_path(object_key)，利于跨路去重。"""
    assert classify_ref(_OUR_PRESIGNED) == ("oss_path", _OUR_KEY)


def test_classify_third_party_url_is_global_url():
    assert classify_ref(_THIRD) == ("global_url", _THIRD)


# --- stable_ref：去 query / 类型前缀 ----------------------------------------


def test_stable_ref_oss_path_prefixed():
    assert stable_ref("oss_path", _OUR_KEY) == f"oss:{_OUR_KEY}"


def test_stable_ref_global_url_strips_query():
    a = stable_ref("global_url", _THIRD + "?v=1")
    b = stable_ref("global_url", _THIRD + "?v=2")
    assert a == b  # query 不参与身份


def test_stable_ref_cross_path_consistency():
    """我方 presigned 与 bare key 经 classify → 同一 stable_ref（去重一致）。"""
    assert stable_ref(*classify_ref(_OUR_PRESIGNED)) == stable_ref(*classify_ref(_OUR_KEY))


def test_stable_ref_oss_vs_url_no_collision():
    assert stable_ref("oss_path", "a/b") != stable_ref("global_url", "https://h/a/b")


# --- register ---------------------------------------------------------------


def test_register_allocates_content_derived_id():
    mid, update = register({}, kind="image", origin="uplink", ref_type="global_url", ref=_THIRD)
    assert mid == _mid_of(_THIRD)  # 内容派生（§B），非顺序 mN
    assert update[mid]["ref"] == _THIRD
    assert update[mid]["kind"] == "image"


def test_register_optional_fields_only_when_set():
    mid, update = register({}, kind="image", origin="uplink", ref_type="global_url", ref=_THIRD, caption="cat")
    mat = update[mid]
    assert mat["caption"] == "cat"
    assert "turn" not in mat  # None 不写入


# --- build_reverse_index ----------------------------------------------------


def test_reverse_index_maps_stable_ref_to_id():
    materials = {"m1": {"id": "m1", "kind": "image", "origin": "uplink", "ref_type": "oss_path", "ref": _OUR_KEY}}
    idx = build_reverse_index(materials)
    assert idx[f"oss:{_OUR_KEY}"] == "m1"


# --- resolve_or_register ----------------------------------------------------


def test_resolve_existing_id_passthrough():
    materials = {"m1": {"id": "m1", "kind": "image", "origin": "uplink", "ref_type": "oss_path", "ref": _OUR_KEY}}
    mid, update = resolve_or_register(materials, "m1", kind="image")
    assert mid == "m1"
    assert update == {}  # 无新建


def test_resolve_dedup_presigned_against_existing_oss_path():
    """既有 oss_path 素材；in-gate 收到同对象的 presigned → 命中既有 id、不新建。"""
    materials = {"m1": {"id": "m1", "kind": "image", "origin": "uplink", "ref_type": "oss_path", "ref": _OUR_KEY}}
    mid, update = resolve_or_register(materials, _OUR_PRESIGNED, kind="image")
    assert mid == "m1"
    assert update == {}


def test_resolve_third_party_registers_global_url_no_network():
    materials: dict = {}
    mid, update = resolve_or_register(materials, _THIRD, kind="image")
    assert mid == _mid_of(_THIRD)
    assert update[mid]["ref_type"] == "global_url"
    assert update[mid]["ref"] == _THIRD  # 原样，不下载不 rehost


def test_resolve_miss_then_hit_same_batch():
    materials: dict = {}
    mid1, up1 = resolve_or_register(materials, _THIRD, kind="image")
    materials.update(up1)
    mid2, up2 = resolve_or_register(materials, _THIRD, kind="image")
    assert mid1 == mid2 == _mid_of(_THIRD)
    assert up2 == {}  # 第二次去重命中


# --- project_display_refs (P8/D8 §4.6 投影) ---------------------------------


def _mat(mid, ref, *, ref_type="oss_path", display=None):
    m = {"id": mid, "kind": "image", "origin": "generate", "ref_type": ref_type, "ref": ref}
    if display is not None:
        m["display"] = display
    return m


def test_project_display_refs_only_display_true_in_id_order():
    materials = {
        "m2": _mat("m2", "agent-artifacts/t/b.png", display=True),
        "m1": _mat("m1", "agent-artifacts/t/a.png", display=True),
        "m3": _mat("m3", "agent-artifacts/t/c.png"),  # 非交付物（无 display）→ 不投影
        "m4": _mat("m4", "agent-artifacts/t/d.png", display=False),  # 显式 false → 不投影
    }
    assert project_display_refs(materials) == ["agent-artifacts/t/a.png", "agent-artifacts/t/b.png"]


def test_project_display_refs_dedup_and_global_url():
    materials = {
        "m1": _mat("m1", "https://cdn/x.png", ref_type="global_url", display=True),
        "m2": _mat("m2", "https://cdn/x.png", ref_type="global_url", display=True),  # 同 ref → 去重
    }
    assert project_display_refs(materials) == ["https://cdn/x.png"]


def test_project_display_refs_empty():
    assert project_display_refs(None) == []
    assert project_display_refs({}) == []
    assert project_display_refs({"m1": _mat("m1", "k")}) == []  # 全无 display
