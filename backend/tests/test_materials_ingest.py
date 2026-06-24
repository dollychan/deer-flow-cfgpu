"""P1 — 上行素材登记（消费侧，cfgpu-docs/materials.md §4.1, materials-impl-plan.md P1）。

覆盖 _normalize_messages / _append_content_block：媒体登记 material、content 零 url、
保留整个 url[]、id 形态、human text url 不扫描、materials 经 graph_input seed。
"""

from app.consumer.agent_runner import _normalize_messages, _project_materials_into_artifacts
from app.consumer.schemas import ContentItem, UserMessage
from deerflow.agents.materials.registry import classify_ref, material_id


def _mid_of(raw: str) -> str:
    """复算上行源地址的内容派生 id（§B），免硬编码 mN。"""
    return material_id(*classify_ref(raw))


def _content(text: str) -> str:
    """从 LangChain content blocks（dict list 或 str）取全部文本，断言无 url。"""
    return text


def _blocks(graph_input):
    return graph_input["messages"][0].content


# --- 媒体登记 + 零 url -------------------------------------------------------


def test_image_url_registers_material_not_image_block():
    msg = UserMessage(role="user", content=[ContentItem(type="image_url", url=["https://cdn/x/hero.png"])])
    gi = _normalize_messages([msg])
    # 不再有 image_url 多模态 block；只有 id 形态 text
    blocks = _blocks(gi)
    m1 = _mid_of("https://cdn/x/hero.png")
    assert all(b["type"] == "text" for b in blocks)
    assert blocks[0]["text"] == f"[image: {m1}]"
    # material 登记入 graph_input，seed channel
    assert gi["materials"][m1]["kind"] == "image"
    assert gi["materials"][m1]["ref"] == "https://cdn/x/hero.png"
    assert gi["materials"][m1]["caption"] == "hero.png"


def test_content_has_no_url():
    msg = UserMessage(role="user", content=[ContentItem(type="image_url", url=["https://cdn/x/hero.png"])])
    gi = _normalize_messages([msg])
    serialized = repr(_blocks(gi))
    assert "http" not in serialized  # content 零 url（I9/I10）


def test_doc_audio_video_registered_not_degraded_text():
    items = [
        ContentItem(type="document_url", url=["https://cdn/a/spec.pdf"]),
        ContentItem(type="audio_url", url=["https://cdn/a/clip.mp3"]),
        ContentItem(type="video_url", url=["https://cdn/a/movie.mp4"]),
    ]
    gi = _normalize_messages([UserMessage(role="user", content=items)])
    kinds = {m["kind"] for m in gi["materials"].values()}
    assert kinds == {"document", "audio", "video"}
    # 不再是 "[document_url: http...]" 裸文本
    assert "http" not in repr(_blocks(gi))


# --- 保留整个 url[]（修 url[0] 局限）----------------------------------------


def test_all_urls_in_list_registered():
    msg = UserMessage(role="user", content=[ContentItem(type="image_url", url=["https://cdn/a.png", "https://cdn/b.png"])])
    gi = _normalize_messages([msg])
    a, b = _mid_of("https://cdn/a.png"), _mid_of("https://cdn/b.png")
    assert set(gi["materials"]) == {a, b}
    assert _blocks(gi)[0]["text"] == f"[image: {a} {b}]"


def test_duplicate_url_deduped_in_batch():
    msg = UserMessage(role="user", content=[ContentItem(type="image_url", url=["https://cdn/a.png", "https://cdn/a.png"])])
    gi = _normalize_messages([msg])
    a = _mid_of("https://cdn/a.png")
    assert set(gi["materials"]) == {a}  # 同 url → 一条
    assert _blocks(gi)[0]["text"] == f"[image: {a} {a}]"


# --- human text url 不扫描（D11）-------------------------------------------


def test_text_url_not_scanned():
    msg = UserMessage(role="user", content=[ContentItem(type="text", text="请分析 https://cdn/x.png")])
    gi = _normalize_messages([msg])
    assert "materials" not in gi  # text 内 url 不登记
    assert _blocks(gi)[0]["text"] == "请分析 https://cdn/x.png"  # 原样保留


# --- 无媒体 / 纯文本不 seed materials --------------------------------------


def test_plain_string_message_no_materials():
    gi = _normalize_messages([UserMessage(role="user", content="hello")])
    assert "materials" not in gi
    assert gi["messages"][0].content == "hello"


# --- 我方 object_key 上行 → oss_path ----------------------------------------


def test_uplink_object_key_is_oss_path():
    msg = UserMessage(role="user", content=[ContentItem(type="image_url", url=["agent-artifacts/t1/images/hero.png"])])
    gi = _normalize_messages([msg])
    m1 = _mid_of("agent-artifacts/t1/images/hero.png")
    assert gi["materials"][m1]["ref_type"] == "oss_path"


# --- result 接缝投影（P8/D8 §4.6）---------------------------------------------


def _disp(mid, ref, ref_type="oss_path"):
    return {"id": mid, "kind": "image", "origin": "generate", "ref_type": ref_type, "ref": ref, "display": True}


def test_seam_unions_present_artifacts_with_display_projection():
    """present_file 写的 artifacts channel ∪ materials.display 投影（去重），materials 键剔除。"""
    materials = {
        "m1": _disp("m1", "agent-artifacts/t/gen.png"),  # 生成媒体（Capture，只在 materials）
        "m2": {"id": "m2", "kind": "image", "origin": "generate", "ref_type": "oss_path", "ref": "agent-artifacts/t/mid.png"},  # 中间产物，非交付
    }
    fsd = {"artifacts": ["agent-artifacts/t/local.png"], "materials": materials, "messages": []}
    _project_materials_into_artifacts(fsd, {"artifacts": ["agent-artifacts/t/local.png"], "materials": materials})
    assert fsd["artifacts"] == ["agent-artifacts/t/local.png", "agent-artifacts/t/gen.png"]  # union，中间产物 m2 不入
    assert "materials" not in fsd  # registry 不下行（I8）


def test_seam_dedup_when_present_and_display_overlap():
    materials = {"m1": _disp("m1", "agent-artifacts/t/x.png")}
    fsd = {"artifacts": ["agent-artifacts/t/x.png"], "materials": materials}
    _project_materials_into_artifacts(fsd, {"artifacts": ["agent-artifacts/t/x.png"], "materials": materials})
    assert fsd["artifacts"] == ["agent-artifacts/t/x.png"]  # 同 ref 不重复


def test_seam_strips_materials_even_without_display():
    """无 display 投影也必须剔除 materials 键（防 blanket-dump 泄漏 registry）。"""
    fsd = {"materials": {"m1": {"id": "m1", "kind": "image", "origin": "generate", "ref_type": "oss_path", "ref": "k"}}, "title": "x"}
    _project_materials_into_artifacts(fsd, {"materials": fsd["materials"]})
    assert "materials" not in fsd
    assert "artifacts" not in fsd  # 无 display 投影 + 原无 artifacts → 不凭空造


def test_seam_noop_when_no_materials():
    fsd = {"artifacts": ["a"], "messages": []}
    _project_materials_into_artifacts(fsd, {"artifacts": ["a"]})
    assert fsd["artifacts"] == ["a"]
    assert "materials" not in fsd
