"""P1 — 上行素材登记（消费侧，cfgpu-docs/materials.md §4.1, materials-impl-plan.md P1）。

覆盖 _normalize_messages / _append_content_block：媒体登记 material、content 零 url、
保留整个 url[]、id 形态、human text url 不扫描、materials 经 graph_input seed。
"""

from app.consumer.agent_runner import _normalize_messages
from app.consumer.schemas import ContentItem, UserMessage


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
    assert all(b["type"] == "text" for b in blocks)
    assert blocks[0]["text"] == "[image: m1]"
    # material 登记入 graph_input，seed channel
    assert gi["materials"]["m1"]["kind"] == "image"
    assert gi["materials"]["m1"]["ref"] == "https://cdn/x/hero.png"
    assert gi["materials"]["m1"]["caption"] == "hero.png"


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
    assert set(gi["materials"]) == {"m1", "m2"}
    assert _blocks(gi)[0]["text"] == "[image: m1 m2]"


def test_duplicate_url_deduped_in_batch():
    msg = UserMessage(role="user", content=[ContentItem(type="image_url", url=["https://cdn/a.png", "https://cdn/a.png"])])
    gi = _normalize_messages([msg])
    assert set(gi["materials"]) == {"m1"}  # 同 url → 一条
    assert _blocks(gi)[0]["text"] == "[image: m1 m1]"


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
    assert gi["materials"]["m1"]["ref_type"] == "oss_path"
