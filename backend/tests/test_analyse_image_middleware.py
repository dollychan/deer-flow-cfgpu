"""P9 — AnalyseImageMiddleware（cfgpu-docs/materials.md §4.7, materials-impl-plan.md P9）。

覆盖重负载注入器：尾部 analyse_image ToolMessage 触发单轮 base64 注入、base64 只进流向模型的
request（不写回 history / 不进 final_state.messages）、多图分别标注归属、第二轮（新 AIMessage 后）
同一 ToolMessage 不再注入（单轮 ephemeral 结构保证）、fetch 失败→文本占位不断流、外部 url 仅
fetch 不 rehost、local_path 读盘无网络。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

import deerflow.agents.middlewares.analyse_image_middleware as mod
from deerflow.agents.materials.materialize import FetchedBytes
from deerflow.agents.middlewares.analyse_image_middleware import AnalyseImageMiddleware

_PNG = b"\x89PNG\r\n\x1a\n" + b"payload"


@dataclass
class FakeRequest:
    messages: list[Any]
    state: dict

    def override(self, *, messages):
        return FakeRequest(messages=messages, state=self.state)


def _img(mid: str, ref: str, ref_type: str = "global_url", **extra) -> dict:
    return {"id": mid, "kind": "image", "origin": "uplink", "ref_type": ref_type, "ref": ref, **extra}


def _analyse_turn(ids: list[str]) -> list[Any]:
    """末条 AIMessage(analyse_image tool_call) + 其 ToolMessage（尾部待回应）。"""
    return [
        HumanMessage(content="看看这些图"),
        AIMessage(content="", tool_calls=[{"name": "analyse_image", "id": "tc1", "args": {"images": ids}}]),
        ToolMessage(content=f"已排队分析图像 {', '.join(ids)}", tool_call_id="tc1", artifact={"analyse_image": {"ids": ids}}),
    ]


def _capture_handler():
    captured: dict = {}

    async def handler(req):
        captured["req"] = req
        return "RESP"

    return captured, handler


def _image_blocks(req) -> list[dict]:
    human = req.messages[-1]
    assert isinstance(human, HumanMessage)
    return [b for b in human.content if isinstance(b, dict) and b.get("type") == "image_url"]


def _text_blob(req) -> str:
    human = req.messages[-1]
    return "".join(b.get("text", "") for b in human.content if isinstance(b, dict) and b.get("type") == "text")


@pytest.fixture
def mw():
    return AnalyseImageMiddleware()


# --- 触发注入 -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tail_toolmessage_triggers_injection(mw, monkeypatch):
    calls: list[str] = []

    async def fake_fetch(url):
        calls.append(url)
        return FetchedBytes(data=_PNG, content_type="image/png")

    monkeypatch.setattr(mod, "fetch_bytes", fake_fetch)
    state = {"materials": {"m1": _img("m1", "https://cdn/a.png")}}
    req = FakeRequest(messages=_analyse_turn(["m1"]), state=state)
    captured, handler = _capture_handler()

    out = await mw.awrap_model_call(req, handler)
    assert out == "RESP"
    blocks = _image_blocks(captured["req"])
    assert len(blocks) == 1
    assert blocks[0]["image_url"]["url"].startswith("data:image/png;base64,")
    assert calls == ["https://cdn/a.png"]  # 外部 url：fetch 唯一触发


@pytest.mark.asyncio
async def test_base64_not_written_back_to_history(mw, monkeypatch):
    async def fake_fetch(url):
        return FetchedBytes(data=_PNG, content_type="image/png")

    monkeypatch.setattr(mod, "fetch_bytes", fake_fetch)
    original = _analyse_turn(["m1"])
    state = {"materials": {"m1": _img("m1", "https://cdn/a.png")}}
    req = FakeRequest(messages=original, state=state)
    captured, handler = _capture_handler()

    await mw.awrap_model_call(req, handler)
    # 注入只在流向模型的 request（override 出的新 messages），原始 history 不变
    assert len(req.messages) == 3
    assert len(captured["req"].messages) == 4
    assert "base64" not in str([m.content for m in original])


@pytest.mark.asyncio
async def test_multi_image_attribution(mw, monkeypatch):
    async def fake_fetch(url):
        return FetchedBytes(data=_PNG, content_type="image/png")

    monkeypatch.setattr(mod, "fetch_bytes", fake_fetch)
    state = {"materials": {"m1": _img("m1", "https://cdn/a.png"), "m2": _img("m2", "https://cdn/b.png")}}
    req = FakeRequest(messages=_analyse_turn(["m1", "m2"]), state=state)
    captured, handler = _capture_handler()

    await mw.awrap_model_call(req, handler)
    assert len(_image_blocks(captured["req"])) == 2
    text = _text_blob(captured["req"])
    assert "m1" in text and "m2" in text  # 分别标注归属


# --- 单轮 ephemeral 结构保证 -----------------------------------------------------


@pytest.mark.asyncio
async def test_second_turn_same_toolmessage_not_reinjected(mw, monkeypatch):
    async def fake_fetch(url):
        return FetchedBytes(data=_PNG, content_type="image/png")

    monkeypatch.setattr(mod, "fetch_bytes", fake_fetch)
    # 模型已在上一轮陈述发现 → 新 AIMessage 落在 ToolMessage 之后 → 尾部不再有待回应 analyse ToolMessage
    messages = [*_analyse_turn(["m1"]), AIMessage(content="我看到 m1 是……")]
    state = {"materials": {"m1": _img("m1", "https://cdn/a.png")}}
    req = FakeRequest(messages=messages, state=state)
    captured, handler = _capture_handler()

    await mw.awrap_model_call(req, handler)
    assert captured["req"] is req  # 未注入：原样放行
    assert len(captured["req"].messages) == 4


@pytest.mark.asyncio
async def test_no_analyse_tail_passthrough(mw):
    messages = [HumanMessage(content="hi"), AIMessage(content="hello")]
    req = FakeRequest(messages=messages, state={"materials": {}})
    captured, handler = _capture_handler()
    await mw.awrap_model_call(req, handler)
    assert captured["req"] is req


# --- 降级 + 不 rehost ------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_failure_text_placeholder_no_throw(mw, monkeypatch):
    async def boom(url):
        raise RuntimeError("dead link")

    monkeypatch.setattr(mod, "fetch_bytes", boom)
    state = {"materials": {"m1": _img("m1", "https://cdn/dead.png")}}
    req = FakeRequest(messages=_analyse_turn(["m1"]), state=state)
    captured, handler = _capture_handler()

    out = await mw.awrap_model_call(req, handler)
    assert out == "RESP"  # 不断流
    assert _image_blocks(captured["req"]) == []  # 无图块
    assert "不可用" in _text_blob(captured["req"])  # 文本占位


@pytest.mark.asyncio
async def test_external_url_only_fetched_not_rehosted(mw, monkeypatch):
    async def fake_fetch(url):
        return FetchedBytes(data=_PNG, content_type="image/png")

    monkeypatch.setattr(mod, "fetch_bytes", fake_fetch)
    materials = {"m1": _img("m1", "https://cdn/a.png")}
    state = {"materials": materials}
    req = FakeRequest(messages=_analyse_turn(["m1"]), state=state)
    _, handler = _capture_handler()

    await mw.awrap_model_call(req, handler)
    # state 不变：ref 仍 global_url、未升级 oss_path（不 rehost）
    assert materials["m1"]["ref_type"] == "global_url"
    assert materials["m1"]["ref"] == "https://cdn/a.png"
    assert "origin_url" not in materials["m1"]


# --- 出口三分：local_path 读盘无网络 --------------------------------------------


@pytest.mark.asyncio
async def test_local_path_read_from_disk_no_network(mw, monkeypatch, tmp_path):
    async def boom(url):
        raise AssertionError("local_path 应读盘，不得 fetch")

    monkeypatch.setattr(mod, "fetch_bytes", boom)
    outputs = tmp_path / "user-data" / "outputs"
    outputs.mkdir(parents=True)
    (outputs / "a.png").write_bytes(_PNG)
    state = {
        "materials": {"m1": _img("m1", "agent-artifacts/t1/x.png", ref_type="oss_path", local_path="/mnt/user-data/outputs/a.png")},
        "thread_data": {"outputs_path": str(outputs)},
    }
    req = FakeRequest(messages=_analyse_turn(["m1"]), state=state)
    captured, handler = _capture_handler()

    await mw.awrap_model_call(req, handler)
    blocks = _image_blocks(captured["req"])
    assert len(blocks) == 1
    assert blocks[0]["image_url"]["url"].startswith("data:image/png;base64,")
