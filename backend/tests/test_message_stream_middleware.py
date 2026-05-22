"""Tests for MessageStreamMiddleware (wrap_model_call / wrap_tool_call hooks)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import AIMessage, ToolMessage

from deerflow.agents.middlewares.message_stream_middleware import (
    MessageStreamMiddleware,
    _extract_text_content,
    _truncate,
)


# ---------------------------------------------------------------------------
# Helper factories
# ---------------------------------------------------------------------------

def _ai_msg(content: str = "", tool_calls: list[dict] | None = None, msg_id: str = "msg_001") -> AIMessage:
    from langchain_core.messages.tool import ToolCall
    calls = [ToolCall(id=tc["id"], name=tc["name"], args=tc["args"]) for tc in (tool_calls or [])]
    return AIMessage(content=content, tool_calls=calls, id=msg_id)


def _tool_msg(content: str = "ok", tool_call_id: str = "tc_1", name: str = "bash", status: str = "success", msg_id: str = "tmsg_001") -> ToolMessage:
    return ToolMessage(content=content, tool_call_id=tool_call_id, name=name, status=status, id=msg_id)


def _middleware(max_content_chars: int = 4096) -> MessageStreamMiddleware:
    return MessageStreamMiddleware(max_content_chars=max_content_chars)


def _mock_request() -> MagicMock:
    """Return a minimal ModelRequest mock."""
    return MagicMock()


def _mock_tool_request() -> MagicMock:
    """Return a minimal ToolCallRequest mock."""
    return MagicMock()


# ---------------------------------------------------------------------------
# Pure helper tests
# ---------------------------------------------------------------------------

class TestExtractTextContent:
    def test_plain_string(self):
        assert _extract_text_content("hello") == "hello"

    def test_list_of_text_blocks(self):
        blocks = [{"type": "text", "text": "foo"}, {"type": "text", "text": "bar"}]
        assert _extract_text_content(blocks) == "foobar"

    def test_list_with_non_text_blocks_skipped(self):
        blocks = [{"type": "image_url", "url": "..."}, {"type": "text", "text": "hi"}]
        assert _extract_text_content(blocks) == "hi"

    def test_list_with_raw_strings(self):
        assert _extract_text_content(["a", "b"]) == "ab"

    def test_empty_list(self):
        assert _extract_text_content([]) == ""

    def test_empty_string(self):
        assert _extract_text_content("") == ""


class TestTruncate:
    def test_under_limit(self):
        assert _truncate("hello", 10) == "hello"

    def test_exactly_at_limit(self):
        assert _truncate("hello", 5) == "hello"

    def test_over_limit(self):
        result = _truncate("hello world", 5)
        assert result.startswith("hello")
        assert "truncated" in result
        assert "6 chars omitted" in result

    def test_empty(self):
        assert _truncate("", 10) == ""


# ---------------------------------------------------------------------------
# wrap_model_call — sync
# Handler returns AIMessage directly (a valid ModelCallResult variant).
# ---------------------------------------------------------------------------

class TestWrapModelCallSync:
    def test_emits_ai_message_with_content(self):
        mw = _middleware()
        ai = _ai_msg(content="Hello there")
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            result = mw.wrap_model_call(_mock_request(), MagicMock(return_value=ai))

        assert result is ai
        assert len(captured) == 1
        evt = captured[0]
        assert evt["type"] == "ai_message"
        assert evt["message_id"] == "msg_001"
        assert evt["content"] == "Hello there"
        assert evt["tool_calls"] == []

    def test_emits_ai_message_with_tool_calls(self):
        mw = _middleware()
        ai = _ai_msg(tool_calls=[{"id": "tc_1", "name": "bash", "args": {"command": "ls"}}])
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_model_call(_mock_request(), MagicMock(return_value=ai))

        evt = captured[0]
        assert evt["tool_calls"] == [{"id": "tc_1", "name": "bash", "args": {"command": "ls"}}]

    def test_skips_empty_ai_message(self):
        mw = _middleware()
        ai = _ai_msg(content="", tool_calls=[])
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_model_call(_mock_request(), MagicMock(return_value=ai))

        assert captured == []

    def test_returns_original_response_unchanged(self):
        mw = _middleware()
        ai = _ai_msg(content="test")
        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = lambda _: None
            result = mw.wrap_model_call(_mock_request(), MagicMock(return_value=ai))

        assert result is ai

    def test_survives_stream_writer_failure(self):
        mw = _middleware()
        ai = _ai_msg(content="hello")
        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer", side_effect=RuntimeError("no writer")):
            result = mw.wrap_model_call(_mock_request(), MagicMock(return_value=ai))

        assert result is ai  # no exception propagated

    def test_block_format_content(self):
        mw = _middleware()
        ai = AIMessage(content=[{"type": "text", "text": "block reply"}], id="msg_blk")
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_model_call(_mock_request(), MagicMock(return_value=ai))

        assert captured[0]["content"] == "block reply"

    def test_extracts_ai_message_from_model_response(self):
        """Handler returning ModelResponse (with .result list) is handled correctly."""
        mw = _middleware()
        ai = _ai_msg(content="from model response")

        # Use a plain stub with only .result — avoids MagicMock's dynamic attribute creation
        # which would cause hasattr(mock, "model_response") to always return True.
        class _FakeModelResponse:
            result = [ai]

        model_response = _FakeModelResponse()
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            result = mw.wrap_model_call(_mock_request(), MagicMock(return_value=model_response))

        assert result is model_response
        assert len(captured) == 1
        assert captured[0]["content"] == "from model response"


# ---------------------------------------------------------------------------
# wrap_model_call — async
# ---------------------------------------------------------------------------

class TestWrapModelCallAsync:
    @pytest.mark.asyncio
    async def test_async_emits_ai_message(self):
        mw = _middleware()
        ai = _ai_msg(content="async reply")
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            result = await mw.awrap_model_call(_mock_request(), AsyncMock(return_value=ai))

        assert result is ai
        assert len(captured) == 1
        assert captured[0]["type"] == "ai_message"
        assert captured[0]["content"] == "async reply"

    @pytest.mark.asyncio
    async def test_async_skips_empty(self):
        mw = _middleware()
        ai = _ai_msg(content="", tool_calls=[])
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            await mw.awrap_model_call(_mock_request(), AsyncMock(return_value=ai))

        assert captured == []


# ---------------------------------------------------------------------------
# wrap_tool_call — sync
# ---------------------------------------------------------------------------

class TestWrapToolCallSync:
    def test_emits_tool_result_success(self):
        mw = _middleware()
        tm = _tool_msg(content="file1.py\nfile2.py", status="success")
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            result = mw.wrap_tool_call(_mock_tool_request(), MagicMock(return_value=tm))

        assert result is tm
        assert len(captured) == 1
        evt = captured[0]
        assert evt["type"] == "tool_result"
        assert evt["tool_call_id"] == "tc_1"
        assert evt["name"] == "bash"
        assert evt["content"] == "file1.py\nfile2.py"
        assert evt["status"] == "success"

    def test_emits_tool_result_error(self):
        mw = _middleware()
        tm = _tool_msg(content="Permission denied", status="error")
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_tool_call(_mock_tool_request(), MagicMock(return_value=tm))

        assert captured[0]["status"] == "error"

    def test_truncates_large_content(self):
        mw = _middleware(max_content_chars=10)
        long_output = "x" * 100
        tm = _tool_msg(content=long_output)
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_tool_call(_mock_tool_request(), MagicMock(return_value=tm))

        content = captured[0]["content"]
        assert content.startswith("x" * 10)
        assert "truncated" in content
        assert "90 chars omitted" in content

    def test_returns_original_tool_message_unchanged(self):
        mw = _middleware()
        tm = _tool_msg()
        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = lambda _: None
            result = mw.wrap_tool_call(_mock_tool_request(), MagicMock(return_value=tm))

        assert result is tm

    def test_survives_stream_writer_failure(self):
        mw = _middleware()
        tm = _tool_msg()
        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer", side_effect=RuntimeError("no writer")):
            result = mw.wrap_tool_call(_mock_tool_request(), MagicMock(return_value=tm))

        assert result is tm

    def test_message_id_included(self):
        mw = _middleware()
        tm = _tool_msg(msg_id="tmsg_999")
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_tool_call(_mock_tool_request(), MagicMock(return_value=tm))

        assert captured[0]["message_id"] == "tmsg_999"

    def test_skips_emit_for_command_result(self):
        """wrap_tool_call must not emit when handler returns a Command (not ToolMessage)."""
        from langgraph.types import Command
        mw = _middleware()
        cmd = Command(update={})
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            result = mw.wrap_tool_call(_mock_tool_request(), MagicMock(return_value=cmd))

        assert result is cmd
        assert captured == []


# ---------------------------------------------------------------------------
# wrap_tool_call — async
# ---------------------------------------------------------------------------

class TestWrapToolCallAsync:
    @pytest.mark.asyncio
    async def test_async_emits_tool_result(self):
        mw = _middleware()
        tm = _tool_msg(content="async result", status="success")
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            result = await mw.awrap_tool_call(_mock_tool_request(), AsyncMock(return_value=tm))

        assert result is tm
        assert captured[0]["type"] == "tool_result"
        assert captured[0]["content"] == "async result"

    @pytest.mark.asyncio
    async def test_async_truncates_large_content(self):
        mw = _middleware(max_content_chars=5)
        tm = _tool_msg(content="hello world")
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            await mw.awrap_tool_call(_mock_tool_request(), AsyncMock(return_value=tm))

        assert captured[0]["content"].startswith("hello")
        assert "truncated" in captured[0]["content"]
