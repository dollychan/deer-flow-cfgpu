"""Tests for MessageStreamMiddleware (wrap_model_call / wrap_tool_call hooks)."""

from __future__ import annotations

import json
from types import SimpleNamespace
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


def _tool_msg(content: str = "ok", tool_call_id: str = "tc_1", name: str = "bash", status: str = "success", msg_id: str = "tmsg_001", artifact=None) -> ToolMessage:
    return ToolMessage(content=content, tool_call_id=tool_call_id, name=name, status=status, id=msg_id, artifact=artifact)


def _tool(name: str, visibility: str | None = None) -> SimpleNamespace:
    """A minimal BaseTool stand-in exposing .name and .metadata."""
    metadata = {"visibility": visibility} if visibility is not None else None
    return SimpleNamespace(name=name, metadata=metadata)


def _model_request(tools: list | None = None) -> SimpleNamespace:
    return SimpleNamespace(tools=list(tools or []))


def _tool_request(tool: SimpleNamespace | None = None) -> SimpleNamespace:
    return SimpleNamespace(tool=tool)


def _middleware(max_content_chars: int = 4096, visibility_patterns=None, default_visibility: str = "internal") -> MessageStreamMiddleware:
    return MessageStreamMiddleware(
        max_content_chars=max_content_chars,
        visibility_patterns=visibility_patterns,
        default_visibility=default_visibility,
    )


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
# Visibility resolution
# ---------------------------------------------------------------------------

class TestResolveVisibility:
    def test_metadata_wins(self):
        mw = _middleware(visibility_patterns=[("present_files", "progress")])
        # metadata says artifact, pattern says progress → metadata wins
        assert mw._resolve_visibility(_tool("present_files", "artifact"), "present_files") == "artifact"

    def test_pattern_fallback_when_no_metadata(self):
        mw = _middleware(visibility_patterns=[("cfgpu_generate_*", "progress")])
        assert mw._resolve_visibility(_tool("cfgpu_generate_image"), "cfgpu_generate_image") == "progress"

    def test_first_pattern_wins(self):
        mw = _middleware(visibility_patterns=[("web_*", "progress"), ("*", "artifact")])
        assert mw._resolve_visibility(None, "web_search") == "progress"

    def test_default_internal_when_unmatched(self):
        mw = _middleware()
        assert mw._resolve_visibility(_tool("bash"), "bash") == "internal"

    def test_custom_default(self):
        mw = _middleware(default_visibility="progress")
        assert mw._resolve_visibility(_tool("bash"), "bash") == "progress"

    def test_invalid_metadata_visibility_falls_through(self):
        mw = _middleware(visibility_patterns=[("*", "progress")])
        assert mw._resolve_visibility(_tool("x", "bogus"), "x") == "progress"

    def test_none_tool_uses_patterns(self):
        mw = _middleware(visibility_patterns=[("present_*", "artifact")])
        assert mw._resolve_visibility(None, "present_files") == "artifact"


# ---------------------------------------------------------------------------
# _resolve_tool_message — unwraps bare ToolMessage or Command
# ---------------------------------------------------------------------------

class TestResolveToolMessage:
    def test_bare_tool_message(self):
        tm = _tool_msg()
        assert MessageStreamMiddleware._resolve_tool_message(tm) is tm

    def test_command_with_tool_message(self):
        from langgraph.types import Command
        tm = _tool_msg()
        cmd = Command(update={"messages": [tm], "artifacts": ["x"]})
        assert MessageStreamMiddleware._resolve_tool_message(cmd) is tm

    def test_command_without_messages(self):
        from langgraph.types import Command
        assert MessageStreamMiddleware._resolve_tool_message(Command(update={})) is None

    def test_command_messages_without_tool_message(self):
        from langgraph.types import Command
        cmd = Command(update={"messages": [AIMessage(content="x")]})
        assert MessageStreamMiddleware._resolve_tool_message(cmd) is None

    def test_other_returns_none(self):
        assert MessageStreamMiddleware._resolve_tool_message("nope") is None


# ---------------------------------------------------------------------------
# wrap_model_call — sync
# ---------------------------------------------------------------------------

class TestWrapModelCallSync:
    def test_emits_ai_message_with_content(self):
        mw = _middleware()
        ai = _ai_msg(content="Hello there")
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            result = mw.wrap_model_call(_model_request(), MagicMock(return_value=ai))

        assert result is ai
        assert len(captured) == 1
        evt = captured[0]
        assert evt["type"] == "ai_message"
        assert evt["message_id"] == "msg_001"
        assert evt["content"] == "Hello there"
        assert evt["tool_calls"] == []

    def test_emits_visible_tool_calls(self):
        mw = _middleware()
        ai = _ai_msg(tool_calls=[{"id": "tc_1", "name": "web_search", "args": {"q": "x"}}])
        req = _model_request(tools=[_tool("web_search", "progress")])
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_model_call(req, MagicMock(return_value=ai))

        assert captured[0]["tool_calls"] == [{"id": "tc_1", "name": "web_search", "args": {"q": "x"}}]

    def test_filters_internal_tool_calls(self):
        """An internal tool's call is dropped; a sibling visible call is kept."""
        mw = _middleware()
        ai = _ai_msg(
            content="working",
            tool_calls=[
                {"id": "tc_1", "name": "bash", "args": {}},          # internal (default)
                {"id": "tc_2", "name": "present_files", "args": {}},  # artifact (metadata)
            ],
        )
        req = _model_request(tools=[_tool("bash"), _tool("present_files", "artifact")])
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_model_call(req, MagicMock(return_value=ai))

        assert [tc["id"] for tc in captured[0]["tool_calls"]] == ["tc_2"]

    def test_skips_when_only_internal_tool_calls_and_no_content(self):
        mw = _middleware()
        ai = _ai_msg(content="", tool_calls=[{"id": "tc_1", "name": "bash", "args": {}}])
        req = _model_request(tools=[_tool("bash")])
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_model_call(req, MagicMock(return_value=ai))

        assert captured == []

    def test_skips_empty_ai_message(self):
        mw = _middleware()
        ai = _ai_msg(content="", tool_calls=[])
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_model_call(_model_request(), MagicMock(return_value=ai))

        assert captured == []

    def test_returns_original_response_unchanged(self):
        mw = _middleware()
        ai = _ai_msg(content="test")
        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = lambda _: None
            result = mw.wrap_model_call(_model_request(), MagicMock(return_value=ai))

        assert result is ai

    def test_survives_stream_writer_failure(self):
        mw = _middleware()
        ai = _ai_msg(content="hello")
        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer", side_effect=RuntimeError("no writer")):
            result = mw.wrap_model_call(_model_request(), MagicMock(return_value=ai))

        assert result is ai  # no exception propagated

    def test_block_format_content(self):
        mw = _middleware()
        ai = AIMessage(content=[{"type": "text", "text": "block reply"}], id="msg_blk")
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_model_call(_model_request(), MagicMock(return_value=ai))

        assert captured[0]["content"] == "block reply"

    def test_extracts_ai_message_from_model_response(self):
        """Handler returning ModelResponse (with .result list) is handled correctly."""
        mw = _middleware()
        ai = _ai_msg(content="from model response")

        class _FakeModelResponse:
            result = [ai]

        model_response = _FakeModelResponse()
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            result = mw.wrap_model_call(_model_request(), MagicMock(return_value=model_response))

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
            result = await mw.awrap_model_call(_model_request(), AsyncMock(return_value=ai))

        assert result is ai
        assert len(captured) == 1
        assert captured[0]["type"] == "ai_message"
        assert captured[0]["content"] == "async reply"

    @pytest.mark.asyncio
    async def test_async_filters_internal_tool_calls(self):
        mw = _middleware()
        ai = _ai_msg(content="", tool_calls=[{"id": "tc_1", "name": "bash", "args": {}}])
        req = _model_request(tools=[_tool("bash")])
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            await mw.awrap_model_call(req, AsyncMock(return_value=ai))

        assert captured == []


# ---------------------------------------------------------------------------
# wrap_tool_call — sync
# ---------------------------------------------------------------------------

class TestWrapToolCallSync:
    def test_progress_emits_tool_result(self):
        mw = _middleware()
        tm = _tool_msg(content="file1.py\nfile2.py", name="web_search", status="success")
        req = _tool_request(_tool("web_search", "progress"))
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            result = mw.wrap_tool_call(req, MagicMock(return_value=tm))

        assert result is tm
        assert len(captured) == 1
        evt = captured[0]
        assert evt["type"] == "tool_result"
        assert evt["tool_call_id"] == "tc_1"
        assert evt["name"] == "web_search"
        # Non-JSON prose is wrapped so content is always an object.
        assert evt["content"] == {"message": "file1.py\nfile2.py"}
        assert evt["status"] == "success"

    def test_internal_tool_emits_nothing(self):
        mw = _middleware()
        tm = _tool_msg(name="bash")
        req = _tool_request(_tool("bash"))  # default internal
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            result = mw.wrap_tool_call(req, MagicMock(return_value=tm))

        assert result is tm
        assert captured == []

    def test_artifact_tool_emits_artifact_event(self):
        from langgraph.types import Command
        mw = _middleware()
        items = [{"ref": "https://cdn.cfgpu.com/gen/hero.png", "kind": "url", "expires_at": None}]
        tm = _tool_msg(content="Successfully presented URLs", name="present_files", tool_call_id="tc_p", artifact={"items": items})
        cmd = Command(update={"artifacts": ["https://cdn.cfgpu.com/gen/hero.png"], "messages": [tm]})
        req = _tool_request(_tool("present_files", "artifact"))
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            result = mw.wrap_tool_call(req, MagicMock(return_value=cmd))

        assert result is cmd
        assert len(captured) == 1
        evt = captured[0]
        assert evt["type"] == "artifact"
        assert evt["name"] == "present_files"
        assert evt["tool_call_id"] == "tc_p"
        assert evt["items"] == items
        assert evt["status"] == "success"

    def test_artifact_tool_error_falls_back_to_tool_result(self):
        """An artifact tool that errors (no artifact payload) surfaces as tool_result."""
        from langgraph.types import Command
        mw = _middleware()
        tm = _tool_msg(content="Error: urls and expires_at_list must have the same length", name="present_files", status="success")
        cmd = Command(update={"messages": [tm]})
        req = _tool_request(_tool("present_files", "artifact"))
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_tool_call(req, MagicMock(return_value=cmd))

        assert len(captured) == 1
        assert captured[0]["type"] == "tool_result"
        assert "Error" in captured[0]["content"]["message"]

    def test_json_object_content_passes_through(self):
        """A tool whose content is a JSON object (e.g. cfgpu generate_image) is emitted as-is."""
        mw = _middleware()
        payload = {"urls": ["https://cdn.cfgpu.com/img-abc.png"], "task_id": "task-1", "cost_tokens": 100, "artifact": True}
        tm = _tool_msg(content=json.dumps(payload), name="cfgpu_generate_image", tool_call_id="tc_g")
        req = _tool_request(_tool("cfgpu_generate_image", "progress"))
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_tool_call(req, MagicMock(return_value=tm))

        assert captured[0]["content"] == payload

    def test_json_array_content_wrapped_in_items(self):
        """A tool whose content is a JSON array (e.g. list_models) is wrapped as {"items": [...]}."""
        mw = _middleware()
        models = [{"adapter_id": "wan-2-0-fast"}, {"adapter_id": "doubao-seedream"}]
        tm = _tool_msg(content=json.dumps(models), name="cfgpu_list_models", tool_call_id="tc_l")
        req = _tool_request(_tool("cfgpu_list_models", "progress"))
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_tool_call(req, MagicMock(return_value=tm))

        assert captured[0]["content"] == {"items": models}

    def test_oversized_json_degrades_to_message(self):
        """JSON beyond max_content_chars degrades to a truncated message object (MQ size guard)."""
        mw = _middleware(max_content_chars=20)
        tm = _tool_msg(content=json.dumps({"k": "v" * 100}), name="cfgpu_generate_image", tool_call_id="tc_b")
        req = _tool_request(_tool("cfgpu_generate_image", "progress"))
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_tool_call(req, MagicMock(return_value=tm))

        content = captured[0]["content"]
        assert set(content.keys()) == {"message"}
        assert "truncated" in content["message"]

    def test_emits_tool_result_error_status(self):
        mw = _middleware()
        tm = _tool_msg(content="Permission denied", name="web_search", status="error")
        req = _tool_request(_tool("web_search", "progress"))
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_tool_call(req, MagicMock(return_value=tm))

        assert captured[0]["status"] == "error"

    def test_truncates_large_content(self):
        mw = _middleware(max_content_chars=10)
        tm = _tool_msg(content="x" * 100, name="web_search")
        req = _tool_request(_tool("web_search", "progress"))
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_tool_call(req, MagicMock(return_value=tm))

        # Non-JSON text degrades to a truncated message object.
        message = captured[0]["content"]["message"]
        assert message.startswith("x" * 10)
        assert "truncated" in message
        assert "90 chars omitted" in message

    def test_returns_original_result_unchanged(self):
        mw = _middleware(default_visibility="progress")
        tm = _tool_msg()
        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = lambda _: None
            result = mw.wrap_tool_call(_tool_request(_tool("bash")), MagicMock(return_value=tm))

        assert result is tm

    def test_survives_stream_writer_failure(self):
        mw = _middleware(default_visibility="progress")
        tm = _tool_msg()
        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer", side_effect=RuntimeError("no writer")):
            result = mw.wrap_tool_call(_tool_request(_tool("bash")), MagicMock(return_value=tm))

        assert result is tm

    def test_message_id_included(self):
        mw = _middleware()
        tm = _tool_msg(msg_id="tmsg_999", name="web_search")
        req = _tool_request(_tool("web_search", "progress"))
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_tool_call(req, MagicMock(return_value=tm))

        assert captured[0]["message_id"] == "tmsg_999"

    def test_skips_emit_for_empty_command(self):
        """A Command with no ToolMessage in its update emits nothing."""
        from langgraph.types import Command
        mw = _middleware(default_visibility="progress")
        cmd = Command(update={})
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            result = mw.wrap_tool_call(_tool_request(_tool("x")), MagicMock(return_value=cmd))

        assert result is cmd
        assert captured == []


# ---------------------------------------------------------------------------
# wrap_tool_call — async
# ---------------------------------------------------------------------------

class TestWrapToolCallAsync:
    @pytest.mark.asyncio
    async def test_async_progress_emits_tool_result(self):
        mw = _middleware()
        tm = _tool_msg(content="async result", name="web_search", status="success")
        req = _tool_request(_tool("web_search", "progress"))
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            result = await mw.awrap_tool_call(req, AsyncMock(return_value=tm))

        assert result is tm
        assert captured[0]["type"] == "tool_result"
        assert captured[0]["content"] == {"message": "async result"}

    @pytest.mark.asyncio
    async def test_async_artifact_command(self):
        from langgraph.types import Command
        mw = _middleware()
        items = [{"ref": "/mnt/user-data/outputs/report.md", "kind": "path", "expires_at": None}]
        tm = _tool_msg(content="Successfully presented files", name="present_files", tool_call_id="tc_f", artifact={"items": items})
        cmd = Command(update={"artifacts": ["/mnt/user-data/outputs/report.md"], "messages": [tm]})
        req = _tool_request(_tool("present_files", "artifact"))
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            await mw.awrap_tool_call(req, AsyncMock(return_value=cmd))

        assert captured[0]["type"] == "artifact"
        assert captured[0]["items"] == items

    @pytest.mark.asyncio
    async def test_async_internal_emits_nothing(self):
        mw = _middleware()
        tm = _tool_msg(name="bash")
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            await mw.awrap_tool_call(_tool_request(_tool("bash")), AsyncMock(return_value=tm))

        assert captured == []
