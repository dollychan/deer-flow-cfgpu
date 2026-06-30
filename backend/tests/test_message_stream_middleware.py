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


def _middleware(max_content_chars: int = 4096, max_structured_bytes: int = 65536, visibility_patterns=None, default_visibility: str = "internal") -> MessageStreamMiddleware:
    return MessageStreamMiddleware(
        max_content_chars=max_content_chars,
        max_structured_bytes=max_structured_bytes,
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
        mw = _middleware(visibility_patterns=[("cfdream_generate_*", "progress")])
        assert mw._resolve_visibility(_tool("cfdream_generate_image"), "cfdream_generate_image") == "progress"

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
        # The tool's textual content rides alongside items (non-JSON prose wrapped).
        assert evt["content"] == {"message": "Successfully presented URLs"}
        assert evt["status"] == "success"

    def test_artifact_tool_emits_json_content_alongside_items(self):
        """An artifact tool whose content is a JSON object (cfdream generate_*) carries it at the same level as items."""
        from langgraph.types import Command
        mw = _middleware()
        payload = {"urls": ["https://cdn.cfgpu.com/img-abc.png"], "task_id": "task-1", "cost_tokens": 100, "artifact": True}
        items = [{"ref": "https://cdn.cfgpu.com/img-abc.png", "kind": "url", "expires_at": None}]
        tm = _tool_msg(content=json.dumps(payload), name="cfdream_generate_image", tool_call_id="tc_g", artifact={"items": items})
        cmd = Command(update={"messages": [tm]})
        req = _tool_request(_tool("cfdream_generate_image", "artifact"))
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_tool_call(req, MagicMock(return_value=cmd))

        evt = captured[0]
        assert evt["type"] == "artifact"
        assert evt["items"] == items
        assert evt["content"] == payload

    def test_artifact_tool_terminal_error_falls_back_to_tool_result(self):
        """An artifact tool whose terminal result is a failure (status='error', no items)
        still surfaces as a tool_result so the client learns the generation failed."""
        from langgraph.types import Command
        mw = _middleware()
        tm = _tool_msg(content="Error: generation failed", name="cfdream_generate_image", status="error")
        cmd = Command(update={"messages": [tm]})
        req = _tool_request(_tool("cfdream_generate_image", "artifact"))
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_tool_call(req, MagicMock(return_value=cmd))

        assert len(captured) == 1
        assert captured[0]["type"] == "tool_result"
        assert "Error" in captured[0]["content"]["message"]

    def test_artifact_tool_content_error_falls_back_to_tool_result(self):
        """An artifact tool whose content reports a failure (truthy ``error``) while
        ToolMessage.status stays ``success`` — cfgpu's non-raising task_failed shape — still
        surfaces as a tool_result with an explicit ``error`` status so the client learns the
        generation failed (it would otherwise be misread as an in-flight intermediate)."""
        from langgraph.types import Command
        mw = _middleware()
        payload = {"error": True, "error_type": "task_failed", "message": "任务执行失败", "retryable": False}
        tm = _tool_msg(content=json.dumps(payload), name="cfdream_generate_image", status="success")
        cmd = Command(update={"messages": [tm]})
        req = _tool_request(_tool("cfdream_generate_image", "artifact"))
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_tool_call(req, MagicMock(return_value=cmd))

        assert len(captured) == 1
        evt = captured[0]
        assert evt["type"] == "tool_result"
        assert evt["status"] == "error"  # uniform error signal despite ToolMessage.status="success"
        assert evt["content"] == payload  # clean structured error preserved (no prose mangling)

    def test_artifact_tool_falsy_error_key_not_treated_as_failure(self):
        """A success result carrying ``error: false`` (no items) is an in-flight intermediate,
        not a failure — it must stay suppressed."""
        from langgraph.types import Command
        mw = _middleware()
        payload = {"task_id": "task-1", "status": "processing", "error": False}
        tm = _tool_msg(content=json.dumps(payload), name="cfdream_task_status", status="success")
        cmd = Command(update={"messages": [tm]})
        req = _tool_request(_tool("cfdream_task_status", "artifact"))
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_tool_call(req, MagicMock(return_value=cmd))

        assert captured == []

    def test_artifact_tool_intermediate_success_no_items_suppressed(self):
        """An artifact tool's in-flight intermediate (status='success', no items — e.g.
        generate_*/task_* polling a not-yet-ready URL) emits nothing downstream. The LLM
        still sees the content; the client only gets the final artifact."""
        from langgraph.types import Command
        mw = _middleware()
        payload = {"task_id": "task-1", "status": "processing"}  # no top-level artifact/urls
        tm = _tool_msg(content=json.dumps(payload), name="cfdream_task_status", status="success")
        cmd = Command(update={"messages": [tm]})
        req = _tool_request(_tool("cfdream_task_status", "artifact"))
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            result = mw.wrap_tool_call(req, MagicMock(return_value=cmd))

        assert result is cmd  # content/ToolMessage unchanged
        assert captured == []  # nothing emitted

    def test_empty_web_search_result_suppressed(self):
        """web_search 'No results found' (content carries an error key) emits no tool_result."""
        mw = _middleware()
        tm = _tool_msg(content=json.dumps({"error": "No results found", "query": "xyzzy"}), name="web_search", status="success")
        req = _tool_request(_tool("web_search", "progress"))
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            result = mw.wrap_tool_call(req, MagicMock(return_value=tm))

        assert result is tm  # content unchanged so the LLM still sees the error
        assert captured == []

    def test_empty_image_search_result_suppressed(self):
        """image_search 'No images found' is suppressed the same way (error-key presence)."""
        mw = _middleware()
        tm = _tool_msg(content=json.dumps({"error": "No images found", "query": "xyzzy"}), name="image_search", status="success")
        req = _tool_request(_tool("image_search", "progress"))
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_tool_call(req, MagicMock(return_value=tm))

        assert captured == []

    def test_non_empty_web_search_still_emits(self):
        """A web_search with real results (no error key) still emits a tool_result."""
        mw = _middleware()
        payload = {"query": "deerflow", "total_results": 1, "results": [{"title": "t", "url": "u", "content": "c"}]}
        tm = _tool_msg(content=json.dumps(payload), name="web_search", status="success")
        req = _tool_request(_tool("web_search", "progress"))
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_tool_call(req, MagicMock(return_value=tm))

        assert len(captured) == 1
        assert captured[0]["type"] == "tool_result"
        assert captured[0]["content"] == payload

    def test_empty_error_key_only_gates_search_tools(self):
        """A non-search progress tool whose content has an error key is NOT suppressed —
        the empty-search gate is scoped to web_search/image_search by name."""
        mw = _middleware()
        tm = _tool_msg(content=json.dumps({"error": "boom"}), name="some_other_tool", status="success")
        req = _tool_request(_tool("some_other_tool", "progress"))
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_tool_call(req, MagicMock(return_value=tm))

        assert len(captured) == 1
        assert captured[0]["type"] == "tool_result"

    def test_json_object_content_passes_through(self):
        """A tool whose content is a JSON object (e.g. cfdream generate_image) is emitted as-is."""
        mw = _middleware()
        payload = {"urls": ["https://cdn.cfgpu.com/img-abc.png"], "task_id": "task-1", "cost_tokens": 100, "artifact": True}
        tm = _tool_msg(content=json.dumps(payload), name="cfdream_generate_image", tool_call_id="tc_g")
        req = _tool_request(_tool("cfdream_generate_image", "progress"))
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_tool_call(req, MagicMock(return_value=tm))

        assert captured[0]["content"] == payload

    def test_json_array_content_wrapped_in_items(self):
        """A tool whose content is a JSON array (e.g. list_models) is wrapped as {"items": [...]}."""
        mw = _middleware()
        models = [{"adapter_id": "wan-2-0-fast"}, {"adapter_id": "doubao-seedream"}]
        tm = _tool_msg(content=json.dumps(models), name="cfdream_list_models", tool_call_id="tc_l")
        req = _tool_request(_tool("cfdream_list_models", "progress"))
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_tool_call(req, MagicMock(return_value=tm))

        assert captured[0]["content"] == {"items": models}

    def test_structured_content_merged_into_tool_result(self):
        """MCP structuredContent (on artifact) merges into a progress tool_result content.

        understand_vision: lean content {id, model, message} + structuredContent
        {reasoning_content, usage, payload} → client gets the full reconstructed result.
        """
        mw = _middleware()
        lean = {"id": "chatcmpl-1", "model": "qwen3-vl", "message": "the answer"}
        sc = {"reasoning_content": "thinking...", "usage": {"prompt_tokens": 10}, "payload": {"model": "qwen", "messages": []}}
        tm = _tool_msg(content=json.dumps(lean), name="cfdream_understand_vision", tool_call_id="tc_u", artifact={"structured_content": sc})
        req = _tool_request(_tool("cfdream_understand_vision", "progress"))
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_tool_call(req, MagicMock(return_value=tm))

        evt = captured[0]
        assert evt["type"] == "tool_result"
        assert evt["content"] == {**lean, **sc}

    def test_structured_content_merged_into_artifact_event(self):
        """MCP structuredContent rides alongside items in an artifact event.

        generate_*: MaterialsMiddleware preserves structured_content next to items, so
        usage/payload reach the client artifact event content.
        """
        from langgraph.types import Command
        mw = _middleware()
        body = {"task_id": "cgt-1", "model_used": "doubao-seedance", "seed": 42, "materials": ["m1"]}
        items = [{"id": "m1", "ref": "agent-artifacts/x.png", "kind": "image", "stable": True}]
        sc = {"usage": {"totalTokens": 100}, "payload": {"model": "doubao", "prompt": "x"}}
        tm = _tool_msg(content=json.dumps(body), name="cfdream_generate_image", tool_call_id="tc_g", artifact={"items": items, "structured_content": sc})
        cmd = Command(update={"messages": [tm]})
        req = _tool_request(_tool("cfdream_generate_image", "artifact"))
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_tool_call(req, MagicMock(return_value=cmd))

        evt = captured[0]
        assert evt["type"] == "artifact"
        assert evt["items"] == items
        assert evt["content"] == {**body, **sc}

    def test_artifact_without_structured_content_unchanged(self):
        """An items-only artifact (no structured_content) emits content from text alone."""
        from langgraph.types import Command
        mw = _middleware()
        items = [{"id": "m1", "ref": "agent-artifacts/x.png", "kind": "image", "stable": True}]
        tm = _tool_msg(content=json.dumps({"materials": ["m1"]}), name="cfdream_generate_image", tool_call_id="tc_g", artifact={"items": items})
        cmd = Command(update={"messages": [tm]})
        req = _tool_request(_tool("cfdream_generate_image", "artifact"))
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_tool_call(req, MagicMock(return_value=cmd))

        assert captured[0]["content"] == {"materials": ["m1"]}

    def test_structured_content_stripped_from_tool_message_after_emit(self):
        """After a progress emit, structured_content is removed from the persisted ToolMessage.

        It rode the downstream event (client side channel) but must not linger on the
        checkpointed message: the model never saw it and its payload can carry presigned URLs.
        """
        mw = _middleware()
        lean = {"id": "chatcmpl-1", "model": "qwen3-vl", "message": "the answer"}
        sc = {"usage": {"prompt_tokens": 10}, "payload": {"reference_images": ["https://oss/...sig"]}}
        tm = _tool_msg(content=json.dumps(lean), name="cfdream_understand_vision", tool_call_id="tc_u", artifact={"structured_content": sc})
        req = _tool_request(_tool("cfdream_understand_vision", "progress"))
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            result = mw.wrap_tool_call(req, MagicMock(return_value=tm))

        # The event still carries the full reconstructed content...
        assert captured[0]["content"] == {**lean, **sc}
        # ...but the persisted message no longer holds the side channel.
        assert result is tm
        assert tm.artifact is None

    def test_structured_content_stripped_but_items_preserved(self):
        """The artifact path strips structured_content while keeping the deliverable items."""
        from langgraph.types import Command
        mw = _middleware()
        body = {"task_id": "cgt-1", "materials": ["m1"]}
        items = [{"id": "m1", "ref": "agent-artifacts/x.png", "kind": "image", "stable": True}]
        sc = {"usage": {"totalTokens": 100}, "payload": {"reference_images": ["https://oss/...sig"]}}
        tm = _tool_msg(content=json.dumps(body), name="cfdream_generate_image", tool_call_id="tc_g", artifact={"items": items, "structured_content": sc})
        cmd = Command(update={"messages": [tm]})
        req = _tool_request(_tool("cfdream_generate_image", "artifact"))
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_tool_call(req, MagicMock(return_value=cmd))

        assert captured[0]["content"] == {**body, **sc}
        assert tm.artifact == {"items": items}

    def test_internal_tool_also_strips_structured_content(self):
        """Internal-visibility tools emit nothing downstream, yet the transient side channel
        is still dropped from the persisted message."""
        mw = _middleware(default_visibility="internal")
        tm = _tool_msg(content="{}", name="cfdream_secret", tool_call_id="tc_i", artifact={"structured_content": {"payload": {}}})
        req = _tool_request(_tool("cfdream_secret"))  # no metadata → default internal
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_tool_call(req, MagicMock(return_value=tm))

        assert captured == []  # internal: nothing emitted
        assert tm.artifact is None

    def test_oversized_json_degrades_to_message(self):
        """JSON beyond max_structured_bytes (hard cap) degrades to a truncated message object (MQ size guard)."""
        mw = _middleware(max_content_chars=20, max_structured_bytes=20)
        tm = _tool_msg(content=json.dumps({"k": "v" * 100}), name="cfdream_generate_image", tool_call_id="tc_b")
        req = _tool_request(_tool("cfdream_generate_image", "progress"))
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_tool_call(req, MagicMock(return_value=tm))

        content = captured[0]["content"]
        assert set(content.keys()) == {"message"}
        assert "truncated" in content["message"]

    def test_large_json_beyond_content_chars_still_parses_to_items(self):
        """A JSON array larger than max_content_chars but within the structured cap reaches the client as {"items": [...]}.

        Regression: web_search returns a pretty-printed JSON array that easily exceeds
        max_content_chars (4096); the decoupled parse gate must still structure it rather
        than degrading to a truncated {"message": ...}.
        """
        mw = _middleware(max_content_chars=4096)
        results = [{"title": f"r{i}", "url": f"https://e.com/{i}", "snippet": "x" * 1000} for i in range(5)]
        text = json.dumps(results, indent=2, ensure_ascii=False)
        assert len(text) > 4096  # the very condition that used to force degradation
        tm = _tool_msg(content=text, name="web_search", tool_call_id="tc_ws")
        req = _tool_request(_tool("web_search", "progress"))
        captured: list[dict] = []

        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_tool_call(req, MagicMock(return_value=tm))

        assert captured[0]["content"] == {"items": results}

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


# ---------------------------------------------------------------------------
# Artifact items projected from materials (D14: emit owns display + items)
# ---------------------------------------------------------------------------

class TestArtifactMaterialsProjection:
    """visibility==artifact + capture-rewritten content → project items from materials,
    stamp display=true (D14). Capture (MaterialsMiddleware) no longer builds items/display.
    """

    @staticmethod
    def _req(materials: dict, tool_name: str = "cfdream_generate_image", vis: str = "artifact"):
        return SimpleNamespace(tool=_tool(tool_name, vis), state={"materials": materials})

    @staticmethod
    def _captured_cmd(ids, *, new_materials=None, name="cfdream_generate_image"):
        from langgraph.types import Command
        body = {"task_id": "t-1", "cost_tokens": 100, "materials": ids}
        tm = _tool_msg(content=json.dumps(body), name=name, tool_call_id="tc_g", artifact=None)
        update: dict = {"messages": [tm]}
        if new_materials is not None:
            update["materials"] = new_materials
        return Command(update=update)

    def _run(self, mw, req, cmd):
        captured: list[dict] = []
        with patch("deerflow.agents.middlewares.message_stream_middleware.get_stream_writer") as mock_writer:
            mock_writer.return_value = captured.append
            mw.wrap_tool_call(req, MagicMock(return_value=cmd))
        return captured

    def test_projects_items_and_stamps_display(self):
        mw = _middleware()
        mid = "m_abc123"
        new_mats = {mid: {"id": mid, "kind": "image", "ref_type": "oss_path", "ref": "agent-artifacts/t1/images/a.png", "stable": True}}
        cmd = self._captured_cmd([mid], new_materials=new_mats)
        captured = self._run(mw, self._req({}), cmd)

        assert len(captured) == 1 and captured[0]["type"] == "artifact"
        assert captured[0]["items"] == [{"id": mid, "ref": "agent-artifacts/t1/images/a.png", "kind": "image", "stable": True}]
        # textual content rides alongside (url-stripped capture body)
        assert captured[0]["content"]["materials"] == [mid]
        # display stamped back onto the persisted material (for final_state projection parity)
        assert cmd.update["materials"][mid]["display"] is True

    def test_unstable_material_yields_no_items_suppressed(self):
        """Failed rehost (fail-open, stable=False) → no items, but status='success' (the
        generate call itself succeeded, only rehost degraded). Under the artifact-tool rule
        this is a non-terminal degraded success → suppressed (no artifact, no tool_result).
        The unstable material is still never delivered (I5)."""
        mw = _middleware()
        mid = "m_fail01"
        new_mats = {mid: {"id": mid, "kind": "image", "ref_type": "global_url", "ref": "https://cdn/expired.png", "stable": False}}
        cmd = self._captured_cmd([mid], new_materials=new_mats)
        captured = self._run(mw, self._req({}), cmd)

        assert captured == []
        # not stamped a deliverable (I5: unstable never display)
        assert "display" not in cmd.update["materials"][mid]

    def test_dedup_replay_id_only_in_prior_state(self):
        mw = _middleware()
        mid = "m_replay"
        prior = {mid: {"id": mid, "kind": "video", "ref_type": "oss_path", "ref": "agent-artifacts/t1/v.mp4", "stable": True}}
        # task_wait replay: dedup → Command carries NO materials, content still references the id
        cmd = self._captured_cmd([mid], new_materials=None, name="cfdream_task_wait")
        captured = self._run(mw, self._req(prior, tool_name="cfdream_task_wait"), cmd)

        assert captured[0]["type"] == "artifact"
        assert captured[0]["items"] == [{"id": mid, "ref": "agent-artifacts/t1/v.mp4", "kind": "video", "stable": True}]
        # partial display stamp added to update so it persists despite dedup
        assert cmd.update["materials"][mid] == {"id": mid, "display": True}

    def test_progress_visibility_no_artifact_no_stamp(self):
        mw = _middleware()
        mid = "m_search1"
        new_mats = {mid: {"id": mid, "kind": "image", "ref_type": "global_url", "ref": "https://img/x.png", "stable": True}}
        cmd = self._captured_cmd([mid], new_materials=new_mats, name="image_search")
        captured = self._run(mw, self._req({}, tool_name="image_search", vis="progress"), cmd)

        assert captured[0]["type"] == "tool_result"  # progress → not an artifact deliverable
        assert "display" not in cmd.update["materials"][mid]  # register material stays display-less
