"""Tests for HumanApprovalMiddleware (after_model hook + state-based resume)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from langchain_core.messages import AIMessage, ToolMessage

from deerflow.agents.middlewares.human_approval_middleware import HumanApprovalMiddleware
from deerflow.agents.thread_state import ThreadState, merge_tool_approvals


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_middleware(patterns=None):
    return HumanApprovalMiddleware(patterns or {"cfgpu__generate_*", "cfgpu__generate_image"})


def _make_state(messages: list, tool_approvals: dict | None = None) -> dict:
    return {
        "messages": messages,
        "tool_approvals": tool_approvals or {},
        "artifacts": [],
        "viewed_images": {},
    }


def _ai_msg(tool_calls: list[dict]) -> AIMessage:
    """Create an AIMessage with typed tool_calls."""
    from langchain_core.messages.tool import ToolCall
    calls = [ToolCall(id=tc["id"], name=tc["name"], args=tc["args"]) for tc in tool_calls]
    return AIMessage(content="", tool_calls=calls)


# ---------------------------------------------------------------------------
# merge_tool_approvals reducer
# ---------------------------------------------------------------------------

class TestMergeToolApprovals:
    def test_none_existing(self):
        assert merge_tool_approvals(None, {"a": {"status": "approved"}}) == {"a": {"status": "approved"}}

    def test_none_new(self):
        assert merge_tool_approvals({"a": 1}, None) == {"a": 1}

    def test_merges(self):
        result = merge_tool_approvals({"a": 1}, {"b": 2})
        assert result == {"a": 1, "b": 2}

    def test_new_overrides(self):
        result = merge_tool_approvals({"a": "old"}, {"a": "new"})
        assert result == {"a": "new"}


# ---------------------------------------------------------------------------
# _needs_approval
# ---------------------------------------------------------------------------

class TestNeedsApproval:
    def test_exact_match(self):
        m = _make_middleware({"cfgpu__generate_image"})
        assert m._needs_approval("cfgpu__generate_image")
        assert not m._needs_approval("cfgpu__generate_video")

    def test_glob_match(self):
        m = _make_middleware({"cfgpu__generate_*"})
        assert m._needs_approval("cfgpu__generate_image")
        assert m._needs_approval("cfgpu__generate_video")
        assert not m._needs_approval("search_web")


# ---------------------------------------------------------------------------
# after_model — no pending approvals
# ---------------------------------------------------------------------------

class TestAfterModelNoPending:
    def test_no_tool_calls(self):
        m = _make_middleware()
        state = _make_state([AIMessage(content="hello")])
        assert m.after_model(state, MagicMock()) is None

    def test_no_matching_tools(self):
        m = _make_middleware()
        msg = _ai_msg([{"id": "t1", "name": "search_web", "args": {"query": "x"}}])
        state = _make_state([msg])
        assert m.after_model(state, MagicMock()) is None

    def test_no_messages(self):
        m = _make_middleware()
        assert m.after_model(_make_state([]), MagicMock()) is None


# ---------------------------------------------------------------------------
# after_model — first call (interrupt path)
# ---------------------------------------------------------------------------

class TestAfterModelFirstCall:
    def test_emits_sse_and_interrupts(self):
        m = _make_middleware()
        msg = _ai_msg([{"id": "tc1", "name": "cfgpu__generate_image", "args": {"prompt": "cat"}}])
        state = _make_state([msg])

        captured_sse = []

        class FakeWriter:
            def __call__(self, data):
                captured_sse.append(data)

        def fake_interrupt(value):
            raise Exception("GraphInterrupt")  # simulate interrupt raising

        with (
            patch("deerflow.agents.middlewares.human_approval_middleware.get_stream_writer", return_value=FakeWriter()),
            patch("deerflow.agents.middlewares.human_approval_middleware.interrupt", side_effect=fake_interrupt),
        ):
            with pytest.raises(Exception, match="GraphInterrupt"):
                m.after_model(state, MagicMock())

        assert len(captured_sse) == 1
        event = captured_sse[0]
        assert event["type"] == "tool_approval_required"
        assert len(event["tool_calls"]) == 1
        assert event["tool_calls"][0]["id"] == "tc1"
        assert event["tool_calls"][0]["name"] == "cfgpu__generate_image"

    def test_batches_multiple_pending_tools(self):
        m = _make_middleware()
        msg = _ai_msg([
            {"id": "tc1", "name": "cfgpu__generate_image", "args": {"prompt": "cat"}},
            {"id": "tc2", "name": "cfgpu__generate_video", "args": {"prompt": "dog"}},
            {"id": "tc3", "name": "search_web", "args": {"query": "x"}},
        ])
        state = _make_state([msg])

        captured_sse = []

        with (
            patch("deerflow.agents.middlewares.human_approval_middleware.get_stream_writer", return_value=lambda d: captured_sse.append(d)),
            patch("deerflow.agents.middlewares.human_approval_middleware.interrupt", side_effect=Exception("interrupt")),
        ):
            with pytest.raises(Exception):
                m.after_model(state, MagicMock())

        assert len(captured_sse) == 1
        # Only the two generate_* tools are in the approval request
        pending_names = {tc["name"] for tc in captured_sse[0]["tool_calls"]}
        assert pending_names == {"cfgpu__generate_image", "cfgpu__generate_video"}
        # search_web is not in the approval request
        assert all(tc["name"] != "search_web" for tc in captured_sse[0]["tool_calls"])


# ---------------------------------------------------------------------------
# after_model — resume path (state-based, no SSE, no interrupt)
# ---------------------------------------------------------------------------

class TestAfterModelResumePath:
    def test_approved_updates_args(self):
        m = _make_middleware()
        original_args = {"prompt": "cat", "width": 512}
        new_args = {"prompt": "fluffy cat", "width": 1024}
        msg = _ai_msg([{"id": "tc1", "name": "cfgpu__generate_image", "args": original_args}])
        state = _make_state(
            [msg],
            tool_approvals={"tc1": {"status": "approved", "args": new_args}},
        )

        writer_called = []
        interrupt_called = []

        with (
            patch("deerflow.agents.middlewares.human_approval_middleware.get_stream_writer", return_value=lambda d: writer_called.append(d)),
            patch("deerflow.agents.middlewares.human_approval_middleware.interrupt", side_effect=lambda v: interrupt_called.append(v)),
        ):
            result = m.after_model(state, MagicMock())

        # SSE must NOT be emitted on resume path
        assert writer_called == []
        # interrupt must NOT be called on resume path
        assert interrupt_called == []

        assert result is not None
        new_msg = next(m for m in result["messages"] if isinstance(m, AIMessage))
        assert new_msg.tool_calls[0]["args"] == new_args

    def test_rejected_retains_tool_call_adds_error_message(self):
        m = _make_middleware()
        msg = _ai_msg([{"id": "tc1", "name": "cfgpu__generate_image", "args": {"prompt": "cat"}}])
        state = _make_state(
            [msg],
            tool_approvals={"tc1": {"status": "rejected", "reason": "Too expensive"}},
        )

        with (
            patch("deerflow.agents.middlewares.human_approval_middleware.get_stream_writer", return_value=lambda d: None),
            patch("deerflow.agents.middlewares.human_approval_middleware.interrupt", side_effect=AssertionError("should not call")),
        ):
            result = m.after_model(state, MagicMock())

        assert result is not None
        messages = result["messages"]
        ai_msg = next(m for m in messages if isinstance(m, AIMessage))
        tool_msgs = [m for m in messages if isinstance(m, ToolMessage)]

        # Rejected tool call is RETAINED so its ToolMessage has a matching tool_call_id
        # (stripping it would orphan the ToolMessage and break history validation).
        assert [tc["id"] for tc in ai_msg.tool_calls] == ["tc1"]
        # Artificial error ToolMessage injected
        assert len(tool_msgs) == 1
        assert tool_msgs[0].tool_call_id == "tc1"
        assert tool_msgs[0].status == "error"
        assert "Too expensive" in tool_msgs[0].content

    def test_rejected_message_is_self_describing(self):
        """Rejected ToolMessage must echo tool name + rejected args so the model
        can attribute it by content, not by positional guess.

        Regression for partial-approval mis-attribution: with an anonymous
        {"status":"cancelled"} blob, the model joined results to calls by
        position and reported a rejected image as 'succeeded'. The rejection
        content must now carry the prompt and an explicit not-executed signal.
        """
        import json

        m = _make_middleware()
        msg = _ai_msg([{"id": "tc1", "name": "cfgpu__generate_image", "args": {"prompt": "一只可爱的狗狗"}}])
        state = _make_state(
            [msg],
            tool_approvals={"tc1": {"status": "rejected"}},
        )

        with (
            patch("deerflow.agents.middlewares.human_approval_middleware.get_stream_writer", return_value=lambda d: None),
            patch("deerflow.agents.middlewares.human_approval_middleware.interrupt", side_effect=AssertionError("should not call")),
        ):
            result = m.after_model(state, MagicMock())

        tool_msg = next(m for m in result["messages"] if isinstance(m, ToolMessage))
        payload = json.loads(tool_msg.content)

        assert payload["status"] == "rejected"
        assert payload["executed"] is False
        assert payload["tool"] == "cfgpu__generate_image"
        # The identifying arg (prompt) is echoed back so the model knows WHICH call
        assert payload["rejected_args"]["prompt"] == "一只可爱的狗狗"
        # The subject also appears in the human-readable message
        assert "一只可爱的狗狗" in payload["message"]
        # And the not-executed signal is explicit (defeats "succeeded" mis-read)
        assert "未执行" in payload["message"]

    def test_mixed_batch_approved_and_rejected(self):
        m = _make_middleware({"cfgpu__generate_*"})
        msg = _ai_msg([
            {"id": "tc1", "name": "cfgpu__generate_image", "args": {"prompt": "cat"}},
            {"id": "tc2", "name": "cfgpu__generate_video", "args": {"prompt": "dog"}},
            {"id": "tc3", "name": "search_web", "args": {"query": "x"}},
        ])
        state = _make_state(
            [msg],
            tool_approvals={
                "tc1": {"status": "approved", "args": {"prompt": "fluffy cat"}},
                "tc2": {"status": "rejected", "reason": "No budget"},
            },
        )

        with (
            patch("deerflow.agents.middlewares.human_approval_middleware.get_stream_writer", return_value=lambda d: None),
            patch("deerflow.agents.middlewares.human_approval_middleware.interrupt", side_effect=AssertionError("no interrupt on resume")),
        ):
            result = m.after_model(state, MagicMock())

        messages = result["messages"]
        ai_msg = next(m for m in messages if isinstance(m, AIMessage))
        tool_msgs = [m for m in messages if isinstance(m, ToolMessage)]

        # tc1 approved with new args
        approved = next(tc for tc in ai_msg.tool_calls if tc["name"] == "cfgpu__generate_image")
        assert approved["args"]["prompt"] == "fluffy cat"
        # tc2 rejected — RETAINED in tool_calls so its error ToolMessage stays paired
        assert any(tc["name"] == "cfgpu__generate_video" for tc in ai_msg.tool_calls)
        # tc3 (non-approval) passes through
        assert any(tc["name"] == "search_web" for tc in ai_msg.tool_calls)
        # One error ToolMessage for tc2
        assert len(tool_msgs) == 1
        assert tool_msgs[0].tool_call_id == "tc2"

    def test_partial_state_does_not_skip_interrupt(self):
        """If only some decisions are in state (partial), interrupt is still called."""
        m = _make_middleware({"cfgpu__generate_*"})
        msg = _ai_msg([
            {"id": "tc1", "name": "cfgpu__generate_image", "args": {}},
            {"id": "tc2", "name": "cfgpu__generate_video", "args": {}},
        ])
        # Only tc1 in state, tc2 missing → should still interrupt
        state = _make_state([msg], tool_approvals={"tc1": {"status": "approved", "args": {}}})

        interrupt_called = []

        with (
            patch("deerflow.agents.middlewares.human_approval_middleware.get_stream_writer", return_value=lambda d: None),
            patch("deerflow.agents.middlewares.human_approval_middleware.interrupt", side_effect=lambda v: (_ for _ in ()).throw(Exception("interrupted"))),
        ):
            with pytest.raises(Exception, match="interrupted"):
                m.after_model(state, MagicMock())

    def test_partial_resume_reemits_only_undecided(self):
        """Partial resume re-requests only the still-undecided tool calls.

        tc1 already decided in state → must NOT reappear in the SSE/interrupt
        payload; only tc2 (undecided) should be re-requested.
        """
        m = _make_middleware({"cfgpu__generate_*"})
        msg = _ai_msg([
            {"id": "tc1", "name": "cfgpu__generate_image", "args": {}},
            {"id": "tc2", "name": "cfgpu__generate_video", "args": {}},
        ])
        state = _make_state([msg], tool_approvals={"tc1": {"status": "approved", "args": {}}})

        captured_sse = []
        captured_interrupt = []

        def fake_interrupt(value):
            captured_interrupt.append(value)
            raise Exception("interrupted")

        with (
            patch("deerflow.agents.middlewares.human_approval_middleware.get_stream_writer", return_value=lambda d: captured_sse.append(d)),
            patch("deerflow.agents.middlewares.human_approval_middleware.interrupt", side_effect=fake_interrupt),
        ):
            with pytest.raises(Exception, match="interrupted"):
                m.after_model(state, MagicMock())

        # SSE re-emitted with ONLY the undecided tc2
        assert len(captured_sse) == 1
        sse_ids = {tc["id"] for tc in captured_sse[0]["tool_calls"]}
        assert sse_ids == {"tc2"}
        # interrupt payload also carries only the undecided tc2
        assert len(captured_interrupt) == 1
        interrupt_ids = {tc["id"] for tc in captured_interrupt[0]["tool_calls"]}
        assert interrupt_ids == {"tc2"}


# ---------------------------------------------------------------------------
# after_model — fallback resume via interrupt() return value
# ---------------------------------------------------------------------------

class TestAfterModelFallbackResume:
    def test_fallback_approved(self):
        m = _make_middleware()
        new_args = {"prompt": "updated"}
        msg = _ai_msg([{"id": "tc1", "name": "cfgpu__generate_image", "args": {"prompt": "cat"}}])
        state = _make_state([msg])  # No tool_approvals in state

        resume_value = {"approved": [{"id": "tc1", "args": new_args}], "rejected": []}

        with (
            patch("deerflow.agents.middlewares.human_approval_middleware.get_stream_writer", return_value=lambda d: None),
            patch("deerflow.agents.middlewares.human_approval_middleware.interrupt", return_value=resume_value),
        ):
            result = m.after_model(state, MagicMock())

        ai_msg = next(m for m in result["messages"] if isinstance(m, AIMessage))
        assert ai_msg.tool_calls[0]["args"] == new_args

    def test_fallback_rejected(self):
        m = _make_middleware()
        msg = _ai_msg([{"id": "tc1", "name": "cfgpu__generate_image", "args": {}}])
        state = _make_state([msg])

        resume_value = {"approved": [], "rejected": ["tc1"]}

        with (
            patch("deerflow.agents.middlewares.human_approval_middleware.get_stream_writer", return_value=lambda d: None),
            patch("deerflow.agents.middlewares.human_approval_middleware.interrupt", return_value=resume_value),
        ):
            result = m.after_model(state, MagicMock())

        ai_msg = next(m for m in result["messages"] if isinstance(m, AIMessage))
        # Rejected call retained to keep ToolMessage pairing valid
        assert [tc["id"] for tc in ai_msg.tool_calls] == ["tc1"]
        tool_msgs = [m for m in result["messages"] if isinstance(m, ToolMessage)]
        assert len(tool_msgs) == 1
        assert tool_msgs[0].tool_call_id == "tc1"
