"""Tests for deerflow.agents.middlewares.mlm_middleware."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import AIMessage, HumanMessage

from deerflow.agents.middlewares.mlm_middleware import (
    MlmMiddleware,
    _already_injected,
    _MLM_INJECTED_KEY,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _human(text: str, *, injected: bool = False, name: str | None = None) -> HumanMessage:
    kwargs = {"hide_from_ui": True, _MLM_INJECTED_KEY: True} if injected else {}
    return HumanMessage(content=text, additional_kwargs=kwargs, name=name)


def _ai(text: str) -> AIMessage:
    return AIMessage(content=text)


def _runtime(*, user_id: str = "u1", project_id: str | None = None, thread_id: str = "t1") -> MagicMock:
    ctx = {"user_id": user_id, "thread_id": thread_id}
    if project_id:
        ctx["project_id"] = project_id
    r = MagicMock()
    r.context = ctx
    return r


def _state(*messages) -> dict:
    return {"messages": list(messages)}


def _enabled_config():
    return SimpleNamespace(mlm_enabled=True, debounce_seconds=30)


def _disabled_config():
    return SimpleNamespace(mlm_enabled=False, debounce_seconds=30)


# ---------------------------------------------------------------------------
# _already_injected
# ---------------------------------------------------------------------------


class TestAlreadyInjected:
    def test_returns_false_for_plain_messages(self):
        assert not _already_injected([_human("hi"), _ai("hello")])

    def test_returns_true_when_injected_message_present(self):
        assert _already_injected([_human("reminder", injected=True), _human("hi")])

    def test_returns_false_for_empty_list(self):
        assert not _already_injected([])


# ---------------------------------------------------------------------------
# before_agent (sync — always None)
# ---------------------------------------------------------------------------


class TestBeforeAgentSync:
    def test_always_returns_none(self):
        mw = MlmMiddleware(agent_name="director")
        result = mw.before_agent(_state(_human("hi")), _runtime())
        assert result is None


# ---------------------------------------------------------------------------
# abefore_agent (async injection)
# ---------------------------------------------------------------------------


class TestAbforeAgent:
    @pytest.mark.anyio
    async def test_returns_none_when_memory_disabled(self):
        mw = MlmMiddleware(agent_name="director")
        with patch("deerflow.agents.middlewares.mlm_middleware.get_memory_config", _disabled_config):
            result = await mw.abefore_agent(_state(_human("hi")), _runtime())
        assert result is None

    @pytest.mark.anyio
    async def test_returns_none_when_already_injected(self):
        mw = MlmMiddleware(agent_name="director")
        messages = [_human("existing reminder", injected=True), _human("user msg")]
        with patch("deerflow.agents.middlewares.mlm_middleware.get_memory_config", _enabled_config):
            result = await mw.abefore_agent(_state(*messages), _runtime())
        assert result is None

    @pytest.mark.anyio
    async def test_returns_none_when_no_human_messages(self):
        mw = MlmMiddleware(agent_name="director")
        with (
            patch("deerflow.agents.middlewares.mlm_middleware.get_memory_config", _enabled_config),
            patch("deerflow.agents.middlewares.mlm_middleware.build_injection", new=AsyncMock(return_value="some content")),
        ):
            result = await mw.abefore_agent(_state(_ai("hi")), _runtime())
        assert result is None

    @pytest.mark.anyio
    async def test_returns_none_when_injection_is_empty(self):
        mw = MlmMiddleware(agent_name="director")
        with (
            patch("deerflow.agents.middlewares.mlm_middleware.get_memory_config", _enabled_config),
            patch("deerflow.agents.middlewares.mlm_middleware.build_injection", new=AsyncMock(return_value="")),
        ):
            result = await mw.abefore_agent(_state(_human("hi")), _runtime())
        assert result is None

    @pytest.mark.anyio
    async def test_injects_reminder_before_first_human_message(self):
        mw = MlmMiddleware(agent_name="director")
        user_msg = _human("hello")
        with (
            patch("deerflow.agents.middlewares.mlm_middleware.get_memory_config", _enabled_config),
            patch("deerflow.agents.middlewares.mlm_middleware.build_injection", new=AsyncMock(return_value="## User Knowledge\n- fact A")),
            patch("deerflow.agents.middlewares.mlm_middleware.resolve_runtime_user_id", return_value="u1"),
        ):
            result = await mw.abefore_agent(_state(user_msg), _runtime())

        assert result is not None
        msgs = result["messages"]
        assert len(msgs) == 2
        reminder, user = msgs
        assert isinstance(reminder, HumanMessage)
        assert reminder.additional_kwargs.get(_MLM_INJECTED_KEY) is True
        assert "<system-reminder>" in reminder.content
        assert "fact A" in reminder.content
        assert isinstance(user, HumanMessage)
        assert user.content == "hello"

    @pytest.mark.anyio
    async def test_reminder_takes_original_message_id(self):
        mw = MlmMiddleware(agent_name="director")
        user_msg = HumanMessage(content="hi", id="msg-abc-123")
        with (
            patch("deerflow.agents.middlewares.mlm_middleware.get_memory_config", _enabled_config),
            patch("deerflow.agents.middlewares.mlm_middleware.build_injection", new=AsyncMock(return_value="memory")),
            patch("deerflow.agents.middlewares.mlm_middleware.resolve_runtime_user_id", return_value="u1"),
        ):
            result = await mw.abefore_agent(_state(user_msg), _runtime())

        reminder, user = result["messages"]
        assert reminder.id == "msg-abc-123"
        assert user.id == "msg-abc-123__user"

    @pytest.mark.anyio
    async def test_build_injection_called_with_correct_args(self):
        mw = MlmMiddleware(agent_name="director")
        mock_build = AsyncMock(return_value="content")
        runtime = _runtime(user_id="alice", project_id="proj-1")
        with (
            patch("deerflow.agents.middlewares.mlm_middleware.get_memory_config", _enabled_config),
            patch("deerflow.agents.middlewares.mlm_middleware.build_injection", mock_build),
            patch("deerflow.agents.middlewares.mlm_middleware.resolve_runtime_user_id", return_value="alice"),
        ):
            await mw.abefore_agent(_state(_human("hi")), runtime)

        mock_build.assert_called_once_with(user_id="alice", agent_name="director", project_id="proj-1")

    @pytest.mark.anyio
    async def test_skips_injected_messages_when_finding_first_human(self):
        """MLM-injected messages must not be treated as the injection target."""
        mw = MlmMiddleware(agent_name="director")
        # The first message is already an injected reminder (from DynamicContextMiddleware),
        # so MlmMiddleware should target the second real user message.
        first_human = _human("real message")
        messages = [_human("existing reminder", injected=True), first_human]
        with (
            patch("deerflow.agents.middlewares.mlm_middleware.get_memory_config", _enabled_config),
            patch("deerflow.agents.middlewares.mlm_middleware.build_injection", new=AsyncMock(return_value="memory")),
            patch("deerflow.agents.middlewares.mlm_middleware.resolve_runtime_user_id", return_value="u1"),
        ):
            result = await mw.abefore_agent(_state(*messages), _runtime())

        # The existing injected message has mlm_injected=True so _already_injected returns True
        assert result is None  # already injected → skip


# ---------------------------------------------------------------------------
# after_agent (sync extraction queuing)
# ---------------------------------------------------------------------------


class TestAfterAgent:
    def test_returns_none_when_memory_disabled(self):
        mw = MlmMiddleware(agent_name="director")
        with patch("deerflow.agents.middlewares.mlm_middleware.get_memory_config", _disabled_config):
            result = mw.after_agent(_state(_human("hi"), _ai("ok")), _runtime())
        assert result is None

    def test_returns_none_when_no_thread_id(self):
        mw = MlmMiddleware(agent_name="director")
        runtime = MagicMock()
        runtime.context = {}  # no thread_id
        with (
            patch("deerflow.agents.middlewares.mlm_middleware.get_memory_config", _enabled_config),
            patch("deerflow.agents.middlewares.mlm_middleware.get_config", return_value={"configurable": {}}),
        ):
            result = mw.after_agent(_state(_human("hi"), _ai("ok")), runtime)
        assert result is None

    def test_enqueues_when_valid_conversation(self):
        mw = MlmMiddleware(agent_name="director")
        mock_queue = MagicMock()
        with (
            patch("deerflow.agents.middlewares.mlm_middleware.get_memory_config", _enabled_config),
            patch("deerflow.agents.middlewares.mlm_middleware.get_mlm_queue", return_value=mock_queue),
            patch("deerflow.agents.middlewares.mlm_middleware.resolve_runtime_user_id", return_value="u1"),
        ):
            result = mw.after_agent(_state(_human("hi"), _ai("ok")), _runtime())

        assert result is None
        mock_queue.add.assert_called_once()
        call_kwargs = mock_queue.add.call_args.kwargs
        assert call_kwargs["thread_id"] == "t1"
        assert call_kwargs["agent_name"] == "director"
        assert call_kwargs["user_id"] == "u1"

    def test_skips_when_no_user_or_ai_messages_after_filtering(self):
        mw = MlmMiddleware(agent_name="director")
        mock_queue = MagicMock()
        with (
            patch("deerflow.agents.middlewares.mlm_middleware.get_memory_config", _enabled_config),
            patch("deerflow.agents.middlewares.mlm_middleware.get_mlm_queue", return_value=mock_queue),
        ):
            # Only AI messages, no human → filter returns nothing useful
            result = mw.after_agent(_state(_ai("unprompted")), _runtime())

        assert result is None
        mock_queue.add.assert_not_called()
