"""Multi-level memory (MLM) middleware.

Lifecycle:
  - ``abefore_agent`` (first turn only): loads user / agent / project memory
    from the DB, wraps the result in a ``<system-reminder>`` HumanMessage,
    and prepends it to the conversation using the ID-swap technique so
    LangGraph's ``add_messages`` replaces it in-place.

  - ``after_agent`` (every turn): enqueues the filtered conversation into
    :class:`~deerflow.agents.memory.mlm_queue.MlmUpdateQueue` for
    background LLM extraction and DB upsert.

The middleware is a no-op when no DB repository is configured (persistence
backend is ``memory``) or when the injection produces no content.
"""

from __future__ import annotations

import logging
import uuid
from typing import override

from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import HumanMessage
from langgraph.config import get_config
from langgraph.graph.message import REMOVE_ALL_MESSAGES, RemoveMessage
from langgraph.runtime import Runtime

from deerflow.agents.memory.injector import build_injection
from deerflow.agents.memory.message_processing import filter_messages_for_memory
from deerflow.agents.memory.mlm_queue import get_mlm_queue
from deerflow.config.memory_config import get_memory_config
from deerflow.runtime.user_context import resolve_runtime_user_id

logger = logging.getLogger(__name__)

_MLM_INJECTED_KEY = "mlm_injected"
_SUMMARY_MESSAGE_NAME = "summary"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_mlm_reminder(message: object) -> bool:
    return isinstance(message, HumanMessage) and bool(message.additional_kwargs.get(_MLM_INJECTED_KEY))


def _already_injected(messages: list) -> bool:
    return any(_is_mlm_reminder(m) for m in messages)


def _is_user_message(message: object) -> bool:
    return (
        isinstance(message, HumanMessage)
        and not _is_mlm_reminder(message)
        and getattr(message, "name", None) != _SUMMARY_MESSAGE_NAME
    )


def _make_reminder_message(original: HumanMessage, reminder_content: str) -> tuple[HumanMessage, HumanMessage]:
    """Return (reminder_msg, user_msg) using the ID-swap technique.

    The reminder takes the original message's ID so ``add_messages`` replaces it
    in-place; the user message gets a derived ``{id}__user`` ID appended right after.
    """
    stable_id = original.id or str(uuid.uuid4())
    reminder_msg = HumanMessage(
        content=reminder_content,
        id=stable_id,
        additional_kwargs={"hide_from_ui": True, _MLM_INJECTED_KEY: True},
    )
    user_msg = HumanMessage(
        content=original.content,
        id=f"{stable_id}__user",
        name=original.name,
        additional_kwargs=original.additional_kwargs,
    )
    return reminder_msg, user_msg


def _get_thread_id(runtime: Runtime) -> str | None:
    thread_id = runtime.context.get("thread_id") if runtime.context else None
    if thread_id is None:
        cfg = get_config()
        thread_id = cfg.get("configurable", {}).get("thread_id")
    return thread_id


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------


class MlmMiddleware(AgentMiddleware):
    """Inject multi-level DB memory once per conversation and queue extraction after each turn."""

    def __init__(self, agent_name: str | None = None) -> None:
        super().__init__()
        self._agent_name = agent_name

    # ── Injection ─────────────────────────────────────────────────────────

    @override
    def before_agent(self, state, runtime: Runtime) -> dict | None:
        return None  # injection requires async; handled by abefore_agent

    @override
    async def abefore_agent(self, state, runtime: Runtime) -> dict | None:
        if not get_memory_config().mlm_enabled:
            return None

        messages = list(state.get("messages", []))
        if _already_injected(messages):
            return None

        first_idx = next((i for i, m in enumerate(messages) if _is_user_message(m)), None)
        if first_idx is None:
            return None

        context = runtime.context or {}
        user_id = resolve_runtime_user_id(runtime)
        project_id = context.get("project_id")

        injection_text = await build_injection(
            user_id=user_id if user_id else None,
            agent_name=self._agent_name,
            project_id=project_id,
        )
        if not injection_text.strip():
            return None

        reminder_content = f"<system-reminder>\n{injection_text.strip()}\n</system-reminder>"
        reminder_msg, user_msg = _make_reminder_message(messages[first_idx], reminder_content)
        logger.info(
            "MlmMiddleware: injecting memory (%d chars) before first HumanMessage id=%r",
            len(reminder_content),
            messages[first_idx].id,
        )
        later_messages = messages[first_idx + 1:]
        if not later_messages:
            return {"messages": [reminder_msg, user_msg]}
        # REMOVE_ALL_MESSAGES discards the existing list and uses right[1:] as the full
        # result, giving us complete control over message order.  The broken alternative
        # (RemoveMessage(id) + re-append) does NOT work: add_messages sees the id still
        # in merged_by_id and updates it in place rather than re-appending to end.
        correct_order = messages[:first_idx] + [reminder_msg, user_msg] + later_messages
        return {"messages": [RemoveMessage(id=REMOVE_ALL_MESSAGES)] + correct_order}

    # ── Extraction ────────────────────────────────────────────────────────

    @override
    def after_agent(self, state, runtime: Runtime) -> dict | None:
        if not get_memory_config().mlm_enabled:
            return None

        thread_id = _get_thread_id(runtime)
        if not thread_id:
            return None

        messages = state.get("messages", [])
        filtered = filter_messages_for_memory(messages)
        has_human = any(getattr(m, "type", None) == "human" for m in filtered)
        has_ai = any(getattr(m, "type", None) == "ai" for m in filtered)
        if not has_human or not has_ai:
            return None

        context = runtime.context or {}
        user_id = resolve_runtime_user_id(runtime)
        project_id = context.get("project_id")

        get_mlm_queue().add(
            thread_id=thread_id,
            messages=filtered,
            user_id=user_id if user_id else None,
            agent_name=self._agent_name,
            project_id=project_id,
        )
        return None
