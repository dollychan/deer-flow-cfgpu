"""Hooks fired before summarization removes messages from state."""

from __future__ import annotations

from deerflow.agents.memory.message_processing import detect_correction, detect_reinforcement, filter_messages_for_memory
from deerflow.agents.memory.mlm_queue import get_mlm_queue
from deerflow.agents.memory.queue import get_memory_queue
from deerflow.agents.middlewares.summarization_middleware import SummarizationEvent
from deerflow.config.memory_config import get_memory_config
from deerflow.runtime.user_context import resolve_runtime_user_id


def mlm_flush_hook(event: SummarizationEvent) -> None:
    """Flush messages about to be summarized into the MLM queue.

    Mirrors :func:`memory_flush_hook` but targets the multi-level memory
    queue instead of the legacy file-based queue.  Registered alongside
    ``memory_flush_hook`` in ``_build_hooks()`` of the lead agent.
    """
    if not get_memory_config().mlm_enabled or not event.thread_id:
        return

    filtered = filter_messages_for_memory(list(event.messages_to_summarize))
    user_messages = [m for m in filtered if getattr(m, "type", None) == "human"]
    ai_messages = [m for m in filtered if getattr(m, "type", None) == "ai"]
    if not user_messages or not ai_messages:
        return

    user_id = resolve_runtime_user_id(event.runtime)
    context = getattr(event.runtime, "context", {}) or {}
    project_id = context.get("project_id")

    get_mlm_queue().add_nowait(
        thread_id=event.thread_id,
        messages=filtered,
        user_id=user_id if user_id else None,
        agent_name=event.agent_name,
        project_id=project_id,
    )


def memory_flush_hook(event: SummarizationEvent) -> None:
    """Flush messages about to be summarized into the memory queue."""
    if not get_memory_config().enabled or not event.thread_id:
        return

    filtered_messages = filter_messages_for_memory(list(event.messages_to_summarize))
    user_messages = [message for message in filtered_messages if getattr(message, "type", None) == "human"]
    assistant_messages = [message for message in filtered_messages if getattr(message, "type", None) == "ai"]
    if not user_messages or not assistant_messages:
        return

    correction_detected = detect_correction(filtered_messages)
    reinforcement_detected = not correction_detected and detect_reinforcement(filtered_messages)
    user_id = resolve_runtime_user_id(event.runtime)
    queue = get_memory_queue()
    queue.add_nowait(
        thread_id=event.thread_id,
        messages=filtered_messages,
        agent_name=event.agent_name,
        user_id=user_id,
        correction_detected=correction_detected,
        reinforcement_detected=reinforcement_detected,
    )
