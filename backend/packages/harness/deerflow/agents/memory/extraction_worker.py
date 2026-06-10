"""DB-backed multi-level memory (MLM) extraction worker.

Replaces the in-process ``MlmUpdateQueue`` (Timer debounce). The enqueue point
(``MlmMiddleware.aafter_agent``) only writes a row into
``memory_extraction_queue`` keyed by ``thread_id``; the actual LLM extraction
runs here, in a per-instance background loop that survives the slot-driven
scheduler (successive turns of one thread may land on different consumer
instances) and instance crashes.

Two layers (design §八):
  - :func:`process_extraction` — single-task logic (pure, testable): read the
    thread's *latest* checkpoint, filter to memory-relevant messages, run the
    three scope extractors, and upsert via the optimistic-locking repository.
    Stores no messages in the queue row — the checkpoint is the authoritative
    source, which also dodges the oversized-message-row pitfall (BUG-002).
  - :func:`run_extraction_loop` — per-instance background coroutine, structurally
    identical to the scheduler loop: claim → process → delete; on failure
    ``bump_attempt`` (release-and-retry, or dead-letter at the attempt limit).

The bg-task wiring (start as a named handle, cancel on shutdown) lives in the
app layer (consumer ``__main__``, Phase G6) so the harness import boundary is
preserved. The checkpointer is injected as a parameter — the harness has no
checkpointer singleton.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from deerflow.agents.memory.extractor import extract_agent_knowledge, extract_project_knowledge, extract_user_knowledge
from deerflow.agents.memory.message_processing import filter_messages_for_memory
from deerflow.config.mlm_config import get_mlm_config
from deerflow.persistence.memory.model import MemoryExtractionRow
from deerflow.persistence.memory.repository import get_memory_repository

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Checkpoint readback
# ---------------------------------------------------------------------------


async def load_latest_thread_messages(thread_id: str, checkpointer: Any) -> list[Any]:
    """Return the messages from a thread's latest checkpoint, or ``[]``.

    The worker stores no messages in the queue row; it reads the authoritative
    conversation state from the LangGraph checkpointer instead (design §八).
    Reading a slightly later turn (more context) is harmless for extraction; the
    ``not_before`` debounce defers extraction until the thread settles anyway.

    Returns ``[]`` when no checkpointer is configured or the thread has no
    checkpoint yet — :func:`process_extraction` then no-ops gracefully.
    """
    if checkpointer is None:
        return []
    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    tup = await checkpointer.aget_tuple(config)
    if tup is None or tup.checkpoint is None:
        return []
    return tup.checkpoint.get("channel_values", {}).get("messages", []) or []


# ---------------------------------------------------------------------------
# Single-task processing (pure, testable)
# ---------------------------------------------------------------------------


async def process_extraction(row: MemoryExtractionRow, checkpointer: Any) -> None:
    """Run the three-scope extraction for one queue row.

    Reads the thread's latest checkpoint, filters to user + final-AI turns, then
    extracts and upserts user / project / agent knowledge. Each scope is gated on
    the dims present on the row (a degraded-context turn may lack a resolved
    user/project), so partial scopes still make progress.

    Raises on unexpected repository / extractor failures so the caller
    (:func:`run_extraction_loop`) can record the failed attempt; individual
    extractor functions already swallow their own LLM/parse errors and return
    empty results, so a raise here means an infrastructural fault.
    """
    repo = get_memory_repository()
    if repo is None:  # backend=memory → nothing to write
        return

    messages = filter_messages_for_memory(await load_latest_thread_messages(row.thread_id, checkpointer))
    if not messages:
        logger.debug("MLM extraction: no messages for thread=%s; skipping", row.thread_id)
        return

    if row.user_id and row.agent_name:
        existing = {r.scope_key: json.loads(r.facts) for r in await repo.load_user_scopes(row.user_id)}
        for res in await extract_user_knowledge(messages, row.user_id, row.agent_name, existing):
            await repo.upsert_user_scope(row.user_id, res.scope_key, res.facts, res.summary)

    if row.project_id and row.agent_name:
        existing = {r.scope_key: json.loads(r.facts) for r in await repo.load_project_scopes(row.project_id)}
        for res in await extract_project_knowledge(messages, row.project_id, row.agent_name, row.user_id, existing):
            await repo.upsert_project_scope(row.project_id, res.scope_key, res.facts, res.summary)

    if row.agent_name:
        agent_row = await repo.load_agent(row.agent_name)
        existing_facts = json.loads(agent_row.facts) if agent_row else []
        res = await extract_agent_knowledge(messages, row.agent_name, existing_facts)
        if res.facts or res.summary:
            await repo.upsert_agent(row.agent_name, res.facts, res.summary)


# ---------------------------------------------------------------------------
# Per-instance background loop
# ---------------------------------------------------------------------------


async def _wait(stop_event: asyncio.Event, timeout: float) -> None:
    """Sleep up to *timeout*, returning early if *stop_event* is set."""
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=timeout)
    except TimeoutError:
        pass


async def run_extraction_loop(
    checkpointer: Any,
    instance_id: str,
    stop_event: asyncio.Event,
    *,
    idle_sleep: float = 2.0,
    stale_after: int = 300,
    max_attempts: int = 3,
) -> None:
    """Per-instance MLM extraction loop (structurally like the scheduler loop).

    Claims the earliest due task (``FOR UPDATE SKIP LOCKED`` on PG; sequential on
    SQLite), processes it, and deletes the row on success. On failure it records
    the attempt: the row is released for retry after the stale window, or
    dead-lettered once ``max_attempts`` is reached.

    Does not start when the persistence backend is ``memory`` (no queue table).
    Backs off ``idle_sleep`` when no task is due, and skips work while MLM is
    disabled at runtime — both interruptible promptly on ``stop_event``.
    """
    repo = get_memory_repository()
    if repo is None:  # backend=memory → loop never runs
        logger.info("MLM extraction loop: no DB repository; not starting (backend=memory)")
        return

    logger.info("MLM extraction loop started (instance=%s)", instance_id)
    while not stop_event.is_set():
        if not get_mlm_config().enabled:
            await _wait(stop_event, idle_sleep)
            continue

        try:
            row = await repo.claim_extraction(instance_id, stale_after_seconds=stale_after)
        except Exception:
            logger.exception("MLM extraction loop: claim failed; backing off")
            await _wait(stop_event, idle_sleep)
            continue

        if row is None:  # nothing due — back off
            await _wait(stop_event, idle_sleep)
            continue

        thread_id = row.thread_id
        try:
            await process_extraction(row, checkpointer)
            await repo.delete_extraction(thread_id)
            logger.debug("MLM extraction done thread=%s", thread_id)
        except Exception:
            logger.exception("MLM extraction failed thread=%s", thread_id)
            try:
                dead = await repo.bump_attempt(thread_id, max_attempts=max_attempts)
                if dead:
                    logger.warning("MLM extraction dead-lettered thread=%s (max_attempts=%d)", thread_id, max_attempts)
            except Exception:
                logger.exception("MLM extraction: bump_attempt failed thread=%s", thread_id)

    logger.info("MLM extraction loop stopped (instance=%s)", instance_id)
