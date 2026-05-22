"""Multi-level memory (MLM) update queue.

Mirrors the debounce + per-thread deduplication pattern of
:class:`~deerflow.agents.memory.queue.MemoryUpdateQueue`, but runs
async DB upserts instead of file-based updates.

Processing (per queued context):
  1. Extract user knowledge via LLM → upsert_user_scope (optimistic CAS)
  2. Extract project knowledge via LLM → upsert_project_scope (optimistic CAS)
  3. Extract agent knowledge via LLM → upsert_agent (single writer; no lock)

All three steps call the extractors defined in
:mod:`deerflow.agents.memory.extractor` and write via
:class:`~deerflow.persistence.memory.repository.MemoryRepository`.

``asyncio.run()`` is used inside the timer thread because ``threading.Timer``
fires outside any running event loop.  A fresh event loop per flush avoids
cross-loop connection reuse bugs.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from deerflow.agents.memory.extractor import extract_agent_knowledge, extract_project_knowledge, extract_user_knowledge
from deerflow.config.memory_config import get_memory_config
from deerflow.persistence.memory.repository import get_memory_repository

logger = logging.getLogger(__name__)

_DEBOUNCE_DEFAULT = 30.0


# ---------------------------------------------------------------------------
# Context
# ---------------------------------------------------------------------------


@dataclass
class MlmContext:
    """Data for one thread's MLM extraction run."""

    thread_id: str
    messages: list[Any]
    user_id: str | None = None
    agent_name: str | None = None
    project_id: str | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))


# ---------------------------------------------------------------------------
# Queue
# ---------------------------------------------------------------------------


class MlmUpdateQueue:
    """Debounced queue for multi-level memory extraction and persistence.

    ``add()`` / ``add_nowait()`` follow the same API as
    :class:`~deerflow.agents.memory.queue.MemoryUpdateQueue` so middleware
    code reads identically for both queues.
    """

    def __init__(self, debounce_seconds: float = _DEBOUNCE_DEFAULT) -> None:
        self._debounce = debounce_seconds
        self._queue: list[MlmContext] = []
        self._lock = threading.Lock()
        self._timer: threading.Timer | None = None
        self._processing = False

    # ── Public API ────────────────────────────────────────────────────────

    def add(
        self,
        thread_id: str,
        messages: list[Any],
        user_id: str | None = None,
        agent_name: str | None = None,
        project_id: str | None = None,
    ) -> None:
        """Enqueue a context and (re)start the debounce timer."""
        if not get_memory_config().mlm_enabled:
            return
        with self._lock:
            self._enqueue_locked(thread_id, messages, user_id, agent_name, project_id)
            self._reset_timer()
        logger.debug("MLM queued for thread=%s (queue size=%d)", thread_id, len(self._queue))

    def add_nowait(
        self,
        thread_id: str,
        messages: list[Any],
        user_id: str | None = None,
        agent_name: str | None = None,
        project_id: str | None = None,
    ) -> None:
        """Enqueue a context and flush immediately in a background thread."""
        if not get_memory_config().mlm_enabled:
            return
        with self._lock:
            self._enqueue_locked(thread_id, messages, user_id, agent_name, project_id)
            self._schedule_timer(0)
        logger.debug("MLM queued (nowait) for thread=%s", thread_id)

    def flush(self) -> None:
        """Cancel the timer and process the queue synchronously."""
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None
        self._process_queue()

    def flush_nowait(self) -> None:
        """Trigger processing in a background thread immediately."""
        with self._lock:
            self._schedule_timer(0)

    def clear(self) -> None:
        """Discard all pending contexts (for tests)."""
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None
            self._queue.clear()
            self._processing = False

    @property
    def pending_count(self) -> int:
        with self._lock:
            return len(self._queue)

    # ── Internal ──────────────────────────────────────────────────────────

    @staticmethod
    def _key(thread_id: str, user_id: str | None, agent_name: str | None, project_id: str | None) -> tuple:
        return (thread_id, user_id, agent_name, project_id)

    def _enqueue_locked(
        self,
        thread_id: str,
        messages: list[Any],
        user_id: str | None,
        agent_name: str | None,
        project_id: str | None,
    ) -> None:
        key = self._key(thread_id, user_id, agent_name, project_id)
        ctx = MlmContext(
            thread_id=thread_id,
            messages=messages,
            user_id=user_id,
            agent_name=agent_name,
            project_id=project_id,
        )
        self._queue = [c for c in self._queue if self._key(c.thread_id, c.user_id, c.agent_name, c.project_id) != key]
        self._queue.append(ctx)

    def _reset_timer(self) -> None:
        self._schedule_timer(self._debounce)

    def _schedule_timer(self, delay: float) -> None:
        if self._timer is not None:
            self._timer.cancel()
        t = threading.Timer(delay, self._process_queue)
        t.daemon = True
        t.start()
        self._timer = t

    def _process_queue(self) -> None:
        with self._lock:
            if self._processing:
                self._schedule_timer(0)
                return
            if not self._queue:
                return
            self._processing = True
            batch = self._queue.copy()
            self._queue.clear()
            self._timer = None

        logger.info("MLM: processing %d context(s)", len(batch))
        try:
            asyncio.run(self._process_batch(batch))
        except Exception:
            logger.exception("MLM: unexpected error during batch processing")
        finally:
            with self._lock:
                self._processing = False

    async def _process_batch(self, batch: list[MlmContext]) -> None:
        for ctx in batch:
            try:
                await self._process_one(ctx)
            except Exception:
                logger.exception("MLM: failed to process context for thread=%s", ctx.thread_id)

    async def _process_one(self, ctx: MlmContext) -> None:
        repo = get_memory_repository()
        if repo is None:
            logger.debug("MLM: no repository configured, skipping thread=%s", ctx.thread_id)
            return

        if ctx.user_id and ctx.agent_name:
            try:
                all_rows = await repo.load_user_scopes(ctx.user_id)
                existing = {r.scope_key: json.loads(r.facts) for r in all_rows}
                results = await extract_user_knowledge(ctx.messages, ctx.user_id, ctx.agent_name, existing)
                for result in results:
                    ok = await repo.upsert_user_scope(ctx.user_id, result.scope_key, result.facts, result.summary)
                    if not ok:
                        logger.warning("MLM: upsert_user_scope failed (user=%s scope=%s)", ctx.user_id, result.scope_key)
            except Exception:
                logger.exception("MLM: user extraction failed for user=%s", ctx.user_id)

        if ctx.project_id and ctx.agent_name:
            try:
                all_rows = await repo.load_project_scopes(ctx.project_id)
                existing = {r.scope_key: json.loads(r.facts) for r in all_rows}
                results = await extract_project_knowledge(ctx.messages, ctx.project_id, ctx.agent_name, ctx.user_id, existing)
                for result in results:
                    ok = await repo.upsert_project_scope(ctx.project_id, result.scope_key, result.facts, result.summary)
                    if not ok:
                        logger.warning("MLM: upsert_project_scope failed (proj=%s scope=%s)", ctx.project_id, result.scope_key)
            except Exception:
                logger.exception("MLM: project extraction failed for project=%s", ctx.project_id)

        if ctx.agent_name:
            try:
                agent_row = await repo.load_agent(ctx.agent_name)
                existing_facts = json.loads(agent_row.facts) if agent_row else []
                result = await extract_agent_knowledge(ctx.messages, ctx.agent_name, existing_facts)
                if result.facts or result.summary:
                    ok = await repo.upsert_agent(ctx.agent_name, result.facts, result.summary)
                    if not ok:
                        logger.warning("MLM: upsert_agent failed (agent=%s)", ctx.agent_name)
            except Exception:
                logger.exception("MLM: agent extraction failed for agent=%s", ctx.agent_name)


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------

_mlm_queue: MlmUpdateQueue | None = None
_mlm_lock = threading.Lock()


def get_mlm_queue() -> MlmUpdateQueue:
    global _mlm_queue
    with _mlm_lock:
        if _mlm_queue is None:
            _mlm_queue = MlmUpdateQueue()
        return _mlm_queue


def reset_mlm_queue() -> None:
    """Reset the singleton (for tests)."""
    global _mlm_queue
    with _mlm_lock:
        if _mlm_queue is not None:
            _mlm_queue.clear()
        _mlm_queue = None
