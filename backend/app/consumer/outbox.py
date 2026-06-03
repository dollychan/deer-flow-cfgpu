"""OutboxProducer — reliable terminal-result delivery loop (design §2.8/§9.3, E2).

The transactional outbox's *producer* half. Terminal close-out (finalize_run /
finalize_paused) and the cancel barrier write ``processed_messages`` rows with
``delivered=false``; AgentRunner's inline fast path marks them delivered when its
best-effort publish succeeds. Anything left undelivered — a crash before publish,
an MQ outage, or a cancel-barrier ``cancelled`` row that has no inline path at all —
is swept here and re-published with at-least-once semantics (the frontend dedups by
``(message_id, message_seq)``).

Boundary: read undelivered rows → ``MQStreamBridge.replay`` → mark delivered / back off
on failure. It owns no claim / finalize / queue logic. ``fetch_undelivered`` uses
``FOR UPDATE SKIP LOCKED`` so multiple instances do not contend on the same row (PG;
no-op on single-instance SQLite); the lock is released when the fetch session closes,
so at-least-once + dedup is the real guarantee, not the lock (design §9.3 / Phase B note).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging

from app.consumer.constants import ProcessedStatus
from app.consumer.models import ProcessedMessageRow
from app.consumer.run_registry import RunRegistry
from app.consumer.stream_bridge.mq import MQStreamBridge

logger = logging.getLogger(__name__)


class OutboxProducer:
    """Periodically re-publishes undelivered terminal results (design §9.3).

    Args:
        registry: Shared RunRegistry (outbox read/mark methods).
        bridge: MQStreamBridge used to replay the terminal envelope downlink.
        batch_size: Max undelivered rows fetched per pass.
        poll_interval: Seconds between passes (also the worst-case redelivery latency
            once a row's backoff window has elapsed).
        poison_threshold: delivery_attempts at/above which a row is logged as poison
            (never silently dropped — delivered=false rows are retained, §9.4).
    """

    def __init__(
        self,
        registry: RunRegistry,
        bridge: MQStreamBridge,
        *,
        batch_size: int = 20,
        poll_interval: float = 2.0,
        poison_threshold: int = 10,
    ) -> None:
        self._registry = registry
        self._bridge = bridge
        self._batch_size = batch_size
        self._poll_interval = poll_interval
        self._poison_threshold = poison_threshold

    # ── Main loop ──────────────────────────────────────────────────────────────

    async def run_loop(self, stop_event: asyncio.Event) -> None:
        """Drain the outbox once per ``poll_interval`` until ``stop_event`` is set."""
        while not stop_event.is_set():
            try:
                await self.drain_once()
            except Exception:
                logger.exception("Outbox drain pass failed; retrying next interval")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop_event.wait(), timeout=self._poll_interval)

    async def drain_once(self) -> int:
        """Publish one batch of undelivered terminal rows. Returns rows delivered.

        Per row: replay the terminal envelope, then mark delivered. On a publish error,
        bump the failure counter (exponential backoff via the registry) and continue —
        a poison row stays undelivered for the next eligible pass, only logged loudly.
        """
        rows = await self._registry.fetch_undelivered(self._batch_size)
        delivered = 0
        for row in rows:
            cache = self._effective_cache(row)
            # AgentRunner stashes the full echo inside result_cache; a bare cancel-barrier
            # row has none, so fall back to the minimal (message_id, thread_id) pair.
            echo = cache.get("echo") or {"message_id": row.message_id, "thread_id": row.thread_id}
            try:
                await self._bridge.replay(cache, echo=echo)
            except Exception as exc:
                attempts = await self._registry.bump_delivery_failure(row.message_id, str(exc))
                if attempts >= self._poison_threshold:
                    logger.error("Outbox poison message_id=%s attempts=%d: %s", row.message_id, attempts, exc)
                else:
                    logger.warning("Outbox publish failed message_id=%s attempt=%d: %s", row.message_id, attempts, exc)
                continue
            await self._registry.mark_delivered(row.message_id)
            delivered += 1
        return delivered

    # ── Envelope reconstruction ─────────────────────────────────────────────────

    @staticmethod
    def _effective_cache(row: ProcessedMessageRow) -> dict:
        """Shape the replay cache, mapping terminal status → downlink (§2.8/§7.3).

        Runner-written caches already carry ``status``/``error`` (plus any buffered events
        / final_state / tool_approval_required / echo), so they pass through untouched. The
        only bare row (result_cache=None) the outbox ever sees is a cancel-barrier
        ``cancelled``, shaped here into a ``result(cancelled)`` so the cancel-covered
        message still gets its terminal.
        """
        rc = dict(row.result_cache or {})
        if "error" not in rc and "status" not in rc:
            rc["status"] = "cancelled" if row.status == ProcessedStatus.CANCELLED.value else "success"
        return rc
