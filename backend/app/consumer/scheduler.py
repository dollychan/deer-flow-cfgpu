"""Scheduler — the v2 claim + dispatch layer (design §2.5/§6.1/§6.3, D1).

Owns the per-instance run slots and the wake-up loop. The ingest layer
(TaskConsumer) only lands rows + ACKs + pokes; the Scheduler is the *sole*
caller of ``RunRegistry.claim_next_runnable`` and the only place that fires
``AgentRunner.run``.

Wake-up is layered by scope (§6.1):
  - local ``asyncio.Event`` — ``poke()`` after ingest enqueue/fold commit and
    after a run releases its slot (per-instance private resources);
  - periodic tick (safety net) — bounds the worst-case dispatch latency when a
    poke is missed or a cross-instance message lands on a busy peer, and drives
    collect settle re-checks + the cancel-covered sweep fallback (§6.4);
  - PostgreSQL LISTEN/NOTIFY (cross-instance ``new_message`` latency optimization)
    is a pure wake-only signal layered on top of the same local event — injected
    via ``notify_listener`` when running on PG, never a correctness dependency.

``FOR UPDATE SKIP LOCKED`` inside claim makes NOTIFY/tick thundering-herd safe:
every instance wakes, only the one that locks a row works, the rest no-op.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.consumer.agent_runner import AgentRunner
    from app.consumer.run_registry import RunRegistry

logger = logging.getLogger(__name__)


class Scheduler:
    """Claims runnable candidates and dispatches them to AgentRunner under a slot semaphore.

    Args:
        registry: Shared RunRegistry (the only DB entry point).
        runner: AgentRunner that executes a ClaimedRun.
        instance_id: Stable identity for this Consumer process.
        max_concurrent_runs: Slot semaphore size.
        tick_interval: Safety-net re-scan period in seconds (§6.1).
        collect_gap_seconds / max_collect_wait_seconds: collect settle params (§6.2.2),
            pushed into the claim SQL; 0 = collect batches immediately (L2 off).
        task_registry: Optional shared set tracking in-flight run tasks (graceful shutdown).
    """

    def __init__(
        self,
        registry: RunRegistry,
        runner: AgentRunner,
        instance_id: str,
        *,
        max_concurrent_runs: int = 10,
        tick_interval: float = 1.0,
        collect_gap_seconds: float = 0.0,
        max_collect_wait_seconds: float = 0.0,
        task_registry: set[asyncio.Task] | None = None,
    ) -> None:
        self._registry = registry
        self._runner = runner
        self._instance_id = instance_id
        self._sem = asyncio.Semaphore(max_concurrent_runs)
        self._tick_interval = tick_interval
        self._collect_gap_seconds = collect_gap_seconds
        self._max_collect_wait_seconds = max_collect_wait_seconds
        self._wake = asyncio.Event()
        self._tasks: set[asyncio.Task] = task_registry if task_registry is not None else set()
        self._stopped = False

    # ── Wake-up ───────────────────────────────────────────────────────────────

    def poke(self) -> None:
        """Wake the claim loop (ingest after commit, or a freed slot). Never blocks."""
        self._wake.set()

    # ── Main loop ──────────────────────────────────────────────────────────────

    async def run_loop(self, stop_event: asyncio.Event) -> None:
        """Claim + dispatch until ``stop_event`` is set.

        Each iteration drains all claimable candidates up to the slot limit, then waits
        for a poke or the periodic tick. On a tick (no poke) it also runs the cancel-
        covered sweep fallback (§6.4): threads with only covered rows and no executable
        candidate never trigger claim's inline cleanup, so the tick sweeps them.
        """
        while not stop_event.is_set():
            await self._drain_claims()
            await self._drain_tombstones()

            self._wake.clear()
            woken = await self._wait(stop_event)
            if stop_event.is_set():
                break
            if not woken:
                # periodic tick: sweep cancel-covered rows that no claim touched (§6.4)
                try:
                    await self._registry.sweep_cancelled()
                except Exception:
                    logger.debug("scheduler tick sweep_cancelled failed", exc_info=True)

    async def _wait(self, stop_event: asyncio.Event) -> bool:
        """Wait for a poke or the tick timeout. Returns True if poked, False on tick."""
        waiters = [asyncio.create_task(self._wake.wait()), asyncio.create_task(stop_event.wait())]
        try:
            done, pending = await asyncio.wait(
                waiters, timeout=self._tick_interval, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            for w in waiters:
                w.cancel()
        return self._wake.is_set()

    async def _drain_claims(self) -> None:
        """Claim and dispatch candidates while slots remain and rows are claimable."""
        while not self._stopped:
            if self._sem.locked():  # no free slot — wait for slot_available poke
                return
            try:
                claimed = await self._registry.claim_next_runnable(
                    self._instance_id,
                    collect_gap_seconds=self._collect_gap_seconds,
                    max_collect_wait_seconds=self._max_collect_wait_seconds,
                )
            except Exception:
                logger.exception("claim_next_runnable failed; backing off to next tick")
                return
            if claimed is None:
                return

            await self._sem.acquire()  # cannot block: checked not-locked just above
            task = asyncio.create_task(
                self._run_and_release(claimed),
                name=f"run-{claimed.message_id[:8]}",
            )
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
            logger.info(
                "Dispatched thread=%s message_id=%s policy=%s",
                claimed.thread_id,
                claimed.message_id,
                claimed.policy,
            )

    async def _run_and_release(self, claimed) -> None:
        """Run one claimed candidate, then free the slot and re-poke for the next."""
        try:
            await self._runner.run(claimed)
        except Exception:
            logger.exception("AgentRunner.run crashed for message_id=%s", claimed.message_id)
        finally:
            self._sem.release()
            self.poke()  # slot_available: re-scan for the next claimable candidate

    async def _drain_tombstones(self) -> None:
        """Claim + dispatch idle/paused delete tombstones while slots remain (§5.5, P7).

        delete does not enqueue a ``thread_msg_queue`` row, so its destroy cannot ride the
        queue claim. This is the Scheduler's second candidate source: ``claim_tombstone``
        durably claims one SENTINEL idle/paused thread (running tombstones are destroyed
        inline by their own run's finalize hook), then ``AgentRunner.destroy`` runs the heavy
        FS/OSS + DB cleanup off the poll-loop under the same slot semaphore.
        """
        while not self._stopped:
            if self._sem.locked():  # no free slot — wait for slot_available poke
                return
            try:
                tid = await self._registry.claim_tombstone(self._instance_id)
            except Exception:
                logger.exception("claim_tombstone failed; backing off to next tick")
                return
            if tid is None:
                return

            await self._sem.acquire()  # cannot block: checked not-locked just above
            task = asyncio.create_task(self._destroy_and_release(tid), name=f"destroy-{tid[:8]}")
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
            logger.info("Dispatched destroy for delete-tombstoned thread=%s", tid)

    async def _destroy_and_release(self, thread_id: str) -> None:
        """Destroy one delete-tombstoned thread, then free the slot and re-poke."""
        try:
            await self._runner.destroy(thread_id)
        except Exception:
            logger.exception("AgentRunner.destroy crashed for thread=%s", thread_id)
        finally:
            self._sem.release()
            self.poke()

    # ── Shutdown ───────────────────────────────────────────────────────────────

    async def drain_tasks(self, timeout: float = 30.0) -> None:
        """Wait for in-flight run tasks to finish, cancelling stragglers (graceful stop)."""
        self._stopped = True
        if not self._tasks:
            return
        logger.info("Waiting up to %.0fs for %d in-flight run(s)...", timeout, len(self._tasks))
        _, pending = await asyncio.wait(set(self._tasks), timeout=timeout)
        if pending:
            logger.warning("Cancelling %d run(s) that did not finish in time", len(pending))
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
