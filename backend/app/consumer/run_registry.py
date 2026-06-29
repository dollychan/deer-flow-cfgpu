"""RunRegistry — all DB operations for the Consumer layer.

One instance is shared per process. Every method acquires its own
short-lived session via the injected session_factory so connections are
never held across long-running agent executions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from sqlalchemy import and_, delete, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import aliased

from app.consumer.constants import (
    DELETE_SENTINEL,
    ClaimResult,
    InstanceStatus,
    ProcessedStatus,
    QueuePolicy,
    QueueRowStatus,
    ThreadStatus,
)
from app.consumer.models import (
    ConsumerInstanceRow,
    ProcessedMessageRow,
    ThreadMsgQueueRow,
    ThreadRunStateRow,
)
from app.consumer.schemas import build_downlink_echo

# Policies that can be claimed as an independent run (design §6.3 phase-1 predicate).
# prefix is pure history (only merged); steer is currently downgraded to followup at ingest.
_EXECUTABLE_POLICIES = (
    QueuePolicy.FOLLOWUP.value,
    QueuePolicy.COLLECT.value,
    QueuePolicy.RESUME.value,
    QueuePolicy.FORK.value,
    QueuePolicy.DRAIN.value,
)

# Outbox publish retry backoff (§9.3): exponential, capped.
_DELIVERY_BACKOFF_BASE_SECONDS = 5
_DELIVERY_BACKOFF_CAP_SECONDS = 300

# delete ack held-gate time (§5.5, P7 method B). A pre-staged ``deleted`` ack is parked
# with next_delivery_at = this far-future sentinel so the outbox producer skips it
# (fetch_undelivered filters next_delivery_at <= now); destroy's last txn flips it to NULL
# to release. A concrete far-future datetime — NOT a Postgres 'infinity' literal — so the
# `<= now` comparison behaves identically under aiosqlite (no infinity-timestamp semantics).
_DELETE_ACK_HELD_UNTIL = datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC)


def delete_ack_message_id(delete_message_id: str, thread_id: str) -> str:
    """Synthetic per-thread PK for a delete's pre-staged ``deleted`` ack (§5.5).

    N threads share one uplink ``delete.message_id``; the real ``message_id`` PK on
    processed_messages would collide, so the held ack row is keyed by this synthetic id
    (same ``:`` convention as the cancel-cleared drain row's ``<paused>:drain``). The real
    downlink ``message_id`` (== the uplink delete id) rides in result_cache.echo instead.
    """
    return f"{delete_message_id}:deleted:{thread_id}"


@dataclass
class ClaimedRun:
    """Result of a successful claim_next_runnable (design §6.3).

    input_bodies is the ordered list of MQ envelopes to reconstruct the run input:
    prefix history (cancel-covered, §6.4) first by seq, then the collect batch by seq
    (candidate first). AgentRunner rebuilds input from these; finalize_run derives the
    covered set from the DB, so it is not returned here.
    """

    thread_id: str
    message_id: str  # run_message_id — the candidate flipped to status='running'
    policy: str
    seq: int
    input_bodies: list[dict] = field(default_factory=list)
    batch_message_ids: list[str] = field(default_factory=list)  # collect batch (incl. candidate)
    prefix_message_ids: list[str] = field(default_factory=list)  # cancel-covered history merged in


@dataclass
class FoldOutcome:
    """What a cancel fold synthesized in its transaction (design §6.4/§6.5).

    drain_synthesized: a ``policy='drain'`` queue row was queued because the cancel
        cleared a HIL gate (paused→idle) — the Scheduler must be poked.
    idle_ack_synthesized: a ``processed_messages(cancelled)`` terminal was written
        (keyed on the cancel's own message_id) because the cancel landed on an idle
        thread with nothing to interrupt — the outbox delivers it, no poke needed.
    """

    drain_synthesized: bool = False
    idle_ack_synthesized: bool = False


class RunRegistry:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._sf = session_factory

    # ── Instance management ───────────────────────────────────────────────────

    async def register_instance(self, instance_id: str, hostname: str, pid: int) -> None:
        now = datetime.now(UTC)
        async with self._sf() as session:
            session.add(
                ConsumerInstanceRow(
                    instance_id=instance_id,
                    hostname=hostname,
                    pid=pid,
                    status=InstanceStatus.ACTIVE,
                    registered_at=now,
                    last_heartbeat=now,
                )
            )
            await session.commit()

    async def heartbeat_instance(self, instance_id: str) -> None:
        async with self._sf() as session:
            await session.execute(
                update(ConsumerInstanceRow)
                .where(ConsumerInstanceRow.instance_id == instance_id)
                .values(last_heartbeat=datetime.now(UTC))
            )
            await session.commit()

    async def mark_instance_draining(self, instance_id: str) -> None:
        async with self._sf() as session:
            await session.execute(
                update(ConsumerInstanceRow)
                .where(ConsumerInstanceRow.instance_id == instance_id)
                .values(status=InstanceStatus.DRAINING)
            )
            await session.commit()

    async def delete_instance(self, instance_id: str) -> None:
        async with self._sf() as session:
            await session.execute(
                delete(ConsumerInstanceRow).where(ConsumerInstanceRow.instance_id == instance_id)
            )
            await session.commit()

    async def get_instance(self, instance_id: str) -> ConsumerInstanceRow | None:
        """Return the ConsumerInstanceRow for a given instance_id, or None if not found."""
        async with self._sf() as session:
            return await session.get(ConsumerInstanceRow, instance_id)

    # ── Thread routing ────────────────────────────────────────────────────────

    async def get_thread_state(self, thread_id: str) -> ThreadRunStateRow | None:
        """Return the current ThreadRunStateRow for a thread, or None if not found."""
        async with self._sf() as session:
            return await session.get(ThreadRunStateRow, thread_id)

    async def claim_thread(
        self,
        thread_id: str,
        instance_id: str,
        message_id: str,
        thread_msg_seq: int = 0,
    ) -> ClaimResult:
        """Atomically claim a thread for execution.

        Uses SELECT FOR UPDATE so that in a PostgreSQL multi-consumer
        deployment only one instance wins. On SQLite the lock is a no-op
        but correctness is preserved because there is only one process.

        Returns "claimed" when the thread was idle and is now owned by
        this instance, or "running" when another instance holds it.
        """
        now = datetime.now(UTC)
        async with self._sf() as session:
            async with session.begin():
                result = await session.execute(
                    select(ThreadRunStateRow)
                    .where(ThreadRunStateRow.thread_id == thread_id)
                    .with_for_update()
                )
                row = result.scalar_one_or_none()

                if row is None or row.status == ThreadStatus.IDLE:
                    if row is None:
                        session.add(
                            ThreadRunStateRow(
                                thread_id=thread_id,
                                instance_id=instance_id,
                                message_id=message_id,
                                thread_msg_seq=thread_msg_seq,
                                status=ThreadStatus.RUNNING,
                                started_at=now,
                                last_heartbeat=now,
                            )
                        )
                    else:
                        row.instance_id = instance_id
                        row.message_id = message_id
                        row.thread_msg_seq = thread_msg_seq
                        row.status = ThreadStatus.RUNNING
                        row.started_at = now
                        row.last_heartbeat = now
                    return ClaimResult.CLAIMED

                return ClaimResult.RUNNING

    async def update_thread_run(
        self,
        thread_id: str,
        new_message_id: str,
    ) -> None:
        """Switch a thread to a new run (used by _drain_and_release)."""
        now = datetime.now(UTC)
        async with self._sf() as session:
            await session.execute(
                update(ThreadRunStateRow)
                .where(ThreadRunStateRow.thread_id == thread_id)
                .values(
                    message_id=new_message_id,
                    status=ThreadStatus.RUNNING,
                    started_at=now,
                    last_heartbeat=now,
                )
            )
            await session.commit()

    async def mark_thread_idle(self, thread_id: str) -> None:
        """Mark thread idle, reset drain_mode, and delete its 'current' crash-recovery row."""
        async with self._sf() as session:
            async with session.begin():
                await session.execute(
                    update(ThreadRunStateRow)
                    .where(ThreadRunStateRow.thread_id == thread_id)
                    .values(
                        status=ThreadStatus.IDLE,
                        drain_mode="followup",
                        last_heartbeat=datetime.now(UTC),
                    )
                )
                await session.execute(
                    delete(ThreadMsgQueueRow).where(
                        ThreadMsgQueueRow.thread_id == thread_id,
                        ThreadMsgQueueRow.policy == QueuePolicy.CURRENT,
                    )
                )

    async def heartbeat_thread(self, thread_id: str) -> None:
        async with self._sf() as session:
            await session.execute(
                update(ThreadRunStateRow)
                .where(ThreadRunStateRow.thread_id == thread_id)
                .values(last_heartbeat=datetime.now(UTC))
            )
            await session.commit()

    # ── Message queue ─────────────────────────────────────────────────────────

    async def enqueue_message(
        self,
        thread_id: str,
        message_id: str,
        body: dict,
        thread_msg_seq: int,
        policy: str,
    ) -> bool:
        """Insert a queue row idempotently.

        Returns True when a new row was inserted, False when the same
        message_id is already queued/current due to RocketMQ redelivery.
        """
        values = {
            "thread_id": thread_id,
            "message_id": message_id,
            "body": body,
            "policy": policy,
            "thread_msg_seq": thread_msg_seq,
            "created_at": datetime.now(UTC),
        }
        async with self._sf() as session:
            inserted = await self._insert_if_absent(
                session, ThreadMsgQueueRow, values, ThreadMsgQueueRow.message_id
            )
            await session.commit()
            return inserted

    async def upsert_current_msg(
        self, thread_id: str, message_id: str, body: dict, thread_msg_seq: int = 0
    ) -> None:
        """Write the 'current' crash-recovery row for a newly claimed run.

        Atomically replaces any existing 'current' row for this thread so the
        watchdog always sees the latest claimed message's complete MQ envelope.
        """
        now = datetime.now(UTC)
        async with self._sf() as session:
            async with session.begin():
                await session.execute(
                    delete(ThreadMsgQueueRow).where(
                        ThreadMsgQueueRow.thread_id == thread_id,
                        ThreadMsgQueueRow.policy == QueuePolicy.CURRENT,
                    )
                )
                await session.execute(
                    delete(ThreadMsgQueueRow).where(
                        ThreadMsgQueueRow.message_id == message_id,
                        ThreadMsgQueueRow.thread_id == thread_id,
                    )
                )
                session.add(
                    ThreadMsgQueueRow(
                        thread_id=thread_id,
                        message_id=message_id,
                        body=body,
                        policy=QueuePolicy.CURRENT,
                        thread_msg_seq=thread_msg_seq,
                        created_at=now,
                    )
                )

    async def get_current_msg(self, thread_id: str) -> ThreadMsgQueueRow | None:
        """Return the 'current' crash-recovery row for this thread, or None."""
        stmt = select(ThreadMsgQueueRow).where(
            ThreadMsgQueueRow.thread_id == thread_id,
            ThreadMsgQueueRow.policy == QueuePolicy.CURRENT,
        )
        async with self._sf() as session:
            result = await session.execute(stmt)
            return result.scalar_one_or_none()

    async def peek_thread_queue(
        self,
        thread_id: str,
        policies: tuple[str, ...] = (QueuePolicy.FOLLOWUP,),
    ) -> list[ThreadMsgQueueRow]:
        """Return queue rows for the given policies, ordered by thread_msg_seq asc."""
        stmt = (
            select(ThreadMsgQueueRow)
            .where(
                ThreadMsgQueueRow.thread_id == thread_id,
                ThreadMsgQueueRow.policy.in_(policies),
            )
            .order_by(ThreadMsgQueueRow.thread_msg_seq.asc())
        )
        async with self._sf() as session:
            result = await session.execute(stmt)
            return list(result.scalars())

    async def find_cancel_after_seq(
        self, thread_id: str, current_task_seq: int
    ) -> ThreadMsgQueueRow | None:
        """Return the earliest cancel row with thread_msg_seq > current_task_seq, or None."""
        stmt = (
            select(ThreadMsgQueueRow)
            .where(
                ThreadMsgQueueRow.thread_id == thread_id,
                ThreadMsgQueueRow.policy == QueuePolicy.CANCEL,
                ThreadMsgQueueRow.thread_msg_seq > current_task_seq,
            )
            .order_by(ThreadMsgQueueRow.thread_msg_seq.asc())
            .limit(1)
        )
        async with self._sf() as session:
            result = await session.execute(stmt)
            return result.scalar_one_or_none()

    async def get_followup_before_seq(
        self, thread_id: str, cancel_seq: int
    ) -> list[ThreadMsgQueueRow]:
        """Return followup rows with thread_msg_seq < cancel_seq, ordered oldest-first."""
        stmt = (
            select(ThreadMsgQueueRow)
            .where(
                ThreadMsgQueueRow.thread_id == thread_id,
                ThreadMsgQueueRow.policy == QueuePolicy.FOLLOWUP,
                ThreadMsgQueueRow.thread_msg_seq < cancel_seq,
            )
            .order_by(ThreadMsgQueueRow.thread_msg_seq.asc())
        )
        async with self._sf() as session:
            result = await session.execute(stmt)
            return list(result.scalars())

    async def convert_to_prefix(self, thread_id: str, row_ids: list[int]) -> None:
        """Convert followup rows to prefix policy (preserve LLM context after cancel)."""
        if not row_ids:
            return
        async with self._sf() as session:
            await session.execute(
                update(ThreadMsgQueueRow)
                .where(ThreadMsgQueueRow.id.in_(row_ids))
                .values(policy=QueuePolicy.PREFIX)
            )
            await session.commit()

    async def delete_queue_items(self, thread_id: str, row_ids: list[int]) -> None:
        """Hard-delete queue rows by id."""
        if not row_ids:
            return
        async with self._sf() as session:
            await session.execute(
                delete(ThreadMsgQueueRow).where(ThreadMsgQueueRow.id.in_(row_ids))
            )
            await session.commit()

    async def get_drain_mode(self, thread_id: str) -> str:
        """Return the drain_mode for a thread ('followup' if thread not found)."""
        async with self._sf() as session:
            row = await session.get(ThreadRunStateRow, thread_id)
            return row.drain_mode if row else "followup"

    async def transition_thread_followup(
        self,
        thread_id: str,
        queue_id: int,
        new_message_id: str,
        new_body: dict,
        thread_msg_seq: int,
        *,
        prefix_ids: list[int] | None = None,
    ) -> None:
        """Atomically: delete followup + prefix rows, advance thread run, replace current row.

        All four operations in one transaction to prevent followup loss on crash.
        """
        now = datetime.now(UTC)
        async with self._sf() as session:
            async with session.begin():
                await session.execute(
                    delete(ThreadMsgQueueRow).where(ThreadMsgQueueRow.id == queue_id)
                )
                if prefix_ids:
                    await session.execute(
                        delete(ThreadMsgQueueRow).where(ThreadMsgQueueRow.id.in_(prefix_ids))
                    )
                await session.execute(
                    update(ThreadRunStateRow)
                    .where(ThreadRunStateRow.thread_id == thread_id)
                    .values(
                        message_id=new_message_id,
                        thread_msg_seq=thread_msg_seq,
                        status=ThreadStatus.RUNNING,
                        started_at=now,
                        last_heartbeat=now,
                    )
                )
                await session.execute(
                    delete(ThreadMsgQueueRow).where(
                        ThreadMsgQueueRow.thread_id == thread_id,
                        ThreadMsgQueueRow.policy == QueuePolicy.CURRENT,
                    )
                )
                session.add(
                    ThreadMsgQueueRow(
                        thread_id=thread_id,
                        message_id=new_message_id,
                        body=new_body,
                        policy=QueuePolicy.CURRENT,
                        thread_msg_seq=thread_msg_seq,
                        created_at=now,
                    )
                )

    # ── Idempotency ───────────────────────────────────────────────────────────

    async def check_processed(self, message_id: str) -> ProcessedMessageRow | None:
        async with self._sf() as session:
            return await session.get(ProcessedMessageRow, message_id)

    async def mark_processed(
        self,
        message_id: str,
        thread_id: str,
        status: str,
        result_cache: dict | None = None,
    ) -> None:
        """Insert idempotency record. Silently skipped on duplicate message_id."""
        async with self._sf() as session:
            try:
                session.add(
                    ProcessedMessageRow(
                        message_id=message_id,
                        thread_id=thread_id,
                        status=status,
                        result_cache=result_cache,
                        processed_at=datetime.now(UTC),
                    )
                )
                await session.commit()
            except IntegrityError:
                await session.rollback()

    # ── Watchdog helpers ──────────────────────────────────────────────────────

    async def claim_stale_run(self, thread_id: str, instance_id: str) -> bool:
        """Atomically claim a stale running thread for retry by this instance.

        Uses SELECT FOR UPDATE so only one watchdog wins in a multi-Consumer cluster.
        Returns True if successfully claimed, False if another instance already claimed it.
        """
        now = datetime.now(UTC)
        async with self._sf() as session:
            async with session.begin():
                result = await session.execute(
                    select(ThreadRunStateRow)
                    .where(ThreadRunStateRow.thread_id == thread_id)
                    .with_for_update()
                )
                row = result.scalar_one_or_none()
                if row is None or row.status != ThreadStatus.RUNNING:
                    return False
                row.instance_id = instance_id
                row.started_at = now
                row.last_heartbeat = now
                return True

    async def increment_retry_count(self, thread_id: str) -> None:
        """Increment stale-run retry counter before a watchdog-triggered re-execution."""
        async with self._sf() as session:
            await session.execute(
                update(ThreadRunStateRow)
                .where(ThreadRunStateRow.thread_id == thread_id)
                .values(retry_count=ThreadRunStateRow.retry_count + 1)
            )
            await session.commit()

    async def reset_retry_count(self, thread_id: str) -> None:
        """Reset stale-run retry counter after a run completes (any terminal status)."""
        async with self._sf() as session:
            await session.execute(
                update(ThreadRunStateRow)
                .where(ThreadRunStateRow.thread_id == thread_id)
                .values(retry_count=0)
            )
            await session.commit()

    async def find_stale_runs(self, timeout_seconds: int = 60) -> list[ThreadRunStateRow]:
        """Return running threads whose heartbeat is stale AND whose owning Consumer instance is also dead.

        Two conditions must both be true to avoid false positives from a healthy
        Consumer whose per-run heartbeat_loop task crashed independently:
          1. thread_run_state.last_heartbeat < cutoff  (run stopped updating)
          2. The owning instance has no fresh row in consumer_instances
             (either missing entirely, or its own heartbeat is also stale)
        """
        cutoff = datetime.now(UTC) - timedelta(seconds=timeout_seconds)

        # Correlated subquery: instance is alive iff it has a fresh heartbeat row
        instance_alive = (
            select(ConsumerInstanceRow.instance_id)
            .where(
                ConsumerInstanceRow.instance_id == ThreadRunStateRow.instance_id,
                ConsumerInstanceRow.last_heartbeat >= cutoff,
            )
            .correlate(ThreadRunStateRow)
            .exists()
        )

        stmt = select(ThreadRunStateRow).where(
            ThreadRunStateRow.status == ThreadStatus.RUNNING,
            ThreadRunStateRow.last_heartbeat < cutoff,
            ~instance_alive,
        )
        async with self._sf() as session:
            result = await session.execute(stmt)
            return list(result.scalars())

    async def cleanup_processed_messages(self, ttl_days: int) -> int:
        """Delete eligible processed_messages records older than ttl_days (design §9.4).

        Because processed_messages doubles as the result outbox (D7), the retention
        sweep must NOT delete by time alone. Only rows that are:
          - delivered=true (undelivered terminal results are kept forever — deleting
            them loses the result, §9.3), AND
          - older than the retention cutoff, AND
          - not a paused_for_approval row whose thread is still paused (method-C
            exemption: while paused this row is the sole dedup guard against re-running
            the original input, §6.5)
        are eligible. Returns the deleted row count.
        """
        cutoff = datetime.now(UTC) - timedelta(days=ttl_days)
        paused_guard = (
            select(ThreadRunStateRow.thread_id)
            .where(
                ThreadRunStateRow.thread_id == ProcessedMessageRow.thread_id,
                ThreadRunStateRow.status == ThreadStatus.PAUSED.value,
            )
            .correlate(ProcessedMessageRow)
            .exists()
        )
        async with self._sf() as session:
            result = await session.execute(
                delete(ProcessedMessageRow).where(
                    ProcessedMessageRow.delivered.is_(True),
                    ProcessedMessageRow.processed_at < cutoff,
                    ~and_(
                        ProcessedMessageRow.status == ProcessedStatus.PAUSED_FOR_APPROVAL.value,
                        paused_guard,
                    ),
                )
            )
            await session.commit()
            return result.rowcount

    # ══════════════════════════════════════════════════════════════════════════
    # v2 (Phase B) — layered scheduler primitives (design §6.3/6.4/7.3/9.3)
    # Additive: coexists with the v1 methods above until Phase C rewrites callers.
    # ══════════════════════════════════════════════════════════════════════════

    # ── helpers ────────────────────────────────────────────────────────────────

    async def _insert_if_absent(
        self,
        session: AsyncSession,
        model: type,
        values: dict,
        conflict_col,
    ) -> bool:
        """Dialect-aware INSERT ... ON CONFLICT(conflict_col) DO NOTHING.

        Centralizes the pg / sqlite / generic-fallback ladder shared by every idempotent
        insert in this module (state placeholder, processed/outbox row, queue/drain row,
        enqueue). Does NOT commit — the caller owns the transaction. Returns True iff a new
        row was inserted (False on conflict / redelivery).
        """
        dialect = (await session.connection()).dialect.name
        if dialect == "postgresql":
            result = await session.execute(
                pg_insert(model).values(**values).on_conflict_do_nothing(index_elements=[conflict_col])
            )
            return (result.rowcount or 0) > 0
        if dialect == "sqlite":
            result = await session.execute(
                sqlite_insert(model).values(**values).on_conflict_do_nothing(index_elements=[conflict_col])
            )
            return (result.rowcount or 0) > 0
        try:
            async with session.begin_nested():
                session.add(model(**values))
            return True
        except IntegrityError:
            return False

    async def _insert_state_if_absent(self, session: AsyncSession, thread_id: str, now: datetime) -> None:
        """INSERT an idle placeholder thread_run_state row ON CONFLICT DO NOTHING (§6.3 phase-2/§6.4).

        instance_id/message_id stay NULL — this row exists only to carry cancel_watermark
        or to be the mutex point a claim will flip to running.
        """
        await self._insert_if_absent(
            session,
            ThreadRunStateRow,
            {"thread_id": thread_id, "status": ThreadStatus.IDLE.value, "last_heartbeat": now},
            ThreadRunStateRow.thread_id,
        )

    async def _insert_processed_if_absent(
        self,
        session: AsyncSession,
        message_id: str,
        thread_id: str,
        status: str,
        result_cache: dict | None,
        delivered: bool,
        now: datetime,
    ) -> None:
        """INSERT a processed_messages row ON CONFLICT(message_id) DO NOTHING (§9.3 outbox).

        First write wins: a row already terminal (e.g. a prefix row's earlier 'cancelled')
        is never overwritten, so finalize cannot clobber a cancelled terminal (§7.3).
        """
        await self._insert_if_absent(
            session,
            ProcessedMessageRow,
            {
                "message_id": message_id,
                "thread_id": thread_id,
                "status": status,
                "result_cache": result_cache,
                "delivered": delivered,
                "delivered_at": now if delivered else None,
                "delivery_attempts": 0,
                "processed_at": now,
            },
            ProcessedMessageRow.message_id,
        )

    # ── (b) ingest side: cancel barrier fold (§6.4) ─────────────────────────────

    @staticmethod
    def _build_drain_body(
        thread_id: str, drain_message_id: str, seq: int, paused_message_id: str,
        client_id: str | None = None,
    ) -> dict:
        """Synthesize the queue-row body for a cancel-cleared HIL gate's drain run (§6.5).

        A minimal valid task envelope: AgentRunner's drain branch ignores the body's
        command and rebuilds an all-reject Command from aget_state, so only thread_id /
        message_id / a schema-valid payload matter. reply_config.stream_events=false and
        the drain policy (set on the queue row) ensure no downlink is produced (I12).
        ``clientId`` is schema-required on every uplink envelope; the drain run produces
        no downlink so the value is never echoed — carry the triggering cancel's clientId
        when known, else a clearly-internal sentinel.
        """
        return {
            "schema_version": "2.5",
            "message_id": drain_message_id,
            "type": "task",
            "thread_id": thread_id,
            "thread_msg_seq": seq,
            "clientId": client_id or "_internal_drain",
            "payload": {
                "command": {"update": {"tool_approvals": {}}},
                "reply_config": {"stream_events": False},
            },
            "_drain_of": paused_message_id,
        }

    async def _insert_queue_if_absent(
        self,
        session: AsyncSession,
        thread_id: str,
        message_id: str,
        body: dict,
        seq: int,
        policy: str,
        now: datetime,
    ) -> None:
        """INSERT a pending thread_msg_queue row ON CONFLICT(message_id) DO NOTHING.

        Used to synthesize the drain row inside the cancel-fold transaction (§6.5/I12);
        the unique message_id makes redelivered cancels idempotent (same drain_message_id).
        """
        await self._insert_if_absent(
            session,
            ThreadMsgQueueRow,
            {
                "thread_id": thread_id,
                "message_id": message_id,
                "body": body,
                "policy": policy,
                "status": QueueRowStatus.PENDING.value,
                "thread_msg_seq": seq,
                "created_at": now,
            },
            ThreadMsgQueueRow.message_id,
        )

    async def fold_cancel_watermark(
        self,
        thread_id: str,
        cancel_seq: int,
        cancel_message_id: str | None = None,
        echo: dict | None = None,
    ) -> FoldOutcome:
        """Fold cancel(seq=N) into the thread_run_state high-water (design §6.4/§6.5, D2/I12).

        One transaction under the state-row lock: cancel_watermark and last_resolved_seq
        both advance via GREATEST (monotonic, I2), and the paused→idle gate-clear compares
        the OLD last_resolved_seq (== the paused run's seq P) against the NEW watermark so a
        late small-seq cancel that does not cover the pending-approval run leaves it paused.

        Two synthesis branches, both in this same transaction (see FoldOutcome):

        - **drain** — when the fold clears a HIL gate (paused run actually covered), queue a
          ``policy='drain'`` row (seq=N, message_id='<paused>:drain', ON CONFLICT DO NOTHING)
          so the Scheduler dispatches a reject-resume that drains the orphaned interrupt
          checkpoint to a clean terminal (I12).

        - **idle ack** (§6.4) — when the cancel lands on an ``idle`` thread and is "live"
          (``cancel_seq > OLD last_resolved_seq``), write a ``processed_messages(cancelled)``
          terminal keyed on the cancel's own ``message_id`` (delivered=false → outbox
          delivers ``result(cancelled, last_resolved_seq=OLD lrs)``), so a cancel that has
          nothing to interrupt (lost task / cancel-before-task) still gives the client a
          terminal. ``cancel_seq <= OLD lrs`` (redundant / redelivered / late small seq) and
          calls without ``cancel_message_id`` synthesize nothing. ON CONFLICT keeps it
          idempotent on redelivery.
        """
        now = datetime.now(UTC)
        async with self._sf() as session:
            async with session.begin():
                await self._insert_state_if_absent(session, thread_id, now)
                state = (
                    await session.execute(
                        select(ThreadRunStateRow)
                        .where(ThreadRunStateRow.thread_id == thread_id)
                        .with_for_update()
                    )
                ).scalar_one()

                old_status = state.status
                old_lrs = state.last_resolved_seq or 0
                old_msg = state.message_id
                new_wm = max(state.cancel_watermark or 0, cancel_seq)
                new_lrs = max(old_lrs, cancel_seq)
                gate_cleared = old_status == ThreadStatus.PAUSED.value and old_lrs < new_wm
                idle_ack = (
                    old_status == ThreadStatus.IDLE.value
                    and cancel_seq > old_lrs
                    and cancel_message_id is not None
                )

                state.cancel_watermark = new_wm
                state.last_resolved_seq = new_lrs
                if gate_cleared:
                    state.status = ThreadStatus.IDLE.value

                outcome = FoldOutcome()
                if gate_cleared and old_msg:
                    drain_mid = f"{old_msg}:drain"
                    await self._insert_queue_if_absent(
                        session,
                        thread_id,
                        drain_mid,
                        self._build_drain_body(
                            thread_id, drain_mid, cancel_seq, old_msg,
                            client_id=(echo or {}).get("clientId"),
                        ),
                        cancel_seq,
                        QueuePolicy.DRAIN.value,
                        now,
                    )
                    outcome.drain_synthesized = True
                elif idle_ack:
                    await self._insert_processed_if_absent(
                        session,
                        cancel_message_id,
                        thread_id,
                        ProcessedStatus.CANCELLED.value,
                        {
                            "status": ProcessedStatus.CANCELLED.value,
                            "last_resolved_seq": old_lrs,
                            "echo": echo or {"message_id": cancel_message_id, "thread_id": thread_id},
                        },
                        False,
                        now,
                    )
                    outcome.idle_ack_synthesized = True
                return outcome

    async def _sweep_covered_locked(
        self, session: AsyncSession, thread_id: str, watermark: int, now: datetime
    ) -> int:
        """Cancel-zone cleanup for one thread (design §6.4), assuming the state row is
        already locked by the caller (claim phase-3, or sweep_cancelled).

        Every pending row with seq < watermark and policy not in (prefix, drain) gets a
        processed_messages(cancelled, delivered=false) and is flipped in place to
        policy='prefix' (status stays pending) — kept as history, never deleted here.
        drain is excluded (B′): it may sit below a later watermark but must still run.
        Returns the number of rows swept.
        """
        # delete tombstone (§5.5): a SENTINEL watermark covers *every* pending row, but
        # delete must NOT spray a per-message ``cancelled`` for each (it emits a single
        # ``deleted``). destroy() wipes these rows wholesale, so skip the cancelled-cleanup
        # entirely here (covers both claim phase-3 and the tick sweep_cancelled fallback).
        if watermark >= DELETE_SENTINEL:
            return 0
        covered = (
            await session.execute(
                select(ThreadMsgQueueRow).where(
                    ThreadMsgQueueRow.thread_id == thread_id,
                    ThreadMsgQueueRow.status == QueueRowStatus.PENDING.value,
                    ThreadMsgQueueRow.thread_msg_seq < watermark,
                    ThreadMsgQueueRow.policy.not_in(
                        [QueuePolicy.PREFIX.value, QueuePolicy.DRAIN.value]
                    ),
                )
            )
        ).scalars().all()
        for row in covered:
            # Carry the echo derived from the stored uplink envelope so the outbox can build a
            # full downlink that mirrors the uplink (thread_msg_seq + bizType + user_id /
            # project_id / agent_name). A bare result_cache=None would degrade the cancelled
            # terminal to message_id/thread_id only (thread_msg_seq=0, missing context).
            await self._insert_processed_if_absent(
                session,
                row.message_id,
                thread_id,
                ProcessedStatus.CANCELLED.value,
                {
                    "status": ProcessedStatus.CANCELLED.value,
                    "echo": build_downlink_echo(row.body or {}),
                },
                False,
                now,
            )
            row.policy = QueuePolicy.PREFIX.value  # keep as history; status stays pending
        return len(covered)

    async def sweep_cancelled(self, thread_id: str | None = None) -> int:
        """Tick fallback sweep (design §6.4): convert cancel-covered pending rows to
        prefix even when no real task triggers a claim on the thread.

        For a thread with only covered rows and no executable candidate, claim never
        runs its inline cleanup; this periodic sweep runs the same per-thread batch
        under the state-row lock. With thread_id=None, sweeps every thread that has a
        state row. Returns the total rows swept.
        """
        now = datetime.now(UTC)
        async with self._sf() as session:
            async with session.begin():
                if thread_id is not None:
                    tids = [thread_id]
                else:
                    # Only threads with a live cancel barrier can have covered rows; a
                    # watermark of 0 sweeps nothing (no seq < 0). Filtering here keeps the
                    # per-tick sweep O(threads-with-cancels) instead of O(all-threads).
                    tids = list(
                        (
                            await session.execute(
                                select(ThreadRunStateRow.thread_id).where(
                                    ThreadRunStateRow.cancel_watermark > 0
                                )
                            )
                        ).scalars()
                    )
                total = 0
                for tid in tids:
                    state = (
                        await session.execute(
                            select(ThreadRunStateRow)
                            .where(ThreadRunStateRow.thread_id == tid)
                            .with_for_update()
                        )
                    ).scalar_one_or_none()
                    if state is None:
                        continue
                    total += await self._sweep_covered_locked(
                        session, tid, state.cancel_watermark or 0, now
                    )
                return total

    # ── (c) scheduler side: two-phase atomic claim (§6.3) ───────────────────────

    def _candidate_stmt(self, now: datetime, collect_gap_seconds: float, max_collect_wait_seconds: float):
        """Phase-1 candidate select: earliest *executable* pending row (design §6.3).

        State-aware ("idle → any policy" / "paused → resume only"), cancel-watermark
        filtered with the drain exemption (method B′), and gated by a NOT EXISTS
        "no earlier executable sibling" — to which the collect settle predicate is
        deliberately NOT added (asymmetry: an unsettled collect still blocks later
        followups, preserving in-order, §6.2.2).
        """
        q = ThreadMsgQueueRow
        s = ThreadRunStateRow
        watermark = func.coalesce(s.cancel_watermark, 0)

        def _state_allows(row):
            return or_(
                s.thread_id.is_(None),
                s.status == ThreadStatus.IDLE.value,
                and_(s.status == ThreadStatus.PAUSED.value, row.policy == QueuePolicy.RESUME.value),
            )

        def _not_covered(row):
            # drain exempt from watermark (B′): seq=N may fall under a later cancel's
            # raised watermark but must still claim/block (§6.5).
            return or_(row.thread_msg_seq >= watermark, row.policy == QueuePolicy.DRAIN.value)

        q2 = aliased(ThreadMsgQueueRow)
        blocking = (
            select(q2.id)
            .where(
                q2.thread_id == q.thread_id,
                q2.status == QueueRowStatus.PENDING.value,
                q2.thread_msg_seq < q.thread_msg_seq,
                _not_covered(q2),
                _state_allows(q2),
            )
            .correlate(q, s)
            .exists()
        )

        conds = [
            q.status == QueueRowStatus.PENDING.value,
            q.policy.in_(_EXECUTABLE_POLICIES),
            _not_covered(q),
            _state_allows(q),
            ~blocking,
        ]

        # collect settle (option b, §6.2.2): only gates collect candidates; OFF by
        # default (gap/wait <= 0) so collect behaves like an immediate followup that
        # still batches. Applied to candidate selection only, never to `blocking`.
        gap_on = bool(collect_gap_seconds and collect_gap_seconds > 0)
        wait_on = bool(max_collect_wait_seconds and max_collect_wait_seconds > 0)
        if gap_on or wait_on:
            settle_terms = [q.policy != QueuePolicy.COLLECT.value]
            if gap_on:
                q3 = aliased(ThreadMsgQueueRow)
                batch_max = (
                    select(func.max(q3.created_at))
                    .where(
                        q3.thread_id == q.thread_id,
                        q3.policy == QueuePolicy.COLLECT.value,
                        q3.status == QueueRowStatus.PENDING.value,
                        q3.thread_msg_seq >= q.thread_msg_seq,
                    )
                    .correlate(q)
                    .scalar_subquery()
                )
                settle_terms.append(batch_max <= now - timedelta(seconds=collect_gap_seconds))
            if wait_on:
                settle_terms.append(q.created_at <= now - timedelta(seconds=max_collect_wait_seconds))
            conds.append(or_(*settle_terms))

        return (
            select(q)
            .outerjoin(s, s.thread_id == q.thread_id)
            .where(*conds)
            .order_by(q.created_at.asc())
            .limit(1)
            .with_for_update(of=q, skip_locked=True)
        )

    async def claim_next_runnable(
        self,
        instance_id: str,
        *,
        collect_gap_seconds: float = 0.0,
        max_collect_wait_seconds: float = 0.0,
    ) -> ClaimedRun | None:
        """Atomically claim the next runnable candidate across all threads (design §6.3).

        Two-phase: phase-1 locks one candidate queue row (FOR UPDATE OF q SKIP LOCKED,
        load-balancing distinct threads across schedulers); phase-3 locks the
        thread_run_state row (the authoritative mutex) and re-checks under that lock.
        Within the same transaction it runs the cancel-zone cleanup (§6.4), folds the
        collect batch and prefix history (§6.2.2/§6.4), and flips the candidate to
        running. Returns a ClaimedRun, or None when nothing is runnable.
        """
        now = datetime.now(UTC)
        async with self._sf() as session:
            async with session.begin():
                # ── phase 1: pick + lock a candidate queue row ──
                cand = (
                    await session.execute(
                        self._candidate_stmt(now, collect_gap_seconds, max_collect_wait_seconds)
                    )
                ).scalars().first()
                if cand is None:
                    return None
                thread_id = cand.thread_id

                # ── phase 2: ensure the state row exists ──
                await self._insert_state_if_absent(session, thread_id, now)

                # ── phase 3: lock the state row (the real mutex) + re-check ──
                state = (
                    await session.execute(
                        select(ThreadRunStateRow)
                        .where(ThreadRunStateRow.thread_id == thread_id)
                        .with_for_update()
                    )
                ).scalar_one()

                watermark = state.cancel_watermark or 0
                allowed = state.status == ThreadStatus.IDLE.value or (
                    state.status == ThreadStatus.PAUSED.value and cand.policy == QueuePolicy.RESUME.value
                )
                not_covered = cand.thread_msg_seq >= watermark or cand.policy == QueuePolicy.DRAIN.value
                if not (allowed and not_covered and cand.status == QueueRowStatus.PENDING.value):
                    # Lost the race / covered by a fresher watermark → no-op (commits the
                    # harmless phase-2 placeholder insert only).
                    return None

                # ── cancel-zone cleanup: covered pending → cancelled + prefix (§6.4) ──
                await self._sweep_covered_locked(session, thread_id, watermark, now)

                # ── collect batch: contiguous collect prefix starting at candidate ──
                if cand.policy == QueuePolicy.COLLECT.value:
                    following = (
                        await session.execute(
                            select(ThreadMsgQueueRow)
                            .where(
                                ThreadMsgQueueRow.thread_id == thread_id,
                                ThreadMsgQueueRow.status == QueueRowStatus.PENDING.value,
                                ThreadMsgQueueRow.thread_msg_seq >= cand.thread_msg_seq,
                                or_(
                                    ThreadMsgQueueRow.thread_msg_seq >= watermark,
                                    ThreadMsgQueueRow.policy == QueuePolicy.DRAIN.value,
                                ),
                            )
                            .order_by(ThreadMsgQueueRow.thread_msg_seq.asc())
                        )
                    ).scalars().all()
                    batch = []
                    for row in following:
                        if row.policy == QueuePolicy.COLLECT.value:
                            batch.append(row)
                        else:
                            break
                else:
                    batch = [cand]

                # ── prefix history: merge cancel-covered rows in front (§6.4②) ──
                # fork lands on a fresh thread; drain runs a reject-resume with no input.
                prefix_rows: list[ThreadMsgQueueRow] = []
                if cand.policy not in (QueuePolicy.FORK.value, QueuePolicy.DRAIN.value):
                    prefix_rows = (
                        await session.execute(
                            select(ThreadMsgQueueRow)
                            .where(
                                ThreadMsgQueueRow.thread_id == thread_id,
                                ThreadMsgQueueRow.status == QueueRowStatus.PENDING.value,
                                ThreadMsgQueueRow.policy == QueuePolicy.PREFIX.value,
                            )
                            .order_by(ThreadMsgQueueRow.thread_msg_seq.asc())
                        )
                    ).scalars().all()

                # ── flip states ──
                for row in prefix_rows:
                    row.status = QueueRowStatus.MERGED.value  # policy stays 'prefix'
                for row in batch[1:]:
                    row.status = QueueRowStatus.MERGED.value
                cand.status = QueueRowStatus.RUNNING.value
                cand.claimed_by = instance_id
                cand.claimed_at = now

                batch_max_seq = max(row.thread_msg_seq for row in batch)
                new_lrs = max(state.last_resolved_seq or 0, batch_max_seq)
                state.status = ThreadStatus.RUNNING.value
                state.instance_id = instance_id
                state.message_id = cand.message_id
                state.started_at = now
                state.last_heartbeat = now
                state.last_resolved_seq = new_lrs
                state.retry_count = 0

                input_bodies = [row.body for row in prefix_rows] + [row.body for row in batch]
                return ClaimedRun(
                    thread_id=thread_id,
                    message_id=cand.message_id,
                    policy=cand.policy,
                    seq=cand.thread_msg_seq,
                    input_bodies=input_bodies,
                    batch_message_ids=[row.message_id for row in batch],
                    prefix_message_ids=[row.message_id for row in prefix_rows],
                )

    # ── (d) terminal close-out (§7.3) ───────────────────────────────────────────

    async def finalize_run(
        self,
        run_message_id: str,
        result_cache: dict | None,
        status: str,
    ) -> bool:
        """Close out a run across its whole batch of message_ids (design §7.3, D6).

        Single transaction guarded by an ownership check inside the state-row lock
        (state.status='running' AND state.message_id=run_message_id), then derives the
        covered set (running + merged), writes processed_messages (only run_message_id
        carries the result + delivered=false; collect-merged are idempotent terminal
        markers; prefix-merged are skipped — already cancelled at the barrier), deletes
        the whole batch, and returns the thread to idle. Returns True if it took effect,
        False on the no-op idempotency conflict (stale / double-call / drain row gone).

        status='drain' takes the housekeeping branch: delete only the drain row, return
        to idle, no processed_messages / outbox (drain produces no downlink, §6.5).
        """
        return await self._finalize(run_message_id, result_cache, status, paused=False)

    async def finalize_paused(self, run_message_id: str, result_cache: dict | None) -> bool:
        """HIL paused close-out — method C (design §6.5).

        Same batch-covered idempotent close as finalize_run, but the thread goes to
        'paused' (not idle), message_id stays the dangling bookmark, and the terminal
        status is paused_for_approval. Returns True/False like finalize_run.
        """
        return await self._finalize(
            run_message_id, result_cache, ProcessedStatus.PAUSED_FOR_APPROVAL.value, paused=True
        )

    async def _finalize(
        self,
        run_message_id: str,
        result_cache: dict | None,
        status: str,
        *,
        paused: bool,
    ) -> bool:
        now = datetime.now(UTC)
        async with self._sf() as session:
            async with session.begin():
                thread_id = (
                    await session.execute(
                        select(ThreadMsgQueueRow.thread_id).where(
                            ThreadMsgQueueRow.message_id == run_message_id
                        )
                    )
                ).scalar_one_or_none()
                if thread_id is None:
                    return False  # queue row already gone — nothing to finalize

                state = (
                    await session.execute(
                        select(ThreadRunStateRow)
                        .where(ThreadRunStateRow.thread_id == thread_id)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                # ── ownership guard (§7.3): must still own the running batch ──
                if (
                    state is None
                    or state.status != ThreadStatus.RUNNING.value
                    or state.message_id != run_message_id
                ):
                    return False

                # ── delete tombstone branch (§5.5): the thread is being destroyed ──
                # The running run was hard-cancelled by the SENTINEL watermark. Do NOT write
                # a per-message terminal (the single ``deleted`` ack covers the whole thread);
                # just drop the running/merged queue rows and leave the thread for destroy().
                # Stay tombstoned (cancel_watermark=SENTINEL) and go idle so the Scheduler
                # tombstone sweep / the runner's own post-finalize destroy hook reclaims it.
                if (state.cancel_watermark or 0) >= DELETE_SENTINEL and not paused:
                    await session.execute(
                        delete(ThreadMsgQueueRow).where(
                            ThreadMsgQueueRow.thread_id == thread_id,
                            ThreadMsgQueueRow.status.in_(
                                [QueueRowStatus.RUNNING.value, QueueRowStatus.MERGED.value]
                            ),
                        )
                    )
                    state.status = ThreadStatus.IDLE.value
                    state.retry_count = 0
                    state.last_heartbeat = now
                    return True

                # ── drain branch (§6.5/§7.3): no downlink, no processed_messages ──
                if status == QueuePolicy.DRAIN.value:
                    await session.execute(
                        delete(ThreadMsgQueueRow).where(
                            ThreadMsgQueueRow.message_id == run_message_id
                        )
                    )
                    state.status = ThreadStatus.IDLE.value
                    state.retry_count = 0
                    state.last_heartbeat = now
                    return True

                # ── derive covered batch (running + merged) under the state lock ──
                covered = (
                    await session.execute(
                        select(ThreadMsgQueueRow.message_id, ThreadMsgQueueRow.policy).where(
                            ThreadMsgQueueRow.thread_id == thread_id,
                            ThreadMsgQueueRow.status.in_(
                                [QueueRowStatus.RUNNING.value, QueueRowStatus.MERGED.value]
                            ),
                        )
                    )
                ).all()
                for mid, policy in covered:
                    if policy == QueuePolicy.PREFIX.value:
                        # prefix history already wrote processed_messages(cancelled) at the
                        # barrier (§6.4①) — only delete its queue row below, never rewrite.
                        continue
                    is_run = mid == run_message_id
                    await self._insert_processed_if_absent(
                        session,
                        mid,
                        thread_id,
                        status,
                        result_cache if is_run else None,
                        not is_run,  # run row: delivered=false (outbox); merged: delivered=true
                        now,
                    )

                await session.execute(
                    delete(ThreadMsgQueueRow).where(
                        ThreadMsgQueueRow.thread_id == thread_id,
                        ThreadMsgQueueRow.status.in_(
                            [QueueRowStatus.RUNNING.value, QueueRowStatus.MERGED.value]
                        ),
                    )
                )
                if paused:
                    state.status = ThreadStatus.PAUSED.value  # message_id stays as bookmark (§4.2)
                else:
                    state.status = ThreadStatus.IDLE.value
                    state.retry_count = 0
                state.last_heartbeat = now
                return True

    # ── (e) outbox (§9.3) ───────────────────────────────────────────────────────

    async def mark_processed_undelivered(
        self,
        message_id: str,
        thread_id: str,
        status: str,
        result_cache: dict | None = None,
    ) -> None:
        """Write a terminal processed_messages row as delivered=false (design §9.3).

        The mark-before-publish half of the transactional outbox; idempotent on
        message_id. Used by terminal writers (stream_bridge, agent_runner) that publish
        out of band and let the producer loop guarantee at-least-once delivery.
        """
        async with self._sf() as session:
            async with session.begin():
                await self._insert_processed_if_absent(
                    session, message_id, thread_id, status, result_cache, False, datetime.now(UTC)
                )

    async def fetch_undelivered(self, limit: int = 20) -> list[ProcessedMessageRow]:
        """Return undelivered terminal rows whose backoff window has elapsed (§9.3).

        Ordered by next_delivery_at; FOR UPDATE SKIP LOCKED so multiple instances do not
        re-publish the same row (PG; no-op on single-instance SQLite). Rows are returned
        detached (expire_on_commit=False) for the producer loop to publish then mark.
        """
        now = datetime.now(UTC)
        stmt = (
            select(ProcessedMessageRow)
            .where(
                ProcessedMessageRow.delivered.is_(False),
                or_(
                    ProcessedMessageRow.next_delivery_at.is_(None),
                    ProcessedMessageRow.next_delivery_at <= now,
                ),
            )
            .order_by(ProcessedMessageRow.next_delivery_at.asc().nullsfirst())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        async with self._sf() as session:
            result = await session.execute(stmt)
            return list(result.scalars())

    async def mark_delivered(self, message_id: str) -> None:
        """Mark a terminal result successfully published (§9.3). Idempotent."""
        now = datetime.now(UTC)
        async with self._sf() as session:
            await session.execute(
                update(ProcessedMessageRow)
                .where(ProcessedMessageRow.message_id == message_id)
                .values(delivered=True, delivered_at=now)
            )
            await session.commit()

    async def bump_delivery_failure(self, message_id: str, error: str) -> int:
        """Record a failed publish: +1 attempt, exponential backoff, last error (§9.3).

        Returns the new delivery_attempts so the caller can alert on poison messages.
        """
        now = datetime.now(UTC)
        async with self._sf() as session:
            async with session.begin():
                row = await session.get(ProcessedMessageRow, message_id)
                if row is None:
                    return 0
                row.delivery_attempts = (row.delivery_attempts or 0) + 1
                row.last_delivery_error = error
                backoff = min(
                    _DELIVERY_BACKOFF_BASE_SECONDS * (2 ** (row.delivery_attempts - 1)),
                    _DELIVERY_BACKOFF_CAP_SECONDS,
                )
                row.next_delivery_at = now + timedelta(seconds=backoff)
                return row.delivery_attempts

    # ── (f) v2 stale recovery (§8) ──────────────────────────────────────────────

    async def get_running_row(self, thread_id: str) -> ThreadMsgQueueRow | None:
        """Return the thread's single status='running' queue row (crash-recovery envelope, §8).

        v2 replacement for get_current_msg: the claimed row flipped in place to 'running'
        carries the complete MQ envelope used to reconstruct the run after a crash.
        """
        async with self._sf() as session:
            return (
                await session.execute(
                    select(ThreadMsgQueueRow)
                    .where(
                        ThreadMsgQueueRow.thread_id == thread_id,
                        ThreadMsgQueueRow.status == QueueRowStatus.RUNNING.value,
                    )
                    .limit(1)
                )
            ).scalar_one_or_none()

    async def requeue_stale_run(self, thread_id: str) -> bool:
        """Reset a stale running thread back into the claim pool (design §8).

        Locks the running queue row(s) first, then the state row (executable-zone order
        queue→state, I1), re-checks the thread is still running, then flips the running
        row plus any non-prefix merged siblings (collect batch) back to pending so the
        Scheduler re-claims and re-batches them; prefix-merged history stays merged (never
        re-executed). state returns to idle with retry_count+1. Returns True if it reset.

        The watchdog only resets state here — it never runs the graph itself; a normal
        sem-gated claim picks the thread up and LangGraph resumes from its checkpoint.
        """
        now = datetime.now(UTC)
        async with self._sf() as session:
            async with session.begin():
                merged_or_running = (
                    await session.execute(
                        select(ThreadMsgQueueRow)
                        .where(
                            ThreadMsgQueueRow.thread_id == thread_id,
                            ThreadMsgQueueRow.status.in_(
                                [QueueRowStatus.RUNNING.value, QueueRowStatus.MERGED.value]
                            ),
                        )
                        .with_for_update()
                    )
                ).scalars().all()
                state = (
                    await session.execute(
                        select(ThreadRunStateRow)
                        .where(ThreadRunStateRow.thread_id == thread_id)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                if state is None or state.status != ThreadStatus.RUNNING.value:
                    return False
                for row in merged_or_running:
                    # prefix history stays merged (cancelled already); everything else
                    # re-enters the claim pool to be re-batched.
                    if row.policy == QueuePolicy.PREFIX.value:
                        continue
                    row.status = QueueRowStatus.PENDING.value
                    row.claimed_by = None
                    row.claimed_at = None
                state.status = ThreadStatus.IDLE.value
                state.retry_count = (state.retry_count or 0) + 1
                state.last_heartbeat = now
                return True

    # ── (g) v2.6 delete: tombstone + held ack + destroy (§5.5, P7) ───────────────

    async def ingest_delete(
        self,
        threads: list[str],
        delete_message_id: str,
        echo_base: dict | None = None,
        *,
        now: datetime | None = None,
    ) -> int:
        """Fan-out a ``type=delete`` into per-thread tombstones + held acks (§5.5, method B).

        One transaction. For each (1-based index i, tid) in ``threads``: ensure the state
        row exists, raise its ``cancel_watermark`` to ``DELETE_SENTINEL`` (GREATEST — cancels
        everything + durably marks for destroy), and pre-stage the ``deleted`` ack into
        processed_messages keyed by the synthetic per-thread PK ``{delete}:deleted:{tid}``,
        ``delivered=false`` and ``next_delivery_at`` parked far-future so the outbox HOLDS it
        until destroy releases it. The real downlink ``message_id`` (== the uplink delete id),
        ``message_seq`` 1..N and ``thread_msg_seq=0`` ride in result_cache.echo. Idempotent:
        a redelivered delete re-raises GREATEST(SENTINEL)=SENTINEL and ON CONFLICT skips the
        already-staged ack. Returns the number of threads stamped (== len(deduped threads)).
        """
        now = now or datetime.now(UTC)
        async with self._sf() as session:
            async with session.begin():
                for i, tid in enumerate(threads, start=1):
                    # lock/upsert the state row (claim's mutex point §6.4) and raise the sentinel.
                    await self._insert_state_if_absent(session, tid, now)
                    state = (
                        await session.execute(
                            select(ThreadRunStateRow)
                            .where(ThreadRunStateRow.thread_id == tid)
                            .with_for_update()
                        )
                    ).scalar_one()
                    state.cancel_watermark = max(state.cancel_watermark or 0, DELETE_SENTINEL)

                    echo = dict(echo_base or {})
                    echo["message_id"] = delete_message_id
                    echo["thread_id"] = tid
                    echo["thread_msg_seq"] = 0
                    echo["message_seq"] = i  # per-message_id 1..N (MQ消息协议.md「deleted」)
                    await self._insert_if_absent(
                        session,
                        ProcessedMessageRow,
                        {
                            "message_id": delete_ack_message_id(delete_message_id, tid),
                            "thread_id": tid,
                            "status": ProcessedStatus.DELETED.value,
                            "result_cache": {"type": "deleted", "payload": {}, "echo": echo},
                            "delivered": False,
                            "delivered_at": None,
                            "delivery_attempts": 0,
                            "next_delivery_at": _DELETE_ACK_HELD_UNTIL,  # held until destroy
                            "processed_at": now,
                        },
                        ProcessedMessageRow.message_id,
                    )
                return len(threads)

    async def is_tombstoned(self, thread_id: str) -> bool:
        """True iff this thread carries the delete tombstone (cancel_watermark == SENTINEL)."""
        state = await self.get_thread_state(thread_id)
        return state is not None and (state.cancel_watermark or 0) >= DELETE_SENTINEL

    async def claim_tombstone(self, instance_id: str) -> str | None:
        """Scheduler second candidate source: claim one idle/paused delete tombstone (§5.5).

        delete does not enqueue a ``thread_msg_queue`` row, so there is nothing for the
        normal queue claim to pick up. This scans ``thread_run_state`` for a SENTINEL row in
        ``idle``/``paused`` (a ``running`` tombstone is handled inline by its own run's
        finalize→destroy), locks it ``FOR UPDATE SKIP LOCKED`` so peers don't double-claim,
        and flips it to ``running`` owned by this instance — a durable claim so a crash mid
        destroy leaves a running+SENTINEL row that §8 stale-recovery reclaims (requeue→idle→
        re-swept here). Returns the thread_id to destroy, or None when none is pending.
        """
        now = datetime.now(UTC)
        async with self._sf() as session:
            async with session.begin():
                row = (
                    await session.execute(
                        select(ThreadRunStateRow)
                        .where(
                            ThreadRunStateRow.cancel_watermark >= DELETE_SENTINEL,
                            ThreadRunStateRow.status.in_(
                                [ThreadStatus.IDLE.value, ThreadStatus.PAUSED.value]
                            ),
                        )
                        .limit(1)
                        .with_for_update(skip_locked=True)
                    )
                ).scalars().first()
                if row is None:
                    return None
                row.status = ThreadStatus.RUNNING.value
                row.instance_id = instance_id
                row.message_id = None
                row.started_at = now
                row.last_heartbeat = now
                return row.thread_id

    async def destroy_thread_state(self, thread_id: str) -> bool:
        """Wipe a delete-tombstoned thread's DB run-state and release its held ack (§5.5 step b).

        Single transaction under the state-row lock (idempotent / serialized — a concurrent
        destroyer that loses the row lock re-checks and no-ops). Requires no delete context:
        the held ack is found by ``status='deleted'`` on the thread, not by a passed id, so a
        cross-instance / crash re-run reads only persisted tombstone state. Steps:
          - verify the row is still tombstoned (cancel_watermark == SENTINEL) else no-op;
          - delete all run-state rows for the thread EXCEPT the ``deleted`` ack
            (thread_msg_queue wholesale; processed_messages where status != 'deleted');
          - release the held ack(s): next_delivery_at = NULL so the outbox publishes ``deleted``;
          - delete the tombstone state row.
        Returns True if a tombstone was found and cleared, False on no-op.

        FS/OSS recycling (checkpoint / threadData dir / OSS prefix) is the caller's
        non-transactional, best-effort prelude (AgentRunner.destroy) — it never touches DB.
        """
        async with self._sf() as session:
            async with session.begin():
                state = (
                    await session.execute(
                        select(ThreadRunStateRow)
                        .where(ThreadRunStateRow.thread_id == thread_id)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                if state is None or (state.cancel_watermark or 0) < DELETE_SENTINEL:
                    return False  # already destroyed / never a tombstone — no-op

                await session.execute(
                    delete(ThreadMsgQueueRow).where(ThreadMsgQueueRow.thread_id == thread_id)
                )
                await session.execute(
                    delete(ProcessedMessageRow).where(
                        ProcessedMessageRow.thread_id == thread_id,
                        ProcessedMessageRow.status != ProcessedStatus.DELETED.value,
                    )
                )
                # release the held ack(s) so the outbox delivers ``deleted`` (§5.5 method B).
                await session.execute(
                    update(ProcessedMessageRow)
                    .where(
                        ProcessedMessageRow.thread_id == thread_id,
                        ProcessedMessageRow.status == ProcessedStatus.DELETED.value,
                        ProcessedMessageRow.delivered.is_(False),
                    )
                    .values(next_delivery_at=None)
                )
                await session.execute(
                    delete(ThreadRunStateRow).where(ThreadRunStateRow.thread_id == thread_id)
                )
                return True
