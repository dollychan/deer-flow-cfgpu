"""RunRegistry — all DB operations for the Consumer layer.

One instance is shared per process. Every method acquires its own
short-lived session via the injected session_factory so connections are
never held across long-running agent executions.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.consumer.models import (
    ConsumerInstanceRow,
    ProcessedMessageRow,
    ThreadMsgQueueRow,
    ThreadRunStateRow,
)

from app.consumer.constants import ClaimResult, InstanceStatus, QueuePolicy, ThreadStatus


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
            bind = await session.connection()
            dialect_name = bind.dialect.name

            if dialect_name == "postgresql":
                stmt = (
                    pg_insert(ThreadMsgQueueRow)
                    .values(**values)
                    .on_conflict_do_nothing(
                        index_elements=[ThreadMsgQueueRow.message_id]
                    )
                )
                result = await session.execute(stmt)
                await session.commit()
                return (result.rowcount or 0) > 0

            if dialect_name == "sqlite":
                stmt = (
                    sqlite_insert(ThreadMsgQueueRow)
                    .values(**values)
                    .on_conflict_do_nothing(
                        index_elements=[ThreadMsgQueueRow.message_id]
                    )
                )
                result = await session.execute(stmt)
                await session.commit()
                return (result.rowcount or 0) > 0

            try:
                session.add(ThreadMsgQueueRow(**values))
                await session.commit()
                return True
            except IntegrityError:
                await session.rollback()
                return False

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
        """Delete processed_messages records older than ttl_days. Returns deleted row count."""
        cutoff = datetime.now(UTC) - timedelta(days=ttl_days)
        async with self._sf() as session:
            result = await session.execute(
                delete(ProcessedMessageRow).where(ProcessedMessageRow.processed_at < cutoff)
            )
            await session.commit()
            return result.rowcount
