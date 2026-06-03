"""Phase A — Consumer v2 ORM schema: tables build, new columns + indexes exist.

Uses in-memory SQLite. Verifies the additive v2 columns (§4.2/§4.3/§4.4) and the
dialect-aware partial indexes (§6.3/§9.3) create cleanly and that the
``ux_thread_running`` partial-unique actually enforces one running row per thread.
"""

from __future__ import annotations

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

# Import registers the consumer tables on the shared Base.
from app.consumer.models import (  # noqa: F401
    ConsumerInstanceRow,
    ProcessedMessageRow,
    ThreadMsgQueueRow,
    ThreadRunStateRow,
)
from deerflow.persistence.base import Base


@pytest.fixture
async def sf():
    """Fresh in-memory SQLite with all tables created (exercises DDL)."""
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


# ---------------------------------------------------------------------------
# Column presence (model definition)
# ---------------------------------------------------------------------------


class TestColumns:
    def test_thread_run_state_v2_columns(self):
        cols = ThreadRunStateRow.__table__.columns.keys()
        assert "cancel_watermark" in cols  # D2 §6.4
        assert "last_resolved_seq" in cols  # D4 §4.2

    def test_thread_msg_queue_v2_columns(self):
        cols = ThreadMsgQueueRow.__table__.columns.keys()
        assert {"status", "claimed_by", "claimed_at"}.issubset(cols)  # D5 §6.3

    def test_processed_messages_outbox_columns(self):
        cols = ProcessedMessageRow.__table__.columns.keys()
        assert {
            "delivered",
            "delivered_at",
            "delivery_attempts",
            "next_delivery_at",
            "last_delivery_error",
        }.issubset(cols)  # D7 §9.3


# ---------------------------------------------------------------------------
# Index creation (DDL actually ran on SQLite — the real Phase A risk)
# ---------------------------------------------------------------------------


class TestIndexes:
    @pytest.mark.anyio
    async def test_partial_indexes_created(self, sf):
        async with sf() as session:
            rows = await session.execute(
                text("SELECT name FROM sqlite_master WHERE type='index'")
            )
            names = {r[0] for r in rows}
        assert "ux_thread_running" in names
        assert "ix_msg_queue_thread_seq" in names
        assert "ix_processed_undelivered" in names


# ---------------------------------------------------------------------------
# ux_thread_running partial-unique enforcement (SQLite supports partial indexes)
# ---------------------------------------------------------------------------


def _queue_row(message_id: str, status: str, thread_id: str = "t1") -> ThreadMsgQueueRow:
    return ThreadMsgQueueRow(
        thread_id=thread_id,
        message_id=message_id,
        body={"message_id": message_id},
        policy="followup",
        status=status,
        thread_msg_seq=1,
    )


class TestRunningUniqueness:
    @pytest.mark.anyio
    async def test_two_running_rows_same_thread_rejected(self, sf):
        async with sf() as session:
            session.add(_queue_row("m1", "running"))
            await session.commit()
        with pytest.raises(IntegrityError):
            async with sf() as session:
                session.add(_queue_row("m2", "running"))
                await session.commit()

    @pytest.mark.anyio
    async def test_many_pending_rows_same_thread_allowed(self, sf):
        # Partial index only constrains status='running'; pending rows are free.
        async with sf() as session:
            session.add_all([_queue_row(f"p{i}", "pending") for i in range(5)])
            await session.commit()
            count = await session.scalar(
                select(text("count(*)")).select_from(ThreadMsgQueueRow.__table__)
            )
        assert count == 5

    @pytest.mark.anyio
    async def test_running_allowed_on_distinct_threads(self, sf):
        async with sf() as session:
            session.add(_queue_row("a", "running", thread_id="tA"))
            session.add(_queue_row("b", "running", thread_id="tB"))
            await session.commit()  # no conflict across threads
