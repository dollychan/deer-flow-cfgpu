"""Phase E — OutboxProducer reliable terminal-result delivery (design §2.8/§9.3).

Real in-memory SQLite RunRegistry (so the outbox read/mark/backoff methods run for
real) + a fake bridge recording replay calls (optionally failing). Covers: undelivered
rows replayed + marked delivered, cancel-barrier rows shaped to result(cancelled),
publish failure → exponential backoff (not re-fetched within the window), poison
threshold logging, and run_loop stop.
"""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.consumer.constants import ProcessedStatus
from app.consumer.models import (  # noqa: F401  (register tables on Base)
    ConsumerInstanceRow,
    ProcessedMessageRow,
    ThreadMsgQueueRow,
    ThreadRunStateRow,
)
from app.consumer.outbox import OutboxProducer
from app.consumer.run_registry import RunRegistry
from deerflow.persistence.base import Base


@pytest.fixture
async def sf():
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


@pytest.fixture
def reg(sf):
    return RunRegistry(sf)


class _FakeBridge:
    def __init__(self, *, fail: bool = False):
        self.replays: list[tuple[dict, dict | None]] = []
        self.fail = fail

    async def replay(self, result_cache, *, echo=None):
        self.replays.append((result_cache, echo))
        if self.fail:
            raise RuntimeError("mq down")


# ── happy path ─────────────────────────────────────────────────────────────────


class TestDrainOnce:
    @pytest.mark.anyio
    async def test_delivers_and_marks(self, reg):
        await reg.mark_processed_undelivered(
            "m1", "t1", ProcessedStatus.COMPLETED.value,
            {"status": "success", "echo": {"message_id": "m1", "thread_id": "t1", "thread_msg_seq": 7}},
        )
        bridge = _FakeBridge()
        producer = OutboxProducer(reg, bridge)
        delivered = await producer.drain_once()
        assert delivered == 1
        cache, echo = bridge.replays[0]
        assert cache["status"] == "success"
        assert echo["thread_msg_seq"] == 7  # echo rebuilt from result_cache
        proc = await reg.check_processed("m1")
        assert proc.delivered is True

    @pytest.mark.anyio
    async def test_already_delivered_not_refetched(self, reg):
        await reg.mark_processed_undelivered("m1", "t1", ProcessedStatus.COMPLETED.value, {"status": "success"})
        bridge = _FakeBridge()
        producer = OutboxProducer(reg, bridge)
        assert await producer.drain_once() == 1
        assert await producer.drain_once() == 0  # nothing left undelivered
        assert len(bridge.replays) == 1

    @pytest.mark.anyio
    async def test_cancel_barrier_row_shaped_to_cancelled(self, reg):
        # cancel-barrier writes result_cache=None, status='cancelled' (Phase B): the
        # producer shapes it into a result(cancelled) with a minimal echo (§2.8 mapping).
        await reg.mark_processed_undelivered("m9", "t1", ProcessedStatus.CANCELLED.value, None)
        bridge = _FakeBridge()
        producer = OutboxProducer(reg, bridge)
        assert await producer.drain_once() == 1
        cache, echo = bridge.replays[0]
        assert cache == {"status": "cancelled"}
        assert echo == {"message_id": "m9", "thread_id": "t1"}
        assert (await reg.check_processed("m9")).delivered is True


# ── failure / backoff ──────────────────────────────────────────────────────────


class TestBackoff:
    @pytest.mark.anyio
    async def test_publish_failure_backs_off_and_keeps_undelivered(self, reg):
        await reg.mark_processed_undelivered("m1", "t1", ProcessedStatus.COMPLETED.value, {"status": "success"})
        producer = OutboxProducer(reg, _FakeBridge(fail=True))
        assert await producer.drain_once() == 0  # publish failed
        proc = await reg.check_processed("m1")
        assert proc.delivered is False
        assert proc.delivery_attempts == 1
        assert proc.next_delivery_at is not None  # backoff window set
        # within the backoff window the row is not eligible → a second pass fetches nothing
        assert await producer.drain_once() == 0
        assert (await reg.check_processed("m1")).delivery_attempts == 1  # not bumped again

    @pytest.mark.anyio
    async def test_poison_threshold_logged(self, reg, caplog):
        await reg.mark_processed_undelivered("p1", "t1", ProcessedStatus.COMPLETED.value, {"status": "success"})
        producer = OutboxProducer(reg, _FakeBridge(fail=True), poison_threshold=1)
        with caplog.at_level("ERROR"):
            await producer.drain_once()
        assert any("poison" in r.message.lower() for r in caplog.records)
        assert (await reg.check_processed("p1")).delivered is False  # never silently dropped


# ── loop control ───────────────────────────────────────────────────────────────


class TestRunLoop:
    @pytest.mark.anyio
    async def test_run_loop_drains_then_stops(self, reg):
        await reg.mark_processed_undelivered("m1", "t1", ProcessedStatus.COMPLETED.value, {"status": "success"})
        bridge = _FakeBridge()
        producer = OutboxProducer(reg, bridge, poll_interval=0.01)
        stop = asyncio.Event()
        task = asyncio.create_task(producer.run_loop(stop))
        await asyncio.sleep(0.05)
        stop.set()
        await asyncio.wait_for(task, timeout=1)
        assert (await reg.check_processed("m1")).delivered is True
        assert len(bridge.replays) >= 1
