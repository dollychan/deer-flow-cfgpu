"""Phase C — TaskConsumer v2 ingest layer (design §2.6).

Ingest-only: lands rows / folds cancel watermarks / pokes the Scheduler. It owns no
claim / dispatch. Uses a real in-memory SQLite RunRegistry plus a stub Scheduler that
records poke() calls and a mock bridge.
"""

from __future__ import annotations

import json

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.consumer.constants import QueuePolicy, QueueRowStatus, ThreadStatus
from app.consumer.models import (  # noqa: F401  (register tables on Base)
    ConsumerInstanceRow,
    ProcessedMessageRow,
    ThreadMsgQueueRow,
    ThreadRunStateRow,
)
from app.consumer.run_registry import RunRegistry
from app.consumer.task_consumer import TaskConsumer
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


class _StubScheduler:
    def __init__(self):
        self.pokes = 0

    def poke(self):
        self.pokes += 1


class _StubBridge:
    def __init__(self):
        self.errors = []
        self.replays = []
        self.pongs = []

    async def publish_error(self, code, **kw):
        self.errors.append((code, kw))

    async def replay(self, result_cache, **kw):
        self.replays.append((result_cache, kw))

    async def publish_pong(self, instance_id, **kw):
        self.pongs.append((instance_id, kw))


@pytest.fixture
def consumer(reg):
    sched = _StubScheduler()
    bridge = _StubBridge()
    tc = TaskConsumer(reg, bridge, "inst-1", scheduler=sched)
    return tc, reg, sched, bridge


def _task(mid, tid="t1", seq=1, *, mode=None, command=None, fork=None):
    config: dict = {}
    if mode:
        config["message_mode"] = mode
    if fork:
        config["fork"] = fork
    payload: dict = {"config": config, "reply_config": {"stream_events": True}}
    if command is not None:
        payload["command"] = command
    else:
        payload["messages"] = [{"role": "user", "content": "hi"}]
    return {
        "schema_version": "2.5",
        "message_id": mid,
        "type": "task",
        "thread_id": tid,
        "thread_msg_seq": seq,
        "clientId": "c1",
        "payload": payload,
    }


def _ping(mid, *, target=None):
    payload: dict = {"instance_id": "host-1"}
    if target is not None:
        payload["instance_id"] = target  # targeted ping nests instance_id under payload
    return {
        "schema_version": "2.5",
        "message_id": mid,
        "type": "ping",
        "clientId": "c1",
        "payload": payload,
    }


def _cancel(mid, tid="t1", seq=5):
    return {
        "schema_version": "2.5",
        "message_id": mid,
        "type": "cancel",
        "thread_id": tid,
        "thread_msg_seq": seq,
        "clientId": "c1",
        "payload": {},
    }


class TestIngestTask:
    @pytest.mark.anyio
    async def test_enqueue_followup_and_poke(self, consumer, sf):
        tc, reg, sched, _ = consumer
        await tc.handle_message(json.dumps(_task("m1")).encode())
        async with sf() as session:
            rows = (await session.execute(ThreadMsgQueueRow.__table__.select())).all()
        assert len(rows) == 1
        assert sched.pokes == 1
        st = await reg.get_thread_state("t1")
        assert st is None  # ingest does NOT claim/dispatch (§2.6)

    @pytest.mark.anyio
    async def test_derived_policy_resume(self, consumer):
        tc, reg, sched, _ = consumer
        cmd = {"update": {"tool_approvals": {"x": {"status": "approved"}}}}
        await tc.handle_message(json.dumps(_task("m1", command=cmd)).encode())
        row = await reg.get_running_row("t1")  # not running
        assert row is None
        st = await reg.peek_thread_queue("t1", policies=(QueuePolicy.RESUME,))
        assert len(st) == 1 and st[0].policy == QueuePolicy.RESUME

    @pytest.mark.anyio
    async def test_derived_policy_fork(self, consumer):
        tc, reg, sched, _ = consumer
        fork = {"parent_thread_id": "parent"}
        cmd = {"update": {"tool_approvals": {"x": {"status": "approved"}}}}
        await tc.handle_message(json.dumps(_task("m1", command=cmd, fork=fork)).encode())
        rows = await reg.peek_thread_queue("t1", policies=(QueuePolicy.FORK,))
        assert len(rows) == 1  # fork wins over command (I4)

    @pytest.mark.anyio
    async def test_duplicate_replays_cached_result(self, consumer):
        tc, reg, sched, bridge = consumer
        await reg.mark_processed("m1", "t1", "completed", {"status": "success"})
        await tc.handle_message(json.dumps(_task("m1")).encode())
        assert len(bridge.replays) == 1
        # no enqueue, no poke for the duplicate
        assert sched.pokes == 0

    @pytest.mark.anyio
    async def test_reject_mode_on_running_thread(self, consumer, sf):
        tc, reg, sched, bridge = consumer
        async with sf() as session:
            session.add(ThreadRunStateRow(thread_id="t1", instance_id="i", message_id="r", status=ThreadStatus.RUNNING))
            await session.commit()
        await tc.handle_message(json.dumps(_task("m1", mode="reject")).encode())
        assert bridge.errors and bridge.errors[0][0] == "AGENT_BUSY"

    @pytest.mark.anyio
    async def test_duplicate_enqueue_skips_second(self, consumer):
        tc, reg, sched, _ = consumer
        await tc.handle_message(json.dumps(_task("m1")).encode())
        await tc.handle_message(json.dumps(_task("m1")).encode())
        rows = await reg.peek_thread_queue("t1", policies=(QueuePolicy.FOLLOWUP,))
        assert len(rows) == 1  # ON CONFLICT dedup
        assert sched.pokes == 1  # only first enqueue pokes


class TestIngestCancel:
    @pytest.mark.anyio
    async def test_cancel_folds_watermark_not_enqueued(self, consumer, sf):
        tc, reg, sched, _ = consumer
        await tc.handle_message(json.dumps(_cancel("c1", seq=7)).encode())
        st = await reg.get_thread_state("t1")
        assert st.cancel_watermark == 7
        async with sf() as session:
            rows = (await session.execute(ThreadMsgQueueRow.__table__.select())).all()
        assert rows == []  # cancel is fire-and-forget, not enqueued (D2)
        assert sched.pokes == 1

    @pytest.mark.anyio
    async def test_targeted_pong_last_heartbeat_is_beijing_format(self, consumer):
        import re

        tc, reg, _, bridge = consumer
        await reg.register_instance("inst-2", "host", 123)  # writes last_heartbeat=now(UTC)
        await tc.handle_message(json.dumps(_ping("p1", target="inst-2")).encode())

        assert len(bridge.pongs) == 1
        _, kw = bridge.pongs[0]
        # Beijing wall-clock: space sep, 3-digit millis, no 'Z'/offset suffix.
        assert re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$", kw["last_heartbeat"])
        assert kw["target_status"] == "active"

    @pytest.mark.anyio
    async def test_cancel_clearing_gate_synthesizes_drain(self, consumer, sf):
        tc, reg, sched, _ = consumer
        async with sf() as session:
            session.add(ThreadRunStateRow(
                thread_id="t1", instance_id="i", message_id="run3",
                status=ThreadStatus.PAUSED, last_resolved_seq=3, cancel_watermark=0,
            ))
            await session.commit()
        await tc.handle_message(json.dumps(_cancel("c1", seq=5)).encode())
        rows = await reg.peek_thread_queue("t1", policies=(QueuePolicy.DRAIN,))
        assert len(rows) == 1 and rows[0].message_id == "run3:drain"
        assert rows[0].status == QueueRowStatus.PENDING
        st = await reg.get_thread_state("t1")
        assert st.status == ThreadStatus.IDLE
