"""P7 — type=delete (batch thread deletion) end-to-end (design §5.5, MQ协议 v2.6).

In-memory SQLite. Covers the four TDD substeps of the implementation plan:
  1. ingest path (pure DB): schema relax + threads validation + fan-out tombstone +
     pre-staged held ``deleted`` ack (next_delivery_at parked → outbox HOLDS it).
  2. destroy dispatch + DB txn: claim_tombstone (Scheduler 2nd candidate source) +
     destroy_thread_state (drop run-state, release ack, delete tombstone).
  3. running/guard: _finalize SENTINEL branch (no per-message cancelled) + sweep guard.
  4. OSS recycling: OSSClient.delete_prefix batch + best-effort _delete_oss_prefix.

Plus integration: outbox holds the ``deleted`` ack until destroy, then delivers it with
message_seq 1..N sharing the uplink delete message_id; routing via TaskConsumer.
"""

from __future__ import annotations

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.consumer.constants import DELETE_SENTINEL, ProcessedStatus, QueuePolicy, QueueRowStatus, ThreadStatus
from app.consumer.models import (  # noqa: F401  (register tables on Base)
    ConsumerInstanceRow,
    ProcessedMessageRow,
    ThreadMsgQueueRow,
    ThreadRunStateRow,
)
from app.consumer.run_registry import RunRegistry, delete_ack_message_id
from app.consumer.schemas import SchemaValidationError, TaskMessage
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


# ── helpers ────────────────────────────────────────────────────────────────────


async def _state(reg, tid):
    return await reg.get_thread_state(tid)


async def _processed(sf, message_id):
    async with sf() as session:
        return await session.get(ProcessedMessageRow, message_id)


async def _all_processed(sf, tid):
    async with sf() as session:
        return list(
            (
                await session.execute(
                    select(ProcessedMessageRow).where(ProcessedMessageRow.thread_id == tid)
                )
            ).scalars()
        )


async def _queue_rows(sf, tid):
    async with sf() as session:
        return list(
            (
                await session.execute(
                    select(ThreadMsgQueueRow).where(ThreadMsgQueueRow.thread_id == tid)
                )
            ).scalars()
        )


def _delete_envelope(mid, threads, *, client="c1"):
    return {
        "schema_version": "2.6",
        "message_id": mid,
        "type": "delete",
        "clientId": client,
        "payload": {"threads": threads},
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Step 0 — schema (relax thread_id/thread_msg_seq, validate payload.threads)
# ═══════════════════════════════════════════════════════════════════════════════


class TestDeleteSchema:
    def test_delete_without_thread_id_ok(self):
        msg = TaskMessage.from_dict(_delete_envelope("d1", ["t1", "t2"]))
        assert msg.type == "delete"
        assert msg.thread_id == ""  # envelope thread_id exempt (§5.5)
        assert msg.threads == ["t1", "t2"]

    def test_empty_threads_is_invalid_schema(self):
        with pytest.raises(SchemaValidationError):
            TaskMessage.from_dict(_delete_envelope("d1", []))

    def test_missing_threads_is_invalid_schema(self):
        env = {"schema_version": "2.6", "message_id": "d1", "type": "delete", "clientId": "c1", "payload": {}}
        with pytest.raises(SchemaValidationError):
            TaskMessage.from_dict(env)

    def test_non_string_thread_is_invalid_schema(self):
        with pytest.raises(SchemaValidationError):
            TaskMessage.from_dict(_delete_envelope("d1", ["t1", 42]))

    def test_threads_deduped_preserving_order(self):
        msg = TaskMessage.from_dict(_delete_envelope("d1", ["t2", "t1", "t2", "t1"]))
        assert msg.threads == ["t2", "t1"]


# ═══════════════════════════════════════════════════════════════════════════════
# Step 1 — ingest fan-out: tombstone + held ack (pure DB)
# ═══════════════════════════════════════════════════════════════════════════════


class TestIngestDelete:
    @pytest.mark.anyio
    async def test_stamps_sentinel_and_held_ack_per_thread(self, reg, sf):
        n = await reg.ingest_delete(["t1", "t2", "t3"], "d1", {"clientId": "c1"})
        assert n == 3
        for i, tid in enumerate(["t1", "t2", "t3"], start=1):
            st = await _state(reg, tid)
            assert st is not None
            assert st.cancel_watermark == DELETE_SENTINEL  # tombstone barrier
            ack = await _processed(sf, delete_ack_message_id("d1", tid))
            assert ack is not None
            assert ack.status == ProcessedStatus.DELETED.value
            assert ack.delivered is False
            assert ack.next_delivery_at is not None  # HELD far-future, not yet deliverable
            echo = ack.result_cache["echo"]
            assert echo["message_id"] == "d1"  # real downlink id shared by all N
            assert echo["thread_id"] == tid
            assert echo["thread_msg_seq"] == 0
            assert echo["message_seq"] == i  # 1..N

    @pytest.mark.anyio
    async def test_held_ack_not_picked_by_outbox_before_destroy(self, reg):
        await reg.ingest_delete(["t1"], "d1", {})
        undelivered = await reg.fetch_undelivered()
        assert undelivered == []  # parked far-future → outbox skips it

    @pytest.mark.anyio
    async def test_redelivered_delete_is_idempotent(self, reg, sf):
        await reg.ingest_delete(["t1"], "d1", {})
        await reg.ingest_delete(["t1"], "d1", {})  # redelivery
        st = await _state(reg, "t1")
        assert st.cancel_watermark == DELETE_SENTINEL
        acks = await _all_processed(sf, "t1")
        assert len(acks) == 1  # ON CONFLICT — no duplicate ack

    @pytest.mark.anyio
    async def test_sentinel_dominates_existing_cancel(self, reg):
        await reg.fold_cancel_watermark("t1", 7)  # prior real cancel barrier
        await reg.ingest_delete(["t1"], "d1", {})
        st = await _state(reg, "t1")
        assert st.cancel_watermark == DELETE_SENTINEL  # GREATEST(7, SENTINEL)


# ═══════════════════════════════════════════════════════════════════════════════
# Step 2 — destroy dispatch (claim_tombstone) + DB txn (destroy_thread_state)
# ═══════════════════════════════════════════════════════════════════════════════


class TestClaimTombstone:
    @pytest.mark.anyio
    async def test_claims_idle_sentinel_and_flips_running(self, reg, sf):
        await reg.ingest_delete(["t1"], "d1", {})
        tid = await reg.claim_tombstone("inst-1")
        assert tid == "t1"
        st = await _state(reg, "t1")
        assert st.status == ThreadStatus.RUNNING.value  # durable claim
        assert st.instance_id == "inst-1"
        assert st.cancel_watermark == DELETE_SENTINEL  # still tombstoned

    @pytest.mark.anyio
    async def test_none_when_no_tombstone(self, reg):
        assert await reg.claim_tombstone("inst-1") is None

    @pytest.mark.anyio
    async def test_claims_paused_tombstone(self, reg, sf):
        async with sf() as session:
            async with session.begin():
                session.add(
                    ThreadRunStateRow(
                        thread_id="t1",
                        status=ThreadStatus.PAUSED.value,
                        cancel_watermark=DELETE_SENTINEL,
                        message_id="m0",
                    )
                )
        assert await reg.claim_tombstone("inst-1") == "t1"

    @pytest.mark.anyio
    async def test_running_tombstone_not_claimed_by_sweep(self, reg, sf):
        # running tombstone is handled inline by its own run's finalize→destroy, not the sweep
        async with sf() as session:
            async with session.begin():
                session.add(
                    ThreadRunStateRow(
                        thread_id="t1",
                        status=ThreadStatus.RUNNING.value,
                        cancel_watermark=DELETE_SENTINEL,
                        message_id="m0",
                        instance_id="inst-x",
                    )
                )
        assert await reg.claim_tombstone("inst-1") is None


class TestDestroyThreadState:
    @pytest.mark.anyio
    async def test_destroy_clears_state_releases_ack_deletes_tombstone(self, reg, sf):
        await reg.ingest_delete(["t1"], "d1", {})
        # add some unrelated run-state rows to prove they get wiped (queue + processed).
        await reg.enqueue_message("t1", "m1", {"message_id": "m1"}, 1, str(QueuePolicy.FOLLOWUP))
        await reg.mark_processed("m1", "t1", ProcessedStatus.COMPLETED.value, {"x": 1})

        ok = await reg.destroy_thread_state("t1")
        assert ok is True
        assert await _state(reg, "t1") is None  # tombstone row deleted
        assert await _queue_rows(sf, "t1") == []  # queue wiped
        # only the deleted ack survives, now RELEASED (next_delivery_at NULL).
        rows = await _all_processed(sf, "t1")
        assert len(rows) == 1
        ack = rows[0]
        assert ack.status == ProcessedStatus.DELETED.value
        assert ack.next_delivery_at is None  # released → outbox delivers

    @pytest.mark.anyio
    async def test_released_ack_now_visible_to_outbox(self, reg):
        await reg.ingest_delete(["t1"], "d1", {})
        await reg.destroy_thread_state("t1")
        undelivered = await reg.fetch_undelivered()
        assert [r.message_id for r in undelivered] == [delete_ack_message_id("d1", "t1")]

    @pytest.mark.anyio
    async def test_destroy_idempotent_second_call_noop(self, reg):
        await reg.ingest_delete(["t1"], "d1", {})
        assert await reg.destroy_thread_state("t1") is True
        assert await reg.destroy_thread_state("t1") is False  # tombstone gone → no-op

    @pytest.mark.anyio
    async def test_destroy_noop_on_non_tombstone(self, reg):
        await reg.fold_cancel_watermark("t1", 5)  # ordinary cancel, not a tombstone
        assert await reg.destroy_thread_state("t1") is False

    @pytest.mark.anyio
    async def test_is_tombstoned(self, reg):
        await reg.ingest_delete(["t1"], "d1", {})
        assert await reg.is_tombstoned("t1") is True
        assert await reg.is_tombstoned("nope") is False


# ═══════════════════════════════════════════════════════════════════════════════
# Step 3 — guards: no per-message cancelled spray; finalize SENTINEL branch
# ═══════════════════════════════════════════════════════════════════════════════


class TestSweepGuard:
    @pytest.mark.anyio
    async def test_sweep_cancelled_skips_sentinel_thread(self, reg, sf):
        # pending rows under a SENTINEL watermark must NOT each get a `cancelled` terminal.
        await reg.enqueue_message("t1", "m1", {"message_id": "m1"}, 1, str(QueuePolicy.FOLLOWUP))
        await reg.enqueue_message("t1", "m2", {"message_id": "m2"}, 2, str(QueuePolicy.FOLLOWUP))
        await reg.ingest_delete(["t1"], "d1", {})
        swept = await reg.sweep_cancelled()
        assert swept == 0  # SENTINEL guard
        # no per-message cancelled rows written
        assert await _processed(sf, "m1") is None
        assert await _processed(sf, "m2") is None


class TestFinalizeSentinelBranch:
    @pytest.mark.anyio
    async def test_running_tombstone_finalize_emits_no_per_message_terminal(self, reg, sf):
        # simulate a running thread that got delete-tombstoned, then finalize(cancelled).
        async with sf() as session:
            async with session.begin():
                session.add(
                    ThreadMsgQueueRow(
                        thread_id="t1",
                        message_id="m1",
                        body={"message_id": "m1"},
                        policy=QueuePolicy.FOLLOWUP.value,
                        status=QueueRowStatus.RUNNING.value,
                        thread_msg_seq=1,
                    )
                )
                session.add(
                    ThreadRunStateRow(
                        thread_id="t1",
                        status=ThreadStatus.RUNNING.value,
                        message_id="m1",
                        cancel_watermark=DELETE_SENTINEL,
                        last_resolved_seq=1,
                        instance_id="inst-1",
                    )
                )
        closed = await reg.finalize_run("m1", {"status": "cancelled"}, str(ProcessedStatus.CANCELLED))
        assert closed is True
        assert await _processed(sf, "m1") is None  # NO per-message cancelled ack
        assert await _queue_rows(sf, "t1") == []  # running row dropped
        st = await _state(reg, "t1")
        assert st.status == ThreadStatus.IDLE.value
        assert st.cancel_watermark == DELETE_SENTINEL  # still tombstoned → destroy reclaims
        assert await reg.is_tombstoned("t1") is True


# ═══════════════════════════════════════════════════════════════════════════════
# Step 4 — OSS recycling (best-effort prefix batch delete)
# ═══════════════════════════════════════════════════════════════════════════════


class _FakeObj:
    def __init__(self, key):
        self.key = key


class _FakeListing:
    def __init__(self, contents, truncated=False, token=None):
        self.contents = contents
        self.is_truncated = truncated
        self.next_continuation_token = token


class _FakeOSSLib:
    """Minimal stand-in for the alibabacloud_oss_v2 module surface delete_prefix uses."""

    def ListObjectsV2Request(self, **kw):  # noqa: N802
        return kw

    def DeleteMultipleObjectsRequest(self, **kw):  # noqa: N802
        return kw

    def DeleteObject(self, key):  # noqa: N802
        return {"key": key}


class _FakeRawClient:
    def __init__(self, pages):
        self._pages = list(pages)
        self.deleted: list[str] = []

    def list_objects_v2(self, req):
        return self._pages.pop(0)

    def delete_multiple_objects(self, req):
        self.deleted.extend(o["key"] for o in req["objects"])


def _make_oss_client(pages):
    from deerflow.oss.client import OSSClient

    c = object.__new__(OSSClient)
    c._oss = _FakeOSSLib()
    c._bucket = "b"
    c._client = _FakeRawClient(pages)
    return c


class TestOSSDeletePrefix:
    def test_deletes_all_objects_under_prefix_paginated(self):
        pages = [
            _FakeListing([_FakeObj("agent-artifacts/t1/a"), _FakeObj("agent-artifacts/t1/b")], truncated=True, token="tok"),
            _FakeListing([_FakeObj("agent-artifacts/t1/c")], truncated=False),
        ]
        c = _make_oss_client(pages)
        n = c.delete_prefix("agent-artifacts/t1/")
        assert n == 3
        assert c._client.deleted == ["agent-artifacts/t1/a", "agent-artifacts/t1/b", "agent-artifacts/t1/c"]

    def test_empty_prefix_is_noop(self):
        c = _make_oss_client([_FakeListing([], truncated=False)])
        assert c.delete_prefix("agent-artifacts/t1/") == 0
        assert c._client.deleted == []


class TestAgentRunnerOSSPrelude:
    @pytest.mark.anyio
    async def test_oss_off_is_noop(self, reg, monkeypatch):
        from app.consumer.agent_runner import AgentRunner

        monkeypatch.setattr("deerflow.oss.client.get_oss_client", lambda: None)
        runner = AgentRunner(reg, _StubBridge(), checkpointer=None)
        await runner._delete_oss_prefix("t1")  # no client → silent no-op

    @pytest.mark.anyio
    async def test_oss_failure_is_swallowed(self, reg, monkeypatch):
        from app.consumer.agent_runner import AgentRunner

        class _Boom:
            def delete_prefix(self, prefix):
                raise RuntimeError("oss down")

        monkeypatch.setattr("deerflow.oss.client.get_oss_client", lambda: _Boom())
        runner = AgentRunner(reg, _StubBridge(), checkpointer=None)
        # best-effort旁路: must NOT raise (local destroy + ack must still proceed).
        await runner._delete_oss_prefix("t1")


# ═══════════════════════════════════════════════════════════════════════════════
# Integration — routing (TaskConsumer), held→released outbox delivery, sweep dispatch
# ═══════════════════════════════════════════════════════════════════════════════


class _StubScheduler:
    def __init__(self):
        self.pokes = 0

    def poke(self):
        self.pokes += 1


class _StubBridge:
    def __init__(self):
        self.sent: list = []

    async def replay(self, result_cache, *, echo=None):
        self.sent.append((result_cache, echo))


class TestTaskConsumerRouting:
    @pytest.mark.anyio
    async def test_delete_routes_to_ingest_and_pokes(self, reg, sf):
        import json

        sched = _StubScheduler()
        tc = TaskConsumer(reg, _StubBridge(), "inst-1", scheduler=sched)
        await tc.handle_message(json.dumps(_delete_envelope("d1", ["t1", "t2"])))
        assert sched.pokes == 1
        assert await reg.is_tombstoned("t1")
        assert await reg.is_tombstoned("t2")
        ack = await _processed(sf, delete_ack_message_id("d1", "t2"))
        assert ack is not None and ack.result_cache["echo"]["message_seq"] == 2

    @pytest.mark.anyio
    async def test_empty_threads_published_as_invalid_schema(self, reg):
        import json

        class _ErrBridge(_StubBridge):
            def __init__(self):
                super().__init__()
                self.errors = []

            async def publish_error(self, code, **kw):
                self.errors.append(code)

        bridge = _ErrBridge()
        tc = TaskConsumer(reg, bridge, "inst-1", scheduler=_StubScheduler())
        await tc.handle_message(json.dumps(_delete_envelope("d1", [])))
        assert "INVALID_SCHEMA" in bridge.errors


class TestOutboxDeliversDeletedAck:
    @pytest.mark.anyio
    async def test_held_then_released_delivers_deleted_with_per_thread_seq(self, reg, sf):
        from app.consumer.outbox import OutboxProducer

        bridge = _StubBridge()
        outbox = OutboxProducer(reg, bridge, poll_interval=0.01)

        await reg.ingest_delete(["t1", "t2"], "d1", {"clientId": "c1"})
        # held: outbox sees nothing yet (the deleted ack is parked far-future).
        assert await outbox.drain_once() == 0
        assert bridge.sent == []

        # destroy releases both acks → outbox now delivers them.
        await reg.destroy_thread_state("t1")
        await reg.destroy_thread_state("t2")
        delivered = await outbox.drain_once()
        assert delivered == 2
        seqs = sorted(echo["message_seq"] for _rc, echo in bridge.sent)
        assert seqs == [1, 2]
        for rc, echo in bridge.sent:
            assert rc["type"] == "deleted"
            assert echo["message_id"] == "d1"  # shared uplink id


class _StubRunner:
    def __init__(self):
        self.destroyed: list[str] = []

    async def destroy(self, thread_id):
        self.destroyed.append(thread_id)


class TestSchedulerTombstoneSweep:
    @pytest.mark.anyio
    async def test_drain_tombstones_dispatches_destroy(self, reg, sf):
        import asyncio

        from app.consumer.scheduler import Scheduler

        await reg.ingest_delete(["t1"], "d1", {})
        runner = _StubRunner()
        sched = Scheduler(reg, runner, "inst-1", max_concurrent_runs=4, tick_interval=10)
        await sched._drain_tombstones()
        # destroy task is fired; let it run.
        await asyncio.sleep(0.05)
        assert runner.destroyed == ["t1"]
        # claim flipped it to running so a second sweep finds nothing more.
        await sched._drain_tombstones()
        await asyncio.sleep(0.01)
        assert runner.destroyed == ["t1"]
