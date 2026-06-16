"""Phase B — RunRegistry v2 scheduler primitives (design §6.3/6.4/7.3/9.3).

In-memory SQLite. Covers fold_cancel_watermark (B1), claim_next_runnable two-phase
+ collect/prefix merge (B2), finalize_run / finalize_paused (B3), and the outbox
method group + retention rule (B4). Asserts invariants I2/I5/I8/I10/I13.
"""

from __future__ import annotations

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.consumer.constants import ProcessedStatus, QueuePolicy, QueueRowStatus, ThreadStatus
from app.consumer.models import (  # noqa: F401  (register tables on Base)
    ConsumerInstanceRow,
    ProcessedMessageRow,
    ThreadMsgQueueRow,
    ThreadRunStateRow,
)
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


# ── helpers ────────────────────────────────────────────────────────────────────


async def _enqueue(reg, thread_id, mid, seq, policy=QueuePolicy.FOLLOWUP, body=None):
    await reg.enqueue_message(
        thread_id=thread_id,
        message_id=mid,
        body=body or {"message_id": mid, "seq": seq},
        thread_msg_seq=seq,
        policy=str(policy),
    )


async def _state(reg, thread_id):
    return await reg.get_thread_state(thread_id)


async def _queue_rows(sf, thread_id):
    async with sf() as session:
        rows = await session.execute(
            select(ThreadMsgQueueRow)
            .where(ThreadMsgQueueRow.thread_id == thread_id)
            .order_by(ThreadMsgQueueRow.thread_msg_seq.asc())
        )
        return list(rows.scalars())


async def _processed(sf, message_id):
    async with sf() as session:
        return (
            await session.execute(
                select(ProcessedMessageRow).where(ProcessedMessageRow.message_id == message_id)
            )
        ).scalar_one_or_none()


# ═══════════════════════════════════════════════════════════════════════════════
# B1 — fold_cancel_watermark (§6.4)
# ═══════════════════════════════════════════════════════════════════════════════


class TestFoldCancelWatermark:
    @pytest.mark.anyio
    async def test_creates_state_and_sets_high_water(self, reg):
        await reg.fold_cancel_watermark("t1", 5)
        st = await _state(reg, "t1")
        assert st is not None
        assert st.cancel_watermark == 5
        assert st.last_resolved_seq == 5  # L2 last_seq advances too (§6.4)
        assert st.status == ThreadStatus.IDLE

    @pytest.mark.anyio
    async def test_greatest_monotonic(self, reg):
        await reg.fold_cancel_watermark("t1", 10)
        await reg.fold_cancel_watermark("t1", 4)  # smaller, must not lower
        st = await _state(reg, "t1")
        assert st.cancel_watermark == 10
        assert st.last_resolved_seq == 10

    @pytest.mark.anyio
    async def test_paused_gate_cleared_when_covered(self, reg, sf):
        # paused run at seq P=3; cancel N=5 covers it → paused→idle.
        async with sf() as session:
            session.add(
                ThreadRunStateRow(
                    thread_id="t1",
                    instance_id="i1",
                    message_id="run3",
                    status=ThreadStatus.PAUSED,
                    last_resolved_seq=3,
                    cancel_watermark=0,
                )
            )
            await session.commit()
        await reg.fold_cancel_watermark("t1", 5)
        st = await _state(reg, "t1")
        assert st.status == ThreadStatus.IDLE
        assert st.cancel_watermark == 5

    @pytest.mark.anyio
    async def test_paused_gate_kept_when_not_covered(self, reg, sf):
        # late small-seq cancel N=2 does NOT cover the paused run at P=3 → stays paused.
        async with sf() as session:
            session.add(
                ThreadRunStateRow(
                    thread_id="t1",
                    instance_id="i1",
                    message_id="run3",
                    status=ThreadStatus.PAUSED,
                    last_resolved_seq=3,
                    cancel_watermark=0,
                )
            )
            await session.commit()
        await reg.fold_cancel_watermark("t1", 2)
        st = await _state(reg, "t1")
        assert st.status == ThreadStatus.PAUSED  # gate held
        assert st.cancel_watermark == 2


# ═══════════════════════════════════════════════════════════════════════════════
# B1b — fold_cancel_watermark idle ack (§6.4): cancel on an idle thread synthesizes
#       a result(cancelled, last_resolved_seq) terminal so the client never blocks.
# ═══════════════════════════════════════════════════════════════════════════════


class TestFoldIdleAck:
    _echo = {"message_id": "cx", "thread_id": "t1", "thread_msg_seq": 5}

    @pytest.mark.anyio
    async def test_idle_live_cancel_synthesizes_cancelled_terminal(self, reg, sf):
        # Fresh idle thread (old_lrs=0); cancel seq=5 > 0 → idle ack keyed on cancel message_id.
        out = await reg.fold_cancel_watermark("t1", 5, cancel_message_id="cx", echo=self._echo)
        assert out.idle_ack_synthesized is True
        assert out.drain_synthesized is False
        row = await _processed(sf, "cx")
        assert row is not None
        assert row.status == ProcessedStatus.CANCELLED
        assert row.delivered is False  # outbox delivers it
        assert row.result_cache["status"] == "cancelled"
        assert row.result_cache["last_resolved_seq"] == 0  # OLD lrs, before this fold advances it
        assert row.result_cache["echo"]["message_id"] == "cx"

    @pytest.mark.anyio
    async def test_redundant_cancel_below_old_lrs_no_ack(self, reg, sf):
        # A prior cancel @10 advances lrs to 10; a later smaller cancel @8 (8 <= old_lrs) → no ack.
        await reg.fold_cancel_watermark("t1", 10, cancel_message_id="c1", echo=self._echo)
        out = await reg.fold_cancel_watermark("t1", 8, cancel_message_id="c2", echo=self._echo)
        assert out.idle_ack_synthesized is False
        assert await _processed(sf, "c2") is None

    @pytest.mark.anyio
    async def test_cancel_seq_equal_old_lrs_no_ack(self, reg, sf):
        # Boundary: cancel_seq == old_lrs is still fully-covered-already → no ack (skip on <=).
        await reg.fold_cancel_watermark("t1", 5, cancel_message_id="c1", echo=self._echo)  # lrs→5
        out = await reg.fold_cancel_watermark("t1", 5, cancel_message_id="c2", echo=self._echo)
        assert out.idle_ack_synthesized is False
        assert await _processed(sf, "c2") is None

    @pytest.mark.anyio
    async def test_redelivered_cancel_idempotent(self, reg, sf):
        # Same cancel message_id redelivered: first writes ack (5>0); second has old_lrs=5 so 5>5
        # is false → no second attempt, and ON CONFLICT would keep one row regardless.
        await reg.fold_cancel_watermark("t1", 5, cancel_message_id="cx", echo=self._echo)
        out = await reg.fold_cancel_watermark("t1", 5, cancel_message_id="cx", echo=self._echo)
        assert out.idle_ack_synthesized is False
        async with sf() as session:
            n = (
                await session.execute(
                    select(ProcessedMessageRow).where(ProcessedMessageRow.message_id == "cx")
                )
            ).scalars().all()
        assert len(n) == 1

    @pytest.mark.anyio
    async def test_running_thread_no_idle_ack(self, reg, sf):
        # Running thread → the watcher path produces the cancelled terminal, not the idle ack.
        async with sf() as session:
            session.add(
                ThreadRunStateRow(
                    thread_id="t1", instance_id="i1", message_id="run1",
                    status=ThreadStatus.RUNNING, last_resolved_seq=1, cancel_watermark=0,
                )
            )
            await session.commit()
        out = await reg.fold_cancel_watermark("t1", 5, cancel_message_id="cx", echo=self._echo)
        assert out.idle_ack_synthesized is False
        assert await _processed(sf, "cx") is None

    @pytest.mark.anyio
    async def test_paused_covered_does_drain_not_idle_ack(self, reg, sf):
        # paused+covered → drain branch; idle ack must NOT also fire.
        async with sf() as session:
            session.add(
                ThreadRunStateRow(
                    thread_id="t1", instance_id="i1", message_id="run3",
                    status=ThreadStatus.PAUSED, last_resolved_seq=3, cancel_watermark=0,
                )
            )
            await session.commit()
        out = await reg.fold_cancel_watermark("t1", 5, cancel_message_id="cx", echo=self._echo)
        assert out.drain_synthesized is True
        assert out.idle_ack_synthesized is False
        assert await _processed(sf, "cx") is None

    @pytest.mark.anyio
    async def test_no_message_id_skips_ack(self, reg, sf):
        # Backward-compatible call (no cancel_message_id) never synthesizes an idle ack.
        out = await reg.fold_cancel_watermark("t1", 5)
        assert out.idle_ack_synthesized is False
        assert out.drain_synthesized is False


# ═══════════════════════════════════════════════════════════════════════════════
# B2 — claim_next_runnable (§6.3)
# ═══════════════════════════════════════════════════════════════════════════════


class TestClaimBasic:
    @pytest.mark.anyio
    async def test_claim_single_followup(self, reg):
        await _enqueue(reg, "t1", "m1", 1)
        claimed = await reg.claim_next_runnable("inst-A")
        assert claimed is not None
        assert claimed.message_id == "m1"
        assert claimed.policy == QueuePolicy.FOLLOWUP
        st = await _state(reg, "t1")
        assert st.status == ThreadStatus.RUNNING
        assert st.instance_id == "inst-A"
        assert st.last_resolved_seq == 1  # I2

    @pytest.mark.anyio
    async def test_empty_returns_none(self, reg):
        assert await reg.claim_next_runnable("inst-A") is None

    @pytest.mark.anyio
    async def test_in_order_earliest_first(self, reg):
        await _enqueue(reg, "t1", "m1", 1)
        await _enqueue(reg, "t1", "m2", 2)
        claimed = await reg.claim_next_runnable("inst-A")
        assert claimed.message_id == "m1"  # earliest seq wins

    @pytest.mark.anyio
    async def test_one_running_per_thread(self, reg, sf):
        # I8: after claiming, a second claim on the same thread yields nothing
        # (thread is running; its other pending row is blocked).
        await _enqueue(reg, "t1", "m1", 1)
        await _enqueue(reg, "t1", "m2", 2)
        await reg.claim_next_runnable("inst-A")
        assert await reg.claim_next_runnable("inst-B") is None
        running = [r for r in await _queue_rows(sf, "t1") if r.status == QueueRowStatus.RUNNING]
        assert len(running) == 1

    @pytest.mark.anyio
    async def test_distinct_threads_both_claimable(self, reg):
        await _enqueue(reg, "tA", "a1", 1)
        await _enqueue(reg, "tB", "b1", 1)
        first = await reg.claim_next_runnable("inst-A")
        second = await reg.claim_next_runnable("inst-A")
        assert {first.thread_id, second.thread_id} == {"tA", "tB"}


class TestClaimWatermark:
    @pytest.mark.anyio
    async def test_covered_row_not_claimed_then_tick_sweeps_to_prefix(self, reg, sf):
        # followup seq=1 then cancel N=5 → seq<5 covered. With no executable candidate,
        # claim does NOT sweep (§6.4); the periodic tick fallback sweep does.
        await _enqueue(reg, "t1", "m1", 1)
        await reg.fold_cancel_watermark("t1", 5)
        assert await reg.claim_next_runnable("inst-A") is None  # nothing executable
        assert (await _queue_rows(sf, "t1"))[0].policy == QueuePolicy.FOLLOWUP  # still pending

        swept = await reg.sweep_cancelled("t1")  # tick fallback
        assert swept == 1
        rows = await _queue_rows(sf, "t1")
        assert rows[0].policy == QueuePolicy.PREFIX
        proc = await reg.check_processed("m1")
        assert proc is not None
        assert proc.status == ProcessedStatus.CANCELLED
        assert proc.delivered is False  # I10: outbox will deliver

    @pytest.mark.anyio
    async def test_sweep_carries_uplink_echo_for_downlink(self, reg, sf):
        # cancel-before-task: a covered task swept to cancelled must echo the uplink envelope
        # (thread_msg_seq + bizType + user_id / project_id / agent_name) so the downlink mirrors
        # the uplink instead of degrading to message_id/thread_id only (regression: thread_msg_seq=0,
        # missing context fields). Echo is reconstructed from the stored queue-row body.
        body = {
            "schema_version": "2.5",
            "message_id": "m8",
            "thread_id": "t1",
            "thread_msg_seq": 8,
            "type": "task",
            "agent_name": "director",
            "user_id": "34",
            "project_id": "proj-1",
            "bizType": "agent_task",
            "payload": {"messages": [{"role": "user", "content": "x"}]},
        }
        await _enqueue(reg, "t1", "m8", 8, body=body)
        await reg.fold_cancel_watermark("t1", 9)  # cancel seq=9 > task seq=8 → covers it
        assert await reg.sweep_cancelled("t1") == 1

        proc = await _processed(sf, "m8")
        assert proc is not None
        assert proc.result_cache is not None  # not None: carries the echo (regression guard)
        echo = proc.result_cache["echo"]
        assert echo["message_id"] == "m8"
        assert echo["thread_id"] == "t1"
        assert echo["thread_msg_seq"] == 8  # not 0
        assert echo["bizType"] == "agent_task"
        assert echo["agent_name"] == "director"
        assert echo["user_id"] == "34"
        assert echo["project_id"] == "proj-1"

    @pytest.mark.anyio
    async def test_sweep_all_threads(self, reg):
        await _enqueue(reg, "tA", "a1", 1)
        await _enqueue(reg, "tB", "b1", 1)
        await reg.fold_cancel_watermark("tA", 9)
        await reg.fold_cancel_watermark("tB", 9)
        assert await reg.sweep_cancelled() == 2  # both swept, thread_id=None

    @pytest.mark.anyio
    async def test_executable_above_watermark_merges_prefix_history(self, reg, sf):
        # m1 seq=1 covered by cancel N=3; m2 seq=5 executable → claim m2 with m1 as prefix.
        await _enqueue(reg, "t1", "m1", 1, body={"message_id": "m1", "text": "hist"})
        await _enqueue(reg, "t1", "m2", 5, body={"message_id": "m2", "text": "new"})
        await reg.fold_cancel_watermark("t1", 3)
        claimed = await reg.claim_next_runnable("inst-A")
        assert claimed.message_id == "m2"
        assert claimed.prefix_message_ids == ["m1"]
        # prefix body comes first in reconstructed input
        assert [b.get("text") for b in claimed.input_bodies] == ["hist", "new"]
        rows = {r.message_id: r for r in await _queue_rows(sf, "t1")}
        assert rows["m1"].status == QueueRowStatus.MERGED
        assert rows["m1"].policy == QueuePolicy.PREFIX  # policy unchanged, only status
        assert rows["m2"].status == QueueRowStatus.RUNNING


class TestClaimPaused:
    @pytest.mark.anyio
    async def test_paused_blocks_followup_allows_resume(self, reg, sf):
        # thread paused; a followup seq=1 and a resume seq=2 both pending.
        async with sf() as session:
            session.add(
                ThreadRunStateRow(
                    thread_id="t1",
                    instance_id="i1",
                    message_id="run0",
                    status=ThreadStatus.PAUSED,
                    last_resolved_seq=0,
                )
            )
            await session.commit()
        await _enqueue(reg, "t1", "f1", 1, policy=QueuePolicy.FOLLOWUP)
        await _enqueue(reg, "t1", "r2", 2, policy=QueuePolicy.RESUME)
        claimed = await reg.claim_next_runnable("inst-A")
        assert claimed is not None
        assert claimed.message_id == "r2"  # resume claimed past the earlier followup
        assert claimed.policy == QueuePolicy.RESUME


class TestClaimDrainExemption:
    @pytest.mark.anyio
    async def test_drain_survives_later_cancel_and_blocks_followup(self, reg, sf):
        # B′ / I13: drain row at seq=N1, then cancel N2>N1 raises watermark past it.
        # drain must still be claimable AND still block a later followup.
        async with sf() as session:
            session.add(
                ThreadRunStateRow(
                    thread_id="t1",
                    instance_id="i1",
                    message_id="x",
                    status=ThreadStatus.IDLE,
                    cancel_watermark=0,
                    last_resolved_seq=0,
                )
            )
            await session.commit()
        await _enqueue(reg, "t1", "d", 5, policy=QueuePolicy.DRAIN)
        await _enqueue(reg, "t1", "f", 9, policy=QueuePolicy.FOLLOWUP)
        await reg.fold_cancel_watermark("t1", 7)  # watermark 7 > drain seq 5
        claimed = await reg.claim_next_runnable("inst-A")
        assert claimed is not None
        assert claimed.message_id == "d"  # drain exempt from watermark, claimed first
        assert claimed.policy == QueuePolicy.DRAIN
        rows = {r.message_id: r for r in await _queue_rows(sf, "t1")}
        assert rows["d"].status == QueueRowStatus.RUNNING
        # followup f (seq 9, > watermark) stayed pending because drain blocked it
        assert rows["f"].status == QueueRowStatus.PENDING


class TestClaimCollect:
    @pytest.mark.anyio
    async def test_collect_batch_merges_contiguous_prefix(self, reg, sf):
        await _enqueue(reg, "t1", "c1", 1, policy=QueuePolicy.COLLECT, body={"message_id": "c1", "t": "a"})
        await _enqueue(reg, "t1", "c2", 2, policy=QueuePolicy.COLLECT, body={"message_id": "c2", "t": "b"})
        await _enqueue(reg, "t1", "c3", 3, policy=QueuePolicy.COLLECT, body={"message_id": "c3", "t": "c"})
        claimed = await reg.claim_next_runnable("inst-A")
        assert claimed.message_id == "c1"  # earliest is the run anchor
        assert claimed.batch_message_ids == ["c1", "c2", "c3"]
        assert [b["t"] for b in claimed.input_bodies] == ["a", "b", "c"]
        rows = {r.message_id: r for r in await _queue_rows(sf, "t1")}
        assert rows["c1"].status == QueueRowStatus.RUNNING
        assert rows["c2"].status == QueueRowStatus.MERGED
        assert rows["c3"].status == QueueRowStatus.MERGED

    @pytest.mark.anyio
    async def test_collect_batch_stops_at_non_collect(self, reg):
        await _enqueue(reg, "t1", "c1", 1, policy=QueuePolicy.COLLECT)
        await _enqueue(reg, "t1", "f2", 2, policy=QueuePolicy.FOLLOWUP)
        await _enqueue(reg, "t1", "c3", 3, policy=QueuePolicy.COLLECT)
        claimed = await reg.claim_next_runnable("inst-A")
        assert claimed.batch_message_ids == ["c1"]  # followup breaks the contiguous run

    @pytest.mark.anyio
    async def test_collect_settle_gap_holds_unsettled_batch(self, reg):
        # With a positive gap, a just-arrived collect candidate is not yet settled.
        await _enqueue(reg, "t1", "c1", 1, policy=QueuePolicy.COLLECT)
        claimed = await reg.claim_next_runnable("inst-A", collect_gap_seconds=3600)
        assert claimed is None  # debounce holds it


# ═══════════════════════════════════════════════════════════════════════════════
# B3 — finalize_run / finalize_paused (§7.3/6.5)
# ═══════════════════════════════════════════════════════════════════════════════


class TestFinalizeRun:
    @pytest.mark.anyio
    async def test_terminal_closes_batch_and_returns_idle(self, reg, sf):
        await _enqueue(reg, "t1", "m1", 1)
        claimed = await reg.claim_next_runnable("inst-A")
        ok = await reg.finalize_run(claimed.message_id, {"r": 1}, ProcessedStatus.COMPLETED)
        assert ok is True
        st = await _state(reg, "t1")
        assert st.status == ThreadStatus.IDLE
        assert st.retry_count == 0
        assert await _queue_rows(sf, "t1") == []  # batch deleted
        proc = await reg.check_processed("m1")
        assert proc.status == ProcessedStatus.COMPLETED
        assert proc.result_cache == {"r": 1}
        assert proc.delivered is False  # I10

    @pytest.mark.anyio
    async def test_ownership_guard_noop_on_wrong_message(self, reg):
        await _enqueue(reg, "t1", "m1", 1)
        await reg.claim_next_runnable("inst-A")
        # finalizing a message that isn't the running one → no-op (I5)
        assert await reg.finalize_run("not-running", None, ProcessedStatus.COMPLETED) is False

    @pytest.mark.anyio
    async def test_double_finalize_is_idempotent(self, reg):
        await _enqueue(reg, "t1", "m1", 1)
        claimed = await reg.claim_next_runnable("inst-A")
        assert await reg.finalize_run(claimed.message_id, None, ProcessedStatus.COMPLETED) is True
        assert await reg.finalize_run(claimed.message_id, None, ProcessedStatus.COMPLETED) is False

    @pytest.mark.anyio
    async def test_collect_merged_marked_delivered_run_undelivered(self, reg):
        await _enqueue(reg, "t1", "c1", 1, policy=QueuePolicy.COLLECT)
        await _enqueue(reg, "t1", "c2", 2, policy=QueuePolicy.COLLECT)
        claimed = await reg.claim_next_runnable("inst-A")
        await reg.finalize_run(claimed.message_id, {"ok": True}, ProcessedStatus.COMPLETED)
        run = await reg.check_processed("c1")
        merged = await reg.check_processed("c2")
        assert run.delivered is False and run.result_cache == {"ok": True}  # sole downlink
        assert merged.delivered is True and merged.result_cache is None  # idempotent marker

    @pytest.mark.anyio
    async def test_prefix_merged_not_rewritten(self, reg):
        # prefix row already cancelled at the barrier; finalize must not overwrite it.
        await _enqueue(reg, "t1", "m1", 1)
        await _enqueue(reg, "t1", "m2", 5)
        await reg.fold_cancel_watermark("t1", 3)  # m1 covered → cancelled + prefix
        claimed = await reg.claim_next_runnable("inst-A")  # m2 claims, m1 merged-prefix
        await reg.finalize_run(claimed.message_id, {"done": 1}, ProcessedStatus.COMPLETED)
        m1 = await reg.check_processed("m1")
        assert m1.status == ProcessedStatus.CANCELLED  # unchanged, not 'completed'

    @pytest.mark.anyio
    async def test_drain_branch_no_processed_no_downlink(self, reg, sf):
        async with sf() as session:
            session.add(
                ThreadRunStateRow(
                    thread_id="t1", instance_id="i1", message_id="x", status=ThreadStatus.IDLE
                )
            )
            await session.commit()
        await _enqueue(reg, "t1", "d", 5, policy=QueuePolicy.DRAIN)
        claimed = await reg.claim_next_runnable("inst-A")
        ok = await reg.finalize_run(claimed.message_id, None, str(QueuePolicy.DRAIN))
        assert ok is True
        st = await _state(reg, "t1")
        assert st.status == ThreadStatus.IDLE
        assert await reg.check_processed("d") is None  # I12: drain produces no downlink
        assert await _queue_rows(sf, "t1") == []


class TestFinalizePaused:
    @pytest.mark.anyio
    async def test_paused_keeps_bookmark_and_deletes_queue(self, reg, sf):
        await _enqueue(reg, "t1", "m1", 1)
        claimed = await reg.claim_next_runnable("inst-A")
        ok = await reg.finalize_paused(claimed.message_id, {"tool_approval_required": True})
        assert ok is True
        st = await _state(reg, "t1")
        assert st.status == ThreadStatus.PAUSED
        assert st.message_id == "m1"  # dangling bookmark (§4.2)
        assert await _queue_rows(sf, "t1") == []
        proc = await reg.check_processed("m1")
        assert proc.status == ProcessedStatus.PAUSED_FOR_APPROVAL
        assert proc.delivered is False


# ═══════════════════════════════════════════════════════════════════════════════
# B4 — outbox (§9.3/9.4)
# ═══════════════════════════════════════════════════════════════════════════════


class TestOutbox:
    @pytest.mark.anyio
    async def test_mark_and_fetch_undelivered(self, reg):
        await reg.mark_processed_undelivered("m1", "t1", ProcessedStatus.COMPLETED, {"r": 1})
        pending = await reg.fetch_undelivered()
        assert [p.message_id for p in pending] == ["m1"]

    @pytest.mark.anyio
    async def test_mark_delivered_excludes_from_fetch(self, reg):
        await reg.mark_processed_undelivered("m1", "t1", ProcessedStatus.COMPLETED, {"r": 1})
        await reg.mark_delivered("m1")
        assert await reg.fetch_undelivered() == []
        proc = await reg.check_processed("m1")
        assert proc.delivered is True
        assert proc.delivered_at is not None

    @pytest.mark.anyio
    async def test_bump_failure_backs_off(self, reg):
        await reg.mark_processed_undelivered("m1", "t1", ProcessedStatus.FAILED, {"e": 1})
        n1 = await reg.bump_delivery_failure("m1", "broker down")
        assert n1 == 1
        # next_delivery_at now in the future → excluded from fetch
        assert await reg.fetch_undelivered() == []
        proc = await reg.check_processed("m1")
        assert proc.delivery_attempts == 1
        assert proc.last_delivery_error == "broker down"
        n2 = await reg.bump_delivery_failure("m1", "still down")
        assert n2 == 2

    @pytest.mark.anyio
    async def test_idempotent_mark_keeps_first_result(self, reg):
        await reg.mark_processed_undelivered("m1", "t1", ProcessedStatus.COMPLETED, {"first": 1})
        await reg.mark_processed_undelivered("m1", "t1", ProcessedStatus.FAILED, {"second": 2})
        proc = await reg.check_processed("m1")
        assert proc.status == ProcessedStatus.COMPLETED  # first write wins
        assert proc.result_cache == {"first": 1}


class TestCleanupRetention:
    @pytest.mark.anyio
    async def test_keeps_undelivered_and_paused(self, reg, sf):
        from datetime import UTC, datetime, timedelta

        old = datetime.now(UTC) - timedelta(days=100)
        async with sf() as session:
            # delivered + old → eligible
            session.add(ProcessedMessageRow(message_id="del", thread_id="t1", status=ProcessedStatus.COMPLETED, delivered=True, processed_at=old))
            # undelivered + old → kept (I10)
            session.add(ProcessedMessageRow(message_id="und", thread_id="t1", status=ProcessedStatus.COMPLETED, delivered=False, processed_at=old))
            # delivered paused_for_approval but thread still paused → kept (§6.5)
            session.add(ProcessedMessageRow(message_id="pau", thread_id="tp", status=ProcessedStatus.PAUSED_FOR_APPROVAL, delivered=True, processed_at=old))
            session.add(ThreadRunStateRow(thread_id="tp", instance_id="i", message_id="pau", status=ThreadStatus.PAUSED))
            await session.commit()
        deleted = await reg.cleanup_processed_messages(ttl_days=1)
        assert deleted == 1  # only "del"
        assert await reg.check_processed("del") is None
        assert await reg.check_processed("und") is not None
        assert await reg.check_processed("pau") is not None


# ═══════════════════════════════════════════════════════════════════════════════
# Phase C — fold_cancel_watermark drain synthesis (§6.5/I12) + requeue_stale (§8)
# ═══════════════════════════════════════════════════════════════════════════════


class TestFoldDrainSynth:
    @pytest.mark.anyio
    async def test_no_drain_when_no_paused_gate(self, reg, sf):
        # Plain cancel on a fresh thread: folds watermark, synthesizes no drain row.
        out = await reg.fold_cancel_watermark("t1", 5)
        assert out.drain_synthesized is False
        rows = await _queue_rows(sf, "t1")
        assert rows == []

    @pytest.mark.anyio
    async def test_drain_synthesized_when_gate_cleared(self, reg, sf):
        # paused run at P=3; cancel N=5 covers it → gate clears AND a drain row is queued.
        async with sf() as session:
            session.add(
                ThreadRunStateRow(
                    thread_id="t1", instance_id="i1", message_id="run3",
                    status=ThreadStatus.PAUSED, last_resolved_seq=3, cancel_watermark=0,
                )
            )
            await session.commit()
        out = await reg.fold_cancel_watermark("t1", 5)
        assert out.drain_synthesized is True
        st = await _state(reg, "t1")
        assert st.status == ThreadStatus.IDLE
        rows = await _queue_rows(sf, "t1")
        assert len(rows) == 1
        drain = rows[0]
        assert drain.message_id == "run3:drain"
        assert drain.policy == QueuePolicy.DRAIN
        assert drain.thread_msg_seq == 5
        assert drain.status == QueueRowStatus.PENDING

    @pytest.mark.anyio
    async def test_drain_synth_idempotent_on_redelivered_cancel(self, reg, sf):
        async with sf() as session:
            session.add(
                ThreadRunStateRow(
                    thread_id="t1", instance_id="i1", message_id="run3",
                    status=ThreadStatus.PAUSED, last_resolved_seq=3, cancel_watermark=0,
                )
            )
            await session.commit()
        assert (await reg.fold_cancel_watermark("t1", 5)).drain_synthesized is True
        # Redelivered cancel: gate already cleared (idle) → no new drain, ON CONFLICT keeps one row.
        again = await reg.fold_cancel_watermark("t1", 5)
        assert again.drain_synthesized is False
        rows = await _queue_rows(sf, "t1")
        assert len([r for r in rows if r.policy == QueuePolicy.DRAIN]) == 1

    @pytest.mark.anyio
    async def test_late_small_cancel_keeps_gate_no_drain(self, reg, sf):
        async with sf() as session:
            session.add(
                ThreadRunStateRow(
                    thread_id="t1", instance_id="i1", message_id="run3",
                    status=ThreadStatus.PAUSED, last_resolved_seq=3, cancel_watermark=0,
                )
            )
            await session.commit()
        out = await reg.fold_cancel_watermark("t1", 2)  # N=2 < P=3 → not covered
        assert out.drain_synthesized is False
        st = await _state(reg, "t1")
        assert st.status == ThreadStatus.PAUSED
        assert await _queue_rows(sf, "t1") == []

    @pytest.mark.anyio
    async def test_synthesized_drain_is_claimable(self, reg, sf):
        async with sf() as session:
            session.add(
                ThreadRunStateRow(
                    thread_id="t1", instance_id="i1", message_id="run3",
                    status=ThreadStatus.PAUSED, last_resolved_seq=3, cancel_watermark=0,
                )
            )
            await session.commit()
        await reg.fold_cancel_watermark("t1", 5)
        claimed = await reg.claim_next_runnable("sched1")
        assert claimed is not None
        assert claimed.policy == QueuePolicy.DRAIN
        assert claimed.message_id == "run3:drain"
        # prefix history is skipped for drain — only the drain body is reconstructed.
        assert claimed.prefix_message_ids == []
        assert len(claimed.input_bodies) == 1


class TestRequeueStale:
    @pytest.mark.anyio
    async def test_requeue_running_back_to_pending(self, reg, sf):
        await _enqueue(reg, "t1", "m1", 1)
        claimed = await reg.claim_next_runnable("i1")
        assert claimed is not None
        st = await _state(reg, "t1")
        assert st.status == ThreadStatus.RUNNING

        ok = await reg.requeue_stale_run("t1")
        assert ok is True
        st = await _state(reg, "t1")
        assert st.status == ThreadStatus.IDLE
        assert st.retry_count == 1
        rows = await _queue_rows(sf, "t1")
        assert rows[0].status == QueueRowStatus.PENDING
        assert rows[0].claimed_by is None
        # re-claimable
        again = await reg.claim_next_runnable("i2")
        assert again is not None and again.message_id == "m1"

    @pytest.mark.anyio
    async def test_requeue_noop_when_idle(self, reg):
        await _enqueue(reg, "t1", "m1", 1)
        # not running → no-op
        assert await reg.requeue_stale_run("t1") is False

    @pytest.mark.anyio
    async def test_requeue_keeps_prefix_merged(self, reg, sf):
        # cancel-covered prefix history must NOT be re-queued (never re-executed).
        await _enqueue(reg, "t1", "old", 1)
        await reg.fold_cancel_watermark("t1", 2)      # covers seq 1
        await reg.sweep_cancelled("t1")               # old → prefix (still pending)
        await _enqueue(reg, "t1", "m2", 3)            # real task merges prefix
        claimed = await reg.claim_next_runnable("i1")
        assert claimed is not None and claimed.message_id == "m2"
        assert "old" in claimed.prefix_message_ids

        await reg.requeue_stale_run("t1")
        rows = {r.message_id: r for r in await _queue_rows(sf, "t1")}
        assert rows["m2"].status == QueueRowStatus.PENDING   # re-claimable
        assert rows["old"].status == QueueRowStatus.MERGED   # prefix stays merged
        assert rows["old"].policy == QueuePolicy.PREFIX

    @pytest.mark.anyio
    async def test_get_running_row(self, reg):
        await _enqueue(reg, "t1", "m1", 1)
        assert await reg.get_running_row("t1") is None
        await reg.claim_next_runnable("i1")
        row = await reg.get_running_row("t1")
        assert row is not None and row.message_id == "m1"
