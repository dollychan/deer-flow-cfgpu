"""Phase D — AgentRunner v2 (design §6.5/§7).

Real in-memory SQLite RunRegistry (so finalize_run actually closes out the batch) +
fake bridge / fake agent / fake checkpointer. Graph construction and _execute are
stubbed so the tests exercise the run() wiring, drain branch, fork_init idempotency,
cancel_watcher, and input reconstruction — not LangGraph internals.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.consumer.agent_runner import AgentRunner
from app.consumer.constants import ProcessedStatus, QueuePolicy, ThreadStatus
from app.consumer.models import (  # noqa: F401  (register tables on Base)
    ConsumerInstanceRow,
    ProcessedMessageRow,
    ThreadMsgQueueRow,
    ThreadRunStateRow,
)
from app.consumer.run_registry import ClaimedRun, RunRegistry
from app.consumer.schemas import TaskMessage
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
    def __init__(self):
        self.results = []
        self.errors = []
        self.progress = []
        self.registered = []
        self.unregistered = []

    def register_run(self, run_id, reply_config, *, echo=None):
        self.registered.append(run_id)

    def unregister_run(self, run_id):
        self.unregistered.append(run_id)

    def get_buffered_events(self, run_id):
        return []

    async def publish(self, run_id, event, data):
        self.progress.append((run_id, event, data))

    async def publish_result(self, run_id, *, status, stream_events, checkpoint_id=None, echo=None, final_state=None, usage=None):
        self.results.append((run_id, status, checkpoint_id))

    async def publish_error(self, code, *, echo, message="", retriable=False, node=None, checkpoint_id=None):
        self.errors.append((code, message, checkpoint_id))


def _runner(reg, *, checkpointer=None):
    return AgentRunner(reg, _FakeBridge(), checkpointer, MagicMock())


def _envelope(mid, tid="t1", seq=1, *, messages=None, command=None):
    payload: dict = {"config": {}, "reply_config": {"stream_events": True}}
    if command is not None:
        payload["command"] = command
    else:
        payload["messages"] = messages or [{"role": "user", "content": f"msg-{mid}"}]
    return {
        "schema_version": "2.5",
        "message_id": mid,
        "type": "task",
        "thread_id": tid,
        "thread_msg_seq": seq,
        "payload": payload,
    }


async def _enqueue(reg, tid, mid, seq, policy=QueuePolicy.FOLLOWUP, body=None):
    await reg.enqueue_message(tid, mid, body or _envelope(mid, tid, seq), seq, str(policy))


# ── input reconstruction (§6.2.2/§6.4) ────────────────────────────────────────


class TestBuildRunMessage:
    def test_merges_prefix_and_collect_batch(self):
        runner = _runner(MagicMock())
        claimed = ClaimedRun(
            thread_id="t1", message_id="c1", policy=QueuePolicy.COLLECT.value, seq=2,
            input_bodies=[
                _envelope("p0", seq=1, messages=[{"role": "user", "content": "history"}]),
                _envelope("c1", seq=2, messages=[{"role": "user", "content": "first"}]),
                _envelope("c2", seq=3, messages=[{"role": "user", "content": "second"}]),
            ],
            batch_message_ids=["c1", "c2"],
            prefix_message_ids=["p0"],
        )
        msg = runner._build_run_message(claimed)
        assert msg.message_id == "c1"  # candidate is the envelope template
        texts = [m.content for m in msg.messages]
        assert texts == ["history", "first", "second"]

    def test_single_followup_no_merge(self):
        runner = _runner(MagicMock())
        claimed = ClaimedRun(
            thread_id="t1", message_id="m1", policy=QueuePolicy.FOLLOWUP.value, seq=1,
            input_bodies=[_envelope("m1", seq=1)], batch_message_ids=["m1"], prefix_message_ids=[],
        )
        msg = runner._build_run_message(claimed)
        assert msg.message_id == "m1"


# ── run() terminal wiring (§7.3) ──────────────────────────────────────────────


class TestRunFinalize:
    @pytest.mark.anyio
    async def test_success_finalizes_completed(self, reg, sf, monkeypatch):
        await _enqueue(reg, "t1", "m1", 1)
        claimed = await reg.claim_next_runnable("i1")
        runner = _runner(reg)
        monkeypatch.setattr(runner, "_build_graph", lambda cfg: MagicMock())
        monkeypatch.setattr(
            runner, "_execute",
            lambda *a, **k: _coro((False, {"status": "success", "checkpoint_id": "ck1"})),
        )
        await runner.run(claimed)
        proc = await reg.check_processed("m1")
        assert proc.status == ProcessedStatus.COMPLETED
        assert proc.delivered is True  # inline publish ok → run() marks delivered (E1)
        assert proc.result_cache.get("echo", {}).get("message_id") == "m1"  # echo persisted for producer
        st = await reg.get_thread_state("t1")
        assert st.status == ThreadStatus.IDLE
        rows = await reg.peek_thread_queue("t1", policies=(QueuePolicy.FOLLOWUP,))
        assert rows == []  # batch deleted

    @pytest.mark.anyio
    async def test_paused_finalizes_paused(self, reg, monkeypatch):
        await _enqueue(reg, "t1", "m1", 1)
        claimed = await reg.claim_next_runnable("i1")
        runner = _runner(reg)
        monkeypatch.setattr(runner, "_build_graph", lambda cfg: MagicMock())
        monkeypatch.setattr(
            runner, "_execute",
            lambda *a, **k: _coro((True, {"status": "paused_for_approval", "checkpoint_id": "ck1"})),
        )
        await runner.run(claimed)
        proc = await reg.check_processed("m1")
        assert proc.status == ProcessedStatus.PAUSED_FOR_APPROVAL
        st = await reg.get_thread_state("t1")
        assert st.status == ThreadStatus.PAUSED  # not idle (method C)

    @pytest.mark.anyio
    async def test_exception_finalizes_failed(self, reg, monkeypatch):
        await _enqueue(reg, "t1", "m1", 1)
        claimed = await reg.claim_next_runnable("i1")
        runner = _runner(reg)
        monkeypatch.setattr(runner, "_build_graph", lambda cfg: MagicMock())

        async def _boom(*a, **k):
            raise RuntimeError("kaboom")

        monkeypatch.setattr(runner, "_execute", _boom)
        await runner.run(claimed)
        proc = await reg.check_processed("m1")
        assert proc.status == ProcessedStatus.FAILED
        assert runner._bridge.errors[0][0] == "INTERNAL_ERROR"
        st = await reg.get_thread_state("t1")
        assert st.status == ThreadStatus.IDLE


# ── LLM-failure fallback detection ────────────────────────────────────────────


class _FallbackAgent:
    """Fake agent that completes with an LLMErrorHandlingMiddleware fallback message as
    its terminal output — no exception raised, no interrupt — mimicking a swallowed
    provider failure. Exercises the real _execute fallback path."""

    def __init__(self, *, reason="circuit_open", content="provider temporarily unavailable"):
        self.astream_calls = []
        self._snap = SimpleNamespace(
            tasks=[],  # no interrupt → not paused
            config={"configurable": {"checkpoint_id": "ck1"}},
            values={
                "messages": [
                    SimpleNamespace(
                        additional_kwargs={
                            "deerflow_error_fallback": True,
                            "error_type": "CircuitBreakerOpen",
                            "error_reason": reason,
                            "error_detail": "llm down",
                        },
                        content=content,
                    )
                ]
            },
        )

    async def aget_state(self, config):
        return self._snap

    async def astream(self, stream_input, *, config, stream_mode):
        self.astream_calls.append(stream_input)
        if False:
            yield  # make this an async generator
        return


class TestFallbackDetection:
    @pytest.mark.anyio
    async def test_fallback_finalizes_failed_not_phantom_success(self, reg, sf, monkeypatch):
        await _enqueue(reg, "t1", "m1", 1)
        claimed = await reg.claim_next_runnable("i1")
        runner = _runner(reg)
        monkeypatch.setattr(runner, "_build_graph", lambda cfg: _FallbackAgent(reason="circuit_open"))
        await runner.run(claimed)
        proc = await reg.check_processed("m1")
        assert proc.status == ProcessedStatus.FAILED  # not COMPLETED
        assert runner._bridge.results == []  # no phantom success envelope
        code, _msg, ckpt = runner._bridge.errors[0]
        assert code == "AGENT_BUSY"  # circuit_open → retriable AGENT_BUSY
        assert ckpt == "ck1"  # fork anchor preserved
        st = await reg.get_thread_state("t1")
        assert st.status == ThreadStatus.IDLE

    @pytest.mark.anyio
    async def test_quota_fallback_maps_to_quota_exceeded(self, reg, sf, monkeypatch):
        await _enqueue(reg, "t1", "m1", 1)
        claimed = await reg.claim_next_runnable("i1")
        runner = _runner(reg)
        monkeypatch.setattr(runner, "_build_graph", lambda cfg: _FallbackAgent(reason="quota"))
        await runner.run(claimed)
        proc = await reg.check_processed("m1")
        assert proc.status == ProcessedStatus.FAILED
        assert runner._bridge.errors[0][0] == "QUOTA_EXCEEDED"  # non-retriable mapping


# ── drain branch (§6.5) ───────────────────────────────────────────────────────


class _DrainAgent:
    def __init__(self, pending_ids):
        self.astream_calls = []
        snap = SimpleNamespace(
            tasks=[SimpleNamespace(interrupts=[
                SimpleNamespace(value={"type": "tool_approval_required",
                                       "tool_calls": [{"id": i} for i in pending_ids]})
            ])] if pending_ids else [],
            config={"configurable": {"checkpoint_id": "ck"}},
            values={},
        )
        self._snap = snap

    async def aget_state(self, config):
        return self._snap

    async def astream(self, stream_input, *, config, stream_mode):
        self.astream_calls.append(stream_input)
        if False:
            yield  # make this an async generator
        return


class TestDrainBranch:
    @pytest.mark.anyio
    async def test_drain_rejects_and_finalizes_no_downlink(self, reg, sf, monkeypatch):
        # synthesize a drain row via fold (paused gate cleared)
        async with sf() as session:
            session.add(ThreadRunStateRow(
                thread_id="t1", instance_id="i", message_id="run3",
                status=ThreadStatus.PAUSED, last_resolved_seq=3, cancel_watermark=0,
            ))
            await session.commit()
        await reg.fold_cancel_watermark("t1", 5)
        claimed = await reg.claim_next_runnable("i1")
        assert claimed.policy == QueuePolicy.DRAIN.value

        agent = _DrainAgent(["tc1", "tc2"])
        runner = _runner(reg)
        monkeypatch.setattr(runner, "_build_graph", lambda cfg: agent)
        await runner.run(claimed)

        # reject-resume issued with all tool_approvals rejected
        assert len(agent.astream_calls) == 1
        cmd = agent.astream_calls[0]
        approvals = cmd.update["tool_approvals"]
        assert set(approvals) == {"tc1", "tc2"}
        assert all(v["status"] == "rejected" for v in approvals.values())
        # no downlink (§6.5): drain produces no result / error
        assert runner._bridge.results == []
        assert runner._bridge.errors == []
        assert runner._bridge.registered == []  # drain never registers a run
        # finalize drain: row deleted, thread idle
        rows = await reg.peek_thread_queue("t1", policies=(QueuePolicy.DRAIN,))
        assert rows == []
        st = await reg.get_thread_state("t1")
        assert st.status == ThreadStatus.IDLE
        # drain writes no processed_messages
        assert await reg.check_processed("run3:drain") is None

    @pytest.mark.anyio
    async def test_drain_already_empty_just_finalizes(self, reg, sf, monkeypatch):
        async with sf() as session:
            session.add(ThreadRunStateRow(
                thread_id="t1", instance_id="i", message_id="run3",
                status=ThreadStatus.PAUSED, last_resolved_seq=3, cancel_watermark=0,
            ))
            await session.commit()
        await reg.fold_cancel_watermark("t1", 5)
        claimed = await reg.claim_next_runnable("i1")
        agent = _DrainAgent([])  # no pending interrupt
        runner = _runner(reg)
        monkeypatch.setattr(runner, "_build_graph", lambda cfg: agent)
        await runner.run(claimed)
        assert agent.astream_calls == []  # nothing to reject
        st = await reg.get_thread_state("t1")
        assert st.status == ThreadStatus.IDLE


# ── fork_init (§7.4, I11) ─────────────────────────────────────────────────────


class _FakeCheckpointer:
    def __init__(self, store):
        self._store = store  # thread_id -> CheckpointTuple-ish
        self.puts = []
        self.write_calls = []

    async def aget_tuple(self, config):
        tid = config["configurable"]["thread_id"]
        return self._store.get(tid)

    async def aput(self, config, checkpoint, metadata, new_versions):
        tid = config["configurable"]["thread_id"]
        self.puts.append(tid)
        self._store[tid] = SimpleNamespace(checkpoint=checkpoint, metadata=metadata, pending_writes=[], config=config)
        return config

    async def aput_writes(self, config, writes, task_id, task_path=""):
        self.write_calls.append((task_id, list(writes)))


def _fork_message(new_tid="child", parent="parent"):
    env = {
        "schema_version": "2.5", "message_id": "f1", "type": "task", "thread_id": new_tid,
        "thread_msg_seq": 1,
        "payload": {
            "config": {"fork": {"parent_thread_id": parent}},
            "command": {"update": {"tool_approvals": {"tc1": {"status": "approved"}}}},
            "reply_config": {"stream_events": True},
        },
    }
    return TaskMessage.from_json(json.dumps(env))


class TestForkInit:
    @pytest.mark.anyio
    async def test_copies_parent_and_strips_resume(self, reg):
        parent_tuple = SimpleNamespace(
            checkpoint={"id": "p-ckpt"}, metadata={"m": 1},
            pending_writes=[("task-a", "__resume__", "old"), ("task-a", "__interrupt__", "keep")],
            config={"configurable": {"thread_id": "parent"}},
        )
        ckpt = _FakeCheckpointer({"parent": parent_tuple})
        runner = _runner(reg, checkpointer=ckpt)
        await runner._fork_init(_fork_message())
        assert ckpt.puts == ["child"]  # copied onto the new thread
        # RESUME stripped, __interrupt__ kept
        all_writes = [w for _tid, ws in ckpt.write_calls for w in ws]
        channels = {ch for ch, _v in all_writes}
        assert "__interrupt__" in channels
        assert "__resume__" not in channels

    @pytest.mark.anyio
    async def test_idempotent_skip_when_child_exists(self, reg):
        existing = SimpleNamespace(checkpoint={"id": "x"}, metadata={}, pending_writes=[], config={})
        ckpt = _FakeCheckpointer({"child": existing, "parent": existing})
        runner = _runner(reg, checkpointer=ckpt)
        await runner._fork_init(_fork_message())
        assert ckpt.puts == []  # already has checkpoint → no copy (I11)


# ── cancel_watcher (§7.1) ─────────────────────────────────────────────────────


class TestCancelWatcher:
    @pytest.mark.anyio
    async def test_cancels_when_watermark_covers_seq(self, reg, sf):
        async with sf() as session:
            session.add(ThreadRunStateRow(
                thread_id="t1", instance_id="i", message_id="m1",
                status=ThreadStatus.RUNNING, cancel_watermark=9,
            ))
            await session.commit()
        runner = _runner(reg)

        async def _target():
            await asyncio.sleep(5)

        target = asyncio.create_task(_target())
        watcher = asyncio.create_task(runner._cancel_watcher("t1", current_task_seq=3, runner_task=target, poll_interval=0))
        with pytest.raises(asyncio.CancelledError):
            await target
        watcher.cancel()

    @pytest.mark.anyio
    async def test_no_cancel_when_seq_above_watermark(self, reg, sf):
        async with sf() as session:
            session.add(ThreadRunStateRow(
                thread_id="t1", instance_id="i", message_id="m1",
                status=ThreadStatus.RUNNING, cancel_watermark=2,
            ))
            await session.commit()
        runner = _runner(reg)
        target = asyncio.create_task(asyncio.sleep(0.05))
        watcher = asyncio.create_task(runner._cancel_watcher("t1", current_task_seq=5, runner_task=target, poll_interval=0))
        await target  # completes normally — not cancelled (seq 5 > watermark 2)
        watcher.cancel()


def _coro(value):
    async def _c():
        return value
    return _c()
