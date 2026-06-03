"""Phase C — Scheduler claim + dispatch loop (design §2.5/§6.1/§6.3).

Uses a fake RunRegistry (queue of ClaimedRun candidates + sweep counter) and a fake
AgentRunner (records dispatched runs, optionally blocks on an event to hold a slot).
"""

from __future__ import annotations

import asyncio

import pytest

from app.consumer.run_registry import ClaimedRun
from app.consumer.scheduler import Scheduler


class _FakeRegistry:
    def __init__(self, candidates=None):
        self._candidates = list(candidates or [])
        self.sweeps = 0
        self.claim_calls = 0

    async def claim_next_runnable(self, instance_id, *, collect_gap_seconds=0.0, max_collect_wait_seconds=0.0):
        self.claim_calls += 1
        if self._candidates:
            return self._candidates.pop(0)
        return None

    def add(self, claimed):
        self._candidates.append(claimed)

    async def sweep_cancelled(self, thread_id=None):
        self.sweeps += 1
        return 0


class _FakeRunner:
    def __init__(self, gate: asyncio.Event | None = None):
        self.runs = []
        self._gate = gate

    async def run(self, claimed):
        self.runs.append(claimed)
        if self._gate is not None:
            await self._gate.wait()


def _claim(mid, tid="t1"):
    return ClaimedRun(thread_id=tid, message_id=mid, policy="followup", seq=1, input_bodies=[{}])


class TestDrainClaims:
    @pytest.mark.anyio
    async def test_dispatches_all_claimable(self):
        reg = _FakeRegistry([_claim("a"), _claim("b"), _claim("c")])
        runner = _FakeRunner()
        sched = Scheduler(reg, runner, "i1", max_concurrent_runs=5)
        await sched._drain_claims()
        await asyncio.sleep(0)  # let run tasks start
        assert {c.message_id for c in runner.runs} == {"a", "b", "c"}

    @pytest.mark.anyio
    async def test_stops_when_no_slot(self):
        gate = asyncio.Event()
        reg = _FakeRegistry([_claim("a"), _claim("b")])
        runner = _FakeRunner(gate)  # first run holds its slot
        sched = Scheduler(reg, runner, "i1", max_concurrent_runs=1)
        await sched._drain_claims()
        await asyncio.sleep(0)
        assert [c.message_id for c in runner.runs] == ["a"]  # b blocked: no slot
        assert reg.claim_calls == 1  # did not even claim b (slot full)
        gate.set()  # release
        await asyncio.sleep(0)

    @pytest.mark.anyio
    async def test_run_and_release_frees_slot_and_repokes(self):
        reg = _FakeRegistry()
        runner = _FakeRunner()
        sched = Scheduler(reg, runner, "i1", max_concurrent_runs=1)
        await sched._sem.acquire()
        assert sched._sem.locked()
        sched._wake.clear()
        await sched._run_and_release(_claim("a"))
        assert not sched._sem.locked()  # slot freed
        assert sched._wake.is_set()  # re-poked


class TestRunLoop:
    @pytest.mark.anyio
    async def test_claims_then_tick_sweeps(self):
        reg = _FakeRegistry([_claim("a")])
        runner = _FakeRunner()
        sched = Scheduler(reg, runner, "i1", max_concurrent_runs=5, tick_interval=0.02)
        stop = asyncio.Event()
        task = asyncio.create_task(sched.run_loop(stop))
        await asyncio.sleep(0.1)  # several ticks
        stop.set()
        sched.poke()
        await asyncio.wait_for(task, timeout=1)
        assert [c.message_id for c in runner.runs] == ["a"]
        assert reg.sweeps >= 1  # tick fallback ran sweep_cancelled (§6.4)

    @pytest.mark.anyio
    async def test_poke_wakes_loop_for_new_candidate(self):
        reg = _FakeRegistry()
        runner = _FakeRunner()
        sched = Scheduler(reg, runner, "i1", max_concurrent_runs=5, tick_interval=10)
        stop = asyncio.Event()
        task = asyncio.create_task(sched.run_loop(stop))
        await asyncio.sleep(0.02)
        reg.add(_claim("late"))
        sched.poke()  # without poke the long tick would delay dispatch
        await asyncio.sleep(0.02)
        stop.set()
        sched.poke()
        await asyncio.wait_for(task, timeout=1)
        assert [c.message_id for c in runner.runs] == ["late"]

    @pytest.mark.anyio
    async def test_drain_tasks_waits_for_inflight(self):
        gate = asyncio.Event()
        reg = _FakeRegistry([_claim("a")])
        runner = _FakeRunner(gate)
        sched = Scheduler(reg, runner, "i1", max_concurrent_runs=5)
        await sched._drain_claims()
        await asyncio.sleep(0)
        drain = asyncio.create_task(sched.drain_tasks(timeout=0.05))
        await asyncio.sleep(0.1)  # timeout elapses → straggler cancelled
        gate.set()
        await asyncio.wait_for(drain, timeout=1)
        assert sched._stopped is True
