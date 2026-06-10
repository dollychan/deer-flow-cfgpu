"""G6 — consumer __main__ wiring of the MLM extraction loop.

The DB-backed memory extraction worker (``run_extraction_loop``) must be started
as a *named maintenance* background task that shares the AgentRunner's checkpointer
instance, and be cancellable as part of the §4.1/#4 draining-first shutdown (step
④, after the final outbox flush and before the MQ/producer close).

``main()`` itself is a monolithic coroutine that builds the whole MQ stack, so —
following the ``_enforce_host_bash_safety`` pattern — the wiring lives in a thin
helper ``_start_mlm_extraction_loop`` that these tests pin: the task name, the
*exact* checkpointer + instance_id forwarded into the loop (a wrong checkpointer
would silently read nothing), and clean cancellation.
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from app.consumer.__main__ import _start_mlm_extraction_loop


@pytest.mark.anyio
async def test_starts_named_task_forwarding_checkpointer_and_instance():
    seen: dict = {}
    started = asyncio.Event()

    async def _fake_loop(checkpointer, instance_id, stop_event, **kwargs):
        seen["checkpointer"] = checkpointer
        seen["instance_id"] = instance_id
        seen["stop_event"] = stop_event
        started.set()
        await stop_event.wait()

    checkpointer = object()
    stop_event = asyncio.Event()
    with patch("app.consumer.__main__.run_extraction_loop", _fake_loop):
        task = _start_mlm_extraction_loop(checkpointer, "host-123", stop_event)
        await asyncio.wait_for(started.wait(), timeout=1.0)

        # named maintenance handle so shutdown can cancel it explicitly
        assert task.get_name() == "mlm-extraction"
        # shares the AgentRunner's checkpointer instance (same object, not a copy)
        assert seen["checkpointer"] is checkpointer
        assert seen["instance_id"] == "host-123"
        assert seen["stop_event"] is stop_event

        # cancellable as part of draining-first shutdown step ④
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.anyio
async def test_loop_exits_when_stop_event_set():
    async def _fake_loop(checkpointer, instance_id, stop_event, **kwargs):
        await stop_event.wait()

    stop_event = asyncio.Event()
    with patch("app.consumer.__main__.run_extraction_loop", _fake_loop):
        task = _start_mlm_extraction_loop(object(), "i", stop_event)
        await asyncio.sleep(0)  # let the task start
        stop_event.set()
        await asyncio.wait_for(task, timeout=1.0)
        assert task.done() and task.exception() is None
