"""Regression: a cooperative cancel must persist the drained protected tool's ToolMessage
to the checkpoint before AgentRunner reads state (BUG-009 / cancel.md §4.3).

The bug
-------
``AgentRunner._execute`` iterated ``async for ... in agent.astream(): break`` and then
read ``aget_state`` (directly, or via ``_run``'s ``CancelledError`` handler) WITHOUT
closing the astream generator. Under the default ``durability="async"`` LangGraph submits
each super-step's checkpoint write to a background executor and only awaits it when the
Pregel loop is *finalized* (``AsyncBackgroundExecutor.__aexit__``). A bare break leaves the
generator suspended at its ``yield`` — the pending write never gets awaited — so the just-
drained cfdream ToolMessage races that write and can be **missing** from the checkpoint. The
next turn then sees a dangling tool_call (DanglingToolCallMiddleware injects a placeholder)
and the already-billed, already-downlinked result is lost from state.

The fix
-------
``_execute`` holds the astream generator handle and ``aclose()``s it in a ``finally``,
flushing *this* run's pending checkpoint write before any ``aget_state`` runs — without
forcing global ``durability="sync"`` (which would serialize every super-step of every run).

Why this test is deterministic
------------------------------
A real ``create_agent`` graph (UninterruptibleToolMiddleware + MessageStreamMiddleware + a
slow tool) runs against a checkpointer with an artificial async-write latency. The latency
makes the race resolve the same way every time: without the ``aclose()`` flush the tool-step
checkpoint write has not landed when state is read, so the ToolMessage is absent; with the
fix the ``aclose()`` awaits the write, so it is present. Reverting the fix turns this test red.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest
from langchain.agents import create_agent
from langchain_core.messages import ToolMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver

from app.consumer.agent_runner import AgentRunner
from app.consumer.schemas import ReplyConfig, TaskMessage, UserMessage
from deerflow.agents.middlewares.message_stream_middleware import MessageStreamMiddleware
from deerflow.agents.middlewares.uninterruptible_tool_middleware import (
    UninterruptibleToolMiddleware,
)
from deerflow.runtime.cancel_signal import (
    get_cancel_state,
    install_cancel_event,
    reset_cancel_event,
)


class _SlowSaver(InMemorySaver):
    """InMemorySaver whose async write incurs a fixed latency, to mimic a real
    (network-bound) Postgres checkpointer and make the background-write race deterministic.
    """

    def __init__(self, latency: float) -> None:
        super().__init__()
        self._latency = latency

    async def aput(self, config, checkpoint, metadata, new_versions):  # type: ignore[override]
        await asyncio.sleep(self._latency)
        return await super().aput(config, checkpoint, metadata, new_versions)


class _NoopBridge:
    """Minimal MQStreamBridge stand-in: _execute only needs an awaitable publish()."""

    async def publish(self, run_id, event, data):
        pass


def _task_message() -> TaskMessage:
    return TaskMessage(
        schema_version="2.5",
        message_id="r1",
        message_seq=1,
        timestamp="",
        type="task",
        thread_id="t1",
        agent_name="lead_agent",
        messages=[UserMessage(role="user", content="draw a cat")],
        command=None,
        config={},
        reply_config=ReplyConfig(stream_events=True, stream_event_types=["custom"]),
    )


@pytest.mark.anyio
async def test_cooperative_cancel_persists_protected_tool_message_to_checkpoint():
    seen_in_flight: list[int] = []

    @tool
    async def slow_generate(prompt: str) -> str:
        """Slow non-cancellable tool (stand-in for cfdream generate)."""
        seen_in_flight.append(get_cancel_state().protected_in_flight)
        await asyncio.sleep(0.2)
        return f"GENERATED:{prompt}"

    from _agent_e2e_helpers import build_single_tool_call_model

    model = build_single_tool_call_model(
        tool_name="slow_generate", tool_args={"prompt": "a cat"}, final_text="done"
    )
    # Write latency >> the (sub-ms) gap between the tool super-step's after_tick and the
    # state read: without the aclose() flush the tool-step checkpoint reliably has not landed.
    saver = _SlowSaver(latency=0.4)
    agent = create_agent(
        model=model,
        tools=[slow_generate],
        middleware=[
            # outer = registered first; Uninterruptible must wrap MessageStream so the
            # tool_result is emitted inside the shield (cancel.md §4.2).
            UninterruptibleToolMiddleware(["slow_generate"]),
            MessageStreamMiddleware(visibility_patterns=[("slow_generate", "progress")]),
        ],
        checkpointer=saver,
    )

    runner = AgentRunner(
        registry=MagicMock(),
        bridge=_NoopBridge(),
        checkpointer=saver,
        app_config=MagicMock(),
    )

    runnable_config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    cancel_event = asyncio.Event()
    token = install_cancel_event(cancel_event)
    cancel_state = get_cancel_state()
    try:
        # Mirror AgentRunner._cancel_watcher's conditional path: once the protected tool is
        # in flight, set ONLY the cooperative Event (no hard cancel) so the run stops at the
        # next super-step boundary after the tool drains.
        async def watcher() -> None:
            for _ in range(2000):
                if cancel_state.protected_in_flight > 0:
                    break
                await asyncio.sleep(0.002)
            else:
                raise AssertionError("protected tool never marked in flight")
            cancel_event.set()

        w = asyncio.create_task(watcher())

        # _execute raises CancelledError on the cooperative stop (translated to the same
        # terminal path as a hard cancel by _run). The aclose() in its finally must have
        # flushed the tool-step checkpoint by the time this propagates.
        with pytest.raises(asyncio.CancelledError):
            await runner._execute(
                _task_message(),
                run_id="r1",
                agent=agent,
                runnable_config=runnable_config,
                cancel_event=cancel_event,
            )
        await w

        snapshot = await agent.aget_state(runnable_config)
        messages = snapshot.values.get("messages", [])
        tool_messages = [m for m in messages if isinstance(m, ToolMessage)]
    finally:
        reset_cancel_event(token)

    assert seen_in_flight == [1], "middleware must mark the protected tool in flight"
    assert tool_messages, "drained protected tool's ToolMessage must be in the checkpoint"
    assert "GENERATED:a cat" in str(tool_messages[-1].content)
