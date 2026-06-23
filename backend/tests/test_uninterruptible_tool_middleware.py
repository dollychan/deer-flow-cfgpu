"""Tests for UninterruptibleToolMiddleware + the cancel-signal carrier (BUG-009).

The middleware shields tool calls matching ``non_interruptible_tools`` (e.g.
``cfdream_generate_*``) so a hard ``runner_task.cancel()`` landing mid-flight does
NOT orphan the non-cancellable remote task. Two stop paths after the tool drains:

  - cooperative carrier installed (consumer run) → set the Event, uncancel, return
    the result so the run stops cleanly at the next super-step boundary (the
    ToolMessage is checkpointed first).
  - no carrier installed (standalone / other runners) → uncancel and re-raise so
    the cancel is honored (not silently dropped); the tool still ran to completion
    so the remote task is not orphaned and its result was already emitted by the
    inner MessageStream middleware.

See cfgpu-docs/cancel.md §4.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

import pytest

from deerflow.agents.middlewares.uninterruptible_tool_middleware import (
    UninterruptibleToolMiddleware,
)
from deerflow.runtime.cancel_signal import (
    get_cancel_state,
    install_cancel_event,
    reset_cancel_event,
    signal_cooperative_cancel,
)

# ---------------------------------------------------------------------------
# Fakes mirroring langchain's ToolCallRequest shape
# ---------------------------------------------------------------------------


@dataclass
class FakeRequest:
    tool_call: dict
    state: dict | None = field(default_factory=dict)


def _request(name: str) -> FakeRequest:
    return FakeRequest(tool_call={"name": name, "args": {}, "id": "tc_1"})


@pytest.fixture
def installed_event():
    """Install a fresh cancel Event for the duration of one test, then reset."""
    event = asyncio.Event()
    token = install_cancel_event(event)
    try:
        yield event
    finally:
        reset_cancel_event(token)


# ---------------------------------------------------------------------------
# cancel_signal carrier
# ---------------------------------------------------------------------------


class TestCancelSignal:
    def test_get_is_none_by_default(self):
        # Outside any install scope there is no carrier.
        assert get_cancel_state() is None

    def test_signal_returns_false_when_uninstalled(self):
        assert signal_cooperative_cancel() is False

    def test_install_get_signal_reset(self):
        event = asyncio.Event()
        token = install_cancel_event(event)
        try:
            assert get_cancel_state().event is event
            assert not event.is_set()
            assert signal_cooperative_cancel() is True
            assert event.is_set()
        finally:
            reset_cancel_event(token)
        assert get_cancel_state() is None


# ---------------------------------------------------------------------------
# _protected pattern matching
# ---------------------------------------------------------------------------


class TestProtectedMatching:
    def test_glob_matches(self):
        mw = UninterruptibleToolMiddleware(["cfdream_generate_*"])
        assert mw._protected(_request("cfdream_generate_video")) is True
        assert mw._protected(_request("cfdream_generate_image")) is True

    def test_non_match(self):
        mw = UninterruptibleToolMiddleware(["cfdream_generate_*"])
        assert mw._protected(_request("bash")) is False
        assert mw._protected(_request("web_search")) is False

    def test_empty_patterns_protect_nothing(self):
        mw = UninterruptibleToolMiddleware([])
        assert mw._protected(_request("cfdream_generate_video")) is False

    def test_none_patterns_protect_nothing(self):
        mw = UninterruptibleToolMiddleware(None)
        assert mw._protected(_request("cfdream_generate_video")) is False


# ---------------------------------------------------------------------------
# awrap_tool_call — unprotected passthrough
# ---------------------------------------------------------------------------


class TestUnprotectedPassthrough:
    @pytest.mark.asyncio
    async def test_unprotected_returns_result(self):
        mw = UninterruptibleToolMiddleware(["cfdream_generate_*"])

        async def handler(req):
            return "RESULT"

        assert await mw.awrap_tool_call(_request("bash"), handler) == "RESULT"

    @pytest.mark.asyncio
    async def test_unprotected_cancel_propagates_immediately(self):
        """bash/LLM keep default hard-cancel behavior: no shielding."""
        mw = UninterruptibleToolMiddleware(["cfdream_generate_*"])
        started = asyncio.Event()

        async def handler(req):
            started.set()
            await asyncio.sleep(60)  # would block forever if not cancelled
            return "RESULT"

        task = asyncio.create_task(mw.awrap_tool_call(_request("bash"), handler))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


# ---------------------------------------------------------------------------
# awrap_tool_call — protected tool, no cancel
# ---------------------------------------------------------------------------


class TestProtectedNoCancel:
    @pytest.mark.asyncio
    async def test_returns_result_normally(self, installed_event):
        mw = UninterruptibleToolMiddleware(["cfdream_generate_*"])

        async def handler(req):
            return "URLS"

        result = await mw.awrap_tool_call(_request("cfdream_generate_video"), handler)
        assert result == "URLS"
        assert not installed_event.is_set()  # no cancel → no cooperative signal

    @pytest.mark.asyncio
    async def test_tool_exception_propagates(self, installed_event):
        mw = UninterruptibleToolMiddleware(["cfdream_generate_*"])

        async def handler(req):
            raise ValueError("tool boom")

        with pytest.raises(ValueError, match="tool boom"):
            await mw.awrap_tool_call(_request("cfdream_generate_video"), handler)
        assert not installed_event.is_set()


# ---------------------------------------------------------------------------
# awrap_tool_call — protected tool, cancel mid-flight
# ---------------------------------------------------------------------------


class TestProtectedCancelMidFlight:
    @pytest.mark.asyncio
    async def test_with_carrier_drains_then_signals(self, installed_event):
        """Hard cancel mid cfdream poll → swallowed; tool finishes; event set; result returned."""
        mw = UninterruptibleToolMiddleware(["cfdream_generate_*"])
        started = asyncio.Event()
        release = asyncio.Event()
        completed = {"v": False}

        async def handler(req):
            started.set()
            await release.wait()  # cfdream still polling
            completed["v"] = True
            return "URLS"

        task = asyncio.create_task(mw.awrap_tool_call(_request("cfdream_generate_video"), handler))
        await started.wait()

        task.cancel()  # hard cancel lands mid-flight
        await asyncio.sleep(0.02)  # let the cancel be delivered + swallowed
        assert not task.done(), "shield must keep the protected tool running"
        assert not completed["v"]

        release.set()  # cfdream returns
        result = await task  # completes normally, no CancelledError escapes

        assert result == "URLS"
        assert completed["v"] is True  # remote task not orphaned
        assert installed_event.is_set()  # cooperative stop signalled
        assert task.cancelling() == 0  # uncancel() balanced the swallowed cancel

    @pytest.mark.asyncio
    async def test_without_carrier_drains_then_reraises(self):
        """No carrier installed: protect the tool to completion, then honor the cancel."""
        mw = UninterruptibleToolMiddleware(["cfdream_generate_*"])
        started = asyncio.Event()
        release = asyncio.Event()
        completed = {"v": False}

        async def handler(req):
            started.set()
            await release.wait()
            completed["v"] = True
            return "URLS"

        # No install_cancel_event → get_cancel_event() is None.
        task = asyncio.create_task(mw.awrap_tool_call(_request("cfdream_generate_video"), handler))
        await started.wait()

        task.cancel()
        await asyncio.sleep(0.02)
        assert not task.done()

        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task  # cancel honored — not silently dropped

        assert completed["v"] is True  # but the tool DID finish (no orphan)

    @pytest.mark.asyncio
    async def test_sticky_cancel_swallowed_repeatedly(self, installed_event):
        """A second cancel during draining is also swallowed; tool still completes."""
        mw = UninterruptibleToolMiddleware(["cfdream_generate_*"])
        started = asyncio.Event()
        release = asyncio.Event()

        async def handler(req):
            started.set()
            await release.wait()
            return "URLS"

        task = asyncio.create_task(mw.awrap_tool_call(_request("cfdream_generate_video"), handler))
        await started.wait()

        task.cancel()
        await asyncio.sleep(0.01)
        task.cancel()  # sticky: cancel again while still draining
        await asyncio.sleep(0.01)
        assert not task.done()

        release.set()
        result = await task
        assert result == "URLS"
        assert installed_event.is_set()


# ---------------------------------------------------------------------------
# wrap_tool_call — sync path passthrough
# ---------------------------------------------------------------------------


class TestSyncPassthrough:
    def test_sync_passthrough(self):
        mw = UninterruptibleToolMiddleware(["cfdream_generate_*"])

        def handler(req):
            return "RESULT"

        # cfdream MCP tools are async; the sync path just passes through.
        assert mw.wrap_tool_call(_request("cfdream_generate_video"), handler) == "RESULT"


# ---------------------------------------------------------------------------
# Real-graph regression: tool_result must reach the stream during a cooperative
# cancel of a shielded tool (BUG-009 / cancel.md §4.3).
#
# This locks the actual bug: LangGraph runs nodes in their own tasks, so a hard
# runner_task.cancel() unwinds astream and DISCARDS the already-emitted
# tool_result of a drained protected tool. The fix (conditional hard cancel:
# withhold it while protected_in_flight > 0) lets the run stop cooperatively at
# the next super-step boundary AFTER the tool_result is downlinked. The earlier
# fake-based tests never exercised this real interaction.
# ---------------------------------------------------------------------------


class TestRealGraphCooperativeDownlink:
    @pytest.mark.anyio
    async def test_protected_tool_result_downlinked_under_cooperative_cancel(self):
        from _agent_e2e_helpers import build_single_tool_call_model
        from langchain.agents import create_agent
        from langchain_core.tools import tool

        from deerflow.agents.middlewares.message_stream_middleware import (
            MessageStreamMiddleware,
        )
        from deerflow.runtime.cancel_signal import get_cancel_state, install_cancel_event

        seen_in_flight: list[int] = []

        @tool
        async def slow_generate(prompt: str) -> str:
            """Slow non-cancellable tool (stand-in for cfdream generate)."""
            # The middleware has incremented the in-flight counter by now.
            seen_in_flight.append(get_cancel_state().protected_in_flight)
            await asyncio.sleep(0.5)
            return f"GENERATED:{prompt}"

        model = build_single_tool_call_model(
            tool_name="slow_generate", tool_args={"prompt": "a cat"}, final_text="done"
        )
        agent = create_agent(
            model=model,
            tools=[slow_generate],
            middleware=[
                # outer = registered first
                UninterruptibleToolMiddleware(["slow_generate"]),
                MessageStreamMiddleware(visibility_patterns=[("slow_generate", "progress")]),
            ],
        )

        cancel_event = asyncio.Event()
        token = install_cancel_event(cancel_event)
        cancel_state = get_cancel_state()
        runner_task = asyncio.current_task()
        try:
            # Conditional watcher (mirrors AgentRunner._cancel_watcher): wait until the
            # protected tool is actually in flight, then ONLY set the cooperative event —
            # withhold the hard cancel because protected_in_flight > 0. That withholding
            # is the whole fix.
            async def watcher() -> None:
                for _ in range(500):
                    if cancel_state.protected_in_flight > 0:
                        break
                    await asyncio.sleep(0.01)
                else:
                    raise AssertionError("protected tool never marked in flight")
                cancel_event.set()  # NOTE: no runner_task.cancel()

            w = asyncio.create_task(watcher())

            tool_results: list[dict] = []
            cooperative_break = False
            # Explicit handle + aclose() so breaking out of the stream tears down
            # langgraph's node tasks deterministically (otherwise they leak until GC
            # and can hang a later test sharing the event loop).
            agen = agent.astream(
                {"messages": [("user", "draw a cat")]}, stream_mode=["custom", "updates"]
            )
            try:
                async for mode, chunk in agen:
                    if mode == "updates":
                        if cancel_event.is_set():
                            cooperative_break = True
                            break
                        continue
                    if isinstance(chunk, dict) and chunk.get("type") == "tool_result":
                        tool_results.append(chunk)
            finally:
                await agen.aclose()
            await w
        finally:
            from deerflow.runtime.cancel_signal import reset_cancel_event

            reset_cancel_event(token)
            _ = runner_task  # referenced for clarity; never cancelled in this path

        # The shielded tool drained AND its tool_result reached the stream, then the
        # run stopped cooperatively at the boundary.
        assert seen_in_flight == [1], "middleware must mark the protected tool in flight"
        assert cancel_state.protected_in_flight == 0, "counter restored after the tool drained"
        assert len(tool_results) == 1, "tool_result must be downlinked before the cooperative stop"
        assert tool_results[0]["name"] == "slow_generate"
        assert cooperative_break, "run must stop at the updates boundary, not via a hard cancel"
