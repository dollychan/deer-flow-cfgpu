"""P3 — cancel-into-container (D7): docker kill the wedged sandbox on hard cancel.

``AioSandbox.execute_command`` is a synchronous, ``threading.Lock``-guarded HTTP call
offloaded via ``asyncio.to_thread``. A ``runner_task.cancel()`` cannot reach into that
worker thread, so a long ffmpeg/bash inside the container keeps running and the next
command wedges on the lock (cancel.md §4.4). The fix: when the watcher issues the hard
cancel, also ``docker kill`` the thread's container — SIGKILL severs the HTTP call, the
``to_thread`` returns, and ``AioSandbox._lock`` releases.

Order is the linchpin (R1): **kill before destroy**. ``provider.destroy`` calls
``sandbox.close()`` which needs ``AioSandbox._lock`` — held by the wedged exec thread until
the kill frees it. Kill first, then destroy cleans the in-memory tracking.

Identity (R1): the container name must match what *acquire* created, i.e. the composite
``_identity_key(thread_id, user_id)`` (P2.5), never the legacy thread-only hash. The
``user_id`` flows in explicitly through ``_cancel_watcher`` — it is NOT re-resolved inside
the helper, so it cannot disagree with the mount bucket.

See ``cfgpu-docs/aio-localbackend-sandbox.md`` D7 / P3.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.consumer import agent_runner as ar_mod
from app.consumer.agent_runner import AgentRunner
from app.consumer.director_sandbox import DirectorAioProvider
from deerflow.runtime.cancel_signal import CancelState
from deerflow.runtime.user_context import reset_current_user, set_current_user

PREFIX = "deer-flow-sandbox"


def _bare_runner() -> AgentRunner:
    return AgentRunner(MagicMock(), MagicMock(), None)


def _director_provider(prefix: str = PREFIX) -> DirectorAioProvider:
    """A DirectorAioProvider with only the bits the kill path touches (real identity algos)."""
    provider = DirectorAioProvider.__new__(DirectorAioProvider)
    provider._config = {"container_prefix": prefix}
    return provider


def _expected_container_name(provider, thread_id: str, user_id: str, prefix: str = PREFIX) -> str:
    sid = provider._deterministic_sandbox_id(provider._identity_key(thread_id, user_id))
    return f"{prefix}-{sid}"


# ── _kill_thread_sandbox: the helper ────────────────────────────────────────────


@pytest.mark.anyio
async def test_c_noop_when_not_aio_local_provider():
    """(c) Local provider (or any non-Aio/non-local backend) → helper is a clean no-op."""
    runner = _bare_runner()
    kill = MagicMock()
    with patch.object(ar_mod, "_maybe_aio_local_provider", lambda: None), patch.object(ar_mod, "_docker_kill", kill):
        await runner._kill_thread_sandbox("userA", "t-1")
    kill.assert_not_called()


@pytest.mark.anyio
async def test_a_kills_container_with_composite_name():
    """(a) The hard-cancel path docker-kills the right container name (prefix + composite sid)."""
    runner = _bare_runner()
    provider = _director_provider()
    provider.destroy = MagicMock()
    kill = MagicMock()

    with patch.object(ar_mod, "_maybe_aio_local_provider", lambda: provider), patch.object(ar_mod, "_docker_kill", kill):
        await runner._kill_thread_sandbox("userA", "t-1")

    kill.assert_called_once_with(_expected_container_name(provider, "t-1", "userA"))


@pytest.mark.anyio
async def test_b_kill_precedes_destroy():
    """(b) kill MUST run before destroy (else destroy wedges on AioSandbox._lock)."""
    runner = _bare_runner()
    provider = _director_provider()
    order: list[str] = []
    provider.destroy = MagicMock(side_effect=lambda sid: order.append("destroy"))
    kill = MagicMock(side_effect=lambda name: order.append("kill"))

    with patch.object(ar_mod, "_maybe_aio_local_provider", lambda: provider), patch.object(ar_mod, "_docker_kill", kill):
        await runner._kill_thread_sandbox("userA", "t-1")

    assert order == ["kill", "destroy"]


@pytest.mark.anyio
async def test_destroy_receives_the_hashed_sandbox_id():
    """destroy() takes the 8-char sandbox_id (not the composite key string)."""
    runner = _bare_runner()
    provider = _director_provider()
    provider.destroy = MagicMock()

    with patch.object(ar_mod, "_maybe_aio_local_provider", lambda: provider), patch.object(ar_mod, "_docker_kill", MagicMock()):
        await runner._kill_thread_sandbox("userA", "t-1")

    expected_sid = provider._deterministic_sandbox_id(provider._identity_key("t-1", "userA"))
    provider.destroy.assert_called_once_with(expected_sid)


@pytest.mark.anyio
async def test_e_two_users_same_thread_kill_distinct_containers():
    """(e) userA and userB colliding on a thread_id kill their *own* containers."""
    runner = _bare_runner()
    provider = _director_provider()
    provider.destroy = MagicMock()
    names: list[str] = []
    kill = MagicMock(side_effect=names.append)

    with patch.object(ar_mod, "_maybe_aio_local_provider", lambda: provider), patch.object(ar_mod, "_docker_kill", kill):
        await runner._kill_thread_sandbox("userA", "shared-tid")
        await runner._kill_thread_sandbox("userB", "shared-tid")

    assert names[0] != names[1]
    assert names[0] == _expected_container_name(provider, "shared-tid", "userA")
    assert names[1] == _expected_container_name(provider, "shared-tid", "userB")


@pytest.mark.anyio
async def test_f_explicit_user_id_wins_over_contextvar():
    """(f) The passed user_id is used verbatim — never re-resolved from the ContextVar.

    A sibling watcher task could carry a stale/absent user ContextVar; the helper must not
    consult it, or the kill lands on the wrong bucket. Here the ContextVar says one user,
    the explicit arg another — the explicit arg must decide the container.
    """
    runner = _bare_runner()
    provider = _director_provider()
    provider.destroy = MagicMock()
    names: list[str] = []
    kill = MagicMock(side_effect=names.append)

    token = set_current_user(SimpleNamespace(id="ctxvar-user"))
    try:
        with patch.object(ar_mod, "_maybe_aio_local_provider", lambda: provider), patch.object(ar_mod, "_docker_kill", kill):
            await runner._kill_thread_sandbox("explicit-user", "t-1")
    finally:
        reset_current_user(token)

    assert names[0] == _expected_container_name(provider, "t-1", "explicit-user")
    assert names[0] != _expected_container_name(provider, "t-1", "ctxvar-user")


# ── _cancel_watcher: wiring user_id through to the helper ───────────────────────


def _watcher_runner_with_watermark(watermark: int) -> AgentRunner:
    runner = _bare_runner()
    runner._registry.get_thread_state = AsyncMock(
        return_value=SimpleNamespace(cancel_watermark=watermark)
    )
    return runner


@pytest.mark.anyio
async def test_watcher_invokes_kill_with_passed_user_id_on_hard_cancel():
    """The watcher hard-cancels AND kills the container, passing the run's user_id (R1/f)."""
    runner = _watcher_runner_with_watermark(9)
    runner._kill_thread_sandbox = AsyncMock()
    cancel_state = CancelState(event=asyncio.Event())  # protected_in_flight=0 → hard cancel

    async def _target():
        await asyncio.sleep(5)

    target = asyncio.create_task(_target())
    watcher = asyncio.create_task(
        runner._cancel_watcher(
            "t1", 3, target, cancel_state, poll_interval=0, user_id="userX"
        )
    )
    with pytest.raises(asyncio.CancelledError):
        await target
    await watcher  # watcher returns after issuing kill

    runner._kill_thread_sandbox.assert_awaited_once_with("userX", "t1")


@pytest.mark.anyio
async def test_watcher_does_not_kill_while_protected_in_flight():
    """(d) cfgpu in flight → cooperative defer branch; no docker kill yet."""
    runner = _watcher_runner_with_watermark(9)
    runner._kill_thread_sandbox = AsyncMock()
    cancel_state = CancelState(event=asyncio.Event(), protected_in_flight=1)

    async def _target():
        await asyncio.sleep(5)

    target = asyncio.create_task(_target())
    watcher = asyncio.create_task(
        runner._cancel_watcher(
            "t1", 3, target, cancel_state, poll_interval=0.01, user_id="userX"
        )
    )
    await asyncio.sleep(0.08)
    assert cancel_state.event.is_set()  # cooperative flag raised
    assert not target.done()  # but not hard-cancelled
    runner._kill_thread_sandbox.assert_not_awaited()  # and no container kill

    watcher.cancel()
    target.cancel()
