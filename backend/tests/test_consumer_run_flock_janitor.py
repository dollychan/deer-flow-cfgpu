"""P4d — run-flock lifecycle + per-host orphan janitor (D13 / BUG-012).

Once a director process has *zero* autonomous destroy power (P4a/I16), something else must
reclaim the containers it leaves behind. That job is a per-host janitor that cannot rely on
the consumer being alive. The handshake is a local-disk flock per sandbox:

  - while a run executes, ``AgentRunner`` holds a **shared** flock on ``runs/{sid}.lock``
    (shared, not exclusive: two runs that somehow collide on one sid don't wedge each other,
    and the janitor still sees "someone is active");
  - on park/finish it releases the flock and stamps the file mtime = park time;
  - a crash drops the flock automatically (kernel releases the fd) — that *is* the orphan
    signal, no liveness RPC needed;
  - the janitor walks running ``deer-flow-sandbox-{sid}`` containers: if it cannot take an
    **exclusive** non-blocking flock, a run holds it → keep; if it can, no run is active →
    reclaim iff ``now - mtime > warm_ttl``, else keep (within the warm-reuse window).

This file pins the shared lock convention, the run-flock acquire/release primitives, the
``AgentRunner`` wiring (gated to the AIO-local provider, same sid source as D7 cancel), and
the janitor's three decisions. See ``cfgpu-docs/aio-localbackend-sandbox.md`` P4 / D13.
"""

from __future__ import annotations

import fcntl
import importlib
import os
import time
from unittest.mock import MagicMock, patch

import pytest

from app.consumer import sandbox_locks
from app.consumer.agent_runner import AgentRunner

# ── shared lock convention ──────────────────────────────────────────────────────


def test_creation_lock_path_on_local_disk(monkeypatch, tmp_path):
    monkeypatch.setenv("DEER_FLOW_SANDBOX_LOCK_DIR", str(tmp_path))
    p = sandbox_locks.creation_lock_path("abcd1234")
    assert p == tmp_path / "abcd1234.lock"
    assert p.parent.exists()


def test_run_flock_path_under_runs_subdir(monkeypatch, tmp_path):
    monkeypatch.setenv("DEER_FLOW_SANDBOX_LOCK_DIR", str(tmp_path))
    p = sandbox_locks.run_flock_path("abcd1234")
    assert p == tmp_path / "runs" / "abcd1234.lock"
    assert p.parent.exists()


def test_director_creation_lock_path_uses_shared_convention(monkeypatch, tmp_path):
    """R3 director seam and the janitor must agree on the creation-lock path."""
    monkeypatch.setenv("DEER_FLOW_SANDBOX_LOCK_DIR", str(tmp_path))
    director = importlib.import_module("app.consumer.director_sandbox").DirectorAioProvider
    provider = director.__new__(director)
    seam_path = provider._creation_lock_path("t-1", "abcd1234", user_id="alice")
    assert seam_path == sandbox_locks.creation_lock_path("abcd1234")


# ── run-flock primitives ────────────────────────────────────────────────────────


def test_run_flock_blocks_exclusive_while_held(monkeypatch, tmp_path):
    monkeypatch.setenv("DEER_FLOW_SANDBOX_LOCK_DIR", str(tmp_path))
    held = sandbox_locks.acquire_run_flock("abcd1234")
    assert held is not None
    try:
        # A separate fd taking EX|NB must fail while a run holds the shared lock.
        other = open(sandbox_locks.run_flock_path("abcd1234"), "a")
        with pytest.raises(OSError):
            fcntl.flock(other.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        other.close()
    finally:
        sandbox_locks.release_run_flock(held)

    # After release, EX|NB succeeds (no active holder).
    other2 = open(sandbox_locks.run_flock_path("abcd1234"), "a")
    fcntl.flock(other2.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    fcntl.flock(other2.fileno(), fcntl.LOCK_UN)
    other2.close()


def test_run_flock_release_stamps_mtime(monkeypatch, tmp_path):
    monkeypatch.setenv("DEER_FLOW_SANDBOX_LOCK_DIR", str(tmp_path))
    held = sandbox_locks.acquire_run_flock("abcd1234")
    path = sandbox_locks.run_flock_path("abcd1234")
    os.utime(path, (1.0, 1.0))  # force an ancient mtime
    sandbox_locks.release_run_flock(held)
    assert os.path.getmtime(path) > 1.0  # park time stamped on release


def test_acquire_run_flock_never_raises_on_bad_dir(monkeypatch, tmp_path):
    """A flock failure must never derail a run — degrade to None."""
    monkeypatch.setenv("DEER_FLOW_SANDBOX_LOCK_DIR", str(tmp_path))
    with patch.object(sandbox_locks.fcntl, "flock", side_effect=OSError("boom")):
        assert sandbox_locks.acquire_run_flock("abcd1234") is None


# ── AgentRunner wiring ──────────────────────────────────────────────────────────


def _bare_runner() -> AgentRunner:
    return AgentRunner(MagicMock(), MagicMock(), None)


def _provider(prefix: str = "deer-flow-sandbox"):
    director = importlib.import_module("app.consumer.director_sandbox").DirectorAioProvider
    provider = director.__new__(director)
    provider._config = {"container_prefix": prefix}
    provider._backend = MagicMock()
    return provider


@pytest.mark.asyncio
async def test_runner_acquires_run_flock_with_composite_sid(monkeypatch):
    runner = _bare_runner()
    provider = _provider()
    expected_sid = provider._deterministic_sandbox_id(provider._identity_key("t-1", "alice"))
    sentinel = object()
    with (
        patch("app.consumer.agent_runner._maybe_aio_local_provider", return_value=provider),
        patch("app.consumer.agent_runner.sandbox_locks.acquire_run_flock", return_value=sentinel) as acq,
    ):
        handle = await runner._acquire_run_flock("alice", "t-1")
    assert handle is sentinel
    acq.assert_called_once_with(expected_sid)


@pytest.mark.asyncio
async def test_runner_skips_flock_when_not_aio_local(monkeypatch):
    runner = _bare_runner()
    with (
        patch("app.consumer.agent_runner._maybe_aio_local_provider", return_value=None),
        patch("app.consumer.agent_runner.sandbox_locks.acquire_run_flock") as acq,
    ):
        handle = await runner._acquire_run_flock("alice", "t-1")
    assert handle is None
    acq.assert_not_called()


@pytest.mark.asyncio
async def test_runner_skips_flock_without_user(monkeypatch):
    runner = _bare_runner()
    with (
        patch("app.consumer.agent_runner._maybe_aio_local_provider", return_value=_provider()),
        patch("app.consumer.agent_runner.sandbox_locks.acquire_run_flock") as acq,
    ):
        handle = await runner._acquire_run_flock(None, "t-1")
    assert handle is None
    acq.assert_not_called()


@pytest.mark.asyncio
async def test_runner_release_run_flock_noop_on_none(monkeypatch):
    runner = _bare_runner()
    with patch("app.consumer.agent_runner.sandbox_locks.release_run_flock") as rel:
        await runner._release_run_flock(None)
    rel.assert_not_called()


@pytest.mark.asyncio
async def test_runner_release_run_flock_releases_handle(monkeypatch):
    runner = _bare_runner()
    handle = object()
    with patch("app.consumer.agent_runner.sandbox_locks.release_run_flock") as rel:
        await runner._release_run_flock(handle)
    rel.assert_called_once_with(handle)


# ── janitor ─────────────────────────────────────────────────────────────────────


@pytest.fixture
def janitor(monkeypatch, tmp_path):
    monkeypatch.setenv("DEER_FLOW_SANDBOX_LOCK_DIR", str(tmp_path))
    return importlib.import_module("scripts.sandbox_janitor")


def test_janitor_keeps_container_with_active_run_flock(janitor):
    rm_calls: list[str] = []
    held = sandbox_locks.acquire_run_flock("abcd1234")
    try:
        with (
            patch.object(janitor, "_running_sandbox_containers", return_value=["deer-flow-sandbox-abcd1234"]),
            patch.object(janitor, "_docker_rm_f", side_effect=rm_calls.append),
        ):
            # warm_ttl=0 + far-future now: would reclaim if it could take the lock.
            result = janitor.reclaim_once(warm_ttl=0.0, now=time.time() + 10_000)
    finally:
        sandbox_locks.release_run_flock(held)
    assert rm_calls == []
    assert "deer-flow-sandbox-abcd1234" in result["kept"]


def test_janitor_reclaims_orphan_past_ttl(janitor):
    rm_calls: list[str] = []
    path = sandbox_locks.run_flock_path("abcd1234")
    path.touch()
    os.utime(path, (1000.0, 1000.0))  # parked long ago, no holder
    with (
        patch.object(janitor, "_running_sandbox_containers", return_value=["deer-flow-sandbox-abcd1234"]),
        patch.object(janitor, "_docker_rm_f", side_effect=rm_calls.append),
    ):
        result = janitor.reclaim_once(warm_ttl=600.0, now=2000.0)
    assert rm_calls == ["deer-flow-sandbox-abcd1234"]
    assert "deer-flow-sandbox-abcd1234" in result["reclaimed"]


def test_janitor_keeps_fresh_orphan_within_ttl(janitor):
    rm_calls: list[str] = []
    path = sandbox_locks.run_flock_path("abcd1234")
    path.touch()
    os.utime(path, (1900.0, 1900.0))  # parked recently, no holder
    with (
        patch.object(janitor, "_running_sandbox_containers", return_value=["deer-flow-sandbox-abcd1234"]),
        patch.object(janitor, "_docker_rm_f", side_effect=rm_calls.append),
    ):
        result = janitor.reclaim_once(warm_ttl=600.0, now=2000.0)
    assert rm_calls == []
    assert "deer-flow-sandbox-abcd1234" in result["kept"]


def test_janitor_recovers_from_crash_released_flock(janitor):
    """A crashed holder's flock is gone (fd closed) → treated as orphan, reclaimable."""
    rm_calls: list[str] = []
    crashed = sandbox_locks.acquire_run_flock("abcd1234")
    crashed.close()  # simulate process death: kernel drops the flock
    path = sandbox_locks.run_flock_path("abcd1234")
    os.utime(path, (1000.0, 1000.0))
    with (
        patch.object(janitor, "_running_sandbox_containers", return_value=["deer-flow-sandbox-abcd1234"]),
        patch.object(janitor, "_docker_rm_f", side_effect=rm_calls.append),
    ):
        janitor.reclaim_once(warm_ttl=600.0, now=2000.0)
    assert rm_calls == ["deer-flow-sandbox-abcd1234"]


def test_janitor_missing_flock_file_is_grace_kept(janitor):
    """A container with no flock file yet (fresh create) is given one TTL of grace."""
    rm_calls: list[str] = []
    with (
        patch.object(janitor, "_running_sandbox_containers", return_value=["deer-flow-sandbox-abcd1234"]),
        patch.object(janitor, "_docker_rm_f", side_effect=rm_calls.append),
    ):
        result = janitor.reclaim_once(warm_ttl=600.0, now=time.time())
    assert rm_calls == []
    assert "deer-flow-sandbox-abcd1234" in result["kept"]
    assert sandbox_locks.run_flock_path("abcd1234").exists()  # adopted with fresh mtime
