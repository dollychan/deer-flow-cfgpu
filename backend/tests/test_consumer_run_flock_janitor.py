"""P4d — run-flock lifecycle + per-host orphan janitor (D13 / BUG-012).

Once a cf-dream consumer process has *zero* autonomous destroy power (P4a/I16), something else must
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


def test_cf_dream_creation_lock_path_uses_shared_convention(monkeypatch, tmp_path):
    """R3 cf-dream provider seam and the janitor must agree on the creation-lock path."""
    monkeypatch.setenv("DEER_FLOW_SANDBOX_LOCK_DIR", str(tmp_path))
    provider_cls = importlib.import_module("app.consumer.cf_dream_sandbox").CfDreamAioProvider
    provider = provider_cls.__new__(provider_cls)
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
    provider_cls = importlib.import_module("app.consumer.cf_dream_sandbox").CfDreamAioProvider
    provider = provider_cls.__new__(provider_cls)
    provider._config = {"container_prefix": prefix}
    provider._backend = MagicMock()
    return provider


@pytest.mark.asyncio
async def test_runner_acquires_run_flock_with_thread_only_sid(monkeypatch):
    """Run-flock keys off the thread_id-only identity sid (thread-tenancy.md D3)."""
    runner = _bare_runner()
    provider = _provider()
    expected_sid = provider._deterministic_sandbox_id(provider._identity_key("t-1"))
    sentinel = object()
    with (
        patch("app.consumer.agent_runner._maybe_aio_local_provider", return_value=provider),
        patch("app.consumer.agent_runner.sandbox_locks.acquire_run_flock", return_value=sentinel) as acq,
    ):
        handle = await runner._acquire_run_flock("t-1")
    assert handle is sentinel
    acq.assert_called_once_with(expected_sid)


@pytest.mark.asyncio
async def test_runner_skips_flock_when_not_aio_local(monkeypatch):
    runner = _bare_runner()
    with (
        patch("app.consumer.agent_runner._maybe_aio_local_provider", return_value=None),
        patch("app.consumer.agent_runner.sandbox_locks.acquire_run_flock") as acq,
    ):
        handle = await runner._acquire_run_flock("t-1")
    assert handle is None
    acq.assert_not_called()


@pytest.mark.asyncio
async def test_runner_acquires_flock_independent_of_user_context(monkeypatch):
    """Regression: the run-flock no longer depends on a user_id (thread-tenancy.md D3).

    With no user ContextVar set at all, the flock is still acquired, keyed by the
    thread-only sid — guards against re-introducing the retired per-user gating.
    """
    runner = _bare_runner()
    provider = _provider()
    expected_sid = provider._deterministic_sandbox_id(provider._identity_key("t-1"))
    sentinel = object()
    # No ambient user ContextVar is set in this test — acquisition must not care.
    with (
        patch("app.consumer.agent_runner._maybe_aio_local_provider", return_value=provider),
        patch("app.consumer.agent_runner.sandbox_locks.acquire_run_flock", return_value=sentinel) as acq,
    ):
        handle = await runner._acquire_run_flock("t-1")
    assert handle is sentinel
    acq.assert_called_once_with(expected_sid)


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
            patch.object(janitor, "_running_sandbox_containers", return_value=["cfdream-sandbox-abcd1234"]),
            patch.object(janitor, "_docker_rm_f", side_effect=rm_calls.append),
        ):
            # warm_ttl=0 + far-future now: would reclaim if it could take the lock.
            result = janitor.reclaim_once(warm_ttl=0.0, now=time.time() + 10_000)
    finally:
        sandbox_locks.release_run_flock(held)
    assert rm_calls == []
    assert "cfdream-sandbox-abcd1234" in result["kept"]


def test_janitor_reclaims_orphan_past_ttl(janitor):
    rm_calls: list[str] = []
    path = sandbox_locks.run_flock_path("abcd1234")
    path.touch()
    os.utime(path, (1000.0, 1000.0))  # parked long ago, no holder
    with (
        patch.object(janitor, "_running_sandbox_containers", return_value=["cfdream-sandbox-abcd1234"]),
        patch.object(janitor, "_docker_rm_f", side_effect=rm_calls.append),
    ):
        result = janitor.reclaim_once(warm_ttl=600.0, now=2000.0)
    assert rm_calls == ["cfdream-sandbox-abcd1234"]
    assert "cfdream-sandbox-abcd1234" in result["reclaimed"]


def test_janitor_keeps_fresh_orphan_within_ttl(janitor):
    rm_calls: list[str] = []
    path = sandbox_locks.run_flock_path("abcd1234")
    path.touch()
    os.utime(path, (1900.0, 1900.0))  # parked recently, no holder
    with (
        patch.object(janitor, "_running_sandbox_containers", return_value=["cfdream-sandbox-abcd1234"]),
        patch.object(janitor, "_docker_rm_f", side_effect=rm_calls.append),
    ):
        result = janitor.reclaim_once(warm_ttl=600.0, now=2000.0)
    assert rm_calls == []
    assert "cfdream-sandbox-abcd1234" in result["kept"]


def test_janitor_recovers_from_crash_released_flock(janitor):
    """A crashed holder's flock is gone (fd closed) → treated as orphan, reclaimable."""
    rm_calls: list[str] = []
    crashed = sandbox_locks.acquire_run_flock("abcd1234")
    crashed.close()  # simulate process death: kernel drops the flock
    path = sandbox_locks.run_flock_path("abcd1234")
    os.utime(path, (1000.0, 1000.0))
    with (
        patch.object(janitor, "_running_sandbox_containers", return_value=["cfdream-sandbox-abcd1234"]),
        patch.object(janitor, "_docker_rm_f", side_effect=rm_calls.append),
    ):
        janitor.reclaim_once(warm_ttl=600.0, now=2000.0)
    assert rm_calls == ["cfdream-sandbox-abcd1234"]


def test_janitor_missing_flock_file_is_grace_kept(janitor):
    """A container with no flock file yet (fresh create) is given one TTL of grace."""
    rm_calls: list[str] = []
    with (
        patch.object(janitor, "_running_sandbox_containers", return_value=["cfdream-sandbox-abcd1234"]),
        patch.object(janitor, "_docker_rm_f", side_effect=rm_calls.append),
    ):
        result = janitor.reclaim_once(warm_ttl=600.0, now=time.time())
    assert rm_calls == []
    assert "cfdream-sandbox-abcd1234" in result["kept"]
    assert sandbox_locks.run_flock_path("abcd1234").exists()  # adopted with fresh mtime


# ── prefix resolution: config is the single source of truth, with safe fallback ──


def _fake_config(container_prefix):
    cfg = MagicMock()
    cfg.sandbox.container_prefix = container_prefix
    return cfg


def test_resolve_default_prefix_reads_config(janitor):
    """When config loads, its sandbox.container_prefix wins (matches the provider).

    Uses a value distinct from DEFAULT_PREFIX so a pass proves config was read, not the
    hardcoded fallback coincidentally matching.
    """
    with patch("deerflow.config.get_app_config", return_value=_fake_config("from-config-prefix")):
        assert janitor._resolve_default_prefix() == "from-config-prefix"


def test_resolve_default_prefix_falls_back_when_config_unset(janitor):
    """A None container_prefix (config default) falls back to the hardcoded prefix."""
    with patch("deerflow.config.get_app_config", return_value=_fake_config(None)):
        assert janitor._resolve_default_prefix() == janitor.DEFAULT_PREFIX


def test_resolve_default_prefix_falls_back_when_config_unavailable(janitor):
    """If the config load chain raises, the janitor still runs on the hardcoded default."""
    with patch("deerflow.config.get_app_config", side_effect=RuntimeError("no config on this host")):
        assert janitor._resolve_default_prefix() == janitor.DEFAULT_PREFIX


def test_main_uses_config_prefix_when_no_cli_arg(janitor):
    """`sandbox_janitor.py` (no --prefix) sweeps the prefix configured in config.yaml."""
    with (
        patch("deerflow.config.get_app_config", return_value=_fake_config("from-config-prefix")),
        patch.object(janitor, "reclaim_once", return_value={"kept": [], "reclaimed": []}) as reclaim,
    ):
        janitor.main([])
    assert reclaim.call_args.kwargs["prefix"] == "from-config-prefix"


def test_main_cli_prefix_overrides_config(janitor):
    """An explicit --prefix beats both config and the hardcoded default."""
    with (
        patch("deerflow.config.get_app_config", return_value=_fake_config("cfdream-sandbox")),
        patch.object(janitor, "reclaim_once", return_value={"kept": [], "reclaimed": []}) as reclaim,
    ):
        janitor.main(["--prefix", "explicit-prefix"])
    assert reclaim.call_args.kwargs["prefix"] == "explicit-prefix"
