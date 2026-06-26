"""P4 — strip the process's autonomous destroy power + robust reclaim + local-disk lock.

In the AIO LocalBackend model every VM runs its own docker daemon and many users/threads
share it. ``_reconcile_orphans`` adopts *every* running container on the host into this
process's warm pool — so any code path that blind-``docker stop``s warm/foreign containers
(idle checker, capacity eviction, shutdown teardown) would nuke containers that *other*
instances on the same VM are actively serving (D5 / BUG-012). The fix is invariant I16:
a process destroys **only** the container it currently holds a run on (the D7 cancel path);
it has *zero* autonomous destroy power. Orphans are reclaimed out-of-band by a per-host
run-flock janitor (deployment), so leaks are bounded without the process guessing.

This file guards three behaviour-preserving deerflow seams and their cf-dream provider overrides:

  P4a  ``_destroy_on_shutdown``   — base destroys (unchanged); cf-dream provider no-ops (I16).
       ``_evict_oldest_warm``     — base capacity-destroys; cf-dream provider never destroys.
  P4b  ``_drop_unhealthy_sandbox`` — base removes the in-process reference *and*
       ``backend.destroy``s the container on a failed ``is_alive`` probe (single-tenant);
       cf-dream provider strips the destroy (I16) and only forgets the dead reference so the acquire
       falls back to discover/create. The probe itself (``_check_tracked_sandbox_alive`` on
       active reuse + warm reclaim) discards a janitor ``rm -f``'d stale entry (R2/I19).
  P4c  ``_creation_lock_path``    — base returns the per-thread virtiofs ``{sid}.lock``
       (unchanged); cf-dream provider relocates it to per-host *local disk* so the cross-process
       creation lock is a real flock, not an unreliable virtiofs one (R3/I17).

Base assertions pin byte-for-byte upstreamable behaviour; cf-dream assertions pin the
single override points. See ``cfgpu-docs/aio-localbackend-sandbox.md`` P4 / D5 / D13.
"""

from __future__ import annotations

import importlib
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

AIO_MOD = "deerflow.community.aio_sandbox.aio_sandbox_provider"


def _base_cls():
    return importlib.import_module(AIO_MOD).AioSandboxProvider


def _cf_dream_cls():
    return importlib.import_module("app.consumer.cf_dream_sandbox").CfDreamAioProvider


def _provider(cls, *, backend=None, config=None):
    """Build a provider with the in-memory maps + lifecycle attrs P4 touches."""
    provider = cls.__new__(cls)
    provider._lock = threading.Lock()
    provider._thread_sandboxes = {}
    provider._thread_locks = {}
    provider._sandboxes = {}
    provider._sandbox_infos = {}
    provider._last_activity = {}
    provider._warm_pool = {}
    provider._shutdown_called = False
    provider._idle_checker_stop = threading.Event()
    provider._idle_checker_thread = None
    provider._backend = backend if backend is not None else MagicMock()
    provider._config = config if config is not None else {"container_prefix": "deer-flow-sandbox"}
    return provider


def _info(sandbox_id: str, name: str | None = None):
    return SimpleNamespace(sandbox_id=sandbox_id, sandbox_url=f"http://h:0/{sandbox_id}", container_name=name or f"deer-flow-sandbox-{sandbox_id}")


# ── P4a: shutdown teardown ──────────────────────────────────────────────────────


def test_base_shutdown_destroys_active_and_warm():
    """Base ``_destroy_on_shutdown`` keeps the historical blind-destroy (upstreamable)."""
    backend = MagicMock()
    provider = _provider(_base_cls(), backend=backend)
    active_info = _info("aaaa")
    warm_info = _info("wwww")
    provider._sandboxes["aaaa"] = MagicMock()
    provider._sandbox_infos["aaaa"] = active_info
    provider._warm_pool["wwww"] = (warm_info, 0.0)

    provider.shutdown()

    destroyed = {call.args[0].sandbox_id for call in backend.destroy.call_args_list}
    assert destroyed == {"aaaa", "wwww"}


def test_cf_dream_shutdown_destroys_nothing():
    """cf-dream provider ``_destroy_on_shutdown`` is a no-op: zero autonomous destroy power (I16)."""
    backend = MagicMock()
    provider = _provider(_cf_dream_cls(), backend=backend)
    provider._sandboxes["aaaa"] = MagicMock()
    provider._sandbox_infos["aaaa"] = _info("aaaa")
    provider._warm_pool["wwww"] = (_info("wwww"), 0.0)

    provider.shutdown()

    backend.destroy.assert_not_called()
    assert provider._shutdown_called is True  # still idempotent-guarded
    assert provider._idle_checker_stop.is_set()  # idle checker still stopped


def test_cf_dream_idle_cleanup_is_noop():
    """Defense-in-depth: even if the idle checker runs, it destroys nothing (I16)."""
    backend = MagicMock()
    provider = _provider(_cf_dream_cls(), backend=backend)
    provider._sandboxes["aaaa"] = MagicMock()
    provider._sandbox_infos["aaaa"] = _info("aaaa")
    provider._last_activity["aaaa"] = 0.0  # ancient → would be "idle"
    provider._warm_pool["wwww"] = (_info("wwww"), 0.0)

    provider._cleanup_idle_sandboxes(idle_timeout=1.0)

    backend.destroy.assert_not_called()
    assert "aaaa" in provider._sandboxes
    assert "wwww" in provider._warm_pool


def test_base_idle_cleanup_still_destroys_idle():
    """Base idle cleanup behaviour is unchanged (single-tenant reclaim)."""
    backend = MagicMock()
    provider = _provider(_base_cls(), backend=backend)
    provider._warm_pool["wwww"] = (_info("wwww"), 0.0)  # released at t=0, ancient

    provider._cleanup_idle_sandboxes(idle_timeout=1.0)

    backend.destroy.assert_called_once()
    assert "wwww" not in provider._warm_pool


def test_cf_dream_evict_oldest_warm_never_destroys():
    """cf-dream provider never capacity-destroys a warm container (another instance may reclaim it)."""
    backend = MagicMock()
    provider = _provider(_cf_dream_cls(), backend=backend)
    provider._warm_pool["wwww"] = (_info("wwww"), 0.0)

    evicted = provider._evict_oldest_warm()

    assert evicted is None
    backend.destroy.assert_not_called()
    assert "wwww" in provider._warm_pool  # retained, not popped


def test_base_evict_oldest_warm_still_destroys():
    """Base eviction behaviour is unchanged (capacity backpressure for single-tenant)."""
    backend = MagicMock()
    provider = _provider(_base_cls(), backend=backend)
    warm_info = _info("wwww")
    provider._warm_pool["wwww"] = (warm_info, 0.0)

    evicted = provider._evict_oldest_warm()

    assert evicted == "wwww"
    backend.destroy.assert_called_once()
    assert "wwww" not in provider._warm_pool


# ── P4b: unhealthy-drop destroy stripping (R2 / I16 / I19) ──────────────────────


def test_base_drop_unhealthy_destroys_container():
    """Base ``_drop_unhealthy_sandbox`` removes tracking AND destroys (upstreamable)."""
    backend = MagicMock()
    provider = _provider(_base_cls(), backend=backend)
    info = _info("aaaa")
    provider._sandboxes["aaaa"] = MagicMock()
    provider._sandbox_infos["aaaa"] = info

    provider._drop_unhealthy_sandbox("aaaa", "test", expected_info=info)

    backend.destroy.assert_called_once_with(info)
    assert "aaaa" not in provider._sandboxes
    assert "aaaa" not in provider._sandbox_infos


def test_cf_dream_drop_unhealthy_strips_destroy():
    """cf-dream provider ``_drop_unhealthy_sandbox`` forgets the reference but never ``docker rm``s (I16)."""
    backend = MagicMock()
    provider = _provider(_cf_dream_cls(), backend=backend)
    info = _info("aaaa")
    handle = MagicMock()
    provider._sandboxes["aaaa"] = handle
    provider._sandbox_infos["aaaa"] = info

    provider._drop_unhealthy_sandbox("aaaa", "test", expected_info=info)

    backend.destroy.assert_not_called()  # zero autonomous destroy power
    handle.close.assert_called_once()  # local client handle still closed
    assert "aaaa" not in provider._sandboxes  # in-process reference forgotten
    assert "aaaa" not in provider._sandbox_infos


def test_reclaim_promotes_live_warm_sandbox():
    """Base/live path: promotable warm entry is reclaimed into active tracking."""
    backend = MagicMock()
    backend.is_alive.return_value = True
    provider = _provider(_base_cls(), backend=backend)
    info = _info("wwww")
    provider._warm_pool["wwww"] = (info, 0.0)

    reclaimed = provider._reclaim_warm_pool_sandbox("t-1", "wwww")

    assert reclaimed == "wwww"
    assert "wwww" in provider._sandboxes
    assert "wwww" not in provider._warm_pool


def test_reclaim_discards_dead_warm_sandbox():
    """R2/I16: a janitor-killed warm entry fails is_alive → reference dropped, never destroyed."""
    backend = MagicMock()
    backend.is_alive.return_value = False
    provider = _provider(_cf_dream_cls(), backend=backend)
    info = _info("wwww")
    provider._warm_pool["wwww"] = (info, 0.0)

    reclaimed = provider._reclaim_warm_pool_sandbox("t-1", "wwww")

    assert reclaimed is None  # not promoted
    assert "wwww" not in provider._warm_pool  # stale entry discarded
    assert "wwww" not in provider._sandboxes  # never registered
    backend.is_alive.assert_called_once_with(info)
    backend.destroy.assert_not_called()  # cf-dream provider never autonomously destroys (I16)


# ── P4c: creation lock path (R3 / I17) ──────────────────────────────────────────


def test_base_creation_lock_path_under_thread_dir(monkeypatch, tmp_path):
    """Base keeps the lock on the per-thread (virtiofs) dir — byte-for-byte unchanged."""
    from deerflow.config import paths as paths_mod

    paths = paths_mod.Paths(base_dir=tmp_path)
    monkeypatch.setattr(paths_mod, "get_paths", lambda: paths)
    monkeypatch.setattr(f"{AIO_MOD}.get_paths", lambda: paths, raising=False)

    provider = _provider(_base_cls())
    lock_path = provider._creation_lock_path("t-1", "abcd1234", user_id="alice")

    expected = paths.thread_dir("t-1", user_id="alice") / "abcd1234.lock"
    assert lock_path == expected


def test_cf_dream_creation_lock_path_on_local_disk(monkeypatch, tmp_path):
    """R3: cf-dream provider relocates the creation lock off virtiofs onto per-host local disk."""
    from deerflow.config import paths as paths_mod

    local_dir = tmp_path / "localdisk" / "locks"
    paths = paths_mod.Paths(base_dir=tmp_path / "virtiofs")
    monkeypatch.setattr(paths_mod, "get_paths", lambda: paths)
    monkeypatch.setattr(f"{AIO_MOD}.get_paths", lambda: paths, raising=False)
    monkeypatch.setenv("DEER_FLOW_SANDBOX_LOCK_DIR", str(local_dir))

    provider = _provider(_cf_dream_cls())
    lock_path = provider._creation_lock_path("t-1", "abcd1234", user_id="alice")

    assert lock_path.name == "abcd1234.lock"
    assert lock_path.parent == local_dir
    assert lock_path.parent.exists()  # ensured by the seam
    # Must NOT be under the virtiofs thread subtree.
    thread_subtree = paths.thread_dir("t-1", user_id="alice")
    assert not str(lock_path).startswith(str(paths.base_dir))
    assert Path(thread_subtree) not in lock_path.parents


# ── P0 deploy wiring: the exact config string in vm-部署.md §5.1 must stay valid ──

DEPLOY_PROVIDER_PATH = "app.consumer.cf_dream_sandbox:CfDreamAioProvider"


def test_deploy_config_provider_path_resolves_to_sandbox_provider():
    """Guard the `sandbox.use` string the AIO route's config.yaml points at (P0).

    If `CfDreamAioProvider` is ever renamed/moved, this fails in CI instead of
    silently breaking every route-B deployment whose config.yaml hard-codes the path.
    Resolves through the *same* reflection seam the runtime uses to build the provider.
    """
    from deerflow.reflection import resolve_class
    from deerflow.sandbox.sandbox_provider import SandboxProvider

    cls = resolve_class(DEPLOY_PROVIDER_PATH, SandboxProvider)

    assert cls is _cf_dream_cls()
    assert issubclass(cls, SandboxProvider)
