"""Shared on-disk lock convention for the AIO local sandbox (P4 / D13).

Single source of truth for *where* per-host sandbox locks live and *how* a run marks
itself active, used by three parties that must agree byte-for-byte:

  - ``DirectorAioProvider._creation_lock_path`` — the cross-process creation flock (R3/I17);
  - ``AgentRunner`` run-flock — the "a run is executing on this sandbox" marker;
  - the per-host janitor (``scripts/sandbox_janitor``) — reclaims orphaned containers.

All paths are on per-host **local** disk (never the shared/virtiofs ``base_dir`` — flock
semantics there are unreliable), keyed by the deterministic 8-char ``sandbox_id`` so every
party derives the same file for a given ``(user, thread)``. Layout under
``$DEER_FLOW_SANDBOX_LOCK_DIR`` (default ``/tmp/deer-flow-sandbox-locks``)::

    {dir}/{sid}.lock          creation lock (exclusive, held only during create)
    {dir}/runs/{sid}.lock     run-active flock (shared, held for a run's whole lifetime)

The run-flock is taken **shared** on purpose: if two runs ever collide on one sid they do
not wedge each other, yet the janitor — which probes with an *exclusive* non-blocking lock —
still sees that someone is active. A crash drops the flock automatically (the kernel
releases the fd), which is exactly the orphan signal the janitor needs.
"""

from __future__ import annotations

import fcntl
import logging
import os
from pathlib import Path
from typing import IO

logger = logging.getLogger(__name__)

DEFAULT_LOCK_DIR = "/tmp/deer-flow-sandbox-locks"


def lock_dir() -> Path:
    """Per-host local-disk root for sandbox locks (env-overridable)."""
    return Path(os.environ.get("DEER_FLOW_SANDBOX_LOCK_DIR", DEFAULT_LOCK_DIR))


def creation_lock_path(sandbox_id: str) -> Path:
    """Path of the cross-process sandbox-creation lock; parent dir ensured."""
    directory = lock_dir()
    directory.mkdir(parents=True, exist_ok=True)
    return directory / f"{sandbox_id}.lock"


def run_flock_dir() -> Path:
    return lock_dir() / "runs"


def run_flock_path(sandbox_id: str) -> Path:
    """Path of the run-active flock for a sandbox; parent dir ensured."""
    directory = run_flock_dir()
    directory.mkdir(parents=True, exist_ok=True)
    return directory / f"{sandbox_id}.lock"


def acquire_run_flock(sandbox_id: str) -> IO | None:
    """Open and take a **shared, non-blocking** flock marking this run active.

    Returns the open file object — keep it for the run's whole lifetime, then pass it to
    :func:`release_run_flock`. A shared lock always succeeds against other shared holders,
    so concurrent runs on one sid never block each other; the janitor's exclusive probe
    still fails while any holder lives. Never raises: a lock failure degrades to ``None``
    (the run proceeds unprotected rather than dying), since the flock is an optimisation
    for reclaim, not a correctness gate for the run itself.
    """
    path = run_flock_path(sandbox_id)
    handle: IO | None = None
    try:
        handle = open(path, "a", encoding="utf-8")
        fcntl.flock(handle.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
        return handle
    except Exception:
        logger.warning("Failed to acquire run-flock for sandbox %s", sandbox_id, exc_info=True)
        if handle is not None:
            try:
                handle.close()
            except Exception:
                pass
        return None


def release_run_flock(handle: IO | None) -> None:
    """Stamp park-time mtime, release the flock, and close. Never raises.

    The mtime is bumped to *now* (park time) **before** unlocking so the janitor's
    ``warm_ttl`` countdown starts from when the run actually parked, while the file is
    still protected from a racing reclaim.
    """
    if handle is None:
        return
    try:
        os.utime(handle.fileno(), None)  # park time → janitor warm_ttl anchor
    except Exception:
        logger.debug("Failed to stamp run-flock mtime on release", exc_info=True)
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    except Exception:
        logger.debug("Failed to release run-flock", exc_info=True)
    try:
        handle.close()
    except Exception:
        pass
