#!/usr/bin/env python3
"""Per-host AIO-sandbox orphan janitor (P4 / D13 / BUG-012).

Once a cf-dream consumer process has *zero* autonomous destroy power (it only ``docker
kill``s the one container a cancel targets, P4a/I16), nothing inside the process reclaims
the containers it leaves parked or orphans on crash. This janitor does, **out-of-band** and
**independent of any consumer being alive** — run it from a systemd oneshot timer (at boot
and periodically). It is the *only* component permitted to ``docker rm -f`` a sandbox that
the local process is not actively running.

Mechanism (the run-flock handshake, see ``app/consumer/sandbox_locks.py``):
  - while a run executes, the consumer holds a **shared** flock on ``runs/{sid}.lock``;
  - the janitor probes each running ``deer-flow-sandbox-{sid}`` container with an
    **exclusive non-blocking** flock:
        * cannot acquire  → a run holds it → **keep** (long task, do not kill);
        * can acquire     → no run active → reclaim iff ``now - mtime > warm_ttl``
          (the warm-reuse window has lapsed), else **keep**.
  - a crashed holder's flock is released by the kernel automatically, so its container
    becomes reclaimable without any liveness RPC.
  - a container with no flock file yet (just created, run-flock not stamped) is adopted
    with a fresh mtime → one ``warm_ttl`` of grace before it can be reclaimed.

This replaces the old age-based blind-kill script, which could not tell a long-running
task from an orphan and would kill live work (D13).

Usage (from backend/):
    DEER_FLOW_SANDBOX_LOCK_DIR=/var/lib/deer-flow/locks \\
    PYTHONPATH=. python scripts/sandbox_janitor.py --warm-ttl 600
    PYTHONPATH=. python scripts/sandbox_janitor.py --dry-run   # report only, no rm
"""

from __future__ import annotations

import argparse
import fcntl
import logging
import os
import subprocess
import sys
import time

from app.consumer import sandbox_locks

logger = logging.getLogger("sandbox_janitor")

DEFAULT_PREFIX = "deer-flow-sandbox"
DEFAULT_WARM_TTL = 600.0  # seconds a parked container may sit before reclaim


def _running_sandbox_containers(prefix: str) -> list[str]:
    """Names of running containers matching ``{prefix}-*`` (one ``docker ps`` call)."""
    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", f"name={prefix}-", "--format", "{{.Names}}"],
            check=False,
            capture_output=True,
            text=True,
        )
    except Exception:
        logger.warning("docker ps failed", exc_info=True)
        return []
    if result.returncode != 0:
        logger.warning("docker ps failed: %s", result.stderr.strip())
        return []
    return [line.strip() for line in result.stdout.splitlines() if line.strip().startswith(f"{prefix}-")]


def _docker_rm_f(container_name: str) -> None:
    """Force-remove an orphaned container; 'no such container' is success, never raises."""
    try:
        result = subprocess.run(
            ["docker", "rm", "-f", container_name],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0 and "no such container" not in result.stderr.lower():
            logger.warning("docker rm -f %s failed: %s", container_name, result.stderr.strip())
    except Exception:
        logger.warning("docker rm -f %s raised", container_name, exc_info=True)


def reclaim_once(*, prefix: str = DEFAULT_PREFIX, warm_ttl: float = DEFAULT_WARM_TTL, now: float | None = None, dry_run: bool = False) -> dict:
    """One reclaim sweep over this host's sandbox containers. Returns a report dict.

    Pure decision logic around the flock handshake; the docker calls are injected via the
    module functions above so the policy can be unit-tested without a daemon.
    """
    now = time.time() if now is None else now
    kept: list[str] = []
    reclaimed: list[str] = []

    for name in _running_sandbox_containers(prefix):
        sid = name[len(prefix) + 1 :]
        if not sid:
            kept.append(name)
            continue
        path = sandbox_locks.run_flock_path(sid)
        # Opening with "a" adopts a missing flock file with a fresh mtime → grace window.
        handle = open(path, "a", encoding="utf-8")
        try:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                # A run holds the shared lock → active, keep it alive.
                kept.append(name)
                continue
            # We now hold the exclusive lock → no run is active on this sandbox.
            idle = now - os.path.getmtime(path)
            if idle > warm_ttl:
                if dry_run:
                    logger.info("[dry-run] would reclaim %s (idle %.0fs > warm_ttl %.0fs)", name, idle, warm_ttl)
                else:
                    _docker_rm_f(name)
                reclaimed.append(name)
            else:
                kept.append(name)
        finally:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass
            handle.close()

    logger.info("janitor sweep: %d kept, %d reclaimed", len(kept), len(reclaimed))
    return {"kept": kept, "reclaimed": reclaimed}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Per-host AIO sandbox orphan janitor (D13)")
    parser.add_argument("--prefix", default=DEFAULT_PREFIX, help="container name prefix (default: %(default)s)")
    parser.add_argument("--warm-ttl", type=float, default=DEFAULT_WARM_TTL, help="seconds a parked container may sit before reclaim (default: %(default)s)")
    parser.add_argument("--dry-run", action="store_true", help="report what would be reclaimed without removing")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    report = reclaim_once(prefix=args.prefix, warm_ttl=args.warm_ttl, dry_run=args.dry_run)
    print(f"kept={len(report['kept'])} reclaimed={len(report['reclaimed'])}")
    if report["reclaimed"]:
        print("reclaimed: " + ", ".join(report["reclaimed"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
