"""One-time migration: collapse per-user threadData into the thread-only layout.

Reverses ``migrate_user_isolation.py``. Implements the data step of the
thread-tenancy collapse (``cfgpu-docs/thread-tenancy.md`` D7): the disk tenancy
unit moved from ``users/{uid}/threads/{tid}`` (per-user) to ``threads/{tid}``
(per-thread), so multiple users that shared one thread_id now share one disk dir.

What it does:
    * Relocates every ``{base}/users/{uid}/threads/{tid}/`` to ``{base}/threads/{tid}/``.
    * When two users hold the same thread_id, their thread dirs are *merged* into
      the single shared dir (the sharing is intentional, not a collision). Users are
      processed in sorted order; the first to populate a given destination path wins.
      A same-path file from a later user is stashed (lossless) under
      ``{base}/migration-conflicts/{tid}/from-{uid}/...`` for manual review.
    * Cleans up an emptied ``users/{uid}/threads/`` afterwards.

What it deliberately leaves alone:
    * ``users/{uid}/agents/`` and ``users/{uid}/memory.json`` — still per-user (§4.2).
    * OSS (``agent-artifacts/{tid}/...``) and LangGraph checkpoints — already keyed
      by thread_id alone, so nothing to migrate there.

Idempotent: re-running after a successful migration is a no-op (no
``users/*/threads`` left to move).

Usage:
    PYTHONPATH=. python scripts/migrate_thread_tenancy.py [--dry-run]

Applies changes by default; pass ``--dry-run`` to preview the report first.
"""

import argparse
import logging
import shutil
from pathlib import Path

from deerflow.config.paths import Paths, get_paths

logger = logging.getLogger(__name__)


def _merge_user_thread_into(src_dir: Path, dest_dir: Path, conflicts_root: Path, *, dry_run: bool) -> int:
    """Merge files from ``src_dir`` into an existing ``dest_dir`` file-by-file.

    Non-conflicting files are moved into ``dest_dir``; a file whose relative path
    already exists in ``dest_dir`` is stashed under ``conflicts_root`` instead (the
    destination copy wins). Returns the number of conflicting files stashed.
    """
    conflicts = 0
    for src_path in sorted(src_dir.rglob("*")):
        if not src_path.is_file():
            continue
        rel = src_path.relative_to(src_dir)
        target = dest_dir / rel
        if target.exists():
            conflicts += 1
            stash = conflicts_root / rel
            logger.warning("Conflict: %s already exists; stashing source copy at %s", target, stash)
            if not dry_run:
                stash.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(src_path), str(stash))
        else:
            if not dry_run:
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(src_path), str(target))
    if not dry_run and src_dir.exists():
        shutil.rmtree(src_dir)  # drop the now-emptied source tree
    return conflicts


def migrate_thread_dirs_to_thread_only(paths: Paths, *, dry_run: bool = False) -> list[dict]:
    """Move every ``users/{uid}/threads/{tid}`` into the thread-only ``threads/{tid}``.

    Args:
        paths: Paths instance (its ``base_dir`` is the migration root).
        dry_run: If True, only build the report; touch nothing on disk.

    Returns:
        One report entry per migrated ``(user_id, thread_id)`` pair, with keys
        ``user_id``, ``thread_id``, ``action`` (``moved -> ...`` | ``merged -> ...``)
        and ``conflicts`` (int).
    """
    report: list[dict] = []
    users_root = paths.base_dir / "users"
    if not users_root.exists():
        logger.info("No users/ directory found — nothing to migrate.")
        return report

    for user_dir in sorted(users_root.iterdir()):
        if not user_dir.is_dir():
            continue
        user_id = user_dir.name
        user_threads = user_dir / "threads"
        if not user_threads.is_dir():
            continue

        for src_thread in sorted(user_threads.iterdir()):
            if not src_thread.is_dir():
                continue
            thread_id = src_thread.name
            dest = paths.thread_dir(thread_id)  # thread-only: {base}/threads/{tid}
            entry = {"user_id": user_id, "thread_id": thread_id, "action": "", "conflicts": 0}

            if dest.exists():
                conflicts_root = paths.base_dir / "migration-conflicts" / thread_id / f"from-{user_id}"
                n = _merge_user_thread_into(src_thread, dest, conflicts_root, dry_run=dry_run)
                entry["conflicts"] = n
                entry["action"] = f"merged -> {dest}" + (f" ({n} conflict(s) stashed)" if n else "")
                logger.info("Merged thread %s (user %s) -> %s (%d conflict(s))", thread_id, user_id, dest, n)
            else:
                entry["action"] = f"moved -> {dest}"
                if not dry_run:
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(src_thread), str(dest))
                logger.info("Moved thread %s (user %s) -> %s", thread_id, user_id, dest)

            report.append(entry)

        # Clean up an emptied per-user threads/ dir (leave agents/ and memory.json).
        if not dry_run and user_threads.exists() and not any(user_threads.iterdir()):
            user_threads.rmdir()

    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Collapse per-user threadData into the thread-only layout (thread-tenancy.md D7)")
    parser.add_argument("--dry-run", action="store_true", help="Log the planned actions without touching the filesystem")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    paths = get_paths()
    logger.info("Base directory: %s", paths.base_dir)
    logger.info("Dry run: %s", args.dry_run)

    report = migrate_thread_dirs_to_thread_only(paths, dry_run=args.dry_run)

    if report:
        logger.info("Thread-tenancy migration report:")
        for entry in report:
            logger.info("  user=%s thread=%s action=%s", entry["user_id"], entry["thread_id"], entry["action"])
        total_conflicts = sum(e["conflicts"] for e in report)
        if total_conflicts:
            logger.warning(
                "%d file conflict(s) stashed under %s — review and merge manually.",
                total_conflicts,
                paths.base_dir / "migration-conflicts",
            )
    else:
        logger.info("No per-user thread data to migrate.")


if __name__ == "__main__":
    main()
