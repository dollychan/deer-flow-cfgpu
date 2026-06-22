"""Tests for the thread-tenancy collapse migration (thread-tenancy.md D7).

Reverses the per-user disk layout: ``users/{uid}/threads/{tid}`` → ``threads/{tid}``.
Multiple users that shared one thread_id are *merged* into the single shared thread
dir (that sharing is the whole point of the collapse). Per-user ``agents/`` and
``memory.json`` (§4.2) stay put — only ``users/{uid}/threads/`` moves.
"""

from pathlib import Path

import pytest

from deerflow.config.paths import Paths


@pytest.fixture
def base_dir(tmp_path: Path) -> Path:
    return tmp_path


@pytest.fixture
def paths(base_dir: Path) -> Paths:
    return Paths(base_dir)


def _seed_user_thread_file(base_dir: Path, user_id: str, thread_id: str, rel: str, content: str) -> Path:
    f = base_dir / "users" / user_id / "threads" / thread_id / rel
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text(content)
    return f


class TestMigrateThreadDirsToThreadOnly:
    def test_moves_single_user_thread_to_thread_only_root(self, base_dir: Path, paths: Paths):
        _seed_user_thread_file(base_dir, "alice", "t1", "user-data/workspace/file.txt", "hello")

        from scripts.migrate_thread_tenancy import migrate_thread_dirs_to_thread_only

        report = migrate_thread_dirs_to_thread_only(paths)

        moved = base_dir / "threads" / "t1" / "user-data" / "workspace" / "file.txt"
        assert moved.read_text() == "hello"
        assert not (base_dir / "users" / "alice" / "threads" / "t1").exists()
        assert len(report) == 1
        assert report[0]["action"].startswith("moved")
        assert report[0]["conflicts"] == 0

    def test_two_users_same_thread_merge_nonconflicting(self, base_dir: Path, paths: Paths):
        _seed_user_thread_file(base_dir, "alice", "shared", "user-data/outputs/a.txt", "from-a")
        _seed_user_thread_file(base_dir, "bob", "shared", "user-data/outputs/b.txt", "from-b")

        from scripts.migrate_thread_tenancy import migrate_thread_dirs_to_thread_only

        migrate_thread_dirs_to_thread_only(paths)

        dest = base_dir / "threads" / "shared" / "user-data" / "outputs"
        assert (dest / "a.txt").read_text() == "from-a"
        assert (dest / "b.txt").read_text() == "from-b"
        assert not (base_dir / "users" / "alice" / "threads" / "shared").exists()
        assert not (base_dir / "users" / "bob" / "threads" / "shared").exists()

    def test_file_conflict_preserves_first_and_stashes_rest(self, base_dir: Path, paths: Paths):
        # alice sorts before bob → alice populates the shared dest; bob's same-path file conflicts.
        _seed_user_thread_file(base_dir, "alice", "t1", "user-data/outputs/r.txt", "alice")
        _seed_user_thread_file(base_dir, "bob", "t1", "user-data/outputs/r.txt", "bob")

        from scripts.migrate_thread_tenancy import migrate_thread_dirs_to_thread_only

        report = migrate_thread_dirs_to_thread_only(paths)

        dest = base_dir / "threads" / "t1" / "user-data" / "outputs" / "r.txt"
        assert dest.read_text() == "alice"
        stashed = base_dir / "migration-conflicts" / "t1" / "from-bob" / "user-data" / "outputs" / "r.txt"
        assert stashed.read_text() == "bob"
        bob_entry = next(e for e in report if e["user_id"] == "bob")
        assert bob_entry["conflicts"] == 1
        assert bob_entry["action"].startswith("merged")

    def test_leaves_user_agents_and_memory_untouched(self, base_dir: Path, paths: Paths):
        _seed_user_thread_file(base_dir, "alice", "t1", "user-data/workspace/f.txt", "x")
        agent = base_dir / "users" / "alice" / "agents" / "my-agent" / "SOUL.md"
        agent.parent.mkdir(parents=True)
        agent.write_text("soul")
        mem = base_dir / "users" / "alice" / "memory.json"
        mem.write_text("{}")

        from scripts.migrate_thread_tenancy import migrate_thread_dirs_to_thread_only

        migrate_thread_dirs_to_thread_only(paths)

        assert agent.read_text() == "soul"
        assert mem.read_text() == "{}"
        assert not (base_dir / "users" / "alice" / "threads").exists()

    def test_cleans_up_empty_user_threads_dir(self, base_dir: Path, paths: Paths):
        _seed_user_thread_file(base_dir, "alice", "t1", "user-data/.keep", "")

        from scripts.migrate_thread_tenancy import migrate_thread_dirs_to_thread_only

        migrate_thread_dirs_to_thread_only(paths)

        assert not (base_dir / "users" / "alice" / "threads").exists()

    def test_no_user_threads_is_noop(self, base_dir: Path, paths: Paths):
        from scripts.migrate_thread_tenancy import migrate_thread_dirs_to_thread_only

        assert migrate_thread_dirs_to_thread_only(paths) == []

    def test_dry_run_moves_nothing(self, base_dir: Path, paths: Paths):
        src = _seed_user_thread_file(base_dir, "alice", "t1", "user-data/workspace/f.txt", "x")

        from scripts.migrate_thread_tenancy import migrate_thread_dirs_to_thread_only

        report = migrate_thread_dirs_to_thread_only(paths, dry_run=True)

        assert len(report) == 1
        assert src.exists()  # untouched
        assert not (base_dir / "threads" / "t1").exists()
