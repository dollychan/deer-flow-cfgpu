"""Tests for deerflow.persistence.memory.repository.

Uses an in-memory SQLite database so tests are fast and self-contained.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from deerflow.config.mlm_config import MlmConfig
from deerflow.persistence.base import Base
from deerflow.persistence.memory.repository import _MAX_RETRIES, _coerce_confidence, MemoryRepository, merge_facts

# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture
async def repo():
    """MemoryRepository backed by a fresh in-memory SQLite database."""
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sf = async_sessionmaker(engine, expire_on_commit=False)
    yield MemoryRepository(sf)
    await engine.dispose()


# ---------------------------------------------------------------------------
# merge_facts
# ---------------------------------------------------------------------------


class TestMergeFacts:
    def test_empty_lists(self):
        assert merge_facts([], []) == []

    def test_candidate_appended_to_empty_existing(self):
        candidate = [{"content": "fact A"}]
        result = merge_facts([], candidate)
        assert result == candidate

    def test_new_fact_added(self):
        existing = [{"content": "fact A"}]
        candidate = [{"content": "fact B"}]
        result = merge_facts(existing, candidate)
        assert len(result) == 2

    def test_duplicate_content_kept_once(self):
        existing = [{"content": "fact A", "version": 1}]
        candidate = [{"content": "fact A", "version": 2}]
        result = merge_facts(existing, candidate)
        assert len(result) == 1
        assert result[0]["version"] == 2  # candidate wins

    def test_preserves_unrelated_existing_facts(self):
        existing = [{"content": "A"}, {"content": "B"}]
        candidate = [{"content": "C"}]
        result = merge_facts(existing, candidate)
        contents = {f["content"] for f in result}
        assert contents == {"A", "B", "C"}

    def test_multiple_candidate_duplicates(self):
        existing = [{"content": "A", "v": 1}, {"content": "B", "v": 1}]
        candidate = [{"content": "A", "v": 2}, {"content": "C", "v": 2}]
        result = merge_facts(existing, candidate)
        by_content = {f["content"]: f for f in result}
        assert by_content["A"]["v"] == 2  # updated
        assert by_content["B"]["v"] == 1  # untouched
        assert "C" in by_content

    # ── confidence threshold ──────────────────────────────────────────────

    def test_low_confidence_candidate_dropped(self):
        candidate = [
            {"content": "keep", "confidence": 0.9},
            {"content": "drop", "confidence": 0.3},
        ]
        result = merge_facts([], candidate, confidence_threshold=0.6)
        assert {f["content"] for f in result} == {"keep"}

    def test_candidate_without_confidence_is_kept(self):
        # Graceful rollout: un-annotated facts bypass the gate.
        candidate = [{"content": "no-conf"}]
        result = merge_facts([], candidate, confidence_threshold=0.9)
        assert {f["content"] for f in result} == {"no-conf"}

    def test_threshold_does_not_re_filter_existing(self):
        existing = [{"content": "old", "confidence": 0.1}]
        result = merge_facts(existing, [], confidence_threshold=0.9)
        assert {f["content"] for f in result} == {"old"}

    def test_confidence_at_threshold_is_kept(self):
        candidate = [{"content": "boundary", "confidence": 0.6}]
        result = merge_facts([], candidate, confidence_threshold=0.6)
        assert {f["content"] for f in result} == {"boundary"}

    # ── max_facts cap ─────────────────────────────────────────────────────

    def test_cap_keeps_highest_confidence(self):
        existing = [
            {"content": "a", "confidence": 0.1},
            {"content": "b", "confidence": 0.9},
        ]
        candidate = [{"content": "c", "confidence": 0.5}]
        result = merge_facts(existing, candidate, max_facts=2)
        assert len(result) == 2
        assert {f["content"] for f in result} == {"b", "c"}  # 0.1 evicted

    def test_cap_treats_missing_confidence_as_neutral(self):
        existing = [
            {"content": "high", "confidence": 0.9},
            {"content": "neutral"},  # ranks at 0.5
            {"content": "low", "confidence": 0.2},
        ]
        result = merge_facts(existing, [], max_facts=2)
        assert {f["content"] for f in result} == {"high", "neutral"}

    def test_no_cap_when_within_limit(self):
        existing = [{"content": "a"}, {"content": "b"}]
        result = merge_facts(existing, [], max_facts=5)
        assert len(result) == 2

    def test_defaults_preserve_legacy_behavior(self):
        # No threshold, no cap → unchanged dedupe-only merge.
        existing = [{"content": "a", "confidence": 0.01}]
        candidate = [{"content": "b", "confidence": 0.02}]
        result = merge_facts(existing, candidate)
        assert {f["content"] for f in result} == {"a", "b"}


class TestCoerceConfidence:
    def test_valid_float(self):
        assert _coerce_confidence(0.7) == 0.7

    def test_int_accepted(self):
        assert _coerce_confidence(1) == 1.0

    def test_numeric_string(self):
        assert _coerce_confidence("0.8") == 0.8

    def test_clamped_to_unit_range(self):
        assert _coerce_confidence(1.5) == 1.0
        assert _coerce_confidence(-0.3) == 0.0

    @pytest.mark.parametrize("value", [None, True, False, "", "abc", float("nan"), float("inf"), {}])
    def test_invalid_returns_none(self, value):
        assert _coerce_confidence(value) is None


# ---------------------------------------------------------------------------
# Load — empty state
# ---------------------------------------------------------------------------


class TestLoadEmpty:
    @pytest.mark.anyio
    async def test_load_user_scopes_empty(self, repo):
        result = await repo.load_user_scopes("user-unknown")
        assert result == []

    @pytest.mark.anyio
    async def test_load_project_scopes_empty(self, repo):
        result = await repo.load_project_scopes("proj-unknown")
        assert result == []

    @pytest.mark.anyio
    async def test_load_agent_not_found(self, repo):
        result = await repo.load_agent("no-such-agent")
        assert result is None


# ---------------------------------------------------------------------------
# upsert_user_scope
# ---------------------------------------------------------------------------


class TestUpsertUserScope:
    @pytest.mark.anyio
    async def test_insert_creates_row(self, repo):
        facts = [{"content": "prefers concise replies"}]
        ok = await repo.upsert_user_scope("u1", "", facts, "summary text")
        assert ok is True

        rows = await repo.load_user_scopes("u1")
        assert len(rows) == 1
        assert rows[0].user_id == "u1"
        assert rows[0].scope_key == ""
        assert json.loads(rows[0].facts) == facts
        assert rows[0].summary == "summary text"
        assert rows[0].version == 0

    @pytest.mark.anyio
    async def test_second_upsert_merges_facts(self, repo):
        await repo.upsert_user_scope("u1", "", [{"content": "fact A"}], None)
        await repo.upsert_user_scope("u1", "", [{"content": "fact B"}], None)

        rows = await repo.load_user_scopes("u1")
        contents = {f["content"] for f in json.loads(rows[0].facts)}
        assert contents == {"fact A", "fact B"}

    @pytest.mark.anyio
    async def test_version_increments_on_update(self, repo):
        await repo.upsert_user_scope("u1", "", [{"content": "x"}], None)
        await repo.upsert_user_scope("u1", "", [{"content": "y"}], None)

        rows = await repo.load_user_scopes("u1")
        assert rows[0].version == 1

    @pytest.mark.anyio
    async def test_summary_replaced_when_provided(self, repo):
        await repo.upsert_user_scope("u1", "", [], "old summary")
        await repo.upsert_user_scope("u1", "", [], "new summary")

        rows = await repo.load_user_scopes("u1")
        assert rows[0].summary == "new summary"

    @pytest.mark.anyio
    async def test_summary_kept_when_candidate_is_none(self, repo):
        await repo.upsert_user_scope("u1", "", [], "keep me")
        await repo.upsert_user_scope("u1", "", [], None)

        rows = await repo.load_user_scopes("u1")
        assert rows[0].summary == "keep me"

    @pytest.mark.anyio
    async def test_different_scope_keys_stored_separately(self, repo):
        await repo.upsert_user_scope("u1", "", [{"content": "general"}], None)
        await repo.upsert_user_scope("u1", "agent:cf-dream", [{"content": "cf-dream pref"}], None)

        rows = await repo.load_user_scopes("u1")
        assert len(rows) == 2
        scope_keys = {r.scope_key for r in rows}
        assert scope_keys == {"", "agent:cf-dream"}

    @pytest.mark.anyio
    async def test_different_users_isolated(self, repo):
        await repo.upsert_user_scope("alice", "", [{"content": "alice fact"}], None)
        await repo.upsert_user_scope("bob", "", [{"content": "bob fact"}], None)

        alice_rows = await repo.load_user_scopes("alice")
        bob_rows = await repo.load_user_scopes("bob")
        assert len(alice_rows) == 1
        assert len(bob_rows) == 1
        assert json.loads(alice_rows[0].facts)[0]["content"] == "alice fact"

    @pytest.mark.anyio
    async def test_candidate_fact_overwrites_existing_by_content(self, repo):
        await repo.upsert_user_scope("u1", "", [{"content": "fact A", "v": 1}], None)
        await repo.upsert_user_scope("u1", "", [{"content": "fact A", "v": 2}], None)

        rows = await repo.load_user_scopes("u1")
        facts = json.loads(rows[0].facts)
        assert len(facts) == 1
        assert facts[0]["v"] == 2


# ---------------------------------------------------------------------------
# upsert_project_scope
# ---------------------------------------------------------------------------


class TestUpsertProjectScope:
    @pytest.mark.anyio
    async def test_insert_creates_row(self, repo):
        facts = [{"content": "12-episode series"}]
        ok = await repo.upsert_project_scope("proj-1", "", facts, None)
        assert ok is True

        rows = await repo.load_project_scopes("proj-1")
        assert len(rows) == 1
        assert json.loads(rows[0].facts) == facts

    @pytest.mark.anyio
    async def test_merge_on_update(self, repo):
        await repo.upsert_project_scope("proj-1", "", [{"content": "A"}], None)
        await repo.upsert_project_scope("proj-1", "", [{"content": "B"}], None)

        rows = await repo.load_project_scopes("proj-1")
        contents = {f["content"] for f in json.loads(rows[0].facts)}
        assert contents == {"A", "B"}

    @pytest.mark.anyio
    async def test_different_scope_keys(self, repo):
        await repo.upsert_project_scope("proj-1", "", [{"content": "general"}], None)
        await repo.upsert_project_scope("proj-1", "agent:cf-dream", [{"content": "cf-dream knowledge"}], None)
        await repo.upsert_project_scope("proj-1", "user:alice", [{"content": "alice role"}], None)

        rows = await repo.load_project_scopes("proj-1")
        assert len(rows) == 3

    @pytest.mark.anyio
    async def test_different_projects_isolated(self, repo):
        await repo.upsert_project_scope("proj-A", "", [{"content": "A fact"}], None)
        await repo.upsert_project_scope("proj-B", "", [{"content": "B fact"}], None)

        assert len(await repo.load_project_scopes("proj-A")) == 1
        assert len(await repo.load_project_scopes("proj-B")) == 1


# ---------------------------------------------------------------------------
# upsert_agent
# ---------------------------------------------------------------------------


class TestUpsertAgent:
    @pytest.mark.anyio
    async def test_insert_creates_row(self, repo):
        facts = [{"content": "use model X for portraits"}]
        ok = await repo.upsert_agent("cf-dream", facts, "agent summary")
        assert ok is True

        row = await repo.load_agent("cf-dream")
        assert row is not None
        assert row.agent_name == "cf-dream"
        assert json.loads(row.facts) == facts
        assert row.summary == "agent summary"
        assert row.version == 0

    @pytest.mark.anyio
    async def test_merge_on_update(self, repo):
        await repo.upsert_agent("cf-dream", [{"content": "A"}], None)
        await repo.upsert_agent("cf-dream", [{"content": "B"}], None)

        row = await repo.load_agent("cf-dream")
        contents = {f["content"] for f in json.loads(row.facts)}
        assert contents == {"A", "B"}

    @pytest.mark.anyio
    async def test_version_increments(self, repo):
        await repo.upsert_agent("cf-dream", [{"content": "A"}], None)
        await repo.upsert_agent("cf-dream", [{"content": "B"}], None)

        row = await repo.load_agent("cf-dream")
        assert row.version == 1

    @pytest.mark.anyio
    async def test_summary_replaced_when_provided(self, repo):
        await repo.upsert_agent("cf-dream", [], "old")
        await repo.upsert_agent("cf-dream", [], "new")

        row = await repo.load_agent("cf-dream")
        assert row.summary == "new"

    @pytest.mark.anyio
    async def test_summary_kept_when_none(self, repo):
        await repo.upsert_agent("cf-dream", [], "keep me")
        await repo.upsert_agent("cf-dream", [], None)

        row = await repo.load_agent("cf-dream")
        assert row.summary == "keep me"

    @pytest.mark.anyio
    async def test_different_agents_isolated(self, repo):
        await repo.upsert_agent("cf-dream", [{"content": "cf-dream fact"}], None)
        await repo.upsert_agent("coder", [{"content": "coder fact"}], None)

        cf_dream = await repo.load_agent("cf-dream")
        coder = await repo.load_agent("coder")
        assert json.loads(cf_dream.facts)[0]["content"] == "cf-dream fact"
        assert json.loads(coder.facts)[0]["content"] == "coder fact"


# ---------------------------------------------------------------------------
# upsert_agent — optimistic locking (Phase G5)
# ---------------------------------------------------------------------------


class _FakeResult:
    def __init__(self, rowcount: int) -> None:
        self.rowcount = rowcount


class _FakeSession:
    """Minimal async-session stand-in driving upsert_agent's CAS retry loop."""

    def __init__(self, row, rowcount: int) -> None:
        self._row = row
        self._rowcount = rowcount
        self.added: list = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, _model, _key):
        return self._row

    async def execute(self, _stmt):
        return _FakeResult(self._rowcount)

    async def commit(self):
        pass

    def add(self, obj):
        self.added.append(obj)


class _FakeSessionFactory:
    """Hands out one pre-scripted session per attempt (per ``self._sf()`` call)."""

    def __init__(self, sessions: list[_FakeSession]) -> None:
        self._sessions = list(sessions)
        self.calls = 0

    def __call__(self):
        self.calls += 1
        return self._sessions.pop(0)


class TestUpsertAgentOptimisticLock:
    @pytest.mark.anyio
    async def test_concurrent_writers_preserve_all_facts(self, repo):
        """No lost update: two racing writers both win (one via a CAS retry).

        Two writers keep the worst-case conflict count (1) well within the
        retry budget, so the merge invariant holds deterministically — unlike a
        large fan-out where a writer could lose every race and exhaust retries.
        """
        await repo.upsert_agent("cf-dream", [{"content": "base"}], None)

        results = await asyncio.gather(
            repo.upsert_agent("cf-dream", [{"content": "w0"}], None),
            repo.upsert_agent("cf-dream", [{"content": "w1"}], None),
        )
        assert all(results)  # neither exhausted its retry budget

        row = await repo.load_agent("cf-dream")
        contents = {f["content"] for f in json.loads(row.facts)}
        assert {"base", "w0", "w1"} <= contents

    @pytest.mark.anyio
    async def test_retries_then_succeeds_on_version_conflict(self):
        """First CAS UPDATE matches 0 rows (stale version) → retry succeeds."""
        row = SimpleNamespace(facts="[]", version=0, summary=None)
        sf = _FakeSessionFactory([_FakeSession(row, rowcount=0), _FakeSession(row, rowcount=1)])
        repo = MemoryRepository(sf)

        ok = await repo.upsert_agent("cf-dream", [{"content": "x"}], None)
        assert ok is True
        assert sf.calls == 2  # one retry

    @pytest.mark.anyio
    async def test_gives_up_after_max_retries(self):
        """Persistent version conflict exhausts the retry budget → False."""
        row = SimpleNamespace(facts="[]", version=0, summary=None)
        sf = _FakeSessionFactory([_FakeSession(row, rowcount=0) for _ in range(_MAX_RETRIES)])
        repo = MemoryRepository(sf)

        ok = await repo.upsert_agent("cf-dream", [{"content": "x"}], None)
        assert ok is False
        assert sf.calls == _MAX_RETRIES


# ---------------------------------------------------------------------------
# Config-driven confidence gate + fact cap through the upsert path
# ---------------------------------------------------------------------------


class TestUpsertFactLimits:
    @pytest.mark.anyio
    async def test_low_confidence_filtered_on_insert(self, repo, monkeypatch):
        monkeypatch.setattr(
            "deerflow.persistence.memory.repository.get_mlm_config",
            lambda: MlmConfig(fact_confidence_threshold=0.6, max_facts_per_scope=50),
        )
        await repo.upsert_user_scope(
            "u1",
            "",
            [{"content": "keep", "confidence": 0.9}, {"content": "drop", "confidence": 0.2}],
            None,
        )
        rows = await repo.load_user_scopes("u1")
        contents = {f["content"] for f in json.loads(rows[0].facts)}
        assert contents == {"keep"}

    @pytest.mark.anyio
    async def test_cap_enforced_on_update(self, repo, monkeypatch):
        monkeypatch.setattr(
            "deerflow.persistence.memory.repository.get_mlm_config",
            lambda: MlmConfig(fact_confidence_threshold=0.0, max_facts_per_scope=2),
        )
        await repo.upsert_agent("cf-dream", [{"content": "a", "confidence": 0.2}], None)
        await repo.upsert_agent(
            "cf-dream",
            [{"content": "b", "confidence": 0.9}, {"content": "c", "confidence": 0.5}],
            None,
        )
        row = await repo.load_agent("cf-dream")
        contents = {f["content"] for f in json.loads(row.facts)}
        assert len(contents) == 2
        assert contents == {"b", "c"}  # lowest-confidence "a" evicted


# ---------------------------------------------------------------------------
# get_memory_repository
# ---------------------------------------------------------------------------


class TestGetMemoryRepository:
    def test_returns_none_when_no_session_factory(self, monkeypatch):

        monkeypatch.setattr("deerflow.persistence.engine._session_factory", None)
        from deerflow.persistence.memory.repository import get_memory_repository

        assert get_memory_repository() is None
