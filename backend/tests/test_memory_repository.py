"""Tests for deerflow.persistence.memory.repository.

Uses an in-memory SQLite database so tests are fast and self-contained.
"""

from __future__ import annotations

import json

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from deerflow.persistence.base import Base
from deerflow.persistence.memory.model import MemoryAgentRow, MemoryProjectRow, MemoryUserRow
from deerflow.persistence.memory.repository import MemoryRepository, merge_facts


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
        await repo.upsert_user_scope("u1", "agent:director", [{"content": "director pref"}], None)

        rows = await repo.load_user_scopes("u1")
        assert len(rows) == 2
        scope_keys = {r.scope_key for r in rows}
        assert scope_keys == {"", "agent:director"}

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
        await repo.upsert_project_scope("proj-1", "agent:director", [{"content": "director knowledge"}], None)
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
        ok = await repo.upsert_agent("director", facts, "agent summary")
        assert ok is True

        row = await repo.load_agent("director")
        assert row is not None
        assert row.agent_name == "director"
        assert json.loads(row.facts) == facts
        assert row.summary == "agent summary"
        assert row.version == 0

    @pytest.mark.anyio
    async def test_merge_on_update(self, repo):
        await repo.upsert_agent("director", [{"content": "A"}], None)
        await repo.upsert_agent("director", [{"content": "B"}], None)

        row = await repo.load_agent("director")
        contents = {f["content"] for f in json.loads(row.facts)}
        assert contents == {"A", "B"}

    @pytest.mark.anyio
    async def test_version_increments(self, repo):
        await repo.upsert_agent("director", [{"content": "A"}], None)
        await repo.upsert_agent("director", [{"content": "B"}], None)

        row = await repo.load_agent("director")
        assert row.version == 1

    @pytest.mark.anyio
    async def test_summary_replaced_when_provided(self, repo):
        await repo.upsert_agent("director", [], "old")
        await repo.upsert_agent("director", [], "new")

        row = await repo.load_agent("director")
        assert row.summary == "new"

    @pytest.mark.anyio
    async def test_summary_kept_when_none(self, repo):
        await repo.upsert_agent("director", [], "keep me")
        await repo.upsert_agent("director", [], None)

        row = await repo.load_agent("director")
        assert row.summary == "keep me"

    @pytest.mark.anyio
    async def test_different_agents_isolated(self, repo):
        await repo.upsert_agent("director", [{"content": "director fact"}], None)
        await repo.upsert_agent("coder", [{"content": "coder fact"}], None)

        director = await repo.load_agent("director")
        coder = await repo.load_agent("coder")
        assert json.loads(director.facts)[0]["content"] == "director fact"
        assert json.loads(coder.facts)[0]["content"] == "coder fact"


# ---------------------------------------------------------------------------
# get_memory_repository
# ---------------------------------------------------------------------------


class TestGetMemoryRepository:
    def test_returns_none_when_no_session_factory(self, monkeypatch):
        from deerflow.persistence import memory as mem_pkg

        monkeypatch.setattr("deerflow.persistence.engine._session_factory", None)
        from deerflow.persistence.memory.repository import get_memory_repository

        assert get_memory_repository() is None
