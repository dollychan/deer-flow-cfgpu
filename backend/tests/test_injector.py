"""Tests for deerflow.agents.memory.injector."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from deerflow.agents.memory.injector import _format_facts, build_injection, filter_by_scope
from deerflow.config.mlm_config import MlmConfig


# ---------------------------------------------------------------------------
# filter_by_scope
# ---------------------------------------------------------------------------


class TestFilterByScope:
    def _row(self, scope_key: str):
        return SimpleNamespace(scope_key=scope_key)

    def test_empty_scope_key_always_matches(self):
        rows = [self._row("")]
        assert filter_by_scope(rows, set()) == rows
        assert filter_by_scope(rows, {"agent:x"}) == rows

    def test_single_dim_matches_when_active(self):
        rows = [self._row("agent:director")]
        assert filter_by_scope(rows, {"agent:director"}) == rows

    def test_single_dim_excluded_when_not_active(self):
        rows = [self._row("agent:director")]
        assert filter_by_scope(rows, {"agent:coder"}) == []

    def test_multi_dim_requires_all(self):
        rows = [self._row("agent:director+user:alice")]
        assert filter_by_scope(rows, {"agent:director", "user:alice"}) == rows
        assert filter_by_scope(rows, {"agent:director"}) == []

    def test_empty_rows_returns_empty(self):
        assert filter_by_scope([], {"agent:x"}) == []

    def test_mixed_rows_filtered_correctly(self):
        general = self._row("")
        agent_only = self._row("agent:director")
        user_only = self._row("user:alice")
        rows = [general, agent_only, user_only]
        result = filter_by_scope(rows, {"agent:director"})
        assert general in result
        assert agent_only in result
        assert user_only not in result


# ---------------------------------------------------------------------------
# build_injection — integration-level with in-memory SQLite
# ---------------------------------------------------------------------------
# _format_facts — injection-time cap
# ---------------------------------------------------------------------------


class TestFormatFacts:
    def _patch_limit(self, monkeypatch, limit):
        monkeypatch.setattr(
            "deerflow.agents.memory.injector.get_mlm_config",
            lambda: MlmConfig(max_injection_facts=limit),
        )

    def test_renders_all_when_within_limit(self, monkeypatch):
        self._patch_limit(monkeypatch, 15)
        facts = json.dumps([{"content": "a"}, {"content": "b"}])
        out = _format_facts(facts, indent=1)
        assert "- a" in out and "- b" in out

    def test_caps_to_highest_confidence(self, monkeypatch):
        self._patch_limit(monkeypatch, 2)
        facts = json.dumps(
            [
                {"content": "low", "confidence": 0.1},
                {"content": "high", "confidence": 0.9},
                {"content": "mid", "confidence": 0.5},
            ]
        )
        out = _format_facts(facts, indent=1)
        assert "high" in out and "mid" in out
        assert "low" not in out

    def test_malformed_json_returns_empty(self, monkeypatch):
        self._patch_limit(monkeypatch, 15)
        assert _format_facts("not json", indent=1) == ""

    def test_non_list_root_returns_empty(self, monkeypatch):
        self._patch_limit(monkeypatch, 15)
        assert _format_facts(json.dumps({"content": "x"}), indent=1) == ""


# ---------------------------------------------------------------------------


@pytest.fixture
async def repo():
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from deerflow.persistence.base import Base
    from deerflow.persistence.memory.repository import MemoryRepository

    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sf = async_sessionmaker(engine, expire_on_commit=False)
    yield MemoryRepository(sf)
    await engine.dispose()


def _patch_repo(monkeypatch, repo_instance):
    import deerflow.agents.memory.injector as inj

    monkeypatch.setattr(inj, "get_memory_repository", lambda: repo_instance)


class TestBuildInjection:
    @pytest.mark.anyio
    async def test_returns_empty_when_no_repo(self, monkeypatch):
        import deerflow.agents.memory.injector as inj

        monkeypatch.setattr(inj, "get_memory_repository", lambda: None)
        result = await build_injection("u1", "director", "proj-1")
        assert result == ""

    @pytest.mark.anyio
    async def test_returns_empty_when_no_memory(self, repo, monkeypatch):
        _patch_repo(monkeypatch, repo)
        result = await build_injection("u1", "director", "proj-1")
        assert result == ""

    @pytest.mark.anyio
    async def test_injects_user_knowledge(self, repo, monkeypatch):
        _patch_repo(monkeypatch, repo)
        await repo.upsert_user_scope("u1", "", [{"content": "prefers concise replies"}], "User is brief")

        result = await build_injection("u1", None, None)
        assert "User Knowledge" in result
        assert "prefers concise replies" in result
        assert "User is brief" in result

    @pytest.mark.anyio
    async def test_injects_agent_knowledge(self, repo, monkeypatch):
        _patch_repo(monkeypatch, repo)
        await repo.upsert_agent("director", [{"content": "use model X for portraits"}], "Director agent notes")

        result = await build_injection(None, "director", None)
        assert "Agent Knowledge" in result
        assert "use model X for portraits" in result

    @pytest.mark.anyio
    async def test_injects_project_knowledge(self, repo, monkeypatch):
        _patch_repo(monkeypatch, repo)
        await repo.upsert_project_scope("proj-1", "", [{"content": "12-episode series"}], "Project overview")

        result = await build_injection(None, None, "proj-1")
        assert "Project Knowledge" in result
        assert "12-episode series" in result

    @pytest.mark.anyio
    async def test_scope_filter_excludes_irrelevant_rows(self, repo, monkeypatch):
        _patch_repo(monkeypatch, repo)
        # General row (scope_key="") → should appear
        await repo.upsert_user_scope("u1", "", [{"content": "general fact"}], None)
        # Agent-specific row for a different agent → should NOT appear
        await repo.upsert_user_scope("u1", "agent:coder", [{"content": "coder fact"}], None)

        result = await build_injection("u1", "director", None)
        assert "general fact" in result
        assert "coder fact" not in result

    @pytest.mark.anyio
    async def test_agent_scope_included_when_active(self, repo, monkeypatch):
        _patch_repo(monkeypatch, repo)
        await repo.upsert_user_scope("u1", "", [{"content": "general"}], None)
        await repo.upsert_user_scope("u1", "agent:director", [{"content": "director pref"}], None)

        result = await build_injection("u1", "director", None)
        assert "general" in result
        assert "director pref" in result

    @pytest.mark.anyio
    async def test_all_three_sections_combined(self, repo, monkeypatch):
        _patch_repo(monkeypatch, repo)
        await repo.upsert_user_scope("u1", "", [{"content": "user fact"}], "user summary")
        await repo.upsert_agent("director", [{"content": "agent fact"}], "agent summary")
        await repo.upsert_project_scope("proj-1", "", [{"content": "proj fact"}], "proj summary")

        result = await build_injection("u1", "director", "proj-1")
        assert "User Knowledge" in result
        assert "Agent Knowledge" in result
        assert "Project Knowledge" in result
        assert "user fact" in result
        assert "agent fact" in result
        assert "proj fact" in result
