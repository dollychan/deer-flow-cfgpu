"""Tests for deerflow.agents.memory.mlm_queue.

No LLM or DB required: extractors and repository are mocked.
"""

from __future__ import annotations

import json
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from deerflow.agents.memory.mlm_queue import MlmContext, MlmUpdateQueue, reset_mlm_queue


@pytest.fixture(autouse=True)
def _reset():
    """Reset the global singleton between tests."""
    reset_mlm_queue()
    yield
    reset_mlm_queue()


# ---------------------------------------------------------------------------
# Queue management tests (no processing)
# ---------------------------------------------------------------------------


class TestMlmQueueManagement:
    def test_add_enqueues_context(self, monkeypatch):
        monkeypatch.setattr("deerflow.agents.memory.mlm_queue.get_memory_config", _enabled_config)
        q = MlmUpdateQueue()
        q._schedule_timer = MagicMock()  # don't actually start a thread

        q.add("t1", [_human("hi")], user_id="u1", agent_name="director")
        assert q.pending_count == 1

    def test_add_deduplicates_same_key(self, monkeypatch):
        monkeypatch.setattr("deerflow.agents.memory.mlm_queue.get_memory_config", _enabled_config)
        q = MlmUpdateQueue()
        q._schedule_timer = MagicMock()

        q.add("t1", [_human("msg1")], user_id="u1", agent_name="director")
        q.add("t1", [_human("msg2")], user_id="u1", agent_name="director")
        assert q.pending_count == 1  # second replaces first

    def test_add_different_threads_kept_separate(self, monkeypatch):
        monkeypatch.setattr("deerflow.agents.memory.mlm_queue.get_memory_config", _enabled_config)
        q = MlmUpdateQueue()
        q._schedule_timer = MagicMock()

        q.add("t1", [_human("x")], user_id="u1", agent_name="director")
        q.add("t2", [_human("y")], user_id="u1", agent_name="director")
        assert q.pending_count == 2

    def test_add_noop_when_memory_disabled(self, monkeypatch):
        monkeypatch.setattr("deerflow.agents.memory.mlm_queue.get_memory_config", _disabled_config)
        q = MlmUpdateQueue()
        q.add("t1", [_human("hi")])
        assert q.pending_count == 0

    def test_clear_empties_queue(self, monkeypatch):
        monkeypatch.setattr("deerflow.agents.memory.mlm_queue.get_memory_config", _enabled_config)
        q = MlmUpdateQueue()
        q._schedule_timer = MagicMock()
        q.add("t1", [_human("x")])
        q.clear()
        assert q.pending_count == 0


# ---------------------------------------------------------------------------
# _process_one (async, mocked extractors + repo)
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_process_one_calls_user_and_agent_extraction():
    """_process_one should call user + agent extraction when both are set."""
    from deerflow.agents.memory.extractor import ExtractionResult

    user_result = ExtractionResult(scope_key="", facts=[{"content": "fact U"}], summary="sum U")
    agent_result = ExtractionResult(scope_key="", facts=[{"content": "fact A"}], summary="sum A")

    repo = _fake_repo()
    ctx = MlmContext(thread_id="t1", messages=[_human("x"), _ai("y")], user_id="u1", agent_name="director")

    with (
        patch("deerflow.agents.memory.mlm_queue.get_memory_repository", return_value=repo),
        patch("deerflow.agents.memory.mlm_queue.extract_user_knowledge", new=AsyncMock(return_value=[user_result])) as mock_user,
        patch("deerflow.agents.memory.mlm_queue.extract_project_knowledge", new=AsyncMock(return_value=[])) as mock_proj,
        patch("deerflow.agents.memory.mlm_queue.extract_agent_knowledge", new=AsyncMock(return_value=agent_result)) as mock_agent,
    ):
        q = MlmUpdateQueue()
        await q._process_one(ctx)

    mock_user.assert_called_once()
    mock_agent.assert_called_once()
    mock_proj.assert_not_called()  # no project_id
    repo.upsert_user_scope.assert_called_once_with("u1", "", [{"content": "fact U"}], "sum U")
    repo.upsert_agent.assert_called_once_with("director", [{"content": "fact A"}], "sum A")


@pytest.mark.anyio
async def test_process_one_calls_project_extraction():
    """_process_one should call project extraction when project_id is set."""
    from deerflow.agents.memory.extractor import ExtractionResult

    proj_result = ExtractionResult(scope_key="", facts=[{"content": "proj fact"}], summary=None)
    agent_result = ExtractionResult(scope_key="", facts=[], summary=None)

    repo = _fake_repo()
    ctx = MlmContext(
        thread_id="t1", messages=[_human("x"), _ai("y")],
        user_id="u1", agent_name="director", project_id="proj-1",
    )

    with (
        patch("deerflow.agents.memory.mlm_queue.get_memory_repository", return_value=repo),
        patch("deerflow.agents.memory.mlm_queue.extract_user_knowledge", new=AsyncMock(return_value=[])),
        patch("deerflow.agents.memory.mlm_queue.extract_project_knowledge", new=AsyncMock(return_value=[proj_result])) as mock_proj,
        patch("deerflow.agents.memory.mlm_queue.extract_agent_knowledge", new=AsyncMock(return_value=agent_result)),
    ):
        q = MlmUpdateQueue()
        await q._process_one(ctx)

    mock_proj.assert_called_once()
    repo.upsert_project_scope.assert_called_once_with("proj-1", "", [{"content": "proj fact"}], None)


@pytest.mark.anyio
async def test_process_one_skips_when_no_repo():
    """_process_one should be a no-op when there is no configured repository."""
    with (
        patch("deerflow.agents.memory.mlm_queue.get_memory_repository", return_value=None),
        patch("deerflow.agents.memory.mlm_queue.extract_user_knowledge", new=AsyncMock()) as mock_u,
    ):
        q = MlmUpdateQueue()
        ctx = MlmContext(thread_id="t1", messages=[], user_id="u1", agent_name="director")
        await q._process_one(ctx)
    mock_u.assert_not_called()


@pytest.mark.anyio
async def test_process_one_skips_agent_when_no_facts_or_summary():
    """upsert_agent should NOT be called if the extraction result is empty."""
    from deerflow.agents.memory.extractor import ExtractionResult

    empty_agent_result = ExtractionResult(scope_key="", facts=[], summary=None)

    repo = _fake_repo()
    ctx = MlmContext(thread_id="t1", messages=[_human("x"), _ai("y")], agent_name="director")

    with (
        patch("deerflow.agents.memory.mlm_queue.get_memory_repository", return_value=repo),
        patch("deerflow.agents.memory.mlm_queue.extract_agent_knowledge", new=AsyncMock(return_value=empty_agent_result)),
    ):
        q = MlmUpdateQueue()
        await q._process_one(ctx)

    repo.upsert_agent.assert_not_called()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _human(text: str):
    return SimpleNamespace(type="human", content=text)


def _ai(text: str):
    return SimpleNamespace(type="ai", content=text)


def _enabled_config():
    return SimpleNamespace(mlm_enabled=True, debounce_seconds=30)


def _disabled_config():
    return SimpleNamespace(mlm_enabled=False, debounce_seconds=30)


def _fake_repo():
    repo = MagicMock()
    repo.load_user_scopes = AsyncMock(return_value=[])
    repo.load_project_scopes = AsyncMock(return_value=[])
    repo.load_agent = AsyncMock(return_value=None)
    repo.upsert_user_scope = AsyncMock(return_value=True)
    repo.upsert_project_scope = AsyncMock(return_value=True)
    repo.upsert_agent = AsyncMock(return_value=True)
    return repo
