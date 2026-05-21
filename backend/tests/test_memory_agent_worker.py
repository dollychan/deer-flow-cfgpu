"""Tests for deerflow.workers.memory_agent_worker."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from deerflow.workers.memory_agent_worker import AgentKnowledgeEvent, MemoryAgentWorker


def _fake_repo(*, upsert_ok: bool = True) -> AsyncMock:
    repo = AsyncMock()
    repo.upsert_agent = AsyncMock(return_value=upsert_ok)
    return repo


class TestMemoryAgentWorker:
    @pytest.mark.anyio
    async def test_handle_calls_upsert_agent(self):
        repo = _fake_repo()
        worker = MemoryAgentWorker()
        event = AgentKnowledgeEvent(agent_name="director", facts=[{"content": "fact A"}], summary="s")

        with patch("deerflow.workers.memory_agent_worker.get_memory_repository", return_value=repo):
            await worker._handle(event)

        repo.upsert_agent.assert_called_once_with("director", [{"content": "fact A"}], "s")

    @pytest.mark.anyio
    async def test_handle_noop_when_no_repo(self):
        repo = _fake_repo()
        worker = MemoryAgentWorker()
        event = AgentKnowledgeEvent(agent_name="director", facts=[])

        with patch("deerflow.workers.memory_agent_worker.get_memory_repository", return_value=None):
            await worker._handle(event)

        repo.upsert_agent.assert_not_called()

    @pytest.mark.anyio
    async def test_handle_logs_warning_on_false_return(self):
        repo = _fake_repo(upsert_ok=False)
        worker = MemoryAgentWorker()
        event = AgentKnowledgeEvent(agent_name="director", facts=[])

        with patch("deerflow.workers.memory_agent_worker.get_memory_repository", return_value=repo):
            await worker._handle(event)  # should not raise

    @pytest.mark.anyio
    async def test_run_processes_event_and_stops(self):
        repo = _fake_repo()
        worker = MemoryAgentWorker()
        event = AgentKnowledgeEvent(agent_name="coder", facts=[{"content": "f"}], summary=None)

        with patch("deerflow.workers.memory_agent_worker.get_memory_repository", return_value=repo):
            await worker.publish(event)
            await worker.stop()
            await worker.run()

        repo.upsert_agent.assert_called_once_with("coder", [{"content": "f"}], None)

    @pytest.mark.anyio
    async def test_publish_and_handle_multiple_events(self):
        repo = _fake_repo()
        worker = MemoryAgentWorker()

        events = [
            AgentKnowledgeEvent(agent_name="director", facts=[{"content": f"fact {i}"}])
            for i in range(3)
        ]

        with patch("deerflow.workers.memory_agent_worker.get_memory_repository", return_value=repo):
            for e in events:
                await worker.publish(e)
            await worker.stop()
            await worker.run()

        assert repo.upsert_agent.call_count == 3
