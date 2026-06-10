"""Phase G4 — DB-backed MLM extraction worker.

Covers the two layers of ``deerflow.agents.memory.extraction_worker``:

- ``load_latest_thread_messages`` — reads ``channel_values["messages"]`` from the
  thread's latest checkpoint via the injected checkpointer; degrades to ``[]``
  when no checkpointer / no checkpoint exists.
- ``process_extraction`` — checkpoint readback → ``filter_messages_for_memory`` →
  three-scope extract → optimistic-lock upsert; gated per-dim; no-op on empty.
- ``run_extraction_loop`` — claim → process → delete on success; ``bump_attempt``
  (dead-letter at the attempt limit) on failure; never starts on backend=memory;
  idles while MLM is disabled at runtime.

In-memory SQLite: ``with_for_update(skip_locked=True)`` is a no-op, matching the
single-process claim degradation documented for the queue.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import AIMessage, HumanMessage
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from deerflow.agents.memory import extraction_worker as ew
from deerflow.agents.memory.extractor import ExtractionResult
from deerflow.persistence.base import Base
from deerflow.persistence.memory.model import MemoryExtractionRow
from deerflow.persistence.memory.repository import MemoryRepository

WORKER = "deerflow.agents.memory.extraction_worker"


@pytest.fixture
async def repo():
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sf = async_sessionmaker(engine, expire_on_commit=False)
    yield MemoryRepository(sf)
    await engine.dispose()


def _checkpointer(messages):
    """Fake checkpointer whose latest checkpoint carries *messages*."""
    tup = MagicMock()
    tup.checkpoint = {"channel_values": {"messages": list(messages)}}
    ck = MagicMock()
    ck.aget_tuple = AsyncMock(return_value=tup)
    return ck


def _convo():
    return [HumanMessage(content="please draw a cat"), AIMessage(content="here is your cat")]


def _enabled_config():
    cfg = MagicMock()
    cfg.enabled = True
    return cfg


def _disabled_config():
    cfg = MagicMock()
    cfg.enabled = False
    return cfg


# ---------------------------------------------------------------------------
# load_latest_thread_messages
# ---------------------------------------------------------------------------


class TestLoadLatestThreadMessages:
    @pytest.mark.anyio
    async def test_none_checkpointer_returns_empty(self):
        assert await ew.load_latest_thread_messages("t-1", None) == []

    @pytest.mark.anyio
    async def test_no_checkpoint_returns_empty(self):
        ck = MagicMock()
        ck.aget_tuple = AsyncMock(return_value=None)
        assert await ew.load_latest_thread_messages("t-1", ck) == []

    @pytest.mark.anyio
    async def test_reads_messages_from_channel_values(self):
        msgs = _convo()
        ck = _checkpointer(msgs)
        out = await ew.load_latest_thread_messages("t-1", ck)
        assert out == msgs
        ck.aget_tuple.assert_awaited_once()
        # reads the thread's main namespace
        cfg = ck.aget_tuple.call_args.args[0]
        assert cfg["configurable"]["thread_id"] == "t-1"
        assert cfg["configurable"]["checkpoint_ns"] == ""

    @pytest.mark.anyio
    async def test_missing_messages_key_returns_empty(self):
        tup = MagicMock()
        tup.checkpoint = {"channel_values": {}}
        ck = MagicMock()
        ck.aget_tuple = AsyncMock(return_value=tup)
        assert await ew.load_latest_thread_messages("t-1", ck) == []


# ---------------------------------------------------------------------------
# process_extraction
# ---------------------------------------------------------------------------


def _row(thread_id="t-1", *, user_id="u1", agent_name="a1", project_id="p1"):
    return MemoryExtractionRow(
        thread_id=thread_id,
        user_id=user_id,
        agent_name=agent_name,
        project_id=project_id,
        not_before=datetime.now(UTC),
        attempt_count=0,
        updated_at=datetime.now(UTC),
    )


class TestProcessExtraction:
    @pytest.mark.anyio
    async def test_no_op_when_repository_missing(self):
        ext = AsyncMock()
        with (
            patch(f"{WORKER}.get_memory_repository", return_value=None),
            patch(f"{WORKER}.extract_user_knowledge", ext),
            patch(f"{WORKER}.extract_project_knowledge", ext),
            patch(f"{WORKER}.extract_agent_knowledge", ext),
        ):
            await ew.process_extraction(_row(), _checkpointer(_convo()))
        ext.assert_not_called()

    @pytest.mark.anyio
    async def test_no_op_when_no_messages(self, repo):
        ext = AsyncMock()
        with (
            patch(f"{WORKER}.get_memory_repository", return_value=repo),
            patch(f"{WORKER}.extract_user_knowledge", ext),
            patch(f"{WORKER}.extract_project_knowledge", ext),
            patch(f"{WORKER}.extract_agent_knowledge", ext),
        ):
            await ew.process_extraction(_row(), _checkpointer([]))
        ext.assert_not_called()

    @pytest.mark.anyio
    async def test_all_three_scopes_extracted_and_upserted(self, repo):
        user_ext = AsyncMock(return_value=[ExtractionResult(scope_key="", facts=[{"content": "likes cats"}], summary="u-sum")])
        proj_ext = AsyncMock(return_value=[ExtractionResult(scope_key="agent:a1", facts=[{"content": "proj fact"}], summary="p-sum")])
        agent_ext = AsyncMock(return_value=ExtractionResult(scope_key="", facts=[{"content": "tool X flaky"}], summary="a-sum"))
        with (
            patch(f"{WORKER}.get_memory_repository", return_value=repo),
            patch(f"{WORKER}.extract_user_knowledge", user_ext),
            patch(f"{WORKER}.extract_project_knowledge", proj_ext),
            patch(f"{WORKER}.extract_agent_knowledge", agent_ext),
        ):
            await ew.process_extraction(_row(), _checkpointer(_convo()))

        user_ext.assert_awaited_once()
        proj_ext.assert_awaited_once()
        agent_ext.assert_awaited_once()

        user_rows = await repo.load_user_scopes("u1")
        assert len(user_rows) == 1 and "likes cats" in user_rows[0].facts
        proj_rows = await repo.load_project_scopes("p1")
        assert len(proj_rows) == 1 and proj_rows[0].scope_key == "agent:a1"
        agent_row = await repo.load_agent("a1")
        assert agent_row is not None and "tool X flaky" in agent_row.facts

    @pytest.mark.anyio
    async def test_skips_project_scope_when_no_project_id(self, repo):
        user_ext = AsyncMock(return_value=[])
        proj_ext = AsyncMock(return_value=[])
        agent_ext = AsyncMock(return_value=ExtractionResult(scope_key=""))
        with (
            patch(f"{WORKER}.get_memory_repository", return_value=repo),
            patch(f"{WORKER}.extract_user_knowledge", user_ext),
            patch(f"{WORKER}.extract_project_knowledge", proj_ext),
            patch(f"{WORKER}.extract_agent_knowledge", agent_ext),
        ):
            await ew.process_extraction(_row(project_id=None), _checkpointer(_convo()))

        user_ext.assert_awaited_once()
        proj_ext.assert_not_called()
        agent_ext.assert_awaited_once()

    @pytest.mark.anyio
    async def test_skips_user_scope_when_no_agent_name(self, repo):
        user_ext = AsyncMock(return_value=[])
        proj_ext = AsyncMock(return_value=[])
        agent_ext = AsyncMock(return_value=ExtractionResult(scope_key=""))
        with (
            patch(f"{WORKER}.get_memory_repository", return_value=repo),
            patch(f"{WORKER}.extract_user_knowledge", user_ext),
            patch(f"{WORKER}.extract_project_knowledge", proj_ext),
            patch(f"{WORKER}.extract_agent_knowledge", agent_ext),
        ):
            await ew.process_extraction(_row(agent_name=None), _checkpointer(_convo()))

        # user + project both gated on agent_name; agent scope gated on agent_name too
        user_ext.assert_not_called()
        proj_ext.assert_not_called()
        agent_ext.assert_not_called()

    @pytest.mark.anyio
    async def test_empty_agent_result_does_not_upsert(self, repo):
        with (
            patch(f"{WORKER}.get_memory_repository", return_value=repo),
            patch(f"{WORKER}.extract_user_knowledge", AsyncMock(return_value=[])),
            patch(f"{WORKER}.extract_project_knowledge", AsyncMock(return_value=[])),
            patch(f"{WORKER}.extract_agent_knowledge", AsyncMock(return_value=ExtractionResult(scope_key="", facts=[], summary=None))),
        ):
            await ew.process_extraction(_row(), _checkpointer(_convo()))

        assert await repo.load_agent("a1") is None


# ---------------------------------------------------------------------------
# run_extraction_loop
# ---------------------------------------------------------------------------


async def _run_until(coro_factory, predicate, *, timeout=2.0):
    """Run the loop task until *predicate()* is true, then stop and await it."""
    stop = asyncio.Event()
    task = asyncio.create_task(coro_factory(stop))
    deadline = asyncio.get_event_loop().time() + timeout
    try:
        while not await predicate():
            if asyncio.get_event_loop().time() > deadline:
                raise AssertionError("predicate not satisfied before timeout")
            await asyncio.sleep(0.01)
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=1.0)


class TestRunExtractionLoop:
    @pytest.mark.anyio
    async def test_does_not_start_when_backend_memory(self):
        stop = asyncio.Event()
        with patch(f"{WORKER}.get_memory_repository", return_value=None):
            # returns immediately without ever touching stop_event
            await asyncio.wait_for(ew.run_extraction_loop(None, "inst-1", stop), timeout=1.0)

    @pytest.mark.anyio
    async def test_claims_processes_and_deletes_on_success(self, repo):
        await repo.enqueue_extraction("t-1", user_id="u1", agent_name="a1", project_id="p1", debounce_seconds=1)
        # make it immediately due
        await _backdate_not_before(repo, "t-1")

        async def gone():
            return await repo.peek_extraction("t-1") is None

        with (
            patch(f"{WORKER}.get_memory_repository", return_value=repo),
            patch(f"{WORKER}.get_mlm_config", _enabled_config),
            patch(f"{WORKER}.process_extraction", new=AsyncMock()) as proc,
        ):
            await _run_until(
                lambda stop: ew.run_extraction_loop(_checkpointer(_convo()), "inst-1", stop, idle_sleep=0.01),
                gone,
            )
        proc.assert_awaited()
        assert await repo.peek_extraction("t-1") is None

    @pytest.mark.anyio
    async def test_failure_dead_letters_after_max_attempts(self, repo):
        await repo.enqueue_extraction("t-1", user_id="u1", agent_name="a1", project_id="p1", debounce_seconds=1)
        await _backdate_not_before(repo, "t-1")

        async def gone():
            return await repo.peek_extraction("t-1") is None

        boom = AsyncMock(side_effect=RuntimeError("extractor down"))
        with (
            patch(f"{WORKER}.get_memory_repository", return_value=repo),
            patch(f"{WORKER}.get_mlm_config", _enabled_config),
            patch(f"{WORKER}.process_extraction", new=boom),
        ):
            await _run_until(
                lambda stop: ew.run_extraction_loop(_checkpointer(_convo()), "inst-1", stop, idle_sleep=0.01, max_attempts=3),
                gone,
            )
        # dead-lettered after exactly max_attempts failed attempts
        assert boom.await_count == 3
        assert await repo.peek_extraction("t-1") is None

    @pytest.mark.anyio
    async def test_idles_without_claiming_when_disabled(self):
        repo = MagicMock()
        repo.claim_extraction = AsyncMock(return_value=None)
        stop = asyncio.Event()
        with (
            patch(f"{WORKER}.get_memory_repository", return_value=repo),
            patch(f"{WORKER}.get_mlm_config", _disabled_config),
        ):
            task = asyncio.create_task(ew.run_extraction_loop(None, "inst-1", stop, idle_sleep=0.01))
            await asyncio.sleep(0.05)
            stop.set()
            await asyncio.wait_for(task, timeout=1.0)
        repo.claim_extraction.assert_not_called()


async def _backdate_not_before(repo: MemoryRepository, thread_id: str, seconds: int = 60) -> None:
    """Force a row's debounce gate into the past so claim picks it up."""
    async with repo._sf() as session:
        row = await session.get(MemoryExtractionRow, thread_id)
        row.not_before = datetime.now(UTC) - timedelta(seconds=seconds)
        await session.commit()
