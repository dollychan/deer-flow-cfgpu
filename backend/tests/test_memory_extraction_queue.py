"""Phase G2 — repository tests for the DB-backed memory extraction queue.

Covers the four queue operations on ``MemoryRepository``:
- ``enqueue_extraction`` — idempotent per-thread merge (one row per thread),
  refreshes dims + pushes ``not_before`` forward + resets any in-flight claim;
- ``claim_extraction`` — only picks rows whose ``not_before <= now`` and that are
  unclaimed or stale; stamps ``claimed_by``/``claimed_at``;
- ``delete_extraction`` — removes a finished row;
- ``bump_attempt`` — increments attempt_count and either releases the claim for
  retry or dead-letters (deletes) once ``max_attempts`` is exhausted.

In-memory SQLite: ``with_for_update(skip_locked=True)`` is a no-op there, which
matches the single-process degradation documented for the claim path.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from deerflow.persistence.base import Base
from deerflow.persistence.memory.model import MemoryExtractionRow
from deerflow.persistence.memory.repository import MemoryRepository


@pytest.fixture
async def repo():
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sf = async_sessionmaker(engine, expire_on_commit=False)
    yield MemoryRepository(sf)
    await engine.dispose()


# ---------------------------------------------------------------------------
# enqueue_extraction
# ---------------------------------------------------------------------------


class TestEnqueueExtraction:
    @pytest.mark.anyio
    async def test_insert_creates_single_row(self, repo):
        await repo.enqueue_extraction("t-1", user_id="u", agent_name="a", project_id="p", debounce_seconds=30)

        row = await repo.peek_extraction("t-1")
        assert row is not None
        assert row.thread_id == "t-1"
        assert row.user_id == "u"
        assert row.agent_name == "a"
        assert row.project_id == "p"
        assert row.claimed_by is None
        assert row.attempt_count == 0

    @pytest.mark.anyio
    async def test_second_enqueue_same_thread_keeps_one_row(self, repo):
        await repo.enqueue_extraction("t-1", user_id="u1", agent_name="a", project_id="p", debounce_seconds=30)
        await repo.enqueue_extraction("t-1", user_id="u2", agent_name="a2", project_id="p2", debounce_seconds=30)

        rows = await repo.all_extractions()
        assert len(rows) == 1
        # dims refreshed to the latest turn
        assert rows[0].user_id == "u2"
        assert rows[0].agent_name == "a2"
        assert rows[0].project_id == "p2"

    @pytest.mark.anyio
    async def test_not_before_pushed_forward(self, repo):
        await repo.enqueue_extraction("t-1", user_id="u", agent_name="a", project_id="p", debounce_seconds=30)
        first = (await repo.peek_extraction("t-1")).not_before
        await repo.enqueue_extraction("t-1", user_id="u", agent_name="a", project_id="p", debounce_seconds=120)
        second = (await repo.peek_extraction("t-1")).not_before
        assert second > first

    @pytest.mark.anyio
    async def test_enqueue_resets_inflight_claim(self, repo):
        # debounce 0 so the row is immediately claimable
        await repo.enqueue_extraction("t-1", user_id="u", agent_name="a", project_id="p", debounce_seconds=0)
        claimed = await repo.claim_extraction("inst-A", stale_after_seconds=300)
        assert claimed is not None and claimed.claimed_by == "inst-A"

        # a new terminal turn arrives mid-flight → claim must be invalidated
        await repo.enqueue_extraction("t-1", user_id="u", agent_name="a", project_id="p", debounce_seconds=0)
        row = await repo.peek_extraction("t-1")
        assert row.claimed_by is None
        assert row.claimed_at is None

    @pytest.mark.anyio
    async def test_enqueue_resets_attempt_count(self, repo):
        await repo.enqueue_extraction("t-1", user_id="u", agent_name="a", project_id="p", debounce_seconds=0)
        await repo.bump_attempt("t-1", max_attempts=5)  # attempt_count -> 1
        assert (await repo.peek_extraction("t-1")).attempt_count == 1

        # fresh terminal content supersedes prior failures
        await repo.enqueue_extraction("t-1", user_id="u", agent_name="a", project_id="p", debounce_seconds=0)
        assert (await repo.peek_extraction("t-1")).attempt_count == 0

    @pytest.mark.anyio
    async def test_nullable_dims_allowed(self, repo):
        await repo.enqueue_extraction("t-1", user_id=None, agent_name=None, project_id=None, debounce_seconds=0)
        row = await repo.peek_extraction("t-1")
        assert row.user_id is None and row.agent_name is None and row.project_id is None


# ---------------------------------------------------------------------------
# claim_extraction
# ---------------------------------------------------------------------------


class TestClaimExtraction:
    @pytest.mark.anyio
    async def test_claims_ready_row(self, repo):
        await repo.enqueue_extraction("t-1", user_id="u", agent_name="a", project_id="p", debounce_seconds=0)
        row = await repo.claim_extraction("inst-A", stale_after_seconds=300)
        assert row is not None
        assert row.thread_id == "t-1"
        assert row.claimed_by == "inst-A"
        assert row.claimed_at is not None

    @pytest.mark.anyio
    async def test_skips_row_not_yet_due(self, repo):
        await repo.enqueue_extraction("t-1", user_id="u", agent_name="a", project_id="p", debounce_seconds=300)
        row = await repo.claim_extraction("inst-A", stale_after_seconds=300)
        assert row is None

    @pytest.mark.anyio
    async def test_skips_already_claimed_fresh_row(self, repo):
        await repo.enqueue_extraction("t-1", user_id="u", agent_name="a", project_id="p", debounce_seconds=0)
        first = await repo.claim_extraction("inst-A", stale_after_seconds=300)
        assert first is not None
        second = await repo.claim_extraction("inst-B", stale_after_seconds=300)
        assert second is None  # still held by inst-A, not stale

    @pytest.mark.anyio
    async def test_reclaims_stale_row(self, repo):
        await repo.enqueue_extraction("t-1", user_id="u", agent_name="a", project_id="p", debounce_seconds=0)
        await repo.claim_extraction("inst-A", stale_after_seconds=300)

        # force the claim to look stale by backdating claimed_at
        await _backdate_claimed_at(repo, "t-1", seconds_ago=600)

        row = await repo.claim_extraction("inst-B", stale_after_seconds=300)
        assert row is not None
        assert row.claimed_by == "inst-B"

    @pytest.mark.anyio
    async def test_returns_none_when_queue_empty(self, repo):
        assert await repo.claim_extraction("inst-A", stale_after_seconds=300) is None

    @pytest.mark.anyio
    async def test_claims_earliest_not_before_first(self, repo):
        await repo.enqueue_extraction("t-late", user_id="u", agent_name="a", project_id="p", debounce_seconds=0)
        await _backdate_not_before(repo, "t-late", seconds_ago=10)
        await repo.enqueue_extraction("t-early", user_id="u", agent_name="a", project_id="p", debounce_seconds=0)
        await _backdate_not_before(repo, "t-early", seconds_ago=100)

        row = await repo.claim_extraction("inst-A", stale_after_seconds=300)
        assert row.thread_id == "t-early"


# ---------------------------------------------------------------------------
# delete_extraction / bump_attempt
# ---------------------------------------------------------------------------


class TestDeleteAndBump:
    @pytest.mark.anyio
    async def test_delete_removes_row(self, repo):
        await repo.enqueue_extraction("t-1", user_id="u", agent_name="a", project_id="p", debounce_seconds=0)
        await repo.delete_extraction("t-1")
        assert await repo.peek_extraction("t-1") is None

    @pytest.mark.anyio
    async def test_delete_missing_row_is_noop(self, repo):
        await repo.delete_extraction("nope")  # must not raise

    @pytest.mark.anyio
    async def test_bump_releases_claim_for_retry(self, repo):
        await repo.enqueue_extraction("t-1", user_id="u", agent_name="a", project_id="p", debounce_seconds=0)
        await repo.claim_extraction("inst-A", stale_after_seconds=300)

        dead = await repo.bump_attempt("t-1", max_attempts=3)
        assert dead is False
        row = await repo.peek_extraction("t-1")
        assert row is not None
        assert row.attempt_count == 1
        assert row.claimed_by is None  # released so another claim can retry

    @pytest.mark.anyio
    async def test_bump_dead_letters_after_max_attempts(self, repo):
        await repo.enqueue_extraction("t-1", user_id="u", agent_name="a", project_id="p", debounce_seconds=0)
        # max_attempts=2: first bump -> 1 (retry), second bump -> 2 (dead-letter)
        assert await repo.bump_attempt("t-1", max_attempts=2) is False
        dead = await repo.bump_attempt("t-1", max_attempts=2)
        assert dead is True
        assert await repo.peek_extraction("t-1") is None


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


async def _backdate_claimed_at(repo: MemoryRepository, thread_id: str, *, seconds_ago: int) -> None:
    async with repo._sf() as session:
        row = await session.get(MemoryExtractionRow, thread_id)
        row.claimed_at = datetime.now(UTC) - timedelta(seconds=seconds_ago)
        await session.commit()


async def _backdate_not_before(repo: MemoryRepository, thread_id: str, *, seconds_ago: int) -> None:
    async with repo._sf() as session:
        row = await session.get(MemoryExtractionRow, thread_id)
        row.not_before = datetime.now(UTC) - timedelta(seconds=seconds_ago)
        await session.commit()
