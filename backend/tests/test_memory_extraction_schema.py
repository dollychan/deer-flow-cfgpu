"""Phase G1 — schema tests for the DB-backed memory extraction queue.

Covers ``MemoryExtractionRow`` (table ``memory_extraction_queue``):
- the table is created by ``Base.metadata.create_all`` on SQLite (cross-dialect
  smoke: the partial index DDL must not raise on a non-Postgres backend);
- a row can be inserted and round-tripped with the expected columns/defaults;
- the partial claimable index exists;
- the model is registered on ``Base.metadata`` via ``persistence.models``.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import inspect, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from deerflow.persistence.base import Base
from deerflow.persistence.memory.model import MemoryExtractionRow


@pytest.fixture
async def session_factory():
    """Fresh in-memory SQLite with all ORM tables created."""
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sf = async_sessionmaker(engine, expire_on_commit=False)
    yield sf
    await engine.dispose()


class TestTableCreation:
    @pytest.mark.anyio
    async def test_table_created_on_sqlite(self, session_factory):
        """create_all must build the table (partial index DDL must not raise)."""
        engine = session_factory.kw["bind"]

        def _tables(conn):
            return set(inspect(conn).get_table_names())

        async with engine.connect() as conn:
            tables = await conn.run_sync(_tables)
        assert "memory_extraction_queue" in tables

    @pytest.mark.anyio
    async def test_claimable_partial_index_exists(self, session_factory):
        engine = session_factory.kw["bind"]

        def _indexes(conn):
            return {ix["name"] for ix in inspect(conn).get_indexes("memory_extraction_queue")}

        async with engine.connect() as conn:
            names = await conn.run_sync(_indexes)
        assert "ix_mem_extract_claimable" in names


class TestRowRoundTrip:
    @pytest.mark.anyio
    async def test_insert_and_read_back(self, session_factory):
        now = datetime.now(UTC)
        async with session_factory() as session:
            session.add(
                MemoryExtractionRow(
                    thread_id="t-1",
                    user_id="alice",
                    agent_name="cf-dream",
                    project_id="proj-1",
                    not_before=now,
                    updated_at=now,
                )
            )
            await session.commit()

        async with session_factory() as session:
            row = await session.get(MemoryExtractionRow, "t-1")
        assert row is not None
        assert row.thread_id == "t-1"
        assert row.user_id == "alice"
        assert row.agent_name == "cf-dream"
        assert row.project_id == "proj-1"
        # unclaimed defaults
        assert row.claimed_by is None
        assert row.claimed_at is None
        assert row.attempt_count == 0

    @pytest.mark.anyio
    async def test_thread_id_is_primary_key(self, session_factory):
        """PK=thread_id means a second add for the same thread is one logical task."""
        now = datetime.now(UTC)
        async with session_factory() as session:
            row = await session.get(MemoryExtractionRow, "t-x")
            assert row is None
            session.add(MemoryExtractionRow(thread_id="t-x", not_before=now, updated_at=now))
            await session.commit()

        async with session_factory() as session:
            result = await session.execute(select(MemoryExtractionRow).where(MemoryExtractionRow.thread_id == "t-x"))
            rows = list(result.scalars().all())
        assert len(rows) == 1

    @pytest.mark.anyio
    async def test_nullable_scope_columns(self, session_factory):
        """user_id/agent_name/project_id may be NULL (degraded-context enqueue)."""
        now = datetime.now(UTC)
        async with session_factory() as session:
            session.add(MemoryExtractionRow(thread_id="t-null", not_before=now, updated_at=now))
            await session.commit()

        async with session_factory() as session:
            row = await session.get(MemoryExtractionRow, "t-null")
        assert row.user_id is None
        assert row.agent_name is None
        assert row.project_id is None


class TestModelRegistration:
    def test_registered_on_metadata(self):
        # Importing the registration entry point must pull in MemoryExtractionRow.
        import deerflow.persistence.models  # noqa: F401

        assert "memory_extraction_queue" in Base.metadata.tables

    def test_exported_from_models_init(self):
        from deerflow.persistence import models

        assert "MemoryExtractionRow" in models.__all__
