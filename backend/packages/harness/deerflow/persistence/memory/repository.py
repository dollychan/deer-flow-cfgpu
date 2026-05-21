"""SQLAlchemy-backed memory repository.

Provides load and upsert operations for the three memory tables.

Concurrency strategy:
- memory_user / memory_project: optimistic locking (version CAS + retry)
  Multiple MQ consumer pods may write the same row concurrently.
- memory_agent: no locking needed; the caller (Memory Update Worker) is
  a single instance, so no concurrent writes occur.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from deerflow.persistence.memory.model import MemoryAgentRow, MemoryProjectRow, MemoryUserRow

logger = logging.getLogger(__name__)

_MAX_RETRIES = 3


def merge_facts(existing: list[dict], candidate: list[dict]) -> list[dict]:
    """Merge two fact lists, deduplicating by the ``content`` field.

    Candidate facts win when their content matches an existing fact, so
    newer extractions naturally overwrite stale ones.
    """
    seen: dict[str, dict] = {f.get("content", ""): f for f in existing}
    for fact in candidate:
        seen[fact.get("content", "")] = fact
    return list(seen.values())


class MemoryRepository:
    """Repository for multi-level memory tables.

    Each public method opens and closes its own short-lived session so no
    connection is held across retries or between calls.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._sf = session_factory

    # ── Load ─────────────────────────────────────────────────────────────────

    async def load_user_scopes(self, user_id: str) -> list[MemoryUserRow]:
        """Return all scope rows for a user. Caller filters by active_dims."""
        async with self._sf() as session:
            result = await session.execute(select(MemoryUserRow).where(MemoryUserRow.user_id == user_id))
            return list(result.scalars().all())

    async def load_project_scopes(self, project_id: str) -> list[MemoryProjectRow]:
        """Return all scope rows for a project. Caller filters by active_dims."""
        async with self._sf() as session:
            result = await session.execute(select(MemoryProjectRow).where(MemoryProjectRow.project_id == project_id))
            return list(result.scalars().all())

    async def load_agent(self, agent_name: str) -> MemoryAgentRow | None:
        async with self._sf() as session:
            return await session.get(MemoryAgentRow, agent_name)

    # ── Upsert ───────────────────────────────────────────────────────────────

    async def upsert_user_scope(
        self,
        user_id: str,
        scope_key: str,
        candidate_facts: list[dict],
        candidate_summary: str | None,
    ) -> bool:
        """Write user-scope knowledge using optimistic locking.

        On each attempt: read current row → merge facts → CAS UPDATE.
        Returns True on success, False if all retries are exhausted.
        """
        now = datetime.now(UTC)
        for attempt in range(_MAX_RETRIES):
            try:
                async with self._sf() as session:
                    row = await session.get(MemoryUserRow, (user_id, scope_key))
                    if row is None:
                        session.add(
                            MemoryUserRow(
                                user_id=user_id,
                                scope_key=scope_key,
                                facts=json.dumps(candidate_facts, ensure_ascii=False),
                                summary=candidate_summary,
                                version=0,
                                updated_at=now,
                            )
                        )
                        await session.commit()
                        return True

                    merged = merge_facts(json.loads(row.facts), candidate_facts)
                    result = await session.execute(
                        update(MemoryUserRow)
                        .where(
                            MemoryUserRow.user_id == user_id,
                            MemoryUserRow.scope_key == scope_key,
                            MemoryUserRow.version == row.version,
                        )
                        .values(
                            facts=json.dumps(merged, ensure_ascii=False),
                            summary=candidate_summary or row.summary,
                            version=row.version + 1,
                            updated_at=now,
                        )
                    )
                    await session.commit()
                    if result.rowcount == 1:
                        return True
                    logger.debug("upsert_user_scope: version conflict, retrying (%d/%d)", attempt + 1, _MAX_RETRIES)

            except IntegrityError:
                logger.debug("upsert_user_scope: concurrent INSERT, retrying (%d/%d)", attempt + 1, _MAX_RETRIES)

        logger.warning("upsert_user_scope: gave up after %d retries for (%s, %s)", _MAX_RETRIES, user_id, scope_key)
        return False

    async def upsert_project_scope(
        self,
        project_id: str,
        scope_key: str,
        candidate_facts: list[dict],
        candidate_summary: str | None,
    ) -> bool:
        """Write project-scope knowledge using optimistic locking.

        Same CAS strategy as ``upsert_user_scope``.
        """
        now = datetime.now(UTC)
        for attempt in range(_MAX_RETRIES):
            try:
                async with self._sf() as session:
                    row = await session.get(MemoryProjectRow, (project_id, scope_key))
                    if row is None:
                        session.add(
                            MemoryProjectRow(
                                project_id=project_id,
                                scope_key=scope_key,
                                facts=json.dumps(candidate_facts, ensure_ascii=False),
                                summary=candidate_summary,
                                version=0,
                                updated_at=now,
                            )
                        )
                        await session.commit()
                        return True

                    merged = merge_facts(json.loads(row.facts), candidate_facts)
                    result = await session.execute(
                        update(MemoryProjectRow)
                        .where(
                            MemoryProjectRow.project_id == project_id,
                            MemoryProjectRow.scope_key == scope_key,
                            MemoryProjectRow.version == row.version,
                        )
                        .values(
                            facts=json.dumps(merged, ensure_ascii=False),
                            summary=candidate_summary or row.summary,
                            version=row.version + 1,
                            updated_at=now,
                        )
                    )
                    await session.commit()
                    if result.rowcount == 1:
                        return True
                    logger.debug("upsert_project_scope: version conflict, retrying (%d/%d)", attempt + 1, _MAX_RETRIES)

            except IntegrityError:
                logger.debug("upsert_project_scope: concurrent INSERT, retrying (%d/%d)", attempt + 1, _MAX_RETRIES)

        logger.warning("upsert_project_scope: gave up after %d retries for (%s, %s)", _MAX_RETRIES, project_id, scope_key)
        return False

    async def upsert_agent(
        self,
        agent_name: str,
        candidate_facts: list[dict],
        candidate_summary: str | None,
    ) -> bool:
        """Write agent knowledge. No optimistic locking needed here.

        The Memory Update Worker is the sole writer for memory_agent rows,
        so concurrent write conflicts cannot occur.
        """
        async with self._sf() as session:
            row = await session.get(MemoryAgentRow, agent_name)
            if row is None:
                session.add(
                    MemoryAgentRow(
                        agent_name=agent_name,
                        facts=json.dumps(candidate_facts, ensure_ascii=False),
                        summary=candidate_summary,
                        version=0,
                        updated_at=datetime.now(UTC),
                    )
                )
            else:
                merged = merge_facts(json.loads(row.facts), candidate_facts)
                row.facts = json.dumps(merged, ensure_ascii=False)
                row.summary = candidate_summary or row.summary
                row.version += 1
                row.updated_at = datetime.now(UTC)
            await session.commit()
            return True


def get_memory_repository() -> MemoryRepository | None:
    """Return a MemoryRepository bound to the active session factory.

    Returns None when the persistence backend is ``memory`` (no DB configured).
    Callers must handle the None case gracefully.
    """
    from deerflow.persistence.engine import get_session_factory

    sf = get_session_factory()
    if sf is None:
        return None
    return MemoryRepository(sf)
