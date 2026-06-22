"""SQLAlchemy-backed memory repository.

Provides load and upsert operations for the three memory tables.

Concurrency strategy:
- memory_user / memory_project / memory_agent: optimistic locking (version
  CAS + retry). Every consumer instance runs its own ``memory_extraction_loop``,
  so multiple instances may write the same row of any of the three tables
  concurrently.
"""

from __future__ import annotations

import json
import logging
import math
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete as sa_delete
from sqlalchemy import or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from deerflow.config.mlm_config import get_mlm_config
from deerflow.persistence.memory.model import MemoryAgentRow, MemoryExtractionRow, MemoryProjectRow, MemoryUserRow

logger = logging.getLogger(__name__)

_MAX_RETRIES = 3

# Neutral confidence used when a fact omits (or malforms) its ``confidence``
# field. Applied only to the cap-sort ordering so un-annotated facts are
# neither preferred nor evicted first; the write-time threshold gate
# deliberately leaves such facts untouched (see ``merge_facts``).
_DEFAULT_CONFIDENCE = 0.5


def _coerce_confidence(value: object) -> float | None:
    """Return *value* as a finite float in [0, 1], or None if absent/invalid.

    Booleans are rejected (``True``/``False`` are not confidences). Strings are
    accepted when they parse as a float so model output that quotes the number
    still counts. Out-of-range values are clamped into [0, 1].
    """
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        try:
            value = float(value)
        except ValueError:
            return None
    if not isinstance(value, (int, float)):
        return None
    value = float(value)
    if not math.isfinite(value):
        return None
    return min(1.0, max(0.0, value))


def _confidence_sort_key(fact: dict) -> float:
    """Confidence used to rank facts when capping; missing → neutral default."""
    conf = _coerce_confidence(fact.get("confidence"))
    return conf if conf is not None else _DEFAULT_CONFIDENCE


def merge_facts(
    existing: list[dict],
    candidate: list[dict],
    *,
    confidence_threshold: float = 0.0,
    max_facts: int | None = None,
) -> list[dict]:
    """Merge two fact lists, deduplicating by the ``content`` field.

    Candidate facts win when their content matches an existing fact, so
    newer extractions naturally overwrite stale ones.

    Args:
        existing: Facts already persisted for this row (never re-filtered).
        candidate: Newly extracted facts to merge in.
        confidence_threshold: A candidate fact whose *explicit* confidence is
            below this value is dropped. Candidates that omit confidence are
            kept (benefit of the doubt) so the gate can be rolled out before
            every extraction skill emits the field.
        max_facts: When set, the merged result is capped to this many facts,
            keeping the highest-confidence ones (missing confidence sorts as
            the neutral default). ``None`` leaves the list uncapped.

    Returns:
        The merged (and optionally filtered/capped) fact list.
    """
    seen: dict[str, dict] = {f.get("content", ""): f for f in existing}
    for fact in candidate:
        conf = _coerce_confidence(fact.get("confidence"))
        if conf is not None and conf < confidence_threshold:
            continue
        seen[fact.get("content", "")] = fact

    merged = list(seen.values())
    if max_facts is not None and len(merged) > max_facts:
        merged.sort(key=_confidence_sort_key, reverse=True)
        merged = merged[:max_facts]
    return merged


class MemoryRepository:
    """Repository for multi-level memory tables.

    Each public method opens and closes its own short-lived session so no
    connection is held across retries or between calls.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._sf = session_factory

    @staticmethod
    def _fact_limits() -> tuple[float, int]:
        """Read the active (confidence_threshold, max_facts) write-time limits."""
        cfg = get_mlm_config()
        return cfg.fact_confidence_threshold, cfg.max_facts_per_scope

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
        Candidate facts are confidence-gated and the merged list is capped per
        ``MlmConfig`` (``fact_confidence_threshold`` / ``max_facts_per_scope``).
        Returns True on success, False if all retries are exhausted.
        """
        threshold, max_facts = self._fact_limits()
        now = datetime.now(UTC)
        for attempt in range(_MAX_RETRIES):
            try:
                async with self._sf() as session:
                    row = await session.get(MemoryUserRow, (user_id, scope_key))
                    if row is None:
                        new_facts = merge_facts([], candidate_facts, confidence_threshold=threshold, max_facts=max_facts)
                        session.add(
                            MemoryUserRow(
                                user_id=user_id,
                                scope_key=scope_key,
                                facts=json.dumps(new_facts, ensure_ascii=False),
                                summary=candidate_summary,
                                version=0,
                                updated_at=now,
                            )
                        )
                        await session.commit()
                        return True

                    merged = merge_facts(json.loads(row.facts), candidate_facts, confidence_threshold=threshold, max_facts=max_facts)
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

        Same CAS strategy as ``upsert_user_scope``, including the per-``MlmConfig``
        confidence gate and fact cap.
        """
        threshold, max_facts = self._fact_limits()
        now = datetime.now(UTC)
        for attempt in range(_MAX_RETRIES):
            try:
                async with self._sf() as session:
                    row = await session.get(MemoryProjectRow, (project_id, scope_key))
                    if row is None:
                        new_facts = merge_facts([], candidate_facts, confidence_threshold=threshold, max_facts=max_facts)
                        session.add(
                            MemoryProjectRow(
                                project_id=project_id,
                                scope_key=scope_key,
                                facts=json.dumps(new_facts, ensure_ascii=False),
                                summary=candidate_summary,
                                version=0,
                                updated_at=now,
                            )
                        )
                        await session.commit()
                        return True

                    merged = merge_facts(json.loads(row.facts), candidate_facts, confidence_threshold=threshold, max_facts=max_facts)
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
        """Write agent knowledge using optimistic locking.

        Under the slot-driven scheduler every consumer instance runs its own
        ``memory_extraction_loop``, so multiple instances may write the same
        ``memory_agent`` row concurrently (the old single-writer Memory Update
        Worker is gone). Same CAS strategy as ``upsert_user_scope``: on each
        attempt read current row → merge facts → version-CAS UPDATE. The same
        per-``MlmConfig`` confidence gate and fact cap apply.
        Returns True on success, False if all retries are exhausted.
        """
        threshold, max_facts = self._fact_limits()
        now = datetime.now(UTC)
        for attempt in range(_MAX_RETRIES):
            try:
                async with self._sf() as session:
                    row = await session.get(MemoryAgentRow, agent_name)
                    if row is None:
                        new_facts = merge_facts([], candidate_facts, confidence_threshold=threshold, max_facts=max_facts)
                        session.add(
                            MemoryAgentRow(
                                agent_name=agent_name,
                                facts=json.dumps(new_facts, ensure_ascii=False),
                                summary=candidate_summary,
                                version=0,
                                updated_at=now,
                            )
                        )
                        await session.commit()
                        return True

                    merged = merge_facts(json.loads(row.facts), candidate_facts, confidence_threshold=threshold, max_facts=max_facts)
                    result = await session.execute(
                        update(MemoryAgentRow)
                        .where(
                            MemoryAgentRow.agent_name == agent_name,
                            MemoryAgentRow.version == row.version,
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
                    logger.debug("upsert_agent: version conflict, retrying (%d/%d)", attempt + 1, _MAX_RETRIES)

            except IntegrityError:
                logger.debug("upsert_agent: concurrent INSERT, retrying (%d/%d)", attempt + 1, _MAX_RETRIES)

        logger.warning("upsert_agent: gave up after %d retries for %s", _MAX_RETRIES, agent_name)
        return False

    # ── Extraction queue (Phase G2) ──────────────────────────────────────────

    async def enqueue_extraction(
        self,
        thread_id: str,
        *,
        user_id: str | None,
        agent_name: str | None,
        project_id: str | None,
        debounce_seconds: int,
    ) -> None:
        """Enqueue one extraction task per thread, idempotently (PK=thread_id).

        On conflict the existing row is refreshed to the latest turn's dims and
        its debounce window is pushed forward (``not_before = now + debounce``).
        Any in-flight claim is reset (``claimed_by``/``claimed_at`` → NULL) and
        ``attempt_count`` → 0 so the worker re-extracts from the newest
        checkpoint rather than a half-processed older state (design §三).
        """
        now = datetime.now(UTC)
        not_before = now + timedelta(seconds=debounce_seconds)
        values = {
            "thread_id": thread_id,
            "user_id": user_id,
            "agent_name": agent_name,
            "project_id": project_id,
            "not_before": not_before,
            "claimed_by": None,
            "claimed_at": None,
            "attempt_count": 0,
            "updated_at": now,
        }
        set_ = {k: values[k] for k in ("user_id", "agent_name", "project_id", "not_before", "claimed_by", "claimed_at", "attempt_count", "updated_at")}

        async with self._sf() as session:
            dialect = (await session.connection()).dialect.name
            if dialect == "postgresql":
                stmt = pg_insert(MemoryExtractionRow).values(**values).on_conflict_do_update(index_elements=["thread_id"], set_=set_)
                await session.execute(stmt)
            elif dialect == "sqlite":
                stmt = sqlite_insert(MemoryExtractionRow).values(**values).on_conflict_do_update(index_elements=["thread_id"], set_=set_)
                await session.execute(stmt)
            else:
                # Generic fallback: UPDATE first, INSERT if absent.
                result = await session.execute(update(MemoryExtractionRow).where(MemoryExtractionRow.thread_id == thread_id).values(**set_))
                if (result.rowcount or 0) == 0:
                    session.add(MemoryExtractionRow(**values))
            await session.commit()

    async def claim_extraction(self, instance_id: str, *, stale_after_seconds: int) -> MemoryExtractionRow | None:
        """Atomically claim the earliest due extraction task, or return None.

        Eligible rows: ``not_before <= now`` AND (unclaimed OR the claim is
        stale, i.e. ``claimed_at < now - stale_after``). On Postgres the SELECT
        uses ``FOR UPDATE SKIP LOCKED`` so concurrent instances pick distinct
        rows; on SQLite (single process, sequential) the lock clause is a no-op.
        """
        now = datetime.now(UTC)
        stale_before = now - timedelta(seconds=stale_after_seconds)
        async with self._sf() as session:
            async with session.begin():
                row = (
                    await session.execute(
                        select(MemoryExtractionRow)
                        .where(
                            MemoryExtractionRow.not_before <= now,
                            or_(
                                MemoryExtractionRow.claimed_by.is_(None),
                                MemoryExtractionRow.claimed_at < stale_before,
                            ),
                        )
                        .order_by(MemoryExtractionRow.not_before.asc())
                        .limit(1)
                        .with_for_update(skip_locked=True)
                    )
                ).scalars().first()
                if row is None:
                    return None
                row.claimed_by = instance_id
                row.claimed_at = now
                row.updated_at = now
            return row

    async def delete_extraction(self, thread_id: str) -> None:
        """Remove a finished extraction row (no-op if already gone)."""
        async with self._sf() as session:
            await session.execute(sa_delete(MemoryExtractionRow).where(MemoryExtractionRow.thread_id == thread_id))
            await session.commit()

    async def bump_attempt(self, thread_id: str, *, max_attempts: int) -> bool:
        """Record a failed extraction attempt.

        Increments ``attempt_count``. If the new count reaches ``max_attempts``
        the row is dead-lettered (deleted) and True is returned; otherwise the
        claim is released (``claimed_by``/``claimed_at`` → NULL) so another
        instance can retry after the stale window, and False is returned.
        """
        async with self._sf() as session:
            async with session.begin():
                row = await session.get(MemoryExtractionRow, thread_id)
                if row is None:
                    return False
                row.attempt_count += 1
                if row.attempt_count >= max_attempts:
                    await session.delete(row)
                    return True
                row.claimed_by = None
                row.claimed_at = None
                row.updated_at = datetime.now(UTC)
            return False

    async def peek_extraction(self, thread_id: str) -> MemoryExtractionRow | None:
        """Return the queue row for a thread without claiming it (read-only)."""
        async with self._sf() as session:
            return await session.get(MemoryExtractionRow, thread_id)

    async def all_extractions(self) -> list[MemoryExtractionRow]:
        """Return every queue row (diagnostics / tests)."""
        async with self._sf() as session:
            result = await session.execute(select(MemoryExtractionRow))
            return list(result.scalars().all())


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
