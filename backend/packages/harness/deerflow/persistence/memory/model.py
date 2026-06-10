"""ORM models for multi-level memory storage.

Three tables cover all knowledge scopes:
- memory_user:    per-user knowledge, optionally scoped to an agent
- memory_project: per-project knowledge, optionally scoped to an agent or user role
- memory_agent:   global agent knowledge shared across all users and projects

Each row stores a JSON array of facts plus an optional plain-text summary.
Optimistic locking via the ``version`` column prevents lost updates under
concurrent writes from multiple MQ consumer pods.
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import DateTime, Index, Integer, String, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from deerflow.persistence.base import Base


class MemoryUserRow(Base):
    """User-scoped knowledge, optionally narrowed to a specific agent.

    Primary key: (user_id, scope_key)

    scope_key values:
      ""               — general user traits, independent of agent or project
      "agent:{name}"   — user's working preferences when talking to that agent
    """

    __tablename__ = "memory_user"

    user_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    scope_key: Mapped[str] = mapped_column(String(256), primary_key=True, default="")
    summary: Mapped[str | None] = mapped_column(Text)
    facts: Mapped[str] = mapped_column(Text, default="[]")  # JSON array
    version: Mapped[int] = mapped_column(Integer, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))


class MemoryProjectRow(Base):
    """Project-scoped knowledge, optionally narrowed to an agent or user role.

    Primary key: (project_id, scope_key)

    scope_key values:
      ""               — general project facts, independent of agent or user
      "agent:{name}"   — agent's specialised knowledge about this project
      "user:{uid}"     — user's role and responsibilities within this project
    """

    __tablename__ = "memory_project"

    project_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    scope_key: Mapped[str] = mapped_column(String(256), primary_key=True, default="")
    summary: Mapped[str | None] = mapped_column(Text)
    facts: Mapped[str] = mapped_column(Text, default="[]")  # JSON array
    version: Mapped[int] = mapped_column(Integer, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))


class MemoryAgentRow(Base):
    """Global agent knowledge shared across all users and projects.

    Primary key: agent_name

    Stores tool performance experience, known failure patterns, and prompt
    heuristics accumulated from all threads using this agent.  Every consumer
    instance runs its own ``memory_extraction_loop``, so writes to a hot
    agent row may race across instances; ``upsert_agent`` therefore uses the
    same optimistic-locking (version CAS + retry) strategy as the user and
    project scopes.
    """

    __tablename__ = "memory_agent"

    agent_name: Mapped[str] = mapped_column(String(128), primary_key=True)
    summary: Mapped[str | None] = mapped_column(Text)
    facts: Mapped[str] = mapped_column(Text, default="[]")  # JSON array
    version: Mapped[int] = mapped_column(Integer, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))


class MemoryExtractionRow(Base):
    """DB-backed extraction queue: one pending task per thread (PK=thread_id).

    Replaces the in-process ``MlmUpdateQueue`` so that under the slot-driven
    scheduler — where successive turns of one thread may run on different
    consumer instances — extraction enqueue/processing survives both
    cross-instance dispatch and instance crashes.

    Lifecycle (see multi-level-memory设计.md §三):
    - ``MlmMiddleware.aafter_agent`` upserts one row per terminal turn, keyed
      by ``thread_id`` (idempotent merge: latest context wins, claim reset).
    - ``not_before = now + debounce`` gates the debounce window without a Timer.
    - ``memory_extraction_loop`` on every instance claims rows whose
      ``not_before <= now`` via FOR UPDATE SKIP LOCKED, stamping
      ``claimed_by``/``claimed_at`` so stale (crashed) claims can be recovered.

    Scope columns are nullable: a degraded-context turn may enqueue without a
    resolved user/agent/project.
    """

    __tablename__ = "memory_extraction_queue"

    thread_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    user_id: Mapped[str | None] = mapped_column(String(128))
    agent_name: Mapped[str | None] = mapped_column(String(128))
    project_id: Mapped[str | None] = mapped_column(String(128))
    not_before: Mapped[datetime] = mapped_column(DateTime(timezone=True))  # debounce gate
    claimed_by: Mapped[str | None] = mapped_column(String(128))  # instance processing this row
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))  # stale-recovery basis
    attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))

    __table_args__ = (
        # Claim path scans only unclaimed rows ordered by not_before. Partial index
        # keeps it small; both dialects support it (cf. consumer models.py ux_thread_running).
        Index(
            "ix_mem_extract_claimable",
            "not_before",
            postgresql_where=text("claimed_by IS NULL"),
            sqlite_where=text("claimed_by IS NULL"),
        ),
    )
