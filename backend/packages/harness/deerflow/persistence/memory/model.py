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

from sqlalchemy import DateTime, Integer, String, Text
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
    heuristics accumulated from all threads using this agent.  Writes are
    serialised through a dedicated Memory Update Worker (single MQ consumer)
    so no optimistic-locking retry is needed here, but the version column
    is kept for auditability.
    """

    __tablename__ = "memory_agent"

    agent_name: Mapped[str] = mapped_column(String(128), primary_key=True)
    summary: Mapped[str | None] = mapped_column(Text)
    facts: Mapped[str] = mapped_column(Text, default="[]")  # JSON array
    version: Mapped[int] = mapped_column(Integer, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
