"""SQLAlchemy ORM models for the Consumer layer.

Four tables that extend deerflow's shared persistence Base.
Import this module before calling init_engine_from_config() so that
Base.metadata.create_all() discovers and creates these tables alongside
the core deerflow tables (runs, threads_meta, etc.).

Table overview:
  consumer_instances — running Consumer process registry + heartbeat
  thread_run_state   — per-thread execution state used for routing (idle/running/paused)
  thread_msg_queue   — message queue (followup/collect/resume/prefix/steer/fork/drain)
  processed_messages — idempotency log + transactional outbox (v2 D7)
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import JSON, Boolean, DateTime, Index, Integer, Text, UniqueConstraint, text
from sqlalchemy.orm import Mapped, mapped_column

from deerflow.persistence.base import Base


class ConsumerInstanceRow(Base):
    """One row per running Consumer process.

    Heartbeat is updated every 10 s; rows with stale heartbeats (> 60 s)
    are treated as dead by the watchdog and their threads are reclaimed.
    """

    __tablename__ = "consumer_instances"

    instance_id: Mapped[str] = mapped_column(Text, primary_key=True)
    # format: "{hostname}-{pid}"
    hostname: Mapped[str] = mapped_column(Text, nullable=False)
    pid: Mapped[int] = mapped_column(nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="active")
    # "active" | "draining" | "dead"
    registered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC))
    last_heartbeat: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC))


class ThreadRunStateRow(Base):
    """Per-thread routing state.

    Exactly one row per thread_id; upserted atomically with SELECT FOR UPDATE
    to ensure only one Consumer instance claims execution at a time.

    status lifecycle (v2, D3):
      idle    → claimed by Consumer → running
      running → run completes      → idle
      running → HIL interrupt      → paused   (approval gate folded into status, §4.5)
      paused  → resume claimed     → running
      paused  → cancel covers it   → idle

    v2 columns (D2/D4):
      cancel_watermark  — monotonic high-water of the "cancel all seq < N" prefix
                          barrier, folded at ingest (§6.4). Claim excludes rows
                          with thread_msg_seq < cancel_watermark.
      last_resolved_seq — monotonic high-water of the highest resolved seq
                          (renamed concept of the old thread_msg_seq; advanced via
                          GREATEST at claim and at cancel fold only, §4.2/§6.2.1).
                          This is the sole state L2 continuity depends on.

    Deprecated (v1 only, removed in Phase B/C once run_registry is rewritten):
      thread_msg_seq — superseded by last_resolved_seq (§4.2).
      drain_mode     — v2 collect is a claim-time strategy, not thread state (§6.2.2).
    """

    __tablename__ = "thread_run_state"

    thread_id: Mapped[str] = mapped_column(Text, primary_key=True)
    instance_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    # FK to consumer_instances.instance_id (no FK constraint; dead instances may vanish).
    # v2: NULL on idle placeholder rows created by cancel-fold (§6.4) / claim phase-2 (§6.3)
    # before any instance has claimed the thread.
    message_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    # message_id of the task currently being executed (dangling bookmark while paused, §4.2).
    # v2: NULL on idle placeholder rows that exist only to carry cancel_watermark.
    status: Mapped[str] = mapped_column(Text, nullable=False)
    # v2 three-state: "idle" | "running" | "paused"
    cancel_watermark: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # v2 (D2): cancel prefix-barrier high-water; claim excludes seq < this value (§6.4)
    last_resolved_seq: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # v2 (D4): highest-resolved-seq high-water (L2 last_seq); GREATEST-advanced (§4.2/§6.2.1)
    thread_msg_seq: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # DEPRECATED (v1): per-thread seq of current task; superseded by last_resolved_seq
    drain_mode: Mapped[str] = mapped_column(Text, nullable=False, default="followup")
    # DEPRECATED (v1): "followup" | "collect"; v2 collect is claim-time, not thread state
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # stale-run auto-retry count; reset to 0 after each run completes
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_heartbeat: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC))


class ThreadMsgQueueRow(Base):
    """Queue for messages associated with a thread (crash recovery + scheduling).

    A row lives its whole life as a single row: pending → running → deleted
    (terminal) / merged (folded into a sibling batch). status (not policy)
    expresses "currently running"; the claimed row is flipped in place to
    'running' and serves as the crash-recovery envelope (§4.3/§6.3).

    policy is the scheduling role, derived once at ingest and used purely by the
    Scheduler claim SQL: followup | collect | resume | prefix | steer | fork |
    drain (see QueuePolicy in constants.py for per-value semantics, §4.3).

    body stores the complete MQ envelope (schema_version, message_id, agent_name,
    user_id, project_id, payload.{messages,command,config,reply_config}, …) so
    TaskMessage.from_json(json.dumps(row.body)) reconstructs the message losslessly.
    """

    __tablename__ = "thread_msg_queue"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    thread_id: Mapped[str] = mapped_column(Text, nullable=False)
    message_id: Mapped[str] = mapped_column(Text, nullable=False)
    body: Mapped[dict] = mapped_column(JSON, nullable=False)
    # complete MQ envelope; TaskMessage.from_json(json.dumps(row.body)) reconstructs losslessly
    policy: Mapped[str] = mapped_column(Text, nullable=False, default="followup")
    # v2 set (§4.3): followup | collect | resume | prefix | steer | fork | drain
    # deprecated: current (→ status='running' row), cancel (→ cancel_watermark)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="pending")
    # v2 (D5): "pending" | "running" | "merged"; replaces consumed_at-NULL + policy='current'.
    # A claimed row is flipped in place to 'running' and serves as the crash-recovery
    # envelope (§6.3 step 5 — not deleted, not copied).
    claimed_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    # v2: instance_id that claimed this row (§6.3)
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # v2: claim timestamp
    thread_msg_seq: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # per-thread sequence number from the uplink message; rows ordered by this for dispatch.
    # write-once (immutable): drain rows are exempted from the cancel_watermark filter
    # instead of being re-stamped, preserving this invariant (§6.5 method B′).
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC))

    __table_args__ = (
        UniqueConstraint("message_id", name="ux_thread_msg_queue_message_id"),
        Index("ix_msg_queue_thread_policy", "thread_id", "policy"),
        # v2: at most one running row per thread (§6.3/§6.5). Partial unique; PG enforces,
        # SQLite (single-process, sequential claim) degrades to app-level guarantee (§6.3 table).
        Index(
            "ux_thread_running",
            "thread_id",
            unique=True,
            postgresql_where=text("status = 'running'"),
            sqlite_where=text("status = 'running'"),
        ),
        # v2: speeds the claim "no earlier executable sibling" NOT EXISTS (§6.3).
        Index(
            "ix_msg_queue_thread_seq",
            "thread_id",
            "thread_msg_seq",
            postgresql_where=text("status = 'pending'"),
            sqlite_where=text("status = 'pending'"),
        ),
    )


class ProcessedMessageRow(Base):
    """Idempotency log for task messages, doubling as the transactional outbox (v2 D7).

    Written after a task finishes (any terminal status). On duplicate
    delivery the Consumer skips re-execution and optionally replays the
    cached result payload from result_cache.

    v2 (D7, §9.3): also serves as the result-delivery outbox. Every terminal
    writer marks delivered=false first, then publishes; an inline fast path plus
    a producer loop (scanning delivered=false with exponential backoff) guarantee
    at-least-once delivery, deduped downstream by message_id.
    """

    __tablename__ = "processed_messages"

    message_id: Mapped[str] = mapped_column(Text, primary_key=True)
    thread_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    # "completed" | "failed" | "cancelled" | "paused_for_approval" | "deleted"
    # paused_for_approval: run triggered HIL interrupt; thread itself is idle
    # deleted (v2.6, §5.5/P7): a type=delete's per-thread ack, pre-staged held at ingest
    #   (next_delivery_at far-future), released to the outbox by destroy. Bare Text column —
    #   adding this value is a no-op DDL-wise (no CHECK constraint, no alembic revision).
    result_cache: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    # cached result payload for replay on duplicate delivery / outbox publish
    delivered: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    # v2 (D7): whether the downlink result has been published
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # v2 (D7): successful publish time
    delivery_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # v2 (D7): publish attempt count (backoff + poison-message alerting)
    next_delivery_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # v2 (D7): earliest next publish time (exponential backoff)
    last_delivery_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    # v2 (D7): last publish error summary
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC))

    __table_args__ = (
        # v2 (D7): producer loop scans only undelivered rows (§9.3).
        Index(
            "ix_processed_undelivered",
            "next_delivery_at",
            postgresql_where=text("delivered = false"),
            sqlite_where=text("delivered = 0"),
        ),
    )
