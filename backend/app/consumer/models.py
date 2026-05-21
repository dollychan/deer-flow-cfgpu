"""SQLAlchemy ORM models for the Consumer layer.

Five tables that extend deerflow's shared persistence Base.
Import this module before calling init_engine_from_config() so that
Base.metadata.create_all() discovers and creates these tables alongside
the core deerflow tables (runs, threads_meta, etc.).

Table overview:
  consumer_instances    — running Consumer process registry + heartbeat
  thread_run_state      — per-thread execution state used for routing
  thread_msg_queue      — followup (and future steer) message queue
  thread_cancel_signals — cancel signals written by any instance, polled by runner
  processed_messages    — idempotency log, also caches results for replay
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import DateTime, Index, Integer, JSON, Text
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

    status lifecycle:
      idle    → claimed by Consumer → running
      running → run completes      → idle

    drain_mode controls how the followup queue is drained after a run ends:
      followup — take the earliest row and start a new independent run (default)
      collect  — take all pending rows, merge their messages, start a single run
    drain_mode is set to 'collect' when a collect-mode message is enqueued while
    the thread is running, and reset to 'followup' when the thread goes idle.
    """

    __tablename__ = "thread_run_state"

    thread_id: Mapped[str] = mapped_column(Text, primary_key=True)
    instance_id: Mapped[str] = mapped_column(Text, nullable=False)
    # FK to consumer_instances.instance_id (no FK constraint; dead instances may vanish)
    message_id: Mapped[str] = mapped_column(Text, nullable=False)
    # message_id of the task currently being executed
    status: Mapped[str] = mapped_column(Text, nullable=False)
    # "running" | "idle"
    drain_mode: Mapped[str] = mapped_column(Text, nullable=False, default="followup")
    # "followup" | "collect"
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # stale-run auto-retry count; reset to 0 after each run completes
    # reply_config removed: full MQ envelope (incl. reply_config) is in thread_msg_queue(policy='current')
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_heartbeat: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC))


class ThreadMsgQueueRow(Base):
    """Queue for messages associated with a thread (crash recovery + followup).

    policy determines which component consumes the row:
      current  — written on claim; stores the complete MQ envelope for crash
                 recovery by stale-run-watchdog; at most one row per thread;
                 deleted when thread returns to idle; consumed_at unused.
      followup — _drain_and_release picks it up after the current run ends,
                 starts a new full round using its body as input.
      steer    — InjectMiddleware consumes it mid-run at the next node
                 boundary (not yet implemented; rows written with policy='steer'
                 are currently treated as followup by _drain_and_release).

    body stores the complete MQ envelope (schema_version, message_id, agent_name,
    user_id, project_id, payload.{messages,command,config,reply_config}, …) so
    TaskMessage.from_json(json.dumps(row.body)) reconstructs the message losslessly.

    consumed_at=NULL means the row is still pending (not applicable to 'current' rows).
    The partial index on (thread_id, policy) WHERE consumed_at IS NULL keeps
    queue lookups fast even with large historical data.
    """

    __tablename__ = "thread_msg_queue"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    thread_id: Mapped[str] = mapped_column(Text, nullable=False)
    message_id: Mapped[str] = mapped_column(Text, nullable=False)
    body: Mapped[dict] = mapped_column(JSON, nullable=False)
    # complete MQ envelope; replaces the old partial 'payload' column
    policy: Mapped[str] = mapped_column(Text, nullable=False, default="followup")
    # "current" | "followup" | "steer" (steer: future)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC))
    consumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (Index("ix_msg_queue_thread_policy_pending", "thread_id", "policy", "consumed_at"),)


class ThreadCancelSignalRow(Base):
    """Cancel signal written by any Consumer instance, polled by the runner.

    Any instance can write here when it receives a cancel message; the
    cancel watcher coroutine inside AgentRunner polls this table every 2 s
    and calls runner_task.cancel() when it finds a row for its thread.
    """

    __tablename__ = "thread_cancel_signals"

    thread_id: Mapped[str] = mapped_column(Text, primary_key=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    # mirrors cancel payload reason: "user_requested" | "timeout" | "admin"
    requested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC))


class ProcessedMessageRow(Base):
    """Idempotency log for task messages.

    Written after a task finishes (any terminal status). On duplicate
    delivery the Consumer skips re-execution and optionally replays the
    cached result payload from result_cache.
    """

    __tablename__ = "processed_messages"

    message_id: Mapped[str] = mapped_column(Text, primary_key=True)
    thread_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    # "completed" | "failed" | "cancelled" | "paused_for_approval"
    # paused_for_approval: run triggered HIL interrupt; thread itself is idle
    result_cache: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    # cached result payload for replay on duplicate delivery
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC))
