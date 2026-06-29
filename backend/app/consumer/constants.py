"""String-enum constants for the Consumer layer.

All status, policy, and mode strings used across run_registry, agent_runner,
task_consumer, and __main__ are defined here. Using StrEnum means comparisons
against raw string literals (e.g. row.status == "running") still work, while
giving compile-time safety against typos.
"""

from __future__ import annotations

from enum import StrEnum

# ── cancel_watermark sentinel (delete tombstone, §5.5/P7) ─────────────────────────
# cancel_watermark is normally a real, bounded thread_msg_seq (cancel-all-seq<N barrier).
# A delete reuses the *same* column with an unreachable sentinel = INT_MAX (PG 32-bit
# INTEGER max): it cancels everything (no real seq ever reaches it) AND durably marks the
# thread for destroy at the resolution point. The column thus encodes three states:
#   0           → no cancel       → resolve to idle
#   finite N    → cancel barrier  → resolve to idle (cancel seq < N)
#   INT_MAX     → delete tombstone→ resolve to destroy (delete checkpoint/dir/queue/OSS)
DELETE_SENTINEL = 2147483647


class ThreadStatus(StrEnum):
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"  # v2 (D3): HIL approval gate folded into thread_run_state.status (§4.2/4.5)


class InstanceStatus(StrEnum):
    ACTIVE = "active"
    DRAINING = "draining"
    DEAD = "dead"  # placeholder only; never written — "dead" is inferred from heartbeat timeout (§4.1/§8)


class QueuePolicy(StrEnum):
    """Scheduling role of a thread_msg_queue row (design §4.3).

    v2 set: followup | collect | resume | prefix | steer | fork | drain.
    Derived once at ingest from (config.fork, payload.command, message_mode),
    fork taking precedence over command (§5.3); claim then routes by policy alone.
    """

    FOLLOWUP = "followup"
    COLLECT = "collect"  # v2 (D5): trailing-edge debounce batch, claim-time strategy (§6.2.2)
    RESUME = "resume"  # v2 (D5): HIL resume; claimable past earlier followups under paused (§6.3)
    PREFIX = "prefix"  # cancel-covered pending kept as history context, merged into next run (§6.4)
    STEER = "steer"  # reserved for InjectMiddleware; currently ingest maps steer→followup (§5.3)
    FORK = "fork"  # v2 (fork): branch-init on a new thread; copy parent checkpoint first (§7.4)
    DRAIN = "drain"  # v2 (drain): synthesized by cancel clearing a HIL gate; reject-resume to clean terminal (§6.5)

    # ── deprecated (v1 only) — removed in Phase C once their readers are rewritten ──
    CURRENT = "current"  # v1 crash-recovery anchor; v2 uses status='running' row instead (§4.3)
    CANCEL = "cancel"  # v1 queued cancel signal; v2 cancel is a control message folded into cancel_watermark (§6.4)


class QueueRowStatus(StrEnum):
    """Lifecycle of a thread_msg_queue row (design §4.3, D5).

    Replaces v1's consumed_at-NULL + policy='current' conventions.
    """

    PENDING = "pending"  # awaiting claim
    RUNNING = "running"  # claimed and executing; holds the crash-recovery envelope
    MERGED = "merged"  # folded into a sibling running row (collect batch / prefix history)


class ProcessedStatus(StrEnum):
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED_FOR_APPROVAL = "paused_for_approval"
    DELETED = "deleted"  # v2.6 (P7): per-thread delete ack, pre-staged held at ingest, released by destroy (§5.5)


class MessageMode(StrEnum):
    FOLLOWUP = "followup"
    COLLECT = "collect"
    STEER = "steer"
    REJECT = "reject"


class ClaimResult(StrEnum):
    CLAIMED = "claimed"  # candidate claimed; thread flipped to running
    RUNNING = "running"  # thread already running on this/another instance
    PAUSED_BLOCKED = "paused_blocked"  # v2: thread paused, candidate is not a resume → left queued (§6.3)
    EMPTY = "empty"  # v2: no runnable candidate for this thread right now (§6.2)
