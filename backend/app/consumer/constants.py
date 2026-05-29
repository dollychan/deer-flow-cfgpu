"""String-enum constants for the Consumer layer.

All status, policy, and mode strings used across run_registry, agent_runner,
task_consumer, and __main__ are defined here. Using StrEnum means comparisons
against raw string literals (e.g. row.status == "running") still work, while
giving compile-time safety against typos.
"""

from __future__ import annotations

from enum import StrEnum


class ThreadStatus(StrEnum):
    IDLE = "idle"
    RUNNING = "running"


class InstanceStatus(StrEnum):
    ACTIVE = "active"
    DRAINING = "draining"
    DEAD = "dead"


class QueuePolicy(StrEnum):
    CURRENT = "current"
    FOLLOWUP = "followup"
    CANCEL = "cancel"
    PREFIX = "prefix"
    STEER = "steer"


class ProcessedStatus(StrEnum):
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED_FOR_APPROVAL = "paused_for_approval"


class MessageMode(StrEnum):
    FOLLOWUP = "followup"
    COLLECT = "collect"
    STEER = "steer"
    REJECT = "reject"


class ClaimResult(StrEnum):
    CLAIMED = "claimed"
    RUNNING = "running"
