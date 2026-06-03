"""Phase A — enum consistency for the Consumer v2 layer.

Asserts constants.py matches design §4.2/§4.3/§4.5 enums. No DB or MQ required.
"""

from __future__ import annotations

from app.consumer.constants import (
    ClaimResult,
    InstanceStatus,
    MessageMode,
    ProcessedStatus,
    QueuePolicy,
    QueueRowStatus,
    ThreadStatus,
)


class TestThreadStatus:
    def test_three_state(self):
        # v2 (D3): paused added — HIL gate folded into thread state (§4.5).
        assert {s.value for s in ThreadStatus} == {"idle", "running", "paused"}

    def test_str_equality(self):
        # StrEnum compares equal to its raw literal (rows store plain strings).
        assert ThreadStatus.PAUSED == "paused"


class TestQueuePolicy:
    def test_v2_set_present(self):
        # Design §4.3 executable/scheduling policies.
        v2 = {"followup", "collect", "resume", "prefix", "steer", "fork", "drain"}
        assert v2.issubset({p.value for p in QueuePolicy})

    def test_deprecated_still_present_for_v1_coexistence(self):
        # current/cancel kept until Phase C rewrites their readers (additive Phase A).
        assert QueuePolicy.CURRENT == "current"
        assert QueuePolicy.CANCEL == "cancel"

    def test_drain_value(self):
        assert QueuePolicy.DRAIN == "drain"


class TestQueueRowStatus:
    def test_lifecycle(self):
        # D5: replaces consumed_at-NULL + policy='current'.
        assert {s.value for s in QueueRowStatus} == {"pending", "running", "merged"}


class TestInstanceStatus:
    def test_unchanged(self):
        assert {s.value for s in InstanceStatus} == {"active", "draining", "dead"}


class TestProcessedStatus:
    def test_terminal_set(self):
        assert {s.value for s in ProcessedStatus} == {
            "completed",
            "failed",
            "cancelled",
            "paused_for_approval",
        }


class TestMessageMode:
    def test_unchanged(self):
        assert {m.value for m in MessageMode} == {
            "followup",
            "collect",
            "steer",
            "reject",
        }


class TestClaimResult:
    def test_v2_outcomes(self):
        assert {"claimed", "running", "paused_blocked", "empty"}.issubset(
            {c.value for c in ClaimResult}
        )
