"""Tests for MQStreamBridge envelope construction (stream_bridge/mq.py).

A fake producer captures the raw JSON bodies so we can assert envelope-level
fields (schema_version, bizType) and terminal payload fields (checkpoint_id)
across the inline publish path and the outbox replay path.
"""

from __future__ import annotations

import json
import re

import pytest

from app.consumer.schemas import ReplyConfig
from app.consumer.stream_bridge.mq import MQStreamBridge

# Beijing wall-clock: "2026-06-04 14:58:19.097" — space sep, 3-digit millis, no tz suffix.
_BEIJING_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$")


class _FakeProducer:
    def __init__(self) -> None:
        self.bodies: list[dict] = []

    async def send_async(self, body: bytes, *, keys: str = "") -> None:
        self.bodies.append(json.loads(body))


def _bridge() -> tuple[MQStreamBridge, _FakeProducer]:
    producer = _FakeProducer()
    return MQStreamBridge(producer), producer


_ECHO = {"message_id": "m1", "thread_id": "t1", "thread_msg_seq": 3, "bizType": "agent_task"}


# ── envelope-level fields ──────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_envelope_schema_version_and_biztype():
    bridge, producer = _bridge()
    bridge.register_run("m1", ReplyConfig(stream_events=True), echo=_ECHO)
    await bridge.publish_result("m1", status="success", stream_events=True, checkpoint_id="ck1")

    env = producer.bodies[-1]
    assert env["schema_version"] == "2.5"
    assert env["bizType"] == "agent_task"
    # Downlink timestamp must be Beijing wall-clock for the frontend (no 'Z'/UTC).
    assert _BEIJING_TS_RE.match(env["timestamp"])
    assert "Z" not in env["timestamp"]


@pytest.mark.anyio
async def test_envelope_biztype_defaults_when_absent():
    bridge, producer = _bridge()
    await bridge.publish_result(
        "m1", status="success", stream_events=True, checkpoint_id="ck1",
        echo={"message_id": "m1", "thread_id": "t1"},
    )
    assert producer.bodies[-1]["bizType"] == "agent_task"


# ── checkpoint_id on result ────────────────────────────────────────────────────


@pytest.mark.anyio
@pytest.mark.parametrize("status", ["success", "cancelled", "paused_for_approval"])
async def test_result_carries_checkpoint_id_all_statuses(status):
    bridge, producer = _bridge()
    bridge.register_run("m1", ReplyConfig(stream_events=True), echo=_ECHO)
    await bridge.publish_result("m1", status=status, stream_events=True, checkpoint_id="ck-xyz")

    payload = producer.bodies[-1]["payload"]
    assert payload["status"] == status
    assert payload["checkpoint_id"] == "ck-xyz"


@pytest.mark.anyio
async def test_result_checkpoint_id_present_even_when_none():
    bridge, producer = _bridge()
    bridge.register_run("m1", ReplyConfig(stream_events=True), echo=_ECHO)
    await bridge.publish_result("m1", status="success", stream_events=True)

    # protocol: checkpoint_id is carried for all result statuses (null when unavailable)
    assert "checkpoint_id" in producer.bodies[-1]["payload"]
    assert producer.bodies[-1]["payload"]["checkpoint_id"] is None


# ── error never carries checkpoint_id (fork anchor is result-only) ─────────────


@pytest.mark.anyio
async def test_error_never_carries_checkpoint_id():
    # Even a mid-run failure that produced a checkpoint must not expose it on the error
    # envelope: fork anchors come only from result (success/cancelled/paused). P0.2.
    bridge, producer = _bridge()
    await bridge.publish_error(
        "AGENT_TIMEOUT", echo=_ECHO, retriable=True, message="boom",
    )
    payload = producer.bodies[-1]["payload"]
    assert payload["error"]["code"] == "AGENT_TIMEOUT"
    assert "checkpoint_id" not in payload


@pytest.mark.anyio
async def test_error_omits_checkpoint_id_when_absent():
    bridge, producer = _bridge()
    await bridge.publish_error("INVALID_SCHEMA", echo=_ECHO, message="bad")
    assert "checkpoint_id" not in producer.bodies[-1]["payload"]


# ── checkpoint_id on outbox replay ─────────────────────────────────────────────


@pytest.mark.anyio
async def test_replay_result_carries_checkpoint_id():
    bridge, producer = _bridge()
    await bridge.replay(
        {"status": "success", "stream_events": True, "checkpoint_id": "ck-replay"},
        echo=_ECHO,
    )
    terminal = producer.bodies[-1]
    assert terminal["type"] == "result"
    assert terminal["payload"]["checkpoint_id"] == "ck-replay"


@pytest.mark.anyio
async def test_replay_error_never_carries_checkpoint_id():
    # A stray checkpoint_id in the error result_cache must not leak onto the replayed
    # error envelope — error is never a fork anchor (P0.2).
    bridge, producer = _bridge()
    await bridge.replay(
        {"error": {"code": "INTERNAL_ERROR", "retriable": False}, "checkpoint_id": "ck-replay"},
        echo=_ECHO,
    )
    terminal = producer.bodies[-1]
    assert terminal["type"] == "error"
    assert "checkpoint_id" not in terminal["payload"]
