"""Tests for MQ message schema validation (schemas.py + task_consumer.handle_message).

No DB or real MQ broker required.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.consumer.schemas import SchemaValidationError, TaskMessage


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _task_envelope(**overrides) -> dict:
    """Return a minimal valid task envelope, with optional field overrides."""
    base = {
        "schema_version": "2.3",
        "message_id": "msg-001",
        "message_seq": 0,
        "timestamp": "2026-05-21T00:00:00.000Z",
        "type": "task",
        "thread_id": "thread-001",
        "payload": {
            "messages": [{"role": "user", "content": "hi"}],
            "command": None,
            "config": {},
            "reply_config": {"stream_events": True, "stream_event_types": ["messages"]},
        },
    }
    base.update(overrides)
    return base


def _cancel_envelope(**overrides) -> dict:
    base = {
        "schema_version": "2.3",
        "message_id": "msg-002",
        "type": "cancel",
        "thread_id": "thread-001",
        "payload": {"reason": "user_requested"},
    }
    base.update(overrides)
    return base


def _ping_envelope(**overrides) -> dict:
    base = {
        "schema_version": "2.3",
        "message_id": "msg-003",
        "type": "ping",
        "thread_id": "thread-001",
        "payload": {},
    }
    base.update(overrides)
    return base


def _resume_envelope(**overrides) -> dict:
    base = {
        "schema_version": "2.3",
        "message_id": "msg-004",
        "type": "task",
        "thread_id": "thread-001",
        "payload": {
            "messages": None,
            "command": {
                "update": {
                    "tool_approvals": {
                        "call_abc": {"status": "approved"},
                    }
                }
            },
            "config": {"ask": True},
        },
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# TaskMessage._validate_raw — valid envelopes
# ---------------------------------------------------------------------------


class TestValidEnvelopes:
    def test_valid_task(self):
        TaskMessage.from_dict(_task_envelope())

    def test_valid_cancel(self):
        TaskMessage.from_dict(_cancel_envelope())

    def test_valid_ping(self):
        TaskMessage.from_dict(_ping_envelope())

    def test_valid_hil_resume(self):
        TaskMessage.from_dict(_resume_envelope())

    def test_schema_version_absent_is_ok(self):
        env = _task_envelope()
        del env["schema_version"]
        TaskMessage.from_dict(env)

    def test_schema_version_2x_minor_variations(self):
        for sv in ("2.0", "2.3", "2.99"):
            TaskMessage.from_dict(_task_envelope(schema_version=sv))

    def test_multimodal_content(self):
        env = _task_envelope()
        env["payload"]["messages"] = [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "看图"},
                    {"type": "image_url", "url": ["https://example.com/img.png"]},
                ],
            }
        ]
        msg = TaskMessage.from_dict(env)
        assert isinstance(msg.messages[0].content, list)


# ---------------------------------------------------------------------------
# TaskMessage._validate_raw — envelope-level failures
# ---------------------------------------------------------------------------


class TestEnvelopeValidationErrors:
    def _check(self, env: dict, fragment: str) -> None:
        with pytest.raises(SchemaValidationError) as exc_info:
            TaskMessage.from_dict(env)
        assert fragment in exc_info.value.reason

    def test_unsupported_schema_version_major(self):
        self._check(_task_envelope(schema_version="3.0"), "schema_version")

    def test_missing_message_id(self):
        env = _task_envelope()
        env.pop("message_id")
        self._check(env, "message_id")

    def test_empty_message_id(self):
        self._check(_task_envelope(message_id=""), "message_id")

    def test_missing_type(self):
        env = _task_envelope()
        env.pop("type")
        self._check(env, "type")

    def test_unknown_type(self):
        self._check(_task_envelope(type="inject"), "inject")

    def test_missing_thread_id(self):
        env = _task_envelope()
        env.pop("thread_id")
        self._check(env, "thread_id")

    def test_empty_thread_id(self):
        self._check(_task_envelope(thread_id=""), "thread_id")

    def test_missing_payload(self):
        env = _task_envelope()
        env.pop("payload")
        self._check(env, "payload")

    def test_payload_not_object(self):
        env = _task_envelope()
        env["payload"] = "bad"
        self._check(env, "payload")


# ---------------------------------------------------------------------------
# TaskMessage._validate_task_payload — task-specific failures
# ---------------------------------------------------------------------------


class TestTaskPayloadValidationErrors:
    def _check(self, payload: dict, fragment: str) -> None:
        env = _task_envelope()
        env["payload"] = payload
        with pytest.raises(SchemaValidationError) as exc_info:
            TaskMessage.from_dict(env)
        assert fragment in exc_info.value.reason

    def test_both_null(self):
        self._check({"messages": None, "command": None, "config": {}}, "null")

    def test_both_present(self):
        self._check(
            {
                "messages": [{"role": "user", "content": "hi"}],
                "command": {"update": {"tool_approvals": {}}},
                "config": {},
            },
            "mutually exclusive",
        )

    def test_messages_empty_list(self):
        self._check({"messages": [], "command": None, "config": {}}, "non-empty")

    def test_messages_not_list(self):
        self._check({"messages": "bad", "command": None, "config": {}}, "non-empty")

    def test_message_item_not_dict(self):
        self._check({"messages": ["oops"], "command": None, "config": {}}, "object")

    def test_message_missing_role(self):
        self._check(
            {"messages": [{"content": "hi"}], "command": None, "config": {}},
            "role",
        )

    def test_message_missing_content(self):
        self._check(
            {"messages": [{"role": "user"}], "command": None, "config": {}},
            "content",
        )

    def test_command_not_dict(self):
        self._check({"messages": None, "command": "bad", "config": {}}, "object")

    def test_command_missing_update(self):
        self._check({"messages": None, "command": {}, "config": {}}, "update")

    def test_command_update_not_dict(self):
        self._check(
            {"messages": None, "command": {"update": "bad"}, "config": {}},
            "update",
        )

    def test_command_tool_approvals_missing(self):
        self._check(
            {"messages": None, "command": {"update": {}}, "config": {}},
            "tool_approvals",
        )

    def test_command_tool_approvals_not_dict(self):
        self._check(
            {
                "messages": None,
                "command": {"update": {"tool_approvals": "bad"}},
                "config": {},
            },
            "tool_approvals",
        )


# ---------------------------------------------------------------------------
# TaskConsumer.handle_message — integration with validation
# ---------------------------------------------------------------------------


def _make_consumer():
    """Build a TaskConsumer with all dependencies mocked."""
    from app.consumer.task_consumer import TaskConsumer

    registry = MagicMock()
    runner = MagicMock()
    bridge = MagicMock()
    bridge.publish_error = AsyncMock()
    bridge.publish_pong = AsyncMock()
    consumer = TaskConsumer(
        registry=registry,
        runner=runner,
        bridge=bridge,
        instance_id="test-instance",
    )
    return consumer, bridge


@pytest.mark.anyio
async def test_invalid_json_drops_silently():
    consumer, bridge = _make_consumer()
    # No message_id recoverable → nothing published
    await consumer.handle_message(b"not json{{{")
    bridge.publish_error.assert_not_called()


@pytest.mark.anyio
async def test_invalid_json_with_recoverable_message_id_publishes_error():
    consumer, bridge = _make_consumer()
    # Trailing comma makes it invalid JSON, but message_id is ASCII-recoverable
    malformed = b'{"message_id": "msg-rescue-001", "thread_id": "t1", "type": "task",}'
    await consumer.handle_message(malformed)

    bridge.publish_error.assert_called_once()
    call = bridge.publish_error.call_args
    assert call.args[0] == "INVALID_SCHEMA"
    assert call.kwargs["echo"]["message_id"] == "msg-rescue-001"
    assert call.kwargs["retriable"] is False
    assert "JSON" in call.kwargs["message"]


@pytest.mark.anyio
async def test_invalid_json_with_recoverable_message_id_no_thread_id():
    consumer, bridge = _make_consumer()
    malformed = b'{"message_id": "msg-rescue-002", "type": "task",}'
    await consumer.handle_message(malformed)

    bridge.publish_error.assert_called_once()
    call = bridge.publish_error.call_args
    assert call.kwargs["echo"]["message_id"] == "msg-rescue-002"
    assert call.kwargs["echo"]["thread_id"] == ""


@pytest.mark.anyio
async def test_schema_error_with_message_id_publishes_error():
    consumer, bridge = _make_consumer()
    # missing thread_id, but message_id is present
    bad = {"message_id": "msg-bad", "type": "task", "payload": {}}
    await consumer.handle_message(json.dumps(bad).encode())

    bridge.publish_error.assert_called_once()
    call = bridge.publish_error.call_args
    assert call.args[0] == "INVALID_SCHEMA"
    assert call.kwargs["echo"]["message_id"] == "msg-bad"
    assert call.kwargs["retriable"] is False
    assert "thread_id" in call.kwargs["message"]


@pytest.mark.anyio
async def test_schema_error_without_message_id_no_publish():
    consumer, bridge = _make_consumer()
    # message_id absent → cannot publish error
    bad = {"type": "task", "thread_id": "t1", "payload": {}}
    await consumer.handle_message(json.dumps(bad).encode())
    bridge.publish_error.assert_not_called()


@pytest.mark.anyio
async def test_valid_ping_does_not_trigger_schema_error():
    consumer, bridge = _make_consumer()
    bridge.publish_pong = AsyncMock()

    with patch.object(consumer, "_handle_ping", new=AsyncMock()) as mock_ping:
        await consumer.handle_message(json.dumps(_ping_envelope()).encode())

    bridge.publish_error.assert_not_called()
    mock_ping.assert_called_once()


@pytest.mark.anyio
async def test_unknown_type_publishes_invalid_schema():
    consumer, bridge = _make_consumer()
    bad = {
        "message_id": "msg-unk",
        "type": "inject",  # deprecated / unknown
        "thread_id": "t1",
        "payload": {},
    }
    await consumer.handle_message(json.dumps(bad).encode())

    bridge.publish_error.assert_called_once()
    assert bridge.publish_error.call_args.args[0] == "INVALID_SCHEMA"
    assert "inject" in bridge.publish_error.call_args.kwargs["message"]
