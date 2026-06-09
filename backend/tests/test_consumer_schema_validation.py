"""Tests for MQ message schema validation (schemas.py + task_consumer.handle_message).

No DB or real MQ broker required.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.consumer.constants import QueuePolicy
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


def _fork_envelope(*, command: bool = False, **fork_fields) -> dict:
    """A fork-init task: config.fork set; messages (default) or command payload."""
    fork = {"parent_thread_id": "t_parent", "fork_checkpoint_id": "ckpt-1"}
    fork.update(fork_fields)
    if command:
        payload = {
            "messages": None,
            "command": {"update": {"tool_approvals": {"call_x": {"status": "approved"}}}},
            "config": {"ask": True, "fork": fork},
        }
    else:
        payload = {
            "messages": [{"role": "user", "content": "继续"}],
            "command": None,
            "config": {"fork": fork},
        }
    return {
        "schema_version": "2.5",
        "message_id": "msg-fork-1",
        "type": "task",
        "thread_id": "t_branch",
        "payload": payload,
    }


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

    def test_valid_ping_without_thread_id(self):
        # Protocol exempts ping from thread_id (MQ消息协议.md field-table note).
        env = _ping_envelope()
        env.pop("thread_id")
        msg = TaskMessage.from_dict(env)
        assert msg.thread_id == ""
        # downlink echo (used by pong) must tolerate the empty thread_id.
        assert msg.downlink_echo()["thread_id"] == ""

    def test_valid_ping_empty_thread_id(self):
        msg = TaskMessage.from_dict(_ping_envelope(thread_id=""))
        assert msg.thread_id == ""

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
# bizType — envelope-level global message type (v2.5)
# ---------------------------------------------------------------------------


class TestBizType:
    def test_parsed_from_envelope(self):
        msg = TaskMessage.from_dict(_task_envelope(bizType="custom_biz"))
        assert msg.biz_type == "custom_biz"

    def test_defaults_to_agent_task_when_absent(self):
        env = _task_envelope()
        env.pop("bizType", None)
        assert TaskMessage.from_dict(env).biz_type == "agent_task"

    def test_empty_string_defaults_to_agent_task(self):
        assert TaskMessage.from_dict(_task_envelope(bizType="")).biz_type == "agent_task"

    def test_echoed_to_downlink(self):
        echo = TaskMessage.from_dict(_task_envelope(bizType="custom_biz")).downlink_echo()
        assert echo["bizType"] == "custom_biz"

    def test_downlink_echo_default(self):
        env = _task_envelope()
        env.pop("bizType", None)
        echo = TaskMessage.from_dict(env).downlink_echo()
        assert echo["bizType"] == "agent_task"


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

    def test_cancel_missing_thread_id_still_rejected(self):
        env = _cancel_envelope()
        env.pop("thread_id")
        self._check(env, "thread_id")

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
# config.fork parsing (§7.4)
# ---------------------------------------------------------------------------


class TestForkParsing:
    def test_normal_task_is_not_fork(self):
        msg = TaskMessage.from_dict(_task_envelope())
        assert msg.is_fork is False
        assert msg.fork is None
        assert msg.parent_thread_id is None
        assert msg.fork_checkpoint_id is None

    def test_fork_fields_exposed(self):
        msg = TaskMessage.from_dict(_fork_envelope())
        assert msg.is_fork is True
        assert msg.parent_thread_id == "t_parent"
        assert msg.fork_checkpoint_id == "ckpt-1"

    def test_fork_checkpoint_optional(self):
        env = _fork_envelope()
        del env["payload"]["config"]["fork"]["fork_checkpoint_id"]
        msg = TaskMessage.from_dict(env)
        assert msg.parent_thread_id == "t_parent"
        assert msg.fork_checkpoint_id is None  # None = parent latest leaf


# ---------------------------------------------------------------------------
# derived_policy precedence: fork > command(resume) > collect > followup (§4.3/§5.3)
# ---------------------------------------------------------------------------


class TestDerivedPolicy:
    def test_plain_task_is_followup(self):
        assert TaskMessage.from_dict(_task_envelope()).derived_policy == QueuePolicy.FOLLOWUP

    def test_collect_mode(self):
        env = _task_envelope()
        env["payload"]["config"] = {"message_mode": "collect"}
        assert TaskMessage.from_dict(env).derived_policy == QueuePolicy.COLLECT

    def test_steer_degrades_to_followup(self):
        # §5.3: steer maps to followup until InjectMiddleware lands.
        env = _task_envelope()
        env["payload"]["config"] = {"message_mode": "steer"}
        assert TaskMessage.from_dict(env).derived_policy == QueuePolicy.FOLLOWUP

    def test_resume_command(self):
        assert TaskMessage.from_dict(_resume_envelope()).derived_policy == QueuePolicy.RESUME

    def test_fork_beats_command(self):
        # Hard constraint: HIL multi-branch fork-init carries both fork and command;
        # fork must win or orphan-resume would kill it (§4.3).
        msg = TaskMessage.from_dict(_fork_envelope(command=True))
        assert msg.is_resume is True  # command present
        assert msg.derived_policy == QueuePolicy.FORK

    def test_fork_with_messages(self):
        assert TaskMessage.from_dict(_fork_envelope()).derived_policy == QueuePolicy.FORK


# ---------------------------------------------------------------------------
# fork validation
# ---------------------------------------------------------------------------


class TestForkValidation:
    def test_fork_missing_parent_thread_id(self):
        env = _fork_envelope()
        del env["payload"]["config"]["fork"]["parent_thread_id"]
        with pytest.raises(SchemaValidationError) as exc:
            TaskMessage.from_dict(env)
        assert "parent_thread_id" in exc.value.reason

    def test_fork_not_object(self):
        env = _task_envelope()
        env["payload"]["config"] = {"fork": "bad"}
        with pytest.raises(SchemaValidationError) as exc:
            TaskMessage.from_dict(env)
        assert "fork" in exc.value.reason

    def test_config_not_object(self):
        env = _task_envelope()
        env["payload"]["config"] = "bad"
        with pytest.raises(SchemaValidationError) as exc:
            TaskMessage.from_dict(env)
        assert "config" in exc.value.reason


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
