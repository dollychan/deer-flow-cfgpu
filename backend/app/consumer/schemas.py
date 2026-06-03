"""RocketMQ message schemas for the Consumer layer.

Deserializes the MQ protocol envelope (v2.5) into typed dataclasses.
All fields map 1-to-1 to the protocol spec in cfgpu-docs/MQ消息协议.md.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from app.consumer.constants import MessageMode, QueuePolicy

# Uplink message types accepted from upstream (task topic + signals topic).
UPLINK_TYPES: frozenset[str] = frozenset({"task", "cancel", "ping"})

# Accepted schema_version major component.  Minor bumps are backwards-compatible.
_SUPPORTED_SCHEMA_MAJOR = "2"


class SchemaValidationError(ValueError):
    """Raised when an incoming MQ message fails schema validation.

    ``reason`` is a human-readable description suitable for publishing back
    to the upstream caller via an ``INVALID_SCHEMA`` error envelope.
    """

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass
class ContentItem:
    """A single block within a user message's content array."""

    type: str  # text | image_url | document_url | audio_url | video_url
    text: str | None = None
    url: list[str] | None = None  # non-null for *_url types

    @classmethod
    def from_dict(cls, d: dict) -> ContentItem:
        return cls(type=d["type"], text=d.get("text"), url=d.get("url"))


@dataclass
class UserMessage:
    """A single user-role message in the task payload."""

    role: str  # always "user" for upstream messages
    content: list[ContentItem] | str  # list for multimodal, str for plain text

    @classmethod
    def from_dict(cls, d: dict) -> UserMessage:
        raw_content = d.get("content", "")
        content: list[ContentItem] | str
        if isinstance(raw_content, list):
            content = [ContentItem.from_dict(c) for c in raw_content]
        else:
            content = str(raw_content)
        return cls(role=d.get("role", "user"), content=content)


@dataclass
class ReplyConfig:
    """Controls what the Consumer publishes back on $AGENT_RESULTS."""

    stream_events: bool = True
    stream_event_types: list[str] = field(default_factory=lambda: ["custom"])

    @classmethod
    def from_dict(cls, d: dict | None) -> ReplyConfig:
        if not d:
            return cls()
        return cls(
            stream_events=d.get("stream_events", True),
            stream_event_types=d.get("stream_event_types", ["custom"]),
        )


@dataclass
class TaskMessage:
    """Parsed MQ message envelope (schema_version 2.5).

    Covers all three upstream message types: task, cancel, ping.
    For non-task types, payload fields (messages, command, config, reply_config)
    hold their zero/None values and should not be accessed.
    """

    # ── envelope ──────────────────────────────────────────────────────────────
    schema_version: str
    message_id: str
    message_seq: int
    timestamp: str
    type: str  # "task" | "cancel" | "ping"
    thread_id: str
    agent_name: str  # defaults to "lead_agent" when absent

    # ── task payload ──────────────────────────────────────────────────────────
    messages: list[UserMessage] | None  # non-null for normal tasks
    command: dict | None  # non-null for HIL resume; mutually exclusive with messages
    config: dict  # message_mode, ask, timeout_seconds, models, …
    reply_config: ReplyConfig

    # ── optional context ──────────────────────────────────────────────────────
    user_id: str | None = None
    project_id: str | None = None
    thread_msg_seq: int = 0  # monotonic sequence within a thread; echoed to downlink
    biz_type: str = "agent_task"  # global message type for the frontend; echoed to downlink

    # ── derived helpers ───────────────────────────────────────────────────────

    def downlink_echo(self) -> dict:
        """Return the envelope fields that must be echoed unchanged to every downlink message."""
        echo: dict = {
            "message_id": self.message_id,
            "thread_id": self.thread_id,
            "thread_msg_seq": self.thread_msg_seq,
            "bizType": self.biz_type,
        }
        if self.agent_name and self.agent_name != "lead_agent":
            echo["agent_name"] = self.agent_name
        if self.user_id:
            echo["user_id"] = self.user_id
        if self.project_id:
            echo["project_id"] = self.project_id
        return echo

    @property
    def message_mode(self) -> str:
        """Concurrent-task policy declared by upstream. Defaults to 'followup'."""
        return self.config.get("message_mode", "followup")

    @property
    def is_resume(self) -> bool:
        """True when this is a HIL resume message (command non-null)."""
        return self.command is not None

    # ── fork (branch-init, §7.4) ───────────────────────────────────────────────

    @property
    def fork(self) -> dict | None:
        """The ``config.fork`` object for branch-init, or None for a normal task.

        Presence of ``parent_thread_id`` marks this task as fork-init: it does not
        start a run on its own thread but first copies a checkpoint from the parent
        thread onto this envelope's (new) thread_id, then executes (§7.4).
        """
        f = self.config.get("fork")
        return f if isinstance(f, dict) else None

    @property
    def is_fork(self) -> bool:
        """True when this task carries ``config.fork`` (branch-init)."""
        return self.fork is not None

    @property
    def parent_thread_id(self) -> str | None:
        """Source thread to fork from (``config.fork.parent_thread_id``)."""
        f = self.fork
        return f.get("parent_thread_id") if f else None

    @property
    def fork_checkpoint_id(self) -> str | None:
        """Fork-point checkpoint in the parent thread; None = parent's latest leaf."""
        f = self.fork
        return f.get("fork_checkpoint_id") if f else None

    @property
    def derived_policy(self) -> QueuePolicy:
        """Queue policy derived once at ingest (design §4.3/§5.3).

        Precedence is a hard constraint: fork > command(resume) > message_mode.
        fork must win over command because a HIL multi-branch fork-init carries
        *both* ``config.fork`` and ``payload.command`` (the branch's approval
        decision); judging command first would mis-route it to resume and the
        orphan-resume path would kill it (§4.3). steer is currently degraded to
        followup until InjectMiddleware lands (§5.3); reject is not enqueued.
        """
        if self.is_fork:
            return QueuePolicy.FORK
        if self.is_resume:
            return QueuePolicy.RESUME
        if self.message_mode == MessageMode.COLLECT:
            return QueuePolicy.COLLECT
        return QueuePolicy.FOLLOWUP

    @property
    def timeout_seconds(self) -> int | None:
        v = self.config.get("timeout_seconds")
        return int(v) if v is not None else None

    # ── validation ────────────────────────────────────────────────────────────

    @classmethod
    def _validate_raw(cls, data: dict) -> None:
        """Validate the raw envelope dict before construction.

        Raises:
            SchemaValidationError: with a human-readable reason on any failure.
        """
        # schema_version — optional but must be a supported major version when present
        sv = data.get("schema_version")
        if sv is not None:
            major = str(sv).split(".")[0]
            if major != _SUPPORTED_SCHEMA_MAJOR:
                raise SchemaValidationError(
                    f"Unsupported schema_version={sv!r}; expected '2.x'"
                )

        # message_id — required, non-empty
        if not data.get("message_id"):
            raise SchemaValidationError("Missing required field: message_id")

        # type — required, must be a known uplink type
        msg_type = data.get("type")
        if not msg_type:
            raise SchemaValidationError("Missing required field: type")
        if msg_type not in UPLINK_TYPES:
            raise SchemaValidationError(
                f"Unknown message type={msg_type!r}; expected one of {sorted(UPLINK_TYPES)}"
            )

        # thread_id — required, non-empty
        if not data.get("thread_id"):
            raise SchemaValidationError("Missing required field: thread_id")

        # payload — required, must be an object
        payload = data.get("payload")
        if payload is None:
            raise SchemaValidationError("Missing required field: payload")
        if not isinstance(payload, dict):
            raise SchemaValidationError(
                f"payload must be an object, got {type(payload).__name__}"
            )

        # task-specific payload rules
        if msg_type == "task":
            cls._validate_task_payload(payload)

    @classmethod
    def _validate_task_payload(cls, payload: dict) -> None:
        messages = payload.get("messages")
        command = payload.get("command")

        if messages is None and command is None:
            raise SchemaValidationError(
                "task message requires either payload.messages (normal task) "
                "or payload.command (HIL resume); both are null"
            )
        if messages is not None and command is not None:
            raise SchemaValidationError(
                "payload.messages and payload.command are mutually exclusive; "
                "provide exactly one"
            )

        if messages is not None:
            if not isinstance(messages, list) or len(messages) == 0:
                raise SchemaValidationError(
                    "payload.messages must be a non-empty array"
                )
            for i, m in enumerate(messages):
                if not isinstance(m, dict):
                    raise SchemaValidationError(
                        f"payload.messages[{i}] must be an object"
                    )
                if not m.get("role"):
                    raise SchemaValidationError(
                        f"payload.messages[{i}].role is required"
                    )
                if "content" not in m:
                    raise SchemaValidationError(
                        f"payload.messages[{i}].content is required"
                    )

        if command is not None:
            if not isinstance(command, dict):
                raise SchemaValidationError("payload.command must be an object")
            update = command.get("update")
            if not isinstance(update, dict):
                raise SchemaValidationError(
                    "payload.command.update must be an object"
                )
            if not isinstance(update.get("tool_approvals"), dict):
                raise SchemaValidationError(
                    "payload.command.update.tool_approvals must be an object"
                )

        # config.fork — optional; when present must be an object with parent_thread_id.
        # fork is orthogonal to the messages/command exclusion above: a HIL multi-branch
        # fork-init legitimately carries fork *and* command (§7.4).
        config = payload.get("config")
        if config is not None and not isinstance(config, dict):
            raise SchemaValidationError(
                f"payload.config must be an object, got {type(config).__name__}"
            )
        fork = config.get("fork") if isinstance(config, dict) else None
        if fork is not None:
            if not isinstance(fork, dict):
                raise SchemaValidationError("payload.config.fork must be an object")
            if not fork.get("parent_thread_id"):
                raise SchemaValidationError(
                    "payload.config.fork.parent_thread_id is required for fork-init"
                )

    # ── factory ───────────────────────────────────────────────────────────────

    @classmethod
    def from_dict(cls, data: dict) -> TaskMessage:
        """Validate and build a TaskMessage from an already-parsed MQ envelope dict.

        Raises:
            SchemaValidationError: If the envelope fails schema validation.
        """
        cls._validate_raw(data)
        payload = data.get("payload") or {}
        messages_raw = payload.get("messages")
        messages = [UserMessage.from_dict(m) for m in messages_raw] if messages_raw else None
        config = dict(payload.get("config") or {})
        # ping: instance_id is a top-level payload field, not nested under config
        if data.get("type") == "ping" and "instance_id" in payload:
            config["instance_id"] = payload["instance_id"]
        return cls(
            schema_version=data.get("schema_version", "2.5"),
            message_id=data["message_id"],
            message_seq=data.get("message_seq", 0),
            timestamp=data.get("timestamp", ""),
            type=data["type"],
            thread_id=data["thread_id"],
            agent_name=data.get("agent_name") or "lead_agent",
            messages=messages,
            command=payload.get("command"),
            config=config,
            reply_config=ReplyConfig.from_dict(payload.get("reply_config")),
            user_id=data.get("user_id"),
            project_id=data.get("project_id"),
            thread_msg_seq=data.get("thread_msg_seq", 0),
            biz_type=data.get("bizType") or "agent_task",
        )

    @classmethod
    def from_json(cls, body: str | bytes) -> TaskMessage:
        """Deserialize a raw MQ message body into a TaskMessage.

        Raises:
            SchemaValidationError: If the envelope fails schema validation.
            json.JSONDecodeError: If body is not valid JSON.
        """
        return cls.from_dict(json.loads(body))
