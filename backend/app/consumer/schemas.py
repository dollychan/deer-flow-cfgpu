"""RocketMQ message schemas for the Consumer layer.

Deserializes the MQ protocol envelope (v2.3) into typed dataclasses.
All fields map 1-to-1 to the protocol spec in cfgpu-docs/MQ消息协议.md.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field


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
    stream_event_types: list[str] = field(default_factory=lambda: ["messages", "custom", "values"])

    @classmethod
    def from_dict(cls, d: dict | None) -> ReplyConfig:
        if not d:
            return cls()
        return cls(
            stream_events=d.get("stream_events", True),
            stream_event_types=d.get("stream_event_types", ["messages", "custom", "values"]),
        )


@dataclass
class TaskMessage:
    """Parsed MQ message envelope (schema_version 2.3).

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

    # ── derived helpers ───────────────────────────────────────────────────────

    @property
    def message_mode(self) -> str:
        """Concurrent-task policy declared by upstream. Defaults to 'followup'."""
        return self.config.get("message_mode", "followup")

    @property
    def is_resume(self) -> bool:
        """True when this is a HIL resume message (command non-null)."""
        return self.command is not None

    @property
    def timeout_seconds(self) -> int | None:
        v = self.config.get("timeout_seconds")
        return int(v) if v is not None else None

    # ── factory ───────────────────────────────────────────────────────────────

    @classmethod
    def from_dict(cls, data: dict) -> TaskMessage:
        """Build a TaskMessage from an already-parsed MQ envelope dict.

        Raises:
            KeyError: If required envelope fields (message_id, type, thread_id) are absent.
        """
        payload = data.get("payload") or {}
        messages_raw = payload.get("messages")
        messages = [UserMessage.from_dict(m) for m in messages_raw] if messages_raw else None
        return cls(
            schema_version=data.get("schema_version", "2.3"),
            message_id=data["message_id"],
            message_seq=data.get("message_seq", 0),
            timestamp=data.get("timestamp", ""),
            type=data["type"],
            thread_id=data["thread_id"],
            agent_name=data.get("agent_name") or "lead_agent",
            messages=messages,
            command=payload.get("command"),
            config=payload.get("config") or {},
            reply_config=ReplyConfig.from_dict(payload.get("reply_config")),
            user_id=data.get("user_id"),
            project_id=data.get("project_id"),
        )

    @classmethod
    def from_json(cls, body: str | bytes) -> TaskMessage:
        """Deserialize a raw MQ message body into a TaskMessage.

        Raises:
            KeyError: If required envelope fields (message_id, type, thread_id) are absent.
            json.JSONDecodeError: If body is not valid JSON.
        """
        return cls.from_dict(json.loads(body))
