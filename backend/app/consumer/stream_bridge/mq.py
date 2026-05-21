"""MQStreamBridge — publishes LangGraph events to RocketMQ $AGENT_RESULTS topic.

Write-only StreamBridge implementation for the Consumer layer.
Subscribing is unsupported; upstream reads results directly from MQ.

Usage pattern:
    bridge = MQStreamBridge(producer, result_topic="$AGENT_RESULTS")

    # AgentRunner: before streaming starts
    bridge.register_run(run_id, thread_id, reply_config)
    try:
        async for mode, chunk in graph.astream(...):
            await bridge.publish(run_id, mode, chunk)
        await bridge.publish_result(run_id, status="success", ...)
    finally:
        bridge.unregister_run(run_id)
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol

from deerflow.runtime.stream_bridge.base import StreamBridge, StreamEvent

from app.consumer.schemas import ReplyConfig

logger = logging.getLogger(__name__)


class MQProducer(Protocol):
    """Minimal interface required from a RocketMQ producer."""

    async def send_async(self, body: bytes, *, keys: str = "") -> None: ...


@dataclass
class _RunContext:
    thread_id: str
    reply_config: ReplyConfig
    seq: int = field(default=0)


# Events that must never be forwarded to upstream
_SUPPRESSED_EVENTS = frozenset({"metadata", "end"})


class MQStreamBridge(StreamBridge):
    """Publishes LangGraph stream events as MQ protocol v2.3 messages.

    Each publish call wraps the LangGraph event in a progress envelope and
    sends it to the result topic.  Result and error envelopes are sent via
    the dedicated publish_result / publish_error methods.

    Thread safety: register_run / unregister_run are expected to be called
    from a single asyncio task per run; concurrent calls for different runs
    are safe because dict operations on CPython are GIL-protected.
    """

    def __init__(self, producer: MQProducer, *, result_topic: str = "$AGENT_RESULTS") -> None:
        self._producer = producer
        self._result_topic = result_topic
        self._runs: dict[str, _RunContext] = {}

    # ── Run context ───────────────────────────────────────────────────────────

    def register_run(self, run_id: str, thread_id: str, reply_config: ReplyConfig) -> None:
        """Associate a run with its thread and reply configuration before streaming."""
        self._runs[run_id] = _RunContext(thread_id=thread_id, reply_config=reply_config)

    def unregister_run(self, run_id: str) -> None:
        """Remove run context after streaming is complete."""
        self._runs.pop(run_id, None)

    # ── StreamBridge interface ────────────────────────────────────────────────

    async def publish(self, run_id: str, event: str, data: Any) -> None:
        """Publish a single LangGraph stream event as a progress MQ message.

        Filtering rules (per MQ protocol v2.3):
          - metadata and end events are suppressed.
          - All events are suppressed when reply_config.stream_events=False.
          - Events not in reply_config.stream_event_types are suppressed.
        """
        ctx = self._runs.get(run_id)
        if ctx is None:
            logger.warning("publish called for unregistered run_id=%s; dropping event=%s", run_id, event)
            return

        if event in _SUPPRESSED_EVENTS:
            return
        if not ctx.reply_config.stream_events:
            return
        if event not in ctx.reply_config.stream_event_types:
            return

        seq = ctx.seq
        ctx.seq += 1

        envelope = self._build_envelope(
            message_id=run_id,
            type="progress",
            payload={"event_type": event, "data": data},
            seq=seq,
            thread_id=ctx.thread_id,
        )
        await self._send(envelope)

    async def publish_end(self, run_id: str) -> None:
        """No-op: MQ stream termination is signalled by publish_result/publish_error."""

    def subscribe(
        self,
        run_id: str,
        *,
        last_event_id: str | None = None,
        heartbeat_interval: float = 15.0,
    ) -> AsyncIterator[StreamEvent]:
        raise NotImplementedError("MQStreamBridge is write-only; use MQ consumer for reading")

    async def cleanup(self, run_id: str, *, delay: float = 0) -> None:
        """No-op: MQ manages message lifecycle independently."""

    # ── MQ-specific publish methods ───────────────────────────────────────────

    async def publish_result(
        self,
        run_id: str,
        *,
        status: str,
        thread_id: str,
        stream_events: bool,
        final_state: dict | None = None,
        usage: dict | None = None,
    ) -> None:
        """Publish the terminal result envelope for a task run.

        Protocol rules:
          - stream_events=True: omit final_state (client already has full stream).
          - stream_events=False: include final_state in the payload.
        """
        ctx = self._runs.get(run_id)
        seq = ctx.seq if ctx else 0

        payload: dict[str, Any] = {"status": status}

        if usage:
            payload["usage"] = usage
        if final_state is not None:
            payload["final_state"] = final_state

        envelope = self._build_envelope(
            message_id=run_id,
            type="result",
            payload=payload,
            seq=seq,
            thread_id=thread_id,
        )
        await self._send(envelope)

    async def publish_error(
        self,
        run_id: str,
        code: str,
        *,
        thread_id: str,
        message: str = "",
        retriable: bool = False,
        node: str | None = None,
    ) -> None:
        """Publish an error envelope for a task run.

        Error codes: AGENT_TIMEOUT | TOOL_FAILED | QUOTA_EXCEEDED |
                     INTERNAL_ERROR | AGENT_BUSY | INVALID_SCHEMA
        """
        ctx = self._runs.get(run_id)
        seq = ctx.seq if ctx else 0

        error: dict[str, Any] = {"code": code, "retriable": retriable}
        if message:
            error["message"] = message
        if node:
            error["node"] = node

        envelope = self._build_envelope(
            message_id=run_id,
            type="error",
            payload={"error": error},
            seq=seq,
            thread_id=thread_id,
        )
        await self._send(envelope)

    async def replay(self, message_id: str, thread_id: str, result_cache: dict) -> None:
        """Replay a cached result for a duplicate message delivery.

        Bypasses run registration so it works outside an active AgentRunner run.
        Sends events in protocol order:
          1. tool_approval_required custom progress event (HIL pause only)
          2. Terminal result or error envelope
        """
        seq = 0

        if tool_approval := result_cache.get("tool_approval_required"):
            progress = self._build_envelope(
                message_id=message_id,
                type="progress",
                payload={"event_type": "custom", "data": tool_approval},
                seq=seq,
                thread_id=thread_id,
            )
            await self._send(progress)
            seq += 1

        if "error" in result_cache:
            terminal = self._build_envelope(
                message_id=message_id,
                type="error",
                payload=result_cache,
                seq=seq,
                thread_id=thread_id,
            )
        else:
            status_payload = {k: v for k, v in result_cache.items() if k != "tool_approval_required"}
            terminal = self._build_envelope(
                message_id=message_id,
                type="result",
                payload=status_payload,
                seq=seq,
                thread_id=thread_id,
            )
        await self._send(terminal)

    async def publish_pong(
        self,
        ping_message_id: str,
        instance_id: str,
        *,
        target_instance_id: str | None = None,
        target_status: str | None = None,
        last_heartbeat: str | None = None,
    ) -> None:
        """Publish a pong reply to a ping health-check message.

        For broadcast pings (no target), payload contains only ``instance_id``.
        For targeted pings, payload also includes ``target_instance_id``,
        ``target_status`` (active | draining | not_found), and ``last_heartbeat``.
        """
        payload: dict[str, Any] = {"instance_id": instance_id}
        if target_instance_id is not None:
            payload["target_instance_id"] = target_instance_id
            payload["target_status"] = target_status
            if last_heartbeat is not None:
                payload["last_heartbeat"] = last_heartbeat
        envelope = self._build_envelope(
            message_id=ping_message_id,
            type="pong",
            payload=payload,
            seq=0,
            thread_id="",
        )
        await self._send(envelope)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _build_envelope(
        self,
        *,
        message_id: str,
        type: str,
        payload: dict,
        seq: int,
        thread_id: str,
    ) -> dict:
        dt = datetime.now(UTC)
        timestamp = dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"
        return {
            "schema_version": "2.3",
            "message_id": message_id,
            "message_seq": seq,
            "timestamp": timestamp,
            "type": type,
            "payload": payload,
            "thread_id": thread_id,
        }

    async def _send(self, envelope: dict) -> None:
        body = json.dumps(envelope, ensure_ascii=False).encode()
        keys = envelope.get("message_id", "")
        await self._producer.send_async(body, keys=keys)
