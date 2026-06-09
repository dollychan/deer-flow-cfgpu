"""TaskConsumer — v2 PollAck / Ingest layer (design §2.6, D1).

The thin MQ→DB entry point. It only deserializes, schema-validates, lands rows,
folds cancel watermarks, and pokes the Scheduler. It owns **no** claim / dispatch /
run tasks — those belong to the Scheduler (§2.5).

Per message type (§4.3/§5):
  - ping   → pong reply (optional instance_id targeting)
  - cancel → fold cancel_watermark (fire-and-forget, not enqueued) + poke
  - task   → idempotency check → reject short-circuit → enqueue(policy) + poke

ACK happens after this returns (ack-at-ingest, §9.1): the DB commit inside each
handler is the durability point, not Agent completion.
"""

from __future__ import annotations

import json
import logging
import re
from typing import TYPE_CHECKING

from app.consumer.constants import MessageMode, ThreadStatus
from app.consumer.run_registry import RunRegistry
from app.consumer.schemas import SchemaValidationError, TaskMessage
from app.consumer.stream_bridge.mq import MQStreamBridge
from app.consumer.timeutil import format_beijing

if TYPE_CHECKING:
    from app.consumer.scheduler import Scheduler

logger = logging.getLogger(__name__)

# Regex to salvage message_id / thread_id from malformed JSON bodies.
# Used only when json.loads() fails so error envelopes can still be sent.
_RESCUE_MSG_ID_RE = re.compile(rb'"message_id"\s*:\s*"([^"]{1,256})"')
_RESCUE_THREAD_ID_RE = re.compile(rb'"thread_id"\s*:\s*"([^"]{1,256})"')


class TaskConsumer:
    """Routes RocketMQ messages to ingest handlers (no execution, §2.6).

    Args:
        registry: Shared RunRegistry for all DB operations.
        bridge: MQStreamBridge for publishing replies (pong, error, replay).
        instance_id: Stable identity for this Consumer process ("{hostname}-{pid}").
        scheduler: Scheduler to poke after a commit lands new work (optional; when
            None, ingest still lands rows and the Scheduler's periodic tick picks them up).
        runner: Deprecated/ignored — accepted for backward-compatible construction only.
    """

    def __init__(
        self,
        registry: RunRegistry,
        bridge: MQStreamBridge,
        instance_id: str,
        *,
        scheduler: Scheduler | None = None,
        runner: object | None = None,  # noqa: ARG002 — deprecated, ignored
    ) -> None:
        self._registry = registry
        self._bridge = bridge
        self._instance_id = instance_id
        self._scheduler = scheduler

    def _poke(self) -> None:
        if self._scheduler is not None:
            self._scheduler.poke()

    async def shutdown(self, timeout: float = 30.0) -> None:
        """No-op in v2: ingest owns no in-flight runs (the Scheduler drains them, §2.6)."""

    # ── Public entry point ────────────────────────────────────────────────────

    async def handle_message(self, body: str | bytes) -> None:
        """Deserialize, validate, and dispatch a single RocketMQ message body.

        Designed to be called from the MQ receive callback. All exceptions are
        caught and logged so the caller can always ack the message.

        On schema validation failure the error is published back to the upstream
        caller via an ``INVALID_SCHEMA`` error envelope (when ``message_id`` is
        recoverable from the raw body).
        """
        # ── Step 1: JSON parse — extract identifiers for error reporting ──────
        message_id: str | None = None
        thread_id: str | None = None
        raw_envelope: dict
        try:
            raw_envelope = json.loads(body)
            message_id = raw_envelope.get("message_id") or None
            thread_id = raw_envelope.get("thread_id") or None
        except Exception as exc:
            logger.error("MQ message is not valid JSON: %s", exc)
            # Best-effort: salvage message_id from malformed body so we can send
            # an error envelope back to the caller even when JSON parse fails.
            raw_bytes = body if isinstance(body, bytes) else body.encode()
            m = _RESCUE_MSG_ID_RE.search(raw_bytes)
            if m:
                rescued_msg_id = m.group(1).decode(errors="replace")
                t = _RESCUE_THREAD_ID_RE.search(raw_bytes)
                rescued_thread_id = t.group(1).decode(errors="replace") if t else ""
                await self._bridge.publish_error(
                    "INVALID_SCHEMA",
                    echo={"message_id": rescued_msg_id, "thread_id": rescued_thread_id},
                    message=f"Message body is not valid JSON: {exc}",
                    retriable=False,
                )
            return

        # ── Step 2: Schema validation + deserialization ───────────────────────
        try:
            message = TaskMessage.from_dict(raw_envelope)
        except SchemaValidationError as exc:
            logger.error(
                "MQ schema validation failed message_id=%s thread_id=%s: %s",
                message_id,
                thread_id,
                exc.reason,
            )
            if message_id:
                await self._bridge.publish_error(
                    "INVALID_SCHEMA",
                    echo={"message_id": message_id, "thread_id": thread_id or ""},
                    message=exc.reason,
                    retriable=False,
                )
            return
        except Exception as exc:
            logger.error(
                "Failed to deserialize MQ message message_id=%s: %s", message_id, exc
            )
            return

        # ── Step 3: Dispatch by message type ──────────────────────────────────
        try:
            if message.type == "ping":
                await self._handle_ping(message)
            elif message.type == "cancel":
                await self._handle_cancel(message)
            elif message.type == "task":
                await self._handle_task(message, raw_envelope)
        except Exception as exc:
            logger.exception(
                "Unhandled error dispatching message_id=%s type=%s: %s",
                message.message_id,
                message.type,
                exc,
            )

    # ── Message-type handlers ─────────────────────────────────────────────────

    async def _handle_ping(self, message: TaskMessage) -> None:
        """Reply with a pong. Targeted pings query DB so any Consumer can answer accurately."""
        target = message.config.get("instance_id")
        if target:
            row = await self._registry.get_instance(target)
            target_status = "not_found" if row is None else row.status
            # Frontend reads timestamps in Beijing wall-clock; the DB column is UTC.
            last_heartbeat = None if row is None else format_beijing(row.last_heartbeat)
            await self._bridge.publish_pong(
                self._instance_id,
                echo=message.downlink_echo(),
                target_instance_id=target,
                target_status=target_status,
                last_heartbeat=last_heartbeat,
            )
        else:
            await self._bridge.publish_pong(self._instance_id, echo=message.downlink_echo())
        logger.debug("Pong sent for ping message_id=%s target=%s", message.message_id, target)

    async def _handle_cancel(self, message: TaskMessage) -> None:
        """Fold cancel(seq=N) into cancel_watermark (fire-and-forget, not enqueued, §6.4/D2).

        Idempotent: a redelivered cancel just re-folds GREATEST(watermark, same N). When the
        fold clears a HIL gate, fold_cancel_watermark atomically synthesizes a drain row in
        the same transaction (§6.5/I12), so we only need to poke afterwards.
        """
        synthesized = await self._registry.fold_cancel_watermark(
            message.thread_id, message.thread_msg_seq
        )
        self._poke()
        logger.info(
            "Cancel folded thread=%s watermark>=%d message_id=%s drain=%s",
            message.thread_id,
            message.thread_msg_seq,
            message.message_id,
            synthesized,
        )

    async def _handle_task(self, message: TaskMessage, raw_envelope: dict) -> None:
        """Idempotency → reject short-circuit → enqueue(derived policy) → poke (§5.3/§9.2)."""
        # ① Skip re-execution on duplicate delivery; replay cached result if any.
        existing = await self._registry.check_processed(message.message_id)
        if existing is not None:
            logger.info(
                "Duplicate message_id=%s (status=%s); replaying cached result if available",
                message.message_id,
                existing.status,
            )
            if existing.result_cache:
                await self._bridge.replay(existing.result_cache, echo=message.downlink_echo())
            return

        # ② reject mode fast short-circuit: thread running + message_mode=reject → AGENT_BUSY
        if message.message_mode == MessageMode.REJECT:
            state = await self._registry.get_thread_state(message.thread_id)
            if state and state.status == ThreadStatus.RUNNING:
                logger.info(
                    "Thread=%s busy; rejecting message_id=%s (message_mode=reject)",
                    message.thread_id,
                    message.message_id,
                )
                await self._bridge.publish_error(
                    "AGENT_BUSY",
                    echo=message.downlink_echo(),
                    retriable=True,
                    message="Thread is already running; retry later",
                )
                return

        # ③ steer degrades to followup for now (§5.3)
        if message.message_mode == MessageMode.STEER:
            logger.info(
                "Steer not yet implemented; degrading to followup for message_id=%s",
                message.message_id,
            )

        # ④ enqueue with the policy derived once at ingest (fork > resume > collect > followup).
        inserted = await self._registry.enqueue_message(
            message.thread_id,
            message.message_id,
            raw_envelope,
            message.thread_msg_seq,
            str(message.derived_policy),
        )
        if not inserted:
            logger.info(
                "Duplicate message_id=%s already queued; skipping enqueue (ACK only)",
                message.message_id,
            )
            return

        # ⑤ poke the Scheduler so the new row is claimed without waiting for the next tick.
        self._poke()
