"""TaskConsumer — routes incoming RocketMQ messages to AgentRunner.

Implements the routing algorithm from Consumer运行管理设计 §4.3:
  - ping   → pong reply (optional instance_id targeting)
  - cancel → insert cancel signal
  - task   → idempotency check → atomic claim → run or enqueue/reject

One instance is shared per Consumer process.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re

from app.consumer.agent_runner import AgentRunner
from app.consumer.constants import ClaimResult, MessageMode, QueuePolicy
from app.consumer.run_registry import RunRegistry
from app.consumer.schemas import SchemaValidationError, TaskMessage
from app.consumer.stream_bridge.mq import MQStreamBridge

logger = logging.getLogger(__name__)

# Regex to salvage message_id / thread_id from malformed JSON bodies.
# Used only when json.loads() fails so error envelopes can still be sent.
_RESCUE_MSG_ID_RE = re.compile(rb'"message_id"\s*:\s*"([^"]{1,256})"')
_RESCUE_THREAD_ID_RE = re.compile(rb'"thread_id"\s*:\s*"([^"]{1,256})"')


class TaskConsumer:
    """Routes RocketMQ messages to the appropriate handler.

    Args:
        registry: Shared RunRegistry for all DB operations.
        runner: AgentRunner that executes task messages.
        bridge: MQStreamBridge for publishing replies (pong, error).
        instance_id: Stable identity for this Consumer process ("{hostname}-{pid}").
        max_concurrent: Maximum simultaneous agent runs on this instance.
    """

    def __init__(
        self,
        registry: RunRegistry,
        runner: AgentRunner,
        bridge: MQStreamBridge,
        instance_id: str,
        max_concurrent: int = 10,
        active_tasks: set[asyncio.Task] | None = None,
    ) -> None:
        self._registry = registry
        self._runner = runner
        self._bridge = bridge
        self._instance_id = instance_id
        self._sem = asyncio.Semaphore(max_concurrent)
        self._max_concurrent = max_concurrent
        self._running_count = 0
        self._active_tasks: set[asyncio.Task] = active_tasks if active_tasks is not None else set()

    @property
    def available_slots(self) -> int:
        """Semaphore slots currently available (for poll-loop throttling)."""
        return self._max_concurrent - self._running_count

    async def shutdown(self, timeout: float = 30.0) -> None:
        """Wait for all active agent run tasks to finish, then cancel stragglers.

        Called during graceful shutdown before tearing down DB / MQ / checkpointer.
        """
        if not self._active_tasks:
            return
        logger.info("Waiting up to %.0fs for %d active agent run(s) to finish...", timeout, len(self._active_tasks))
        _, pending = await asyncio.wait(self._active_tasks, timeout=timeout)
        if pending:
            logger.warning("Cancelling %d agent run(s) that did not finish in time", len(pending))
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

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
                    rescued_msg_id,
                    "INVALID_SCHEMA",
                    thread_id=rescued_thread_id,
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
                    message_id,
                    "INVALID_SCHEMA",
                    thread_id=thread_id or "",
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
            last_heartbeat = None if row is None else row.last_heartbeat.isoformat()
            await self._bridge.publish_pong(
                message.message_id,
                self._instance_id,
                target_instance_id=target,
                target_status=target_status,
                last_heartbeat=last_heartbeat,
            )
        else:
            await self._bridge.publish_pong(message.message_id, self._instance_id)
        logger.debug("Pong sent for ping message_id=%s target=%s", message.message_id, target)

    async def _handle_cancel(self, message: TaskMessage) -> None:
        """Write a cancel signal for the thread; AgentRunner polls and reacts."""
        reason = message.config.get("reason")
        await self._registry.insert_cancel_signal(message.thread_id, reason)
        logger.info(
            "Cancel signal inserted for thread=%s reason=%s (message_id=%s)",
            message.thread_id,
            reason,
            message.message_id,
        )

    async def _handle_task(self, message: TaskMessage, raw_envelope: dict) -> None:
        """Main task routing: idempotency → claim → run | enqueue | reject."""
        # ① Skip re-execution on duplicate delivery
        existing = await self._registry.check_processed(message.message_id)
        if existing is not None:
            logger.info(
                "Duplicate message_id=%s (status=%s); replaying cached result if available",
                message.message_id,
                existing.status,
            )
            if existing.result_cache:
                await self._bridge.replay(
                    message.message_id,
                    message.thread_id,
                    existing.result_cache,
                )
            return

        # ② Only one instance runs a thread at a time
        result = await self._registry.claim_thread(
            message.thread_id,
            self._instance_id,
            message.message_id,
        )

        if result == ClaimResult.CLAIMED:
            # ③ Crash-recovery row must be written before execution starts
            await self._registry.upsert_current_msg(
                message.thread_id, message.message_id, raw_envelope
            )
            await self._start_run(message)
        else:
            await self._handle_busy(message, raw_envelope)

    # ── Helpers ───────────────────────────────────────────────────────────────

    async def _start_run(self, message: TaskMessage) -> None:
        """Acquire concurrency slot and fire the agent run as a background task."""
        await self._sem.acquire()
        self._running_count += 1
        task = asyncio.create_task(
            self._run_and_release(message),
            name=f"run-{message.message_id[:8]}",
        )
        self._active_tasks.add(task)
        task.add_done_callback(self._active_tasks.discard)
        logger.info(
            "Started run thread=%s message_id=%s task=%s",
            message.thread_id,
            message.message_id,
            task.get_name(),
        )

    async def _run_and_release(self, message: TaskMessage) -> None:
        """Wrapper that releases the semaphore after the run finishes."""
        try:
            await self._runner.run(message)
        finally:
            self._running_count -= 1
            self._sem.release()

    async def _handle_busy(self, message: TaskMessage, raw_envelope: dict) -> None:
        """Handle the case where the thread is already running on another task."""
        mode = message.message_mode
        if mode == MessageMode.REJECT:
            logger.info(
                "Thread=%s busy; rejecting message_id=%s (message_mode=reject)",
                message.thread_id,
                message.message_id,
            )
            await self._bridge.publish_error(
                message.message_id,
                "AGENT_BUSY",
                thread_id=message.thread_id,
                retriable=True,
                message="Thread is already running; retry later",
            )
            return
        # followup (default) and steer both enqueue; steer degrades to followup for now
        if mode == MessageMode.STEER:
            logger.info(
                "Steer not yet implemented; degrading to followup for message_id=%s",
                message.message_id,
            )
        await self._registry.enqueue_inject(
            message.thread_id,
            message.message_id,
            raw_envelope,
            QueuePolicy.FOLLOWUP,
        )
        logger.info(
            "Enqueued followup for thread=%s message_id=%s",
            message.thread_id,
            message.message_id,
        )


