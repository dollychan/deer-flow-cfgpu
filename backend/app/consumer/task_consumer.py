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

from app.consumer.agent_runner import AgentRunner
from app.consumer.constants import ClaimResult, MessageMode, QueuePolicy
from app.consumer.run_registry import RunRegistry
from app.consumer.schemas import TaskMessage
from app.consumer.stream_bridge.mq import MQStreamBridge

logger = logging.getLogger(__name__)


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
    ) -> None:
        self._registry = registry
        self._runner = runner
        self._bridge = bridge
        self._instance_id = instance_id
        self._sem = asyncio.Semaphore(max_concurrent)
        self._max_concurrent = max_concurrent
        self._running_count = 0

    @property
    def available_slots(self) -> int:
        """Semaphore slots currently available (for poll-loop throttling)."""
        return self._max_concurrent - self._running_count

    # ── Public entry point ────────────────────────────────────────────────────

    async def handle_message(self, body: str | bytes) -> None:
        """Deserialize and dispatch a single RocketMQ message body.

        Designed to be called from the MQ receive callback. Exceptions are
        caught and logged so the caller can always ack the message.
        """
        try:
            raw_envelope: dict = json.loads(body)
            message = TaskMessage.from_dict(raw_envelope)
        except Exception as exc:
            logger.error("Failed to deserialize MQ message: %s", exc)
            return

        try:
            if message.type == "ping":
                await self._handle_ping(message)
            elif message.type == "cancel":
                await self._handle_cancel(message)
            elif message.type == "task":
                await self._handle_task(message, raw_envelope)
            else:
                logger.warning("Unknown message type=%s, message_id=%s", message.type, message.message_id)
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
                await self._bridge.publish(
                    message.message_id,
                    "result",
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


