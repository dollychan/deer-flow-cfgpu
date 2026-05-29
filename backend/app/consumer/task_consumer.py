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
from app.consumer.constants import (
    ClaimResult,
    MessageMode,
    ProcessedStatus,
    QueuePolicy,
    ThreadStatus,
)
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
                await self._handle_cancel(message, raw_envelope)
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
                self._instance_id,
                echo=message.downlink_echo(),
                target_instance_id=target,
                target_status=target_status,
                last_heartbeat=last_heartbeat,
            )
        else:
            await self._bridge.publish_pong(self._instance_id, echo=message.downlink_echo())
        logger.debug("Pong sent for ping message_id=%s target=%s", message.message_id, target)

    async def _handle_cancel(self, message: TaskMessage, raw_envelope: dict) -> None:
        """Enqueue a cancel row in thread_msg_queue with thread_msg_seq for ordering."""
        inserted = await self._registry.enqueue_message(
            message.thread_id,
            message.message_id,
            raw_envelope,
            message.thread_msg_seq,
            QueuePolicy.CANCEL,
        )
        if not inserted:
            logger.info(
                "Duplicate cancel message_id=%s already queued; skipping enqueue",
                message.message_id,
            )
            return
        logger.info(
            "Cancel enqueued thread=%s seq=%d message_id=%s",
            message.thread_id,
            message.thread_msg_seq,
            message.message_id,
        )

    async def _handle_task(self, message: TaskMessage, raw_envelope: dict) -> None:
        """Main task routing: idempotency → reject-check → enqueue → try_dispatch."""
        # ① Skip re-execution on duplicate delivery
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

        # ② reject 模式快速短路：thread running + message_mode=reject → error + return
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

        # ③ steer degrades to followup for now
        if message.message_mode == MessageMode.STEER:
            logger.info(
                "Steer not yet implemented; degrading to followup for message_id=%s",
                message.message_id,
            )

        # ④ enqueue as followup (all task messages go to queue first)
        inserted = await self._registry.enqueue_message(
            message.thread_id,
            message.message_id,
            raw_envelope,
            message.thread_msg_seq,
            QueuePolicy.FOLLOWUP,
        )
        if not inserted:
            logger.info(
                "Duplicate message_id=%s already queued/current; skipping enqueue",
                message.message_id,
            )
            return

        # ⑤ try to dispatch immediately if thread is idle and slots available
        asyncio.create_task(
            self._try_dispatch(message.thread_id),
            name=f"dispatch-{message.message_id[:8]}",
        )

    # ── Helpers ───────────────────────────────────────────────────────────────

    async def _try_dispatch(self, thread_id: str) -> None:
        """Attempt to dispatch the next queued task for a thread.

        Called after every enqueue. Acquires a semaphore slot and claims the thread
        only if the thread is idle; if running, _drain_and_release handles the chain.
        """
        if self._sem._value == 0:
            return

        pending = await self._registry.peek_thread_queue(
            thread_id,
            policies=(QueuePolicy.FOLLOWUP, QueuePolicy.CANCEL, QueuePolicy.PREFIX),
        )
        if not pending:
            return

        # cancel barrier: convert pre-cancel followups to prefix, notify upstream
        cancel_idx = next(
            (i for i, r in enumerate(pending) if r.policy == QueuePolicy.CANCEL), None
        )
        if cancel_idx is not None:
            cancel_row = pending[cancel_idx]
            tasks_before = [
                r for r in pending[:cancel_idx]
                if r.policy in (QueuePolicy.FOLLOWUP, QueuePolicy.PREFIX)
            ]
            if tasks_before:
                await self._registry.convert_to_prefix(
                    thread_id, [r.id for r in tasks_before]
                )
                for row in tasks_before:
                    await self._bridge.publish_result(
                        row.message_id,
                        status=ProcessedStatus.CANCELLED,
                        stream_events=False,
                        echo={
                            "message_id": row.message_id,
                            "thread_id": thread_id,
                            "thread_msg_seq": row.thread_msg_seq,
                        },
                    )
            await self._registry.delete_queue_items(thread_id, [cancel_row.id])
            asyncio.create_task(self._try_dispatch(thread_id))
            return

        followup_rows = [r for r in pending if r.policy == QueuePolicy.FOLLOWUP]
        prefix_rows   = [r for r in pending if r.policy == QueuePolicy.PREFIX]
        if not followup_rows:
            return

        next_row = followup_rows[0]

        # Acquire slot, then claim the thread atomically
        await self._sem.acquire()
        self._running_count += 1
        try:
            result = await self._registry.claim_thread(
                thread_id, self._instance_id, next_row.message_id, next_row.thread_msg_seq
            )
            if result == ClaimResult.RUNNING:
                # Another instance or _drain_and_release already holds the thread
                self._running_count -= 1
                self._sem.release()
                return
            await self._registry.upsert_current_msg(
                thread_id, next_row.message_id, next_row.body, next_row.thread_msg_seq
            )
            await self._registry.delete_queue_items(
                thread_id, [next_row.id] + [r.id for r in prefix_rows]
            )
        except Exception:
            self._running_count -= 1
            self._sem.release()
            raise

        # TODO: merge prefix_rows messages into next_task input for full prefix context
        next_task = TaskMessage.from_json(json.dumps(next_row.body))
        task = asyncio.create_task(
            self._run_and_release(next_task),
            name=f"run-{next_row.message_id[:8]}",
        )
        self._active_tasks.add(task)
        task.add_done_callback(self._active_tasks.discard)
        logger.info(
            "Dispatched thread=%s message_id=%s task=%s",
            thread_id,
            next_row.message_id,
            task.get_name(),
        )

    async def _run_and_release(self, message: TaskMessage) -> None:
        """Wrapper that releases the semaphore after the run finishes."""
        try:
            await self._runner.run(message)
        finally:
            self._running_count -= 1
            self._sem.release()
