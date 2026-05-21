"""Consumer process entry point.

Starts a standalone DeerFlow Consumer that reads task messages from RocketMQ
($AGENT_TASKS) and control signals from ($AGENT_SIGNALS), runs LangGraph
agent graphs, and publishes results back to RocketMQ ($AGENT_RESULTS).

Usage::

    # From the backend/ directory:
    python -m app.consumer

Configuration::

    # config.yaml (supports $ENV_VAR substitution)
    consumer:
      endpoint: $ROCKETMQ_ENDPOINT          # host:port
      username: $ROCKETMQ_USERNAME           # access key
      password: $ROCKETMQ_PASSWORD           # secret key
      task_topic: $AGENT_TASKS               # task messages only
      signal_topic: $AGENT_SIGNALS           # cancel / ping control signals
      result_topic: $AGENT_RESULTS
      consumer_group: $AGENT_CONSUMER_GROUP
      signal_consumer_group: $AGENT_SIGNAL_CONSUMER_GROUP
      max_concurrent_runs: 10
      poll_batch_size: 20
      invisible_duration_seconds: 300        # must exceed max agent run time

Requirements:
    - database.backend: sqlite or postgres  (memory is rejected at startup)
    - RocketMQ gRPC endpoint reachable from this host
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import socket
from concurrent.futures import ThreadPoolExecutor
from typing import Any

# ── CRITICAL: import Consumer ORM models BEFORE init_engine_from_config ──────
# This registers the 5 consumer tables (consumer_instances, thread_run_state,
# thread_msg_queue, thread_cancel_signals, processed_messages) into
# Base.metadata so that create_all() creates them alongside the core tables.
import app.consumer.models  # noqa: F401

from deerflow.config.app_config import get_app_config
from deerflow.persistence import close_engine, get_session_factory, init_engine_from_config
from deerflow.runtime.checkpointer import make_checkpointer

from app.consumer.agent_runner import AgentRunner
from app.consumer.run_registry import RunRegistry
from app.consumer.schemas import TaskMessage
from app.consumer.stream_bridge.mq import MQStreamBridge
from app.consumer.task_consumer import TaskConsumer

logger = logging.getLogger(__name__)


# ── RocketMQ producer adapter ─────────────────────────────────────────────────


class _RocketMQProducerAdapter:
    """Wraps the sync RocketMQ Producer to satisfy the MQProducer protocol.

    `send_async` builds a Message envelope and dispatches the blocking
    `producer.send()` call on a thread-pool executor.
    """

    def __init__(self, producer: Any, result_topic: str, tag: str = "", executor: ThreadPoolExecutor | None = None) -> None:
        self._producer = producer
        self._result_topic = result_topic
        self._tag = tag
        self._executor = executor

    async def send_async(self, body: bytes, *, keys: str = "") -> None:
        from rocketmq import Message

        msg = Message()
        msg.topic = self._result_topic
        msg.body = body
        if self._tag:
            msg.tag = self._tag
        if keys:
            msg.keys = keys

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._executor, self._producer.send, msg)


# ── Poll loop ─────────────────────────────────────────────────────────────────


async def _poll_loop(
    mq_consumer: Any,
    task_consumer: TaskConsumer,
    executor: ThreadPoolExecutor,
    *,
    batch_size: int,
    invisible_duration: int,
    stop_event: asyncio.Event,
    throttle: bool,
    task_prefix: str,
    loop_name: str,
) -> None:
    """Pull messages from an MQ consumer and dispatch to TaskConsumer.

    When throttle=True (task topic): backs off when all run slots are occupied
    so unbounded tasks don't pile up behind the semaphore.
    When throttle=False (signal topic): always polls — cancel/ping must not be
    delayed by semaphore pressure on the task topic.
    """
    loop = asyncio.get_running_loop()

    while not stop_event.is_set():
        if throttle:
            available = task_consumer.available_slots
            if available == 0:
                await asyncio.sleep(0.2)
                continue
            capacity = min(batch_size, available)
        else:
            capacity = batch_size

        try:
            msgs = await loop.run_in_executor(
                executor,
                mq_consumer.receive,
                capacity,
                invisible_duration,
            )
        except Exception as exc:
            if not stop_event.is_set():
                logger.warning("RocketMQ %s receive error: %s", loop_name, exc)
                await asyncio.sleep(1)
            continue

        for msg in msgs:
            if stop_event.is_set():
                break
            body = getattr(msg, "body", b"")
            asyncio.create_task(
                _handle_and_ack(msg, body, mq_consumer, task_consumer, executor),
                name=f"{task_prefix}-{str(getattr(msg, 'message_id', ''))[:8]}",
            )


async def _handle_and_ack(
    msg: Any,
    body: bytes | str,
    mq_consumer: Any,
    task_consumer: TaskConsumer,
    executor: ThreadPoolExecutor,
) -> None:
    """Dispatch to TaskConsumer then ack unconditionally.

    TaskConsumer.handle_message never raises — errors are converted to
    MQ error envelopes. We always ack so the message is not redelivered.
    """
    try:
        await task_consumer.handle_message(body)
    except Exception as exc:
        logger.exception("Unexpected error in handle_message: %s", exc)
    finally:
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(executor, mq_consumer.ack, msg)
        except Exception as exc:
            logger.warning("Failed to ack message %s: %s", getattr(msg, "message_id", "?"), exc)


# ── Background watchdog coroutines ────────────────────────────────────────────


async def _instance_heartbeat_loop(registry: RunRegistry, instance_id: str, interval: int = 10) -> None:
    while True:
        await asyncio.sleep(interval)
        try:
            await registry.heartbeat_instance(instance_id)
        except Exception:
            logger.debug("Instance heartbeat failed for %s", instance_id, exc_info=True)


async def _stale_run_watchdog(
    registry: RunRegistry,
    runner: AgentRunner,
    instance_id: str,
    interval: int = 30,
    timeout_seconds: int = 60,
    max_retries: int = 3,
) -> None:
    """Detect and recover stale running threads from dead Consumer instances.

    For each stale run (both run heartbeat and owning instance heartbeat expired):
      - If already in processed_messages → trigger drain (followup) or mark idle.
      - If retry_count < max_retries → claim_stale_run + AgentRunner.run() from
        LangGraph checkpoint; LangGraph resumes from the exact breakpoint.
      - If retry_count >= max_retries → publish FATAL error and mark thread idle.
    """
    while True:
        await asyncio.sleep(interval)
        try:
            stale = await registry.find_stale_runs(timeout_seconds)
            for row in stale:
                logger.warning(
                    "Stale run detected: thread=%s instance=%s message=%s retry_count=%d",
                    row.thread_id,
                    row.instance_id,
                    row.message_id,
                    row.retry_count,
                )

                # Already completed before crash — clean up and drain followups
                processed = await registry.check_processed(row.message_id)
                if processed is not None:
                    logger.info(
                        "Stale run already completed (status=%s) thread=%s; triggering drain",
                        processed.status,
                        row.thread_id,
                    )
                    await runner.trigger_drain(row.thread_id)
                    continue

                # Exceeded retry budget — publish fatal and abandon
                if row.retry_count >= max_retries:
                    logger.error(
                        "Stale run exceeded max_retries=%d thread=%s; publishing FATAL error",
                        max_retries,
                        row.thread_id,
                    )
                    await runner.publish_fatal_error(
                        row.message_id,
                        row.thread_id,
                        f"Agent crashed repeatedly; giving up after {max_retries} retries",
                    )
                    await registry.mark_thread_idle(row.thread_id)
                    continue

                # Compete for ownership (multi-Consumer: only one watchdog wins)
                claimed = await registry.claim_stale_run(row.thread_id, instance_id)
                if not claimed:
                    logger.info(
                        "Stale run already reclaimed by another instance: thread=%s",
                        row.thread_id,
                    )
                    continue

                current = await registry.get_current_msg(row.thread_id)
                if current is None:
                    logger.warning(
                        "No current msg for stale run thread=%s; marking idle",
                        row.thread_id,
                    )
                    await registry.mark_thread_idle(row.thread_id)
                    continue

                await registry.increment_retry_count(row.thread_id)
                try:
                    message = TaskMessage.from_json(json.dumps(current.body))
                except Exception as exc:
                    logger.error(
                        "Failed to reconstruct TaskMessage for stale retry thread=%s: %s",
                        row.thread_id,
                        exc,
                    )
                    await registry.mark_thread_idle(row.thread_id)
                    continue

                logger.info(
                    "Retrying stale run thread=%s message=%s (attempt %d/%d)",
                    row.thread_id,
                    row.message_id,
                    row.retry_count + 1,
                    max_retries,
                )
                asyncio.create_task(
                    runner.run(message),
                    name=f"stale-retry-{row.message_id[:8]}",
                )
        except Exception:
            logger.debug("Stale run watchdog error", exc_info=True)


async def _processed_messages_cleanup(registry: RunRegistry, ttl_days: int, interval: int = 3600) -> None:
    """Hourly cleanup of processed_messages records older than ttl_days."""
    while True:
        await asyncio.sleep(interval)
        try:
            deleted = await registry.cleanup_processed_messages(ttl_days)
            if deleted:
                logger.info("Cleaned up %d expired processed_messages records (ttl=%dd)", deleted, ttl_days)
        except Exception:
            logger.debug("processed_messages cleanup error", exc_info=True)


# ── Main ──────────────────────────────────────────────────────────────────────


async def main() -> None:
    # 1. Load configuration
    config = get_app_config()
    consumer_cfg = config.consumer

    logging.basicConfig(
        level=getattr(logging, config.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if not consumer_cfg.endpoint:
        raise RuntimeError(
            "consumer.endpoint is not configured. "
            "Set ROCKETMQ_ENDPOINT (or consumer.endpoint in config.yaml)."
        )

    # 2. Init DB — consumer ORM tables already registered by the top-level import
    await init_engine_from_config(config.database)
    session_factory = get_session_factory()
    if session_factory is None:
        raise RuntimeError(
            "Consumer requires a persistent database backend. "
            "Set database.backend to 'sqlite' or 'postgres' in config.yaml."
        )

    # 3. Init LangGraph checkpointer (context-manager owns the connection)
    async with make_checkpointer(config) as checkpointer:
        # 4. Build thread-pool executor shared by MQ producer + consumers
        # Needs 6 workers: task-receive(1) + signal-receive(1) + send(1) + ack×2 + spare(1)
        executor = ThreadPoolExecutor(max_workers=6, thread_name_prefix="rmq")

        # 5. Build RocketMQ producer
        from rocketmq import ClientConfiguration, Credentials, FilterExpression, Producer, SimpleConsumer

        credentials = Credentials(consumer_cfg.username, consumer_cfg.password)
        mq_client_config = ClientConfiguration(consumer_cfg.endpoint, credentials, request_timeout=10)

        mq_producer = Producer(mq_client_config, (consumer_cfg.result_topic,))
        mq_producer.startup()
        logger.info("RocketMQ producer started (result_topic=%s)", consumer_cfg.result_topic)

        producer_adapter = _RocketMQProducerAdapter(
            mq_producer,
            result_topic=consumer_cfg.result_topic,
            executor=executor,
        )

        # 6. Build Consumer components
        instance_id = f"{socket.gethostname()}-{os.getpid()}"
        registry = RunRegistry(session_factory)
        await registry.register_instance(instance_id, socket.gethostname(), os.getpid())
        logger.info("Consumer instance registered: %s", instance_id)

        bridge = MQStreamBridge(producer_adapter, result_topic=consumer_cfg.result_topic)
        active_tasks: set[asyncio.Task] = set()
        runner = AgentRunner(registry, bridge, checkpointer, config, task_registry=active_tasks)
        task_consumer = TaskConsumer(
            registry,
            runner,
            bridge,
            instance_id,
            max_concurrent=consumer_cfg.max_concurrent_runs,
            active_tasks=active_tasks,
        )

        # 7. Build RocketMQ consumers (SimpleConsumer — message-granularity, POP mode)
        def _make_filter(tag: str) -> FilterExpression:
            if tag:
                from rocketmq.grpc_protocol import FilterType
                return FilterExpression(f"TAGS = '{tag}'", FilterType.SQL)
            return FilterExpression("*")

        # Task consumer ($AGENT_TASKS): throttled by semaphore slots
        mq_task_consumer = SimpleConsumer(
            mq_client_config,
            consumer_cfg.consumer_group,
            subscription={consumer_cfg.task_topic: _make_filter(consumer_cfg.task_topic_tag)},
            await_duration=20,
        )
        mq_task_consumer.startup()
        logger.info(
            "RocketMQ task consumer started (task_topic=%s group=%s)",
            consumer_cfg.task_topic,
            consumer_cfg.consumer_group,
        )

        # Signal consumer ($AGENT_SIGNALS): always polls, not throttled
        mq_signal_consumer = SimpleConsumer(
            mq_client_config,
            consumer_cfg.signal_consumer_group,
            subscription={consumer_cfg.signal_topic: _make_filter(consumer_cfg.signal_topic_tag)},
            await_duration=20,
        )
        mq_signal_consumer.startup()
        logger.info(
            "RocketMQ signal consumer started (signal_topic=%s group=%s)",
            consumer_cfg.signal_topic,
            consumer_cfg.signal_consumer_group,
        )

        # 8. Shutdown event + signal handlers
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop_event.set)

        # 9. Start background tasks
        bg_tasks = [
            asyncio.create_task(
                _instance_heartbeat_loop(registry, instance_id),
                name="instance-heartbeat",
            ),
            asyncio.create_task(
                _stale_run_watchdog(registry, runner, instance_id),
                name="stale-run-watchdog",
            ),
            asyncio.create_task(
                _poll_loop(
                    mq_task_consumer,
                    task_consumer,
                    executor,
                    batch_size=consumer_cfg.poll_batch_size,
                    invisible_duration=consumer_cfg.invisible_duration_seconds,
                    stop_event=stop_event,
                    throttle=True,
                    task_prefix="msg",
                    loop_name="task",
                ),
                name="task-poll-loop",
            ),
            asyncio.create_task(
                _poll_loop(
                    mq_signal_consumer,
                    task_consumer,
                    executor,
                    batch_size=10,
                    invisible_duration=30,
                    stop_event=stop_event,
                    throttle=False,
                    task_prefix="sig",
                    loop_name="signal",
                ),
                name="signal-poll-loop",
            ),
        ]
        if consumer_cfg.processed_messages_ttl_days > 0:
            bg_tasks.append(
                asyncio.create_task(
                    _processed_messages_cleanup(registry, consumer_cfg.processed_messages_ttl_days),
                    name="processed-messages-cleanup",
                )
            )

        logger.info(
            "Consumer %s ready — max_concurrent=%d task_batch=%d task_invisible=%ds signal_topic=%s",
            instance_id,
            consumer_cfg.max_concurrent_runs,
            consumer_cfg.poll_batch_size,
            consumer_cfg.invisible_duration_seconds,
            consumer_cfg.signal_topic,
        )

        # 10. Block until SIGTERM / SIGINT
        await stop_event.wait()
        logger.info("Shutdown signal received, stopping poll loop...")

        # 11. Graceful shutdown — stop polls, then wait for in-flight agent runs
        for task in bg_tasks:
            task.cancel()
        await asyncio.gather(*bg_tasks, return_exceptions=True)

        await task_consumer.shutdown(timeout=30.0)

        mq_task_consumer.shutdown()
        mq_signal_consumer.shutdown()
        logger.info("RocketMQ consumers shut down")

        await registry.mark_instance_draining(instance_id)
        await registry.delete_instance(instance_id)

        mq_producer.shutdown()
        logger.info("RocketMQ producer shut down")

        executor.shutdown(wait=False)

    await close_engine()
    logger.info("Consumer %s shutdown complete", instance_id)


if __name__ == "__main__":
    asyncio.run(main())
