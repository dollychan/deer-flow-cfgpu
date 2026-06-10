"""Consumer process entry point.

Starts a standalone DeerFlow Consumer that reads all message types (task/cancel/ping)
from a single RocketMQ topic ($AGENT_TASKS), runs LangGraph agent graphs, and
publishes results back to RocketMQ ($AGENT_RESULTS).

Usage::

    # From the backend/ directory:
    python -m app.consumer

Configuration::

    # config.yaml (supports $ENV_VAR substitution)
    consumer:
      endpoint: $ROCKETMQ_ENDPOINT          # host:port
      username: $ROCKETMQ_USERNAME           # access key
      password: $ROCKETMQ_PASSWORD           # secret key
      task_topic: $AGENT_TASKS               # single topic: task/cancel/ping
      result_topic: $AGENT_RESULTS
      consumer_group: $AGENT_CONSUMER_GROUP
      max_concurrent_runs: 10
      poll_batch_size: 20
      invisible_duration_seconds: 300        # must exceed max agent run time
      processed_messages_ttl_days: 7         # 0 = disabled

Requirements:
    - database.backend: sqlite or postgres  (memory is rejected at startup)
    - RocketMQ gRPC endpoint reachable from this host
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
from concurrent.futures import ThreadPoolExecutor
from typing import Any

# ── CRITICAL: import Consumer ORM models BEFORE init_engine_from_config ──────
# This registers the 4 consumer tables (consumer_instances, thread_run_state,
# thread_msg_queue, processed_messages) into Base.metadata so that create_all()
# creates them alongside the core deerflow tables.
import app.consumer.models  # noqa: F401
from app.consumer.agent_runner import AgentRunner
from app.consumer.constants import ProcessedStatus
from app.consumer.outbox import OutboxProducer
from app.consumer.run_registry import RunRegistry
from app.consumer.scheduler import Scheduler
from app.consumer.stream_bridge.mq import MQStreamBridge
from app.consumer.task_consumer import TaskConsumer
from deerflow.agents.memory.extraction_worker import run_extraction_loop
from deerflow.config.app_config import get_app_config
from deerflow.persistence import close_engine, get_session_factory, init_engine_from_config
from deerflow.runtime.checkpointer import make_checkpointer

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
) -> None:
    """Pull messages from a single MQ topic and dispatch to TaskConsumer.

    No throttle: all message types (task/cancel/ping) are pulled at full speed
    and immediately ACKed. Capacity control is done at dispatch time in _try_dispatch.
    """
    loop = asyncio.get_running_loop()

    while not stop_event.is_set():
        try:
            msgs = await loop.run_in_executor(
                executor,
                mq_consumer.receive,
                batch_size,
                invisible_duration,
            )
        except Exception as exc:
            if not stop_event.is_set():
                logger.warning("RocketMQ receive error: %s", exc)
                await asyncio.sleep(1)
            continue

        for msg in msgs:
            if stop_event.is_set():
                break
            body = getattr(msg, "body", b"")
            asyncio.create_task(
                _handle_and_ack(msg, body, mq_consumer, task_consumer, executor),
                name=f"msg-{str(getattr(msg, 'message_id', ''))[:8]}",
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
    scheduler: Scheduler,
    instance_id: str,
    interval: int = 30,
    timeout_seconds: int = 60,
    max_retries: int = 3,
) -> None:
    """Detect and recover stale running threads from dead Consumer instances (design §8).

    The watchdog only resets state — it never runs the graph itself; a normal sem-gated
    Scheduler claim picks the reset thread up and LangGraph resumes from its checkpoint.
    For each stale run (both run heartbeat and owning instance heartbeat expired):
      - already in processed_messages → finalize_run closes out the batch idempotently;
      - retry_count >= max_retries → finalize_run(failed) (FATAL goes to the outbox, §8);
      - otherwise → requeue_stale_run flips the batch back to pending + thread idle.
    All paths poke the Scheduler so an idle peer re-claims promptly.
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

                # Completed before crash — idempotent batch close-out (deletes rows + idle).
                processed = await registry.check_processed(row.message_id)
                if processed is not None:
                    await registry.finalize_run(row.message_id, None, processed.status)
                    scheduler.poke()
                    continue

                # Exceeded retry budget — FATAL terminal via outbox (§8), never direct publish.
                if row.retry_count >= max_retries:
                    logger.error(
                        "Stale run exceeded max_retries=%d thread=%s; FATAL via outbox",
                        max_retries,
                        row.thread_id,
                    )
                    fatal = {
                        "error": {
                            "code": "INTERNAL_ERROR",
                            "retriable": False,
                            "message": f"Agent crashed repeatedly; giving up after {max_retries} retries",
                        }
                    }
                    await registry.finalize_run(row.message_id, fatal, str(ProcessedStatus.FAILED))
                    scheduler.poke()
                    continue

                # Reset back into the claim pool (multi-instance: only one watchdog wins).
                if await registry.requeue_stale_run(row.thread_id):
                    logger.info(
                        "Stale run reset to pending thread=%s (attempt %d/%d)",
                        row.thread_id,
                        row.retry_count + 1,
                        max_retries,
                    )
                    scheduler.poke()
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


def _start_mlm_extraction_loop(
    checkpointer: Any,
    instance_id: str,
    stop_event: asyncio.Event,
) -> asyncio.Task:
    """Start the DB-backed MLM extraction worker as a named maintenance task (G6).

    The worker claims rows from ``memory_extraction_queue`` and reads each thread's
    latest checkpoint to extract memory, so it must share the *same* checkpointer
    instance the AgentRunner writes through — passing a different one would read an
    empty/foreign store. It self-disables when the persistence backend is ``memory``
    or MLM is turned off at runtime, and exits promptly on ``stop_event``.

    Returned as a named handle so the draining-first shutdown (§4.1/#4) cancels it in
    the maintenance-loop batch at step ④ (after the final outbox flush, before the
    MQ/producer close).
    """
    return asyncio.create_task(
        run_extraction_loop(checkpointer, instance_id, stop_event),
        name="mlm-extraction",
    )


# ── Main ──────────────────────────────────────────────────────────────────────


async def main() -> None:
    # 1. Load configuration
    config = get_app_config()
    consumer_cfg = config.consumer

    logging.basicConfig(
        level=getattr(logging, config.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # Alibaba Cloud RocketMQ SDK auto-exports client metrics via OTLP/gRPC to
    # port 8081 on the broker host. Suppress DEADLINE_EXCEEDED noise when that
    # port is firewalled — the consumer operates correctly without metrics export.
    logging.getLogger("opentelemetry.exporter.otlp.proto.grpc.exporter").setLevel(logging.CRITICAL)

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
        # 4. Build thread-pool executor shared by MQ producer + consumer
        # Needs 5 workers: task-receive(1) + send(1) + ack×2 + spare(1)
        executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix="rmq")

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

        bridge = MQStreamBridge(producer_adapter)
        active_tasks: set[asyncio.Task] = set()
        runner = AgentRunner(registry, bridge, checkpointer, config, task_registry=active_tasks)
        # v2 layering (§2.5/§2.6): Scheduler owns claim + dispatch; ingest only lands + pokes.
        scheduler = Scheduler(
            registry,
            runner,
            instance_id,
            max_concurrent_runs=consumer_cfg.max_concurrent_runs,
            task_registry=active_tasks,
        )
        task_consumer = TaskConsumer(
            registry,
            bridge,
            instance_id,
            scheduler=scheduler,
        )
        # Transactional outbox producer (§9.3): re-publishes undelivered terminal rows
        # (crash-before-publish, MQ outage, cancel-barrier cancelled) with at-least-once.
        outbox = OutboxProducer(registry, bridge)

        # 7. Build single RocketMQ consumer (all message types on one topic)
        def _make_filter(tag: str) -> FilterExpression:
            if tag:
                from rocketmq.grpc_protocol import FilterType
                return FilterExpression(f"TAGS = '{tag}'", FilterType.SQL)
            return FilterExpression("*")

        mq_task_consumer = SimpleConsumer(
            mq_client_config,
            consumer_cfg.consumer_group,
            subscription={consumer_cfg.task_topic: _make_filter(consumer_cfg.task_topic_tag)},
            await_duration=20,
        )
        mq_task_consumer.startup()
        logger.info(
            "RocketMQ consumer started (task_topic=%s group=%s)",
            consumer_cfg.task_topic,
            consumer_cfg.consumer_group,
        )

        # 8. Shutdown event + signal handlers
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop_event.set)

        # 9. Start background tasks (kept as named handles so shutdown can stop them in
        #    the draining-first order of §4.1/#4 — not all-at-once).
        heartbeat_task = asyncio.create_task(
            _instance_heartbeat_loop(registry, instance_id),
            name="instance-heartbeat",
        )
        watchdog_task = asyncio.create_task(
            _stale_run_watchdog(registry, scheduler, instance_id),
            name="stale-run-watchdog",
        )
        scheduler_task = asyncio.create_task(
            scheduler.run_loop(stop_event),
            name="scheduler",
        )
        poll_task = asyncio.create_task(
            _poll_loop(
                mq_task_consumer,
                task_consumer,
                executor,
                batch_size=consumer_cfg.poll_batch_size,
                invisible_duration=consumer_cfg.invisible_duration_seconds,
                stop_event=stop_event,
            ),
            name="poll-loop",
        )
        outbox_task = asyncio.create_task(
            outbox.run_loop(stop_event),
            name="outbox-producer",
        )
        # MLM extraction worker (G6): shares the AgentRunner's checkpointer so it can
        # read each thread's latest checkpoint for memory extraction. Maintenance class
        # → cancelled in the step-④ batch below.
        mlm_extraction_task = _start_mlm_extraction_loop(checkpointer, instance_id, stop_event)
        maintenance_tasks = [heartbeat_task, watchdog_task, outbox_task, mlm_extraction_task]
        if consumer_cfg.processed_messages_ttl_days > 0:
            maintenance_tasks.append(
                asyncio.create_task(
                    _processed_messages_cleanup(registry, consumer_cfg.processed_messages_ttl_days),
                    name="processed-messages-cleanup",
                )
            )

        logger.info(
            "Consumer %s ready — max_concurrent=%d task_batch=%d invisible=%ds",
            instance_id,
            consumer_cfg.max_concurrent_runs,
            consumer_cfg.poll_batch_size,
            consumer_cfg.invisible_duration_seconds,
        )

        # 10. Block until SIGTERM / SIGINT
        await stop_event.wait()
        logger.info("Shutdown signal received; entering draining-first shutdown (§4.1/#4)")

        # 11. Graceful shutdown — draining-first ordering (§4.1/#4):
        #   ① mark draining BEFORE draining so the instance status reflects reality early;
        #   ② stop accepting new work: poll-loop (ingest) + scheduler claim loop;
        #   ③ drain in-flight runs (bounded) — heartbeat stays alive so a peer watchdog
        #      does not false-positive our still-running runs as stale (§2.9);
        #   ④ final outbox flush, then stop maintenance loops, then close MQ/producer;
        #   ⑤ delete the instance row last.

        # ① draining marker (proactive, before any teardown)
        await registry.mark_instance_draining(instance_id)

        # ② stop accepting new work (both already observe stop_event; cancel for promptness)
        poll_task.cancel()
        scheduler_task.cancel()
        await asyncio.gather(poll_task, scheduler_task, return_exceptions=True)

        # ③ drain in-flight runs (heartbeat + outbox still running)
        await scheduler.drain_tasks(timeout=30.0)

        # ④ flush any results the drained runs could not publish inline, then stop the
        #    maintenance loops while the producer is still up.
        try:
            await outbox.drain_once()
        except Exception:
            logger.debug("final outbox flush failed", exc_info=True)
        for task in maintenance_tasks:
            task.cancel()
        await asyncio.gather(*maintenance_tasks, return_exceptions=True)

        mq_task_consumer.shutdown()
        logger.info("RocketMQ consumer shut down")
        mq_producer.shutdown()
        logger.info("RocketMQ producer shut down")
        executor.shutdown(wait=False)

        # ⑤ delete the instance row last (residual running rows, if any, are recovered by
        #    a peer watchdog via ~instance_alive, §8).
        await registry.delete_instance(instance_id)

    await close_engine()
    logger.info("Consumer %s shutdown complete", instance_id)


if __name__ == "__main__":
    asyncio.run(main())
