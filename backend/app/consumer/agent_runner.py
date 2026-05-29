"""AgentRunner — drives LangGraph execution and publishes events to MQ.

Replaces deerflow's run_agent / worker.py in Consumer mode:
  - Input  : TaskMessage from RocketMQ (not an HTTP RunRecord)
  - Output : MQStreamBridge → $AGENT_RESULTS topic
  - Cancel : DB-polled cancel watcher (same asyncio.Task.cancel() mechanism)
  - Drain  : _drain_and_release chains followup runs without polling
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from langchain_core.runnables import RunnableConfig

from deerflow.config.app_config import AppConfig
from deerflow.runtime.serialization import serialize

from app.consumer.constants import ProcessedStatus, QueuePolicy
from app.consumer.run_registry import RunRegistry
from app.consumer.schemas import ContentItem, TaskMessage, UserMessage
from app.consumer.stream_bridge.mq import MQStreamBridge

logger = logging.getLogger(__name__)


class AgentRunner:
    """Executes one agent run per TaskMessage, publishing events via MQStreamBridge.

    One instance is shared per Consumer process; concurrent runs are separate
    asyncio Tasks, each with their own local state.
    """

    def __init__(
        self,
        registry: RunRegistry,
        bridge: MQStreamBridge,
        checkpointer: Any,
        app_config: AppConfig,
        task_registry: set[asyncio.Task] | None = None,
    ) -> None:
        self._registry = registry
        self._bridge = bridge
        self._checkpointer = checkpointer
        self._app_config = app_config
        self._task_registry = task_registry  # shared with TaskConsumer._active_tasks

    # ── Public entry point ────────────────────────────────────────────────────

    async def run(self, message: TaskMessage) -> None:
        """Execute a task message end-to-end.

        Designed to be fired as asyncio.create_task() — does not raise.
        All exceptions are caught, logged, and published as MQ error messages.
        """
        thread_id = message.thread_id
        run_id = message.message_id
        current_task_seq = message.thread_msg_seq
        runner_task = asyncio.current_task()

        self._bridge.register_run(run_id, message.reply_config, echo=message.downlink_echo())

        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(thread_id, interval=10),
            name=f"heartbeat-{run_id[:8]}",
        )
        cancel_watcher_task = asyncio.create_task(
            self._cancel_watcher(thread_id, current_task_seq, runner_task, poll_interval=2),
            name=f"cancel-watcher-{run_id[:8]}",
        )

        processed_status = ProcessedStatus.COMPLETED
        result_cache: dict | None = None
        try:
            coro = self._execute(message, run_id)
            if message.timeout_seconds:
                is_paused, result_cache = await asyncio.wait_for(coro, timeout=message.timeout_seconds)
            else:
                is_paused, result_cache = await coro

            if is_paused:
                processed_status = ProcessedStatus.PAUSED_FOR_APPROVAL

        except asyncio.CancelledError:
            processed_status = ProcessedStatus.CANCELLED
            # cancel barrier: convert any pending followups before this cancel to prefix,
            # notify upstream, then delete the cancel row
            cancel_row = await self._registry.find_cancel_after_seq(thread_id, current_task_seq)
            if cancel_row:
                followup_before = await self._registry.get_followup_before_seq(
                    thread_id, cancel_row.thread_msg_seq
                )
                if followup_before:
                    await self._registry.convert_to_prefix(
                        thread_id, [r.id for r in followup_before]
                    )
                    for row in followup_before:
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
            await self._bridge.publish_result(
                run_id,
                status=ProcessedStatus.CANCELLED,
                stream_events=message.reply_config.stream_events,
            )
            result_cache = {"status": ProcessedStatus.CANCELLED}

        except (asyncio.TimeoutError, TimeoutError):
            processed_status = ProcessedStatus.FAILED
            timeout_msg = f"Agent execution timed out after {message.timeout_seconds}s"
            await self._bridge.publish_error(
                "AGENT_TIMEOUT",
                echo=message.downlink_echo(),
                retriable=True,
                message=timeout_msg,
            )
            result_cache = {"error": {"code": "AGENT_TIMEOUT", "retriable": True, "message": timeout_msg}}

        except Exception as exc:
            processed_status = ProcessedStatus.FAILED
            logger.exception("Run %s failed: %s", run_id, exc)
            error_msg = str(exc)
            await self._bridge.publish_error(
                "INTERNAL_ERROR",
                echo=message.downlink_echo(),
                retriable=False,
                message=error_msg,
            )
            result_cache = {"error": {"code": "INTERNAL_ERROR", "retriable": False, "message": error_msg}}

        finally:
            cancel_watcher_task.cancel()
            heartbeat_task.cancel()
            self._bridge.unregister_run(run_id)
            await self._registry.mark_processed(run_id, thread_id, processed_status, result_cache)
            await self._registry.reset_retry_count(thread_id)
            await self._drain_and_release(thread_id)

    # ── Core execution ────────────────────────────────────────────────────────

    async def _execute(self, message: TaskMessage, run_id: str) -> tuple[bool, dict]:
        """Run the agent graph; returns (is_paused, result_payload) where result_payload
        mirrors the payload sent via publish_result for use as result_cache."""
        # Auto-correct missing ask=True on HIL resume messages
        if message.is_resume and not message.config.get("ask"):
            await self._bridge.publish(
                run_id,
                "custom",
                {
                    "type": "warning",
                    "code": "HIL_ASK_REQUIRED",
                    "message": "HIL resume 消息必须设置 config.ask=true；本次已自动修正，请检查客户端实现",
                },
            )

        runnable_config = self._build_config(message, run_id)
        agent = self._build_graph(runnable_config)

        if message.is_resume:
            from langgraph.types import Command as LGCommand

            stream_input: Any = LGCommand(**message.command)
            logger.info("Run %s: HIL resume (command keys: %s)", run_id, list(message.command.keys()))
        else:
            stream_input = _normalize_messages(message.messages or [])

        # Use client-requested event types as stream_mode so LangGraph only
        # generates events that will actually be forwarded. When stream_events
        # is disabled, use "values" as a minimal mode to drive graph execution
        # without emitting any published events.
        rc = message.reply_config
        stream_mode: list[str] = (
            rc.stream_event_types if rc.stream_events and rc.stream_event_types else ["values"]
        )
        async for mode, chunk in agent.astream(
            stream_input,
            config=runnable_config,
            stream_mode=stream_mode,
        ):
            if mode == "messages" and _is_empty_message_chunk(chunk):
                continue
            await self._bridge.publish(run_id, mode, serialize(chunk, mode=mode))

        is_paused = False
        try:
            final_state = await agent.aget_state(runnable_config)
            if final_state and any(t.interrupts for t in (final_state.tasks or [])):
                is_paused = True
        except Exception:
            logger.debug("Run %s: could not inspect final state", run_id, exc_info=True)
            final_state = None

        if is_paused:
            # Collect tool_approval_required payload for result_cache so that
            # duplicate deliveries can replay the approval prompt to the client.
            tool_approval_payload: dict | None = None
            for task in final_state.tasks or []:
                for intr in task.interrupts or []:
                    v = getattr(intr, "value", None)
                    if isinstance(v, dict) and v.get("type") == "tool_approval_required":
                        tool_approval_payload = v
                        # Ensure the event reaches upstream if the middleware
                        # stream_writer dropped it before astream() yielded it.
                        if not v.get("sse_emitted"):
                            logger.info(
                                "Run %s: re-publishing tool_approval_required (%d calls)",
                                run_id,
                                len(v.get("tool_calls", [])),
                            )
                            await self._bridge.publish(run_id, "custom", v)

            await self._bridge.publish_result(
                run_id,
                status=ProcessedStatus.PAUSED_FOR_APPROVAL,
                stream_events=message.reply_config.stream_events,
            )
            # Thread goes idle — HIL interrupt stored in LangGraph checkpoint.
            # Resume message arrives as a new task and is claimed normally.
            hil_cache: dict = {"status": ProcessedStatus.PAUSED_FOR_APPROVAL}
            if tool_approval_payload is not None:
                hil_cache["tool_approval_required"] = tool_approval_payload
            return True, hil_cache

        rc = message.reply_config

        # Only serialize final_state for non-streaming clients; streaming clients
        # already have all content via custom events and don't need the state dump.
        final_state_data = None
        if final_state is not None and not rc.stream_events:
            from deerflow.runtime.serialization import serialize_channel_values

            try:
                final_state_data = serialize_channel_values(dict(final_state.values or {}))
            except Exception:
                logger.debug("Run %s: could not serialize final state", run_id, exc_info=True)

        await self._bridge.publish_result(
            run_id,
            status="success",
            stream_events=rc.stream_events,
            final_state=final_state_data,
        )

        result_payload: dict = {"status": "success", "stream_events": rc.stream_events}
        if rc.stream_events:
            # Store buffered custom events so duplicate deliveries can replay the stream.
            result_payload["events"] = self._bridge.get_buffered_events(run_id)
        elif final_state_data is not None:
            result_payload["final_state"] = final_state_data
        return False, result_payload

    # ── Public error publishing ───────────────────────────────────────────────

    async def publish_fatal_error(self, message_id: str, thread_id: str, message: str) -> None:
        """Publish a non-retriable INTERNAL_ERROR for a run that cannot be recovered."""
        await self._bridge.publish_error(
            "INTERNAL_ERROR",
            echo={"message_id": message_id, "thread_id": thread_id},
            retriable=False,
            message=message,
        )

    # ── Graph construction ────────────────────────────────────────────────────

    def _build_graph(self, runnable_config: RunnableConfig) -> Any:
        """Instantiate the lead-agent compiled graph with checkpointer attached."""
        from deerflow.agents.lead_agent.agent import make_lead_agent

        agent = make_lead_agent(config=runnable_config)
        if self._checkpointer is not None:
            agent.checkpointer = self._checkpointer
        return agent

    def _build_config(self, message: TaskMessage, run_id: str) -> RunnableConfig:
        """Build the LangGraph RunnableConfig from a TaskMessage."""
        task_cfg = message.config

        # agent_name="lead_agent" (default) is treated as no custom agent
        agent_name = message.agent_name if message.agent_name != "lead_agent" else None

        thinking_enabled = task_cfg.get("thinking_enabled", True)
        configurable: dict[str, Any] = {
            "thread_id": message.thread_id,
            "run_id": run_id,
            "agent_name": agent_name,
            "thinking_enabled": thinking_enabled,
            # MQ protocol uses thinking_enabled as the plan-mode switch
            "is_plan_mode": thinking_enabled,
            "ask": task_cfg.get("ask", False),
            "app_config": self._app_config,
            # Controls whether web-group tools (web_search, web_fetch) are loaded
            "web_search_enabled": task_cfg.get("web_search_enabled", True),
        }

        # Optional pass-through fields
        if task_cfg.get("model_name"):
            configurable["model_name"] = task_cfg["model_name"]
        # subagent_enabled and reasoning_effort have no config.yaml global default;
        # pass through only when the MQ message explicitly sets them.
        if task_cfg.get("subagent_enabled") is not None:
            configurable["subagent_enabled"] = task_cfg["subagent_enabled"]
        if task_cfg.get("reasoning_effort") is not None:
            configurable["reasoning_effort"] = task_cfg["reasoning_effort"]

        # HIL resume: ensure ask=True so approval interrupts are active
        if message.is_resume:
            configurable["ask"] = True

        # Build runtime context — mirrors what worker.py's _build_runtime_context does for the
        # Gateway path. Consumer bypasses worker.py entirely, so we must populate config["context"]
        # ourselves. Without this, runtime.context is None/empty for all middleware and tool calls:
        #   - sandbox_middleware.py  reads thread_id → per-thread sandbox
        #   - memory_middleware.py   reads thread_id → correct memory association
        #   - thread_data_middleware reads thread_id + run_id
        #   - present_file_tool      reads thread_id
        #   - resolve_runtime_user_id reads user_id  → per-user file isolation
        #   - setup/update_agent     reads agent_name, user_id
        context: dict[str, Any] = {
            # Always required — infrastructure keys
            "thread_id":  message.thread_id,
            "run_id":     run_id,
            "app_config": self._app_config,
        }
        # Per-user isolation (memory, sandbox path, custom agents)
        if message.user_id:
            context["user_id"] = message.user_id
        if message.project_id:
            context["project_id"] = message.project_id
        # Mirror all configurable keys that _CONTEXT_CONFIGURABLE_KEYS defines in services.py,
        # so tools/middleware that read runtime.context see the same values as Gateway runs.
        for key in ("agent_name", "thinking_enabled", "is_plan_mode", "ask",
                    "web_search_enabled", "model_name", "subagent_enabled", "reasoning_effort"):
            if configurable.get(key) is not None:
                context[key] = configurable[key]
        # Per-request model preferences for cfgpu image/video tools
        if task_cfg.get("models"):
            context["models"] = task_cfg["models"]

        # Inject Runtime so middleware/tools see runtime.context (reads from
        # configurable["__pregel_runtime"], NOT from RunnableConfig(context=...)).
        from langgraph.runtime import Runtime

        configurable["__pregel_runtime"] = Runtime(context=context)

        return RunnableConfig(configurable=configurable)

    # ── Drain-before-Idle ─────────────────────────────────────────────────────

    async def _drain_and_release(self, thread_id: str) -> None:
        """After a run ends, drive the next queued followup or mark thread idle.

        Processes cancel barriers before dispatching: followup rows before a cancel
        are converted to prefix (preserving LLM context) and upstream is notified.
        Naturally chains via each followup run's own finally block.
        """
        pending = await self._registry.peek_thread_queue(
            thread_id,
            policies=(QueuePolicy.FOLLOWUP, QueuePolicy.CANCEL, QueuePolicy.PREFIX),
        )

        # ── cancel barrier ────────────────────────────────────────────────────
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
            await self._drain_and_release(thread_id)
            return

        # ── normal drain ──────────────────────────────────────────────────────
        followup_rows = [r for r in pending if r.policy == QueuePolicy.FOLLOWUP]
        prefix_rows   = [r for r in pending if r.policy == QueuePolicy.PREFIX]

        if not followup_rows:
            await self._registry.mark_thread_idle(thread_id)
            return

        drain_mode = await self._registry.get_drain_mode(thread_id)

        if drain_mode != "followup":
            # collect mode — not yet implemented; fall back to followup
            logger.info("Thread %s: collect drain_mode not implemented; using followup", thread_id)

        next_row = followup_rows[0]
        next_task = TaskMessage.from_json(json.dumps(next_row.body))

        if prefix_rows and next_task.messages is not None:
            prefix_messages: list[UserMessage] = []
            for row in prefix_rows:
                try:
                    prefix_msg = TaskMessage.from_json(json.dumps(row.body))
                    if prefix_msg.messages:
                        prefix_messages.extend(prefix_msg.messages)
                except Exception:
                    logger.warning("Prefix row %s parse failed; skipping", row.id)
            if prefix_messages:
                next_task.messages = prefix_messages + next_task.messages

        await self._registry.transition_thread_followup(
            thread_id,
            next_row.id,
            next_row.message_id,
            next_row.body,
            next_row.thread_msg_seq,
            prefix_ids=[r.id for r in prefix_rows],
        )

        logger.info("Thread %s: draining followup message_id=%s", thread_id, next_row.message_id)
        task = asyncio.create_task(
            self.run(next_task),
            name=f"followup-{next_row.message_id[:8]}",
        )
        if self._task_registry is not None:
            self._task_registry.add(task)
            task.add_done_callback(self._task_registry.discard)

    async def trigger_drain(self, thread_id: str) -> None:
        """Trigger drain from outside a run context (e.g., stale-run-watchdog)."""
        await self._drain_and_release(thread_id)

    # ── Background coroutines ─────────────────────────────────────────────────

    async def _heartbeat_loop(self, thread_id: str, interval: int = 10) -> None:
        while True:
            await asyncio.sleep(interval)
            try:
                await self._registry.heartbeat_thread(thread_id)
            except Exception:
                logger.debug("Heartbeat update failed for thread %s", thread_id, exc_info=True)

    async def _cancel_watcher(
        self,
        thread_id: str,
        current_task_seq: int,
        runner_task: asyncio.Task,
        poll_interval: int = 2,
    ) -> None:
        """Poll thread_msg_queue for cancel rows with seq > current_task_seq."""
        while True:
            await asyncio.sleep(poll_interval)
            try:
                cancel_row = await self._registry.find_cancel_after_seq(
                    thread_id, current_task_seq
                )
                if cancel_row:
                    logger.info(
                        "Cancel signal detected for thread %s seq=%d — cancelling task",
                        thread_id,
                        cancel_row.thread_msg_seq,
                    )
                    runner_task.cancel()
                    return  # cancel row preserved; except CancelledError handles cleanup
            except Exception:
                logger.debug("Cancel watcher error for thread %s", thread_id, exc_info=True)


# ── Module-level helpers ──────────────────────────────────────────────────────


def _is_empty_message_chunk(chunk: Any) -> bool:
    """Return True if a messages-mode chunk carries no meaningful content.

    LangGraph messages mode yields (AIMessageChunk, metadata) tuples. Chunks
    with empty content and no tool call data are start/end bookkeeping events
    that add no value to the upstream consumer.
    """
    msg = chunk[0] if isinstance(chunk, tuple) else chunk
    content = getattr(msg, "content", None)
    if content:
        return False
    if getattr(msg, "tool_call_chunks", None):
        return False
    if getattr(msg, "tool_calls", None):
        return False
    return True


def _normalize_messages(messages: list[UserMessage]) -> dict[str, Any]:
    """Convert MQ UserMessage list to a LangGraph graph_input dict."""
    from langchain_core.messages import HumanMessage

    lc_messages = []
    for msg in messages:
        if isinstance(msg.content, str):
            lc_messages.append(HumanMessage(content=msg.content))
        else:
            blocks: list[dict] = []
            for item in msg.content:
                _append_content_block(blocks, item)
            if blocks:
                lc_messages.append(HumanMessage(content=blocks))

    return {"messages": lc_messages}


def _append_content_block(blocks: list[dict], item: ContentItem) -> None:
    """Append a single LangChain content block for a ContentItem."""
    if item.type == "text" and item.text:
        blocks.append({"type": "text", "text": item.text})
    elif item.type == "image_url" and item.url:
        blocks.append({"type": "image_url", "image_url": {"url": item.url[0]}})
    elif item.type in ("document_url", "audio_url", "video_url") and item.url:
        # Non-image URL types: pass through as text so the agent sees the URL
        blocks.append({"type": "text", "text": f"[{item.type}: {item.url[0]}]"})
    elif item.text:
        blocks.append({"type": "text", "text": item.text})


