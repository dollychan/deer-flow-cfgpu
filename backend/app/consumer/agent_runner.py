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

from app.consumer.constants import ProcessedStatus
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
    ) -> None:
        self._registry = registry
        self._bridge = bridge
        self._checkpointer = checkpointer
        self._app_config = app_config

    # ── Public entry point ────────────────────────────────────────────────────

    async def run(self, message: TaskMessage) -> None:
        """Execute a task message end-to-end.

        Designed to be fired as asyncio.create_task() — does not raise.
        All exceptions are caught, logged, and published as MQ error messages.
        """
        thread_id = message.thread_id
        run_id = message.message_id
        runner_task = asyncio.current_task()

        self._bridge.register_run(run_id, thread_id, message.reply_config)

        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(thread_id, interval=10),
            name=f"heartbeat-{run_id[:8]}",
        )
        cancel_watcher_task = asyncio.create_task(
            self._cancel_watcher(thread_id, runner_task, poll_interval=2),
            name=f"cancel-watcher-{run_id[:8]}",
        )

        processed_status = ProcessedStatus.COMPLETED
        try:
            coro = self._execute(message, run_id)
            if message.timeout_seconds:
                is_paused = await asyncio.wait_for(coro, timeout=message.timeout_seconds)
            else:
                is_paused = await coro

            if is_paused:
                processed_status = ProcessedStatus.PAUSED_FOR_APPROVAL

        except asyncio.CancelledError:
            processed_status = ProcessedStatus.CANCELLED
            await self._bridge.publish_result(
                run_id,
                status=ProcessedStatus.CANCELLED,
                thread_id=thread_id,
                stream_events=message.reply_config.stream_events,
            )

        except (asyncio.TimeoutError, TimeoutError):
            processed_status = ProcessedStatus.FAILED
            await self._bridge.publish_error(
                run_id,
                "AGENT_TIMEOUT",
                thread_id=thread_id,
                retriable=True,
                message=f"Agent execution timed out after {message.timeout_seconds}s",
            )

        except Exception as exc:
            processed_status = ProcessedStatus.FAILED
            logger.exception("Run %s failed: %s", run_id, exc)
            await self._bridge.publish_error(
                run_id,
                "INTERNAL_ERROR",
                thread_id=thread_id,
                retriable=False,
                message=str(exc),
            )

        finally:
            cancel_watcher_task.cancel()
            heartbeat_task.cancel()
            self._bridge.unregister_run(run_id)
            await self._registry.mark_processed(run_id, thread_id, processed_status)
            await self._registry.reset_retry_count(thread_id)
            await self._drain_and_release(thread_id)

    # ── Core execution ────────────────────────────────────────────────────────

    async def _execute(self, message: TaskMessage, run_id: str) -> bool:
        """Run the agent graph; returns True when paused for HIL, False on success."""
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

        async for mode, chunk in agent.astream(
            stream_input,
            config=runnable_config,
            stream_mode=["messages", "values", "custom"],
        ):
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
            # Ensure tool_approval_required custom event reaches upstream.
            # The middleware normally publishes via stream_writer; if it was
            # dropped by LangGraph before astream() yielded it, re-publish here.
            for task in final_state.tasks or []:
                for intr in task.interrupts or []:
                    v = getattr(intr, "value", None)
                    if isinstance(v, dict) and v.get("type") == "tool_approval_required":
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
                thread_id=message.thread_id,
                stream_events=message.reply_config.stream_events,
            )
            # Thread goes idle — HIL interrupt stored in LangGraph checkpoint.
            # Resume message arrives as a new task and is claimed normally.
            return True

        # Normal success — include final_state when stream_events=False
        final_state_data = None
        if not message.reply_config.stream_events and final_state is not None:
            from deerflow.runtime.serialization import serialize_channel_values

            try:
                final_state_data = serialize_channel_values(final_state.values or {})
            except Exception:
                logger.debug("Run %s: could not serialize final state", run_id, exc_info=True)

        await self._bridge.publish_result(
            run_id,
            status="success",
            thread_id=message.thread_id,
            stream_events=message.reply_config.stream_events,
            final_state=final_state_data,
        )
        return False

    # ── Public error publishing ───────────────────────────────────────────────

    async def publish_fatal_error(self, message_id: str, thread_id: str, message: str) -> None:
        """Publish a non-retriable INTERNAL_ERROR for a run that cannot be recovered."""
        await self._bridge.publish_error(
            message_id,
            "INTERNAL_ERROR",
            thread_id=thread_id,
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
        # Mirror all configurable keys that _CONTEXT_CONFIGURABLE_KEYS defines in services.py,
        # so tools/middleware that read runtime.context see the same values as Gateway runs.
        for key in ("agent_name", "thinking_enabled", "is_plan_mode", "ask",
                    "web_search_enabled", "model_name", "subagent_enabled", "reasoning_effort"):
            if configurable.get(key) is not None:
                context[key] = configurable[key]
        # Per-request model preferences for cfgpu image/video tools
        if task_cfg.get("models"):
            context["models"] = task_cfg["models"]

        return RunnableConfig(configurable=configurable, context=context)

    # ── Drain-before-Idle ─────────────────────────────────────────────────────

    async def _drain_and_release(self, thread_id: str) -> None:
        """After a run ends, drive the next queued followup or mark thread idle.

        Triggered by run completion (not polling). Each followup run's own
        finally block calls _drain_and_release again, naturally chaining until
        the queue is empty.
        """
        pending = await self._registry.peek_inject_queue(thread_id, policy="followup")

        if not pending:
            await self._registry.mark_thread_idle(thread_id)
            return

        next_row = pending[0]
        next_task = TaskMessage.from_json(json.dumps(next_row.body))

        # Atomic: consume followup + advance thread run + write new current msg.
        # If the process crashes mid-transition without this, the followup is lost.
        await self._registry.transition_thread_followup(
            thread_id, next_row.id, next_row.message_id, next_row.body
        )

        logger.info("Thread %s: draining followup message_id=%s", thread_id, next_row.message_id)
        asyncio.create_task(
            self.run(next_task),
            name=f"followup-{next_row.message_id[:8]}",
        )

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
        runner_task: asyncio.Task,
        poll_interval: int = 2,
    ) -> None:
        """Poll thread_cancel_signals; inject CancelledError into runner_task on match."""
        while True:
            await asyncio.sleep(poll_interval)
            try:
                if await self._registry.has_cancel_signal(thread_id):
                    await self._registry.clear_cancel_signal(thread_id)
                    logger.info("Cancel signal detected for thread %s — cancelling task", thread_id)
                    runner_task.cancel()
                    return
            except Exception:
                logger.debug("Cancel watcher error for thread %s", thread_id, exc_info=True)


# ── Module-level helpers ──────────────────────────────────────────────────────


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


