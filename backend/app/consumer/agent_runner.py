"""AgentRunner — drives LangGraph execution and publishes events to MQ (v2, design §7).

Replaces deerflow's run_agent / worker.py in Consumer mode:
  - Input  : ClaimedRun from the Scheduler (not an HTTP RunRecord, not a raw TaskMessage)
  - Output : MQStreamBridge → $AGENT_RESULTS topic (progress best-effort)
  - Terminal: finalize_run / finalize_paused close out the whole batch (§7.3/§6.5)
  - Cancel : cancel_watcher reads thread_run_state.cancel_watermark (§7.1)
  - Fork   : fork_init copies the parent checkpoint before building the graph (§7.4)
  - Drain  : reject-resume a cancel-orphaned interrupt to a clean terminal (§6.5)

The AgentRunner never claims and never folds watermarks — the Scheduler claims, the
ingest layer folds. It only executes one ClaimedRun end-to-end.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from langchain_core.runnables import RunnableConfig

from app.consumer.constants import ProcessedStatus, QueuePolicy
from app.consumer.run_registry import ClaimedRun, RunRegistry
from app.consumer.schemas import ContentItem, TaskMessage, UserMessage
from app.consumer.stream_bridge.mq import MQStreamBridge
from deerflow.config.app_config import AppConfig
from deerflow.runtime.cancel_signal import CancelState, get_cancel_state, install_cancel_event, reset_cancel_event
from deerflow.runtime.serialization import serialize
from deerflow.runtime.user_context import reset_current_user, set_current_user

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _ConsumerUser:
    """Minimal ``CurrentUser`` (the Protocol needs only ``.id``) for consumer runs.

    The consumer has no auth middleware, so ``_current_user`` is never set and
    ``get_effective_user_id()`` would resolve to ``"default"`` — splitting the
    in-graph user bucket from ``resolve_runtime_user_id`` (which reads the real
    ``user_id`` off ``runtime.context``). ``AgentRunner.run`` sets this for the
    duration of the run so sandbox/uploads/acp/present all resolve to the same
    real bucket (see cfgpu-docs/bugs-todo.md BUG-008).
    """

    id: str

# LangGraph's RESUME pending-write channel (== langgraph.constants.RESUME). Hardcoded to
# the stable channel name so fork_init does not import the now-private constant (§7.4).
_RESUME_CHANNEL = "__resume__"


class _LLMFallbackError(Exception):
    """Raised when a run terminates via an LLMErrorHandlingMiddleware fallback message
    (provider failure swallowed into a terminal AIMessage) instead of a real completion.

    Carries the MQ error-envelope fields so run() reports FAILED rather than a phantom
    "success" that delivers only an apology text and no artifact.
    """

    def __init__(self, code: str, *, retriable: bool, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.retriable = retriable


class AgentRunner:
    """Executes one ClaimedRun per call, publishing events via MQStreamBridge.

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
        self._task_registry = task_registry  # shared with the Scheduler's in-flight set

    # ── Public entry point ────────────────────────────────────────────────────

    async def run(self, claimed: ClaimedRun) -> None:
        """Execute a claimed run end-to-end (design §7).

        Designed to be fired as asyncio.create_task() — does not raise. All exceptions
        are caught, logged, published as MQ error messages, and closed out via finalize.
        """
        message = self._build_run_message(claimed)
        # Set the run's real user_id on the _current_user contextvar so the in-graph
        # stack (sandbox path mappings, uploads, acp, present_files) resolves
        # get_effective_user_id() to the right bucket instead of "default". The
        # consumer has no auth middleware to do this; runtime.context already carries
        # user_id for resolve_runtime_user_id, but the contextvar channel covers the
        # call sites that still use get_effective_user_id() (BUG-008). Task-local:
        # each run is its own asyncio task (scheduler.py), so concurrent runs for
        # different users do not cross-contaminate.
        user_token = set_current_user(_ConsumerUser(id=message.user_id)) if message.user_id else None
        try:
            await self._run(claimed, message)
        finally:
            if user_token is not None:
                reset_current_user(user_token)

    async def _run(self, claimed: ClaimedRun, message: TaskMessage) -> None:
        """Execute one claimed run under the established user context (see ``run``)."""
        # drain is internal housekeeping: no register_run, no downlink, no message_seq (§6.5).
        if claimed.policy == QueuePolicy.DRAIN.value:
            await self._run_drain(claimed, message)
            return

        thread_id = claimed.thread_id
        run_id = claimed.message_id
        seq = claimed.seq
        runner_task = asyncio.current_task()

        # Cooperative-cancel signal (BUG-009 / cancel.md §4.3). Shared with
        # UninterruptibleToolMiddleware — which swallows a hard cancel that lands on a
        # non-cancellable tool (e.g. cfgpu generate, no remote cancel API) and drains it
        # — and with the _execute astream loop, which stops at the next super-step
        # boundary once this is set. Installed on a task-local ContextVar so the in-graph
        # middleware reads the same Event: it runs in this run task, or in the wait_for
        # inner task (timeout path), which snapshots this context at creation. The watcher
        # gets the Event explicitly (it is a sibling task, not in this context).
        cancel_event = asyncio.Event()
        cancel_event_token = install_cancel_event(cancel_event)
        # The watcher is a sibling task that does not inherit later ContextVar
        # mutations, so hand it the live CancelState (event + protected_in_flight)
        # explicitly. install_cancel_event always returns a fresh state, so this is
        # never None here.
        cancel_state = get_cancel_state()

        self._bridge.register_run(run_id, message.reply_config, echo=message.downlink_echo())
        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(thread_id, interval=10),
            name=f"heartbeat-{run_id[:8]}",
        )
        cancel_watcher_task = asyncio.create_task(
            self._cancel_watcher(thread_id, seq, runner_task, cancel_state, poll_interval=2),
            name=f"cancel-watcher-{run_id[:8]}",
        )

        agent: Any = None
        runnable_config: RunnableConfig | None = None
        processed_status = ProcessedStatus.COMPLETED
        result_cache: dict | None = None
        # Whether the inline (best-effort fast-path) terminal envelope was published.
        # True ⟹ the run row can be marked delivered after finalize; False leaves it in
        # the outbox for the producer loop to retry (§2.8/§9.3, E1).
        downlink_sent = False
        try:
            # fork-init sentinel: copy the parent checkpoint before the graph runs (§7.4).
            if claimed.policy == QueuePolicy.FORK.value:
                await self._fork_init(message)

            runnable_config = self._build_config(message, run_id)
            agent = self._build_graph(runnable_config)

            coro = self._execute(message, run_id, agent, runnable_config, cancel_event)
            if message.timeout_seconds:
                is_paused, result_cache = await asyncio.wait_for(coro, timeout=message.timeout_seconds)
            else:
                is_paused, result_cache = await coro
            # _execute published the terminal envelope inline before returning (every
            # return path is preceded by publish_result); a publish failure would have
            # raised instead of returning, so a normal return ⟹ inline delivery ok.
            downlink_sent = True

            if is_paused:
                processed_status = ProcessedStatus.PAUSED_FOR_APPROVAL

        except asyncio.CancelledError:
            processed_status = ProcessedStatus.CANCELLED
            # cancelled runs still need a checkpoint_id (fork anchor, §7.4)
            checkpoint_id = await self._safe_checkpoint_id(agent, runnable_config)
            result_cache = {"status": ProcessedStatus.CANCELLED.value, "checkpoint_id": checkpoint_id}
            await self._bridge.publish_result(
                run_id,
                status=ProcessedStatus.CANCELLED,
                stream_events=message.reply_config.stream_events,
                checkpoint_id=checkpoint_id,
            )
            downlink_sent = True  # set only after a successful publish (else producer retries)

        except TimeoutError:
            processed_status = ProcessedStatus.FAILED
            timeout_msg = f"Agent execution timed out after {message.timeout_seconds}s"
            # error envelopes never carry checkpoint_id: fork anchors come only from
            # result (success / cancelled / paused_for_approval), §7.4 / prerequisites P0.2.
            result_cache = {
                "error": {"code": "AGENT_TIMEOUT", "retriable": True, "message": timeout_msg},
            }
            await self._bridge.publish_error(
                "AGENT_TIMEOUT",
                echo=message.downlink_echo(),
                retriable=True,
                message=timeout_msg,
            )
            downlink_sent = True

        except _LLMFallbackError as exc:
            processed_status = ProcessedStatus.FAILED
            logger.warning("Run %s: LLM provider fallback (%s, retriable=%s): %s", run_id, exc.code, exc.retriable, exc)
            error_msg = str(exc)
            result_cache = {
                "error": {"code": exc.code, "retriable": exc.retriable, "message": error_msg},
            }
            await self._bridge.publish_error(
                exc.code,
                echo=message.downlink_echo(),
                retriable=exc.retriable,
                message=error_msg,
            )
            downlink_sent = True

        except Exception as exc:
            processed_status = ProcessedStatus.FAILED
            logger.exception("Run %s failed: %s", run_id, exc)
            error_msg = str(exc)
            result_cache = {
                "error": {"code": "INTERNAL_ERROR", "retriable": False, "message": error_msg},
            }
            await self._bridge.publish_error(
                "INTERNAL_ERROR",
                echo=message.downlink_echo(),
                retriable=False,
                message=error_msg,
            )
            downlink_sent = True

        finally:
            reset_cancel_event(cancel_event_token)
            cancel_watcher_task.cancel()
            heartbeat_task.cancel()
            self._bridge.unregister_run(run_id)
            # Terminal close-out across the whole batch (§7.3/§6.5). finalize writes the
            # outbox row (delivered=false); the inline publish above is the best-effort
            # fast path. Carry the echo in result_cache so the outbox producer can rebuild
            # a faithful downlink envelope from the persisted row alone (§2.8, E1).
            if result_cache is not None:
                result_cache.setdefault("echo", message.downlink_echo())
            if processed_status == ProcessedStatus.PAUSED_FOR_APPROVAL:
                closed = await self._registry.finalize_paused(run_id, result_cache)
            else:
                closed = await self._registry.finalize_run(run_id, result_cache, str(processed_status))
            # Reconcile inline success → mark the outbox row delivered so the producer loop
            # does not re-send (at-least-once; a missed mark just double-sends, deduped).
            if closed and downlink_sent:
                await self._registry.mark_delivered(run_id)

    # ── Core execution ────────────────────────────────────────────────────────

    async def _execute(
        self,
        message: TaskMessage,
        run_id: str,
        agent: Any,
        runnable_config: RunnableConfig,
        cancel_event: asyncio.Event | None = None,
    ) -> tuple[bool, dict]:
        """Run the agent graph; returns (is_paused, result_cache).

        result_cache mirrors the payload sent via publish_result and always carries
        checkpoint_id for terminal states (fork anchor, §7.3/§7.4).
        """
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

        if message.is_resume:
            from langgraph.types import Command as LGCommand

            # Orphan resume (§6.5): resume landed on a thread with no pending interrupt
            # (run already finished / late resume). Discard — do not feed Command(resume=).
            if not await self._has_pending_interrupt(agent, runnable_config):
                logger.info("Run %s: orphan resume (no pending approval); discarding", run_id)
                checkpoint_id = await self._safe_checkpoint_id(agent, runnable_config)
                await self._bridge.publish_result(
                    run_id,
                    status="success",
                    stream_events=message.reply_config.stream_events,
                    checkpoint_id=checkpoint_id,
                )
                return False, {
                    "status": "success",
                    "stream_events": message.reply_config.stream_events,
                    "checkpoint_id": checkpoint_id,
                    "warning": "no pending approval to resume; resume discarded",
                }
            stream_input: Any = LGCommand(**message.command)
            logger.info("Run %s: HIL resume (command keys: %s)", run_id, list(message.command.keys()))
        else:
            stream_input = _normalize_messages(message.messages or [])

        rc = message.reply_config
        stream_mode: list[str] = (
            rc.stream_event_types if rc.stream_events and rc.stream_event_types else ["custom"]
        )
        # Add "updates" as an internal-only super-step boundary signal for cooperative
        # cancel (BUG-009 / cancel.md §4.3). It fires once per completed super-step — the
        # only point where no node is in flight and the just-committed step (incl. a
        # drained cfgpu ToolMessage) is already checkpointed (durability=sync). It is
        # never downlinked. Requires checkpoint durability=sync, else a resume re-runs the
        # node and re-submits cfgpu (double billing).
        effective_stream_mode = stream_mode if "updates" in stream_mode else [*stream_mode, "updates"]
        cooperative_cancel = False
        async for mode, chunk in agent.astream(
            stream_input,
            config=runnable_config,
            stream_mode=effective_stream_mode,
        ):
            if mode == "updates":
                # Super-step boundary. If a cancel was folded while a shielded tool was
                # draining, stop here — cleanly, after the tool's result is checkpointed
                # and (via inner MessageStreamMiddleware) already downlinked.
                if cancel_event is not None and cancel_event.is_set():
                    cooperative_cancel = True
                    break
                continue  # internal-only: never downlink "updates"
            if mode == "messages" and _is_empty_message_chunk(chunk):
                continue
            await self._bridge.publish(run_id, mode, serialize(chunk, mode=mode))

        if cooperative_cancel:
            # Translate the cooperative stop into the same terminal path as a hard cancel:
            # _run's `except asyncio.CancelledError` publishes CANCELLED with a checkpoint_id
            # (fork anchor) read from the last committed super-step. The shielded tool that
            # ran to completion is part of that checkpoint, so nothing is orphaned.
            logger.info("Run %s: cooperative cancel at super-step boundary (cancel.md §4.3)", run_id)
            raise asyncio.CancelledError()

        is_paused = False
        try:
            final_state = await agent.aget_state(runnable_config)
            if final_state and any(t.interrupts for t in (final_state.tasks or [])):
                is_paused = True
        except Exception:
            logger.debug("Run %s: could not inspect final state", run_id, exc_info=True)
            final_state = None

        checkpoint_id = _checkpoint_id_of(final_state)

        if is_paused:
            # Collect tool_approval_required payload for result_cache so duplicate
            # deliveries / the approval audit can replay the prompt (§6.5).
            tool_approval_payload: dict | None = None
            for task in final_state.tasks or []:
                for intr in task.interrupts or []:
                    v = getattr(intr, "value", None)
                    if isinstance(v, dict) and v.get("type") == "tool_approval_required":
                        tool_approval_payload = v
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
                checkpoint_id=checkpoint_id,
            )
            hil_cache: dict = {
                "status": ProcessedStatus.PAUSED_FOR_APPROVAL.value,
                "checkpoint_id": checkpoint_id,
            }
            if tool_approval_payload is not None:
                hil_cache["tool_approval_required"] = tool_approval_payload
            return True, hil_cache

        # LLM-failure fallback: LLMErrorHandlingMiddleware swallows provider errors into a
        # terminal AIMessage (no exception, no interrupt). Without this the run would be
        # reported as a phantom "success" carrying only an apology and no artifact. Inspect
        # ONLY this run's terminal message (the fallback has no tool_calls so it is always
        # last) — never rescan persisted history, unlike the Gateway worker.py path.
        fallback = _extract_fallback_error(final_state)
        if fallback is not None:
            code, retriable, fb_message = fallback
            raise _LLMFallbackError(code, retriable=retriable, message=fb_message)

        # Only serialize final_state for non-streaming clients; streaming clients
        # already have all content via custom events.
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
            checkpoint_id=checkpoint_id,
            final_state=final_state_data,
        )

        result_payload: dict = {
            "status": "success",
            "stream_events": rc.stream_events,
            "checkpoint_id": checkpoint_id,
        }
        if rc.stream_events:
            result_payload["events"] = self._bridge.get_buffered_events(run_id)
        elif final_state_data is not None:
            result_payload["final_state"] = final_state_data
        return False, result_payload

    # ── Drain branch (§6.5) ────────────────────────────────────────────────────

    async def _run_drain(self, claimed: ClaimedRun, message: TaskMessage) -> None:
        """Reject-resume a cancel-orphaned interrupt to a clean terminal (§6.5).

        Produces no downlink and does not advance message_seq. aget_state reads the
        interrupt's pending tool_call ids, then astream(Command(update={tool_approvals:
        all rejected})) replays HumanApprovalMiddleware's reject path with no LLM call,
        landing the checkpoint on a normal completed super-step. finalize_run's drain
        branch then only deletes the drain row and returns the thread to idle.

        Crash-idempotent: if the checkpoint already has no pending interrupt (already
        drained on a prior attempt), finalize still just deletes the row + idle.
        """
        thread_id = claimed.thread_id
        drain_id = claimed.message_id
        try:
            runnable_config = self._build_config(message, drain_id)
            agent = self._build_graph(runnable_config)
            pending_ids = await self._pending_approval_ids(agent, runnable_config)
            if pending_ids:
                from langgraph.types import Command as LGCommand

                approvals = {
                    tid: {"status": "rejected", "reason": "cancelled"} for tid in pending_ids
                }
                async for _mode, _chunk in agent.astream(
                    LGCommand(update={"tool_approvals": approvals}),
                    config=runnable_config,
                    stream_mode=["custom"],
                ):
                    pass
            else:
                logger.info("Drain %s: no pending interrupt; already drained", thread_id)
        except Exception:
            logger.exception("Drain reject-resume failed thread=%s; finalizing anyway", thread_id)
        await self._registry.finalize_run(drain_id, None, QueuePolicy.DRAIN.value)

    # ── Thread Fork (§7.4) ──────────────────────────────────────────────────────

    async def _fork_init(self, message: TaskMessage) -> None:
        """Copy the parent checkpoint onto this (new) thread before the graph runs (§7.4).

        Idempotent (I11): skips the copy when the new thread already has a checkpoint
        (MQ redelivery / stale rerun). Strips RESUME pending-writes but keeps __interrupt__
        so Command(resume=this branch's approvals) applies the branch decision freshly.
        """
        if self._checkpointer is None:
            return
        parent = message.parent_thread_id
        if not parent:
            return
        new_tid = message.thread_id
        new_cfg = {"configurable": {"thread_id": new_tid, "checkpoint_ns": ""}}

        existing = await self._checkpointer.aget_tuple(new_cfg)
        if existing is not None:
            logger.info("fork_init: thread=%s already has checkpoint; skipping copy (I11)", new_tid)
            return

        src_cfg: dict = {"configurable": {"thread_id": parent, "checkpoint_ns": ""}}
        if message.fork_checkpoint_id:
            src_cfg["configurable"]["checkpoint_id"] = message.fork_checkpoint_id
        src = await self._checkpointer.aget_tuple(src_cfg)
        if src is None:
            raise RuntimeError(
                f"fork parent checkpoint not found: {parent}@{message.fork_checkpoint_id}"
            )

        clean = [w for w in (src.pending_writes or []) if w[1] != _RESUME_CHANNEL]
        await self._checkpointer.aput(new_cfg, src.checkpoint, src.metadata, {})
        by_task: dict[str, list[tuple[str, Any]]] = defaultdict(list)
        for task_id, channel, value in clean:
            by_task[task_id].append((channel, value))
        for task_id, writes in by_task.items():
            await self._checkpointer.aput_writes(new_cfg, writes, task_id)

        if (message.fork or {}).get("copy_sandbox"):
            logger.info("fork_init: copy_sandbox requested but not yet implemented (deferred)")

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

        thinking_enabled = task_cfg.get("thinking_enabled", False)
        configurable: dict[str, Any] = {
            "thread_id": message.thread_id,
            "run_id": run_id,
            "agent_name": agent_name,
            "thinking_enabled": thinking_enabled,
            # MQ protocol uses thinking_enabled as the plan-mode switch
            "is_plan_mode": thinking_enabled,
            "ask": task_cfg.get("ask", True),
            "app_config": self._app_config,
            # Controls whether web-group tools (web_search, web_fetch) are loaded
            "web_search_enabled": task_cfg.get("web_search_enabled", True),
        }

        # Optional pass-through fields
        if task_cfg.get("model_name"):
            configurable["model_name"] = task_cfg["model_name"]
        if task_cfg.get("subagent_enabled") is not None:
            configurable["subagent_enabled"] = task_cfg["subagent_enabled"]
        if task_cfg.get("reasoning_effort") is not None:
            configurable["reasoning_effort"] = task_cfg["reasoning_effort"]

        # HIL resume / fork: ensure ask=True so approval interrupts are active
        if message.is_resume:
            configurable["ask"] = True

        context: dict[str, Any] = {
            "thread_id": message.thread_id,
            "run_id": run_id,
            "app_config": self._app_config,
        }
        if message.user_id:
            context["user_id"] = message.user_id
        if message.project_id:
            context["project_id"] = message.project_id
        for key in ("agent_name", "thinking_enabled", "is_plan_mode", "ask",
                    "web_search_enabled", "model_name", "subagent_enabled", "reasoning_effort"):
            if configurable.get(key) is not None:
                context[key] = configurable[key]
        # Consumed per-run by RuntimeConfigMiddleware: config.models constrains cfgpu generate
        # model selection (方案 3 whitelist); config.skills is eager-injected (Model B).
        if task_cfg.get("models"):
            context["models"] = task_cfg["models"]
        if task_cfg.get("skills"):
            context["skills"] = task_cfg["skills"]

        from langgraph.runtime import Runtime

        configurable["__pregel_runtime"] = Runtime(context=context)

        return RunnableConfig(configurable=configurable)

    # ── Run input reconstruction ────────────────────────────────────────────────

    def _build_run_message(self, claimed: ClaimedRun) -> TaskMessage:
        """Reconstruct the run's TaskMessage from a ClaimedRun's input bodies (§6.2.2/§6.4).

        input_bodies = prefix history (cancel-covered) first, then the collect batch by seq
        (candidate first). The candidate body is the run envelope template; for messages-based
        runs the merged input is the flattened messages of prefix + entire batch.
        """
        prefix_count = len(claimed.prefix_message_ids)
        bodies = claimed.input_bodies
        prefix_bodies = bodies[:prefix_count]
        batch_bodies = bodies[prefix_count:]
        candidate_body = batch_bodies[0] if batch_bodies else bodies[-1]
        message = TaskMessage.from_json(json.dumps(candidate_body))

        if message.messages is not None and (prefix_bodies or len(batch_bodies) > 1):
            merged: list[UserMessage] = []
            for body in prefix_bodies + batch_bodies:
                try:
                    m = TaskMessage.from_json(json.dumps(body))
                    if m.messages:
                        merged.extend(m.messages)
                except Exception:
                    logger.warning("Run %s: failed to parse merged body; skipping", claimed.message_id)
            if merged:
                message.messages = merged
        return message

    # ── Checkpoint helpers ──────────────────────────────────────────────────────

    async def _safe_checkpoint_id(self, agent: Any, runnable_config: RunnableConfig | None) -> str | None:
        """Best-effort aget_state → checkpoint_id; None if unavailable (e.g. pre-graph cancel)."""
        if agent is None or runnable_config is None:
            return None
        try:
            return _checkpoint_id_of(await agent.aget_state(runnable_config))
        except Exception:
            logger.debug("could not fetch checkpoint_id", exc_info=True)
            return None

    async def _has_pending_interrupt(self, agent: Any, runnable_config: RunnableConfig) -> bool:
        try:
            state = await agent.aget_state(runnable_config)
            return bool(state and any(t.interrupts for t in (state.tasks or [])))
        except Exception:
            logger.debug("could not inspect pending interrupt", exc_info=True)
            return False

    async def _pending_approval_ids(self, agent: Any, runnable_config: RunnableConfig) -> list[str]:
        """Read pending tool_approval_required tool_call ids from the interrupt checkpoint."""
        ids: list[str] = []
        try:
            state = await agent.aget_state(runnable_config)
        except Exception:
            logger.debug("drain: could not aget_state", exc_info=True)
            return ids
        for task in (state.tasks if state else []) or []:
            for intr in task.interrupts or []:
                v = getattr(intr, "value", None)
                if isinstance(v, dict) and v.get("type") == "tool_approval_required":
                    for tc in v.get("tool_calls", []):
                        tid = tc.get("id")
                        if tid:
                            ids.append(tid)
        return ids

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
        cancel_state: CancelState | None = None,
        poll_interval: int = 2,
    ) -> None:
        """Poll thread_run_state.cancel_watermark; cancel the run when it covers this seq (§7.1).

        Pure interrupter — touches no DB writes. cancel persistence already lives in the
        folded watermark, so the watcher just translates ``seq < watermark`` into a stop.

        Conditional hard cancel (BUG-009 / cancel.md §4.3). The cooperative ``event`` is
        set unconditionally; the hard ``runner_task.cancel()`` is issued **only when no
        protected tool is in flight**. A hard cancel tears down ``astream`` and discards a
        shielded tool's already-emitted ``tool_result`` (langgraph runs nodes in their own
        tasks, so the cancel never lands inside the shield — it just unwinds the stream).
        So:
          - protected tool (cfgpu) in flight → set the Event, do NOT hard-cancel; the run
            stops cooperatively at the next super-step boundary, after the result has been
            downlinked and checkpointed. Keep polling: once it drains (and if the run has
            not already stopped on the cooperative flag) a later iteration hard-cancels.
          - nothing protected in flight (LLM / bash / parked) → set the Event AND hard-cancel
            for an immediate interrupt, then return.
        """
        event_set = False
        while True:
            await asyncio.sleep(poll_interval)
            try:
                state = await self._registry.get_thread_state(thread_id)
                watermark = (state.cancel_watermark or 0) if state else 0
                if watermark <= current_task_seq:
                    continue
                if cancel_state is not None and not event_set:
                    logger.info(
                        "Cancel watermark=%d covers run seq=%d thread=%s — signalling cooperative stop",
                        watermark,
                        current_task_seq,
                        thread_id,
                    )
                    cancel_state.event.set()
                    event_set = True
                # Withhold the hard cancel while a shielded tool is draining so its
                # tool_result is not lost to the astream teardown.
                if cancel_state is not None and cancel_state.protected_in_flight > 0:
                    logger.info(
                        "Cancel: %d protected tool(s) draining thread=%s — deferring hard cancel",
                        cancel_state.protected_in_flight,
                        thread_id,
                    )
                    continue
                logger.info("Cancel: hard-cancelling run seq=%d thread=%s", current_task_seq, thread_id)
                runner_task.cancel()
                return
            except Exception:
                logger.debug("Cancel watcher error for thread %s", thread_id, exc_info=True)


# ── Module-level helpers ──────────────────────────────────────────────────────


def _checkpoint_id_of(state: Any) -> str | None:
    """Extract checkpoint_id from a StateSnapshot's config, or None."""
    if state is None:
        return None
    try:
        return (state.config or {}).get("configurable", {}).get("checkpoint_id")
    except Exception:
        return None


# Map LLMErrorHandlingMiddleware fallback reasons to the MQ error vocabulary
# (publish_error: AGENT_TIMEOUT | TOOL_FAILED | QUOTA_EXCEEDED | INTERNAL_ERROR |
# AGENT_BUSY | INVALID_SCHEMA). (code, retriable). Unknown reasons → INTERNAL_ERROR.
# NOTE: AGENT_BUSY is reserved by the protocol for ingest-time reject (thread busy +
# message_mode=reject), NOT a run-execution failure — so a provider being temporarily
# unavailable maps to INTERNAL_ERROR with retriable=True, conveying retry-worthiness
# via the flag rather than overloading AGENT_BUSY's meaning.
_FALLBACK_ERROR_CODES: dict[str, tuple[str, bool]] = {
    "quota": ("QUOTA_EXCEEDED", False),
    "transient": ("INTERNAL_ERROR", True),
    "busy": ("INTERNAL_ERROR", True),
    "circuit_open": ("INTERNAL_ERROR", True),
    "auth": ("INTERNAL_ERROR", False),
}


def _extract_fallback_error(state: Any) -> tuple[str, bool, str] | None:
    """Detect an LLMErrorHandlingMiddleware fallback as THIS run's terminal message.

    Returns (error_code, retriable, message) when the final message carries the
    ``deerflow_error_fallback`` marker, else None. Only the last message is inspected
    — the fallback has no tool_calls so it is always terminal — which avoids the Gateway
    worker.py pitfall of rescanning persisted history and mistaking an earlier turn's
    fallback for the current run's outcome.
    """
    if state is None:
        return None
    try:
        messages = (state.values or {}).get("messages")
    except Exception:
        return None
    if not messages:
        return None
    last = messages[-1]
    kwargs = getattr(last, "additional_kwargs", None)
    if not isinstance(kwargs, dict) and isinstance(last, dict):
        kwargs = last.get("additional_kwargs")
    if not isinstance(kwargs, dict) or not kwargs.get("deerflow_error_fallback"):
        return None
    reason = kwargs.get("error_reason") or "unknown"
    code, retriable = _FALLBACK_ERROR_CODES.get(reason, ("INTERNAL_ERROR", False))
    content = last.get("content") if isinstance(last, dict) else getattr(last, "content", None)
    message = content if isinstance(content, str) and content else (kwargs.get("error_detail") or "LLM provider failed")
    return code, retriable, message


def _is_empty_message_chunk(chunk: Any) -> bool:
    """Return True if a messages-mode chunk carries no meaningful content."""
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
        blocks.append({"type": "text", "text": f"[{item.type}: {item.url[0]}]"})
    elif item.text:
        blocks.append({"type": "text", "text": item.text})
