"""UninterruptibleToolMiddleware — shield non-cancellable tool calls (BUG-009).

cfdream generate has no remote cancel API: a task that starts always produces a billed
result. A hard ``runner_task.cancel()`` landing mid-poll therefore orphans that remote
task — its ``task_id`` never reaches a checkpoint, so the consumer cannot reclaim what
it paid for (``cfgpu-docs/cancel.md`` §1).

This middleware hooks ``awrap_tool_call`` — the only LangGraph layer with a per-tool
execution boundary — and, for tool names matching ``non_interruptible_tools`` (fnmatch,
e.g. ``cfdream_generate_*``), runs the inner handler on its own task behind an
``asyncio.shield``. When a cancel lands while the tool is in flight it is **swallowed**:
the tool is allowed to drain to completion, then one of two stop paths runs.

  - cooperative carrier installed (consumer run): set the task-local cancel Event and
    ``uncancel()`` — the run stops cleanly at the next super-step boundary, *after*
    this tool's ToolMessage has been checkpointed. The result (already emitted by the
    inner MessageStreamMiddleware) is returned so it is written to the checkpoint.
  - no carrier installed (standalone / other runners): ``uncancel()`` then re-raise so
    the cancel is honored rather than silently dropped. The tool still ran to
    completion (no orphan) and its result was already emitted downstream.

Non-matching tools (bash, LLM, web_search, …) pass straight through, keeping their
default hard-cancel behavior — they stop immediately at their current ``await``.

Onion placement
---------------
``awrap_tool_call`` composes as an onion where the earlier-registered middleware is the
**outer** layer. This middleware must sit **outside** ``MessageStreamMiddleware`` so its
``handler(request)`` *contains* the ``tool_result`` emit: the client receives the
image/video before the run stops at the boundary (zero waste). It is registered adjacent
to (and after) ``HumanApprovalMiddleware`` — see ``lead_agent/agent.py`` and
``cfgpu-docs/cancel.md`` §4.2.
"""

from __future__ import annotations

import asyncio
import fnmatch
import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import override

from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import ToolMessage
from langgraph.prebuilt.tool_node import ToolCallRequest
from langgraph.types import Command

from deerflow.runtime.cancel_signal import (
    enter_protected_tool,
    exit_protected_tool,
    signal_cooperative_cancel,
)

logger = logging.getLogger(__name__)


class UninterruptibleToolMiddleware(AgentMiddleware):
    """Shield ``non_interruptible_tools`` so a mid-flight cancel does not orphan them.

    Args:
        patterns: fnmatch tool-name patterns whose in-flight calls must drain to
            completion before the run stops (e.g. ``["cfdream_generate_*"]``). An empty
            or ``None`` list protects nothing (every tool stays hard-cancellable).
    """

    def __init__(self, patterns: Sequence[str] | None) -> None:
        self._patterns: tuple[str, ...] = tuple(patterns or ())

    def _protected(self, request: ToolCallRequest) -> bool:
        name = request.tool_call["name"]
        return any(fnmatch.fnmatch(name, pat) for pat in self._patterns)

    @override
    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
    ) -> ToolMessage | Command:
        if not self._protected(request):
            # bash / LLM / other tools: hard cancel passes straight through.
            return await handler(request)
        # Mark this protected tool in flight so the cancel-watcher withholds the hard
        # cancel while it drains (conditional hard cancel, cancel.md §4.3) — otherwise
        # runner_task.cancel() tears down astream and discards this tool's already-
        # emitted tool_result. enter/exit bracket the whole shielded execution.
        enter_protected_tool()
        try:
            return await self._run_uninterruptible(request, handler)
        finally:
            exit_protected_tool()

    async def _run_uninterruptible(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
    ) -> ToolMessage | Command:
        name = request.tool_call["name"]
        # Run the inner onion (incl. MessageStream emit) + real tool on its own task so
        # the cancel cannot reach into the tool coroutine; shield protects the await, not
        # the task. ensure_future is required — a bare ``await handler(...)`` would let the
        # cancel land inside the tool and shield could not help.
        inner: asyncio.Task = asyncio.ensure_future(handler(request))
        swallowed = False
        while True:
            try:
                result = await asyncio.shield(inner)
                break
            except asyncio.CancelledError:
                if inner.done():
                    # Tool already finished — nothing left to protect; honor the cancel.
                    raise
                swallowed = True
                logger.info(
                    "UninterruptibleTool: swallowed cancel while %s in flight; draining",
                    name,
                )

        if swallowed:
            # The swallowed cancel must be cleared either way before we continue past
            # this tool (otherwise the next await would re-raise it).
            current = asyncio.current_task()
            if current is not None:
                current.uncancel()
            if signal_cooperative_cancel():
                # Cooperative carrier present: stop at the next super-step boundary,
                # after this ToolMessage is checkpointed.
                logger.info(
                    "UninterruptibleTool: %s drained after cancel; cooperative stop signalled",
                    name,
                )
            else:
                # No carrier: honor the cancel so it is not silently dropped. The tool
                # still completed (no orphan) and its result was already emitted inner.
                logger.info(
                    "UninterruptibleTool: %s drained after cancel; no carrier, re-raising",
                    name,
                )
                raise asyncio.CancelledError()
        return result

    @override
    def wrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], ToolMessage | Command],
    ) -> ToolMessage | Command:
        # cfdream MCP tools are async, so protected tools never take the sync path. Pass
        # through so any sync tool keeps its default (hard-cancellable) behavior.
        return handler(request)
