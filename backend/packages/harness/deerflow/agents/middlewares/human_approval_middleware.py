"""HumanApprovalMiddleware — batch HIL approval via after_model hook.

Flow (first call — interrupt path):
  1. after_model fires after LLM produces an AIMessage with tool_calls.
  2. Collect all tool calls whose name matches any configured fnmatch pattern.
  3. Check state.tool_approvals for existing decisions (none on first call).
  4. Emit a single "tool_approval_required" custom SSE event with all pending calls.
  5. Call interrupt() ONCE for the entire batch → graph checkpoints and pauses.

Flow (resume path — state-based, no duplicate SSE):
  6. Client POSTs a new run with:
       command: {
         update: {
           tool_approvals: {
             "<tool_call_id>": {"status": "approved", "args": {...}},
             "<tool_call_id>": {"status": "rejected", "reason": "..."}
           }
         }
       }
  7. LangGraph applies the state update BEFORE re-executing after_model.
  8. after_model detects all decisions already in state.tool_approvals → skips
     SSE and interrupt(), applies decisions directly.
  9. Returns modified AIMessage (approved calls with updated args, rejected calls
     removed) plus artificial ToolMessages for any rejected calls.

Client protocol summary:
  Subscribe to stream_mode including "custom" to receive approval events.

  Approval event:
    {
      "type": "tool_approval_required",
      "tool_calls": [{"id": "...", "name": "...", "args": {...}}, ...]
    }

  Resume command (primary — clean, no duplicate SSE):
    POST /threads/{id}/runs  or  POST /threads/{id}/runs/stream
    {
      "command": {
        "update": {
          "tool_approvals": {
            "<id>": {"status": "approved", "args": {...}},
            "<id>": {"status": "rejected", "reason": "Too expensive"}
          }
        }
      }
    }

  Fallback (resume value only — may emit duplicate SSE):
    {"command": {"resume": {"approved": [{"id": ..., "args": {...}}], "rejected": [...]}}}
"""

from __future__ import annotations

import fnmatch
import json
import logging
from typing import Any, override

from langchain.agents import AgentState
from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import AIMessage, ToolMessage
from langgraph.config import get_stream_writer
from langgraph.types import interrupt

logger = logging.getLogger(__name__)

_APPROVAL_SSE_TYPE = "tool_approval_required"


class HumanApprovalMiddleware(AgentMiddleware[AgentState]):
    """Pause and request human confirmation before executing high-cost tools.

    Intercepts at after_model so the entire batch of tool calls from one LLM
    response is presented for approval in a single interrupt, avoiding the
    parallel-interrupt race that occurs when intercepting per-tool in wrap_tool_call.

    Matching uses fnmatch patterns, e.g.:
      - "cfgpu__generate_image"   exact MCP-prefixed name
      - "cfgpu__generate_*"       MCP server glob
      - "*generate*"              substring glob

    Args:
        tool_patterns: Set of tool name patterns that require approval.
    """

    state_schema = AgentState

    def __init__(self, tool_patterns: set[str]) -> None:
        self._patterns = frozenset(tool_patterns)

    def _needs_approval(self, tool_name: str) -> bool:
        return any(fnmatch.fnmatch(tool_name, pat) for pat in self._patterns)

    def _pending_tool_calls(self, ai_msg: AIMessage) -> list[dict]:
        return [tc for tc in (ai_msg.tool_calls or []) if self._needs_approval(tc["name"])]

    def _build_response(
        self,
        ai_msg: AIMessage,
        tool_approvals: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Apply approval decisions to the AIMessage.

        - approved: keep tool_call with (possibly modified) args
        - rejected: drop from tool_calls, inject error ToolMessage
        - non-approval tools: pass through unchanged
        """
        new_tool_calls: list[dict] = []
        artificial_messages: list[ToolMessage] = []

        for tc in ai_msg.tool_calls or []:
            if not self._needs_approval(tc["name"]):
                new_tool_calls.append(tc)
                continue

            decision = tool_approvals.get(tc["id"])
            if decision is None:
                new_tool_calls.append(tc)
                continue

            if decision.get("status") == "approved":
                approved_args = decision.get("args", tc["args"])
                new_tool_calls.append({**tc, "args": approved_args})
                logger.info(
                    "HumanApproval: approved tool=%s id=%s args_modified=%s",
                    tc["name"],
                    tc["id"],
                    approved_args != tc["args"],
                )
            else:
                reason = decision.get("reason", "User rejected the tool call.")
                artificial_messages.append(
                    ToolMessage(
                        content=json.dumps({"status": "cancelled", "reason": reason}),
                        tool_call_id=tc["id"],
                        name=tc["name"],
                        status="error",
                    )
                )
                logger.info("HumanApproval: rejected tool=%s id=%s", tc["name"], tc["id"])

        new_msg = ai_msg.model_copy(update={"tool_calls": new_tool_calls})
        return {"messages": [new_msg, *artificial_messages]}

    @override
    def after_model(self, state: AgentState, runtime: Any) -> dict[str, Any] | None:
        messages = state.get("messages") or []
        last_msg = next((m for m in reversed(messages) if isinstance(m, AIMessage)), None)
        if not last_msg:
            return None

        pending = self._pending_tool_calls(last_msg)
        if not pending:
            return None

        # --- Resume path: check state for existing decisions ---
        tool_approvals: dict[str, Any] = state.get("tool_approvals") or {}
        pending_ids = {tc["id"] for tc in pending}
        decided_ids = pending_ids & tool_approvals.keys()

        if decided_ids == pending_ids:
            logger.info(
                "HumanApproval: all %d decisions found in state, applying without interrupt",
                len(decided_ids),
            )
            return self._build_response(last_msg, tool_approvals)

        # --- First call: emit SSE and interrupt ---
        pending_payload = [{"id": tc["id"], "name": tc["name"], "args": tc["args"]} for tc in pending]

        try:
            writer = get_stream_writer()
            writer({"type": _APPROVAL_SSE_TYPE, "tool_calls": pending_payload})
            logger.info("HumanApproval: stream_writer emitted %s successfully", _APPROVAL_SSE_TYPE)
        except Exception:
            logger.warning("HumanApproval: get_stream_writer failed; %s event will be emitted by worker fallback", _APPROVAL_SSE_TYPE, exc_info=True)

        logger.info(
            "HumanApproval: emitting approval request for %d tool call(s), pausing graph",
            len(pending),
        )

        # Single interrupt for the whole batch.
        # Raises GraphInterrupt on the first execution (graph checkpoints and pauses).
        # On an unexpected resume-without-state-update, returns the resume value as fallback.
        fallback_decision: dict = interrupt({"type": _APPROVAL_SSE_TYPE, "tool_calls": pending_payload})

        # Fallback: client sent Command(resume=...) without writing to state.
        # Build a tool_approvals map from the resume value and apply it.
        fallback_approvals: dict[str, Any] = {}
        if isinstance(fallback_decision, dict):
            for item in fallback_decision.get("approved", []):
                tc_id = item.get("id") if isinstance(item, dict) else str(item)
                args = item.get("args") if isinstance(item, dict) else None
                if tc_id:
                    fallback_approvals[tc_id] = {"status": "approved", **({"args": args} if args is not None else {})}
            for item in fallback_decision.get("rejected", []):
                tc_id = item.get("id") if isinstance(item, dict) else str(item)
                if tc_id:
                    fallback_approvals[tc_id] = {"status": "rejected"}

        merged = {**tool_approvals, **fallback_approvals}
        return self._build_response(last_msg, merged)

    @override
    async def aafter_model(self, state: AgentState, runtime: Any) -> dict[str, Any] | None:
        return self.after_model(state, runtime)
