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
     retained in tool_calls) plus artificial ToolMessages for any rejected calls.
     Rejected calls are kept in AIMessage.tool_calls so that every ToolMessage has a
     matching tool_call_id — stripping them would create orphaned ToolMessages that
     break LangChain message history validation. Routing still goes to END because
     should_continue checks messages[-1], which is the ToolMessage, not the AIMessage.

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
      - "cfgpu_generate_image"   exact MCP-prefixed name
      - "cfgpu_generate_*"       MCP server glob
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
        approval_ids: set[str],
    ) -> dict[str, Any] | None:
        """Apply approval decisions to the AIMessage.

        - approved: keep tool_call with (possibly modified) args
        - rejected: keep in tool_calls (preserves AIMessage↔ToolMessage pairing), inject error ToolMessage
        - non-approval tools: pass through unchanged
        """
        new_tool_calls: list[dict] = []
        artificial_messages: list[ToolMessage] = []

        for tc in ai_msg.tool_calls or []:
            if tc["id"] not in approval_ids:
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
                reason = decision.get("reason") or "用户拒绝了这次调用，请向用户确认后再继续。"
                # Self-describing rejection: the structured fields (tool, rejected_args,
                # executed=False) carry the call's identity in the ToolMessage content, so the
                # model attributes it by content rather than joining tool_call_id back to the
                # AIMessage by position. Without this anchor an anonymous {"status":"cancelled"}
                # blob makes the model guess by position; in a partial-approval batch that guess
                # flips and it reports a rejected call as "succeeded". message is a short
                # human-readable summary — it names the tool and the not-executed signal but does
                # NOT re-dump args/reason (those live in rejected_args/reason).
                # See BUG: partial-approval tool_result mis-attribution.
                rejected_payload = {
                    "status": "rejected",
                    "executed": False,
                    "output": None,
                    "tool": tc["name"],
                    "rejected_args": tc["args"],
                    "reason": reason,
                    "message": (
                        f"❌ 用户拒绝了对 {tc['name']} 的调用，该调用【未执行、无任何产物】。"
                        "不要判定为成功或重复其它已成功的调用；如需继续请先向用户确认。"
                    ),
                }
                # Keep the tool_call in the AIMessage so ToolMessage.tool_call_id has a
                # matching entry — stripping it creates an orphaned ToolMessage that breaks
                # LangChain message history validation and confuses the model on future turns.
                # Routing still goes to END because messages[-1] is the ToolMessage (not AIMessage).
                new_tool_calls.append(tc)
                artificial_messages.append(
                    ToolMessage(
                        content=json.dumps(rejected_payload, ensure_ascii=False),
                        tool_call_id=tc["id"],
                        name=tc["name"],
                        status="error",
                    )
                )
                logger.info("HumanApproval: rejected tool=%s id=%s", tc["name"], tc["id"])

        new_msg = ai_msg.model_copy(update={"tool_calls": new_tool_calls})
        # Reclaim tool_approvals: the decisions for this batch have now been consumed —
        # approved args are baked into new_msg, rejected calls have their error ToolMessage.
        # Nothing downstream (ToolNode / should_continue / future model) ever reads
        # tool_approvals again, so we clear it in the SAME return dict that carries the
        # rewritten AIMessage. Both land atomically in one super-step (merge_tool_approvals
        # treats {} as a clear sentinel). This is the only safe reclaim point: it must not
        # precede this line (approved args live ONLY in tool_approvals until baked above),
        # and a full clear is safe because the graph is strictly serial (one suspend point
        # per thread) so this never drops a concurrent batch — it also self-heals any
        # historical residue. Reached on both terminal consumers: the decided==pending
        # resume path and the Command(resume=...) fallback path.
        # See cfgpu-docs/human_approval_middleware.md §9.
        return {"messages": [new_msg, *artificial_messages], "tool_approvals": {}}

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
            return self._build_response(last_msg, tool_approvals, pending_ids)

        # --- First call (or partial resume): emit SSE and interrupt ---
        # Only re-request the still-undecided calls. On a partial resume some
        # decisions are already merged into state.tool_approvals (merge reducer);
        # re-emitting those would make the client re-confirm tools it already
        # answered. The final _build_response below still applies the full set.
        undecided = [tc for tc in pending if tc["id"] not in tool_approvals]
        pending_payload = [{"id": tc["id"], "name": tc["name"], "args": tc["args"]} for tc in undecided]

        sse_emitted = False
        try:
            writer = get_stream_writer()
            writer({"type": _APPROVAL_SSE_TYPE, "tool_calls": pending_payload})
            sse_emitted = True
            logger.info("HumanApproval: stream_writer emitted %s successfully", _APPROVAL_SSE_TYPE)
        except Exception:
            logger.warning("HumanApproval: get_stream_writer failed; %s event will be emitted by worker fallback", _APPROVAL_SSE_TYPE, exc_info=True)

        logger.info(
            "HumanApproval: emitting approval request for %d undecided tool call(s) (%d already decided), pausing graph",
            len(undecided),
            len(pending) - len(undecided),
        )

        # Single interrupt for the whole batch.
        # sse_emitted=True tells worker.py not to re-publish the SSE from the checkpoint,
        # preventing duplicate delivery to the client.
        # Raises GraphInterrupt on the first execution (graph checkpoints and pauses).
        # On an unexpected resume-without-state-update, returns the resume value as fallback.
        fallback_decision: dict = interrupt({"type": _APPROVAL_SSE_TYPE, "tool_calls": pending_payload, "sse_emitted": sse_emitted})

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
        return self._build_response(last_msg, merged, pending_ids)

    @override
    async def aafter_model(self, state: AgentState, runtime: Any) -> dict[str, Any] | None:
        return self.after_model(state, runtime)
