"""MessageStreamMiddleware — emits ai_message / tool_result / artifact custom events.

Hooks:
  - wrap_model_call / awrap_model_call: after each main-agent LLM call, emits an
    ``ai_message`` custom event with the AI response text and the *visible*
    tool_calls (internal-visibility tool_calls are filtered out).
  - wrap_tool_call / awrap_tool_call: after each tool execution, dispatches by the
    tool's client-facing visibility — ``progress`` → ``tool_result`` event,
    ``artifact`` → ``artifact`` event (carrying ``ToolMessage.artifact``),
    ``internal`` (default) → nothing. Visibility resolves via tool
    metadata["visibility"] → configured fnmatch patterns → default. Tools may
    return a bare ToolMessage or a Command wrapping one; both are handled.

Combined with ``stream_event_types=["custom"]``, the downstream client receives
only semantically meaningful events and is not exposed to LangGraph's automatic
values/messages events (which include middleware-internal state mutations such as
summarization, dynamic context injection, and MLM memory injection).

Naturally excluded (do not pass through wrap hooks):
  - SummarizationMiddleware internal LLM calls (direct chain, not agent model binding)
  - DynamicContextMiddleware / MLMMiddleware state injections (no model call)
  - DanglingToolCallMiddleware placeholder ToolMessages (no actual tool call)
  - HumanApprovalMiddleware artificial rejection ToolMessages (direct state update)
"""

from __future__ import annotations

import fnmatch
import json
import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, override

from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import (
    ModelCallResult,
    ModelRequest,
    ModelResponse,
)
from langchain_core.messages import AIMessage, ToolMessage
from langgraph.config import get_stream_writer
from langgraph.prebuilt.tool_node import ToolCallRequest
from langgraph.types import Command

logger = logging.getLogger(__name__)

_MAX_CONTENT_CHARS = 4096

# Tool client-facing visibility levels (see docs/message_stream_middleware.md §2.5):
#   internal — fully suppressed: no tool_result, and the tool_call is filtered out
#              of the emitted ai_message (plumbing the end user need not see).
#   progress — emit a tool_result event (intermediate result worth showing).
#   artifact — emit an artifact event carrying ToolMessage.artifact (final deliverable).
VISIBILITY_INTERNAL = "internal"
VISIBILITY_PROGRESS = "progress"
VISIBILITY_ARTIFACT = "artifact"
_VALID_VISIBILITIES = frozenset({VISIBILITY_INTERNAL, VISIBILITY_PROGRESS, VISIBILITY_ARTIFACT})


def _extract_text_content(content: Any) -> str:
    """Normalise LangChain message content (str or block list) to a plain string."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict) and block.get("type") == "text":
                parts.append(block.get("text", ""))
        return "".join(parts)
    return str(content)


def _truncate(text: str, max_chars: int) -> str:
    """Truncate text to max_chars, appending a marker with the omitted count."""
    if len(text) <= max_chars:
        return text
    omitted = len(text) - max_chars
    return text[:max_chars] + f"[truncated: {omitted} chars omitted]"


class MessageStreamMiddleware(AgentMiddleware):
    """Emit ai_message and tool_result custom events at key execution points.

    Wraps the main-agent model call and each tool call to emit semantic custom
    events via LangGraph's stream_writer. Pair with stream_event_types=["custom"]
    to receive a clean, noise-free event stream from the MQ consumer.

    Args:
        max_content_chars: Tool result content is truncated to this many characters
            before emission to avoid hitting MQ message size limits. Default: 4096.
    """

    def __init__(
        self,
        *,
        max_content_chars: int = _MAX_CONTENT_CHARS,
        visibility_patterns: Sequence[tuple[str, str]] | None = None,
        default_visibility: str = VISIBILITY_INTERNAL,
    ) -> None:
        super().__init__()
        self._max_content_chars = max_content_chars
        # Ordered (fnmatch-pattern, visibility) pairs; first match wins. Used as a
        # fallback for tools (e.g. MCP tools) that do not declare their own
        # metadata["visibility"]. Built-in tool metadata always takes precedence.
        self._visibility_patterns: list[tuple[str, str]] = [
            (pat, vis) for pat, vis in (visibility_patterns or []) if vis in _VALID_VISIBILITIES
        ]
        self._default_visibility = default_visibility if default_visibility in _VALID_VISIBILITIES else VISIBILITY_INTERNAL

    # ── Visibility resolution ────────────────────────────────────────────────────

    def _resolve_visibility(self, tool: Any, name: str) -> str:
        """Resolve a tool's client-facing visibility by name.

        Order: tool.metadata["visibility"] → configured fnmatch patterns → default.
        Both wrap_model_call (via request.tools) and wrap_tool_call (via request.tool)
        resolve through this same path so a tool_call's presence in the emitted
        ai_message stays consistent with whether a tool_result/artifact follows.
        """
        meta = getattr(tool, "metadata", None)
        if isinstance(meta, dict):
            vis = meta.get("visibility")
            if vis in _VALID_VISIBILITIES:
                return vis
        for pattern, vis in self._visibility_patterns:
            if fnmatch.fnmatch(name, pattern):
                return vis
        return self._default_visibility

    @staticmethod
    def _tools_by_name(request: Any) -> dict[str, Any]:
        """Build a {tool_name: BaseTool} map from a ModelRequest's bound tools."""
        tools = getattr(request, "tools", None)
        if not isinstance(tools, (list, tuple)):
            return {}
        result: dict[str, Any] = {}
        for tool in tools:
            tool_name = getattr(tool, "name", None)
            if tool_name:
                result[tool_name] = tool
        return result

    @staticmethod
    def _resolve_tool_message(result: Any) -> ToolMessage | None:
        """Extract the ToolMessage from a bare ToolMessage or a Command update."""
        if isinstance(result, ToolMessage):
            return result
        if isinstance(result, Command):
            update = result.update
            if isinstance(update, dict):
                for msg in update.get("messages") or []:
                    if isinstance(msg, ToolMessage):
                        return msg
        return None

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _emit(self, event: dict) -> None:
        try:
            writer = get_stream_writer()
            writer(event)
        except Exception:
            logger.debug(
                "MessageStreamMiddleware: stream_writer unavailable, %s event not emitted",
                event.get("type"),
                exc_info=True,
            )

    def _extract_ai_message(self, response: Any) -> AIMessage | None:
        """Extract AIMessage from a model call response (ModelResponse, ExtendedModelResponse, or AIMessage)."""
        if isinstance(response, AIMessage):
            return response
        # ExtendedModelResponse wraps a ModelResponse
        if hasattr(response, "model_response"):
            response = response.model_response
        # ModelResponse.result is list[BaseMessage]
        messages = getattr(response, "result", None)
        if isinstance(messages, list):
            for msg in reversed(messages):
                if isinstance(msg, AIMessage):
                    return msg
        return None

    def _emit_ai_message(self, ai_msg: AIMessage, tools_by_name: dict[str, Any]) -> None:
        content = _extract_text_content(ai_msg.content)
        # Drop tool_calls for internal-visibility tools: they emit no tool_result, so
        # leaving them here would make the client wait for a result that never arrives.
        tool_calls = [
            {"id": tc["id"], "name": tc["name"], "args": tc["args"]}
            for tc in (ai_msg.tool_calls or [])
            if self._resolve_visibility(tools_by_name.get(tc["name"]), tc["name"]) != VISIBILITY_INTERNAL
        ]
        if not content and not tool_calls:
            return

        event: dict = {
            "type": "ai_message",
            "message_id": ai_msg.id or "",
            "content": content,
            "tool_calls": tool_calls,
        }
        if ai_msg.usage_metadata:
            event["usage"] = dict(ai_msg.usage_metadata)
        self._emit(event)
        logger.debug(
            "MessageStreamMiddleware: emitted ai_message id=%s tool_calls=%d",
            ai_msg.id,
            len(tool_calls),
        )

    def _structure_content(self, text: str) -> dict:
        """Normalise a tool's text output into a JSON object for a uniform client contract.

        The on-wire ``content`` is *always* an object so the client never has to guess
        whether to parse it:
          - JSON object  → used as-is (e.g. cfdream generate_*/task_* flat result, error dict).
          - JSON array   → wrapped as ``{"items": [...]}`` (e.g. list_models).
          - anything else (prose, markdown, non-JSON) → ``{"message": <text>}``.

        ``max_content_chars`` stays the size gate: structured parsing only applies when the
        raw JSON text fits the limit, otherwise the result degrades to a truncated
        ``{"message": ...}``. This keeps the MQ message-size guard intact while still
        delivering structure for the common (small) case — cfdream media results are tiny.

        A tool that needs to report token cost adds a ``usage`` key inside its own result
        payload (it lands in the JSON object here); the middleware does not synthesise one.
        """
        stripped = text.strip()
        if stripped[:1] in ("{", "[") and len(stripped) <= self._max_content_chars:
            try:
                parsed = json.loads(stripped)
            except (ValueError, TypeError):
                parsed = None
            if isinstance(parsed, dict):
                return parsed
            if isinstance(parsed, list):
                return {"items": parsed}
        return {"message": _truncate(text, self._max_content_chars)}

    def _emit_tool_result(self, tool_msg: ToolMessage) -> None:
        content = self._structure_content(_extract_text_content(tool_msg.content))
        status = getattr(tool_msg, "status", None) or "success"

        self._emit({
            "type": "tool_result",
            "message_id": tool_msg.id or "",
            "tool_call_id": tool_msg.tool_call_id or "",
            "name": tool_msg.name or "",
            "content": content,
            "status": status,
        })
        logger.debug(
            "MessageStreamMiddleware: emitted tool_result tool_call_id=%s name=%s status=%s",
            tool_msg.tool_call_id,
            tool_msg.name,
            status,
        )

    def _emit_artifact(self, tool_msg: ToolMessage, artifact: dict) -> None:
        status = getattr(tool_msg, "status", None) or "success"
        self._emit({
            "type": "artifact",
            "message_id": tool_msg.id or "",
            "tool_call_id": tool_msg.tool_call_id or "",
            "name": tool_msg.name or "",
            "items": artifact.get("items") or [],
            "status": status,
        })
        logger.debug(
            "MessageStreamMiddleware: emitted artifact tool_call_id=%s name=%s items=%d",
            tool_msg.tool_call_id,
            tool_msg.name,
            len(artifact.get("items") or []),
        )

    def _emit_for_tool_message(self, request: Any, tool_msg: ToolMessage) -> None:
        """Dispatch a resolved ToolMessage to the right event by tool visibility."""
        visibility = self._resolve_visibility(getattr(request, "tool", None), tool_msg.name or "")
        if visibility == VISIBILITY_INTERNAL:
            return
        artifact = getattr(tool_msg, "artifact", None)
        # artifact-visibility tools carry their deliverable in ToolMessage.artifact;
        # on the error path (no artifact payload) fall back to a tool_result so the
        # failure still surfaces.
        if visibility == VISIBILITY_ARTIFACT and isinstance(artifact, dict) and artifact.get("items") is not None:
            self._emit_artifact(tool_msg, artifact)
        else:
            self._emit_tool_result(tool_msg)

    # ── Model wrapping ─────────────────────────────────────────────────────────

    @override
    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> ModelCallResult:
        response = handler(request)
        ai_msg = self._extract_ai_message(response)
        if ai_msg is not None:
            self._emit_ai_message(ai_msg, self._tools_by_name(request))
        return response

    @override
    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelCallResult:
        response = await handler(request)
        ai_msg = self._extract_ai_message(response)
        if ai_msg is not None:
            self._emit_ai_message(ai_msg, self._tools_by_name(request))
        return response

    # ── Tool wrapping ──────────────────────────────────────────────────────────

    @override
    def wrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], ToolMessage | Command],
    ) -> ToolMessage | Command:
        result = handler(request)
        tool_msg = self._resolve_tool_message(result)
        if tool_msg is not None:
            self._emit_for_tool_message(request, tool_msg)
        return result

    @override
    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
    ) -> ToolMessage | Command:
        result = await handler(request)
        tool_msg = self._resolve_tool_message(result)
        if tool_msg is not None:
            self._emit_for_tool_message(request, tool_msg)
        return result
