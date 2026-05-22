"""MessageStreamMiddleware — emits ai_message and tool_result custom events.

Hooks:
  - wrap_model_call / awrap_model_call: after each main-agent LLM call, emits an
    ``ai_message`` custom event containing the AI response text and tool_calls.
  - wrap_tool_call / awrap_tool_call: after each tool execution, emits a
    ``tool_result`` custom event containing the output and execution status.

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

import logging
from collections.abc import Awaitable, Callable
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

    def __init__(self, *, max_content_chars: int = _MAX_CONTENT_CHARS) -> None:
        super().__init__()
        self._max_content_chars = max_content_chars

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

    def _emit_ai_message(self, ai_msg: AIMessage) -> None:
        content = _extract_text_content(ai_msg.content)
        tool_calls = [
            {"id": tc["id"], "name": tc["name"], "args": tc["args"]}
            for tc in (ai_msg.tool_calls or [])
        ]
        if not content and not tool_calls:
            return

        self._emit({
            "type": "ai_message",
            "message_id": ai_msg.id or "",
            "content": content,
            "tool_calls": tool_calls,
        })
        logger.debug(
            "MessageStreamMiddleware: emitted ai_message id=%s tool_calls=%d",
            ai_msg.id,
            len(tool_calls),
        )

    def _emit_tool_result(self, tool_msg: ToolMessage) -> None:
        content = _truncate(_extract_text_content(tool_msg.content), self._max_content_chars)
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
            self._emit_ai_message(ai_msg)
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
            self._emit_ai_message(ai_msg)
        return response

    # ── Tool wrapping ──────────────────────────────────────────────────────────

    @override
    def wrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], ToolMessage | Command],
    ) -> ToolMessage | Command:
        result = handler(request)
        if isinstance(result, ToolMessage):
            self._emit_tool_result(result)
        return result

    @override
    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
    ) -> ToolMessage | Command:
        result = await handler(request)
        if isinstance(result, ToolMessage):
            self._emit_tool_result(result)
        return result
