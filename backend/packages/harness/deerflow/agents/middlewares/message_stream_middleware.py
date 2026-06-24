"""MessageStreamMiddleware — emits ai_message / tool_result / artifact custom events.

Hooks:
  - wrap_model_call / awrap_model_call: after each main-agent LLM call, emits an
    ``ai_message`` custom event with the AI response text and the *visible*
    tool_calls (internal-visibility tool_calls are filtered out).
  - wrap_tool_call / awrap_tool_call: after each tool execution, dispatches by the
    tool's client-facing visibility — ``progress`` → ``tool_result`` event,
    ``artifact`` → ``artifact`` event (carrying ``ToolMessage.artifact`` items plus
    the normalised tool ``content`` at the same level),
    ``internal`` (default) → nothing. Visibility resolves via tool
    metadata["visibility"] → configured fnmatch patterns → default. Tools may
    return a bare ToolMessage or a Command wrapping one; both are handled.
    For MCP tools, any ``structuredContent`` (carried on
    ``ToolMessage.artifact["structured_content"]``) is merged into the emitted
    event ``content`` — a client-only side channel that the model never saw, so the
    client gets the full result (cfdream usage/payload, understand_vision
    reasoning_content) without it bloating the model's tool result. **Once emitted,
    the side channel is stripped from the returned ToolMessage** so it does not
    persist in the checkpoint: it was never model context, the checkpoint has no
    reader for it, and its payload can carry presigned URLs (cfdream
    ``reference_images``) we keep out of persisted state (materials I9/I10). This is
    the clean content/structuredContent split — ``content`` → ToolMessage → LLM
    context; ``structuredContent`` → downstream event → client only.

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

from deerflow.agents.materials.registry import project_artifact_items

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
    def _structured_side_channel(tool_msg: ToolMessage) -> dict | None:
        """Extract the MCP structuredContent carried on ``ToolMessage.artifact``.

        langchain-mcp-adapters maps an MCP tool's ``structuredContent`` onto
        ``ToolMessage.artifact == {"structured_content": {...}}`` (its ``MCPToolArtifact``
        TypedDict). That payload is a *client-only* side channel: it never entered the
        model context (the LLM only saw the lean ``ToolMessage.content``). We merge it
        back into the emitted event ``content`` so the client receives the full result
        — e.g. cfdream ``generate_*`` ``usage``/``payload`` or ``understand_vision``
        ``reasoning_content``/``usage``/``payload`` — without bloating (and getting
        truncated out of) the model's tool result. Returns None when absent.

        MaterialsMiddleware's media-capture rewrite (``_rewrite_result``) preserves this
        key alongside ``items``, so it survives for ``generate_*`` artifact events too.
        """
        artifact = getattr(tool_msg, "artifact", None)
        if isinstance(artifact, dict):
            sc = artifact.get("structured_content")
            if isinstance(sc, dict):
                return sc
        return None

    @staticmethod
    def _strip_structured_side_channel(tool_msg: ToolMessage) -> None:
        """Drop the MCP ``structured_content`` from the persisted ToolMessage.

        Called after emit has merged the side channel into the downstream event: it
        has now served its only purpose (client display). It never entered the model
        context (the LLM saw only ``content``), the checkpoint has no reader for it,
        and its ``payload`` can carry presigned URLs (cfdream ``reference_images``)
        that must not persist in state (materials I9/I10). Mutating in place keeps the
        ToolMessage's identity, so the same object the wrap hook returns — bare or
        wrapped in a ``Command.update["messages"]`` — is the one that gets
        checkpointed. Any sibling keys (e.g. ``items``) are preserved; an artifact
        left empty collapses to ``None``.
        """
        artifact = getattr(tool_msg, "artifact", None)
        if isinstance(artifact, dict) and "structured_content" in artifact:
            remaining = {k: v for k, v in artifact.items() if k != "structured_content"}
            tool_msg.artifact = remaining or None

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
        sc = self._structured_side_channel(tool_msg)
        if sc:
            content = {**content, **sc}
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

    def _emit_artifact(self, tool_msg: ToolMessage, items: list[dict]) -> None:
        status = getattr(tool_msg, "status", None) or "success"
        # The client needs the tool's textual result (e.g. cfdream generate_*'s
        # task_id/cost_tokens) alongside the artifact items, so carry the
        # normalised content at the same level as items via the same
        # _structure_content contract used by tool_result.
        content = self._structure_content(_extract_text_content(tool_msg.content))
        sc = self._structured_side_channel(tool_msg)
        if sc:
            content = {**content, **sc}
        self._emit({
            "type": "artifact",
            "message_id": tool_msg.id or "",
            "tool_call_id": tool_msg.tool_call_id or "",
            "name": tool_msg.name or "",
            "content": content,
            "items": items,
            "status": status,
        })
        logger.debug(
            "MessageStreamMiddleware: emitted artifact tool_call_id=%s name=%s items=%d",
            tool_msg.tool_call_id,
            tool_msg.name,
            len(items),
        )

    @staticmethod
    def _material_ids_from_content(tool_msg: ToolMessage) -> list[str]:
        """Read the ``materials:[id...]`` list the MaterialsCapture rewrite stamped into content."""
        text = _extract_text_content(tool_msg.content)
        if not text:
            return []
        try:
            parsed = json.loads(text.strip())
        except (ValueError, TypeError):
            return []
        if isinstance(parsed, dict):
            ids = parsed.get("materials")
            if isinstance(ids, list):
                return [i for i in ids if isinstance(i, str)]
        return []

    @staticmethod
    def _materials_view(request: Any, result: Any) -> dict:
        """Materials visible at emit time = prior ``state.materials`` ∪ this call's ``Command.update``.

        The just-captured materials live on the returned Command (not yet merged into graph
        state inside the wrap onion), so both sources must be unioned to resolve fresh ids.
        """
        state = getattr(request, "state", None)
        base = state.get("materials") if isinstance(state, dict) else None
        merged = dict(base or {})
        if isinstance(result, Command) and isinstance(result.update, dict):
            upd = result.update.get("materials")
            if isinstance(upd, dict):
                merged.update(upd)
        return merged

    @staticmethod
    def _stamp_display(result: Any, ids: list[str]) -> None:
        """Persist ``display=true`` onto the emitted materials (D14: emit owns the deliverable flag).

        Writes into the returned ``Command.update["materials"]`` so the non-streaming final_state
        projection (consumer ``project_display_refs``) sees the same deliverable set the live
        artifact event carried. Partial ``{id, display}`` stamps field-attach via ``merge_materials``.
        """
        if not isinstance(result, Command) or not isinstance(result.update, dict):
            return
        mats = result.update.get("materials")
        if not isinstance(mats, dict):
            mats = {}
            result.update["materials"] = mats
        for mid in ids:
            existing = mats.get(mid)
            if isinstance(existing, dict):
                existing["display"] = True
            else:
                mats[mid] = {"id": mid, "display": True}

    def _artifact_items(self, request: Any, result: Any, tool_msg: ToolMessage) -> list[dict]:
        """Resolve the artifact items for an artifact-visibility tool result.

        Two sources: (1) a tool that self-builds its deliverable on ``ToolMessage.artifact``
        (e.g. present_files) → use those items directly; (2) MaterialsCapture path → project
        from the materials the rewritten content references, filtered by ``stable``, and stamp
        ``display=true`` so the persisted registry agrees with the live event (D14).
        """
        artifact = getattr(tool_msg, "artifact", None)
        if isinstance(artifact, dict) and artifact.get("items"):
            return artifact["items"]
        ids = self._material_ids_from_content(tool_msg)
        if not ids:
            return []
        items = project_artifact_items(self._materials_view(request, result), ids)
        if items:
            self._stamp_display(result, [it["id"] for it in items])
        return items

    def _emit_for_tool_message(self, request: Any, tool_msg: ToolMessage, result: Any) -> None:
        """Dispatch a resolved ToolMessage to the right event by tool visibility, then
        strip the client-only ``structured_content`` so it never reaches the checkpoint.
        """
        visibility = self._resolve_visibility(getattr(request, "tool", None), tool_msg.name or "")
        if visibility != VISIBILITY_INTERNAL:
            # artifact-visibility deliverables: project items (self-built or from materials).
            # No items (error / failed rehost / nothing captured) → fall back to tool_result so
            # the result still surfaces and no empty/unstable artifact is emitted (I5).
            if visibility == VISIBILITY_ARTIFACT:
                items = self._artifact_items(request, result, tool_msg)
                if items:
                    self._emit_artifact(tool_msg, items)
                else:
                    self._emit_tool_result(tool_msg)
            else:
                self._emit_tool_result(tool_msg)
        # The side channel has now been delivered downstream (or, for internal tools,
        # there is no downstream event by design); in every case it is transient client
        # data that must not linger on the persisted ToolMessage.
        self._strip_structured_side_channel(tool_msg)

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
            self._emit_for_tool_message(request, tool_msg, result)
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
            self._emit_for_tool_message(request, tool_msg, result)
        return result
