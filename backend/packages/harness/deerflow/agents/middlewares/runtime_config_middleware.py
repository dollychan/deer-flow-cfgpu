"""Middleware that applies per-run client runtime config (``config.skills`` / ``config.models``).

Two independent client-controlled parameters from the MQ task message are surfaced on
``runtime.context`` by the consumer and consumed here, in one middleware:

1. ``config.skills`` → ``runtime.context["skills"]`` — **Model B (client fully controls skills)**.
   The client UI lets the user explicitly pick the skill(s) for a task; the selection is an
   authoritative instruction, so the chosen skill's full ``SKILL.md`` is eager-injected into
   the conversation as a ``<system-reminder>`` HumanMessage in ``(a)before_agent`` (before the
   first LLM call), instead of relying on deerflow's progressive ``<available_skills>`` listing
   (suppressed for the cf-dream agent via its ``config.yaml: skills: []``).

2. ``config.models`` → ``runtime.context["models"]`` — **方案 3 (router-whitelist), human-final**.
   The client UI restricts the *selection range* of cfdream generate models per task type. The
   top-level ``type`` field gates whether the whitelist is applied at all:

   - ``type == "manual"`` — **server-authoritative**. In ``(a)after_model`` the ``model`` argument
     of each cfdream ``generate_image`` / ``generate_video`` tool call in the freshly produced
     AIMessage is constrained to the allowed cfdream model IDs: the LLM's preference is kept when it
     is inside the range, otherwise the whole allowed range is written down so cfdream's own router
     (``select_model(allowed=...)``) scores within it.
   - ``type == "auto"`` (also the default for a missing/unknown ``type``) — **LLM's choice stands**.
     The whitelist is *not* enforced; the model the LLM picked is passed through unchanged.

   In **both** modes (and even with **no** ``config.models`` at all), a null/empty generate ``model``
   is defaulted to ``"auto"`` so the cfdream tool never receives ``null`` (its own ``model`` default
   is ``"auto"``). This null-safety normalization is independent of the ``type`` gate.

   **Ordering matters.** This runs in ``after_model`` and is registered *after*
   ``HumanApprovalMiddleware`` (HAM) so that — because LangChain dispatches ``after_model`` in
   reverse registration order — this constraint applies *before* HAM builds its approval payload.
   A human approver therefore sees (and approves) the already-constrained model, and on resume the
   call runs with exactly what was approved. Precedence is **human-final**: a human approver may
   edit the model outside the range and it is honored (no second clamp at tool-execution time).
   This is a deliberate product choice (see cfgpu-docs/config.md "config.models — 方案 3").

Design: see cfgpu-docs/config.md "config.skills 的处理 — Model B" and "config.models 的处理 — 方案 3".

Skill-injection properties:
- Fires once per run in ``(a)before_agent`` — the reminder then persists for every model round.
- Reads ``runtime.context["skills"]`` (list of skill names, usually one). No value → no-op.
- Loads each skill's SKILL.md full text via ``get_or_new_skill_storage``; blocking file IO is
  offloaded via ``asyncio.to_thread`` on the async path (blocking-IO gate).
- Missing skill → strategy B: WARNING log + a visible note telling the agent to inform the user;
  run continues (a missing skill is a client-menu/server-dir desync, not fatal).
- Idempotent per turn via the ``runtime_config_skill_reminder`` ``additional_kwargs`` flag.
"""

from __future__ import annotations

import asyncio
import fnmatch
import logging
from typing import TYPE_CHECKING, override

from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.runtime import Runtime

if TYPE_CHECKING:
    from deerflow.config.app_config import AppConfig

logger = logging.getLogger(__name__)

_SKILL_REMINDER_KEY = "runtime_config_skill_reminder"
_SUMMARY_MESSAGE_NAME = "summary"

# Built-in default model_bindings: tool-name fnmatch pattern → config.models task-type slice.
# Patterns use a leading ``*`` so they match both the native tool names (``generate_image`` /
# ``generate_video``) and MCP-prefixed names (``cfdream_generate_image``). An agent's
# ``config.yaml: model_bindings`` replaces this default entirely when set.
_DEFAULT_MODEL_BINDINGS: dict[str, str] = {
    "*generate_image": "image",
    "*generate_video": "video",
}
_AUTO_MODEL = "auto"
_MANUAL_MODE = "manual"


def _is_manual_selection(models_cfg: object) -> bool:
    """Return whether ``config.models`` requests manual (server-authoritative) selection.

    Only an explicit top-level ``type == "manual"`` (case/space-insensitive) enables the whitelist
    constraint. Any other value — ``"auto"``, missing, or malformed — is treated as auto: the LLM's
    own ``model`` choice stands and only the null/empty → ``"auto"`` null-safety normalization runs.
    """
    return isinstance(models_cfg, dict) and str(models_cfg.get("type", "")).strip().lower() == _MANUAL_MODE


def _is_real_user_message(message: object) -> bool:
    """Return whether *message* is a genuine user turn (not a hidden reminder/summary)."""
    if not isinstance(message, HumanMessage):
        return False
    ak = getattr(message, "additional_kwargs", None) or {}
    if ak.get("hide_from_ui") or ak.get(_SKILL_REMINDER_KEY):
        return False
    if getattr(message, "name", None) == _SUMMARY_MESSAGE_NAME:
        return False
    return True


def _is_skill_reminder(message: object) -> bool:
    """Return whether *message* is a skill reminder injected by this middleware."""
    return isinstance(message, HumanMessage) and bool((getattr(message, "additional_kwargs", None) or {}).get(_SKILL_REMINDER_KEY))


def _context_dict(runtime: object) -> dict:
    ctx = getattr(runtime, "context", None) or {}
    return ctx if isinstance(ctx, dict) else {}


def _build_reminder(found: list[tuple[str, str]], missing: list[str], out_of_scope: list[str]) -> str:
    """Build the ``<system-reminder>`` content from loaded bodies, missing and out-of-scope names.

    ``out_of_scope`` names are still present in ``found`` (injected best-effort), but are also
    annotated so the agent knows they fall outside this agent's normal whitelist (strategy B).
    """
    lines: list[str] = ["<system-reminder>"]
    if found:
        lines.append("The user has selected the following skill(s) for THIS message. You MUST follow their workflow and instructions for this task:")
        lines.append("")
        for name, content in found:
            lines.append("<skill>")
            lines.append(f"<name>{name}</name>")
            lines.append(content.strip())
            lines.append("</skill>")
    if out_of_scope:
        if found:
            lines.append("")
        lines.append(f"Note: the following skill(s) are outside this agent's normal scope but were applied as explicitly requested by the client: {', '.join(out_of_scope)}.")
    if missing:
        if found or out_of_scope:
            lines.append("")
        lines.append(f"The following requested skill(s) were NOT found and cannot be applied: {', '.join(missing)}. Tell the user you cannot apply them.")
    lines.append("</system-reminder>")
    return "\n".join(lines)


# ── config.models — 方案 3 helpers ───────────────────────────────────────────────


def _task_type_for_tool(name: str, bindings: dict[str, str]) -> str | None:
    """Resolve a tool name to its config.models task-type slice via *bindings*, or None.

    *bindings* maps fnmatch tool-name patterns to a task-type key ("image"/"video").
    First matching pattern (in insertion order) wins.
    """
    if not name:
        return None
    for pattern, task_type in bindings.items():
        if fnmatch.fnmatch(name, pattern):
            return task_type
    return None


def _allowed_models_for_task(models_cfg: object, task_type: str) -> list[str]:
    """Extract the allowed cfdream model IDs for *task_type* from ``config.models``.

    ``config.models`` shape: ``{"type": "auto"|"manual", "content": [{"type": "image"|"video",
    "model_names": [<cfdream model id>, ...]}]}``. Returns the (de-duplicated) model_names for the
    matching task type, or ``[]`` when the field is absent/malformed for that type.
    """
    if not isinstance(models_cfg, dict):
        return []
    content = models_cfg.get("content")
    if not isinstance(content, list):
        return []
    out: list[str] = []
    for entry in content:
        if isinstance(entry, dict) and entry.get("type") == task_type:
            names = entry.get("model_names")
            if isinstance(names, list):
                out.extend(str(n) for n in names if n)
    return list(dict.fromkeys(out))


def _restrict_model_arg(args: dict, allowed: list[str]) -> dict | None:
    """Constrain the tool ``model`` argument to *allowed*; return new args or None if unchanged.

    Rule: effective = (LLM-requested ids) ∩ allowed; if empty (LLM said "auto" or chose outside
    the range) fall back to the whole allowed range so cfdream's router scores within it. A single
    surviving id is passed as a string, multiple as a list — matching the tool's ``str|list[str]``
    ``model`` schema.
    """
    requested = args.get("model", _AUTO_MODEL)
    if isinstance(requested, str):
        req_ids = [] if requested == _AUTO_MODEL else [requested]
    elif isinstance(requested, list):
        req_ids = [str(x) for x in requested if x and x != _AUTO_MODEL]
    else:
        req_ids = []

    effective = [m for m in req_ids if m in allowed] or list(allowed)
    new_model: str | list[str] = effective[0] if len(effective) == 1 else effective
    if new_model == requested:
        return None
    return {**args, "model": new_model}


def _normalize_empty_model(args: dict) -> dict | None:
    """Default a missing/null/empty ``model`` arg to the ``"auto"`` sentinel.

    Independent of ``config.models`` (applies even when the client sent no whitelist): cfdream's
    generate tools default ``model`` to ``"auto"`` and treat it as "let the router pick", so a
    generate call must never reach the tool carrying ``null`` / ``""`` / an all-empty list.
    Returns new args when a rewrite is needed, else None (``model`` already usable).
    """
    model = args.get("model")
    if isinstance(model, str):
        empty = not model.strip()
    elif isinstance(model, list):
        empty = not [x for x in model if x and str(x).strip()]
    else:  # None, missing key, or any non-str/list value
        empty = model is None
    if not empty or model == _AUTO_MODEL:
        return None
    return {**args, "model": _AUTO_MODEL}


class RuntimeConfigMiddleware(AgentMiddleware):
    """Apply per-run client ``config.skills`` (eager inject) and ``config.models`` (whitelist).

    Register this **after** ``HumanApprovalMiddleware`` so its ``after_model`` model constraint
    runs before HAM's (after_model dispatches in reverse registration order).
    """

    def __init__(
        self,
        *,
        app_config: AppConfig | None = None,
        model_bindings: dict[str, str] | None = None,
        available_skills: set[str] | None = None,
    ) -> None:
        super().__init__()
        self._app_config = app_config
        # Effective bindings: the agent's config.yaml model_bindings if set, else the built-in
        # default. A configured dict replaces the default entirely (config wins).
        self._model_bindings: dict[str, str] = dict(model_bindings) if model_bindings else dict(_DEFAULT_MODEL_BINDINGS)
        # Agent whitelist (the result of ``_available_skill_names``): runtime.skills outside it
        # are injected best-effort but flagged (strategy B). ``None`` = full pool → no constraint.
        self._available_skills: set[str] | None = set(available_skills) if available_skills is not None else None

    # ── config.skills — eager injection (Model B) ────────────────────────────────

    def _requested_skills(self, runtime: Runtime) -> list[str]:
        raw = _context_dict(runtime).get("skills")
        if not raw:
            return []
        if isinstance(raw, str):
            return [raw]
        try:
            return [str(s) for s in raw if s]
        except TypeError:
            return []

    def _load_blocks(self, names: list[str]) -> tuple[list[tuple[str, str]], list[str], list[str]]:
        """Resolve skill names to (name, SKILL.md text) blocks; return (found, missing, out_of_scope).

        ``out_of_scope`` is the subset of ``found`` names that fall outside this agent's whitelist
        (``self._available_skills``): they are still loaded and injected best-effort, but flagged so
        the reminder can annotate them (strategy B). When the whitelist is ``None`` (full pool) no
        name is out of scope.

        Blocking file IO — call directly on the sync path, via ``to_thread`` on async.
        """
        from deerflow.skills.storage import get_or_new_skill_storage

        found: list[tuple[str, str]] = []
        missing: list[str] = []
        out_of_scope: list[str] = []
        try:
            storage = get_or_new_skill_storage(app_config=self._app_config)
            by_name = {s.name: s for s in storage.load_skills()}
        except Exception:
            logger.exception("RuntimeConfig: failed to load skills storage; treating all as missing")
            return [], list(names), []

        for name in names:
            skill = by_name.get(name)
            if skill is None:
                logger.warning("RuntimeConfig: requested skill %r not found in skills directory", name)
                missing.append(name)
                continue
            try:
                content = skill.skill_file.read_text(encoding="utf-8")
            except Exception:
                logger.warning(
                    "RuntimeConfig: failed to read SKILL.md for %r at %s",
                    name,
                    skill.skill_file,
                    exc_info=True,
                )
                missing.append(name)
                continue
            if self._available_skills is not None and name not in self._available_skills:
                logger.warning(
                    "RuntimeConfig: requested skill %r is outside this agent's whitelist; injecting best-effort",
                    name,
                )
                out_of_scope.append(name)
            found.append((name, content))
        return found, missing, out_of_scope

    def _build_update(self, state, found: list[tuple[str, str]], missing: list[str], out_of_scope: list[str]) -> dict | None:
        messages = list(state.get("messages", []))
        if not messages:
            return None

        # Anchor to the current turn = the last genuine user message.
        target_idx: int | None = None
        for i in range(len(messages) - 1, -1, -1):
            if _is_real_user_message(messages[i]):
                target_idx = i
                break
        if target_idx is None:
            # No user message to anchor to (e.g. pure HIL resume) — the original turn's
            # reminder is already in history; nothing new to inject.
            return None

        # Idempotency: this turn already carries a skill reminder.
        if any(_is_skill_reminder(m) for m in messages[target_idx + 1 :]):
            return None

        reminder = HumanMessage(
            content=_build_reminder(found, missing, out_of_scope),
            additional_kwargs={"hide_from_ui": True, _SKILL_REMINDER_KEY: True},
        )
        return {"messages": [reminder]}

    @override
    def before_agent(self, state, runtime: Runtime) -> dict | None:
        names = self._requested_skills(runtime)
        if not names:
            return None
        found, missing, out_of_scope = self._load_blocks(names)
        return self._build_update(state, found, missing, out_of_scope)

    @override
    async def abefore_agent(self, state, runtime: Runtime) -> dict | None:
        names = self._requested_skills(runtime)
        if not names:
            return None
        found, missing, out_of_scope = await asyncio.to_thread(self._load_blocks, names)
        return self._build_update(state, found, missing, out_of_scope)

    # ── config.models — router-whitelist, applied before HAM (方案 3) ─────────────

    def _constrain_tool_call(self, tool_call: dict, models_cfg: object) -> dict | None:
        """Return a model-constrained/normalized copy of *tool_call*, or None to pass through.

        Only in **manual** mode (``config.models.type == "manual"``) with a non-empty whitelist for
        this task type is ``model`` constrained to the allowed range (this also rewrites a
        null/``"auto"`` model into the range). In **auto** mode (or with no whitelist) the LLM's
        choice stands; we only ensure the ``model`` arg is never null/empty by defaulting it to
        ``"auto"``.
        """
        task_type = _task_type_for_tool(tool_call.get("name", "") or "", self._model_bindings)
        if task_type is None:
            return None
        args = tool_call.get("args")
        if not isinstance(args, dict):
            return None
        allowed = _allowed_models_for_task(models_cfg, task_type)
        if allowed and _is_manual_selection(models_cfg):
            new_args = _restrict_model_arg(args, allowed)
        else:
            new_args = _normalize_empty_model(args)
        if new_args is None:
            return None
        logger.debug(
            "RuntimeConfig: normalized %s model %r → %r (allowed=%s)",
            tool_call.get("name"),
            args.get("model"),
            new_args.get("model"),
            allowed,
        )
        return {**tool_call, "args": new_args}

    def _apply_models(self, state, runtime: Runtime) -> dict | None:
        """Constrain/normalize cfdream generate ``model`` args in the latest AIMessage.

        Applies the ``config.models`` whitelist only in manual mode (``type == "manual"``); in auto
        mode the LLM's choice stands. In every mode a null/empty generate ``model`` is defaulted to
        ``"auto"`` so the tool never receives ``null``.
        """
        models_cfg = _context_dict(runtime).get("models")
        messages = state.get("messages") or []
        ai_msg = next((m for m in reversed(messages) if isinstance(m, AIMessage)), None)
        if ai_msg is None or not ai_msg.tool_calls:
            return None

        new_tool_calls: list[dict] = []
        changed = False
        for tc in ai_msg.tool_calls:
            constrained = self._constrain_tool_call(tc, models_cfg)
            if constrained is None:
                new_tool_calls.append(tc)
            else:
                new_tool_calls.append(constrained)
                changed = True
        if not changed:
            return None
        # model_copy preserves the message id so add_messages replaces it in place.
        return {"messages": [ai_msg.model_copy(update={"tool_calls": new_tool_calls})]}

    @override
    def after_model(self, state, runtime: Runtime) -> dict | None:
        return self._apply_models(state, runtime)

    @override
    async def aafter_model(self, state, runtime: Runtime) -> dict | None:
        return self._apply_models(state, runtime)
