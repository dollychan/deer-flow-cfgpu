"""LLM-based knowledge extractors for multi-level memory.

Each extractor reads an extraction skill (a Markdown prompt template), fills in
the conversation transcript and existing facts, invokes the LLM synchronously
via ``asyncio.to_thread``, and parses the returned JSON array into a list of
:class:`ExtractionResult` objects.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from typing import Any

from deerflow.agents.memory.prompt import format_conversation_for_update
from deerflow.agents.memory.skill_resolver import MissingSkillError, get_extraction_skill
from deerflow.agents.memory.updater import _extract_text  # shared text normalizer
from deerflow.config.memory_config import get_memory_config
from deerflow.models import create_chat_model

logger = logging.getLogger(__name__)

_model_cache: dict[str | None, Any] = {}


@dataclass
class ExtractionResult:
    """One scope-group of extracted knowledge."""

    scope_key: str
    facts: list[dict] = field(default_factory=list)
    summary: str | None = None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _get_model():
    name = get_memory_config().model_name
    if name not in _model_cache:
        _model_cache[name] = create_chat_model(name=name, thinking_enabled=False)
    return _model_cache[name]


def _parse_response(raw: Any) -> list[ExtractionResult]:
    """Parse a JSON array of ``{scope_key, facts, summary}`` dicts."""
    text = _extract_text(raw).strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

    data = json.loads(text)
    if not isinstance(data, list):
        logger.warning("extractor: unexpected root type %s, expected list", type(data).__name__)
        return []

    results: list[ExtractionResult] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        scope_key = item.get("scope_key", "")
        facts = item.get("facts", [])
        summary = item.get("summary") or None
        if not isinstance(facts, list):
            facts = []
        results.append(ExtractionResult(scope_key=str(scope_key), facts=facts, summary=summary))
    return results


def _invoke_sync(prompt: str) -> list[ExtractionResult]:
    model = _get_model()
    response = model.invoke(prompt, config={"run_name": "mlm_extractor"})
    return _parse_response(response.content)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def extract_user_knowledge(
    messages: list[Any],
    user_id: str,
    agent_name: str,
    existing: dict[str, list[dict]],
) -> list[ExtractionResult]:
    """Extract user-scoped knowledge from *messages*.

    Args:
        messages: Filtered conversation messages (human + final AI turns).
        user_id: The user whose knowledge is being updated.
        agent_name: The agent handling the conversation (used for skill resolution).
        existing: Map of ``scope_key → [fact, ...]`` already stored for this user.

    Returns:
        List of :class:`ExtractionResult`, one per scope group. Empty on failure.
    """
    conversation = format_conversation_for_update(messages)
    if not conversation.strip():
        return []

    try:
        skill_text = get_extraction_skill("user", agent_name)
    except MissingSkillError as exc:
        logger.warning("extract_user_knowledge: skill missing — %s", exc)
        return []

    existing_json = json.dumps(existing, ensure_ascii=False, indent=2)
    prompt = skill_text.format(
        conversation=conversation,
        existing_facts_json=existing_json,
    )

    try:
        return await asyncio.to_thread(_invoke_sync, prompt)
    except json.JSONDecodeError as exc:
        logger.warning("extract_user_knowledge: JSON parse error — %s", exc)
        return []
    except Exception as exc:
        logger.exception("extract_user_knowledge: unexpected error — %s", exc)
        return []


async def extract_project_knowledge(
    messages: list[Any],
    project_id: str,
    agent_name: str,
    user_id: str | None,
    existing: dict[str, list[dict]],
) -> list[ExtractionResult]:
    """Extract project-scoped knowledge from *messages*.

    Args:
        messages: Filtered conversation messages.
        project_id: The project being discussed.
        agent_name: The agent handling the conversation.
        user_id: The user's ID (used for ``user:{uid}`` scope key generation).
        existing: Map of ``scope_key → [fact, ...]`` already stored for this project.

    Returns:
        List of :class:`ExtractionResult`. Empty on failure.
    """
    conversation = format_conversation_for_update(messages)
    if not conversation.strip():
        return []

    try:
        skill_text = get_extraction_skill("project", agent_name)
    except MissingSkillError as exc:
        logger.warning("extract_project_knowledge: skill missing — %s", exc)
        return []

    existing_json = json.dumps(existing, ensure_ascii=False, indent=2)
    prompt = skill_text.format(
        conversation=conversation,
        existing_facts_json=existing_json,
        project_id=project_id,
        user_id=user_id or "",
        agent_name=agent_name,
    )

    try:
        return await asyncio.to_thread(_invoke_sync, prompt)
    except json.JSONDecodeError as exc:
        logger.warning("extract_project_knowledge: JSON parse error — %s", exc)
        return []
    except Exception as exc:
        logger.exception("extract_project_knowledge: unexpected error — %s", exc)
        return []


async def extract_agent_knowledge(
    messages: list[Any],
    agent_name: str,
    existing_facts: list[dict],
) -> ExtractionResult:
    """Extract global agent knowledge from *messages*.

    The extraction skill for agent type is agent-specific (no public fallback).
    If the skill file is missing or the LLM call fails, returns an empty result.

    Args:
        messages: Filtered conversation messages.
        agent_name: The agent whose knowledge is being updated.
        existing_facts: Existing agent-level facts (for deduplication guidance).

    Returns:
        A single :class:`ExtractionResult` (scope_key always ``""``). Empty on failure.
    """
    conversation = format_conversation_for_update(messages)
    if not conversation.strip():
        return ExtractionResult(scope_key="")

    try:
        skill_text = get_extraction_skill("agent", agent_name)
    except MissingSkillError as exc:
        logger.debug("extract_agent_knowledge: skill missing — %s", exc)
        return ExtractionResult(scope_key="")

    existing_json = json.dumps(existing_facts, ensure_ascii=False, indent=2)
    prompt = skill_text.format(
        conversation=conversation,
        existing_facts_json=existing_json,
        agent_name=agent_name,
    )

    try:
        results = await asyncio.to_thread(_invoke_sync, prompt)
        if results:
            # Agent extraction returns a single result; take the first group.
            return results[0]
        return ExtractionResult(scope_key="")
    except json.JSONDecodeError as exc:
        logger.warning("extract_agent_knowledge: JSON parse error — %s", exc)
        return ExtractionResult(scope_key="")
    except Exception as exc:
        logger.exception("extract_agent_knowledge: unexpected error — %s", exc)
        return ExtractionResult(scope_key="")
