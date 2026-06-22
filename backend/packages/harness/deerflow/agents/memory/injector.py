"""Multi-level memory injection for the MLM middleware.

Loads user, agent, and project memory rows from the DB, filters them by the
active scope dimensions, and formats them as a single text block suitable for
prepending to the conversation as a system reminder.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from deerflow.config.mlm_config import get_mlm_config
from deerflow.persistence.memory.repository import _confidence_sort_key, get_memory_repository

if TYPE_CHECKING:
    from deerflow.persistence.memory.model import MemoryAgentRow, MemoryProjectRow, MemoryUserRow

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Scope filtering
# ---------------------------------------------------------------------------


def filter_by_scope(rows: list, active_dims: set[str]) -> list:
    """Return rows whose scope_key requirements are all satisfied by *active_dims*.

    ``scope_key`` is a ``"+"``-separated list of required dimension tags, e.g.
    ``"agent:director+user:alice"``.  An empty scope_key (``""``) matches all.

    Args:
        rows: ORM rows with a ``scope_key`` attribute.
        active_dims: Currently active dimension tags, e.g. ``{"agent:director"}``.

    Returns:
        Subset of *rows* whose required tags are all present in *active_dims*.
    """
    result = []
    for row in rows:
        required = {s for s in row.scope_key.split("+") if s}
        if required.issubset(active_dims):
            result.append(row)
    return result


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def _format_facts(facts_json: str, *, indent: int = 2) -> str:
    """Return a bullet list of fact contents from a JSON-encoded facts array.

    Caps the rendered facts at ``MlmConfig.max_injection_facts`` per row,
    keeping the highest-confidence facts so a large stored row does not bloat
    the prompt. Facts without an explicit confidence rank at the neutral
    default (see ``_confidence_sort_key``).
    """
    try:
        facts = json.loads(facts_json)
    except (json.JSONDecodeError, TypeError):
        return ""
    if not isinstance(facts, list):
        return ""

    limit = get_mlm_config().max_injection_facts
    if len(facts) > limit:
        facts = sorted(facts, key=_confidence_sort_key, reverse=True)[:limit]

    lines = []
    for fact in facts:
        if not isinstance(fact, dict):
            continue
        content = fact.get("content", "")
        if content:
            lines.append(f"{'  ' * indent}- {content}")
    return "\n".join(lines)


def _format_user_rows(rows: "list[MemoryUserRow]") -> str:
    sections = []
    for row in rows:
        label = f"[scope: {row.scope_key}]" if row.scope_key else "[general]"
        parts = []
        if row.summary:
            parts.append(f"  Summary: {row.summary}")
        fact_text = _format_facts(row.facts, indent=1)
        if fact_text:
            parts.append(fact_text)
        if parts:
            sections.append(f"  {label}\n" + "\n".join(parts))
    if not sections:
        return ""
    return "## User Knowledge\n" + "\n".join(sections)


def _format_agent_row(row: "MemoryAgentRow") -> str:
    parts = []
    if row.summary:
        parts.append(f"  Summary: {row.summary}")
    fact_text = _format_facts(row.facts, indent=1)
    if fact_text:
        parts.append(fact_text)
    if not parts:
        return ""
    return "## Agent Knowledge\n" + "\n".join(parts)


def _format_project_rows(rows: "list[MemoryProjectRow]") -> str:
    sections = []
    for row in rows:
        label = f"[scope: {row.scope_key}]" if row.scope_key else "[general]"
        parts = []
        if row.summary:
            parts.append(f"  Summary: {row.summary}")
        fact_text = _format_facts(row.facts, indent=1)
        if fact_text:
            parts.append(fact_text)
        if parts:
            sections.append(f"  {label}\n" + "\n".join(parts))
    if not sections:
        return ""
    return "## Project Knowledge\n" + "\n".join(sections)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def build_injection(
    user_id: str | None,
    agent_name: str | None,
    project_id: str | None,
) -> str:
    """Load and format MLM memory rows for injection into the conversation.

    Returns an empty string when no relevant memory is found or when no DB is
    configured (repository is None).

    The injection text is structured as Markdown sections:
    - ``## User Knowledge`` — filtered user-scoped facts
    - ``## Agent Knowledge`` — global agent facts
    - ``## Project Knowledge`` — filtered project-scoped facts
    """
    repo = get_memory_repository()
    if repo is None:
        return ""

    active_dims: set[str] = set()
    if agent_name:
        active_dims.add(f"agent:{agent_name}")
    if user_id:
        active_dims.add(f"user:{user_id}")
    if project_id:
        active_dims.add(f"project:{project_id}")

    sections: list[str] = []

    if user_id:
        try:
            all_user_rows = await repo.load_user_scopes(user_id)
            relevant = filter_by_scope(all_user_rows, active_dims)
            if relevant:
                text = _format_user_rows(relevant)
                if text:
                    sections.append(text)
        except Exception:
            logger.exception("build_injection: failed to load user memory for %s", user_id)

    if agent_name:
        try:
            agent_row = await repo.load_agent(agent_name)
            if agent_row:
                text = _format_agent_row(agent_row)
                if text:
                    sections.append(text)
        except Exception:
            logger.exception("build_injection: failed to load agent memory for %s", agent_name)

    if project_id:
        try:
            all_proj_rows = await repo.load_project_scopes(project_id)
            relevant = filter_by_scope(all_proj_rows, active_dims)
            if relevant:
                text = _format_project_rows(relevant)
                if text:
                    sections.append(text)
        except Exception:
            logger.exception("build_injection: failed to load project memory for %s", project_id)

    return "\n\n".join(sections)
