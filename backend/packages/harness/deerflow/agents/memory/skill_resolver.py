"""Resolution of memory extraction skill files.

Lookup order for ``get_extraction_skill(entity_type, agent_name)``:

  1. Agent-specific override:
       ``{base_dir}/agents/{agent_name}/memory/extract-{entity_type}.md``

  2. Public fallback (user / project only):
       ``{skills_root}/public/memory/extract-{entity_type}.md``

The ``"agent"`` entity type has no public fallback because agent knowledge
is domain-specific and must be curated per-agent.
"""

from __future__ import annotations

from deerflow.config.app_config import get_app_config
from deerflow.config.paths import get_paths

_ENTITY_TYPES = frozenset({"user", "project", "agent"})


class MissingSkillError(FileNotFoundError):
    """Raised when a required extraction skill file cannot be found."""


def get_extraction_skill(entity_type: str, agent_name: str) -> str:
    """Return the text of the extraction skill for *entity_type* and *agent_name*.

    Raises:
        ValueError: If *entity_type* is not one of ``"user"``, ``"project"``, ``"agent"``.
        MissingSkillError: If no suitable skill file can be found.
    """
    if entity_type not in _ENTITY_TYPES:
        raise ValueError(f"Unknown entity_type {entity_type!r}; expected one of {sorted(_ENTITY_TYPES)}")

    agent_path = get_paths().base_dir / "agents" / agent_name / "memory" / f"extract-{entity_type}.md"
    if agent_path.is_file():
        return agent_path.read_text(encoding="utf-8")

    if entity_type in ("user", "project"):
        public_path = get_app_config().skills.get_skills_path() / "public" / "memory" / f"extract-{entity_type}.md"
        if public_path.is_file():
            return public_path.read_text(encoding="utf-8")
        raise MissingSkillError(
            f"Public extraction skill not found: {public_path}. "
            f"Create skills/public/memory/extract-{entity_type}.md "
            f"or an agent-specific override at {agent_path}."
        )

    raise MissingSkillError(
        f"Agent '{agent_name}' must provide "
        f"{agent_path} (no public fallback exists for 'agent' entity type)."
    )
