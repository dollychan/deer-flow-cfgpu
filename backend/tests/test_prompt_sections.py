"""Tests for per-agent optional prompt-section selection.

Covers ``AgentConfig.prompt_sections`` driving ``apply_prompt_template`` to drop
optional/advisory sections (citations, clarification) while leaving core/infra
sections and the default-agent output untouched.
"""

from __future__ import annotations

from deerflow.agents.lead_agent.prompt import (
    OPTIONAL_PROMPT_SECTIONS,
    apply_prompt_template,
)
from deerflow.config.agents_config import AgentConfig, PromptSectionsConfig


def test_default_agent_keeps_all_sections() -> None:
    prompt = apply_prompt_template()
    assert "<citations>" in prompt
    assert "<clarification_system>" in prompt


def test_excluding_citations_removes_only_that_block() -> None:
    full = apply_prompt_template()
    trimmed = apply_prompt_template(excluded_sections=frozenset({"citations"}))

    assert "<citations>" not in trimmed
    assert "</citations>" not in trimmed
    # Clarification (another optional block) and core blocks survive.
    assert "<clarification_system>" in trimmed
    assert "<role>" in trimmed
    assert "<working_directory" in trimmed
    assert len(trimmed) < len(full)


def test_excluding_multiple_sections() -> None:
    trimmed = apply_prompt_template(excluded_sections=frozenset({"citations", "clarification"}))
    assert "<citations>" not in trimmed
    assert "<clarification_system>" not in trimmed
    # Core/infra sections remain.
    assert "<role>" in trimmed
    assert "<response_style>" in trimmed
    assert "<critical_reminders>" in trimmed


def test_no_exclusion_is_byte_identical_to_empty_frozenset() -> None:
    # Both the default arg and an explicit empty set must skip stripping entirely.
    assert apply_prompt_template() == apply_prompt_template(excluded_sections=frozenset())


def test_unknown_section_name_is_ignored() -> None:
    # Unknown names must not crash and must not alter the prompt.
    baseline = apply_prompt_template()
    assert apply_prompt_template(excluded_sections=frozenset({"does-not-exist"})) == baseline


def test_only_advisory_sections_are_controllable() -> None:
    # Guard against accidentally exposing core/infra sections as droppable.
    assert set(OPTIONAL_PROMPT_SECTIONS) == {"citations", "clarification"}


def test_agent_config_prompt_sections_defaults_to_none() -> None:
    cfg = AgentConfig(name="plain")
    assert cfg.prompt_sections is None


def test_agent_config_prompt_sections_parses_exclude_list() -> None:
    cfg = AgentConfig(name="director", prompt_sections={"exclude": ["citations", "clarification"]})
    assert isinstance(cfg.prompt_sections, PromptSectionsConfig)
    assert cfg.prompt_sections.exclude == ["citations", "clarification"]


def test_prompt_sections_config_exclude_defaults_empty() -> None:
    assert PromptSectionsConfig().exclude == []
