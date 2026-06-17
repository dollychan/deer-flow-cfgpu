"""Tests for agent-level skill whitelist: ``skill_dirs`` expansion + ``_available_skill_names``.

Covers (see cfgpu-docs/config.md "agent 白名单"):
- ``_expand_skill_dirs``: category-relative prefix matching, cross-category vs ``public/``/``custom/``
  prefix disambiguation, subtree nesting, exact match (no false-prefix), category-only entry.
- ``_available_skill_names``: both-None → full (None), union of ``skills`` (name) and ``skill_dirs``
  (path), ``skills: []`` suppression, bootstrap short-circuit.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from deerflow.agents.lead_agent.agent import _available_skill_names, _expand_skill_dirs
from deerflow.config.agents_config import AgentConfig
from deerflow.skills.types import Skill, SkillCategory


def _skill(name: str, category: SkillCategory, rel: str) -> Skill:
    return Skill(
        name=name,
        description="d",
        license=None,
        skill_dir=Path("/x"),
        skill_file=Path("/x/SKILL.md"),
        relative_path=Path(rel),
        category=category,
        allowed_tools=None,
        enabled=True,
    )


# Pool: A/E custom, B/D public; A&B share the director/public subtree across categories.
_POOL = [
    _skill("A", SkillCategory.CUSTOM, "director/public/seedance"),
    _skill("B", SkillCategory.PUBLIC, "director/public/poster"),
    _skill("C", SkillCategory.CUSTOM, "video/clipx"),
    _skill("D", SkillCategory.PUBLIC, "image/posterx"),
    _skill("E", SkillCategory.CUSTOM, "solo"),
]


@pytest.fixture
def _patch_pool(monkeypatch):
    monkeypatch.setattr(
        "deerflow.agents.lead_agent.prompt.get_enabled_skills_for_config",
        lambda app_config=None: list(_POOL),
    )


# ── _expand_skill_dirs ───────────────────────────────────────────────────────────


def test_category_prefix_pins_to_one_category(_patch_pool):
    assert _expand_skill_dirs(["custom/director/public"], app_config=None) == {"A"}


def test_bare_prefix_matches_across_categories(_patch_pool):
    assert _expand_skill_dirs(["director/public"], app_config=None) == {"A", "B"}


def test_subtree_prefix_collects_nested(_patch_pool):
    assert _expand_skill_dirs(["director"], app_config=None) == {"A", "B"}


def test_single_segment_prefix(_patch_pool):
    assert _expand_skill_dirs(["video"], app_config=None) == {"C"}


def test_exact_skill_path_match(_patch_pool):
    assert _expand_skill_dirs(["solo"], app_config=None) == {"E"}


def test_partial_segment_is_not_a_prefix(_patch_pool):
    # "sol" must not match "solo" (only whole-segment prefixes count).
    assert _expand_skill_dirs(["sol"], app_config=None) == set()


def test_category_only_entry_selects_whole_category(_patch_pool):
    assert _expand_skill_dirs(["custom"], app_config=None) == {"A", "C", "E"}


def test_empty_and_blank_entries(_patch_pool):
    assert _expand_skill_dirs([], app_config=None) == set()
    assert _expand_skill_dirs(["", "  ", "/"], app_config=None) == set()


def test_leading_trailing_slashes_tolerated(_patch_pool):
    assert _expand_skill_dirs(["/video/"], app_config=None) == {"C"}


# ── _available_skill_names ───────────────────────────────────────────────────────


def _cfg(skills=None, skill_dirs=None) -> AgentConfig:
    return AgentConfig(name="t", skills=skills, skill_dirs=skill_dirs)


def test_both_none_means_full_pool(_patch_pool):
    assert _available_skill_names(_cfg(), is_bootstrap=False, app_config=None) is None


def test_skills_only_is_name_set(_patch_pool):
    assert _available_skill_names(_cfg(skills=["X"]), is_bootstrap=False, app_config=None) == {"X"}


def test_empty_skills_suppresses(_patch_pool):
    # skills: [] (skill_dirs omitted) → active but empty whitelist → no skills.
    assert _available_skill_names(_cfg(skills=[]), is_bootstrap=False, app_config=None) == set()


def test_union_of_skills_and_skill_dirs(_patch_pool):
    result = _available_skill_names(_cfg(skills=["X"], skill_dirs=["video"]), is_bootstrap=False, app_config=None)
    assert result == {"X", "C"}


def test_skill_dirs_only(_patch_pool):
    result = _available_skill_names(_cfg(skill_dirs=["custom/director/public"]), is_bootstrap=False, app_config=None)
    assert result == {"A"}


def test_none_agent_config_is_full_pool(_patch_pool):
    assert _available_skill_names(None, is_bootstrap=False, app_config=None) is None


def test_bootstrap_short_circuits(_patch_pool):
    result = _available_skill_names(_cfg(skill_dirs=["video"]), is_bootstrap=True, app_config=None)
    assert isinstance(result, set) and "C" not in result  # bootstrap set, not the dir expansion
