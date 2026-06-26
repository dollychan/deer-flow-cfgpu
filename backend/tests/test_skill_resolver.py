"""Tests for deerflow.agents.memory.skill_resolver."""

from __future__ import annotations

from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestGetExtractionSkill:
    def test_invalid_entity_type_raises_value_error(self, tmp_path, monkeypatch):
        _patch_paths(tmp_path, monkeypatch)
        from deerflow.agents.memory.skill_resolver import get_extraction_skill

        with pytest.raises(ValueError, match="Unknown entity_type"):
            get_extraction_skill("unknown", "some-agent")

    def test_agent_specific_file_takes_priority(self, tmp_path, monkeypatch):
        _patch_paths(tmp_path, monkeypatch)
        agent_file = tmp_path / "base" / "agents" / "cf-dream" / "memory" / "extract-user.md"
        _write(agent_file, "agent-specific user prompt")

        public_file = tmp_path / "skills" / "public" / "memory" / "extract-user.md"
        _write(public_file, "public user prompt")

        from deerflow.agents.memory.skill_resolver import get_extraction_skill

        result = get_extraction_skill("user", "cf-dream")
        assert result == "agent-specific user prompt"

    def test_public_fallback_used_when_no_agent_file_for_user(self, tmp_path, monkeypatch):
        _patch_paths(tmp_path, monkeypatch)
        public_file = tmp_path / "skills" / "public" / "memory" / "extract-user.md"
        _write(public_file, "public user prompt")

        from deerflow.agents.memory.skill_resolver import get_extraction_skill

        result = get_extraction_skill("user", "cf-dream")
        assert result == "public user prompt"

    def test_public_fallback_used_when_no_agent_file_for_project(self, tmp_path, monkeypatch):
        _patch_paths(tmp_path, monkeypatch)
        public_file = tmp_path / "skills" / "public" / "memory" / "extract-project.md"
        _write(public_file, "public project prompt")

        from deerflow.agents.memory.skill_resolver import get_extraction_skill

        result = get_extraction_skill("project", "cf-dream")
        assert result == "public project prompt"

    def test_missing_public_skill_raises_missing_skill_error(self, tmp_path, monkeypatch):
        _patch_paths(tmp_path, monkeypatch)
        from deerflow.agents.memory.skill_resolver import MissingSkillError, get_extraction_skill

        with pytest.raises(MissingSkillError):
            get_extraction_skill("user", "cf-dream")

    def test_agent_entity_type_no_public_fallback(self, tmp_path, monkeypatch):
        _patch_paths(tmp_path, monkeypatch)
        # Put a file at the public location — should NOT be picked up for "agent" type
        public_file = tmp_path / "skills" / "public" / "memory" / "extract-agent.md"
        _write(public_file, "should not be used")

        from deerflow.agents.memory.skill_resolver import MissingSkillError, get_extraction_skill

        with pytest.raises(MissingSkillError, match="no public fallback"):
            get_extraction_skill("agent", "cf-dream")

    def test_agent_entity_type_uses_agent_specific_file(self, tmp_path, monkeypatch):
        _patch_paths(tmp_path, monkeypatch)
        agent_file = tmp_path / "base" / "agents" / "cf-dream" / "memory" / "extract-agent.md"
        _write(agent_file, "cf-dream agent prompt")

        from deerflow.agents.memory.skill_resolver import get_extraction_skill

        result = get_extraction_skill("agent", "cf-dream")
        assert result == "cf-dream agent prompt"

    def test_different_agents_resolve_independently(self, tmp_path, monkeypatch):
        _patch_paths(tmp_path, monkeypatch)
        _write(tmp_path / "base" / "agents" / "cf-dream" / "memory" / "extract-user.md", "cf-dream user skill")
        _write(tmp_path / "skills" / "public" / "memory" / "extract-user.md", "public user skill")

        from deerflow.agents.memory.skill_resolver import get_extraction_skill

        assert get_extraction_skill("user", "cf-dream") == "cf-dream user skill"
        assert get_extraction_skill("user", "coder") == "public user skill"


# ---------------------------------------------------------------------------
# Fixtures / patching helpers
# ---------------------------------------------------------------------------


def _patch_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Redirect get_paths() and get_app_config().skills.get_skills_path() to tmp_path."""
    base_dir = tmp_path / "base"
    skills_root = tmp_path / "skills"
    base_dir.mkdir(parents=True, exist_ok=True)
    skills_root.mkdir(parents=True, exist_ok=True)

    from deerflow.config import paths as paths_mod

    class _FakePaths:
        @property
        def base_dir(self):
            return base_dir

    monkeypatch.setattr(paths_mod, "_paths", _FakePaths())

    # Patch app_config's skills path
    import deerflow.agents.memory.skill_resolver as sr_mod

    monkeypatch.setattr(
        sr_mod,
        "get_app_config",
        lambda: _FakeAppConfig(skills_root),
    )


class _FakeSkillsConfig:
    def __init__(self, root: Path):
        self._root = root

    def get_skills_path(self) -> Path:
        return self._root


class _FakeAppConfig:
    def __init__(self, skills_root: Path):
        self.skills = _FakeSkillsConfig(skills_root)
