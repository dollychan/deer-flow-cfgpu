"""Tests for deerflow.agents.memory.extractor.

All LLM calls are mocked so no API key is needed.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from deerflow.agents.memory.extractor import ExtractionResult, _parse_response


# ---------------------------------------------------------------------------
# _parse_response (pure unit tests, no I/O)
# ---------------------------------------------------------------------------


class TestParseResponse:
    def test_parses_valid_json_array(self):
        raw = json.dumps([{"scope_key": "", "facts": [{"content": "fact A"}], "summary": "sum"}])
        result = _parse_response(raw)
        assert len(result) == 1
        assert result[0].scope_key == ""
        assert result[0].facts == [{"content": "fact A"}]
        assert result[0].summary == "sum"

    def test_strips_markdown_fences(self):
        raw = "```json\n" + json.dumps([{"scope_key": "agent:x", "facts": [], "summary": None}]) + "\n```"
        result = _parse_response(raw)
        assert result[0].scope_key == "agent:x"

    def test_empty_list_returns_empty(self):
        assert _parse_response("[]") == []

    def test_non_list_root_returns_empty(self):
        result = _parse_response(json.dumps({"scope_key": "", "facts": []}))
        assert result == []

    def test_missing_fields_default_gracefully(self):
        raw = json.dumps([{}])
        result = _parse_response(raw)
        assert result[0].scope_key == ""
        assert result[0].facts == []
        assert result[0].summary is None

    def test_multiple_scope_groups(self):
        data = [
            {"scope_key": "", "facts": [{"content": "A"}], "summary": "general"},
            {"scope_key": "agent:director", "facts": [{"content": "B"}], "summary": "agent"},
        ]
        result = _parse_response(json.dumps(data))
        assert len(result) == 2
        assert {r.scope_key for r in result} == {"", "agent:director"}

    def test_summary_none_when_falsy(self):
        raw = json.dumps([{"scope_key": "", "facts": [], "summary": ""}])
        result = _parse_response(raw)
        assert result[0].summary is None


# ---------------------------------------------------------------------------
# extract_user_knowledge
# ---------------------------------------------------------------------------


class TestExtractUserKnowledge:
    @pytest.mark.anyio
    async def test_returns_empty_on_empty_conversation(self, tmp_path, monkeypatch):
        _patch_resolver(tmp_path, monkeypatch, "user", "agent-a", "prompt {conversation} {existing_facts_json}")
        from deerflow.agents.memory.extractor import extract_user_knowledge

        result = await extract_user_knowledge([], "u1", "agent-a", {})
        assert result == []

    @pytest.mark.anyio
    async def test_returns_empty_when_skill_missing(self, tmp_path, monkeypatch):
        _patch_resolver_missing(monkeypatch)
        from deerflow.agents.memory.extractor import extract_user_knowledge

        messages = [_human("hello")]
        result = await extract_user_knowledge(messages, "u1", "agent-a", {})
        assert result == []

    @pytest.mark.anyio
    async def test_returns_extraction_results(self, tmp_path, monkeypatch):
        _patch_resolver(tmp_path, monkeypatch, "user", "agent-a", "{conversation}{existing_facts_json}")
        llm_response = json.dumps([{"scope_key": "", "facts": [{"content": "fact A"}], "summary": "s"}])
        _patch_model(monkeypatch, llm_response)

        from deerflow.agents.memory.extractor import extract_user_knowledge

        result = await extract_user_knowledge([_human("hi"), _ai("hello")], "u1", "agent-a", {})
        assert len(result) == 1
        assert result[0].facts == [{"content": "fact A"}]

    @pytest.mark.anyio
    async def test_returns_empty_on_json_decode_error(self, tmp_path, monkeypatch):
        _patch_resolver(tmp_path, monkeypatch, "user", "agent-a", "{conversation}{existing_facts_json}")
        _patch_model(monkeypatch, "not json")

        from deerflow.agents.memory.extractor import extract_user_knowledge

        result = await extract_user_knowledge([_human("hi"), _ai("ok")], "u1", "agent-a", {})
        assert result == []


# ---------------------------------------------------------------------------
# extract_project_knowledge
# ---------------------------------------------------------------------------


class TestExtractProjectKnowledge:
    @pytest.mark.anyio
    async def test_returns_extraction_results(self, tmp_path, monkeypatch):
        _patch_resolver(
            tmp_path, monkeypatch, "project", "agent-a",
            "{conversation}{existing_facts_json}{project_id}{user_id}{agent_name}"
        )
        llm_response = json.dumps([{"scope_key": "", "facts": [{"content": "proj fact"}], "summary": None}])
        _patch_model(monkeypatch, llm_response)

        from deerflow.agents.memory.extractor import extract_project_knowledge

        result = await extract_project_knowledge([_human("x"), _ai("y")], "proj-1", "agent-a", "u1", {})
        assert result[0].facts == [{"content": "proj fact"}]

    @pytest.mark.anyio
    async def test_returns_empty_when_skill_missing(self, tmp_path, monkeypatch):
        _patch_resolver_missing(monkeypatch)
        from deerflow.agents.memory.extractor import extract_project_knowledge

        result = await extract_project_knowledge([_human("x"), _ai("y")], "proj-1", "agent-a", None, {})
        assert result == []


# ---------------------------------------------------------------------------
# extract_agent_knowledge
# ---------------------------------------------------------------------------


class TestExtractAgentKnowledge:
    @pytest.mark.anyio
    async def test_returns_empty_result_when_skill_missing(self, tmp_path, monkeypatch):
        _patch_resolver_missing(monkeypatch)
        from deerflow.agents.memory.extractor import extract_agent_knowledge

        result = await extract_agent_knowledge([_human("x"), _ai("y")], "director", [])
        assert result == ExtractionResult(scope_key="")

    @pytest.mark.anyio
    async def test_returns_first_result_group(self, tmp_path, monkeypatch):
        _patch_resolver(
            tmp_path, monkeypatch, "agent", "director",
            "{conversation}{existing_facts_json}{agent_name}"
        )
        llm_response = json.dumps([{"scope_key": "", "facts": [{"content": "agent fact"}], "summary": "s"}])
        _patch_model(monkeypatch, llm_response)

        from deerflow.agents.memory.extractor import extract_agent_knowledge

        result = await extract_agent_knowledge([_human("x"), _ai("y")], "director", [])
        assert result.facts == [{"content": "agent fact"}]

    @pytest.mark.anyio
    async def test_returns_empty_on_empty_conversation(self, tmp_path, monkeypatch):
        _patch_resolver(tmp_path, monkeypatch, "agent", "director", "{conversation}{existing_facts_json}{agent_name}")
        from deerflow.agents.memory.extractor import extract_agent_knowledge

        result = await extract_agent_knowledge([], "director", [])
        assert result == ExtractionResult(scope_key="")


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


def _human(text: str):
    return SimpleNamespace(type="human", content=text)


def _ai(text: str):
    return SimpleNamespace(type="ai", content=text)


def _patch_resolver(tmp_path: Path, monkeypatch, entity_type: str, agent_name: str, skill_text: str):
    """Make get_extraction_skill return skill_text for the given entity_type/agent_name."""
    import deerflow.agents.memory.extractor as ext_mod

    def _resolver(et, an):
        if et == entity_type and an == agent_name:
            return skill_text
        from deerflow.agents.memory.skill_resolver import MissingSkillError
        raise MissingSkillError(f"no skill for {et}/{an}")

    monkeypatch.setattr(ext_mod, "get_extraction_skill", _resolver)


def _patch_resolver_missing(monkeypatch):
    """Make get_extraction_skill always raise MissingSkillError."""
    import deerflow.agents.memory.extractor as ext_mod
    from deerflow.agents.memory.skill_resolver import MissingSkillError

    monkeypatch.setattr(ext_mod, "get_extraction_skill", lambda *_: (_ for _ in ()).throw(MissingSkillError("missing")))


def _patch_model(monkeypatch, response_text: str):
    """Patch _invoke_sync to return a parsed result from response_text without hitting the LLM."""
    import deerflow.agents.memory.extractor as ext_mod

    def _fake_invoke(prompt: str):
        return _parse_response(response_text)

    monkeypatch.setattr(ext_mod, "_invoke_sync", _fake_invoke)
