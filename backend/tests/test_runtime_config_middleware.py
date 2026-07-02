"""Tests for RuntimeConfigMiddleware + config.skills/config.models transport.

Covers:
- config.skills (Model B): eager full-content injection, str-as-single-skill, missing-skill
  strategy B (warn + visible note), partial found/missing, idempotency, async offload path.
- config.models (方案 3): in manual mode (type == "manual") the cfdream generate-tool model arg is
  constrained to the client-allowed range in after_model — keep LLM choice when in range, else fall
  back to the allowed range; in auto mode the LLM's choice stands. Empty/null model is always
  normalized to "auto" (null-safety), independent of the type gate.
- consumer `_build_config` transport (config.skills/config.models → runtime.context).

See cfgpu-docs/config.md "config.skills — Model B" / "config.models — 方案 3".
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from langchain_core.messages import AIMessage, HumanMessage

from deerflow.agents.middlewares.runtime_config_middleware import (
    _DEFAULT_MODEL_BINDINGS,
    _MODELS_REMINDER_KEY,
    _SKILL_REMINDER_KEY,
    RuntimeConfigMiddleware,
    _allowed_models_for_task,
    _is_manual_selection,
    _normalize_empty_model,
    _restrict_model_arg,
    _task_type_for_tool,
)

# cf-dream binds all three media types in its config.yaml; construct with the same bindings so the
# manual-whitelist injection tests exercise image / video / audio like the real agent.
_ALL_MEDIA_BINDINGS = {"*generate_image": "image", "*generate_video": "video", "*generate_audio": "audio"}

_REMINDER_TAG = "<system-reminder>"
_SKILL_NAME = "Seedance 2.0 视频创作"
_SKILL_MARKER = "DISTINCTIVE_WORKFLOW_MARKER_分镜"


# ── helpers ────────────────────────────────────────────────────────────────────


def _write_skill(root: Path, dir_name: str, name: str, body_marker: str) -> None:
    skill_dir = root / "public" / dir_name
    skill_dir.mkdir(parents=True, exist_ok=True)
    (skill_dir / "SKILL.md").write_text(
        f"---\nname: {name}\ndescription: 用该模型做创作的工作流\n---\n\n# 工作流\n{body_marker}\n",
        encoding="utf-8",
    )


def _patch_storage(monkeypatch, root: Path) -> None:
    from deerflow.skills.storage.local_skill_storage import LocalSkillStorage

    storage = LocalSkillStorage(host_path=str(root), container_path="/mnt/skills")
    monkeypatch.setattr(
        "deerflow.skills.storage.get_or_new_skill_storage",
        lambda **kwargs: storage,
    )


def _runtime(skills) -> SimpleNamespace:
    return SimpleNamespace(context={} if skills is None else {"skills": skills})


def _skill_reminder(content: str = "<system-reminder>x</system-reminder>") -> HumanMessage:
    return HumanMessage(content=content, additional_kwargs={"hide_from_ui": True, _SKILL_REMINDER_KEY: True})


# ── config.skills: no-op when nothing selected ───────────────────────────────────


def test_no_skills_is_noop(monkeypatch, tmp_path):
    _patch_storage(monkeypatch, tmp_path)
    mw = RuntimeConfigMiddleware()
    state = {"messages": [HumanMessage(content="hi")]}
    assert mw.before_agent(state, _runtime(None)) is None
    assert mw.before_agent(state, _runtime([])) is None


# ── config.skills: eager injection of selected skill full content ───────────────


def test_injects_selected_skill_full_content(monkeypatch, tmp_path):
    _write_skill(tmp_path, "seedance", _SKILL_NAME, _SKILL_MARKER)
    _patch_storage(monkeypatch, tmp_path)
    mw = RuntimeConfigMiddleware()
    state = {"messages": [HumanMessage(content="拍一个短片")]}

    result = mw.before_agent(state, _runtime([_SKILL_NAME]))

    assert result is not None
    msgs = result["messages"]
    assert len(msgs) == 1
    reminder = msgs[0]
    assert isinstance(reminder, HumanMessage)
    assert reminder.additional_kwargs.get(_SKILL_REMINDER_KEY) is True
    assert reminder.additional_kwargs.get("hide_from_ui") is True
    assert _REMINDER_TAG in reminder.content
    assert _SKILL_NAME in reminder.content
    assert _SKILL_MARKER in reminder.content  # full SKILL.md body inlined
    assert "MUST follow" in reminder.content
    assert "拍一个短片" not in reminder.content  # reminder only, not user text


def test_accepts_single_skill_as_string(monkeypatch, tmp_path):
    _write_skill(tmp_path, "seedance", _SKILL_NAME, _SKILL_MARKER)
    _patch_storage(monkeypatch, tmp_path)
    mw = RuntimeConfigMiddleware()
    state = {"messages": [HumanMessage(content="go")]}

    result = mw.before_agent(state, _runtime(_SKILL_NAME))  # str, not list

    assert result is not None
    assert _SKILL_MARKER in result["messages"][0].content


# ── config.skills: strategy B — missing skill → warn + visible note ─────────────


def test_missing_skill_injects_not_found_note(monkeypatch, tmp_path, caplog):
    _patch_storage(monkeypatch, tmp_path)  # empty skills dir
    mw = RuntimeConfigMiddleware()
    state = {"messages": [HumanMessage(content="go")]}

    import logging

    with caplog.at_level(logging.WARNING):
        result = mw.before_agent(state, _runtime(["Ghost Skill"]))

    assert result is not None  # run continues, not aborted
    content = result["messages"][0].content
    assert "NOT found" in content
    assert "Ghost Skill" in content
    assert any("not found" in r.message.lower() for r in caplog.records)


def test_partial_found_and_missing(monkeypatch, tmp_path):
    _write_skill(tmp_path, "seedance", _SKILL_NAME, _SKILL_MARKER)
    _patch_storage(monkeypatch, tmp_path)
    mw = RuntimeConfigMiddleware()
    state = {"messages": [HumanMessage(content="go")]}

    result = mw.before_agent(state, _runtime([_SKILL_NAME, "Ghost"]))

    content = result["messages"][0].content
    assert _SKILL_MARKER in content  # found one inlined
    assert "NOT found" in content and "Ghost" in content  # other flagged


# ── config.skills: out-of-whitelist defensive check (strategy B, best-effort) ────


def test_out_of_whitelist_skill_injected_but_flagged(monkeypatch, tmp_path, caplog):
    # The skill exists in the global pool but is NOT in this agent's whitelist.
    _write_skill(tmp_path, "seedance", _SKILL_NAME, _SKILL_MARKER)
    _patch_storage(monkeypatch, tmp_path)
    mw = RuntimeConfigMiddleware(available_skills={"Some Other Skill"})
    state = {"messages": [HumanMessage(content="go")]}

    import logging

    with caplog.at_level(logging.WARNING):
        result = mw.before_agent(state, _runtime([_SKILL_NAME]))

    assert result is not None  # run continues
    content = result["messages"][0].content
    assert _SKILL_MARKER in content  # best-effort: full body still injected
    assert "outside this agent's normal scope" in content  # annotated
    assert "NOT found" not in content  # it WAS found, just out of scope
    assert any("outside this agent's whitelist" in r.message.lower() for r in caplog.records)


def test_in_whitelist_skill_not_flagged(monkeypatch, tmp_path):
    _write_skill(tmp_path, "seedance", _SKILL_NAME, _SKILL_MARKER)
    _patch_storage(monkeypatch, tmp_path)
    mw = RuntimeConfigMiddleware(available_skills={_SKILL_NAME})
    state = {"messages": [HumanMessage(content="go")]}

    result = mw.before_agent(state, _runtime([_SKILL_NAME]))

    content = result["messages"][0].content
    assert _SKILL_MARKER in content
    assert "outside this agent's normal scope" not in content  # in whitelist → no flag


def test_none_whitelist_imposes_no_constraint(monkeypatch, tmp_path):
    # available_skills=None means "full pool" → never flagged as out of scope.
    _write_skill(tmp_path, "seedance", _SKILL_NAME, _SKILL_MARKER)
    _patch_storage(monkeypatch, tmp_path)
    mw = RuntimeConfigMiddleware(available_skills=None)
    state = {"messages": [HumanMessage(content="go")]}

    result = mw.before_agent(state, _runtime([_SKILL_NAME]))

    content = result["messages"][0].content
    assert _SKILL_MARKER in content
    assert "outside this agent's normal scope" not in content


# ── config.skills: idempotency ───────────────────────────────────────────────────


def test_not_reinjected_when_turn_already_has_reminder(monkeypatch, tmp_path):
    _write_skill(tmp_path, "seedance", _SKILL_NAME, _SKILL_MARKER)
    _patch_storage(monkeypatch, tmp_path)
    mw = RuntimeConfigMiddleware()
    # current turn already carries a skill reminder after the user message
    state = {"messages": [HumanMessage(content="go"), _skill_reminder()]}

    assert mw.before_agent(state, _runtime([_SKILL_NAME])) is None


def test_reinjects_on_new_turn(monkeypatch, tmp_path):
    _write_skill(tmp_path, "seedance", _SKILL_NAME, _SKILL_MARKER)
    _patch_storage(monkeypatch, tmp_path)
    mw = RuntimeConfigMiddleware()
    # an older turn had a reminder; a brand-new user turn arrives after the AI reply
    state = {
        "messages": [
            HumanMessage(content="turn-1"),
            _skill_reminder(),
            AIMessage(content="ok"),
            HumanMessage(content="turn-2"),
        ]
    }

    result = mw.before_agent(state, _runtime([_SKILL_NAME]))
    assert result is not None
    assert _SKILL_MARKER in result["messages"][0].content


def test_pure_resume_without_user_message_is_noop(monkeypatch, tmp_path):
    _write_skill(tmp_path, "seedance", _SKILL_NAME, _SKILL_MARKER)
    _patch_storage(monkeypatch, tmp_path)
    mw = RuntimeConfigMiddleware()
    # no genuine user message to anchor to (only hidden reminder + tool/ai)
    state = {"messages": [_skill_reminder(), AIMessage(content="...")]}

    assert mw.before_agent(state, _runtime([_SKILL_NAME])) is None


# ── config.skills: async path mirrors sync ───────────────────────────────────────


@pytest.mark.asyncio
async def test_async_path_injects(monkeypatch, tmp_path):
    _write_skill(tmp_path, "seedance", _SKILL_NAME, _SKILL_MARKER)
    _patch_storage(monkeypatch, tmp_path)
    mw = RuntimeConfigMiddleware()
    state = {"messages": [HumanMessage(content="go")]}

    result = await mw.abefore_agent(state, _runtime([_SKILL_NAME]))

    assert result is not None
    assert _SKILL_MARKER in result["messages"][0].content


@pytest.mark.asyncio
async def test_async_noop_when_empty(monkeypatch, tmp_path):
    _patch_storage(monkeypatch, tmp_path)
    mw = RuntimeConfigMiddleware()
    state = {"messages": [HumanMessage(content="go")]}
    assert await mw.abefore_agent(state, _runtime(None)) is None


# ── config.models: pure helpers (方案 3) ────────────────────────────────────────


def test_task_type_for_tool_matches_native_and_mcp_names():
    b = _DEFAULT_MODEL_BINDINGS
    assert _task_type_for_tool("generate_image", b) == "image"
    assert _task_type_for_tool("cfdream_generate_image", b) == "image"  # MCP prefix matches *generate_image
    assert _task_type_for_tool("generate_video", b) == "video"
    assert _task_type_for_tool("cfdream_generate_video", b) == "video"
    assert _task_type_for_tool("list_models", b) is None
    assert _task_type_for_tool("", b) is None


def test_task_type_for_tool_honors_custom_bindings():
    bindings = {"my_img_*": "image", "*video*": "video"}
    assert _task_type_for_tool("my_img_tool", bindings) == "image"
    assert _task_type_for_tool("some_video_gen", bindings) == "video"
    # default markers no longer apply when custom bindings are supplied
    assert _task_type_for_tool("generate_image", bindings) is None


def test_allowed_models_extracts_per_task_type():
    cfg = {
        "type": "auto",
        "content": [
            {"type": "image", "model_names": ["doubao-seedream-5-0-lite", "seedream"]},
            {"type": "video", "model_names": ["wan-2-0-fast"]},
        ],
    }
    assert _allowed_models_for_task(cfg, "image") == ["doubao-seedream-5-0-lite", "seedream"]
    assert _allowed_models_for_task(cfg, "video") == ["wan-2-0-fast"]
    # absent / malformed → empty
    assert _allowed_models_for_task(cfg, "audio") == []
    assert _allowed_models_for_task(None, "image") == []
    assert _allowed_models_for_task({"content": "nope"}, "image") == []


def test_restrict_model_arg_keeps_in_range_choice():
    allowed = ["wan-2-0", "wan-2-0-fast"]
    # LLM picked an in-range model → unchanged
    assert _restrict_model_arg({"model": "wan-2-0"}, allowed) is None


def test_restrict_model_arg_auto_falls_back_to_range():
    allowed = ["wan-2-0", "wan-2-0-fast"]
    out = _restrict_model_arg({"model": "auto"}, allowed)
    assert out is not None and out["model"] == allowed  # whole range passed to router


def test_restrict_model_arg_out_of_range_falls_back_to_range():
    allowed = ["wan-2-0", "wan-2-0-fast"]
    out = _restrict_model_arg({"model": "some-forbidden-model"}, allowed)
    assert out["model"] == allowed


def test_restrict_model_arg_single_allowed_pins():
    allowed = ["wan-2-0-fast"]
    out = _restrict_model_arg({"model": "auto"}, allowed)
    assert out["model"] == "wan-2-0-fast"  # single id collapses to string (manual-pin behaviour)


def test_restrict_model_arg_list_intersects():
    allowed = ["a", "b", "c"]
    out = _restrict_model_arg({"model": ["b", "z"]}, allowed)
    assert out["model"] == "b"  # intersection {b}, collapsed to string


# ── config.models: _normalize_empty_model (null/empty → "auto", no whitelist needed) ──


@pytest.mark.parametrize(
    "args",
    [
        {"prompt": "x"},  # model key absent
        {"prompt": "x", "model": None},  # explicit null
        {"prompt": "x", "model": ""},  # empty string
        {"prompt": "x", "model": "   "},  # whitespace only
        {"prompt": "x", "model": []},  # empty list
        {"prompt": "x", "model": ["", None]},  # all-empty list
    ],
)
def test_normalize_empty_model_defaults_to_auto(args):
    out = _normalize_empty_model(args)
    assert out is not None
    assert out["model"] == "auto"
    assert out["prompt"] == "x"  # other args preserved


@pytest.mark.parametrize(
    "model",
    ["auto", "wan-2-0-fast", ["wan-2-0", "wan-2-0-fast"], ["wan-2-0", ""]],
)
def test_normalize_empty_model_leaves_usable_value(model):
    # already "auto" or a real id (incl. a list with at least one real id) → no rewrite
    assert _normalize_empty_model({"model": model}) is None


# ── config.models: _is_manual_selection (type gate) ─────────────────────────────


@pytest.mark.parametrize(
    "cfg,expected",
    [
        ({"type": "manual", "content": []}, True),
        ({"type": "MANUAL"}, True),  # case-insensitive
        ({"type": " manual "}, True),  # whitespace-tolerant
        ({"type": "auto", "content": []}, False),
        ({"content": []}, False),  # type absent → auto
        ({"type": "weird"}, False),  # unknown → auto
        (None, False),
        ("not a dict", False),
    ],
)
def test_is_manual_selection(cfg, expected):
    assert _is_manual_selection(cfg) is expected


# ── config.models: after_model integration (runs before HAM) ─────────────────────


def _models_cfg(task_type: str, names: list[str], mode: str = "manual") -> dict:
    # Default to manual: the whitelist constraint only applies in manual mode (auto keeps the
    # LLM's choice), so most integration tests below exercise the manual/constrain path.
    return {"type": mode, "content": [{"type": task_type, "model_names": names}]}


def _models_runtime(models_cfg) -> SimpleNamespace:
    return SimpleNamespace(context=({} if models_cfg is None else {"models": models_cfg}))


def _ai_with_tool_call(name: str, args: dict) -> AIMessage:
    return AIMessage(content="", tool_calls=[{"name": name, "args": args, "id": "tc1"}])


def _model_of(result: dict) -> object:
    return result["messages"][0].tool_calls[0]["args"]["model"]


def test_after_model_constrains_out_of_range_model():
    mw = RuntimeConfigMiddleware()
    state = {"messages": [_ai_with_tool_call("cfdream_generate_video", {"prompt": "x", "model": "forbidden"})]}

    result = mw.after_model(state, _models_runtime(_models_cfg("video", ["wan-2-0-fast"])))

    assert result is not None
    assert _model_of(result) == "wan-2-0-fast"  # forced into allowed range
    # original AIMessage id preserved so add_messages replaces it in place
    assert result["messages"][0].id == state["messages"][0].id


def test_after_model_passes_through_when_in_range():
    mw = RuntimeConfigMiddleware()
    state = {"messages": [_ai_with_tool_call("generate_image", {"prompt": "x", "model": "seedream"})]}

    result = mw.after_model(state, _models_runtime(_models_cfg("image", ["seedream", "other"])))

    assert result is None  # in-range → no rewrite


def test_after_model_noop_for_non_generate_tool():
    mw = RuntimeConfigMiddleware()
    state = {"messages": [_ai_with_tool_call("list_models", {"model": "forbidden"})]}

    assert mw.after_model(state, _models_runtime(_models_cfg("image", ["seedream"]))) is None


def test_after_model_noop_when_no_models_config():
    # No whitelist + a real model id → left untouched (router/tool will honour it).
    mw = RuntimeConfigMiddleware()
    state = {"messages": [_ai_with_tool_call("generate_image", {"model": "anything"})]}

    assert mw.after_model(state, _models_runtime(None)) is None


@pytest.mark.parametrize("model_arg", [None, "", "   ", []])
def test_after_model_defaults_null_model_to_auto_without_whitelist(model_arg):
    # No config.models at all, but an empty/null generate model must not reach the tool as null.
    mw = RuntimeConfigMiddleware()
    state = {"messages": [_ai_with_tool_call("cfdream_generate_image", {"prompt": "x", "model": model_arg})]}

    result = mw.after_model(state, _models_runtime(None))

    assert result is not None
    assert _model_of(result) == "auto"
    assert result["messages"][0].id == state["messages"][0].id  # in-place replace


def test_after_model_defaults_missing_model_to_auto_without_whitelist():
    mw = RuntimeConfigMiddleware()
    state = {"messages": [_ai_with_tool_call("generate_video", {"prompt": "x"})]}  # no model key

    result = mw.after_model(state, _models_runtime(None))

    assert result is not None and _model_of(result) == "auto"


def test_after_model_null_model_default_skips_non_generate_tool():
    # Normalization is scoped to generate tools via bindings — other tools' model arg is left alone.
    mw = RuntimeConfigMiddleware()
    state = {"messages": [_ai_with_tool_call("list_models", {"model": None})]}

    assert mw.after_model(state, _models_runtime(None)) is None


def test_after_model_noop_when_no_tool_calls():
    mw = RuntimeConfigMiddleware()
    state = {"messages": [AIMessage(content="just text")]}

    assert mw.after_model(state, _models_runtime(_models_cfg("image", ["seedream"]))) is None


def test_after_model_constrains_only_generate_calls_in_mixed_message():
    mw = RuntimeConfigMiddleware()
    ai = AIMessage(
        content="",
        tool_calls=[
            {"name": "list_models", "args": {"task_type": "video"}, "id": "a"},
            {"name": "generate_video", "args": {"model": "auto"}, "id": "b"},
        ],
    )
    result = mw.after_model({"messages": [ai]}, _models_runtime(_models_cfg("video", ["wan-2-0", "wan-2-0-fast"])))

    tcs = {tc["id"]: tc for tc in result["messages"][0].tool_calls}
    assert tcs["a"]["args"] == {"task_type": "video"}  # untouched
    assert tcs["b"]["args"]["model"] == ["wan-2-0", "wan-2-0-fast"]  # auto → full range


@pytest.mark.asyncio
async def test_aafter_model_constrains_model():
    mw = RuntimeConfigMiddleware()
    state = {"messages": [_ai_with_tool_call("generate_video", {"model": "auto"})]}

    result = await mw.aafter_model(state, _models_runtime(_models_cfg("video", ["wan-2-0", "wan-2-0-fast"])))

    assert _model_of(result) == ["wan-2-0", "wan-2-0-fast"]  # auto → full range


def test_after_model_custom_bindings_replace_default():
    # A configured model_bindings replaces the built-in default entirely.
    mw = RuntimeConfigMiddleware(model_bindings={"render_*": "image"})
    cfg = _models_cfg("image", ["seedream"])

    # custom-bound tool is constrained
    r1 = mw.after_model({"messages": [_ai_with_tool_call("render_pic", {"model": "auto"})]}, _models_runtime(cfg))
    assert _model_of(r1) == "seedream"

    # default-named generate_image is NO LONGER bound (default replaced) → untouched
    r2 = mw.after_model({"messages": [_ai_with_tool_call("generate_image", {"model": "anything"})]}, _models_runtime(cfg))
    assert r2 is None


# ── config.models: type gate — manual constrains, auto keeps LLM's choice ────────


def test_after_model_manual_constrains_out_of_range_model():
    # Explicit type=manual → whitelist enforced, out-of-range model forced into the range.
    mw = RuntimeConfigMiddleware()
    state = {"messages": [_ai_with_tool_call("cfdream_generate_video", {"prompt": "x", "model": "forbidden"})]}

    result = mw.after_model(state, _models_runtime(_models_cfg("video", ["wan-2-0-fast"], mode="manual")))

    assert result is not None
    assert _model_of(result) == "wan-2-0-fast"


def test_after_model_auto_keeps_out_of_range_llm_choice():
    # type=auto → whitelist NOT enforced; the LLM's own (out-of-range) pick stands untouched.
    mw = RuntimeConfigMiddleware()
    state = {"messages": [_ai_with_tool_call("cfdream_generate_video", {"prompt": "x", "model": "forbidden"})]}

    result = mw.after_model(state, _models_runtime(_models_cfg("video", ["wan-2-0-fast"], mode="auto")))

    assert result is None  # LLM choice preserved, no rewrite


def test_after_model_auto_keeps_llm_choice_over_configured_range():
    # Even a concrete in-list vs out-of-list distinction is irrelevant in auto: LLM decides.
    mw = RuntimeConfigMiddleware()
    state = {"messages": [_ai_with_tool_call("generate_image", {"model": "some-other-model"})]}

    result = mw.after_model(state, _models_runtime(_models_cfg("image", ["seedream"], mode="auto")))

    assert result is None


def test_after_model_missing_type_defaults_to_auto():
    # A config.models object without a top-level type behaves as auto (no constraint).
    mw = RuntimeConfigMiddleware()
    cfg = {"content": [{"type": "video", "model_names": ["wan-2-0-fast"]}]}
    state = {"messages": [_ai_with_tool_call("generate_video", {"model": "forbidden"})]}

    assert mw.after_model(state, _models_runtime(cfg)) is None


@pytest.mark.parametrize("model_arg", [None, "", "   ", []])
def test_after_model_auto_still_normalizes_empty_model(model_arg):
    # Null-safety is independent of the type gate: auto mode still defaults empty/null → "auto".
    mw = RuntimeConfigMiddleware()
    state = {"messages": [_ai_with_tool_call("cfdream_generate_image", {"prompt": "x", "model": model_arg})]}

    result = mw.after_model(state, _models_runtime(_models_cfg("image", ["seedream"], mode="auto")))

    assert result is not None
    assert _model_of(result) == "auto"  # normalized despite auto mode not enforcing the whitelist


# ── config.models: manual-mode whitelist injection (before_agent) ────────────────


def _models_reminder(content: str = "<system-reminder>x</system-reminder>") -> HumanMessage:
    return HumanMessage(content=content, additional_kwargs={"hide_from_ui": True, _MODELS_REMINDER_KEY: True})


def _multi_models_cfg(mode: str, content: list[dict]) -> dict:
    return {"type": mode, "content": content}


def test_before_agent_injects_manual_whitelist_reminder():
    mw = RuntimeConfigMiddleware()  # default bindings (image/video)
    cfg = _multi_models_cfg(
        "manual",
        [
            {"type": "image", "model_names": ["doubao-seedream-5-0-lite"]},
            {"type": "video", "model_names": ["wan-2-0", "wan-2-0-fast"]},
        ],
    )
    state = {"messages": [HumanMessage(content="做个短片")]}

    result = mw.before_agent(state, _models_runtime(cfg))

    assert result is not None
    msgs = result["messages"]
    assert len(msgs) == 1
    reminder = msgs[0]
    assert isinstance(reminder, HumanMessage)
    assert reminder.additional_kwargs.get(_MODELS_REMINDER_KEY) is True
    assert reminder.additional_kwargs.get("hide_from_ui") is True
    content = reminder.content
    assert _REMINDER_TAG in content
    assert "manual mode" in content
    assert "MUST choose" in content
    assert "ask_clarification" in content  # ask when it cannot decide
    assert "Ignore any model not listed" in content
    # each bound type + its allowed ids are listed
    assert "image: doubao-seedream-5-0-lite" in content
    assert "video: wan-2-0, wan-2-0-fast" in content
    assert "做个短片" not in content  # reminder only, not the user text


def test_before_agent_lists_audio_when_bound():
    # An agent that binds audio (like cf-dream) surfaces the audio slice too.
    mw = RuntimeConfigMiddleware(model_bindings=_ALL_MEDIA_BINDINGS)
    cfg = _multi_models_cfg("manual", [{"type": "audio", "model_names": ["minimax-speech-2-8-hd", "seed-tts-2-0"]}])
    state = {"messages": [HumanMessage(content="配个音")]}

    result = mw.before_agent(state, _models_runtime(cfg))

    assert result is not None
    assert "audio: minimax-speech-2-8-hd, seed-tts-2-0" in result["messages"][0].content


def test_before_agent_drops_unbound_task_type():
    # Default bindings only cover image/video → an audio slice is not advertised (would not be enforced).
    mw = RuntimeConfigMiddleware()  # image/video only
    cfg = _multi_models_cfg(
        "manual",
        [
            {"type": "image", "model_names": ["seedream"]},
            {"type": "audio", "model_names": ["minimax-speech-2-8-hd"]},
        ],
    )
    state = {"messages": [HumanMessage(content="go")]}

    content = mw.before_agent(state, _models_runtime(cfg))["messages"][0].content
    assert "image: seedream" in content
    assert "audio" not in content  # unbound type omitted


def test_before_agent_no_reminder_in_auto_mode():
    mw = RuntimeConfigMiddleware()
    cfg = _multi_models_cfg("auto", [{"type": "image", "model_names": ["seedream"]}])
    state = {"messages": [HumanMessage(content="go")]}

    assert mw.before_agent(state, _models_runtime(cfg)) is None


def test_before_agent_no_reminder_when_no_models():
    mw = RuntimeConfigMiddleware()
    state = {"messages": [HumanMessage(content="go")]}

    assert mw.before_agent(state, _models_runtime(None)) is None


def test_before_agent_no_reminder_when_manual_but_all_slices_empty():
    mw = RuntimeConfigMiddleware()
    cfg = _multi_models_cfg("manual", [{"type": "image", "model_names": []}])
    state = {"messages": [HumanMessage(content="go")]}

    assert mw.before_agent(state, _models_runtime(cfg)) is None


def test_manual_reminder_is_idempotent_within_turn():
    mw = RuntimeConfigMiddleware()
    cfg = _multi_models_cfg("manual", [{"type": "image", "model_names": ["seedream"]}])
    state = {"messages": [HumanMessage(content="go"), _models_reminder()]}

    assert mw.before_agent(state, _models_runtime(cfg)) is None


def test_manual_reminder_reinjected_on_new_turn():
    mw = RuntimeConfigMiddleware()
    cfg = _multi_models_cfg("manual", [{"type": "image", "model_names": ["seedream"]}])
    state = {
        "messages": [
            HumanMessage(content="turn-1"),
            _models_reminder(),
            AIMessage(content="ok"),
            HumanMessage(content="turn-2"),
        ]
    }

    result = mw.before_agent(state, _models_runtime(cfg))
    assert result is not None
    assert "image: seedream" in result["messages"][0].content


def test_manual_reminder_noop_on_pure_resume_without_user_message():
    mw = RuntimeConfigMiddleware()
    cfg = _multi_models_cfg("manual", [{"type": "image", "model_names": ["seedream"]}])
    state = {"messages": [_models_reminder(), AIMessage(content="...")]}

    assert mw.before_agent(state, _models_runtime(cfg)) is None


def test_before_agent_injects_both_skill_and_models_reminders(monkeypatch, tmp_path):
    # skills (Model B) and manual models whitelist are independent; both fire in before_agent.
    _write_skill(tmp_path, "seedance", _SKILL_NAME, _SKILL_MARKER)
    _patch_storage(monkeypatch, tmp_path)
    mw = RuntimeConfigMiddleware()
    cfg = _multi_models_cfg("manual", [{"type": "video", "model_names": ["wan-2-0-fast"]}])
    runtime = SimpleNamespace(context={"skills": [_SKILL_NAME], "models": cfg})
    state = {"messages": [HumanMessage(content="go")]}

    result = mw.before_agent(state, runtime)

    assert result is not None
    msgs = result["messages"]
    assert len(msgs) == 2
    assert any(m.additional_kwargs.get(_SKILL_REMINDER_KEY) and _SKILL_MARKER in m.content for m in msgs)
    assert any(m.additional_kwargs.get(_MODELS_REMINDER_KEY) and "video: wan-2-0-fast" in m.content for m in msgs)


@pytest.mark.asyncio
async def test_async_before_agent_injects_manual_whitelist():
    mw = RuntimeConfigMiddleware()
    cfg = _multi_models_cfg("manual", [{"type": "image", "model_names": ["seedream"]}])
    state = {"messages": [HumanMessage(content="go")]}

    result = await mw.abefore_agent(state, _models_runtime(cfg))

    assert result is not None
    assert "image: seedream" in result["messages"][0].content


# ── ordering: RuntimeConfigMiddleware must be registered AFTER HumanApprovalMiddleware ──
# (after_model dispatches in reverse registration order, so registering RC after HAM makes
# its model constraint run BEFORE HAM builds the approval payload → human sees the constrained
# model and human edits are final. This ordering is load-bearing; lock it.)


def test_runtime_config_registered_after_human_approval():
    from deerflow.agents.lead_agent.agent import build_middlewares
    from deerflow.config import get_app_config
    from deerflow.config.agents_config import AgentConfig

    agent_cfg = AgentConfig(name="cf-dream", approval_required_tools=["*generate_*"])
    mws = build_middlewares(
        {"configurable": {"ask": True}},
        model_name=None,
        agent_name="cf-dream",
        app_config=get_app_config(),
        agent_config=agent_cfg,
    )
    names = [type(m).__name__ for m in mws]
    assert "HumanApprovalMiddleware" in names, "HAM should be enabled with ask=True + approval_required_tools"
    assert "RuntimeConfigMiddleware" in names
    assert names.index("RuntimeConfigMiddleware") > names.index("HumanApprovalMiddleware")


# ── transport: config.skills/config.models → runtime.context ────────────────────


def _task_message(skills=None, models=None):
    from app.consumer.schemas import TaskMessage

    config: dict = {}
    if skills is not None:
        config["skills"] = skills
    if models is not None:
        config["models"] = models
    env = {
        "schema_version": "2.5",
        "message_id": "m1",
        "type": "task",
        "thread_id": "t1",
        "thread_msg_seq": 1,
        "clientId": "c1",
        "payload": {"messages": [{"role": "user", "content": "hi"}], "config": config, "reply_config": {}},
    }
    return TaskMessage.from_dict(env)


def _build_context(skills=None, models=None):
    from app.consumer.agent_runner import AgentRunner

    runner = AgentRunner(MagicMock(), MagicMock(), None, MagicMock())
    rc = runner._build_config(_task_message(skills=skills, models=models), "run-1")
    return rc["configurable"]["__pregel_runtime"].context


def test_build_config_resolves_live_config_when_no_override():
    """No pinned app_config → each build re-resolves via get_app_config().

    Locks the consumer hot-reload fix: a long-running consumer must pick up
    config.yaml edits (e.g. models[*].supports_thinking) without a restart,
    matching the Gateway request path. A startup snapshot would freeze it.
    """
    from unittest.mock import patch

    from app.consumer.agent_runner import AgentRunner

    runner = AgentRunner(MagicMock(), MagicMock(), None)  # no app_config override
    sentinel = MagicMock(name="live_app_config")
    with patch("app.consumer.agent_runner.get_app_config", return_value=sentinel) as gac:
        rc = runner._build_config(_task_message(), "run-1")
    gac.assert_called_once()
    assert rc["configurable"]["app_config"] is sentinel


def test_build_config_prefers_pinned_override():
    """An explicit app_config (test seam) is used verbatim, bypassing get_app_config()."""
    from unittest.mock import patch

    from app.consumer.agent_runner import AgentRunner

    pinned = MagicMock(name="pinned_app_config")
    runner = AgentRunner(MagicMock(), MagicMock(), None, pinned)
    with patch("app.consumer.agent_runner.get_app_config") as gac:
        rc = runner._build_config(_task_message(), "run-1")
    gac.assert_not_called()
    assert rc["configurable"]["app_config"] is pinned


def test_build_config_passes_skills_to_context():
    ctx = _build_context(skills=[_SKILL_NAME])
    assert ctx["skills"] == [_SKILL_NAME]


def test_build_config_passes_models_to_context():
    cfg = _models_cfg("image", ["seedream"])
    ctx = _build_context(models=cfg)
    assert ctx["models"] == cfg


def test_build_config_omits_when_absent():
    ctx = _build_context()
    assert "skills" not in ctx
    assert "models" not in ctx
