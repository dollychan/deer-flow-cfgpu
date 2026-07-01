"""P5 — summarization 一致性契约（cfgpu-docs/materials.md §7, materials-impl-plan.md P5）。

覆盖 MaterialsSummarizationMiddleware：摘要 prompt 末尾注入**纯素材指令**（不列清单）、禁 url、
禁编造判定/反馈、只保留用户明确表达过的关系、空台账不追加、before_model/abefore_model 经
ContextVar 搬运 materials 且 finally 复位（并发安全）。用 __new__ 绕开父类重模型构造，
monkeypatch 父链隔离 override 行为。
"""

from __future__ import annotations

import pytest

from deerflow.agents.materials.summarization import (
    MaterialsSummarizationMiddleware,
    _build_materials_summary_section,
    _materials_ctx,
)
from deerflow.agents.middlewares.summarization_middleware import DeerFlowSummarizationMiddleware


def _mat(mid, *, kind="image", origin="generate", ref_type="oss_path", ref="agent-artifacts/t/x.png", **extra):
    m = {"id": mid, "kind": kind, "origin": origin, "ref_type": ref_type, "ref": ref}
    m.update(extra)
    return m


def _bare_instance() -> MaterialsSummarizationMiddleware:
    return MaterialsSummarizationMiddleware.__new__(MaterialsSummarizationMiddleware)


# --- _build_materials_summary_section：纯函数 -----------------------------------


def test_section_none_when_empty():
    assert _build_materials_summary_section(None) is None
    assert _build_materials_summary_section({}) is None


def test_section_is_pure_instruction_no_inventory():
    """§7 改造：纯指令，**不列素材清单**——[m1]/caption 不得出现在段内（清单归 live 台账 SSOT）。"""
    section = _build_materials_summary_section({"m1": _mat("m1", caption="暮色独行")})
    assert section is not None
    # 指令要件
    assert "唯一实时真相" in section  # 台账为 SSOT
    assert "清单" in section  # 禁止输出清单
    assert "禁止复述其 url/object_key" in section
    assert "衍生关系" in section
    assert "严禁" in section  # 禁编造判定
    assert "投射" in section  # 禁把反馈泛化到最新素材
    # 反向：不得把台账内容抄进段里
    assert "[m1]" not in section
    assert "暮色独行" not in section


def test_section_forbids_fabricated_verdicts():
    """伪造判定/反馈是本次 bug 的直接根因——指令必须显式点名这些词并压过 base prompt。"""
    section = _build_materials_summary_section({"m1": _mat("m1")})
    assert "用户是否满意" in section
    assert "需重新生成" in section
    assert "用户明确表达过" in section
    assert "以本段为准" in section  # 覆盖 base prompt 的 ARTIFACTS/逐项判定


def test_section_never_contains_url():
    section = _build_materials_summary_section(
        {
            "m1": _mat("m1", ref_type="oss_path", ref="agent-artifacts/t/secret.png"),
            "m2": _mat("m2", ref_type="global_url", ref="https://cdn.cfgpu.com/x.png"),
        }
    )
    assert "http" not in section
    assert "agent-artifacts" not in section
    assert "secret" not in section


# --- _build_summary_prompt override --------------------------------------------


def test_prompt_appends_section(monkeypatch):
    monkeypatch.setattr(DeerFlowSummarizationMiddleware, "_build_summary_prompt", lambda self, msgs: "BASE PROMPT")
    inst = _bare_instance()
    token = _materials_ctx.set({"m1": _mat("m1", caption="参考图")})
    try:
        out = inst._build_summary_prompt([])
    finally:
        _materials_ctx.reset(token)
    assert out.startswith("BASE PROMPT")
    # 追加的是纯指令段，不复述素材本体
    assert "唯一实时真相" in out
    assert "严禁" in out
    assert "[m1]" not in out
    assert "参考图" not in out


def test_prompt_none_passthrough(monkeypatch):
    """父类返回 None（trim 后无内容）时不追加，保持 None。"""
    monkeypatch.setattr(DeerFlowSummarizationMiddleware, "_build_summary_prompt", lambda self, msgs: None)
    inst = _bare_instance()
    token = _materials_ctx.set({"m1": _mat("m1")})
    try:
        assert inst._build_summary_prompt([]) is None
    finally:
        _materials_ctx.reset(token)


def test_prompt_no_materials_unchanged(monkeypatch):
    monkeypatch.setattr(DeerFlowSummarizationMiddleware, "_build_summary_prompt", lambda self, msgs: "BASE PROMPT")
    inst = _bare_instance()
    token = _materials_ctx.set({})
    try:
        assert inst._build_summary_prompt([]) == "BASE PROMPT"
    finally:
        _materials_ctx.reset(token)


# --- before_model / abefore_model：ContextVar 搬运 ------------------------------


def test_before_model_sets_and_resets_ctx(monkeypatch):
    seen: dict = {}

    def fake_before(self, state, runtime):
        seen["materials"] = dict(_materials_ctx.get())
        return None

    monkeypatch.setattr(DeerFlowSummarizationMiddleware, "before_model", fake_before)
    inst = _bare_instance()
    state = {"materials": {"m1": _mat("m1")}, "messages": []}
    inst.before_model(state, runtime=None)
    assert "m1" in seen["materials"]  # ctx 在父调用期间可见
    assert _materials_ctx.get() == {}  # finally 已复位为默认


def test_before_model_missing_materials_key(monkeypatch):
    seen: dict = {}
    monkeypatch.setattr(DeerFlowSummarizationMiddleware, "before_model", lambda self, state, runtime: seen.update(m=dict(_materials_ctx.get())))
    inst = _bare_instance()
    inst.before_model({"messages": []}, runtime=None)  # 无 materials 键
    assert seen["m"] == {}


@pytest.mark.asyncio
async def test_abefore_model_sets_and_resets_ctx(monkeypatch):
    seen: dict = {}

    async def fake_abefore(self, state, runtime):
        seen["materials"] = dict(_materials_ctx.get())
        return None

    monkeypatch.setattr(DeerFlowSummarizationMiddleware, "abefore_model", fake_abefore)
    inst = _bare_instance()
    state = {"materials": {"m2": _mat("m2", caption="async")}, "messages": []}
    await inst.abefore_model(state, runtime=None)
    assert "m2" in seen["materials"]
    assert _materials_ctx.get() == {}


def test_end_to_end_seam(monkeypatch):
    """before_model 搬运 → _build_summary_prompt 读到：模拟父类在 before_model 内触发 prompt 构建。"""
    captured: dict = {}

    def fake_before(self, state, runtime):
        # 父 _maybe_summarize 会在此调用链内构建 prompt；此处直接调用 override 验证闭环
        captured["prompt"] = self._build_summary_prompt([])
        return None

    monkeypatch.setattr(DeerFlowSummarizationMiddleware, "before_model", fake_before)
    monkeypatch.setattr(DeerFlowSummarizationMiddleware, "_build_summary_prompt", lambda self, msgs: "SUMMARY")
    inst = _bare_instance()
    inst.before_model({"materials": {"m3": _mat("m3", caption="图生视频源")}, "messages": []}, runtime=None)
    assert "SUMMARY" in captured["prompt"]
    # 指令段接上，但不复述素材本体
    assert "严禁" in captured["prompt"]
    assert "[m3]" not in captured["prompt"]
    assert "图生视频源" not in captured["prompt"]


def test_name_preserves_parent_for_frontend_key():
    """子类化不得改 LangGraph update key——前端按 DeerFlowSummarizationMiddleware.before_model 识别。"""
    inst = _bare_instance()
    assert inst.name == "DeerFlowSummarizationMiddleware"
