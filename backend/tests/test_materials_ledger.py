"""P4 — `<materials>` 台账注入（cfgpu-docs/materials.md §6, materials-impl-plan.md P4）。

覆盖 render_materials_ledger / MaterialsMiddleware.wrap_model_call：
行格式（id+kind+来源+turn+caption+ref_type）、**绝不含 url/object_key**（I9）、空表不注入、
每轮重建（第二轮含新素材）、注入只活在 request override 不写回 history、asset scope 尾标、
unstable 标注、windowing 折叠、async 路径同构。
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace

import pytest
from langchain_core.messages import HumanMessage

from deerflow.agents.materials.middleware import (
    MaterialsMiddleware,
    _build_ledger_message,
    render_materials_ledger,
)

# --- 夹具：镜像 langchain ModelRequest 的 dataclass + override 语义 -----------------


@dataclass
class FakeModelRequest:
    messages: list
    state: dict = field(default_factory=dict)

    def override(self, **overrides):
        return replace(self, **overrides)


def _request(materials=None, messages=None):
    return FakeModelRequest(
        messages=list(messages or [HumanMessage(content="给我画一张图")]),
        state={"materials": materials} if materials is not None else {},
    )


def _capturing_handler():
    box: dict = {}

    def handler(request):
        box["req"] = request
        return "MODEL_RESPONSE"

    return handler, box


def _mat(mid, *, kind="image", origin="generate", ref_type="oss_path", ref="agent-artifacts/t/x.png", **extra):
    m = {"id": mid, "kind": kind, "origin": origin, "ref_type": ref_type, "ref": ref}
    m.update(extra)
    return m


# --- render_materials_ledger：纯函数 --------------------------------------------


def test_empty_materials_no_ledger():
    assert render_materials_ledger(None) is None
    assert render_materials_ledger({}) is None


def test_line_format_has_id_kind_origin_turn_caption_reftype():
    mats = {"m1": _mat("m1", kind="image", origin="uplink", ref_type="global_url", ref="https://x/y.png", turn=1, caption="用户提供人物参考")}
    out = render_materials_ledger(mats)
    assert out is not None
    line = next(line for line in out.splitlines() if line.startswith("- [m1]"))
    assert "[m1]" in line
    assert "image" in line
    assert "上行" in line  # origin label
    assert "第1轮" in line
    assert "用户提供人物参考" in line
    assert "global_url" in line


def test_ledger_never_contains_url_or_object_key():
    """硬约束（§6/I9）：台账绝不出现 url / object_key——只给 ref_type。"""
    mats = {
        "m1": _mat("m1", ref_type="oss_path", ref="agent-artifacts/t/secret-key.png", caption="生成图"),
        "m2": _mat("m2", ref_type="global_url", ref="https://cdn.cfgpu.com/img-abc.png", caption="外链图"),
    }
    out = render_materials_ledger(mats)
    assert out is not None
    assert "agent-artifacts" not in out
    assert "secret-key" not in out
    assert "http" not in out
    assert "cdn.cfgpu.com" not in out


def test_ordered_by_numeric_id():
    mats = {"m10": _mat("m10"), "m2": _mat("m2"), "m1": _mat("m1")}
    out = render_materials_ledger(mats)
    idx = [out.index(f"[{mid}]") for mid in ("m1", "m2", "m10")]
    assert idx == sorted(idx)


def test_asset_scope_tail():
    mats = {"m5": _mat("m5", kind="asset", origin="uplink", ref_type="asset_url", ref="asset://seedance/x", scope="doubao-seedance-*")}
    out = render_materials_ledger(mats)
    assert "※仅 doubao-seedance-* 可用" in out
    assert "asset_url" not in out  # scope tail 取代 ref_type


def test_unstable_marked():
    mats = {"m1": _mat("m1", ref_type="global_url", ref="https://x/y.png", stable=False)}
    out = render_materials_ledger(mats)
    assert "⚠未落盘" in out


def test_windowing_folds_early_materials():
    mats = {f"m{i}": _mat(f"m{i}") for i in range(1, 8)}
    out = render_materials_ledger(mats, window=3)
    assert "另有 4 个早期素材" in out
    # 只列最近 3 个
    assert "[m5]" in out and "[m6]" in out and "[m7]" in out
    assert "[m1]" not in out


def test_no_fold_when_under_window():
    mats = {f"m{i}": _mat(f"m{i}") for i in range(1, 4)}
    out = render_materials_ledger(mats, window=50)
    assert "另有" not in out
    assert all(f"[m{i}]" in out for i in (1, 2, 3))


def test_usage_hint_present():
    out = render_materials_ledger({"m1": _mat("m1")})
    assert "material id" in out
    assert "<materials>" in out and "</materials>" in out


# --- _build_ledger_message：hidden reminder 包装 --------------------------------


def test_build_ledger_message_wraps_system_reminder_and_hidden():
    msg = _build_ledger_message({"m1": _mat("m1")})
    assert isinstance(msg, HumanMessage)
    assert "<system-reminder>" in msg.content
    assert "<materials>" in msg.content
    assert msg.additional_kwargs.get("hide_from_ui") is True


def test_build_ledger_message_empty_is_none():
    assert _build_ledger_message({}) is None
    assert _build_ledger_message(None) is None


# --- wrap_model_call：注入 ------------------------------------------------------


def test_wrap_injects_ledger_as_last_message():
    mw = MaterialsMiddleware()
    handler, box = _capturing_handler()
    req = _request(materials={"m1": _mat("m1", caption="暮色独行")})
    result = mw.wrap_model_call(req, handler)
    assert result == "MODEL_RESPONSE"
    seen = box["req"].messages
    assert len(seen) == 2  # 原始 user + 注入台账
    assert isinstance(seen[-1], HumanMessage)
    assert "<materials>" in seen[-1].content
    assert "暮色独行" in seen[-1].content


def test_wrap_does_not_mutate_history():
    """注入只活在 override 后的 request；原 request.messages 不被改（不写回 history）。"""
    mw = MaterialsMiddleware()
    handler, _ = _capturing_handler()
    req = _request(materials={"m1": _mat("m1")})
    original_len = len(req.messages)
    mw.wrap_model_call(req, handler)
    assert len(req.messages) == original_len  # 原对象未被改写


def test_wrap_empty_materials_passthrough():
    mw = MaterialsMiddleware()
    handler, box = _capturing_handler()
    req = _request(materials={})
    mw.wrap_model_call(req, handler)
    # 无注入：handler 收到原 request（messages 未增）
    assert len(box["req"].messages) == 1
    assert all("<materials>" not in getattr(m, "content", "") for m in box["req"].messages)


def test_rebuilt_each_turn_reflects_new_material():
    """每轮重建（§6 区别于冻结首轮）：第二轮 state 多一个素材 → 台账含之。"""
    mw = MaterialsMiddleware()

    handler1, box1 = _capturing_handler()
    mw.wrap_model_call(_request(materials={"m1": _mat("m1", caption="第一张")}), handler1)
    ledger1 = box1["req"].messages[-1].content
    assert "第一张" in ledger1 and "[m2]" not in ledger1

    handler2, box2 = _capturing_handler()
    mw.wrap_model_call(_request(materials={"m1": _mat("m1", caption="第一张"), "m2": _mat("m2", caption="第二张")}), handler2)
    ledger2 = box2["req"].messages[-1].content
    assert "第一张" in ledger2 and "第二张" in ledger2 and "[m2]" in ledger2


def test_no_materials_key_in_state_passthrough():
    mw = MaterialsMiddleware()
    handler, box = _capturing_handler()
    req = _request(materials=None)  # state 无 materials 键
    mw.wrap_model_call(req, handler)
    assert len(box["req"].messages) == 1


@pytest.mark.asyncio
async def test_awrap_injects_ledger():
    mw = MaterialsMiddleware()
    box: dict = {}

    async def handler(request):
        box["req"] = request
        return "ASYNC_RESPONSE"

    req = _request(materials={"m1": _mat("m1", caption="async素材")})
    result = await mw.awrap_model_call(req, handler)
    assert result == "ASYNC_RESPONSE"
    assert "async素材" in box["req"].messages[-1].content
