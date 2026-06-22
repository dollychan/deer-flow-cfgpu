"""P9 — analyse_image 工具（cfgpu-docs/materials.md §4.7, materials-impl-plan.md P9）。

覆盖 analyse_image_tool（触发器+归一器，零 base64）：url 入参 in-gate 归一为 id 且去重命中既有、
未知 id / 非图 → error ToolMessage（fail-fast）、id 形态既有素材直接放行、信号 ToolMessage 零
base64 / 零 url 且 .artifact 带结构化 ids 供中间件取。
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest
from langgraph.types import Command

from deerflow.tools.builtins.analyse_image_tool import analyse_image_tool


@dataclass
class FakeRuntime:
    state: dict
    _thread_id: str = "t1"

    @property
    def context(self) -> dict:
        return {"thread_id": self._thread_id}

    config: dict = field(default_factory=dict)


async def _invoke(images, materials, **kwargs):
    runtime = FakeRuntime(state={"materials": dict(materials)})
    return await analyse_image_tool.coroutine(runtime=runtime, images=images, tool_call_id="tc_1", **kwargs)


def _img(mid: str, ref: str, ref_type: str = "global_url", kind: str = "image") -> dict:
    return {"id": mid, "kind": kind, "origin": "uplink", "ref_type": ref_type, "ref": ref}


# --- url 归一 + 去重 ------------------------------------------------------------


@pytest.mark.asyncio
async def test_url_normalised_to_id_and_signals_middleware():
    result = await _invoke(["https://img.search/z.png"], {})
    assert isinstance(result, Command)
    materials = result.update["materials"]
    (mid, mat) = next(iter(materials.items()))
    assert mat["ref_type"] == "global_url"
    assert mat["ref"] == "https://img.search/z.png"

    msg = result.update["messages"][0]
    assert msg.status == "success"
    # 信号 ToolMessage：零 base64 / 零 url（I3/I9）
    assert "http" not in msg.content
    assert "base64" not in msg.content
    assert mid in msg.content
    # .artifact 带结构化 ids 供中间件取
    assert msg.artifact["analyse_image"]["ids"] == [mid]


@pytest.mark.asyncio
async def test_url_dedup_hits_existing_no_new_material():
    materials = {"m1": _img("m1", "https://img.search/z.png")}
    result = await _invoke(["https://img.search/z.png"], materials)
    msg = result.update["messages"][0]
    assert msg.artifact["analyse_image"]["ids"] == ["m1"]
    assert "materials" not in result.update  # 命中既有 → 无新登记


@pytest.mark.asyncio
async def test_existing_id_passes_through():
    materials = {"m3": _img("m3", "https://cdn/a.png")}
    result = await _invoke(["m3"], materials)
    msg = result.update["messages"][0]
    assert msg.artifact["analyse_image"]["ids"] == ["m3"]
    assert "materials" not in result.update


@pytest.mark.asyncio
async def test_mixed_ids_and_urls_dedup_and_order():
    materials = {"m1": _img("m1", "https://cdn/a.png")}
    result = await _invoke(["m1", "https://cdn/b.png", "m1"], materials)
    msg = result.update["messages"][0]
    ids = msg.artifact["analyse_image"]["ids"]
    assert ids[0] == "m1"
    assert len(ids) == 2  # m1 去重 + 新登记的 b.png
    assert ids[1] in result.update["materials"]


# --- 廉价校验 fail-fast ----------------------------------------------------------


@pytest.mark.asyncio
async def test_unknown_id_errors():
    result = await _invoke(["m9"], {})
    msg = result.update["messages"][0]
    assert msg.status == "error"
    assert "m9" in msg.content
    assert "materials" not in result.update  # 不把悬空 id 误注册


@pytest.mark.asyncio
async def test_non_image_kind_errors():
    materials = {"m2": _img("m2", "https://cdn/v.mp4", kind="video")}
    result = await _invoke(["m2"], materials)
    msg = result.update["messages"][0]
    assert msg.status == "error"
    assert "m2" in msg.content


@pytest.mark.asyncio
async def test_empty_images_errors():
    result = await _invoke([], {})
    assert result.update["messages"][0].status == "error"


@pytest.mark.asyncio
async def test_question_and_focus_carried_in_signal():
    result = await _invoke(["https://cdn/a.png"], {}, question="哪只手崩了？", focus="hands")
    signal = result.update["messages"][0].artifact["analyse_image"]
    assert signal["question"] == "哪只手崩了？"
    assert signal["focus"] == "hands"
