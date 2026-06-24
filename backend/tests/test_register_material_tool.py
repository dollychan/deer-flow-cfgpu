"""P10 — register_material 工具（cfgpu-docs/materials.md §4.8.3）。

覆盖 register_material_tool：本地文件**廉价登记**成 local 素材（零网络、不上传）、回 id 形态
ToolMessage、materials 更新进 Command、文件缺失/状态缺失回 error、find_by_local_path 去重。
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest
from langgraph.types import Command

from deerflow.tools.builtins.register_material_tool import _virtual_to_physical, register_material_tool


@dataclass
class FakeRuntime:
    state: dict
    _thread_id: str = "t1"

    @property
    def context(self) -> dict:
        return {"thread_id": self._thread_id}

    config: dict = field(default_factory=dict)


async def _invoke(tool, runtime, **kwargs):
    return await tool.coroutine(runtime=runtime, tool_call_id="tc_1", **kwargs)


def _runtime_with_file(tmp_path, name="a.png"):
    outputs = tmp_path / "user-data" / "outputs"
    outputs.mkdir(parents=True)
    (outputs / name).write_bytes(b"x")
    return FakeRuntime(state={"thread_data": {"outputs_path": str(outputs)}, "materials": {}})


def test_virtual_to_physical_maps_user_data(tmp_path):
    outputs = tmp_path / "user-data" / "outputs"
    phys = _virtual_to_physical("/mnt/user-data/outputs/a.png", str(outputs))
    assert phys == str((tmp_path / "user-data" / "outputs" / "a.png").resolve())
    phys_ws = _virtual_to_physical("/mnt/user-data/workspace/b.png", str(outputs))
    assert phys_ws == str((tmp_path / "user-data" / "workspace" / "b.png").resolve())


@pytest.mark.asyncio
async def test_register_creates_local_material_zero_network(tmp_path):
    runtime = _runtime_with_file(tmp_path)
    result = await _invoke(register_material_tool, runtime, filepath="/mnt/user-data/outputs/a.png", caption="草图")
    assert isinstance(result, Command)
    materials = result.update["materials"]
    (mid, mat) = next(iter(materials.items()))
    assert mat["ref_type"] == "local"
    assert "ref" not in mat  # I13：local 无远程 ref
    assert mat["local_path"] == "/mnt/user-data/outputs/a.png"
    assert mat["origin"] == "local"
    assert mat["caption"] == "草图"
    assert mat.get("stable") is False

    msg = result.update["messages"][0]
    assert mid in msg.content
    # id 形态：content 零 url/object_key（I3）
    assert "http" not in msg.content
    assert "agent-artifacts" not in msg.content


@pytest.mark.asyncio
async def test_register_dedup_same_file(tmp_path):
    runtime = _runtime_with_file(tmp_path)
    r1 = await _invoke(register_material_tool, runtime, filepath="/mnt/user-data/outputs/a.png")
    runtime.state["materials"] = r1.update["materials"]
    r2 = await _invoke(register_material_tool, runtime, filepath="/mnt/user-data/outputs/a.png")
    assert "materials" not in r2.update  # 命中既有 → 无新登记


@pytest.mark.asyncio
async def test_register_file_not_found(tmp_path):
    outputs = tmp_path / "user-data" / "outputs"
    outputs.mkdir(parents=True)
    runtime = FakeRuntime(state={"thread_data": {"outputs_path": str(outputs)}, "materials": {}})
    result = await _invoke(register_material_tool, runtime, filepath="/mnt/user-data/outputs/missing.png")
    assert result.update["messages"][0].status == "error"
    assert "materials" not in result.update


@pytest.mark.asyncio
async def test_register_missing_outputs_path(tmp_path):
    runtime = FakeRuntime(state={"thread_data": {}, "materials": {}})
    result = await _invoke(register_material_tool, runtime, filepath="/mnt/user-data/outputs/a.png")
    assert result.update["messages"][0].status == "error"
