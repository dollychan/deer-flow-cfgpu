"""P6 — stage_material 工具（cfgpu-docs/materials.md §4.5/§8, materials-impl-plan.md P6）。

覆盖 stage_material_tool：本地文件 → rehost → 登记 oss_path origin=local、回 id 形态 ToolMessage
（零 url）、materials 更新进 Command、文件缺失/状态缺失回 error、虚拟路径→物理映射。
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest
from langgraph.types import Command

import deerflow.agents.materials.materialize as mz
from deerflow.tools.builtins.stage_material_tool import _virtual_to_physical, stage_material_tool


@dataclass
class FakeRuntime:
    state: dict
    _thread_id: str = "t1"

    @property
    def context(self) -> dict:
        return {"thread_id": self._thread_id}

    config: dict = field(default_factory=dict)


class _FakeUploader:
    def __init__(self) -> None:
        self.upload_calls: list[tuple[str, str, str]] = []

    async def upload_local_file(self, virtual_path: str, physical_path: str, thread_id: str) -> str:
        self.upload_calls.append((virtual_path, physical_path, thread_id))
        return f"agent-artifacts/{thread_id}/files/{virtual_path.rsplit('/', 1)[-1]}"


@pytest.fixture(autouse=True)
def _restore_uploader(monkeypatch):
    monkeypatch.setattr(mz, "get_oss_uploader", mz.get_oss_uploader)
    yield


async def _invoke(tool, runtime, **kwargs):
    return await tool.coroutine(runtime=runtime, tool_call_id="tc_1", **kwargs)


def _runtime_with_file(tmp_path, name="a.png"):
    outputs = tmp_path / "user-data" / "outputs"
    outputs.mkdir(parents=True)
    (outputs / name).write_bytes(b"x")
    state = {"thread_data": {"outputs_path": str(outputs)}, "materials": {}}
    return FakeRuntime(state=state)


# --- 路径映射 -------------------------------------------------------------------


def test_virtual_to_physical_maps_user_data(tmp_path):
    outputs = tmp_path / "user-data" / "outputs"
    phys = _virtual_to_physical("/mnt/user-data/outputs/a.png", str(outputs))
    assert phys == str((tmp_path / "user-data" / "outputs" / "a.png").resolve())
    # 不限于 outputs 子树：workspace 也可
    phys_ws = _virtual_to_physical("/mnt/user-data/workspace/b.png", str(outputs))
    assert phys_ws == str((tmp_path / "user-data" / "workspace" / "b.png").resolve())


# --- 正常登记 -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stage_material_registers_oss_path(tmp_path):
    up = _FakeUploader()
    mz.get_oss_uploader = lambda: up  # type: ignore[assignment]
    runtime = _runtime_with_file(tmp_path)

    result = await _invoke(stage_material_tool, runtime, filepath="/mnt/user-data/outputs/a.png", caption="草图")
    assert isinstance(result, Command)
    materials = result.update["materials"]
    (mid, mat) = next(iter(materials.items()))
    assert mat["ref_type"] == "oss_path"
    assert mat["origin"] == "local"
    assert mat["ref"] == "agent-artifacts/t1/files/a.png"
    assert mat["caption"] == "草图"
    assert len(up.upload_calls) == 1

    msg = result.update["messages"][0]
    assert mid in msg.content
    # id 形态：content 零 url/object_key（I3）
    assert "http" not in msg.content
    assert "agent-artifacts" not in msg.content
    # artifact 轨带稳定 ref 供客户端
    assert msg.artifact["items"][0]["ref"] == "agent-artifacts/t1/files/a.png"


@pytest.mark.asyncio
async def test_stage_material_dedup_skips_second_upload(tmp_path):
    up = _FakeUploader()
    mz.get_oss_uploader = lambda: up  # type: ignore[assignment]
    runtime = _runtime_with_file(tmp_path)

    r1 = await _invoke(stage_material_tool, runtime, filepath="/mnt/user-data/outputs/a.png")
    runtime.state["materials"] = r1.update["materials"]  # 把登记结果回灌 state
    r2 = await _invoke(stage_material_tool, runtime, filepath="/mnt/user-data/outputs/a.png")
    assert len(up.upload_calls) == 1  # R2：第二次跳过 upload
    assert "materials" not in r2.update  # 命中既有 → 无新 materials 更新


# --- 错误路径 -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stage_material_file_not_found(tmp_path):
    up = _FakeUploader()
    mz.get_oss_uploader = lambda: up  # type: ignore[assignment]
    outputs = tmp_path / "user-data" / "outputs"
    outputs.mkdir(parents=True)
    runtime = FakeRuntime(state={"thread_data": {"outputs_path": str(outputs)}, "materials": {}})

    result = await _invoke(stage_material_tool, runtime, filepath="/mnt/user-data/outputs/missing.png")
    msg = result.update["messages"][0]
    assert msg.status == "error"
    assert "materials" not in result.update
    assert up.upload_calls == []


@pytest.mark.asyncio
async def test_stage_material_missing_outputs_path(tmp_path):
    runtime = FakeRuntime(state={"thread_data": {}, "materials": {}})
    result = await _invoke(stage_material_tool, runtime, filepath="/mnt/user-data/outputs/a.png")
    assert result.update["messages"][0].status == "error"
