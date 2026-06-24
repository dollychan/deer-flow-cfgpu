"""P10 — localize_material 工具（cfgpu-docs/materials.md §4.8.3）。

覆盖 localize_material_tool：远程素材 → 下载本地副本、返回 /mnt/user-data/workspace 虚拟路径
（非 url，I9）、attach local_path、未知 id / asset_url 回 error、幂等（已有本地副本不重复下载）。
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest
from langgraph.types import Command

import deerflow.agents.materials.materialize as mz
from deerflow.tools.builtins.localize_material_tool import localize_material_tool


@dataclass
class FakeRuntime:
    state: dict

    @property
    def context(self) -> dict:
        return {"thread_id": "t1"}

    config: dict = field(default_factory=dict)


async def _invoke(runtime, **kwargs):
    return await localize_material_tool.coroutine(runtime=runtime, tool_call_id="tc_1", **kwargs)


def _runtime(tmp_path, materials):
    outputs = tmp_path / "user-data" / "outputs"
    outputs.mkdir(parents=True)
    return FakeRuntime(state={"thread_data": {"outputs_path": str(outputs)}, "materials": materials})


@pytest.mark.asyncio
async def test_localize_downloads_and_returns_local_path(tmp_path, monkeypatch):
    fetches = []

    async def fake_fetch(url):
        fetches.append(url)
        return mz.FetchedBytes(data=b"bytes", content_type="image/png")

    monkeypatch.setattr(mz, "fetch_bytes", fake_fetch)
    materials = {"m1": {"id": "m1", "kind": "image", "origin": "generate", "ref_type": "oss_path", "ref": "agent-artifacts/t1/images/x.png"}}
    runtime = _runtime(tmp_path, materials)

    result = await _invoke(runtime, material_id="m1")
    assert isinstance(result, Command)
    msg = result.update["messages"][0]
    assert msg.status == "success"
    # 返回的是本地虚拟路径（非 url，I9）
    assert "/mnt/user-data/workspace/" in msg.content
    assert "http" not in msg.content
    assert result.update["materials"]["m1"]["local_path"].startswith("/mnt/user-data/workspace/")
    assert len(fetches) == 1


@pytest.mark.asyncio
async def test_localize_unknown_id_errors(tmp_path):
    runtime = _runtime(tmp_path, {})
    result = await _invoke(runtime, material_id="m99")
    assert result.update["messages"][0].status == "error"
    assert "materials" not in result.update


@pytest.mark.asyncio
async def test_localize_asset_url_rejected(tmp_path):
    materials = {"m1": {"id": "m1", "kind": "asset", "origin": "tool", "ref_type": "asset_url", "ref": "asset://x"}}
    runtime = _runtime(tmp_path, materials)
    result = await _invoke(runtime, material_id="m1")
    assert result.update["messages"][0].status == "error"


@pytest.mark.asyncio
async def test_localize_idempotent_when_already_local(tmp_path, monkeypatch):
    # 物理副本已在 → 不下载
    def boom(*a, **k):
        raise AssertionError("should not fetch")

    monkeypatch.setattr(mz, "fetch_bytes", boom)
    outputs = tmp_path / "user-data" / "outputs"
    outputs.mkdir(parents=True)
    existing = tmp_path / "user-data" / "workspace" / "a.png"
    existing.parent.mkdir(parents=True)
    existing.write_bytes(b"x")
    materials = {"m1": {"id": "m1", "kind": "image", "origin": "local", "ref_type": "local", "local_path": "/mnt/user-data/workspace/a.png"}}
    runtime = FakeRuntime(state={"thread_data": {"outputs_path": str(outputs)}, "materials": materials})

    result = await _invoke(runtime, material_id="m1")
    assert result.update["messages"][0].status == "success"
    assert "/mnt/user-data/workspace/a.png" in result.update["messages"][0].content
