"""P10 — 本地素材生命周期三原语（cfgpu-docs/materials.md §4.8, D15/D16）。

覆盖 register_local_file（零网络登记 local 态）、localize（id→本地副本，幂等/拒 asset_url）、
stage_to_oss（id→oss_path：local/global_url 升级、oss_path deduped、id 不变）、以及 local→oss_path
经 merge_materials 放行。用假 uploader/假 fetch 计数 IO 验证「跳过」即「未调用」。
"""

from __future__ import annotations

import pytest

import deerflow.agents.materials.materialize as mz
from deerflow.agents.materials.materialize import (
    find_by_local_path,
    localize,
    register_local_file,
    stage_to_oss,
)
from deerflow.agents.thread_state import merge_materials


class _FakeUploader:
    def __init__(self) -> None:
        self.upload_calls: list[tuple[str, str, str]] = []
        self.rehost_calls: list[tuple[str, str]] = []

    async def upload_local_file(self, virtual_path: str, physical_path: str, thread_id: str) -> str:
        self.upload_calls.append((virtual_path, physical_path, thread_id))
        return f"agent-artifacts/{thread_id}/files/{virtual_path.rsplit('/', 1)[-1]}"

    async def rehost_url(self, url: str, thread_id: str) -> tuple[str, int]:
        self.rehost_calls.append((url, thread_id))
        return f"agent-artifacts/{thread_id}/images/{url.rsplit('/', 1)[-1]}", 1234


@pytest.fixture(autouse=True)
def _restore_uploader(monkeypatch):
    monkeypatch.setattr(mz, "get_oss_uploader", mz.get_oss_uploader)
    yield


def _patch(up):
    mz.get_oss_uploader = lambda: up  # type: ignore[assignment]


# --- register 原语·local 分支（零网络） ------------------------------------------


def test_register_local_file_creates_local_state():
    up = _FakeUploader()
    _patch(up)
    out = register_local_file({}, "/mnt/user-data/outputs/a.png", kind="image", caption="草图")
    assert out.ref_type == "local"
    assert out.ref == ""
    assert out.stable is False
    assert out.deduped is False
    mat = out.update[out.id]
    assert mat["ref_type"] == "local"
    assert "ref" not in mat  # I13：local 态无远程 ref
    assert mat["local_path"] == "/mnt/user-data/outputs/a.png"
    assert mat["origin"] == "local"
    assert mat["caption"] == "草图"
    assert up.upload_calls == []  # 零网络


def test_register_local_file_dedup_by_local_path():
    out1 = register_local_file({}, "/mnt/user-data/outputs/a.png", kind="image")
    materials = merge_materials({}, out1.update)
    out2 = register_local_file(materials, "/mnt/user-data/outputs/a.png", kind="image")
    assert out2.deduped is True and out2.id == out1.id and out2.update == {}


def test_register_local_id_identity_is_local_path():
    """不同 local_path → 不同 id；相同 local_path → 相同 id（§4.8.1 身份=local_path）。"""
    a = register_local_file({}, "/mnt/user-data/outputs/a.png", kind="image")
    b = register_local_file({}, "/mnt/user-data/outputs/b.png", kind="image")
    a2 = register_local_file({}, "/mnt/user-data/outputs/a.png", kind="image")
    assert a.id != b.id
    assert a.id == a2.id


# --- localize 原语（id → 本地副本） ---------------------------------------------


@pytest.mark.asyncio
async def test_localize_downloads_oss_path(tmp_path, monkeypatch):
    fetches: list[str] = []

    async def fake_fetch(url):
        fetches.append(url)
        return mz.FetchedBytes(data=b"bytes", content_type="image/png")

    monkeypatch.setattr(mz, "fetch_bytes", fake_fetch)
    materials = {"m1": {"id": "m1", "kind": "image", "origin": "generate", "ref_type": "oss_path", "ref": "agent-artifacts/t1/images/x.png"}}

    def to_physical(v):
        return str(tmp_path / v.lstrip("/"))

    path, update = await localize(
        materials, "m1", to_physical=to_physical, dest_virtual="/mnt/user-data/workspace/m1-x.png", presign=lambda k: f"https://signed/{k}"
    )
    assert path == "/mnt/user-data/workspace/m1-x.png"
    assert update["m1"]["local_path"] == path
    assert fetches == ["https://signed/agent-artifacts/t1/images/x.png"]  # presign 后下载
    # 返回的是本地路径，非 url（I9）
    assert "http" not in path


@pytest.mark.asyncio
async def test_localize_idempotent_when_local_present(tmp_path, monkeypatch):
    monkeypatch.setattr(mz, "fetch_bytes", None)  # 若真 fetch 会 TypeError → 证明没 fetch
    existing = tmp_path / "mnt/user-data/outputs/a.png"
    existing.parent.mkdir(parents=True)
    existing.write_bytes(b"x")
    materials = {"m1": {"id": "m1", "kind": "image", "origin": "local", "ref_type": "local", "local_path": "/mnt/user-data/outputs/a.png"}}

    def to_physical(v):
        return str(tmp_path / v.lstrip("/"))

    path, update = await localize(materials, "m1", to_physical=to_physical, dest_virtual="/mnt/user-data/workspace/x", presign=lambda k: k)
    assert path == "/mnt/user-data/outputs/a.png"
    assert update == {}  # 已有本地副本 → 幂等无更新


@pytest.mark.asyncio
async def test_localize_rejects_asset_url(tmp_path):
    materials = {"m1": {"id": "m1", "kind": "asset", "origin": "tool", "ref_type": "asset_url", "ref": "asset://x", "scope": "doubao-*"}}
    with pytest.raises(ValueError):
        await localize(materials, "m1", to_physical=lambda v: v, dest_virtual="/d", presign=lambda k: k)


# --- stage_to_oss 原语（id → oss_path） -----------------------------------------


@pytest.mark.asyncio
async def test_stage_local_uploads_and_upgrades_id_stable():
    """local → oss_path 升级，id 不变（§4.8.1）。"""
    up = _FakeUploader()
    _patch(up)
    reg = register_local_file({}, "/mnt/user-data/outputs/a.png", kind="image")
    materials = merge_materials({}, reg.update)
    out = await stage_to_oss(materials, reg.id, thread_id="t1", to_physical=lambda v: f"/host/{v.rsplit('/', 1)[-1]}")
    assert out.id == reg.id  # id 稳定
    assert out.ref_type == "oss_path"
    assert out.ref == "agent-artifacts/t1/files/a.png"
    merged = merge_materials(materials, out.update)
    assert merged[reg.id]["ref_type"] == "oss_path"  # 升级放行
    assert merged[reg.id]["local_path"] == "/mnt/user-data/outputs/a.png"  # 保留
    assert up.upload_calls == [("/mnt/user-data/outputs/a.png", "/host/a.png", "t1")]


@pytest.mark.asyncio
async def test_stage_oss_path_is_deduped_noop():
    up = _FakeUploader()
    _patch(up)
    materials = {"m1": {"id": "m1", "kind": "image", "origin": "generate", "ref_type": "oss_path", "ref": "agent-artifacts/t1/images/x.png"}}
    out = await stage_to_oss(materials, "m1", thread_id="t1", to_physical=lambda v: v)
    assert out.deduped is True and out.id == "m1" and out.update == {}
    assert up.upload_calls == [] and up.rehost_calls == []  # 已持久 → 零 IO


@pytest.mark.asyncio
async def test_stage_global_url_rehosts():
    up = _FakeUploader()
    _patch(up)
    materials = {"m1": {"id": "m1", "kind": "image", "origin": "generate", "ref_type": "global_url", "ref": "https://cdn.cfgpu.com/x.png"}}
    out = await stage_to_oss(materials, "m1", thread_id="t1", to_physical=lambda v: v)
    assert out.ref_type == "oss_path"
    assert up.rehost_calls == [("https://cdn.cfgpu.com/x.png", "t1")]


@pytest.mark.asyncio
async def test_stage_rejects_asset_url():
    materials = {"m1": {"id": "m1", "kind": "asset", "origin": "tool", "ref_type": "asset_url", "ref": "asset://x"}}
    with pytest.raises(ValueError):
        await stage_to_oss(materials, "m1", thread_id="t1", to_physical=lambda v: v)


def test_local_to_oss_merge_upgrade_allowed():
    """reducer 放行 local → oss_path 升级（D15）。"""
    materials = {"m1": {"id": "m1", "kind": "image", "origin": "local", "ref_type": "local", "local_path": "/mnt/x.png"}}
    out = merge_materials(materials, {"m1": {"id": "m1", "ref_type": "oss_path", "ref": "agent-artifacts/t1/files/x.png", "local_path": "/mnt/x.png"}})  # type: ignore[dict-item]
    assert out["m1"]["ref_type"] == "oss_path"
    assert find_by_local_path(out, "/mnt/x.png") == "m1"
