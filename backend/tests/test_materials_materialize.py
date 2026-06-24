"""P6 — 物化 helper（cfgpu-docs/materials.md §4.5/§8, materials-impl-plan.md P6）。

覆盖 materialize 三原语 + 查重收口：find_by_address/find_by_local_path（R3/R4/R2）、
stage 重复跳过 fetch（R1）、rehost_remote_url 地址去重 + D4 我方对象短路、rehost_local_file
登记 oss_path + R2 跳过 upload + ref_type 升级链（global_url+local_path→oss_path）、
register_remote_url 去重不落盘。用假 uploader 计数 IO，验证「跳过」即「未调用」。
"""

from __future__ import annotations

import pytest

import deerflow.agents.materials.materialize as mz
from deerflow.agents.materials.materialize import (
    find_by_address,
    find_by_local_path,
    register_remote_url,
    rehost_local_file,
    rehost_remote_url,
    stage,
)
from deerflow.agents.materials.registry import register
from deerflow.agents.thread_state import merge_materials


class _FakeUploader:
    def __init__(self) -> None:
        self.rehost_calls: list[tuple[str, str]] = []
        self.upload_calls: list[tuple[str, str, str]] = []

    async def rehost_url(self, url: str, thread_id: str) -> str:
        self.rehost_calls.append((url, thread_id))
        return f"agent-artifacts/{thread_id}/images/{url.rsplit('/', 1)[-1]}"

    async def upload_local_file(self, virtual_path: str, physical_path: str, thread_id: str) -> str:
        self.upload_calls.append((virtual_path, physical_path, thread_id))
        return f"agent-artifacts/{thread_id}/files/{virtual_path.rsplit('/', 1)[-1]}"


@pytest.fixture(autouse=True)
def _restore_uploader(monkeypatch):
    monkeypatch.setattr(mz, "get_oss_uploader", mz.get_oss_uploader)
    yield


def _patch(up):
    mz.get_oss_uploader = lambda: up  # type: ignore[assignment]


# --- 查重原语 -------------------------------------------------------------------


def test_find_by_address_matches_ref_and_origin_url():
    mid, upd = register({}, kind="image", origin="generate", ref_type="oss_path", ref="agent-artifacts/t1/images/a.png", origin_url="https://cdn.cfgpu.com/x.png")
    materials = upd
    assert find_by_address(materials, "oss_path", "agent-artifacts/t1/images/a.png") == mid
    # origin_url 也挂索引（rehost 前外链再浮现仍幂等）
    assert find_by_address(materials, "global_url", "https://cdn.cfgpu.com/x.png") == mid
    assert find_by_address(materials, "global_url", "https://other.com/z.png") is None


def test_find_by_local_path():
    materials = {"m1": {"id": "m1", "kind": "image", "origin": "local", "ref_type": "oss_path", "ref": "k", "local_path": "/mnt/user-data/outputs/a.png"}}
    assert find_by_local_path(materials, "/mnt/user-data/outputs/a.png") == "m1"
    assert find_by_local_path(materials, "/mnt/user-data/outputs/b.png") is None
    assert find_by_local_path(materials, None) is None
    assert find_by_local_path(None, "/x") is None


# --- stage（R1：远程 → 本地，重复跳过 fetch）-----------------------------------


@pytest.mark.asyncio
async def test_stage_skips_fetch_when_file_present(tmp_path, monkeypatch):
    calls: list[str] = []

    async def fake_fetch(url):
        calls.append(url)
        return mz.FetchedBytes(data=b"payload", content_type="image/png")

    monkeypatch.setattr(mz, "fetch_bytes", fake_fetch)
    dest = tmp_path / "sub" / "a.png"

    p1 = await stage("https://cdn/x.png", str(dest))
    assert p1 == str(dest)
    assert dest.read_bytes() == b"payload"
    assert calls == ["https://cdn/x.png"]  # 首次 fetch

    p2 = await stage("https://cdn/x.png", str(dest))  # 文件已在 → 跳过 fetch
    assert p2 == str(dest)
    assert calls == ["https://cdn/x.png"]  # 仍只一次


# --- rehost_remote_url（Capture 远程路径）---------------------------------------


@pytest.mark.asyncio
async def test_rehost_remote_registers_oss_path():
    up = _FakeUploader()
    _patch(up)
    out = await rehost_remote_url({}, "https://cdn.cfgpu.com/x.png", thread_id="t1", kind="image")
    assert out.ref_type == "oss_path"
    assert out.ref == "agent-artifacts/t1/images/x.png"
    assert out.deduped is False
    assert out.update[out.id]["origin_url"] == "https://cdn.cfgpu.com/x.png"
    assert up.rehost_calls == [("https://cdn.cfgpu.com/x.png", "t1")]


@pytest.mark.asyncio
async def test_rehost_remote_dedup_skips_upload():
    up = _FakeUploader()
    _patch(up)
    out1 = await rehost_remote_url({}, "https://cdn.cfgpu.com/x.png", thread_id="t1", kind="image")
    materials = merge_materials({}, out1.update)
    out2 = await rehost_remote_url(materials, "https://cdn.cfgpu.com/x.png", thread_id="t1", kind="image")
    assert out2.deduped is True
    assert out2.id == out1.id
    assert out2.update == {}
    assert len(up.rehost_calls) == 1  # 第二次跳过 upload


@pytest.mark.asyncio
async def test_rehost_remote_our_object_shortcuts_no_fetch():
    up = _FakeUploader()
    _patch(up)
    # url 已是我方对象（agent-artifacts/ path）→ D4 登记 oss_path，不 fetch
    out = await rehost_remote_url({}, "https://oss.example.com/agent-artifacts/t1/images/y.png", thread_id="t1", kind="image")
    assert out.ref_type == "oss_path"
    assert out.ref == "agent-artifacts/t1/images/y.png"
    assert up.rehost_calls == []  # 跳过 fetch


@pytest.mark.asyncio
async def test_rehost_remote_raises_when_uploader_missing():
    mz.get_oss_uploader = lambda: None  # type: ignore[assignment]
    with pytest.raises(RuntimeError):
        await rehost_remote_url({}, "https://cdn.cfgpu.com/x.png", thread_id="t1", kind="image")


# --- register_remote_url（register policy：不落盘）-------------------------------


def test_register_remote_keeps_global_url_and_dedups():
    out1 = register_remote_url({}, "https://img.search/z.png", kind="image")
    assert out1.ref_type == "global_url"
    assert out1.ref == "https://img.search/z.png"
    materials = merge_materials({}, out1.update)
    out2 = register_remote_url(materials, "https://img.search/z.png", kind="image")
    assert out2.deduped is True and out2.id == out1.id


# --- rehost_local_file（R2 + 登记 oss_path + 升级链）----------------------------


@pytest.mark.asyncio
async def test_rehost_local_registers_oss_path():
    up = _FakeUploader()
    _patch(up)
    out = await rehost_local_file({}, "/mnt/user-data/outputs/a.png", "/host/a.png", thread_id="t1", kind="image")
    assert out.ref_type == "oss_path"
    assert out.ref == "agent-artifacts/t1/files/a.png"
    mat = out.update[out.id]
    assert mat["origin"] == "local"
    assert mat["local_path"] == "/mnt/user-data/outputs/a.png"
    assert up.upload_calls == [("/mnt/user-data/outputs/a.png", "/host/a.png", "t1")]


@pytest.mark.asyncio
async def test_rehost_local_dedup_skips_upload():
    up = _FakeUploader()
    _patch(up)
    out1 = await rehost_local_file({}, "/mnt/user-data/outputs/a.png", "/host/a.png", thread_id="t1", kind="image")
    materials = merge_materials({}, out1.update)
    out2 = await rehost_local_file(materials, "/mnt/user-data/outputs/a.png", "/host/a.png", thread_id="t1", kind="image")
    assert out2.deduped is True and out2.id == out1.id
    assert len(up.upload_calls) == 1  # 第二次跳过 upload


@pytest.mark.asyncio
async def test_rehost_local_upgrades_ref_type_chain():
    """global_url + local_path → oss_path 升级链经 merge_materials 放行（§4.5 lifecycle）。"""
    up = _FakeUploader()
    _patch(up)
    # 起点：某素材是 global_url 且已 stage 到本地（带 local_path）
    materials = {
        "m1": {"id": "m1", "kind": "image", "origin": "generate", "ref_type": "global_url", "ref": "https://cdn/x.png", "local_path": "/mnt/user-data/outputs/a.png"}
    }
    out = await rehost_local_file(materials, "/mnt/user-data/outputs/a.png", "/host/a.png", thread_id="t1", kind="image")
    assert out.id == "m1"  # R3：命中既有，不新建
    merged = merge_materials(materials, out.update)
    assert merged["m1"]["ref_type"] == "oss_path"  # 升级放行
    assert merged["m1"]["ref"] == "agent-artifacts/t1/files/a.png"
    assert merged["m1"]["local_path"] == "/mnt/user-data/outputs/a.png"  # 保留


@pytest.mark.asyncio
async def test_rehost_local_raises_when_uploader_missing():
    mz.get_oss_uploader = lambda: None  # type: ignore[assignment]
    with pytest.raises(RuntimeError):
        await rehost_local_file({}, "/mnt/user-data/outputs/a.png", "/host/a.png", thread_id="t1", kind="image")
