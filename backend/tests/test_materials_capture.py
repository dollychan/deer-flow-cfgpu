"""P3 — MaterialsCapture 准入⊥转存三态 + 双轨改写（cfgpu-docs/materials.md §4.2, impl-plan P3）。

覆盖 policy.resolve_capture_policy / middleware._capture（经 awrap_tool_call）：
cfgpu artifact 信号探测、rehost 落盘、register 不落盘、rehost 失败 stable=false、
task_wait 重放去重、D4 我方对象短路、双轨改写（content 零 url / artifact 带 object_key）。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace

import pytest
from langchain_core.messages import ToolMessage
from langgraph.types import Command

import deerflow.agents.materials.middleware as mw
from deerflow.agents.materials.middleware import MaterialsMiddleware, _extract_artifact_urls, _infer_kind
from deerflow.agents.materials.policy import resolve_capture_policy

# --- 夹具 -------------------------------------------------------------------


@dataclass
class FakeTool:
    metadata: dict | None = None


@dataclass
class FakeRuntime:
    thread_id: str = "t1"

    @property
    def context(self) -> dict:
        return {"thread_id": self.thread_id}

    config: dict = field(default_factory=dict)


@dataclass
class FakeRequest:
    tool_call: dict
    state: dict = field(default_factory=dict)
    tool: FakeTool | None = None
    runtime: FakeRuntime | None = None

    def override(self, **overrides):
        return replace(self, **overrides)


class _FakeUploader:
    def __init__(self, fail: bool = False) -> None:
        self.calls: list[tuple[str, str]] = []
        self._fail = fail

    async def rehost_url(self, url: str, thread_id: str) -> str:
        self.calls.append((url, thread_id))
        if self._fail:
            raise RuntimeError("boom")
        return f"agent-artifacts/{thread_id}/images/{url.rsplit('/', 1)[-1]}"


def _cfgpu_result(urls, *, name="cfgpu_generate_image", artifact=True, extra=None):
    body = {"urls": urls, "expires_at": "2026-06-06T10:00:00+00:00", "model_used": "doubao", "usage": {"total_tokens": 100}}
    if artifact:
        body["artifact"] = True
    if extra:
        body.update(extra)
    return ToolMessage(content=json.dumps(body), tool_call_id="tc_1", name=name)


async def _run(result, *, fake_uploader, materials=None, metadata=None, name="cfgpu_generate_image", thread_id="t1"):
    mw.get_oss_uploader = lambda: fake_uploader  # type: ignore[assignment]
    request = FakeRequest(
        tool_call={"name": name, "args": {}, "id": "tc_1"},
        state={"materials": materials} if materials is not None else {},
        tool=FakeTool(metadata=metadata),
        runtime=FakeRuntime(thread_id=thread_id),
    )

    async def handler(_req):
        return result

    return await MaterialsMiddleware().awrap_tool_call(request, handler)


@pytest.fixture(autouse=True)
def _restore_uploader(monkeypatch):
    monkeypatch.setattr(mw, "get_oss_uploader", mw.get_oss_uploader)
    yield


# --- policy 解析 ------------------------------------------------------------


def test_policy_cfgpu_default_rehost():
    assert resolve_capture_policy("cfgpu_generate_image") == "rehost"
    assert resolve_capture_policy("cfgpu_task_wait") == "rehost"


def test_policy_metadata_override():
    assert resolve_capture_policy("image_search", {"materials_capture": "register"}) == "register"
    assert resolve_capture_policy("cfgpu_generate_image", {"materials_capture": "off"}) == "off"


def test_policy_default_off():
    assert resolve_capture_policy("bash") == "off"
    assert resolve_capture_policy("web_search") == "off"


# --- 信号探测：只认 artifact:true + urls -----------------------------------


def test_extract_requires_artifact_flag():
    with_flag = _cfgpu_result(["https://cdn.cfgpu.com/a.png"])
    assert _extract_artifact_urls(with_flag) == ["https://cdn.cfgpu.com/a.png"]


def test_extract_skips_without_flag():
    # status-only 信封 / 无 artifact 标志 → 不准入
    no_flag = _cfgpu_result(["https://cdn.cfgpu.com/a.png"], artifact=False)
    assert _extract_artifact_urls(no_flag) == []


def test_extract_skips_error_dict():
    err = ToolMessage(content=json.dumps({"error": True, "error_type": "content_blocked", "message": "x"}), tool_call_id="tc_1", name="cfgpu_generate_image")
    assert _extract_artifact_urls(err) == []


def test_extract_skips_async_stub():
    stub = ToolMessage(content=json.dumps({"task_id": "task-abc", "status": "pending"}), tool_call_id="tc_1", name="cfgpu_generate_image")
    assert _extract_artifact_urls(stub) == []


def test_infer_kind_by_ext():
    assert _infer_kind("https://cdn.cfgpu.com/x.mp4") == "video"
    assert _infer_kind("https://cdn.cfgpu.com/x.png") == "image"
    assert _infer_kind("https://cdn.cfgpu.com/x.mp3") == "audio"


# --- rehost 落盘 + 双轨改写 -------------------------------------------------


@pytest.mark.asyncio
async def test_rehost_registers_oss_path_and_dual_track():
    up = _FakeUploader()
    result = _cfgpu_result(["https://cdn.cfgpu.com/img-abc.png"])
    out = await _run(result, fake_uploader=up)

    assert isinstance(out, Command)
    mats = out.update["materials"]
    assert mats["m1"]["ref_type"] == "oss_path"
    assert mats["m1"]["ref"] == "agent-artifacts/t1/images/img-abc.png"
    assert mats["m1"]["origin_url"] == "https://cdn.cfgpu.com/img-abc.png"
    assert mats["m1"]["stable"] is True
    assert up.calls == [("https://cdn.cfgpu.com/img-abc.png", "t1")]

    # 双轨：content 去 url 留 id；artifact 带 object_key
    tm = out.update["messages"][0]
    body = json.loads(tm.content)
    assert body["materials"] == ["m1"]
    assert "urls" not in body and "http" not in tm.content
    assert tm.artifact["items"][0]["ref"] == "agent-artifacts/t1/images/img-abc.png"
    assert tm.artifact["items"][0]["kind"] == "image"


@pytest.mark.asyncio
async def test_register_policy_keeps_global_url_no_upload():
    up = _FakeUploader()
    result = _cfgpu_result(["https://third.cdn/x.png"], name="image_search")
    out = await _run(result, fake_uploader=up, metadata={"materials_capture": "register"}, name="image_search")

    mats = out.update["materials"]
    assert mats["m1"]["ref_type"] == "global_url"
    assert mats["m1"]["ref"] == "https://third.cdn/x.png"
    assert up.calls == []  # register 不 fetch/不 upload


@pytest.mark.asyncio
async def test_rehost_failure_marks_unstable_not_deliverable():
    up = _FakeUploader(fail=True)
    result = _cfgpu_result(["https://cdn.cfgpu.com/img-abc.png"])
    out = await _run(result, fake_uploader=up)

    mats = out.update["materials"]
    assert mats["m1"]["stable"] is False
    assert mats["m1"]["ref_type"] == "global_url"
    assert mats["m1"].get("display") is None  # 不作交付物（I5）
    # content 仍被改写为 id 形态（不泄漏临期 url 给后续模型轮）
    assert "http" not in out.update["messages"][0].content


@pytest.mark.asyncio
async def test_our_object_url_shortcuts_no_rehost():
    # D4：cfgpu 结果里若已是我方 OSS 对象 → 登记 oss_path，跳过 fetch
    up = _FakeUploader()
    result = _cfgpu_result(["https://oss.cfgpu.com/agent-artifacts/t1/x.png"])
    out = await _run(result, fake_uploader=up)

    mats = out.update["materials"]
    assert mats["m1"]["ref_type"] == "oss_path"
    assert mats["m1"]["ref"] == "agent-artifacts/t1/x.png"
    assert up.calls == []


@pytest.mark.asyncio
async def test_task_wait_replay_dedup_no_double_rehost():
    up = _FakeUploader()
    url = "https://cdn.cfgpu.com/vid-abc.mp4"
    first = await _run(_cfgpu_result([url], name="cfgpu_task_wait"), fake_uploader=up, name="cfgpu_task_wait")
    mats = dict(first.update["materials"])
    # 重放：同 url 再浮现，materials 已含 m1
    second = await _run(_cfgpu_result([url], name="cfgpu_task_wait"), fake_uploader=up, materials=mats, name="cfgpu_task_wait")

    assert len(up.calls) == 1  # 只 rehost 一次（不双计费）
    assert "materials" not in second.update  # 无新建
    assert json.loads(second.update["messages"][0].content)["materials"] == ["m1"]  # 仍指向既有 id


@pytest.mark.asyncio
async def test_off_policy_passes_result_through():
    up = _FakeUploader()
    result = _cfgpu_result(["https://cdn.cfgpu.com/a.png"], name="bash")
    out = await _run(result, fake_uploader=up, name="bash")
    assert out is result  # off：不接管，原样放行
    assert up.calls == []


@pytest.mark.asyncio
async def test_multi_url_sequential_ids():
    up = _FakeUploader()
    result = _cfgpu_result(["https://cdn.cfgpu.com/a.png", "https://cdn.cfgpu.com/b.png"])
    out = await _run(result, fake_uploader=up)
    assert set(out.update["materials"]) == {"m1", "m2"}
    assert json.loads(out.update["messages"][0].content)["materials"] == ["m1", "m2"]
    assert len(up.calls) == 2
