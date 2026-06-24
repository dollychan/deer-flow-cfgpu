"""P3 — MaterialsCapture 准入⊥转存三态 + 双轨改写（cfgpu-docs/materials.md §4.2, impl-plan P3）。

覆盖 policy.resolve_capture_policy / middleware._capture（经 awrap_tool_call）：
cfdream artifact 信号探测、rehost 落盘、register 不落盘、rehost 失败 stable=false、
task_wait 重放去重、D4 我方对象短路、双轨改写（content 零 url / artifact 带 object_key）。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace

import pytest
from langchain_core.messages import ToolMessage
from langgraph.types import Command

import deerflow.agents.materials.materialize as mz
from deerflow.agents.materials.middleware import MaterialsMiddleware, _extract_urls_by_path, _infer_kind
from deerflow.agents.materials.policy import resolve_capture_policy, resolve_url_path
from deerflow.agents.materials.registry import classify_ref, material_id


def _mid_of(url: str) -> str:
    """复算某产物源 url 的内容派生 id（§B）——capture rehost/register/D4 三路同口径。"""
    return material_id(*classify_ref(url))


# Capture 配置（D13+/D14，cfdream_ 前缀硬编码已退役）——模拟 director config.yaml 的两组 fnmatch：
# policy / url 抽取字段路径。测试夹具 `_cfdream_result` 把 url 放顶层 `urls`，故 url 路径统一配
# "urls"；`results[*].image_url` 这类结构化路径另有专测覆盖。**display 与 artifact items 不在 capture
# 层**（D14：归 MessageStreamMiddleware），故此处无 visibility 配置。
_CAPTURE_PATTERNS = [("cfdream_*", "rehost"), ("image_search", "register")]
_URLPATH_PATTERNS = [("*", "urls")]

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


def _cfdream_result(urls, *, name="cfdream_generate_image", artifact=True, extra=None):
    body = {"urls": urls, "expires_at": "2026-06-06T10:00:00+00:00", "model_used": "doubao", "usage": {"total_tokens": 100}}
    if artifact:
        body["artifact"] = True
    if extra:
        body.update(extra)
    return ToolMessage(content=json.dumps(body), tool_call_id="tc_1", name=name)


async def _run(result, *, fake_uploader, materials=None, metadata=None, name="cfdream_generate_image", thread_id="t1"):
    # P6 收口：rehost 改经 materialize helper → 在 materialize 命名空间打桩 uploader。
    mz.get_oss_uploader = lambda: fake_uploader  # type: ignore[assignment]
    request = FakeRequest(
        tool_call={"name": name, "args": {}, "id": "tc_1"},
        state={"materials": materials} if materials is not None else {},
        tool=FakeTool(metadata=metadata),
        runtime=FakeRuntime(thread_id=thread_id),
    )

    async def handler(_req):
        return result

    middleware = MaterialsMiddleware(
        capture_patterns=_CAPTURE_PATTERNS,
        url_path_patterns=_URLPATH_PATTERNS,
    )
    return await middleware.awrap_tool_call(request, handler)


@pytest.fixture(autouse=True)
def _restore_uploader(monkeypatch):
    monkeypatch.setattr(mz, "get_oss_uploader", mz.get_oss_uploader)
    yield


# --- policy 解析（配置驱动，cfdream_ 前缀硬编码已退役 D13+）-------------------


def test_policy_config_pattern_match():
    # 无 metadata 时按配置 fnmatch 首匹配；无内置 cfdream_ 默认。
    assert resolve_capture_policy("cfdream_generate_image", None, _CAPTURE_PATTERNS) == "rehost"
    assert resolve_capture_policy("cfdream_task_wait", None, _CAPTURE_PATTERNS) == "rehost"
    assert resolve_capture_policy("image_search", None, _CAPTURE_PATTERNS) == "register"


def test_policy_no_config_is_off():
    # cfdream_ 不再有内置默认：无配置 → off（零意外落盘）。
    assert resolve_capture_policy("cfdream_generate_image") == "off"
    assert resolve_capture_policy("cfdream_generate_image", None, None) == "off"


def test_policy_metadata_override():
    assert resolve_capture_policy("image_search", {"materials_capture": "register"}) == "register"
    # metadata 优先于配置：配置说 rehost，metadata 说 off → off。
    assert resolve_capture_policy("cfdream_generate_image", {"materials_capture": "off"}, _CAPTURE_PATTERNS) == "off"


def test_policy_default_off():
    assert resolve_capture_policy("bash", None, _CAPTURE_PATTERNS) == "off"
    assert resolve_capture_policy("web_search", None, _CAPTURE_PATTERNS) == "off"


# --- url 抽取：按 per-tool JSON 字段路径（不再靠 artifact:true 门控）-------------


def test_url_path_resolution():
    assert resolve_url_path("cfdream_generate_image", None, _URLPATH_PATTERNS) == "urls"
    assert resolve_url_path("anything", None, None) is None
    # metadata 优先
    assert resolve_url_path("x", {"materials_url_path": "data.images"}, _URLPATH_PATTERNS) == "data.images"


def test_extract_top_level_urls_field():
    res = _cfdream_result(["https://cdn.cfgpu.com/a.png"])
    assert _extract_urls_by_path(res, "urls") == ["https://cdn.cfgpu.com/a.png"]


def test_extract_without_artifact_flag_still_reads_path():
    # artifact:true 门控已废：只看字段路径，无标志也抽得到。
    no_flag = _cfdream_result(["https://cdn.cfgpu.com/a.png"], artifact=False)
    assert _extract_urls_by_path(no_flag, "urls") == ["https://cdn.cfgpu.com/a.png"]


def test_extract_no_path_is_empty():
    res = _cfdream_result(["https://cdn.cfgpu.com/a.png"])
    assert _extract_urls_by_path(res, None) == []


def test_extract_nested_wildcard_path():
    # image_search 真实结构：results[*].image_url
    body = {"results": [{"image_url": "https://img/a.png"}, {"image_url": "https://img/b.png"}, {"image_url": ""}]}
    res = ToolMessage(content=json.dumps(body), tool_call_id="tc_1", name="image_search")
    assert _extract_urls_by_path(res, "results[*].image_url") == ["https://img/a.png", "https://img/b.png"]


def test_extract_skips_error_dict():
    err = ToolMessage(content=json.dumps({"error": True, "error_type": "content_blocked", "message": "x"}), tool_call_id="tc_1", name="cfdream_generate_image")
    assert _extract_urls_by_path(err, "urls") == []


def test_extract_skips_async_stub():
    stub = ToolMessage(content=json.dumps({"task_id": "task-abc", "status": "pending"}), tool_call_id="tc_1", name="cfdream_generate_image")
    assert _extract_urls_by_path(stub, "urls") == []


def test_infer_kind_by_ext():
    assert _infer_kind("https://cdn.cfgpu.com/x.mp4") == "video"
    assert _infer_kind("https://cdn.cfgpu.com/x.png") == "image"
    assert _infer_kind("https://cdn.cfgpu.com/x.mp3") == "audio"


# --- rehost 落盘 + 双轨改写 -------------------------------------------------


@pytest.mark.asyncio
async def test_rehost_registers_oss_path_and_dual_track():
    up = _FakeUploader()
    result = _cfdream_result(["https://cdn.cfgpu.com/img-abc.png"])
    out = await _run(result, fake_uploader=up)

    assert isinstance(out, Command)
    m1 = _mid_of("https://cdn.cfgpu.com/img-abc.png")
    mats = out.update["materials"]
    assert mats[m1]["ref_type"] == "oss_path"
    assert mats[m1]["ref"] == "agent-artifacts/t1/images/img-abc.png"
    assert mats[m1]["origin_url"] == "https://cdn.cfgpu.com/img-abc.png"
    assert mats[m1]["stable"] is True
    assert mats[m1].get("display") is None  # capture 不设 display（D14：归 MessageStream）
    assert up.calls == [("https://cdn.cfgpu.com/img-abc.png", "t1")]

    # content 去 url 留 id；artifact 不再由 capture 建 items（无 structured_content → None）
    tm = out.update["messages"][0]
    body = json.loads(tm.content)
    assert body["materials"] == [m1]
    assert "urls" not in body and "http" not in tm.content
    assert tm.artifact is None


@pytest.mark.asyncio
async def test_rewrite_preserves_mcp_structured_content():
    # cfdream split routes usage/payload into MCP structuredContent → arrives as
    # ToolMessage.artifact["structured_content"]; the media-capture rewrite must carry it
    # forward next to items (else the client artifact event loses usage/payload).
    up = _FakeUploader()
    result = _cfdream_result(["https://cdn.cfgpu.com/img-abc.png"])
    sc = {"usage": {"totalTokens": 100}, "payload": {"model": "doubao", "prompt": "x"}}
    result.artifact = {"structured_content": sc}
    out = await _run(result, fake_uploader=up)

    tm = out.update["messages"][0]
    # capture 只透传 structured_content（无 items）；emit 端再投影 items。
    assert tm.artifact == {"structured_content": sc}


@pytest.mark.asyncio
async def test_rewrite_keeps_terminal_status_hint():
    # cfdream annotate_artifact stamps a terminal status alongside artifact:true; the
    # content rewrite strips urls but must keep the hint so the LLM stops polling
    # task_status/task_wait on an already-finished generation.
    up = _FakeUploader()
    hint = "Success. URLs already generated; no further task_status/task_wait polling needed."
    result = _cfdream_result(
        ["https://cdn.cfgpu.com/img-abc.png"], name="cfdream_task_wait", extra={"status": hint}
    )
    out = await _run(result, fake_uploader=up, name="cfdream_task_wait")

    body = json.loads(out.update["messages"][0].content)
    assert body["status"] == hint
    assert body["materials"] == [_mid_of("https://cdn.cfgpu.com/img-abc.png")]
    assert "urls" not in body and "http" not in out.update["messages"][0].content


@pytest.mark.asyncio
async def test_register_policy_keeps_global_url_no_upload():
    up = _FakeUploader()
    result = _cfdream_result(["https://third.cdn/x.png"], name="image_search")
    out = await _run(result, fake_uploader=up, metadata={"materials_capture": "register"}, name="image_search")

    m1 = _mid_of("https://third.cdn/x.png")
    mats = out.update["materials"]
    assert mats[m1]["ref_type"] == "global_url"
    assert mats[m1]["ref"] == "https://third.cdn/x.png"
    assert mats[m1].get("display") is None  # capture 不设 display（D14：归 MessageStream）
    assert up.calls == []  # register 不 fetch/不 upload


@pytest.mark.asyncio
async def test_rehost_failure_marks_unstable_not_deliverable():
    # fail-open（I5）：rehost 落不了盘 → 置 stable=false 登记 global_url 续跑（不阻断 run）；
    # 临期 url 只进 ref，content 改写后无 http（不进 LLM/checkpoint）。emit 端按 stable 过滤不交付。
    up = _FakeUploader(fail=True)
    result = _cfdream_result(["https://cdn.cfgpu.com/img-abc.png"])
    out = await _run(result, fake_uploader=up)

    m1 = _mid_of("https://cdn.cfgpu.com/img-abc.png")
    mats = out.update["materials"]
    assert mats[m1]["stable"] is False
    assert mats[m1]["ref_type"] == "global_url"
    assert mats[m1].get("display") is None
    assert "http" not in out.update["messages"][0].content


@pytest.mark.asyncio
async def test_our_object_url_shortcuts_no_rehost():
    # D4：cfdream 结果里若已是我方 OSS 对象 → 登记 oss_path，跳过 fetch
    up = _FakeUploader()
    result = _cfdream_result(["https://oss.cfgpu.com/agent-artifacts/t1/x.png"])
    out = await _run(result, fake_uploader=up)

    m1 = _mid_of("https://oss.cfgpu.com/agent-artifacts/t1/x.png")
    mats = out.update["materials"]
    assert mats[m1]["ref_type"] == "oss_path"
    assert mats[m1]["ref"] == "agent-artifacts/t1/x.png"
    assert up.calls == []


@pytest.mark.asyncio
async def test_task_wait_replay_dedup_no_double_rehost():
    up = _FakeUploader()
    url = "https://cdn.cfgpu.com/vid-abc.mp4"
    first = await _run(_cfdream_result([url], name="cfdream_task_wait"), fake_uploader=up, name="cfdream_task_wait")
    mats = dict(first.update["materials"])
    # 重放：同 url 再浮现，materials 已含 m1
    second = await _run(_cfdream_result([url], name="cfdream_task_wait"), fake_uploader=up, materials=mats, name="cfdream_task_wait")

    assert len(up.calls) == 1  # 只 rehost 一次（不双计费）
    assert "materials" not in second.update  # 无新建
    assert json.loads(second.update["messages"][0].content)["materials"] == [_mid_of(url)]  # 仍指向既有 id


@pytest.mark.asyncio
async def test_off_policy_passes_result_through():
    up = _FakeUploader()
    result = _cfdream_result(["https://cdn.cfgpu.com/a.png"], name="bash")
    out = await _run(result, fake_uploader=up, name="bash")
    assert out is result  # off：不接管，原样放行
    assert up.calls == []


@pytest.mark.asyncio
async def test_multi_url_distinct_content_derived_ids():
    up = _FakeUploader()
    result = _cfdream_result(["https://cdn.cfgpu.com/a.png", "https://cdn.cfgpu.com/b.png"])
    out = await _run(result, fake_uploader=up)
    a, b = _mid_of("https://cdn.cfgpu.com/a.png"), _mid_of("https://cdn.cfgpu.com/b.png")
    assert set(out.update["materials"]) == {a, b}
    assert json.loads(out.update["messages"][0].content)["materials"] == [a, b]
    assert len(up.calls) == 2
