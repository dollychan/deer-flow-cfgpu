"""P2 — MaterialResolve 出口签发（cfgpu-docs/materials.md §4.3, materials-impl-plan.md P2）。

覆盖 MaterialsMiddleware._resolve_outgate / wrap_tool_call：
id→presigned、oss_path→presigned、完整 url 透传、截断 url/悬空 id→error 且 cfdream 未被调用、
prose 不被篡改、非 cfdream 工具放行，以及 resolve+capture 合并的安全护栏（presigned 不回灌 content）。
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace

import pytest
from langchain_core.messages import ToolMessage
from langgraph.types import Command

import deerflow.agents.materials.materialize as mz
import deerflow.agents.materials.middleware as mw
from deerflow.agents.materials.middleware import MaterialsMiddleware

# --- 夹具：镜像 langchain ToolCallRequest 的 dataclass + override 语义 ---------


@dataclass
class FakeRuntime:
    context: dict = field(default_factory=dict)
    config: dict = field(default_factory=dict)


@dataclass
class FakeRequest:
    tool_call: dict
    state: dict | None = field(default_factory=dict)
    runtime: FakeRuntime | None = None

    def override(self, **overrides):
        return replace(self, **overrides)


def _request(args, materials=None, name="cfdream_edit_image"):
    return FakeRequest(
        tool_call={"name": name, "args": args, "id": "tc_1"},
        state={"materials": materials} if materials is not None else {},
    )


def _capturing_handler():
    box: dict = {}

    def handler(request):
        box["req"] = request
        return ToolMessage(content="done", tool_call_id="tc_1", name=request.tool_call["name"])

    return handler, box


class _FakeOSS:
    """presign = 本地拼一个带签名 query 的 url（模拟 HMAC，无网络）。"""

    def presign(self, object_key: str) -> str:
        return f"https://oss.test/{object_key}?Signature=SIG"


@pytest.fixture
def fake_oss(monkeypatch):
    monkeypatch.setattr(mw, "get_oss_client", lambda: _FakeOSS())


def _img(ref_type: str, ref: str) -> dict:
    return {"id": "m1", "kind": "image", "origin": "uplink", "ref_type": ref_type, "ref": ref}


# --- id → presigned ---------------------------------------------------------


def test_id_oss_path_resolved_to_presigned(fake_oss):
    materials = {"m1": _img("oss_path", "agent-artifacts/t1/a.png")}
    handler, box = _capturing_handler()
    MaterialsMiddleware().wrap_tool_call(_request({"image": "m1"}, materials), handler)
    assert box["req"].tool_call["args"]["image"] == "https://oss.test/agent-artifacts/t1/a.png?Signature=SIG"


def test_id_global_url_resolved_to_raw_url(fake_oss):
    materials = {"m1": _img("global_url", "https://third.cdn/x.png")}
    handler, box = _capturing_handler()
    MaterialsMiddleware().wrap_tool_call(_request({"image": "m1"}, materials), handler)
    # 第三方 ref 原样透传，不签名
    assert box["req"].tool_call["args"]["image"] == "https://third.cdn/x.png"


def test_id_asset_url_passthrough(fake_oss):
    materials = {"m1": {"id": "m1", "kind": "asset", "origin": "tool", "ref_type": "asset_url", "ref": "asset://lib/logo"}}
    handler, box = _capturing_handler()
    MaterialsMiddleware().wrap_tool_call(_request({"ref": "m1"}, materials), handler)
    assert box["req"].tool_call["args"]["ref"] == "asset://lib/logo"


# --- 裸 object_key / url 透传 ----------------------------------------------


def test_bare_our_object_key_signed(fake_oss):
    handler, box = _capturing_handler()
    MaterialsMiddleware().wrap_tool_call(_request({"image": "agent-artifacts/t1/b.png"}), handler)
    assert box["req"].tool_call["args"]["image"] == "https://oss.test/agent-artifacts/t1/b.png?Signature=SIG"


def test_full_third_party_url_passthrough(fake_oss):
    handler, box = _capturing_handler()
    url = "https://third.cdn/x.png"
    MaterialsMiddleware().wrap_tool_call(_request({"image": url}), handler)
    assert box["req"].tool_call["args"]["image"] == url


def test_our_oss_full_url_resigned(fake_oss):
    # 我方 host 的完整（可能已失效）url → 抽 object_key 现签新 presigned
    handler, box = _capturing_handler()
    stale = "https://oss.cfgpu.com/agent-artifacts/t1/c.png?Signature=STALE"
    MaterialsMiddleware().wrap_tool_call(_request({"image": stale}), handler)
    assert box["req"].tool_call["args"]["image"] == "https://oss.test/agent-artifacts/t1/c.png?Signature=SIG"


def test_non_our_bare_path_untouched(fake_oss):
    # 第三方裸路径（非 agent-artifacts/）不碰，原样
    handler, box = _capturing_handler()
    MaterialsMiddleware().wrap_tool_call(_request({"image": "some/other/key.png"}), handler)
    assert box["req"].tool_call["args"]["image"] == "some/other/key.png"


# --- error：截断 url / 悬空 id → cfdream 未被调用 -----------------------------


def test_unknown_id_errors_without_calling_cfdream(fake_oss):
    called = {"n": 0}

    def handler(_request):
        called["n"] += 1
        return ToolMessage(content="done", tool_call_id="tc_1", name="cfdream_edit_image")

    out = MaterialsMiddleware().wrap_tool_call(_request({"image": "m99"}, materials={}), handler)
    assert isinstance(out, ToolMessage)
    assert out.status == "error"
    assert "m99" in out.content
    assert called["n"] == 0  # 解析失败短路，绝不调 cfdream(计费)


def test_truncated_url_errors_without_calling_cfdream(fake_oss):
    called = {"n": 0}

    def handler(_request):
        called["n"] += 1
        return ToolMessage(content="done", tool_call_id="tc_1", name="cfdream_edit_image")

    out = MaterialsMiddleware().wrap_tool_call(_request({"image": "https://oss.cfgpu.com"}), handler)
    assert isinstance(out, ToolMessage)
    assert out.status == "error"
    assert called["n"] == 0


# --- prose / 非 cfdream 工具不被触碰 -----------------------------------------


def test_prompt_prose_not_corrupted(fake_oss):
    materials = {"m1": _img("oss_path", "agent-artifacts/t1/a.png")}
    handler, box = _capturing_handler()
    args = {"prompt": "a cinematic photo of m3 cat in 4k", "image": "m1"}
    MaterialsMiddleware().wrap_tool_call(_request(args, materials), handler)
    # 带空白的 prompt 整叶放过；只解析 image 叶
    assert box["req"].tool_call["args"]["prompt"] == "a cinematic photo of m3 cat in 4k"
    assert box["req"].tool_call["args"]["image"].startswith("https://oss.test/")


def test_non_cfdream_tool_untouched(fake_oss):
    materials = {"m1": _img("oss_path", "agent-artifacts/t1/a.png")}
    handler, box = _capturing_handler()
    MaterialsMiddleware().wrap_tool_call(_request({"image": "m1"}, materials, name="bash"), handler)
    assert box["req"].tool_call["args"]["image"] == "m1"  # 普通工具不解析


def test_list_args_resolved(fake_oss):
    materials = {"m1": _img("oss_path", "agent-artifacts/t1/a.png"), "m2": _img("oss_path", "agent-artifacts/t1/b.png")}
    materials["m2"]["id"] = "m2"
    handler, box = _capturing_handler()
    MaterialsMiddleware().wrap_tool_call(_request({"images": ["m1", "m2"]}, materials), handler)
    out = box["req"].tool_call["args"]["images"]
    assert all(u.startswith("https://oss.test/") for u in out)


def test_understand_vision_images_and_video_resolved(fake_oss):
    """Paradigm C (materials §4.7): vision analysis is the cfdream MCP `understand_vision` tool.

    The lead passes only material ids; this seam (cfdream_* prefix) signs `images` (list) and
    `video` (scalar) to URLs before the call reaches the MCP, and the prose `prompt` is untouched.
    The MCP owns model selection + base64 — no deerflow-side analysis tool exists.
    """
    materials = {
        "m1": _img("oss_path", "agent-artifacts/t1/a.png"),
        "m2": {"id": "m2", "kind": "video", "origin": "generate", "ref_type": "global_url", "ref": "https://third.cdn/clip.mp4"},
    }
    handler, box = _capturing_handler()
    MaterialsMiddleware().wrap_tool_call(
        _request({"prompt": "describe m1 and m2", "images": ["m1"], "video": "m2"}, materials, name="cfdream_understand_vision"),
        handler,
    )
    args = box["req"].tool_call["args"]
    assert args["images"] == ["https://oss.test/agent-artifacts/t1/a.png?Signature=SIG"]
    assert args["video"] == "https://third.cdn/clip.mp4"
    assert args["prompt"] == "describe m1 and m2"  # prose 整段不碰


# --- 安全护栏：presigned 只活在 request，不回灌下行 content/.artifact --------


def test_guardrail_presigned_only_in_request_not_in_result(fake_oss):
    materials = {"m1": _img("oss_path", "agent-artifacts/t1/a.png")}

    captured: dict = {}

    def handler(request):
        captured["signed"] = request.tool_call["args"]["image"]
        # cfdream 回 ToolMessage：content/.artifact 是产物 object_key，绝不回声入参 presigned
        return ToolMessage(
            content="已编辑：agent-artifacts/t1/out.png",
            tool_call_id="tc_1",
            name="cfdream_edit_image",
            artifact="agent-artifacts/t1/out.png",
        )

    result = MaterialsMiddleware().wrap_tool_call(_request({"image": "m1"}, materials), handler)

    # presigned 确实进了流向 cfdream 的 request
    assert "Signature=SIG" in captured["signed"]
    # 但 P2 post 段不读 request、不回灌：下行 content/.artifact 零 presigned
    assert "Signature" not in result.content
    assert "Signature" not in (result.artifact or "")


# --- OSS 未启用兜底：回裸 object_key 不报错 --------------------------------


def test_oss_disabled_returns_bare_object_key(monkeypatch):
    monkeypatch.setattr(mw, "get_oss_client", lambda: None)
    materials = {"m1": _img("oss_path", "agent-artifacts/t1/a.png")}
    handler, box = _capturing_handler()
    MaterialsMiddleware().wrap_tool_call(_request({"image": "m1"}, materials), handler)
    assert box["req"].tool_call["args"]["image"] == "agent-artifacts/t1/a.png"


# --- 异步路径同等行为（awrap_tool_call）------------------------------------


@pytest.mark.asyncio
async def test_awrap_resolves_and_short_circuits(fake_oss):
    materials = {"m1": _img("oss_path", "agent-artifacts/t1/a.png")}
    box: dict = {}

    async def handler(request):
        box["req"] = request
        return ToolMessage(content="done", tool_call_id="tc_1", name="cfdream_edit_image")

    await MaterialsMiddleware().awrap_tool_call(_request({"image": "m1"}, materials), handler)
    assert box["req"].tool_call["args"]["image"].startswith("https://oss.test/")

    # 解析失败：async 路径同样短路，handler 不被 await
    called = {"n": 0}

    async def handler2(_request):
        called["n"] += 1
        return ToolMessage(content="x", tool_call_id="tc_1", name="cfdream_edit_image")

    out = await MaterialsMiddleware().awrap_tool_call(_request({"image": "m99"}, materials={}), handler2)
    assert isinstance(out, ToolMessage) and out.status == "error"
    assert called["n"] == 0


# --- local 素材：I14 resolve 挡 + awrap 自动 stage（§4.8.4, P10）---------------


def _local(mid: str, local_path: str) -> dict:
    return {"id": mid, "kind": "image", "origin": "local", "ref_type": "local", "local_path": local_path}


@pytest.mark.asyncio
async def test_local_material_rejected_when_not_stageable(fake_oss):
    """I14：纯 local 素材撞 resolve（无 runtime → 自动 stage 跳过）→ error，不调 cfdream。"""
    materials = {"m1": _local("m1", "/mnt/user-data/outputs/a.png")}
    called = {"n": 0}

    async def handler(_request):
        called["n"] += 1
        return ToolMessage(content="done", tool_call_id="tc_1", name="cfdream_edit_image")

    # 无 runtime → _thread_id_from_request 返回 "" → auto-stage 跳过 → resolve 按 I14 报错
    out = await MaterialsMiddleware().awrap_tool_call(_request({"image": "m1"}, materials), handler)
    assert isinstance(out, ToolMessage) and out.status == "error"
    assert "m1" in out.content
    assert called["n"] == 0


class _FakeUploader:
    def __init__(self) -> None:
        self.upload_calls: list = []

    async def upload_local_file(self, virtual_path, physical_path, thread_id):
        self.upload_calls.append((virtual_path, thread_id))
        return f"agent-artifacts/{thread_id}/files/{virtual_path.rsplit('/', 1)[-1]}"


@pytest.mark.asyncio
async def test_awrap_auto_stages_local_then_resolves(fake_oss, monkeypatch):
    """便利层（D16d）：cfdream 入参引用 local 素材 → awrap 前段自动上传 oss_path → resolve 签发。"""
    up = _FakeUploader()
    monkeypatch.setattr(mz, "get_oss_uploader", lambda: up)
    materials = {"m1": _local("m1", "/mnt/user-data/outputs/a.png")}
    req = FakeRequest(
        tool_call={"name": "cfdream_edit_image", "args": {"image": "m1"}, "id": "tc_1"},
        state={"materials": materials, "thread_data": {"outputs_path": "/host/threads/t1/user-data/outputs"}},
        runtime=FakeRuntime(context={"thread_id": "t1"}),
    )
    box: dict = {}

    async def handler(request):
        box["signed"] = request.tool_call["args"]["image"]
        return ToolMessage(content="ok", tool_call_id="tc_1", name="cfdream_edit_image")

    out = await MaterialsMiddleware().awrap_tool_call(req, handler)
    # 自动上传发生（懒上传命中真消费）
    assert up.upload_calls == [("/mnt/user-data/outputs/a.png", "t1")]
    # resolve 把升级后的 oss_path 签成 presigned 交给 cfdream
    assert box["signed"] == "https://oss.test/agent-artifacts/t1/files/a.png?Signature=SIG"
    # local→oss_path 升级折进结果 Command 以持久化
    assert isinstance(out, Command)
    assert out.update["materials"]["m1"]["ref_type"] == "oss_path"
