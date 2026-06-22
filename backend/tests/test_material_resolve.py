"""P2 — MaterialResolve 出口签发（cfgpu-docs/materials.md §4.3, materials-impl-plan.md P2）。

覆盖 MaterialsMiddleware._resolve_outgate / wrap_tool_call：
id→presigned、oss_path→presigned、完整 url 透传、截断 url/悬空 id→error 且 cfgpu 未被调用、
prose 不被篡改、非 cfgpu 工具放行，以及 resolve+capture 合并的安全护栏（presigned 不回灌 content）。
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace

import pytest
from langchain_core.messages import ToolMessage

import deerflow.agents.materials.middleware as mw
from deerflow.agents.materials.middleware import MaterialsMiddleware

# --- 夹具：镜像 langchain ToolCallRequest 的 dataclass + override 语义 ---------


@dataclass
class FakeRequest:
    tool_call: dict
    state: dict | None = field(default_factory=dict)

    def override(self, **overrides):
        return replace(self, **overrides)


def _request(args, materials=None, name="cfgpu_edit_image"):
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


# --- error：截断 url / 悬空 id → cfgpu 未被调用 -----------------------------


def test_unknown_id_errors_without_calling_cfgpu(fake_oss):
    called = {"n": 0}

    def handler(_request):
        called["n"] += 1
        return ToolMessage(content="done", tool_call_id="tc_1", name="cfgpu_edit_image")

    out = MaterialsMiddleware().wrap_tool_call(_request({"image": "m99"}, materials={}), handler)
    assert isinstance(out, ToolMessage)
    assert out.status == "error"
    assert "m99" in out.content
    assert called["n"] == 0  # 解析失败短路，绝不调 cfgpu(计费)


def test_truncated_url_errors_without_calling_cfgpu(fake_oss):
    called = {"n": 0}

    def handler(_request):
        called["n"] += 1
        return ToolMessage(content="done", tool_call_id="tc_1", name="cfgpu_edit_image")

    out = MaterialsMiddleware().wrap_tool_call(_request({"image": "https://oss.cfgpu.com"}), handler)
    assert isinstance(out, ToolMessage)
    assert out.status == "error"
    assert called["n"] == 0


# --- prose / 非 cfgpu 工具不被触碰 -----------------------------------------


def test_prompt_prose_not_corrupted(fake_oss):
    materials = {"m1": _img("oss_path", "agent-artifacts/t1/a.png")}
    handler, box = _capturing_handler()
    args = {"prompt": "a cinematic photo of m3 cat in 4k", "image": "m1"}
    MaterialsMiddleware().wrap_tool_call(_request(args, materials), handler)
    # 带空白的 prompt 整叶放过；只解析 image 叶
    assert box["req"].tool_call["args"]["prompt"] == "a cinematic photo of m3 cat in 4k"
    assert box["req"].tool_call["args"]["image"].startswith("https://oss.test/")


def test_non_cfgpu_tool_untouched(fake_oss):
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


# --- 安全护栏：presigned 只活在 request，不回灌下行 content/.artifact --------


def test_guardrail_presigned_only_in_request_not_in_result(fake_oss):
    materials = {"m1": _img("oss_path", "agent-artifacts/t1/a.png")}

    captured: dict = {}

    def handler(request):
        captured["signed"] = request.tool_call["args"]["image"]
        # cfgpu 回 ToolMessage：content/.artifact 是产物 object_key，绝不回声入参 presigned
        return ToolMessage(
            content="已编辑：agent-artifacts/t1/out.png",
            tool_call_id="tc_1",
            name="cfgpu_edit_image",
            artifact="agent-artifacts/t1/out.png",
        )

    result = MaterialsMiddleware().wrap_tool_call(_request({"image": "m1"}, materials), handler)

    # presigned 确实进了流向 cfgpu 的 request
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
        return ToolMessage(content="done", tool_call_id="tc_1", name="cfgpu_edit_image")

    await MaterialsMiddleware().awrap_tool_call(_request({"image": "m1"}, materials), handler)
    assert box["req"].tool_call["args"]["image"].startswith("https://oss.test/")

    # 解析失败：async 路径同样短路，handler 不被 await
    called = {"n": 0}

    async def handler2(_request):
        called["n"] += 1
        return ToolMessage(content="x", tool_call_id="tc_1", name="cfgpu_edit_image")

    out = await MaterialsMiddleware().awrap_tool_call(_request({"image": "m99"}, materials={}), handler2)
    assert isinstance(out, ToolMessage) and out.status == "error"
    assert called["n"] == 0
