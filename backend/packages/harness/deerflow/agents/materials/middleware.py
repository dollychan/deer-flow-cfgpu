"""Unified materials middleware (cfgpu-docs/materials.md §5, materials-impl-plan.md P1–P4).

§4.1 Ingest / §4.2 Capture / §4.3 Resolve / §6 台账 四个角色不是四条独立策略，而是
**同一个 registry 子系统的四个 hook**（共享 ``materials`` channel + ``stable_ref→id``
索引 + ``resolve_or_register`` 原语，且 Resolve/Capture 都要贴最内工具层）。统一实现为
这一个 ``MaterialsMiddleware``，渐进长大、factory 注册一次：

- ``before_agent``      = Ingest（§4.1）—— **见下方说明，消费侧已承载，故暂空**。
- ``wrap_tool_call``    = pre ``_resolve_outgate``（§4.3, P2 ✓）+ post ``_capture``（§4.2, P3）。
- ``wrap_model_call``   = ``<materials>`` 台账注入（§6, P4）。

洋葱位（§5）：放 MessageStream **内层**（factory 中 append 在 MessageStreamMiddleware 之后
= wrap_tool_call 洋葱的内层），使 ``_resolve_outgate`` 见最终入参（无人再改 url）、``_capture``
在 MessageStream emit 前已稳定化。护栏：``_resolve_outgate`` 签发的 presigned 只活在流向
cfgpu 的 ``request``，``_capture``（P3）只读 ``result.artifact`` 不读 ``request`` → 凭证不
回灌 content（I9）。本类不引用已签 url 做任何下行写入，是单中间件合并 resolve+capture 的安全前提。

P2（出口签发，取代 ArtifactUrlGuard）：``_resolve_outgate`` 扫 cfgpu MCP 工具入参的**整叶
引用 token**——material id → 查台账解析 ref；我方 oss_path → 现签 presigned（本地 HMAC）；
完整第三方 url / asset_url → 原样透传；id 形但台账无 / http 形但非完整 url（summarization
截断残骸）→ 不静默、不调 cfgpu(计费)，回 error ToolMessage 引导用台账 id 重引用。**只动整叶
单 token，带内部空白的 prose（prompt 文本）一律不碰**，避免篡改作者内容。

before_agent（Ingest）现状：上行素材登记发生在**消费侧** ``agent_runner._normalize_messages``
——唯一能保证「url 永不进入持久化 HumanMessage / 首个 checkpoint」的位置（before_agent 是图内
节点，其重写晚于首个 checkpoint）。故本类 ``before_agent`` 无消费侧职责，留作未来图内 ingest
（如 gateway uploaded_files 路径）的挂点。
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Any, override
from urllib.parse import urlsplit

from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import ToolMessage
from langgraph.prebuilt.tool_node import ToolCallRequest
from langgraph.types import Command

from deerflow.agents.materials.registry import classify_ref, is_our_object_key
from deerflow.agents.materials.types import Material
from deerflow.oss.client import get_oss_client

logger = logging.getLogger(__name__)

# cfgpu MCP 工具名前缀（langchain-mcp-adapters 命名 = server_name + "_" + tool，
# server 名为 "cfgpu" → cfgpu_generate_image / cfgpu_task_wait / ...）。出口签发只作用于
# 这些工具的入参，普通工具（bash/present_files/...）原样放行。
_CFGPU_PREFIX = "cfgpu_"


class _UnresolvableRef(Exception):
    """整叶引用 token 既非可解析 id、又非完整 url、又非我方 object_key —— 挡在调用 cfgpu 之前。"""

    def __init__(self, token: str, reason: str) -> None:
        super().__init__(reason)
        self.token = token
        self.reason = reason


def _is_id_token(token: str) -> bool:
    """material id 形态 ``m\\d+``（与 ``new_material_id`` 一致）。"""
    return len(token) >= 2 and token[0] == "m" and token[1:].isdigit()


def _is_full_url(token: str) -> bool:
    """完整可取 url：http(s) scheme + 非空 host + 非空对象 path（非仅 ``/``）。

    截断残骸（``https://`` / ``http://host`` 无 path）判 False → 出口报错，不放给 cfgpu。
    """
    parts = urlsplit(token)
    return parts.scheme in ("http", "https") and bool(parts.netloc) and parts.path not in ("", "/")


def _presign(object_key: str) -> str:
    """我方 object_key → presigned GET url（本地 HMAC，无网络）。

    OSS 未启用（本地 dev）时回裸 object_key 兜底（BUG-027 bare-key 模式）——不报错，
    保 OSS-off 环境可跑；该模式下客户端/cfgpu 按约定自取，属已知限制。
    """
    oss = get_oss_client()
    if oss is None:
        return object_key
    return oss.presign(object_key)


def _resolve_token(token: str, materials: dict[str, Material]) -> str:
    """归一单个整叶引用 token 为流向 cfgpu 的 ref；无法解析则 raise ``_UnresolvableRef``。

    - id ``m3``：台账命中 → 解析其 ref（oss_path 现签 / 其余原样）；台账无 → 报错（悬空/截断）。
    - 完整 http(s) url：我方对象 → 现签 presigned；第三方 → 原样透传。
    - http 形但非完整 url（截断残骸）→ 报错。
    - 裸我方 object_key（``agent-artifacts/``）→ 现签 presigned。
    - 其余（第三方裸路径 / 非引用串）→ 原样（不碰）。
    """
    if _is_id_token(token):
        mat = materials.get(token)
        if mat is None:
            raise _UnresolvableRef(token, "台账中无此 material id")
        ref_type = mat.get("ref_type")
        ref = mat.get("ref", "")
        return _presign(ref) if ref_type == "oss_path" else ref

    lowered = token[:8].lower()
    if lowered.startswith("http://") or lowered.startswith("https://"):
        if not _is_full_url(token):
            raise _UnresolvableRef(token, "URL 不完整（疑似被摘要截断）")
        ref_type, ref = classify_ref(token)
        return _presign(ref) if ref_type == "oss_path" else token

    if "/" in token and is_our_object_key(token):
        return _presign(token.lstrip("/"))

    return token


def _resolve_value(value: Any, materials: dict[str, Material]) -> Any:
    """递归归一工具入参的每个字符串叶。仅动**整叶单 token**（无内部空白）。

    带空白的字符串 = prose（如 prompt 文本），整叶放过——绝不在散文里搜 id/url 子串改写
    （正则分不出「引用」vs「顺带提及」，改写即篡改作者内容）。
    """
    if isinstance(value, str):
        token = value.strip()
        if not token or any(ch.isspace() for ch in token):
            return value
        resolved = _resolve_token(token, materials)
        return resolved if resolved != token else value
    if isinstance(value, dict):
        return {k: _resolve_value(v, materials) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_value(v, materials) for v in value]
    if isinstance(value, tuple):
        return tuple(_resolve_value(v, materials) for v in value)
    return value


class MaterialsMiddleware(AgentMiddleware):
    """Registry 子系统的统一中间件（P2：出口签发 ``_resolve_outgate``；Capture/台账自 P3 起加）。"""

    def _resolve_outgate(self, request: ToolCallRequest) -> ToolCallRequest | ToolMessage:
        """cfgpu 工具入参出口签发。返回改写后的 request，或解析失败的 error ToolMessage。"""
        tool_call = request.tool_call
        name = tool_call.get("name") or ""
        if not name.startswith(_CFGPU_PREFIX):
            return request

        args = tool_call.get("args")
        if not isinstance(args, (dict, list)):
            return request

        state = getattr(request, "state", None)
        materials = state.get("materials") if isinstance(state, dict) else None
        materials = materials or {}

        try:
            new_args = _resolve_value(args, materials)
        except _UnresolvableRef as exc:
            logger.info("MaterialsResolve: rejected unresolvable ref %r in %r (%s)", exc.token, name, exc.reason)
            return ToolMessage(
                content=(
                    f"无法解析素材引用 {exc.token!r}：{exc.reason}。"
                    "请改用素材台账中的 material id（如 m3）重新引用，"
                    "不要直接粘贴可能已失效或被截断的 URL。"
                ),
                tool_call_id=tool_call.get("id", ""),
                name=name,
                status="error",
            )

        if new_args == args:
            return request
        logger.info("MaterialsResolve: signed/resolved material ref(s) in %r args", name)
        return request.override(tool_call={**tool_call, "args": new_args})

    @override
    def wrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], ToolMessage | Command],
    ) -> ToolMessage | Command:
        resolved = self._resolve_outgate(request)
        if isinstance(resolved, ToolMessage):
            return resolved  # 解析失败：短路，不调 cfgpu(计费)
        return handler(resolved)

    @override
    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
    ) -> ToolMessage | Command:
        resolved = self._resolve_outgate(request)
        if isinstance(resolved, ToolMessage):
            return resolved
        return await handler(resolved)
