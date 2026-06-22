"""Unified materials middleware (cfgpu-docs/materials.md §5, materials-impl-plan.md P1–P4).

§4.1 Ingest / §4.2 Capture / §4.3 Resolve / §6 台账 四个角色不是四条独立策略，而是
**同一个 registry 子系统的四个 hook**（共享 ``materials`` channel + ``stable_ref→id``
索引 + ``resolve_or_register`` 原语，且 Resolve/Capture 都要贴最内工具层）。统一实现为
这一个 ``MaterialsMiddleware``，渐进长大、factory 注册一次：

- ``before_agent``      = Ingest（§4.1）—— **见下方说明，消费侧已承载，故暂空**。
- ``wrap_tool_call``    = pre ``_resolve_outgate``（§4.3, P2 ✓）+ post ``_capture``（§4.2, P3 ✓）。
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

P3（产物准入⊥转存，取代 present_urls 手动 rehost）：``_capture`` 在 awrap post 段（async-only，
rehost 是 fetch+upload 网络）按 per-tool policy 三态（policy.py）处理工具产物。**准入信号=cfgpu
自声明的顶层 ``artifact: true`` + ``urls``**（result_structure.json 权威），非媒体结果（status/
error/list_models）无此标志 → 抽不到 url → no-op。命中后：我方对象(D4)登记 oss_path 跳过 fetch /
``rehost`` policy fetch+upload→object_key / ``register`` policy 保持 global_url 不落盘 / rehost
失败→stable=false 不作交付物(I5)。``stable_ref→id`` 反查去重保 task_wait 重放幂等(不双计费)。
**双轨改写 ToolMessage**：content 去 url 留 id 形态(I10)、``.artifact`` 写稳定 ref 供客户端。

before_agent（Ingest）现状：上行素材登记发生在**消费侧** ``agent_runner._normalize_messages``
——唯一能保证「url 永不进入持久化 HumanMessage / 首个 checkpoint」的位置（before_agent 是图内
节点，其重写晚于首个 checkpoint）。故本类 ``before_agent`` 无消费侧职责，留作未来图内 ingest
（如 gateway uploaded_files 路径）的挂点。
"""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any, override
from urllib.parse import urlsplit

from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import ToolMessage
from langgraph.prebuilt.tool_node import ToolCallRequest
from langgraph.types import Command

from deerflow.agents.materials.policy import CapturePolicy, resolve_capture_policy
from deerflow.agents.materials.registry import build_reverse_index, classify_ref, is_our_object_key, register, stable_ref
from deerflow.agents.materials.types import Kind, Material
from deerflow.oss.client import get_oss_client
from deerflow.oss.uploader import get_oss_uploader

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


# ── Capture（§4.2，P3）：工具产物准入⊥转存 + 双轨改写 ────────────────────────

_KIND_BY_EXT: dict[str, Kind] = {
    ".png": "image", ".jpg": "image", ".jpeg": "image", ".webp": "image", ".gif": "image",
    ".mp4": "video", ".mov": "video", ".webm": "video", ".mkv": "video",
    ".mp3": "audio", ".wav": "audio", ".m4a": "audio", ".flac": "audio",
}


def _content_text(result: Any) -> str | None:
    """从 ToolMessage 取拼接后的文本（content 可能是 str 或 content-block list）。"""
    content = getattr(result, "content", None)
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                parts.append(str(block.get("text", "")))
            elif isinstance(block, str):
                parts.append(block)
        return "".join(parts) if parts else None
    return None


def _parse_content_json(result: Any) -> Any:
    text = _content_text(result)
    if not text:
        return None
    try:
        return json.loads(text)
    except (ValueError, TypeError):
        return None


def _extract_artifact_urls(result: Any) -> list[str]:
    """cfgpu 媒体产物 url 抽取——**只认 cfgpu 自声明的 `artifact: true` + `urls`**。

    cfgpu 四个媒体工具在结果带生成媒体时加顶层 ``artifact: true``（result_structure.json 权威，
    由 tool_registry.annotate_artifact() 添加）；status-only 信封 / error dict / list_models
    从不带。故探测既非扫自由文本（§4.2 禁），也不靠工具名猜字段路径——读 cfgpu 自声明信号即可。
    """
    parsed = _parse_content_json(result)
    if not isinstance(parsed, dict) or parsed.get("artifact") is not True:
        return []
    urls = parsed.get("urls")
    if not isinstance(urls, list):
        return []
    return [u for u in urls if isinstance(u, str) and u]


def _infer_kind(url: str) -> Kind:
    suffix = urlsplit(url).path.rsplit("/", 1)[-1]
    dot = suffix.rfind(".")
    if dot != -1:
        ext = suffix[dot:].lower()
        if ext in _KIND_BY_EXT:
            return _KIND_BY_EXT[ext]
    return "image"


def _thread_id_from_request(request: ToolCallRequest) -> str:
    """cfgpu rehost 的 object_key 前缀。runtime.context → config → 兜底 default。"""
    runtime = getattr(request, "runtime", None)
    if runtime is not None:
        ctx = getattr(runtime, "context", None)
        if ctx:
            tid = ctx.get("thread_id") if isinstance(ctx, dict) else getattr(ctx, "thread_id", None)
            if tid:
                return str(tid)
        cfg = getattr(runtime, "config", None) or {}
        tid = cfg.get("configurable", {}).get("thread_id") if isinstance(cfg, dict) else None
        if tid:
            return str(tid)
    return "default"


async def _rehost(url: str, thread_id: str) -> str:
    """fetch 临期 url → 落我方 OSS → object_key。OSS 未启用 / 失败均 raise（调用方置 stable=false）。"""
    uploader = get_oss_uploader()
    if uploader is None:
        raise RuntimeError("OSS uploader unavailable — cannot re-host")
    return await uploader.rehost_url(url, thread_id)


def _rewrite_result(result: ToolMessage, ordered_urls: list[str], id_for_url: dict[str, str], items: list[dict]) -> ToolMessage:
    """双轨改写：``.content`` 去 url 留 id 形态（I10），``.artifact`` 写稳定 ref 供客户端。"""
    parsed = _parse_content_json(result)
    ids = [id_for_url[u] for u in ordered_urls if u in id_for_url]
    if isinstance(parsed, dict):
        body = dict(parsed)
        body.pop("urls", None)
        body.pop("expires_at", None)  # url-bound，url 已去
        body.pop("artifact", None)
        body["materials"] = ids  # 后续引用用 id
        new_content = json.dumps(body, ensure_ascii=False)
    else:
        new_content = json.dumps({"materials": ids}, ensure_ascii=False)
    return ToolMessage(
        content=new_content,
        tool_call_id=result.tool_call_id,
        name=result.name,
        status=getattr(result, "status", None) or "success",
        artifact={"items": items},
    )


class MaterialsMiddleware(AgentMiddleware):
    """Registry 子系统的统一中间件（P2 出口签发 + P3 Capture 准入/转存；台账自 P4 起加）。"""

    async def _capture(self, result: ToolMessage | Command, *, policy: CapturePolicy, materials: dict[str, Material], thread_id: str) -> Command | None:
        """工具产物 Capture：rehost/register + 双轨改写。无可捕获产物 → None（原样放行）。

        只作用于 ToolMessage（cfgpu MCP 结果）；Command（present_* 自管 artifact）暂不接管。
        """
        if policy == "off" or not isinstance(result, ToolMessage):
            return None
        urls = _extract_artifact_urls(result)
        if not urls:
            return None

        working = dict(materials)
        index = build_reverse_index(working)
        updates: dict[str, Material] = {}
        id_for_url: dict[str, str] = {}
        items: list[dict] = []

        for url in urls:
            ref_type, ref = classify_ref(url)
            skey = stable_ref(ref_type, ref)
            hit = index.get(skey)
            if hit is not None:
                id_for_url[url] = hit  # 幂等：task_wait 重放 / 同批重复 url 不二次 rehost（不双计费）
                continue
            kind = _infer_kind(url)
            if ref_type == "oss_path":
                # D4：已是我方对象 → 登记 oss_path，跳过 fetch
                mid, upd = register(working, kind=kind, origin="generate", ref_type="oss_path", ref=ref, display=True, stable=True)
            elif policy == "rehost":
                object_key: str | None = None
                try:
                    object_key = await _rehost(url, thread_id)
                except Exception as exc:  # noqa: BLE001 — rehost 失败不得阻断 run
                    logger.warning("MaterialsCapture: rehost failed for %s (%s) — marking unstable", url, exc)
                if object_key is not None:
                    mid, upd = register(working, kind=kind, origin="generate", ref_type="oss_path", ref=object_key, origin_url=url, display=True, stable=True)
                else:
                    # 临期 url 落不了盘 → stable=false + 不作交付物（display 缺省，I5）
                    mid, upd = register(working, kind=kind, origin="generate", ref_type="global_url", ref=url, origin_url=url, stable=False)
            else:  # register：仅准入，ref 保持 global_url，不 fetch/不 upload
                mid, upd = register(working, kind=kind, origin="generate", ref_type="global_url", ref=url, display=True, stable=True)
            updates.update(upd)
            working.update(upd)
            index[skey] = mid
            id_for_url[url] = mid
            mat = upd[mid]
            items.append({"id": mid, "ref": mat["ref"], "kind": kind, "stable": mat.get("stable", True)})

        new_msg = _rewrite_result(result, urls, id_for_url, items)
        update: dict[str, Any] = {"messages": [new_msg]}
        if updates:
            update["materials"] = updates
        return Command(update=update)

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

    def _capture_policy(self, request: ToolCallRequest) -> CapturePolicy:
        tool = getattr(request, "tool", None)
        metadata = getattr(tool, "metadata", None) if tool is not None else None
        return resolve_capture_policy(request.tool_call.get("name") or "", metadata)

    @override
    def wrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], ToolMessage | Command],
    ) -> ToolMessage | Command:
        # 同步路径只做出口签发，不做 Capture：Capture 的 rehost 是 fetch+upload（网络），
        # 须 async；cfgpu/MCP 媒体工具走 awrap（async）路径，同步路径不承载可 rehost 的产物。
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
        result = await handler(resolved)
        policy = self._capture_policy(request)
        if policy == "off":
            return result
        state = getattr(request, "state", None)
        materials = state.get("materials") if isinstance(state, dict) else None
        captured = await self._capture(
            result,
            policy=policy,
            materials=materials or {},
            thread_id=_thread_id_from_request(request),
        )
        return captured if captured is not None else result
