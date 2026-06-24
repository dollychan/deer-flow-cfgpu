"""Unified materials middleware (cfgpu-docs/materials.md §5, materials-impl-plan.md P1–P4).

§4.1 Ingest / §4.2 Capture / §4.3 Resolve / §6 台账 四个角色不是四条独立策略，而是
**同一个 registry 子系统的四个 hook**（共享 ``materials`` channel + ``stable_ref→id``
索引 + ``resolve_or_register`` 原语，且 Resolve/Capture 都要贴最内工具层）。统一实现为
这一个 ``MaterialsMiddleware``，渐进长大、factory 注册一次：

- ``before_agent``      = Ingest（§4.1）—— **见下方说明，消费侧已承载，故暂空**。
- ``wrap_tool_call``    = pre ``_resolve_outgate``（§4.3, P2 ✓）+ post ``_capture``（§4.2, P3 ✓）。
- ``wrap_model_call``   = ``<materials>`` 台账注入（§6, P4 ✓）。

洋葱位（§5）：放 MessageStream **内层**（factory 中 append 在 MessageStreamMiddleware 之后
= wrap_tool_call 洋葱的内层），使 ``_resolve_outgate`` 见最终入参（无人再改 url）、``_capture``
在 MessageStream emit 前已稳定化。护栏：``_resolve_outgate`` 签发的 presigned 只活在流向
cfdream 的 ``request``，``_capture``（P3）只读 ``result.artifact`` 不读 ``request`` → 凭证不
回灌 content（I9）。本类不引用已签 url 做任何下行写入，是单中间件合并 resolve+capture 的安全前提。

P2（出口签发，取代 ArtifactUrlGuard）：``_resolve_outgate`` 扫 cfdream MCP 工具入参的**整叶
引用 token**——material id → 查台账解析 ref；我方 oss_path → 现签 presigned（本地 HMAC）；
完整第三方 url / asset_url → 原样透传；id 形但台账无 / http 形但非完整 url（summarization
截断残骸）→ 不静默、不调 cfdream(计费)，回 error ToolMessage 引导用台账 id 重引用。**只动整叶
单 token，带内部空白的 prose（prompt 文本）一律不碰**，避免篡改作者内容。

P3（产物准入⊥转存，取代 present_urls 手动 rehost）：``_capture`` 在 awrap post 段（async-only，
rehost 是 fetch+upload 网络）按 per-tool policy 三态（policy.py）处理工具产物。**准入信号=cfdream
自声明的顶层 ``artifact: true`` + ``urls``**（result_structure.json 权威），非媒体结果（status/
error/list_models）无此标志 → 抽不到 url → no-op。命中后：我方对象(D4)登记 oss_path 跳过 fetch /
``rehost`` policy fetch+upload→object_key / ``register`` policy 保持 global_url 不落盘 / rehost
失败→**fail-open** 置 ``stable=false`` 登记 global_url 续跑（不阻断 run；emit 端按 stable 过滤
不交付，I5）。``stable_ref→id`` 反查去重保 task_wait 重放幂等(不双计费)。
**双轨改写 ToolMessage**：content 去 url 留 id 形态(I10)、``.artifact`` 写稳定 ref 供客户端。

before_agent（Ingest）现状：上行素材登记发生在**消费侧** ``agent_runner._normalize_messages``
——唯一能保证「url 永不进入持久化 HumanMessage / 首个 checkpoint」的位置（before_agent 是图内
节点，其重写晚于首个 checkpoint）。故本类 ``before_agent`` 无消费侧职责，留作未来图内 ingest
（如 gateway uploaded_files 路径）的挂点。
"""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, override
from urllib.parse import urlsplit

from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import ModelRequest, ModelResponse
from langchain_core.messages import HumanMessage, ToolMessage
from langgraph.prebuilt.tool_node import ToolCallRequest
from langgraph.types import Command

from deerflow.agents.materials.materialize import MaterializeOutcome, register_remote_url, rehost_remote_url, stage_to_oss
from deerflow.agents.materials.policy import CapturePolicy, resolve_capture_policy, resolve_url_path
from deerflow.agents.materials.registry import classify_ref, is_our_object_key, register
from deerflow.agents.materials.types import Kind, Material
from deerflow.config.paths import map_virtual_to_physical
from deerflow.oss.client import get_oss_client

logger = logging.getLogger(__name__)

# cfdream MCP 工具名前缀（langchain-mcp-adapters 命名 = server_name + "_" + tool，
# server 名为 "cfdream" → cfdream_generate_image / cfdream_task_wait / ...）。出口签发只作用于
# 这些工具的入参，普通工具（bash/present_files/...）原样放行。
_CFDREAM_PREFIX = "cfdream_"


class _UnresolvableRef(Exception):
    """整叶引用 token 既非可解析 id、又非完整 url、又非我方 object_key —— 挡在调用 cfdream 之前。"""

    def __init__(self, token: str, reason: str) -> None:
        super().__init__(reason)
        self.token = token
        self.reason = reason


_HEX = frozenset("0123456789abcdef")


def _is_id_token(token: str) -> bool:
    """material id 形态：内容派生 ``m_<hex>``（``material_id``，§B），或 legacy ``m\\d+``。

    ``m_`` 前缀确保不与散文词（如 ``made``/``cafe`` 这类纯 hex 串）撞——它们不带下划线。
    legacy ``m\\d+`` 分支保留：手填台账 id / 既有测试夹具仍用得到。
    """
    if token.startswith("m_") and len(token) > 2 and all(c in _HEX for c in token[2:]):
        return True
    return len(token) >= 2 and token[0] == "m" and token[1:].isdigit()


def _is_full_url(token: str) -> bool:
    """完整可取 url：http(s) scheme + 非空 host + 非空对象 path（非仅 ``/``）。

    截断残骸（``https://`` / ``http://host`` 无 path）判 False → 出口报错，不放给 cfdream。
    """
    parts = urlsplit(token)
    return parts.scheme in ("http", "https") and bool(parts.netloc) and parts.path not in ("", "/")


def _presign(object_key: str) -> str:
    """我方 object_key → presigned GET url（本地 HMAC，无网络）。

    OSS 未启用（本地 dev）时回裸 object_key 兜底（BUG-027 bare-key 模式）——不报错，
    保 OSS-off 环境可跑；该模式下客户端/cfdream 按约定自取，属已知限制。
    """
    oss = get_oss_client()
    if oss is None:
        return object_key
    return oss.presign(object_key)


def _resolve_token(token: str, materials: dict[str, Material]) -> str:
    """归一单个整叶引用 token 为流向 cfdream 的 ref；无法解析则 raise ``_UnresolvableRef``。

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
        if ref_type == "local":
            # I14（§4.8.4）：纯本地素材无远程 ref，resolve(sync/无网络)不得签发 → 报错引导先
            # present/stage（awrap 前段的自动 stage 已在此之前把它升级成 oss_path，正常不会撞到）。
            raise _UnresolvableRef(token, "该素材仅在本地（未上传），请先用 present_files 或让系统自动上传后再引用")
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


def _resolve_value_internal(value: Any, materials: dict[str, Material], collect_local: Callable[[str], None]) -> Any:
    """递归归一工具入参，同时收集 local id（用于 aresolve_outgate 批量 stage）。"""
    if isinstance(value, str):
        token = value.strip()
        if token and not any(ch.isspace() for ch in token) and _is_id_token(token):
            mat = materials.get(token)
            if mat is not None and mat.get("ref_type") == "local":
                collect_local(token)
        return value  # aresolve 在 stage 后再 resolve
    if isinstance(value, dict):
        return {k: _resolve_value_internal(v, materials, collect_local) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_value_internal(v, materials, collect_local) for v in value]
    if isinstance(value, tuple):
        return tuple(_resolve_value_internal(v, materials, collect_local) for v in value)
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


def _walk_json_path(node: Any, segments: Sequence[str]) -> list[str]:
    """按已切分的字段路径段下行，收集叶子字符串。``[*]`` 段在 list 上分叉。

    终点取值：str → [它]；list → 其中的 str 元素；其余 → []。中途 key 缺失 / 类型不符 → []。
    """
    if not segments:
        if isinstance(node, str):
            return [node]
        if isinstance(node, list):
            return [x for x in node if isinstance(x, str)]
        return []
    seg, rest = segments[0], segments[1:]
    wildcard = seg.endswith("[*]")
    key = seg[:-3] if wildcard else seg
    if not isinstance(node, dict) or key not in node:
        return []
    child = node[key]
    if wildcard:
        if not isinstance(child, list):
            return []
        out: list[str] = []
        for item in child:
            out.extend(_walk_json_path(item, rest))
        return out
    return _walk_json_path(child, rest)


def _extract_urls_by_path(result: Any, url_path: str | None) -> list[str]:
    """按 per-tool JSON 字段路径（``materials_url_path``）抽取产物 url——rehost/register 共用。

    路径语法：``.`` 分隔的键，每段可带 ``[*]`` list 通配（如 ``urls`` / ``results[*].image_url``）。
    无路径 / 非 JSON dict / 路径未命中 → []（无产物准入）。**不扫自由文本、不靠工具名猜**（§4.2）：
    cfdream 异步 stub / error / list_models 无 ``urls`` 字段 → 该路径自然抽空，无需 artifact 标志门控。
    """
    if not url_path:
        return []
    parsed = _parse_content_json(result)
    if not isinstance(parsed, dict):
        return []
    urls = _walk_json_path(parsed, url_path.split("."))
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
    """cfdream rehost 的 object_key 前缀。runtime.context → config → 兜底 ``"default"``。"""
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


def _rewrite_result(result: ToolMessage, ordered_urls: list[str], id_for_url: dict[str, str]) -> ToolMessage:
    """改写 ``.content`` 去 url 留 id 形态（I10），加 ``materials:[id...]`` 供后续引用 + emit 投影。

    **不再在此重建 ``.artifact`` items**（D14：rehost 与 emit artifact 平行隔离）——交付 items 由
    MessageStreamMiddleware 在 ``visibility==artifact`` 时按 ``materials`` id 投影。``.artifact`` 仅
    原样透传 MCP structuredContent（cfdream split 出的 usage/payload，客户端侧旁路），不透传会丢
    generate_* 的 usage/payload；无则置 None。**注意此处只是把 structuredContent 接力到外层**：
    MessageStreamMiddleware emit 完下行事件后会从 ToolMessage 上剥除它（``_strip_structured_side_channel``），
    故它不进 checkpoint —— content 进 LLM、structuredContent 只走下行给客户端，二者干净分离。
    """
    parsed = _parse_content_json(result)
    ids = [id_for_url[u] for u in ordered_urls if u in id_for_url]
    if isinstance(parsed, dict):
        body = dict(parsed)
        body.pop("urls", None)
        body.pop("expires_at", None)  # url-bound，url 已去
        body.pop("artifact", None)
        body["materials"] = ids  # 后续引用用 id；MessageStream 据此投影 artifact items
        new_content = json.dumps(body, ensure_ascii=False)
    else:
        new_content = json.dumps({"materials": ids}, ensure_ascii=False)
    new_artifact: dict[str, Any] | None = None
    orig_artifact = getattr(result, "artifact", None)
    if isinstance(orig_artifact, dict) and isinstance(orig_artifact.get("structured_content"), dict):
        new_artifact = {"structured_content": orig_artifact["structured_content"]}
    return ToolMessage(
        content=new_content,
        tool_call_id=result.tool_call_id,
        name=result.name,
        status=getattr(result, "status", None) or "success",
        artifact=new_artifact,
    )


# ── 台账注入（§6，P4）：每轮重建的 `<materials>` hidden current-turn context ──────────

_ORIGIN_LABELS: dict[str, str] = {"uplink": "上行", "generate": "生成", "tool": "工具", "local": "本地"}

# 全量列出阈值：素材数 ≤ 此值全列；超出则只列最近 N + 折叠早期为一行。D9：默认大，实际不折叠
# （id 是注册表主键永久可解析，折叠不丢可用性，只省 token）；windowing 留接口，调大默认即关闭。
_LEDGER_WINDOW = 50

# hidden 台账消息的标记（additional_kwargs）：仅注入 request override、不写回 history，故无需
# 靠它去重；保留供下游识别/审计（与 DynamicContext / SkillActivation 的 reminder 标记同惯例）。
_LEDGER_MARKER_KEY = "materials_ledger"


def _material_sort_key(mat: Material) -> tuple[int, int, int, str]:
    """台账排序（§6）：主键 ``turn``（产生轮次＝时序），次键 legacy ``mN`` 数值。

    内容派生 id（``m_<hex>``）无内在顺序，故时序靠 ``turn`` 体现；同轮 / 无 turn 时，legacy ``mN``
    退回数值序（保 ``test_ordered_by_numeric_id`` 等夹具），hash id 退回 id 串（确定性）。
    """
    mid = str(mat.get("id", ""))
    turn = mat.get("turn") or 0
    legacy = mid[1:].isdigit()
    # legacy mN → 数值序；hash id → 常量 0（同轮间靠 sorted 的**稳定性**保留 dict 插入序＝时序，
    # 因 merge_materials 把新素材 append 在末尾）。
    return (turn, 0 if legacy else 1, int(mid[1:]) if legacy else 0)


def _render_material_line(mat: Material) -> str:
    """单行：``- [id] kind (来源,第N轮) "caption" ref_type``——**零 url/object_key**（I9）。

    asset_url 带 scope 时尾标 ``※仅 {scope} 可用``（替 ref_type，对齐 §6 示例）；其余尾标 ref_type。
    """
    mid = mat.get("id", "?")
    kind = mat.get("kind", "?")
    origin = _ORIGIN_LABELS.get(str(mat.get("origin", "")), str(mat.get("origin", "")))
    turn = mat.get("turn")
    turn_part = f",第{turn}轮" if turn else ""
    caption = mat.get("caption")
    caption_part = f' "{caption}"' if caption else ""
    ref_type = mat.get("ref_type", "")
    scope = mat.get("scope")
    tail = f"※仅 {scope} 可用" if ref_type == "asset_url" and scope else ref_type
    if not mat.get("stable", True):
        tail = f"{tail} ⚠未落盘".strip()
    return f"- [{mid}] {kind} ({origin}{turn_part}){caption_part} {tail}".rstrip()


def render_materials_ledger(materials: dict[str, Material] | None, *, window: int = _LEDGER_WINDOW) -> str | None:
    """渲染 `<materials>` 台账块（§6）。空表→None（不注入）。**绝不含 url/object_key**（I9）。

    每轮重建（区别于 DynamicContext 冻结首轮）：素材会增长，台账须反映当前全量。按 id 数值升序。
    """
    if not materials:
        return None
    ordered = sorted(materials.values(), key=_material_sort_key)
    lines = ["<materials>"]
    if len(ordered) > window:
        folded = ordered[:-window]
        ordered = ordered[-window:]
        lines.append(f"- 另有 {len(folded)} 个早期素材，按 id 引用（id 永久可解析）")
    lines.extend(_render_material_line(mat) for mat in ordered)
    lines.append("用法: 引用素材请填其 material id（如 m3）；不要复述 url/object_key。")
    lines.append("</materials>")
    return "\n".join(lines)


def _build_ledger_message(materials: dict[str, Material] | None) -> HumanMessage | None:
    body = render_materials_ledger(materials)
    if body is None:
        return None
    return HumanMessage(
        content=f"<system-reminder>\n{body}\n</system-reminder>",
        additional_kwargs={"hide_from_ui": True, _LEDGER_MARKER_KEY: True},
    )


class MaterialsMiddleware(AgentMiddleware):
    """Registry 子系统的统一中间件（P2 出口签发 + P3 Capture 准入/转存 + P4 台账注入）。

    Capture 由 ``AgentConfig`` 驱动（D13+/D14，无 cfdream_ 硬编码），两组 per-tool fnmatch 配置经
    ``lead_agent`` factory 喂入：``capture_patterns``（policy 三态）、``url_path_patterns``（url 抽取
    JSON 字段路径）。**本类纯 capture，零 visibility/display 知识**——交付物判定（``display`` + live
    artifact items）由 ``MessageStreamMiddleware`` 在 ``visibility==artifact`` 时独立负责（D14：rehost
    与 emit artifact 平行隔离）。
    """

    def __init__(
        self,
        *,
        capture_patterns: Sequence[tuple[str, str]] | None = None,
        url_path_patterns: Sequence[tuple[str, str]] | None = None,
    ) -> None:
        super().__init__()
        # 有序 (fnmatch-pattern, value)；首匹配胜。builtin tool.metadata 优先于这些配置。
        self._capture_patterns: list[tuple[str, str]] = list(capture_patterns or [])
        self._url_path_patterns: list[tuple[str, str]] = list(url_path_patterns or [])

    def _inject_ledger(self, request: ModelRequest) -> ModelRequest | None:
        """读 state.materials 渲染台账，作为 hidden current-turn 消息追加进 request override。

        只活在流向模型的 ``request``，不返回 state 更新 → **不写回 history**（每轮自动重建）。
        空台账 → None（原样放行）。追加在末尾＝最新上下文，紧贴模型生成。
        """
        state = getattr(request, "state", None)
        materials = state.get("materials") if isinstance(state, dict) else None
        msg = _build_ledger_message(materials)
        if msg is None:
            return None
        return request.override(messages=[*request.messages, msg])

    @override
    def wrap_model_call(self, request: ModelRequest, handler: Callable[[ModelRequest], ModelResponse]) -> ModelResponse:
        prepared = self._inject_ledger(request)
        return handler(prepared if prepared is not None else request)

    @override
    async def awrap_model_call(self, request: ModelRequest, handler: Callable[[ModelRequest], Awaitable[ModelResponse]]) -> ModelResponse:
        prepared = self._inject_ledger(request)
        return await handler(prepared if prepared is not None else request)

    async def _capture(
        self,
        result: ToolMessage | Command,
        *,
        policy: CapturePolicy,
        url_path: str | None,
        materials: dict[str, Material],
        thread_id: str,
    ) -> Command | None:
        """工具产物 Capture：rehost/register + content 改写。无可捕获产物 → None（原样放行）。

        只作用于 ToolMessage（cfdream MCP 结果）；Command（present_* 自管 artifact）暂不接管。
        url 抽取走 per-tool ``url_path``（JSON 字段路径）。**本类不设 display、不建 artifact items**
        （D14）：交付判定归 MessageStreamMiddleware。rehost 落不了盘（fetch/upload 失败）→
        **fail-open**：置 ``stable=false`` 登记 global_url 续跑（不阻断 run；emit 端按 stable 过滤
        不交付，I5），临期 url 只进 ``ref`` 不进 content/artifact/final_state（I9）。
        """
        if policy == "off" or not isinstance(result, ToolMessage):
            return None
        urls = _extract_urls_by_path(result, url_path)
        if not urls:
            return None

        # 物化收口（§8 R3/R4）：逐 url 走统一 helper（地址反查去重 + rehost/register），
        # ``working`` 滚动更新使同批重复 url / task_wait 重放幂等（不双计费）。
        working = dict(materials)
        updates: dict[str, Material] = {}
        id_for_url: dict[str, str] = {}

        for url in urls:
            kind = _infer_kind(url)
            if policy == "rehost":
                try:
                    outcome = await rehost_remote_url(working, url, thread_id=thread_id, kind=kind, origin="generate")
                except Exception as exc:  # noqa: BLE001 — rehost 失败不得阻断 run（fail-open，I5）
                    logger.warning("MaterialsCapture: rehost failed for %s (%s) — marking unstable", url, exc)
                    mid, upd = register(working, kind=kind, origin="generate", ref_type="global_url", ref=url, origin_url=url, stable=False)
                    outcome = MaterializeOutcome(id=mid, update=upd, ref_type="global_url", ref=url, stable=False, deduped=False)
            else:  # register：仅准入，ref 保持 global_url，不 fetch/不 upload
                outcome = register_remote_url(working, url, kind=kind, origin="generate")
            id_for_url[url] = outcome.id
            if outcome.update:
                updates.update(outcome.update)
                working.update(outcome.update)

        new_msg = _rewrite_result(result, urls, id_for_url)
        update: dict[str, Any] = {"messages": [new_msg]}
        if updates:
            update["materials"] = updates
        return Command(update=update)

    async def _aresolve_outgate(
        self, request: ToolCallRequest, *, thread_id: str, to_physical: Callable[[str], str], materials: dict[str, Material] | None = None
    ) -> tuple[ToolCallRequest | ToolMessage, dict[str, Material]]:
        """cfdream 工具入参出口签发（async，含 local 自动 stage）。

        简化后实现：local 素材在 resolve 内部 stage，而非前置两段式。懒上传仍命中真消费时刻，
        stage 失败仍报错（fail-closed，不调 cfgpu 计费）。返回 (resolved_request_or_error, materials_updates)。
        """
        tool_call = request.tool_call
        name = tool_call.get("name") or ""
        if not name.startswith(_CFDREAM_PREFIX):
            return request, {}

        args = tool_call.get("args")
        if not isinstance(args, (dict, list)):
            return request, {}

        if materials is None:
            state = getattr(request, "state", None)
            materials = state.get("materials") if isinstance(state, dict) else None
        materials = dict(materials or {})

        # 扫描并收集所有 local ids
        local_ids: list[str] = []
        _resolve_value_internal(args, materials, local_ids.append)

        # 批量 stage local 素材
        stage_updates: dict[str, Material] = {}
        working = dict(materials)
        for mid in local_ids:
            try:
                outcome = await stage_to_oss(working, mid, thread_id=thread_id, to_physical=to_physical, display=False)
                if outcome.update:
                    stage_updates.update(outcome.update)
                    working.update(outcome.update)
            except Exception as exc:  # noqa: BLE001 — stage 失败，后续 resolve 会报错
                logger.warning("MaterialsResolve: stage of local material %s failed (%s)", mid, exc)

        # 用更新后的 materials 做 resolve
        try:
            new_args = _resolve_value(args, working)
        except _UnresolvableRef as exc:
            logger.info("MaterialsResolve: rejected unresolvable ref %r in %r (%s)", exc.token, name, exc.reason)
            error_msg = (
                f"无法解析素材引用 {exc.token!r}：{exc.reason}。"
                "请改用素材台账中的 material id（如 m3）重新引用，"
                "不要直接粘贴可能已失效或被截断的 URL。"
            )
            if local_ids and exc.token in local_ids:
                # local 素材 staging 失败
                error_msg = f"本地素材 {exc.token!r} 上传失败，请检查文件是否存在或稍后重试。"
            return (
                ToolMessage(content=error_msg, tool_call_id=tool_call.get("id", ""), name=name, status="error"),
                {},
            )

        result = request
        if new_args != args:
            result = request.override(tool_call={**tool_call, "args": new_args})
            logger.info("MaterialsResolve: signed/resolved material ref(s) in %r args", name)
        return result, stage_updates

    @staticmethod
    def _resolve_outgate(request: ToolCallRequest, materials: dict[str, Material] | None = None) -> ToolCallRequest | ToolMessage:
        """sync 路径的出口签发（local 报错，不支持 stage）。"""
        tool_call = request.tool_call
        name = tool_call.get("name") or ""
        if not name.startswith(_CFDREAM_PREFIX):
            return request

        args = tool_call.get("args")
        if not isinstance(args, (dict, list)):
            return request

        if materials is None:
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
        return resolve_capture_policy(request.tool_call.get("name") or "", metadata, self._capture_patterns)

    def _capture_url_path(self, request: ToolCallRequest) -> str | None:
        tool = getattr(request, "tool", None)
        metadata = getattr(tool, "metadata", None) if tool is not None else None
        return resolve_url_path(request.tool_call.get("name") or "", metadata, self._url_path_patterns)

    @override
    def wrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], ToolMessage | Command],
    ) -> ToolMessage | Command:
        # 同步路径只做出口签发，不做 Capture：Capture 的 rehost 是 fetch+upload（网络），
        # 须 async；cfdream/MCP 媒体工具走 awrap（async）路径，同步路径不承载可 rehost 的产物。
        resolved = self._resolve_outgate(request)
        if isinstance(resolved, ToolMessage):
            return resolved  # 解析失败：短路，不调 cfdream(计费)
        return handler(resolved)

    @override
    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
    ) -> ToolMessage | Command:
        state = getattr(request, "state", None)
        materials = (state.get("materials") if isinstance(state, dict) else None) or {}
        thread_id = _thread_id_from_request(request) or "default"

        # cfdream 工具：使用 aresolve（含 local 自动 stage）
        resolved = request
        stage_updates: dict[str, Material] = {}
        if (request.tool_call.get("name") or "").startswith(_CFDREAM_PREFIX):
            thread_data = (state.get("thread_data") if isinstance(state, dict) else None) or {}
            outputs_path = thread_data.get("outputs_path", "")

            def to_physical(vpath: str) -> str:
                return map_virtual_to_physical(vpath, outputs_path)

            resolved, stage_updates = await self._aresolve_outgate(request, thread_id=thread_id, to_physical=to_physical, materials=materials)
            if isinstance(resolved, ToolMessage):
                return resolved  # 短路：不调 cfdream(计费)

        # 调用 handler
        result = await handler(resolved)

        # capture（所有工具，非 cfdream 工具也走）
        policy = self._capture_policy(request)
        if policy != "off":
            captured = await self._capture(
                result,
                policy=policy,
                url_path=self._capture_url_path(request),
                materials=materials,
                thread_id=thread_id,
            )
            if captured is not None:
                result = captured

        # 折入 stage_updates（local→oss_path 升级）
        if stage_updates:
            if isinstance(result, Command):
                upd = dict(result.update) if isinstance(result.update, dict) else {}
                upd["materials"] = {**(upd.get("materials") or {}), **stage_updates}
                result.update = upd
            else:
                result = Command(update={"messages": [result], "materials": stage_updates})
        return result
