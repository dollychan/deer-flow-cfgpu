"""``AnalyseImageMiddleware``（cfgpu-docs/materials.md §4.7, materials-impl-plan.md P9）。

``analyse_image`` 工具的**重负载注入器**——与 ``analyse_image_tool`` 按持久化边界切两半：
工具做触发器+归一器（零 base64），本中间件在 ``wrap_model_call`` 把像素 base64 **单轮注入**
即将发出的这次主模型请求，**不写回 ``messages``**（持久化 history 只留 id 引用，I9/I10）。

独立于 ``MaterialsMiddleware``（§5「P9 不并入」）：工具触发（非常驻）/ vision feature gate /
重像素载荷（与台账「轻」相反）/ 是 ``ViewImageMiddleware`` 的 1:1 ephemeral 替身。与 materials
子系统唯一共享 ``resolve_or_register``，且那一步在**工具内**完成；本中间件只读 ``state.materials``。

注入逻辑（§4.7）：
- 扫**待回应尾部**——末条 AIMessage 的 ``analyse_image`` tool_calls + 其后对应 ToolMessage
  （``.artifact["analyse_image"]["ids"]``）。取 id。
- **出口三分 fetch→base64**：``local_path``→读盘（无网络，优先）/ ``oss_path``→现签 presigned→
  fetch / 外部 ``global_url``/``asset_url``→直 fetch（**fetch 唯一触发，不 rehost**，state 不变）。
  fetch 失败→注入文本占位「mN 不可用」，模型如实说看不到（接受第三方死链风险）。
- 注入一条 hidden HumanMessage（标注归属「以下为 m1 图像」）进 ``request``，**仅本次请求**。

**单轮 ephemeral 由结构保证**：注入仅本次请求；下一次模型调用时该 ToolMessage 已落在新
AIMessage 之前、不在尾部 → 不再命中注入，无需额外 state 通道。
"""

from __future__ import annotations

import asyncio
import base64
import logging
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, override

import httpx
from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import ModelRequest, ModelResponse
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from deerflow.agents.materials.materialize import fetch_bytes
from deerflow.agents.materials.types import Material
from deerflow.config.paths import VIRTUAL_PATH_PREFIX
from deerflow.oss.client import get_oss_client

logger = logging.getLogger(__name__)

_ANALYSE_TOOL_NAME = "analyse_image"
# 与工具入参/出口三分同口径的取字节超时（卡死的 CDN 不得吊住一次主模型调用）。
_FETCH_TIMEOUT_S = 60.0


def _detect_image_mime(data: bytes) -> str:
    """从魔数判 mime（注入 ``data:`` URI 用）；未知回 ``image/png`` 兜底。"""
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if len(data) >= 12 and data.startswith(b"RIFF") and data[8:12] == b"WEBP":
        return "image/webp"
    if data.startswith(b"GIF87a") or data.startswith(b"GIF89a"):
        return "image/gif"
    return "image/png"


def _presign(object_key: str) -> str:
    """我方 object_key → presigned GET url（本地 HMAC）。OSS 未启用→回裸 key（fetch 大概率失败→占位）。"""
    oss = get_oss_client()
    return object_key if oss is None else oss.presign(object_key)


class AnalyseImageMiddleware(AgentMiddleware):
    """``analyse_image`` 的单轮 base64 注入器（vision-gate；ViewImageMiddleware 的 ephemeral 替身）。"""

    # ── 待回应尾部扫描 ──────────────────────────────────────────────────────────

    def _last_ai_index(self, messages: list[Any]) -> int:
        for idx in range(len(messages) - 1, -1, -1):
            if isinstance(messages[idx], AIMessage):
                return idx
        return -1

    def _collect_pending_ids(self, messages: list[Any]) -> list[str]:
        """末条 AIMessage 的 analyse_image tool_calls + 其后对应 ToolMessage → 去重 id 列表。

        只认**尾部**（末条 AIMessage 之后）的 ToolMessage：下一轮新 AIMessage 产生后，该
        ToolMessage 不再在尾部 → 返回空 → 不再注入（单轮 ephemeral 的结构保证）。
        """
        if not messages:
            return []
        ai_idx = self._last_ai_index(messages)
        if ai_idx < 0:
            return []
        ai = messages[ai_idx]
        tool_calls = getattr(ai, "tool_calls", None) or []
        analyse_ids = {tc.get("id") for tc in tool_calls if tc.get("name") == _ANALYSE_TOOL_NAME and tc.get("id")}
        if not analyse_ids:
            return []
        ids: list[str] = []
        for msg in messages[ai_idx + 1 :]:
            if not isinstance(msg, ToolMessage) or msg.tool_call_id not in analyse_ids:
                continue
            artifact = getattr(msg, "artifact", None)
            signal = artifact.get(_ANALYSE_TOOL_NAME) if isinstance(artifact, dict) else None
            if not isinstance(signal, dict):
                continue
            for mid in signal.get("ids", []):
                if isinstance(mid, str) and mid not in ids:
                    ids.append(mid)
        return ids

    # ── 出口三分（按 ref_type）──────────────────────────────────────────────────

    def _virtual_to_physical(self, virtual_path: str, thread_data: dict | None) -> str | None:
        """``/mnt/user-data/<rest>`` → host 物理路径（thread_data.outputs_path 的 user-data 父目录）。"""
        outputs = thread_data.get("outputs_path") if thread_data else None
        if not outputs:
            return None
        user_data_dir = Path(outputs).resolve().parent
        prefix = VIRTUAL_PATH_PREFIX.lstrip("/")
        stripped = virtual_path.lstrip("/")
        if stripped == prefix or stripped.startswith(prefix + "/"):
            relative = stripped[len(prefix) :].lstrip("/")
            return str((user_data_dir / relative).resolve())
        return str(Path(virtual_path).expanduser().resolve())

    def _plan(self, mat: Material, thread_data: dict | None) -> tuple[str, str] | None:
        """决定出口形态：``("local", physical)`` / ``("fetch", url)`` / None（无可取来源）。"""
        local = mat.get("local_path")
        if local:
            physical = self._virtual_to_physical(local, thread_data)
            if physical and Path(physical).is_file():
                return ("local", physical)
        ref_type = mat.get("ref_type")
        ref = mat.get("ref") or ""
        if ref_type == "oss_path":
            return ("fetch", _presign(ref))
        if ref_type in ("global_url", "asset_url") and ref:
            return ("fetch", ref)
        return None

    def _read_local(self, physical: str) -> tuple[bytes, str] | None:
        try:
            data = Path(physical).read_bytes()
        except OSError as exc:
            logger.warning("AnalyseImage: local read failed for %s (%s)", physical, exc)
            return None
        return (data, _detect_image_mime(data)) if data else None

    async def _aload(self, mat: Material, thread_data: dict | None) -> tuple[bytes, str] | None:
        plan = self._plan(mat, thread_data)
        if plan is None:
            return None
        kind, target = plan
        if kind == "local":
            return await asyncio.to_thread(self._read_local, target)
        try:
            fetched = await fetch_bytes(target)
        except Exception as exc:  # noqa: BLE001 — fetch 失败不得断流，降级文本占位
            logger.warning("AnalyseImage: fetch failed for material ref (%s)", exc)
            return None
        return (fetched.data, _detect_image_mime(fetched.data)) if fetched.data else None

    def _load_sync(self, mat: Material, thread_data: dict | None) -> tuple[bytes, str] | None:
        plan = self._plan(mat, thread_data)
        if plan is None:
            return None
        kind, target = plan
        if kind == "local":
            return self._read_local(target)
        try:
            with httpx.Client(follow_redirects=True, timeout=_FETCH_TIMEOUT_S) as client:
                resp = client.get(target)
                resp.raise_for_status()
                data = resp.content
        except Exception as exc:  # noqa: BLE001
            logger.warning("AnalyseImage: sync fetch failed for material ref (%s)", exc)
            return None
        return (data, _detect_image_mime(data)) if data else None

    # ── 注入构造 ────────────────────────────────────────────────────────────────

    def _build_blocks(self, loaded: list[tuple[str, tuple[bytes, str] | None]]) -> list[dict]:
        blocks: list[dict] = [{"type": "text", "text": "以下为你请求分析的图像（仅本轮可见，请立即陈述发现）："}]
        for mid, result in loaded:
            if result is None:
                blocks.append({"type": "text", "text": f"\n- [{mid}] 不可用（无法获取图像，请如实说明看不到）"})
                continue
            data, mime = result
            b64 = base64.b64encode(data).decode("utf-8")
            blocks.append({"type": "text", "text": f"\n- 以下为 {mid} 图像："})
            blocks.append({"type": "image_url", "image_url": {"url": f"data:{mime};base64,{b64}"}})
        return blocks

    def _inject(self, request: ModelRequest, blocks: list[dict]) -> ModelRequest:
        # hidden current-turn context（hide_from_ui）：仅本次请求，不写回 history。
        human = HumanMessage(content=blocks, additional_kwargs={"hide_from_ui": True, "materials_analyse": True})
        return request.override(messages=[*request.messages, human])

    def _state_of(self, request: ModelRequest) -> dict:
        state = getattr(request, "state", None)
        return state if isinstance(state, dict) else {}

    # ── hooks ──────────────────────────────────────────────────────────────────

    @override
    def wrap_model_call(self, request: ModelRequest, handler: Callable[[ModelRequest], ModelResponse]) -> ModelResponse:
        ids = self._collect_pending_ids(list(request.messages))
        if not ids:
            return handler(request)
        state = self._state_of(request)
        materials = state.get("materials") or {}
        thread_data = state.get("thread_data")
        loaded = [(mid, self._load_sync(materials[mid], thread_data) if mid in materials else None) for mid in ids]
        prepared = self._inject(request, self._build_blocks(loaded))
        return handler(prepared)

    @override
    async def awrap_model_call(self, request: ModelRequest, handler: Callable[[ModelRequest], Awaitable[ModelResponse]]) -> ModelResponse:
        ids = self._collect_pending_ids(list(request.messages))
        if not ids:
            return await handler(request)
        state = self._state_of(request)
        materials = state.get("materials") or {}
        thread_data = state.get("thread_data")
        loaded = [(mid, await self._aload(materials[mid], thread_data) if mid in materials else None) for mid in ids]
        prepared = self._inject(request, self._build_blocks(loaded))
        return await handler(prepared)
