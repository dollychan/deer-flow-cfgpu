"""``analyse_image`` 工具（cfgpu-docs/materials.md §4.7, materials-impl-plan.md P9）。

视觉分析从「上行 image_url 直注多模态 block」改为**显式工具**：图引用变成可管理的工具参数
（material id），消除 content 内 url，并修掉旧 ``_append_content_block`` 只取 ``url[0]`` 的局限。

**按持久化边界切两半**（§4.7）：base64 永不离开 ``wrap_model_call`` 调用帧，故重 fetch 落
``AnalyseImageMiddleware``；本工具只做**触发器 + 归一器**：
- **in-gate 归一**：id∪url 混合入参 → 每个 url 经 ``resolve_or_register`` 归一成 id（去重命中
  既有 / 未命中新建 ``origin=uplink``），之后整条链路只剩 id（raw url 绝不越过 in-gate，I11）。
- **廉价校验**（不下载）：registry 查在不在 / 是不是图 → 未知 id / 非图当场回 error ToolMessage
  （fail-fast，LLM 可纠正）。
- 产出 **id-only 轻量信号 ToolMessage**（"已排队分析 m1, m5"，零 base64 / 零 url，I3/I9），
  ``.artifact`` 带结构化 ``{"analyse_image": {"ids": [...]}}`` 供中间件取 id。**工具不产出分析**——
  分析由主模型在 post-tool 那次调用亲看像素后产出。

**契约（正确性前提）**：base64 单轮可见，analyse 后**必须本轮陈述发现**（先说所见再做别的），
否则下一轮 base64 蒸发、视觉细节丢失。下一轮要再看 → 重调 analyse_image（id→ref 重取，便宜）。
"""

from __future__ import annotations

import logging
from typing import Annotated

from langchain.tools import InjectedToolCallId, tool
from langchain_core.messages import ToolMessage
from langgraph.types import Command

from deerflow.tools.types import Runtime

logger = logging.getLogger(__name__)


def _is_id_token(token: str) -> bool:
    """material id 形态 ``m\\d+``（与 ``new_material_id`` 一致）。"""
    return len(token) >= 2 and token[0] == "m" and token[1:].isdigit()


@tool("analyse_image", parse_docstring=True)
async def analyse_image_tool(
    runtime: Runtime,
    images: list[str],
    tool_call_id: Annotated[str, InjectedToolCallId],
    question: str | None = None,
    focus: str | None = None,
) -> Command:
    """Look at one or more images so you can analyse them in your very next reply.

    Pass a mix of material ids (e.g. `m3`) and/or raw image URLs the user wrote in their
    message. Each image becomes visible to you **for this one turn only** — you MUST state
    what you see in your immediate next reply (compare variants, pick the best, check
    consistency, spot artifacts). If you need to look again later, call this tool again.

    Use this whenever you need to actually see pixels: judging a generated image, comparing
    options, or checking a reference the user linked.

    Args:
        images: List of material ids (like `m3`) and/or image URLs to look at. Prefer ids;
            only pass a raw URL when the user typed an image link directly in their message.
        question: Optional question you want answered about the image(s).
        focus: Optional aspect to focus on (e.g. "hands", "background consistency").
    """
    # Lazy import: analyse_image_tool is imported by deerflow.tools.builtins at package
    # init; a module-top materials import would re-enter deerflow.agents.__init__ → factory →
    # the half-built builtins package (circular). Importing inside the call breaks the cycle.
    from deerflow.agents.materials.registry import resolve_or_register

    if runtime.state is None:
        return Command(update={"messages": [ToolMessage("Error: runtime state unavailable", tool_call_id=tool_call_id, status="error")]})

    if not images:
        return Command(update={"messages": [ToolMessage("Error: no images provided to analyse", tool_call_id=tool_call_id, status="error")]})

    working: dict = dict(runtime.state.get("materials") or {})
    updates: dict = {}
    ids: list[str] = []
    errors: list[str] = []

    for raw in images:
        token = (raw or "").strip()
        if not token:
            continue
        if _is_id_token(token):
            # id 形态：必须已在台账，否则报错（绝不把悬空 id 当 object_key 误注册）
            if token not in working:
                errors.append(f"{token}：未知 material id")
                continue
            mid = token
        else:
            # url / object_key：in-gate 归一（去重命中既有 / 未命中新建 origin=uplink），不下载
            mid, upd = resolve_or_register(working, token, kind="image", origin="uplink")
            if upd:
                working.update(upd)
                updates.update(upd)
        mat = working.get(mid)
        if mat is None:
            errors.append(f"{mid}：未知 material id")
            continue
        if mat.get("kind") != "image":
            errors.append(f"{mid}：不是图像（kind={mat.get('kind')}）")
            continue
        if mid not in ids:
            ids.append(mid)

    if errors:
        # fail-fast：任一无法解析就整体报错，引导 LLM 纠正；不提交部分登记（幂等，下次重来）。
        detail = "；".join(errors)
        return Command(update={"messages": [ToolMessage(f"Error: 无法分析以下图像引用：{detail}。请改用素材台账中的 image material id 重试。", tool_call_id=tool_call_id, status="error")]})

    if not ids:
        return Command(update={"messages": [ToolMessage("Error: no valid image materials to analyse", tool_call_id=tool_call_id, status="error")]})

    signal: dict = {"ids": ids}
    if question:
        signal["question"] = question
    if focus:
        signal["focus"] = focus

    content = f"已排队分析图像 {', '.join(ids)}。请在本轮即查看图像并陈述你的发现（图像仅本轮可见）。"
    update: dict = {
        "messages": [
            ToolMessage(
                content,
                tool_call_id=tool_call_id,
                status="success",
                artifact={"analyse_image": signal},
            )
        ]
    }
    if updates:
        update["materials"] = updates
    return Command(update=update)


# Client-facing visibility: analyse_image is an INPUT step — it only signals the middleware
# to inject pixels for the model (vision) and returns a tiny id-only ToolMessage with no
# deliverable artifact. Mark internal so MessageStreamMiddleware emits no tool_result event
# (mirrors view_image; output-side delivery stays with present_files / materials Capture).
analyse_image_tool.metadata = {"visibility": "internal"}
