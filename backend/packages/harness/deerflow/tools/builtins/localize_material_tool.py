"""``localize_material`` 工具（cfgpu-docs/materials.md §4.8.3, P10 — 新增）。

``localize`` 原语的工具面：``id → 本地路径``【确保本地可达】。给 bash/ffmpeg 取一份**本地副本**
来处理（如把生成的视频片段拉进沙盒做 ffmpeg 拼接）——补 ``MaterialResolve``（只签 cfdream 槽、
给远程）覆盖不到的"本地消费"出口，与 resolve（id→远程 presigned）形成反向对。

返回的是 ``/mnt/user-data/workspace/...`` **虚拟路径（非 url）**，I9 安全可暴露给 LLM。幂等：素材
已有本地副本且文件在 → 直接返回，不重复下载（``materialize.stage`` 目标已在则跳过 fetch）。
``asset_url`` 素材拒绝（I4，不可下载）；纯 ``local`` 但文件丢失 → 悬空报错。
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated
from urllib.parse import urlsplit

from langchain.tools import InjectedToolCallId, tool
from langchain_core.messages import ToolMessage
from langgraph.types import Command

from deerflow.agents.materials.materialize import localize
from deerflow.config.paths import VIRTUAL_PATH_PREFIX, map_virtual_to_physical
from deerflow.oss.client import get_oss_client
from deerflow.tools.types import Runtime

logger = logging.getLogger(__name__)

_LOCALIZE_DIR = f"{VIRTUAL_PATH_PREFIX}/workspace"
_KIND_EXT = {"image": ".png", "video": ".mp4", "audio": ".mp3", "document": ".bin", "asset": ".bin"}


def _presign(object_key: str) -> str:
    """我方 object_key → presigned GET url；OSS 未启用回裸 key 兜底（同中间件 ``_presign``）。"""
    oss = get_oss_client()
    return oss.presign(object_key) if oss is not None else object_key


def _dest_virtual(mid: str, mat: dict) -> str:
    """为下载副本挑一个确定性虚拟落点 ``/mnt/user-data/workspace/<mid>-<name>``（避免重名碰撞）。"""
    ref = mat.get("ref") or ""
    path = urlsplit(ref).path if "://" in ref else ref
    name = path.rsplit("/", 1)[-1].strip("/")
    if not name:
        name = mid + _KIND_EXT.get(str(mat.get("kind", "")), ".bin")
    return f"{_LOCALIZE_DIR}/{mid}-{name}"


@tool("localize_material", parse_docstring=True)
async def localize_material_tool(
    runtime: Runtime,
    material_id: str,
    tool_call_id: Annotated[str, InjectedToolCallId],
) -> Command:
    """Download a material into the sandbox and return a local path you can read/process.

    Use this when you need a LOCAL copy of a material to operate on with bash/ffmpeg/etc. —
    for example pulling a generated video clip into the sandbox to stitch with ffmpeg. Pass the
    material id (e.g. `m7`); you get back a `/mnt/user-data/workspace/...` path. This is the
    inverse of how cfdream tools consume materials (which resolve ids to remote URLs for you).

    When NOT to use:
    - To feed a material to a cfdream image/video tool — just pass its id directly; resolution
      to a URL is automatic.
    - To show a deliverable to the user — use `present_files`.

    Args:
        material_id: The material id to localize (e.g. `m7`).
    """
    if runtime.state is None:
        return Command(update={"messages": [ToolMessage("Error: runtime state unavailable", tool_call_id=tool_call_id, status="error")]})

    thread_data = runtime.state.get("thread_data") or {}
    outputs_path = thread_data.get("outputs_path")
    if not outputs_path:
        return Command(update={"messages": [ToolMessage("Error: thread outputs path unavailable", tool_call_id=tool_call_id, status="error")]})

    materials = runtime.state.get("materials") or {}
    mat = materials.get(material_id)
    if mat is None:
        return Command(update={"messages": [ToolMessage(f"Error: no material with id {material_id!r}", tool_call_id=tool_call_id, status="error")]})

    def to_physical(vpath: str) -> str:
        return map_virtual_to_physical(vpath, outputs_path)

    try:
        local_path, update = await localize(
            materials, material_id, to_physical=to_physical, dest_virtual=_dest_virtual(material_id, mat), presign=_presign
        )
    except Exception as exc:  # noqa: BLE001 — 下载/拒绝失败回 error，不阻断 run
        logger.warning("localize_material: failed for %s (%s)", material_id, exc)
        return Command(update={"messages": [ToolMessage(f"Error: failed to localize {material_id}: {exc}", tool_call_id=tool_call_id, status="error")]})

    out: dict = {
        "messages": [
            ToolMessage(
                f"Localized material {material_id} to {local_path}. You can now read/process this local file.",
                tool_call_id=tool_call_id,
                status="success",
            )
        ]
    }
    if update:
        out["materials"] = update
    return Command(update=out)


# 返回本地路径（非 url），结果走默认 internal 可见性——localize 是给 agent 自己取本地副本，非交付出口。
