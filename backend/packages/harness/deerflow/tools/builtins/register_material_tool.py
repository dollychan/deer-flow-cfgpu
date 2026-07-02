"""``register_material`` 工具（cfgpu-docs/materials.md §4.8.3, P10 — 取代 stage_material 命名）。

把 agent 在沙盒里造出的本地文件（``/mnt/user-data/*``）**廉价登记**进素材注册表：``register``
原语 local 分支——只记 ``local_path``、**零网络不上传 OSS**，回 **id 形态** ToolMessage（零 url，I3）。
该 ``local`` 素材 ``stable=false``（未落盘），喂 cfdream / 展示前由 awrap 自动 stage（§4.8.4）或
``present_files`` 升级成 oss_path。此后可用 material id（如 m7）在 cfdream 工具入参里引用。

bash 产物无自动准入（§11 缺口⑤，按设计）——本工具是 agent 把本地产物显式入册的唯一入口。
重复 register 同一文件经 ``find_by_local_path`` 去重命中既有 id（不重复登记）。**懒上传**：上传只在
真被 cfdream 消费（awrap 自动 stage）或 present 时发生，省 ffmpeg 临时帧的无谓上传。
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated

from langchain.tools import InjectedToolCallId, tool
from langchain_core.messages import ToolMessage
from langgraph.types import Command

from deerflow.agents.materials.materialize import register_local_file
from deerflow.config.paths import map_virtual_to_physical
from deerflow.tools.types import Runtime

logger = logging.getLogger(__name__)

# kind 推断（与 materialize/registry 同口径）：扩展名 → Material.kind。
_KIND_BY_EXT: dict[str, str] = {
    ".png": "image", ".jpg": "image", ".jpeg": "image", ".webp": "image", ".gif": "image",
    ".mp4": "video", ".mov": "video", ".webm": "video", ".mkv": "video",
    ".mp3": "audio", ".wav": "audio", ".m4a": "audio", ".flac": "audio",
}


def _infer_kind(filename: str) -> str:
    ext = Path(filename).suffix.lower()
    return _KIND_BY_EXT.get(ext, "document")


def _virtual_to_physical(virtual_path: str, outputs_path: str) -> str:
    """``/mnt/user-data/<rest>`` → host 物理路径（thread outputs 的 user-data 父目录）。"""
    return map_virtual_to_physical(virtual_path, outputs_path)


@tool("register_material", parse_docstring=True)
async def register_material_tool(
    runtime: Runtime,
    filepath: str,
    tool_call_id: Annotated[str, InjectedToolCallId],
    caption: str | None = None,
) -> Command:
    """Register a local file you created as a reusable material, returning a stable material id.

    Use this when you have produced a file under `/mnt/user-data/` (e.g. via bash, ffmpeg, or a
    download) and want to reference it later as an input to image/video tools. Registration is
    cheap and local — the file is NOT uploaded yet; it is uploaded automatically only when you
    actually pass its id to a cfdream tool, or when you `present_files` it. You get a short
    material id (e.g. `m7`); pass that id wherever a material is expected — never paste raw paths
    or URLs.

    When NOT to use:
    - To show a finished deliverable to the user — use `present_files` instead (it also uploads).
    - For files already registered as materials (they already have an id).

    Args:
        filepath: Absolute path to a local file under `/mnt/user-data/` to register.
        caption: Optional short description to help you recall the material later.
    """
    if runtime.state is None:
        return Command(update={"messages": [ToolMessage("Error: runtime state unavailable", tool_call_id=tool_call_id, status="error")]})

    thread_data = runtime.state.get("thread_data") or {}
    outputs_path = thread_data.get("outputs_path")
    if not outputs_path:
        return Command(update={"messages": [ToolMessage("Error: thread outputs path unavailable", tool_call_id=tool_call_id, status="error")]})

    physical = _virtual_to_physical(filepath, outputs_path)
    if not Path(physical).is_file():
        return Command(update={"messages": [ToolMessage(f"Error: file not found: {filepath}", tool_call_id=tool_call_id, status="error")]})

    materials = runtime.state.get("materials") or {}
    kind = _infer_kind(physical)
    outcome = register_local_file(materials, filepath, kind=kind, origin="local", caption=caption, display=False)

    update: dict = {
        "messages": [
            ToolMessage(
                f"Registered material {outcome.id} ({kind}, local). Reference it by id {outcome.id} in later tool calls; "
                "it will be uploaded automatically when first used.",
                tool_call_id=tool_call_id,
                status="success",
            )
        ]
    }
    if outcome.update:
        update["materials"] = outcome.update
    return Command(update=update)


# Capture 自身已在 awrap 内处理产物下行；register_material 是懒登记器，结果走默认 internal 可见性。
# 本地素材 stable=false → 不进 artifact 投影（§4.8.1），故不带 artifact items。
