"""``stage_material`` 工具（cfgpu-docs/materials.md §4.5/§8, P6）。

把 agent 在沙盒里造出的本地文件（``/mnt/user-data/*``）登记进素材注册表：rehost 到我方 OSS →
``object_key``，登记 ``origin=local`` 的 oss_path material，回 **id 形态** ToolMessage（零 url，I3）。
此后该文件可用 material id（如 m7）在 cfdream 工具入参里引用，出口由 MaterialResolve 现签。

bash 产物无自动准入（§11 缺口⑤，按设计）——本工具是 agent 把本地产物显式入册的唯一入口；
present_files 只投影交付物、不进注册表，二者职责不同。重复 stage 同一文件经 ``rehost_local_file``
的 R2 查重跳过重复 upload（不双计费）。
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated

from langchain.tools import InjectedToolCallId, tool
from langchain_core.messages import ToolMessage
from langgraph.types import Command

from deerflow.agents.materials.materialize import rehost_local_file
from deerflow.config.paths import VIRTUAL_PATH_PREFIX
from deerflow.tools.types import Runtime

logger = logging.getLogger(__name__)

# kind 推断（与 materialize/registry 同口径）：扩展名 → Material.kind。
_KIND_BY_EXT: dict[str, str] = {
    ".png": "image", ".jpg": "image", ".jpeg": "image", ".webp": "image", ".gif": "image",
    ".mp4": "video", ".mov": "video", ".webm": "video", ".mkv": "video",
    ".mp3": "audio", ".wav": "audio", ".m4a": "audio", ".flac": "audio",
}


def _get_thread_id(runtime: Runtime) -> str | None:
    thread_id = runtime.context.get("thread_id") if runtime.context else None
    if thread_id:
        return thread_id
    runtime_config = getattr(runtime, "config", None) or {}
    return runtime_config.get("configurable", {}).get("thread_id")


def _virtual_to_physical(virtual_path: str, outputs_path: str) -> str:
    """``/mnt/user-data/<rest>`` → host 物理路径（thread_data.outputs_path 的 user-data 父目录）。

    与 present_file_tool 同源映射（ThreadDataMiddleware 是路径唯一真源），但不限于 outputs 子树——
    workspace/uploads/outputs 下的本地产物都可入册（§4.5：本地文件 → 注册表）。
    """
    user_data_dir = Path(outputs_path).resolve().parent
    virtual_prefix = VIRTUAL_PATH_PREFIX.lstrip("/")
    stripped = virtual_path.lstrip("/")
    if stripped == virtual_prefix or stripped.startswith(virtual_prefix + "/"):
        relative = stripped[len(virtual_prefix):].lstrip("/")
        return str((user_data_dir / relative).resolve())
    return str(Path(virtual_path).expanduser().resolve())


def _infer_kind(filename: str) -> str:
    ext = Path(filename).suffix.lower()
    return _KIND_BY_EXT.get(ext, "document")


@tool("stage_material", parse_docstring=True)
async def stage_material_tool(
    runtime: Runtime,
    filepath: str,
    tool_call_id: Annotated[str, InjectedToolCallId],
    caption: str | None = None,
) -> Command:
    """Register a local file you created as a reusable material, returning a stable material id.

    Use this when you have produced a file under `/mnt/user-data/` (e.g. via bash or a
    download) and want to reference it later as an input to image/video tools. The file is
    uploaded to durable storage and assigned a short material id (e.g. `m7`); pass that id
    wherever a material is expected — never paste raw URLs or object keys.

    When NOT to use:
    - To show a finished deliverable to the user — use `present_files` instead.
    - For files already registered as materials (they already have an id).

    Args:
        filepath: Absolute path to a local file under `/mnt/user-data/` to register.
        caption: Optional short description to help you recall the material later.
    """
    if runtime.state is None:
        return Command(update={"messages": [ToolMessage("Error: runtime state unavailable", tool_call_id=tool_call_id, status="error")]})

    thread_id = _get_thread_id(runtime)
    if not thread_id:
        return Command(update={"messages": [ToolMessage("Error: thread id unavailable", tool_call_id=tool_call_id, status="error")]})

    thread_data = runtime.state.get("thread_data") or {}
    outputs_path = thread_data.get("outputs_path")
    if not outputs_path:
        return Command(update={"messages": [ToolMessage("Error: thread outputs path unavailable", tool_call_id=tool_call_id, status="error")]})

    physical = _virtual_to_physical(filepath, outputs_path)
    if not Path(physical).is_file():
        return Command(update={"messages": [ToolMessage(f"Error: file not found: {filepath}", tool_call_id=tool_call_id, status="error")]})

    materials = runtime.state.get("materials") or {}
    kind = _infer_kind(physical)
    try:
        outcome = await rehost_local_file(
            materials, filepath, physical, thread_id=str(thread_id), kind=kind, origin="local", caption=caption, display=False
        )
    except Exception as exc:  # noqa: BLE001 — upload 失败回 error，不阻断 run
        logger.warning("stage_material: rehost failed for %s (%s)", filepath, exc)
        return Command(update={"messages": [ToolMessage(f"Error: failed to stage material: {exc}", tool_call_id=tool_call_id, status="error")]})

    update: dict = {
        "messages": [
            ToolMessage(
                f"Registered material {outcome.id} ({kind}). Reference it by id {outcome.id} in later tool calls.",
                tool_call_id=tool_call_id,
                status="success",
                artifact={"items": [{"id": outcome.id, "ref": outcome.ref, "kind": kind, "stable": outcome.stable}]},
            )
        ]
    }
    if outcome.update:
        update["materials"] = outcome.update
    return Command(update=update)


# Capture 自身已在 awrap 内处理产物下行；stage_material 是注册器，结果走默认 internal 可见性。
