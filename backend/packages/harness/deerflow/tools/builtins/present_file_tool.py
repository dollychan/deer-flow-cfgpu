from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated

from langchain.tools import InjectedToolCallId, tool
from langchain_core.messages import ToolMessage
from langgraph.config import get_config
from langgraph.types import Command

from deerflow.agents.materials.materialize import stage_to_oss
from deerflow.agents.materials.types import Material
from deerflow.config.paths import VIRTUAL_PATH_PREFIX, map_virtual_to_physical
from deerflow.oss.client import get_oss_client
from deerflow.tools.types import Runtime

logger = logging.getLogger(__name__)

OUTPUTS_VIRTUAL_PREFIX = f"{VIRTUAL_PATH_PREFIX}/outputs"


def _presign(object_key: str) -> str:
    """我方 object_key → presigned GET url；OSS 未启用回裸 key 兜底。"""
    oss = get_oss_client()
    return oss.presign(object_key) if oss is not None else object_key


def _get_thread_id(runtime: Runtime) -> str | None:
    """Resolve the current thread id from runtime context or RunnableConfig."""
    thread_id = runtime.context.get("thread_id") if runtime.context else None
    if thread_id:
        return thread_id

    runtime_config = getattr(runtime, "config", None) or {}
    thread_id = runtime_config.get("configurable", {}).get("thread_id")
    if thread_id:
        return thread_id

    try:
        return get_config().get("configurable", {}).get("thread_id")
    except RuntimeError:
        return None


def _normalize_presented_filepath(
    runtime: Runtime,
    filepath: str,
) -> str:
    """Normalize a presented file path to the `/mnt/user-data/outputs/*` contract.

    Accepts either:
    - A virtual sandbox path such as `/mnt/user-data/outputs/report.md`
    - A host-side thread outputs path such as
      `/app/backend/.deer-flow/threads/<thread>/user-data/outputs/report.md`

    Returns:
        The normalized virtual path.

    Raises:
        ValueError: If runtime metadata is missing or the path is outside the
            current thread's outputs directory.
    """
    if runtime.state is None:
        raise ValueError("Thread runtime state is not available")

    thread_id = _get_thread_id(runtime)
    if not thread_id:
        raise ValueError("Thread ID is not available in runtime context or runtime config")

    thread_data = runtime.state.get("thread_data") or {}
    outputs_path = thread_data.get("outputs_path")
    if not outputs_path:
        raise ValueError("Thread outputs path is not available in runtime state")

    outputs_dir = Path(outputs_path).resolve()
    stripped = filepath.lstrip("/")
    virtual_prefix = VIRTUAL_PATH_PREFIX.lstrip("/")

    if stripped == virtual_prefix or stripped.startswith(virtual_prefix + "/"):
        # Decoupled resolution (cfgpu-docs/thread-tenancy.md §4.3 / I3+): map the virtual
        # `/mnt/user-data/<rest>` path straight onto the host user-data dir derived from
        # the thread_data outputs_path (outputs_dir.parent == .../user-data) — never
        # re-resolving a user bucket. ThreadDataMiddleware is the single source of truth
        # for which bucket this thread uses, so present_files inherits it for free and is
        # immune to the tenant model (this also retires the BUG-008 user_id re-resolution
        # fragility). The relative_to(outputs_dir) check below still confines presents to
        # the outputs subtree, so workspace/uploads virtual paths are rejected as before.
        user_data_dir = outputs_dir.parent
        relative = stripped[len(virtual_prefix):].lstrip("/")
        actual_path = (user_data_dir / relative).resolve()
    else:
        actual_path = Path(filepath).expanduser().resolve()

    try:
        relative_path = actual_path.relative_to(outputs_dir)
    except ValueError as exc:
        raise ValueError(f"Only files in {OUTPUTS_VIRTUAL_PREFIX} can be presented: {filepath}") from exc

    return f"{OUTPUTS_VIRTUAL_PREFIX}/{relative_path.as_posix()}"


def _virtual_to_physical(virtual_path: str, outputs_path: str) -> str:
    """Derive the physical filesystem path from a normalized virtual outputs path."""
    relative = virtual_path[len(OUTPUTS_VIRTUAL_PREFIX):].lstrip("/")
    return str(Path(outputs_path).resolve() / relative)


def _artifact_item(ref: str) -> dict:
    """Build an artifact item, classifying ref as a fetchable URL or virtual path.

    `kind="url"` (OSS presigned link) is fetched directly by the client;
    `kind="path"` (virtual outputs path) is fetched via the artifacts API route.
    """
    kind = "url" if ref.startswith(("http://", "https://")) else "path"
    return {"ref": ref, "kind": kind, "expires_at": None}


async def _present_materials(
    runtime: Runtime,
    ids: list[str],
    materials: dict[str, Material],
) -> tuple[list[str], dict[str, Material]]:
    """Stage each material id to durable OSS + mark it as a deliverable (display=true).

    present = ``stage`` 原语 + ``display=true``（§4.8.3/D16）：确保 oss_path（durable）再标交付物投影。
    任意 material id 皆可展示（generate 产物 / 第三方 / 本地）。返回 (presigned refs, materials update)。
    """
    thread_id = _get_thread_id(runtime) or "unknown"
    thread_data = (runtime.state or {}).get("thread_data") or {}
    outputs_path = thread_data.get("outputs_path", "")

    def to_physical(vpath: str) -> str:
        return map_virtual_to_physical(vpath, outputs_path)

    working = dict(materials)
    update: dict[str, Material] = {}
    refs: list[str] = []
    for mid in ids:
        outcome = await stage_to_oss(working, mid, thread_id=str(thread_id), to_physical=to_physical, display=True)
        # 合并 stage 升级 + 确保 display=true（oss_path 已持久时 outcome.update 为空，须显式置）。
        ent: dict = dict(outcome.update.get(mid) or {"id": mid})
        ent["display"] = True
        update[mid] = ent  # type: ignore[assignment]
        working[mid] = {**working.get(mid, {}), **ent}  # type: ignore[typeddict-item]
        refs.append(_presign(outcome.ref or working[mid].get("ref", "")))
    return refs, update


@tool("present_files", parse_docstring=True)
async def present_file_tool(
    runtime: Runtime,
    filepaths: list[str],
    tool_call_id: Annotated[str, InjectedToolCallId],
) -> Command:
    """Make files or materials visible to the user for viewing and rendering in the client.

    When to use the present_files tool:

    - Making any file or material available for the user to view, download, or interact with
    - Presenting multiple related deliverables at once
    - After creating files that should be presented to the user

    When NOT to use the present_files tool:
    - When you only need to read file contents for your own processing
    - For temporary or intermediate files not meant for user viewing

    Notes:
    - You may pass either a local file path under `/mnt/user-data/outputs`, OR a material id
      (e.g. `m7`) for an already-registered material (a generated image/video, or anything you
      registered with `register_material`). Material ids are uploaded to durable storage and
      marked as deliverables automatically.
    - This tool can be safely called in parallel with other tools. State updates are handled by a reducer to prevent conflicts.

    Args:
        filepaths: List of absolute file paths in `/mnt/user-data/outputs` and/or material ids to present to the user.
    """
    state = runtime.state or {}
    materials = state.get("materials") or {}
    id_entries = [e for e in filepaths if isinstance(e, str) and e in materials]
    path_entries = [e for e in filepaths if e not in materials]

    artifacts: list[str] = []
    items: list[dict] = []
    materials_update: dict[str, Material] = {}

    # --- material ids: stage to durable OSS + display=true (§4.8.3) ---
    if id_entries:
        try:
            refs, materials_update = await _present_materials(runtime, id_entries, materials)
        except Exception as exc:  # noqa: BLE001 — stage 失败回 error，不阻断 run
            logger.warning("present_files: failed to stage material(s) %s (%s)", id_entries, exc)
            return Command(update={"messages": [ToolMessage(f"Error: failed to present material(s): {exc}", tool_call_id=tool_call_id, status="error")]})
        artifacts.extend(refs)
        items.extend(_artifact_item(r) for r in refs)

    # --- local paths: existing behaviour (artifacts channel) ---
    if path_entries:
        try:
            normalized_paths = [_normalize_presented_filepath(runtime, fp) for fp in path_entries]
        except ValueError as exc:
            return Command(update={"messages": [ToolMessage(f"Error: {exc}", tool_call_id=tool_call_id)]})

        from deerflow.oss.uploader import get_oss_uploader

        uploader = get_oss_uploader()
        if uploader is None:
            # OSS not configured: original behaviour — store virtual paths directly.
            artifacts.extend(normalized_paths)
            items.extend(_artifact_item(p) for p in normalized_paths)
        else:
            thread_id = _get_thread_id(runtime) or "unknown"
            thread_data = state.get("thread_data") or {}
            outputs_path = thread_data.get("outputs_path", "")
            for vpath in normalized_paths:
                try:
                    physical = _virtual_to_physical(vpath, outputs_path)
                    url = await uploader.upload_local_file(vpath, physical, thread_id)
                    artifacts.append(url)
                    items.append(_artifact_item(url))
                except Exception:
                    logger.warning("present_files: OSS upload failed for %s, falling back to local path", vpath)
                    artifacts.append(vpath)
                    items.append(_artifact_item(vpath))

    update: dict = {
        "artifacts": artifacts,
        "messages": [ToolMessage("Successfully presented files", tool_call_id=tool_call_id, artifact={"items": items})],
    }
    if materials_update:
        update["materials"] = materials_update
    return Command(update=update)


# Client-facing visibility for MessageStreamMiddleware: presented files are final
# deliverables, emitted as an `artifact` event (carrying ToolMessage.artifact).
present_file_tool.metadata = {"visibility": "artifact"}
