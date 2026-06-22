from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated

from langchain.tools import InjectedToolCallId, tool
from langchain_core.messages import ToolMessage
from langgraph.config import get_config
from langgraph.types import Command

from deerflow.config.paths import VIRTUAL_PATH_PREFIX
from deerflow.tools.types import Runtime

logger = logging.getLogger(__name__)

OUTPUTS_VIRTUAL_PREFIX = f"{VIRTUAL_PATH_PREFIX}/outputs"


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


@tool("present_files", parse_docstring=True)
async def present_file_tool(
    runtime: Runtime,
    filepaths: list[str],
    tool_call_id: Annotated[str, InjectedToolCallId],
) -> Command:
    """Make files visible to the user for viewing and rendering in the client interface.

    When to use the present_files tool:

    - Making any file available for the user to view, download, or interact with
    - Presenting multiple related files at once
    - After creating files that should be presented to the user

    When NOT to use the present_files tool:
    - When you only need to read file contents for your own processing
    - For temporary or intermediate files not meant for user viewing

    Notes:
    - You should call this tool after creating files and moving them to the `/mnt/user-data/outputs` directory.
    - This tool can be safely called in parallel with other tools. State updates are handled by a reducer to prevent conflicts.

    Args:
        filepaths: List of absolute file paths to present to the user. **Only** files in `/mnt/user-data/outputs` can be presented.
    """
    try:
        normalized_paths = [_normalize_presented_filepath(runtime, filepath) for filepath in filepaths]
    except ValueError as exc:
        return Command(
            update={"messages": [ToolMessage(f"Error: {exc}", tool_call_id=tool_call_id)]},
        )

    from deerflow.oss.uploader import get_oss_uploader

    uploader = get_oss_uploader()
    if uploader is None:
        # OSS not configured: original behaviour — store virtual paths directly.
        return Command(
            update={
                "artifacts": normalized_paths,
                "messages": [
                    ToolMessage(
                        "Successfully presented files",
                        tool_call_id=tool_call_id,
                        artifact={"items": [_artifact_item(p) for p in normalized_paths]},
                    )
                ],
            },
        )

    # OSS enabled: upload each file and replace path with presigned URL.
    thread_id = _get_thread_id(runtime) or "unknown"
    thread_data = (runtime.state or {}).get("thread_data") or {}
    outputs_path = thread_data.get("outputs_path", "")

    resolved: list[str] = []
    for vpath in normalized_paths:
        try:
            physical = _virtual_to_physical(vpath, outputs_path)
            url = await uploader.upload_local_file(vpath, physical, thread_id)
            resolved.append(url)
        except Exception:
            logger.warning("present_files: OSS upload failed for %s, falling back to local path", vpath)
            resolved.append(vpath)

    return Command(
        update={
            "artifacts": resolved,
            "messages": [
                ToolMessage(
                    "Successfully presented files",
                    tool_call_id=tool_call_id,
                    artifact={"items": [_artifact_item(r) for r in resolved]},
                )
            ],
        },
    )


# Client-facing visibility for MessageStreamMiddleware: presented files are final
# deliverables, emitted as an `artifact` event (carrying ToolMessage.artifact).
present_file_tool.metadata = {"visibility": "artifact"}
