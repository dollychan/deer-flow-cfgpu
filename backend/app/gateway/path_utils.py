"""Shared path resolution for thread virtual paths (e.g. mnt/user-data/outputs/...)."""

from pathlib import Path

from fastapi import HTTPException

from deerflow.config.paths import get_paths


def resolve_thread_virtual_path(thread_id: str, virtual_path: str) -> Path:
    """Resolve a virtual path to the actual filesystem path under thread user-data.

    Args:
        thread_id: The thread ID.
        virtual_path: The virtual path as seen inside the sandbox
                      (e.g., /mnt/user-data/outputs/file.txt).

    Returns:
        The resolved filesystem path.

    Raises:
        HTTPException: If the path is invalid or outside allowed directories.
    """
    try:
        # Thread-only tenancy (thread-tenancy.md §4.1): gateway artifact serving resolves
        # by thread_id alone, matching the thread-only disk layout. No graph state here
        # (HTTP path), so it resolves directly with user_id=None per I3+.
        return get_paths().resolve_virtual_path(thread_id, virtual_path)
    except ValueError as e:
        status = 403 if "traversal" in str(e) else 400
        raise HTTPException(status_code=status, detail=str(e))
