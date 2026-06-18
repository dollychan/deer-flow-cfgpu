"""High-level OSS upload service.

Provides ``upload_local_file``: upload a file from the server filesystem and return
a reference (presigned URL or bare object key, per the ``presigned_url`` config).

Lifecycle management of remote cfgpu URLs (refresh / re-upload) is intentionally out
of scope here and is handled separately.
"""

from __future__ import annotations

import asyncio
import logging
import mimetypes
from pathlib import Path

from deerflow.oss.client import OSSClient, get_oss_client
from deerflow.oss.oss_config import OSSConfig

logger = logging.getLogger(__name__)

_CATEGORY_BY_MIME_PREFIX: list[tuple[str, str]] = [
    ("image/", "images"),
    ("video/", "videos"),
    ("audio/", "audios"),
]
_DEFAULT_CATEGORY = "files"


def _infer_category(filename: str) -> str:
    mime, _ = mimetypes.guess_type(filename)
    if mime:
        for prefix, category in _CATEGORY_BY_MIME_PREFIX:
            if mime.startswith(prefix):
                return category
    return _DEFAULT_CATEGORY


class OSSUploader:
    """Business-layer upload service used by ``present_files``."""

    def __init__(self, client: OSSClient, config: OSSConfig) -> None:
        self._client = client
        self._config = config

    # ── Public API ─────────────────────────────────────────────────────────────

    async def upload_local_file(
        self,
        virtual_path: str,
        physical_path: str,
        thread_id: str,
    ) -> str:
        """Upload a local file to OSS and return a reference to it.

        The reference is a presigned URL or the bare object key, depending on the
        ``presigned_url`` config (resolved in :class:`OSSClient`).

        Args:
            virtual_path: Virtual sandbox path (used only for logging).
            physical_path: Actual filesystem path on the host.
            thread_id: Used as the top-level prefix in the bucket.
        """
        filename = Path(physical_path).name
        category = _infer_category(filename)
        object_key = f"agent-artifacts/{thread_id}/{category}/{filename}"

        loop = asyncio.get_event_loop()
        ref = await loop.run_in_executor(
            None, self._client.upload_file, object_key, physical_path
        )
        logger.info("OSSUploader: uploaded %s → %s", virtual_path, object_key)
        return ref


# ── Singleton ──────────────────────────────────────────────────────────────────

_uploader: OSSUploader | None = None
_uploader_config: OSSConfig | None = None


def get_oss_uploader() -> OSSUploader | None:
    """Return the process-level OSSUploader, or None if OSS is disabled."""
    return _uploader


def init_oss_uploader(config: OSSConfig) -> None:
    """Initialise (or reinitialise) the singleton.

    Called by :func:`deerflow.config.app_config.AppConfig._apply_singleton_configs`
    on every config hot-reload. No-ops when ``config.enabled`` is False, and skips
    reconstruction when the OSS config is unchanged. The underlying OSSClient is only
    rebuilt when its config changes (see :func:`deerflow.oss.client.init_oss_client`),
    so an unchanged config means the existing uploader still wraps the current client.
    """
    global _uploader, _uploader_config
    client = get_oss_client()
    if not config.enabled or client is None:
        _uploader = None
        _uploader_config = None
        return
    if _uploader is not None and _uploader_config == config:
        return
    _uploader = OSSUploader(client, config)
    _uploader_config = config
