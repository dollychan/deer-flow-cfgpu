"""High-level OSS upload service.

Provides two operations:
- ``upload_local_file``: upload a file from the server filesystem and return a presigned URL.
- ``handle_remote_url``: pass through a remote URL as-is, or re-upload it to AliOSS when
  the URL is about to expire (controlled by ``cfgpu_url_refresh_threshold_hours``).
"""

from __future__ import annotations

import asyncio
import logging
import mimetypes
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

from deerflow.oss.client import OSSClient, get_oss_client
from deerflow.oss.oss_config import OSSConfig, get_oss_config

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


def _url_filename(url: str) -> str | None:
    """Extract a filename from a URL path component."""
    path = urlparse(url).path
    name = Path(path).name
    return name or None


class OSSUploader:
    """Business-layer upload service used by ``present_files`` and ``present_urls``."""

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
        """Upload a local file to MinIO and return a presigned URL.

        Args:
            virtual_path: Virtual sandbox path (used only for logging).
            physical_path: Actual filesystem path on the host.
            thread_id: Used as the top-level prefix in the bucket.
        """
        filename = Path(physical_path).name
        category = _infer_category(filename)
        object_key = f"agent-artifacts/{thread_id}/{category}/{filename}"

        loop = asyncio.get_event_loop()
        url = await loop.run_in_executor(
            None, self._client.upload_file, object_key, physical_path
        )
        logger.info("OSSUploader: uploaded %s → %s", virtual_path, object_key)
        return url

    async def handle_remote_url(
        self,
        url: str,
        expires_at: datetime | None,
        thread_id: str,
        filename_hint: str | None = None,
    ) -> str:
        """Return the URL as-is, or re-upload to MinIO if it is about to expire.

        Args:
            url: The original remote URL (e.g. from cfgpu generate_image).
            expires_at: When the URL expires. ``None`` = unknown (treated as valid).
            thread_id: Used as the top-level prefix in the bucket.
            filename_hint: Preferred filename for the uploaded object.
        """
        if not self._needs_reupload(expires_at):
            return url

        logger.info("OSSUploader: cfgpu URL expiring soon, re-uploading to AliOSS — %s", url)
        try:
            import httpx

            async with httpx.AsyncClient(follow_redirects=True, timeout=60) as http:
                resp = await http.get(url)
                resp.raise_for_status()

            data = resp.content
            content_type = resp.headers.get("content-type", "application/octet-stream").split(";")[0].strip()
            filename = filename_hint or _url_filename(url) or "file"
            category = _infer_category(filename) if filename != "file" else _content_type_category(content_type)
            object_key = f"agent-artifacts/{thread_id}/{category}/{filename}"

            loop = asyncio.get_event_loop()
            new_url = await loop.run_in_executor(
                None, self._client.upload_bytes, object_key, data, content_type
            )
            logger.info("OSSUploader: re-uploaded cfgpu URL → %s", object_key)
            return new_url
        except Exception:
            logger.warning("OSSUploader: failed to re-upload %s, returning original URL", url, exc_info=True)
            return url

    # ── Internals ──────────────────────────────────────────────────────────────

    def _needs_reupload(self, expires_at: datetime | None) -> bool:
        if self._config.cfgpu_url_refresh_threshold_hours == 0:
            return False
        if expires_at is None:
            return False
        threshold = timedelta(hours=self._config.cfgpu_url_refresh_threshold_hours)
        return (expires_at - datetime.now(timezone.utc)) < threshold


def _content_type_category(content_type: str) -> str:
    for prefix, category in _CATEGORY_BY_MIME_PREFIX:
        if content_type.startswith(prefix):
            return category
    return _DEFAULT_CATEGORY


# ── Singleton ──────────────────────────────────────────────────────────────────

_uploader: OSSUploader | None = None


def get_oss_uploader() -> OSSUploader | None:
    """Return the process-level OSSUploader, or None if OSS is disabled."""
    return _uploader


def init_oss_uploader(config: OSSConfig) -> None:
    """Initialise (or reinitialise) the singleton.

    Called by :func:`deerflow.config.app_config.AppConfig._apply_singleton_configs`.
    No-ops when ``config.enabled`` is False.
    """
    global _uploader
    client = get_oss_client()
    if not config.enabled or client is None:
        _uploader = None
        return
    _uploader = OSSUploader(client, config)
