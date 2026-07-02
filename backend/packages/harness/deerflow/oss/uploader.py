"""High-level OSS upload service.

Provides ``upload_local_file`` (stage a server-filesystem file) and ``rehost_url``
(fetch a remote URL and re-host its bytes into our bucket). Both return a reference;
``rehost_url`` always returns the bare object_key (materials presign at the out-gate).
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import mimetypes
from pathlib import Path
from urllib.parse import unquote, urlsplit

import httpx

from deerflow.oss.client import OSSClient, get_oss_client
from deerflow.oss.oss_config import OSSConfig

logger = logging.getLogger(__name__)

# Re-host fetch limits: cfdream temp media is bounded; a stuck CDN must not hang a run.
_REHOST_TIMEOUT_S = 60.0
_REHOST_MAX_BYTES = 256 * 1024 * 1024  # 256 MiB ceiling (video safety)

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

    async def upload_inline_bytes(
        self,
        object_key: str,
        data: bytes,
        content_type: str | None = None,
    ) -> str:
        """Upload an in-memory byte payload and return a client-facing reference.

        Used by ``present_files`` for synthesized artifacts (the wrapped-document
        HTML and its PNG snapshot, cfgpu-docs/present-files-tool.md §6.1). The
        blocking ``put_object`` is offloaded to a thread so the event loop is
        never blocked.

        Mirrors :meth:`upload_local_file`'s return contract: a presigned GET URL
        when ``presigned_url`` is enabled (``kind="url"`` on the client), else the
        bare object key (``kind="path"``). ``presign`` is a local HMAC with no
        network IO, so it is safe to call inline.

        Args:
            object_key: The full object key (callers derive a content-hash key for
                idempotency — re-presenting the same file does not duplicate).
            data: The raw bytes to upload.
            content_type: MIME type; inferred from ``object_key`` when omitted.
        """
        loop = asyncio.get_event_loop()
        key = await loop.run_in_executor(
            None, self._client.upload_bytes, object_key, data, content_type
        )
        return self._client.presign(key) if self._config.presigned_url else key

    def inline_object_key(
        self,
        data: bytes,
        thread_id: str,
        *,
        mime_type: str | None = None,
        filename: str | None = None,
    ) -> str:
        """Derive the (deterministic) object_key for inline bytes **without** uploading.

        Pure content-hash key derivation so callers can dedup against the registry
        before spending an upload (mirrors the ``find_by_address`` short-circuit on the
        URL path). :meth:`rehost_bytes` derives the same key.
        """
        name = _inline_filename(data, mime_type, filename)
        return f"agent-artifacts/{thread_id}/{_infer_category(name)}/{name}"

    async def rehost_bytes(
        self,
        data: bytes,
        thread_id: str,
        *,
        mime_type: str | None = None,
        filename: str | None = None,
    ) -> str:
        """Re-host an in-memory media payload into our bucket; return the **object_key**.

        Sibling of :meth:`rehost_url` for inline media that arrives without a URL (e.g.
        MiniMax speech's hex audio blob, normalised by the MCP into a base64
        ``inline_media`` descriptor). The object_key embeds a content hash so re-hosting
        the same bytes (a ``task_wait`` replay or a repeat within one batch) is idempotent
        — same key, no duplicate object.

        The blocking OSS ``put_object`` is offloaded to a thread so the event loop is never
        blocked. Raises on oversize / upload failure — the caller fail-opens (drops the item).
        """
        if len(data) > _REHOST_MAX_BYTES:
            raise ValueError(f"inline payload {len(data)} bytes exceeds {_REHOST_MAX_BYTES} ceiling")
        object_key = self.inline_object_key(data, thread_id, mime_type=mime_type, filename=filename)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._client.upload_bytes, object_key, data, mime_type)
        logger.info("OSSUploader: re-hosted inline bytes → %s (%d bytes)", object_key, len(data))
        return object_key

    async def rehost_url(self, url: str, thread_id: str) -> tuple[str, int]:
        """Fetch a remote URL and re-host its bytes into our bucket; return ``(object_key, size)``.

        Used by ``MaterialsMiddleware`` Capture (§4.2): a freshly generated cfdream URL is
        short-lived, so its bytes are pulled into our OSS once and thereafter referenced by
        the stable object_key (presigned at the out-gate). The object_key embeds a stable
        hash of the source URL so re-hosting the same URL (e.g. a ``task_wait`` replay) is
        idempotent — same key, no duplicate object, dedup-friendly.

        ``size`` is the fetched byte count — the caller records it on the material so the
        downstream ``artifact`` item can surface the file size (only the uploader holds the
        bytes on this path, so it is the natural place to measure).

        Network fetch is async (httpx); the blocking OSS ``put_object`` is offloaded to a
        thread so the event loop is never blocked.

        Raises on fetch / upload failure — the caller marks the material ``stable=false``.
        """
        async with httpx.AsyncClient(follow_redirects=True, timeout=_REHOST_TIMEOUT_S) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.content
            content_type = (resp.headers.get("content-type") or "").split(";")[0].strip() or None
        size = len(data)
        if size > _REHOST_MAX_BYTES:
            raise ValueError(f"re-host payload {size} bytes exceeds {_REHOST_MAX_BYTES} ceiling")

        filename = _filename_from_url(url, content_type)
        category = _infer_category(filename)
        object_key = f"agent-artifacts/{thread_id}/{category}/{filename}"

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._client.upload_bytes, object_key, data, content_type)
        logger.info("OSSUploader: re-hosted %s → %s (%d bytes)", url, object_key, size)
        return object_key, size


def _inline_filename(data: bytes, mime_type: str | None, filename: str | None) -> str:
    """Stable, collision-resistant object filename for inline bytes: ``<sha1(data)[:16]>-<name>``.

    The content hash makes re-hosting identical bytes deterministic (idempotent key); the
    extension is taken from ``filename`` or guessed from ``mime_type`` so the object serves
    with a sensible type (and ``_infer_category`` can bucket it as image/video/audio).
    """
    digest = hashlib.sha1(data).hexdigest()[:16]
    name = filename or "inline"
    if not Path(name).suffix and mime_type:
        ext = mimetypes.guess_extension(mime_type)
        if ext:
            name = f"{name}{ext}"
    return f"{digest}-{name}"


def _filename_from_url(url: str, content_type: str | None) -> str:
    """Stable, collision-resistant object filename for a re-hosted URL.

    ``<sha1(url)[:8]>-<basename>``; basename comes from the URL path, with an extension
    guessed from ``content_type`` when the path has none. The URL hash keeps distinct
    sources apart and makes re-hosting the same URL deterministic (idempotent key).
    """
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:8]
    name = Path(unquote(urlsplit(url).path)).name
    if not name:
        name = "file"
    if not Path(name).suffix and content_type:
        ext = mimetypes.guess_extension(content_type)
        if ext:
            name = f"{name}{ext}"
    return f"{digest}-{name}"


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
