"""Alibaba Cloud OSS client wrapper — thin singleton around alibabacloud-oss-v2 SDK."""

from __future__ import annotations

import logging
import mimetypes
from datetime import timedelta
from pathlib import Path

from deerflow.oss.oss_config import OSSConfig

logger = logging.getLogger(__name__)


def _guess_content_type(filename: str) -> str:
    mime, _ = mimetypes.guess_type(filename)
    return mime or "application/octet-stream"


class OSSClient:
    """Thin wrapper around the alibabacloud_oss_v2 client providing upload + presigned-URL generation.

    Callers should not instantiate this directly; use :func:`get_oss_client` instead.
    """

    def __init__(self, config: OSSConfig) -> None:
        try:
            import alibabacloud_oss_v2 as oss
            from alibabacloud_oss_v2 import credentials
        except ImportError as exc:
            raise ImportError(
                "The 'alibabacloud-oss-v2' package is required for OSS integration. "
                "Install it with: uv add alibabacloud-oss-v2"
            ) from exc

        self._oss = oss
        creds = credentials.StaticCredentialsProvider(
            access_key_id=config.access_key_id,
            access_key_secret=config.access_key_secret,
        )
        cfg = oss.config.load_default()
        cfg.credentials_provider = creds
        if config.region:
            cfg.region = config.region

        self._client = oss.Client(cfg)
        self._bucket = config.bucket
        self._expires = timedelta(days=config.presigned_url_expires_days)
        self._return_presigned = config.presigned_url
        self._check_bucket()

    # ── Public API ─────────────────────────────────────────────────────────────

    def upload_file(self, object_key: str, local_path: str) -> str:
        """Upload a local file and return a reference to it.

        Returns a presigned GET URL when ``presigned_url`` is enabled, otherwise the
        bare ``object_key`` (the file's path inside the bucket).
        """
        content_type = _guess_content_type(Path(local_path).name)
        with open(local_path, "rb") as f:
            self._client.put_object(
                self._oss.PutObjectRequest(
                    bucket=self._bucket,
                    key=object_key,
                    body=f,
                    content_type=content_type,
                )
            )
        return self._presigned_url(object_key) if self._return_presigned else object_key

    def upload_bytes(self, object_key: str, data: bytes, content_type: str | None = None) -> str:
        """Upload an in-memory byte payload under ``object_key`` and return its **bare object key**.

        Used by ``OSSUploader.rehost_url`` to re-host a remote (cfgpu temp) URL into our
        bucket without staging a local file. Always returns the object_key (NOT a presigned
        URL): materials store the stable object_key and presign at the out-gate (§4.2/§4.3).
        """
        self._client.put_object(
            self._oss.PutObjectRequest(
                bucket=self._bucket,
                key=object_key,
                body=data,
                content_type=content_type or _guess_content_type(object_key),
            )
        )
        return object_key

    def presign(self, object_key: str) -> str:
        """Always return a presigned GET URL for ``object_key`` (local HMAC, no network IO).

        Unlike :meth:`upload_file`'s return value, this **ignores** the ``presigned_url``
        config toggle: a cfgpu tool consuming the ref needs a fetchable URL regardless of
        the client-facing ``present_files`` default (BUG-027 deployment split). Used by
        ``MaterialsMiddleware`` out-gate signing (cfgpu-docs/materials.md §4.3).
        """
        return self._presigned_url(object_key)

    # ── Internals ──────────────────────────────────────────────────────────────

    def _presigned_url(self, object_key: str) -> str:
        result = self._client.presign(
            self._oss.GetObjectRequest(bucket=self._bucket, key=object_key),
            expires=self._expires,
        )
        return result.url

    def _check_bucket(self) -> None:
        """Best-effort bucket existence check at startup. Logs warning on failure instead of raising.

        RAM sub-accounts typically lack GetBucketInfo permission; upload errors will surface
        naturally when the first put_object is attempted.
        """
        try:
            self._client.get_bucket_info(self._oss.GetBucketInfoRequest(bucket=self._bucket))
            logger.info("OSSClient: bucket %r verified", self._bucket)
        except Exception as exc:
            exc_str = str(exc)
            if "NoSuchBucket" in exc_str or "404" in exc_str:
                logger.warning(
                    "OSSClient: bucket %r not found — check bucket name and region config", self._bucket
                )
            else:
                logger.debug("OSSClient: bucket check skipped (%s)", exc_str.split("\n")[0])


# ── Singleton ──────────────────────────────────────────────────────────────────

_client: OSSClient | None = None
_client_config: OSSConfig | None = None


def get_oss_client() -> OSSClient | None:
    """Return the process-level OSSClient, or None if OSS is disabled."""
    return _client


def init_oss_client(config: OSSConfig) -> None:
    """Initialise (or reinitialise) the singleton from the given config.

    Called by :func:`deerflow.config.app_config.AppConfig._apply_singleton_configs`
    after config is loaded — i.e. on every config hot-reload. No-ops when
    ``config.enabled`` is False, and skips reconstruction when the OSS config is
    unchanged so a config.yaml mtime bump does not trigger a fresh ``oss.Client``
    plus a ``_check_bucket()`` network round-trip on every reload.
    """
    global _client, _client_config
    if not config.enabled:
        _client = None
        _client_config = None
        return
    if _client is not None and _client_config == config:
        return
    _client = OSSClient(config)
    _client_config = config
    logger.info(
        "OSSClient: initialised — region=%s bucket=%s",
        config.region or "(default)",
        config.bucket,
    )
