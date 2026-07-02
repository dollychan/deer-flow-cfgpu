"""Configuration model for OSS (object storage) integration."""

from __future__ import annotations

from pydantic import BaseModel, Field


class OSSConfig(BaseModel):
    """OSS configuration for serving agent-generated files via Alibaba Cloud OSS.

    When ``enabled`` is false (default), ``present_files`` retains its current
    behaviour: virtual paths are written to ``artifacts`` as-is and served by
    the Gateway ``/api/threads/{id}/artifacts/{path}`` endpoint.

    When ``enabled`` is true, ``present_files`` uploads each output file to AliOSS.
    With ``presigned_url`` true it replaces the local path with a presigned URL before
    writing to ``artifacts``; with ``presigned_url`` false (default) it writes the
    bare object key (the path inside the bucket) instead.

    ``domain`` (optional) sets a custom CNAME domain for the generated URLs. When empty
    (default), presigned URLs use the SDK's auto-generated virtual-hosted endpoint
    ``{bucket}.oss-{region}.aliyuncs.com``. When set (e.g. ``dream-oss.cfgpu.com``), the
    OSS client is configured with ``endpoint=domain`` + ``use_cname=True`` so every
    presigned URL is rooted at that unified domain (``https://dream-oss.cfgpu.com/{key}?...``);
    the domain must be CNAME-mapped to the bucket in DNS. Scheme is optional (defaults to
    https).

    ``delete_artifacts_on_thread_delete`` (default false) gates whether handling an MQ
    ``type=delete`` also recycles the thread's OSS artifact prefix
    ``agent-artifacts/{thread_id}/``. Off by default so OSS artifacts survive a thread
    delete; turn on to have the Consumer reclaim them alongside the local state wipe.

    Config path: ``oss:`` section in config.yaml.
    All string fields support ``$ENV_VAR`` substitution via AppConfig.resolve_env_variables.

    Example config.yaml snippet::

        oss:
          enabled: true
          access_key_id: $OSS_ACCESS_KEY_ID
          access_key_secret: $OSS_ACCESS_KEY_SECRET
          bucket: cf-dream
          region: cn-beijing
          domain: $OSS_DOMAIN            # e.g. dream-oss.cfgpu.com — unified CNAME domain for URLs
          presigned_url_expires_days: 7
          presigned_url: false
          delete_artifacts_on_thread_delete: false
    """

    enabled: bool = Field(default=False, description="Enable OSS upload for presented files")
    access_key_id: str = Field(default="", description="Alibaba Cloud access key ID")
    access_key_secret: str = Field(default="", description="Alibaba Cloud access key secret")
    bucket: str = Field(default="cf-dream", description="Target bucket name")
    region: str = Field(default="", description="AliOSS region, e.g. cn-beijing; required for V4 signing")
    domain: str = Field(
        default="",
        description=(
            "Custom CNAME domain for generated URLs, e.g. dream-oss.cfgpu.com. When set, the OSS "
            "client uses endpoint=domain + use_cname=True so presigned URLs are rooted at this unified "
            "domain instead of the auto-generated bucket endpoint. Must be CNAME-mapped to the bucket. "
            "Scheme optional (defaults to https). Empty = SDK default endpoint."
        ),
    )
    presigned_url_expires_days: int = Field(
        default=7,
        ge=1,
        le=7,
        description="Presigned URL validity in days (AliOSS V4 max: 7 days)",
    )
    presigned_url: bool = Field(
        default=False,
        description="When true, uploads return a presigned GET URL; when false, return the bare object key (bucket path).",
    )
    delete_artifacts_on_thread_delete: bool = Field(
        default=False,
        description="When true, handling an MQ type=delete also recycles the thread's OSS artifact prefix agent-artifacts/{thread_id}/; when false (default), OSS artifacts are left untouched and preserved.",
    )


_oss_config: OSSConfig = OSSConfig()


def get_oss_config() -> OSSConfig:
    return _oss_config


def set_oss_config(config: OSSConfig) -> None:
    global _oss_config
    _oss_config = config


def load_oss_config_from_dict(config_dict: dict | None) -> None:
    global _oss_config
    _oss_config = OSSConfig(**(config_dict or {}))
