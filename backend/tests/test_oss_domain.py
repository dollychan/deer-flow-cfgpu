"""OSS custom-domain (CNAME) wiring.

``OSSConfig.domain`` lets deployments root every generated URL at a unified CNAME
domain (e.g. ``dream-oss.cfgpu.com``) instead of the SDK's auto-generated bucket
endpoint. When set, ``OSSClient`` configures the SDK client with ``endpoint=domain``
+ ``use_cname=True``.

The alibabacloud SDK is an optional extra and is not installed in the test venv, so
these tests inject a minimal fake ``alibabacloud_oss_v2`` module into ``sys.modules``
to exercise ``OSSClient.__init__``'s config wiring without any real SDK / network IO.
"""

from __future__ import annotations

import sys
import types

import pytest

from deerflow.oss.client import OSSClient
from deerflow.oss.oss_config import OSSConfig

# ── OSSConfig.domain ─────────────────────────────────────────────────────────────


def test_domain_defaults_empty():
    assert OSSConfig().domain == ""


def test_domain_can_be_set():
    assert OSSConfig(domain="dream-oss.cfgpu.com").domain == "dream-oss.cfgpu.com"


# ── Fake SDK ─────────────────────────────────────────────────────────────────────


class _FakeConfig:
    """Stand-in for the object returned by ``oss.config.load_default()``."""

    def __init__(self):
        self.credentials_provider = None
        self.region = None
        self.endpoint = None
        self.use_cname = None


class _FakeReq:
    def __init__(self, **kw):
        self.kw = kw


class _FakeClient:
    def __init__(self, cfg):
        self.cfg = cfg

    def get_bucket_info(self, req):  # _check_bucket() best-effort probe
        return None


def _install_fake_sdk(monkeypatch) -> dict:
    """Inject a fake ``alibabacloud_oss_v2`` (+ ``.credentials``) and return captured state."""
    captured: dict = {}

    oss_mod = types.ModuleType("alibabacloud_oss_v2")
    creds_mod = types.ModuleType("alibabacloud_oss_v2.credentials")

    class _StaticCredentialsProvider:
        def __init__(self, access_key_id=None, access_key_secret=None):
            self.access_key_id = access_key_id
            self.access_key_secret = access_key_secret

    creds_mod.StaticCredentialsProvider = _StaticCredentialsProvider

    config_ns = types.SimpleNamespace(load_default=lambda: _FakeConfig())

    def _client_factory(cfg):
        captured["cfg"] = cfg
        return _FakeClient(cfg)

    oss_mod.credentials = creds_mod
    oss_mod.config = config_ns
    oss_mod.Client = _client_factory
    oss_mod.PutObjectRequest = _FakeReq
    oss_mod.GetObjectRequest = _FakeReq
    oss_mod.GetBucketInfoRequest = _FakeReq

    monkeypatch.setitem(sys.modules, "alibabacloud_oss_v2", oss_mod)
    monkeypatch.setitem(sys.modules, "alibabacloud_oss_v2.credentials", creds_mod)
    return captured


# ── OSSClient.__init__ domain wiring ─────────────────────────────────────────────


def test_domain_sets_endpoint_and_use_cname(monkeypatch):
    captured = _install_fake_sdk(monkeypatch)
    OSSClient(OSSConfig(enabled=True, bucket="b", region="cn-beijing", domain="dream-oss.cfgpu.com"))

    cfg = captured["cfg"]
    assert cfg.endpoint == "dream-oss.cfgpu.com"
    assert cfg.use_cname is True
    assert cfg.region == "cn-beijing"


def test_no_domain_leaves_endpoint_unset(monkeypatch):
    captured = _install_fake_sdk(monkeypatch)
    OSSClient(OSSConfig(enabled=True, bucket="b", region="cn-beijing"))

    cfg = captured["cfg"]
    assert cfg.endpoint is None
    assert cfg.use_cname is None


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
