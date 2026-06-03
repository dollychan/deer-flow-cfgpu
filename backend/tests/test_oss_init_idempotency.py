"""OSS singleton init idempotency (Option B change-gate).

init_oss_client / init_oss_uploader run on every config hot-reload. They must skip
reconstruction when the OSS config is unchanged so a config.yaml mtime bump does not
rebuild ``oss.Client`` + fire a ``_check_bucket()`` network round-trip every reload.
OSSClient/OSSUploader construction is mocked so these stay pure unit tests.
"""

from __future__ import annotations

import deerflow.oss.client as oss_client
import deerflow.oss.uploader as oss_uploader
from deerflow.oss.oss_config import OSSConfig


def _enabled(**over) -> OSSConfig:
    base = {
        "enabled": True,
        "access_key_id": "ak",
        "access_key_secret": "sk",
        "bucket": "b",
        "region": "cn-beijing",
    }
    base.update(over)
    return OSSConfig(**base)


def _reset_client(monkeypatch):
    calls: list[OSSConfig] = []

    class _FakeClient:
        def __init__(self, config):
            calls.append(config)

    monkeypatch.setattr(oss_client, "OSSClient", _FakeClient)
    monkeypatch.setattr(oss_client, "_client", None)
    monkeypatch.setattr(oss_client, "_client_config", None)
    return calls


def _reset_uploader(monkeypatch, *, client=object()):
    calls: list[OSSConfig] = []

    class _FakeUploader:
        def __init__(self, client, config):
            calls.append(config)

    monkeypatch.setattr(oss_uploader, "OSSUploader", _FakeUploader)
    monkeypatch.setattr(oss_uploader, "_uploader", None)
    monkeypatch.setattr(oss_uploader, "_uploader_config", None)
    monkeypatch.setattr(oss_uploader, "get_oss_client", lambda: client)
    return calls


# ── client ────────────────────────────────────────────────────────────────────


def test_client_skips_rebuild_when_config_unchanged(monkeypatch):
    calls = _reset_client(monkeypatch)
    oss_client.init_oss_client(_enabled())
    oss_client.init_oss_client(_enabled())  # equal value, fresh instance
    assert len(calls) == 1  # not rebuilt → no second _check_bucket() round-trip
    assert oss_client.get_oss_client() is not None


def test_client_rebuilds_on_config_change(monkeypatch):
    calls = _reset_client(monkeypatch)
    oss_client.init_oss_client(_enabled(bucket="b1"))
    oss_client.init_oss_client(_enabled(bucket="b2"))
    assert len(calls) == 2


def test_client_disabled_clears_singleton_and_config(monkeypatch):
    _reset_client(monkeypatch)
    oss_client.init_oss_client(_enabled())
    oss_client.init_oss_client(OSSConfig(enabled=False))
    assert oss_client.get_oss_client() is None
    assert oss_client._client_config is None


# ── uploader ──────────────────────────────────────────────────────────────────


def test_uploader_skips_rebuild_when_config_unchanged(monkeypatch):
    calls = _reset_uploader(monkeypatch)
    oss_uploader.init_oss_uploader(_enabled())
    oss_uploader.init_oss_uploader(_enabled())
    assert len(calls) == 1
    assert oss_uploader.get_oss_uploader() is not None


def test_uploader_rebuilds_on_config_change(monkeypatch):
    calls = _reset_uploader(monkeypatch)
    oss_uploader.init_oss_uploader(_enabled(bucket="b1"))
    oss_uploader.init_oss_uploader(_enabled(bucket="b2"))
    assert len(calls) == 2


def test_uploader_none_when_client_missing(monkeypatch):
    calls = _reset_uploader(monkeypatch, client=None)
    oss_uploader.init_oss_uploader(_enabled())
    assert oss_uploader.get_oss_uploader() is None
    assert calls == []
