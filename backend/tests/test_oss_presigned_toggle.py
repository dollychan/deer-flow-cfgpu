"""OSS ``presigned_url`` toggle + removal of ``cfdream_url_refresh_threshold_hours``.

`OSSClient.upload_file` returns a presigned GET URL when ``presigned_url`` is true
(default) and the bare object key when it is false. The SDK client is faked via
``__new__`` so these stay pure unit tests with no network or SDK construction.
"""

from __future__ import annotations

from datetime import timedelta

from deerflow.oss.client import OSSClient
from deerflow.oss.oss_config import OSSConfig

# ── OSSConfig ───────────────────────────────────────────────────────────────────


def test_presigned_url_defaults_true():
    assert OSSConfig().presigned_url is True


def test_presigned_url_can_be_disabled():
    assert OSSConfig(presigned_url=False).presigned_url is False


def test_cfdream_refresh_threshold_field_removed():
    # The re-upload threshold knob is gone; cfdream URL lifecycle is handled elsewhere.
    assert not hasattr(OSSConfig(), "cfdream_url_refresh_threshold_hours")
    assert "cfdream_url_refresh_threshold_hours" not in OSSConfig.model_fields


# ── OSSClient.upload_file toggle ─────────────────────────────────────────────────


class _FakeReq:
    def __init__(self, **kw):
        self.kw = kw


class _FakeOssModule:
    PutObjectRequest = _FakeReq
    GetObjectRequest = _FakeReq


class _FakePresignResult:
    def __init__(self, url: str):
        self.url = url


class _FakeOssClient:
    def __init__(self):
        self.put_calls: list[_FakeReq] = []

    def put_object(self, req):
        self.put_calls.append(req)

    def presign(self, req, expires=None):
        return _FakePresignResult(f"https://signed/{req.kw['key']}")


def _make_client(presigned: bool) -> OSSClient:
    client = OSSClient.__new__(OSSClient)  # bypass __init__ (no SDK / network)
    client._oss = _FakeOssModule()
    client._client = _FakeOssClient()
    client._bucket = "b"
    client._expires = timedelta(days=7)
    client._return_presigned = presigned
    return client


def test_upload_file_returns_presigned_url_when_enabled(tmp_path):
    f = tmp_path / "x.txt"
    f.write_text("hi")
    client = _make_client(presigned=True)
    key = "agent-artifacts/t/files/x.txt"

    result = client.upload_file(key, str(f))

    assert result == f"https://signed/{key}"
    assert len(client._client.put_calls) == 1  # object still uploaded


def test_upload_file_returns_object_key_when_disabled(tmp_path):
    f = tmp_path / "x.txt"
    f.write_text("hi")
    client = _make_client(presigned=False)
    key = "agent-artifacts/t/files/x.txt"

    result = client.upload_file(key, str(f))

    assert result == key  # bare object key, no presign
    assert len(client._client.put_calls) == 1
