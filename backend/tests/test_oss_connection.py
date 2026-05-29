"""AliOSS connection test — upload, presigned URL, download.

Run:
    cd backend
    PYTHONPATH=. uv run python tests/test_oss_connection.py
"""

from __future__ import annotations

import io
import os
import sys
import datetime


def _load_env() -> None:
    env_path = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
    if not os.path.exists(env_path):
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())


_load_env()

ACCESS_KEY_ID = os.environ.get("OSS_ACCESS_KEY_ID", "")
ACCESS_KEY_SECRET = os.environ.get("OSS_ACCESS_KEY_SECRET", "")
REGION = "cn-beijing"
BUCKET = "cf-dream"
TEST_OBJECT_KEY = "agent-artifacts/_test/files/oss_test.txt"
TEST_CONTENT = b"Hello from deerflow OSS test!"


def _make_client():
    import alibabacloud_oss_v2 as oss
    from alibabacloud_oss_v2 import credentials

    creds = credentials.StaticCredentialsProvider(
        access_key_id=ACCESS_KEY_ID,
        access_key_secret=ACCESS_KEY_SECRET,
    )
    cfg = oss.config.load_default()
    cfg.credentials_provider = creds
    cfg.region = REGION
    return oss.Client(cfg), oss


def test_upload() -> str:
    print(f"\n[1] Upload: key={TEST_OBJECT_KEY}")
    client, oss = _make_client()
    client.put_object(oss.PutObjectRequest(
        bucket=BUCKET,
        key=TEST_OBJECT_KEY,
        body=io.BytesIO(TEST_CONTENT),
        content_length=len(TEST_CONTENT),
        content_type="text/plain",
    ))
    print("    Upload OK")
    return TEST_OBJECT_KEY


def test_presign(object_key: str) -> str:
    print(f"\n[2] Presign: key={object_key}")
    client, oss = _make_client()
    result = client.presign(
        oss.GetObjectRequest(bucket=BUCKET, key=object_key),
        expires=datetime.timedelta(days=7),
    )
    url = result.url
    print(f"    Presigned URL: {url[:80]}...")
    return url


def test_download(url: str) -> None:
    print(f"\n[3] Download via presigned URL")
    import urllib.request
    with urllib.request.urlopen(url) as resp:
        data = resp.read()
    assert data == TEST_CONTENT, f"Content mismatch: {data!r}"
    print(f"    Downloaded {len(data)} bytes, content matches: {data.decode()!r}")


def main() -> None:
    if not ACCESS_KEY_ID or not ACCESS_KEY_SECRET:
        print("ERROR: OSS_ACCESS_KEY_ID / OSS_ACCESS_KEY_SECRET not set", file=sys.stderr)
        sys.exit(1)

    print(f"Region   : {REGION}")
    print(f"Bucket   : {BUCKET}")
    print(f"AK       : {ACCESS_KEY_ID[:8]}...")

    try:
        key = test_upload()
        url = test_presign(key)
        test_download(url)
        print("\n✓ All tests passed")
    except Exception as exc:
        print(f"\n✗ Test failed: {exc}", file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
