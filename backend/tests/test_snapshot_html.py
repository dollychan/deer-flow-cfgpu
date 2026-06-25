"""Tests for the ``Sandbox.snapshot_html`` capability (cfgpu-docs/present-files-tool.md §3).

The AIO implementation drives the sandbox's built-in ``chromium-browser`` headless
``--screenshot`` CLI over the shell/file API (the gem runtime exposes no browser HTTP
API), then fetches the PNG with ``download_file`` (binary-safe — base64-over-shell was
unreliable for tens of KB). Covers:
- mkdir → write HTML → render → download PNG bytes, with temp cleanup,
- fail-open (render/download failure → None; exception → None),
- ``LocalSandbox.snapshot_html`` (inherited default) is always None.
"""

import pytest


@pytest.fixture()
def sandbox():
    from unittest.mock import patch

    with patch("deerflow.community.aio_sandbox.aio_sandbox.AioSandboxClient"):
        from deerflow.community.aio_sandbox.aio_sandbox import AioSandbox

        sb = AioSandbox(id="test-sandbox", base_url="http://localhost:8080")
        # Replace the high-level helpers with spies; snapshot_html composes them.
        sb.write_file = _Spy()
        sb.execute_command = _Spy(default="(no output)")
        sb.download_file = _Spy()
        return sb


class _Spy:
    def __init__(self, default=None):
        self.calls = []
        self.side_effect = None
        self.return_value = default

    def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if self.side_effect is not None:
            raise self.side_effect
        return self.return_value


def _commands(spy):
    return [c[0][0] for c in spy.calls if c[0]]


def test_snapshot_html_renders_and_downloads_png(sandbox):
    png_raw = b"\x89PNG\r\n\x1a\nfake-bytes"
    sandbox.download_file.return_value = png_raw

    out = sandbox.snapshot_html("<h1>hi</h1>", full_page=True)

    assert out == png_raw
    # HTML written under the virtual prefix (a hidden snapshots dir) so download_file
    # — which is path-restricted to that prefix — can fetch the result.
    (write_args, _) = sandbox.write_file.calls[0]
    assert "/.deerflow-snapshots/" in write_args[0] and write_args[0].endswith(".html")
    assert write_args[1] == "<h1>hi</h1>"
    cmds = _commands(sandbox.execute_command)
    assert any(c.startswith("mkdir -p ") for c in cmds)
    render = next(c for c in cmds if "chromium-browser" in c)
    assert "--headless=new --no-sandbox" in render
    assert "--screenshot=" in render and "file://" in render
    # PNG fetched via download_file (NOT base64-over-shell), from the rendered path.
    (dl_args, _) = sandbox.download_file.calls[0]
    assert dl_args[0].endswith(".png") and "/.deerflow-snapshots/" in dl_args[0]
    # Temp files cleaned up in finally.
    assert any(c.startswith("rm -f ") for c in cmds)


def test_snapshot_html_fold_uses_shorter_window(sandbox):
    sandbox.download_file.return_value = b"\x89PNGx"
    sandbox.snapshot_html("<p>x</p>", full_page=False)
    render = next(c for c in _commands(sandbox.execute_command) if "chromium-browser" in c)
    assert f"--window-size=1280,{sandbox._SNAPSHOT_HEIGHT_FOLD}" in render


def test_snapshot_html_none_on_empty_png(sandbox):
    sandbox.download_file.return_value = b""  # render produced a 0-byte file
    assert sandbox.snapshot_html("<h1>hi</h1>") is None
    assert any(c.startswith("rm -f ") for c in _commands(sandbox.execute_command))  # still cleaned up


def test_snapshot_html_fail_open_on_download_error(sandbox):
    sandbox.download_file.side_effect = OSError("no such file")  # render produced no file
    assert sandbox.snapshot_html("<h1>hi</h1>") is None


def test_snapshot_html_fail_open_on_exception(sandbox):
    sandbox.write_file.side_effect = RuntimeError("boom")
    assert sandbox.snapshot_html("<h1>hi</h1>") is None


def test_snapshot_html_none_when_client_closed(sandbox):
    sandbox._client = None
    assert sandbox.snapshot_html("<h1>hi</h1>") is None


def test_local_sandbox_snapshot_html_is_none():
    from deerflow.sandbox.local.local_sandbox import LocalSandbox

    sb = LocalSandbox(id="local:test")
    assert sb.snapshot_html("<h1>hi</h1>") is None
