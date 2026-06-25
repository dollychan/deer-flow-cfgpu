"""Tests for the ``Sandbox.snapshot_html`` capability (cfgpu-docs/present-files-tool.md §3).

The AIO implementation drives the sandbox's built-in ``chromium-browser`` headless
``--screenshot`` CLI over the shell/file API (the gem runtime exposes no browser HTTP
API), reading the PNG back as base64. Covers:
- write HTML → render → base64-decode → PNG bytes, with temp cleanup,
- fail-open (empty/``Error:``/``(no output)`` chromium output → None; exception → None),
- ``LocalSandbox.snapshot_html`` (inherited default) is always None.
"""

import base64
from unittest.mock import patch

import pytest


@pytest.fixture()
def sandbox():
    with patch("deerflow.community.aio_sandbox.aio_sandbox.AioSandboxClient"):
        from deerflow.community.aio_sandbox.aio_sandbox import AioSandbox

        sb = AioSandbox(id="test-sandbox", base_url="http://localhost:8080")
        # Replace the high-level helpers with spies; snapshot_html composes them.
        sb.write_file = _Spy()
        sb.execute_command = _Spy()
        return sb


class _Spy:
    def __init__(self):
        self.calls = []
        self.side_effect = None
        self.returns = {}

    def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if self.side_effect is not None:
            raise self.side_effect
        cmd = args[0] if args else ""
        for needle, value in self.returns.items():
            if needle in cmd:
                return value
        return "(no output)"


def test_snapshot_html_renders_and_decodes_png(sandbox):
    png_raw = b"\x89PNG\r\n\x1a\nfake-bytes"
    b64 = base64.b64encode(png_raw).decode()
    # The render command contains "chromium-browser ... ; base64 ..."; the rm
    # cleanup command contains "rm -f". Route base64 output to the render call.
    sandbox.execute_command.returns = {"chromium-browser": b64, "rm -f": "(no output)"}

    out = sandbox.snapshot_html("<h1>hi</h1>", full_page=True)

    assert out == png_raw
    # HTML written to a /tmp path inside the sandbox.
    (write_args, _) = sandbox.write_file.calls[0]
    assert write_args[0].startswith("/tmp/snap-") and write_args[0].endswith(".html")
    assert write_args[1] == "<h1>hi</h1>"
    # Render command shape: headless chromium screenshot of the file:// URL.
    render_cmd = sandbox.execute_command.calls[0][0][0]
    assert "chromium-browser --headless=new --no-sandbox" in render_cmd
    assert "--screenshot=/tmp/snap-" in render_cmd
    assert "file:///tmp/snap-" in render_cmd
    assert "base64 -w0" in render_cmd
    # Temp files cleaned up in finally.
    assert any("rm -f" in c[0][0] for c in sandbox.execute_command.calls)


def test_snapshot_html_fold_uses_shorter_window(sandbox):
    sandbox.execute_command.returns = {"chromium-browser": base64.b64encode(b"\x89PNGx").decode()}
    sandbox.snapshot_html("<p>x</p>", full_page=False)
    render_cmd = sandbox.execute_command.calls[0][0][0]
    assert f"--window-size=1280,{sandbox._SNAPSHOT_HEIGHT_FOLD}" in render_cmd


def test_snapshot_html_none_on_empty_output(sandbox):
    sandbox.execute_command.returns = {"chromium-browser": "(no output)"}
    assert sandbox.snapshot_html("<h1>hi</h1>") is None
    assert any("rm -f" in c[0][0] for c in sandbox.execute_command.calls)  # still cleaned up


def test_snapshot_html_none_on_error_string(sandbox):
    sandbox.execute_command.returns = {"chromium-browser": "Error: command failed"}
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
