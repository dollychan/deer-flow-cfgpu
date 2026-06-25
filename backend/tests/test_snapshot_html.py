"""Tests for the ``Sandbox.snapshot_html`` capability (cfgpu-docs/present-files-tool.md §3).

Covers:
- ``AioSandbox.snapshot_html`` drives the browser create→activate→navigate→screenshot→close
  sequence, joins the streamed PNG bytes, and is fail-open (exception → None).
- ``LocalSandbox.snapshot_html`` (inherited default) is always None — no browser capability.
"""

import base64
from types import SimpleNamespace
from unittest.mock import patch

import pytest


@pytest.fixture()
def sandbox():
    with patch("deerflow.community.aio_sandbox.aio_sandbox.AioSandboxClient"):
        from deerflow.community.aio_sandbox.aio_sandbox import AioSandbox

        return AioSandbox(id="test-sandbox", base_url="http://localhost:8080")


def _wire_browser(sb, *, existing_tabs=0, screenshot_chunks=(b"\x89PNG", b"data")):
    """Wire the mocked agent_sandbox browser namespaces onto the sandbox client."""
    sb._client.browser_tabs.list.return_value = SimpleNamespace(data=[None] * existing_tabs)
    sb._client.browser_tabs.create.return_value = SimpleNamespace(data=None)
    sb._client.browser_page.screenshot.return_value = iter(screenshot_chunks)
    return sb


def test_snapshot_html_drives_browser_sequence(sandbox):
    _wire_browser(sandbox, existing_tabs=2, screenshot_chunks=(b"\x89PNG\r\n", b"\x1a\n"))

    png = sandbox.snapshot_html("<h1>hi</h1>", full_page=True)

    assert png == b"\x89PNG\r\n\x1a\n"
    # New tab is appended at index == prior tab count.
    sandbox._client.browser_tabs.activate.assert_called_once_with(2)
    sandbox._client.browser_tabs.close.assert_called_once_with(2)
    # Navigated to a data: URL carrying the base64 HTML (no disk round-trip).
    nav_kwargs = sandbox._client.browser_page.navigate.call_args.kwargs
    assert nav_kwargs["url"].startswith("data:text/html;base64,")
    decoded = base64.b64decode(nav_kwargs["url"].split(",", 1)[1]).decode()
    assert decoded == "<h1>hi</h1>"
    assert nav_kwargs["wait_until"] == "load"
    sandbox._client.browser_page.screenshot.assert_called_once_with(full_page=True, format="png")


def test_snapshot_html_does_not_take_shell_lock(sandbox):
    """Browser calls must not serialize behind the shell lock (I3)."""
    _wire_browser(sandbox)
    # If snapshot_html grabbed self._lock, acquiring it here first would deadlock;
    # instead we assert the lock is free during the call.
    assert sandbox._lock.acquire(blocking=False)
    try:
        sandbox.snapshot_html("<p>x</p>")
    finally:
        sandbox._lock.release()


def test_snapshot_html_fail_open_on_exception(sandbox):
    _wire_browser(sandbox)
    sandbox._client.browser_page.navigate.side_effect = RuntimeError("boom")

    assert sandbox.snapshot_html("<h1>hi</h1>") is None
    # Tab is still closed on the failure path (finally).
    sandbox._client.browser_tabs.close.assert_called_once_with(0)


def test_snapshot_html_none_when_client_closed(sandbox):
    sandbox._client = None
    assert sandbox.snapshot_html("<h1>hi</h1>") is None


def test_snapshot_html_empty_png_is_none(sandbox):
    _wire_browser(sandbox, screenshot_chunks=())
    assert sandbox.snapshot_html("<h1>hi</h1>") is None


def test_local_sandbox_snapshot_html_is_none(tmp_path):
    from deerflow.sandbox.local.local_sandbox import LocalSandbox

    sb = LocalSandbox(id="local:test")
    assert sb.snapshot_html("<h1>hi</h1>") is None
