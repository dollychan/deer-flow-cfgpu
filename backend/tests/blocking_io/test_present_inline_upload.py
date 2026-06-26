"""Regression anchor: present_files non-media wrapping must not block the event loop.

The wrapping path (cfgpu-docs/present-files-tool.md §4, I5) does blocking IO that
must be offloaded:

- rendering the snapshot (``sandbox.snapshot_html`` via ``asyncio.to_thread``),
- the OSS ``put_object`` (``OSSUploader.upload_inline_bytes`` via ``run_in_executor``),
- reading the local file (``_read_bytes`` via ``asyncio.to_thread``).

This drives the new wrapping surface (``_build_rich_item`` plus the ``_read_bytes``
offload that precedes it in the tool loop) directly under the strict Blockbuster
gate. If any of those calls regress onto the event loop, Blockbuster raises and
this test fails.

It deliberately does *not* drive the full ``present_file_tool.coroutine``: the
pre-existing path-normalization (``Path.resolve()`` → ``os.path.realpath``) blocks
the loop, but that is original deerflow code outside this feature's scope. The
anchor targets exactly the newly added blocking surfaces.
"""

from __future__ import annotations

import asyncio
import importlib
import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.asyncio

mod = importlib.import_module("deerflow.tools.builtins.present_file_tool")


class _BlockingUploader:
    """Imitates OSSUploader: the blocking put is offloaded to a thread (I5)."""

    def __init__(self):
        self.inline_calls = 0

    async def upload_inline_bytes(self, object_key, data, content_type=None):
        self.inline_calls += 1
        loop = asyncio.get_event_loop()
        # A real blocking sink (time.sleep) offloaded exactly like put_object.
        await loop.run_in_executor(None, time.sleep, 0.01)
        return f"https://oss.example/{object_key}"


class _BlockingSandbox:
    """Imitates AioSandbox.snapshot_html: a blocking render returning PNG bytes."""

    def snapshot_html(self, html, *, full_page=True):
        time.sleep(0.01)  # blocking — must be reached only via asyncio.to_thread
        return b"\x89PNGsnapshot"


async def test_present_wrapping_does_not_block_event_loop(tmp_path: Path) -> None:
    report = tmp_path / "report.md"
    await asyncio.to_thread(report.write_text, "# Title\n\nbody text", "utf-8")

    uploader = _BlockingUploader()
    sandbox = _BlockingSandbox()

    # Mirror the tool loop: read offloaded, then wrap (snapshot + uploads offloaded).
    raw = await asyncio.to_thread(mod._read_bytes, str(report))
    result = await mod._build_rich_item(uploader, sandbox, "thread-1", "report.md", raw)

    assert result is not None
    ref, item = result
    assert item["mime"] == "text/html"
    assert item["ref"] is not None  # poster PNG (snapshot)
    assert uploader.inline_calls == 2  # HTML + PNG


async def test_present_iframe_does_not_block_event_loop(tmp_path: Path) -> None:
    """The PDF iframe-shell path (§6.3) offloads the same blocking sinks (I5/I8)."""
    uploader = _BlockingUploader()
    sandbox = _BlockingSandbox()

    # No bytes are read for the iframe path; only the shell upload + snapshot run.
    ref, item = await mod._build_iframe_item(uploader, sandbox, "thread-1", "report.pdf", "https://oss.example/local/report.pdf")

    assert item["mime"] == "text/html"
    assert item["download"] == "https://oss.example/local/report.pdf"
    assert item["ref"] is not None  # poster PNG (snapshot)
    assert uploader.inline_calls == 2  # shell HTML + PNG
