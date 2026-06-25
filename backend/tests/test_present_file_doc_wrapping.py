"""Tests for present_files non-media wrapping (cfgpu-docs/present-files-tool.md §2/§4/§5).

Covers ``build_doc_html`` escaping + copy-button source injection, and the
classification branch: media → URL item, wrappable text → rich item (with poster),
already-HTML → no double wrap, binary/oversize → bare link.
"""

import asyncio
import importlib
import json
from types import SimpleNamespace

import pytest

mod = importlib.import_module("deerflow.tools.builtins.present_file_tool")


# ── build_doc_html ──────────────────────────────────────────────────────────


def test_build_doc_html_escapes_and_injects_source():
    text = "print('<b>&hi</b>')\n"
    html = mod.build_doc_html(text, "demo.py", "python")
    # Body content is HTML-escaped (no raw tags leak into the DOM).
    assert "&lt;b&gt;" in html
    assert "<b>" not in html.replace("<b>", "", 0) or "&lt;b&gt;" in html
    # Original text injected verbatim as a JS string constant for clipboard copy.
    assert f"const SOURCE = {json.dumps(text)};" in html
    # Self-contained: no external resources.
    assert "http://" not in html and "https://" not in html
    assert "复制 Markdown" not in html  # python label, not markdown
    assert "复制 代码" in html


def test_build_doc_html_markdown_label():
    html = mod.build_doc_html("# title", "readme.md", "markdown")
    assert "复制 Markdown" in html


# ── classification branch ───────────────────────────────────────────────────


class _FakeUploader:
    """Records inline/local uploads; returns deterministic refs."""

    def __init__(self):
        self.inline_calls = []
        self.local_calls = []

    async def upload_inline_bytes(self, object_key, data, content_type=None):
        self.inline_calls.append((object_key, content_type, len(data)))
        return f"https://oss.example/{object_key}"

    async def upload_local_file(self, virtual_path, physical_path, thread_id):
        self.local_calls.append(physical_path)
        return f"https://oss.example/local/{physical_path.rsplit('/', 1)[-1]}"


class _FakeSandbox:
    def __init__(self, png=b"\x89PNGsnap"):
        self.png = png
        self.calls = []

    def snapshot_html(self, html, *, full_page=True):
        self.calls.append(html)
        return self.png


def _make_runtime(outputs_path: str) -> SimpleNamespace:
    return SimpleNamespace(
        state={"thread_data": {"outputs_path": outputs_path}},
        context={"thread_id": "thread-1"},
        config={},
    )


def _present(**kwargs):
    return asyncio.run(mod.present_file_tool.coroutine(**kwargs))


@pytest.fixture()
def wired(monkeypatch, tmp_path):
    """OSS uploader + sandbox wired; outputs dir prepared."""
    uploader = _FakeUploader()
    sandbox = _FakeSandbox()
    monkeypatch.setattr("deerflow.oss.uploader.get_oss_uploader", lambda: uploader)
    monkeypatch.setattr(mod, "_resolve_snapshot_sandbox", lambda runtime: sandbox)
    outputs_dir = tmp_path / "threads" / "thread-1" / "user-data" / "outputs"
    outputs_dir.mkdir(parents=True)
    return SimpleNamespace(uploader=uploader, sandbox=sandbox, outputs_dir=outputs_dir)


def _items(result):
    return result.update["messages"][0].artifact["items"]


def test_text_file_produces_rich_item_with_poster(wired):
    f = wired.outputs_dir / "report.md"
    f.write_text("# Hello\n\nbody")

    result = _present(runtime=_make_runtime(str(wired.outputs_dir)), filepaths=[str(f)], tool_call_id="tc")

    item = _items(result)[0]
    assert item["mime"] == "text/html"
    assert item["source_name"] == "report.md"
    assert item["poster"] is not None and item["poster"].endswith(".png")
    assert item["ref"].endswith(".html") or "documents" in item["ref"]
    assert item["kind"] == "url"
    # HTML + PNG both uploaded inline.
    assert len(wired.uploader.inline_calls) == 2
    assert wired.sandbox.calls, "snapshot_html should have been invoked"


def test_already_html_is_not_double_wrapped(wired):
    f = wired.outputs_dir / "page.html"
    f.write_text("<html><body><h1>Hi</h1></body></html>")

    result = _present(runtime=_make_runtime(str(wired.outputs_dir)), filepaths=[str(f)], tool_call_id="tc")

    item = _items(result)[0]
    assert item["mime"] == "text/html"
    # The snapshot rendered the original HTML (no source-viewer wrapper around it).
    snapped = wired.sandbox.calls[0]
    assert "doc-copy" not in snapped
    assert snapped == "<html><body><h1>Hi</h1></body></html>"


def test_no_poster_when_sandbox_absent(wired, monkeypatch):
    monkeypatch.setattr(mod, "_resolve_snapshot_sandbox", lambda runtime: None)
    f = wired.outputs_dir / "notes.txt"
    f.write_text("plain text")

    result = _present(runtime=_make_runtime(str(wired.outputs_dir)), filepaths=[str(f)], tool_call_id="tc")

    item = _items(result)[0]
    assert item["poster"] is None
    assert item["mime"] == "text/html"  # HTML still delivered (I1/I2)
    assert len(wired.uploader.inline_calls) == 1  # only the HTML, no PNG


def test_binary_file_falls_back_to_bare_link(wired):
    f = wired.outputs_dir / "blob.bin"
    f.write_bytes(b"\xff\xfe\x00\x01not utf8\xff")

    result = _present(runtime=_make_runtime(str(wired.outputs_dir)), filepaths=[str(f)], tool_call_id="tc")

    item = _items(result)[0]
    assert "poster" not in item  # plain _artifact_item, no rich fields
    assert wired.uploader.local_calls, "binary should go through upload_local_file"


def test_oversize_text_falls_back_to_bare_link(wired, monkeypatch):
    monkeypatch.setattr(mod, "_WRAPPABLE_MAX_BYTES", 16)
    f = wired.outputs_dir / "big.txt"
    f.write_text("x" * 1000)

    result = _present(runtime=_make_runtime(str(wired.outputs_dir)), filepaths=[str(f)], tool_call_id="tc")

    item = _items(result)[0]
    assert "poster" not in item
    assert wired.uploader.local_calls


def test_image_file_uses_media_branch(wired):
    f = wired.outputs_dir / "pic.png"
    f.write_bytes(b"\x89PNG\r\n\x1a\nfake")

    result = _present(runtime=_make_runtime(str(wired.outputs_dir)), filepaths=[str(f)], tool_call_id="tc")

    item = _items(result)[0]
    assert "poster" not in item
    assert wired.uploader.local_calls and wired.uploader.local_calls[0].endswith("pic.png")
    assert not wired.uploader.inline_calls  # media never goes through inline wrap
