from __future__ import annotations

import asyncio
import hashlib
import html as html_lib
import json
import logging
from pathlib import Path
from typing import Annotated

from langchain.tools import InjectedToolCallId, tool
from langchain_core.messages import ToolMessage
from langgraph.config import get_config
from langgraph.types import Command

from deerflow.agents.materials.materialize import stage_to_oss
from deerflow.agents.materials.registry import classify_ref
from deerflow.agents.materials.types import Material
from deerflow.config.paths import VIRTUAL_PATH_PREFIX, map_virtual_to_physical
from deerflow.oss.client import get_oss_client
from deerflow.tools.types import Runtime

logger = logging.getLogger(__name__)

OUTPUTS_VIRTUAL_PREFIX = f"{VIRTUAL_PATH_PREFIX}/outputs"


def _presign(object_key: str) -> str:
    """我方 object_key → presigned GET url；OSS 未启用回裸 key 兜底。"""
    oss = get_oss_client()
    return oss.presign(object_key) if oss is not None else object_key


def _get_thread_id(runtime: Runtime) -> str | None:
    """Resolve the current thread id from runtime context or RunnableConfig."""
    thread_id = runtime.context.get("thread_id") if runtime.context else None
    if thread_id:
        return thread_id

    runtime_config = getattr(runtime, "config", None) or {}
    thread_id = runtime_config.get("configurable", {}).get("thread_id")
    if thread_id:
        return thread_id

    try:
        return get_config().get("configurable", {}).get("thread_id")
    except RuntimeError:
        return None


def _normalize_presented_filepath(
    runtime: Runtime,
    filepath: str,
) -> str:
    """Normalize a presented file path to the `/mnt/user-data/outputs/*` contract.

    Accepts either:
    - A virtual sandbox path such as `/mnt/user-data/outputs/report.md`
    - A host-side thread outputs path such as
      `/app/backend/.deer-flow/threads/<thread>/user-data/outputs/report.md`

    Returns:
        The normalized virtual path.

    Raises:
        ValueError: If runtime metadata is missing or the path is outside the
            current thread's outputs directory.
    """
    if runtime.state is None:
        raise ValueError("Thread runtime state is not available")

    thread_id = _get_thread_id(runtime)
    if not thread_id:
        raise ValueError("Thread ID is not available in runtime context or runtime config")

    thread_data = runtime.state.get("thread_data") or {}
    outputs_path = thread_data.get("outputs_path")
    if not outputs_path:
        raise ValueError("Thread outputs path is not available in runtime state")

    outputs_dir = Path(outputs_path).resolve()
    stripped = filepath.lstrip("/")
    virtual_prefix = VIRTUAL_PATH_PREFIX.lstrip("/")

    if stripped == virtual_prefix or stripped.startswith(virtual_prefix + "/"):
        # Decoupled resolution (cfgpu-docs/thread-tenancy.md §4.3 / I3+): map the virtual
        # `/mnt/user-data/<rest>` path straight onto the host user-data dir derived from
        # the thread_data outputs_path (outputs_dir.parent == .../user-data) — never
        # re-resolving a user bucket. ThreadDataMiddleware is the single source of truth
        # for which bucket this thread uses, so present_files inherits it for free and is
        # immune to the tenant model (this also retires the BUG-008 user_id re-resolution
        # fragility). The relative_to(outputs_dir) check below still confines presents to
        # the outputs subtree, so workspace/uploads virtual paths are rejected as before.
        user_data_dir = outputs_dir.parent
        relative = stripped[len(virtual_prefix) :].lstrip("/")
        actual_path = (user_data_dir / relative).resolve()
    else:
        actual_path = Path(filepath).expanduser().resolve()

    try:
        relative_path = actual_path.relative_to(outputs_dir)
    except ValueError as exc:
        raise ValueError(f"Only files in {OUTPUTS_VIRTUAL_PREFIX} can be presented: {filepath}") from exc

    return f"{OUTPUTS_VIRTUAL_PREFIX}/{relative_path.as_posix()}"


def _virtual_to_physical(virtual_path: str, outputs_path: str) -> str:
    """Derive the physical filesystem path from a normalized virtual outputs path."""
    relative = virtual_path[len(OUTPUTS_VIRTUAL_PREFIX) :].lstrip("/")
    return str(Path(outputs_path).resolve() / relative)


def _artifact_item(ref: str, size: int | None = None) -> dict:
    """Build an artifact item, classifying ref as a fetchable URL or virtual path.

    `kind="url"` (OSS presigned link) is fetched directly by the client;
    `kind="path"` (virtual outputs path) is fetched via the artifacts API route.

    `size` is the presented file's byte count (measured at upload time); `None` when the
    size is unknown, so every emitted item carries the field for a uniform client contract.
    """
    kind = "url" if ref.startswith(("http://", "https://")) else "path"
    return {"ref": ref, "kind": kind, "expires_at": None, "size": size}


def _path_size(physical_path: str) -> int | None:
    """Byte count of a local file for an artifact item; None when unavailable (missing/perm)."""
    try:
        return Path(physical_path).stat().st_size
    except OSError:
        return None


# Non-media text files larger than this are not wrapped (treated as opaque
# downloads) so we never pull a huge file into memory just to render it.
_WRAPPABLE_MAX_BYTES = 2 * 1024 * 1024  # 2 MiB

# Lightweight language hint for the wrapped source view, by file suffix.
_LANG_BY_SUFFIX: dict[str, str] = {
    ".py": "python",
    ".md": "markdown",
    ".markdown": "markdown",
    ".json": "json",
    ".yaml": "yaml",
    ".yml": "yaml",
    ".toml": "toml",
    ".txt": "text",
    ".log": "text",
    ".csv": "text",
    ".js": "javascript",
    ".mjs": "javascript",
    ".ts": "typescript",
    ".tsx": "typescript",
    ".css": "css",
    ".sh": "bash",
    ".sql": "sql",
    ".xml": "xml",
}


def _lang_for(filename: str) -> str:
    return _LANG_BY_SUFFIX.get(Path(filename).suffix.lower(), "text")


def _is_html(filename: str) -> bool:
    return Path(filename).suffix.lower() in (".html", ".htm")


def _is_wrappable_text(filename: str) -> bool:
    """True for files delivered as a source-view HTML doc (small UTF-8 text or HTML).

    Everything else — binaries (pdf/pptx/docx/xlsx/zip…) and unknown types — is
    delivered through the iframe shell instead (preview + download button, §6.3),
    so we never read a binary into memory just to discover it is not text. Detected
    by suffix (not mimetypes) to avoid the blocking mimetypes.init() on first call,
    mirroring _is_html.
    """
    return _is_html(filename) or Path(filename).suffix.lower() in _LANG_BY_SUFFIX


def _inline_key(thread_id: str, category: str, data: bytes, name: str) -> str:
    """Content-hash-derived object key, so re-presenting the same file is idempotent.

    Mirrors ``_filename_from_url`` in oss/uploader.py: ``<sha1(data)[:8]>-<name>``
    keeps distinct payloads apart while making a repeat present deterministic.
    """
    digest = hashlib.sha1(data).hexdigest()[:8]
    return f"agent-artifacts/{thread_id}/{category}/{digest}-{name}"


_DOC_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>
  * {{ box-sizing: border-box; }}
  body {{ margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #f5f5f7; }}
  .doc-card {{ max-width: 920px; margin: 24px auto; border-radius: 12px; overflow: hidden;
              box-shadow: 0 1px 4px rgba(0,0,0,.12); background: #1e1e2e; }}
  .doc-bar {{ display: flex; align-items: center; justify-content: space-between;
             padding: 10px 16px; background: #2a2a3c; color: #e6e6e6; font-size: 13px; }}
  .doc-name {{ font-weight: 600; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
  .doc-copy {{ border: 1px solid #4a4a60; background: #34344a; color: #e6e6e6; cursor: pointer;
              border-radius: 6px; padding: 4px 12px; font-size: 12px; flex: none; }}
  .doc-copy:hover {{ background: #41415a; }}
  .doc-copy.copied {{ background: #2e7d4f; border-color: #2e7d4f; }}
  pre {{ margin: 0; padding: 16px; max-height: 640px; overflow: auto; color: #d4d4d4;
        font-family: "SF Mono", Menlo, Consolas, monospace; font-size: 12.5px; line-height: 1.55;
        white-space: pre; tab-size: 4; }}
</style>
</head>
<body>
  <div class="doc-card">
    <div class="doc-bar">
      <span class="doc-name">{name}</span>
      <button class="doc-copy" id="copyBtn">复制{lang_label}</button>
    </div>
    <pre id="src">{escaped}</pre>
  </div>
  <script>
    const SOURCE = {source_json};
    const btn = document.getElementById('copyBtn');
    btn.addEventListener('click', async () => {{
      try {{
        await navigator.clipboard.writeText(SOURCE);
        btn.textContent = '已复制';
        btn.classList.add('copied');
        setTimeout(() => {{ btn.textContent = '复制{lang_label}'; btn.classList.remove('copied'); }}, 1500);
      }} catch (e) {{ btn.textContent = '复制失败'; }}
    }});
  </script>
</body>
</html>"""


def build_doc_html(text: str, filename: str, lang: str) -> str:
    """Wrap raw source text in a self-contained source-viewer HTML.

    Fully inline (no external assets) so it renders identically offline, in the
    snapshot browser, and in the client's sandboxed iframe. The original text is
    injected as a JS string constant for the copy button (``navigator.clipboard``)
    so it is byte-exact, not reconstructed from the escaped DOM.
    """
    label_by_lang = {"markdown": " Markdown", "python": " 代码", "json": " JSON"}
    lang_label = label_by_lang.get(lang, "")
    return _DOC_HTML_TEMPLATE.format(
        title=html_lib.escape(filename),
        name=html_lib.escape(filename),
        lang_label=lang_label,
        escaped=html_lib.escape(text),
        source_json=json.dumps(text),
    )


_IFRAME_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>
  * {{ box-sizing: border-box; }}
  html, body {{ margin: 0; height: 100%; }}
  body {{ display: flex; flex-direction: column; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #f5f5f7; }}
  .doc-bar {{ display: flex; align-items: center; justify-content: space-between;
             padding: 10px 16px; background: #2a2a3c; color: #e6e6e6; font-size: 13px; flex: none; }}
  .doc-name {{ font-weight: 600; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
  .doc-dl {{ color: #9ecbff; text-decoration: none; flex: none; margin-left: 12px; }}
  iframe {{ flex: 1 1 auto; width: 100%; min-height: 0; border: 0; background: #fff; }}
</style>
</head>
<body>
  <div class="doc-bar">
    <span class="doc-name">{name}</span>
    <a class="doc-dl" href="{src}" download>下载原文件</a>
  </div>
  <iframe src="{src}" title="{name}"></iframe>
</body>
</html>"""


def build_iframe_html(file_url: str, filename: str) -> str:
    """Wrap any non-text binary in a self-contained <iframe> shell + download button.

    The original file is referenced by ``file_url`` (a presigned OSS URL) for both the
    iframe ``src`` (preview) and the download link — it is **never** inlined, so the
    shell is a stable content-hash OSS object while its iframe ``src`` expires with the
    presigned URL. That lifetime mismatch is the accepted debt (cfgpu-docs §6.3 / D8 /
    I8): the shell still opens after expiry but the embedded preview goes blank/403.

    The iframe previews whatever the browser can render natively (pdf/text/img/svg…);
    for formats it cannot (pptx/docx/xlsx/zip…) the iframe stays blank and the always-
    present download button is the fallback (the file is never converted, I8).
    """
    safe_url = html_lib.escape(file_url, quote=True)
    return _IFRAME_HTML_TEMPLATE.format(
        title=html_lib.escape(filename),
        name=html_lib.escape(filename),
        src=safe_url,
    )


def _read_bytes(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()


def _resolve_snapshot_sandbox(runtime: Runtime):
    """Best-effort resolve the thread's Sandbox for snapshot rendering.

    present_files runs in the consumer/gateway host process but holds the
    runtime, so it can reach the per-thread Sandbox object. Resolution failures
    (no sandbox state, provider miss) are non-fatal: snapshots are optional, so
    we return ``None`` and the caller simply omits the poster (I1/I2).
    """
    try:
        from deerflow.sandbox.tools import sandbox_from_runtime

        return sandbox_from_runtime(runtime)
    except Exception as exc:
        # Non-fatal, but log it: a silently-missing sandbox is the most common
        # reason a poster comes back null, and is otherwise invisible.
        logger.warning("present_files: no sandbox for snapshot (%s) — poster will be omitted", exc)
        return None


async def _build_rich_item(
    uploader,
    sandbox,
    thread_id: str,
    filename: str,
    raw: bytes,
) -> tuple[str, dict] | None:
    """Wrap a non-media text file as an HTML doc + PNG snapshot → a rich artifact item.

    Returns ``(ref, item)`` on success, or ``None`` when the file is not wrappable
    (binary / too large) so the caller falls back to a bare download link. The
    snapshot is best-effort: if the sandbox has no browser (LocalSandbox → ``None``)
    or rendering fails, ``ref`` is ``None`` while ``html`` still points at the HTML,
    so the file is still delivered without a poster (I1).

    ``size`` describes what ``ref`` points to — here the snapshot PNG — so it is the
    poster's byte count (``None`` when there is no snapshot), NOT the source file.
    """
    if len(raw) > _WRAPPABLE_MAX_BYTES:
        return None
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return None  # binary (pdf/zip/...) → caller retains the bare-link path

    if _is_html(filename):
        # Already HTML: deliver it as-is (user wants the rendered page, not a
        # source view), and snapshot the page itself (D6).
        html_doc = text
        html_name = filename
    else:
        html_doc = build_doc_html(text, filename, _lang_for(filename))
        html_name = f"{filename}.html"

    html_bytes = html_doc.encode("utf-8")
    html_key = _inline_key(thread_id, "documents", html_bytes, html_name)
    html_ref = await uploader.upload_inline_bytes(html_key, html_bytes, "text/html")

    snapshot_ref = None
    snapshot_size: int | None = None
    if sandbox is not None:
        png = await asyncio.to_thread(sandbox.snapshot_html, html_doc)
        if png:
            snapshot_size = len(png)
            png_key = _inline_key(thread_id, "images", png, f"{Path(filename).stem}.png")
            snapshot_ref = await uploader.upload_inline_bytes(png_key, png, "image/png")
        else:
            # snapshot_html already logged the underlying cause (fail-open);
            # surface that this file ends up snapshotless so it is greppable.
            logger.warning("present_files: snapshot returned no PNG for %s — delivering without snapshot", filename)
    else:
        logger.info("present_files: no snapshot sandbox for %s — delivering without snapshot", filename)

    # ``ref`` is the snapshot PNG (what the client renders); ``html`` always carries
    # the source-viewer HTML's OSS path. When the snapshot is unavailable (no sandbox
    # / render failure) ``ref`` is ``None`` while ``html`` still points at the HTML, so
    # the file is delivered without a poster (I1). ``kind``/``expires_at`` classify off
    # ``html_ref`` (png and html share the same OSS scheme) so they hold even when the
    # snapshot is absent. ``size`` tracks ``ref`` (the poster PNG), so it is ``None``
    # when there is no snapshot.
    item = {
        **_artifact_item(html_ref, snapshot_size),
        "ref": snapshot_ref,
        "mime": "text/html",
        "html": html_ref,
        "source_name": filename,
    }
    return html_ref, item


async def _build_iframe_item(
    uploader,
    sandbox,
    thread_id: str,
    filename: str,
    file_url: str,
) -> tuple[str, dict]:
    """Wrap any non-text binary as an <iframe> shell + snapshot → rich item.

    ``file_url`` is the original file's OSS reference (already uploaded), used as both
    the iframe ``src`` (preview) and the ``download`` link. Unlike ``_build_rich_item``
    the original is **not** read into memory or inlined — large files stay on disk and
    the shell references them by URL (so the shell outlives its presigned src; accepted
    debt, cfgpu-docs §6.3 / D8 / I8). The snapshot is best-effort: for formats headless
    chromium cannot render (embedded PDF, or non-renderable Office/zip), ``ref`` (poster)
    is ``None`` while ``html`` still carries the shell — the file is delivered with its
    download button and no poster (I1/I8).

    ``size`` describes what ``ref`` points to — the snapshot PNG — so it is the poster's
    byte count (``None`` when there is no snapshot), NOT the original file (see ``download``).
    """
    html_doc = build_iframe_html(file_url, filename)
    html_bytes = html_doc.encode("utf-8")
    html_key = _inline_key(thread_id, "documents", html_bytes, f"{filename}.html")
    html_ref = await uploader.upload_inline_bytes(html_key, html_bytes, "text/html")

    snapshot_ref = None
    snapshot_size: int | None = None
    if sandbox is not None:
        png = await asyncio.to_thread(sandbox.snapshot_html, html_doc)
        if png:
            snapshot_size = len(png)
            png_key = _inline_key(thread_id, "images", png, f"{Path(filename).stem}.png")
            snapshot_ref = await uploader.upload_inline_bytes(png_key, png, "image/png")
        else:
            logger.warning("present_files: snapshot returned no PNG for %s — delivering iframe without snapshot", filename)
    else:
        logger.info("present_files: no snapshot sandbox for %s — delivering iframe without snapshot", filename)

    # Same rich-item shape as the text path (``ref``=poster png, ``html``=shell), plus
    # ``download`` carrying the original file URL so the client always has the as-is
    # file even when the preview is unavailable. ``kind``/``expires_at`` classify off
    # ``html_ref`` (poster and shell share the OSS scheme), holding when poster is None.
    # ``size`` tracks ``ref`` (the poster PNG), so it is ``None`` when there is no snapshot.
    item = {
        **_artifact_item(html_ref, snapshot_size),
        "ref": snapshot_ref,
        "mime": "text/html",
        "html": html_ref,
        "download": file_url,
        "source_name": filename,
    }
    return html_ref, item


async def _present_materials(
    runtime: Runtime,
    ids: list[str],
    materials: dict[str, Material],
) -> tuple[list[tuple[str, int | None]], dict[str, Material]]:
    """Stage each material id to durable OSS + mark it as a deliverable (display=true).

    present = ``stage`` 原语 + ``display=true``（§4.8.3/D16）：确保 oss_path（durable）再标交付物投影。
    任意 material id 皆可展示（generate 产物 / 第三方 / 本地）。返回 ((presigned ref, size) 列表,
    materials update)——``size`` 取自 stage 后 material 的 ``size`` 字段（rehost/upload 时算入），
    未落盘素材（第三方 global_url 从未下载）为 None。
    """
    thread_id = _get_thread_id(runtime) or "unknown"
    thread_data = (runtime.state or {}).get("thread_data") or {}
    outputs_path = thread_data.get("outputs_path", "")

    def to_physical(vpath: str) -> str:
        return map_virtual_to_physical(vpath, outputs_path)

    working = dict(materials)
    update: dict[str, Material] = {}
    refs: list[tuple[str, int | None]] = []
    for mid in ids:
        outcome = await stage_to_oss(working, mid, thread_id=str(thread_id), to_physical=to_physical, display=True)
        # 合并 stage 升级 + 确保 display=true（oss_path 已持久时 outcome.update 为空，须显式置）。
        ent: dict = dict(outcome.update.get(mid) or {"id": mid})
        ent["display"] = True
        update[mid] = ent  # type: ignore[assignment]
        working[mid] = {**working.get(mid, {}), **ent}  # type: ignore[typeddict-item]
        refs.append((_presign(outcome.ref or working[mid].get("ref", "")), working[mid].get("size")))
    return refs, update


@tool("present_files", parse_docstring=True)
async def present_file_tool(
    runtime: Runtime,
    filepaths: list[str],
    tool_call_id: Annotated[str, InjectedToolCallId],
) -> Command:
    """Make files or materials visible to the user for viewing and rendering in the client.

    When to use the present_files tool:

    - Making any file or material available for the user to view, download, or interact with
    - Presenting multiple related deliverables at once
    - After creating files that should be presented to the user

    When NOT to use the present_files tool:
    - When you only need to read file contents for your own processing
    - For temporary or intermediate files not meant for user viewing

    Notes:
    - You may pass either a local file path under `/mnt/user-data/outputs`, OR a material id
      (e.g. `m7`) for an already-registered material (a generated image/video, or anything you
      registered with `register_material`). Material ids are uploaded to durable storage and
      marked as deliverables automatically.
    - This tool can be safely called in parallel with other tools. State updates are handled by a reducer to prevent conflicts.

    Args:
        filepaths: List of absolute file paths in `/mnt/user-data/outputs` and/or material ids to present to the user.
    """
    state = runtime.state or {}
    materials = state.get("materials") or {}
    id_entries = [e for e in filepaths if isinstance(e, str) and e in materials]
    path_entries = [e for e in filepaths if e not in materials]

    artifacts: list[str] = []
    items: list[dict] = []
    materials_update: dict[str, Material] = {}

    # --- material ids: stage to durable OSS + display=true (§4.8.3) ---
    if id_entries:
        try:
            refs, materials_update = await _present_materials(runtime, id_entries, materials)
        except Exception as exc:  # noqa: BLE001 — stage 失败回 error，不阻断 run
            logger.warning("present_files: failed to stage material(s) %s (%s)", id_entries, exc)
            return Command(update={"messages": [ToolMessage(f"Error: failed to present material(s): {exc}", tool_call_id=tool_call_id, status="error")]})
        artifacts.extend(r for r, _ in refs)
        items.extend(_artifact_item(r, size) for r, size in refs)

    # --- local paths: existing behaviour (artifacts channel) ---
    if path_entries:
        try:
            normalized_paths = [_normalize_presented_filepath(runtime, fp) for fp in path_entries]
        except ValueError as exc:
            return Command(update={"messages": [ToolMessage(f"Error: {exc}", tool_call_id=tool_call_id)]})

        from deerflow.oss.uploader import _infer_category, get_oss_uploader

        thread_data = state.get("thread_data") or {}
        outputs_path = thread_data.get("outputs_path", "")

        uploader = get_oss_uploader()
        if uploader is None:
            # OSS not configured: original behaviour — store virtual paths directly.
            # size still measurable from the local file the virtual path maps to.
            artifacts.extend(normalized_paths)
            items.extend(_artifact_item(p, _path_size(_virtual_to_physical(p, outputs_path))) for p in normalized_paths)
        else:
            thread_id = _get_thread_id(runtime) or "unknown"
            # Resolve the sandbox once for snapshot rendering; tolerate its
            # absence (e.g. LocalSandbox / not initialized) → no poster (I1/I2).
            sandbox = _resolve_snapshot_sandbox(runtime)
            for vpath in normalized_paths:
                try:
                    physical = _virtual_to_physical(vpath, outputs_path)
                    size = _path_size(physical)
                    name = Path(physical).name
                    category = _infer_category(name)
                    if category in ("images", "videos", "audios"):
                        # Media: existing behaviour — single URL artifact item.
                        url = await uploader.upload_local_file(vpath, physical, thread_id)
                        artifacts.append(url)
                        items.append(_artifact_item(url, size))
                        continue
                    # Non-media. Small UTF-8 text / HTML → source-view HTML doc + snapshot
                    # poster. We only read bytes into memory for plausibly-text, in-limit
                    # files; binaries skip the read and fall through to the iframe shell. The
                    # item ``size`` tracks its ``ref`` (the poster), computed inside the builder.
                    rich = None
                    if _is_wrappable_text(name) and (size is None or size <= _WRAPPABLE_MAX_BYTES):
                        raw = await asyncio.to_thread(_read_bytes, physical)
                        rich = await _build_rich_item(uploader, sandbox, thread_id, name, raw)
                    if rich is not None:
                        ref, item = rich
                        artifacts.append(ref)
                        items.append(item)
                        continue
                    # Everything else (binary / oversized / wrap failed): upload the original
                    # as-is (never converted, I8) and wrap it in an <iframe> shell + download
                    # button (§6.3). The iframe previews whatever the browser renders natively
                    # (pdf/text/img…); for formats it cannot (pptx/docx/xlsx…) the always-present
                    # download button is the fallback. No bytes are read into memory here; the
                    # item ``size`` tracks its ``ref`` (the snapshot poster), not the original —
                    # which stays reachable via ``download``.
                    file_ref = await uploader.upload_local_file(vpath, physical, thread_id)
                    # The shell HTML embeds this URL as BOTH the <iframe src> and the download
                    # link, so a browser resolves it relative to the shell's own OSS URL — it MUST
                    # be an absolute presigned URL. Reuse the materials out-gate resolve
                    # (cfgpu-docs/materials.md §4.3): classify the upload ref to its object_key
                    # (oss_path) and presign fresh, exactly like MaterialResolve. This covers both
                    # presigned_url=false (bare object key → presign) and presigned_url=true
                    # (presigned URL → strip signature back to object_key → re-presign fresh). A
                    # third-party URL (ref_type != oss_path) passes through untouched. Without this
                    # a bare relative key resolves against the shell's `.../documents/` dir into a
                    # broken doubled `.../documents/agent-artifacts/.../files/...` path.
                    ref_type, resolved = classify_ref(file_ref)
                    file_url = _presign(resolved) if ref_type == "oss_path" else file_ref
                    ref, item = await _build_iframe_item(uploader, sandbox, thread_id, name, file_url)
                    artifacts.append(ref)
                    items.append(item)
                except Exception:
                    logger.warning("present_files: OSS upload failed for %s, falling back to local path", vpath)
                    artifacts.append(vpath)
                    items.append(_artifact_item(vpath, _path_size(_virtual_to_physical(vpath, outputs_path))))

    update: dict = {
        "artifacts": artifacts,
        "messages": [ToolMessage("Successfully presented files", tool_call_id=tool_call_id, artifact={"items": items})],
    }
    if materials_update:
        update["materials"] = materials_update
    return Command(update=update)


# Client-facing visibility for MessageStreamMiddleware: presented files are final
# deliverables, emitted as an `artifact` event (carrying ToolMessage.artifact).
present_file_tool.metadata = {"visibility": "artifact"}
