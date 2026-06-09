"""Core behavior tests for present_files path normalization."""

import asyncio
import importlib
from types import SimpleNamespace

import pytest

present_file_tool_module = importlib.import_module("deerflow.tools.builtins.present_file_tool")


@pytest.fixture(autouse=True)
def _no_oss(monkeypatch):
    """Force the OSS-disabled branch so these path-normalization tests are
    deterministic regardless of test ordering. ``present_files`` lazily imports
    ``get_oss_uploader`` from ``deerflow.oss.uploader``; when a prior test has
    initialized the OSS singleton (real bucket from config) it would otherwise
    upload and return a presigned URL instead of the virtual path under test.
    """
    import deerflow.oss.uploader as oss_uploader_module

    monkeypatch.setattr(oss_uploader_module, "get_oss_uploader", lambda: None)


def _present(**kwargs):
    """Invoke the async present_files tool from sync test code.

    ``present_file_tool`` is decorated ``async def``; ``@tool`` exposes the
    implementation on ``.coroutine`` (``.func`` is ``None`` for async tools).
    """
    return asyncio.run(present_file_tool_module.present_file_tool.coroutine(**kwargs))


def _make_runtime(outputs_path: str) -> SimpleNamespace:
    return SimpleNamespace(
        state={"thread_data": {"outputs_path": outputs_path}},
        context={"thread_id": "thread-1"},
        config={},
    )


def test_present_files_normalizes_host_outputs_path(tmp_path):
    outputs_dir = tmp_path / "threads" / "thread-1" / "user-data" / "outputs"
    outputs_dir.mkdir(parents=True)
    artifact_path = outputs_dir / "report.md"
    artifact_path.write_text("ok")

    result = _present(
        runtime=_make_runtime(str(outputs_dir)),
        filepaths=[str(artifact_path)],
        tool_call_id="tc-1",
    )

    assert result.update["artifacts"] == ["/mnt/user-data/outputs/report.md"]
    assert result.update["messages"][0].content == "Successfully presented files"


def test_present_files_keeps_virtual_outputs_path(tmp_path, monkeypatch):
    outputs_dir = tmp_path / "threads" / "thread-1" / "user-data" / "outputs"
    outputs_dir.mkdir(parents=True)
    artifact_path = outputs_dir / "summary.json"
    artifact_path.write_text("{}")

    monkeypatch.setattr(
        present_file_tool_module,
        "get_paths",
        lambda: SimpleNamespace(resolve_virtual_path=lambda thread_id, path, *, user_id=None: artifact_path),
    )

    result = _present(
        runtime=_make_runtime(str(outputs_dir)),
        filepaths=["/mnt/user-data/outputs/summary.json"],
        tool_call_id="tc-2",
    )

    assert result.update["artifacts"] == ["/mnt/user-data/outputs/summary.json"]


def test_present_files_uses_config_thread_id_when_context_missing(tmp_path, monkeypatch):
    outputs_dir = tmp_path / "threads" / "thread-from-config" / "user-data" / "outputs"
    outputs_dir.mkdir(parents=True)
    artifact_path = outputs_dir / "summary.json"
    artifact_path.write_text("{}")

    monkeypatch.setattr(
        present_file_tool_module,
        "get_paths",
        lambda: SimpleNamespace(resolve_virtual_path=lambda thread_id, path: artifact_path),
    )

    runtime = SimpleNamespace(
        state={"thread_data": {"outputs_path": str(outputs_dir)}},
        context={},
        config={"configurable": {"thread_id": "thread-from-config"}},
    )

    result = _present(
        runtime=runtime,
        filepaths=["/mnt/user-data/outputs/summary.json"],
        tool_call_id="tc-config",
    )

    assert result.update["artifacts"] == ["/mnt/user-data/outputs/summary.json"]
    assert result.update["messages"][0].content == "Successfully presented files"


def test_present_files_resolves_user_bucket_from_runtime_context(tmp_path, monkeypatch):
    """Consumer regression (BUG-008): the user bucket must come from
    runtime.context["user_id"], not the _current_user contextvar.

    In the consumer there is no auth middleware, so get_effective_user_id()
    resolves to "default" while ThreadDataMiddleware/cfgpu write under the real
    user_id ("34"). present_files must resolve the virtual path under "34" so it
    matches the thread's outputs_path; otherwise relative_to() splits the bucket
    and raises "Only files can be presented".
    """
    # Real on-disk layout under the user-34 bucket.
    outputs_dir = tmp_path / "users" / "34" / "threads" / "thread-1" / "user-data" / "outputs"
    outputs_dir.mkdir(parents=True)
    artifact_path = outputs_dir / "doraemon_park.jpg"
    artifact_path.write_bytes(b"jpg")

    captured: dict = {}

    def fake_resolve_virtual_path(thread_id, path, *, user_id=None):
        captured["user_id"] = user_id
        # Resolve into the per-user bucket, mirroring Paths.resolve_virtual_path.
        relative = path.split("/mnt/user-data/", 1)[1]
        return tmp_path / "users" / str(user_id) / "threads" / thread_id / "user-data" / relative

    monkeypatch.setattr(
        present_file_tool_module,
        "get_paths",
        lambda: SimpleNamespace(resolve_virtual_path=fake_resolve_virtual_path),
    )

    # The contextvar is unset in the consumer (get_effective_user_id() -> "default");
    # resolve_runtime_user_id must pick "34" from runtime.context instead.
    runtime = SimpleNamespace(
        state={"thread_data": {"outputs_path": str(outputs_dir)}},
        context={"thread_id": "thread-1", "user_id": "34"},
        config={},
    )

    result = _present(
        runtime=runtime,
        filepaths=["/mnt/user-data/outputs/doraemon_park.jpg"],
        tool_call_id="tc-bucket",
    )

    assert captured["user_id"] == "34"
    assert result.update["artifacts"] == ["/mnt/user-data/outputs/doraemon_park.jpg"]
    assert result.update["messages"][0].content == "Successfully presented files"


def test_present_files_rejects_paths_outside_outputs(tmp_path):
    outputs_dir = tmp_path / "threads" / "thread-1" / "user-data" / "outputs"
    workspace_dir = tmp_path / "threads" / "thread-1" / "user-data" / "workspace"
    outputs_dir.mkdir(parents=True)
    workspace_dir.mkdir(parents=True)
    leaked_path = workspace_dir / "notes.txt"
    leaked_path.write_text("leak")

    result = _present(
        runtime=_make_runtime(str(outputs_dir)),
        filepaths=[str(leaked_path)],
        tool_call_id="tc-3",
    )

    assert "artifacts" not in result.update
    assert result.update["messages"][0].content == f"Error: Only files in /mnt/user-data/outputs can be presented: {leaked_path}"
