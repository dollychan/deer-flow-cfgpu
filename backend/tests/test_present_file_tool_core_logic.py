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
    # 每个下行 item 带 size（本地文件字节数），即便 OSS 关闭仍从虚拟路径映射的本地文件量得
    items = result.update["messages"][0].artifact["items"]
    assert items[0]["size"] == 2  # "ok" == 2 bytes


def test_present_files_keeps_virtual_outputs_path(tmp_path):
    """A virtual outputs path is mapped onto the host user-data dir derived purely from
    thread_data.outputs_path (thread-tenancy.md §4.3) — no get_paths / user_id involved."""
    outputs_dir = tmp_path / "threads" / "thread-1" / "user-data" / "outputs"
    outputs_dir.mkdir(parents=True)
    (outputs_dir / "summary.json").write_text("{}")

    result = _present(
        runtime=_make_runtime(str(outputs_dir)),
        filepaths=["/mnt/user-data/outputs/summary.json"],
        tool_call_id="tc-2",
    )

    assert result.update["artifacts"] == ["/mnt/user-data/outputs/summary.json"]


def test_present_files_uses_config_thread_id_when_context_missing(tmp_path):
    outputs_dir = tmp_path / "threads" / "thread-from-config" / "user-data" / "outputs"
    outputs_dir.mkdir(parents=True)
    (outputs_dir / "summary.json").write_text("{}")

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


def test_present_files_is_decoupled_from_user_id_in_context(tmp_path):
    """Thread-only tenancy (thread-tenancy.md §4.3 / I3+): present_files resolves the
    virtual path purely from thread_data.outputs_path with ZERO user_id, even when
    runtime.context carries a user_id. This retires the BUG-008 per-user-bucket
    re-resolution: the bucket is whatever ThreadDataMiddleware published, full stop.
    """
    # Disk layout is thread-only; the file lives directly under the thread outputs dir.
    outputs_dir = tmp_path / "threads" / "thread-1" / "user-data" / "outputs"
    outputs_dir.mkdir(parents=True)
    (outputs_dir / "doraemon_park.jpg").write_bytes(b"jpg")

    # A user_id is present in context but MUST NOT influence resolution.
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

    # Presented successfully against the thread-only bucket (no "users/34/" path involved).
    assert result.update["artifacts"] == ["/mnt/user-data/outputs/doraemon_park.jpg"]
    assert result.update["messages"][0].content == "Successfully presented files"
    assert result.update["artifacts"] == ["/mnt/user-data/outputs/doraemon_park.jpg"]
    assert result.update["messages"][0].content == "Successfully presented files"


def test_present_material_id_stages_and_marks_display(tmp_path, monkeypatch):
    """present_files 吃 material id（§4.8.3）：local 素材 → stage 上传 oss_path + display=true。"""
    import deerflow.agents.materials.materialize as mz

    class _Uploader:
        def __init__(self):
            self.calls = []

        async def upload_local_file(self, virtual_path, physical_path, thread_id):
            self.calls.append(virtual_path)
            return f"agent-artifacts/{thread_id}/files/{virtual_path.rsplit('/', 1)[-1]}"

    up = _Uploader()
    monkeypatch.setattr(mz, "get_oss_uploader", lambda: up)
    monkeypatch.setattr(present_file_tool_module, "get_oss_client", lambda: None)  # presign 回裸 key

    outputs_dir = tmp_path / "threads" / "thread-1" / "user-data" / "outputs"
    outputs_dir.mkdir(parents=True)
    (outputs_dir / "a.png").write_bytes(b"z" * 77)  # 真文件 → stage 记 size=77
    materials = {"m1": {"id": "m1", "kind": "image", "origin": "local", "ref_type": "local", "local_path": "/mnt/user-data/outputs/a.png"}}
    runtime = SimpleNamespace(
        state={"thread_data": {"outputs_path": str(outputs_dir)}, "materials": materials},
        context={"thread_id": "thread-1"},
        config={},
    )

    result = _present(runtime=runtime, filepaths=["m1"], tool_call_id="tc-mat")

    assert up.calls == ["/mnt/user-data/outputs/a.png"]  # local → 上传
    # 升级 oss_path + display=true 进 materials 更新
    assert result.update["materials"]["m1"]["ref_type"] == "oss_path"
    assert result.update["materials"]["m1"]["display"] is True
    # 交付物 ref（presign 回裸 key，OSS off）进 artifacts
    assert result.update["artifacts"] == ["agent-artifacts/thread-1/files/a.png"]
    # material 记入 size，下行 item 带出（stage 上传时量得）
    assert result.update["materials"]["m1"]["size"] == 77
    assert result.update["messages"][0].artifact["items"][0]["size"] == 77


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
