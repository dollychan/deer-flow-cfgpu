"""P2 — per-thread mount isolation guard (D3, the isolation linchpin).

The AIO sandbox is the director's blast radius: whatever the container can read is
whatever we bind-mount into it. The provider is *already* correct — ``_get_thread_mounts``
mounts only the current thread's ``workspace/uploads/outputs`` (RW) + ``acp-workspace``
(RO), never ``/root/fs`` wholesale — so P2 adds **no provider code**. It adds guards that
lock that behaviour against regression, framed at the consumer's contract:

  - Thread-only tenancy (thread-tenancy.md D1/D3): mount sources are keyed by ``thread_id``
    alone (``threads/{tid}/...``), never per-user. These tests drive the **real** user
    ContextVar (they do *not* monkeypatch ``get_effective_user_id``) to prove the mount
    path is *independent* of the effective user: two users on the same thread_id get the
    identical mount source (one shared disk, D4). A regression that re-introduces a
    ``/users/{uid}/`` bucket — or widens the mount to a shared root — fails here.
  - Config hygiene: the shipped ``SandboxConfig`` must not bind a wide host root (the
    container's bash can read every mounted byte) and must not inject secrets via
    ``sandbox.environment`` (readable by the same bash). These are expressed as reusable
    predicates exercised against clean and poisoned configs.

See ``cfgpu-docs/thread-tenancy.md`` (D1/D3/D4) and ``aio-localbackend-sandbox.md`` (D3 / P2).
"""

from __future__ import annotations

import importlib
from types import SimpleNamespace

from deerflow.config.paths import Paths
from deerflow.config.sandbox_config import SandboxConfig, VolumeMountConfig
from deerflow.runtime.user_context import reset_current_user, set_current_user

AIO_MOD = "deerflow.community.aio_sandbox.aio_sandbox_provider"

# Container-side mount targets the provider is allowed to expose for a thread.
ALLOWED_CONTAINER_TARGETS = {
    "/mnt/user-data/workspace",
    "/mnt/user-data/uploads",
    "/mnt/user-data/outputs",
    "/mnt/acp-workspace",
}


def _thread_mounts_for(tmp_path, monkeypatch, *, user_id: str, thread_id: str):
    """Resolve ``_get_thread_mounts`` with the real user ContextVar set.

    Only ``get_paths`` is redirected (to a tmp base_dir so directory creation is
    sandboxed); the user_id is set on the genuine ContextVar so the test proves the mount
    path is *independent* of it (thread-only tenancy, thread-tenancy.md §4.1).
    """
    aio_mod = importlib.import_module(AIO_MOD)
    monkeypatch.setattr(aio_mod, "get_paths", lambda: Paths(base_dir=tmp_path))

    token = set_current_user(SimpleNamespace(id=user_id))
    try:
        return aio_mod.AioSandboxProvider._get_thread_mounts(thread_id)
    finally:
        reset_current_user(token)


# ── mount isolation (thread-only; user ContextVar must NOT affect the path) ─────


def test_mounts_only_expose_the_current_thread_subtree(tmp_path, monkeypatch):
    """(a) Every mount source lives under *this* thread's directory — nothing broader."""
    mounts = _thread_mounts_for(tmp_path, monkeypatch, user_id="userA", thread_id="t-1")

    # Targets are exactly the four per-thread mounts, no surprise extras.
    targets = {container_path for _, container_path, _ in mounts}
    assert targets == ALLOWED_CONTAINER_TARGETS

    thread_root = str(tmp_path / "threads" / "t-1")
    for host_path, container_path, _ in mounts:
        assert host_path.startswith(thread_root), (
            f"mount {container_path} escapes the thread subtree: {host_path}"
        )


def test_mount_paths_carry_no_user_bucket(tmp_path, monkeypatch):
    """(b) No mount host path is under a ``/users/{uid}/`` bucket (thread-only, D2)."""
    mounts = _thread_mounts_for(tmp_path, monkeypatch, user_id="userA", thread_id="t-1")

    for host_path, container_path, _ in mounts:
        normalized = host_path.replace("\\", "/")
        assert "/users/" not in normalized, (
            f"mount {container_path} leaked a per-user bucket: {host_path}"
        )
        assert "/threads/t-1/" in normalized, (
            f"mount {container_path} is not under the thread bucket: {host_path}"
        )


def test_mounts_never_expose_a_shared_root(tmp_path, monkeypatch):
    """(c) No mount source is the DEER_FLOW_HOME root or the bare ``threads`` dir.

    Mounting a shared root would let the container read every thread's data — the exact
    leak D3 exists to prevent. Per-thread subtrees are the narrowest correct boundary.
    """
    mounts = _thread_mounts_for(tmp_path, monkeypatch, user_id="userA", thread_id="t-1")

    base_root = str(tmp_path)
    threads_root = str(tmp_path / "threads")
    for host_path, container_path, _ in mounts:
        normalized = host_path.rstrip("/\\")
        assert normalized not in {base_root, threads_root}, (
            f"mount {container_path} exposes a shared root: {host_path}"
        )


def test_same_thread_two_users_share_identical_mounts(tmp_path, monkeypatch):
    """(d) Same thread_id under two users yields IDENTICAL host paths (shared disk, D4).

    This is the inversion of the retired D11 per-user isolation: multi-user sharing of one
    thread's materials is the intended behaviour, kept concurrency-safe by the thread_id
    serial claim lock (thread-tenancy.md §2).
    """
    a_mounts = _thread_mounts_for(tmp_path, monkeypatch, user_id="userA", thread_id="shared-tid")
    b_mounts = _thread_mounts_for(tmp_path, monkeypatch, user_id="userB", thread_id="shared-tid")

    assert a_mounts == b_mounts, "two users on one thread must mount the same shared disk"
    for host_path, _, _ in a_mounts:
        assert "/users/" not in host_path.replace("\\", "/")


def test_acp_workspace_is_read_only(tmp_path, monkeypatch):
    """The lead agent only reads ACP results; the container must not be able to forge them."""
    mounts = _thread_mounts_for(tmp_path, monkeypatch, user_id="userA", thread_id="t-1")
    by_target = {container_path: read_only for _, container_path, read_only in mounts}
    assert by_target["/mnt/acp-workspace"] is True


# ── config hygiene predicates ──────────────────────────────────────────────────


def _wide_host_mounts(cfg: SandboxConfig) -> list[str]:
    """Return any configured host mount that exposes a shared root.

    A hygienic director config bind-mounts nothing globally — per-thread data is mounted
    by the provider at acquire time. A static mount of ``/root/fs`` (or any filesystem
    root) would defeat per-thread isolation for every container.
    """
    offenders: list[str] = []
    for m in cfg.mounts:
        normalized = m.host_path.rstrip("/\\") or "/"
        # Filesystem root, the DEER_FLOW_HOME share, or its bare threads/users dirs.
        if normalized in {"", "/"} or normalized.endswith("/root/fs") or normalized.endswith("/fs") or normalized.endswith("/threads") or normalized.endswith("/users"):
            offenders.append(m.host_path)
    return offenders


# Substrings that mark an env value/key as a credential the container must never see.
_SECRET_MARKERS = ("token", "secret", "password", "passwd", "api_key", "apikey", "auth", "credential", "private_key")


def _secret_env_keys(cfg: SandboxConfig) -> list[str]:
    """Return any ``sandbox.environment`` key that looks like a credential.

    The container's bash can ``env`` — anything here is exfiltratable. Secrets belong in
    the host process env or a per-task token channel, never injected into the sandbox.
    """
    offenders: list[str] = []
    for key in cfg.environment:
        lowered = key.lower()
        if any(marker in lowered for marker in _SECRET_MARKERS):
            offenders.append(key)
    return offenders


def test_hygiene_predicates_pass_for_a_clean_config():
    cfg = SandboxConfig(
        use="deerflow.community.aio_sandbox.aio_sandbox_provider:AioSandboxProvider",
        image="registry.example.com/aio-sandbox:latest",
        container_prefix="deer-flow-sandbox",
        # No static mounts (provider mounts per-thread); only non-secret env.
        environment={"TZ": "UTC", "FFMPEG_THREADS": "2"},
    )
    assert _wide_host_mounts(cfg) == []
    assert _secret_env_keys(cfg) == []


def test_hygiene_predicate_catches_wide_root_mount():
    cfg = SandboxConfig(
        use="x:Y",
        mounts=[VolumeMountConfig(host_path="/root/fs", container_path="/mnt/fs")],
    )
    assert _wide_host_mounts(cfg) == ["/root/fs"]


def test_hygiene_predicate_catches_filesystem_root_mount():
    cfg = SandboxConfig(
        use="x:Y",
        mounts=[VolumeMountConfig(host_path="/", container_path="/mnt/host")],
    )
    assert _wide_host_mounts(cfg) == ["/"]


def test_hygiene_predicate_catches_secret_env():
    cfg = SandboxConfig(
        use="x:Y",
        environment={
            "TZ": "UTC",
            "CFGPU_API_TOKEN": "$CFGPU_API_TOKEN",
            "OPENAI_API_KEY": "sk-xxx",
        },
    )
    assert set(_secret_env_keys(cfg)) == {"CFGPU_API_TOKEN", "OPENAI_API_KEY"}


def test_hygiene_predicate_allows_benign_env():
    cfg = SandboxConfig(use="x:Y", environment={"TZ": "UTC", "LANG": "C.UTF-8"})
    assert _secret_env_keys(cfg) == []
