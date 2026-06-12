"""BUG-014 — host-bash-on-local-sandbox root guard (_enforce_host_bash_safety).

LocalSandboxProvider + sandbox.allow_host_bash makes bash a raw host subprocess with
no sandbox boundary; the only remaining protection is the process UID + OS hardening.
The guard fails closed when that combo runs as root, no-ops for Aio / disabled bash,
and only warns (does not block) when non-root. These tests pin each branch by faking
os.geteuid + the override env var, so they run identically on root and non-root CI.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.consumer.__main__ import _ALLOW_ROOT_HOST_BASH_ENV, _enforce_host_bash_safety

_LOCAL = "deerflow.sandbox.local:LocalSandboxProvider"
_AIO = "deerflow.community.aio_sandbox.aio_sandbox_provider:AioSandboxProvider"


def _config(use: str, allow_host_bash: bool) -> SimpleNamespace:
    return SimpleNamespace(sandbox=SimpleNamespace(use=use, allow_host_bash=allow_host_bash))


@pytest.fixture
def fake_uid(monkeypatch):
    """Force os.geteuid() to a chosen value; default to a normal user."""

    def _set(uid: int) -> None:
        monkeypatch.setattr("app.consumer.__main__.os.geteuid", lambda: uid, raising=False)

    _set(1000)
    return _set


@pytest.fixture(autouse=True)
def _clear_override(monkeypatch):
    monkeypatch.delenv(_ALLOW_ROOT_HOST_BASH_ENV, raising=False)


# ── fail-closed: the one dangerous combo ──────────────────────────────────────


def test_root_local_host_bash_raises(fake_uid):
    fake_uid(0)
    with pytest.raises(RuntimeError, match="Refusing to start"):
        _enforce_host_bash_safety(_config(_LOCAL, allow_host_bash=True))


def test_root_override_env_allows(fake_uid, monkeypatch):
    fake_uid(0)
    monkeypatch.setenv(_ALLOW_ROOT_HOST_BASH_ENV, "1")
    _enforce_host_bash_safety(_config(_LOCAL, allow_host_bash=True))  # no raise


def test_root_override_env_wrong_value_still_raises(fake_uid, monkeypatch):
    fake_uid(0)
    monkeypatch.setenv(_ALLOW_ROOT_HOST_BASH_ENV, "true")  # only "1" counts
    with pytest.raises(RuntimeError, match="Refusing to start"):
        _enforce_host_bash_safety(_config(_LOCAL, allow_host_bash=True))


# ── allowed branches (no raise) ───────────────────────────────────────────────


def test_nonroot_local_host_bash_warns_only(fake_uid):
    fake_uid(1000)
    _enforce_host_bash_safety(_config(_LOCAL, allow_host_bash=True))  # warns, no raise


def test_root_local_bash_disabled_ok(fake_uid):
    fake_uid(0)
    _enforce_host_bash_safety(_config(_LOCAL, allow_host_bash=False))  # disabled → no-op


def test_root_aio_provider_ok(fake_uid):
    fake_uid(0)
    # Aio: allow_host_bash is irrelevant (bash runs in container) → never blocks.
    _enforce_host_bash_safety(_config(_AIO, allow_host_bash=True))


def test_no_sandbox_section_ok(fake_uid):
    fake_uid(0)
    _enforce_host_bash_safety(SimpleNamespace(sandbox=None))


def test_geteuid_absent_treated_nonroot(monkeypatch):
    # Windows-like platforms have no os.geteuid → cannot be root → only warn.
    monkeypatch.delattr("app.consumer.__main__.os.geteuid", raising=False)
    _enforce_host_bash_safety(_config(_LOCAL, allow_host_bash=True))  # no raise
