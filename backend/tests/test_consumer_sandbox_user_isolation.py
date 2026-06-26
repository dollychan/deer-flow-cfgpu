"""Thread-only sandbox identity (thread-tenancy.md D3/D4): the D11 composite key is retired.

The former design keyed a sandbox by ``(user_id, thread_id)`` because thread data lived at
``users/{uid}/threads/{tid}/`` — a per-user disk bucket. ``thread_id`` is globally unique
per the MQ protocol, but the *same* thread_id may legitimately be driven by multiple users
(intended shared session, e.g. a group chat). The thread-tenancy overhaul collapses the
disk to a single shared ``threads/{tid}/`` bucket, so a sandbox reused across users now
serves the one shared disk — by design (D4), not the old "cross-user reuse leak".

Consequently the sandbox identity is keyed by ``thread_id`` alone for BOTH the deerflow base
and the app ``CfDreamAioProvider`` (the per-user ``_identity_key`` override, D11/P2.5, was
deleted). All three reuse paths — in-process cache, warm-pool reclaim, thread lock — share
across users on the same thread_id. Concurrency stays safe because the serial claim lock is
itself thread_id-single-key (``thread_run_state`` PK = thread_id), so two users on one
thread never run at once.

See ``cfgpu-docs/thread-tenancy.md`` (D3/D4) and ``aio-localbackend-sandbox.md`` (D11 retired).
"""

from __future__ import annotations

import importlib
import threading
from types import SimpleNamespace

from deerflow.runtime.user_context import reset_current_user, set_current_user

AIO_MOD = "deerflow.community.aio_sandbox.aio_sandbox_provider"


def _bare_provider(cls):
    """Build a provider instance with only the in-memory maps the keying touches."""
    provider = cls.__new__(cls)
    provider._lock = threading.Lock()
    provider._thread_sandboxes = {}
    provider._thread_locks = {}
    provider._sandboxes = {}
    provider._sandbox_infos = {}
    provider._last_activity = {}
    provider._warm_pool = {}
    return provider


def _base_cls():
    return importlib.import_module(AIO_MOD).AioSandboxProvider


def _cf_dream_cls():
    return importlib.import_module("app.consumer.cf_dream_sandbox").CfDreamAioProvider


def _sandbox_id_under_user(provider, user_id: str, thread_id: str) -> str:
    token = set_current_user(SimpleNamespace(id=user_id))
    try:
        return provider._sandbox_id_for_thread(thread_id)
    finally:
        reset_current_user(token)


# ── base class: thread-only identity (upstreamable, unchanged) ──────────────────


def test_base_identity_key_is_thread_id_unchanged():
    """The deerflow base keys by thread_id alone — no user folded in."""
    provider = _bare_provider(_base_cls())
    assert provider._identity_key("t-1") == "t-1"


def test_base_sandbox_id_unchanged_byte_for_byte():
    """Base sandbox_id derivation stays sha256(thread_id)[:8] (upstream parity)."""
    base = _base_cls()
    provider = _bare_provider(base)
    token = set_current_user(SimpleNamespace(id="whoever"))
    try:
        got = provider._sandbox_id_for_thread("t-1")
    finally:
        reset_current_user(token)
    assert got == base._deterministic_sandbox_id("t-1")


# ── cf-dream provider subclass: thread-only identity, shared across users (D3/D4) ──


def test_cf_dream_does_not_override_identity_key():
    """The D11 per-user override is retired: cf-dream provider inherits the base thread-only key."""
    provider = _cf_dream_cls()
    base = _base_cls()
    # The subclass must not redefine _identity_key — it uses the base method object.
    assert "_identity_key" not in provider.__dict__
    assert provider._identity_key is base._identity_key


def test_cf_dream_identity_key_is_thread_only_ignores_user():
    """The identity key is thread_id alone, regardless of the effective user."""
    provider = _bare_provider(_cf_dream_cls())
    token = set_current_user(SimpleNamespace(id="userA"))
    try:
        key = provider._identity_key("t-1")
    finally:
        reset_current_user(token)
    assert key == "t-1"


def test_same_thread_two_users_get_same_sandbox_id():
    """Two users colliding on a thread_id derive the SAME sandbox_id/name (shared, D4)."""
    provider = _bare_provider(_cf_dream_cls())
    a = _sandbox_id_under_user(provider, "userA", "shared-tid")
    b = _sandbox_id_under_user(provider, "userB", "shared-tid")
    assert a == b
    assert a == _cf_dream_cls()._deterministic_sandbox_id("shared-tid")


def test_in_process_cache_is_shared_across_users():
    """userB reuses userA's active in-process sandbox under the same thread_id (shared disk)."""
    provider = _bare_provider(_cf_dream_cls())

    token_a = set_current_user(SimpleNamespace(id="userA"))
    try:
        sid_a = provider._sandbox_id_for_thread("shared-tid")
        provider._sandboxes[sid_a] = object()
        provider._register_created_sandbox(
            "shared-tid", sid_a, SimpleNamespace(sandbox_url="http://a", sandbox_id=sid_a)
        )
        assert provider._reuse_in_process_sandbox("shared-tid") == sid_a
    finally:
        reset_current_user(token_a)

    # userB on the same thread_id reuses the very same sandbox — intended sharing.
    token_b = set_current_user(SimpleNamespace(id="userB"))
    try:
        assert provider._reuse_in_process_sandbox("shared-tid") == sid_a
    finally:
        reset_current_user(token_b)


def test_warm_pool_reclaim_is_shared_across_users():
    """warm-pool reclaim promotes the same sandbox for any user on the thread_id (D4)."""
    provider = _bare_provider(_cf_dream_cls())

    sid_a = _sandbox_id_under_user(provider, "userA", "shared-tid")
    provider._warm_pool[sid_a] = (SimpleNamespace(sandbox_url="http://a", sandbox_id=sid_a), 0.0)

    # userB derives the SAME sandbox_id → reclaim keyed by that id hits.
    sid_b = _sandbox_id_under_user(provider, "userB", "shared-tid")
    assert sid_b == sid_a


def test_thread_locks_are_shared_across_users():
    """Two users on the same thread_id share one in-process lock (serial, one shared disk)."""
    provider = _bare_provider(_cf_dream_cls())

    token_a = set_current_user(SimpleNamespace(id="userA"))
    try:
        lock_a = provider._get_thread_lock("shared-tid")
    finally:
        reset_current_user(token_a)

    token_b = set_current_user(SimpleNamespace(id="userB"))
    try:
        lock_b = provider._get_thread_lock("shared-tid")
    finally:
        reset_current_user(token_b)

    assert lock_a is lock_b


def test_same_thread_is_stable():
    """Determinism: the same thread_id always derives the same sandbox_id."""
    provider = _bare_provider(_cf_dream_cls())
    first = _sandbox_id_under_user(provider, "userA", "t-1")
    second = _sandbox_id_under_user(provider, "userB", "t-1")
    assert first == second
