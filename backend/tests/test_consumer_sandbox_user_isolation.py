"""P2.5 — composite sandbox_id (D11): plug the cross-user reuse leak.

The original ``sandbox_id = sha256(thread_id)[:8]`` keyed a sandbox by thread_id alone,
but the isolation boundary is ``(user_id, thread_id)`` — the mount path is
``users/{uid}/threads/{tid}/``. ``thread_id`` is *not* guaranteed globally unique across
users, so if userA and userB collide on a thread_id, all three reuse paths leak:

  1. in-process cache ``_thread_sandboxes[thread_id]`` → userB grabs userA's live sandbox;
  2. warm-pool reclaim keyed by the thread-only hash;
  3. backend discover by container name (the thread-only hash).

Mounts are frozen at create time by the *creator's* user, so userB would read/write into
userA's ``outputs/``.

Fix (D11): route the three keying sites — container-name source, ``_thread_sandboxes``
key, thread-lock key — through one seam ``_identity_key(thread_id)``. The deerflow base
keeps it ``return thread_id`` (byte-for-byte unchanged, upstreamable). The app subclass
``DirectorAioProvider`` overrides it to fold in the *same* user_id source the mounts use
(``get_effective_user_id``), so the identity key and the mount bucket are derived from a
single source. One override point, fail-loud here.

See ``cfgpu-docs/aio-localbackend-sandbox.md`` D11 / P2.5.
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


def _director_cls():
    return importlib.import_module("app.consumer.director_sandbox").DirectorAioProvider


# ── base class: behaviour-preserving default (upstreamable) ─────────────────────


def test_base_identity_key_is_thread_id_unchanged():
    """The deerflow base keys by thread_id alone — no behaviour change, no user folded in."""
    provider = _bare_provider(_base_cls())
    assert provider._identity_key("t-1") == "t-1"


def test_base_sandbox_id_unchanged_byte_for_byte():
    """Base sandbox_id derivation must stay sha256(thread_id)[:8] (upstream parity)."""
    base = _base_cls()
    provider = _bare_provider(base)
    # Independent of any user ContextVar — the base ignores the user entirely.
    token = set_current_user(SimpleNamespace(id="whoever"))
    try:
        got = provider._sandbox_id_for_thread("t-1")
    finally:
        reset_current_user(token)
    assert got == base._deterministic_sandbox_id("t-1")


# ── director subclass: composite identity (the fix) ─────────────────────────────


def _sandbox_id_under_user(provider, user_id: str, thread_id: str) -> str:
    token = set_current_user(SimpleNamespace(id=user_id))
    try:
        return provider._sandbox_id_for_thread(thread_id)
    finally:
        reset_current_user(token)


def test_a_same_thread_two_users_get_distinct_sandbox_ids():
    """(a) userA and userB colliding on a thread_id derive different sandbox_ids/names."""
    provider = _bare_provider(_director_cls())
    a = _sandbox_id_under_user(provider, "userA", "shared-tid")
    b = _sandbox_id_under_user(provider, "userB", "shared-tid")
    assert a != b


def test_director_identity_key_folds_in_user():
    """The override folds the effective user into the key, co-located with the mount bucket."""
    provider = _bare_provider(_director_cls())
    token = set_current_user(SimpleNamespace(id="userA"))
    try:
        key = provider._identity_key("t-1")
    finally:
        reset_current_user(token)
    assert "userA" in key
    assert "t-1" in key


def test_b_in_process_cache_does_not_cross_buckets():
    """(b) userB must not reuse userA's active in-process sandbox under the same thread_id."""
    provider = _bare_provider(_director_cls())

    # userA acquires: register an active sandbox keyed by userA's identity.
    token_a = set_current_user(SimpleNamespace(id="userA"))
    try:
        sid_a = provider._sandbox_id_for_thread("shared-tid")
        provider._sandboxes[sid_a] = object()
        provider._register_created_sandbox(
            "shared-tid", sid_a, SimpleNamespace(sandbox_url="http://a", sandbox_id=sid_a)
        )
        # userA reuses its own sandbox.
        assert provider._reuse_in_process_sandbox("shared-tid") == sid_a
    finally:
        reset_current_user(token_a)

    # userB on the same thread_id must NOT see userA's sandbox.
    token_b = set_current_user(SimpleNamespace(id="userB"))
    try:
        assert provider._reuse_in_process_sandbox("shared-tid") is None
    finally:
        reset_current_user(token_b)


def test_c_warm_pool_reclaim_does_not_cross_buckets():
    """(c) warm-pool reclaim must not promote userA's warm sandbox for userB."""
    provider = _bare_provider(_director_cls())

    # userA parks a sandbox in the warm pool under its composite sandbox_id.
    sid_a = _sandbox_id_under_user(provider, "userA", "shared-tid")
    provider._warm_pool[sid_a] = (SimpleNamespace(sandbox_url="http://a", sandbox_id=sid_a), 0.0)

    # userB derives a *different* sandbox_id → reclaim keyed by that id misses.
    sid_b = _sandbox_id_under_user(provider, "userB", "shared-tid")
    assert sid_b != sid_a
    token_b = set_current_user(SimpleNamespace(id="userB"))
    try:
        assert provider._reclaim_warm_pool_sandbox("shared-tid", sid_b) is None
    finally:
        reset_current_user(token_b)
    # userA's warm entry is untouched.
    assert sid_a in provider._warm_pool


def test_thread_locks_are_not_shared_across_users():
    """Two users on the same thread_id must not share one in-process lock (③)."""
    provider = _bare_provider(_director_cls())

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

    assert lock_a is not lock_b


def test_same_user_same_thread_is_stable():
    """Determinism: the same (user, thread) always derives the same sandbox_id."""
    provider = _bare_provider(_director_cls())
    first = _sandbox_id_under_user(provider, "userA", "t-1")
    second = _sandbox_id_under_user(provider, "userA", "t-1")
    assert first == second
