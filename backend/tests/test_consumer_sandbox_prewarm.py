"""P1 — consumer __main__ wiring of the sandbox-provider prewarm + shutdown.

The AIO sandbox provider (BUG-010/011, see ``cfgpu-docs/aio-localbackend-sandbox.md``
D6/P1) is lazily constructed on the *first* sandbox acquire. Two problems follow if
that construction happens on the main thread / event loop after the consumer installs
its own signal handlers:

  - BUG-010: ``AioSandboxProvider.__init__`` calls ``signal.signal()`` which, on the
    main thread, *steals* the consumer's ``loop.add_signal_handler`` and breaks the
    draining-first shutdown. Off the main thread ``signal.signal()`` raises
    ``ValueError`` (which the provider already swallows) so nothing is stolen.
  - BUG-011: the same ``__init__`` runs ``_reconcile_orphans()`` → ``list_running()``
    (``docker ps``), a synchronous blocking call that must not run on the event loop.

The fix is a thin ``_prewarm_sandbox_provider`` helper that constructs the provider via
``asyncio.to_thread`` at startup (before signal handlers are installed), plus a
``_shutdown_sandbox_provider`` helper that explicitly tears the containers down during
draining (we no longer rely on the provider's own signal handler). Following the
``_start_mlm_extraction_loop`` pattern, ``main()`` stays monolithic and these helpers
are pinned here.
"""

from __future__ import annotations

import signal
import threading
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from app.consumer.__main__ import _prewarm_sandbox_provider, _shutdown_sandbox_provider

# ── prewarm ───────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_prewarm_constructs_off_the_main_thread():
    """BUG-010/011: provider construction must run in a worker thread, not the loop."""
    seen: dict = {}

    def _getter():
        seen["thread"] = threading.current_thread()
        seen["is_main"] = threading.current_thread() is threading.main_thread()
        return SimpleNamespace()

    with patch("app.consumer.__main__.get_sandbox_provider", _getter):
        provider = await _prewarm_sandbox_provider()

    assert provider is not None
    # Constructed off the main thread → AioSandboxProvider's signal.signal() would
    # raise ValueError (swallowed) instead of stealing the consumer's handlers.
    assert seen["is_main"] is False


@pytest.mark.anyio
async def test_prewarm_does_not_steal_consumer_signal_handlers():
    """BUG-010: prewarming off the main thread leaves consumer signal handlers intact.

    Simulates ``AioSandboxProvider.__init__``: it tries to grab SIGTERM but swallows
    the ValueError raised when called off the main thread. Because prewarm runs the
    factory via ``to_thread``, the consumer's sentinel handler must survive untouched.
    If someone regresses prewarm to a direct main-thread call, the factory's
    ``signal.signal`` succeeds and this test fails.
    """

    def _sentinel(signum, frame):  # consumer's handler stand-in
        pass

    def _provider_factory():
        # mimic AioSandboxProvider: attempt to register, swallow off-main-thread error
        try:
            signal.signal(signal.SIGTERM, lambda *a: None)
        except ValueError:
            pass
        return SimpleNamespace()

    previous = signal.signal(signal.SIGTERM, _sentinel)
    try:
        with patch("app.consumer.__main__.get_sandbox_provider", _provider_factory):
            await _prewarm_sandbox_provider()
        # Handler not stolen: still the consumer's sentinel.
        assert signal.getsignal(signal.SIGTERM) is _sentinel
    finally:
        signal.signal(signal.SIGTERM, previous)


@pytest.mark.anyio
async def test_prewarm_calls_singleton_getter_exactly_once():
    """Prewarm primes the cached singleton; the first real acquire then reuses it."""
    calls = {"n": 0}
    provider = SimpleNamespace()

    def _getter():
        calls["n"] += 1
        return provider

    with patch("app.consumer.__main__.get_sandbox_provider", _getter):
        result = await _prewarm_sandbox_provider()

    assert result is provider
    assert calls["n"] == 1


@pytest.mark.anyio
async def test_prewarm_returns_none_on_failure_keeping_lazy_fallback():
    """A construction failure must not abort startup — lazy first-acquire is the fallback."""

    def _boom():
        raise RuntimeError("docker daemon unreachable")

    with patch("app.consumer.__main__.get_sandbox_provider", _boom):
        result = await _prewarm_sandbox_provider()

    assert result is None


# ── shutdown ──────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_shutdown_calls_provider_shutdown_off_the_loop():
    """Draining tears containers down explicitly, off the event loop."""
    called: dict = {}

    def _shutdown():
        called["thread"] = threading.current_thread()

    provider = SimpleNamespace(shutdown=_shutdown)
    await _shutdown_sandbox_provider(provider)

    assert "thread" in called
    # Ran via to_thread, not inline on the event loop.
    assert called["thread"] is not threading.main_thread()


@pytest.mark.anyio
async def test_shutdown_is_noop_for_provider_without_shutdown():
    """LocalSandboxProvider has no shutdown(); the helper must be a clean no-op."""
    provider = SimpleNamespace()  # no shutdown attribute
    await _shutdown_sandbox_provider(provider)  # must not raise


@pytest.mark.anyio
async def test_shutdown_is_noop_on_none():
    """Prewarm may have returned None (failure path); shutdown must tolerate it."""
    await _shutdown_sandbox_provider(None)  # must not raise


@pytest.mark.anyio
async def test_shutdown_swallows_provider_errors():
    """A failing container teardown must not break the draining sequence."""

    def _boom():
        raise RuntimeError("docker gone")

    provider = SimpleNamespace(shutdown=_boom)
    await _shutdown_sandbox_provider(provider)  # logged, not raised
