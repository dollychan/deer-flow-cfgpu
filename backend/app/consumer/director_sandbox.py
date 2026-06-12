"""Director-specific AIO sandbox provider.

The deerflow base ``AioSandboxProvider`` keys a thread's sandbox identity by ``thread_id``
alone — correct for single-tenant use, but the director runs many users through one VM and
bind-mounts per-``(user, thread)`` data (``users/{uid}/threads/{tid}/``). ``thread_id`` is
not globally unique across users, so the base keying would let two users colliding on a
thread_id reuse each other's sandbox and read into each other's bucket (D11 cross-user
reuse leak).

This subclass is the *single* override point: it folds the effective user into the one
keying seam ``_identity_key``. Everything downstream — the container-name hash, the
``_thread_sandboxes`` reuse map, the in-process thread lock, the cross-process file lock —
inherits the composite key for free, because the base already routes all three sites
through this method.

Wired via ``config.yaml`` ``sandbox.use: app.consumer.director_sandbox:DirectorAioProvider``.
See ``cfgpu-docs/aio-localbackend-sandbox.md`` D11 / P2.5.
"""

from __future__ import annotations

import logging
from pathlib import Path

from app.consumer import sandbox_locks
from deerflow.community.aio_sandbox.aio_sandbox_provider import AioSandboxProvider
from deerflow.community.aio_sandbox.sandbox_info import SandboxInfo
from deerflow.runtime.user_context import get_effective_user_id

logger = logging.getLogger(__name__)


class DirectorAioProvider(AioSandboxProvider):
    """AIO sandbox provider whose sandbox identity is composite ``(user_id, thread_id)``."""

    def _identity_key(self, thread_id: str, user_id: str | None = None) -> str:
        """Fold the effective user into the sandbox identity key.

        ``user_id`` is preferred when given (the cancel-into-container path passes it
        explicitly, R1). Otherwise it is read from ``get_effective_user_id`` — the *same*
        source ``_get_thread_mounts`` uses — so the identity key and the mounted data
        bucket are always derived from one place (BUG-008). The two agree during a run:
        the consumer sets the ContextVar from the same ``message.user_id`` it later hands
        the cancel path. The composite ``"{user_id}/{thread_id}"`` is then hashed by the
        base into the 8-char container suffix, keeping container names opaque to the
        backend/discover/reconcile machinery while making them unique per user.
        """
        uid = user_id if user_id is not None else get_effective_user_id()
        return f"{uid}/{thread_id}"

    # ── P4: strip autonomous destroy power (I16) ────────────────────────────────

    def _destroy_on_shutdown(self, sandbox_ids: list[str], warm_items: list[tuple[str, tuple[SandboxInfo, float]]]) -> None:
        """No-op: a director process never blind-destroys containers on shutdown.

        ``_reconcile_orphans`` adopts *every* running container on this VM — including
        ones other instances are serving — into the warm pool, so destroying them here
        would nuke live cross-instance work (D5 / BUG-012). The process keeps only the
        D7 cancel path's targeted ``docker kill`` as its destroy power; orphaned/parked
        containers are reclaimed out-of-band by the per-host run-flock janitor (D13).
        """
        logger.info("DirectorAioProvider.shutdown: leaving %d active + %d warm container(s) for the janitor (I16)", len(sandbox_ids), len(warm_items))

    def _cleanup_idle_sandboxes(self, idle_timeout: float) -> None:
        """No-op: idle-based teardown is the janitor's job, never the process's (I16).

        Deployment sets ``idle_timeout: 0`` so the idle-checker thread is never even
        spawned; this override is the defense-in-depth guarantee that *if* a misconfigured
        ``idle_timeout > 0`` does start the checker, it still cannot blind-``docker stop``
        active or warm containers that other instances on this VM may be serving.
        """
        return None

    def _evict_oldest_warm(self) -> str | None:
        """Never capacity-destroy a warm container.

        A warm container parked by this process is still running and may be discovered
        and reclaimed by another instance on the same VM; tearing it down on local
        capacity pressure would sever that. Capacity is bounded by the janitor's warm
        TTL, not by us (I16). The base soft-cap log still fires for the over-budget case.
        """
        return None

    # ── P4 / R2: discard janitor-killed warm references on reclaim (I19) ─────────

    def _warm_is_promotable(self, info: SandboxInfo) -> bool:
        """Probe the container before promoting a warm entry back to active.

        The per-host janitor may have ``docker rm -f``'d a parked container while its
        ``SandboxInfo`` still sits in this process's warm pool. Promoting that dead
        reference would hand the run a sandbox whose container is gone; instead probe
        ``is_alive`` so a dead entry is discarded and acquire falls back to create.
        """
        return self._backend.is_alive(info)

    # ── P4 / R3: move the creation flock off virtiofs onto local disk (I17) ──────

    def _creation_lock_path(self, thread_id: str, sandbox_id: str, *, user_id: str | None = None) -> Path:
        """Relocate the cross-process creation lock to per-host local disk.

        The base keeps it under the per-thread virtiofs dir, where flock is unreliable.
        Containers (and thus name conflicts) are per-host, so the lock only needs to
        serialize same-host processes — a real local-disk flock keyed by ``sandbox_id``.
        Routed through :mod:`app.consumer.sandbox_locks` so the provider, the run-flock,
        and the janitor all derive the identical path.
        """
        return sandbox_locks.creation_lock_path(sandbox_id)
