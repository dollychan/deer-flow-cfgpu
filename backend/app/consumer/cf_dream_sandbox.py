"""CF-Dream consumer AIO sandbox provider.

This subclass exists only to strip the deerflow base provider's autonomous container
destroy power (P4 / I16) and to relocate the cross-process creation flock onto per-host
local disk (R3 / I17). It does NOT customise the sandbox identity key.

Tenancy is by ``thread_id`` alone (cfgpu-docs/thread-tenancy.md D3). The former per-user
composite ``_identity_key`` override — ``hash("{user_id}/{thread_id}")``, D11 — has been
retired together with the per-user disk layout: thread data is now bind-mounted from the
single shared ``threads/{thread_id}/...`` bucket, so a warm container reused across users
serves the one shared disk (D4, intended), not a foreign one (the old "leak"). The base
``_identity_key`` already keys by ``thread_id``, so removing the override is sufficient;
the serial claim lock is itself thread_id-single-key, keeping same-thread runs serial.

Wired via ``config.yaml`` ``sandbox.use: app.consumer.cf_dream_sandbox:CfDreamAioProvider``.
See ``cfgpu-docs/aio-localbackend-sandbox.md`` (D11 retired) and ``thread-tenancy.md``.

The provider class was renamed ``DirectorAioProvider`` → ``CfDreamAioProvider`` to align
with the agent name (``cf-dream``). The Docker image tag ``sandbox-cfdream`` /
``all-in-one-sandbox-cfdream`` is intentionally left unchanged — it is deployment-stable
and renaming it would require rebuilding/pushing images; only the Python class/module
identifier moved.
"""

from __future__ import annotations

import logging
from pathlib import Path

from app.consumer import sandbox_locks
from deerflow.community.aio_sandbox.aio_sandbox_provider import AioSandboxProvider
from deerflow.community.aio_sandbox.sandbox_info import SandboxInfo

logger = logging.getLogger(__name__)


class CfDreamAioProvider(AioSandboxProvider):
    """AIO provider that yields its autonomous destroy power to the per-host janitor.

    Identity is inherited from the base (``thread_id``-keyed); see module docstring and
    cfgpu-docs/thread-tenancy.md for why the former per-user override (D11) was retired.
    """

    # ── P4: strip autonomous destroy power (I16) ────────────────────────────────

    def _destroy_on_shutdown(self, sandbox_ids: list[str], warm_items: list[tuple[str, tuple[SandboxInfo, float]]]) -> None:
        """No-op: a cf-dream consumer process never blind-destroys containers on shutdown.

        ``_reconcile_orphans`` adopts *every* running container on this VM — including
        ones other instances are serving — into the warm pool, so destroying them here
        would nuke live cross-instance work (D5 / BUG-012). The process keeps only the
        D7 cancel path's targeted ``docker kill`` as its destroy power; orphaned/parked
        containers are reclaimed out-of-band by the per-host run-flock janitor (D13).
        """
        logger.info("CfDreamAioProvider.shutdown: leaving %d active + %d warm container(s) for the janitor (I16)", len(sandbox_ids), len(warm_items))

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

    # ── P4 / R2: drop dead cache references without autonomous destroy (I16 / I19) ─

    def _destroy_dropped_sandbox(self, sandbox_id: str, info: SandboxInfo) -> None:
        """No-op: forget a dead cache reference but never autonomously ``docker rm`` it.

        Override of the base ``_drop_unhealthy_sandbox`` teardown seam. On a failed
        ``is_alive`` probe (active reuse or warm reclaim) the base still forgets the stale
        in-process reference — under its ``expected_info`` identity guard, so a freshly
        recreated same-id sandbox is never evicted — and closes the local client handle;
        acquire then falls through to discover/create. We only strip the autonomous
        ``self._backend.destroy`` (I16, see ``_destroy_on_shutdown`` / ``_evict_oldest_warm``):
        the deterministic container name is shared across same-host instances, so a blind
        ``docker rm`` (no creation/run flock held) could TOCTOU-kill a container a peer just
        (re)created with that same name. ``is_alive`` already returned False here, so the
        container is gone — there is nothing to destroy anyway; any genuinely orphaned
        container is reclaimed out-of-band by the per-host run-flock janitor (D13).
        """
        logger.info("CfDreamAioProvider: left container teardown to the janitor for sandbox %s (I16)", sandbox_id)

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
