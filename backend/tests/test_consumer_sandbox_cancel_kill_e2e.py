"""P6 — end-to-end smoke for D7 cancel-into-container (real Docker).

Goal ① of the P6 acceptance (the codeable half): a hard cancel **really** ``docker
kill``s the thread's sandbox container, which severs any in-flight in-container work
(the wedged ffmpeg / blocking HTTP the design relies on), so the run thread unwedges,
the container vanishes (``--rm``), and the next command is not blocked. This drives the
*production* function ``app.consumer.agent_runner._docker_kill`` against a real Docker
daemon with a lightweight ``busybox`` image (no enterprise sandbox image / HTTP server
needed — the invariant under test is the container-layer kill semantics, exactly the
"limited to direct backend/container operations" scope of the existing orphan e2e).

Goal ② (cross-machine thread-migration product continuity via virtiofs ``cache`` mode)
is a deployment-only confirmation — not reproducible on a single dev host — and lives in
the ``vm-部署.md`` §8 acceptance checklist instead.

Run with: PYTHONPATH=. uv run pytest tests/test_consumer_sandbox_cancel_kill_e2e.py -v -s
Requires: Docker running locally.
"""

import subprocess
import threading
import time

import pytest

from app.consumer import agent_runner


def _docker_available() -> bool:
    try:
        result = subprocess.run(["docker", "info"], capture_output=True, timeout=5)
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _container_running(container_name: str) -> bool:
    result = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", container_name],
        capture_output=True,
        text=True,
        timeout=5,
    )
    return result.returncode == 0 and result.stdout.strip().lower() == "true"


E2E_TEST_IMAGE = "busybox:latest"
E2E_PREFIX = "deer-flow-sandbox-canceltest"


@pytest.fixture(autouse=True)
def cleanup_test_containers():
    """Force-remove any container created by this module, before and after each test."""

    def _sweep():
        result = subprocess.run(
            ["docker", "ps", "-a", "--filter", f"name={E2E_PREFIX}-", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        for name in result.stdout.strip().splitlines():
            name = name.strip()
            if name:
                subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=10)

    _sweep()
    yield
    _sweep()


@pytest.fixture(scope="module", autouse=True)
def ensure_busybox_image():
    """Pre-pull busybox so per-test ``docker run`` is fast and not racing the network."""
    if not _docker_available():
        return
    subprocess.run(["docker", "pull", E2E_TEST_IMAGE], capture_output=True, timeout=120)


@pytest.mark.skipif(not _docker_available(), reason="Docker not available")
class TestCancelKillE2E:
    def test_docker_kill_severs_in_flight_work_and_unwedges(self):
        """The linchpin: killing the container severs an in-flight in-container command.

        Models the wedged ffmpeg — an ``execute_command`` whose blocking call cannot be
        reached by ``runner_task.cancel()``. ``docker kill`` is the only thing that frees
        it. We assert the in-flight ``docker exec`` returns *fast* (severed, not waiting out
        its 300s sleep) with a non-zero status, and that the container is gone afterward.
        """
        container_name = f"{E2E_PREFIX}-wedge01"

        # A long-lived container (the sandbox), busy with a 300s task.
        result = subprocess.run(
            ["docker", "run", "--rm", "-d", "--name", container_name, E2E_TEST_IMAGE, "sleep", "300"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, f"Failed to start container: {result.stderr}"
        assert _container_running(container_name)

        # An in-flight, long-blocking command inside the container (the "wedged ffmpeg").
        exec_outcome: dict = {}

        def run_in_flight_exec():
            start = time.monotonic()
            proc = subprocess.run(
                ["docker", "exec", container_name, "sh", "-c", "sleep 300"],
                capture_output=True,
                text=True,
                timeout=120,  # safety net; real unwedge should be << this
            )
            exec_outcome["elapsed"] = time.monotonic() - start
            exec_outcome["returncode"] = proc.returncode

        exec_thread = threading.Thread(target=run_in_flight_exec, daemon=True)
        exec_thread.start()
        time.sleep(1.5)  # let the exec actually start running inside the container

        # ── Production cancel path: hard docker kill of the whole container (D7). ──
        kill_start = time.monotonic()
        agent_runner._docker_kill(container_name)
        kill_elapsed = time.monotonic() - kill_start

        # The wedged exec must return promptly (container death severs it), not sit for 300s.
        exec_thread.join(timeout=30)
        assert not exec_thread.is_alive(), "in-flight exec did not return after docker kill (still wedged)"
        assert exec_outcome.get("elapsed", 999) < 30, f"exec took too long to unwedge: {exec_outcome}"
        assert exec_outcome.get("returncode", 0) != 0, "severed exec should report failure, not success"
        assert kill_elapsed < 30, f"docker kill itself was slow: {kill_elapsed:.1f}s"

        # Container vanished (`--rm`) → nothing left for `_reconcile_orphans` to adopt (I12).
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and _container_running(container_name):
            time.sleep(0.2)
        assert not _container_running(container_name), "container still running after docker kill"

    def test_next_command_not_blocked_after_kill(self):
        """After cancel, the freed name/host accept a fresh container immediately (no wedge)."""
        container_name = f"{E2E_PREFIX}-reuse01"

        subprocess.run(
            ["docker", "run", "--rm", "-d", "--name", container_name, E2E_TEST_IMAGE, "sleep", "300"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert _container_running(container_name)

        agent_runner._docker_kill(container_name)

        # Wait for the `--rm` cleanup to release the name.
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and _container_running(container_name):
            time.sleep(0.2)

        # A subsequent command (new container, same name) must succeed promptly.
        start = time.monotonic()
        result = subprocess.run(
            ["docker", "run", "--rm", "-d", "--name", container_name, E2E_TEST_IMAGE, "sleep", "5"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        elapsed = time.monotonic() - start
        assert result.returncode == 0, f"next command was blocked / failed: {result.stderr}"
        assert elapsed < 30, f"next command took too long: {elapsed:.1f}s"

    def test_docker_kill_missing_container_is_graceful_noop(self):
        """``_docker_kill`` on an absent container never raises (cancel must not derail)."""
        # Must not raise — 'no such container' is treated as success (--rm vanish semantics).
        agent_runner._docker_kill(f"{E2E_PREFIX}-does-not-exist-zzz")
