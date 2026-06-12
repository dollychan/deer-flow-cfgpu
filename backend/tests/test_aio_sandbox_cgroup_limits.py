"""P5 / D9 — per-container cgroup resource limits (behavior-preserving additive seam).

The AIO LocalContainerBackend gains optional ``container_cpus`` / ``container_memory``
knobs that map to ``docker run --cpus / --memory``. They are config-gated: when unset
(the default) the produced ``docker run`` command is byte-for-byte identical to the
original — so this is an upstreamable additive seam, not a behavior change. The director
deployment sets them (2-core small disk) so one runaway ffmpeg transcode can't starve the
consumer's poll_loop (D9 quality isolation, NOT count-based throttling).
"""

from types import SimpleNamespace

from deerflow.community.aio_sandbox import aio_sandbox_provider as aio_mod
from deerflow.community.aio_sandbox.local_backend import LocalContainerBackend
from deerflow.config.sandbox_config import SandboxConfig


def _capture_start_container_command(monkeypatch, backend: LocalContainerBackend, runtime: str = "docker") -> list[str]:
    monkeypatch.setattr(backend, "_runtime", runtime)
    captured_cmd: list[str] = []

    def fake_run(cmd, **kwargs):
        captured_cmd.extend(cmd)
        return SimpleNamespace(stdout="container-id\n", stderr="", returncode=0)

    monkeypatch.setattr("subprocess.run", fake_run)
    backend._start_container("sandbox-test", 18080)
    return captured_cmd


def _backend(**overrides) -> LocalContainerBackend:
    kwargs = dict(
        image="sandbox:latest",
        base_port=8080,
        container_prefix="sandbox",
        config_mounts=[],
        environment={},
    )
    kwargs.update(overrides)
    return LocalContainerBackend(**kwargs)


def test_no_cgroup_config_keeps_original_command(monkeypatch):
    """Default (no cgroup config) → no --cpus / --memory flags at all (byte-identical)."""
    backend = _backend()
    cmd = _capture_start_container_command(monkeypatch, backend)

    assert "--cpus" not in cmd
    assert "--memory" not in cmd


def test_container_cpus_appends_cpus_flag(monkeypatch):
    backend = _backend(container_cpus="2")
    cmd = _capture_start_container_command(monkeypatch, backend)

    assert cmd[cmd.index("--cpus") + 1] == "2"
    assert "--memory" not in cmd


def test_container_memory_appends_memory_flag(monkeypatch):
    backend = _backend(container_memory="4g")
    cmd = _capture_start_container_command(monkeypatch, backend)

    assert cmd[cmd.index("--memory") + 1] == "4g"
    assert "--cpus" not in cmd


def test_both_limits_append_both_flags(monkeypatch):
    backend = _backend(container_cpus="1.5", container_memory="2g")
    cmd = _capture_start_container_command(monkeypatch, backend)

    assert cmd[cmd.index("--cpus") + 1] == "1.5"
    assert cmd[cmd.index("--memory") + 1] == "2g"


def test_cgroup_flags_precede_image(monkeypatch):
    """Resource flags must sit in the OPTIONS section, before the IMAGE positional."""
    backend = _backend(container_cpus="2", container_memory="4g")
    cmd = _capture_start_container_command(monkeypatch, backend)

    image_idx = cmd.index("sandbox:latest")
    assert cmd.index("--cpus") < image_idx
    assert cmd.index("--memory") < image_idx


def test_cgroup_flags_gated_to_docker_runtime(monkeypatch):
    """Apple Container runtime path: cgroup flags are docker-gated, so they stay absent.

    Matches the existing ``if self._runtime == "docker"`` gating for --security-opt /
    port binding. cgroup quality isolation is a linux/docker deployment concern; macOS
    apple-container dev silently skips it rather than risking an unsupported-flag failure.
    """
    backend = _backend(container_cpus="2", container_memory="4g")
    cmd = _capture_start_container_command(monkeypatch, backend, runtime="container")

    assert "--cpus" not in cmd
    assert "--memory" not in cmd


def test_zero_or_empty_values_treated_as_unset(monkeypatch):
    """Empty-string / falsy cgroup values are treated as unset (no flag emitted)."""
    backend = _backend(container_cpus="", container_memory="")
    cmd = _capture_start_container_command(monkeypatch, backend)

    assert "--cpus" not in cmd
    assert "--memory" not in cmd


# ── provider plumbing: config → backend ──────────────────────────────────────


def test_sandbox_config_defaults_cgroup_to_none():
    """Behavior preservation at the schema level: unset cgroup fields default to None."""
    cfg = SandboxConfig(use="deerflow.sandbox.local:LocalSandboxProvider")
    assert cfg.container_cpus is None
    assert cfg.container_memory is None


def test_load_config_reads_cgroup_fields(monkeypatch):
    """_load_config surfaces container_cpus / container_memory from SandboxConfig."""
    sandbox_config = SandboxConfig(
        use="x:Y",
        container_cpus="2",
        container_memory="4g",
    )
    monkeypatch.setattr(aio_mod, "get_app_config", lambda: SimpleNamespace(sandbox=sandbox_config))

    provider = aio_mod.AioSandboxProvider.__new__(aio_mod.AioSandboxProvider)
    config = provider._load_config()

    assert config["container_cpus"] == "2"
    assert config["container_memory"] == "4g"


def test_create_backend_passes_cgroup_to_local_backend(monkeypatch):
    """_create_backend forwards the cgroup values into LocalContainerBackend."""
    provider = aio_mod.AioSandboxProvider.__new__(aio_mod.AioSandboxProvider)
    provider._config = {
        "image": "sandbox:latest",
        "port": 8080,
        "container_prefix": "deer-flow-sandbox",
        "mounts": [],
        "environment": {},
        "provisioner_url": "",
        "container_cpus": "1.5",
        "container_memory": "2g",
    }
    # avoid runtime auto-detection subprocess in the test environment
    monkeypatch.setattr(LocalContainerBackend, "_detect_runtime", lambda self: "docker")

    backend = provider._create_backend()

    assert isinstance(backend, LocalContainerBackend)
    assert backend._container_cpus == "1.5"
    assert backend._container_memory == "2g"
