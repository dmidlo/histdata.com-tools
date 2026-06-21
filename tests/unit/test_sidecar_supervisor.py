"""Tests for Temporal sidecar lifecycle supervision."""

from __future__ import annotations

import json
from pathlib import Path

from histdatacom.sidecar.supervisor import (
    SIDECAR_STATE_SCHEMA_VERSION,
    SidecarPaths,
    SidecarSupervisor,
    build_temporal_start_command,
)
from histdatacom.sidecar.runtime import (
    SidecarRuntimePolicy,
    build_sidecar_runtime_policy,
)


class _FakeProcess:
    """Minimal subprocess shape used by the supervisor."""

    def __init__(self, pid: int, returncode: int | None = None) -> None:
        self.pid = pid
        self.returncode = returncode

    def poll(self) -> int | None:
        """Return the fake process return code."""
        return self.returncode


def _executable(tmp_path: Path) -> Path:
    """Create an executable test file."""
    executable = tmp_path / "temporal"
    executable.write_text("#!/bin/sh\n", encoding="utf-8")
    executable.chmod(0o755)
    return executable


def _policy(tmp_path: Path) -> SidecarRuntimePolicy:
    """Create a deterministic runtime policy for supervisor tests."""
    return build_sidecar_runtime_policy(
        workspace=tmp_path / "workspace",
        runtime_home=tmp_path / "runtime",
        environ={},
    )


def test_build_temporal_start_command_uses_runtime_defaults(
    tmp_path: Path,
) -> None:
    """Start command construction should be centralized and deterministic."""
    executable = _executable(tmp_path)
    policy = _policy(tmp_path)

    assert build_temporal_start_command(
        executable,
        ("--namespace", "histdatacom"),
        runtime_policy=policy,
    ) == (
        str(executable),
        "server",
        "start-dev",
        *policy.temporal_start_args(),
        "--namespace",
        "histdatacom",
    )


def test_start_writes_state_and_is_idempotent_for_running_sidecar(
    tmp_path: Path,
) -> None:
    """A healthy existing sidecar should not spawn a duplicate process."""
    executable = _executable(tmp_path)
    policy = _policy(tmp_path)
    paths = policy.paths
    calls: list[list[str]] = []
    live_pids = {1234}

    def process_factory(command: list[str], **kwargs: object) -> _FakeProcess:
        calls.append(command)
        return _FakeProcess(1234)

    supervisor = SidecarSupervisor(
        runtime_policy=policy,
        process_exists=lambda pid: pid in live_pids,
        process_factory=process_factory,
        port_available=lambda bind_ip, port: True,
    )

    first = supervisor.start(executable=executable)
    second = supervisor.start(executable=executable)
    state = json.loads(paths.pid_file.read_text(encoding="utf-8"))

    assert first.state == "running"
    assert second.message == "Sidecar is already running."
    assert calls == [
        [str(executable), "server", "start-dev", *policy.temporal_start_args()]
    ]
    assert state["schema_version"] == SIDECAR_STATE_SCHEMA_VERSION
    assert state["pids"] == {"server": 1234}
    assert state["runtime_policy"]["paths"]["sqlite_db"] == str(paths.sqlite_db)
    assert paths.runtime_manifest.exists()
    assert not paths.lock_file.exists()


def test_start_repairs_stale_pid_and_lock_files(tmp_path: Path) -> None:
    """Dead PID and lock files should not block a new start."""
    executable = _executable(tmp_path)
    policy = _policy(tmp_path)
    paths = policy.paths
    paths.state_dir.mkdir(parents=True)
    paths.pid_file.write_text(
        json.dumps({"pids": {"server": 111}, "command": ["old"]}),
        encoding="utf-8",
    )
    paths.lock_file.write_text(
        json.dumps({"owner_pid": 222}),
        encoding="utf-8",
    )

    supervisor = SidecarSupervisor(
        runtime_policy=policy,
        process_exists=lambda pid: pid == 333,
        process_factory=lambda command, **kwargs: _FakeProcess(333),
        port_available=lambda bind_ip, port: True,
    )

    status = supervisor.start(executable=executable)
    state = json.loads(paths.pid_file.read_text(encoding="utf-8"))

    assert status.pids == {"server": 333}
    assert state["pids"] == {"server": 333}
    assert not paths.lock_file.exists()


def test_status_reports_stale_state_without_repair(tmp_path: Path) -> None:
    """Status should report stale state without deleting it unless asked."""
    paths = SidecarPaths.from_state_dir(tmp_path / "state")
    paths.state_dir.mkdir(parents=True)
    paths.pid_file.write_text(
        json.dumps(
            {
                "pids": {"server": 999},
                "command": ["/tmp/temporal", "server", "start-dev"],
            }
        ),
        encoding="utf-8",
    )
    supervisor = SidecarSupervisor(paths, process_exists=lambda pid: False)

    status = supervisor.status()

    assert status.state == "stale"
    assert status.pids == {"server": 999}
    assert paths.pid_file.exists()


def test_status_repairs_state_without_valid_pids(tmp_path: Path) -> None:
    """Malformed PID payloads should be stale and repairable."""
    paths = SidecarPaths.from_state_dir(tmp_path / "state")
    paths.state_dir.mkdir(parents=True)
    paths.pid_file.write_text(
        json.dumps({"pids": {"server": "bad"}, "command": ["old"]}),
        encoding="utf-8",
    )
    supervisor = SidecarSupervisor(paths, process_exists=lambda pid: False)

    status = supervisor.status(repair=True)

    assert status.state == "stale"
    assert status.pids == {}
    assert not paths.pid_file.exists()


def test_stop_terminates_all_known_processes_and_removes_state(
    tmp_path: Path,
) -> None:
    """Stopping should not leave known server or worker PIDs orphaned."""
    paths = SidecarPaths.from_state_dir(tmp_path / "state")
    paths.state_dir.mkdir(parents=True)
    paths.pid_file.write_text(
        json.dumps(
            {
                "pids": {"server": 100, "worker": 200},
                "command": ["/tmp/temporal", "server", "start-dev"],
            }
        ),
        encoding="utf-8",
    )
    live_pids = {100, 200}
    terminated: list[int] = []

    def terminate(pid: int) -> None:
        terminated.append(pid)
        live_pids.discard(pid)

    supervisor = SidecarSupervisor(
        paths,
        process_exists=lambda pid: pid in live_pids,
        process_terminate=terminate,
        sleep=lambda seconds: None,
    )

    status = supervisor.stop()

    assert status.state == "stopped"
    assert terminated == [100, 200]
    assert not paths.pid_file.exists()
    assert not paths.lock_file.exists()


def test_default_state_dir_accepts_environment_override(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Environment override should support tests and GUI launchers."""
    from histdatacom.sidecar.supervisor import default_sidecar_state_dir

    monkeypatch.setenv("HISTDATACOM_SIDECAR_HOME", str(tmp_path))
    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True)
    monkeypatch.chdir(workspace)

    assert default_sidecar_state_dir().name == "state"
    assert default_sidecar_state_dir().is_relative_to(tmp_path)
