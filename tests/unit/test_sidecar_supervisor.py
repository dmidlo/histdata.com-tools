"""Tests for Temporal sidecar lifecycle supervision."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from histdatacom.sidecar.supervisor import (
    SIDECAR_STATE_SCHEMA_VERSION,
    SidecarPaths,
    SidecarSupervisor,
    build_sidecar_worker_start_command,
    build_temporal_start_command,
)
from histdatacom.sidecar.queues import (
    TaskQueueLane,
    build_sidecar_worker_config,
)
from histdatacom.sidecar.readiness import write_worker_readiness_payload
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


class _LateCrashingProcess(_FakeProcess):
    """Fake process that survives the launch check and then exits."""

    def __init__(self, pid: int, *, live_polls: int = 1) -> None:
        super().__init__(pid)
        self.live_polls = live_polls
        self.polls = 0

    def poll(self) -> int | None:
        """Return running for a bounded number of polls, then exited."""
        self.polls += 1
        return None if self.polls <= self.live_polls else 1


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


def _supervisor(
    *,
    runtime_policy: SidecarRuntimePolicy | None = None,
    paths: SidecarPaths | None = None,
    process_exists=lambda pid: False,
    process_terminate=lambda pid: None,
    process_factory=lambda command, **kwargs: _FakeProcess(1234),
    worker_lanes=tuple(TaskQueueLane),
) -> SidecarSupervisor:
    """Create a supervisor with deterministic fake readiness probes."""
    return SidecarSupervisor(
        paths,
        runtime_policy=runtime_policy,
        process_exists=process_exists,
        process_terminate=process_terminate,
        process_factory=process_factory,
        port_available=lambda bind_ip, port: True,
        frontend_ready=lambda runtime_policy: True,
        worker_dependency_available=lambda: True,
        sleep=lambda seconds: None,
        worker_lanes=worker_lanes,
    )


def _write_ready_marker(state_dir: Path | str, lane: str, pid: int) -> None:
    """Write a deterministic fake readiness marker."""
    write_worker_readiness_payload(
        state_dir,
        lane,
        {
            "component": f"worker:{lane}",
            "pid": pid,
            "state": "ready",
            "message": "fake worker ready",
            "namespace": "default",
            "task_queue": f"histdatacom.test.{lane}",
            "target_host": "127.0.0.1:17233",
        },
    )


def _write_ready_marker_from_command(command: list[str], pid: int) -> None:
    """Write a fake readiness marker for worker subprocess commands."""
    if "histdatacom.sidecar.worker" not in command or "--lane" not in command:
        return
    lane = command[command.index("--lane") + 1]
    state_dir = command[command.index("--state-dir") + 1]
    _write_ready_marker(state_dir, lane, pid)


def _running_ports(grpc: int = 19999) -> dict[str, object]:
    """Return persisted dynamic sidecar ports for tests."""
    return {
        "bind_ip": "127.0.0.1",
        "grpc": grpc,
        "ui": grpc + 1000,
        "source": "workspace",
        "collisions": [grpc - 1],
    }


def _write_running_state(
    policy: SidecarRuntimePolicy,
    *,
    ports: dict[str, object] | None = None,
    namespace: str = "default",
    task_queue_prefix: str = "histdatacom",
    include_worker_fleet: bool = True,
) -> None:
    """Write a healthy persisted sidecar state file."""
    paths = policy.paths
    paths.state_dir.mkdir(parents=True, exist_ok=True)
    pids = {
        "server": 100,
        "worker:orchestration": 200,
        "worker:network": 300,
        "worker:cpu-file": 400,
        "worker:influx": 500,
    }
    for lane, pid in {
        "orchestration": 200,
        "network": 300,
        "cpu-file": 400,
        "influx": 500,
    }.items():
        _write_ready_marker(paths.state_dir, lane, pid)
    running_ports = ports or _running_ports()
    runtime_policy = policy.to_dict()
    runtime_policy["ports"] = running_ports
    state: dict[str, object] = {
        "schema_version": SIDECAR_STATE_SCHEMA_VERSION,
        "pids": pids,
        "command": ["/tmp/temporal", "server", "start-dev"],
        "ports": running_ports,
        "runtime_policy": runtime_policy,
    }
    if include_worker_fleet:
        config = build_sidecar_worker_config(
            runtime_policy=policy,
            namespace=namespace,
            task_queue_prefix=task_queue_prefix,
            cpu_utilization="high",
            network_multiplier=5,
            orchestration_workers=2,
            influx_workers=3,
        )
        state["worker_fleet"] = {
            "namespace": config.namespace,
            "task_queue_prefix": config.task_queues.prefix,
            "task_queues": config.task_queues.to_dict(),
            "lanes": [lane.value for lane in TaskQueueLane],
            "concurrency": config.concurrency_profile.to_dict(),
        }
    paths.pid_file.write_text(json.dumps(state), encoding="utf-8")


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


def test_build_sidecar_worker_start_command_uses_lane_config(
    tmp_path: Path,
) -> None:
    """Worker subprocess commands should inherit runtime and lane policy."""
    policy = _policy(tmp_path)
    config = build_sidecar_worker_config(
        runtime_policy=policy,
        namespace="histdatacom-test",
        task_queue_prefix="histdatacom-test",
        lane=TaskQueueLane.NETWORK,
        concurrency_overrides={TaskQueueLane.NETWORK: 7},
    )

    command = build_sidecar_worker_start_command(config)

    assert command[:3] == (
        command[0],
        "-m",
        "histdatacom.sidecar.worker",
    )
    assert "--workspace" in command
    assert str(policy.workspace) in command
    assert "--runtime-home" in command
    assert str(policy.runtime_home) in command
    assert "--state-dir" in command
    assert str(policy.paths.state_dir) in command
    assert "--namespace" in command
    assert "histdatacom-test" in command
    assert "--task-queue-prefix" in command
    assert "--lane" in command
    assert command[command.index("--lane") + 1] == "network"
    assert command[command.index("--max-concurrent-activities") + 1] == "7"


def test_start_writes_state_and_is_idempotent_for_running_sidecar(
    tmp_path: Path,
) -> None:
    """A healthy existing sidecar should not spawn a duplicate process."""
    executable = _executable(tmp_path)
    policy = _policy(tmp_path)
    paths = policy.paths
    calls: list[list[str]] = []
    live_pids: set[int] = set()
    next_pid = iter(range(1234, 1240))

    def process_factory(command: list[str], **kwargs: object) -> _FakeProcess:
        pid = next(next_pid)
        live_pids.add(pid)
        calls.append(command)
        _write_ready_marker_from_command(command, pid)
        return _FakeProcess(pid)

    supervisor = _supervisor(
        runtime_policy=policy,
        process_exists=lambda pid: pid in live_pids,
        process_factory=process_factory,
    )

    first = supervisor.start(executable=executable)
    second = supervisor.start(executable=executable)
    state = json.loads(paths.pid_file.read_text(encoding="utf-8"))

    assert first.state == "running"
    assert second.message == "Sidecar is already running."
    assert len(calls) == 5
    assert calls[0] == [
        str(executable),
        "server",
        "start-dev",
        *policy.temporal_start_args(),
    ]
    assert [call[call.index("--lane") + 1] for call in calls[1:]] == [
        "orchestration",
        "network",
        "cpu-file",
        "influx",
    ]
    assert state["schema_version"] == SIDECAR_STATE_SCHEMA_VERSION
    assert state["pids"] == {
        "server": 1234,
        "worker:orchestration": 1235,
        "worker:network": 1236,
        "worker:cpu-file": 1237,
        "worker:influx": 1238,
    }
    assert state["worker_fleet"]["lanes"] == [
        "orchestration",
        "network",
        "cpu-file",
        "influx",
    ]
    assert all(
        readiness["state"] == "ready"
        for readiness in first.worker_readiness.values()
    )
    assert state["worker_readiness"]["network"]["pid"] == 1236
    assert first.to_dict()["worker_readiness"]["network"]["ready"] is True
    assert first.components == {
        "server": "running",
        "worker:orchestration": "running",
        "worker:network": "running",
        "worker:cpu-file": "running",
        "worker:influx": "running",
    }
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
    live_pids: set[int] = set()
    next_pid = iter(range(333, 339))

    def process_factory(command: list[str], **kwargs: object) -> _FakeProcess:
        pid = next(next_pid)
        live_pids.add(pid)
        _write_ready_marker_from_command(command, pid)
        return _FakeProcess(pid)

    supervisor = _supervisor(
        runtime_policy=policy,
        process_exists=lambda pid: pid in live_pids,
        process_factory=process_factory,
    )

    status = supervisor.start(executable=executable)
    state = json.loads(paths.pid_file.read_text(encoding="utf-8"))

    assert status.pids == {
        "server": 333,
        "worker:orchestration": 334,
        "worker:network": 335,
        "worker:cpu-file": 336,
        "worker:influx": 337,
    }
    assert state["pids"] == status.pids
    assert not paths.lock_file.exists()


def test_start_fails_when_worker_lane_never_reports_ready(
    tmp_path: Path,
) -> None:
    """Startup should fail clearly when a live worker never becomes ready."""
    executable = _executable(tmp_path)
    policy = _policy(tmp_path)
    live_pids: set[int] = set()
    terminated: list[int] = []
    next_pid = iter(range(400, 403))

    def process_factory(command: list[str], **kwargs: object) -> _FakeProcess:
        pid = next(next_pid)
        live_pids.add(pid)
        return _FakeProcess(pid)

    def terminate(pid: int) -> None:
        terminated.append(pid)
        live_pids.discard(pid)

    supervisor = _supervisor(
        runtime_policy=policy,
        process_exists=lambda pid: pid in live_pids,
        process_terminate=terminate,
        process_factory=process_factory,
        worker_lanes=(TaskQueueLane.NETWORK,),
    )

    with pytest.raises(
        RuntimeError,
        match="worker lane 'network' did not report readiness",
    ):
        supervisor.start(executable=executable, startup_timeout=0.01)

    assert terminated == [401, 400]
    assert not policy.paths.pid_file.exists()


def test_start_fails_when_worker_crashes_before_ready(
    tmp_path: Path,
) -> None:
    """A worker that exits after launch but before readiness is unhealthy."""
    executable = _executable(tmp_path)
    policy = _policy(tmp_path)
    live_pids = {500, 501}
    calls = 0

    def process_factory(command: list[str], **kwargs: object) -> _FakeProcess:
        nonlocal calls
        calls += 1
        if "histdatacom.sidecar.worker" in command:
            return _LateCrashingProcess(501, live_polls=1)
        return _FakeProcess(500)

    supervisor = _supervisor(
        runtime_policy=policy,
        process_exists=lambda pid: pid in live_pids,
        process_factory=process_factory,
        worker_lanes=(TaskQueueLane.NETWORK,),
    )

    with pytest.raises(
        RuntimeError,
        match="worker lane 'network' exited before readiness",
    ):
        supervisor.start(executable=executable, startup_timeout=0.1)

    assert calls == 2


def test_start_fails_when_required_lane_is_partially_ready(
    tmp_path: Path,
) -> None:
    """Partial worker readiness should not be accepted as a running fleet."""
    executable = _executable(tmp_path)
    policy = _policy(tmp_path)
    live_pids: set[int] = set()
    terminated: list[int] = []
    next_pid = iter(range(600, 604))

    def process_factory(command: list[str], **kwargs: object) -> _FakeProcess:
        pid = next(next_pid)
        live_pids.add(pid)
        if "--lane" in command:
            lane = command[command.index("--lane") + 1]
            if lane == "orchestration":
                _write_ready_marker_from_command(command, pid)
        return _FakeProcess(pid)

    def terminate(pid: int) -> None:
        terminated.append(pid)
        live_pids.discard(pid)

    supervisor = _supervisor(
        runtime_policy=policy,
        process_exists=lambda pid: pid in live_pids,
        process_terminate=terminate,
        process_factory=process_factory,
        worker_lanes=(TaskQueueLane.ORCHESTRATION, TaskQueueLane.NETWORK),
    )

    with pytest.raises(
        RuntimeError,
        match="worker lane 'network' did not report readiness",
    ):
        supervisor.start(executable=executable, startup_timeout=0.01)

    assert terminated == [601, 602, 600]


def test_start_fails_before_server_when_worker_dependency_missing(
    tmp_path: Path,
) -> None:
    """Worker dependency failures should not leave a partial server running."""
    executable = _executable(tmp_path)
    calls: list[list[str]] = []
    supervisor = SidecarSupervisor(
        runtime_policy=_policy(tmp_path),
        process_factory=lambda command, **kwargs: calls.append(command),
        port_available=lambda bind_ip, port: True,
        frontend_ready=lambda runtime_policy: True,
        worker_dependency_available=lambda: False,
        sleep=lambda seconds: None,
    )

    with pytest.raises(RuntimeError, match="temporalio"):
        supervisor.start(executable=executable)

    assert calls == []


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
    supervisor = _supervisor(paths=paths, process_exists=lambda pid: False)

    status = supervisor.status()

    assert status.state == "stale"
    assert status.pids == {"server": 999}
    assert paths.pid_file.exists()


def test_status_reports_stale_state_when_worker_lane_missing(
    tmp_path: Path,
) -> None:
    """A live server without the worker fleet is not a healthy sidecar."""
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
    supervisor = _supervisor(paths=paths, process_exists=lambda pid: True)

    status = supervisor.status()

    assert status.state == "stale"
    assert status.components["server"] == "running"
    assert status.components["worker:network"] == "missing"
    assert "missing components" in status.message


def test_status_reports_stale_state_when_worker_not_ready(
    tmp_path: Path,
) -> None:
    """Live worker PIDs should still be stale until readiness is reported."""
    paths = SidecarPaths.from_state_dir(tmp_path / "state")
    paths.state_dir.mkdir(parents=True)
    paths.pid_file.write_text(
        json.dumps(
            {
                "pids": {"server": 100, "worker:network": 200},
                "command": ["/tmp/temporal", "server", "start-dev"],
            }
        ),
        encoding="utf-8",
    )
    supervisor = _supervisor(
        paths=paths,
        process_exists=lambda pid: True,
        worker_lanes=(TaskQueueLane.NETWORK,),
    )

    status = supervisor.status()

    assert status.state == "stale"
    assert status.components["worker:network"] == "not_ready"
    assert status.worker_readiness["network"]["state"] == "not_ready"
    assert "workers not ready" in status.message


def test_client_worker_config_uses_running_state_ports_and_worker_fleet(
    tmp_path: Path,
) -> None:
    """Client config should use persisted live sidecar routing metadata."""
    policy = _policy(tmp_path)
    _write_running_state(
        policy,
        ports=_running_ports(19999),
        namespace="histdatacom-custom",
        task_queue_prefix="histdatacom-custom",
    )
    supervisor = _supervisor(
        runtime_policy=policy,
        process_exists=lambda pid: True,
    )

    config = supervisor.client_worker_config(require_running=True)

    assert config.target_host == "127.0.0.1:19999"
    assert config.namespace == "histdatacom-custom"
    assert config.task_queues.prefix == "histdatacom-custom"
    assert config.task_queues.orchestration.startswith(
        f"histdatacom-custom.{policy.workspace_id}."
    )
    assert config.concurrency_profile.network_multiplier == 5
    assert config.concurrency_profile.orchestration_workers == 2
    assert config.concurrency_profile.influx_workers == 3


def test_client_worker_config_fails_on_malformed_running_state(
    tmp_path: Path,
) -> None:
    """Running state without worker-fleet metadata should not fall back."""
    policy = _policy(tmp_path)
    _write_running_state(policy, include_worker_fleet=False)
    supervisor = _supervisor(
        runtime_policy=policy,
        process_exists=lambda pid: True,
    )

    with pytest.raises(RuntimeError, match="missing worker_fleet"):
        supervisor.client_worker_config(require_running=True)


def test_status_repairs_state_without_valid_pids(tmp_path: Path) -> None:
    """Malformed PID payloads should be stale and repairable."""
    paths = SidecarPaths.from_state_dir(tmp_path / "state")
    paths.state_dir.mkdir(parents=True)
    paths.pid_file.write_text(
        json.dumps({"pids": {"server": "bad"}, "command": ["old"]}),
        encoding="utf-8",
    )
    supervisor = _supervisor(paths=paths, process_exists=lambda pid: False)

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
                "pids": {
                    "server": 100,
                    "worker:orchestration": 200,
                    "worker:network": 300,
                },
                "command": ["/tmp/temporal", "server", "start-dev"],
            }
        ),
        encoding="utf-8",
    )
    live_pids = {100, 200, 300}
    terminated: list[int] = []

    def terminate(pid: int) -> None:
        terminated.append(pid)
        live_pids.discard(pid)

    supervisor = _supervisor(
        paths=paths,
        process_exists=lambda pid: pid in live_pids,
        process_terminate=terminate,
    )

    status = supervisor.stop()

    assert status.state == "stopped"
    assert terminated == [200, 300, 100]
    assert not paths.pid_file.exists()
    assert not paths.lock_file.exists()


def test_restart_stops_existing_fleet_and_starts_new_fleet(
    tmp_path: Path,
) -> None:
    """Restart should terminate the whole fleet before writing new PIDs."""
    executable = _executable(tmp_path)
    policy = _policy(tmp_path)
    paths = policy.paths
    paths.state_dir.mkdir(parents=True)
    paths.pid_file.write_text(
        json.dumps(
            {
                "pids": {
                    "server": 100,
                    "worker:orchestration": 101,
                    "worker:network": 102,
                    "worker:cpu-file": 103,
                    "worker:influx": 104,
                },
                "command": ["/tmp/temporal", "server", "start-dev"],
            }
        ),
        encoding="utf-8",
    )
    live_pids = {100, 101, 102, 103, 104}
    terminated: list[int] = []
    launched_commands: list[list[str]] = []
    next_pid = iter(range(200, 206))

    def terminate(pid: int) -> None:
        terminated.append(pid)
        live_pids.discard(pid)

    def process_factory(command: list[str], **kwargs: object) -> _FakeProcess:
        pid = next(next_pid)
        live_pids.add(pid)
        launched_commands.append(command)
        _write_ready_marker_from_command(command, pid)
        return _FakeProcess(pid)

    supervisor = _supervisor(
        runtime_policy=policy,
        process_exists=lambda pid: pid in live_pids,
        process_terminate=terminate,
        process_factory=process_factory,
    )

    status = supervisor.restart(executable=executable)
    state = json.loads(paths.pid_file.read_text(encoding="utf-8"))

    assert terminated == [101, 102, 103, 104, 100]
    assert len(launched_commands) == 5
    assert status.pids == {
        "server": 200,
        "worker:orchestration": 201,
        "worker:network": 202,
        "worker:cpu-file": 203,
        "worker:influx": 204,
    }
    assert state["pids"] == status.pids


def test_doctor_reports_frontend_and_worker_lane_health(
    tmp_path: Path,
) -> None:
    """Doctor should expose server, frontend, and lane-level worker status."""
    paths = SidecarPaths.from_state_dir(tmp_path / "state")
    paths.state_dir.mkdir(parents=True)
    paths.pid_file.write_text(
        json.dumps(
            {
                "pids": {
                    "server": 100,
                    "worker:orchestration": 200,
                    "worker:network": 300,
                    "worker:cpu-file": 400,
                    "worker:influx": 500,
                },
                "command": ["/tmp/temporal", "server", "start-dev"],
            }
        ),
        encoding="utf-8",
    )
    for lane, pid in {
        "orchestration": 200,
        "network": 300,
        "cpu-file": 400,
        "influx": 500,
    }.items():
        _write_ready_marker(paths.state_dir, lane, pid)
    supervisor = _supervisor(paths=paths, process_exists=lambda pid: True)

    doctor = supervisor.doctor()

    assert doctor["frontend"]["ready"] is True
    assert doctor["components"]["server"] == "running"
    assert doctor["workers"]["orchestration"]["state"] == "running"
    assert doctor["workers"]["orchestration"]["ready"] is True
    assert doctor["workers"]["orchestration"]["readiness_state"] == "ready"
    assert doctor["workers"]["network"]["component"] == "worker:network"
    assert doctor["workers"]["cpu-file"]["log"].endswith(
        "temporal-worker-cpu-file.log"
    )
    assert doctor["persistence"]["sidecar_state"]["state"] == (
        "legacy_unversioned"
    )
    assert doctor["persistence"]["sidecar_state"]["schema_version"] == 0


def test_future_sidecar_state_schema_is_reported_stale(
    tmp_path: Path,
) -> None:
    """Newer state JSON should fail clearly without deleting state."""
    policy = _policy(tmp_path)
    policy.paths.state_dir.mkdir(parents=True)
    policy.paths.pid_file.write_text(
        json.dumps(
            {
                "schema_version": SIDECAR_STATE_SCHEMA_VERSION + 1,
                "pids": {"server": 100},
                "command": ["/tmp/temporal", "server", "start-dev"],
            }
        ),
        encoding="utf-8",
    )
    supervisor = _supervisor(
        runtime_policy=policy,
        process_exists=lambda pid: True,
    )

    status = supervisor.status()
    doctor = supervisor.doctor()

    assert status.state == "stale"
    assert "Unsupported sidecar state schema version" in status.message
    assert policy.paths.pid_file.exists()
    assert doctor["persistence"]["sidecar_state"]["state"] == "unsupported"
    assert doctor["persistence"]["sidecar_state"]["schema_version"] == (
        SIDECAR_STATE_SCHEMA_VERSION + 1
    )


def test_doctor_checks_persisted_running_frontend_port(
    tmp_path: Path,
) -> None:
    """Doctor should inspect the actual frontend port in running state."""
    policy = _policy(tmp_path)
    _write_running_state(policy, ports=_running_ports(20123))
    checked_ports: list[int] = []

    def frontend_ready(runtime_policy: SidecarRuntimePolicy) -> bool:
        checked_ports.append(runtime_policy.ports.grpc)
        return True

    supervisor = SidecarSupervisor(
        runtime_policy=policy,
        process_exists=lambda pid: True,
        process_factory=lambda command, **kwargs: _FakeProcess(1234),
        port_available=lambda bind_ip, port: True,
        frontend_ready=frontend_ready,
        worker_dependency_available=lambda: True,
        sleep=lambda seconds: None,
    )

    doctor = supervisor.doctor()

    assert checked_ports == [20123]
    assert doctor["frontend"]["target_host"] == "127.0.0.1:20123"
    assert doctor["runtime_policy"]["ports"]["grpc"] == 20123


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
