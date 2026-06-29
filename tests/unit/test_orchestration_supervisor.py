"""Tests for Temporal orchestration lifecycle supervision."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
import json
import subprocess
from pathlib import Path

import pytest

import histdatacom.orchestration.supervisor as supervisor_module
from histdatacom.orchestration.supervisor import (
    ORCHESTRATION_STATE_SCHEMA_VERSION,
    OrchestrationPaths,
    OrchestrationSupervisor,
    build_temporal_namespace_create_command,
    build_temporal_namespace_describe_command,
    build_orchestration_worker_start_command,
    build_temporal_start_command,
)
from histdatacom.orchestration.queues import (
    TaskQueueLane,
    build_orchestration_worker_config,
)
from histdatacom.orchestration.readiness import write_worker_readiness_payload
from histdatacom.orchestration.runtime import (
    OrchestrationRuntimePolicy,
    build_orchestration_runtime_policy,
)
from histdatacom.orchestration.resources import TemporalRuntimeResolution


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


def _policy(tmp_path: Path) -> OrchestrationRuntimePolicy:
    """Create a deterministic runtime policy for supervisor tests."""
    return build_orchestration_runtime_policy(
        workspace=tmp_path / "workspace",
        runtime_home=tmp_path / "runtime",
        environ={},
    )


def _missing_process(_pid: int) -> bool:
    return False


def _noop_process(_pid: int) -> None:
    return None


def _default_process_factory(
    command: list[str],
    **kwargs: object,
) -> _FakeProcess:
    return _FakeProcess(1234)


def _default_command_runner(
    command: list[str],
    **kwargs: object,
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(command, 0, stdout="", stderr="")


def _port_available(_bind_ip: str, _port: int) -> bool:
    return True


def _frontend_ready(_runtime_policy: OrchestrationRuntimePolicy) -> bool:
    return True


def _worker_dependency_available() -> bool:
    return True


def _sleep(_seconds: float) -> None:
    return None


def _supervisor(
    *,
    runtime_policy: OrchestrationRuntimePolicy | None = None,
    paths: OrchestrationPaths | None = None,
    process_exists: Callable[[int], bool] = _missing_process,
    process_terminate: Callable[[int], None] = _noop_process,
    process_kill: Callable[[int], None] = _noop_process,
    process_factory: Callable[..., _FakeProcess] = _default_process_factory,
    command_runner: Callable[
        ...,
        subprocess.CompletedProcess[str],
    ] = _default_command_runner,
    worker_lanes: tuple[TaskQueueLane, ...] = tuple(TaskQueueLane),
    namespace: str = "default",
    task_queue_prefix: str = "histdatacom",
) -> OrchestrationSupervisor:
    """Create a supervisor with deterministic fake readiness probes."""
    return OrchestrationSupervisor(
        paths,
        runtime_policy=runtime_policy,
        process_exists=process_exists,
        process_terminate=process_terminate,
        process_kill=process_kill,
        process_factory=process_factory,
        command_runner=command_runner,
        port_available=_port_available,
        frontend_ready=_frontend_ready,
        worker_dependency_available=_worker_dependency_available,
        sleep=_sleep,
        worker_lanes=worker_lanes,
        namespace=namespace,
        task_queue_prefix=task_queue_prefix,
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
    if (
        "histdatacom.orchestration.worker" not in command
        or "--lane" not in command
    ):
        return
    lane = command[command.index("--lane") + 1]
    state_dir = command[command.index("--state-dir") + 1]
    _write_ready_marker(state_dir, lane, pid)


def _running_ports(grpc: int = 19999) -> dict[str, object]:
    """Return persisted dynamic orchestration ports for tests."""
    return {
        "bind_ip": "127.0.0.1",
        "grpc": grpc,
        "ui": grpc + 1000,
        "source": "workspace",
        "collisions": [grpc - 1],
    }


def _write_running_state(
    policy: OrchestrationRuntimePolicy,
    *,
    ports: dict[str, object] | None = None,
    namespace: str = "default",
    task_queue_prefix: str = "histdatacom",
    include_worker_fleet: bool = True,
) -> None:
    """Write a healthy persisted orchestration state file."""
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
        "schema_version": ORCHESTRATION_STATE_SCHEMA_VERSION,
        "pids": pids,
        "command": ["/tmp/temporal", "server", "start-dev"],
        "ports": running_ports,
        "runtime_policy": runtime_policy,
    }
    if include_worker_fleet:
        config = build_orchestration_worker_config(
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


def test_build_temporal_namespace_commands_use_runtime_address(
    tmp_path: Path,
) -> None:
    """Namespace provisioning should target the selected local frontend."""
    executable = _executable(tmp_path)
    policy = _policy(tmp_path)
    target_host = f"{policy.ports.bind_ip}:{policy.ports.grpc}"

    assert build_temporal_namespace_describe_command(
        executable,
        "histdatacom-smoke",
        runtime_policy=policy,
    ) == (
        str(executable),
        "operator",
        "namespace",
        "describe",
        "--address",
        target_host,
        "--namespace",
        "histdatacom-smoke",
        "--command-timeout",
        "10s",
    )
    assert build_temporal_namespace_create_command(
        executable,
        "histdatacom-smoke",
        runtime_policy=policy,
    ) == (
        str(executable),
        "operator",
        "namespace",
        "create",
        "--address",
        target_host,
        "--namespace",
        "histdatacom-smoke",
        "--command-timeout",
        "10s",
    )


def test_build_orchestration_worker_start_command_uses_lane_config(
    tmp_path: Path,
) -> None:
    """Worker subprocess commands should inherit runtime and lane policy."""
    policy = _policy(tmp_path)
    config = build_orchestration_worker_config(
        runtime_policy=policy,
        namespace="histdatacom-test",
        task_queue_prefix="histdatacom-test",
        lane=TaskQueueLane.NETWORK,
        concurrency_overrides={TaskQueueLane.NETWORK: 7},
    )

    command = build_orchestration_worker_start_command(config)

    assert command[:3] == (
        command[0],
        "-m",
        "histdatacom.orchestration.worker",
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


def test_start_writes_state_and_is_idempotent_for_running_orchestration(
    tmp_path: Path,
) -> None:
    """A healthy existing orchestration should not spawn a duplicate process."""
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
    assert second.message == "Orchestration is already running."
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
    assert state["schema_version"] == ORCHESTRATION_STATE_SCHEMA_VERSION
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


def test_start_without_explicit_executable_uses_runtime_resolver(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Default startup should resolve the Temporal binary through provisioning."""
    executable = _executable(tmp_path)
    policy = _policy(tmp_path)
    calls: list[list[str]] = []
    live_pids: set[int] = set()
    next_pid = iter(range(2200, 2202))

    @contextmanager
    def fake_resolver() -> Iterator[TemporalRuntimeResolution]:
        yield TemporalRuntimeResolution(
            executable=executable,
            source="cache",
            platform_key="linux-x86_64",
            version="1.7.2",
            archive_sha256="abc",
            cache_entry=tmp_path / "cache-entry",
            provenance_path=tmp_path / "cache-entry" / "provenance.json",
        )

    def process_factory(command: list[str], **kwargs: object) -> _FakeProcess:
        pid = next(next_pid)
        live_pids.add(pid)
        calls.append(command)
        _write_ready_marker_from_command(command, pid)
        return _FakeProcess(pid)

    monkeypatch.setattr(
        supervisor_module,
        "temporal_runtime_executable_path",
        fake_resolver,
    )
    supervisor = _supervisor(
        runtime_policy=policy,
        process_exists=lambda pid: pid in live_pids,
        process_factory=process_factory,
        worker_lanes=(TaskQueueLane.NETWORK,),
    )

    status = supervisor.start()
    state = json.loads(policy.paths.pid_file.read_text(encoding="utf-8"))

    assert status.state == "running"
    assert calls[0][0] == str(executable)
    assert state["runtime_resolution"]["source"] == "cache"
    assert state["runtime_resolution"]["cache_entry"] == str(
        tmp_path / "cache-entry"
    )


def test_start_creates_non_default_namespace_before_workers(
    tmp_path: Path,
) -> None:
    """Custom local namespaces should be provisioned before workers connect."""
    executable = _executable(tmp_path)
    policy = _policy(tmp_path)
    events: list[tuple[str, str]] = []
    live_pids: set[int] = set()
    next_pid = iter(range(2000, 2003))

    def process_factory(command: list[str], **kwargs: object) -> _FakeProcess:
        pid = next(next_pid)
        live_pids.add(pid)
        label = "server"
        if "histdatacom.orchestration.worker" in command:
            label = f"worker:{command[command.index('--lane') + 1]}"
            _write_ready_marker_from_command(command, pid)
        events.append(("process", label))
        return _FakeProcess(pid)

    def command_runner(
        command: list[str],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        action = command[command.index("namespace") + 1]
        events.append(("namespace", action))
        return subprocess.CompletedProcess(
            command,
            1 if action == "describe" else 0,
            stdout="",
            stderr="not found" if action == "describe" else "",
        )

    supervisor = _supervisor(
        runtime_policy=policy,
        process_exists=lambda pid: pid in live_pids,
        process_factory=process_factory,
        command_runner=command_runner,
        namespace="histdatacom-smoke",
        task_queue_prefix="histdatacom-smoke",
        worker_lanes=(TaskQueueLane.NETWORK,),
    )

    status = supervisor.start(executable=executable)
    state = json.loads(policy.paths.pid_file.read_text(encoding="utf-8"))

    assert status.state == "running"
    assert events == [
        ("process", "server"),
        ("namespace", "describe"),
        ("namespace", "create"),
        ("process", "worker:network"),
    ]
    assert state["worker_fleet"]["namespace"] == "histdatacom-smoke"
    assert state["worker_fleet"]["task_queue_prefix"] == "histdatacom-smoke"


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
        if "histdatacom.orchestration.worker" in command:
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
    supervisor = OrchestrationSupervisor(
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
    paths = OrchestrationPaths.from_state_dir(tmp_path / "state")
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


def test_status_reports_posix_disk_headroom(tmp_path: Path) -> None:
    """Runtime status should expose actual filesystem write headroom."""
    supervisor = _supervisor(runtime_policy=_policy(tmp_path))

    payload = supervisor.status().to_dict()

    disk = payload["disk"]
    assert disk["semantics"] == "posix_write_available"
    assert disk["free_bytes"] >= 0
    assert disk["total_bytes"] >= disk["free_bytes"]
    assert "purgeable" in disk["note"]


def test_status_reports_stale_state_when_worker_lane_missing(
    tmp_path: Path,
) -> None:
    """A live server without the worker fleet is not a healthy orchestration."""
    paths = OrchestrationPaths.from_state_dir(tmp_path / "state")
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
    paths = OrchestrationPaths.from_state_dir(tmp_path / "state")
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
    """Client config should use persisted live orchestration routing metadata."""
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
    paths = OrchestrationPaths.from_state_dir(tmp_path / "state")
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
    paths = OrchestrationPaths.from_state_dir(tmp_path / "state")
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


def test_stop_force_kills_processes_that_ignore_termination(
    tmp_path: Path,
) -> None:
    """Stopping should escalate when known processes survive SIGTERM."""
    paths = OrchestrationPaths.from_state_dir(tmp_path / "state")
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
    killed: list[int] = []

    def terminate(pid: int) -> None:
        terminated.append(pid)

    def kill(pid: int) -> None:
        killed.append(pid)
        live_pids.discard(pid)

    supervisor = _supervisor(
        paths=paths,
        process_exists=lambda pid: pid in live_pids,
        process_terminate=terminate,
        process_kill=kill,
    )

    status = supervisor.stop(stop_timeout=0.0)

    assert status.state == "stopped"
    assert terminated == [200, 300, 100]
    assert killed == [200, 300, 100]
    assert not paths.pid_file.exists()
    assert not paths.lock_file.exists()


def test_process_exists_treats_reaped_child_pid_as_dead(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exited children should not keep shutdown status in a running state."""
    kill_calls: list[tuple[int, int]] = []

    monkeypatch.setattr(
        supervisor_module,
        "_reap_child_process",
        lambda pid: pid == 700,
    )
    monkeypatch.setattr(
        supervisor_module.os,
        "kill",
        lambda pid, sig: kill_calls.append((pid, sig)),
    )

    assert not supervisor_module._process_exists(700)
    assert kill_calls == []


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
    paths = OrchestrationPaths.from_state_dir(tmp_path / "state")
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
    assert doctor["persistence"]["orchestration_state"]["state"] == (
        "legacy_unversioned"
    )
    assert doctor["persistence"]["orchestration_state"]["schema_version"] == 0


def test_future_orchestration_state_schema_is_reported_stale(
    tmp_path: Path,
) -> None:
    """Newer state JSON should fail clearly without deleting state."""
    policy = _policy(tmp_path)
    policy.paths.state_dir.mkdir(parents=True)
    policy.paths.pid_file.write_text(
        json.dumps(
            {
                "schema_version": ORCHESTRATION_STATE_SCHEMA_VERSION + 1,
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
    assert "Unsupported orchestration state schema version" in status.message
    assert policy.paths.pid_file.exists()
    assert (
        doctor["persistence"]["orchestration_state"]["state"] == "unsupported"
    )
    assert doctor["persistence"]["orchestration_state"]["schema_version"] == (
        ORCHESTRATION_STATE_SCHEMA_VERSION + 1
    )


def test_doctor_checks_persisted_running_frontend_port(
    tmp_path: Path,
) -> None:
    """Doctor should inspect the actual frontend port in running state."""
    policy = _policy(tmp_path)
    _write_running_state(policy, ports=_running_ports(20123))
    checked_ports: list[int] = []

    def frontend_ready(runtime_policy: OrchestrationRuntimePolicy) -> bool:
        checked_ports.append(runtime_policy.ports.grpc)
        return True

    supervisor = OrchestrationSupervisor(
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
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Environment override should support tests and GUI launchers."""
    from histdatacom.orchestration.supervisor import (
        default_orchestration_state_dir,
    )

    monkeypatch.setenv("HISTDATACOM_RUNTIME_HOME", str(tmp_path))
    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True)
    monkeypatch.chdir(workspace)

    assert default_orchestration_state_dir().name == "state"
    assert default_orchestration_state_dir().is_relative_to(tmp_path)
