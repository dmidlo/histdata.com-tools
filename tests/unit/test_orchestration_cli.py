"""Tests for Temporal orchestration lifecycle CLI wiring."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from histdatacom.runtime_contracts import RunRequest, WorkStatus
from histdatacom.runtime_contracts import ArtifactRef, StatusEvent
from histdatacom.orchestration import client as orchestration_client
from histdatacom.orchestration import cli
from histdatacom.orchestration.control import (
    JobLifecycle,
    JobProgressSnapshot,
    OrchestrationJobList,
    OrchestrationJobSnapshot,
)
from histdatacom.orchestration.queues import build_orchestration_worker_config
from histdatacom.orchestration.runtime import build_orchestration_runtime_policy


class _StatusOnlySupervisor:
    """Test double for runtime CLI dispatch."""

    def __init__(self, state: str = "stopped") -> None:
        self.state = state

    def status(self, repair: bool = False):
        """Return a fake status object."""
        return _FakeStatus(self.state)

    def doctor(self) -> dict:
        """Return fake diagnostics."""
        return {
            "status": _FakeStatus(self.state).to_dict(),
            "platform": {"message": "ok"},
            "runtime_policy": {
                "ports": {"bind_ip": "127.0.0.1", "grpc": 17233, "ui": 18233}
            },
        }


class _LifecycleSupervisor(_StatusOnlySupervisor):
    """Test double for mutating lifecycle commands."""

    def __init__(self) -> None:
        super().__init__("stopped")
        self.calls: list[tuple[str, dict]] = []

    def start(self, **kwargs: object):
        """Record a start call."""
        self.calls.append(("start", kwargs))
        return _FakeStatus("running")

    def stop(self, **kwargs: object):
        """Record a stop call."""
        self.calls.append(("stop", kwargs))
        return _FakeStatus("stopped")

    def restart(self, **kwargs: object):
        """Record a restart call."""
        self.calls.append(("restart", kwargs))
        return _FakeStatus("running")


class _MaintenanceSupervisor(_StatusOnlySupervisor):
    """Test double for orchestration maintenance commands."""

    def __init__(self, runtime_policy, state: str = "stopped") -> None:
        super().__init__(state)
        self.runtime_policy = runtime_policy


class _FakeStatus:
    """Minimal status object consumed by the CLI."""

    def __init__(self, state: str) -> None:
        self.state = state
        self.message = f"{state} message"

    def to_dict(self) -> dict:
        """Return fake status payload."""
        return {
            "state": self.state,
            "message": self.message,
            "state_dir": "/tmp/runtime",
            "pid_file": "/tmp/runtime/runtime.pid.json",
            "lock_file": "/tmp/runtime/runtime.lock",
            "logs": {},
            "pids": {},
            "command": [],
            "ports": {"bind_ip": "127.0.0.1", "grpc": 17233, "ui": 18233},
        }


class _FakeConfig:
    """Minimal worker config marker for CLI control tests."""


class _Handle:
    """Minimal job handle shape for snapshots."""

    request_id = "run-cli"
    workflow_id = "histdatacom-run-cli"
    run_id = "run-fake"
    task_queue = "histdatacom.test.orchestration"
    namespace = "default"


def _snapshot(
    *,
    lifecycle: JobLifecycle = JobLifecycle.RUNNING,
    status: WorkStatus = WorkStatus.UNKNOWN,
) -> OrchestrationJobSnapshot:
    """Return a fake control snapshot."""
    return OrchestrationJobSnapshot.from_handle(
        _Handle(),
        lifecycle=lifecycle,
        status=status,
    )


def _snapshot_with_progress(
    *,
    status: WorkStatus = WorkStatus.UNKNOWN,
) -> OrchestrationJobSnapshot:
    """Return a fake control snapshot with progress telemetry."""
    progress = JobProgressSnapshot(
        workflow_name="HistDataRunWorkflow",
        request_id="run-cli",
        status=status,
        current_stage="download EURUSD M1 2024",
        total_children=4,
        completed_children=2,
        unit="datasets",
        started_at_utc="2026-06-25T12:00:00Z",
        updated_at_utc="2026-06-25T12:00:02Z",
        rate_per_second=1.0,
        planned_children=(
            "plan",
            "download EURUSD M1 2024",
            "quality",
            "cache",
        ),
        completed_stages=("plan",),
        events=(
            StatusEvent(
                status=WorkStatus.UNKNOWN,
                stage="plan",
                message="Planned 4 datasets.",
                timestamp_utc="2026-06-25T12:00:00Z",
            ),
            StatusEvent(
                status=WorkStatus.UNKNOWN,
                stage="download EURUSD M1 2024",
                message="Downloading CSV ZIP.",
                timestamp_utc="2026-06-25T12:00:02Z",
            ),
        ),
        artifacts=(
            ArtifactRef(
                kind="cache",
                path="/tmp/histdatacom/EURUSD.csv",
                size_bytes=4096,
                sha256="0123456789abcdef",
            ),
        ),
    )
    return _snapshot(status=status).with_progress(progress)


def test_orchestration_status_cli_emits_json(monkeypatch, capsys) -> None:
    """Orchestration status should be available as a first-class CLI command."""
    monkeypatch.setattr(
        cli, "_supervisor", lambda args: _StatusOnlySupervisor("running")
    )

    exit_code = cli.main(["status", "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["state"] == "running"


def test_orchestration_doctor_cli_returns_diagnostics(
    monkeypatch, capsys
) -> None:
    """Doctor should expose orchestration diagnostics for humans and tools."""
    monkeypatch.setattr(
        cli, "_supervisor", lambda args: _StatusOnlySupervisor("stopped")
    )

    exit_code = cli.main(["doctor"])
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "stopped message" in output
    assert "ok" in output


def test_orchestration_lifecycle_cli_commands_delegate_to_supervisor(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Start, stop, and restart should be first-class runtime commands."""
    supervisor = _LifecycleSupervisor()
    monkeypatch.setattr(cli, "_supervisor", lambda args: supervisor)

    assert (
        cli.main(
            [
                "--state-dir",
                str(tmp_path),
                "start",
                "--executable",
                "/tmp/temporal",
                "--",
                "--namespace",
                "histdatacom",
            ]
        )
        == 0
    )
    assert cli.main(["--state-dir", str(tmp_path), "stop"]) == 0
    assert (
        cli.main(
            [
                "--state-dir",
                str(tmp_path),
                "restart",
                "--executable",
                "/tmp/temporal",
            ]
        )
        == 0
    )

    assert [call[0] for call in supervisor.calls] == [
        "start",
        "stop",
        "restart",
    ]
    assert supervisor.calls[0][1]["executable"] == "/tmp/temporal"
    assert supervisor.calls[0][1]["extra_args"] == (
        "--namespace",
        "histdatacom",
    )


def test_orchestration_start_supervisor_inherits_worker_fleet_options(
    tmp_path: Path,
) -> None:
    """Lifecycle worker flags should flow into the constructed supervisor."""
    args = cli.build_parser().parse_args(
        [
            "--state-dir",
            str(tmp_path / "state"),
            "start",
            "--namespace",
            "histdatacom-test",
            "--task-queue-prefix",
            "histdatacom-test",
            "--cpu-utilization",
            "high",
            "--network-multiplier",
            "5",
            "--orchestration-workers",
            "2",
            "--influx-workers",
            "3",
        ]
    )

    supervisor = cli._supervisor(args)

    assert supervisor.namespace == "histdatacom-test"
    assert supervisor.task_queue_prefix == "histdatacom-test"
    assert supervisor.cpu_utilization == "high"
    assert supervisor.network_multiplier == 5
    assert supervisor.orchestration_workers == 2
    assert supervisor.influx_workers == 3


def test_orchestration_maintenance_cli_emits_json(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Maintenance should expose a GUI-ready JSON payload."""
    runtime_policy = build_orchestration_runtime_policy(
        workspace=tmp_path / "workspace",
        runtime_home=tmp_path / "runtime",
    )
    runtime_policy.paths.logs_dir.mkdir(parents=True)
    runtime_policy.paths.server_log.write_text("x" * 16, encoding="utf-8")
    monkeypatch.setattr(
        cli,
        "_supervisor",
        lambda args: _MaintenanceSupervisor(runtime_policy),
    )

    exit_code = cli.main(
        [
            "maintenance",
            "--json",
            "--max-log-bytes",
            "4",
            "--max-rotated-logs",
            "1",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["state"] == "completed"
    assert payload["logs"][0]["action"] == "rotated"
    assert payload["downloaded_artifacts_removed"] is False


def test_histdatacom_main_dispatches_jobs_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The top-level histdatacom command should route job telemetry."""
    import histdatacom.histdata_com as histdata_com
    import histdatacom.orchestration.cli as orchestration_cli

    captured: dict[str, tuple[str, ...]] = {}

    def fake_jobs_main(argv: list[str]) -> int:
        captured["argv"] = tuple(argv)
        return 0

    monkeypatch.setattr(orchestration_cli, "jobs_main", fake_jobs_main)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "jobs",
            "list",
            "--json",
        ],
    )

    assert histdata_com.main() == 0
    assert captured["argv"] == ("list", "--json")


def test_histdatacom_main_dispatches_jobs_command_after_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The router should allow --config before a routed command."""
    import histdatacom.histdata_com as histdata_com
    import histdatacom.orchestration.cli as orchestration_cli

    config_path = tmp_path / "histdatacom.yaml"
    captured: dict[str, tuple[str, ...]] = {}

    def fake_jobs_main(argv: list[str]) -> int:
        captured["argv"] = tuple(argv)
        return 0

    monkeypatch.setattr(orchestration_cli, "jobs_main", fake_jobs_main)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "--config",
            str(config_path),
            "jobs",
            "list",
            "--json",
        ],
    )

    assert histdata_com.main() == 0
    assert captured["argv"] == (
        "--config",
        str(config_path),
        "list",
        "--json",
    )


def test_histdatacom_main_dispatches_runtime_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The top-level histdatacom command should route runtime diagnostics."""
    import histdatacom.histdata_com as histdata_com
    import histdatacom.orchestration.cli as orchestration_cli

    captured: dict[str, tuple[str, ...]] = {}

    def fake_runtime_main(argv: list[str]) -> int:
        captured["argv"] = tuple(argv)
        return 0

    monkeypatch.setattr(orchestration_cli, "main", fake_runtime_main)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "runtime",
            "status",
            "--json",
        ],
    )

    assert histdata_com.main() == 0
    assert captured["argv"] == ("status", "--json")


def test_histdatacom_main_does_not_route_orchestration_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The top-level user CLI should not expose orchestration lifecycle commands."""
    import histdatacom.histdata_com as histdata_com

    monkeypatch.setattr(sys, "argv", ["histdatacom", "orchestration", "status"])

    with pytest.raises(SystemExit) as err:
        histdata_com.main()

    assert err.value.code == 2


def test_orchestration_jobs_inspect_cli_emits_control_snapshot_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Jobs inspect should return the GUI-ready snapshot payload."""
    monkeypatch.setattr(
        cli,
        "_supervisor",
        lambda args: _StatusOnlySupervisor("running"),
    )
    monkeypatch.setattr(cli, "_worker_config", lambda args: _FakeConfig())
    monkeypatch.setattr(
        cli,
        "inspect_job_status_sync",
        lambda workflow_id, **kwargs: _snapshot(),
    )

    exit_code = cli.main(["jobs", "--json", "inspect", "histdatacom-run-cli"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["workflow_id"] == "histdatacom-run-cli"
    assert payload["lifecycle"] == JobLifecycle.RUNNING.value


def test_orchestration_jobs_progress_cli_renders_rich_dashboard(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Jobs progress should default to a human Rich progress dashboard."""
    monkeypatch.setattr(
        cli,
        "_supervisor",
        lambda args: _StatusOnlySupervisor("running"),
    )
    monkeypatch.setattr(cli, "_worker_config", lambda args: _FakeConfig())
    monkeypatch.setattr(
        cli,
        "inspect_job_status_sync",
        lambda workflow_id, **kwargs: _snapshot_with_progress(),
    )

    exit_code = cli.main(["jobs", "progress", "histdatacom-run-cli"])
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "HistData job progress" in output
    assert "download EURUSD M1 2024" in output
    assert "2/4 datasets" in output
    assert "Recent Events" in output
    assert "/tmp/histdatacom/EURUSD.csv" in output


def test_orchestration_jobs_progress_json_keeps_machine_payload(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Jobs progress JSON should stay stable for automation."""
    monkeypatch.setattr(
        cli,
        "_supervisor",
        lambda args: _StatusOnlySupervisor("running"),
    )
    monkeypatch.setattr(cli, "_worker_config", lambda args: _FakeConfig())
    monkeypatch.setattr(
        cli,
        "inspect_job_status_sync",
        lambda workflow_id, **kwargs: _snapshot_with_progress(),
    )

    exit_code = cli.main(["jobs", "--json", "progress", "histdatacom-run-cli"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["workflow_id"] == "histdatacom-run-cli"
    assert payload["progress"]["completed_children"] == 2
    assert payload["progress"]["current_stage"] == "download EURUSD M1 2024"


def test_orchestration_jobs_progress_watch_uses_live_renderer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Progress watch should delegate polling to the Rich live renderer."""
    captured: dict[str, object] = {}

    def fake_watch(fetch_snapshot, *, interval_seconds: float):
        captured["snapshot"] = fetch_snapshot()
        captured["interval_seconds"] = interval_seconds
        return captured["snapshot"]

    monkeypatch.setattr(
        cli,
        "_supervisor",
        lambda args: _StatusOnlySupervisor("running"),
    )
    monkeypatch.setattr(cli, "_worker_config", lambda args: _FakeConfig())
    monkeypatch.setattr(
        cli,
        "inspect_job_status_sync",
        lambda workflow_id, **kwargs: _snapshot_with_progress(
            status=WorkStatus.COMPLETED
        ),
    )
    monkeypatch.setattr(cli, "watch_job_progress", fake_watch)

    exit_code = cli.main(
        [
            "jobs",
            "progress",
            "histdatacom-run-cli",
            "--watch",
            "--interval",
            "0.25",
        ]
    )

    assert exit_code == 0
    assert captured["interval_seconds"] == 0.25
    assert isinstance(captured["snapshot"], OrchestrationJobSnapshot)


def test_orchestration_jobs_cli_resolves_config_from_running_supervisor(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Jobs commands should use the running orchestration config resolver."""
    supervisor = _StatusOnlySupervisor("running")
    resolved_config = _FakeConfig()
    captured: dict[str, object] = {}

    def fake_resolve_config(**kwargs: object) -> _FakeConfig:
        captured["resolver_kwargs"] = kwargs
        return resolved_config

    def fake_inspect(
        workflow_id: str,
        **kwargs: object,
    ) -> OrchestrationJobSnapshot:
        captured["workflow_id"] = workflow_id
        captured["inspect_kwargs"] = kwargs
        return _snapshot()

    monkeypatch.setattr(cli, "_supervisor", lambda args: supervisor)
    monkeypatch.setattr(
        cli,
        "resolve_orchestration_worker_config",
        fake_resolve_config,
    )
    monkeypatch.setattr(cli, "inspect_job_status_sync", fake_inspect)

    exit_code = cli.main(["jobs", "--json", "inspect", "histdatacom-run-cli"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["workflow_id"] == "histdatacom-run-cli"
    assert captured["resolver_kwargs"] == {"supervisor": supervisor}
    inspect_kwargs = captured["inspect_kwargs"]
    assert isinstance(inspect_kwargs, dict)
    assert inspect_kwargs["config"] is resolved_config
    assert inspect_kwargs["supervisor"] is supervisor


@pytest.mark.parametrize(
    "argv",
    (
        ["jobs", "--json", "list"],
        ["jobs", "list", "--json"],
    ),
)
def test_orchestration_jobs_list_accepts_json_before_or_after_subcommand(
    argv: list[str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Documented job commands should accept JSON flags naturally."""
    monkeypatch.setattr(
        cli,
        "_supervisor",
        lambda args: _StatusOnlySupervisor("running"),
    )
    monkeypatch.setattr(cli, "_worker_config", lambda args: _FakeConfig())
    monkeypatch.setattr(
        cli,
        "list_job_statuses_sync",
        lambda **kwargs: OrchestrationJobList(jobs=(_snapshot(),)),
    )

    exit_code = cli.main(argv)
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["jobs"][0]["workflow_id"] == "histdatacom-run-cli"


def test_orchestration_jobs_cli_reads_yaml_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Issue #31: jobs commands should accept recurrent YAML defaults."""
    captured: dict[str, object] = {}
    config_path = tmp_path / "histdatacom.yaml"
    config_path.write_text(
        """
histdatacom:
  jobs:
    command: list
    json: true
    offline: true
    query: WorkflowType = "HistDataRunWorkflow"
    limit: 7
""",
        encoding="utf-8",
    )

    def fake_list_jobs(**kwargs: object) -> OrchestrationJobList:
        captured.update(kwargs)
        return OrchestrationJobList(jobs=(_snapshot(),))

    monkeypatch.setattr(
        cli,
        "_supervisor",
        lambda args: _StatusOnlySupervisor("running"),
    )
    monkeypatch.setattr(cli, "_worker_config", lambda args: _FakeConfig())
    monkeypatch.setattr(cli, "list_job_statuses_sync", fake_list_jobs)

    exit_code = cli.jobs_main(["--config", str(config_path)])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["jobs"][0]["workflow_id"] == "histdatacom-run-cli"
    assert captured["offline"] is True
    assert captured["limit"] == 7
    assert captured["query"] == 'WorkflowType = "HistDataRunWorkflow"'


def test_orchestration_runtime_cli_reads_yaml_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Issue #31: runtime commands should accept recurrent YAML defaults."""
    config_path = tmp_path / "histdatacom.yaml"
    config_path.write_text(
        f"""
histdatacom:
  runtime:
    command: start
    workspace: {tmp_path / "workspace"}
    runtime_home: {tmp_path / "runtime"}
    state_dir: {tmp_path / "state"}
    json: true
    executable: /tmp/temporal
    startup_timeout: 1.5
    namespace: histdatacom-test
    task_queue_prefix: histdatacom-test
    cpu_utilization: high
    network_multiplier: 5
    orchestration_workers: 2
    influx_workers: 3
""",
        encoding="utf-8",
    )
    supervisor = _LifecycleSupervisor()
    captured: dict[str, object] = {}

    def fake_supervisor(args: object) -> _LifecycleSupervisor:
        captured["args"] = args
        return supervisor

    monkeypatch.setattr(cli, "_supervisor", fake_supervisor)

    exit_code = cli.main(["--config", str(config_path)])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["state"] == "running"
    assert supervisor.calls[0][0] == "start"
    assert supervisor.calls[0][1]["executable"] == "/tmp/temporal"
    assert supervisor.calls[0][1]["startup_timeout"] == 1.5
    args = captured["args"]
    assert getattr(args, "namespace") == "histdatacom-test"
    assert getattr(args, "task_queue_prefix") == "histdatacom-test"
    assert getattr(args, "cpu_utilization") == "high"
    assert getattr(args, "network_multiplier") == 5
    assert getattr(args, "orchestration_workers") == 2
    assert getattr(args, "influx_workers") == 3


def test_orchestration_runtime_jobs_cli_reads_nested_yaml_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Runtime jobs should support nested recurrent YAML defaults."""
    captured: dict[str, object] = {}
    config_path = tmp_path / "histdatacom.yaml"
    config_path.write_text(
        """
histdatacom:
  runtime:
    command: jobs
    jobs:
      command: list
      json: true
      offline: true
      limit: 3
""",
        encoding="utf-8",
    )

    def fake_list_jobs(**kwargs: object) -> OrchestrationJobList:
        captured.update(kwargs)
        return OrchestrationJobList(jobs=(_snapshot(),))

    monkeypatch.setattr(
        cli,
        "_supervisor",
        lambda args: _StatusOnlySupervisor("running"),
    )
    monkeypatch.setattr(cli, "_worker_config", lambda args: _FakeConfig())
    monkeypatch.setattr(cli, "list_job_statuses_sync", fake_list_jobs)

    exit_code = cli.main(["--config", str(config_path)])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["jobs"][0]["workflow_id"] == "histdatacom-run-cli"
    assert captured["offline"] is True
    assert captured["limit"] == 3


@pytest.mark.parametrize(
    "argv",
    (
        ["jobs", "--json", "--offline", "inspect", "histdatacom-run-cli"],
        ["jobs", "inspect", "histdatacom-run-cli", "--json", "--offline"],
    ),
)
def test_orchestration_jobs_inspect_accepts_shared_flags_after_subcommand(
    argv: list[str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Offline and JSON flags should work in documented user order."""
    captured: dict[str, object] = {}

    def fake_inspect(
        workflow_id: str,
        **kwargs: object,
    ) -> OrchestrationJobSnapshot:
        captured["workflow_id"] = workflow_id
        captured["kwargs"] = kwargs
        return _snapshot()

    monkeypatch.setattr(
        cli,
        "_supervisor",
        lambda args: _StatusOnlySupervisor("stopped"),
    )
    monkeypatch.setattr(cli, "_worker_config", lambda args: _FakeConfig())
    monkeypatch.setattr(cli, "inspect_job_status_sync", fake_inspect)

    exit_code = cli.main(argv)
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["workflow_id"] == "histdatacom-run-cli"
    assert captured["workflow_id"] == "histdatacom-run-cli"
    kwargs = captured["kwargs"]
    assert isinstance(kwargs, dict)
    assert kwargs["offline"] is True


def test_orchestration_jobs_offline_cli_reads_persisted_store(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Offline job commands should use the local durable status store."""
    config = build_orchestration_worker_config(
        runtime_policy=build_orchestration_runtime_policy(
            workspace=tmp_path / "workspace",
            runtime_home=tmp_path / "runtime",
        )
    )
    orchestration_client.orchestration_job_store(config).write_job_snapshot(
        _snapshot()
    )
    monkeypatch.setattr(
        cli,
        "_supervisor",
        lambda args: _StatusOnlySupervisor("stopped"),
    )
    monkeypatch.setattr(cli, "_worker_config", lambda args: config)

    list_exit = cli.main(["jobs", "--json", "--offline", "list"])
    list_payload = json.loads(capsys.readouterr().out)
    inspect_exit = cli.main(
        ["jobs", "--json", "--offline", "inspect", "histdatacom-run-cli"]
    )
    inspect_payload = json.loads(capsys.readouterr().out)

    assert list_exit == 0
    assert inspect_exit == 0
    assert list_payload["jobs"][0]["workflow_id"] == "histdatacom-run-cli"
    assert inspect_payload["workflow_id"] == "histdatacom-run-cli"


def test_orchestration_jobs_cancel_cli_passes_reason(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Jobs cancel should surface explicit cancellation state."""
    captured: dict[str, object] = {}

    def fake_cancel(
        workflow_id: str, **kwargs: object
    ) -> OrchestrationJobSnapshot:
        captured["workflow_id"] = workflow_id
        captured["kwargs"] = kwargs
        return _snapshot().request_cancel(reason=str(kwargs["reason"]))

    monkeypatch.setattr(
        cli,
        "_supervisor",
        lambda args: _StatusOnlySupervisor("running"),
    )
    monkeypatch.setattr(cli, "_worker_config", lambda args: _FakeConfig())
    monkeypatch.setattr(cli, "cancel_job_sync", fake_cancel)

    exit_code = cli.main(
        [
            "jobs",
            "--json",
            "cancel",
            "histdatacom-run-cli",
            "--reason",
            "operator",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert captured["workflow_id"] == "histdatacom-run-cli"
    assert captured["kwargs"]["reason"] == "operator"
    assert payload["lifecycle"] == JobLifecycle.CANCEL_REQUESTED.value


def test_orchestration_jobs_retry_cli_passes_recompute_flag(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Jobs retry should pass the explicit recompute preference."""
    captured: dict[str, object] = {}

    def fake_retry(
        workflow_id: str, **kwargs: object
    ) -> OrchestrationJobSnapshot:
        captured["workflow_id"] = workflow_id
        captured["kwargs"] = kwargs
        return _snapshot().mark_retrying(
            metadata={
                "reuse_completed_artifacts": bool(
                    kwargs["reuse_completed_artifacts"]
                )
            }
        )

    monkeypatch.setattr(
        cli,
        "_supervisor",
        lambda args: _StatusOnlySupervisor("running"),
    )
    monkeypatch.setattr(cli, "_worker_config", lambda args: _FakeConfig())
    monkeypatch.setattr(cli, "retry_job_sync", fake_retry)

    exit_code = cli.main(
        [
            "jobs",
            "--json",
            "retry",
            "histdatacom-run-cli",
            "--reason",
            "operator",
            "--recompute-complete",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert captured["workflow_id"] == "histdatacom-run-cli"
    assert captured["kwargs"]["reuse_completed_artifacts"] is False
    assert payload["lifecycle"] == JobLifecycle.RETRYING.value


def test_orchestration_jobs_submit_cli_loads_run_request_json(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Jobs submit should accept a serialized RunRequest payload."""
    request_path = tmp_path / "request.json"
    request_path.write_text(
        json.dumps(RunRequest(request_id="run-cli").to_dict())
    )
    captured: dict[str, object] = {}

    def fake_submit(
        request: RunRequest, **kwargs: object
    ) -> OrchestrationJobSnapshot:
        captured["request"] = request
        captured["kwargs"] = kwargs
        return _snapshot(lifecycle=JobLifecycle.SUBMITTED)

    monkeypatch.setattr(
        cli,
        "_supervisor",
        lambda args: _StatusOnlySupervisor("running"),
    )
    monkeypatch.setattr(cli, "_worker_config", lambda args: _FakeConfig())
    monkeypatch.setattr(cli, "submit_control_job_sync", fake_submit)

    exit_code = cli.main(
        [
            "jobs",
            "--json",
            "submit",
            "--request-json",
            str(request_path),
            "--submit-only",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert captured["request"].request_id == "run-cli"
    assert captured["kwargs"]["wait_for_result"] is False
    assert payload["lifecycle"] == JobLifecycle.SUBMITTED.value


def test_orchestration_jobs_submit_start_defers_config_until_after_start(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Submit --start should not resolve stale config before startup."""
    request_path = tmp_path / "request.json"
    request_path.write_text(
        json.dumps(RunRequest(request_id="run-cli").to_dict())
    )
    captured: dict[str, object] = {}
    supervisor = _StatusOnlySupervisor("stopped")

    def fail_worker_config(args: object) -> _FakeConfig:
        raise AssertionError("config should be resolved after startup")

    def fake_submit(
        request: RunRequest,
        **kwargs: object,
    ) -> OrchestrationJobSnapshot:
        captured["request"] = request
        captured["kwargs"] = kwargs
        return _snapshot(lifecycle=JobLifecycle.SUBMITTED)

    monkeypatch.setattr(cli, "_supervisor", lambda args: supervisor)
    monkeypatch.setattr(cli, "_worker_config", fail_worker_config)
    monkeypatch.setattr(cli, "submit_control_job_sync", fake_submit)

    exit_code = cli.main(
        [
            "jobs",
            "--json",
            "submit",
            "--request-json",
            str(request_path),
            "--start",
            "--submit-only",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    kwargs = captured["kwargs"]
    assert isinstance(kwargs, dict)
    assert kwargs["config"] is None
    assert kwargs["supervisor"] is supervisor
    assert kwargs["start_if_needed"] is True
    assert payload["lifecycle"] == JobLifecycle.SUBMITTED.value
