"""Tests for Temporal sidecar lifecycle CLI wiring."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from histdatacom.runtime_contracts import RunRequest, WorkStatus
from histdatacom.sidecar import client as sidecar_client
from histdatacom.sidecar import cli
from histdatacom.sidecar.control import JobLifecycle, SidecarJobSnapshot
from histdatacom.sidecar.queues import build_sidecar_worker_config
from histdatacom.sidecar.runtime import build_sidecar_runtime_policy


class _StatusOnlySupervisor:
    """Test double for sidecar CLI dispatch."""

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
    """Test double for sidecar maintenance commands."""

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
            "state_dir": "/tmp/sidecar",
            "pid_file": "/tmp/sidecar/sidecar.pid.json",
            "lock_file": "/tmp/sidecar/sidecar.lock",
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
) -> SidecarJobSnapshot:
    """Return a fake control snapshot."""
    return SidecarJobSnapshot.from_handle(
        _Handle(),
        lifecycle=lifecycle,
        status=status,
    )


def test_sidecar_status_cli_emits_json(monkeypatch, capsys) -> None:
    """Sidecar status should be available as a first-class CLI command."""
    monkeypatch.setattr(
        cli, "_supervisor", lambda args: _StatusOnlySupervisor("running")
    )

    exit_code = cli.main(["status", "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["state"] == "running"


def test_sidecar_doctor_cli_returns_diagnostics(monkeypatch, capsys) -> None:
    """Doctor should expose sidecar diagnostics for humans and tools."""
    monkeypatch.setattr(
        cli, "_supervisor", lambda args: _StatusOnlySupervisor("stopped")
    )

    exit_code = cli.main(["doctor"])
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "stopped message" in output
    assert "ok" in output


def test_sidecar_lifecycle_cli_commands_delegate_to_supervisor(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Start, stop, and restart should be first-class sidecar commands."""
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


def test_sidecar_start_supervisor_inherits_worker_fleet_options(
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


def test_sidecar_maintenance_cli_emits_json(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Maintenance should expose a GUI-ready JSON payload."""
    runtime_policy = build_sidecar_runtime_policy(
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


def test_histdatacom_main_dispatches_sidecar_command(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The top-level histdatacom command should route sidecar subcommands."""
    import histdatacom.histdata_com as histdata_com

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "sidecar",
            "--state-dir",
            str(tmp_path),
            "status",
        ],
    )

    assert histdata_com.main() == 0


def test_sidecar_jobs_inspect_cli_emits_control_snapshot_json(
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


def test_sidecar_jobs_offline_cli_reads_persisted_store(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Offline job commands should use the local durable status store."""
    config = build_sidecar_worker_config(
        runtime_policy=build_sidecar_runtime_policy(
            workspace=tmp_path / "workspace",
            runtime_home=tmp_path / "runtime",
        )
    )
    sidecar_client.sidecar_job_store(config).write_job_snapshot(_snapshot())
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


def test_sidecar_jobs_cancel_cli_passes_reason(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Jobs cancel should surface explicit cancellation state."""
    captured: dict[str, object] = {}

    def fake_cancel(workflow_id: str, **kwargs: object) -> SidecarJobSnapshot:
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


def test_sidecar_jobs_retry_cli_passes_recompute_flag(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Jobs retry should pass the explicit recompute preference."""
    captured: dict[str, object] = {}

    def fake_retry(workflow_id: str, **kwargs: object) -> SidecarJobSnapshot:
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


def test_sidecar_jobs_submit_cli_loads_run_request_json(
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
    ) -> SidecarJobSnapshot:
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
