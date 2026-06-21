"""Tests for Temporal sidecar lifecycle CLI wiring."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from histdatacom.runtime_contracts import RunRequest, WorkStatus
from histdatacom.sidecar import cli
from histdatacom.sidecar.control import JobLifecycle, SidecarJobSnapshot


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
