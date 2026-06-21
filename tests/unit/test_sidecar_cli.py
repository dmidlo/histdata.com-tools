"""Tests for Temporal sidecar lifecycle CLI wiring."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from histdatacom.sidecar import cli


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
