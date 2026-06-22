"""Tests for operator-gated live sidecar smoke helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

import histdatacom.sidecar.live_smoke as live_smoke
from histdatacom.runtime_contracts import ArtifactRef, RunRequest, WorkStatus
from histdatacom.sidecar.control import JobLifecycle, SidecarJobSnapshot
from histdatacom.sidecar.live_smoke import (
    DEFAULT_LIVE_SIDECAR_SMOKE_LANES,
    LiveSidecarSmokeError,
    default_live_sidecar_smoke_request,
    run_live_sidecar_smoke,
)
from histdatacom.sidecar.queues import TaskQueueLane
from histdatacom.sidecar.runtime import SidecarRuntimePolicy
from histdatacom.sidecar.supervisor import SidecarStatus


class _FakeSupervisor:
    def __init__(
        self,
        *,
        runtime_policy: SidecarRuntimePolicy,
        worker_lanes: tuple[TaskQueueLane, ...],
        **kwargs: object,
    ) -> None:
        self.runtime_policy = runtime_policy
        self.worker_lanes = worker_lanes
        self.kwargs = kwargs
        self.started_executable: Path | None = None
        self.stopped = False
        self.runtime_policy.ensure_directories()
        self.runtime_policy.paths.server_log.write_text(
            "server started\n",
            encoding="utf-8",
        )

    def start(
        self,
        *,
        executable: Path | None,
        startup_timeout: float,
    ) -> SidecarStatus:
        self.started_executable = executable
        return self.status()

    def stop(self, *, stop_timeout: float = 0.0) -> SidecarStatus:
        self.stopped = True
        return self._status("stopped")

    def status(self, *, repair: bool = False) -> SidecarStatus:
        return self._status("running")

    def doctor(self) -> dict[str, object]:
        return {
            "status": self.status().to_dict(),
            "runtime_policy": self.runtime_policy.to_dict(),
            "workers": {
                lane.value: {"state": "running"} for lane in self.worker_lanes
            },
        }

    def _status(
        self,
        state: str,
        *,
        pids: dict[str, int] | None = None,
    ) -> SidecarStatus:
        return SidecarStatus(
            state=state,
            message=state,
            state_dir=str(self.runtime_policy.paths.state_dir),
            pid_file=str(self.runtime_policy.paths.pid_file),
            lock_file=str(self.runtime_policy.paths.lock_file),
            logs={"server": str(self.runtime_policy.paths.server_log)},
            pids=(
                pids
                if pids is not None
                else (
                    {"server": 1234} if state in {"running", "stopping"} else {}
                )
            ),
            components={
                "server": state,
                **{f"worker:{lane.value}": state for lane in self.worker_lanes},
            },
        )


class _StopRaisesSupervisor(_FakeSupervisor):
    def stop(self, *, stop_timeout: float = 0.0) -> SidecarStatus:
        self.stopped = True
        raise RuntimeError("stop exploded")


class _StuckStoppingSupervisor(_FakeSupervisor):
    def stop(self, *, stop_timeout: float = 0.0) -> SidecarStatus:
        self.stopped = True
        return self._status("stopping")

    def status(self, *, repair: bool = False) -> SidecarStatus:
        if self.stopped:
            return self._status("stopping")
        return self._status("running")


class _StoppedWithRemainingPidsSupervisor(_FakeSupervisor):
    def stop(self, *, stop_timeout: float = 0.0) -> SidecarStatus:
        self.stopped = True
        return self._status("stopped", pids={"server": 1234})


class _MissingStopStatusSupervisor(_FakeSupervisor):
    def stop(self, *, stop_timeout: float = 0.0) -> Any:
        self.stopped = True
        return None


def _completed_snapshot(request: RunRequest) -> SidecarJobSnapshot:
    return SidecarJobSnapshot(
        job_id=f"histdatacom-{request.request_id}",
        request_id=request.request_id,
        workflow_id=f"histdatacom-{request.request_id}",
        lifecycle=JobLifecycle.SUCCEEDED,
        status=WorkStatus.COMPLETED,
        artifacts=(
            ArtifactRef(
                kind="repository",
                path="/tmp/histdatacom-live-smoke/.repo",
                size_bytes=512,
            ),
        ),
    )


def test_default_live_sidecar_smoke_request_is_minimal_non_influx(
    tmp_path: Path,
) -> None:
    request = default_live_sidecar_smoke_request(
        data_directory=tmp_path / "data"
    )

    assert request.available_remote_data is True
    assert request.validate_urls is True
    assert request.import_to_influxdb is False
    assert request.download_data_archives is False
    assert request.extract_csvs is False
    assert request.pairs == ("eurusd",)
    assert request.formats == ("ascii",)
    assert request.timeframes == ("M1",)


def test_run_live_sidecar_smoke_uses_non_influx_lanes_and_stops(
    tmp_path: Path,
) -> None:
    supervisors: list[_FakeSupervisor] = []
    captured: dict[str, Any] = {}

    def supervisor_factory(**kwargs: Any) -> _FakeSupervisor:
        supervisor = _FakeSupervisor(**kwargs)
        supervisors.append(supervisor)
        return supervisor

    def submit_job(request: RunRequest, **kwargs: Any) -> SidecarJobSnapshot:
        captured["request"] = request
        captured["kwargs"] = kwargs
        return _completed_snapshot(request)

    result = run_live_sidecar_smoke(
        workspace=tmp_path / "workspace",
        runtime_home=tmp_path / "runtime",
        data_directory=tmp_path / "data",
        supervisor_factory=supervisor_factory,
        submit_job=submit_job,
    )

    assert supervisors[0].worker_lanes == DEFAULT_LIVE_SIDECAR_SMOKE_LANES
    assert TaskQueueLane.INFLUX not in supervisors[0].worker_lanes
    assert supervisors[0].stopped is True
    assert result.stopped_status is not None
    assert result.stopped_status.state == "stopped"
    assert captured["request"].validate_urls is True
    assert captured["request"].import_to_influxdb is False
    assert captured["kwargs"]["start_if_needed"] is False
    assert captured["kwargs"]["wait_for_result"] is True
    assert result.snapshot.lifecycle == JobLifecycle.SUCCEEDED
    assert result.snapshot.artifacts


def test_run_live_sidecar_smoke_failure_includes_log_diagnostics(
    tmp_path: Path,
) -> None:
    def supervisor_factory(**kwargs: Any) -> _FakeSupervisor:
        return _FakeSupervisor(**kwargs)

    def submit_job(request: RunRequest, **kwargs: Any) -> SidecarJobSnapshot:
        return SidecarJobSnapshot(
            job_id=request.request_id,
            request_id=request.request_id,
            workflow_id=request.request_id,
            lifecycle=JobLifecycle.RUNNING,
            status=WorkStatus.UNKNOWN,
        )

    with pytest.raises(LiveSidecarSmokeError) as raised:
        run_live_sidecar_smoke(
            workspace=tmp_path / "workspace",
            runtime_home=tmp_path / "runtime",
            data_directory=tmp_path / "data",
            supervisor_factory=supervisor_factory,
            submit_job=submit_job,
        )

    diagnostics = raised.value.diagnostics
    assert diagnostics["snapshot"]["status"] == WorkStatus.UNKNOWN.value
    assert diagnostics["logs"]["server"]["exists"] is True
    assert "server started" in diagnostics["logs"]["server"]["text"]


def test_run_live_sidecar_smoke_fails_when_stop_raises(
    tmp_path: Path,
) -> None:
    def supervisor_factory(**kwargs: Any) -> _StopRaisesSupervisor:
        return _StopRaisesSupervisor(**kwargs)

    def submit_job(request: RunRequest, **kwargs: Any) -> SidecarJobSnapshot:
        return _completed_snapshot(request)

    with pytest.raises(
        LiveSidecarSmokeError, match="shutdown failed"
    ) as raised:
        run_live_sidecar_smoke(
            workspace=tmp_path / "workspace",
            runtime_home=tmp_path / "runtime",
            data_directory=tmp_path / "data",
            supervisor_factory=supervisor_factory,
            submit_job=submit_job,
        )

    diagnostics = raised.value.diagnostics
    assert "stop exploded" in diagnostics["error"]
    assert diagnostics["status"]["state"] == "running"
    assert diagnostics["logs"]["server"]["exists"] is True
    assert "server started" in diagnostics["logs"]["server"]["text"]


def test_run_live_sidecar_smoke_fails_when_stop_remains_stopping(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(live_smoke.time, "sleep", lambda _seconds: None)

    def supervisor_factory(**kwargs: Any) -> _StuckStoppingSupervisor:
        return _StuckStoppingSupervisor(**kwargs)

    def submit_job(request: RunRequest, **kwargs: Any) -> SidecarJobSnapshot:
        return _completed_snapshot(request)

    with pytest.raises(LiveSidecarSmokeError, match="state=stopping") as raised:
        run_live_sidecar_smoke(
            workspace=tmp_path / "workspace",
            runtime_home=tmp_path / "runtime",
            data_directory=tmp_path / "data",
            supervisor_factory=supervisor_factory,
            submit_job=submit_job,
        )

    diagnostics = raised.value.diagnostics
    assert diagnostics["stopped_status"]["state"] == "stopping"
    assert diagnostics["stopped_status"]["pids"] == {"server": 1234}
    assert diagnostics["logs"]["server"]["exists"] is True
    assert "server started" in diagnostics["logs"]["server"]["text"]


def test_run_live_sidecar_smoke_fails_when_stopped_status_has_pids(
    tmp_path: Path,
) -> None:
    def supervisor_factory(
        **kwargs: Any,
    ) -> _StoppedWithRemainingPidsSupervisor:
        return _StoppedWithRemainingPidsSupervisor(**kwargs)

    def submit_job(request: RunRequest, **kwargs: Any) -> SidecarJobSnapshot:
        return _completed_snapshot(request)

    with pytest.raises(LiveSidecarSmokeError, match="remaining pids") as raised:
        run_live_sidecar_smoke(
            workspace=tmp_path / "workspace",
            runtime_home=tmp_path / "runtime",
            data_directory=tmp_path / "data",
            supervisor_factory=supervisor_factory,
            submit_job=submit_job,
        )

    diagnostics = raised.value.diagnostics
    assert diagnostics["stopped_status"]["state"] == "stopped"
    assert diagnostics["stopped_status"]["pids"] == {"server": 1234}


def test_run_live_sidecar_smoke_fails_when_stop_returns_no_status(
    tmp_path: Path,
) -> None:
    def supervisor_factory(**kwargs: Any) -> _MissingStopStatusSupervisor:
        return _MissingStopStatusSupervisor(**kwargs)

    def submit_job(request: RunRequest, **kwargs: Any) -> SidecarJobSnapshot:
        return _completed_snapshot(request)

    with pytest.raises(
        LiveSidecarSmokeError, match="did not return a status"
    ) as raised:
        run_live_sidecar_smoke(
            workspace=tmp_path / "workspace",
            runtime_home=tmp_path / "runtime",
            data_directory=tmp_path / "data",
            supervisor_factory=supervisor_factory,
            submit_job=submit_job,
        )

    diagnostics = raised.value.diagnostics
    assert "did not return a status" in diagnostics["error"]
    assert diagnostics["logs"]["server"]["exists"] is True
