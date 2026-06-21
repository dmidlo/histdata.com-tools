"""Operator-gated live Temporal sidecar smoke checks."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from histdatacom.runtime_contracts import RunRequest, WorkStatus
from histdatacom.sidecar.client import submit_control_job
from histdatacom.sidecar.control import JobLifecycle, SidecarJobSnapshot
from histdatacom.sidecar.queues import (
    DEFAULT_TASK_QUEUE_PREFIX,
    DEFAULT_TEMPORAL_NAMESPACE,
    SidecarWorkerConfig,
    TaskQueueLane,
    build_sidecar_worker_config,
)
from histdatacom.sidecar.resources import (
    SidecarExecutableUnavailable,
    UnsupportedSidecarPlatform,
    sidecar_executable_path,
)
from histdatacom.sidecar.runtime import (
    SidecarRuntimePolicy,
    build_sidecar_runtime_policy,
)
from histdatacom.sidecar.supervisor import SidecarStatus, SidecarSupervisor

LIVE_SIDECAR_SMOKE_ENV = "HISTDATACOM_LIVE_SIDECAR_SMOKE"
TEMPORAL_EXECUTABLE_ENV = "HISTDATACOM_TEMPORAL_EXECUTABLE"
LIVE_INFLUX_SMOKE_ENV = "HISTDATACOM_LIVE_SIDECAR_INFLUX"
DEFAULT_LIVE_SIDECAR_SMOKE_REQUEST_ID = "live-sidecar-smoke"
DEFAULT_LIVE_SIDECAR_SMOKE_STARTUP_TIMEOUT = 30.0
DEFAULT_LIVE_SIDECAR_SMOKE_COMPLETION_TIMEOUT = 180.0
DEFAULT_LIVE_SIDECAR_SMOKE_STOP_TIMEOUT = 30.0
DEFAULT_LOG_TAIL_LINES = 120
DEFAULT_LOG_TAIL_BYTES = 64_000
DEFAULT_LIVE_SIDECAR_SMOKE_LANES = (
    TaskQueueLane.ORCHESTRATION,
    TaskQueueLane.NETWORK,
    TaskQueueLane.CPU_FILE,
)

SupervisorFactory = Callable[..., SidecarSupervisor]
SubmitJob = Callable[..., SidecarJobSnapshot]


class LiveSidecarSmokeError(RuntimeError):
    """Raised when the live sidecar smoke fails with diagnostics."""

    def __init__(self, message: str, diagnostics: Mapping[str, Any]) -> None:
        super().__init__(message)
        self.diagnostics = dict(diagnostics)


@dataclass(frozen=True, slots=True)
class LiveSidecarSmokeResult:
    """Result payload for an operator-gated live sidecar smoke run."""

    request: RunRequest
    worker_config: SidecarWorkerConfig
    started_status: SidecarStatus
    snapshot: SidecarJobSnapshot
    doctor: Mapping[str, Any]
    stopped_status: SidecarStatus | None = None
    diagnostics: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible smoke report."""
        return {
            "request": self.request.to_dict(),
            "worker_config": self.worker_config.to_dict(),
            "started_status": self.started_status.to_dict(),
            "snapshot": self.snapshot.to_dict(),
            "doctor": dict(self.doctor),
            "stopped_status": (
                self.stopped_status.to_dict()
                if self.stopped_status is not None
                else None
            ),
            "diagnostics": dict(self.diagnostics),
            "influx": live_influx_smoke_status(),
        }


def live_sidecar_smoke_enabled(
    environ: Mapping[str, str] | None = None,
) -> bool:
    """Return whether the operator explicitly enabled live sidecar smoke."""
    env = environ if environ is not None else os.environ
    return _truthy(env.get(LIVE_SIDECAR_SMOKE_ENV, ""))


def live_influx_smoke_status(
    environ: Mapping[str, str] | None = None,
) -> dict[str, bool | str]:
    """Return whether optional Influx smoke coverage is configured."""
    env = environ if environ is not None else os.environ
    configured = _truthy(env.get(LIVE_INFLUX_SMOKE_ENV, ""))
    return {
        "configured": configured,
        "skipped": not configured,
        "reason": (
            "" if configured else f"{LIVE_INFLUX_SMOKE_ENV} is not enabled."
        ),
    }


def live_sidecar_smoke_skip_reason(
    *,
    environ: Mapping[str, str] | None = None,
    temporal_executable: Path | str | None = None,
) -> str:
    """Return a skip reason, or an empty string when live smoke can run."""
    env = environ if environ is not None else os.environ
    if not live_sidecar_smoke_enabled(env):
        return f"{LIVE_SIDECAR_SMOKE_ENV}=1 is required."

    if importlib.util.find_spec("temporalio") is None:
        return "temporalio is not installed; install histdatacom[temporal]."

    executable = _temporal_executable_from_inputs(
        temporal_executable=temporal_executable,
        environ=env,
    )
    if executable is not None:
        return _provided_executable_skip_reason(executable)

    try:
        with sidecar_executable_path():
            return ""
    except (SidecarExecutableUnavailable, UnsupportedSidecarPlatform) as err:
        return (
            f"{TEMPORAL_EXECUTABLE_ENV} is not set and no packaged Temporal "
            f"executable is available: {err}"
        )


def default_live_sidecar_smoke_request(
    *,
    request_id: str = DEFAULT_LIVE_SIDECAR_SMOKE_REQUEST_ID,
    data_directory: Path | str,
) -> RunRequest:
    """Build the minimal non-Influx live sidecar smoke request."""
    return RunRequest(
        request_id=request_id,
        pairs=("eurusd",),
        formats=("ascii",),
        timeframes=("M1",),
        start_yearmonth="202201",
        end_yearmonth="202201",
        data_directory=str(Path(data_directory).expanduser()),
        available_remote_data=True,
        validate_urls=True,
        metadata={"live_sidecar_smoke": True},
    )


def run_live_sidecar_smoke(
    *,
    workspace: Path | str,
    runtime_home: Path | str,
    data_directory: Path | str,
    temporal_executable: Path | str | None = None,
    startup_timeout: float = DEFAULT_LIVE_SIDECAR_SMOKE_STARTUP_TIMEOUT,
    completion_timeout: float = DEFAULT_LIVE_SIDECAR_SMOKE_COMPLETION_TIMEOUT,
    stop_timeout: float = DEFAULT_LIVE_SIDECAR_SMOKE_STOP_TIMEOUT,
    request_id: str = DEFAULT_LIVE_SIDECAR_SMOKE_REQUEST_ID,
    namespace: str = DEFAULT_TEMPORAL_NAMESPACE,
    task_queue_prefix: str = DEFAULT_TASK_QUEUE_PREFIX,
    worker_lanes: Sequence[str | TaskQueueLane] = (
        DEFAULT_LIVE_SIDECAR_SMOKE_LANES
    ),
    environ: Mapping[str, str] | None = None,
    supervisor_factory: SupervisorFactory = SidecarSupervisor,
    submit_job: SubmitJob | None = None,
) -> LiveSidecarSmokeResult:
    """Run a live Temporal server, worker fleet, and minimal job smoke."""
    env = environ if environ is not None else os.environ
    executable = _temporal_executable_from_inputs(
        temporal_executable=temporal_executable,
        environ=env,
    )
    request = default_live_sidecar_smoke_request(
        request_id=request_id,
        data_directory=data_directory,
    )
    runtime_policy = build_sidecar_runtime_policy(
        workspace=workspace,
        runtime_home=runtime_home,
        check_ports=True,
    )
    lanes = tuple(TaskQueueLane.from_value(lane) for lane in worker_lanes)
    supervisor = supervisor_factory(
        runtime_policy=runtime_policy,
        namespace=namespace,
        task_queue_prefix=task_queue_prefix,
        worker_lanes=lanes,
    )
    started_status: SidecarStatus | None = None
    snapshot: SidecarJobSnapshot | None = None
    stopped_status: SidecarStatus | None = None
    worker_config: SidecarWorkerConfig | None = None
    doctor: Mapping[str, Any] | None = None
    diagnostics: Mapping[str, Any] = {}
    try:
        started_status = supervisor.start(
            executable=executable,
            startup_timeout=startup_timeout,
        )
        worker_config = build_sidecar_worker_config(
            runtime_policy=supervisor.runtime_policy,
            namespace=namespace,
            task_queue_prefix=task_queue_prefix,
        )
        snapshot = _submit_live_smoke_job(
            request,
            config=worker_config,
            supervisor=supervisor,
            completion_timeout=completion_timeout,
            submit_job=submit_job,
        )
        _validate_live_smoke_snapshot(snapshot)
        doctor = supervisor.doctor()
        diagnostics = collect_live_sidecar_smoke_diagnostics(
            supervisor=supervisor,
            runtime_policy=supervisor.runtime_policy,
            request=request,
            snapshot=snapshot,
            doctor=doctor,
        )
    except Exception as err:
        diagnostics = collect_live_sidecar_smoke_diagnostics(
            supervisor=supervisor,
            runtime_policy=supervisor.runtime_policy,
            request=request,
            snapshot=snapshot,
            error=err,
        )
        raise LiveSidecarSmokeError(
            f"live sidecar smoke failed: {err}",
            diagnostics,
        ) from err
    finally:
        try:
            stopped_status = _stop_live_sidecar(
                supervisor,
                stop_timeout=stop_timeout,
            )
        except Exception:
            pass
    if started_status is None or worker_config is None or snapshot is None:
        raise LiveSidecarSmokeError(
            "live sidecar smoke did not produce a complete result",
            diagnostics,
        )
    return LiveSidecarSmokeResult(
        request=request,
        worker_config=worker_config,
        started_status=started_status,
        stopped_status=stopped_status,
        snapshot=snapshot,
        doctor=dict(doctor or {}),
        diagnostics=diagnostics,
    )


def collect_live_sidecar_smoke_diagnostics(
    *,
    supervisor: SidecarSupervisor,
    runtime_policy: SidecarRuntimePolicy,
    request: RunRequest | None = None,
    snapshot: SidecarJobSnapshot | None = None,
    doctor: Mapping[str, Any] | None = None,
    error: BaseException | None = None,
    log_tail_lines: int = DEFAULT_LOG_TAIL_LINES,
    log_tail_bytes: int = DEFAULT_LOG_TAIL_BYTES,
) -> dict[str, Any]:
    """Collect logs and runtime policy details for live smoke failures."""
    status_payload: dict[str, Any]
    try:
        status_payload = supervisor.status(repair=False).to_dict()
    except Exception as status_err:
        status_payload = {"error": repr(status_err)}

    doctor_payload: Mapping[str, Any]
    if doctor is not None:
        doctor_payload = doctor
    else:
        try:
            doctor_payload = supervisor.doctor()
        except Exception as doctor_err:
            doctor_payload = {"error": repr(doctor_err)}

    logs = _diagnostic_log_paths(status_payload, runtime_policy)
    return {
        "error": repr(error) if error is not None else "",
        "runtime_policy": runtime_policy.to_dict(),
        "status": status_payload,
        "doctor": dict(doctor_payload),
        "request": request.to_dict() if request is not None else None,
        "snapshot": snapshot.to_dict() if snapshot is not None else None,
        "logs": {
            component: _tail_text(
                Path(log_path),
                max_lines=log_tail_lines,
                max_bytes=log_tail_bytes,
            )
            for component, log_path in logs.items()
        },
    }


def _submit_live_smoke_job(
    request: RunRequest,
    *,
    config: SidecarWorkerConfig,
    supervisor: SidecarSupervisor,
    completion_timeout: float,
    submit_job: SubmitJob | None,
) -> SidecarJobSnapshot:
    if submit_job is not None:
        return submit_job(
            request,
            config=config,
            supervisor=supervisor,
            start_if_needed=False,
            wait_for_result=True,
        )
    return asyncio.run(
        asyncio.wait_for(
            submit_control_job(
                request,
                config=config,
                supervisor=supervisor,
                start_if_needed=False,
                wait_for_result=True,
            ),
            timeout=completion_timeout,
        )
    )


def _stop_live_sidecar(
    supervisor: SidecarSupervisor,
    *,
    stop_timeout: float,
) -> SidecarStatus:
    status = supervisor.stop(stop_timeout=stop_timeout)
    retry_timeout = min(2.0, max(0.1, stop_timeout))
    for _attempt in range(5):
        if status.state != "stopping":
            return status
        time.sleep(1.0)
        status = supervisor.stop(stop_timeout=retry_timeout)
    time.sleep(2.0)
    repaired = supervisor.status(repair=True)
    if repaired.state in {"stale", "stopped"}:
        return supervisor.stop(stop_timeout=retry_timeout)
    return status


def _validate_live_smoke_snapshot(snapshot: SidecarJobSnapshot) -> None:
    if snapshot.lifecycle != JobLifecycle.SUCCEEDED:
        raise LiveSidecarSmokeError(
            "live sidecar smoke job did not succeed",
            {"snapshot": snapshot.to_dict()},
        )
    if snapshot.status != WorkStatus.COMPLETED:
        raise LiveSidecarSmokeError(
            "live sidecar smoke job did not complete",
            {"snapshot": snapshot.to_dict()},
        )
    if not snapshot.artifacts:
        raise LiveSidecarSmokeError(
            "live sidecar smoke job completed without artifact references",
            {"snapshot": snapshot.to_dict()},
        )


def _temporal_executable_from_inputs(
    *,
    temporal_executable: Path | str | None,
    environ: Mapping[str, str],
) -> Path | None:
    executable = temporal_executable or environ.get(TEMPORAL_EXECUTABLE_ENV)
    if executable is None or str(executable).strip() == "":
        return None
    return Path(executable).expanduser()


def _provided_executable_skip_reason(executable: Path) -> str:
    if not executable.is_file():
        return f"Temporal executable is not a file: {executable}"
    if os.name != "nt" and not os.access(executable, os.X_OK):
        return f"Temporal executable is not executable: {executable}"
    return ""


def _diagnostic_log_paths(
    status_payload: Mapping[str, Any],
    runtime_policy: SidecarRuntimePolicy,
) -> dict[str, str]:
    logs = status_payload.get("logs")
    if isinstance(logs, Mapping):
        return {
            str(component): str(path)
            for component, path in logs.items()
            if isinstance(component, str) and isinstance(path, str)
        }
    return {"server": str(runtime_policy.paths.server_log)}


def _tail_text(
    path: Path,
    *,
    max_lines: int,
    max_bytes: int,
) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False, "text": ""}
    with path.open("rb") as source:
        source.seek(0, os.SEEK_END)
        size = source.tell()
        source.seek(max(0, size - max_bytes))
        text = source.read().decode("utf-8", errors="replace")
    lines = text.splitlines()
    return {
        "path": str(path),
        "exists": True,
        "size_bytes": size,
        "text": "\n".join(lines[-max_lines:]),
    }


def _truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def diagnostics_json(diagnostics: Mapping[str, Any]) -> str:
    """Return pretty diagnostic JSON for pytest failure messages."""
    return json.dumps(dict(diagnostics), indent=2, sort_keys=True)
