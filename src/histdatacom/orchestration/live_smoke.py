"""Operator-gated live Temporal orchestration smoke checks."""

from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from histdatacom.runtime_contracts import RunRequest, WorkStatus
from histdatacom.orchestration.client import submit_control_job
from histdatacom.orchestration.control import (
    JobLifecycle,
    OrchestrationJobSnapshot,
)
from histdatacom.orchestration.queues import (
    DEFAULT_TASK_QUEUE_PREFIX,
    DEFAULT_TEMPORAL_NAMESPACE,
    OrchestrationWorkerConfig,
    TaskQueueLane,
    build_orchestration_worker_config,
)
from histdatacom.orchestration.runtime import (
    OrchestrationRuntimePolicy,
    build_orchestration_runtime_policy,
)
from histdatacom.orchestration.supervisor import (
    OrchestrationStatus,
    OrchestrationSupervisor,
)

TEMPORAL_EXECUTABLE_ENV = "HISTDATACOM_TEMPORAL_EXECUTABLE"
LIVE_INFLUX_SMOKE_ENV = "HISTDATACOM_LIVE_ORCHESTRATION_INFLUX"
DEFAULT_LIVE_ORCHESTRATION_SMOKE_REQUEST_ID = "live-orchestration-smoke"
DEFAULT_LIVE_ORCHESTRATION_SMOKE_STARTUP_TIMEOUT = 30.0
DEFAULT_LIVE_ORCHESTRATION_SMOKE_COMPLETION_TIMEOUT = 180.0
DEFAULT_LIVE_ORCHESTRATION_SMOKE_STOP_TIMEOUT = 30.0
DEFAULT_LOG_TAIL_LINES = 120
DEFAULT_LOG_TAIL_BYTES = 64_000
DEFAULT_HERMETIC_ORCHESTRATION_SMOKE_REQUEST_ID = "hermetic-orchestration-smoke"
DEFAULT_CLIENT_ROUTING_ORCHESTRATION_SMOKE_REQUEST_ID = (
    "default-client-routing-orchestration-smoke"
)
DEFAULT_CLIENT_ROUTING_SMOKE_NAMESPACE = "histdatacom-smoke"
DEFAULT_CLIENT_ROUTING_SMOKE_TASK_QUEUE_PREFIX = "histdatacom-smoke"
DEFAULT_LIVE_ORCHESTRATION_SMOKE_LANES = (
    TaskQueueLane.ORCHESTRATION,
    TaskQueueLane.NETWORK,
    TaskQueueLane.CPU_FILE,
)

SupervisorFactory = Callable[..., OrchestrationSupervisor]
SubmitJob = Callable[..., OrchestrationJobSnapshot]


class LiveOrchestrationSmokeError(RuntimeError):
    """Raised when the live orchestration smoke fails with diagnostics."""

    def __init__(self, message: str, diagnostics: Mapping[str, Any]) -> None:
        super().__init__(message)
        self.diagnostics = dict(diagnostics)


class LiveOrchestrationStopError(RuntimeError):
    """Raised when orchestration shutdown does not reach a terminal state."""

    def __init__(
        self, message: str, status: OrchestrationStatus | None = None
    ) -> None:
        super().__init__(message)
        self.status = status


_STOPPED_ORCHESTRATION_STATES = frozenset({"stale", "stopped"})


@dataclass(frozen=True, slots=True)
class LiveOrchestrationSmokeResult:
    """Result payload for an operator-gated live orchestration smoke run."""

    request: RunRequest
    worker_config: OrchestrationWorkerConfig
    started_status: OrchestrationStatus
    snapshot: OrchestrationJobSnapshot
    doctor: Mapping[str, Any]
    stopped_status: OrchestrationStatus | None = None
    client_routing: str = "explicit_config"
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
            "client_routing": self.client_routing,
            "diagnostics": dict(self.diagnostics),
            "influx": live_influx_smoke_status(),
        }


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


def default_live_orchestration_smoke_request(
    *,
    request_id: str = DEFAULT_LIVE_ORCHESTRATION_SMOKE_REQUEST_ID,
    data_directory: Path | str,
) -> RunRequest:
    """Build the external HistData.com non-Influx live smoke request."""
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
        metadata={"live_orchestration_smoke": True},
    )


def default_hermetic_orchestration_smoke_request(
    *,
    request_id: str = DEFAULT_HERMETIC_ORCHESTRATION_SMOKE_REQUEST_ID,
    data_directory: Path | str,
) -> RunRequest:
    """Build a local-only orchestration smoke request for installed wheels."""
    return RunRequest(
        request_id=request_id,
        pairs=("eurusd",),
        formats=("ascii",),
        timeframes=("M1",),
        start_yearmonth="202201",
        end_yearmonth="202201",
        data_directory=str(Path(data_directory).expanduser()),
        available_remote_data=False,
        update_remote_data=False,
        validate_urls=False,
        download_data_archives=False,
        extract_csvs=False,
        import_to_influxdb=False,
        metadata={"hermetic_orchestration_smoke": True},
    )


def default_client_routing_orchestration_smoke_request(
    *,
    request_id: str = DEFAULT_CLIENT_ROUTING_ORCHESTRATION_SMOKE_REQUEST_ID,
    data_directory: Path | str,
) -> RunRequest:
    """Build a local-only request for default client-routing smoke coverage."""
    return RunRequest(
        request_id=request_id,
        pairs=("eurusd",),
        formats=("ascii",),
        timeframes=("M1",),
        start_yearmonth="202201",
        end_yearmonth="202201",
        data_directory=str(Path(data_directory).expanduser()),
        available_remote_data=False,
        update_remote_data=False,
        validate_urls=False,
        download_data_archives=False,
        extract_csvs=False,
        import_to_influxdb=False,
        metadata={
            "hermetic_orchestration_smoke": True,
            "default_client_routing_smoke": True,
        },
    )


def run_hermetic_orchestration_smoke(
    *,
    workspace: Path | str,
    runtime_home: Path | str,
    data_directory: Path | str,
    temporal_executable: Path | str | None = None,
    startup_timeout: float = DEFAULT_LIVE_ORCHESTRATION_SMOKE_STARTUP_TIMEOUT,
    completion_timeout: float = DEFAULT_LIVE_ORCHESTRATION_SMOKE_COMPLETION_TIMEOUT,
    stop_timeout: float = DEFAULT_LIVE_ORCHESTRATION_SMOKE_STOP_TIMEOUT,
    request_id: str = DEFAULT_HERMETIC_ORCHESTRATION_SMOKE_REQUEST_ID,
    namespace: str = DEFAULT_TEMPORAL_NAMESPACE,
    task_queue_prefix: str = DEFAULT_TASK_QUEUE_PREFIX,
    worker_lanes: Sequence[str | TaskQueueLane] = (
        DEFAULT_LIVE_ORCHESTRATION_SMOKE_LANES
    ),
    environ: Mapping[str, str] | None = None,
    supervisor_factory: SupervisorFactory = OrchestrationSupervisor,
    submit_job: SubmitJob | None = None,
) -> LiveOrchestrationSmokeResult:
    """Run a local-only installed-wheel orchestration runtime smoke."""
    request = default_hermetic_orchestration_smoke_request(
        request_id=request_id,
        data_directory=data_directory,
    )
    return _run_orchestration_smoke(
        smoke_name="hermetic orchestration smoke",
        request=request,
        workspace=workspace,
        runtime_home=runtime_home,
        temporal_executable=temporal_executable,
        startup_timeout=startup_timeout,
        completion_timeout=completion_timeout,
        stop_timeout=stop_timeout,
        namespace=namespace,
        task_queue_prefix=task_queue_prefix,
        worker_lanes=worker_lanes,
        environ=environ,
        supervisor_factory=supervisor_factory,
        submit_job=submit_job,
        default_client_routing=False,
    )


def run_default_client_routing_orchestration_smoke(
    *,
    workspace: Path | str,
    runtime_home: Path | str,
    data_directory: Path | str,
    temporal_executable: Path | str | None = None,
    startup_timeout: float = DEFAULT_LIVE_ORCHESTRATION_SMOKE_STARTUP_TIMEOUT,
    completion_timeout: float = DEFAULT_LIVE_ORCHESTRATION_SMOKE_COMPLETION_TIMEOUT,
    stop_timeout: float = DEFAULT_LIVE_ORCHESTRATION_SMOKE_STOP_TIMEOUT,
    request_id: str = DEFAULT_CLIENT_ROUTING_ORCHESTRATION_SMOKE_REQUEST_ID,
    namespace: str = DEFAULT_CLIENT_ROUTING_SMOKE_NAMESPACE,
    task_queue_prefix: str = DEFAULT_CLIENT_ROUTING_SMOKE_TASK_QUEUE_PREFIX,
    worker_lanes: Sequence[str | TaskQueueLane] = (
        DEFAULT_LIVE_ORCHESTRATION_SMOKE_LANES
    ),
    environ: Mapping[str, str] | None = None,
    supervisor_factory: SupervisorFactory = OrchestrationSupervisor,
    submit_job: SubmitJob | None = None,
) -> LiveOrchestrationSmokeResult:
    """Run installed-wheel smoke through the default client resolver path."""
    request = default_client_routing_orchestration_smoke_request(
        request_id=request_id,
        data_directory=data_directory,
    )
    return _run_orchestration_smoke(
        smoke_name="default client-routing orchestration smoke",
        request=request,
        workspace=workspace,
        runtime_home=runtime_home,
        temporal_executable=temporal_executable,
        startup_timeout=startup_timeout,
        completion_timeout=completion_timeout,
        stop_timeout=stop_timeout,
        namespace=namespace,
        task_queue_prefix=task_queue_prefix,
        worker_lanes=worker_lanes,
        environ=environ,
        supervisor_factory=supervisor_factory,
        submit_job=submit_job,
        default_client_routing=True,
    )


def run_live_orchestration_smoke(
    *,
    workspace: Path | str,
    runtime_home: Path | str,
    data_directory: Path | str,
    temporal_executable: Path | str | None = None,
    startup_timeout: float = DEFAULT_LIVE_ORCHESTRATION_SMOKE_STARTUP_TIMEOUT,
    completion_timeout: float = DEFAULT_LIVE_ORCHESTRATION_SMOKE_COMPLETION_TIMEOUT,
    stop_timeout: float = DEFAULT_LIVE_ORCHESTRATION_SMOKE_STOP_TIMEOUT,
    request_id: str = DEFAULT_LIVE_ORCHESTRATION_SMOKE_REQUEST_ID,
    namespace: str = DEFAULT_TEMPORAL_NAMESPACE,
    task_queue_prefix: str = DEFAULT_TASK_QUEUE_PREFIX,
    worker_lanes: Sequence[str | TaskQueueLane] = (
        DEFAULT_LIVE_ORCHESTRATION_SMOKE_LANES
    ),
    environ: Mapping[str, str] | None = None,
    supervisor_factory: SupervisorFactory = OrchestrationSupervisor,
    submit_job: SubmitJob | None = None,
) -> LiveOrchestrationSmokeResult:
    """Run an external HistData.com Temporal orchestration smoke."""
    request = default_live_orchestration_smoke_request(
        request_id=request_id,
        data_directory=data_directory,
    )
    return _run_orchestration_smoke(
        smoke_name="external HistData.com orchestration smoke",
        request=request,
        workspace=workspace,
        runtime_home=runtime_home,
        temporal_executable=temporal_executable,
        startup_timeout=startup_timeout,
        completion_timeout=completion_timeout,
        stop_timeout=stop_timeout,
        namespace=namespace,
        task_queue_prefix=task_queue_prefix,
        worker_lanes=worker_lanes,
        environ=environ,
        supervisor_factory=supervisor_factory,
        submit_job=submit_job,
        default_client_routing=False,
    )


def _run_orchestration_smoke(
    *,
    smoke_name: str,
    request: RunRequest,
    workspace: Path | str,
    runtime_home: Path | str,
    temporal_executable: Path | str | None,
    startup_timeout: float,
    completion_timeout: float,
    stop_timeout: float,
    namespace: str,
    task_queue_prefix: str,
    worker_lanes: Sequence[str | TaskQueueLane],
    environ: Mapping[str, str] | None,
    supervisor_factory: SupervisorFactory,
    submit_job: SubmitJob | None,
    default_client_routing: bool,
) -> LiveOrchestrationSmokeResult:
    """Run a Temporal orchestration, worker fleet, and supplied job request."""
    env = environ if environ is not None else os.environ
    executable = _temporal_executable_from_inputs(
        temporal_executable=temporal_executable,
        environ=env,
    )
    runtime_policy = build_orchestration_runtime_policy(
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
    started_status: OrchestrationStatus | None = None
    snapshot: OrchestrationJobSnapshot | None = None
    stopped_status: OrchestrationStatus | None = None
    worker_config: OrchestrationWorkerConfig | None = None
    job_config: OrchestrationWorkerConfig | None = None
    doctor: Mapping[str, Any] | None = None
    diagnostics: Mapping[str, Any] = {}
    try:
        started_status = supervisor.start(
            executable=executable,
            startup_timeout=startup_timeout,
        )
        if default_client_routing:
            worker_config = supervisor.client_worker_config(
                require_running=True,
            )
        else:
            worker_config = build_orchestration_worker_config(
                runtime_policy=supervisor.runtime_policy,
                namespace=namespace,
                task_queue_prefix=task_queue_prefix,
            )
            job_config = worker_config
        snapshot = _submit_live_smoke_job(
            request,
            config=job_config,
            supervisor=supervisor,
            completion_timeout=completion_timeout,
            submit_job=submit_job,
        )
        _validate_live_smoke_snapshot(snapshot)
        doctor = supervisor.doctor()
        _validate_live_smoke_routing(snapshot, worker_config, doctor)
        diagnostics = collect_live_orchestration_smoke_diagnostics(
            supervisor=supervisor,
            runtime_policy=supervisor.runtime_policy,
            request=request,
            snapshot=snapshot,
            doctor=doctor,
        )
    except Exception as err:
        diagnostics = collect_live_orchestration_smoke_diagnostics(
            supervisor=supervisor,
            runtime_policy=supervisor.runtime_policy,
            request=request,
            snapshot=snapshot,
            error=err,
        )
        raise LiveOrchestrationSmokeError(
            f"{smoke_name} failed: {err}",
            diagnostics,
        ) from err
    finally:
        try:
            stopped_status = _stop_live_orchestration(
                supervisor,
                stop_timeout=stop_timeout,
            )
            _raise_for_incomplete_orchestration_stop(stopped_status)
        except Exception as err:
            diagnostics = collect_live_orchestration_smoke_diagnostics(
                supervisor=supervisor,
                runtime_policy=supervisor.runtime_policy,
                request=request,
                snapshot=snapshot,
                doctor=doctor,
                error=err,
            )
            _attach_stop_status_diagnostics(
                diagnostics,
                status=stopped_status,
                error=err,
            )
            raise LiveOrchestrationSmokeError(
                f"{smoke_name} shutdown failed: {err}",
                diagnostics,
            ) from err
    if started_status is None or worker_config is None or snapshot is None:
        raise LiveOrchestrationSmokeError(
            f"{smoke_name} did not produce a complete result",
            diagnostics,
        )
    return LiveOrchestrationSmokeResult(
        request=request,
        worker_config=worker_config,
        started_status=started_status,
        stopped_status=stopped_status,
        snapshot=snapshot,
        doctor=dict(doctor or {}),
        client_routing=(
            "default_client_routing"
            if default_client_routing
            else "explicit_config"
        ),
        diagnostics=diagnostics,
    )


def collect_live_orchestration_smoke_diagnostics(
    *,
    supervisor: OrchestrationSupervisor,
    runtime_policy: OrchestrationRuntimePolicy,
    request: RunRequest | None = None,
    snapshot: OrchestrationJobSnapshot | None = None,
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
    config: OrchestrationWorkerConfig | None,
    supervisor: OrchestrationSupervisor,
    completion_timeout: float,
    submit_job: SubmitJob | None,
) -> OrchestrationJobSnapshot:
    kwargs: dict[str, Any] = {
        "supervisor": supervisor,
        "start_if_needed": False,
        "wait_for_result": True,
    }
    if config is not None:
        kwargs["config"] = config
    if submit_job is not None:
        return submit_job(request, **kwargs)
    return asyncio.run(
        asyncio.wait_for(
            submit_control_job(
                request,
                **kwargs,
            ),
            timeout=completion_timeout,
        )
    )


def _stop_live_orchestration(
    supervisor: OrchestrationSupervisor,
    *,
    stop_timeout: float,
) -> OrchestrationStatus:
    status = supervisor.stop(stop_timeout=stop_timeout)
    retry_timeout = min(2.0, max(0.1, stop_timeout))
    for _attempt in range(5):
        if status is None:
            raise LiveOrchestrationStopError(
                _format_orchestration_stop_failure(status),
                status,
            )
        if _orchestration_stop_is_complete(status):
            return status
        if status.state != "stopping":
            raise LiveOrchestrationStopError(
                _format_orchestration_stop_failure(status),
                status,
            )
        time.sleep(1.0)
        status = supervisor.stop(stop_timeout=retry_timeout)
    time.sleep(2.0)
    repaired = supervisor.status(repair=True)
    if repaired.state in _STOPPED_ORCHESTRATION_STATES:
        status = supervisor.stop(stop_timeout=retry_timeout)
        if _orchestration_stop_is_complete(status):
            return status
        raise LiveOrchestrationStopError(
            _format_orchestration_stop_failure(status),
            status,
        )
    raise LiveOrchestrationStopError(
        _format_orchestration_stop_failure(repaired), repaired
    )


def _orchestration_stop_is_complete(status: OrchestrationStatus | None) -> bool:
    return (
        status is not None
        and status.state in _STOPPED_ORCHESTRATION_STATES
        and not status.pids
    )


def _format_orchestration_stop_failure(
    status: OrchestrationStatus | None,
) -> str:
    if status is None:
        return "orchestration stop did not return a status"
    pid_summary = ", ".join(
        f"{component}={pid}" for component, pid in sorted(status.pids.items())
    )
    suffix = f"; remaining pids: {pid_summary}" if pid_summary else ""
    return f"orchestration stop did not complete: state={status.state}{suffix}"


def _raise_for_incomplete_orchestration_stop(
    status: OrchestrationStatus | None,
) -> OrchestrationStatus:
    if status is None:
        raise LiveOrchestrationStopError(
            _format_orchestration_stop_failure(status), status
        )
    if _orchestration_stop_is_complete(status):
        return status
    raise LiveOrchestrationStopError(
        _format_orchestration_stop_failure(status), status
    )


def _attach_stop_status_diagnostics(
    diagnostics: dict[str, Any],
    *,
    status: OrchestrationStatus | None,
    error: BaseException,
) -> None:
    stopped_status = status
    if stopped_status is None and isinstance(error, LiveOrchestrationStopError):
        stopped_status = error.status
    if stopped_status is not None:
        diagnostics["stopped_status"] = stopped_status.to_dict()


def _validate_live_smoke_snapshot(snapshot: OrchestrationJobSnapshot) -> None:
    if snapshot.lifecycle != JobLifecycle.SUCCEEDED:
        raise LiveOrchestrationSmokeError(
            "live orchestration smoke job did not succeed",
            {"snapshot": snapshot.to_dict()},
        )
    if snapshot.status != WorkStatus.COMPLETED:
        raise LiveOrchestrationSmokeError(
            "live orchestration smoke job did not complete",
            {"snapshot": snapshot.to_dict()},
        )
    if not snapshot.artifacts:
        raise LiveOrchestrationSmokeError(
            "live orchestration smoke job completed without artifact references",
            {"snapshot": snapshot.to_dict()},
        )


def _validate_live_smoke_routing(
    snapshot: OrchestrationJobSnapshot,
    worker_config: OrchestrationWorkerConfig,
    doctor: Mapping[str, Any],
) -> None:
    if snapshot.namespace and snapshot.namespace != worker_config.namespace:
        raise LiveOrchestrationSmokeError(
            "live orchestration smoke job used an unexpected namespace",
            {
                "expected": worker_config.namespace,
                "actual": snapshot.namespace,
                "snapshot": snapshot.to_dict(),
            },
        )
    expected_queue = worker_config.task_queues.orchestration
    if snapshot.task_queue and snapshot.task_queue != expected_queue:
        raise LiveOrchestrationSmokeError(
            "live orchestration smoke job used an unexpected task queue",
            {
                "expected": expected_queue,
                "actual": snapshot.task_queue,
                "snapshot": snapshot.to_dict(),
            },
        )

    frontend = doctor.get("frontend")
    target_host = (
        frontend.get("target_host") if isinstance(frontend, Mapping) else ""
    )
    if target_host and target_host != worker_config.target_host:
        raise LiveOrchestrationSmokeError(
            "live orchestration smoke doctor reported an unexpected frontend",
            {
                "expected": worker_config.target_host,
                "actual": target_host,
                "doctor": dict(doctor),
            },
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


def _diagnostic_log_paths(
    status_payload: Mapping[str, Any],
    runtime_policy: OrchestrationRuntimePolicy,
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
