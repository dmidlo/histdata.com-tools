"""Temporal client connection and job submission helpers."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from importlib import import_module
from inspect import isawaitable
from typing import Any, Mapping

from histdatacom.runtime_contracts import (
    ArtifactRef,
    JSONValue,
    RunRequest,
    WorkStatus,
)
from histdatacom.sidecar.control import (
    JobLogEntry,
    JobProgressSnapshot,
    SidecarJobList,
    SidecarJobSnapshot,
    lifecycle_from_work_status,
)
from histdatacom.sidecar.queues import (
    SidecarWorkerConfig,
    build_sidecar_worker_config,
)
from histdatacom.sidecar.supervisor import SidecarStatus, SidecarSupervisor
from histdatacom.sidecar.workflow_metadata import (
    TASK_QUEUE_METADATA_KEY,
    TOPOLOGY_METADATA_KEY,
    TOPOLOGY_SCHEMA_VERSION,
)

TEMPORAL_EXTRA_HINT = (
    "Temporal support requires the optional dependency surface. "
    "Install histdatacom[temporal] to use sidecar client and worker features."
)
DEFAULT_RUN_WORKFLOW_NAME = "HistDataRunWorkflow"


class TemporalDependencyError(RuntimeError):
    """Raised when Temporal SDK functionality is used without temporalio."""


class SidecarUnavailableError(RuntimeError):
    """Raised when a sidecar-backed run is requested but unavailable."""


@dataclass(frozen=True, slots=True)
class SidecarJobHandle:
    """Serializable metadata returned after submitting a sidecar job."""

    request_id: str
    workflow_id: str
    run_id: str
    task_queue: str
    namespace: str

    def to_dict(self) -> dict[str, str]:
        """Return JSON-compatible job handle metadata."""
        return {
            "request_id": self.request_id,
            "workflow_id": self.workflow_id,
            "run_id": self.run_id,
            "task_queue": self.task_queue,
            "namespace": self.namespace,
        }


@dataclass(frozen=True, slots=True)
class SidecarJobResult:
    """Serializable result for a submitted or observed sidecar job."""

    handle: SidecarJobHandle
    status: str
    result: Any = None
    sidecar_status: SidecarStatus | None = None
    snapshot: SidecarJobSnapshot | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return JSON-compatible sidecar job metadata and result payload."""
        payload = {
            "status": self.status,
            "handle": self.handle.to_dict(),
            "result": self.result,
        }
        if self.sidecar_status is not None:
            payload["sidecar_status"] = self.sidecar_status.to_dict()
        if self.snapshot is not None:
            payload["snapshot"] = self.snapshot.to_dict()
        return payload


async def connect_temporal_client(
    *,
    config: SidecarWorkerConfig | None = None,
    client_class: Any | None = None,
) -> Any:
    """Connect to the workspace Temporal sidecar frontend."""
    resolved_config = config or build_sidecar_worker_config()
    temporal_client_class = client_class or _load_temporal_client_class()
    connect = getattr(temporal_client_class, "connect", None)
    if connect is None:
        raise TypeError("Temporal client class must define connect()")
    return await _maybe_await(
        connect(
            resolved_config.target_host,
            namespace=resolved_config.namespace,
        )
    )


async def submit_run_request(
    request: RunRequest,
    *,
    config: SidecarWorkerConfig | None = None,
    client: Any | None = None,
    client_class: Any | None = None,
    workflow: Any = DEFAULT_RUN_WORKFLOW_NAME,
) -> SidecarJobHandle:
    """Submit a serialized HistData run request without activity imports."""
    resolved_config = config or build_sidecar_worker_config()
    temporal_client = client or await connect_temporal_client(
        config=resolved_config,
        client_class=client_class,
    )
    workflow_id = workflow_id_for_request(request)
    handle = await _maybe_await(
        temporal_client.start_workflow(
            workflow,
            run_request_payload(request, resolved_config),
            id=workflow_id,
            task_queue=resolved_config.task_queues.orchestration,
        )
    )
    return SidecarJobHandle(
        request_id=request.request_id,
        workflow_id=str(getattr(handle, "id", workflow_id)),
        run_id=str(getattr(handle, "run_id", "")),
        task_queue=resolved_config.task_queues.orchestration,
        namespace=resolved_config.namespace,
    )


async def submit_run_request_and_observe(
    request: RunRequest,
    *,
    config: SidecarWorkerConfig | None = None,
    client: Any | None = None,
    client_class: Any | None = None,
    supervisor: SidecarSupervisor | None = None,
    start_if_needed: bool = False,
    wait_for_result: bool = True,
    workflow: Any = DEFAULT_RUN_WORKFLOW_NAME,
) -> SidecarJobResult:
    """Submit a run through a healthy sidecar and optionally await its result."""
    resolved_config = config or build_sidecar_worker_config()
    resolved_supervisor = supervisor or SidecarSupervisor(
        runtime_policy=resolved_config.runtime_policy
    )
    sidecar_status = _ensure_sidecar_available(
        resolved_supervisor,
        start_if_needed=start_if_needed,
    )
    temporal_client = client or await connect_temporal_client(
        config=resolved_config,
        client_class=client_class,
    )
    workflow_id = workflow_id_for_request(request)
    raw_handle = await _start_workflow(
        temporal_client,
        workflow,
        run_request_payload(request, resolved_config),
        workflow_id=workflow_id,
        task_queue=resolved_config.task_queues.orchestration,
    )
    handle = _job_handle_from_workflow_handle(
        request,
        raw_handle,
        workflow_id=workflow_id,
        config=resolved_config,
    )
    if not wait_for_result:
        snapshot = SidecarJobSnapshot.from_handle(
            handle,
            sidecar_status=sidecar_status,
        )
        return SidecarJobResult(
            handle=handle,
            status="submitted",
            sidecar_status=sidecar_status,
            snapshot=snapshot,
        )

    result = await observe_workflow_result(raw_handle)
    snapshot = SidecarJobSnapshot.from_handle(
        handle,
        sidecar_status=sidecar_status,
    ).with_result(result)
    return SidecarJobResult(
        handle=handle,
        status="completed",
        result=result,
        sidecar_status=sidecar_status,
        snapshot=snapshot,
    )


def submit_run_request_and_observe_sync(
    request: RunRequest,
    **kwargs: Any,
) -> SidecarJobResult:
    """Synchronously submit and observe a sidecar run for CLI/API callers."""
    return asyncio.run(submit_run_request_and_observe(request, **kwargs))


async def submit_control_job(
    request: RunRequest,
    **kwargs: Any,
) -> SidecarJobSnapshot:
    """Submit a job and return the GUI-ready control snapshot."""
    result = await submit_run_request_and_observe(request, **kwargs)
    if result.snapshot is not None:
        return result.snapshot
    return SidecarJobSnapshot.from_handle(
        result.handle,
        sidecar_status=result.sidecar_status,
    ).with_result(result.result)


def submit_control_job_sync(
    request: RunRequest,
    **kwargs: Any,
) -> SidecarJobSnapshot:
    """Synchronously submit a job and return a control snapshot."""
    return asyncio.run(submit_control_job(request, **kwargs))


async def observe_workflow_result(workflow_handle: Any) -> Any:
    """Return the completed workflow result from a Temporal handle."""
    result = getattr(workflow_handle, "result", None)
    if result is None:
        raise TypeError("Temporal workflow handle must define result()")
    return await _maybe_await(result())


async def inspect_job_status(
    workflow_id: str,
    *,
    run_id: str = "",
    config: SidecarWorkerConfig | None = None,
    client: Any | None = None,
    client_class: Any | None = None,
    supervisor: SidecarSupervisor | None = None,
) -> SidecarJobSnapshot:
    """Return a GUI-ready status snapshot for one sidecar job."""
    resolved_config = config or build_sidecar_worker_config()
    temporal_client = client or await connect_temporal_client(
        config=resolved_config,
        client_class=client_class,
    )
    workflow_handle = _workflow_handle_for_job(
        temporal_client,
        workflow_id,
        run_id=run_id,
    )
    handle = _job_handle_from_workflow_identity(
        workflow_id,
        run_id=run_id or str(getattr(workflow_handle, "run_id", "") or ""),
        config=resolved_config,
    )
    sidecar_status = _sidecar_status(supervisor)
    return await _inspect_workflow_handle(
        workflow_handle,
        handle=handle,
        sidecar_status=sidecar_status,
    )


def inspect_job_status_sync(
    workflow_id: str,
    **kwargs: Any,
) -> SidecarJobSnapshot:
    """Synchronously return a job status snapshot."""
    return asyncio.run(inspect_job_status(workflow_id, **kwargs))


async def list_job_statuses(
    *,
    query: str = "",
    config: SidecarWorkerConfig | None = None,
    client: Any | None = None,
    client_class: Any | None = None,
) -> SidecarJobList:
    """List known HistData job handles without reading workflow histories."""
    resolved_config = config or build_sidecar_worker_config()
    temporal_client = client or await connect_temporal_client(
        config=resolved_config,
        client_class=client_class,
    )
    list_workflows = getattr(temporal_client, "list_workflows", None)
    if list_workflows is None:
        raise TypeError("Temporal client must define list_workflows()")
    raw_jobs = list_workflows(
        query=query or "WorkflowType='HistDataRunWorkflow'"
    )
    descriptions = await _collect_workflow_descriptions(raw_jobs)
    return SidecarJobList(
        jobs=tuple(
            _snapshot_from_workflow_description(
                description,
                config=resolved_config,
            )
            for description in descriptions
        )
    )


def list_job_statuses_sync(**kwargs: Any) -> SidecarJobList:
    """Synchronously list known sidecar jobs."""
    return asyncio.run(list_job_statuses(**kwargs))


async def get_job_progress(
    workflow_id: str,
    **kwargs: Any,
) -> JobProgressSnapshot | None:
    """Return progress for one job."""
    return (await inspect_job_status(workflow_id, **kwargs)).progress


def get_job_progress_sync(
    workflow_id: str,
    **kwargs: Any,
) -> JobProgressSnapshot | None:
    """Synchronously return progress for one job."""
    return asyncio.run(get_job_progress(workflow_id, **kwargs))


async def get_job_logs(
    workflow_id: str,
    **kwargs: Any,
) -> tuple[JobLogEntry, ...]:
    """Return control API logs derived from workflow status events."""
    snapshot: SidecarJobSnapshot = await inspect_job_status(
        workflow_id, **kwargs
    )
    return snapshot.logs


def get_job_logs_sync(
    workflow_id: str,
    **kwargs: Any,
) -> tuple[JobLogEntry, ...]:
    """Synchronously return job logs."""
    return asyncio.run(get_job_logs(workflow_id, **kwargs))


async def get_job_artifacts(
    workflow_id: str,
    **kwargs: Any,
) -> tuple[ArtifactRef, ...]:
    """Return artifacts discovered for one job."""
    snapshot: SidecarJobSnapshot = await inspect_job_status(
        workflow_id, **kwargs
    )
    return snapshot.artifacts


def get_job_artifacts_sync(
    workflow_id: str,
    **kwargs: Any,
) -> tuple[ArtifactRef, ...]:
    """Synchronously return job artifacts."""
    return asyncio.run(get_job_artifacts(workflow_id, **kwargs))


async def get_job_result(
    workflow_id: str,
    *,
    run_id: str = "",
    config: SidecarWorkerConfig | None = None,
    client: Any | None = None,
    client_class: Any | None = None,
    supervisor: SidecarSupervisor | None = None,
) -> SidecarJobSnapshot:
    """Return a snapshot with the workflow result payload attached."""
    resolved_config = config or build_sidecar_worker_config()
    temporal_client = client or await connect_temporal_client(
        config=resolved_config,
        client_class=client_class,
    )
    workflow_handle = _workflow_handle_for_job(
        temporal_client,
        workflow_id,
        run_id=run_id,
    )
    handle = _job_handle_from_workflow_identity(
        workflow_id,
        run_id=run_id or str(getattr(workflow_handle, "run_id", "") or ""),
        config=resolved_config,
    )
    sidecar_status = _sidecar_status(supervisor)
    snapshot = await _inspect_workflow_handle(
        workflow_handle,
        handle=handle,
        sidecar_status=sidecar_status,
    )
    return snapshot.with_result(await observe_workflow_result(workflow_handle))


def get_job_result_sync(
    workflow_id: str,
    **kwargs: Any,
) -> SidecarJobSnapshot:
    """Synchronously return a job snapshot with result payload."""
    return asyncio.run(get_job_result(workflow_id, **kwargs))


async def cancel_job(
    workflow_id: str,
    *,
    run_id: str = "",
    reason: str = "",
    config: SidecarWorkerConfig | None = None,
    client: Any | None = None,
    client_class: Any | None = None,
    supervisor: SidecarSupervisor | None = None,
) -> SidecarJobSnapshot:
    """Request cancellation and return explicit cancellation state."""
    resolved_config = config or build_sidecar_worker_config()
    temporal_client = client or await connect_temporal_client(
        config=resolved_config,
        client_class=client_class,
    )
    workflow_handle = _workflow_handle_for_job(
        temporal_client,
        workflow_id,
        run_id=run_id,
    )
    handle = _job_handle_from_workflow_identity(
        workflow_id,
        run_id=run_id or str(getattr(workflow_handle, "run_id", "") or ""),
        config=resolved_config,
    )
    snapshot = await _inspect_workflow_handle(
        workflow_handle,
        handle=handle,
        sidecar_status=_sidecar_status(supervisor),
    )
    cancel = getattr(workflow_handle, "cancel", None)
    if cancel is None:
        raise TypeError("Temporal workflow handle must define cancel()")
    await _maybe_await(cancel())
    return snapshot.request_cancel(reason=reason)


def cancel_job_sync(
    workflow_id: str,
    **kwargs: Any,
) -> SidecarJobSnapshot:
    """Synchronously request job cancellation."""
    return asyncio.run(cancel_job(workflow_id, **kwargs))


async def retry_job(
    workflow_id: str,
    *,
    reason: str = "",
    **kwargs: Any,
) -> SidecarJobSnapshot:
    """Represent retry intent for a job in the control API."""
    return (await inspect_job_status(workflow_id, **kwargs)).request_retry(
        reason=reason
    )


def retry_job_sync(
    workflow_id: str,
    **kwargs: Any,
) -> SidecarJobSnapshot:
    """Synchronously represent retry intent for a job."""
    return asyncio.run(retry_job(workflow_id, **kwargs))


async def resume_job(
    workflow_id: str,
    *,
    reason: str = "",
    **kwargs: Any,
) -> SidecarJobSnapshot:
    """Represent resume intent for a job in the control API."""
    return (await inspect_job_status(workflow_id, **kwargs)).request_resume(
        reason=reason
    )


def resume_job_sync(
    workflow_id: str,
    **kwargs: Any,
) -> SidecarJobSnapshot:
    """Synchronously represent resume intent for a job."""
    return asyncio.run(resume_job(workflow_id, **kwargs))


def workflow_id_for_request(request: RunRequest) -> str:
    """Return the stable Temporal workflow ID for a run request."""
    request_id = request.request_id.strip() or "request"
    return f"histdatacom-{request_id}"


def run_request_payload(
    request: RunRequest,
    config: SidecarWorkerConfig,
) -> dict[str, Any]:
    """Return a request payload enriched with sidecar workflow metadata."""
    payload: dict[str, Any] = request.to_dict()
    metadata = dict(payload.get("metadata") or {})
    metadata[TASK_QUEUE_METADATA_KEY] = config.task_queues.to_dict()
    metadata[TOPOLOGY_METADATA_KEY] = TOPOLOGY_SCHEMA_VERSION
    payload["metadata"] = metadata
    return payload


def _ensure_sidecar_available(
    supervisor: SidecarSupervisor,
    *,
    start_if_needed: bool,
) -> SidecarStatus:
    status = supervisor.status(repair=True)
    if status.running:
        return status
    if not start_if_needed:
        raise SidecarUnavailableError(
            "Temporal sidecar is not running. Start it with "
            "`histdatacom sidecar start` or enable sidecar_start."
        )
    started = supervisor.start()
    if started.running:
        return started
    raise SidecarUnavailableError(
        "Temporal sidecar could not be started: " f"{started.message}"
    )


async def _start_workflow(
    temporal_client: Any,
    workflow: Any,
    payload: Mapping[str, JSONValue],
    *,
    workflow_id: str,
    task_queue: str,
) -> Any:
    return await _maybe_await(
        temporal_client.start_workflow(
            workflow,
            dict(payload),
            id=workflow_id,
            task_queue=task_queue,
        )
    )


def _job_handle_from_workflow_handle(
    request: RunRequest,
    workflow_handle: Any,
    *,
    workflow_id: str,
    config: SidecarWorkerConfig,
) -> SidecarJobHandle:
    return SidecarJobHandle(
        request_id=request.request_id,
        workflow_id=str(getattr(workflow_handle, "id", workflow_id)),
        run_id=str(getattr(workflow_handle, "run_id", "")),
        task_queue=config.task_queues.orchestration,
        namespace=config.namespace,
    )


def _job_handle_from_workflow_identity(
    workflow_id: str,
    *,
    run_id: str,
    config: SidecarWorkerConfig,
) -> SidecarJobHandle:
    request_id = workflow_id
    if workflow_id.startswith("histdatacom-"):
        request_id = workflow_id.removeprefix("histdatacom-")
    return SidecarJobHandle(
        request_id=request_id,
        workflow_id=workflow_id,
        run_id=run_id,
        task_queue=config.task_queues.orchestration,
        namespace=config.namespace,
    )


async def _inspect_workflow_handle(
    workflow_handle: Any,
    *,
    handle: SidecarJobHandle,
    sidecar_status: SidecarStatus | None,
) -> SidecarJobSnapshot:
    status_payload = await _query_workflow_status(workflow_handle)
    snapshot = SidecarJobSnapshot.from_workflow_status(
        handle,
        status_payload,
        sidecar_status=sidecar_status,
    )
    if snapshot.progress is None:
        return snapshot
    return replace(
        snapshot,
        logs=_logs_from_progress(snapshot.progress),
    )


async def _query_workflow_status(workflow_handle: Any) -> Mapping[str, Any]:
    query = getattr(workflow_handle, "query", None)
    if query is None:
        raise TypeError("Temporal workflow handle must define query()")
    status_payload = await _maybe_await(query("status"))
    if not isinstance(status_payload, Mapping):
        raise TypeError("Workflow status query must return a mapping")
    return status_payload


def _workflow_handle_for_job(
    temporal_client: Any,
    workflow_id: str,
    *,
    run_id: str = "",
) -> Any:
    get_workflow_handle = getattr(
        temporal_client,
        "get_workflow_handle",
        None,
    )
    if get_workflow_handle is None:
        raise TypeError("Temporal client must define get_workflow_handle()")
    if run_id:
        return get_workflow_handle(workflow_id, run_id=run_id)
    return get_workflow_handle(workflow_id)


def _logs_from_progress(
    progress: JobProgressSnapshot,
) -> tuple[JobLogEntry, ...]:
    return tuple(
        JobLogEntry(
            source=str(event.metadata.get("source") or event.stage),
            message=event.message,
            level=str(
                event.metadata.get("level")
                or ("error" if event.status == WorkStatus.FAILED else "info")
            ),
            timestamp_utc=event.timestamp_utc,
            metadata=event.metadata,
        )
        for event in progress.events
    )


async def _collect_workflow_descriptions(raw_jobs: Any) -> list[Any]:
    jobs = await _maybe_await(raw_jobs)
    if hasattr(jobs, "__aiter__"):
        collected = []
        async for job in jobs:
            collected.append(job)
        return collected
    return list(jobs)


def _snapshot_from_workflow_description(
    description: Any,
    *,
    config: SidecarWorkerConfig,
) -> SidecarJobSnapshot:
    execution = getattr(description, "execution", description)
    workflow_id = str(
        getattr(execution, "workflow_id", "")
        or getattr(description, "workflow_id", "")
    )
    run_id = str(
        getattr(execution, "run_id", "") or getattr(description, "run_id", "")
    )
    raw_status = str(getattr(description, "status", "") or "")
    status = _status_from_temporal_description(raw_status)
    handle = _job_handle_from_workflow_identity(
        workflow_id,
        run_id=run_id,
        config=config,
    )
    return SidecarJobSnapshot.from_handle(
        handle,
        lifecycle=lifecycle_from_work_status(status),
        status=status,
    )


def _status_from_temporal_description(raw_status: str) -> WorkStatus:
    normalized = raw_status.upper().removeprefix("WORKFLOW_EXECUTION_STATUS_")
    if normalized in {"RUNNING", "CONTINUED_AS_NEW"}:
        return WorkStatus.UNKNOWN
    return WorkStatus.from_value(normalized)


def _sidecar_status(
    supervisor: SidecarSupervisor | None,
) -> SidecarStatus | None:
    if supervisor is None:
        return None
    return supervisor.status(repair=True)


def _load_temporal_client_class() -> Any:
    try:
        return getattr(import_module("temporalio.client"), "Client")
    except ModuleNotFoundError as err:
        if (err.name or "").split(".")[0] == "temporalio":
            raise TemporalDependencyError(TEMPORAL_EXTRA_HINT) from err
        raise


async def _maybe_await(value: Any) -> Any:
    if isawaitable(value):
        return await value
    return value
