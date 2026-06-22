"""Temporal client connection and job submission helpers."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from importlib import import_module
from inspect import isawaitable
from pathlib import Path
from typing import Any, Mapping

from histdatacom.cancellation import (
    PartialArtifactDisposition,
    cleanup_partial_artifacts,
    operation_resume_policy,
)
from histdatacom.manifest_store import (
    STATUS_STORE_REF_KEY,
    ManifestStatusStore,
)
from histdatacom.runtime_contracts import (
    ArtifactRef,
    JSONValue,
    RunRequest,
    WorkStatus,
)
from histdatacom.sidecar.control import (
    JobLifecycle,
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
from histdatacom.sidecar.resources import SidecarResourceError
from histdatacom.sidecar.supervisor import SidecarStatus, SidecarSupervisor
from histdatacom.sidecar.workflow_metadata import (
    TASK_QUEUE_METADATA_KEY,
    TOPOLOGY_METADATA_KEY,
    TOPOLOGY_SCHEMA_VERSION,
)

TEMPORAL_EXTRA_HINT = (
    "Temporal support requires temporalio. Base histdatacom installs include "
    "this dependency; reinstall histdatacom with dependencies enabled or "
    "install the temporal compatibility extra."
)
DEFAULT_RUN_WORKFLOW_NAME = "HistDataRunWorkflow"
RUN_REQUEST_METADATA_KEY = "run_request"
CONTROL_ATTEMPTS_METADATA_KEY = "control_attempts"
CONTROL_EXECUTION_METADATA_KEY = "control_execution"
TEMPORAL_EXECUTION_STATUS_PREFIX = "WORKFLOW_EXECUTION_STATUS_"
TEMPORAL_EXECUTION_STATUS_METADATA_KEY = "temporal_execution_status"


class SidecarUnavailableError(RuntimeError):
    """Raised when a sidecar-backed run is requested but unavailable."""


class TemporalDependencyError(SidecarUnavailableError):
    """Raised when Temporal SDK functionality is used without temporalio."""


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
    status_store: ManifestStatusStore | None = None,
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
    job_handle = SidecarJobHandle(
        request_id=request.request_id,
        workflow_id=str(getattr(handle, "id", workflow_id)),
        run_id=str(getattr(handle, "run_id", "")),
        task_queue=resolved_config.task_queues.orchestration,
        namespace=resolved_config.namespace,
    )
    _persist_job_snapshot(
        _snapshot_with_run_request(
            SidecarJobSnapshot.from_handle(job_handle),
            request,
        ),
        config=resolved_config,
        status_store=status_store,
    )
    return job_handle


async def submit_run_request_and_observe(
    request: RunRequest,
    *,
    config: SidecarWorkerConfig | None = None,
    client: Any | None = None,
    client_class: Any | None = None,
    supervisor: SidecarSupervisor | None = None,
    start_if_needed: bool = False,
    wait_for_result: bool = True,
    status_store: ManifestStatusStore | None = None,
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
    submitted_snapshot = _persist_job_snapshot(
        _snapshot_with_run_request(
            SidecarJobSnapshot.from_handle(
                handle,
                sidecar_status=sidecar_status,
            ),
            request,
        ),
        config=resolved_config,
        status_store=status_store,
    )
    if not wait_for_result:
        return SidecarJobResult(
            handle=handle,
            status="submitted",
            sidecar_status=sidecar_status,
            snapshot=submitted_snapshot,
        )

    result = await observe_workflow_result(raw_handle)
    snapshot = _persist_job_snapshot(
        submitted_snapshot.with_result(result),
        config=resolved_config,
        status_store=status_store,
    )
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
    status_store: ManifestStatusStore | None = None,
    offline: bool = False,
    store_fallback: bool = True,
) -> SidecarJobSnapshot:
    """Return a GUI-ready status snapshot for one sidecar job."""
    resolved_config = config or build_sidecar_worker_config()
    if offline:
        return _stored_job_snapshot_or_raise(
            workflow_id,
            config=resolved_config,
            status_store=status_store,
        )
    try:
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
        return _persist_job_snapshot(
            snapshot,
            config=resolved_config,
            status_store=status_store,
        )
    except Exception as err:
        if store_fallback:
            return _stored_job_snapshot_or_raise(
                workflow_id,
                config=resolved_config,
                status_store=status_store,
                cause=err,
            )
        raise


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
    status_store: ManifestStatusStore | None = None,
    offline: bool = False,
    store_fallback: bool = True,
    status: WorkStatus | str | None = None,
    limit: int | None = None,
) -> SidecarJobList:
    """List known HistData job handles without reading workflow histories."""
    resolved_config = config or build_sidecar_worker_config()
    if offline:
        return list_stored_job_statuses(
            config=resolved_config,
            status_store=status_store,
            status=status,
            limit=limit,
        )
    try:
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
        snapshots = tuple(
            _snapshot_from_workflow_description(
                description,
                config=resolved_config,
            )
            for description in descriptions
        )
        for snapshot in snapshots:
            _persist_job_snapshot(
                snapshot,
                config=resolved_config,
                status_store=status_store,
            )
        return SidecarJobList(jobs=snapshots)
    except Exception:
        if store_fallback:
            return list_stored_job_statuses(
                config=resolved_config,
                status_store=status_store,
                status=status,
                limit=limit,
            )
        raise


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
    return tuple(snapshot.logs)


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
    return tuple(snapshot.artifacts)


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
    status_store: ManifestStatusStore | None = None,
    offline: bool = False,
    store_fallback: bool = True,
) -> SidecarJobSnapshot:
    """Return a snapshot with the workflow result payload attached."""
    resolved_config = config or build_sidecar_worker_config()
    if offline:
        return _stored_job_snapshot_or_raise(
            workflow_id,
            config=resolved_config,
            status_store=status_store,
        )
    try:
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
        result_snapshot = snapshot.with_result(
            await observe_workflow_result(workflow_handle)
        )
        return _persist_job_snapshot(
            result_snapshot,
            config=resolved_config,
            status_store=status_store,
        )
    except Exception as err:
        if store_fallback:
            return _stored_job_snapshot_or_raise(
                workflow_id,
                config=resolved_config,
                status_store=status_store,
                cause=err,
            )
        raise


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
    status_store: ManifestStatusStore | None = None,
    offline: bool = False,
) -> SidecarJobSnapshot:
    """Request cancellation and return explicit cancellation state."""
    resolved_config = config or build_sidecar_worker_config()
    if offline:
        snapshot = _stored_job_snapshot_or_raise(
            workflow_id,
            config=resolved_config,
            status_store=status_store,
        ).request_cancel(
            reason=reason,
            message="Cancellation requested while offline.",
            metadata={"offline": True},
        )
        return _persist_job_snapshot(
            snapshot,
            config=resolved_config,
            status_store=status_store,
        )
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
    return _persist_job_snapshot(
        snapshot.request_cancel(reason=reason),
        config=resolved_config,
        status_store=status_store,
    )


def cancel_job_sync(
    workflow_id: str,
    **kwargs: Any,
) -> SidecarJobSnapshot:
    """Synchronously request job cancellation."""
    return asyncio.run(cancel_job(workflow_id, **kwargs))


async def retry_job(
    workflow_id: str,
    *,
    run_id: str = "",
    reason: str = "",
    config: SidecarWorkerConfig | None = None,
    client: Any | None = None,
    client_class: Any | None = None,
    supervisor: SidecarSupervisor | None = None,
    status_store: ManifestStatusStore | None = None,
    offline: bool = False,
    reuse_completed_artifacts: bool = True,
    workflow: Any = DEFAULT_RUN_WORKFLOW_NAME,
) -> SidecarJobSnapshot:
    """Start a deterministic replacement workflow for retry."""
    return await _start_replacement_job(
        workflow_id,
        action="retry",
        run_id=run_id,
        reason=reason,
        config=config,
        client=client,
        client_class=client_class,
        supervisor=supervisor,
        status_store=status_store,
        offline=offline,
        reuse_completed_artifacts=reuse_completed_artifacts,
        workflow=workflow,
    )


def retry_job_sync(
    workflow_id: str,
    **kwargs: Any,
) -> SidecarJobSnapshot:
    """Synchronously start a deterministic retry replacement workflow."""
    return asyncio.run(retry_job(workflow_id, **kwargs))


async def resume_job(
    workflow_id: str,
    *,
    run_id: str = "",
    reason: str = "",
    config: SidecarWorkerConfig | None = None,
    client: Any | None = None,
    client_class: Any | None = None,
    supervisor: SidecarSupervisor | None = None,
    status_store: ManifestStatusStore | None = None,
    offline: bool = False,
    reuse_completed_artifacts: bool = True,
    workflow: Any = DEFAULT_RUN_WORKFLOW_NAME,
) -> SidecarJobSnapshot:
    """Start a deterministic replacement workflow for resume."""
    return await _start_replacement_job(
        workflow_id,
        action="resume",
        run_id=run_id,
        reason=reason,
        config=config,
        client=client,
        client_class=client_class,
        supervisor=supervisor,
        status_store=status_store,
        offline=offline,
        reuse_completed_artifacts=reuse_completed_artifacts,
        workflow=workflow,
    )


def resume_job_sync(
    workflow_id: str,
    **kwargs: Any,
) -> SidecarJobSnapshot:
    """Synchronously start a deterministic resume replacement workflow."""
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
    metadata[STATUS_STORE_REF_KEY] = sidecar_job_store(
        config
    ).status_store_ref()
    payload["metadata"] = metadata
    return payload


def sidecar_job_store_root(
    config: SidecarWorkerConfig | None = None,
) -> Path:
    """Return the workspace-scoped root used for durable job snapshots."""
    resolved_config = config or build_sidecar_worker_config()
    return Path(resolved_config.runtime_policy.paths.manifests_dir)


def sidecar_job_store_path(
    config: SidecarWorkerConfig | None = None,
) -> Path:
    """Return the SQLite path for durable sidecar job snapshots."""
    return Path(
        ManifestStatusStore.path_for_root(sidecar_job_store_root(config))
    )


def sidecar_job_store(
    config: SidecarWorkerConfig | None = None,
) -> ManifestStatusStore:
    """Return the workspace-scoped durable sidecar job status store."""
    return ManifestStatusStore(sidecar_job_store_root(config))


def list_stored_job_statuses(
    *,
    config: SidecarWorkerConfig | None = None,
    status_store: ManifestStatusStore | None = None,
    status: WorkStatus | str | None = None,
    limit: int | None = None,
) -> SidecarJobList:
    """List durable sidecar jobs without querying Temporal."""
    store = status_store or sidecar_job_store(config)
    return SidecarJobList(
        jobs=tuple(
            SidecarJobSnapshot.from_dict(payload)
            for payload in store.list_job_snapshots(
                status=status,
                limit=limit,
            )
        )
    )


async def _start_replacement_job(
    workflow_id: str,
    *,
    action: str,
    run_id: str = "",
    reason: str = "",
    config: SidecarWorkerConfig | None = None,
    client: Any | None = None,
    client_class: Any | None = None,
    supervisor: SidecarSupervisor | None = None,
    status_store: ManifestStatusStore | None = None,
    offline: bool = False,
    reuse_completed_artifacts: bool = True,
    workflow: Any = DEFAULT_RUN_WORKFLOW_NAME,
) -> SidecarJobSnapshot:
    if offline:
        raise SidecarUnavailableError(
            "Retry and resume require a live Temporal sidecar; "
            "--offline is only supported for read-only job commands."
        )
    if action not in {"retry", "resume"}:
        raise ValueError(f"unsupported control action: {action}")

    resolved_config = config or build_sidecar_worker_config()
    temporal_client = client or await connect_temporal_client(
        config=resolved_config,
        client_class=client_class,
    )
    snapshot = await inspect_job_status(
        workflow_id,
        run_id=run_id,
        config=resolved_config,
        client=temporal_client,
        supervisor=supervisor,
        status_store=status_store,
        store_fallback=True,
    )
    stage = _snapshot_stage(snapshot)
    policy = operation_resume_policy(stage)
    cleanup_results = _cleanup_partial_artifacts_for_snapshot(snapshot, stage)
    attempt = _next_control_attempt(snapshot, action)
    replacement_request = _replacement_run_request(
        snapshot,
        action=action,
        stage=policy.stage,
        reason=reason,
        attempt=attempt,
        reuse_completed_artifacts=reuse_completed_artifacts,
        cleanup_results=cleanup_results,
    )
    replacement_workflow_id = workflow_id_for_request(replacement_request)
    raw_handle = await _start_workflow(
        temporal_client,
        workflow,
        run_request_payload(replacement_request, resolved_config),
        workflow_id=replacement_workflow_id,
        task_queue=resolved_config.task_queues.orchestration,
    )
    replacement_handle = _job_handle_from_workflow_handle(
        replacement_request,
        raw_handle,
        workflow_id=replacement_workflow_id,
        config=resolved_config,
    )
    execution_metadata = _control_execution_metadata(
        snapshot,
        replacement_handle,
        action=action,
        stage=stage,
        normalized_stage=policy.stage,
        reason=reason,
        attempt=attempt,
        reuse_completed_artifacts=reuse_completed_artifacts,
        cleanup_results=cleanup_results,
    )
    requested_snapshot = _control_requested_snapshot(
        snapshot,
        action=action,
        reason=reason,
        stage=stage,
        metadata=execution_metadata,
    )
    _persist_job_snapshot(
        _snapshot_with_control_metadata(
            requested_snapshot,
            action=action,
            attempt=attempt,
            execution_metadata=execution_metadata,
        ),
        config=resolved_config,
        status_store=status_store,
    )

    replacement_snapshot = _control_replacement_snapshot(
        replacement_handle,
        action=action,
        sidecar_status=_sidecar_status(supervisor),
        metadata=execution_metadata,
    )
    return _persist_job_snapshot(
        _snapshot_with_run_request(
            _snapshot_with_metadata(
                replacement_snapshot,
                {CONTROL_EXECUTION_METADATA_KEY: execution_metadata},
            ),
            replacement_request,
        ),
        config=resolved_config,
        status_store=status_store,
    )


def _persist_job_snapshot(
    snapshot: SidecarJobSnapshot,
    *,
    config: SidecarWorkerConfig,
    status_store: ManifestStatusStore | None = None,
) -> SidecarJobSnapshot:
    store = status_store or sidecar_job_store(config)
    snapshot = _merge_stored_snapshot_metadata(snapshot, store)
    store.write_job_snapshot(snapshot)
    return snapshot


def _snapshot_with_run_request(
    snapshot: SidecarJobSnapshot,
    request: RunRequest,
) -> SidecarJobSnapshot:
    return _snapshot_with_metadata(
        snapshot,
        {RUN_REQUEST_METADATA_KEY: request.to_dict()},
    )


def _snapshot_with_metadata(
    snapshot: SidecarJobSnapshot,
    metadata: Mapping[str, JSONValue],
) -> SidecarJobSnapshot:
    return replace(
        snapshot,
        metadata={
            **snapshot.metadata,
            **dict(metadata),
        },
    )


def _merge_stored_snapshot_metadata(
    snapshot: SidecarJobSnapshot,
    store: ManifestStatusStore,
) -> SidecarJobSnapshot:
    payload = store.get_job_snapshot(snapshot.job_id or snapshot.workflow_id)
    if payload is None:
        return snapshot
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        return snapshot
    return replace(
        snapshot,
        metadata={
            **dict(metadata),
            **snapshot.metadata,
        },
    )


def _replacement_run_request(
    snapshot: SidecarJobSnapshot,
    *,
    action: str,
    stage: str,
    reason: str,
    attempt: int,
    reuse_completed_artifacts: bool,
    cleanup_results: tuple[Any, ...],
) -> RunRequest:
    request = _run_request_from_snapshot(snapshot)
    policy = operation_resume_policy(stage)
    request_id = _replacement_request_id(
        request.request_id,
        action=action,
        stage=stage,
        attempt=attempt,
    )
    metadata = dict(request.metadata)
    metadata[CONTROL_EXECUTION_METADATA_KEY] = {
        "action": action,
        "parent_job_id": snapshot.job_id,
        "parent_workflow_id": snapshot.workflow_id,
        "previous_run_id": snapshot.run_id,
        "stage": stage,
        "reason": reason,
        "attempt": attempt,
        "reuse_completed_artifacts": reuse_completed_artifacts,
        "partial_artifact_disposition": (
            policy.partial_artifact_disposition.value
        ),
        "resume_policy": policy.to_dict(),
        "cleanup": [result.to_dict() for result in cleanup_results],
    }
    return replace(
        request,
        request_id=request_id,
        metadata=metadata,
    )


def _run_request_from_snapshot(snapshot: SidecarJobSnapshot) -> RunRequest:
    payload = snapshot.metadata.get(RUN_REQUEST_METADATA_KEY)
    if not isinstance(payload, Mapping):
        raise SidecarUnavailableError(
            "Cannot retry/resume job without a persisted RunRequest snapshot. "
            "Submit the job again with the current sidecar client first."
        )
    return RunRequest.from_dict(payload)


def _replacement_request_id(
    request_id: str,
    *,
    action: str,
    stage: str,
    attempt: int,
) -> str:
    base = request_id.strip() or "request"
    stage_slug = _slug(stage or "job")
    return f"{base}-{action}-{stage_slug}-{attempt:03d}"


def _slug(value: str) -> str:
    chars = [
        char.lower() if char.isalnum() else "-"
        for char in str(value or "").strip()
    ]
    slug = "-".join(filter(None, "".join(chars).split("-")))
    return slug or "job"


def _next_control_attempt(snapshot: SidecarJobSnapshot, action: str) -> int:
    attempts = snapshot.metadata.get(CONTROL_ATTEMPTS_METADATA_KEY)
    if not isinstance(attempts, Mapping):
        return 1
    value = attempts.get(action, 0)
    if not isinstance(value, str | int | float):
        return 1
    try:
        return int(value or 0) + 1
    except (TypeError, ValueError):
        return 1


def _cleanup_partial_artifacts_for_snapshot(
    snapshot: SidecarJobSnapshot,
    stage: str,
) -> tuple[Any, ...]:
    policy = operation_resume_policy(stage)
    if (
        policy.partial_artifact_disposition
        != PartialArtifactDisposition.REMOVE_TEMP
    ):
        return ()
    candidates: set[Path] = set()
    for artifact in snapshot.artifacts:
        if not artifact.path:
            continue
        artifact_path = Path(artifact.path)
        parent = artifact_path.parent
        if not parent.exists():
            continue
        for pattern in policy.partial_artifact_patterns:
            candidates.update(parent.glob(pattern))
            if not pattern.startswith("."):
                candidates.update(parent.glob(f".{pattern}"))
        candidates.update(parent.glob(f".{artifact_path.name}.*.tmp"))
    return tuple(
        cleanup_partial_artifacts(
            tuple(sorted(candidates, key=lambda path: str(path)))
        )
    )


def _control_execution_metadata(
    snapshot: SidecarJobSnapshot,
    replacement_handle: SidecarJobHandle,
    *,
    action: str,
    stage: str,
    normalized_stage: str,
    reason: str,
    attempt: int,
    reuse_completed_artifacts: bool,
    cleanup_results: tuple[Any, ...],
) -> dict[str, JSONValue]:
    policy = operation_resume_policy(stage)
    return {
        "action": action,
        "parent_job_id": snapshot.job_id,
        "parent_workflow_id": snapshot.workflow_id,
        "previous_run_id": snapshot.run_id,
        "replacement_request_id": replacement_handle.request_id,
        "replacement_workflow_id": replacement_handle.workflow_id,
        "replacement_handle": {
            key: str(value)
            for key, value in replacement_handle.to_dict().items()
        },
        "stage": normalized_stage,
        "requested_stage": stage,
        "reason": reason,
        "attempt": attempt,
        "reuse_completed_artifacts": reuse_completed_artifacts,
        "partial_artifact_disposition": (
            policy.partial_artifact_disposition.value
        ),
        "resume_policy": policy.to_dict(),
        "cleanup": [result.to_dict() for result in cleanup_results],
    }


def _control_requested_snapshot(
    snapshot: SidecarJobSnapshot,
    *,
    action: str,
    reason: str,
    stage: str,
    metadata: Mapping[str, JSONValue],
) -> SidecarJobSnapshot:
    if action == "retry":
        return snapshot.request_retry(
            reason=reason,
            stage=stage,
            metadata=metadata,
        )
    return snapshot.request_resume(
        reason=reason,
        stage=stage,
        metadata=metadata,
    )


def _snapshot_with_control_metadata(
    snapshot: SidecarJobSnapshot,
    *,
    action: str,
    attempt: int,
    execution_metadata: Mapping[str, JSONValue],
) -> SidecarJobSnapshot:
    raw_attempts = snapshot.metadata.get(CONTROL_ATTEMPTS_METADATA_KEY)
    attempts: dict[str, JSONValue] = (
        dict(raw_attempts) if isinstance(raw_attempts, Mapping) else {}
    )
    attempts[action] = attempt
    return _snapshot_with_metadata(
        snapshot,
        {
            CONTROL_ATTEMPTS_METADATA_KEY: attempts,
            CONTROL_EXECUTION_METADATA_KEY: dict(execution_metadata),
        },
    )


def _control_replacement_snapshot(
    handle: SidecarJobHandle,
    *,
    action: str,
    sidecar_status: SidecarStatus | None,
    metadata: Mapping[str, JSONValue],
) -> SidecarJobSnapshot:
    snapshot = SidecarJobSnapshot.from_handle(
        handle,
        lifecycle=(
            JobLifecycle.RETRYING
            if action == "retry"
            else JobLifecycle.RESUMING
        ),
        status=WorkStatus.UNKNOWN,
        sidecar_status=sidecar_status,
    )
    if action == "retry":
        return snapshot.mark_retrying(metadata=metadata)
    return snapshot.mark_resuming(metadata=metadata)


def _stored_job_snapshot(
    workflow_id: str,
    *,
    config: SidecarWorkerConfig,
    status_store: ManifestStatusStore | None = None,
) -> SidecarJobSnapshot | None:
    store = status_store or sidecar_job_store(config)
    payload = store.get_job_snapshot(workflow_id)
    if payload is None and not workflow_id.startswith("histdatacom-"):
        payload = store.get_job_snapshot(f"histdatacom-{workflow_id}")
    if payload is None:
        return None
    return SidecarJobSnapshot.from_dict(payload)


def _stored_job_snapshot_or_raise(
    workflow_id: str,
    *,
    config: SidecarWorkerConfig,
    status_store: ManifestStatusStore | None = None,
    cause: Exception | None = None,
) -> SidecarJobSnapshot:
    snapshot = _stored_job_snapshot(
        workflow_id,
        config=config,
        status_store=status_store,
    )
    if snapshot is not None:
        return snapshot
    error = SidecarUnavailableError(
        "No durable sidecar job snapshot found for "
        f"{workflow_id!r} in {sidecar_job_store_path(config)}"
    )
    if cause is not None:
        raise error from cause
    raise error


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
    try:
        started = supervisor.start()
    except SidecarResourceError as err:
        raise SidecarUnavailableError(str(err)) from err
    except RuntimeError as err:
        if _is_missing_temporal_dependency_error(err):
            raise SidecarUnavailableError(str(err)) from err
        raise
    if started.running:
        return started
    raise SidecarUnavailableError(
        "Temporal sidecar could not be started: " f"{started.message}"
    )


def _is_missing_temporal_dependency_error(err: RuntimeError) -> bool:
    message = str(err)
    return (
        "temporalio" in message
        or "histdatacom[temporal]" in message
        or "Temporal worker support requires" in message
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


def _snapshot_stage(snapshot: SidecarJobSnapshot) -> str:
    if snapshot.progress is None:
        return ""
    return str(snapshot.progress.current_stage)


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
    return _snapshot_with_metadata(
        SidecarJobSnapshot.from_handle(
            handle,
            lifecycle=lifecycle_from_work_status(status),
            status=status,
        ),
        {
            TEMPORAL_EXECUTION_STATUS_METADATA_KEY: {
                "raw": raw_status,
                "normalized": _normalized_temporal_description(raw_status),
            }
        },
    )


def _status_from_temporal_description(raw_status: str) -> WorkStatus:
    normalized = _normalized_temporal_description(raw_status)
    if normalized in {"RUNNING", "CONTINUED_AS_NEW"}:
        return WorkStatus.UNKNOWN
    return WorkStatus.from_value(normalized)


def _normalized_temporal_description(raw_status: str) -> str:
    return (
        raw_status.strip()
        .upper()
        .removeprefix(TEMPORAL_EXECUTION_STATUS_PREFIX)
    )


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
