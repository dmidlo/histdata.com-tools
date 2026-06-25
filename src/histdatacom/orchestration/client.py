"""Temporal client connection and job submission helpers."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, replace
from importlib import import_module
from inspect import isawaitable
import logging
from pathlib import Path
from typing import Any, Mapping

from histdatacom.cancellation import (
    PartialArtifactDisposition,
    cleanup_partial_artifacts,
    operation_resume_policy,
)
from histdatacom.exceptions import DependencyOperationError, ErrorCategory
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
from histdatacom.verbosity import safe_log_extra
from histdatacom.orchestration.control import (
    JobLifecycle,
    JobLogEntry,
    JobProgressSnapshot,
    OrchestrationJobList,
    OrchestrationJobSnapshot,
    lifecycle_from_work_status,
)
from histdatacom.orchestration.queues import (
    OrchestrationWorkerConfig,
)
from histdatacom.orchestration.resources import OrchestrationResourceError
from histdatacom.orchestration.supervisor import (
    OrchestrationStatus,
    OrchestrationSupervisor,
)
from histdatacom.orchestration.workflow_metadata import (
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
LOGGER = logging.getLogger(__name__)
ProgressObserver = Callable[[OrchestrationJobSnapshot], None]


class OrchestrationUnavailableError(DependencyOperationError):
    """Raised when an orchestrated run is requested but unavailable."""

    category = ErrorCategory.DEPENDENCY
    code = "ORCHESTRATION_UNAVAILABLE"
    retryable = False
    exit_code = 1


class TemporalDependencyError(OrchestrationUnavailableError):
    """Raised when Temporal SDK functionality is used without temporalio."""

    code = "TEMPORAL_DEPENDENCY_UNAVAILABLE"


@dataclass(frozen=True, slots=True)
class OrchestrationJobHandle:
    """Serializable metadata returned after submitting an orchestration job."""

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
class OrchestrationJobResult:
    """Serializable result for a submitted or observed orchestration job."""

    handle: OrchestrationJobHandle
    status: str
    result: Any = None
    orchestration_status: OrchestrationStatus | None = None
    snapshot: OrchestrationJobSnapshot | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return JSON-compatible orchestration job metadata and result payload."""
        payload = {
            "status": self.status,
            "handle": self.handle.to_dict(),
            "result": self.result,
        }
        if self.orchestration_status is not None:
            payload["orchestration_status"] = (
                self.orchestration_status.to_dict()
            )
        if self.snapshot is not None:
            payload["snapshot"] = self.snapshot.to_dict()
        return payload


JobHandle = OrchestrationJobHandle
JobResult = OrchestrationJobResult
RuntimeDependencyError = TemporalDependencyError


async def connect_temporal_client(
    *,
    config: OrchestrationWorkerConfig | None = None,
    supervisor: OrchestrationSupervisor | None = None,
    client_class: Any | None = None,
) -> Any:
    """Connect to the workspace Temporal orchestration frontend."""
    resolved_config = resolve_orchestration_worker_config(
        config=config,
        supervisor=supervisor,
    )
    temporal_client_class = client_class or _load_temporal_client_class()
    connect = getattr(temporal_client_class, "connect", None)
    if connect is None:
        raise TypeError("Temporal client class must define connect()")
    LOGGER.debug(
        "Connecting Temporal client target_host=%s namespace=%s",
        resolved_config.target_host,
        resolved_config.namespace,
        extra=_config_log_context(resolved_config),
    )
    try:
        connected = await _maybe_await(
            connect(
                resolved_config.target_host,
                namespace=resolved_config.namespace,
            )
        )
    except Exception:
        LOGGER.exception(
            "Temporal client connection failed target_host=%s namespace=%s",
            resolved_config.target_host,
            resolved_config.namespace,
            extra=_config_log_context(resolved_config),
        )
        raise
    LOGGER.debug(
        "Connected Temporal client target_host=%s namespace=%s",
        resolved_config.target_host,
        resolved_config.namespace,
        extra=_config_log_context(resolved_config),
    )
    return connected


async def submit_run_request(
    request: RunRequest,
    *,
    config: OrchestrationWorkerConfig | None = None,
    supervisor: OrchestrationSupervisor | None = None,
    client: Any | None = None,
    client_class: Any | None = None,
    status_store: ManifestStatusStore | None = None,
    workflow: Any = DEFAULT_RUN_WORKFLOW_NAME,
) -> OrchestrationJobHandle:
    """Submit a serialized HistData run request without activity imports."""
    resolved_config = resolve_orchestration_worker_config(
        config=config,
        supervisor=supervisor,
        require_running=config is None,
    )
    temporal_client = client or await connect_temporal_client(
        config=resolved_config,
        supervisor=supervisor,
        client_class=client_class,
    )
    workflow_id = workflow_id_for_request(request)
    LOGGER.info(
        "Submitting HistData orchestration job request_id=%s workflow_id=%s",
        request.request_id,
        workflow_id,
        extra=_request_log_context(
            request,
            workflow_id=workflow_id,
            task_queue=resolved_config.task_queues.orchestration,
            namespace=resolved_config.namespace,
            wait_for_result=False,
        ),
    )
    handle = await _maybe_await(
        temporal_client.start_workflow(
            workflow,
            run_request_payload(request, resolved_config),
            id=workflow_id,
            task_queue=resolved_config.task_queues.orchestration,
        )
    )
    job_handle = OrchestrationJobHandle(
        request_id=request.request_id,
        workflow_id=str(getattr(handle, "id", workflow_id)),
        run_id=str(getattr(handle, "run_id", "")),
        task_queue=resolved_config.task_queues.orchestration,
        namespace=resolved_config.namespace,
    )
    _persist_job_snapshot(
        _snapshot_with_run_request(
            OrchestrationJobSnapshot.from_handle(job_handle),
            request,
        ),
        config=resolved_config,
        status_store=status_store,
    )
    LOGGER.info(
        "Submitted HistData orchestration job request_id=%s workflow_id=%s "
        "run_id=%s",
        request.request_id,
        job_handle.workflow_id,
        job_handle.run_id,
        extra=_request_log_context(
            request,
            workflow_id=job_handle.workflow_id,
            run_id=job_handle.run_id,
            task_queue=job_handle.task_queue,
            namespace=job_handle.namespace,
            status="submitted",
        ),
    )
    return job_handle


async def submit_run_request_and_observe(
    request: RunRequest,
    *,
    config: OrchestrationWorkerConfig | None = None,
    client: Any | None = None,
    client_class: Any | None = None,
    supervisor: OrchestrationSupervisor | None = None,
    start_if_needed: bool = False,
    wait_for_result: bool = True,
    status_store: ManifestStatusStore | None = None,
    workflow: Any = DEFAULT_RUN_WORKFLOW_NAME,
    progress_observer: ProgressObserver | None = None,
    progress_interval_seconds: float = 1.0,
) -> OrchestrationJobResult:
    """Submit a run through a healthy orchestration and optionally await its result."""
    resolved_supervisor = supervisor or OrchestrationSupervisor(
        runtime_policy=config.runtime_policy if config is not None else None
    )
    LOGGER.info(
        "Preparing HistData orchestration job request_id=%s "
        "start_if_needed=%s wait_for_result=%s",
        request.request_id,
        start_if_needed,
        wait_for_result,
        extra=_request_log_context(
            request,
            start_if_needed=start_if_needed,
            wait_for_result=wait_for_result,
        ),
    )
    orchestration_status = _ensure_orchestration_available(
        resolved_supervisor,
        start_if_needed=start_if_needed,
    )
    resolved_config = resolve_orchestration_worker_config(
        config=config,
        supervisor=resolved_supervisor,
        status=orchestration_status,
        require_running=True,
    )
    temporal_client = client or await connect_temporal_client(
        config=resolved_config,
        supervisor=resolved_supervisor,
        client_class=client_class,
    )
    workflow_id = workflow_id_for_request(request)
    LOGGER.info(
        "Submitting HistData orchestration job request_id=%s workflow_id=%s",
        request.request_id,
        workflow_id,
        extra=_request_log_context(
            request,
            workflow_id=workflow_id,
            task_queue=resolved_config.task_queues.orchestration,
            namespace=resolved_config.namespace,
            wait_for_result=wait_for_result,
        ),
    )
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
            OrchestrationJobSnapshot.from_handle(
                handle,
                orchestration_status=orchestration_status,
            ),
            request,
        ),
        config=resolved_config,
        status_store=status_store,
    )
    LOGGER.info(
        "Submitted HistData orchestration job request_id=%s workflow_id=%s "
        "run_id=%s",
        request.request_id,
        handle.workflow_id,
        handle.run_id,
        extra=_request_log_context(
            request,
            workflow_id=handle.workflow_id,
            run_id=handle.run_id,
            task_queue=handle.task_queue,
            namespace=handle.namespace,
            status="submitted",
        ),
    )
    if not wait_for_result:
        return OrchestrationJobResult(
            handle=handle,
            status="submitted",
            orchestration_status=orchestration_status,
            snapshot=submitted_snapshot,
        )

    _notify_progress_observer(progress_observer, submitted_snapshot)
    LOGGER.info(
        "Waiting for HistData orchestration job request_id=%s workflow_id=%s",
        request.request_id,
        handle.workflow_id,
        extra=_request_log_context(
            request,
            workflow_id=handle.workflow_id,
            run_id=handle.run_id,
            task_queue=handle.task_queue,
            namespace=handle.namespace,
            wait_for_result=True,
        ),
    )
    if progress_observer is not None:
        result = await _observe_workflow_result_with_progress(
            raw_handle,
            handle=handle,
            orchestration_status=orchestration_status,
            config=resolved_config,
            status_store=status_store,
            progress_observer=progress_observer,
            progress_interval_seconds=progress_interval_seconds,
        )
    else:
        result = await observe_workflow_result(raw_handle)
    snapshot = _persist_job_snapshot(
        submitted_snapshot.with_result(result),
        config=resolved_config,
        status_store=status_store,
    )
    _notify_progress_observer(progress_observer, snapshot)
    observed = OrchestrationJobResult(
        handle=handle,
        status=_observed_job_result_status(result, snapshot),
        result=result,
        orchestration_status=orchestration_status,
        snapshot=snapshot,
    )
    LOGGER.log(
        _job_result_log_level(observed.status),
        "Observed HistData orchestration job request_id=%s workflow_id=%s "
        "status=%s",
        request.request_id,
        handle.workflow_id,
        observed.status,
        extra=_request_log_context(
            request,
            workflow_id=handle.workflow_id,
            run_id=handle.run_id,
            task_queue=handle.task_queue,
            namespace=handle.namespace,
            status=observed.status,
        ),
    )
    return observed


def submit_run_request_and_observe_sync(
    request: RunRequest,
    **kwargs: Any,
) -> OrchestrationJobResult:
    """Synchronously submit and observe an orchestration run for CLI/API callers."""
    return asyncio.run(submit_run_request_and_observe(request, **kwargs))


def _observed_job_result_status(
    result: Any,
    snapshot: OrchestrationJobSnapshot,
) -> str:
    """Return the public status for an awaited orchestration workflow result."""
    result_status = (
        WorkStatus.from_value(result.get("status"))
        if isinstance(result, Mapping)
        else None
    )
    for status in (result_status, snapshot.status):
        if status == WorkStatus.FAILED:
            return "failed"
        if status == WorkStatus.CANCELLED:
            return "cancelled"
    if snapshot.lifecycle == JobLifecycle.FAILED:
        return "failed"
    if snapshot.lifecycle == JobLifecycle.CANCELLED:
        return "cancelled"
    return "completed"


def _job_result_log_level(status: str) -> int:
    if status == "failed":
        return logging.ERROR
    if status == "cancelled":
        return logging.WARNING
    return logging.INFO


async def submit_control_job(
    request: RunRequest,
    **kwargs: Any,
) -> OrchestrationJobSnapshot:
    """Submit a job and return the GUI-ready control snapshot."""
    result = await submit_run_request_and_observe(request, **kwargs)
    if result.snapshot is not None:
        return result.snapshot
    return OrchestrationJobSnapshot.from_handle(
        result.handle,
        orchestration_status=result.orchestration_status,
    ).with_result(result.result)


def submit_control_job_sync(
    request: RunRequest,
    **kwargs: Any,
) -> OrchestrationJobSnapshot:
    """Synchronously submit a job and return a control snapshot."""
    return asyncio.run(submit_control_job(request, **kwargs))


async def observe_workflow_result(workflow_handle: Any) -> Any:
    """Return the completed workflow result from a Temporal handle."""
    result = getattr(workflow_handle, "result", None)
    if result is None:
        raise TypeError("Temporal workflow handle must define result()")
    return await _maybe_await(result())


async def _observe_workflow_result_with_progress(
    workflow_handle: Any,
    *,
    handle: OrchestrationJobHandle,
    orchestration_status: OrchestrationStatus | None,
    config: OrchestrationWorkerConfig,
    status_store: ManifestStatusStore | None,
    progress_observer: ProgressObserver,
    progress_interval_seconds: float,
) -> Any:
    """Await a workflow result while periodically notifying progress UI."""
    result_task = asyncio.create_task(observe_workflow_result(workflow_handle))
    interval_seconds = max(0.1, progress_interval_seconds)
    while not result_task.done():
        done, _pending = await asyncio.wait(
            {result_task},
            timeout=interval_seconds,
        )
        if done:
            break
        try:
            snapshot = await _inspect_workflow_handle(
                workflow_handle,
                handle=handle,
                orchestration_status=orchestration_status,
            )
        except Exception as err:
            LOGGER.debug(
                "Progress query skipped for workflow_id=%s: %s",
                handle.workflow_id,
                err,
                extra=safe_log_extra(
                    workflow_id=handle.workflow_id,
                    run_id=handle.run_id,
                ),
            )
            continue
        _notify_progress_observer(
            progress_observer,
            _persist_job_snapshot(
                snapshot,
                config=config,
                status_store=status_store,
            ),
        )
    return await result_task


def _notify_progress_observer(
    progress_observer: ProgressObserver | None,
    snapshot: OrchestrationJobSnapshot,
) -> None:
    if progress_observer is None:
        return
    try:
        progress_observer(snapshot)
    except Exception as err:  # pragma: no cover - defensive terminal UI guard
        LOGGER.debug(
            "Progress observer failed for workflow_id=%s: %s",
            snapshot.workflow_id,
            err,
            extra=safe_log_extra(workflow_id=snapshot.workflow_id),
        )


async def inspect_job_status(
    workflow_id: str,
    *,
    run_id: str = "",
    config: OrchestrationWorkerConfig | None = None,
    client: Any | None = None,
    client_class: Any | None = None,
    supervisor: OrchestrationSupervisor | None = None,
    status_store: ManifestStatusStore | None = None,
    offline: bool = False,
    store_fallback: bool = True,
) -> OrchestrationJobSnapshot:
    """Return a GUI-ready status snapshot for one orchestration job."""
    resolved_config = resolve_orchestration_worker_config(
        config=config,
        supervisor=supervisor,
    )
    if offline:
        return _stored_job_snapshot_or_raise(
            workflow_id,
            config=resolved_config,
            status_store=status_store,
        )
    try:
        temporal_client = client or await connect_temporal_client(
            config=resolved_config,
            supervisor=supervisor,
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
        orchestration_status = _orchestration_status(supervisor)
        snapshot = await _inspect_workflow_handle(
            workflow_handle,
            handle=handle,
            orchestration_status=orchestration_status,
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
) -> OrchestrationJobSnapshot:
    """Synchronously return a job status snapshot."""
    return asyncio.run(inspect_job_status(workflow_id, **kwargs))


async def list_job_statuses(
    *,
    query: str = "",
    config: OrchestrationWorkerConfig | None = None,
    client: Any | None = None,
    client_class: Any | None = None,
    supervisor: OrchestrationSupervisor | None = None,
    status_store: ManifestStatusStore | None = None,
    offline: bool = False,
    store_fallback: bool = True,
    status: WorkStatus | str | None = None,
    limit: int | None = None,
) -> OrchestrationJobList:
    """List known HistData job handles without reading workflow histories."""
    resolved_config = resolve_orchestration_worker_config(
        config=config,
        supervisor=supervisor,
    )
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
            supervisor=supervisor,
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
        return OrchestrationJobList(jobs=snapshots)
    except Exception:
        if store_fallback:
            return list_stored_job_statuses(
                config=resolved_config,
                status_store=status_store,
                status=status,
                limit=limit,
            )
        raise


def list_job_statuses_sync(**kwargs: Any) -> OrchestrationJobList:
    """Synchronously list known orchestration jobs."""
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
    snapshot: OrchestrationJobSnapshot = await inspect_job_status(
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
    snapshot: OrchestrationJobSnapshot = await inspect_job_status(
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
    config: OrchestrationWorkerConfig | None = None,
    client: Any | None = None,
    client_class: Any | None = None,
    supervisor: OrchestrationSupervisor | None = None,
    status_store: ManifestStatusStore | None = None,
    offline: bool = False,
    store_fallback: bool = True,
) -> OrchestrationJobSnapshot:
    """Return a snapshot with the workflow result payload attached."""
    resolved_config = resolve_orchestration_worker_config(
        config=config,
        supervisor=supervisor,
    )
    if offline:
        return _stored_job_snapshot_or_raise(
            workflow_id,
            config=resolved_config,
            status_store=status_store,
        )
    try:
        temporal_client = client or await connect_temporal_client(
            config=resolved_config,
            supervisor=supervisor,
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
        orchestration_status = _orchestration_status(supervisor)
        snapshot = await _inspect_workflow_handle(
            workflow_handle,
            handle=handle,
            orchestration_status=orchestration_status,
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
) -> OrchestrationJobSnapshot:
    """Synchronously return a job snapshot with result payload."""
    return asyncio.run(get_job_result(workflow_id, **kwargs))


async def cancel_job(
    workflow_id: str,
    *,
    run_id: str = "",
    reason: str = "",
    config: OrchestrationWorkerConfig | None = None,
    client: Any | None = None,
    client_class: Any | None = None,
    supervisor: OrchestrationSupervisor | None = None,
    status_store: ManifestStatusStore | None = None,
    offline: bool = False,
) -> OrchestrationJobSnapshot:
    """Request cancellation and return explicit cancellation state."""
    resolved_config = resolve_orchestration_worker_config(
        config=config,
        supervisor=supervisor,
    )
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
        supervisor=supervisor,
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
        orchestration_status=_orchestration_status(supervisor),
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
) -> OrchestrationJobSnapshot:
    """Synchronously request job cancellation."""
    return asyncio.run(cancel_job(workflow_id, **kwargs))


async def retry_job(
    workflow_id: str,
    *,
    run_id: str = "",
    reason: str = "",
    config: OrchestrationWorkerConfig | None = None,
    client: Any | None = None,
    client_class: Any | None = None,
    supervisor: OrchestrationSupervisor | None = None,
    status_store: ManifestStatusStore | None = None,
    offline: bool = False,
    reuse_completed_artifacts: bool = True,
    workflow: Any = DEFAULT_RUN_WORKFLOW_NAME,
) -> OrchestrationJobSnapshot:
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
) -> OrchestrationJobSnapshot:
    """Synchronously start a deterministic retry replacement workflow."""
    return asyncio.run(retry_job(workflow_id, **kwargs))


async def resume_job(
    workflow_id: str,
    *,
    run_id: str = "",
    reason: str = "",
    config: OrchestrationWorkerConfig | None = None,
    client: Any | None = None,
    client_class: Any | None = None,
    supervisor: OrchestrationSupervisor | None = None,
    status_store: ManifestStatusStore | None = None,
    offline: bool = False,
    reuse_completed_artifacts: bool = True,
    workflow: Any = DEFAULT_RUN_WORKFLOW_NAME,
) -> OrchestrationJobSnapshot:
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
) -> OrchestrationJobSnapshot:
    """Synchronously start a deterministic resume replacement workflow."""
    return asyncio.run(resume_job(workflow_id, **kwargs))


def workflow_id_for_request(request: RunRequest) -> str:
    """Return the stable Temporal workflow ID for a run request."""
    request_id = request.request_id.strip() or "request"
    return f"histdatacom-{request_id}"


def resolve_orchestration_worker_config(
    *,
    config: OrchestrationWorkerConfig | None = None,
    supervisor: OrchestrationSupervisor | None = None,
    status: OrchestrationStatus | None = None,
    require_running: bool = False,
) -> OrchestrationWorkerConfig:
    """Resolve client config from explicit or running orchestration state."""
    if config is not None:
        return config
    resolved_supervisor = supervisor or OrchestrationSupervisor()
    try:
        return resolved_supervisor.client_worker_config(
            status=status,
            require_running=require_running,
        )
    except RuntimeError as err:
        raise OrchestrationUnavailableError(str(err)) from err


def run_request_payload(
    request: RunRequest,
    config: OrchestrationWorkerConfig,
) -> dict[str, Any]:
    """Return a request payload enriched with orchestration workflow metadata."""
    payload: dict[str, Any] = request.to_dict()
    metadata = dict(payload.get("metadata") or {})
    metadata[TASK_QUEUE_METADATA_KEY] = config.task_queues.to_dict()
    metadata[TOPOLOGY_METADATA_KEY] = TOPOLOGY_SCHEMA_VERSION
    metadata[STATUS_STORE_REF_KEY] = orchestration_job_store(
        config
    ).status_store_ref()
    payload["metadata"] = metadata
    return payload


def orchestration_job_store_root(
    config: OrchestrationWorkerConfig | None = None,
) -> Path:
    """Return the workspace-scoped root used for durable job snapshots."""
    resolved_config = resolve_orchestration_worker_config(config=config)
    return Path(resolved_config.runtime_policy.paths.manifests_dir)


def orchestration_job_store_path(
    config: OrchestrationWorkerConfig | None = None,
) -> Path:
    """Return the SQLite path for durable orchestration job snapshots."""
    return Path(
        ManifestStatusStore.path_for_root(orchestration_job_store_root(config))
    )


def orchestration_job_store(
    config: OrchestrationWorkerConfig | None = None,
) -> ManifestStatusStore:
    """Return the workspace-scoped durable orchestration job status store."""
    return ManifestStatusStore(orchestration_job_store_root(config))


resolve_worker_config = resolve_orchestration_worker_config
job_store_root = orchestration_job_store_root
job_store_path = orchestration_job_store_path
job_store = orchestration_job_store


def list_stored_job_statuses(
    *,
    config: OrchestrationWorkerConfig | None = None,
    status_store: ManifestStatusStore | None = None,
    status: WorkStatus | str | None = None,
    limit: int | None = None,
) -> OrchestrationJobList:
    """List durable orchestration jobs without querying Temporal."""
    store = status_store or orchestration_job_store(config)
    return OrchestrationJobList(
        jobs=tuple(
            OrchestrationJobSnapshot.from_dict(payload)
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
    config: OrchestrationWorkerConfig | None = None,
    client: Any | None = None,
    client_class: Any | None = None,
    supervisor: OrchestrationSupervisor | None = None,
    status_store: ManifestStatusStore | None = None,
    offline: bool = False,
    reuse_completed_artifacts: bool = True,
    workflow: Any = DEFAULT_RUN_WORKFLOW_NAME,
) -> OrchestrationJobSnapshot:
    if offline:
        raise OrchestrationUnavailableError(
            "Retry and resume require a live Temporal orchestration; "
            "--offline is only supported for read-only job commands."
        )
    if action not in {"retry", "resume"}:
        raise ValueError(f"unsupported control action: {action}")

    resolved_config = resolve_orchestration_worker_config(
        config=config,
        supervisor=supervisor,
        require_running=True,
    )
    temporal_client = client or await connect_temporal_client(
        config=resolved_config,
        supervisor=supervisor,
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
        orchestration_status=_orchestration_status(supervisor),
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
    snapshot: OrchestrationJobSnapshot,
    *,
    config: OrchestrationWorkerConfig,
    status_store: ManifestStatusStore | None = None,
) -> OrchestrationJobSnapshot:
    store = status_store or orchestration_job_store(config)
    snapshot = _merge_stored_snapshot_metadata(snapshot, store)
    store.write_job_snapshot(snapshot)
    return snapshot


def _snapshot_with_run_request(
    snapshot: OrchestrationJobSnapshot,
    request: RunRequest,
) -> OrchestrationJobSnapshot:
    return _snapshot_with_metadata(
        snapshot,
        {RUN_REQUEST_METADATA_KEY: request.to_dict()},
    )


def _snapshot_with_metadata(
    snapshot: OrchestrationJobSnapshot,
    metadata: Mapping[str, JSONValue],
) -> OrchestrationJobSnapshot:
    return replace(
        snapshot,
        metadata={
            **snapshot.metadata,
            **dict(metadata),
        },
    )


def _merge_stored_snapshot_metadata(
    snapshot: OrchestrationJobSnapshot,
    store: ManifestStatusStore,
) -> OrchestrationJobSnapshot:
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
    snapshot: OrchestrationJobSnapshot,
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


def _run_request_from_snapshot(
    snapshot: OrchestrationJobSnapshot,
) -> RunRequest:
    payload = snapshot.metadata.get(RUN_REQUEST_METADATA_KEY)
    if not isinstance(payload, Mapping):
        raise OrchestrationUnavailableError(
            "Cannot retry/resume job without a persisted RunRequest snapshot. "
            "Submit the job again with the current orchestration client first."
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


def _next_control_attempt(
    snapshot: OrchestrationJobSnapshot, action: str
) -> int:
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
    snapshot: OrchestrationJobSnapshot,
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
    snapshot: OrchestrationJobSnapshot,
    replacement_handle: OrchestrationJobHandle,
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
    snapshot: OrchestrationJobSnapshot,
    *,
    action: str,
    reason: str,
    stage: str,
    metadata: Mapping[str, JSONValue],
) -> OrchestrationJobSnapshot:
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
    snapshot: OrchestrationJobSnapshot,
    *,
    action: str,
    attempt: int,
    execution_metadata: Mapping[str, JSONValue],
) -> OrchestrationJobSnapshot:
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
    handle: OrchestrationJobHandle,
    *,
    action: str,
    orchestration_status: OrchestrationStatus | None,
    metadata: Mapping[str, JSONValue],
) -> OrchestrationJobSnapshot:
    snapshot = OrchestrationJobSnapshot.from_handle(
        handle,
        lifecycle=(
            JobLifecycle.RETRYING
            if action == "retry"
            else JobLifecycle.RESUMING
        ),
        status=WorkStatus.UNKNOWN,
        orchestration_status=orchestration_status,
    )
    if action == "retry":
        return snapshot.mark_retrying(metadata=metadata)
    return snapshot.mark_resuming(metadata=metadata)


def _stored_job_snapshot(
    workflow_id: str,
    *,
    config: OrchestrationWorkerConfig,
    status_store: ManifestStatusStore | None = None,
) -> OrchestrationJobSnapshot | None:
    store = status_store or orchestration_job_store(config)
    payload = store.get_job_snapshot(workflow_id)
    if payload is None and not workflow_id.startswith("histdatacom-"):
        payload = store.get_job_snapshot(f"histdatacom-{workflow_id}")
    if payload is None:
        return None
    return OrchestrationJobSnapshot.from_dict(payload)


def _stored_job_snapshot_or_raise(
    workflow_id: str,
    *,
    config: OrchestrationWorkerConfig,
    status_store: ManifestStatusStore | None = None,
    cause: Exception | None = None,
) -> OrchestrationJobSnapshot:
    snapshot = _stored_job_snapshot(
        workflow_id,
        config=config,
        status_store=status_store,
    )
    if snapshot is not None:
        return snapshot
    error = OrchestrationUnavailableError(
        "No durable orchestration job snapshot found for "
        f"{workflow_id!r} in {orchestration_job_store_path(config)}"
    )
    if cause is not None:
        raise error from cause
    raise error


def _ensure_orchestration_available(
    supervisor: OrchestrationSupervisor,
    *,
    start_if_needed: bool,
) -> OrchestrationStatus:
    status = supervisor.status(repair=True)
    LOGGER.debug(
        "Checked Temporal orchestration availability state=%s running=%s",
        status.state,
        status.running,
        extra=_status_log_context(status, start_if_needed=start_if_needed),
    )
    if status.running:
        return status
    if not start_if_needed:
        LOGGER.warning(
            "Temporal orchestration unavailable state=%s start_if_needed=%s",
            status.state,
            start_if_needed,
            extra=_status_log_context(
                status,
                start_if_needed=start_if_needed,
            ),
        )
        raise OrchestrationUnavailableError(
            "Temporal orchestration is not running. Rerun without "
            "--no-orchestration-start or start the runtime before submitting "
            "work."
        )
    try:
        LOGGER.info(
            "Starting Temporal orchestration runtime state=%s",
            status.state,
            extra=_status_log_context(
                status,
                start_if_needed=start_if_needed,
            ),
        )
        started = supervisor.start()
    except OrchestrationResourceError as err:
        LOGGER.warning(
            "Temporal orchestration startup unavailable error=%s",
            str(err),
            extra=_status_log_context(
                status,
                start_if_needed=start_if_needed,
                error_type=type(err).__name__,
            ),
        )
        raise OrchestrationUnavailableError(str(err)) from err
    except RuntimeError as err:
        if _is_missing_temporal_dependency_error(err):
            LOGGER.warning(
                "Temporal orchestration startup dependency unavailable "
                "error=%s",
                str(err),
                extra=_status_log_context(
                    status,
                    start_if_needed=start_if_needed,
                    error_type=type(err).__name__,
                ),
            )
            raise OrchestrationUnavailableError(str(err)) from err
        LOGGER.exception(
            "Temporal orchestration startup failed error=%s",
            str(err),
            extra=_status_log_context(
                status,
                start_if_needed=start_if_needed,
                error_type=type(err).__name__,
            ),
        )
        raise
    if started.running:
        LOGGER.info(
            "Temporal orchestration runtime started state=%s",
            started.state,
            extra=_status_log_context(
                started,
                start_if_needed=start_if_needed,
            ),
        )
        return started
    LOGGER.warning(
        "Temporal orchestration startup did not reach running state=%s",
        started.state,
        extra=_status_log_context(
            started,
            start_if_needed=start_if_needed,
        ),
    )
    raise OrchestrationUnavailableError(
        "Temporal orchestration could not be started: " f"{started.message}"
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
    LOGGER.debug(
        "Starting Temporal workflow workflow_id=%s task_queue=%s",
        workflow_id,
        task_queue,
        extra=safe_log_extra(
            workflow=str(workflow),
            workflow_id=workflow_id,
            task_queue=task_queue,
        ),
    )
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
    config: OrchestrationWorkerConfig,
) -> OrchestrationJobHandle:
    return OrchestrationJobHandle(
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
    config: OrchestrationWorkerConfig,
) -> OrchestrationJobHandle:
    request_id = workflow_id
    if workflow_id.startswith("histdatacom-"):
        request_id = workflow_id.removeprefix("histdatacom-")
    return OrchestrationJobHandle(
        request_id=request_id,
        workflow_id=workflow_id,
        run_id=run_id,
        task_queue=config.task_queues.orchestration,
        namespace=config.namespace,
    )


async def _inspect_workflow_handle(
    workflow_handle: Any,
    *,
    handle: OrchestrationJobHandle,
    orchestration_status: OrchestrationStatus | None,
) -> OrchestrationJobSnapshot:
    status_payload = await _query_workflow_status(workflow_handle)
    snapshot = OrchestrationJobSnapshot.from_workflow_status(
        handle,
        status_payload,
        orchestration_status=orchestration_status,
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
    LOGGER.debug(
        "Querying Temporal workflow status workflow_id=%s run_id=%s",
        str(getattr(workflow_handle, "id", "") or ""),
        str(getattr(workflow_handle, "run_id", "") or ""),
        extra=safe_log_extra(
            workflow_id=str(getattr(workflow_handle, "id", "") or ""),
            run_id=str(getattr(workflow_handle, "run_id", "") or ""),
        ),
    )
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


def _snapshot_stage(snapshot: OrchestrationJobSnapshot) -> str:
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
    config: OrchestrationWorkerConfig,
) -> OrchestrationJobSnapshot:
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
        OrchestrationJobSnapshot.from_handle(
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


def _orchestration_status(
    supervisor: OrchestrationSupervisor | None,
) -> OrchestrationStatus | None:
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


def _config_log_context(
    config: OrchestrationWorkerConfig,
    **values: Any,
) -> dict[str, object]:
    context: dict[str, object] = safe_log_extra(
        namespace=config.namespace,
        target_host=config.target_host,
        task_queue=config.task_queue,
        lane=config.lane.value,
        **values,
    )
    return context


def _request_log_context(
    request: RunRequest,
    **values: Any,
) -> dict[str, object]:
    context: dict[str, object] = safe_log_extra(
        request_id=request.request_id,
        operations=_request_operation_names(request),
        pairs_count=len(request.pairs),
        formats=list(request.formats),
        timeframes=list(request.timeframes),
        start_yearmonth=request.start_yearmonth,
        end_yearmonth=request.end_yearmonth,
        api_return_type=request.api_return_type,
        data_quality=request.data_quality,
        repo_quality_refresh=request.repo_quality_refresh,
        metadata_key_count=len(request.metadata),
        **values,
    )
    return context


def _request_operation_names(request: RunRequest) -> tuple[str, ...]:
    operations: list[str] = []
    if request.available_remote_data:
        operations.append("available_remote_data")
    if request.update_remote_data:
        operations.append("update_remote_data")
    if request.validate_urls:
        operations.append("validate_urls")
    if request.download_data_archives:
        operations.append("download_archives")
    if request.extract_csvs:
        operations.append("extract_csv")
    if request.api_return_type:
        operations.append("api_return")
    if request.import_to_influxdb:
        operations.append("import_to_influx")
    if request.data_quality:
        operations.append("data_quality")
    if request.repo_quality_refresh:
        operations.append("repo_quality_refresh")
    return tuple(operations)


def _status_log_context(
    status: OrchestrationStatus,
    **values: Any,
) -> dict[str, object]:
    context: dict[str, object] = safe_log_extra(
        orchestration_state=status.state,
        orchestration_running=status.running,
        state_dir=status.state_dir,
        pid_count=len(status.pids),
        log_components=sorted(status.logs),
        **values,
    )
    return context


async def _maybe_await(value: Any) -> Any:
    if isawaitable(value):
        return await value
    return value
