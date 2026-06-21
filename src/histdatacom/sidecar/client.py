"""Temporal client connection and job submission helpers."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from importlib import import_module
from inspect import isawaitable
from typing import Any, Mapping

from histdatacom.runtime_contracts import JSONValue, RunRequest
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

    def to_dict(self) -> dict[str, Any]:
        """Return JSON-compatible sidecar job metadata and result payload."""
        payload = {
            "status": self.status,
            "handle": self.handle.to_dict(),
            "result": self.result,
        }
        if self.sidecar_status is not None:
            payload["sidecar_status"] = self.sidecar_status.to_dict()
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
        return SidecarJobResult(
            handle=handle,
            status="submitted",
            sidecar_status=sidecar_status,
        )

    result = await observe_workflow_result(raw_handle)
    return SidecarJobResult(
        handle=handle,
        status="completed",
        result=result,
        sidecar_status=sidecar_status,
    )


def submit_run_request_and_observe_sync(
    request: RunRequest,
    **kwargs: Any,
) -> SidecarJobResult:
    """Synchronously submit and observe a sidecar run for CLI/API callers."""
    return asyncio.run(submit_run_request_and_observe(request, **kwargs))


async def observe_workflow_result(workflow_handle: Any) -> Any:
    """Return the completed workflow result from a Temporal handle."""
    result = getattr(workflow_handle, "result", None)
    if result is None:
        raise TypeError("Temporal workflow handle must define result()")
    return await _maybe_await(result())


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
