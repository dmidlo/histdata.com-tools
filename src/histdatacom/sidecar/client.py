"""Temporal client connection and job submission helpers."""

from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
from inspect import isawaitable
from typing import Any

from histdatacom.runtime_contracts import RunRequest
from histdatacom.sidecar.queues import (
    SidecarWorkerConfig,
    build_sidecar_worker_config,
)
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
