"""Public orchestration facade for HistData runtime integrations."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS: dict[str, tuple[str, str]] = {
    "ArtifactRef": ("histdatacom.orchestration.contracts", "ArtifactRef"),
    "FailureInfo": ("histdatacom.orchestration.contracts", "FailureInfo"),
    "JSONScalar": ("histdatacom.orchestration.contracts", "JSONScalar"),
    "JSONValue": ("histdatacom.orchestration.contracts", "JSONValue"),
    "JobControlAction": (
        "histdatacom.orchestration.control",
        "JobControlAction",
    ),
    "JobControlStates": (
        "histdatacom.orchestration.control",
        "JobControlStates",
    ),
    "JobHandle": ("histdatacom.orchestration.client", "JobHandle"),
    "JobLifecycle": ("histdatacom.orchestration.control", "JobLifecycle"),
    "JobList": ("histdatacom.orchestration.control", "JobList"),
    "JobLogEntry": ("histdatacom.orchestration.control", "JobLogEntry"),
    "JobProgressSnapshot": (
        "histdatacom.orchestration.control",
        "JobProgressSnapshot",
    ),
    "JobResult": ("histdatacom.orchestration.client", "JobResult"),
    "JobSnapshot": ("histdatacom.orchestration.control", "JobSnapshot"),
    "OrchestrationUnavailableError": (
        "histdatacom.orchestration.client",
        "OrchestrationUnavailableError",
    ),
    "PortAllocationError": (
        "histdatacom.orchestration.runtime",
        "PortAllocationError",
    ),
    "RunRequest": ("histdatacom.orchestration.contracts", "RunRequest"),
    "RuntimeDependencyError": (
        "histdatacom.orchestration.client",
        "RuntimeDependencyError",
    ),
    "RuntimePaths": ("histdatacom.orchestration.runtime", "RuntimePaths"),
    "RuntimePolicy": ("histdatacom.orchestration.runtime", "RuntimePolicy"),
    "RuntimePorts": ("histdatacom.orchestration.runtime", "RuntimePorts"),
    "RuntimeStatus": (
        "histdatacom.orchestration.supervisor",
        "RuntimeStatus",
    ),
    "RuntimeSupervisor": (
        "histdatacom.orchestration.supervisor",
        "RuntimeSupervisor",
    ),
    "StageResult": ("histdatacom.orchestration.contracts", "StageResult"),
    "StatusEvent": ("histdatacom.orchestration.contracts", "StatusEvent"),
    "WorkItem": ("histdatacom.orchestration.contracts", "WorkItem"),
    "WorkStatus": ("histdatacom.orchestration.contracts", "WorkStatus"),
    "build_runtime_policy": (
        "histdatacom.orchestration.runtime",
        "build_runtime_policy",
    ),
    "cancel_job": ("histdatacom.orchestration.client", "cancel_job"),
    "cancel_job_sync": ("histdatacom.orchestration.client", "cancel_job_sync"),
    "get_job_artifacts": (
        "histdatacom.orchestration.client",
        "get_job_artifacts",
    ),
    "get_job_artifacts_sync": (
        "histdatacom.orchestration.client",
        "get_job_artifacts_sync",
    ),
    "get_job_logs": ("histdatacom.orchestration.client", "get_job_logs"),
    "get_job_logs_sync": (
        "histdatacom.orchestration.client",
        "get_job_logs_sync",
    ),
    "get_job_progress": (
        "histdatacom.orchestration.client",
        "get_job_progress",
    ),
    "get_job_progress_sync": (
        "histdatacom.orchestration.client",
        "get_job_progress_sync",
    ),
    "get_job_result": ("histdatacom.orchestration.client", "get_job_result"),
    "get_job_result_sync": (
        "histdatacom.orchestration.client",
        "get_job_result_sync",
    ),
    "inspect_job_status": (
        "histdatacom.orchestration.client",
        "inspect_job_status",
    ),
    "inspect_job_status_sync": (
        "histdatacom.orchestration.client",
        "inspect_job_status_sync",
    ),
    "list_job_statuses": (
        "histdatacom.orchestration.client",
        "list_job_statuses",
    ),
    "list_job_statuses_sync": (
        "histdatacom.orchestration.client",
        "list_job_statuses_sync",
    ),
    "submit_run_request": (
        "histdatacom.orchestration.client",
        "submit_run_request",
    ),
    "submit_run_request_and_observe": (
        "histdatacom.orchestration.client",
        "submit_run_request_and_observe",
    ),
    "submit_run_request_and_observe_sync": (
        "histdatacom.orchestration.client",
        "submit_run_request_and_observe_sync",
    ),
    "workflow_id_for_request": (
        "histdatacom.orchestration.client",
        "workflow_id_for_request",
    ),
}


def __getattr__(name: str) -> Any:
    """Resolve facade exports lazily."""
    if name not in _EXPORTS:
        raise AttributeError(name)
    module_name, attribute = _EXPORTS[name]
    return getattr(import_module(module_name), attribute)


def __dir__() -> list[str]:
    """Return package attributes for interactive help and completion."""
    return sorted((*globals(), *_EXPORTS))


__all__ = sorted(_EXPORTS)
