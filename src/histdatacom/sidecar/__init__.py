"""Temporal sidecar packaging and resource helpers."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS: dict[str, tuple[str, str]] = {
    "BATCHING_METADATA_KEY": (
        "histdatacom.sidecar.workflows",
        "BATCHING_METADATA_KEY",
    ),
    "CONTROL_SCHEMA_VERSION": (
        "histdatacom.sidecar.control",
        "CONTROL_SCHEMA_VERSION",
    ),
    "DATASET_PLAN_BATCHES_KEY": (
        "histdatacom.manifest_store",
        "DATASET_PLAN_BATCHES_KEY",
    ),
    "DATASET_PLAN_REF_KEY": (
        "histdatacom.manifest_store",
        "DATASET_PLAN_REF_KEY",
    ),
    "DEFAULT_DATASET_PLAN_INLINE_WORK_ITEM_LIMIT": (
        "histdatacom.manifest_store",
        "DEFAULT_DATASET_PLAN_INLINE_WORK_ITEM_LIMIT",
    ),
    "DEFAULT_MAX_WORK_ITEMS_PER_BATCH": (
        "histdatacom.sidecar.workflows",
        "DEFAULT_MAX_WORK_ITEMS_PER_BATCH",
    ),
    "DEFAULT_MAX_PARALLEL_CHILD_WORKFLOWS": (
        "histdatacom.sidecar.workflows",
        "DEFAULT_MAX_PARALLEL_CHILD_WORKFLOWS",
    ),
    "DEFAULT_MAX_ARTIFACTS_PER_OWNER": (
        "histdatacom.sidecar.maintenance",
        "DEFAULT_MAX_ARTIFACTS_PER_OWNER",
    ),
    "DEFAULT_MAX_DATASET_PLANS_PER_REQUEST": (
        "histdatacom.sidecar.maintenance",
        "DEFAULT_MAX_DATASET_PLANS_PER_REQUEST",
    ),
    "DEFAULT_MAX_JOB_SNAPSHOTS": (
        "histdatacom.sidecar.maintenance",
        "DEFAULT_MAX_JOB_SNAPSHOTS",
    ),
    "DEFAULT_MAX_LOG_BYTES": (
        "histdatacom.sidecar.maintenance",
        "DEFAULT_MAX_LOG_BYTES",
    ),
    "DEFAULT_MAX_ROTATED_LOGS": (
        "histdatacom.sidecar.maintenance",
        "DEFAULT_MAX_ROTATED_LOGS",
    ),
    "DEFAULT_MAX_STAGE_RESULTS_PER_WORK_ITEM": (
        "histdatacom.sidecar.maintenance",
        "DEFAULT_MAX_STAGE_RESULTS_PER_WORK_ITEM",
    ),
    "DEFAULT_MAX_STATUS_EVENTS_PER_OWNER": (
        "histdatacom.sidecar.maintenance",
        "DEFAULT_MAX_STATUS_EVENTS_PER_OWNER",
    ),
    "DEFAULT_MAX_TEMPORAL_SQLITE_BYTES": (
        "histdatacom.sidecar.maintenance",
        "DEFAULT_MAX_TEMPORAL_SQLITE_BYTES",
    ),
    "DEFAULT_RUN_WORKFLOW_NAME": (
        "histdatacom.sidecar.client",
        "DEFAULT_RUN_WORKFLOW_NAME",
    ),
    "DEFAULT_TASK_QUEUE_PREFIX": (
        "histdatacom.sidecar.queues",
        "DEFAULT_TASK_QUEUE_PREFIX",
    ),
    "DEFAULT_TEMPORAL_NAMESPACE": (
        "histdatacom.sidecar.queues",
        "DEFAULT_TEMPORAL_NAMESPACE",
    ),
    "DEFAULT_WORKFLOWS": ("histdatacom.sidecar.workflows", "DEFAULT_WORKFLOWS"),
    "FANOUT_METADATA_KEY": (
        "histdatacom.sidecar.workflows",
        "FANOUT_METADATA_KEY",
    ),
    "MAX_PARALLEL_CHILD_WORKFLOWS_METADATA_KEY": (
        "histdatacom.sidecar.workflows",
        "MAX_PARALLEL_CHILD_WORKFLOWS_METADATA_KEY",
    ),
    "MAX_WORK_ITEMS_PER_BATCH_METADATA_KEY": (
        "histdatacom.sidecar.workflows",
        "MAX_WORK_ITEMS_PER_BATCH_METADATA_KEY",
    ),
    "TASK_QUEUE_METADATA_KEY": (
        "histdatacom.sidecar.workflow_metadata",
        "TASK_QUEUE_METADATA_KEY",
    ),
    "TOPOLOGY_METADATA_KEY": (
        "histdatacom.sidecar.workflow_metadata",
        "TOPOLOGY_METADATA_KEY",
    ),
    "TOPOLOGY_SCHEMA_VERSION": (
        "histdatacom.sidecar.workflow_metadata",
        "TOPOLOGY_SCHEMA_VERSION",
    ),
    "WORKFLOW_TOPOLOGY": ("histdatacom.sidecar.workflows", "WORKFLOW_TOPOLOGY"),
    "ArtifactRef": ("histdatacom.sidecar.contracts", "ArtifactRef"),
    "ActivityExecutor": ("histdatacom.sidecar.workflows", "ActivityExecutor"),
    "ChildWorkflowExecutor": (
        "histdatacom.sidecar.workflows",
        "ChildWorkflowExecutor",
    ),
    "ControlOperationName": (
        "histdatacom.sidecar.control",
        "ControlOperationName",
    ),
    "ControlOperationPhase": (
        "histdatacom.sidecar.control",
        "ControlOperationPhase",
    ),
    "ControlOperationState": (
        "histdatacom.sidecar.control",
        "ControlOperationState",
    ),
    "FailureInfo": ("histdatacom.sidecar.contracts", "FailureInfo"),
    "HistDataRunWorkflow": (
        "histdatacom.sidecar.workflows",
        "HistDataRunWorkflow",
    ),
    "INLINE_WORK_ITEM_LIMIT_METADATA_KEY": (
        "histdatacom.manifest_store",
        "INLINE_WORK_ITEM_LIMIT_METADATA_KEY",
    ),
    "JobControlAction": ("histdatacom.sidecar.control", "JobControlAction"),
    "JobControlStates": ("histdatacom.sidecar.control", "JobControlStates"),
    "JobLifecycle": ("histdatacom.sidecar.control", "JobLifecycle"),
    "JobLogEntry": ("histdatacom.sidecar.control", "JobLogEntry"),
    "JobProgressSnapshot": (
        "histdatacom.sidecar.control",
        "JobProgressSnapshot",
    ),
    "JSONScalar": ("histdatacom.sidecar.contracts", "JSONScalar"),
    "JSONValue": ("histdatacom.sidecar.contracts", "JSONValue"),
    "LogMaintenanceResult": (
        "histdatacom.sidecar.maintenance",
        "LogMaintenanceResult",
    ),
    "MAINTENANCE_SCHEMA_VERSION": (
        "histdatacom.sidecar.maintenance",
        "MAINTENANCE_SCHEMA_VERSION",
    ),
    "PortAllocationError": (
        "histdatacom.sidecar.runtime",
        "PortAllocationError",
    ),
    "PLAN_SPILL_METADATA_KEY": (
        "histdatacom.manifest_store",
        "PLAN_SPILL_METADATA_KEY",
    ),
    "SidecarExecutableUnavailable": (
        "histdatacom.sidecar.resources",
        "SidecarExecutableUnavailable",
    ),
    "SidecarJobHandle": ("histdatacom.sidecar.client", "SidecarJobHandle"),
    "SidecarJobList": ("histdatacom.sidecar.control", "SidecarJobList"),
    "SidecarJobResult": ("histdatacom.sidecar.client", "SidecarJobResult"),
    "SidecarJobSnapshot": (
        "histdatacom.sidecar.control",
        "SidecarJobSnapshot",
    ),
    "SidecarManifest": ("histdatacom.sidecar.resources", "SidecarManifest"),
    "SidecarMaintenanceResult": (
        "histdatacom.sidecar.maintenance",
        "SidecarMaintenanceResult",
    ),
    "SidecarPaths": ("histdatacom.sidecar.runtime", "SidecarPaths"),
    "SidecarPlatformResource": (
        "histdatacom.sidecar.resources",
        "SidecarPlatformResource",
    ),
    "SidecarPorts": ("histdatacom.sidecar.runtime", "SidecarPorts"),
    "SidecarResourceError": (
        "histdatacom.sidecar.resources",
        "SidecarResourceError",
    ),
    "SidecarRetentionPolicy": (
        "histdatacom.sidecar.maintenance",
        "SidecarRetentionPolicy",
    ),
    "SidecarRuntimePolicy": (
        "histdatacom.sidecar.runtime",
        "SidecarRuntimePolicy",
    ),
    "SidecarStatus": ("histdatacom.sidecar.supervisor", "SidecarStatus"),
    "SidecarSupervisor": (
        "histdatacom.sidecar.supervisor",
        "SidecarSupervisor",
    ),
    "SidecarTaskQueues": ("histdatacom.sidecar.queues", "SidecarTaskQueues"),
    "SidecarUnavailableError": (
        "histdatacom.sidecar.client",
        "SidecarUnavailableError",
    ),
    "SidecarWorkerConfig": (
        "histdatacom.sidecar.queues",
        "SidecarWorkerConfig",
    ),
    "StatusStoreMaintenanceResult": (
        "histdatacom.sidecar.maintenance",
        "StatusStoreMaintenanceResult",
    ),
    "RunRequest": ("histdatacom.sidecar.contracts", "RunRequest"),
    "StageResult": ("histdatacom.sidecar.contracts", "StageResult"),
    "StatusEvent": ("histdatacom.sidecar.contracts", "StatusEvent"),
    "TaskQueueLane": ("histdatacom.sidecar.queues", "TaskQueueLane"),
    "TemporalSqliteMaintenanceResult": (
        "histdatacom.sidecar.maintenance",
        "TemporalSqliteMaintenanceResult",
    ),
    "TemporalActivityExecutor": (
        "histdatacom.sidecar.workflows",
        "TemporalActivityExecutor",
    ),
    "TemporalDependencyError": (
        "histdatacom.sidecar.client",
        "TemporalDependencyError",
    ),
    "TemporalRuntimeArtifact": (
        "histdatacom.sidecar.resources",
        "TemporalRuntimeArtifact",
    ),
    "TemporalRuntimeCacheEntry": (
        "histdatacom.sidecar.resources",
        "TemporalRuntimeCacheEntry",
    ),
    "TemporalRuntimeChecksumError": (
        "histdatacom.sidecar.resources",
        "TemporalRuntimeChecksumError",
    ),
    "TemporalRuntimeIndex": (
        "histdatacom.sidecar.resources",
        "TemporalRuntimeIndex",
    ),
    "TemporalRuntimeOfflineError": (
        "histdatacom.sidecar.resources",
        "TemporalRuntimeOfflineError",
    ),
    "TemporalRuntimeProvisioningError": (
        "histdatacom.sidecar.resources",
        "TemporalRuntimeProvisioningError",
    ),
    "TemporalRuntimeResolution": (
        "histdatacom.sidecar.resources",
        "TemporalRuntimeResolution",
    ),
    "UnsupportedSidecarPlatform": (
        "histdatacom.sidecar.resources",
        "UnsupportedSidecarPlatform",
    ),
    "WorkItem": ("histdatacom.sidecar.contracts", "WorkItem"),
    "WorkStatus": ("histdatacom.sidecar.contracts", "WorkStatus"),
    "WorkflowInvocation": (
        "histdatacom.sidecar.workflows",
        "WorkflowInvocation",
    ),
    "WorkflowProgress": ("histdatacom.sidecar.workflows", "WorkflowProgress"),
    "WorkflowSpec": ("histdatacom.sidecar.workflows", "WorkflowSpec"),
    "build_cache_activity": (
        "histdatacom.sidecar.activities",
        "build_cache_activity",
    ),
    "build_run_child_invocations": (
        "histdatacom.sidecar.workflows",
        "build_run_child_invocations",
    ),
    "build_sidecar_runtime_policy": (
        "histdatacom.sidecar.runtime",
        "build_sidecar_runtime_policy",
    ),
    "build_sidecar_task_queues": (
        "histdatacom.sidecar.queues",
        "build_sidecar_task_queues",
    ),
    "build_sidecar_worker_config": (
        "histdatacom.sidecar.queues",
        "build_sidecar_worker_config",
    ),
    "build_symbol_batch_invocations": (
        "histdatacom.sidecar.workflows",
        "build_symbol_batch_invocations",
    ),
    "build_symbol_child_invocations": (
        "histdatacom.sidecar.workflows",
        "build_symbol_child_invocations",
    ),
    "build_temporal_start_command": (
        "histdatacom.sidecar.supervisor",
        "build_temporal_start_command",
    ),
    "build_temporal_worker": (
        "histdatacom.sidecar.worker",
        "build_temporal_worker",
    ),
    "cancel_job": ("histdatacom.sidecar.client", "cancel_job"),
    "cancel_job_sync": ("histdatacom.sidecar.client", "cancel_job_sync"),
    "connect_temporal_client": (
        "histdatacom.sidecar.client",
        "connect_temporal_client",
    ),
    "current_platform_key": (
        "histdatacom.sidecar.resources",
        "current_platform_key",
    ),
    "dataset_plan_activity": (
        "histdatacom.sidecar.activities",
        "dataset_plan_activity",
    ),
    "default_activities": (
        "histdatacom.sidecar.activities",
        "default_activities",
    ),
    "default_sidecar_runtime_home": (
        "histdatacom.sidecar.runtime",
        "default_sidecar_runtime_home",
    ),
    "default_sidecar_state_dir": (
        "histdatacom.sidecar.runtime",
        "default_sidecar_state_dir",
    ),
    "default_sidecar_workspace": (
        "histdatacom.sidecar.runtime",
        "default_sidecar_workspace",
    ),
    "default_temporal_runtime_cache_dir": (
        "histdatacom.sidecar.resources",
        "default_temporal_runtime_cache_dir",
    ),
    "default_worker_activities": (
        "histdatacom.sidecar.worker",
        "default_activities",
    ),
    "default_workflows": ("histdatacom.sidecar.worker", "default_workflows"),
    "derive_work_id": ("histdatacom.sidecar.contracts", "derive_work_id"),
    "download_archives_activity": (
        "histdatacom.sidecar.activities",
        "download_archives_activity",
    ),
    "execute_histdata_run_workflow": (
        "histdatacom.sidecar.workflows",
        "execute_histdata_run_workflow",
    ),
    "execute_symbol_timeframe_workflow": (
        "histdatacom.sidecar.workflows",
        "execute_symbol_timeframe_workflow",
    ),
    "extract_csv_activity": (
        "histdatacom.sidecar.activities",
        "extract_csv_activity",
    ),
    "get_job_artifacts": ("histdatacom.sidecar.client", "get_job_artifacts"),
    "get_job_artifacts_sync": (
        "histdatacom.sidecar.client",
        "get_job_artifacts_sync",
    ),
    "get_job_logs": ("histdatacom.sidecar.client", "get_job_logs"),
    "get_job_logs_sync": ("histdatacom.sidecar.client", "get_job_logs_sync"),
    "get_job_progress": ("histdatacom.sidecar.client", "get_job_progress"),
    "get_job_progress_sync": (
        "histdatacom.sidecar.client",
        "get_job_progress_sync",
    ),
    "get_job_result": ("histdatacom.sidecar.client", "get_job_result"),
    "get_job_result_sync": (
        "histdatacom.sidecar.client",
        "get_job_result_sync",
    ),
    "import_to_influx_activity": (
        "histdatacom.sidecar.activities",
        "import_to_influx_activity",
    ),
    "inspect_job_status": (
        "histdatacom.sidecar.client",
        "inspect_job_status",
    ),
    "inspect_job_status_sync": (
        "histdatacom.sidecar.client",
        "inspect_job_status_sync",
    ),
    "lifecycle_from_work_status": (
        "histdatacom.sidecar.control",
        "lifecycle_from_work_status",
    ),
    "list_job_statuses": ("histdatacom.sidecar.client", "list_job_statuses"),
    "list_job_statuses_sync": (
        "histdatacom.sidecar.client",
        "list_job_statuses_sync",
    ),
    "list_stored_job_statuses": (
        "histdatacom.sidecar.client",
        "list_stored_job_statuses",
    ),
    "load_sidecar_manifest": (
        "histdatacom.sidecar.resources",
        "load_sidecar_manifest",
    ),
    "load_temporal_runtime_index": (
        "histdatacom.sidecar.resources",
        "load_temporal_runtime_index",
    ),
    "max_work_items_per_batch": (
        "histdatacom.sidecar.workflows",
        "max_work_items_per_batch",
    ),
    "max_parallel_child_workflows": (
        "histdatacom.sidecar.workflows",
        "max_parallel_child_workflows",
    ),
    "merge_cache_activity": (
        "histdatacom.sidecar.activities",
        "merge_cache_activity",
    ),
    "new_request_id": ("histdatacom.sidecar.contracts", "new_request_id"),
    "observe_workflow_result": (
        "histdatacom.sidecar.client",
        "observe_workflow_result",
    ),
    "period_batch_partitions": (
        "histdatacom.sidecar.workflows",
        "period_batch_partitions",
    ),
    "repository_refresh_activity": (
        "histdatacom.sidecar.activities",
        "repository_refresh_activity",
    ),
    "request_partitions": (
        "histdatacom.sidecar.workflows",
        "request_partitions",
    ),
    "resume_job": ("histdatacom.sidecar.client", "resume_job"),
    "resume_job_sync": ("histdatacom.sidecar.client", "resume_job_sync"),
    "retry_job": ("histdatacom.sidecar.client", "retry_job"),
    "retry_job_sync": ("histdatacom.sidecar.client", "retry_job_sync"),
    "run_temporal_worker": (
        "histdatacom.sidecar.worker",
        "run_temporal_worker",
    ),
    "run_sidecar_maintenance": (
        "histdatacom.sidecar.maintenance",
        "run_sidecar_maintenance",
    ),
    "sidecar_asset": ("histdatacom.sidecar.resources", "sidecar_asset"),
    "sidecar_executable_path": (
        "histdatacom.sidecar.resources",
        "sidecar_executable_path",
    ),
    "sidecar_job_store": ("histdatacom.sidecar.client", "sidecar_job_store"),
    "sidecar_job_store_path": (
        "histdatacom.sidecar.client",
        "sidecar_job_store_path",
    ),
    "sidecar_job_store_root": (
        "histdatacom.sidecar.client",
        "sidecar_job_store_root",
    ),
    "status_has_csv_artifact": (
        "histdatacom.sidecar.contracts",
        "status_has_csv_artifact",
    ),
    "submit_control_job": (
        "histdatacom.sidecar.client",
        "submit_control_job",
    ),
    "submit_control_job_sync": (
        "histdatacom.sidecar.client",
        "submit_control_job_sync",
    ),
    "submit_run_request": (
        "histdatacom.sidecar.client",
        "submit_run_request",
    ),
    "submit_run_request_and_observe": (
        "histdatacom.sidecar.client",
        "submit_run_request_and_observe",
    ),
    "submit_run_request_and_observe_sync": (
        "histdatacom.sidecar.client",
        "submit_run_request_and_observe_sync",
    ),
    "inspect_temporal_runtime_cache": (
        "histdatacom.sidecar.resources",
        "inspect_temporal_runtime_cache",
    ),
    "prune_temporal_runtime_cache": (
        "histdatacom.sidecar.resources",
        "prune_temporal_runtime_cache",
    ),
    "resolve_temporal_runtime_executable": (
        "histdatacom.sidecar.resources",
        "resolve_temporal_runtime_executable",
    ),
    "temporal_runtime_artifact": (
        "histdatacom.sidecar.resources",
        "temporal_runtime_artifact",
    ),
    "temporal_runtime_cache_entry_dir": (
        "histdatacom.sidecar.resources",
        "temporal_runtime_cache_entry_dir",
    ),
    "temporal_runtime_executable_path": (
        "histdatacom.sidecar.resources",
        "temporal_runtime_executable_path",
    ),
    "validate_urls_activity": (
        "histdatacom.sidecar.activities",
        "validate_urls_activity",
    ),
    "workflow_id_for_request": (
        "histdatacom.sidecar.client",
        "workflow_id_for_request",
    ),
    "workflow_names": ("histdatacom.sidecar.workflows", "workflow_names"),
    "workflow_topology_document": (
        "histdatacom.sidecar.workflows",
        "workflow_topology_document",
    ),
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    """Lazily import public sidecar symbols without workflow import side effects."""
    try:
        module_name, attribute = _EXPORTS[name]
    except KeyError as err:
        raise AttributeError(name) from err
    module = import_module(module_name)
    value = getattr(module, attribute)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """Return module attributes including lazy public exports."""
    return sorted({*globals(), *__all__})
