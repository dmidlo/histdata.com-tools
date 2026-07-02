"""Public Temporal orchestration facade for HistData runtime integrations."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS: dict[str, tuple[str, str]] = {
    "JobHandle": ("histdatacom.orchestration.client", "JobHandle"),
    "JobList": ("histdatacom.orchestration.control", "JobList"),
    "JobResult": ("histdatacom.orchestration.client", "JobResult"),
    "JobSnapshot": ("histdatacom.orchestration.control", "JobSnapshot"),
    "RuntimeDependencyError": (
        "histdatacom.orchestration.client",
        "RuntimeDependencyError",
    ),
    "RuntimeManifest": (
        "histdatacom.orchestration.resources",
        "RuntimeManifest",
    ),
    "RuntimePaths": ("histdatacom.orchestration.runtime", "RuntimePaths"),
    "RuntimePlatformResource": (
        "histdatacom.orchestration.resources",
        "RuntimePlatformResource",
    ),
    "RuntimePolicy": ("histdatacom.orchestration.runtime", "RuntimePolicy"),
    "RuntimePorts": ("histdatacom.orchestration.runtime", "RuntimePorts"),
    "RuntimeStatus": ("histdatacom.orchestration.supervisor", "RuntimeStatus"),
    "RuntimeSupervisor": (
        "histdatacom.orchestration.supervisor",
        "RuntimeSupervisor",
    ),
    "RuntimeTaskQueues": (
        "histdatacom.orchestration.queues",
        "RuntimeTaskQueues",
    ),
    "TemporalExecutableUnavailable": (
        "histdatacom.orchestration.resources",
        "TemporalExecutableUnavailable",
    ),
    "UnsupportedRuntimePlatform": (
        "histdatacom.orchestration.resources",
        "UnsupportedRuntimePlatform",
    ),
    "WorkerConfig": ("histdatacom.orchestration.queues", "WorkerConfig"),
    "BATCHING_METADATA_KEY": (
        "histdatacom.orchestration.workflows",
        "BATCHING_METADATA_KEY",
    ),
    "CONTROL_SCHEMA_VERSION": (
        "histdatacom.orchestration.control",
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
        "histdatacom.orchestration.workflows",
        "DEFAULT_MAX_WORK_ITEMS_PER_BATCH",
    ),
    "DEFAULT_MAX_PARALLEL_CHILD_WORKFLOWS": (
        "histdatacom.orchestration.workflows",
        "DEFAULT_MAX_PARALLEL_CHILD_WORKFLOWS",
    ),
    "DEFAULT_MAX_ARTIFACTS_PER_OWNER": (
        "histdatacom.orchestration.maintenance",
        "DEFAULT_MAX_ARTIFACTS_PER_OWNER",
    ),
    "DEFAULT_MAX_DATASET_PLANS_PER_REQUEST": (
        "histdatacom.orchestration.maintenance",
        "DEFAULT_MAX_DATASET_PLANS_PER_REQUEST",
    ),
    "DEFAULT_MAX_JOB_SNAPSHOTS": (
        "histdatacom.orchestration.maintenance",
        "DEFAULT_MAX_JOB_SNAPSHOTS",
    ),
    "DEFAULT_MAX_LOG_BYTES": (
        "histdatacom.orchestration.maintenance",
        "DEFAULT_MAX_LOG_BYTES",
    ),
    "DEFAULT_MAX_ROTATED_LOGS": (
        "histdatacom.orchestration.maintenance",
        "DEFAULT_MAX_ROTATED_LOGS",
    ),
    "DEFAULT_MAX_STAGE_RESULTS_PER_WORK_ITEM": (
        "histdatacom.orchestration.maintenance",
        "DEFAULT_MAX_STAGE_RESULTS_PER_WORK_ITEM",
    ),
    "DEFAULT_MAX_STATUS_EVENTS_PER_OWNER": (
        "histdatacom.orchestration.maintenance",
        "DEFAULT_MAX_STATUS_EVENTS_PER_OWNER",
    ),
    "DEFAULT_MAX_TEMPORAL_SQLITE_BYTES": (
        "histdatacom.orchestration.maintenance",
        "DEFAULT_MAX_TEMPORAL_SQLITE_BYTES",
    ),
    "DEFAULT_RUN_WORKFLOW_NAME": (
        "histdatacom.orchestration.client",
        "DEFAULT_RUN_WORKFLOW_NAME",
    ),
    "DEFAULT_TASK_QUEUE_PREFIX": (
        "histdatacom.orchestration.queues",
        "DEFAULT_TASK_QUEUE_PREFIX",
    ),
    "DEFAULT_TEMPORAL_NAMESPACE": (
        "histdatacom.orchestration.queues",
        "DEFAULT_TEMPORAL_NAMESPACE",
    ),
    "DEFAULT_WORKFLOWS": (
        "histdatacom.orchestration.workflows",
        "DEFAULT_WORKFLOWS",
    ),
    "FANOUT_METADATA_KEY": (
        "histdatacom.orchestration.workflows",
        "FANOUT_METADATA_KEY",
    ),
    "MAX_PARALLEL_CHILD_WORKFLOWS_METADATA_KEY": (
        "histdatacom.orchestration.workflows",
        "MAX_PARALLEL_CHILD_WORKFLOWS_METADATA_KEY",
    ),
    "MAX_WORK_ITEMS_PER_BATCH_METADATA_KEY": (
        "histdatacom.orchestration.workflows",
        "MAX_WORK_ITEMS_PER_BATCH_METADATA_KEY",
    ),
    "TASK_QUEUE_METADATA_KEY": (
        "histdatacom.orchestration.workflow_metadata",
        "TASK_QUEUE_METADATA_KEY",
    ),
    "TOPOLOGY_METADATA_KEY": (
        "histdatacom.orchestration.workflow_metadata",
        "TOPOLOGY_METADATA_KEY",
    ),
    "TOPOLOGY_SCHEMA_VERSION": (
        "histdatacom.orchestration.workflow_metadata",
        "TOPOLOGY_SCHEMA_VERSION",
    ),
    "WORKFLOW_TOPOLOGY": (
        "histdatacom.orchestration.workflows",
        "WORKFLOW_TOPOLOGY",
    ),
    "ArtifactRef": ("histdatacom.orchestration.contracts", "ArtifactRef"),
    "ActivityExecutor": (
        "histdatacom.orchestration.workflows",
        "ActivityExecutor",
    ),
    "ChildWorkflowExecutor": (
        "histdatacom.orchestration.workflows",
        "ChildWorkflowExecutor",
    ),
    "ControlOperationName": (
        "histdatacom.orchestration.control",
        "ControlOperationName",
    ),
    "ControlOperationPhase": (
        "histdatacom.orchestration.control",
        "ControlOperationPhase",
    ),
    "ControlOperationState": (
        "histdatacom.orchestration.control",
        "ControlOperationState",
    ),
    "FailureInfo": ("histdatacom.orchestration.contracts", "FailureInfo"),
    "HistDataRunWorkflow": (
        "histdatacom.orchestration.workflows",
        "HistDataRunWorkflow",
    ),
    "INLINE_WORK_ITEM_LIMIT_METADATA_KEY": (
        "histdatacom.manifest_store",
        "INLINE_WORK_ITEM_LIMIT_METADATA_KEY",
    ),
    "JobControlAction": (
        "histdatacom.orchestration.control",
        "JobControlAction",
    ),
    "JobControlStates": (
        "histdatacom.orchestration.control",
        "JobControlStates",
    ),
    "JobLifecycle": ("histdatacom.orchestration.control", "JobLifecycle"),
    "JobLogEntry": ("histdatacom.orchestration.control", "JobLogEntry"),
    "JobProgressSnapshot": (
        "histdatacom.orchestration.control",
        "JobProgressSnapshot",
    ),
    "JSONScalar": ("histdatacom.orchestration.contracts", "JSONScalar"),
    "JSONValue": ("histdatacom.orchestration.contracts", "JSONValue"),
    "LogMaintenanceResult": (
        "histdatacom.orchestration.maintenance",
        "LogMaintenanceResult",
    ),
    "MAINTENANCE_SCHEMA_VERSION": (
        "histdatacom.orchestration.maintenance",
        "MAINTENANCE_SCHEMA_VERSION",
    ),
    "PortAllocationError": (
        "histdatacom.orchestration.runtime",
        "PortAllocationError",
    ),
    "PLAN_SPILL_METADATA_KEY": (
        "histdatacom.manifest_store",
        "PLAN_SPILL_METADATA_KEY",
    ),
    "OrchestrationExecutableUnavailable": (
        "histdatacom.orchestration.resources",
        "OrchestrationExecutableUnavailable",
    ),
    "OrchestrationJobHandle": (
        "histdatacom.orchestration.client",
        "OrchestrationJobHandle",
    ),
    "OrchestrationJobList": (
        "histdatacom.orchestration.control",
        "OrchestrationJobList",
    ),
    "OrchestrationJobResult": (
        "histdatacom.orchestration.client",
        "OrchestrationJobResult",
    ),
    "OrchestrationJobSnapshot": (
        "histdatacom.orchestration.control",
        "OrchestrationJobSnapshot",
    ),
    "OrchestrationManifest": (
        "histdatacom.orchestration.resources",
        "OrchestrationManifest",
    ),
    "OrchestrationMaintenanceResult": (
        "histdatacom.orchestration.maintenance",
        "OrchestrationMaintenanceResult",
    ),
    "OrchestrationPaths": (
        "histdatacom.orchestration.runtime",
        "OrchestrationPaths",
    ),
    "OrchestrationPlatformResource": (
        "histdatacom.orchestration.resources",
        "OrchestrationPlatformResource",
    ),
    "OrchestrationPorts": (
        "histdatacom.orchestration.runtime",
        "OrchestrationPorts",
    ),
    "OrchestrationResourceError": (
        "histdatacom.orchestration.resources",
        "OrchestrationResourceError",
    ),
    "OrchestrationRetentionPolicy": (
        "histdatacom.orchestration.maintenance",
        "OrchestrationRetentionPolicy",
    ),
    "OrchestrationRuntimePolicy": (
        "histdatacom.orchestration.runtime",
        "OrchestrationRuntimePolicy",
    ),
    "OrchestrationStatus": (
        "histdatacom.orchestration.supervisor",
        "OrchestrationStatus",
    ),
    "OrchestrationSupervisor": (
        "histdatacom.orchestration.supervisor",
        "OrchestrationSupervisor",
    ),
    "OrchestrationTaskQueues": (
        "histdatacom.orchestration.queues",
        "OrchestrationTaskQueues",
    ),
    "OrchestrationOverlapError": (
        "histdatacom.orchestration.client",
        "OrchestrationOverlapError",
    ),
    "OrchestrationUnavailableError": (
        "histdatacom.orchestration.client",
        "OrchestrationUnavailableError",
    ),
    "OrchestrationWorkerConfig": (
        "histdatacom.orchestration.queues",
        "OrchestrationWorkerConfig",
    ),
    "StatusStoreMaintenanceResult": (
        "histdatacom.orchestration.maintenance",
        "StatusStoreMaintenanceResult",
    ),
    "RunRequest": ("histdatacom.orchestration.contracts", "RunRequest"),
    "StageResult": ("histdatacom.orchestration.contracts", "StageResult"),
    "StatusEvent": ("histdatacom.orchestration.contracts", "StatusEvent"),
    "TaskQueueLane": ("histdatacom.orchestration.queues", "TaskQueueLane"),
    "TemporalSqliteMaintenanceResult": (
        "histdatacom.orchestration.maintenance",
        "TemporalSqliteMaintenanceResult",
    ),
    "TemporalActivityExecutor": (
        "histdatacom.orchestration.workflows",
        "TemporalActivityExecutor",
    ),
    "TemporalDependencyError": (
        "histdatacom.orchestration.client",
        "TemporalDependencyError",
    ),
    "TemporalRuntimeArtifact": (
        "histdatacom.orchestration.resources",
        "TemporalRuntimeArtifact",
    ),
    "TemporalRuntimeCacheEntry": (
        "histdatacom.orchestration.resources",
        "TemporalRuntimeCacheEntry",
    ),
    "TemporalRuntimeChecksumError": (
        "histdatacom.orchestration.resources",
        "TemporalRuntimeChecksumError",
    ),
    "TemporalRuntimeIndex": (
        "histdatacom.orchestration.resources",
        "TemporalRuntimeIndex",
    ),
    "TemporalRuntimeOfflineError": (
        "histdatacom.orchestration.resources",
        "TemporalRuntimeOfflineError",
    ),
    "TemporalRuntimeProvisioningError": (
        "histdatacom.orchestration.resources",
        "TemporalRuntimeProvisioningError",
    ),
    "TemporalRuntimeResolution": (
        "histdatacom.orchestration.resources",
        "TemporalRuntimeResolution",
    ),
    "UnsupportedOrchestrationPlatform": (
        "histdatacom.orchestration.resources",
        "UnsupportedOrchestrationPlatform",
    ),
    "WorkItem": ("histdatacom.orchestration.contracts", "WorkItem"),
    "WorkStatus": ("histdatacom.orchestration.contracts", "WorkStatus"),
    "WorkflowInvocation": (
        "histdatacom.orchestration.workflows",
        "WorkflowInvocation",
    ),
    "WorkflowProgress": (
        "histdatacom.orchestration.workflows",
        "WorkflowProgress",
    ),
    "WorkflowSpec": ("histdatacom.orchestration.workflows", "WorkflowSpec"),
    "build_cache_activity": (
        "histdatacom.orchestration.activities",
        "build_cache_activity",
    ),
    "build_run_child_invocations": (
        "histdatacom.orchestration.workflows",
        "build_run_child_invocations",
    ),
    "build_orchestration_runtime_policy": (
        "histdatacom.orchestration.runtime",
        "build_orchestration_runtime_policy",
    ),
    "build_runtime_policy": (
        "histdatacom.orchestration.runtime",
        "build_runtime_policy",
    ),
    "build_orchestration_task_queues": (
        "histdatacom.orchestration.queues",
        "build_orchestration_task_queues",
    ),
    "build_task_queues": (
        "histdatacom.orchestration.queues",
        "build_task_queues",
    ),
    "build_orchestration_worker_config": (
        "histdatacom.orchestration.queues",
        "build_orchestration_worker_config",
    ),
    "build_worker_config": (
        "histdatacom.orchestration.queues",
        "build_worker_config",
    ),
    "build_worker_start_command": (
        "histdatacom.orchestration.supervisor",
        "build_worker_start_command",
    ),
    "build_symbol_batch_invocations": (
        "histdatacom.orchestration.workflows",
        "build_symbol_batch_invocations",
    ),
    "build_symbol_child_invocations": (
        "histdatacom.orchestration.workflows",
        "build_symbol_child_invocations",
    ),
    "build_temporal_start_command": (
        "histdatacom.orchestration.supervisor",
        "build_temporal_start_command",
    ),
    "build_temporal_worker": (
        "histdatacom.orchestration.worker",
        "build_temporal_worker",
    ),
    "cancel_job": ("histdatacom.orchestration.client", "cancel_job"),
    "cancel_job_sync": ("histdatacom.orchestration.client", "cancel_job_sync"),
    "connect_temporal_client": (
        "histdatacom.orchestration.client",
        "connect_temporal_client",
    ),
    "current_platform_key": (
        "histdatacom.orchestration.resources",
        "current_platform_key",
    ),
    "dataset_plan_activity": (
        "histdatacom.orchestration.activities",
        "dataset_plan_activity",
    ),
    "default_activities": (
        "histdatacom.orchestration.activities",
        "default_activities",
    ),
    "default_orchestration_runtime_home": (
        "histdatacom.orchestration.runtime",
        "default_orchestration_runtime_home",
    ),
    "default_runtime_home": (
        "histdatacom.orchestration.runtime",
        "default_runtime_home",
    ),
    "default_orchestration_state_dir": (
        "histdatacom.orchestration.runtime",
        "default_orchestration_state_dir",
    ),
    "default_state_dir": (
        "histdatacom.orchestration.runtime",
        "default_state_dir",
    ),
    "default_orchestration_workspace": (
        "histdatacom.orchestration.runtime",
        "default_orchestration_workspace",
    ),
    "default_workspace": (
        "histdatacom.orchestration.runtime",
        "default_workspace",
    ),
    "default_temporal_runtime_cache_dir": (
        "histdatacom.orchestration.resources",
        "default_temporal_runtime_cache_dir",
    ),
    "default_worker_activities": (
        "histdatacom.orchestration.worker",
        "default_activities",
    ),
    "default_workflows": (
        "histdatacom.orchestration.worker",
        "default_workflows",
    ),
    "derive_work_id": ("histdatacom.orchestration.contracts", "derive_work_id"),
    "download_archives_activity": (
        "histdatacom.orchestration.activities",
        "download_archives_activity",
    ),
    "execute_histdata_run_workflow": (
        "histdatacom.orchestration.workflows",
        "execute_histdata_run_workflow",
    ),
    "execute_symbol_timeframe_workflow": (
        "histdatacom.orchestration.workflows",
        "execute_symbol_timeframe_workflow",
    ),
    "extract_csv_activity": (
        "histdatacom.orchestration.activities",
        "extract_csv_activity",
    ),
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
    "import_to_influx_activity": (
        "histdatacom.orchestration.activities",
        "import_to_influx_activity",
    ),
    "inspect_job_status": (
        "histdatacom.orchestration.client",
        "inspect_job_status",
    ),
    "inspect_job_status_sync": (
        "histdatacom.orchestration.client",
        "inspect_job_status_sync",
    ),
    "lifecycle_from_work_status": (
        "histdatacom.orchestration.control",
        "lifecycle_from_work_status",
    ),
    "list_job_statuses": (
        "histdatacom.orchestration.client",
        "list_job_statuses",
    ),
    "list_job_statuses_sync": (
        "histdatacom.orchestration.client",
        "list_job_statuses_sync",
    ),
    "list_stored_job_statuses": (
        "histdatacom.orchestration.client",
        "list_stored_job_statuses",
    ),
    "load_orchestration_manifest": (
        "histdatacom.orchestration.resources",
        "load_orchestration_manifest",
    ),
    "load_temporal_runtime_index": (
        "histdatacom.orchestration.resources",
        "load_temporal_runtime_index",
    ),
    "max_work_items_per_batch": (
        "histdatacom.orchestration.workflows",
        "max_work_items_per_batch",
    ),
    "max_parallel_child_workflows": (
        "histdatacom.orchestration.workflows",
        "max_parallel_child_workflows",
    ),
    "merge_cache_activity": (
        "histdatacom.orchestration.activities",
        "merge_cache_activity",
    ),
    "new_request_id": ("histdatacom.orchestration.contracts", "new_request_id"),
    "observe_workflow_result": (
        "histdatacom.orchestration.client",
        "observe_workflow_result",
    ),
    "period_batch_partitions": (
        "histdatacom.orchestration.workflows",
        "period_batch_partitions",
    ),
    "repository_refresh_activity": (
        "histdatacom.orchestration.activities",
        "repository_refresh_activity",
    ),
    "request_partitions": (
        "histdatacom.orchestration.workflows",
        "request_partitions",
    ),
    "resume_job": ("histdatacom.orchestration.client", "resume_job"),
    "resume_job_sync": ("histdatacom.orchestration.client", "resume_job_sync"),
    "retry_job": ("histdatacom.orchestration.client", "retry_job"),
    "retry_job_sync": ("histdatacom.orchestration.client", "retry_job_sync"),
    "run_temporal_worker": (
        "histdatacom.orchestration.worker",
        "run_temporal_worker",
    ),
    "run_orchestration_maintenance": (
        "histdatacom.orchestration.maintenance",
        "run_orchestration_maintenance",
    ),
    "orchestration_asset": (
        "histdatacom.orchestration.resources",
        "orchestration_asset",
    ),
    "runtime_asset": ("histdatacom.orchestration.resources", "runtime_asset"),
    "orchestration_executable_path": (
        "histdatacom.orchestration.resources",
        "orchestration_executable_path",
    ),
    "packaged_temporal_executable_path": (
        "histdatacom.orchestration.resources",
        "packaged_temporal_executable_path",
    ),
    "orchestration_job_store": (
        "histdatacom.orchestration.client",
        "orchestration_job_store",
    ),
    "job_store": ("histdatacom.orchestration.client", "job_store"),
    "orchestration_job_store_path": (
        "histdatacom.orchestration.client",
        "orchestration_job_store_path",
    ),
    "job_store_path": ("histdatacom.orchestration.client", "job_store_path"),
    "orchestration_job_store_root": (
        "histdatacom.orchestration.client",
        "orchestration_job_store_root",
    ),
    "job_store_root": ("histdatacom.orchestration.client", "job_store_root"),
    "status_has_csv_artifact": (
        "histdatacom.orchestration.contracts",
        "status_has_csv_artifact",
    ),
    "submit_control_job": (
        "histdatacom.orchestration.client",
        "submit_control_job",
    ),
    "submit_control_job_sync": (
        "histdatacom.orchestration.client",
        "submit_control_job_sync",
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
    "inspect_temporal_runtime_cache": (
        "histdatacom.orchestration.resources",
        "inspect_temporal_runtime_cache",
    ),
    "load_runtime_manifest": (
        "histdatacom.orchestration.resources",
        "load_runtime_manifest",
    ),
    "prune_temporal_runtime_cache": (
        "histdatacom.orchestration.resources",
        "prune_temporal_runtime_cache",
    ),
    "read_runtime_asset_text": (
        "histdatacom.orchestration.resources",
        "read_runtime_asset_text",
    ),
    "resolve_temporal_runtime_executable": (
        "histdatacom.orchestration.resources",
        "resolve_temporal_runtime_executable",
    ),
    "resolve_worker_config": (
        "histdatacom.orchestration.client",
        "resolve_worker_config",
    ),
    "runtime_platform_resource": (
        "histdatacom.orchestration.resources",
        "runtime_platform_resource",
    ),
    "temporal_runtime_artifact": (
        "histdatacom.orchestration.resources",
        "temporal_runtime_artifact",
    ),
    "temporal_runtime_cache_entry_dir": (
        "histdatacom.orchestration.resources",
        "temporal_runtime_cache_entry_dir",
    ),
    "temporal_runtime_executable_path": (
        "histdatacom.orchestration.resources",
        "temporal_runtime_executable_path",
    ),
    "validate_urls_activity": (
        "histdatacom.orchestration.activities",
        "validate_urls_activity",
    ),
    "workflow_id_for_request": (
        "histdatacom.orchestration.client",
        "workflow_id_for_request",
    ),
    "workflow_names": ("histdatacom.orchestration.workflows", "workflow_names"),
    "workflow_topology_document": (
        "histdatacom.orchestration.workflows",
        "workflow_topology_document",
    ),
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    """Lazily import public orchestration symbols without workflow import side effects."""
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
