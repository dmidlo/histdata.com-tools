"""Temporal workflow topology for HistData sidecar jobs.

The topology keeps workflow histories bounded so they carry request metadata,
partition identifiers, status events, and artifact references instead of
downloaded rows, dataframes, or queue payloads.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta
from importlib import import_module
from typing import Any, Callable, Mapping, Protocol, TypeVar, cast

from histdatacom.exceptions import (
    ActivityRetryPolicy,
    IDEMPOTENT_WRITE_RETRY_POLICY,
    NETWORK_RETRY_POLICY,
    NO_RETRY_POLICY,
    STANDARD_RETRY_POLICY,
)
from histdatacom.manifest_store import (
    DATASET_PLAN_BATCHES_KEY,
    DATASET_PLAN_REF_KEY,
    DEFAULT_DATASET_PLAN_INLINE_WORK_ITEM_LIMIT,
    INLINE_WORK_ITEM_LIMIT_METADATA_KEY,
    PLAN_SPILL_METADATA_KEY,
)
from histdatacom.observability import (
    PROGRESS_EVENT_TYPE,
    progress_percent,
    progress_rate_per_second,
)
from histdatacom.runtime_contracts import (
    ArtifactRef,
    FailureInfo,
    JSONValue,
    RunRequest,
    StageResult,
    StatusEvent,
    WorkItem,
    WorkStatus,
    derive_work_id,
)
from histdatacom.sidecar.client import (
    TEMPORAL_EXTRA_HINT,
    TemporalDependencyError,
)
from histdatacom.sidecar.queues import TaskQueueLane
from histdatacom.sidecar.workflow_metadata import (
    TASK_QUEUE_METADATA_KEY,
    TOPOLOGY_METADATA_KEY,
    TOPOLOGY_SCHEMA_VERSION,
)


class _NoopWorkflowApi:
    """No-op decorator shim used when temporalio is not installed."""

    def defn(self, decorated: Any | None = None, **kwargs: Any) -> Any:
        """Return a class decorator compatible with temporalio.workflow.defn."""

        def decorator(value: Any) -> Any:
            return value

        return decorator if decorated is None else decorated

    def run(self, decorated: Any) -> Any:
        """Return a method decorator compatible with temporalio.workflow.run."""
        return decorated

    def query(self, decorated: Any) -> Any:
        """Return a method decorator compatible with temporalio.workflow.query."""
        return decorated

    async def execute_child_workflow(self, *args: Any, **kwargs: Any) -> Any:
        """Fail clearly if real Temporal execution is attempted."""
        raise TemporalDependencyError(TEMPORAL_EXTRA_HINT)


def _load_workflow_api() -> Any:
    try:
        return import_module("temporalio.workflow")
    except ModuleNotFoundError as err:
        if (err.name or "").split(".")[0] == "temporalio":
            return _NoopWorkflowApi()
        raise


workflow = _load_workflow_api()
_Decorated = TypeVar("_Decorated")
_Callable = TypeVar("_Callable", bound=Callable[..., Any])


def workflow_defn(decorated: _Decorated) -> _Decorated:
    """Apply Temporal's workflow class decorator with local typing."""
    return cast(_Decorated, workflow.defn(decorated))


def workflow_run(decorated: _Callable) -> _Callable:
    """Apply Temporal's workflow run decorator with local typing."""
    return cast(_Callable, workflow.run(decorated))


def workflow_query(decorated: _Callable) -> _Callable:
    """Apply Temporal's workflow query decorator with local typing."""
    return cast(_Callable, workflow.query(decorated))


@dataclass(frozen=True, slots=True)
class WorkflowSpec:
    """Document one workflow boundary in the sidecar topology."""

    name: str
    lane: TaskQueueLane
    operation_family: str
    children: tuple[str, ...] = ()
    history_policy: str = (
        "Pass request metadata, partition ids, status events, and artifact "
        "references only; keep rows/dataframes on disk."
    )

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible topology metadata."""
        return {
            "name": self.name,
            "lane": self.lane.value,
            "operation_family": self.operation_family,
            "children": list(self.children),
            "history_policy": self.history_policy,
        }


@dataclass(frozen=True, slots=True)
class ActivityExecutionPolicy:
    """Temporal activity policy metadata for one operation activity."""

    activity_name: str
    start_to_close_timeout_seconds: int
    heartbeat_timeout_seconds: int
    retry_policy: ActivityRetryPolicy

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible activity execution policy metadata."""
        return {
            "activity_name": self.activity_name,
            "start_to_close_timeout_seconds": (
                self.start_to_close_timeout_seconds
            ),
            "heartbeat_timeout_seconds": self.heartbeat_timeout_seconds,
            "retry_policy": self.retry_policy.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class WorkflowInvocation:
    """A planned child workflow call with bounded payload metadata."""

    workflow_name: str
    workflow_id: str
    task_queue_lane: TaskQueueLane
    task_queue: str
    payload: dict[str, JSONValue]

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible invocation metadata."""
        return {
            "workflow_name": self.workflow_name,
            "workflow_id": self.workflow_id,
            "task_queue_lane": self.task_queue_lane.value,
            "task_queue": self.task_queue,
            "payload": dict(self.payload),
        }


@dataclass(slots=True)
class WorkflowProgress:
    """Queryable progress state for a parent or child workflow."""

    workflow_name: str
    request_id: str = ""
    status: WorkStatus = WorkStatus.PLANNED
    current_stage: str = ""
    total_children: int = 0
    completed_children: int = 0
    unit: str = "children"
    started_at_utc: str = ""
    updated_at_utc: str = ""
    rate_per_second: float = 0.0
    last_error: str = ""
    planned_children: tuple[str, ...] = ()
    completed_stages: tuple[str, ...] = ()
    events: tuple[StatusEvent, ...] = ()
    artifacts: tuple[ArtifactRef, ...] = ()

    def start(
        self,
        *,
        request_id: str,
        planned_children: tuple[str, ...],
    ) -> None:
        """Mark the workflow as running with a known child plan."""
        self.request_id = request_id
        self.status = WorkStatus.UNKNOWN
        self.current_stage = "started"
        self.total_children = len(planned_children)
        self.completed_children = 0
        self.unit = "children"
        self.started_at_utc = ""
        self.updated_at_utc = ""
        self.rate_per_second = 0.0
        self.last_error = ""
        self.planned_children = planned_children
        self.completed_stages = ()
        self.events = ()
        self.artifacts = ()

    def record_child(self, stage: str, result: StageResult) -> None:
        """Record one completed child or activity result."""
        self.current_stage = stage
        self.completed_children += 1
        self.completed_stages = (*self.completed_stages, stage)
        latest_timestamp = _latest_event_timestamp(result.events)
        if latest_timestamp:
            if not self.started_at_utc:
                self.started_at_utc = latest_timestamp
            self.updated_at_utc = latest_timestamp
        if result.failure is not None:
            self.last_error = result.failure.message
        else:
            self.last_error = (
                _last_event_error(result.events) or self.last_error
            )
        self.rate_per_second = progress_rate_per_second(
            float(self.completed_children),
            self.started_at_utc,
            self.updated_at_utc,
        )
        self.events = (
            *self.events,
            *result.events,
            self._child_progress_event(stage, result),
        )
        self.artifacts = (*self.artifacts, *result.artifacts)
        if result.status == WorkStatus.CANCELLED:
            self.status = WorkStatus.CANCELLED
        elif result.failure is not None:
            self.status = WorkStatus.FAILED

    def finish(self, status: WorkStatus) -> None:
        """Mark the workflow terminal status."""
        self.status = status
        self.current_stage = "finished"

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible progress for CLI and GUI queries."""
        return {
            "workflow_name": self.workflow_name,
            "request_id": self.request_id,
            "status": self.status.value,
            "current_stage": self.current_stage,
            "total_children": self.total_children,
            "completed_children": self.completed_children,
            "unit": self.unit,
            "percent_complete": progress_percent(
                float(self.completed_children),
                float(self.total_children),
            ),
            "rate_per_second": self.rate_per_second,
            "started_at_utc": self.started_at_utc,
            "updated_at_utc": self.updated_at_utc,
            "last_error": self.last_error,
            "planned_children": list(self.planned_children),
            "completed_stages": list(self.completed_stages),
            "events": [event.to_dict() for event in self.events],
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
        }

    def _child_progress_event(
        self,
        stage: str,
        result: StageResult,
    ) -> StatusEvent:
        return StatusEvent(
            status=result.status,
            stage=stage,
            message=f"{stage} completed.",
            work_id=result.work_id,
            timestamp_utc=self.updated_at_utc,
            metadata={
                "event_type": PROGRESS_EVENT_TYPE,
                "completed": self.completed_children,
                "total": self.total_children,
                "unit": self.unit,
                "increment": 1,
                "percent_complete": progress_percent(
                    float(self.completed_children),
                    float(self.total_children),
                ),
                "rate_per_second": self.rate_per_second,
                "started_at_utc": self.started_at_utc,
                "updated_at_utc": self.updated_at_utc,
                "last_error": self.last_error,
                "child_stage": result.stage,
                "artifact_count": len(result.artifacts),
            },
        )


@dataclass(frozen=True, slots=True)
class _PreparedChildInvocation:
    """A child workflow call prepared for deterministic execution."""

    invocation: WorkflowInvocation
    payload: dict[str, JSONValue] | None = None
    skipped_result: StageResult | None = None


class ChildWorkflowExecutor(Protocol):
    """Execute one child workflow from a parent workflow."""

    async def execute_child_workflow(
        self,
        workflow_name: str,
        payload: Mapping[str, JSONValue],
        *,
        workflow_id: str,
        task_queue: str,
    ) -> Mapping[str, Any]:
        """Execute a child workflow and return its summary payload."""


class ActivityExecutor(Protocol):
    """Execute one operation activity from a child workflow."""

    async def execute_activity(
        self,
        activity_name: str,
        payload: Mapping[str, JSONValue],
        *,
        task_queue: str,
    ) -> Mapping[str, Any]:
        """Execute or fake one activity and return a stage result payload."""


class TemporalChildWorkflowExecutor:
    """Temporal-backed child workflow executor."""

    async def execute_child_workflow(
        self,
        workflow_name: str,
        payload: Mapping[str, JSONValue],
        *,
        workflow_id: str,
        task_queue: str,
    ) -> Mapping[str, Any]:
        """Run a child workflow using Temporal's workflow API."""
        options: dict[str, str] = {"id": workflow_id}
        if task_queue:
            options["task_queue"] = task_queue
        result = await workflow.execute_child_workflow(
            workflow_name,
            dict(payload),
            **options,
        )
        return _coerce_mapping(result)


class TemporalActivityExecutor:
    """Temporal-backed activity executor."""

    async def execute_activity(
        self,
        activity_name: str,
        payload: Mapping[str, JSONValue],
        *,
        task_queue: str,
    ) -> Mapping[str, Any]:
        """Run an operation activity using Temporal's workflow API."""
        policy = activity_execution_policy(activity_name)
        options: dict[str, Any] = {
            "start_to_close_timeout": timedelta(
                seconds=policy.start_to_close_timeout_seconds,
            ),
            "heartbeat_timeout": timedelta(
                seconds=policy.heartbeat_timeout_seconds,
            ),
            "retry_policy": _temporal_retry_policy(policy.retry_policy),
        }
        if task_queue:
            options["task_queue"] = task_queue
        result = await workflow.execute_activity(
            activity_name,
            dict(payload),
            **options,
        )
        return _coerce_mapping(result)


RUN_CHILDREN = (
    "RepositoryRefreshWorkflow",
    "DataQualityWorkflow",
    "DatasetPlanWorkflow",
    "SymbolTimeframeWorkflow",
)
SYMBOL_CHILDREN = (
    "ValidateUrlsWorkflow",
    "DownloadArchivesWorkflow",
    "ExtractCsvWorkflow",
    "BuildCacheWorkflow",
    "MergeCacheWorkflow",
    "ImportWorkflow",
)
WORKFLOW_TOPOLOGY = (
    WorkflowSpec(
        name="HistDataRunWorkflow",
        lane=TaskQueueLane.ORCHESTRATION,
        operation_family="user-visible job",
        children=RUN_CHILDREN,
    ),
    WorkflowSpec(
        name="RepositoryRefreshWorkflow",
        lane=TaskQueueLane.NETWORK,
        operation_family="repository metadata refresh",
    ),
    WorkflowSpec(
        name="DataQualityWorkflow",
        lane=TaskQueueLane.CPU_FILE,
        operation_family="data quality assessment",
    ),
    WorkflowSpec(
        name="DatasetPlanWorkflow",
        lane=TaskQueueLane.CPU_FILE,
        operation_family="dataset planning",
    ),
    WorkflowSpec(
        name="SymbolTimeframeWorkflow",
        lane=TaskQueueLane.ORCHESTRATION,
        operation_family="symbol/timeframe partition",
        children=SYMBOL_CHILDREN,
    ),
    WorkflowSpec(
        name="ValidateUrlsWorkflow",
        lane=TaskQueueLane.NETWORK,
        operation_family="URL validation",
    ),
    WorkflowSpec(
        name="DownloadArchivesWorkflow",
        lane=TaskQueueLane.NETWORK,
        operation_family="archive download",
    ),
    WorkflowSpec(
        name="ExtractCsvWorkflow",
        lane=TaskQueueLane.CPU_FILE,
        operation_family="archive extraction",
    ),
    WorkflowSpec(
        name="BuildCacheWorkflow",
        lane=TaskQueueLane.CPU_FILE,
        operation_family="cache build",
    ),
    WorkflowSpec(
        name="MergeCacheWorkflow",
        lane=TaskQueueLane.CPU_FILE,
        operation_family="cache merge",
    ),
    WorkflowSpec(
        name="ImportWorkflow",
        lane=TaskQueueLane.INFLUX,
        operation_family="Influx import",
    ),
)
WORKFLOW_SPECS_BY_NAME = {spec.name: spec for spec in WORKFLOW_TOPOLOGY}
OPERATION_ACTIVITIES = {
    "RepositoryRefreshWorkflow": "repository_refresh",
    "DataQualityWorkflow": "data_quality",
    "DatasetPlanWorkflow": "dataset_plan",
    "ValidateUrlsWorkflow": "validate_urls",
    "DownloadArchivesWorkflow": "download_archives",
    "ExtractCsvWorkflow": "extract_csv",
    "BuildCacheWorkflow": "build_cache",
    "MergeCacheWorkflow": "merge_cache",
    "ImportWorkflow": "import_to_influx",
}
ACTIVITY_EXECUTION_POLICIES = {
    "repository_refresh": ActivityExecutionPolicy(
        activity_name="repository_refresh",
        start_to_close_timeout_seconds=600,
        heartbeat_timeout_seconds=30,
        retry_policy=NETWORK_RETRY_POLICY,
    ),
    "dataset_plan": ActivityExecutionPolicy(
        activity_name="dataset_plan",
        start_to_close_timeout_seconds=300,
        heartbeat_timeout_seconds=30,
        retry_policy=NO_RETRY_POLICY,
    ),
    "data_quality": ActivityExecutionPolicy(
        activity_name="data_quality",
        start_to_close_timeout_seconds=1800,
        heartbeat_timeout_seconds=30,
        retry_policy=NO_RETRY_POLICY,
    ),
    "validate_urls": ActivityExecutionPolicy(
        activity_name="validate_urls",
        start_to_close_timeout_seconds=600,
        heartbeat_timeout_seconds=30,
        retry_policy=NETWORK_RETRY_POLICY,
    ),
    "download_archives": ActivityExecutionPolicy(
        activity_name="download_archives",
        start_to_close_timeout_seconds=3600,
        heartbeat_timeout_seconds=60,
        retry_policy=NETWORK_RETRY_POLICY,
    ),
    "extract_csv": ActivityExecutionPolicy(
        activity_name="extract_csv",
        start_to_close_timeout_seconds=900,
        heartbeat_timeout_seconds=30,
        retry_policy=NO_RETRY_POLICY,
    ),
    "build_cache": ActivityExecutionPolicy(
        activity_name="build_cache",
        start_to_close_timeout_seconds=1800,
        heartbeat_timeout_seconds=30,
        retry_policy=NO_RETRY_POLICY,
    ),
    "merge_cache": ActivityExecutionPolicy(
        activity_name="merge_cache",
        start_to_close_timeout_seconds=1800,
        heartbeat_timeout_seconds=30,
        retry_policy=STANDARD_RETRY_POLICY,
    ),
    "import_to_influx": ActivityExecutionPolicy(
        activity_name="import_to_influx",
        start_to_close_timeout_seconds=3600,
        heartbeat_timeout_seconds=30,
        retry_policy=IDEMPOTENT_WRITE_RETRY_POLICY,
    ),
}
DEFAULT_MAX_WORK_ITEMS_PER_BATCH = 64
BATCHING_METADATA_KEY = "temporal_batching"
MAX_WORK_ITEMS_PER_BATCH_METADATA_KEY = "max_work_items_per_batch"
DEFAULT_MAX_PARALLEL_CHILD_WORKFLOWS = 4
FANOUT_METADATA_KEY = "temporal_fanout"
MAX_PARALLEL_CHILD_WORKFLOWS_METADATA_KEY = "max_parallel_child_workflows"
SYMBOL_TIMEFRAME_WORKFLOW = "SymbolTimeframeWorkflow"


def workflow_topology_document() -> dict[str, JSONValue]:
    """Return machine-readable workflow topology documentation."""
    return {
        "schema_version": TOPOLOGY_SCHEMA_VERSION,
        "metadata_keys": {
            "task_queues": TASK_QUEUE_METADATA_KEY,
            "topology_version": TOPOLOGY_METADATA_KEY,
            "batching": BATCHING_METADATA_KEY,
            "max_work_items_per_batch": (MAX_WORK_ITEMS_PER_BATCH_METADATA_KEY),
            "fanout": FANOUT_METADATA_KEY,
            "max_parallel_child_workflows": (
                MAX_PARALLEL_CHILD_WORKFLOWS_METADATA_KEY
            ),
            "dataset_plan_ref": DATASET_PLAN_REF_KEY,
            "dataset_plan_batches": DATASET_PLAN_BATCHES_KEY,
            "plan_spill": PLAN_SPILL_METADATA_KEY,
            "inline_work_item_limit": INLINE_WORK_ITEM_LIMIT_METADATA_KEY,
        },
        "activity_policies": [
            policy.to_dict() for policy in ACTIVITY_EXECUTION_POLICIES.values()
        ],
        "history_policy": (
            "Workflow histories carry bounded metadata only. Stage outputs "
            "must return StageResult and ArtifactRef payloads rather than "
            "rows, dataframes, or archive bytes. Dataset plans above "
            f"{DEFAULT_DATASET_PLAN_INLINE_WORK_ITEM_LIMIT} work items spill "
            "work-item metadata to the manifest store and return compact "
            "dataset_plan_ref and dataset_plan_batches payloads. Dataset "
            "work items are grouped into deterministic "
            "pair/timeframe/format/year-month batches before operation "
            "workflows run. Independent symbol/timeframe batch workflows are "
            "started with deterministic bounded fan-out."
        ),
        "workflows": [spec.to_dict() for spec in WORKFLOW_TOPOLOGY],
    }


def workflow_names() -> tuple[str, ...]:
    """Return all registered workflow class names in topology order."""
    return tuple(spec.name for spec in WORKFLOW_TOPOLOGY)


def activity_execution_policy(activity_name: str) -> ActivityExecutionPolicy:
    """Return Temporal execution policy metadata for an activity."""
    try:
        return ACTIVITY_EXECUTION_POLICIES[activity_name]
    except KeyError as err:
        raise ValueError(f"unknown activity policy: {activity_name}") from err


def _temporal_retry_policy(policy: ActivityRetryPolicy) -> Any:
    from temporalio.common import RetryPolicy

    return RetryPolicy(
        initial_interval=timedelta(
            seconds=policy.initial_interval_seconds,
        ),
        backoff_coefficient=policy.backoff_coefficient,
        maximum_interval=timedelta(
            seconds=policy.maximum_interval_seconds,
        ),
        maximum_attempts=policy.maximum_attempts,
        non_retryable_error_types=policy.non_retryable_error_types,
    )


def build_run_child_invocations(
    request: RunRequest,
) -> tuple[WorkflowInvocation, ...]:
    """Plan top-level child workflow calls for a user-visible job."""
    invocations: list[WorkflowInvocation] = []
    if request.data_quality:
        return (_invocation(request, "DataQualityWorkflow"),)

    if request.available_remote_data or request.update_remote_data:
        invocations.append(_invocation(request, "RepositoryRefreshWorkflow"))

    if _requires_dataset_work(request):
        invocations.append(_invocation(request, "DatasetPlanWorkflow"))
        for partition in request_partitions(request):
            invocations.append(
                _invocation(
                    request,
                    "SymbolTimeframeWorkflow",
                    partition=partition,
                )
            )

    return tuple(invocations)


def build_symbol_child_invocations(
    request: RunRequest,
    partition: Mapping[str, str],
    work_items: tuple[WorkItem, ...] = (),
    plan_ref: Mapping[str, JSONValue] | None = None,
) -> tuple[WorkflowInvocation, ...]:
    """Plan operation-family child workflow calls for one partition."""
    invocations: list[WorkflowInvocation] = []
    for workflow_name in _operation_workflow_names(request):
        invocations.append(
            _invocation(
                request,
                workflow_name,
                partition=partition,
                work_items=work_items,
                plan_ref=plan_ref,
            )
        )
    return tuple(invocations)


def build_symbol_batch_invocations(
    request: RunRequest,
    partition: Mapping[str, str],
    work_items: tuple[WorkItem, ...],
    *,
    max_work_items_per_batch: int | None = None,
    plan_ref: Mapping[str, JSONValue] | None = None,
) -> tuple[WorkflowInvocation, ...]:
    """Plan bounded symbol/timeframe child workflows for planned work items."""
    partition_work_items = _partition_work_items(work_items, partition)
    return tuple(
        _invocation(
            request,
            SYMBOL_TIMEFRAME_WORKFLOW,
            partition=batch_partition,
            work_items=batch_work_items,
            plan_ref=plan_ref,
        )
        for batch_partition, batch_work_items in _work_item_batches(
            request,
            partition_work_items,
            max_work_items_per_batch=max_work_items_per_batch,
        )
    )


def request_partitions(request: RunRequest) -> tuple[dict[str, str], ...]:
    """Return coarse symbol/timeframe partitions for child workflows."""
    pairs = request.pairs or ("requested-pairs",)
    timeframes = request.timeframes or ("requested-timeframes",)
    return tuple(
        {"pair": pair, "timeframe": timeframe}
        for pair in pairs
        for timeframe in timeframes
    )


def period_batch_partitions(
    request: RunRequest,
    work_items: tuple[WorkItem, ...],
    *,
    max_work_items_per_batch: int | None = None,
) -> tuple[dict[str, str], ...]:
    """Return deterministic pair/timeframe/format/year-month batches."""
    return tuple(
        partition
        for partition, _batch_work_items in _work_item_batches(
            request,
            work_items,
            max_work_items_per_batch=max_work_items_per_batch,
        )
    )


def max_work_items_per_batch(
    request: RunRequest,
    *,
    default: int = DEFAULT_MAX_WORK_ITEMS_PER_BATCH,
) -> int:
    """Return the configured maximum work items per child workflow batch."""
    metadata = request.metadata
    batching = metadata.get(BATCHING_METADATA_KEY)
    value: object | None = None
    if isinstance(batching, Mapping):
        value = batching.get(MAX_WORK_ITEMS_PER_BATCH_METADATA_KEY)
    if value is None:
        value = metadata.get(MAX_WORK_ITEMS_PER_BATCH_METADATA_KEY)
    if value is None:
        return _positive_batch_size(default)
    return _positive_batch_size(value)


def max_parallel_child_workflows(
    request: RunRequest,
    *,
    default: int = DEFAULT_MAX_PARALLEL_CHILD_WORKFLOWS,
) -> int:
    """Return the configured maximum independent child workflow fan-out."""
    metadata = request.metadata
    fanout = metadata.get(FANOUT_METADATA_KEY)
    value: object | None = None
    if isinstance(fanout, Mapping):
        value = fanout.get(MAX_PARALLEL_CHILD_WORKFLOWS_METADATA_KEY)
    if value is None:
        value = metadata.get(MAX_PARALLEL_CHILD_WORKFLOWS_METADATA_KEY)
    if value is None:
        return _positive_parallelism(default)
    return _positive_parallelism(value)


async def execute_histdata_run_workflow(
    request: RunRequest,
    *,
    executor: ChildWorkflowExecutor,
    progress: WorkflowProgress,
) -> dict[str, JSONValue]:
    """Execute the top-level workflow composition."""
    invocations = build_run_child_invocations(request)
    return await _execute_child_plan(
        request,
        workflow_name="HistDataRunWorkflow",
        invocations=invocations,
        executor=executor,
        progress=progress,
    )


async def execute_symbol_timeframe_workflow(
    request: RunRequest,
    partition: Mapping[str, str],
    work_items: tuple[WorkItem, ...] = (),
    plan_ref: Mapping[str, JSONValue] | None = None,
    *,
    executor: ChildWorkflowExecutor,
    progress: WorkflowProgress,
) -> dict[str, JSONValue]:
    """Execute operation children for one symbol/timeframe partition."""
    invocations = build_symbol_child_invocations(
        request,
        partition,
        work_items=work_items,
        plan_ref=plan_ref,
    )
    return await _execute_child_plan(
        request,
        workflow_name="SymbolTimeframeWorkflow",
        invocations=invocations,
        executor=executor,
        progress=progress,
        partition=partition,
        work_items=work_items,
        plan_ref=plan_ref,
    )


async def execute_activity_workflow(
    workflow_name: str,
    payload: Mapping[str, JSONValue],
    *,
    activity_executor: ActivityExecutor,
    progress: WorkflowProgress,
) -> dict[str, JSONValue]:
    """Execute one leaf activity workflow through an activity executor."""
    request = RunRequest.from_dict(_coerce_mapping(payload.get("request", {})))
    spec = WORKFLOW_SPECS_BY_NAME[workflow_name]
    activity_name = OPERATION_ACTIVITIES[workflow_name]
    task_queue = _task_queue_for_lane(request, spec.lane)
    activity_work_items = _work_items_from_payload(payload)
    stage_payload = {
        **dict(payload),
        "stage": activity_name,
        "activity": activity_name,
        "task_queue": task_queue,
    }
    progress.start(
        request_id=request.request_id,
        planned_children=(activity_name,),
    )
    activity_payload = await activity_executor.execute_activity(
        activity_name,
        stage_payload,
        task_queue=task_queue,
    )
    result = _stage_result_from_mapping(
        activity_payload,
        fallback_stage=activity_name,
    )
    output_work_items = _forwarded_work_items(activity_payload)
    if not _has_work_item_payload(activity_payload):
        output_work_items = activity_work_items
    progress.record_child(activity_name, result)
    progress.finish(result.status)
    return _summary_payload(
        request=request,
        workflow_name=workflow_name,
        progress=progress,
        stage_results=(result,),
        partition=_coerce_mapping(payload.get("partition", {})),
        work_items=output_work_items,
        include_work_items=_has_work_item_payload(activity_payload)
        or bool(activity_work_items),
    )


@workflow_defn
class HistDataRunWorkflow:
    """Parent workflow for one user-visible HistData job."""

    def __init__(
        self,
        executor: ChildWorkflowExecutor | None = None,
    ) -> None:
        self._executor = executor
        self._progress = WorkflowProgress("HistDataRunWorkflow")

    @workflow_run
    async def run(self, request_payload: dict[str, Any]) -> dict:
        """Run repository, planning, and partition child workflows."""
        request = RunRequest.from_dict(request_payload)
        return await execute_histdata_run_workflow(
            request,
            executor=self._executor or TemporalChildWorkflowExecutor(),
            progress=self._progress,
        )

    @workflow_query
    def status(self) -> dict:
        """Return queryable parent workflow progress."""
        return self._progress.to_dict()


@workflow_defn
class SymbolTimeframeWorkflow:
    """Partition workflow for one symbol/timeframe group."""

    def __init__(
        self,
        executor: ChildWorkflowExecutor | None = None,
    ) -> None:
        self._executor = executor
        self._progress = WorkflowProgress("SymbolTimeframeWorkflow")

    @workflow_run
    async def run(self, payload: dict[str, Any]) -> dict:
        """Run operation-family children for one partition."""
        request = RunRequest.from_dict(
            _coerce_mapping(payload.get("request", {}))
        )
        partition = _string_mapping(payload.get("partition", {}))
        work_items = _partition_work_items(
            _work_items_from_payload(payload),
            partition,
        )
        plan_ref = _plan_ref_from_payload(payload)
        return await execute_symbol_timeframe_workflow(
            request,
            partition,
            work_items,
            plan_ref=plan_ref,
            executor=self._executor or TemporalChildWorkflowExecutor(),
            progress=self._progress,
        )

    @workflow_query
    def status(self) -> dict:
        """Return queryable partition workflow progress."""
        return self._progress.to_dict()


class _ActivityWorkflowBase:
    """Base class for leaf workflows that delegate to one activity."""

    workflow_name = ""

    def __init__(
        self,
        activity_executor: ActivityExecutor | None = None,
    ) -> None:
        self._activity_executor = activity_executor
        self._progress = WorkflowProgress(self.workflow_name)

    def _activity_executor_or_default(self) -> ActivityExecutor:
        return self._activity_executor or TemporalActivityExecutor()

    async def _run_activity(self, payload: dict[str, Any]) -> dict:
        return await execute_activity_workflow(
            self.workflow_name,
            payload,
            activity_executor=self._activity_executor_or_default(),
            progress=self._progress,
        )

    def status(self) -> dict:
        """Return queryable leaf workflow progress."""
        return self._progress.to_dict()


@workflow_defn
class RepositoryRefreshWorkflow(_ActivityWorkflowBase):
    """Child workflow for repository metadata refresh."""

    workflow_name = "RepositoryRefreshWorkflow"

    @workflow_run
    async def run(self, payload: dict[str, Any]) -> dict:
        """Run repository refresh as a real activity."""
        return await execute_activity_workflow(
            self.workflow_name,
            payload,
            activity_executor=self._activity_executor_or_default(),
            progress=self._progress,
        )

    status = workflow_query(_ActivityWorkflowBase.status)


@workflow_defn
class DatasetPlanWorkflow(_ActivityWorkflowBase):
    """Child workflow for bounded dataset planning."""

    workflow_name = "DatasetPlanWorkflow"

    @workflow_run
    async def run(self, payload: dict[str, Any]) -> dict:
        """Run dataset planning as a real activity."""
        return await execute_activity_workflow(
            self.workflow_name,
            payload,
            activity_executor=self._activity_executor_or_default(),
            progress=self._progress,
        )

    status = workflow_query(_ActivityWorkflowBase.status)


@workflow_defn
class DataQualityWorkflow(_ActivityWorkflowBase):
    """Child workflow for offline data-quality assessment."""

    workflow_name = "DataQualityWorkflow"

    @workflow_run
    async def run(self, payload: dict[str, Any]) -> dict:
        """Run data quality as a real activity."""
        return await execute_activity_workflow(
            self.workflow_name,
            payload,
            activity_executor=self._activity_executor_or_default(),
            progress=self._progress,
        )

    status = workflow_query(_ActivityWorkflowBase.status)


@workflow_defn
class ValidateUrlsWorkflow(_ActivityWorkflowBase):
    """Child workflow for URL validation."""

    workflow_name = "ValidateUrlsWorkflow"

    @workflow_run
    async def run(self, payload: dict[str, Any]) -> dict:
        """Run URL validation as a real activity."""
        return await execute_activity_workflow(
            self.workflow_name,
            payload,
            activity_executor=self._activity_executor_or_default(),
            progress=self._progress,
        )

    status = workflow_query(_ActivityWorkflowBase.status)


@workflow_defn
class DownloadArchivesWorkflow(_ActivityWorkflowBase):
    """Child workflow for archive downloads."""

    workflow_name = "DownloadArchivesWorkflow"

    @workflow_run
    async def run(self, payload: dict[str, Any]) -> dict:
        """Run archive download as a real activity."""
        return await execute_activity_workflow(
            self.workflow_name,
            payload,
            activity_executor=self._activity_executor_or_default(),
            progress=self._progress,
        )

    status = workflow_query(_ActivityWorkflowBase.status)


@workflow_defn
class ExtractCsvWorkflow(_ActivityWorkflowBase):
    """Child workflow for CSV extraction."""

    workflow_name = "ExtractCsvWorkflow"

    @workflow_run
    async def run(self, payload: dict[str, Any]) -> dict:
        """Run CSV extraction as a real activity."""
        return await execute_activity_workflow(
            self.workflow_name,
            payload,
            activity_executor=self._activity_executor_or_default(),
            progress=self._progress,
        )

    status = workflow_query(_ActivityWorkflowBase.status)


@workflow_defn
class BuildCacheWorkflow(_ActivityWorkflowBase):
    """Child workflow for cache building."""

    workflow_name = "BuildCacheWorkflow"

    @workflow_run
    async def run(self, payload: dict[str, Any]) -> dict:
        """Run cache build as a real activity."""
        return await execute_activity_workflow(
            self.workflow_name,
            payload,
            activity_executor=self._activity_executor_or_default(),
            progress=self._progress,
        )

    status = workflow_query(_ActivityWorkflowBase.status)


@workflow_defn
class MergeCacheWorkflow(_ActivityWorkflowBase):
    """Child workflow for cache merging."""

    workflow_name = "MergeCacheWorkflow"

    @workflow_run
    async def run(self, payload: dict[str, Any]) -> dict:
        """Run cache merge as a real activity."""
        return await execute_activity_workflow(
            self.workflow_name,
            payload,
            activity_executor=self._activity_executor_or_default(),
            progress=self._progress,
        )

    status = workflow_query(_ActivityWorkflowBase.status)


@workflow_defn
class ImportWorkflow(_ActivityWorkflowBase):
    """Child workflow for Influx import."""

    workflow_name = "ImportWorkflow"

    @workflow_run
    async def run(self, payload: dict[str, Any]) -> dict:
        """Run Influx import as a real activity."""
        return await execute_activity_workflow(
            self.workflow_name,
            payload,
            activity_executor=self._activity_executor_or_default(),
            progress=self._progress,
        )

    status = workflow_query(_ActivityWorkflowBase.status)


DEFAULT_WORKFLOWS = (
    HistDataRunWorkflow,
    RepositoryRefreshWorkflow,
    DataQualityWorkflow,
    DatasetPlanWorkflow,
    SymbolTimeframeWorkflow,
    ValidateUrlsWorkflow,
    DownloadArchivesWorkflow,
    ExtractCsvWorkflow,
    BuildCacheWorkflow,
    MergeCacheWorkflow,
    ImportWorkflow,
)


def _execute_status(results: tuple[StageResult, ...]) -> WorkStatus:
    if any(result.status == WorkStatus.CANCELLED for result in results):
        return WorkStatus.CANCELLED
    if any(result.failure is not None for result in results):
        return WorkStatus.FAILED
    if results and all(
        result.status == WorkStatus.COMPLETED for result in results
    ):
        return WorkStatus.COMPLETED
    if any(result.status == WorkStatus.PLANNED for result in results):
        return WorkStatus.PLANNED
    return WorkStatus.COMPLETED


async def _execute_child_plan(
    request: RunRequest,
    *,
    workflow_name: str,
    invocations: tuple[WorkflowInvocation, ...],
    executor: ChildWorkflowExecutor,
    progress: WorkflowProgress,
    partition: Mapping[str, str] | None = None,
    work_items: tuple[WorkItem, ...] = (),
    plan_ref: Mapping[str, JSONValue] | None = None,
) -> dict[str, JSONValue]:
    progress.start(
        request_id=request.request_id,
        planned_children=tuple(
            invocation.workflow_name for invocation in invocations
        ),
    )
    results: list[StageResult] = []
    current_work_items = tuple(work_items)
    current_plan_ref = dict(plan_ref or {})
    current_plan_batches: tuple[dict[str, str], ...] = ()
    pending_invocations = list(invocations)
    index = 0
    while index < len(pending_invocations):
        invocation = pending_invocations[index]
        if invocation.workflow_name == SYMBOL_TIMEFRAME_WORKFLOW:
            pending_invocations, expanded = _expand_pending_symbol_invocations(
                request,
                pending_invocations,
                start_index=index,
                current_work_items=current_work_items,
                plan_ref=current_plan_ref,
                plan_batches=current_plan_batches,
            )
            if expanded:
                _refresh_progress_plan(progress, tuple(pending_invocations))
            symbol_invocations, next_index = _contiguous_invocations(
                pending_invocations,
                start_index=index,
                workflow_name=SYMBOL_TIMEFRAME_WORKFLOW,
            )
            symbol_results = await _execute_parallel_symbol_invocations(
                request,
                invocations=symbol_invocations,
                current_work_items=current_work_items,
                plan_ref=current_plan_ref,
                executor=executor,
                progress=progress,
            )
            results.extend(symbol_results)
            index = next_index
            if any(_stops_symbol_fanout(result) for result in symbol_results):
                break
            continue

        prepared = _prepare_child_invocation(
            invocation,
            current_work_items,
            plan_ref=current_plan_ref,
        )
        if prepared.skipped_result is not None:
            result = prepared.skipped_result
            results.append(result)
            progress.record_child(invocation.workflow_name, result)
            index += 1
            continue

        result_payload = await executor.execute_child_workflow(
            invocation.workflow_name,
            prepared.payload or {},
            workflow_id=invocation.workflow_id,
            task_queue=invocation.task_queue,
        )
        result = _stage_result_from_mapping(
            result_payload,
            fallback_stage=invocation.workflow_name,
        )
        forwarded_work_items = _forwarded_work_items(result_payload)
        if invocation.workflow_name == "DatasetPlanWorkflow":
            current_work_items = forwarded_work_items
            current_plan_ref = _plan_ref_from_payload(result_payload)
            current_plan_batches = _plan_batches_from_payload(result_payload)
        elif invocation.workflow_name == "SymbolTimeframeWorkflow":
            pass
        elif _requires_work_items(invocation.workflow_name):
            if forwarded_work_items or _has_work_item_payload(result_payload):
                current_work_items = forwarded_work_items
        results.append(result)
        progress.record_child(invocation.workflow_name, result)
        index += 1
        if result.status == WorkStatus.CANCELLED:
            break
    progress.finish(_execute_status(tuple(results)))
    return _summary_payload(
        request=request,
        workflow_name=workflow_name,
        progress=progress,
        stage_results=tuple(results),
        partition=partition,
        work_items=current_work_items,
        include_work_items=bool(work_items),
    )


def _expand_pending_symbol_invocations(
    request: RunRequest,
    pending_invocations: list[WorkflowInvocation],
    *,
    start_index: int,
    current_work_items: tuple[WorkItem, ...],
    plan_ref: Mapping[str, JSONValue],
    plan_batches: tuple[dict[str, str], ...],
) -> tuple[list[WorkflowInvocation], bool]:
    expanded: list[WorkflowInvocation] = []
    changed = False
    for index, invocation in enumerate(pending_invocations):
        if (
            index < start_index
            or invocation.workflow_name != SYMBOL_TIMEFRAME_WORKFLOW
        ):
            expanded.append(invocation)
            continue

        partition = _string_mapping(invocation.payload.get("partition", {}))
        if _is_period_batch_partition(partition):
            expanded.append(invocation)
            continue

        if current_work_items:
            batch_invocations = build_symbol_batch_invocations(
                request,
                partition,
                current_work_items,
                plan_ref=plan_ref,
            )
        else:
            batch_invocations = _symbol_batch_invocations_from_plan_ref(
                request,
                partition,
                plan_ref=plan_ref,
                plan_batches=plan_batches,
            )
        if not batch_invocations:
            expanded.append(invocation)
            continue

        expanded.extend(batch_invocations)
        changed = True
    return expanded, changed


def _contiguous_invocations(
    invocations: list[WorkflowInvocation],
    *,
    start_index: int,
    workflow_name: str,
) -> tuple[tuple[WorkflowInvocation, ...], int]:
    group: list[WorkflowInvocation] = []
    index = start_index
    while (
        index < len(invocations)
        and invocations[index].workflow_name == workflow_name
    ):
        group.append(invocations[index])
        index += 1
    return tuple(group), index


def _symbol_batch_invocations_from_plan_ref(
    request: RunRequest,
    partition: Mapping[str, str],
    *,
    plan_ref: Mapping[str, JSONValue],
    plan_batches: tuple[dict[str, str], ...],
) -> tuple[WorkflowInvocation, ...]:
    if not plan_ref:
        return ()
    return tuple(
        _invocation(
            request,
            SYMBOL_TIMEFRAME_WORKFLOW,
            partition=batch_partition,
            plan_ref=plan_ref,
        )
        for batch_partition in plan_batches
        if _partition_matches(batch_partition, partition)
    )


async def _execute_parallel_symbol_invocations(
    request: RunRequest,
    *,
    invocations: tuple[WorkflowInvocation, ...],
    current_work_items: tuple[WorkItem, ...],
    plan_ref: Mapping[str, JSONValue],
    executor: ChildWorkflowExecutor,
    progress: WorkflowProgress,
) -> tuple[StageResult, ...]:
    max_parallel = max_parallel_child_workflows(request)
    prepared = tuple(
        _prepare_child_invocation(
            invocation,
            current_work_items,
            plan_ref=plan_ref,
        )
        for invocation in invocations
    )
    results: list[StageResult] = []
    index = 0
    while index < len(prepared):
        window = prepared[index : index + max_parallel]
        window_results = await _execute_prepared_child_window(
            window,
            executor=executor,
        )
        for prepared_child, result in zip(window, window_results):
            results.append(result)
            progress.record_child(
                prepared_child.invocation.workflow_name,
                result,
            )
        index += max_parallel
        if any(_stops_symbol_fanout(result) for result in window_results):
            break
    return tuple(results)


async def _execute_prepared_child_window(
    window: tuple[_PreparedChildInvocation, ...],
    *,
    executor: ChildWorkflowExecutor,
) -> tuple[StageResult, ...]:
    result_by_position: dict[int, StageResult] = {}
    executable_children: list[
        tuple[int, WorkflowInvocation, dict[str, JSONValue]]
    ] = []
    for position, prepared in enumerate(window):
        if prepared.skipped_result is not None:
            result_by_position[position] = prepared.skipped_result
            continue
        payload = prepared.payload
        if payload is None:
            result_by_position[position] = _skipped_workflow_result(
                prepared.invocation,
                reason="No child workflow payload was prepared.",
            )
            continue
        executable_children.append((position, prepared.invocation, payload))

    child_payloads = await asyncio.gather(
        *(
            executor.execute_child_workflow(
                invocation.workflow_name,
                payload,
                workflow_id=invocation.workflow_id,
                task_queue=invocation.task_queue,
            )
            for _position, invocation, payload in executable_children
        ),
        return_exceptions=True,
    )
    for (position, invocation, _payload), payload_or_error in zip(
        executable_children,
        child_payloads,
    ):
        if isinstance(payload_or_error, asyncio.CancelledError):
            raise payload_or_error
        if isinstance(payload_or_error, BaseException):
            result_by_position[position] = _child_exception_result(
                invocation,
                payload_or_error,
            )
            continue
        result_by_position[position] = _stage_result_from_mapping(
            payload_or_error,
            fallback_stage=invocation.workflow_name,
        )

    return tuple(
        result_by_position[position] for position in range(len(window))
    )


def _prepare_child_invocation(
    invocation: WorkflowInvocation,
    current_work_items: tuple[WorkItem, ...],
    *,
    plan_ref: Mapping[str, JSONValue],
) -> _PreparedChildInvocation:
    partition = _string_mapping(invocation.payload.get("partition", {}))
    work_items = _partition_work_items(current_work_items, partition)
    if _requires_work_items(invocation.workflow_name):
        if not work_items:
            invocation_plan_ref = _plan_ref_from_payload(invocation.payload)
            if invocation_plan_ref or plan_ref:
                payload = dict(invocation.payload)
                if DATASET_PLAN_REF_KEY not in payload:
                    payload[DATASET_PLAN_REF_KEY] = cast(
                        JSONValue,
                        dict(invocation_plan_ref or plan_ref),
                    )
                return _PreparedChildInvocation(
                    invocation=invocation,
                    payload=payload,
                )
            return _PreparedChildInvocation(
                invocation=invocation,
                skipped_result=_skipped_workflow_result(
                    invocation,
                    reason="No forwardable work items for this stage.",
                ),
            )
        return _PreparedChildInvocation(
            invocation=invocation,
            payload=_payload_with_work_items(invocation.payload, work_items),
        )
    return _PreparedChildInvocation(
        invocation=invocation,
        payload=dict(invocation.payload),
    )


def _child_exception_result(
    invocation: WorkflowInvocation,
    error: BaseException,
) -> StageResult:
    message = str(error) or type(error).__name__
    return StageResult(
        work_id=invocation.workflow_id,
        stage=invocation.workflow_name,
        status=WorkStatus.FAILED,
        failure=FailureInfo(
            code="CHILD_WORKFLOW_FAILED",
            message=message,
            retryable=True,
        ),
        metrics={
            "child_workflow_exception": type(error).__name__,
            "workflow_id": invocation.workflow_id,
        },
    )


def _stops_symbol_fanout(result: StageResult) -> bool:
    if result.status == WorkStatus.CANCELLED:
        return True
    return (
        result.failure is not None
        and result.failure.code == "CHILD_WORKFLOW_FAILED"
    )


def _summary_payload(
    *,
    request: RunRequest,
    workflow_name: str,
    progress: WorkflowProgress,
    stage_results: tuple[StageResult, ...],
    partition: Mapping[str, str] | None = None,
    work_items: tuple[WorkItem, ...] = (),
    include_work_items: bool = False,
) -> dict[str, JSONValue]:
    artifacts = [
        cast(JSONValue, artifact.to_dict())
        for result in stage_results
        for artifact in result.artifacts
    ]
    payload: dict[str, JSONValue] = {
        "request_id": request.request_id,
        "workflow_name": workflow_name,
        "status": progress.status.value,
        "progress": progress.to_dict(),
        "stage_results": [
            cast(JSONValue, result.to_dict()) for result in stage_results
        ],
        "artifacts": artifacts,
    }
    if partition:
        payload["partition"] = cast(JSONValue, dict(partition))
    if include_work_items or work_items:
        payload["work_items"] = cast(
            JSONValue,
            [item.to_dict() for item in work_items],
        )
    return payload


def _invocation(
    request: RunRequest,
    workflow_name: str,
    *,
    partition: Mapping[str, str] | None = None,
    work_items: tuple[WorkItem, ...] = (),
    plan_ref: Mapping[str, JSONValue] | None = None,
) -> WorkflowInvocation:
    spec = WORKFLOW_SPECS_BY_NAME[workflow_name]
    partition_payload = dict(partition or {})
    workflow_id = _workflow_id(request, workflow_name, partition_payload)
    payload: dict[str, JSONValue] = {
        "request": request.to_dict(),
        "workflow_name": workflow_name,
        "workflow_id": workflow_id,
        "stage": OPERATION_ACTIVITIES.get(workflow_name, workflow_name),
        "partition": cast(JSONValue, partition_payload),
        "history_policy": spec.history_policy,
    }
    if work_items:
        payload["work_items"] = cast(
            JSONValue,
            [item.to_dict() for item in work_items],
        )
    if plan_ref:
        payload[DATASET_PLAN_REF_KEY] = cast(JSONValue, dict(plan_ref))
    return WorkflowInvocation(
        workflow_name=workflow_name,
        workflow_id=workflow_id,
        task_queue_lane=spec.lane,
        task_queue=_task_queue_for_lane(request, spec.lane),
        payload=payload,
    )


def _requires_dataset_work(request: RunRequest) -> bool:
    if request.available_remote_data and not request.update_remote_data:
        return _has_data_operations(request)
    return True


def _has_data_operations(request: RunRequest) -> bool:
    return any(
        (
            request.validate_urls,
            request.download_data_archives,
            request.extract_csvs,
            bool(request.api_return_type),
            request.import_to_influxdb,
        )
    )


def _operation_workflow_names(request: RunRequest) -> tuple[str, ...]:
    workflows: list[str] = []
    if request.validate_urls:
        workflows.append("ValidateUrlsWorkflow")
    if request.download_data_archives:
        workflows.append("DownloadArchivesWorkflow")
    if request.extract_csvs:
        workflows.append("ExtractCsvWorkflow")
    if request.api_return_type:
        workflows.append("BuildCacheWorkflow")
        workflows.append("MergeCacheWorkflow")
    if request.import_to_influxdb:
        workflows.append("ImportWorkflow")
    return tuple(workflows)


def _requires_work_items(workflow_name: str) -> bool:
    return workflow_name in {
        SYMBOL_TIMEFRAME_WORKFLOW,
        *SYMBOL_CHILDREN,
    }


def _workflow_id(
    request: RunRequest,
    workflow_name: str,
    partition: Mapping[str, str],
) -> str:
    work_id = derive_work_id(
        request.request_id,
        workflow_name,
        partition.get("pair", ""),
        partition.get("timeframe", ""),
        partition.get("format", ""),
        partition.get("start_yearmonth", ""),
        partition.get("end_yearmonth", ""),
        partition.get("batch_index", ""),
        partition.get("batch_key", ""),
    )
    return (
        f"{request.request_id}-{workflow_name}-{work_id.removeprefix('work-')}"
    )


def _task_queue_for_lane(request: RunRequest, lane: TaskQueueLane) -> str:
    queues = request.metadata.get(TASK_QUEUE_METADATA_KEY)
    if not isinstance(queues, Mapping):
        return ""
    key = _task_queue_metadata_key(lane)
    return str(queues.get(key, "") or "")


def _task_queue_metadata_key(lane: TaskQueueLane) -> str:
    if lane == TaskQueueLane.CPU_FILE:
        return "cpu_file"
    return str(lane.value)


def _stage_result_from_mapping(
    data: Mapping[str, Any],
    *,
    fallback_stage: str,
) -> StageResult:
    if "result" in data:
        return StageResult.from_dict(_coerce_mapping(data.get("result")))
    if "stage_results" in data:
        stage_results = data.get("stage_results") or []
        if isinstance(stage_results, list) and stage_results:
            if "workflow_name" in data and "status" in data:
                return _workflow_summary_stage_result(
                    data,
                    fallback_stage=fallback_stage,
                )
            return StageResult.from_dict(_coerce_mapping(stage_results[-1]))
    if "stage" in data and "status" in data:
        return StageResult.from_dict(data)
    return StageResult(
        work_id=str(data.get("work_id", "")),
        stage=str(data.get("workflow_name", fallback_stage)),
        status=WorkStatus.from_value(data.get("status")),
    )


def _workflow_summary_stage_result(
    data: Mapping[str, Any],
    *,
    fallback_stage: str,
) -> StageResult:
    stage_result_payloads = data.get("stage_results") or []
    stage_results = (
        tuple(
            StageResult.from_dict(_coerce_mapping(stage_result))
            for stage_result in stage_result_payloads
            if isinstance(stage_result, Mapping)
        )
        if isinstance(stage_result_payloads, list)
        else ()
    )
    artifacts = tuple(
        ArtifactRef.from_dict(_coerce_mapping(artifact))
        for artifact in data.get("artifacts", [])
        if isinstance(artifact, Mapping)
    )
    metrics = _workflow_summary_metrics(stage_results)
    metrics.update(
        {
            "child_stage_count": len(stage_results),
            "work_item_count": len(_work_items_from_payload(data)),
        }
    )
    return StageResult(
        work_id=str(data.get("request_id", "")),
        stage=str(data.get("workflow_name", fallback_stage)),
        status=WorkStatus.from_value(data.get("status")),
        artifacts=artifacts,
        failure=next(
            (
                stage_result.failure
                for stage_result in stage_results
                if stage_result.failure is not None
            ),
            None,
        ),
        metrics=metrics,
    )


def _workflow_summary_metrics(
    stage_results: tuple[StageResult, ...],
) -> dict[str, JSONValue]:
    if len(stage_results) != 1:
        return {}
    metrics = stage_results[0].metrics
    return {
        key: metrics[key]
        for key in ("available_data", "filter_pairs", "repo_file_exists")
        if key in metrics
    }


def _payload_with_work_items(
    payload: Mapping[str, JSONValue],
    work_items: tuple[WorkItem, ...],
) -> dict[str, JSONValue]:
    updated = dict(payload)
    updated["work_items"] = cast(
        JSONValue,
        [item.to_dict() for item in work_items],
    )
    return updated


def _plan_ref_from_payload(
    payload: Mapping[str, Any],
) -> dict[str, JSONValue]:
    value = payload.get(DATASET_PLAN_REF_KEY)
    if not isinstance(value, Mapping):
        return {}
    return {
        str(key): cast(JSONValue, item)
        for key, item in value.items()
        if isinstance(key, str)
    }


def _plan_batches_from_payload(
    payload: Mapping[str, Any],
) -> tuple[dict[str, str], ...]:
    value = payload.get(DATASET_PLAN_BATCHES_KEY)
    if not isinstance(value, list):
        return ()
    return tuple(
        _string_mapping(batch) for batch in value if isinstance(batch, Mapping)
    )


def _work_items_from_payload(
    payload: Mapping[str, Any],
) -> tuple[WorkItem, ...]:
    work_item = payload.get("work_item")
    if isinstance(work_item, Mapping):
        return (WorkItem.from_dict(work_item),)

    work_items = payload.get("work_items")
    if isinstance(work_items, list):
        return tuple(
            WorkItem.from_dict(item)
            for item in work_items
            if isinstance(item, Mapping)
        )
    return ()


def _forwarded_work_items(
    payload: Mapping[str, Any],
) -> tuple[WorkItem, ...]:
    work_items = _work_items_from_payload(payload)
    if not work_items:
        return ()

    forward_by_work_id = _forward_flags_by_work_id(payload)
    top_level_forward = payload.get("forward")
    if isinstance(top_level_forward, bool):
        return work_items if top_level_forward else ()

    return tuple(
        item
        for item in work_items
        if forward_by_work_id.get(
            item.work_id,
            _work_item_is_forwardable(item),
        )
    )


def _forward_flags_by_work_id(payload: Mapping[str, Any]) -> dict[str, bool]:
    stage_results = payload.get("stage_results") or []
    if not isinstance(stage_results, list):
        return {}

    flags: dict[str, bool] = {}
    for stage_result in stage_results:
        result = StageResult.from_dict(_coerce_mapping(stage_result))
        forward = result.metrics.get("forward")
        if isinstance(forward, bool) and result.work_id:
            flags[result.work_id] = forward
    return flags


def _work_item_is_forwardable(item: WorkItem) -> bool:
    return item.status not in {
        WorkStatus.URL_NO_REPO_DATA,
        WorkStatus.RETRIED,
        WorkStatus.FAILED,
        WorkStatus.CANCELLED,
        WorkStatus.SKIPPED,
        WorkStatus.COMPLETED,
        WorkStatus.INFLUX_UPLOAD,
    }


def _has_work_item_payload(payload: Mapping[str, Any]) -> bool:
    return "work_item" in payload or "work_items" in payload


def _partition_work_items(
    work_items: tuple[WorkItem, ...],
    partition: Mapping[str, str],
) -> tuple[WorkItem, ...]:
    if not partition:
        return work_items

    pair = str(partition.get("pair", "") or "").lower()
    timeframe = str(partition.get("timeframe", "") or "").lower()
    data_format = str(partition.get("format", "") or "").lower()
    work_ids = _partition_work_ids(partition)
    return tuple(
        item
        for item in work_items
        if (not pair or item.data_fxpair.lower() == pair)
        and (not timeframe or item.data_timeframe.lower() == timeframe)
        and (not data_format or item.data_format.lower() == data_format)
        and (not work_ids or item.work_id in work_ids)
    )


def _partition_matches(
    candidate: Mapping[str, str],
    requested: Mapping[str, str],
) -> bool:
    pair = str(requested.get("pair", "") or "").lower()
    timeframe = str(requested.get("timeframe", "") or "").lower()
    data_format = str(requested.get("format", "") or "").lower()
    return (
        (not pair or str(candidate.get("pair", "")).lower() == pair)
        and (
            not timeframe
            or str(candidate.get("timeframe", "")).lower() == timeframe
        )
        and (
            not data_format
            or str(candidate.get("format", "")).lower() == data_format
        )
    )


def _work_item_batches(
    request: RunRequest,
    work_items: tuple[WorkItem, ...],
    *,
    max_work_items_per_batch: int | None = None,
) -> tuple[tuple[dict[str, str], tuple[WorkItem, ...]], ...]:
    batch_size = (
        max_work_items_per_batch
        if max_work_items_per_batch is not None
        else _max_work_items_per_batch_for_request(request)
    )
    normalized_batch_size = _positive_batch_size(batch_size)
    grouped: dict[tuple[str, str, str], list[WorkItem]] = {}
    for item in sorted(work_items, key=_work_item_sort_key):
        key = (item.data_fxpair, item.data_timeframe, item.data_format)
        grouped.setdefault(key, []).append(item)

    batches: list[tuple[dict[str, str], tuple[WorkItem, ...]]] = []
    for key in sorted(
        grouped,
        key=lambda values: tuple(value.lower() for value in values),
    ):
        items = tuple(grouped[key])
        chunks = tuple(
            items[index : index + normalized_batch_size]
            for index in range(0, len(items), normalized_batch_size)
        )
        for batch_index, chunk in enumerate(chunks, start=1):
            partition = _batch_partition(
                key,
                chunk,
                batch_index=batch_index,
                batch_count=len(chunks),
            )
            batches.append((partition, chunk))
    return tuple(batches)


def _max_work_items_per_batch_for_request(request: RunRequest) -> int:
    """Internal alias kept short where batching helpers compose."""
    return max_work_items_per_batch(request)


def _batch_partition(
    key: tuple[str, str, str],
    work_items: tuple[WorkItem, ...],
    *,
    batch_index: int,
    batch_count: int,
) -> dict[str, str]:
    pair, timeframe, data_format = key
    periods = tuple(
        dict.fromkeys(
            period
            for period in (_work_item_period(item) for item in work_items)
            if period
        )
    )
    start_yearmonth = periods[0] if periods else ""
    end_yearmonth = periods[-1] if periods else start_yearmonth
    work_ids = tuple(item.work_id for item in work_items)
    batch_key = derive_work_id(
        pair.lower(),
        timeframe.lower(),
        data_format.lower(),
        start_yearmonth,
        end_yearmonth,
        str(batch_index),
        ",".join(work_ids),
    ).removeprefix("work-")
    return {
        "pair": pair,
        "timeframe": timeframe,
        "format": data_format,
        "start_yearmonth": start_yearmonth,
        "end_yearmonth": end_yearmonth,
        "periods": ",".join(periods),
        "batch_index": str(batch_index),
        "batch_count": str(batch_count),
        "batch_key": batch_key,
        "work_item_count": str(len(work_items)),
        "work_ids": ",".join(work_ids),
    }


def _work_item_sort_key(item: WorkItem) -> tuple[str, str, str, str, str]:
    return (
        item.data_fxpair.lower(),
        item.data_timeframe.lower(),
        item.data_format.lower(),
        _work_item_period(item),
        item.work_id,
    )


def _work_item_period(item: WorkItem) -> str:
    if item.data_datemonth:
        return str(item.data_datemonth)
    if item.data_year and item.data_month:
        return f"{item.data_year}{item.data_month}"
    return str(item.data_year or item.data_month)


def _partition_work_ids(partition: Mapping[str, str]) -> set[str]:
    raw_work_ids = str(partition.get("work_ids", "") or "")
    if not raw_work_ids:
        return set()
    return {
        work_id.strip()
        for work_id in raw_work_ids.split(",")
        if work_id.strip()
    }


def _is_period_batch_partition(partition: Mapping[str, str]) -> bool:
    return bool(partition.get("batch_key"))


def _positive_batch_size(value: object) -> int:
    if isinstance(value, bool):
        raise ValueError("max_work_items_per_batch must be a positive integer")
    if isinstance(value, int):
        normalized = value
    elif isinstance(value, str):
        normalized = int(value)
    else:
        normalized = int(str(value))
    if normalized < 1:
        raise ValueError("max_work_items_per_batch must be a positive integer")
    return normalized


def _positive_parallelism(value: object) -> int:
    if isinstance(value, bool):
        raise ValueError(
            "max_parallel_child_workflows must be a positive integer"
        )
    if isinstance(value, int):
        normalized = value
    elif isinstance(value, str):
        normalized = int(value)
    else:
        normalized = int(str(value))
    if normalized < 1:
        raise ValueError(
            "max_parallel_child_workflows must be a positive integer"
        )
    return normalized


def _refresh_progress_plan(
    progress: WorkflowProgress,
    invocations: tuple[WorkflowInvocation, ...],
) -> None:
    progress.total_children = len(invocations)
    progress.planned_children = tuple(
        invocation.workflow_name for invocation in invocations
    )


def _skipped_workflow_result(
    invocation: WorkflowInvocation,
    *,
    reason: str,
) -> StageResult:
    partition = _string_mapping(invocation.payload.get("partition", {}))
    return StageResult(
        work_id=invocation.workflow_id,
        stage=invocation.workflow_name,
        status=WorkStatus.SKIPPED,
        events=(
            StatusEvent(
                status=WorkStatus.SKIPPED,
                stage=invocation.workflow_name,
                message=reason,
                work_id=invocation.workflow_id,
                metadata={
                    "partition": cast(JSONValue, dict(partition)),
                    "task_queue": invocation.task_queue,
                },
            ),
        ),
        metrics={
            "forward": False,
            "work_item_count": 0,
            "skipped_empty_work_items": True,
        },
    )


def _coerce_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _latest_event_timestamp(events: tuple[StatusEvent, ...]) -> str:
    for event in reversed(events):
        if event.timestamp_utc:
            return str(event.timestamp_utc)
        updated_at = event.metadata.get("updated_at_utc")
        if updated_at:
            return str(updated_at)
    return ""


def _last_event_error(events: tuple[StatusEvent, ...]) -> str:
    for event in reversed(events):
        value = event.metadata.get("last_error")
        if value:
            return str(value)
        if event.status == WorkStatus.FAILED and event.message:
            return str(event.message)
    return ""


def _string_mapping(value: Any) -> dict[str, str]:
    return {key: str(item) for key, item in _coerce_mapping(value).items()}
