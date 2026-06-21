"""Temporal workflow topology for HistData sidecar jobs.

The topology is intentionally coarse-grained so workflow histories carry
request metadata, partition identifiers, status events, and artifact
references instead of downloaded rows, dataframes, or queue payloads.
"""

from __future__ import annotations

from datetime import timedelta
from dataclasses import dataclass
from importlib import import_module
from typing import Any, Callable, Mapping, Protocol, TypeVar, cast

from histdatacom.runtime_contracts import (
    ArtifactRef,
    JSONValue,
    RunRequest,
    StageResult,
    StatusEvent,
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
        self.planned_children = planned_children
        self.completed_stages = ()
        self.events = ()
        self.artifacts = ()

    def record_child(self, stage: str, result: StageResult) -> None:
        """Record one completed child or activity result."""
        self.current_stage = stage
        self.completed_children += 1
        self.completed_stages = (*self.completed_stages, stage)
        self.events = (*self.events, *result.events)
        self.artifacts = (*self.artifacts, *result.artifacts)
        if result.failure is not None:
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
            "planned_children": list(self.planned_children),
            "completed_stages": list(self.completed_stages),
            "events": [event.to_dict() for event in self.events],
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
        }


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


class PendingActivityExecutor:
    """Placeholder activity executor until Temporal activity wiring exists."""

    async def execute_activity(
        self,
        activity_name: str,
        payload: Mapping[str, JSONValue],
        *,
        task_queue: str,
    ) -> Mapping[str, Any]:
        """Return a planned result without executing legacy queue code."""
        stage = str(payload.get("stage", activity_name))
        result = StageResult(
            work_id=str(payload.get("work_id", "")),
            stage=stage,
            status=WorkStatus.PLANNED,
            events=(
                StatusEvent(
                    status=WorkStatus.PLANNED,
                    stage=stage,
                    message=(
                        "Temporal activity wiring is pending; queue-free "
                        "stage functions are available for implementation."
                    ),
                    metadata={
                        "activity": activity_name,
                        "task_queue": task_queue,
                    },
                ),
            ),
            metrics={"activity_pending": True},
        )
        return dict(result.to_dict())


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
        options: dict[str, Any] = {
            "start_to_close_timeout": timedelta(minutes=30),
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
    "DatasetPlanWorkflow": "dataset_plan",
    "ValidateUrlsWorkflow": "validate_urls",
    "DownloadArchivesWorkflow": "download_archives",
    "ExtractCsvWorkflow": "extract_csv",
    "BuildCacheWorkflow": "build_cache",
    "MergeCacheWorkflow": "merge_cache",
    "ImportWorkflow": "import_to_influx",
}


def workflow_topology_document() -> dict[str, JSONValue]:
    """Return machine-readable workflow topology documentation."""
    return {
        "schema_version": TOPOLOGY_SCHEMA_VERSION,
        "metadata_keys": {
            "task_queues": TASK_QUEUE_METADATA_KEY,
            "topology_version": TOPOLOGY_METADATA_KEY,
        },
        "history_policy": (
            "Workflow histories carry bounded metadata only. Stage outputs "
            "must return StageResult and ArtifactRef payloads rather than "
            "rows, dataframes, or archive bytes."
        ),
        "workflows": [spec.to_dict() for spec in WORKFLOW_TOPOLOGY],
    }


def workflow_names() -> tuple[str, ...]:
    """Return all registered workflow class names in topology order."""
    return tuple(spec.name for spec in WORKFLOW_TOPOLOGY)


def build_run_child_invocations(
    request: RunRequest,
) -> tuple[WorkflowInvocation, ...]:
    """Plan top-level child workflow calls for a user-visible job."""
    invocations: list[WorkflowInvocation] = []
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
) -> tuple[WorkflowInvocation, ...]:
    """Plan operation-family child workflow calls for one partition."""
    invocations: list[WorkflowInvocation] = []
    for workflow_name in _operation_workflow_names(request):
        invocations.append(
            _invocation(request, workflow_name, partition=partition)
        )
    return tuple(invocations)


def request_partitions(request: RunRequest) -> tuple[dict[str, str], ...]:
    """Return coarse symbol/timeframe partitions for child workflows."""
    pairs = request.pairs or ("requested-pairs",)
    timeframes = request.timeframes or ("requested-timeframes",)
    return tuple(
        {"pair": pair, "timeframe": timeframe}
        for pair in pairs
        for timeframe in timeframes
    )


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
    *,
    executor: ChildWorkflowExecutor,
    progress: WorkflowProgress,
) -> dict[str, JSONValue]:
    """Execute operation children for one symbol/timeframe partition."""
    invocations = build_symbol_child_invocations(request, partition)
    return await _execute_child_plan(
        request,
        workflow_name="SymbolTimeframeWorkflow",
        invocations=invocations,
        executor=executor,
        progress=progress,
        partition=partition,
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
    result = _stage_result_from_mapping(
        await activity_executor.execute_activity(
            activity_name,
            stage_payload,
            task_queue=task_queue,
        ),
        fallback_stage=activity_name,
    )
    progress.record_child(activity_name, result)
    progress.finish(result.status)
    return _summary_payload(
        request=request,
        workflow_name=workflow_name,
        progress=progress,
        stage_results=(result,),
        partition=_coerce_mapping(payload.get("partition", {})),
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
    async def run(self, request_payload: Mapping[str, JSONValue]) -> dict:
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
    async def run(self, payload: Mapping[str, JSONValue]) -> dict:
        """Run operation-family children for one partition."""
        request = RunRequest.from_dict(
            _coerce_mapping(payload.get("request", {}))
        )
        partition = _string_mapping(payload.get("partition", {}))
        return await execute_symbol_timeframe_workflow(
            request,
            partition,
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

    async def _run_activity(self, payload: Mapping[str, JSONValue]) -> dict:
        return await execute_activity_workflow(
            self.workflow_name,
            payload,
            activity_executor=(
                self._activity_executor or PendingActivityExecutor()
            ),
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
    async def run(self, payload: Mapping[str, JSONValue]) -> dict:
        """Run repository refresh as a real activity."""
        return await execute_activity_workflow(
            self.workflow_name,
            payload,
            activity_executor=(
                self._activity_executor or TemporalActivityExecutor()
            ),
            progress=self._progress,
        )

    status = workflow_query(_ActivityWorkflowBase.status)


@workflow_defn
class DatasetPlanWorkflow(_ActivityWorkflowBase):
    """Child workflow for bounded dataset planning."""

    workflow_name = "DatasetPlanWorkflow"

    @workflow_run
    async def run(self, payload: Mapping[str, JSONValue]) -> dict:
        """Run dataset planning as a real activity."""
        return await execute_activity_workflow(
            self.workflow_name,
            payload,
            activity_executor=(
                self._activity_executor or TemporalActivityExecutor()
            ),
            progress=self._progress,
        )

    status = workflow_query(_ActivityWorkflowBase.status)


@workflow_defn
class ValidateUrlsWorkflow(_ActivityWorkflowBase):
    """Child workflow for URL validation."""

    workflow_name = "ValidateUrlsWorkflow"

    @workflow_run
    async def run(self, payload: Mapping[str, JSONValue]) -> dict:
        """Run URL validation as a real activity."""
        return await execute_activity_workflow(
            self.workflow_name,
            payload,
            activity_executor=(
                self._activity_executor or TemporalActivityExecutor()
            ),
            progress=self._progress,
        )

    status = workflow_query(_ActivityWorkflowBase.status)


@workflow_defn
class DownloadArchivesWorkflow(_ActivityWorkflowBase):
    """Child workflow for archive downloads."""

    workflow_name = "DownloadArchivesWorkflow"

    @workflow_run
    async def run(self, payload: Mapping[str, JSONValue]) -> dict:
        """Run archive download as a real activity."""
        return await execute_activity_workflow(
            self.workflow_name,
            payload,
            activity_executor=(
                self._activity_executor or TemporalActivityExecutor()
            ),
            progress=self._progress,
        )

    status = workflow_query(_ActivityWorkflowBase.status)


@workflow_defn
class ExtractCsvWorkflow(_ActivityWorkflowBase):
    """Child workflow for CSV extraction."""

    workflow_name = "ExtractCsvWorkflow"

    @workflow_run
    async def run(self, payload: Mapping[str, JSONValue]) -> dict:
        """Run CSV extraction as a real activity."""
        return await execute_activity_workflow(
            self.workflow_name,
            payload,
            activity_executor=(
                self._activity_executor or TemporalActivityExecutor()
            ),
            progress=self._progress,
        )

    status = workflow_query(_ActivityWorkflowBase.status)


@workflow_defn
class BuildCacheWorkflow(_ActivityWorkflowBase):
    """Child workflow for cache building."""

    workflow_name = "BuildCacheWorkflow"

    @workflow_run
    async def run(self, payload: Mapping[str, JSONValue]) -> dict:
        """Run cache build as a real activity."""
        return await execute_activity_workflow(
            self.workflow_name,
            payload,
            activity_executor=(
                self._activity_executor or TemporalActivityExecutor()
            ),
            progress=self._progress,
        )

    status = workflow_query(_ActivityWorkflowBase.status)


@workflow_defn
class MergeCacheWorkflow(_ActivityWorkflowBase):
    """Child workflow for cache merging."""

    workflow_name = "MergeCacheWorkflow"

    @workflow_run
    async def run(self, payload: Mapping[str, JSONValue]) -> dict:
        """Run cache merge as a real activity."""
        return await execute_activity_workflow(
            self.workflow_name,
            payload,
            activity_executor=(
                self._activity_executor or TemporalActivityExecutor()
            ),
            progress=self._progress,
        )

    status = workflow_query(_ActivityWorkflowBase.status)


@workflow_defn
class ImportWorkflow(_ActivityWorkflowBase):
    """Child workflow for Influx import."""

    workflow_name = "ImportWorkflow"

    @workflow_run
    async def run(self, payload: Mapping[str, JSONValue]) -> dict:
        """Run Influx import activity placeholder."""
        return await self._run_activity(payload)

    status = workflow_query(_ActivityWorkflowBase.status)


DEFAULT_WORKFLOWS = (
    HistDataRunWorkflow,
    RepositoryRefreshWorkflow,
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
) -> dict[str, JSONValue]:
    progress.start(
        request_id=request.request_id,
        planned_children=tuple(
            invocation.workflow_name for invocation in invocations
        ),
    )
    results: list[StageResult] = []
    for invocation in invocations:
        result = _stage_result_from_mapping(
            await executor.execute_child_workflow(
                invocation.workflow_name,
                invocation.payload,
                workflow_id=invocation.workflow_id,
                task_queue=invocation.task_queue,
            ),
            fallback_stage=invocation.workflow_name,
        )
        results.append(result)
        progress.record_child(invocation.workflow_name, result)
    progress.finish(_execute_status(tuple(results)))
    return _summary_payload(
        request=request,
        workflow_name=workflow_name,
        progress=progress,
        stage_results=tuple(results),
        partition=partition,
    )


def _summary_payload(
    *,
    request: RunRequest,
    workflow_name: str,
    progress: WorkflowProgress,
    stage_results: tuple[StageResult, ...],
    partition: Mapping[str, str] | None = None,
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
    return payload


def _invocation(
    request: RunRequest,
    workflow_name: str,
    *,
    partition: Mapping[str, str] | None = None,
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
            return StageResult.from_dict(_coerce_mapping(stage_results[-1]))
    if "stage" in data and "status" in data:
        return StageResult.from_dict(data)
    return StageResult(
        work_id=str(data.get("work_id", "")),
        stage=str(data.get("workflow_name", fallback_stage)),
        status=WorkStatus.from_value(data.get("status")),
    )


def _coerce_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _string_mapping(value: Any) -> dict[str, str]:
    return {key: str(item) for key, item in _coerce_mapping(value).items()}
