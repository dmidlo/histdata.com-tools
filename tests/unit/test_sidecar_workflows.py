"""Tests for Temporal sidecar workflow topology."""

from __future__ import annotations

import asyncio
import json
from typing import Any, Mapping

import pytest

from histdatacom.exceptions import RetryPolicyName
from histdatacom.manifest_store import (
    DATASET_PLAN_BATCHES_KEY,
    DATASET_PLAN_REF_KEY,
)
from histdatacom.runtime_contracts import (
    ArtifactRef,
    FailureInfo,
    JSONValue,
    RunRequest,
    StageResult,
    WorkItem,
    WorkStatus,
)
from histdatacom.sidecar import workflows
from histdatacom.sidecar.workflow_metadata import TASK_QUEUE_METADATA_KEY


class _RecordingChildExecutor:
    """Fake child workflow executor used by composition tests."""

    def __init__(
        self,
        work_items: tuple[WorkItem, ...] | None = None,
    ) -> None:
        self.calls: list[dict[str, object]] = []
        self.work_items = work_items or _planned_work_items()

    async def execute_child_workflow(
        self,
        workflow_name: str,
        payload: Mapping[str, JSONValue],
        *,
        workflow_id: str,
        task_queue: str,
    ) -> Mapping[str, object]:
        """Record child workflow calls and return a completed stage."""
        self.calls.append(
            {
                "workflow_name": workflow_name,
                "payload": dict(payload),
                "workflow_id": workflow_id,
                "task_queue": task_queue,
            }
        )
        if workflow_name == "DatasetPlanWorkflow":
            return {
                "result": StageResult(
                    work_id=workflow_id,
                    stage="dataset_plan",
                    status=WorkStatus.COMPLETED,
                    artifacts=(
                        ArtifactRef(
                            kind="manifest",
                            path=f"{workflow_id}.json",
                        ),
                    ),
                ).to_dict(),
                "work_items": [item.to_dict() for item in self.work_items],
            }
        return StageResult(
            work_id=workflow_id,
            stage=workflow_name,
            status=WorkStatus.COMPLETED,
            artifacts=(
                ArtifactRef(
                    kind="manifest",
                    path=f"{workflow_id}.json",
                ),
            ),
        ).to_dict()


class _PlanReferenceChildExecutor:
    """Fake executor that spills dataset plan work items by reference."""

    def __init__(self, work_items: tuple[WorkItem, ...]) -> None:
        self.calls: list[dict[str, object]] = []
        self.work_items = work_items

    async def execute_child_workflow(
        self,
        workflow_name: str,
        payload: Mapping[str, JSONValue],
        *,
        workflow_id: str,
        task_queue: str,
    ) -> Mapping[str, object]:
        """Record child workflow calls and return compact plan metadata."""
        self.calls.append(
            {
                "workflow_name": workflow_name,
                "payload": dict(payload),
                "workflow_id": workflow_id,
                "task_queue": task_queue,
            }
        )
        if workflow_name == "DatasetPlanWorkflow":
            request = RunRequest.from_dict(dict(payload["request"]))
            return {
                "result": StageResult(
                    work_id=workflow_id,
                    stage="dataset_plan",
                    status=WorkStatus.COMPLETED,
                    metrics={
                        "work_item_count": len(self.work_items),
                        "work_items_spilled": True,
                    },
                ).to_dict(),
                DATASET_PLAN_REF_KEY: {
                    "kind": "dataset_plan",
                    "plan_id": "plan-spilled",
                    "store_root": "/tmp/histdatacom-plan",
                    "store_path": "/tmp/histdatacom-plan/.histdatacom/db",
                    "work_item_count": len(self.work_items),
                },
                DATASET_PLAN_BATCHES_KEY: [
                    partition
                    for partition in workflows.period_batch_partitions(
                        request,
                        self.work_items,
                    )
                ],
            }

        return StageResult(
            work_id=workflow_id,
            stage=workflow_name,
            status=WorkStatus.COMPLETED,
        ).to_dict()


class _BoundedFanoutChildExecutor:
    """Fake child executor that tracks concurrent symbol batch execution."""

    def __init__(
        self,
        work_items: tuple[WorkItem, ...],
        *,
        cancel_at_symbol_call: int | None = None,
        raise_at_symbol_call: int | None = None,
    ) -> None:
        self.calls: list[dict[str, object]] = []
        self.work_items = work_items
        self.cancel_at_symbol_call = cancel_at_symbol_call
        self.raise_at_symbol_call = raise_at_symbol_call
        self.active_symbol_children = 0
        self.max_active_symbol_children = 0
        self.symbol_call_count = 0

    async def execute_child_workflow(
        self,
        workflow_name: str,
        payload: Mapping[str, JSONValue],
        *,
        workflow_id: str,
        task_queue: str,
    ) -> Mapping[str, object]:
        """Record child calls and keep symbol children alive for one tick."""
        self.calls.append(
            {
                "workflow_name": workflow_name,
                "payload": dict(payload),
                "workflow_id": workflow_id,
                "task_queue": task_queue,
            }
        )
        if workflow_name == "DatasetPlanWorkflow":
            return {
                "result": StageResult(
                    work_id=workflow_id,
                    stage="dataset_plan",
                    status=WorkStatus.COMPLETED,
                ).to_dict(),
                "work_items": [item.to_dict() for item in self.work_items],
            }

        self.symbol_call_count += 1
        call_number = self.symbol_call_count
        self.active_symbol_children += 1
        self.max_active_symbol_children = max(
            self.max_active_symbol_children,
            self.active_symbol_children,
        )
        try:
            await asyncio.sleep(0.01)
        finally:
            self.active_symbol_children -= 1

        if call_number == self.raise_at_symbol_call:
            raise RuntimeError("child workflow exploded")

        if call_number == self.cancel_at_symbol_call:
            return StageResult(
                work_id=workflow_id,
                stage=workflow_name,
                status=WorkStatus.CANCELLED,
                failure=FailureInfo(
                    code="OPERATION_CANCELLED",
                    message="operator cancelled",
                    retryable=False,
                ),
            ).to_dict()
        return StageResult(
            work_id=workflow_id,
            stage=workflow_name,
            status=WorkStatus.COMPLETED,
        ).to_dict()


class _RepositoryMetricsChildExecutor:
    """Fake child executor that returns repository stage metrics."""

    async def execute_child_workflow(
        self,
        workflow_name: str,
        payload: Mapping[str, JSONValue],
        *,
        workflow_id: str,
        task_queue: str,
    ) -> Mapping[str, object]:
        """Return a child workflow summary with available repository data."""
        return {
            "workflow_name": workflow_name,
            "request_id": "run-topology",
            "status": WorkStatus.COMPLETED.value,
            "stage_results": [
                StageResult(
                    work_id=workflow_id,
                    stage="repository_refresh",
                    status=WorkStatus.COMPLETED,
                    metrics={
                        "available_data": {
                            "eurusd": {
                                "start": "200005",
                                "end": "202212",
                            }
                        }
                    },
                ).to_dict()
            ],
            "artifacts": [],
        }


class _CancellingChildExecutor:
    """Fake child workflow executor that cancels the first child."""

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def execute_child_workflow(
        self,
        workflow_name: str,
        payload: Mapping[str, JSONValue],
        *,
        workflow_id: str,
        task_queue: str,
    ) -> Mapping[str, object]:
        """Record child workflow calls and return a cancelled stage."""
        self.calls.append(
            {
                "workflow_name": workflow_name,
                "payload": dict(payload),
                "workflow_id": workflow_id,
                "task_queue": task_queue,
            }
        )
        return StageResult(
            work_id=workflow_id,
            stage=workflow_name,
            status=WorkStatus.CANCELLED,
            failure=FailureInfo(
                code="OPERATION_CANCELLED",
                message="operator cancelled",
                retryable=False,
            ),
        ).to_dict()


class _NoForwardChildExecutor:
    """Fake child executor that marks the first stage non-forwardable."""

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def execute_child_workflow(
        self,
        workflow_name: str,
        payload: Mapping[str, JSONValue],
        *,
        workflow_id: str,
        task_queue: str,
    ) -> Mapping[str, object]:
        """Record child calls and return no forwardable work after validate."""
        self.calls.append(
            {
                "workflow_name": workflow_name,
                "payload": dict(payload),
                "workflow_id": workflow_id,
                "task_queue": task_queue,
            }
        )
        [work_item] = _work_items_from_payload(payload)
        blocked = work_item.with_status(WorkStatus.URL_NO_REPO_DATA)
        return {
            "work_items": [blocked.to_dict()],
            "stage_results": [
                StageResult(
                    work_id=blocked.work_id,
                    stage="validate_urls",
                    status=WorkStatus.URL_NO_REPO_DATA,
                    metrics={"forward": False},
                ).to_dict()
            ],
            "result": StageResult(
                work_id="",
                stage="validate_urls",
                status=WorkStatus.COMPLETED,
                metrics={"forward_count": 0},
            ).to_dict(),
        }


class _FailingChildExecutor:
    """Fake child executor that fails the first operation stage."""

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def execute_child_workflow(
        self,
        workflow_name: str,
        payload: Mapping[str, JSONValue],
        *,
        workflow_id: str,
        task_queue: str,
    ) -> Mapping[str, object]:
        """Record child calls and return a failed non-forwarded item."""
        self.calls.append(
            {
                "workflow_name": workflow_name,
                "payload": dict(payload),
                "workflow_id": workflow_id,
                "task_queue": task_queue,
            }
        )
        [work_item] = _work_items_from_payload(payload)
        failed = work_item.with_status(WorkStatus.FAILED)
        failure = FailureInfo(
            code="VALIDATION_FAILED",
            message="validation failed",
            retryable=False,
        )
        return {
            "work_items": [failed.to_dict()],
            "stage_results": [
                StageResult(
                    work_id=failed.work_id,
                    stage="validate_urls",
                    status=WorkStatus.FAILED,
                    failure=failure,
                    metrics={"forward": False},
                ).to_dict()
            ],
            "result": StageResult(
                work_id="",
                stage="validate_urls",
                status=WorkStatus.FAILED,
                failure=failure,
                metrics={"forward_count": 0},
            ).to_dict(),
        }


class _RecordingActivityExecutor:
    """Fake activity executor used by leaf workflow tests."""

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def execute_activity(
        self,
        activity_name: str,
        payload: Mapping[str, JSONValue],
        *,
        task_queue: str,
    ) -> Mapping[str, object]:
        """Record activity calls and return a completed stage."""
        self.calls.append(
            {
                "activity_name": activity_name,
                "payload": dict(payload),
                "task_queue": task_queue,
            }
        )
        return StageResult(
            work_id=str(payload.get("workflow_id", "")),
            stage=activity_name,
            status=WorkStatus.COMPLETED,
            artifacts=(
                ArtifactRef(
                    kind="activity-result",
                    path=f"{activity_name}.json",
                ),
            ),
        ).to_dict()


class _ThreadingChildExecutor:
    """Fake executor that threads work items through a full workflow chain."""

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def execute_child_workflow(
        self,
        workflow_name: str,
        payload: Mapping[str, JSONValue],
        *,
        workflow_id: str,
        task_queue: str,
    ) -> Mapping[str, object]:
        """Run nested fake children while recording every call payload."""
        self.calls.append(
            {
                "workflow_name": workflow_name,
                "payload": dict(payload),
                "workflow_id": workflow_id,
                "task_queue": task_queue,
            }
        )
        if workflow_name == "DatasetPlanWorkflow":
            return {
                "workflow_name": workflow_name,
                "request_id": "run-topology",
                "status": WorkStatus.COMPLETED.value,
                "stage_results": [
                    StageResult(
                        work_id=workflow_id,
                        stage="dataset_plan",
                        status=WorkStatus.COMPLETED,
                    ).to_dict()
                ],
                "work_items": [
                    item.to_dict() for item in _planned_work_items()
                ],
                "artifacts": [],
            }
        if workflow_name == "SymbolTimeframeWorkflow":
            workflow = workflows.SymbolTimeframeWorkflow(executor=self)
            return await workflow.run(payload)
        return _operation_payload(workflow_name, payload)


def _planned_work_items() -> tuple[WorkItem, ...]:
    return (
        _work_item("EURUSD", "M1", "2022-01"),
        _work_item("GBPUSD", "M1", "2022-01"),
    )


def _multi_period_work_items(
    pair: str = "EURUSD",
    *,
    timeframe: str = "M1",
    data_format: str = "ascii",
    count: int = 5,
) -> tuple[WorkItem, ...]:
    return tuple(
        _work_item(
            pair,
            timeframe,
            f"2022-{month:02d}",
            data_format=data_format,
        )
        for month in range(1, count + 1)
    )


def _work_item(
    pair: str,
    timeframe: str,
    datemonth: str,
    *,
    data_format: str = "ascii",
    status: WorkStatus = WorkStatus.URL_NEW,
) -> WorkItem:
    pair_lower = pair.lower()
    return WorkItem(
        work_id=f"work-{pair_lower}-{timeframe.lower()}-{datemonth}",
        status=status,
        url=f"https://example.test/{pair_lower}/{timeframe}/{datemonth}",
        data_format=data_format,
        data_timeframe=timeframe,
        data_fxpair=pair,
        data_datemonth=datemonth,
        data_dir=f"/tmp/{pair_lower}",
    )


def _operation_payload(
    workflow_name: str,
    payload: Mapping[str, JSONValue],
) -> Mapping[str, object]:
    raw_work_items = payload.get("work_items", [])
    work_items = [
        WorkItem.from_dict(item)
        for item in (raw_work_items if isinstance(raw_work_items, list) else [])
        if isinstance(item, Mapping)
    ]
    stage_status = {
        "ValidateUrlsWorkflow": WorkStatus.URL_VALID,
        "DownloadArchivesWorkflow": WorkStatus.CSV_ZIP,
        "ExtractCsvWorkflow": WorkStatus.CSV_FILE,
        "BuildCacheWorkflow": WorkStatus.CACHE_READY,
        "ImportWorkflow": WorkStatus.INFLUX_UPLOAD,
    }
    if workflow_name == "MergeCacheWorkflow":
        return {
            "workflow_name": workflow_name,
            "request_id": "run-topology",
            "status": WorkStatus.COMPLETED.value,
            "stage_results": [
                StageResult(
                    work_id="merge-work",
                    stage="merge_cache",
                    status=WorkStatus.COMPLETED,
                    metrics={"work_item_count": len(work_items)},
                ).to_dict()
            ],
            "artifacts": [],
        }

    next_status = stage_status[workflow_name]
    forwarded_items = tuple(
        _with_stage_metadata(item, next_status) for item in work_items
    )
    return {
        "workflow_name": workflow_name,
        "request_id": "run-topology",
        "status": WorkStatus.COMPLETED.value,
        "stage_results": [
            StageResult(
                work_id=item.work_id,
                stage=str(payload.get("stage", workflow_name)),
                status=next_status,
                metrics={
                    "forward": next_status is not WorkStatus.INFLUX_UPLOAD
                },
            ).to_dict()
            for item in forwarded_items
        ],
        "work_items": [item.to_dict() for item in forwarded_items],
        "artifacts": [],
    }


def _with_stage_metadata(item: WorkItem, status: WorkStatus) -> WorkItem:
    if status == WorkStatus.CSV_ZIP:
        return item.with_status(status)
    if status == WorkStatus.CSV_FILE:
        return item.with_status(status)
    if status == WorkStatus.CACHE_READY:
        return WorkItem.from_dict(
            {
                **item.to_dict(),
                "status": status.value,
                "cache_filename": "data.parquet",
                "cache_line_count": "3",
                "cache_start": "2022-01-01T00:00:00Z",
                "cache_end": "2022-01-01T00:02:00Z",
            }
        )
    return item.with_status(status)


def _payload_work_items(call: Mapping[str, object]) -> list[Mapping[str, str]]:
    payload = call["payload"]
    assert isinstance(payload, Mapping)
    raw_items = payload.get("work_items", [])
    assert isinstance(raw_items, list)
    return [item for item in raw_items if isinstance(item, Mapping)]


def _work_items_from_payload(
    payload: Mapping[str, JSONValue],
) -> tuple[WorkItem, ...]:
    raw_items = payload.get("work_items", [])
    assert isinstance(raw_items, list)
    return tuple(
        WorkItem.from_dict(item)
        for item in raw_items
        if isinstance(item, Mapping)
    )


def _statuses_for_operation(
    calls: list[dict[str, object]],
    workflow_name: str,
) -> set[str]:
    return {
        str(item["status"])
        for call in calls
        if call["workflow_name"] == workflow_name
        for item in _payload_work_items(call)
    }


def _request(**overrides: object) -> RunRequest:
    values = {
        "request_id": "run-topology",
        "pairs": ("EURUSD", "GBPUSD"),
        "formats": ("ascii",),
        "timeframes": ("M1",),
        "validate_urls": True,
        "download_data_archives": True,
        "extract_csvs": True,
        "api_return_type": "polars",
        "import_to_influxdb": True,
        "metadata": {
            TASK_QUEUE_METADATA_KEY: {
                "orchestration": "queue-orchestration",
                "network": "queue-network",
                "cpu_file": "queue-cpu-file",
                "influx": "queue-influx",
            }
        },
    }
    values.update(overrides)
    return RunRequest(**values)


def test_workflow_topology_documents_expected_hierarchy() -> None:
    """Topology docs should list the parent and child workflow hierarchy."""
    document = workflows.workflow_topology_document()
    specs = {item["name"]: item for item in document["workflows"]}
    activity_policies = {
        item["activity_name"]: item for item in document["activity_policies"]
    }

    assert document["schema_version"] == 1
    assert document["metadata_keys"]["fanout"] == workflows.FANOUT_METADATA_KEY
    assert (
        document["metadata_keys"]["max_parallel_child_workflows"]
        == workflows.MAX_PARALLEL_CHILD_WORKFLOWS_METADATA_KEY
    )
    assert document["metadata_keys"]["dataset_plan_ref"] == DATASET_PLAN_REF_KEY
    assert (
        document["metadata_keys"]["dataset_plan_batches"]
        == DATASET_PLAN_BATCHES_KEY
    )
    assert set(workflows.workflow_names()) == {
        "HistDataRunWorkflow",
        "RepositoryRefreshWorkflow",
        "DataQualityWorkflow",
        "DatasetPlanWorkflow",
        "SymbolTimeframeWorkflow",
        "ValidateUrlsWorkflow",
        "DownloadArchivesWorkflow",
        "ExtractCsvWorkflow",
        "BuildCacheWorkflow",
        "MergeCacheWorkflow",
        "ImportWorkflow",
    }
    assert specs["HistDataRunWorkflow"]["children"] == [
        "RepositoryRefreshWorkflow",
        "DataQualityWorkflow",
        "DatasetPlanWorkflow",
        "SymbolTimeframeWorkflow",
    ]
    assert specs["SymbolTimeframeWorkflow"]["children"] == [
        "ValidateUrlsWorkflow",
        "DownloadArchivesWorkflow",
        "ExtractCsvWorkflow",
        "BuildCacheWorkflow",
        "MergeCacheWorkflow",
        "ImportWorkflow",
    ]
    assert "rows" in str(document["history_policy"])
    assert set(activity_policies) == set(
        workflows.OPERATION_ACTIVITIES.values()
    )
    assert (
        activity_policies["validate_urls"]["retry_policy"]["name"]
        == RetryPolicyName.NETWORK.value
    )
    assert (
        activity_policies["import_to_influx"]["retry_policy"]["name"]
        == RetryPolicyName.IDEMPOTENT_WRITE.value
    )
    assert (
        activity_policies["dataset_plan"]["retry_policy"]["name"]
        == RetryPolicyName.NONE.value
    )
    assert (
        activity_policies["data_quality"]["retry_policy"]["name"]
        == RetryPolicyName.NONE.value
    )
    assert (
        activity_policies["download_archives"]["heartbeat_timeout_seconds"]
        == 60
    )


def test_activity_execution_policy_rejects_unknown_activity() -> None:
    """Activity policy lookups should fail loudly for missing metadata."""
    with pytest.raises(ValueError, match="unknown activity policy"):
        workflows.activity_execution_policy("unknown_activity")


def test_config_or_local_only_activity_policies_do_not_retry() -> None:
    """Config/local validation stages should not receive Temporal retries."""
    for activity_name in (
        "dataset_plan",
        "data_quality",
        "extract_csv",
        "build_cache",
    ):
        policy = workflows.activity_execution_policy(activity_name)
        assert policy.retry_policy.name is RetryPolicyName.NONE
        assert policy.retry_policy.maximum_attempts == 1


def test_temporal_activity_executor_passes_stage_policy_options(
    monkeypatch,
) -> None:
    """Temporal activity calls should receive timeout, heartbeat, and retry options."""
    captured: dict[str, Any] = {}

    async def execute_activity(
        activity_name: str,
        payload: Mapping[str, JSONValue],
        **options: Any,
    ) -> Mapping[str, object]:
        captured["activity_name"] = activity_name
        captured["payload"] = dict(payload)
        captured["options"] = dict(options)
        return StageResult(
            work_id="download-work",
            stage="download_archives",
            status=WorkStatus.COMPLETED,
        ).to_dict()

    monkeypatch.setattr(
        workflows.workflow,
        "execute_activity",
        execute_activity,
    )

    result = asyncio.run(
        workflows.TemporalActivityExecutor().execute_activity(
            "download_archives",
            {"stage": "download_archives"},
            task_queue="queue-network",
        )
    )

    options = captured["options"]
    assert isinstance(options, dict)
    assert captured["activity_name"] == "download_archives"
    assert options["task_queue"] == "queue-network"
    assert options["start_to_close_timeout"].total_seconds() == 3600
    assert options["heartbeat_timeout"].total_seconds() == 60
    retry_policy = options["retry_policy"]
    assert retry_policy.maximum_attempts == 5
    assert retry_policy.initial_interval.total_seconds() == 2.0
    assert result["status"] == WorkStatus.COMPLETED.value


def test_repository_only_request_only_plans_repository_refresh() -> None:
    """A repo metadata request should not fan out dataset partitions."""
    request = _request(
        available_remote_data=True,
        pairs=(),
        timeframes=(),
        validate_urls=False,
        download_data_archives=False,
        extract_csvs=False,
        api_return_type="",
        import_to_influxdb=False,
    )

    invocations = workflows.build_run_child_invocations(request)

    assert [item.workflow_name for item in invocations] == [
        "RepositoryRefreshWorkflow"
    ]


def test_data_quality_request_only_plans_quality_workflow() -> None:
    """A quality request should stay offline and skip dataset fan-out."""
    request = _request(
        data_quality=True,
        quality_paths=("/tmp/histdata",),
        quality_check_groups=("inventory",),
        available_remote_data=True,
        update_remote_data=True,
    )

    invocations = workflows.build_run_child_invocations(request)

    assert [item.workflow_name for item in invocations] == [
        "DataQualityWorkflow"
    ]
    assert invocations[0].task_queue_lane.value == "cpu-file"
    assert invocations[0].payload["request"]["data_quality"] is True


def test_parent_workflow_preserves_repository_available_data_metrics() -> None:
    """Parent results should retain bounded repo data for API parity."""
    request = _request(
        available_remote_data=True,
        pairs=("eurusd",),
        timeframes=(),
        validate_urls=False,
        download_data_archives=False,
        extract_csvs=False,
        api_return_type="",
        import_to_influxdb=False,
    )
    workflow = workflows.HistDataRunWorkflow(
        executor=_RepositoryMetricsChildExecutor()
    )

    summary = asyncio.run(workflow.run(request.to_dict()))

    stage_result = summary["stage_results"][0]
    metrics = stage_result["metrics"]
    assert metrics["available_data"] == {
        "eurusd": {"start": "200005", "end": "202212"}
    }
    assert metrics["child_stage_count"] == 1


def test_parent_workflow_composes_symbol_timeframe_children() -> None:
    """The parent workflow should fan out to coarse partition workflows."""
    executor = _RecordingChildExecutor()
    workflow = workflows.HistDataRunWorkflow(executor=executor)
    request = _request()

    summary = asyncio.run(workflow.run(request.to_dict()))

    assert [call["workflow_name"] for call in executor.calls] == [
        "DatasetPlanWorkflow",
        "SymbolTimeframeWorkflow",
        "SymbolTimeframeWorkflow",
    ]
    assert executor.calls[0]["task_queue"] == "queue-cpu-file"
    assert executor.calls[1]["task_queue"] == "queue-orchestration"
    assert summary["status"] == WorkStatus.COMPLETED.value
    assert summary["progress"]["completed_children"] == 3
    assert workflow.status()["status"] == WorkStatus.COMPLETED.value
    assert len(summary["artifacts"]) == 3


def test_period_batch_partitions_split_by_format_period_and_size() -> None:
    """Planned work items should batch by pair/timeframe/format/year-month."""
    request = _request(
        metadata={
            **_request().metadata,
            workflows.BATCHING_METADATA_KEY: {
                workflows.MAX_WORK_ITEMS_PER_BATCH_METADATA_KEY: 2
            },
        }
    )
    work_items = (
        *_multi_period_work_items(count=5),
        *_multi_period_work_items(
            pair="EURUSD",
            timeframe="M1",
            data_format="zip",
            count=2,
        ),
    )

    partitions = workflows.period_batch_partitions(request, work_items)

    assert [partition["format"] for partition in partitions] == [
        "ascii",
        "ascii",
        "ascii",
        "zip",
    ]
    assert [partition["work_item_count"] for partition in partitions] == [
        "2",
        "2",
        "1",
        "2",
    ]
    assert partitions[0]["periods"] == "2022-01,2022-02"
    assert partitions[0]["batch_index"] == "1"
    assert partitions[0]["batch_count"] == "3"
    assert partitions[-1]["batch_count"] == "1"
    assert all(partition["batch_key"] for partition in partitions)


def test_parent_workflow_expands_symbol_children_to_period_batches() -> None:
    """Large planned partitions should become bounded child workflows."""
    request = _request(
        pairs=("EURUSD",),
        metadata={
            **_request().metadata,
            workflows.BATCHING_METADATA_KEY: {
                workflows.MAX_WORK_ITEMS_PER_BATCH_METADATA_KEY: 2
            },
        },
    )
    executor = _RecordingChildExecutor(
        work_items=_multi_period_work_items(count=5)
    )
    workflow = workflows.HistDataRunWorkflow(executor=executor)
    coarse_symbol_id = workflows.build_run_child_invocations(request)[
        1
    ].workflow_id

    summary = asyncio.run(workflow.run(request.to_dict()))

    symbol_calls = [
        call
        for call in executor.calls
        if call["workflow_name"] == "SymbolTimeframeWorkflow"
    ]
    partitions = [
        call["payload"]["partition"]
        for call in symbol_calls
        if isinstance(call["payload"], Mapping)
    ]

    assert len(symbol_calls) == 3
    assert summary["progress"]["total_children"] == 4
    assert summary["progress"]["completed_children"] == 4
    assert [len(_payload_work_items(call)) for call in symbol_calls] == [
        2,
        2,
        1,
    ]
    assert [partition["batch_index"] for partition in partitions] == [
        "1",
        "2",
        "3",
    ]
    assert all(partition["batch_key"] for partition in partitions)
    assert all(call["workflow_id"] != coarse_symbol_id for call in symbol_calls)


def test_parent_workflow_expands_large_plan_from_compact_reference() -> None:
    """Spilled plans should not embed full work items in parent history."""
    request = _request(
        pairs=("EURUSD",),
        metadata={
            **_request().metadata,
            workflows.BATCHING_METADATA_KEY: {
                workflows.MAX_WORK_ITEMS_PER_BATCH_METADATA_KEY: 1
            },
            workflows.FANOUT_METADATA_KEY: {
                workflows.MAX_PARALLEL_CHILD_WORKFLOWS_METADATA_KEY: 4
            },
        },
    )
    executor = _PlanReferenceChildExecutor(
        work_items=_multi_period_work_items(count=50)
    )
    workflow = workflows.HistDataRunWorkflow(executor=executor)

    summary = asyncio.run(workflow.run(request.to_dict()))

    symbol_calls = [
        call
        for call in executor.calls
        if call["workflow_name"] == "SymbolTimeframeWorkflow"
    ]
    symbol_payloads = [
        call["payload"]
        for call in symbol_calls
        if isinstance(call["payload"], Mapping)
    ]

    assert len(symbol_calls) == 50
    assert "work_items" not in summary
    assert all("work_items" not in payload for payload in symbol_payloads)
    assert all(DATASET_PLAN_REF_KEY in payload for payload in symbol_payloads)
    assert summary["progress"]["total_children"] == 51
    assert len(json.dumps(summary, sort_keys=True)) < 60000
    assert (
        max(
            len(json.dumps(payload, sort_keys=True))
            for payload in symbol_payloads
        )
        < 2500
    )


def test_parent_workflow_runs_symbol_batches_with_bounded_fanout() -> None:
    """Independent symbol batches should run in bounded parallel windows."""
    request = _request(
        pairs=("EURUSD",),
        metadata={
            **_request().metadata,
            workflows.BATCHING_METADATA_KEY: {
                workflows.MAX_WORK_ITEMS_PER_BATCH_METADATA_KEY: 1
            },
            workflows.FANOUT_METADATA_KEY: {
                workflows.MAX_PARALLEL_CHILD_WORKFLOWS_METADATA_KEY: 2
            },
        },
    )
    executor = _BoundedFanoutChildExecutor(
        work_items=_multi_period_work_items(count=5)
    )
    workflow = workflows.HistDataRunWorkflow(executor=executor)

    assert workflows.max_parallel_child_workflows(request) == 2

    summary = asyncio.run(workflow.run(request.to_dict()))

    symbol_calls = [
        call
        for call in executor.calls
        if call["workflow_name"] == "SymbolTimeframeWorkflow"
    ]
    partitions = [
        call["payload"]["partition"]
        for call in symbol_calls
        if isinstance(call["payload"], Mapping)
    ]
    symbol_result_ids = [
        result["work_id"] for result in summary["stage_results"][1:]
    ]

    assert executor.max_active_symbol_children == 2
    assert [partition["batch_index"] for partition in partitions] == [
        "1",
        "2",
        "3",
        "4",
        "5",
    ]
    assert symbol_result_ids == [call["workflow_id"] for call in symbol_calls]
    assert summary["status"] == WorkStatus.COMPLETED.value
    assert summary["progress"]["total_children"] == 6
    assert summary["progress"]["completed_children"] == 6


def test_parent_workflow_stops_fanout_after_cancelled_window() -> None:
    """A cancelled symbol batch should prevent later windows from starting."""
    request = _request(
        pairs=("EURUSD",),
        metadata={
            **_request().metadata,
            workflows.BATCHING_METADATA_KEY: {
                workflows.MAX_WORK_ITEMS_PER_BATCH_METADATA_KEY: 1
            },
            workflows.FANOUT_METADATA_KEY: {
                workflows.MAX_PARALLEL_CHILD_WORKFLOWS_METADATA_KEY: 2
            },
        },
    )
    executor = _BoundedFanoutChildExecutor(
        work_items=_multi_period_work_items(count=5),
        cancel_at_symbol_call=1,
    )
    workflow = workflows.HistDataRunWorkflow(executor=executor)

    summary = asyncio.run(workflow.run(request.to_dict()))

    symbol_calls = [
        call
        for call in executor.calls
        if call["workflow_name"] == "SymbolTimeframeWorkflow"
    ]

    assert executor.max_active_symbol_children == 2
    assert len(symbol_calls) == 2
    assert summary["status"] == WorkStatus.CANCELLED.value
    assert summary["progress"]["total_children"] == 6
    assert summary["progress"]["completed_children"] == 3
    assert summary["progress"]["last_error"] == "operator cancelled"


def test_parent_workflow_stops_fanout_after_child_exception() -> None:
    """A child exception should be recorded after its window drains."""
    request = _request(
        pairs=("EURUSD",),
        metadata={
            **_request().metadata,
            workflows.BATCHING_METADATA_KEY: {
                workflows.MAX_WORK_ITEMS_PER_BATCH_METADATA_KEY: 1
            },
            workflows.FANOUT_METADATA_KEY: {
                workflows.MAX_PARALLEL_CHILD_WORKFLOWS_METADATA_KEY: 2
            },
        },
    )
    executor = _BoundedFanoutChildExecutor(
        work_items=_multi_period_work_items(count=5),
        raise_at_symbol_call=1,
    )
    workflow = workflows.HistDataRunWorkflow(executor=executor)

    summary = asyncio.run(workflow.run(request.to_dict()))

    symbol_calls = [
        call
        for call in executor.calls
        if call["workflow_name"] == "SymbolTimeframeWorkflow"
    ]

    assert executor.max_active_symbol_children == 2
    assert len(symbol_calls) == 2
    assert summary["status"] == WorkStatus.FAILED.value
    assert summary["progress"]["completed_children"] == 3
    assert summary["progress"]["last_error"] == "child workflow exploded"


def test_parent_workflow_threads_work_items_through_full_chain() -> None:
    """A full fake chain should never submit empty operation work items."""
    executor = _ThreadingChildExecutor()
    workflow = workflows.HistDataRunWorkflow(executor=executor)
    request = _request()

    summary = asyncio.run(workflow.run(request.to_dict()))

    symbol_calls = [
        call
        for call in executor.calls
        if call["workflow_name"] == "SymbolTimeframeWorkflow"
    ]
    operation_calls = [
        call
        for call in executor.calls
        if str(call["workflow_name"]).endswith("Workflow")
        and call["workflow_name"]
        not in {
            "DatasetPlanWorkflow",
            "SymbolTimeframeWorkflow",
        }
    ]

    assert summary["status"] == WorkStatus.COMPLETED.value
    assert [
        [item["data_fxpair"] for item in _payload_work_items(call)]
        for call in symbol_calls
    ] == [["EURUSD"], ["GBPUSD"]]
    assert operation_calls
    assert all(_payload_work_items(call) for call in operation_calls)
    assert _statuses_for_operation(
        operation_calls, "DownloadArchivesWorkflow"
    ) == {WorkStatus.URL_VALID.value}
    assert _statuses_for_operation(operation_calls, "ExtractCsvWorkflow") == {
        WorkStatus.CSV_ZIP.value
    }
    assert _statuses_for_operation(operation_calls, "BuildCacheWorkflow") == {
        WorkStatus.CSV_FILE.value
    }
    assert _statuses_for_operation(operation_calls, "MergeCacheWorkflow") == {
        WorkStatus.CACHE_READY.value
    }
    assert _statuses_for_operation(operation_calls, "ImportWorkflow") == {
        WorkStatus.CACHE_READY.value
    }


def test_symbol_timeframe_workflow_composes_operation_children() -> None:
    """Partition workflows should call operation-family child workflows."""
    executor = _RecordingChildExecutor()
    workflow = workflows.SymbolTimeframeWorkflow(executor=executor)
    request = _request()

    summary = asyncio.run(
        workflow.run(
            {
                "request": request.to_dict(),
                "partition": {"pair": "EURUSD", "timeframe": "M1"},
                "work_items": [_planned_work_items()[0].to_dict()],
            }
        )
    )

    assert [call["workflow_name"] for call in executor.calls] == [
        "ValidateUrlsWorkflow",
        "DownloadArchivesWorkflow",
        "ExtractCsvWorkflow",
        "BuildCacheWorkflow",
        "MergeCacheWorkflow",
        "ImportWorkflow",
    ]
    assert [call["task_queue"] for call in executor.calls] == [
        "queue-network",
        "queue-network",
        "queue-cpu-file",
        "queue-cpu-file",
        "queue-cpu-file",
        "queue-influx",
    ]
    assert summary["partition"] == {"pair": "EURUSD", "timeframe": "M1"}
    assert summary["progress"]["completed_children"] == 6
    assert workflow.status()["planned_children"] == [
        "ValidateUrlsWorkflow",
        "DownloadArchivesWorkflow",
        "ExtractCsvWorkflow",
        "BuildCacheWorkflow",
        "MergeCacheWorkflow",
        "ImportWorkflow",
    ]


def test_symbol_timeframe_workflow_stops_after_cancelled_child() -> None:
    """A cancellation result should prevent later child workflows starting."""
    executor = _CancellingChildExecutor()
    workflow = workflows.SymbolTimeframeWorkflow(executor=executor)
    request = _request()

    summary = asyncio.run(
        workflow.run(
            {
                "request": request.to_dict(),
                "partition": {"pair": "EURUSD", "timeframe": "M1"},
                "work_items": [_planned_work_items()[0].to_dict()],
            }
        )
    )

    assert [call["workflow_name"] for call in executor.calls] == [
        "ValidateUrlsWorkflow"
    ]
    assert summary["status"] == WorkStatus.CANCELLED.value
    assert summary["progress"]["completed_children"] == 1
    assert summary["progress"]["last_error"] == "operator cancelled"


def test_symbol_timeframe_workflow_skips_after_no_forwardable_items() -> None:
    """A non-forwarded stage should not launch empty downstream children."""
    executor = _NoForwardChildExecutor()
    workflow = workflows.SymbolTimeframeWorkflow(executor=executor)
    request = _request()

    summary = asyncio.run(
        workflow.run(
            {
                "request": request.to_dict(),
                "partition": {"pair": "EURUSD", "timeframe": "M1"},
                "work_items": [_planned_work_items()[0].to_dict()],
            }
        )
    )

    assert [call["workflow_name"] for call in executor.calls] == [
        "ValidateUrlsWorkflow"
    ]
    assert summary["status"] == WorkStatus.COMPLETED.value
    assert [result["status"] for result in summary["stage_results"][1:]] == [
        WorkStatus.SKIPPED.value
    ] * 5


def test_symbol_timeframe_workflow_fails_without_empty_downstream() -> None:
    """A failed stage should fail the partition and skip later child calls."""
    executor = _FailingChildExecutor()
    workflow = workflows.SymbolTimeframeWorkflow(executor=executor)
    request = _request()

    summary = asyncio.run(
        workflow.run(
            {
                "request": request.to_dict(),
                "partition": {"pair": "EURUSD", "timeframe": "M1"},
                "work_items": [_planned_work_items()[0].to_dict()],
            }
        )
    )

    assert [call["workflow_name"] for call in executor.calls] == [
        "ValidateUrlsWorkflow"
    ]
    assert summary["status"] == WorkStatus.FAILED.value
    assert summary["progress"]["last_error"] == "validation failed"


def test_leaf_workflow_uses_mocked_activity_executor() -> None:
    """Leaf workflows should be testable with mocked activities."""
    activity_executor = _RecordingActivityExecutor()
    workflow = workflows.ValidateUrlsWorkflow(
        activity_executor=activity_executor
    )
    request = _request()
    invocation = workflows.build_symbol_child_invocations(
        request,
        {"pair": "EURUSD", "timeframe": "M1"},
    )[0]

    summary = asyncio.run(workflow.run(invocation.payload))

    assert activity_executor.calls == [
        {
            "activity_name": "validate_urls",
            "payload": {
                **invocation.payload,
                "activity": "validate_urls",
                "stage": "validate_urls",
                "task_queue": "queue-network",
            },
            "task_queue": "queue-network",
        }
    ]
    assert summary["status"] == WorkStatus.COMPLETED.value
    assert workflow.status()["completed_children"] == 1


def test_leaf_workflow_defaults_to_temporal_activity_executor(
    monkeypatch,
) -> None:
    """Leaf workflows without test executors should invoke Temporal activities."""
    captured: dict[str, Any] = {}

    async def execute_activity(
        activity_name: str,
        payload: Mapping[str, JSONValue],
        **options: Any,
    ) -> Mapping[str, object]:
        captured["activity_name"] = activity_name
        captured["payload"] = dict(payload)
        captured["options"] = dict(options)
        return StageResult(
            work_id="default-activity-work",
            stage=activity_name,
            status=WorkStatus.COMPLETED,
            metrics={"source": "temporal-default"},
        ).to_dict()

    monkeypatch.setattr(
        workflows.workflow,
        "execute_activity",
        execute_activity,
    )

    request = _request()
    invocation = workflows.build_symbol_child_invocations(
        request,
        {"pair": "EURUSD", "timeframe": "M1"},
    )[0]
    workflow = workflows.ValidateUrlsWorkflow()

    summary = asyncio.run(workflow.run(invocation.payload))

    pending_metric = "activity" + "_pending"
    assert not hasattr(workflows, "Pending" "ActivityExecutor")
    assert captured["activity_name"] == "validate_urls"
    assert captured["options"]["task_queue"] == "queue-network"
    assert summary["status"] == WorkStatus.COMPLETED.value
    assert pending_metric not in summary["stage_results"][0].get(
        "metrics",
        {},
    )
    assert workflow.status()["completed_children"] == 1


def test_extract_csv_workflow_uses_activity_executor() -> None:
    """The extraction leaf workflow should execute its registered activity."""
    activity_executor = _RecordingActivityExecutor()
    workflow = workflows.ExtractCsvWorkflow(activity_executor=activity_executor)
    request = _request(
        validate_urls=False,
        download_data_archives=False,
        extract_csvs=True,
        api_return_type="",
        import_to_influxdb=False,
    )
    [invocation] = workflows.build_symbol_child_invocations(
        request,
        {"pair": "EURUSD", "timeframe": "M1"},
    )

    summary = asyncio.run(workflow.run(invocation.payload))

    assert activity_executor.calls == [
        {
            "activity_name": "extract_csv",
            "payload": {
                **invocation.payload,
                "activity": "extract_csv",
                "stage": "extract_csv",
                "task_queue": "queue-cpu-file",
            },
            "task_queue": "queue-cpu-file",
        }
    ]
    assert summary["status"] == WorkStatus.COMPLETED.value
    assert workflow.status()["completed_children"] == 1


def test_build_cache_workflow_uses_activity_executor() -> None:
    """The cache leaf workflow should execute its registered activity."""
    activity_executor = _RecordingActivityExecutor()
    workflow = workflows.BuildCacheWorkflow(activity_executor=activity_executor)
    request = _request(
        validate_urls=False,
        download_data_archives=False,
        extract_csvs=False,
        api_return_type="polars",
        import_to_influxdb=False,
    )
    invocation = workflows.build_symbol_child_invocations(
        request,
        {"pair": "EURUSD", "timeframe": "M1"},
    )[0]

    summary = asyncio.run(workflow.run(invocation.payload))

    assert activity_executor.calls == [
        {
            "activity_name": "build_cache",
            "payload": {
                **invocation.payload,
                "activity": "build_cache",
                "stage": "build_cache",
                "task_queue": "queue-cpu-file",
            },
            "task_queue": "queue-cpu-file",
        }
    ]
    assert summary["status"] == WorkStatus.COMPLETED.value
    assert workflow.status()["completed_children"] == 1


def test_merge_cache_workflow_uses_activity_executor() -> None:
    """The cache merge leaf workflow should execute its registered activity."""
    activity_executor = _RecordingActivityExecutor()
    workflow = workflows.MergeCacheWorkflow(activity_executor=activity_executor)
    request = _request(
        validate_urls=False,
        download_data_archives=False,
        extract_csvs=False,
        api_return_type="polars",
        import_to_influxdb=False,
    )
    invocation = workflows.build_symbol_child_invocations(
        request,
        {"pair": "EURUSD", "timeframe": "M1"},
    )[0]

    summary = asyncio.run(workflow.run(invocation.payload))

    assert activity_executor.calls == [
        {
            "activity_name": "merge_cache",
            "payload": {
                **invocation.payload,
                "activity": "merge_cache",
                "stage": "merge_cache",
                "task_queue": "queue-cpu-file",
            },
            "task_queue": "queue-cpu-file",
        }
    ]
    assert summary["status"] == WorkStatus.COMPLETED.value
    assert workflow.status()["completed_children"] == 1


def test_import_workflow_uses_activity_executor() -> None:
    """The Influx import leaf workflow should execute its activity."""
    activity_executor = _RecordingActivityExecutor()
    workflow = workflows.ImportWorkflow(activity_executor=activity_executor)
    request = _request(
        validate_urls=False,
        download_data_archives=False,
        extract_csvs=False,
        api_return_type="",
        import_to_influxdb=True,
    )
    invocation = workflows.build_symbol_child_invocations(
        request,
        {"pair": "EURUSD", "timeframe": "M1"},
    )[0]

    summary = asyncio.run(workflow.run(invocation.payload))

    assert activity_executor.calls == [
        {
            "activity_name": "import_to_influx",
            "payload": {
                **invocation.payload,
                "activity": "import_to_influx",
                "stage": "import_to_influx",
                "task_queue": "queue-influx",
            },
            "task_queue": "queue-influx",
        }
    ]
    assert summary["status"] == WorkStatus.COMPLETED.value
    assert workflow.status()["completed_children"] == 1


def test_dataset_plan_workflow_uses_activity_executor() -> None:
    """Dataset planning should run through the activity executor seam."""
    activity_executor = _RecordingActivityExecutor()
    workflow = workflows.DatasetPlanWorkflow(
        activity_executor=activity_executor
    )
    request = _request()
    invocation = workflows.build_run_child_invocations(request)[0]

    summary = asyncio.run(workflow.run(invocation.payload))

    assert activity_executor.calls == [
        {
            "activity_name": "dataset_plan",
            "payload": {
                **invocation.payload,
                "activity": "dataset_plan",
                "stage": "dataset_plan",
                "task_queue": "queue-cpu-file",
            },
            "task_queue": "queue-cpu-file",
        }
    ]
    assert summary["status"] == WorkStatus.COMPLETED.value
    assert workflow.status()["planned_children"] == ["dataset_plan"]


def test_data_quality_workflow_uses_activity_executor() -> None:
    """Quality assessment should run through the activity executor seam."""
    activity_executor = _RecordingActivityExecutor()
    workflow = workflows.DataQualityWorkflow(
        activity_executor=activity_executor
    )
    request = _request(
        data_quality=True,
        quality_paths=("/tmp/histdata",),
        quality_check_groups=("inventory",),
    )
    invocation = workflows.build_run_child_invocations(request)[0]

    summary = asyncio.run(workflow.run(invocation.payload))

    assert activity_executor.calls == [
        {
            "activity_name": "data_quality",
            "payload": {
                **invocation.payload,
                "activity": "data_quality",
                "stage": "data_quality",
                "task_queue": "queue-cpu-file",
            },
            "task_queue": "queue-cpu-file",
        }
    ]
    assert summary["status"] == WorkStatus.COMPLETED.value
    assert workflow.status()["planned_children"] == ["data_quality"]


def test_download_archives_workflow_uses_activity_executor() -> None:
    """Archive downloads should run through the activity executor seam."""
    activity_executor = _RecordingActivityExecutor()
    workflow = workflows.DownloadArchivesWorkflow(
        activity_executor=activity_executor
    )
    request = _request()
    invocation = workflows.build_symbol_child_invocations(
        request,
        {"pair": "EURUSD", "timeframe": "M1"},
    )[1]

    summary = asyncio.run(workflow.run(invocation.payload))

    assert activity_executor.calls == [
        {
            "activity_name": "download_archives",
            "payload": {
                **invocation.payload,
                "activity": "download_archives",
                "stage": "download_archives",
                "task_queue": "queue-network",
            },
            "task_queue": "queue-network",
        }
    ]
    assert summary["status"] == WorkStatus.COMPLETED.value
    assert workflow.status()["planned_children"] == ["download_archives"]


def test_repository_refresh_workflow_uses_activity_executor() -> None:
    """Repository refresh should run through the activity executor seam."""
    activity_executor = _RecordingActivityExecutor()
    workflow = workflows.RepositoryRefreshWorkflow(
        activity_executor=activity_executor
    )
    request = _request(available_remote_data=True, update_remote_data=False)
    invocation = workflows.build_run_child_invocations(request)[0]

    summary = asyncio.run(workflow.run(invocation.payload))

    assert activity_executor.calls == [
        {
            "activity_name": "repository_refresh",
            "payload": {
                **invocation.payload,
                "activity": "repository_refresh",
                "stage": "repository_refresh",
                "task_queue": "queue-network",
            },
            "task_queue": "queue-network",
        }
    ]
    assert summary["status"] == WorkStatus.COMPLETED.value
    assert workflow.status()["planned_children"] == ["repository_refresh"]
