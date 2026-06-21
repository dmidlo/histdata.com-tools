"""Tests for Temporal sidecar workflow topology."""

from __future__ import annotations

import asyncio
from typing import Mapping

from histdatacom.runtime_contracts import (
    ArtifactRef,
    FailureInfo,
    JSONValue,
    RunRequest,
    StageResult,
    WorkStatus,
)
from histdatacom.sidecar import workflows
from histdatacom.sidecar.workflow_metadata import TASK_QUEUE_METADATA_KEY


class _RecordingChildExecutor:
    """Fake child workflow executor used by composition tests."""

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
        """Record child workflow calls and return a completed stage."""
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
            status=WorkStatus.COMPLETED,
            artifacts=(
                ArtifactRef(
                    kind="manifest",
                    path=f"{workflow_id}.json",
                ),
            ),
        ).to_dict()


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

    assert document["schema_version"] == 1
    assert set(workflows.workflow_names()) == {
        "HistDataRunWorkflow",
        "RepositoryRefreshWorkflow",
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
            }
        )
    )

    assert [call["workflow_name"] for call in executor.calls] == [
        "ValidateUrlsWorkflow"
    ]
    assert summary["status"] == WorkStatus.CANCELLED.value
    assert summary["progress"]["completed_children"] == 1
    assert summary["progress"]["last_error"] == "operator cancelled"


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
