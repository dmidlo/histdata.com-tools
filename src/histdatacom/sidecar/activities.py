"""Temporal activity functions for sidecar operation migration."""

from __future__ import annotations

from importlib import import_module
from pathlib import Path
from typing import Any, Callable, Mapping, TypeVar, cast

from histdatacom.activity_stages import (
    dataset_plan_stage,
    repository_refresh_stage,
    validate_url_work_item,
)
from histdatacom.runtime_contracts import (
    FailureInfo,
    JSONValue,
    RunRequest,
    StageResult,
    WorkItem,
    WorkStatus,
)
from histdatacom.utils import set_working_data_dir


class _NoopActivityApi:
    """No-op decorator shim used when temporalio is not installed."""

    def defn(self, decorated: Any | None = None, **kwargs: Any) -> Any:
        """Return a decorator compatible with temporalio.activity.defn."""

        def decorator(value: Any) -> Any:
            return value

        return decorator if decorated is None else decorated


def _load_activity_api() -> Any:
    try:
        return import_module("temporalio.activity")
    except ModuleNotFoundError as err:
        if (err.name or "").split(".")[0] == "temporalio":
            return _NoopActivityApi()
        raise


activity = _load_activity_api()
_Callable = TypeVar("_Callable", bound=Callable[..., Any])


def activity_defn(**kwargs: Any) -> Callable[[_Callable], _Callable]:
    """Apply Temporal's activity decorator with local typing."""
    raw_decorator = activity.defn(**kwargs)

    def decorator(decorated: _Callable) -> _Callable:
        return cast(_Callable, raw_decorator(decorated))

    return decorator


@activity_defn(name="repository_refresh")
def repository_refresh_activity(
    payload: dict[str, JSONValue],
) -> dict[str, Any]:
    """Run repository refresh/listing as a Temporal activity."""
    request = RunRequest.from_dict(_mapping(payload.get("request", {})))
    repo_path = _repo_local_path(request)
    output = repository_refresh_stage(
        repo_data={},
        repo_file_exists=repo_path.exists(),
        repo_local_path=repo_path,
        pairs=request.pairs,
        by=str(request.metadata.get("repo_sort", "") or ""),
        available_remote_data=request.available_remote_data,
        update_remote_data=request.update_remote_data,
    )
    return cast(dict[str, Any], output.result.to_dict())


@activity_defn(name="dataset_plan")
def dataset_plan_activity(payload: dict[str, JSONValue]) -> dict[str, Any]:
    """Run deterministic URL and dataset planning as a Temporal activity."""
    request = RunRequest.from_dict(_mapping(payload.get("request", {})))
    output = dataset_plan_stage(
        start_yearmonth=request.start_yearmonth,
        end_yearmonth=request.end_yearmonth,
        formats=request.formats,
        pairs=request.pairs,
        timeframes=request.timeframes,
        default_download_dir=set_working_data_dir(request.data_directory),
        zip_persist=request.zip_persist,
    )
    return output.to_dict()


@activity_defn(name="validate_urls")
def validate_urls_activity(payload: dict[str, JSONValue]) -> dict[str, Any]:
    """Run URL validation and form metadata scraping as an activity."""
    request = RunRequest.from_dict(_mapping(payload.get("request", {})))
    work_items = _work_items_from_payload(payload)
    if not work_items:
        return cast(
            dict[str, Any], _missing_work_item_result(payload).to_dict()
        )

    args = {
        "default_download_dir": set_working_data_dir(request.data_directory),
        "requests_timeout": _request_timeout(request),
    }
    outputs = tuple(
        validate_url_work_item(work_item, args=args) for work_item in work_items
    )
    if len(outputs) == 1:
        return outputs[0].to_dict()

    return cast(
        dict[str, Any],
        {
            "work_items": [output.work_item.to_dict() for output in outputs],
            "stage_results": [output.result.to_dict() for output in outputs],
            "result": _aggregate_validation_result(outputs).to_dict(),
        },
    )


def default_activities() -> tuple[Callable[..., Any], ...]:
    """Return default sidecar activities for worker registration."""
    return (
        repository_refresh_activity,
        dataset_plan_activity,
        validate_urls_activity,
    )


def _repo_local_path(request: RunRequest) -> Path:
    data_dir = Path(set_working_data_dir(request.data_directory))
    return data_dir / ".repo"


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


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


def _missing_work_item_result(payload: Mapping[str, Any]) -> StageResult:
    workflow_id = str(payload.get("workflow_id", "") or "")
    return StageResult(
        work_id=workflow_id,
        stage="validate_urls",
        status=WorkStatus.FAILED,
        failure=FailureInfo(
            code="MISSING_WORK_ITEM",
            message="validate_urls activity requires work_item or work_items.",
            retryable=False,
        ),
        metrics={"work_item_count": 0},
    )


def _request_timeout(request: RunRequest) -> int:
    value = request.metadata.get("requests_timeout")
    if not isinstance(value, (str, int, float)):
        return 10
    try:
        return int(value)
    except (TypeError, ValueError):
        return 10


def _aggregate_validation_result(outputs: tuple[Any, ...]) -> StageResult:
    statuses = tuple(output.result.status for output in outputs)
    status = (
        WorkStatus.FAILED
        if WorkStatus.FAILED in statuses
        else (
            WorkStatus.RETRIED
            if WorkStatus.RETRIED in statuses
            else WorkStatus.COMPLETED
        )
    )
    return StageResult(
        work_id="",
        stage="validate_urls",
        status=status,
        artifacts=tuple(
            artifact
            for output in outputs
            for artifact in output.result.artifacts
        ),
        events=tuple(
            event for output in outputs for event in output.result.events
        ),
        failure=next(
            (
                output.result.failure
                for output in outputs
                if output.result.failure is not None
            ),
            None,
        ),
        metrics={
            "work_item_count": len(outputs),
            "forward_count": sum(1 for output in outputs if output.forward),
        },
    )
