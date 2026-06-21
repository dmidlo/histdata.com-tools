"""Temporal activity functions for sidecar operation migration."""

from __future__ import annotations

from importlib import import_module
from pathlib import Path
from typing import Any, Callable, Mapping, TypeVar, cast

from histdatacom.activity_stages import (
    build_cache_work_item,
    dataset_plan_stage,
    download_archive_work_item,
    extract_csv_work_item,
    merge_cache_work_items,
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
            dict[str, Any],
            _missing_work_item_result(payload, "validate_urls").to_dict(),
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
            "result": _aggregate_activity_outputs(
                outputs,
                "validate_urls",
            ).to_dict(),
        },
    )


@activity_defn(name="download_archives")
def download_archives_activity(
    payload: dict[str, JSONValue],
) -> dict[str, Any]:
    """Run idempotent archive download as an activity."""
    request = RunRequest.from_dict(_mapping(payload.get("request", {})))
    work_items = _work_items_from_payload(payload)
    if not work_items:
        return cast(
            dict[str, Any],
            _missing_work_item_result(payload, "download_archives").to_dict(),
        )

    args = {
        "default_download_dir": set_working_data_dir(request.data_directory),
        "requests_timeout": _request_timeout(request),
        "from_api": bool(request.api_return_type),
    }
    outputs = tuple(
        download_archive_work_item(work_item, args=args)
        for work_item in work_items
    )
    if len(outputs) == 1:
        return outputs[0].to_dict()

    return cast(
        dict[str, Any],
        {
            "work_items": [output.work_item.to_dict() for output in outputs],
            "stage_results": [output.result.to_dict() for output in outputs],
            "result": _aggregate_activity_outputs(
                outputs,
                "download_archives",
            ).to_dict(),
        },
    )


@activity_defn(name="extract_csv")
def extract_csv_activity(
    payload: dict[str, JSONValue],
) -> dict[str, Any]:
    """Run idempotent archive extraction as an activity."""
    request = RunRequest.from_dict(_mapping(payload.get("request", {})))
    work_items = _work_items_from_payload(payload)
    if not work_items:
        return cast(
            dict[str, Any],
            _missing_work_item_result(payload, "extract_csv").to_dict(),
        )

    args = {
        "default_download_dir": set_working_data_dir(request.data_directory),
        "zip_persist": request.zip_persist,
    }
    outputs = tuple(
        extract_csv_work_item(work_item, args=args) for work_item in work_items
    )
    if len(outputs) == 1:
        return outputs[0].to_dict()

    return cast(
        dict[str, Any],
        {
            "work_items": [output.work_item.to_dict() for output in outputs],
            "stage_results": [output.result.to_dict() for output in outputs],
            "result": _aggregate_activity_outputs(
                outputs,
                "extract_csv",
            ).to_dict(),
        },
    )


@activity_defn(name="build_cache")
def build_cache_activity(
    payload: dict[str, JSONValue],
) -> dict[str, Any]:
    """Run Polars cache build/validation as an activity."""
    request = RunRequest.from_dict(_mapping(payload.get("request", {})))
    work_items = _work_items_from_payload(payload)
    if not work_items:
        return cast(
            dict[str, Any],
            _missing_work_item_result(payload, "build_cache").to_dict(),
        )

    args = {
        "default_download_dir": set_working_data_dir(request.data_directory),
    }
    outputs = tuple(
        build_cache_work_item(work_item, args=args) for work_item in work_items
    )
    if len(outputs) == 1:
        return outputs[0].to_dict()

    return cast(
        dict[str, Any],
        {
            "work_items": [output.work_item.to_dict() for output in outputs],
            "stage_results": [output.result.to_dict() for output in outputs],
            "result": _aggregate_activity_outputs(
                outputs,
                "build_cache",
            ).to_dict(),
        },
    )


@activity_defn(name="merge_cache")
def merge_cache_activity(
    payload: dict[str, JSONValue],
) -> dict[str, Any]:
    """Assemble cache merge references without materializing dataframes."""
    work_items = _work_items_from_payload(payload)
    if not work_items:
        return cast(
            dict[str, Any],
            _missing_work_item_result(payload, "merge_cache").to_dict(),
        )

    output = merge_cache_work_items(
        work_items,
        materialize=False,
    )
    return output.to_dict()


def default_activities() -> tuple[Callable[..., Any], ...]:
    """Return default sidecar activities for worker registration."""
    return (
        repository_refresh_activity,
        dataset_plan_activity,
        validate_urls_activity,
        download_archives_activity,
        extract_csv_activity,
        build_cache_activity,
        merge_cache_activity,
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


def _missing_work_item_result(
    payload: Mapping[str, Any],
    stage: str,
) -> StageResult:
    workflow_id = str(payload.get("workflow_id", "") or "")
    return StageResult(
        work_id=workflow_id,
        stage=stage,
        status=WorkStatus.FAILED,
        failure=FailureInfo(
            code="MISSING_WORK_ITEM",
            message=f"{stage} activity requires work_item or work_items.",
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


def _aggregate_activity_outputs(
    outputs: tuple[Any, ...],
    stage: str,
) -> StageResult:
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
        stage=stage,
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
