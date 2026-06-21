"""Temporal activity functions for sidecar operation migration."""

from __future__ import annotations

from dataclasses import replace
from importlib import import_module
from pathlib import Path
from typing import Any, Callable, Mapping, TypeVar, cast

from histdatacom.activity_stages import (
    ActivityStageOutput,
    build_cache_work_item,
    dataset_plan_stage,
    download_archive_work_item,
    extract_csv_work_item,
    import_to_influx_work_item,
    merge_cache_work_items,
    repository_refresh_stage,
    validate_url_work_item,
)
from histdatacom.exceptions import influx_failure_info
from histdatacom.observability import attach_progress_metadata
from histdatacom.runtime_contracts import (
    FailureInfo,
    JSONValue,
    RunRequest,
    StageResult,
    StatusEvent,
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
    result = _observe_stage_result(
        output.result,
        total=1,
        completed=1,
        unit="operations",
    )
    return cast(dict[str, Any], result.to_dict())


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
    output = replace(
        output,
        result=_observe_stage_result(
            output.result,
            total=len(output.work_items),
            completed=len(output.work_items),
            unit="work_items",
            increment=len(output.work_items),
        ),
    )
    return cast(dict[str, Any], output.to_dict())


@activity_defn(name="validate_urls")
def validate_urls_activity(payload: dict[str, JSONValue]) -> dict[str, Any]:
    """Run URL validation and form metadata scraping as an activity."""
    request = RunRequest.from_dict(_mapping(payload.get("request", {})))
    work_items = _work_items_from_payload(payload)
    if not work_items:
        result = _observe_stage_result(
            _missing_work_item_result(payload, "validate_urls"),
            total=0,
            completed=0,
            unit="work_items",
            increment=0,
        )
        return cast(
            dict[str, Any],
            result.to_dict(),
        )

    total = len(work_items)
    args = {
        "default_download_dir": set_working_data_dir(request.data_directory),
        "requests_timeout": _request_timeout(request),
    }
    outputs = tuple(
        _observe_activity_output(
            validate_url_work_item(work_item, args=args),
            total=total,
            completed=index,
        )
        for index, work_item in enumerate(work_items, start=1)
    )
    if len(outputs) == 1:
        return cast(dict[str, Any], outputs[0].to_dict())

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
        result = _observe_stage_result(
            _missing_work_item_result(payload, "download_archives"),
            total=0,
            completed=0,
            unit="work_items",
            increment=0,
        )
        return cast(
            dict[str, Any],
            result.to_dict(),
        )

    total = len(work_items)
    args = {
        "default_download_dir": set_working_data_dir(request.data_directory),
        "requests_timeout": _request_timeout(request),
        "from_api": bool(request.api_return_type),
    }
    outputs = tuple(
        _observe_activity_output(
            download_archive_work_item(work_item, args=args),
            total=total,
            completed=index,
        )
        for index, work_item in enumerate(work_items, start=1)
    )
    if len(outputs) == 1:
        return cast(dict[str, Any], outputs[0].to_dict())

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
        result = _observe_stage_result(
            _missing_work_item_result(payload, "extract_csv"),
            total=0,
            completed=0,
            unit="work_items",
            increment=0,
        )
        return cast(
            dict[str, Any],
            result.to_dict(),
        )

    total = len(work_items)
    args = {
        "default_download_dir": set_working_data_dir(request.data_directory),
        "zip_persist": request.zip_persist,
    }
    outputs = tuple(
        _observe_activity_output(
            extract_csv_work_item(work_item, args=args),
            total=total,
            completed=index,
        )
        for index, work_item in enumerate(work_items, start=1)
    )
    if len(outputs) == 1:
        return cast(dict[str, Any], outputs[0].to_dict())

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
        result = _observe_stage_result(
            _missing_work_item_result(payload, "build_cache"),
            total=0,
            completed=0,
            unit="work_items",
            increment=0,
        )
        return cast(
            dict[str, Any],
            result.to_dict(),
        )

    total = len(work_items)
    args = {
        "default_download_dir": set_working_data_dir(request.data_directory),
    }
    outputs = tuple(
        _observe_activity_output(
            build_cache_work_item(work_item, args=args),
            total=total,
            completed=index,
        )
        for index, work_item in enumerate(work_items, start=1)
    )
    if len(outputs) == 1:
        return cast(dict[str, Any], outputs[0].to_dict())

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
        result = _observe_stage_result(
            _missing_work_item_result(payload, "merge_cache"),
            total=0,
            completed=0,
            unit="work_items",
            increment=0,
        )
        return cast(
            dict[str, Any],
            result.to_dict(),
        )

    output = merge_cache_work_items(
        work_items,
        materialize=False,
    )
    output = replace(
        output,
        result=_observe_stage_result(
            output.result,
            total=len(work_items),
            completed=len(work_items),
            unit="work_items",
            increment=len(work_items),
        ),
    )
    return cast(dict[str, Any], output.to_dict())


@activity_defn(name="import_to_influx")
def import_to_influx_activity(
    payload: dict[str, JSONValue],
) -> dict[str, Any]:
    """Upload cache batches to InfluxDB without queue-backed writers."""
    request = RunRequest.from_dict(_mapping(payload.get("request", {})))
    work_items = _work_items_from_payload(payload)
    if not work_items:
        result = _observe_stage_result(
            _missing_work_item_result(payload, "import_to_influx"),
            total=0,
            completed=0,
            unit="work_items",
            increment=0,
        )
        return cast(
            dict[str, Any],
            result.to_dict(),
        )

    total = len(work_items)
    args = _influx_args(request)
    try:
        with _influx_batch_writer(args) as writer:
            outputs = tuple(
                _observe_activity_output(
                    _import_to_influx_with_writer(
                        work_item,
                        args=args,
                        writer=writer,
                    ),
                    total=total,
                    completed=index,
                )
                for index, work_item in enumerate(work_items, start=1)
            )
    except (Exception, SystemExit) as err:
        outputs = tuple(
            _observe_activity_output(
                _influx_failure_output(work_item, err),
                total=total,
                completed=index,
            )
            for index, work_item in enumerate(work_items, start=1)
        )

    if len(outputs) == 1:
        return cast(dict[str, Any], outputs[0].to_dict())

    return cast(
        dict[str, Any],
        {
            "work_items": [output.work_item.to_dict() for output in outputs],
            "stage_results": [output.result.to_dict() for output in outputs],
            "result": _aggregate_activity_outputs(
                outputs,
                "import_to_influx",
            ).to_dict(),
        },
    )


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
        import_to_influx_activity,
    )


def _repo_local_path(request: RunRequest) -> Path:
    data_dir = Path(set_working_data_dir(request.data_directory))
    return data_dir / ".repo"


def _influx_args(request: RunRequest) -> dict[str, Any]:
    return {
        "default_download_dir": set_working_data_dir(request.data_directory),
        "batch_size": request.batch_size,
        "delete_after_influx": request.delete_after_influx,
    }


def _influx_batch_writer(args: Mapping[str, Any]) -> Any:
    from histdatacom.influx import InfluxBatchWriter

    return InfluxBatchWriter(dict(args))


def _import_to_influx_with_writer(
    work_item: WorkItem,
    *,
    args: Mapping[str, Any],
    writer: Any,
) -> ActivityStageOutput:
    batch_events: list[StatusEvent] = []
    batch_index = 0

    def emit_lines(lines: list[str]) -> None:
        nonlocal batch_index
        writer.write_lines(lines)
        batch_index += 1
        metadata: dict[str, JSONValue] = {
            "batch_index": batch_index,
            "line_count": len(lines),
            "cache_path": str(
                Path(work_item.data_dir, work_item.cache_filename)
            ),
        }
        _activity_heartbeat(metadata)
        batch_events.append(
            StatusEvent(
                status=WorkStatus.INFLUX_UPLOAD,
                stage="import_to_influx",
                message="Uploaded InfluxDB line-protocol batch.",
                work_id=work_item.work_id,
                metadata=metadata,
            )
        )

    try:
        output = import_to_influx_work_item(
            work_item,
            args=args,
            emit_lines=emit_lines,
        )
    except (Exception, SystemExit) as err:
        return _influx_failure_output(work_item, err)

    metrics = dict(output.result.metrics)
    metrics["heartbeat_count"] = len(batch_events)
    metrics["idempotency_key"] = (
        f"{work_item.cache_filename}:"
        f"{work_item.cache_start}:"
        f"{work_item.cache_end}:"
        f"{args.get('batch_size', '')}"
    )
    result = replace(
        output.result,
        events=(*output.result.events, *batch_events),
        metrics=metrics,
    )
    return ActivityStageOutput(
        work_item=output.work_item,
        result=result,
        forward=output.forward,
    )


def _activity_heartbeat(metadata: Mapping[str, JSONValue]) -> None:
    heartbeat = getattr(activity, "heartbeat", None)
    if callable(heartbeat):
        heartbeat(dict(metadata))


def _observe_activity_output(
    output: ActivityStageOutput,
    *,
    total: int,
    completed: int,
) -> ActivityStageOutput:
    return replace(
        output,
        result=_observe_stage_result(
            output.result,
            total=total,
            completed=completed,
            unit="work_items",
        ),
    )


def _observe_stage_result(
    result: StageResult,
    *,
    total: int,
    completed: int,
    unit: str,
    increment: int = 1,
) -> StageResult:
    failure_message = result.failure.message if result.failure else ""
    base_events = result.events or (
        StatusEvent(
            status=result.status,
            stage=result.stage,
            message=f"{result.stage} status updated.",
            work_id=result.work_id,
        ),
    )
    observed_event = attach_progress_metadata(
        base_events[-1],
        total=float(total),
        completed=float(completed),
        unit=unit,
        increment=float(increment),
        last_error=failure_message,
        metadata={
            "artifact_count": len(result.artifacts),
            "failure_code": result.failure.code if result.failure else "",
        },
    )
    observed_events = (*base_events[:-1], observed_event)
    progress_metadata = dict(observed_event.metadata)
    progress_metadata.update(
        {
            "stage": result.stage,
            "status": result.status.value,
            "work_id": result.work_id,
        }
    )
    _activity_heartbeat(progress_metadata)
    metrics = dict(result.metrics)
    metrics["progress"] = progress_metadata
    return replace(result, events=observed_events, metrics=metrics)


def _influx_failure_output(
    work_item: WorkItem,
    err: Exception | SystemExit,
) -> ActivityStageOutput:
    failure = _influx_failure_info(err)
    result = StageResult(
        work_id=work_item.work_id,
        stage="import_to_influx",
        status=WorkStatus.RETRIED if failure.retryable else WorkStatus.FAILED,
        failure=failure,
        metrics={"forward": False, "retryable": failure.retryable},
    )
    return ActivityStageOutput(
        work_item=work_item.with_status(result.status),
        result=result,
        forward=False,
    )


def _influx_failure_info(err: Exception | SystemExit) -> FailureInfo:
    return influx_failure_info(err)


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
