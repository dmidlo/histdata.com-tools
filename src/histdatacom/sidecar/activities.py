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
    read_repository_data_file,
    repository_refresh_stage,
    validate_url_work_item,
    write_repository_data_file,
)
from histdatacom.cancellation import (
    cancellation_metadata,
    cleanup_partial_artifacts,
    operation_resume_metadata,
    partial_artifact_candidates,
)
from histdatacom.data_quality import (
    QUALITY_REPORT_SCHEMA_VERSION,
    QualityDiscoveryError,
    QualityExitPolicy,
    bounded_quality_payload,
    coverage_manifest_metadata,
    discover_quality_targets,
    normalize_quality_check_groups,
    quality_rules_for_groups,
    quality_run_rules_for_groups,
    run_quality_assessment,
    write_quality_report,
)
from histdatacom.exceptions import (
    CancellationOperationError,
    influx_failure_info,
    retry_policy_for_error,
)
from histdatacom.manifest_store import (
    DATASET_PLAN_BATCHES_KEY,
    DATASET_PLAN_REF_KEY,
    DEFAULT_DATASET_PLAN_INLINE_WORK_ITEM_LIMIT,
    INLINE_WORK_ITEM_LIMIT_METADATA_KEY,
    MANIFEST_SCHEMA_VERSION,
    PLAN_SPILL_METADATA_KEY,
    STATUS_STORE_REF_KEY,
    STATUS_STORE_REF_KIND,
    ManifestStatusStore,
)
from histdatacom.observability import attach_progress_metadata
from histdatacom.repository_quality import (
    repository_data_with_quality_payload,
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
from histdatacom.utils import set_working_data_dir
from histdatacom.sidecar.workflow_metadata import TASK_QUEUE_METADATA_KEY


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
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Run repository refresh/listing as a Temporal activity."""
    request = RunRequest.from_dict(_mapping(payload.get("request", {})))
    repo_path = _repo_local_path(request)
    if _activity_cancelled():
        result = _observe_and_persist_stage_result(
            _cancelled_stage_result(
                "repository_refresh",
                work_id=request.request_id,
                cleanup_paths=partial_artifact_candidates(
                    "repository_refresh",
                    repo_local_path=repo_path,
                ),
            ),
            total=1,
            completed=0,
            unit="operations",
            increment=0,
            payload=payload,
            request=request,
        )
        return cast(dict[str, Any], result.to_dict())

    output = repository_refresh_stage(
        repo_data={},
        repo_file_exists=repo_path.exists(),
        repo_local_path=repo_path,
        pairs=request.pairs,
        by=str(request.metadata.get("repo_sort", "") or ""),
        available_remote_data=request.available_remote_data,
        update_remote_data=request.update_remote_data,
    )
    result = _observe_and_persist_stage_result(
        output.result,
        total=1,
        completed=1,
        unit="operations",
        payload=payload,
        request=request,
    )
    _raise_for_retryable_activity_result(result)
    return cast(dict[str, Any], result.to_dict())


@activity_defn(name="dataset_plan")
def dataset_plan_activity(payload: dict[str, Any]) -> dict[str, Any]:
    """Run deterministic URL and dataset planning as a Temporal activity."""
    request = RunRequest.from_dict(_mapping(payload.get("request", {})))
    data_root = set_working_data_dir(request.data_directory)
    if _activity_cancelled():
        result = _observe_and_persist_stage_result(
            _cancelled_stage_result(
                "dataset_plan",
                work_id=request.request_id,
            ),
            total=0,
            completed=0,
            unit="work_items",
            increment=0,
            payload=payload,
            request=request,
        )
        return cast(
            dict[str, Any],
            {"work_items": [], "result": result.to_dict()},
        )

    output = dataset_plan_stage(
        start_yearmonth=request.start_yearmonth,
        end_yearmonth=request.end_yearmonth,
        formats=request.formats,
        pairs=request.pairs,
        timeframes=request.timeframes,
        default_download_dir=data_root,
        zip_persist=request.zip_persist,
    )
    plan_id = derive_work_id(
        request.request_id,
        output.result.work_id,
        "dataset_plan",
    )
    store = ManifestStatusStore(data_root)
    plan_ref = store.write_dataset_plan(
        plan_id=plan_id,
        request_id=request.request_id,
        work_items=output.work_items,
        metadata={
            "start_yearmonth": request.start_yearmonth,
            "end_yearmonth": request.end_yearmonth,
            "formats": list(request.formats),
            "pairs": list(request.pairs),
            "timeframes": list(request.timeframes),
        },
    )
    plan_batches = _dataset_plan_batches(request, output.work_items)
    inline_limit = _dataset_plan_inline_limit(request)
    spilled = len(output.work_items) > inline_limit
    metrics = dict(output.result.metrics)
    metrics.update(
        {
            "dataset_plan_ref": plan_ref,
            "dataset_plan_batch_count": len(plan_batches),
            "inline_work_item_limit": inline_limit,
            "work_items_spilled": spilled,
        }
    )
    artifact = ArtifactRef(
        kind="dataset-plan",
        path=str(store.db_path),
        metadata={
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "plan_id": plan_id,
            "work_item_count": len(output.work_items),
            "batch_count": len(plan_batches),
        },
    )
    output = replace(
        output,
        result=replace(
            output.result,
            artifacts=(*output.result.artifacts, artifact),
            metrics=metrics,
        ),
    )
    output = replace(
        output,
        result=_observe_and_persist_stage_result(
            output.result,
            total=len(output.work_items),
            completed=len(output.work_items),
            unit="work_items",
            increment=len(output.work_items),
            payload=payload,
            request=request,
        ),
    )
    output_payload: dict[str, Any] = output.to_dict()
    output_payload[DATASET_PLAN_REF_KEY] = plan_ref
    output_payload[DATASET_PLAN_BATCHES_KEY] = [
        dict(batch) for batch in plan_batches
    ]
    if spilled:
        output_payload.pop("work_items", None)
    _raise_for_retryable_activity_result(output.result)
    return output_payload


@activity_defn(name="data_quality")
def data_quality_activity(payload: dict[str, Any]) -> dict[str, Any]:
    """Run offline quality assessment and persist details as an artifact."""
    request = RunRequest.from_dict(_mapping(payload.get("request", {})))
    if _activity_cancelled():
        result = _observe_and_persist_stage_result(
            _cancelled_stage_result(
                "data_quality",
                work_id=request.request_id,
            ),
            total=0,
            completed=0,
            unit="targets",
            increment=0,
            payload=payload,
            request=request,
        )
        return {"result": result.to_dict()}

    try:
        check_groups = normalize_quality_check_groups(
            request.quality_check_groups
        )
        discovery = discover_quality_targets(request.quality_paths)
        exit_policy = QualityExitPolicy.from_values(
            fail_on=request.quality_fail_on,
            max_errors=request.quality_max_errors,
            max_warnings=request.quality_max_warnings,
        )
        report = run_quality_assessment(
            discovery.targets,
            quality_rules_for_groups(check_groups),
            run_rules=quality_run_rules_for_groups(check_groups),
            metadata={
                "operation": "data-quality",
                "check_groups": list(check_groups),
                "request_id": request.request_id,
                "coverage_manifest": coverage_manifest_metadata(
                    roots=discovery.roots,
                    request=request.to_dict(),
                    expected_dimensions=_quality_expected_dimensions(
                        request.metadata
                    ),
                ),
            },
        )
        artifact = write_quality_report(report, _quality_report_path(request))
        decision = exit_policy.evaluate(report.summary())
        quality_payload = bounded_quality_payload(
            operation="data-quality",
            check_groups=check_groups,
            discovery=discovery.to_dict(),
            report=report,
            decision=decision,
            artifact=artifact,
        )
        repo_artifact = _refresh_repo_quality_metadata(
            request,
            quality_payload,
        )
        if repo_artifact is not None:
            quality_payload["repo_quality"] = {
                "refreshed": True,
                "repo_artifact": repo_artifact.to_dict(),
            }
        artifacts = (
            (artifact,)
            if repo_artifact is None
            else (
                artifact,
                repo_artifact,
            )
        )
        result = StageResult(
            work_id=request.request_id,
            stage="data_quality",
            status=(
                WorkStatus.FAILED
                if int(decision.exit_code)
                else WorkStatus.COMPLETED
            ),
            artifacts=artifacts,
            failure=(
                FailureInfo(
                    code="DATA_QUALITY_FAILED",
                    message=decision.reason,
                    retryable=False,
                )
                if int(decision.exit_code)
                else None
            ),
            metrics={
                "quality": quality_payload,
                "quality_report_path": artifact.path,
                "quality_report_sha256": artifact.sha256,
                "repo_quality_refreshed": repo_artifact is not None,
                "repo_quality_path": (
                    "" if repo_artifact is None else repo_artifact.path
                ),
            },
        )
        total_targets = report.summary().target_count
        completed_targets = total_targets
    except (QualityDiscoveryError, ValueError, OSError) as err:
        quality_payload = {
            "operation": "data-quality",
            "report_schema_version": QUALITY_REPORT_SCHEMA_VERSION,
            "error": str(err),
        }
        result = StageResult(
            work_id=request.request_id,
            stage="data_quality",
            status=WorkStatus.FAILED,
            failure=FailureInfo(
                code="DATA_QUALITY_ERROR",
                message=str(err),
                retryable=False,
            ),
            metrics={"quality": quality_payload},
        )
        total_targets = 0
        completed_targets = 0

    result = _observe_and_persist_stage_result(
        result,
        total=total_targets,
        completed=completed_targets,
        unit="targets",
        payload=payload,
        request=request,
    )
    _raise_for_retryable_activity_result(result)
    return {"result": result.to_dict(), "quality": quality_payload}


@activity_defn(name="validate_urls")
def validate_urls_activity(payload: dict[str, Any]) -> dict[str, Any]:
    """Run URL validation and form metadata scraping as an activity."""
    request = RunRequest.from_dict(_mapping(payload.get("request", {})))
    work_items = _activity_work_items_from_payload(payload)
    if not work_items:
        result = _observe_and_persist_stage_result(
            _missing_work_item_result(payload, "validate_urls"),
            total=0,
            completed=0,
            unit="work_items",
            increment=0,
            payload=payload,
            request=request,
        )
        return cast(
            dict[str, Any],
            result.to_dict(),
        )

    args = {
        "default_download_dir": set_working_data_dir(request.data_directory),
        "requests_timeout": _request_timeout(request),
    }
    outputs = _cancellable_outputs(
        "validate_url",
        work_items,
        lambda work_item: validate_url_work_item(work_item, args=args),
        payload=payload,
        request=request,
    )
    if len(outputs) == 1:
        return _activity_output_payload(outputs[0])

    aggregate = _aggregate_activity_outputs(
        outputs,
        "validate_urls",
    )
    _raise_for_retryable_activity_result(aggregate)
    return _activity_batch_payload(outputs, aggregate)


@activity_defn(name="download_archives")
def download_archives_activity(
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Run idempotent archive download as an activity."""
    request = RunRequest.from_dict(_mapping(payload.get("request", {})))
    work_items = _activity_work_items_from_payload(payload)
    if not work_items:
        result = _observe_and_persist_stage_result(
            _missing_work_item_result(payload, "download_archives"),
            total=0,
            completed=0,
            unit="work_items",
            increment=0,
            payload=payload,
            request=request,
        )
        return cast(
            dict[str, Any],
            result.to_dict(),
        )

    args = {
        "default_download_dir": set_working_data_dir(request.data_directory),
        "requests_timeout": _request_timeout(request),
        "from_api": bool(request.api_return_type),
    }
    outputs = _cancellable_outputs(
        "download_archive",
        work_items,
        lambda work_item: download_archive_work_item(work_item, args=args),
        payload=payload,
        request=request,
    )
    if len(outputs) == 1:
        return _activity_output_payload(outputs[0])

    aggregate = _aggregate_activity_outputs(
        outputs,
        "download_archives",
    )
    _raise_for_retryable_activity_result(aggregate)
    return _activity_batch_payload(outputs, aggregate)


@activity_defn(name="extract_csv")
def extract_csv_activity(
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Run idempotent archive extraction as an activity."""
    request = RunRequest.from_dict(_mapping(payload.get("request", {})))
    work_items = _activity_work_items_from_payload(payload)
    if not work_items:
        result = _observe_and_persist_stage_result(
            _missing_work_item_result(payload, "extract_csv"),
            total=0,
            completed=0,
            unit="work_items",
            increment=0,
            payload=payload,
            request=request,
        )
        return cast(
            dict[str, Any],
            result.to_dict(),
        )

    args = {
        "default_download_dir": set_working_data_dir(request.data_directory),
        "zip_persist": request.zip_persist,
    }
    outputs = _cancellable_outputs(
        "extract_csv",
        work_items,
        lambda work_item: extract_csv_work_item(work_item, args=args),
        payload=payload,
        request=request,
    )
    if len(outputs) == 1:
        return _activity_output_payload(outputs[0])

    aggregate = _aggregate_activity_outputs(
        outputs,
        "extract_csv",
    )
    _raise_for_retryable_activity_result(aggregate)
    return _activity_batch_payload(outputs, aggregate)


@activity_defn(name="build_cache")
def build_cache_activity(
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Run Polars cache build/validation as an activity."""
    request = RunRequest.from_dict(_mapping(payload.get("request", {})))
    work_items = _activity_work_items_from_payload(payload)
    if not work_items:
        result = _observe_and_persist_stage_result(
            _missing_work_item_result(payload, "build_cache"),
            total=0,
            completed=0,
            unit="work_items",
            increment=0,
            payload=payload,
            request=request,
        )
        return cast(
            dict[str, Any],
            result.to_dict(),
        )

    args = {
        "default_download_dir": set_working_data_dir(request.data_directory),
    }
    outputs = _cancellable_outputs(
        "build_cache",
        work_items,
        lambda work_item: build_cache_work_item(work_item, args=args),
        payload=payload,
        request=request,
    )
    if len(outputs) == 1:
        return _activity_output_payload(outputs[0])

    aggregate = _aggregate_activity_outputs(
        outputs,
        "build_cache",
    )
    _raise_for_retryable_activity_result(aggregate)
    return _activity_batch_payload(outputs, aggregate)


@activity_defn(name="merge_cache")
def merge_cache_activity(
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Assemble cache merge references without materializing dataframes."""
    request = RunRequest.from_dict(_mapping(payload.get("request", {})))
    work_items = _activity_work_items_from_payload(payload)
    if not work_items:
        result = _observe_and_persist_stage_result(
            _missing_work_item_result(payload, "merge_cache"),
            total=0,
            completed=0,
            unit="work_items",
            increment=0,
            payload=payload,
            request=request,
        )
        return cast(
            dict[str, Any],
            result.to_dict(),
        )

    if _activity_cancelled():
        result = _observe_and_persist_stage_result(
            _cancelled_stage_result("merge_cache"),
            total=len(work_items),
            completed=0,
            unit="work_items",
            increment=0,
            payload=payload,
            request=request,
        )
        return cast(
            dict[str, Any],
            {"result": result.to_dict(), "merge_sets": []},
        )

    output = merge_cache_work_items(
        work_items,
        materialize=False,
    )
    output = replace(
        output,
        result=_observe_and_persist_stage_result(
            output.result,
            total=len(work_items),
            completed=len(work_items),
            unit="work_items",
            increment=len(work_items),
            payload=payload,
            request=request,
        ),
    )
    output_payload: dict[str, Any] = output.to_dict()
    _raise_for_retryable_activity_result(output.result)
    return output_payload


@activity_defn(name="import_to_influx")
def import_to_influx_activity(
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Upload cache batches to InfluxDB without queue-backed writers."""
    request = RunRequest.from_dict(_mapping(payload.get("request", {})))
    work_items = _activity_work_items_from_payload(payload)
    if not work_items:
        result = _observe_and_persist_stage_result(
            _missing_work_item_result(payload, "import_to_influx"),
            total=0,
            completed=0,
            unit="work_items",
            increment=0,
            payload=payload,
            request=request,
        )
        return cast(
            dict[str, Any],
            result.to_dict(),
        )

    total = len(work_items)
    args = _influx_args(request)
    try:
        with _influx_batch_writer(args) as writer:
            outputs = _cancellable_outputs(
                "import_to_influx",
                work_items,
                lambda work_item: _import_to_influx_with_writer(
                    work_item,
                    args=args,
                    writer=writer,
                ),
                payload=payload,
                request=request,
            )
    except (Exception, SystemExit) as err:
        outputs = tuple(
            _observe_and_persist_activity_output(
                _influx_failure_output(work_item, err),
                total=total,
                completed=index,
                payload=payload,
                request=request,
            )
            for index, work_item in enumerate(work_items, start=1)
        )

    if len(outputs) == 1:
        return _activity_output_payload(outputs[0])

    aggregate = _aggregate_activity_outputs(
        outputs,
        "import_to_influx",
    )
    _raise_for_retryable_activity_result(aggregate)
    return _activity_batch_payload(outputs, aggregate)


def default_activities() -> tuple[Callable[..., Any], ...]:
    """Return default sidecar activities for worker registration."""
    return (
        repository_refresh_activity,
        dataset_plan_activity,
        data_quality_activity,
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


def _quality_report_path(request: RunRequest) -> Path:
    if request.quality_report_path:
        return Path(request.quality_report_path)
    data_dir = Path(set_working_data_dir(request.data_directory))
    request_id = request.request_id.strip() or "request"
    return data_dir / ".quality" / "reports" / f"{request_id}.json"


def _refresh_repo_quality_metadata(
    request: RunRequest,
    quality_payload: Mapping[str, Any],
) -> ArtifactRef | None:
    if not request.repo_quality_refresh:
        return None
    repo_path = _repo_local_path(request)
    repo_data = read_repository_data_file(repo_path)
    updated = repository_data_with_quality_payload(
        repo_data,
        _portable_repo_quality_payload(quality_payload, repo_path=repo_path),
        request_id=request.request_id,
    )
    return write_repository_data_file(updated, repo_path)


def _portable_repo_quality_payload(
    quality_payload: Mapping[str, Any],
    *,
    repo_path: Path,
) -> dict[str, Any]:
    payload = dict(quality_payload)
    artifact = quality_payload.get("report_artifact")
    if isinstance(artifact, Mapping):
        portable_artifact = dict(artifact)
        portable_artifact["path"] = _portable_repo_artifact_path(
            str(artifact.get("path", "") or ""),
            repo_path=repo_path,
        )
        payload["report_artifact"] = portable_artifact
    return payload


def _portable_repo_artifact_path(value: str, *, repo_path: Path) -> str:
    path = Path(value)
    if not value or not path.is_absolute():
        return value
    try:
        return str(path.resolve().relative_to(repo_path.parent.resolve()))
    except ValueError:
        return value


def _quality_expected_dimensions(
    metadata: Mapping[str, Any],
) -> tuple[Mapping[str, Any], ...] | None:
    coverage = metadata.get("coverage_manifest")
    if isinstance(coverage, Mapping):
        expected = coverage.get("expected_dimensions")
        if isinstance(expected, list):
            return tuple(item for item in expected if isinstance(item, Mapping))
    expected = metadata.get("quality_expected_dimensions")
    if isinstance(expected, list):
        return tuple(item for item in expected if isinstance(item, Mapping))
    return None


def _influx_args(request: RunRequest) -> dict[str, Any]:
    args = {
        "default_download_dir": set_working_data_dir(request.data_directory),
        "batch_size": request.batch_size,
        "delete_after_influx": request.delete_after_influx,
    }
    influx_config = request.metadata.get("influx_config")
    if isinstance(influx_config, Mapping):
        args.update(
            {
                key: str(influx_config.get(key, "") or "")
                for key in (
                    "INFLUX_ORG",
                    "INFLUX_BUCKET",
                    "INFLUX_URL",
                    "INFLUX_TOKEN",
                )
            }
        )
    return args


def _influx_batch_writer(args: Mapping[str, Any]) -> Any:
    from histdatacom.influx import InfluxBatchWriter

    return InfluxBatchWriter(dict(args))


def _dataset_plan_batches(
    request: RunRequest,
    work_items: tuple[WorkItem, ...],
) -> tuple[dict[str, str], ...]:
    from histdatacom.sidecar.workflows import period_batch_partitions

    batches: tuple[dict[str, str], ...] = period_batch_partitions(
        request,
        work_items,
    )
    return batches


def _dataset_plan_inline_limit(request: RunRequest) -> int:
    spill_config = request.metadata.get(PLAN_SPILL_METADATA_KEY)
    value: object | None = None
    if isinstance(spill_config, Mapping):
        value = spill_config.get(INLINE_WORK_ITEM_LIMIT_METADATA_KEY)
    if value is None:
        value = request.metadata.get(INLINE_WORK_ITEM_LIMIT_METADATA_KEY)
    if value is None:
        default_limit: int = DEFAULT_DATASET_PLAN_INLINE_WORK_ITEM_LIMIT
        return default_limit
    return _positive_inline_limit(value)


def _positive_inline_limit(value: object) -> int:
    if isinstance(value, bool):
        raise ValueError("inline_work_item_limit must be a positive integer")
    if isinstance(value, int):
        normalized = value
    elif isinstance(value, str):
        normalized = int(value)
    else:
        normalized = int(str(value))
    if normalized < 1:
        raise ValueError("inline_work_item_limit must be a positive integer")
    return normalized


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
        try:
            heartbeat(dict(metadata))
        except RuntimeError as err:
            if _outside_activity_context(err):
                return
            raise


def _activity_cancelled() -> bool:
    for attribute in (
        "is_cancelled",
        "is_cancel_requested",
        "is_cancellation_requested",
    ):
        value = getattr(activity, attribute, None)
        if callable(value):
            try:
                return bool(value())
            except RuntimeError as err:
                if _outside_activity_context(err):
                    continue
                raise
        if isinstance(value, bool):
            return value
    return False


def _outside_activity_context(err: RuntimeError) -> bool:
    """Return whether Temporal reports that no activity context is active."""
    return "not in activity context" in str(err).lower()


def _cancellable_outputs(
    stage: str,
    work_items: tuple[WorkItem, ...],
    run_one: Callable[[WorkItem], ActivityStageOutput],
    *,
    payload: Mapping[str, Any],
    request: RunRequest,
) -> tuple[ActivityStageOutput, ...]:
    total = len(work_items)
    outputs: list[ActivityStageOutput] = []
    for index, work_item in enumerate(work_items, start=1):
        if _activity_cancelled():
            outputs.append(
                _observe_and_persist_activity_output(
                    _cancelled_activity_output(work_item, stage),
                    total=total,
                    completed=len(outputs),
                    increment=0,
                    payload=payload,
                    request=request,
                )
            )
            break
        outputs.append(
            _observe_and_persist_activity_output(
                run_one(work_item),
                total=total,
                completed=index,
                payload=payload,
                request=request,
            )
        )
    return tuple(outputs)


def _cancelled_activity_output(
    work_item: WorkItem,
    stage: str,
) -> ActivityStageOutput:
    return ActivityStageOutput(
        work_item=work_item.with_status(WorkStatus.CANCELLED),
        result=_cancelled_stage_result(
            stage,
            work_id=work_item.work_id,
            cleanup_paths=partial_artifact_candidates(stage, work_item),
        ),
        forward=False,
    )


def _cancelled_stage_result(
    stage: str,
    *,
    work_id: str = "",
    reason: str = "Activity cancellation requested.",
    cleanup_paths: tuple[Path, ...] = (),
) -> StageResult:
    cleanup_results = cleanup_partial_artifacts(cleanup_paths)
    metadata = cancellation_metadata(
        stage,
        reason=reason,
        cleanup_results=cleanup_results,
    )
    failure = CancellationOperationError(
        reason,
        detail={"cancellation": metadata},
    ).to_failure_info()
    return StageResult(
        work_id=work_id,
        stage=stage,
        status=WorkStatus.CANCELLED,
        failure=failure,
        events=(
            StatusEvent(
                status=WorkStatus.CANCELLED,
                stage=stage,
                message=reason,
                work_id=work_id,
                metadata={"cancellation": metadata},
            ),
        ),
        metrics={
            "forward": False,
            "cancelled": True,
            "cleanup": metadata["cleanup"],
            "resume_policy": operation_resume_metadata(stage),
        },
    )


def _observe_activity_output(
    output: ActivityStageOutput,
    *,
    total: int,
    completed: int,
    increment: int = 1,
) -> ActivityStageOutput:
    return replace(
        output,
        result=_observe_stage_result(
            output.result,
            total=total,
            completed=completed,
            unit="work_items",
            increment=increment,
        ),
    )


def _observe_and_persist_activity_output(
    output: ActivityStageOutput,
    *,
    total: int,
    completed: int,
    payload: Mapping[str, Any],
    request: RunRequest,
    increment: int = 1,
) -> ActivityStageOutput:
    observed = _observe_activity_output(
        output,
        total=total,
        completed=completed,
        increment=increment,
    )
    _persist_activity_stage_update(
        payload,
        request,
        observed.result,
        work_item=observed.work_item,
    )
    return observed


def _activity_output_payload(output: ActivityStageOutput) -> dict[str, Any]:
    _raise_for_retryable_activity_result(output.result)
    payload: dict[str, Any] = output.to_dict()
    return payload


def _activity_batch_payload(
    outputs: tuple[ActivityStageOutput, ...],
    aggregate: StageResult,
) -> dict[str, Any]:
    return {
        "work_items": [output.work_item.to_dict() for output in outputs],
        "stage_results": [output.result.to_dict() for output in outputs],
        "result": aggregate.to_dict(),
    }


def _raise_for_retryable_activity_result(result: StageResult) -> None:
    failure = result.failure
    if failure is None or not failure.retryable:
        return
    from temporalio.exceptions import ApplicationError

    raise ApplicationError(
        failure.message,
        {
            "stage_result": result.to_dict(),
            "retry_policy": retry_policy_for_error(failure).to_dict(),
        },
        type=failure.code,
        non_retryable=False,
    )


def _observe_and_persist_stage_result(
    result: StageResult,
    *,
    total: int,
    completed: int,
    unit: str,
    payload: Mapping[str, Any],
    request: RunRequest,
    increment: int = 1,
) -> StageResult:
    observed = _observe_stage_result(
        result,
        total=total,
        completed=completed,
        unit=unit,
        increment=increment,
    )
    _persist_activity_stage_update(payload, request, observed)
    return observed


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


def _persist_activity_stage_update(
    payload: Mapping[str, Any],
    request: RunRequest,
    result: StageResult,
    *,
    work_item: WorkItem | None = None,
) -> None:
    store = _activity_status_store(request)
    if store is None:
        return
    store.write_live_stage_update(
        request_id=request.request_id,
        job_id=_job_id_for_request(request),
        workflow_id=_job_id_for_request(request),
        result=result,
        work_item=work_item,
        task_queue=_activity_task_queue(request),
        metadata=_activity_status_metadata(payload),
    )


def _activity_status_store(request: RunRequest) -> ManifestStatusStore | None:
    ref = _mapping(request.metadata.get(STATUS_STORE_REF_KEY, {}))
    if str(ref.get("kind", "") or "") != STATUS_STORE_REF_KIND:
        return None
    store_root = str(ref.get("store_root", "") or "")
    if not store_root:
        return None
    return ManifestStatusStore(store_root)


def _job_id_for_request(request: RunRequest) -> str:
    request_id = request.request_id.strip() or "request"
    return f"histdatacom-{request_id}"


def _activity_task_queue(request: RunRequest) -> str:
    task_queues = _mapping(request.metadata.get(TASK_QUEUE_METADATA_KEY, {}))
    return str(task_queues.get("orchestration", "") or "")


def _activity_status_metadata(
    payload: Mapping[str, Any],
) -> dict[str, JSONValue]:
    metadata: dict[str, JSONValue] = {}
    workflow_id = str(payload.get("workflow_id", "") or "")
    if workflow_id:
        metadata["activity_workflow_id"] = workflow_id
    partition = _mapping(payload.get("partition", {}))
    if partition:
        metadata["partition"] = {
            str(key): str(value) for key, value in partition.items()
        }
    return metadata


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
    failure: FailureInfo = influx_failure_info(err)
    return failure


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


def _activity_work_items_from_payload(
    payload: Mapping[str, Any],
) -> tuple[WorkItem, ...]:
    inline_work_items = _work_items_from_payload(payload)
    if inline_work_items:
        return inline_work_items
    return _work_items_from_dataset_plan_ref(payload)


def _work_items_from_dataset_plan_ref(
    payload: Mapping[str, Any],
) -> tuple[WorkItem, ...]:
    plan_ref = _mapping(payload.get(DATASET_PLAN_REF_KEY, {}))
    plan_id = str(plan_ref.get("plan_id", "") or "")
    store_root = str(plan_ref.get("store_root", "") or "")
    if not plan_id or not store_root:
        return ()
    partition = _mapping(payload.get("partition", {}))
    work_ids = _partition_work_ids(partition)
    work_items: tuple[WorkItem, ...] = ManifestStatusStore(
        store_root
    ).get_dataset_plan_work_items(
        plan_id,
        work_ids=work_ids,
    )
    return work_items


def _partition_work_ids(partition: Mapping[str, Any]) -> tuple[str, ...]:
    raw_work_ids = str(partition.get("work_ids", "") or "")
    if not raw_work_ids:
        return ()
    return tuple(
        work_id.strip()
        for work_id in raw_work_ids.split(",")
        if work_id.strip()
    )


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
        WorkStatus.CANCELLED
        if WorkStatus.CANCELLED in statuses
        else (
            WorkStatus.FAILED
            if WorkStatus.FAILED in statuses
            else (
                WorkStatus.RETRIED
                if WorkStatus.RETRIED in statuses
                else WorkStatus.COMPLETED
            )
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
