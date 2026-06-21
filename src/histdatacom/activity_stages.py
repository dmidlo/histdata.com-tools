"""Queue-free stage functions for the Temporal sidecar migration."""

from __future__ import annotations

import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, MutableMapping, Sequence

from histdatacom.histdata_ascii import (
    CACHE_FILENAME,
    convert_polars_datetime_to_utc_ms,
    format_influx_line,
    read_ascii_file_to_polars,
    read_polars_cache,
    write_polars_cache,
)
from histdatacom.records import Record
from histdatacom.runtime_contracts import (
    ArtifactRef,
    FailureInfo,
    StageResult,
    StatusEvent,
    WorkItem,
    WorkStatus,
    derive_work_id,
    status_has_csv_artifact,
)
from histdatacom.utils import check_installed_module


@dataclass(frozen=True, slots=True)
class ActivityStageOutput:
    """A single-work-item stage result plus the next explicit work item."""

    work_item: WorkItem
    result: StageResult
    forward: bool = True

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation for future activities."""
        return {
            "work_item": self.work_item.to_dict(),
            "result": self.result.to_dict(),
            "forward": self.forward,
        }


@dataclass(frozen=True, slots=True)
class MergeStageOutput:
    """A merge-stage result plus the in-memory API return payload."""

    data: Any
    result: StageResult


RecordTransformer = Callable[[Record], Record]
RecordAction = Callable[[Record], None]
NoArgBool = Callable[[], bool]
LineSink = Callable[[list[str]], None]


def validate_url_work_item(
    work_item: WorkItem,
    *,
    args: Mapping[str, Any],
    scrape_record_info: RecordTransformer,
    check_for_valid_download: RecordAction,
    check_if_queue_is_needed: NoArgBool | None = None,
    set_repo_datum: RecordAction | None = None,
) -> ActivityStageOutput:
    """Validate one HistData URL without touching global queues."""
    record = _record_from_work_item(work_item)
    try:
        if record.status == WorkStatus.URL_NEW.value:
            record = scrape_record_info(record)
            check_for_valid_download(record)
            if (
                check_if_queue_is_needed is not None
                and check_if_queue_is_needed()
                and set_repo_datum is not None
            ):
                set_repo_datum(record)

            record.status = WorkStatus.URL_VALID.value
            record.write_memento_file(base_dir=_default_download_dir(args))

        updated = _work_item_from_record(record, work_item)
        return _activity_output(
            updated,
            stage="validate_url",
            status=updated.status,
            metrics={"forward": True},
        )
    except ValueError:
        record.status = WorkStatus.URL_NO_REPO_DATA.value
        record.write_memento_file(base_dir=_default_download_dir(args))
        updated = _work_item_from_record(record, work_item)
        return _activity_output(
            updated,
            stage="validate_url",
            status=WorkStatus.URL_NO_REPO_DATA,
            forward=False,
            metrics={"forward": False, "missing_repo_data": True},
            message="HistData has no downloadable archive for this URL.",
        )
    except Exception as err:
        record.delete_momento_file()
        raise SystemExit(1) from err


def download_archive_work_item(
    work_item: WorkItem,
    *,
    args: Mapping[str, Any],
    download_file: RecordAction,
) -> ActivityStageOutput:
    """Download one ZIP archive without touching global queues."""
    record = _record_from_work_item(work_item)
    try:
        if WorkStatus.URL_VALID.value in record.status or (
            bool(args.get("from_api"))
            and not _existing_archive_artifact_on_disk(record)
        ):
            download_file(record)
            record.status = (
                WorkStatus.CSV_ZIP.value
                if record.status == WorkStatus.URL_VALID.value
                else record.status
            )
            record.write_memento_file(base_dir=_default_download_dir(args))

        updated = _work_item_from_record(record, work_item)
        return _activity_output(
            updated,
            stage="download_archive",
            status=updated.status,
            artifacts=_artifact_refs_for_record(record, "zip"),
            metrics={"forward": True},
        )
    except KeyError as err:
        record.delete_momento_file()
        failed = _work_item_from_record(record, work_item).with_status(
            WorkStatus.FAILED
        )
        return _activity_output(
            failed,
            stage="download_archive",
            status=WorkStatus.FAILED,
            forward=False,
            failure=FailureInfo(
                code="INVALID_ZIP_RESPONSE",
                message=str(err),
                retryable=True,
                detail={"url": record.url},
            ),
            metrics={"forward": False},
        )
    except Exception:
        record.delete_momento_file()
        raise


def extract_csv_work_item(
    work_item: WorkItem,
    *,
    args: Mapping[str, Any],
) -> ActivityStageOutput:
    """Extract one CSV/XLSX payload without touching global queues."""
    record = _record_from_work_item(work_item)
    try:
        if WorkStatus.CSV_ZIP.value in record.status:
            zip_path = Path(record.data_dir, record.zip_filename)

            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                data_members = [
                    name
                    for name in zip_ref.namelist()
                    if name.lower().endswith((".csv", ".xlsx"))
                ]
                if len(data_members) != 1:
                    raise ValueError(
                        "expected ZIP archive to contain one CSV/XLSX file"
                    )
                [record.csv_filename] = data_members
                zip_ref.extract(record.csv_filename, path=record.data_dir)

            zip_path.unlink()
            record.status = WorkStatus.CSV_FILE.value
            record.write_memento_file(base_dir=_default_download_dir(args))

        updated = _work_item_from_record(record, work_item)
        return _activity_output(
            updated,
            stage="extract_csv",
            status=updated.status,
            artifacts=_artifact_refs_for_record(record, "csv"),
        )
    except (OSError, ValueError) as err:
        record.delete_momento_file()
        raise SystemExit(1) from err


def build_cache_work_item(
    work_item: WorkItem,
    *,
    args: Mapping[str, Any],
    download_file: RecordAction | None = None,
) -> ActivityStageOutput:
    """Build or validate one Polars cache without touching global queues."""
    record = _record_from_work_item(work_item)
    if not _supports_cache(record):
        return _activity_output(
            work_item,
            stage="build_cache",
            status=WorkStatus.SKIPPED,
            metrics={"cache_supported": False},
        )

    cache_path = Path(record.data_dir, CACHE_FILENAME)
    created = False
    if not cache_path.exists():
        if not status_has_csv_artifact(record.status):
            if download_file is None:
                raise ValueError(
                    "download_file is required when no local source exists"
                )
            download_file(record)
        create_cache_file(record, args)
        created = True
    elif not record.cache_filename:
        record.cache_filename = CACHE_FILENAME

    record.status = WorkStatus.CACHE_READY.value
    record.write_memento_file(base_dir=_default_download_dir(args))
    updated = _work_item_from_record(record, work_item)
    return _activity_output(
        updated,
        stage="build_cache",
        status=WorkStatus.CACHE_READY,
        artifacts=_artifact_refs_for_record(record, "cache"),
        metrics={
            "cache_created": created,
            "cache_line_count": _json_int(record.cache_line_count),
        },
    )


def merge_cache_work_items(
    work_items: Sequence[WorkItem],
    *,
    return_type: str,
) -> MergeStageOutput:
    """Merge cache artifacts from explicit work items."""
    mergeable = [
        item
        for item in work_items
        if item.cache_filename == CACHE_FILENAME
        and Path(item.data_dir, item.cache_filename).exists()
    ]
    sets_to_merge = _collate_cache_sets(mergeable)
    for cache_set in sets_to_merge:
        cache_set["data"] = merge_cache_items(
            cache_set["records"],
            return_type=return_type,
        )

    data = (
        sets_to_merge[0]["data"] if len(sets_to_merge) == 1 else sets_to_merge
    )
    result = StageResult(
        work_id=derive_work_id(
            "merge_cache", *(item.work_id for item in mergeable)
        ),
        stage="merge_cache",
        status=WorkStatus.COMPLETED if sets_to_merge else WorkStatus.SKIPPED,
        events=(
            StatusEvent(
                status=(
                    WorkStatus.COMPLETED
                    if sets_to_merge
                    else WorkStatus.SKIPPED
                ),
                stage="merge_cache",
                message="Merged cache artifacts.",
                metadata={
                    "record_count": len(mergeable),
                    "set_count": len(sets_to_merge),
                },
            ),
        ),
        metrics={
            "record_count": len(mergeable),
            "set_count": len(sets_to_merge),
        },
    )
    return MergeStageOutput(data=data, result=result)


def merge_cache_items(
    work_items: Sequence[WorkItem],
    *,
    return_type: str,
) -> Any:
    """Merge one pair/timeframe cache set into the requested API type."""
    import polars as pl

    ordered_items = sorted(work_items, key=lambda item: item.cache_start)
    frames = [
        read_polars_cache(Path(item.data_dir, item.cache_filename))
        for item in ordered_items
    ]
    merged = pl.concat(frames) if frames else pl.DataFrame()
    return _convert_cache_frame(merged, return_type)


def merge_cache_records(
    records: Sequence[Any],
    *,
    return_type: str,
) -> Any:
    """Merge one legacy record set through the queue-free implementation."""
    return merge_cache_items(
        [WorkItem.from_record(record) for record in records],
        return_type=return_type,
    )


def import_to_influx_work_item(
    work_item: WorkItem,
    *,
    args: Mapping[str, Any],
    emit_lines: LineSink,
) -> ActivityStageOutput:
    """Convert one cache to Influx line-protocol batches."""
    record = _record_from_work_item(work_item)
    batch_count = 0
    line_count = 0
    try:
        if (
            record.status != WorkStatus.INFLUX_UPLOAD.value
            and str.lower(record.data_format) == "ascii"
        ):
            cache_path = Path(record.data_dir, CACHE_FILENAME)
            if not cache_path.exists() and status_has_csv_artifact(
                record.status
            ):
                cache_output = build_cache_work_item(
                    _work_item_from_record(record, work_item),
                    args=args,
                )
                record = _record_from_work_item(cache_output.work_item)
                cache_path = Path(record.data_dir, CACHE_FILENAME)

            if cache_path.exists():
                record.cache_filename = record.cache_filename or CACHE_FILENAME
                batch_count, line_count = emit_influx_cache_batches(
                    _work_item_from_record(record, work_item),
                    args=args,
                    emit_lines=emit_lines,
                )

        record.status = WorkStatus.INFLUX_UPLOAD.value
        record.write_memento_file(base_dir=_default_download_dir(args))

        if bool(args.get("delete_after_influx")):
            _unlink_if_present(record.data_dir, record.zip_filename)
            _unlink_if_present(record.data_dir, record.cache_filename)

        updated = _work_item_from_record(record, work_item)
        return _activity_output(
            updated,
            stage="import_to_influx",
            status=WorkStatus.INFLUX_UPLOAD,
            metrics={"batch_count": batch_count, "line_count": line_count},
        )
    except Exception:
        record.delete_momento_file()
        raise


def emit_influx_cache_batches(
    work_item: WorkItem,
    *,
    args: Mapping[str, Any],
    emit_lines: LineSink,
) -> tuple[int, int]:
    """Emit bounded Influx line-protocol batches for one cache artifact."""
    cache_filename = work_item.cache_filename or CACHE_FILENAME
    cache = read_polars_cache(Path(work_item.data_dir, cache_filename))
    batch_size = coerce_batch_size(args["batch_size"])
    batch_count = 0
    line_count = 0
    for rows in iter_polars_row_batches(cache, batch_size):
        lines = [
            format_influx_line(
                work_item.data_fxpair,
                work_item.data_format,
                work_item.data_timeframe,
                row,
            )
            for row in rows
        ]
        if lines:
            emit_lines(lines)
            batch_count += 1
            line_count += len(lines)
    return batch_count, line_count


def coerce_batch_size(batch_size: Any) -> int:
    """Return a positive integer batch size."""
    try:
        normalized = int(batch_size)
    except (TypeError, ValueError) as err:
        raise ValueError("batch_size must be a positive integer") from err

    if normalized < 1:
        raise ValueError("batch_size must be a positive integer")

    return normalized


def iter_polars_row_batches(
    frame: Any, batch_size: int
) -> Iterable[list[tuple[Any, ...]]]:
    """Yield bounded row batches from a Polars dataframe."""
    for frame_slice in frame.iter_slices(n_rows=batch_size):
        rows = list(frame_slice.iter_rows())
        if rows:
            yield rows


def apply_stage_output_to_record(
    output: ActivityStageOutput, record: Record
) -> Record:
    """Apply an explicit stage output to a legacy mutable record."""
    record(**output.work_item.to_record_kwargs())
    return record


def _record_from_work_item(work_item: WorkItem) -> Record:
    return Record(**work_item.to_record_kwargs())


def _work_item_from_record(record: Record, original: WorkItem) -> WorkItem:
    return WorkItem.from_record(record, work_id=original.work_id)


def _activity_output(
    work_item: WorkItem,
    *,
    stage: str,
    status: WorkStatus,
    artifacts: tuple[ArtifactRef, ...] = (),
    forward: bool = True,
    failure: FailureInfo | None = None,
    metrics: Mapping[str, Any] | None = None,
    message: str = "",
) -> ActivityStageOutput:
    result = StageResult(
        work_id=work_item.work_id,
        stage=stage,
        status=status,
        artifacts=artifacts,
        events=(
            StatusEvent(
                status=status,
                stage=stage,
                message=message,
                work_id=work_item.work_id,
            ),
        ),
        failure=failure,
        metrics=dict(metrics or {}),
    )
    return ActivityStageOutput(
        work_item=work_item, result=result, forward=forward
    )


def _default_download_dir(args: Mapping[str, Any]) -> str:
    return str(args.get("default_download_dir", "") or "")


def _artifact_refs_for_record(
    record: Record, artifact_kind: str
) -> tuple[ArtifactRef, ...]:
    filenames = {
        "cache": (record.cache_filename,),
        "csv": (record.csv_filename,),
        "zip": (record.zip_filename,),
    }[artifact_kind]
    refs = []
    for filename in filenames:
        if not filename:
            continue
        path = Path(record.data_dir, filename)
        if path.exists():
            refs.append(
                ArtifactRef(
                    kind=artifact_kind,
                    path=str(path),
                    size_bytes=path.stat().st_size,
                )
            )
    return tuple(refs)


def _existing_archive_artifact_on_disk(record: Record) -> bool:
    return any(
        Path(record.data_dir, filename).exists()
        for filename in (
            record.zip_filename,
            record.csv_filename,
            record.cache_filename,
        )
        if filename
    )


def _supports_cache(record: Record) -> bool:
    return str.lower(record.data_format) == "ascii" and (
        record.data_timeframe in ["T", "M1"]
    )


def create_cache_file(record: Record, args: Mapping[str, Any]) -> None:
    zip_path = Path(record.data_dir, record.zip_filename)
    csv_path = Path(record.data_dir, record.csv_filename)

    if zip_path.exists():
        file_data = _import_source_to_polars(record, zip_path)
    elif csv_path.exists():
        file_data = _import_source_to_polars(record, csv_path)
    else:
        raise ValueError("expected downloaded ZIP or CSV source file")

    record.cache_filename = CACHE_FILENAME
    cache_path = Path(record.data_dir, record.cache_filename)
    write_polars_cache(file_data, cache_path)

    record.cache_line_count = file_data.height
    record.cache_start = str(_extract_single_value(file_data, 0, "datetime"))
    record.cache_end = str(
        _extract_single_value(file_data, file_data.height - 1, "datetime")
    )
    record.write_memento_file(base_dir=_default_download_dir(args))


def _import_source_to_polars(record: Record, source_path: Path) -> Any:
    try:
        raw_frame = read_ascii_file_to_polars(
            source_path, record.data_timeframe
        )
        return convert_polars_datetime_to_utc_ms(
            raw_frame,
            record.data_timeframe,
        )
    except ValueError as err:
        raise SystemExit(1) from err


def _extract_single_value(frame: Any, row: int, column: str) -> int:
    return int(frame.item(row, column))


def _convert_cache_frame(frame: Any, return_type: str) -> Any:
    match return_type:
        case "arrow":
            check_installed_module("arrow", True)
            return frame.to_arrow()
        case "pandas":
            check_installed_module("pandas", True)
            return frame.to_pandas()
        case "polars":
            check_installed_module("polars", True)
            return frame
        case _:
            raise ValueError(f"unsupported API return type: {return_type}")


def _collate_cache_sets(
    work_items: Sequence[WorkItem],
) -> list[MutableMapping[str, Any]]:
    sets_to_merge: list[MutableMapping[str, Any]] = []
    sets_by_key: dict[tuple[str, str], MutableMapping[str, Any]] = {}
    for item in work_items:
        key = (item.data_timeframe, item.data_fxpair)
        if key not in sets_by_key:
            cache_set: MutableMapping[str, Any] = {
                "timeframe": item.data_timeframe,
                "pair": item.data_fxpair,
                "records": [],
                "data": None,
            }
            sets_by_key[key] = cache_set
            sets_to_merge.append(cache_set)

        sets_by_key[key]["records"].append(item)

    return sets_to_merge


def _json_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _unlink_if_present(data_dir: str, filename: str) -> None:
    if not filename:
        return
    path = Path(data_dir, filename)
    if path.exists():
        path.unlink()
