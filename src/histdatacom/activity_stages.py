"""Queue-free stage functions for the Temporal sidecar migration."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import ssl
import zipfile
from dataclasses import dataclass, replace
from email.message import Message
from pathlib import Path, PurePosixPath
from ssl import SSLCertVerificationError
from typing import Any, Callable, Iterable, Mapping, MutableMapping, Sequence
from urllib.error import URLError
from urllib.request import urlopen

import certifi
import requests
from bs4 import BeautifulSoup

from histdatacom.cancellation import deterministic_partial_path
from histdatacom.exceptions import (
    ArchiveDownloadError,
    ArchiveExtractionError,
    CacheBuildError,
    HistDataNoDataError,
    UrlValidationError,
    failure_info_from_exception,
)
from histdatacom.fx_enums import Format, Timeframe, get_valid_format_timeframes
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
    JSONValue,
    StageResult,
    StatusEvent,
    WorkItem,
    WorkStatus,
    derive_work_id,
    status_has_csv_artifact,
)
from histdatacom.utils import (
    check_installed_module,
    create_full_path,
    force_datemonth_if_only_year,
    get_current_datemonth_gmt_minus5,
    get_month_from_datemonth,
    get_now_utc_timestamp,
    get_year_from_datemonth,
    hash_dict,
)

DEFAULT_REPOSITORY_URL = (
    "https://raw.githubusercontent.com/dmidlo/"
    "histdata.com-tools/main/data/.repo"
)
DEFAULT_HISTDATA_BASE_URL = "http://www.histdata.com/download-free-forex-data/"


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
    merge_sets: tuple["CacheMergeSetSummary", ...] = ()

    def to_dict(self) -> dict[str, Any]:
        """Return JSON-compatible merge metadata without dataframe payloads."""
        return {
            "result": self.result.to_dict(),
            "merge_sets": [
                merge_set.to_dict() for merge_set in self.merge_sets
            ],
        }


@dataclass(frozen=True, slots=True)
class CacheMergeSetSummary:
    """Bounded metadata for one pair/timeframe cache merge set."""

    timeframe: str
    pair: str
    record_count: int
    line_count: int
    start: str
    end: str
    artifacts: tuple[ArtifactRef, ...]
    work_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible merge-set metadata."""
        return {
            "timeframe": self.timeframe,
            "pair": self.pair,
            "record_count": self.record_count,
            "line_count": self.line_count,
            "start": self.start,
            "end": self.end,
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
            "work_ids": list(self.work_ids),
        }


@dataclass(frozen=True, slots=True)
class RepositoryStageOutput:
    """Repository metadata operation result with explicit state."""

    repo_data: dict[str, Any]
    available_data: dict[str, Any]
    filter_pairs: tuple[str, ...]
    repo_file_exists: bool
    result: StageResult

    def to_dict(self) -> dict[str, Any]:
        """Return JSON-compatible repository operation output."""
        return {
            "repo_data": dict(self.repo_data),
            "available_data": dict(self.available_data),
            "filter_pairs": list(self.filter_pairs),
            "repo_file_exists": self.repo_file_exists,
            "result": self.result.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class DatasetPlanOutput:
    """Deterministic dataset planning result."""

    work_items: tuple[WorkItem, ...]
    result: StageResult

    def to_dict(self) -> dict[str, Any]:
        """Return JSON-compatible planned work items and stage result."""
        return {
            "work_items": [item.to_dict() for item in self.work_items],
            "result": self.result.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class DatasetPeriod:
    """A HistData URL period component."""

    year: str
    month: str
    url_path: str
    datemonth: str


@dataclass(frozen=True, slots=True)
class UrlPageData:
    """Fetched HistData archive page metadata."""

    html: str
    encoding: str
    bytes_length: str
    headers: dict[str, str]

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible page metadata."""
        return {
            "html": self.html,
            "page_content": self.html,
            "encoding": self.encoding,
            "bytes_length": self.bytes_length,
            "headers": dict(self.headers),
        }

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "UrlPageData":
        """Create page metadata from legacy or activity payloads."""
        return cls(
            html=str(data.get("html") or data.get("page_content") or ""),
            encoding=str(data.get("encoding", "") or ""),
            bytes_length=str(data.get("bytes_length", "") or ""),
            headers={
                str(key): str(value)
                for key, value in dict(data.get("headers") or {}).items()
            },
        )


@dataclass(frozen=True, slots=True)
class UrlFormMetadata:
    """Parsed HistData download form metadata."""

    data_tk: str
    data_date: str
    data_datemonth: str
    data_format: str
    data_timeframe: str
    data_fxpair: str
    encoding: str
    bytes_length: str

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible form metadata."""
        return {
            "data_tk": self.data_tk,
            "data_date": self.data_date,
            "data_datemonth": self.data_datemonth,
            "data_format": self.data_format,
            "data_timeframe": self.data_timeframe,
            "data_fxpair": self.data_fxpair,
            "encoding": self.encoding,
            "bytes_length": self.bytes_length,
        }


@dataclass(frozen=True, slots=True)
class ArchiveDownloadResult:
    """A downloaded or reused archive artifact."""

    filename: str
    path: str
    size_bytes: int
    sha256: str
    reused_existing: bool = False
    artifact_kind: str = "zip"

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible archive metadata."""
        return {
            "filename": self.filename,
            "path": self.path,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "reused_existing": self.reused_existing,
            "artifact_kind": self.artifact_kind,
        }


@dataclass(frozen=True, slots=True)
class ArchiveExtractionResult:
    """An extracted or reused CSV/XLSX artifact."""

    filename: str
    path: str
    size_bytes: int
    sha256: str
    reused_existing: bool = False
    zip_deleted: bool = False

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible extraction metadata."""
        return {
            "filename": self.filename,
            "path": self.path,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "reused_existing": self.reused_existing,
            "zip_deleted": self.zip_deleted,
        }


@dataclass(frozen=True, slots=True)
class CacheBuildResult:
    """A built or validated Polars cache artifact."""

    filename: str
    path: str
    size_bytes: int
    sha256: str
    line_count: int
    start: str
    end: str
    timeframe: str
    schema: dict[str, str]
    reused_existing: bool = False

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible cache metadata."""
        return {
            "filename": self.filename,
            "path": self.path,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "line_count": self.line_count,
            "start": self.start,
            "end": self.end,
            "timeframe": self.timeframe,
            "schema": dict(self.schema),
            "reused_existing": self.reused_existing,
        }


RecordTransformer = Callable[[Record], Record]
RecordAction = Callable[[Record], None]
NoArgBool = Callable[[], bool]
LineSink = Callable[[list[str]], None]
RepositoryFetcher = Callable[[str], Mapping[str, Any]]
UrlPageFetcher = Callable[[str, int], UrlPageData | Mapping[str, Any]]
ArchivePoster = Callable[..., Any]


def validate_url_work_item(
    work_item: WorkItem,
    *,
    args: Mapping[str, Any],
    scrape_record_info: RecordTransformer | None = None,
    check_for_valid_download: RecordAction | None = None,
    fetch_page_data: UrlPageFetcher | None = None,
    repo_validation_needed: NoArgBool | None = None,
    set_repo_datum: RecordAction | None = None,
) -> ActivityStageOutput:
    """Validate one HistData URL with explicit repository hooks."""
    record = _record_from_work_item(work_item)
    updated = _work_item_from_record(record, work_item)
    try:
        if record.status == WorkStatus.URL_NEW.value:
            if scrape_record_info is not None:
                record = scrape_record_info(record)
                if check_for_valid_download is None:
                    _check_form_token(record.data_tk)
                else:
                    check_for_valid_download(record)
                updated = _work_item_from_record(record, work_item)
            else:
                page_fetcher = fetch_page_data or fetch_histdata_page_data
                page_data = page_fetcher(
                    record.url,
                    _requests_timeout(args),
                )
                metadata = parse_histdata_form_metadata(page_data)
                updated = apply_form_metadata_to_work_item(
                    work_item,
                    metadata,
                )
                record = _record_from_work_item(updated)

            if (
                repo_validation_needed is not None
                and repo_validation_needed()
                and set_repo_datum is not None
            ):
                set_repo_datum(record)

            record.status = WorkStatus.URL_VALID.value
            record.write_memento_file(base_dir=_default_download_dir(args))
            updated = _work_item_from_record(record, updated)

        return _activity_output(
            updated,
            stage="validate_url",
            status=updated.status,
            metrics={
                "forward": True,
                "encoding": updated.encoding,
                "bytes_length": updated.bytes_length,
            },
        )
    except (HistDataNoDataError, ValueError) as err:
        record.status = WorkStatus.URL_NO_REPO_DATA.value
        record.write_memento_file(base_dir=_default_download_dir(args))
        updated = _work_item_from_record(record, work_item)
        return _activity_output(
            updated,
            stage="validate_url",
            status=WorkStatus.URL_NO_REPO_DATA,
            forward=False,
            metrics={"forward": False, "missing_repo_data": True},
            message=(
                "HistData has no downloadable archive for this URL."
                if isinstance(err, ValueError)
                else err.message
            ),
        )
    except UrlValidationError as err:
        status = WorkStatus.RETRIED if err.retryable else WorkStatus.FAILED
        updated = _work_item_from_record(record, work_item).with_status(status)
        return _activity_output(
            updated,
            stage="validate_url",
            status=status,
            forward=False,
            failure=failure_info_from_exception(
                err, detail={"url": record.url}
            ),
            metrics={
                "forward": False,
                "retryable": err.retryable,
                "failed": not err.retryable,
            },
            message=err.message,
        )
    except Exception as err:
        updated = _work_item_from_record(record, work_item).with_status(
            WorkStatus.FAILED
        )
        return _activity_output(
            updated,
            stage="validate_url",
            status=WorkStatus.FAILED,
            forward=False,
            failure=failure_info_from_exception(
                err,
                default_code="URL_VALIDATION_FAILED",
                detail={"url": record.url},
            ),
            metrics={"forward": False, "failed": True},
            message=str(err),
        )


def fetch_histdata_page_data(
    url: str,
    timeout: int,
    *,
    headers: Mapping[str, str] | None = None,
    request_get: Callable[..., Any] | None = None,
) -> UrlPageData:
    """Fetch a HistData archive page with invocation-local headers."""
    local_headers = dict(headers or {})
    get = request_get or requests.get
    kwargs: dict[str, Any] = {"timeout": timeout}
    if local_headers:
        kwargs["headers"] = local_headers

    try:
        response = get(url, **kwargs)
        raise_for_status = getattr(response, "raise_for_status", None)
        if callable(raise_for_status):
            raise_for_status()
    except requests.RequestException as err:
        raise UrlValidationError(
            "URL_FETCH_RETRYABLE",
            str(err),
            retryable=True,
            detail={"url": url},
        ) from err
    except OSError as err:
        raise UrlValidationError(
            "URL_FETCH_RETRYABLE",
            str(err),
            retryable=True,
            detail={"url": url},
        ) from err

    response_headers = {
        str(key): str(value)
        for key, value in dict(getattr(response, "headers", {}) or {}).items()
    }
    encoding = _required_header(response_headers, "Content-Encoding", url=url)
    bytes_length = _required_header(
        response_headers,
        "Content-Length",
        url=url,
    )
    if not bytes_length.isdigit():
        raise UrlValidationError(
            "MALFORMED_HEADERS",
            "HistData response Content-Length header is not numeric.",
            detail={"url": url, "content_length": bytes_length},
        )

    return UrlPageData(
        html=_response_html(response),
        encoding=encoding,
        bytes_length=bytes_length,
        headers=response_headers,
    )


def parse_histdata_form_metadata(
    page_data: UrlPageData | Mapping[str, Any],
) -> UrlFormMetadata:
    """Parse HistData download form values from a fetched archive page."""
    page = (
        page_data
        if isinstance(page_data, UrlPageData)
        else UrlPageData.from_mapping(page_data)
    )
    soup = BeautifulSoup(page.html, "html.parser")
    form = soup.find("form", id="file_down")
    if form is None:
        raise HistDataNoDataError(
            "HistData page does not include a download form.",
        )

    values = {
        key: _form_value(form, key)
        for key in (
            "tk",
            "date",
            "datemonth",
            "platform",
            "timeframe",
            "fxpair",
        )
    }
    if not values["tk"]:
        raise HistDataNoDataError(
            "HistData page does not include a download token.",
        )

    missing = sorted(key for key, value in values.items() if not value)
    if missing:
        raise UrlValidationError(
            "MALFORMED_FORM",
            "HistData download form is missing required fields.",
            detail={"missing_fields": ",".join(missing)},
        )

    return UrlFormMetadata(
        data_tk=values["tk"],
        data_date=values["date"],
        data_datemonth=values["datemonth"],
        data_format=values["platform"],
        data_timeframe=values["timeframe"],
        data_fxpair=values["fxpair"],
        encoding=page.encoding,
        bytes_length=page.bytes_length,
    )


def apply_form_metadata_to_work_item(
    work_item: WorkItem,
    metadata: UrlFormMetadata,
) -> WorkItem:
    """Return a validated work item with parsed form metadata applied."""
    return replace(
        work_item,
        status=WorkStatus.URL_VALID,
        status_text="",
        encoding=metadata.encoding,
        bytes_length=metadata.bytes_length,
        data_tk=metadata.data_tk,
        data_date=metadata.data_date,
        data_datemonth=metadata.data_datemonth,
        data_format=metadata.data_format,
        data_timeframe=metadata.data_timeframe,
        data_fxpair=metadata.data_fxpair,
    )


def _check_form_token(token: str) -> None:
    if not token:
        raise HistDataNoDataError(
            "HistData page does not include a download token.",
        )


def download_archive_work_item(
    work_item: WorkItem,
    *,
    args: Mapping[str, Any],
    download_file: RecordAction | None = None,
    post_archive: ArchivePoster | None = None,
) -> ActivityStageOutput:
    """Download one ZIP archive through an explicit work item."""
    record = _record_from_work_item(work_item)
    try:
        _ensure_record_data_dir(record, args)
        existing = existing_archive_artifact(record)
        if existing is not None:
            record.status = _status_for_archive_kind(
                existing.artifact_kind
            ).value
            record.write_memento_file(base_dir=_default_download_dir(args))
            updated = _work_item_from_record(record, work_item)
            return _activity_output(
                updated,
                stage="download_archive",
                status=updated.status,
                artifacts=_artifact_refs_for_record(
                    record,
                    existing.artifact_kind,
                ),
                metrics={
                    "forward": True,
                    "decision": "reuse_existing",
                    "existing_artifact_kind": existing.artifact_kind,
                    "filename": existing.filename,
                    "size_bytes": existing.size_bytes,
                    "sha256": existing.sha256,
                },
                message="Existing local archive artifact reused.",
            )

        should_download = WorkStatus.URL_VALID.value in record.status or bool(
            args.get("from_api")
        )
        if should_download:
            if download_file is None:
                download_result = download_histdata_archive_to_record(
                    record,
                    timeout=_requests_timeout(args),
                    post_archive=post_archive,
                )
            else:
                download_file(record)
                download_result = archive_download_result_for_record(record)

            record.status = WorkStatus.CSV_ZIP.value
            record.write_memento_file(base_dir=_default_download_dir(args))
            updated = _work_item_from_record(record, work_item)
            return _activity_output(
                updated,
                stage="download_archive",
                status=updated.status,
                artifacts=_artifact_refs_for_record(record, "zip"),
                metrics={
                    "forward": True,
                    "decision": "downloaded",
                    "filename": download_result.filename,
                    "size_bytes": download_result.size_bytes,
                    "sha256": download_result.sha256,
                },
            )

        updated = _work_item_from_record(record, work_item)
        return _activity_output(
            updated,
            stage="download_archive",
            status=updated.status,
            metrics={
                "forward": True,
                "decision": "skipped_not_ready",
            },
        )
    except (ArchiveDownloadError, KeyError, OSError, zipfile.BadZipFile) as err:
        record.delete_momento_file()
        failure = _archive_download_failure(err, record)
        status = WorkStatus.RETRIED if failure.retryable else WorkStatus.FAILED
        failed = _work_item_from_record(record, work_item).with_status(
            status,
        )
        return _activity_output(
            failed,
            stage="download_archive",
            status=status,
            forward=False,
            failure=failure,
            metrics={
                "forward": False,
                "retryable": failure.retryable,
            },
            message=failure.message,
        )
    except Exception as err:
        record.delete_momento_file()
        failed = _work_item_from_record(record, work_item).with_status(
            WorkStatus.FAILED
        )
        failure = failure_info_from_exception(
            err,
            default_code="ARCHIVE_DOWNLOAD_FAILED",
            detail={"url": record.url, "data_dir": record.data_dir},
        )
        return _activity_output(
            failed,
            stage="download_archive",
            status=WorkStatus.FAILED,
            forward=False,
            failure=failure,
            metrics={"forward": False, "retryable": False},
            message=failure.message,
        )


def download_histdata_archive_to_record(
    record: Record,
    *,
    timeout: int,
    post_headers: Mapping[str, str] | None = None,
    post_archive: ArchivePoster | None = None,
) -> ArchiveDownloadResult:
    """POST for a HistData ZIP and atomically persist it on success."""
    response = post_histdata_archive(
        record,
        timeout=timeout,
        post_headers=post_headers,
        post_archive=post_archive,
    )
    filename = archive_filename_from_response(response)
    content = _response_content_bytes(response)
    target_path = atomic_write_zip_archive(
        Path(record.data_dir),
        filename,
        content,
        work_id=record.url,
    )
    record.zip_filename = target_path.name
    return archive_download_result_for_path(target_path)


def post_histdata_archive(
    record: Record,
    *,
    timeout: int,
    post_headers: Mapping[str, str] | None = None,
    post_archive: ArchivePoster | None = None,
) -> Any:
    """Submit the HistData archive download form."""
    _validate_archive_request(record)
    headers = _archive_post_headers(record.url, post_headers)
    post = post_archive or requests.post
    try:
        response = post(
            "http://www.histdata.com/get.php",
            data={
                "tk": record.data_tk,
                "date": record.data_date,
                "datemonth": record.data_datemonth,
                "platform": record.data_format,
                "timeframe": record.data_timeframe,
                "fxpair": record.data_fxpair,
            },
            headers=headers,
            timeout=timeout,
        )
        raise_for_status = getattr(response, "raise_for_status", None)
        if callable(raise_for_status):
            raise_for_status()
        return response
    except requests.RequestException as err:
        raise ArchiveDownloadError(
            "ARCHIVE_NETWORK_ERROR",
            str(err),
            retryable=True,
            detail={"url": record.url},
        ) from err
    except OSError as err:
        raise ArchiveDownloadError(
            "ARCHIVE_NETWORK_ERROR",
            str(err),
            retryable=True,
            detail={"url": record.url},
        ) from err


def archive_filename_from_response(response: Any) -> str:
    """Return a safe ZIP filename from Content-Disposition."""
    headers = dict(getattr(response, "headers", {}) or {})
    content_disposition = ""
    for key, value in headers.items():
        if str(key).lower() == "content-disposition":
            content_disposition = str(value)
            break
    if not content_disposition:
        raise ArchiveDownloadError(
            "INVALID_CONTENT_DISPOSITION",
            "Archive response is missing Content-Disposition.",
            retryable=True,
        )

    message = Message()
    message["Content-Disposition"] = content_disposition
    filename = message.get_param(
        "filename",
        header="content-disposition",
    )
    filename = Path(str(filename or "")).name
    if not filename:
        raise ArchiveDownloadError(
            "INVALID_CONTENT_DISPOSITION",
            "Archive response does not include a filename.",
            retryable=True,
            detail={"content_disposition": content_disposition},
        )
    return filename


def atomic_write_zip_archive(
    data_dir: Path,
    filename: str,
    content: bytes,
    *,
    work_id: str,
) -> Path:
    """Write a ZIP through a temp file, validate it, then rename."""
    target_path = data_dir / filename
    temp_path = target_path.with_name(
        f".{target_path.name}.{derive_work_id(work_id).removeprefix('work-')}.tmp"
    )
    try:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path.write_bytes(content)
        _validate_zip_payload(temp_path)
        temp_path.replace(target_path)
        return target_path
    except zipfile.BadZipFile as err:
        _unlink_path(temp_path)
        raise ArchiveDownloadError(
            "INVALID_ZIP_PAYLOAD",
            str(err),
            retryable=True,
            detail={"path": str(temp_path)},
        ) from err
    except OSError as err:
        _unlink_path(temp_path)
        raise ArchiveDownloadError(
            "ARCHIVE_FILESYSTEM_ERROR",
            str(err),
            retryable=False,
            detail={"path": str(target_path)},
        ) from err


def archive_download_result_for_record(record: Record) -> ArchiveDownloadResult:
    """Return archive metadata for the ZIP path stored on a record."""
    if not record.zip_filename:
        raise ArchiveDownloadError(
            "INVALID_CONTENT_DISPOSITION",
            "Archive download did not set a ZIP filename.",
            retryable=True,
            detail={"url": record.url},
        )
    return archive_download_result_for_path(
        Path(record.data_dir, record.zip_filename)
    )


def archive_download_result_for_path(
    path: Path,
    *,
    reused_existing: bool = False,
    artifact_kind: str = "zip",
) -> ArchiveDownloadResult:
    """Return size/hash metadata for an archive artifact path."""
    return ArchiveDownloadResult(
        filename=path.name,
        path=str(path),
        size_bytes=path.stat().st_size,
        sha256=_file_sha256(path),
        reused_existing=reused_existing,
        artifact_kind=artifact_kind,
    )


def existing_archive_artifact(record: Record) -> ArchiveDownloadResult | None:
    """Return the first existing ZIP/CSV/cache artifact for a record."""
    if not record.data_dir:
        return None
    for artifact_kind, filename in (
        ("zip", record.zip_filename),
        ("csv", record.csv_filename),
        ("cache", record.cache_filename),
    ):
        if not filename:
            continue
        path = Path(record.data_dir, filename)
        if path.exists():
            return archive_download_result_for_path(
                path,
                reused_existing=True,
                artifact_kind=artifact_kind,
            )
    return None


def existing_extraction_artifact(
    record: Record,
    *,
    zip_persist: bool,
) -> ArchiveExtractionResult | None:
    """Return an existing CSV/XLSX artifact and safe ZIP cleanup outcome."""
    if not record.data_dir or not record.csv_filename:
        return None

    path = Path(record.data_dir, record.csv_filename)
    if not path.exists():
        return None

    return archive_extraction_result_for_path(
        path,
        reused_existing=True,
        zip_deleted=_delete_zip_after_extraction(
            record,
            zip_persist=zip_persist,
        ),
    )


def extract_csv_work_item(
    work_item: WorkItem,
    *,
    args: Mapping[str, Any],
) -> ActivityStageOutput:
    """Extract one CSV/XLSX payload through an explicit work item."""
    record = _record_from_work_item(work_item)
    try:
        _ensure_record_data_dir(record, args)
        zip_persist = _zip_persist_enabled(args, record)
        existing = existing_extraction_artifact(
            record,
            zip_persist=zip_persist,
        )
        if existing is not None:
            record.status = WorkStatus.CSV_FILE.value
            record.write_memento_file(base_dir=_default_download_dir(args))
            updated = _work_item_from_record(record, work_item)
            return _activity_output(
                updated,
                stage="extract_csv",
                status=updated.status,
                artifacts=_artifact_refs_for_record(record, "csv"),
                metrics={
                    "forward": True,
                    "decision": "reuse_existing",
                    **existing.to_dict(),
                },
                message="Existing CSV/XLSX artifact reused.",
            )

        if WorkStatus.CSV_ZIP.value in record.status:
            extraction = extract_archive_to_record(
                record,
                zip_persist=zip_persist,
            )
            record.status = WorkStatus.CSV_FILE.value
            record.write_memento_file(base_dir=_default_download_dir(args))
            updated = _work_item_from_record(record, work_item)
            return _activity_output(
                updated,
                stage="extract_csv",
                status=updated.status,
                artifacts=_artifact_refs_for_record(record, "csv"),
                metrics={
                    "forward": True,
                    "decision": (
                        "reuse_existing"
                        if extraction.reused_existing
                        else "extracted"
                    ),
                    **extraction.to_dict(),
                },
            )

        updated = _work_item_from_record(record, work_item)
        return _activity_output(
            updated,
            stage="extract_csv",
            status=updated.status,
            metrics={
                "forward": True,
                "decision": "skipped_not_ready",
            },
        )
    except (ArchiveExtractionError, OSError, zipfile.BadZipFile) as err:
        record.delete_momento_file()
        failure = _archive_extraction_failure(err, record)
        status = WorkStatus.RETRIED if failure.retryable else WorkStatus.FAILED
        failed = _work_item_from_record(record, work_item).with_status(status)
        return _activity_output(
            failed,
            stage="extract_csv",
            status=status,
            forward=False,
            failure=failure,
            metrics={
                "forward": False,
                "retryable": failure.retryable,
            },
            message=failure.message,
        )


def extract_archive_to_record(
    record: Record,
    *,
    zip_persist: bool,
) -> ArchiveExtractionResult:
    """Extract the single CSV/XLSX member from a ZIP into record.data_dir."""
    _validate_extraction_request(record)
    zip_path = Path(record.data_dir, record.zip_filename)
    if not zip_path.exists():
        raise ArchiveExtractionError(
            "ARCHIVE_NOT_FOUND",
            "Archive extraction source ZIP does not exist.",
            retryable=False,
            detail={"path": str(zip_path), "url": record.url},
        )

    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            member = _single_data_member(zip_ref, zip_path)
            filename = _safe_data_member_filename(member)
            record.csv_filename = filename
            target_path = Path(record.data_dir, filename)
            reused_existing = target_path.exists()
            if not reused_existing:
                atomic_extract_archive_member(
                    zip_ref,
                    member,
                    target_path,
                    work_id=record.url or str(zip_path),
                )
    except zipfile.BadZipFile as err:
        raise ArchiveExtractionError(
            "INVALID_ARCHIVE_PAYLOAD",
            str(err),
            retryable=False,
            detail={"path": str(zip_path)},
        ) from err
    except (KeyError, RuntimeError) as err:
        raise ArchiveExtractionError(
            "INVALID_ARCHIVE_PAYLOAD",
            str(err),
            retryable=False,
            detail={"path": str(zip_path)},
        ) from err
    except OSError as err:
        raise ArchiveExtractionError(
            "EXTRACTION_FILESYSTEM_ERROR",
            str(err),
            retryable=False,
            detail={"path": str(zip_path)},
        ) from err

    zip_deleted = _delete_zip_after_extraction(
        record,
        zip_persist=zip_persist,
    )
    return archive_extraction_result_for_path(
        target_path,
        reused_existing=reused_existing,
        zip_deleted=zip_deleted,
    )


def archive_extraction_result_for_path(
    path: Path,
    *,
    reused_existing: bool = False,
    zip_deleted: bool = False,
) -> ArchiveExtractionResult:
    """Return extracted artifact metadata for a CSV/XLSX path."""
    return ArchiveExtractionResult(
        filename=path.name,
        path=str(path),
        size_bytes=path.stat().st_size,
        sha256=_file_sha256(path),
        reused_existing=reused_existing,
        zip_deleted=zip_deleted,
    )


def atomic_extract_archive_member(
    zip_ref: zipfile.ZipFile,
    member: str,
    target_path: Path,
    *,
    work_id: str,
) -> Path:
    """Extract one ZIP member through a temp file, then rename atomically."""
    temp_path = target_path.with_name(
        f".{target_path.name}.{derive_work_id(work_id).removeprefix('work-')}.tmp"
    )
    try:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        with zip_ref.open(member, "r") as source, temp_path.open("wb") as sink:
            shutil.copyfileobj(source, sink)
        temp_path.replace(target_path)
        return target_path
    except (OSError, zipfile.BadZipFile, RuntimeError, KeyError):
        _unlink_path(temp_path)
        raise


def build_cache_work_item(
    work_item: WorkItem,
    *,
    args: Mapping[str, Any],
    download_file: RecordAction | None = None,
) -> ActivityStageOutput:
    """Build or validate one Polars cache through an explicit work item."""
    record = _record_from_work_item(work_item)
    try:
        _ensure_record_data_dir(record, args)
        if not _supports_cache(record):
            return _activity_output(
                _work_item_from_record(record, work_item),
                stage="build_cache",
                status=WorkStatus.SKIPPED,
                metrics={
                    "forward": True,
                    "cache_supported": False,
                    "decision": "skipped_unsupported",
                    "data_format": record.data_format,
                    "timeframe": record.data_timeframe,
                },
            )

        cache_path = Path(record.data_dir, CACHE_FILENAME)
        created = False
        if cache_path.exists():
            record.cache_filename = CACHE_FILENAME
            cache_result = cache_build_result_for_record(
                record,
                reused_existing=True,
            )
        else:
            if not _cache_source_artifact_exists(record):
                if download_file is None:
                    raise CacheBuildError(
                        "CACHE_SOURCE_NOT_FOUND",
                        "Cache build requires a local ZIP or CSV source file.",
                        retryable=False,
                        detail={
                            "data_dir": record.data_dir,
                            "zip_filename": record.zip_filename,
                            "csv_filename": record.csv_filename,
                        },
                    )
                download_file(record)

            create_cache_file(record, args)
            created = True
            cache_result = cache_build_result_for_record(record)

        record.status = WorkStatus.CACHE_READY.value
        record.write_memento_file(base_dir=_default_download_dir(args))
        updated = _work_item_from_record(record, work_item)
        metrics = {
            "forward": True,
            "decision": "built" if created else "reuse_existing",
            "cache_created": created,
            "cache_line_count": cache_result.line_count,
            "cache_start": cache_result.start,
            "cache_end": cache_result.end,
            "timeframe": cache_result.timeframe,
            "schema": cache_result.schema,
            **cache_result.to_dict(),
        }
        return _activity_output(
            updated,
            stage="build_cache",
            status=WorkStatus.CACHE_READY,
            artifacts=_artifact_refs_for_record(record, "cache"),
            metrics=metrics,
        )
    except (CacheBuildError, OSError, ValueError) as err:
        record.delete_momento_file()
        failure = _cache_build_failure(err, record)
        status = WorkStatus.RETRIED if failure.retryable else WorkStatus.FAILED
        failed = _work_item_from_record(record, work_item).with_status(status)
        return _activity_output(
            failed,
            stage="build_cache",
            status=status,
            forward=False,
            failure=failure,
            metrics={
                "forward": False,
                "retryable": failure.retryable,
            },
            message=failure.message,
        )
    except SystemExit as err:
        record.delete_momento_file()
        failure = failure_info_from_exception(
            err,
            default_code="CACHE_BUILD_INTERRUPTED",
            detail={
                "data_dir": record.data_dir,
                "zip_filename": record.zip_filename,
                "csv_filename": record.csv_filename,
            },
        )
        failed = _work_item_from_record(record, work_item).with_status(
            WorkStatus.FAILED
        )
        return _activity_output(
            failed,
            stage="build_cache",
            status=WorkStatus.FAILED,
            forward=False,
            failure=failure,
            metrics={"forward": False, "retryable": False},
            message=failure.message,
        )


def cache_build_result_for_record(
    record: Record,
    *,
    reused_existing: bool = False,
) -> CacheBuildResult:
    """Validate a cache artifact and return bounded cache metadata."""
    if not record.data_dir:
        raise CacheBuildError(
            "INVALID_CACHE_REQUEST",
            "Cache validation requires a data directory.",
            retryable=False,
        )
    cache_filename = record.cache_filename or CACHE_FILENAME
    cache_path = Path(record.data_dir, cache_filename)
    if not cache_path.exists():
        raise CacheBuildError(
            "CACHE_ARTIFACT_NOT_FOUND",
            "Cache artifact does not exist.",
            retryable=False,
            detail={"path": str(cache_path)},
        )

    try:
        frame = read_polars_cache(cache_path)
    except ValueError as err:
        raise CacheBuildError(
            "CACHE_INVALID_LEGACY_PAYLOAD",
            str(err),
            retryable=False,
            detail={"path": str(cache_path)},
        ) from err
    except OSError as err:
        raise CacheBuildError(
            "CACHE_FILESYSTEM_ERROR",
            str(err),
            retryable=False,
            detail={"path": str(cache_path)},
        ) from err

    if getattr(frame, "height", 0) < 1:
        raise CacheBuildError(
            "CACHE_EMPTY",
            "Cache artifact contains no rows.",
            retryable=False,
            detail={"path": str(cache_path)},
        )

    line_count = int(frame.height)
    record.cache_filename = cache_filename
    record.cache_line_count = str(line_count)
    record.cache_start = str(_extract_single_value(frame, 0, "datetime"))
    record.cache_end = str(
        _extract_single_value(frame, frame.height - 1, "datetime")
    )
    return CacheBuildResult(
        filename=cache_path.name,
        path=str(cache_path),
        size_bytes=cache_path.stat().st_size,
        sha256=_file_sha256(cache_path),
        line_count=line_count,
        start=record.cache_start,
        end=record.cache_end,
        timeframe=record.data_timeframe,
        schema=_cache_schema(frame),
        reused_existing=reused_existing,
    )


def merge_cache_work_items(
    work_items: Sequence[WorkItem],
    *,
    return_type: str = "polars",
    materialize: bool = True,
) -> MergeStageOutput:
    """Merge cache artifacts from explicit work items."""
    mergeable = _mergeable_cache_items(work_items)
    sets_to_merge = _collate_cache_sets(mergeable)
    merge_sets: list[CacheMergeSetSummary] = []
    for cache_set in sets_to_merge:
        ordered_items = order_cache_items(cache_set["records"])
        cache_set["records"] = ordered_items
        merge_summary = summarize_cache_merge_set(
            timeframe=str(cache_set["timeframe"]),
            pair=str(cache_set["pair"]),
            work_items=ordered_items,
        )
        merge_sets.append(merge_summary)
        if materialize:
            cache_set["data"] = merge_cache_items(
                ordered_items,
                return_type=return_type,
                already_ordered=True,
            )

    if not materialize:
        data = None
    elif len(sets_to_merge) == 1:
        data = sets_to_merge[0]["data"]
    else:
        data = sets_to_merge

    merge_set_metrics: list[JSONValue] = [
        merge_set.to_dict() for merge_set in merge_sets
    ]
    result = StageResult(
        work_id=derive_work_id(
            "merge_cache", *(item.work_id for item in mergeable)
        ),
        stage="merge_cache",
        status=WorkStatus.COMPLETED if sets_to_merge else WorkStatus.SKIPPED,
        artifacts=tuple(
            artifact
            for merge_set in merge_sets
            for artifact in merge_set.artifacts
        ),
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
                    "materialized": materialize,
                },
            ),
        ),
        metrics={
            "record_count": len(mergeable),
            "set_count": len(sets_to_merge),
            "materialized": materialize,
            "sets": merge_set_metrics,
        },
    )
    return MergeStageOutput(
        data=data,
        result=result,
        merge_sets=tuple(merge_sets),
    )


def merge_cache_items(
    work_items: Sequence[WorkItem],
    *,
    return_type: str,
    already_ordered: bool = False,
) -> Any:
    """Merge one pair/timeframe cache set into the requested API type."""
    import polars as pl

    ordered_items = (
        tuple(work_items) if already_ordered else order_cache_items(work_items)
    )
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


def order_cache_items(
    work_items: Sequence[WorkItem],
) -> tuple[WorkItem, ...]:
    """Return cache work items in stable cache-start order."""
    return tuple(sorted(work_items, key=lambda item: item.cache_start))


def summarize_cache_merge_set(
    *,
    timeframe: str,
    pair: str,
    work_items: Sequence[WorkItem],
) -> CacheMergeSetSummary:
    """Return bounded metadata for a cache merge set."""
    artifacts = tuple(
        _cache_artifact_ref_for_work_item(item) for item in work_items
    )
    line_count = sum(_coerce_int(item.cache_line_count) for item in work_items)
    starts = [item.cache_start for item in work_items if item.cache_start]
    ends = [item.cache_end for item in work_items if item.cache_end]
    return CacheMergeSetSummary(
        timeframe=timeframe,
        pair=pair,
        record_count=len(work_items),
        line_count=line_count,
        start=starts[0] if starts else "",
        end=ends[-1] if ends else "",
        artifacts=artifacts,
        work_ids=tuple(item.work_id for item in work_items),
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
            else:
                raise FileNotFoundError(
                    "Influx import requires a local Polars cache artifact."
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


def dataset_plan_stage(
    *,
    start_yearmonth: str | None,
    end_yearmonth: str | None,
    formats: Iterable[Any],
    pairs: Iterable[Any] | None,
    timeframes: Iterable[Any],
    default_download_dir: str = "",
    base_url: str = DEFAULT_HISTDATA_BASE_URL,
    current_yearmonth: str | None = None,
    zip_persist: bool = False,
) -> DatasetPlanOutput:
    """Plan explicit dataset work items without queues or side effects."""
    formats_input = tuple(formats or ())
    pairs_input = tuple(pairs or ()) if pairs is not None else None
    timeframes_input = tuple(timeframes or ())
    work_items = plan_dataset_work_items(
        start_yearmonth=start_yearmonth,
        end_yearmonth=end_yearmonth,
        formats=formats_input,
        pairs=pairs_input,
        timeframes=timeframes_input,
        default_download_dir=default_download_dir,
        base_url=base_url,
        current_yearmonth=current_yearmonth,
        zip_persist=zip_persist,
    )
    normalized_formats = normalize_dataset_formats(formats_input)
    normalized_timeframes = normalize_dataset_timeframes(timeframes_input)
    normalized_pairs = normalize_dataset_pairs(pairs_input)
    resolved_current = current_yearmonth or get_current_datemonth_gmt_minus5()
    result = StageResult(
        work_id=derive_work_id(
            "dataset_plan",
            _coerce_yearmonth(start_yearmonth) or "",
            _coerce_yearmonth(end_yearmonth) or "",
            *normalized_formats,
            *normalized_timeframes,
            *normalized_pairs,
        ),
        stage="dataset_plan",
        status=WorkStatus.COMPLETED,
        events=(
            StatusEvent(
                status=WorkStatus.COMPLETED,
                stage="dataset_plan",
                message="Dataset work items planned.",
                metadata={"work_item_count": len(work_items)},
            ),
        ),
        metrics={
            "work_item_count": len(work_items),
            "pairs": list(normalized_pairs),
            "formats": list(normalized_formats),
            "timeframes": list(normalized_timeframes),
            "start_yearmonth": _coerce_yearmonth(start_yearmonth) or "",
            "end_yearmonth": _coerce_yearmonth(end_yearmonth) or "",
            "current_yearmonth": resolved_current,
        },
    )
    return DatasetPlanOutput(work_items=work_items, result=result)


def plan_dataset_work_items(
    *,
    start_yearmonth: str | None,
    end_yearmonth: str | None,
    formats: Iterable[Any],
    pairs: Iterable[Any] | None,
    timeframes: Iterable[Any],
    default_download_dir: str = "",
    base_url: str = DEFAULT_HISTDATA_BASE_URL,
    current_yearmonth: str | None = None,
    zip_persist: bool = False,
) -> tuple[WorkItem, ...]:
    """Return deterministic URL work items for a HistData request."""
    formats_input = tuple(formats or ())
    pairs_input = tuple(pairs or ()) if pairs is not None else None
    timeframes_input = tuple(timeframes or ())
    resolved_current = current_yearmonth or get_current_datemonth_gmt_minus5()
    start = _coerce_yearmonth(start_yearmonth)
    end = _coerce_yearmonth(end_yearmonth)
    if not start and not end:
        start = "200001"
        end = resolved_current

    planned: list[WorkItem] = []
    normalized_pairs = normalize_dataset_pairs(pairs_input)
    for csv_format, timeframe in valid_dataset_dimensions(
        formats_input,
        timeframes_input,
    ):
        for pair in normalized_pairs:
            for period in iter_dataset_periods(
                start,
                end,
                timeframe=timeframe,
                current_yearmonth=resolved_current,
            ):
                planned.append(
                    dataset_work_item(
                        csv_format=csv_format,
                        timeframe=timeframe,
                        pair=pair,
                        period=period,
                        default_download_dir=default_download_dir,
                        base_url=base_url,
                        zip_persist=zip_persist,
                    )
                )
    return tuple(planned)


def plan_dataset_urls(
    *,
    start_yearmonth: str | None,
    end_yearmonth: str | None,
    formats: Iterable[Any],
    pairs: Iterable[Any] | None,
    timeframes: Iterable[Any],
    base_url: str = DEFAULT_HISTDATA_BASE_URL,
    current_yearmonth: str | None = None,
) -> tuple[str, ...]:
    """Return deterministic HistData URLs for a request."""
    return tuple(
        item.url
        for item in plan_dataset_work_items(
            start_yearmonth=start_yearmonth,
            end_yearmonth=end_yearmonth,
            formats=formats,
            pairs=pairs,
            timeframes=timeframes,
            base_url=base_url,
            current_yearmonth=current_yearmonth,
        )
    )


def valid_dataset_dimensions(
    formats: Iterable[Any],
    timeframes: Iterable[Any],
) -> tuple[tuple[str, str], ...]:
    """Return supported format/timeframe pairs in deterministic order."""
    normalized_formats = normalize_dataset_formats(formats)
    normalized_timeframes = normalize_dataset_timeframes(timeframes)
    return tuple(
        (csv_format, timeframe)
        for csv_format in normalized_formats
        for timeframe in normalized_timeframes
        if timeframe in get_valid_format_timeframes(csv_format)
    )


def normalize_dataset_formats(formats: Iterable[Any]) -> tuple[str, ...]:
    """Normalize HistData format inputs to enum values."""
    return tuple(
        sorted({_format_value(csv_format) for csv_format in formats or ()})
    )


def normalize_dataset_timeframes(timeframes: Iterable[Any]) -> tuple[str, ...]:
    """Normalize HistData timeframe inputs to enum keys."""
    return tuple(
        sorted({_timeframe_key(timeframe) for timeframe in timeframes or ()})
    )


def normalize_dataset_pairs(pairs: Iterable[Any] | None) -> tuple[str, ...]:
    """Normalize symbols to lowercase pair keys in deterministic order."""
    if pairs is None:
        return ()
    return tuple(sorted({str(pair).lower() for pair in pairs}))


def iter_dataset_periods(
    start_yearmonth: str | None,
    end_yearmonth: str | None,
    *,
    timeframe: str,
    current_yearmonth: str,
) -> tuple[DatasetPeriod, ...]:
    """Return HistData period URL components for the requested range."""
    start = _coerce_yearmonth(start_yearmonth)
    end = _coerce_yearmonth(end_yearmonth)
    if not start:
        return ()

    if not end:
        return tuple(
            _dataset_period_from_path(path)
            for path in _single_year_or_month_paths(
                timeframe,
                start,
                current_yearmonth=current_yearmonth,
            )
        )

    start_year = int(get_year_from_datemonth(start))
    start_month = int(get_month_from_datemonth(start))
    end_year = int(get_year_from_datemonth(end))
    end_month = int(get_month_from_datemonth(end))
    current_year = int(get_year_from_datemonth(current_yearmonth))
    paths: list[str] = []
    for year in range(start_year, end_year + 1):
        paths.extend(
            _year_range_paths(
                year,
                timeframe=timeframe,
                start_year=start_year,
                start_month=start_month,
                end_year=end_year,
                end_month=end_month,
                current_year=current_year,
            )
        )
    return tuple(_dataset_period_from_path(path) for path in paths)


def dataset_work_item(
    *,
    csv_format: str,
    timeframe: str,
    pair: str,
    period: DatasetPeriod,
    default_download_dir: str = "",
    base_url: str = DEFAULT_HISTDATA_BASE_URL,
    zip_persist: bool = False,
) -> WorkItem:
    """Build one deterministic planned dataset work item."""
    normalized_format = _format_value(csv_format)
    normalized_timeframe = _timeframe_key(timeframe)
    normalized_pair = pair.lower()
    return WorkItem(
        work_id=derive_work_id(
            "dataset_plan",
            normalized_format,
            normalized_timeframe,
            normalized_pair,
            period.year,
            period.month,
        ),
        status=WorkStatus.URL_NEW,
        url=_dataset_url(
            normalized_format,
            normalized_timeframe,
            normalized_pair,
            period,
            base_url=base_url,
        ),
        data_date=period.datemonth,
        data_year=period.year,
        data_month=period.month,
        data_datemonth=period.datemonth,
        data_format=Format(normalized_format).name,
        data_timeframe=normalized_timeframe,
        data_fxpair=normalized_pair,
        data_dir=_dataset_data_dir(
            normalized_format,
            normalized_timeframe,
            normalized_pair,
            period,
            default_download_dir=default_download_dir,
        ),
        zip_persist=str(zip_persist),
        metadata={
            "format": normalized_format,
            "timeframe": normalized_timeframe,
            "pair": normalized_pair,
            "year": period.year,
            "month": period.month,
            "datemonth": period.datemonth,
        },
    )


def repository_refresh_stage(
    *,
    repo_data: Mapping[str, Any],
    repo_file_exists: bool,
    repo_local_path: str | Path,
    repo_url: str = DEFAULT_REPOSITORY_URL,
    pairs: Iterable[str] = (),
    by: str | None = None,
    available_remote_data: bool = False,
    update_remote_data: bool = False,
    fetch_remote_repository: RepositoryFetcher | None = None,
) -> RepositoryStageOutput:
    """Refresh/list repository metadata with explicit inputs and outputs."""
    fetcher = fetch_remote_repository or fetch_repository_data_from_url
    requested_pairs = set(pairs)
    repo_path = Path(repo_local_path)
    local_exists = bool(repo_file_exists or repo_path.exists())
    working_repo = dict(repo_data)
    artifacts = _repository_artifacts(repo_path)
    remote_checked = False
    remote_refreshed = False
    local_written = False

    if local_exists and not working_repo:
        try:
            working_repo.update(read_repository_data_file(repo_path))
        except (OSError, json.JSONDecodeError) as err:
            return _repository_failure_output(
                repo_data=working_repo,
                repo_file_exists=local_exists,
                repo_local_path=repo_path,
                pairs=requested_pairs,
                by=by,
                code="REPOSITORY_READ_ERROR",
                message=str(err),
                retryable=False,
            )

    try:
        if available_remote_data or update_remote_data or not local_exists:
            remote_checked = True
            remote_repo = dict(fetcher(repo_url))
            refreshed_repo = merge_remote_repository_data(
                working_repo,
                remote_repo,
                repo_file_exists=local_exists,
            )
            remote_refreshed = refreshed_repo != working_repo
            working_repo = refreshed_repo

            if not local_exists or remote_refreshed or update_remote_data:
                artifact = write_repository_data_file(working_repo, repo_path)
                working_repo = read_repository_data_file(repo_path)
                artifacts = (artifact,)
                local_exists = True
                local_written = True
    except (SSLCertVerificationError, URLError) as err:
        return _repository_failure_output(
            repo_data=working_repo,
            repo_file_exists=local_exists,
            repo_local_path=repo_path,
            pairs=requested_pairs,
            by=by,
            code="REPOSITORY_NETWORK_ERROR",
            message=str(err),
            retryable=True,
        )
    except (OSError, ValueError, json.JSONDecodeError) as err:
        return _repository_failure_output(
            repo_data=working_repo,
            repo_file_exists=local_exists,
            repo_local_path=repo_path,
            pairs=requested_pairs,
            by=by,
            code="REPOSITORY_REFRESH_ERROR",
            message=str(err),
            retryable=False,
        )

    filter_pairs = repository_missing_pairs(working_repo, requested_pairs)
    available_data = sort_repository_data(working_repo, requested_pairs, by)
    result = StageResult(
        work_id=derive_work_id(
            "repository_refresh",
            str(repo_path),
            repo_url,
            *(sorted(requested_pairs)),
        ),
        stage="repository_refresh",
        status=WorkStatus.COMPLETED,
        artifacts=artifacts,
        events=(
            StatusEvent(
                status=WorkStatus.COMPLETED,
                stage="repository_refresh",
                message="Repository metadata refreshed/listed.",
                metadata={
                    "pair_count": len(repository_pair_data(working_repo)),
                    "remote_checked": remote_checked,
                    "remote_refreshed": remote_refreshed,
                    "local_written": local_written,
                },
            ),
        ),
        metrics={
            "available_data": available_data,
            "filter_pairs": list(filter_pairs),
            "pair_count": len(repository_pair_data(working_repo)),
            "repo_file_exists": local_exists,
            "remote_checked": remote_checked,
            "remote_refreshed": remote_refreshed,
            "local_written": local_written,
            "repo_url": repo_url,
        },
    )
    return RepositoryStageOutput(
        repo_data=working_repo,
        available_data=available_data,
        filter_pairs=tuple(filter_pairs),
        repo_file_exists=local_exists,
        result=result,
    )


def fetch_repository_data_from_url(repo_url: str) -> Mapping[str, Any]:
    """Fetch repository metadata JSON from the configured remote URL."""
    with urlopen(  # noqa:S310
        repo_url,
        context=ssl.create_default_context(cafile=certifi.where()),
    ) as repo_data:
        loaded = json.load(repo_data)
    if not isinstance(loaded, Mapping):
        raise ValueError("repository response must be a JSON object")
    return loaded


def read_repository_data_file(repo_local_path: str | Path) -> dict[str, Any]:
    """Read repository metadata from disk without mutating globals."""
    repo_path = Path(repo_local_path)
    if not repo_path.exists():
        return {}
    with repo_path.open("r", encoding="UTF-8") as source:
        loaded = json.load(source)
    if not isinstance(loaded, Mapping):
        raise ValueError("repository file must contain a JSON object")
    return dict(loaded)


def write_repository_data_file(
    repo_data: Mapping[str, Any],
    repo_local_path: str | Path,
) -> ArtifactRef:
    """Write repository metadata to disk and return an artifact reference."""
    repo_path = Path(repo_local_path)
    temp_path = deterministic_partial_path(repo_path, str(repo_path))
    create_full_path(repo_path.parent)
    hashed_data = hash_repository_data(repo_data)
    try:
        with temp_path.open("w", encoding="UTF-8") as target:
            json.dump(hashed_data, target)
        temp_path.replace(repo_path)
    except (OSError, TypeError, ValueError):
        _unlink_path(temp_path)
        raise
    return ArtifactRef(
        kind="repository",
        path=str(repo_path),
        size_bytes=repo_path.stat().st_size,
        metadata={"pair_count": len(repository_pair_data(hashed_data))},
    )


def hash_repository_data(repo_data: Mapping[str, Any]) -> dict[str, Any]:
    """Return repository metadata with refreshed hash fields."""
    clean_repo = {
        key: value
        for key, value in dict(repo_data).items()
        if key not in {"hash", "hash_utc"}
    }
    clean_repo["hash"] = hash_dict(clean_repo)
    clean_repo["hash_utc"] = get_now_utc_timestamp()
    return clean_repo


def merge_remote_repository_data(
    local_repo_data: Mapping[str, Any],
    remote_repo_data: Mapping[str, Any],
    *,
    repo_file_exists: bool,
) -> dict[str, Any]:
    """Return the repository data that should be used after remote refresh."""
    if not repo_file_exists:
        return dict(remote_repo_data)
    if not local_repo_data:
        return dict(remote_repo_data)

    local_hash = str(local_repo_data.get("hash", "") or "")
    remote_hash = str(remote_repo_data.get("hash", "") or "")
    local_time = _json_float(local_repo_data.get("hash_utc"))
    remote_time = _json_float(remote_repo_data.get("hash_utc"))
    if local_hash != remote_hash and local_time < remote_time:
        return dict(remote_repo_data)
    return dict(local_repo_data)


def repository_data_with_record(
    repo_data: Mapping[str, Any],
    record: Record,
) -> dict[str, Any]:
    """Return repository metadata updated from one validated record."""
    updated = {
        key: dict(value) if isinstance(value, Mapping) else value
        for key, value in dict(repo_data).items()
        if key not in {"hash", "hash_utc"}
    }
    datemonth = force_datemonth_if_only_year(record.data_datemonth)
    pair = record.data_fxpair.lower()
    current = updated.get(pair)
    if not isinstance(current, Mapping):
        updated[pair] = {"start": datemonth, "end": datemonth}
        return updated

    start = str(current.get("start", datemonth))
    end = str(current.get("end", datemonth))
    updated[pair] = {
        "start": datemonth if int(datemonth) < int(start) else start,
        "end": datemonth if int(datemonth) > int(end) else end,
    }
    return updated


def repository_pair_data(repo_data: Mapping[str, Any]) -> dict[str, Any]:
    """Return only pair range entries from repository metadata."""
    return {
        str(pair): dict(value)
        for pair, value in repo_data.items()
        if isinstance(value, Mapping) and "start" in value and "end" in value
    }


def repository_missing_pairs(
    repo_data: Mapping[str, Any],
    pairs: set[str],
) -> tuple[str, ...]:
    """Return requested pairs absent from repository metadata."""
    if not pairs:
        return ()
    return tuple(sorted(pairs - set(repository_pair_data(repo_data))))


def repository_validation_needed(
    args: Mapping[str, Any],
    *,
    repo_file_exists: bool,
    filter_pairs: Iterable[str] | None,
) -> bool:
    """Return whether repository coverage validation is needed."""
    return bool(
        (
            not repo_file_exists
            and bool(args.get("available_remote_data", False))
        )
        or bool(args.get("update_remote_data", False))
        or tuple(filter_pairs or ())
    )


def repository_should_create_or_update(
    args: Mapping[str, Any],
    *,
    repo_file_exists: bool,
    filter_pairs: Iterable[str] | None,
) -> bool:
    """Return whether a repo metadata file should be written."""
    return bool(
        bool(args.get("update_remote_data", False))
        or not repo_file_exists
        or tuple(filter_pairs or ())
    )


def sort_repository_data(
    repo_data: Mapping[str, Any],
    filter_pairs: set[str],
    by: str | None,
) -> dict[str, Any]:
    """Return repository pair metadata filtered and sorted."""
    filtered = filter_repository_data_by_pairs(repo_data, filter_pairs)
    match by:
        case "pair_asc" | None:
            return dict(sorted(filtered.items()))
        case "pair_dsc":
            return dict(sorted(filtered.items(), reverse=True))
        case "start_asc":
            return dict(
                sorted(filtered.items(), key=lambda pair: pair[1]["start"])
            )
        case "start_dsc":
            return dict(
                sorted(
                    filtered.items(),
                    key=lambda pair: pair[1]["start"],
                    reverse=True,
                )
            )
        case _:
            return dict(sorted(filtered.items()))


def filter_repository_data_by_pairs(
    repo_data: Mapping[str, Any],
    filter_pairs: set[str],
) -> dict[str, Any]:
    """Filter repository pair metadata by requested pairs."""
    pairs = repository_pair_data(repo_data)
    if not filter_pairs:
        return pairs
    return {
        pair: pairs[pair] for pair in sorted(set(pairs) & set(filter_pairs))
    }


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


def _required_header(
    headers: Mapping[str, str],
    name: str,
    *,
    url: str,
) -> str:
    for key, value in headers.items():
        if key.lower() == name.lower() and value:
            return value
    raise UrlValidationError(
        "MALFORMED_HEADERS",
        f"HistData response is missing {name}.",
        detail={"url": url, "missing_header": name},
    )


def _response_html(response: Any) -> str:
    text = getattr(response, "text", None)
    if isinstance(text, str) and text:
        return text

    content = getattr(response, "content", b"")
    if isinstance(content, str):
        return content
    if isinstance(content, bytes):
        encoding = str(getattr(response, "encoding", "") or "utf-8")
        return content.decode(encoding, errors="replace")
    return str(content or "")


def _form_value(form: Any, field_id: str) -> str:
    element = form.find(id=field_id)
    if element is None:
        return ""
    value = element.get("value")
    return str(value or "")


def _requests_timeout(args: Mapping[str, Any]) -> int:
    try:
        return int(args.get("requests_timeout") or args.get("timeout") or 10)
    except (TypeError, ValueError):
        return 10


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


def _ensure_record_data_dir(record: Record, args: Mapping[str, Any]) -> None:
    if record.data_dir:
        return
    default_download_dir = _default_download_dir(args)
    if default_download_dir:
        record._set_record_data_dir(default_download_dir)  # noqa:SLF001


def _status_for_archive_kind(artifact_kind: str) -> WorkStatus:
    if artifact_kind == "cache":
        return WorkStatus.CACHE_READY
    if artifact_kind == "csv":
        return WorkStatus.CSV_FILE
    return WorkStatus.CSV_ZIP


def _archive_download_failure(
    err: Exception,
    record: Record,
) -> FailureInfo:
    if isinstance(err, ArchiveDownloadError):
        return failure_info_from_exception(
            err,
            detail={"url": record.url, "data_dir": record.data_dir},
        )
    if isinstance(err, KeyError):
        return FailureInfo(
            code="INVALID_CONTENT_DISPOSITION",
            message=str(err),
            retryable=True,
            detail={"url": record.url},
        )
    if isinstance(err, zipfile.BadZipFile):
        return FailureInfo(
            code="INVALID_ZIP_PAYLOAD",
            message=str(err),
            retryable=True,
            detail={"url": record.url},
        )
    return FailureInfo(
        code="ARCHIVE_FILESYSTEM_ERROR",
        message=str(err),
        retryable=False,
        detail={"url": record.url, "data_dir": record.data_dir},
    )


def _archive_extraction_failure(
    err: Exception,
    record: Record,
) -> FailureInfo:
    if isinstance(err, ArchiveExtractionError):
        return failure_info_from_exception(
            err,
            detail={
                "url": record.url,
                "data_dir": record.data_dir,
                "zip_filename": record.zip_filename,
            },
        )
    if isinstance(err, zipfile.BadZipFile):
        return FailureInfo(
            code="INVALID_ARCHIVE_PAYLOAD",
            message=str(err),
            retryable=False,
            detail={"url": record.url, "zip_filename": record.zip_filename},
        )
    return FailureInfo(
        code="EXTRACTION_FILESYSTEM_ERROR",
        message=str(err),
        retryable=False,
        detail={
            "url": record.url,
            "data_dir": record.data_dir,
            "zip_filename": record.zip_filename,
        },
    )


def _cache_build_failure(
    err: Exception,
    record: Record,
) -> FailureInfo:
    if isinstance(err, CacheBuildError):
        return failure_info_from_exception(
            err,
            detail={
                "data_dir": record.data_dir,
                "zip_filename": record.zip_filename,
                "csv_filename": record.csv_filename,
                "cache_filename": record.cache_filename,
            },
        )
    return FailureInfo(
        code="CACHE_BUILD_FAILED",
        message=str(err),
        retryable=False,
        detail={
            "data_dir": record.data_dir,
            "zip_filename": record.zip_filename,
            "csv_filename": record.csv_filename,
            "cache_filename": record.cache_filename,
        },
    )


def _validate_archive_request(record: Record) -> None:
    missing = [
        field_name
        for field_name in (
            "data_tk",
            "data_date",
            "data_datemonth",
            "data_format",
            "data_timeframe",
            "data_fxpair",
            "data_dir",
        )
        if not getattr(record, field_name)
    ]
    if missing:
        raise ArchiveDownloadError(
            "INVALID_ARCHIVE_REQUEST",
            "Archive download request is missing required fields.",
            retryable=False,
            detail={"missing_fields": ",".join(missing), "url": record.url},
        )


def _validate_extraction_request(record: Record) -> None:
    missing = [
        field_name
        for field_name in ("data_dir", "zip_filename")
        if not getattr(record, field_name)
    ]
    if missing:
        raise ArchiveExtractionError(
            "INVALID_EXTRACTION_REQUEST",
            "Archive extraction request is missing required fields.",
            retryable=False,
            detail={"missing_fields": ",".join(missing), "url": record.url},
        )


def _single_data_member(zip_ref: zipfile.ZipFile, zip_path: Path) -> str:
    data_members = [
        name for name in zip_ref.namelist() if _is_data_archive_member(name)
    ]
    if len(data_members) != 1:
        raise ArchiveExtractionError(
            "INVALID_ARCHIVE_PAYLOAD",
            "Archive must contain exactly one CSV/XLSX data member.",
            retryable=False,
            detail={
                "path": str(zip_path),
                "data_member_count": len(data_members),
                "data_members": list(data_members[:10]),
            },
        )
    return data_members[0]


def _is_data_archive_member(member: str) -> bool:
    member_name = member.replace("\\", "/")
    if member_name.endswith("/"):
        return False
    filename = PurePosixPath(member_name).name
    return filename.lower().endswith((".csv", ".xlsx"))


def _safe_data_member_filename(member: str) -> str:
    filename = PurePosixPath(member.replace("\\", "/")).name
    if (
        not filename
        or filename in {".", ".."}
        or not filename.lower().endswith((".csv", ".xlsx"))
    ):
        raise ArchiveExtractionError(
            "INVALID_ARCHIVE_PAYLOAD",
            "Archive data member does not have a safe CSV/XLSX filename.",
            retryable=False,
            detail={"member": member},
        )
    return filename


def _delete_zip_after_extraction(
    record: Record,
    *,
    zip_persist: bool,
) -> bool:
    if zip_persist or not record.data_dir or not record.zip_filename:
        return False

    zip_path = Path(record.data_dir, record.zip_filename)
    if not zip_path.exists():
        return False

    try:
        zip_path.unlink()
    except OSError as err:
        raise ArchiveExtractionError(
            "EXTRACTION_FILESYSTEM_ERROR",
            str(err),
            retryable=False,
            detail={"path": str(zip_path), "url": record.url},
        ) from err
    return True


def _zip_persist_enabled(args: Mapping[str, Any], record: Record) -> bool:
    return _truthy_config_value(args.get("zip_persist")) or (
        _truthy_config_value(getattr(record, "zip_persist", ""))
    )


def _truthy_config_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _archive_post_headers(
    referer: str,
    post_headers: Mapping[str, str] | None,
) -> dict[str, str]:
    if post_headers is None:
        from histdatacom import config as histdata_config

        post_headers = histdata_config.POST_HEADERS
    headers = dict(post_headers)
    headers["Referer"] = referer
    return headers


def _response_content_bytes(response: Any) -> bytes:
    content = getattr(response, "content", b"")
    if isinstance(content, bytes):
        return content
    if isinstance(content, str):
        return content.encode("utf-8")
    return bytes(content or b"")


def _validate_zip_payload(path: Path) -> None:
    with zipfile.ZipFile(path, "r") as archive:
        bad_member = archive.testzip()
    if bad_member:
        raise zipfile.BadZipFile(f"bad member in ZIP archive: {bad_member}")


def _unlink_path(path: Path) -> None:
    if path.exists():
        path.unlink()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _mergeable_cache_items(
    work_items: Sequence[WorkItem],
) -> list[WorkItem]:
    return [
        item
        for item in work_items
        if item.cache_filename == CACHE_FILENAME
        and Path(item.data_dir, item.cache_filename).is_file()
    ]


def _cache_artifact_ref_for_work_item(item: WorkItem) -> ArtifactRef:
    path = Path(item.data_dir, item.cache_filename)
    return ArtifactRef(
        kind="cache",
        path=str(path),
        size_bytes=path.stat().st_size,
        sha256=_file_sha256(path),
        metadata={
            "filename": item.cache_filename,
            "timeframe": item.data_timeframe,
            "pair": item.data_fxpair,
            "line_count": item.cache_line_count,
            "start": item.cache_start,
            "end": item.cache_end,
            "work_id": item.work_id,
        },
    )


def _coerce_int(value: object) -> int:
    try:
        return int(str(value or "0"))
    except ValueError:
        return 0


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
                    sha256=_file_sha256(path),
                    metadata={"filename": filename},
                )
            )
    return tuple(refs)


def _existing_archive_artifact_on_disk(record: Record) -> bool:
    return existing_archive_artifact(record) is not None


def _supports_cache(record: Record) -> bool:
    return str.lower(record.data_format) == "ascii" and (
        record.data_timeframe in ["T", "M1"]
    )


def _cache_source_artifact_exists(record: Record) -> bool:
    return any(
        _source_artifact_path(record, filename) is not None
        for filename in (record.zip_filename, record.csv_filename)
    )


def _source_artifact_path(record: Record, filename: str) -> Path | None:
    if not filename:
        return None
    path = Path(record.data_dir, filename)
    if not path.is_file():
        return None
    return path


def create_cache_file(record: Record, args: Mapping[str, Any]) -> None:
    zip_path = _source_artifact_path(record, record.zip_filename)
    csv_path = _source_artifact_path(record, record.csv_filename)

    if zip_path is not None:
        file_data = _import_source_to_polars(record, zip_path)
    elif csv_path is not None:
        file_data = _import_source_to_polars(record, csv_path)
    else:
        raise CacheBuildError(
            "CACHE_SOURCE_NOT_FOUND",
            "Cache build requires a local ZIP or CSV source file.",
            retryable=False,
            detail={
                "data_dir": record.data_dir,
                "zip_filename": record.zip_filename,
                "csv_filename": record.csv_filename,
            },
        )

    if getattr(file_data, "height", 0) < 1:
        raise CacheBuildError(
            "CACHE_EMPTY",
            "Cache source contains no rows.",
            retryable=False,
            detail={
                "data_dir": record.data_dir,
                "zip_filename": record.zip_filename,
                "csv_filename": record.csv_filename,
            },
        )

    record.cache_filename = CACHE_FILENAME
    cache_path = Path(record.data_dir, record.cache_filename)
    atomic_write_polars_cache(
        file_data,
        cache_path,
        work_id=record.url or str(zip_path or csv_path),
    )

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
        raise CacheBuildError(
            "CACHE_SOURCE_INVALID",
            str(err),
            retryable=False,
            detail={
                "path": str(source_path),
                "timeframe": record.data_timeframe,
            },
        ) from err
    except Exception as err:
        raise CacheBuildError(
            "CACHE_SOURCE_INVALID",
            str(err),
            retryable=False,
            detail={
                "path": str(source_path),
                "timeframe": record.data_timeframe,
            },
        ) from err


def atomic_write_polars_cache(
    frame: Any,
    target_path: Path,
    *,
    work_id: str,
) -> Path:
    """Write a Polars IPC cache through a temp file, then rename."""
    temp_path = target_path.with_name(
        f".{target_path.name}.{derive_work_id(work_id).removeprefix('work-')}.tmp"
    )
    try:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        write_polars_cache(frame, temp_path)
        temp_path.replace(target_path)
        return target_path
    except OSError as err:
        _unlink_path(temp_path)
        raise CacheBuildError(
            "CACHE_FILESYSTEM_ERROR",
            str(err),
            retryable=False,
            detail={"path": str(target_path)},
        ) from err
    except Exception as err:
        _unlink_path(temp_path)
        raise CacheBuildError(
            "CACHE_WRITE_FAILED",
            str(err),
            retryable=False,
            detail={"path": str(target_path)},
        ) from err


def _cache_schema(frame: Any) -> dict[str, str]:
    return {str(column): str(dtype) for column, dtype in frame.schema.items()}


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


def _coerce_yearmonth(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value)
    return normalized or None


def _format_value(csv_format: Any) -> str:
    normalized = str(csv_format).lower()
    if normalized in Format.list_values():
        return normalized
    if str(csv_format) in Format.__members__:
        return str(Format[str(csv_format)].value)
    return str(Format(normalized).value)


def _timeframe_key(timeframe: Any) -> str:
    normalized = str(timeframe)
    if normalized in Timeframe.__members__:
        return normalized
    return str(Timeframe(normalized).name)


def _dataset_url(
    csv_format: str,
    timeframe: str,
    pair: str,
    period: DatasetPeriod,
    *,
    base_url: str,
) -> str:
    normalized_base = f"{base_url.rstrip('/')}/"
    return (
        f"{normalized_base}?/{csv_format}/"
        f"{Timeframe[timeframe].value}/{pair}/{period.url_path}"
    )


def _dataset_data_dir(
    csv_format: str,
    timeframe: str,
    pair: str,
    period: DatasetPeriod,
    *,
    default_download_dir: str,
) -> str:
    if not default_download_dir:
        return ""

    data_path = (
        Path(default_download_dir)
        / Format(csv_format).name
        / timeframe
        / pair
        / period.year
    )
    if period.month:
        data_path /= period.month
    return f"{data_path}{os.sep}"


def _dataset_period_from_path(url_path: str) -> DatasetPeriod:
    parts = url_path.split("/")
    year = parts[0]
    month = parts[1] if len(parts) > 1 else ""
    datemonth = f"{year}{int(month):02d}" if month else year
    return DatasetPeriod(
        year=year,
        month=month,
        url_path=url_path,
        datemonth=datemonth,
    )


def _year_range_paths(
    year: int,
    *,
    timeframe: str,
    start_year: int,
    start_month: int,
    end_year: int,
    end_month: int,
    current_year: int,
) -> tuple[str, ...]:
    if year == current_year:
        return _current_year_paths(
            year,
            start_year=start_year,
            start_month=start_month,
            end_year=end_year,
            end_month=end_month,
        )
    if start_year == year == end_year:
        return _same_year_paths(timeframe, year, start_month, end_month)
    if year == start_year != end_year:
        return _start_year_paths(timeframe, year, start_month)
    if year == end_year != start_year:
        return _end_year_paths(timeframe, year, end_month)
    return _year_paths(timeframe, year)


def _current_year_paths(
    year: int,
    *,
    start_year: int,
    start_month: int,
    end_year: int,
    end_month: int,
) -> tuple[str, ...]:
    first_month = start_month if start_year == end_year else 1
    return tuple(
        f"{year}/{month}" for month in range(first_month, end_month + 1)
    )


def _same_year_paths(
    timeframe: str,
    year: int,
    start_month: int,
    end_month: int,
) -> tuple[str, ...]:
    if timeframe == "M1":
        return (f"{year}",)
    return tuple(
        f"{year}/{month}" for month in range(start_month, end_month + 1)
    )


def _start_year_paths(
    timeframe: str,
    year: int,
    start_month: int,
) -> tuple[str, ...]:
    if timeframe == "M1":
        return (f"{year}",)
    return tuple(f"{year}/{month}" for month in range(start_month, 12 + 1))


def _end_year_paths(
    timeframe: str,
    year: int,
    end_month: int,
) -> tuple[str, ...]:
    if timeframe == "M1":
        return (f"{year}",)
    return tuple(f"{year}/{month}" for month in range(1, end_month + 1))


def _year_paths(timeframe: str, year: int) -> tuple[str, ...]:
    if timeframe == "M1":
        return (f"{year}",)
    return tuple(f"{year}/{month}" for month in range(1, 12 + 1))


def _single_year_or_month_paths(
    timeframe: str,
    start_yearmonth: str,
    *,
    current_yearmonth: str,
) -> tuple[str, ...]:
    current_year = int(get_year_from_datemonth(current_yearmonth))
    current_month = int(get_month_from_datemonth(current_yearmonth))
    start_year = int(get_year_from_datemonth(start_yearmonth))
    start_month = int(get_month_from_datemonth(start_yearmonth))

    if start_month == 0:
        if start_year == current_year:
            return tuple(
                f"{start_year}/{month}" for month in range(1, current_month + 1)
            )
        return _year_paths(timeframe, start_year)

    if start_year == current_year:
        return (f"{start_year}/{start_month}",)
    if timeframe == "M1":
        return (f"{start_year}",)
    return (f"{start_year}/{start_month}",)


def _json_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _json_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _repository_failure_output(
    *,
    repo_data: Mapping[str, Any],
    repo_file_exists: bool,
    repo_local_path: Path,
    pairs: Iterable[str],
    by: str | None,
    code: str,
    message: str,
    retryable: bool,
) -> RepositoryStageOutput:
    available_data = sort_repository_data(repo_data, set(pairs), by)
    filter_pairs = repository_missing_pairs(repo_data, set(pairs))
    result = StageResult(
        work_id=derive_work_id("repository_refresh", str(repo_local_path)),
        stage="repository_refresh",
        status=WorkStatus.FAILED,
        artifacts=_repository_artifacts(repo_local_path),
        events=(
            StatusEvent(
                status=WorkStatus.FAILED,
                stage="repository_refresh",
                message=message,
            ),
        ),
        failure=FailureInfo(
            code=code,
            message=message,
            retryable=retryable,
            detail={"repo_local_path": str(repo_local_path)},
        ),
        metrics={
            "available_data": available_data,
            "filter_pairs": list(filter_pairs),
            "pair_count": len(repository_pair_data(repo_data)),
            "repo_file_exists": repo_file_exists,
        },
    )
    return RepositoryStageOutput(
        repo_data=dict(repo_data),
        available_data=available_data,
        filter_pairs=filter_pairs,
        repo_file_exists=repo_file_exists,
        result=result,
    )


def _repository_artifacts(repo_local_path: Path) -> tuple[ArtifactRef, ...]:
    if not repo_local_path.exists():
        return ()
    return (
        ArtifactRef(
            kind="repository",
            path=str(repo_local_path),
            size_bytes=repo_local_path.stat().st_size,
        ),
    )


def _unlink_if_present(data_dir: str, filename: str) -> None:
    if not filename:
        return
    path = Path(data_dir, filename)
    if path.exists():
        path.unlink()
