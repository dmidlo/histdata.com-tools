"""Serializable runtime contracts for the Temporal sidecar migration."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Any, Mapping

JSONScalar = str | int | float | bool | None
JSONValue = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]

RECORD_FIELDS = (
    "url",
    "encoding",
    "bytes_length",
    "data_date",
    "data_year",
    "data_month",
    "data_datemonth",
    "data_format",
    "data_timeframe",
    "data_fxpair",
    "data_dir",
    "data_tk",
    "zip_filename",
    "csv_filename",
    "cache_filename",
    "cache_line_count",
    "cache_start",
    "cache_end",
    "zip_persist",
)


def _record_field_kwargs(values: Mapping[str, str]) -> dict[str, Any]:
    """Return legacy record fields as constructor keyword arguments."""
    return {
        field_name: values.get(field_name, "") for field_name in RECORD_FIELDS
    }


def _normalized_status_text(value: str | "WorkStatus" | None) -> str:
    """Return the normalized text form used for status comparisons."""
    if isinstance(value, WorkStatus):
        return value.value
    return (value or "").strip().upper()


class WorkStatus(str, Enum):
    """Stable public status values for sidecar work items."""

    PLANNED = "PLANNED"
    URL_NEW = "URL_NEW"
    URL_VALID = "URL_VALID"
    URL_NO_REPO_DATA = "URL_NO_REPO_DATA"
    CSV_ZIP = "CSV_ZIP"
    CSV_FILE = "CSV_FILE"
    CACHE_READY = "CACHE_READY"
    INFLUX_UPLOAD = "INFLUX_UPLOAD"
    SKIPPED = "SKIPPED"
    RETRIED = "RETRIED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    COMPLETED = "COMPLETED"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def from_value(cls, value: str | "WorkStatus" | None) -> "WorkStatus":
        """Normalize legacy status strings into stable status values."""
        if isinstance(value, cls):
            return value

        normalized = _normalized_status_text(value)
        if not normalized:
            return cls.PLANNED

        aliases = {
            "CSV": cls.CSV_FILE,
            "DONE": cls.COMPLETED,
            "COMPLETE": cls.COMPLETED,
            "MISSING": cls.URL_NO_REPO_DATA,
            "NO_DATA": cls.URL_NO_REPO_DATA,
        }
        if normalized in aliases:
            return aliases[normalized]

        try:
            return cls(normalized)
        except ValueError:
            return cls.UNKNOWN

    @property
    def terminal(self) -> bool:
        """Return whether this status represents no further work for the item."""
        return self in {
            self.URL_NO_REPO_DATA,
            self.FAILED,
            self.CANCELLED,
            self.COMPLETED,
            self.INFLUX_UPLOAD,
        }


def status_has_csv_artifact(value: str | WorkStatus | None) -> bool:
    """Return whether a legacy or stable status indicates CSV-like local data."""
    return "CSV" in _normalized_status_text(value)


@dataclass(frozen=True, slots=True)
class ArtifactRef:
    """Reference to a side-effect artifact kept outside workflow history."""

    kind: str
    path: str
    size_bytes: int | None = None
    sha256: str = ""
    metadata: dict[str, JSONValue] = field(default_factory=dict)

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {
            "kind": self.kind,
            "path": self.path,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ArtifactRef":
        """Create an artifact reference from JSON-compatible data."""
        return cls(
            kind=str(data.get("kind", "")),
            path=str(data.get("path", "")),
            size_bytes=data.get("size_bytes"),
            sha256=str(data.get("sha256", "")),
            metadata=dict(data.get("metadata") or {}),
        )


@dataclass(frozen=True, slots=True)
class FailureInfo:
    """Structured failure metadata for activities and workflow steps."""

    code: str
    message: str
    retryable: bool = False
    detail: dict[str, JSONValue] = field(default_factory=dict)

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
            "detail": dict(self.detail),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> "FailureInfo | None":
        """Create failure metadata from JSON-compatible data."""
        if not data:
            return None
        return cls(
            code=str(data.get("code", "")),
            message=str(data.get("message", "")),
            retryable=bool(data.get("retryable", False)),
            detail=dict(data.get("detail") or {}),
        )


@dataclass(frozen=True, slots=True)
class StatusEvent:
    """A GUI/CLI-readable status event emitted by sidecar jobs."""

    status: WorkStatus
    stage: str
    message: str = ""
    work_id: str = ""
    timestamp_utc: str = ""
    metadata: dict[str, JSONValue] = field(default_factory=dict)

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {
            "status": self.status.value,
            "stage": self.stage,
            "message": self.message,
            "work_id": self.work_id,
            "timestamp_utc": self.timestamp_utc,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "StatusEvent":
        """Create a status event from JSON-compatible data."""
        return cls(
            status=WorkStatus.from_value(data.get("status")),
            stage=str(data.get("stage", "")),
            message=str(data.get("message", "")),
            work_id=str(data.get("work_id", "")),
            timestamp_utc=str(data.get("timestamp_utc", "")),
            metadata=dict(data.get("metadata") or {}),
        )


def new_request_id() -> str:
    """Return a new public request identifier."""
    import uuid

    return f"run-{uuid.uuid4().hex}"


def derive_work_id(*parts: str) -> str:
    """Return a stable work-item identifier for deterministic planning."""
    identity = "|".join(part for part in parts if part)
    if not identity:
        return "work-empty"
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]
    return f"work-{digest}"


@dataclass(frozen=True, slots=True)
class WorkItem:
    """Serializable replacement for queue-carried mutable Record objects."""

    work_id: str
    status: WorkStatus = WorkStatus.PLANNED
    status_text: str = ""
    url: str = ""
    encoding: str = ""
    bytes_length: str = ""
    data_date: str = ""
    data_year: str = ""
    data_month: str = ""
    data_datemonth: str = ""
    data_format: str = ""
    data_timeframe: str = ""
    data_fxpair: str = ""
    data_dir: str = ""
    data_tk: str = ""
    zip_filename: str = ""
    csv_filename: str = ""
    cache_filename: str = ""
    cache_line_count: str = ""
    cache_start: str = ""
    cache_end: str = ""
    zip_persist: str = ""
    metadata: dict[str, JSONValue] = field(default_factory=dict)

    @classmethod
    def from_record(cls, record: Any, work_id: str = "") -> "WorkItem":
        """Create a sidecar work item from the legacy Record shape."""
        values = {
            field_name: str(getattr(record, field_name, "") or "")
            for field_name in RECORD_FIELDS
        }
        raw_status = str(getattr(record, "status", "") or "")
        status = WorkStatus.from_value(raw_status)
        return cls(
            work_id=work_id
            or derive_work_id(
                values["url"],
                values["data_format"],
                values["data_timeframe"],
                values["data_fxpair"],
                values["data_datemonth"],
            ),
            status=status,
            status_text="" if status != WorkStatus.UNKNOWN else raw_status,
            **_record_field_kwargs(values),
        )

    @property
    def legacy_status(self) -> str:
        """Return the best status string for legacy Record compatibility."""
        return self.status_text or self.status.value

    def with_status(
        self, status: WorkStatus | str, *, status_text: str = ""
    ) -> "WorkItem":
        """Return a copy with an updated normalized status."""
        next_status = WorkStatus.from_value(status)
        return replace(
            self,
            status=next_status,
            status_text=(
                status_text
                if status_text or next_status == WorkStatus.UNKNOWN
                else ""
            ),
        )

    def to_record_kwargs(self) -> dict[str, str]:
        """Return keyword arguments accepted by the legacy Record class."""
        values = {
            field_name: getattr(self, field_name)
            for field_name in RECORD_FIELDS
        }
        values["status"] = self.legacy_status
        return values

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        values: dict[str, JSONValue] = {
            "work_id": self.work_id,
            "status": self.status.value,
            "status_text": self.status_text,
            "metadata": dict(self.metadata),
        }
        values.update(
            {
                field_name: getattr(self, field_name)
                for field_name in RECORD_FIELDS
            }
        )
        return values

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "WorkItem":
        """Create a work item from JSON-compatible data."""
        values = {
            field_name: str(data.get(field_name, "") or "")
            for field_name in RECORD_FIELDS
        }
        status = WorkStatus.from_value(data.get("status"))
        return cls(
            work_id=str(data.get("work_id", ""))
            or derive_work_id(values["url"]),
            status=status,
            status_text=str(data.get("status_text", "") or ""),
            metadata=dict(data.get("metadata") or {}),
            **_record_field_kwargs(values),
        )


@dataclass(frozen=True, slots=True)
class StageResult:
    """Result returned by an activity-safe stage function."""

    work_id: str
    stage: str
    status: WorkStatus
    artifacts: tuple[ArtifactRef, ...] = ()
    events: tuple[StatusEvent, ...] = ()
    failure: FailureInfo | None = None
    metrics: dict[str, JSONValue] = field(default_factory=dict)

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {
            "work_id": self.work_id,
            "stage": self.stage,
            "status": self.status.value,
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
            "events": [event.to_dict() for event in self.events],
            "failure": (
                self.failure.to_dict() if self.failure is not None else None
            ),
            "metrics": dict(self.metrics),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "StageResult":
        """Create a stage result from JSON-compatible data."""
        return cls(
            work_id=str(data.get("work_id", "")),
            stage=str(data.get("stage", "")),
            status=WorkStatus.from_value(data.get("status")),
            artifacts=tuple(
                ArtifactRef.from_dict(artifact)
                for artifact in data.get("artifacts", [])
            ),
            events=tuple(
                StatusEvent.from_dict(event) for event in data.get("events", [])
            ),
            failure=FailureInfo.from_dict(data.get("failure")),
            metrics=dict(data.get("metrics") or {}),
        )


@dataclass(frozen=True, slots=True)
class RunRequest:
    """Serializable job request shared by CLI, API, sidecar, and GUI."""

    request_id: str
    pairs: tuple[str, ...] = ()
    formats: tuple[str, ...] = ()
    timeframes: tuple[str, ...] = ()
    start_yearmonth: str = ""
    end_yearmonth: str = ""
    data_directory: str = "data"
    api_return_type: str = ""
    cpu_utilization: str = "medium"
    batch_size: str = "5000"
    available_remote_data: bool = False
    update_remote_data: bool = False
    validate_urls: bool = False
    download_data_archives: bool = False
    extract_csvs: bool = False
    import_to_influxdb: bool = False
    delete_after_influx: bool = False
    zip_persist: bool = False
    metadata: dict[str, JSONValue] = field(default_factory=dict)

    @classmethod
    def from_options(cls, options: Any, request_id: str = "") -> "RunRequest":
        """Create a request contract from the current Options object shape."""
        metadata = dict(getattr(options, "metadata", {}) or {})
        repo_sort = str(getattr(options, "by", "") or "")
        if repo_sort:
            metadata["repo_sort"] = repo_sort
        return cls(
            request_id=request_id or new_request_id(),
            pairs=tuple(sorted(getattr(options, "pairs", ()) or ())),
            formats=tuple(sorted(getattr(options, "formats", ()) or ())),
            timeframes=tuple(sorted(getattr(options, "timeframes", ()) or ())),
            start_yearmonth=str(getattr(options, "start_yearmonth", "") or ""),
            end_yearmonth=str(getattr(options, "end_yearmonth", "") or ""),
            data_directory=str(
                getattr(options, "data_directory", "data") or "data"
            ),
            api_return_type=str(getattr(options, "api_return_type", "") or ""),
            cpu_utilization=str(
                getattr(options, "cpu_utilization", "medium") or "medium"
            ),
            batch_size=str(getattr(options, "batch_size", "5000") or "5000"),
            available_remote_data=bool(
                getattr(options, "available_remote_data", False)
            ),
            update_remote_data=bool(
                getattr(options, "update_remote_data", False)
            ),
            validate_urls=bool(getattr(options, "validate_urls", False)),
            download_data_archives=bool(
                getattr(options, "download_data_archives", False)
            ),
            extract_csvs=bool(getattr(options, "extract_csvs", False)),
            import_to_influxdb=bool(
                getattr(options, "import_to_influxdb", False)
            ),
            delete_after_influx=bool(
                getattr(options, "delete_after_influx", False)
            ),
            zip_persist=bool(getattr(options, "zip_persist", False)),
            metadata=metadata,
        )

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {
            "request_id": self.request_id,
            "pairs": list(self.pairs),
            "formats": list(self.formats),
            "timeframes": list(self.timeframes),
            "start_yearmonth": self.start_yearmonth,
            "end_yearmonth": self.end_yearmonth,
            "data_directory": self.data_directory,
            "api_return_type": self.api_return_type,
            "cpu_utilization": self.cpu_utilization,
            "batch_size": self.batch_size,
            "available_remote_data": self.available_remote_data,
            "update_remote_data": self.update_remote_data,
            "validate_urls": self.validate_urls,
            "download_data_archives": self.download_data_archives,
            "extract_csvs": self.extract_csvs,
            "import_to_influxdb": self.import_to_influxdb,
            "delete_after_influx": self.delete_after_influx,
            "zip_persist": self.zip_persist,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "RunRequest":
        """Create a request contract from JSON-compatible data."""
        return cls(
            request_id=str(data.get("request_id", "")),
            pairs=tuple(str(item) for item in data.get("pairs", [])),
            formats=tuple(str(item) for item in data.get("formats", [])),
            timeframes=tuple(str(item) for item in data.get("timeframes", [])),
            start_yearmonth=str(data.get("start_yearmonth", "") or ""),
            end_yearmonth=str(data.get("end_yearmonth", "") or ""),
            data_directory=str(data.get("data_directory", "data") or "data"),
            api_return_type=str(data.get("api_return_type", "") or ""),
            cpu_utilization=str(
                data.get("cpu_utilization", "medium") or "medium"
            ),
            batch_size=str(data.get("batch_size", "5000") or "5000"),
            available_remote_data=bool(
                data.get("available_remote_data", False)
            ),
            update_remote_data=bool(data.get("update_remote_data", False)),
            validate_urls=bool(data.get("validate_urls", False)),
            download_data_archives=bool(
                data.get("download_data_archives", False)
            ),
            extract_csvs=bool(data.get("extract_csvs", False)),
            import_to_influxdb=bool(data.get("import_to_influxdb", False)),
            delete_after_influx=bool(data.get("delete_after_influx", False)),
            zip_persist=bool(data.get("zip_persist", False)),
            metadata=dict(data.get("metadata") or {}),
        )
