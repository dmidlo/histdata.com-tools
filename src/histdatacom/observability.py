"""Renderer-neutral progress and status event helpers."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Callable, Mapping

from histdatacom.runtime_contracts import JSONValue, StatusEvent, WorkStatus

DEFAULT_PROGRESS_UNIT = "items"
PROGRESS_EVENT_TYPE = "progress"
ProgressEventSink = Callable[[StatusEvent], None]


def utc_now_iso() -> str:
    """Return the current UTC timestamp in the repository wire format."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def progress_percent(completed: float, total: float) -> float:
    """Return a bounded completion percentage."""
    if total <= 0:
        return 0.0
    return min(100.0, max(0.0, (completed / total) * 100))


def progress_rate_per_second(
    completed: float,
    started_at_utc: str,
    updated_at_utc: str,
) -> float:
    """Return an item rate when timestamps are available."""
    started = _parse_utc(started_at_utc)
    updated = _parse_utc(updated_at_utc)
    if started is None or updated is None:
        return 0.0
    elapsed_seconds = (updated - started).total_seconds()
    if elapsed_seconds <= 0:
        return 0.0
    return completed / elapsed_seconds


def progress_increment(event: StatusEvent) -> float:
    """Return the renderer advance amount carried by a progress event."""
    value = event.metadata.get("increment", 0.0)
    if not isinstance(value, (str, int, float)):
        return 0.0
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def attach_progress_metadata(
    event: StatusEvent,
    *,
    total: float,
    completed: float,
    unit: str = DEFAULT_PROGRESS_UNIT,
    increment: float = 0.0,
    started_at_utc: str = "",
    updated_at_utc: str = "",
    last_error: str = "",
    current: str = "",
    metadata: Mapping[str, JSONValue] | None = None,
) -> StatusEvent:
    """Return a status event enriched with GUI/CLI progress metadata."""
    timestamp = event.timestamp_utc or updated_at_utc or utc_now_iso()
    started = started_at_utc or timestamp
    progress_metadata: dict[str, JSONValue] = {
        "event_type": PROGRESS_EVENT_TYPE,
        "completed": completed,
        "total": total,
        "unit": unit,
        "increment": increment,
        "percent_complete": progress_percent(completed, total),
        "rate_per_second": progress_rate_per_second(
            completed,
            started,
            timestamp,
        ),
        "started_at_utc": started,
        "updated_at_utc": timestamp,
        "last_error": last_error,
    }
    if current:
        progress_metadata["current"] = current
    progress_metadata.update(dict(metadata or {}))
    return replace(
        event,
        timestamp_utc=timestamp,
        metadata={**event.metadata, **progress_metadata},
    )


@dataclass(slots=True)
class ProgressState:
    """Mutable progress aggregate that emits structured status events."""

    stage: str
    total: float = 0.0
    completed: float = 0.0
    unit: str = DEFAULT_PROGRESS_UNIT
    status: WorkStatus = WorkStatus.UNKNOWN
    work_id: str = ""
    started_at_utc: str = field(default_factory=utc_now_iso)
    updated_at_utc: str = ""
    last_error: str = ""
    events: tuple[StatusEvent, ...] = ()
    event_sink: ProgressEventSink | None = field(
        default=None,
        compare=False,
        repr=False,
    )

    @property
    def percent_complete(self) -> float:
        """Return the current bounded completion percentage."""
        return progress_percent(self.completed, self.total)

    @property
    def rate_per_second(self) -> float:
        """Return the current progress rate when timestamps permit."""
        return progress_rate_per_second(
            self.completed,
            self.started_at_utc,
            self.updated_at_utc,
        )

    def advance(
        self,
        increment: float = 1.0,
        *,
        status: WorkStatus | None = None,
        message: str = "",
        current: str = "",
        metadata: Mapping[str, JSONValue] | None = None,
    ) -> StatusEvent:
        """Advance progress and emit a structured event."""
        if status is not None:
            self.status = status
        if self.total > 0:
            self.completed = min(
                self.total, max(0.0, self.completed + increment)
            )
        else:
            self.completed = max(0.0, self.completed + increment)
        self.updated_at_utc = utc_now_iso()
        event = attach_progress_metadata(
            StatusEvent(
                status=self.status,
                stage=self.stage,
                message=message,
                work_id=self.work_id,
                timestamp_utc=self.updated_at_utc,
            ),
            total=self.total,
            completed=self.completed,
            unit=self.unit,
            increment=increment,
            started_at_utc=self.started_at_utc,
            updated_at_utc=self.updated_at_utc,
            last_error=self.last_error,
            current=current,
            metadata=metadata,
        )
        return self._record_event(event)

    def fail(
        self,
        error: BaseException | str,
        *,
        increment: float = 0.0,
        message: str = "",
        metadata: Mapping[str, JSONValue] | None = None,
    ) -> StatusEvent:
        """Record an error as failed progress and emit it."""
        self.status = WorkStatus.FAILED
        self.last_error = str(error)
        return self.advance(
            increment,
            status=WorkStatus.FAILED,
            message=message or self.last_error,
            metadata=metadata,
        )

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible aggregate snapshot."""
        return {
            "stage": self.stage,
            "status": self.status.value,
            "total": self.total,
            "completed": self.completed,
            "unit": self.unit,
            "percent_complete": self.percent_complete,
            "rate_per_second": self.rate_per_second,
            "started_at_utc": self.started_at_utc,
            "updated_at_utc": self.updated_at_utc,
            "last_error": self.last_error,
            "events": [event.to_dict() for event in self.events],
        }

    def _record_event(self, event: StatusEvent) -> StatusEvent:
        self.events = (*self.events, event)
        if self.event_sink is not None:
            self.event_sink(event)
        return event


def _parse_utc(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
