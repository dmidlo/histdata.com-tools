"""Calendar and market-session tags for HistData quality reports."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
import zipfile

from histdatacom.data_quality.contracts import (
    QualityFinding,
    QualityLocation,
    QualityRule,
    QualitySeverity,
    QualityTarget,
    QualityTargetKind,
)
from histdatacom.data_quality.calendar_profiles import (
    HistDataCalendarDateTag,
    HistDataCalendarProfile,
    HistDataCalendarWindowTag,
    default_calendar_profile,
)
from histdatacom.data_quality.symbols import symbol_metadata_for
from histdatacom.data_quality.time import (
    _SourceReadError as _TimestampSourceReadError,
    _TimestampSample,
    _TimestampScan,
    _timestamp_scan_for_target,
)
from histdatacom.histdata_ascii import (
    EST_NO_DST_OFFSET_MS,
    M1,
    TICK,
    columns_for_timeframe,
    delimiter_for_timeframe,
    parse_histdata_datetime_to_utc_ms,
)
from histdatacom.runtime_contracts import JSONValue

DOMAIN_CALENDAR_SESSION_RULE_ID = "domain.calendar_sessions"
SOURCE_TIMEZONE = "EST-no-DST"
SOURCE_UTC_OFFSET = "-05:00"
CANONICAL_TIMEZONE = "UTC"
MAX_CALENDAR_SAMPLES = 5
UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

SESSION_ASIA = "asia"
SESSION_LONDON = "london"
SESSION_NEW_YORK = "new_york"
SESSION_MARKET_CLOSED = "market_closed"
SESSION_NO_ACTIVE_WINDOW = "no_active_session_window"
SESSION_STATE_MARKET_OPEN = "market_open"
SESSION_STATE_WEEKEND_CLOSURE = "weekend_closure"
SESSION_STATE_SUNDAY_OPEN = "sunday_open"
SESSION_STATE_FRIDAY_CLOSE = "friday_close"

FX_FRIDAY_CLOSE_WEEKDAY = 4
FX_SUNDAY_OPEN_WEEKDAY = 6
FX_CLOSE_OPEN_MINUTE = 17 * 60
MILLISECONDS_PER_MINUTE = 60_000
MILLISECONDS_PER_DAY = 24 * 60 * MILLISECONDS_PER_MINUTE


@dataclass(frozen=True, slots=True)
class HistDataSessionWindow:
    """A fixed UTC clock window used for coarse FX session tagging."""

    name: str
    start_minute_utc: int
    end_minute_utc: int
    description: str

    def contains(self, minute_of_day: int) -> bool:
        """Return whether a UTC minute is inside this window."""
        return _contains_minute(
            minute_of_day,
            start_minute=self.start_minute_utc,
            end_minute=self.end_minute_utc,
        )

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible session window description."""
        return {
            "name": self.name,
            "timezone": CANONICAL_TIMEZONE,
            "start": _minute_label(self.start_minute_utc),
            "end": _minute_label(self.end_minute_utc),
            "description": self.description,
        }


@dataclass(frozen=True, slots=True)
class HistDataClockWindow:
    """A named clock window used for source or UTC special tags."""

    name: str
    start_minute: int
    end_minute: int
    timezone: str
    description: str

    def contains(self, minute_of_day: int) -> bool:
        """Return whether a minute is inside this window."""
        return _contains_minute(
            minute_of_day,
            start_minute=self.start_minute,
            end_minute=self.end_minute,
        )

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible window description."""
        return {
            "name": self.name,
            "timezone": self.timezone,
            "start": _minute_label(self.start_minute),
            "end": _minute_label(self.end_minute),
            "description": self.description,
        }


@dataclass(frozen=True, slots=True)
class HistDataStaticHoliday:
    """A static source-calendar holiday tag."""

    name: str
    month: int
    day: int
    description: str

    def matches(self, source: datetime) -> bool:
        """Return whether the source date matches this static holiday."""
        return source.month == self.month and source.day == self.day

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible holiday description."""
        return {
            "name": self.name,
            "month": self.month,
            "day": self.day,
            "description": self.description,
        }


@dataclass(frozen=True, slots=True)
class HistDataCalendarClassification:
    """Session and calendar tags for one normalized HistData timestamp."""

    timestamp_utc_ms: int
    source_timestamp: str
    source_datetime: datetime
    utc_datetime: datetime
    session_state: str
    clock_sessions: tuple[str, ...]
    active_sessions: tuple[str, ...]
    overlaps: tuple[str, ...]
    special_tags: tuple[str, ...]
    holiday_tags: tuple[str, ...]
    event_tags: tuple[str, ...]
    calendar_tags: tuple[str, ...]

    @property
    def utc_timestamp(self) -> str:
        """Return canonical UTC timestamp text."""
        return _utc_iso_from_datetime(self.utc_datetime)

    @property
    def source_timestamp_iso(self) -> str:
        """Return fixed-offset source timestamp text."""
        return _source_iso_from_datetime(self.source_datetime)

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return JSON-compatible session/calendar tags."""
        return {
            "timestamp_utc_ms": self.timestamp_utc_ms,
            "utc_timestamp": self.utc_timestamp,
            "source_timestamp": self.source_timestamp,
            "source_datetime": self.source_timestamp_iso,
            "source_timezone": SOURCE_TIMEZONE,
            "source_utc_offset": SOURCE_UTC_OFFSET,
            "source_date": self.source_datetime.strftime("%Y-%m-%d"),
            "source_weekday": self.source_datetime.strftime("%A").lower(),
            "session_state": self.session_state,
            "clock_sessions": list(self.clock_sessions),
            "active_sessions": list(self.active_sessions),
            "overlaps": list(self.overlaps),
            "special_tags": list(self.special_tags),
            "holiday_tags": list(self.holiday_tags),
            "event_tags": list(self.event_tags),
            "calendar_tags": list(self.calendar_tags),
        }


@dataclass(frozen=True, slots=True)
class _TextPayload:
    data: bytes
    source_member: str = ""


@dataclass(frozen=True, slots=True)
class _SourceReadError(Exception):
    code: str
    message: str
    metadata: dict[str, JSONValue]


@dataclass(frozen=True, slots=True)
class _CalendarSample:
    row_number: int
    classification: HistDataCalendarClassification
    source_member: str = ""

    def to_dict(self) -> dict[str, JSONValue]:
        """Return bounded JSON-compatible sample metadata."""
        return {
            "row_number": self.row_number,
            "source_member": self.source_member,
            **self.classification.to_metadata(),
        }


@dataclass(frozen=True, slots=True)
class _InvalidTimestampSample:
    row_number: int
    timestamp_source: str
    source_member: str = ""
    error: str = ""

    def to_dict(self) -> dict[str, JSONValue]:
        """Return bounded JSON-compatible invalid-timestamp context."""
        return {
            "row_number": self.row_number,
            "timestamp_source": self.timestamp_source,
            "source_member": self.source_member,
            "error": self.error,
        }


@dataclass(slots=True)
class _CalendarScan:
    row_count: int = 0
    parsed_row_count: int = 0
    invalid_timestamp_count: int = 0
    session_state_counts: Counter[str] = field(default_factory=Counter)
    clock_session_counts: Counter[str] = field(default_factory=Counter)
    active_session_counts: Counter[str] = field(default_factory=Counter)
    overlap_counts: Counter[str] = field(default_factory=Counter)
    special_tag_counts: Counter[str] = field(default_factory=Counter)
    holiday_tag_counts: Counter[str] = field(default_factory=Counter)
    event_tag_counts: Counter[str] = field(default_factory=Counter)
    calendar_tag_counts: Counter[str] = field(default_factory=Counter)
    samples: list[_CalendarSample] = field(default_factory=list)
    invalid_timestamps: list[_InvalidTimestampSample] = field(
        default_factory=list
    )


SESSION_WINDOWS = (
    HistDataSessionWindow(
        name=SESSION_ASIA,
        start_minute_utc=0,
        end_minute_utc=9 * 60,
        description="Coarse Asia trading-session window.",
    ),
    HistDataSessionWindow(
        name=SESSION_LONDON,
        start_minute_utc=7 * 60,
        end_minute_utc=16 * 60,
        description="Coarse London trading-session window.",
    ),
    HistDataSessionWindow(
        name=SESSION_NEW_YORK,
        start_minute_utc=12 * 60,
        end_minute_utc=21 * 60,
        description="Coarse New York trading-session window.",
    ),
)

DAILY_ROLLOVER_WINDOW = HistDataClockWindow(
    name="daily_rollover",
    start_minute=16 * 60 + 55,
    end_minute=17 * 60 + 5,
    timezone=SOURCE_TIMEZONE,
    description="Fixed EST-no-DST 5 p.m. rollover window.",
)
SUNDAY_OPEN_WINDOW = HistDataClockWindow(
    name=SESSION_STATE_SUNDAY_OPEN,
    start_minute=17 * 60,
    end_minute=18 * 60,
    timezone=SOURCE_TIMEZONE,
    description="Sunday source-clock market open window.",
)
FRIDAY_CLOSE_WINDOW = HistDataClockWindow(
    name=SESSION_STATE_FRIDAY_CLOSE,
    start_minute=16 * 60,
    end_minute=17 * 60,
    timezone=SOURCE_TIMEZONE,
    description="Friday source-clock market close window.",
)
LONDON_FIX_WINDOW = HistDataClockWindow(
    name="london_4pm_fix_window",
    start_minute=15 * 60 + 55,
    end_minute=16 * 60 + 5,
    timezone=CANONICAL_TIMEZONE,
    description="Advisory window around the London 4 p.m. fix.",
)

STATIC_MAJOR_HOLIDAYS = (
    HistDataStaticHoliday(
        name="new_years_day",
        month=1,
        day=1,
        description="Static source-calendar New Year's Day tag.",
    ),
    HistDataStaticHoliday(
        name="christmas_day",
        month=12,
        day=25,
        description="Static source-calendar Christmas Day tag.",
    ),
)


def classify_histdata_timestamp(
    timestamp_utc_ms: int,
    *,
    source_timestamp: str = "",
    calendar_profile: HistDataCalendarProfile | None = None,
    asset_class: str = "",
) -> HistDataCalendarClassification:
    """Classify one canonical UTC millisecond timestamp.

    HistData source timestamps are first interpreted as EST without DST. The
    source projection is then used for market-open/close, rollover, and
    month/quarter/year-end tags; UTC projection is used for coarse session
    windows and the London fix window.
    """
    utc = _utc_datetime_from_ms(timestamp_utc_ms)
    source = _source_datetime_from_utc_ms(timestamp_utc_ms)
    profile = calendar_profile or default_calendar_profile()
    session_state = _session_state_for_source(source)
    clock_sessions = _clock_sessions_for_utc(utc)
    active_sessions = (
        () if session_state == SESSION_STATE_WEEKEND_CLOSURE else clock_sessions
    )
    overlaps = _overlaps_for_sessions(active_sessions)
    special_tags = _special_tags(source, utc, session_state)
    holiday_tags = _holiday_tags(
        source,
        calendar_profile=profile,
        asset_class=asset_class,
    )
    event_tags = _event_tags(
        source,
        calendar_profile=profile,
        asset_class=asset_class,
    )
    calendar_tags = tuple(
        dict.fromkeys(
            (
                *active_sessions,
                *overlaps,
                session_state,
                *special_tags,
                *holiday_tags,
                *event_tags,
            )
        )
    )
    return HistDataCalendarClassification(
        timestamp_utc_ms=timestamp_utc_ms,
        source_timestamp=source_timestamp,
        source_datetime=source,
        utc_datetime=utc,
        session_state=session_state,
        clock_sessions=clock_sessions,
        active_sessions=active_sessions,
        overlaps=overlaps,
        special_tags=special_tags,
        holiday_tags=holiday_tags,
        event_tags=event_tags,
        calendar_tags=calendar_tags,
    )


def classify_histdata_source_timestamp(
    value: str,
    timeframe: str,
    *,
    calendar_profile: HistDataCalendarProfile | None = None,
    asset_class: str = "",
) -> HistDataCalendarClassification:
    """Parse and classify a raw HistData source timestamp."""
    timestamp_utc_ms = parse_histdata_datetime_to_utc_ms(value, timeframe)
    return classify_histdata_timestamp(
        timestamp_utc_ms,
        source_timestamp=value.strip(),
        calendar_profile=calendar_profile,
        asset_class=asset_class,
    )


def calendar_policy_metadata(
    calendar_profile: HistDataCalendarProfile | None = None,
) -> dict[str, JSONValue]:
    """Return the tagging policy embedded in quality-report summaries."""
    profile = calendar_profile or default_calendar_profile()
    profile_metadata = profile.to_metadata()
    return {
        "source_timezone": SOURCE_TIMEZONE,
        "source_utc_offset": SOURCE_UTC_OFFSET,
        "canonical_timezone": CANONICAL_TIMEZONE,
        "utc_normalization_offset_ms": EST_NO_DST_OFFSET_MS,
        "projection_policy": (
            "HistData timestamps are parsed as fixed EST without daylight "
            "saving before UTC/session/calendar tags are derived."
        ),
        "session_windows": [window.to_metadata() for window in SESSION_WINDOWS],
        "special_windows": [
            DAILY_ROLLOVER_WINDOW.to_metadata(),
            SUNDAY_OPEN_WINDOW.to_metadata(),
            FRIDAY_CLOSE_WINDOW.to_metadata(),
            LONDON_FIX_WINDOW.to_metadata(),
        ],
        "calendar_profile": profile_metadata,
        "holiday_calendar_source": profile.source,
        "holiday_calendar_complete": profile.complete,
        "holiday_calendar_static_advisory": profile.static_advisory,
        "holiday_calendar_limitations": "; ".join(profile.limitations),
        "static_major_holidays": [
            holiday.to_metadata() for holiday in STATIC_MAJOR_HOLIDAYS
        ],
        "profile_date_tags": profile_metadata["date_tags"],
        "profile_window_tags": profile_metadata["window_tags"],
        "month_end_policy": "source_calendar_date",
        "fix_window_policy": "advisory_utc_london_4pm_window",
    }


@dataclass(slots=True)
class HistDataCalendarSessionRule:
    """Emit session, rollover, fix, and calendar-regime tags."""

    calendar_profile: HistDataCalendarProfile = field(
        default_factory=default_calendar_profile
    )
    timestamp_severity: QualitySeverity = QualitySeverity.WARNING
    source_severity: QualitySeverity = QualitySeverity.WARNING
    profile_missing_severity: QualitySeverity = QualitySeverity.INFO
    rule_id: str = DOMAIN_CALENDAR_SESSION_RULE_ID
    description: str = (
        "Tag HistData rows with FX/CFD sessions, open/close, rollover, "
        "fix-window, holiday, and calendar-regime metadata."
    )

    def evaluate(self, target: QualityTarget) -> tuple[QualityFinding, ...]:
        """Return calendar/session findings for one target."""
        if not _is_ascii_text_target(target):
            return ()

        asset_class = _target_asset_class(target)
        try:
            timestamp_scan = _timestamp_scan_for_target(target)
        except (ValueError, _TimestampSourceReadError):
            timestamp_scan = None
        if timestamp_scan is not None and _can_use_timestamp_scan_for_calendar(
            timestamp_scan
        ):
            source_member = (
                timestamp_scan.valid_rows[0].source_member
                if timestamp_scan.valid_rows
                else ""
            )
            scan = _scan_calendar_timestamp_samples(
                tuple(timestamp_scan.valid_rows),
                calendar_profile=self.calendar_profile,
                asset_class=asset_class,
            )
            return self._findings_for_scan(
                target,
                scan,
                source_member=source_member,
            )
        if (
            timestamp_scan is not None
            and timestamp_scan.polars_frame is not None
        ):
            scan = _scan_calendar_projected_frame(
                timestamp_scan.polars_frame,
                target=target,
                calendar_profile=self.calendar_profile,
                asset_class=asset_class,
            )
            return self._findings_for_scan(target, scan, source_member="")

        try:
            delimiter = delimiter_for_timeframe(target.timeframe)
            columns = columns_for_timeframe(target.timeframe)
            payload = _read_text_payload(target)
            text = payload.data.decode("utf-8")
        except ValueError as exc:
            return (
                _finding(
                    target,
                    code="DOMAIN_CALENDAR_METADATA_UNSUPPORTED",
                    message="Target metadata does not describe supported "
                    "HistData ASCII data for calendar tagging.",
                    severity=self.source_severity,
                    metadata={"timeframe": target.timeframe, "error": str(exc)},
                ),
            )
        except UnicodeDecodeError as exc:
            return (
                _finding(
                    target,
                    code="DOMAIN_CALENDAR_TEXT_ENCODING_INVALID",
                    message="Target text could not be decoded for calendar "
                    "tagging.",
                    severity=self.source_severity,
                    metadata={"error": str(exc)},
                ),
            )
        except _SourceReadError as exc:
            return (
                _finding(
                    target,
                    code=exc.code,
                    message=exc.message,
                    severity=self.source_severity,
                    metadata=exc.metadata,
                ),
            )

        scan = _scan_calendar_rows(
            target,
            text.splitlines(),
            delimiter=delimiter,
            timestamp_index=columns.index("datetime"),
            source_member=payload.source_member,
            calendar_profile=self.calendar_profile,
            asset_class=asset_class,
        )
        findings = [
            *_scan_findings(
                target,
                scan,
                source_member=payload.source_member,
                calendar_profile=self.calendar_profile,
                profile_missing_severity=self.profile_missing_severity,
            )
        ]
        if scan.invalid_timestamp_count:
            findings.append(
                _finding(
                    target,
                    code="DOMAIN_CALENDAR_TIMESTAMP_UNPARSEABLE",
                    message="One or more timestamps could not be parsed for "
                    "calendar/session tagging.",
                    severity=self.timestamp_severity,
                    location=_invalid_location(
                        target,
                        scan.invalid_timestamps[0],
                    ),
                    metadata={
                        **_base_metadata(target, payload.source_member),
                        "invalid_timestamp_count": (
                            scan.invalid_timestamp_count
                        ),
                        "samples": [
                            sample.to_dict()
                            for sample in scan.invalid_timestamps
                        ],
                    },
                )
            )
        return tuple(findings)

    def _findings_for_scan(
        self,
        target: QualityTarget,
        scan: _CalendarScan,
        *,
        source_member: str,
    ) -> tuple[QualityFinding, ...]:
        return _scan_findings(
            target,
            scan,
            source_member=source_member,
            calendar_profile=self.calendar_profile,
            profile_missing_severity=self.profile_missing_severity,
        )


def calendar_quality_rules(
    calendar_profile: HistDataCalendarProfile | None = None,
    *,
    profile_missing_severity: QualitySeverity = QualitySeverity.INFO,
) -> tuple[QualityRule, ...]:
    """Return calendar/session quality rules."""
    rule: QualityRule = HistDataCalendarSessionRule(
        calendar_profile=calendar_profile or default_calendar_profile(),
        profile_missing_severity=profile_missing_severity,
    )
    return (rule,)


def _scan_calendar_rows(
    target: QualityTarget,
    lines: list[str],
    *,
    delimiter: str,
    timestamp_index: int,
    source_member: str,
    calendar_profile: HistDataCalendarProfile,
    asset_class: str,
) -> _CalendarScan:
    scan = _CalendarScan()
    for row_number, raw in enumerate(lines, start=1):
        if not raw.strip():
            continue
        scan.row_count += 1
        values = _parse_row(raw, delimiter)
        timestamp_source = (
            values[timestamp_index].strip()
            if len(values) > timestamp_index
            else ""
        )
        try:
            classification = classify_histdata_source_timestamp(
                timestamp_source,
                target.timeframe,
                calendar_profile=calendar_profile,
                asset_class=asset_class,
            )
        except ValueError as exc:
            scan.invalid_timestamp_count += 1
            _append_invalid_timestamp(
                scan.invalid_timestamps,
                _InvalidTimestampSample(
                    row_number=row_number,
                    timestamp_source=timestamp_source,
                    source_member=source_member,
                    error=str(exc),
                ),
            )
            continue

        scan.parsed_row_count += 1
        _record_classification(
            scan,
            classification,
            row_number=row_number,
            source_member=source_member,
        )
    return scan


def _can_use_timestamp_scan_for_calendar(scan: _TimestampScan) -> bool:
    return bool(
        scan.invalid_timestamp_count == 0
        and scan.field_count_error_count == 0
        and scan.header_row_count == 0
        and scan.row_count == scan.parsed_row_count
        and len(scan.valid_rows) == scan.parsed_row_count
    )


def _scan_calendar_timestamp_samples(
    samples: tuple[_TimestampSample, ...],
    *,
    calendar_profile: HistDataCalendarProfile,
    asset_class: str,
) -> _CalendarScan:
    scan = _CalendarScan(row_count=len(samples), parsed_row_count=len(samples))
    for sample in samples:
        source_minute = sample.source_time_of_day_ms // MILLISECONDS_PER_MINUTE
        utc_minute = (
            sample.timestamp_utc_ms % MILLISECONDS_PER_DAY
        ) // MILLISECONDS_PER_MINUTE
        session_state = _session_state_for_source_fields(
            weekday=sample.source_weekday,
            minute=source_minute,
        )
        clock_sessions = _clock_sessions_for_minute(utc_minute)
        active_sessions = (
            ()
            if session_state == SESSION_STATE_WEEKEND_CLOSURE
            else clock_sessions
        )
        overlaps = _overlaps_for_sessions(active_sessions)
        special_tags = _special_tags_for_source_fields(
            source_month=sample.source_month,
            source_day=sample.source_day,
            source_month_length=sample.source_month_length,
            source_minute=source_minute,
            utc_minute=utc_minute,
            session_state=session_state,
        )
        holiday_tags = _holiday_tags_for_source_fields(
            source_year=sample.source_year,
            source_month=sample.source_month,
            source_day=sample.source_day,
            calendar_profile=calendar_profile,
            asset_class=asset_class,
        )
        event_tags = _event_tags_for_source_fields(
            source_month=sample.source_month,
            source_day=sample.source_day,
            source_day_number=sample.source_day_ordinal
            - UNIX_EPOCH.toordinal(),
            calendar_profile=calendar_profile,
            asset_class=asset_class,
        )
        calendar_tags = tuple(
            dict.fromkeys(
                (
                    *active_sessions,
                    *overlaps,
                    session_state,
                    *special_tags,
                    *holiday_tags,
                    *event_tags,
                )
            )
        )
        _record_calendar_values(
            scan,
            row_number=sample.row_number,
            source_member=sample.source_member,
            timestamp_sample=sample,
            session_state=session_state,
            clock_sessions=clock_sessions,
            active_sessions=active_sessions,
            overlaps=overlaps,
            special_tags=special_tags,
            holiday_tags=holiday_tags,
            event_tags=event_tags,
            calendar_tags=calendar_tags,
            calendar_profile=calendar_profile,
            asset_class=asset_class,
        )
    return scan


def _scan_calendar_projected_frame(
    frame: Any,
    *,
    target: QualityTarget,
    calendar_profile: HistDataCalendarProfile,
    asset_class: str,
) -> _CalendarScan:
    import polars as pl

    scan = _CalendarScan(row_count=frame.height, parsed_row_count=frame.height)
    if frame.is_empty():
        return scan

    source_minute = pl.col("_source_time_of_day_ms") // MILLISECONDS_PER_MINUTE
    utc_minute = (pl.col("datetime") % MILLISECONDS_PER_DAY) // (
        MILLISECONDS_PER_MINUTE
    )
    asia = utc_minute.is_between(0, 9 * 60 - 1)
    london = utc_minute.is_between(7 * 60, 16 * 60 - 1)
    new_york = utc_minute.is_between(12 * 60, 21 * 60 - 1)
    friday_close = (
        pl.col("_source_weekday") == FX_FRIDAY_CLOSE_WEEKDAY
    ) & source_minute.is_between(16 * 60, 17 * 60 - 1)
    sunday_open = (
        pl.col("_source_weekday") == FX_SUNDAY_OPEN_WEEKDAY
    ) & source_minute.is_between(17 * 60, 18 * 60 - 1)
    weekend = (
        (pl.col("_source_weekday") == 5)
        | (
            (pl.col("_source_weekday") == FX_FRIDAY_CLOSE_WEEKDAY)
            & (source_minute >= FX_CLOSE_OPEN_MINUTE)
        )
        | (
            (pl.col("_source_weekday") == FX_SUNDAY_OPEN_WEEKDAY)
            & (source_minute < FX_CLOSE_OPEN_MINUTE)
        )
    )
    market_open = ~(weekend | friday_close | sunday_open)
    non_weekend = ~weekend
    any_session = asia | london | new_york
    month_length = _polars_month_length_expr()
    month_end = pl.col("_source_day") == month_length
    quarter_end = pl.col("_source_month").is_in([3, 6, 9, 12]) & month_end
    year_end = (pl.col("_source_month") == 12) & (pl.col("_source_day") == 31)
    london_fix = utc_minute.is_between(15 * 60 + 55, 16 * 60 + 4)
    daily_rollover = source_minute.is_between(16 * 60 + 55, 17 * 60 + 4)

    session_counts = {
        SESSION_STATE_MARKET_OPEN: _polars_count(frame, market_open),
        SESSION_STATE_WEEKEND_CLOSURE: _polars_count(frame, weekend),
        SESSION_STATE_SUNDAY_OPEN: _polars_count(frame, sunday_open),
        SESSION_STATE_FRIDAY_CLOSE: _polars_count(frame, friday_close),
    }
    scan.session_state_counts.update(
        {key: count for key, count in session_counts.items() if count}
    )
    scan.clock_session_counts.update(
        {
            key: count
            for key, count in {
                SESSION_ASIA: _polars_count(frame, asia),
                SESSION_LONDON: _polars_count(frame, london),
                SESSION_NEW_YORK: _polars_count(frame, new_york),
            }.items()
            if count
        },
    )
    scan.active_session_counts.update(
        {
            key: count
            for key, count in {
                SESSION_MARKET_CLOSED: session_counts[
                    SESSION_STATE_WEEKEND_CLOSURE
                ],
                SESSION_ASIA: _polars_count(frame, non_weekend & asia),
                SESSION_LONDON: _polars_count(frame, non_weekend & london),
                SESSION_NEW_YORK: _polars_count(frame, non_weekend & new_york),
                SESSION_NO_ACTIVE_WINDOW: _polars_count(
                    frame,
                    non_weekend & ~any_session,
                ),
            }.items()
            if count
        },
    )
    scan.overlap_counts.update(
        {
            key: count
            for key, count in {
                "asia_london_overlap": _polars_count(
                    frame,
                    non_weekend & asia & london,
                ),
                "london_new_york_overlap": _polars_count(
                    frame,
                    non_weekend & london & new_york,
                ),
                "multi_session_overlap": _polars_count(
                    frame,
                    non_weekend
                    & (
                        (
                            asia.cast(pl.Int8)
                            + london.cast(pl.Int8)
                            + new_york.cast(pl.Int8)
                        )
                        > 1
                    ),
                ),
            }.items()
            if count
        },
    )
    special_counts = {
        SESSION_STATE_WEEKEND_CLOSURE: session_counts[
            SESSION_STATE_WEEKEND_CLOSURE
        ],
        SESSION_STATE_SUNDAY_OPEN: session_counts[SESSION_STATE_SUNDAY_OPEN],
        SESSION_STATE_FRIDAY_CLOSE: session_counts[SESSION_STATE_FRIDAY_CLOSE],
        DAILY_ROLLOVER_WINDOW.name: _polars_count(frame, daily_rollover),
        LONDON_FIX_WINDOW.name: _polars_count(frame, london_fix),
        "month_end_fix_window": _polars_count(frame, london_fix & month_end),
        "quarter_end_fix_window": _polars_count(
            frame,
            london_fix & quarter_end,
        ),
        "year_end_fix_window": _polars_count(frame, london_fix & year_end),
        "month_end": _polars_count(frame, month_end),
        "quarter_end": _polars_count(frame, quarter_end),
        "year_end": _polars_count(frame, year_end),
    }
    scan.special_tag_counts.update(
        {key: count for key, count in special_counts.items() if count}
    )
    holiday_counts = _polars_date_tag_counts(
        frame,
        calendar_profile=calendar_profile,
        asset_class=asset_class,
    )
    scan.holiday_tag_counts.update(
        {key: count for key, count in holiday_counts.items() if count}
    )
    event_counts = _polars_window_tag_counts(
        frame,
        calendar_profile=calendar_profile,
        asset_class=asset_class,
    )
    scan.event_tag_counts.update(
        {key: count for key, count in event_counts.items() if count}
    )
    for counter in (
        scan.active_session_counts,
        scan.overlap_counts,
        scan.session_state_counts,
        Counter(
            {
                key: count
                for key, count in special_counts.items()
                if key
                not in {
                    SESSION_STATE_WEEKEND_CLOSURE,
                    SESSION_STATE_SUNDAY_OPEN,
                    SESSION_STATE_FRIDAY_CLOSE,
                }
                and count
            }
        ),
        scan.holiday_tag_counts,
        scan.event_tag_counts,
    ):
        scan.calendar_tag_counts.update(counter)

    for row in frame.head(MAX_CALENDAR_SAMPLES).iter_rows(named=True):
        sample = _projected_calendar_timestamp_sample(row, target=target)
        scan.samples.append(
            _CalendarSample(
                row_number=sample.row_number,
                classification=classify_histdata_timestamp(
                    sample.timestamp_utc_ms,
                    source_timestamp=sample.timestamp_source,
                    calendar_profile=calendar_profile,
                    asset_class=asset_class,
                ),
                source_member="",
            )
        )
    return scan


def _projected_calendar_timestamp_sample(
    row: Mapping[str, Any],
    *,
    target: QualityTarget,
) -> _TimestampSample:
    source_year = int(row["_source_year"])
    source_month = int(row["_source_month"])
    source_day = int(row["_source_day"])
    return _TimestampSample(
        row_number=int(row["_row_number"]),
        timestamp_source=str(row["_source_timestamp"]),
        timestamp_utc_ms=int(row["datetime"]),
        source_period=str(row["_source_period"]),
        utc_period=str(row["_utc_period"]),
        source_year=source_year,
        source_month=source_month,
        source_day=source_day,
        source_month_length=_month_length(source_year, source_month),
        source_weekday=int(row["_source_weekday"]),
        source_day_ordinal=datetime(
            source_year,
            source_month,
            source_day,
            tzinfo=timezone.utc,
        ).toordinal(),
        source_time_of_day_ms=int(row["_source_time_of_day_ms"]),
        source_member="",
    )


def _polars_count(frame: Any, expression: Any) -> int:
    try:
        return int(frame.select(expression.sum()).item() or 0)
    except Exception:
        return 0


def _polars_date_tag_counts(
    frame: Any,
    *,
    calendar_profile: HistDataCalendarProfile,
    asset_class: str,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    years = _polars_source_year_range(frame)
    for tag in calendar_profile.applicable_date_tags(asset_class=asset_class):
        counts[tag.tag] = _polars_count(
            frame,
            _polars_date_tag_expression(tag, years=years),
        )
    return counts


def _polars_window_tag_counts(
    frame: Any,
    *,
    calendar_profile: HistDataCalendarProfile,
    asset_class: str,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for tag in calendar_profile.applicable_window_tags(asset_class=asset_class):
        counts[tag.tag] = _polars_count(
            frame,
            _polars_window_tag_expression(tag),
        )
    return counts


def _polars_date_tag_expression(
    tag: HistDataCalendarDateTag,
    *,
    years: range,
) -> Any:
    import polars as pl

    if tag.rule:
        expression = pl.lit(False)
        for year in years:
            match = tag.movable_date_for_year(year)
            expression = expression | (
                (pl.col("_source_year") == year)
                & (pl.col("_source_month") == match.month)
                & (pl.col("_source_day") == match.day)
            )
        return expression
    return (pl.col("_source_month") == tag.month) & (
        pl.col("_source_day") == tag.day
    )


def _polars_window_tag_expression(tag: HistDataCalendarWindowTag) -> Any:
    import polars as pl

    if tag.uses_absolute_dates:
        return pl.col("_source_day_number").is_between(
            tag.start_day_number,
            tag.end_day_number,
        )
    value = pl.col("_source_month") * 100 + pl.col("_source_day")
    start = tag.start_month * 100 + tag.start_day
    end = tag.end_month * 100 + tag.end_day
    if start <= end:
        return value.is_between(start, end)
    return (value >= start) | (value <= end)


def _polars_source_year_range(frame: Any) -> range:
    import polars as pl

    try:
        row = frame.select(
            [
                pl.col("_source_year").min(),
                pl.col("_source_year").max(),
            ]
        ).row(0)
    except Exception:
        return range(0)
    start = int(row[0] or 0)
    end = int(row[1] or 0)
    if not start or not end or end < start:
        return range(0)
    return range(start, end + 1)


def _polars_month_length_expr() -> Any:
    import polars as pl

    year = pl.col("_source_year")
    month = pl.col("_source_month")
    leap_year = ((year % 4 == 0) & (year % 100 != 0)) | (year % 400 == 0)
    return (
        pl.when(month.is_in([1, 3, 5, 7, 8, 10, 12]))
        .then(pl.lit(31))
        .when((month == 2) & leap_year)
        .then(pl.lit(29))
        .when(month == 2)
        .then(pl.lit(28))
        .otherwise(pl.lit(30))
    )


def _record_classification(
    scan: _CalendarScan,
    classification: HistDataCalendarClassification,
    *,
    row_number: int,
    source_member: str,
) -> None:
    scan.session_state_counts[classification.session_state] += 1
    _increment_counts(scan.clock_session_counts, classification.clock_sessions)
    _increment_counts(
        scan.active_session_counts,
        classification.active_sessions
        or (
            (SESSION_MARKET_CLOSED,)
            if classification.session_state == SESSION_STATE_WEEKEND_CLOSURE
            else (SESSION_NO_ACTIVE_WINDOW,)
        ),
    )
    _increment_counts(scan.overlap_counts, classification.overlaps)
    _increment_counts(scan.special_tag_counts, classification.special_tags)
    _increment_counts(scan.holiday_tag_counts, classification.holiday_tags)
    _increment_counts(scan.event_tag_counts, classification.event_tags)
    _increment_counts(scan.calendar_tag_counts, classification.calendar_tags)
    if len(scan.samples) < MAX_CALENDAR_SAMPLES:
        scan.samples.append(
            _CalendarSample(
                row_number=row_number,
                classification=classification,
                source_member=source_member,
            )
        )


def _record_calendar_values(
    scan: _CalendarScan,
    *,
    row_number: int,
    source_member: str,
    timestamp_sample: _TimestampSample | None,
    session_state: str,
    clock_sessions: tuple[str, ...],
    active_sessions: tuple[str, ...],
    overlaps: tuple[str, ...],
    special_tags: tuple[str, ...],
    holiday_tags: tuple[str, ...],
    event_tags: tuple[str, ...],
    calendar_tags: tuple[str, ...],
    calendar_profile: HistDataCalendarProfile,
    asset_class: str,
) -> None:
    scan.session_state_counts[session_state] += 1
    _increment_counts(scan.clock_session_counts, clock_sessions)
    _increment_counts(
        scan.active_session_counts,
        active_sessions
        or (
            (SESSION_MARKET_CLOSED,)
            if session_state == SESSION_STATE_WEEKEND_CLOSURE
            else (SESSION_NO_ACTIVE_WINDOW,)
        ),
    )
    _increment_counts(scan.overlap_counts, overlaps)
    _increment_counts(scan.special_tag_counts, special_tags)
    _increment_counts(scan.holiday_tag_counts, holiday_tags)
    _increment_counts(scan.event_tag_counts, event_tags)
    _increment_counts(scan.calendar_tag_counts, calendar_tags)
    if (
        timestamp_sample is not None
        and len(scan.samples) < MAX_CALENDAR_SAMPLES
    ):
        scan.samples.append(
            _CalendarSample(
                row_number=row_number,
                classification=classify_histdata_timestamp(
                    timestamp_sample.timestamp_utc_ms,
                    source_timestamp=timestamp_sample.timestamp_source,
                    calendar_profile=calendar_profile,
                    asset_class=asset_class,
                ),
                source_member=source_member,
            )
        )


def _summary_finding(
    target: QualityTarget,
    scan: _CalendarScan,
    *,
    source_member: str,
    calendar_profile: HistDataCalendarProfile,
) -> QualityFinding:
    return _finding(
        target,
        code="DOMAIN_CALENDAR_SESSION_SUMMARY",
        message="HistData calendar/session tagging profile.",
        severity=QualitySeverity.INFO,
        metadata={
            **_base_metadata(target, source_member),
            "row_count": scan.row_count,
            "parsed_row_count": scan.parsed_row_count,
            "invalid_timestamp_count": scan.invalid_timestamp_count,
            "session_state_counts": _counter_metadata(
                scan.session_state_counts
            ),
            "clock_session_counts": _counter_metadata(
                scan.clock_session_counts
            ),
            "active_session_counts": _counter_metadata(
                scan.active_session_counts
            ),
            "overlap_counts": _counter_metadata(scan.overlap_counts),
            "special_tag_counts": _counter_metadata(scan.special_tag_counts),
            "holiday_tag_counts": _counter_metadata(scan.holiday_tag_counts),
            "event_tag_counts": _counter_metadata(scan.event_tag_counts),
            "calendar_tag_counts": _counter_metadata(scan.calendar_tag_counts),
            "samples": [sample.to_dict() for sample in scan.samples],
            "calendar_policy": calendar_policy_metadata(calendar_profile),
        },
    )


def _scan_findings(
    target: QualityTarget,
    scan: _CalendarScan,
    *,
    source_member: str,
    calendar_profile: HistDataCalendarProfile,
    profile_missing_severity: QualitySeverity,
) -> tuple[QualityFinding, ...]:
    findings = [
        _summary_finding(
            target,
            scan,
            source_member=source_member,
            calendar_profile=calendar_profile,
        )
    ]
    if not calendar_profile.complete:
        findings.append(
            _finding(
                target,
                code="DOMAIN_CALENDAR_PROFILE_INCOMPLETE",
                message=(
                    "Calendar profile is incomplete or advisory; optional "
                    "holiday, exchange-closure, and event data may be absent."
                ),
                severity=profile_missing_severity,
                metadata={
                    **_base_metadata(target, source_member),
                    "calendar_profile": calendar_profile.to_metadata(),
                    "calendar_policy": calendar_policy_metadata(
                        calendar_profile
                    ),
                    "missing_optional_calendar_data": True,
                },
            )
        )
    return tuple(findings)


def _session_state_for_source(source: datetime) -> str:
    weekday = source.weekday()
    minute = _minute_of_day(source)
    return _session_state_for_source_fields(weekday=weekday, minute=minute)


def _session_state_for_source_fields(*, weekday: int, minute: int) -> str:
    if weekday == 5:
        return SESSION_STATE_WEEKEND_CLOSURE
    if weekday == FX_FRIDAY_CLOSE_WEEKDAY:
        if FRIDAY_CLOSE_WINDOW.contains(minute):
            return SESSION_STATE_FRIDAY_CLOSE
        if minute >= FX_CLOSE_OPEN_MINUTE:
            return SESSION_STATE_WEEKEND_CLOSURE
    if weekday == FX_SUNDAY_OPEN_WEEKDAY:
        if minute < FX_CLOSE_OPEN_MINUTE:
            return SESSION_STATE_WEEKEND_CLOSURE
        if SUNDAY_OPEN_WINDOW.contains(minute):
            return SESSION_STATE_SUNDAY_OPEN
    return SESSION_STATE_MARKET_OPEN


def _clock_sessions_for_utc(utc: datetime) -> tuple[str, ...]:
    return _clock_sessions_for_minute(_minute_of_day(utc))


def _clock_sessions_for_minute(minute: int) -> tuple[str, ...]:
    sessions: list[str] = []
    if 0 <= minute < 9 * 60:
        sessions.append(SESSION_ASIA)
    if 7 * 60 <= minute < 16 * 60:
        sessions.append(SESSION_LONDON)
    if 12 * 60 <= minute < 21 * 60:
        sessions.append(SESSION_NEW_YORK)
    return tuple(sessions)


def _overlaps_for_sessions(sessions: tuple[str, ...]) -> tuple[str, ...]:
    if len(sessions) < 2:
        return ()
    if sessions == (SESSION_ASIA, SESSION_LONDON):
        return ("asia_london_overlap", "multi_session_overlap")
    if sessions == (SESSION_LONDON, SESSION_NEW_YORK):
        return ("london_new_york_overlap", "multi_session_overlap")
    return ("multi_session_overlap",)


def _special_tags(
    source: datetime,
    utc: datetime,
    session_state: str,
) -> tuple[str, ...]:
    source_month_end = _is_month_end(source)
    return _special_tags_for_source_fields(
        source_month=source.month,
        source_day=source.day,
        source_month_length=source.day if source_month_end else source.day + 1,
        source_minute=_minute_of_day(source),
        utc_minute=_minute_of_day(utc),
        session_state=session_state,
    )


def _special_tags_for_source_fields(
    *,
    source_month: int,
    source_day: int,
    source_month_length: int,
    source_minute: int,
    utc_minute: int,
    session_state: str,
) -> tuple[str, ...]:
    tags: list[str] = []
    source_month_end = source_day == source_month_length
    source_quarter_end = source_month in {3, 6, 9, 12} and source_month_end
    source_year_end = source_month == 12 and source_day == 31
    if session_state in {
        SESSION_STATE_WEEKEND_CLOSURE,
        SESSION_STATE_SUNDAY_OPEN,
        SESSION_STATE_FRIDAY_CLOSE,
    }:
        tags.append(session_state)
    if DAILY_ROLLOVER_WINDOW.contains(source_minute):
        tags.append(DAILY_ROLLOVER_WINDOW.name)
    if LONDON_FIX_WINDOW.contains(utc_minute):
        tags.append(LONDON_FIX_WINDOW.name)
        if source_month_end:
            tags.append("month_end_fix_window")
        if source_quarter_end:
            tags.append("quarter_end_fix_window")
        if source_year_end:
            tags.append("year_end_fix_window")
    if source_month_end:
        tags.append("month_end")
    if source_quarter_end:
        tags.append("quarter_end")
    if source_year_end:
        tags.append("year_end")
    return tuple(dict.fromkeys(tags))


def _holiday_tags(
    source: datetime,
    *,
    calendar_profile: HistDataCalendarProfile,
    asset_class: str,
) -> tuple[str, ...]:
    return calendar_profile.holiday_tags_for(source, asset_class=asset_class)


def _event_tags(
    source: datetime,
    *,
    calendar_profile: HistDataCalendarProfile,
    asset_class: str,
) -> tuple[str, ...]:
    return calendar_profile.event_tags_for(source, asset_class=asset_class)


def _holiday_tags_for_source_fields(
    *,
    source_year: int,
    source_month: int,
    source_day: int,
    calendar_profile: HistDataCalendarProfile,
    asset_class: str,
) -> tuple[str, ...]:
    return calendar_profile.holiday_tags_for_fields(
        source_year=source_year,
        source_month=source_month,
        source_day=source_day,
        asset_class=asset_class,
    )


def _event_tags_for_source_fields(
    *,
    source_month: int,
    source_day: int,
    source_day_number: int | None,
    calendar_profile: HistDataCalendarProfile,
    asset_class: str,
) -> tuple[str, ...]:
    return calendar_profile.event_tags_for_fields(
        source_month=source_month,
        source_day=source_day,
        source_day_number=source_day_number,
        asset_class=asset_class,
    )


def _legacy_static_holiday_tags(source: datetime) -> tuple[str, ...]:
    return _holiday_tags_for_source_fields(
        source_year=source.year,
        source_month=source.month,
        source_day=source.day,
        calendar_profile=default_calendar_profile(),
        asset_class="",
    )


def _is_month_end(source: datetime) -> bool:
    return source.day == _month_length(source.year, source.month)


def _month_length(year: int, month: int) -> int:
    next_month_year = year + (1 if month == 12 else 0)
    next_month = 1 if month == 12 else month + 1
    first_next_month = datetime(next_month_year, next_month, 1)
    return (first_next_month - timedelta(days=1)).day


def _is_quarter_end(source: datetime) -> bool:
    return source.month in {3, 6, 9, 12} and _is_month_end(source)


def _is_year_end(source: datetime) -> bool:
    return source.month == 12 and source.day == 31


def _read_text_payload(target: QualityTarget) -> _TextPayload:
    path = Path(target.path)
    if target.kind is QualityTargetKind.CSV:
        try:
            return _TextPayload(data=path.read_bytes())
        except OSError as exc:
            raise _SourceReadError(
                code="DOMAIN_CALENDAR_SOURCE_UNREADABLE",
                message="Target text could not be read for calendar tagging.",
                metadata={"error": str(exc)},
            ) from exc

    if target.kind is QualityTargetKind.ZIP:
        try:
            with zipfile.ZipFile(path) as archive:
                members = tuple(
                    name
                    for name in archive.namelist()
                    if not name.endswith("/") and Path(name).suffix == ".csv"
                )
                if not members:
                    raise _SourceReadError(
                        code="DOMAIN_CALENDAR_ZIP_MEMBER_UNAVAILABLE",
                        message="ZIP archive has no CSV member for calendar "
                        "tagging.",
                        metadata={"member_count": 0},
                    )
                member = sorted(members)[0]
                with archive.open(member) as source:
                    return _TextPayload(
                        data=source.read(),
                        source_member=member,
                    )
        except zipfile.BadZipFile as exc:
            raise _SourceReadError(
                code="DOMAIN_CALENDAR_ZIP_UNREADABLE",
                message="ZIP archive could not be read for calendar tagging.",
                metadata={"error": str(exc)},
            ) from exc
        except OSError as exc:
            raise _SourceReadError(
                code="DOMAIN_CALENDAR_SOURCE_UNREADABLE",
                message="Target archive could not be read for calendar "
                "tagging.",
                metadata={"error": str(exc)},
            ) from exc

    raise _SourceReadError(
        code="DOMAIN_CALENDAR_TARGET_UNSUPPORTED",
        message="Target kind is not supported for calendar tagging.",
        metadata={"kind": target.kind.value},
    )


def _is_ascii_text_target(target: QualityTarget) -> bool:
    return (
        target.data_format == "ascii"
        and target.timeframe in {M1, TICK}
        and target.kind
        in {
            QualityTargetKind.CSV,
            QualityTargetKind.ZIP,
            QualityTargetKind.CACHE,
        }
    )


def _target_asset_class(target: QualityTarget) -> str:
    return str(symbol_metadata_for(target.symbol).asset_class)


def _base_metadata(
    target: QualityTarget,
    source_member: str,
) -> dict[str, JSONValue]:
    return {
        "source_timezone": SOURCE_TIMEZONE,
        "source_utc_offset": SOURCE_UTC_OFFSET,
        "canonical_timezone": CANONICAL_TIMEZONE,
        "utc_normalization_offset_ms": EST_NO_DST_OFFSET_MS,
        "target_period": target.period,
        "symbol": target.symbol,
        "timeframe": target.timeframe,
        "source_member": source_member,
    }


def _invalid_location(
    target: QualityTarget,
    sample: _InvalidTimestampSample,
) -> QualityLocation:
    return QualityLocation(
        path=target.path,
        row_number=sample.row_number,
        timestamp_source=sample.timestamp_source,
        column="datetime",
        metadata={"source_member": sample.source_member, "error": sample.error},
    )


def _finding(
    target: QualityTarget,
    *,
    code: str,
    message: str,
    severity: QualitySeverity,
    metadata: dict[str, JSONValue] | None = None,
    location: QualityLocation | None = None,
) -> QualityFinding:
    return QualityFinding(
        severity=severity,
        code=code,
        message=message,
        rule_id=DOMAIN_CALENDAR_SESSION_RULE_ID,
        target=target,
        location=location or QualityLocation(path=target.path),
        metadata=metadata or {},
    )


def _parse_row(raw: str, delimiter: str) -> list[str]:
    return raw.split(delimiter)


def _increment_counts(counter: Counter[str], values: tuple[str, ...]) -> None:
    for value in values:
        counter[value] += 1


def _counter_metadata(counter: Counter[str]) -> dict[str, JSONValue]:
    return {key: count for key, count in sorted(counter.items())}


def _append_invalid_timestamp(
    samples: list[_InvalidTimestampSample],
    sample: _InvalidTimestampSample,
) -> None:
    if len(samples) < MAX_CALENDAR_SAMPLES:
        samples.append(sample)


def _contains_minute(
    minute_of_day: int,
    *,
    start_minute: int,
    end_minute: int,
) -> bool:
    if start_minute <= end_minute:
        return start_minute <= minute_of_day < end_minute
    return minute_of_day >= start_minute or minute_of_day < end_minute


def _minute_of_day(timestamp: datetime) -> int:
    return timestamp.hour * 60 + timestamp.minute


def _minute_label(minute: int) -> str:
    hour, clock_minute = divmod(minute % (24 * 60), 60)
    return f"{hour:02d}:{clock_minute:02d}"


def _utc_datetime_from_ms(timestamp_utc_ms: int) -> datetime:
    seconds, milliseconds = divmod(timestamp_utc_ms, 1_000)
    return UNIX_EPOCH + timedelta(
        seconds=seconds,
        milliseconds=milliseconds,
    )


def _source_datetime_from_utc_ms(timestamp_utc_ms: int) -> datetime:
    return _utc_datetime_from_ms(timestamp_utc_ms) - timedelta(
        milliseconds=EST_NO_DST_OFFSET_MS
    )


def _utc_iso_from_datetime(timestamp: datetime) -> str:
    if timestamp.microsecond:
        return timestamp.isoformat(timespec="milliseconds").replace(
            "+00:00", "Z"
        )
    return timestamp.isoformat(timespec="seconds").replace("+00:00", "Z")


def _source_iso_from_datetime(timestamp: datetime) -> str:
    naive = timestamp.replace(tzinfo=None)
    if timestamp.microsecond:
        return naive.isoformat(timespec="milliseconds") + SOURCE_UTC_OFFSET
    return naive.isoformat(timespec="seconds") + SOURCE_UTC_OFFSET
