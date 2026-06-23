"""Calendar and market-session tags for HistData quality reports."""

from __future__ import annotations

import csv
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
import zipfile

from histdatacom.data_quality.contracts import (
    QualityFinding,
    QualityLocation,
    QualityRule,
    QualitySeverity,
    QualityTarget,
    QualityTargetKind,
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
) -> HistDataCalendarClassification:
    """Classify one canonical UTC millisecond timestamp.

    HistData source timestamps are first interpreted as EST without DST. The
    source projection is then used for market-open/close, rollover, and
    month/quarter/year-end tags; UTC projection is used for coarse session
    windows and the London fix window.
    """
    utc = _utc_datetime_from_ms(timestamp_utc_ms)
    source = _source_datetime_from_utc_ms(timestamp_utc_ms)
    session_state = _session_state_for_source(source)
    clock_sessions = _clock_sessions_for_utc(utc)
    active_sessions = (
        () if session_state == SESSION_STATE_WEEKEND_CLOSURE else clock_sessions
    )
    overlaps = _overlaps_for_sessions(active_sessions)
    special_tags = _special_tags(source, utc, session_state)
    holiday_tags = _holiday_tags(source)
    calendar_tags = tuple(
        dict.fromkeys(
            (
                *active_sessions,
                *overlaps,
                session_state,
                *special_tags,
                *holiday_tags,
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
        calendar_tags=calendar_tags,
    )


def classify_histdata_source_timestamp(
    value: str,
    timeframe: str,
) -> HistDataCalendarClassification:
    """Parse and classify a raw HistData source timestamp."""
    timestamp_utc_ms = parse_histdata_datetime_to_utc_ms(value, timeframe)
    return classify_histdata_timestamp(
        timestamp_utc_ms,
        source_timestamp=value.strip(),
    )


def calendar_policy_metadata() -> dict[str, JSONValue]:
    """Return the tagging policy embedded in quality-report summaries."""
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
        "holiday_calendar_source": "static_month_day_major_holidays",
        "holiday_calendar_complete": False,
        "holiday_calendar_limitations": (
            "No network-backed or exchange-specific holiday calendar is "
            "bundled; static holiday tags are advisory."
        ),
        "static_major_holidays": [
            holiday.to_metadata() for holiday in STATIC_MAJOR_HOLIDAYS
        ],
        "month_end_policy": "source_calendar_date",
        "fix_window_policy": "advisory_utc_london_4pm_window",
    }


@dataclass(slots=True)
class HistDataCalendarSessionRule:
    """Emit session, rollover, fix, and calendar-regime tags."""

    timestamp_severity: QualitySeverity = QualitySeverity.WARNING
    source_severity: QualitySeverity = QualitySeverity.WARNING
    rule_id: str = DOMAIN_CALENDAR_SESSION_RULE_ID
    description: str = (
        "Tag HistData rows with FX/CFD sessions, open/close, rollover, "
        "fix-window, holiday, and calendar-regime metadata."
    )

    def evaluate(self, target: QualityTarget) -> tuple[QualityFinding, ...]:
        """Return calendar/session findings for one target."""
        if not _is_ascii_text_target(target):
            return ()

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
        )
        findings = [
            _summary_finding(target, scan, source_member=payload.source_member)
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


def calendar_quality_rules() -> tuple[QualityRule, ...]:
    """Return calendar/session quality rules."""
    rule: QualityRule = HistDataCalendarSessionRule()
    return (rule,)


def _scan_calendar_rows(
    target: QualityTarget,
    lines: list[str],
    *,
    delimiter: str,
    timestamp_index: int,
    source_member: str,
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
    _increment_counts(scan.calendar_tag_counts, classification.calendar_tags)
    if len(scan.samples) < MAX_CALENDAR_SAMPLES:
        scan.samples.append(
            _CalendarSample(
                row_number=row_number,
                classification=classification,
                source_member=source_member,
            )
        )


def _summary_finding(
    target: QualityTarget,
    scan: _CalendarScan,
    *,
    source_member: str,
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
            "calendar_tag_counts": _counter_metadata(scan.calendar_tag_counts),
            "samples": [sample.to_dict() for sample in scan.samples],
            "calendar_policy": calendar_policy_metadata(),
        },
    )


def _session_state_for_source(source: datetime) -> str:
    weekday = source.weekday()
    minute = _minute_of_day(source)
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
    minute = _minute_of_day(utc)
    return tuple(
        window.name for window in SESSION_WINDOWS if window.contains(minute)
    )


def _overlaps_for_sessions(sessions: tuple[str, ...]) -> tuple[str, ...]:
    session_set = set(sessions)
    overlaps: list[str] = []
    if {SESSION_ASIA, SESSION_LONDON}.issubset(session_set):
        overlaps.append("asia_london_overlap")
    if {SESSION_LONDON, SESSION_NEW_YORK}.issubset(session_set):
        overlaps.append("london_new_york_overlap")
    if len(session_set) > 1:
        overlaps.append("multi_session_overlap")
    return tuple(overlaps)


def _special_tags(
    source: datetime,
    utc: datetime,
    session_state: str,
) -> tuple[str, ...]:
    tags: list[str] = []
    source_minute = _minute_of_day(source)
    utc_minute = _minute_of_day(utc)
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
        if _is_month_end(source):
            tags.append("month_end_fix_window")
        if _is_quarter_end(source):
            tags.append("quarter_end_fix_window")
        if _is_year_end(source):
            tags.append("year_end_fix_window")
    if _is_month_end(source):
        tags.append("month_end")
    if _is_quarter_end(source):
        tags.append("quarter_end")
    if _is_year_end(source):
        tags.append("year_end")
    return tuple(dict.fromkeys(tags))


def _holiday_tags(source: datetime) -> tuple[str, ...]:
    return tuple(
        f"major_holiday:{holiday.name}"
        for holiday in STATIC_MAJOR_HOLIDAYS
        if holiday.matches(source)
    )


def _is_month_end(source: datetime) -> bool:
    next_month_year = source.year + (1 if source.month == 12 else 0)
    next_month = 1 if source.month == 12 else source.month + 1
    first_next_month = datetime(next_month_year, next_month, 1)
    return source.day == (first_next_month - timedelta(days=1)).day


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
        and target.kind in {QualityTargetKind.CSV, QualityTargetKind.ZIP}
    )


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
    return next(csv.reader((raw,), delimiter=delimiter), [])


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
