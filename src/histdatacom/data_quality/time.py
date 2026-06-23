"""Timestamp and calendar quality checks for HistData ASCII artifacts."""

from __future__ import annotations

import csv
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
import zipfile

from histdatacom.data_quality.contracts import (
    QualityFinding,
    QualityLocation,
    QualityReport,
    QualityRule,
    QualityRuleResult,
    QualityRunRule,
    QualitySeverity,
    QualityTarget,
    QualityTargetKind,
)
from histdatacom.histdata_ascii import (
    EST_NO_DST_OFFSET_MS,
    columns_for_timeframe,
    delimiter_for_timeframe,
    parse_histdata_datetime_to_utc_ms,
)
from histdatacom.runtime_contracts import JSONValue

ASCII_EST_NO_DST_TIME_RULE_ID = "time.ascii.est_no_dst"
ASCII_TIMESTAMP_SEQUENCE_RULE_ID = "time.ascii.sequence"
ASCII_TIMESTAMP_GAP_RULE_ID = "time.ascii.gaps"
ASCII_TIMESTAMP_CONTINUITY_RULE_ID = "time.ascii.continuity"
TIMESTAMP_CONTINUITY_METADATA_KEY = "timestamp_continuity"
SOURCE_TIMEZONE = "EST-no-DST"
SOURCE_UTC_OFFSET = "-05:00"
CANONICAL_TIMEZONE = "UTC"
M1_GRANULARITY_MS = 60_000
EXPECTED_TICK_MILLISECOND_DIGITS = 3
DEFAULT_GAP_BUCKETS_MS = (
    60_000,
    5 * 60_000,
    30 * 60_000,
    60 * 60_000,
    24 * 60 * 60_000,
)
FX_FRIDAY_CLOSE_WEEKDAY = 4
FX_SUNDAY_OPEN_WEEKDAY = 6
FX_CLOSE_OPEN_MINUTE = 17 * 60
MAX_TIMESTAMP_SAMPLES = 5
UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


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
class _TimestampSample:
    row_number: int
    timestamp_source: str
    timestamp_utc_ms: int
    source_period: str
    utc_period: str
    source_member: str = ""
    row_values: tuple[str, ...] = ()

    @property
    def utc_timestamp(self) -> str:
        """Return canonical UTC timestamp text for the sample."""
        return _utc_iso_from_ms(self.timestamp_utc_ms)

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a bounded JSON-compatible timestamp sample."""
        return {
            "row_number": self.row_number,
            "timestamp_source": self.timestamp_source,
            "timestamp_utc_ms": self.timestamp_utc_ms,
            "utc_timestamp": self.utc_timestamp,
            "source_period": self.source_period,
            "utc_period": self.utc_period,
            "source_member": self.source_member,
        }


@dataclass(slots=True)
class _TimestampScan:
    parsed_row_count: int = 0
    invalid_timestamp_count: int = 0
    valid_rows: list[_TimestampSample] = field(default_factory=list)
    samples: list[_TimestampSample] = field(default_factory=list)
    period_mismatch_count: int = 0
    period_mismatches: list[_TimestampSample] = field(default_factory=list)
    utc_month_boundary_count: int = 0
    utc_month_boundaries: list[_TimestampSample] = field(default_factory=list)
    m1_granularity_drift_count: int = 0
    m1_granularity_drifts: list["_TimestampIssueSample"] = field(
        default_factory=list
    )
    tick_precision_mismatch_count: int = 0
    tick_precision_mismatches: list["_TimestampIssueSample"] = field(
        default_factory=list
    )


@dataclass(frozen=True, slots=True)
class _TimestampIssueSample:
    row_number: int
    timestamp_source: str
    timestamp_utc_ms: int | None
    source_member: str = ""
    metadata: dict[str, JSONValue] = field(default_factory=dict)

    @property
    def utc_timestamp(self) -> str:
        """Return canonical UTC timestamp text when one is available."""
        if self.timestamp_utc_ms is None:
            return ""
        return _utc_iso_from_ms(self.timestamp_utc_ms)

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a bounded JSON-compatible issue sample."""
        return {
            "row_number": self.row_number,
            "timestamp_source": self.timestamp_source,
            "timestamp_utc_ms": self.timestamp_utc_ms,
            "utc_timestamp": self.utc_timestamp,
            "source_member": self.source_member,
            "metadata": dict(self.metadata),
        }


@dataclass(slots=True)
class _TimestampSequenceScan:
    non_monotonic_count: int = 0
    non_monotonic_rows: list[_TimestampIssueSample] = field(
        default_factory=list
    )
    m1_duplicate_timestamp_count: int = 0
    m1_duplicate_timestamps: list[_TimestampIssueSample] = field(
        default_factory=list
    )
    tick_duplicate_row_count: int = 0
    tick_duplicate_rows: list[_TimestampIssueSample] = field(
        default_factory=list
    )


@dataclass(frozen=True, slots=True)
class HistDataGapTolerance:
    """Configurable gap and session tolerance windows for HistData checks."""

    expected_interval_ms: int = M1_GRANULARITY_MS
    suspicious_gap_ms: int = 5 * M1_GRANULARITY_MS
    bucket_thresholds_ms: tuple[int, ...] = DEFAULT_GAP_BUCKETS_MS
    session_boundary_grace_ms: int = 60 * M1_GRANULARITY_MS
    dynamic_window_initial_ms: int = 5 * M1_GRANULARITY_MS
    dynamic_window_max_ms: int = 60 * M1_GRANULARITY_MS
    dynamic_window_growth_factor: float = 2.0
    dynamic_window_shrink_factor: float = 0.5

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return JSON-compatible tolerance configuration metadata."""
        return {
            "expected_interval_ms": self.expected_interval_ms,
            "suspicious_gap_ms": self.suspicious_gap_ms,
            "bucket_thresholds_ms": list(self.bucket_thresholds_ms),
            "session_boundary_grace_ms": self.session_boundary_grace_ms,
            "dynamic_window_initial_ms": self.dynamic_window_initial_ms,
            "dynamic_window_max_ms": self.dynamic_window_max_ms,
            "dynamic_window_growth_factor": self.dynamic_window_growth_factor,
            "dynamic_window_shrink_factor": self.dynamic_window_shrink_factor,
        }


@dataclass(frozen=True, slots=True)
class _TimestampGapSample:
    previous: _TimestampSample
    current: _TimestampSample
    gap_ms: int
    classification: str
    dynamic_window_ms: int
    dynamic_score_increment: float

    @property
    def row_number(self) -> int:
        """Return the current row number for location context."""
        return self.current.row_number

    @property
    def timestamp_source(self) -> str:
        """Return the current source timestamp text."""
        return self.current.timestamp_source

    @property
    def timestamp_utc_ms(self) -> int:
        """Return the current canonical UTC timestamp."""
        return self.current.timestamp_utc_ms

    @property
    def source_member(self) -> str:
        """Return source ZIP member context, when available."""
        return self.current.source_member

    @property
    def utc_timestamp(self) -> str:
        """Return canonical UTC timestamp text for the current row."""
        return self.current.utc_timestamp

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a bounded JSON-compatible gap sample."""
        return {
            "previous_row_number": self.previous.row_number,
            "previous_timestamp_source": self.previous.timestamp_source,
            "previous_timestamp_utc_ms": self.previous.timestamp_utc_ms,
            "previous_utc_timestamp": self.previous.utc_timestamp,
            "row_number": self.current.row_number,
            "timestamp_source": self.current.timestamp_source,
            "timestamp_utc_ms": self.current.timestamp_utc_ms,
            "utc_timestamp": self.current.utc_timestamp,
            "gap_ms": self.gap_ms,
            "classification": self.classification,
            "dynamic_window_ms": self.dynamic_window_ms,
            "dynamic_score_increment": self.dynamic_score_increment,
            "source_member": self.source_member,
        }


@dataclass(frozen=True, slots=True)
class _WeekendActivitySample:
    row: _TimestampSample
    session_state: str

    @property
    def utc_timestamp(self) -> str:
        """Return canonical UTC timestamp text for the row."""
        return self.row.utc_timestamp

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a bounded JSON-compatible weekend activity sample."""
        return {
            "row_number": self.row.row_number,
            "timestamp_source": self.row.timestamp_source,
            "timestamp_utc_ms": self.row.timestamp_utc_ms,
            "utc_timestamp": self.row.utc_timestamp,
            "session_state": self.session_state,
            "source_member": self.row.source_member,
        }


@dataclass(slots=True)
class _TimestampGapScan:
    pair_count: int = 0
    tracked_gap_count: int = 0
    max_gap_ms: int = 0
    max_gap: _TimestampGapSample | None = None
    bucket_counts: dict[str, int] = field(default_factory=dict)
    expected_session_closure_count: int = 0
    expected_session_closures: list[_TimestampGapSample] = field(
        default_factory=list
    )
    suspicious_gap_count: int = 0
    suspicious_gaps: list[_TimestampGapSample] = field(default_factory=list)
    weekend_activity_count: int = 0
    weekend_activity: list[_WeekendActivitySample] = field(default_factory=list)
    dynamic_gap_score: float = 0.0
    final_dynamic_window_ms: int = 0


@dataclass(frozen=True, order=True, slots=True)
class _ContinuityGroupKey:
    data_format: str
    timeframe: str
    symbol: str

    @classmethod
    def from_target(cls, target: QualityTarget) -> "_ContinuityGroupKey | None":
        if not (target.data_format and target.timeframe and target.symbol):
            return None
        return cls(
            data_format=_normalize_data_format(target.data_format),
            timeframe=_normalize_timeframe(target.timeframe),
            symbol=_normalize_symbol(target.symbol),
        )

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "data_format": self.data_format,
            "timeframe": self.timeframe,
            "symbol": self.symbol,
        }


@dataclass(frozen=True, slots=True)
class _ContinuityBoundary:
    target: QualityTarget
    first: _TimestampSample
    last: _TimestampSample
    parsed_row_count: int
    invalid_timestamp_count: int
    source_member: str

    @property
    def period(self) -> str:
        return str(self.target.period)

    @property
    def path(self) -> str:
        return str(self.target.path)

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "path": self.target.path,
            "kind": self.target.kind.value,
            "period": self.target.period,
            "source_member": self.source_member,
            "parsed_row_count": self.parsed_row_count,
            "invalid_timestamp_count": self.invalid_timestamp_count,
            "first": self.first.to_dict(),
            "last": self.last.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class _ContinuityComparison:
    key: _ContinuityGroupKey
    previous: _ContinuityBoundary
    current: _ContinuityBoundary
    gap_ms: int
    classification: str
    missing_periods: tuple[str, ...] = ()

    @property
    def previous_path(self) -> str:
        return self.previous.path

    @property
    def current_path(self) -> str:
        return self.current.path

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "group": self.key.to_dict(),
            "classification": self.classification,
            "gap_ms": self.gap_ms,
            "missing_periods": list(self.missing_periods),
            "previous": self.previous.to_dict(),
            "current": self.current.to_dict(),
        }


@dataclass(slots=True)
class _ContinuityScan:
    target_count: int = 0
    candidate_target_count: int = 0
    comparable_target_count: int = 0
    skipped_target_count: int = 0
    group_count: int = 0
    adjacent_pair_count: int = 0
    clean_boundary_count: int = 0
    missing_period_count: int = 0
    period_gap_count: int = 0
    duplicate_overlap_count: int = 0
    reversed_order_count: int = 0
    suspicious_gap_count: int = 0
    expected_session_closure_count: int = 0
    missing_periods: list[_ContinuityComparison] = field(default_factory=list)
    duplicate_overlaps: list[_ContinuityComparison] = field(
        default_factory=list
    )
    reversed_order: list[_ContinuityComparison] = field(default_factory=list)
    suspicious_gaps: list[_ContinuityComparison] = field(default_factory=list)
    expected_session_closures: list[_ContinuityComparison] = field(
        default_factory=list
    )


@dataclass(slots=True)
class HistDataAsciiEstNoDstTimeRule:
    """Validate HistData fixed EST-no-DST timestamp normalization."""

    rule_id: str = ASCII_EST_NO_DST_TIME_RULE_ID
    description: str = (
        "Validate HistData ASCII source timestamps as fixed EST without DST "
        "before canonical UTC normalization."
    )

    def evaluate(self, target: QualityTarget) -> tuple[QualityFinding, ...]:
        """Return EST-no-DST timestamp findings for one target."""
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
                    code="ASCII_TIME_METADATA_UNSUPPORTED",
                    message="Target metadata does not describe a supported "
                    "HistData ASCII timeframe.",
                    metadata={
                        "timeframe": target.timeframe,
                        "error": str(exc),
                    },
                ),
            )
        except UnicodeDecodeError as exc:
            return (
                _finding(
                    target,
                    code="ASCII_TIME_TEXT_ENCODING_INVALID",
                    message="ASCII file does not decode as strict UTF-8 for "
                    "timestamp checks.",
                    metadata={
                        "encoding": "utf-8",
                        "error": str(exc),
                        "byte_start": exc.start,
                        "byte_end": exc.end,
                        "source_member": payload.source_member,
                    },
                ),
            )
        except _SourceReadError as exc:
            return (
                _finding(
                    target,
                    code=exc.code,
                    message=exc.message,
                    metadata=exc.metadata,
                ),
            )

        scan = _scan_timestamp_rows(
            text,
            target=target,
            delimiter=delimiter,
            columns=columns,
            source_member=payload.source_member,
        )
        return _timestamp_findings(target=target, scan=scan)


@dataclass(slots=True)
class HistDataAsciiTimestampSequenceRule:
    """Validate per-file timestamp ordering, uniqueness, and precision."""

    rule_id: str = ASCII_TIMESTAMP_SEQUENCE_RULE_ID
    description: str = (
        "Validate HistData ASCII timestamp monotonicity, duplicate rows, and "
        "format granularity after EST-no-DST normalization."
    )

    def evaluate(self, target: QualityTarget) -> tuple[QualityFinding, ...]:
        """Return timestamp sequence findings for one target."""
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
                    code="ASCII_TIME_METADATA_UNSUPPORTED",
                    message="Target metadata does not describe a supported "
                    "HistData ASCII timeframe.",
                    rule_id=self.rule_id,
                    metadata={
                        "timeframe": target.timeframe,
                        "error": str(exc),
                    },
                ),
            )
        except UnicodeDecodeError as exc:
            return (
                _finding(
                    target,
                    code="ASCII_TIME_TEXT_ENCODING_INVALID",
                    message="ASCII file does not decode as strict UTF-8 for "
                    "timestamp sequence checks.",
                    rule_id=self.rule_id,
                    metadata={
                        "encoding": "utf-8",
                        "error": str(exc),
                        "byte_start": exc.start,
                        "byte_end": exc.end,
                        "source_member": payload.source_member,
                    },
                ),
            )
        except _SourceReadError as exc:
            return (
                _finding(
                    target,
                    code=exc.code,
                    message=exc.message,
                    rule_id=self.rule_id,
                    metadata=exc.metadata,
                ),
            )

        scan = _scan_timestamp_rows(
            text,
            target=target,
            delimiter=delimiter,
            columns=columns,
            source_member=payload.source_member,
        )
        sequence = _scan_timestamp_sequence(target, scan.valid_rows)
        return _timestamp_sequence_findings(
            target=target,
            scan=scan,
            sequence=sequence,
        )


@dataclass(slots=True)
class HistDataAsciiTimestampGapRule:
    """Profile timestamp gaps, market closures, and weekend activity."""

    tolerance: HistDataGapTolerance = field(
        default_factory=HistDataGapTolerance
    )
    warning_severity: QualitySeverity = QualitySeverity.WARNING
    rule_id: str = ASCII_TIMESTAMP_GAP_RULE_ID
    description: str = (
        "Compute gap buckets, expected FX closure gaps, unexpected weekend "
        "activity, and dynamic tolerance scores over normalized timestamps."
    )

    def evaluate(self, target: QualityTarget) -> tuple[QualityFinding, ...]:
        """Return gap and session findings for one target."""
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
                    code="ASCII_TIME_METADATA_UNSUPPORTED",
                    message="Target metadata does not describe a supported "
                    "HistData ASCII timeframe.",
                    rule_id=self.rule_id,
                    metadata={
                        "timeframe": target.timeframe,
                        "error": str(exc),
                    },
                ),
            )
        except UnicodeDecodeError as exc:
            return (
                _finding(
                    target,
                    code="ASCII_TIME_TEXT_ENCODING_INVALID",
                    message="ASCII file does not decode as strict UTF-8 for "
                    "timestamp gap checks.",
                    rule_id=self.rule_id,
                    metadata={
                        "encoding": "utf-8",
                        "error": str(exc),
                        "byte_start": exc.start,
                        "byte_end": exc.end,
                        "source_member": payload.source_member,
                    },
                ),
            )
        except _SourceReadError as exc:
            return (
                _finding(
                    target,
                    code=exc.code,
                    message=exc.message,
                    rule_id=self.rule_id,
                    metadata=exc.metadata,
                ),
            )

        timestamp_scan = _scan_timestamp_rows(
            text,
            target=target,
            delimiter=delimiter,
            columns=columns,
            source_member=payload.source_member,
        )
        gap_scan = _scan_timestamp_gaps(
            timestamp_scan.valid_rows,
            tolerance=self.tolerance,
        )
        return _timestamp_gap_findings(
            target=target,
            timestamp_scan=timestamp_scan,
            gap_scan=gap_scan,
            tolerance=self.tolerance,
            severity=self.warning_severity,
            rule_id=self.rule_id,
        )


@dataclass(slots=True)
class HistDataAsciiTimestampContinuityRule:
    """Validate cross-file timestamp continuity for adjacent monthly files."""

    tolerance: HistDataGapTolerance = field(
        default_factory=HistDataGapTolerance
    )
    warning_severity: QualitySeverity = QualitySeverity.WARNING
    rule_id: str = ASCII_TIMESTAMP_CONTINUITY_RULE_ID
    description: str = (
        "Compare adjacent monthly HistData ASCII file boundary timestamps by "
        "symbol, format, and timeframe."
    )

    def evaluate_run(
        self,
        targets: Iterable[QualityTarget],
        *,
        metadata: Mapping[str, JSONValue] | None = None,
    ) -> QualityReport:
        """Return cross-file continuity findings for the full target set."""
        target_tuple = tuple(targets)
        scan = _scan_timestamp_continuity(
            target_tuple,
            tolerance=self.tolerance,
        )
        payload = _timestamp_continuity_payload(
            scan,
            tolerance=self.tolerance,
        )
        if not _has_continuity_surface(scan):
            return QualityReport(
                metadata={TIMESTAMP_CONTINUITY_METADATA_KEY: payload},
            )

        continuity_target = _continuity_target(
            target_tuple,
            metadata=metadata,
            payload=payload,
        )
        findings = _timestamp_continuity_findings(
            continuity_target,
            scan=scan,
            tolerance=self.tolerance,
            severity=self.warning_severity,
            rule_id=self.rule_id,
        )
        return QualityReport(
            targets=(continuity_target,),
            rule_results=(
                QualityRuleResult(
                    rule_id=self.rule_id,
                    target=continuity_target,
                    findings=findings,
                ),
            ),
            metadata={TIMESTAMP_CONTINUITY_METADATA_KEY: payload},
        )


def time_quality_rules() -> tuple[QualityRule, ...]:
    """Return timestamp quality rules in deterministic execution order."""
    est_no_dst_rule: QualityRule = HistDataAsciiEstNoDstTimeRule()
    sequence_rule: QualityRule = HistDataAsciiTimestampSequenceRule()
    gap_rule: QualityRule = HistDataAsciiTimestampGapRule()
    return (est_no_dst_rule, sequence_rule, gap_rule)


def time_quality_run_rules() -> tuple[QualityRunRule, ...]:
    """Return run-scoped timestamp quality rules."""
    continuity_rule: QualityRunRule = HistDataAsciiTimestampContinuityRule()
    return (continuity_rule,)


def _is_ascii_text_target(target: QualityTarget) -> bool:
    return target.data_format == "ascii" and target.kind in {
        QualityTargetKind.CSV,
        QualityTargetKind.ZIP,
    }


def _read_text_payload(target: QualityTarget) -> _TextPayload:
    path = Path(target.path)
    if target.kind is QualityTargetKind.CSV:
        try:
            return _TextPayload(path.read_bytes())
        except OSError as exc:
            raise _source_error(
                "ASCII_TIME_SOURCE_UNREADABLE",
                "ASCII file could not be read for timestamp checks.",
                exc,
            ) from exc

    try:
        with zipfile.ZipFile(path) as archive:
            members = tuple(
                name
                for name in archive.namelist()
                if not name.endswith("/")
                and Path(name).suffix.lower() == ".csv"
            )
            if len(members) != 1:
                raise _SourceReadError(
                    code="ASCII_TIME_ZIP_MEMBER_UNAVAILABLE",
                    message="ZIP archive must contain exactly one CSV member "
                    "for timestamp checks.",
                    metadata={"csv_members": list(members)},
                )
            member = members[0]
            return _TextPayload(
                data=archive.read(member),
                source_member=member,
            )
    except _SourceReadError:
        raise
    except zipfile.BadZipFile as exc:
        raise _source_error(
            "ASCII_TIME_ZIP_UNREADABLE",
            "ZIP archive could not be opened for timestamp checks.",
            exc,
        ) from exc
    except (KeyError, OSError) as exc:
        raise _source_error(
            "ASCII_TIME_SOURCE_UNREADABLE",
            "ASCII source could not be read for timestamp checks.",
            exc,
        ) from exc


def _source_error(
    code: str,
    message: str,
    exc: Exception,
) -> _SourceReadError:
    return _SourceReadError(
        code=code,
        message=message,
        metadata={"error_type": type(exc).__name__, "error": str(exc)},
    )


def _scan_timestamp_rows(
    text: str,
    *,
    target: QualityTarget,
    delimiter: str,
    columns: tuple[str, ...],
    source_member: str,
) -> _TimestampScan:
    scan = _TimestampScan()
    expected_count = len(columns)
    for row_number, raw in enumerate(text.splitlines(), start=1):
        if not raw.strip():
            continue
        row = _parse_row(raw, delimiter)
        if len(row) != expected_count or tuple(row) == columns:
            continue

        timestamp_source = row[0].strip()
        row_values = tuple(cell.strip() for cell in row)
        timestamp_utc_ms: int | None = None
        try:
            timestamp_utc_ms = parse_histdata_datetime_to_utc_ms(
                timestamp_source,
                target.timeframe,
            )
        except ValueError:
            scan.invalid_timestamp_count += 1

        _record_raw_timestamp_shape(
            scan,
            row_number=row_number,
            timestamp_source=timestamp_source,
            timestamp_utc_ms=timestamp_utc_ms,
            source_member=source_member,
            timeframe=target.timeframe,
        )

        if timestamp_utc_ms is None:
            continue

        sample = _TimestampSample(
            row_number=row_number,
            timestamp_source=timestamp_source,
            timestamp_utc_ms=timestamp_utc_ms,
            source_period=_source_period(timestamp_source),
            utc_period=_utc_period(timestamp_utc_ms),
            source_member=source_member,
            row_values=row_values,
        )
        scan.parsed_row_count += 1
        scan.valid_rows.append(sample)
        _append_sample(scan.samples, sample)

        if target.period and sample.source_period != target.period:
            scan.period_mismatch_count += 1
            _append_sample(scan.period_mismatches, sample)
            continue
        if target.period and sample.source_period != sample.utc_period:
            scan.utc_month_boundary_count += 1
            _append_sample(scan.utc_month_boundaries, sample)
    return scan


def _timestamp_findings(
    *,
    target: QualityTarget,
    scan: _TimestampScan,
) -> tuple[QualityFinding, ...]:
    findings: list[QualityFinding] = [
        _finding(
            target,
            code="ASCII_TIMESTAMP_EST_NO_DST_SUMMARY",
            message="ASCII timestamps were interpreted as fixed EST-no-DST "
            "source times and normalized to UTC.",
            severity=QualitySeverity.INFO,
            metadata={
                **_base_metadata(
                    target,
                    source_member=(
                        scan.samples[0].source_member if scan.samples else ""
                    ),
                ),
                "parsed_row_count": scan.parsed_row_count,
                "invalid_timestamp_count": scan.invalid_timestamp_count,
                "source_period_mismatch_count": scan.period_mismatch_count,
                "utc_month_boundary_count": scan.utc_month_boundary_count,
                "samples": _samples(scan.samples),
            },
        )
    ]

    if scan.period_mismatches:
        findings.append(
            _sample_finding(
                target,
                code="ASCII_TIMESTAMP_SOURCE_PERIOD_MISMATCH",
                message="Timestamp source month does not match the target "
                "file period.",
                severity=QualitySeverity.ERROR,
                samples=scan.period_mismatches,
                row_count=scan.period_mismatch_count,
            )
        )
    if scan.utc_month_boundaries:
        findings.append(
            _sample_finding(
                target,
                code="ASCII_TIMESTAMP_UTC_MONTH_BOUNDARY",
                message="Timestamp source month differs from canonical UTC "
                "month; file membership is evaluated in source EST-no-DST "
                "time.",
                severity=QualitySeverity.INFO,
                samples=scan.utc_month_boundaries,
                row_count=scan.utc_month_boundary_count,
            )
        )
    return tuple(findings)


def _scan_timestamp_sequence(
    target: QualityTarget,
    rows: list[_TimestampSample],
) -> _TimestampSequenceScan:
    scan = _TimestampSequenceScan()
    previous: _TimestampSample | None = None
    seen_m1_timestamps: dict[int, _TimestampSample] = {}
    seen_tick_rows: dict[tuple[str, ...], _TimestampSample] = {}

    for row in rows:
        if (
            previous is not None
            and row.timestamp_utc_ms < previous.timestamp_utc_ms
        ):
            scan.non_monotonic_count += 1
            _append_issue_sample(
                scan.non_monotonic_rows,
                _TimestampIssueSample(
                    row_number=row.row_number,
                    timestamp_source=row.timestamp_source,
                    timestamp_utc_ms=row.timestamp_utc_ms,
                    source_member=row.source_member,
                    metadata={
                        "previous_row_number": previous.row_number,
                        "previous_timestamp_source": previous.timestamp_source,
                        "previous_timestamp_utc_ms": previous.timestamp_utc_ms,
                        "previous_utc_timestamp": previous.utc_timestamp,
                    },
                ),
            )
        previous = row

        if target.timeframe == "M1":
            duplicate = seen_m1_timestamps.get(row.timestamp_utc_ms)
            if duplicate is not None:
                scan.m1_duplicate_timestamp_count += 1
                _append_issue_sample(
                    scan.m1_duplicate_timestamps,
                    _TimestampIssueSample(
                        row_number=row.row_number,
                        timestamp_source=row.timestamp_source,
                        timestamp_utc_ms=row.timestamp_utc_ms,
                        source_member=row.source_member,
                        metadata={
                            "duplicate_of_row": duplicate.row_number,
                            "duplicate_timestamp_utc_ms": row.timestamp_utc_ms,
                            "duplicate_utc_timestamp": row.utc_timestamp,
                            "dedupe_policy": "report-only",
                        },
                    ),
                )
            else:
                seen_m1_timestamps[row.timestamp_utc_ms] = row

        if target.timeframe == "T":
            duplicate = seen_tick_rows.get(row.row_values)
            if duplicate is not None:
                scan.tick_duplicate_row_count += 1
                _append_issue_sample(
                    scan.tick_duplicate_rows,
                    _TimestampIssueSample(
                        row_number=row.row_number,
                        timestamp_source=row.timestamp_source,
                        timestamp_utc_ms=row.timestamp_utc_ms,
                        source_member=row.source_member,
                        metadata={
                            "duplicate_of_row": duplicate.row_number,
                            "duplicate_row_values": list(row.row_values),
                            "dedupe_policy": "report-only",
                        },
                    ),
                )
            else:
                seen_tick_rows[row.row_values] = row

    return scan


def _timestamp_sequence_findings(
    *,
    target: QualityTarget,
    scan: _TimestampScan,
    sequence: _TimestampSequenceScan,
) -> tuple[QualityFinding, ...]:
    source_member = scan.samples[0].source_member if scan.samples else ""
    findings: list[QualityFinding] = [
        _finding(
            target,
            code="ASCII_TIMESTAMP_SEQUENCE_SUMMARY",
            message="ASCII timestamp ordering, duplicate, and precision "
            "profile.",
            severity=QualitySeverity.INFO,
            rule_id=ASCII_TIMESTAMP_SEQUENCE_RULE_ID,
            metadata={
                **_base_metadata(target, source_member=source_member),
                "parsed_row_count": scan.parsed_row_count,
                "invalid_timestamp_count": scan.invalid_timestamp_count,
                "non_monotonic_count": sequence.non_monotonic_count,
                "m1_duplicate_timestamp_count": (
                    sequence.m1_duplicate_timestamp_count
                ),
                "tick_duplicate_row_count": sequence.tick_duplicate_row_count,
                "m1_granularity_drift_count": (scan.m1_granularity_drift_count),
                "tick_precision_mismatch_count": (
                    scan.tick_precision_mismatch_count
                ),
                "m1_expected_granularity_ms": M1_GRANULARITY_MS,
                "tick_expected_fractional_digits": (
                    EXPECTED_TICK_MILLISECOND_DIGITS
                ),
                "duplicate_policy": "detect-only",
            },
        )
    ]

    if sequence.non_monotonic_rows:
        findings.append(
            _issue_sample_finding(
                target,
                code="ASCII_TIMESTAMP_NON_MONOTONIC",
                message="Timestamp order decreases within the file.",
                severity=QualitySeverity.WARNING,
                rule_id=ASCII_TIMESTAMP_SEQUENCE_RULE_ID,
                samples=sequence.non_monotonic_rows,
                row_count=sequence.non_monotonic_count,
            )
        )
    if sequence.m1_duplicate_timestamps:
        findings.append(
            _issue_sample_finding(
                target,
                code="ASCII_M1_DUPLICATE_TIMESTAMP",
                message="M1 file contains duplicate normalized timestamps.",
                severity=QualitySeverity.WARNING,
                rule_id=ASCII_TIMESTAMP_SEQUENCE_RULE_ID,
                samples=sequence.m1_duplicate_timestamps,
                row_count=sequence.m1_duplicate_timestamp_count,
            )
        )
    if sequence.tick_duplicate_rows:
        findings.append(
            _issue_sample_finding(
                target,
                code="ASCII_TICK_DUPLICATE_ROW",
                message="Tick file contains exact duplicate timestamp, bid, "
                "ask, and volume rows.",
                severity=QualitySeverity.WARNING,
                rule_id=ASCII_TIMESTAMP_SEQUENCE_RULE_ID,
                samples=sequence.tick_duplicate_rows,
                row_count=sequence.tick_duplicate_row_count,
            )
        )
    if scan.m1_granularity_drifts:
        findings.append(
            _issue_sample_finding(
                target,
                code="ASCII_M1_GRANULARITY_DRIFT",
                message="M1 timestamp is not aligned to a one-minute "
                "boundary.",
                severity=QualitySeverity.ERROR,
                rule_id=ASCII_TIMESTAMP_SEQUENCE_RULE_ID,
                samples=scan.m1_granularity_drifts,
                row_count=scan.m1_granularity_drift_count,
            )
        )
    if scan.tick_precision_mismatches:
        findings.append(
            _issue_sample_finding(
                target,
                code="ASCII_TICK_PRECISION_MISMATCH",
                message="Tick timestamp does not use the expected "
                "millisecond precision width.",
                severity=QualitySeverity.ERROR,
                rule_id=ASCII_TIMESTAMP_SEQUENCE_RULE_ID,
                samples=scan.tick_precision_mismatches,
                row_count=scan.tick_precision_mismatch_count,
            )
        )

    return tuple(findings)


def _scan_timestamp_gaps(
    rows: list[_TimestampSample],
    *,
    tolerance: HistDataGapTolerance,
) -> _TimestampGapScan:
    scan = _TimestampGapScan(
        bucket_counts=_empty_gap_bucket_counts(tolerance),
        final_dynamic_window_ms=tolerance.dynamic_window_initial_ms,
    )
    dynamic_window_ms = tolerance.dynamic_window_initial_ms

    for row in rows:
        if _is_weekend_closure_row(row):
            scan.weekend_activity_count += 1
            _append_weekend_activity_sample(
                scan.weekend_activity,
                _WeekendActivitySample(
                    row=row,
                    session_state="weekend_closure",
                ),
            )

    for previous, current in zip(rows, rows[1:], strict=False):
        gap_ms = current.timestamp_utc_ms - previous.timestamp_utc_ms
        if gap_ms <= 0:
            continue

        scan.pair_count += 1
        if gap_ms > scan.max_gap_ms:
            scan.max_gap_ms = gap_ms

        if gap_ms <= tolerance.expected_interval_ms:
            dynamic_window_ms = _grow_dynamic_window(
                dynamic_window_ms,
                tolerance,
            )
            scan.final_dynamic_window_ms = dynamic_window_ms
            continue

        scan.tracked_gap_count += 1
        _increment_gap_buckets(scan.bucket_counts, gap_ms, tolerance)
        classification = _classify_gap(previous, current, gap_ms, tolerance)
        score_increment = _dynamic_score_increment(
            gap_ms,
            classification=classification,
            dynamic_window_ms=dynamic_window_ms,
            tolerance=tolerance,
        )
        sample = _TimestampGapSample(
            previous=previous,
            current=current,
            gap_ms=gap_ms,
            classification=classification,
            dynamic_window_ms=dynamic_window_ms,
            dynamic_score_increment=score_increment,
        )
        if gap_ms == scan.max_gap_ms:
            scan.max_gap = sample

        if classification == "expected_session_closure":
            scan.expected_session_closure_count += 1
            _append_gap_sample(scan.expected_session_closures, sample)
            dynamic_window_ms = _grow_dynamic_window(
                dynamic_window_ms,
                tolerance,
            )
        elif classification == "suspicious":
            scan.suspicious_gap_count += 1
            _append_gap_sample(scan.suspicious_gaps, sample)
            scan.dynamic_gap_score += score_increment
            dynamic_window_ms = _shrink_dynamic_window(
                dynamic_window_ms,
                tolerance,
            )
        else:
            dynamic_window_ms = _grow_dynamic_window(
                dynamic_window_ms,
                tolerance,
            )

        scan.final_dynamic_window_ms = dynamic_window_ms

    scan.dynamic_gap_score = round(scan.dynamic_gap_score, 4)
    return scan


def _timestamp_gap_findings(
    *,
    target: QualityTarget,
    timestamp_scan: _TimestampScan,
    gap_scan: _TimestampGapScan,
    tolerance: HistDataGapTolerance,
    severity: QualitySeverity,
    rule_id: str,
) -> tuple[QualityFinding, ...]:
    source_member = (
        timestamp_scan.samples[0].source_member
        if timestamp_scan.samples
        else ""
    )
    findings: list[QualityFinding] = [
        _finding(
            target,
            code="ASCII_TIMESTAMP_GAP_SUMMARY",
            message="ASCII timestamp gap distribution and market-session "
            "profile.",
            severity=QualitySeverity.INFO,
            rule_id=rule_id,
            metadata={
                **_base_metadata(target, source_member=source_member),
                "parsed_row_count": timestamp_scan.parsed_row_count,
                "invalid_timestamp_count": (
                    timestamp_scan.invalid_timestamp_count
                ),
                "pair_count": gap_scan.pair_count,
                "tracked_gap_count": gap_scan.tracked_gap_count,
                "max_gap_ms": gap_scan.max_gap_ms,
                "gap_bucket_counts": dict(gap_scan.bucket_counts),
                "expected_session_closure_count": (
                    gap_scan.expected_session_closure_count
                ),
                "suspicious_gap_count": gap_scan.suspicious_gap_count,
                "weekend_activity_count": gap_scan.weekend_activity_count,
                "dynamic_gap_score": gap_scan.dynamic_gap_score,
                "final_dynamic_window_ms": gap_scan.final_dynamic_window_ms,
                "dynamic_window_policy": "inverted-tcp-backoff",
                "tolerance": tolerance.to_metadata(),
                "max_gap": (
                    {}
                    if gap_scan.max_gap is None
                    else gap_scan.max_gap.to_dict()
                ),
                "samples": _gap_samples(
                    [
                        *gap_scan.suspicious_gaps,
                        *gap_scan.expected_session_closures,
                    ][:MAX_TIMESTAMP_SAMPLES]
                ),
            },
        )
    ]

    if gap_scan.expected_session_closures:
        findings.append(
            _gap_sample_finding(
                target,
                code="ASCII_TIMESTAMP_EXPECTED_SESSION_CLOSURE_GAP",
                message="Timestamp gap matches the configured FX weekend "
                "closure tolerance window.",
                severity=QualitySeverity.INFO,
                rule_id=rule_id,
                samples=gap_scan.expected_session_closures,
                row_count=gap_scan.expected_session_closure_count,
            )
        )

    if gap_scan.suspicious_gaps:
        findings.append(
            _gap_sample_finding(
                target,
                code="ASCII_TIMESTAMP_SUSPICIOUS_GAP",
                message="Timestamp gap exceeds the suspicious-gap tolerance "
                "and does not match an expected session closure.",
                severity=severity,
                rule_id=rule_id,
                samples=gap_scan.suspicious_gaps,
                row_count=gap_scan.suspicious_gap_count,
            )
        )

    if gap_scan.weekend_activity:
        findings.append(
            _weekend_activity_finding(
                target,
                code="ASCII_TIMESTAMP_WEEKEND_ACTIVITY",
                message="Timestamp falls inside the configured FX weekend "
                "closure window.",
                severity=severity,
                rule_id=rule_id,
                samples=gap_scan.weekend_activity,
                row_count=gap_scan.weekend_activity_count,
            )
        )

    return tuple(findings)


def _scan_timestamp_continuity(
    targets: tuple[QualityTarget, ...],
    *,
    tolerance: HistDataGapTolerance,
) -> _ContinuityScan:
    scan = _ContinuityScan(target_count=len(targets))
    boundaries_by_group: dict[
        _ContinuityGroupKey, dict[str, list[_ContinuityBoundary]]
    ] = defaultdict(lambda: defaultdict(list))

    for target in targets:
        if not _is_ascii_text_target(target):
            continue
        scan.candidate_target_count += 1
        key = _ContinuityGroupKey.from_target(target)
        if key is None or not _valid_period(target.period):
            scan.skipped_target_count += 1
            continue
        boundary = _timestamp_boundary_for_target(target)
        if boundary is None:
            scan.skipped_target_count += 1
            continue
        scan.comparable_target_count += 1
        boundaries_by_group[key][target.period].append(boundary)

    scan.group_count = len(boundaries_by_group)
    for key, period_boundaries in sorted(boundaries_by_group.items()):
        representatives = {
            period: _preferred_boundary(boundaries)
            for period, boundaries in period_boundaries.items()
        }
        periods = tuple(sorted(representatives))
        for previous_period, current_period in zip(
            periods,
            periods[1:],
            strict=False,
        ):
            previous = representatives[previous_period]
            current = representatives[current_period]
            missing_periods = _missing_periods_between(
                previous_period,
                current_period,
            )
            gap_ms = (
                current.first.timestamp_utc_ms - previous.last.timestamp_utc_ms
            )
            if missing_periods:
                comparison = _ContinuityComparison(
                    key=key,
                    previous=previous,
                    current=current,
                    gap_ms=gap_ms,
                    classification="missing_period",
                    missing_periods=missing_periods,
                )
                scan.period_gap_count += 1
                scan.missing_period_count += len(missing_periods)
                _append_continuity_sample(scan.missing_periods, comparison)
                continue

            scan.adjacent_pair_count += 1
            comparison = _classify_continuity_pair(
                key=key,
                previous=previous,
                current=current,
                gap_ms=gap_ms,
                tolerance=tolerance,
            )
            if comparison.classification == "clean":
                scan.clean_boundary_count += 1
            elif comparison.classification == "expected_session_closure":
                scan.expected_session_closure_count += 1
                _append_continuity_sample(
                    scan.expected_session_closures,
                    comparison,
                )
            elif comparison.classification == "duplicate_overlap":
                scan.duplicate_overlap_count += 1
                _append_continuity_sample(scan.duplicate_overlaps, comparison)
            elif comparison.classification == "reversed_order":
                scan.reversed_order_count += 1
                _append_continuity_sample(scan.reversed_order, comparison)
            elif comparison.classification == "suspicious_gap":
                scan.suspicious_gap_count += 1
                _append_continuity_sample(scan.suspicious_gaps, comparison)

    return scan


def _timestamp_boundary_for_target(
    target: QualityTarget,
) -> _ContinuityBoundary | None:
    try:
        delimiter = delimiter_for_timeframe(target.timeframe)
        columns = columns_for_timeframe(target.timeframe)
        payload = _read_text_payload(target)
        text = payload.data.decode("utf-8")
    except (ValueError, UnicodeDecodeError, _SourceReadError):
        return None

    scan = _scan_timestamp_rows(
        text,
        target=target,
        delimiter=delimiter,
        columns=columns,
        source_member=payload.source_member,
    )
    if not scan.valid_rows:
        return None
    return _ContinuityBoundary(
        target=target,
        first=scan.valid_rows[0],
        last=scan.valid_rows[-1],
        parsed_row_count=scan.parsed_row_count,
        invalid_timestamp_count=scan.invalid_timestamp_count,
        source_member=payload.source_member,
    )


def _preferred_boundary(
    boundaries: list[_ContinuityBoundary],
) -> _ContinuityBoundary:
    return sorted(boundaries, key=_boundary_sort_key)[0]


def _boundary_sort_key(
    boundary: _ContinuityBoundary,
) -> tuple[int, str]:
    kind_rank = {
        QualityTargetKind.CSV: 0,
        QualityTargetKind.ZIP: 1,
    }.get(boundary.target.kind, 2)
    return (kind_rank, boundary.target.path)


def _classify_continuity_pair(
    *,
    key: _ContinuityGroupKey,
    previous: _ContinuityBoundary,
    current: _ContinuityBoundary,
    gap_ms: int,
    tolerance: HistDataGapTolerance,
) -> _ContinuityComparison:
    classification = "clean"
    if gap_ms < 0:
        classification = "reversed_order"
    elif gap_ms == 0:
        classification = "duplicate_overlap"
    elif _is_expected_session_closure_gap(
        previous.last,
        current.first,
        tolerance,
    ):
        classification = "expected_session_closure"
    elif gap_ms > tolerance.suspicious_gap_ms:
        classification = "suspicious_gap"

    return _ContinuityComparison(
        key=key,
        previous=previous,
        current=current,
        gap_ms=gap_ms,
        classification=classification,
    )


def _timestamp_continuity_payload(
    scan: _ContinuityScan,
    *,
    tolerance: HistDataGapTolerance,
) -> dict[str, JSONValue]:
    samples = [
        *scan.missing_periods,
        *scan.duplicate_overlaps,
        *scan.reversed_order,
        *scan.suspicious_gaps,
        *scan.expected_session_closures,
    ][:MAX_TIMESTAMP_SAMPLES]
    return {
        "target_count": scan.target_count,
        "candidate_target_count": scan.candidate_target_count,
        "comparable_target_count": scan.comparable_target_count,
        "skipped_target_count": scan.skipped_target_count,
        "group_count": scan.group_count,
        "adjacent_pair_count": scan.adjacent_pair_count,
        "clean_boundary_count": scan.clean_boundary_count,
        "missing_period_count": scan.missing_period_count,
        "period_gap_count": scan.period_gap_count,
        "duplicate_overlap_count": scan.duplicate_overlap_count,
        "reversed_order_count": scan.reversed_order_count,
        "suspicious_gap_count": scan.suspicious_gap_count,
        "expected_session_closure_count": scan.expected_session_closure_count,
        "tolerance": tolerance.to_metadata(),
        "samples": _continuity_samples(samples),
    }


def _has_continuity_surface(scan: _ContinuityScan) -> bool:
    return bool(scan.adjacent_pair_count or scan.period_gap_count)


def _timestamp_continuity_findings(
    target: QualityTarget,
    *,
    scan: _ContinuityScan,
    tolerance: HistDataGapTolerance,
    severity: QualitySeverity,
    rule_id: str,
) -> tuple[QualityFinding, ...]:
    findings: list[QualityFinding] = [
        _finding(
            target,
            code="ASCII_TIMESTAMP_CONTINUITY_SUMMARY",
            message="ASCII monthly file boundary timestamp continuity profile.",
            severity=QualitySeverity.INFO,
            rule_id=rule_id,
            metadata={
                **_base_metadata(target),
                **_timestamp_continuity_payload(scan, tolerance=tolerance),
            },
        )
    ]

    if scan.missing_periods:
        findings.append(
            _continuity_sample_finding(
                target,
                code="ASCII_TIMESTAMP_CONTINUITY_PERIOD_MISSING",
                message="Observed monthly files skip one or more intermediate "
                "periods for the same symbol, format, and timeframe.",
                severity=severity,
                rule_id=rule_id,
                samples=scan.missing_periods,
                row_count=scan.period_gap_count,
            )
        )
    if scan.duplicate_overlaps:
        findings.append(
            _continuity_sample_finding(
                target,
                code="ASCII_TIMESTAMP_CONTINUITY_DUPLICATE_OVERLAP",
                message="Adjacent monthly files share the same boundary "
                "timestamp.",
                severity=severity,
                rule_id=rule_id,
                samples=scan.duplicate_overlaps,
                row_count=scan.duplicate_overlap_count,
            )
        )
    if scan.reversed_order:
        findings.append(
            _continuity_sample_finding(
                target,
                code="ASCII_TIMESTAMP_CONTINUITY_REVERSED_ORDER",
                message="The next monthly file starts before the previous "
                "monthly file ends.",
                severity=severity,
                rule_id=rule_id,
                samples=scan.reversed_order,
                row_count=scan.reversed_order_count,
            )
        )
    if scan.suspicious_gaps:
        findings.append(
            _continuity_sample_finding(
                target,
                code="ASCII_TIMESTAMP_CONTINUITY_SUSPICIOUS_GAP",
                message="Adjacent monthly files have a suspicious boundary "
                "gap that is not an expected market closure.",
                severity=severity,
                rule_id=rule_id,
                samples=scan.suspicious_gaps,
                row_count=scan.suspicious_gap_count,
            )
        )
    if scan.expected_session_closures:
        findings.append(
            _continuity_sample_finding(
                target,
                code="ASCII_TIMESTAMP_CONTINUITY_EXPECTED_SESSION_CLOSURE",
                message="Adjacent monthly file boundary gap matches the "
                "configured FX weekend closure tolerance window.",
                severity=QualitySeverity.INFO,
                rule_id=rule_id,
                samples=scan.expected_session_closures,
                row_count=scan.expected_session_closure_count,
            )
        )
    return tuple(findings)


def _sample_finding(
    target: QualityTarget,
    *,
    code: str,
    message: str,
    severity: QualitySeverity,
    samples: list[_TimestampSample],
    row_count: int,
) -> QualityFinding:
    first = samples[0]
    return _finding(
        target,
        code=code,
        message=message,
        severity=severity,
        location=QualityLocation(
            path=target.path,
            row_number=first.row_number,
            timestamp_source=first.timestamp_source,
            timestamp_utc_ms=first.timestamp_utc_ms,
            column="datetime",
            metadata={
                "source_timezone": SOURCE_TIMEZONE,
                "source_utc_offset": SOURCE_UTC_OFFSET,
                "utc_timestamp": first.utc_timestamp,
                "source_period": first.source_period,
                "utc_period": first.utc_period,
                "target_period": target.period,
                "source_member": first.source_member,
            },
        ),
        metadata={
            **_base_metadata(target, source_member=first.source_member),
            "row_count": row_count,
            "samples": _samples(samples),
        },
    )


def _issue_sample_finding(
    target: QualityTarget,
    *,
    code: str,
    message: str,
    severity: QualitySeverity,
    rule_id: str,
    samples: list[_TimestampIssueSample],
    row_count: int,
) -> QualityFinding:
    first = samples[0]
    return _finding(
        target,
        code=code,
        message=message,
        severity=severity,
        rule_id=rule_id,
        location=QualityLocation(
            path=target.path,
            row_number=first.row_number,
            timestamp_source=first.timestamp_source,
            timestamp_utc_ms=first.timestamp_utc_ms,
            column="datetime",
            metadata={
                "source_timezone": SOURCE_TIMEZONE,
                "source_utc_offset": SOURCE_UTC_OFFSET,
                "utc_timestamp": first.utc_timestamp,
                "source_member": first.source_member,
                **first.metadata,
            },
        ),
        metadata={
            **_base_metadata(target, source_member=first.source_member),
            "row_count": row_count,
            "samples": _issue_samples(samples),
        },
    )


def _gap_sample_finding(
    target: QualityTarget,
    *,
    code: str,
    message: str,
    severity: QualitySeverity,
    rule_id: str,
    samples: list[_TimestampGapSample],
    row_count: int,
) -> QualityFinding:
    first = samples[0]
    return _finding(
        target,
        code=code,
        message=message,
        severity=severity,
        rule_id=rule_id,
        location=QualityLocation(
            path=target.path,
            row_number=first.row_number,
            timestamp_source=first.timestamp_source,
            timestamp_utc_ms=first.timestamp_utc_ms,
            column="datetime",
            metadata={
                "source_timezone": SOURCE_TIMEZONE,
                "source_utc_offset": SOURCE_UTC_OFFSET,
                "utc_timestamp": first.utc_timestamp,
                "source_member": first.source_member,
                "gap_ms": first.gap_ms,
                "classification": first.classification,
                "previous_row_number": first.previous.row_number,
                "previous_timestamp_source": first.previous.timestamp_source,
                "previous_timestamp_utc_ms": first.previous.timestamp_utc_ms,
                "previous_utc_timestamp": first.previous.utc_timestamp,
                "dynamic_window_ms": first.dynamic_window_ms,
                "dynamic_score_increment": first.dynamic_score_increment,
            },
        ),
        metadata={
            **_base_metadata(target, source_member=first.source_member),
            "row_count": row_count,
            "samples": _gap_samples(samples),
        },
    )


def _weekend_activity_finding(
    target: QualityTarget,
    *,
    code: str,
    message: str,
    severity: QualitySeverity,
    rule_id: str,
    samples: list[_WeekendActivitySample],
    row_count: int,
) -> QualityFinding:
    first = samples[0]
    return _finding(
        target,
        code=code,
        message=message,
        severity=severity,
        rule_id=rule_id,
        location=QualityLocation(
            path=target.path,
            row_number=first.row.row_number,
            timestamp_source=first.row.timestamp_source,
            timestamp_utc_ms=first.row.timestamp_utc_ms,
            column="datetime",
            metadata={
                "source_timezone": SOURCE_TIMEZONE,
                "source_utc_offset": SOURCE_UTC_OFFSET,
                "utc_timestamp": first.utc_timestamp,
                "source_member": first.row.source_member,
                "session_state": first.session_state,
            },
        ),
        metadata={
            **_base_metadata(target, source_member=first.row.source_member),
            "row_count": row_count,
            "samples": _weekend_activity_samples(samples),
        },
    )


def _continuity_sample_finding(
    target: QualityTarget,
    *,
    code: str,
    message: str,
    severity: QualitySeverity,
    rule_id: str,
    samples: list[_ContinuityComparison],
    row_count: int,
) -> QualityFinding:
    first = samples[0]
    current = first.current.first
    return _finding(
        target,
        code=code,
        message=message,
        severity=severity,
        rule_id=rule_id,
        location=QualityLocation(
            path=first.current_path,
            row_number=current.row_number,
            timestamp_source=current.timestamp_source,
            timestamp_utc_ms=current.timestamp_utc_ms,
            column="datetime",
            metadata={
                "source_timezone": SOURCE_TIMEZONE,
                "source_utc_offset": SOURCE_UTC_OFFSET,
                "utc_timestamp": current.utc_timestamp,
                "previous_path": first.previous_path,
                "current_path": first.current_path,
                "previous_period": first.previous.period,
                "current_period": first.current.period,
                "gap_ms": first.gap_ms,
                "classification": first.classification,
                "missing_periods": list(first.missing_periods),
                "previous_last": first.previous.last.to_dict(),
                "current_first": current.to_dict(),
            },
        ),
        metadata={
            **_base_metadata(target),
            "row_count": row_count,
            "samples": _continuity_samples(samples),
        },
    )


def _base_metadata(
    target: QualityTarget,
    *,
    source_member: str = "",
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


def _parse_row(raw: str, delimiter: str) -> list[str]:
    return next(csv.reader((raw,), delimiter=delimiter), [])


def _empty_gap_bucket_counts(
    tolerance: HistDataGapTolerance,
) -> dict[str, int]:
    return {
        _gap_bucket_label(threshold_ms): 0
        for threshold_ms in tolerance.bucket_thresholds_ms
    }


def _increment_gap_buckets(
    bucket_counts: dict[str, int],
    gap_ms: int,
    tolerance: HistDataGapTolerance,
) -> None:
    for threshold_ms in tolerance.bucket_thresholds_ms:
        if gap_ms > threshold_ms:
            bucket_counts[_gap_bucket_label(threshold_ms)] += 1


def _gap_bucket_label(threshold_ms: int) -> str:
    match threshold_ms:
        case 60_000:
            return "gt_1m"
        case 300_000:
            return "gt_5m"
        case 1_800_000:
            return "gt_30m"
        case 3_600_000:
            return "gt_1h"
        case 86_400_000:
            return "gt_1d"
        case _:
            return f"gt_{threshold_ms}ms"


def _classify_gap(
    previous: _TimestampSample,
    current: _TimestampSample,
    gap_ms: int,
    tolerance: HistDataGapTolerance,
) -> str:
    if _is_expected_session_closure_gap(previous, current, tolerance):
        return "expected_session_closure"
    if gap_ms > tolerance.suspicious_gap_ms:
        return "suspicious"
    return "tracked"


def _dynamic_score_increment(
    gap_ms: int,
    *,
    classification: str,
    dynamic_window_ms: int,
    tolerance: HistDataGapTolerance,
) -> float:
    if classification != "suspicious":
        return 0.0

    base = gap_ms / max(tolerance.suspicious_gap_ms, 1)
    window_multiplier = tolerance.dynamic_window_max_ms / max(
        dynamic_window_ms,
        1,
    )
    return round(base * max(window_multiplier, 1.0), 4)


def _grow_dynamic_window(
    current_window_ms: int,
    tolerance: HistDataGapTolerance,
) -> int:
    grown = int(current_window_ms * tolerance.dynamic_window_growth_factor)
    return min(
        max(grown, tolerance.dynamic_window_initial_ms),
        tolerance.dynamic_window_max_ms,
    )


def _shrink_dynamic_window(
    current_window_ms: int,
    tolerance: HistDataGapTolerance,
) -> int:
    shrunk = int(current_window_ms * tolerance.dynamic_window_shrink_factor)
    return max(shrunk, tolerance.dynamic_window_initial_ms)


def _is_expected_session_closure_gap(
    previous: _TimestampSample,
    current: _TimestampSample,
    tolerance: HistDataGapTolerance,
) -> bool:
    previous_source = _source_datetime_from_utc_ms(previous.timestamp_utc_ms)
    current_source = _source_datetime_from_utc_ms(current.timestamp_utc_ms)
    if current_source <= previous_source:
        return False

    for friday_close in _friday_closes_between(previous_source, current_source):
        sunday_open = friday_close + timedelta(days=2)
        close_window_start = friday_close - timedelta(
            milliseconds=tolerance.session_boundary_grace_ms
        )
        close_window_end = friday_close + timedelta(
            milliseconds=tolerance.session_boundary_grace_ms
        )
        open_window_start = sunday_open - timedelta(
            milliseconds=tolerance.session_boundary_grace_ms
        )
        open_window_end = sunday_open + timedelta(
            milliseconds=tolerance.session_boundary_grace_ms
        )
        if (
            close_window_start <= previous_source <= close_window_end
            and open_window_start <= current_source <= open_window_end
        ):
            return True
    return False


def _friday_closes_between(
    start: datetime,
    end: datetime,
) -> tuple[datetime, ...]:
    closes: list[datetime] = []
    cursor = (start - timedelta(days=7)).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    end_day = (end + timedelta(days=7)).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    while cursor <= end_day:
        if cursor.weekday() == FX_FRIDAY_CLOSE_WEEKDAY:
            closes.append(
                cursor.replace(
                    hour=FX_CLOSE_OPEN_MINUTE // 60,
                    minute=FX_CLOSE_OPEN_MINUTE % 60,
                )
            )
        cursor += timedelta(days=1)
    return tuple(closes)


def _is_weekend_closure_row(row: _TimestampSample) -> bool:
    source = _source_datetime_from_utc_ms(row.timestamp_utc_ms)
    source_minute = source.hour * 60 + source.minute
    weekday = source.weekday()
    if weekday == 5:
        return True
    if weekday == FX_FRIDAY_CLOSE_WEEKDAY:
        return source_minute > FX_CLOSE_OPEN_MINUTE
    if weekday == FX_SUNDAY_OPEN_WEEKDAY:
        return source_minute < FX_CLOSE_OPEN_MINUTE
    return False


def _continuity_target(
    targets: tuple[QualityTarget, ...],
    *,
    metadata: Mapping[str, JSONValue] | None,
    payload: Mapping[str, JSONValue],
) -> QualityTarget:
    root = _continuity_root(metadata)
    if not root and targets:
        root = str(Path(targets[0].path).parent)
    return QualityTarget(
        path=root or "timestamp-continuity",
        kind=QualityTargetKind.DIRECTORY,
        metadata={
            "manifest": "timestamp-continuity",
            "rule_id": ASCII_TIMESTAMP_CONTINUITY_RULE_ID,
            "target_count": _metadata_int(payload.get("target_count")),
            "candidate_target_count": _metadata_int(
                payload.get("candidate_target_count")
            ),
            "comparable_target_count": _metadata_int(
                payload.get("comparable_target_count")
            ),
            "adjacent_pair_count": _metadata_int(
                payload.get("adjacent_pair_count")
            ),
            "missing_period_count": _metadata_int(
                payload.get("missing_period_count")
            ),
            "suspicious_gap_count": _metadata_int(
                payload.get("suspicious_gap_count")
            ),
        },
    )


def _continuity_root(metadata: Mapping[str, JSONValue] | None) -> str:
    metadata_map = dict(metadata or {})
    roots = metadata_map.get("roots")
    if not isinstance(roots, list):
        coverage_manifest = metadata_map.get("coverage_manifest")
        if isinstance(coverage_manifest, Mapping):
            roots = coverage_manifest.get("roots")
    if isinstance(roots, list) and roots:
        return str(roots[0])
    return ""


def _valid_period(value: str) -> bool:
    if len(value) != 6 or not value.isdigit():
        return False
    month = int(value[4:])
    return 1 <= month <= 12


def _missing_periods_between(
    previous_period: str,
    current_period: str,
) -> tuple[str, ...]:
    previous_index = _period_index(previous_period)
    current_index = _period_index(current_period)
    if current_index - previous_index <= 1:
        return ()
    return tuple(
        _period_from_index(index)
        for index in range(previous_index + 1, current_index)
    )


def _period_index(period: str) -> int:
    year = int(period[:4])
    month = int(period[4:])
    return year * 12 + month


def _period_from_index(index: int) -> str:
    year, month = divmod(index - 1, 12)
    return f"{year:04d}{month + 1:02d}"


def _normalize_data_format(value: str) -> str:
    return str(value or "").strip().lower()


def _normalize_timeframe(value: str) -> str:
    return str(value or "").strip().upper()


def _normalize_symbol(value: str) -> str:
    return str(value or "").strip().upper().replace("_", "")


def _metadata_int(value: JSONValue | None) -> int:
    if isinstance(value, bool | list | dict) or value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _record_raw_timestamp_shape(
    scan: _TimestampScan,
    *,
    row_number: int,
    timestamp_source: str,
    timestamp_utc_ms: int | None,
    source_member: str,
    timeframe: str,
) -> None:
    if timeframe == "M1":
        sample = _m1_granularity_drift_sample(
            row_number=row_number,
            timestamp_source=timestamp_source,
            timestamp_utc_ms=timestamp_utc_ms,
            source_member=source_member,
        )
        if sample is not None:
            scan.m1_granularity_drift_count += 1
            _append_issue_sample(scan.m1_granularity_drifts, sample)
    if timeframe == "T":
        sample = _tick_precision_mismatch_sample(
            row_number=row_number,
            timestamp_source=timestamp_source,
            timestamp_utc_ms=timestamp_utc_ms,
            source_member=source_member,
        )
        if sample is not None:
            scan.tick_precision_mismatch_count += 1
            _append_issue_sample(scan.tick_precision_mismatches, sample)


def _m1_granularity_drift_sample(
    *,
    row_number: int,
    timestamp_source: str,
    timestamp_utc_ms: int | None,
    source_member: str,
) -> _TimestampIssueSample | None:
    parts = _source_timestamp_parts(timestamp_source)
    if parts is None:
        return None

    seconds = parts["second"]
    fraction = parts["fraction"]
    if seconds == "00" and not fraction:
        return None

    actual_modulus_ms = (
        None
        if timestamp_utc_ms is None
        else timestamp_utc_ms % M1_GRANULARITY_MS
    )
    return _TimestampIssueSample(
        row_number=row_number,
        timestamp_source=timestamp_source,
        timestamp_utc_ms=timestamp_utc_ms,
        source_member=source_member,
        metadata={
            "expected_granularity_ms": M1_GRANULARITY_MS,
            "actual_modulus_ms": actual_modulus_ms,
            "source_second": int(seconds),
            "source_subsecond_digits": fraction,
        },
    )


def _tick_precision_mismatch_sample(
    *,
    row_number: int,
    timestamp_source: str,
    timestamp_utc_ms: int | None,
    source_member: str,
) -> _TimestampIssueSample | None:
    parts = _source_timestamp_parts(timestamp_source)
    if parts is None:
        return None

    fraction = parts["fraction"]
    if len(fraction) == EXPECTED_TICK_MILLISECOND_DIGITS and fraction.isdigit():
        return None

    return _TimestampIssueSample(
        row_number=row_number,
        timestamp_source=timestamp_source,
        timestamp_utc_ms=timestamp_utc_ms,
        source_member=source_member,
        metadata={
            "expected_fractional_digits": EXPECTED_TICK_MILLISECOND_DIGITS,
            "observed_fractional_digits": len(fraction),
            "source_fraction": fraction,
            "precision_policy": "millisecond",
        },
    )


def _source_timestamp_parts(timestamp_source: str) -> dict[str, str] | None:
    raw = timestamp_source.strip()
    if len(raw) < 15 or raw[8:9] != " ":
        return None

    date = raw[:8]
    clock = raw[9:15]
    fraction = raw[15:]
    if not date.isdigit() or not clock.isdigit():
        return None
    if fraction and not fraction.isdigit():
        return None

    return {
        "date": date,
        "hour": clock[:2],
        "minute": clock[2:4],
        "second": clock[4:6],
        "fraction": fraction,
    }


def _source_period(timestamp_source: str) -> str:
    return timestamp_source.strip()[:6]


def _utc_period(timestamp_utc_ms: int) -> str:
    timestamp = _utc_datetime_from_ms(timestamp_utc_ms)
    return f"{timestamp.year:04d}{timestamp.month:02d}"


def _utc_iso_from_ms(timestamp_utc_ms: int) -> str:
    timestamp = _utc_datetime_from_ms(timestamp_utc_ms)
    if timestamp.microsecond:
        return timestamp.isoformat(timespec="milliseconds").replace(
            "+00:00", "Z"
        )
    return timestamp.isoformat(timespec="seconds").replace("+00:00", "Z")


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


def _append_sample(
    samples: list[_TimestampSample],
    sample: _TimestampSample,
) -> None:
    if len(samples) < MAX_TIMESTAMP_SAMPLES:
        samples.append(sample)


def _append_issue_sample(
    samples: list[_TimestampIssueSample],
    sample: _TimestampIssueSample,
) -> None:
    if len(samples) < MAX_TIMESTAMP_SAMPLES:
        samples.append(sample)


def _append_gap_sample(
    samples: list[_TimestampGapSample],
    sample: _TimestampGapSample,
) -> None:
    if len(samples) < MAX_TIMESTAMP_SAMPLES:
        samples.append(sample)


def _append_weekend_activity_sample(
    samples: list[_WeekendActivitySample],
    sample: _WeekendActivitySample,
) -> None:
    if len(samples) < MAX_TIMESTAMP_SAMPLES:
        samples.append(sample)


def _append_continuity_sample(
    samples: list[_ContinuityComparison],
    sample: _ContinuityComparison,
) -> None:
    if len(samples) < MAX_TIMESTAMP_SAMPLES:
        samples.append(sample)


def _samples(samples: list[_TimestampSample]) -> list[JSONValue]:
    return [sample.to_dict() for sample in samples]


def _issue_samples(samples: list[_TimestampIssueSample]) -> list[JSONValue]:
    return [sample.to_dict() for sample in samples]


def _gap_samples(samples: list[_TimestampGapSample]) -> list[JSONValue]:
    return [sample.to_dict() for sample in samples]


def _weekend_activity_samples(
    samples: list[_WeekendActivitySample],
) -> list[JSONValue]:
    return [sample.to_dict() for sample in samples]


def _continuity_samples(
    samples: list[_ContinuityComparison],
) -> list[JSONValue]:
    return [sample.to_dict() for sample in samples]


def _finding(
    target: QualityTarget,
    *,
    code: str,
    message: str,
    severity: QualitySeverity = QualitySeverity.ERROR,
    rule_id: str = ASCII_EST_NO_DST_TIME_RULE_ID,
    location: QualityLocation | None = None,
    metadata: dict[str, JSONValue] | None = None,
) -> QualityFinding:
    return QualityFinding(
        severity=severity,
        code=code,
        message=message,
        rule_id=rule_id,
        target=target,
        location=location or QualityLocation(path=target.path),
        metadata=dict(metadata or {}),
    )
