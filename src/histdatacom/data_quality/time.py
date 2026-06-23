"""Timestamp and calendar quality checks for HistData ASCII artifacts."""

from __future__ import annotations

import csv
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
    columns_for_timeframe,
    delimiter_for_timeframe,
    parse_histdata_datetime_to_utc_ms,
)
from histdatacom.runtime_contracts import JSONValue

ASCII_EST_NO_DST_TIME_RULE_ID = "time.ascii.est_no_dst"
ASCII_TIMESTAMP_SEQUENCE_RULE_ID = "time.ascii.sequence"
SOURCE_TIMEZONE = "EST-no-DST"
SOURCE_UTC_OFFSET = "-05:00"
CANONICAL_TIMEZONE = "UTC"
M1_GRANULARITY_MS = 60_000
EXPECTED_TICK_MILLISECOND_DIGITS = 3
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


def time_quality_rules() -> tuple[QualityRule, ...]:
    """Return timestamp quality rules in deterministic execution order."""
    est_no_dst_rule: QualityRule = HistDataAsciiEstNoDstTimeRule()
    sequence_rule: QualityRule = HistDataAsciiTimestampSequenceRule()
    return (est_no_dst_rule, sequence_rule)


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


def _samples(samples: list[_TimestampSample]) -> list[JSONValue]:
    return [sample.to_dict() for sample in samples]


def _issue_samples(samples: list[_TimestampIssueSample]) -> list[JSONValue]:
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
