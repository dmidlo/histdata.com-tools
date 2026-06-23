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
SOURCE_TIMEZONE = "EST-no-DST"
SOURCE_UTC_OFFSET = "-05:00"
CANONICAL_TIMEZONE = "UTC"
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
    samples: list[_TimestampSample] = field(default_factory=list)
    period_mismatch_count: int = 0
    period_mismatches: list[_TimestampSample] = field(default_factory=list)
    utc_month_boundary_count: int = 0
    utc_month_boundaries: list[_TimestampSample] = field(default_factory=list)


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


def time_quality_rules() -> tuple[QualityRule, ...]:
    """Return timestamp quality rules in deterministic execution order."""
    est_no_dst_rule: QualityRule = HistDataAsciiEstNoDstTimeRule()
    return (est_no_dst_rule,)


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
        try:
            timestamp_utc_ms = parse_histdata_datetime_to_utc_ms(
                timestamp_source,
                target.timeframe,
            )
        except ValueError:
            scan.invalid_timestamp_count += 1
            continue

        sample = _TimestampSample(
            row_number=row_number,
            timestamp_source=timestamp_source,
            timestamp_utc_ms=timestamp_utc_ms,
            source_period=_source_period(timestamp_source),
            utc_period=_utc_period(timestamp_utc_ms),
            source_member=source_member,
        )
        scan.parsed_row_count += 1
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


def _samples(samples: list[_TimestampSample]) -> list[JSONValue]:
    return [sample.to_dict() for sample in samples]


def _finding(
    target: QualityTarget,
    *,
    code: str,
    message: str,
    severity: QualitySeverity = QualitySeverity.ERROR,
    location: QualityLocation | None = None,
    metadata: dict[str, JSONValue] | None = None,
) -> QualityFinding:
    return QualityFinding(
        severity=severity,
        code=code,
        message=message,
        rule_id=ASCII_EST_NO_DST_TIME_RULE_ID,
        target=target,
        location=location or QualityLocation(path=target.path),
        metadata=dict(metadata or {}),
    )
