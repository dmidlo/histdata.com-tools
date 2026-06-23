"""M1 bar integrity data-quality checks for HistData ASCII artifacts."""

from __future__ import annotations

import csv
import math
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
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
    M1,
    columns_for_timeframe,
    delimiter_for_timeframe,
    parse_histdata_datetime_to_utc_ms,
)
from histdatacom.runtime_contracts import JSONValue

ASCII_M1_BAR_INTEGRITY_RULE_ID = "bars.ascii.m1_ohlc"
SOURCE_TIMEZONE = "EST-no-DST"
SOURCE_UTC_OFFSET = "-05:00"
CANONICAL_TIMEZONE = "UTC"
MAX_BAR_SAMPLES = 5


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
class _M1BarSample:
    row_number: int
    timestamp_source: str
    timestamp_utc_ms: int
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    violations: tuple[str, ...] = ()
    non_positive_columns: tuple[str, ...] = ()
    source_member: str = ""

    @property
    def utc_timestamp(self) -> str:
        """Return canonical UTC timestamp text for the sampled row."""
        return _utc_iso_from_ms(self.timestamp_utc_ms)

    @property
    def primary_column(self) -> str:
        """Return the most actionable OHLC column for location context."""
        if self.non_positive_columns:
            return self.non_positive_columns[0]
        if "high_below_open_or_close" in self.violations:
            return "high"
        if "low_above_open_or_close" in self.violations:
            return "low"
        if "high_below_low" in self.violations:
            return "high"
        return ""

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a bounded JSON-compatible sample."""
        return {
            "row_number": self.row_number,
            "timestamp_source": self.timestamp_source,
            "timestamp_utc_ms": self.timestamp_utc_ms,
            "utc_timestamp": self.utc_timestamp,
            "values": {
                "open": self.open_price,
                "high": self.high_price,
                "low": self.low_price,
                "close": self.close_price,
            },
            "violations": list(self.violations),
            "non_positive_columns": list(self.non_positive_columns),
            "source_member": self.source_member,
        }


@dataclass(slots=True)
class _M1BarScan:
    parsed_row_count: int = 0
    invalid_ohlc_count: int = 0
    non_positive_price_count: int = 0
    invalid_ohlc_rows: list[_M1BarSample] = field(default_factory=list)
    non_positive_price_rows: list[_M1BarSample] = field(default_factory=list)


@dataclass(slots=True)
class HistDataAsciiM1BarIntegrityRule:
    """Validate M1 OHLC ordering and strictly positive bid prices."""

    rule_id: str = ASCII_M1_BAR_INTEGRITY_RULE_ID
    description: str = (
        "Validate HistData M1 bid OHLC bars for positive prices and internal "
        "open/high/low/close consistency."
    )

    def evaluate(self, target: QualityTarget) -> tuple[QualityFinding, ...]:
        """Return M1 bar-integrity findings for one target."""
        if not _is_m1_ascii_text_target(target):
            return ()

        try:
            delimiter = delimiter_for_timeframe(target.timeframe)
            payload = _read_text_payload(target)
            text = payload.data.decode("utf-8")
        except ValueError as exc:
            return (
                _finding(
                    target,
                    code="ASCII_M1_BARS_METADATA_UNSUPPORTED",
                    message="Target metadata does not describe a supported "
                    "HistData M1 ASCII timeframe.",
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
                    code="ASCII_M1_BARS_TEXT_ENCODING_INVALID",
                    message="ASCII file does not decode as strict UTF-8 for "
                    "M1 bar checks.",
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

        scan = _scan_m1_bar_rows(
            text,
            target=target,
            delimiter=delimiter,
            source_member=payload.source_member,
        )
        return _bar_findings(
            target=target,
            scan=scan,
            source_member=payload.source_member,
            rule_id=self.rule_id,
        )


def bars_quality_rules() -> tuple[QualityRule, ...]:
    """Return M1 bar quality rules in deterministic execution order."""
    m1_rule: QualityRule = HistDataAsciiM1BarIntegrityRule()
    return (m1_rule,)


def _is_m1_ascii_text_target(target: QualityTarget) -> bool:
    return (
        target.data_format == "ascii"
        and target.timeframe == M1
        and target.kind in {QualityTargetKind.CSV, QualityTargetKind.ZIP}
    )


def _read_text_payload(target: QualityTarget) -> _TextPayload:
    path = Path(target.path)
    if target.kind is QualityTargetKind.CSV:
        try:
            return _TextPayload(path.read_bytes())
        except OSError as exc:
            raise _source_error(
                "ASCII_M1_BARS_SOURCE_UNREADABLE",
                "ASCII file could not be read for M1 bar checks.",
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
                    code="ASCII_M1_BARS_ZIP_MEMBER_UNAVAILABLE",
                    message="ZIP archive must contain exactly one CSV member "
                    "for M1 bar checks.",
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
            "ASCII_M1_BARS_ZIP_UNREADABLE",
            "ZIP archive could not be opened for M1 bar checks.",
            exc,
        ) from exc
    except (KeyError, OSError) as exc:
        raise _source_error(
            "ASCII_M1_BARS_SOURCE_UNREADABLE",
            "ASCII source could not be read for M1 bar checks.",
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


def _scan_m1_bar_rows(
    text: str,
    *,
    target: QualityTarget,
    delimiter: str,
    source_member: str,
) -> _M1BarScan:
    scan = _M1BarScan()
    columns = columns_for_timeframe(M1)
    expected_count = len(columns)
    for row_number, raw in enumerate(text.splitlines(), start=1):
        if not raw.strip():
            continue
        row = _parse_row(raw, delimiter)
        if len(row) != expected_count or tuple(row) == columns:
            continue

        sample = _m1_bar_sample(
            row,
            row_number=row_number,
            timeframe=target.timeframe,
            source_member=source_member,
        )
        if sample is None:
            continue

        scan.parsed_row_count += 1
        if sample.violations:
            scan.invalid_ohlc_count += 1
            _append_bar_sample(scan.invalid_ohlc_rows, sample)
        if sample.non_positive_columns:
            scan.non_positive_price_count += 1
            _append_bar_sample(scan.non_positive_price_rows, sample)
    return scan


def _m1_bar_sample(
    row: list[str],
    *,
    row_number: int,
    timeframe: str,
    source_member: str,
) -> _M1BarSample | None:
    values = tuple(cell.strip() for cell in row)
    try:
        timestamp_utc_ms = parse_histdata_datetime_to_utc_ms(
            values[0],
            timeframe,
        )
        open_price = float(values[1])
        high_price = float(values[2])
        low_price = float(values[3])
        close_price = float(values[4])
    except (ValueError, IndexError):
        return None

    prices = (open_price, high_price, low_price, close_price)
    if not all(math.isfinite(price) for price in prices):
        return None

    violations: list[str] = []
    if high_price < max(open_price, close_price):
        violations.append("high_below_open_or_close")
    if low_price > min(open_price, close_price):
        violations.append("low_above_open_or_close")
    if high_price < low_price:
        violations.append("high_below_low")

    non_positive_columns = tuple(
        column
        for column, price in zip(
            ("open", "high", "low", "close"),
            prices,
            strict=True,
        )
        if price <= 0.0
    )

    return _M1BarSample(
        row_number=row_number,
        timestamp_source=values[0],
        timestamp_utc_ms=timestamp_utc_ms,
        open_price=open_price,
        high_price=high_price,
        low_price=low_price,
        close_price=close_price,
        violations=tuple(violations),
        non_positive_columns=non_positive_columns,
        source_member=source_member,
    )


def _bar_findings(
    *,
    target: QualityTarget,
    scan: _M1BarScan,
    source_member: str,
    rule_id: str,
) -> tuple[QualityFinding, ...]:
    findings: list[QualityFinding] = [
        _finding(
            target,
            code="ASCII_M1_OHLC_SUMMARY",
            message="M1 OHLC bar integrity profile.",
            severity=QualitySeverity.INFO,
            rule_id=rule_id,
            metadata={
                **_base_metadata(target, source_member=source_member),
                "parsed_row_count": scan.parsed_row_count,
                "invalid_ohlc_count": scan.invalid_ohlc_count,
                "non_positive_price_count": scan.non_positive_price_count,
                "positive_price_required": True,
                "ohlc_checks": [
                    "high >= max(open, close)",
                    "low <= min(open, close)",
                    "high >= low",
                ],
            },
        )
    ]
    if scan.invalid_ohlc_rows:
        findings.append(
            _bar_sample_finding(
                target,
                code="ASCII_M1_OHLC_INVALID",
                message="M1 rows violate OHLC ordering constraints.",
                samples=scan.invalid_ohlc_rows,
                row_count=scan.invalid_ohlc_count,
                rule_id=rule_id,
            )
        )
    if scan.non_positive_price_rows:
        findings.append(
            _bar_sample_finding(
                target,
                code="ASCII_M1_PRICE_NON_POSITIVE",
                message="M1 OHLC prices must be strictly positive.",
                samples=scan.non_positive_price_rows,
                row_count=scan.non_positive_price_count,
                rule_id=rule_id,
            )
        )
    return tuple(findings)


def _bar_sample_finding(
    target: QualityTarget,
    *,
    code: str,
    message: str,
    samples: list[_M1BarSample],
    row_count: int,
    rule_id: str,
) -> QualityFinding:
    first = samples[0]
    return _finding(
        target,
        code=code,
        message=message,
        severity=QualitySeverity.ERROR,
        rule_id=rule_id,
        location=QualityLocation(
            path=target.path,
            row_number=first.row_number,
            timestamp_source=first.timestamp_source,
            timestamp_utc_ms=first.timestamp_utc_ms,
            column=first.primary_column,
            metadata={
                "source_timezone": SOURCE_TIMEZONE,
                "source_utc_offset": SOURCE_UTC_OFFSET,
                "utc_timestamp": first.utc_timestamp,
                "source_member": first.source_member,
                "violations": list(first.violations),
                "non_positive_columns": list(first.non_positive_columns),
                "values": {
                    "open": first.open_price,
                    "high": first.high_price,
                    "low": first.low_price,
                    "close": first.close_price,
                },
            },
        ),
        metadata={
            **_base_metadata(target, source_member=first.source_member),
            "row_count": row_count,
            "samples": _bar_samples(samples),
        },
    )


def _base_metadata(
    target: QualityTarget,
    *,
    source_member: str,
) -> dict[str, JSONValue]:
    return {
        "symbol": target.symbol,
        "timeframe": target.timeframe,
        "period": target.period,
        "data_format": target.data_format,
        "source_member": source_member,
        "source_timezone": SOURCE_TIMEZONE,
        "source_utc_offset": SOURCE_UTC_OFFSET,
        "canonical_timezone": CANONICAL_TIMEZONE,
        "price_columns": ["open", "high", "low", "close"],
        "quote_side": "bid",
    }


def _finding(
    target: QualityTarget,
    *,
    code: str,
    message: str,
    severity: QualitySeverity = QualitySeverity.ERROR,
    rule_id: str = ASCII_M1_BAR_INTEGRITY_RULE_ID,
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


def _parse_row(raw: str, delimiter: str) -> list[str]:
    return next(csv.reader((raw,), delimiter=delimiter), [])


def _append_bar_sample(
    samples: list[_M1BarSample],
    sample: _M1BarSample,
) -> None:
    if len(samples) < MAX_BAR_SAMPLES:
        samples.append(sample)


def _bar_samples(samples: Iterable[_M1BarSample]) -> list[JSONValue]:
    return [sample.to_dict() for sample in samples]


def _utc_iso_from_ms(timestamp_utc_ms: int) -> str:
    seconds, milliseconds = divmod(timestamp_utc_ms, 1000)
    return (
        datetime.fromtimestamp(seconds, tz=timezone.utc)
        .replace(microsecond=milliseconds * 1000)
        .isoformat()
        .replace("+00:00", "Z")
    )
