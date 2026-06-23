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
from histdatacom.data_quality.symbols import (
    ASSET_CLASS_UNKNOWN,
    HistDataSymbolMetadata,
    HistDataSymbolPrecisionRule,
    symbol_metadata_for,
)
from histdatacom.histdata_ascii import (
    M1,
    columns_for_timeframe,
    delimiter_for_timeframe,
    parse_histdata_datetime_to_utc_ms,
)
from histdatacom.runtime_contracts import JSONValue

ASCII_M1_BAR_INTEGRITY_RULE_ID = "bars.ascii.m1_ohlc"
ASCII_M1_PRECISION_RULE_ID = "bars.ascii.m1_precision"
SOURCE_TIMEZONE = "EST-no-DST"
SOURCE_UTC_OFFSET = "-05:00"
CANONICAL_TIMEZONE = "UTC"
MAX_BAR_SAMPLES = 5
M1_PRICE_COLUMNS = ("open", "high", "low", "close")


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


@dataclass(frozen=True, slots=True)
class _M1PrecisionSample:
    row_number: int
    column: str
    raw_value: str
    decimal_places: int
    timestamp_source: str
    timestamp_utc_ms: int
    expected_rule: HistDataSymbolPrecisionRule
    source_member: str = ""

    @property
    def utc_timestamp(self) -> str:
        """Return canonical UTC timestamp text for the sampled row."""
        return _utc_iso_from_ms(self.timestamp_utc_ms)

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a bounded JSON-compatible precision sample."""
        return {
            "row_number": self.row_number,
            "column": self.column,
            "raw_value": self.raw_value,
            "decimal_places": self.decimal_places,
            "timestamp_source": self.timestamp_source,
            "timestamp_utc_ms": self.timestamp_utc_ms,
            "utc_timestamp": self.utc_timestamp,
            "expected_rule": self.expected_rule.to_metadata(),
            "source_member": self.source_member,
        }


@dataclass(frozen=True, slots=True)
class _M1PrecisionRegimeShift:
    column: str
    observed_decimal_places: tuple[int, ...]
    counts: dict[int, int]
    first_sample: _M1PrecisionSample

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a bounded JSON-compatible regime-shift sample."""
        return {
            "column": self.column,
            "observed_decimal_places": list(self.observed_decimal_places),
            "counts": {
                str(decimal_places): count
                for decimal_places, count in sorted(self.counts.items())
            },
            "first_sample": self.first_sample.to_dict(),
        }


@dataclass(slots=True)
class _M1PrecisionScan:
    parsed_row_count: int = 0
    observed_decimal_counts: dict[int, int] = field(default_factory=dict)
    observed_column_decimal_counts: dict[str, dict[int, int]] = field(
        default_factory=dict
    )
    regime_samples: dict[str, dict[int, _M1PrecisionSample]] = field(
        default_factory=dict
    )
    unexpected_precision_count: int = 0
    unexpected_precision: list[_M1PrecisionSample] = field(default_factory=list)
    regime_shift_count: int = 0
    regime_shifts: list[_M1PrecisionRegimeShift] = field(default_factory=list)


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


@dataclass(slots=True)
class HistDataAsciiM1PrecisionRule:
    """Validate M1 price precision against symbol-aware expectations."""

    warning_severity: QualitySeverity = QualitySeverity.WARNING
    rule_id: str = ASCII_M1_PRECISION_RULE_ID
    description: str = (
        "Validate HistData M1 bid OHLC decimal precision against "
        "instrument-aware pip and tick-size expectations."
    )

    def evaluate(self, target: QualityTarget) -> tuple[QualityFinding, ...]:
        """Return M1 precision findings for one target."""
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
                    code="ASCII_M1_PRECISION_METADATA_UNSUPPORTED",
                    message="Target metadata does not describe a supported "
                    "HistData M1 ASCII timeframe.",
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
                    code="ASCII_M1_PRECISION_TEXT_ENCODING_INVALID",
                    message="ASCII file does not decode as strict UTF-8 for "
                    "M1 precision checks.",
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
                    code=exc.code.replace("BARS", "PRECISION"),
                    message=exc.message.replace(
                        "bar checks", "precision checks"
                    ),
                    rule_id=self.rule_id,
                    metadata=exc.metadata,
                ),
            )

        symbol_metadata = symbol_metadata_for(target.symbol)
        scan = _scan_m1_precision_rows(
            text,
            target=target,
            delimiter=delimiter,
            source_member=payload.source_member,
            symbol_metadata=symbol_metadata,
        )
        return _precision_findings(
            target=target,
            scan=scan,
            source_member=payload.source_member,
            symbol_metadata=symbol_metadata,
            severity=self.warning_severity,
            rule_id=self.rule_id,
        )


def bars_quality_rules() -> tuple[QualityRule, ...]:
    """Return M1 bar quality rules in deterministic execution order."""
    m1_rule: QualityRule = HistDataAsciiM1BarIntegrityRule()
    precision_rule: QualityRule = HistDataAsciiM1PrecisionRule()
    return (m1_rule, precision_rule)


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


def _scan_m1_precision_rows(
    text: str,
    *,
    target: QualityTarget,
    delimiter: str,
    source_member: str,
    symbol_metadata: HistDataSymbolMetadata,
) -> _M1PrecisionScan:
    scan = _M1PrecisionScan()
    columns = columns_for_timeframe(M1)
    expected_count = len(columns)
    expected_rule = symbol_metadata.precision_rule
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
            continue

        parsed_price_count = 0
        for column, raw_value in zip(
            M1_PRICE_COLUMNS,
            row[1:5],
            strict=True,
        ):
            value = raw_value.strip()
            try:
                parsed_price = float(value)
            except ValueError:
                continue
            if not math.isfinite(parsed_price):
                continue

            decimal_places = _decimal_places(value)
            if decimal_places is None:
                continue

            parsed_price_count += 1
            _increment_count(scan.observed_decimal_counts, decimal_places)
            _increment_count(
                scan.observed_column_decimal_counts.setdefault(column, {}),
                decimal_places,
            )

            if expected_rule is not None:
                sample = _M1PrecisionSample(
                    row_number=row_number,
                    column=column,
                    raw_value=value,
                    decimal_places=decimal_places,
                    timestamp_source=timestamp_source,
                    timestamp_utc_ms=timestamp_utc_ms,
                    expected_rule=expected_rule,
                    source_member=source_member,
                )
                scan.regime_samples.setdefault(column, {}).setdefault(
                    decimal_places,
                    sample,
                )
                if decimal_places not in expected_rule.expected_decimal_places:
                    scan.unexpected_precision_count += 1
                    _append_precision_sample(
                        scan.unexpected_precision,
                        sample,
                    )
        if parsed_price_count == len(M1_PRICE_COLUMNS):
            scan.parsed_row_count += 1

    if expected_rule is not None:
        scan.regime_shifts.extend(
            _precision_regime_shifts(
                scan.observed_column_decimal_counts,
                scan.regime_samples,
                expected_rule,
            )
        )
        scan.regime_shift_count = len(scan.regime_shifts)
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
            M1_PRICE_COLUMNS,
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


def _precision_findings(
    *,
    target: QualityTarget,
    scan: _M1PrecisionScan,
    source_member: str,
    symbol_metadata: HistDataSymbolMetadata,
    severity: QualitySeverity,
    rule_id: str,
) -> tuple[QualityFinding, ...]:
    findings: list[QualityFinding] = [
        _finding(
            target,
            code="ASCII_M1_PRECISION_SUMMARY",
            message="M1 price precision and tick-size profile.",
            severity=QualitySeverity.INFO,
            rule_id=rule_id,
            metadata={
                **_base_metadata(target, source_member=source_member),
                "symbol_metadata": symbol_metadata.to_metadata(),
                "parsed_row_count": scan.parsed_row_count,
                "observed_decimal_places": _decimal_count_metadata(
                    scan.observed_decimal_counts
                ),
                "observed_column_decimal_places": (
                    _column_decimal_count_metadata(
                        scan.observed_column_decimal_counts
                    )
                ),
                "unexpected_precision_count": (scan.unexpected_precision_count),
                "regime_shift_count": scan.regime_shift_count,
                "precision_rule_available": (
                    symbol_metadata.precision_rule is not None
                ),
            },
        )
    ]
    if symbol_metadata.asset_class == ASSET_CLASS_UNKNOWN:
        findings.append(
            _precision_metadata_finding(
                target,
                code="ASCII_M1_SYMBOL_METADATA_UNKNOWN",
                message="Symbol metadata is unknown, so M1 precision "
                "expectations cannot be selected.",
                severity=severity,
                source_member=source_member,
                symbol_metadata=symbol_metadata,
                scan=scan,
                rule_id=rule_id,
            )
        )
    elif symbol_metadata.precision_rule is None:
        findings.append(
            _precision_metadata_finding(
                target,
                code="ASCII_M1_PRECISION_RULE_UNAVAILABLE",
                message="Symbol metadata is known, but no M1 precision "
                "threshold is configured for this asset class yet.",
                severity=severity,
                source_member=source_member,
                symbol_metadata=symbol_metadata,
                scan=scan,
                rule_id=rule_id,
            )
        )
    if scan.unexpected_precision:
        findings.append(
            _precision_sample_finding(
                target,
                code="ASCII_M1_PRECISION_UNEXPECTED",
                message="M1 OHLC decimal precision does not match the "
                "symbol-aware pip and tick-size expectation.",
                severity=severity,
                samples=scan.unexpected_precision,
                row_count=scan.unexpected_precision_count,
                symbol_metadata=symbol_metadata,
                rule_id=rule_id,
            )
        )
    if scan.regime_shifts:
        findings.append(
            _precision_regime_finding(
                target,
                code="ASCII_M1_PRECISION_REGIME_SHIFT",
                message="M1 OHLC decimal precision changes across rows for "
                "one or more price columns.",
                severity=severity,
                shifts=scan.regime_shifts,
                row_count=scan.regime_shift_count,
                symbol_metadata=symbol_metadata,
                rule_id=rule_id,
            )
        )
    return tuple(findings)


def _precision_metadata_finding(
    target: QualityTarget,
    *,
    code: str,
    message: str,
    severity: QualitySeverity,
    source_member: str,
    symbol_metadata: HistDataSymbolMetadata,
    scan: _M1PrecisionScan,
    rule_id: str,
) -> QualityFinding:
    return _finding(
        target,
        code=code,
        message=message,
        severity=severity,
        rule_id=rule_id,
        metadata={
            **_base_metadata(target, source_member=source_member),
            "symbol_metadata": symbol_metadata.to_metadata(),
            "parsed_row_count": scan.parsed_row_count,
            "observed_decimal_places": _decimal_count_metadata(
                scan.observed_decimal_counts
            ),
            "observed_column_decimal_places": _column_decimal_count_metadata(
                scan.observed_column_decimal_counts
            ),
        },
    )


def _precision_sample_finding(
    target: QualityTarget,
    *,
    code: str,
    message: str,
    severity: QualitySeverity,
    samples: list[_M1PrecisionSample],
    row_count: int,
    symbol_metadata: HistDataSymbolMetadata,
    rule_id: str,
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
            column=first.column,
            metadata={
                "source_timezone": SOURCE_TIMEZONE,
                "source_utc_offset": SOURCE_UTC_OFFSET,
                "utc_timestamp": first.utc_timestamp,
                "source_member": first.source_member,
                "raw_value": first.raw_value,
                "decimal_places": first.decimal_places,
                "expected_rule": first.expected_rule.to_metadata(),
                "symbol_metadata": symbol_metadata.to_metadata(),
            },
        ),
        metadata={
            **_base_metadata(target, source_member=first.source_member),
            "symbol_metadata": symbol_metadata.to_metadata(),
            "expected_rule": first.expected_rule.to_metadata(),
            "row_count": row_count,
            "samples": _precision_samples(samples),
        },
    )


def _precision_regime_finding(
    target: QualityTarget,
    *,
    code: str,
    message: str,
    severity: QualitySeverity,
    shifts: list[_M1PrecisionRegimeShift],
    row_count: int,
    symbol_metadata: HistDataSymbolMetadata,
    rule_id: str,
) -> QualityFinding:
    first = shifts[0].first_sample
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
            column=first.column,
            metadata={
                "source_timezone": SOURCE_TIMEZONE,
                "source_utc_offset": SOURCE_UTC_OFFSET,
                "utc_timestamp": first.utc_timestamp,
                "source_member": first.source_member,
                "observed_decimal_places": list(
                    shifts[0].observed_decimal_places
                ),
                "expected_rule": first.expected_rule.to_metadata(),
                "symbol_metadata": symbol_metadata.to_metadata(),
            },
        ),
        metadata={
            **_base_metadata(target, source_member=first.source_member),
            "symbol_metadata": symbol_metadata.to_metadata(),
            "row_count": row_count,
            "samples": _regime_shift_samples(shifts),
        },
    )


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
        "price_columns": list(M1_PRICE_COLUMNS),
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


def _append_precision_sample(
    samples: list[_M1PrecisionSample],
    sample: _M1PrecisionSample,
) -> None:
    if len(samples) < MAX_BAR_SAMPLES:
        samples.append(sample)


def _bar_samples(samples: Iterable[_M1BarSample]) -> list[JSONValue]:
    return [sample.to_dict() for sample in samples]


def _precision_samples(
    samples: Iterable[_M1PrecisionSample],
) -> list[JSONValue]:
    return [sample.to_dict() for sample in samples]


def _regime_shift_samples(
    samples: Iterable[_M1PrecisionRegimeShift],
) -> list[JSONValue]:
    return [sample.to_dict() for sample in samples]


def _precision_regime_shifts(
    observed_column_decimal_counts: dict[str, dict[int, int]],
    regime_samples: dict[str, dict[int, _M1PrecisionSample]],
    expected_rule: HistDataSymbolPrecisionRule,
) -> list[_M1PrecisionRegimeShift]:
    shifts: list[_M1PrecisionRegimeShift] = []
    for column, counts in observed_column_decimal_counts.items():
        observed = tuple(sorted(counts))
        if len(observed) <= 1:
            continue
        representative_decimal_places = next(
            (
                decimal_places
                for decimal_places in observed
                if decimal_places not in expected_rule.expected_decimal_places
            ),
            observed[0],
        )
        sample = regime_samples[column][representative_decimal_places]
        shifts.append(
            _M1PrecisionRegimeShift(
                column=column,
                observed_decimal_places=observed,
                counts=dict(counts),
                first_sample=sample,
            )
        )
        if len(shifts) >= MAX_BAR_SAMPLES:
            break
    return shifts


def _decimal_places(value: str) -> int | None:
    normalized = value.strip()
    if not normalized:
        return None
    if "e" in normalized.lower():
        return None
    whole, dot, fractional = normalized.partition(".")
    if not dot:
        return 0
    if not whole.lstrip("+-").isdigit():
        return None
    if not fractional.isdigit():
        return None
    return len(fractional)


def _increment_count(counts: dict[int, int], key: int) -> None:
    counts[key] = counts.get(key, 0) + 1


def _decimal_count_metadata(counts: dict[int, int]) -> dict[str, JSONValue]:
    return {
        str(decimal_places): count
        for decimal_places, count in sorted(counts.items())
    }


def _column_decimal_count_metadata(
    counts: dict[str, dict[int, int]],
) -> dict[str, JSONValue]:
    return {
        column: _decimal_count_metadata(column_counts)
        for column, column_counts in sorted(counts.items())
    }


def _utc_iso_from_ms(timestamp_utc_ms: int) -> str:
    seconds, milliseconds = divmod(timestamp_utc_ms, 1000)
    return (
        datetime.fromtimestamp(seconds, tz=timezone.utc)
        .replace(microsecond=milliseconds * 1000)
        .isoformat()
        .replace("+00:00", "Z")
    )
