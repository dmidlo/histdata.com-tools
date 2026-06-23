"""M1 bar integrity data-quality checks for HistData ASCII artifacts."""

from __future__ import annotations

import csv
import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
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
    normalize_histdata_symbol,
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
ASCII_M1_OUTLIER_RULE_ID = "bars.ascii.m1_outliers"
SOURCE_TIMEZONE = "EST-no-DST"
SOURCE_UTC_OFFSET = "-05:00"
CANONICAL_TIMEZONE = "UTC"
MAX_BAR_SAMPLES = 5
M1_PRICE_COLUMNS = ("open", "high", "low", "close")
MAD_SCALE_FACTOR = 1.4826


@dataclass(frozen=True, slots=True)
class HistDataM1OutlierThresholds:
    """Configurable warning thresholds for M1 market-anomaly profiling."""

    max_range_ratio: float = 0.005
    max_open_jump_ratio: float = 0.005
    flatline_run_length: int = 5
    return_mad_multiplier: float = 12.0
    return_absolute_ratio: float = 0.005
    min_return_samples: int = 8

    def __post_init__(self) -> None:
        """Validate threshold values at construction time."""
        if self.max_range_ratio <= 0.0:
            msg = "max_range_ratio must be positive"
            raise ValueError(msg)
        if self.max_open_jump_ratio <= 0.0:
            msg = "max_open_jump_ratio must be positive"
            raise ValueError(msg)
        if self.flatline_run_length < 2:
            msg = "flatline_run_length must be at least 2"
            raise ValueError(msg)
        if self.return_mad_multiplier <= 0.0:
            msg = "return_mad_multiplier must be positive"
            raise ValueError(msg)
        if self.return_absolute_ratio <= 0.0:
            msg = "return_absolute_ratio must be positive"
            raise ValueError(msg)
        if self.min_return_samples < 2:
            msg = "min_return_samples must be at least 2"
            raise ValueError(msg)

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return JSON-compatible threshold metadata."""
        return {
            "max_range_ratio": self.max_range_ratio,
            "max_open_jump_ratio": self.max_open_jump_ratio,
            "flatline_run_length": self.flatline_run_length,
            "return_mad_multiplier": self.return_mad_multiplier,
            "return_absolute_ratio": self.return_absolute_ratio,
            "min_return_samples": self.min_return_samples,
        }


DEFAULT_M1_OUTLIER_THRESHOLDS = HistDataM1OutlierThresholds()


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


@dataclass(frozen=True, slots=True)
class _M1ParsedBar:
    row_number: int
    timestamp_source: str
    timestamp_utc_ms: int
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    source_member: str = ""

    @property
    def utc_timestamp(self) -> str:
        """Return canonical UTC timestamp text for the sampled row."""
        return _utc_iso_from_ms(self.timestamp_utc_ms)

    @property
    def midpoint(self) -> float:
        """Return high/low midpoint used for range-ratio checks."""
        return (self.high_price + self.low_price) / 2.0

    @property
    def range_ratio(self) -> float:
        """Return high-low range as a fraction of midpoint price."""
        denominator = max(abs(self.midpoint), 1e-12)
        return (self.high_price - self.low_price) / denominator

    def values_metadata(self) -> dict[str, JSONValue]:
        """Return JSON-compatible OHLC values."""
        return {
            "open": self.open_price,
            "high": self.high_price,
            "low": self.low_price,
            "close": self.close_price,
        }


@dataclass(frozen=True, slots=True)
class _M1OutlierThresholdSelection:
    thresholds: HistDataM1OutlierThresholds
    source: str
    key: str

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return JSON-compatible threshold-selection metadata."""
        return {
            "source": self.source,
            "key": self.key,
            "thresholds": self.thresholds.to_metadata(),
        }


@dataclass(frozen=True, slots=True)
class _M1OutlierSample:
    row_number: int
    timestamp_source: str
    timestamp_utc_ms: int
    metric: str
    metric_value: float | int
    threshold_value: float | int
    column: str
    values: dict[str, JSONValue]
    source_member: str = ""
    previous_row_number: int | None = None
    previous_timestamp_source: str = ""
    previous_timestamp_utc_ms: int | None = None
    previous_close_price: float | None = None
    metadata: dict[str, JSONValue] = field(default_factory=dict)

    @property
    def utc_timestamp(self) -> str:
        """Return canonical UTC timestamp text for the sampled row."""
        return _utc_iso_from_ms(self.timestamp_utc_ms)

    @property
    def previous_utc_timestamp(self) -> str:
        """Return canonical UTC timestamp text for prior-row context."""
        if self.previous_timestamp_utc_ms is None:
            return ""
        return _utc_iso_from_ms(self.previous_timestamp_utc_ms)

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a bounded JSON-compatible outlier sample."""
        return {
            "row_number": self.row_number,
            "timestamp_source": self.timestamp_source,
            "timestamp_utc_ms": self.timestamp_utc_ms,
            "utc_timestamp": self.utc_timestamp,
            "metric": self.metric,
            "metric_value": self.metric_value,
            "threshold_value": self.threshold_value,
            "column": self.column,
            "values": dict(self.values),
            "source_member": self.source_member,
            "previous_row_number": self.previous_row_number,
            "previous_timestamp_source": self.previous_timestamp_source,
            "previous_timestamp_utc_ms": self.previous_timestamp_utc_ms,
            "previous_utc_timestamp": self.previous_utc_timestamp,
            "previous_close_price": self.previous_close_price,
            "metadata": dict(self.metadata),
        }


@dataclass(slots=True)
class _M1OutlierScan:
    parsed_row_count: int = 0
    range_outlier_count: int = 0
    open_jump_count: int = 0
    flatline_run_count: int = 0
    flatline_affected_row_count: int = 0
    return_sample_count: int = 0
    return_outlier_count: int = 0
    max_range_ratio: float = 0.0
    max_open_jump_ratio: float = 0.0
    max_abs_log_return: float = 0.0
    return_median: float | None = None
    return_mad: float | None = None
    return_effective_threshold: float | None = None
    range_outliers: list[_M1OutlierSample] = field(default_factory=list)
    open_jumps: list[_M1OutlierSample] = field(default_factory=list)
    flatline_runs: list[_M1OutlierSample] = field(default_factory=list)
    return_outliers: list[_M1OutlierSample] = field(default_factory=list)


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


@dataclass(slots=True)
class HistDataAsciiM1OutlierRule:
    """Profile warning-first M1 range, jump, flatline, and return outliers."""

    thresholds: HistDataM1OutlierThresholds = DEFAULT_M1_OUTLIER_THRESHOLDS
    thresholds_by_symbol: Mapping[str, HistDataM1OutlierThresholds] = field(
        default_factory=dict
    )
    thresholds_by_asset_class: Mapping[
        str,
        HistDataM1OutlierThresholds,
    ] = field(default_factory=dict)
    warning_severity: QualitySeverity = QualitySeverity.WARNING
    rule_id: str = ASCII_M1_OUTLIER_RULE_ID
    description: str = (
        "Profile HistData M1 bid OHLC market anomalies including high-low "
        "range outliers, previous-close/current-open jumps, flatline runs, "
        "and robust close-return outliers."
    )

    def evaluate(self, target: QualityTarget) -> tuple[QualityFinding, ...]:
        """Return M1 outlier findings for one target."""
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
                    code="ASCII_M1_OUTLIER_METADATA_UNSUPPORTED",
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
                    code="ASCII_M1_OUTLIER_TEXT_ENCODING_INVALID",
                    message="ASCII file does not decode as strict UTF-8 for "
                    "M1 outlier checks.",
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
                    code=exc.code.replace("BARS", "OUTLIER"),
                    message=exc.message.replace("bar checks", "outlier checks"),
                    rule_id=self.rule_id,
                    metadata=exc.metadata,
                ),
            )

        symbol_metadata = symbol_metadata_for(target.symbol)
        threshold_selection = _outlier_threshold_selection(
            symbol_metadata,
            thresholds=self.thresholds,
            thresholds_by_symbol=self.thresholds_by_symbol,
            thresholds_by_asset_class=self.thresholds_by_asset_class,
        )
        scan = _scan_m1_outlier_rows(
            text,
            target=target,
            delimiter=delimiter,
            source_member=payload.source_member,
            thresholds=threshold_selection.thresholds,
        )
        return _outlier_findings(
            target=target,
            scan=scan,
            source_member=payload.source_member,
            symbol_metadata=symbol_metadata,
            threshold_selection=threshold_selection,
            severity=self.warning_severity,
            rule_id=self.rule_id,
        )


def bars_quality_rules() -> tuple[QualityRule, ...]:
    """Return M1 bar quality rules in deterministic execution order."""
    m1_rule: QualityRule = HistDataAsciiM1BarIntegrityRule()
    precision_rule: QualityRule = HistDataAsciiM1PrecisionRule()
    outlier_rule: QualityRule = HistDataAsciiM1OutlierRule()
    return (m1_rule, precision_rule, outlier_rule)


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


def _scan_m1_outlier_rows(
    text: str,
    *,
    target: QualityTarget,
    delimiter: str,
    source_member: str,
    thresholds: HistDataM1OutlierThresholds,
) -> _M1OutlierScan:
    scan = _M1OutlierScan()
    bars: list[_M1ParsedBar] = []
    columns = columns_for_timeframe(M1)
    expected_count = len(columns)
    previous_bar: _M1ParsedBar | None = None
    flatline_start: _M1ParsedBar | None = None
    flatline_end: _M1ParsedBar | None = None
    flatline_length = 0

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
        bar = _parsed_bar_from_sample(sample)
        if bar is None:
            continue

        bars.append(bar)
        scan.parsed_row_count += 1

        range_ratio = bar.range_ratio
        scan.max_range_ratio = max(scan.max_range_ratio, range_ratio)
        if range_ratio > thresholds.max_range_ratio:
            scan.range_outlier_count += 1
            _append_outlier_sample(
                scan.range_outliers,
                _range_outlier_sample(bar, thresholds.max_range_ratio),
            )

        if previous_bar is not None:
            open_jump_ratio = _ratio(
                abs(bar.open_price - previous_bar.close_price),
                previous_bar.close_price,
            )
            scan.max_open_jump_ratio = max(
                scan.max_open_jump_ratio,
                open_jump_ratio,
            )
            if open_jump_ratio > thresholds.max_open_jump_ratio:
                scan.open_jump_count += 1
                _append_outlier_sample(
                    scan.open_jumps,
                    _open_jump_sample(
                        bar,
                        previous_bar,
                        open_jump_ratio,
                        thresholds.max_open_jump_ratio,
                    ),
                )

        if _is_flatline_bar(bar):
            if flatline_start is None:
                flatline_start = bar
            flatline_end = bar
            flatline_length += 1
        else:
            _finalize_flatline_run(
                scan,
                flatline_start,
                flatline_end,
                flatline_length,
                thresholds,
            )
            flatline_start = None
            flatline_end = None
            flatline_length = 0

        previous_bar = bar

    _finalize_flatline_run(
        scan,
        flatline_start,
        flatline_end,
        flatline_length,
        thresholds,
    )
    _scan_return_outliers(scan, bars, thresholds)
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


def _parsed_bar_from_sample(
    sample: _M1BarSample | None,
) -> _M1ParsedBar | None:
    if sample is None:
        return None
    if sample.violations or sample.non_positive_columns:
        return None
    return _M1ParsedBar(
        row_number=sample.row_number,
        timestamp_source=sample.timestamp_source,
        timestamp_utc_ms=sample.timestamp_utc_ms,
        open_price=sample.open_price,
        high_price=sample.high_price,
        low_price=sample.low_price,
        close_price=sample.close_price,
        source_member=sample.source_member,
    )


def _range_outlier_sample(
    bar: _M1ParsedBar,
    threshold: float,
) -> _M1OutlierSample:
    return _M1OutlierSample(
        row_number=bar.row_number,
        timestamp_source=bar.timestamp_source,
        timestamp_utc_ms=bar.timestamp_utc_ms,
        metric="high_low_range_ratio",
        metric_value=bar.range_ratio,
        threshold_value=threshold,
        column="high",
        values=bar.values_metadata(),
        source_member=bar.source_member,
        metadata={
            "high_low_range": bar.high_price - bar.low_price,
            "range_ratio_denominator": "high_low_midpoint",
            "midpoint": bar.midpoint,
        },
    )


def _open_jump_sample(
    bar: _M1ParsedBar,
    previous_bar: _M1ParsedBar,
    open_jump_ratio: float,
    threshold: float,
) -> _M1OutlierSample:
    return _M1OutlierSample(
        row_number=bar.row_number,
        timestamp_source=bar.timestamp_source,
        timestamp_utc_ms=bar.timestamp_utc_ms,
        metric="previous_close_to_open_ratio",
        metric_value=open_jump_ratio,
        threshold_value=threshold,
        column="open",
        values=bar.values_metadata(),
        source_member=bar.source_member,
        previous_row_number=previous_bar.row_number,
        previous_timestamp_source=previous_bar.timestamp_source,
        previous_timestamp_utc_ms=previous_bar.timestamp_utc_ms,
        previous_close_price=previous_bar.close_price,
        metadata={
            "open_price": bar.open_price,
            "previous_close_price": previous_bar.close_price,
            "absolute_jump": abs(bar.open_price - previous_bar.close_price),
        },
    )


def _flatline_run_sample(
    start: _M1ParsedBar,
    end: _M1ParsedBar,
    run_length: int,
    threshold: int,
) -> _M1OutlierSample:
    return _M1OutlierSample(
        row_number=start.row_number,
        timestamp_source=start.timestamp_source,
        timestamp_utc_ms=start.timestamp_utc_ms,
        metric="flatline_run_length",
        metric_value=run_length,
        threshold_value=threshold,
        column="close",
        values=start.values_metadata(),
        source_member=start.source_member,
        metadata={
            "run_start_row_number": start.row_number,
            "run_end_row_number": end.row_number,
            "run_start_timestamp_source": start.timestamp_source,
            "run_end_timestamp_source": end.timestamp_source,
            "run_start_timestamp_utc_ms": start.timestamp_utc_ms,
            "run_end_timestamp_utc_ms": end.timestamp_utc_ms,
            "run_start_utc_timestamp": start.utc_timestamp,
            "run_end_utc_timestamp": end.utc_timestamp,
            "run_length": run_length,
            "flatline_price": start.close_price,
        },
    )


def _return_outlier_sample(
    bar: _M1ParsedBar,
    previous_bar: _M1ParsedBar,
    log_return: float,
    median_return: float,
    mad_return: float,
    effective_threshold: float,
) -> _M1OutlierSample:
    deviation = abs(log_return - median_return)
    return _M1OutlierSample(
        row_number=bar.row_number,
        timestamp_source=bar.timestamp_source,
        timestamp_utc_ms=bar.timestamp_utc_ms,
        metric="absolute_log_return_deviation",
        metric_value=deviation,
        threshold_value=effective_threshold,
        column="close",
        values=bar.values_metadata(),
        source_member=bar.source_member,
        previous_row_number=previous_bar.row_number,
        previous_timestamp_source=previous_bar.timestamp_source,
        previous_timestamp_utc_ms=previous_bar.timestamp_utc_ms,
        previous_close_price=previous_bar.close_price,
        metadata={
            "log_return": log_return,
            "median_log_return": median_return,
            "mad_log_return": mad_return,
            "absolute_log_return": abs(log_return),
            "close_price": bar.close_price,
            "previous_close_price": previous_bar.close_price,
        },
    )


def _is_flatline_bar(bar: _M1ParsedBar) -> bool:
    return (
        bar.open_price == bar.high_price
        and bar.high_price == bar.low_price
        and bar.low_price == bar.close_price
    )


def _finalize_flatline_run(
    scan: _M1OutlierScan,
    start: _M1ParsedBar | None,
    end: _M1ParsedBar | None,
    run_length: int,
    thresholds: HistDataM1OutlierThresholds,
) -> None:
    if (
        start is None
        or end is None
        or run_length < thresholds.flatline_run_length
    ):
        return
    scan.flatline_run_count += 1
    scan.flatline_affected_row_count += run_length
    _append_outlier_sample(
        scan.flatline_runs,
        _flatline_run_sample(
            start,
            end,
            run_length,
            thresholds.flatline_run_length,
        ),
    )


def _scan_return_outliers(
    scan: _M1OutlierScan,
    bars: list[_M1ParsedBar],
    thresholds: HistDataM1OutlierThresholds,
) -> None:
    return_rows = tuple(
        (
            previous_bar,
            current_bar,
            math.log(current_bar.close_price / previous_bar.close_price),
        )
        for previous_bar, current_bar in zip(bars, bars[1:], strict=False)
        if previous_bar.close_price > 0.0 and current_bar.close_price > 0.0
    )
    scan.return_sample_count = len(return_rows)
    if not return_rows:
        return

    returns = tuple(log_return for _, _, log_return in return_rows)
    scan.max_abs_log_return = max(abs(log_return) for log_return in returns)
    if len(return_rows) < thresholds.min_return_samples:
        return

    median_return = float(median(returns))
    mad_return = float(
        median(abs(log_return - median_return) for log_return in returns)
    )
    scaled_mad_threshold = (
        thresholds.return_mad_multiplier * MAD_SCALE_FACTOR * mad_return
    )
    effective_threshold = max(
        scaled_mad_threshold,
        thresholds.return_absolute_ratio,
    )
    scan.return_median = median_return
    scan.return_mad = mad_return
    scan.return_effective_threshold = effective_threshold

    for previous_bar, current_bar, log_return in return_rows:
        if abs(log_return - median_return) <= effective_threshold:
            continue
        scan.return_outlier_count += 1
        _append_outlier_sample(
            scan.return_outliers,
            _return_outlier_sample(
                current_bar,
                previous_bar,
                log_return,
                median_return,
                mad_return,
                effective_threshold,
            ),
        )


def _ratio(numerator: float, denominator: float) -> float:
    return numerator / max(abs(denominator), 1e-12)


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


def _outlier_findings(
    *,
    target: QualityTarget,
    scan: _M1OutlierScan,
    source_member: str,
    symbol_metadata: HistDataSymbolMetadata,
    threshold_selection: _M1OutlierThresholdSelection,
    severity: QualitySeverity,
    rule_id: str,
) -> tuple[QualityFinding, ...]:
    findings: list[QualityFinding] = [
        _finding(
            target,
            code="ASCII_M1_OUTLIER_SUMMARY",
            message="M1 market-anomaly outlier profile.",
            severity=QualitySeverity.INFO,
            rule_id=rule_id,
            metadata={
                **_base_metadata(target, source_member=source_member),
                "symbol_metadata": symbol_metadata.to_metadata(),
                "threshold_selection": threshold_selection.to_metadata(),
                "parsed_row_count": scan.parsed_row_count,
                "range_outlier_count": scan.range_outlier_count,
                "open_jump_count": scan.open_jump_count,
                "flatline_run_count": scan.flatline_run_count,
                "flatline_affected_row_count": (
                    scan.flatline_affected_row_count
                ),
                "return_sample_count": scan.return_sample_count,
                "return_outlier_count": scan.return_outlier_count,
                "max_range_ratio": scan.max_range_ratio,
                "max_open_jump_ratio": scan.max_open_jump_ratio,
                "max_abs_log_return": scan.max_abs_log_return,
                "return_median": scan.return_median,
                "return_mad": scan.return_mad,
                "return_effective_threshold": (scan.return_effective_threshold),
            },
        )
    ]
    if scan.range_outliers:
        findings.append(
            _outlier_sample_finding(
                target,
                code="ASCII_M1_RANGE_OUTLIER",
                message="M1 high-low range is unusually large for the "
                "selected symbol or asset-class threshold.",
                severity=severity,
                samples=scan.range_outliers,
                row_count=scan.range_outlier_count,
                symbol_metadata=symbol_metadata,
                threshold_selection=threshold_selection,
                rule_id=rule_id,
            )
        )
    if scan.open_jumps:
        findings.append(
            _outlier_sample_finding(
                target,
                code="ASCII_M1_OPEN_CLOSE_JUMP",
                message="M1 current open jumps unusually far from the "
                "previous close.",
                severity=severity,
                samples=scan.open_jumps,
                row_count=scan.open_jump_count,
                symbol_metadata=symbol_metadata,
                threshold_selection=threshold_selection,
                rule_id=rule_id,
            )
        )
    if scan.flatline_runs:
        findings.append(
            _outlier_sample_finding(
                target,
                code="ASCII_M1_FLATLINE_RUN",
                message="M1 rows contain a long run where open, high, low, "
                "and close are identical.",
                severity=severity,
                samples=scan.flatline_runs,
                row_count=scan.flatline_run_count,
                symbol_metadata=symbol_metadata,
                threshold_selection=threshold_selection,
                rule_id=rule_id,
                metadata_extra={
                    "affected_row_count": scan.flatline_affected_row_count,
                },
            )
        )
    if scan.return_outliers:
        findings.append(
            _outlier_sample_finding(
                target,
                code="ASCII_M1_RETURN_OUTLIER",
                message="M1 close-to-close log return is an outlier under "
                "the robust MAD-style threshold.",
                severity=severity,
                samples=scan.return_outliers,
                row_count=scan.return_outlier_count,
                symbol_metadata=symbol_metadata,
                threshold_selection=threshold_selection,
                rule_id=rule_id,
                metadata_extra={
                    "return_median": scan.return_median,
                    "return_mad": scan.return_mad,
                    "return_effective_threshold": (
                        scan.return_effective_threshold
                    ),
                },
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


def _outlier_sample_finding(
    target: QualityTarget,
    *,
    code: str,
    message: str,
    severity: QualitySeverity,
    samples: list[_M1OutlierSample],
    row_count: int,
    symbol_metadata: HistDataSymbolMetadata,
    threshold_selection: _M1OutlierThresholdSelection,
    rule_id: str,
    metadata_extra: dict[str, JSONValue] | None = None,
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
                "metric": first.metric,
                "metric_value": first.metric_value,
                "threshold_value": first.threshold_value,
                "values": dict(first.values),
                "symbol_metadata": symbol_metadata.to_metadata(),
                "threshold_selection": threshold_selection.to_metadata(),
                **dict(first.metadata),
            },
        ),
        metadata={
            **_base_metadata(target, source_member=first.source_member),
            "symbol_metadata": symbol_metadata.to_metadata(),
            "threshold_selection": threshold_selection.to_metadata(),
            "row_count": row_count,
            "samples": _outlier_samples(samples),
            **dict(metadata_extra or {}),
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


def _append_outlier_sample(
    samples: list[_M1OutlierSample],
    sample: _M1OutlierSample,
) -> None:
    if len(samples) < MAX_BAR_SAMPLES:
        samples.append(sample)


def _bar_samples(samples: Iterable[_M1BarSample]) -> list[JSONValue]:
    return [sample.to_dict() for sample in samples]


def _precision_samples(
    samples: Iterable[_M1PrecisionSample],
) -> list[JSONValue]:
    return [sample.to_dict() for sample in samples]


def _outlier_samples(
    samples: Iterable[_M1OutlierSample],
) -> list[JSONValue]:
    return [sample.to_dict() for sample in samples]


def _regime_shift_samples(
    samples: Iterable[_M1PrecisionRegimeShift],
) -> list[JSONValue]:
    return [sample.to_dict() for sample in samples]


def _outlier_threshold_selection(
    symbol_metadata: HistDataSymbolMetadata,
    *,
    thresholds: HistDataM1OutlierThresholds,
    thresholds_by_symbol: Mapping[str, HistDataM1OutlierThresholds],
    thresholds_by_asset_class: Mapping[str, HistDataM1OutlierThresholds],
) -> _M1OutlierThresholdSelection:
    normalized_symbol = normalize_histdata_symbol(
        symbol_metadata.normalized_symbol
    )
    normalized_symbol_thresholds = {
        normalize_histdata_symbol(symbol): threshold
        for symbol, threshold in thresholds_by_symbol.items()
    }
    if normalized_symbol in normalized_symbol_thresholds:
        return _M1OutlierThresholdSelection(
            thresholds=normalized_symbol_thresholds[normalized_symbol],
            source="symbol",
            key=normalized_symbol,
        )

    normalized_asset_class = symbol_metadata.asset_class.lower()
    normalized_asset_thresholds = {
        asset_class.lower(): threshold
        for asset_class, threshold in thresholds_by_asset_class.items()
    }
    if normalized_asset_class in normalized_asset_thresholds:
        return _M1OutlierThresholdSelection(
            thresholds=normalized_asset_thresholds[normalized_asset_class],
            source="asset_class",
            key=normalized_asset_class,
        )

    return _M1OutlierThresholdSelection(
        thresholds=thresholds,
        source="default",
        key="default",
    )


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
