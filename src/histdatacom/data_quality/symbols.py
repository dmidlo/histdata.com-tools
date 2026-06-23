"""Symbol metadata used by data-quality domain checks."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
import math
from pathlib import Path
from typing import TypeVar
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
from histdatacom.fx_enums import Pairs
from histdatacom.histdata_ascii import M1, TICK, read_ascii_file
from histdatacom.runtime_contracts import JSONValue

DOMAIN_SYMBOL_METADATA_RULE_ID = "domain.symbol_metadata"
DOMAIN_CROSS_INSTRUMENT_RULE_ID = "domain.cross_instrument_consistency"
CROSS_INSTRUMENT_METADATA_KEY = "cross_instrument_consistency"

ASSET_CLASS_FX = "fx"
ASSET_CLASS_METAL = "metal"
ASSET_CLASS_OIL = "oil"
ASSET_CLASS_INDEX = "index"
ASSET_CLASS_UNKNOWN = "unknown"

FX_NON_JPY_PRECISION_RULE_NAME = "fx_non_jpy_six_decimal_bid"
FX_JPY_PRECISION_RULE_NAME = "fx_jpy_three_decimal_bid"

M1_BID_PRICE_COLUMNS = ("open", "high", "low", "close")
TICK_BID_ASK_PRICE_COLUMNS = ("bid", "ask")

CURRENCY_CODES = frozenset(
    {
        "AUD",
        "CAD",
        "CHF",
        "CZK",
        "DKK",
        "EUR",
        "GBP",
        "HKD",
        "HUF",
        "JPY",
        "MXN",
        "NOK",
        "NZD",
        "PLN",
        "SEK",
        "SGD",
        "TRY",
        "USD",
        "ZAR",
    }
)
METAL_BASES = frozenset({"XAG", "XAU"})
OIL_BASES = frozenset({"BCO", "WTI"})
INDEX_SYMBOLS = frozenset(
    {
        "AUXAUD",
        "ETXEUR",
        "FRXEUR",
        "GRXEUR",
        "HKXHKD",
        "JPXJPY",
        "NSXUSD",
        "SPXUSD",
        "UDXUSD",
        "UKXGBP",
    }
)

MAX_CROSS_INSTRUMENT_SAMPLES = 5

_CrossSample = TypeVar("_CrossSample")


@dataclass(frozen=True, slots=True)
class HistDataSymbolPrecisionRule:
    """Instrument-aware price precision expectations."""

    name: str
    expected_decimal_places: tuple[int, ...]
    pip_size: str
    tick_size: str
    quote_side: str = "bid"

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return JSON-compatible rule metadata."""
        return {
            "name": self.name,
            "expected_decimal_places": list(self.expected_decimal_places),
            "pip_size": self.pip_size,
            "tick_size": self.tick_size,
            "quote_side": self.quote_side,
        }


@dataclass(frozen=True, slots=True)
class HistDataSymbolMetadata:
    """Normalized HistData symbol metadata for quality checks."""

    symbol: str
    normalized_symbol: str
    asset_class: str
    base: str = ""
    quote: str = ""
    pair_key: str = ""
    source: str = "unknown"
    precision_rule: HistDataSymbolPrecisionRule | None = None
    aliases: tuple[str, ...] = ()

    @property
    def known(self) -> bool:
        """Return whether the symbol was recognized."""
        return self.asset_class != ASSET_CLASS_UNKNOWN

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return JSON-compatible symbol metadata."""
        return {
            "symbol": self.symbol,
            "normalized_symbol": self.normalized_symbol,
            "asset_class": self.asset_class,
            "base": self.base,
            "quote": self.quote,
            "pair_key": self.pair_key,
            "source": self.source,
            "known": self.known,
            "aliases": list(self.aliases),
            "pip_size": _precision_value(self.precision_rule, "pip_size"),
            "tick_size": _precision_value(self.precision_rule, "tick_size"),
            "quote_side": _precision_value(self.precision_rule, "quote_side"),
            "m1_bid_only": True,
            "precision_rule": (
                None
                if self.precision_rule is None
                else self.precision_rule.to_metadata()
            ),
        }


@dataclass(frozen=True, slots=True)
class HistDataCrossInstrumentTolerance:
    """Thresholds for run-level cross-instrument consistency checks."""

    triangular_warning_relative_tolerance: float = 0.005
    triangular_error_relative_tolerance: float = 0.05
    inverse_warning_relative_tolerance: float = 0.005
    inverse_error_relative_tolerance: float = 0.05
    minimum_common_timestamp_ratio: float = 0.5
    stale_forward_fill_min_run: int = 2

    def __post_init__(self) -> None:
        """Validate public tolerance values."""
        if self.triangular_warning_relative_tolerance < 0.0:
            msg = "triangular warning tolerance must be non-negative"
            raise ValueError(msg)
        if (
            self.triangular_error_relative_tolerance
            < self.triangular_warning_relative_tolerance
        ):
            msg = "triangular error tolerance must be >= warning tolerance"
            raise ValueError(msg)
        if self.inverse_warning_relative_tolerance < 0.0:
            msg = "inverse warning tolerance must be non-negative"
            raise ValueError(msg)
        if (
            self.inverse_error_relative_tolerance
            < self.inverse_warning_relative_tolerance
        ):
            msg = "inverse error tolerance must be >= warning tolerance"
            raise ValueError(msg)
        if not 0.0 <= self.minimum_common_timestamp_ratio <= 1.0:
            msg = "minimum common timestamp ratio must be between 0 and 1"
            raise ValueError(msg)
        if self.stale_forward_fill_min_run < 1:
            msg = "stale forward-fill run length must be positive"
            raise ValueError(msg)

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return JSON-compatible tolerance metadata."""
        return {
            "triangular_warning_relative_tolerance": (
                self.triangular_warning_relative_tolerance
            ),
            "triangular_error_relative_tolerance": (
                self.triangular_error_relative_tolerance
            ),
            "inverse_warning_relative_tolerance": (
                self.inverse_warning_relative_tolerance
            ),
            "inverse_error_relative_tolerance": (
                self.inverse_error_relative_tolerance
            ),
            "minimum_common_timestamp_ratio": (
                self.minimum_common_timestamp_ratio
            ),
            "stale_forward_fill_min_run": self.stale_forward_fill_min_run,
        }


DEFAULT_CROSS_INSTRUMENT_TOLERANCE = HistDataCrossInstrumentTolerance()


@dataclass(frozen=True, slots=True)
class _CrossInstrumentPoint:
    timestamp_utc_ms: int
    price: float
    row_number: int


@dataclass(frozen=True, slots=True)
class _CrossInstrumentSeries:
    target: QualityTarget
    metadata: HistDataSymbolMetadata
    timeframe: str
    period: str
    price_kind: str
    points: dict[int, _CrossInstrumentPoint]

    @property
    def symbol(self) -> str:
        return self.metadata.normalized_symbol

    @property
    def base(self) -> str:
        return self.metadata.base

    @property
    def quote(self) -> str:
        return self.metadata.quote

    @property
    def timestamp_count(self) -> int:
        return len(self.points)

    @property
    def timestamps(self) -> set[int]:
        return set(self.points)

    @property
    def start_utc_ms(self) -> int | None:
        if not self.points:
            return None
        return min(self.points)

    @property
    def end_utc_ms(self) -> int | None:
        if not self.points:
            return None
        return max(self.points)

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return JSON-compatible series profile metadata."""
        return {
            "path": self.target.path,
            "symbol": self.symbol,
            "base": self.base,
            "quote": self.quote,
            "timeframe": self.timeframe,
            "period": self.period,
            "price_kind": self.price_kind,
            "timestamp_count": self.timestamp_count,
            "start_timestamp_utc_ms": self.start_utc_ms,
            "end_timestamp_utc_ms": self.end_utc_ms,
        }


@dataclass(frozen=True, slots=True)
class _CrossInstrumentUnavailable:
    reason: str
    symbols: tuple[str, ...] = ()
    timeframe: str = ""
    period: str = ""
    metadata: dict[str, JSONValue] = field(default_factory=dict)

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return JSON-compatible unavailable metadata."""
        return {
            "reason": self.reason,
            "symbols": list(self.symbols),
            "timeframe": self.timeframe,
            "period": self.period,
            **dict(self.metadata),
        }


@dataclass(frozen=True, slots=True)
class _TriangularComparisonSample:
    direct: _CrossInstrumentSeries
    numerator: _CrossInstrumentSeries
    denominator: _CrossInstrumentSeries
    timestamp_utc_ms: int
    direct_price: float
    implied_price: float
    relative_difference: float
    severity: QualitySeverity

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return JSON-compatible triangular comparison metadata."""
        return {
            "direct_symbol": self.direct.symbol,
            "numerator_symbol": self.numerator.symbol,
            "denominator_symbol": self.denominator.symbol,
            "timeframe": self.direct.timeframe,
            "period": self.direct.period,
            "timestamp_utc_ms": self.timestamp_utc_ms,
            "direct_price": self.direct_price,
            "implied_price": self.implied_price,
            "relative_difference": self.relative_difference,
            "severity": self.severity.value,
            "relationship": (
                f"{self.numerator.symbol} / {self.denominator.symbol} "
                f"~= {self.direct.symbol}"
            ),
            "paths": {
                "direct": self.direct.target.path,
                "numerator": self.numerator.target.path,
                "denominator": self.denominator.target.path,
            },
        }


@dataclass(frozen=True, slots=True)
class _InverseComparisonSample:
    left: _CrossInstrumentSeries
    right: _CrossInstrumentSeries
    timestamp_utc_ms: int
    left_price: float
    right_price: float
    product: float
    relative_difference: float
    severity: QualitySeverity

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return JSON-compatible inverse comparison metadata."""
        return {
            "left_symbol": self.left.symbol,
            "right_symbol": self.right.symbol,
            "timeframe": self.left.timeframe,
            "period": self.left.period,
            "timestamp_utc_ms": self.timestamp_utc_ms,
            "left_price": self.left_price,
            "right_price": self.right_price,
            "product": self.product,
            "relative_difference": self.relative_difference,
            "severity": self.severity.value,
            "relationship": f"{self.left.symbol} * {self.right.symbol} ~= 1",
            "paths": {
                "left": self.left.target.path,
                "right": self.right.target.path,
            },
        }


@dataclass(frozen=True, slots=True)
class _TimestampGridSample:
    timeframe: str
    period: str
    symbols: tuple[str, ...]
    union_timestamp_count: int
    common_timestamp_count: int
    common_timestamp_ratio: float
    missing_by_symbol: dict[str, int]

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return JSON-compatible timestamp-grid metadata."""
        return {
            "timeframe": self.timeframe,
            "period": self.period,
            "symbols": list(self.symbols),
            "union_timestamp_count": self.union_timestamp_count,
            "common_timestamp_count": self.common_timestamp_count,
            "common_timestamp_ratio": self.common_timestamp_ratio,
            "missing_by_symbol": dict(self.missing_by_symbol),
        }


@dataclass(frozen=True, slots=True)
class _StaleJoinSample:
    stale_series: _CrossInstrumentSeries
    active_series: _CrossInstrumentSeries
    stale_value_timestamp_utc_ms: int
    start_timestamp_utc_ms: int
    end_timestamp_utc_ms: int
    affected_timestamp_count: int

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return JSON-compatible stale join metadata."""
        return {
            "stale_symbol": self.stale_series.symbol,
            "active_symbol": self.active_series.symbol,
            "timeframe": self.stale_series.timeframe,
            "period": self.stale_series.period,
            "stale_value_timestamp_utc_ms": (self.stale_value_timestamp_utc_ms),
            "start_timestamp_utc_ms": self.start_timestamp_utc_ms,
            "end_timestamp_utc_ms": self.end_timestamp_utc_ms,
            "affected_timestamp_count": self.affected_timestamp_count,
            "paths": {
                "stale": self.stale_series.target.path,
                "active": self.active_series.target.path,
            },
        }


@dataclass(slots=True)
class _CrossInstrumentScan:
    target_count: int = 0
    ascii_target_count: int = 0
    fx_series: list[_CrossInstrumentSeries] = field(default_factory=list)
    source_error_count: int = 0
    invalid_price_count: int = 0
    triangular_candidate_count: int = 0
    triangular_compared_timestamp_count: int = 0
    triangular_warning_count: int = 0
    triangular_error_count: int = 0
    inverse_candidate_count: int = 0
    inverse_compared_timestamp_count: int = 0
    inverse_warning_count: int = 0
    inverse_error_count: int = 0
    timestamp_grid_group_count: int = 0
    sparse_grid_count: int = 0
    stale_join_risk_count: int = 0
    unavailable: list[_CrossInstrumentUnavailable] = field(default_factory=list)
    triangular_warnings: list[_TriangularComparisonSample] = field(
        default_factory=list
    )
    triangular_errors: list[_TriangularComparisonSample] = field(
        default_factory=list
    )
    inverse_warnings: list[_InverseComparisonSample] = field(
        default_factory=list
    )
    inverse_errors: list[_InverseComparisonSample] = field(default_factory=list)
    sparse_grids: list[_TimestampGridSample] = field(default_factory=list)
    stale_join_risks: list[_StaleJoinSample] = field(default_factory=list)


FX_NON_JPY_PRECISION_RULE = HistDataSymbolPrecisionRule(
    name=FX_NON_JPY_PRECISION_RULE_NAME,
    expected_decimal_places=(6,),
    pip_size="0.0001",
    tick_size="0.000001",
)
FX_JPY_PRECISION_RULE = HistDataSymbolPrecisionRule(
    name=FX_JPY_PRECISION_RULE_NAME,
    expected_decimal_places=(3,),
    pip_size="0.01",
    tick_size="0.001",
)


def normalize_histdata_symbol(symbol: str) -> str:
    """Normalize HistData symbols to compact uppercase text."""
    return "".join(
        character
        for character in str(symbol or "").upper()
        if character.isalnum()
    )


def symbol_metadata_for(symbol: str) -> HistDataSymbolMetadata:
    """Return normalized metadata and precision expectations for a symbol."""
    normalized = normalize_histdata_symbol(symbol)
    if not normalized:
        return _unknown_symbol_metadata(symbol, normalized)

    pair_value = _PAIR_VALUES_BY_SYMBOL.get(normalized)
    base, quote = _split_pair_value(pair_value, normalized)
    pair_key = _PAIR_KEYS_BY_SYMBOL.get(normalized, "")
    source = "fx_enums.Pairs" if pair_value is not None else "inferred"

    if base in CURRENCY_CODES and quote in CURRENCY_CODES:
        return HistDataSymbolMetadata(
            symbol=str(symbol),
            normalized_symbol=normalized,
            asset_class=ASSET_CLASS_FX,
            base=base,
            quote=quote,
            pair_key=pair_key,
            source=source,
            precision_rule=(
                FX_JPY_PRECISION_RULE
                if quote == "JPY"
                else FX_NON_JPY_PRECISION_RULE
            ),
            aliases=_aliases_for_symbol(normalized),
        )

    if base in METAL_BASES:
        return HistDataSymbolMetadata(
            symbol=str(symbol),
            normalized_symbol=normalized,
            asset_class=ASSET_CLASS_METAL,
            base=base,
            quote=quote,
            pair_key=pair_key,
            source=source,
            aliases=_aliases_for_symbol(normalized),
        )

    if base in OIL_BASES:
        return HistDataSymbolMetadata(
            symbol=str(symbol),
            normalized_symbol=normalized,
            asset_class=ASSET_CLASS_OIL,
            base=base,
            quote=quote,
            pair_key=pair_key,
            source=source,
            aliases=_aliases_for_symbol(normalized),
        )

    if normalized in INDEX_SYMBOLS:
        return HistDataSymbolMetadata(
            symbol=str(symbol),
            normalized_symbol=normalized,
            asset_class=ASSET_CLASS_INDEX,
            base=base,
            quote=quote,
            pair_key=pair_key,
            source=source,
            aliases=_aliases_for_symbol(normalized),
        )

    return _unknown_symbol_metadata(symbol, normalized)


@dataclass(slots=True)
class HistDataSymbolMetadataRule:
    """Emit normalized symbol metadata and quote-convention assumptions."""

    warning_severity: QualitySeverity = QualitySeverity.WARNING
    rule_id: str = DOMAIN_SYMBOL_METADATA_RULE_ID
    description: str = (
        "Normalize HistData symbols into asset classes, base/quote metadata, "
        "alias context, and quote-convention assumptions."
    )

    def evaluate(self, target: QualityTarget) -> tuple[QualityFinding, ...]:
        """Return domain metadata findings for one target."""
        symbol = _target_symbol(target)
        if not symbol:
            return ()

        symbol_metadata = symbol_metadata_for(symbol)
        metadata: dict[str, JSONValue] = {
            "symbol_metadata": symbol_metadata.to_metadata(),
            "quote_convention": _quote_convention_metadata(symbol_metadata),
            "format_assumptions": _format_assumptions_metadata(target),
        }
        findings: list[QualityFinding] = [
            _domain_finding(
                target,
                code="DOMAIN_SYMBOL_METADATA_SUMMARY",
                message="Normalized symbol metadata and quote-convention "
                "profile.",
                severity=QualitySeverity.INFO,
                rule_id=self.rule_id,
                metadata=metadata,
            )
        ]
        if not symbol_metadata.known:
            findings.append(
                _domain_finding(
                    target,
                    code="DOMAIN_SYMBOL_METADATA_UNKNOWN",
                    message="Symbol metadata is unknown; downstream domain "
                    "checks should treat asset-class assumptions as "
                    "unavailable.",
                    severity=self.warning_severity,
                    rule_id=self.rule_id,
                    metadata=metadata,
                )
            )
        return tuple(findings)


@dataclass(slots=True)
class HistDataCrossInstrumentConsistencyRule:
    """Validate cross-instrument FX consistency across discovered targets."""

    tolerance: HistDataCrossInstrumentTolerance = (
        DEFAULT_CROSS_INSTRUMENT_TOLERANCE
    )
    warning_severity: QualitySeverity = QualitySeverity.WARNING
    error_severity: QualitySeverity = QualitySeverity.ERROR
    rule_id: str = DOMAIN_CROSS_INSTRUMENT_RULE_ID
    description: str = (
        "Compare related FX instruments across one quality run for "
        "triangular consistency, inverse consistency, common timestamp-grid "
        "coverage, and stale forward-fill join risk."
    )

    def evaluate_run(
        self,
        targets: Iterable[QualityTarget],
        *,
        metadata: Mapping[str, JSONValue] | None = None,
    ) -> QualityReport:
        """Return cross-instrument consistency findings."""
        target_tuple = tuple(targets)
        scan = _scan_cross_instrument_consistency(
            target_tuple,
            tolerance=self.tolerance,
            warning_severity=self.warning_severity,
            error_severity=self.error_severity,
        )
        payload = _cross_instrument_payload(
            scan,
            tolerance=self.tolerance,
        )
        if not _has_cross_instrument_surface(scan):
            return QualityReport(
                metadata={CROSS_INSTRUMENT_METADATA_KEY: payload},
            )

        run_target = _cross_instrument_target(
            target_tuple,
            metadata=metadata,
            payload=payload,
        )
        findings = _cross_instrument_findings(
            run_target,
            scan=scan,
            tolerance=self.tolerance,
            rule_id=self.rule_id,
        )
        return QualityReport(
            rule_results=(
                QualityRuleResult(
                    rule_id=self.rule_id,
                    target=run_target,
                    findings=findings,
                ),
            ),
            metadata={CROSS_INSTRUMENT_METADATA_KEY: payload},
        )


def domain_quality_rules() -> tuple[QualityRule, ...]:
    """Return domain quality rules in deterministic execution order."""
    symbol_rule: QualityRule = HistDataSymbolMetadataRule()
    return (symbol_rule,)


def domain_quality_run_rules() -> tuple[QualityRunRule, ...]:
    """Return run-scoped domain quality rules."""
    cross_instrument_rule: QualityRunRule = (
        HistDataCrossInstrumentConsistencyRule()
    )
    return (cross_instrument_rule,)


def _unknown_symbol_metadata(
    symbol: str,
    normalized: str,
) -> HistDataSymbolMetadata:
    return HistDataSymbolMetadata(
        symbol=str(symbol),
        normalized_symbol=normalized,
        asset_class=ASSET_CLASS_UNKNOWN,
    )


def _quote_convention_metadata(
    metadata: HistDataSymbolMetadata,
) -> dict[str, JSONValue]:
    price_unit = (
        f"{metadata.quote} per {metadata.base}"
        if (metadata.base and metadata.quote)
        else ""
    )
    return {
        "asset_class": metadata.asset_class,
        "base": metadata.base,
        "quote": metadata.quote,
        "pair_direction": (
            "base_quote" if metadata.base and metadata.quote else ""
        ),
        "price_unit": price_unit,
        "fx_base_currency": (
            metadata.base if metadata.asset_class == ASSET_CLASS_FX else ""
        ),
        "fx_quote_currency": (
            metadata.quote if metadata.asset_class == ASSET_CLASS_FX else ""
        ),
        "m1_quote_side": "bid",
        "m1_bid_only": True,
        "tick_quote_sides": list(TICK_BID_ASK_PRICE_COLUMNS),
        "tick_spread_definition": "ask - bid",
    }


def _format_assumptions_metadata(target: QualityTarget) -> dict[str, JSONValue]:
    timeframe = target.timeframe
    return {
        "data_format": target.data_format,
        "timeframe": timeframe,
        "m1_bid_ohlc": timeframe == M1,
        "m1_bid_only": timeframe == M1,
        "m1_price_columns": list(M1_BID_PRICE_COLUMNS),
        "tick_bid_ask": timeframe == TICK,
        "tick_price_columns": list(TICK_BID_ASK_PRICE_COLUMNS),
        "active_quote_side": _active_quote_side(timeframe),
    }


def _active_quote_side(timeframe: str) -> str:
    if timeframe == M1:
        return "bid"
    if timeframe == TICK:
        return "bid/ask"
    return ""


def _target_symbol(target: QualityTarget) -> str:
    return str(target.symbol or target.metadata.get("symbol", "") or "")


def _domain_finding(
    target: QualityTarget,
    *,
    code: str,
    message: str,
    severity: QualitySeverity,
    rule_id: str,
    metadata: dict[str, JSONValue],
) -> QualityFinding:
    return QualityFinding(
        severity=QualitySeverity.from_value(severity),
        code=code,
        message=message,
        rule_id=rule_id,
        target=target,
        location=QualityLocation(
            path=target.path,
            column="symbol",
            metadata={"symbol": _target_symbol(target)},
        ),
        metadata=dict(metadata),
    )


def _scan_cross_instrument_consistency(
    targets: tuple[QualityTarget, ...],
    *,
    tolerance: HistDataCrossInstrumentTolerance,
    warning_severity: QualitySeverity,
    error_severity: QualitySeverity,
) -> _CrossInstrumentScan:
    scan = _CrossInstrumentScan(target_count=len(targets))
    for target in targets:
        if not _is_ascii_text_target(target):
            continue
        scan.ascii_target_count += 1
        series, unavailable, invalid_count = _cross_instrument_series(target)
        scan.invalid_price_count += invalid_count
        if unavailable is not None:
            scan.source_error_count += 1
            _append_unavailable(scan, unavailable)
            continue
        if series is None:
            continue
        if series.metadata.asset_class != ASSET_CLASS_FX:
            continue
        if not (series.base and series.quote and series.points):
            _append_unavailable(
                scan,
                _CrossInstrumentUnavailable(
                    reason="fx_series_incomplete",
                    symbols=(series.symbol,),
                    timeframe=series.timeframe,
                    period=series.period,
                    metadata={
                        "path": series.target.path,
                        "timestamp_count": series.timestamp_count,
                    },
                ),
            )
            continue
        scan.fx_series.append(series)

    if not scan.fx_series:
        _append_unavailable(
            scan,
            _CrossInstrumentUnavailable(
                reason="no_fx_series",
                metadata={
                    "target_count": scan.target_count,
                    "ascii_target_count": scan.ascii_target_count,
                },
            ),
        )
        return scan

    grouped = _series_by_timeframe_period(scan.fx_series)
    if not any(len(group) >= 2 for group in grouped.values()):
        _append_unavailable(
            scan,
            _CrossInstrumentUnavailable(
                reason="no_multi_instrument_group",
                metadata={"fx_series_count": len(scan.fx_series)},
            ),
        )

    for (timeframe, period), group in sorted(grouped.items()):
        if len(group) < 2:
            continue
        _record_timestamp_grid_checks(
            scan,
            group,
            timeframe=timeframe,
            period=period,
            tolerance=tolerance,
        )
        _record_stale_join_checks(
            scan,
            group,
            tolerance=tolerance,
        )
        _record_triangular_checks(
            scan,
            group,
            timeframe=timeframe,
            period=period,
            tolerance=tolerance,
            warning_severity=warning_severity,
            error_severity=error_severity,
        )
        _record_inverse_checks(
            scan,
            group,
            timeframe=timeframe,
            period=period,
            tolerance=tolerance,
            warning_severity=warning_severity,
            error_severity=error_severity,
        )

    if scan.triangular_candidate_count == 0:
        _append_unavailable(
            scan,
            _CrossInstrumentUnavailable(
                reason="no_triangular_symbol_sets",
                metadata={"fx_series_count": len(scan.fx_series)},
            ),
        )
    if scan.inverse_candidate_count == 0:
        _append_unavailable(
            scan,
            _CrossInstrumentUnavailable(
                reason="no_inverse_symbol_sets",
                metadata={"fx_series_count": len(scan.fx_series)},
            ),
        )
    return scan


def _is_ascii_text_target(target: QualityTarget) -> bool:
    return (
        target.data_format == "ascii"
        and target.timeframe in {M1, TICK}
        and target.kind in {QualityTargetKind.CSV, QualityTargetKind.ZIP}
    )


def _cross_instrument_series(
    target: QualityTarget,
) -> tuple[
    _CrossInstrumentSeries | None,
    _CrossInstrumentUnavailable | None,
    int,
]:
    try:
        batch = read_ascii_file(Path(target.path), target.timeframe)
    except (OSError, UnicodeDecodeError, ValueError, zipfile.BadZipFile) as exc:
        return (
            None,
            _CrossInstrumentUnavailable(
                reason="source_unreadable",
                symbols=(normalize_histdata_symbol(_target_symbol(target)),),
                timeframe=target.timeframe,
                period=target.period,
                metadata={
                    "path": target.path,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
            ),
            0,
        )

    points: dict[int, _CrossInstrumentPoint] = {}
    invalid_price_count = 0
    for row_number, row in enumerate(batch.rows, start=1):
        timestamp_utc_ms = int(row[0])
        price = _cross_instrument_price(target.timeframe, row)
        if price is None:
            invalid_price_count += 1
            continue
        if timestamp_utc_ms not in points:
            points[timestamp_utc_ms] = _CrossInstrumentPoint(
                timestamp_utc_ms=timestamp_utc_ms,
                price=price,
                row_number=row_number,
            )

    if not points:
        return (
            None,
            _CrossInstrumentUnavailable(
                reason="no_valid_prices",
                symbols=(normalize_histdata_symbol(_target_symbol(target)),),
                timeframe=target.timeframe,
                period=target.period,
                metadata={
                    "path": target.path,
                    "row_count": len(batch.rows),
                    "invalid_price_count": invalid_price_count,
                },
            ),
            invalid_price_count,
        )

    return (
        _CrossInstrumentSeries(
            target=target,
            metadata=symbol_metadata_for(_target_symbol(target)),
            timeframe=target.timeframe,
            period=target.period,
            price_kind=_cross_instrument_price_kind(target.timeframe),
            points=points,
        ),
        None,
        invalid_price_count,
    )


def _cross_instrument_price(
    timeframe: str,
    row: tuple[object, ...],
) -> float | None:
    try:
        if timeframe == M1:
            price = float(str(row[4]))
        elif timeframe == TICK:
            price = (float(str(row[1])) + float(str(row[2]))) / 2.0
        else:
            return None
    except (IndexError, TypeError, ValueError):
        return None
    if not math.isfinite(price) or price <= 0.0:
        return None
    return price


def _cross_instrument_price_kind(timeframe: str) -> str:
    if timeframe == M1:
        return "close_bid"
    if timeframe == TICK:
        return "mid_bid_ask"
    return "unknown"


def _series_by_timeframe_period(
    series: Iterable[_CrossInstrumentSeries],
) -> dict[tuple[str, str], list[_CrossInstrumentSeries]]:
    grouped: dict[tuple[str, str], list[_CrossInstrumentSeries]] = {}
    for item in series:
        key = (item.timeframe, item.period)
        grouped.setdefault(key, []).append(item)
    for group in grouped.values():
        group.sort(key=lambda item: item.symbol)
    return grouped


def _record_timestamp_grid_checks(
    scan: _CrossInstrumentScan,
    group: list[_CrossInstrumentSeries],
    *,
    timeframe: str,
    period: str,
    tolerance: HistDataCrossInstrumentTolerance,
) -> None:
    scan.timestamp_grid_group_count += 1
    union = set().union(*(series.timestamps for series in group))
    common = set.intersection(*(series.timestamps for series in group))
    if not union:
        return
    ratio = len(common) / len(union)
    if ratio >= tolerance.minimum_common_timestamp_ratio:
        return
    sample = _TimestampGridSample(
        timeframe=timeframe,
        period=period,
        symbols=tuple(series.symbol for series in group),
        union_timestamp_count=len(union),
        common_timestamp_count=len(common),
        common_timestamp_ratio=ratio,
        missing_by_symbol={
            series.symbol: len(union.difference(series.timestamps))
            for series in group
        },
    )
    scan.sparse_grid_count += 1
    _append_sample(scan.sparse_grids, sample)


def _record_stale_join_checks(
    scan: _CrossInstrumentScan,
    group: list[_CrossInstrumentSeries],
    *,
    tolerance: HistDataCrossInstrumentTolerance,
) -> None:
    for left_index, left in enumerate(group):
        for right in group[left_index + 1 :]:
            _record_stale_join_direction(
                scan,
                stale_series=left,
                active_series=right,
                tolerance=tolerance,
            )
            _record_stale_join_direction(
                scan,
                stale_series=right,
                active_series=left,
                tolerance=tolerance,
            )


def _record_stale_join_direction(
    scan: _CrossInstrumentScan,
    *,
    stale_series: _CrossInstrumentSeries,
    active_series: _CrossInstrumentSeries,
    tolerance: HistDataCrossInstrumentTolerance,
) -> None:
    stale_timestamps = stale_series.timestamps
    active_timestamps = active_series.timestamps
    union = sorted(stale_timestamps.union(active_timestamps))
    previous_stale_timestamp: int | None = None
    run_start: int | None = None
    run_end: int | None = None
    run_length = 0
    run_stale_timestamp: int | None = None

    for timestamp in union:
        if timestamp in stale_timestamps:
            _finalize_stale_join_run(
                scan,
                stale_series=stale_series,
                active_series=active_series,
                stale_value_timestamp_utc_ms=run_stale_timestamp,
                start_timestamp_utc_ms=run_start,
                end_timestamp_utc_ms=run_end,
                affected_timestamp_count=run_length,
                tolerance=tolerance,
            )
            previous_stale_timestamp = timestamp
            run_start = None
            run_end = None
            run_length = 0
            run_stale_timestamp = None
            continue

        if (
            timestamp not in active_timestamps
            or previous_stale_timestamp is None
        ):
            _finalize_stale_join_run(
                scan,
                stale_series=stale_series,
                active_series=active_series,
                stale_value_timestamp_utc_ms=run_stale_timestamp,
                start_timestamp_utc_ms=run_start,
                end_timestamp_utc_ms=run_end,
                affected_timestamp_count=run_length,
                tolerance=tolerance,
            )
            run_start = None
            run_end = None
            run_length = 0
            run_stale_timestamp = None
            continue

        if run_start is None:
            run_start = timestamp
            run_stale_timestamp = previous_stale_timestamp
        run_end = timestamp
        run_length += 1

    _finalize_stale_join_run(
        scan,
        stale_series=stale_series,
        active_series=active_series,
        stale_value_timestamp_utc_ms=run_stale_timestamp,
        start_timestamp_utc_ms=run_start,
        end_timestamp_utc_ms=run_end,
        affected_timestamp_count=run_length,
        tolerance=tolerance,
    )


def _finalize_stale_join_run(
    scan: _CrossInstrumentScan,
    *,
    stale_series: _CrossInstrumentSeries,
    active_series: _CrossInstrumentSeries,
    stale_value_timestamp_utc_ms: int | None,
    start_timestamp_utc_ms: int | None,
    end_timestamp_utc_ms: int | None,
    affected_timestamp_count: int,
    tolerance: HistDataCrossInstrumentTolerance,
) -> None:
    if (
        stale_value_timestamp_utc_ms is None
        or start_timestamp_utc_ms is None
        or end_timestamp_utc_ms is None
        or affected_timestamp_count < tolerance.stale_forward_fill_min_run
    ):
        return
    scan.stale_join_risk_count += 1
    _append_sample(
        scan.stale_join_risks,
        _StaleJoinSample(
            stale_series=stale_series,
            active_series=active_series,
            stale_value_timestamp_utc_ms=stale_value_timestamp_utc_ms,
            start_timestamp_utc_ms=start_timestamp_utc_ms,
            end_timestamp_utc_ms=end_timestamp_utc_ms,
            affected_timestamp_count=affected_timestamp_count,
        ),
    )


def _record_triangular_checks(
    scan: _CrossInstrumentScan,
    group: list[_CrossInstrumentSeries],
    *,
    timeframe: str,
    period: str,
    tolerance: HistDataCrossInstrumentTolerance,
    warning_severity: QualitySeverity,
    error_severity: QualitySeverity,
) -> None:
    by_pair = {(series.base, series.quote): series for series in group}
    seen_candidates: set[tuple[str, str, str]] = set()
    for numerator in group:
        for denominator in group:
            if numerator is denominator:
                continue
            if numerator.quote != denominator.quote:
                continue
            direct = by_pair.get((numerator.base, denominator.base))
            if direct is None:
                continue
            candidate_key = (
                direct.symbol,
                numerator.symbol,
                denominator.symbol,
            )
            if candidate_key in seen_candidates:
                continue
            seen_candidates.add(candidate_key)
            scan.triangular_candidate_count += 1
            common = sorted(
                numerator.timestamps.intersection(
                    denominator.timestamps
                ).intersection(direct.timestamps)
            )
            if not common:
                _append_unavailable(
                    scan,
                    _CrossInstrumentUnavailable(
                        reason="triangular_no_common_timestamps",
                        symbols=(
                            direct.symbol,
                            numerator.symbol,
                            denominator.symbol,
                        ),
                        timeframe=timeframe,
                        period=period,
                    ),
                )
                continue
            for timestamp in common:
                numerator_price = numerator.points[timestamp].price
                denominator_price = denominator.points[timestamp].price
                direct_price = direct.points[timestamp].price
                implied_price = numerator_price / denominator_price
                severity = _relative_difference_severity(
                    _relative_difference(implied_price, direct_price),
                    warning_threshold=(
                        tolerance.triangular_warning_relative_tolerance
                    ),
                    error_threshold=(
                        tolerance.triangular_error_relative_tolerance
                    ),
                    warning_severity=warning_severity,
                    error_severity=error_severity,
                )
                scan.triangular_compared_timestamp_count += 1
                if severity is None:
                    continue
                sample = _TriangularComparisonSample(
                    direct=direct,
                    numerator=numerator,
                    denominator=denominator,
                    timestamp_utc_ms=timestamp,
                    direct_price=direct_price,
                    implied_price=implied_price,
                    relative_difference=_relative_difference(
                        implied_price,
                        direct_price,
                    ),
                    severity=severity,
                )
                if severity is error_severity:
                    scan.triangular_error_count += 1
                    _append_sample(scan.triangular_errors, sample)
                else:
                    scan.triangular_warning_count += 1
                    _append_sample(scan.triangular_warnings, sample)


def _record_inverse_checks(
    scan: _CrossInstrumentScan,
    group: list[_CrossInstrumentSeries],
    *,
    timeframe: str,
    period: str,
    tolerance: HistDataCrossInstrumentTolerance,
    warning_severity: QualitySeverity,
    error_severity: QualitySeverity,
) -> None:
    by_pair = {(series.base, series.quote): series for series in group}
    seen_pairs: set[tuple[str, str]] = set()
    for left in group:
        right = by_pair.get((left.quote, left.base))
        if right is None:
            continue
        pair_key = (
            min(left.symbol, right.symbol),
            max(left.symbol, right.symbol),
        )
        if pair_key in seen_pairs:
            continue
        seen_pairs.add(pair_key)
        scan.inverse_candidate_count += 1
        common = sorted(left.timestamps.intersection(right.timestamps))
        if not common:
            _append_unavailable(
                scan,
                _CrossInstrumentUnavailable(
                    reason="inverse_no_common_timestamps",
                    symbols=(left.symbol, right.symbol),
                    timeframe=timeframe,
                    period=period,
                ),
            )
            continue
        for timestamp in common:
            product = (
                left.points[timestamp].price * right.points[timestamp].price
            )
            relative_difference = abs(product - 1.0)
            severity = _relative_difference_severity(
                relative_difference,
                warning_threshold=tolerance.inverse_warning_relative_tolerance,
                error_threshold=tolerance.inverse_error_relative_tolerance,
                warning_severity=warning_severity,
                error_severity=error_severity,
            )
            scan.inverse_compared_timestamp_count += 1
            if severity is None:
                continue
            sample = _InverseComparisonSample(
                left=left,
                right=right,
                timestamp_utc_ms=timestamp,
                left_price=left.points[timestamp].price,
                right_price=right.points[timestamp].price,
                product=product,
                relative_difference=relative_difference,
                severity=severity,
            )
            if severity is error_severity:
                scan.inverse_error_count += 1
                _append_sample(scan.inverse_errors, sample)
            else:
                scan.inverse_warning_count += 1
                _append_sample(scan.inverse_warnings, sample)


def _relative_difference(candidate: float, expected: float) -> float:
    if expected == 0.0:
        return math.inf
    return abs(candidate - expected) / abs(expected)


def _relative_difference_severity(
    relative_difference: float,
    *,
    warning_threshold: float,
    error_threshold: float,
    warning_severity: QualitySeverity,
    error_severity: QualitySeverity,
) -> QualitySeverity | None:
    if relative_difference > error_threshold:
        return error_severity
    if relative_difference > warning_threshold:
        return warning_severity
    return None


def _cross_instrument_payload(
    scan: _CrossInstrumentScan,
    *,
    tolerance: HistDataCrossInstrumentTolerance,
) -> dict[str, JSONValue]:
    return {
        "target_count": scan.target_count,
        "ascii_target_count": scan.ascii_target_count,
        "fx_series_count": len(scan.fx_series),
        "source_error_count": scan.source_error_count,
        "invalid_price_count": scan.invalid_price_count,
        "triangular_candidate_count": scan.triangular_candidate_count,
        "triangular_compared_timestamp_count": (
            scan.triangular_compared_timestamp_count
        ),
        "triangular_warning_count": scan.triangular_warning_count,
        "triangular_error_count": scan.triangular_error_count,
        "inverse_candidate_count": scan.inverse_candidate_count,
        "inverse_compared_timestamp_count": (
            scan.inverse_compared_timestamp_count
        ),
        "inverse_warning_count": scan.inverse_warning_count,
        "inverse_error_count": scan.inverse_error_count,
        "timestamp_grid_group_count": scan.timestamp_grid_group_count,
        "sparse_grid_count": scan.sparse_grid_count,
        "stale_join_risk_count": scan.stale_join_risk_count,
        "unavailable_count": len(scan.unavailable),
        "tolerance": tolerance.to_metadata(),
        "series_profiles": [
            series.to_metadata()
            for series in sorted(
                scan.fx_series,
                key=lambda item: (
                    item.timeframe,
                    item.period,
                    item.symbol,
                    item.target.path,
                ),
            )
        ],
        "unavailable_samples": _metadata_samples(scan.unavailable),
        "triangular_warning_samples": _metadata_samples(
            scan.triangular_warnings
        ),
        "triangular_error_samples": _metadata_samples(scan.triangular_errors),
        "inverse_warning_samples": _metadata_samples(scan.inverse_warnings),
        "inverse_error_samples": _metadata_samples(scan.inverse_errors),
        "sparse_grid_samples": _metadata_samples(scan.sparse_grids),
        "stale_join_risk_samples": _metadata_samples(scan.stale_join_risks),
    }


def _has_cross_instrument_surface(scan: _CrossInstrumentScan) -> bool:
    return scan.target_count > 0


def _cross_instrument_target(
    targets: tuple[QualityTarget, ...],
    *,
    metadata: Mapping[str, JSONValue] | None,
    payload: Mapping[str, JSONValue],
) -> QualityTarget:
    root = _quality_run_root(metadata)
    if not root and targets:
        root = str(Path(targets[0].path).parent)
    return QualityTarget(
        path=root or "cross-instrument-consistency",
        kind=QualityTargetKind.DIRECTORY,
        data_format="ascii",
        metadata={
            "manifest": "cross-instrument-consistency",
            "rule_id": DOMAIN_CROSS_INSTRUMENT_RULE_ID,
            "target_count": _json_int(payload.get("target_count")),
            "fx_series_count": _json_int(payload.get("fx_series_count")),
            "triangular_candidate_count": _json_int(
                payload.get("triangular_candidate_count")
            ),
            "inverse_candidate_count": _json_int(
                payload.get("inverse_candidate_count")
            ),
            "sparse_grid_count": _json_int(payload.get("sparse_grid_count")),
            "stale_join_risk_count": _json_int(
                payload.get("stale_join_risk_count")
            ),
            "unavailable_count": _json_int(payload.get("unavailable_count")),
        },
    )


def _quality_run_root(metadata: Mapping[str, JSONValue] | None) -> str:
    metadata_map = dict(metadata or {})
    roots = metadata_map.get("roots")
    if isinstance(roots, list) and roots:
        return str(roots[0])
    return ""


def _cross_instrument_findings(
    target: QualityTarget,
    *,
    scan: _CrossInstrumentScan,
    tolerance: HistDataCrossInstrumentTolerance,
    rule_id: str,
) -> tuple[QualityFinding, ...]:
    findings: list[QualityFinding] = [
        _cross_instrument_finding(
            target,
            code="DOMAIN_CROSS_INSTRUMENT_SUMMARY",
            message="Cross-instrument FX consistency profile.",
            severity=QualitySeverity.INFO,
            rule_id=rule_id,
            metadata=_cross_instrument_payload(scan, tolerance=tolerance),
        )
    ]
    if scan.unavailable:
        findings.append(
            _cross_instrument_finding(
                target,
                code="DOMAIN_CROSS_INSTRUMENT_UNAVAILABLE",
                message="One or more cross-instrument consistency checks are "
                "unavailable because required symbol sets or timestamp "
                "overlaps are absent.",
                severity=QualitySeverity.INFO,
                rule_id=rule_id,
                metadata={
                    "row_count": len(scan.unavailable),
                    "samples": _metadata_samples(scan.unavailable),
                },
            )
        )
    findings.extend(
        _comparison_findings(
            warning_code="DOMAIN_CROSS_INSTRUMENT_TRIANGULAR_WARNING",
            error_code="DOMAIN_CROSS_INSTRUMENT_TRIANGULAR_ERROR",
            warning_message=(
                "Triangular FX relationship differs from the direct pair "
                "beyond the warning tolerance."
            ),
            error_message=(
                "Triangular FX relationship differs from the direct pair "
                "beyond the error tolerance."
            ),
            warning_samples=scan.triangular_warnings,
            error_samples=scan.triangular_errors,
            warning_count=scan.triangular_warning_count,
            error_count=scan.triangular_error_count,
            rule_id=rule_id,
            target=target,
        )
    )
    findings.extend(
        _comparison_findings(
            warning_code="DOMAIN_CROSS_INSTRUMENT_INVERSE_WARNING",
            error_code="DOMAIN_CROSS_INSTRUMENT_INVERSE_ERROR",
            warning_message=(
                "Inverse FX pair product differs from one beyond the "
                "warning tolerance."
            ),
            error_message=(
                "Inverse FX pair product differs from one beyond the error "
                "tolerance."
            ),
            warning_samples=scan.inverse_warnings,
            error_samples=scan.inverse_errors,
            warning_count=scan.inverse_warning_count,
            error_count=scan.inverse_error_count,
            rule_id=rule_id,
            target=target,
        )
    )
    if scan.sparse_grids:
        findings.append(
            _cross_instrument_finding(
                target,
                code="DOMAIN_CROSS_INSTRUMENT_TIMESTAMP_GRID_SPARSE",
                message="Common timestamp grid coverage is sparse across "
                "one or more instrument groups.",
                severity=QualitySeverity.WARNING,
                rule_id=rule_id,
                metadata={
                    "row_count": scan.sparse_grid_count,
                    "minimum_common_timestamp_ratio": (
                        tolerance.minimum_common_timestamp_ratio
                    ),
                    "samples": _metadata_samples(scan.sparse_grids),
                },
            )
        )
    if scan.stale_join_risks:
        findings.append(
            _cross_instrument_finding(
                target,
                code="DOMAIN_CROSS_INSTRUMENT_STALE_JOIN_RISK",
                message="Sparse timestamp alignment would forward-fill stale "
                "instrument prices into active instrument timestamps.",
                severity=QualitySeverity.WARNING,
                rule_id=rule_id,
                metadata={
                    "row_count": scan.stale_join_risk_count,
                    "stale_forward_fill_min_run": (
                        tolerance.stale_forward_fill_min_run
                    ),
                    "samples": _metadata_samples(scan.stale_join_risks),
                },
            )
        )
    return tuple(findings)


def _comparison_findings(
    *,
    target: QualityTarget,
    warning_code: str,
    error_code: str,
    warning_message: str,
    error_message: str,
    warning_samples: Sequence[
        _TriangularComparisonSample | _InverseComparisonSample
    ],
    error_samples: Sequence[
        _TriangularComparisonSample | _InverseComparisonSample
    ],
    warning_count: int,
    error_count: int,
    rule_id: str,
) -> tuple[QualityFinding, ...]:
    findings: list[QualityFinding] = []
    if warning_samples:
        findings.append(
            _cross_instrument_finding(
                target,
                code=warning_code,
                message=warning_message,
                severity=QualitySeverity.WARNING,
                rule_id=rule_id,
                metadata={
                    "row_count": warning_count,
                    "samples": _metadata_samples(warning_samples),
                },
            )
        )
    if error_samples:
        findings.append(
            _cross_instrument_finding(
                target,
                code=error_code,
                message=error_message,
                severity=QualitySeverity.ERROR,
                rule_id=rule_id,
                metadata={
                    "row_count": error_count,
                    "samples": _metadata_samples(error_samples),
                },
            )
        )
    return tuple(findings)


def _cross_instrument_finding(
    target: QualityTarget,
    *,
    code: str,
    message: str,
    severity: QualitySeverity,
    rule_id: str,
    metadata: dict[str, JSONValue],
) -> QualityFinding:
    first_sample = _first_sample(metadata)
    location_metadata = {}
    if isinstance(first_sample, Mapping):
        location_metadata = {
            key: value
            for key, value in first_sample.items()
            if key
            in {
                "direct_symbol",
                "left_symbol",
                "right_symbol",
                "symbols",
                "stale_symbol",
                "active_symbol",
                "timeframe",
                "period",
                "reason",
            }
        }
    return QualityFinding(
        severity=QualitySeverity.from_value(severity),
        code=code,
        message=message,
        rule_id=rule_id,
        target=target,
        location=QualityLocation(
            path=target.path,
            timestamp_utc_ms=_sample_timestamp(first_sample),
            column="price",
            metadata=location_metadata,
        ),
        metadata=dict(metadata),
    )


def _first_sample(metadata: Mapping[str, JSONValue]) -> JSONValue | None:
    samples = metadata.get("samples")
    if isinstance(samples, list) and samples:
        return samples[0]
    return None


def _sample_timestamp(sample: JSONValue | None) -> int | None:
    if not isinstance(sample, Mapping):
        return None
    value = sample.get("timestamp_utc_ms")
    if value is None:
        value = sample.get("start_timestamp_utc_ms")
    if isinstance(value, bool | list | dict):
        return None
    try:
        return None if value is None else int(value)
    except (TypeError, ValueError):
        return None


def _metadata_samples(
    samples: Iterable[
        _CrossInstrumentUnavailable
        | _TriangularComparisonSample
        | _InverseComparisonSample
        | _TimestampGridSample
        | _StaleJoinSample
    ],
) -> list[JSONValue]:
    return [sample.to_metadata() for sample in samples]


def _append_unavailable(
    scan: _CrossInstrumentScan,
    sample: _CrossInstrumentUnavailable,
) -> None:
    _append_sample(scan.unavailable, sample)


def _append_sample(
    samples: list[_CrossSample],
    sample: _CrossSample,
) -> None:
    if len(samples) < MAX_CROSS_INSTRUMENT_SAMPLES:
        samples.append(sample)


def _json_int(value: JSONValue | None) -> int:
    if isinstance(value, bool | list | dict) or value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _precision_value(
    rule: HistDataSymbolPrecisionRule | None,
    field_name: str,
) -> str:
    if rule is None:
        return ""
    return str(getattr(rule, field_name))


def _split_pair_value(
    pair_value: str | None,
    normalized: str,
) -> tuple[str, str]:
    if pair_value:
        base, quote = pair_value.split("_", maxsplit=1)
        return base, quote
    if len(normalized) >= 6:
        return normalized[:3], normalized[3:6]
    return "", ""


def _aliases_for_symbol(normalized: str) -> tuple[str, ...]:
    return _PAIR_ALIASES_BY_SYMBOL.get(normalized, ())


_PAIR_VALUES_BY_SYMBOL: dict[str, str] = {}
_PAIR_KEYS_BY_SYMBOL: dict[str, str] = {}
_PAIR_ALIASES_BY_SYMBOL: dict[str, tuple[str, ...]] = {}
for _pair in Pairs:
    _normalized_key = normalize_histdata_symbol(_pair.name)
    _normalized_value = normalize_histdata_symbol(_pair.value)
    _PAIR_VALUES_BY_SYMBOL[_normalized_key] = _pair.value
    _PAIR_VALUES_BY_SYMBOL[_normalized_value] = _pair.value
    _PAIR_KEYS_BY_SYMBOL[_normalized_key] = _pair.name
    _PAIR_KEYS_BY_SYMBOL[_normalized_value] = _pair.name
    _aliases = tuple(
        dict.fromkeys(
            (
                _pair.name.upper(),
                _pair.value,
                _normalized_value,
            )
        )
    )
    _PAIR_ALIASES_BY_SYMBOL[_normalized_key] = _aliases
    _PAIR_ALIASES_BY_SYMBOL[_normalized_value] = _aliases
