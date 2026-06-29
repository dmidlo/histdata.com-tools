"""Tick bid/ask spread quality checks for HistData ASCII artifacts."""

from __future__ import annotations

import csv
import math
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TypeVar, cast
import zipfile

from histdatacom.data_quality.contracts import (
    QualityFinding,
    QualityLocation,
    QualityRule,
    QualityRuleResult,
    QualitySeverity,
    QualityTarget,
    QualityTargetKind,
)
from histdatacom.data_quality.calendar import (
    SESSION_ASIA,
    SESSION_LONDON,
    SESSION_MARKET_CLOSED,
    SESSION_NEW_YORK,
    SESSION_NO_ACTIVE_WINDOW,
    SESSION_STATE_FRIDAY_CLOSE,
    SESSION_STATE_MARKET_OPEN,
    SESSION_STATE_SUNDAY_OPEN,
    SESSION_STATE_WEEKEND_CLOSURE,
)
from histdatacom.data_quality.polars_cache import read_quality_polars_cache
from histdatacom.data_quality.symbols import (
    ASSET_CLASS_INDEX,
    ASSET_CLASS_METAL,
    ASSET_CLASS_OIL,
    normalize_histdata_symbol,
    symbol_metadata_for,
)
from histdatacom.histdata_ascii import (
    EST_NO_DST_OFFSET_MS,
    TICK,
    columns_for_timeframe,
    delimiter_for_timeframe,
    parse_histdata_datetime_to_utc_ms,
)
from histdatacom.runtime_contracts import JSONValue

ASCII_TICK_SPREAD_RULE_ID = "ticks.ascii.spread"
ASCII_TICK_MICROSTRUCTURE_RULE_ID = "ticks.ascii.microstructure"
ASCII_TICK_SPREAD_REGIME_RULE_ID = "ticks.ascii.spread_regimes"
SOURCE_TIMEZONE = "EST-no-DST"
SOURCE_UTC_OFFSET = "-05:00"
CANONICAL_TIMEZONE = "UTC"
MAX_TICK_SAMPLES = 5
TICK_PRICE_COLUMNS = ("bid", "ask")
DEFAULT_SESSION_PROFILE = "default"
DUPLICATE_TICK_OWNER_RULE_ID = "time.ascii.timestamp_sequence"
DUPLICATE_TICK_OWNER_FINDING_CODE = "ASCII_TICK_DUPLICATE_ROW"
SPECIAL_SPREAD_REGIMES = ("daily_rollover", "sunday_open", "friday_close")
FX_FRIDAY_CLOSE_WEEKDAY = 4
FX_SUNDAY_OPEN_WEEKDAY = 6
FX_CLOSE_OPEN_MINUTE = 17 * 60
DAILY_ROLLOVER_SOURCE_MINUTES = (16 * 60 + 55, 17 * 60 + 5)
FRIDAY_CLOSE_SOURCE_MINUTES = (16 * 60, 17 * 60)
SUNDAY_OPEN_SOURCE_MINUTES = (17 * 60, 18 * 60)
LONDON_FIX_UTC_MINUTES = (15 * 60 + 55, 16 * 60 + 5)
_ThresholdT = TypeVar("_ThresholdT")


@dataclass(frozen=True, slots=True)
class HistDataTickSpreadThresholds:
    """Configurable tick spread thresholds and severities."""

    zero_spread_run_length: int = 1

    def __post_init__(self) -> None:
        """Validate threshold values at construction time."""
        if self.zero_spread_run_length < 1:
            msg = "zero_spread_run_length must be at least 1"
            raise ValueError(msg)

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return JSON-compatible threshold metadata."""
        return {"zero_spread_run_length": self.zero_spread_run_length}


DEFAULT_TICK_SPREAD_THRESHOLDS = HistDataTickSpreadThresholds()
DEFAULT_TICK_SPREAD_THRESHOLDS_BY_ASSET_CLASS = {
    ASSET_CLASS_METAL: HistDataTickSpreadThresholds(
        zero_spread_run_length=2,
    ),
    ASSET_CLASS_OIL: HistDataTickSpreadThresholds(
        zero_spread_run_length=2,
    ),
    ASSET_CLASS_INDEX: HistDataTickSpreadThresholds(
        zero_spread_run_length=2,
    ),
}


@dataclass(frozen=True, slots=True)
class HistDataTickMicrostructureThresholds:
    """Configurable warning thresholds for tick microstructure checks."""

    stale_quote_run_length: int = 3
    stale_max_gap_ms: int = 60_000
    burst_max_interval_ms: int = 100
    burst_run_length: int = 3
    one_sided_run_length: int = 2

    def __post_init__(self) -> None:
        """Validate threshold values at construction time."""
        if self.stale_quote_run_length < 2:
            msg = "stale_quote_run_length must be at least 2"
            raise ValueError(msg)
        if self.stale_max_gap_ms < 0:
            msg = "stale_max_gap_ms must be non-negative"
            raise ValueError(msg)
        if self.burst_max_interval_ms < 0:
            msg = "burst_max_interval_ms must be non-negative"
            raise ValueError(msg)
        if self.burst_run_length < 2:
            msg = "burst_run_length must be at least 2"
            raise ValueError(msg)
        if self.one_sided_run_length < 1:
            msg = "one_sided_run_length must be at least 1"
            raise ValueError(msg)

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return JSON-compatible threshold metadata."""
        return {
            "stale_quote_run_length": self.stale_quote_run_length,
            "stale_max_gap_ms": self.stale_max_gap_ms,
            "burst_max_interval_ms": self.burst_max_interval_ms,
            "burst_run_length": self.burst_run_length,
            "one_sided_run_length": self.one_sided_run_length,
        }


DEFAULT_TICK_MICROSTRUCTURE_THRESHOLDS = HistDataTickMicrostructureThresholds()
DEFAULT_TICK_MICROSTRUCTURE_THRESHOLDS_BY_ASSET_CLASS = {
    ASSET_CLASS_METAL: HistDataTickMicrostructureThresholds(
        stale_quote_run_length=4,
        stale_max_gap_ms=120_000,
        burst_max_interval_ms=250,
        burst_run_length=4,
        one_sided_run_length=3,
    ),
    ASSET_CLASS_OIL: HistDataTickMicrostructureThresholds(
        stale_quote_run_length=4,
        stale_max_gap_ms=120_000,
        burst_max_interval_ms=250,
        burst_run_length=4,
        one_sided_run_length=3,
    ),
    ASSET_CLASS_INDEX: HistDataTickMicrostructureThresholds(
        stale_quote_run_length=4,
        stale_max_gap_ms=180_000,
        burst_max_interval_ms=500,
        burst_run_length=4,
        one_sided_run_length=3,
    ),
}


@dataclass(frozen=True, slots=True)
class HistDataTickSpreadRegimeThresholds:
    """Configurable warning thresholds for tick spread-regime profiles."""

    wide_spread_multiplier: float = 3.0
    jump_spread_multiplier: float = 2.0
    regime_median_multiplier: float = 2.0
    minimum_wide_spread: float = 0.0
    minimum_spread_jump: float = 0.0

    def __post_init__(self) -> None:
        """Validate spread-regime threshold values."""
        if self.wide_spread_multiplier <= 1.0:
            msg = "wide_spread_multiplier must be greater than 1.0"
            raise ValueError(msg)
        if self.jump_spread_multiplier <= 0.0:
            msg = "jump_spread_multiplier must be positive"
            raise ValueError(msg)
        if self.regime_median_multiplier <= 1.0:
            msg = "regime_median_multiplier must be greater than 1.0"
            raise ValueError(msg)
        if self.minimum_wide_spread < 0.0:
            msg = "minimum_wide_spread must be non-negative"
            raise ValueError(msg)
        if self.minimum_spread_jump < 0.0:
            msg = "minimum_spread_jump must be non-negative"
            raise ValueError(msg)

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return JSON-compatible threshold metadata."""
        return {
            "wide_spread_multiplier": self.wide_spread_multiplier,
            "jump_spread_multiplier": self.jump_spread_multiplier,
            "regime_median_multiplier": self.regime_median_multiplier,
            "minimum_wide_spread": self.minimum_wide_spread,
            "minimum_spread_jump": self.minimum_spread_jump,
            "special_spread_regimes": list(SPECIAL_SPREAD_REGIMES),
        }


DEFAULT_TICK_SPREAD_REGIME_THRESHOLDS = HistDataTickSpreadRegimeThresholds()
DEFAULT_TICK_SPREAD_REGIME_THRESHOLDS_BY_ASSET_CLASS = {
    ASSET_CLASS_METAL: HistDataTickSpreadRegimeThresholds(
        wide_spread_multiplier=5.0,
        jump_spread_multiplier=3.0,
        regime_median_multiplier=3.0,
        minimum_wide_spread=0.05,
        minimum_spread_jump=0.05,
    ),
    ASSET_CLASS_OIL: HistDataTickSpreadRegimeThresholds(
        wide_spread_multiplier=5.0,
        jump_spread_multiplier=3.0,
        regime_median_multiplier=3.0,
        minimum_wide_spread=0.02,
        minimum_spread_jump=0.02,
    ),
    ASSET_CLASS_INDEX: HistDataTickSpreadRegimeThresholds(
        wide_spread_multiplier=5.0,
        jump_spread_multiplier=3.0,
        regime_median_multiplier=3.0,
        minimum_wide_spread=0.5,
        minimum_spread_jump=0.5,
    ),
}


@dataclass(frozen=True, slots=True)
class _TickSpreadThresholdSelection:
    thresholds: HistDataTickSpreadThresholds
    source: str
    symbol_key: str
    asset_class_key: str
    profile_key: str

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return JSON-compatible threshold selection metadata."""
        return {
            "source": self.source,
            "symbol_key": self.symbol_key,
            "asset_class_key": self.asset_class_key,
            "profile_key": self.profile_key,
            "values": self.thresholds.to_metadata(),
        }


@dataclass(frozen=True, slots=True)
class _TickSpreadRegimeThresholdSelection:
    thresholds: HistDataTickSpreadRegimeThresholds
    source: str
    symbol_key: str
    asset_class_key: str
    profile_key: str

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return JSON-compatible threshold selection metadata."""
        return {
            "source": self.source,
            "symbol_key": self.symbol_key,
            "asset_class_key": self.asset_class_key,
            "profile_key": self.profile_key,
            "values": self.thresholds.to_metadata(),
        }


@dataclass(frozen=True, slots=True)
class _TickMicrostructureThresholdSelection:
    thresholds: HistDataTickMicrostructureThresholds
    source: str
    symbol_key: str
    session_key: str
    asset_class_key: str
    profile_key: str

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return JSON-compatible threshold selection metadata."""
        return {
            "source": self.source,
            "symbol_key": self.symbol_key,
            "session_key": self.session_key,
            "asset_class_key": self.asset_class_key,
            "profile_key": self.profile_key,
            "values": self.thresholds.to_metadata(),
        }


@dataclass(frozen=True, slots=True)
class _TextPayload:
    data: bytes
    source_member: str = ""


@dataclass(frozen=True, slots=True)
class _TickLineSource:
    iter_lines: Callable[[], Iterable[str]]
    source_member: str = ""


@dataclass(frozen=True, slots=True)
class _SourceReadError(Exception):
    code: str
    message: str
    metadata: dict[str, JSONValue]


@dataclass(frozen=True, slots=True)
class _TickSpreadSample:
    row_number: int
    timestamp_source: str
    timestamp_utc_ms: int | None
    column: str
    bid: float | None
    ask: float | None
    spread: float | None
    raw_values: tuple[str, ...]
    source_member: str = ""
    metadata: dict[str, JSONValue] = field(default_factory=dict)

    @property
    def utc_timestamp(self) -> str:
        """Return canonical UTC timestamp text, when available."""
        if self.timestamp_utc_ms is None:
            return ""
        return _utc_iso_from_ms(self.timestamp_utc_ms)

    def values_metadata(self) -> dict[str, JSONValue]:
        """Return JSON-compatible bid/ask/spread values."""
        return {
            "bid": self.bid,
            "ask": self.ask,
            "spread": self.spread,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a bounded JSON-compatible sample."""
        return {
            "row_number": self.row_number,
            "timestamp_source": self.timestamp_source,
            "timestamp_utc_ms": self.timestamp_utc_ms,
            "utc_timestamp": self.utc_timestamp,
            "column": self.column,
            "values": self.values_metadata(),
            "raw_values": list(self.raw_values),
            "source_member": self.source_member,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True, slots=True)
class _ZeroSpreadRunSample:
    start: _TickSpreadSample
    end: _TickSpreadSample
    run_length: int

    @property
    def row_number(self) -> int:
        """Return the first row number in the run."""
        return self.start.row_number

    @property
    def timestamp_source(self) -> str:
        """Return the first source timestamp in the run."""
        return self.start.timestamp_source

    @property
    def timestamp_utc_ms(self) -> int | None:
        """Return the first normalized timestamp in the run."""
        return self.start.timestamp_utc_ms

    @property
    def utc_timestamp(self) -> str:
        """Return canonical UTC timestamp text for the run start."""
        return self.start.utc_timestamp

    @property
    def source_member(self) -> str:
        """Return source ZIP member context, when available."""
        return self.start.source_member

    def to_dict(self) -> dict[str, JSONValue]:
        """Return bounded JSON-compatible zero-spread run context."""
        return {
            "run_start_row_number": self.start.row_number,
            "run_end_row_number": self.end.row_number,
            "run_length": self.run_length,
            "run_start_timestamp_source": self.start.timestamp_source,
            "run_end_timestamp_source": self.end.timestamp_source,
            "run_start_timestamp_utc_ms": self.start.timestamp_utc_ms,
            "run_end_timestamp_utc_ms": self.end.timestamp_utc_ms,
            "run_start_utc_timestamp": self.start.utc_timestamp,
            "run_end_utc_timestamp": self.end.utc_timestamp,
            "values": self.start.values_metadata(),
            "source_member": self.source_member,
        }


@dataclass(frozen=True, slots=True)
class _TickMicrostructureRunSample:
    start: _TickSpreadSample
    end: _TickSpreadSample
    run_length: int
    metric: str
    direction: str = ""
    metadata: dict[str, JSONValue] = field(default_factory=dict)

    @property
    def row_number(self) -> int:
        """Return the first row number in the run."""
        return self.start.row_number

    @property
    def timestamp_source(self) -> str:
        """Return the first source timestamp in the run."""
        return self.start.timestamp_source

    @property
    def timestamp_utc_ms(self) -> int | None:
        """Return the first normalized timestamp in the run."""
        return self.start.timestamp_utc_ms

    @property
    def utc_timestamp(self) -> str:
        """Return canonical UTC timestamp text for the run start."""
        return self.start.utc_timestamp

    @property
    def source_member(self) -> str:
        """Return source ZIP member context, when available."""
        return self.start.source_member

    def to_dict(self) -> dict[str, JSONValue]:
        """Return bounded JSON-compatible run context."""
        return {
            "run_start_row_number": self.start.row_number,
            "run_end_row_number": self.end.row_number,
            "run_length": self.run_length,
            "metric": self.metric,
            "direction": self.direction,
            "run_start_timestamp_source": self.start.timestamp_source,
            "run_end_timestamp_source": self.end.timestamp_source,
            "run_start_timestamp_utc_ms": self.start.timestamp_utc_ms,
            "run_end_timestamp_utc_ms": self.end.timestamp_utc_ms,
            "run_start_utc_timestamp": self.start.utc_timestamp,
            "run_end_utc_timestamp": self.end.utc_timestamp,
            "start_values": self.start.values_metadata(),
            "end_values": self.end.values_metadata(),
            "source_member": self.source_member,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True, slots=True)
class _TickSpreadRegimeSample:
    sample: _TickSpreadSample
    symbol_key: str
    source_hour: str
    utc_hour: str
    session_state: str
    session_keys: tuple[str, ...]
    special_regime_keys: tuple[str, ...]
    previous: _TickSpreadSample | None = None
    spread_delta: float | None = None
    threshold: float | None = None
    profile_key: str = ""

    @property
    def row_number(self) -> int:
        """Return source row number."""
        return self.sample.row_number

    @property
    def timestamp_source(self) -> str:
        """Return source timestamp text."""
        return self.sample.timestamp_source

    @property
    def timestamp_utc_ms(self) -> int | None:
        """Return normalized timestamp."""
        return self.sample.timestamp_utc_ms

    @property
    def utc_timestamp(self) -> str:
        """Return canonical UTC timestamp text."""
        return self.sample.utc_timestamp

    @property
    def spread(self) -> float | None:
        """Return row spread."""
        return self.sample.spread

    @property
    def source_member(self) -> str:
        """Return source ZIP member context, when available."""
        return self.sample.source_member

    def to_dict(self) -> dict[str, JSONValue]:
        """Return bounded JSON-compatible regime sample context."""
        return {
            "row_number": self.row_number,
            "timestamp_source": self.timestamp_source,
            "timestamp_utc_ms": self.timestamp_utc_ms,
            "utc_timestamp": self.utc_timestamp,
            "symbol": self.symbol_key,
            "source_hour": self.source_hour,
            "utc_hour": self.utc_hour,
            "session_state": self.session_state,
            "session_keys": list(self.session_keys),
            "special_regime_keys": list(self.special_regime_keys),
            "profile_key": self.profile_key,
            "values": self.sample.values_metadata(),
            "spread_delta": self.spread_delta,
            "threshold": self.threshold,
            "previous": (
                None
                if self.previous is None
                else {
                    "row_number": self.previous.row_number,
                    "timestamp_source": self.previous.timestamp_source,
                    "timestamp_utc_ms": self.previous.timestamp_utc_ms,
                    "utc_timestamp": self.previous.utc_timestamp,
                    "values": self.previous.values_metadata(),
                }
            ),
            "source_member": self.source_member,
        }


@dataclass(slots=True)
class _SpreadRegimeProfile:
    values: list[float] = field(default_factory=list)
    samples: list[_TickSpreadRegimeSample] = field(default_factory=list)
    max_sample: _TickSpreadRegimeSample | None = None

    def add(self, sample: _TickSpreadRegimeSample) -> None:
        """Record one spread in this profile."""
        if sample.spread is None:
            return
        self.values.append(sample.spread)
        current_max = (
            None if self.max_sample is None else self.max_sample.spread
        )
        if current_max is None or sample.spread > current_max:
            self.max_sample = _materialize_spread_regime_sample(sample)
        if len(self.samples) < MAX_TICK_SAMPLES:
            self.samples.append(_materialize_spread_regime_sample(sample))

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return robust spread summary metadata."""
        return {
            **_spread_stats_metadata(self.values),
            "samples": [sample.to_dict() for sample in self.samples],
        }


@dataclass(slots=True)
class _TickSpreadScan:
    row_count: int = 0
    parsed_row_count: int = 0
    missing_bid_ask_count: int = 0
    invalid_bid_ask_count: int = 0
    negative_spread_count: int = 0
    zero_spread_count: int = 0
    zero_spread_run_count: int = 0
    min_spread: float | None = None
    max_spread: float | None = None
    missing_bid_ask: list[_TickSpreadSample] = field(default_factory=list)
    invalid_bid_ask: list[_TickSpreadSample] = field(default_factory=list)
    negative_spreads: list[_TickSpreadSample] = field(default_factory=list)
    zero_spread_runs: list[_ZeroSpreadRunSample] = field(default_factory=list)


@dataclass(slots=True)
class _TickMicrostructureScan:
    row_count: int = 0
    parsed_row_count: int = 0
    invalid_tick_count: int = 0
    duplicate_row_count: int = 0
    stale_quote_repeat_count: int = 0
    stale_quote_run_count: int = 0
    stale_quote_run_row_count: int = 0
    burst_interval_count: int = 0
    burst_run_count: int = 0
    burst_tick_count: int = 0
    one_sided_movement_count: int = 0
    one_sided_run_count: int = 0
    bid_only_movement_count: int = 0
    ask_only_movement_count: int = 0
    duplicate_rows: list[_TickSpreadSample] = field(default_factory=list)
    stale_quote_runs: list[_TickMicrostructureRunSample] = field(
        default_factory=list
    )
    burst_runs: list[_TickMicrostructureRunSample] = field(default_factory=list)
    one_sided_runs: list[_TickMicrostructureRunSample] = field(
        default_factory=list
    )


@dataclass(slots=True)
class _TickSpreadRegimeScan:
    row_count: int = 0
    parsed_row_count: int = 0
    profiled_row_count: int = 0
    invalid_tick_count: int = 0
    invalid_timestamp_count: int = 0
    negative_spread_count: int = 0
    symbol_profiles: dict[str, _SpreadRegimeProfile] = field(
        default_factory=dict
    )
    source_hour_profiles: dict[str, _SpreadRegimeProfile] = field(
        default_factory=dict
    )
    session_profiles: dict[str, _SpreadRegimeProfile] = field(
        default_factory=dict
    )
    special_regime_profiles: dict[str, _SpreadRegimeProfile] = field(
        default_factory=dict
    )
    liquid_profile: _SpreadRegimeProfile = field(
        default_factory=_SpreadRegimeProfile
    )
    global_profile: _SpreadRegimeProfile = field(
        default_factory=_SpreadRegimeProfile
    )
    wide_spread_count: int = 0
    spread_jump_count: int = 0
    regime_shift_count: int = 0
    spread_jump_values: list[float] = field(default_factory=list)
    wide_candidates: list[_TickSpreadRegimeSample] = field(default_factory=list)
    spread_jump_candidates: list[_TickSpreadRegimeSample] = field(
        default_factory=list
    )
    wide_spreads: list[_TickSpreadRegimeSample] = field(default_factory=list)
    spread_jumps: list[_TickSpreadRegimeSample] = field(default_factory=list)
    regime_shifts: list[_TickSpreadRegimeSample] = field(default_factory=list)


@dataclass(slots=True)
class _TickQualityScans:
    spread: _TickSpreadScan
    microstructure: _TickMicrostructureScan
    spread_regime: _TickSpreadRegimeScan
    source_member: str = ""


@dataclass(slots=True)
class HistDataAsciiTickSpreadRule:
    """Validate tick bid/ask ordering and zero-spread regimes."""

    thresholds: HistDataTickSpreadThresholds = DEFAULT_TICK_SPREAD_THRESHOLDS
    thresholds_by_asset_class: Mapping[
        str,
        HistDataTickSpreadThresholds,
    ] = field(
        default_factory=lambda: dict(
            DEFAULT_TICK_SPREAD_THRESHOLDS_BY_ASSET_CLASS
        )
    )
    zero_spread_severity: QualitySeverity = QualitySeverity.WARNING
    negative_spread_severity: QualitySeverity = QualitySeverity.ERROR
    schema_severity: QualitySeverity = QualitySeverity.ERROR
    rule_id: str = ASCII_TICK_SPREAD_RULE_ID
    description: str = (
        "Validate HistData tick bid/ask parseability, ask-bid spread "
        "non-negativity, and zero-spread runs."
    )

    def evaluate(self, target: QualityTarget) -> tuple[QualityFinding, ...]:
        """Return tick spread findings for one target."""
        if not _is_tick_ascii_text_target(target):
            return ()
        if target.kind is QualityTargetKind.CACHE:
            return _cache_tick_spread_findings(target, self)

        try:
            delimiter = delimiter_for_timeframe(TICK)
            columns = columns_for_timeframe(TICK)
            line_source = _read_tick_line_source(target, columns=columns)
        except ValueError as exc:
            return (
                _finding(
                    target,
                    code="ASCII_TICK_SPREAD_METADATA_UNSUPPORTED",
                    message="Target metadata does not describe supported "
                    "HistData tick ASCII data.",
                    metadata={"timeframe": target.timeframe, "error": str(exc)},
                ),
            )
        except UnicodeDecodeError as exc:
            return (
                _finding(
                    target,
                    code="ASCII_TICK_SPREAD_TEXT_ENCODING_INVALID",
                    message="ASCII file does not decode as strict UTF-8 for "
                    "tick spread checks.",
                    metadata={
                        "encoding": "utf-8",
                        "error": str(exc),
                        "byte_start": exc.start,
                        "byte_end": exc.end,
                        "source_member": "",
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

        threshold_selection = _spread_threshold_selection(
            target,
            thresholds=self.thresholds,
            thresholds_by_asset_class=self.thresholds_by_asset_class,
        )
        try:
            scan = _scan_tick_spread_rows(
                line_source.iter_lines(),
                target=target,
                delimiter=delimiter,
                columns=columns,
                source_member=line_source.source_member,
                thresholds=threshold_selection.thresholds,
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
        return _spread_findings(
            target=target,
            scan=scan,
            source_member=line_source.source_member,
            threshold_selection=threshold_selection,
            zero_spread_severity=self.zero_spread_severity,
            negative_spread_severity=self.negative_spread_severity,
            schema_severity=self.schema_severity,
            rule_id=self.rule_id,
        )


@dataclass(slots=True)
class HistDataAsciiTickMicrostructureRule:
    """Profile tick duplicate, stale, burst, and one-sided quote patterns."""

    thresholds: HistDataTickMicrostructureThresholds = (
        DEFAULT_TICK_MICROSTRUCTURE_THRESHOLDS
    )
    thresholds_by_symbol: Mapping[
        str,
        HistDataTickMicrostructureThresholds,
    ] = field(default_factory=dict)
    thresholds_by_session: Mapping[
        str,
        HistDataTickMicrostructureThresholds,
    ] = field(default_factory=dict)
    thresholds_by_asset_class: Mapping[
        str,
        HistDataTickMicrostructureThresholds,
    ] = field(
        default_factory=lambda: dict(
            DEFAULT_TICK_MICROSTRUCTURE_THRESHOLDS_BY_ASSET_CLASS
        )
    )
    thresholds_by_symbol_session: Mapping[
        str,
        HistDataTickMicrostructureThresholds,
    ] = field(default_factory=dict)
    session_name: str = DEFAULT_SESSION_PROFILE
    warning_severity: QualitySeverity = QualitySeverity.WARNING
    rule_id: str = ASCII_TICK_MICROSTRUCTURE_RULE_ID
    description: str = (
        "Profile HistData tick microstructure anomalies including exact "
        "duplicate row summaries, stale quotes, bursts, and one-sided "
        "bid/ask movement."
    )

    def evaluate(self, target: QualityTarget) -> tuple[QualityFinding, ...]:
        """Return tick microstructure findings for one target."""
        if not _is_tick_ascii_text_target(target):
            return ()
        if target.kind is QualityTargetKind.CACHE:
            return _cache_tick_microstructure_findings(target, self)

        try:
            delimiter = delimiter_for_timeframe(TICK)
            columns = columns_for_timeframe(TICK)
            line_source = _read_tick_line_source(target, columns=columns)
        except ValueError as exc:
            return (
                _finding(
                    target,
                    code="ASCII_TICK_MICROSTRUCTURE_METADATA_UNSUPPORTED",
                    message="Target metadata does not describe supported "
                    "HistData tick ASCII data.",
                    rule_id=self.rule_id,
                    metadata={"timeframe": target.timeframe, "error": str(exc)},
                ),
            )
        except UnicodeDecodeError as exc:
            return (
                _finding(
                    target,
                    code="ASCII_TICK_MICROSTRUCTURE_TEXT_ENCODING_INVALID",
                    message="ASCII file does not decode as strict UTF-8 for "
                    "tick microstructure checks.",
                    rule_id=self.rule_id,
                    metadata={
                        "encoding": "utf-8",
                        "error": str(exc),
                        "byte_start": exc.start,
                        "byte_end": exc.end,
                        "source_member": "",
                    },
                ),
            )
        except _SourceReadError as exc:
            return (
                _finding(
                    target,
                    code=exc.code.replace("SPREAD", "MICROSTRUCTURE"),
                    message=exc.message.replace(
                        "spread checks",
                        "microstructure checks",
                    ),
                    rule_id=self.rule_id,
                    metadata=exc.metadata,
                ),
            )

        threshold_selection = _microstructure_threshold_selection(
            target,
            thresholds=self.thresholds,
            thresholds_by_symbol=self.thresholds_by_symbol,
            thresholds_by_session=self.thresholds_by_session,
            thresholds_by_asset_class=self.thresholds_by_asset_class,
            thresholds_by_symbol_session=self.thresholds_by_symbol_session,
            session_name=self.session_name,
        )
        try:
            scan = _scan_tick_microstructure_rows(
                line_source.iter_lines(),
                delimiter=delimiter,
                columns=columns,
                source_member=line_source.source_member,
                thresholds=threshold_selection.thresholds,
            )
        except _SourceReadError as exc:
            return (
                _finding(
                    target,
                    code=exc.code.replace("SPREAD", "MICROSTRUCTURE"),
                    message=exc.message.replace(
                        "spread checks",
                        "microstructure checks",
                    ),
                    rule_id=self.rule_id,
                    metadata=exc.metadata,
                ),
            )
        return _microstructure_findings(
            target=target,
            scan=scan,
            source_member=line_source.source_member,
            threshold_selection=threshold_selection,
            severity=self.warning_severity,
            rule_id=self.rule_id,
        )


@dataclass(slots=True)
class HistDataAsciiTickSpreadRegimeRule:
    """Profile tick spread regimes by symbol, hour, session, and rollover."""

    thresholds: HistDataTickSpreadRegimeThresholds = (
        DEFAULT_TICK_SPREAD_REGIME_THRESHOLDS
    )
    thresholds_by_asset_class: Mapping[
        str,
        HistDataTickSpreadRegimeThresholds,
    ] = field(
        default_factory=lambda: dict(
            DEFAULT_TICK_SPREAD_REGIME_THRESHOLDS_BY_ASSET_CLASS
        )
    )
    warning_severity: QualitySeverity = QualitySeverity.WARNING
    schema_severity: QualitySeverity = QualitySeverity.WARNING
    rule_id: str = ASCII_TICK_SPREAD_REGIME_RULE_ID
    description: str = (
        "Profile HistData tick spread regimes by symbol, source hour, "
        "market session, rollover/open/close windows, and spread jumps."
    )

    def evaluate(self, target: QualityTarget) -> tuple[QualityFinding, ...]:
        """Return tick spread-regime findings for one target."""
        if not _is_tick_ascii_text_target(target):
            return ()
        if target.kind is QualityTargetKind.CACHE:
            return _cache_tick_spread_regime_findings(target, self)

        try:
            delimiter = delimiter_for_timeframe(TICK)
            columns = columns_for_timeframe(TICK)
            line_source = _read_tick_line_source(target, columns=columns)
        except ValueError as exc:
            return (
                _finding(
                    target,
                    code="ASCII_TICK_SPREAD_REGIME_METADATA_UNSUPPORTED",
                    message="Target metadata does not describe supported "
                    "HistData tick ASCII data.",
                    severity=self.schema_severity,
                    rule_id=self.rule_id,
                    metadata={"timeframe": target.timeframe, "error": str(exc)},
                ),
            )
        except UnicodeDecodeError as exc:
            return (
                _finding(
                    target,
                    code="ASCII_TICK_SPREAD_REGIME_TEXT_ENCODING_INVALID",
                    message="ASCII file does not decode as strict UTF-8 for "
                    "tick spread-regime checks.",
                    severity=self.schema_severity,
                    rule_id=self.rule_id,
                    metadata={
                        "encoding": "utf-8",
                        "error": str(exc),
                        "byte_start": exc.start,
                        "byte_end": exc.end,
                        "source_member": "",
                    },
                ),
            )
        except _SourceReadError as exc:
            return (
                _finding(
                    target,
                    code=exc.code.replace("SPREAD", "SPREAD_REGIME"),
                    message=exc.message.replace(
                        "spread checks",
                        "spread-regime checks",
                    ),
                    severity=self.schema_severity,
                    rule_id=self.rule_id,
                    metadata=exc.metadata,
                ),
            )

        threshold_selection = _spread_regime_threshold_selection(
            target,
            thresholds=self.thresholds,
            thresholds_by_asset_class=self.thresholds_by_asset_class,
        )
        try:
            scan = _scan_tick_spread_regime_rows(
                line_source.iter_lines,
                target=target,
                delimiter=delimiter,
                columns=columns,
                source_member=line_source.source_member,
                thresholds=threshold_selection.thresholds,
            )
        except _SourceReadError as exc:
            return (
                _finding(
                    target,
                    code=exc.code.replace("SPREAD", "SPREAD_REGIME"),
                    message=exc.message.replace(
                        "spread checks",
                        "spread-regime checks",
                    ),
                    severity=self.schema_severity,
                    rule_id=self.rule_id,
                    metadata=exc.metadata,
                ),
            )
        return _spread_regime_findings(
            target=target,
            scan=scan,
            source_member=line_source.source_member,
            threshold_selection=threshold_selection,
            severity=self.warning_severity,
            rule_id=self.rule_id,
        )


def ticks_quality_rules() -> tuple[QualityRule, ...]:
    """Return tick quality rules in deterministic execution order."""
    spread_rule: QualityRule = HistDataAsciiTickSpreadRule()
    microstructure_rule: QualityRule = HistDataAsciiTickMicrostructureRule()
    regime_rule: QualityRule = HistDataAsciiTickSpreadRegimeRule()
    return (spread_rule, microstructure_rule, regime_rule)


def _cache_tick_spread_findings(
    target: QualityTarget,
    rule: HistDataAsciiTickSpreadRule,
) -> tuple[QualityFinding, ...]:
    threshold_selection = _spread_threshold_selection(
        target,
        thresholds=rule.thresholds,
        thresholds_by_asset_class=rule.thresholds_by_asset_class,
    )
    try:
        scans = _cache_tick_quality_scans(
            target,
            spread_thresholds=threshold_selection.thresholds,
            microstructure_thresholds=DEFAULT_TICK_MICROSTRUCTURE_THRESHOLDS,
            regime_thresholds=DEFAULT_TICK_SPREAD_REGIME_THRESHOLDS,
        )
    except _SourceReadError as exc:
        return (
            _finding(
                target,
                code=exc.code,
                message=exc.message,
                rule_id=rule.rule_id,
                metadata=exc.metadata,
            ),
        )
    return _spread_findings(
        target=target,
        scan=scans.spread,
        source_member=scans.source_member,
        threshold_selection=threshold_selection,
        zero_spread_severity=rule.zero_spread_severity,
        negative_spread_severity=rule.negative_spread_severity,
        schema_severity=rule.schema_severity,
        rule_id=rule.rule_id,
    )


def _cache_tick_microstructure_findings(
    target: QualityTarget,
    rule: HistDataAsciiTickMicrostructureRule,
) -> tuple[QualityFinding, ...]:
    threshold_selection = _microstructure_threshold_selection(
        target,
        thresholds=rule.thresholds,
        thresholds_by_symbol=rule.thresholds_by_symbol,
        thresholds_by_session=rule.thresholds_by_session,
        thresholds_by_asset_class=rule.thresholds_by_asset_class,
        thresholds_by_symbol_session=rule.thresholds_by_symbol_session,
        session_name=rule.session_name,
    )
    try:
        scans = _cache_tick_quality_scans(
            target,
            spread_thresholds=DEFAULT_TICK_SPREAD_THRESHOLDS,
            microstructure_thresholds=threshold_selection.thresholds,
            regime_thresholds=DEFAULT_TICK_SPREAD_REGIME_THRESHOLDS,
        )
    except _SourceReadError as exc:
        return (
            _finding(
                target,
                code=exc.code.replace("SPREAD", "MICROSTRUCTURE"),
                message=exc.message.replace(
                    "spread checks",
                    "microstructure checks",
                ),
                rule_id=rule.rule_id,
                metadata=exc.metadata,
            ),
        )
    return _microstructure_findings(
        target=target,
        scan=scans.microstructure,
        source_member=scans.source_member,
        threshold_selection=threshold_selection,
        severity=rule.warning_severity,
        rule_id=rule.rule_id,
    )


def _cache_tick_spread_regime_findings(
    target: QualityTarget,
    rule: HistDataAsciiTickSpreadRegimeRule,
) -> tuple[QualityFinding, ...]:
    threshold_selection = _spread_regime_threshold_selection(
        target,
        thresholds=rule.thresholds,
        thresholds_by_asset_class=rule.thresholds_by_asset_class,
    )
    try:
        scans = _cache_tick_quality_scans(
            target,
            spread_thresholds=DEFAULT_TICK_SPREAD_THRESHOLDS,
            microstructure_thresholds=DEFAULT_TICK_MICROSTRUCTURE_THRESHOLDS,
            regime_thresholds=threshold_selection.thresholds,
        )
    except _SourceReadError as exc:
        return (
            _finding(
                target,
                code=exc.code.replace("SPREAD", "SPREAD_REGIME"),
                message=exc.message.replace(
                    "spread checks",
                    "spread-regime checks",
                ),
                severity=rule.schema_severity,
                rule_id=rule.rule_id,
                metadata=exc.metadata,
            ),
        )
    return _spread_regime_findings(
        target=target,
        scan=scans.spread_regime,
        source_member=scans.source_member,
        threshold_selection=threshold_selection,
        severity=rule.warning_severity,
        rule_id=rule.rule_id,
    )


def can_evaluate_tick_quality_bundle(
    target: QualityTarget,
    rules: Sequence[QualityRule],
) -> bool:
    """Return whether cache-backed TICK rules can share one scan."""
    return (
        target.kind is QualityTargetKind.CACHE
        and _is_tick_ascii_text_target(target)
        and len(rules) == 3
        and isinstance(rules[0], HistDataAsciiTickSpreadRule)
        and isinstance(rules[1], HistDataAsciiTickMicrostructureRule)
        and isinstance(rules[2], HistDataAsciiTickSpreadRegimeRule)
    )


def evaluate_tick_quality_bundle(
    target: QualityTarget,
    rules: Sequence[QualityRule],
) -> tuple[QualityRuleResult, ...]:
    """Evaluate the cache-backed TICK rule trio through shared scans."""
    if not can_evaluate_tick_quality_bundle(target, rules):
        msg = "target and rules do not describe a cache-backed TICK bundle"
        raise ValueError(msg)

    spread_rule = rules[0]
    microstructure_rule = rules[1]
    regime_rule = rules[2]
    assert isinstance(spread_rule, HistDataAsciiTickSpreadRule)
    assert isinstance(microstructure_rule, HistDataAsciiTickMicrostructureRule)
    assert isinstance(regime_rule, HistDataAsciiTickSpreadRegimeRule)

    try:
        columns = columns_for_timeframe(TICK)
        frame = _read_tick_cache_frame(target, columns=columns)
    except ValueError as exc:
        return (
            QualityRuleResult(
                rule_id=spread_rule.rule_id,
                target=target,
                findings=(
                    _finding(
                        target,
                        code="ASCII_TICK_SPREAD_METADATA_UNSUPPORTED",
                        message="Target metadata does not describe supported "
                        "HistData tick ASCII data.",
                        rule_id=spread_rule.rule_id,
                        metadata={
                            "timeframe": target.timeframe,
                            "error": str(exc),
                        },
                    ),
                ),
            ),
            QualityRuleResult(
                rule_id=microstructure_rule.rule_id,
                target=target,
                findings=(
                    _finding(
                        target,
                        code="ASCII_TICK_MICROSTRUCTURE_METADATA_UNSUPPORTED",
                        message="Target metadata does not describe supported "
                        "HistData tick ASCII data.",
                        rule_id=microstructure_rule.rule_id,
                        metadata={
                            "timeframe": target.timeframe,
                            "error": str(exc),
                        },
                    ),
                ),
            ),
            QualityRuleResult(
                rule_id=regime_rule.rule_id,
                target=target,
                findings=(
                    _finding(
                        target,
                        code="ASCII_TICK_SPREAD_REGIME_METADATA_UNSUPPORTED",
                        message="Target metadata does not describe supported "
                        "HistData tick ASCII data.",
                        severity=regime_rule.schema_severity,
                        rule_id=regime_rule.rule_id,
                        metadata={
                            "timeframe": target.timeframe,
                            "error": str(exc),
                        },
                    ),
                ),
            ),
        )
    except _SourceReadError as exc:
        return _tick_bundle_source_error_results(
            target,
            spread_rule=spread_rule,
            microstructure_rule=microstructure_rule,
            regime_rule=regime_rule,
            source_error=exc,
        )

    spread_threshold_selection = _spread_threshold_selection(
        target,
        thresholds=spread_rule.thresholds,
        thresholds_by_asset_class=spread_rule.thresholds_by_asset_class,
    )
    microstructure_threshold_selection = _microstructure_threshold_selection(
        target,
        thresholds=microstructure_rule.thresholds,
        thresholds_by_symbol=microstructure_rule.thresholds_by_symbol,
        thresholds_by_session=microstructure_rule.thresholds_by_session,
        thresholds_by_asset_class=(
            microstructure_rule.thresholds_by_asset_class
        ),
        thresholds_by_symbol_session=(
            microstructure_rule.thresholds_by_symbol_session
        ),
        session_name=microstructure_rule.session_name,
    )
    regime_threshold_selection = _spread_regime_threshold_selection(
        target,
        thresholds=regime_rule.thresholds,
        thresholds_by_asset_class=regime_rule.thresholds_by_asset_class,
    )

    try:
        scans = _scan_tick_cache_quality_rows(
            frame,
            target=target,
            source_member="",
            spread_thresholds=spread_threshold_selection.thresholds,
            microstructure_thresholds=(
                microstructure_threshold_selection.thresholds
            ),
            regime_thresholds=regime_threshold_selection.thresholds,
        )
    except _SourceReadError as exc:
        return _tick_bundle_source_error_results(
            target,
            spread_rule=spread_rule,
            microstructure_rule=microstructure_rule,
            regime_rule=regime_rule,
            source_error=exc,
        )

    return (
        QualityRuleResult(
            rule_id=spread_rule.rule_id,
            target=target,
            findings=_spread_findings(
                target=target,
                scan=scans.spread,
                source_member=scans.source_member,
                threshold_selection=spread_threshold_selection,
                zero_spread_severity=spread_rule.zero_spread_severity,
                negative_spread_severity=spread_rule.negative_spread_severity,
                schema_severity=spread_rule.schema_severity,
                rule_id=spread_rule.rule_id,
            ),
        ),
        QualityRuleResult(
            rule_id=microstructure_rule.rule_id,
            target=target,
            findings=_microstructure_findings(
                target=target,
                scan=scans.microstructure,
                source_member=scans.source_member,
                threshold_selection=microstructure_threshold_selection,
                severity=microstructure_rule.warning_severity,
                rule_id=microstructure_rule.rule_id,
            ),
        ),
        QualityRuleResult(
            rule_id=regime_rule.rule_id,
            target=target,
            findings=_spread_regime_findings(
                target=target,
                scan=scans.spread_regime,
                source_member=scans.source_member,
                threshold_selection=regime_threshold_selection,
                severity=regime_rule.warning_severity,
                rule_id=regime_rule.rule_id,
            ),
        ),
    )


def _tick_bundle_source_error_results(
    target: QualityTarget,
    *,
    spread_rule: HistDataAsciiTickSpreadRule,
    microstructure_rule: HistDataAsciiTickMicrostructureRule,
    regime_rule: HistDataAsciiTickSpreadRegimeRule,
    source_error: _SourceReadError,
) -> tuple[QualityRuleResult, ...]:
    return (
        QualityRuleResult(
            rule_id=spread_rule.rule_id,
            target=target,
            findings=(
                _finding(
                    target,
                    code=source_error.code,
                    message=source_error.message,
                    rule_id=spread_rule.rule_id,
                    metadata=source_error.metadata,
                ),
            ),
        ),
        QualityRuleResult(
            rule_id=microstructure_rule.rule_id,
            target=target,
            findings=(
                _finding(
                    target,
                    code=source_error.code.replace(
                        "SPREAD",
                        "MICROSTRUCTURE",
                    ),
                    message=source_error.message.replace(
                        "spread checks",
                        "microstructure checks",
                    ),
                    rule_id=microstructure_rule.rule_id,
                    metadata=source_error.metadata,
                ),
            ),
        ),
        QualityRuleResult(
            rule_id=regime_rule.rule_id,
            target=target,
            findings=(
                _finding(
                    target,
                    code=source_error.code.replace(
                        "SPREAD",
                        "SPREAD_REGIME",
                    ),
                    message=source_error.message.replace(
                        "spread checks",
                        "spread-regime checks",
                    ),
                    severity=regime_rule.schema_severity,
                    rule_id=regime_rule.rule_id,
                    metadata=source_error.metadata,
                ),
            ),
        ),
    )


def _is_tick_ascii_text_target(target: QualityTarget) -> bool:
    return (
        target.data_format == "ascii"
        and target.timeframe == TICK
        and target.kind
        in {
            QualityTargetKind.CSV,
            QualityTargetKind.ZIP,
            QualityTargetKind.CACHE,
        }
    )


def _read_text_payload(target: QualityTarget) -> _TextPayload:
    path = Path(target.path)
    if target.kind is QualityTargetKind.CSV:
        try:
            return _TextPayload(path.read_bytes())
        except OSError as exc:
            raise _source_error(
                "ASCII_TICK_SPREAD_SOURCE_UNREADABLE",
                "ASCII file could not be read for tick spread checks.",
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
                    code="ASCII_TICK_SPREAD_ZIP_MEMBER_UNAVAILABLE",
                    message="ZIP archive must contain exactly one CSV member "
                    "for tick spread checks.",
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
            "ASCII_TICK_SPREAD_ZIP_UNREADABLE",
            "ZIP archive could not be opened for tick spread checks.",
            exc,
        ) from exc
    except (KeyError, OSError) as exc:
        raise _source_error(
            "ASCII_TICK_SPREAD_SOURCE_UNREADABLE",
            "ASCII source could not be read for tick spread checks.",
            exc,
        ) from exc


def _read_tick_line_source(
    target: QualityTarget,
    *,
    columns: tuple[str, ...],
) -> _TickLineSource:
    payload = _read_text_payload(target)
    try:
        text = payload.data.decode("utf-8")
    except UnicodeDecodeError:
        raise
    return _TickLineSource(
        iter_lines=text.splitlines,
        source_member=payload.source_member,
    )


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


def _cache_tick_quality_scans(
    target: QualityTarget,
    *,
    spread_thresholds: HistDataTickSpreadThresholds,
    microstructure_thresholds: HistDataTickMicrostructureThresholds,
    regime_thresholds: HistDataTickSpreadRegimeThresholds,
) -> _TickQualityScans:
    columns = columns_for_timeframe(TICK)
    frame = _read_tick_cache_frame(target, columns=columns)
    return _scan_tick_cache_quality_rows(
        frame,
        target=target,
        source_member="",
        spread_thresholds=spread_thresholds,
        microstructure_thresholds=microstructure_thresholds,
        regime_thresholds=regime_thresholds,
    )


def _read_tick_cache_frame(
    target: QualityTarget,
    *,
    columns: tuple[str, ...],
) -> Any:
    cache = read_quality_polars_cache(
        target,
        required_columns=columns,
    )
    if cache is None:
        raise _SourceReadError(
            code="ASCII_TICK_SPREAD_CACHE_SCHEMA_UNSUPPORTED",
            message="Polars cache is missing columns required for tick "
            "spread checks.",
            metadata={"required_columns": list(columns)},
        )

    try:
        return cache.frame.select(list(columns))
    except Exception as exc:
        raise _SourceReadError(
            code="ASCII_TICK_SPREAD_CACHE_SCHEMA_UNSUPPORTED",
            message="Polars cache could not be projected for tick spread "
            "checks.",
            metadata={
                "required_columns": list(columns),
                "error_type": type(exc).__name__,
                "error": str(exc),
            },
        ) from exc


def _cache_tick_timestamp_source(value: object) -> str:
    if isinstance(value, bool) or not isinstance(value, int):
        return ""
    utc_ms = value
    source_ms = utc_ms - EST_NO_DST_OFFSET_MS
    seconds, milliseconds = divmod(source_ms, 1000)
    try:
        source_dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
    except (OSError, OverflowError, ValueError):
        return ""
    return f"{source_dt:%Y%m%d %H%M%S}{milliseconds:03d}"


def _cache_cell(value: object) -> str:
    return "" if value is None else str(value)


def _scan_tick_cache_quality_rows(
    frame: Any,
    *,
    target: QualityTarget,
    source_member: str,
    spread_thresholds: HistDataTickSpreadThresholds,
    microstructure_thresholds: HistDataTickMicrostructureThresholds,
    regime_thresholds: HistDataTickSpreadRegimeThresholds,
) -> _TickQualityScans:
    spread_scan = _TickSpreadScan()
    microstructure_scan = _TickMicrostructureScan()
    regime_scan = _TickSpreadRegimeScan()
    try:
        duplicate_count, duplicate_rows = _cache_duplicate_summary(
            frame,
            source_member=source_member,
        )
        rows = frame.iter_rows(named=False)
    except Exception as exc:
        raise _SourceReadError(
            code="ASCII_TICK_SPREAD_CACHE_SCHEMA_UNSUPPORTED",
            message="Polars cache could not be projected for tick spread "
            "checks.",
            metadata={
                "required_columns": list(columns_for_timeframe(TICK)),
                "error_type": type(exc).__name__,
                "error": str(exc),
            },
        ) from exc

    microstructure_scan.duplicate_row_count = duplicate_count
    microstructure_scan.duplicate_rows.extend(duplicate_rows)
    zero_run_start: _TickSpreadSample | None = None
    zero_run_end: _TickSpreadSample | None = None
    zero_run_length = 0

    previous: _TickSpreadSample | None = None
    stale_start: _TickSpreadSample | None = None
    stale_end: _TickSpreadSample | None = None
    stale_length = 0
    burst_start: _TickSpreadSample | None = None
    burst_end: _TickSpreadSample | None = None
    burst_length = 0
    one_sided_start: _TickSpreadSample | None = None
    one_sided_end: _TickSpreadSample | None = None
    one_sided_previous: _TickSpreadSample | None = None
    one_sided_length = 0
    one_sided_direction = ""

    symbol_key = normalize_histdata_symbol(target.symbol)
    previous_regime_sample: _TickSpreadRegimeSample | None = None

    for row_number, row in enumerate(rows, start=1):
        datetime_value, bid_value, ask_value, volume_value = row
        spread_scan.row_count += 1
        microstructure_scan.row_count += 1
        regime_scan.row_count += 1

        missing_column = _cache_missing_bid_ask_column(
            bid_value,
            ask_value,
        )
        if missing_column:
            _finalize_zero_spread_run(
                spread_scan,
                zero_run_start,
                zero_run_end,
                zero_run_length,
                spread_thresholds,
            )
            zero_run_start = None
            zero_run_end = None
            zero_run_length = 0
            spread_scan.missing_bid_ask_count += 1
            microstructure_scan.invalid_tick_count += 1
            _append_spread_sample(
                spread_scan.missing_bid_ask,
                _cache_sample_from_row(
                    datetime_value,
                    bid_value,
                    ask_value,
                    volume_value,
                    row_number=row_number,
                    column=missing_column,
                    source_member=source_member,
                    metadata={
                        "expected_field_count": len(
                            columns_for_timeframe(TICK)
                        ),
                        "field_count": len(columns_for_timeframe(TICK)),
                        "required_columns": list(TICK_PRICE_COLUMNS),
                    },
                ),
            )
            (
                stale_start,
                stale_end,
                stale_length,
                burst_start,
                burst_end,
                burst_length,
                one_sided_start,
                one_sided_end,
                one_sided_previous,
                one_sided_length,
                one_sided_direction,
            ) = _finalize_microstructure_runs(
                microstructure_scan,
                stale_start,
                stale_end,
                stale_length,
                burst_start,
                burst_end,
                burst_length,
                one_sided_start,
                one_sided_end,
                one_sided_previous,
                one_sided_length,
                one_sided_direction,
                microstructure_thresholds,
            )
            previous = None
            regime_scan.invalid_tick_count += 1
            continue

        parsed = _cache_parsed_tick_spread_sample(
            datetime_value,
            bid_value,
            ask_value,
            volume_value,
            row_number=row_number,
            source_member=source_member,
        )
        if parsed is None:
            _finalize_zero_spread_run(
                spread_scan,
                zero_run_start,
                zero_run_end,
                zero_run_length,
                spread_thresholds,
            )
            zero_run_start = None
            zero_run_end = None
            zero_run_length = 0
            spread_scan.invalid_bid_ask_count += 1
            microstructure_scan.invalid_tick_count += 1
            regime_scan.invalid_tick_count += 1
            _append_spread_sample(
                spread_scan.invalid_bid_ask,
                _cache_invalid_bid_ask_sample(
                    datetime_value,
                    bid_value,
                    ask_value,
                    volume_value,
                    row_number=row_number,
                    source_member=source_member,
                ),
            )
            (
                stale_start,
                stale_end,
                stale_length,
                burst_start,
                burst_end,
                burst_length,
                one_sided_start,
                one_sided_end,
                one_sided_previous,
                one_sided_length,
                one_sided_direction,
            ) = _finalize_microstructure_runs(
                microstructure_scan,
                stale_start,
                stale_end,
                stale_length,
                burst_start,
                burst_end,
                burst_length,
                one_sided_start,
                one_sided_end,
                one_sided_previous,
                one_sided_length,
                one_sided_direction,
                microstructure_thresholds,
            )
            previous = None
            continue

        _record_cache_spread_row(
            spread_scan,
            parsed,
        )
        (
            zero_run_start,
            zero_run_end,
            zero_run_length,
        ) = _next_zero_spread_run_state(
            spread_scan,
            parsed,
            thresholds=spread_thresholds,
            zero_run_start=zero_run_start,
            zero_run_end=zero_run_end,
            zero_run_length=zero_run_length,
        )
        (
            previous,
            stale_start,
            stale_end,
            stale_length,
            burst_start,
            burst_end,
            burst_length,
            one_sided_start,
            one_sided_end,
            one_sided_previous,
            one_sided_length,
            one_sided_direction,
        ) = _next_microstructure_state(
            microstructure_scan,
            parsed,
            previous=previous,
            stale_start=stale_start,
            stale_end=stale_end,
            stale_length=stale_length,
            burst_start=burst_start,
            burst_end=burst_end,
            burst_length=burst_length,
            one_sided_start=one_sided_start,
            one_sided_end=one_sided_end,
            one_sided_previous=one_sided_previous,
            one_sided_length=one_sided_length,
            one_sided_direction=one_sided_direction,
            thresholds=microstructure_thresholds,
        )
        previous_regime_sample = _record_cache_spread_regime_row(
            regime_scan,
            target=target,
            parsed=parsed,
            previous=previous_regime_sample,
            symbol_key=symbol_key,
        )

    _finalize_zero_spread_run(
        spread_scan,
        zero_run_start,
        zero_run_end,
        zero_run_length,
        spread_thresholds,
    )
    _finalize_microstructure_runs(
        microstructure_scan,
        stale_start,
        stale_end,
        stale_length,
        burst_start,
        burst_end,
        burst_length,
        one_sided_start,
        one_sided_end,
        one_sided_previous,
        one_sided_length,
        one_sided_direction,
        microstructure_thresholds,
    )
    _record_cache_spread_regime_warnings(
        regime_scan,
        thresholds=regime_thresholds,
    )
    return _TickQualityScans(
        spread=spread_scan,
        microstructure=microstructure_scan,
        spread_regime=regime_scan,
        source_member=source_member,
    )


def _record_cache_spread_row(
    scan: _TickSpreadScan,
    parsed: _TickSpreadSample,
) -> None:
    scan.parsed_row_count += 1
    spread = parsed.spread
    if spread is None:
        return
    scan.min_spread = (
        spread if scan.min_spread is None else min(scan.min_spread, spread)
    )
    scan.max_spread = (
        spread if scan.max_spread is None else max(scan.max_spread, spread)
    )


def _next_zero_spread_run_state(
    scan: _TickSpreadScan,
    parsed: _TickSpreadSample,
    *,
    thresholds: HistDataTickSpreadThresholds,
    zero_run_start: _TickSpreadSample | None,
    zero_run_end: _TickSpreadSample | None,
    zero_run_length: int,
) -> tuple[_TickSpreadSample | None, _TickSpreadSample | None, int]:
    spread = parsed.spread
    if spread is not None and spread < 0.0:
        _finalize_zero_spread_run(
            scan,
            zero_run_start,
            zero_run_end,
            zero_run_length,
            thresholds,
        )
        scan.negative_spread_count += 1
        _append_spread_sample(scan.negative_spreads, parsed)
        return None, None, 0
    if spread == 0.0:
        scan.zero_spread_count += 1
        return (
            parsed if zero_run_start is None else zero_run_start,
            parsed,
            zero_run_length + 1,
        )

    _finalize_zero_spread_run(
        scan,
        zero_run_start,
        zero_run_end,
        zero_run_length,
        thresholds,
    )
    return None, None, 0


def _next_microstructure_state(
    scan: _TickMicrostructureScan,
    parsed: _TickSpreadSample,
    *,
    previous: _TickSpreadSample | None,
    stale_start: _TickSpreadSample | None,
    stale_end: _TickSpreadSample | None,
    stale_length: int,
    burst_start: _TickSpreadSample | None,
    burst_end: _TickSpreadSample | None,
    burst_length: int,
    one_sided_start: _TickSpreadSample | None,
    one_sided_end: _TickSpreadSample | None,
    one_sided_previous: _TickSpreadSample | None,
    one_sided_length: int,
    one_sided_direction: str,
    thresholds: HistDataTickMicrostructureThresholds,
) -> tuple[
    _TickSpreadSample,
    _TickSpreadSample | None,
    _TickSpreadSample | None,
    int,
    _TickSpreadSample | None,
    _TickSpreadSample | None,
    int,
    _TickSpreadSample | None,
    _TickSpreadSample | None,
    _TickSpreadSample | None,
    int,
    str,
]:
    scan.parsed_row_count += 1
    if previous is None:
        return (
            parsed,
            stale_start,
            stale_end,
            stale_length,
            burst_start,
            burst_end,
            burst_length,
            one_sided_start,
            one_sided_end,
            one_sided_previous,
            one_sided_length,
            one_sided_direction,
        )

    interval_ms = _tick_interval_ms(previous, parsed)
    if _is_stale_quote_pair(previous, parsed, interval_ms, thresholds):
        scan.stale_quote_repeat_count += 1
        if stale_start is None:
            stale_start = previous
            stale_length = 2
        else:
            stale_length += 1
        stale_end = parsed
    else:
        _finalize_stale_quote_run(
            scan,
            stale_start,
            stale_end,
            stale_length,
            thresholds,
        )
        stale_start = None
        stale_end = None
        stale_length = 0

    if _is_burst_interval(interval_ms, thresholds):
        scan.burst_interval_count += 1
        if burst_start is None:
            burst_start = previous
            burst_length = 2
        else:
            burst_length += 1
        burst_end = parsed
    else:
        _finalize_burst_run(
            scan,
            burst_start,
            burst_end,
            burst_length,
            thresholds,
        )
        burst_start = None
        burst_end = None
        burst_length = 0

    direction = _one_sided_quote_direction(previous, parsed)
    if direction:
        scan.one_sided_movement_count += 1
        if direction == "bid_only":
            scan.bid_only_movement_count += 1
        else:
            scan.ask_only_movement_count += 1

        if one_sided_start is not None and (one_sided_direction == direction):
            one_sided_length += 1
            one_sided_end = parsed
        else:
            _finalize_one_sided_run(
                scan,
                one_sided_start,
                one_sided_end,
                one_sided_previous,
                one_sided_length,
                one_sided_direction,
                thresholds,
            )
            one_sided_start = parsed
            one_sided_end = parsed
            one_sided_previous = previous
            one_sided_length = 1
            one_sided_direction = direction
    else:
        _finalize_one_sided_run(
            scan,
            one_sided_start,
            one_sided_end,
            one_sided_previous,
            one_sided_length,
            one_sided_direction,
            thresholds,
        )
        one_sided_start = None
        one_sided_end = None
        one_sided_previous = None
        one_sided_length = 0
        one_sided_direction = ""

    return (
        parsed,
        stale_start,
        stale_end,
        stale_length,
        burst_start,
        burst_end,
        burst_length,
        one_sided_start,
        one_sided_end,
        one_sided_previous,
        one_sided_length,
        one_sided_direction,
    )


def _record_cache_spread_regime_row(
    scan: _TickSpreadRegimeScan,
    *,
    target: QualityTarget,
    parsed: _TickSpreadSample,
    previous: _TickSpreadRegimeSample | None,
    symbol_key: str,
) -> _TickSpreadRegimeSample | None:
    if parsed.spread is None:
        scan.invalid_tick_count += 1
        return previous
    scan.parsed_row_count += 1
    if parsed.timestamp_utc_ms is None:
        scan.invalid_timestamp_count += 1
        return previous
    if parsed.spread < 0.0:
        scan.negative_spread_count += 1
        return previous

    sample = _tick_spread_regime_sample(
        target,
        parsed,
        symbol_key=symbol_key,
    )
    scan.profiled_row_count += 1
    _record_spread_regime_sample(scan, sample)
    _record_spread_regime_warning_candidates(scan, sample, previous)
    return sample


def _record_spread_regime_warning_candidates(
    scan: _TickSpreadRegimeScan,
    sample: _TickSpreadRegimeSample,
    previous: _TickSpreadRegimeSample | None,
) -> None:
    if sample.spread is not None:
        _record_top_regime_candidate(
            scan.wide_candidates,
            sample,
            score=sample.spread,
        )
    if previous is None or previous.spread is None or sample.spread is None:
        return
    spread_delta = sample.spread - previous.spread
    scan.spread_jump_values.append(abs(spread_delta))
    _record_top_regime_candidate(
        scan.spread_jump_candidates,
        _TickSpreadRegimeSample(
            sample=sample.sample,
            symbol_key=sample.symbol_key,
            source_hour=sample.source_hour,
            utc_hour=sample.utc_hour,
            session_state=sample.session_state,
            session_keys=sample.session_keys,
            special_regime_keys=sample.special_regime_keys,
            previous=previous.sample,
            spread_delta=spread_delta,
        ),
        score=abs(spread_delta),
    )


def _record_top_regime_candidate(
    candidates: list[_TickSpreadRegimeSample],
    sample: _TickSpreadRegimeSample,
    *,
    score: float,
) -> None:
    if len(candidates) < MAX_TICK_SAMPLES:
        candidates.append(sample)
        return
    min_index, min_score = min(
        enumerate(
            _regime_candidate_score(candidate) for candidate in candidates
        ),
        key=lambda item: item[1],
    )
    if score > min_score:
        candidates[min_index] = sample


def _regime_candidate_score(sample: _TickSpreadRegimeSample) -> float:
    if sample.spread_delta is not None:
        return abs(sample.spread_delta)
    return sample.spread or 0.0


def _record_cache_spread_regime_warnings(
    scan: _TickSpreadRegimeScan,
    *,
    thresholds: HistDataTickSpreadRegimeThresholds,
) -> None:
    baseline_profile, _baseline_source = _spread_regime_baseline(scan)
    baseline_median = _median_or_none(baseline_profile.values) or 0.0
    wide_threshold = _wide_spread_threshold(
        baseline_median,
        thresholds,
    )
    jump_threshold = _spread_jump_threshold(
        baseline_median,
        thresholds,
    )

    if wide_threshold is not None:
        scan.wide_spread_count = sum(
            1
            for spread in scan.global_profile.values
            if spread > wide_threshold
        )
        for sample in scan.wide_candidates:
            if sample.spread is not None and sample.spread > wide_threshold:
                _append_spread_regime_sample(
                    scan.wide_spreads,
                    _regime_warning_sample(
                        sample,
                        threshold=wide_threshold,
                        profile_key="wide_spread",
                    ),
                )

    if jump_threshold is not None:
        scan.spread_jump_count = sum(
            1 for value in scan.spread_jump_values if value > jump_threshold
        )
        for sample in scan.spread_jump_candidates:
            if (
                sample.spread_delta is not None
                and abs(sample.spread_delta) > jump_threshold
            ):
                _append_spread_regime_sample(
                    scan.spread_jumps,
                    _regime_warning_sample(
                        sample,
                        previous=sample.previous,
                        spread_delta=sample.spread_delta,
                        threshold=jump_threshold,
                        profile_key="spread_jump",
                    ),
                )

    regime_threshold = _regime_median_threshold(
        baseline_median,
        thresholds,
    )
    if regime_threshold is None:
        return
    for regime in SPECIAL_SPREAD_REGIMES:
        profile = scan.special_regime_profiles.get(regime)
        if profile is None or not profile.values:
            continue
        regime_median = _median_or_none(profile.values)
        if regime_median is None or regime_median <= regime_threshold:
            continue
        scan.regime_shift_count += 1
        if profile.max_sample is not None:
            _append_spread_regime_sample(
                scan.regime_shifts,
                _regime_warning_sample(
                    profile.max_sample,
                    threshold=regime_threshold,
                    profile_key=regime,
                ),
            )


def _cache_duplicate_summary(
    frame: Any,
    *,
    source_member: str,
) -> tuple[int, list[_TickSpreadSample]]:
    import polars as pl

    valid_frame = frame.filter(
        pl.col("bid").is_not_null()
        & pl.col("ask").is_not_null()
        & pl.col("bid").is_finite()
        & pl.col("ask").is_finite()
    )
    duplicate_count = max(valid_frame.height - valid_frame.unique().height, 0)
    if not duplicate_count:
        return 0, []

    samples: list[_TickSpreadSample] = []
    seen_rows: dict[tuple[object, ...], _TickSpreadSample] = {}
    for row_number, row in enumerate(frame.iter_rows(named=False), start=1):
        datetime_value, bid_value, ask_value, volume_value = row
        if _cache_missing_bid_ask_column(bid_value, ask_value):
            continue
        parsed = _cache_parsed_tick_spread_sample(
            datetime_value,
            bid_value,
            ask_value,
            volume_value,
            row_number=row_number,
            source_member=source_member,
        )
        if parsed is None:
            continue
        key = (datetime_value, bid_value, ask_value, volume_value)
        duplicate = seen_rows.get(key)
        if duplicate is not None:
            _append_spread_sample(
                samples,
                _duplicate_microstructure_sample(parsed, duplicate),
            )
            if len(samples) >= MAX_TICK_SAMPLES:
                break
        else:
            seen_rows[key] = parsed
    return duplicate_count, samples


def _cache_missing_bid_ask_column(bid: object, ask: object) -> str:
    if bid is None:
        return "bid"
    if ask is None:
        return "ask"
    return ""


def _cache_parsed_tick_spread_sample(
    datetime_value: object,
    bid_value: object,
    ask_value: object,
    volume_value: object,
    *,
    row_number: int,
    source_member: str,
    materialize_timestamp: bool = False,
) -> _TickSpreadSample | None:
    bid = _cache_float_or_none(bid_value)
    ask = _cache_float_or_none(ask_value)
    if bid is None or ask is None:
        return None

    timestamp_utc_ms = _cache_timestamp_utc_ms_or_none(datetime_value)
    timestamp_source = (
        _cache_tick_timestamp_source(datetime_value)
        if materialize_timestamp
        else ""
    )
    spread = ask - bid
    return _TickSpreadSample(
        row_number=row_number,
        timestamp_source=timestamp_source,
        timestamp_utc_ms=timestamp_utc_ms,
        column="ask" if spread < 0.0 else "spread",
        bid=bid,
        ask=ask,
        spread=spread,
        raw_values=(
            _cache_raw_values(
                timestamp_source,
                bid_value,
                ask_value,
                volume_value,
            )
            if materialize_timestamp
            else ()
        ),
        source_member=source_member,
    )


def _cache_sample_from_row(
    datetime_value: object,
    bid_value: object,
    ask_value: object,
    volume_value: object,
    *,
    row_number: int,
    column: str,
    source_member: str,
    metadata: dict[str, JSONValue] | None = None,
) -> _TickSpreadSample:
    timestamp_source = _cache_tick_timestamp_source(datetime_value)
    bid = _cache_float_or_none(bid_value)
    ask = _cache_float_or_none(ask_value)
    return _TickSpreadSample(
        row_number=row_number,
        timestamp_source=timestamp_source,
        timestamp_utc_ms=_cache_timestamp_utc_ms_or_none(datetime_value),
        column=column,
        bid=bid,
        ask=ask,
        spread=ask - bid if bid is not None and ask is not None else None,
        raw_values=_cache_raw_values(
            timestamp_source,
            bid_value,
            ask_value,
            volume_value,
        ),
        source_member=source_member,
        metadata=dict(metadata or {}),
    )


def _cache_invalid_bid_ask_sample(
    datetime_value: object,
    bid_value: object,
    ask_value: object,
    volume_value: object,
    *,
    row_number: int,
    source_member: str,
) -> _TickSpreadSample:
    bid_raw = _cache_cell(bid_value)
    ask_raw = _cache_cell(ask_value)
    column = "bid" if _cache_float_or_none(bid_value) is None else "ask"
    return _cache_sample_from_row(
        datetime_value,
        bid_value,
        ask_value,
        volume_value,
        row_number=row_number,
        column=column,
        source_member=source_member,
        metadata={
            "raw_bid": bid_raw,
            "raw_ask": ask_raw,
            "error": "bid and ask must parse as finite decimal numbers",
        },
    )


def _cache_timestamp_utc_ms_or_none(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _cache_float_or_none(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(cast(Any, value))
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _cache_raw_values(
    timestamp_source: str,
    bid_value: object,
    ask_value: object,
    volume_value: object,
) -> tuple[str, ...]:
    return (
        timestamp_source,
        _cache_cell(bid_value),
        _cache_cell(ask_value),
        _cache_cell(volume_value),
    )


def _materialize_tick_sample(
    sample: _TickSpreadSample,
) -> _TickSpreadSample:
    if sample.timestamp_source and (
        not sample.raw_values or sample.raw_values[0]
    ):
        return sample
    timestamp_source = sample.timestamp_source
    if not timestamp_source and sample.timestamp_utc_ms is not None:
        timestamp_source = _cache_tick_timestamp_source(sample.timestamp_utc_ms)
    raw_values = sample.raw_values
    if raw_values and not raw_values[0] and timestamp_source:
        raw_values = (timestamp_source, *raw_values[1:])
    elif not raw_values:
        raw_values = (
            timestamp_source,
            _cache_cell(sample.bid),
            _cache_cell(sample.ask),
            "",
        )
    return _TickSpreadSample(
        row_number=sample.row_number,
        timestamp_source=timestamp_source,
        timestamp_utc_ms=sample.timestamp_utc_ms,
        column=sample.column,
        bid=sample.bid,
        ask=sample.ask,
        spread=sample.spread,
        raw_values=raw_values,
        source_member=sample.source_member,
        metadata=dict(sample.metadata),
    )


def _materialize_zero_spread_run(
    sample: _ZeroSpreadRunSample,
) -> _ZeroSpreadRunSample:
    return _ZeroSpreadRunSample(
        start=_materialize_tick_sample(sample.start),
        end=_materialize_tick_sample(sample.end),
        run_length=sample.run_length,
    )


def _materialize_microstructure_run(
    sample: _TickMicrostructureRunSample,
) -> _TickMicrostructureRunSample:
    return _TickMicrostructureRunSample(
        start=_materialize_tick_sample(sample.start),
        end=_materialize_tick_sample(sample.end),
        run_length=sample.run_length,
        metric=sample.metric,
        direction=sample.direction,
        metadata=dict(sample.metadata),
    )


def _materialize_spread_regime_sample(
    sample: _TickSpreadRegimeSample,
) -> _TickSpreadRegimeSample:
    return _TickSpreadRegimeSample(
        sample=_materialize_tick_sample(sample.sample),
        symbol_key=sample.symbol_key,
        source_hour=sample.source_hour,
        utc_hour=sample.utc_hour,
        session_state=sample.session_state,
        session_keys=sample.session_keys,
        special_regime_keys=sample.special_regime_keys,
        previous=(
            None
            if sample.previous is None
            else _materialize_tick_sample(sample.previous)
        ),
        spread_delta=sample.spread_delta,
        threshold=sample.threshold,
        profile_key=sample.profile_key,
    )


def _scan_tick_spread_rows(
    lines: Iterable[str],
    *,
    target: QualityTarget,
    delimiter: str,
    columns: tuple[str, ...],
    source_member: str,
    thresholds: HistDataTickSpreadThresholds,
) -> _TickSpreadScan:
    scan = _TickSpreadScan()
    expected_count = len(columns)
    zero_run_start: _TickSpreadSample | None = None
    zero_run_end: _TickSpreadSample | None = None
    zero_run_length = 0

    for row_number, raw in enumerate(lines, start=1):
        if not raw.strip():
            continue
        scan.row_count += 1
        row = _parse_row(raw, delimiter)
        if tuple(row) == columns:
            continue

        missing_column = _missing_bid_ask_column(row, expected_count)
        if missing_column:
            _finalize_zero_spread_run(
                scan,
                zero_run_start,
                zero_run_end,
                zero_run_length,
                thresholds,
            )
            zero_run_start = None
            zero_run_end = None
            zero_run_length = 0
            scan.missing_bid_ask_count += 1
            _append_spread_sample(
                scan.missing_bid_ask,
                _sample_from_row(
                    row,
                    row_number=row_number,
                    column=missing_column,
                    source_member=source_member,
                    metadata={
                        "expected_field_count": expected_count,
                        "field_count": len(row),
                        "required_columns": list(TICK_PRICE_COLUMNS),
                    },
                ),
            )
            continue

        parsed = _parsed_tick_spread_sample(
            row,
            row_number=row_number,
            source_member=source_member,
        )
        if parsed is None:
            _finalize_zero_spread_run(
                scan,
                zero_run_start,
                zero_run_end,
                zero_run_length,
                thresholds,
            )
            zero_run_start = None
            zero_run_end = None
            zero_run_length = 0
            scan.invalid_bid_ask_count += 1
            _append_spread_sample(
                scan.invalid_bid_ask,
                _invalid_bid_ask_sample(
                    row,
                    row_number=row_number,
                    source_member=source_member,
                ),
            )
            continue

        scan.parsed_row_count += 1
        spread = parsed.spread
        if spread is not None:
            scan.min_spread = (
                spread
                if scan.min_spread is None
                else min(scan.min_spread, spread)
            )
            scan.max_spread = (
                spread
                if scan.max_spread is None
                else max(scan.max_spread, spread)
            )

        if spread is not None and spread < 0.0:
            _finalize_zero_spread_run(
                scan,
                zero_run_start,
                zero_run_end,
                zero_run_length,
                thresholds,
            )
            zero_run_start = None
            zero_run_end = None
            zero_run_length = 0
            scan.negative_spread_count += 1
            _append_spread_sample(scan.negative_spreads, parsed)
        elif spread == 0.0:
            scan.zero_spread_count += 1
            if zero_run_start is None:
                zero_run_start = parsed
            zero_run_end = parsed
            zero_run_length += 1
        else:
            _finalize_zero_spread_run(
                scan,
                zero_run_start,
                zero_run_end,
                zero_run_length,
                thresholds,
            )
            zero_run_start = None
            zero_run_end = None
            zero_run_length = 0

    _finalize_zero_spread_run(
        scan,
        zero_run_start,
        zero_run_end,
        zero_run_length,
        thresholds,
    )
    return scan


def _scan_tick_microstructure_rows(
    lines: Iterable[str],
    *,
    delimiter: str,
    columns: tuple[str, ...],
    source_member: str,
    thresholds: HistDataTickMicrostructureThresholds,
) -> _TickMicrostructureScan:
    scan = _TickMicrostructureScan()
    expected_count = len(columns)
    previous: _TickSpreadSample | None = None
    seen_rows: dict[tuple[str, ...], _TickSpreadSample] = {}

    stale_start: _TickSpreadSample | None = None
    stale_end: _TickSpreadSample | None = None
    stale_length = 0

    burst_start: _TickSpreadSample | None = None
    burst_end: _TickSpreadSample | None = None
    burst_length = 0

    one_sided_start: _TickSpreadSample | None = None
    one_sided_end: _TickSpreadSample | None = None
    one_sided_previous: _TickSpreadSample | None = None
    one_sided_length = 0
    one_sided_direction = ""

    for row_number, raw in enumerate(lines, start=1):
        if not raw.strip():
            continue
        scan.row_count += 1
        row = _parse_row(raw, delimiter)
        if tuple(row) == columns:
            continue

        parsed = (
            None
            if _missing_bid_ask_column(row, expected_count)
            else _parsed_tick_spread_sample(
                row,
                row_number=row_number,
                source_member=source_member,
            )
        )
        if parsed is None:
            (
                stale_start,
                stale_end,
                stale_length,
                burst_start,
                burst_end,
                burst_length,
                one_sided_start,
                one_sided_end,
                one_sided_previous,
                one_sided_length,
                one_sided_direction,
            ) = _finalize_microstructure_runs(
                scan,
                stale_start,
                stale_end,
                stale_length,
                burst_start,
                burst_end,
                burst_length,
                one_sided_start,
                one_sided_end,
                one_sided_previous,
                one_sided_length,
                one_sided_direction,
                thresholds,
            )
            scan.invalid_tick_count += 1
            previous = None
            continue

        scan.parsed_row_count += 1
        duplicate = seen_rows.get(parsed.raw_values)
        if duplicate is not None:
            scan.duplicate_row_count += 1
            _append_spread_sample(
                scan.duplicate_rows,
                _duplicate_microstructure_sample(parsed, duplicate),
            )
        else:
            seen_rows[parsed.raw_values] = parsed

        if previous is None:
            previous = parsed
            continue

        interval_ms = _tick_interval_ms(previous, parsed)
        if _is_stale_quote_pair(previous, parsed, interval_ms, thresholds):
            scan.stale_quote_repeat_count += 1
            if stale_start is None:
                stale_start = previous
                stale_length = 2
            else:
                stale_length += 1
            stale_end = parsed
        else:
            _finalize_stale_quote_run(
                scan,
                stale_start,
                stale_end,
                stale_length,
                thresholds,
            )
            stale_start = None
            stale_end = None
            stale_length = 0

        if _is_burst_interval(interval_ms, thresholds):
            scan.burst_interval_count += 1
            if burst_start is None:
                burst_start = previous
                burst_length = 2
            else:
                burst_length += 1
            burst_end = parsed
        else:
            _finalize_burst_run(
                scan,
                burst_start,
                burst_end,
                burst_length,
                thresholds,
            )
            burst_start = None
            burst_end = None
            burst_length = 0

        direction = _one_sided_quote_direction(previous, parsed)
        if direction:
            scan.one_sided_movement_count += 1
            if direction == "bid_only":
                scan.bid_only_movement_count += 1
            else:
                scan.ask_only_movement_count += 1

            if one_sided_start is not None and (
                one_sided_direction == direction
            ):
                one_sided_length += 1
                one_sided_end = parsed
            else:
                _finalize_one_sided_run(
                    scan,
                    one_sided_start,
                    one_sided_end,
                    one_sided_previous,
                    one_sided_length,
                    one_sided_direction,
                    thresholds,
                )
                one_sided_start = parsed
                one_sided_end = parsed
                one_sided_previous = previous
                one_sided_length = 1
                one_sided_direction = direction
        else:
            _finalize_one_sided_run(
                scan,
                one_sided_start,
                one_sided_end,
                one_sided_previous,
                one_sided_length,
                one_sided_direction,
                thresholds,
            )
            one_sided_start = None
            one_sided_end = None
            one_sided_previous = None
            one_sided_length = 0
            one_sided_direction = ""

        previous = parsed

    _finalize_microstructure_runs(
        scan,
        stale_start,
        stale_end,
        stale_length,
        burst_start,
        burst_end,
        burst_length,
        one_sided_start,
        one_sided_end,
        one_sided_previous,
        one_sided_length,
        one_sided_direction,
        thresholds,
    )
    return scan


def _scan_tick_spread_regime_rows(
    lines_factory: Callable[[], Iterable[str]],
    *,
    target: QualityTarget,
    delimiter: str,
    columns: tuple[str, ...],
    source_member: str,
    thresholds: HistDataTickSpreadRegimeThresholds,
) -> _TickSpreadRegimeScan:
    scan = _TickSpreadRegimeScan()
    expected_count = len(columns)
    symbol_key = normalize_histdata_symbol(target.symbol)

    for row_number, raw in enumerate(lines_factory(), start=1):
        if not raw.strip():
            continue
        scan.row_count += 1
        row = _parse_row(raw, delimiter)
        if tuple(row) == columns:
            continue

        parsed = (
            None
            if _missing_bid_ask_column(row, expected_count)
            else _parsed_tick_spread_sample(
                row,
                row_number=row_number,
                source_member=source_member,
            )
        )
        if parsed is None or parsed.spread is None:
            scan.invalid_tick_count += 1
            continue

        scan.parsed_row_count += 1
        if parsed.timestamp_utc_ms is None:
            scan.invalid_timestamp_count += 1
            continue
        if parsed.spread < 0.0:
            scan.negative_spread_count += 1
            continue

        sample = _tick_spread_regime_sample(
            target,
            parsed,
            symbol_key=symbol_key,
        )
        scan.profiled_row_count += 1
        _record_spread_regime_sample(scan, sample)

    _record_spread_regime_warnings(
        scan,
        lines_factory,
        target=target,
        delimiter=delimiter,
        columns=columns,
        source_member=source_member,
        thresholds=thresholds,
        symbol_key=symbol_key,
    )
    return scan


def _record_spread_regime_warnings(
    scan: _TickSpreadRegimeScan,
    lines_factory: Callable[[], Iterable[str]],
    *,
    target: QualityTarget,
    delimiter: str,
    columns: tuple[str, ...],
    source_member: str,
    thresholds: HistDataTickSpreadRegimeThresholds,
    symbol_key: str = "",
) -> None:
    baseline_profile, _baseline_source = _spread_regime_baseline(scan)
    baseline_median = _median_or_none(baseline_profile.values) or 0.0
    wide_threshold = _wide_spread_threshold(
        baseline_median,
        thresholds,
    )
    jump_threshold = _spread_jump_threshold(
        baseline_median,
        thresholds,
    )

    previous: _TickSpreadRegimeSample | None = None
    for sample in _iter_tick_spread_regime_samples(
        lines_factory(),
        target=target,
        delimiter=delimiter,
        columns=columns,
        source_member=source_member,
        symbol_key=symbol_key,
    ):
        if sample.spread is None:
            continue
        if wide_threshold is not None and sample.spread > wide_threshold:
            scan.wide_spread_count += 1
            _append_spread_regime_sample(
                scan.wide_spreads,
                _regime_warning_sample(
                    sample,
                    threshold=wide_threshold,
                    profile_key="wide_spread",
                ),
            )

        if (
            jump_threshold is not None
            and previous is not None
            and previous.spread is not None
            and abs(sample.spread - previous.spread) > jump_threshold
        ):
            scan.spread_jump_count += 1
            _append_spread_regime_sample(
                scan.spread_jumps,
                _regime_warning_sample(
                    sample,
                    previous=previous.sample,
                    spread_delta=sample.spread - previous.spread,
                    threshold=jump_threshold,
                    profile_key="spread_jump",
                ),
            )
        previous = sample

    regime_threshold = _regime_median_threshold(
        baseline_median,
        thresholds,
    )
    if regime_threshold is None:
        return
    for regime in SPECIAL_SPREAD_REGIMES:
        profile = scan.special_regime_profiles.get(regime)
        if profile is None or not profile.values:
            continue
        regime_median = _median_or_none(profile.values)
        if regime_median is None or regime_median <= regime_threshold:
            continue
        scan.regime_shift_count += 1
        if profile.max_sample is not None:
            _append_spread_regime_sample(
                scan.regime_shifts,
                _regime_warning_sample(
                    profile.max_sample,
                    threshold=regime_threshold,
                    profile_key=regime,
                ),
            )


def _iter_tick_spread_regime_samples(
    lines: Iterable[str],
    *,
    target: QualityTarget,
    delimiter: str,
    columns: tuple[str, ...],
    source_member: str,
    symbol_key: str = "",
) -> Iterable[_TickSpreadRegimeSample]:
    expected_count = len(columns)
    for row_number, raw in enumerate(lines, start=1):
        if not raw.strip():
            continue
        row = _parse_row(raw, delimiter)
        if tuple(row) == columns or _missing_bid_ask_column(
            row, expected_count
        ):
            continue
        parsed = _parsed_tick_spread_sample(
            row,
            row_number=row_number,
            source_member=source_member,
        )
        if (
            parsed is None
            or parsed.timestamp_utc_ms is None
            or parsed.spread is None
            or parsed.spread < 0.0
        ):
            continue
        yield _tick_spread_regime_sample(
            target,
            parsed,
            symbol_key=symbol_key,
        )


def _tick_spread_regime_sample(
    target: QualityTarget,
    parsed: _TickSpreadSample,
    *,
    symbol_key: str = "",
) -> _TickSpreadRegimeSample:
    if parsed.timestamp_utc_ms is None:
        msg = "spread-regime samples require normalized timestamps"
        raise ValueError(msg)
    projection = _tick_spread_regime_projection(parsed.timestamp_utc_ms)
    session_keys = _spread_regime_session_keys(
        projection.session_state,
        projection.active_sessions,
    )
    return _TickSpreadRegimeSample(
        sample=parsed,
        symbol_key=symbol_key or normalize_histdata_symbol(target.symbol),
        source_hour=f"{projection.source_hour:02d}",
        utc_hour=f"{projection.utc_hour:02d}",
        session_state=projection.session_state,
        session_keys=session_keys,
        special_regime_keys=projection.special_tags,
    )


@dataclass(frozen=True, slots=True)
class _TickSpreadRegimeProjection:
    source_hour: int
    utc_hour: int
    session_state: str
    active_sessions: tuple[str, ...]
    special_tags: tuple[str, ...]


def _tick_spread_regime_projection(
    timestamp_utc_ms: int,
) -> _TickSpreadRegimeProjection:
    utc = _datetime_from_utc_ms(timestamp_utc_ms)
    source = _datetime_from_utc_ms(timestamp_utc_ms - EST_NO_DST_OFFSET_MS)
    utc_minute = utc.hour * 60 + utc.minute
    source_minute = source.hour * 60 + source.minute
    session_state = _tick_session_state(
        source_weekday=source.weekday(),
        source_minute=source_minute,
    )
    clock_sessions = _tick_clock_sessions(utc_minute)
    active_sessions = (
        () if session_state == SESSION_STATE_WEEKEND_CLOSURE else clock_sessions
    )
    return _TickSpreadRegimeProjection(
        source_hour=source.hour,
        utc_hour=utc.hour,
        session_state=session_state,
        active_sessions=active_sessions,
        special_tags=_tick_special_tags(
            source=source,
            source_minute=source_minute,
            utc_minute=utc_minute,
            session_state=session_state,
        ),
    )


def _spread_regime_session_keys(
    session_state: str,
    active_sessions: tuple[str, ...],
) -> tuple[str, ...]:
    if active_sessions:
        return active_sessions
    if session_state == SESSION_STATE_MARKET_OPEN:
        return (SESSION_NO_ACTIVE_WINDOW,)
    return (SESSION_MARKET_CLOSED,)


def _datetime_from_utc_ms(timestamp_utc_ms: int) -> datetime:
    seconds, milliseconds = divmod(timestamp_utc_ms, 1000)
    return datetime.fromtimestamp(seconds, tz=timezone.utc).replace(
        microsecond=milliseconds * 1000
    )


def _tick_session_state(
    *,
    source_weekday: int,
    source_minute: int,
) -> str:
    if source_weekday == 5:
        return str(SESSION_STATE_WEEKEND_CLOSURE)
    if source_weekday == FX_FRIDAY_CLOSE_WEEKDAY:
        if _minute_in_window(source_minute, *FRIDAY_CLOSE_SOURCE_MINUTES):
            return str(SESSION_STATE_FRIDAY_CLOSE)
        if source_minute >= FX_CLOSE_OPEN_MINUTE:
            return str(SESSION_STATE_WEEKEND_CLOSURE)
    if source_weekday == FX_SUNDAY_OPEN_WEEKDAY:
        if source_minute < FX_CLOSE_OPEN_MINUTE:
            return str(SESSION_STATE_WEEKEND_CLOSURE)
        if _minute_in_window(source_minute, *SUNDAY_OPEN_SOURCE_MINUTES):
            return str(SESSION_STATE_SUNDAY_OPEN)
    return str(SESSION_STATE_MARKET_OPEN)


def _tick_clock_sessions(utc_minute: int) -> tuple[str, ...]:
    sessions: list[str] = []
    if 0 <= utc_minute < 9 * 60:
        sessions.append(SESSION_ASIA)
    if 7 * 60 <= utc_minute < 16 * 60:
        sessions.append(SESSION_LONDON)
    if 12 * 60 <= utc_minute < 21 * 60:
        sessions.append(SESSION_NEW_YORK)
    return tuple(sessions)


def _tick_special_tags(
    *,
    source: datetime,
    source_minute: int,
    utc_minute: int,
    session_state: str,
) -> tuple[str, ...]:
    tags: list[str] = []
    month_end = source.day == _month_length(source.year, source.month)
    quarter_end = source.month in {3, 6, 9, 12} and month_end
    year_end = source.month == 12 and source.day == 31
    if session_state in {
        SESSION_STATE_WEEKEND_CLOSURE,
        SESSION_STATE_SUNDAY_OPEN,
        SESSION_STATE_FRIDAY_CLOSE,
    }:
        tags.append(session_state)
    if _minute_in_window(source_minute, *DAILY_ROLLOVER_SOURCE_MINUTES):
        tags.append("daily_rollover")
    if _minute_in_window(utc_minute, *LONDON_FIX_UTC_MINUTES):
        tags.append("london_4pm_fix_window")
        if month_end:
            tags.append("month_end_fix_window")
        if quarter_end:
            tags.append("quarter_end_fix_window")
        if year_end:
            tags.append("year_end_fix_window")
    if month_end:
        tags.append("month_end")
    if quarter_end:
        tags.append("quarter_end")
    if year_end:
        tags.append("year_end")
    return tuple(dict.fromkeys(tags))


def _minute_in_window(
    minute: int,
    start_minute: int,
    end_minute: int,
) -> bool:
    if start_minute <= end_minute:
        return start_minute <= minute < end_minute
    return minute >= start_minute or minute < end_minute


def _month_length(year: int, month: int) -> int:
    if month == 2:
        if year % 400 == 0 or (year % 4 == 0 and year % 100 != 0):
            return 29
        return 28
    if month in {4, 6, 9, 11}:
        return 30
    return 31


def _record_spread_regime_sample(
    scan: _TickSpreadRegimeScan,
    sample: _TickSpreadRegimeSample,
) -> None:
    _profile_for(scan.symbol_profiles, sample.symbol_key or "unknown").add(
        sample
    )
    _profile_for(scan.source_hour_profiles, sample.source_hour).add(sample)
    for session_key in sample.session_keys:
        _profile_for(scan.session_profiles, session_key).add(sample)
    for regime_key in sample.special_regime_keys:
        _profile_for(scan.special_regime_profiles, regime_key).add(sample)
    scan.global_profile.add(sample)
    if _is_liquid_baseline_sample(sample):
        scan.liquid_profile.add(sample)


def _profile_for(
    profiles: dict[str, _SpreadRegimeProfile],
    key: str,
) -> _SpreadRegimeProfile:
    normalized = str(key or "unknown")
    if normalized not in profiles:
        profiles[normalized] = _SpreadRegimeProfile()
    return profiles[normalized]


def _is_liquid_baseline_sample(sample: _TickSpreadRegimeSample) -> bool:
    return (
        sample.session_state == SESSION_STATE_MARKET_OPEN
        and sample.session_keys != (SESSION_NO_ACTIVE_WINDOW,)
        and not sample.special_regime_keys
    )


def _parsed_tick_spread_sample(
    row: list[str],
    *,
    row_number: int,
    source_member: str,
) -> _TickSpreadSample | None:
    values = tuple(cell.strip() for cell in row)
    try:
        bid = float(values[1])
        ask = float(values[2])
    except (ValueError, IndexError):
        return None
    if not math.isfinite(bid) or not math.isfinite(ask):
        return None

    timestamp_source = values[0]
    timestamp_utc_ms = _timestamp_utc_ms_or_none(timestamp_source)
    spread = ask - bid
    return _TickSpreadSample(
        row_number=row_number,
        timestamp_source=timestamp_source,
        timestamp_utc_ms=timestamp_utc_ms,
        column="ask" if spread < 0.0 else "spread",
        bid=bid,
        ask=ask,
        spread=spread,
        raw_values=values,
        source_member=source_member,
    )


def _sample_from_row(
    row: list[str],
    *,
    row_number: int,
    column: str,
    source_member: str,
    metadata: dict[str, JSONValue] | None = None,
) -> _TickSpreadSample:
    values = tuple(cell.strip() for cell in row)
    timestamp_source = values[0] if values else ""
    return _TickSpreadSample(
        row_number=row_number,
        timestamp_source=timestamp_source,
        timestamp_utc_ms=_timestamp_utc_ms_or_none(timestamp_source),
        column=column,
        bid=_float_or_none(values[1] if len(values) > 1 else ""),
        ask=_float_or_none(values[2] if len(values) > 2 else ""),
        spread=None,
        raw_values=values,
        source_member=source_member,
        metadata=dict(metadata or {}),
    )


def _invalid_bid_ask_sample(
    row: list[str],
    *,
    row_number: int,
    source_member: str,
) -> _TickSpreadSample:
    values = tuple(cell.strip() for cell in row)
    bid_raw = values[1] if len(values) > 1 else ""
    ask_raw = values[2] if len(values) > 2 else ""
    column = "bid" if _float_or_none(bid_raw) is None else "ask"
    return _sample_from_row(
        row,
        row_number=row_number,
        column=column,
        source_member=source_member,
        metadata={
            "raw_bid": bid_raw,
            "raw_ask": ask_raw,
            "error": "bid and ask must parse as finite decimal numbers",
        },
    )


def _missing_bid_ask_column(row: list[str], expected_count: int) -> str:
    if len(row) != expected_count:
        if len(row) <= 1:
            return "bid"
        if len(row) <= 2:
            return "ask"
        return "schema"
    if not row[1].strip():
        return "bid"
    if not row[2].strip():
        return "ask"
    return ""


def _finalize_zero_spread_run(
    scan: _TickSpreadScan,
    start: _TickSpreadSample | None,
    end: _TickSpreadSample | None,
    run_length: int,
    thresholds: HistDataTickSpreadThresholds,
) -> None:
    if start is None or end is None:
        return
    if run_length < thresholds.zero_spread_run_length:
        return
    scan.zero_spread_run_count += 1
    _append_zero_spread_run(
        scan.zero_spread_runs,
        _ZeroSpreadRunSample(
            start=start,
            end=end,
            run_length=run_length,
        ),
    )


def _finalize_microstructure_runs(
    scan: _TickMicrostructureScan,
    stale_start: _TickSpreadSample | None,
    stale_end: _TickSpreadSample | None,
    stale_length: int,
    burst_start: _TickSpreadSample | None,
    burst_end: _TickSpreadSample | None,
    burst_length: int,
    one_sided_start: _TickSpreadSample | None,
    one_sided_end: _TickSpreadSample | None,
    one_sided_previous: _TickSpreadSample | None,
    one_sided_length: int,
    one_sided_direction: str,
    thresholds: HistDataTickMicrostructureThresholds,
) -> tuple[
    None,
    None,
    int,
    None,
    None,
    int,
    None,
    None,
    None,
    int,
    str,
]:
    _finalize_stale_quote_run(
        scan,
        stale_start,
        stale_end,
        stale_length,
        thresholds,
    )
    _finalize_burst_run(
        scan,
        burst_start,
        burst_end,
        burst_length,
        thresholds,
    )
    _finalize_one_sided_run(
        scan,
        one_sided_start,
        one_sided_end,
        one_sided_previous,
        one_sided_length,
        one_sided_direction,
        thresholds,
    )
    return (None, None, 0, None, None, 0, None, None, None, 0, "")


def _finalize_stale_quote_run(
    scan: _TickMicrostructureScan,
    start: _TickSpreadSample | None,
    end: _TickSpreadSample | None,
    run_length: int,
    thresholds: HistDataTickMicrostructureThresholds,
) -> None:
    if start is None or end is None:
        return
    if run_length < thresholds.stale_quote_run_length:
        return
    scan.stale_quote_run_count += 1
    scan.stale_quote_run_row_count += run_length
    _append_microstructure_run(
        scan.stale_quote_runs,
        _TickMicrostructureRunSample(
            start=start,
            end=end,
            run_length=run_length,
            metric="stale_quote_rows",
            metadata={
                "stale_max_gap_ms": thresholds.stale_max_gap_ms,
                "threshold_run_length": thresholds.stale_quote_run_length,
            },
        ),
    )


def _finalize_burst_run(
    scan: _TickMicrostructureScan,
    start: _TickSpreadSample | None,
    end: _TickSpreadSample | None,
    run_length: int,
    thresholds: HistDataTickMicrostructureThresholds,
) -> None:
    if start is None or end is None:
        return
    if run_length < thresholds.burst_run_length:
        return
    scan.burst_run_count += 1
    scan.burst_tick_count += run_length
    _append_microstructure_run(
        scan.burst_runs,
        _TickMicrostructureRunSample(
            start=start,
            end=end,
            run_length=run_length,
            metric="burst_ticks",
            metadata={
                "burst_max_interval_ms": thresholds.burst_max_interval_ms,
                "threshold_run_length": thresholds.burst_run_length,
                "duration_ms": _tick_interval_ms(start, end),
            },
        ),
    )


def _finalize_one_sided_run(
    scan: _TickMicrostructureScan,
    start: _TickSpreadSample | None,
    end: _TickSpreadSample | None,
    previous: _TickSpreadSample | None,
    run_length: int,
    direction: str,
    thresholds: HistDataTickMicrostructureThresholds,
) -> None:
    if start is None or end is None or previous is None or not direction:
        return
    if run_length < thresholds.one_sided_run_length:
        return
    previous = _materialize_tick_sample(previous)
    scan.one_sided_run_count += 1
    _append_microstructure_run(
        scan.one_sided_runs,
        _TickMicrostructureRunSample(
            start=start,
            end=end,
            run_length=run_length,
            metric="one_sided_quote_movements",
            direction=direction,
            metadata={
                "threshold_run_length": thresholds.one_sided_run_length,
                "previous_row_number": previous.row_number,
                "previous_timestamp_source": previous.timestamp_source,
                "previous_timestamp_utc_ms": previous.timestamp_utc_ms,
                "previous_values": previous.values_metadata(),
            },
        ),
    )


def _duplicate_microstructure_sample(
    sample: _TickSpreadSample,
    duplicate: _TickSpreadSample,
) -> _TickSpreadSample:
    return _TickSpreadSample(
        row_number=sample.row_number,
        timestamp_source=sample.timestamp_source,
        timestamp_utc_ms=sample.timestamp_utc_ms,
        column="row",
        bid=sample.bid,
        ask=sample.ask,
        spread=sample.spread,
        raw_values=sample.raw_values,
        source_member=sample.source_member,
        metadata={
            "duplicate_of_row": duplicate.row_number,
            "duplicate_timestamp_utc_ms": duplicate.timestamp_utc_ms,
            "dedupe_policy": "summary-only",
            "owner_rule_id": DUPLICATE_TICK_OWNER_RULE_ID,
            "owner_finding_code": DUPLICATE_TICK_OWNER_FINDING_CODE,
        },
    )


def _tick_interval_ms(
    previous: _TickSpreadSample,
    current: _TickSpreadSample,
) -> int | None:
    if previous.timestamp_utc_ms is None or current.timestamp_utc_ms is None:
        return None
    return current.timestamp_utc_ms - previous.timestamp_utc_ms


def _is_stale_quote_pair(
    previous: _TickSpreadSample,
    current: _TickSpreadSample,
    interval_ms: int | None,
    thresholds: HistDataTickMicrostructureThresholds,
) -> bool:
    return (
        interval_ms is not None
        and 0 <= interval_ms <= thresholds.stale_max_gap_ms
        and previous.bid == current.bid
        and previous.ask == current.ask
    )


def _is_burst_interval(
    interval_ms: int | None,
    thresholds: HistDataTickMicrostructureThresholds,
) -> bool:
    return (
        interval_ms is not None
        and 0 <= interval_ms <= thresholds.burst_max_interval_ms
    )


def _one_sided_quote_direction(
    previous: _TickSpreadSample,
    current: _TickSpreadSample,
) -> str:
    bid_changed = previous.bid != current.bid
    ask_changed = previous.ask != current.ask
    if bid_changed and not ask_changed:
        return "bid_only"
    if ask_changed and not bid_changed:
        return "ask_only"
    return ""


def _spread_threshold_selection(
    target: QualityTarget,
    *,
    thresholds: HistDataTickSpreadThresholds,
    thresholds_by_asset_class: Mapping[str, HistDataTickSpreadThresholds],
) -> _TickSpreadThresholdSelection:
    symbol_key = normalize_histdata_symbol(target.symbol)
    asset_class_key = symbol_metadata_for(target.symbol).asset_class.lower()
    asset_profiles = _normalized_asset_class_thresholds(
        thresholds_by_asset_class
    )
    if asset_class_key in asset_profiles:
        return _TickSpreadThresholdSelection(
            thresholds=asset_profiles[asset_class_key],
            source="asset_class",
            symbol_key=symbol_key,
            asset_class_key=asset_class_key,
            profile_key=asset_class_key,
        )

    return _TickSpreadThresholdSelection(
        thresholds=thresholds,
        source="default",
        symbol_key=symbol_key,
        asset_class_key=asset_class_key,
        profile_key=DEFAULT_SESSION_PROFILE,
    )


def _spread_regime_threshold_selection(
    target: QualityTarget,
    *,
    thresholds: HistDataTickSpreadRegimeThresholds,
    thresholds_by_asset_class: Mapping[
        str,
        HistDataTickSpreadRegimeThresholds,
    ],
) -> _TickSpreadRegimeThresholdSelection:
    symbol_key = normalize_histdata_symbol(target.symbol)
    asset_class_key = symbol_metadata_for(target.symbol).asset_class.lower()
    asset_profiles = _normalized_asset_class_thresholds(
        thresholds_by_asset_class
    )
    if asset_class_key in asset_profiles:
        return _TickSpreadRegimeThresholdSelection(
            thresholds=asset_profiles[asset_class_key],
            source="asset_class",
            symbol_key=symbol_key,
            asset_class_key=asset_class_key,
            profile_key=asset_class_key,
        )

    return _TickSpreadRegimeThresholdSelection(
        thresholds=thresholds,
        source="default",
        symbol_key=symbol_key,
        asset_class_key=asset_class_key,
        profile_key=DEFAULT_SESSION_PROFILE,
    )


def _microstructure_threshold_selection(
    target: QualityTarget,
    *,
    thresholds: HistDataTickMicrostructureThresholds,
    thresholds_by_symbol: Mapping[str, HistDataTickMicrostructureThresholds],
    thresholds_by_session: Mapping[str, HistDataTickMicrostructureThresholds],
    thresholds_by_asset_class: Mapping[
        str,
        HistDataTickMicrostructureThresholds,
    ],
    thresholds_by_symbol_session: Mapping[
        str,
        HistDataTickMicrostructureThresholds,
    ],
    session_name: str,
) -> _TickMicrostructureThresholdSelection:
    symbol_key = normalize_histdata_symbol(target.symbol)
    asset_class_key = symbol_metadata_for(target.symbol).asset_class.lower()
    session_key = _normalize_session_key(session_name)
    profile_key = f"{symbol_key}:{session_key}" if symbol_key else session_key

    symbol_session_profiles = _normalized_symbol_session_thresholds(
        thresholds_by_symbol_session
    )
    if profile_key in symbol_session_profiles:
        return _TickMicrostructureThresholdSelection(
            thresholds=symbol_session_profiles[profile_key],
            source="symbol-session",
            symbol_key=symbol_key,
            session_key=session_key,
            asset_class_key=asset_class_key,
            profile_key=profile_key,
        )

    symbol_profiles = _normalized_symbol_thresholds(thresholds_by_symbol)
    if symbol_key in symbol_profiles:
        return _TickMicrostructureThresholdSelection(
            thresholds=symbol_profiles[symbol_key],
            source="symbol",
            symbol_key=symbol_key,
            session_key=session_key,
            asset_class_key=asset_class_key,
            profile_key=symbol_key,
        )

    session_profiles = _normalized_session_thresholds(thresholds_by_session)
    if session_key in session_profiles:
        return _TickMicrostructureThresholdSelection(
            thresholds=session_profiles[session_key],
            source="session",
            symbol_key=symbol_key,
            session_key=session_key,
            asset_class_key=asset_class_key,
            profile_key=session_key,
        )

    asset_profiles = _normalized_asset_class_thresholds(
        thresholds_by_asset_class
    )
    if asset_class_key in asset_profiles:
        return _TickMicrostructureThresholdSelection(
            thresholds=asset_profiles[asset_class_key],
            source="asset_class",
            symbol_key=symbol_key,
            session_key=session_key,
            asset_class_key=asset_class_key,
            profile_key=asset_class_key,
        )

    return _TickMicrostructureThresholdSelection(
        thresholds=thresholds,
        source="default",
        symbol_key=symbol_key,
        session_key=session_key,
        asset_class_key=asset_class_key,
        profile_key=DEFAULT_SESSION_PROFILE,
    )


def _normalized_symbol_thresholds(
    thresholds: Mapping[str, HistDataTickMicrostructureThresholds],
) -> dict[str, HistDataTickMicrostructureThresholds]:
    return {
        normalize_histdata_symbol(symbol): value
        for symbol, value in thresholds.items()
        if normalize_histdata_symbol(symbol)
    }


def _normalized_session_thresholds(
    thresholds: Mapping[str, HistDataTickMicrostructureThresholds],
) -> dict[str, HistDataTickMicrostructureThresholds]:
    return {
        _normalize_session_key(session): value
        for session, value in thresholds.items()
        if _normalize_session_key(session)
    }


def _normalized_symbol_session_thresholds(
    thresholds: Mapping[str, HistDataTickMicrostructureThresholds],
) -> dict[str, HistDataTickMicrostructureThresholds]:
    normalized: dict[str, HistDataTickMicrostructureThresholds] = {}
    for key, value in thresholds.items():
        symbol_key, session_key = _split_symbol_session_key(key)
        if symbol_key and session_key:
            normalized[f"{symbol_key}:{session_key}"] = value
    return normalized


def _normalized_asset_class_thresholds(
    thresholds: Mapping[str, _ThresholdT],
) -> dict[str, _ThresholdT]:
    return {
        str(asset_class).strip().lower(): value
        for asset_class, value in thresholds.items()
        if str(asset_class).strip()
    }


def _split_symbol_session_key(key: str) -> tuple[str, str]:
    raw = str(key or "").strip()
    separator = ":" if ":" in raw else "/"
    if separator not in raw:
        return "", ""
    symbol, session = raw.split(separator, maxsplit=1)
    return normalize_histdata_symbol(symbol), _normalize_session_key(session)


def _normalize_session_key(value: str) -> str:
    normalized = str(value or DEFAULT_SESSION_PROFILE).strip().lower()
    return normalized.replace(" ", "_") or DEFAULT_SESSION_PROFILE


def _spread_regime_findings(
    *,
    target: QualityTarget,
    scan: _TickSpreadRegimeScan,
    source_member: str,
    threshold_selection: _TickSpreadRegimeThresholdSelection,
    severity: QualitySeverity,
    rule_id: str,
) -> tuple[QualityFinding, ...]:
    thresholds = threshold_selection.thresholds
    baseline_profile, baseline_source = _spread_regime_baseline(scan)
    baseline_median = _median_or_none(baseline_profile.values) or 0.0
    findings: list[QualityFinding] = [
        _finding(
            target,
            code="ASCII_TICK_SPREAD_REGIME_SUMMARY",
            message="Tick spread profiles by symbol, source hour, session, "
            "and special market regime.",
            severity=QualitySeverity.INFO,
            rule_id=rule_id,
            metadata={
                **_base_metadata(target, source_member=source_member),
                "row_count": scan.row_count,
                "parsed_row_count": scan.parsed_row_count,
                "profiled_row_count": scan.profiled_row_count,
                "invalid_tick_count": scan.invalid_tick_count,
                "invalid_timestamp_count": scan.invalid_timestamp_count,
                "negative_spread_count": scan.negative_spread_count,
                "wide_spread_count": scan.wide_spread_count,
                "spread_jump_count": scan.spread_jump_count,
                "regime_shift_count": scan.regime_shift_count,
                "baseline_profile": {
                    "source": baseline_source,
                    **baseline_profile.to_metadata(),
                },
                "wide_spread_threshold": _wide_spread_threshold(
                    baseline_median,
                    thresholds,
                ),
                "spread_jump_threshold": _spread_jump_threshold(
                    baseline_median,
                    thresholds,
                ),
                "regime_median_threshold": _regime_median_threshold(
                    baseline_median,
                    thresholds,
                ),
                "symbol_spread_profiles": _spread_regime_profiles_metadata(
                    scan.symbol_profiles
                ),
                "source_hour_spread_profiles": (
                    _spread_regime_profiles_metadata(scan.source_hour_profiles)
                ),
                "session_spread_profiles": _spread_regime_profiles_metadata(
                    scan.session_profiles
                ),
                "special_regime_spread_profiles": (
                    _spread_regime_profiles_metadata(
                        scan.special_regime_profiles
                    )
                ),
                "threshold_profile": threshold_selection.to_metadata(),
                "thresholds": thresholds.to_metadata(),
            },
        )
    ]
    if scan.wide_spreads:
        findings.append(
            _spread_regime_sample_finding(
                target,
                code="ASCII_TICK_SPREAD_REGIME_WIDE_SPREAD",
                message="Tick spread exceeds the liquid-hour baseline "
                "threshold.",
                severity=severity,
                samples=scan.wide_spreads,
                row_count=scan.wide_spread_count,
                source_member=source_member,
                threshold_selection=threshold_selection,
                rule_id=rule_id,
                column="spread",
            )
        )
    if scan.spread_jumps:
        findings.append(
            _spread_regime_sample_finding(
                target,
                code="ASCII_TICK_SPREAD_REGIME_JUMP",
                message="Tick spread changes abruptly relative to the "
                "liquid-hour baseline threshold.",
                severity=severity,
                samples=scan.spread_jumps,
                row_count=scan.spread_jump_count,
                source_member=source_member,
                threshold_selection=threshold_selection,
                rule_id=rule_id,
                column="spread",
            )
        )
    if scan.regime_shifts:
        findings.append(
            _spread_regime_sample_finding(
                target,
                code="ASCII_TICK_SPREAD_REGIME_SHIFT",
                message="Special spread regime median exceeds the "
                "liquid-hour baseline threshold.",
                severity=severity,
                samples=scan.regime_shifts,
                row_count=scan.regime_shift_count,
                source_member=source_member,
                threshold_selection=threshold_selection,
                rule_id=rule_id,
                column="spread",
            )
        )
    return tuple(findings)


def _spread_regime_sample_finding(
    target: QualityTarget,
    *,
    code: str,
    message: str,
    severity: QualitySeverity,
    samples: list[_TickSpreadRegimeSample],
    row_count: int,
    source_member: str,
    threshold_selection: _TickSpreadRegimeThresholdSelection,
    rule_id: str,
    column: str,
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
            column=column,
            metadata={
                "source_timezone": SOURCE_TIMEZONE,
                "source_utc_offset": SOURCE_UTC_OFFSET,
                "utc_timestamp": first.utc_timestamp,
                "source_member": first.source_member,
                "symbol": first.symbol_key,
                "source_hour": first.source_hour,
                "utc_hour": first.utc_hour,
                "session_state": first.session_state,
                "session_keys": list(first.session_keys),
                "special_regime_keys": list(first.special_regime_keys),
                "profile_key": first.profile_key,
                "spread_delta": first.spread_delta,
                "threshold": first.threshold,
                "values": first.sample.values_metadata(),
            },
        ),
        metadata={
            **_base_metadata(target, source_member=source_member),
            "row_count": row_count,
            "threshold_profile": threshold_selection.to_metadata(),
            "thresholds": threshold_selection.thresholds.to_metadata(),
            "samples": _spread_regime_samples(samples),
        },
    )


def _spread_regime_baseline(
    scan: _TickSpreadRegimeScan,
) -> tuple[_SpreadRegimeProfile, str]:
    if scan.liquid_profile.values:
        return scan.liquid_profile, "liquid_session_non_special"
    return scan.global_profile, "global_profile_fallback"


def _wide_spread_threshold(
    baseline_median: float,
    thresholds: HistDataTickSpreadRegimeThresholds,
) -> float | None:
    if baseline_median <= 0.0 and thresholds.minimum_wide_spread <= 0.0:
        return None
    return max(
        baseline_median * thresholds.wide_spread_multiplier,
        thresholds.minimum_wide_spread,
    )


def _spread_jump_threshold(
    baseline_median: float,
    thresholds: HistDataTickSpreadRegimeThresholds,
) -> float | None:
    if baseline_median <= 0.0 and thresholds.minimum_spread_jump <= 0.0:
        return None
    return max(
        baseline_median * thresholds.jump_spread_multiplier,
        thresholds.minimum_spread_jump,
    )


def _regime_median_threshold(
    baseline_median: float,
    thresholds: HistDataTickSpreadRegimeThresholds,
) -> float | None:
    if baseline_median <= 0.0:
        return None
    return baseline_median * thresholds.regime_median_multiplier


def _spread_regime_profiles_metadata(
    profiles: Mapping[str, _SpreadRegimeProfile],
) -> dict[str, JSONValue]:
    return {
        key: profile.to_metadata() for key, profile in sorted(profiles.items())
    }


def _spread_stats_metadata(values: list[float]) -> dict[str, JSONValue]:
    return {
        "count": len(values),
        "min_spread": min(values) if values else None,
        "median_spread": _median_or_none(values),
        "max_spread": max(values) if values else None,
        "mean_spread": (sum(values) / len(values) if values else None),
    }


def _median_or_none(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / 2.0


def _regime_warning_sample(
    sample: _TickSpreadRegimeSample,
    *,
    threshold: float,
    previous: _TickSpreadSample | None = None,
    spread_delta: float | None = None,
    profile_key: str,
) -> _TickSpreadRegimeSample:
    sample = _materialize_spread_regime_sample(sample)
    return _TickSpreadRegimeSample(
        sample=sample.sample,
        symbol_key=sample.symbol_key,
        source_hour=sample.source_hour,
        utc_hour=sample.utc_hour,
        session_state=sample.session_state,
        session_keys=sample.session_keys,
        special_regime_keys=sample.special_regime_keys,
        previous=previous,
        spread_delta=spread_delta,
        threshold=threshold,
        profile_key=profile_key,
    )


def _spread_findings(
    *,
    target: QualityTarget,
    scan: _TickSpreadScan,
    source_member: str,
    threshold_selection: _TickSpreadThresholdSelection,
    zero_spread_severity: QualitySeverity,
    negative_spread_severity: QualitySeverity,
    schema_severity: QualitySeverity,
    rule_id: str,
) -> tuple[QualityFinding, ...]:
    thresholds = threshold_selection.thresholds
    findings: list[QualityFinding] = [
        _finding(
            target,
            code="ASCII_TICK_SPREAD_SUMMARY",
            message="Tick bid/ask spread integrity profile.",
            severity=QualitySeverity.INFO,
            rule_id=rule_id,
            metadata={
                **_base_metadata(target, source_member=source_member),
                "row_count": scan.row_count,
                "parsed_row_count": scan.parsed_row_count,
                "missing_bid_ask_count": scan.missing_bid_ask_count,
                "invalid_bid_ask_count": scan.invalid_bid_ask_count,
                "negative_spread_count": scan.negative_spread_count,
                "zero_spread_count": scan.zero_spread_count,
                "zero_spread_run_count": scan.zero_spread_run_count,
                "min_spread": scan.min_spread,
                "max_spread": scan.max_spread,
                "threshold_profile": threshold_selection.to_metadata(),
                "thresholds": thresholds.to_metadata(),
            },
        )
    ]
    if scan.missing_bid_ask:
        findings.append(
            _spread_sample_finding(
                target,
                code="ASCII_TICK_BID_ASK_MISSING",
                message="Tick rows are missing one or more required bid/ask "
                "schema fields.",
                severity=schema_severity,
                samples=scan.missing_bid_ask,
                row_count=scan.missing_bid_ask_count,
                source_member=source_member,
                rule_id=rule_id,
            )
        )
    if scan.invalid_bid_ask:
        findings.append(
            _spread_sample_finding(
                target,
                code="ASCII_TICK_BID_ASK_INVALID",
                message="Tick bid/ask fields must parse as finite decimal "
                "numbers.",
                severity=schema_severity,
                samples=scan.invalid_bid_ask,
                row_count=scan.invalid_bid_ask_count,
                source_member=source_member,
                rule_id=rule_id,
            )
        )
    if scan.negative_spreads:
        findings.append(
            _spread_sample_finding(
                target,
                code="ASCII_TICK_NEGATIVE_SPREAD",
                message="Tick ask price is below bid price.",
                severity=negative_spread_severity,
                samples=scan.negative_spreads,
                row_count=scan.negative_spread_count,
                source_member=source_member,
                rule_id=rule_id,
            )
        )
    if scan.zero_spread_runs:
        findings.append(
            _zero_spread_run_finding(
                target,
                samples=scan.zero_spread_runs,
                zero_spread_count=scan.zero_spread_count,
                source_member=source_member,
                threshold_selection=threshold_selection,
                severity=zero_spread_severity,
                rule_id=rule_id,
            )
        )
    return tuple(findings)


def _spread_sample_finding(
    target: QualityTarget,
    *,
    code: str,
    message: str,
    severity: QualitySeverity,
    samples: list[_TickSpreadSample],
    row_count: int,
    source_member: str,
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
                "values": first.values_metadata(),
                **dict(first.metadata),
            },
        ),
        metadata={
            **_base_metadata(target, source_member=source_member),
            "row_count": row_count,
            "samples": _spread_samples(samples),
        },
    )


def _zero_spread_run_finding(
    target: QualityTarget,
    *,
    samples: list[_ZeroSpreadRunSample],
    zero_spread_count: int,
    source_member: str,
    threshold_selection: _TickSpreadThresholdSelection,
    severity: QualitySeverity,
    rule_id: str,
) -> QualityFinding:
    first = samples[0]
    return _finding(
        target,
        code="ASCII_TICK_ZERO_SPREAD_RUN",
        message="Tick rows contain one or more zero-spread runs.",
        severity=severity,
        rule_id=rule_id,
        location=QualityLocation(
            path=target.path,
            row_number=first.row_number,
            timestamp_source=first.timestamp_source,
            timestamp_utc_ms=first.timestamp_utc_ms,
            column="spread",
            metadata={
                "source_timezone": SOURCE_TIMEZONE,
                "source_utc_offset": SOURCE_UTC_OFFSET,
                "utc_timestamp": first.utc_timestamp,
                "source_member": first.source_member,
                "run_length": first.run_length,
                "values": first.start.values_metadata(),
            },
        ),
        metadata={
            **_base_metadata(target, source_member=source_member),
            "row_count": len(samples),
            "zero_spread_count": zero_spread_count,
            "threshold_profile": threshold_selection.to_metadata(),
            "thresholds": threshold_selection.thresholds.to_metadata(),
            "samples": _zero_spread_run_samples(samples),
        },
    )


def _microstructure_findings(
    *,
    target: QualityTarget,
    scan: _TickMicrostructureScan,
    source_member: str,
    threshold_selection: _TickMicrostructureThresholdSelection,
    severity: QualitySeverity,
    rule_id: str,
) -> tuple[QualityFinding, ...]:
    findings: list[QualityFinding] = [
        _finding(
            target,
            code="ASCII_TICK_MICROSTRUCTURE_SUMMARY",
            message="Tick duplicate, stale, burst, and one-sided quote "
            "microstructure profile.",
            severity=QualitySeverity.INFO,
            rule_id=rule_id,
            metadata={
                **_base_metadata(target, source_member=source_member),
                "row_count": scan.row_count,
                "parsed_row_count": scan.parsed_row_count,
                "invalid_tick_count": scan.invalid_tick_count,
                "duplicate_row_count": scan.duplicate_row_count,
                "stale_quote_repeat_count": scan.stale_quote_repeat_count,
                "stale_quote_run_count": scan.stale_quote_run_count,
                "stale_quote_run_row_count": (scan.stale_quote_run_row_count),
                "burst_interval_count": scan.burst_interval_count,
                "burst_run_count": scan.burst_run_count,
                "burst_tick_count": scan.burst_tick_count,
                "one_sided_movement_count": (scan.one_sided_movement_count),
                "one_sided_run_count": scan.one_sided_run_count,
                "bid_only_movement_count": scan.bid_only_movement_count,
                "ask_only_movement_count": scan.ask_only_movement_count,
                "threshold_profile": threshold_selection.to_metadata(),
                "duplicate_detection": {
                    "summary_only": True,
                    "owner_rule_id": DUPLICATE_TICK_OWNER_RULE_ID,
                    "owner_finding_code": DUPLICATE_TICK_OWNER_FINDING_CODE,
                },
                "duplicate_samples": _spread_samples(scan.duplicate_rows),
            },
        )
    ]
    if scan.stale_quote_runs:
        findings.append(
            _microstructure_run_finding(
                target,
                code="ASCII_TICK_STALE_QUOTE_RUN",
                message="Tick bid/ask quotes are unchanged across a stale "
                "run inside the configured active-window gap.",
                severity=severity,
                samples=scan.stale_quote_runs,
                source_member=source_member,
                threshold_selection=threshold_selection,
                rule_id=rule_id,
                column="bid,ask",
                count_metadata={
                    "stale_quote_repeat_count": (scan.stale_quote_repeat_count),
                    "stale_quote_run_count": scan.stale_quote_run_count,
                    "stale_quote_run_row_count": (
                        scan.stale_quote_run_row_count
                    ),
                },
            )
        )
    if scan.burst_runs:
        findings.append(
            _microstructure_run_finding(
                target,
                code="ASCII_TICK_BURST_RUN",
                message="Tick timestamps contain one or more dense burst "
                "runs under the configured interval threshold.",
                severity=severity,
                samples=scan.burst_runs,
                source_member=source_member,
                threshold_selection=threshold_selection,
                rule_id=rule_id,
                column="datetime",
                count_metadata={
                    "burst_interval_count": scan.burst_interval_count,
                    "burst_run_count": scan.burst_run_count,
                    "burst_tick_count": scan.burst_tick_count,
                },
            )
        )
    if scan.one_sided_runs:
        findings.append(
            _microstructure_run_finding(
                target,
                code="ASCII_TICK_ONE_SIDED_QUOTE_RUN",
                message="Tick quotes contain one-sided bid/ask movement "
                "runs.",
                severity=severity,
                samples=scan.one_sided_runs,
                source_member=source_member,
                threshold_selection=threshold_selection,
                rule_id=rule_id,
                column="bid,ask",
                count_metadata={
                    "one_sided_movement_count": (scan.one_sided_movement_count),
                    "one_sided_run_count": scan.one_sided_run_count,
                    "bid_only_movement_count": (scan.bid_only_movement_count),
                    "ask_only_movement_count": (scan.ask_only_movement_count),
                },
            )
        )
    return tuple(findings)


def _microstructure_run_finding(
    target: QualityTarget,
    *,
    code: str,
    message: str,
    severity: QualitySeverity,
    samples: list[_TickMicrostructureRunSample],
    source_member: str,
    threshold_selection: _TickMicrostructureThresholdSelection,
    rule_id: str,
    column: str,
    count_metadata: dict[str, JSONValue],
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
            column=column,
            metadata={
                "source_timezone": SOURCE_TIMEZONE,
                "source_utc_offset": SOURCE_UTC_OFFSET,
                "utc_timestamp": first.utc_timestamp,
                "source_member": first.source_member,
                "run_length": first.run_length,
                "metric": first.metric,
                "direction": first.direction,
                "values": first.start.values_metadata(),
                **dict(first.metadata),
            },
        ),
        metadata={
            **_base_metadata(target, source_member=source_member),
            **count_metadata,
            "row_count": len(samples),
            "threshold_profile": threshold_selection.to_metadata(),
            "samples": _microstructure_run_samples(samples),
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
        "price_columns": list(TICK_PRICE_COLUMNS),
        "quote_sides": list(TICK_PRICE_COLUMNS),
        "spread_definition": "ask - bid",
    }


def _finding(
    target: QualityTarget,
    *,
    code: str,
    message: str,
    severity: QualitySeverity = QualitySeverity.ERROR,
    rule_id: str = ASCII_TICK_SPREAD_RULE_ID,
    location: QualityLocation | None = None,
    metadata: dict[str, JSONValue] | None = None,
) -> QualityFinding:
    return QualityFinding(
        severity=QualitySeverity.from_value(severity),
        code=code,
        message=message,
        rule_id=rule_id,
        target=target,
        location=location or QualityLocation(path=target.path),
        metadata=dict(metadata or {}),
    )


def _parse_row(raw: str, delimiter: str) -> list[str]:
    return next(csv.reader((raw,), delimiter=delimiter), [])


def _timestamp_utc_ms_or_none(value: str) -> int | None:
    if not value:
        return None
    try:
        return int(parse_histdata_datetime_to_utc_ms(value, TICK))
    except ValueError:
        return None


def _float_or_none(value: str) -> float | None:
    try:
        parsed = float(value.strip())
    except ValueError:
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _append_spread_sample(
    samples: list[_TickSpreadSample],
    sample: _TickSpreadSample,
) -> None:
    if len(samples) < MAX_TICK_SAMPLES:
        samples.append(_materialize_tick_sample(sample))


def _append_zero_spread_run(
    samples: list[_ZeroSpreadRunSample],
    sample: _ZeroSpreadRunSample,
) -> None:
    if len(samples) < MAX_TICK_SAMPLES:
        samples.append(_materialize_zero_spread_run(sample))


def _append_microstructure_run(
    samples: list[_TickMicrostructureRunSample],
    sample: _TickMicrostructureRunSample,
) -> None:
    if len(samples) < MAX_TICK_SAMPLES:
        samples.append(_materialize_microstructure_run(sample))


def _append_spread_regime_sample(
    samples: list[_TickSpreadRegimeSample],
    sample: _TickSpreadRegimeSample,
) -> None:
    if len(samples) < MAX_TICK_SAMPLES:
        samples.append(_materialize_spread_regime_sample(sample))


def _spread_samples(samples: Iterable[_TickSpreadSample]) -> list[JSONValue]:
    return [sample.to_dict() for sample in samples]


def _zero_spread_run_samples(
    samples: Iterable[_ZeroSpreadRunSample],
) -> list[JSONValue]:
    return [sample.to_dict() for sample in samples]


def _microstructure_run_samples(
    samples: Iterable[_TickMicrostructureRunSample],
) -> list[JSONValue]:
    return [sample.to_dict() for sample in samples]


def _spread_regime_samples(
    samples: Iterable[_TickSpreadRegimeSample],
) -> list[JSONValue]:
    return [sample.to_dict() for sample in samples]


def _utc_iso_from_ms(timestamp_utc_ms: int) -> str:
    seconds, milliseconds = divmod(timestamp_utc_ms, 1000)
    return (
        datetime.fromtimestamp(seconds, tz=timezone.utc)
        .replace(microsecond=milliseconds * 1000)
        .isoformat()
        .replace("+00:00", "Z")
    )
