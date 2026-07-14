"""Deterministic event-time cross-currency reconstruction and validation.

This module turns individually carved symbol streams into one synchronized
generation unit.  It never forward-fills quotes.  Relationships are evaluated
only at exact event times, duplicate timestamps are paired by deterministic
event order, and immutable observations are never projected.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from enum import Enum
import hashlib
import math
from typing import Any, cast

from histdatacom.data_quality.contracts import QualityReport
from histdatacom.data_quality.symbols import (
    CrossInstrumentPointInput,
    CrossInstrumentSeriesInput,
    HistDataCrossInstrumentConsistencyRule,
    HistDataCrossInstrumentTolerance,
)
from histdatacom.histdata_ascii import TICK
from histdatacom.runtime_contracts import JSONValue
from histdatacom.synthetic.contracts import (
    SyntheticEventOrigin,
    SyntheticEventStreamV1,
    SyntheticEventV1,
    canonical_contract_json,
)
from histdatacom.synthetic.streaming import (
    PartitionManifestV1,
    ReconstructionRunV1,
    ReconstructionWindowV1,
    plan_reconstruction_windows,
)

CROSS_CURRENCY_ENGINE_ID = "histdatacom.cross-currency-reconciliation"
CROSS_CURRENCY_ENGINE_VERSION = "1.0.0"
CROSS_CURRENCY_SYMBOL_COVERAGE_SCHEMA_VERSION = (
    "histdatacom.cross-currency-symbol-coverage.v1"
)
CROSS_CURRENCY_EXCLUDED_SPAN_SCHEMA_VERSION = (
    "histdatacom.cross-currency-excluded-span.v1"
)
CROSS_CURRENCY_WINDOW_PLAN_SCHEMA_VERSION = (
    "histdatacom.cross-currency-window-plan.v1"
)
CROSS_CURRENCY_RELATIONSHIP_SCHEMA_VERSION = (
    "histdatacom.cross-currency-relationship.v1"
)
CROSS_CURRENCY_CONFIG_SCHEMA_VERSION = (
    "histdatacom.cross-currency-reconciliation-config.v1"
)
CROSS_CURRENCY_CONDITION_SCHEMA_VERSION = (
    "histdatacom.cross-currency-condition.v1"
)
CROSS_CURRENCY_PROJECTION_LINEAGE_SCHEMA_VERSION = (
    "histdatacom.cross-currency-projection-lineage.v1"
)
CROSS_CURRENCY_RELATIONSHIP_SUPPORT_SCHEMA_VERSION = (
    "histdatacom.cross-currency-relationship-support.v1"
)
CROSS_CURRENCY_RESIDUAL_SLICE_SCHEMA_VERSION = (
    "histdatacom.cross-currency-residual-slice.v1"
)
CROSS_CURRENCY_VALIDATION_SCHEMA_VERSION = (
    "histdatacom.cross-currency-validation.v1"
)
CROSS_CURRENCY_GROUP_SCHEMA_VERSION = (
    "histdatacom.cross-currency-reconciled-group.v1"
)

EURUSD_TRIANGLE_SYMBOLS = ("eurgbp", "eurusd", "gbpusd")
DEFAULT_CROSS_CURRENCY_MAX_PROJECTION_RELATIVE = 0.05
DEFAULT_CROSS_CURRENCY_RESIDUAL_TOLERANCE = 1e-10
DEFAULT_CROSS_CURRENCY_SPREAD_TOLERANCE_MULTIPLIER = 1.0
DEFAULT_CROSS_CURRENCY_ROUNDING_DIGITS = 12
MAX_CROSS_CURRENCY_CONDITIONS = 4096
MAX_CROSS_CURRENCY_FAILURE_REASONS = 128
MAX_CROSS_CURRENCY_RESIDUAL_SLICES = 4096


class CrossCurrencyCoverageStatus(str, Enum):
    """Whether one symbol has usable source coverage."""

    AVAILABLE = "available"
    MISSING = "missing"


class CrossCurrencyWindowPlanStatus(str, Enum):
    """Whether common synchronized windows could be planned."""

    PLANNED = "planned"
    REFUSED = "refused"


class CrossCurrencyExcludedReason(str, Enum):
    """Why a requested span is outside common reconstruction support."""

    SYMBOL_NOT_YET_AVAILABLE = "symbol_not_yet_available"
    SYMBOL_NO_LONGER_AVAILABLE = "symbol_no_longer_available"
    MISSING_SYMBOL = "missing_symbol"
    NO_COMMON_SUPPORT = "no_common_support"


class CrossCurrencyRelationshipKind(str, Enum):
    """Supported deterministic FX algebra relationships."""

    TRIANGLE = "triangle"
    INVERSE = "inverse"


class CrossCurrencyValidationStage(str, Enum):
    """Mandatory cross-series validation boundaries."""

    GENERATION = "generation"
    POST_BROKER = "post_broker"


class CrossCurrencyValidationStatus(str, Enum):
    """Whether a synchronized output satisfies its relationship contract."""

    PASSED = "passed"
    FAILED = "failed"


class CrossCurrencyGroupStatus(str, Enum):
    """Whether a group is eligible to proceed beyond generation."""

    RECONCILED = "reconciled"
    REFUSED = "refused"


@dataclass(frozen=True, slots=True)
class CrossCurrencySymbolCoverageV1:
    """Half-open source coverage for one member of a synchronized group."""

    symbol: str
    start_ns: int | None = None
    end_ns: int | None = None
    source_periods: tuple[str, ...] = ()
    status: CrossCurrencyCoverageStatus = CrossCurrencyCoverageStatus.AVAILABLE
    schema_version: str = CROSS_CURRENCY_SYMBOL_COVERAGE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            CROSS_CURRENCY_SYMBOL_COVERAGE_SCHEMA_VERSION,
            "symbol coverage",
        )
        object.__setattr__(self, "symbol", _normalized_symbol(self.symbol))
        object.__setattr__(
            self, "status", CrossCurrencyCoverageStatus(self.status)
        )
        periods = _normalized_text_tuple(self.source_periods)
        object.__setattr__(self, "source_periods", periods)
        if self.status is CrossCurrencyCoverageStatus.MISSING:
            if self.start_ns is not None or self.end_ns is not None:
                raise ValueError("missing symbol coverage cannot have bounds")
            return
        if self.start_ns is None or self.end_ns is None:
            raise ValueError("available symbol coverage requires bounds")
        start = _int64(self.start_ns, "start_ns")
        end = _int64(self.end_ns, "end_ns")
        if end <= start:
            raise ValueError("coverage end_ns must be greater than start_ns")
        object.__setattr__(self, "start_ns", start)
        object.__setattr__(self, "end_ns", end)

    @classmethod
    def missing(cls, symbol: str) -> "CrossCurrencySymbolCoverageV1":
        """Return explicit missing-symbol coverage."""
        return cls(symbol=symbol, status=CrossCurrencyCoverageStatus.MISSING)

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "symbol": self.symbol,
            "status": self.status.value,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "source_periods": list(self.source_periods),
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "CrossCurrencySymbolCoverageV1":
        return cls(
            symbol=str(data.get("symbol", "")),
            start_ns=cast(int | None, data.get("start_ns")),
            end_ns=cast(int | None, data.get("end_ns")),
            source_periods=_string_tuple(data.get("source_periods")),
            status=CrossCurrencyCoverageStatus(str(data.get("status", ""))),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CrossCurrencyExcludedSpanV1:
    """One deterministic explanation for excluded requested coverage."""

    symbol: str
    start_ns: int
    end_ns: int
    reason: CrossCurrencyExcludedReason
    schema_version: str = CROSS_CURRENCY_EXCLUDED_SPAN_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            CROSS_CURRENCY_EXCLUDED_SPAN_SCHEMA_VERSION,
            "excluded span",
        )
        symbol = str(self.symbol or "").strip().upper()
        if symbol != "*":
            symbol = _normalized_symbol(symbol)
        object.__setattr__(self, "symbol", symbol)
        start = _int64(self.start_ns, "start_ns")
        end = _int64(self.end_ns, "end_ns")
        if end <= start:
            raise ValueError("excluded span end_ns must exceed start_ns")
        object.__setattr__(self, "start_ns", start)
        object.__setattr__(self, "end_ns", end)
        object.__setattr__(
            self, "reason", CrossCurrencyExcludedReason(self.reason)
        )

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "symbol": self.symbol,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "reason": self.reason.value,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "CrossCurrencyExcludedSpanV1":
        return cls(
            symbol=str(data.get("symbol", "")),
            start_ns=_strict_int(data.get("start_ns"), "start_ns"),
            end_ns=_strict_int(data.get("end_ns"), "end_ns"),
            reason=CrossCurrencyExcludedReason(str(data.get("reason", ""))),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CrossCurrencyWindowPlanV1:
    """Deterministic common-coverage plan with explicit excluded spans."""

    run_id: str
    ensemble_member_id: str
    symbols: tuple[str, ...]
    requested_start_ns: int
    requested_end_ns: int
    coverages: tuple[CrossCurrencySymbolCoverageV1, ...]
    excluded_spans: tuple[CrossCurrencyExcludedSpanV1, ...]
    windows: tuple[ReconstructionWindowV1, ...]
    status: CrossCurrencyWindowPlanStatus
    missing_symbols: tuple[str, ...] = ()
    plan_id: str = ""
    schema_version: str = CROSS_CURRENCY_WINDOW_PLAN_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            CROSS_CURRENCY_WINDOW_PLAN_SCHEMA_VERSION,
            "window plan",
        )
        object.__setattr__(self, "run_id", _required_text(self.run_id))
        object.__setattr__(
            self,
            "ensemble_member_id",
            _required_text(self.ensemble_member_id),
        )
        symbols = _normalized_symbols(self.symbols)
        object.__setattr__(self, "symbols", symbols)
        start = _int64(self.requested_start_ns, "requested_start_ns")
        end = _int64(self.requested_end_ns, "requested_end_ns")
        if end <= start:
            raise ValueError("requested_end_ns must exceed requested_start_ns")
        object.__setattr__(self, "requested_start_ns", start)
        object.__setattr__(self, "requested_end_ns", end)
        coverages = tuple(sorted(self.coverages, key=lambda item: item.symbol))
        if tuple(item.symbol for item in coverages) != symbols:
            raise ValueError("coverage symbols must exactly match plan symbols")
        object.__setattr__(self, "coverages", coverages)
        excluded = tuple(
            sorted(
                self.excluded_spans,
                key=lambda item: (
                    item.start_ns,
                    item.end_ns,
                    item.symbol,
                    item.reason.value,
                ),
            )
        )
        object.__setattr__(self, "excluded_spans", excluded)
        windows = tuple(self.windows)
        object.__setattr__(self, "windows", windows)
        missing = _normalized_symbols(self.missing_symbols, allow_empty=True)
        if not set(missing).issubset(symbols):
            raise ValueError("missing symbols are outside the plan")
        object.__setattr__(self, "missing_symbols", missing)
        object.__setattr__(
            self, "status", CrossCurrencyWindowPlanStatus(self.status)
        )
        if self.status is CrossCurrencyWindowPlanStatus.PLANNED:
            if missing or not windows:
                raise ValueError("planned common coverage requires windows")
            if any(
                item.run_id != self.run_id
                or item.ensemble_member_id != self.ensemble_member_id
                or item.symbols != symbols
                for item in windows
            ):
                raise ValueError("planned windows differ from common scope")
        elif windows:
            raise ValueError("refused common coverage cannot contain windows")
        expected = _stable_id("cross-currency-window-plan", self.payload())
        supplied = _optional_text(self.plan_id)
        if supplied is not None and supplied != expected:
            raise ValueError("cross-currency plan_id differs")
        object.__setattr__(self, "plan_id", expected)

    @property
    def common_start_ns(self) -> int | None:
        return self.windows[0].core_start_ns if self.windows else None

    @property
    def common_end_ns(self) -> int | None:
        return self.windows[-1].core_end_ns if self.windows else None

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "ensemble_member_id": self.ensemble_member_id,
            "symbols": list(self.symbols),
            "requested_start_ns": self.requested_start_ns,
            "requested_end_ns": self.requested_end_ns,
            "status": self.status.value,
            "missing_symbols": list(self.missing_symbols),
            "coverage": [item.to_dict() for item in self.coverages],
            "excluded_spans": [item.to_dict() for item in self.excluded_spans],
            "window_ids": [item.window_id for item in self.windows],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.payload(),
            "plan_id": self.plan_id,
            "windows": [item.to_dict() for item in self.windows],
            "common_start_ns": self.common_start_ns,
            "common_end_ns": self.common_end_ns,
        }

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CrossCurrencyWindowPlanV1":
        return cls(
            run_id=str(data.get("run_id", "")),
            ensemble_member_id=str(data.get("ensemble_member_id", "")),
            symbols=_string_tuple(data.get("symbols")),
            requested_start_ns=_strict_int(
                data.get("requested_start_ns"), "requested_start_ns"
            ),
            requested_end_ns=_strict_int(
                data.get("requested_end_ns"), "requested_end_ns"
            ),
            coverages=tuple(
                CrossCurrencySymbolCoverageV1.from_dict(item)
                for item in _mapping_sequence(data, "coverage")
            ),
            excluded_spans=tuple(
                CrossCurrencyExcludedSpanV1.from_dict(item)
                for item in _mapping_sequence(data, "excluded_spans")
            ),
            windows=tuple(
                ReconstructionWindowV1.from_dict(item)
                for item in _mapping_sequence(data, "windows")
            ),
            status=CrossCurrencyWindowPlanStatus(str(data.get("status", ""))),
            missing_symbols=_string_tuple(data.get("missing_symbols")),
            plan_id=str(data.get("plan_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "CrossCurrencyWindowPlanV1":
        return cls.from_dict(_json_mapping(text))


def plan_cross_currency_windows(
    run: ReconstructionRunV1,
    *,
    ensemble_member_id: str,
    requested_start_ns: int,
    requested_end_ns: int,
    window_size_ns: int,
    coverages: Iterable[CrossCurrencySymbolCoverageV1],
    left_halo_ns: int = 0,
    right_lookahead_ns: int = 0,
) -> CrossCurrencyWindowPlanV1:
    """Plan only the exact common coverage of the complete symbol group."""
    requested_start = _int64(requested_start_ns, "requested_start_ns")
    requested_end = _int64(requested_end_ns, "requested_end_ns")
    if requested_end <= requested_start:
        raise ValueError("requested_end_ns must exceed requested_start_ns")
    supplied: dict[str, CrossCurrencySymbolCoverageV1] = {}
    for coverage in coverages:
        if coverage.symbol in supplied:
            raise ValueError("duplicate symbol coverage")
        if coverage.symbol not in run.symbols:
            raise ValueError(
                "coverage symbol is outside the reconstruction run"
            )
        supplied[coverage.symbol] = coverage
    normalized = tuple(
        supplied.get(symbol, CrossCurrencySymbolCoverageV1.missing(symbol))
        for symbol in run.symbols
    )
    missing = tuple(
        item.symbol
        for item in normalized
        if item.status is CrossCurrencyCoverageStatus.MISSING
    )
    excluded: list[CrossCurrencyExcludedSpanV1] = []
    for item in normalized:
        if item.status is CrossCurrencyCoverageStatus.MISSING:
            excluded.append(
                CrossCurrencyExcludedSpanV1(
                    symbol=item.symbol,
                    start_ns=requested_start,
                    end_ns=requested_end,
                    reason=CrossCurrencyExcludedReason.MISSING_SYMBOL,
                )
            )
            continue
        assert item.start_ns is not None and item.end_ns is not None
        if item.start_ns > requested_start:
            _append_excluded_span(
                excluded,
                symbol=item.symbol,
                start_ns=requested_start,
                end_ns=min(item.start_ns, requested_end),
                reason=CrossCurrencyExcludedReason.SYMBOL_NOT_YET_AVAILABLE,
            )
        if item.end_ns < requested_end:
            _append_excluded_span(
                excluded,
                symbol=item.symbol,
                start_ns=max(item.end_ns, requested_start),
                end_ns=requested_end,
                reason=CrossCurrencyExcludedReason.SYMBOL_NO_LONGER_AVAILABLE,
            )
    if missing:
        return CrossCurrencyWindowPlanV1(
            run_id=run.run_id,
            ensemble_member_id=ensemble_member_id,
            symbols=run.symbols,
            requested_start_ns=requested_start,
            requested_end_ns=requested_end,
            coverages=normalized,
            excluded_spans=tuple(excluded),
            windows=(),
            status=CrossCurrencyWindowPlanStatus.REFUSED,
            missing_symbols=missing,
        )
    available = tuple(normalized)
    common_start = max(
        requested_start,
        *(cast(int, item.start_ns) for item in available),
    )
    common_end = min(
        requested_end,
        *(cast(int, item.end_ns) for item in available),
    )
    if common_end <= common_start:
        excluded.append(
            CrossCurrencyExcludedSpanV1(
                symbol="*",
                start_ns=requested_start,
                end_ns=requested_end,
                reason=CrossCurrencyExcludedReason.NO_COMMON_SUPPORT,
            )
        )
        return CrossCurrencyWindowPlanV1(
            run_id=run.run_id,
            ensemble_member_id=ensemble_member_id,
            symbols=run.symbols,
            requested_start_ns=requested_start,
            requested_end_ns=requested_end,
            coverages=normalized,
            excluded_spans=tuple(excluded),
            windows=(),
            status=CrossCurrencyWindowPlanStatus.REFUSED,
        )
    _append_excluded_span(
        excluded,
        symbol="*",
        start_ns=requested_start,
        end_ns=common_start,
        reason=CrossCurrencyExcludedReason.NO_COMMON_SUPPORT,
    )
    _append_excluded_span(
        excluded,
        symbol="*",
        start_ns=common_end,
        end_ns=requested_end,
        reason=CrossCurrencyExcludedReason.NO_COMMON_SUPPORT,
    )
    windows = plan_reconstruction_windows(
        run,
        ensemble_member_id=ensemble_member_id,
        start_ns=common_start,
        end_ns=common_end,
        window_size_ns=window_size_ns,
        left_halo_ns=left_halo_ns,
        right_lookahead_ns=right_lookahead_ns,
    )
    return CrossCurrencyWindowPlanV1(
        run_id=run.run_id,
        ensemble_member_id=ensemble_member_id,
        symbols=run.symbols,
        requested_start_ns=requested_start,
        requested_end_ns=requested_end,
        coverages=normalized,
        excluded_spans=tuple(excluded),
        windows=windows,
        status=CrossCurrencyWindowPlanStatus.PLANNED,
    )


@dataclass(frozen=True, slots=True)
class CrossCurrencyRelationshipV1:
    """One triangle or inverse algebra constraint and projection priority."""

    kind: CrossCurrencyRelationshipKind
    symbols: tuple[str, ...]
    projection_priority: tuple[str, ...]
    relationship_id: str = ""
    schema_version: str = CROSS_CURRENCY_RELATIONSHIP_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            CROSS_CURRENCY_RELATIONSHIP_SCHEMA_VERSION,
            "relationship",
        )
        object.__setattr__(
            self, "kind", CrossCurrencyRelationshipKind(self.kind)
        )
        symbols = tuple(_normalized_symbol(item) for item in self.symbols)
        expected_count = (
            3 if self.kind is CrossCurrencyRelationshipKind.TRIANGLE else 2
        )
        if len(symbols) != expected_count or len(set(symbols)) != len(symbols):
            raise ValueError("relationship symbol cardinality differs")
        object.__setattr__(self, "symbols", symbols)
        priority = tuple(
            _normalized_symbol(item) for item in self.projection_priority
        )
        if len(priority) != len(symbols) or set(priority) != set(symbols):
            raise ValueError(
                "projection priority must permute relationship symbols"
            )
        object.__setattr__(self, "projection_priority", priority)
        expected = _stable_id("cross-currency-relationship", self.payload())
        supplied = _optional_text(self.relationship_id)
        if supplied is not None and supplied != expected:
            raise ValueError("cross-currency relationship_id differs")
        object.__setattr__(self, "relationship_id", expected)

    @classmethod
    def triangle(
        cls,
        *,
        direct: str,
        numerator: str,
        denominator: str,
        projection_priority: Sequence[str] | None = None,
    ) -> "CrossCurrencyRelationshipV1":
        symbols = (direct, numerator, denominator)
        return cls(
            kind=CrossCurrencyRelationshipKind.TRIANGLE,
            symbols=symbols,
            projection_priority=tuple(projection_priority or symbols),
        )

    @classmethod
    def inverse(
        cls,
        *,
        left: str,
        right: str,
        projection_priority: Sequence[str] | None = None,
    ) -> "CrossCurrencyRelationshipV1":
        symbols = (left, right)
        return cls(
            kind=CrossCurrencyRelationshipKind.INVERSE,
            symbols=symbols,
            projection_priority=tuple(projection_priority or symbols),
        )

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "kind": self.kind.value,
            "symbols": list(self.symbols),
            "projection_priority": list(self.projection_priority),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "relationship_id": self.relationship_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "CrossCurrencyRelationshipV1":
        return cls(
            kind=CrossCurrencyRelationshipKind(str(data.get("kind", ""))),
            symbols=_string_tuple(data.get("symbols")),
            projection_priority=_string_tuple(data.get("projection_priority")),
            relationship_id=str(data.get("relationship_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CrossCurrencyReconciliationConfigV1:
    """Versioned deterministic projection and validation policy."""

    relationships: tuple[CrossCurrencyRelationshipV1, ...]
    max_projection_relative: float = (
        DEFAULT_CROSS_CURRENCY_MAX_PROJECTION_RELATIVE
    )
    residual_tolerance: float = DEFAULT_CROSS_CURRENCY_RESIDUAL_TOLERANCE
    spread_tolerance_multiplier: float = (
        DEFAULT_CROSS_CURRENCY_SPREAD_TOLERANCE_MULTIPLIER
    )
    rounding_digits: int = DEFAULT_CROSS_CURRENCY_ROUNDING_DIGITS
    config_id: str = ""
    schema_version: str = CROSS_CURRENCY_CONFIG_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            CROSS_CURRENCY_CONFIG_SCHEMA_VERSION,
            "reconciliation config",
        )
        relationships = tuple(
            sorted(self.relationships, key=lambda item: item.relationship_id)
        )
        if not relationships:
            raise ValueError("reconciliation config requires relationships")
        if len({item.relationship_id for item in relationships}) != len(
            relationships
        ):
            raise ValueError("duplicate reconciliation relationship")
        object.__setattr__(self, "relationships", relationships)
        for name in (
            "max_projection_relative",
            "residual_tolerance",
            "spread_tolerance_multiplier",
        ):
            value = _nonnegative_finite_float(getattr(self, name), name)
            object.__setattr__(self, name, value)
        if self.max_projection_relative > 1.0:
            raise ValueError("max_projection_relative cannot exceed one")
        if (
            isinstance(self.rounding_digits, bool)
            or not isinstance(self.rounding_digits, int)
            or not 6 <= self.rounding_digits <= 15
        ):
            raise ValueError("rounding_digits must be between 6 and 15")
        expected = _stable_id("cross-currency-config", self.payload())
        supplied = _optional_text(self.config_id)
        if supplied is not None and supplied != expected:
            raise ValueError("cross-currency config_id differs")
        object.__setattr__(self, "config_id", expected)

    @property
    def symbols(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                {
                    symbol
                    for relationship in self.relationships
                    for symbol in relationship.symbols
                }
            )
        )

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "engine_id": CROSS_CURRENCY_ENGINE_ID,
            "engine_version": CROSS_CURRENCY_ENGINE_VERSION,
            "relationships": [item.to_dict() for item in self.relationships],
            "max_projection_relative": self.max_projection_relative,
            "residual_tolerance": self.residual_tolerance,
            "spread_tolerance_multiplier": self.spread_tolerance_multiplier,
            "rounding_digits": self.rounding_digits,
            "join_policy": "exact_event_time_no_forward_fill",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "config_id": self.config_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "CrossCurrencyReconciliationConfigV1":
        return cls(
            relationships=tuple(
                CrossCurrencyRelationshipV1.from_dict(item)
                for item in _mapping_sequence(data, "relationships")
            ),
            max_projection_relative=float(
                data.get("max_projection_relative", 0.0)
            ),
            residual_tolerance=float(data.get("residual_tolerance", 0.0)),
            spread_tolerance_multiplier=float(
                data.get("spread_tolerance_multiplier", 0.0)
            ),
            rounding_digits=_strict_int(
                data.get("rounding_digits"), "rounding_digits"
            ),
            config_id=str(data.get("config_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def eurusd_triangle_reconciliation_config() -> (
    CrossCurrencyReconciliationConfigV1
):
    """Return the first certified EURUSD/GBPUSD/EURGBP relationship."""
    return CrossCurrencyReconciliationConfigV1(
        relationships=(
            CrossCurrencyRelationshipV1.triangle(
                direct="EURGBP",
                numerator="EURUSD",
                denominator="GBPUSD",
                projection_priority=("EURGBP", "EURUSD", "GBPUSD"),
            ),
        )
    )


@dataclass(frozen=True, slots=True)
class CrossCurrencyConditionV1:
    """One event-time interval used to stratify residual support."""

    start_ns: int
    end_ns: int
    session_key: str = "unclassified"
    event_key: str = "unclassified"
    feed_epoch_key: str = "unclassified"
    condition_id: str = ""
    schema_version: str = CROSS_CURRENCY_CONDITION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            CROSS_CURRENCY_CONDITION_SCHEMA_VERSION,
            "cross-currency condition",
        )
        start = _int64(self.start_ns, "start_ns")
        end = _int64(self.end_ns, "end_ns")
        if end <= start:
            raise ValueError("condition end_ns must exceed start_ns")
        object.__setattr__(self, "start_ns", start)
        object.__setattr__(self, "end_ns", end)
        for name in ("session_key", "event_key", "feed_epoch_key"):
            object.__setattr__(
                self, name, _normalized_key(getattr(self, name), name)
            )
        expected = _stable_id("cross-currency-condition", self.payload())
        supplied = _optional_text(self.condition_id)
        if supplied is not None and supplied != expected:
            raise ValueError("cross-currency condition_id differs")
        object.__setattr__(self, "condition_id", expected)

    def covers(self, timestamp_ns: int) -> bool:
        return self.start_ns <= timestamp_ns < self.end_ns

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "session_key": self.session_key,
            "event_key": self.event_key,
            "feed_epoch_key": self.feed_epoch_key,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "condition_id": self.condition_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CrossCurrencyConditionV1":
        return cls(
            start_ns=_strict_int(data.get("start_ns"), "start_ns"),
            end_ns=_strict_int(data.get("end_ns"), "end_ns"),
            session_key=str(data.get("session_key", "")),
            event_key=str(data.get("event_key", "")),
            feed_epoch_key=str(data.get("feed_epoch_key", "")),
            condition_id=str(data.get("condition_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CrossCurrencyProjectionLineageV1:
    """Content-bound evidence for one synthetic quote projection."""

    relationship_id: str
    symbol: str
    event_time_ns: int
    event_sequence: int
    input_event_id: str
    output_event_id: str
    input_content_sha256: str
    output_content_sha256: str
    original_bid: float
    original_ask: float
    output_bid: float
    output_ask: float
    pre_residual: float
    post_residual: float
    allowed_residual: float
    projection_relative: float
    condition_ids: tuple[str, ...]
    lineage_id: str = ""
    schema_version: str = CROSS_CURRENCY_PROJECTION_LINEAGE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            CROSS_CURRENCY_PROJECTION_LINEAGE_SCHEMA_VERSION,
            "projection lineage",
        )
        for name in (
            "relationship_id",
            "input_event_id",
            "output_event_id",
            "input_content_sha256",
            "output_content_sha256",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(self, "symbol", _normalized_symbol(self.symbol))
        object.__setattr__(
            self, "event_time_ns", _int64(self.event_time_ns, "event_time_ns")
        )
        if self.event_sequence < 0:
            raise ValueError("event_sequence must be non-negative")
        for name in (
            "original_bid",
            "original_ask",
            "output_bid",
            "output_ask",
            "pre_residual",
            "post_residual",
            "allowed_residual",
            "projection_relative",
        ):
            object.__setattr__(
                self, name, _nonnegative_finite_float(getattr(self, name), name)
            )
        if (
            self.original_ask < self.original_bid
            or self.output_ask < self.output_bid
        ):
            raise ValueError("projection lineage contains a negative spread")
        object.__setattr__(
            self, "condition_ids", _normalized_text_tuple(self.condition_ids)
        )
        expected = _stable_id("cross-currency-projection", self.payload())
        supplied = _optional_text(self.lineage_id)
        if supplied is not None and supplied != expected:
            raise ValueError("cross-currency projection lineage_id differs")
        object.__setattr__(self, "lineage_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "relationship_id": self.relationship_id,
            "symbol": self.symbol,
            "event_time_ns": self.event_time_ns,
            "event_sequence": self.event_sequence,
            "input_event_id": self.input_event_id,
            "output_event_id": self.output_event_id,
            "input_content_sha256": self.input_content_sha256,
            "output_content_sha256": self.output_content_sha256,
            "original_bid": self.original_bid,
            "original_ask": self.original_ask,
            "output_bid": self.output_bid,
            "output_ask": self.output_ask,
            "pre_residual": self.pre_residual,
            "post_residual": self.post_residual,
            "allowed_residual": self.allowed_residual,
            "projection_relative": self.projection_relative,
            "condition_ids": list(self.condition_ids),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "lineage_id": self.lineage_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "CrossCurrencyProjectionLineageV1":
        return cls(
            relationship_id=str(data.get("relationship_id", "")),
            symbol=str(data.get("symbol", "")),
            event_time_ns=_strict_int(
                data.get("event_time_ns"), "event_time_ns"
            ),
            event_sequence=_strict_int(
                data.get("event_sequence"), "event_sequence"
            ),
            input_event_id=str(data.get("input_event_id", "")),
            output_event_id=str(data.get("output_event_id", "")),
            input_content_sha256=str(data.get("input_content_sha256", "")),
            output_content_sha256=str(data.get("output_content_sha256", "")),
            original_bid=float(data.get("original_bid", 0.0)),
            original_ask=float(data.get("original_ask", 0.0)),
            output_bid=float(data.get("output_bid", 0.0)),
            output_ask=float(data.get("output_ask", 0.0)),
            pre_residual=float(data.get("pre_residual", 0.0)),
            post_residual=float(data.get("post_residual", 0.0)),
            allowed_residual=float(data.get("allowed_residual", 0.0)),
            projection_relative=float(data.get("projection_relative", 0.0)),
            condition_ids=_string_tuple(data.get("condition_ids")),
            lineage_id=str(data.get("lineage_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CrossCurrencyRelationshipSupportV1:
    """Bounded aggregate support and residuals for one relationship."""

    relationship_id: str
    support_count: int
    projected_count: int
    infeasible_count: int
    pre_residual_max: float
    pre_residual_mean: float
    post_residual_max: float
    post_residual_mean: float
    allowed_residual_max: float
    schema_version: str = CROSS_CURRENCY_RELATIONSHIP_SUPPORT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            CROSS_CURRENCY_RELATIONSHIP_SUPPORT_SCHEMA_VERSION,
            "relationship support",
        )
        object.__setattr__(
            self, "relationship_id", _required_text(self.relationship_id)
        )
        for name in ("support_count", "projected_count", "infeasible_count"):
            value = _nonnegative_int(getattr(self, name), name)
            object.__setattr__(self, name, value)
        if self.projected_count > self.support_count:
            raise ValueError("projected_count exceeds relationship support")
        if self.infeasible_count > self.support_count:
            raise ValueError("infeasible_count exceeds relationship support")
        for name in (
            "pre_residual_max",
            "pre_residual_mean",
            "post_residual_max",
            "post_residual_mean",
            "allowed_residual_max",
        ):
            object.__setattr__(
                self, name, _nonnegative_finite_float(getattr(self, name), name)
            )

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "relationship_id": self.relationship_id,
            "support_count": self.support_count,
            "projected_count": self.projected_count,
            "infeasible_count": self.infeasible_count,
            "pre_residual_max": self.pre_residual_max,
            "pre_residual_mean": self.pre_residual_mean,
            "post_residual_max": self.post_residual_max,
            "post_residual_mean": self.post_residual_mean,
            "allowed_residual_max": self.allowed_residual_max,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "CrossCurrencyRelationshipSupportV1":
        return cls(
            relationship_id=str(data.get("relationship_id", "")),
            support_count=_strict_int(
                data.get("support_count"), "support_count"
            ),
            projected_count=_strict_int(
                data.get("projected_count"), "projected_count"
            ),
            infeasible_count=_strict_int(
                data.get("infeasible_count"), "infeasible_count"
            ),
            pre_residual_max=float(data.get("pre_residual_max", 0.0)),
            pre_residual_mean=float(data.get("pre_residual_mean", 0.0)),
            post_residual_max=float(data.get("post_residual_max", 0.0)),
            post_residual_mean=float(data.get("post_residual_mean", 0.0)),
            allowed_residual_max=float(data.get("allowed_residual_max", 0.0)),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CrossCurrencyResidualSliceV1:
    """Relationship residual support stratified by one condition dimension."""

    relationship_id: str
    dimension: str
    key: str
    support_count: int
    projected_count: int
    infeasible_count: int
    pre_residual_max: float
    post_residual_max: float
    schema_version: str = CROSS_CURRENCY_RESIDUAL_SLICE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            CROSS_CURRENCY_RESIDUAL_SLICE_SCHEMA_VERSION,
            "residual slice",
        )
        object.__setattr__(
            self, "relationship_id", _required_text(self.relationship_id)
        )
        if self.dimension not in {"session", "event", "feed_epoch"}:
            raise ValueError("unsupported residual slice dimension")
        object.__setattr__(self, "key", _normalized_key(self.key, "key"))
        for name in ("support_count", "projected_count", "infeasible_count"):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        for name in ("pre_residual_max", "post_residual_max"):
            object.__setattr__(
                self, name, _nonnegative_finite_float(getattr(self, name), name)
            )

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "relationship_id": self.relationship_id,
            "dimension": self.dimension,
            "key": self.key,
            "support_count": self.support_count,
            "projected_count": self.projected_count,
            "infeasible_count": self.infeasible_count,
            "pre_residual_max": self.pre_residual_max,
            "post_residual_max": self.post_residual_max,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "CrossCurrencyResidualSliceV1":
        return cls(
            relationship_id=str(data.get("relationship_id", "")),
            dimension=str(data.get("dimension", "")),
            key=str(data.get("key", "")),
            support_count=_strict_int(
                data.get("support_count"), "support_count"
            ),
            projected_count=_strict_int(
                data.get("projected_count"), "projected_count"
            ),
            infeasible_count=_strict_int(
                data.get("infeasible_count"), "infeasible_count"
            ),
            pre_residual_max=float(data.get("pre_residual_max", 0.0)),
            post_residual_max=float(data.get("post_residual_max", 0.0)),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CrossCurrencyValidationReportV1:
    """Content-bound generation or mandatory post-broker validation."""

    run_id: str
    window_id: str
    synchronization_unit_id: str
    ensemble_member_id: str
    symbols: tuple[str, ...]
    config_id: str
    stage: CrossCurrencyValidationStage
    status: CrossCurrencyValidationStatus
    relationship_support: tuple[CrossCurrencyRelationshipSupportV1, ...]
    residual_slices: tuple[CrossCurrencyResidualSliceV1, ...]
    union_timestamp_count: int
    common_timestamp_count: int
    asynchronous_timestamp_count: int
    duplicate_timestamp_event_count: int
    stale_join_risk_count: int
    observed_event_count: int
    anchor_preserved: bool
    output_content_sha256: str
    failure_reasons: tuple[str, ...] = ()
    validation_id: str = ""
    schema_version: str = CROSS_CURRENCY_VALIDATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            CROSS_CURRENCY_VALIDATION_SCHEMA_VERSION,
            "cross-currency validation",
        )
        for name in (
            "run_id",
            "window_id",
            "synchronization_unit_id",
            "ensemble_member_id",
            "config_id",
            "output_content_sha256",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(self, "symbols", _normalized_symbols(self.symbols))
        object.__setattr__(
            self, "stage", CrossCurrencyValidationStage(self.stage)
        )
        object.__setattr__(
            self, "status", CrossCurrencyValidationStatus(self.status)
        )
        supports = tuple(
            sorted(
                self.relationship_support, key=lambda item: item.relationship_id
            )
        )
        object.__setattr__(self, "relationship_support", supports)
        slices = tuple(
            sorted(
                self.residual_slices,
                key=lambda item: (
                    item.relationship_id,
                    item.dimension,
                    item.key,
                ),
            )
        )
        if len(slices) > MAX_CROSS_CURRENCY_RESIDUAL_SLICES:
            raise ValueError("cross-currency residual slices exceed limit")
        object.__setattr__(self, "residual_slices", slices)
        for name in (
            "union_timestamp_count",
            "common_timestamp_count",
            "asynchronous_timestamp_count",
            "duplicate_timestamp_event_count",
            "stale_join_risk_count",
            "observed_event_count",
        ):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        reasons = _normalized_text_tuple(self.failure_reasons)
        if len(reasons) > MAX_CROSS_CURRENCY_FAILURE_REASONS:
            raise ValueError("cross-currency failure reasons exceed limit")
        object.__setattr__(self, "failure_reasons", reasons)
        if self.status is CrossCurrencyValidationStatus.PASSED and (
            reasons or not self.anchor_preserved
        ):
            raise ValueError("passing validation cannot retain failures")
        if self.status is CrossCurrencyValidationStatus.FAILED and not reasons:
            raise ValueError("failed validation requires a reason")
        expected = _stable_id("cross-currency-validation", self.payload())
        supplied = _optional_text(self.validation_id)
        if supplied is not None and supplied != expected:
            raise ValueError("cross-currency validation_id differs")
        object.__setattr__(self, "validation_id", expected)

    @property
    def passed(self) -> bool:
        return self.status is CrossCurrencyValidationStatus.PASSED

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "window_id": self.window_id,
            "synchronization_unit_id": self.synchronization_unit_id,
            "ensemble_member_id": self.ensemble_member_id,
            "symbols": list(self.symbols),
            "config_id": self.config_id,
            "stage": self.stage.value,
            "status": self.status.value,
            "relationship_support": [
                item.to_dict() for item in self.relationship_support
            ],
            "residual_slices": [
                item.to_dict() for item in self.residual_slices
            ],
            "union_timestamp_count": self.union_timestamp_count,
            "common_timestamp_count": self.common_timestamp_count,
            "asynchronous_timestamp_count": self.asynchronous_timestamp_count,
            "duplicate_timestamp_event_count": (
                self.duplicate_timestamp_event_count
            ),
            "stale_join_risk_count": self.stale_join_risk_count,
            "observed_event_count": self.observed_event_count,
            "anchor_preserved": self.anchor_preserved,
            "output_content_sha256": self.output_content_sha256,
            "failure_reasons": list(self.failure_reasons),
            "join_policy": "exact_event_time_no_forward_fill",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "validation_id": self.validation_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "CrossCurrencyValidationReportV1":
        return cls(
            run_id=str(data.get("run_id", "")),
            window_id=str(data.get("window_id", "")),
            synchronization_unit_id=str(
                data.get("synchronization_unit_id", "")
            ),
            ensemble_member_id=str(data.get("ensemble_member_id", "")),
            symbols=_string_tuple(data.get("symbols")),
            config_id=str(data.get("config_id", "")),
            stage=CrossCurrencyValidationStage(str(data.get("stage", ""))),
            status=CrossCurrencyValidationStatus(str(data.get("status", ""))),
            relationship_support=tuple(
                CrossCurrencyRelationshipSupportV1.from_dict(item)
                for item in _mapping_sequence(data, "relationship_support")
            ),
            residual_slices=tuple(
                CrossCurrencyResidualSliceV1.from_dict(item)
                for item in _mapping_sequence(data, "residual_slices")
            ),
            union_timestamp_count=_strict_int(
                data.get("union_timestamp_count"), "union_timestamp_count"
            ),
            common_timestamp_count=_strict_int(
                data.get("common_timestamp_count"), "common_timestamp_count"
            ),
            asynchronous_timestamp_count=_strict_int(
                data.get("asynchronous_timestamp_count"),
                "asynchronous_timestamp_count",
            ),
            duplicate_timestamp_event_count=_strict_int(
                data.get("duplicate_timestamp_event_count"),
                "duplicate_timestamp_event_count",
            ),
            stale_join_risk_count=_strict_int(
                data.get("stale_join_risk_count"), "stale_join_risk_count"
            ),
            observed_event_count=_strict_int(
                data.get("observed_event_count"), "observed_event_count"
            ),
            anchor_preserved=bool(data.get("anchor_preserved")),
            output_content_sha256=str(data.get("output_content_sha256", "")),
            failure_reasons=_string_tuple(data.get("failure_reasons")),
            validation_id=str(data.get("validation_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CrossCurrencyReconciledGroupV1:
    """Process-local all-symbol output plus bounded reconciliation evidence."""

    run_id: str
    window_id: str
    synchronization_unit_id: str
    ensemble_member_id: str
    symbols: tuple[str, ...]
    status: CrossCurrencyGroupStatus
    streams: tuple[SyntheticEventStreamV1, ...]
    missing_symbols: tuple[str, ...]
    input_stream_ids: dict[str, str]
    config: CrossCurrencyReconciliationConfigV1
    condition_ids: tuple[str, ...]
    projection_lineage: tuple[CrossCurrencyProjectionLineageV1, ...]
    generation_validation: CrossCurrencyValidationReportV1
    group_id: str = ""
    schema_version: str = CROSS_CURRENCY_GROUP_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            CROSS_CURRENCY_GROUP_SCHEMA_VERSION,
            "reconciled group",
        )
        for name in (
            "run_id",
            "window_id",
            "synchronization_unit_id",
            "ensemble_member_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        symbols = _normalized_symbols(self.symbols)
        object.__setattr__(self, "symbols", symbols)
        object.__setattr__(
            self, "status", CrossCurrencyGroupStatus(self.status)
        )
        streams = tuple(sorted(self.streams, key=lambda item: item.symbol))
        if len({item.symbol for item in streams}) != len(streams):
            raise ValueError("reconciled group contains duplicate streams")
        if any(
            item.symbol not in symbols
            or item.run_id != self.run_id
            or item.ensemble_member_id != self.ensemble_member_id
            for item in streams
        ):
            raise ValueError("reconciled stream differs from group scope")
        object.__setattr__(self, "streams", streams)
        missing = _normalized_symbols(self.missing_symbols, allow_empty=True)
        if set(missing) != set(symbols).difference(
            item.symbol for item in streams
        ):
            raise ValueError("missing symbols do not reconcile with streams")
        object.__setattr__(self, "missing_symbols", missing)
        input_ids = {
            _normalized_symbol(symbol): _required_text(stream_id)
            for symbol, stream_id in self.input_stream_ids.items()
        }
        if set(input_ids) != {item.symbol for item in streams}:
            raise ValueError("input stream IDs do not cover available streams")
        object.__setattr__(
            self, "input_stream_ids", dict(sorted(input_ids.items()))
        )
        if not isinstance(self.config, CrossCurrencyReconciliationConfigV1):
            raise ValueError("reconciled group requires a v1 config")
        object.__setattr__(
            self, "condition_ids", _normalized_text_tuple(self.condition_ids)
        )
        lineage = tuple(
            sorted(
                self.projection_lineage,
                key=lambda item: (
                    item.event_time_ns,
                    item.event_sequence,
                    item.symbol,
                    item.lineage_id,
                ),
            )
        )
        object.__setattr__(self, "projection_lineage", lineage)
        validation = self.generation_validation
        if (
            validation.stage is not CrossCurrencyValidationStage.GENERATION
            or validation.run_id != self.run_id
            or validation.window_id != self.window_id
            or validation.synchronization_unit_id
            != self.synchronization_unit_id
            or validation.ensemble_member_id != self.ensemble_member_id
            or validation.symbols != symbols
            or validation.config_id != self.config.config_id
        ):
            raise ValueError("generation validation differs from group scope")
        if self.status is CrossCurrencyGroupStatus.RECONCILED and (
            missing or not validation.passed or len(streams) != len(symbols)
        ):
            raise ValueError(
                "reconciled status requires a complete passing group"
            )
        if (
            self.status is CrossCurrencyGroupStatus.REFUSED
            and validation.passed
        ):
            raise ValueError(
                "refused group cannot have passing generation validation"
            )
        expected = _stable_id("cross-currency-group", self.payload())
        supplied = _optional_text(self.group_id)
        if supplied is not None and supplied != expected:
            raise ValueError("cross-currency group_id differs")
        object.__setattr__(self, "group_id", expected)

    @property
    def generation_ready(self) -> bool:
        return self.status is CrossCurrencyGroupStatus.RECONCILED

    @property
    def requires_post_broker_validation(self) -> bool:
        return True

    def stream_for(self, symbol: str) -> SyntheticEventStreamV1:
        wanted = _normalized_symbol(symbol)
        for stream in self.streams:
            if stream.symbol == wanted:
                return stream
        raise KeyError(wanted)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "window_id": self.window_id,
            "synchronization_unit_id": self.synchronization_unit_id,
            "ensemble_member_id": self.ensemble_member_id,
            "symbols": list(self.symbols),
            "status": self.status.value,
            "missing_symbols": list(self.missing_symbols),
            "input_stream_ids": dict(self.input_stream_ids),
            "output_stream_ids": {
                item.symbol: item.stream_id for item in self.streams
            },
            "output_content_sha256": _streams_content_sha256(self.streams),
            "config_id": self.config.config_id,
            "condition_ids": list(self.condition_ids),
            "projection_count": len(self.projection_lineage),
            "projection_content_sha256": _content_sha256(
                [item.to_dict() for item in self.projection_lineage]
            ),
            "generation_validation_id": self.generation_validation.validation_id,
            "post_broker_validation_required": True,
            "atomic_commit_unit": "complete_synchronization_unit",
        }

    def metadata(self) -> dict[str, JSONValue]:
        return {
            **self.payload(),
            "group_id": self.group_id,
            "event_rows_inline": False,
            "projection_rows_inline": False,
            "generation_validation": self.generation_validation.to_dict(),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.payload(),
            "group_id": self.group_id,
            "streams": [item.to_dict() for item in self.streams],
            "config": self.config.to_dict(),
            "projection_lineage": [
                item.to_dict() for item in self.projection_lineage
            ],
            "generation_validation": self.generation_validation.to_dict(),
        }

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "CrossCurrencyReconciledGroupV1":
        return cls(
            run_id=str(data.get("run_id", "")),
            window_id=str(data.get("window_id", "")),
            synchronization_unit_id=str(
                data.get("synchronization_unit_id", "")
            ),
            ensemble_member_id=str(data.get("ensemble_member_id", "")),
            symbols=_string_tuple(data.get("symbols")),
            status=CrossCurrencyGroupStatus(str(data.get("status", ""))),
            streams=tuple(
                SyntheticEventStreamV1.from_dict(item)
                for item in _mapping_sequence(data, "streams")
            ),
            missing_symbols=_string_tuple(data.get("missing_symbols")),
            input_stream_ids={
                str(key): str(value)
                for key, value in _mapping(data.get("input_stream_ids")).items()
            },
            config=CrossCurrencyReconciliationConfigV1.from_dict(
                _mapping(data.get("config"))
            ),
            condition_ids=_string_tuple(data.get("condition_ids")),
            projection_lineage=tuple(
                CrossCurrencyProjectionLineageV1.from_dict(item)
                for item in _mapping_sequence(data, "projection_lineage")
            ),
            generation_validation=CrossCurrencyValidationReportV1.from_dict(
                _mapping(data.get("generation_validation"))
            ),
            group_id=str(data.get("group_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "CrossCurrencyReconciledGroupV1":
        return cls.from_dict(_json_mapping(text))

    def validate_atomic_manifest(
        self,
        manifest: PartitionManifestV1,
        *,
        post_broker_validation: CrossCurrencyValidationReportV1,
    ) -> None:
        """Require a complete final validation before any group commit."""
        validate_cross_currency_atomic_manifest(
            window_scope=(
                self.run_id,
                self.window_id,
                self.synchronization_unit_id,
                self.ensemble_member_id,
                self.symbols,
            ),
            streams=self.streams,
            manifest=manifest,
            post_broker_validation=post_broker_validation,
        )


@dataclass(slots=True)
class _ResidualAccumulator:
    pre: list[float] = field(default_factory=list)
    post: list[float] = field(default_factory=list)
    allowed: list[float] = field(default_factory=list)
    projected_count: int = 0
    infeasible_count: int = 0


def reconcile_cross_currency_window(
    *,
    run: ReconstructionRunV1,
    window: ReconstructionWindowV1,
    streams: Mapping[str, SyntheticEventStreamV1 | None],
    config: CrossCurrencyReconciliationConfigV1,
    conditions: Iterable[CrossCurrencyConditionV1] = (),
) -> CrossCurrencyReconciledGroupV1:
    """Reconcile one complete event-time group with no forward-filled joins."""
    _validate_run_window_config(run, window, config)
    condition_tuple = _normalized_conditions(conditions)
    available, missing = _normalized_stream_inputs(run, window, streams)
    input_stream_ids = {
        symbol: stream.stream_id for symbol, stream in available.items()
    }
    original_observed = _observed_event_content(available.values())
    if missing:
        validation = _failed_validation(
            run=run,
            window=window,
            config=config,
            streams=tuple(available.values()),
            reasons=tuple(f"missing_symbol:{symbol}" for symbol in missing),
            anchor_preserved=True,
        )
        return CrossCurrencyReconciledGroupV1(
            run_id=run.run_id,
            window_id=window.window_id,
            synchronization_unit_id=window.synchronization_unit_id,
            ensemble_member_id=window.ensemble_member_id,
            symbols=run.symbols,
            status=CrossCurrencyGroupStatus.REFUSED,
            streams=tuple(available.values()),
            missing_symbols=missing,
            input_stream_ids=input_stream_ids,
            config=config,
            condition_ids=tuple(item.condition_id for item in condition_tuple),
            projection_lineage=(),
            generation_validation=validation,
        )

    events = {
        symbol: {item.event_id: item for item in stream.events}
        for symbol, stream in available.items()
    }
    accumulators: dict[str, _ResidualAccumulator] = {}
    slices: dict[tuple[str, str, str], _ResidualAccumulator] = defaultdict(
        _ResidualAccumulator
    )
    lineage: list[CrossCurrencyProjectionLineageV1] = []
    failures: list[str] = []

    for relationship in config.relationships:
        accumulator = _ResidualAccumulator()
        accumulators[relationship.relationship_id] = accumulator
        matches = _relationship_matches(
            relationship,
            available,
            events,
            window,
        )
        if not matches:
            failures.append(
                f"relationship_no_exact_event_time_support:"
                f"{relationship.relationship_id}"
            )
            continue
        for matched in matches:
            pre = _relationship_residual(relationship, matched)
            allowed = _allowed_residual(relationship, matched, config)
            projected = False
            infeasible = False
            post = pre
            if pre > config.residual_tolerance:
                target_symbol = _projection_symbol(relationship, matched)
                if target_symbol is None:
                    if pre > allowed:
                        infeasible = True
                else:
                    target_index = relationship.symbols.index(target_symbol)
                    original = matched[target_index]
                    target_bid, target_ask = _required_projection_quote(
                        relationship,
                        matched,
                        target_symbol,
                    )
                    projection_relative = max(
                        abs(target_bid - original.bid) / original.bid,
                        abs(target_ask - original.ask) / original.ask,
                    )
                    if projection_relative > config.max_projection_relative:
                        infeasible = True
                    else:
                        replacement = _project_quote(
                            original,
                            target_bid=target_bid,
                            target_ask=target_ask,
                            rounding_digits=config.rounding_digits,
                        )
                        if replacement is None:
                            infeasible = True
                        else:
                            projected_events = list(matched)
                            projected_events[target_index] = replacement
                            projected_tuple = tuple(projected_events)
                            post = _relationship_residual(
                                relationship, projected_tuple
                            )
                            post_allowed = _allowed_residual(
                                relationship, projected_tuple, config
                            )
                            if post > post_allowed:
                                infeasible = True
                            else:
                                events[target_symbol][
                                    original.event_id
                                ] = replacement
                                matched = projected_tuple
                                allowed = post_allowed
                                projected = True
                                condition_ids = tuple(
                                    item.condition_id
                                    for item in condition_tuple
                                    if item.covers(original.event_time_ns)
                                )
                                lineage.append(
                                    CrossCurrencyProjectionLineageV1(
                                        relationship_id=(
                                            relationship.relationship_id
                                        ),
                                        symbol=target_symbol,
                                        event_time_ns=original.event_time_ns,
                                        event_sequence=original.event_sequence,
                                        input_event_id=original.event_id,
                                        output_event_id=replacement.event_id,
                                        input_content_sha256=_event_content_sha256(
                                            original
                                        ),
                                        output_content_sha256=_event_content_sha256(
                                            replacement
                                        ),
                                        original_bid=original.bid,
                                        original_ask=original.ask,
                                        output_bid=replacement.bid,
                                        output_ask=replacement.ask,
                                        pre_residual=pre,
                                        post_residual=post,
                                        allowed_residual=allowed,
                                        projection_relative=(
                                            projection_relative
                                        ),
                                        condition_ids=condition_ids,
                                    )
                                )
            if infeasible:
                failures.append(
                    f"infeasible_relationship_point:"
                    f"{relationship.relationship_id}:"
                    f"{matched[0].event_time_ns}"
                )
                accumulator.infeasible_count += 1
            if projected:
                accumulator.projected_count += 1
            accumulator.pre.append(pre)
            accumulator.post.append(post)
            accumulator.allowed.append(allowed)
            for dimension, key in _condition_keys(
                matched[0].event_time_ns,
                matched,
                condition_tuple,
            ).items():
                slice_acc = slices[
                    (relationship.relationship_id, dimension, key)
                ]
                slice_acc.pre.append(pre)
                slice_acc.post.append(post)
                slice_acc.allowed.append(allowed)
                slice_acc.projected_count += int(projected)
                slice_acc.infeasible_count += int(infeasible)

    output_streams = tuple(
        SyntheticEventStreamV1(
            run_id=stream.run_id,
            ensemble_member_id=stream.ensemble_member_id,
            symbol=symbol,
            events=tuple(events[symbol].values()),
            source_version_ids=stream.source_version_ids,
        )
        for symbol, stream in sorted(available.items())
    )
    anchor_preserved = _anchors_preserved(original_observed, output_streams)
    if not anchor_preserved:
        failures.append("observed_anchor_content_changed")
    validation = _validation_report(
        run=run,
        window=window,
        config=config,
        streams=output_streams,
        stage=CrossCurrencyValidationStage.GENERATION,
        accumulators=accumulators,
        slices=slices,
        anchor_preserved=anchor_preserved,
        failures=failures,
    )
    status = (
        CrossCurrencyGroupStatus.RECONCILED
        if validation.passed
        else CrossCurrencyGroupStatus.REFUSED
    )
    return CrossCurrencyReconciledGroupV1(
        run_id=run.run_id,
        window_id=window.window_id,
        synchronization_unit_id=window.synchronization_unit_id,
        ensemble_member_id=window.ensemble_member_id,
        symbols=run.symbols,
        status=status,
        streams=output_streams,
        missing_symbols=(),
        input_stream_ids=input_stream_ids,
        config=config,
        condition_ids=tuple(item.condition_id for item in condition_tuple),
        projection_lineage=tuple(lineage),
        generation_validation=validation,
    )


def validate_cross_currency_output(
    *,
    run: ReconstructionRunV1,
    window: ReconstructionWindowV1,
    streams: Mapping[str, SyntheticEventStreamV1 | None],
    config: CrossCurrencyReconciliationConfigV1,
    stage: CrossCurrencyValidationStage,
    observed_anchors: Iterable[SyntheticEventV1],
    conditions: Iterable[CrossCurrencyConditionV1] = (),
) -> CrossCurrencyValidationReportV1:
    """Validate generation output or the mandatory post-broker output."""
    _validate_run_window_config(run, window, config)
    condition_tuple = _normalized_conditions(conditions)
    available, missing = _normalized_stream_inputs(run, window, streams)
    failures = [f"missing_symbol:{symbol}" for symbol in missing]
    anchor_map = {
        item.event_id: _event_content_sha256(item)
        for item in observed_anchors
        if item.origin is SyntheticEventOrigin.OBSERVED
    }
    anchor_preserved = _anchors_preserved(anchor_map, tuple(available.values()))
    if not anchor_preserved:
        failures.append("observed_anchor_content_changed")
    accumulators: dict[str, _ResidualAccumulator] = {}
    slices: dict[tuple[str, str, str], _ResidualAccumulator] = defaultdict(
        _ResidualAccumulator
    )
    event_maps = {
        symbol: {event.event_id: event for event in stream.events}
        for symbol, stream in available.items()
    }
    if not missing:
        for relationship in config.relationships:
            accumulator = _ResidualAccumulator()
            accumulators[relationship.relationship_id] = accumulator
            matches = _relationship_matches(
                relationship,
                available,
                event_maps,
                window,
            )
            if not matches:
                failures.append(
                    f"relationship_no_exact_event_time_support:"
                    f"{relationship.relationship_id}"
                )
                continue
            for matched in matches:
                residual = _relationship_residual(relationship, matched)
                allowed = _allowed_residual(relationship, matched, config)
                infeasible = residual > allowed
                if infeasible:
                    failures.append(
                        f"relationship_residual_exceeded:"
                        f"{relationship.relationship_id}:"
                        f"{matched[0].event_time_ns}"
                    )
                    accumulator.infeasible_count += 1
                accumulator.pre.append(residual)
                accumulator.post.append(residual)
                accumulator.allowed.append(allowed)
                for dimension, key in _condition_keys(
                    matched[0].event_time_ns,
                    matched,
                    condition_tuple,
                ).items():
                    slice_acc = slices[
                        (relationship.relationship_id, dimension, key)
                    ]
                    slice_acc.pre.append(residual)
                    slice_acc.post.append(residual)
                    slice_acc.allowed.append(allowed)
                    slice_acc.infeasible_count += int(infeasible)
    return _validation_report(
        run=run,
        window=window,
        config=config,
        streams=tuple(available.values()),
        stage=CrossCurrencyValidationStage(stage),
        accumulators=accumulators,
        slices=slices,
        anchor_preserved=anchor_preserved,
        failures=failures,
    )


def validate_cross_currency_atomic_manifest(
    *,
    window_scope: tuple[str, str, str, str, tuple[str, ...]],
    streams: Sequence[SyntheticEventStreamV1],
    manifest: PartitionManifestV1,
    post_broker_validation: CrossCurrencyValidationReportV1,
) -> None:
    """Reject partial or unvalidated all-symbol manifest publication."""
    run_id, window_id, sync_id, member_id, symbols = window_scope
    if (
        post_broker_validation.stage
        is not CrossCurrencyValidationStage.POST_BROKER
        or not post_broker_validation.passed
    ):
        raise ValueError(
            "atomic commit requires passing post-broker validation"
        )
    if (
        post_broker_validation.run_id != run_id
        or post_broker_validation.window_id != window_id
        or post_broker_validation.synchronization_unit_id != sync_id
        or post_broker_validation.ensemble_member_id != member_id
        or post_broker_validation.symbols != symbols
    ):
        raise ValueError("post-broker validation scope differs from commit")
    stream_map = {item.symbol: item for item in streams}
    if set(stream_map) != set(symbols):
        raise ValueError("atomic commit requires every synchronized symbol")
    if (
        manifest.run_id != run_id
        or manifest.window_id != window_id
        or manifest.synchronization_unit_id != sync_id
        or manifest.ensemble_member_id != member_id
        or manifest.symbols != symbols
    ):
        raise ValueError("partition manifest scope differs from group")
    expected_counts = {
        symbol: len(stream_map[symbol].events) for symbol in symbols
    }
    if manifest.symbol_event_counts != expected_counts:
        raise ValueError("partition manifest counts differ from final streams")
    if post_broker_validation.output_content_sha256 != _streams_content_sha256(
        tuple(stream_map[symbol] for symbol in symbols)
    ):
        raise ValueError("post-broker validation content differs from commit")


def cross_currency_series_inputs(
    group: CrossCurrencyReconciledGroupV1,
    *,
    period: str,
) -> tuple[CrossInstrumentSeriesInput, ...]:
    """Adapt a group to #331's millisecond diagnostic surface.

    Nanosecond event times are deterministically bucketed to HistData's
    millisecond diagnostic grain.  Collisions remain duplicate points and are
    surfaced by the existing diagnostic; they are never forward-filled.
    """
    normalized_period = _required_text(period)
    inputs: list[CrossInstrumentSeriesInput] = []
    for stream in group.streams:
        points = tuple(
            CrossInstrumentPointInput(
                timestamp_utc_ms=event.event_time_ns // 1_000_000,
                price=_midpoint(event),
                row_id=index,
                source_row_number=(event.source_row_id or 0),
                event_seq=event.event_sequence,
            )
            for index, event in enumerate(stream.events, start=1)
        )
        if not points:
            continue
        inputs.append(
            CrossInstrumentSeriesInput(
                symbol=stream.symbol,
                timeframe=TICK,
                period=normalized_period,
                series_id=f"{group.group_id}:{stream.symbol}",
                points=points,
                path=f"reconstructed://{group.group_id}/{stream.symbol}",
            )
        )
    return tuple(inputs)


def cross_currency_quality_report(
    group: CrossCurrencyReconciledGroupV1,
    *,
    period: str,
    tolerance: HistDataCrossInstrumentTolerance | None = None,
) -> QualityReport:
    """Run the existing #331 consistency rule over reconstructed streams."""
    rule = HistDataCrossInstrumentConsistencyRule(
        tolerance=tolerance or HistDataCrossInstrumentTolerance()
    )
    return rule.evaluate_series(
        cross_currency_series_inputs(group, period=period),
        metadata={
            "cross_currency_group_id": group.group_id,
            "validation_id": group.generation_validation.validation_id,
        },
    )


def _validation_report(
    *,
    run: ReconstructionRunV1,
    window: ReconstructionWindowV1,
    config: CrossCurrencyReconciliationConfigV1,
    streams: tuple[SyntheticEventStreamV1, ...],
    stage: CrossCurrencyValidationStage,
    accumulators: Mapping[str, _ResidualAccumulator],
    slices: Mapping[tuple[str, str, str], _ResidualAccumulator],
    anchor_preserved: bool,
    failures: Iterable[str],
) -> CrossCurrencyValidationReportV1:
    topology = _event_time_topology(streams, window)
    reasons = tuple(sorted(set(failures)))
    supports = tuple(
        _relationship_support(relationship_id, accumulator)
        for relationship_id, accumulator in sorted(accumulators.items())
    )
    residual_slices = tuple(
        CrossCurrencyResidualSliceV1(
            relationship_id=relationship_id,
            dimension=dimension,
            key=key,
            support_count=len(accumulator.pre),
            projected_count=accumulator.projected_count,
            infeasible_count=accumulator.infeasible_count,
            pre_residual_max=max(accumulator.pre, default=0.0),
            post_residual_max=max(accumulator.post, default=0.0),
        )
        for (relationship_id, dimension, key), accumulator in sorted(
            slices.items()
        )
    )
    return CrossCurrencyValidationReportV1(
        run_id=run.run_id,
        window_id=window.window_id,
        synchronization_unit_id=window.synchronization_unit_id,
        ensemble_member_id=window.ensemble_member_id,
        symbols=run.symbols,
        config_id=config.config_id,
        stage=stage,
        status=(
            CrossCurrencyValidationStatus.FAILED
            if reasons or not anchor_preserved
            else CrossCurrencyValidationStatus.PASSED
        ),
        relationship_support=supports,
        residual_slices=residual_slices,
        union_timestamp_count=topology["union"],
        common_timestamp_count=topology["common"],
        asynchronous_timestamp_count=topology["asynchronous"],
        duplicate_timestamp_event_count=topology["duplicates"],
        stale_join_risk_count=topology["stale_join_risks"],
        observed_event_count=sum(
            stream.observed_event_count for stream in streams
        ),
        anchor_preserved=anchor_preserved,
        output_content_sha256=_streams_content_sha256(streams),
        failure_reasons=reasons,
    )


def _failed_validation(
    *,
    run: ReconstructionRunV1,
    window: ReconstructionWindowV1,
    config: CrossCurrencyReconciliationConfigV1,
    streams: tuple[SyntheticEventStreamV1, ...],
    reasons: tuple[str, ...],
    anchor_preserved: bool,
) -> CrossCurrencyValidationReportV1:
    return _validation_report(
        run=run,
        window=window,
        config=config,
        streams=streams,
        stage=CrossCurrencyValidationStage.GENERATION,
        accumulators={},
        slices={},
        anchor_preserved=anchor_preserved,
        failures=reasons,
    )


def _relationship_support(
    relationship_id: str,
    accumulator: _ResidualAccumulator,
) -> CrossCurrencyRelationshipSupportV1:
    return CrossCurrencyRelationshipSupportV1(
        relationship_id=relationship_id,
        support_count=len(accumulator.pre),
        projected_count=accumulator.projected_count,
        infeasible_count=accumulator.infeasible_count,
        pre_residual_max=max(accumulator.pre, default=0.0),
        pre_residual_mean=_mean(accumulator.pre),
        post_residual_max=max(accumulator.post, default=0.0),
        post_residual_mean=_mean(accumulator.post),
        allowed_residual_max=max(accumulator.allowed, default=0.0),
    )


def _validate_run_window_config(
    run: ReconstructionRunV1,
    window: ReconstructionWindowV1,
    config: CrossCurrencyReconciliationConfigV1,
) -> None:
    if (
        window.run_id != run.run_id
        or window.ensemble_member_id not in run.ensemble_member_ids
        or window.symbols != run.symbols
    ):
        raise ValueError("cross-currency window differs from run")
    if not set(config.symbols).issubset(run.symbols):
        raise ValueError("cross-currency config symbols are outside run")


def _normalized_stream_inputs(
    run: ReconstructionRunV1,
    window: ReconstructionWindowV1,
    streams: Mapping[str, SyntheticEventStreamV1 | None],
) -> tuple[dict[str, SyntheticEventStreamV1], tuple[str, ...]]:
    normalized: dict[str, SyntheticEventStreamV1 | None] = {}
    for symbol, stream in streams.items():
        key = _normalized_symbol(symbol)
        if key in normalized:
            raise ValueError("duplicate cross-currency input symbol")
        if key not in run.symbols:
            raise ValueError("cross-currency input symbol is outside run")
        normalized[key] = stream
    available: dict[str, SyntheticEventStreamV1] = {}
    missing: list[str] = []
    for symbol in run.symbols:
        stream = normalized.get(symbol)
        if stream is None:
            missing.append(symbol)
            continue
        if (
            stream.symbol != symbol
            or stream.run_id != run.run_id
            or stream.ensemble_member_id != window.ensemble_member_id
        ):
            raise ValueError("cross-currency stream differs from window scope")
        available[symbol] = stream
    return dict(sorted(available.items())), tuple(missing)


def _normalized_conditions(
    conditions: Iterable[CrossCurrencyConditionV1],
) -> tuple[CrossCurrencyConditionV1, ...]:
    normalized = tuple(
        sorted(
            tuple(conditions),
            key=lambda item: (item.start_ns, item.end_ns, item.condition_id),
        )
    )
    if len(normalized) > MAX_CROSS_CURRENCY_CONDITIONS:
        raise ValueError("cross-currency conditions exceed limit")
    if len({item.condition_id for item in normalized}) != len(normalized):
        raise ValueError("duplicate cross-currency condition")
    return normalized


def _relationship_matches(
    relationship: CrossCurrencyRelationshipV1,
    streams: Mapping[str, SyntheticEventStreamV1],
    current: Mapping[str, Mapping[str, SyntheticEventV1]],
    window: ReconstructionWindowV1,
) -> tuple[tuple[SyntheticEventV1, ...], ...]:
    indexed: dict[str, dict[int, list[SyntheticEventV1]]] = {}
    for symbol in relationship.symbols:
        by_time: dict[int, list[SyntheticEventV1]] = defaultdict(list)
        for original in streams[symbol].events:
            event = current[symbol][original.event_id]
            if window.owns_event_time(event.event_time_ns):
                by_time[event.event_time_ns].append(event)
        for values in by_time.values():
            values.sort(key=lambda item: (item.event_sequence, item.event_id))
        indexed[symbol] = by_time
    common_times = set.intersection(
        *(set(indexed[symbol]) for symbol in relationship.symbols)
    )
    matches: list[tuple[SyntheticEventV1, ...]] = []
    for timestamp in sorted(common_times):
        count = min(
            len(indexed[symbol][timestamp]) for symbol in relationship.symbols
        )
        for ordinal in range(count):
            matches.append(
                tuple(
                    indexed[symbol][timestamp][ordinal]
                    for symbol in relationship.symbols
                )
            )
    return tuple(matches)


def _relationship_residual(
    relationship: CrossCurrencyRelationshipV1,
    events: Sequence[SyntheticEventV1],
) -> float:
    if relationship.kind is CrossCurrencyRelationshipKind.TRIANGLE:
        direct, numerator, denominator = events
        implied_bid = numerator.bid / denominator.ask
        implied_ask = numerator.ask / denominator.bid
        return float(
            max(
                abs(direct.bid - implied_bid) / implied_bid,
                abs(direct.ask - implied_ask) / implied_ask,
            )
        )
    left, right = events
    implied_bid = 1.0 / right.ask
    implied_ask = 1.0 / right.bid
    return float(
        max(
            abs(left.bid - implied_bid) / implied_bid,
            abs(left.ask - implied_ask) / implied_ask,
        )
    )


def _allowed_residual(
    relationship: CrossCurrencyRelationshipV1,
    events: Sequence[SyntheticEventV1],
    config: CrossCurrencyReconciliationConfigV1,
) -> float:
    relative_half_spreads = sum(
        ((item.ask - item.bid) / 2.0) / _midpoint(item) for item in events
    )
    return float(
        config.residual_tolerance
        + (config.spread_tolerance_multiplier * relative_half_spreads)
    )


def _projection_symbol(
    relationship: CrossCurrencyRelationshipV1,
    events: Sequence[SyntheticEventV1],
) -> str | None:
    by_symbol = {item.symbol: item for item in events}
    for symbol in relationship.projection_priority:
        if by_symbol[symbol].origin is SyntheticEventOrigin.SYNTHETIC:
            return symbol
    return None


def _required_projection_quote(
    relationship: CrossCurrencyRelationshipV1,
    events: Sequence[SyntheticEventV1],
    target_symbol: str,
) -> tuple[float, float]:
    by_symbol = {item.symbol: item for item in events}
    if relationship.kind is CrossCurrencyRelationshipKind.TRIANGLE:
        direct, numerator, denominator = relationship.symbols
        if target_symbol == direct:
            return (
                by_symbol[numerator].bid / by_symbol[denominator].ask,
                by_symbol[numerator].ask / by_symbol[denominator].bid,
            )
        if target_symbol == numerator:
            return (
                by_symbol[direct].bid * by_symbol[denominator].ask,
                by_symbol[direct].ask * by_symbol[denominator].bid,
            )
        return (
            by_symbol[numerator].ask / by_symbol[direct].ask,
            by_symbol[numerator].bid / by_symbol[direct].bid,
        )
    left, right = relationship.symbols
    other = right if target_symbol == left else left
    return (1.0 / by_symbol[other].ask, 1.0 / by_symbol[other].bid)


def _project_quote(
    event: SyntheticEventV1,
    *,
    target_bid: float,
    target_ask: float,
    rounding_digits: int,
) -> SyntheticEventV1 | None:
    bid = round(target_bid, rounding_digits)
    ask = round(target_ask, rounding_digits)
    if (
        not math.isfinite(bid)
        or not math.isfinite(ask)
        or bid <= 0.0
        or ask < bid
    ):
        return None
    return replace(event, bid=bid, ask=ask, event_id="")


def _condition_keys(
    timestamp_ns: int,
    events: Sequence[SyntheticEventV1],
    conditions: Sequence[CrossCurrencyConditionV1],
) -> dict[str, str]:
    matching = tuple(item for item in conditions if item.covers(timestamp_ns))
    session = _composite_key(item.session_key for item in matching)
    event = _composite_key(item.event_key for item in matching)
    condition_epochs = tuple(item.feed_epoch_key for item in matching)
    event_epochs = tuple(
        item.feed_epoch_id for item in events if item.feed_epoch_id is not None
    )
    epoch = _composite_key(condition_epochs or event_epochs)
    return {"session": session, "event": event, "feed_epoch": epoch}


def _event_time_topology(
    streams: Sequence[SyntheticEventStreamV1],
    window: ReconstructionWindowV1,
) -> dict[str, int]:
    by_symbol: dict[str, list[int]] = {
        stream.symbol: [
            item.event_time_ns
            for item in stream.events
            if window.owns_event_time(item.event_time_ns)
        ]
        for stream in streams
    }
    sets = {symbol: set(values) for symbol, values in by_symbol.items()}
    union = set().union(*sets.values()) if sets else set()
    common = set.intersection(*sets.values()) if sets else set()
    duplicates = sum(
        sum(count - 1 for count in Counter(values).values() if count > 1)
        for values in by_symbol.values()
    )
    return {
        "union": len(union),
        "common": len(common),
        "asynchronous": len(union.difference(common)),
        "duplicates": duplicates,
        "stale_join_risks": _stale_join_risk_count(sets),
    }


def _stale_join_risk_count(
    timestamps: Mapping[str, set[int]],
) -> int:
    union = sorted(set().union(*timestamps.values())) if timestamps else []
    risks = 0
    for symbol, present in timestamps.items():
        del symbol
        previous_present = False
        missing_run = 0
        for timestamp in union:
            if timestamp in present:
                if previous_present and missing_run >= 2:
                    risks += 1
                previous_present = True
                missing_run = 0
            elif previous_present:
                missing_run += 1
        if previous_present and missing_run >= 2:
            risks += 1
    return risks


def _observed_event_content(
    streams: Iterable[SyntheticEventStreamV1],
) -> dict[str, str]:
    return {
        event.event_id: _event_content_sha256(event)
        for stream in streams
        for event in stream.events
        if event.origin is SyntheticEventOrigin.OBSERVED
    }


def _anchors_preserved(
    expected: Mapping[str, str],
    streams: Sequence[SyntheticEventStreamV1],
) -> bool:
    actual = _observed_event_content(streams)
    return all(
        actual.get(event_id) == digest for event_id, digest in expected.items()
    )


def _streams_content_sha256(
    streams: Sequence[SyntheticEventStreamV1],
) -> str:
    return _content_sha256(
        [
            {
                "symbol": stream.symbol,
                "events": [item.to_dict() for item in stream.events],
            }
            for stream in sorted(streams, key=lambda item: item.symbol)
        ]
    )


def _event_content_sha256(event: SyntheticEventV1) -> str:
    return _content_sha256(event.to_dict())


def _content_sha256(value: JSONValue) -> str:
    payload = str(canonical_contract_json(value)).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        str(canonical_contract_json(dict(payload))).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _midpoint(event: SyntheticEventV1) -> float:
    return float((event.bid + event.ask) / 2.0)


def _mean(values: Sequence[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _composite_key(values: Iterable[str]) -> str:
    normalized = sorted(
        {
            _normalized_key(value, "condition key")
            for value in values
            if str(value or "").strip()
        }
    )
    return "+".join(normalized) if normalized else "unclassified"


def _append_excluded_span(
    spans: list[CrossCurrencyExcludedSpanV1],
    *,
    symbol: str,
    start_ns: int,
    end_ns: int,
    reason: CrossCurrencyExcludedReason,
) -> None:
    if end_ns <= start_ns:
        return
    spans.append(
        CrossCurrencyExcludedSpanV1(
            symbol=symbol,
            start_ns=start_ns,
            end_ns=end_ns,
            reason=reason,
        )
    )


def _normalized_symbols(
    values: Iterable[str], *, allow_empty: bool = False
) -> tuple[str, ...]:
    symbols = tuple(sorted({_normalized_symbol(item) for item in values}))
    if not symbols and not allow_empty:
        raise ValueError("symbol group cannot be empty")
    return symbols


def _normalized_symbol(value: str) -> str:
    symbol = "".join(
        character for character in str(value).lower() if character.isalnum()
    )
    if not symbol:
        raise ValueError("symbol cannot be empty")
    return symbol


def _normalized_key(value: str, name: str) -> str:
    normalized = str(value or "").strip().lower().replace(" ", "_")
    if not normalized:
        raise ValueError(f"{name} cannot be empty")
    return normalized


def _normalized_text_tuple(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted({_required_text(item) for item in values}))


def _required_text(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError("required text cannot be empty")
    return text


def _optional_text(value: str | None) -> str | None:
    text = str(value or "").strip()
    return text or None


def _int64(value: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    if not -(2**63) <= value <= (2**63 - 1):
        raise ValueError(f"{name} exceeds int64 bounds")
    return value


def _strict_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _nonnegative_int(value: int, name: str) -> int:
    result = _strict_int(value, name)
    if result < 0:
        raise ValueError(f"{name} must be non-negative")
    return result


def _nonnegative_finite_float(value: float, name: str) -> float:
    result = float(value)
    if not math.isfinite(result) or result < 0.0:
        raise ValueError(f"{name} must be finite and non-negative")
    return result


def _require_version(actual: str, expected: str, name: str) -> None:
    if actual != expected:
        raise ValueError(f"unsupported {name} schema")


def _string_tuple(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ValueError("expected a sequence of strings")
    return tuple(str(item) for item in value)


def _mapping(value: object) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("expected a mapping")
    return cast(Mapping[str, Any], value)


def _mapping_sequence(
    data: Mapping[str, Any], name: str
) -> tuple[Mapping[str, Any], ...]:
    value = data.get(name)
    if value is None:
        return ()
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ValueError(f"{name} must be a sequence")
    return tuple(_mapping(item) for item in value)


def _json_mapping(text: str) -> Mapping[str, Any]:
    import json

    loaded = json.loads(text)
    return _mapping(loaded)
