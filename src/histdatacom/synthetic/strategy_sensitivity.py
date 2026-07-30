"""Bounded strategy-sensitivity evidence for reconstructed market history.

The contracts in this module evaluate deterministic strategy and execution
assumptions across time-aligned market-data surfaces.  Results are descriptive
execution-sensitivity evidence, not profit claims, strategy optimization, or a
promotion decision for synthetic data.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict, deque
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol, TypeVar, cast, runtime_checkable

from histdatacom.runtime_contracts import JSONValue
from histdatacom.synthetic.bars import DerivedBarV1
from histdatacom.synthetic.benchmark import BenchmarkEventV1
from histdatacom.synthetic.contracts import (
    SyntheticEventV1,
    canonical_contract_json,
)
from histdatacom.synthetic.information import (
    InformationAuditReportV1,
    InformationMode,
)

STRATEGY_SPECIFICATION_SCHEMA_VERSION = "histdatacom.strategy-specification.v1"
STRATEGY_EXECUTION_SPECIFICATION_SCHEMA_VERSION = (
    "histdatacom.strategy-execution-specification.v1"
)
STRATEGY_EVALUATION_POLICY_SCHEMA_VERSION = (
    "histdatacom.strategy-evaluation-policy.v1"
)
STRATEGY_EVALUATION_CASE_SCHEMA_VERSION = (
    "histdatacom.strategy-evaluation-case.v1"
)
STRATEGY_EVALUATION_PLAN_SCHEMA_VERSION = (
    "histdatacom.strategy-evaluation-plan.v1"
)
STRATEGY_QUOTE_SCHEMA_VERSION = "histdatacom.strategy-quote.v1"
STRATEGY_SIGNAL_SCHEMA_VERSION = "histdatacom.strategy-signal.v1"
STRATEGY_SLICE_RESULT_SCHEMA_VERSION = "histdatacom.strategy-slice-result.v1"
STRATEGY_WINDOW_RESULT_SCHEMA_VERSION = "histdatacom.strategy-window-result.v1"
STRATEGY_UNCERTAINTY_SUMMARY_SCHEMA_VERSION = (
    "histdatacom.strategy-uncertainty-summary.v1"
)
STRATEGY_RESTORATION_RESULT_SCHEMA_VERSION = (
    "histdatacom.strategy-restoration-result.v1"
)
STRATEGY_SENSITIVITY_REPORT_SCHEMA_VERSION = (
    "histdatacom.strategy-sensitivity-report.v1"
)

REFERENCE_MOMENTUM_STRATEGY_ID = "reference-lagged-midpoint-momentum"
REFERENCE_MOMENTUM_STRATEGY_VERSION = "1.0.0"
STRATEGY_INVALID_FOR_BACKTEST_LABEL = "invalid-for-backtest"
STRATEGY_OUTPUT_MODE = "bounded-derived-metadata"

DEFAULT_STRATEGY_HORIZONS_NS = (
    1_000_000_000,
    5_000_000_000,
    60_000_000_000,
)
DEFAULT_STRATEGY_MAX_CASES = 256
DEFAULT_STRATEGY_MAX_QUOTES_PER_WINDOW = 1_000_000
DEFAULT_STRATEGY_MAX_SIGNALS_PER_WINDOW = 100_000
DEFAULT_STRATEGY_MAX_PENDING_SIGNALS = 10_000
DEFAULT_STRATEGY_MAX_SLICES = 4_096
DEFAULT_STRATEGY_MAX_PAYLOAD_BYTES = 4_194_304
DEFAULT_STRATEGY_ROUNDING_DIGITS = 12

MAX_STRATEGY_CASES = 2_048
MAX_STRATEGY_QUOTES_PER_WINDOW = 10_000_000
MAX_STRATEGY_SIGNALS_PER_WINDOW = 1_000_000
MAX_STRATEGY_PENDING_SIGNALS = 100_000
MAX_STRATEGY_SLICES = 32_768
MAX_STRATEGY_HORIZONS = 32
MAX_STRATEGY_PAYLOAD_BYTES = 16_777_216
MAX_STRATEGY_TEXT = 1_024
MAX_STRATEGY_PARAMETERS = 128
MAX_STRATEGY_PARAMETER_BYTES = 65_536
_BPS_SCALE = 10_000.0

_EnumT = TypeVar("_EnumT", bound=Enum)


class StrategySourceKind(str, Enum):
    """Supported market-data surfaces for one aligned evaluation case."""

    OBSERVED = "observed"
    DEGRADED_HOLDOUT = "degraded_holdout"
    RECONSTRUCTED = "reconstructed"
    UNCONDITIONED_RECONSTRUCTION = "unconditioned_reconstruction"
    BROKER_CONDITIONED = "broker_conditioned"
    DERIVED_BARS = "derived_bars"

    @classmethod
    def from_value(
        cls, value: str | "StrategySourceKind"
    ) -> "StrategySourceKind":
        """Return a strict normalized source kind."""
        return _enum_value(cls, value, "strategy source kind")


class StrategySide(str, Enum):
    """Normalized directional exposure for one reference signal."""

    LONG = "long"
    SHORT = "short"

    @classmethod
    def from_value(cls, value: str | "StrategySide") -> "StrategySide":
        """Return a strict normalized side."""
        return _enum_value(cls, value, "strategy side")

    @property
    def sign(self) -> float:
        """Return the arithmetic sign used for response calculations."""
        return 1.0 if self is StrategySide.LONG else -1.0


class StrategyWindowStatus(str, Enum):
    """Terminal status of one bounded aligned evaluation window."""

    COMPLETED = "completed"
    NO_TRADE = "no_trade"
    MISSING_SUPPORT = "missing_support"
    REFUSED = "refused"
    FAILED = "failed"

    @classmethod
    def from_value(
        cls, value: str | "StrategyWindowStatus"
    ) -> "StrategyWindowStatus":
        """Return a strict normalized terminal status."""
        return _enum_value(cls, value, "strategy window status")


class StrategyEvaluationFailure(RuntimeError):
    """A pluggable strategy refused to complete for a scientific reason."""


class StrategyResourceLimitError(StrategyEvaluationFailure):
    """Evaluation stopped before a configured resource bound was exceeded."""


@dataclass(frozen=True, slots=True)
class StrategySpecificationV1:
    """Versioned deterministic strategy logic and parameter assumptions."""

    method_id: str
    implementation_version: str
    parameters: Mapping[str, JSONValue] = field(default_factory=dict)
    specification_id: str = ""
    schema_version: str = STRATEGY_SPECIFICATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_version(
            self.schema_version,
            STRATEGY_SPECIFICATION_SCHEMA_VERSION,
            "strategy specification",
        )
        object.__setattr__(self, "method_id", _required_text(self.method_id))
        object.__setattr__(
            self,
            "implementation_version",
            _required_text(self.implementation_version),
        )
        parameters = _bounded_mapping(self.parameters, "strategy parameters")
        object.__setattr__(self, "parameters", parameters)
        expected = _stable_id("strategy-specification", self.identity_payload())
        supplied = _optional_text(self.specification_id)
        if supplied is not None and supplied != expected:
            raise ValueError("strategy specification_id differs from content")
        object.__setattr__(self, "specification_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return fields used for deterministic specification identity."""
        return {
            "schema_version": self.schema_version,
            "method_id": self.method_id,
            "implementation_version": self.implementation_version,
            "parameters": dict(self.parameters),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible strategy metadata."""
        return {
            **self.identity_payload(),
            "specification_id": self.specification_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "StrategySpecificationV1":
        """Restore and verify one strategy specification."""
        _require_schema(data, STRATEGY_SPECIFICATION_SCHEMA_VERSION)
        return cls(
            method_id=str(data.get("method_id", "")),
            implementation_version=str(data.get("implementation_version", "")),
            parameters=_mapping(data.get("parameters"), "parameters"),
            specification_id=str(data.get("specification_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class StrategyExecutionSpecificationV1:
    """Versioned quote-crossing, latency, slippage, and cost assumptions."""

    entry_latency_ns: int = 0
    max_execution_wait_ns: int = 1_000_000_000
    slippage_bps_per_side: float = 0.0
    fixed_cost_bps_per_side: float = 0.0
    price_semantics: str = "cross_bid_ask"
    exposure_semantics: str = "normalized_unit_exposure"
    specification_id: str = ""
    schema_version: str = STRATEGY_EXECUTION_SPECIFICATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_version(
            self.schema_version,
            STRATEGY_EXECUTION_SPECIFICATION_SCHEMA_VERSION,
            "strategy execution specification",
        )
        object.__setattr__(
            self,
            "entry_latency_ns",
            _nonnegative_int(self.entry_latency_ns, "entry_latency_ns"),
        )
        object.__setattr__(
            self,
            "max_execution_wait_ns",
            _positive_int(self.max_execution_wait_ns, "max_execution_wait_ns"),
        )
        for name in ("slippage_bps_per_side", "fixed_cost_bps_per_side"):
            value = _nonnegative_float(getattr(self, name), name)
            if value > 10_000.0:
                raise ValueError(f"{name} exceeds the bounded assumption range")
            object.__setattr__(self, name, value)
        if self.price_semantics != "cross_bid_ask":
            raise ValueError("version one execution must cross bid/ask")
        if self.exposure_semantics != "normalized_unit_exposure":
            raise ValueError(
                "version one forbids currency profit/notional claims"
            )
        expected = _stable_id("strategy-execution", self.identity_payload())
        supplied = _optional_text(self.specification_id)
        if supplied is not None and supplied != expected:
            raise ValueError("execution specification_id differs from content")
        object.__setattr__(self, "specification_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return fields used for deterministic execution identity."""
        return {
            "schema_version": self.schema_version,
            "entry_latency_ns": self.entry_latency_ns,
            "max_execution_wait_ns": self.max_execution_wait_ns,
            "slippage_bps_per_side": self.slippage_bps_per_side,
            "fixed_cost_bps_per_side": self.fixed_cost_bps_per_side,
            "price_semantics": self.price_semantics,
            "exposure_semantics": self.exposure_semantics,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible execution assumptions."""
        return {
            **self.identity_payload(),
            "specification_id": self.specification_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "StrategyExecutionSpecificationV1":
        """Restore and verify execution assumptions."""
        _require_schema(data, STRATEGY_EXECUTION_SPECIFICATION_SCHEMA_VERSION)
        return cls(
            entry_latency_ns=_strict_int(
                data.get("entry_latency_ns"), "entry_latency_ns"
            ),
            max_execution_wait_ns=_strict_int(
                data.get("max_execution_wait_ns"), "max_execution_wait_ns"
            ),
            slippage_bps_per_side=_finite_float(
                data.get("slippage_bps_per_side"), "slippage_bps_per_side"
            ),
            fixed_cost_bps_per_side=_finite_float(
                data.get("fixed_cost_bps_per_side"), "fixed_cost_bps_per_side"
            ),
            price_semantics=str(data.get("price_semantics", "")),
            exposure_semantics=str(data.get("exposure_semantics", "")),
            specification_id=str(data.get("specification_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class StrategyEvaluationPolicyV1:
    """Multiple horizons plus hard streaming and report resource bounds."""

    horizons_ns: tuple[int, ...] = DEFAULT_STRATEGY_HORIZONS_NS
    max_cases: int = DEFAULT_STRATEGY_MAX_CASES
    max_quotes_per_window: int = DEFAULT_STRATEGY_MAX_QUOTES_PER_WINDOW
    max_signals_per_window: int = DEFAULT_STRATEGY_MAX_SIGNALS_PER_WINDOW
    max_pending_signals: int = DEFAULT_STRATEGY_MAX_PENDING_SIGNALS
    max_slices: int = DEFAULT_STRATEGY_MAX_SLICES
    max_payload_bytes: int = DEFAULT_STRATEGY_MAX_PAYLOAD_BYTES
    rounding_digits: int = DEFAULT_STRATEGY_ROUNDING_DIGITS
    policy_id: str = ""
    schema_version: str = STRATEGY_EVALUATION_POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_version(
            self.schema_version,
            STRATEGY_EVALUATION_POLICY_SCHEMA_VERSION,
            "strategy evaluation policy",
        )
        horizons = tuple(
            _positive_int(item, "strategy horizon") for item in self.horizons_ns
        )
        if not horizons or len(horizons) > MAX_STRATEGY_HORIZONS:
            raise ValueError("strategy horizon count is outside bounds")
        if tuple(sorted(set(horizons))) != horizons:
            raise ValueError("strategy horizons must be unique and increasing")
        object.__setattr__(self, "horizons_ns", horizons)
        for name, maximum in (
            ("max_cases", MAX_STRATEGY_CASES),
            ("max_quotes_per_window", MAX_STRATEGY_QUOTES_PER_WINDOW),
            ("max_signals_per_window", MAX_STRATEGY_SIGNALS_PER_WINDOW),
            ("max_pending_signals", MAX_STRATEGY_PENDING_SIGNALS),
            ("max_slices", MAX_STRATEGY_SLICES),
            ("max_payload_bytes", MAX_STRATEGY_PAYLOAD_BYTES),
        ):
            value = _positive_int(getattr(self, name), name)
            if value > maximum:
                raise ValueError(f"{name} exceeds the hard maximum")
            object.__setattr__(self, name, value)
        rounding = _bounded_int(self.rounding_digits, "rounding_digits", 0, 15)
        object.__setattr__(self, "rounding_digits", rounding)
        expected = _stable_id(
            "strategy-evaluation-policy", self.identity_payload()
        )
        supplied = _optional_text(self.policy_id)
        if supplied is not None and supplied != expected:
            raise ValueError("strategy policy_id differs from content")
        object.__setattr__(self, "policy_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return fields used for deterministic policy identity."""
        return {
            "schema_version": self.schema_version,
            "horizons_ns": list(self.horizons_ns),
            "max_cases": self.max_cases,
            "max_quotes_per_window": self.max_quotes_per_window,
            "max_signals_per_window": self.max_signals_per_window,
            "max_pending_signals": self.max_pending_signals,
            "max_slices": self.max_slices,
            "max_payload_bytes": self.max_payload_bytes,
            "rounding_digits": self.rounding_digits,
            "retention_semantics": "online_aggregates_only",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible evaluation policy."""
        return {**self.identity_payload(), "policy_id": self.policy_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "StrategyEvaluationPolicyV1":
        """Restore and verify an evaluation policy."""
        _require_schema(data, STRATEGY_EVALUATION_POLICY_SCHEMA_VERSION)
        _require_derived(data, "retention_semantics", "online_aggregates_only")
        return cls(
            horizons_ns=tuple(
                _strict_int(item, "strategy horizon")
                for item in _sequence(data.get("horizons_ns"), "horizons_ns")
            ),
            max_cases=_strict_int(data.get("max_cases"), "max_cases"),
            max_quotes_per_window=_strict_int(
                data.get("max_quotes_per_window"), "max_quotes_per_window"
            ),
            max_signals_per_window=_strict_int(
                data.get("max_signals_per_window"), "max_signals_per_window"
            ),
            max_pending_signals=_strict_int(
                data.get("max_pending_signals"), "max_pending_signals"
            ),
            max_slices=_strict_int(data.get("max_slices"), "max_slices"),
            max_payload_bytes=_strict_int(
                data.get("max_payload_bytes"), "max_payload_bytes"
            ),
            rounding_digits=_strict_int(
                data.get("rounding_digits"), "rounding_digits"
            ),
            policy_id=str(data.get("policy_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class StrategyEvaluationCaseV1:
    """One source surface bound to an exact aligned half-open time window."""

    run_id: str
    alignment_window_id: str
    source_kind: StrategySourceKind
    source_artifact_id: str
    symbol: str
    start_ns: int
    end_ns: int
    information_mode: InformationMode
    information_manifest_id: str
    information_audit_id: str
    ensemble_member_id: str
    broker_profile_id: str | None = None
    source_scope: str | None = None
    bar_interval_code: str | None = None
    invalid_for_backtest_reason: str | None = None
    case_id: str = ""
    schema_version: str = STRATEGY_EVALUATION_CASE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_version(
            self.schema_version,
            STRATEGY_EVALUATION_CASE_SCHEMA_VERSION,
            "strategy evaluation case",
        )
        for name in (
            "run_id",
            "alignment_window_id",
            "source_artifact_id",
            "information_manifest_id",
            "information_audit_id",
            "ensemble_member_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self, "source_kind", StrategySourceKind.from_value(self.source_kind)
        )
        object.__setattr__(self, "symbol", _normalized_symbol(self.symbol))
        start = _strict_int(self.start_ns, "start_ns")
        end = _strict_int(self.end_ns, "end_ns")
        if start >= end:
            raise ValueError("strategy case requires start_ns < end_ns")
        object.__setattr__(self, "start_ns", start)
        object.__setattr__(self, "end_ns", end)
        object.__setattr__(
            self,
            "information_mode",
            InformationMode.from_value(self.information_mode),
        )
        for name in (
            "broker_profile_id",
            "source_scope",
            "bar_interval_code",
            "invalid_for_backtest_reason",
        ):
            object.__setattr__(self, name, _optional_text(getattr(self, name)))
        if self.source_kind is StrategySourceKind.BROKER_CONDITIONED:
            if self.broker_profile_id is None:
                raise ValueError("broker-conditioned case requires a profile")
        if self.source_kind is StrategySourceKind.DERIVED_BARS:
            if self.source_scope is None or self.bar_interval_code is None:
                raise ValueError("derived-bar case requires scope and interval")
        elif self.bar_interval_code is not None:
            raise ValueError("bar_interval_code is only valid for derived bars")
        if (
            self.information_mode is InformationMode.EX_POST_RECONSTRUCTION
            and self.invalid_for_backtest_reason is None
        ):
            raise ValueError(
                "ex-post strategy case requires invalid-for-backtest reason"
            )
        expected = _stable_id(
            "strategy-evaluation-case", self.identity_payload()
        )
        supplied = _optional_text(self.case_id)
        if supplied is not None and supplied != expected:
            raise ValueError("strategy case_id differs from content")
        object.__setattr__(self, "case_id", expected)

    @property
    def valid_for_backtest(self) -> bool:
        """Return whether this case can support prospective backtest claims."""
        return self.invalid_for_backtest_reason is None

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return fields used for deterministic case identity."""
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "alignment_window_id": self.alignment_window_id,
            "source_kind": self.source_kind.value,
            "source_artifact_id": self.source_artifact_id,
            "symbol": self.symbol,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "interval": "[start_ns,end_ns)",
            "information_mode": self.information_mode.value,
            "information_manifest_id": self.information_manifest_id,
            "information_audit_id": self.information_audit_id,
            "ensemble_member_id": self.ensemble_member_id,
            "broker_profile_id": self.broker_profile_id,
            "source_scope": self.source_scope,
            "bar_interval_code": self.bar_interval_code,
            "invalid_for_backtest_reason": self.invalid_for_backtest_reason,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible case metadata."""
        return {
            **self.identity_payload(),
            "case_id": self.case_id,
            "valid_for_backtest": self.valid_for_backtest,
            "backtest_label": (
                None
                if self.valid_for_backtest
                else STRATEGY_INVALID_FOR_BACKTEST_LABEL
            ),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "StrategyEvaluationCaseV1":
        """Restore and verify one aligned evaluation case."""
        _require_schema(data, STRATEGY_EVALUATION_CASE_SCHEMA_VERSION)
        case = cls(
            run_id=str(data.get("run_id", "")),
            alignment_window_id=str(data.get("alignment_window_id", "")),
            source_kind=StrategySourceKind.from_value(
                str(data.get("source_kind", ""))
            ),
            source_artifact_id=str(data.get("source_artifact_id", "")),
            symbol=str(data.get("symbol", "")),
            start_ns=_strict_int(data.get("start_ns"), "start_ns"),
            end_ns=_strict_int(data.get("end_ns"), "end_ns"),
            information_mode=InformationMode.from_value(
                str(data.get("information_mode", ""))
            ),
            information_manifest_id=str(
                data.get("information_manifest_id", "")
            ),
            information_audit_id=str(data.get("information_audit_id", "")),
            ensemble_member_id=str(data.get("ensemble_member_id", "")),
            broker_profile_id=_mapping_optional_text(data, "broker_profile_id"),
            source_scope=_mapping_optional_text(data, "source_scope"),
            bar_interval_code=_mapping_optional_text(data, "bar_interval_code"),
            invalid_for_backtest_reason=_mapping_optional_text(
                data, "invalid_for_backtest_reason"
            ),
            case_id=str(data.get("case_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        _require_derived(data, "interval", "[start_ns,end_ns)")
        _require_derived(data, "valid_for_backtest", case.valid_for_backtest)
        _require_derived(
            data,
            "backtest_label",
            (
                None
                if case.valid_for_backtest
                else STRATEGY_INVALID_FOR_BACKTEST_LABEL
            ),
        )
        return case


@dataclass(frozen=True, slots=True)
class StrategyEvaluationPlanV1:
    """One comparison plan applying identical logic to aligned source cases."""

    run_id: str
    strategy: StrategySpecificationV1
    execution: StrategyExecutionSpecificationV1
    policy: StrategyEvaluationPolicyV1
    cases: tuple[StrategyEvaluationCaseV1, ...]
    invalid_for_backtest_reason: str | None = None
    plan_id: str = ""
    schema_version: str = STRATEGY_EVALUATION_PLAN_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_version(
            self.schema_version,
            STRATEGY_EVALUATION_PLAN_SCHEMA_VERSION,
            "strategy evaluation plan",
        )
        object.__setattr__(self, "run_id", _required_text(self.run_id))
        if not isinstance(self.strategy, StrategySpecificationV1):
            raise TypeError(
                "strategy plan requires a v1 strategy specification"
            )
        if not isinstance(self.execution, StrategyExecutionSpecificationV1):
            raise TypeError(
                "strategy plan requires a v1 execution specification"
            )
        if not isinstance(self.policy, StrategyEvaluationPolicyV1):
            raise TypeError("strategy plan requires a v1 evaluation policy")
        cases = tuple(sorted(self.cases, key=lambda item: item.case_id))
        if len(cases) < 2:
            raise ValueError(
                "strategy comparison plan requires at least two cases"
            )
        if len(cases) > self.policy.max_cases:
            raise ValueError("strategy cases exceed plan policy")
        if any(
            not isinstance(item, StrategyEvaluationCaseV1) for item in cases
        ):
            raise TypeError("strategy cases must use the v1 contract")
        if len({item.case_id for item in cases}) != len(cases):
            raise ValueError("strategy case identities must be unique")
        if any(item.run_id != self.run_id for item in cases):
            raise ValueError("strategy case run differs from plan")
        _validate_alignment_groups(cases)
        object.__setattr__(self, "cases", cases)
        invalid = _optional_text(self.invalid_for_backtest_reason)
        modes = {item.information_mode for item in cases}
        if len(modes) > 1 and invalid is None:
            raise ValueError(
                "mixed information modes require an explicit invalid-for-backtest label"
            )
        object.__setattr__(self, "invalid_for_backtest_reason", invalid)
        expected = _stable_id(
            "strategy-evaluation-plan", self.identity_payload()
        )
        supplied = _optional_text(self.plan_id)
        if supplied is not None and supplied != expected:
            raise ValueError("strategy plan_id differs from content")
        object.__setattr__(self, "plan_id", expected)

    @property
    def valid_for_backtest(self) -> bool:
        """Return whether every case and the comparison are prospective-valid."""
        return self.invalid_for_backtest_reason is None and all(
            item.valid_for_backtest for item in self.cases
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return fields used for deterministic comparison-plan identity."""
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "strategy": self.strategy.to_dict(),
            "execution": self.execution.to_dict(),
            "policy": self.policy.to_dict(),
            "cases": [item.to_dict() for item in self.cases],
            "invalid_for_backtest_reason": self.invalid_for_backtest_reason,
            "comparison_semantics": "identical_logic_time_aligned_windows",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible plan metadata."""
        return {
            **self.identity_payload(),
            "plan_id": self.plan_id,
            "valid_for_backtest": self.valid_for_backtest,
            "backtest_label": (
                None
                if self.valid_for_backtest
                else STRATEGY_INVALID_FOR_BACKTEST_LABEL
            ),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "StrategyEvaluationPlanV1":
        """Restore and verify one comparison plan."""
        _require_schema(data, STRATEGY_EVALUATION_PLAN_SCHEMA_VERSION)
        plan = cls(
            run_id=str(data.get("run_id", "")),
            strategy=StrategySpecificationV1.from_dict(
                _mapping(data.get("strategy"), "strategy")
            ),
            execution=StrategyExecutionSpecificationV1.from_dict(
                _mapping(data.get("execution"), "execution")
            ),
            policy=StrategyEvaluationPolicyV1.from_dict(
                _mapping(data.get("policy"), "policy")
            ),
            cases=tuple(
                StrategyEvaluationCaseV1.from_dict(item)
                for item in _mapping_sequence(data.get("cases"), "cases")
            ),
            invalid_for_backtest_reason=_mapping_optional_text(
                data, "invalid_for_backtest_reason"
            ),
            plan_id=str(data.get("plan_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        _require_derived(
            data,
            "comparison_semantics",
            "identical_logic_time_aligned_windows",
        )
        _require_derived(data, "valid_for_backtest", plan.valid_for_backtest)
        _require_derived(
            data,
            "backtest_label",
            (
                None
                if plan.valid_for_backtest
                else STRATEGY_INVALID_FOR_BACKTEST_LABEL
            ),
        )
        return plan


@dataclass(frozen=True, slots=True)
class StrategyQuoteV1:
    """Minimal normalized quote and regime context consumed by strategies."""

    source_event_id: str
    symbol: str
    event_time_ns: int
    event_sequence: int
    bid: float
    ask: float
    epoch_id: str
    session: str
    event_state: str
    sparsity: str
    ensemble_member_id: str | None = None
    broker_profile_id: str | None = None
    source_scope: str | None = None
    bar_interval_code: str | None = None
    quote_id: str = ""
    schema_version: str = STRATEGY_QUOTE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_version(
            self.schema_version, STRATEGY_QUOTE_SCHEMA_VERSION, "strategy quote"
        )
        object.__setattr__(
            self, "source_event_id", _required_text(self.source_event_id)
        )
        object.__setattr__(self, "symbol", _normalized_symbol(self.symbol))
        object.__setattr__(
            self,
            "event_time_ns",
            _strict_int(self.event_time_ns, "event_time_ns"),
        )
        sequence = _nonnegative_int(self.event_sequence, "event_sequence")
        object.__setattr__(self, "event_sequence", sequence)
        bid = _positive_float(self.bid, "bid")
        ask = _positive_float(self.ask, "ask")
        if ask < bid:
            raise ValueError("strategy quote ask is below bid")
        object.__setattr__(self, "bid", bid)
        object.__setattr__(self, "ask", ask)
        for name in ("epoch_id", "session", "event_state", "sparsity"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self, "ensemble_member_id", _optional_text(self.ensemble_member_id)
        )
        object.__setattr__(
            self, "broker_profile_id", _optional_text(self.broker_profile_id)
        )
        object.__setattr__(
            self, "source_scope", _optional_text(self.source_scope)
        )
        object.__setattr__(
            self, "bar_interval_code", _optional_text(self.bar_interval_code)
        )
        if (self.source_scope is None) != (self.bar_interval_code is None):
            raise ValueError(
                "strategy quote bar scope and interval must be paired"
            )
        expected = _stable_id("strategy-quote", self.identity_payload())
        supplied = _optional_text(self.quote_id)
        if supplied is not None and supplied != expected:
            raise ValueError("strategy quote_id differs from content")
        object.__setattr__(self, "quote_id", expected)

    @property
    def mid(self) -> float:
        """Return the quote midpoint."""
        return (self.bid + self.ask) / 2.0

    @property
    def spread(self) -> float:
        """Return the non-negative quoted spread."""
        return self.ask - self.bid

    @property
    def order_key(self) -> tuple[int, int, str]:
        """Return the stable within-window ordering key."""
        return (self.event_time_ns, self.event_sequence, self.quote_id)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return fields used for deterministic quote identity."""
        return {
            "schema_version": self.schema_version,
            "source_event_id": self.source_event_id,
            "symbol": self.symbol,
            "event_time_ns": self.event_time_ns,
            "event_sequence": self.event_sequence,
            "bid": self.bid,
            "ask": self.ask,
            "epoch_id": self.epoch_id,
            "session": self.session,
            "event_state": self.event_state,
            "sparsity": self.sparsity,
            "ensemble_member_id": self.ensemble_member_id,
            "broker_profile_id": self.broker_profile_id,
            "source_scope": self.source_scope,
            "bar_interval_code": self.bar_interval_code,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible quote metadata."""
        return {**self.identity_payload(), "quote_id": self.quote_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "StrategyQuoteV1":
        """Restore and verify one normalized strategy quote."""
        _require_schema(data, STRATEGY_QUOTE_SCHEMA_VERSION)
        return cls(
            source_event_id=str(data.get("source_event_id", "")),
            symbol=str(data.get("symbol", "")),
            event_time_ns=_strict_int(
                data.get("event_time_ns"), "event_time_ns"
            ),
            event_sequence=_strict_int(
                data.get("event_sequence"), "event_sequence"
            ),
            bid=_finite_float(data.get("bid"), "bid"),
            ask=_finite_float(data.get("ask"), "ask"),
            epoch_id=str(data.get("epoch_id", "")),
            session=str(data.get("session", "")),
            event_state=str(data.get("event_state", "")),
            sparsity=str(data.get("sparsity", "")),
            ensemble_member_id=_mapping_optional_text(
                data, "ensemble_member_id"
            ),
            broker_profile_id=_mapping_optional_text(data, "broker_profile_id"),
            source_scope=_mapping_optional_text(data, "source_scope"),
            bar_interval_code=_mapping_optional_text(data, "bar_interval_code"),
            quote_id=str(data.get("quote_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_benchmark_event(
        cls,
        event: BenchmarkEventV1,
        *,
        broker_profile_id: str | None = None,
    ) -> "StrategyQuoteV1":
        """Adapt one observed, degraded, or reconstructed benchmark event."""
        if not isinstance(event, BenchmarkEventV1):
            raise TypeError("strategy benchmark adapter requires event v1")
        return cls(
            source_event_id=event.benchmark_event_id,
            symbol=event.symbol,
            event_time_ns=event.event_time_ns,
            event_sequence=event.event_sequence,
            bid=event.bid,
            ask=event.ask,
            epoch_id=event.epoch_id,
            session=event.session,
            event_state=event.event_state,
            sparsity=event.sparsity,
            ensemble_member_id=event.ensemble_member_id,
            broker_profile_id=broker_profile_id,
        )

    @classmethod
    def from_synthetic_event(
        cls,
        event: SyntheticEventV1,
        *,
        epoch_id: str,
        session: str,
        event_state: str,
        sparsity: str,
    ) -> "StrategyQuoteV1":
        """Adapt one final observed or generated reconstructed event."""
        if not isinstance(event, SyntheticEventV1):
            raise TypeError("strategy reconstruction adapter requires event v1")
        return cls(
            source_event_id=event.event_id,
            symbol=event.symbol,
            event_time_ns=event.event_time_ns,
            event_sequence=event.event_sequence,
            bid=event.bid,
            ask=event.ask,
            epoch_id=epoch_id,
            session=session,
            event_state=event_state,
            sparsity=sparsity,
            ensemble_member_id=event.ensemble_member_id,
            broker_profile_id=event.broker_profile_id,
        )

    @classmethod
    def from_derived_bar(
        cls,
        bar: DerivedBarV1,
        *,
        session: str,
        event_state: str,
        sparsity: str = "derived_bar",
    ) -> "StrategyQuoteV1":
        """Adapt one verified derived bar close without fabricating volume."""
        if not isinstance(bar, DerivedBarV1):
            raise TypeError("strategy bar adapter requires derived bar v1")
        return cls(
            source_event_id=bar.bar_id,
            symbol=bar.symbol,
            event_time_ns=bar.last_event_time_ns,
            event_sequence=0,
            bid=bar.bid_close,
            ask=bar.ask_close,
            epoch_id=(
                _single_or_mixed(bar.feed_epoch_ids, "unclassified")
                or "unclassified"
            ),
            session=session,
            event_state=event_state,
            sparsity=sparsity,
            ensemble_member_id=bar.ensemble_member_id,
            broker_profile_id=_single_or_mixed(bar.broker_profile_ids, None),
            source_scope=bar.scope.value,
            bar_interval_code=bar.interval_code,
        )


@dataclass(frozen=True, slots=True)
class StrategySignalV1:
    """One deterministic directional decision emitted from a current quote."""

    strategy_specification_id: str
    source_quote_id: str
    decision_time_ns: int
    side: StrategySide
    signal_id: str = ""
    schema_version: str = STRATEGY_SIGNAL_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_version(
            self.schema_version,
            STRATEGY_SIGNAL_SCHEMA_VERSION,
            "strategy signal",
        )
        object.__setattr__(
            self,
            "strategy_specification_id",
            _required_text(self.strategy_specification_id),
        )
        object.__setattr__(
            self, "source_quote_id", _required_text(self.source_quote_id)
        )
        object.__setattr__(
            self,
            "decision_time_ns",
            _strict_int(self.decision_time_ns, "decision_time_ns"),
        )
        object.__setattr__(self, "side", StrategySide.from_value(self.side))
        expected = _stable_id("strategy-signal", self.identity_payload())
        supplied = _optional_text(self.signal_id)
        if supplied is not None and supplied != expected:
            raise ValueError("strategy signal_id differs from content")
        object.__setattr__(self, "signal_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return fields used for deterministic signal identity."""
        return {
            "schema_version": self.schema_version,
            "strategy_specification_id": self.strategy_specification_id,
            "source_quote_id": self.source_quote_id,
            "decision_time_ns": self.decision_time_ns,
            "side": self.side.value,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic signal metadata."""
        return {**self.identity_payload(), "signal_id": self.signal_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "StrategySignalV1":
        """Restore and verify one strategy signal."""
        _require_schema(data, STRATEGY_SIGNAL_SCHEMA_VERSION)
        return cls(
            strategy_specification_id=str(
                data.get("strategy_specification_id", "")
            ),
            source_quote_id=str(data.get("source_quote_id", "")),
            decision_time_ns=_strict_int(
                data.get("decision_time_ns"), "decision_time_ns"
            ),
            side=StrategySide.from_value(str(data.get("side", ""))),
            signal_id=str(data.get("signal_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@runtime_checkable
class StrategySignalStateV1(Protocol):
    """Window-local streaming state supplied by a strategy plugin."""

    def observe(self, quote: StrategyQuoteV1) -> Sequence[StrategySignalV1]:
        """Consume one current quote and emit zero or more current signals."""


@runtime_checkable
class StrategySignalEngineV1(Protocol):
    """Pluggable deterministic strategy boundary used by the evaluator."""

    specification: StrategySpecificationV1

    def start_window(
        self, evaluation_case: StrategyEvaluationCaseV1
    ) -> StrategySignalStateV1:
        """Return fresh bounded state for one aligned evaluation window."""


class ReferenceMomentumStrategyV1:
    """Transparent lagged-midpoint momentum fixture for accounting tests."""

    def __init__(
        self,
        *,
        lookback_ns: int = 1_000_000_000,
        decision_interval_ns: int = 1_000_000_000,
        threshold_bps: float = 0.0,
        max_state_quotes: int = 4_096,
    ) -> None:
        lookback = _positive_int(lookback_ns, "lookback_ns")
        interval = _positive_int(decision_interval_ns, "decision_interval_ns")
        threshold = _nonnegative_float(threshold_bps, "threshold_bps")
        state_limit = _bounded_int(
            max_state_quotes, "max_state_quotes", 2, 1_000_000
        )
        self.specification = StrategySpecificationV1(
            method_id=REFERENCE_MOMENTUM_STRATEGY_ID,
            implementation_version=REFERENCE_MOMENTUM_STRATEGY_VERSION,
            parameters={
                "lookback_ns": lookback,
                "decision_interval_ns": interval,
                "threshold_bps": threshold,
                "max_state_quotes": state_limit,
                "signal_semantics": "lagged_midpoint_direction",
            },
        )
        self._lookback_ns = lookback
        self._decision_interval_ns = interval
        self._threshold_bps = threshold
        self._max_state_quotes = state_limit

    def start_window(
        self, evaluation_case: StrategyEvaluationCaseV1
    ) -> StrategySignalStateV1:
        """Return independent state so cases cannot contaminate one another."""
        if not isinstance(evaluation_case, StrategyEvaluationCaseV1):
            raise TypeError("reference strategy requires an evaluation case")
        return _ReferenceMomentumState(
            specification_id=self.specification.specification_id,
            lookback_ns=self._lookback_ns,
            decision_interval_ns=self._decision_interval_ns,
            threshold_bps=self._threshold_bps,
            max_state_quotes=self._max_state_quotes,
        )


@dataclass(slots=True)
class _ReferenceMomentumState:
    specification_id: str
    lookback_ns: int
    decision_interval_ns: int
    threshold_bps: float
    max_state_quotes: int
    history: deque[StrategyQuoteV1] = field(default_factory=deque)
    last_decision_ns: int | None = None

    def observe(self, quote: StrategyQuoteV1) -> Sequence[StrategySignalV1]:
        target = quote.event_time_ns - self.lookback_ns
        while (
            len(self.history) >= 2 and self.history[1].event_time_ns <= target
        ):
            self.history.popleft()
        if len(self.history) >= self.max_state_quotes:
            raise StrategyResourceLimitError(
                "reference strategy state exceeds max_state_quotes"
            )
        self.history.append(quote)
        reference = self.history[0]
        if reference.event_time_ns > target:
            return ()
        if (
            self.last_decision_ns is not None
            and quote.event_time_ns - self.last_decision_ns
            < self.decision_interval_ns
        ):
            return ()
        change_bps = (quote.mid - reference.mid) / reference.mid * _BPS_SCALE
        if change_bps > self.threshold_bps:
            side = StrategySide.LONG
        elif change_bps < -self.threshold_bps:
            side = StrategySide.SHORT
        else:
            return ()
        self.last_decision_ns = quote.event_time_ns
        return (
            StrategySignalV1(
                strategy_specification_id=self.specification_id,
                source_quote_id=quote.quote_id,
                decision_time_ns=quote.event_time_ns,
                side=side,
            ),
        )


@dataclass(frozen=True, slots=True)
class StrategySliceResultV1:
    """Bounded execution response for one full source/regime/member slice."""

    case_id: str
    alignment_window_id: str
    source_kind: StrategySourceKind
    symbol: str
    epoch_id: str
    session: str
    event_state: str
    sparsity: str
    broker_profile_id: str
    ensemble_member_id: str
    horizon_ns: int
    signal_count: int
    completed_count: int
    missing_support_count: int
    mean_gross_response_bps: float
    mean_net_execution_response_bps: float
    mean_cost_drag_bps: float
    mean_entry_delay_ns: float
    favorable_response_rate: float
    slice_result_id: str = ""
    schema_version: str = STRATEGY_SLICE_RESULT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_version(
            self.schema_version,
            STRATEGY_SLICE_RESULT_SCHEMA_VERSION,
            "strategy slice result",
        )
        for name in (
            "case_id",
            "alignment_window_id",
            "epoch_id",
            "session",
            "event_state",
            "sparsity",
            "broker_profile_id",
            "ensemble_member_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self, "source_kind", StrategySourceKind.from_value(self.source_kind)
        )
        object.__setattr__(self, "symbol", _normalized_symbol(self.symbol))
        object.__setattr__(
            self, "horizon_ns", _positive_int(self.horizon_ns, "horizon_ns")
        )
        for name in (
            "signal_count",
            "completed_count",
            "missing_support_count",
        ):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        if self.signal_count < 1:
            raise ValueError("strategy slice requires signal support")
        if (
            self.signal_count
            != self.completed_count + self.missing_support_count
        ):
            raise ValueError("strategy slice outcome counts do not reconcile")
        for name in (
            "mean_gross_response_bps",
            "mean_net_execution_response_bps",
            "mean_cost_drag_bps",
            "mean_entry_delay_ns",
            "favorable_response_rate",
        ):
            object.__setattr__(
                self, name, _finite_float(getattr(self, name), name)
            )
        if self.mean_cost_drag_bps < 0 or self.mean_entry_delay_ns < 0:
            raise ValueError("strategy cost/delay summaries cannot be negative")
        if not 0.0 <= self.favorable_response_rate <= 1.0:
            raise ValueError("favorable_response_rate must be in [0,1]")
        expected = _stable_id("strategy-slice-result", self.identity_payload())
        supplied = _optional_text(self.slice_result_id)
        if supplied is not None and supplied != expected:
            raise ValueError("strategy slice_result_id differs from content")
        object.__setattr__(self, "slice_result_id", expected)

    @property
    def comparison_key(self) -> tuple[str, str, str, str, str, int]:
        """Return the common key used for reverse-degradation comparison."""
        return (
            self.alignment_window_id,
            self.symbol,
            self.epoch_id,
            self.session,
            self.event_state,
            self.horizon_ns,
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return fields used for deterministic slice-result identity."""
        return {
            "schema_version": self.schema_version,
            "case_id": self.case_id,
            "alignment_window_id": self.alignment_window_id,
            "source_kind": self.source_kind.value,
            "symbol": self.symbol,
            "epoch_id": self.epoch_id,
            "session": self.session,
            "event_state": self.event_state,
            "sparsity": self.sparsity,
            "broker_profile_id": self.broker_profile_id,
            "ensemble_member_id": self.ensemble_member_id,
            "horizon_ns": self.horizon_ns,
            "signal_count": self.signal_count,
            "completed_count": self.completed_count,
            "missing_support_count": self.missing_support_count,
            "mean_gross_response_bps": self.mean_gross_response_bps,
            "mean_net_execution_response_bps": self.mean_net_execution_response_bps,
            "mean_cost_drag_bps": self.mean_cost_drag_bps,
            "mean_entry_delay_ns": self.mean_entry_delay_ns,
            "favorable_response_rate": self.favorable_response_rate,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic slice-result metadata."""
        return {
            **self.identity_payload(),
            "slice_result_id": self.slice_result_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "StrategySliceResultV1":
        """Restore and verify one slice result."""
        _require_schema(data, STRATEGY_SLICE_RESULT_SCHEMA_VERSION)
        return cls(
            case_id=str(data.get("case_id", "")),
            alignment_window_id=str(data.get("alignment_window_id", "")),
            source_kind=StrategySourceKind.from_value(
                str(data.get("source_kind", ""))
            ),
            symbol=str(data.get("symbol", "")),
            epoch_id=str(data.get("epoch_id", "")),
            session=str(data.get("session", "")),
            event_state=str(data.get("event_state", "")),
            sparsity=str(data.get("sparsity", "")),
            broker_profile_id=str(data.get("broker_profile_id", "")),
            ensemble_member_id=str(data.get("ensemble_member_id", "")),
            horizon_ns=_strict_int(data.get("horizon_ns"), "horizon_ns"),
            signal_count=_strict_int(data.get("signal_count"), "signal_count"),
            completed_count=_strict_int(
                data.get("completed_count"), "completed_count"
            ),
            missing_support_count=_strict_int(
                data.get("missing_support_count"), "missing_support_count"
            ),
            mean_gross_response_bps=_finite_float(
                data.get("mean_gross_response_bps"), "mean_gross_response_bps"
            ),
            mean_net_execution_response_bps=_finite_float(
                data.get("mean_net_execution_response_bps"),
                "mean_net_execution_response_bps",
            ),
            mean_cost_drag_bps=_finite_float(
                data.get("mean_cost_drag_bps"), "mean_cost_drag_bps"
            ),
            mean_entry_delay_ns=_finite_float(
                data.get("mean_entry_delay_ns"), "mean_entry_delay_ns"
            ),
            favorable_response_rate=_finite_float(
                data.get("favorable_response_rate"), "favorable_response_rate"
            ),
            slice_result_id=str(data.get("slice_result_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class StrategyWindowResultV1:
    """One bounded source/window result without retained quotes or outcomes."""

    case_id: str
    alignment_window_id: str
    source_kind: StrategySourceKind
    status: StrategyWindowStatus
    quote_count: int
    signal_count: int
    outcome_count: int
    completed_outcome_count: int
    missing_support_count: int
    mean_interarrival_ns: float
    max_interarrival_ns: int
    mean_spread_bps: float
    slices: tuple[StrategySliceResultV1, ...] = ()
    reason: str | None = None
    invalid_for_backtest_reason: str | None = None
    window_result_id: str = ""
    schema_version: str = STRATEGY_WINDOW_RESULT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_version(
            self.schema_version,
            STRATEGY_WINDOW_RESULT_SCHEMA_VERSION,
            "strategy window result",
        )
        object.__setattr__(self, "case_id", _required_text(self.case_id))
        object.__setattr__(
            self,
            "alignment_window_id",
            _required_text(self.alignment_window_id),
        )
        object.__setattr__(
            self, "source_kind", StrategySourceKind.from_value(self.source_kind)
        )
        object.__setattr__(
            self, "status", StrategyWindowStatus.from_value(self.status)
        )
        for name in (
            "quote_count",
            "signal_count",
            "outcome_count",
            "completed_outcome_count",
            "missing_support_count",
            "max_interarrival_ns",
        ):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        if (
            self.outcome_count
            != self.completed_outcome_count + self.missing_support_count
        ):
            raise ValueError("strategy window outcome counts do not reconcile")
        object.__setattr__(
            self,
            "mean_interarrival_ns",
            _nonnegative_float(
                self.mean_interarrival_ns, "mean_interarrival_ns"
            ),
        )
        object.__setattr__(
            self,
            "mean_spread_bps",
            _nonnegative_float(self.mean_spread_bps, "mean_spread_bps"),
        )
        slices = tuple(
            sorted(self.slices, key=lambda item: item.slice_result_id)
        )
        if any(not isinstance(item, StrategySliceResultV1) for item in slices):
            raise TypeError("window slices must use strategy slice result v1")
        if sum(item.signal_count for item in slices) != self.outcome_count:
            raise ValueError("window slices do not reconcile outcome support")
        if (
            sum(item.completed_count for item in slices)
            != self.completed_outcome_count
        ):
            raise ValueError("window slices do not reconcile completed support")
        object.__setattr__(self, "slices", slices)
        object.__setattr__(self, "reason", _optional_text(self.reason))
        object.__setattr__(
            self,
            "invalid_for_backtest_reason",
            _optional_text(self.invalid_for_backtest_reason),
        )
        _validate_window_status(self)
        expected = _stable_id("strategy-window-result", self.identity_payload())
        supplied = _optional_text(self.window_result_id)
        if supplied is not None and supplied != expected:
            raise ValueError("strategy window_result_id differs from content")
        object.__setattr__(self, "window_result_id", expected)

    @property
    def valid_for_backtest(self) -> bool:
        """Return whether this result can support prospective backtest claims."""
        return self.invalid_for_backtest_reason is None

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return fields used for deterministic window-result identity."""
        return {
            "schema_version": self.schema_version,
            "case_id": self.case_id,
            "alignment_window_id": self.alignment_window_id,
            "source_kind": self.source_kind.value,
            "status": self.status.value,
            "quote_count": self.quote_count,
            "signal_count": self.signal_count,
            "outcome_count": self.outcome_count,
            "completed_outcome_count": self.completed_outcome_count,
            "missing_support_count": self.missing_support_count,
            "mean_interarrival_ns": self.mean_interarrival_ns,
            "max_interarrival_ns": self.max_interarrival_ns,
            "mean_spread_bps": self.mean_spread_bps,
            "slices": [item.to_dict() for item in self.slices],
            "reason": self.reason,
            "invalid_for_backtest_reason": self.invalid_for_backtest_reason,
            "quotes_retained": False,
            "outcomes_retained": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic bounded window evidence."""
        return {
            **self.identity_payload(),
            "window_result_id": self.window_result_id,
            "valid_for_backtest": self.valid_for_backtest,
            "backtest_label": (
                None
                if self.valid_for_backtest
                else STRATEGY_INVALID_FOR_BACKTEST_LABEL
            ),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "StrategyWindowResultV1":
        """Restore and verify one bounded window result."""
        _require_schema(data, STRATEGY_WINDOW_RESULT_SCHEMA_VERSION)
        _require_derived(data, "quotes_retained", False)
        _require_derived(data, "outcomes_retained", False)
        result = cls(
            case_id=str(data.get("case_id", "")),
            alignment_window_id=str(data.get("alignment_window_id", "")),
            source_kind=StrategySourceKind.from_value(
                str(data.get("source_kind", ""))
            ),
            status=StrategyWindowStatus.from_value(str(data.get("status", ""))),
            quote_count=_strict_int(data.get("quote_count"), "quote_count"),
            signal_count=_strict_int(data.get("signal_count"), "signal_count"),
            outcome_count=_strict_int(
                data.get("outcome_count"), "outcome_count"
            ),
            completed_outcome_count=_strict_int(
                data.get("completed_outcome_count"), "completed_outcome_count"
            ),
            missing_support_count=_strict_int(
                data.get("missing_support_count"), "missing_support_count"
            ),
            mean_interarrival_ns=_finite_float(
                data.get("mean_interarrival_ns"), "mean_interarrival_ns"
            ),
            max_interarrival_ns=_strict_int(
                data.get("max_interarrival_ns"), "max_interarrival_ns"
            ),
            mean_spread_bps=_finite_float(
                data.get("mean_spread_bps"), "mean_spread_bps"
            ),
            slices=tuple(
                StrategySliceResultV1.from_dict(item)
                for item in _mapping_sequence(data.get("slices"), "slices")
            ),
            reason=_mapping_optional_text(data, "reason"),
            invalid_for_backtest_reason=_mapping_optional_text(
                data, "invalid_for_backtest_reason"
            ),
            window_result_id=str(data.get("window_result_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        _require_derived(data, "valid_for_backtest", result.valid_for_backtest)
        _require_derived(
            data,
            "backtest_label",
            (
                None
                if result.valid_for_backtest
                else STRATEGY_INVALID_FOR_BACKTEST_LABEL
            ),
        )
        return result


@dataclass(frozen=True, slots=True)
class StrategyUncertaintySummaryV1:
    """Member/window dispersion for one source and regime sensitivity cell."""

    source_kind: StrategySourceKind
    symbol: str
    epoch_id: str
    session: str
    event_state: str
    sparsity: str
    broker_profile_id: str
    horizon_ns: int
    window_count: int
    ensemble_member_ids: tuple[str, ...]
    completed_outcome_count: int
    mean_net_response_bps: float
    min_net_response_bps: float
    max_net_response_bps: float
    standard_deviation_bps: float
    summary_id: str = ""
    schema_version: str = STRATEGY_UNCERTAINTY_SUMMARY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_version(
            self.schema_version,
            STRATEGY_UNCERTAINTY_SUMMARY_SCHEMA_VERSION,
            "strategy uncertainty summary",
        )
        object.__setattr__(
            self, "source_kind", StrategySourceKind.from_value(self.source_kind)
        )
        object.__setattr__(self, "symbol", _normalized_symbol(self.symbol))
        for name in (
            "epoch_id",
            "session",
            "event_state",
            "sparsity",
            "broker_profile_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self, "horizon_ns", _positive_int(self.horizon_ns, "horizon_ns")
        )
        object.__setattr__(
            self,
            "window_count",
            _positive_int(self.window_count, "window_count"),
        )
        members = _normalized_text_tuple(self.ensemble_member_ids)
        if not members:
            raise ValueError("uncertainty summary requires ensemble members")
        object.__setattr__(self, "ensemble_member_ids", members)
        object.__setattr__(
            self,
            "completed_outcome_count",
            _positive_int(
                self.completed_outcome_count, "completed_outcome_count"
            ),
        )
        for name in (
            "mean_net_response_bps",
            "min_net_response_bps",
            "max_net_response_bps",
            "standard_deviation_bps",
        ):
            object.__setattr__(
                self, name, _finite_float(getattr(self, name), name)
            )
        if self.standard_deviation_bps < 0:
            raise ValueError(
                "uncertainty standard deviation cannot be negative"
            )
        if (
            not self.min_net_response_bps
            <= self.mean_net_response_bps
            <= self.max_net_response_bps
        ):
            raise ValueError("uncertainty mean is outside the observed range")
        expected = _stable_id("strategy-uncertainty", self.identity_payload())
        supplied = _optional_text(self.summary_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "strategy uncertainty summary_id differs from content"
            )
        object.__setattr__(self, "summary_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return fields used for deterministic uncertainty identity."""
        return {
            "schema_version": self.schema_version,
            "source_kind": self.source_kind.value,
            "symbol": self.symbol,
            "epoch_id": self.epoch_id,
            "session": self.session,
            "event_state": self.event_state,
            "sparsity": self.sparsity,
            "broker_profile_id": self.broker_profile_id,
            "horizon_ns": self.horizon_ns,
            "window_count": self.window_count,
            "ensemble_member_ids": list(self.ensemble_member_ids),
            "completed_outcome_count": self.completed_outcome_count,
            "mean_net_response_bps": self.mean_net_response_bps,
            "min_net_response_bps": self.min_net_response_bps,
            "max_net_response_bps": self.max_net_response_bps,
            "standard_deviation_bps": self.standard_deviation_bps,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic uncertainty metadata."""
        return {**self.identity_payload(), "summary_id": self.summary_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "StrategyUncertaintySummaryV1":
        """Restore and verify one uncertainty summary."""
        _require_schema(data, STRATEGY_UNCERTAINTY_SUMMARY_SCHEMA_VERSION)
        return cls(
            source_kind=StrategySourceKind.from_value(
                str(data.get("source_kind", ""))
            ),
            symbol=str(data.get("symbol", "")),
            epoch_id=str(data.get("epoch_id", "")),
            session=str(data.get("session", "")),
            event_state=str(data.get("event_state", "")),
            sparsity=str(data.get("sparsity", "")),
            broker_profile_id=str(data.get("broker_profile_id", "")),
            horizon_ns=_strict_int(data.get("horizon_ns"), "horizon_ns"),
            window_count=_strict_int(data.get("window_count"), "window_count"),
            ensemble_member_ids=tuple(
                str(item)
                for item in _sequence(
                    data.get("ensemble_member_ids"), "ensemble_member_ids"
                )
            ),
            completed_outcome_count=_strict_int(
                data.get("completed_outcome_count"), "completed_outcome_count"
            ),
            mean_net_response_bps=_finite_float(
                data.get("mean_net_response_bps"), "mean_net_response_bps"
            ),
            min_net_response_bps=_finite_float(
                data.get("min_net_response_bps"), "min_net_response_bps"
            ),
            max_net_response_bps=_finite_float(
                data.get("max_net_response_bps"), "max_net_response_bps"
            ),
            standard_deviation_bps=_finite_float(
                data.get("standard_deviation_bps"), "standard_deviation_bps"
            ),
            summary_id=str(data.get("summary_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class StrategyRestorationResultV1:
    """Reverse-degradation distance from candidate execution to dense reference."""

    alignment_window_id: str
    candidate_case_id: str
    candidate_source_kind: StrategySourceKind
    symbol: str
    epoch_id: str
    session: str
    event_state: str
    sparsity: str
    broker_profile_id: str
    ensemble_member_id: str
    horizon_ns: int
    dense_reference_response_bps: float
    degraded_response_bps: float
    candidate_response_bps: float
    degraded_absolute_error_bps: float
    candidate_absolute_error_bps: float
    restoration_gain_bps: float
    approaches_dense_reference: bool
    result_id: str = ""
    schema_version: str = STRATEGY_RESTORATION_RESULT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_version(
            self.schema_version,
            STRATEGY_RESTORATION_RESULT_SCHEMA_VERSION,
            "strategy restoration result",
        )
        for name in (
            "alignment_window_id",
            "candidate_case_id",
            "epoch_id",
            "session",
            "event_state",
            "sparsity",
            "broker_profile_id",
            "ensemble_member_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self,
            "candidate_source_kind",
            StrategySourceKind.from_value(self.candidate_source_kind),
        )
        if self.candidate_source_kind in {
            StrategySourceKind.OBSERVED,
            StrategySourceKind.DEGRADED_HOLDOUT,
        }:
            raise ValueError("restoration candidate must be reconstructed")
        object.__setattr__(self, "symbol", _normalized_symbol(self.symbol))
        object.__setattr__(
            self, "horizon_ns", _positive_int(self.horizon_ns, "horizon_ns")
        )
        for name in (
            "dense_reference_response_bps",
            "degraded_response_bps",
            "candidate_response_bps",
            "degraded_absolute_error_bps",
            "candidate_absolute_error_bps",
            "restoration_gain_bps",
        ):
            object.__setattr__(
                self, name, _finite_float(getattr(self, name), name)
            )
        if (
            self.degraded_absolute_error_bps < 0
            or self.candidate_absolute_error_bps < 0
        ):
            raise ValueError("restoration absolute errors cannot be negative")
        expected_gain = (
            self.degraded_absolute_error_bps - self.candidate_absolute_error_bps
        )
        if not math.isclose(
            self.restoration_gain_bps, expected_gain, abs_tol=1e-10
        ):
            raise ValueError("restoration gain does not reconcile")
        expected_approach = (
            self.candidate_absolute_error_bps
            <= self.degraded_absolute_error_bps
        )
        if self.approaches_dense_reference is not expected_approach:
            raise ValueError("restoration approach flag does not reconcile")
        expected = _stable_id("strategy-restoration", self.identity_payload())
        supplied = _optional_text(self.result_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "strategy restoration result_id differs from content"
            )
        object.__setattr__(self, "result_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return fields used for deterministic restoration identity."""
        return {
            "schema_version": self.schema_version,
            "alignment_window_id": self.alignment_window_id,
            "candidate_case_id": self.candidate_case_id,
            "candidate_source_kind": self.candidate_source_kind.value,
            "symbol": self.symbol,
            "epoch_id": self.epoch_id,
            "session": self.session,
            "event_state": self.event_state,
            "sparsity": self.sparsity,
            "broker_profile_id": self.broker_profile_id,
            "ensemble_member_id": self.ensemble_member_id,
            "horizon_ns": self.horizon_ns,
            "dense_reference_response_bps": self.dense_reference_response_bps,
            "degraded_response_bps": self.degraded_response_bps,
            "candidate_response_bps": self.candidate_response_bps,
            "degraded_absolute_error_bps": self.degraded_absolute_error_bps,
            "candidate_absolute_error_bps": self.candidate_absolute_error_bps,
            "restoration_gain_bps": self.restoration_gain_bps,
            "approaches_dense_reference": self.approaches_dense_reference,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic restoration metadata."""
        return {**self.identity_payload(), "result_id": self.result_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "StrategyRestorationResultV1":
        """Restore and verify one restoration result."""
        _require_schema(data, STRATEGY_RESTORATION_RESULT_SCHEMA_VERSION)
        return cls(
            alignment_window_id=str(data.get("alignment_window_id", "")),
            candidate_case_id=str(data.get("candidate_case_id", "")),
            candidate_source_kind=StrategySourceKind.from_value(
                str(data.get("candidate_source_kind", ""))
            ),
            symbol=str(data.get("symbol", "")),
            epoch_id=str(data.get("epoch_id", "")),
            session=str(data.get("session", "")),
            event_state=str(data.get("event_state", "")),
            sparsity=str(data.get("sparsity", "")),
            broker_profile_id=str(data.get("broker_profile_id", "")),
            ensemble_member_id=str(data.get("ensemble_member_id", "")),
            horizon_ns=_strict_int(data.get("horizon_ns"), "horizon_ns"),
            dense_reference_response_bps=_finite_float(
                data.get("dense_reference_response_bps"),
                "dense_reference_response_bps",
            ),
            degraded_response_bps=_finite_float(
                data.get("degraded_response_bps"), "degraded_response_bps"
            ),
            candidate_response_bps=_finite_float(
                data.get("candidate_response_bps"), "candidate_response_bps"
            ),
            degraded_absolute_error_bps=_finite_float(
                data.get("degraded_absolute_error_bps"),
                "degraded_absolute_error_bps",
            ),
            candidate_absolute_error_bps=_finite_float(
                data.get("candidate_absolute_error_bps"),
                "candidate_absolute_error_bps",
            ),
            restoration_gain_bps=_finite_float(
                data.get("restoration_gain_bps"), "restoration_gain_bps"
            ),
            approaches_dense_reference=_strict_bool(
                data.get("approaches_dense_reference"),
                "approaches_dense_reference",
            ),
            result_id=str(data.get("result_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class StrategySensitivityReportV1:
    """Bounded comparison report emphasizing sensitivity and uncertainty."""

    plan: StrategyEvaluationPlanV1
    window_results: tuple[StrategyWindowResultV1, ...]
    uncertainty_summaries: tuple[StrategyUncertaintySummaryV1, ...]
    restoration_results: tuple[StrategyRestorationResultV1, ...]
    restoration_unavailable_count: int
    report_id: str = ""
    schema_version: str = STRATEGY_SENSITIVITY_REPORT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_version(
            self.schema_version,
            STRATEGY_SENSITIVITY_REPORT_SCHEMA_VERSION,
            "strategy sensitivity report",
        )
        if not isinstance(self.plan, StrategyEvaluationPlanV1):
            raise TypeError("strategy report requires evaluation plan v1")
        windows = tuple(
            sorted(self.window_results, key=lambda item: item.window_result_id)
        )
        if len(windows) != len(self.plan.cases) or {
            item.case_id for item in windows
        } != {item.case_id for item in self.plan.cases}:
            raise ValueError(
                "strategy report does not cover every plan case exactly"
            )
        object.__setattr__(self, "window_results", windows)
        uncertainty = tuple(
            sorted(self.uncertainty_summaries, key=lambda item: item.summary_id)
        )
        restoration = tuple(
            sorted(self.restoration_results, key=lambda item: item.result_id)
        )
        if any(
            not isinstance(item, StrategyUncertaintySummaryV1)
            for item in uncertainty
        ):
            raise TypeError("strategy report uncertainty must use v1 summaries")
        if any(
            not isinstance(item, StrategyRestorationResultV1)
            for item in restoration
        ):
            raise TypeError("strategy report restoration must use v1 results")
        object.__setattr__(self, "uncertainty_summaries", uncertainty)
        object.__setattr__(self, "restoration_results", restoration)
        object.__setattr__(
            self,
            "restoration_unavailable_count",
            _nonnegative_int(
                self.restoration_unavailable_count,
                "restoration_unavailable_count",
            ),
        )
        expected = _stable_id(
            "strategy-sensitivity-report", self.identity_payload()
        )
        supplied = _optional_text(self.report_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "strategy sensitivity report_id differs from content"
            )
        object.__setattr__(self, "report_id", expected)
        _ensure_payload_size(self.to_dict(), self.plan.policy.max_payload_bytes)

    @property
    def valid_for_backtest(self) -> bool:
        """Return whether plan and every result support prospective claims."""
        return self.plan.valid_for_backtest and all(
            item.valid_for_backtest for item in self.window_results
        )

    @property
    def summary(self) -> dict[str, JSONValue]:
        """Return bounded failure/no-trade/support rates and counts."""
        total = len(self.window_results)
        counts = {
            status.value: sum(
                item.status is status for item in self.window_results
            )
            for status in StrategyWindowStatus
        }
        status_counts: dict[str, JSONValue] = dict(counts)
        outcomes = sum(item.outcome_count for item in self.window_results)
        missing = sum(
            item.missing_support_count for item in self.window_results
        )
        return {
            "window_count": total,
            "status_counts": status_counts,
            "completed_window_rate": _ratio(counts["completed"], total),
            "failure_window_rate": _ratio(counts["failed"], total),
            "no_trade_window_rate": _ratio(counts["no_trade"], total),
            "missing_support_window_rate": _ratio(
                counts["missing_support"], total
            ),
            "refused_window_rate": _ratio(counts["refused"], total),
            "outcome_count": outcomes,
            "missing_support_outcome_count": missing,
            "missing_support_outcome_rate": _ratio(missing, outcomes),
            "uncertainty_summary_count": len(self.uncertainty_summaries),
            "restoration_result_count": len(self.restoration_results),
            "restoration_unavailable_count": self.restoration_unavailable_count,
        }

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return fields used for deterministic report identity."""
        return {
            "schema_version": self.schema_version,
            "plan": self.plan.to_dict(),
            "window_results": [item.to_dict() for item in self.window_results],
            "uncertainty_summaries": [
                item.to_dict() for item in self.uncertainty_summaries
            ],
            "restoration_results": [
                item.to_dict() for item in self.restoration_results
            ],
            "restoration_unavailable_count": self.restoration_unavailable_count,
            "summary": self.summary,
            "interpretation": "sensitivity_robustness_and_uncertainty_only",
            "output_mode": STRATEGY_OUTPUT_MODE,
            "event_schema_augmented": False,
            "profit_claim": False,
            "investment_recommendation": False,
            "automatic_winner": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic bounded report metadata."""
        return {
            **self.identity_payload(),
            "report_id": self.report_id,
            "valid_for_backtest": self.valid_for_backtest,
            "backtest_label": (
                None
                if self.valid_for_backtest
                else STRATEGY_INVALID_FOR_BACKTEST_LABEL
            ),
        }

    def to_json(self) -> str:
        """Return canonical compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "StrategySensitivityReportV1":
        """Restore and verify one sensitivity report."""
        _require_schema(data, STRATEGY_SENSITIVITY_REPORT_SCHEMA_VERSION)
        for name, expected in (
            ("interpretation", "sensitivity_robustness_and_uncertainty_only"),
            ("output_mode", STRATEGY_OUTPUT_MODE),
            ("event_schema_augmented", False),
            ("profit_claim", False),
            ("investment_recommendation", False),
            ("automatic_winner", False),
        ):
            _require_derived(data, name, expected)
        report = cls(
            plan=StrategyEvaluationPlanV1.from_dict(
                _mapping(data.get("plan"), "plan")
            ),
            window_results=tuple(
                StrategyWindowResultV1.from_dict(item)
                for item in _mapping_sequence(
                    data.get("window_results"), "window_results"
                )
            ),
            uncertainty_summaries=tuple(
                StrategyUncertaintySummaryV1.from_dict(item)
                for item in _mapping_sequence(
                    data.get("uncertainty_summaries"), "uncertainty_summaries"
                )
            ),
            restoration_results=tuple(
                StrategyRestorationResultV1.from_dict(item)
                for item in _mapping_sequence(
                    data.get("restoration_results"), "restoration_results"
                )
            ),
            restoration_unavailable_count=_strict_int(
                data.get("restoration_unavailable_count"),
                "restoration_unavailable_count",
            ),
            report_id=str(data.get("report_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        _require_derived(data, "summary", report.summary)
        _require_derived(data, "valid_for_backtest", report.valid_for_backtest)
        _require_derived(
            data,
            "backtest_label",
            (
                None
                if report.valid_for_backtest
                else STRATEGY_INVALID_FOR_BACKTEST_LABEL
            ),
        )
        return report

    @classmethod
    def from_json(cls, text: str) -> "StrategySensitivityReportV1":
        """Restore a report from canonical JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(slots=True)
class _SliceAccumulator:
    signal_count: int = 0
    completed_count: int = 0
    missing_support_count: int = 0
    gross_total: float = 0.0
    net_total: float = 0.0
    cost_total: float = 0.0
    entry_delay_total: int = 0
    favorable_count: int = 0

    def add_signal(self) -> None:
        self.signal_count += 1

    def add_missing(self) -> None:
        self.missing_support_count += 1

    def add_completed(
        self,
        *,
        gross_bps: float,
        net_bps: float,
        cost_drag_bps: float,
        entry_delay_ns: int,
    ) -> None:
        self.completed_count += 1
        self.gross_total += gross_bps
        self.net_total += net_bps
        self.cost_total += cost_drag_bps
        self.entry_delay_total += entry_delay_ns
        self.favorable_count += int(net_bps > 0.0)


@dataclass(slots=True)
class _PendingSignal:
    signal: StrategySignalV1
    decision_quote: StrategyQuoteV1
    unresolved_horizons: set[int]
    entry_quote: StrategyQuoteV1 | None = None
    entry_price: float | None = None


def evaluate_strategy_sensitivity(
    plan: StrategyEvaluationPlanV1,
    quote_streams: Mapping[str, Iterable[StrategyQuoteV1]],
    information_audits: Mapping[str, InformationAuditReportV1],
    engine: StrategySignalEngineV1,
) -> StrategySensitivityReportV1:
    """Evaluate every plan case sequentially using bounded streaming state."""
    if not isinstance(plan, StrategyEvaluationPlanV1):
        raise TypeError("strategy evaluation requires plan v1")
    if not isinstance(engine, StrategySignalEngineV1):
        raise TypeError("strategy engine does not implement the v1 protocol")
    if engine.specification != plan.strategy:
        raise ValueError("strategy engine specification differs from plan")
    unknown_streams = set(quote_streams).difference(
        item.case_id for item in plan.cases
    )
    if unknown_streams:
        raise ValueError("quote streams include unplanned strategy cases")
    windows: list[StrategyWindowResultV1] = []
    for evaluation_case in plan.cases:
        audit = information_audits.get(evaluation_case.information_audit_id)
        if audit is None:
            raise ValueError("strategy case lacks its information audit")
        _validate_information_audit(evaluation_case, audit)
        invalid_reason = (
            plan.invalid_for_backtest_reason
            or evaluation_case.invalid_for_backtest_reason
        )
        stream = quote_streams.get(evaluation_case.case_id)
        if stream is None:
            windows.append(
                _empty_window_result(
                    evaluation_case,
                    StrategyWindowStatus.MISSING_SUPPORT,
                    reason="quote_stream_missing",
                    invalid_for_backtest_reason=invalid_reason,
                )
            )
            continue
        try:
            result = _evaluate_strategy_window(
                evaluation_case,
                stream,
                engine,
                plan.execution,
                plan.policy,
                invalid_for_backtest_reason=invalid_reason,
            )
        except StrategyResourceLimitError as err:
            result = _empty_window_result(
                evaluation_case,
                StrategyWindowStatus.REFUSED,
                reason=str(err),
                invalid_for_backtest_reason=invalid_reason,
            )
        except StrategyEvaluationFailure as err:
            result = _empty_window_result(
                evaluation_case,
                StrategyWindowStatus.FAILED,
                reason=str(err),
                invalid_for_backtest_reason=invalid_reason,
            )
        windows.append(result)
    uncertainty = _build_uncertainty_summaries(windows, plan.policy)
    restoration, unavailable = _build_restoration_results(windows, plan.policy)
    return StrategySensitivityReportV1(
        plan=plan,
        window_results=tuple(windows),
        uncertainty_summaries=uncertainty,
        restoration_results=restoration,
        restoration_unavailable_count=unavailable,
    )


def strategy_sensitivity_benchmark_hooks(
    result: StrategyWindowResultV1,
    *,
    rounding_digits: int = DEFAULT_STRATEGY_ROUNDING_DIGITS,
) -> dict[str, float]:
    """Return canonical numeric hooks for #436/#442 integration.

    Unsupported, no-trade, refused, and failed windows deliberately do not
    receive a plausible zero-valued downstream sensitivity. Callers must keep
    their terminal status in the report and omit the required ensemble hook so
    existing calibration logic refuses that member.
    """
    if not isinstance(result, StrategyWindowResultV1):
        raise TypeError("strategy benchmark hooks require window result v1")
    if result.status is not StrategyWindowStatus.COMPLETED:
        raise ValueError("strategy benchmark hooks require a completed window")
    digits = _bounded_int(rounding_digits, "rounding_digits", 0, 15)
    support = sum(item.completed_count for item in result.slices)
    if support < 1:
        raise ValueError("strategy benchmark hooks require completed outcomes")

    def weighted(name: str) -> float:
        return _rounded(
            sum(
                getattr(item, name) * item.completed_count
                for item in result.slices
            )
            / support,
            digits,
        )

    return {
        "downstream_sensitivity": weighted("mean_net_execution_response_bps"),
        "strategy_gross_response_bps": weighted("mean_gross_response_bps"),
        "strategy_cost_drag_bps": weighted("mean_cost_drag_bps"),
        "strategy_entry_delay_ns": weighted("mean_entry_delay_ns"),
        "strategy_missing_support_rate": _rounded(
            _ratio(result.missing_support_count, result.outcome_count), digits
        ),
    }


def _evaluate_strategy_window(
    evaluation_case: StrategyEvaluationCaseV1,
    quotes: Iterable[StrategyQuoteV1],
    engine: StrategySignalEngineV1,
    execution: StrategyExecutionSpecificationV1,
    policy: StrategyEvaluationPolicyV1,
    *,
    invalid_for_backtest_reason: str | None,
) -> StrategyWindowResultV1:
    state = engine.start_window(evaluation_case)
    if not isinstance(state, StrategySignalStateV1):
        raise TypeError("strategy engine returned incompatible window state")
    accumulators: dict[
        tuple[str, str, str, str, str, str, int], _SliceAccumulator
    ] = {}
    pending: list[_PendingSignal] = []
    quote_count = 0
    signal_count = 0
    spread_bps_total = 0.0
    interarrival_total = 0
    interarrival_count = 0
    max_interarrival = 0
    previous_key: tuple[int, int, str] | None = None
    previous_time: int | None = None
    for quote in quotes:
        quote_count += 1
        if quote_count > policy.max_quotes_per_window:
            raise StrategyResourceLimitError("max_quotes_per_window exceeded")
        _validate_case_quote(evaluation_case, quote, previous_key)
        previous_key = quote.order_key
        if previous_time is not None:
            gap = quote.event_time_ns - previous_time
            interarrival_total += gap
            interarrival_count += 1
            max_interarrival = max(max_interarrival, gap)
        previous_time = quote.event_time_ns
        spread_bps_total += quote.spread / quote.mid * _BPS_SCALE
        _advance_pending(
            pending, quote, execution, evaluation_case, accumulators
        )
        emitted = tuple(state.observe(quote))
        for signal in emitted:
            _validate_signal(signal, quote, engine.specification)
            signal_count += 1
            if signal_count > policy.max_signals_per_window:
                raise StrategyResourceLimitError(
                    "max_signals_per_window exceeded"
                )
            pending_signal = _PendingSignal(
                signal=signal,
                decision_quote=quote,
                unresolved_horizons=set(policy.horizons_ns),
            )
            pending.append(pending_signal)
            for horizon in policy.horizons_ns:
                _slice_accumulator(
                    accumulators, quote, evaluation_case, horizon
                ).add_signal()
            if len(pending) > policy.max_pending_signals:
                raise StrategyResourceLimitError("max_pending_signals exceeded")
        _advance_pending(
            pending, quote, execution, evaluation_case, accumulators
        )
    for item in pending:
        for horizon in item.unresolved_horizons:
            _slice_accumulator(
                accumulators, item.decision_quote, evaluation_case, horizon
            ).add_missing()
    slices = _finalize_slices(accumulators, evaluation_case, policy)
    outcome_count = sum(item.signal_count for item in slices)
    completed_count = sum(item.completed_count for item in slices)
    missing_count = sum(item.missing_support_count for item in slices)
    if quote_count == 0:
        status = StrategyWindowStatus.MISSING_SUPPORT
        reason = "empty_quote_stream"
    elif signal_count == 0:
        status = StrategyWindowStatus.NO_TRADE
        reason = "strategy_emitted_no_signals"
    elif completed_count == 0:
        status = StrategyWindowStatus.MISSING_SUPPORT
        reason = "no_signal_horizon_had_execution_support"
    else:
        status = StrategyWindowStatus.COMPLETED
        reason = None
    return StrategyWindowResultV1(
        case_id=evaluation_case.case_id,
        alignment_window_id=evaluation_case.alignment_window_id,
        source_kind=evaluation_case.source_kind,
        status=status,
        quote_count=quote_count,
        signal_count=signal_count,
        outcome_count=outcome_count,
        completed_outcome_count=completed_count,
        missing_support_count=missing_count,
        mean_interarrival_ns=(
            interarrival_total / interarrival_count
            if interarrival_count
            else 0.0
        ),
        max_interarrival_ns=max_interarrival,
        mean_spread_bps=(
            spread_bps_total / quote_count if quote_count else 0.0
        ),
        slices=slices,
        reason=reason,
        invalid_for_backtest_reason=invalid_for_backtest_reason,
    )


def _advance_pending(
    pending: list[_PendingSignal],
    quote: StrategyQuoteV1,
    execution: StrategyExecutionSpecificationV1,
    evaluation_case: StrategyEvaluationCaseV1,
    accumulators: dict[
        tuple[str, str, str, str, str, str, int], _SliceAccumulator
    ],
) -> None:
    completed: list[_PendingSignal] = []
    for item in pending:
        if item.entry_quote is None:
            target = item.signal.decision_time_ns + execution.entry_latency_ns
            if quote.event_time_ns < target:
                continue
            if quote.event_time_ns > target + execution.max_execution_wait_ns:
                for horizon in item.unresolved_horizons:
                    _accumulator_for_pending(
                        accumulators, item, evaluation_case, horizon
                    ).add_missing()
                item.unresolved_horizons.clear()
                completed.append(item)
                continue
            item.entry_quote = quote
            item.entry_price = _entry_price(item.signal.side, quote, execution)
        entry_quote = item.entry_quote
        for horizon in tuple(sorted(item.unresolved_horizons)):
            target = entry_quote.event_time_ns + horizon
            if quote.event_time_ns < target:
                continue
            accumulator = _accumulator_for_pending(
                accumulators, item, evaluation_case, horizon
            )
            if quote.event_time_ns > target + execution.max_execution_wait_ns:
                accumulator.add_missing()
            else:
                entry_price = cast(float, item.entry_price)
                exit_price = _exit_price(item.signal.side, quote, execution)
                gross = (
                    item.signal.side.sign
                    * (quote.mid - item.decision_quote.mid)
                    / item.decision_quote.mid
                    * _BPS_SCALE
                )
                net = (
                    item.signal.side.sign
                    * (exit_price - entry_price)
                    / item.decision_quote.mid
                    * _BPS_SCALE
                    - 2.0 * execution.fixed_cost_bps_per_side
                )
                accumulator.add_completed(
                    gross_bps=gross,
                    net_bps=net,
                    cost_drag_bps=max(0.0, gross - net),
                    entry_delay_ns=entry_quote.event_time_ns
                    - item.signal.decision_time_ns,
                )
            item.unresolved_horizons.remove(horizon)
        if not item.unresolved_horizons:
            completed.append(item)
    for item in completed:
        pending.remove(item)


def _slice_accumulator(
    target: dict[tuple[str, str, str, str, str, str, int], _SliceAccumulator],
    quote: StrategyQuoteV1,
    evaluation_case: StrategyEvaluationCaseV1,
    horizon_ns: int,
) -> _SliceAccumulator:
    key = (
        quote.epoch_id,
        quote.session,
        quote.event_state,
        quote.sparsity,
        quote.broker_profile_id
        or evaluation_case.broker_profile_id
        or "unconditioned",
        quote.ensemble_member_id or evaluation_case.ensemble_member_id,
        horizon_ns,
    )
    return target.setdefault(key, _SliceAccumulator())


def _accumulator_for_pending(
    target: dict[tuple[str, str, str, str, str, str, int], _SliceAccumulator],
    pending: _PendingSignal,
    evaluation_case: StrategyEvaluationCaseV1,
    horizon_ns: int,
) -> _SliceAccumulator:
    return _slice_accumulator(
        target,
        pending.decision_quote,
        evaluation_case,
        horizon_ns,
    )


def _finalize_slices(
    accumulators: Mapping[
        tuple[str, str, str, str, str, str, int], _SliceAccumulator
    ],
    evaluation_case: StrategyEvaluationCaseV1,
    policy: StrategyEvaluationPolicyV1,
) -> tuple[StrategySliceResultV1, ...]:
    if len(accumulators) > policy.max_slices:
        raise StrategyResourceLimitError("max_slices exceeded")
    results: list[StrategySliceResultV1] = []
    for key, value in sorted(accumulators.items()):
        epoch, session, event_state, sparsity, broker, member, horizon = key
        completed = value.completed_count
        results.append(
            StrategySliceResultV1(
                case_id=evaluation_case.case_id,
                alignment_window_id=evaluation_case.alignment_window_id,
                source_kind=evaluation_case.source_kind,
                symbol=evaluation_case.symbol,
                epoch_id=epoch,
                session=session,
                event_state=event_state,
                sparsity=sparsity,
                broker_profile_id=broker,
                ensemble_member_id=member,
                horizon_ns=horizon,
                signal_count=value.signal_count,
                completed_count=completed,
                missing_support_count=value.missing_support_count,
                mean_gross_response_bps=(
                    _rounded(
                        value.gross_total / completed, policy.rounding_digits
                    )
                    if completed
                    else 0.0
                ),
                mean_net_execution_response_bps=(
                    _rounded(
                        value.net_total / completed, policy.rounding_digits
                    )
                    if completed
                    else 0.0
                ),
                mean_cost_drag_bps=(
                    _rounded(
                        value.cost_total / completed, policy.rounding_digits
                    )
                    if completed
                    else 0.0
                ),
                mean_entry_delay_ns=(
                    _rounded(
                        value.entry_delay_total / completed,
                        policy.rounding_digits,
                    )
                    if completed
                    else 0.0
                ),
                favorable_response_rate=(
                    _rounded(
                        value.favorable_count / completed,
                        policy.rounding_digits,
                    )
                    if completed
                    else 0.0
                ),
            )
        )
    return tuple(results)


def _build_uncertainty_summaries(
    windows: Sequence[StrategyWindowResultV1],
    policy: StrategyEvaluationPolicyV1,
) -> tuple[StrategyUncertaintySummaryV1, ...]:
    grouped: dict[
        tuple[StrategySourceKind, str, str, str, str, str, str, int],
        list[StrategySliceResultV1],
    ] = defaultdict(list)
    for window in windows:
        for item in window.slices:
            if item.completed_count:
                grouped[
                    (
                        item.source_kind,
                        item.symbol,
                        item.epoch_id,
                        item.session,
                        item.event_state,
                        item.sparsity,
                        item.broker_profile_id,
                        item.horizon_ns,
                    )
                ].append(item)
    if len(grouped) > policy.max_slices:
        raise StrategyResourceLimitError(
            "uncertainty summaries exceed max_slices"
        )
    results: list[StrategyUncertaintySummaryV1] = []
    for key, items in sorted(grouped.items(), key=lambda pair: str(pair[0])):
        values = [item.mean_net_execution_response_bps for item in items]
        mean = sum(values) / len(values)
        variance = sum((item - mean) ** 2 for item in values) / len(values)
        (
            source,
            symbol,
            epoch,
            session,
            event_state,
            sparsity,
            broker,
            horizon,
        ) = key
        results.append(
            StrategyUncertaintySummaryV1(
                source_kind=source,
                symbol=symbol,
                epoch_id=epoch,
                session=session,
                event_state=event_state,
                sparsity=sparsity,
                broker_profile_id=broker,
                horizon_ns=horizon,
                window_count=len({item.alignment_window_id for item in items}),
                ensemble_member_ids=tuple(
                    item.ensemble_member_id for item in items
                ),
                completed_outcome_count=sum(
                    item.completed_count for item in items
                ),
                mean_net_response_bps=_rounded(mean, policy.rounding_digits),
                min_net_response_bps=min(values),
                max_net_response_bps=max(values),
                standard_deviation_bps=_rounded(
                    math.sqrt(variance), policy.rounding_digits
                ),
            )
        )
    return tuple(results)


def _build_restoration_results(
    windows: Sequence[StrategyWindowResultV1],
    policy: StrategyEvaluationPolicyV1,
) -> tuple[tuple[StrategyRestorationResultV1, ...], int]:
    by_source: dict[
        StrategySourceKind,
        dict[tuple[str, str, str, str, str, int], list[StrategySliceResultV1]],
    ] = defaultdict(lambda: defaultdict(list))
    for window in windows:
        for item in window.slices:
            if item.completed_count:
                by_source[item.source_kind][item.comparison_key].append(item)
    dense = by_source.get(StrategySourceKind.OBSERVED, {})
    degraded = by_source.get(StrategySourceKind.DEGRADED_HOLDOUT, {})
    candidate_kinds = tuple(
        item
        for item in StrategySourceKind
        if item
        not in {
            StrategySourceKind.OBSERVED,
            StrategySourceKind.DEGRADED_HOLDOUT,
        }
    )
    results: list[StrategyRestorationResultV1] = []
    unavailable = 0
    for source in candidate_kinds:
        for key, candidates in by_source.get(source, {}).items():
            reference_items = dense.get(key, ())
            degraded_items = degraded.get(key, ())
            if not reference_items or not degraded_items:
                unavailable += len(candidates)
                continue
            reference = _weighted_response(reference_items)
            degraded_response = _weighted_response(degraded_items)
            degraded_error = abs(degraded_response - reference)
            for candidate in candidates:
                candidate_response = candidate.mean_net_execution_response_bps
                candidate_error = abs(candidate_response - reference)
                rounded_degraded_error = _rounded(
                    degraded_error, policy.rounding_digits
                )
                rounded_candidate_error = _rounded(
                    candidate_error, policy.rounding_digits
                )
                results.append(
                    StrategyRestorationResultV1(
                        alignment_window_id=candidate.alignment_window_id,
                        candidate_case_id=candidate.case_id,
                        candidate_source_kind=candidate.source_kind,
                        symbol=candidate.symbol,
                        epoch_id=candidate.epoch_id,
                        session=candidate.session,
                        event_state=candidate.event_state,
                        sparsity=candidate.sparsity,
                        broker_profile_id=candidate.broker_profile_id,
                        ensemble_member_id=candidate.ensemble_member_id,
                        horizon_ns=candidate.horizon_ns,
                        dense_reference_response_bps=_rounded(
                            reference, policy.rounding_digits
                        ),
                        degraded_response_bps=_rounded(
                            degraded_response, policy.rounding_digits
                        ),
                        candidate_response_bps=candidate_response,
                        degraded_absolute_error_bps=rounded_degraded_error,
                        candidate_absolute_error_bps=rounded_candidate_error,
                        restoration_gain_bps=_rounded(
                            rounded_degraded_error - rounded_candidate_error,
                            policy.rounding_digits,
                        ),
                        approaches_dense_reference=candidate_error
                        <= degraded_error,
                    )
                )
    if len(results) > policy.max_slices:
        raise StrategyResourceLimitError(
            "restoration results exceed max_slices"
        )
    return tuple(results), unavailable


def _weighted_response(items: Sequence[StrategySliceResultV1]) -> float:
    support = sum(item.completed_count for item in items)
    if not support:
        return 0.0
    return (
        sum(
            item.mean_net_execution_response_bps * item.completed_count
            for item in items
        )
        / support
    )


def _entry_price(
    side: StrategySide,
    quote: StrategyQuoteV1,
    execution: StrategyExecutionSpecificationV1,
) -> float:
    slip = execution.slippage_bps_per_side / _BPS_SCALE
    return (
        quote.ask * (1.0 + slip)
        if side is StrategySide.LONG
        else quote.bid * (1.0 - slip)
    )


def _exit_price(
    side: StrategySide,
    quote: StrategyQuoteV1,
    execution: StrategyExecutionSpecificationV1,
) -> float:
    slip = execution.slippage_bps_per_side / _BPS_SCALE
    return (
        quote.bid * (1.0 - slip)
        if side is StrategySide.LONG
        else quote.ask * (1.0 + slip)
    )


def _validate_information_audit(
    evaluation_case: StrategyEvaluationCaseV1,
    audit: InformationAuditReportV1,
) -> None:
    if not isinstance(audit, InformationAuditReportV1):
        raise TypeError("strategy information audit must use report v1")
    if audit.audit_id != evaluation_case.information_audit_id:
        raise ValueError("strategy case information audit identity differs")
    if audit.run_id != evaluation_case.run_id:
        raise ValueError("strategy case information audit run differs")
    if audit.manifest_id != evaluation_case.information_manifest_id:
        raise ValueError("strategy case information manifest differs")
    if audit.information_mode is not evaluation_case.information_mode:
        raise ValueError("strategy case information mode differs from audit")
    if not audit.accepted:
        raise ValueError(
            "strategy evaluation requires an accepted information audit"
        )
    if (
        evaluation_case.valid_for_backtest
        and not audit.valid_for_strategy_usefulness_claim
    ):
        raise ValueError(
            "strategy-valid case lacks strategy-usefulness audit approval"
        )


def _validate_alignment_groups(
    cases: Sequence[StrategyEvaluationCaseV1],
) -> None:
    groups: dict[str, tuple[str, int, int]] = {}
    seen_roles: set[tuple[str, StrategySourceKind, str, str | None]] = set()
    for item in cases:
        axis = (item.symbol, item.start_ns, item.end_ns)
        previous = groups.setdefault(item.alignment_window_id, axis)
        if previous != axis:
            raise ValueError(
                "aligned strategy cases differ in symbol or time bounds"
            )
        role = (
            item.alignment_window_id,
            item.source_kind,
            item.ensemble_member_id,
            item.broker_profile_id,
        )
        if role in seen_roles:
            raise ValueError(
                "aligned strategy source/member role is duplicated"
            )
        seen_roles.add(role)


def _validate_case_quote(
    evaluation_case: StrategyEvaluationCaseV1,
    quote: StrategyQuoteV1,
    previous_key: tuple[int, int, str] | None,
) -> None:
    if not isinstance(quote, StrategyQuoteV1):
        raise TypeError("strategy quote stream must use quote v1")
    if quote.symbol != evaluation_case.symbol:
        raise ValueError("strategy quote symbol differs from case")
    if (
        not evaluation_case.start_ns
        <= quote.event_time_ns
        < evaluation_case.end_ns
    ):
        raise ValueError(
            "strategy quote falls outside aligned half-open window"
        )
    if previous_key is not None and quote.order_key <= previous_key:
        raise ValueError("strategy quote stream is not strictly ordered")
    if (
        quote.ensemble_member_id is not None
        and quote.ensemble_member_id != evaluation_case.ensemble_member_id
    ):
        raise ValueError("strategy quote ensemble member differs from case")
    if (
        quote.broker_profile_id is not None
        and evaluation_case.broker_profile_id is not None
        and quote.broker_profile_id != evaluation_case.broker_profile_id
    ):
        raise ValueError("strategy quote broker profile differs from case")
    if evaluation_case.source_kind is StrategySourceKind.DERIVED_BARS:
        if quote.source_scope != evaluation_case.source_scope:
            raise ValueError("strategy bar quote scope differs from case")
        if quote.bar_interval_code != evaluation_case.bar_interval_code:
            raise ValueError("strategy bar quote interval differs from case")
    elif quote.source_scope is not None or quote.bar_interval_code is not None:
        raise ValueError(
            "non-bar strategy case received derived-bar quote metadata"
        )


def _validate_signal(
    signal: StrategySignalV1,
    quote: StrategyQuoteV1,
    specification: StrategySpecificationV1,
) -> None:
    if not isinstance(signal, StrategySignalV1):
        raise TypeError("strategy plugin emitted a non-v1 signal")
    if signal.strategy_specification_id != specification.specification_id:
        raise ValueError("strategy signal specification differs from engine")
    if signal.source_quote_id != quote.quote_id:
        raise ValueError("strategy signal is not bound to the current quote")
    if signal.decision_time_ns != quote.event_time_ns:
        raise ValueError(
            "strategy signal decision time differs from current quote"
        )


def _empty_window_result(
    evaluation_case: StrategyEvaluationCaseV1,
    status: StrategyWindowStatus,
    *,
    reason: str,
    invalid_for_backtest_reason: str | None,
) -> StrategyWindowResultV1:
    return StrategyWindowResultV1(
        case_id=evaluation_case.case_id,
        alignment_window_id=evaluation_case.alignment_window_id,
        source_kind=evaluation_case.source_kind,
        status=status,
        quote_count=0,
        signal_count=0,
        outcome_count=0,
        completed_outcome_count=0,
        missing_support_count=0,
        mean_interarrival_ns=0.0,
        max_interarrival_ns=0,
        mean_spread_bps=0.0,
        reason=reason,
        invalid_for_backtest_reason=invalid_for_backtest_reason,
    )


def _validate_window_status(result: StrategyWindowResultV1) -> None:
    if result.status is StrategyWindowStatus.COMPLETED:
        if result.completed_outcome_count < 1 or result.reason is not None:
            raise ValueError("completed strategy window status is inconsistent")
        return
    if result.reason is None:
        raise ValueError("non-completed strategy window requires a reason")
    if result.status is StrategyWindowStatus.NO_TRADE and result.signal_count:
        raise ValueError("no-trade strategy window cannot contain signals")
    if result.status in {
        StrategyWindowStatus.REFUSED,
        StrategyWindowStatus.FAILED,
    }:
        if result.quote_count or result.signal_count or result.slices:
            raise ValueError(
                "failed/refused strategy window cannot claim partial results"
            )


def _bounded_mapping(
    value: Mapping[str, JSONValue], label: str
) -> dict[str, JSONValue]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{label} must be a mapping")
    if len(value) > MAX_STRATEGY_PARAMETERS:
        raise ValueError(f"{label} exceeds item limit")
    normalized = {str(key): item for key, item in sorted(value.items())}
    _ensure_payload_size(normalized, MAX_STRATEGY_PARAMETER_BYTES)
    return normalized


def _ensure_payload_size(value: Mapping[str, JSONValue], maximum: int) -> None:
    encoded = canonical_contract_json(value).encode("utf-8")
    if len(encoded) > maximum:
        raise ValueError("strategy payload exceeds configured byte limit")


def _stable_id(prefix: str, value: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(value).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _required_text(value: Any) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError("required strategy text is empty")
    if len(normalized) > MAX_STRATEGY_TEXT:
        raise ValueError("strategy text exceeds length limit")
    return normalized


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    return _required_text(normalized)


def _normalized_symbol(value: Any) -> str:
    return _required_text(value).upper()


def _normalized_text_tuple(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted({_required_text(item) for item in values}))


def _single_or_mixed(values: Sequence[str], fallback: str | None) -> str | None:
    normalized = _normalized_text_tuple(values)
    if not normalized:
        return fallback
    if len(normalized) == 1:
        return normalized[0]
    return "mixed"


def _strict_int(value: Any, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{name} must be an integer")
    return value


def _nonnegative_int(value: Any, name: str) -> int:
    result = _strict_int(value, name)
    if result < 0:
        raise ValueError(f"{name} must be non-negative")
    return result


def _positive_int(value: Any, name: str) -> int:
    result = _strict_int(value, name)
    if result <= 0:
        raise ValueError(f"{name} must be positive")
    return result


def _bounded_int(value: Any, name: str, minimum: int, maximum: int) -> int:
    result = _strict_int(value, name)
    if not minimum <= result <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return result


def _finite_float(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _nonnegative_float(value: Any, name: str) -> float:
    result = _finite_float(value, name)
    if result < 0:
        raise ValueError(f"{name} must be non-negative")
    return result


def _positive_float(value: Any, name: str) -> float:
    result = _finite_float(value, name)
    if result <= 0:
        raise ValueError(f"{name} must be positive")
    return result


def _strict_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a boolean")
    return value


def _ratio(numerator: int, denominator: int) -> float:
    return numerator / denominator if denominator else 0.0


def _rounded(value: float, digits: int) -> float:
    return round(float(value), digits)


def _enum_value(enum_type: type[_EnumT], value: Any, label: str) -> _EnumT:
    if isinstance(value, enum_type):
        return value
    try:
        return enum_type(str(value).strip().lower())
    except ValueError as err:
        raise ValueError(f"unsupported {label}") from err


def _require_schema_version(value: str, expected: str, label: str) -> None:
    if value != expected:
        raise ValueError(f"unsupported {label} schema version")


def _require_schema(data: Mapping[str, Any], expected: str) -> None:
    if str(data.get("schema_version", "")) != expected:
        raise ValueError("unsupported strategy contract schema version")


def _require_derived(data: Mapping[str, Any], name: str, expected: Any) -> None:
    if data.get(name) != expected:
        raise ValueError(f"derived strategy field {name} differs")


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    return value


def _mapping_optional_text(data: Mapping[str, Any], name: str) -> str | None:
    return _optional_text(data.get(name))


def _sequence(value: Any, name: str) -> tuple[Any, ...]:
    if not isinstance(value, (list, tuple)):
        raise TypeError(f"{name} must be a sequence")
    return tuple(value)


def _mapping_sequence(value: Any, name: str) -> tuple[Mapping[str, Any], ...]:
    return tuple(_mapping(item, name) for item in _sequence(value, name))


def _json_mapping(text: str) -> Mapping[str, Any]:
    value = json.loads(text)
    if not isinstance(value, Mapping):
        raise TypeError("strategy JSON must contain an object")
    return value


__all__ = [
    "DEFAULT_STRATEGY_HORIZONS_NS",
    "REFERENCE_MOMENTUM_STRATEGY_ID",
    "REFERENCE_MOMENTUM_STRATEGY_VERSION",
    "STRATEGY_EVALUATION_CASE_SCHEMA_VERSION",
    "STRATEGY_EVALUATION_PLAN_SCHEMA_VERSION",
    "STRATEGY_EVALUATION_POLICY_SCHEMA_VERSION",
    "STRATEGY_EXECUTION_SPECIFICATION_SCHEMA_VERSION",
    "STRATEGY_INVALID_FOR_BACKTEST_LABEL",
    "STRATEGY_QUOTE_SCHEMA_VERSION",
    "STRATEGY_RESTORATION_RESULT_SCHEMA_VERSION",
    "STRATEGY_SENSITIVITY_REPORT_SCHEMA_VERSION",
    "STRATEGY_SIGNAL_SCHEMA_VERSION",
    "STRATEGY_SLICE_RESULT_SCHEMA_VERSION",
    "STRATEGY_SPECIFICATION_SCHEMA_VERSION",
    "STRATEGY_UNCERTAINTY_SUMMARY_SCHEMA_VERSION",
    "STRATEGY_WINDOW_RESULT_SCHEMA_VERSION",
    "ReferenceMomentumStrategyV1",
    "StrategyEvaluationCaseV1",
    "StrategyEvaluationFailure",
    "StrategyEvaluationPlanV1",
    "StrategyEvaluationPolicyV1",
    "StrategyExecutionSpecificationV1",
    "StrategyQuoteV1",
    "StrategyResourceLimitError",
    "StrategyRestorationResultV1",
    "StrategySensitivityReportV1",
    "StrategySide",
    "StrategySignalEngineV1",
    "StrategySignalStateV1",
    "StrategySignalV1",
    "StrategySliceResultV1",
    "StrategySourceKind",
    "StrategySpecificationV1",
    "StrategyUncertaintySummaryV1",
    "StrategyWindowResultV1",
    "StrategyWindowStatus",
    "evaluate_strategy_sensitivity",
    "strategy_sensitivity_benchmark_hooks",
]
