"""Bounded discrete Schrödinger-bridge reconstruction challenger.

The module implements the issue-455 research surface as a finite-state
Schrödinger bridge.  A train-only Markov reference law is scaled to explicit
source and broker-conditioned endpoint marginals with Sinkhorn/IPF.  Generated
state trajectories are sampled from the conditional Markov-bridge law.  Raw
observations stay outside the transported state and are returned unchanged.
"""

from __future__ import annotations

import hashlib
import json
import math
import platform
import random
import time
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from itertools import pairwise
from typing import Any, cast

from histdatacom.resource_usage import peak_rss_bytes
from histdatacom.runtime_contracts import JSONValue
from histdatacom.synthetic.benchmark import (
    BENCHMARK_EVENT_SCHEMA_VERSION,
    BenchmarkCandidateKind,
    BenchmarkCandidateV1,
    BenchmarkEventV1,
    BenchmarkGeneratorV1,
    BenchmarkScenarioV1,
)
from histdatacom.synthetic.broker_transfer import (
    BrokerProfileSelectionV1,
    BrokerTransferConfigV1,
    BrokerTransferStatus,
)
from histdatacom.synthetic.carving import HistoricalCarvingQuarantineV1
from histdatacom.synthetic.contracts import (
    SyntheticEventOrigin,
    SyntheticEventV1,
    canonical_contract_json,
    derive_anchor_interval_id,
)
from histdatacom.synthetic.event_clock import EventClockCalibrationWindowV1
from histdatacom.synthetic.generation import (
    CANDIDATE_ONLY_CONSTRAINT_SET_ID,
    MotifGenerationStatus,
)
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.streaming import (
    ReconstructionRunV1,
    ReconstructionWindowV1,
)

SB_RESOURCE_LIMITS_SCHEMA_VERSION = (
    "histdatacom.schrodinger-bridge-resource-limits.v1"
)
SB_CONFIG_SCHEMA_VERSION = "histdatacom.schrodinger-bridge-config.v1"
SB_BROKER_TARGET_SCHEMA_VERSION = (
    "histdatacom.schrodinger-bridge-broker-target.v1"
)
SB_WINDOW_CONTEXT_SCHEMA_VERSION = (
    "histdatacom.schrodinger-bridge-window-context.v1"
)
SB_PROTECTED_WINDOW_SCHEMA_VERSION = (
    "histdatacom.schrodinger-bridge-protected-window.v1"
)
SB_DATASET_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.schrodinger-bridge-dataset-manifest.v1"
)
SB_SOLVER_EVIDENCE_SCHEMA_VERSION = (
    "histdatacom.schrodinger-bridge-solver-evidence.v1"
)
SB_CHECKPOINT_SCHEMA_VERSION = "histdatacom.schrodinger-bridge-checkpoint.v1"
SB_FIT_RESULT_SCHEMA_VERSION = "histdatacom.schrodinger-bridge-fit-result.v1"
SB_GENERATION_LINEAGE_SCHEMA_VERSION = (
    "histdatacom.schrodinger-bridge-generation-lineage.v1"
)
SB_GENERATION_EVIDENCE_SCHEMA_VERSION = (
    "histdatacom.schrodinger-bridge-generation-evidence.v1"
)
SB_CANDIDATE_LINEAGE_SCHEMA_VERSION = (
    "histdatacom.schrodinger-bridge-candidate-lineage.v1"
)
SB_CANDIDATE_BATCH_SCHEMA_VERSION = (
    "histdatacom.schrodinger-bridge-candidate-batch.v1"
)

SB_ARCHITECTURE = "finite_state_markov_sinkhorn_cpu_v1"
SB_IMPLEMENTATION_VERSION = "1.0.0"
SB_GENERATOR_ID = "histdatacom.schrodinger-bridge.markov-sinkhorn-cpu-v1"
SB_HYPOTHESIS_ID = "issue-455-joint-heldout-improvement-v1"
SB_PROMOTION_POLICY = "separate-evidence-backed-promotion-issue-required-v1"
NANOSECONDS_PER_SECOND = 1_000_000_000
MARK_STATES = ("ask_only", "bid_only", "joint", "unchanged")
TRIANGLE_SYMBOLS = ("EURGBP", "EURUSD", "GBPUSD")
ASSIGNMENT_KINDS = ("epoch", "transition")
_EXPOSURES: Mapping[str, tuple[int, int, int]] = {
    "EURGBP": (1, -1, 0),
    "EURUSD": (1, 0, -1),
    "GBPUSD": (0, 1, -1),
}


class SchrodingerBridgeFitError(RuntimeError):
    """Raised when a fitted bridge is absent or inconsistent."""


class SchrodingerBridgeGenerationError(RuntimeError):
    """Raised when bridge generation cannot complete safely."""


class _BridgeRefusal(SchrodingerBridgeGenerationError):
    """Internal all-or-nothing refusal marker."""


class _SolverRefusal(_BridgeRefusal):
    """Solver refusal that preserves the complete bounded trace."""

    def __init__(
        self, reason: str, evidence: SchrodingerBridgeSolverEvidenceV1
    ) -> None:
        super().__init__(reason)
        self.evidence = evidence


class SchrodingerBridgeFitStatus(str, Enum):
    """Terminal bridge-fit state."""

    FITTED = "fitted"
    REFUSED = "refused"
    FAILED = "failed"


class SchrodingerBridgeGenerationStatus(str, Enum):
    """Terminal bridge-generation state."""

    GENERATED = "generated"
    EMPTY = "empty"
    REFUSED = "refused"
    FAILED = "failed"


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode()
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _required_text(value: Any, name: str = "text") -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 2048:
        raise ValueError(f"{name} is invalid")
    return value


def _optional_text(value: Any, name: str = "text") -> str | None:
    return None if value is None else _required_text(value, name)


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    return value


def _bounded_int(value: Any, name: str, minimum: int, maximum: int) -> int:
    result = _strict_int(value, name)
    if not minimum <= result <= maximum:
        raise ValueError(f"{name} is outside bounds")
    return result


def _strict_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be boolean")
    return value


def _finite_float(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be an object")
    return cast(Mapping[str, Any], value)


def _sequence(value: Any, name: str) -> Sequence[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise TypeError(f"{name} must be an array")
    return value


def _json_mapping(text: str, maximum: int) -> Mapping[str, Any]:
    if not isinstance(text, str) or len(text.encode()) > maximum:
        raise ValueError("JSON payload exceeds the bounded limit")
    return _mapping(json.loads(text), "JSON payload")


def _runtime_metadata() -> dict[str, JSONValue]:
    return {
        "operating_system": platform.system(),
        "machine": platform.machine(),
        "python_implementation": platform.python_implementation(),
        "python_version": platform.python_version(),
        "accelerator_policy": "cpu-only-no-accelerator-runtime",
        "deterministic_math_scope": "stdlib-float-fixed-order-v1",
    }


def _event_key(event: BenchmarkEventV1) -> tuple[Any, ...]:
    return (
        event.event_time_ns,
        event.symbol,
        event.event_sequence,
        event.source_event_id,
    )


def _event_content_digest(events: Sequence[BenchmarkEventV1]) -> str:
    payload = [item.to_dict() for item in sorted(events, key=_event_key)]
    return hashlib.sha256(canonical_contract_json(payload).encode()).hexdigest()


def _anchor_digest(events: Sequence[BenchmarkEventV1]) -> str:
    payload = sorted(
        (
            item.symbol.lower(),
            item.event_time_ns,
            item.event_sequence,
            item.bid,
            item.ask,
        )
        for item in events
    )
    return hashlib.sha256(canonical_contract_json(payload).encode()).hexdigest()


def _event_mark(
    previous: BenchmarkEventV1 | None,
    event: BenchmarkEventV1,
) -> str:
    if previous is None:
        return "joint"
    bid_changed = not math.isclose(
        event.bid, previous.bid, rel_tol=0.0, abs_tol=1e-15
    )
    ask_changed = not math.isclose(
        event.ask, previous.ask, rel_tol=0.0, abs_tol=1e-15
    )
    if bid_changed and ask_changed:
        return "joint"
    if bid_changed:
        return "bid_only"
    if ask_changed:
        return "ask_only"
    return "unchanged"


def _state_name(bin_index: int, symbol: str, mark: str) -> str:
    return f"bin={bin_index}|symbol={symbol}|mark={mark}"


def _parse_state(value: str) -> tuple[int, str, str]:
    parts = value.split("|")
    if len(parts) != 3:
        raise ValueError("bridge state is malformed")
    try:
        bin_index = int(parts[0].removeprefix("bin="))
    except ValueError as err:
        raise ValueError("bridge state bin is malformed") from err
    symbol = parts[1].removeprefix("symbol=")
    mark = parts[2].removeprefix("mark=")
    if symbol not in TRIANGLE_SYMBOLS or mark not in MARK_STATES:
        raise ValueError("bridge state has unsupported symbol or mark")
    return bin_index, symbol, mark


def _matrix_vector(
    matrix: Sequence[Sequence[float]], vector: Sequence[float]
) -> list[float]:
    return [
        math.fsum(value * vector[index] for index, value in enumerate(row))
        for row in matrix
    ]


def _transpose_vector(
    matrix: Sequence[Sequence[float]], vector: Sequence[float]
) -> list[float]:
    size = len(matrix)
    return [
        math.fsum(matrix[row][column] * vector[row] for row in range(size))
        for column in range(size)
    ]


def _matrix_multiply(
    left: Sequence[Sequence[float]], right: Sequence[Sequence[float]]
) -> tuple[tuple[float, ...], ...]:
    size = len(left)
    columns = [
        tuple(right[row][column] for row in range(size))
        for column in range(size)
    ]
    return tuple(
        tuple(
            math.fsum(a * b for a, b in zip(row, column)) for column in columns
        )
        for row in left
    )


def _matrix_power(
    matrix: Sequence[Sequence[float]], exponent: int
) -> tuple[tuple[float, ...], ...]:
    size = len(matrix)
    result: tuple[tuple[float, ...], ...] = tuple(
        tuple(1.0 if row == column else 0.0 for column in range(size))
        for row in range(size)
    )
    base = tuple(tuple(item for item in row) for row in matrix)
    remaining = exponent
    while remaining:
        if remaining % 2:
            result = _matrix_multiply(result, base)
        remaining //= 2
        if remaining:
            base = _matrix_multiply(base, base)
    return result


def _sample_index(weights: Sequence[float], rng: random.Random) -> int:
    total = math.fsum(weights)
    if not math.isfinite(total) or total <= 0.0:
        raise _BridgeRefusal("sampling_distribution_has_no_support")
    threshold = rng.random() * total
    cumulative = 0.0
    for index, value in enumerate(weights):
        cumulative += value
        if threshold < cumulative:
            return index
    return len(weights) - 1


@dataclass(frozen=True, slots=True)
class SchrodingerBridgeResourceLimitsV1:
    """Independent fit, solver, generation, and serialization limits."""

    max_fit_windows: int = 32
    max_events_per_window: int = 8192
    max_states: int = 256
    max_solver_iterations: int = 2000
    max_solver_work: int = 100_000_000
    max_residual_trace: int = 256
    max_checkpoint_bytes: int = 32 * 1024 * 1024
    max_fit_wall_time_ms: int = 120_000
    max_fit_memory_bytes: int = 1024 * 1024 * 1024
    max_history_events: int = 256
    max_generation_events: int = 16_384
    max_generation_work: int = 20_000_000
    max_generation_wall_time_ms: int = 60_000
    max_generation_memory_bytes: int = 512 * 1024 * 1024
    max_candidate_amplification: float = 8.0
    max_json_bytes: int = 32 * 1024 * 1024
    schema_version: str = SB_RESOURCE_LIMITS_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SB_RESOURCE_LIMITS_SCHEMA_VERSION:
            raise ValueError("unsupported bridge resource schema")
        bounds = {
            "max_fit_windows": (2, 1024),
            "max_events_per_window": (2, 1_000_000),
            "max_states": (8, 4096),
            "max_solver_iterations": (1, 100_000),
            "max_solver_work": (1, 10**12),
            "max_residual_trace": (1, 4096),
            "max_checkpoint_bytes": (1024, 1024**3),
            "max_fit_wall_time_ms": (1, 3_600_000),
            "max_fit_memory_bytes": (1024, 64 * 1024**3),
            "max_history_events": (0, 1_000_000),
            "max_generation_events": (1, 1_000_000),
            "max_generation_work": (1, 10**12),
            "max_generation_wall_time_ms": (1, 3_600_000),
            "max_generation_memory_bytes": (1024, 64 * 1024**3),
            "max_json_bytes": (1024, 1024**3),
        }
        for name, (minimum, maximum) in bounds.items():
            object.__setattr__(
                self,
                name,
                _bounded_int(getattr(self, name), name, minimum, maximum),
            )
        amplification = _finite_float(
            self.max_candidate_amplification, "max_candidate_amplification"
        )
        if not 1.0 <= amplification <= 100.0:
            raise ValueError("candidate amplification is outside bounds")
        object.__setattr__(self, "max_candidate_amplification", amplification)

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            name: cast(JSONValue, getattr(self, name))
            for name in self.__dataclass_fields__
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> SchrodingerBridgeResourceLimitsV1:
        if set(data) != set(cls.__dataclass_fields__):
            raise ValueError("bridge resource fields differ")
        values = dict(data)
        values["max_candidate_amplification"] = _finite_float(
            values["max_candidate_amplification"], "max_candidate_amplification"
        )
        for name in set(cls.__dataclass_fields__) - {
            "schema_version",
            "max_candidate_amplification",
        }:
            values[name] = _strict_int(values[name], name)
        return cls(**values)


@dataclass(frozen=True, slots=True)
class SchrodingerBridgeConfigV1:
    """Fixed finite-state reference law and Sinkhorn solver policy."""

    architecture: str = SB_ARCHITECTURE
    time_bin_count: int = 8
    bridge_steps: int = 4
    entropic_regularization: float = 0.75
    sinkhorn_tolerance: float = 1e-9
    sinkhorn_max_iterations: int = 500
    transition_smoothing: float = 0.25
    maximum_transport_cost: float = 8.0
    time_cost_weight: float = 1.0
    mark_cost_weight: float = 1.0
    cross_currency_cost_weight: float = 0.5
    triangle_projection_strength: float = 0.25
    base_seed: int = 455
    accelerator_count: int = 0
    state_policy: str = "normalized-time-bin-x-symbol-x-quote-transition-v1"
    reference_process: str = "train-only-first-order-lazy-markov-chain-v1"
    solver_policy: str = "finite-state-sinkhorn-ipf-v1"
    path_sampling_policy: str = "conditional-markov-bridge-doob-h-v1"
    cardinality_policy: str = "broker-target-poisson-missing-count-v1"
    endpoint_policy: str = "half-open-core-strict-anchor-interior-v1"
    anchor_policy: str = "observed-external-immutable-v1"
    quarantine_policy: str = "half-open-hard-exclusion-v1"
    streaming_policy: str = "bounded-strict-prior-last-state-v1"
    quote_projection_policy: str = "anchor-linear-mark-triangle-log-blend-v1"
    promotion_policy: str = SB_PROMOTION_POLICY
    limits: SchrodingerBridgeResourceLimitsV1 = field(
        default_factory=SchrodingerBridgeResourceLimitsV1
    )
    config_id: str = ""
    schema_version: str = SB_CONFIG_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SB_CONFIG_SCHEMA_VERSION:
            raise ValueError("unsupported bridge config schema")
        if self.architecture != SB_ARCHITECTURE:
            raise ValueError("bridge architecture is not fixed")
        if not isinstance(self.limits, SchrodingerBridgeResourceLimitsV1):
            raise TypeError("bridge limits require the v1 contract")
        object.__setattr__(
            self,
            "time_bin_count",
            _bounded_int(
                self.time_bin_count,
                "time_bin_count",
                2,
                self.limits.max_states // 12,
            ),
        )
        object.__setattr__(
            self,
            "bridge_steps",
            _bounded_int(self.bridge_steps, "bridge_steps", 2, 32),
        )
        object.__setattr__(
            self,
            "sinkhorn_max_iterations",
            _bounded_int(
                self.sinkhorn_max_iterations,
                "sinkhorn_max_iterations",
                1,
                self.limits.max_solver_iterations,
            ),
        )
        for name, minimum, maximum in (
            ("entropic_regularization", 1e-6, 100.0),
            ("sinkhorn_tolerance", 1e-15, 0.1),
            ("transition_smoothing", 0.0, 100.0),
            ("maximum_transport_cost", 0.0, 1000.0),
            ("time_cost_weight", 0.0, 100.0),
            ("mark_cost_weight", 0.0, 100.0),
            ("cross_currency_cost_weight", 0.0, 100.0),
            ("triangle_projection_strength", 0.0, 1.0),
        ):
            value = _finite_float(getattr(self, name), name)
            if not minimum <= value <= maximum:
                raise ValueError(f"{name} is outside bounds")
            object.__setattr__(self, name, value)
        object.__setattr__(
            self,
            "base_seed",
            _bounded_int(self.base_seed, "base_seed", 0, 2**63 - 1),
        )
        if _strict_int(self.accelerator_count, "accelerator_count") != 0:
            raise ValueError("bridge refuses accelerator requests")
        for name in (
            "state_policy",
            "reference_process",
            "solver_policy",
            "path_sampling_policy",
            "cardinality_policy",
            "endpoint_policy",
            "anchor_policy",
            "quarantine_policy",
            "streaming_policy",
            "quote_projection_policy",
            "promotion_policy",
        ):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        if self.promotion_policy != SB_PROMOTION_POLICY:
            raise ValueError("bridge promotion policy cannot be weakened")
        expected = _stable_id(
            "schrodinger-bridge-config", self.identity_payload()
        )
        if self.config_id and self.config_id != expected:
            raise ValueError("bridge config_id differs")
        object.__setattr__(self, "config_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "architecture": self.architecture,
            "time_bin_count": self.time_bin_count,
            "bridge_steps": self.bridge_steps,
            "entropic_regularization": self.entropic_regularization,
            "sinkhorn_tolerance": self.sinkhorn_tolerance,
            "sinkhorn_max_iterations": self.sinkhorn_max_iterations,
            "transition_smoothing": self.transition_smoothing,
            "maximum_transport_cost": self.maximum_transport_cost,
            "time_cost_weight": self.time_cost_weight,
            "mark_cost_weight": self.mark_cost_weight,
            "cross_currency_cost_weight": self.cross_currency_cost_weight,
            "triangle_projection_strength": self.triangle_projection_strength,
            "base_seed": self.base_seed,
            "accelerator_count": self.accelerator_count,
            "state_policy": self.state_policy,
            "reference_process": self.reference_process,
            "solver_policy": self.solver_policy,
            "path_sampling_policy": self.path_sampling_policy,
            "cardinality_policy": self.cardinality_policy,
            "endpoint_policy": self.endpoint_policy,
            "anchor_policy": self.anchor_policy,
            "quarantine_policy": self.quarantine_policy,
            "streaming_policy": self.streaming_policy,
            "quote_projection_policy": self.quote_projection_policy,
            "promotion_policy": self.promotion_policy,
            "limits": self.limits.to_dict(),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "config_id": self.config_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> SchrodingerBridgeConfigV1:
        expected = set(cls.__dataclass_fields__) | set(cls().identity_payload())
        if set(data) != expected:
            raise ValueError("bridge config fields differ")
        kwargs: dict[str, Any] = {
            name: data[name]
            for name in cls.__dataclass_fields__
            if name not in {"limits"}
        }
        kwargs["limits"] = SchrodingerBridgeResourceLimitsV1.from_dict(
            _mapping(data["limits"], "limits")
        )
        return cls(**kwargs)

    @classmethod
    def from_json(cls, text: str) -> SchrodingerBridgeConfigV1:
        return cls.from_dict(_json_mapping(text, 32 * 1024 * 1024))


def default_schrodinger_bridge_config() -> SchrodingerBridgeConfigV1:
    """Return the one default bounded issue-455 config."""
    return SchrodingerBridgeConfigV1()


@dataclass(frozen=True, slots=True)
class SchrodingerBridgeBrokerTargetV1:
    """Explicit broker-conditioned endpoint and cardinality target law."""

    broker_profile_selection_id: str
    fingerprint_id: str
    broker_support_status: str
    selected_at_utc_ns: int
    profile_effective_start_utc_ns: int
    profile_effective_end_utc_ns: int | None
    transfer_config_id: str
    transfer_strength: float
    target_mean_event_count: float
    target_cadence_ns: float
    symbol_weights: Mapping[str, float]
    mark_weights: Mapping[str, float]
    time_bin_weights: tuple[float, ...]
    spread_target: float | None
    currency_triangle_id: str = "EURGBP-EURUSD-GBPUSD-log-mid-identity-v1"
    delivery_mode: str = "broker-conditioned-research-target"
    target_id: str = ""
    schema_version: str = SB_BROKER_TARGET_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SB_BROKER_TARGET_SCHEMA_VERSION:
            raise ValueError("unsupported bridge broker-target schema")
        for name in (
            "broker_profile_selection_id",
            "fingerprint_id",
            "broker_support_status",
            "transfer_config_id",
            "currency_triangle_id",
            "delivery_mode",
        ):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        selected = _bounded_int(
            self.selected_at_utc_ns,
            "selected_at_utc_ns",
            0,
            2**63 - 1,
        )
        effective_start = _bounded_int(
            self.profile_effective_start_utc_ns,
            "profile_effective_start_utc_ns",
            0,
            2**63 - 1,
        )
        effective_end = self.profile_effective_end_utc_ns
        if effective_end is not None:
            effective_end = _bounded_int(
                effective_end,
                "profile_effective_end_utc_ns",
                0,
                2**63 - 1,
            )
            if effective_end <= effective_start:
                raise ValueError("bridge broker profile interval is empty")
        if selected < effective_start or (
            effective_end is not None and selected >= effective_end
        ):
            raise ValueError("bridge broker profile is stale at selection")
        object.__setattr__(self, "selected_at_utc_ns", selected)
        object.__setattr__(
            self,
            "profile_effective_start_utc_ns",
            effective_start,
        )
        object.__setattr__(
            self,
            "profile_effective_end_utc_ns",
            effective_end,
        )
        strength = _finite_float(self.transfer_strength, "transfer_strength")
        if not 0.0 < strength <= 1.0:
            raise ValueError(
                "bridge target requires positive transfer strength"
            )
        object.__setattr__(self, "transfer_strength", strength)
        for name in ("target_mean_event_count", "target_cadence_ns"):
            value = _finite_float(getattr(self, name), name)
            if value <= 0.0:
                raise ValueError(f"{name} must be positive")
            object.__setattr__(self, name, value)
        symbols = {
            str(name).upper(): _finite_float(value, f"symbol_weights.{name}")
            for name, value in self.symbol_weights.items()
        }
        if (
            set(symbols) != set(TRIANGLE_SYMBOLS)
            or any(value < 0.0 for value in symbols.values())
            or math.fsum(symbols.values()) <= 0.0
        ):
            raise ValueError("bridge target symbol weights are invalid")
        object.__setattr__(
            self, "symbol_weights", dict(sorted(symbols.items()))
        )
        marks = {
            str(name): _finite_float(value, f"mark_weights.{name}")
            for name, value in self.mark_weights.items()
        }
        if (
            set(marks) != set(MARK_STATES)
            or any(value < 0.0 for value in marks.values())
            or math.fsum(marks.values()) <= 0.0
        ):
            raise ValueError("bridge target mark weights are invalid")
        object.__setattr__(self, "mark_weights", dict(sorted(marks.items())))
        bins = tuple(
            _finite_float(item, "time_bin_weight")
            for item in self.time_bin_weights
        )
        if (
            len(bins) < 2
            or any(item < 0.0 for item in bins)
            or math.fsum(bins) <= 0
        ):
            raise ValueError("bridge target time-bin weights are invalid")
        object.__setattr__(self, "time_bin_weights", bins)
        if self.spread_target is not None:
            spread = _finite_float(self.spread_target, "spread_target")
            if spread < 0.0:
                raise ValueError("spread_target must be non-negative")
            object.__setattr__(self, "spread_target", spread)
        expected = _stable_id(
            "schrodinger-bridge-broker-target", self.identity_payload()
        )
        if self.target_id and self.target_id != expected:
            raise ValueError("bridge target_id differs")
        object.__setattr__(self, "target_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "broker_profile_selection_id": self.broker_profile_selection_id,
            "fingerprint_id": self.fingerprint_id,
            "broker_support_status": self.broker_support_status,
            "selected_at_utc_ns": self.selected_at_utc_ns,
            "profile_effective_start_utc_ns": (
                self.profile_effective_start_utc_ns
            ),
            "profile_effective_end_utc_ns": (self.profile_effective_end_utc_ns),
            "transfer_config_id": self.transfer_config_id,
            "transfer_strength": self.transfer_strength,
            "target_mean_event_count": self.target_mean_event_count,
            "target_cadence_ns": self.target_cadence_ns,
            "symbol_weights": dict(self.symbol_weights),
            "mark_weights": dict(self.mark_weights),
            "time_bin_weights": list(self.time_bin_weights),
            "spread_target": self.spread_target,
            "currency_triangle_id": self.currency_triangle_id,
            "delivery_mode": self.delivery_mode,
            "conditioning_semantics": (
                "endpoint-mass-and-cardinality-before-anchor-projection-v1"
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "target_id": self.target_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> SchrodingerBridgeBrokerTargetV1:
        allowed = {
            "schema_version",
            "broker_profile_selection_id",
            "fingerprint_id",
            "broker_support_status",
            "selected_at_utc_ns",
            "profile_effective_start_utc_ns",
            "profile_effective_end_utc_ns",
            "transfer_config_id",
            "transfer_strength",
            "target_mean_event_count",
            "target_cadence_ns",
            "symbol_weights",
            "mark_weights",
            "time_bin_weights",
            "spread_target",
            "currency_triangle_id",
            "delivery_mode",
            "conditioning_semantics",
            "target_id",
        }
        if set(data) != allowed:
            raise ValueError("bridge broker-target fields differ")
        return cls(
            broker_profile_selection_id=str(
                data["broker_profile_selection_id"]
            ),
            fingerprint_id=str(data["fingerprint_id"]),
            broker_support_status=str(data["broker_support_status"]),
            selected_at_utc_ns=_strict_int(
                data["selected_at_utc_ns"], "selected_at_utc_ns"
            ),
            profile_effective_start_utc_ns=_strict_int(
                data["profile_effective_start_utc_ns"],
                "profile_effective_start_utc_ns",
            ),
            profile_effective_end_utc_ns=(
                None
                if data["profile_effective_end_utc_ns"] is None
                else _strict_int(
                    data["profile_effective_end_utc_ns"],
                    "profile_effective_end_utc_ns",
                )
            ),
            transfer_config_id=str(data["transfer_config_id"]),
            transfer_strength=_finite_float(
                data["transfer_strength"], "transfer_strength"
            ),
            target_mean_event_count=_finite_float(
                data["target_mean_event_count"], "target_mean_event_count"
            ),
            target_cadence_ns=_finite_float(
                data["target_cadence_ns"], "target_cadence_ns"
            ),
            symbol_weights={
                str(name): _finite_float(value, str(name))
                for name, value in _mapping(
                    data["symbol_weights"], "symbol_weights"
                ).items()
            },
            mark_weights={
                str(name): _finite_float(value, str(name))
                for name, value in _mapping(
                    data["mark_weights"], "mark_weights"
                ).items()
            },
            time_bin_weights=tuple(
                _finite_float(item, "time_bin_weight")
                for item in _sequence(
                    data["time_bin_weights"], "time_bin_weights"
                )
            ),
            spread_target=(
                None
                if data["spread_target"] is None
                else _finite_float(data["spread_target"], "spread_target")
            ),
            currency_triangle_id=str(data["currency_triangle_id"]),
            delivery_mode=str(data["delivery_mode"]),
            target_id=str(data["target_id"]),
            schema_version=str(data["schema_version"]),
        )

    @classmethod
    def from_json(cls, text: str) -> SchrodingerBridgeBrokerTargetV1:
        return cls.from_dict(_json_mapping(text, 16 * 1024 * 1024))


def _broker_cadence(metrics: Mapping[str, float]) -> float | None:
    cadence = metrics.get("active_quote_interarrival_ns")
    if cadence is None:
        cadence = metrics.get("quote_interarrival_ns")
    intensity = metrics.get("quote_intensity_hz")
    if cadence is None and intensity is not None and intensity > 0.0:
        cadence = NANOSECONDS_PER_SECOND / intensity
    if cadence is None or not math.isfinite(cadence) or cadence <= 0.0:
        return None
    burst = max(0.0, metrics.get("burst_interval_rate", 0.0))
    quiet = max(0.0, metrics.get("quiet_interval_rate", 0.0))
    outage_rate = max(0.0, metrics.get("event_kind.outage_end_rate", 0.0))
    outage_duration = max(0.0, metrics.get("outage_or_gap_duration_ns", 0.0))
    structure = max(0.25, 1.0 + quiet - 0.5 * burst)
    structure += outage_rate * min(10.0, outage_duration / cadence)
    return max(1.0, cadence * structure)


def build_schrodinger_bridge_broker_target(
    selection: BrokerProfileSelectionV1,
    transfer_config: BrokerTransferConfigV1,
    calibration_windows: Sequence[EventClockCalibrationWindowV1],
    *,
    time_bin_count: int = 8,
) -> SchrodingerBridgeBrokerTargetV1:
    """Derive an explicit target law from one supported broker profile cell."""
    if not isinstance(selection, BrokerProfileSelectionV1):
        raise TypeError("bridge target requires a broker profile selection")
    if selection.status is BrokerTransferStatus.REFUSED:
        raise ValueError("refused broker profile cannot define a target law")
    if selection.selected_at_utc_ns < selection.profile_effective_start_utc_ns:
        raise ValueError("broker profile is not yet effective at selection")
    if (
        selection.profile_effective_end_utc_ns is not None
        and selection.selected_at_utc_ns
        >= selection.profile_effective_end_utc_ns
    ):
        raise ValueError("stale broker profile cannot define a target law")
    if not isinstance(transfer_config, BrokerTransferConfigV1):
        raise TypeError("bridge target requires a broker transfer config")
    if transfer_config.strength <= 0.0:
        raise ValueError("zero-strength broker transfer cannot define a target")
    windows = tuple(calibration_windows)
    if not windows:
        raise ValueError("bridge target requires calibration windows")
    durations = [item.end_ns - item.start_ns for item in windows]
    if any(item <= 0 for item in durations):
        raise ValueError("bridge target calibration duration is invalid")
    historical_count = math.fsum(len(item.events) for item in windows) / len(
        windows
    )
    mean_duration = math.fsum(durations) / len(durations)
    cadence = _broker_cadence(selection.metrics)
    if cadence is None:
        raise ValueError("broker profile lacks supported cadence evidence")
    broker_count = mean_duration / cadence
    strength = transfer_config.strength
    target_count = (1.0 - strength) * historical_count + strength * broker_count
    stale = max(0.0, selection.metrics.get("stale_quote_rate", 0.0))
    duplicate = max(0.0, selection.metrics.get("exact_duplicate_rate", 0.0))
    unchanged_weight = 1.0 + strength * min(10.0, stale + duplicate)
    symbol_weights = {
        symbol: max(
            0.0,
            selection.metrics.get(f"symbol_weight.{symbol}", 1.0),
        )
        for symbol in TRIANGLE_SYMBOLS
    }
    return SchrodingerBridgeBrokerTargetV1(
        broker_profile_selection_id=selection.selection_id,
        fingerprint_id=selection.fingerprint_id,
        broker_support_status=selection.support_status,
        selected_at_utc_ns=selection.selected_at_utc_ns,
        profile_effective_start_utc_ns=(
            selection.profile_effective_start_utc_ns
        ),
        profile_effective_end_utc_ns=(selection.profile_effective_end_utc_ns),
        transfer_config_id=transfer_config.config_id,
        transfer_strength=strength,
        target_mean_event_count=max(1.0, target_count),
        target_cadence_ns=cadence,
        symbol_weights=symbol_weights,
        mark_weights={
            "ask_only": 1.0,
            "bid_only": 1.0,
            "joint": 1.0,
            "unchanged": unchanged_weight,
        },
        time_bin_weights=tuple(1.0 for _ in range(time_bin_count)),
        spread_target=selection.metrics.get("spread"),
    )


@dataclass(frozen=True, slots=True)
class SchrodingerBridgeWindowContextV1:
    """Point-in-time session and feed assignment for one whole window."""

    window_id: str
    session: str
    technology_assignment_kind: str
    technology_label: str
    feed_epoch_definition_id: str
    epoch_id: str | None = None
    boundary_id: str | None = None
    boundary_support: float | None = None
    uncertainty_start_period: str | None = None
    uncertainty_end_period: str | None = None
    observed_context_id: str | None = None
    observed_context_available_ns: int | None = None
    observed_context_used_ns: int | None = None
    context_id: str = ""
    schema_version: str = SB_WINDOW_CONTEXT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SB_WINDOW_CONTEXT_SCHEMA_VERSION:
            raise ValueError("unsupported bridge context schema")
        for name in (
            "window_id",
            "session",
            "technology_assignment_kind",
            "technology_label",
            "feed_epoch_definition_id",
        ):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        if self.technology_assignment_kind not in ASSIGNMENT_KINDS:
            raise ValueError("bridge technology assignment is invalid")
        boundary_fields = (
            self.boundary_id,
            self.boundary_support,
            self.uncertainty_start_period,
            self.uncertainty_end_period,
        )
        if self.technology_assignment_kind == "transition":
            if self.epoch_id is not None or any(
                item is None for item in boundary_fields
            ):
                raise ValueError("bridge transition context is inconsistent")
            object.__setattr__(
                self, "boundary_id", _required_text(self.boundary_id)
            )
            support = _finite_float(self.boundary_support, "boundary_support")
            if not 0.0 <= support <= 1.0:
                raise ValueError("bridge boundary support is outside bounds")
            object.__setattr__(self, "boundary_support", support)
            object.__setattr__(
                self,
                "uncertainty_start_period",
                _required_text(self.uncertainty_start_period),
            )
            object.__setattr__(
                self,
                "uncertainty_end_period",
                _required_text(self.uncertainty_end_period),
            )
        else:
            object.__setattr__(self, "epoch_id", _required_text(self.epoch_id))
            if any(item is not None for item in boundary_fields):
                raise ValueError("stable bridge epoch has transition evidence")
        object.__setattr__(
            self,
            "observed_context_id",
            _optional_text(self.observed_context_id),
        )
        for name in (
            "observed_context_available_ns",
            "observed_context_used_ns",
        ):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(
                    self, name, _bounded_int(value, name, 0, 2**63 - 1)
                )
        expected = _stable_id(
            "schrodinger-bridge-window-context", self.identity_payload()
        )
        if self.context_id and self.context_id != expected:
            raise ValueError("bridge context_id differs")
        object.__setattr__(self, "context_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "window_id": self.window_id,
            "session": self.session,
            "technology_assignment_kind": self.technology_assignment_kind,
            "technology_label": self.technology_label,
            "feed_epoch_definition_id": self.feed_epoch_definition_id,
            "epoch_id": self.epoch_id,
            "boundary_id": self.boundary_id,
            "boundary_support": self.boundary_support,
            "uncertainty_start_period": self.uncertainty_start_period,
            "uncertainty_end_period": self.uncertainty_end_period,
            "observed_context_id": self.observed_context_id,
            "observed_context_available_ns": self.observed_context_available_ns,
            "observed_context_used_ns": self.observed_context_used_ns,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "context_id": self.context_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> SchrodingerBridgeWindowContextV1:
        if set(data) != set(cls.__dataclass_fields__):
            raise ValueError("bridge context fields differ")
        return cls(**dict(data))

    @classmethod
    def from_json(cls, text: str) -> SchrodingerBridgeWindowContextV1:
        return cls.from_dict(_json_mapping(text, 8 * 1024 * 1024))


@dataclass(frozen=True, slots=True)
class SchrodingerBridgeProtectedWindowV1:
    """Row-free identity for an untouched validation or final window."""

    window_id: str
    role: str
    start_ns: int
    end_ns: int
    event_count: int
    event_content_sha256: str
    context_id: str
    near_signature: int
    schema_version: str = SB_PROTECTED_WINDOW_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SB_PROTECTED_WINDOW_SCHEMA_VERSION:
            raise ValueError("unsupported bridge protected-window schema")
        object.__setattr__(self, "window_id", _required_text(self.window_id))
        if self.role not in {"validation", "final_holdout"}:
            raise ValueError("bridge protected role is invalid")
        start = _bounded_int(self.start_ns, "start_ns", 0, 2**63 - 1)
        end = _bounded_int(self.end_ns, "end_ns", 0, 2**63 - 1)
        if end <= start:
            raise ValueError("bridge protected interval is empty")
        object.__setattr__(self, "start_ns", start)
        object.__setattr__(self, "end_ns", end)
        object.__setattr__(
            self,
            "event_count",
            _bounded_int(self.event_count, "event_count", 0, 10**9),
        )
        digest = _required_text(self.event_content_sha256)
        if len(digest) != 64 or any(
            item not in "0123456789abcdef" for item in digest
        ):
            raise ValueError("protected event digest is invalid")
        object.__setattr__(self, "context_id", _required_text(self.context_id))
        object.__setattr__(
            self,
            "near_signature",
            _bounded_int(self.near_signature, "near_signature", 0, 2**64 - 1),
        )

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "window_id": self.window_id,
            "role": self.role,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "event_count": self.event_count,
            "event_content_sha256": self.event_content_sha256,
            "context_id": self.context_id,
            "near_signature": self.near_signature,
            "rows_inline": False,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> SchrodingerBridgeProtectedWindowV1:
        allowed = set(cls.__dataclass_fields__) | {"rows_inline"}
        if set(data) != allowed or data.get("rows_inline") is not False:
            raise ValueError("bridge protected-window fields differ")
        return cls(**{name: data[name] for name in cls.__dataclass_fields__})


def _near_signature(events: Sequence[BenchmarkEventV1]) -> int:
    bits = [0] * 64
    for event in events:
        token = (
            f"{event.symbol}|{event.event_time_ns // 1_000_000}|"
            f"{event.bid:.7f}|{event.ask:.7f}"
        )
        digest = int.from_bytes(
            hashlib.sha256(token.encode()).digest()[:8], "big"
        )
        for index in range(64):
            bits[index] += 1 if digest & (1 << index) else -1
    return sum(1 << index for index, value in enumerate(bits) if value >= 0)


def build_schrodinger_bridge_protected_window(
    window: EventClockCalibrationWindowV1,
    context: SchrodingerBridgeWindowContextV1,
    *,
    role: str,
) -> SchrodingerBridgeProtectedWindowV1:
    """Reduce a holdout to row-free leakage evidence before fitting."""
    if context.window_id != window.window_id:
        raise ValueError("bridge protected context and window differ")
    events = tuple(window.events)
    return SchrodingerBridgeProtectedWindowV1(
        window_id=window.window_id,
        role=role,
        start_ns=window.start_ns,
        end_ns=window.end_ns,
        event_count=len(events),
        event_content_sha256=_event_content_digest(events),
        context_id=context.context_id,
        near_signature=_near_signature(events),
    )


@dataclass(frozen=True, slots=True)
class SchrodingerBridgeDatasetWindowV1:
    """Row-free fit-window identity and deterministic split assignment."""

    window_id: str
    role: str
    start_ns: int
    end_ns: int
    event_count: int
    event_content_sha256: str
    context_id: str
    session: str
    near_signature: int
    schema_version: str = "histdatacom.schrodinger-bridge-dataset-window.v1"

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != "histdatacom.schrodinger-bridge-dataset-window.v1"
        ):
            raise ValueError("unsupported bridge dataset-window schema")
        object.__setattr__(self, "window_id", _required_text(self.window_id))
        if self.role not in {"train", "tune"}:
            raise ValueError("bridge dataset role is invalid")
        start = _bounded_int(self.start_ns, "start_ns", 0, 2**63 - 1)
        end = _bounded_int(self.end_ns, "end_ns", 0, 2**63 - 1)
        if end <= start:
            raise ValueError("bridge dataset interval is empty")
        object.__setattr__(self, "start_ns", start)
        object.__setattr__(self, "end_ns", end)
        object.__setattr__(
            self,
            "event_count",
            _bounded_int(self.event_count, "event_count", 1, 10**9),
        )
        digest = _required_text(self.event_content_sha256)
        if len(digest) != 64 or any(
            item not in "0123456789abcdef" for item in digest
        ):
            raise ValueError("bridge dataset digest is invalid")
        object.__setattr__(self, "context_id", _required_text(self.context_id))
        object.__setattr__(self, "session", _required_text(self.session))
        object.__setattr__(
            self,
            "near_signature",
            _bounded_int(self.near_signature, "near_signature", 0, 2**64 - 1),
        )

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "window_id": self.window_id,
            "role": self.role,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "event_count": self.event_count,
            "event_content_sha256": self.event_content_sha256,
            "context_id": self.context_id,
            "session": self.session,
            "near_signature": self.near_signature,
            "rows_inline": False,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> SchrodingerBridgeDatasetWindowV1:
        allowed = set(cls.__dataclass_fields__) | {"rows_inline"}
        if set(data) != allowed or data.get("rows_inline") is not False:
            raise ValueError("bridge dataset-window fields differ")
        return cls(**{name: data[name] for name in cls.__dataclass_fields__})


@dataclass(frozen=True, slots=True)
class SchrodingerBridgeDatasetManifestV1:
    """Content-addressed split and leakage boundary without protected rows."""

    config_id: str
    broker_target_id: str
    state_vocabulary: tuple[str, ...]
    windows: tuple[SchrodingerBridgeDatasetWindowV1, ...]
    protected_windows: tuple[SchrodingerBridgeProtectedWindowV1, ...]
    training_window_count: int
    tuning_window_count: int
    protected_window_count: int
    exact_duplicate_count: int
    near_duplicate_collision_count: int
    interval_overlap_count: int
    dataset_id: str = ""
    schema_version: str = SB_DATASET_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SB_DATASET_MANIFEST_SCHEMA_VERSION:
            raise ValueError("unsupported bridge dataset schema")
        object.__setattr__(self, "config_id", _required_text(self.config_id))
        object.__setattr__(
            self, "broker_target_id", _required_text(self.broker_target_id)
        )
        states = tuple(
            _required_text(item, "state") for item in self.state_vocabulary
        )
        if not states or len(set(states)) != len(states):
            raise ValueError("bridge state vocabulary is empty or duplicated")
        for state in states:
            _parse_state(state)
        object.__setattr__(self, "state_vocabulary", states)
        windows = tuple(
            sorted(
                self.windows, key=lambda item: (item.start_ns, item.window_id)
            )
        )
        protected = tuple(
            sorted(
                self.protected_windows,
                key=lambda item: (item.start_ns, item.window_id),
            )
        )
        if any(
            not isinstance(item, SchrodingerBridgeDatasetWindowV1)
            for item in windows
        ):
            raise TypeError("bridge dataset windows require v1 contracts")
        if any(
            not isinstance(item, SchrodingerBridgeProtectedWindowV1)
            for item in protected
        ):
            raise TypeError("bridge protected windows require v1 contracts")
        object.__setattr__(self, "windows", windows)
        object.__setattr__(self, "protected_windows", protected)
        actual_train = sum(item.role == "train" for item in windows)
        actual_tune = sum(item.role == "tune" for item in windows)
        counts = {
            "training_window_count": (self.training_window_count, actual_train),
            "tuning_window_count": (self.tuning_window_count, actual_tune),
            "protected_window_count": (
                self.protected_window_count,
                len(protected),
            ),
        }
        for name, (supplied, actual) in counts.items():
            if _strict_int(supplied, name) != actual:
                raise ValueError(f"{name} differs")
        for name in (
            "exact_duplicate_count",
            "near_duplicate_collision_count",
            "interval_overlap_count",
        ):
            object.__setattr__(
                self, name, _bounded_int(getattr(self, name), name, 0, 10**9)
            )
        expected = _stable_id(
            "schrodinger-bridge-dataset", self.identity_payload()
        )
        if self.dataset_id and self.dataset_id != expected:
            raise ValueError("bridge dataset_id differs")
        object.__setattr__(self, "dataset_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "config_id": self.config_id,
            "broker_target_id": self.broker_target_id,
            "state_vocabulary": list(self.state_vocabulary),
            "windows": [item.to_dict() for item in self.windows],
            "protected_windows": [
                item.to_dict() for item in self.protected_windows
            ],
            "training_window_count": self.training_window_count,
            "tuning_window_count": self.tuning_window_count,
            "protected_window_count": self.protected_window_count,
            "exact_duplicate_count": self.exact_duplicate_count,
            "near_duplicate_collision_count": self.near_duplicate_collision_count,
            "interval_overlap_count": self.interval_overlap_count,
            "protected_rows_inline": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "dataset_id": self.dataset_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> SchrodingerBridgeDatasetManifestV1:
        allowed = {
            "schema_version",
            "config_id",
            "broker_target_id",
            "state_vocabulary",
            "windows",
            "protected_windows",
            "training_window_count",
            "tuning_window_count",
            "protected_window_count",
            "exact_duplicate_count",
            "near_duplicate_collision_count",
            "interval_overlap_count",
            "protected_rows_inline",
            "dataset_id",
        }
        if (
            set(data) != allowed
            or data.get("protected_rows_inline") is not False
        ):
            raise ValueError("bridge dataset fields differ")
        return cls(
            config_id=str(data["config_id"]),
            broker_target_id=str(data["broker_target_id"]),
            state_vocabulary=tuple(
                str(item)
                for item in _sequence(data["state_vocabulary"], "states")
            ),
            windows=tuple(
                SchrodingerBridgeDatasetWindowV1.from_dict(
                    _mapping(item, "window")
                )
                for item in _sequence(data["windows"], "windows")
            ),
            protected_windows=tuple(
                SchrodingerBridgeProtectedWindowV1.from_dict(
                    _mapping(item, "protected_window")
                )
                for item in _sequence(
                    data["protected_windows"], "protected_windows"
                )
            ),
            training_window_count=_strict_int(
                data["training_window_count"], "training_window_count"
            ),
            tuning_window_count=_strict_int(
                data["tuning_window_count"], "tuning_window_count"
            ),
            protected_window_count=_strict_int(
                data["protected_window_count"], "protected_window_count"
            ),
            exact_duplicate_count=_strict_int(
                data["exact_duplicate_count"], "exact_duplicate_count"
            ),
            near_duplicate_collision_count=_strict_int(
                data["near_duplicate_collision_count"],
                "near_duplicate_collision_count",
            ),
            interval_overlap_count=_strict_int(
                data["interval_overlap_count"], "interval_overlap_count"
            ),
            dataset_id=str(data["dataset_id"]),
            schema_version=str(data["schema_version"]),
        )

    @classmethod
    def from_json(cls, text: str) -> SchrodingerBridgeDatasetManifestV1:
        return cls.from_dict(_json_mapping(text, 32 * 1024 * 1024))


@dataclass(frozen=True, slots=True)
class SchrodingerBridgeSolverEvidenceV1:
    """Bounded IPF convergence, stability, approximation, and resource evidence."""

    converged: bool
    iterations: int
    source_marginal_residual: float
    target_marginal_residual: float
    maximum_marginal_residual: float
    residual_trace: tuple[float, ...]
    support_missing_count: int
    numerical_repair_count: int
    minimum_positive_kernel: float
    maximum_kernel: float
    minimum_positive_scaling: float
    maximum_scaling: float
    expected_transport_cost: float
    relative_entropy: float
    regularized_objective: float
    quantization_mean_abs_error: float
    window_boundary_transition_l1: float
    solver_work: int
    wall_time_ms: int
    peak_memory_bytes: int
    schema_version: str = SB_SOLVER_EVIDENCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SB_SOLVER_EVIDENCE_SCHEMA_VERSION:
            raise ValueError("unsupported bridge solver-evidence schema")
        object.__setattr__(
            self, "converged", _strict_bool(self.converged, "converged")
        )
        for name in (
            "iterations",
            "support_missing_count",
            "numerical_repair_count",
            "solver_work",
            "wall_time_ms",
            "peak_memory_bytes",
        ):
            object.__setattr__(
                self, name, _bounded_int(getattr(self, name), name, 0, 10**15)
            )
        for name in (
            "source_marginal_residual",
            "target_marginal_residual",
            "maximum_marginal_residual",
            "minimum_positive_kernel",
            "maximum_kernel",
            "minimum_positive_scaling",
            "maximum_scaling",
            "expected_transport_cost",
            "relative_entropy",
            "regularized_objective",
            "quantization_mean_abs_error",
            "window_boundary_transition_l1",
        ):
            value = _finite_float(getattr(self, name), name)
            if value < 0.0:
                raise ValueError(f"{name} must be non-negative")
            object.__setattr__(self, name, value)
        trace = tuple(
            _finite_float(item, "residual_trace")
            for item in self.residual_trace
        )
        if any(item < 0.0 for item in trace):
            raise ValueError("bridge residual trace is negative")
        object.__setattr__(self, "residual_trace", trace)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            name: (
                list(value)
                if name == "residual_trace"
                else cast(JSONValue, value)
            )
            for name, value in (
                (item, getattr(self, item))
                for item in self.__dataclass_fields__
                if item not in {"wall_time_ms", "peak_memory_bytes"}
            )
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "wall_time_ms": self.wall_time_ms,
            "peak_memory_bytes": self.peak_memory_bytes,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> SchrodingerBridgeSolverEvidenceV1:
        if set(data) != set(cls.__dataclass_fields__):
            raise ValueError("bridge solver-evidence fields differ")
        values = dict(data)
        values["residual_trace"] = tuple(
            _finite_float(item, "residual_trace")
            for item in _sequence(values["residual_trace"], "residual_trace")
        )
        return cls(**values)


def _finite_vector(
    values: Sequence[Any], size: int, name: str
) -> tuple[float, ...]:
    result = tuple(_finite_float(item, name) for item in values)
    if len(result) != size or any(item < 0.0 for item in result):
        raise ValueError(f"{name} has invalid shape or values")
    return result


def _finite_square_matrix(
    values: Sequence[Any], size: int, name: str
) -> tuple[tuple[float, ...], ...]:
    rows = tuple(
        _finite_vector(_sequence(item, name), size, name) for item in values
    )
    if len(rows) != size:
        raise ValueError(f"{name} has invalid shape")
    return rows


@dataclass(frozen=True, slots=True)
class SchrodingerBridgeCheckpointV1:
    """Immutable reference kernel, endpoint coupling, and comparator evidence."""

    config_id: str
    broker_target_id: str
    dataset_id: str
    state_vocabulary: tuple[str, ...]
    source_marginal: tuple[float, ...]
    target_marginal: tuple[float, ...]
    reference_transition: tuple[tuple[float, ...], ...]
    endpoint_reference_kernel: tuple[tuple[float, ...], ...]
    endpoint_coupling: tuple[tuple[float, ...], ...]
    source_mean_event_count: float
    target_mean_event_count: float
    mean_training_window_duration_ns: float
    tune_joint_nll: float
    source_iid_tune_nll: float
    uniform_tune_nll: float
    solver_evidence: SchrodingerBridgeSolverEvidenceV1
    parameter_count: int
    parameter_bytes: int
    checkpoint_id: str = ""
    schema_version: str = SB_CHECKPOINT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SB_CHECKPOINT_SCHEMA_VERSION:
            raise ValueError("unsupported bridge checkpoint schema")
        for name in ("config_id", "broker_target_id", "dataset_id"):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        states = tuple(_required_text(item) for item in self.state_vocabulary)
        size = len(states)
        if size < 2 or len(set(states)) != size:
            raise ValueError("bridge checkpoint states are invalid")
        object.__setattr__(self, "state_vocabulary", states)
        object.__setattr__(
            self,
            "source_marginal",
            _finite_vector(self.source_marginal, size, "source_marginal"),
        )
        object.__setattr__(
            self,
            "target_marginal",
            _finite_vector(self.target_marginal, size, "target_marginal"),
        )
        for name in (
            "reference_transition",
            "endpoint_reference_kernel",
            "endpoint_coupling",
        ):
            object.__setattr__(
                self,
                name,
                _finite_square_matrix(getattr(self, name), size, name),
            )
        if not math.isclose(math.fsum(self.source_marginal), 1.0, abs_tol=1e-8):
            raise ValueError("bridge source marginal is not normalized")
        if not math.isclose(math.fsum(self.target_marginal), 1.0, abs_tol=1e-8):
            raise ValueError("bridge target marginal is not normalized")
        if any(
            not math.isclose(math.fsum(row), 1.0, abs_tol=1e-8)
            for row in self.reference_transition
        ):
            raise ValueError("bridge reference transition is not stochastic")
        for name in (
            "source_mean_event_count",
            "target_mean_event_count",
            "mean_training_window_duration_ns",
            "tune_joint_nll",
            "source_iid_tune_nll",
            "uniform_tune_nll",
        ):
            value = _finite_float(getattr(self, name), name)
            if value < 0.0:
                raise ValueError(f"{name} must be non-negative")
            object.__setattr__(self, name, value)
        if not isinstance(
            self.solver_evidence, SchrodingerBridgeSolverEvidenceV1
        ):
            raise TypeError("bridge checkpoint requires solver evidence")
        object.__setattr__(
            self,
            "parameter_count",
            _bounded_int(self.parameter_count, "parameter_count", 1, 10**9),
        )
        object.__setattr__(
            self,
            "parameter_bytes",
            _bounded_int(self.parameter_bytes, "parameter_bytes", 1, 10**9),
        )
        expected = _stable_id(
            "schrodinger-bridge-checkpoint", self.identity_payload()
        )
        if self.checkpoint_id and self.checkpoint_id != expected:
            raise ValueError("bridge checkpoint_id differs")
        object.__setattr__(self, "checkpoint_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "architecture": SB_ARCHITECTURE,
            "config_id": self.config_id,
            "broker_target_id": self.broker_target_id,
            "dataset_id": self.dataset_id,
            "state_vocabulary": list(self.state_vocabulary),
            "source_marginal": list(self.source_marginal),
            "target_marginal": list(self.target_marginal),
            "reference_transition": [
                list(item) for item in self.reference_transition
            ],
            "endpoint_reference_kernel": [
                list(item) for item in self.endpoint_reference_kernel
            ],
            "endpoint_coupling": [
                list(item) for item in self.endpoint_coupling
            ],
            "source_mean_event_count": self.source_mean_event_count,
            "target_mean_event_count": self.target_mean_event_count,
            "mean_training_window_duration_ns": self.mean_training_window_duration_ns,
            "tune_joint_nll": self.tune_joint_nll,
            "source_iid_tune_nll": self.source_iid_tune_nll,
            "uniform_tune_nll": self.uniform_tune_nll,
            "solver_evidence": self.solver_evidence.identity_payload(),
            "parameter_count": self.parameter_count,
            "parameter_bytes": self.parameter_bytes,
            "reference_path_law": "first-order-markov-whole-window-v1",
            "endpoint_scaling": "diag-u-reference-diag-v-v1",
            "automatic_winner": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "solver_evidence": self.solver_evidence.to_dict(),
            "checkpoint_id": self.checkpoint_id,
        }

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> SchrodingerBridgeCheckpointV1:
        allowed = {
            "schema_version",
            "architecture",
            "config_id",
            "broker_target_id",
            "dataset_id",
            "state_vocabulary",
            "source_marginal",
            "target_marginal",
            "reference_transition",
            "endpoint_reference_kernel",
            "endpoint_coupling",
            "source_mean_event_count",
            "target_mean_event_count",
            "mean_training_window_duration_ns",
            "tune_joint_nll",
            "source_iid_tune_nll",
            "uniform_tune_nll",
            "solver_evidence",
            "parameter_count",
            "parameter_bytes",
            "reference_path_law",
            "endpoint_scaling",
            "automatic_winner",
            "checkpoint_id",
        }
        if set(data) != allowed or data.get("architecture") != SB_ARCHITECTURE:
            raise ValueError("bridge checkpoint fields differ")
        return cls(
            config_id=str(data["config_id"]),
            broker_target_id=str(data["broker_target_id"]),
            dataset_id=str(data["dataset_id"]),
            state_vocabulary=tuple(
                str(item)
                for item in _sequence(data["state_vocabulary"], "states")
            ),
            source_marginal=tuple(
                _finite_float(item, "source_marginal")
                for item in _sequence(
                    data["source_marginal"], "source_marginal"
                )
            ),
            target_marginal=tuple(
                _finite_float(item, "target_marginal")
                for item in _sequence(
                    data["target_marginal"], "target_marginal"
                )
            ),
            reference_transition=tuple(
                tuple(
                    _finite_float(value, "reference_transition")
                    for value in _sequence(row, "row")
                )
                for row in _sequence(
                    data["reference_transition"], "reference_transition"
                )
            ),
            endpoint_reference_kernel=tuple(
                tuple(
                    _finite_float(value, "endpoint_reference_kernel")
                    for value in _sequence(row, "row")
                )
                for row in _sequence(
                    data["endpoint_reference_kernel"],
                    "endpoint_reference_kernel",
                )
            ),
            endpoint_coupling=tuple(
                tuple(
                    _finite_float(value, "endpoint_coupling")
                    for value in _sequence(row, "row")
                )
                for row in _sequence(
                    data["endpoint_coupling"], "endpoint_coupling"
                )
            ),
            source_mean_event_count=_finite_float(
                data["source_mean_event_count"], "source_mean_event_count"
            ),
            target_mean_event_count=_finite_float(
                data["target_mean_event_count"], "target_mean_event_count"
            ),
            mean_training_window_duration_ns=_finite_float(
                data["mean_training_window_duration_ns"],
                "mean_training_window_duration_ns",
            ),
            tune_joint_nll=_finite_float(
                data["tune_joint_nll"], "tune_joint_nll"
            ),
            source_iid_tune_nll=_finite_float(
                data["source_iid_tune_nll"], "source_iid_tune_nll"
            ),
            uniform_tune_nll=_finite_float(
                data["uniform_tune_nll"], "uniform_tune_nll"
            ),
            solver_evidence=SchrodingerBridgeSolverEvidenceV1.from_dict(
                _mapping(data["solver_evidence"], "solver_evidence")
            ),
            parameter_count=_strict_int(
                data["parameter_count"], "parameter_count"
            ),
            parameter_bytes=_strict_int(
                data["parameter_bytes"], "parameter_bytes"
            ),
            checkpoint_id=str(data["checkpoint_id"]),
            schema_version=str(data["schema_version"]),
        )

    @classmethod
    def from_json(cls, text: str) -> SchrodingerBridgeCheckpointV1:
        return cls.from_dict(_json_mapping(text, 64 * 1024 * 1024))


@dataclass(frozen=True, slots=True)
class SchrodingerBridgeFitResultV1:
    """Terminal fit result with immutable artifacts or bounded refusal evidence."""

    config_id: str
    broker_target_id: str
    information_mode: InformationMode
    as_of_ns: int | None
    status: SchrodingerBridgeFitStatus
    converged: bool
    training_window_count: int
    tuning_window_count: int
    training_event_count: int
    tuning_event_count: int
    dataset_manifest: SchrodingerBridgeDatasetManifestV1 | None
    checkpoint: SchrodingerBridgeCheckpointV1 | None
    solver_evidence: SchrodingerBridgeSolverEvidenceV1 | None
    runtime_metadata: Mapping[str, JSONValue]
    fit_wall_time_ms: int
    fit_peak_memory_bytes: int
    failure_reason: str | None
    fit_id: str = ""
    schema_version: str = SB_FIT_RESULT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SB_FIT_RESULT_SCHEMA_VERSION:
            raise ValueError("unsupported bridge fit-result schema")
        object.__setattr__(self, "config_id", _required_text(self.config_id))
        object.__setattr__(
            self, "broker_target_id", _required_text(self.broker_target_id)
        )
        mode = InformationMode.from_value(self.information_mode)
        object.__setattr__(self, "information_mode", mode)
        if self.as_of_ns is not None:
            object.__setattr__(
                self,
                "as_of_ns",
                _bounded_int(self.as_of_ns, "as_of_ns", 0, 2**63 - 1),
            )
        if mode is InformationMode.EX_ANTE_SIMULATION and self.as_of_ns is None:
            raise ValueError("ex-ante bridge fit requires as_of_ns")
        if (
            mode is InformationMode.EX_POST_RECONSTRUCTION
            and self.as_of_ns is not None
        ):
            raise ValueError("ex-post bridge fit forbids as_of_ns")
        status = SchrodingerBridgeFitStatus(self.status)
        object.__setattr__(self, "status", status)
        object.__setattr__(
            self, "converged", _strict_bool(self.converged, "converged")
        )
        for name in (
            "training_window_count",
            "tuning_window_count",
            "training_event_count",
            "tuning_event_count",
            "fit_wall_time_ms",
            "fit_peak_memory_bytes",
        ):
            object.__setattr__(
                self, name, _bounded_int(getattr(self, name), name, 0, 10**15)
            )
        runtime = {
            str(name): value for name, value in self.runtime_metadata.items()
        }
        object.__setattr__(
            self, "runtime_metadata", dict(sorted(runtime.items()))
        )
        object.__setattr__(
            self, "failure_reason", _optional_text(self.failure_reason)
        )
        if status is SchrodingerBridgeFitStatus.FITTED:
            if (
                not self.converged
                or self.dataset_manifest is None
                or self.checkpoint is None
                or self.solver_evidence is None
                or self.failure_reason is not None
            ):
                raise ValueError("fitted bridge result is incomplete")
            if (
                self.dataset_manifest.config_id != self.config_id
                or self.dataset_manifest.broker_target_id
                != self.broker_target_id
                or self.checkpoint.config_id != self.config_id
                or self.checkpoint.broker_target_id != self.broker_target_id
                or self.checkpoint.dataset_id
                != self.dataset_manifest.dataset_id
                or not self.checkpoint.solver_evidence.converged
                or self.solver_evidence != self.checkpoint.solver_evidence
            ):
                raise ValueError("fitted bridge artifact identities differ")
        elif (
            self.converged
            or self.checkpoint is not None
            or self.failure_reason is None
        ):
            raise ValueError("closed bridge fit contains successful state")
        if self.solver_evidence is not None and not isinstance(
            self.solver_evidence, SchrodingerBridgeSolverEvidenceV1
        ):
            raise TypeError(
                "bridge fit solver evidence requires the v1 contract"
            )
        expected = _stable_id("schrodinger-bridge-fit", self.identity_payload())
        if self.fit_id and self.fit_id != expected:
            raise ValueError("bridge fit_id differs")
        object.__setattr__(self, "fit_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "config_id": self.config_id,
            "broker_target_id": self.broker_target_id,
            "information_mode": self.information_mode.value,
            "as_of_ns": self.as_of_ns,
            "status": self.status.value,
            "converged": self.converged,
            "training_window_count": self.training_window_count,
            "tuning_window_count": self.tuning_window_count,
            "training_event_count": self.training_event_count,
            "tuning_event_count": self.tuning_event_count,
            "dataset_id": (
                self.dataset_manifest.dataset_id
                if self.dataset_manifest is not None
                else None
            ),
            "checkpoint_id": (
                self.checkpoint.checkpoint_id
                if self.checkpoint is not None
                else None
            ),
            "solver_evidence": (
                self.solver_evidence.identity_payload()
                if self.solver_evidence is not None
                else None
            ),
            "failure_reason": self.failure_reason,
            "hypothesis_id": SB_HYPOTHESIS_ID,
            "transparent_comparators": [
                "source-iid-endpoint",
                "uniform-iid-endpoint",
                "dense/no-fill",
                "linear-interpolation",
                "empirical-motif",
                "acd-1-1-exponential",
                "non-homogeneous-poisson",
                "cox-gamma-poisson",
                "hidden-markov-duration-mark",
                "marked-hawkes-zero-excitation",
                "marked-hawkes-diagonal-self-excitation",
                "marked-hawkes-full-self-cross-excitation",
                "regime-hawkes-baseline-only",
                "regime-hawkes-baseline-and-excitation",
            ],
            "automatic_winner": False,
            "promotion_policy": SB_PROMOTION_POLICY,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "dataset_manifest": (
                self.dataset_manifest.to_dict()
                if self.dataset_manifest is not None
                else None
            ),
            "checkpoint": (
                self.checkpoint.to_dict()
                if self.checkpoint is not None
                else None
            ),
            "runtime_metadata": dict(self.runtime_metadata),
            "solver_evidence": (
                self.solver_evidence.to_dict()
                if self.solver_evidence is not None
                else None
            ),
            "fit_wall_time_ms": self.fit_wall_time_ms,
            "fit_peak_memory_bytes": self.fit_peak_memory_bytes,
            "fit_id": self.fit_id,
        }

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> SchrodingerBridgeFitResultV1:
        dataset = data.get("dataset_manifest")
        checkpoint = data.get("checkpoint")
        solver = data.get("solver_evidence")
        return cls(
            config_id=str(data.get("config_id", "")),
            broker_target_id=str(data.get("broker_target_id", "")),
            information_mode=InformationMode.from_value(
                _required_text(data.get("information_mode"), "information_mode")
            ),
            as_of_ns=(
                None
                if data.get("as_of_ns") is None
                else _strict_int(data.get("as_of_ns"), "as_of_ns")
            ),
            status=SchrodingerBridgeFitStatus(
                _required_text(data.get("status"), "status")
            ),
            converged=_strict_bool(data.get("converged"), "converged"),
            training_window_count=_strict_int(
                data.get("training_window_count"), "training_window_count"
            ),
            tuning_window_count=_strict_int(
                data.get("tuning_window_count"), "tuning_window_count"
            ),
            training_event_count=_strict_int(
                data.get("training_event_count"), "training_event_count"
            ),
            tuning_event_count=_strict_int(
                data.get("tuning_event_count"), "tuning_event_count"
            ),
            dataset_manifest=(
                SchrodingerBridgeDatasetManifestV1.from_dict(
                    _mapping(dataset, "dataset_manifest")
                )
                if dataset is not None
                else None
            ),
            checkpoint=(
                SchrodingerBridgeCheckpointV1.from_dict(
                    _mapping(checkpoint, "checkpoint")
                )
                if checkpoint is not None
                else None
            ),
            solver_evidence=(
                SchrodingerBridgeSolverEvidenceV1.from_dict(
                    _mapping(solver, "solver_evidence")
                )
                if solver is not None
                else None
            ),
            runtime_metadata={
                str(name): cast(JSONValue, value)
                for name, value in _mapping(
                    data.get("runtime_metadata"), "runtime_metadata"
                ).items()
            },
            fit_wall_time_ms=_strict_int(
                data.get("fit_wall_time_ms"), "fit_wall_time_ms"
            ),
            fit_peak_memory_bytes=_strict_int(
                data.get("fit_peak_memory_bytes"), "fit_peak_memory_bytes"
            ),
            failure_reason=_optional_text(data.get("failure_reason")),
            fit_id=str(data.get("fit_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> SchrodingerBridgeFitResultV1:
        return cls.from_dict(_json_mapping(text, 128 * 1024 * 1024))


def _state_vocabulary(config: SchrodingerBridgeConfigV1) -> tuple[str, ...]:
    return tuple(
        _state_name(bin_index, symbol, mark)
        for bin_index in range(config.time_bin_count)
        for symbol in TRIANGLE_SYMBOLS
        for mark in MARK_STATES
    )


def _window_state_indices(
    window: EventClockCalibrationWindowV1,
    state_index: Mapping[str, int],
    config: SchrodingerBridgeConfigV1,
) -> tuple[int, ...]:
    prior_by_symbol: dict[str, BenchmarkEventV1] = {}
    result: list[int] = []
    duration = window.end_ns - window.start_ns
    for event in sorted(window.events, key=_event_key):
        symbol = event.symbol.upper()
        if symbol not in TRIANGLE_SYMBOLS:
            raise ValueError("bridge calibration symbol is unsupported")
        if not window.start_ns <= event.event_time_ns < window.end_ns:
            raise ValueError("bridge calibration event is outside its window")
        normalized = (event.event_time_ns - window.start_ns) / duration
        bin_index = min(
            config.time_bin_count - 1, int(normalized * config.time_bin_count)
        )
        mark = _event_mark(prior_by_symbol.get(symbol), event)
        prior_by_symbol[symbol] = event
        result.append(state_index[_state_name(bin_index, symbol, mark)])
    return tuple(result)


def _split_fit_windows(
    windows: Sequence[EventClockCalibrationWindowV1],
    contexts: Sequence[SchrodingerBridgeWindowContextV1],
) -> tuple[
    tuple[
        tuple[EventClockCalibrationWindowV1, SchrodingerBridgeWindowContextV1],
        ...,
    ],
    tuple[
        tuple[EventClockCalibrationWindowV1, SchrodingerBridgeWindowContextV1],
        ...,
    ],
]:
    context_by_id = {item.window_id: item for item in contexts}
    if len(context_by_id) != len(contexts) or set(context_by_id) != {
        item.window_id for item in windows
    }:
        raise ValueError("bridge calibration windows and contexts differ")
    by_session: dict[
        str,
        list[
            tuple[
                EventClockCalibrationWindowV1, SchrodingerBridgeWindowContextV1
            ]
        ],
    ] = defaultdict(list)
    for window in sorted(
        windows, key=lambda item: (item.start_ns, item.window_id)
    ):
        context = context_by_id[window.window_id]
        by_session[context.session].append((window, context))
    train: list[
        tuple[EventClockCalibrationWindowV1, SchrodingerBridgeWindowContextV1]
    ] = []
    tune: list[
        tuple[EventClockCalibrationWindowV1, SchrodingerBridgeWindowContextV1]
    ] = []
    for session in sorted(by_session):
        occurrences = by_session[session]
        if len(occurrences) < 2:
            raise ValueError(
                "each bridge calibration session requires train and tune occurrences"
            )
        train.append(occurrences[0])
        tune.extend(occurrences[1:])
    return tuple(train), tuple(tune)


def _build_dataset(
    config: SchrodingerBridgeConfigV1,
    target: SchrodingerBridgeBrokerTargetV1,
    windows: Sequence[EventClockCalibrationWindowV1],
    contexts: Sequence[SchrodingerBridgeWindowContextV1],
    protected_windows: Sequence[SchrodingerBridgeProtectedWindowV1],
    *,
    information_mode: InformationMode,
    as_of_ns: int | None,
) -> tuple[
    SchrodingerBridgeDatasetManifestV1,
    tuple[
        tuple[EventClockCalibrationWindowV1, SchrodingerBridgeWindowContextV1],
        ...,
    ],
    tuple[
        tuple[EventClockCalibrationWindowV1, SchrodingerBridgeWindowContextV1],
        ...,
    ],
]:
    if len(target.time_bin_weights) != config.time_bin_count:
        raise ValueError("bridge target time-bin shape differs from config")
    values = tuple(windows)
    if not 2 <= len(values) <= config.limits.max_fit_windows:
        raise ValueError("bridge fit-window count is outside bounds")
    if any(
        not isinstance(item, EventClockCalibrationWindowV1)
        or not item.events
        or len(item.events) > config.limits.max_events_per_window
        for item in values
    ):
        raise ValueError("bridge calibration window is invalid or oversized")
    mode = InformationMode.from_value(information_mode)
    if mode is InformationMode.EX_ANTE_SIMULATION:
        if as_of_ns is None:
            raise ValueError("ex-ante bridge fit requires as_of_ns")
        if any(
            event.event_time_ns > as_of_ns
            for window in values
            for event in window.events
        ):
            raise ValueError("ex-ante bridge fit contains future events")
        if any(
            context.observed_context_available_ns is not None
            and context.observed_context_available_ns > as_of_ns
            for context in contexts
        ):
            raise ValueError("ex-ante bridge context is unavailable")
    elif as_of_ns is not None:
        raise ValueError("ex-post bridge fit forbids as_of_ns")
    train, tune = _split_fit_windows(values, contexts)
    dataset_windows = tuple(
        SchrodingerBridgeDatasetWindowV1(
            window_id=window.window_id,
            role=role,
            start_ns=window.start_ns,
            end_ns=window.end_ns,
            event_count=len(window.events),
            event_content_sha256=_event_content_digest(tuple(window.events)),
            context_id=context.context_id,
            session=context.session,
            near_signature=_near_signature(tuple(window.events)),
        )
        for role, pairs in (("train", train), ("tune", tune))
        for window, context in pairs
    )
    protected = tuple(protected_windows)
    all_items: list[tuple[str, int, int, str, int]] = [
        (
            item.role,
            item.start_ns,
            item.end_ns,
            item.event_content_sha256,
            item.near_signature,
        )
        for item in dataset_windows
    ] + [
        (
            item.role,
            item.start_ns,
            item.end_ns,
            item.event_content_sha256,
            item.near_signature,
        )
        for item in protected
    ]
    exact = 0
    near = 0
    overlap = 0
    for left, right in pairwise(
        sorted(all_items, key=lambda item: (item[1], item[2], item[0]))
    ):
        if left[2] > right[1]:
            overlap += 1
    for index, left in enumerate(all_items):
        for right in all_items[index + 1 :]:
            if left[0] == right[0]:
                continue
            exact += left[3] == right[3]
            near += left[4] == right[4]
    manifest = SchrodingerBridgeDatasetManifestV1(
        config_id=config.config_id,
        broker_target_id=target.target_id,
        state_vocabulary=_state_vocabulary(config),
        windows=dataset_windows,
        protected_windows=protected,
        training_window_count=len(train),
        tuning_window_count=len(tune),
        protected_window_count=len(protected),
        exact_duplicate_count=exact,
        near_duplicate_collision_count=near,
        interval_overlap_count=overlap,
    )
    if exact or near or overlap:
        raise ValueError(
            "bridge train/tune/protected split has duplication or overlap"
        )
    return manifest, train, tune


def _state_cost(
    left: str, right: str, config: SchrodingerBridgeConfigV1
) -> float:
    left_bin, left_symbol, left_mark = _parse_state(left)
    right_bin, right_symbol, right_mark = _parse_state(right)
    time_cost = abs(left_bin - right_bin) / max(1, config.time_bin_count - 1)
    mark_cost = 0.0 if left_mark == right_mark else 1.0
    left_exposure = _EXPOSURES[left_symbol]
    right_exposure = _EXPOSURES[right_symbol]
    currency_cost = (
        math.fsum(
            abs(left_value - right_value)
            for left_value, right_value in zip(left_exposure, right_exposure)
        )
        / 4.0
    )
    return (
        config.time_cost_weight * time_cost
        + config.mark_cost_weight * mark_cost
        + config.cross_currency_cost_weight * currency_cost
    )


def _normalized(values: Sequence[float], name: str) -> tuple[float, ...]:
    total = math.fsum(values)
    if not math.isfinite(total) or total <= 0.0:
        raise ValueError(f"{name} has no mass")
    return tuple(item / total for item in values)


def _reference_transition(
    states: Sequence[str],
    train_sequences: Sequence[Sequence[int]],
    config: SchrodingerBridgeConfigV1,
) -> tuple[tuple[float, ...], ...]:
    size = len(states)
    counts = [[0.0 for _ in range(size)] for _ in range(size)]
    for sequence in train_sequences:
        for left, right in pairwise(sequence):
            counts[left][right] += 1.0
    for row in range(size):
        counts[row][row] += 1.0
        for column in range(size):
            cost = _state_cost(states[row], states[column], config)
            if cost <= config.maximum_transport_cost:
                counts[row][column] += config.transition_smoothing * math.exp(
                    -cost / config.entropic_regularization
                )
        total = math.fsum(counts[row])
        if total <= 0.0:
            counts[row][row] = 1.0
            total = 1.0
        counts[row] = [item / total for item in counts[row]]
    return tuple(tuple(item for item in row) for row in counts)


def _boundary_transition_l1(
    sequences: Sequence[Sequence[int]],
    transition: Sequence[Sequence[float]],
) -> float:
    size = len(transition)
    stitched = [[0.0 for _ in range(size)] for _ in range(size)]
    for sequence in sequences:
        for left, right in pairwise(sequence):
            stitched[left][right] += 1.0
    ordered = [item for item in sequences if item]
    for left_sequence, right_sequence in pairwise(ordered):
        stitched[left_sequence[-1]][right_sequence[0]] += 1.0
    values: list[float] = []
    for row in range(size):
        total = math.fsum(stitched[row])
        if total > 0:
            empirical = [item / total for item in stitched[row]]
            values.append(
                math.fsum(
                    abs(empirical[column] - transition[row][column])
                    for column in range(size)
                )
            )
    return math.fsum(values) / len(values) if values else 0.0


def _sinkhorn(
    source: Sequence[float],
    target: Sequence[float],
    endpoint: Sequence[Sequence[float]],
    costs: Sequence[Sequence[float]],
    config: SchrodingerBridgeConfigV1,
    *,
    quantization_error: float,
    boundary_error: float,
) -> tuple[tuple[tuple[float, ...], ...], SchrodingerBridgeSolverEvidenceV1]:
    started = time.perf_counter()
    before_peak = peak_rss_bytes()
    size = len(source)
    kernel = [
        [source[row] * endpoint[row][column] for column in range(size)]
        for row in range(size)
    ]
    missing = sum(
        target[column] > 0.0
        and not any(
            source[row] > 0.0 and kernel[row][column] > 0.0
            for row in range(size)
        )
        for column in range(size)
    )
    if missing:
        evidence = SchrodingerBridgeSolverEvidenceV1(
            converged=False,
            iterations=0,
            source_marginal_residual=1.0,
            target_marginal_residual=1.0,
            maximum_marginal_residual=1.0,
            residual_trace=(1.0,),
            support_missing_count=missing,
            numerical_repair_count=0,
            minimum_positive_kernel=0.0,
            maximum_kernel=max(max(row) for row in kernel),
            minimum_positive_scaling=0.0,
            maximum_scaling=0.0,
            expected_transport_cost=0.0,
            relative_entropy=0.0,
            regularized_objective=0.0,
            quantization_mean_abs_error=quantization_error,
            window_boundary_transition_l1=boundary_error,
            solver_work=0,
            wall_time_ms=round((time.perf_counter() - started) * 1000),
            peak_memory_bytes=max(0, peak_rss_bytes() - before_peak),
        )
        return (
            tuple(tuple(0.0 for _ in range(size)) for _ in range(size)),
            evidence,
        )
    projected_work = config.sinkhorn_max_iterations * size * size * 2
    if projected_work > config.limits.max_solver_work:
        raise _BridgeRefusal("solver_work_preflight_exceeded")
    u = [1.0 if item > 0.0 else 0.0 for item in source]
    v = [1.0 for _ in target]
    trace: list[float] = []
    row_residual = 1.0
    column_residual = 1.0
    iterations = 0
    repairs = 0
    stride = max(
        1,
        config.sinkhorn_max_iterations // config.limits.max_residual_trace,
    )
    coupling = [[0.0 for _ in range(size)] for _ in range(size)]
    for iteration in range(1, config.sinkhorn_max_iterations + 1):
        kv = _matrix_vector(kernel, v)
        for index in range(size):
            if source[index] == 0.0:
                u[index] = 0.0
            elif kv[index] <= 0.0 or not math.isfinite(kv[index]):
                repairs += 1
                u[index] = 0.0
            else:
                u[index] = source[index] / kv[index]
        ktu = _transpose_vector(kernel, u)
        for index in range(size):
            if target[index] == 0.0:
                v[index] = 0.0
            elif ktu[index] <= 0.0 or not math.isfinite(ktu[index]):
                repairs += 1
                v[index] = 0.0
            else:
                v[index] = target[index] / ktu[index]
        if any(not math.isfinite(item) or item > 1e300 for item in (*u, *v)):
            raise _BridgeRefusal("solver_scaling_became_numerically_unstable")
        for row in range(size):
            coupling[row] = [
                u[row] * kernel[row][column] * v[column]
                for column in range(size)
            ]
        row_mass = [math.fsum(row) for row in coupling]
        column_mass = [
            math.fsum(coupling[row][column] for row in range(size))
            for column in range(size)
        ]
        row_residual = max(
            abs(row_mass[index] - source[index]) for index in range(size)
        )
        column_residual = max(
            abs(column_mass[index] - target[index]) for index in range(size)
        )
        residual = max(row_residual, column_residual)
        if (iteration == 1 or iteration % stride == 0) and len(
            trace
        ) < config.limits.max_residual_trace:
            trace.append(residual)
        iterations = iteration
        if residual <= config.sinkhorn_tolerance:
            break
    converged = max(row_residual, column_residual) <= config.sinkhorn_tolerance
    final_residual = max(row_residual, column_residual)
    if not trace or trace[-1] != final_residual:
        if len(trace) == config.limits.max_residual_trace:
            trace[-1] = final_residual
        else:
            trace.append(final_residual)
    positive_kernel = [item for row in kernel for item in row if item > 0.0]
    positive_scaling = [item for item in (*u, *v) if item > 0.0]
    expected_cost = math.fsum(
        coupling[row][column] * costs[row][column]
        for row in range(size)
        for column in range(size)
    )
    relative_entropy = math.fsum(
        coupling[row][column]
        * math.log(coupling[row][column] / kernel[row][column])
        for row in range(size)
        for column in range(size)
        if coupling[row][column] > 0.0 and kernel[row][column] > 0.0
    )
    evidence = SchrodingerBridgeSolverEvidenceV1(
        converged=converged,
        iterations=iterations,
        source_marginal_residual=row_residual,
        target_marginal_residual=column_residual,
        maximum_marginal_residual=final_residual,
        residual_trace=tuple(trace),
        support_missing_count=missing,
        numerical_repair_count=repairs,
        minimum_positive_kernel=(
            min(positive_kernel) if positive_kernel else 0.0
        ),
        maximum_kernel=max(positive_kernel) if positive_kernel else 0.0,
        minimum_positive_scaling=(
            min(positive_scaling) if positive_scaling else 0.0
        ),
        maximum_scaling=max(positive_scaling) if positive_scaling else 0.0,
        expected_transport_cost=max(0.0, expected_cost),
        relative_entropy=max(0.0, relative_entropy),
        regularized_objective=max(
            0.0,
            expected_cost + config.entropic_regularization * relative_entropy,
        ),
        quantization_mean_abs_error=quantization_error,
        window_boundary_transition_l1=boundary_error,
        solver_work=iterations * size * size * 2,
        wall_time_ms=round((time.perf_counter() - started) * 1000),
        peak_memory_bytes=max(0, peak_rss_bytes() - before_peak),
    )
    return tuple(tuple(item for item in row) for row in coupling), evidence


def _tune_nll(
    sequences: Sequence[Sequence[int]],
    marginal: Sequence[float],
    transition: Sequence[Sequence[float]] | None = None,
) -> float:
    losses: list[float] = []
    floor = 1e-300
    for sequence in sequences:
        for index, state in enumerate(sequence):
            if transition is None or index == 0:
                probability = marginal[state]
            else:
                probability = transition[sequence[index - 1]][state]
            losses.append(-math.log(max(floor, probability)))
    return math.fsum(losses) / len(losses) if losses else 0.0


def _quantization_error(
    pairs: Sequence[
        tuple[EventClockCalibrationWindowV1, SchrodingerBridgeWindowContextV1]
    ],
    bins: int,
) -> float:
    errors: list[float] = []
    for window, _ in pairs:
        duration = window.end_ns - window.start_ns
        for event in window.events:
            normalized = (event.event_time_ns - window.start_ns) / duration
            index = min(bins - 1, int(normalized * bins))
            center = (index + 0.5) / bins
            errors.append(abs(normalized - center))
    return math.fsum(errors) / len(errors) if errors else 0.0


def _fit_checkpoint(
    config: SchrodingerBridgeConfigV1,
    target: SchrodingerBridgeBrokerTargetV1,
    dataset: SchrodingerBridgeDatasetManifestV1,
    train: Sequence[
        tuple[EventClockCalibrationWindowV1, SchrodingerBridgeWindowContextV1]
    ],
    tune: Sequence[
        tuple[EventClockCalibrationWindowV1, SchrodingerBridgeWindowContextV1]
    ],
) -> SchrodingerBridgeCheckpointV1:
    states = dataset.state_vocabulary
    index = {state: position for position, state in enumerate(states)}
    train_sequences = tuple(
        _window_state_indices(window, index, config) for window, _ in train
    )
    tune_sequences = tuple(
        _window_state_indices(window, index, config) for window, _ in tune
    )
    counts = [0.0 for _ in states]
    for sequence in train_sequences:
        for state in sequence:
            counts[state] += 1.0
    source = _normalized(counts, "bridge source marginal")
    broker_weights: list[float] = []
    for state_name in states:
        bin_index, symbol, mark = _parse_state(state_name)
        broker_weights.append(
            target.time_bin_weights[bin_index]
            * target.symbol_weights[symbol]
            * target.mark_weights[mark]
        )
    broker_profile = _normalized(
        broker_weights, "bridge broker endpoint profile"
    )
    strength = target.transfer_strength
    target_marginal = _normalized(
        tuple(
            (1.0 - strength) * source[index] + strength * broker_profile[index]
            for index in range(len(states))
        ),
        "bridge target marginal",
    )
    transition = _reference_transition(states, train_sequences, config)
    endpoint = _matrix_power(transition, config.bridge_steps)
    costs = tuple(
        tuple(_state_cost(left, right, config) for right in states)
        for left in states
    )
    boundary_error = _boundary_transition_l1(train_sequences, transition)
    coupling, solver = _sinkhorn(
        source,
        target_marginal,
        endpoint,
        costs,
        config,
        quantization_error=_quantization_error(tune, config.time_bin_count),
        boundary_error=boundary_error,
    )
    if not solver.converged:
        reason = (
            "endpoint_off_support"
            if solver.support_missing_count
            else "sinkhorn_non_convergence"
        )
        raise _SolverRefusal(reason, solver)
    durations = [window.end_ns - window.start_ns for window, _ in train]
    source_count = math.fsum(len(window.events) for window, _ in train) / len(
        train
    )
    parameter_count = len(states) * len(states) * 3 + len(states) * 2
    parameter_bytes = parameter_count * 8
    if parameter_bytes > config.limits.max_checkpoint_bytes:
        raise _BridgeRefusal("checkpoint_size_limit_exceeded")
    return SchrodingerBridgeCheckpointV1(
        config_id=config.config_id,
        broker_target_id=target.target_id,
        dataset_id=dataset.dataset_id,
        state_vocabulary=states,
        source_marginal=source,
        target_marginal=target_marginal,
        reference_transition=transition,
        endpoint_reference_kernel=endpoint,
        endpoint_coupling=coupling,
        source_mean_event_count=source_count,
        target_mean_event_count=target.target_mean_event_count,
        mean_training_window_duration_ns=math.fsum(durations) / len(durations),
        tune_joint_nll=_tune_nll(tune_sequences, target_marginal, transition),
        source_iid_tune_nll=_tune_nll(tune_sequences, source),
        uniform_tune_nll=math.log(len(states)),
        solver_evidence=solver,
        parameter_count=parameter_count,
        parameter_bytes=parameter_bytes,
    )


def fit_schrodinger_bridge_challenger(
    config: SchrodingerBridgeConfigV1,
    broker_target: SchrodingerBridgeBrokerTargetV1,
    calibration_windows: Sequence[EventClockCalibrationWindowV1],
    *,
    window_contexts: Sequence[SchrodingerBridgeWindowContextV1],
    protected_windows: Sequence[SchrodingerBridgeProtectedWindowV1] = (),
    information_mode: InformationMode = InformationMode.EX_POST_RECONSTRUCTION,
    as_of_ns: int | None = None,
) -> SchrodingerBridgeFitResultV1:
    """Fit the bounded reference process and endpoint bridge fail closed."""
    started = time.perf_counter()
    before_peak = peak_rss_bytes()
    if not isinstance(config, SchrodingerBridgeConfigV1):
        raise TypeError("bridge fit requires a v1 config")
    if not isinstance(broker_target, SchrodingerBridgeBrokerTargetV1):
        raise TypeError("bridge fit requires a v1 broker target")
    dataset: SchrodingerBridgeDatasetManifestV1 | None = None
    solver_evidence: SchrodingerBridgeSolverEvidenceV1 | None = None
    train_count = 0
    tune_count = 0
    train_events = 0
    tune_events = 0
    mode = InformationMode.from_value(information_mode)
    try:
        estimated_states = (
            config.time_bin_count * len(TRIANGLE_SYMBOLS) * len(MARK_STATES)
        )
        estimated_memory = estimated_states * estimated_states * 8 * 8
        if estimated_memory > config.limits.max_fit_memory_bytes:
            raise _BridgeRefusal("fit_memory_preflight_exceeded")
        dataset, train, tune = _build_dataset(
            config,
            broker_target,
            calibration_windows,
            window_contexts,
            protected_windows,
            information_mode=mode,
            as_of_ns=as_of_ns,
        )
        train_count = len(train)
        tune_count = len(tune)
        train_events = sum(len(window.events) for window, _ in train)
        tune_events = sum(len(window.events) for window, _ in tune)
        checkpoint = _fit_checkpoint(
            config, broker_target, dataset, train, tune
        )
        solver_evidence = checkpoint.solver_evidence
        wall = round((time.perf_counter() - started) * 1000)
        if wall > config.limits.max_fit_wall_time_ms:
            raise _BridgeRefusal("fit_wall_time_limit_exceeded")
        return SchrodingerBridgeFitResultV1(
            config_id=config.config_id,
            broker_target_id=broker_target.target_id,
            information_mode=mode,
            as_of_ns=as_of_ns,
            status=SchrodingerBridgeFitStatus.FITTED,
            converged=True,
            training_window_count=train_count,
            tuning_window_count=tune_count,
            training_event_count=train_events,
            tuning_event_count=tune_events,
            dataset_manifest=dataset,
            checkpoint=checkpoint,
            solver_evidence=solver_evidence,
            runtime_metadata=_runtime_metadata(),
            fit_wall_time_ms=wall,
            fit_peak_memory_bytes=max(0, peak_rss_bytes() - before_peak),
            failure_reason=None,
        )
    except (
        _BridgeRefusal,
        ArithmeticError,
        KeyError,
        TypeError,
        ValueError,
    ) as err:
        if isinstance(err, _SolverRefusal):
            solver_evidence = err.evidence
        status = (
            SchrodingerBridgeFitStatus.REFUSED
            if isinstance(err, (_BridgeRefusal, ValueError))
            else SchrodingerBridgeFitStatus.FAILED
        )
        return SchrodingerBridgeFitResultV1(
            config_id=config.config_id,
            broker_target_id=broker_target.target_id,
            information_mode=mode,
            as_of_ns=as_of_ns,
            status=status,
            converged=False,
            training_window_count=train_count,
            tuning_window_count=tune_count,
            training_event_count=train_events,
            tuning_event_count=tune_events,
            dataset_manifest=dataset,
            checkpoint=None,
            solver_evidence=solver_evidence,
            runtime_metadata=_runtime_metadata(),
            fit_wall_time_ms=round((time.perf_counter() - started) * 1000),
            fit_peak_memory_bytes=max(0, peak_rss_bytes() - before_peak),
            failure_reason=f"fit_{status.value}:{type(err).__name__}:{err}",
        )


@dataclass(frozen=True, slots=True)
class SchrodingerBridgeGenerationLineageV1:
    """One sampled endpoint pair and conditional reference-state trajectory."""

    source_event_id: str
    source_state: str
    target_state: str
    state_path: tuple[str, ...]
    destination_symbol: str
    transition_mark: str
    coupling_probability: float
    conditional_path_probability: float
    left_anchor_event_id: str
    right_anchor_event_id: str
    anchor_interval_id: str
    triangle_residual_before: float
    triangle_residual_after: float
    quarantines_checked: int
    lineage_id: str = ""
    schema_version: str = SB_GENERATION_LINEAGE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SB_GENERATION_LINEAGE_SCHEMA_VERSION:
            raise ValueError("unsupported bridge generation-lineage schema")
        for name in (
            "source_event_id",
            "source_state",
            "target_state",
            "destination_symbol",
            "transition_mark",
            "left_anchor_event_id",
            "right_anchor_event_id",
            "anchor_interval_id",
        ):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        _parse_state(self.source_state)
        _, target_symbol, target_mark = _parse_state(self.target_state)
        path = tuple(
            _required_text(item, "state_path") for item in self.state_path
        )
        if (
            len(path) < 3
            or path[0] != self.source_state
            or path[-1] != self.target_state
        ):
            raise ValueError("bridge state path endpoints differ")
        for item in path:
            _parse_state(item)
        object.__setattr__(self, "state_path", path)
        if (
            self.destination_symbol != target_symbol
            or self.transition_mark != target_mark
        ):
            raise ValueError("bridge target state and delivered mark differ")
        for name in (
            "coupling_probability",
            "conditional_path_probability",
            "triangle_residual_before",
            "triangle_residual_after",
        ):
            value = _finite_float(getattr(self, name), name)
            if value < 0.0:
                raise ValueError(f"{name} must be non-negative")
            object.__setattr__(self, name, value)
        object.__setattr__(
            self,
            "quarantines_checked",
            _bounded_int(
                self.quarantines_checked, "quarantines_checked", 0, 10**9
            ),
        )
        expected = _stable_id(
            "schrodinger-bridge-generation-lineage", self.identity_payload()
        )
        if self.lineage_id and self.lineage_id != expected:
            raise ValueError("bridge generation lineage_id differs")
        object.__setattr__(self, "lineage_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "source_event_id": self.source_event_id,
            "source_state": self.source_state,
            "target_state": self.target_state,
            "state_path": list(self.state_path),
            "destination_symbol": self.destination_symbol,
            "transition_mark": self.transition_mark,
            "coupling_probability": self.coupling_probability,
            "conditional_path_probability": self.conditional_path_probability,
            "left_anchor_event_id": self.left_anchor_event_id,
            "right_anchor_event_id": self.right_anchor_event_id,
            "anchor_interval_id": self.anchor_interval_id,
            "triangle_residual_before": self.triangle_residual_before,
            "triangle_residual_after": self.triangle_residual_after,
            "quarantines_checked": self.quarantines_checked,
            "observed_anchor_mutation": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "lineage_id": self.lineage_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> SchrodingerBridgeGenerationLineageV1:
        return cls(
            source_event_id=str(data.get("source_event_id", "")),
            source_state=str(data.get("source_state", "")),
            target_state=str(data.get("target_state", "")),
            state_path=tuple(
                str(item)
                for item in _sequence(data.get("state_path"), "state_path")
            ),
            destination_symbol=str(data.get("destination_symbol", "")),
            transition_mark=str(data.get("transition_mark", "")),
            coupling_probability=_finite_float(
                data.get("coupling_probability"), "coupling_probability"
            ),
            conditional_path_probability=_finite_float(
                data.get("conditional_path_probability"),
                "conditional_path_probability",
            ),
            left_anchor_event_id=str(data.get("left_anchor_event_id", "")),
            right_anchor_event_id=str(data.get("right_anchor_event_id", "")),
            anchor_interval_id=str(data.get("anchor_interval_id", "")),
            triangle_residual_before=_finite_float(
                data.get("triangle_residual_before"), "triangle_residual_before"
            ),
            triangle_residual_after=_finite_float(
                data.get("triangle_residual_after"), "triangle_residual_after"
            ),
            quarantines_checked=_strict_int(
                data.get("quarantines_checked"), "quarantines_checked"
            ),
            lineage_id=str(data.get("lineage_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class SchrodingerBridgeGenerationEvidenceV1:
    """All-or-nothing generation, boundary, constraint, and resource evidence."""

    config_id: str
    broker_target_id: str
    fit_id: str
    dataset_id: str
    checkpoint_id: str
    window_id: str
    window_context_id: str | None
    ensemble_member_id: str
    status: SchrodingerBridgeGenerationStatus
    attempted: bool
    input_event_count: int
    history_event_count: int
    expected_total_event_count: float
    requested_generated_event_count: int
    generated_event_count: int
    skipped_outside_anchor_count: int
    skipped_quarantine_count: int
    collision_count: int
    boundary_conditioning_l1: float
    mean_triangle_residual_before: float
    mean_triangle_residual_after: float
    generation_work: int
    semantic_seed: int | None
    input_anchor_sha256: str | None
    input_event_content_sha256: str | None
    history_content_sha256: str | None
    window_context_sha256: str | None
    lineage_content_sha256: str | None
    wall_time_ms: int
    peak_memory_bytes: int
    failure_reason: str | None = None
    evidence_id: str = ""
    schema_version: str = SB_GENERATION_EVIDENCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SB_GENERATION_EVIDENCE_SCHEMA_VERSION:
            raise ValueError("unsupported bridge generation-evidence schema")
        for name in (
            "config_id",
            "broker_target_id",
            "fit_id",
            "dataset_id",
            "checkpoint_id",
            "window_id",
            "ensemble_member_id",
        ):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        object.__setattr__(
            self, "window_context_id", _optional_text(self.window_context_id)
        )
        status = SchrodingerBridgeGenerationStatus(self.status)
        object.__setattr__(self, "status", status)
        object.__setattr__(
            self, "attempted", _strict_bool(self.attempted, "attempted")
        )
        for name in (
            "input_event_count",
            "history_event_count",
            "requested_generated_event_count",
            "generated_event_count",
            "skipped_outside_anchor_count",
            "skipped_quarantine_count",
            "collision_count",
            "generation_work",
            "wall_time_ms",
            "peak_memory_bytes",
        ):
            object.__setattr__(
                self, name, _bounded_int(getattr(self, name), name, 0, 10**15)
            )
        if self.semantic_seed is not None:
            object.__setattr__(
                self,
                "semantic_seed",
                _bounded_int(self.semantic_seed, "semantic_seed", 0, 2**64 - 1),
            )
        for name in (
            "expected_total_event_count",
            "boundary_conditioning_l1",
            "mean_triangle_residual_before",
            "mean_triangle_residual_after",
        ):
            value = _finite_float(getattr(self, name), name)
            if value < 0.0:
                raise ValueError(f"{name} must be non-negative")
            object.__setattr__(self, name, value)
        for name in (
            "input_anchor_sha256",
            "input_event_content_sha256",
            "history_content_sha256",
            "window_context_sha256",
            "lineage_content_sha256",
        ):
            value = getattr(self, name)
            if value is not None and (
                not isinstance(value, str)
                or len(value) != 64
                or any(item not in "0123456789abcdef" for item in value)
            ):
                raise ValueError(f"{name} is invalid")
        object.__setattr__(
            self, "failure_reason", _optional_text(self.failure_reason)
        )
        if status in {
            SchrodingerBridgeGenerationStatus.REFUSED,
            SchrodingerBridgeGenerationStatus.FAILED,
        }:
            if self.generated_event_count or self.failure_reason is None:
                raise ValueError(
                    "closed bridge generation contains output or lacks reason"
                )
        elif self.failure_reason is not None:
            raise ValueError(
                "successful bridge generation contains failure reason"
            )
        if (
            status is SchrodingerBridgeGenerationStatus.GENERATED
            and not self.generated_event_count
        ):
            raise ValueError("generated bridge evidence has no events")
        expected = _stable_id(
            "schrodinger-bridge-generation-evidence", self.identity_payload()
        )
        if self.evidence_id and self.evidence_id != expected:
            raise ValueError("bridge generation evidence_id differs")
        object.__setattr__(self, "evidence_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "config_id": self.config_id,
            "broker_target_id": self.broker_target_id,
            "fit_id": self.fit_id,
            "dataset_id": self.dataset_id,
            "checkpoint_id": self.checkpoint_id,
            "window_id": self.window_id,
            "window_context_id": self.window_context_id,
            "ensemble_member_id": self.ensemble_member_id,
            "status": self.status.value,
            "attempted": self.attempted,
            "input_event_count": self.input_event_count,
            "history_event_count": self.history_event_count,
            "expected_total_event_count": self.expected_total_event_count,
            "requested_generated_event_count": self.requested_generated_event_count,
            "generated_event_count": self.generated_event_count,
            "skipped_outside_anchor_count": self.skipped_outside_anchor_count,
            "skipped_quarantine_count": self.skipped_quarantine_count,
            "collision_count": self.collision_count,
            "boundary_conditioning_l1": self.boundary_conditioning_l1,
            "mean_triangle_residual_before": self.mean_triangle_residual_before,
            "mean_triangle_residual_after": self.mean_triangle_residual_after,
            "generation_work": self.generation_work,
            "semantic_seed": self.semantic_seed,
            "input_anchor_sha256": self.input_anchor_sha256,
            "input_event_content_sha256": self.input_event_content_sha256,
            "history_content_sha256": self.history_content_sha256,
            "window_context_sha256": self.window_context_sha256,
            "lineage_content_sha256": self.lineage_content_sha256,
            "failure_reason": self.failure_reason,
            "observed_anchor_mutation_count": 0,
            "automatic_winner": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "wall_time_ms": self.wall_time_ms,
            "peak_memory_bytes": self.peak_memory_bytes,
            "evidence_id": self.evidence_id,
        }

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> SchrodingerBridgeGenerationEvidenceV1:
        kwargs = dict(data)
        kwargs.pop("observed_anchor_mutation_count", None)
        kwargs.pop("automatic_winner", None)
        kwargs["status"] = SchrodingerBridgeGenerationStatus(
            _required_text(kwargs["status"], "status")
        )
        return cls(**kwargs)

    @classmethod
    def from_json(cls, text: str) -> SchrodingerBridgeGenerationEvidenceV1:
        return cls.from_dict(_json_mapping(text, 16 * 1024 * 1024))


@dataclass(frozen=True, slots=True)
class SchrodingerBridgeGenerationResultV1:
    """One complete candidate stream, sampled path lineage, and evidence."""

    events: tuple[BenchmarkEventV1, ...]
    event_lineage: tuple[SchrodingerBridgeGenerationLineageV1, ...]
    evidence: SchrodingerBridgeGenerationEvidenceV1

    def __post_init__(self) -> None:
        events = tuple(sorted(self.events, key=_event_key))
        lineages = tuple(
            sorted(self.event_lineage, key=lambda item: item.source_event_id)
        )
        if any(not isinstance(item, BenchmarkEventV1) for item in events):
            raise TypeError("bridge result events require benchmark event v1")
        if any(
            not isinstance(item, SchrodingerBridgeGenerationLineageV1)
            for item in lineages
        ):
            raise TypeError("bridge result lineage requires v1 contracts")
        generated_ids = {
            item.source_event_id
            for item in events
            if item.sparsity.startswith("schrodinger-bridge-")
        }
        if generated_ids != {item.source_event_id for item in lineages}:
            raise ValueError("bridge generated events and lineage differ")
        if self.evidence.generated_event_count != len(lineages):
            raise ValueError("bridge generation evidence count differs")
        if self.evidence.status in {
            SchrodingerBridgeGenerationStatus.REFUSED,
            SchrodingerBridgeGenerationStatus.FAILED,
        } and (events or lineages):
            raise ValueError("closed bridge result contains output")
        object.__setattr__(self, "events", events)
        object.__setattr__(self, "event_lineage", lineages)


def _validate_fit(
    config: SchrodingerBridgeConfigV1,
    target: SchrodingerBridgeBrokerTargetV1,
    fit: SchrodingerBridgeFitResultV1,
) -> None:
    if (
        fit.status is not SchrodingerBridgeFitStatus.FITTED
        or not fit.converged
        or fit.config_id != config.config_id
        or fit.broker_target_id != target.target_id
        or fit.dataset_manifest is None
        or fit.checkpoint is None
    ):
        raise SchrodingerBridgeFitError("bridge fit is absent or inconsistent")


def _semantic_seed(payload: Mapping[str, JSONValue]) -> int:
    return int(
        hashlib.sha256(canonical_contract_json(payload).encode()).hexdigest()[
            :16
        ],
        16,
    )


def _retained_history(
    events: Sequence[BenchmarkEventV1],
    *,
    config: SchrodingerBridgeConfigV1,
    window: ReconstructionWindowV1,
) -> tuple[BenchmarkEventV1, ...]:
    ordered = tuple(sorted(events, key=_event_key))
    if any(item.event_time_ns >= window.core_start_ns for item in ordered):
        raise SchrodingerBridgeGenerationError(
            "bridge streaming history contains current/future events"
        )
    return (
        ordered[-config.limits.max_history_events :]
        if config.limits.max_history_events
        else ()
    )


def _enclosing_anchor_pair(
    anchors: Mapping[str, Sequence[BenchmarkEventV1]],
    symbol: str,
    event_time_ns: int,
) -> tuple[BenchmarkEventV1, BenchmarkEventV1] | None:
    values = anchors.get(symbol, ())
    for left, right in pairwise(values):
        if left.event_time_ns < event_time_ns < right.event_time_ns:
            return left, right
    return None


def _interpolated_quote(
    anchors: Mapping[str, Sequence[BenchmarkEventV1]],
    symbol: str,
    event_time_ns: int,
) -> tuple[BenchmarkEventV1, BenchmarkEventV1, float, float] | None:
    pair = _enclosing_anchor_pair(anchors, symbol, event_time_ns)
    if pair is None:
        return None
    left, right = pair
    fraction = (event_time_ns - left.event_time_ns) / (
        right.event_time_ns - left.event_time_ns
    )
    bid = left.bid + fraction * (right.bid - left.bid)
    ask = left.ask + fraction * (right.ask - left.ask)
    return left, right, bid, ask


def _triangle_residual(midpoints: Mapping[str, float]) -> float:
    if set(midpoints) != set(TRIANGLE_SYMBOLS) or any(
        not math.isfinite(item) or item <= 0.0 for item in midpoints.values()
    ):
        raise SchrodingerBridgeGenerationError(
            "bridge triangle midpoint support is incomplete"
        )
    return abs(
        math.log(midpoints["EURGBP"])
        + math.log(midpoints["GBPUSD"])
        - math.log(midpoints["EURUSD"])
    )


def _project_quote(
    anchors: Mapping[str, Sequence[BenchmarkEventV1]],
    *,
    symbol: str,
    mark: str,
    event_time_ns: int,
    target: SchrodingerBridgeBrokerTargetV1,
    config: SchrodingerBridgeConfigV1,
) -> tuple[BenchmarkEventV1, BenchmarkEventV1, float, float, float, float]:
    selected = _interpolated_quote(anchors, symbol, event_time_ns)
    if selected is None:
        raise _BridgeRefusal("candidate_is_outside_destination_anchor_support")
    left, right, interpolated_bid, interpolated_ask = selected
    if mark == "unchanged":
        bid, ask = left.bid, left.ask
    elif mark == "ask_only":
        bid, ask = left.bid, max(left.bid, interpolated_ask)
    elif mark == "bid_only":
        bid, ask = min(left.ask, interpolated_bid), left.ask
    elif mark == "joint":
        bid, ask = interpolated_bid, interpolated_ask
    else:
        raise SchrodingerBridgeGenerationError(
            "bridge quote mark is unsupported"
        )
    midpoint_support: dict[str, float] = {}
    for triangle_symbol in TRIANGLE_SYMBOLS:
        quote = _interpolated_quote(anchors, triangle_symbol, event_time_ns)
        if quote is None:
            raise _BridgeRefusal("triangle_anchor_support_is_incomplete")
        midpoint_support[triangle_symbol] = (quote[2] + quote[3]) / 2.0
    current_mid = (bid + ask) / 2.0
    # The pre-projection residual must describe the marked proposal being
    # projected, not the unmarked linear interpolation it replaced.
    midpoint_support[symbol] = current_mid
    before = _triangle_residual(midpoint_support)
    if symbol == "EURUSD":
        implied = midpoint_support["EURGBP"] * midpoint_support["GBPUSD"]
    elif symbol == "EURGBP":
        implied = midpoint_support["EURUSD"] / midpoint_support["GBPUSD"]
    else:
        implied = midpoint_support["EURUSD"] / midpoint_support["EURGBP"]
    projected_mid = math.exp(
        (1.0 - config.triangle_projection_strength) * math.log(current_mid)
        + config.triangle_projection_strength * math.log(implied)
    )
    spread = ask - bid
    if target.spread_target is not None:
        bounded_target = min(target.spread_target, max(spread * 2.0, 1e-15))
        spread = (
            1.0 - target.transfer_strength
        ) * spread + target.transfer_strength * bounded_target
    bid = projected_mid - spread / 2.0
    ask = projected_mid + spread / 2.0
    midpoint_support[symbol] = projected_mid
    after = _triangle_residual(midpoint_support)
    if (
        not all(math.isfinite(item) and item > 0.0 for item in (bid, ask))
        or ask < bid
        or after > before + 1e-12
    ):
        raise SchrodingerBridgeGenerationError(
            "bridge quote/triangle projection is invalid"
        )
    return left, right, bid, ask, before, after


def _poisson_count(
    mean: float, rng: random.Random, work: list[int], limit: int
) -> int:
    if mean <= 0.0:
        return 0
    elapsed = 0.0
    count = 0
    while True:
        work[0] += 1
        if work[0] > limit:
            raise _BridgeRefusal("generation_poisson_work_limit_exceeded")
        elapsed += rng.expovariate(mean)
        if elapsed >= 1.0:
            return count
        count += 1


def _conditional_path(
    source: int,
    target: int,
    transition: Sequence[Sequence[float]],
    powers: Sequence[Sequence[Sequence[float]]],
    rng: random.Random,
    work: list[int],
    limit: int,
) -> tuple[tuple[int, ...], float]:
    steps = len(powers) - 1
    current = source
    path = [source]
    probability = 1.0
    for step in range(steps):
        remaining = steps - step
        denominator = powers[remaining][current][target]
        if denominator <= 0.0:
            raise _BridgeRefusal("conditional_bridge_endpoint_is_off_support")
        weights = [
            transition[current][candidate]
            * powers[remaining - 1][candidate][target]
            / denominator
            for candidate in range(len(transition))
        ]
        work[0] += len(weights)
        if work[0] > limit:
            raise _BridgeRefusal("generation_path_work_limit_exceeded")
        selected = _sample_index(weights, rng)
        total = math.fsum(weights)
        probability *= weights[selected] / total
        path.append(selected)
        current = selected
    if path[-1] != target:
        raise SchrodingerBridgeGenerationError(
            "conditional bridge did not reach its endpoint"
        )
    return tuple(path), probability


def _history_conditioned_coupling(
    checkpoint: SchrodingerBridgeCheckpointV1,
    history: Sequence[BenchmarkEventV1],
    config: SchrodingerBridgeConfigV1,
) -> tuple[tuple[float, ...], float]:
    size = len(checkpoint.state_vocabulary)
    flat = [item for row in checkpoint.endpoint_coupling for item in row]
    if not history:
        return tuple(flat), 0.0
    last = history[-1]
    same_symbol = [item for item in history if item.symbol == last.symbol]
    previous = same_symbol[-2] if len(same_symbol) > 1 else None
    mark = _event_mark(previous, last)
    state = _state_name(config.time_bin_count - 1, last.symbol, mark)
    try:
        last_index = checkpoint.state_vocabulary.index(state)
    except ValueError as err:
        raise _BridgeRefusal("streaming_boundary_state_is_off_support") from err
    conditioned = [
        checkpoint.endpoint_coupling[row][column]
        * checkpoint.reference_transition[last_index][row]
        for row in range(size)
        for column in range(size)
    ]
    conditioned_total = math.fsum(conditioned)
    if conditioned_total <= 0.0:
        raise _BridgeRefusal("streaming_boundary_coupling_has_no_support")
    normalized_conditioned = [item / conditioned_total for item in conditioned]
    original_total = math.fsum(flat)
    normalized_original = [item / original_total for item in flat]
    l1 = math.fsum(
        abs(left - right)
        for left, right in zip(normalized_original, normalized_conditioned)
    )
    return tuple(normalized_conditioned), l1


def _validate_generation_inputs(
    config: SchrodingerBridgeConfigV1,
    target: SchrodingerBridgeBrokerTargetV1,
    fit: SchrodingerBridgeFitResultV1,
    context: SchrodingerBridgeWindowContextV1,
    events: Sequence[BenchmarkEventV1],
    scenario: BenchmarkScenarioV1,
    window: ReconstructionWindowV1,
    ensemble_member_id: str,
) -> tuple[BenchmarkEventV1, ...]:
    _validate_fit(config, target, fit)
    if context.window_id != window.window_id:
        raise SchrodingerBridgeGenerationError(
            "bridge context and generation window differ"
        )
    if ensemble_member_id != window.ensemble_member_id:
        raise SchrodingerBridgeGenerationError(
            "bridge ensemble member and window differ"
        )
    if scenario.epoch_id != context.technology_label:
        raise SchrodingerBridgeGenerationError(
            "bridge scenario and feed context differ"
        )
    ordered = tuple(sorted(events, key=_event_key))
    if not ordered:
        raise _BridgeRefusal("bridge generation requires observed anchors")
    if len({item.benchmark_event_id for item in ordered}) != len(ordered):
        raise SchrodingerBridgeGenerationError("bridge anchors are duplicated")
    by_symbol: Counter[str] = Counter()
    for event in ordered:
        if (
            event.symbol not in TRIANGLE_SYMBOLS
            or event.symbol.lower() not in window.symbols
            or not window.reads_event_time(event.event_time_ns)
            or not math.isfinite(event.bid)
            or not math.isfinite(event.ask)
            or event.bid <= 0.0
            or event.ask < event.bid
        ):
            raise SchrodingerBridgeGenerationError(
                "bridge anchor is invalid or outside the generation window"
            )
        by_symbol[event.symbol] += 1
    if any(by_symbol[symbol] < 2 for symbol in TRIANGLE_SYMBOLS):
        raise _BridgeRefusal("bridge generation lacks triangle anchor support")
    return ordered


@dataclass(frozen=True, slots=True)
class FittedSchrodingerBridgeBenchmarkGeneratorV1(BenchmarkGeneratorV1):
    """Benchmark adapter for one fitted, opt-in Schrödinger bridge."""

    candidate: BenchmarkCandidateV1
    config: SchrodingerBridgeConfigV1
    broker_target: SchrodingerBridgeBrokerTargetV1
    fit_result: SchrodingerBridgeFitResultV1
    window_contexts: Mapping[str, SchrodingerBridgeWindowContextV1] = field(
        default_factory=dict
    )
    quarantines: tuple[HistoricalCarvingQuarantineV1, ...] = ()
    candidate_id: str = field(init=False)
    event_schema_version: str = BENCHMARK_EVENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.candidate.kind is not BenchmarkCandidateKind.CANDIDATE
            or self.candidate.method_id != SB_GENERATOR_ID
        ):
            raise ValueError("bridge adapter requires its candidate")
        _validate_fit(self.config, self.broker_target, self.fit_result)
        contexts = dict(self.window_contexts)
        if any(key != value.window_id for key, value in contexts.items()):
            raise ValueError("bridge context key differs")
        quarantines = tuple(
            sorted(self.quarantines, key=lambda item: item.quarantine_id)
        )
        if any(
            not isinstance(item, HistoricalCarvingQuarantineV1)
            for item in quarantines
        ):
            raise TypeError("bridge quarantines require carving v1 contracts")
        object.__setattr__(self, "window_contexts", contexts)
        object.__setattr__(self, "quarantines", quarantines)
        object.__setattr__(self, "candidate_id", self.candidate.candidate_id)

    def generate(
        self,
        degraded_events: Sequence[BenchmarkEventV1],
        *,
        scenario: BenchmarkScenarioV1,
        window: ReconstructionWindowV1,
        ensemble_member_id: str,
    ) -> Sequence[BenchmarkEventV1]:
        result = self.generate_with_evidence(
            degraded_events,
            scenario=scenario,
            window=window,
            ensemble_member_id=ensemble_member_id,
        )
        if result.evidence.status in {
            SchrodingerBridgeGenerationStatus.REFUSED,
            SchrodingerBridgeGenerationStatus.FAILED,
        }:
            raise SchrodingerBridgeGenerationError(
                result.evidence.failure_reason or "bridge generation failed"
            )
        return result.events

    def generate_with_evidence(
        self,
        degraded_events: Sequence[BenchmarkEventV1],
        *,
        scenario: BenchmarkScenarioV1,
        window: ReconstructionWindowV1,
        ensemble_member_id: str,
        history_events: Sequence[BenchmarkEventV1] = (),
    ) -> SchrodingerBridgeGenerationResultV1:
        """Sample bounded endpoint pairs and conditional Markov bridge paths."""
        started = time.perf_counter()
        before_peak = peak_rss_bytes()
        raw = tuple(degraded_events)
        context = self.window_contexts.get(window.window_id)
        dataset = self.fit_result.dataset_manifest
        checkpoint = self.fit_result.checkpoint
        if dataset is None or checkpoint is None:
            raise SchrodingerBridgeFitError(
                "bridge fitted artifacts are absent"
            )
        history_count = 0
        expected_total = 0.0
        requested = 0
        skipped_anchor = 0
        skipped_quarantine = 0
        collisions = 0
        boundary_l1 = 0.0
        work = [0]
        seed: int | None = None
        anchor_hash: str | None = None
        input_hash: str | None = None
        history_hash: str | None = None
        context_hash: str | None = None
        try:
            if context is None:
                raise SchrodingerBridgeGenerationError(
                    "bridge generation context is absent"
                )
            ordered = _validate_generation_inputs(
                self.config,
                self.broker_target,
                self.fit_result,
                context,
                raw,
                scenario,
                window,
                ensemble_member_id,
            )
            history = _retained_history(
                history_events, config=self.config, window=window
            )
            history_count = len(history)
            anchor_hash = _anchor_digest(ordered)
            input_hash = _event_content_digest(ordered)
            history_hash = _event_content_digest(history)
            context_hash = hashlib.sha256(
                canonical_contract_json(context.to_dict()).encode()
            ).hexdigest()
            seed = _semantic_seed(
                {
                    "architecture": self.config.architecture,
                    "config_id": self.config.config_id,
                    "broker_target_id": self.broker_target.target_id,
                    "fit_id": self.fit_result.fit_id,
                    "dataset_id": dataset.dataset_id,
                    "checkpoint_id": checkpoint.checkpoint_id,
                    "scenario_id": scenario.scenario_id,
                    "window_id": window.window_id,
                    "ensemble_member_id": ensemble_member_id,
                    "input_event_content_sha256": input_hash,
                    "history_content_sha256": history_hash,
                    "window_context_sha256": context_hash,
                    "quarantine_ids": [
                        item.quarantine_id for item in self.quarantines
                    ],
                }
            )
            rng = random.Random(seed)
            duration_scale = (window.core_end_ns - window.core_start_ns) / (
                checkpoint.mean_training_window_duration_ns
            )
            expected_total = checkpoint.target_mean_event_count * duration_scale
            requested = _poisson_count(
                max(0.0, expected_total - len(ordered)),
                rng,
                work,
                self.config.limits.max_generation_work,
            )
            if requested > self.config.limits.max_generation_events:
                raise _BridgeRefusal("generation_event_limit_exceeded")
            if (
                requested
                > len(ordered) * self.config.limits.max_candidate_amplification
            ):
                raise _BridgeRefusal("candidate_amplification_limit_exceeded")
            estimated_memory = (
                1024 * 1024
                + len(checkpoint.state_vocabulary) ** 2 * 32
                + (len(ordered) + requested) * 2048
            )
            if (
                estimated_memory
                > self.config.limits.max_generation_memory_bytes
            ):
                raise _BridgeRefusal("generation_memory_preflight_exceeded")
            coupling, boundary_l1 = _history_conditioned_coupling(
                checkpoint, history, self.config
            )
            anchors: dict[str, list[BenchmarkEventV1]] = defaultdict(list)
            for event in ordered:
                anchors[event.symbol].append(event)
            occupied = {item.event_time_ns for item in ordered}
            generated: list[BenchmarkEventV1] = []
            lineages: list[SchrodingerBridgeGenerationLineageV1] = []
            size = len(checkpoint.state_vocabulary)
            # Conditional paths reuse the same finite-horizon Doob factors.
            # Computing these per proposal would make campaign cost scale with
            # both event count and dense matrix multiplication.
            power_work = size**3 * self.config.bridge_steps
            work[0] += power_work
            if work[0] > self.config.limits.max_generation_work:
                raise _BridgeRefusal(
                    "generation_path_power_work_limit_exceeded"
                )
            path_powers = tuple(
                _matrix_power(checkpoint.reference_transition, exponent)
                for exponent in range(self.config.bridge_steps + 1)
            )
            if (
                time.perf_counter() - started
                > self.config.limits.max_generation_wall_time_ms / 1000.0
            ):
                raise _BridgeRefusal("generation_wall_time_limit_exceeded")
            for ordinal in range(requested):
                if (
                    time.perf_counter() - started
                    > self.config.limits.max_generation_wall_time_ms / 1000.0
                ):
                    raise _BridgeRefusal("generation_wall_time_limit_exceeded")
                pair_index = _sample_index(coupling, rng)
                source_index, target_index = divmod(pair_index, size)
                state_path, path_probability = _conditional_path(
                    source_index,
                    target_index,
                    checkpoint.reference_transition,
                    path_powers,
                    rng,
                    work,
                    self.config.limits.max_generation_work,
                )
                bin_index, symbol, mark = _parse_state(
                    checkpoint.state_vocabulary[target_index]
                )
                bin_start = window.core_start_ns + (
                    (window.core_end_ns - window.core_start_ns)
                    * bin_index
                    // self.config.time_bin_count
                )
                bin_end = window.core_start_ns + (
                    (window.core_end_ns - window.core_start_ns)
                    * (bin_index + 1)
                    // self.config.time_bin_count
                )
                if bin_end - bin_start <= 1:
                    raise _BridgeRefusal("generation_time_bin_is_empty")
                event_time_ns = bin_start + rng.randrange(bin_end - bin_start)
                if event_time_ns in occupied:
                    collisions += 1
                    continue
                pair = _enclosing_anchor_pair(anchors, symbol, event_time_ns)
                if pair is None or not window.owns_event_time(event_time_ns):
                    skipped_anchor += 1
                    continue
                if any(
                    _enclosing_anchor_pair(
                        anchors, triangle_symbol, event_time_ns
                    )
                    is None
                    for triangle_symbol in TRIANGLE_SYMBOLS
                ):
                    # Real synchronized windows have asynchronous first/last
                    # observations. A proposal outside their common anchor
                    # support is rejected locally; it is never an emitted path
                    # and does not invalidate otherwise supported proposals.
                    skipped_anchor += 1
                    continue
                if any(
                    item.symbol == symbol
                    and item.start_ns <= event_time_ns < item.end_ns
                    for item in self.quarantines
                ):
                    skipped_quarantine += 1
                    continue
                left, right, bid, ask, triangle_before, triangle_after = (
                    _project_quote(
                        anchors,
                        symbol=symbol,
                        mark=mark,
                        event_time_ns=event_time_ns,
                        target=self.broker_target,
                        config=self.config,
                    )
                )
                interval_id = derive_anchor_interval_id(
                    left.benchmark_event_id, right.benchmark_event_id
                )
                source_id = _stable_id(
                    "schrodinger-bridge-event",
                    {
                        "semantic_seed": seed,
                        "ordinal": ordinal,
                        "event_time_ns": event_time_ns,
                        "source_state": checkpoint.state_vocabulary[
                            source_index
                        ],
                        "target_state": checkpoint.state_vocabulary[
                            target_index
                        ],
                        "state_path": [
                            checkpoint.state_vocabulary[item]
                            for item in state_path
                        ],
                        "anchor_interval_id": interval_id,
                        "checkpoint_id": checkpoint.checkpoint_id,
                    },
                )
                occupied.add(event_time_ns)
                generated.append(
                    BenchmarkEventV1(
                        source_event_id=source_id,
                        symbol=symbol,
                        event_time_ns=event_time_ns,
                        event_sequence=ordinal + 1,
                        bid=bid,
                        ask=ask,
                        epoch_id=left.epoch_id,
                        session=context.session,
                        event_state=mark,
                        sparsity="schrodinger-bridge-markov-sinkhorn-cpu-v1",
                        ensemble_member_id=ensemble_member_id,
                        anchor_id=None,
                        support_lower_mid=min(
                            left.mid, right.mid, (bid + ask) / 2.0
                        ),
                        support_upper_mid=max(
                            left.mid, right.mid, (bid + ask) / 2.0
                        ),
                    )
                )
                lineages.append(
                    SchrodingerBridgeGenerationLineageV1(
                        source_event_id=source_id,
                        source_state=checkpoint.state_vocabulary[source_index],
                        target_state=checkpoint.state_vocabulary[target_index],
                        state_path=tuple(
                            checkpoint.state_vocabulary[item]
                            for item in state_path
                        ),
                        destination_symbol=symbol,
                        transition_mark=mark,
                        coupling_probability=coupling[pair_index],
                        conditional_path_probability=path_probability,
                        left_anchor_event_id=left.benchmark_event_id,
                        right_anchor_event_id=right.benchmark_event_id,
                        anchor_interval_id=interval_id,
                        triangle_residual_before=triangle_before,
                        triangle_residual_after=triangle_after,
                        quarantines_checked=len(self.quarantines),
                    )
                )
            measured_peak = max(0, peak_rss_bytes() - before_peak)
            if measured_peak > self.config.limits.max_generation_memory_bytes:
                raise _BridgeRefusal("generation_measured_memory_exceeded")
            lineage_hash = hashlib.sha256(
                canonical_contract_json(
                    [item.to_dict() for item in lineages]
                ).encode()
            ).hexdigest()
            status = (
                SchrodingerBridgeGenerationStatus.GENERATED
                if generated
                else SchrodingerBridgeGenerationStatus.EMPTY
            )
            before_mean = (
                math.fsum(item.triangle_residual_before for item in lineages)
                / len(lineages)
                if lineages
                else 0.0
            )
            after_mean = (
                math.fsum(item.triangle_residual_after for item in lineages)
                / len(lineages)
                if lineages
                else 0.0
            )
            evidence = SchrodingerBridgeGenerationEvidenceV1(
                config_id=self.config.config_id,
                broker_target_id=self.broker_target.target_id,
                fit_id=self.fit_result.fit_id,
                dataset_id=dataset.dataset_id,
                checkpoint_id=checkpoint.checkpoint_id,
                window_id=window.window_id,
                window_context_id=context.context_id,
                ensemble_member_id=ensemble_member_id,
                status=status,
                attempted=True,
                input_event_count=len(ordered),
                history_event_count=history_count,
                expected_total_event_count=expected_total,
                requested_generated_event_count=requested,
                generated_event_count=len(generated),
                skipped_outside_anchor_count=skipped_anchor,
                skipped_quarantine_count=skipped_quarantine,
                collision_count=collisions,
                boundary_conditioning_l1=boundary_l1,
                mean_triangle_residual_before=before_mean,
                mean_triangle_residual_after=after_mean,
                generation_work=work[0],
                semantic_seed=seed,
                input_anchor_sha256=anchor_hash,
                input_event_content_sha256=input_hash,
                history_content_sha256=history_hash,
                window_context_sha256=context_hash,
                lineage_content_sha256=lineage_hash,
                wall_time_ms=round((time.perf_counter() - started) * 1000),
                peak_memory_bytes=measured_peak,
                failure_reason=None,
            )
            return SchrodingerBridgeGenerationResultV1(
                tuple(ordered) + tuple(generated), tuple(lineages), evidence
            )
        except (
            SchrodingerBridgeFitError,
            SchrodingerBridgeGenerationError,
            ArithmeticError,
            KeyError,
            TypeError,
            ValueError,
        ) as err:
            status = (
                SchrodingerBridgeGenerationStatus.REFUSED
                if isinstance(err, (_BridgeRefusal, SchrodingerBridgeFitError))
                else SchrodingerBridgeGenerationStatus.FAILED
            )
            evidence = SchrodingerBridgeGenerationEvidenceV1(
                config_id=self.config.config_id,
                broker_target_id=self.broker_target.target_id,
                fit_id=self.fit_result.fit_id,
                dataset_id=dataset.dataset_id,
                checkpoint_id=checkpoint.checkpoint_id,
                window_id=window.window_id,
                window_context_id=(
                    context.context_id if context is not None else None
                ),
                ensemble_member_id=ensemble_member_id,
                status=status,
                attempted=True,
                input_event_count=len(raw),
                history_event_count=history_count,
                expected_total_event_count=expected_total,
                requested_generated_event_count=requested,
                generated_event_count=0,
                skipped_outside_anchor_count=skipped_anchor,
                skipped_quarantine_count=skipped_quarantine,
                collision_count=collisions,
                boundary_conditioning_l1=boundary_l1,
                mean_triangle_residual_before=0.0,
                mean_triangle_residual_after=0.0,
                generation_work=work[0],
                semantic_seed=seed,
                input_anchor_sha256=anchor_hash,
                input_event_content_sha256=input_hash,
                history_content_sha256=history_hash,
                window_context_sha256=context_hash,
                lineage_content_sha256=None,
                wall_time_ms=round((time.perf_counter() - started) * 1000),
                peak_memory_bytes=max(0, peak_rss_bytes() - before_peak),
                failure_reason=f"generation_{status.value}:{type(err).__name__}:{err}",
            )
            return SchrodingerBridgeGenerationResultV1((), (), evidence)


def build_schrodinger_bridge_benchmark_candidate(
    config: SchrodingerBridgeConfigV1,
    broker_target: SchrodingerBridgeBrokerTargetV1,
    fit_result: SchrodingerBridgeFitResultV1,
    *,
    ensemble_member_ids: Sequence[str],
) -> BenchmarkCandidateV1:
    """Describe the fitted research challenger without promotion authority."""
    if (
        fit_result.config_id != config.config_id
        or fit_result.broker_target_id != broker_target.target_id
    ):
        raise ValueError("bridge fit, config, and broker target differ")
    return BenchmarkCandidateV1(
        kind=BenchmarkCandidateKind.CANDIDATE,
        method_id=SB_GENERATOR_ID,
        implementation_version=SB_IMPLEMENTATION_VERSION,
        parameters={
            "config_id": config.config_id,
            "broker_target_id": broker_target.target_id,
            "fit_id": fit_result.fit_id,
            "hypothesis_id": SB_HYPOTHESIS_ID,
            "automatic_winner": False,
            "promotion_policy": SB_PROMOTION_POLICY,
        },
        ensemble_member_ids=tuple(ensemble_member_ids),
    )


def build_fitted_schrodinger_bridge_generator(
    config: SchrodingerBridgeConfigV1,
    broker_target: SchrodingerBridgeBrokerTargetV1,
    fit_result: SchrodingerBridgeFitResultV1,
    *,
    ensemble_member_ids: Sequence[str],
    window_contexts: Mapping[str, SchrodingerBridgeWindowContextV1],
    quarantines: Sequence[HistoricalCarvingQuarantineV1] = (),
) -> FittedSchrodingerBridgeBenchmarkGeneratorV1:
    """Build the benchmark adapter only for one converged fitted bridge."""
    _validate_fit(config, broker_target, fit_result)
    candidate = build_schrodinger_bridge_benchmark_candidate(
        config,
        broker_target,
        fit_result,
        ensemble_member_ids=ensemble_member_ids,
    )
    return FittedSchrodingerBridgeBenchmarkGeneratorV1(
        candidate=candidate,
        config=config,
        broker_target=broker_target,
        fit_result=fit_result,
        window_contexts=window_contexts,
        quarantines=tuple(quarantines),
    )


@dataclass(frozen=True, slots=True)
class SchrodingerBridgeCandidateLineageV1:
    """Shared-carving pointer to one sampled bridge path."""

    event_id: str
    transformation_id: str
    generation_lineage_id: str
    schema_version: str = SB_CANDIDATE_LINEAGE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SB_CANDIDATE_LINEAGE_SCHEMA_VERSION:
            raise ValueError("unsupported bridge candidate-lineage schema")
        for name in ("event_id", "transformation_id", "generation_lineage_id"):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "transformation_id": self.transformation_id,
            "generation_lineage_id": self.generation_lineage_id,
        }


@dataclass(frozen=True, slots=True)
class SchrodingerBridgeCandidateBatchV1:
    """Candidate-only bridge proposals for one immutable anchor interval."""

    run_id: str
    window_id: str
    ensemble_member_id: str
    symbol: str
    anchor_interval_id: str
    left_anchor_event_id: str
    right_anchor_event_id: str
    generator_config_id: str
    information_mode: InformationMode
    session_state: str
    special_tags: tuple[str, ...]
    event_tags: tuple[str, ...]
    status: MotifGenerationStatus
    events: tuple[SyntheticEventV1, ...]
    event_lineage: tuple[SchrodingerBridgeCandidateLineageV1, ...]
    broker_target_id: str
    fit_id: str
    dataset_id: str
    checkpoint_id: str
    generation_evidence_id: str
    window_context_id: str
    batch_id: str = ""
    schema_version: str = SB_CANDIDATE_BATCH_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SB_CANDIDATE_BATCH_SCHEMA_VERSION:
            raise ValueError("unsupported bridge candidate-batch schema")
        for name in (
            "run_id",
            "window_id",
            "ensemble_member_id",
            "symbol",
            "anchor_interval_id",
            "left_anchor_event_id",
            "right_anchor_event_id",
            "generator_config_id",
            "session_state",
            "broker_target_id",
            "fit_id",
            "dataset_id",
            "checkpoint_id",
            "generation_evidence_id",
            "window_context_id",
        ):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        object.__setattr__(self, "symbol", self.symbol.lower())
        object.__setattr__(
            self,
            "information_mode",
            InformationMode.from_value(self.information_mode),
        )
        object.__setattr__(
            self,
            "special_tags",
            tuple(sorted({_required_text(item) for item in self.special_tags})),
        )
        object.__setattr__(
            self,
            "event_tags",
            tuple(sorted({_required_text(item) for item in self.event_tags})),
        )
        status = MotifGenerationStatus(self.status)
        object.__setattr__(self, "status", status)
        events = tuple(
            sorted(
                self.events,
                key=lambda item: (
                    item.event_time_ns,
                    item.event_sequence,
                    item.event_id,
                ),
            )
        )
        lineages = tuple(
            sorted(self.event_lineage, key=lambda item: item.event_id)
        )
        if status is MotifGenerationStatus.GENERATED and not events:
            raise ValueError("generated bridge batch requires events")
        if status is not MotifGenerationStatus.GENERATED and events:
            raise ValueError("closed bridge batch contains events")
        if any(
            item.origin is not SyntheticEventOrigin.SYNTHETIC
            or item.run_id != self.run_id
            or item.ensemble_member_id != self.ensemble_member_id
            or item.symbol != self.symbol
            or item.anchor_interval_id != self.anchor_interval_id
            or item.left_anchor_event_id != self.left_anchor_event_id
            or item.right_anchor_event_id != self.right_anchor_event_id
            or item.generator_config_id != self.generator_config_id
            or item.constraint_set_id != CANDIDATE_ONLY_CONSTRAINT_SET_ID
            for item in events
        ):
            raise ValueError("bridge candidate event differs from batch scope")
        if {item.event_id for item in events} != {
            item.event_id for item in lineages
        }:
            raise ValueError("bridge candidate lineage does not reconcile")
        object.__setattr__(self, "events", events)
        object.__setattr__(self, "event_lineage", lineages)
        expected = _stable_id(
            "schrodinger-bridge-candidate-batch", self.identity_payload()
        )
        if self.batch_id and self.batch_id != expected:
            raise ValueError("bridge candidate batch_id differs")
        object.__setattr__(self, "batch_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "window_id": self.window_id,
            "ensemble_member_id": self.ensemble_member_id,
            "symbol": self.symbol,
            "anchor_interval_id": self.anchor_interval_id,
            "left_anchor_event_id": self.left_anchor_event_id,
            "right_anchor_event_id": self.right_anchor_event_id,
            "generator_config_id": self.generator_config_id,
            "information_mode": self.information_mode.value,
            "session_state": self.session_state,
            "special_tags": list(self.special_tags),
            "event_tags": list(self.event_tags),
            "status": self.status.value,
            "broker_target_id": self.broker_target_id,
            "fit_id": self.fit_id,
            "dataset_id": self.dataset_id,
            "checkpoint_id": self.checkpoint_id,
            "generation_evidence_id": self.generation_evidence_id,
            "window_context_id": self.window_context_id,
            "event_content_sha256": hashlib.sha256(
                canonical_contract_json(
                    [item.to_dict() for item in self.events]
                ).encode()
            ).hexdigest(),
            "lineage_content_sha256": hashlib.sha256(
                canonical_contract_json(
                    [item.to_dict() for item in self.event_lineage]
                ).encode()
            ).hexdigest(),
            "candidate_only": True,
        }

    def metadata(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "batch_id": self.batch_id,
            "events_inline": False,
            "event_lineage_inline": False,
        }

    def lineage_for(self, event_id: str) -> SchrodingerBridgeCandidateLineageV1:
        wanted = _required_text(event_id)
        for lineage in self.event_lineage:
            if lineage.event_id == wanted:
                return lineage
        raise KeyError(wanted)


def _synthetic_anchor_digest(events: Iterable[SyntheticEventV1]) -> str:
    payload = sorted(
        (
            item.symbol.lower(),
            item.event_time_ns,
            item.event_sequence,
            item.bid,
            item.ask,
        )
        for item in events
    )
    return hashlib.sha256(canonical_contract_json(payload).encode()).hexdigest()


def build_schrodinger_bridge_candidate_batches(
    *,
    run: ReconstructionRunV1,
    window: ReconstructionWindowV1,
    config: SchrodingerBridgeConfigV1,
    broker_target: SchrodingerBridgeBrokerTargetV1,
    fit_result: SchrodingerBridgeFitResultV1,
    generation_result: SchrodingerBridgeGenerationResultV1,
    context: SchrodingerBridgeWindowContextV1,
    observed_events: Sequence[SyntheticEventV1],
    session_state: str,
    special_tags: Sequence[str] = (),
    event_tags: Sequence[str] = (),
) -> tuple[SchrodingerBridgeCandidateBatchV1, ...]:
    """Project bridge proposals into the generator-neutral carving protocol."""
    if (
        window.run_id != run.run_id
        or window.ensemble_member_id not in run.ensemble_member_ids
    ):
        raise ValueError("bridge candidate window does not belong to run")
    if (
        config.config_id not in run.configuration_ids
        or broker_target.target_id not in run.configuration_ids
    ):
        raise ValueError(
            "bridge config or target is absent from reconstruction run"
        )
    _validate_fit(config, broker_target, fit_result)
    dataset = fit_result.dataset_manifest
    checkpoint = fit_result.checkpoint
    if dataset is None or checkpoint is None:
        raise ValueError("bridge candidate requires fitted artifacts")
    evidence = generation_result.evidence
    if (
        evidence.config_id != config.config_id
        or evidence.broker_target_id != broker_target.target_id
        or evidence.fit_id != fit_result.fit_id
        or evidence.dataset_id != dataset.dataset_id
        or evidence.checkpoint_id != checkpoint.checkpoint_id
        or evidence.window_id != window.window_id
        or evidence.window_context_id != context.context_id
        or context.window_id != window.window_id
    ):
        raise ValueError("bridge fit/generation/context identities differ")
    observed = tuple(
        sorted(
            observed_events,
            key=lambda item: (
                item.symbol,
                item.event_time_ns,
                item.event_sequence,
                item.event_id,
            ),
        )
    )
    if not observed or any(
        not isinstance(item, SyntheticEventV1)
        or item.origin is not SyntheticEventOrigin.OBSERVED
        or item.run_id != run.run_id
        or item.ensemble_member_id != window.ensemble_member_id
        or item.symbol not in window.symbols
        or not window.reads_event_time(item.event_time_ns)
        for item in observed
    ):
        raise ValueError("bridge carving projection requires observed anchors")
    if (
        evidence.input_anchor_sha256 is not None
        and _synthetic_anchor_digest(observed) != evidence.input_anchor_sha256
    ):
        raise ValueError("bridge carving anchors differ from generation input")
    upstream_closed = evidence.status in {
        SchrodingerBridgeGenerationStatus.REFUSED,
        SchrodingerBridgeGenerationStatus.FAILED,
    }
    proposals = tuple(
        item
        for item in generation_result.events
        if item.sparsity.startswith("schrodinger-bridge-")
    )
    lineage_by_id = {
        item.source_event_id: item for item in generation_result.event_lineage
    }
    if set(lineage_by_id) != {item.source_event_id for item in proposals}:
        raise ValueError("bridge proposals and path lineage differ")
    by_symbol: dict[str, list[SyntheticEventV1]] = defaultdict(list)
    for event in observed:
        by_symbol[event.symbol].append(event)
    assigned: set[str] = set()
    batches: list[SchrodingerBridgeCandidateBatchV1] = []
    for symbol in sorted(by_symbol):
        anchors = by_symbol[symbol]
        if len(anchors) < 2:
            raise ValueError("each bridge carving symbol requires two anchors")
        for left, right in pairwise(anchors):
            interval_id = derive_anchor_interval_id(
                left.event_id, right.event_id
            )
            selected = tuple(
                item
                for item in proposals
                if item.symbol.lower() == symbol
                and left.event_time_ns
                < item.event_time_ns
                < right.event_time_ns
            )
            assigned.update(item.source_event_id for item in selected)
            transformation_id = _stable_id(
                "schrodinger-bridge-interval-transformation",
                {
                    "fit_id": fit_result.fit_id,
                    "checkpoint_id": checkpoint.checkpoint_id,
                    "generation_evidence_id": evidence.evidence_id,
                    "window_context_id": context.context_id,
                    "anchor_interval_id": interval_id,
                },
            )
            synthetic = tuple(
                SyntheticEventV1.generated(
                    symbol=symbol,
                    event_time_ns=item.event_time_ns,
                    event_sequence=ordinal,
                    bid=item.bid,
                    ask=item.ask,
                    run_id=run.run_id,
                    ensemble_member_id=window.ensemble_member_id,
                    source_version_id=left.source_version_id,
                    left_anchor_event_id=left.event_id,
                    right_anchor_event_id=right.event_id,
                    anchor_interval_id=interval_id,
                    generator_id=SB_GENERATOR_ID,
                    generator_version=SB_IMPLEMENTATION_VERSION,
                    generator_config_id=config.config_id,
                    reference_id=item.source_event_id,
                    motif_id=SB_GENERATOR_ID,
                    feed_epoch_id=item.epoch_id,
                    constraint_set_id=CANDIDATE_ONLY_CONSTRAINT_SET_ID,
                    confidence=lineage_by_id[
                        item.source_event_id
                    ].coupling_probability,
                )
                for ordinal, item in enumerate(selected, start=1)
            )
            status = (
                MotifGenerationStatus.REFUSED
                if upstream_closed
                else (
                    MotifGenerationStatus.GENERATED
                    if synthetic
                    else MotifGenerationStatus.EMPTY
                )
            )
            batches.append(
                SchrodingerBridgeCandidateBatchV1(
                    run_id=run.run_id,
                    window_id=window.window_id,
                    ensemble_member_id=window.ensemble_member_id,
                    symbol=symbol,
                    anchor_interval_id=interval_id,
                    left_anchor_event_id=left.event_id,
                    right_anchor_event_id=right.event_id,
                    generator_config_id=config.config_id,
                    information_mode=fit_result.information_mode,
                    session_state=session_state,
                    special_tags=tuple(special_tags),
                    event_tags=tuple(event_tags),
                    status=status,
                    events=synthetic if not upstream_closed else (),
                    event_lineage=(
                        tuple(
                            SchrodingerBridgeCandidateLineageV1(
                                event_id=event.event_id,
                                transformation_id=transformation_id,
                                generation_lineage_id=lineage_by_id[
                                    cast(str, event.reference_id)
                                ].lineage_id,
                            )
                            for event in synthetic
                        )
                        if not upstream_closed
                        else ()
                    ),
                    broker_target_id=broker_target.target_id,
                    fit_id=fit_result.fit_id,
                    dataset_id=dataset.dataset_id,
                    checkpoint_id=checkpoint.checkpoint_id,
                    generation_evidence_id=evidence.evidence_id,
                    window_context_id=context.context_id,
                )
            )
    if assigned != {item.source_event_id for item in proposals}:
        raise ValueError("bridge proposal lies outside observed anchors")
    return tuple(batches)


__all__ = [
    "SB_ARCHITECTURE",
    "SB_GENERATOR_ID",
    "SB_HYPOTHESIS_ID",
    "SB_IMPLEMENTATION_VERSION",
    "SB_PROMOTION_POLICY",
    "FittedSchrodingerBridgeBenchmarkGeneratorV1",
    "SchrodingerBridgeBrokerTargetV1",
    "SchrodingerBridgeCandidateBatchV1",
    "SchrodingerBridgeCandidateLineageV1",
    "SchrodingerBridgeCheckpointV1",
    "SchrodingerBridgeConfigV1",
    "SchrodingerBridgeDatasetManifestV1",
    "SchrodingerBridgeDatasetWindowV1",
    "SchrodingerBridgeFitError",
    "SchrodingerBridgeFitResultV1",
    "SchrodingerBridgeFitStatus",
    "SchrodingerBridgeGenerationError",
    "SchrodingerBridgeGenerationEvidenceV1",
    "SchrodingerBridgeGenerationLineageV1",
    "SchrodingerBridgeGenerationResultV1",
    "SchrodingerBridgeGenerationStatus",
    "SchrodingerBridgeProtectedWindowV1",
    "SchrodingerBridgeResourceLimitsV1",
    "SchrodingerBridgeSolverEvidenceV1",
    "SchrodingerBridgeWindowContextV1",
    "build_fitted_schrodinger_bridge_generator",
    "build_schrodinger_bridge_benchmark_candidate",
    "build_schrodinger_bridge_broker_target",
    "build_schrodinger_bridge_candidate_batches",
    "build_schrodinger_bridge_protected_window",
    "default_schrodinger_bridge_config",
    "fit_schrodinger_bridge_challenger",
]
