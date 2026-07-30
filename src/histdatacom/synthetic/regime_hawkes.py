"""Bounded two-state regime-switching marked Hawkes challengers.

The implementation is an opt-in research surface for issue #452.  It uses a
fixed-bin (MMHP-delta) approximation: the shared latent state and conditional
intensities are constant inside each synchronized bin, and observations or
proposals in a bin can affect excitation only from the next bin.  Technological
feed epochs are immutable conditioning evidence, never latent market states.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import random
import time
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from itertools import pairwise
from typing import Any, cast

from histdatacom.resource_usage import peak_rss_bytes
from histdatacom.runtime_contracts import JSONScalar, JSONValue
from histdatacom.synthetic.benchmark import (
    BENCHMARK_EVENT_SCHEMA_VERSION,
    BenchmarkCandidateKind,
    BenchmarkCandidateV1,
    BenchmarkEventV1,
    BenchmarkGeneratorV1,
    BenchmarkScenarioV1,
)
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

REGIME_HAWKES_RESOURCE_LIMITS_SCHEMA_VERSION = (
    "histdatacom.regime-hawkes-resource-limits.v1"
)
REGIME_HAWKES_CONFIG_SCHEMA_VERSION = "histdatacom.regime-hawkes-config.v1"
REGIME_HAWKES_WINDOW_CONTEXT_SCHEMA_VERSION = (
    "histdatacom.regime-hawkes-window-context.v1"
)
REGIME_HAWKES_FIT_RESULT_SCHEMA_VERSION = (
    "histdatacom.regime-hawkes-fit-result.v1"
)
REGIME_HAWKES_GENERATION_EVIDENCE_SCHEMA_VERSION = (
    "histdatacom.regime-hawkes-generation-evidence.v1"
)
REGIME_HAWKES_GENERATION_LINEAGE_SCHEMA_VERSION = (
    "histdatacom.regime-hawkes-generation-lineage.v1"
)
REGIME_HAWKES_CANDIDATE_LINEAGE_SCHEMA_VERSION = (
    "histdatacom.regime-hawkes-candidate-lineage.v1"
)
REGIME_HAWKES_CANDIDATE_BATCH_SCHEMA_VERSION = (
    "histdatacom.regime-hawkes-candidate-batch.v1"
)
REGIME_HAWKES_IMPLEMENTATION_VERSION = "1.0.0"
REGIME_HAWKES_GENERATOR_PREFIX = "histdatacom.regime-hawkes"

NANOSECONDS_PER_SECOND = 1_000_000_000
MARK_STATES = ("ask_only", "bid_only", "joint", "unchanged")
STATE_LABELS = ("calm", "active")
ASSIGNMENT_KINDS = ("epoch", "transition")
MAX_REGIME_FIT_EVENTS = 100_000
MAX_REGIME_FIT_WINDOWS = 256
MAX_REGIME_FIT_BINS = 1_000_000
MAX_REGIME_DIMENSIONS = 16
MAX_REGIME_ITERATIONS = 512
MAX_REGIME_PARAMETERS_BYTES = 8_000_000
MAX_REGIME_DIAGNOSTICS = 128
MAX_REGIME_DIAGNOSTIC_BYTES = 64 * 1024**2
MAX_REGIME_GENERATED_EVENTS = 100_000
MAX_REGIME_HISTORY_EVENTS = 100_000
MAX_REGIME_HISTORY_NS = 7 * 86_400 * NANOSECONDS_PER_SECOND


class RegimeHawkesModulation(str, Enum):
    """The two fixed nested regime-switching ablations."""

    BASELINE_ONLY = "baseline_only"
    BASELINE_AND_EXCITATION = "baseline_and_excitation"


class RegimeHawkesFitStatus(str, Enum):
    """Terminal state of one bounded fit."""

    FITTED = "fitted"
    REFUSED = "refused"
    FAILED = "failed"


class RegimeHawkesGenerationStatus(str, Enum):
    """Terminal state of one bounded synchronized generation attempt."""

    GENERATED = "generated"
    EMPTY = "empty"
    REFUSED = "refused"
    FAILED = "failed"


class RegimeHawkesFitError(ValueError):
    """Raised when a regime model cannot be used safely."""


class RegimeHawkesGenerationError(ValueError):
    """Raised when bounded regime generation must fail closed."""


@dataclass(frozen=True, slots=True)
class RegimeHawkesResourceLimitsV1:
    """Hard event, bin, state, parameter, memory, and runtime envelopes."""

    max_fit_events: int = 20_000
    max_fit_windows: int = 96
    max_fit_bins: int = 100_000
    max_generation_bins: int = 100_000
    max_iterations: int = 64
    max_dimensions: int = 8
    max_conditioning_cells: int = 64
    max_generated_events_per_bin: int = 256
    max_generated_events_per_interval: int = 1_024
    max_generated_events_per_window: int = 8_192
    max_poisson_iterations: int = 100_000
    max_poisson_iterations_per_window: int = 1_000_000
    max_history_events: int = 4_096
    max_history_ns: int = 3_600 * NANOSECONDS_PER_SECOND
    max_candidate_amplification: float = 8.0
    max_parameters_bytes: int = 4_000_000
    max_peak_memory_bytes: int = 512 * 1024**2
    max_wall_time_ms: int = 60_000
    max_diagnostics: int = 96
    max_diagnostic_bytes: int = 16 * 1024**2
    estimated_bytes_per_fit_event: int = 1_024
    estimated_bytes_per_fit_bin: int = 512
    estimated_bytes_per_generated_event: int = 2_048
    estimated_bytes_per_generation_bin: int = 512
    limits_id: str = ""
    schema_version: str = REGIME_HAWKES_RESOURCE_LIMITS_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            REGIME_HAWKES_RESOURCE_LIMITS_SCHEMA_VERSION,
            "regime Hawkes resource limits",
        )
        bounds = (
            ("max_fit_events", self.max_fit_events, 2, MAX_REGIME_FIT_EVENTS),
            (
                "max_fit_windows",
                self.max_fit_windows,
                1,
                MAX_REGIME_FIT_WINDOWS,
            ),
            ("max_fit_bins", self.max_fit_bins, 2, MAX_REGIME_FIT_BINS),
            (
                "max_generation_bins",
                self.max_generation_bins,
                1,
                MAX_REGIME_FIT_BINS,
            ),
            ("max_iterations", self.max_iterations, 2, MAX_REGIME_ITERATIONS),
            ("max_dimensions", self.max_dimensions, 1, MAX_REGIME_DIMENSIONS),
            ("max_conditioning_cells", self.max_conditioning_cells, 1, 256),
            (
                "max_generated_events_per_bin",
                self.max_generated_events_per_bin,
                1,
                MAX_REGIME_GENERATED_EVENTS,
            ),
            (
                "max_generated_events_per_interval",
                self.max_generated_events_per_interval,
                1,
                MAX_REGIME_GENERATED_EVENTS,
            ),
            (
                "max_generated_events_per_window",
                self.max_generated_events_per_window,
                1,
                MAX_REGIME_GENERATED_EVENTS,
            ),
            (
                "max_poisson_iterations",
                self.max_poisson_iterations,
                1,
                10_000_000,
            ),
            (
                "max_poisson_iterations_per_window",
                self.max_poisson_iterations_per_window,
                1,
                100_000_000,
            ),
            (
                "max_history_events",
                self.max_history_events,
                0,
                MAX_REGIME_HISTORY_EVENTS,
            ),
            ("max_history_ns", self.max_history_ns, 0, MAX_REGIME_HISTORY_NS),
            (
                "max_parameters_bytes",
                self.max_parameters_bytes,
                1_024,
                MAX_REGIME_PARAMETERS_BYTES,
            ),
            (
                "max_peak_memory_bytes",
                self.max_peak_memory_bytes,
                1,
                16 * 1024**3,
            ),
            ("max_wall_time_ms", self.max_wall_time_ms, 1, 3_600_000),
            (
                "max_diagnostics",
                self.max_diagnostics,
                1,
                MAX_REGIME_DIAGNOSTICS,
            ),
            (
                "max_diagnostic_bytes",
                self.max_diagnostic_bytes,
                1_024,
                MAX_REGIME_DIAGNOSTIC_BYTES,
            ),
            (
                "estimated_bytes_per_fit_event",
                self.estimated_bytes_per_fit_event,
                64,
                1_000_000,
            ),
            (
                "estimated_bytes_per_fit_bin",
                self.estimated_bytes_per_fit_bin,
                64,
                1_000_000,
            ),
            (
                "estimated_bytes_per_generated_event",
                self.estimated_bytes_per_generated_event,
                64,
                1_000_000,
            ),
            (
                "estimated_bytes_per_generation_bin",
                self.estimated_bytes_per_generation_bin,
                64,
                1_000_000,
            ),
        )
        for name, value, lower, upper in bounds:
            _bounded_int(value, name, lower, upper)
        if (
            self.max_generated_events_per_bin
            > self.max_generated_events_per_window
        ):
            raise ValueError("per-bin regime limit exceeds window limit")
        if (
            self.max_generated_events_per_interval
            > self.max_generated_events_per_window
        ):
            raise ValueError("per-interval regime limit exceeds window limit")
        amplification = _positive_float(
            self.max_candidate_amplification, "max_candidate_amplification"
        )
        if amplification > 1_000.0:
            raise ValueError("candidate amplification exceeds hard bound")
        expected = _stable_id(
            "regime-hawkes-resource-limits", self.identity_payload()
        )
        if self.limits_id and self.limits_id != expected:
            raise ValueError("regime Hawkes limits_id differs")
        object.__setattr__(self, "limits_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "max_fit_events": self.max_fit_events,
            "max_fit_windows": self.max_fit_windows,
            "max_fit_bins": self.max_fit_bins,
            "max_generation_bins": self.max_generation_bins,
            "max_iterations": self.max_iterations,
            "max_dimensions": self.max_dimensions,
            "max_conditioning_cells": self.max_conditioning_cells,
            "max_generated_events_per_bin": self.max_generated_events_per_bin,
            "max_generated_events_per_interval": self.max_generated_events_per_interval,
            "max_generated_events_per_window": self.max_generated_events_per_window,
            "max_poisson_iterations": self.max_poisson_iterations,
            "max_poisson_iterations_per_window": (
                self.max_poisson_iterations_per_window
            ),
            "max_history_events": self.max_history_events,
            "max_history_ns": self.max_history_ns,
            "max_candidate_amplification": self.max_candidate_amplification,
            "max_parameters_bytes": self.max_parameters_bytes,
            "max_peak_memory_bytes": self.max_peak_memory_bytes,
            "max_wall_time_ms": self.max_wall_time_ms,
            "max_diagnostics": self.max_diagnostics,
            "max_diagnostic_bytes": self.max_diagnostic_bytes,
            "estimated_bytes_per_fit_event": self.estimated_bytes_per_fit_event,
            "estimated_bytes_per_fit_bin": self.estimated_bytes_per_fit_bin,
            "estimated_bytes_per_generated_event": (
                self.estimated_bytes_per_generated_event
            ),
            "estimated_bytes_per_generation_bin": (
                self.estimated_bytes_per_generation_bin
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "limits_id": self.limits_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> RegimeHawkesResourceLimitsV1:
        _require_schema(data, REGIME_HAWKES_RESOURCE_LIMITS_SCHEMA_VERSION)
        names = (
            "max_fit_events",
            "max_fit_windows",
            "max_fit_bins",
            "max_generation_bins",
            "max_iterations",
            "max_dimensions",
            "max_conditioning_cells",
            "max_generated_events_per_bin",
            "max_generated_events_per_interval",
            "max_generated_events_per_window",
            "max_poisson_iterations",
            "max_poisson_iterations_per_window",
            "max_history_events",
            "max_history_ns",
            "max_parameters_bytes",
            "max_peak_memory_bytes",
            "max_wall_time_ms",
            "max_diagnostics",
            "max_diagnostic_bytes",
            "estimated_bytes_per_fit_event",
            "estimated_bytes_per_fit_bin",
            "estimated_bytes_per_generated_event",
            "estimated_bytes_per_generation_bin",
        )
        kwargs = {name: _strict_int(data.get(name), name) for name in names}
        return cls(
            **kwargs,
            max_candidate_amplification=_finite_float(
                data.get("max_candidate_amplification"),
                "max_candidate_amplification",
            ),
            limits_id=str(data.get("limits_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class RegimeHawkesConfigV1:
    """Versioned MMHP-delta fit, state, and generation policy."""

    modulation: RegimeHawkesModulation
    bin_width_ns: int = 500_000_000
    decay_per_second: float = 2.0
    maximum_branching_ratio: float = 0.90
    convergence_tolerance: float = 1e-4
    parameter_floor: float = 1e-8
    transition_smoothing_count: float = 1.0
    mark_smoothing_count: float = 1.0
    minimum_events_per_symbol: int = 8
    minimum_state_occupancy: float = 0.03
    minimum_activity_contrast: float = 0.01
    minimum_expected_transitions: float = 0.25
    base_seed: int = 452_000
    limits: RegimeHawkesResourceLimitsV1 = field(
        default_factory=RegimeHawkesResourceLimitsV1
    )
    config_id: str = ""
    schema_version: str = REGIME_HAWKES_CONFIG_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            REGIME_HAWKES_CONFIG_SCHEMA_VERSION,
            "regime Hawkes config",
        )
        object.__setattr__(
            self, "modulation", RegimeHawkesModulation(self.modulation)
        )
        _bounded_int(
            self.bin_width_ns,
            "bin_width_ns",
            1_000,
            86_400 * NANOSECONDS_PER_SECOND,
        )
        _positive_float(self.decay_per_second, "decay_per_second")
        ratio = _positive_float(
            self.maximum_branching_ratio, "maximum_branching_ratio"
        )
        if ratio >= 1.0:
            raise ValueError(
                "regime Hawkes branching-ratio limit must be below one"
            )
        tolerance = _positive_float(
            self.convergence_tolerance, "convergence_tolerance"
        )
        if tolerance >= 1.0:
            raise ValueError("convergence tolerance must be below one")
        floor = _positive_float(self.parameter_floor, "parameter_floor")
        if floor >= 1.0:
            raise ValueError("parameter floor must be below one")
        _positive_float(
            self.transition_smoothing_count, "transition_smoothing_count"
        )
        _positive_float(self.mark_smoothing_count, "mark_smoothing_count")
        _bounded_int(
            self.minimum_events_per_symbol,
            "minimum_events_per_symbol",
            2,
            MAX_REGIME_FIT_EVENTS,
        )
        occupancy = _finite_float(
            self.minimum_state_occupancy, "minimum_state_occupancy"
        )
        if not 0.0 < occupancy < 0.5:
            raise ValueError("minimum state occupancy must be inside (0, 0.5)")
        contrast = _finite_float(
            self.minimum_activity_contrast, "minimum_activity_contrast"
        )
        if not 0.0 < contrast < 1.0:
            raise ValueError("minimum activity contrast must be inside (0, 1)")
        _positive_float(
            self.minimum_expected_transitions, "minimum_expected_transitions"
        )
        _bounded_int(self.base_seed, "base_seed", 0, 2**63 - 1)
        if not isinstance(self.limits, RegimeHawkesResourceLimitsV1):
            raise TypeError("regime Hawkes config requires v1 resource limits")
        expected = _stable_id("regime-hawkes-config", self.identity_payload())
        if self.config_id and self.config_id != expected:
            raise ValueError("regime Hawkes config_id differs")
        object.__setattr__(self, "config_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "modulation": self.modulation.value,
            "model": "two-state-shared-chain-mmhp-delta-v1",
            "state_labels": list(STATE_LABELS),
            "state_ordering": "expected-aggregate-activity-ascending-v1",
            "bin_width_ns": self.bin_width_ns,
            "same_bin_update_policy": "affects-following-bin-only-v1",
            "decay_per_second": self.decay_per_second,
            "maximum_branching_ratio": self.maximum_branching_ratio,
            "convergence_tolerance": self.convergence_tolerance,
            "parameter_floor": self.parameter_floor,
            "transition_smoothing_count": self.transition_smoothing_count,
            "mark_policy": "quote-transition-state-conditioned-v1",
            "mark_states": list(MARK_STATES),
            "mark_smoothing_count": self.mark_smoothing_count,
            "minimum_events_per_symbol": self.minimum_events_per_symbol,
            "minimum_state_occupancy": self.minimum_state_occupancy,
            "minimum_activity_contrast": self.minimum_activity_contrast,
            "minimum_expected_transitions": self.minimum_expected_transitions,
            "inference": "scaled-forward-backward-bounded-em-v1",
            "initialization": "aggregate-count-quantile-v1",
            "fit_boundary_policy": "reset-each-calibration-window-v1",
            "ex_ante_state_policy": "filtered-probabilities-only-v1",
            "ex_post_state_policy": "smoothed-diagnostics-only-v1",
            "uncertainty_method": "posterior-effective-count-wald-95-v1",
            "generation_algorithm": "bounded-synchronized-bin-simulation-v1",
            "base_seed": self.base_seed,
            "limits": self.limits.to_dict(),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "config_id": self.config_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> RegimeHawkesConfigV1:
        _require_schema(data, REGIME_HAWKES_CONFIG_SCHEMA_VERSION)
        literals = {
            "model": "two-state-shared-chain-mmhp-delta-v1",
            "state_ordering": "expected-aggregate-activity-ascending-v1",
            "same_bin_update_policy": "affects-following-bin-only-v1",
            "mark_policy": "quote-transition-state-conditioned-v1",
            "inference": "scaled-forward-backward-bounded-em-v1",
            "initialization": "aggregate-count-quantile-v1",
            "fit_boundary_policy": "reset-each-calibration-window-v1",
            "ex_ante_state_policy": "filtered-probabilities-only-v1",
            "ex_post_state_policy": "smoothed-diagnostics-only-v1",
            "uncertainty_method": "posterior-effective-count-wald-95-v1",
            "generation_algorithm": "bounded-synchronized-bin-simulation-v1",
        }
        for key, value in literals.items():
            _require_literal(data, key, value)
        if _string_tuple(data.get("state_labels")) != STATE_LABELS:
            raise ValueError("regime Hawkes state-label registry differs")
        if _string_tuple(data.get("mark_states")) != MARK_STATES:
            raise ValueError("regime Hawkes mark-state registry differs")
        return cls(
            modulation=RegimeHawkesModulation(str(data.get("modulation", ""))),
            bin_width_ns=_strict_int(data.get("bin_width_ns"), "bin_width_ns"),
            decay_per_second=_finite_float(
                data.get("decay_per_second"), "decay_per_second"
            ),
            maximum_branching_ratio=_finite_float(
                data.get("maximum_branching_ratio"), "maximum_branching_ratio"
            ),
            convergence_tolerance=_finite_float(
                data.get("convergence_tolerance"), "convergence_tolerance"
            ),
            parameter_floor=_finite_float(
                data.get("parameter_floor"), "parameter_floor"
            ),
            transition_smoothing_count=_finite_float(
                data.get("transition_smoothing_count"),
                "transition_smoothing_count",
            ),
            mark_smoothing_count=_finite_float(
                data.get("mark_smoothing_count"), "mark_smoothing_count"
            ),
            minimum_events_per_symbol=_strict_int(
                data.get("minimum_events_per_symbol"),
                "minimum_events_per_symbol",
            ),
            minimum_state_occupancy=_finite_float(
                data.get("minimum_state_occupancy"), "minimum_state_occupancy"
            ),
            minimum_activity_contrast=_finite_float(
                data.get("minimum_activity_contrast"),
                "minimum_activity_contrast",
            ),
            minimum_expected_transitions=_finite_float(
                data.get("minimum_expected_transitions"),
                "minimum_expected_transitions",
            ),
            base_seed=_strict_int(data.get("base_seed"), "base_seed"),
            limits=RegimeHawkesResourceLimitsV1.from_dict(
                _mapping(data.get("limits"), "limits")
            ),
            config_id=str(data.get("config_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> RegimeHawkesConfigV1:
        return cls.from_dict(_mapping(json.loads(text), "regime Hawkes config"))


@dataclass(frozen=True, slots=True)
class RegimeHawkesWindowContextV1:
    """Content-bound technology/session context, separate from latent state."""

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
    use_time_ns: int | None = None
    filtered_initial_probabilities: tuple[float, float] | None = None
    context_id: str = ""
    schema_version: str = REGIME_HAWKES_WINDOW_CONTEXT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            REGIME_HAWKES_WINDOW_CONTEXT_SCHEMA_VERSION,
            "regime Hawkes window context",
        )
        for name in (
            "window_id",
            "session",
            "technology_label",
            "feed_epoch_definition_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        if "|" in self.session or "|" in self.technology_label:
            raise ValueError(
                "regime context labels contain a reserved separator"
            )
        epoch_id = _optional_text(self.epoch_id)
        boundary_id = _optional_text(self.boundary_id)
        uncertainty_start = _optional_text(self.uncertainty_start_period)
        uncertainty_end = _optional_text(self.uncertainty_end_period)
        object.__setattr__(self, "epoch_id", epoch_id)
        object.__setattr__(self, "boundary_id", boundary_id)
        object.__setattr__(self, "uncertainty_start_period", uncertainty_start)
        object.__setattr__(self, "uncertainty_end_period", uncertainty_end)
        if self.technology_assignment_kind not in ASSIGNMENT_KINDS:
            raise ValueError("unsupported technological assignment kind")
        if self.technology_assignment_kind == "epoch":
            if epoch_id is None or any(
                value is not None
                for value in (
                    boundary_id,
                    self.boundary_support,
                    uncertainty_start,
                    uncertainty_end,
                )
            ):
                raise ValueError("stable epoch context requires only epoch_id")
        else:
            if (
                epoch_id is not None
                or boundary_id is None
                or self.boundary_support is None
            ):
                raise ValueError(
                    "transition context requires boundary identity/support"
                )
            support = _finite_float(self.boundary_support, "boundary_support")
            if not 0.0 <= support <= 1.0:
                raise ValueError("boundary support must be inside [0,1]")
            if uncertainty_start is None or uncertainty_end is None:
                raise ValueError(
                    "transition context requires uncertainty periods"
                )
            if not _valid_period(uncertainty_start) or not _valid_period(
                uncertainty_end
            ):
                raise ValueError(
                    "transition uncertainty periods require YYYYMM"
                )
            if uncertainty_start > uncertainty_end:
                raise ValueError("transition uncertainty period order differs")
        observed = _optional_text(self.observed_context_id)
        object.__setattr__(self, "observed_context_id", observed)
        availability = self.observed_context_available_ns
        use_time = self.use_time_ns
        probabilities = self.filtered_initial_probabilities
        if observed is None:
            if (
                availability is not None
                or use_time is not None
                or probabilities is not None
            ):
                raise ValueError(
                    "context prior requires observed context identity"
                )
        else:
            if availability is None or use_time is None:
                raise ValueError(
                    "observed context requires availability and use time"
                )
            _strict_int(availability, "observed_context_available_ns")
            _strict_int(use_time, "use_time_ns")
            if availability > use_time:
                raise ValueError("observed context was unavailable at use time")
            if probabilities is not None:
                normalized = _probability_pair(
                    probabilities, "filtered_initial_probabilities"
                )
                object.__setattr__(
                    self, "filtered_initial_probabilities", normalized
                )
        expected = _stable_id(
            "regime-hawkes-window-context", self.identity_payload()
        )
        if self.context_id and self.context_id != expected:
            raise ValueError("regime Hawkes context_id differs")
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
            "use_time_ns": self.use_time_ns,
            "filtered_initial_probabilities": (
                list(self.filtered_initial_probabilities)
                if self.filtered_initial_probabilities is not None
                else None
            ),
            "latent_market_state_is_technology_epoch": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "context_id": self.context_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> RegimeHawkesWindowContextV1:
        _require_schema(data, REGIME_HAWKES_WINDOW_CONTEXT_SCHEMA_VERSION)
        if data.get("latent_market_state_is_technology_epoch") is not False:
            raise ValueError("regime and technology axes must remain distinct")
        raw_probabilities = data.get("filtered_initial_probabilities")
        return cls(
            window_id=str(data.get("window_id", "")),
            session=str(data.get("session", "")),
            technology_assignment_kind=str(
                data.get("technology_assignment_kind", "")
            ),
            technology_label=str(data.get("technology_label", "")),
            feed_epoch_definition_id=str(
                data.get("feed_epoch_definition_id", "")
            ),
            epoch_id=_optional_text(data.get("epoch_id")),
            boundary_id=_optional_text(data.get("boundary_id")),
            boundary_support=_optional_float(data.get("boundary_support")),
            uncertainty_start_period=_optional_text(
                data.get("uncertainty_start_period")
            ),
            uncertainty_end_period=_optional_text(
                data.get("uncertainty_end_period")
            ),
            observed_context_id=_optional_text(data.get("observed_context_id")),
            observed_context_available_ns=_optional_int(
                data.get("observed_context_available_ns")
            ),
            use_time_ns=_optional_int(data.get("use_time_ns")),
            filtered_initial_probabilities=(
                cast(tuple[float, float], tuple(_sequence(raw_probabilities)))
                if raw_probabilities is not None
                else None
            ),
            context_id=str(data.get("context_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> RegimeHawkesWindowContextV1:
        return cls.from_dict(
            _mapping(json.loads(text), "regime Hawkes context")
        )


@dataclass(frozen=True, slots=True)
class RegimeHawkesFitResultV1:
    """Content-addressed fit with separate filtered/smoothed diagnostics."""

    modulation: RegimeHawkesModulation
    config_id: str
    calibration_content_sha256: str
    calibration_context_sha256: str
    information_mode: InformationMode
    symbols: tuple[str, ...]
    status: RegimeHawkesFitStatus
    converged: bool
    iteration_count: int
    fitted_event_count: int
    fitted_window_count: int
    fitted_bin_count: int
    log_likelihood: float | None
    parameters: Mapping[str, JSONValue]
    uncertainty: Mapping[str, JSONValue]
    diagnostics: Mapping[str, JSONScalar]
    state_diagnostics: Mapping[str, JSONValue]
    estimated_peak_memory_bytes: int
    failure_reason: str | None = None
    as_of_ns: int | None = None
    fit_id: str = ""
    schema_version: str = REGIME_HAWKES_FIT_RESULT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            REGIME_HAWKES_FIT_RESULT_SCHEMA_VERSION,
            "regime Hawkes fit",
        )
        object.__setattr__(
            self, "modulation", RegimeHawkesModulation(self.modulation)
        )
        object.__setattr__(self, "config_id", _required_text(self.config_id))
        _sha256(self.calibration_content_sha256, "calibration_content_sha256")
        _sha256(self.calibration_context_sha256, "calibration_context_sha256")
        mode = InformationMode.from_value(self.information_mode)
        object.__setattr__(self, "information_mode", mode)
        symbols = tuple(sorted({_symbol(value) for value in self.symbols}))
        if not symbols:
            raise ValueError("regime Hawkes fit requires symbols")
        object.__setattr__(self, "symbols", symbols)
        status = RegimeHawkesFitStatus(self.status)
        object.__setattr__(self, "status", status)
        converged = _strict_bool(self.converged, "converged")
        object.__setattr__(self, "converged", converged)
        for name in (
            "iteration_count",
            "fitted_event_count",
            "fitted_window_count",
            "fitted_bin_count",
            "estimated_peak_memory_bytes",
        ):
            if _strict_int(getattr(self, name), name) < 0:
                raise ValueError(f"{name} must be nonnegative")
        if self.log_likelihood is not None:
            object.__setattr__(
                self,
                "log_likelihood",
                _finite_float(self.log_likelihood, "log_likelihood"),
            )
        parameters = _json_mapping(self.parameters, "parameters")
        uncertainty = _json_mapping(self.uncertainty, "uncertainty")
        diagnostics = {
            _required_text(str(key)): _json_scalar(value, str(key))
            for key, value in self.diagnostics.items()
        }
        state_diagnostics = _json_mapping(
            self.state_diagnostics, "state_diagnostics"
        )
        if len(diagnostics) > MAX_REGIME_DIAGNOSTICS:
            raise ValueError("regime Hawkes diagnostics exceed hard bound")
        if (
            len(canonical_contract_json(parameters).encode())
            > MAX_REGIME_PARAMETERS_BYTES
        ):
            raise ValueError("regime Hawkes parameters exceed hard bound")
        diagnostic_bytes = _diagnostic_payload_bytes(
            uncertainty, diagnostics, state_diagnostics
        )
        if diagnostic_bytes > MAX_REGIME_DIAGNOSTIC_BYTES:
            raise ValueError(
                "regime Hawkes diagnostic payload exceeds hard bound"
            )
        object.__setattr__(self, "parameters", parameters)
        object.__setattr__(self, "uncertainty", uncertainty)
        object.__setattr__(
            self, "diagnostics", dict(sorted(diagnostics.items()))
        )
        object.__setattr__(self, "state_diagnostics", state_diagnostics)
        failure = _optional_text(self.failure_reason)
        object.__setattr__(self, "failure_reason", failure)
        if status is RegimeHawkesFitStatus.FITTED:
            if (
                not converged
                or failure is not None
                or not parameters
                or not uncertainty
                or not state_diagnostics
            ):
                raise ValueError(
                    "fitted regime Hawkes result lacks complete model"
                )
            if diagnostics.get("diagnostic_bytes") != diagnostic_bytes:
                raise ValueError("regime Hawkes diagnostic byte count differs")
            _validate_fitted_parameters(parameters, self.modulation, symbols)
            _validate_fit_uncertainty(uncertainty, parameters, symbols)
            _validate_state_diagnostics(state_diagnostics, parameters)
        elif (
            converged
            or failure is None
            or parameters
            or uncertainty
            or state_diagnostics
        ):
            raise ValueError("closed regime Hawkes fit exposes usable state")
        if status is RegimeHawkesFitStatus.FITTED:
            if mode is InformationMode.EX_ANTE_SIMULATION:
                if self.as_of_ns is None:
                    raise ValueError(
                        "ex-ante regime Hawkes fit requires as_of_ns"
                    )
                _strict_int(self.as_of_ns, "as_of_ns")
            elif self.as_of_ns is not None:
                raise ValueError("ex-post regime Hawkes fit rejects as_of_ns")
        elif self.as_of_ns is not None:
            _strict_int(self.as_of_ns, "as_of_ns")
        expected = _stable_id("regime-hawkes-fit", self.identity_payload())
        if self.fit_id and self.fit_id != expected:
            raise ValueError("regime Hawkes fit_id differs")
        object.__setattr__(self, "fit_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "modulation": self.modulation.value,
            "config_id": self.config_id,
            "calibration_content_sha256": self.calibration_content_sha256,
            "calibration_context_sha256": self.calibration_context_sha256,
            "information_mode": self.information_mode.value,
            "as_of_ns": self.as_of_ns,
            "symbols": list(self.symbols),
            "status": self.status.value,
            "converged": self.converged,
            "iteration_count": self.iteration_count,
            "fitted_event_count": self.fitted_event_count,
            "fitted_window_count": self.fitted_window_count,
            "fitted_bin_count": self.fitted_bin_count,
            "log_likelihood": self.log_likelihood,
            "parameters": dict(self.parameters),
            "uncertainty": dict(self.uncertainty),
            "diagnostics": dict(self.diagnostics),
            "state_diagnostics": dict(self.state_diagnostics),
            "estimated_peak_memory_bytes": self.estimated_peak_memory_bytes,
            "failure_reason": self.failure_reason,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "fit_id": self.fit_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> RegimeHawkesFitResultV1:
        _require_schema(data, REGIME_HAWKES_FIT_RESULT_SCHEMA_VERSION)
        return cls(
            modulation=RegimeHawkesModulation(str(data.get("modulation", ""))),
            config_id=str(data.get("config_id", "")),
            calibration_content_sha256=str(
                data.get("calibration_content_sha256", "")
            ),
            calibration_context_sha256=str(
                data.get("calibration_context_sha256", "")
            ),
            information_mode=InformationMode.from_value(
                str(data.get("information_mode", ""))
            ),
            symbols=_string_tuple(data.get("symbols")),
            status=RegimeHawkesFitStatus(str(data.get("status", ""))),
            converged=_strict_bool(data.get("converged"), "converged"),
            iteration_count=_strict_int(
                data.get("iteration_count"), "iteration_count"
            ),
            fitted_event_count=_strict_int(
                data.get("fitted_event_count"), "fitted_event_count"
            ),
            fitted_window_count=_strict_int(
                data.get("fitted_window_count"), "fitted_window_count"
            ),
            fitted_bin_count=_strict_int(
                data.get("fitted_bin_count"), "fitted_bin_count"
            ),
            log_likelihood=_optional_float(data.get("log_likelihood")),
            parameters=cast(
                Mapping[str, JSONValue], data.get("parameters", {})
            ),
            uncertainty=cast(
                Mapping[str, JSONValue], data.get("uncertainty", {})
            ),
            diagnostics=cast(
                Mapping[str, JSONScalar], data.get("diagnostics", {})
            ),
            state_diagnostics=cast(
                Mapping[str, JSONValue], data.get("state_diagnostics", {})
            ),
            estimated_peak_memory_bytes=_strict_int(
                data.get("estimated_peak_memory_bytes"),
                "estimated_peak_memory_bytes",
            ),
            failure_reason=_optional_text(data.get("failure_reason")),
            as_of_ns=_optional_int(data.get("as_of_ns")),
            fit_id=str(data.get("fit_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> RegimeHawkesFitResultV1:
        return cls.from_dict(_mapping(json.loads(text), "regime Hawkes fit"))


def default_regime_hawkes_configs() -> tuple[RegimeHawkesConfigV1, ...]:
    """Return both fixed ablations in nested order without choosing a winner."""
    return tuple(
        RegimeHawkesConfigV1(modulation=value, base_seed=452_000 + index)
        for index, value in enumerate(RegimeHawkesModulation, start=1)
    )


def fit_regime_hawkes_challenger(
    config: RegimeHawkesConfigV1,
    calibration_windows: Sequence[EventClockCalibrationWindowV1],
    *,
    window_contexts: Sequence[RegimeHawkesWindowContextV1] = (),
    information_mode: InformationMode = InformationMode.EX_POST_RECONSTRUCTION,
    as_of_ns: int | None = None,
) -> RegimeHawkesFitResultV1:
    """Fit one bounded two-state MMHP-delta ablation on calibration only."""
    if not isinstance(config, RegimeHawkesConfigV1):
        raise TypeError("unsupported regime Hawkes configuration")
    started = time.perf_counter()
    deadline = started + config.limits.max_wall_time_ms / 1000.0
    windows = tuple(calibration_windows)
    mode = InformationMode.from_value(information_mode)
    if any(
        not isinstance(item, EventClockCalibrationWindowV1) for item in windows
    ):
        return _closed_fit_result(
            config,
            calibration_hash=hashlib.sha256(
                b"invalid-calibration-input"
            ).hexdigest(),
            context_hash=hashlib.sha256(b"[]").hexdigest(),
            mode=mode,
            as_of_ns=as_of_ns,
            symbols=("UNKNOWN",),
            status=RegimeHawkesFitStatus.REFUSED,
            event_count=0,
            window_count=len(windows),
            bin_count=0,
            estimated_memory=0,
            reason="fit input contains an invalid calibration window",
        )
    calibration_hash = _calibration_hash(windows)
    symbols = tuple(
        sorted({event.symbol for item in windows for event in item.events})
    )
    event_count = sum(len(item.events) for item in windows)
    contexts: tuple[RegimeHawkesWindowContextV1, ...] = ()
    bin_count = sum(
        max(0, math.ceil((item.end_ns - item.start_ns) / config.bin_width_ns))
        for item in windows
    )
    context_hash = hashlib.sha256(b"[]").hexdigest()
    estimated_memory = (
        event_count * config.limits.estimated_bytes_per_fit_event
        + bin_count * config.limits.estimated_bytes_per_fit_bin
    )
    try:
        contexts = _validated_contexts(windows, window_contexts)
        context_hash = _context_hash(contexts)
        reason = _fit_refusal_reason(
            config,
            windows,
            contexts,
            mode=mode,
            as_of_ns=as_of_ns,
            symbols=symbols,
            event_count=event_count,
            bin_count=bin_count,
            estimated_memory=estimated_memory,
        )
        if reason is not None:
            return _closed_fit_result(
                config,
                calibration_hash=calibration_hash,
                context_hash=context_hash,
                mode=mode,
                as_of_ns=as_of_ns,
                symbols=symbols or ("UNKNOWN",),
                status=RegimeHawkesFitStatus.REFUSED,
                event_count=event_count,
                window_count=len(windows),
                bin_count=bin_count,
                estimated_memory=estimated_memory,
                reason=reason,
            )
        context_by_id = {item.window_id: item for item in contexts}
        groups: dict[str, list[EventClockCalibrationWindowV1]] = defaultdict(
            list
        )
        for window in windows:
            context = context_by_id[window.window_id]
            groups[
                _exact_key(context.technology_label, context.session)
            ].append(window)
            groups[_session_key(context.session)].append(window)
        if len(groups) > config.limits.max_conditioning_cells:
            raise RegimeHawkesFitError("conditioning cell limit exceeded")
        models: dict[str, JSONValue] = {}
        uncertainties: dict[str, JSONValue] = {}
        state_diagnostics: dict[str, JSONValue] = {}
        total_likelihood = 0.0
        maximum_iterations = 0
        for key, grouped_windows in sorted(groups.items()):
            if not _has_symbol_support(config, grouped_windows, symbols):
                continue
            model, model_uncertainty, state_evidence = _fit_conditioning_model(
                config, grouped_windows, symbols, deadline=deadline
            )
            models[key] = cast(JSONValue, model)
            uncertainties[key] = cast(JSONValue, model_uncertainty)
            state_diagnostics[key] = cast(JSONValue, state_evidence)
            if key.startswith("exact|"):
                total_likelihood += _finite_float(
                    model["log_likelihood"], "log_likelihood"
                )
            maximum_iterations = max(
                maximum_iterations, int(model["iteration_count"])
            )
            if (
                round((time.perf_counter() - started) * 1000)
                > config.limits.max_wall_time_ms
            ):
                raise RegimeHawkesFitError("fit wall-time limit exceeded")
        if not models or not any(key.startswith("session|") for key in models):
            raise RegimeHawkesFitError(
                "no supported session-conditioned regime model"
            )
        parameters: dict[str, JSONValue] = {
            "model": "two-state-shared-chain-mmhp-delta-v1",
            "modulation": config.modulation.value,
            "state_labels": list(STATE_LABELS),
            "symbols": list(symbols),
            "bin_width_ns": config.bin_width_ns,
            "decay_per_second": config.decay_per_second,
            "same_bin_update_policy": "affects-following-bin-only-v1",
            "conditioning_policy": "exact-technology-session-then-session-v1",
            "fit_boundary_policy": "reset-each-calibration-window-v1",
            "conditioning_models": models,
        }
        fit_uncertainty_payload: dict[str, JSONValue] = {
            "method": "posterior-effective-count-wald-95-v1",
            "confidence_level": 0.95,
            "conditioning_models": uncertainties,
        }
        parameter_size = len(canonical_contract_json(parameters).encode())
        if parameter_size > config.limits.max_parameters_bytes:
            raise RegimeHawkesFitError("fit parameter payload exceeds limit")
        all_models = [
            cast(Mapping[str, Any], value) for value in models.values()
        ]
        diagnostics: dict[str, JSONScalar] = {
            "conditioning_cell_count": len(models),
            "exact_conditioning_cell_count": sum(
                key.startswith("exact|") for key in models
            ),
            "session_backoff_cell_count": sum(
                key.startswith("session|") for key in models
            ),
            "parameter_bytes": parameter_size,
            "calibration_history_reset_count": len(windows),
            "filtered_probability_policy": "available-through-current-bin-only",
            "smoothed_probability_policy": "ex-post-diagnostics-only",
            "minimum_state_occupancy": min(
                float(value["minimum_occupancy"]) for value in all_models
            ),
            "minimum_calm_state_occupancy": min(
                float(cast(Sequence[Any], value["occupancy"])[0])
                for value in all_models
            ),
            "minimum_active_state_occupancy": min(
                float(cast(Sequence[Any], value["occupancy"])[1])
                for value in all_models
            ),
            "minimum_activity_contrast": min(
                float(value["activity_contrast"]) for value in all_models
            ),
            "minimum_expected_transition_count": min(
                float(value["expected_transition_count"])
                for value in all_models
            ),
            "maximum_spectral_radius": max(
                max(float(item) for item in value["spectral_radii"])
                for value in all_models
            ),
            "stability_margin": min(
                1.0 - max(float(item) for item in value["spectral_radii"])
                for value in all_models
            ),
            "minimum_mean_dwell_bins": min(
                float(item)
                for value in all_models
                for item in cast(Sequence[Any], value["mean_dwell_bins"])
            ),
            "maximum_mean_dwell_bins": max(
                float(item)
                for value in all_models
                for item in cast(Sequence[Any], value["mean_dwell_bins"])
            ),
            "mean_posterior_entropy": sum(
                float(value["posterior_entropy_mean"]) for value in all_models
            )
            / len(all_models),
            "technology_transition_cell_count": len(
                {
                    item.technology_label
                    for item in contexts
                    if item.technology_assignment_kind == "transition"
                }
            ),
        }
        diagnostics["diagnostic_bytes"] = 0
        for _ in range(8):
            measured_diagnostic_bytes = _diagnostic_payload_bytes(
                fit_uncertainty_payload,
                diagnostics,
                state_diagnostics,
            )
            if diagnostics["diagnostic_bytes"] == measured_diagnostic_bytes:
                break
            diagnostics["diagnostic_bytes"] = measured_diagnostic_bytes
        else:
            raise RegimeHawkesFitError(
                "fit diagnostic byte accounting did not converge"
            )
        if measured_diagnostic_bytes > config.limits.max_diagnostic_bytes:
            raise RegimeHawkesFitError("fit diagnostic payload exceeds limit")
        if len(diagnostics) > config.limits.max_diagnostics:
            raise RegimeHawkesFitError("fit diagnostic count exceeds limit")
        result = RegimeHawkesFitResultV1(
            modulation=config.modulation,
            config_id=config.config_id,
            calibration_content_sha256=calibration_hash,
            calibration_context_sha256=context_hash,
            information_mode=mode,
            as_of_ns=as_of_ns,
            symbols=symbols,
            status=RegimeHawkesFitStatus.FITTED,
            converged=True,
            iteration_count=maximum_iterations,
            fitted_event_count=event_count,
            fitted_window_count=len(windows),
            fitted_bin_count=bin_count,
            log_likelihood=total_likelihood,
            parameters=parameters,
            uncertainty=fit_uncertainty_payload,
            diagnostics=diagnostics,
            state_diagnostics=state_diagnostics,
            estimated_peak_memory_bytes=estimated_memory,
        )
        _validate_fit_against_config(config, result)
        return result
    except (
        ArithmeticError,
        KeyError,
        RegimeHawkesFitError,
        TypeError,
        ValueError,
    ) as err:
        return _closed_fit_result(
            config,
            calibration_hash=calibration_hash,
            context_hash=context_hash,
            mode=mode,
            as_of_ns=as_of_ns,
            symbols=symbols or ("UNKNOWN",),
            status=RegimeHawkesFitStatus.FAILED,
            event_count=event_count,
            window_count=len(windows),
            bin_count=bin_count,
            estimated_memory=estimated_memory,
            reason=f"fit_failed:{type(err).__name__}:{err}",
        )


def _fit_conditioning_model(
    config: RegimeHawkesConfigV1,
    windows: Sequence[EventClockCalibrationWindowV1],
    symbols: tuple[str, ...],
    *,
    deadline: float,
) -> tuple[dict[str, JSONValue], dict[str, JSONValue], dict[str, JSONValue]]:
    binned = _bin_calibration_windows(
        config, windows, symbols, deadline=deadline
    )
    total_bins = sum(len(item["counts"]) for item in binned)
    if total_bins < 4:
        raise RegimeHawkesFitError("regime fit requires at least four bins")
    dt = config.bin_width_ns / NANOSECONDS_PER_SECOND
    transition = [[0.94, 0.06], [0.08, 0.92]]
    initial = [0.5, 0.5]
    scores = [
        sum(int(value) for value in counts)
        + 0.05 * sum(float(value) for value in history)
        for item in binned
        for counts, history in zip(
            cast(Sequence[Sequence[int]], item["counts"]),
            cast(Sequence[Sequence[float]], item["history"]),
        )
    ]
    threshold = _median(scores)
    hard_states = [int(score > threshold) for score in scores]
    if not any(hard_states) or all(hard_states):
        order = sorted(
            range(len(scores)), key=lambda index: (scores[index], index)
        )
        hard_states = [0] * len(scores)
        for index in order[len(order) // 2 :]:
            hard_states[index] = 1
    gamma_seed = [
        [0.9, 0.1] if state == 0 else [0.1, 0.9] for state in hard_states
    ]
    baselines, matrices, marks = _m_step_parameters(
        config, binned, symbols, gamma_seed
    )
    previous_likelihood: float | None = None
    converged = False
    filtered_by_window: list[list[list[float]]] = []
    smoothed_by_window: list[list[list[float]]] = []
    xi_totals = [[0.0, 0.0], [0.0, 0.0]]
    iteration_count = 0
    likelihood = -math.inf
    likelihood_trace: list[float] = []
    previous_parameter_snapshot: (
        tuple[
            list[list[float]],
            list[list[list[float]]],
            list[list[dict[str, float]]],
            list[list[float]],
            list[float],
        ]
        | None
    ) = None
    for iteration in range(1, config.limits.max_iterations + 1):
        if time.perf_counter() > deadline:
            raise RegimeHawkesFitError("fit wall-time limit exceeded")
        filtered_by_window = []
        smoothed_by_window = []
        xi_totals = [[0.0, 0.0], [0.0, 0.0]]
        likelihood = 0.0
        flattened_gamma: list[list[float]] = []
        for item in binned:
            if time.perf_counter() > deadline:
                raise RegimeHawkesFitError("fit wall-time limit exceeded")
            emissions = _emission_log_probabilities(
                item, baselines, matrices, marks, config, dt
            )
            filtered, smoothed, xi, window_likelihood = (
                _scaled_forward_backward(initial, transition, emissions)
            )
            filtered_by_window.append(filtered)
            smoothed_by_window.append(smoothed)
            flattened_gamma.extend(smoothed)
            likelihood += window_likelihood
            for left in range(2):
                for right in range(2):
                    xi_totals[left][right] += xi[left][right]
        iteration_count = iteration
        likelihood_trace.append(likelihood)
        if previous_likelihood is not None:
            tolerance = config.convergence_tolerance * (
                1.0 + abs(previous_likelihood)
            )
            improvement = likelihood - previous_likelihood
            if improvement < -tolerance:
                if previous_parameter_snapshot is None:
                    raise RegimeHawkesFitError(
                        "bounded EM lacks a backtracking checkpoint"
                    )
                proposed_parameter_snapshot = copy.deepcopy(
                    (baselines, matrices, marks, transition, initial)
                )
                backtracked = _backtracked_model_parameters(
                    previous_parameter_snapshot,
                    proposed_parameter_snapshot,
                    binned,
                    config,
                    dt,
                    minimum_likelihood=previous_likelihood,
                    deadline=deadline,
                )
                likelihood_trace.pop()
                if backtracked is None:
                    (
                        baselines,
                        matrices,
                        marks,
                        transition,
                        initial,
                    ) = copy.deepcopy(previous_parameter_snapshot)
                    likelihood = previous_likelihood
                    converged = True
                    break
                baselines, matrices, marks, transition, initial = backtracked
                # Re-enter the E-step with the accepted interpolation.  The
                # rejected proposal is deliberately absent from the trace and
                # can never influence the next M-step posteriors.
                continue
            if improvement < 0.0:
                if previous_parameter_snapshot is None:
                    raise RegimeHawkesFitError(
                        "bounded EM lacks a rollback checkpoint"
                    )
                (
                    baselines,
                    matrices,
                    marks,
                    transition,
                    initial,
                ) = copy.deepcopy(previous_parameter_snapshot)
                likelihood_trace.pop()
                likelihood = previous_likelihood
                iteration_count -= 1
                converged = True
                break
            if abs(improvement) <= tolerance:
                converged = True
                break
        previous_likelihood = likelihood
        previous_parameter_snapshot = copy.deepcopy(
            (baselines, matrices, marks, transition, initial)
        )
        smoothing = config.transition_smoothing_count
        for left in range(2):
            denominator = sum(xi_totals[left]) + 2.0 * smoothing
            transition[left] = [
                (xi_totals[left][right] + smoothing) / denominator
                for right in range(2)
            ]
        initial = _normalized_pair(
            [
                sum(values[0][state] for values in smoothed_by_window)
                for state in range(2)
            ]
        )
        baselines, matrices, marks = _m_step_parameters(
            config, binned, symbols, flattened_gamma
        )
        baselines, matrices, marks, transition, initial, flattened_gamma = (
            _canonicalize_states(
                baselines,
                matrices,
                marks,
                transition,
                initial,
                flattened_gamma,
                decay_per_second=config.decay_per_second,
            )
        )
    iteration_count = len(likelihood_trace)
    if not converged:
        raise RegimeHawkesFitError("bounded EM reached its iteration limit")
    # Recompute after canonicalization so stored filtered/smoothed labels agree.
    filtered_by_window = []
    smoothed_by_window = []
    xi_totals = [[0.0, 0.0], [0.0, 0.0]]
    likelihood = 0.0
    for item in binned:
        emissions = _emission_log_probabilities(
            item, baselines, matrices, marks, config, dt
        )
        filtered, smoothed, xi, window_likelihood = _scaled_forward_backward(
            initial, transition, emissions
        )
        filtered_by_window.append(filtered)
        smoothed_by_window.append(smoothed)
        likelihood += window_likelihood
        for left in range(2):
            for right in range(2):
                xi_totals[left][right] += xi[left][right]
    all_gamma = [row for window in smoothed_by_window for row in window]
    occupancy = [
        sum(row[state] for row in all_gamma) / len(all_gamma)
        for state in range(2)
    ]
    activity = [
        sum(baselines[state])
        + config.decay_per_second * sum(sum(row) for row in matrices[state])
        for state in range(2)
    ]
    contrast = (activity[1] - activity[0]) / max(
        activity[1], config.parameter_floor
    )
    expected_transitions = xi_totals[0][1] + xi_totals[1][0]
    if min(occupancy) < config.minimum_state_occupancy:
        raise RegimeHawkesFitError("low latent-state occupancy")
    if contrast < config.minimum_activity_contrast:
        raise RegimeHawkesFitError("collapsed latent-state activity contrast")
    if expected_transitions < config.minimum_expected_transitions:
        raise RegimeHawkesFitError(
            "unsupported latent-state transition estimate"
        )
    radii = [_spectral_radius(matrix) for matrix in matrices]
    if any(
        not 0.0 <= value < config.maximum_branching_ratio for value in radii
    ):
        raise RegimeHawkesFitError("unstable state-specific excitation")
    entropy_values = [
        -sum(
            probability * math.log(max(probability, config.parameter_floor))
            for probability in row
        )
        for row in all_gamma
    ]
    dwell_bins = _posterior_dwell_bins(smoothed_by_window)
    model: dict[str, JSONValue] = {
        "transition_matrix": cast(JSONValue, transition),
        "initial_probabilities": cast(JSONValue, initial),
        "baseline_rates_per_second": cast(JSONValue, baselines),
        "excitation_matrices": cast(JSONValue, matrices),
        "mark_probabilities": cast(JSONValue, marks),
        "spectral_radii": cast(JSONValue, radii),
        "occupancy": cast(JSONValue, occupancy),
        "activity_levels": cast(JSONValue, activity),
        "activity_contrast": contrast,
        "expected_transition_count": expected_transitions,
        "transition_counts": cast(JSONValue, xi_totals),
        "mean_dwell_bins": cast(JSONValue, dwell_bins),
        "posterior_entropy_mean": sum(entropy_values) / len(entropy_values),
        "log_likelihood": likelihood,
        "log_likelihood_trace": cast(JSONValue, likelihood_trace),
        "iteration_count": iteration_count,
        "minimum_occupancy": min(occupancy),
    }
    uncertainty = _fit_uncertainty(
        baselines, matrices, occupancy, total_bins, dt
    )
    state_evidence: dict[str, JSONValue] = {
        "probability_semantics": {
            "filtered": "P(z_b|observations through bin b)",
            "smoothed": "P(z_b|complete calibration window); ex-post only",
        },
        "windows": [
            {
                "window_id": str(item["window_id"]),
                "bin_count": len(cast(Sequence[Any], item["counts"])),
                "filtered_probabilities": cast(JSONValue, filtered),
                "smoothed_probabilities": cast(JSONValue, smoothed),
            }
            for item, filtered, smoothed in zip(
                binned, filtered_by_window, smoothed_by_window
            )
        ],
    }
    return model, uncertainty, state_evidence


def _bin_calibration_windows(
    config: RegimeHawkesConfigV1,
    windows: Sequence[EventClockCalibrationWindowV1],
    symbols: tuple[str, ...],
    *,
    deadline: float,
) -> list[dict[str, Any]]:
    symbol_index = {symbol: index for index, symbol in enumerate(symbols)}
    result: list[dict[str, Any]] = []
    decay = math.exp(
        -config.decay_per_second * config.bin_width_ns / NANOSECONDS_PER_SECOND
    )
    for window in windows:
        if time.perf_counter() > deadline:
            raise RegimeHawkesFitError("fit wall-time limit exceeded")
        bin_count = math.ceil(
            (window.end_ns - window.start_ns) / config.bin_width_ns
        )
        counts = [[0 for _ in symbols] for _ in range(bin_count)]
        mark_counts = [
            [[0 for _ in MARK_STATES] for _ in symbols]
            for _ in range(bin_count)
        ]
        prior_quotes: dict[str, tuple[float, float]] = {}
        for event in window.events:
            if time.perf_counter() > deadline:
                raise RegimeHawkesFitError("fit wall-time limit exceeded")
            index = min(
                bin_count - 1,
                (event.event_time_ns - window.start_ns) // config.bin_width_ns,
            )
            destination = symbol_index[event.symbol]
            counts[index][destination] += 1
            mark = _event_mark(event, prior_quotes.get(event.symbol))
            mark_counts[index][destination][MARK_STATES.index(mark)] += 1
            prior_quotes[event.symbol] = (event.bid, event.ask)
        history: list[list[float]] = []
        recursion = [0.0 for _ in symbols]
        for row in counts:
            if time.perf_counter() > deadline:
                raise RegimeHawkesFitError("fit wall-time limit exceeded")
            history.append(list(recursion))
            recursion = [
                decay * recursion[index] + row[index]
                for index in range(len(symbols))
            ]
        result.append(
            {
                "window_id": window.window_id,
                "counts": counts,
                "mark_counts": mark_counts,
                "history": history,
            }
        )
    return result


def _m_step_parameters(
    config: RegimeHawkesConfigV1,
    binned: Sequence[Mapping[str, Any]],
    symbols: tuple[str, ...],
    gamma: Sequence[Sequence[float]],
) -> tuple[
    list[list[float]], list[list[list[float]]], list[list[dict[str, float]]]
]:
    counts = [
        row
        for item in binned
        for row in cast(Sequence[Sequence[int]], item["counts"])
    ]
    histories = [
        row
        for item in binned
        for row in cast(Sequence[Sequence[float]], item["history"])
    ]
    mark_counts = [
        row
        for item in binned
        for row in cast(Sequence[Sequence[Sequence[int]]], item["mark_counts"])
    ]
    if len(gamma) != len(counts):
        raise RegimeHawkesFitError("posterior/bin cardinality differs")
    dt = config.bin_width_ns / NANOSECONDS_PER_SECOND
    dimension = len(symbols)
    baselines = [[config.parameter_floor] * dimension for _ in range(2)]
    matrices = [[[0.0] * dimension for _ in range(dimension)] for _ in range(2)]
    marks: list[list[dict[str, float]]] = [
        [{} for _ in symbols] for _ in range(2)
    ]
    for state in range(2):
        weight_sum = sum(row[state] for row in gamma)
        if weight_sum <= config.parameter_floor:
            raise RegimeHawkesFitError(
                "latent state has zero effective support"
            )
        for destination in range(dimension):
            mean_rate = sum(
                gamma[index][state] * counts[index][destination]
                for index in range(len(counts))
            ) / (weight_sum * dt)
            baselines[state][destination] = max(
                config.parameter_floor, mean_rate * 0.88
            )
            mark_values = {
                mark: config.mark_smoothing_count
                + sum(
                    gamma[index][state]
                    * mark_counts[index][destination][mark_index]
                    for index in range(len(counts))
                )
                for mark_index, mark in enumerate(MARK_STATES)
            }
            total_marks = sum(mark_values.values())
            marks[state][destination] = {
                key: value / total_marks for key, value in mark_values.items()
            }
            for source in range(dimension):
                weighted_x = (
                    sum(
                        gamma[index][state] * histories[index][source]
                        for index in range(len(counts))
                    )
                    / weight_sum
                )
                weighted_y = (
                    sum(
                        gamma[index][state] * counts[index][destination]
                        for index in range(len(counts))
                    )
                    / weight_sum
                )
                covariance = (
                    sum(
                        gamma[index][state]
                        * (histories[index][source] - weighted_x)
                        * (counts[index][destination] - weighted_y)
                        for index in range(len(counts))
                    )
                    / weight_sum
                )
                variance = (
                    sum(
                        gamma[index][state]
                        * (histories[index][source] - weighted_x) ** 2
                        for index in range(len(counts))
                    )
                    / weight_sum
                )
                coefficient = max(
                    0.0, covariance / max(variance, config.parameter_floor)
                )
                matrices[state][destination][source] = min(
                    0.05,
                    coefficient / max(config.decay_per_second * dt, 1.0),
                )
    if config.modulation is RegimeHawkesModulation.BASELINE_ONLY:
        shared = [
            [
                0.5 * (matrices[0][row][column] + matrices[1][row][column])
                for column in range(dimension)
            ]
            for row in range(dimension)
        ]
        matrices = [
            [list(row) for row in shared],
            [list(row) for row in shared],
        ]
    matrices = [
        _stabilized_matrix(matrix, config.maximum_branching_ratio)
        for matrix in matrices
    ]
    return baselines, matrices, marks


def _emission_log_probabilities(
    item: Mapping[str, Any],
    baselines: Sequence[Sequence[float]],
    matrices: Sequence[Sequence[Sequence[float]]],
    marks: Sequence[Sequence[Mapping[str, float]]],
    config: RegimeHawkesConfigV1,
    dt: float,
) -> list[list[float]]:
    result: list[list[float]] = []
    counts = cast(Sequence[Sequence[int]], item["counts"])
    histories = cast(Sequence[Sequence[float]], item["history"])
    mark_counts = cast(Sequence[Sequence[Sequence[int]]], item["mark_counts"])
    for bin_index, row in enumerate(counts):
        values: list[float] = []
        for state in range(2):
            log_probability = 0.0
            for destination, count in enumerate(row):
                intensity = baselines[state][
                    destination
                ] + config.decay_per_second * sum(
                    matrices[state][destination][source]
                    * histories[bin_index][source]
                    for source in range(len(row))
                )
                mean = max(config.parameter_floor, intensity * dt)
                log_probability += (
                    count * math.log(mean) - mean - math.lgamma(count + 1.0)
                )
                for mark_index, mark in enumerate(MARK_STATES):
                    log_probability += mark_counts[bin_index][destination][
                        mark_index
                    ] * math.log(
                        max(
                            config.parameter_floor,
                            marks[state][destination][mark],
                        )
                    )
            values.append(log_probability)
        result.append(values)
    return result


def _scaled_forward_backward(
    initial: Sequence[float],
    transition: Sequence[Sequence[float]],
    log_emissions: Sequence[Sequence[float]],
) -> tuple[list[list[float]], list[list[float]], list[list[float]], float]:
    if not log_emissions:
        raise RegimeHawkesFitError("forward-backward requires bins")
    filtered: list[list[float]] = []
    scales: list[float] = []
    emission_weights: list[list[float]] = []
    offsets: list[float] = []
    for values in log_emissions:
        offset = max(values)
        offsets.append(offset)
        emission_weights.append([math.exp(value - offset) for value in values])
    first_raw = [
        initial[state] * emission_weights[0][state] for state in range(2)
    ]
    first_scale = sum(first_raw)
    if first_scale <= 0.0 or not math.isfinite(first_scale):
        raise RegimeHawkesFitError("forward probability collapsed")
    filtered.append([value / first_scale for value in first_raw])
    scales.append(first_scale)
    for index in range(1, len(log_emissions)):
        raw = [
            emission_weights[index][state]
            * sum(
                filtered[index - 1][prior] * transition[prior][state]
                for prior in range(2)
            )
            for state in range(2)
        ]
        scale = sum(raw)
        if scale <= 0.0 or not math.isfinite(scale):
            raise RegimeHawkesFitError("forward probability collapsed")
        filtered.append([value / scale for value in raw])
        scales.append(scale)
    backward = [[1.0, 1.0] for _ in log_emissions]
    for index in range(len(log_emissions) - 2, -1, -1):
        backward[index] = [
            sum(
                transition[state][following]
                * emission_weights[index + 1][following]
                * backward[index + 1][following]
                for following in range(2)
            )
            / scales[index + 1]
            for state in range(2)
        ]
    smoothed = [
        _normalized_pair(
            [
                filtered[index][state] * backward[index][state]
                for state in range(2)
            ]
        )
        for index in range(len(filtered))
    ]
    xi_totals = [[0.0, 0.0], [0.0, 0.0]]
    for index in range(len(filtered) - 1):
        xi_values = [
            [
                filtered[index][left]
                * transition[left][right]
                * emission_weights[index + 1][right]
                * backward[index + 1][right]
                for right in range(2)
            ]
            for left in range(2)
        ]
        denominator = sum(sum(row) for row in xi_values)
        if denominator <= 0.0:
            raise RegimeHawkesFitError("transition posterior collapsed")
        for left in range(2):
            for right in range(2):
                xi_totals[left][right] += xi_values[left][right] / denominator
    likelihood = sum(
        math.log(scale) + offset for scale, offset in zip(scales, offsets)
    )
    return filtered, smoothed, xi_totals, likelihood


def _model_log_likelihood(
    binned: Sequence[Mapping[str, Any]],
    baselines: Sequence[Sequence[float]],
    matrices: Sequence[Sequence[Sequence[float]]],
    marks: Sequence[Sequence[Mapping[str, float]]],
    transition: Sequence[Sequence[float]],
    initial: Sequence[float],
    config: RegimeHawkesConfigV1,
    dt: float,
    *,
    deadline: float,
) -> float:
    likelihood = 0.0
    for item in binned:
        if time.perf_counter() > deadline:
            raise RegimeHawkesFitError("fit wall-time limit exceeded")
        emissions = _emission_log_probabilities(
            item, baselines, matrices, marks, config, dt
        )
        _, _, _, window_likelihood = _scaled_forward_backward(
            initial, transition, emissions
        )
        likelihood += window_likelihood
    if not math.isfinite(likelihood):
        raise RegimeHawkesFitError("backtracked likelihood is non-finite")
    return likelihood


def _blend_model_parameters(
    previous: tuple[
        list[list[float]],
        list[list[list[float]]],
        list[list[dict[str, float]]],
        list[list[float]],
        list[float],
    ],
    proposed: tuple[
        list[list[float]],
        list[list[list[float]]],
        list[list[dict[str, float]]],
        list[list[float]],
        list[float],
    ],
    weight: float,
    config: RegimeHawkesConfigV1,
) -> tuple[
    list[list[float]],
    list[list[list[float]]],
    list[list[dict[str, float]]],
    list[list[float]],
    list[float],
]:
    if not 0.0 < weight < 1.0:
        raise ValueError(
            "backtracking weight must be strictly between zero and one"
        )
    old_baselines, old_matrices, old_marks, old_transition, old_initial = (
        previous
    )
    new_baselines, new_matrices, new_marks, new_transition, new_initial = (
        proposed
    )
    baselines = [
        [
            (1.0 - weight) * old_value + weight * new_value
            for old_value, new_value in zip(old_row, new_row)
        ]
        for old_row, new_row in zip(old_baselines, new_baselines)
    ]
    matrices = [
        _stabilized_matrix(
            [
                [
                    (1.0 - weight) * old_value + weight * new_value
                    for old_value, new_value in zip(old_row, new_row)
                ]
                for old_row, new_row in zip(old_state, new_state)
            ],
            config.maximum_branching_ratio,
        )
        for old_state, new_state in zip(old_matrices, new_matrices)
    ]
    marks = [
        [
            {
                mark: (1.0 - weight) * old_values[mark]
                + weight * new_values[mark]
                for mark in MARK_STATES
            }
            for old_values, new_values in zip(old_state, new_state)
        ]
        for old_state, new_state in zip(old_marks, new_marks)
    ]
    transition = [
        _normalized_pair(
            [
                (1.0 - weight) * old_value + weight * new_value
                for old_value, new_value in zip(old_row, new_row)
            ]
        )
        for old_row, new_row in zip(old_transition, new_transition)
    ]
    initial = _normalized_pair(
        [
            (1.0 - weight) * old_value + weight * new_value
            for old_value, new_value in zip(old_initial, new_initial)
        ]
    )
    return baselines, matrices, marks, transition, initial


def _backtracked_model_parameters(
    previous: tuple[
        list[list[float]],
        list[list[list[float]]],
        list[list[dict[str, float]]],
        list[list[float]],
        list[float],
    ],
    proposed: tuple[
        list[list[float]],
        list[list[list[float]]],
        list[list[dict[str, float]]],
        list[list[float]],
        list[float],
    ],
    binned: Sequence[Mapping[str, Any]],
    config: RegimeHawkesConfigV1,
    dt: float,
    *,
    minimum_likelihood: float,
    deadline: float,
) -> (
    tuple[
        list[list[float]],
        list[list[list[float]]],
        list[list[dict[str, float]]],
        list[list[float]],
        list[float],
    ]
    | None
):
    # Eight deterministic halvings bound the extra work while preserving a
    # true generalized-EM invariant: only a non-decreasing interpolation can
    # become an accepted checkpoint.
    for exponent in range(1, 9):
        candidate = _blend_model_parameters(
            previous, proposed, 0.5**exponent, config
        )
        likelihood = _model_log_likelihood(
            binned,
            *candidate[:3],
            candidate[3],
            candidate[4],
            config,
            dt,
            deadline=deadline,
        )
        if likelihood >= minimum_likelihood:
            return candidate
    return None


def _canonicalize_states(
    baselines: list[list[float]],
    matrices: list[list[list[float]]],
    marks: list[list[dict[str, float]]],
    transition: list[list[float]],
    initial: list[float],
    gamma: list[list[float]],
    *,
    decay_per_second: float,
) -> tuple[
    list[list[float]],
    list[list[list[float]]],
    list[list[dict[str, float]]],
    list[list[float]],
    list[float],
    list[list[float]],
]:
    activity = [
        sum(baselines[state])
        + decay_per_second * sum(sum(row) for row in matrices[state])
        for state in range(2)
    ]
    if activity[0] <= activity[1]:
        return baselines, matrices, marks, transition, initial, gamma
    return (
        [baselines[1], baselines[0]],
        [matrices[1], matrices[0]],
        [marks[1], marks[0]],
        [
            [transition[1][1], transition[1][0]],
            [transition[0][1], transition[0][0]],
        ],
        [initial[1], initial[0]],
        [[row[1], row[0]] for row in gamma],
    )


def _fit_uncertainty(
    baselines: Sequence[Sequence[float]],
    matrices: Sequence[Sequence[Sequence[float]]],
    occupancy: Sequence[float],
    bin_count: int,
    dt: float,
) -> dict[str, JSONValue]:
    baseline_intervals: list[list[list[float]]] = []
    matrix_intervals: list[list[list[list[float]]]] = []
    for state in range(2):
        effective = max(1.0, occupancy[state] * bin_count)
        baseline_intervals.append(
            [
                [
                    max(
                        0.0,
                        value
                        - 1.96
                        * math.sqrt(max(value, 1e-12) / (effective * dt)),
                    ),
                    value
                    + 1.96 * math.sqrt(max(value, 1e-12) / (effective * dt)),
                ]
                for value in baselines[state]
            ]
        )
        matrix_intervals.append(
            [
                [
                    [
                        max(
                            0.0,
                            value
                            - 1.96 * math.sqrt(max(value, 1e-12) / effective),
                        ),
                        value + 1.96 * math.sqrt(max(value, 1e-12) / effective),
                    ]
                    for value in row
                ]
                for row in matrices[state]
            ]
        )
    return {
        "baseline_rate_intervals": cast(JSONValue, baseline_intervals),
        "excitation_intervals": cast(JSONValue, matrix_intervals),
        "effective_state_bins": cast(
            JSONValue, [value * bin_count for value in occupancy]
        ),
    }


def _posterior_dwell_bins(
    windows: Sequence[Sequence[Sequence[float]]],
) -> list[float]:
    runs: list[list[int]] = [[], []]
    for values in windows:
        path = [int(row[1] > row[0]) for row in values]
        if not path:
            continue
        current = path[0]
        length = 1
        for value in path[1:]:
            if value == current:
                length += 1
            else:
                runs[current].append(length)
                current = value
                length = 1
        runs[current].append(length)
    return [sum(values) / len(values) if values else 0.0 for values in runs]


def _stabilized_matrix(
    matrix: Sequence[Sequence[float]], maximum: float
) -> list[list[float]]:
    values = [
        [max(0.0, _finite_float(value, "excitation")) for value in row]
        for row in matrix
    ]
    radius = _spectral_radius(values)
    target = maximum * 0.95
    if radius >= target and radius > 0.0:
        scale = target / radius
        values = [[value * scale for value in row] for row in values]
    return values


def _spectral_radius(matrix: Sequence[Sequence[float]]) -> float:
    dimension = len(matrix)
    if dimension == 0 or any(len(row) != dimension for row in matrix):
        raise ValueError("excitation matrix must be square")
    values = [
        [_finite_float(value, "excitation") for value in row] for row in matrix
    ]
    if any(value < 0.0 for row in values for value in row):
        raise ValueError("excitation matrix values must be non-negative")

    # Reduce first to irreducible diagonal blocks.  Besides making the power
    # calculation faster, this handles reducible and nilpotent matrices exactly
    # instead of allowing a transient path to masquerade as a non-zero radius.
    index = 0
    indices = [-1] * dimension
    lowlinks = [0] * dimension
    stack: list[int] = []
    on_stack = [False] * dimension
    components: list[list[int]] = []

    def visit(node: int) -> None:
        nonlocal index
        indices[node] = index
        lowlinks[node] = index
        index += 1
        stack.append(node)
        on_stack[node] = True
        for neighbour, value in enumerate(values[node]):
            if value <= 0.0:
                continue
            if indices[neighbour] < 0:
                visit(neighbour)
                lowlinks[node] = min(lowlinks[node], lowlinks[neighbour])
            elif on_stack[neighbour]:
                lowlinks[node] = min(lowlinks[node], indices[neighbour])
        if lowlinks[node] != indices[node]:
            return
        component: list[int] = []
        while True:
            member = stack.pop()
            on_stack[member] = False
            component.append(member)
            if member == node:
                break
        components.append(component)

    for node in range(dimension):
        if indices[node] < 0:
            visit(node)

    def block_radius(component: Sequence[int]) -> float:
        if len(component) == 1:
            member = component[0]
            return values[member][member]
        block = [
            [values[row][column] for column in component] for row in component
        ]
        size = len(block)
        vector = [1.0 / size] * size
        estimate = 0.0
        # A + I is primitive for an irreducible non-negative A, so this avoids
        # the two-cycle oscillation of ordinary power iteration on periodic
        # excitation matrices.  Collatz bounds then estimate rho(A) directly.
        for _ in range(4096):
            shifted_product = [
                vector[row]
                + sum(
                    block[row][column] * vector[column]
                    for column in range(size)
                )
                for row in range(size)
            ]
            norm = sum(shifted_product)
            next_vector = [value / norm for value in shifted_product]
            product = [
                sum(
                    block[row][column] * next_vector[column]
                    for column in range(size)
                )
                for row in range(size)
            ]
            ratios = [product[row] / next_vector[row] for row in range(size)]
            lower = min(ratios)
            upper = max(ratios)
            estimate = (lower + upper) / 2.0
            vector = next_vector
            if upper - lower <= 1e-12 * (1.0 + upper):
                break
        return estimate

    return max(block_radius(component) for component in components)


@dataclass(frozen=True, slots=True)
class RegimeHawkesGenerationLineageV1:
    """Auditable latent-state, intensity, mark, and source evidence."""

    source_event_id: str
    destination_symbol: str
    bin_start_ns: int
    state_label: str
    filtered_state_probability: float
    conditional_intensity: float
    excitation_source_symbol: str | None
    excitation_source_contribution: float
    state_transitioned: bool
    event_state: str
    lineage_id: str = ""
    schema_version: str = REGIME_HAWKES_GENERATION_LINEAGE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            REGIME_HAWKES_GENERATION_LINEAGE_SCHEMA_VERSION,
            "regime Hawkes generation lineage",
        )
        object.__setattr__(
            self, "source_event_id", _required_text(self.source_event_id)
        )
        object.__setattr__(
            self, "destination_symbol", _symbol(self.destination_symbol)
        )
        _strict_int(self.bin_start_ns, "bin_start_ns")
        if self.state_label not in STATE_LABELS:
            raise ValueError("unsupported regime state label")
        probability = _finite_float(
            self.filtered_state_probability, "filtered_state_probability"
        )
        if not 0.0 <= probability <= 1.0:
            raise ValueError("filtered state probability must be inside [0,1]")
        intensity = _positive_float(
            self.conditional_intensity, "conditional_intensity"
        )
        if self.excitation_source_symbol is not None:
            object.__setattr__(
                self,
                "excitation_source_symbol",
                _symbol(self.excitation_source_symbol),
            )
        contribution = _finite_float(
            self.excitation_source_contribution,
            "excitation_source_contribution",
        )
        if contribution < 0.0:
            raise ValueError(
                "excitation source contribution must be nonnegative"
            )
        if self.excitation_source_symbol is None and contribution != 0.0:
            raise ValueError("source-free excitation contribution must be zero")
        if self.excitation_source_symbol is not None and contribution <= 0.0:
            raise ValueError(
                "named excitation source must contribute positively"
            )
        if contribution > intensity:
            raise ValueError("excitation source contribution exceeds intensity")
        object.__setattr__(
            self,
            "state_transitioned",
            _strict_bool(self.state_transitioned, "state_transitioned"),
        )
        if self.event_state not in MARK_STATES:
            raise ValueError("unsupported regime Hawkes mark")
        expected = _stable_id(
            "regime-hawkes-generation-lineage", self.identity_payload()
        )
        if self.lineage_id and self.lineage_id != expected:
            raise ValueError("regime Hawkes generation lineage_id differs")
        object.__setattr__(self, "lineage_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "source_event_id": self.source_event_id,
            "destination_symbol": self.destination_symbol,
            "bin_start_ns": self.bin_start_ns,
            "state_label": self.state_label,
            "filtered_state_probability": self.filtered_state_probability,
            "conditional_intensity": self.conditional_intensity,
            "excitation_source_symbol": self.excitation_source_symbol,
            "excitation_source_contribution": (
                self.excitation_source_contribution
            ),
            "state_transitioned": self.state_transitioned,
            "event_state": self.event_state,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "lineage_id": self.lineage_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> RegimeHawkesGenerationLineageV1:
        _require_schema(data, REGIME_HAWKES_GENERATION_LINEAGE_SCHEMA_VERSION)
        return cls(
            source_event_id=str(data.get("source_event_id", "")),
            destination_symbol=str(data.get("destination_symbol", "")),
            bin_start_ns=_strict_int(data.get("bin_start_ns"), "bin_start_ns"),
            state_label=str(data.get("state_label", "")),
            filtered_state_probability=_finite_float(
                data.get("filtered_state_probability"),
                "filtered_state_probability",
            ),
            conditional_intensity=_finite_float(
                data.get("conditional_intensity"), "conditional_intensity"
            ),
            excitation_source_symbol=_optional_text(
                data.get("excitation_source_symbol")
            ),
            excitation_source_contribution=_finite_float(
                data.get("excitation_source_contribution"),
                "excitation_source_contribution",
            ),
            state_transitioned=_strict_bool(
                data.get("state_transitioned"), "state_transitioned"
            ),
            event_state=str(data.get("event_state", "")),
            lineage_id=str(data.get("lineage_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class RegimeHawkesGenerationEvidenceV1:
    """Complete measured evidence for one all-or-nothing generation attempt."""

    fit_id: str
    window_id: str
    window_context_id: str | None
    ensemble_member_id: str
    status: RegimeHawkesGenerationStatus
    attempted: bool
    input_event_count: int
    history_event_count: int
    generated_event_count: int
    processed_bin_count: int
    poisson_iteration_count: int
    state_bin_counts: tuple[int, int]
    state_transition_count: int
    initial_state_policy: str
    final_filtered_probabilities: tuple[float, float] | None
    input_anchor_sha256: str | None
    input_event_content_sha256: str | None
    history_content_sha256: str | None
    window_context_sha256: str | None
    conditioning_support_level: str
    conditioning_model_key: str | None
    maximum_spectral_radius: float | None
    lineage_content_sha256: str | None
    wall_time_ms: int
    peak_memory_bytes: int
    failure_reason: str | None = None
    evidence_id: str = ""
    schema_version: str = REGIME_HAWKES_GENERATION_EVIDENCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            REGIME_HAWKES_GENERATION_EVIDENCE_SCHEMA_VERSION,
            "regime Hawkes generation evidence",
        )
        for name in (
            "fit_id",
            "window_id",
            "ensemble_member_id",
            "initial_state_policy",
            "conditioning_support_level",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self, "window_context_id", _optional_text(self.window_context_id)
        )
        status = RegimeHawkesGenerationStatus(self.status)
        object.__setattr__(self, "status", status)
        object.__setattr__(
            self, "attempted", _strict_bool(self.attempted, "attempted")
        )
        if not self.attempted:
            raise ValueError(
                "regime generation evidence must represent an attempt"
            )
        for name in (
            "input_event_count",
            "history_event_count",
            "generated_event_count",
            "processed_bin_count",
            "poisson_iteration_count",
            "state_transition_count",
            "wall_time_ms",
            "peak_memory_bytes",
        ):
            if _strict_int(getattr(self, name), name) < 0:
                raise ValueError(f"{name} must be nonnegative")
        state_counts = tuple(
            _strict_int(value, "state_bin_count")
            for value in self.state_bin_counts
        )
        if len(state_counts) != 2 or any(value < 0 for value in state_counts):
            raise ValueError("state bin counts require two nonnegative values")
        object.__setattr__(self, "state_bin_counts", state_counts)
        probabilities = self.final_filtered_probabilities
        if probabilities is not None:
            object.__setattr__(
                self,
                "final_filtered_probabilities",
                _probability_pair(
                    probabilities, "final_filtered_probabilities"
                ),
            )
        anchor_hash = _optional_sha256(
            self.input_anchor_sha256, "input_anchor_sha256"
        )
        input_content_hash = _optional_sha256(
            self.input_event_content_sha256,
            "input_event_content_sha256",
        )
        history_hash = _optional_sha256(
            self.history_content_sha256, "history_content_sha256"
        )
        context_hash = _optional_sha256(
            self.window_context_sha256, "window_context_sha256"
        )
        lineage_hash = _optional_sha256(
            self.lineage_content_sha256, "lineage_content_sha256"
        )
        object.__setattr__(self, "input_anchor_sha256", anchor_hash)
        object.__setattr__(
            self, "input_event_content_sha256", input_content_hash
        )
        object.__setattr__(self, "history_content_sha256", history_hash)
        object.__setattr__(self, "window_context_sha256", context_hash)
        object.__setattr__(self, "lineage_content_sha256", lineage_hash)
        object.__setattr__(
            self,
            "conditioning_model_key",
            _optional_text(self.conditioning_model_key),
        )
        radius = self.maximum_spectral_radius
        if radius is not None:
            radius = _finite_float(radius, "maximum_spectral_radius")
            if not 0.0 <= radius < 1.0:
                raise ValueError("generation evidence contains unstable radius")
            object.__setattr__(self, "maximum_spectral_radius", radius)
        failure = _optional_text(self.failure_reason)
        object.__setattr__(self, "failure_reason", failure)
        successful = status in {
            RegimeHawkesGenerationStatus.GENERATED,
            RegimeHawkesGenerationStatus.EMPTY,
        }
        if successful:
            if failure is not None or any(
                value is None
                for value in (
                    self.window_context_id,
                    anchor_hash,
                    input_content_hash,
                    history_hash,
                    context_hash,
                    self.conditioning_model_key,
                    radius,
                    lineage_hash,
                    self.final_filtered_probabilities,
                )
            ):
                raise ValueError(
                    "successful regime generation lacks audit evidence"
                )
            if sum(state_counts) != self.processed_bin_count:
                raise ValueError("state counts differ from processed bins")
            if self.input_event_count <= 0 or self.processed_bin_count <= 0:
                raise ValueError(
                    "successful regime generation lacks bounded work"
                )
        else:
            if failure is None:
                raise ValueError(
                    "failed/refused regime generation requires reason"
                )
            if self.generated_event_count != 0 or lineage_hash is not None:
                raise ValueError(
                    "closed regime generation exposes generated output"
                )
        if (
            status is RegimeHawkesGenerationStatus.EMPTY
            and self.generated_event_count != 0
        ):
            raise ValueError("empty regime generation contains events")
        if (
            status is RegimeHawkesGenerationStatus.GENERATED
            and self.generated_event_count == 0
        ):
            raise ValueError("generated regime evidence has no events")
        expected = _stable_id(
            "regime-hawkes-generation-evidence", self.identity_payload()
        )
        if self.evidence_id and self.evidence_id != expected:
            raise ValueError("regime Hawkes evidence_id differs")
        object.__setattr__(self, "evidence_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "fit_id": self.fit_id,
            "window_id": self.window_id,
            "window_context_id": self.window_context_id,
            "ensemble_member_id": self.ensemble_member_id,
            "status": self.status.value,
            "attempted": self.attempted,
            "input_event_count": self.input_event_count,
            "history_event_count": self.history_event_count,
            "generated_event_count": self.generated_event_count,
            "processed_bin_count": self.processed_bin_count,
            "poisson_iteration_count": self.poisson_iteration_count,
            "state_bin_counts": list(self.state_bin_counts),
            "state_transition_count": self.state_transition_count,
            "initial_state_policy": self.initial_state_policy,
            "final_filtered_probabilities": (
                list(self.final_filtered_probabilities)
                if self.final_filtered_probabilities is not None
                else None
            ),
            "input_anchor_sha256": self.input_anchor_sha256,
            "input_event_content_sha256": self.input_event_content_sha256,
            "history_content_sha256": self.history_content_sha256,
            "window_context_sha256": self.window_context_sha256,
            "conditioning_support_level": self.conditioning_support_level,
            "conditioning_model_key": self.conditioning_model_key,
            "maximum_spectral_radius": self.maximum_spectral_radius,
            "lineage_content_sha256": self.lineage_content_sha256,
            "wall_time_ms": self.wall_time_ms,
            "peak_memory_bytes": self.peak_memory_bytes,
            "failure_reason": self.failure_reason,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "evidence_id": self.evidence_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> RegimeHawkesGenerationEvidenceV1:
        _require_schema(data, REGIME_HAWKES_GENERATION_EVIDENCE_SCHEMA_VERSION)
        raw_probabilities = data.get("final_filtered_probabilities")
        return cls(
            fit_id=str(data.get("fit_id", "")),
            window_id=str(data.get("window_id", "")),
            window_context_id=_optional_text(data.get("window_context_id")),
            ensemble_member_id=str(data.get("ensemble_member_id", "")),
            status=RegimeHawkesGenerationStatus(str(data.get("status", ""))),
            attempted=_strict_bool(data.get("attempted"), "attempted"),
            input_event_count=_strict_int(
                data.get("input_event_count"), "input_event_count"
            ),
            history_event_count=_strict_int(
                data.get("history_event_count"), "history_event_count"
            ),
            generated_event_count=_strict_int(
                data.get("generated_event_count"), "generated_event_count"
            ),
            processed_bin_count=_strict_int(
                data.get("processed_bin_count"), "processed_bin_count"
            ),
            poisson_iteration_count=_strict_int(
                data.get("poisson_iteration_count"), "poisson_iteration_count"
            ),
            state_bin_counts=cast(
                tuple[int, int], tuple(_sequence(data.get("state_bin_counts")))
            ),
            state_transition_count=_strict_int(
                data.get("state_transition_count"), "state_transition_count"
            ),
            initial_state_policy=str(data.get("initial_state_policy", "")),
            final_filtered_probabilities=(
                cast(tuple[float, float], tuple(_sequence(raw_probabilities)))
                if raw_probabilities is not None
                else None
            ),
            input_anchor_sha256=_optional_text(data.get("input_anchor_sha256")),
            input_event_content_sha256=_optional_text(
                data.get("input_event_content_sha256")
            ),
            history_content_sha256=_optional_text(
                data.get("history_content_sha256")
            ),
            window_context_sha256=_optional_text(
                data.get("window_context_sha256")
            ),
            conditioning_support_level=str(
                data.get("conditioning_support_level", "")
            ),
            conditioning_model_key=_optional_text(
                data.get("conditioning_model_key")
            ),
            maximum_spectral_radius=_optional_float(
                data.get("maximum_spectral_radius")
            ),
            lineage_content_sha256=_optional_text(
                data.get("lineage_content_sha256")
            ),
            wall_time_ms=_strict_int(data.get("wall_time_ms"), "wall_time_ms"),
            peak_memory_bytes=_strict_int(
                data.get("peak_memory_bytes"), "peak_memory_bytes"
            ),
            failure_reason=_optional_text(data.get("failure_reason")),
            evidence_id=str(data.get("evidence_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> RegimeHawkesGenerationEvidenceV1:
        return cls.from_dict(
            _mapping(json.loads(text), "regime Hawkes generation evidence")
        )


@dataclass(frozen=True, slots=True)
class RegimeHawkesGenerationResultV1:
    """Process-local synchronized rows, lineage, and complete evidence."""

    events: tuple[BenchmarkEventV1, ...]
    event_lineage: tuple[RegimeHawkesGenerationLineageV1, ...]
    evidence: RegimeHawkesGenerationEvidenceV1

    def __post_init__(self) -> None:
        if any(not isinstance(item, BenchmarkEventV1) for item in self.events):
            raise TypeError("regime generation contains invalid events")
        lineages = tuple(
            sorted(self.event_lineage, key=lambda item: item.source_event_id)
        )
        if any(
            not isinstance(item, RegimeHawkesGenerationLineageV1)
            for item in lineages
        ):
            raise TypeError("regime generation contains invalid lineage")
        generated_ids = {
            item.source_event_id
            for item in self.events
            if item.sparsity.startswith("regime-hawkes-")
        }
        if generated_ids != {item.source_event_id for item in lineages}:
            raise ValueError("regime generated events and lineage differ")
        if self.evidence.generated_event_count != len(lineages):
            raise ValueError("regime generation evidence count differs")
        if self.evidence.status in {
            RegimeHawkesGenerationStatus.REFUSED,
            RegimeHawkesGenerationStatus.FAILED,
        } and (self.events or lineages):
            raise ValueError("closed regime generation exposes partial output")
        if (
            self.evidence.lineage_content_sha256 is not None
            and self.evidence.lineage_content_sha256
            != _lineage_sha256(lineages)
        ):
            raise ValueError("regime generation lineage digest differs")
        object.__setattr__(self, "event_lineage", lineages)


@dataclass(frozen=True, slots=True)
class FittedRegimeHawkesBenchmarkGeneratorV1(BenchmarkGeneratorV1):
    """Adapter exposing one fitted regime ablation to the benchmark."""

    candidate: BenchmarkCandidateV1
    config: RegimeHawkesConfigV1
    fit_result: RegimeHawkesFitResultV1
    window_contexts: Mapping[str, RegimeHawkesWindowContextV1] = field(
        default_factory=dict
    )
    candidate_id: str = field(init=False)
    event_schema_version: str = BENCHMARK_EVENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.candidate.kind is not BenchmarkCandidateKind.CANDIDATE:
            raise ValueError("regime Hawkes adapter requires a candidate")
        if self.candidate.method_id != _generator_id(self.config.modulation):
            raise ValueError("regime Hawkes candidate method differs")
        if self.fit_result.status is not RegimeHawkesFitStatus.FITTED:
            raise RegimeHawkesFitError(
                "regime Hawkes adapter requires a fitted model"
            )
        _validate_fit_against_config(self.config, self.fit_result)
        contexts = dict(self.window_contexts)
        if any(key != value.window_id for key, value in contexts.items()):
            raise ValueError("regime generation context key differs")
        object.__setattr__(self, "window_contexts", contexts)
        if self.event_schema_version != BENCHMARK_EVENT_SCHEMA_VERSION:
            raise ValueError(
                "regime Hawkes adapter requires benchmark event v1"
            )
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
            RegimeHawkesGenerationStatus.REFUSED,
            RegimeHawkesGenerationStatus.FAILED,
        }:
            raise RegimeHawkesGenerationError(
                result.evidence.failure_reason
                or "regime Hawkes generation failed"
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
        window_context: RegimeHawkesWindowContextV1 | None = None,
    ) -> RegimeHawkesGenerationResultV1:
        started = time.perf_counter()
        before_peak = peak_rss_bytes()
        raw_events = tuple(degraded_events)
        context = window_context or self.window_contexts.get(window.window_id)
        audit_context = (
            context
            if isinstance(context, RegimeHawkesWindowContextV1)
            else None
        )
        input_hash: str | None = None
        input_content_hash: str | None = None
        history_hash: str | None = None
        context_hash: str | None = None
        model_key: str | None = None
        support_level = "not_evaluated"
        radius: float | None = None
        processed_bins = 0
        poisson_iterations = 0
        state_counts = (0, 0)
        transitions = 0
        retained_history_count = 0
        final_probabilities: tuple[float, float] | None = None
        initial_policy = "not_evaluated"
        try:
            if context is None or not isinstance(
                context, RegimeHawkesWindowContextV1
            ):
                raise RegimeHawkesGenerationError(
                    "generation window context is required"
                )
            if context.window_id != window.window_id:
                raise RegimeHawkesGenerationError(
                    "generation window context identity differs"
                )
            context_hash = hashlib.sha256(
                canonical_contract_json(context.to_dict()).encode()
            ).hexdigest()
            if not raw_events or any(
                not isinstance(item, BenchmarkEventV1) for item in raw_events
            ):
                raise RegimeHawkesGenerationError(
                    "degraded input requires benchmark anchors"
                )
            ordered = tuple(sorted(raw_events, key=_benchmark_event_key))
            if any(
                not window.reads_event_time(item.event_time_ns)
                for item in ordered
            ):
                raise RegimeHawkesGenerationError(
                    "degraded anchor lies outside window input"
                )
            if any(
                item.sparsity.startswith("regime-hawkes-") for item in ordered
            ):
                raise RegimeHawkesGenerationError(
                    "degraded input contains prior regime proposals"
                )
            input_hash = _benchmark_anchor_sha256(ordered)
            input_content_hash = _benchmark_content_sha256(ordered)
            history = _validated_history(
                history_events, config=self.config, window=window
            )
            history_hash = _benchmark_content_sha256(history)
            retained_history_count = len(history)
            model_key, support_level, model = _conditioning_model(
                self.fit_result, context
            )
            _validate_model(model, self.config)
            radius = max(
                float(value)
                for value in cast(Sequence[Any], model["spectral_radii"])
            )
            events, lineage, generation_metrics = _generate_events(
                self.config,
                self.fit_result,
                model,
                ordered,
                context=context,
                scenario=scenario,
                window=window,
                ensemble_member_id=ensemble_member_id,
                history_events=history,
                input_event_content_sha256=input_content_hash,
                history_content_sha256=history_hash,
                deadline=(
                    started + self.config.limits.max_wall_time_ms / 1000.0
                ),
            )
            processed_bins = generation_metrics["processed_bins"]
            poisson_iterations = generation_metrics["poisson_iterations"]
            state_counts = cast(
                tuple[int, int], generation_metrics["state_counts"]
            )
            transitions = generation_metrics["transitions"]
            final_probabilities = cast(
                tuple[float, float], generation_metrics["final_probabilities"]
            )
            initial_policy = generation_metrics["initial_policy"]
            measured_peak = _incremental_peak_rss_bytes(before_peak)
            if measured_peak > self.config.limits.max_peak_memory_bytes:
                raise RegimeHawkesGenerationError(
                    "measured generation memory exceeds limit"
                )
            elapsed = round((time.perf_counter() - started) * 1000)
            if elapsed > self.config.limits.max_wall_time_ms:
                raise RegimeHawkesGenerationError(
                    "generation wall-time limit exceeded"
                )
            generated_count = len(lineage)
            status = (
                RegimeHawkesGenerationStatus.GENERATED
                if generated_count
                else RegimeHawkesGenerationStatus.EMPTY
            )
            evidence = RegimeHawkesGenerationEvidenceV1(
                fit_id=self.fit_result.fit_id,
                window_id=window.window_id,
                window_context_id=context.context_id,
                ensemble_member_id=ensemble_member_id,
                status=status,
                attempted=True,
                input_event_count=len(ordered),
                history_event_count=len(history),
                generated_event_count=generated_count,
                processed_bin_count=processed_bins,
                poisson_iteration_count=poisson_iterations,
                state_bin_counts=state_counts,
                state_transition_count=transitions,
                initial_state_policy=initial_policy,
                final_filtered_probabilities=final_probabilities,
                input_anchor_sha256=input_hash,
                input_event_content_sha256=input_content_hash,
                history_content_sha256=history_hash,
                window_context_sha256=context_hash,
                conditioning_support_level=support_level,
                conditioning_model_key=model_key,
                maximum_spectral_radius=radius,
                lineage_content_sha256=_lineage_sha256(lineage),
                wall_time_ms=elapsed,
                peak_memory_bytes=measured_peak,
            )
            return RegimeHawkesGenerationResultV1(
                events=events, event_lineage=lineage, evidence=evidence
            )
        except RegimeHawkesGenerationError as err:
            evidence = _closed_generation_evidence(
                self.fit_result,
                window,
                ensemble_member_id,
                status=RegimeHawkesGenerationStatus.REFUSED,
                reason=str(err),
                raw_count=len(raw_events),
                history_count=retained_history_count,
                context=audit_context,
                input_hash=input_hash,
                input_content_hash=input_content_hash,
                history_hash=history_hash,
                context_hash=context_hash,
                model_key=model_key,
                support_level=support_level,
                radius=radius,
                processed_bins=processed_bins,
                poisson_iterations=poisson_iterations,
                state_counts=state_counts,
                transitions=transitions,
                final_probabilities=final_probabilities,
                initial_policy=initial_policy,
                started=started,
                before_peak=before_peak,
            )
            return RegimeHawkesGenerationResultV1(
                events=(), event_lineage=(), evidence=evidence
            )
        except (ArithmeticError, KeyError, TypeError, ValueError) as err:
            evidence = _closed_generation_evidence(
                self.fit_result,
                window,
                ensemble_member_id,
                status=RegimeHawkesGenerationStatus.FAILED,
                reason=f"generation_failed:{type(err).__name__}:{err}",
                raw_count=len(raw_events),
                history_count=retained_history_count,
                context=audit_context,
                input_hash=input_hash,
                input_content_hash=input_content_hash,
                history_hash=history_hash,
                context_hash=context_hash,
                model_key=model_key,
                support_level=support_level,
                radius=None,
                processed_bins=processed_bins,
                poisson_iterations=poisson_iterations,
                state_counts=state_counts,
                transitions=transitions,
                final_probabilities=final_probabilities,
                initial_policy=initial_policy,
                started=started,
                before_peak=before_peak,
            )
            return RegimeHawkesGenerationResultV1(
                events=(), event_lineage=(), evidence=evidence
            )


def build_regime_hawkes_benchmark_candidate(
    config: RegimeHawkesConfigV1,
    fit_result: RegimeHawkesFitResultV1,
    *,
    ensemble_member_ids: Sequence[str],
) -> BenchmarkCandidateV1:
    """Describe a fit attempt without promoting or hiding a failed ablation."""
    if (
        fit_result.config_id != config.config_id
        or fit_result.modulation is not config.modulation
    ):
        raise ValueError("regime Hawkes fit and config differ")
    return BenchmarkCandidateV1(
        kind=BenchmarkCandidateKind.CANDIDATE,
        method_id=_generator_id(config.modulation),
        implementation_version=REGIME_HAWKES_IMPLEMENTATION_VERSION,
        parameters={
            "config_id": config.config_id,
            "fit_id": fit_result.fit_id,
            "modulation": config.modulation.value,
            "automatic_winner": False,
        },
        ensemble_member_ids=tuple(ensemble_member_ids),
    )


def build_fitted_regime_hawkes_generator(
    config: RegimeHawkesConfigV1,
    fit_result: RegimeHawkesFitResultV1,
    *,
    ensemble_member_ids: Sequence[str],
    window_contexts: Sequence[RegimeHawkesWindowContextV1] = (),
) -> FittedRegimeHawkesBenchmarkGeneratorV1:
    """Bind one stable fit to a benchmark candidate and context registry."""
    candidate = build_regime_hawkes_benchmark_candidate(
        config, fit_result, ensemble_member_ids=ensemble_member_ids
    )
    return FittedRegimeHawkesBenchmarkGeneratorV1(
        candidate=candidate,
        config=config,
        fit_result=fit_result,
        window_contexts={item.window_id: item for item in window_contexts},
    )


def _generate_events(
    config: RegimeHawkesConfigV1,
    fit: RegimeHawkesFitResultV1,
    model: Mapping[str, Any],
    anchors: tuple[BenchmarkEventV1, ...],
    *,
    context: RegimeHawkesWindowContextV1,
    scenario: BenchmarkScenarioV1,
    window: ReconstructionWindowV1,
    ensemble_member_id: str,
    history_events: tuple[BenchmarkEventV1, ...],
    input_event_content_sha256: str,
    history_content_sha256: str,
    deadline: float,
) -> tuple[
    tuple[BenchmarkEventV1, ...],
    tuple[RegimeHawkesGenerationLineageV1, ...],
    dict[str, Any],
]:
    symbols = fit.symbols
    if tuple(sorted(value.upper() for value in window.symbols)) != symbols:
        raise RegimeHawkesGenerationError(
            "window symbols differ from fitted regime model"
        )
    by_symbol = {
        symbol: tuple(item for item in anchors if item.symbol == symbol)
        for symbol in symbols
    }
    if any(len(values) < 2 for values in by_symbol.values()):
        raise RegimeHawkesGenerationError(
            "each symbol requires two destination anchors"
        )
    if any(
        any(
            left.event_time_ns >= right.event_time_ns
            for left, right in pairwise(values)
        )
        for values in by_symbol.values()
    ):
        raise RegimeHawkesGenerationError(
            "destination anchors are not strictly ordered"
        )
    transition = cast(Sequence[Sequence[float]], model["transition_matrix"])
    initial = cast(Sequence[float], model["initial_probabilities"])
    baselines = cast(
        Sequence[Sequence[float]], model["baseline_rates_per_second"]
    )
    matrices = cast(
        Sequence[Sequence[Sequence[float]]], model["excitation_matrices"]
    )
    marks = cast(
        Sequence[Sequence[Mapping[str, float]]], model["mark_probabilities"]
    )
    if context.filtered_initial_probabilities is not None:
        probabilities = list(context.filtered_initial_probabilities)
        initial_policy = "point-in-time-observed-context-filtered-prior-v1"
    else:
        probabilities = list(initial)
        initial_policy = "fitted-window-reset-distribution-v1"
    rng = random.Random(
        _semantic_seed(
            config.base_seed,
            config.config_id,
            _generation_model_sha256(fit),
            window.window_id,
            ensemble_member_id,
            context.context_id,
            scenario.scenario_id,
            input_event_content_sha256,
            history_content_sha256,
        )
    )
    state = _sample_index(probabilities, rng)
    start = window.core_start_ns
    end = window.core_end_ns
    bin_count = math.ceil((end - start) / config.bin_width_ns)
    if bin_count > config.limits.max_generation_bins:
        raise RegimeHawkesGenerationError("generation bin limit exceeded")
    estimated_generation_bytes = (
        (
            len(anchors)
            + len(history_events)
            + config.limits.max_generated_events_per_window
        )
        * config.limits.estimated_bytes_per_generated_event
        + bin_count * config.limits.estimated_bytes_per_generation_bin
    )
    if estimated_generation_bytes > config.limits.max_peak_memory_bytes:
        raise RegimeHawkesGenerationError(
            "generation memory estimate exceeds limit"
        )
    decay = math.exp(
        -config.decay_per_second * config.bin_width_ns / NANOSECONDS_PER_SECOND
    )
    recursion = [0.0 for _ in symbols]
    lookback_start = start - config.limits.max_history_ns
    for event in history_events:
        if event.event_time_ns >= lookback_start:
            age_bins = max(
                1,
                math.ceil((start - event.event_time_ns) / config.bin_width_ns),
            )
            recursion[symbols.index(event.symbol)] += decay**age_bins
    anchors_by_bin: Counter[tuple[int, int]] = Counter()
    for event in anchors:
        index = (event.event_time_ns - start) // config.bin_width_ns
        if 0 <= index < bin_count:
            anchors_by_bin[(index, symbols.index(event.symbol))] += 1
    generated: list[BenchmarkEventV1] = []
    lineage: list[RegimeHawkesGenerationLineageV1] = []
    state_counts = [0, 0]
    transition_count = 0
    poisson_iterations = 0
    amplification = _missing_intensity_scale(scenario)
    generated_in_previous = [0 for _ in symbols]
    for bin_index in range(bin_count):
        if time.perf_counter() > deadline:
            raise RegimeHawkesGenerationError(
                "generation wall-time limit exceeded"
            )
        state_transitioned = False
        if bin_index:
            probabilities = [
                sum(
                    probabilities[prior] * transition[prior][target]
                    for prior in range(2)
                )
                for target in range(2)
            ]
            probabilities = list(_normalized_pair(probabilities))
            next_state = _sample_index(transition[state], rng)
            state_transitioned = next_state != state
            transition_count += int(state_transitioned)
            state = next_state
            recursion = [
                decay * recursion[index]
                + anchors_by_bin[(bin_index - 1, index)]
                + generated_in_previous[index]
                for index in range(len(symbols))
            ]
        state_counts[state] += 1
        generated_in_current = [0 for _ in symbols]
        bin_start = start + bin_index * config.bin_width_ns
        bin_end = min(end, bin_start + config.bin_width_ns)
        for destination, symbol in enumerate(symbols):
            intervals = _active_anchor_intervals(
                by_symbol[symbol], bin_start, bin_end
            )
            if not intervals:
                continue
            intensity = baselines[state][
                destination
            ] + config.decay_per_second * sum(
                matrices[state][destination][source] * recursion[source]
                for source in range(len(symbols))
            )
            if not math.isfinite(intensity) or intensity <= 0.0:
                raise RegimeHawkesGenerationError(
                    "nonpositive/nonfinite conditional intensity"
                )
            total_duration = sum(
                right - left for left, right, _, _ in intervals
            )
            mean = (
                intensity
                * (total_duration / NANOSECONDS_PER_SECOND)
                * amplification
            )
            remaining_poisson_iterations = (
                config.limits.max_poisson_iterations_per_window
                - poisson_iterations
            )
            if remaining_poisson_iterations <= 0:
                raise RegimeHawkesGenerationError(
                    "window Poisson iteration limit exceeded"
                )
            count, iterations = _poisson(
                mean,
                rng,
                min(
                    config.limits.max_poisson_iterations,
                    remaining_poisson_iterations,
                ),
            )
            poisson_iterations += iterations
            if time.perf_counter() > deadline:
                raise RegimeHawkesGenerationError(
                    "generation wall-time limit exceeded"
                )
            if count > config.limits.max_generated_events_per_bin:
                raise RegimeHawkesGenerationError(
                    "per-bin generated-event limit exceeded"
                )
            if (
                sum(generated_in_current) + count
                > config.limits.max_generated_events_per_bin
            ):
                raise RegimeHawkesGenerationError(
                    "aggregate per-bin generated-event limit exceeded"
                )
            if count > sum(
                max(0, right - left - 1) for left, right, _, _ in intervals
            ):
                raise RegimeHawkesGenerationError(
                    "timestamp cardinality exceeds anchor gaps"
                )
            times = _uniform_interval_times(intervals, count, rng)
            interval_counts = [
                sum(left <= value < right for value in times)
                for left, right, _, _ in intervals
            ]
            if interval_counts and max(interval_counts) > (
                config.limits.max_generated_events_per_interval
            ):
                raise RegimeHawkesGenerationError(
                    "per-interval generated-event limit exceeded"
                )
            contribution_values = [
                matrices[state][destination][source] * recursion[source]
                for source in range(len(symbols))
            ]
            source_index = max(
                range(len(symbols)),
                key=lambda index: contribution_values[index],
            )
            source_symbol = (
                symbols[source_index]
                if contribution_values[source_index] > 0.0
                else None
            )
            source_contribution = (
                config.decay_per_second * contribution_values[source_index]
                if source_symbol is not None
                else 0.0
            )
            for ordinal, event_time in enumerate(times, start=1):
                left_anchor, right_anchor = _bracketing_anchors(
                    by_symbol[symbol], event_time
                )
                mark = _sample_mark(marks[state][destination], rng)
                bid, ask = _project_quote(
                    left_anchor, right_anchor, event_time, mark
                )
                source_id = _stable_id(
                    "regime-hawkes-generated-event",
                    {
                        "fit_id": fit.fit_id,
                        "window_id": window.window_id,
                        "ensemble_member_id": ensemble_member_id,
                        "bin_index": bin_index,
                        "symbol": symbol,
                        "event_time_ns": event_time,
                        "ordinal": ordinal,
                        "state": STATE_LABELS[state],
                    },
                )
                event = BenchmarkEventV1(
                    source_event_id=source_id,
                    symbol=symbol,
                    event_time_ns=event_time,
                    event_sequence=len(generated) + 1,
                    bid=bid,
                    ask=ask,
                    epoch_id=context.technology_label,
                    session=context.session,
                    event_state=mark,
                    sparsity=f"regime-hawkes-{config.modulation.value}",
                    ensemble_member_id=ensemble_member_id,
                    anchor_id=None,
                )
                generated.append(event)
                lineage.append(
                    RegimeHawkesGenerationLineageV1(
                        source_event_id=source_id,
                        destination_symbol=symbol,
                        bin_start_ns=bin_start,
                        state_label=STATE_LABELS[state],
                        filtered_state_probability=probabilities[state],
                        conditional_intensity=intensity,
                        excitation_source_symbol=source_symbol,
                        excitation_source_contribution=source_contribution,
                        state_transitioned=state_transitioned,
                        event_state=mark,
                    )
                )
                generated_in_current[destination] += 1
                if (
                    len(generated)
                    > config.limits.max_generated_events_per_window
                ):
                    raise RegimeHawkesGenerationError(
                        "window generated-event limit exceeded"
                    )
                if len(
                    generated
                ) > config.limits.max_candidate_amplification * len(anchors):
                    raise RegimeHawkesGenerationError(
                        "candidate amplification limit exceeded"
                    )
        probabilities = list(
            _filter_anchor_counts(
                probabilities,
                [
                    anchors_by_bin[(bin_index, index)]
                    for index in range(len(symbols))
                ],
                baselines,
                matrices,
                recursion,
                config=config,
                duration_seconds=(bin_end - bin_start) / NANOSECONDS_PER_SECOND,
            )
        )
        generated_in_previous = generated_in_current
    ordered_generated = tuple(sorted(generated, key=_benchmark_event_key))
    combined = tuple(
        sorted((*anchors, *ordered_generated), key=_benchmark_event_key)
    )
    combined_anchors = tuple(
        item
        for item in combined
        if not item.sparsity.startswith("regime-hawkes-")
    )
    if combined_anchors != tuple(sorted(anchors, key=_benchmark_event_key)):
        raise RegimeHawkesGenerationError(
            "immutable anchors changed during generation"
        )
    if _benchmark_anchor_sha256(combined) != _benchmark_anchor_sha256(anchors):
        raise RegimeHawkesGenerationError(
            "immutable anchors changed during generation"
        )
    return (
        combined,
        tuple(lineage),
        {
            "processed_bins": bin_count,
            "poisson_iterations": poisson_iterations,
            "state_counts": tuple(state_counts),
            "transitions": transition_count,
            "final_probabilities": tuple(probabilities),
            "initial_policy": initial_policy,
        },
    )


def _filter_anchor_counts(
    prior: Sequence[float],
    counts: Sequence[int],
    baselines: Sequence[Sequence[float]],
    matrices: Sequence[Sequence[Sequence[float]]],
    recursion: Sequence[float],
    *,
    config: RegimeHawkesConfigV1,
    duration_seconds: float,
) -> tuple[float, float]:
    """Filter state probabilities using anchors available through a bin."""
    log_weights: list[float] = []
    for state in range(2):
        value = math.log(max(config.parameter_floor, prior[state]))
        for destination, count in enumerate(counts):
            intensity = baselines[state][destination] + (
                config.decay_per_second
                * sum(
                    matrices[state][destination][source] * recursion[source]
                    for source in range(len(recursion))
                )
            )
            mean = max(
                config.parameter_floor,
                intensity * duration_seconds,
            )
            value += count * math.log(mean) - mean - math.lgamma(count + 1.0)
        log_weights.append(value)
    offset = max(log_weights)
    normalized = _normalized_pair(
        [math.exp(value - offset) for value in log_weights]
    )
    return normalized[0], normalized[1]


@dataclass(frozen=True, slots=True)
class RegimeHawkesCandidateLineageV1:
    """Compact carveable pointer with latent-state generation provenance."""

    event_id: str
    transformation_id: str
    generation_lineage_id: str
    state_label: str
    excitation_source_symbol: str | None
    excitation_source_contribution: float
    state_transitioned: bool
    event_state: str
    schema_version: str = REGIME_HAWKES_CANDIDATE_LINEAGE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            REGIME_HAWKES_CANDIDATE_LINEAGE_SCHEMA_VERSION,
            "regime Hawkes candidate lineage",
        )
        for name in ("event_id", "transformation_id", "generation_lineage_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        if self.state_label not in STATE_LABELS:
            raise ValueError("unsupported regime state label")
        if self.excitation_source_symbol is not None:
            object.__setattr__(
                self,
                "excitation_source_symbol",
                _symbol(self.excitation_source_symbol),
            )
        contribution = _finite_float(
            self.excitation_source_contribution,
            "excitation_source_contribution",
        )
        if contribution < 0.0:
            raise ValueError(
                "excitation source contribution must be nonnegative"
            )
        if self.excitation_source_symbol is None and contribution != 0.0:
            raise ValueError("source-free excitation contribution must be zero")
        if self.excitation_source_symbol is not None and contribution <= 0.0:
            raise ValueError(
                "named excitation source must contribute positively"
            )
        object.__setattr__(
            self,
            "state_transitioned",
            _strict_bool(self.state_transitioned, "state_transitioned"),
        )
        if self.event_state not in MARK_STATES:
            raise ValueError("unsupported regime candidate mark")

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "transformation_id": self.transformation_id,
            "generation_lineage_id": self.generation_lineage_id,
            "state_label": self.state_label,
            "excitation_source_symbol": self.excitation_source_symbol,
            "excitation_source_contribution": (
                self.excitation_source_contribution
            ),
            "state_transitioned": self.state_transitioned,
            "event_state": self.event_state,
        }


@dataclass(frozen=True, slots=True)
class RegimeHawkesCandidateBatchV1:
    """One context-bound anchor-interval batch for generic carving."""

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
    event_lineage: tuple[RegimeHawkesCandidateLineageV1, ...]
    fit_id: str
    generation_evidence_id: str
    window_context_id: str
    batch_id: str = ""
    schema_version: str = REGIME_HAWKES_CANDIDATE_BATCH_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            REGIME_HAWKES_CANDIDATE_BATCH_SCHEMA_VERSION,
            "regime Hawkes candidate batch",
        )
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
            "fit_id",
            "generation_evidence_id",
            "window_context_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(self, "symbol", _symbol(self.symbol).lower())
        object.__setattr__(
            self,
            "information_mode",
            InformationMode.from_value(self.information_mode),
        )
        object.__setattr__(
            self,
            "special_tags",
            tuple(
                sorted({_required_text(value) for value in self.special_tags})
            ),
        )
        object.__setattr__(
            self,
            "event_tags",
            tuple(sorted({_required_text(value) for value in self.event_tags})),
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
        lineage = tuple(
            sorted(self.event_lineage, key=lambda item: item.event_id)
        )
        if status is MotifGenerationStatus.GENERATED and not events:
            raise ValueError("generated regime batch requires events")
        if status is not MotifGenerationStatus.GENERATED and events:
            raise ValueError("empty/refused regime batch cannot have events")
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
            raise ValueError("regime candidate event differs from batch scope")
        if {item.event_id for item in events} != {
            item.event_id for item in lineage
        } or len({item.event_id for item in events}) != len(events):
            raise ValueError("regime candidate lineage does not reconcile")
        object.__setattr__(self, "events", events)
        object.__setattr__(self, "event_lineage", lineage)
        expected = _stable_id(
            "regime-hawkes-candidate-batch", self.identity_payload()
        )
        if self.batch_id and self.batch_id != expected:
            raise ValueError("regime Hawkes candidate batch_id differs")
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
            "fit_id": self.fit_id,
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

    def lineage_for(self, event_id: str) -> RegimeHawkesCandidateLineageV1:
        wanted = _required_text(event_id)
        for lineage in self.event_lineage:
            if lineage.event_id == wanted:
                return lineage
        raise KeyError(wanted)


def build_regime_hawkes_candidate_batches(
    *,
    run: ReconstructionRunV1,
    window: ReconstructionWindowV1,
    config: RegimeHawkesConfigV1,
    fit_result: RegimeHawkesFitResultV1,
    generation_result: RegimeHawkesGenerationResultV1,
    window_context: RegimeHawkesWindowContextV1,
    observed_events: Sequence[SyntheticEventV1],
    session_state: str,
    special_tags: Sequence[str] = (),
    event_tags: Sequence[str] = (),
) -> tuple[RegimeHawkesCandidateBatchV1, ...]:
    """Project all-or-nothing proposals into generator-neutral carving."""
    if (
        window.run_id != run.run_id
        or window.ensemble_member_id not in run.ensemble_member_ids
    ):
        raise ValueError(
            "regime candidate window does not belong to run/member"
        )
    if config.config_id not in run.configuration_ids:
        raise ValueError("regime config is absent from reconstruction run")
    if (
        fit_result.config_id != config.config_id
        or fit_result.fit_id != generation_result.evidence.fit_id
    ):
        raise ValueError("regime fit, config, and generation differ")
    if (
        window_context.window_id != window.window_id
        or generation_result.evidence.window_context_id
        != window_context.context_id
    ):
        raise ValueError("regime candidate context differs from generation")
    expected_context_hash = hashlib.sha256(
        canonical_contract_json(window_context.to_dict()).encode()
    ).hexdigest()
    if (
        generation_result.evidence.window_context_sha256 is not None
        and generation_result.evidence.window_context_sha256
        != expected_context_hash
    ):
        raise ValueError("regime candidate context digest differs")
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
        raise ValueError("regime carving projection requires observed anchors")
    if (
        generation_result.evidence.input_anchor_sha256 is not None
        and _synthetic_anchor_sha256(observed)
        != generation_result.evidence.input_anchor_sha256
    ):
        raise ValueError("carving anchors differ from regime generation input")
    proposals = tuple(
        item
        for item in generation_result.events
        if item.sparsity.startswith("regime-hawkes-")
    )
    if len({item.source_event_id for item in proposals}) != len(proposals):
        raise ValueError("regime proposals have duplicate source identity")
    lineage_by_source = {
        item.source_event_id: item for item in generation_result.event_lineage
    }
    if set(lineage_by_source) != {item.source_event_id for item in proposals}:
        raise ValueError("regime proposals and lineage differ")
    refused = generation_result.evidence.status in {
        RegimeHawkesGenerationStatus.REFUSED,
        RegimeHawkesGenerationStatus.FAILED,
    }
    by_symbol: dict[str, list[SyntheticEventV1]] = defaultdict(list)
    for event in observed:
        by_symbol[event.symbol].append(event)
    batches: list[RegimeHawkesCandidateBatchV1] = []
    assigned: set[str] = set()
    for symbol, anchors in sorted(by_symbol.items()):
        if len(anchors) < 2:
            raise ValueError("each regime carving symbol requires two anchors")
        for left_anchor, right_anchor in pairwise(anchors):
            interval_id = derive_anchor_interval_id(
                left_anchor.event_id, right_anchor.event_id
            )
            selected = tuple(
                item
                for item in proposals
                if item.symbol.lower() == symbol
                and left_anchor.event_time_ns
                < item.event_time_ns
                < right_anchor.event_time_ns
            )
            assigned.update(item.source_event_id for item in selected)
            transformation_id = _stable_id(
                "regime-hawkes-interval-transformation",
                {
                    "fit_id": fit_result.fit_id,
                    "generation_evidence_id": generation_result.evidence.evidence_id,
                    "window_context_id": window_context.context_id,
                    "anchor_interval_id": interval_id,
                },
            )
            events = tuple(
                SyntheticEventV1.generated(
                    symbol=symbol,
                    event_time_ns=item.event_time_ns,
                    event_sequence=ordinal,
                    bid=item.bid,
                    ask=item.ask,
                    run_id=run.run_id,
                    ensemble_member_id=window.ensemble_member_id,
                    source_version_id=fit_result.fit_id,
                    left_anchor_event_id=left_anchor.event_id,
                    right_anchor_event_id=right_anchor.event_id,
                    anchor_interval_id=interval_id,
                    generator_id=_generator_id(config.modulation),
                    generator_version=REGIME_HAWKES_IMPLEMENTATION_VERSION,
                    generator_config_id=config.config_id,
                    reference_id=item.source_event_id,
                    motif_id=_generator_id(config.modulation),
                    feed_epoch_id=window_context.technology_label,
                    constraint_set_id=CANDIDATE_ONLY_CONSTRAINT_SET_ID,
                )
                for ordinal, item in enumerate(selected, start=1)
            )
            status = (
                MotifGenerationStatus.REFUSED
                if refused
                else (
                    MotifGenerationStatus.GENERATED
                    if events
                    else MotifGenerationStatus.EMPTY
                )
            )
            candidate_lineage = tuple(
                RegimeHawkesCandidateLineageV1(
                    event_id=event.event_id,
                    transformation_id=transformation_id,
                    generation_lineage_id=lineage_by_source[
                        proposal.source_event_id
                    ].lineage_id,
                    state_label=lineage_by_source[
                        proposal.source_event_id
                    ].state_label,
                    excitation_source_symbol=lineage_by_source[
                        proposal.source_event_id
                    ].excitation_source_symbol,
                    excitation_source_contribution=lineage_by_source[
                        proposal.source_event_id
                    ].excitation_source_contribution,
                    state_transitioned=lineage_by_source[
                        proposal.source_event_id
                    ].state_transitioned,
                    event_state=lineage_by_source[
                        proposal.source_event_id
                    ].event_state,
                )
                for event, proposal in zip(events, selected)
            )
            batches.append(
                RegimeHawkesCandidateBatchV1(
                    run_id=run.run_id,
                    window_id=window.window_id,
                    ensemble_member_id=window.ensemble_member_id,
                    symbol=symbol,
                    anchor_interval_id=interval_id,
                    left_anchor_event_id=left_anchor.event_id,
                    right_anchor_event_id=right_anchor.event_id,
                    generator_config_id=config.config_id,
                    information_mode=fit_result.information_mode,
                    session_state=session_state,
                    special_tags=tuple(special_tags),
                    event_tags=tuple(event_tags),
                    status=status,
                    events=events if not refused else (),
                    event_lineage=candidate_lineage if not refused else (),
                    fit_id=fit_result.fit_id,
                    generation_evidence_id=generation_result.evidence.evidence_id,
                    window_context_id=window_context.context_id,
                )
            )
    if assigned != {item.source_event_id for item in proposals}:
        raise ValueError("regime proposal lies outside observed anchors")
    return tuple(batches)


def _validated_contexts(
    windows: Sequence[EventClockCalibrationWindowV1],
    values: Sequence[RegimeHawkesWindowContextV1],
) -> tuple[RegimeHawkesWindowContextV1, ...]:
    supplied = tuple(values)
    window_ids = {item.window_id for item in windows}
    if supplied:
        if any(
            not isinstance(item, RegimeHawkesWindowContextV1)
            for item in supplied
        ):
            raise TypeError("fit context registry contains invalid context")
        if (
            len({item.window_id for item in supplied}) != len(supplied)
            or {item.window_id for item in supplied} != window_ids
        ):
            raise ValueError(
                "fit context registry must cover each calibration window exactly once"
            )
        contexts = supplied
    else:
        inferred: list[RegimeHawkesWindowContextV1] = []
        for window in windows:
            epochs = {item.epoch_id for item in window.events}
            sessions = {item.session for item in window.events}
            if len(epochs) != 1 or len(sessions) != 1:
                raise ValueError(
                    "inferred fit context requires one technology label and session"
                )
            label = next(iter(epochs))
            if label.startswith("transition:"):
                raise ValueError(
                    "transition calibration windows require v2 boundary context"
                )
            inferred.append(
                RegimeHawkesWindowContextV1(
                    window_id=window.window_id,
                    session=next(iter(sessions)),
                    technology_assignment_kind="epoch",
                    technology_label=label,
                    feed_epoch_definition_id="benchmark-event-epoch-label-v1",
                    epoch_id=label,
                )
            )
        contexts = tuple(inferred)
    context_by_window = {item.window_id: item for item in contexts}
    for window in windows:
        context = context_by_window[window.window_id]
        if {event.session for event in window.events} != {context.session}:
            raise ValueError(
                "calibration event session differs from window context"
            )
        if {event.epoch_id for event in window.events} != {
            context.technology_label
        }:
            raise ValueError(
                "calibration event technology label differs from context"
            )
    return tuple(sorted(contexts, key=lambda item: item.window_id))


def _fit_refusal_reason(
    config: RegimeHawkesConfigV1,
    windows: Sequence[EventClockCalibrationWindowV1],
    contexts: Sequence[RegimeHawkesWindowContextV1],
    *,
    mode: InformationMode,
    as_of_ns: int | None,
    symbols: Sequence[str],
    event_count: int,
    bin_count: int,
    estimated_memory: int,
) -> str | None:
    if not windows:
        return "no calibration windows"
    if any(
        not isinstance(item, EventClockCalibrationWindowV1) for item in windows
    ):
        return "fit input contains an invalid calibration window"
    if len(windows) > config.limits.max_fit_windows:
        return "fit window limit exceeded"
    if event_count > config.limits.max_fit_events:
        return "fit event limit exceeded"
    if bin_count > config.limits.max_fit_bins:
        return "fit bin limit exceeded"
    if not symbols or len(symbols) > config.limits.max_dimensions:
        return "fit symbol dimension is outside bounds"
    if any(
        sum(
            event.symbol == symbol
            for window in windows
            for event in window.events
        )
        < config.minimum_events_per_symbol
        for symbol in symbols
    ):
        return "insufficient per-symbol event support"
    if len(contexts) != len(windows):
        return "fit context coverage differs"
    if estimated_memory > config.limits.max_peak_memory_bytes:
        return "estimated fit memory exceeds limit"
    if mode is InformationMode.EX_ANTE_SIMULATION:
        if as_of_ns is None:
            return "ex-ante fit requires as_of_ns"
        if any(
            event.event_time_ns > as_of_ns
            for window in windows
            for event in window.events
        ):
            return "calibration event is unavailable at ex-ante fit time"
        if any(
            context.observed_context_available_ns is not None
            and context.observed_context_available_ns > as_of_ns
            for context in contexts
        ):
            return "observed context is unavailable at ex-ante fit time"
    elif as_of_ns is not None:
        return "ex-post fit rejects as_of_ns"
    return None


def _closed_fit_result(
    config: RegimeHawkesConfigV1,
    *,
    calibration_hash: str,
    context_hash: str,
    mode: InformationMode,
    as_of_ns: int | None,
    symbols: tuple[str, ...],
    status: RegimeHawkesFitStatus,
    event_count: int,
    window_count: int,
    bin_count: int,
    estimated_memory: int,
    reason: str,
) -> RegimeHawkesFitResultV1:
    return RegimeHawkesFitResultV1(
        modulation=config.modulation,
        config_id=config.config_id,
        calibration_content_sha256=calibration_hash,
        calibration_context_sha256=context_hash,
        information_mode=mode,
        as_of_ns=as_of_ns,
        symbols=symbols,
        status=status,
        converged=False,
        iteration_count=0,
        fitted_event_count=event_count,
        fitted_window_count=window_count,
        fitted_bin_count=bin_count,
        log_likelihood=None,
        parameters={},
        uncertainty={},
        diagnostics={},
        state_diagnostics={},
        estimated_peak_memory_bytes=estimated_memory,
        failure_reason=reason,
    )


def _has_symbol_support(
    config: RegimeHawkesConfigV1,
    windows: Sequence[EventClockCalibrationWindowV1],
    symbols: Sequence[str],
) -> bool:
    return all(
        sum(
            event.symbol == symbol
            for window in windows
            for event in window.events
        )
        >= config.minimum_events_per_symbol
        for symbol in symbols
    )


def _validate_fit_against_config(
    config: RegimeHawkesConfigV1, fit: RegimeHawkesFitResultV1
) -> None:
    if fit.fit_id != _stable_id("regime-hawkes-fit", fit.identity_payload()):
        raise RegimeHawkesFitError("regime fit content identity differs")
    if (
        fit.config_id != config.config_id
        or fit.modulation is not config.modulation
    ):
        raise RegimeHawkesFitError("regime fit and configuration differ")
    if fit.status is not RegimeHawkesFitStatus.FITTED:
        raise RegimeHawkesFitError("regime fit is not usable")
    if (
        fit.fitted_event_count > config.limits.max_fit_events
        or fit.fitted_window_count > config.limits.max_fit_windows
        or fit.fitted_bin_count > config.limits.max_fit_bins
    ):
        raise RegimeHawkesFitError(
            "regime fit exceeds current resource contract"
        )
    if (
        _diagnostic_payload_bytes(
            fit.uncertainty, fit.diagnostics, fit.state_diagnostics
        )
        > config.limits.max_diagnostic_bytes
    ):
        raise RegimeHawkesFitError(
            "regime fit diagnostics exceed current resource contract"
        )
    _validate_fitted_parameters(fit.parameters, config.modulation, fit.symbols)
    _validate_fit_diagnostic_summaries(fit)
    for raw_model in cast(
        Mapping[str, Any], fit.parameters["conditioning_models"]
    ).values():
        _validate_model(cast(Mapping[str, Any], raw_model), config)


def _validate_fit_diagnostic_summaries(fit: RegimeHawkesFitResultV1) -> None:
    diagnostics = fit.diagnostics
    models = _mapping(
        fit.parameters.get("conditioning_models"), "conditioning_models"
    )
    parsed = [
        _mapping(value, f"conditioning model {key}")
        for key, value in models.items()
    ]
    exact_keys = [key for key in models if key.startswith("exact|")]
    session_keys = [key for key in models if key.startswith("session|")]

    def integer(name: str, expected: int) -> None:
        if _strict_int(diagnostics.get(name), name) != expected:
            raise ValueError(f"regime fit {name} differs")

    def numeric(name: str, expected: float) -> None:
        value = _finite_float(diagnostics.get(name), name)
        if abs(value - expected) > 1e-9 * (1.0 + abs(expected)):
            raise ValueError(f"regime fit {name} differs")

    integer("conditioning_cell_count", len(models))
    integer("exact_conditioning_cell_count", len(exact_keys))
    integer("session_backoff_cell_count", len(session_keys))
    integer(
        "parameter_bytes",
        len(canonical_contract_json(fit.parameters).encode()),
    )
    integer("calibration_history_reset_count", fit.fitted_window_count)
    integer(
        "diagnostic_bytes",
        _diagnostic_payload_bytes(
            fit.uncertainty, fit.diagnostics, fit.state_diagnostics
        ),
    )
    if (
        diagnostics.get("filtered_probability_policy")
        != "available-through-current-bin-only"
        or diagnostics.get("smoothed_probability_policy")
        != "ex-post-diagnostics-only"
    ):
        raise ValueError("regime fit probability policy differs")
    numeric(
        "minimum_state_occupancy",
        min(float(value["minimum_occupancy"]) for value in parsed),
    )
    numeric(
        "minimum_calm_state_occupancy",
        min(float(_sequence(value["occupancy"])[0]) for value in parsed),
    )
    numeric(
        "minimum_active_state_occupancy",
        min(float(_sequence(value["occupancy"])[1]) for value in parsed),
    )
    numeric(
        "minimum_activity_contrast",
        min(float(value["activity_contrast"]) for value in parsed),
    )
    numeric(
        "minimum_expected_transition_count",
        min(float(value["expected_transition_count"]) for value in parsed),
    )
    radii = [
        float(radius)
        for value in parsed
        for radius in _sequence(value["spectral_radii"])
    ]
    numeric("maximum_spectral_radius", max(radii))
    numeric(
        "stability_margin",
        min(
            1.0
            - max(
                float(radius) for radius in _sequence(value["spectral_radii"])
            )
            for value in parsed
        ),
    )
    dwell = [
        float(item)
        for value in parsed
        for item in _sequence(value["mean_dwell_bins"])
    ]
    numeric("minimum_mean_dwell_bins", min(dwell))
    numeric("maximum_mean_dwell_bins", max(dwell))
    numeric(
        "mean_posterior_entropy",
        sum(float(value["posterior_entropy_mean"]) for value in parsed)
        / len(parsed),
    )
    transition_labels = {
        key.split("|", 2)[1]
        for key in exact_keys
        if key.split("|", 2)[1].startswith("transition:")
    }
    integer("technology_transition_cell_count", len(transition_labels))
    if fit.log_likelihood is None:
        raise ValueError("regime fitted likelihood is missing")
    expected_likelihood = sum(
        float(_mapping(models[key], key)["log_likelihood"])
        for key in exact_keys
    )
    if abs(fit.log_likelihood - expected_likelihood) > 1e-9 * (
        1.0 + abs(expected_likelihood)
    ):
        raise ValueError("regime fitted likelihood summary differs")
    expected_iterations = max(int(value["iteration_count"]) for value in parsed)
    if fit.iteration_count != expected_iterations:
        raise ValueError("regime fitted iteration summary differs")
    exact_diagnostics = [
        _mapping(fit.state_diagnostics[key], key) for key in exact_keys
    ]
    exact_bin_count = sum(
        _strict_int(_mapping(window, "window").get("bin_count"), "bin_count")
        for evidence in exact_diagnostics
        for window in _sequence(evidence.get("windows"))
    )
    if fit.fitted_bin_count != exact_bin_count:
        raise ValueError("regime fitted bin summary differs")


def _validate_fitted_parameters(
    parameters: Mapping[str, Any],
    modulation: RegimeHawkesModulation,
    symbols: Sequence[str],
) -> None:
    if parameters.get("model") != "two-state-shared-chain-mmhp-delta-v1":
        raise ValueError("regime model identity differs")
    if parameters.get("modulation") != modulation.value:
        raise ValueError("regime modulation differs")
    if tuple(parameters.get("state_labels", ())) != STATE_LABELS:
        raise ValueError("regime state labels differ")
    if tuple(parameters.get("symbols", ())) != tuple(symbols):
        raise ValueError("regime fit symbols differ")
    if (
        parameters.get("same_bin_update_policy")
        != "affects-following-bin-only-v1"
    ):
        raise ValueError("same-bin excitation policy differs")
    models = parameters.get("conditioning_models")
    if not isinstance(models, Mapping) or not models:
        raise ValueError("regime fit lacks conditioning models")
    decay = _positive_float(
        parameters.get("decay_per_second"), "decay_per_second"
    )
    for model in models.values():
        if not isinstance(model, Mapping):
            raise TypeError("regime conditioning model is invalid")
        _validate_model_shape(model, len(symbols), modulation, decay)


def _validate_fit_uncertainty(
    uncertainty: Mapping[str, Any],
    parameters: Mapping[str, Any],
    symbols: Sequence[str],
) -> None:
    if uncertainty.get("method") != "posterior-effective-count-wald-95-v1":
        raise ValueError("regime fit uncertainty method differs")
    if (
        _finite_float(uncertainty.get("confidence_level"), "confidence_level")
        != 0.95
    ):
        raise ValueError("regime fit uncertainty confidence differs")
    models = _mapping(
        parameters.get("conditioning_models"), "conditioning_models"
    )
    uncertainty_models = _mapping(
        uncertainty.get("conditioning_models"),
        "uncertainty conditioning_models",
    )
    if set(uncertainty_models) != set(models):
        raise ValueError("regime fit uncertainty cells differ")
    dimension = len(symbols)

    def interval(raw: Any, value: float, name: str) -> None:
        lower, upper = _float_vector(raw, 2, name)
        if lower < 0.0 or lower > value or upper < value:
            raise ValueError(f"{name} does not contain its estimate")

    for key, raw_uncertainty in uncertainty_models.items():
        selected = _mapping(raw_uncertainty, f"uncertainty {key}")
        model = _mapping(models[key], f"conditioning model {key}")
        baselines = _sequence(model.get("baseline_rates_per_second"))
        matrices = _sequence(model.get("excitation_matrices"))
        baseline_intervals = _sequence(selected.get("baseline_rate_intervals"))
        excitation_intervals = _sequence(selected.get("excitation_intervals"))
        if len(baseline_intervals) != 2 or len(excitation_intervals) != 2:
            raise ValueError("regime uncertainty state cardinality differs")
        for state in range(2):
            baseline = _float_vector(baselines[state], dimension, "baseline")
            state_intervals = _sequence(baseline_intervals[state])
            if len(state_intervals) != dimension:
                raise ValueError(
                    "regime baseline uncertainty dimension differs"
                )
            for destination in range(dimension):
                interval(
                    state_intervals[destination],
                    baseline[destination],
                    "baseline uncertainty interval",
                )
            matrix = _float_matrix(
                matrices[state], dimension, "excitation matrix"
            )
            matrix_intervals = _sequence(excitation_intervals[state])
            if len(matrix_intervals) != dimension:
                raise ValueError(
                    "regime excitation uncertainty dimension differs"
                )
            for destination in range(dimension):
                row_intervals = _sequence(matrix_intervals[destination])
                if len(row_intervals) != dimension:
                    raise ValueError(
                        "regime excitation uncertainty row differs"
                    )
                for source in range(dimension):
                    interval(
                        row_intervals[source],
                        matrix[destination][source],
                        "excitation uncertainty interval",
                    )
        effective = _float_vector(
            selected.get("effective_state_bins"),
            2,
            "effective_state_bins",
        )
        if any(value <= 0.0 for value in effective):
            raise ValueError("regime uncertainty has no effective state bins")


def _validate_state_diagnostics(
    state_diagnostics: Mapping[str, Any],
    parameters: Mapping[str, Any],
) -> None:
    models = _mapping(
        parameters.get("conditioning_models"), "conditioning_models"
    )
    if set(state_diagnostics) != set(models):
        raise ValueError("regime state-diagnostic cells differ")
    expected_semantics = {
        "filtered": "P(z_b|observations through bin b)",
        "smoothed": "P(z_b|complete calibration window); ex-post only",
    }
    for key, raw_evidence in state_diagnostics.items():
        evidence = _mapping(raw_evidence, f"state diagnostics {key}")
        semantics = _mapping(
            evidence.get("probability_semantics"),
            "probability_semantics",
        )
        if dict(semantics) != expected_semantics:
            raise ValueError("regime probability semantics differ")
        windows = _sequence(evidence.get("windows"))
        if not windows:
            raise ValueError("regime state diagnostics lack windows")
        window_ids: set[str] = set()
        total_bins = 0
        for raw_window in windows:
            window = _mapping(raw_window, "state diagnostic window")
            window_id = _required_text(window.get("window_id"))
            if window_id in window_ids:
                raise ValueError("regime state diagnostics duplicate a window")
            window_ids.add(window_id)
            bin_count = _bounded_int(
                window.get("bin_count"),
                "state diagnostic bin_count",
                1,
                MAX_REGIME_FIT_BINS,
            )
            filtered = _sequence(window.get("filtered_probabilities"))
            smoothed = _sequence(window.get("smoothed_probabilities"))
            if len(filtered) != bin_count or len(smoothed) != bin_count:
                raise ValueError("regime state probability/bin count differs")
            for name, values in (
                ("filtered", filtered),
                ("smoothed", smoothed),
            ):
                for probabilities in values:
                    _probability_pair(probabilities, f"{name} probabilities")
            total_bins += bin_count
        if total_bins > MAX_REGIME_FIT_BINS:
            raise ValueError("regime state diagnostics exceed the bin bound")


def _validate_model_shape(
    model: Mapping[str, Any],
    dimension: int,
    modulation: RegimeHawkesModulation,
    decay_per_second: float,
) -> None:
    transition = _float_matrix(
        model.get("transition_matrix"), 2, "transition_matrix"
    )
    if any(
        abs(sum(row) - 1.0) > 1e-8
        or any(not 0.0 < value < 1.0 for value in row)
        for row in transition
    ):
        raise ValueError("regime transition matrix is invalid")
    _probability_pair(
        _float_vector(
            model.get("initial_probabilities"), 2, "initial_probabilities"
        ),
        "initial_probabilities",
    )
    baselines = model.get("baseline_rates_per_second")
    matrices = model.get("excitation_matrices")
    marks = model.get("mark_probabilities")
    radii = _float_vector(model.get("spectral_radii"), 2, "spectral_radii")
    if any(not 0.0 <= value < 1.0 for value in radii):
        raise ValueError("regime spectral radius is unstable")
    if (
        not isinstance(baselines, Sequence)
        or len(baselines) != 2
        or not isinstance(matrices, Sequence)
        or len(matrices) != 2
        or not isinstance(marks, Sequence)
        or len(marks) != 2
    ):
        raise ValueError("regime state parameter cardinality differs")
    parsed_matrices: list[list[list[float]]] = []
    activity: list[float] = []
    for state in range(2):
        baseline = _float_vector(baselines[state], dimension, "baseline")
        if any(value <= 0.0 for value in baseline):
            raise ValueError("regime baseline must be positive")
        matrix = _float_matrix(matrices[state], dimension, "excitation_matrix")
        if any(value < 0.0 for row in matrix for value in row):
            raise ValueError("regime excitation must be nonnegative")
        if abs(_spectral_radius(matrix) - radii[state]) > 1e-7:
            raise ValueError("regime spectral radius differs")
        parsed_matrices.append(matrix)
        activity.append(
            sum(baseline) + decay_per_second * sum(sum(row) for row in matrix)
        )
        if (
            not isinstance(marks[state], Sequence)
            or len(marks[state]) != dimension
        ):
            raise ValueError("regime mark dimension differs")
        for values in marks[state]:
            if not isinstance(values, Mapping) or set(values) != set(
                MARK_STATES
            ):
                raise ValueError("regime mark registry differs")
            probabilities = [
                _finite_float(values[key], key) for key in MARK_STATES
            ]
            if (
                any(value <= 0.0 for value in probabilities)
                or abs(sum(probabilities) - 1.0) > 1e-8
            ):
                raise ValueError("regime mark probabilities are invalid")
    if activity[0] >= activity[1]:
        raise ValueError("regime state labels are switched or collapsed")
    declared_activity = _float_vector(
        model.get("activity_levels"), 2, "activity_levels"
    )
    if any(
        abs(declared - computed) > 1e-7 * (1.0 + abs(computed))
        for declared, computed in zip(declared_activity, activity)
    ):
        raise ValueError("regime activity levels differ from parameters")
    contrast = (activity[1] - activity[0]) / max(activity[1], 1e-300)
    if (
        abs(
            _finite_float(model.get("activity_contrast"), "activity_contrast")
            - contrast
        )
        > 1e-7
    ):
        raise ValueError("regime activity contrast differs from parameters")
    occupancy = _probability_pair(
        _float_vector(model.get("occupancy"), 2, "occupancy"),
        "occupancy",
    )
    if any(value <= 0.0 for value in occupancy):
        raise ValueError("regime occupancy is degenerate")
    transition_counts = _float_matrix(
        model.get("transition_counts"), 2, "transition_counts"
    )
    if any(value < 0.0 for row in transition_counts for value in row):
        raise ValueError("regime transition counts are negative")
    expected_transitions = _finite_float(
        model.get("expected_transition_count"),
        "expected_transition_count",
    )
    if expected_transitions <= 0.0 or abs(
        expected_transitions - transition_counts[0][1] - transition_counts[1][0]
    ) > 1e-7 * (1.0 + expected_transitions):
        raise ValueError("regime transition summary differs")
    dwell = _float_vector(model.get("mean_dwell_bins"), 2, "mean_dwell_bins")
    if any(value <= 0.0 for value in dwell):
        raise ValueError("regime dwell summary is degenerate")
    entropy = _finite_float(
        model.get("posterior_entropy_mean"), "posterior_entropy_mean"
    )
    if not 0.0 <= entropy <= math.log(2.0) + 1e-8:
        raise ValueError("regime posterior entropy is outside bounds")
    log_likelihood = _finite_float(
        model.get("log_likelihood"), "log_likelihood"
    )
    iteration_count = _strict_int(
        model.get("iteration_count"), "iteration_count"
    )
    if iteration_count <= 0:
        raise ValueError("regime iteration count must be positive")
    likelihood_trace = _float_vector(
        model.get("log_likelihood_trace"),
        iteration_count,
        "log_likelihood_trace",
    )
    if abs(likelihood_trace[-1] - log_likelihood) > 1e-7 * (
        1.0 + abs(log_likelihood)
    ):
        raise ValueError("regime likelihood trace terminal value differs")
    if any(
        right < left - 1e-7 * (1.0 + abs(left))
        for left, right in pairwise(likelihood_trace)
    ):
        raise ValueError("regime likelihood trace decreases")
    if (
        modulation is RegimeHawkesModulation.BASELINE_ONLY
        and parsed_matrices[0] != parsed_matrices[1]
    ):
        raise ValueError("baseline-only ablation has state-specific excitation")


def _validate_model(
    model: Mapping[str, Any], config: RegimeHawkesConfigV1
) -> None:
    _validate_model_shape(
        model,
        len(cast(Sequence[Any], model["baseline_rates_per_second"])[0]),
        config.modulation,
        config.decay_per_second,
    )
    radii = _float_vector(model.get("spectral_radii"), 2, "spectral_radii")
    if any(value >= config.maximum_branching_ratio for value in radii):
        raise RegimeHawkesGenerationError(
            "regime excitation violates configured stability margin"
        )
    occupancy = _float_vector(model.get("occupancy"), 2, "occupancy")
    if min(occupancy) < config.minimum_state_occupancy:
        raise RegimeHawkesGenerationError(
            "regime state occupancy is degenerate"
        )
    if (
        _finite_float(model.get("activity_contrast"), "activity_contrast")
        < config.minimum_activity_contrast
    ):
        raise RegimeHawkesGenerationError(
            "regime activity contrast is degenerate"
        )
    if (
        _finite_float(
            model.get("expected_transition_count"), "expected_transition_count"
        )
        < config.minimum_expected_transitions
    ):
        raise RegimeHawkesGenerationError(
            "regime transition estimate is unsupported"
        )


def _conditioning_model(
    fit: RegimeHawkesFitResultV1,
    context: RegimeHawkesWindowContextV1,
) -> tuple[str, str, Mapping[str, Any]]:
    models = cast(Mapping[str, Any], fit.parameters["conditioning_models"])
    exact = _exact_key(context.technology_label, context.session)
    if exact in models:
        return (
            exact,
            "exact_technology_session",
            cast(Mapping[str, Any], models[exact]),
        )
    session = _session_key(context.session)
    if session in models:
        return (
            session,
            "session_backoff",
            cast(Mapping[str, Any], models[session]),
        )
    raise RegimeHawkesGenerationError("no supported regime conditioning model")


def _closed_generation_evidence(
    fit: RegimeHawkesFitResultV1,
    window: ReconstructionWindowV1,
    ensemble_member_id: str,
    *,
    status: RegimeHawkesGenerationStatus,
    reason: str,
    raw_count: int,
    history_count: int,
    context: RegimeHawkesWindowContextV1 | None,
    input_hash: str | None,
    input_content_hash: str | None,
    history_hash: str | None,
    context_hash: str | None,
    model_key: str | None,
    support_level: str,
    radius: float | None,
    processed_bins: int,
    poisson_iterations: int,
    state_counts: tuple[int, int],
    transitions: int,
    final_probabilities: tuple[float, float] | None,
    initial_policy: str,
    started: float,
    before_peak: int,
) -> RegimeHawkesGenerationEvidenceV1:
    return RegimeHawkesGenerationEvidenceV1(
        fit_id=fit.fit_id,
        window_id=window.window_id,
        window_context_id=context.context_id if context is not None else None,
        ensemble_member_id=ensemble_member_id,
        status=status,
        attempted=True,
        input_event_count=raw_count,
        history_event_count=history_count,
        generated_event_count=0,
        processed_bin_count=processed_bins,
        poisson_iteration_count=poisson_iterations,
        state_bin_counts=state_counts,
        state_transition_count=transitions,
        initial_state_policy=initial_policy,
        final_filtered_probabilities=final_probabilities,
        input_anchor_sha256=input_hash,
        input_event_content_sha256=input_content_hash,
        history_content_sha256=history_hash,
        window_context_sha256=context_hash,
        conditioning_support_level=support_level,
        conditioning_model_key=model_key,
        maximum_spectral_radius=radius,
        lineage_content_sha256=None,
        wall_time_ms=round((time.perf_counter() - started) * 1000),
        peak_memory_bytes=_incremental_peak_rss_bytes(before_peak),
        failure_reason=reason,
    )


def _validated_history(
    values: Sequence[BenchmarkEventV1],
    *,
    config: RegimeHawkesConfigV1,
    window: ReconstructionWindowV1,
) -> tuple[BenchmarkEventV1, ...]:
    supplied = tuple(values)
    if len(supplied) > config.limits.max_history_events:
        raise RegimeHawkesGenerationError("history event limit exceeded")
    if any(not isinstance(item, BenchmarkEventV1) for item in supplied):
        raise RegimeHawkesGenerationError(
            "history contains a non-benchmark event"
        )
    if any(item.event_time_ns >= window.core_start_ns for item in supplied):
        raise RegimeHawkesGenerationError("history is not prior-only")
    if any(
        item.symbol not in {value.upper() for value in window.symbols}
        for item in supplied
    ):
        raise RegimeHawkesGenerationError(
            "history symbol differs from synchronized window"
        )
    lower_bound = window.core_start_ns - config.limits.max_history_ns
    retained = tuple(
        sorted(
            (item for item in supplied if item.event_time_ns >= lower_bound),
            key=_benchmark_event_key,
        )
    )
    estimated = (
        len(retained) * config.limits.estimated_bytes_per_generated_event
    )
    if estimated > config.limits.max_peak_memory_bytes:
        raise RegimeHawkesGenerationError(
            "history memory estimate exceeds limit"
        )
    return retained


def _active_anchor_intervals(
    anchors: Sequence[BenchmarkEventV1],
    start_ns: int,
    end_ns: int,
) -> list[tuple[int, int, BenchmarkEventV1, BenchmarkEventV1]]:
    result = []
    for left, right in pairwise(anchors):
        left_time = max(start_ns, left.event_time_ns + 1)
        right_time = min(end_ns, right.event_time_ns)
        if left_time < right_time:
            result.append((left_time, right_time, left, right))
    return result


def _uniform_interval_times(
    intervals: Sequence[tuple[int, int, BenchmarkEventV1, BenchmarkEventV1]],
    count: int,
    rng: random.Random,
) -> tuple[int, ...]:
    if count <= 0:
        return ()
    lengths = [right - left for left, right, _, _ in intervals]
    total = sum(lengths)
    selected: set[int] = set()
    maximum_attempts = max(64, count * 32)
    for _ in range(maximum_attempts):
        if len(selected) == count:
            break
        position = rng.randrange(total)
        offset = 0
        for (left, right, _, _), length in zip(intervals, lengths):
            if position < offset + length:
                selected.add(left + position - offset)
                break
            offset += length
    if len(selected) != count:
        available = (
            value
            for left, right, _, _ in intervals
            for value in range(left, right)
        )
        for value in available:
            selected.add(value)
            if len(selected) == count:
                break
    if len(selected) != count:
        raise RegimeHawkesGenerationError(
            "unable to allocate unique candidate times"
        )
    return tuple(sorted(selected))


def _bracketing_anchors(
    anchors: Sequence[BenchmarkEventV1], event_time_ns: int
) -> tuple[BenchmarkEventV1, BenchmarkEventV1]:
    for left, right in pairwise(anchors):
        if left.event_time_ns < event_time_ns < right.event_time_ns:
            return left, right
    raise RegimeHawkesGenerationError(
        "candidate lacks strict destination anchors"
    )


def _project_quote(
    left: BenchmarkEventV1,
    right: BenchmarkEventV1,
    event_time_ns: int,
    mark: str,
) -> tuple[float, float]:
    fraction = (event_time_ns - left.event_time_ns) / (
        right.event_time_ns - left.event_time_ns
    )
    target_bid = left.bid + fraction * (right.bid - left.bid)
    target_ask = left.ask + fraction * (right.ask - left.ask)
    if mark == "ask_only":
        bid, ask = left.bid, max(left.bid, target_ask)
    elif mark == "bid_only":
        bid, ask = min(target_bid, left.ask), left.ask
    elif mark == "unchanged":
        bid, ask = left.bid, left.ask
    else:
        bid, ask = target_bid, target_ask
    if bid <= 0.0 or ask < bid:
        raise RegimeHawkesGenerationError("quote projection is invalid")
    return bid, ask


def _event_mark(
    event: BenchmarkEventV1, prior: tuple[float, float] | None
) -> str:
    if prior is None:
        return "unchanged"
    bid_changed = event.bid != prior[0]
    ask_changed = event.ask != prior[1]
    if bid_changed and ask_changed:
        return "joint"
    if bid_changed:
        return "bid_only"
    if ask_changed:
        return "ask_only"
    return "unchanged"


def _sample_mark(probabilities: Mapping[str, float], rng: random.Random) -> str:
    values = [_finite_float(probabilities[name], name) for name in MARK_STATES]
    return MARK_STATES[_sample_index(values, rng)]


def _sample_index(probabilities: Sequence[float], rng: random.Random) -> int:
    normalized = [value / sum(probabilities) for value in probabilities]
    draw = rng.random()
    cumulative = 0.0
    for index, probability in enumerate(normalized):
        cumulative += probability
        if draw <= cumulative:
            return index
    return len(normalized) - 1


def _poisson(
    mean: float, rng: random.Random, maximum_iterations: int
) -> tuple[int, int]:
    if not math.isfinite(mean) or mean < 0.0:
        raise RegimeHawkesGenerationError("Poisson mean is invalid")
    if mean == 0.0:
        return 0, 0
    if mean < 30.0:
        threshold = math.exp(-mean)
        product = 1.0
        count = 0
        while product > threshold:
            count += 1
            if count > maximum_iterations:
                raise RegimeHawkesGenerationError(
                    "Poisson iteration limit exceeded"
                )
            product *= rng.random()
        return count - 1, count
    square_root = math.sqrt(mean)
    log_mean = math.log(mean)
    b = 0.931 + 2.53 * square_root
    a = -0.059 + 0.02483 * b
    inverse_alpha = 1.1239 + 1.1328 / (b - 3.4)
    squeeze = 0.9277 - 3.6224 / (b - 2.0)
    for iteration in range(1, maximum_iterations + 1):
        centered_uniform = rng.random() - 0.5
        uniform = rng.random()
        distance = 0.5 - abs(centered_uniform)
        if distance <= 0.0:
            continue
        candidate = math.floor(
            (2.0 * a / distance + b) * centered_uniform + mean + 0.43
        )
        if candidate >= 0 and distance >= 0.07 and uniform <= squeeze:
            return candidate, iteration
        if candidate < 0 or (distance < 0.013 and uniform > distance):
            continue
        if uniform == 0.0:
            return candidate, iteration
        acceptance = -mean + candidate * log_mean - math.lgamma(candidate + 1.0)
        proposal = (
            math.log(uniform)
            + math.log(inverse_alpha)
            - math.log(a / (distance * distance) + b)
        )
        if proposal <= acceptance:
            return candidate, iteration
    raise RegimeHawkesGenerationError(
        "Poisson transformed rejection exceeded iteration limit"
    )


def _missing_intensity_scale(scenario: BenchmarkScenarioV1) -> float:
    raw = scenario.degradation_parameters.get("retention_probability", 1.0)
    retention = _finite_float(raw, "retention_probability")
    if not 0.0 < retention <= 1.0:
        raise RegimeHawkesGenerationError(
            "retention probability is outside (0,1]"
        )
    return max(0.0, (1.0 - retention) / retention)


def _calibration_hash(windows: Sequence[EventClockCalibrationWindowV1]) -> str:
    payload = [
        item.metadata()
        for item in sorted(windows, key=lambda value: value.window_id)
    ]
    return hashlib.sha256(canonical_contract_json(payload).encode()).hexdigest()


def _context_hash(contexts: Sequence[RegimeHawkesWindowContextV1]) -> str:
    payload = [
        item.to_dict()
        for item in sorted(contexts, key=lambda value: value.window_id)
    ]
    return hashlib.sha256(canonical_contract_json(payload).encode()).hexdigest()


def _generation_model_sha256(fit: RegimeHawkesFitResultV1) -> str:
    """Bind stochastic generation only to fitted inputs and usable parameters."""
    payload: dict[str, JSONValue] = {
        "config_id": fit.config_id,
        "calibration_content_sha256": fit.calibration_content_sha256,
        "calibration_context_sha256": fit.calibration_context_sha256,
        "information_mode": fit.information_mode.value,
        "as_of_ns": fit.as_of_ns,
        "symbols": list(fit.symbols),
        "parameters": dict(fit.parameters),
    }
    return hashlib.sha256(canonical_contract_json(payload).encode()).hexdigest()


def _diagnostic_payload_bytes(
    uncertainty: Mapping[str, JSONValue],
    diagnostics: Mapping[str, JSONScalar],
    state_diagnostics: Mapping[str, JSONValue],
) -> int:
    return len(
        canonical_contract_json(
            {
                "uncertainty": dict(uncertainty),
                "diagnostics": dict(diagnostics),
                "state_diagnostics": dict(state_diagnostics),
            }
        ).encode()
    )


def _benchmark_content_sha256(events: Sequence[BenchmarkEventV1]) -> str:
    payload = [
        item.to_dict() for item in sorted(events, key=_benchmark_event_key)
    ]
    return hashlib.sha256(canonical_contract_json(payload).encode()).hexdigest()


def _benchmark_anchor_sha256(events: Sequence[BenchmarkEventV1]) -> str:
    return _anchor_sha256(
        (
            item.symbol,
            item.event_time_ns,
            item.event_sequence,
            item.bid,
            item.ask,
        )
        for item in events
        if not item.sparsity.startswith("regime-hawkes-")
    )


def _synthetic_anchor_sha256(events: Sequence[SyntheticEventV1]) -> str:
    return _anchor_sha256(
        (
            item.symbol,
            item.event_time_ns,
            item.event_sequence,
            item.bid,
            item.ask,
        )
        for item in events
    )


def _anchor_sha256(values: Any) -> str:
    normalized = sorted(
        (
            str(symbol).lower(),
            int(event_time_ns),
            int(event_sequence),
            float(bid),
            float(ask),
        )
        for symbol, event_time_ns, event_sequence, bid, ask in values
    )
    return hashlib.sha256(
        canonical_contract_json(normalized).encode()
    ).hexdigest()


def _lineage_sha256(values: Sequence[RegimeHawkesGenerationLineageV1]) -> str:
    return hashlib.sha256(
        canonical_contract_json(
            [
                item.to_dict()
                for item in sorted(
                    values, key=lambda item: item.source_event_id
                )
            ]
        ).encode()
    ).hexdigest()


def _semantic_seed(base_seed: int, *values: str) -> int:
    digest = hashlib.sha256(
        canonical_contract_json([base_seed, *values]).encode()
    ).digest()
    return int.from_bytes(digest[:8], "big", signed=False)


def _generator_id(modulation: RegimeHawkesModulation) -> str:
    return f"{REGIME_HAWKES_GENERATOR_PREFIX}.{modulation.value}"


def _exact_key(technology_label: str, session: str) -> str:
    return f"exact|{technology_label}|{session}"


def _session_key(session: str) -> str:
    return f"session|{session}"


def _benchmark_event_key(event: BenchmarkEventV1) -> tuple[int, str, int, str]:
    return (
        event.event_time_ns,
        event.symbol,
        event.event_sequence,
        event.benchmark_event_id,
    )


def _incremental_peak_rss_bytes(before_peak: int) -> int:
    return int(max(0, peak_rss_bytes() - before_peak))


def _median(values: Sequence[float]) -> float:
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return 0.5 * (ordered[middle - 1] + ordered[middle])


def _valid_period(value: str) -> bool:
    return len(value) == 6 and value.isdigit() and 1 <= int(value[4:]) <= 12


def _normalized_pair(values: Sequence[float]) -> list[float]:
    normalized = _probability_pair(values, "probability pair")
    return [normalized[0], normalized[1]]


def _probability_pair(values: Sequence[Any], name: str) -> tuple[float, float]:
    parsed = tuple(_finite_float(value, name) for value in values)
    if len(parsed) != 2 or any(value < 0.0 for value in parsed):
        raise ValueError(f"{name} requires two nonnegative values")
    total = sum(parsed)
    if total <= 0.0:
        raise ValueError(f"{name} has zero mass")
    normalized = (parsed[0] / total, parsed[1] / total)
    # Construction is strict; internal callers normalize explicitly.
    if abs(total - 1.0) > 1e-8 and name != "probability pair":
        raise ValueError(f"{name} must sum to one")
    return normalized


def _float_vector(value: Any, size: int, name: str) -> list[float]:
    if (
        not isinstance(value, Sequence)
        or isinstance(value, (str, bytes))
        or len(value) != size
    ):
        raise ValueError(f"{name} has wrong dimension")
    return [_finite_float(item, name) for item in value]


def _float_matrix(value: Any, size: int, name: str) -> list[list[float]]:
    if (
        not isinstance(value, Sequence)
        or isinstance(value, (str, bytes))
        or len(value) != size
    ):
        raise ValueError(f"{name} has wrong dimension")
    return [_float_vector(row, size, name) for row in value]


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode()
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _require_schema(data: Mapping[str, Any], expected: str) -> None:
    if data.get("schema_version") != expected:
        raise ValueError(f"unsupported schema; expected {expected}")


def _require_schema_value(value: str, expected: str, name: str) -> None:
    if value != expected:
        raise ValueError(f"unsupported {name} schema")


def _require_literal(data: Mapping[str, Any], key: str, expected: str) -> None:
    if data.get(key) != expected:
        raise ValueError(f"{key} differs from fixed contract")


def _required_text(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("required text is empty")
    return value.strip()


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    return _required_text(value)


def _strict_bool(value: Any, name: str) -> bool:
    if type(value) is not bool:
        raise TypeError(f"{name} must be boolean")
    return value


def _strict_int(value: Any, name: str) -> int:
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    return value


def _bounded_int(value: Any, name: str, lower: int, upper: int) -> int:
    parsed = _strict_int(value, name)
    if not lower <= parsed <= upper:
        raise ValueError(f"{name} is outside bounds")
    return parsed


def _finite_float(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be numeric")
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f"{name} must be finite")
    return parsed


def _positive_float(value: Any, name: str) -> float:
    parsed = _finite_float(value, name)
    if parsed <= 0.0:
        raise ValueError(f"{name} must be positive")
    return parsed


def _optional_float(value: Any) -> float | None:
    return None if value is None else _finite_float(value, "optional float")


def _optional_int(value: Any) -> int | None:
    return None if value is None else _strict_int(value, "optional int")


def _sequence(value: Any) -> tuple[Any, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError("value must be a sequence")
    return tuple(value)


def _string_tuple(value: Any) -> tuple[str, ...]:
    return tuple(_required_text(item) for item in _sequence(value))


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    return value


def _symbol(value: Any) -> str:
    text = _required_text(value).upper()
    if not text.isalpha() or len(text) > 16:
        raise ValueError("symbol is invalid")
    return text


def _sha256(value: Any, name: str) -> str:
    text = _required_text(value)
    if len(text) != 64 or any(
        character not in "0123456789abcdef" for character in text
    ):
        raise ValueError(f"{name} must be lowercase sha256")
    return text


def _optional_sha256(value: Any, name: str) -> str | None:
    return None if value is None else _sha256(value, name)


def _json_mapping(values: Mapping[str, Any], name: str) -> dict[str, JSONValue]:
    if not isinstance(values, Mapping):
        raise TypeError(f"{name} must be a mapping")
    encoded = canonical_contract_json(dict(values))
    restored = json.loads(encoded)
    if not isinstance(restored, dict):
        raise TypeError(f"{name} must encode as an object")
    return cast(dict[str, JSONValue], restored)


def _json_scalar(value: Any, name: str) -> JSONScalar:
    if value is None or isinstance(value, (str, bool)):
        return cast(JSONScalar, value)
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    raise TypeError(f"{name} must be a JSON scalar")
