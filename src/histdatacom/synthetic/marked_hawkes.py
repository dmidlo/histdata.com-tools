"""Bounded marked multivariate Hawkes challengers for reconstruction research.

This module is an opt-in research surface.  It fits calibration-only,
exponential-kernel Hawkes models, simulates one synchronized multi-symbol
timeline, and exposes deterministic evidence through the existing benchmark
and generator-neutral carving seams.  It never selects or replaces the
production reconstruction generator.
"""

from __future__ import annotations

import hashlib
import json
import math
import random
import time
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from itertools import groupby, pairwise
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

MARKED_HAWKES_RESOURCE_LIMITS_SCHEMA_VERSION = (
    "histdatacom.marked-hawkes-resource-limits.v1"
)
MARKED_HAWKES_CONFIG_SCHEMA_VERSION = "histdatacom.marked-hawkes-config.v1"
MARKED_HAWKES_FIT_RESULT_SCHEMA_VERSION = (
    "histdatacom.marked-hawkes-fit-result.v1"
)
MARKED_HAWKES_GENERATION_EVIDENCE_SCHEMA_VERSION = (
    "histdatacom.marked-hawkes-generation-evidence.v1"
)
MARKED_HAWKES_GENERATION_LINEAGE_SCHEMA_VERSION = (
    "histdatacom.marked-hawkes-generation-lineage.v1"
)
MARKED_HAWKES_CANDIDATE_LINEAGE_SCHEMA_VERSION = (
    "histdatacom.marked-hawkes-candidate-lineage.v1"
)
MARKED_HAWKES_CANDIDATE_BATCH_SCHEMA_VERSION = (
    "histdatacom.marked-hawkes-candidate-batch.v1"
)
MARKED_HAWKES_IMPLEMENTATION_VERSION = "1.0.0"
MARKED_HAWKES_GENERATOR_PREFIX = "histdatacom.marked-hawkes"

NANOSECONDS_PER_SECOND = 1_000_000_000
MAX_HAWKES_FIT_EVENTS = 100_000
MAX_HAWKES_FIT_WINDOWS = 256
MAX_HAWKES_ITERATIONS = 512
MAX_HAWKES_DIMENSIONS = 16
MAX_HAWKES_CONDITIONING_CELLS = 256
MAX_HAWKES_GENERATED_EVENTS = 100_000
MAX_HAWKES_PROPOSALS = 1_000_000
MAX_HAWKES_HISTORY_EVENTS = 100_000
MAX_HAWKES_HISTORY_NS = 7 * 86_400 * NANOSECONDS_PER_SECOND
MAX_HAWKES_PARAMETERS_BYTES = 4_000_000
MAX_HAWKES_DIAGNOSTICS = 128
MARK_STATES = ("ask_only", "bid_only", "joint", "unchanged")


class HawkesExcitationStructure(str, Enum):
    """Predeclared nested ablations for excitation value."""

    ZERO = "zero_excitation"
    DIAGONAL = "diagonal_self_excitation"
    FULL = "full_self_cross_excitation"


class MarkedHawkesFitStatus(str, Enum):
    """Terminal state of one bounded calibration attempt."""

    FITTED = "fitted"
    REFUSED = "refused"
    FAILED = "failed"


class MarkedHawkesGenerationStatus(str, Enum):
    """Terminal state of one synchronized simulation attempt."""

    GENERATED = "generated"
    EMPTY = "empty"
    REFUSED = "refused"
    FAILED = "failed"


class MarkedHawkesFitError(ValueError):
    """Raised when an unusable fit is bound as a benchmark generator."""


class MarkedHawkesGenerationError(ValueError):
    """Raised when simulation violates model or resource contracts."""


@dataclass(frozen=True, slots=True)
class MarkedHawkesResourceLimitsV1:
    """Hard fit, parameter, history, proposal, and output limits."""

    max_fit_events: int = 20_000
    max_fit_windows: int = 96
    max_iterations: int = 256
    max_dimensions: int = 8
    max_conditioning_cells: int = 64
    max_generated_events_per_interval: int = 1_024
    max_generated_events_per_window: int = 8_192
    max_ogata_proposals: int = 100_000
    max_history_events: int = 4_096
    max_history_ns: int = 3_600 * NANOSECONDS_PER_SECOND
    max_candidate_amplification: float = 8.0
    max_parameters_bytes: int = 2_000_000
    max_peak_memory_bytes: int = 512 * 1024**2
    max_wall_time_ms: int = 60_000
    max_diagnostics: int = 64
    estimated_bytes_per_fit_event: int = 1_024
    estimated_bytes_per_generated_event: int = 1_024
    limits_id: str = ""
    schema_version: str = MARKED_HAWKES_RESOURCE_LIMITS_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            MARKED_HAWKES_RESOURCE_LIMITS_SCHEMA_VERSION,
            "marked Hawkes resource limits",
        )
        bounds = (
            ("max_fit_events", self.max_fit_events, 2, MAX_HAWKES_FIT_EVENTS),
            (
                "max_fit_windows",
                self.max_fit_windows,
                1,
                MAX_HAWKES_FIT_WINDOWS,
            ),
            ("max_iterations", self.max_iterations, 1, MAX_HAWKES_ITERATIONS),
            ("max_dimensions", self.max_dimensions, 1, MAX_HAWKES_DIMENSIONS),
            (
                "max_conditioning_cells",
                self.max_conditioning_cells,
                1,
                MAX_HAWKES_CONDITIONING_CELLS,
            ),
            (
                "max_generated_events_per_interval",
                self.max_generated_events_per_interval,
                1,
                MAX_HAWKES_GENERATED_EVENTS,
            ),
            (
                "max_generated_events_per_window",
                self.max_generated_events_per_window,
                1,
                MAX_HAWKES_GENERATED_EVENTS,
            ),
            (
                "max_ogata_proposals",
                self.max_ogata_proposals,
                1,
                MAX_HAWKES_PROPOSALS,
            ),
            (
                "max_history_events",
                self.max_history_events,
                0,
                MAX_HAWKES_HISTORY_EVENTS,
            ),
            (
                "max_history_ns",
                self.max_history_ns,
                0,
                MAX_HAWKES_HISTORY_NS,
            ),
            (
                "max_parameters_bytes",
                self.max_parameters_bytes,
                1_024,
                MAX_HAWKES_PARAMETERS_BYTES,
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
                MAX_HAWKES_DIAGNOSTICS,
            ),
            (
                "estimated_bytes_per_fit_event",
                self.estimated_bytes_per_fit_event,
                64,
                1_000_000,
            ),
            (
                "estimated_bytes_per_generated_event",
                self.estimated_bytes_per_generated_event,
                64,
                1_000_000,
            ),
        )
        for name, value, lower, upper in bounds:
            _bounded_int(value, name, lower, upper)
        if self.max_generated_events_per_interval > (
            self.max_generated_events_per_window
        ):
            raise ValueError("per-interval Hawkes limit exceeds window limit")
        amplification = _positive_float(
            self.max_candidate_amplification,
            "max_candidate_amplification",
        )
        if amplification > 1_000.0:
            raise ValueError("candidate amplification exceeds hard bound")
        expected = _stable_id(
            "marked-hawkes-resource-limits", self.identity_payload()
        )
        if self.limits_id and self.limits_id != expected:
            raise ValueError("marked Hawkes limits_id differs")
        object.__setattr__(self, "limits_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "max_fit_events": self.max_fit_events,
            "max_fit_windows": self.max_fit_windows,
            "max_iterations": self.max_iterations,
            "max_dimensions": self.max_dimensions,
            "max_conditioning_cells": self.max_conditioning_cells,
            "max_generated_events_per_interval": (
                self.max_generated_events_per_interval
            ),
            "max_generated_events_per_window": (
                self.max_generated_events_per_window
            ),
            "max_ogata_proposals": self.max_ogata_proposals,
            "max_history_events": self.max_history_events,
            "max_history_ns": self.max_history_ns,
            "max_candidate_amplification": self.max_candidate_amplification,
            "max_parameters_bytes": self.max_parameters_bytes,
            "max_peak_memory_bytes": self.max_peak_memory_bytes,
            "max_wall_time_ms": self.max_wall_time_ms,
            "max_diagnostics": self.max_diagnostics,
            "estimated_bytes_per_fit_event": (
                self.estimated_bytes_per_fit_event
            ),
            "estimated_bytes_per_generated_event": (
                self.estimated_bytes_per_generated_event
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "limits_id": self.limits_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> MarkedHawkesResourceLimitsV1:
        _require_schema(data, MARKED_HAWKES_RESOURCE_LIMITS_SCHEMA_VERSION)
        return cls(
            max_fit_events=_strict_int(
                data.get("max_fit_events"), "max_fit_events"
            ),
            max_fit_windows=_strict_int(
                data.get("max_fit_windows"), "max_fit_windows"
            ),
            max_iterations=_strict_int(
                data.get("max_iterations"), "max_iterations"
            ),
            max_dimensions=_strict_int(
                data.get("max_dimensions"), "max_dimensions"
            ),
            max_conditioning_cells=_strict_int(
                data.get("max_conditioning_cells"), "max_conditioning_cells"
            ),
            max_generated_events_per_interval=_strict_int(
                data.get("max_generated_events_per_interval"),
                "max_generated_events_per_interval",
            ),
            max_generated_events_per_window=_strict_int(
                data.get("max_generated_events_per_window"),
                "max_generated_events_per_window",
            ),
            max_ogata_proposals=_strict_int(
                data.get("max_ogata_proposals"), "max_ogata_proposals"
            ),
            max_history_events=_strict_int(
                data.get("max_history_events"), "max_history_events"
            ),
            max_history_ns=_strict_int(
                data.get("max_history_ns"), "max_history_ns"
            ),
            max_candidate_amplification=_finite_float(
                data.get("max_candidate_amplification"),
                "max_candidate_amplification",
            ),
            max_parameters_bytes=_strict_int(
                data.get("max_parameters_bytes"), "max_parameters_bytes"
            ),
            max_peak_memory_bytes=_strict_int(
                data.get("max_peak_memory_bytes"), "max_peak_memory_bytes"
            ),
            max_wall_time_ms=_strict_int(
                data.get("max_wall_time_ms"), "max_wall_time_ms"
            ),
            max_diagnostics=_strict_int(
                data.get("max_diagnostics"), "max_diagnostics"
            ),
            estimated_bytes_per_fit_event=_strict_int(
                data.get("estimated_bytes_per_fit_event"),
                "estimated_bytes_per_fit_event",
            ),
            estimated_bytes_per_generated_event=_strict_int(
                data.get("estimated_bytes_per_generated_event"),
                "estimated_bytes_per_generated_event",
            ),
            limits_id=str(data.get("limits_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class MarkedHawkesConfigV1:
    """Versioned exponential-kernel fit and simulation policy."""

    excitation_structure: HawkesExcitationStructure
    decay_candidates_per_second: tuple[float, ...] = (0.5, 2.0, 8.0)
    maximum_branching_ratio: float = 0.95
    convergence_tolerance: float = 1e-3
    parameter_floor: float = 1e-10
    mark_smoothing_count: float = 1.0
    minimum_events_per_symbol: int = 8
    minimum_conditioning_events: int = 2
    base_seed: int = 451_000
    limits: MarkedHawkesResourceLimitsV1 = field(
        default_factory=MarkedHawkesResourceLimitsV1
    )
    config_id: str = ""
    schema_version: str = MARKED_HAWKES_CONFIG_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            MARKED_HAWKES_CONFIG_SCHEMA_VERSION,
            "marked Hawkes config",
        )
        object.__setattr__(
            self,
            "excitation_structure",
            HawkesExcitationStructure(self.excitation_structure),
        )
        decays = tuple(
            sorted(
                {
                    _positive_float(value, "decay candidate")
                    for value in self.decay_candidates_per_second
                }
            )
        )
        if not decays or len(decays) > 16 or decays[-1] > 1_000_000.0:
            raise ValueError("decay candidate grid is outside bounds")
        object.__setattr__(self, "decay_candidates_per_second", decays)
        ratio = _positive_float(
            self.maximum_branching_ratio, "maximum_branching_ratio"
        )
        if ratio >= 1.0:
            raise ValueError("Hawkes branching-ratio limit must be below one")
        tolerance = _positive_float(
            self.convergence_tolerance, "convergence_tolerance"
        )
        if tolerance >= 1.0:
            raise ValueError("convergence tolerance must be below one")
        floor = _positive_float(self.parameter_floor, "parameter_floor")
        if floor >= 1.0:
            raise ValueError("parameter floor must be below one")
        _positive_float(self.mark_smoothing_count, "mark_smoothing_count")
        _bounded_int(
            self.minimum_events_per_symbol,
            "minimum_events_per_symbol",
            2,
            MAX_HAWKES_FIT_EVENTS,
        )
        _bounded_int(
            self.minimum_conditioning_events,
            "minimum_conditioning_events",
            1,
            MAX_HAWKES_FIT_EVENTS,
        )
        _bounded_int(self.base_seed, "base_seed", 0, 2**63 - 1)
        if not isinstance(self.limits, MarkedHawkesResourceLimitsV1):
            raise TypeError("marked Hawkes config requires v1 resource limits")
        expected = _stable_id("marked-hawkes-config", self.identity_payload())
        if self.config_id and self.config_id != expected:
            raise ValueError("marked Hawkes config_id differs")
        object.__setattr__(self, "config_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "excitation_structure": self.excitation_structure.value,
            "kernel": "exponential-integrated-mass-v1",
            "decay_candidates_per_second": list(
                self.decay_candidates_per_second
            ),
            "maximum_branching_ratio": self.maximum_branching_ratio,
            "convergence_tolerance": self.convergence_tolerance,
            "parameter_floor": self.parameter_floor,
            "mark_policy": "quote-transition-source-destination-v1",
            "mark_states": list(MARK_STATES),
            "mark_smoothing_count": self.mark_smoothing_count,
            "minimum_events_per_symbol": self.minimum_events_per_symbol,
            "minimum_conditioning_events": (self.minimum_conditioning_events),
            "conditioning_policy": "exact-epoch-session-then-session-v1",
            "uncertainty_method": "responsibility-count-wald-95-v1",
            "fit_boundary_policy": "reset-each-calibration-window-v1",
            "generation_algorithm": "bounded-ogata-thinning-v1",
            "base_seed": self.base_seed,
            "limits": self.limits.to_dict(),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "config_id": self.config_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> MarkedHawkesConfigV1:
        _require_schema(data, MARKED_HAWKES_CONFIG_SCHEMA_VERSION)
        _require_literal(data, "kernel", "exponential-integrated-mass-v1")
        _require_literal(
            data,
            "mark_policy",
            "quote-transition-source-destination-v1",
        )
        _require_literal(
            data,
            "conditioning_policy",
            "exact-epoch-session-then-session-v1",
        )
        _require_literal(
            data,
            "uncertainty_method",
            "responsibility-count-wald-95-v1",
        )
        _require_literal(
            data,
            "fit_boundary_policy",
            "reset-each-calibration-window-v1",
        )
        _require_literal(
            data, "generation_algorithm", "bounded-ogata-thinning-v1"
        )
        if _string_tuple(data.get("mark_states")) != MARK_STATES:
            raise ValueError("marked Hawkes mark-state registry differs")
        return cls(
            excitation_structure=HawkesExcitationStructure(
                str(data.get("excitation_structure", ""))
            ),
            decay_candidates_per_second=tuple(
                _finite_float(value, "decay candidate")
                for value in _sequence(data.get("decay_candidates_per_second"))
            ),
            maximum_branching_ratio=_finite_float(
                data.get("maximum_branching_ratio"),
                "maximum_branching_ratio",
            ),
            convergence_tolerance=_finite_float(
                data.get("convergence_tolerance"), "convergence_tolerance"
            ),
            parameter_floor=_finite_float(
                data.get("parameter_floor"), "parameter_floor"
            ),
            mark_smoothing_count=_finite_float(
                data.get("mark_smoothing_count"), "mark_smoothing_count"
            ),
            minimum_events_per_symbol=_strict_int(
                data.get("minimum_events_per_symbol"),
                "minimum_events_per_symbol",
            ),
            minimum_conditioning_events=_strict_int(
                data.get("minimum_conditioning_events"),
                "minimum_conditioning_events",
            ),
            base_seed=_strict_int(data.get("base_seed"), "base_seed"),
            limits=MarkedHawkesResourceLimitsV1.from_dict(
                _mapping(data.get("limits"), "limits")
            ),
            config_id=str(data.get("config_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> MarkedHawkesConfigV1:
        return cls.from_dict(_mapping(json.loads(text), "marked Hawkes config"))


@dataclass(frozen=True, slots=True)
class MarkedHawkesFitResultV1:
    """Content-addressed bounded fit, including stability and uncertainty."""

    excitation_structure: HawkesExcitationStructure
    config_id: str
    calibration_content_sha256: str
    information_mode: InformationMode
    symbols: tuple[str, ...]
    status: MarkedHawkesFitStatus
    converged: bool
    iteration_count: int
    fitted_event_count: int
    fitted_window_count: int
    log_likelihood: float | None
    parameters: Mapping[str, JSONValue]
    uncertainty: Mapping[str, JSONValue]
    diagnostics: Mapping[str, JSONScalar]
    estimated_peak_memory_bytes: int
    failure_reason: str | None = None
    as_of_ns: int | None = None
    fit_id: str = ""
    schema_version: str = MARKED_HAWKES_FIT_RESULT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            MARKED_HAWKES_FIT_RESULT_SCHEMA_VERSION,
            "marked Hawkes fit",
        )
        object.__setattr__(
            self,
            "excitation_structure",
            HawkesExcitationStructure(self.excitation_structure),
        )
        object.__setattr__(self, "config_id", _required_text(self.config_id))
        _sha256(self.calibration_content_sha256, "calibration_content_sha256")
        mode = InformationMode.from_value(self.information_mode)
        object.__setattr__(self, "information_mode", mode)
        symbols = tuple(sorted({_symbol(item) for item in self.symbols}))
        if not symbols:
            raise ValueError("marked Hawkes fit requires symbols")
        object.__setattr__(self, "symbols", symbols)
        status = MarkedHawkesFitStatus(self.status)
        object.__setattr__(self, "status", status)
        converged = _strict_bool(self.converged, "converged")
        object.__setattr__(self, "converged", converged)
        for name in (
            "iteration_count",
            "fitted_event_count",
            "fitted_window_count",
            "estimated_peak_memory_bytes",
        ):
            if _strict_int(getattr(self, name), name) < 0:
                raise ValueError(f"{name} must be nonnegative")
        likelihood = self.log_likelihood
        if likelihood is not None:
            object.__setattr__(
                self,
                "log_likelihood",
                _finite_float(likelihood, "log_likelihood"),
            )
        parameters = _json_mapping(self.parameters, "parameters")
        uncertainty = _json_mapping(self.uncertainty, "uncertainty")
        diagnostics = {
            _required_text(str(key)): _json_scalar(value, str(key))
            for key, value in self.diagnostics.items()
        }
        if len(diagnostics) > MAX_HAWKES_DIAGNOSTICS:
            raise ValueError("marked Hawkes diagnostics exceed hard bound")
        if len(canonical_contract_json(parameters).encode()) > (
            MAX_HAWKES_PARAMETERS_BYTES
        ):
            raise ValueError("marked Hawkes parameters exceed hard bound")
        object.__setattr__(self, "parameters", parameters)
        object.__setattr__(self, "uncertainty", uncertainty)
        object.__setattr__(
            self, "diagnostics", dict(sorted(diagnostics.items()))
        )
        failure = _optional_text(self.failure_reason)
        object.__setattr__(self, "failure_reason", failure)
        if status is MarkedHawkesFitStatus.FITTED:
            if not converged or failure is not None or not parameters:
                raise ValueError("fitted Hawkes result lacks converged model")
            if not uncertainty:
                raise ValueError("fitted Hawkes result lacks uncertainty")
            _validate_fitted_parameters(
                parameters,
                excitation_structure=self.excitation_structure,
                expected_symbols=symbols,
            )
            _validate_uncertainty(uncertainty, parameters=parameters)
        elif converged or failure is None or parameters or uncertainty:
            raise ValueError(
                "failed/refused Hawkes fit must contain no usable parameters"
            )
        if mode is InformationMode.EX_ANTE_SIMULATION:
            if self.as_of_ns is None:
                raise ValueError("ex-ante Hawkes fit requires as_of_ns")
            _strict_int(self.as_of_ns, "as_of_ns")
        elif self.as_of_ns is not None:
            raise ValueError("ex-post Hawkes fit rejects as_of_ns")
        expected = _stable_id("marked-hawkes-fit", self.identity_payload())
        if self.fit_id and self.fit_id != expected:
            raise ValueError("marked Hawkes fit_id differs")
        object.__setattr__(self, "fit_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "excitation_structure": self.excitation_structure.value,
            "config_id": self.config_id,
            "calibration_content_sha256": self.calibration_content_sha256,
            "information_mode": self.information_mode.value,
            "as_of_ns": self.as_of_ns,
            "symbols": list(self.symbols),
            "status": self.status.value,
            "converged": self.converged,
            "iteration_count": self.iteration_count,
            "fitted_event_count": self.fitted_event_count,
            "fitted_window_count": self.fitted_window_count,
            "log_likelihood": self.log_likelihood,
            "parameters": dict(self.parameters),
            "uncertainty": dict(self.uncertainty),
            "diagnostics": dict(self.diagnostics),
            "estimated_peak_memory_bytes": self.estimated_peak_memory_bytes,
            "failure_reason": self.failure_reason,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "fit_id": self.fit_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> MarkedHawkesFitResultV1:
        _require_schema(data, MARKED_HAWKES_FIT_RESULT_SCHEMA_VERSION)
        return cls(
            excitation_structure=HawkesExcitationStructure(
                str(data.get("excitation_structure", ""))
            ),
            config_id=str(data.get("config_id", "")),
            calibration_content_sha256=str(
                data.get("calibration_content_sha256", "")
            ),
            information_mode=InformationMode.from_value(
                str(data.get("information_mode", ""))
            ),
            as_of_ns=_optional_int(data.get("as_of_ns")),
            symbols=_string_tuple(data.get("symbols")),
            status=MarkedHawkesFitStatus(str(data.get("status", ""))),
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
            estimated_peak_memory_bytes=_strict_int(
                data.get("estimated_peak_memory_bytes"),
                "estimated_peak_memory_bytes",
            ),
            failure_reason=_optional_text(data.get("failure_reason")),
            fit_id=str(data.get("fit_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> MarkedHawkesFitResultV1:
        return cls.from_dict(_mapping(json.loads(text), "marked Hawkes fit"))


@dataclass(frozen=True, slots=True)
class MarkedHawkesGenerationLineageV1:
    """Process-local parent-component and mark evidence for one proposal."""

    source_event_id: str
    destination_symbol: str
    excitation_source_symbol: str | None
    event_state: str
    conditional_intensity: float
    lineage_id: str = ""
    schema_version: str = MARKED_HAWKES_GENERATION_LINEAGE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            MARKED_HAWKES_GENERATION_LINEAGE_SCHEMA_VERSION,
            "marked Hawkes generation lineage",
        )
        object.__setattr__(
            self, "source_event_id", _required_text(self.source_event_id)
        )
        object.__setattr__(
            self, "destination_symbol", _symbol(self.destination_symbol)
        )
        source = self.excitation_source_symbol
        if source is not None:
            source = _symbol(source)
        object.__setattr__(self, "excitation_source_symbol", source)
        if self.event_state not in MARK_STATES:
            raise ValueError("marked Hawkes lineage mark is unsupported")
        _positive_float(self.conditional_intensity, "conditional_intensity")
        expected = _stable_id(
            "marked-hawkes-generation-lineage", self.identity_payload()
        )
        if self.lineage_id and self.lineage_id != expected:
            raise ValueError("marked Hawkes generation lineage_id differs")
        object.__setattr__(self, "lineage_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "source_event_id": self.source_event_id,
            "destination_symbol": self.destination_symbol,
            "excitation_source_symbol": self.excitation_source_symbol,
            "event_state": self.event_state,
            "conditional_intensity": self.conditional_intensity,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "lineage_id": self.lineage_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> MarkedHawkesGenerationLineageV1:
        _require_schema(data, MARKED_HAWKES_GENERATION_LINEAGE_SCHEMA_VERSION)
        return cls(
            source_event_id=str(data.get("source_event_id", "")),
            destination_symbol=str(data.get("destination_symbol", "")),
            excitation_source_symbol=_optional_text(
                data.get("excitation_source_symbol")
            ),
            event_state=str(data.get("event_state", "")),
            conditional_intensity=_finite_float(
                data.get("conditional_intensity"), "conditional_intensity"
            ),
            lineage_id=str(data.get("lineage_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class MarkedHawkesGenerationEvidenceV1:
    """Bounded measured evidence for one synchronized Ogata attempt."""

    fit_id: str
    window_id: str
    ensemble_member_id: str
    status: MarkedHawkesGenerationStatus
    attempted: bool
    generated_event_count: int
    input_event_count: int
    history_event_count: int
    proposal_count: int
    input_anchor_sha256: str | None
    conditioning_support_level: str
    conditioning_model_key: str | None
    spectral_radius: float | None
    lineage_content_sha256: str | None
    wall_time_ms: int
    peak_memory_bytes: int
    failure_reason: str | None = None
    evidence_id: str = ""
    schema_version: str = MARKED_HAWKES_GENERATION_EVIDENCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            MARKED_HAWKES_GENERATION_EVIDENCE_SCHEMA_VERSION,
            "marked Hawkes generation evidence",
        )
        for name in ("fit_id", "window_id", "ensemble_member_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        status = MarkedHawkesGenerationStatus(self.status)
        object.__setattr__(self, "status", status)
        object.__setattr__(
            self, "attempted", _strict_bool(self.attempted, "attempted")
        )
        for name in (
            "generated_event_count",
            "input_event_count",
            "history_event_count",
            "proposal_count",
            "wall_time_ms",
            "peak_memory_bytes",
        ):
            if _strict_int(getattr(self, name), name) < 0:
                raise ValueError(f"{name} must be nonnegative")
        anchor_hash = self.input_anchor_sha256
        if anchor_hash is not None:
            anchor_hash = _sha256(anchor_hash, "input_anchor_sha256")
        object.__setattr__(self, "input_anchor_sha256", anchor_hash)
        object.__setattr__(
            self,
            "conditioning_support_level",
            _required_text(self.conditioning_support_level),
        )
        object.__setattr__(
            self,
            "conditioning_model_key",
            _optional_text(self.conditioning_model_key),
        )
        radius = self.spectral_radius
        if radius is not None:
            radius = _finite_float(radius, "spectral_radius")
            if radius < 0.0 or radius >= 1.0:
                raise ValueError("generation spectral radius is unstable")
        object.__setattr__(self, "spectral_radius", radius)
        lineage_hash = self.lineage_content_sha256
        if lineage_hash is not None:
            lineage_hash = _sha256(lineage_hash, "lineage_content_sha256")
        object.__setattr__(self, "lineage_content_sha256", lineage_hash)
        failure = _optional_text(self.failure_reason)
        object.__setattr__(self, "failure_reason", failure)
        successful = status in {
            MarkedHawkesGenerationStatus.GENERATED,
            MarkedHawkesGenerationStatus.EMPTY,
        }
        if successful:
            if failure is not None:
                raise ValueError("successful Hawkes generation has a failure")
            if (
                anchor_hash is None
                or self.conditioning_model_key is None
                or radius is None
                or lineage_hash is None
            ):
                raise ValueError(
                    "successful Hawkes generation lacks audit evidence"
                )
        elif failure is None:
            raise ValueError("failed/refused Hawkes generation needs a reason")
        if status is MarkedHawkesGenerationStatus.EMPTY and (
            self.generated_event_count != 0
        ):
            raise ValueError("empty Hawkes generation contains events")
        if status is MarkedHawkesGenerationStatus.GENERATED and (
            self.generated_event_count == 0
        ):
            raise ValueError("generated Hawkes evidence has no events")
        expected = _stable_id(
            "marked-hawkes-generation-evidence", self.identity_payload()
        )
        if self.evidence_id and self.evidence_id != expected:
            raise ValueError("marked Hawkes generation evidence_id differs")
        object.__setattr__(self, "evidence_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "fit_id": self.fit_id,
            "window_id": self.window_id,
            "ensemble_member_id": self.ensemble_member_id,
            "status": self.status.value,
            "attempted": self.attempted,
            "generated_event_count": self.generated_event_count,
            "input_event_count": self.input_event_count,
            "history_event_count": self.history_event_count,
            "proposal_count": self.proposal_count,
            "input_anchor_sha256": self.input_anchor_sha256,
            "conditioning_support_level": self.conditioning_support_level,
            "conditioning_model_key": self.conditioning_model_key,
            "spectral_radius": self.spectral_radius,
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
    ) -> MarkedHawkesGenerationEvidenceV1:
        _require_schema(data, MARKED_HAWKES_GENERATION_EVIDENCE_SCHEMA_VERSION)
        return cls(
            fit_id=str(data.get("fit_id", "")),
            window_id=str(data.get("window_id", "")),
            ensemble_member_id=str(data.get("ensemble_member_id", "")),
            status=MarkedHawkesGenerationStatus(str(data.get("status", ""))),
            attempted=_strict_bool(data.get("attempted"), "attempted"),
            generated_event_count=_strict_int(
                data.get("generated_event_count"), "generated_event_count"
            ),
            input_event_count=_strict_int(
                data.get("input_event_count"), "input_event_count"
            ),
            history_event_count=_strict_int(
                data.get("history_event_count"), "history_event_count"
            ),
            proposal_count=_strict_int(
                data.get("proposal_count"), "proposal_count"
            ),
            input_anchor_sha256=_optional_text(data.get("input_anchor_sha256")),
            conditioning_support_level=str(
                data.get("conditioning_support_level", "")
            ),
            conditioning_model_key=_optional_text(
                data.get("conditioning_model_key")
            ),
            spectral_radius=_optional_float(data.get("spectral_radius")),
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
    def from_json(cls, text: str) -> MarkedHawkesGenerationEvidenceV1:
        return cls.from_dict(
            _mapping(json.loads(text), "marked Hawkes generation evidence")
        )


@dataclass(frozen=True, slots=True)
class MarkedHawkesGenerationResultV1:
    """Process-local synchronized rows, compact lineage, and evidence."""

    events: tuple[BenchmarkEventV1, ...]
    event_lineage: tuple[MarkedHawkesGenerationLineageV1, ...]
    evidence: MarkedHawkesGenerationEvidenceV1

    def __post_init__(self) -> None:
        if any(not isinstance(item, BenchmarkEventV1) for item in self.events):
            raise TypeError("Hawkes generation result contains invalid events")
        lineages = tuple(
            sorted(self.event_lineage, key=lambda item: item.source_event_id)
        )
        if any(
            not isinstance(item, MarkedHawkesGenerationLineageV1)
            for item in lineages
        ):
            raise TypeError("Hawkes generation result contains invalid lineage")
        generated_ids = {
            item.source_event_id
            for item in self.events
            if item.sparsity.startswith("marked-hawkes-")
        }
        if generated_ids != {item.source_event_id for item in lineages}:
            raise ValueError("Hawkes generated events and lineage differ")
        digest = _lineage_sha256(lineages)
        if self.evidence.lineage_content_sha256 is not None and (
            self.evidence.lineage_content_sha256 != digest
        ):
            raise ValueError("Hawkes generation lineage digest differs")
        object.__setattr__(self, "event_lineage", lineages)


@dataclass(frozen=True, slots=True)
class MarkedHawkesCandidateLineageV1:
    """Compact carveable pointer plus fitted excitation provenance."""

    event_id: str
    transformation_id: str
    generation_lineage_id: str
    excitation_source_symbol: str | None
    event_state: str
    schema_version: str = MARKED_HAWKES_CANDIDATE_LINEAGE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            MARKED_HAWKES_CANDIDATE_LINEAGE_SCHEMA_VERSION,
            "marked Hawkes candidate lineage",
        )
        for name in (
            "event_id",
            "transformation_id",
            "generation_lineage_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        source = self.excitation_source_symbol
        if source is not None:
            source = _symbol(source)
        object.__setattr__(self, "excitation_source_symbol", source)
        if self.event_state not in MARK_STATES:
            raise ValueError("Hawkes candidate lineage mark is unsupported")

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "transformation_id": self.transformation_id,
            "generation_lineage_id": self.generation_lineage_id,
            "excitation_source_symbol": self.excitation_source_symbol,
            "event_state": self.event_state,
        }


@dataclass(frozen=True, slots=True)
class MarkedHawkesCandidateBatchV1:
    """One anchor-interval proposal batch accepted by generic carving."""

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
    event_lineage: tuple[MarkedHawkesCandidateLineageV1, ...]
    fit_id: str
    generation_evidence_id: str
    batch_id: str = ""
    schema_version: str = MARKED_HAWKES_CANDIDATE_BATCH_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            MARKED_HAWKES_CANDIDATE_BATCH_SCHEMA_VERSION,
            "marked Hawkes candidate batch",
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
            "fit_id",
            "generation_evidence_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(self, "symbol", _symbol(self.symbol).lower())
        object.__setattr__(
            self,
            "information_mode",
            InformationMode.from_value(self.information_mode),
        )
        object.__setattr__(
            self, "session_state", _required_text(self.session_state)
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
            raise ValueError("generated Hawkes batch requires events")
        if status is not MotifGenerationStatus.GENERATED and events:
            raise ValueError("empty/refused Hawkes batch cannot have events")
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
            raise ValueError("Hawkes candidate event differs from batch scope")
        event_ids = {item.event_id for item in events}
        if len(event_ids) != len(events) or event_ids != {
            item.event_id for item in lineages
        }:
            raise ValueError("Hawkes candidate lineage does not reconcile")
        object.__setattr__(self, "events", events)
        object.__setattr__(self, "event_lineage", lineages)
        expected = _stable_id(
            "marked-hawkes-candidate-batch", self.identity_payload()
        )
        if self.batch_id and self.batch_id != expected:
            raise ValueError("marked Hawkes candidate batch_id differs")
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

    def lineage_for(self, event_id: str) -> MarkedHawkesCandidateLineageV1:
        wanted = _required_text(event_id)
        for lineage in self.event_lineage:
            if lineage.event_id == wanted:
                return lineage
        raise KeyError(wanted)


@dataclass(frozen=True, slots=True)
class FittedMarkedHawkesBenchmarkGeneratorV1(BenchmarkGeneratorV1):
    """Adapter exposing one stable fitted Hawkes ablation to the benchmark."""

    candidate: BenchmarkCandidateV1
    config: MarkedHawkesConfigV1
    fit_result: MarkedHawkesFitResultV1
    candidate_id: str = field(init=False)
    event_schema_version: str = BENCHMARK_EVENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.candidate.kind is not BenchmarkCandidateKind.CANDIDATE:
            raise ValueError("marked Hawkes adapter requires a candidate")
        if self.candidate.method_id != _generator_id(
            self.config.excitation_structure
        ):
            raise ValueError("marked Hawkes candidate method differs")
        if self.fit_result.status is not MarkedHawkesFitStatus.FITTED:
            raise MarkedHawkesFitError(
                "marked Hawkes adapter requires a fitted model"
            )
        if (
            self.fit_result.config_id != self.config.config_id
            or self.fit_result.excitation_structure
            is not self.config.excitation_structure
        ):
            raise ValueError("marked Hawkes fit and config differ")
        _validate_fit_against_config(self.config, self.fit_result)
        if self.event_schema_version != BENCHMARK_EVENT_SCHEMA_VERSION:
            raise ValueError(
                "marked Hawkes adapter requires benchmark event v1"
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
        """Generate one synchronized stream, raising on fail-closed refusal."""
        result = self.generate_with_evidence(
            degraded_events,
            scenario=scenario,
            window=window,
            ensemble_member_id=ensemble_member_id,
        )
        if result.evidence.status in {
            MarkedHawkesGenerationStatus.REFUSED,
            MarkedHawkesGenerationStatus.FAILED,
        }:
            raise MarkedHawkesGenerationError(
                result.evidence.failure_reason
                or "marked Hawkes generation failed"
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
    ) -> MarkedHawkesGenerationResultV1:
        """Simulate with explicit prior-only history and measured evidence."""
        started = time.perf_counter()
        before_peak = peak_rss_bytes()
        raw_events = tuple(degraded_events)
        input_anchor_sha256: str | None = None
        support_level = "not_evaluated"
        model_key: str | None = None
        spectral_radius: float | None = None
        proposals = 0
        proposal_counter = [0]
        try:
            if any(
                not isinstance(item, BenchmarkEventV1) for item in raw_events
            ):
                raise MarkedHawkesGenerationError(
                    "degraded input contains a non-benchmark event"
                )
            ordered = tuple(
                sorted(
                    raw_events,
                    key=lambda item: (
                        item.event_time_ns,
                        item.symbol,
                        item.event_sequence,
                        item.benchmark_event_id,
                    ),
                )
            )
            input_anchor_sha256 = _benchmark_anchor_sha256(ordered)
            history = _validated_history(
                history_events, config=self.config, window=window
            )
            model_key, support_level, model = _conditioning_model(
                self.fit_result, ordered, scenario
            )
            _validate_model_stability(
                model,
                excitation_structure=self.config.excitation_structure,
                maximum_branching_ratio=self.config.maximum_branching_ratio,
            )
            spectral_radius = _finite_float(
                model.get("spectral_radius"), "spectral_radius"
            )
            events, lineages, proposals = _generate_events(
                self.config,
                self.fit_result,
                model,
                ordered,
                scenario=scenario,
                window=window,
                ensemble_member_id=ensemble_member_id,
                history_events=history,
                proposal_counter=proposal_counter,
            )
            measured_peak = _incremental_peak_rss_bytes(before_peak)
            if measured_peak > self.config.limits.max_peak_memory_bytes:
                raise MarkedHawkesGenerationError(
                    "measured generation memory exceeds limit"
                )
            elapsed_ms = round((time.perf_counter() - started) * 1000)
            if elapsed_ms > self.config.limits.max_wall_time_ms:
                raise MarkedHawkesGenerationError(
                    "generation wall-time limit exceeded"
                )
            generated_count = len(lineages)
            status = (
                MarkedHawkesGenerationStatus.GENERATED
                if generated_count
                else MarkedHawkesGenerationStatus.EMPTY
            )
            evidence = MarkedHawkesGenerationEvidenceV1(
                fit_id=self.fit_result.fit_id,
                window_id=window.window_id,
                ensemble_member_id=ensemble_member_id,
                status=status,
                attempted=True,
                generated_event_count=generated_count,
                input_event_count=len(ordered),
                history_event_count=len(history),
                proposal_count=proposals,
                input_anchor_sha256=input_anchor_sha256,
                conditioning_support_level=support_level,
                conditioning_model_key=model_key,
                spectral_radius=spectral_radius,
                lineage_content_sha256=_lineage_sha256(lineages),
                wall_time_ms=elapsed_ms,
                peak_memory_bytes=measured_peak,
            )
            return MarkedHawkesGenerationResultV1(
                events=events,
                event_lineage=lineages,
                evidence=evidence,
            )
        except MarkedHawkesGenerationError as err:
            proposals = proposal_counter[0]
            evidence = MarkedHawkesGenerationEvidenceV1(
                fit_id=self.fit_result.fit_id,
                window_id=window.window_id,
                ensemble_member_id=ensemble_member_id,
                status=MarkedHawkesGenerationStatus.REFUSED,
                attempted=True,
                generated_event_count=0,
                input_event_count=len(raw_events),
                history_event_count=len(history_events),
                proposal_count=proposals,
                input_anchor_sha256=input_anchor_sha256,
                conditioning_support_level=support_level,
                conditioning_model_key=model_key,
                spectral_radius=spectral_radius,
                lineage_content_sha256=None,
                wall_time_ms=round((time.perf_counter() - started) * 1000),
                peak_memory_bytes=_incremental_peak_rss_bytes(before_peak),
                failure_reason=str(err),
            )
            return MarkedHawkesGenerationResultV1(
                events=(), event_lineage=(), evidence=evidence
            )
        except (ArithmeticError, KeyError, TypeError, ValueError) as err:
            proposals = proposal_counter[0]
            evidence = MarkedHawkesGenerationEvidenceV1(
                fit_id=self.fit_result.fit_id,
                window_id=window.window_id,
                ensemble_member_id=ensemble_member_id,
                status=MarkedHawkesGenerationStatus.FAILED,
                attempted=True,
                generated_event_count=0,
                input_event_count=len(raw_events),
                history_event_count=len(history_events),
                proposal_count=proposals,
                input_anchor_sha256=input_anchor_sha256,
                conditioning_support_level=support_level,
                conditioning_model_key=model_key,
                spectral_radius=None,
                lineage_content_sha256=None,
                wall_time_ms=round((time.perf_counter() - started) * 1000),
                peak_memory_bytes=_incremental_peak_rss_bytes(before_peak),
                failure_reason=f"generation_failed:{type(err).__name__}:{err}",
            )
            return MarkedHawkesGenerationResultV1(
                events=(), event_lineage=(), evidence=evidence
            )


def default_marked_hawkes_configs() -> tuple[MarkedHawkesConfigV1, ...]:
    """Return the fixed ablation order without selecting a winner."""
    return tuple(
        MarkedHawkesConfigV1(
            excitation_structure=structure,
            base_seed=451_000 + index,
        )
        for index, structure in enumerate(HawkesExcitationStructure, start=1)
    )


def fit_marked_hawkes_challenger(
    config: MarkedHawkesConfigV1,
    calibration_windows: Sequence[EventClockCalibrationWindowV1],
    *,
    information_mode: InformationMode = InformationMode.EX_POST_RECONSTRUCTION,
    as_of_ns: int | None = None,
) -> MarkedHawkesFitResultV1:
    """Fit one ablation on calibration rows with explicit refusal evidence."""
    if not isinstance(config, MarkedHawkesConfigV1):
        raise TypeError("unsupported marked Hawkes configuration")
    started = time.perf_counter()
    windows = tuple(calibration_windows)
    mode = InformationMode.from_value(information_mode)
    calibration_hash = _calibration_hash(windows)
    symbols = tuple(
        sorted({event.symbol for item in windows for event in item.events})
    )
    event_count = sum(len(item.events) for item in windows)
    estimated_memory = event_count * config.limits.estimated_bytes_per_fit_event
    reason = _fit_refusal_reason(
        config,
        windows,
        mode=mode,
        as_of_ns=as_of_ns,
        symbols=symbols,
        event_count=event_count,
        estimated_memory=estimated_memory,
    )
    if reason is not None:
        return _closed_fit_result(
            config,
            calibration_hash=calibration_hash,
            mode=mode,
            as_of_ns=as_of_ns,
            symbols=symbols,
            status=MarkedHawkesFitStatus.REFUSED,
            event_count=event_count,
            window_count=len(windows),
            estimated_memory=estimated_memory,
            reason=reason,
        )
    try:
        groups = _conditioning_groups(windows)
        if len(groups) > config.limits.max_conditioning_cells:
            raise MarkedHawkesFitError("conditioning cell limit exceeded")
        models: dict[str, JSONValue] = {}
        uncertainties: dict[str, JSONValue] = {}
        total_likelihood = 0.0
        maximum_iterations = 0
        exact_count = 0
        session_count = 0
        for key, grouped_windows in sorted(groups.items()):
            if not _has_conditioning_support(config, grouped_windows, symbols):
                continue
            model, uncertainty, iterations = _fit_conditioning_model(
                config, grouped_windows, symbols
            )
            models[key] = cast(JSONValue, model)
            uncertainties[key] = cast(JSONValue, uncertainty)
            if key.startswith("exact|"):
                total_likelihood += _finite_float(
                    model["log_likelihood"], "log_likelihood"
                )
            maximum_iterations = max(maximum_iterations, iterations)
            if key.startswith("exact|"):
                exact_count += 1
            else:
                session_count += 1
            if round((time.perf_counter() - started) * 1000) > (
                config.limits.max_wall_time_ms
            ):
                raise MarkedHawkesFitError("fit wall-time limit exceeded")
        if not models or not session_count:
            raise MarkedHawkesFitError(
                "no supported session-conditioned Hawkes model"
            )
        parameters: dict[str, JSONValue] = {
            "kernel": "exponential-integrated-mass-v1",
            "excitation_structure": config.excitation_structure.value,
            "symbols": list(symbols),
            "mark_states": list(MARK_STATES),
            "conditioning_policy": "exact-epoch-session-then-session-v1",
            "fit_boundary_policy": "reset-each-calibration-window-v1",
            "maximum_branching_ratio": config.maximum_branching_ratio,
            "conditioning_models": models,
        }
        uncertainty_payload: dict[str, JSONValue] = {
            "method": "responsibility-count-wald-95-v1",
            "confidence_level": 0.95,
            "conditioning_models": uncertainties,
        }
        parameter_size = len(canonical_contract_json(parameters).encode())
        if parameter_size > config.limits.max_parameters_bytes:
            raise MarkedHawkesFitError("fit parameter payload exceeds limit")
        diagnostics: dict[str, JSONScalar] = {
            "conditioning_cell_count": len(models),
            "exact_conditioning_cell_count": exact_count,
            "session_backoff_cell_count": session_count,
            "decay_candidate_count": len(config.decay_candidates_per_second),
            "parameter_bytes": parameter_size,
            "maximum_spectral_radius": max(
                float(cast(Mapping[str, Any], value)["spectral_radius"])
                for value in models.values()
            ),
            "stability_margin": min(
                1.0 - float(cast(Mapping[str, Any], value)["spectral_radius"])
                for value in models.values()
            ),
            "calibration_history_reset_count": len(windows),
        }
        if len(diagnostics) > config.limits.max_diagnostics:
            raise MarkedHawkesFitError("fit diagnostic count exceeds limit")
        result = MarkedHawkesFitResultV1(
            excitation_structure=config.excitation_structure,
            config_id=config.config_id,
            calibration_content_sha256=calibration_hash,
            information_mode=mode,
            as_of_ns=as_of_ns,
            symbols=symbols,
            status=MarkedHawkesFitStatus.FITTED,
            converged=True,
            iteration_count=maximum_iterations,
            fitted_event_count=event_count,
            fitted_window_count=len(windows),
            log_likelihood=total_likelihood,
            parameters=parameters,
            uncertainty=uncertainty_payload,
            diagnostics=diagnostics,
            estimated_peak_memory_bytes=estimated_memory,
        )
        _validate_fit_against_config(config, result)
        return result
    except (ArithmeticError, MarkedHawkesFitError, ValueError) as err:
        return _closed_fit_result(
            config,
            calibration_hash=calibration_hash,
            mode=mode,
            as_of_ns=as_of_ns,
            symbols=symbols,
            status=MarkedHawkesFitStatus.FAILED,
            event_count=event_count,
            window_count=len(windows),
            estimated_memory=estimated_memory,
            reason=f"fit_failed:{type(err).__name__}:{err}",
        )


def build_marked_hawkes_benchmark_candidate(
    config: MarkedHawkesConfigV1,
    fit_result: MarkedHawkesFitResultV1,
    *,
    ensemble_member_ids: Sequence[str],
) -> BenchmarkCandidateV1:
    """Describe a Hawkes fit attempt, including failed ablations."""
    if (
        fit_result.config_id != config.config_id
        or fit_result.excitation_structure is not config.excitation_structure
    ):
        raise ValueError("marked Hawkes fit and config differ")
    return BenchmarkCandidateV1(
        kind=BenchmarkCandidateKind.CANDIDATE,
        method_id=_generator_id(config.excitation_structure),
        implementation_version=MARKED_HAWKES_IMPLEMENTATION_VERSION,
        parameters={
            "config_id": config.config_id,
            "fit_id": fit_result.fit_id,
            "excitation_structure": config.excitation_structure.value,
            "automatic_winner": False,
        },
        ensemble_member_ids=tuple(ensemble_member_ids),
    )


def build_fitted_marked_hawkes_generator(
    config: MarkedHawkesConfigV1,
    fit_result: MarkedHawkesFitResultV1,
    *,
    ensemble_member_ids: Sequence[str],
) -> FittedMarkedHawkesBenchmarkGeneratorV1:
    """Bind one stable fit to an explicit benchmark candidate."""
    candidate = build_marked_hawkes_benchmark_candidate(
        config, fit_result, ensemble_member_ids=ensemble_member_ids
    )
    return FittedMarkedHawkesBenchmarkGeneratorV1(
        candidate=candidate, config=config, fit_result=fit_result
    )


def build_marked_hawkes_candidate_batches(
    *,
    run: ReconstructionRunV1,
    window: ReconstructionWindowV1,
    config: MarkedHawkesConfigV1,
    fit_result: MarkedHawkesFitResultV1,
    generation_result: MarkedHawkesGenerationResultV1,
    observed_events: Sequence[SyntheticEventV1],
    session_state: str,
    special_tags: Sequence[str] = (),
    event_tags: Sequence[str] = (),
) -> tuple[MarkedHawkesCandidateBatchV1, ...]:
    """Project synchronized Hawkes proposals into the generic carving seam."""
    if window.run_id != run.run_id:
        raise ValueError("Hawkes candidate window does not belong to run")
    if window.ensemble_member_id not in run.ensemble_member_ids:
        raise ValueError("Hawkes candidate member is outside run")
    if config.config_id not in run.configuration_ids:
        raise ValueError("Hawkes config is absent from reconstruction run")
    if (
        fit_result.config_id != config.config_id
        or fit_result.fit_id != generation_result.evidence.fit_id
    ):
        raise ValueError("Hawkes fit, config, and generation differ")
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
        raise ValueError("Hawkes carving projection requires observed anchors")
    if generation_result.evidence.input_anchor_sha256 is not None and (
        _synthetic_anchor_sha256(observed)
        != generation_result.evidence.input_anchor_sha256
    ):
        raise ValueError("carving anchors differ from Hawkes generation input")
    benchmark_proposals = tuple(
        item
        for item in generation_result.events
        if item.sparsity.startswith("marked-hawkes-")
    )
    if len({item.source_event_id for item in benchmark_proposals}) != len(
        benchmark_proposals
    ):
        raise ValueError("Hawkes proposals have duplicate source identity")
    upstream_refused = generation_result.evidence.status in {
        MarkedHawkesGenerationStatus.REFUSED,
        MarkedHawkesGenerationStatus.FAILED,
    }
    lineage_by_source = {
        item.source_event_id: item for item in generation_result.event_lineage
    }
    if set(lineage_by_source) != {
        item.source_event_id for item in benchmark_proposals
    }:
        raise ValueError("Hawkes proposals and generation lineage differ")
    batches: list[MarkedHawkesCandidateBatchV1] = []
    assigned_proposal_ids: set[str] = set()
    by_symbol: dict[str, list[SyntheticEventV1]] = defaultdict(list)
    for event in observed:
        by_symbol[event.symbol].append(event)
    for symbol in sorted(by_symbol):
        anchors = by_symbol[symbol]
        if len(anchors) < 2:
            raise ValueError("each Hawkes carving symbol requires two anchors")
        for left_anchor, right_anchor in pairwise(anchors):
            interval_id = derive_anchor_interval_id(
                left_anchor.event_id, right_anchor.event_id
            )
            selected = tuple(
                item
                for item in benchmark_proposals
                if item.symbol.lower() == symbol
                and left_anchor.event_time_ns
                < item.event_time_ns
                < right_anchor.event_time_ns
            )
            assigned_proposal_ids.update(
                item.source_event_id for item in selected
            )
            transformation_id = _stable_id(
                "marked-hawkes-interval-transformation",
                {
                    "fit_id": fit_result.fit_id,
                    "generation_evidence_id": (
                        generation_result.evidence.evidence_id
                    ),
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
                    generator_id=_generator_id(config.excitation_structure),
                    generator_version=MARKED_HAWKES_IMPLEMENTATION_VERSION,
                    generator_config_id=config.config_id,
                    reference_id=item.source_event_id,
                    motif_id=_generator_id(config.excitation_structure),
                    feed_epoch_id=item.epoch_id,
                    constraint_set_id=CANDIDATE_ONLY_CONSTRAINT_SET_ID,
                )
                for ordinal, item in enumerate(selected, start=1)
            )
            status = (
                MotifGenerationStatus.REFUSED
                if upstream_refused
                else (
                    MotifGenerationStatus.GENERATED
                    if events
                    else MotifGenerationStatus.EMPTY
                )
            )
            candidate_lineage = tuple(
                MarkedHawkesCandidateLineageV1(
                    event_id=event.event_id,
                    transformation_id=transformation_id,
                    generation_lineage_id=lineage_by_source[
                        proposal.source_event_id
                    ].lineage_id,
                    excitation_source_symbol=lineage_by_source[
                        proposal.source_event_id
                    ].excitation_source_symbol,
                    event_state=lineage_by_source[
                        proposal.source_event_id
                    ].event_state,
                )
                for event, proposal in zip(events, selected)
            )
            batches.append(
                MarkedHawkesCandidateBatchV1(
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
                    events=events if not upstream_refused else (),
                    event_lineage=(
                        candidate_lineage if not upstream_refused else ()
                    ),
                    fit_id=fit_result.fit_id,
                    generation_evidence_id=(
                        generation_result.evidence.evidence_id
                    ),
                )
            )
    if assigned_proposal_ids != {
        item.source_event_id for item in benchmark_proposals
    }:
        raise ValueError("Hawkes proposal lies outside observed anchors")
    return tuple(batches)


def _closed_fit_result(
    config: MarkedHawkesConfigV1,
    *,
    calibration_hash: str,
    mode: InformationMode,
    as_of_ns: int | None,
    symbols: Sequence[str],
    status: MarkedHawkesFitStatus,
    event_count: int,
    window_count: int,
    estimated_memory: int,
    reason: str,
) -> MarkedHawkesFitResultV1:
    return MarkedHawkesFitResultV1(
        excitation_structure=config.excitation_structure,
        config_id=config.config_id,
        calibration_content_sha256=calibration_hash,
        information_mode=mode,
        as_of_ns=as_of_ns,
        symbols=tuple(symbols) or ("UNSUPPORTED",),
        status=status,
        converged=False,
        iteration_count=0,
        fitted_event_count=event_count,
        fitted_window_count=window_count,
        log_likelihood=None,
        parameters={},
        uncertainty={},
        diagnostics={},
        estimated_peak_memory_bytes=estimated_memory,
        failure_reason=reason,
    )


def _fit_refusal_reason(
    config: MarkedHawkesConfigV1,
    windows: Sequence[EventClockCalibrationWindowV1],
    *,
    mode: InformationMode,
    as_of_ns: int | None,
    symbols: Sequence[str],
    event_count: int,
    estimated_memory: int,
) -> str | None:
    if not windows:
        return "missing_calibration_windows"
    if any(
        not isinstance(item, EventClockCalibrationWindowV1) for item in windows
    ):
        return "invalid_calibration_window"
    if len(windows) > config.limits.max_fit_windows:
        return "fit_window_limit"
    if event_count > config.limits.max_fit_events:
        return "fit_event_limit"
    if estimated_memory > config.limits.max_peak_memory_bytes:
        return "fit_memory_limit"
    if len(symbols) > config.limits.max_dimensions:
        return "fit_dimension_limit"
    if mode is InformationMode.EX_ANTE_SIMULATION:
        if as_of_ns is None:
            return "ex_ante_as_of_missing"
        if any(item.end_ns > as_of_ns for item in windows):
            return "calibration_not_available_as_of"
    elif as_of_ns is not None:
        return "ex_post_as_of_forbidden"
    counts = Counter(event.symbol for item in windows for event in item.events)
    if not counts or any(
        count < config.minimum_events_per_symbol for count in counts.values()
    ):
        return "insufficient_symbol_support"
    expected_symbols = set(symbols)
    for window in windows:
        if {event.symbol for event in window.events} != expected_symbols:
            return "incomplete_synchronized_symbol_support"
        cells = {(event.epoch_id, event.session) for event in window.events}
        if len(cells) != 1:
            return "mixed_conditioning_window"
    return None


def _conditioning_groups(
    windows: Sequence[EventClockCalibrationWindowV1],
) -> dict[str, tuple[EventClockCalibrationWindowV1, ...]]:
    values: dict[str, list[EventClockCalibrationWindowV1]] = defaultdict(list)
    for window in windows:
        epoch, session = next(
            iter({(event.epoch_id, event.session) for event in window.events})
        )
        values[_exact_key(epoch, session)].append(window)
        values[_session_key(session)].append(window)
    return {key: tuple(items) for key, items in sorted(values.items())}


def _has_conditioning_support(
    config: MarkedHawkesConfigV1,
    windows: Sequence[EventClockCalibrationWindowV1],
    symbols: Sequence[str],
) -> bool:
    counts = Counter(
        event.symbol for window in windows for event in window.events
    )
    return all(
        counts[symbol]
        >= max(
            config.minimum_events_per_symbol,
            config.minimum_conditioning_events,
        )
        for symbol in symbols
    )


def _fit_conditioning_model(
    config: MarkedHawkesConfigV1,
    windows: Sequence[EventClockCalibrationWindowV1],
    symbols: Sequence[str],
) -> tuple[dict[str, JSONValue], dict[str, JSONValue], int]:
    indexed_windows = _indexed_windows(windows, symbols)
    best: (
        tuple[
            float,
            float,
            list[float],
            list[list[float]],
            int,
            dict[str, Any],
        ]
        | None
    ) = None
    for decay in config.decay_candidates_per_second:
        fitted = _fit_fixed_decay(config, indexed_windows, symbols, decay)
        likelihood, baseline, excitation, iterations, responsibilities = fitted
        candidate = (
            likelihood,
            decay,
            baseline,
            excitation,
            iterations,
            responsibilities,
        )
        if best is None or candidate[0] > best[0]:
            best = candidate
    if best is None:
        raise MarkedHawkesFitError("no decay candidate converged")
    likelihood, decay, baseline, excitation, iterations, responsibilities = best
    radius = _spectral_radius(excitation)
    if radius >= config.maximum_branching_ratio or radius >= 1.0:
        raise MarkedHawkesFitError("fitted excitation is unstable")
    mark_parameters = _mark_parameters(
        config,
        indexed_windows,
        symbols,
        decay,
        baseline,
        excitation,
    )
    exposure_seconds = sum(
        (end_ns - start_ns) / NANOSECONDS_PER_SECOND
        for start_ns, end_ns, _events in indexed_windows
    )
    event_count = sum(len(events) for _start, _end, events in indexed_windows)
    model: dict[str, JSONValue] = {
        "symbols": list(symbols),
        "decay_per_second": decay,
        "baseline_rates_per_second": cast(JSONValue, baseline),
        "excitation_matrix": cast(JSONValue, excitation),
        "spectral_radius": radius,
        "stability_margin": 1.0 - radius,
        "log_likelihood": likelihood,
        "fit_event_count": event_count,
        "fit_window_count": len(indexed_windows),
        "exposure_seconds": exposure_seconds,
        "immigrant_mark_probabilities": cast(JSONValue, mark_parameters[0]),
        "excitation_mark_probabilities": cast(JSONValue, mark_parameters[1]),
    }
    uncertainty = _fit_uncertainty(
        baseline,
        excitation,
        responsibilities,
        exposure_seconds,
    )
    return model, uncertainty, iterations


def _indexed_windows(
    windows: Sequence[EventClockCalibrationWindowV1],
    symbols: Sequence[str],
) -> tuple[tuple[int, int, tuple[tuple[int, int, str], ...]], ...]:
    symbol_index = {symbol: index for index, symbol in enumerate(symbols)}
    result = []
    for window in windows:
        marks = _window_marks(window.events)
        events = tuple(
            sorted(
                (
                    event.event_time_ns,
                    symbol_index[event.symbol],
                    marks[event.benchmark_event_id],
                )
                for event in window.events
            )
        )
        result.append((window.start_ns, window.end_ns, events))
    return tuple(result)


def _fit_fixed_decay(
    config: MarkedHawkesConfigV1,
    windows: Sequence[tuple[int, int, tuple[tuple[int, int, str], ...]]],
    symbols: Sequence[str],
    decay_per_second: float,
) -> tuple[float, list[float], list[list[float]], int, dict[str, Any]]:
    dimension = len(symbols)
    exposure = sum(
        (end - start) / NANOSECONDS_PER_SECOND
        for start, end, _events in windows
    )
    counts = [0] * dimension
    for _start, _end, events in windows:
        for _time_ns, index, _mark in events:
            counts[index] += 1
    baseline_scale = (
        1.0
        if config.excitation_structure is HawkesExcitationStructure.ZERO
        else 0.9
    )
    baseline = [
        max(config.parameter_floor, count / exposure * baseline_scale)
        for count in counts
    ]
    excitation = _initial_excitation(config.excitation_structure, dimension)
    converged = config.excitation_structure is HawkesExcitationStructure.ZERO
    responsibilities: dict[str, Any] = {}
    iteration_count = 1
    if not converged:
        for iteration_count in range(1, config.limits.max_iterations + 1):
            responsibilities = _responsibility_statistics(
                windows,
                baseline,
                excitation,
                decay_per_second=decay_per_second,
            )
            updated_baseline = [
                max(
                    config.parameter_floor,
                    value / exposure,
                )
                for value in responsibilities["immigrant_counts"]
            ]
            updated_excitation = _updated_excitation(
                config,
                responsibilities["offspring_counts"],
                responsibilities["source_exposure"],
            )
            change = max(
                [
                    abs(left - right)
                    for left, right in zip(updated_baseline, baseline)
                ]
                + [
                    abs(updated_excitation[i][j] - excitation[i][j])
                    for i in range(dimension)
                    for j in range(dimension)
                ]
            )
            baseline = updated_baseline
            excitation = updated_excitation
            if change <= config.convergence_tolerance:
                converged = True
                break
        if not converged:
            raise MarkedHawkesFitError("fixed-decay EM did not converge")
    responsibilities = _responsibility_statistics(
        windows,
        baseline,
        excitation,
        decay_per_second=decay_per_second,
    )
    likelihood = _hawkes_log_likelihood(
        windows,
        baseline,
        excitation,
        decay_per_second=decay_per_second,
    )
    return likelihood, baseline, excitation, iteration_count, responsibilities


def _initial_excitation(
    structure: HawkesExcitationStructure, dimension: int
) -> list[list[float]]:
    if structure is HawkesExcitationStructure.ZERO:
        return [
            [0.0 for _source in range(dimension)] for _dest in range(dimension)
        ]
    return [
        [
            (
                0.02
                if (
                    structure is HawkesExcitationStructure.FULL
                    or destination == source
                )
                else 0.0
            )
            for source in range(dimension)
        ]
        for destination in range(dimension)
    ]


def _responsibility_statistics(
    windows: Sequence[tuple[int, int, tuple[tuple[int, int, str], ...]]],
    baseline: Sequence[float],
    excitation: Sequence[Sequence[float]],
    *,
    decay_per_second: float,
) -> dict[str, Any]:
    dimension = len(baseline)
    immigrant = [0.0] * dimension
    offspring = [[0.0] * dimension for _destination in range(dimension)]
    source_exposure = [0.0] * dimension
    for _start_ns, end_ns, events in windows:
        recursion = [0.0] * dimension
        previous_ns = events[0][0] if events else end_ns
        for event_time_ns, grouped in groupby(events, key=lambda item: item[0]):
            simultaneous = tuple(grouped)
            elapsed = (event_time_ns - previous_ns) / NANOSECONDS_PER_SECOND
            factor = math.exp(-decay_per_second * max(0.0, elapsed))
            recursion = [value * factor for value in recursion]
            for _time_ns, destination, _mark in simultaneous:
                intensity = baseline[destination] + decay_per_second * sum(
                    excitation[destination][source] * recursion[source]
                    for source in range(dimension)
                )
                if not math.isfinite(intensity) or intensity <= 0.0:
                    raise MarkedHawkesFitError("nonpositive fitted intensity")
                immigrant[destination] += baseline[destination] / intensity
                for source in range(dimension):
                    offspring[destination][source] += (
                        decay_per_second
                        * excitation[destination][source]
                        * recursion[source]
                        / intensity
                    )
            for _time_ns, destination, _mark in simultaneous:
                recursion[destination] += 1.0
                remaining = (end_ns - event_time_ns) / NANOSECONDS_PER_SECOND
                source_exposure[destination] += 1.0 - math.exp(
                    -decay_per_second * max(0.0, remaining)
                )
            previous_ns = event_time_ns
    return {
        "immigrant_counts": immigrant,
        "offspring_counts": offspring,
        "source_exposure": source_exposure,
    }


def _updated_excitation(
    config: MarkedHawkesConfigV1,
    offspring_counts: Sequence[Sequence[float]],
    source_exposure: Sequence[float],
) -> list[list[float]]:
    dimension = len(offspring_counts)
    matrix = []
    for destination in range(dimension):
        row = []
        for source in range(dimension):
            allowed = (
                config.excitation_structure is HawkesExcitationStructure.FULL
                or (
                    config.excitation_structure
                    is HawkesExcitationStructure.DIAGONAL
                    and destination == source
                )
            )
            if not allowed:
                row.append(0.0)
                continue
            denominator = source_exposure[source]
            row.append(
                max(
                    config.parameter_floor,
                    offspring_counts[destination][source]
                    / max(config.parameter_floor, denominator),
                )
            )
        matrix.append(row)
    radius = _spectral_radius(matrix)
    if radius >= config.maximum_branching_ratio:
        scale = (
            config.maximum_branching_ratio
            * (1.0 - config.convergence_tolerance)
            / max(radius, config.parameter_floor)
        )
        matrix = [[value * scale for value in row] for row in matrix]
    return matrix


def _hawkes_log_likelihood(
    windows: Sequence[tuple[int, int, tuple[tuple[int, int, str], ...]]],
    baseline: Sequence[float],
    excitation: Sequence[Sequence[float]],
    *,
    decay_per_second: float,
) -> float:
    dimension = len(baseline)
    result = 0.0
    for start_ns, end_ns, events in windows:
        recursion = [0.0] * dimension
        previous_ns = start_ns
        for event_time_ns, grouped in groupby(events, key=lambda item: item[0]):
            simultaneous = tuple(grouped)
            elapsed = (event_time_ns - previous_ns) / NANOSECONDS_PER_SECOND
            factor = math.exp(-decay_per_second * max(0.0, elapsed))
            recursion = [value * factor for value in recursion]
            for _time_ns, destination, _mark in simultaneous:
                intensity = baseline[destination] + decay_per_second * sum(
                    excitation[destination][source] * recursion[source]
                    for source in range(dimension)
                )
                if intensity <= 0.0 or not math.isfinite(intensity):
                    raise MarkedHawkesFitError(
                        "nonpositive likelihood intensity"
                    )
                result += math.log(intensity)
            for _time_ns, destination, _mark in simultaneous:
                recursion[destination] += 1.0
            previous_ns = event_time_ns
        duration = (end_ns - start_ns) / NANOSECONDS_PER_SECOND
        result -= sum(baseline) * duration
        for event_time_ns, source, _mark in events:
            remaining = (end_ns - event_time_ns) / NANOSECONDS_PER_SECOND
            kernel_mass = 1.0 - math.exp(
                -decay_per_second * max(0.0, remaining)
            )
            result -= kernel_mass * sum(
                excitation[destination][source]
                for destination in range(dimension)
            )
    if not math.isfinite(result):
        raise MarkedHawkesFitError("nonfinite Hawkes log likelihood")
    return result


def _mark_parameters(
    config: MarkedHawkesConfigV1,
    windows: Sequence[tuple[int, int, tuple[tuple[int, int, str], ...]]],
    symbols: Sequence[str],
    decay_per_second: float,
    baseline: Sequence[float],
    excitation: Sequence[Sequence[float]],
) -> tuple[dict[str, JSONValue], dict[str, JSONValue]]:
    dimension = len(symbols)
    immigrant_weights: dict[tuple[int, str], float] = defaultdict(float)
    excitation_weights: dict[tuple[int, int, str], float] = defaultdict(float)
    for _start, end_ns, events in windows:
        recursion = [0.0] * dimension
        previous_ns = events[0][0] if events else end_ns
        for event_time_ns, grouped in groupby(events, key=lambda item: item[0]):
            simultaneous = tuple(grouped)
            elapsed = (event_time_ns - previous_ns) / NANOSECONDS_PER_SECOND
            factor = math.exp(-decay_per_second * max(0.0, elapsed))
            recursion = [value * factor for value in recursion]
            for _time_ns, destination, mark in simultaneous:
                components = [
                    decay_per_second
                    * excitation[destination][source]
                    * recursion[source]
                    for source in range(dimension)
                ]
                intensity = baseline[destination] + sum(components)
                immigrant_weights[(destination, mark)] += (
                    baseline[destination] / intensity
                )
                for source, component in enumerate(components):
                    excitation_weights[(destination, source, mark)] += (
                        component / intensity
                    )
            for _time_ns, destination, _mark in simultaneous:
                recursion[destination] += 1.0
            previous_ns = event_time_ns
    smoothing = config.mark_smoothing_count
    immigrant_probs: dict[str, JSONValue] = {}
    excitation_probs: dict[str, JSONValue] = {}
    for destination, destination_symbol in enumerate(symbols):
        weights = {
            mark: immigrant_weights[(destination, mark)] + smoothing
            for mark in MARK_STATES
        }
        immigrant_probs[destination_symbol] = cast(
            JSONValue, _normalized_probabilities(weights)
        )
        by_source: dict[str, JSONValue] = {}
        for source, source_symbol in enumerate(symbols):
            source_weights = {
                mark: excitation_weights[(destination, source, mark)]
                + smoothing
                for mark in MARK_STATES
            }
            by_source[source_symbol] = cast(
                JSONValue, _normalized_probabilities(source_weights)
            )
        excitation_probs[destination_symbol] = cast(JSONValue, by_source)
    return immigrant_probs, excitation_probs


def _fit_uncertainty(
    baseline: Sequence[float],
    excitation: Sequence[Sequence[float]],
    responsibilities: Mapping[str, Any],
    exposure_seconds: float,
) -> dict[str, JSONValue]:
    immigrant_counts = cast(
        Sequence[float], responsibilities["immigrant_counts"]
    )
    offspring_counts = cast(
        Sequence[Sequence[float]], responsibilities["offspring_counts"]
    )
    source_exposure = cast(Sequence[float], responsibilities["source_exposure"])
    baseline_intervals = []
    for value, count in zip(baseline, immigrant_counts):
        standard_error = math.sqrt(max(0.0, count)) / exposure_seconds
        baseline_intervals.append(
            {
                "estimate": value,
                "standard_error": standard_error,
                "lower": max(0.0, value - 1.96 * standard_error),
                "upper": value + 1.96 * standard_error,
            }
        )
    excitation_intervals = []
    for destination, row in enumerate(excitation):
        values = []
        for source, value in enumerate(row):
            denominator = max(1e-12, source_exposure[source])
            standard_error = (
                math.sqrt(max(0.0, offspring_counts[destination][source]))
                / denominator
            )
            values.append(
                {
                    "estimate": value,
                    "standard_error": standard_error,
                    "lower": max(0.0, value - 1.96 * standard_error),
                    "upper": value + 1.96 * standard_error,
                }
            )
        excitation_intervals.append(values)
    return {
        "method": "responsibility-count-wald-95-v1",
        "baseline_rates_per_second": cast(JSONValue, baseline_intervals),
        "excitation_matrix": cast(JSONValue, excitation_intervals),
        "nonclaim": "descriptive-curvature-approximation-not-exact-coverage",
    }


def _conditioning_model(
    fit: MarkedHawkesFitResultV1,
    events: Sequence[BenchmarkEventV1],
    scenario: BenchmarkScenarioV1,
) -> tuple[str, str, Mapping[str, Any]]:
    if not events:
        raise MarkedHawkesGenerationError("Hawkes generation requires anchors")
    sessions = {event.session for event in events}
    epochs = {event.epoch_id for event in events}
    if len(sessions) != 1:
        raise MarkedHawkesGenerationError(
            "generation window spans multiple sessions"
        )
    if epochs != {scenario.epoch_id}:
        raise MarkedHawkesGenerationError(
            "generation anchors differ from scenario feed epoch"
        )
    session = next(iter(sessions))
    models = _mapping(
        fit.parameters.get("conditioning_models"), "conditioning_models"
    )
    exact = _exact_key(scenario.epoch_id, session)
    if exact in models:
        return exact, "exact_epoch_session", _mapping(models[exact], exact)
    fallback = _session_key(session)
    if fallback in models:
        return fallback, "session_backoff", _mapping(models[fallback], fallback)
    raise MarkedHawkesGenerationError(
        "requested epoch/session conditioning support is missing"
    )


def _generate_events(
    config: MarkedHawkesConfigV1,
    fit: MarkedHawkesFitResultV1,
    model: Mapping[str, Any],
    anchors: Sequence[BenchmarkEventV1],
    *,
    scenario: BenchmarkScenarioV1,
    window: ReconstructionWindowV1,
    ensemble_member_id: str,
    history_events: Sequence[BenchmarkEventV1],
    proposal_counter: list[int],
) -> tuple[
    tuple[BenchmarkEventV1, ...],
    tuple[MarkedHawkesGenerationLineageV1, ...],
    int,
]:
    if ensemble_member_id != window.ensemble_member_id:
        raise MarkedHawkesGenerationError(
            "ensemble member differs from reconstruction window"
        )
    if set(item.upper() for item in window.symbols) != set(fit.symbols):
        raise MarkedHawkesGenerationError(
            "generation symbols differ from fitted synchronized dimensions"
        )
    if any(
        event.event_time_ns < window.core_start_ns
        or event.event_time_ns >= window.core_end_ns
        or event.symbol not in fit.symbols
        for event in anchors
    ):
        raise MarkedHawkesGenerationError(
            "generation anchor lies outside synchronized window"
        )
    by_symbol: dict[str, list[BenchmarkEventV1]] = defaultdict(list)
    for event in anchors:
        by_symbol[event.symbol].append(event)
    if set(by_symbol) != set(fit.symbols) or any(
        len(values) < 2 for values in by_symbol.values()
    ):
        raise MarkedHawkesGenerationError(
            "each fitted symbol requires at least two synchronized anchors"
        )
    symbols = tuple(_string_tuple(model.get("symbols")))
    if symbols != fit.symbols:
        raise MarkedHawkesGenerationError("conditioning model symbols differ")
    dimension = len(symbols)
    symbol_index = {symbol: index for index, symbol in enumerate(symbols)}
    decay = _positive_float(model.get("decay_per_second"), "decay_per_second")
    baseline = _float_vector(
        model.get("baseline_rates_per_second"), dimension, "baseline rates"
    )
    excitation = _float_matrix(
        model.get("excitation_matrix"), dimension, "excitation matrix"
    )
    _validate_model_stability(
        model,
        excitation_structure=config.excitation_structure,
        maximum_branching_ratio=config.maximum_branching_ratio,
    )
    immigrant_marks = _mapping(
        model.get("immigrant_mark_probabilities"), "immigrant marks"
    )
    excitation_marks = _mapping(
        model.get("excitation_mark_probabilities"), "excitation marks"
    )
    estimated_generation_bytes = (
        len(anchors) + config.limits.max_generated_events_per_window
    ) * config.limits.estimated_bytes_per_generated_event
    if estimated_generation_bytes > config.limits.max_peak_memory_bytes:
        raise MarkedHawkesGenerationError(
            "generation memory estimate exceeds limit"
        )
    seed = _semantic_seed(
        config.base_seed,
        fit.fit_id,
        scenario.scenario_id,
        window.window_id,
        ensemble_member_id,
        _benchmark_content_sha256(anchors),
        _benchmark_content_sha256(history_events),
    )
    rng = random.Random(seed)
    recursion = [0.0] * dimension
    for event in history_events:
        elapsed = (
            window.core_start_ns - event.event_time_ns
        ) / NANOSECONDS_PER_SECOND
        if elapsed <= config.limits.max_history_ns / NANOSECONDS_PER_SECOND:
            recursion[symbol_index[event.symbol]] += math.exp(-decay * elapsed)
    ordered_anchors = tuple(
        sorted(
            anchors,
            key=lambda item: (
                item.event_time_ns,
                item.symbol,
                item.event_sequence,
                item.benchmark_event_id,
            ),
        )
    )
    anchor_index = 0
    current_ns = window.core_start_ns
    generated: list[BenchmarkEventV1] = []
    lineages: list[MarkedHawkesGenerationLineageV1] = []
    per_interval: Counter[tuple[str, str, str]] = Counter()
    if len(proposal_counter) != 1 or proposal_counter[0] != 0:
        raise ValueError("proposal counter must start at zero")
    generation_started = time.perf_counter()
    missing_scale = _missing_intensity_scale(scenario)

    while current_ns < window.core_end_ns:
        next_anchor_ns = (
            ordered_anchors[anchor_index].event_time_ns
            if anchor_index < len(ordered_anchors)
            else window.core_end_ns
        )
        if next_anchor_ns <= current_ns:
            while (
                anchor_index < len(ordered_anchors)
                and ordered_anchors[anchor_index].event_time_ns == current_ns
            ):
                recursion[
                    symbol_index[ordered_anchors[anchor_index].symbol]
                ] += 1.0
                anchor_index += 1
            continue
        active_intervals = _active_intervals(by_symbol, current_ns)
        intensities = _conditional_intensities(
            baseline,
            excitation,
            recursion,
            decay_per_second=decay,
            active_symbols=set(active_intervals),
            symbols=symbols,
            scale=missing_scale,
        )
        upper = sum(intensities)
        if upper <= 0.0:
            _decay_recursion(
                recursion,
                decay,
                (next_anchor_ns - current_ns) / NANOSECONDS_PER_SECOND,
            )
            current_ns = next_anchor_ns
            continue
        wait_seconds = rng.expovariate(upper)
        proposed_ns = current_ns + max(
            1, round(wait_seconds * NANOSECONDS_PER_SECOND)
        )
        proposal_counter[0] += 1
        if proposal_counter[0] > config.limits.max_ogata_proposals:
            raise MarkedHawkesGenerationError("Ogata proposal limit exceeded")
        if (
            proposal_counter[0] % 1_024 == 0
            and (time.perf_counter() - generation_started) * 1000
            > config.limits.max_wall_time_ms
        ):
            raise MarkedHawkesGenerationError(
                "generation wall-time limit exceeded"
            )
        if proposed_ns >= next_anchor_ns:
            _decay_recursion(
                recursion,
                decay,
                (next_anchor_ns - current_ns) / NANOSECONDS_PER_SECOND,
            )
            current_ns = next_anchor_ns
            continue
        _decay_recursion(
            recursion,
            decay,
            (proposed_ns - current_ns) / NANOSECONDS_PER_SECOND,
        )
        current_ns = proposed_ns
        active_intervals = _active_intervals(by_symbol, current_ns)
        proposed_intensities = _conditional_intensities(
            baseline,
            excitation,
            recursion,
            decay_per_second=decay,
            active_symbols=set(active_intervals),
            symbols=symbols,
            scale=missing_scale,
        )
        actual = sum(proposed_intensities)
        if actual <= 0.0 or rng.random() * upper > actual:
            continue
        destination = _sample_weighted_index(proposed_intensities, rng)
        destination_symbol = symbols[destination]
        interval = active_intervals.get(destination_symbol)
        if interval is None:
            raise MarkedHawkesGenerationError(
                "Ogata destination has no enclosing anchors"
            )
        left, right = interval
        interval_key = (
            destination_symbol,
            left.benchmark_event_id,
            right.benchmark_event_id,
        )
        per_interval[interval_key] += 1
        if per_interval[interval_key] > (
            config.limits.max_generated_events_per_interval
        ):
            raise MarkedHawkesGenerationError(
                "generated interval cardinality exceeds limit"
            )
        if len(generated) >= config.limits.max_generated_events_per_window:
            raise MarkedHawkesGenerationError(
                "generated window cardinality exceeds limit"
            )
        if len(generated) + 1 > max(
            1,
            math.floor(
                len(anchors) * config.limits.max_candidate_amplification
            ),
        ):
            raise MarkedHawkesGenerationError(
                "candidate amplification limit exceeded"
            )
        source_components = [
            missing_scale
            * decay
            * excitation[destination][source]
            * recursion[source]
            for source in range(dimension)
        ]
        component_weights = [
            missing_scale * baseline[destination],
            *source_components,
        ]
        component = _sample_weighted_index(component_weights, rng)
        source_symbol = None if component == 0 else symbols[component - 1]
        mark_probabilities = (
            _mapping(immigrant_marks[destination_symbol], "immigrant mark cell")
            if source_symbol is None
            else _mapping(
                _mapping(
                    excitation_marks[destination_symbol],
                    "excitation destination marks",
                )[source_symbol],
                "excitation source marks",
            )
        )
        mark = _sample_mark(mark_probabilities, rng)
        bid, ask = _project_quote(left, right, current_ns, mark)
        candidate_mid = (bid + ask) / 2.0
        ordinal = len(generated) + 1
        source_event_id = _stable_id(
            "marked-hawkes-generated-event",
            {
                "fit_id": fit.fit_id,
                "scenario_id": scenario.scenario_id,
                "window_id": window.window_id,
                "ensemble_member_id": ensemble_member_id,
                "destination_symbol": destination_symbol,
                "source_symbol": source_symbol,
                "event_time_ns": current_ns,
                "ordinal": ordinal,
            },
        )
        generated.append(
            BenchmarkEventV1(
                source_event_id=source_event_id,
                symbol=destination_symbol,
                event_time_ns=current_ns,
                event_sequence=ordinal,
                bid=bid,
                ask=ask,
                epoch_id=scenario.epoch_id,
                session=left.session,
                event_state=mark,
                sparsity=("marked-hawkes-" + config.excitation_structure.value),
                ensemble_member_id=ensemble_member_id,
                support_lower_mid=min(left.mid, right.mid, candidate_mid),
                support_upper_mid=max(left.mid, right.mid, candidate_mid),
            )
        )
        lineages.append(
            MarkedHawkesGenerationLineageV1(
                source_event_id=source_event_id,
                destination_symbol=destination_symbol,
                excitation_source_symbol=source_symbol,
                event_state=mark,
                conditional_intensity=proposed_intensities[destination],
            )
        )
        recursion[destination] += 1.0
    combined = tuple(
        sorted(
            (*anchors, *generated),
            key=lambda item: (
                item.event_time_ns,
                item.symbol,
                item.event_sequence,
                item.benchmark_event_id,
            ),
        )
    )
    if _benchmark_anchor_sha256(combined) != _benchmark_anchor_sha256(anchors):
        raise MarkedHawkesGenerationError(
            "generation changed immutable anchors"
        )
    return combined, tuple(lineages), proposal_counter[0]


def _active_intervals(
    by_symbol: Mapping[str, Sequence[BenchmarkEventV1]], current_ns: int
) -> dict[str, tuple[BenchmarkEventV1, BenchmarkEventV1]]:
    result = {}
    for symbol, anchors in by_symbol.items():
        for left, right in pairwise(anchors):
            if left.event_time_ns <= current_ns < right.event_time_ns:
                result[symbol] = (left, right)
                break
    return result


def _conditional_intensities(
    baseline: Sequence[float],
    excitation: Sequence[Sequence[float]],
    recursion: Sequence[float],
    *,
    decay_per_second: float,
    active_symbols: set[str],
    symbols: Sequence[str],
    scale: float,
) -> list[float]:
    return [
        (
            scale
            * (
                baseline[destination]
                + decay_per_second
                * sum(
                    excitation[destination][source] * recursion[source]
                    for source in range(len(symbols))
                )
            )
            if symbol in active_symbols
            else 0.0
        )
        for destination, symbol in enumerate(symbols)
    ]


def _decay_recursion(
    recursion: list[float], decay: float, elapsed_seconds: float
) -> None:
    factor = math.exp(-decay * max(0.0, elapsed_seconds))
    for index, value in enumerate(recursion):
        recursion[index] = value * factor


def _project_quote(
    left: BenchmarkEventV1,
    right: BenchmarkEventV1,
    event_time_ns: int,
    mark: str,
) -> tuple[float, float]:
    fraction = (event_time_ns - left.event_time_ns) / (
        right.event_time_ns - left.event_time_ns
    )
    interpolated_bid = left.bid + fraction * (right.bid - left.bid)
    interpolated_ask = left.ask + fraction * (right.ask - left.ask)
    if mark == "bid_only":
        bid, ask = min(interpolated_bid, left.ask), left.ask
    elif mark == "ask_only":
        bid, ask = left.bid, max(left.bid, interpolated_ask)
    elif mark == "joint":
        bid, ask = interpolated_bid, interpolated_ask
    else:
        bid, ask = left.bid, left.ask
    bid = max(1e-12, bid)
    ask = max(bid, ask)
    return bid, ask


def _validated_history(
    events: Sequence[BenchmarkEventV1],
    *,
    config: MarkedHawkesConfigV1,
    window: ReconstructionWindowV1,
) -> tuple[BenchmarkEventV1, ...]:
    values = tuple(events)
    if len(values) > config.limits.max_history_events:
        raise MarkedHawkesGenerationError("history cardinality exceeds limit")
    if any(not isinstance(item, BenchmarkEventV1) for item in values):
        raise MarkedHawkesGenerationError(
            "history contains a non-benchmark event"
        )
    lower_bound = window.core_start_ns - config.limits.max_history_ns
    symbols = {item.upper() for item in window.symbols}
    if any(
        item.event_time_ns >= window.core_start_ns
        or item.symbol.upper() not in symbols
        for item in values
    ):
        raise MarkedHawkesGenerationError(
            "history must be prior-only, bounded, and synchronized to symbols"
        )
    retained = tuple(
        item for item in values if item.event_time_ns >= lower_bound
    )
    estimated = len(retained) * config.limits.estimated_bytes_per_fit_event
    if estimated > config.limits.max_peak_memory_bytes:
        raise MarkedHawkesGenerationError(
            "history memory estimate exceeds limit"
        )
    return tuple(
        sorted(
            retained,
            key=lambda item: (
                item.event_time_ns,
                item.symbol,
                item.event_sequence,
                item.benchmark_event_id,
            ),
        )
    )


def _validate_fit_against_config(
    config: MarkedHawkesConfigV1, fit: MarkedHawkesFitResultV1
) -> None:
    if (
        fit.status is not MarkedHawkesFitStatus.FITTED
        or fit.config_id != config.config_id
        or fit.excitation_structure is not config.excitation_structure
    ):
        raise MarkedHawkesFitError("marked Hawkes fit is not usable by config")
    parameters = _mapping(fit.parameters, "fit parameters")
    if len(canonical_contract_json(parameters).encode()) > (
        config.limits.max_parameters_bytes
    ):
        raise MarkedHawkesFitError("fit parameter payload exceeds config limit")
    declared_limit = _finite_float(
        parameters.get("maximum_branching_ratio"),
        "maximum_branching_ratio",
    )
    if not math.isclose(
        declared_limit,
        config.maximum_branching_ratio,
        rel_tol=0.0,
        abs_tol=1e-12,
    ):
        raise MarkedHawkesFitError("fit branching-ratio policy differs")
    models = _mapping(
        parameters.get("conditioning_models"), "conditioning_models"
    )
    for model in models.values():
        _validate_model_stability(
            _mapping(model, "conditioning model"),
            excitation_structure=config.excitation_structure,
            maximum_branching_ratio=config.maximum_branching_ratio,
        )


def _validate_uncertainty(
    uncertainty: Mapping[str, JSONValue],
    *,
    parameters: Mapping[str, JSONValue],
) -> None:
    _require_literal(
        uncertainty,
        "method",
        "responsibility-count-wald-95-v1",
    )
    confidence = _finite_float(
        uncertainty.get("confidence_level"), "confidence_level"
    )
    if confidence != 0.95:
        raise ValueError("Hawkes uncertainty confidence level differs")
    models = _mapping(
        uncertainty.get("conditioning_models"),
        "uncertainty conditioning models",
    )
    parameter_models = _mapping(
        parameters.get("conditioning_models"), "conditioning_models"
    )
    if set(models) != set(parameter_models):
        raise ValueError("Hawkes uncertainty conditioning cells differ")
    for key, raw in models.items():
        model = _mapping(raw, f"uncertainty model {key}")
        _require_literal(
            model,
            "method",
            "responsibility-count-wald-95-v1",
        )
        _require_literal(
            model,
            "nonclaim",
            "descriptive-curvature-approximation-not-exact-coverage",
        )
        symbols = _string_tuple(
            _mapping(parameter_models[key], "conditioning model").get("symbols")
        )
        baseline = _sequence(model.get("baseline_rates_per_second"))
        excitation = _sequence(model.get("excitation_matrix"))
        if len(baseline) != len(symbols) or len(excitation) != len(symbols):
            raise ValueError("Hawkes uncertainty dimensions differ")


def _validate_fitted_parameters(
    parameters: Mapping[str, JSONValue],
    *,
    excitation_structure: HawkesExcitationStructure,
    expected_symbols: Sequence[str],
) -> None:
    _require_literal(parameters, "kernel", "exponential-integrated-mass-v1")
    if (
        str(parameters.get("excitation_structure"))
        != excitation_structure.value
    ):
        raise ValueError("Hawkes fitted excitation structure differs")
    if _string_tuple(parameters.get("symbols")) != tuple(expected_symbols):
        raise ValueError("Hawkes fitted symbols differ")
    if _string_tuple(parameters.get("mark_states")) != MARK_STATES:
        raise ValueError("Hawkes fitted mark-state registry differs")
    _require_literal(
        parameters,
        "conditioning_policy",
        "exact-epoch-session-then-session-v1",
    )
    _require_literal(
        parameters,
        "fit_boundary_policy",
        "reset-each-calibration-window-v1",
    )
    maximum = _positive_float(
        parameters.get("maximum_branching_ratio"),
        "maximum_branching_ratio",
    )
    if maximum >= 1.0:
        raise ValueError("fitted branching-ratio policy is unstable")
    models = _mapping(
        parameters.get("conditioning_models"), "conditioning_models"
    )
    if not models:
        raise ValueError("Hawkes fit lacks conditioning models")
    if not any(key.startswith("session|") for key in models):
        raise ValueError("Hawkes fit lacks session backoff support")
    for key, raw_model in models.items():
        if not (
            str(key).startswith("exact|") or str(key).startswith("session|")
        ):
            raise ValueError("Hawkes conditioning model key is invalid")
        model = _mapping(raw_model, "conditioning model")
        if _string_tuple(model.get("symbols")) != tuple(expected_symbols):
            raise ValueError("Hawkes conditioning model symbols differ")
        _validate_model_stability(
            model,
            excitation_structure=excitation_structure,
            maximum_branching_ratio=maximum,
        )
        immigrant = _mapping(
            model.get("immigrant_mark_probabilities"), "immigrant marks"
        )
        excited = _mapping(
            model.get("excitation_mark_probabilities"), "excitation marks"
        )
        if set(immigrant) != set(expected_symbols) or set(excited) != set(
            expected_symbols
        ):
            raise ValueError("Hawkes fitted mark destinations differ")
        for destination in expected_symbols:
            _validate_mark_probabilities(
                _mapping(immigrant[destination], "immigrant mark cell")
            )
            by_source = _mapping(excited[destination], "excited mark cell")
            if set(by_source) != set(expected_symbols):
                raise ValueError("Hawkes fitted mark sources differ")
            for source in expected_symbols:
                _validate_mark_probabilities(
                    _mapping(by_source[source], "excited source mark cell")
                )


def _validate_model_stability(
    model: Mapping[str, Any],
    *,
    excitation_structure: HawkesExcitationStructure,
    maximum_branching_ratio: float,
) -> None:
    symbols = _string_tuple(model.get("symbols"))
    if not symbols:
        raise ValueError("Hawkes model lacks symbols")
    dimension = len(symbols)
    _float_vector(
        model.get("baseline_rates_per_second"), dimension, "baseline rates"
    )
    matrix = _float_matrix(
        model.get("excitation_matrix"), dimension, "excitation matrix"
    )
    for destination in range(dimension):
        for source in range(dimension):
            value = matrix[destination][source]
            if value < 0.0:
                raise ValueError("Hawkes excitation mass is negative")
            if (
                excitation_structure is HawkesExcitationStructure.ZERO
                and value != 0.0
            ):
                raise ValueError("zero-excitation ablation contains excitation")
            if (
                excitation_structure is HawkesExcitationStructure.DIAGONAL
                and destination != source
                and value != 0.0
            ):
                raise ValueError("diagonal Hawkes ablation contains cross mass")
    radius = _spectral_radius(matrix)
    declared = _finite_float(model.get("spectral_radius"), "spectral_radius")
    if not math.isclose(radius, declared, rel_tol=1e-9, abs_tol=1e-10):
        raise ValueError("declared Hawkes spectral radius differs")
    if radius >= maximum_branching_ratio or radius >= 1.0:
        raise ValueError("Hawkes excitation is unstable")
    margin = _finite_float(model.get("stability_margin"), "stability_margin")
    if not math.isclose(margin, 1.0 - radius, rel_tol=1e-9, abs_tol=1e-10):
        raise ValueError("declared Hawkes stability margin differs")


def _spectral_radius(matrix: Sequence[Sequence[float]]) -> float:
    dimension = len(matrix)
    if dimension == 0 or any(len(row) != dimension for row in matrix):
        raise ValueError("spectral-radius matrix must be nonempty and square")
    if all(value == 0.0 for row in matrix for value in row):
        return 0.0
    if all(
        value == 0.0
        for destination, row in enumerate(matrix)
        for source, value in enumerate(row)
        if destination != source
    ):
        return max(matrix[index][index] for index in range(dimension))
    vector = [1.0 / dimension] * dimension
    for _iteration in range(2_048):
        product = [
            sum(
                matrix[row][column] * vector[column]
                for column in range(dimension)
            )
            for row in range(dimension)
        ]
        norm = sum(product)
        if norm <= 0.0:
            return 0.0
        updated = [value / norm for value in product]
        if (
            max(abs(left - right) for left, right in zip(updated, vector))
            < 1e-14
        ):
            vector = updated
            break
        vector = updated
    product = [
        sum(matrix[row][column] * vector[column] for column in range(dimension))
        for row in range(dimension)
    ]
    denominator = sum(value * value for value in vector)
    radius = (
        sum(left * right for left, right in zip(vector, product)) / denominator
    )
    if not math.isfinite(radius) or radius < 0.0:
        raise ValueError("spectral-radius calculation failed")
    return radius


def _window_marks(events: Sequence[BenchmarkEventV1]) -> dict[str, str]:
    result: dict[str, str] = {}
    by_symbol: dict[str, list[BenchmarkEventV1]] = defaultdict(list)
    for event in events:
        by_symbol[event.symbol].append(event)
    for values in by_symbol.values():
        ordered = sorted(
            values,
            key=lambda item: (
                item.event_time_ns,
                item.event_sequence,
                item.benchmark_event_id,
            ),
        )
        result[ordered[0].benchmark_event_id] = "unchanged"
        for left, right in pairwise(ordered):
            bid_changed = right.bid != left.bid
            ask_changed = right.ask != left.ask
            if bid_changed and ask_changed:
                mark = "joint"
            elif bid_changed:
                mark = "bid_only"
            elif ask_changed:
                mark = "ask_only"
            else:
                mark = "unchanged"
            result[right.benchmark_event_id] = mark
    return result


def _normalized_probabilities(values: Mapping[str, float]) -> dict[str, float]:
    total = sum(values.values())
    if total <= 0.0:
        raise ValueError("mark probability mass is nonpositive")
    return {mark: values[mark] / total for mark in MARK_STATES}


def _validate_mark_probabilities(values: Mapping[str, Any]) -> None:
    if set(values) != set(MARK_STATES):
        raise ValueError("Hawkes mark probability states differ")
    probabilities = [
        _finite_float(values[mark], f"mark probability {mark}")
        for mark in MARK_STATES
    ]
    if any(
        value < 0.0 or value > 1.0 for value in probabilities
    ) or not math.isclose(sum(probabilities), 1.0, rel_tol=1e-9, abs_tol=1e-9):
        raise ValueError("Hawkes mark probabilities are invalid")


def _sample_mark(values: Mapping[str, Any], rng: random.Random) -> str:
    _validate_mark_probabilities(values)
    threshold = rng.random()
    cumulative = 0.0
    for mark in MARK_STATES:
        cumulative += float(values[mark])
        if threshold <= cumulative:
            return mark
    return MARK_STATES[-1]


def _sample_weighted_index(values: Sequence[float], rng: random.Random) -> int:
    total = sum(values)
    if total <= 0.0 or any(value < 0.0 for value in values):
        raise ValueError("weighted sample requires nonnegative positive mass")
    threshold = rng.random() * total
    cumulative = 0.0
    for index, value in enumerate(values):
        cumulative += value
        if threshold <= cumulative:
            return index
    return len(values) - 1


def _missing_intensity_scale(scenario: BenchmarkScenarioV1) -> float:
    raw = scenario.degradation_parameters.get("retention_probability")
    if raw is None:
        return 1.0
    retention = _finite_float(raw, "retention_probability")
    if retention < 0.0 or retention > 1.0:
        raise MarkedHawkesGenerationError(
            "retention probability is outside [0,1]"
        )
    return 1.0 - retention


def _calibration_hash(
    windows: Sequence[EventClockCalibrationWindowV1],
) -> str:
    return hashlib.sha256(
        canonical_contract_json(
            [
                item.metadata()
                for item in sorted(windows, key=lambda value: value.window_id)
            ]
        ).encode()
    ).hexdigest()


def _benchmark_content_sha256(
    events: Sequence[BenchmarkEventV1],
) -> str:
    return hashlib.sha256(
        canonical_contract_json([item.to_dict() for item in events]).encode()
    ).hexdigest()


def _benchmark_anchor_sha256(
    events: Sequence[BenchmarkEventV1],
) -> str:
    return _anchor_sha256(
        (
            item.symbol,
            item.event_time_ns,
            item.event_sequence,
            item.bid,
            item.ask,
        )
        for item in events
        if not item.sparsity.startswith("marked-hawkes-")
    )


def _synthetic_anchor_sha256(
    events: Sequence[SyntheticEventV1],
) -> str:
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


def _anchor_sha256(
    values: Sequence[tuple[str, int, int, float, float]] | Any,
) -> str:
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


def _lineage_sha256(
    values: Sequence[MarkedHawkesGenerationLineageV1],
) -> str:
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


def _incremental_peak_rss_bytes(before_peak: int) -> int:
    return int(max(0, peak_rss_bytes() - before_peak))


def _semantic_seed(base_seed: int, *values: str) -> int:
    payload = canonical_contract_json(
        {"base_seed": base_seed, "values": list(values)}
    )
    return int.from_bytes(hashlib.sha256(payload.encode()).digest()[:8], "big")


def _generator_id(structure: HawkesExcitationStructure) -> str:
    return f"{MARKED_HAWKES_GENERATOR_PREFIX}.{structure.value}"


def _exact_key(epoch_id: str, session: str) -> str:
    epoch = _required_text(epoch_id)
    session_value = _required_text(session)
    if "|" in epoch or "|" in session_value:
        raise ValueError("conditioning labels cannot contain pipe separators")
    return f"exact|{epoch}|{session_value}"


def _session_key(session: str) -> str:
    value = _required_text(session)
    if "|" in value:
        raise ValueError("conditioning labels cannot contain pipe separators")
    return f"session|{value}"


def _float_vector(value: Any, size: int, name: str) -> list[float]:
    values = _sequence(value)
    if len(values) != size:
        raise ValueError(f"{name} length differs")
    result = [_finite_float(item, name) for item in values]
    if any(item <= 0.0 for item in result):
        raise ValueError(f"{name} must be positive")
    return result


def _float_matrix(value: Any, size: int, name: str) -> list[list[float]]:
    rows = _sequence(value)
    if len(rows) != size:
        raise ValueError(f"{name} row count differs")
    result = []
    for row in rows:
        values = _sequence(row)
        if len(values) != size:
            raise ValueError(f"{name} column count differs")
        result.append([_finite_float(item, name) for item in values])
    return result


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode()
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _require_schema(data: Mapping[str, Any], expected: str) -> None:
    if data.get("schema_version") != expected:
        raise ValueError(f"schema_version must be {expected}")


def _require_schema_value(value: str, expected: str, name: str) -> None:
    if value != expected:
        raise ValueError(f"unsupported {name} schema")


def _require_literal(data: Mapping[str, Any], key: str, expected: str) -> None:
    if data.get(key) != expected:
        raise ValueError(f"{key} must be {expected}")


def _required_text(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("value must be non-empty text")
    return value.strip()


def _optional_text(value: Any) -> str | None:
    return None if value is None else _required_text(value)


def _strict_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be boolean")
    return value


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    return value


def _bounded_int(value: Any, name: str, lower: int, upper: int) -> int:
    integer = _strict_int(value, name)
    if not lower <= integer <= upper:
        raise ValueError(f"{name} is outside [{lower},{upper}]")
    return integer


def _finite_float(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _positive_float(value: Any, name: str) -> float:
    result = _finite_float(value, name)
    if result <= 0.0:
        raise ValueError(f"{name} must be positive")
    return result


def _optional_float(value: Any) -> float | None:
    return None if value is None else _finite_float(value, "optional float")


def _optional_int(value: Any) -> int | None:
    return None if value is None else _strict_int(value, "optional int")


def _sequence(value: Any) -> tuple[Any, ...]:
    if not isinstance(value, (list, tuple)):
        raise TypeError("value must be a sequence")
    return tuple(value)


def _string_tuple(value: Any) -> tuple[str, ...]:
    return tuple(_required_text(item) for item in _sequence(value))


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    return value


def _symbol(value: Any) -> str:
    symbol = _required_text(value).upper()
    if not symbol.isascii() or not symbol.isalnum() or len(symbol) > 32:
        raise ValueError("invalid marked Hawkes symbol")
    return symbol


def _sha256(value: Any, name: str) -> str:
    text = _required_text(value)
    if len(text) != 64 or any(
        character not in "0123456789abcdef" for character in text
    ):
        raise ValueError(f"{name} must be lowercase SHA-256")
    return text


def _json_mapping(
    value: Mapping[str, JSONValue], name: str
) -> dict[str, JSONValue]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    encoded = canonical_contract_json(dict(value))
    return cast(dict[str, JSONValue], json.loads(encoded))


def _json_scalar(value: Any, name: str) -> JSONScalar:
    if value is None or isinstance(value, (str, bool)):
        return cast(JSONScalar, value)
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    raise ValueError(f"{name} must be a finite JSON scalar")


__all__ = [
    "FittedMarkedHawkesBenchmarkGeneratorV1",
    "HawkesExcitationStructure",
    "MARKED_HAWKES_CANDIDATE_BATCH_SCHEMA_VERSION",
    "MARKED_HAWKES_CANDIDATE_LINEAGE_SCHEMA_VERSION",
    "MARKED_HAWKES_CONFIG_SCHEMA_VERSION",
    "MARKED_HAWKES_FIT_RESULT_SCHEMA_VERSION",
    "MARKED_HAWKES_GENERATION_EVIDENCE_SCHEMA_VERSION",
    "MARKED_HAWKES_GENERATION_LINEAGE_SCHEMA_VERSION",
    "MARKED_HAWKES_IMPLEMENTATION_VERSION",
    "MARKED_HAWKES_RESOURCE_LIMITS_SCHEMA_VERSION",
    "MarkedHawkesCandidateBatchV1",
    "MarkedHawkesCandidateLineageV1",
    "MarkedHawkesConfigV1",
    "MarkedHawkesFitError",
    "MarkedHawkesFitResultV1",
    "MarkedHawkesFitStatus",
    "MarkedHawkesGenerationError",
    "MarkedHawkesGenerationEvidenceV1",
    "MarkedHawkesGenerationLineageV1",
    "MarkedHawkesGenerationResultV1",
    "MarkedHawkesGenerationStatus",
    "MarkedHawkesResourceLimitsV1",
    "build_fitted_marked_hawkes_generator",
    "build_marked_hawkes_benchmark_candidate",
    "build_marked_hawkes_candidate_batches",
    "default_marked_hawkes_configs",
    "fit_marked_hawkes_challenger",
]
