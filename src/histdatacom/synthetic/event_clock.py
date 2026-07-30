"""Bounded classical event-clock challengers for reconstruction research.

The contracts in this module are opt-in research surfaces.  They fit only
explicit calibration windows, generate one synchronized multi-symbol window,
and expose deterministic model/config identities plus bounded execution
evidence.  They do not select or replace the empirical production generator.
"""

from __future__ import annotations

import hashlib
import json
import math
import random
import statistics
import time
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from enum import Enum
from itertools import pairwise
from typing import Any, Protocol, TypeAlias, cast, runtime_checkable

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
from histdatacom.synthetic.generation import (
    CANDIDATE_ONLY_CONSTRAINT_SET_ID,
    MotifGenerationStatus,
)
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.streaming import (
    ReconstructionRunV1,
    ReconstructionWindowV1,
)

EVENT_CLOCK_RESOURCE_LIMITS_SCHEMA_VERSION = (
    "histdatacom.event-clock-resource-limits.v1"
)
NHPP_EVENT_CLOCK_CONFIG_SCHEMA_VERSION = (
    "histdatacom.event-clock-nhpp-config.v1"
)
COX_EVENT_CLOCK_CONFIG_SCHEMA_VERSION = "histdatacom.event-clock-cox-config.v1"
ACD_EVENT_CLOCK_CONFIG_SCHEMA_VERSION = "histdatacom.event-clock-acd-config.v1"
HIDDEN_MARKOV_EVENT_CLOCK_CONFIG_SCHEMA_VERSION = (
    "histdatacom.event-clock-hidden-markov-config.v1"
)
EVENT_CLOCK_CALIBRATION_WINDOW_SCHEMA_VERSION = (
    "histdatacom.event-clock-calibration-window.v1"
)
EVENT_CLOCK_FIT_RESULT_SCHEMA_VERSION = "histdatacom.event-clock-fit-result.v1"
EVENT_CLOCK_GENERATION_EVIDENCE_SCHEMA_VERSION = (
    "histdatacom.event-clock-generation-evidence.v1"
)
EVENT_CLOCK_CANDIDATE_BATCH_SCHEMA_VERSION = (
    "histdatacom.event-clock-candidate-batch.v1"
)
EVENT_CLOCK_CANDIDATE_LINEAGE_SCHEMA_VERSION = (
    "histdatacom.event-clock-candidate-lineage.v1"
)

NHPP_GENERATOR_ID = "histdatacom.event-clock.nhpp"
COX_GENERATOR_ID = "histdatacom.event-clock.cox"
ACD_GENERATOR_ID = "histdatacom.event-clock.acd"
HIDDEN_MARKOV_GENERATOR_ID = (
    "histdatacom.event-clock.hidden-markov-duration-mark"
)
EVENT_CLOCK_IMPLEMENTATION_VERSION = "1.0.0"

MAX_EVENT_CLOCK_FIT_EVENTS = 100_000
MAX_EVENT_CLOCK_FIT_WINDOWS = 256
MAX_EVENT_CLOCK_ITERATIONS = 512
MAX_EVENT_CLOCK_GENERATED_EVENTS = 100_000
MAX_EVENT_CLOCK_HISTORY_EVENTS = 100_000
MAX_EVENT_CLOCK_HISTORY_NS = 7 * 86_400 * 1_000_000_000
MAX_EVENT_CLOCK_DIAGNOSTICS = 64
MAX_EVENT_CLOCK_PARAMETERS_BYTES = 1_000_000
NANOSECONDS_PER_SECOND = 1_000_000_000
SECONDS_PER_DAY = 86_400.0


class EventClockFamily(str, Enum):
    """Supported transparent classical event-time families."""

    NHPP = "non_homogeneous_poisson"
    COX = "cox_gamma_poisson"
    ACD = "acd_1_1_exponential"
    HIDDEN_MARKOV = "hidden_markov_duration_mark"


class EventClockFitStatus(str, Enum):
    """Terminal status of a bounded calibration attempt."""

    FITTED = "fitted"
    REFUSED = "refused"
    FAILED = "failed"


class EventClockGenerationStatus(str, Enum):
    """Terminal status of a bounded synchronized generation attempt."""

    GENERATED = "generated"
    EMPTY = "empty"
    REFUSED = "refused"
    FAILED = "failed"


class EventClockFitError(ValueError):
    """Raised when callers request a fitted generator from a failed fit."""


class EventClockGenerationError(ValueError):
    """Raised when generation violates support or resource contracts."""


@dataclass(frozen=True, slots=True)
class EventClockResourceLimitsV1:
    """Hard fit, diagnostic, and generation bounds shared by all families."""

    max_fit_events: int = 20_000
    max_fit_windows: int = 96
    max_iterations: int = 128
    max_generated_events_per_interval: int = 1_024
    max_generated_events_per_window: int = 8_192
    max_history_events: int = 4_096
    max_history_ns: int = 3_600 * NANOSECONDS_PER_SECOND
    max_peak_memory_bytes: int = 512 * 1024**2
    max_diagnostics: int = 32
    estimated_bytes_per_fit_event: int = 512
    estimated_bytes_per_generated_event: int = 768
    limits_id: str = ""
    schema_version: str = EVENT_CLOCK_RESOURCE_LIMITS_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EVENT_CLOCK_RESOURCE_LIMITS_SCHEMA_VERSION:
            raise ValueError("unsupported event-clock resource limits")
        bounds = (
            (
                "max_fit_events",
                self.max_fit_events,
                2,
                MAX_EVENT_CLOCK_FIT_EVENTS,
            ),
            (
                "max_fit_windows",
                self.max_fit_windows,
                1,
                MAX_EVENT_CLOCK_FIT_WINDOWS,
            ),
            (
                "max_iterations",
                self.max_iterations,
                1,
                MAX_EVENT_CLOCK_ITERATIONS,
            ),
            (
                "max_generated_events_per_interval",
                self.max_generated_events_per_interval,
                1,
                MAX_EVENT_CLOCK_GENERATED_EVENTS,
            ),
            (
                "max_generated_events_per_window",
                self.max_generated_events_per_window,
                1,
                MAX_EVENT_CLOCK_GENERATED_EVENTS,
            ),
            (
                "max_history_events",
                self.max_history_events,
                0,
                MAX_EVENT_CLOCK_HISTORY_EVENTS,
            ),
            (
                "max_history_ns",
                self.max_history_ns,
                0,
                MAX_EVENT_CLOCK_HISTORY_NS,
            ),
            (
                "max_peak_memory_bytes",
                self.max_peak_memory_bytes,
                1,
                16 * 1024**3,
            ),
            (
                "max_diagnostics",
                self.max_diagnostics,
                1,
                MAX_EVENT_CLOCK_DIAGNOSTICS,
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
        if (
            self.max_generated_events_per_interval
            > self.max_generated_events_per_window
        ):
            raise ValueError(
                "per-interval generation limit exceeds window limit"
            )
        expected = _stable_id(
            "event-clock-resource-limits", self.identity_payload()
        )
        if self.limits_id and self.limits_id != expected:
            raise ValueError("event-clock limits_id differs")
        object.__setattr__(self, "limits_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "max_fit_events": self.max_fit_events,
            "max_fit_windows": self.max_fit_windows,
            "max_iterations": self.max_iterations,
            "max_generated_events_per_interval": (
                self.max_generated_events_per_interval
            ),
            "max_generated_events_per_window": self.max_generated_events_per_window,
            "max_history_events": self.max_history_events,
            "max_history_ns": self.max_history_ns,
            "max_peak_memory_bytes": self.max_peak_memory_bytes,
            "max_diagnostics": self.max_diagnostics,
            "estimated_bytes_per_fit_event": self.estimated_bytes_per_fit_event,
            "estimated_bytes_per_generated_event": (
                self.estimated_bytes_per_generated_event
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "limits_id": self.limits_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EventClockResourceLimitsV1:
        _require_schema(data, EVENT_CLOCK_RESOURCE_LIMITS_SCHEMA_VERSION)
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
            max_generated_events_per_interval=_strict_int(
                data.get("max_generated_events_per_interval"),
                "max_generated_events_per_interval",
            ),
            max_generated_events_per_window=_strict_int(
                data.get("max_generated_events_per_window"),
                "max_generated_events_per_window",
            ),
            max_history_events=_strict_int(
                data.get("max_history_events"), "max_history_events"
            ),
            max_history_ns=_strict_int(
                data.get("max_history_ns"), "max_history_ns"
            ),
            max_peak_memory_bytes=_strict_int(
                data.get("max_peak_memory_bytes"), "max_peak_memory_bytes"
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


@runtime_checkable
class EventClockConfigV1(Protocol):
    """Structural configuration boundary used by the shared fit lifecycle."""

    @property
    def family(self) -> EventClockFamily: ...

    @property
    def base_seed(self) -> int: ...

    @property
    def minimum_events_per_symbol(self) -> int: ...

    @property
    def minimum_conditioning_events(self) -> int: ...

    @property
    def limits(self) -> EventClockResourceLimitsV1: ...

    @property
    def config_id(self) -> str: ...

    @property
    def schema_version(self) -> str: ...

    def to_dict(self) -> dict[str, JSONValue]:
        """Return the exact versioned configuration payload."""


@dataclass(frozen=True, slots=True)
class NonHomogeneousPoissonConfigV1:
    """Piecewise-constant time-of-day NHPP configuration."""

    intensity_bin_count: int = 24
    smoothing_count: float = 0.5
    base_seed: int = 450_001
    minimum_events_per_symbol: int = 8
    minimum_conditioning_events: int = 2
    limits: EventClockResourceLimitsV1 = field(
        default_factory=EventClockResourceLimitsV1
    )
    config_id: str = ""
    family: EventClockFamily = field(init=False, default=EventClockFamily.NHPP)
    schema_version: str = NHPP_EVENT_CLOCK_CONFIG_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _validate_common_config(self)
        _bounded_int(self.intensity_bin_count, "intensity_bin_count", 2, 288)
        _positive_float(self.smoothing_count, "smoothing_count")
        _set_config_id(self, "event-clock-nhpp-config")

    def to_dict(self) -> dict[str, JSONValue]:
        return _config_dict(
            self,
            {
                "intensity_bin_count": self.intensity_bin_count,
                "smoothing_count": self.smoothing_count,
            },
        )


@dataclass(frozen=True, slots=True)
class CoxProcessConfigV1:
    """Gamma-mixed Cox process configuration."""

    minimum_gamma_shape: float = 0.05
    maximum_gamma_shape: float = 10_000.0
    dispersion_floor: float = 1e-9
    base_seed: int = 450_002
    minimum_events_per_symbol: int = 8
    minimum_conditioning_events: int = 2
    limits: EventClockResourceLimitsV1 = field(
        default_factory=EventClockResourceLimitsV1
    )
    config_id: str = ""
    family: EventClockFamily = field(init=False, default=EventClockFamily.COX)
    schema_version: str = COX_EVENT_CLOCK_CONFIG_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _validate_common_config(self)
        lower = _positive_float(self.minimum_gamma_shape, "minimum_gamma_shape")
        upper = _positive_float(self.maximum_gamma_shape, "maximum_gamma_shape")
        if upper <= lower:
            raise ValueError(
                "maximum_gamma_shape must exceed minimum_gamma_shape"
            )
        _positive_float(self.dispersion_floor, "dispersion_floor")
        _set_config_id(self, "event-clock-cox-config")

    def to_dict(self) -> dict[str, JSONValue]:
        return _config_dict(
            self,
            {
                "minimum_gamma_shape": self.minimum_gamma_shape,
                "maximum_gamma_shape": self.maximum_gamma_shape,
                "dispersion_floor": self.dispersion_floor,
            },
        )


@dataclass(frozen=True, slots=True)
class AutoregressiveConditionalDurationConfigV1:
    """Exponential ACD(1,1) bounded-grid configuration."""

    coefficient_grid_size: int = 8
    stationarity_margin: float = 0.01
    minimum_conditional_duration_seconds: float = 1e-9
    base_seed: int = 450_003
    minimum_events_per_symbol: int = 8
    minimum_conditioning_events: int = 2
    limits: EventClockResourceLimitsV1 = field(
        default_factory=EventClockResourceLimitsV1
    )
    config_id: str = ""
    family: EventClockFamily = field(init=False, default=EventClockFamily.ACD)
    schema_version: str = ACD_EVENT_CLOCK_CONFIG_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _validate_common_config(self)
        _bounded_int(self.coefficient_grid_size, "coefficient_grid_size", 2, 32)
        margin = _finite_float(self.stationarity_margin, "stationarity_margin")
        if not 0.0 < margin < 1.0:
            raise ValueError("stationarity_margin must be inside (0,1)")
        _positive_float(
            self.minimum_conditional_duration_seconds,
            "minimum_conditional_duration_seconds",
        )
        if self.coefficient_grid_size**2 > self.limits.max_iterations:
            raise ValueError("ACD coefficient grid exceeds iteration limit")
        _set_config_id(self, "event-clock-acd-config")

    def to_dict(self) -> dict[str, JSONValue]:
        return _config_dict(
            self,
            {
                "coefficient_grid_size": self.coefficient_grid_size,
                "stationarity_margin": self.stationarity_margin,
                "minimum_conditional_duration_seconds": (
                    self.minimum_conditional_duration_seconds
                ),
            },
        )


@dataclass(frozen=True, slots=True)
class HiddenMarkovDurationMarkConfigV1:
    """Two-state log-duration/categorical-mark hard-EM configuration."""

    state_count: int = 2
    convergence_tolerance: float = 1e-6
    variance_floor: float = 1e-6
    probability_smoothing: float = 0.5
    base_seed: int = 450_004
    minimum_events_per_symbol: int = 12
    minimum_conditioning_events: int = 2
    limits: EventClockResourceLimitsV1 = field(
        default_factory=EventClockResourceLimitsV1
    )
    config_id: str = ""
    family: EventClockFamily = field(
        init=False, default=EventClockFamily.HIDDEN_MARKOV
    )
    schema_version: str = HIDDEN_MARKOV_EVENT_CLOCK_CONFIG_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _validate_common_config(self)
        if self.state_count != 2:
            raise ValueError("event-clock hidden Markov v1 requires two states")
        _positive_float(self.convergence_tolerance, "convergence_tolerance")
        _positive_float(self.variance_floor, "variance_floor")
        _positive_float(self.probability_smoothing, "probability_smoothing")
        _set_config_id(self, "event-clock-hidden-markov-config")

    def to_dict(self) -> dict[str, JSONValue]:
        return _config_dict(
            self,
            {
                "state_count": self.state_count,
                "convergence_tolerance": self.convergence_tolerance,
                "variance_floor": self.variance_floor,
                "probability_smoothing": self.probability_smoothing,
            },
        )


EventClockConfigurationV1: TypeAlias = (
    NonHomogeneousPoissonConfigV1
    | CoxProcessConfigV1
    | AutoregressiveConditionalDurationConfigV1
    | HiddenMarkovDurationMarkConfigV1
)


@dataclass(frozen=True, slots=True)
class EventClockCalibrationWindowV1:
    """Process-local calibration rows plus a row-free deterministic identity."""

    window_id: str
    start_ns: int
    end_ns: int
    events: tuple[BenchmarkEventV1, ...]
    split_kind: str = "calibration"
    content_sha256: str = ""
    schema_version: str = EVENT_CLOCK_CALIBRATION_WINDOW_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EVENT_CLOCK_CALIBRATION_WINDOW_SCHEMA_VERSION:
            raise ValueError("unsupported event-clock calibration window")
        object.__setattr__(self, "window_id", _required_text(self.window_id))
        if self.split_kind != "calibration":
            raise ValueError("event-clock fit accepts calibration windows only")
        start = _strict_int(self.start_ns, "start_ns")
        end = _strict_int(self.end_ns, "end_ns")
        if end <= start:
            raise ValueError("calibration end must exceed start")
        ordered = tuple(
            sorted(
                self.events,
                key=lambda item: (
                    item.event_time_ns,
                    item.symbol,
                    item.event_sequence,
                    item.benchmark_event_id,
                ),
            )
        )
        if not ordered or any(
            not isinstance(item, BenchmarkEventV1) for item in ordered
        ):
            raise ValueError("calibration window requires benchmark events")
        if any(not start <= item.event_time_ns < end for item in ordered):
            raise ValueError("calibration event lies outside its window")
        object.__setattr__(self, "events", ordered)
        expected = hashlib.sha256(
            canonical_contract_json(
                [item.to_dict() for item in ordered]
            ).encode()
        ).hexdigest()
        if self.content_sha256 and self.content_sha256 != expected:
            raise ValueError("calibration content hash differs")
        object.__setattr__(self, "content_sha256", expected)

    def metadata(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "window_id": self.window_id,
            "split_kind": self.split_kind,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "event_count": len(self.events),
            "symbols": cast(
                JSONValue, sorted({item.symbol for item in self.events})
            ),
            "content_sha256": self.content_sha256,
            "events_inline": False,
        }


@dataclass(frozen=True, slots=True)
class EventClockFitResultV1:
    """Deterministic fitted model plus bounded convergence/failure evidence."""

    family: EventClockFamily
    config_id: str
    calibration_content_sha256: str
    information_mode: InformationMode
    symbols: tuple[str, ...]
    status: EventClockFitStatus
    converged: bool
    iteration_count: int
    fitted_event_count: int
    fitted_window_count: int
    log_likelihood: float | None
    parameters: Mapping[str, JSONValue]
    diagnostics: Mapping[str, JSONScalar]
    estimated_peak_memory_bytes: int
    failure_reason: str | None = None
    as_of_ns: int | None = None
    fit_id: str = ""
    schema_version: str = EVENT_CLOCK_FIT_RESULT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EVENT_CLOCK_FIT_RESULT_SCHEMA_VERSION:
            raise ValueError("unsupported event-clock fit result")
        object.__setattr__(self, "family", EventClockFamily(self.family))
        object.__setattr__(self, "config_id", _required_text(self.config_id))
        _sha256(self.calibration_content_sha256, "calibration_content_sha256")
        mode = InformationMode.from_value(self.information_mode)
        object.__setattr__(self, "information_mode", mode)
        symbols = tuple(sorted({_symbol(item) for item in self.symbols}))
        if not symbols:
            raise ValueError("event-clock fit requires symbols")
        object.__setattr__(self, "symbols", symbols)
        status = EventClockFitStatus(self.status)
        object.__setattr__(self, "status", status)
        converged = _strict_bool(self.converged, "converged")
        object.__setattr__(self, "converged", converged)
        for name in (
            "iteration_count",
            "fitted_event_count",
            "fitted_window_count",
            "estimated_peak_memory_bytes",
        ):
            value = _strict_int(getattr(self, name), name)
            if value < 0:
                raise ValueError(f"{name} must be nonnegative")
        likelihood = self.log_likelihood
        if likelihood is not None:
            likelihood = _finite_float(likelihood, "log_likelihood")
            object.__setattr__(self, "log_likelihood", likelihood)
        parameters = _json_mapping(self.parameters, "parameters")
        diagnostics = {
            _required_text(str(key)): _json_scalar(value, str(key))
            for key, value in self.diagnostics.items()
        }
        if len(diagnostics) > MAX_EVENT_CLOCK_DIAGNOSTICS:
            raise ValueError("event-clock diagnostics exceed hard bound")
        if len(canonical_contract_json(parameters).encode()) > (
            MAX_EVENT_CLOCK_PARAMETERS_BYTES
        ):
            raise ValueError("event-clock parameters exceed hard bound")
        object.__setattr__(self, "parameters", parameters)
        object.__setattr__(
            self, "diagnostics", dict(sorted(diagnostics.items()))
        )
        failure = _optional_text(self.failure_reason)
        object.__setattr__(self, "failure_reason", failure)
        if status is EventClockFitStatus.FITTED:
            if not converged or failure is not None or not parameters:
                raise ValueError(
                    "fitted event-clock result lacks converged model"
                )
        elif converged or failure is None or parameters:
            raise ValueError(
                "failed/refused fit must fail closed without parameters"
            )
        if mode is InformationMode.EX_ANTE_SIMULATION:
            if self.as_of_ns is None:
                raise ValueError("ex-ante event-clock fit requires as_of_ns")
            _strict_int(self.as_of_ns, "as_of_ns")
        elif self.as_of_ns is not None:
            raise ValueError("ex-post event-clock fit rejects as_of_ns")
        expected = _stable_id("event-clock-fit", self.identity_payload())
        if self.fit_id and self.fit_id != expected:
            raise ValueError("event-clock fit_id differs")
        object.__setattr__(self, "fit_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "family": self.family.value,
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
            "diagnostics": dict(self.diagnostics),
            "estimated_peak_memory_bytes": self.estimated_peak_memory_bytes,
            "failure_reason": self.failure_reason,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "fit_id": self.fit_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EventClockFitResultV1:
        _require_schema(data, EVENT_CLOCK_FIT_RESULT_SCHEMA_VERSION)
        return cls(
            family=EventClockFamily(str(data.get("family", ""))),
            config_id=str(data.get("config_id", "")),
            calibration_content_sha256=str(
                data.get("calibration_content_sha256", "")
            ),
            information_mode=InformationMode.from_value(
                str(data.get("information_mode", ""))
            ),
            as_of_ns=_optional_int(data.get("as_of_ns")),
            symbols=_string_tuple(data.get("symbols")),
            status=EventClockFitStatus(str(data.get("status", ""))),
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
    def from_json(cls, text: str) -> EventClockFitResultV1:
        return cls.from_dict(_json_mapping(json.loads(text), "fit result"))


@dataclass(frozen=True, slots=True)
class EventClockGenerationEvidenceV1:
    """Bounded measured evidence for one synchronized generation attempt."""

    fit_id: str
    window_id: str
    ensemble_member_id: str
    status: EventClockGenerationStatus
    attempted: bool
    generated_event_count: int
    input_event_count: int
    history_event_count: int
    input_anchor_sha256: str | None
    conditioning_support_level: str
    wall_time_ms: int
    peak_memory_bytes: int
    failure_reason: str | None = None
    evidence_id: str = ""
    schema_version: str = EVENT_CLOCK_GENERATION_EVIDENCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != EVENT_CLOCK_GENERATION_EVIDENCE_SCHEMA_VERSION
        ):
            raise ValueError("unsupported event-clock generation evidence")
        for name in ("fit_id", "window_id", "ensemble_member_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self, "status", EventClockGenerationStatus(self.status)
        )
        object.__setattr__(
            self, "attempted", _strict_bool(self.attempted, "attempted")
        )
        for name in (
            "generated_event_count",
            "input_event_count",
            "history_event_count",
            "wall_time_ms",
            "peak_memory_bytes",
        ):
            value = _strict_int(getattr(self, name), name)
            if value < 0:
                raise ValueError(f"{name} must be nonnegative")
        failure = _optional_text(self.failure_reason)
        object.__setattr__(self, "failure_reason", failure)
        input_anchor_sha256 = self.input_anchor_sha256
        if input_anchor_sha256 is not None:
            input_anchor_sha256 = _sha256(
                input_anchor_sha256, "input_anchor_sha256"
            )
        object.__setattr__(self, "input_anchor_sha256", input_anchor_sha256)
        object.__setattr__(
            self,
            "conditioning_support_level",
            _required_text(self.conditioning_support_level),
        )
        if (
            self.status
            in {
                EventClockGenerationStatus.REFUSED,
                EventClockGenerationStatus.FAILED,
            }
            and failure is None
        ):
            raise ValueError("failed generation evidence requires a reason")
        if (
            self.status
            in {
                EventClockGenerationStatus.GENERATED,
                EventClockGenerationStatus.EMPTY,
            }
            and failure is not None
        ):
            raise ValueError(
                "successful/empty generation cannot have a failure"
            )
        if (
            self.status
            in {
                EventClockGenerationStatus.GENERATED,
                EventClockGenerationStatus.EMPTY,
            }
            and input_anchor_sha256 is None
        ):
            raise ValueError(
                "successful/empty generation requires an input anchor digest"
            )
        expected = _stable_id(
            "event-clock-generation-evidence", self.identity_payload()
        )
        if self.evidence_id and self.evidence_id != expected:
            raise ValueError("event-clock generation evidence_id differs")
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
            "input_anchor_sha256": self.input_anchor_sha256,
            "conditioning_support_level": self.conditioning_support_level,
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
    ) -> EventClockGenerationEvidenceV1:
        _require_schema(data, EVENT_CLOCK_GENERATION_EVIDENCE_SCHEMA_VERSION)
        return cls(
            fit_id=str(data.get("fit_id", "")),
            window_id=str(data.get("window_id", "")),
            ensemble_member_id=str(data.get("ensemble_member_id", "")),
            status=EventClockGenerationStatus(str(data.get("status", ""))),
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
            input_anchor_sha256=_optional_text(data.get("input_anchor_sha256")),
            conditioning_support_level=str(
                data.get("conditioning_support_level", "")
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
    def from_json(cls, text: str) -> EventClockGenerationEvidenceV1:
        return cls.from_dict(
            _json_mapping(json.loads(text), "generation evidence")
        )


@dataclass(frozen=True, slots=True)
class EventClockGenerationResultV1:
    """Process-local generated rows paired with bounded evidence."""

    events: tuple[BenchmarkEventV1, ...]
    evidence: EventClockGenerationEvidenceV1


@dataclass(frozen=True, slots=True)
class EventClockCandidateLineageV1:
    """Compact pointer from one carveable event to its fitted interval."""

    event_id: str
    transformation_id: str
    schema_version: str = EVENT_CLOCK_CANDIDATE_LINEAGE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EVENT_CLOCK_CANDIDATE_LINEAGE_SCHEMA_VERSION:
            raise ValueError("unsupported event-clock candidate lineage")
        object.__setattr__(self, "event_id", _required_text(self.event_id))
        object.__setattr__(
            self, "transformation_id", _required_text(self.transformation_id)
        )

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "transformation_id": self.transformation_id,
        }


@dataclass(frozen=True, slots=True)
class EventClockCandidateBatchV1:
    """Process-local event-clock proposals for one immutable anchor interval."""

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
    event_lineage: tuple[EventClockCandidateLineageV1, ...]
    fit_id: str
    generation_evidence_id: str
    batch_id: str = ""
    schema_version: str = EVENT_CLOCK_CANDIDATE_BATCH_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EVENT_CLOCK_CANDIDATE_BATCH_SCHEMA_VERSION:
            raise ValueError("unsupported event-clock candidate batch")
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
        object.__setattr__(self, "symbol", _required_text(self.symbol).lower())
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
            raise ValueError("generated event-clock batch requires events")
        if status is not MotifGenerationStatus.GENERATED and events:
            raise ValueError(
                "empty/refused event-clock batch cannot have events"
            )
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
            raise ValueError(
                "event-clock candidate event differs from batch scope"
            )
        event_ids = {item.event_id for item in events}
        if len(event_ids) != len(events) or event_ids != {
            item.event_id for item in lineages
        }:
            raise ValueError("event-clock candidate lineage does not reconcile")
        object.__setattr__(self, "events", events)
        object.__setattr__(self, "event_lineage", lineages)
        expected = _stable_id(
            "event-clock-candidate-batch", self.identity_payload()
        )
        if self.batch_id and self.batch_id != expected:
            raise ValueError("event-clock candidate batch_id differs")
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

    def lineage_for(self, event_id: str) -> EventClockCandidateLineageV1:
        wanted = _required_text(event_id)
        for lineage in self.event_lineage:
            if lineage.event_id == wanted:
                return lineage
        raise KeyError(wanted)


@dataclass(frozen=True, slots=True)
class FittedEventClockBenchmarkGeneratorV1(BenchmarkGeneratorV1):
    """Adapter exposing one fitted classical model to the v1 benchmark."""

    candidate: BenchmarkCandidateV1
    config: EventClockConfigurationV1
    fit_result: EventClockFitResultV1
    candidate_id: str = field(init=False)
    event_schema_version: str = BENCHMARK_EVENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.candidate.kind is not BenchmarkCandidateKind.CANDIDATE:
            raise ValueError("event-clock adapter requires a candidate")
        if self.candidate.method_id != _generator_id(self.config.family):
            raise ValueError("event-clock candidate method differs")
        if self.fit_result.status is not EventClockFitStatus.FITTED:
            raise EventClockFitError(
                "event-clock adapter requires a fitted model"
            )
        if (
            self.fit_result.config_id != self.config.config_id
            or self.fit_result.family is not self.config.family
        ):
            raise ValueError("event-clock fit and config differ")
        if self.event_schema_version != BENCHMARK_EVENT_SCHEMA_VERSION:
            raise ValueError("event-clock adapter requires benchmark event v1")
        object.__setattr__(self, "candidate_id", self.candidate.candidate_id)

    def generate(
        self,
        degraded_events: Sequence[BenchmarkEventV1],
        *,
        scenario: BenchmarkScenarioV1,
        window: ReconstructionWindowV1,
        ensemble_member_id: str,
    ) -> Sequence[BenchmarkEventV1]:
        """Generate one bounded synchronized candidate stream or fail closed."""
        result = self.generate_with_evidence(
            degraded_events,
            scenario=scenario,
            window=window,
            ensemble_member_id=ensemble_member_id,
        )
        if result.evidence.status in {
            EventClockGenerationStatus.REFUSED,
            EventClockGenerationStatus.FAILED,
        }:
            raise EventClockGenerationError(
                result.evidence.failure_reason
                or "event-clock generation failed"
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
    ) -> EventClockGenerationResultV1:
        """Generate with an explicit bounded, prior-only history seam."""
        started = time.perf_counter()
        before_peak = peak_rss_bytes()
        raw_events = tuple(degraded_events)
        conditioning_level = "not_evaluated"
        input_anchor_sha256: str | None = None
        try:
            if any(
                not isinstance(item, BenchmarkEventV1) for item in raw_events
            ):
                raise EventClockGenerationError(
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
                history_events,
                config=self.config,
                window=window,
            )
            conditioning_level = _conditioning_support_level(
                self.config,
                self.fit_result,
                ordered,
                scenario,
            )
            if conditioning_level == "unsupported":
                raise EventClockGenerationError(
                    "requested epoch/session conditioning support is missing"
                )
            events = _generate_events(
                self.config,
                self.fit_result,
                ordered,
                scenario=scenario,
                window=window,
                ensemble_member_id=ensemble_member_id,
                history_events=history,
            )
            generated_count = sum(
                item.sparsity.startswith("event-clock-") for item in events
            )
            measured_peak = _incremental_peak_rss_bytes(before_peak)
            if measured_peak > self.config.limits.max_peak_memory_bytes:
                raise EventClockGenerationError(
                    "measured generation memory exceeds limit"
                )
            status = (
                EventClockGenerationStatus.GENERATED
                if generated_count
                else EventClockGenerationStatus.EMPTY
            )
            evidence = EventClockGenerationEvidenceV1(
                fit_id=self.fit_result.fit_id,
                window_id=window.window_id,
                ensemble_member_id=ensemble_member_id,
                status=status,
                attempted=True,
                generated_event_count=generated_count,
                input_event_count=len(ordered),
                history_event_count=len(history),
                input_anchor_sha256=input_anchor_sha256,
                conditioning_support_level=conditioning_level,
                wall_time_ms=round((time.perf_counter() - started) * 1000),
                peak_memory_bytes=measured_peak,
            )
            return EventClockGenerationResultV1(
                events=events, evidence=evidence
            )
        except EventClockGenerationError as err:
            evidence = EventClockGenerationEvidenceV1(
                fit_id=self.fit_result.fit_id,
                window_id=window.window_id,
                ensemble_member_id=ensemble_member_id,
                status=EventClockGenerationStatus.REFUSED,
                attempted=True,
                generated_event_count=0,
                input_event_count=len(raw_events),
                history_event_count=len(history_events),
                input_anchor_sha256=input_anchor_sha256,
                conditioning_support_level=conditioning_level,
                wall_time_ms=round((time.perf_counter() - started) * 1000),
                peak_memory_bytes=_incremental_peak_rss_bytes(before_peak),
                failure_reason=str(err),
            )
            return EventClockGenerationResultV1(events=(), evidence=evidence)
        except (ArithmeticError, KeyError, TypeError, ValueError) as err:
            evidence = EventClockGenerationEvidenceV1(
                fit_id=self.fit_result.fit_id,
                window_id=window.window_id,
                ensemble_member_id=ensemble_member_id,
                status=EventClockGenerationStatus.FAILED,
                attempted=True,
                generated_event_count=0,
                input_event_count=len(raw_events),
                history_event_count=len(history_events),
                input_anchor_sha256=input_anchor_sha256,
                conditioning_support_level=conditioning_level,
                wall_time_ms=round((time.perf_counter() - started) * 1000),
                peak_memory_bytes=_incremental_peak_rss_bytes(before_peak),
                failure_reason=f"generation_failed:{type(err).__name__}:{err}",
            )
            return EventClockGenerationResultV1(events=(), evidence=evidence)


def default_event_clock_configs() -> tuple[EventClockConfigurationV1, ...]:
    """Return the fixed registry order without selecting a preferred family."""
    return (
        NonHomogeneousPoissonConfigV1(),
        CoxProcessConfigV1(),
        AutoregressiveConditionalDurationConfigV1(),
        HiddenMarkovDurationMarkConfigV1(),
    )


def fit_event_clock_challenger(
    config: EventClockConfigurationV1,
    calibration_windows: Sequence[EventClockCalibrationWindowV1],
    *,
    information_mode: InformationMode = InformationMode.EX_POST_RECONSTRUCTION,
    as_of_ns: int | None = None,
) -> EventClockFitResultV1:
    """Fit one family on calibration-only rows with explicit refusal evidence."""
    if not isinstance(config, EventClockConfigV1):
        raise TypeError("unsupported event-clock configuration")
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
        event_count=event_count,
        estimated_memory=estimated_memory,
    )
    if reason is not None:
        return EventClockFitResultV1(
            family=config.family,
            config_id=config.config_id,
            calibration_content_sha256=calibration_hash,
            information_mode=mode,
            as_of_ns=as_of_ns,
            symbols=symbols or ("UNSUPPORTED",),
            status=EventClockFitStatus.REFUSED,
            converged=False,
            iteration_count=0,
            fitted_event_count=event_count,
            fitted_window_count=len(windows),
            log_likelihood=None,
            parameters={},
            diagnostics={},
            estimated_peak_memory_bytes=estimated_memory,
            failure_reason=reason,
        )
    try:
        parameters, likelihood, iterations, diagnostics = _fit_family(
            config, windows
        )
        conditioning_support = _conditioning_support(windows)
        parameters["conditioning_support"] = cast(
            JSONValue, conditioning_support
        )
        diagnostics["conditioning_cell_count"] = len(conditioning_support)
    except (ArithmeticError, StatisticsError, ValueError) as err:
        return EventClockFitResultV1(
            family=config.family,
            config_id=config.config_id,
            calibration_content_sha256=calibration_hash,
            information_mode=mode,
            as_of_ns=as_of_ns,
            symbols=symbols,
            status=EventClockFitStatus.FAILED,
            converged=False,
            iteration_count=0,
            fitted_event_count=event_count,
            fitted_window_count=len(windows),
            log_likelihood=None,
            parameters={},
            diagnostics={},
            estimated_peak_memory_bytes=estimated_memory,
            failure_reason=f"fit_failed:{type(err).__name__}:{err}",
        )
    return EventClockFitResultV1(
        family=config.family,
        config_id=config.config_id,
        calibration_content_sha256=calibration_hash,
        information_mode=mode,
        as_of_ns=as_of_ns,
        symbols=symbols,
        status=EventClockFitStatus.FITTED,
        converged=True,
        iteration_count=iterations,
        fitted_event_count=event_count,
        fitted_window_count=len(windows),
        log_likelihood=likelihood,
        parameters=parameters,
        diagnostics=diagnostics,
        estimated_peak_memory_bytes=estimated_memory,
    )


def build_fitted_event_clock_generator(
    config: EventClockConfigurationV1,
    fit_result: EventClockFitResultV1,
    *,
    ensemble_member_ids: Sequence[str],
) -> FittedEventClockBenchmarkGeneratorV1:
    """Bind a successful fit to one explicit benchmark candidate."""
    candidate = build_event_clock_benchmark_candidate(
        config,
        fit_result,
        ensemble_member_ids=ensemble_member_ids,
    )
    return FittedEventClockBenchmarkGeneratorV1(
        candidate=candidate,
        config=config,
        fit_result=fit_result,
    )


def build_event_clock_benchmark_candidate(
    config: EventClockConfigurationV1,
    fit_result: EventClockFitResultV1,
    *,
    ensemble_member_ids: Sequence[str],
) -> BenchmarkCandidateV1:
    """Describe a fit attempt even when it failed and has no generator."""
    if (
        fit_result.config_id != config.config_id
        or fit_result.family is not config.family
    ):
        raise ValueError("event-clock fit and config differ")
    return BenchmarkCandidateV1(
        kind=BenchmarkCandidateKind.CANDIDATE,
        method_id=_generator_id(config.family),
        implementation_version=EVENT_CLOCK_IMPLEMENTATION_VERSION,
        parameters={
            "config_id": config.config_id,
            "fit_id": fit_result.fit_id,
            "family": config.family.value,
        },
        ensemble_member_ids=tuple(ensemble_member_ids),
    )


def build_event_clock_candidate_batches(
    *,
    run: ReconstructionRunV1,
    window: ReconstructionWindowV1,
    config: EventClockConfigurationV1,
    fit_result: EventClockFitResultV1,
    generation_result: EventClockGenerationResultV1,
    observed_events: Sequence[SyntheticEventV1],
    session_state: str,
    special_tags: Sequence[str] = (),
    event_tags: Sequence[str] = (),
) -> tuple[EventClockCandidateBatchV1, ...]:
    """Project synchronized benchmark proposals into the shared carving seam."""
    if window.run_id != run.run_id:
        raise ValueError("event-clock candidate window does not belong to run")
    if window.ensemble_member_id not in run.ensemble_member_ids:
        raise ValueError("event-clock candidate member is outside run")
    if config.config_id not in run.configuration_ids:
        raise ValueError("event-clock config is absent from reconstruction run")
    if (
        fit_result.config_id != config.config_id
        or fit_result.fit_id != generation_result.evidence.fit_id
    ):
        raise ValueError("event-clock fit, config, and generation differ")
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
        raise ValueError(
            "event-clock carving projection requires observed anchors"
        )
    benchmark_proposals = tuple(
        item
        for item in generation_result.events
        if item.sparsity.startswith("event-clock-")
    )
    if len({item.source_event_id for item in benchmark_proposals}) != len(
        benchmark_proposals
    ):
        raise ValueError("event-clock proposals have duplicate source identity")
    upstream_refused = generation_result.evidence.status in {
        EventClockGenerationStatus.REFUSED,
        EventClockGenerationStatus.FAILED,
    }
    if generation_result.evidence.input_anchor_sha256 is not None:
        if (
            _synthetic_anchor_sha256(observed)
            != generation_result.evidence.input_anchor_sha256
        ):
            raise ValueError(
                "carving anchors differ from event-clock generation input"
            )
    elif not upstream_refused:
        benchmark_anchors = tuple(
            item
            for item in generation_result.events
            if not item.sparsity.startswith("event-clock-")
        )
        benchmark_signature = {
            (
                item.symbol.lower(),
                item.event_time_ns,
                item.event_sequence,
                item.bid,
                item.ask,
            )
            for item in benchmark_anchors
        }
        observed_signature = {
            (
                item.symbol,
                item.event_time_ns,
                item.event_sequence,
                item.bid,
                item.ask,
            )
            for item in observed
        }
        if benchmark_signature != observed_signature:
            raise ValueError(
                "carving anchors differ from event-clock generation input"
            )
    batches: list[EventClockCandidateBatchV1] = []
    assigned_proposal_ids: set[str] = set()
    by_symbol: dict[str, list[SyntheticEventV1]] = {}
    for event in observed:
        by_symbol.setdefault(event.symbol, []).append(event)
    for symbol in sorted(by_symbol):
        anchors = by_symbol[symbol]
        if len(anchors) < 2:
            raise ValueError(
                "each carving symbol requires two observed anchors"
            )
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
                "event-clock-interval-transformation",
                {
                    "fit_id": fit_result.fit_id,
                    "generation_evidence_id": generation_result.evidence.evidence_id,
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
                    generator_id=_generator_id(config.family),
                    generator_version=EVENT_CLOCK_IMPLEMENTATION_VERSION,
                    generator_config_id=config.config_id,
                    reference_id=item.source_event_id,
                    motif_id=_generator_id(config.family),
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
            batches.append(
                EventClockCandidateBatchV1(
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
                        tuple(
                            EventClockCandidateLineageV1(
                                event_id=item.event_id,
                                transformation_id=transformation_id,
                            )
                            for item in events
                        )
                        if not upstream_refused
                        else ()
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
        raise ValueError("event-clock proposal lies outside observed anchors")
    return tuple(batches)


class StatisticsError(ValueError):
    """Internal bounded fitting failure."""


def _fit_family(
    config: EventClockConfigurationV1,
    windows: Sequence[EventClockCalibrationWindowV1],
) -> tuple[dict[str, JSONValue], float, int, dict[str, JSONScalar]]:
    by_symbol = _events_by_symbol(windows)
    profiles = {
        symbol: _quote_profile_sequences(_symbol_sequences(windows, symbol))
        for symbol in by_symbol
    }
    if config.family is EventClockFamily.NHPP:
        typed = cast(NonHomogeneousPoissonConfigV1, config)
        models = {
            symbol: _fit_nhpp_symbol(typed, windows, symbol)
            for symbol in sorted(by_symbol)
        }
        likelihood = sum(
            _finite_float(model["log_likelihood"], "log_likelihood")
            for model in models.values()
        )
        parameters: dict[str, JSONValue] = {
            "model": "piecewise_constant_nhpp",
            "symbols": {
                symbol: {**model, "quote_profile": profiles[symbol]}
                for symbol, model in models.items()
            },
        }
        return (
            parameters,
            likelihood,
            1,
            {
                "conditional_intensity_available": True,
                "intensity_bin_count": typed.intensity_bin_count,
                "symbol_count": len(models),
            },
        )
    if config.family is EventClockFamily.COX:
        typed_cox = cast(CoxProcessConfigV1, config)
        models = {
            symbol: _fit_cox_symbol(typed_cox, windows, symbol)
            for symbol in sorted(by_symbol)
        }
        likelihood = sum(
            _finite_float(model["log_likelihood"], "log_likelihood")
            for model in models.values()
        )
        parameters = {
            "model": "gamma_mixed_cox",
            "symbols": {
                symbol: {**model, "quote_profile": profiles[symbol]}
                for symbol, model in models.items()
            },
        }
        return (
            parameters,
            likelihood,
            1,
            {
                "conditional_intensity_available": True,
                "marginal_count_likelihood_available": True,
                "symbol_count": len(models),
            },
        )
    if config.family is EventClockFamily.ACD:
        typed_acd = cast(AutoregressiveConditionalDurationConfigV1, config)
        models = {
            symbol: _fit_acd_symbol(typed_acd, windows, symbol)
            for symbol in sorted(by_symbol)
        }
        likelihood = sum(
            _finite_float(model["log_likelihood"], "log_likelihood")
            for model in models.values()
        )
        iterations = max(
            _strict_int(
                model["coefficient_candidate_count"],
                "coefficient_candidate_count",
            )
            for model in models.values()
        )
        parameters = {
            "model": "exponential_acd_1_1",
            "symbols": {
                symbol: {**model, "quote_profile": profiles[symbol]}
                for symbol, model in models.items()
            },
        }
        return (
            parameters,
            likelihood,
            iterations,
            {
                "conditional_duration_likelihood_available": True,
                "stationarity_enforced": True,
                "symbol_count": len(models),
            },
        )
    typed_hmm = cast(HiddenMarkovDurationMarkConfigV1, config)
    models = {
        symbol: _fit_hidden_markov_symbol(typed_hmm, windows, symbol)
        for symbol in sorted(by_symbol)
    }
    likelihood = sum(
        _finite_float(model["log_likelihood"], "log_likelihood")
        for model in models.values()
    )
    iterations = max(
        _strict_int(model["iteration_count"], "iteration_count")
        for model in models.values()
    )
    parameters = {
        "model": "two_state_log_duration_categorical_mark_hard_em",
        "symbols": {
            symbol: {**model, "quote_profile": profiles[symbol]}
            for symbol, model in models.items()
        },
    }
    return (
        parameters,
        likelihood,
        iterations,
        {
            "hidden_state_count": 2,
            "explicit_duration_distribution": "lognormal_by_state",
            "mark_distribution": "categorical_by_state",
            "symbol_count": len(models),
        },
    )


def _fit_nhpp_symbol(
    config: NonHomogeneousPoissonConfigV1,
    windows: Sequence[EventClockCalibrationWindowV1],
    symbol: str,
) -> dict[str, JSONValue]:
    counts = [0] * config.intensity_bin_count
    exposure = [0.0] * config.intensity_bin_count
    for window in windows:
        for event in window.events:
            if event.symbol == symbol:
                counts[
                    _time_bin(event.event_time_ns, config.intensity_bin_count)
                ] += 1
        _accumulate_bin_exposure(
            exposure, window.start_ns, window.end_ns, config.intensity_bin_count
        )
    total_exposure = sum(exposure)
    total_count = sum(counts)
    if total_exposure <= 0.0 or total_count <= 0:
        raise StatisticsError(f"NHPP support missing for {symbol}")
    global_rate = total_count / total_exposure
    rates = [
        (
            (count + config.smoothing_count)
            / (seconds + config.smoothing_count / global_rate)
            if seconds > 0.0
            else global_rate
        )
        for count, seconds in zip(counts, exposure)
    ]
    likelihood = sum(
        count * math.log(max(rate, 1e-300)) - rate * seconds
        for count, rate, seconds in zip(counts, rates, exposure)
    )
    return {
        "rates_per_second": cast(JSONValue, rates),
        "bin_exposure_seconds": cast(JSONValue, exposure),
        "bin_event_counts": cast(JSONValue, counts),
        "global_rate_per_second": global_rate,
        "log_likelihood": likelihood,
    }


def _fit_cox_symbol(
    config: CoxProcessConfigV1,
    windows: Sequence[EventClockCalibrationWindowV1],
    symbol: str,
) -> dict[str, JSONValue]:
    rates: list[float] = []
    counts: list[int] = []
    durations: list[float] = []
    for window in windows:
        count = sum(event.symbol == symbol for event in window.events)
        duration = (window.end_ns - window.start_ns) / NANOSECONDS_PER_SECOND
        counts.append(count)
        durations.append(duration)
        rates.append(count / duration)
    mean_rate = statistics.fmean(rates)
    if mean_rate <= 0.0:
        raise StatisticsError(f"Cox support missing for {symbol}")
    variance = statistics.variance(rates) if len(rates) > 1 else 0.0
    if variance <= config.dispersion_floor:
        shape = config.maximum_gamma_shape
    else:
        shape = min(
            config.maximum_gamma_shape,
            max(config.minimum_gamma_shape, mean_rate**2 / variance),
        )
    scale = mean_rate / shape
    likelihood = 0.0
    for count, duration in zip(counts, durations):
        mean_count = mean_rate * duration
        likelihood += _negative_binomial_log_pmf(count, shape, mean_count)
    return {
        "mean_rate_per_second": mean_rate,
        "rate_variance": variance,
        "gamma_shape": shape,
        "gamma_scale": scale,
        "window_event_counts": cast(JSONValue, counts),
        "window_exposure_seconds": cast(JSONValue, durations),
        "log_likelihood": likelihood,
    }


def _fit_acd_symbol(
    config: AutoregressiveConditionalDurationConfigV1,
    windows: Sequence[EventClockCalibrationWindowV1],
    symbol: str,
) -> dict[str, JSONValue]:
    duration_sequences = tuple(
        tuple(_durations_seconds(events))
        for events in _symbol_sequences(windows, symbol)
        if len(events) >= 2
    )
    durations = [
        duration for values in duration_sequences for duration in values
    ]
    if len(durations) < config.minimum_events_per_symbol - 1:
        raise StatisticsError("ACD duration support is insufficient")
    mean_duration = statistics.fmean(durations)
    best: tuple[float, float, float, float] | None = None
    evaluated = 0
    grid = config.coefficient_grid_size
    upper = 1.0 - config.stationarity_margin
    for alpha_index in range(grid):
        alpha = upper * alpha_index / grid
        for beta_index in range(grid):
            beta = upper * beta_index / grid
            if alpha + beta >= upper:
                continue
            evaluated += 1
            omega = max(
                config.minimum_conditional_duration_seconds,
                mean_duration * (1.0 - alpha - beta),
            )
            likelihood = sum(
                _acd_log_likelihood(
                    values,
                    omega=omega,
                    alpha=alpha,
                    beta=beta,
                    floor=config.minimum_conditional_duration_seconds,
                )
                for values in duration_sequences
            )
            if best is None or likelihood > best[0]:
                best = (likelihood, omega, alpha, beta)
    if best is None:
        raise StatisticsError("ACD stationary grid contains no candidate")
    likelihood, omega, alpha, beta = best
    return {
        "omega_seconds": omega,
        "alpha": alpha,
        "beta": beta,
        "unconditional_mean_seconds": omega / (1.0 - alpha - beta),
        "last_duration_seconds": duration_sequences[-1][-1],
        "last_conditional_duration_seconds": _last_acd_psi(
            duration_sequences[-1], omega, alpha, beta
        ),
        "calibration_sequence_count": len(duration_sequences),
        "recursion_reset_at_window_boundary": True,
        "coefficient_candidate_count": evaluated,
        "log_likelihood": likelihood,
    }


def _fit_hidden_markov_symbol(
    config: HiddenMarkovDurationMarkConfigV1,
    windows: Sequence[EventClockCalibrationWindowV1],
    symbol: str,
) -> dict[str, JSONValue]:
    sequences = _symbol_sequences(windows, symbol)
    duration_sequences = tuple(
        tuple(_durations_seconds(events))
        for events in sequences
        if len(events) >= 2
    )
    mark_sequences = tuple(
        tuple(_transition_marks(events))
        for events in sequences
        if len(events) >= 2
    )
    durations = [
        duration for values in duration_sequences for duration in values
    ]
    marks = [mark for values in mark_sequences for mark in values]
    if len(durations) < config.minimum_events_per_symbol - 1:
        raise StatisticsError("hidden Markov duration support is insufficient")
    logs = [math.log(max(value, 1e-12)) for value in durations]
    centers = [min(logs), max(logs)]
    states = [0] * len(logs)
    converged = False
    iterations = 0
    for iterations in range(1, config.limits.max_iterations + 1):
        new_states = [
            min(range(2), key=lambda state: abs(value - centers[state]))
            for value in logs
        ]
        if set(new_states) != {0, 1}:
            midpoint = statistics.median(logs)
            new_states = [int(value > midpoint) for value in logs]
        new_centers = [
            statistics.fmean(
                value
                for value, state_value in zip(logs, new_states)
                if state_value == state
            )
            for state in range(2)
        ]
        delta = max(
            abs(left - right) for left, right in zip(centers, new_centers)
        )
        states = new_states
        centers = new_centers
        if delta <= config.convergence_tolerance:
            converged = True
            break
    if not converged:
        raise StatisticsError("hidden Markov hard-EM did not converge")
    state_sequences: list[tuple[int, ...]] = []
    offset = 0
    for values in duration_sequences:
        state_sequences.append(tuple(states[offset : offset + len(values)]))
        offset += len(values)
    if offset != len(states):
        raise StatisticsError("hidden Markov state sequence does not reconcile")
    variances = []
    for state in range(2):
        selected = [
            value
            for value, state_value in zip(logs, states)
            if state_value == state
        ]
        variance = statistics.variance(selected) if len(selected) > 1 else 0.0
        variances.append(max(config.variance_floor, variance))
    transition_counts = [[config.probability_smoothing] * 2 for _ in range(2)]
    for sequence_states in state_sequences:
        for left, right in pairwise(sequence_states):
            transition_counts[left][right] += 1.0
    transitions = [
        [value / sum(row) for value in row] for row in transition_counts
    ]
    mark_names = sorted(
        set(marks) | {"ask_only", "bid_only", "joint", "unchanged"}
    )
    mark_counts = [
        dict.fromkeys(mark_names, config.probability_smoothing)
        for _ in range(2)
    ]
    for state, mark in zip(states, marks):
        mark_counts[state][mark] += 1.0
    mark_probabilities = [
        {name: value / sum(row.values()) for name, value in sorted(row.items())}
        for row in mark_counts
    ]
    initial_counts = [
        config.probability_smoothing,
        config.probability_smoothing,
    ]
    for sequence_states in state_sequences:
        initial_counts[sequence_states[0]] += 1.0
    initial = [value / sum(initial_counts) for value in initial_counts]
    likelihood = sum(
        _hidden_path_log_likelihood(
            [math.log(max(value, 1e-12)) for value in duration_values],
            mark_values,
            sequence_states,
            centers,
            variances,
            transitions,
            mark_probabilities,
            initial,
        )
        for duration_values, mark_values, sequence_states in zip(
            duration_sequences,
            mark_sequences,
            state_sequences,
        )
    )
    return {
        "log_duration_means": cast(JSONValue, centers),
        "log_duration_variances": cast(JSONValue, variances),
        "transition_matrix": cast(JSONValue, transitions),
        "initial_probabilities": cast(JSONValue, initial),
        "mark_probabilities": cast(JSONValue, mark_probabilities),
        "last_state": state_sequences[-1][-1],
        "calibration_sequence_count": len(state_sequences),
        "transition_reset_at_window_boundary": True,
        "iteration_count": iterations,
        "log_likelihood": likelihood,
    }


def _generate_events(
    config: EventClockConfigurationV1,
    fit: EventClockFitResultV1,
    degraded_events: Sequence[BenchmarkEventV1],
    *,
    scenario: BenchmarkScenarioV1,
    window: ReconstructionWindowV1,
    ensemble_member_id: str,
    history_events: Sequence[BenchmarkEventV1],
) -> tuple[BenchmarkEventV1, ...]:
    if window.ensemble_member_id != ensemble_member_id:
        raise EventClockGenerationError("ensemble member differs from window")
    window_symbols = tuple(sorted(item.upper() for item in window.symbols))
    if window_symbols != tuple(sorted(fit.symbols)):
        raise EventClockGenerationError(
            "fit symbols differ from synchronized window"
        )
    if not degraded_events:
        raise EventClockGenerationError(
            "degraded window has no immutable anchors"
        )
    if any(
        item.symbol.upper() not in window_symbols for item in degraded_events
    ):
        raise EventClockGenerationError(
            "degraded event symbol is outside window"
        )
    estimated = (
        config.limits.max_generated_events_per_window
        * config.limits.estimated_bytes_per_generated_event
    )
    if estimated > config.limits.max_peak_memory_bytes:
        raise EventClockGenerationError(
            "generation memory estimate exceeds limit"
        )
    seed = _semantic_seed(
        config.base_seed,
        fit.fit_id,
        scenario.scenario_id,
        window.window_id,
        ensemble_member_id,
        _benchmark_content_sha256(history_events),
    )
    rng = random.Random(seed)
    anchors = tuple(
        replace(
            item,
            ensemble_member_id=ensemble_member_id,
            benchmark_event_id="",
        )
        for item in degraded_events
        if window.reads_event_time(item.event_time_ns)
    )
    by_symbol: dict[str, list[BenchmarkEventV1]] = {
        symbol: [] for symbol in fit.symbols
    }
    for event in anchors:
        by_symbol[event.symbol.upper()].append(event)
    if any(len(values) < 2 for values in by_symbol.values()):
        raise EventClockGenerationError(
            "every symbol requires two immutable anchors"
        )
    parameter_symbols = cast(
        Mapping[str, Any], fit.parameters.get("symbols", {})
    )
    generated: list[BenchmarkEventV1] = []
    for symbol in fit.symbols:
        model = cast(Mapping[str, Any], parameter_symbols.get(symbol, {}))
        if not model:
            raise EventClockGenerationError(f"fit parameters omit {symbol}")
        ordered = sorted(
            by_symbol[symbol],
            key=lambda item: (
                item.event_time_ns,
                item.event_sequence,
                item.benchmark_event_id,
            ),
        )
        family_state: dict[str, Any] = {}
        for interval_index, (left, right) in enumerate(pairwise(ordered)):
            if right.event_time_ns <= window.core_start_ns:
                continue
            if left.event_time_ns >= window.core_end_ns:
                break
            times = _proposal_times(
                config,
                model,
                left.event_time_ns,
                right.event_time_ns,
                rng,
                family_state,
            )
            if len(times) > config.limits.max_generated_events_per_interval:
                raise EventClockGenerationError(
                    "generated interval cardinality exceeds limit"
                )
            profile = cast(Mapping[str, Any], model.get("quote_profile", {}))
            hidden_states = cast(
                Sequence[int], family_state.pop("proposal_hidden_states", ())
            )
            if config.family is EventClockFamily.HIDDEN_MARKOV and len(
                hidden_states
            ) != len(times):
                raise EventClockGenerationError(
                    "hidden-state mark lineage differs from proposal times"
                )
            for proposal_index, event_time_ns in enumerate(times):
                ordinal = proposal_index + 1
                if not window.owns_event_time(event_time_ns):
                    continue
                progress = (event_time_ns - left.event_time_ns) / (
                    right.event_time_ns - left.event_time_ns
                )
                bid = left.bid + progress * (right.bid - left.bid)
                ask = left.ask + progress * (right.ask - left.ask)
                if config.family is EventClockFamily.HIDDEN_MARKOV:
                    state_mark_probabilities = cast(
                        Sequence[Mapping[str, float]],
                        model.get("mark_probabilities", ()),
                    )
                    try:
                        mark_probabilities = state_mark_probabilities[
                            hidden_states[proposal_index]
                        ]
                    except (IndexError, TypeError) as err:
                        raise EventClockGenerationError(
                            "hidden-state mark parameters are incomplete"
                        ) from err
                else:
                    mark_probabilities = cast(
                        Mapping[str, float],
                        profile.get("mark_probabilities", {}),
                    )
                mark = _sample_categorical(mark_probabilities, rng)
                source_id = _stable_id(
                    "event-clock-generated-event",
                    {
                        "fit_id": fit.fit_id,
                        "scenario_id": scenario.scenario_id,
                        "window_id": window.window_id,
                        "ensemble_member_id": ensemble_member_id,
                        "symbol": symbol,
                        "interval_index": interval_index,
                        "event_time_ns": event_time_ns,
                        "ordinal": ordinal,
                    },
                )
                generated.append(
                    BenchmarkEventV1(
                        source_event_id=source_id,
                        symbol=symbol,
                        event_time_ns=event_time_ns,
                        event_sequence=ordinal,
                        bid=max(1e-12, bid),
                        ask=max(max(1e-12, bid), ask),
                        epoch_id=scenario.epoch_id,
                        session=left.session,
                        event_state=mark,
                        sparsity=f"event-clock-{config.family.value}",
                        ensemble_member_id=ensemble_member_id,
                        support_lower_mid=min(left.mid, right.mid),
                        support_upper_mid=max(left.mid, right.mid),
                    )
                )
                if (
                    len(generated)
                    > config.limits.max_generated_events_per_window
                ):
                    raise EventClockGenerationError(
                        "generated window cardinality exceeds limit"
                    )
    return tuple(
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


def _proposal_times(
    config: EventClockConfigurationV1,
    model: Mapping[str, Any],
    left_ns: int,
    right_ns: int,
    rng: random.Random,
    state: dict[str, Any],
) -> tuple[int, ...]:
    gap_seconds = (right_ns - left_ns) / NANOSECONDS_PER_SECOND
    if gap_seconds <= 0.0:
        return ()
    maximum = config.limits.max_generated_events_per_interval
    if config.family is EventClockFamily.NHPP:
        rates = cast(Sequence[float], model["rates_per_second"])
        midpoint = left_ns + (right_ns - left_ns) // 2
        rate = float(rates[_time_bin(midpoint, len(rates))])
        count = max(
            0,
            _poisson(
                rate * gap_seconds,
                rng,
                max_iterations=config.limits.max_iterations,
            )
            - 1,
        )
        if count > maximum:
            raise EventClockGenerationError(
                "generated interval cardinality exceeds limit"
            )
        return _uniform_times(left_ns, right_ns, count, rng)
    if config.family is EventClockFamily.COX:
        shape = float(model["gamma_shape"])
        scale = float(model["gamma_scale"])
        rate = rng.gammavariate(shape, scale)
        count = max(
            0,
            _poisson(
                rate * gap_seconds,
                rng,
                max_iterations=config.limits.max_iterations,
            )
            - 1,
        )
        if count > maximum:
            raise EventClockGenerationError(
                "generated interval cardinality exceeds limit"
            )
        return _uniform_times(left_ns, right_ns, count, rng)
    if config.family is EventClockFamily.ACD:
        omega = float(model["omega_seconds"])
        alpha = float(model["alpha"])
        beta = float(model["beta"])
        previous_duration = float(
            state.get("previous_duration", model["unconditional_mean_seconds"])
        )
        psi = float(state.get("psi", model["unconditional_mean_seconds"]))
        elapsed = 0.0
        values: list[int] = []
        while True:
            psi = max(1e-12, omega + alpha * previous_duration + beta * psi)
            duration = rng.expovariate(1.0 / psi)
            elapsed += duration
            if elapsed >= gap_seconds:
                break
            if len(values) >= maximum:
                raise EventClockGenerationError(
                    "generated interval cardinality exceeds limit"
                )
            proposal_time = left_ns + max(
                1, round(elapsed * NANOSECONDS_PER_SECOND)
            )
            if (
                values and proposal_time <= values[-1]
            ) or proposal_time >= right_ns:
                raise EventClockGenerationError(
                    "generated times exceed nanosecond timestamp support"
                )
            values.append(proposal_time)
            previous_duration = duration
        state["previous_duration"] = previous_duration
        state["psi"] = psi
        return tuple(values)
    transitions = cast(Sequence[Sequence[float]], model["transition_matrix"])
    means = cast(Sequence[float], model["log_duration_means"])
    variances = cast(Sequence[float], model["log_duration_variances"])
    if "hidden_state" in state:
        current_state = int(state["hidden_state"])
    else:
        current_state = _sample_index(
            cast(Sequence[float], model["initial_probabilities"]), rng
        )
    elapsed = 0.0
    values = []
    proposal_states: list[int] = []
    while True:
        duration = rng.lognormvariate(
            float(means[current_state]),
            math.sqrt(float(variances[current_state])),
        )
        elapsed += duration
        if elapsed >= gap_seconds:
            break
        if len(values) >= maximum:
            raise EventClockGenerationError(
                "generated interval cardinality exceeds limit"
            )
        proposal_time = left_ns + max(
            1, round(elapsed * NANOSECONDS_PER_SECOND)
        )
        if (
            values and proposal_time <= values[-1]
        ) or proposal_time >= right_ns:
            raise EventClockGenerationError(
                "generated times exceed nanosecond timestamp support"
            )
        values.append(proposal_time)
        proposal_states.append(current_state)
        current_state = _sample_index(transitions[current_state], rng)
    state["hidden_state"] = current_state
    state["proposal_hidden_states"] = tuple(proposal_states)
    return tuple(values)


def _fit_refusal_reason(
    config: EventClockConfigV1,
    windows: Sequence[EventClockCalibrationWindowV1],
    *,
    mode: InformationMode,
    as_of_ns: int | None,
    event_count: int,
    estimated_memory: int,
) -> str | None:
    if not windows:
        return "missing_calibration_windows"
    if len(windows) > config.limits.max_fit_windows:
        return "fit_window_limit"
    if event_count > config.limits.max_fit_events:
        return "fit_event_limit"
    if estimated_memory > config.limits.max_peak_memory_bytes:
        return "fit_memory_limit"
    if any(item.split_kind != "calibration" for item in windows):
        return "non_calibration_input"
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
    if len(counts) != len({event.symbol for event in windows[0].events}):
        return "incomplete_synchronized_symbol_support"
    return None


def _validated_history(
    events: Sequence[BenchmarkEventV1],
    *,
    config: EventClockConfigurationV1,
    window: ReconstructionWindowV1,
) -> tuple[BenchmarkEventV1, ...]:
    """Validate a bounded, strictly prior synchronized history surface."""
    values = tuple(events)
    if len(values) > config.limits.max_history_events:
        raise EventClockGenerationError("history cardinality exceeds limit")
    if any(not isinstance(item, BenchmarkEventV1) for item in values):
        raise EventClockGenerationError(
            "history contains a non-benchmark event"
        )
    lower_bound = window.core_start_ns - config.limits.max_history_ns
    symbols = {item.upper() for item in window.symbols}
    if any(
        item.event_time_ns >= window.core_start_ns
        or item.event_time_ns < lower_bound
        or item.symbol.upper() not in symbols
        for item in values
    ):
        raise EventClockGenerationError(
            "history must be prior-only, bounded, and synchronized to symbols"
        )
    estimated = len(values) * config.limits.estimated_bytes_per_fit_event
    if estimated > config.limits.max_peak_memory_bytes:
        raise EventClockGenerationError("history memory estimate exceeds limit")
    return tuple(
        sorted(
            values,
            key=lambda item: (
                item.event_time_ns,
                item.symbol,
                item.event_sequence,
                item.benchmark_event_id,
            ),
        )
    )


def _events_by_symbol(
    windows: Sequence[EventClockCalibrationWindowV1],
) -> dict[str, tuple[BenchmarkEventV1, ...]]:
    values: dict[str, list[BenchmarkEventV1]] = {}
    for window in windows:
        for event in window.events:
            values.setdefault(event.symbol, []).append(event)
    return {
        symbol: tuple(
            sorted(
                events,
                key=lambda item: (
                    item.event_time_ns,
                    item.event_sequence,
                    item.benchmark_event_id,
                ),
            )
        )
        for symbol, events in sorted(values.items())
    }


def _conditioning_key(symbol: str, epoch_id: str, session: str) -> str:
    return "exact|" + "|".join(
        (
            _symbol(symbol),
            _required_text(epoch_id),
            _required_text(session).lower(),
        )
    )


def _session_conditioning_key(symbol: str, session: str) -> str:
    return "session|" + "|".join(
        (_symbol(symbol), _required_text(session).lower())
    )


def _conditioning_support(
    windows: Sequence[EventClockCalibrationWindowV1],
) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for window in windows:
        for event in window.events:
            counts[
                _conditioning_key(event.symbol, event.epoch_id, event.session)
            ] += 1
            counts[_session_conditioning_key(event.symbol, event.session)] += 1
    return dict(sorted(counts.items()))


def _conditioning_support_level(
    config: EventClockConfigurationV1,
    fit: EventClockFitResultV1,
    events: Sequence[BenchmarkEventV1],
    scenario: BenchmarkScenarioV1,
) -> str:
    support = cast(
        Mapping[str, Any], fit.parameters.get("conditioning_support", {})
    )
    exact = {
        _conditioning_key(item.symbol, scenario.epoch_id, item.session)
        for item in events
    }
    if all(
        int(support.get(key, 0)) >= config.minimum_conditioning_events
        for key in exact
    ):
        return "exact_epoch_session"
    session = {
        _session_conditioning_key(item.symbol, item.session) for item in events
    }
    if all(
        int(support.get(key, 0)) >= config.minimum_conditioning_events
        for key in session
    ):
        return "session_backoff"
    return "unsupported"


def _symbol_sequences(
    windows: Sequence[EventClockCalibrationWindowV1], symbol: str
) -> tuple[tuple[BenchmarkEventV1, ...], ...]:
    return tuple(
        tuple(event for event in window.events if event.symbol == symbol)
        for window in windows
        if any(event.symbol == symbol for event in window.events)
    )


def _quote_profile_sequences(
    sequences: Sequence[Sequence[BenchmarkEventV1]],
) -> dict[str, JSONValue]:
    spreads = [event.spread for events in sequences for event in events]
    marks = [mark for events in sequences for mark in _transition_marks(events)]
    counts = Counter(marks)
    names = ("ask_only", "bid_only", "joint", "unchanged")
    total = sum(counts.values()) + len(names)
    probabilities = {name: (counts[name] + 1) / total for name in names}
    return {
        "mean_spread": statistics.fmean(spreads),
        "mark_probabilities": cast(JSONValue, probabilities),
    }


def _transition_marks(events: Sequence[BenchmarkEventV1]) -> list[str]:
    values: list[str] = []
    for left, right in pairwise(events):
        bid_changed = right.bid != left.bid
        ask_changed = right.ask != left.ask
        if bid_changed and ask_changed:
            values.append("joint")
        elif bid_changed:
            values.append("bid_only")
        elif ask_changed:
            values.append("ask_only")
        else:
            values.append("unchanged")
    return values


def _durations_seconds(events: Sequence[BenchmarkEventV1]) -> list[float]:
    return [
        (right.event_time_ns - left.event_time_ns) / NANOSECONDS_PER_SECOND
        for left, right in pairwise(events)
        if right.event_time_ns > left.event_time_ns
    ]


def _acd_log_likelihood(
    durations: Sequence[float],
    *,
    omega: float,
    alpha: float,
    beta: float,
    floor: float,
) -> float:
    psi = max(floor, statistics.fmean(durations))
    likelihood = 0.0
    previous = psi
    for duration in durations:
        psi = max(floor, omega + alpha * previous + beta * psi)
        likelihood -= math.log(psi) + duration / psi
        previous = duration
    return likelihood


def _last_acd_psi(
    durations: Sequence[float], omega: float, alpha: float, beta: float
) -> float:
    psi = statistics.fmean(durations)
    previous = psi
    for duration in durations:
        psi = omega + alpha * previous + beta * psi
        previous = duration
    return psi


def _hidden_path_log_likelihood(
    logs: Sequence[float],
    marks: Sequence[str],
    states: Sequence[int],
    means: Sequence[float],
    variances: Sequence[float],
    transitions: Sequence[Sequence[float]],
    mark_probabilities: Sequence[Mapping[str, float]],
    initial: Sequence[float],
) -> float:
    result = math.log(max(initial[states[0]], 1e-300))
    for index, (value, mark, state) in enumerate(zip(logs, marks, states)):
        variance = variances[state]
        result += -0.5 * (
            math.log(2.0 * math.pi * variance)
            + (value - means[state]) ** 2 / variance
        )
        result -= value
        result += math.log(max(mark_probabilities[state][mark], 1e-300))
        if index:
            result += math.log(
                max(transitions[states[index - 1]][state], 1e-300)
            )
    return result


def _negative_binomial_log_pmf(count: int, shape: float, mean: float) -> float:
    probability = shape / (shape + mean)
    return (
        math.lgamma(count + shape)
        - math.lgamma(shape)
        - math.lgamma(count + 1)
        + shape * math.log(probability)
        + count * math.log(max(1e-300, 1.0 - probability))
    )


def _poisson(mean: float, rng: random.Random, *, max_iterations: int) -> int:
    if mean <= 0.0:
        return 0
    if mean < 30.0:
        threshold = math.exp(-mean)
        product = 1.0
        for count in range(1, max_iterations + 1):
            product *= rng.random()
            if product <= threshold:
                return count - 1
        raise EventClockGenerationError(
            "Poisson inversion exceeded iteration limit"
        )
    square_root = math.sqrt(mean)
    log_mean = math.log(mean)
    b = 0.931 + 2.53 * square_root
    a = -0.059 + 0.02483 * b
    inverse_alpha = 1.1239 + 1.1328 / (b - 3.4)
    squeeze = 0.9277 - 3.6224 / (b - 2.0)
    for _ in range(max_iterations):
        centered_uniform = rng.random() - 0.5
        uniform = rng.random()
        distance = 0.5 - abs(centered_uniform)
        if distance <= 0.0:
            continue
        candidate = math.floor(
            (2.0 * a / distance + b) * centered_uniform + mean + 0.43
        )
        if candidate >= 0 and distance >= 0.07 and uniform <= squeeze:
            return candidate
        if candidate < 0 or (distance < 0.013 and uniform > distance):
            continue
        if uniform == 0.0:
            return candidate
        acceptance = -mean + candidate * log_mean - math.lgamma(candidate + 1.0)
        proposal = (
            math.log(uniform)
            + math.log(inverse_alpha)
            - math.log(a / (distance * distance) + b)
        )
        if proposal <= acceptance:
            return candidate
    raise EventClockGenerationError(
        "Poisson transformed rejection exceeded iteration limit"
    )


def _uniform_times(
    left_ns: int, right_ns: int, count: int, rng: random.Random
) -> tuple[int, ...]:
    if count <= 0 or right_ns - left_ns <= 1:
        return ()
    interior_count = right_ns - left_ns - 1
    if count > interior_count:
        raise EventClockGenerationError(
            "generated cardinality exceeds timestamp support"
        )
    return tuple(
        left_ns + offset
        for offset in sorted(rng.sample(range(1, right_ns - left_ns), count))
    )


def _sample_categorical(
    probabilities: Mapping[str, float], rng: random.Random
) -> str:
    if not probabilities:
        return "joint"
    threshold = rng.random()
    cumulative = 0.0
    for name, probability in sorted(probabilities.items()):
        cumulative += float(probability)
        if threshold <= cumulative:
            return name
    return max(probabilities)


def _sample_index(probabilities: Sequence[float], rng: random.Random) -> int:
    threshold = rng.random()
    cumulative = 0.0
    for index, probability in enumerate(probabilities):
        cumulative += float(probability)
        if threshold <= cumulative:
            return index
    return len(probabilities) - 1


def _time_bin(event_time_ns: int, bin_count: int) -> int:
    seconds = (event_time_ns / NANOSECONDS_PER_SECOND) % SECONDS_PER_DAY
    return min(bin_count - 1, math.floor(seconds / SECONDS_PER_DAY * bin_count))


def _accumulate_bin_exposure(
    exposure: list[float], start_ns: int, end_ns: int, bin_count: int
) -> None:
    start_seconds = start_ns / NANOSECONDS_PER_SECOND
    end_seconds = end_ns / NANOSECONDS_PER_SECOND
    cursor = start_seconds
    bin_width = SECONDS_PER_DAY / bin_count
    while cursor < end_seconds:
        day_position = cursor % SECONDS_PER_DAY
        index = min(bin_count - 1, math.floor(day_position / bin_width))
        next_boundary = cursor + (index + 1) * bin_width - day_position
        stop = min(end_seconds, next_boundary)
        exposure[index] += stop - cursor
        cursor = stop


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
    values: Iterable[tuple[str, int, int, float, float]],
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


def _incremental_peak_rss_bytes(before_peak: int) -> int:
    """Return process high-water growth attributable to this operation.

    ``ru_maxrss`` and Windows ``PeakWorkingSetSize`` are lifetime high-water
    marks.  Comparing their absolute value to an operation budget makes a
    long-lived worker inherit unrelated earlier allocations.  The operation
    is admitted by the deterministic memory estimate and records/enforces
    only high-water growth above its entry baseline.
    """
    return int(max(0, peak_rss_bytes() - before_peak))


def _semantic_seed(base_seed: int, *values: str) -> int:
    payload = canonical_contract_json(
        {"base_seed": base_seed, "values": list(values)}
    )
    return int.from_bytes(hashlib.sha256(payload.encode()).digest()[:8], "big")


def _generator_id(family: EventClockFamily) -> str:
    return {
        EventClockFamily.NHPP: NHPP_GENERATOR_ID,
        EventClockFamily.COX: COX_GENERATOR_ID,
        EventClockFamily.ACD: ACD_GENERATOR_ID,
        EventClockFamily.HIDDEN_MARKOV: HIDDEN_MARKOV_GENERATOR_ID,
    }[family]


def _validate_common_config(config: EventClockConfigurationV1) -> None:
    _strict_int(config.base_seed, "base_seed")
    _bounded_int(
        config.minimum_events_per_symbol,
        "minimum_events_per_symbol",
        2,
        MAX_EVENT_CLOCK_FIT_EVENTS,
    )
    _bounded_int(
        config.minimum_conditioning_events,
        "minimum_conditioning_events",
        1,
        MAX_EVENT_CLOCK_FIT_EVENTS,
    )
    if not isinstance(config.limits, EventClockResourceLimitsV1):
        raise TypeError("event-clock config requires v1 resource limits")


def _set_config_id(config: EventClockConfigurationV1, prefix: str) -> None:
    expected = _stable_id(prefix, _config_identity(config))
    supplied = _optional_text(config.config_id) if config.config_id else None
    if supplied is not None and supplied != expected:
        raise ValueError("event-clock config_id differs")
    object.__setattr__(config, "config_id", expected)


def _config_identity(config: EventClockConfigurationV1) -> dict[str, JSONValue]:
    payload = config.to_dict()
    return {key: value for key, value in payload.items() if key != "config_id"}


def _config_dict(
    config: EventClockConfigurationV1,
    family_parameters: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    return {
        "schema_version": config.schema_version,
        "family": config.family.value,
        "base_seed": config.base_seed,
        "minimum_events_per_symbol": config.minimum_events_per_symbol,
        "minimum_conditioning_events": config.minimum_conditioning_events,
        "limits": config.limits.to_dict(),
        **dict(family_parameters),
        "config_id": config.config_id,
    }


def event_clock_config_from_dict(
    data: Mapping[str, Any],
) -> EventClockConfigurationV1:
    """Restore one exact family configuration and verify its content ID."""
    schema = str(data.get("schema_version", ""))
    base_seed = _strict_int(data.get("base_seed"), "base_seed")
    minimum_events = _strict_int(
        data.get("minimum_events_per_symbol"), "minimum_events_per_symbol"
    )
    minimum_conditioning_events = _strict_int(
        data.get("minimum_conditioning_events"),
        "minimum_conditioning_events",
    )
    limits = EventClockResourceLimitsV1.from_dict(
        cast(Mapping[str, Any], data.get("limits", {}))
    )
    config_id = str(data.get("config_id", ""))
    if schema == NHPP_EVENT_CLOCK_CONFIG_SCHEMA_VERSION:
        return NonHomogeneousPoissonConfigV1(
            intensity_bin_count=_strict_int(
                data.get("intensity_bin_count"), "intensity_bin_count"
            ),
            smoothing_count=_finite_float(
                data.get("smoothing_count"), "smoothing_count"
            ),
            base_seed=base_seed,
            minimum_events_per_symbol=minimum_events,
            minimum_conditioning_events=minimum_conditioning_events,
            limits=limits,
            config_id=config_id,
            schema_version=schema,
        )
    if schema == COX_EVENT_CLOCK_CONFIG_SCHEMA_VERSION:
        return CoxProcessConfigV1(
            minimum_gamma_shape=_finite_float(
                data.get("minimum_gamma_shape"), "minimum_gamma_shape"
            ),
            maximum_gamma_shape=_finite_float(
                data.get("maximum_gamma_shape"), "maximum_gamma_shape"
            ),
            dispersion_floor=_finite_float(
                data.get("dispersion_floor"), "dispersion_floor"
            ),
            base_seed=base_seed,
            minimum_events_per_symbol=minimum_events,
            minimum_conditioning_events=minimum_conditioning_events,
            limits=limits,
            config_id=config_id,
            schema_version=schema,
        )
    if schema == ACD_EVENT_CLOCK_CONFIG_SCHEMA_VERSION:
        return AutoregressiveConditionalDurationConfigV1(
            coefficient_grid_size=_strict_int(
                data.get("coefficient_grid_size"), "coefficient_grid_size"
            ),
            stationarity_margin=_finite_float(
                data.get("stationarity_margin"), "stationarity_margin"
            ),
            minimum_conditional_duration_seconds=_finite_float(
                data.get("minimum_conditional_duration_seconds"),
                "minimum_conditional_duration_seconds",
            ),
            base_seed=base_seed,
            minimum_events_per_symbol=minimum_events,
            minimum_conditioning_events=minimum_conditioning_events,
            limits=limits,
            config_id=config_id,
            schema_version=schema,
        )
    if schema == HIDDEN_MARKOV_EVENT_CLOCK_CONFIG_SCHEMA_VERSION:
        return HiddenMarkovDurationMarkConfigV1(
            state_count=_strict_int(data.get("state_count"), "state_count"),
            convergence_tolerance=_finite_float(
                data.get("convergence_tolerance"), "convergence_tolerance"
            ),
            variance_floor=_finite_float(
                data.get("variance_floor"), "variance_floor"
            ),
            probability_smoothing=_finite_float(
                data.get("probability_smoothing"), "probability_smoothing"
            ),
            base_seed=base_seed,
            minimum_events_per_symbol=minimum_events,
            minimum_conditioning_events=minimum_conditioning_events,
            limits=limits,
            config_id=config_id,
            schema_version=schema,
        )
    raise ValueError("unsupported event-clock config schema")


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode()
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _require_schema(data: Mapping[str, Any], expected: str) -> None:
    if data.get("schema_version") != expected:
        raise ValueError(f"schema_version must be {expected}")


def _required_text(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("value must be non-empty text")
    return value.strip()


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    return _required_text(value)


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


def _string_tuple(value: Any) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        raise TypeError("value must be a sequence of text")
    return tuple(_required_text(item) for item in value)


def _symbol(value: Any) -> str:
    symbol = _required_text(value).upper()
    if not symbol.isascii() or not symbol.isalnum() or len(symbol) > 32:
        raise ValueError("invalid event-clock symbol")
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
    restored = cast(dict[str, JSONValue], json.loads(encoded))
    return restored


def _json_scalar(value: Any, name: str) -> JSONScalar:
    if value is None or isinstance(value, (str, bool)):
        return cast(JSONScalar, value)
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    raise ValueError(f"{name} must be a finite JSON scalar")


__all__ = [
    "ACD_EVENT_CLOCK_CONFIG_SCHEMA_VERSION",
    "ACD_GENERATOR_ID",
    "COX_EVENT_CLOCK_CONFIG_SCHEMA_VERSION",
    "COX_GENERATOR_ID",
    "EVENT_CLOCK_CALIBRATION_WINDOW_SCHEMA_VERSION",
    "EVENT_CLOCK_CANDIDATE_BATCH_SCHEMA_VERSION",
    "EVENT_CLOCK_CANDIDATE_LINEAGE_SCHEMA_VERSION",
    "EVENT_CLOCK_FIT_RESULT_SCHEMA_VERSION",
    "EVENT_CLOCK_GENERATION_EVIDENCE_SCHEMA_VERSION",
    "EVENT_CLOCK_IMPLEMENTATION_VERSION",
    "EVENT_CLOCK_RESOURCE_LIMITS_SCHEMA_VERSION",
    "HIDDEN_MARKOV_EVENT_CLOCK_CONFIG_SCHEMA_VERSION",
    "HIDDEN_MARKOV_GENERATOR_ID",
    "NHPP_EVENT_CLOCK_CONFIG_SCHEMA_VERSION",
    "NHPP_GENERATOR_ID",
    "AutoregressiveConditionalDurationConfigV1",
    "CoxProcessConfigV1",
    "EventClockCalibrationWindowV1",
    "EventClockCandidateBatchV1",
    "EventClockCandidateLineageV1",
    "EventClockConfigV1",
    "EventClockConfigurationV1",
    "EventClockFamily",
    "EventClockFitError",
    "EventClockFitResultV1",
    "EventClockFitStatus",
    "EventClockGenerationError",
    "EventClockGenerationEvidenceV1",
    "EventClockGenerationResultV1",
    "EventClockGenerationStatus",
    "EventClockResourceLimitsV1",
    "FittedEventClockBenchmarkGeneratorV1",
    "HiddenMarkovDurationMarkConfigV1",
    "NonHomogeneousPoissonConfigV1",
    "build_event_clock_benchmark_candidate",
    "build_event_clock_candidate_batches",
    "build_fitted_event_clock_generator",
    "default_event_clock_configs",
    "event_clock_config_from_dict",
    "fit_event_clock_challenger",
]
