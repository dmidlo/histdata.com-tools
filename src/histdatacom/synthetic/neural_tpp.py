"""Bounded dependency-free recurrent marked temporal point-process challenger.

The issue-#453 surface implements one small CPU-only RMTPP.  It is deliberately
separate from the classical, Hawkes, and regime registries.  Training uses
deterministic full-batch BPTT, and generation uses the declared intensity's
closed-form inverse CDF.  It is a research challenger, never a default.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import platform
import random
import time
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator, Mapping, Sequence
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

NEURAL_TPP_RESOURCE_LIMITS_SCHEMA_VERSION = (
    "histdatacom.neural-tpp-resource-limits.v1"
)
NEURAL_TPP_CONFIG_SCHEMA_VERSION = "histdatacom.neural-tpp-config.v1"
NEURAL_TPP_WINDOW_CONTEXT_SCHEMA_VERSION = (
    "histdatacom.neural-tpp-window-context.v1"
)
NEURAL_TPP_PROTECTED_WINDOW_SCHEMA_VERSION = (
    "histdatacom.neural-tpp-protected-window.v1"
)
NEURAL_TPP_DATASET_WINDOW_SCHEMA_VERSION = (
    "histdatacom.neural-tpp-dataset-window.v1"
)
NEURAL_TPP_DATASET_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.neural-tpp-dataset-manifest.v1"
)
NEURAL_TPP_TRAINING_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.neural-tpp-training-manifest.v1"
)
NEURAL_TPP_CHECKPOINT_SCHEMA_VERSION = "histdatacom.neural-tpp-checkpoint.v1"
NEURAL_TPP_FIT_RESULT_SCHEMA_VERSION = "histdatacom.neural-tpp-fit-result.v1"
NEURAL_TPP_GENERATION_LINEAGE_SCHEMA_VERSION = (
    "histdatacom.neural-tpp-generation-lineage.v1"
)
NEURAL_TPP_GENERATION_EVIDENCE_SCHEMA_VERSION = (
    "histdatacom.neural-tpp-generation-evidence.v1"
)
NEURAL_TPP_CANDIDATE_LINEAGE_SCHEMA_VERSION = (
    "histdatacom.neural-tpp-candidate-lineage.v1"
)
NEURAL_TPP_CANDIDATE_BATCH_SCHEMA_VERSION = (
    "histdatacom.neural-tpp-candidate-batch.v1"
)
NEURAL_TPP_IMPLEMENTATION_VERSION = "1.0.0"
NEURAL_TPP_ARCHITECTURE = "rmtpp_cpu_v1"
NEURAL_TPP_GENERATOR_ID = "histdatacom.neural-tpp.rmtpp-cpu-v1"

NANOSECONDS_PER_SECOND = 1_000_000_000
MARK_STATES = ("ask_only", "bid_only", "joint", "unchanged")
ASSIGNMENT_KINDS = ("epoch", "transition")
TRAINING_ROLES = ("train", "tune")
PROTECTED_ROLES = ("validation", "final_holdout")
PARAMETER_NAMES = (
    "initial_hidden",
    "recurrent_weights",
    "input_weights",
    "hidden_bias",
    "time_weights",
    "time_bias",
    "mark_weights",
    "mark_bias",
)


class NeuralTPPFitError(RuntimeError):
    """Raised when bounded neural training cannot produce a usable fit."""


class NeuralTPPGenerationError(RuntimeError):
    """Raised when bounded neural generation fails or refuses."""


class NeuralTPPFitStatus(str, Enum):
    """Terminal fit state."""

    FITTED = "fitted"
    REFUSED = "refused"
    FAILED = "failed"


class NeuralTPPGenerationStatus(str, Enum):
    """Terminal all-or-nothing generation state."""

    GENERATED = "generated"
    EMPTY = "empty"
    REFUSED = "refused"
    FAILED = "failed"


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode()
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _require_schema_value(actual: str, expected: str, name: str) -> None:
    if actual != expected:
        raise ValueError(f"unsupported {name} schema")


def _required_text(value: Any) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 1024:
        raise ValueError("required text is invalid")
    return value


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    return _required_text(value)


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    return value


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


def _positive_float(value: Any, name: str) -> float:
    result = _finite_float(value, name)
    if result <= 0.0:
        raise ValueError(f"{name} must be positive")
    return result


def _nonnegative_float(value: Any, name: str) -> float:
    result = _finite_float(value, name)
    if result < 0.0:
        raise ValueError(f"{name} must be nonnegative")
    return result


def _bounded_int(value: Any, name: str, minimum: int, maximum: int) -> int:
    result = _strict_int(value, name)
    if not minimum <= result <= maximum:
        raise ValueError(f"{name} is outside bounds")
    return result


def _sha256(value: Any, name: str) -> str:
    text = _required_text(value)
    if len(text) != 64 or any(char not in "0123456789abcdef" for char in text):
        raise ValueError(f"{name} must be lowercase SHA-256")
    return text


def _optional_sha256(value: Any, name: str) -> str | None:
    return None if value is None else _sha256(value, name)


def _symbol(value: Any) -> str:
    text = _required_text(value).upper()
    if not text.isalpha() or not 3 <= len(text) <= 16:
        raise ValueError("symbol is invalid")
    return text


def _sequence(value: Any, name: str) -> Sequence[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise TypeError(f"{name} must be a sequence")
    return value


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be an object")
    return cast(Mapping[str, Any], value)


def _json_mapping(text: str, maximum: int) -> Mapping[str, Any]:
    if not isinstance(text, str) or len(text.encode()) > maximum:
        raise ValueError("JSON payload exceeds bound")
    value = json.loads(text)
    return _mapping(value, "JSON payload")


def _benchmark_event_key(event: BenchmarkEventV1) -> tuple[Any, ...]:
    return (
        event.event_time_ns,
        event.symbol,
        event.event_sequence,
        event.benchmark_event_id,
    )


def _event_mark(
    event: BenchmarkEventV1,
    previous: tuple[float, float] | None,
) -> str:
    if previous is None:
        return "unchanged"
    bid_changed = event.bid != previous[0]
    ask_changed = event.ask != previous[1]
    if bid_changed and ask_changed:
        return "joint"
    if bid_changed:
        return "bid_only"
    if ask_changed:
        return "ask_only"
    return "unchanged"


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


def _event_content_digest(events: Sequence[BenchmarkEventV1]) -> str:
    return hashlib.sha256(
        canonical_contract_json(
            [
                item.to_dict()
                for item in sorted(events, key=_benchmark_event_key)
            ]
        ).encode()
    ).hexdigest()


def _runtime_metadata() -> dict[str, JSONValue]:
    return {
        "accelerator_policy": "cpu_only",
        "accelerator_count": 0,
        "python_implementation": platform.python_implementation(),
        "python_version": platform.python_version(),
        "machine": platform.machine() or "unknown",
        "operating_system": platform.system() or "unknown",
        "deterministic_math_scope": (
            "same-python-implementation-version-and-machine-class"
        ),
    }


@dataclass(frozen=True, slots=True)
class NeuralTPPResourceLimitsV1:
    """Independent fit, checkpoint, and generation resource envelopes."""

    max_fit_events: int = 100_000
    max_fit_windows: int = 256
    max_sequence_events: int = 50_000
    max_hidden_dimension: int = 16
    max_mark_count: int = 64
    max_input_dimension: int = 128
    max_epochs: int = 128
    max_gradient_work: int = 200_000_000
    max_parameter_count: int = 100_000
    max_parameter_bytes: int = 4 * 1024 * 1024
    max_diagnostic_bytes: int = 8 * 1024 * 1024
    max_checkpoint_bytes: int = 4 * 1024 * 1024
    max_fit_memory_bytes: int = 512 * 1024 * 1024
    max_fit_wall_time_ms: int = 30_000
    max_generation_steps: int = 100_000
    max_generated_events: int = 20_000
    max_events_per_interval: int = 512
    max_candidate_amplification: float = 8.0
    max_history_events: int = 20_000
    max_history_lookback_ns: int = 7 * 24 * 60 * 60 * NANOSECONDS_PER_SECOND
    max_generation_memory_bytes: int = 512 * 1024 * 1024
    max_generation_output_bytes: int = 64 * 1024 * 1024
    max_generation_wall_time_ms: int = 10_000
    schema_version: str = NEURAL_TPP_RESOURCE_LIMITS_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            NEURAL_TPP_RESOURCE_LIMITS_SCHEMA_VERSION,
            "neural TPP resource limits",
        )
        bounds = {
            "max_fit_events": (1, 1_000_000),
            "max_fit_windows": (2, 10_000),
            "max_sequence_events": (2, 1_000_000),
            "max_hidden_dimension": (1, 128),
            "max_mark_count": (2, 1024),
            "max_input_dimension": (4, 2048),
            "max_epochs": (1, 10_000),
            "max_gradient_work": (1, 10_000_000_000),
            "max_parameter_count": (1, 10_000_000),
            "max_parameter_bytes": (1024, 1024**3),
            "max_diagnostic_bytes": (1024, 1024**3),
            "max_checkpoint_bytes": (1024, 1024**3),
            "max_fit_memory_bytes": (1024, 16 * 1024**3),
            "max_fit_wall_time_ms": (1, 3_600_000),
            "max_generation_steps": (1, 10_000_000),
            "max_generated_events": (1, 1_000_000),
            "max_events_per_interval": (1, 100_000),
            "max_history_events": (0, 1_000_000),
            "max_history_lookback_ns": (1, 365 * 24 * 60 * 60 * 10**9),
            "max_generation_memory_bytes": (1024, 16 * 1024**3),
            "max_generation_output_bytes": (1024, 1024**3),
            "max_generation_wall_time_ms": (1, 3_600_000),
        }
        for name, (minimum, maximum) in bounds.items():
            _bounded_int(getattr(self, name), name, minimum, maximum)
        amplification = _positive_float(
            self.max_candidate_amplification,
            "max_candidate_amplification",
        )
        if amplification > 1000.0:
            raise ValueError("max_candidate_amplification is outside bounds")

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            **{
                name: cast(JSONValue, getattr(self, name))
                for name in self.__dataclass_fields__
                if name != "schema_version"
            },
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> NeuralTPPResourceLimitsV1:
        _require_schema_value(
            str(data.get("schema_version", "")),
            NEURAL_TPP_RESOURCE_LIMITS_SCHEMA_VERSION,
            "neural TPP resource limits",
        )
        defaults = cls()
        return cls(
            **{
                name: data.get(name, getattr(defaults, name))
                for name in cls.__dataclass_fields__
            }
        )


@dataclass(frozen=True, slots=True)
class NeuralTPPConfigV1:
    """Identity-bearing fixed RMTPP architecture and training policy."""

    hidden_dimension: int = 8
    elapsed_slope_per_second: float = 0.001
    learning_rate: float = 0.002
    gradient_clip_norm: float = 5.0
    parameter_clip_absolute: float = 5.0
    max_epochs: int = 40
    early_stopping_patience: int = 7
    early_stopping_min_delta: float = 1e-6
    initialization_seed: int = 453
    initialization_scale: float = 0.05
    duration_normalization_epsilon: float = 1e-8
    minimum_elapsed_seconds: float = 1e-9
    near_duplicate_hamming_threshold: int = 1
    accelerator_policy: str = "cpu_only"
    requested_accelerator_count: int = 0
    split_policy: str = "session-first-occurrence-train-second-tune-v1"
    ordering_policy: str = "time-symbol-sequence-identity-v1"
    mark_policy: str = "destination-symbol-x-quote-transition-v1"
    duration_transform: str = "log1p-seconds-training-zscore-v1"
    start_token_policy: str = "explicit-start-one-hot-v1"
    prior_quote_reset_policy: str = "per-window-per-symbol-v1"
    unsupported_interval_policy: str = "skip-with-hazard-cursor-advance-v1"
    initialization_policy: str = "seeded-uniform-weights-uniform-mark-bias-v1"
    uncertainty_method: str = "empirical-pit-and-mark-frequency-v1"
    limits: NeuralTPPResourceLimitsV1 = field(
        default_factory=NeuralTPPResourceLimitsV1
    )
    config_id: str = ""
    architecture: str = NEURAL_TPP_ARCHITECTURE
    schema_version: str = NEURAL_TPP_CONFIG_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            NEURAL_TPP_CONFIG_SCHEMA_VERSION,
            "neural TPP config",
        )
        if self.architecture != NEURAL_TPP_ARCHITECTURE:
            raise ValueError("neural TPP architecture is not fixed")
        hidden = _bounded_int(
            self.hidden_dimension,
            "hidden_dimension",
            1,
            self.limits.max_hidden_dimension,
        )
        if hidden != self.hidden_dimension:
            raise ValueError("hidden dimension differs")
        for name in (
            "elapsed_slope_per_second",
            "learning_rate",
            "gradient_clip_norm",
            "parameter_clip_absolute",
            "early_stopping_min_delta",
            "initialization_scale",
            "duration_normalization_epsilon",
            "minimum_elapsed_seconds",
        ):
            value = _positive_float(getattr(self, name), name)
            if value > 100.0:
                raise ValueError(f"{name} is outside bounds")
        _bounded_int(self.max_epochs, "max_epochs", 1, self.limits.max_epochs)
        _bounded_int(
            self.early_stopping_patience,
            "early_stopping_patience",
            1,
            self.max_epochs,
        )
        _bounded_int(
            self.initialization_seed,
            "initialization_seed",
            0,
            2**63 - 1,
        )
        _bounded_int(
            self.near_duplicate_hamming_threshold,
            "near_duplicate_hamming_threshold",
            0,
            8,
        )
        if self.accelerator_policy != "cpu_only":
            raise ValueError("neural TPP accelerator policy must be CPU-only")
        if (
            _strict_int(
                self.requested_accelerator_count,
                "requested_accelerator_count",
            )
            != 0
        ):
            raise ValueError("neural TPP refuses accelerator requests")
        for name in (
            "split_policy",
            "ordering_policy",
            "mark_policy",
            "duration_transform",
            "start_token_policy",
            "prior_quote_reset_policy",
            "unsupported_interval_policy",
            "initialization_policy",
            "uncertainty_method",
        ):
            _required_text(getattr(self, name))
        expected = _stable_id("neural-tpp-config", self.identity_payload())
        if self.config_id and self.config_id != expected:
            raise ValueError("neural TPP config_id differs")
        object.__setattr__(self, "config_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "architecture": self.architecture,
            "hidden_dimension": self.hidden_dimension,
            "elapsed_slope_per_second": self.elapsed_slope_per_second,
            "learning_rate": self.learning_rate,
            "gradient_clip_norm": self.gradient_clip_norm,
            "parameter_clip_absolute": self.parameter_clip_absolute,
            "max_epochs": self.max_epochs,
            "early_stopping_patience": self.early_stopping_patience,
            "early_stopping_min_delta": self.early_stopping_min_delta,
            "initialization_seed": self.initialization_seed,
            "initialization_scale": self.initialization_scale,
            "duration_normalization_epsilon": (
                self.duration_normalization_epsilon
            ),
            "minimum_elapsed_seconds": self.minimum_elapsed_seconds,
            "near_duplicate_hamming_threshold": (
                self.near_duplicate_hamming_threshold
            ),
            "accelerator_policy": self.accelerator_policy,
            "requested_accelerator_count": self.requested_accelerator_count,
            "split_policy": self.split_policy,
            "ordering_policy": self.ordering_policy,
            "mark_policy": self.mark_policy,
            "duration_transform": self.duration_transform,
            "start_token_policy": self.start_token_policy,
            "prior_quote_reset_policy": self.prior_quote_reset_policy,
            "unsupported_interval_policy": self.unsupported_interval_policy,
            "initialization_policy": self.initialization_policy,
            "uncertainty_method": self.uncertainty_method,
            "limits": self.limits.to_dict(),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "config_id": self.config_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> NeuralTPPConfigV1:
        _require_schema_value(
            str(data.get("schema_version", "")),
            NEURAL_TPP_CONFIG_SCHEMA_VERSION,
            "neural TPP config",
        )
        return cls(
            hidden_dimension=_strict_int(
                data.get("hidden_dimension"), "hidden_dimension"
            ),
            elapsed_slope_per_second=_finite_float(
                data.get("elapsed_slope_per_second"),
                "elapsed_slope_per_second",
            ),
            learning_rate=_finite_float(
                data.get("learning_rate"), "learning_rate"
            ),
            gradient_clip_norm=_finite_float(
                data.get("gradient_clip_norm"), "gradient_clip_norm"
            ),
            parameter_clip_absolute=_finite_float(
                data.get("parameter_clip_absolute"),
                "parameter_clip_absolute",
            ),
            max_epochs=_strict_int(data.get("max_epochs"), "max_epochs"),
            early_stopping_patience=_strict_int(
                data.get("early_stopping_patience"),
                "early_stopping_patience",
            ),
            early_stopping_min_delta=_finite_float(
                data.get("early_stopping_min_delta"),
                "early_stopping_min_delta",
            ),
            initialization_seed=_strict_int(
                data.get("initialization_seed"), "initialization_seed"
            ),
            initialization_scale=_finite_float(
                data.get("initialization_scale"), "initialization_scale"
            ),
            duration_normalization_epsilon=_finite_float(
                data.get("duration_normalization_epsilon"),
                "duration_normalization_epsilon",
            ),
            minimum_elapsed_seconds=_finite_float(
                data.get("minimum_elapsed_seconds"),
                "minimum_elapsed_seconds",
            ),
            near_duplicate_hamming_threshold=_strict_int(
                data.get("near_duplicate_hamming_threshold"),
                "near_duplicate_hamming_threshold",
            ),
            accelerator_policy=str(data.get("accelerator_policy", "")),
            requested_accelerator_count=_strict_int(
                data.get("requested_accelerator_count"),
                "requested_accelerator_count",
            ),
            split_policy=str(data.get("split_policy", "")),
            ordering_policy=str(data.get("ordering_policy", "")),
            mark_policy=str(data.get("mark_policy", "")),
            duration_transform=str(data.get("duration_transform", "")),
            start_token_policy=str(data.get("start_token_policy", "")),
            prior_quote_reset_policy=str(
                data.get("prior_quote_reset_policy", "")
            ),
            unsupported_interval_policy=str(
                data.get("unsupported_interval_policy", "")
            ),
            initialization_policy=str(data.get("initialization_policy", "")),
            uncertainty_method=str(data.get("uncertainty_method", "")),
            limits=NeuralTPPResourceLimitsV1.from_dict(
                _mapping(data.get("limits"), "limits")
            ),
            config_id=str(data.get("config_id", "")),
            architecture=str(data.get("architecture", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> NeuralTPPConfigV1:
        return cls.from_dict(_json_mapping(text, 8 * 1024 * 1024))


def default_neural_tpp_config() -> NeuralTPPConfigV1:
    """Return the one fixed neural challenger configuration."""
    return NeuralTPPConfigV1()


@dataclass(frozen=True, slots=True)
class NeuralTPPWindowContextV1:
    """Separate technological/session/observed context for one window."""

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
    context_id: str = ""
    schema_version: str = NEURAL_TPP_WINDOW_CONTEXT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            NEURAL_TPP_WINDOW_CONTEXT_SCHEMA_VERSION,
            "neural TPP window context",
        )
        for name in (
            "window_id",
            "session",
            "technology_label",
            "feed_epoch_definition_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        kind = _required_text(self.technology_assignment_kind)
        if kind not in ASSIGNMENT_KINDS:
            raise ValueError("technology assignment kind is invalid")
        epoch_id = _optional_text(self.epoch_id)
        boundary_id = _optional_text(self.boundary_id)
        support = self.boundary_support
        start = _optional_text(self.uncertainty_start_period)
        end = _optional_text(self.uncertainty_end_period)
        if kind == "epoch":
            if epoch_id is None or any(
                value is not None
                for value in (boundary_id, support, start, end)
            ):
                raise ValueError(
                    "stable epoch context has invalid boundary fields"
                )
        else:
            if epoch_id is not None or any(
                value is None for value in (boundary_id, support, start, end)
            ):
                raise ValueError("transition context lacks boundary evidence")
            support = _finite_float(support, "boundary_support")
            if not 0.0 <= support <= 1.0:
                raise ValueError(
                    "boundary support is outside probability bounds"
                )
            if (
                not _period(start)
                or not _period(end)
                or cast(str, start) > cast(str, end)
            ):
                raise ValueError("transition uncertainty period is invalid")
        observed = _optional_text(self.observed_context_id)
        available = self.observed_context_available_ns
        use = self.use_time_ns
        if observed is None:
            if available is not None or use is not None:
                raise ValueError("unbound observed context has timing fields")
        else:
            if available is None or use is None:
                raise ValueError("observed context lacks availability/use time")
            available = _strict_int(available, "observed_context_available_ns")
            use = _strict_int(use, "use_time_ns")
            if available < 0 or use < 0 or available > use:
                raise ValueError("observed context is not point-in-time valid")
        object.__setattr__(self, "technology_assignment_kind", kind)
        object.__setattr__(self, "epoch_id", epoch_id)
        object.__setattr__(self, "boundary_id", boundary_id)
        object.__setattr__(self, "boundary_support", support)
        object.__setattr__(self, "uncertainty_start_period", start)
        object.__setattr__(self, "uncertainty_end_period", end)
        object.__setattr__(self, "observed_context_id", observed)
        object.__setattr__(self, "observed_context_available_ns", available)
        object.__setattr__(self, "use_time_ns", use)
        expected = _stable_id(
            "neural-tpp-window-context", self.identity_payload()
        )
        if self.context_id and self.context_id != expected:
            raise ValueError("neural TPP context_id differs")
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
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "context_id": self.context_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> NeuralTPPWindowContextV1:
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
            epoch_id=cast(str | None, data.get("epoch_id")),
            boundary_id=cast(str | None, data.get("boundary_id")),
            boundary_support=cast(float | None, data.get("boundary_support")),
            uncertainty_start_period=cast(
                str | None, data.get("uncertainty_start_period")
            ),
            uncertainty_end_period=cast(
                str | None, data.get("uncertainty_end_period")
            ),
            observed_context_id=cast(
                str | None, data.get("observed_context_id")
            ),
            observed_context_available_ns=cast(
                int | None, data.get("observed_context_available_ns")
            ),
            use_time_ns=cast(int | None, data.get("use_time_ns")),
            context_id=str(data.get("context_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> NeuralTPPWindowContextV1:
        return cls.from_dict(_json_mapping(text, 1024 * 1024))


def _period(value: str | None) -> bool:
    return bool(
        value
        and len(value) == 6
        and value.isdigit()
        and 1 <= int(value[4:]) <= 12
    )


@dataclass(frozen=True, slots=True)
class NeuralTPPDatasetWindowV1:
    """Row-free content and near-duplicate evidence for one split window."""

    window_id: str
    role: str
    start_ns: int
    end_ns: int
    event_count: int
    event_content_sha256: str
    near_duplicate_signature: str
    session: str
    technology_label: str
    context_id: str
    evidence_id: str = ""
    schema_version: str = NEURAL_TPP_DATASET_WINDOW_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            NEURAL_TPP_DATASET_WINDOW_SCHEMA_VERSION,
            "neural TPP dataset window",
        )
        for name in (
            "window_id",
            "session",
            "technology_label",
            "context_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        role = _required_text(self.role)
        if role not in {*TRAINING_ROLES, *PROTECTED_ROLES}:
            raise ValueError("neural TPP dataset role is invalid")
        object.__setattr__(self, "role", role)
        start = _strict_int(self.start_ns, "start_ns")
        end = _strict_int(self.end_ns, "end_ns")
        if start < 0 or end <= start:
            raise ValueError("neural TPP dataset window bounds are invalid")
        count = _strict_int(self.event_count, "event_count")
        if count <= 0:
            raise ValueError("neural TPP dataset window requires events")
        object.__setattr__(
            self,
            "event_content_sha256",
            _sha256(self.event_content_sha256, "event_content_sha256"),
        )
        signature = _required_text(self.near_duplicate_signature)
        if len(signature) != 16 or any(
            char not in "0123456789abcdef" for char in signature
        ):
            raise ValueError("near-duplicate signature must be 64-bit hex")
        object.__setattr__(self, "near_duplicate_signature", signature)
        expected = _stable_id(
            "neural-tpp-dataset-window", self.identity_payload()
        )
        if self.evidence_id and self.evidence_id != expected:
            raise ValueError("neural TPP dataset window evidence_id differs")
        object.__setattr__(self, "evidence_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "window_id": self.window_id,
            "role": self.role,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "event_count": self.event_count,
            "event_content_sha256": self.event_content_sha256,
            "near_duplicate_signature": self.near_duplicate_signature,
            "session": self.session,
            "technology_label": self.technology_label,
            "context_id": self.context_id,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "evidence_id": self.evidence_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> NeuralTPPDatasetWindowV1:
        return cls(
            window_id=str(data.get("window_id", "")),
            role=str(data.get("role", "")),
            start_ns=_strict_int(data.get("start_ns"), "start_ns"),
            end_ns=_strict_int(data.get("end_ns"), "end_ns"),
            event_count=_strict_int(data.get("event_count"), "event_count"),
            event_content_sha256=str(data.get("event_content_sha256", "")),
            near_duplicate_signature=str(
                data.get("near_duplicate_signature", "")
            ),
            session=str(data.get("session", "")),
            technology_label=str(data.get("technology_label", "")),
            context_id=str(data.get("context_id", "")),
            evidence_id=str(data.get("evidence_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> NeuralTPPDatasetWindowV1:
        return cls.from_dict(_json_mapping(text, 1024 * 1024))


@dataclass(frozen=True, slots=True)
class NeuralTPPProtectedWindowV1:
    """Protected split evidence accepted without exposing event rows."""

    window: NeuralTPPDatasetWindowV1
    schema_version: str = NEURAL_TPP_PROTECTED_WINDOW_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            NEURAL_TPP_PROTECTED_WINDOW_SCHEMA_VERSION,
            "neural TPP protected window",
        )
        if self.window.role not in PROTECTED_ROLES:
            raise ValueError("protected neural window has a training role")

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "window": self.window.to_dict(),
        }

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> NeuralTPPProtectedWindowV1:
        return cls(
            window=NeuralTPPDatasetWindowV1.from_dict(
                _mapping(data.get("window"), "window")
            ),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> NeuralTPPProtectedWindowV1:
        return cls.from_dict(_json_mapping(text, 1024 * 1024))


@dataclass(frozen=True, slots=True)
class NeuralTPPDatasetManifestV1:
    """Immutable row-free train/tune/protected split and leakage evidence."""

    config_id: str
    split_policy: str
    ordering_policy: str
    mark_policy: str
    duration_transform: str
    start_token_policy: str
    prior_quote_reset_policy: str
    symbols: tuple[str, ...]
    mark_vocabulary: tuple[str, ...]
    windows: tuple[NeuralTPPDatasetWindowV1, ...]
    exact_duplicate_count: int
    near_duplicate_collision_count: int
    overlap_count: int
    event_count_by_role: Mapping[str, int]
    window_count_by_role: Mapping[str, int]
    dataset_id: str = ""
    schema_version: str = NEURAL_TPP_DATASET_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            NEURAL_TPP_DATASET_MANIFEST_SCHEMA_VERSION,
            "neural TPP dataset manifest",
        )
        object.__setattr__(self, "config_id", _required_text(self.config_id))
        for name in (
            "split_policy",
            "ordering_policy",
            "mark_policy",
            "duration_transform",
            "start_token_policy",
            "prior_quote_reset_policy",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        symbols = tuple(sorted({_symbol(value) for value in self.symbols}))
        if not symbols:
            raise ValueError("neural TPP dataset requires symbols")
        marks = tuple(_required_text(value) for value in self.mark_vocabulary)
        if len(marks) != len(set(marks)) or not marks:
            raise ValueError("neural TPP mark vocabulary is invalid")
        windows = tuple(
            sorted(
                self.windows,
                key=lambda item: (item.role, item.start_ns, item.window_id),
            )
        )
        if len(windows) < 2 or len({item.window_id for item in windows}) != len(
            windows
        ):
            raise ValueError("neural TPP dataset window identities are invalid")
        roles = {item.role for item in windows}
        if not set(TRAINING_ROLES).issubset(roles):
            raise ValueError("neural TPP dataset lacks train/tune windows")
        for name in (
            "exact_duplicate_count",
            "near_duplicate_collision_count",
            "overlap_count",
        ):
            if _strict_int(getattr(self, name), name) != 0:
                raise ValueError("neural TPP dataset leakage audit failed")
        event_counts = {
            str(key): _strict_int(value, f"event_count_by_role:{key}")
            for key, value in self.event_count_by_role.items()
        }
        window_counts = {
            str(key): _strict_int(value, f"window_count_by_role:{key}")
            for key, value in self.window_count_by_role.items()
        }
        expected_events: dict[str, int] = {}
        expected_windows: dict[str, int] = {}
        for item in windows:
            expected_events[item.role] = expected_events.get(item.role, 0) + (
                item.event_count
            )
            expected_windows[item.role] = expected_windows.get(item.role, 0) + 1
        if event_counts != expected_events or window_counts != expected_windows:
            raise ValueError("neural TPP dataset role summaries differ")
        object.__setattr__(self, "symbols", symbols)
        object.__setattr__(self, "mark_vocabulary", marks)
        object.__setattr__(self, "windows", windows)
        object.__setattr__(self, "event_count_by_role", event_counts)
        object.__setattr__(self, "window_count_by_role", window_counts)
        expected = _stable_id("neural-tpp-dataset", self.identity_payload())
        if self.dataset_id and self.dataset_id != expected:
            raise ValueError("neural TPP dataset_id differs")
        object.__setattr__(self, "dataset_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "config_id": self.config_id,
            "split_policy": self.split_policy,
            "ordering_policy": self.ordering_policy,
            "mark_policy": self.mark_policy,
            "duration_transform": self.duration_transform,
            "start_token_policy": self.start_token_policy,
            "prior_quote_reset_policy": self.prior_quote_reset_policy,
            "symbols": list(self.symbols),
            "mark_vocabulary": list(self.mark_vocabulary),
            "windows": [item.to_dict() for item in self.windows],
            "exact_duplicate_count": self.exact_duplicate_count,
            "near_duplicate_collision_count": (
                self.near_duplicate_collision_count
            ),
            "overlap_count": self.overlap_count,
            "event_count_by_role": dict(self.event_count_by_role),
            "window_count_by_role": dict(self.window_count_by_role),
            "rows_embedded": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "dataset_id": self.dataset_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> NeuralTPPDatasetManifestV1:
        if data.get("rows_embedded") is not False:
            raise ValueError("neural TPP dataset manifest embeds rows")
        return cls(
            config_id=str(data.get("config_id", "")),
            split_policy=str(data.get("split_policy", "")),
            ordering_policy=str(data.get("ordering_policy", "")),
            mark_policy=str(data.get("mark_policy", "")),
            duration_transform=str(data.get("duration_transform", "")),
            start_token_policy=str(data.get("start_token_policy", "")),
            prior_quote_reset_policy=str(
                data.get("prior_quote_reset_policy", "")
            ),
            symbols=tuple(
                str(value)
                for value in _sequence(data.get("symbols"), "symbols")
            ),
            mark_vocabulary=tuple(
                str(value)
                for value in _sequence(
                    data.get("mark_vocabulary"), "mark_vocabulary"
                )
            ),
            windows=tuple(
                NeuralTPPDatasetWindowV1.from_dict(_mapping(value, "window"))
                for value in _sequence(data.get("windows"), "windows")
            ),
            exact_duplicate_count=_strict_int(
                data.get("exact_duplicate_count"), "exact_duplicate_count"
            ),
            near_duplicate_collision_count=_strict_int(
                data.get("near_duplicate_collision_count"),
                "near_duplicate_collision_count",
            ),
            overlap_count=_strict_int(
                data.get("overlap_count"), "overlap_count"
            ),
            event_count_by_role={
                str(key): _strict_int(value, str(key))
                for key, value in _mapping(
                    data.get("event_count_by_role"), "event_count_by_role"
                ).items()
            },
            window_count_by_role={
                str(key): _strict_int(value, str(key))
                for key, value in _mapping(
                    data.get("window_count_by_role"), "window_count_by_role"
                ).items()
            },
            dataset_id=str(data.get("dataset_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> NeuralTPPDatasetManifestV1:
        return cls.from_dict(_json_mapping(text, 8 * 1024 * 1024))


@dataclass(frozen=True, slots=True)
class NeuralTPPCheckpointV1:
    """Immutable selected parameters and training-only preprocessing state."""

    config_id: str
    dataset_id: str
    architecture: str
    hidden_dimension: int
    input_dimension: int
    mark_vocabulary: tuple[str, ...]
    input_vocabulary: tuple[str, ...]
    duration_log_mean: float
    duration_log_scale: float
    selected_epoch: int
    train_negative_log_likelihood: float
    tune_negative_log_likelihood: float
    train_time_negative_log_likelihood: float
    train_mark_negative_log_likelihood: float
    tune_time_negative_log_likelihood: float
    tune_mark_negative_log_likelihood: float
    tune_mark_accuracy: float
    tune_log_duration_rmse: float
    tune_mean_pit: float
    parameters: Mapping[str, JSONValue]
    parameter_count: int
    parameter_bytes: int
    checkpoint_id: str = ""
    schema_version: str = NEURAL_TPP_CHECKPOINT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            NEURAL_TPP_CHECKPOINT_SCHEMA_VERSION,
            "neural TPP checkpoint",
        )
        for name in ("config_id", "dataset_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        if self.architecture != NEURAL_TPP_ARCHITECTURE:
            raise ValueError("neural TPP checkpoint architecture differs")
        hidden = _bounded_int(self.hidden_dimension, "hidden_dimension", 1, 128)
        input_dimension = _bounded_int(
            self.input_dimension, "input_dimension", 2, 2048
        )
        marks = tuple(_required_text(value) for value in self.mark_vocabulary)
        input_vocabulary = tuple(
            _required_text(value) for value in self.input_vocabulary
        )
        expected_inputs = (*marks, "<START>", "<LOG_DURATION>")
        if input_vocabulary != expected_inputs or input_dimension != len(
            input_vocabulary
        ):
            raise ValueError("neural TPP checkpoint input shape differs")
        scale = _positive_float(self.duration_log_scale, "duration_log_scale")
        object.__setattr__(
            self,
            "duration_log_mean",
            _finite_float(self.duration_log_mean, "duration_log_mean"),
        )
        object.__setattr__(self, "duration_log_scale", scale)
        _bounded_int(self.selected_epoch, "selected_epoch", 0, 10_000)
        for name in (
            "train_negative_log_likelihood",
            "tune_negative_log_likelihood",
            "train_time_negative_log_likelihood",
            "train_mark_negative_log_likelihood",
            "tune_time_negative_log_likelihood",
            "tune_mark_negative_log_likelihood",
        ):
            _finite_float(getattr(self, name), name)
        if not math.isclose(
            self.train_negative_log_likelihood,
            self.train_time_negative_log_likelihood
            + self.train_mark_negative_log_likelihood,
            rel_tol=1e-12,
            abs_tol=1e-12,
        ) or not math.isclose(
            self.tune_negative_log_likelihood,
            self.tune_time_negative_log_likelihood
            + self.tune_mark_negative_log_likelihood,
            rel_tol=1e-12,
            abs_tol=1e-12,
        ):
            raise ValueError("neural TPP checkpoint likelihood parts differ")
        _nonnegative_float(
            self.tune_log_duration_rmse, "tune_log_duration_rmse"
        )
        accuracy = _finite_float(self.tune_mark_accuracy, "tune_mark_accuracy")
        if not 0.0 <= accuracy <= 1.0:
            raise ValueError("neural TPP checkpoint accuracy is invalid")
        mean_pit = _finite_float(self.tune_mean_pit, "tune_mean_pit")
        if not 0.0 <= mean_pit <= 1.0:
            raise ValueError("neural TPP checkpoint PIT mean is invalid")
        parsed_parameters = _validate_parameter_shape(
            self.parameters,
            hidden,
            input_dimension,
            len(marks),
        )
        count = _parameter_count(parsed_parameters)
        parameter_bytes = len(
            canonical_contract_json(parsed_parameters).encode()
        )
        if count != _strict_int(self.parameter_count, "parameter_count"):
            raise ValueError("neural TPP checkpoint parameter count differs")
        if parameter_bytes != _strict_int(
            self.parameter_bytes, "parameter_bytes"
        ):
            raise ValueError("neural TPP checkpoint parameter bytes differ")
        object.__setattr__(self, "mark_vocabulary", marks)
        object.__setattr__(self, "input_vocabulary", input_vocabulary)
        object.__setattr__(self, "parameters", parsed_parameters)
        expected = _stable_id("neural-tpp-checkpoint", self.identity_payload())
        if self.checkpoint_id and self.checkpoint_id != expected:
            raise ValueError("neural TPP checkpoint_id differs")
        object.__setattr__(self, "checkpoint_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "config_id": self.config_id,
            "dataset_id": self.dataset_id,
            "architecture": self.architecture,
            "hidden_dimension": self.hidden_dimension,
            "input_dimension": self.input_dimension,
            "mark_vocabulary": list(self.mark_vocabulary),
            "input_vocabulary": list(self.input_vocabulary),
            "duration_log_mean": self.duration_log_mean,
            "duration_log_scale": self.duration_log_scale,
            "selected_epoch": self.selected_epoch,
            "train_negative_log_likelihood": (
                self.train_negative_log_likelihood
            ),
            "tune_negative_log_likelihood": self.tune_negative_log_likelihood,
            "train_time_negative_log_likelihood": (
                self.train_time_negative_log_likelihood
            ),
            "train_mark_negative_log_likelihood": (
                self.train_mark_negative_log_likelihood
            ),
            "tune_time_negative_log_likelihood": (
                self.tune_time_negative_log_likelihood
            ),
            "tune_mark_negative_log_likelihood": (
                self.tune_mark_negative_log_likelihood
            ),
            "tune_mark_accuracy": self.tune_mark_accuracy,
            "tune_log_duration_rmse": self.tune_log_duration_rmse,
            "tune_mean_pit": self.tune_mean_pit,
            "parameters": dict(self.parameters),
            "parameter_count": self.parameter_count,
            "parameter_bytes": self.parameter_bytes,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "checkpoint_id": self.checkpoint_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> NeuralTPPCheckpointV1:
        return cls(
            config_id=str(data.get("config_id", "")),
            dataset_id=str(data.get("dataset_id", "")),
            architecture=str(data.get("architecture", "")),
            hidden_dimension=_strict_int(
                data.get("hidden_dimension"), "hidden_dimension"
            ),
            input_dimension=_strict_int(
                data.get("input_dimension"), "input_dimension"
            ),
            mark_vocabulary=tuple(
                str(value)
                for value in _sequence(
                    data.get("mark_vocabulary"), "mark_vocabulary"
                )
            ),
            input_vocabulary=tuple(
                str(value)
                for value in _sequence(
                    data.get("input_vocabulary"), "input_vocabulary"
                )
            ),
            duration_log_mean=_finite_float(
                data.get("duration_log_mean"), "duration_log_mean"
            ),
            duration_log_scale=_finite_float(
                data.get("duration_log_scale"), "duration_log_scale"
            ),
            selected_epoch=_strict_int(
                data.get("selected_epoch"), "selected_epoch"
            ),
            train_negative_log_likelihood=_finite_float(
                data.get("train_negative_log_likelihood"),
                "train_negative_log_likelihood",
            ),
            tune_negative_log_likelihood=_finite_float(
                data.get("tune_negative_log_likelihood"),
                "tune_negative_log_likelihood",
            ),
            train_time_negative_log_likelihood=_finite_float(
                data.get("train_time_negative_log_likelihood"),
                "train_time_negative_log_likelihood",
            ),
            train_mark_negative_log_likelihood=_finite_float(
                data.get("train_mark_negative_log_likelihood"),
                "train_mark_negative_log_likelihood",
            ),
            tune_time_negative_log_likelihood=_finite_float(
                data.get("tune_time_negative_log_likelihood"),
                "tune_time_negative_log_likelihood",
            ),
            tune_mark_negative_log_likelihood=_finite_float(
                data.get("tune_mark_negative_log_likelihood"),
                "tune_mark_negative_log_likelihood",
            ),
            tune_mark_accuracy=_finite_float(
                data.get("tune_mark_accuracy"), "tune_mark_accuracy"
            ),
            tune_log_duration_rmse=_finite_float(
                data.get("tune_log_duration_rmse"),
                "tune_log_duration_rmse",
            ),
            tune_mean_pit=_finite_float(
                data.get("tune_mean_pit"), "tune_mean_pit"
            ),
            parameters=_mapping(data.get("parameters"), "parameters"),
            parameter_count=_strict_int(
                data.get("parameter_count"), "parameter_count"
            ),
            parameter_bytes=_strict_int(
                data.get("parameter_bytes"), "parameter_bytes"
            ),
            checkpoint_id=str(data.get("checkpoint_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> NeuralTPPCheckpointV1:
        return cls.from_dict(_json_mapping(text, 8 * 1024 * 1024))


@dataclass(frozen=True, slots=True)
class NeuralTPPTrainingManifestV1:
    """Bounded deterministic optimization trace and runtime evidence."""

    config_id: str
    dataset_id: str
    checkpoint_id: str
    initialization_seed: int
    selected_epoch: int
    completed_epoch_count: int
    early_stopped: bool
    loss_trace: tuple[Mapping[str, JSONScalar], ...]
    maximum_gradient_norm: float
    clipped_epoch_count: int
    gradient_work: int
    runtime_metadata: Mapping[str, JSONValue]
    wall_time_ms: int
    peak_memory_bytes: int
    training_id: str = ""
    schema_version: str = NEURAL_TPP_TRAINING_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            NEURAL_TPP_TRAINING_MANIFEST_SCHEMA_VERSION,
            "neural TPP training manifest",
        )
        for name in ("config_id", "dataset_id", "checkpoint_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        _bounded_int(
            self.initialization_seed, "initialization_seed", 0, 2**63 - 1
        )
        selected = _bounded_int(
            self.selected_epoch, "selected_epoch", 0, 10_000
        )
        completed = _bounded_int(
            self.completed_epoch_count, "completed_epoch_count", 1, 10_000
        )
        if selected > completed:
            raise ValueError("selected neural epoch follows completed training")
        object.__setattr__(
            self,
            "early_stopped",
            _strict_bool(self.early_stopped, "early_stopped"),
        )
        trace = tuple(dict(item) for item in self.loss_trace)
        if len(trace) != completed + 1:
            raise ValueError("neural TPP loss trace cardinality differs")
        for expected_epoch, item in enumerate(trace):
            if _strict_int(item.get("epoch"), "epoch") != expected_epoch:
                raise ValueError(
                    "neural TPP loss trace epochs are not contiguous"
                )
            for name in (
                "train_negative_log_likelihood",
                "tune_negative_log_likelihood",
                "train_time_negative_log_likelihood",
                "train_mark_negative_log_likelihood",
                "tune_time_negative_log_likelihood",
                "tune_mark_negative_log_likelihood",
                "gradient_norm",
            ):
                _finite_float(item.get(name), name)
            _nonnegative_float(
                item.get("tune_log_duration_rmse"),
                "tune_log_duration_rmse",
            )
            _nonnegative_float(item.get("gradient_norm"), "gradient_norm")
            accuracy = _finite_float(
                item.get("tune_mark_accuracy"), "tune_mark_accuracy"
            )
            if not 0.0 <= accuracy <= 1.0:
                raise ValueError("neural TPP trace accuracy is invalid")
            mean_pit = _finite_float(item.get("tune_mean_pit"), "tune_mean_pit")
            if not 0.0 <= mean_pit <= 1.0:
                raise ValueError("neural TPP trace PIT mean is invalid")
        _nonnegative_float(self.maximum_gradient_norm, "maximum_gradient_norm")
        _bounded_int(
            self.clipped_epoch_count,
            "clipped_epoch_count",
            0,
            completed,
        )
        _bounded_int(self.gradient_work, "gradient_work", 1, 10_000_000_000)
        metadata = dict(self.runtime_metadata)
        if (
            metadata.get("accelerator_policy") != "cpu_only"
            or metadata.get("accelerator_count") != 0
        ):
            raise ValueError("neural TPP runtime metadata is not CPU-only")
        _bounded_int(self.wall_time_ms, "wall_time_ms", 0, 3_600_000)
        _bounded_int(
            self.peak_memory_bytes, "peak_memory_bytes", 0, 16 * 1024**3
        )
        object.__setattr__(self, "loss_trace", trace)
        object.__setattr__(self, "runtime_metadata", metadata)
        expected = _stable_id("neural-tpp-training", self.identity_payload())
        if self.training_id and self.training_id != expected:
            raise ValueError("neural TPP training_id differs")
        object.__setattr__(self, "training_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "config_id": self.config_id,
            "dataset_id": self.dataset_id,
            "checkpoint_id": self.checkpoint_id,
            "initialization_seed": self.initialization_seed,
            "selected_epoch": self.selected_epoch,
            "completed_epoch_count": self.completed_epoch_count,
            "early_stopped": self.early_stopped,
            "loss_trace": [dict(item) for item in self.loss_trace],
            "maximum_gradient_norm": self.maximum_gradient_norm,
            "clipped_epoch_count": self.clipped_epoch_count,
            "gradient_work": self.gradient_work,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "runtime_metadata": dict(self.runtime_metadata),
            "wall_time_ms": self.wall_time_ms,
            "peak_memory_bytes": self.peak_memory_bytes,
            "training_id": self.training_id,
        }

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> NeuralTPPTrainingManifestV1:
        return cls(
            config_id=str(data.get("config_id", "")),
            dataset_id=str(data.get("dataset_id", "")),
            checkpoint_id=str(data.get("checkpoint_id", "")),
            initialization_seed=_strict_int(
                data.get("initialization_seed"), "initialization_seed"
            ),
            selected_epoch=_strict_int(
                data.get("selected_epoch"), "selected_epoch"
            ),
            completed_epoch_count=_strict_int(
                data.get("completed_epoch_count"), "completed_epoch_count"
            ),
            early_stopped=_strict_bool(
                data.get("early_stopped"), "early_stopped"
            ),
            loss_trace=tuple(
                _mapping(value, "loss trace")
                for value in _sequence(data.get("loss_trace"), "loss_trace")
            ),
            maximum_gradient_norm=_finite_float(
                data.get("maximum_gradient_norm"), "maximum_gradient_norm"
            ),
            clipped_epoch_count=_strict_int(
                data.get("clipped_epoch_count"), "clipped_epoch_count"
            ),
            gradient_work=_strict_int(
                data.get("gradient_work"), "gradient_work"
            ),
            runtime_metadata=_mapping(
                data.get("runtime_metadata"), "runtime_metadata"
            ),
            wall_time_ms=_strict_int(data.get("wall_time_ms"), "wall_time_ms"),
            peak_memory_bytes=_strict_int(
                data.get("peak_memory_bytes"), "peak_memory_bytes"
            ),
            training_id=str(data.get("training_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> NeuralTPPTrainingManifestV1:
        return cls.from_dict(_json_mapping(text, 16 * 1024 * 1024))


@dataclass(frozen=True, slots=True)
class NeuralTPPFitResultV1:
    """Closed or fitted neural challenger with immutable nested artifacts."""

    config_id: str
    dataset_content_sha256: str
    context_content_sha256: str
    information_mode: InformationMode
    as_of_ns: int | None
    symbols: tuple[str, ...]
    status: NeuralTPPFitStatus
    converged: bool
    training_event_count: int
    tuning_event_count: int
    training_window_count: int
    tuning_window_count: int
    selected_epoch: int
    train_negative_log_likelihood: float | None
    tune_negative_log_likelihood: float | None
    dataset_manifest: NeuralTPPDatasetManifestV1 | None
    training_manifest: NeuralTPPTrainingManifestV1 | None
    checkpoint: NeuralTPPCheckpointV1 | None
    diagnostics: Mapping[str, JSONScalar]
    estimated_peak_memory_bytes: int
    failure_reason: str | None = None
    fit_id: str = ""
    schema_version: str = NEURAL_TPP_FIT_RESULT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            NEURAL_TPP_FIT_RESULT_SCHEMA_VERSION,
            "neural TPP fit result",
        )
        object.__setattr__(self, "config_id", _required_text(self.config_id))
        object.__setattr__(
            self,
            "dataset_content_sha256",
            _sha256(self.dataset_content_sha256, "dataset_content_sha256"),
        )
        object.__setattr__(
            self,
            "context_content_sha256",
            _sha256(self.context_content_sha256, "context_content_sha256"),
        )
        mode = InformationMode.from_value(self.information_mode)
        object.__setattr__(self, "information_mode", mode)
        if mode is InformationMode.EX_ANTE_SIMULATION:
            if (
                self.as_of_ns is None
                or _strict_int(self.as_of_ns, "as_of_ns") < 0
            ):
                raise ValueError("ex-ante neural fit requires as_of_ns")
        elif self.as_of_ns is not None:
            raise ValueError("ex-post neural fit forbids as_of_ns")
        symbols = tuple(sorted({_symbol(value) for value in self.symbols}))
        if not symbols:
            raise ValueError("neural TPP fit requires symbols")
        object.__setattr__(self, "symbols", symbols)
        status = NeuralTPPFitStatus(self.status)
        object.__setattr__(self, "status", status)
        converged = _strict_bool(self.converged, "converged")
        object.__setattr__(self, "converged", converged)
        for name in (
            "training_event_count",
            "tuning_event_count",
            "training_window_count",
            "tuning_window_count",
            "selected_epoch",
            "estimated_peak_memory_bytes",
        ):
            if _strict_int(getattr(self, name), name) < 0:
                raise ValueError(f"{name} must be nonnegative")
        diagnostics = {
            _required_text(key): _json_scalar(value, key)
            for key, value in self.diagnostics.items()
        }
        object.__setattr__(self, "diagnostics", diagnostics)
        failure = _optional_text(self.failure_reason)
        object.__setattr__(self, "failure_reason", failure)
        if status is NeuralTPPFitStatus.FITTED:
            if (
                not converged
                or failure is not None
                or self.dataset_manifest is None
                or self.training_manifest is None
                or self.checkpoint is None
                or self.train_negative_log_likelihood is None
                or self.tune_negative_log_likelihood is None
            ):
                raise ValueError("fitted neural TPP lacks complete artifacts")
            if (
                min(
                    self.training_event_count,
                    self.tuning_event_count,
                    self.training_window_count,
                    self.tuning_window_count,
                    self.selected_epoch,
                )
                <= 0
            ):
                raise ValueError("fitted neural TPP lacks training support")
            train_nll = _finite_float(
                self.train_negative_log_likelihood,
                "train_negative_log_likelihood",
            )
            tune_nll = _finite_float(
                self.tune_negative_log_likelihood,
                "tune_negative_log_likelihood",
            )
            object.__setattr__(self, "train_negative_log_likelihood", train_nll)
            object.__setattr__(self, "tune_negative_log_likelihood", tune_nll)
            if (
                self.dataset_manifest.config_id != self.config_id
                or self.training_manifest.config_id != self.config_id
                or self.checkpoint.config_id != self.config_id
                or self.training_manifest.dataset_id
                != self.dataset_manifest.dataset_id
                or self.checkpoint.dataset_id
                != self.dataset_manifest.dataset_id
                or self.training_manifest.checkpoint_id
                != self.checkpoint.checkpoint_id
                or self.selected_epoch != self.checkpoint.selected_epoch
                or train_nll != self.checkpoint.train_negative_log_likelihood
                or tune_nll != self.checkpoint.tune_negative_log_likelihood
            ):
                raise ValueError("neural TPP nested artifact identities differ")
        else:
            if (
                converged
                or failure is None
                or self.dataset_manifest is not None
                or self.training_manifest is not None
                or self.checkpoint is not None
                or self.train_negative_log_likelihood is not None
                or self.tune_negative_log_likelihood is not None
                or self.selected_epoch != 0
            ):
                raise ValueError("closed neural TPP fit exposes partial model")
        expected = _stable_id("neural-tpp-fit", self.identity_payload())
        if self.fit_id and self.fit_id != expected:
            raise ValueError("neural TPP fit_id differs")
        object.__setattr__(self, "fit_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "config_id": self.config_id,
            "dataset_content_sha256": self.dataset_content_sha256,
            "context_content_sha256": self.context_content_sha256,
            "information_mode": self.information_mode.value,
            "as_of_ns": self.as_of_ns,
            "symbols": list(self.symbols),
            "status": self.status.value,
            "converged": self.converged,
            "training_event_count": self.training_event_count,
            "tuning_event_count": self.tuning_event_count,
            "training_window_count": self.training_window_count,
            "tuning_window_count": self.tuning_window_count,
            "selected_epoch": self.selected_epoch,
            "train_negative_log_likelihood": (
                self.train_negative_log_likelihood
            ),
            "tune_negative_log_likelihood": self.tune_negative_log_likelihood,
            "dataset_manifest": (
                self.dataset_manifest.to_dict()
                if self.dataset_manifest is not None
                else None
            ),
            "training_manifest": (
                {
                    **self.training_manifest.identity_payload(),
                    "training_id": self.training_manifest.training_id,
                }
                if self.training_manifest is not None
                else None
            ),
            "checkpoint": (
                self.checkpoint.to_dict()
                if self.checkpoint is not None
                else None
            ),
            "diagnostics": dict(self.diagnostics),
            "estimated_peak_memory_bytes": self.estimated_peak_memory_bytes,
            "failure_reason": self.failure_reason,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        payload = self.identity_payload()
        if self.training_manifest is not None:
            payload["training_manifest"] = self.training_manifest.to_dict()
        return {**payload, "fit_id": self.fit_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> NeuralTPPFitResultV1:
        dataset_value = data.get("dataset_manifest")
        training_value = data.get("training_manifest")
        checkpoint_value = data.get("checkpoint")
        return cls(
            config_id=str(data.get("config_id", "")),
            dataset_content_sha256=str(data.get("dataset_content_sha256", "")),
            context_content_sha256=str(data.get("context_content_sha256", "")),
            information_mode=InformationMode.from_value(
                _required_text(data.get("information_mode"))
            ),
            as_of_ns=cast(int | None, data.get("as_of_ns")),
            symbols=tuple(
                str(value)
                for value in _sequence(data.get("symbols"), "symbols")
            ),
            status=NeuralTPPFitStatus(str(data.get("status", ""))),
            converged=_strict_bool(data.get("converged"), "converged"),
            training_event_count=_strict_int(
                data.get("training_event_count"), "training_event_count"
            ),
            tuning_event_count=_strict_int(
                data.get("tuning_event_count"), "tuning_event_count"
            ),
            training_window_count=_strict_int(
                data.get("training_window_count"), "training_window_count"
            ),
            tuning_window_count=_strict_int(
                data.get("tuning_window_count"), "tuning_window_count"
            ),
            selected_epoch=_strict_int(
                data.get("selected_epoch"), "selected_epoch"
            ),
            train_negative_log_likelihood=cast(
                float | None, data.get("train_negative_log_likelihood")
            ),
            tune_negative_log_likelihood=cast(
                float | None, data.get("tune_negative_log_likelihood")
            ),
            dataset_manifest=(
                NeuralTPPDatasetManifestV1.from_dict(
                    _mapping(dataset_value, "dataset_manifest")
                )
                if dataset_value is not None
                else None
            ),
            training_manifest=(
                NeuralTPPTrainingManifestV1.from_dict(
                    _mapping(training_value, "training_manifest")
                )
                if training_value is not None
                else None
            ),
            checkpoint=(
                NeuralTPPCheckpointV1.from_dict(
                    _mapping(checkpoint_value, "checkpoint")
                )
                if checkpoint_value is not None
                else None
            ),
            diagnostics={
                str(key): _json_scalar(value, str(key))
                for key, value in _mapping(
                    data.get("diagnostics"), "diagnostics"
                ).items()
            },
            estimated_peak_memory_bytes=_strict_int(
                data.get("estimated_peak_memory_bytes"),
                "estimated_peak_memory_bytes",
            ),
            failure_reason=cast(str | None, data.get("failure_reason")),
            fit_id=str(data.get("fit_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> NeuralTPPFitResultV1:
        return cls.from_dict(_json_mapping(text, 32 * 1024 * 1024))


def _json_scalar(value: Any, name: str) -> JSONScalar:
    if value is None or isinstance(value, (str, bool)):
        return cast(JSONScalar, value)
    if isinstance(value, int):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    raise ValueError(f"{name} must be a JSON scalar")


@dataclass(frozen=True, slots=True)
class _TrainingSequence:
    window_id: str
    start_ns: int
    end_ns: int
    durations_seconds: tuple[float, ...]
    marks: tuple[int, ...]
    censor_seconds: float


def build_neural_tpp_protected_window(
    window: EventClockCalibrationWindowV1,
    context: NeuralTPPWindowContextV1,
    *,
    role: str,
    symbols: Sequence[str],
) -> NeuralTPPProtectedWindowV1:
    """Build row-free protected-split evidence for leakage auditing."""
    if role not in PROTECTED_ROLES:
        raise ValueError("protected neural window role is invalid")
    if context.window_id != window.window_id:
        raise ValueError("protected neural window context identity differs")
    ordered_symbols = tuple(sorted({_symbol(value) for value in symbols}))
    evidence = _dataset_window_evidence(
        window,
        context,
        role=role,
        symbols=ordered_symbols,
    )
    return NeuralTPPProtectedWindowV1(evidence)


def _dataset_window_evidence(
    window: EventClockCalibrationWindowV1,
    context: NeuralTPPWindowContextV1,
    *,
    role: str,
    symbols: tuple[str, ...],
) -> NeuralTPPDatasetWindowV1:
    events = tuple(sorted(window.events, key=_benchmark_event_key))
    if not events:
        raise NeuralTPPFitError("neural TPP dataset window is empty")
    if any(
        event.symbol not in symbols
        or not window.start_ns <= event.event_time_ns < window.end_ns
        for event in events
    ):
        raise NeuralTPPFitError("neural TPP dataset event is outside scope")
    return NeuralTPPDatasetWindowV1(
        window_id=window.window_id,
        role=role,
        start_ns=window.start_ns,
        end_ns=window.end_ns,
        event_count=len(events),
        event_content_sha256=_event_content_digest(events),
        near_duplicate_signature=_near_duplicate_signature(
            events, window.start_ns
        ),
        session=context.session,
        technology_label=context.technology_label,
        context_id=context.context_id,
    )


def _near_duplicate_signature(
    events: Sequence[BenchmarkEventV1], start_ns: int
) -> str:
    prior_quotes: dict[str, tuple[float, float]] = {}
    previous_ns = start_ns
    weights = [0] * 64
    for event in sorted(events, key=_benchmark_event_key):
        delta_bucket = max(0, (event.event_time_ns - previous_ns) // 1_000_000)
        mark = _event_mark(event, prior_quotes.get(event.symbol))
        token = f"{event.symbol}|{mark}|{min(delta_bucket, 60_000)}"
        digest = int.from_bytes(
            hashlib.sha256(token.encode()).digest()[:8], "big"
        )
        for bit in range(64):
            weights[bit] += 1 if digest & (1 << bit) else -1
        prior_quotes[event.symbol] = (event.bid, event.ask)
        previous_ns = event.event_time_ns
    signature = sum(1 << bit for bit, value in enumerate(weights) if value >= 0)
    return f"{signature:016x}"


def _hamming(left: str, right: str) -> int:
    return (int(left, 16) ^ int(right, 16)).bit_count()


def _mark_vocabulary(symbols: Sequence[str]) -> tuple[str, ...]:
    return tuple(
        f"{symbol}:{mark}" for symbol in sorted(symbols) for mark in MARK_STATES
    )


def _split_assignments(
    windows: Sequence[EventClockCalibrationWindowV1],
    contexts: Mapping[str, NeuralTPPWindowContextV1],
    explicit: Mapping[str, str] | None,
) -> dict[str, str]:
    if explicit is not None:
        explicit_assignments = {
            str(key): str(value) for key, value in explicit.items()
        }
        if set(explicit_assignments) != {
            item.window_id for item in windows
        } or any(
            value not in TRAINING_ROLES
            for value in explicit_assignments.values()
        ):
            raise NeuralTPPFitError(
                "explicit neural split assignments are invalid"
            )
        if set(explicit_assignments.values()) != set(TRAINING_ROLES):
            raise NeuralTPPFitError(
                "explicit neural split lacks train/tune roles"
            )
        return explicit_assignments
    by_session: dict[str, list[EventClockCalibrationWindowV1]] = defaultdict(
        list
    )
    for window in windows:
        by_session[contexts[window.window_id].session].append(window)
    assignments: dict[str, str] = {}
    for values in by_session.values():
        ordered = sorted(
            values, key=lambda item: (item.start_ns, item.window_id)
        )
        if len(ordered) < 2:
            raise NeuralTPPFitError(
                "neural split requires two calibration windows per session"
            )
        cut = max(1, len(ordered) // 2)
        if cut == len(ordered):
            cut -= 1
        assignments.update(
            {
                item.window_id: "train" if index < cut else "tune"
                for index, item in enumerate(ordered)
            }
        )
    return assignments


def _build_dataset(
    config: NeuralTPPConfigV1,
    windows: Sequence[EventClockCalibrationWindowV1],
    contexts: Sequence[NeuralTPPWindowContextV1],
    protected_windows: Sequence[NeuralTPPProtectedWindowV1],
    split_assignments: Mapping[str, str] | None,
    symbols: tuple[str, ...],
) -> tuple[
    NeuralTPPDatasetManifestV1,
    tuple[_TrainingSequence, ...],
    tuple[_TrainingSequence, ...],
    float,
    float,
]:
    context_by_window = {item.window_id: item for item in contexts}
    if len(context_by_window) != len(contexts) or set(context_by_window) != {
        item.window_id for item in windows
    }:
        raise NeuralTPPFitError("neural TPP calibration contexts differ")
    assignments = _split_assignments(
        windows, context_by_window, split_assignments
    )
    evidence = [
        _dataset_window_evidence(
            window,
            context_by_window[window.window_id],
            role=assignments[window.window_id],
            symbols=symbols,
        )
        for window in windows
    ]
    evidence.extend(item.window for item in protected_windows)
    exact_duplicates = 0
    near_duplicates = 0
    overlaps = 0
    for index, left in enumerate(evidence):
        for right in evidence[index + 1 :]:
            exact_duplicates += int(
                left.event_content_sha256 == right.event_content_sha256
            )
            if left.role != right.role:
                near_duplicates += int(
                    _hamming(
                        left.near_duplicate_signature,
                        right.near_duplicate_signature,
                    )
                    <= config.near_duplicate_hamming_threshold
                )
            overlaps += int(
                max(left.start_ns, right.start_ns)
                < min(left.end_ns, right.end_ns)
            )
    if exact_duplicates or near_duplicates or overlaps:
        raise NeuralTPPFitError("neural TPP dataset leakage audit failed")
    event_counts = dict(Counter(item.role for item in evidence))
    event_counts = {
        role: sum(item.event_count for item in evidence if item.role == role)
        for role in event_counts
    }
    window_counts = dict(Counter(item.role for item in evidence))
    manifest = NeuralTPPDatasetManifestV1(
        config_id=config.config_id,
        split_policy=config.split_policy,
        ordering_policy=config.ordering_policy,
        mark_policy=config.mark_policy,
        duration_transform=config.duration_transform,
        start_token_policy=config.start_token_policy,
        prior_quote_reset_policy=config.prior_quote_reset_policy,
        symbols=symbols,
        mark_vocabulary=_mark_vocabulary(symbols),
        windows=tuple(evidence),
        exact_duplicate_count=0,
        near_duplicate_collision_count=0,
        overlap_count=0,
        event_count_by_role=event_counts,
        window_count_by_role=window_counts,
    )
    vocabulary = {
        value: index for index, value in enumerate(manifest.mark_vocabulary)
    }
    sequences = [
        _training_sequence(window, vocabulary, config)
        for window in sorted(
            windows, key=lambda item: (item.start_ns, item.window_id)
        )
    ]
    train = tuple(
        item for item in sequences if assignments[item.window_id] == "train"
    )
    tune = tuple(
        item for item in sequences if assignments[item.window_id] == "tune"
    )
    train_logs = [
        math.log1p(duration)
        for item in train
        for duration in item.durations_seconds
    ]
    if not train_logs:
        raise NeuralTPPFitError("neural TPP training split has no durations")
    mean = sum(train_logs) / len(train_logs)
    variance = sum((value - mean) ** 2 for value in train_logs) / len(
        train_logs
    )
    scale = max(config.duration_normalization_epsilon, math.sqrt(variance))
    return manifest, train, tune, mean, scale


def _training_sequence(
    window: EventClockCalibrationWindowV1,
    vocabulary: Mapping[str, int],
    config: NeuralTPPConfigV1,
) -> _TrainingSequence:
    events = tuple(sorted(window.events, key=_benchmark_event_key))
    previous_ns = window.start_ns
    prior_quotes: dict[str, tuple[float, float]] = {}
    durations: list[float] = []
    marks: list[int] = []
    for event in events:
        delta_ns = event.event_time_ns - previous_ns
        if delta_ns < 0:
            raise NeuralTPPFitError("neural TPP event ordering reverses time")
        mark = _event_mark(event, prior_quotes.get(event.symbol))
        durations.append(
            max(
                delta_ns / NANOSECONDS_PER_SECOND,
                config.minimum_elapsed_seconds,
            )
        )
        marks.append(vocabulary[f"{event.symbol}:{mark}"])
        prior_quotes[event.symbol] = (event.bid, event.ask)
        previous_ns = event.event_time_ns
    return _TrainingSequence(
        window_id=window.window_id,
        start_ns=window.start_ns,
        end_ns=window.end_ns,
        durations_seconds=tuple(durations),
        marks=tuple(marks),
        censor_seconds=max(
            0.0, (window.end_ns - previous_ns) / NANOSECONDS_PER_SECOND
        ),
    )


def _validate_parameter_shape(
    values: Mapping[str, JSONValue],
    hidden: int,
    input_dimension: int,
    mark_count: int,
) -> dict[str, JSONValue]:
    if set(values) != set(PARAMETER_NAMES):
        raise ValueError("neural TPP checkpoint parameter names differ")
    initial = _float_vector(values["initial_hidden"], hidden, "initial_hidden")
    recurrent = _float_matrix(
        values["recurrent_weights"], hidden, hidden, "recurrent_weights"
    )
    inputs = _float_matrix(
        values["input_weights"], hidden, input_dimension, "input_weights"
    )
    hidden_bias = _float_vector(values["hidden_bias"], hidden, "hidden_bias")
    time_weights = _float_vector(values["time_weights"], hidden, "time_weights")
    time_bias = _finite_float(values["time_bias"], "time_bias")
    mark_weights = _float_matrix(
        values["mark_weights"], mark_count, hidden, "mark_weights"
    )
    mark_bias = _float_vector(values["mark_bias"], mark_count, "mark_bias")
    return {
        "initial_hidden": cast(JSONValue, initial),
        "recurrent_weights": cast(JSONValue, recurrent),
        "input_weights": cast(JSONValue, inputs),
        "hidden_bias": cast(JSONValue, hidden_bias),
        "time_weights": cast(JSONValue, time_weights),
        "time_bias": time_bias,
        "mark_weights": cast(JSONValue, mark_weights),
        "mark_bias": cast(JSONValue, mark_bias),
    }


def _float_vector(value: Any, length: int, name: str) -> list[float]:
    sequence = _sequence(value, name)
    if len(sequence) != length:
        raise ValueError(f"{name} length differs")
    return [_finite_float(item, name) for item in sequence]


def _float_matrix(
    value: Any,
    rows: int,
    columns: int,
    name: str,
) -> list[list[float]]:
    sequence = _sequence(value, name)
    if len(sequence) != rows:
        raise ValueError(f"{name} row count differs")
    return [_float_vector(row, columns, name) for row in sequence]


def _parameter_count(parameters: Mapping[str, JSONValue]) -> int:
    return sum(1 for _ in _iter_numbers(parameters))


def _iter_numbers(value: Any) -> Iterator[float]:
    if isinstance(value, Mapping):
        for item in value.values():
            yield from _iter_numbers(item)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        for item in value:
            yield from _iter_numbers(item)
    else:
        yield _finite_float(value, "parameter")


def _initialize_parameters(
    config: NeuralTPPConfigV1,
    train: Sequence[_TrainingSequence],
    input_dimension: int,
    mark_count: int,
) -> dict[str, JSONValue]:
    rng = random.Random(config.initialization_seed)
    hidden = config.hidden_dimension

    def random_vector(length: int) -> list[float]:
        return [
            rng.uniform(
                -config.initialization_scale, config.initialization_scale
            )
            for _ in range(length)
        ]

    event_count = sum(len(item.marks) for item in train)
    exposure = sum(
        (item.end_ns - item.start_ns) / NANOSECONDS_PER_SECOND for item in train
    )
    rate = max(1e-8, event_count / max(exposure, 1e-8))
    parameters: dict[str, JSONValue] = {
        "initial_hidden": cast(JSONValue, random_vector(hidden)),
        "recurrent_weights": cast(
            JSONValue, [random_vector(hidden) for _ in range(hidden)]
        ),
        "input_weights": cast(
            JSONValue, [random_vector(input_dimension) for _ in range(hidden)]
        ),
        "hidden_bias": cast(JSONValue, [0.0] * hidden),
        "time_weights": cast(JSONValue, random_vector(hidden)),
        "time_bias": math.log(rate),
        "mark_weights": cast(
            JSONValue, [random_vector(hidden) for _ in range(mark_count)]
        ),
        "mark_bias": cast(JSONValue, [0.0] * mark_count),
    }
    return _validate_parameter_shape(
        parameters, hidden, input_dimension, mark_count
    )


def _input_vector(
    mark: int,
    duration: float,
    mark_count: int,
    duration_mean: float,
    duration_scale: float,
) -> list[float]:
    result = [0.0] * (mark_count + 2)
    result[mark] = 1.0
    result[-1] = (math.log1p(duration) - duration_mean) / duration_scale
    return result


def _start_input_vector(mark_count: int) -> list[float]:
    result = [0.0] * (mark_count + 2)
    result[mark_count] = 1.0
    return result


def _hidden_step(
    parameters: Mapping[str, JSONValue],
    previous: Sequence[float],
    inputs: Sequence[float],
) -> list[float]:
    recurrent = cast(Sequence[Sequence[float]], parameters["recurrent_weights"])
    input_weights = cast(Sequence[Sequence[float]], parameters["input_weights"])
    bias = cast(Sequence[float], parameters["hidden_bias"])
    return [
        math.tanh(
            bias[row]
            + sum(
                value * previous[column]
                for column, value in enumerate(recurrent[row])
            )
            + sum(
                value * inputs[column]
                for column, value in enumerate(input_weights[row])
            )
        )
        for row in range(len(previous))
    ]


def _time_base(
    parameters: Mapping[str, JSONValue], hidden: Sequence[float]
) -> float:
    weights = cast(Sequence[float], parameters["time_weights"])
    value = _finite_float(parameters["time_bias"], "time_bias") + sum(
        weight * state for weight, state in zip(weights, hidden)
    )
    if not -60.0 <= value <= 60.0:
        raise NeuralTPPFitError(
            "neural TPP time log-rate is outside safe bounds"
        )
    return value


def _time_integral(base: float, slope: float, duration: float) -> float:
    if duration < 0.0 or not math.isfinite(duration):
        raise NeuralTPPFitError("neural TPP duration is invalid")
    exponent = slope * duration
    if exponent > 60.0:
        raise NeuralTPPFitError("neural TPP compensator exponent exceeds bound")
    value = math.exp(base) * math.expm1(exponent) / slope
    if not math.isfinite(value):
        raise NeuralTPPFitError("neural TPP compensator is non-finite")
    return value


def _softmax(logits: Sequence[float]) -> list[float]:
    offset = max(logits)
    values = [math.exp(value - offset) for value in logits]
    total = sum(values)
    if total <= 0.0 or not math.isfinite(total):
        raise NeuralTPPFitError("neural TPP mark softmax collapsed")
    return [value / total for value in values]


def _mark_probabilities(
    parameters: Mapping[str, JSONValue], hidden: Sequence[float]
) -> list[float]:
    weights = cast(Sequence[Sequence[float]], parameters["mark_weights"])
    bias = cast(Sequence[float], parameters["mark_bias"])
    return _softmax(
        [
            bias[row]
            + sum(
                value * hidden[column]
                for column, value in enumerate(weights[row])
            )
            for row in range(len(weights))
        ]
    )


def _empty_gradients(
    hidden: int, input_dimension: int, mark_count: int
) -> dict[str, Any]:
    return {
        "initial_hidden": [0.0] * hidden,
        "recurrent_weights": [[0.0] * hidden for _ in range(hidden)],
        "input_weights": [[0.0] * input_dimension for _ in range(hidden)],
        "hidden_bias": [0.0] * hidden,
        "time_weights": [0.0] * hidden,
        "time_bias": 0.0,
        "mark_weights": [[0.0] * hidden for _ in range(mark_count)],
        "mark_bias": [0.0] * mark_count,
    }


def _loss_metrics_and_gradients(
    parameters: Mapping[str, JSONValue],
    sequences: Sequence[_TrainingSequence],
    config: NeuralTPPConfigV1,
    duration_mean: float,
    duration_scale: float,
    mark_count: int,
    *,
    gradients_required: bool,
    deadline: float,
) -> tuple[dict[str, float], dict[str, Any] | None]:
    hidden_dimension = config.hidden_dimension
    input_dimension = mark_count + 2
    gradients = (
        _empty_gradients(hidden_dimension, input_dimension, mark_count)
        if gradients_required
        else None
    )
    time_nll = 0.0
    mark_nll = 0.0
    squared_log_error = 0.0
    correct_marks = 0
    pit_sum = 0.0
    event_count = 0
    for sequence in sequences:
        if time.perf_counter() > deadline:
            raise NeuralTPPFitError("neural TPP fit wall-time limit exceeded")
        initial_hidden = list(
            cast(Sequence[float], parameters["initial_hidden"])
        )
        start_inputs = _start_input_vector(mark_count)
        states: list[list[float]] = [
            _hidden_step(parameters, initial_hidden, start_inputs)
        ]
        inputs_by_event: list[list[float]] = []
        state_gradients = [
            [0.0] * hidden_dimension for _ in range(len(sequence.marks) + 1)
        ]
        for index, (duration, mark) in enumerate(
            zip(sequence.durations_seconds, sequence.marks)
        ):
            hidden = states[index]
            base = _time_base(parameters, hidden)
            integral = _time_integral(
                base, config.elapsed_slope_per_second, duration
            )
            event_time_nll = (
                -base - config.elapsed_slope_per_second * duration + integral
            )
            probabilities = _mark_probabilities(parameters, hidden)
            event_mark_nll = -math.log(max(probabilities[mark], 1e-300))
            time_nll += event_time_nll
            mark_nll += event_mark_nll
            target_integral = math.log(2.0)
            median = (
                math.log1p(
                    config.elapsed_slope_per_second
                    * target_integral
                    / math.exp(base)
                )
                / config.elapsed_slope_per_second
            )
            squared_log_error += (
                math.log1p(median) - math.log1p(duration)
            ) ** 2
            pit = 1.0 - math.exp(-integral)
            pit_sum += pit
            correct_marks += int(
                max(range(mark_count), key=probabilities.__getitem__) == mark
            )
            event_count += 1
            if gradients is not None:
                time_derivative = -1.0 + integral
                time_weights = cast(Sequence[float], parameters["time_weights"])
                for row in range(hidden_dimension):
                    gradients["time_weights"][row] += (
                        time_derivative * hidden[row]
                    )
                    state_gradients[index][row] += (
                        time_derivative * time_weights[row]
                    )
                gradients["time_bias"] += time_derivative
                mark_weights = cast(
                    Sequence[Sequence[float]], parameters["mark_weights"]
                )
                for mark_index, probability in enumerate(probabilities):
                    derivative = probability - float(mark_index == mark)
                    gradients["mark_bias"][mark_index] += derivative
                    for row in range(hidden_dimension):
                        gradients["mark_weights"][mark_index][row] += (
                            derivative * hidden[row]
                        )
                        state_gradients[index][row] += (
                            derivative * mark_weights[mark_index][row]
                        )
            input_values = _input_vector(
                mark,
                duration,
                mark_count,
                duration_mean,
                duration_scale,
            )
            inputs_by_event.append(input_values)
            states.append(_hidden_step(parameters, hidden, input_values))
        remaining = sequence.censor_seconds
        final_hidden = states[-1]
        final_base = _time_base(parameters, final_hidden)
        censor_integral = _time_integral(
            final_base, config.elapsed_slope_per_second, remaining
        )
        time_nll += censor_integral
        if gradients is not None:
            time_weights = cast(Sequence[float], parameters["time_weights"])
            for row in range(hidden_dimension):
                gradients["time_weights"][row] += (
                    censor_integral * final_hidden[row]
                )
                state_gradients[-1][row] += censor_integral * time_weights[row]
            gradients["time_bias"] += censor_integral
            recurrent = cast(
                Sequence[Sequence[float]], parameters["recurrent_weights"]
            )
            for index in range(len(sequence.marks) - 1, -1, -1):
                current = states[index + 1]
                previous = states[index]
                input_values = inputs_by_event[index]
                pre_gradient = [
                    state_gradients[index + 1][row] * (1.0 - current[row] ** 2)
                    for row in range(hidden_dimension)
                ]
                for row in range(hidden_dimension):
                    value = pre_gradient[row]
                    gradients["hidden_bias"][row] += value
                    for column in range(hidden_dimension):
                        gradients["recurrent_weights"][row][column] += (
                            value * previous[column]
                        )
                        state_gradients[index][column] += (
                            value * recurrent[row][column]
                        )
                    for column in range(input_dimension):
                        gradients["input_weights"][row][column] += (
                            value * input_values[column]
                        )
            start_state = states[0]
            start_pre_gradient = [
                state_gradients[0][row] * (1.0 - start_state[row] ** 2)
                for row in range(hidden_dimension)
            ]
            for row in range(hidden_dimension):
                value = start_pre_gradient[row]
                gradients["hidden_bias"][row] += value
                for column in range(hidden_dimension):
                    gradients["recurrent_weights"][row][column] += (
                        value * initial_hidden[column]
                    )
                    gradients["initial_hidden"][column] += (
                        value * recurrent[row][column]
                    )
                for column in range(input_dimension):
                    gradients["input_weights"][row][column] += (
                        value * start_inputs[column]
                    )
    if event_count <= 0:
        raise NeuralTPPFitError("neural TPP split contains no events")
    metrics = {
        "negative_log_likelihood": (time_nll + mark_nll) / event_count,
        "time_negative_log_likelihood": time_nll / event_count,
        "mark_negative_log_likelihood": mark_nll / event_count,
        "mark_accuracy": correct_marks / event_count,
        "log_duration_rmse": math.sqrt(squared_log_error / event_count),
        "mean_pit": pit_sum / event_count,
        "event_count": float(event_count),
    }
    if any(not math.isfinite(value) for value in metrics.values()):
        raise NeuralTPPFitError("neural TPP loss metric is non-finite")
    if gradients is not None:
        scale = 1.0 / event_count
        _scale_nested_gradients(gradients, scale)
    return metrics, gradients


def _scale_nested_gradients(gradients: dict[str, Any], scale: float) -> None:
    for name in PARAMETER_NAMES:
        value = gradients[name]
        if isinstance(value, list):
            for row_index, row in enumerate(value):
                if isinstance(row, list):
                    value[row_index] = [item * scale for item in row]
                else:
                    value[row_index] = row * scale
        else:
            gradients[name] = value * scale


def _gradient_values(gradients: Mapping[str, Any]) -> Iterator[float]:
    for name in PARAMETER_NAMES:
        value = gradients[name]
        if isinstance(value, list):
            for row in value:
                if isinstance(row, list):
                    yield from row
                else:
                    yield row
        else:
            yield value


def _apply_gradients(
    parameters: dict[str, JSONValue],
    gradients: Mapping[str, Any],
    config: NeuralTPPConfigV1,
) -> tuple[float, bool]:
    norm = math.sqrt(
        sum(value * value for value in _gradient_values(gradients))
    )
    if not math.isfinite(norm):
        raise NeuralTPPFitError("neural TPP gradient norm is non-finite")
    clipped = norm > config.gradient_clip_norm
    scale = min(1.0, config.gradient_clip_norm / max(norm, 1e-300))
    learning_scale = config.learning_rate * scale
    limit = config.parameter_clip_absolute
    for name in PARAMETER_NAMES:
        value = parameters[name]
        gradient = gradients[name]
        if isinstance(value, list):
            updated: list[Any] = []
            for row_index, row in enumerate(value):
                if isinstance(row, list):
                    updated.append(
                        [
                            max(
                                -limit,
                                min(
                                    limit,
                                    _finite_float(item, name)
                                    - learning_scale
                                    * gradient[row_index][column],
                                ),
                            )
                            for column, item in enumerate(row)
                        ]
                    )
                else:
                    updated.append(
                        max(
                            -limit,
                            min(
                                limit,
                                _finite_float(row, name)
                                - learning_scale * gradient[row_index],
                            ),
                        )
                    )
            parameters[name] = cast(JSONValue, updated)
        else:
            parameters[name] = max(
                -limit,
                min(
                    limit,
                    _finite_float(value, name)
                    - learning_scale * float(gradient),
                ),
            )
    return norm, clipped


def _train_model(
    config: NeuralTPPConfigV1,
    dataset: NeuralTPPDatasetManifestV1,
    train: Sequence[_TrainingSequence],
    tune: Sequence[_TrainingSequence],
    duration_mean: float,
    duration_scale: float,
    *,
    deadline: float,
    started: float,
    before_peak: int,
) -> tuple[NeuralTPPCheckpointV1, NeuralTPPTrainingManifestV1]:
    mark_count = len(dataset.mark_vocabulary)
    input_dimension = mark_count + 2
    parameters = _initialize_parameters(
        config, train, input_dimension, mark_count
    )
    parameter_count = _parameter_count(parameters)
    if parameter_count > config.limits.max_parameter_count:
        raise NeuralTPPFitError("neural TPP parameter count exceeds limit")
    initial_train, _ = _loss_metrics_and_gradients(
        parameters,
        train,
        config,
        duration_mean,
        duration_scale,
        mark_count,
        gradients_required=False,
        deadline=deadline,
    )
    initial_tune, _ = _loss_metrics_and_gradients(
        parameters,
        tune,
        config,
        duration_mean,
        duration_scale,
        mark_count,
        gradients_required=False,
        deadline=deadline,
    )
    trace: list[dict[str, JSONScalar]] = [
        _trace_row(0, initial_train, initial_tune, 0.0)
    ]
    best_parameters = copy.deepcopy(parameters)
    best_train = initial_train
    best_tune = initial_tune
    best_epoch = 0
    patience = 0
    clipped_count = 0
    maximum_gradient_norm = 0.0
    completed_epochs = 0
    for epoch in range(1, config.max_epochs + 1):
        if time.perf_counter() > deadline:
            raise NeuralTPPFitError("neural TPP fit wall-time limit exceeded")
        train_before, gradients = _loss_metrics_and_gradients(
            parameters,
            train,
            config,
            duration_mean,
            duration_scale,
            mark_count,
            gradients_required=True,
            deadline=deadline,
        )
        if gradients is None:
            raise NeuralTPPFitError("neural TPP training lacks gradients")
        gradient_norm, clipped = _apply_gradients(parameters, gradients, config)
        maximum_gradient_norm = max(maximum_gradient_norm, gradient_norm)
        clipped_count += int(clipped)
        train_after, _ = _loss_metrics_and_gradients(
            parameters,
            train,
            config,
            duration_mean,
            duration_scale,
            mark_count,
            gradients_required=False,
            deadline=deadline,
        )
        tune_after, _ = _loss_metrics_and_gradients(
            parameters,
            tune,
            config,
            duration_mean,
            duration_scale,
            mark_count,
            gradients_required=False,
            deadline=deadline,
        )
        if train_after["negative_log_likelihood"] > (
            train_before["negative_log_likelihood"] + 100.0
        ):
            raise NeuralTPPFitError("neural TPP training loss diverged")
        trace.append(_trace_row(epoch, train_after, tune_after, gradient_norm))
        completed_epochs = epoch
        if tune_after["negative_log_likelihood"] < (
            best_tune["negative_log_likelihood"]
            - config.early_stopping_min_delta
        ):
            best_parameters = copy.deepcopy(parameters)
            best_train = train_after
            best_tune = tune_after
            best_epoch = epoch
            patience = 0
        else:
            patience += 1
        if patience >= config.early_stopping_patience:
            break
    if best_epoch <= 0:
        raise NeuralTPPFitError(
            "neural TPP training found no improving checkpoint"
        )
    parameter_bytes = len(canonical_contract_json(best_parameters).encode())
    if parameter_bytes > config.limits.max_parameter_bytes:
        raise NeuralTPPFitError("neural TPP parameter payload exceeds limit")
    checkpoint = NeuralTPPCheckpointV1(
        config_id=config.config_id,
        dataset_id=dataset.dataset_id,
        architecture=config.architecture,
        hidden_dimension=config.hidden_dimension,
        input_dimension=input_dimension,
        mark_vocabulary=dataset.mark_vocabulary,
        input_vocabulary=(
            *dataset.mark_vocabulary,
            "<START>",
            "<LOG_DURATION>",
        ),
        duration_log_mean=duration_mean,
        duration_log_scale=duration_scale,
        selected_epoch=best_epoch,
        train_negative_log_likelihood=best_train["negative_log_likelihood"],
        tune_negative_log_likelihood=best_tune["negative_log_likelihood"],
        train_time_negative_log_likelihood=best_train[
            "time_negative_log_likelihood"
        ],
        train_mark_negative_log_likelihood=best_train[
            "mark_negative_log_likelihood"
        ],
        tune_time_negative_log_likelihood=best_tune[
            "time_negative_log_likelihood"
        ],
        tune_mark_negative_log_likelihood=best_tune[
            "mark_negative_log_likelihood"
        ],
        tune_mark_accuracy=best_tune["mark_accuracy"],
        tune_log_duration_rmse=best_tune["log_duration_rmse"],
        tune_mean_pit=best_tune["mean_pit"],
        parameters=best_parameters,
        parameter_count=parameter_count,
        parameter_bytes=parameter_bytes,
    )
    checkpoint_bytes = len(checkpoint.to_json().encode())
    if checkpoint_bytes > config.limits.max_checkpoint_bytes:
        raise NeuralTPPFitError("neural TPP checkpoint exceeds byte limit")
    gradient_work = (
        completed_epochs
        * sum(len(item.marks) for item in train)
        * parameter_count
    )
    if gradient_work > config.limits.max_gradient_work:
        raise NeuralTPPFitError("neural TPP gradient work exceeds limit")
    manifest = NeuralTPPTrainingManifestV1(
        config_id=config.config_id,
        dataset_id=dataset.dataset_id,
        checkpoint_id=checkpoint.checkpoint_id,
        initialization_seed=config.initialization_seed,
        selected_epoch=best_epoch,
        completed_epoch_count=completed_epochs,
        early_stopped=completed_epochs < config.max_epochs,
        loss_trace=tuple(trace),
        maximum_gradient_norm=maximum_gradient_norm,
        clipped_epoch_count=clipped_count,
        gradient_work=gradient_work,
        runtime_metadata=_runtime_metadata(),
        wall_time_ms=round((time.perf_counter() - started) * 1000),
        peak_memory_bytes=max(0, peak_rss_bytes() - before_peak),
    )
    return checkpoint, manifest


def _trace_row(
    epoch: int,
    train: Mapping[str, float],
    tune: Mapping[str, float],
    gradient_norm: float,
) -> dict[str, JSONScalar]:
    return {
        "epoch": epoch,
        "train_negative_log_likelihood": train["negative_log_likelihood"],
        "tune_negative_log_likelihood": tune["negative_log_likelihood"],
        "train_time_negative_log_likelihood": train[
            "time_negative_log_likelihood"
        ],
        "train_mark_negative_log_likelihood": train[
            "mark_negative_log_likelihood"
        ],
        "tune_time_negative_log_likelihood": tune[
            "time_negative_log_likelihood"
        ],
        "tune_mark_negative_log_likelihood": tune[
            "mark_negative_log_likelihood"
        ],
        "tune_mark_accuracy": tune["mark_accuracy"],
        "tune_log_duration_rmse": tune["log_duration_rmse"],
        "tune_mean_pit": tune["mean_pit"],
        "gradient_norm": gradient_norm,
    }


def fit_neural_tpp_challenger(
    config: NeuralTPPConfigV1,
    windows: Sequence[EventClockCalibrationWindowV1],
    *,
    window_contexts: Sequence[NeuralTPPWindowContextV1],
    protected_windows: Sequence[NeuralTPPProtectedWindowV1] = (),
    split_assignments: Mapping[str, str] | None = None,
    information_mode: InformationMode = InformationMode.EX_POST_RECONSTRUCTION,
    as_of_ns: int | None = None,
) -> NeuralTPPFitResultV1:
    """Fit the fixed bounded RMTPP or return a complete closed result."""
    if not isinstance(config, NeuralTPPConfigV1):
        raise TypeError("neural TPP fit requires a v1 config")
    mode = InformationMode.from_value(information_mode)
    started = time.perf_counter()
    before_peak = peak_rss_bytes()
    deadline = started + config.limits.max_fit_wall_time_ms / 1000
    calibration_windows = tuple(windows)
    contexts = tuple(window_contexts)
    protected = tuple(protected_windows)
    dataset_hash = _safe_content_digest(
        {
            "windows": [
                {
                    "window_id": getattr(item, "window_id", None),
                    "start_ns": getattr(item, "start_ns", None),
                    "end_ns": getattr(item, "end_ns", None),
                    "events": [
                        event.to_dict()
                        for event in getattr(item, "events", ())
                        if isinstance(event, BenchmarkEventV1)
                    ],
                }
                for item in calibration_windows
            ],
            "protected": [
                item.to_dict()
                for item in protected
                if isinstance(item, NeuralTPPProtectedWindowV1)
            ],
            "split_assignments": (
                dict(split_assignments)
                if split_assignments is not None
                else None
            ),
        }
    )
    context_hash = _safe_content_digest(
        {
            "contexts": [
                item.to_dict()
                for item in contexts
                if isinstance(item, NeuralTPPWindowContextV1)
            ]
        }
    )
    symbols: tuple[str, ...] = ("UNKNOWN",)
    training_event_count = 0
    tuning_event_count = 0
    training_window_count = 0
    tuning_window_count = 0
    estimated_memory = 0
    try:
        if mode is InformationMode.EX_ANTE_SIMULATION:
            if as_of_ns is None:
                raise NeuralTPPFitError("ex-ante neural fit requires as_of_ns")
            as_of = _strict_int(as_of_ns, "as_of_ns")
            if as_of < 0:
                raise NeuralTPPFitError("as_of_ns must be nonnegative")
        elif as_of_ns is not None:
            raise NeuralTPPFitError("ex-post neural fit forbids as_of_ns")
        if not calibration_windows or len(calibration_windows) < 2:
            raise NeuralTPPFitError(
                "neural TPP fit requires at least two windows"
            )
        if len(calibration_windows) > config.limits.max_fit_windows:
            return _closed_fit_result(
                config,
                dataset_hash,
                context_hash,
                mode,
                as_of_ns,
                symbols,
                NeuralTPPFitStatus.REFUSED,
                training_event_count,
                tuning_event_count,
                training_window_count,
                tuning_window_count,
                estimated_memory,
                "fit_refused:window_limit_exceeded",
            )
        if any(
            not isinstance(item, EventClockCalibrationWindowV1)
            for item in calibration_windows
        ):
            raise NeuralTPPFitError("neural TPP received an invalid window")
        if any(
            not isinstance(item, NeuralTPPWindowContextV1) for item in contexts
        ) or any(
            not isinstance(item, NeuralTPPProtectedWindowV1)
            for item in protected
        ):
            raise NeuralTPPFitError(
                "neural TPP received invalid context evidence"
            )
        event_count = sum(len(item.events) for item in calibration_windows)
        if event_count > config.limits.max_fit_events:
            return _closed_fit_result(
                config,
                dataset_hash,
                context_hash,
                mode,
                as_of_ns,
                symbols,
                NeuralTPPFitStatus.REFUSED,
                0,
                0,
                0,
                0,
                0,
                "fit_refused:event_limit_exceeded",
            )
        if any(
            len(item.events) > config.limits.max_sequence_events
            for item in calibration_windows
        ):
            raise NeuralTPPFitError("neural TPP sequence limit exceeded")
        symbols = tuple(
            sorted(
                {
                    _symbol(event.symbol)
                    for window in calibration_windows
                    for event in window.events
                }
            )
        )
        if len(_mark_vocabulary(symbols)) > config.limits.max_mark_count:
            raise NeuralTPPFitError("neural TPP mark vocabulary exceeds limit")
        if (
            len(_mark_vocabulary(symbols)) + 2
            > config.limits.max_input_dimension
        ):
            raise NeuralTPPFitError(
                "neural TPP input feature count exceeds limit"
            )
        if mode is InformationMode.EX_ANTE_SIMULATION:
            boundary = cast(int, as_of_ns)
            if any(
                event.event_time_ns > boundary
                for window in calibration_windows
                for event in window.events
            ) or any(
                context.use_time_ns is not None
                and context.use_time_ns > boundary
                for context in contexts
            ):
                return _closed_fit_result(
                    config,
                    dataset_hash,
                    context_hash,
                    mode,
                    as_of_ns,
                    symbols,
                    NeuralTPPFitStatus.REFUSED,
                    0,
                    0,
                    0,
                    0,
                    0,
                    "fit_refused:point_in_time_boundary_exceeded",
                )
        estimated_memory = _estimated_fit_memory(
            event_count,
            config.hidden_dimension,
            len(_mark_vocabulary(symbols)),
        )
        if estimated_memory > config.limits.max_fit_memory_bytes:
            return _closed_fit_result(
                config,
                dataset_hash,
                context_hash,
                mode,
                as_of_ns,
                symbols,
                NeuralTPPFitStatus.REFUSED,
                0,
                0,
                0,
                0,
                estimated_memory,
                "fit_refused:memory_preflight_exceeded",
            )
        dataset, train, tune, duration_mean, duration_scale = _build_dataset(
            config,
            calibration_windows,
            contexts,
            protected,
            split_assignments,
            symbols,
        )
        training_event_count = sum(len(item.marks) for item in train)
        tuning_event_count = sum(len(item.marks) for item in tune)
        training_window_count = len(train)
        tuning_window_count = len(tune)
        checkpoint, training = _train_model(
            config,
            dataset,
            train,
            tune,
            duration_mean,
            duration_scale,
            deadline=deadline,
            started=started,
            before_peak=before_peak,
        )
        if training.peak_memory_bytes > config.limits.max_fit_memory_bytes:
            raise NeuralTPPFitError(
                "neural TPP measured fit memory exceeds limit"
            )
        diagnostics: dict[str, JSONScalar] = {
            "architecture": config.architecture,
            "accelerator_policy": config.accelerator_policy,
            "accelerator_count": 0,
            "mark_count": len(dataset.mark_vocabulary),
            "parameter_count": checkpoint.parameter_count,
            "parameter_bytes": checkpoint.parameter_bytes,
            "checkpoint_bytes": len(checkpoint.to_json().encode()),
            "dataset_manifest_bytes": len(dataset.to_json().encode()),
            "training_manifest_bytes": len(
                canonical_contract_json(
                    {
                        **training.identity_payload(),
                        "training_id": training.training_id,
                    }
                ).encode()
            ),
            "exact_duplicate_count": dataset.exact_duplicate_count,
            "near_duplicate_collision_count": (
                dataset.near_duplicate_collision_count
            ),
            "overlap_count": dataset.overlap_count,
            "protected_window_count": sum(
                count
                for role, count in dataset.window_count_by_role.items()
                if role in PROTECTED_ROLES
            ),
            "selected_epoch": checkpoint.selected_epoch,
            "completed_epoch_count": training.completed_epoch_count,
            "early_stopped": training.early_stopped,
            "maximum_gradient_norm": training.maximum_gradient_norm,
            "clipped_epoch_count": training.clipped_epoch_count,
            "gradient_work": training.gradient_work,
            "tune_mark_accuracy": checkpoint.tune_mark_accuracy,
            "tune_log_duration_rmse": checkpoint.tune_log_duration_rmse,
            "train_time_negative_log_likelihood": (
                checkpoint.train_time_negative_log_likelihood
            ),
            "train_mark_negative_log_likelihood": (
                checkpoint.train_mark_negative_log_likelihood
            ),
            "tune_time_negative_log_likelihood": (
                checkpoint.tune_time_negative_log_likelihood
            ),
            "tune_mark_negative_log_likelihood": (
                checkpoint.tune_mark_negative_log_likelihood
            ),
            "tune_mean_pit": checkpoint.tune_mean_pit,
            "technology_transition_window_count": sum(
                item.technology_assignment_kind == "transition"
                for item in contexts
            ),
            "diagnostic_bytes": 0,
        }
        for _ in range(8):
            measured = len(canonical_contract_json(diagnostics).encode())
            if diagnostics["diagnostic_bytes"] == measured:
                break
            diagnostics["diagnostic_bytes"] = measured
        else:
            raise NeuralTPPFitError("neural diagnostic byte accounting failed")
        if measured > config.limits.max_diagnostic_bytes:
            raise NeuralTPPFitError("neural TPP diagnostics exceed byte limit")
        result = NeuralTPPFitResultV1(
            config_id=config.config_id,
            dataset_content_sha256=dataset_hash,
            context_content_sha256=context_hash,
            information_mode=mode,
            as_of_ns=as_of_ns,
            symbols=symbols,
            status=NeuralTPPFitStatus.FITTED,
            converged=True,
            training_event_count=training_event_count,
            tuning_event_count=tuning_event_count,
            training_window_count=training_window_count,
            tuning_window_count=tuning_window_count,
            selected_epoch=checkpoint.selected_epoch,
            train_negative_log_likelihood=(
                checkpoint.train_negative_log_likelihood
            ),
            tune_negative_log_likelihood=(
                checkpoint.tune_negative_log_likelihood
            ),
            dataset_manifest=dataset,
            training_manifest=training,
            checkpoint=checkpoint,
            diagnostics=diagnostics,
            estimated_peak_memory_bytes=estimated_memory,
        )
        _validate_fit_against_config(config, result)
        return result
    except (
        ArithmeticError,
        KeyError,
        NeuralTPPFitError,
        TypeError,
        ValueError,
    ) as err:
        return _closed_fit_result(
            config,
            dataset_hash,
            context_hash,
            mode,
            as_of_ns,
            symbols,
            NeuralTPPFitStatus.FAILED,
            training_event_count,
            tuning_event_count,
            training_window_count,
            tuning_window_count,
            estimated_memory,
            f"fit_failed:{type(err).__name__}:{err}",
        )


def _safe_content_digest(payload: Mapping[str, Any]) -> str:
    try:
        encoded = canonical_contract_json(payload)
    except (TypeError, ValueError):
        encoded = canonical_contract_json({"invalid_payload": True})
    return hashlib.sha256(encoded.encode()).hexdigest()


def _closed_fit_result(
    config: NeuralTPPConfigV1,
    dataset_hash: str,
    context_hash: str,
    mode: InformationMode,
    as_of_ns: int | None,
    symbols: tuple[str, ...],
    status: NeuralTPPFitStatus,
    training_event_count: int,
    tuning_event_count: int,
    training_window_count: int,
    tuning_window_count: int,
    estimated_memory: int,
    reason: str,
) -> NeuralTPPFitResultV1:
    if status is NeuralTPPFitStatus.FITTED:
        raise ValueError("closed neural fit cannot be fitted")
    safe_mode = InformationMode.from_value(mode)
    safe_as_of = as_of_ns
    if safe_mode is InformationMode.EX_ANTE_SIMULATION:
        if (
            not isinstance(safe_as_of, int)
            or isinstance(safe_as_of, bool)
            or safe_as_of < 0
        ):
            safe_as_of = 0
    else:
        safe_as_of = None
    safe_symbols = tuple(
        value for value in symbols if isinstance(value, str) and value != ""
    ) or ("UNKNOWN",)
    return NeuralTPPFitResultV1(
        config_id=config.config_id,
        dataset_content_sha256=dataset_hash,
        context_content_sha256=context_hash,
        information_mode=safe_mode,
        as_of_ns=safe_as_of,
        symbols=safe_symbols,
        status=status,
        converged=False,
        training_event_count=max(0, training_event_count),
        tuning_event_count=max(0, tuning_event_count),
        training_window_count=max(0, training_window_count),
        tuning_window_count=max(0, tuning_window_count),
        selected_epoch=0,
        train_negative_log_likelihood=None,
        tune_negative_log_likelihood=None,
        dataset_manifest=None,
        training_manifest=None,
        checkpoint=None,
        diagnostics={},
        estimated_peak_memory_bytes=max(0, estimated_memory),
        failure_reason=reason,
    )


def _estimated_fit_memory(event_count: int, hidden: int, marks: int) -> int:
    parameter_count = hidden * hidden + hidden * (marks + 2) + marks * hidden
    return (
        1024 * 1024
        + event_count * (hidden + marks + 8) * 32
        + parameter_count * 64
    )


def _validate_fit_against_config(
    config: NeuralTPPConfigV1, fit: NeuralTPPFitResultV1
) -> None:
    if fit.config_id != config.config_id:
        raise ValueError("neural TPP fit/config identity differs")
    if fit.status is not NeuralTPPFitStatus.FITTED:
        return
    if (
        fit.checkpoint is None
        or fit.dataset_manifest is None
        or fit.training_manifest is None
    ):
        raise ValueError("neural TPP fitted artifacts are absent")
    if (
        fit.dataset_manifest.split_policy != config.split_policy
        or fit.dataset_manifest.ordering_policy != config.ordering_policy
        or fit.dataset_manifest.mark_policy != config.mark_policy
        or fit.dataset_manifest.duration_transform != config.duration_transform
        or fit.dataset_manifest.start_token_policy != config.start_token_policy
        or fit.dataset_manifest.prior_quote_reset_policy
        != config.prior_quote_reset_policy
    ):
        raise ValueError("neural TPP dataset preprocessing policy differs")
    restored = NeuralTPPCheckpointV1.from_json(fit.checkpoint.to_json())
    if restored != fit.checkpoint:
        raise ValueError("neural TPP checkpoint round trip differs")
    if fit.checkpoint.hidden_dimension != config.hidden_dimension:
        raise ValueError("neural TPP checkpoint hidden dimension differs")
    if fit.checkpoint.input_dimension > config.limits.max_input_dimension:
        raise ValueError("neural TPP checkpoint input feature limit differs")
    if fit.checkpoint.parameter_count > config.limits.max_parameter_count:
        raise ValueError("neural TPP checkpoint parameter limit differs")
    if fit.checkpoint.parameter_bytes > config.limits.max_parameter_bytes:
        raise ValueError("neural TPP checkpoint parameter bytes exceed config")
    if (
        len(fit.checkpoint.to_json().encode())
        > config.limits.max_checkpoint_bytes
    ):
        raise ValueError("neural TPP checkpoint bytes exceed config")
    if fit.training_manifest.gradient_work > config.limits.max_gradient_work:
        raise ValueError("neural TPP training work exceeds config")


@dataclass(frozen=True, slots=True)
class NeuralTPPGenerationLineageV1:
    """Exact sampled-time, joint-mark, state, and anchor lineage."""

    source_event_id: str
    step_index: int
    destination_symbol: str
    mark_label: str
    mark_probability: float
    elapsed_seconds: float
    conditional_intensity: float
    log_joint_density: float
    hidden_state_sha256: str
    anchor_interval_id: str
    parent_event_id: str | None
    lineage_id: str = ""
    schema_version: str = NEURAL_TPP_GENERATION_LINEAGE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            NEURAL_TPP_GENERATION_LINEAGE_SCHEMA_VERSION,
            "neural TPP generation lineage",
        )
        object.__setattr__(
            self, "source_event_id", _required_text(self.source_event_id)
        )
        if _strict_int(self.step_index, "step_index") <= 0:
            raise ValueError("neural TPP lineage step must be positive")
        object.__setattr__(
            self, "destination_symbol", _symbol(self.destination_symbol)
        )
        mark = _required_text(self.mark_label)
        if mark not in MARK_STATES:
            raise ValueError("neural TPP lineage mark is invalid")
        probability = _positive_float(self.mark_probability, "mark_probability")
        if probability > 1.0:
            raise ValueError("neural TPP mark probability exceeds one")
        _positive_float(self.elapsed_seconds, "elapsed_seconds")
        _positive_float(self.conditional_intensity, "conditional_intensity")
        _finite_float(self.log_joint_density, "log_joint_density")
        object.__setattr__(
            self,
            "hidden_state_sha256",
            _sha256(self.hidden_state_sha256, "hidden_state_sha256"),
        )
        object.__setattr__(
            self, "anchor_interval_id", _required_text(self.anchor_interval_id)
        )
        object.__setattr__(
            self, "parent_event_id", _optional_text(self.parent_event_id)
        )
        expected = _stable_id(
            "neural-tpp-generation-lineage", self.identity_payload()
        )
        if self.lineage_id and self.lineage_id != expected:
            raise ValueError("neural TPP generation lineage_id differs")
        object.__setattr__(self, "lineage_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "source_event_id": self.source_event_id,
            "step_index": self.step_index,
            "destination_symbol": self.destination_symbol,
            "mark_label": self.mark_label,
            "mark_probability": self.mark_probability,
            "elapsed_seconds": self.elapsed_seconds,
            "conditional_intensity": self.conditional_intensity,
            "log_joint_density": self.log_joint_density,
            "hidden_state_sha256": self.hidden_state_sha256,
            "anchor_interval_id": self.anchor_interval_id,
            "parent_event_id": self.parent_event_id,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "lineage_id": self.lineage_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> NeuralTPPGenerationLineageV1:
        return cls(
            source_event_id=str(data.get("source_event_id", "")),
            step_index=_strict_int(data.get("step_index"), "step_index"),
            destination_symbol=str(data.get("destination_symbol", "")),
            mark_label=str(data.get("mark_label", "")),
            mark_probability=_finite_float(
                data.get("mark_probability"), "mark_probability"
            ),
            elapsed_seconds=_finite_float(
                data.get("elapsed_seconds"), "elapsed_seconds"
            ),
            conditional_intensity=_finite_float(
                data.get("conditional_intensity"), "conditional_intensity"
            ),
            log_joint_density=_finite_float(
                data.get("log_joint_density"), "log_joint_density"
            ),
            hidden_state_sha256=str(data.get("hidden_state_sha256", "")),
            anchor_interval_id=str(data.get("anchor_interval_id", "")),
            parent_event_id=_optional_text(data.get("parent_event_id")),
            lineage_id=str(data.get("lineage_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> NeuralTPPGenerationLineageV1:
        return cls.from_dict(_json_mapping(text, 1024 * 1024))


@dataclass(frozen=True, slots=True)
class NeuralTPPGenerationEvidenceV1:
    """Complete all-or-nothing neural generation evidence."""

    config_id: str
    fit_id: str
    dataset_id: str
    training_id: str
    checkpoint_id: str
    window_id: str
    window_context_id: str | None
    ensemble_member_id: str
    status: NeuralTPPGenerationStatus
    attempted: bool
    input_event_count: int
    history_event_count: int
    generated_event_count: int
    processed_step_count: int
    skipped_unsupported_count: int
    semantic_seed: int | None
    input_anchor_sha256: str | None
    input_event_content_sha256: str | None
    history_content_sha256: str | None
    window_context_sha256: str | None
    lineage_content_sha256: str | None
    parameter_bytes: int
    wall_time_ms: int
    peak_memory_bytes: int
    failure_reason: str | None = None
    evidence_id: str = ""
    schema_version: str = NEURAL_TPP_GENERATION_EVIDENCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            NEURAL_TPP_GENERATION_EVIDENCE_SCHEMA_VERSION,
            "neural TPP generation evidence",
        )
        for name in (
            "config_id",
            "fit_id",
            "dataset_id",
            "training_id",
            "checkpoint_id",
            "window_id",
            "ensemble_member_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self, "window_context_id", _optional_text(self.window_context_id)
        )
        status = NeuralTPPGenerationStatus(self.status)
        object.__setattr__(self, "status", status)
        if not _strict_bool(self.attempted, "attempted"):
            raise ValueError("neural TPP evidence must represent an attempt")
        for name in (
            "input_event_count",
            "history_event_count",
            "generated_event_count",
            "processed_step_count",
            "skipped_unsupported_count",
            "parameter_bytes",
            "wall_time_ms",
            "peak_memory_bytes",
        ):
            if _strict_int(getattr(self, name), name) < 0:
                raise ValueError(f"{name} must be nonnegative")
        if self.semantic_seed is not None:
            _bounded_int(
                self.semantic_seed,
                "semantic_seed",
                0,
                2**64 - 1,
            )
        for name in (
            "input_anchor_sha256",
            "input_event_content_sha256",
            "history_content_sha256",
            "window_context_sha256",
            "lineage_content_sha256",
        ):
            object.__setattr__(
                self,
                name,
                _optional_sha256(getattr(self, name), name),
            )
        failure = _optional_text(self.failure_reason)
        object.__setattr__(self, "failure_reason", failure)
        successful = status in {
            NeuralTPPGenerationStatus.GENERATED,
            NeuralTPPGenerationStatus.EMPTY,
        }
        required = (
            self.window_context_id,
            self.semantic_seed,
            self.input_anchor_sha256,
            self.input_event_content_sha256,
            self.history_content_sha256,
            self.window_context_sha256,
            self.lineage_content_sha256,
        )
        if successful:
            if failure is not None or any(value is None for value in required):
                raise ValueError(
                    "successful neural generation lacks audit evidence"
                )
            if self.input_event_count <= 0 or self.processed_step_count <= 0:
                raise ValueError(
                    "successful neural generation lacks bounded work"
                )
        elif failure is None:
            raise ValueError("closed neural generation requires a reason")
        elif (
            self.generated_event_count != 0
            or self.lineage_content_sha256 is not None
        ):
            raise ValueError("closed neural generation exposes partial output")
        if (
            status is NeuralTPPGenerationStatus.GENERATED
            and self.generated_event_count <= 0
        ):
            raise ValueError("generated neural evidence has no generated rows")
        if (
            status is NeuralTPPGenerationStatus.EMPTY
            and self.generated_event_count != 0
        ):
            raise ValueError("empty neural evidence contains generated rows")
        expected = _stable_id(
            "neural-tpp-generation-evidence", self.identity_payload()
        )
        if self.evidence_id and self.evidence_id != expected:
            raise ValueError("neural TPP generation evidence_id differs")
        object.__setattr__(self, "evidence_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "config_id": self.config_id,
            "fit_id": self.fit_id,
            "dataset_id": self.dataset_id,
            "training_id": self.training_id,
            "checkpoint_id": self.checkpoint_id,
            "window_id": self.window_id,
            "window_context_id": self.window_context_id,
            "ensemble_member_id": self.ensemble_member_id,
            "status": self.status.value,
            "attempted": self.attempted,
            "input_event_count": self.input_event_count,
            "history_event_count": self.history_event_count,
            "generated_event_count": self.generated_event_count,
            "processed_step_count": self.processed_step_count,
            "skipped_unsupported_count": self.skipped_unsupported_count,
            "semantic_seed": self.semantic_seed,
            "input_anchor_sha256": self.input_anchor_sha256,
            "input_event_content_sha256": self.input_event_content_sha256,
            "history_content_sha256": self.history_content_sha256,
            "window_context_sha256": self.window_context_sha256,
            "lineage_content_sha256": self.lineage_content_sha256,
            "parameter_bytes": self.parameter_bytes,
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
    ) -> NeuralTPPGenerationEvidenceV1:
        return cls(
            config_id=str(data.get("config_id", "")),
            fit_id=str(data.get("fit_id", "")),
            dataset_id=str(data.get("dataset_id", "")),
            training_id=str(data.get("training_id", "")),
            checkpoint_id=str(data.get("checkpoint_id", "")),
            window_id=str(data.get("window_id", "")),
            window_context_id=_optional_text(data.get("window_context_id")),
            ensemble_member_id=str(data.get("ensemble_member_id", "")),
            status=NeuralTPPGenerationStatus(str(data.get("status", ""))),
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
            processed_step_count=_strict_int(
                data.get("processed_step_count"), "processed_step_count"
            ),
            skipped_unsupported_count=_strict_int(
                data.get("skipped_unsupported_count"),
                "skipped_unsupported_count",
            ),
            semantic_seed=cast(int | None, data.get("semantic_seed")),
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
            lineage_content_sha256=_optional_text(
                data.get("lineage_content_sha256")
            ),
            parameter_bytes=_strict_int(
                data.get("parameter_bytes"), "parameter_bytes"
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
    def from_json(cls, text: str) -> NeuralTPPGenerationEvidenceV1:
        return cls.from_dict(_json_mapping(text, 4 * 1024 * 1024))


def _lineage_digest(
    values: Sequence[NeuralTPPGenerationLineageV1],
) -> str:
    return hashlib.sha256(
        canonical_contract_json([item.to_dict() for item in values]).encode()
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class NeuralTPPGenerationResultV1:
    """Process-local anchors, neural proposals, lineage, and evidence."""

    events: tuple[BenchmarkEventV1, ...]
    event_lineage: tuple[NeuralTPPGenerationLineageV1, ...]
    evidence: NeuralTPPGenerationEvidenceV1

    def __post_init__(self) -> None:
        events = tuple(sorted(self.events, key=_benchmark_event_key))
        lineages = tuple(
            sorted(self.event_lineage, key=lambda item: item.step_index)
        )
        if any(not isinstance(item, BenchmarkEventV1) for item in events):
            raise TypeError("neural generation contains an invalid event")
        if any(
            not isinstance(item, NeuralTPPGenerationLineageV1)
            for item in lineages
        ):
            raise TypeError("neural generation contains invalid lineage")
        generated = {
            item.source_event_id
            for item in events
            if item.sparsity.startswith("neural-tpp-")
        }
        if generated != {item.source_event_id for item in lineages}:
            raise ValueError("neural generation events and lineage differ")
        if self.evidence.generated_event_count != len(lineages):
            raise ValueError("neural generation evidence count differs")
        if self.evidence.status in {
            NeuralTPPGenerationStatus.REFUSED,
            NeuralTPPGenerationStatus.FAILED,
        } and (events or lineages):
            raise ValueError("closed neural generation exposes partial output")
        if (
            self.evidence.lineage_content_sha256 is not None
            and self.evidence.lineage_content_sha256
            != _lineage_digest(lineages)
        ):
            raise ValueError("neural generation lineage digest differs")
        object.__setattr__(self, "events", events)
        object.__setattr__(self, "event_lineage", lineages)


class _NeuralTPPRefusal(NeuralTPPGenerationError):
    """Internal distinction between a bounded refusal and a defect."""


def _semantic_seed(payload: Mapping[str, JSONValue]) -> int:
    digest = hashlib.sha256(canonical_contract_json(payload).encode()).digest()
    return int.from_bytes(digest[:8], "big")


def _hidden_digest(hidden: Sequence[float]) -> str:
    return hashlib.sha256(
        canonical_contract_json(list(hidden)).encode()
    ).hexdigest()


def _sample_mark(probabilities: Sequence[float], rng: random.Random) -> int:
    draw = rng.random()
    cumulative = 0.0
    for index, probability in enumerate(probabilities):
        cumulative += probability
        if draw < cumulative:
            return index
    return len(probabilities) - 1


def _inverse_elapsed_seconds(
    base: float, slope: float, rng: random.Random
) -> tuple[float, float, float]:
    draw = min(1.0 - 2**-53, max(2**-53, rng.random()))
    target = -math.log1p(-draw)
    exp_base = math.exp(base)
    elapsed = math.log1p(slope * target / exp_base) / slope
    elapsed = max(elapsed, 1e-15)
    intensity = math.exp(base + slope * elapsed)
    if not all(
        math.isfinite(value) and value > 0.0 for value in (elapsed, intensity)
    ):
        raise NeuralTPPGenerationError("neural inverse-CDF sample is invalid")
    return elapsed, intensity, target


def _retained_history(
    history_events: Sequence[BenchmarkEventV1],
    *,
    config: NeuralTPPConfigV1,
    fit: NeuralTPPFitResultV1,
    window: ReconstructionWindowV1,
) -> tuple[BenchmarkEventV1, ...]:
    raw = tuple(history_events)
    if len(raw) > config.limits.max_history_events:
        raise _NeuralTPPRefusal("history_event_limit_exceeded")
    if any(not isinstance(item, BenchmarkEventV1) for item in raw):
        raise NeuralTPPGenerationError("history contains a non-benchmark event")
    if any(item.symbol not in fit.symbols for item in raw):
        raise _NeuralTPPRefusal("history_contains_unsupported_symbol")
    if any(item.event_time_ns >= window.input_start_ns for item in raw):
        raise _NeuralTPPRefusal("history_is_not_strict_prior")
    lower = window.input_start_ns - config.limits.max_history_lookback_ns
    retained = tuple(
        sorted(
            (item for item in raw if item.event_time_ns >= lower),
            key=_benchmark_event_key,
        )
    )
    if len({item.benchmark_event_id for item in retained}) != len(retained):
        raise _NeuralTPPRefusal("history_contains_duplicate_identity")
    return retained


def _enclosing_anchor_pair(
    anchors: Mapping[str, Sequence[BenchmarkEventV1]],
    symbol: str,
    event_time_ns: int,
) -> tuple[BenchmarkEventV1, BenchmarkEventV1] | None:
    for left, right in pairwise(anchors.get(symbol, ())):
        if left.event_time_ns < event_time_ns < right.event_time_ns:
            return left, right
    return None


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
    if mark == "unchanged":
        bid, ask = left.bid, left.ask
    elif mark == "ask_only":
        bid, ask = left.bid, max(left.bid, interpolated_ask)
    elif mark == "bid_only":
        bid, ask = min(left.ask, interpolated_bid), left.ask
    else:
        bid, ask = interpolated_bid, interpolated_ask
    if not all(math.isfinite(value) and value > 0.0 for value in (bid, ask)):
        raise NeuralTPPGenerationError("neural quote projection is invalid")
    if ask < bid:
        midpoint = (bid + ask) / 2.0
        bid = midpoint
        ask = midpoint
    return bid, ask


def _advance_hidden(
    parameters: Mapping[str, JSONValue],
    hidden: Sequence[float],
    event: BenchmarkEventV1,
    *,
    last_event_time_ns: int,
    previous_quotes: dict[str, tuple[float, float]],
    vocabulary: Mapping[str, int],
    checkpoint: NeuralTPPCheckpointV1,
    config: NeuralTPPConfigV1,
) -> list[float]:
    elapsed = max(
        (event.event_time_ns - last_event_time_ns) / NANOSECONDS_PER_SECOND,
        config.minimum_elapsed_seconds,
    )
    if event.event_time_ns < last_event_time_ns:
        raise NeuralTPPGenerationError("neural recurrent update reverses time")
    transition = _event_mark(event, previous_quotes.get(event.symbol))
    try:
        mark = vocabulary[f"{event.symbol}:{transition}"]
    except KeyError as err:
        raise NeuralTPPGenerationError(
            "neural recurrent update mark is unsupported"
        ) from err
    values = _input_vector(
        mark,
        elapsed,
        len(checkpoint.mark_vocabulary),
        checkpoint.duration_log_mean,
        checkpoint.duration_log_scale,
    )
    next_hidden = _hidden_step(parameters, hidden, values)
    previous_quotes[event.symbol] = (event.bid, event.ask)
    return next_hidden


def _validate_generation_inputs(
    config: NeuralTPPConfigV1,
    fit: NeuralTPPFitResultV1,
    context: NeuralTPPWindowContextV1,
    events: Sequence[BenchmarkEventV1],
    scenario: BenchmarkScenarioV1,
    window: ReconstructionWindowV1,
    ensemble_member_id: str,
) -> tuple[BenchmarkEventV1, ...]:
    _validate_fit_against_config(config, fit)
    if fit.status is not NeuralTPPFitStatus.FITTED:
        raise NeuralTPPGenerationError(
            "neural generator requires a fitted model"
        )
    if context.window_id != window.window_id:
        raise NeuralTPPGenerationError(
            "neural generation context/window differs"
        )
    if window.ensemble_member_id != ensemble_member_id:
        raise NeuralTPPGenerationError(
            "neural generation member/window differs"
        )
    if scenario.event_schema_version != BENCHMARK_EVENT_SCHEMA_VERSION:
        raise NeuralTPPGenerationError(
            "neural generation scenario schema differs"
        )
    raw = tuple(events)
    if any(not isinstance(item, BenchmarkEventV1) for item in raw):
        raise NeuralTPPGenerationError("input contains a non-benchmark event")
    if not raw:
        raise _NeuralTPPRefusal("generation_input_is_empty")
    if len(raw) > config.limits.max_sequence_events:
        raise _NeuralTPPRefusal("input_event_limit_exceeded")
    if any(
        item.symbol not in fit.symbols
        or not window.reads_event_time(item.event_time_ns)
        for item in raw
    ):
        raise _NeuralTPPRefusal("input_is_outside_synchronized_scope")
    ordered = tuple(sorted(raw, key=_benchmark_event_key))
    if len({item.benchmark_event_id for item in ordered}) != len(ordered):
        raise _NeuralTPPRefusal("input_contains_duplicate_identity")
    by_symbol = Counter(item.symbol for item in ordered)
    if any(by_symbol.get(symbol, 0) < 2 for symbol in fit.symbols):
        raise _NeuralTPPRefusal("destination_symbol_lacks_two_anchors")
    if (
        fit.information_mode is InformationMode.EX_ANTE_SIMULATION
        and context.observed_context_id is not None
        and cast(int, context.observed_context_available_ns)
        > window.core_start_ns
    ):
        raise _NeuralTPPRefusal("context_is_not_available_ex_ante")
    return ordered


@dataclass(frozen=True, slots=True)
class FittedNeuralTPPBenchmarkGeneratorV1(BenchmarkGeneratorV1):
    """Adapter exposing the single fitted RMTPP challenger."""

    candidate: BenchmarkCandidateV1
    config: NeuralTPPConfigV1
    fit_result: NeuralTPPFitResultV1
    window_contexts: Mapping[str, NeuralTPPWindowContextV1] = field(
        default_factory=dict
    )
    candidate_id: str = field(init=False)
    event_schema_version: str = BENCHMARK_EVENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.candidate.kind is not BenchmarkCandidateKind.CANDIDATE:
            raise ValueError("neural TPP adapter requires a candidate")
        if self.candidate.method_id != NEURAL_TPP_GENERATOR_ID:
            raise ValueError("neural TPP candidate method differs")
        _validate_fit_against_config(self.config, self.fit_result)
        if self.fit_result.status is not NeuralTPPFitStatus.FITTED:
            raise NeuralTPPFitError(
                "neural TPP adapter requires a fitted model"
            )
        contexts = dict(self.window_contexts)
        if any(key != value.window_id for key, value in contexts.items()):
            raise ValueError("neural TPP context key differs")
        object.__setattr__(self, "window_contexts", contexts)
        if self.event_schema_version != BENCHMARK_EVENT_SCHEMA_VERSION:
            raise ValueError("neural TPP adapter requires benchmark event v1")
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
            NeuralTPPGenerationStatus.REFUSED,
            NeuralTPPGenerationStatus.FAILED,
        }:
            raise NeuralTPPGenerationError(
                result.evidence.failure_reason or "neural generation failed"
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
    ) -> NeuralTPPGenerationResultV1:
        started = time.perf_counter()
        before_peak = peak_rss_bytes()
        raw = tuple(degraded_events)
        history_count = 0
        processed_steps = 0
        skipped = 0
        seed: int | None = None
        anchor_hash: str | None = None
        input_hash: str | None = None
        history_hash: str | None = None
        context_hash: str | None = None
        checkpoint = self.fit_result.checkpoint
        dataset = self.fit_result.dataset_manifest
        training = self.fit_result.training_manifest
        if checkpoint is None or dataset is None or training is None:
            raise NeuralTPPFitError("fitted neural artifacts are absent")
        context = self.window_contexts.get(window.window_id)
        try:
            if context is None:
                raise NeuralTPPGenerationError(
                    "neural generation context is absent"
                )
            ordered = _validate_generation_inputs(
                self.config,
                self.fit_result,
                context,
                raw,
                scenario,
                window,
                ensemble_member_id,
            )
            retained_history = _retained_history(
                history_events,
                config=self.config,
                fit=self.fit_result,
                window=window,
            )
            history_count = len(retained_history)
            anchor_hash = _anchor_digest(ordered)
            input_hash = _event_content_digest(ordered)
            history_hash = _event_content_digest(retained_history)
            context_hash = hashlib.sha256(
                context.to_json().encode()
            ).hexdigest()
            seed = _semantic_seed(
                {
                    "architecture": self.config.architecture,
                    "config_id": self.config.config_id,
                    "fit_id": self.fit_result.fit_id,
                    "dataset_id": dataset.dataset_id,
                    "training_id": training.training_id,
                    "checkpoint_id": checkpoint.checkpoint_id,
                    "scenario_id": scenario.scenario_id,
                    "window_id": window.window_id,
                    "ensemble_member_id": ensemble_member_id,
                    "input_event_content_sha256": input_hash,
                    "history_content_sha256": history_hash,
                    "window_context_sha256": context_hash,
                }
            )
            estimated_memory = (
                1024 * 1024
                + (len(ordered) + len(retained_history)) * 2048
                + self.config.limits.max_generated_events * 4096
            )
            if (
                estimated_memory
                > self.config.limits.max_generation_memory_bytes
            ):
                raise _NeuralTPPRefusal("generation_memory_preflight_exceeded")
            deadline = started + (
                self.config.limits.max_generation_wall_time_ms / 1000.0
            )
            parameters = checkpoint.parameters
            vocabulary = {
                value: index
                for index, value in enumerate(checkpoint.mark_vocabulary)
            }
            hidden = _hidden_step(
                parameters,
                cast(Sequence[float], parameters["initial_hidden"]),
                _start_input_vector(len(checkpoint.mark_vocabulary)),
            )
            prior_quotes: dict[str, tuple[float, float]] = {}
            last_event_time = window.input_start_ns
            parent_event_id: str | None = None
            for event in retained_history:
                hidden = _advance_hidden(
                    parameters,
                    hidden,
                    event,
                    last_event_time_ns=min(
                        last_event_time, event.event_time_ns
                    ),
                    previous_quotes=prior_quotes,
                    vocabulary=vocabulary,
                    checkpoint=checkpoint,
                    config=self.config,
                )
                last_event_time = event.event_time_ns
                parent_event_id = event.source_event_id
            if retained_history:
                last_event_time = retained_history[-1].event_time_ns
            anchors_by_symbol: dict[str, list[BenchmarkEventV1]] = defaultdict(
                list
            )
            for event in ordered:
                anchors_by_symbol[event.symbol].append(event)
            rng = random.Random(seed)
            cursor = window.input_start_ns
            anchor_index = 0
            generated: list[BenchmarkEventV1] = []
            lineages: list[NeuralTPPGenerationLineageV1] = []
            occupied_times = {item.event_time_ns for item in ordered}
            interval_counts: Counter[str] = Counter()
            while cursor < window.core_end_ns:
                if time.perf_counter() > deadline:
                    raise _NeuralTPPRefusal("generation_wall_time_exceeded")
                processed_steps += 1
                if processed_steps > self.config.limits.max_generation_steps:
                    raise _NeuralTPPRefusal("generation_step_limit_exceeded")
                elapsed_from_event = max(
                    0.0,
                    (cursor - last_event_time) / NANOSECONDS_PER_SECOND,
                )
                base = _time_base(parameters, hidden) + (
                    self.config.elapsed_slope_per_second * elapsed_from_event
                )
                elapsed, intensity, integrated_hazard = (
                    _inverse_elapsed_seconds(
                        base, self.config.elapsed_slope_per_second, rng
                    )
                )
                sampled_ns = cursor + max(
                    1, round(elapsed * NANOSECONDS_PER_SECOND)
                )
                if (
                    anchor_index < len(ordered)
                    and ordered[anchor_index].event_time_ns <= sampled_ns
                ):
                    anchor = ordered[anchor_index]
                    hidden = _advance_hidden(
                        parameters,
                        hidden,
                        anchor,
                        last_event_time_ns=last_event_time,
                        previous_quotes=prior_quotes,
                        vocabulary=vocabulary,
                        checkpoint=checkpoint,
                        config=self.config,
                    )
                    last_event_time = anchor.event_time_ns
                    parent_event_id = anchor.source_event_id
                    cursor = max(cursor, anchor.event_time_ns)
                    anchor_index += 1
                    continue
                if sampled_ns >= window.core_end_ns:
                    break
                probabilities = _mark_probabilities(parameters, hidden)
                mark_index = _sample_mark(probabilities, rng)
                joint_mark = checkpoint.mark_vocabulary[mark_index]
                destination_symbol, mark = joint_mark.split(":", 1)
                pair = _enclosing_anchor_pair(
                    anchors_by_symbol, destination_symbol, sampled_ns
                )
                if pair is None or not window.owns_event_time(sampled_ns):
                    skipped += 1
                    cursor = sampled_ns
                    continue
                left, right = pair
                interval_id = derive_anchor_interval_id(
                    left.benchmark_event_id, right.benchmark_event_id
                )
                if sampled_ns in occupied_times:
                    skipped += 1
                    cursor = sampled_ns
                    continue
                interval_counts[interval_id] += 1
                if (
                    interval_counts[interval_id]
                    > self.config.limits.max_events_per_interval
                ):
                    raise _NeuralTPPRefusal(
                        "events_per_interval_limit_exceeded"
                    )
                if len(generated) >= self.config.limits.max_generated_events:
                    raise _NeuralTPPRefusal("generated_event_limit_exceeded")
                if len(generated) + 1 > (
                    len(ordered)
                    * self.config.limits.max_candidate_amplification
                ):
                    raise _NeuralTPPRefusal(
                        "candidate_amplification_limit_exceeded"
                    )
                bid, ask = _project_quote(left, right, sampled_ns, mark)
                source_id = _stable_id(
                    "neural-tpp-event",
                    {
                        "semantic_seed": seed,
                        "step_index": processed_steps,
                        "event_time_ns": sampled_ns,
                        "joint_mark": joint_mark,
                        "anchor_interval_id": interval_id,
                        "checkpoint_id": checkpoint.checkpoint_id,
                    },
                )
                event = BenchmarkEventV1(
                    source_event_id=source_id,
                    symbol=destination_symbol,
                    event_time_ns=sampled_ns,
                    event_sequence=processed_steps,
                    bid=bid,
                    ask=ask,
                    epoch_id=left.epoch_id,
                    session=context.session,
                    event_state=mark,
                    sparsity="neural-tpp-rmtpp-cpu-v1",
                    ensemble_member_id=ensemble_member_id,
                    anchor_id=None,
                    support_lower_mid=min(
                        left.mid, right.mid, (bid + ask) / 2.0
                    ),
                    support_upper_mid=max(
                        left.mid, right.mid, (bid + ask) / 2.0
                    ),
                )
                probability = probabilities[mark_index]
                lineage = NeuralTPPGenerationLineageV1(
                    source_event_id=source_id,
                    step_index=processed_steps,
                    destination_symbol=destination_symbol,
                    mark_label=mark,
                    mark_probability=probability,
                    elapsed_seconds=elapsed,
                    conditional_intensity=intensity,
                    log_joint_density=(
                        math.log(intensity)
                        - integrated_hazard
                        + math.log(max(probability, 1e-300))
                    ),
                    hidden_state_sha256=_hidden_digest(hidden),
                    anchor_interval_id=interval_id,
                    parent_event_id=parent_event_id,
                )
                hidden = _advance_hidden(
                    parameters,
                    hidden,
                    event,
                    last_event_time_ns=last_event_time,
                    previous_quotes=prior_quotes,
                    vocabulary=vocabulary,
                    checkpoint=checkpoint,
                    config=self.config,
                )
                generated.append(event)
                lineages.append(lineage)
                occupied_times.add(sampled_ns)
                last_event_time = sampled_ns
                parent_event_id = source_id
                cursor = sampled_ns
            lineage_hash = _lineage_digest(lineages)
            output_events = tuple(
                sorted(ordered + tuple(generated), key=_benchmark_event_key)
            )
            output_bytes = len(
                canonical_contract_json(
                    {
                        "events": [item.to_dict() for item in output_events],
                        "lineage": [item.to_dict() for item in lineages],
                    }
                ).encode()
            )
            if output_bytes > self.config.limits.max_generation_output_bytes:
                raise _NeuralTPPRefusal("generation_output_limit_exceeded")
            if time.perf_counter() > deadline:
                raise _NeuralTPPRefusal("generation_wall_time_exceeded")
            measured_peak = max(0, peak_rss_bytes() - before_peak)
            if measured_peak > self.config.limits.max_generation_memory_bytes:
                raise _NeuralTPPRefusal("generation_measured_memory_exceeded")
            status = (
                NeuralTPPGenerationStatus.GENERATED
                if generated
                else NeuralTPPGenerationStatus.EMPTY
            )
            evidence = NeuralTPPGenerationEvidenceV1(
                config_id=self.config.config_id,
                fit_id=self.fit_result.fit_id,
                dataset_id=dataset.dataset_id,
                training_id=training.training_id,
                checkpoint_id=checkpoint.checkpoint_id,
                window_id=window.window_id,
                window_context_id=context.context_id,
                ensemble_member_id=ensemble_member_id,
                status=status,
                attempted=True,
                input_event_count=len(ordered),
                history_event_count=history_count,
                generated_event_count=len(generated),
                processed_step_count=processed_steps,
                skipped_unsupported_count=skipped,
                semantic_seed=seed,
                input_anchor_sha256=anchor_hash,
                input_event_content_sha256=input_hash,
                history_content_sha256=history_hash,
                window_context_sha256=context_hash,
                lineage_content_sha256=lineage_hash,
                parameter_bytes=checkpoint.parameter_bytes,
                wall_time_ms=round((time.perf_counter() - started) * 1000),
                peak_memory_bytes=measured_peak,
            )
            return NeuralTPPGenerationResultV1(
                events=output_events,
                event_lineage=tuple(lineages),
                evidence=evidence,
            )
        except (
            ArithmeticError,
            KeyError,
            NeuralTPPFitError,
            NeuralTPPGenerationError,
            TypeError,
            ValueError,
        ) as err:
            status = (
                NeuralTPPGenerationStatus.REFUSED
                if isinstance(err, _NeuralTPPRefusal)
                else NeuralTPPGenerationStatus.FAILED
            )
            evidence = NeuralTPPGenerationEvidenceV1(
                config_id=self.config.config_id,
                fit_id=self.fit_result.fit_id,
                dataset_id=dataset.dataset_id,
                training_id=training.training_id,
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
                generated_event_count=0,
                processed_step_count=processed_steps,
                skipped_unsupported_count=skipped,
                semantic_seed=seed,
                input_anchor_sha256=anchor_hash,
                input_event_content_sha256=input_hash,
                history_content_sha256=history_hash,
                window_context_sha256=context_hash,
                lineage_content_sha256=None,
                parameter_bytes=checkpoint.parameter_bytes,
                wall_time_ms=round((time.perf_counter() - started) * 1000),
                peak_memory_bytes=max(0, peak_rss_bytes() - before_peak),
                failure_reason=(
                    f"generation_{status.value}:{type(err).__name__}:{err}"
                ),
            )
            return NeuralTPPGenerationResultV1((), (), evidence)


def build_neural_tpp_benchmark_candidate(
    config: NeuralTPPConfigV1,
    fit_result: NeuralTPPFitResultV1,
    *,
    ensemble_member_ids: Sequence[str],
) -> BenchmarkCandidateV1:
    """Describe the single neural fit attempt without promoting it."""
    if fit_result.config_id != config.config_id:
        raise ValueError("neural TPP fit and config differ")
    return BenchmarkCandidateV1(
        kind=BenchmarkCandidateKind.CANDIDATE,
        method_id=NEURAL_TPP_GENERATOR_ID,
        implementation_version=NEURAL_TPP_IMPLEMENTATION_VERSION,
        parameters={
            "config_id": config.config_id,
            "fit_id": fit_result.fit_id,
            "architecture": config.architecture,
            "automatic_winner": False,
        },
        ensemble_member_ids=tuple(ensemble_member_ids),
    )


def build_fitted_neural_tpp_generator(
    config: NeuralTPPConfigV1,
    fit_result: NeuralTPPFitResultV1,
    *,
    ensemble_member_ids: Sequence[str],
    window_contexts: Mapping[str, NeuralTPPWindowContextV1],
) -> FittedNeuralTPPBenchmarkGeneratorV1:
    """Bind one fitted RMTPP checkpoint to the benchmark adapter."""
    return FittedNeuralTPPBenchmarkGeneratorV1(
        candidate=build_neural_tpp_benchmark_candidate(
            config,
            fit_result,
            ensemble_member_ids=ensemble_member_ids,
        ),
        config=config,
        fit_result=fit_result,
        window_contexts=window_contexts,
    )


@dataclass(frozen=True, slots=True)
class NeuralTPPCandidateLineageV1:
    """Carving pointer retaining the sampled neural lineage identity."""

    event_id: str
    transformation_id: str
    generation_lineage_id: str
    schema_version: str = NEURAL_TPP_CANDIDATE_LINEAGE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            NEURAL_TPP_CANDIDATE_LINEAGE_SCHEMA_VERSION,
            "neural TPP candidate lineage",
        )
        for name in (
            "event_id",
            "transformation_id",
            "generation_lineage_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "transformation_id": self.transformation_id,
            "generation_lineage_id": self.generation_lineage_id,
        }


@dataclass(frozen=True, slots=True)
class NeuralTPPCandidateBatchV1:
    """Process-local neural proposals for one immutable anchor interval."""

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
    event_lineage: tuple[NeuralTPPCandidateLineageV1, ...]
    fit_id: str
    dataset_id: str
    training_id: str
    checkpoint_id: str
    generation_evidence_id: str
    window_context_id: str
    batch_id: str = ""
    schema_version: str = NEURAL_TPP_CANDIDATE_BATCH_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            NEURAL_TPP_CANDIDATE_BATCH_SCHEMA_VERSION,
            "neural TPP candidate batch",
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
            "dataset_id",
            "training_id",
            "checkpoint_id",
            "generation_evidence_id",
            "window_context_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
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
            raise ValueError("generated neural candidate batch requires events")
        if status is not MotifGenerationStatus.GENERATED and events:
            raise ValueError("closed neural candidate batch contains events")
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
            raise ValueError("neural candidate event differs from batch scope")
        event_ids = {item.event_id for item in events}
        if len(event_ids) != len(events) or event_ids != {
            item.event_id for item in lineages
        }:
            raise ValueError("neural candidate lineage does not reconcile")
        if len({item.generation_lineage_id for item in lineages}) != len(
            lineages
        ):
            raise ValueError("neural generation lineage is reused")
        object.__setattr__(self, "events", events)
        object.__setattr__(self, "event_lineage", lineages)
        expected = _stable_id(
            "neural-tpp-candidate-batch", self.identity_payload()
        )
        if self.batch_id and self.batch_id != expected:
            raise ValueError("neural TPP candidate batch_id differs")
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
            "dataset_id": self.dataset_id,
            "training_id": self.training_id,
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

    def lineage_for(self, event_id: str) -> NeuralTPPCandidateLineageV1:
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


def build_neural_tpp_candidate_batches(
    *,
    run: ReconstructionRunV1,
    window: ReconstructionWindowV1,
    config: NeuralTPPConfigV1,
    fit_result: NeuralTPPFitResultV1,
    generation_result: NeuralTPPGenerationResultV1,
    context: NeuralTPPWindowContextV1,
    observed_events: Sequence[SyntheticEventV1],
    session_state: str,
    special_tags: Sequence[str] = (),
    event_tags: Sequence[str] = (),
) -> tuple[NeuralTPPCandidateBatchV1, ...]:
    """Project neural proposals into the generator-neutral carving seam."""
    if window.run_id != run.run_id:
        raise ValueError("neural candidate window does not belong to run")
    if window.ensemble_member_id not in run.ensemble_member_ids:
        raise ValueError("neural candidate member is outside run")
    if config.config_id not in run.configuration_ids:
        raise ValueError("neural config is absent from reconstruction run")
    _validate_fit_against_config(config, fit_result)
    dataset = fit_result.dataset_manifest
    training = fit_result.training_manifest
    checkpoint = fit_result.checkpoint
    if dataset is None or training is None or checkpoint is None:
        raise ValueError("neural candidate requires fitted artifacts")
    evidence = generation_result.evidence
    if (
        evidence.config_id != config.config_id
        or evidence.fit_id != fit_result.fit_id
        or evidence.dataset_id != dataset.dataset_id
        or evidence.training_id != training.training_id
        or evidence.checkpoint_id != checkpoint.checkpoint_id
        or evidence.window_id != window.window_id
        or evidence.window_context_id != context.context_id
        or context.window_id != window.window_id
    ):
        raise ValueError("neural fit/generation/context identities differ")
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
        raise ValueError("neural carving projection requires observed anchors")
    if evidence.input_anchor_sha256 is not None and (
        _synthetic_anchor_digest(observed) != evidence.input_anchor_sha256
    ):
        raise ValueError("neural carving anchors differ from generation input")
    upstream_closed = evidence.status in {
        NeuralTPPGenerationStatus.REFUSED,
        NeuralTPPGenerationStatus.FAILED,
    }
    proposals = tuple(
        item
        for item in generation_result.events
        if item.sparsity.startswith("neural-tpp-")
    )
    generation_lineage = {
        item.source_event_id: item for item in generation_result.event_lineage
    }
    if set(generation_lineage) != {item.source_event_id for item in proposals}:
        raise ValueError("neural proposal and generation lineage differ")
    batches: list[NeuralTPPCandidateBatchV1] = []
    assigned: set[str] = set()
    by_symbol: dict[str, list[SyntheticEventV1]] = defaultdict(list)
    for event in observed:
        by_symbol[event.symbol].append(event)
    for symbol in sorted(by_symbol):
        anchors = by_symbol[symbol]
        if len(anchors) < 2:
            raise ValueError("each neural carving symbol requires two anchors")
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
                "neural-tpp-interval-transformation",
                {
                    "fit_id": fit_result.fit_id,
                    "checkpoint_id": checkpoint.checkpoint_id,
                    "generation_evidence_id": evidence.evidence_id,
                    "window_context_id": context.context_id,
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
                    source_version_id=left.source_version_id,
                    left_anchor_event_id=left.event_id,
                    right_anchor_event_id=right.event_id,
                    anchor_interval_id=interval_id,
                    generator_id=NEURAL_TPP_GENERATOR_ID,
                    generator_version=NEURAL_TPP_IMPLEMENTATION_VERSION,
                    generator_config_id=config.config_id,
                    reference_id=item.source_event_id,
                    motif_id=NEURAL_TPP_GENERATOR_ID,
                    feed_epoch_id=item.epoch_id,
                    constraint_set_id=CANDIDATE_ONLY_CONSTRAINT_SET_ID,
                    confidence=generation_lineage[
                        item.source_event_id
                    ].mark_probability,
                )
                for ordinal, item in enumerate(selected, start=1)
            )
            status = (
                MotifGenerationStatus.REFUSED
                if upstream_closed
                else (
                    MotifGenerationStatus.GENERATED
                    if events
                    else MotifGenerationStatus.EMPTY
                )
            )
            batches.append(
                NeuralTPPCandidateBatchV1(
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
                    events=events if not upstream_closed else (),
                    event_lineage=(
                        tuple(
                            NeuralTPPCandidateLineageV1(
                                event_id=event.event_id,
                                transformation_id=transformation_id,
                                generation_lineage_id=generation_lineage[
                                    cast(str, event.reference_id)
                                ].lineage_id,
                            )
                            for event in events
                        )
                        if not upstream_closed
                        else ()
                    ),
                    fit_id=fit_result.fit_id,
                    dataset_id=dataset.dataset_id,
                    training_id=training.training_id,
                    checkpoint_id=checkpoint.checkpoint_id,
                    generation_evidence_id=evidence.evidence_id,
                    window_context_id=context.context_id,
                )
            )
    if assigned != {item.source_event_id for item in proposals}:
        raise ValueError("neural proposal lies outside observed anchors")
    return tuple(batches)
