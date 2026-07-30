"""Bounded marked Add-Thin sequence challenger.

This module implements the fixed ``histogram_marked_add_thin_cpu_v1``
research surface from issue #454.  It follows the Add-Thin forward process
and B/C/D/E reverse decomposition, but deliberately replaces the paper's
neural posterior approximators with finite train-only time-bin/mark tables.
The categorical mark model is a project-specific extension: the reference
paper models arrival times and explicitly leaves marks for future work.
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

ADD_THIN_RESOURCE_LIMITS_SCHEMA_VERSION = (
    "histdatacom.add-thin-resource-limits.v1"
)
ADD_THIN_CONFIG_SCHEMA_VERSION = "histdatacom.add-thin-config.v1"
ADD_THIN_WINDOW_CONTEXT_SCHEMA_VERSION = (
    "histdatacom.add-thin-window-context.v1"
)
ADD_THIN_PROTECTED_WINDOW_SCHEMA_VERSION = (
    "histdatacom.add-thin-protected-window.v1"
)
ADD_THIN_DATASET_WINDOW_SCHEMA_VERSION = (
    "histdatacom.add-thin-dataset-window.v1"
)
ADD_THIN_DATASET_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.add-thin-dataset-manifest.v1"
)
ADD_THIN_CHECKPOINT_SCHEMA_VERSION = "histdatacom.add-thin-checkpoint.v1"
ADD_THIN_FIT_RESULT_SCHEMA_VERSION = "histdatacom.add-thin-fit-result.v1"
ADD_THIN_STEP_EVIDENCE_SCHEMA_VERSION = "histdatacom.add-thin-step-evidence.v1"
ADD_THIN_GENERATION_LINEAGE_SCHEMA_VERSION = (
    "histdatacom.add-thin-generation-lineage.v1"
)
ADD_THIN_GENERATION_EVIDENCE_SCHEMA_VERSION = (
    "histdatacom.add-thin-generation-evidence.v1"
)
ADD_THIN_CANDIDATE_LINEAGE_SCHEMA_VERSION = (
    "histdatacom.add-thin-candidate-lineage.v1"
)
ADD_THIN_CANDIDATE_BATCH_SCHEMA_VERSION = (
    "histdatacom.add-thin-candidate-batch.v1"
)

ADD_THIN_ARCHITECTURE = "histogram_marked_add_thin_cpu_v1"
ADD_THIN_IMPLEMENTATION_VERSION = "1.0.0"
ADD_THIN_GENERATOR_ID = "histdatacom.add-thin.histogram-marked-cpu-v1"
NANOSECONDS_PER_SECOND = 1_000_000_000
MARK_STATES = ("ask_only", "bid_only", "joint", "unchanged")
ASSIGNMENT_KINDS = ("epoch", "transition")


class AddThinFitError(RuntimeError):
    """Raised when the bounded Add-Thin estimator cannot fit safely."""


class AddThinGenerationError(RuntimeError):
    """Raised when bounded Add-Thin generation cannot complete safely."""


class _AddThinRefusal(AddThinGenerationError):
    """Internal marker for expected all-or-nothing generation refusal."""


class AddThinFitStatus(str, Enum):
    """Terminal fit state."""

    FITTED = "fitted"
    REFUSED = "refused"
    FAILED = "failed"


class AddThinGenerationStatus(str, Enum):
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


def _required_text(value: Any) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 1024:
        raise ValueError("required text is invalid")
    return value


def _optional_text(value: Any) -> str | None:
    return None if value is None else _required_text(value)


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


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be an object")
    return cast(Mapping[str, Any], value)


def _sequence(value: Any, name: str) -> Sequence[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise TypeError(f"{name} must be a sequence")
    return value


def _json_mapping(text: str, maximum: int) -> Mapping[str, Any]:
    if not isinstance(text, str) or len(text.encode()) > maximum:
        raise ValueError("JSON payload exceeds bound")
    return _mapping(json.loads(text), "JSON payload")


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


def _benchmark_event_key(event: BenchmarkEventV1) -> tuple[Any, ...]:
    return (
        event.event_time_ns,
        event.symbol,
        event.event_sequence,
        event.benchmark_event_id,
    )


def _event_content_digest(events: Sequence[BenchmarkEventV1]) -> str:
    return hashlib.sha256(
        canonical_contract_json(
            [
                item.to_dict()
                for item in sorted(events, key=_benchmark_event_key)
            ]
        ).encode()
    ).hexdigest()


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


def _near_signature(events: Sequence[BenchmarkEventV1]) -> int:
    """Return a deterministic 64-bit SimHash over row-free event features."""
    weights = [0] * 64
    previous: dict[str, tuple[float, float]] = {}
    for event in sorted(events, key=_benchmark_event_key):
        mark = _event_mark(event, previous.get(event.symbol))
        previous[event.symbol] = (event.bid, event.ask)
        token = (
            f"{event.symbol}|{event.event_time_ns // 1_000_000}|{mark}|"
            f"{round(event.spread, 10)}"
        )
        digest = hashlib.sha256(token.encode()).digest()[:8]
        bits = int.from_bytes(digest, "big")
        for index in range(64):
            weights[index] += 1 if bits & (1 << index) else -1
    signature = 0
    for index, weight in enumerate(weights):
        if weight >= 0:
            signature |= 1 << index
    return signature


@dataclass(frozen=True, slots=True)
class AddThinResourceLimitsV1:
    """Independent fit, checkpoint, and generation envelopes."""

    max_fit_events: int = 100_000
    max_fit_windows: int = 256
    max_sequence_events: int = 50_000
    max_time_bins: int = 64
    max_mark_count: int = 64
    max_diffusion_steps: int = 32
    max_smoothing_candidates: int = 16
    max_corruption_points: int = 500_000
    max_poisson_draw_work: int = 5_000_000
    max_parameter_count: int = 100_000
    max_checkpoint_bytes: int = 8_000_000
    max_diagnostics: int = 256
    max_fit_memory_bytes: int = 256 * 1024 * 1024
    max_fit_wall_time_ms: int = 60_000
    max_generation_points: int = 100_000
    max_generation_steps: int = 32
    max_events_per_bin: int = 10_000
    max_events_per_interval: int = 512
    max_candidate_amplification: float = 8.0
    max_history_events: int = 100_000
    max_history_lookback_ns: int = 7 * 86_400 * NANOSECONDS_PER_SECOND
    max_generation_memory_bytes: int = 256 * 1024 * 1024
    max_output_bytes: int = 64 * 1024 * 1024
    max_generation_wall_time_ms: int = 30_000
    max_json_bytes: int = 16 * 1024 * 1024
    schema_version: str = ADD_THIN_RESOURCE_LIMITS_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ADD_THIN_RESOURCE_LIMITS_SCHEMA_VERSION:
            raise ValueError("unsupported Add-Thin resource schema")
        integer_bounds = {
            "max_fit_events": (1, 1_000_000),
            "max_fit_windows": (2, 4096),
            "max_sequence_events": (1, 1_000_000),
            "max_time_bins": (2, 256),
            "max_mark_count": (1, 256),
            "max_diffusion_steps": (2, 256),
            "max_smoothing_candidates": (1, 64),
            "max_corruption_points": (1, 10_000_000),
            "max_poisson_draw_work": (1, 100_000_000),
            "max_parameter_count": (1, 10_000_000),
            "max_checkpoint_bytes": (1024, 128 * 1024 * 1024),
            "max_diagnostics": (1, 4096),
            "max_fit_memory_bytes": (1024 * 1024, 4 * 1024**3),
            "max_fit_wall_time_ms": (1, 3_600_000),
            "max_generation_points": (1, 1_000_000),
            "max_generation_steps": (2, 256),
            "max_events_per_bin": (1, 1_000_000),
            "max_events_per_interval": (1, 100_000),
            "max_history_events": (0, 1_000_000),
            "max_history_lookback_ns": (
                1,
                365 * 86_400 * NANOSECONDS_PER_SECOND,
            ),
            "max_generation_memory_bytes": (1024 * 1024, 4 * 1024**3),
            "max_output_bytes": (1024, 1024 * 1024**2),
            "max_generation_wall_time_ms": (1, 3_600_000),
            "max_json_bytes": (1024, 128 * 1024 * 1024),
        }
        for name, (minimum, maximum) in integer_bounds.items():
            object.__setattr__(
                self,
                name,
                _bounded_int(getattr(self, name), name, minimum, maximum),
            )
        amplification = _finite_float(
            self.max_candidate_amplification,
            "max_candidate_amplification",
        )
        if not 1.0 <= amplification <= 100.0:
            raise ValueError("candidate amplification is outside bounds")
        object.__setattr__(self, "max_candidate_amplification", amplification)

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
    def from_dict(cls, data: Mapping[str, Any]) -> AddThinResourceLimitsV1:
        if (
            data.get("schema_version")
            != ADD_THIN_RESOURCE_LIMITS_SCHEMA_VERSION
        ):
            raise ValueError("unsupported Add-Thin resource schema")
        allowed = set(cls.__dataclass_fields__)
        if set(data) != allowed:
            raise ValueError("Add-Thin resource fields differ")
        kwargs: dict[str, Any] = {"schema_version": data["schema_version"]}
        for name in allowed - {"schema_version", "max_candidate_amplification"}:
            kwargs[name] = _strict_int(data[name], name)
        kwargs["max_candidate_amplification"] = _finite_float(
            data["max_candidate_amplification"],
            "max_candidate_amplification",
        )
        return cls(**kwargs)


@dataclass(frozen=True, slots=True)
class AddThinConfigV1:
    """Fixed bounded Add-Thin approximation and schedule."""

    architecture: str = ADD_THIN_ARCHITECTURE
    time_bin_count: int = 16
    step_keep_probabilities: tuple[float, ...] = (0.8, 0.75, 2.0 / 3.0, 0.5)
    smoothing_candidates: tuple[float, ...] = (0.25, 1.0, 4.0)
    near_duplicate_hamming_threshold: int = 0
    base_seed: int = 454
    accelerator_count: int = 0
    normalized_time_policy: str = "half-open-window-unit-interval-v1"
    endpoint_policy: str = "exclude-window-endpoints-nanosecond-v1"
    noise_policy: str = "training-mean-hpp-uniform-time-mark-v1"
    clean_intensity_policy: str = "piecewise-constant-time-bin-joint-mark-v1"
    classifier_policy: str = "empirical-bayes-bin-mark-step-v1"
    reverse_policy: str = "add-thin-b-c-d-e-v1"
    mark_policy: str = "destination-symbol-x-quote-transition-v1"
    quote_projection_policy: str = "enclosing-anchor-linear-transition-v1"
    unsupported_mark_policy: str = "fail-closed-v1"
    prior_quote_reset_policy: str = "whole-window-and-symbol-v1"
    equal_time_order_policy: str = "time-symbol-source-sequence-identity-v1"
    collision_policy: str = "account-and-skip-no-time-shift-v1"
    anchor_policy: str = "anchors-external-immutable-mask-v1"
    limits: AddThinResourceLimitsV1 = field(
        default_factory=AddThinResourceLimitsV1
    )
    config_id: str = ""
    schema_version: str = ADD_THIN_CONFIG_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ADD_THIN_CONFIG_SCHEMA_VERSION:
            raise ValueError("unsupported Add-Thin config schema")
        if self.architecture != ADD_THIN_ARCHITECTURE:
            raise ValueError("Add-Thin architecture is not fixed")
        if not isinstance(self.limits, AddThinResourceLimitsV1):
            raise TypeError("Add-Thin limits require the v1 contract")
        bins = _bounded_int(
            self.time_bin_count,
            "time_bin_count",
            2,
            self.limits.max_time_bins,
        )
        object.__setattr__(self, "time_bin_count", bins)
        steps = tuple(
            _finite_float(item, "step_keep_probability")
            for item in self.step_keep_probabilities
        )
        if not 2 <= len(steps) <= self.limits.max_diffusion_steps or any(
            not 0.0 < item < 1.0 for item in steps
        ):
            raise ValueError("Add-Thin keep schedule is invalid")
        cumulative = 1.0
        for item in steps:
            cumulative *= item
        if cumulative >= 0.5:
            raise ValueError("Add-Thin terminal noise schedule is too weak")
        object.__setattr__(self, "step_keep_probabilities", steps)
        smoothing = tuple(
            sorted(
                {
                    _finite_float(item, "smoothing_candidate")
                    for item in self.smoothing_candidates
                }
            )
        )
        if (
            not smoothing
            or len(smoothing) > self.limits.max_smoothing_candidates
            or any(item <= 0.0 or item > 100.0 for item in smoothing)
        ):
            raise ValueError("Add-Thin smoothing grid is invalid")
        object.__setattr__(self, "smoothing_candidates", smoothing)
        object.__setattr__(
            self,
            "near_duplicate_hamming_threshold",
            _bounded_int(
                self.near_duplicate_hamming_threshold,
                "near_duplicate_hamming_threshold",
                0,
                64,
            ),
        )
        object.__setattr__(
            self,
            "base_seed",
            _bounded_int(self.base_seed, "base_seed", 0, 2**63 - 1),
        )
        if _strict_int(self.accelerator_count, "accelerator_count") != 0:
            raise ValueError("Add-Thin refuses accelerator requests")
        for name in (
            "normalized_time_policy",
            "endpoint_policy",
            "noise_policy",
            "clean_intensity_policy",
            "classifier_policy",
            "reverse_policy",
            "mark_policy",
            "quote_projection_policy",
            "unsupported_mark_policy",
            "prior_quote_reset_policy",
            "equal_time_order_policy",
            "collision_policy",
            "anchor_policy",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        expected = _stable_id("add-thin-config", self.identity_payload())
        if self.config_id and self.config_id != expected:
            raise ValueError("Add-Thin config_id differs")
        object.__setattr__(self, "config_id", expected)

    @property
    def cumulative_keep_probabilities(self) -> tuple[float, ...]:
        result: list[float] = []
        cumulative = 1.0
        for item in self.step_keep_probabilities:
            cumulative *= item
            result.append(cumulative)
        return tuple(result)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "architecture": self.architecture,
            "time_bin_count": self.time_bin_count,
            "step_keep_probabilities": list(self.step_keep_probabilities),
            "cumulative_keep_probabilities": list(
                self.cumulative_keep_probabilities
            ),
            "smoothing_candidates": list(self.smoothing_candidates),
            "near_duplicate_hamming_threshold": (
                self.near_duplicate_hamming_threshold
            ),
            "base_seed": self.base_seed,
            "accelerator_count": self.accelerator_count,
            "normalized_time_policy": self.normalized_time_policy,
            "endpoint_policy": self.endpoint_policy,
            "noise_policy": self.noise_policy,
            "clean_intensity_policy": self.clean_intensity_policy,
            "classifier_policy": self.classifier_policy,
            "reverse_policy": self.reverse_policy,
            "mark_policy": self.mark_policy,
            "quote_projection_policy": self.quote_projection_policy,
            "unsupported_mark_policy": self.unsupported_mark_policy,
            "prior_quote_reset_policy": self.prior_quote_reset_policy,
            "equal_time_order_policy": self.equal_time_order_policy,
            "collision_policy": self.collision_policy,
            "anchor_policy": self.anchor_policy,
            "limits": self.limits.to_dict(),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "config_id": self.config_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> AddThinConfigV1:
        allowed = {
            "schema_version",
            "architecture",
            "time_bin_count",
            "step_keep_probabilities",
            "cumulative_keep_probabilities",
            "smoothing_candidates",
            "near_duplicate_hamming_threshold",
            "base_seed",
            "accelerator_count",
            "normalized_time_policy",
            "endpoint_policy",
            "noise_policy",
            "clean_intensity_policy",
            "classifier_policy",
            "reverse_policy",
            "mark_policy",
            "quote_projection_policy",
            "unsupported_mark_policy",
            "prior_quote_reset_policy",
            "equal_time_order_policy",
            "collision_policy",
            "anchor_policy",
            "limits",
            "config_id",
        }
        if set(data) != allowed:
            raise ValueError("Add-Thin config fields differ")
        config = cls(
            schema_version=str(data["schema_version"]),
            architecture=str(data["architecture"]),
            time_bin_count=_strict_int(
                data["time_bin_count"], "time_bin_count"
            ),
            step_keep_probabilities=tuple(
                _finite_float(item, "step_keep_probability")
                for item in _sequence(
                    data["step_keep_probabilities"],
                    "step_keep_probabilities",
                )
            ),
            smoothing_candidates=tuple(
                _finite_float(item, "smoothing_candidate")
                for item in _sequence(
                    data["smoothing_candidates"], "smoothing_candidates"
                )
            ),
            near_duplicate_hamming_threshold=_strict_int(
                data["near_duplicate_hamming_threshold"],
                "near_duplicate_hamming_threshold",
            ),
            base_seed=_strict_int(data["base_seed"], "base_seed"),
            accelerator_count=_strict_int(
                data["accelerator_count"], "accelerator_count"
            ),
            normalized_time_policy=str(data["normalized_time_policy"]),
            endpoint_policy=str(data["endpoint_policy"]),
            noise_policy=str(data["noise_policy"]),
            clean_intensity_policy=str(data["clean_intensity_policy"]),
            classifier_policy=str(data["classifier_policy"]),
            reverse_policy=str(data["reverse_policy"]),
            mark_policy=str(data["mark_policy"]),
            quote_projection_policy=str(data["quote_projection_policy"]),
            unsupported_mark_policy=str(data["unsupported_mark_policy"]),
            prior_quote_reset_policy=str(data["prior_quote_reset_policy"]),
            equal_time_order_policy=str(data["equal_time_order_policy"]),
            collision_policy=str(data["collision_policy"]),
            anchor_policy=str(data["anchor_policy"]),
            limits=AddThinResourceLimitsV1.from_dict(
                _mapping(data["limits"], "limits")
            ),
            config_id=str(data["config_id"]),
        )
        supplied_cumulative = tuple(
            _finite_float(item, "cumulative_keep_probability")
            for item in _sequence(
                data["cumulative_keep_probabilities"],
                "cumulative_keep_probabilities",
            )
        )
        if supplied_cumulative != config.cumulative_keep_probabilities:
            raise ValueError("Add-Thin cumulative schedule differs")
        return config

    @classmethod
    def from_json(cls, text: str) -> AddThinConfigV1:
        data = _json_mapping(text, 16 * 1024 * 1024)
        return cls.from_dict(data)


def default_add_thin_config() -> AddThinConfigV1:
    """Return the one fixed bounded Add-Thin challenger config."""
    return AddThinConfigV1()


@dataclass(frozen=True, slots=True)
class AddThinWindowContextV1:
    """Point-in-time context kept separate from the denoising state."""

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
    schema_version: str = ADD_THIN_WINDOW_CONTEXT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ADD_THIN_WINDOW_CONTEXT_SCHEMA_VERSION:
            raise ValueError("unsupported Add-Thin context schema")
        for name in (
            "window_id",
            "session",
            "technology_assignment_kind",
            "technology_label",
            "feed_epoch_definition_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        if self.technology_assignment_kind not in ASSIGNMENT_KINDS:
            raise ValueError("Add-Thin technology assignment is invalid")
        boundary_fields = (
            self.boundary_id,
            self.boundary_support,
            self.uncertainty_start_period,
            self.uncertainty_end_period,
        )
        if self.technology_assignment_kind == "transition":
            if self.epoch_id is not None:
                raise ValueError(
                    "Add-Thin transition context has epoch identity"
                )
            if any(item is None for item in boundary_fields):
                raise ValueError("Add-Thin transition context is incomplete")
            object.__setattr__(
                self, "boundary_id", _required_text(self.boundary_id)
            )
            object.__setattr__(
                self,
                "boundary_support",
                _finite_float(self.boundary_support, "boundary_support"),
            )
            if not 0.0 <= cast(float, self.boundary_support) <= 1.0:
                raise ValueError("Add-Thin boundary support is outside bounds")
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
                raise ValueError(
                    "stable Add-Thin epoch has transition evidence"
                )
        object.__setattr__(
            self,
            "observed_context_id",
            _optional_text(self.observed_context_id),
        )
        context_times = (
            self.observed_context_available_ns,
            self.observed_context_used_ns,
        )
        if self.observed_context_id is None:
            if any(item is not None for item in context_times):
                raise ValueError("Add-Thin context times lack content")
        else:
            if any(item is None for item in context_times):
                raise ValueError(
                    "Add-Thin observed context times are incomplete"
                )
            available = _strict_int(
                self.observed_context_available_ns,
                "observed_context_available_ns",
            )
            used = _strict_int(
                self.observed_context_used_ns, "observed_context_used_ns"
            )
            if available > used:
                raise ValueError("Add-Thin context is used before available")
            object.__setattr__(self, "observed_context_available_ns", available)
            object.__setattr__(self, "observed_context_used_ns", used)
        expected = _stable_id(
            "add-thin-window-context", self.identity_payload()
        )
        if self.context_id and self.context_id != expected:
            raise ValueError("Add-Thin context_id differs")
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
    def from_dict(cls, data: Mapping[str, Any]) -> AddThinWindowContextV1:
        allowed = {
            "schema_version",
            "window_id",
            "session",
            "technology_assignment_kind",
            "technology_label",
            "feed_epoch_definition_id",
            "epoch_id",
            "boundary_id",
            "boundary_support",
            "uncertainty_start_period",
            "uncertainty_end_period",
            "observed_context_id",
            "observed_context_available_ns",
            "observed_context_used_ns",
            "context_id",
        }
        if set(data) != allowed:
            raise ValueError("Add-Thin context fields differ")
        return cls(
            window_id=str(data["window_id"]),
            session=str(data["session"]),
            technology_assignment_kind=str(data["technology_assignment_kind"]),
            technology_label=str(data["technology_label"]),
            feed_epoch_definition_id=str(data["feed_epoch_definition_id"]),
            epoch_id=_optional_text(data["epoch_id"]),
            boundary_id=_optional_text(data["boundary_id"]),
            boundary_support=(
                None
                if data["boundary_support"] is None
                else _finite_float(data["boundary_support"], "boundary_support")
            ),
            uncertainty_start_period=_optional_text(
                data["uncertainty_start_period"]
            ),
            uncertainty_end_period=_optional_text(
                data["uncertainty_end_period"]
            ),
            observed_context_id=_optional_text(data["observed_context_id"]),
            observed_context_available_ns=(
                None
                if data["observed_context_available_ns"] is None
                else _strict_int(
                    data["observed_context_available_ns"],
                    "observed_context_available_ns",
                )
            ),
            observed_context_used_ns=(
                None
                if data["observed_context_used_ns"] is None
                else _strict_int(
                    data["observed_context_used_ns"],
                    "observed_context_used_ns",
                )
            ),
            context_id=str(data["context_id"]),
            schema_version=str(data["schema_version"]),
        )

    @classmethod
    def from_json(cls, text: str) -> AddThinWindowContextV1:
        return cls.from_dict(_json_mapping(text, 1024 * 1024))


@dataclass(frozen=True, slots=True)
class AddThinProtectedWindowV1:
    """Row-free validation/final evidence used only by leakage audit."""

    window_id: str
    role: str
    start_ns: int
    end_ns: int
    event_count: int
    event_content_sha256: str
    near_duplicate_signature: int
    context_id: str
    schema_version: str = ADD_THIN_PROTECTED_WINDOW_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ADD_THIN_PROTECTED_WINDOW_SCHEMA_VERSION:
            raise ValueError("unsupported Add-Thin protected-window schema")
        object.__setattr__(self, "window_id", _required_text(self.window_id))
        if self.role not in {"validation", "final_holdout"}:
            raise ValueError("protected Add-Thin role is invalid")
        start = _strict_int(self.start_ns, "start_ns")
        end = _strict_int(self.end_ns, "end_ns")
        if end <= start:
            raise ValueError("protected Add-Thin window bounds are invalid")
        object.__setattr__(
            self,
            "event_count",
            _bounded_int(self.event_count, "event_count", 1, 1_000_000),
        )
        object.__setattr__(
            self,
            "event_content_sha256",
            _sha256(self.event_content_sha256, "event_content_sha256"),
        )
        object.__setattr__(
            self,
            "near_duplicate_signature",
            _bounded_int(
                self.near_duplicate_signature,
                "near_duplicate_signature",
                0,
                2**64 - 1,
            ),
        )
        object.__setattr__(self, "context_id", _required_text(self.context_id))

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "window_id": self.window_id,
            "role": self.role,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "event_count": self.event_count,
            "event_content_sha256": self.event_content_sha256,
            "near_duplicate_signature": self.near_duplicate_signature,
            "context_id": self.context_id,
        }

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> AddThinProtectedWindowV1:
        if set(data) != set(cls.__dataclass_fields__):
            raise ValueError("Add-Thin protected-window fields differ")
        return cls(
            window_id=str(data["window_id"]),
            role=str(data["role"]),
            start_ns=_strict_int(data["start_ns"], "start_ns"),
            end_ns=_strict_int(data["end_ns"], "end_ns"),
            event_count=_strict_int(data["event_count"], "event_count"),
            event_content_sha256=str(data["event_content_sha256"]),
            near_duplicate_signature=_strict_int(
                data["near_duplicate_signature"], "near_duplicate_signature"
            ),
            context_id=str(data["context_id"]),
            schema_version=str(data["schema_version"]),
        )

    @classmethod
    def from_json(cls, text: str) -> AddThinProtectedWindowV1:
        return cls.from_dict(_json_mapping(text, 1024 * 1024))


@dataclass(frozen=True, slots=True)
class AddThinDatasetWindowV1:
    """Row-free train/tune window evidence."""

    window_id: str
    role: str
    start_ns: int
    end_ns: int
    event_count: int
    event_content_sha256: str
    near_duplicate_signature: int
    context_id: str
    session: str
    symbol_support: tuple[str, ...]
    mark_support: tuple[str, ...]
    evidence_id: str = ""
    schema_version: str = ADD_THIN_DATASET_WINDOW_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ADD_THIN_DATASET_WINDOW_SCHEMA_VERSION:
            raise ValueError("unsupported Add-Thin dataset-window schema")
        object.__setattr__(self, "window_id", _required_text(self.window_id))
        if self.role not in {"train", "tune"}:
            raise ValueError("Add-Thin dataset role is invalid")
        start = _strict_int(self.start_ns, "start_ns")
        end = _strict_int(self.end_ns, "end_ns")
        if end <= start:
            raise ValueError("Add-Thin dataset bounds are invalid")
        object.__setattr__(
            self,
            "event_count",
            _bounded_int(self.event_count, "event_count", 1, 1_000_000),
        )
        object.__setattr__(
            self,
            "event_content_sha256",
            _sha256(self.event_content_sha256, "event_content_sha256"),
        )
        object.__setattr__(
            self,
            "near_duplicate_signature",
            _bounded_int(
                self.near_duplicate_signature,
                "near_duplicate_signature",
                0,
                2**64 - 1,
            ),
        )
        object.__setattr__(self, "context_id", _required_text(self.context_id))
        object.__setattr__(self, "session", _required_text(self.session))
        symbols = tuple(
            sorted(
                {_required_text(item).upper() for item in self.symbol_support}
            )
        )
        marks = tuple(
            sorted({_required_text(item) for item in self.mark_support})
        )
        if not symbols or not marks:
            raise ValueError("Add-Thin dataset support is empty")
        object.__setattr__(self, "symbol_support", symbols)
        object.__setattr__(self, "mark_support", marks)
        expected = _stable_id(
            "add-thin-dataset-window", self.identity_payload()
        )
        if self.evidence_id and self.evidence_id != expected:
            raise ValueError("Add-Thin window evidence_id differs")
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
            "context_id": self.context_id,
            "session": self.session,
            "symbol_support": list(self.symbol_support),
            "mark_support": list(self.mark_support),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "evidence_id": self.evidence_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> AddThinDatasetWindowV1:
        allowed = set(cls.__dataclass_fields__)
        if set(data) != allowed:
            raise ValueError("Add-Thin dataset-window fields differ")
        return cls(
            window_id=str(data["window_id"]),
            role=str(data["role"]),
            start_ns=_strict_int(data["start_ns"], "start_ns"),
            end_ns=_strict_int(data["end_ns"], "end_ns"),
            event_count=_strict_int(data["event_count"], "event_count"),
            event_content_sha256=str(data["event_content_sha256"]),
            near_duplicate_signature=_strict_int(
                data["near_duplicate_signature"], "near_duplicate_signature"
            ),
            context_id=str(data["context_id"]),
            session=str(data["session"]),
            symbol_support=tuple(
                str(item)
                for item in _sequence(data["symbol_support"], "symbol_support")
            ),
            mark_support=tuple(
                str(item)
                for item in _sequence(data["mark_support"], "mark_support")
            ),
            evidence_id=str(data["evidence_id"]),
            schema_version=str(data["schema_version"]),
        )


@dataclass(frozen=True, slots=True)
class AddThinDatasetManifestV1:
    """Content-addressed row-free Add-Thin split and leakage evidence."""

    config_id: str
    symbols: tuple[str, ...]
    mark_vocabulary: tuple[str, ...]
    time_bin_count: int
    windows: tuple[AddThinDatasetWindowV1, ...]
    protected_windows: tuple[AddThinProtectedWindowV1, ...]
    protected_window_count: int
    exact_duplicate_count: int
    near_duplicate_collision_count: int
    interval_overlap_count: int
    dataset_id: str = ""
    schema_version: str = ADD_THIN_DATASET_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ADD_THIN_DATASET_MANIFEST_SCHEMA_VERSION:
            raise ValueError("unsupported Add-Thin dataset schema")
        object.__setattr__(self, "config_id", _required_text(self.config_id))
        symbols = tuple(
            sorted({_required_text(item).upper() for item in self.symbols})
        )
        vocabulary = tuple(
            _required_text(item) for item in self.mark_vocabulary
        )
        expected_vocabulary = tuple(
            f"{symbol}:{mark}" for symbol in symbols for mark in MARK_STATES
        )
        if not symbols or vocabulary != expected_vocabulary:
            raise ValueError("Add-Thin mark vocabulary is invalid")
        object.__setattr__(self, "symbols", symbols)
        object.__setattr__(self, "mark_vocabulary", vocabulary)
        object.__setattr__(
            self,
            "time_bin_count",
            _bounded_int(self.time_bin_count, "time_bin_count", 2, 256),
        )
        windows = tuple(
            sorted(self.windows, key=lambda item: (item.role, item.window_id))
        )
        if not windows or {item.role for item in windows} != {"train", "tune"}:
            raise ValueError("Add-Thin dataset lacks train/tune windows")
        if len({item.window_id for item in windows}) != len(windows):
            raise ValueError("Add-Thin dataset window identity is duplicated")
        object.__setattr__(self, "windows", windows)
        protected = tuple(
            sorted(
                self.protected_windows,
                key=lambda item: (item.role, item.start_ns, item.window_id),
            )
        )
        if len({item.window_id for item in protected}) != len(protected):
            raise ValueError("Add-Thin protected identity is duplicated")
        if {item.window_id for item in windows} & {
            item.window_id for item in protected
        }:
            raise ValueError("Add-Thin window identity crosses split roles")
        if self.protected_window_count != len(protected):
            raise ValueError("Add-Thin protected-window count differs")
        object.__setattr__(self, "protected_windows", protected)
        for name in (
            "protected_window_count",
            "exact_duplicate_count",
            "near_duplicate_collision_count",
            "interval_overlap_count",
        ):
            object.__setattr__(
                self,
                name,
                _bounded_int(getattr(self, name), name, 0, 1_000_000),
            )
        if any(
            getattr(self, name) != 0
            for name in (
                "exact_duplicate_count",
                "near_duplicate_collision_count",
                "interval_overlap_count",
            )
        ):
            raise ValueError("Add-Thin dataset leakage audit failed")
        expected = _stable_id("add-thin-dataset", self.identity_payload())
        if self.dataset_id and self.dataset_id != expected:
            raise ValueError("Add-Thin dataset_id differs")
        object.__setattr__(self, "dataset_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "config_id": self.config_id,
            "symbols": list(self.symbols),
            "mark_vocabulary": list(self.mark_vocabulary),
            "time_bin_count": self.time_bin_count,
            "windows": [item.to_dict() for item in self.windows],
            "protected_windows": [
                item.to_dict() for item in self.protected_windows
            ],
            "protected_window_count": self.protected_window_count,
            "exact_duplicate_count": self.exact_duplicate_count,
            "near_duplicate_collision_count": self.near_duplicate_collision_count,
            "interval_overlap_count": self.interval_overlap_count,
            "rows_inline": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "dataset_id": self.dataset_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> AddThinDatasetManifestV1:
        allowed = {
            "schema_version",
            "config_id",
            "symbols",
            "mark_vocabulary",
            "time_bin_count",
            "windows",
            "protected_windows",
            "protected_window_count",
            "exact_duplicate_count",
            "near_duplicate_collision_count",
            "interval_overlap_count",
            "rows_inline",
            "dataset_id",
        }
        if set(data) != allowed or data["rows_inline"] is not False:
            raise ValueError("Add-Thin dataset fields differ or embed rows")
        return cls(
            config_id=str(data["config_id"]),
            symbols=tuple(
                str(item) for item in _sequence(data["symbols"], "symbols")
            ),
            mark_vocabulary=tuple(
                str(item)
                for item in _sequence(
                    data["mark_vocabulary"], "mark_vocabulary"
                )
            ),
            time_bin_count=_strict_int(
                data["time_bin_count"], "time_bin_count"
            ),
            windows=tuple(
                AddThinDatasetWindowV1.from_dict(_mapping(item, "window"))
                for item in _sequence(data["windows"], "windows")
            ),
            protected_windows=tuple(
                AddThinProtectedWindowV1.from_dict(
                    _mapping(item, "protected_window")
                )
                for item in _sequence(
                    data["protected_windows"], "protected_windows"
                )
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
    def from_json(cls, text: str) -> AddThinDatasetManifestV1:
        return cls.from_dict(_json_mapping(text, 16 * 1024 * 1024))


def _finite_matrix(
    value: Sequence[Sequence[float]],
    *,
    rows: int,
    columns: int,
    name: str,
    probability: bool = False,
) -> tuple[tuple[float, ...], ...]:
    matrix = tuple(
        tuple(_finite_float(item, name) for item in row) for row in value
    )
    if len(matrix) != rows or any(len(row) != columns for row in matrix):
        raise ValueError(f"{name} shape differs")
    if probability and any(
        not 0.0 <= item <= 1.0 for row in matrix for item in row
    ):
        raise ValueError(f"{name} probability is invalid")
    if not probability and any(item <= 0.0 for row in matrix for item in row):
        raise ValueError(f"{name} intensity is invalid")
    return matrix


@dataclass(frozen=True, slots=True)
class AddThinCheckpointV1:
    """Immutable finite-bin estimator and denoising diagnostics."""

    config_id: str
    dataset_id: str
    architecture: str
    symbols: tuple[str, ...]
    mark_vocabulary: tuple[str, ...]
    time_bin_count: int
    step_keep_probabilities: tuple[float, ...]
    cumulative_keep_probabilities: tuple[float, ...]
    selected_smoothing: float
    clean_intensity: tuple[tuple[float, ...], ...]
    classifier_probabilities: tuple[tuple[float, ...], ...]
    noise_mean_event_count: float
    mean_training_window_duration_ns: float
    train_classifier_bce: float
    train_missing_poisson_nll: float
    train_objective: float
    tune_classifier_bce: float
    tune_missing_poisson_nll: float
    tune_objective: float
    baseline_tune_objective: float
    tune_count_relative_error: float
    tune_mark_l1: float
    candidate_objectives: tuple[tuple[float, float], ...]
    parameter_count: int
    parameter_bytes: int
    checkpoint_id: str = ""
    schema_version: str = ADD_THIN_CHECKPOINT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ADD_THIN_CHECKPOINT_SCHEMA_VERSION:
            raise ValueError("unsupported Add-Thin checkpoint schema")
        object.__setattr__(self, "config_id", _required_text(self.config_id))
        object.__setattr__(self, "dataset_id", _required_text(self.dataset_id))
        if self.architecture != ADD_THIN_ARCHITECTURE:
            raise ValueError("Add-Thin checkpoint architecture differs")
        symbols = tuple(
            sorted({_required_text(item).upper() for item in self.symbols})
        )
        vocabulary = tuple(
            _required_text(item) for item in self.mark_vocabulary
        )
        expected_vocabulary = tuple(
            f"{symbol}:{mark}" for symbol in symbols for mark in MARK_STATES
        )
        if not symbols or vocabulary != expected_vocabulary:
            raise ValueError("Add-Thin checkpoint vocabulary differs")
        object.__setattr__(self, "symbols", symbols)
        object.__setattr__(self, "mark_vocabulary", vocabulary)
        bins = _bounded_int(self.time_bin_count, "time_bin_count", 2, 256)
        steps = tuple(
            _finite_float(item, "step_keep_probability")
            for item in self.step_keep_probabilities
        )
        cumulative = tuple(
            _finite_float(item, "cumulative_keep_probability")
            for item in self.cumulative_keep_probabilities
        )
        if len(steps) < 2 or len(steps) != len(cumulative):
            raise ValueError("Add-Thin checkpoint schedule differs")
        running = 1.0
        expected_cumulative: list[float] = []
        for item in steps:
            if not 0.0 < item < 1.0:
                raise ValueError(
                    "Add-Thin checkpoint keep probability is invalid"
                )
            running *= item
            expected_cumulative.append(running)
        if tuple(expected_cumulative) != cumulative:
            raise ValueError("Add-Thin checkpoint cumulative schedule differs")
        object.__setattr__(self, "step_keep_probabilities", steps)
        object.__setattr__(self, "cumulative_keep_probabilities", cumulative)
        smoothing = _finite_float(self.selected_smoothing, "selected_smoothing")
        if smoothing <= 0.0:
            raise ValueError("Add-Thin checkpoint smoothing is invalid")
        object.__setattr__(self, "selected_smoothing", smoothing)
        cells = bins * len(vocabulary)
        clean = _finite_matrix(
            self.clean_intensity,
            rows=bins,
            columns=len(vocabulary),
            name="clean_intensity",
        )
        classifier = _finite_matrix(
            self.classifier_probabilities,
            rows=len(steps),
            columns=cells,
            name="classifier_probabilities",
            probability=True,
        )
        object.__setattr__(self, "clean_intensity", clean)
        object.__setattr__(self, "classifier_probabilities", classifier)
        for name in (
            "noise_mean_event_count",
            "mean_training_window_duration_ns",
        ):
            value = _finite_float(getattr(self, name), name)
            if value <= 0.0:
                raise ValueError(f"{name} must be positive")
            object.__setattr__(self, name, value)
        for name in (
            "train_classifier_bce",
            "train_missing_poisson_nll",
            "train_objective",
            "tune_classifier_bce",
            "tune_missing_poisson_nll",
            "tune_objective",
            "baseline_tune_objective",
            "tune_count_relative_error",
            "tune_mark_l1",
        ):
            value = _finite_float(getattr(self, name), name)
            if value < 0.0:
                raise ValueError(f"{name} must be nonnegative")
            object.__setattr__(self, name, value)
        if not math.isclose(
            self.train_objective,
            self.train_classifier_bce + self.train_missing_poisson_nll,
            rel_tol=1e-12,
            abs_tol=1e-12,
        ) or not math.isclose(
            self.tune_objective,
            self.tune_classifier_bce + self.tune_missing_poisson_nll,
            rel_tol=1e-12,
            abs_tol=1e-12,
        ):
            raise ValueError("Add-Thin checkpoint objective parts differ")
        objectives = tuple(
            (
                _finite_float(item[0], "candidate_smoothing"),
                _finite_float(item[1], "candidate_objective"),
            )
            for item in self.candidate_objectives
        )
        if not objectives or any(a <= 0.0 or b < 0.0 for a, b in objectives):
            raise ValueError("Add-Thin candidate objectives are invalid")
        object.__setattr__(self, "candidate_objectives", objectives)
        expected_parameter_count = bins * len(vocabulary) + len(steps) * cells
        if self.parameter_count != expected_parameter_count:
            raise ValueError("Add-Thin checkpoint parameter count differs")
        expected_parameter_bytes = len(
            canonical_contract_json(
                {
                    "clean_intensity": [list(row) for row in clean],
                    "classifier_probabilities": [
                        list(row) for row in classifier
                    ],
                }
            ).encode()
        )
        if self.parameter_bytes != expected_parameter_bytes:
            raise ValueError("Add-Thin checkpoint parameter bytes differ")
        expected = _stable_id("add-thin-checkpoint", self.identity_payload())
        if self.checkpoint_id and self.checkpoint_id != expected:
            raise ValueError("Add-Thin checkpoint_id differs")
        object.__setattr__(self, "checkpoint_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "config_id": self.config_id,
            "dataset_id": self.dataset_id,
            "architecture": self.architecture,
            "symbols": list(self.symbols),
            "mark_vocabulary": list(self.mark_vocabulary),
            "time_bin_count": self.time_bin_count,
            "step_keep_probabilities": list(self.step_keep_probabilities),
            "cumulative_keep_probabilities": list(
                self.cumulative_keep_probabilities
            ),
            "selected_smoothing": self.selected_smoothing,
            "clean_intensity": [list(row) for row in self.clean_intensity],
            "classifier_probabilities": [
                list(row) for row in self.classifier_probabilities
            ],
            "noise_mean_event_count": self.noise_mean_event_count,
            "mean_training_window_duration_ns": (
                self.mean_training_window_duration_ns
            ),
            "train_classifier_bce": self.train_classifier_bce,
            "train_missing_poisson_nll": self.train_missing_poisson_nll,
            "train_objective": self.train_objective,
            "tune_classifier_bce": self.tune_classifier_bce,
            "tune_missing_poisson_nll": self.tune_missing_poisson_nll,
            "tune_objective": self.tune_objective,
            "baseline_tune_objective": self.baseline_tune_objective,
            "tune_count_relative_error": self.tune_count_relative_error,
            "tune_mark_l1": self.tune_mark_l1,
            "candidate_objectives": [
                [smoothing, objective]
                for smoothing, objective in self.candidate_objectives
            ],
            "parameter_count": self.parameter_count,
            "parameter_bytes": self.parameter_bytes,
            "optimizer_state": None,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "checkpoint_id": self.checkpoint_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> AddThinCheckpointV1:
        allowed = {
            "schema_version",
            "config_id",
            "dataset_id",
            "architecture",
            "symbols",
            "mark_vocabulary",
            "time_bin_count",
            "step_keep_probabilities",
            "cumulative_keep_probabilities",
            "selected_smoothing",
            "clean_intensity",
            "classifier_probabilities",
            "noise_mean_event_count",
            "mean_training_window_duration_ns",
            "train_classifier_bce",
            "train_missing_poisson_nll",
            "train_objective",
            "tune_classifier_bce",
            "tune_missing_poisson_nll",
            "tune_objective",
            "baseline_tune_objective",
            "tune_count_relative_error",
            "tune_mark_l1",
            "candidate_objectives",
            "parameter_count",
            "parameter_bytes",
            "optimizer_state",
            "checkpoint_id",
        }
        if set(data) != allowed or data["optimizer_state"] is not None:
            raise ValueError("Add-Thin checkpoint fields differ")
        return cls(
            config_id=str(data["config_id"]),
            dataset_id=str(data["dataset_id"]),
            architecture=str(data["architecture"]),
            symbols=tuple(
                str(item) for item in _sequence(data["symbols"], "symbols")
            ),
            mark_vocabulary=tuple(
                str(item)
                for item in _sequence(
                    data["mark_vocabulary"], "mark_vocabulary"
                )
            ),
            time_bin_count=_strict_int(
                data["time_bin_count"], "time_bin_count"
            ),
            step_keep_probabilities=tuple(
                _finite_float(item, "step_keep_probability")
                for item in _sequence(
                    data["step_keep_probabilities"], "step_keep_probabilities"
                )
            ),
            cumulative_keep_probabilities=tuple(
                _finite_float(item, "cumulative_keep_probability")
                for item in _sequence(
                    data["cumulative_keep_probabilities"],
                    "cumulative_keep_probabilities",
                )
            ),
            selected_smoothing=_finite_float(
                data["selected_smoothing"], "selected_smoothing"
            ),
            clean_intensity=tuple(
                tuple(
                    _finite_float(item, "clean_intensity")
                    for item in _sequence(row, "clean_intensity_row")
                )
                for row in _sequence(data["clean_intensity"], "clean_intensity")
            ),
            classifier_probabilities=tuple(
                tuple(
                    _finite_float(item, "classifier_probability")
                    for item in _sequence(row, "classifier_probability_row")
                )
                for row in _sequence(
                    data["classifier_probabilities"],
                    "classifier_probabilities",
                )
            ),
            noise_mean_event_count=_finite_float(
                data["noise_mean_event_count"], "noise_mean_event_count"
            ),
            mean_training_window_duration_ns=_finite_float(
                data["mean_training_window_duration_ns"],
                "mean_training_window_duration_ns",
            ),
            train_classifier_bce=_finite_float(
                data["train_classifier_bce"], "train_classifier_bce"
            ),
            train_missing_poisson_nll=_finite_float(
                data["train_missing_poisson_nll"],
                "train_missing_poisson_nll",
            ),
            train_objective=_finite_float(
                data["train_objective"], "train_objective"
            ),
            tune_classifier_bce=_finite_float(
                data["tune_classifier_bce"], "tune_classifier_bce"
            ),
            tune_missing_poisson_nll=_finite_float(
                data["tune_missing_poisson_nll"], "tune_missing_poisson_nll"
            ),
            tune_objective=_finite_float(
                data["tune_objective"], "tune_objective"
            ),
            baseline_tune_objective=_finite_float(
                data["baseline_tune_objective"], "baseline_tune_objective"
            ),
            tune_count_relative_error=_finite_float(
                data["tune_count_relative_error"], "tune_count_relative_error"
            ),
            tune_mark_l1=_finite_float(data["tune_mark_l1"], "tune_mark_l1"),
            candidate_objectives=tuple(
                (
                    _finite_float(
                        _sequence(item, "candidate_objective")[0],
                        "candidate_smoothing",
                    ),
                    _finite_float(
                        _sequence(item, "candidate_objective")[1],
                        "candidate_objective",
                    ),
                )
                for item in _sequence(
                    data["candidate_objectives"], "candidate_objectives"
                )
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
    def from_json(cls, text: str) -> AddThinCheckpointV1:
        return cls.from_dict(_json_mapping(text, 16 * 1024 * 1024))


@dataclass(frozen=True, slots=True)
class AddThinFitResultV1:
    """Closed or fitted Add-Thin result with immutable nested artifacts."""

    config_id: str
    information_mode: InformationMode
    as_of_ns: int | None
    symbols: tuple[str, ...]
    status: AddThinFitStatus
    converged: bool
    training_window_count: int
    tuning_window_count: int
    training_event_count: int
    tuning_event_count: int
    dataset_manifest: AddThinDatasetManifestV1 | None
    checkpoint: AddThinCheckpointV1 | None
    runtime_metadata: Mapping[str, JSONValue]
    fit_wall_time_ms: int
    fit_peak_memory_bytes: int
    failure_reason: str | None = None
    fit_id: str = ""
    schema_version: str = ADD_THIN_FIT_RESULT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ADD_THIN_FIT_RESULT_SCHEMA_VERSION:
            raise ValueError("unsupported Add-Thin fit schema")
        object.__setattr__(self, "config_id", _required_text(self.config_id))
        mode = InformationMode.from_value(self.information_mode)
        object.__setattr__(self, "information_mode", mode)
        if mode is InformationMode.EX_ANTE_SIMULATION:
            if self.as_of_ns is None:
                raise ValueError("ex-ante Add-Thin fit requires as_of_ns")
            object.__setattr__(
                self, "as_of_ns", _strict_int(self.as_of_ns, "as_of_ns")
            )
        elif self.as_of_ns is not None:
            raise ValueError("ex-post Add-Thin fit forbids as_of_ns")
        symbols = tuple(
            sorted({_required_text(item).upper() for item in self.symbols})
        )
        if not symbols:
            raise ValueError("Add-Thin fit requires symbols")
        object.__setattr__(self, "symbols", symbols)
        status = AddThinFitStatus(self.status)
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
                self, name, _bounded_int(getattr(self, name), name, 0, 10**12)
            )
        metadata = dict(self.runtime_metadata)
        if (
            metadata.get("accelerator_policy") != "cpu_only"
            or metadata.get("accelerator_count") != 0
        ):
            raise ValueError("Add-Thin runtime metadata is not CPU-only")
        object.__setattr__(self, "runtime_metadata", metadata)
        if status is AddThinFitStatus.FITTED:
            if (
                not self.converged
                or self.dataset_manifest is None
                or self.checkpoint is None
            ):
                raise ValueError(
                    "fitted Add-Thin result lacks complete artifacts"
                )
            if self.failure_reason is not None:
                raise ValueError("fitted Add-Thin result has failure reason")
            if (
                self.dataset_manifest.config_id != self.config_id
                or self.checkpoint.config_id != self.config_id
                or self.checkpoint.dataset_id
                != self.dataset_manifest.dataset_id
                or self.dataset_manifest.symbols != symbols
                or self.checkpoint.symbols != symbols
            ):
                raise ValueError("Add-Thin nested artifact identities differ")
            if (
                min(
                    self.training_window_count,
                    self.tuning_window_count,
                    self.training_event_count,
                    self.tuning_event_count,
                )
                <= 0
            ):
                raise ValueError("fitted Add-Thin result lacks split support")
        else:
            if (
                self.converged
                or self.dataset_manifest is not None
                or self.checkpoint is not None
            ):
                raise ValueError("closed Add-Thin fit exposes partial model")
            object.__setattr__(
                self, "failure_reason", _required_text(self.failure_reason)
            )
        expected = _stable_id("add-thin-fit", self.identity_payload())
        if self.fit_id and self.fit_id != expected:
            raise ValueError("Add-Thin fit_id differs")
        object.__setattr__(self, "fit_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "config_id": self.config_id,
            "information_mode": self.information_mode.value,
            "as_of_ns": self.as_of_ns,
            "symbols": list(self.symbols),
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
            "failure_reason": self.failure_reason,
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
            "fit_wall_time_ms": self.fit_wall_time_ms,
            "fit_peak_memory_bytes": self.fit_peak_memory_bytes,
            "fit_id": self.fit_id,
        }

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> AddThinFitResultV1:
        allowed = {
            "schema_version",
            "config_id",
            "information_mode",
            "as_of_ns",
            "symbols",
            "status",
            "converged",
            "training_window_count",
            "tuning_window_count",
            "training_event_count",
            "tuning_event_count",
            "dataset_id",
            "checkpoint_id",
            "failure_reason",
            "dataset_manifest",
            "checkpoint",
            "runtime_metadata",
            "fit_wall_time_ms",
            "fit_peak_memory_bytes",
            "fit_id",
        }
        if set(data) != allowed:
            raise ValueError("Add-Thin fit fields differ")
        dataset = (
            None
            if data["dataset_manifest"] is None
            else AddThinDatasetManifestV1.from_dict(
                _mapping(data["dataset_manifest"], "dataset_manifest")
            )
        )
        checkpoint = (
            None
            if data["checkpoint"] is None
            else AddThinCheckpointV1.from_dict(
                _mapping(data["checkpoint"], "checkpoint")
            )
        )
        if data["dataset_id"] != (
            dataset.dataset_id if dataset is not None else None
        ) or data["checkpoint_id"] != (
            checkpoint.checkpoint_id if checkpoint is not None else None
        ):
            raise ValueError("Add-Thin nested artifact pointers differ")
        return cls(
            config_id=str(data["config_id"]),
            information_mode=InformationMode.from_value(
                str(data["information_mode"])
            ),
            as_of_ns=(
                None
                if data["as_of_ns"] is None
                else _strict_int(data["as_of_ns"], "as_of_ns")
            ),
            symbols=tuple(
                str(item) for item in _sequence(data["symbols"], "symbols")
            ),
            status=AddThinFitStatus(str(data["status"])),
            converged=_strict_bool(data["converged"], "converged"),
            training_window_count=_strict_int(
                data["training_window_count"], "training_window_count"
            ),
            tuning_window_count=_strict_int(
                data["tuning_window_count"], "tuning_window_count"
            ),
            training_event_count=_strict_int(
                data["training_event_count"], "training_event_count"
            ),
            tuning_event_count=_strict_int(
                data["tuning_event_count"], "tuning_event_count"
            ),
            dataset_manifest=dataset,
            checkpoint=checkpoint,
            runtime_metadata=cast(
                Mapping[str, JSONValue],
                dict(_mapping(data["runtime_metadata"], "runtime_metadata")),
            ),
            fit_wall_time_ms=_strict_int(
                data["fit_wall_time_ms"], "fit_wall_time_ms"
            ),
            fit_peak_memory_bytes=_strict_int(
                data["fit_peak_memory_bytes"], "fit_peak_memory_bytes"
            ),
            failure_reason=_optional_text(data["failure_reason"]),
            fit_id=str(data["fit_id"]),
            schema_version=str(data["schema_version"]),
        )

    @classmethod
    def from_json(cls, text: str) -> AddThinFitResultV1:
        return cls.from_dict(_json_mapping(text, 16 * 1024 * 1024))


@dataclass(frozen=True, slots=True)
class _MarkedRow:
    cell: int
    mark_index: int


def build_add_thin_protected_window(
    window: EventClockCalibrationWindowV1,
    context: AddThinWindowContextV1,
    *,
    role: str,
    symbols: Sequence[str],
) -> AddThinProtectedWindowV1:
    """Reduce a protected split to row-free leakage evidence."""
    if not isinstance(window, EventClockCalibrationWindowV1):
        raise TypeError(
            "Add-Thin protected window requires calibration-window v1"
        )
    if context.window_id != window.window_id:
        raise ValueError("Add-Thin protected window context differs")
    wanted = {item.upper() for item in symbols}
    events = tuple(
        item for item in window.events if item.symbol.upper() in wanted
    )
    if not events or len(events) != len(window.events):
        raise ValueError("Add-Thin protected window symbol support differs")
    return AddThinProtectedWindowV1(
        window_id=window.window_id,
        role=role,
        start_ns=window.start_ns,
        end_ns=window.end_ns,
        event_count=len(events),
        event_content_sha256=_event_content_digest(events),
        near_duplicate_signature=_near_signature(events),
        context_id=context.context_id,
    )


def _validate_calibration_window(
    window: EventClockCalibrationWindowV1,
    *,
    symbols: tuple[str, ...],
    limits: AddThinResourceLimitsV1,
) -> tuple[BenchmarkEventV1, ...]:
    events = tuple(sorted(window.events, key=_benchmark_event_key))
    if not events or len(events) > limits.max_sequence_events:
        raise AddThinFitError("Add-Thin calibration sequence size is invalid")
    if any(
        not isinstance(item, BenchmarkEventV1)
        or item.symbol not in symbols
        or not window.start_ns <= item.event_time_ns < window.end_ns
        or not all(
            math.isfinite(value) and value > 0.0
            for value in (item.bid, item.ask)
        )
        or item.ask < item.bid
        for item in events
    ):
        raise AddThinFitError("Add-Thin calibration event is outside scope")
    if {item.symbol for item in events} != set(symbols):
        raise AddThinFitError(
            "Add-Thin calibration window lacks symbol support"
        )
    return events


def _window_marks(
    events: Sequence[BenchmarkEventV1],
    *,
    vocabulary: tuple[str, ...],
    start_ns: int,
    end_ns: int,
    bins: int,
) -> tuple[_MarkedRow, ...]:
    vocabulary_index = {value: index for index, value in enumerate(vocabulary)}
    previous: dict[str, tuple[float, float]] = {}
    rows: list[_MarkedRow] = []
    duration = end_ns - start_ns
    for event in sorted(events, key=_benchmark_event_key):
        mark = _event_mark(event, previous.get(event.symbol))
        previous[event.symbol] = (event.bid, event.ask)
        joint = f"{event.symbol}:{mark}"
        try:
            mark_index = vocabulary_index[joint]
        except KeyError as err:
            raise AddThinFitError(
                "Add-Thin encountered an unknown mark"
            ) from err
        offset = event.event_time_ns - start_ns
        bin_index = min(bins - 1, (offset * bins) // duration)
        rows.append(
            _MarkedRow(
                cell=int(bin_index) * len(vocabulary) + mark_index,
                mark_index=mark_index,
            )
        )
    return tuple(rows)


def _dataset_window(
    window: EventClockCalibrationWindowV1,
    context: AddThinWindowContextV1,
    *,
    role: str,
    symbols: tuple[str, ...],
) -> AddThinDatasetWindowV1:
    events = tuple(sorted(window.events, key=_benchmark_event_key))
    previous: dict[str, tuple[float, float]] = {}
    support: set[str] = set()
    for event in events:
        mark = _event_mark(event, previous.get(event.symbol))
        previous[event.symbol] = (event.bid, event.ask)
        support.add(f"{event.symbol}:{mark}")
    return AddThinDatasetWindowV1(
        window_id=window.window_id,
        role=role,
        start_ns=window.start_ns,
        end_ns=window.end_ns,
        event_count=len(events),
        event_content_sha256=_event_content_digest(events),
        near_duplicate_signature=_near_signature(events),
        context_id=context.context_id,
        session=context.session,
        symbol_support=symbols,
        mark_support=tuple(sorted(support)),
    )


def _hamming_distance(left: int, right: int) -> int:
    return (left ^ right).bit_count()


def _build_dataset(
    config: AddThinConfigV1,
    windows: Sequence[EventClockCalibrationWindowV1],
    contexts: Sequence[AddThinWindowContextV1],
    protected_windows: Sequence[AddThinProtectedWindowV1],
    *,
    symbols: tuple[str, ...],
    information_mode: InformationMode,
    as_of_ns: int | None,
) -> tuple[
    AddThinDatasetManifestV1,
    tuple[tuple[AddThinDatasetWindowV1, EventClockCalibrationWindowV1], ...],
]:
    if (
        len(windows) != len(contexts)
        or len(windows) > config.limits.max_fit_windows
    ):
        raise AddThinFitError(
            "Add-Thin calibration window/context count differs"
        )
    context_by_window = {item.window_id: item for item in contexts}
    if len(context_by_window) != len(contexts):
        raise AddThinFitError("Add-Thin context identity is duplicated")
    ordered_windows = tuple(
        sorted(windows, key=lambda item: (item.start_ns, item.window_id))
    )
    if any(item.window_id not in context_by_window for item in ordered_windows):
        raise AddThinFitError("Add-Thin calibration context is absent")
    by_session: dict[str, list[EventClockCalibrationWindowV1]] = defaultdict(
        list
    )
    for window in ordered_windows:
        context = context_by_window[window.window_id]
        by_session[context.session].append(window)
    if len(by_session) != 3 or any(
        len(items) != 2 for items in by_session.values()
    ):
        raise AddThinFitError(
            "Add-Thin split requires exactly two windows for each of three sessions"
        )
    roles: dict[str, str] = {}
    for items in by_session.values():
        ranked = sorted(items, key=lambda item: (item.start_ns, item.window_id))
        roles[ranked[0].window_id] = "train"
        roles[ranked[1].window_id] = "tune"
    paired: list[
        tuple[AddThinDatasetWindowV1, EventClockCalibrationWindowV1]
    ] = []
    total_events = 0
    for window in ordered_windows:
        events = _validate_calibration_window(
            window, symbols=symbols, limits=config.limits
        )
        total_events += len(events)
        if total_events > config.limits.max_fit_events:
            raise AddThinFitError("Add-Thin fit event limit exceeded")
        context = context_by_window[window.window_id]
        if information_mode is InformationMode.EX_ANTE_SIMULATION:
            if as_of_ns is None:
                raise AddThinFitError("ex-ante Add-Thin fit requires as_of_ns")
            if window.end_ns > as_of_ns:
                raise AddThinFitError(
                    "future events entered ex-ante Add-Thin fit"
                )
            if (
                context.observed_context_available_ns is not None
                and context.observed_context_available_ns > as_of_ns
            ):
                raise AddThinFitError(
                    "future context entered ex-ante Add-Thin fit"
                )
        evidence = _dataset_window(
            window,
            context,
            role=roles[window.window_id],
            symbols=symbols,
        )
        paired.append((evidence, window))
    evidence_all: list[AddThinDatasetWindowV1 | AddThinProtectedWindowV1] = [
        item[0] for item in paired
    ] + list(protected_windows)
    if len({item.window_id for item in evidence_all}) != len(evidence_all):
        raise AddThinFitError("Add-Thin window identity crosses split roles")
    exact = 0
    near = 0
    overlaps = 0
    for index, left in enumerate(evidence_all):
        for right in evidence_all[index + 1 :]:
            if left.event_content_sha256 == right.event_content_sha256:
                exact += 1
            if (
                _hamming_distance(
                    left.near_duplicate_signature,
                    right.near_duplicate_signature,
                )
                <= config.near_duplicate_hamming_threshold
            ):
                near += 1
            if max(left.start_ns, right.start_ns) < min(
                left.end_ns, right.end_ns
            ):
                overlaps += 1
    vocabulary = tuple(
        f"{symbol}:{mark}" for symbol in symbols for mark in MARK_STATES
    )
    manifest = AddThinDatasetManifestV1(
        config_id=config.config_id,
        symbols=symbols,
        mark_vocabulary=vocabulary,
        time_bin_count=config.time_bin_count,
        windows=tuple(item[0] for item in paired),
        protected_windows=tuple(protected_windows),
        protected_window_count=len(protected_windows),
        exact_duplicate_count=exact,
        near_duplicate_collision_count=near,
        interval_overlap_count=overlaps,
    )
    return manifest, tuple(paired)


def _poisson(
    mean: float,
    rng: random.Random,
    *,
    work: list[int],
    work_limit: int,
) -> int:
    """Sample an exact Poisson variate by independent bounded chunks."""
    mean = _finite_float(mean, "Poisson mean")
    if mean < 0.0:
        raise ValueError("Poisson mean must be nonnegative")
    total = 0
    remaining = mean
    while remaining > 0.0:
        chunk = min(remaining, 20.0)
        threshold = math.exp(-chunk)
        product = 1.0
        draws = 0
        while product > threshold:
            work[0] += 1
            if work[0] > work_limit:
                raise AddThinFitError("Add-Thin Poisson work limit exceeded")
            product *= max(rng.random(), 1e-300)
            draws += 1
        total += draws - 1
        remaining -= chunk
    return total


def _semantic_seed(payload: Mapping[str, JSONValue]) -> int:
    digest = hashlib.sha256(canonical_contract_json(payload).encode()).digest()
    return int.from_bytes(digest[:8], "big")


def _clean_intensity(
    rows_by_window: Sequence[Sequence[_MarkedRow]],
    *,
    cells: int,
    smoothing: float,
) -> tuple[float, ...]:
    counts = [0] * cells
    total = 0
    for rows in rows_by_window:
        total += len(rows)
        for row in rows:
            counts[row.cell] += 1
    mean_count = total / len(rows_by_window)
    denominator = total + smoothing * cells
    return tuple(
        mean_count * (count + smoothing) / denominator for count in counts
    )


def _classifier_tables(
    rates: Sequence[float],
    *,
    noise_mean: float,
    cumulative: Sequence[float],
) -> tuple[tuple[float, ...], ...]:
    noise_cell = noise_mean / len(rates)
    tables: list[tuple[float, ...]] = []
    for keep in cumulative:
        row: list[float] = []
        for rate in rates:
            denominator = keep * rate + (1.0 - keep) * noise_cell
            probability = (
                keep * rate / denominator if denominator > 0.0 else 0.5
            )
            row.append(min(1.0 - 1e-12, max(1e-12, probability)))
        tables.append(tuple(row))
    return tuple(tables)


def _forward_corrupt_cells(
    clean_cells: Sequence[int],
    *,
    keep_probability: float,
    noise_mean: float,
    cell_count: int,
    rng: random.Random,
    work: list[int],
    limits: AddThinResourceLimitsV1,
) -> tuple[tuple[int, ...], tuple[int, ...], tuple[int, ...]]:
    """Thin clean cells and independently superpose HPP-noise cells."""
    keep = _finite_float(keep_probability, "keep_probability")
    if not 0.0 < keep < 1.0 or cell_count <= 0:
        raise AddThinFitError("Add-Thin forward-corruption inputs differ")
    retained: list[int] = []
    missing: list[int] = []
    for cell in clean_cells:
        if not 0 <= cell < cell_count:
            raise AddThinFitError("Add-Thin clean cell is outside support")
        (retained if rng.random() < keep else missing).append(cell)
    noise_count = _poisson(
        (1.0 - keep) * noise_mean,
        rng,
        work=work,
        work_limit=limits.max_poisson_draw_work,
    )
    if len(clean_cells) + noise_count > limits.max_corruption_points:
        raise AddThinFitError("Add-Thin corruption point limit exceeded")
    noise = tuple(rng.randrange(cell_count) for _ in range(noise_count))
    return tuple(retained), tuple(missing), noise


def _reverse_coefficients(
    config: AddThinConfigV1, step_index: int
) -> tuple[float, float, float]:
    """Return the paper's C, D, and E coefficients for n -> n-1."""
    if not 1 <= step_index < len(config.step_keep_probabilities):
        raise ValueError("Add-Thin reverse step index is invalid")
    alpha_n = config.step_keep_probabilities[step_index]
    bar_n = config.cumulative_keep_probabilities[step_index]
    bar_previous = config.cumulative_keep_probabilities[step_index - 1]
    return (
        (bar_previous - bar_n) / (1.0 - bar_n),
        (1.0 - bar_previous) * (1.0 - alpha_n),
        (alpha_n - bar_n) / (1.0 - bar_n),
    )


def _evaluate_estimator(
    rows_by_window: Sequence[Sequence[_MarkedRow]],
    *,
    rates: Sequence[float],
    classifier: Sequence[Sequence[float]],
    noise_mean: float,
    cumulative: Sequence[float],
    seed_key: str,
    limits: AddThinResourceLimitsV1,
) -> tuple[float, float, float]:
    bce_total = 0.0
    bce_count = 0
    nll_total = 0.0
    nll_count = 0
    work = [0]
    for window_index, rows in enumerate(rows_by_window):
        for step_index, keep in enumerate(cumulative):
            rng = random.Random(
                _semantic_seed(
                    {
                        "seed_key": seed_key,
                        "window_index": window_index,
                        "step_index": step_index,
                    }
                )
            )
            retained_cells, missing_cells, noise_cells = _forward_corrupt_cells(
                tuple(item.cell for item in rows),
                keep_probability=keep,
                noise_mean=noise_mean,
                cell_count=len(rates),
                rng=rng,
                work=work,
                limits=limits,
            )
            missing = Counter(missing_cells)
            for cell in retained_cells:
                probability = classifier[step_index][cell]
                bce_total -= math.log(probability)
                bce_count += 1
            for cell in noise_cells:
                probability = classifier[step_index][cell]
                bce_total -= math.log1p(-probability)
                bce_count += 1
            for cell in range(len(rates)):
                observed = missing[cell]
                mean = max(1e-12, (1.0 - keep) * rates[cell])
                nll_total += (
                    mean - observed * math.log(mean) + math.lgamma(observed + 1)
                )
                nll_count += 1
    bce = bce_total / max(1, bce_count)
    nll = nll_total / max(1, nll_count)
    objective = bce + nll
    if not all(
        math.isfinite(item) and item >= 0.0 for item in (bce, nll, objective)
    ):
        raise AddThinFitError("Add-Thin objective is non-finite")
    return bce, nll, objective


def _tune_shape_metrics(
    rows_by_window: Sequence[Sequence[_MarkedRow]],
    rates: Sequence[float],
    *,
    mark_count: int,
) -> tuple[float, float]:
    predicted_count = sum(rates)
    count_errors = [
        abs(predicted_count - len(rows)) / max(1, len(rows))
        for rows in rows_by_window
    ]
    observed_marks = [0] * mark_count
    for rows in rows_by_window:
        for row in rows:
            observed_marks[row.mark_index] += 1
    predicted_marks = [0.0] * mark_count
    for cell, rate in enumerate(rates):
        predicted_marks[cell % mark_count] += rate
    observed_total = max(1, sum(observed_marks))
    predicted_total = max(1e-12, sum(predicted_marks))
    mark_l1 = sum(
        abs(observed / observed_total - predicted / predicted_total)
        for observed, predicted in zip(
            observed_marks, predicted_marks, strict=True
        )
    )
    return sum(count_errors) / len(count_errors), mark_l1


def _fit_checkpoint(
    config: AddThinConfigV1,
    dataset: AddThinDatasetManifestV1,
    paired: Sequence[
        tuple[AddThinDatasetWindowV1, EventClockCalibrationWindowV1]
    ],
) -> AddThinCheckpointV1:
    vocabulary = dataset.mark_vocabulary
    training_rows: list[tuple[_MarkedRow, ...]] = []
    tuning_rows: list[tuple[_MarkedRow, ...]] = []
    training_durations: list[int] = []
    for evidence, window in paired:
        rows = _window_marks(
            window.events,
            vocabulary=vocabulary,
            start_ns=window.start_ns,
            end_ns=window.end_ns,
            bins=config.time_bin_count,
        )
        if evidence.role == "train":
            training_rows.append(rows)
            training_durations.append(window.end_ns - window.start_ns)
        else:
            tuning_rows.append(rows)
    if not training_rows or not tuning_rows:
        raise AddThinFitError("Add-Thin fit lacks train/tune rows")
    noise_mean = sum(len(rows) for rows in training_rows) / len(training_rows)
    cells = config.time_bin_count * len(vocabulary)
    candidate_results: list[
        tuple[
            float,
            float,
            tuple[float, ...],
            tuple[tuple[float, ...], ...],
        ]
    ] = []
    for smoothing in config.smoothing_candidates:
        rates = _clean_intensity(
            training_rows, cells=cells, smoothing=smoothing
        )
        classifier = _classifier_tables(
            rates,
            noise_mean=noise_mean,
            cumulative=config.cumulative_keep_probabilities,
        )
        _, _, objective = _evaluate_estimator(
            tuning_rows,
            rates=rates,
            classifier=classifier,
            noise_mean=noise_mean,
            cumulative=config.cumulative_keep_probabilities,
            seed_key=f"tune|{dataset.dataset_id}",
            limits=config.limits,
        )
        candidate_results.append((objective, smoothing, rates, classifier))
    candidate_results.sort(key=lambda item: (item[0], item[1]))
    tune_objective, smoothing, rates, classifier = candidate_results[0]
    train_bce, train_nll, train_objective = _evaluate_estimator(
        training_rows,
        rates=rates,
        classifier=classifier,
        noise_mean=noise_mean,
        cumulative=config.cumulative_keep_probabilities,
        seed_key=f"train|{dataset.dataset_id}",
        limits=config.limits,
    )
    tune_bce, tune_nll, verified_tune_objective = _evaluate_estimator(
        tuning_rows,
        rates=rates,
        classifier=classifier,
        noise_mean=noise_mean,
        cumulative=config.cumulative_keep_probabilities,
        seed_key=f"tune|{dataset.dataset_id}",
        limits=config.limits,
    )
    if verified_tune_objective != tune_objective:
        raise AddThinFitError("Add-Thin tuning objective replay differs")
    uniform_rates = tuple(noise_mean / cells for _ in range(cells))
    uniform_classifier = _classifier_tables(
        uniform_rates,
        noise_mean=noise_mean,
        cumulative=config.cumulative_keep_probabilities,
    )
    _, _, baseline_tune = _evaluate_estimator(
        tuning_rows,
        rates=uniform_rates,
        classifier=uniform_classifier,
        noise_mean=noise_mean,
        cumulative=config.cumulative_keep_probabilities,
        seed_key=f"tune|{dataset.dataset_id}",
        limits=config.limits,
    )
    count_error, mark_l1 = _tune_shape_metrics(
        tuning_rows, rates, mark_count=len(vocabulary)
    )
    diagnostic_count = (
        len(candidate_results) + len(config.step_keep_probabilities) + 8
    )
    if diagnostic_count > config.limits.max_diagnostics:
        raise AddThinFitError("Add-Thin diagnostic count exceeds limit")
    clean_matrix = tuple(
        tuple(
            rates[bin_index * len(vocabulary) + mark_index]
            for mark_index in range(len(vocabulary))
        )
        for bin_index in range(config.time_bin_count)
    )
    parameter_count = cells + len(classifier) * cells
    parameter_bytes = len(
        canonical_contract_json(
            {
                "clean_intensity": [list(row) for row in clean_matrix],
                "classifier_probabilities": [list(row) for row in classifier],
            }
        ).encode()
    )
    if parameter_count > config.limits.max_parameter_count:
        raise AddThinFitError("Add-Thin parameter count exceeds limit")
    checkpoint = AddThinCheckpointV1(
        config_id=config.config_id,
        dataset_id=dataset.dataset_id,
        architecture=config.architecture,
        symbols=dataset.symbols,
        mark_vocabulary=vocabulary,
        time_bin_count=config.time_bin_count,
        step_keep_probabilities=config.step_keep_probabilities,
        cumulative_keep_probabilities=config.cumulative_keep_probabilities,
        selected_smoothing=smoothing,
        clean_intensity=clean_matrix,
        classifier_probabilities=classifier,
        noise_mean_event_count=noise_mean,
        mean_training_window_duration_ns=(
            sum(training_durations) / len(training_durations)
        ),
        train_classifier_bce=train_bce,
        train_missing_poisson_nll=train_nll,
        train_objective=train_objective,
        tune_classifier_bce=tune_bce,
        tune_missing_poisson_nll=tune_nll,
        tune_objective=tune_objective,
        baseline_tune_objective=baseline_tune,
        tune_count_relative_error=count_error,
        tune_mark_l1=mark_l1,
        candidate_objectives=tuple(
            (candidate_smoothing, objective)
            for objective, candidate_smoothing, _, _ in sorted(
                candidate_results, key=lambda item: item[1]
            )
        ),
        parameter_count=parameter_count,
        parameter_bytes=parameter_bytes,
    )
    if len(checkpoint.to_json().encode()) > config.limits.max_checkpoint_bytes:
        raise AddThinFitError("Add-Thin checkpoint exceeds byte limit")
    return checkpoint


def fit_add_thin_challenger(
    config: AddThinConfigV1,
    windows: Sequence[EventClockCalibrationWindowV1],
    *,
    window_contexts: Sequence[AddThinWindowContextV1],
    protected_windows: Sequence[AddThinProtectedWindowV1] = (),
    information_mode: InformationMode,
    as_of_ns: int | None = None,
) -> AddThinFitResultV1:
    """Fit the bounded train-only Add-Thin table estimator."""
    if not isinstance(config, AddThinConfigV1):
        raise TypeError("Add-Thin fit requires a v1 config")
    started = time.perf_counter()
    before_peak = peak_rss_bytes()
    mode = InformationMode.from_value(information_mode)
    raw_windows = tuple(windows)
    symbols = tuple(
        sorted(
            {
                item.symbol
                for window in raw_windows
                for item in window.events
                if isinstance(item, BenchmarkEventV1)
            }
        )
    )
    result_symbols = symbols or ("UNAVAILABLE",)
    training_windows = 0
    tuning_windows = 0
    training_events = 0
    tuning_events = 0
    try:
        if mode is InformationMode.EX_ANTE_SIMULATION and as_of_ns is None:
            raise AddThinFitError("ex-ante Add-Thin fit requires as_of_ns")
        if (
            mode is InformationMode.EX_POST_RECONSTRUCTION
            and as_of_ns is not None
        ):
            raise AddThinFitError("ex-post Add-Thin fit forbids as_of_ns")
        if not raw_windows or not symbols:
            raise AddThinFitError("Add-Thin calibration support is empty")
        if len(symbols) * len(MARK_STATES) > config.limits.max_mark_count:
            raise AddThinFitError("Add-Thin mark count exceeds limit")
        estimated_memory = (
            1024 * 1024
            + sum(len(item.events) for item in raw_windows) * 256
            + config.time_bin_count * len(symbols) * len(MARK_STATES) * 512
        )
        if estimated_memory > config.limits.max_fit_memory_bytes:
            raise AddThinFitError("Add-Thin fit memory preflight exceeded")
        dataset, paired = _build_dataset(
            config,
            raw_windows,
            tuple(window_contexts),
            tuple(protected_windows),
            symbols=symbols,
            information_mode=mode,
            as_of_ns=as_of_ns,
        )
        training_windows = sum(item.role == "train" for item, _ in paired)
        tuning_windows = sum(item.role == "tune" for item, _ in paired)
        training_events = sum(
            item.event_count for item, _ in paired if item.role == "train"
        )
        tuning_events = sum(
            item.event_count for item, _ in paired if item.role == "tune"
        )
        checkpoint = _fit_checkpoint(config, dataset, paired)
        elapsed_ms = round((time.perf_counter() - started) * 1000)
        if elapsed_ms > config.limits.max_fit_wall_time_ms:
            raise AddThinFitError("Add-Thin fit wall-time limit exceeded")
        measured_peak = max(0, peak_rss_bytes() - before_peak)
        if measured_peak > config.limits.max_fit_memory_bytes:
            raise AddThinFitError("Add-Thin measured fit memory exceeded")
        result = AddThinFitResultV1(
            config_id=config.config_id,
            information_mode=mode,
            as_of_ns=as_of_ns,
            symbols=symbols,
            status=AddThinFitStatus.FITTED,
            converged=True,
            training_window_count=training_windows,
            tuning_window_count=tuning_windows,
            training_event_count=training_events,
            tuning_event_count=tuning_events,
            dataset_manifest=dataset,
            checkpoint=checkpoint,
            runtime_metadata=_runtime_metadata(),
            fit_wall_time_ms=elapsed_ms,
            fit_peak_memory_bytes=measured_peak,
        )
        AddThinFitResultV1.from_json(result.to_json())
        return result
    except (
        AddThinFitError,
        ArithmeticError,
        KeyError,
        TypeError,
        ValueError,
    ) as err:
        return AddThinFitResultV1(
            config_id=config.config_id,
            information_mode=mode,
            as_of_ns=as_of_ns,
            symbols=result_symbols,
            status=AddThinFitStatus.REFUSED,
            converged=False,
            training_window_count=training_windows,
            tuning_window_count=tuning_windows,
            training_event_count=training_events,
            tuning_event_count=tuning_events,
            dataset_manifest=None,
            checkpoint=None,
            runtime_metadata=_runtime_metadata(),
            fit_wall_time_ms=round((time.perf_counter() - started) * 1000),
            fit_peak_memory_bytes=max(0, peak_rss_bytes() - before_peak),
            failure_reason=f"fit_refused:{type(err).__name__}:{err}",
        )


@dataclass(frozen=True, slots=True)
class AddThinStepEvidenceV1:
    """Cardinality accounting for one reverse denoising step."""

    step_index: int
    input_count: int
    b_count: int
    c_count: int
    d_count: int
    e_count: int
    thinned_count: int
    collision_count: int
    output_count: int
    schema_version: str = ADD_THIN_STEP_EVIDENCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ADD_THIN_STEP_EVIDENCE_SCHEMA_VERSION:
            raise ValueError("unsupported Add-Thin step-evidence schema")
        for name in (
            "step_index",
            "input_count",
            "b_count",
            "c_count",
            "d_count",
            "e_count",
            "thinned_count",
            "collision_count",
            "output_count",
        ):
            object.__setattr__(
                self, name, _bounded_int(getattr(self, name), name, 0, 10**9)
            )
        if self.b_count + self.e_count + self.thinned_count != self.input_count:
            raise ValueError("Add-Thin step input accounting differs")
        if (
            self.b_count + self.c_count + self.d_count + self.e_count
            != self.output_count
        ):
            raise ValueError("Add-Thin step output accounting differs")

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "step_index": self.step_index,
            "input_count": self.input_count,
            "b_count": self.b_count,
            "c_count": self.c_count,
            "d_count": self.d_count,
            "e_count": self.e_count,
            "thinned_count": self.thinned_count,
            "collision_count": self.collision_count,
            "output_count": self.output_count,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> AddThinStepEvidenceV1:
        if set(data) != set(cls.__dataclass_fields__):
            raise ValueError("Add-Thin step-evidence fields differ")
        return cls(
            step_index=_strict_int(data["step_index"], "step_index"),
            input_count=_strict_int(data["input_count"], "input_count"),
            b_count=_strict_int(data["b_count"], "b_count"),
            c_count=_strict_int(data["c_count"], "c_count"),
            d_count=_strict_int(data["d_count"], "d_count"),
            e_count=_strict_int(data["e_count"], "e_count"),
            thinned_count=_strict_int(data["thinned_count"], "thinned_count"),
            collision_count=_strict_int(
                data["collision_count"], "collision_count"
            ),
            output_count=_strict_int(data["output_count"], "output_count"),
            schema_version=str(data["schema_version"]),
        )


@dataclass(frozen=True, slots=True)
class AddThinGenerationLineageV1:
    """Denoising and anchor lineage for one emitted proposal."""

    source_event_id: str
    origin: str
    created_step: int
    survival_steps: int
    final_survival: bool
    time_bin: int
    destination_symbol: str
    transition_mark: str
    mark_probability: float
    bin_intensity: float
    parent_point_id: str | None
    left_anchor_event_id: str
    right_anchor_event_id: str
    anchor_interval_id: str
    lineage_id: str = ""
    schema_version: str = ADD_THIN_GENERATION_LINEAGE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ADD_THIN_GENERATION_LINEAGE_SCHEMA_VERSION:
            raise ValueError("unsupported Add-Thin generation-lineage schema")
        for name in (
            "source_event_id",
            "origin",
            "destination_symbol",
            "transition_mark",
            "left_anchor_event_id",
            "right_anchor_event_id",
            "anchor_interval_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self, "parent_point_id", _optional_text(self.parent_point_id)
        )
        if not _strict_bool(self.final_survival, "final_survival"):
            raise ValueError(
                "emitted Add-Thin lineage must survive the final step"
            )
        for name in ("created_step", "survival_steps", "time_bin"):
            object.__setattr__(
                self, name, _bounded_int(getattr(self, name), name, 0, 10**6)
            )
        probability = _finite_float(self.mark_probability, "mark_probability")
        intensity = _finite_float(self.bin_intensity, "bin_intensity")
        if not 0.0 <= probability <= 1.0 or intensity <= 0.0:
            raise ValueError(
                "Add-Thin lineage probability/intensity is invalid"
            )
        object.__setattr__(self, "mark_probability", probability)
        object.__setattr__(self, "bin_intensity", intensity)
        expected = _stable_id(
            "add-thin-generation-lineage", self.identity_payload()
        )
        if self.lineage_id and self.lineage_id != expected:
            raise ValueError("Add-Thin generation lineage_id differs")
        object.__setattr__(self, "lineage_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "source_event_id": self.source_event_id,
            "origin": self.origin,
            "created_step": self.created_step,
            "survival_steps": self.survival_steps,
            "final_survival": self.final_survival,
            "time_bin": self.time_bin,
            "destination_symbol": self.destination_symbol,
            "transition_mark": self.transition_mark,
            "mark_probability": self.mark_probability,
            "bin_intensity": self.bin_intensity,
            "parent_point_id": self.parent_point_id,
            "left_anchor_event_id": self.left_anchor_event_id,
            "right_anchor_event_id": self.right_anchor_event_id,
            "anchor_interval_id": self.anchor_interval_id,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "lineage_id": self.lineage_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> AddThinGenerationLineageV1:
        if set(data) != set(cls.__dataclass_fields__):
            raise ValueError("Add-Thin generation-lineage fields differ")
        return cls(
            source_event_id=str(data["source_event_id"]),
            origin=str(data["origin"]),
            created_step=_strict_int(data["created_step"], "created_step"),
            survival_steps=_strict_int(
                data["survival_steps"], "survival_steps"
            ),
            final_survival=_strict_bool(
                data["final_survival"], "final_survival"
            ),
            time_bin=_strict_int(data["time_bin"], "time_bin"),
            destination_symbol=str(data["destination_symbol"]),
            transition_mark=str(data["transition_mark"]),
            mark_probability=_finite_float(
                data["mark_probability"], "mark_probability"
            ),
            bin_intensity=_finite_float(data["bin_intensity"], "bin_intensity"),
            parent_point_id=_optional_text(data["parent_point_id"]),
            left_anchor_event_id=str(data["left_anchor_event_id"]),
            right_anchor_event_id=str(data["right_anchor_event_id"]),
            anchor_interval_id=str(data["anchor_interval_id"]),
            lineage_id=str(data["lineage_id"]),
            schema_version=str(data["schema_version"]),
        )

    @classmethod
    def from_json(cls, text: str) -> AddThinGenerationLineageV1:
        return cls.from_dict(_json_mapping(text, 1024 * 1024))


@dataclass(frozen=True, slots=True)
class AddThinGenerationEvidenceV1:
    """All-or-nothing generation identity, cardinality, and resource record."""

    config_id: str
    fit_id: str
    dataset_id: str
    checkpoint_id: str
    window_id: str
    window_context_id: str | None
    ensemble_member_id: str
    status: AddThinGenerationStatus
    attempted: bool
    input_event_count: int
    history_event_count: int
    history_conditioning_scale: float
    initial_noise_count: int
    final_point_count: int
    generated_event_count: int
    skipped_unsupported_count: int
    collision_count: int
    poisson_draw_work: int
    semantic_seed: int | None
    input_anchor_sha256: str | None
    input_event_content_sha256: str | None
    history_content_sha256: str | None
    window_context_sha256: str | None
    lineage_content_sha256: str | None
    step_evidence: tuple[AddThinStepEvidenceV1, ...]
    parameter_bytes: int
    wall_time_ms: int
    peak_memory_bytes: int
    failure_reason: str | None = None
    evidence_id: str = ""
    schema_version: str = ADD_THIN_GENERATION_EVIDENCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ADD_THIN_GENERATION_EVIDENCE_SCHEMA_VERSION:
            raise ValueError("unsupported Add-Thin generation-evidence schema")
        for name in (
            "config_id",
            "fit_id",
            "dataset_id",
            "checkpoint_id",
            "window_id",
            "ensemble_member_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self, "window_context_id", _optional_text(self.window_context_id)
        )
        status = AddThinGenerationStatus(self.status)
        object.__setattr__(self, "status", status)
        if not _strict_bool(self.attempted, "attempted"):
            raise ValueError("Add-Thin evidence must represent an attempt")
        for name in (
            "input_event_count",
            "history_event_count",
            "initial_noise_count",
            "final_point_count",
            "generated_event_count",
            "skipped_unsupported_count",
            "collision_count",
            "poisson_draw_work",
            "parameter_bytes",
            "wall_time_ms",
            "peak_memory_bytes",
        ):
            object.__setattr__(
                self, name, _bounded_int(getattr(self, name), name, 0, 10**12)
            )
        if self.semantic_seed is not None:
            object.__setattr__(
                self,
                "semantic_seed",
                _bounded_int(self.semantic_seed, "semantic_seed", 0, 2**64 - 1),
            )
        for name in (
            "input_anchor_sha256",
            "input_event_content_sha256",
            "history_content_sha256",
            "window_context_sha256",
            "lineage_content_sha256",
        ):
            value = getattr(self, name)
            object.__setattr__(
                self, name, None if value is None else _sha256(value, name)
            )
        steps = tuple(self.step_evidence)
        if len({item.step_index for item in steps}) != len(steps):
            raise ValueError("Add-Thin step evidence is duplicated")
        object.__setattr__(self, "step_evidence", steps)
        history_scale = _finite_float(
            self.history_conditioning_scale,
            "history_conditioning_scale",
        )
        if not 0.5 <= history_scale <= 2.0:
            raise ValueError("Add-Thin history conditioning scale differs")
        object.__setattr__(self, "history_conditioning_scale", history_scale)
        if status in {
            AddThinGenerationStatus.GENERATED,
            AddThinGenerationStatus.EMPTY,
        }:
            if self.failure_reason is not None:
                raise ValueError(
                    "successful Add-Thin evidence has failure reason"
                )
            if any(
                value is None
                for value in (
                    self.semantic_seed,
                    self.input_anchor_sha256,
                    self.input_event_content_sha256,
                    self.history_content_sha256,
                    self.window_context_sha256,
                    self.lineage_content_sha256,
                )
            ):
                raise ValueError(
                    "successful Add-Thin evidence lacks identities"
                )
            if self.window_context_id is None or not steps:
                raise ValueError(
                    "successful Add-Thin evidence lacks context/steps"
                )
            if self.generated_event_count > self.final_point_count:
                raise ValueError("Add-Thin emitted count exceeds final points")
        else:
            object.__setattr__(
                self, "failure_reason", _required_text(self.failure_reason)
            )
            if (
                self.generated_event_count != 0
                or self.lineage_content_sha256 is not None
            ):
                raise ValueError(
                    "closed Add-Thin evidence exposes partial output"
                )
        if (
            status is AddThinGenerationStatus.GENERATED
            and self.generated_event_count == 0
        ):
            raise ValueError("generated Add-Thin evidence has no emitted rows")
        if (
            status is AddThinGenerationStatus.EMPTY
            and self.generated_event_count != 0
        ):
            raise ValueError("empty Add-Thin evidence contains emitted rows")
        expected = _stable_id(
            "add-thin-generation-evidence", self.identity_payload()
        )
        if self.evidence_id and self.evidence_id != expected:
            raise ValueError("Add-Thin generation evidence_id differs")
        object.__setattr__(self, "evidence_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "config_id": self.config_id,
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
            "history_conditioning_scale": self.history_conditioning_scale,
            "initial_noise_count": self.initial_noise_count,
            "final_point_count": self.final_point_count,
            "generated_event_count": self.generated_event_count,
            "skipped_unsupported_count": self.skipped_unsupported_count,
            "collision_count": self.collision_count,
            "poisson_draw_work": self.poisson_draw_work,
            "semantic_seed": self.semantic_seed,
            "input_anchor_sha256": self.input_anchor_sha256,
            "input_event_content_sha256": self.input_event_content_sha256,
            "history_content_sha256": self.history_content_sha256,
            "window_context_sha256": self.window_context_sha256,
            "lineage_content_sha256": self.lineage_content_sha256,
            "step_evidence": [item.to_dict() for item in self.step_evidence],
            "parameter_bytes": self.parameter_bytes,
            "failure_reason": self.failure_reason,
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
    def from_dict(cls, data: Mapping[str, Any]) -> AddThinGenerationEvidenceV1:
        if set(data) != set(cls.__dataclass_fields__):
            raise ValueError("Add-Thin generation-evidence fields differ")
        optional_hashes = {
            name: None if data[name] is None else str(data[name])
            for name in (
                "input_anchor_sha256",
                "input_event_content_sha256",
                "history_content_sha256",
                "window_context_sha256",
                "lineage_content_sha256",
            )
        }
        return cls(
            config_id=str(data["config_id"]),
            fit_id=str(data["fit_id"]),
            dataset_id=str(data["dataset_id"]),
            checkpoint_id=str(data["checkpoint_id"]),
            window_id=str(data["window_id"]),
            window_context_id=_optional_text(data["window_context_id"]),
            ensemble_member_id=str(data["ensemble_member_id"]),
            status=AddThinGenerationStatus(str(data["status"])),
            attempted=_strict_bool(data["attempted"], "attempted"),
            input_event_count=_strict_int(
                data["input_event_count"], "input_event_count"
            ),
            history_event_count=_strict_int(
                data["history_event_count"], "history_event_count"
            ),
            history_conditioning_scale=_finite_float(
                data["history_conditioning_scale"],
                "history_conditioning_scale",
            ),
            initial_noise_count=_strict_int(
                data["initial_noise_count"], "initial_noise_count"
            ),
            final_point_count=_strict_int(
                data["final_point_count"], "final_point_count"
            ),
            generated_event_count=_strict_int(
                data["generated_event_count"], "generated_event_count"
            ),
            skipped_unsupported_count=_strict_int(
                data["skipped_unsupported_count"],
                "skipped_unsupported_count",
            ),
            collision_count=_strict_int(
                data["collision_count"], "collision_count"
            ),
            poisson_draw_work=_strict_int(
                data["poisson_draw_work"], "poisson_draw_work"
            ),
            semantic_seed=(
                None
                if data["semantic_seed"] is None
                else _strict_int(data["semantic_seed"], "semantic_seed")
            ),
            input_anchor_sha256=optional_hashes["input_anchor_sha256"],
            input_event_content_sha256=optional_hashes[
                "input_event_content_sha256"
            ],
            history_content_sha256=optional_hashes["history_content_sha256"],
            window_context_sha256=optional_hashes["window_context_sha256"],
            lineage_content_sha256=optional_hashes["lineage_content_sha256"],
            step_evidence=tuple(
                AddThinStepEvidenceV1.from_dict(_mapping(item, "step_evidence"))
                for item in _sequence(data["step_evidence"], "step_evidence")
            ),
            parameter_bytes=_strict_int(
                data["parameter_bytes"], "parameter_bytes"
            ),
            wall_time_ms=_strict_int(data["wall_time_ms"], "wall_time_ms"),
            peak_memory_bytes=_strict_int(
                data["peak_memory_bytes"], "peak_memory_bytes"
            ),
            failure_reason=_optional_text(data["failure_reason"]),
            evidence_id=str(data["evidence_id"]),
            schema_version=str(data["schema_version"]),
        )

    @classmethod
    def from_json(cls, text: str) -> AddThinGenerationEvidenceV1:
        return cls.from_dict(_json_mapping(text, 16 * 1024 * 1024))


@dataclass(frozen=True, slots=True)
class AddThinGenerationResultV1:
    """Generated benchmark rows and their all-or-nothing evidence."""

    events: tuple[BenchmarkEventV1, ...]
    event_lineage: tuple[AddThinGenerationLineageV1, ...]
    evidence: AddThinGenerationEvidenceV1

    def __post_init__(self) -> None:
        events = tuple(sorted(self.events, key=_benchmark_event_key))
        lineages = tuple(
            sorted(self.event_lineage, key=lambda item: item.source_event_id)
        )
        if self.evidence.status in {
            AddThinGenerationStatus.REFUSED,
            AddThinGenerationStatus.FAILED,
        }:
            if events or lineages:
                raise ValueError(
                    "closed Add-Thin generation contains partial rows"
                )
        else:
            generated = tuple(
                item for item in events if item.sparsity.startswith("add-thin-")
            )
            if {item.source_event_id for item in generated} != {
                item.source_event_id for item in lineages
            }:
                raise ValueError("Add-Thin rows and lineage do not reconcile")
            if len(generated) != self.evidence.generated_event_count:
                raise ValueError("Add-Thin generated count differs")
        object.__setattr__(self, "events", events)
        object.__setattr__(self, "event_lineage", lineages)


@dataclass(frozen=True, slots=True)
class _Point:
    point_id: str
    event_time_ns: int
    mark_index: int
    origin: str
    created_step: int
    parent_point_id: str | None
    survival_steps: int = 0


def _validate_fit(config: AddThinConfigV1, fit: AddThinFitResultV1) -> None:
    if (
        fit.config_id != config.config_id
        or fit.status is not AddThinFitStatus.FITTED
    ):
        raise AddThinFitError("Add-Thin fit/config binding is invalid")
    if fit.dataset_manifest is None or fit.checkpoint is None:
        raise AddThinFitError("Add-Thin fitted artifacts are absent")
    AddThinConfigV1.from_json(config.to_json())
    AddThinFitResultV1.from_json(fit.to_json())


def _retained_history(
    history_events: Sequence[BenchmarkEventV1],
    *,
    config: AddThinConfigV1,
    fit: AddThinFitResultV1,
    window: ReconstructionWindowV1,
) -> tuple[BenchmarkEventV1, ...]:
    raw = tuple(history_events)
    if len(raw) > config.limits.max_history_events:
        raise _AddThinRefusal("history_event_limit_exceeded")
    if any(not isinstance(item, BenchmarkEventV1) for item in raw):
        raise AddThinGenerationError("history contains a non-benchmark event")
    if any(item.symbol not in fit.symbols for item in raw):
        raise _AddThinRefusal("history_contains_unsupported_symbol")
    if any(item.event_time_ns >= window.input_start_ns for item in raw):
        raise _AddThinRefusal("history_is_not_strict_prior")
    lower = window.input_start_ns - config.limits.max_history_lookback_ns
    retained = tuple(
        sorted(
            (item for item in raw if item.event_time_ns >= lower),
            key=_benchmark_event_key,
        )
    )
    if len({item.benchmark_event_id for item in retained}) != len(retained):
        raise _AddThinRefusal("history_contains_duplicate_identity")
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
    if mark not in MARK_STATES:
        raise AddThinGenerationError("Add-Thin quote mark is unsupported")
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
    if (
        not all(math.isfinite(item) and item > 0.0 for item in (bid, ask))
        or ask < bid
    ):
        raise AddThinGenerationError("Add-Thin quote projection is invalid")
    return bid, ask


def _new_point(
    *,
    event_time_ns: int,
    mark_index: int,
    origin: str,
    created_step: int,
    ordinal: int,
    seed: int,
    parent_point_id: str | None = None,
) -> _Point:
    point_id = _stable_id(
        "add-thin-point",
        {
            "seed": seed,
            "event_time_ns": event_time_ns,
            "mark_index": mark_index,
            "origin": origin,
            "created_step": created_step,
            "ordinal": ordinal,
            "parent_point_id": parent_point_id,
        },
    )
    return _Point(
        point_id=point_id,
        event_time_ns=event_time_ns,
        mark_index=mark_index,
        origin=origin,
        created_step=created_step,
        parent_point_id=parent_point_id,
    )


def _sample_hpp_points(
    count: int,
    *,
    start_ns: int,
    end_ns: int,
    mark_count: int,
    origin: str,
    created_step: int,
    seed: int,
    rng: random.Random,
    occupied: set[int],
) -> tuple[list[_Point], int]:
    points: list[_Point] = []
    collisions = 0
    span = end_ns - start_ns - 1
    if span <= 0:
        raise AddThinGenerationError("Add-Thin generation interval is empty")
    for ordinal in range(count):
        event_time_ns = start_ns + 1 + rng.randrange(span)
        if event_time_ns in occupied:
            collisions += 1
            continue
        occupied.add(event_time_ns)
        points.append(
            _new_point(
                event_time_ns=event_time_ns,
                mark_index=rng.randrange(mark_count),
                origin=origin,
                created_step=created_step,
                ordinal=ordinal,
                seed=seed,
            )
        )
    return points, collisions


def _sample_cell_points(
    mean_by_cell: Sequence[float],
    *,
    start_ns: int,
    end_ns: int,
    bins: int,
    mark_count: int,
    origin: str,
    created_step: int,
    seed: int,
    rng: random.Random,
    occupied: set[int],
    work: list[int],
    limits: AddThinResourceLimitsV1,
) -> tuple[list[_Point], int]:
    points: list[_Point] = []
    collisions = 0
    duration = end_ns - start_ns
    ordinal = 0
    for cell, mean in enumerate(mean_by_cell):
        count = _poisson(
            mean,
            rng,
            work=work,
            work_limit=limits.max_poisson_draw_work,
        )
        if count > limits.max_events_per_bin:
            raise _AddThinRefusal("events_per_bin_limit_exceeded")
        bin_index = cell // mark_count
        mark_index = cell % mark_count
        bin_start = start_ns + (duration * bin_index) // bins
        bin_end = start_ns + (duration * (bin_index + 1)) // bins
        low = max(start_ns + 1, bin_start)
        high = min(end_ns, bin_end)
        if high <= low:
            collisions += count
            continue
        for _ in range(count):
            event_time_ns = low + rng.randrange(high - low)
            if event_time_ns in occupied:
                collisions += 1
                continue
            occupied.add(event_time_ns)
            points.append(
                _new_point(
                    event_time_ns=event_time_ns,
                    mark_index=mark_index,
                    origin=origin,
                    created_step=created_step,
                    ordinal=ordinal,
                    seed=seed,
                )
            )
            ordinal += 1
    return points, collisions


def _validate_generation_inputs(
    config: AddThinConfigV1,
    fit: AddThinFitResultV1,
    context: AddThinWindowContextV1,
    events: Sequence[BenchmarkEventV1],
    scenario: BenchmarkScenarioV1,
    window: ReconstructionWindowV1,
    ensemble_member_id: str,
) -> tuple[BenchmarkEventV1, ...]:
    _validate_fit(config, fit)
    if context.window_id != window.window_id:
        raise AddThinGenerationError(
            "Add-Thin generation context/window differs"
        )
    if window.ensemble_member_id != ensemble_member_id:
        raise AddThinGenerationError(
            "Add-Thin generation member/window differs"
        )
    if scenario.event_schema_version != BENCHMARK_EVENT_SCHEMA_VERSION:
        raise AddThinGenerationError(
            "Add-Thin generation scenario schema differs"
        )
    if scenario.epoch_id != context.technology_label:
        raise AddThinGenerationError(
            "Add-Thin generation scenario/context epoch differs"
        )
    raw = tuple(events)
    if any(not isinstance(item, BenchmarkEventV1) for item in raw):
        raise AddThinGenerationError("input contains a non-benchmark event")
    if not raw:
        raise _AddThinRefusal("generation_input_is_empty")
    if len(raw) > config.limits.max_sequence_events:
        raise _AddThinRefusal("input_event_limit_exceeded")
    if any(
        item.symbol not in fit.symbols
        or not window.reads_event_time(item.event_time_ns)
        for item in raw
    ):
        raise _AddThinRefusal("input_is_outside_synchronized_scope")
    ordered = tuple(sorted(raw, key=_benchmark_event_key))
    if len({item.benchmark_event_id for item in ordered}) != len(ordered):
        raise _AddThinRefusal("input_contains_duplicate_identity")
    by_symbol = Counter(item.symbol for item in ordered)
    if any(by_symbol.get(symbol, 0) < 2 for symbol in fit.symbols):
        raise _AddThinRefusal("destination_symbol_lacks_two_anchors")
    if (
        fit.information_mode is InformationMode.EX_ANTE_SIMULATION
        and context.observed_context_id is not None
        and cast(int, context.observed_context_available_ns)
        > window.core_start_ns
    ):
        raise _AddThinRefusal("context_is_not_available_ex_ante")
    return ordered


def _point_cell(
    point: _Point,
    *,
    start_ns: int,
    end_ns: int,
    bins: int,
    mark_count: int,
) -> int:
    duration = end_ns - start_ns
    bin_index = min(
        bins - 1,
        max(0, ((point.event_time_ns - start_ns) * bins) // duration),
    )
    return int(bin_index) * mark_count + point.mark_index


def _lineage_digest(
    lineages: Sequence[AddThinGenerationLineageV1],
) -> str:
    return hashlib.sha256(
        canonical_contract_json(
            [
                item.to_dict()
                for item in sorted(
                    lineages, key=lambda item: item.source_event_id
                )
            ]
        ).encode()
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class FittedAddThinBenchmarkGeneratorV1(BenchmarkGeneratorV1):
    """Adapter exposing one fitted bounded marked Add-Thin challenger."""

    candidate: BenchmarkCandidateV1
    config: AddThinConfigV1
    fit_result: AddThinFitResultV1
    window_contexts: Mapping[str, AddThinWindowContextV1] = field(
        default_factory=dict
    )
    candidate_id: str = field(init=False)
    event_schema_version: str = BENCHMARK_EVENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.candidate.kind is not BenchmarkCandidateKind.CANDIDATE:
            raise ValueError("Add-Thin adapter requires a candidate")
        if self.candidate.method_id != ADD_THIN_GENERATOR_ID:
            raise ValueError("Add-Thin candidate method differs")
        _validate_fit(self.config, self.fit_result)
        contexts = dict(self.window_contexts)
        if any(key != value.window_id for key, value in contexts.items()):
            raise ValueError("Add-Thin context key differs")
        object.__setattr__(self, "window_contexts", contexts)
        if self.event_schema_version != BENCHMARK_EVENT_SCHEMA_VERSION:
            raise ValueError("Add-Thin adapter requires benchmark event v1")
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
            AddThinGenerationStatus.REFUSED,
            AddThinGenerationStatus.FAILED,
        }:
            raise AddThinGenerationError(
                result.evidence.failure_reason or "Add-Thin generation failed"
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
    ) -> AddThinGenerationResultV1:
        """Run the fixed HPP start and B/C/D/E reverse decomposition."""
        started = time.perf_counter()
        before_peak = peak_rss_bytes()
        raw = tuple(degraded_events)
        context = self.window_contexts.get(window.window_id)
        dataset = self.fit_result.dataset_manifest
        checkpoint = self.fit_result.checkpoint
        if dataset is None or checkpoint is None:
            raise AddThinFitError("fitted Add-Thin artifacts are absent")
        history_count = 0
        history_scale = 1.0
        initial_noise_count = 0
        final_point_count = 0
        skipped = 0
        collisions = 0
        work = [0]
        seed: int | None = None
        anchor_hash: str | None = None
        input_hash: str | None = None
        history_hash: str | None = None
        context_hash: str | None = None
        step_evidence: list[AddThinStepEvidenceV1] = []
        try:
            if context is None:
                raise AddThinGenerationError(
                    "Add-Thin generation context is absent"
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
            history_scale = 1.0 + 0.1 * math.tanh(
                (history_count - checkpoint.noise_mean_event_count)
                / max(1.0, checkpoint.noise_mean_event_count)
            )
            anchor_hash = _anchor_digest(ordered)
            input_hash = _event_content_digest(ordered)
            history_hash = _event_content_digest(retained_history)
            context_hash = hashlib.sha256(
                canonical_contract_json(context.to_dict()).encode()
            ).hexdigest()
            seed = _semantic_seed(
                {
                    "architecture": self.config.architecture,
                    "config_id": self.config.config_id,
                    "fit_id": self.fit_result.fit_id,
                    "dataset_id": dataset.dataset_id,
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
                + self.config.limits.max_generation_points * 2048
            )
            if (
                estimated_memory
                > self.config.limits.max_generation_memory_bytes
            ):
                raise _AddThinRefusal("generation_memory_preflight_exceeded")
            deadline = started + (
                self.config.limits.max_generation_wall_time_ms / 1000.0
            )
            start_ns = window.core_start_ns
            end_ns = window.core_end_ns
            duration_scale = (end_ns - start_ns) / (
                checkpoint.mean_training_window_duration_ns
            )
            mark_count = len(checkpoint.mark_vocabulary)
            rng = random.Random(seed)
            occupied = {item.event_time_ns for item in ordered}
            initial_noise_count = _poisson(
                checkpoint.noise_mean_event_count
                * duration_scale
                * history_scale,
                rng,
                work=work,
                work_limit=self.config.limits.max_poisson_draw_work,
            )
            if initial_noise_count > self.config.limits.max_generation_points:
                raise _AddThinRefusal("initial_noise_point_limit_exceeded")
            points, initial_collisions = _sample_hpp_points(
                initial_noise_count,
                start_ns=start_ns,
                end_ns=end_ns,
                mark_count=mark_count,
                origin="initial_noise",
                created_step=len(checkpoint.step_keep_probabilities),
                seed=seed,
                rng=rng,
                occupied=occupied,
            )
            collisions += initial_collisions
            cumulative = checkpoint.cumulative_keep_probabilities
            alphas = checkpoint.step_keep_probabilities
            if len(alphas) > self.config.limits.max_generation_steps:
                raise _AddThinRefusal("generation_step_limit_exceeded")
            rates = tuple(
                value for row in checkpoint.clean_intensity for value in row
            )
            for step_index in range(len(alphas) - 1, 0, -1):
                if time.perf_counter() > deadline:
                    raise _AddThinRefusal("generation_wall_time_exceeded")
                input_count = len(points)
                b_points: list[_Point] = []
                e_points: list[_Point] = []
                thinned = 0
                c_coefficient, d_coefficient, e_probability = (
                    _reverse_coefficients(self.config, step_index)
                )
                classifier = checkpoint.classifier_probabilities[step_index]
                for point in points:
                    cell = _point_cell(
                        point,
                        start_ns=start_ns,
                        end_ns=end_ns,
                        bins=checkpoint.time_bin_count,
                        mark_count=mark_count,
                    )
                    retained = _Point(
                        point_id=point.point_id,
                        event_time_ns=point.event_time_ns,
                        mark_index=point.mark_index,
                        origin=point.origin,
                        created_step=point.created_step,
                        parent_point_id=point.parent_point_id,
                        survival_steps=point.survival_steps + 1,
                    )
                    if rng.random() < classifier[cell]:
                        b_points.append(retained)
                    elif rng.random() < e_probability:
                        e_points.append(retained)
                    else:
                        thinned += 1
                occupied = {item.event_time_ns for item in ordered}
                occupied.update(item.event_time_ns for item in b_points)
                occupied.update(item.event_time_ns for item in e_points)
                missing_clean_means = tuple(
                    (1.0 - cumulative[step_index])
                    * rate
                    * duration_scale
                    * history_scale
                    for rate in rates
                )
                c_means = tuple(
                    c_coefficient * mean for mean in missing_clean_means
                )
                c_points, c_collisions = _sample_cell_points(
                    c_means,
                    start_ns=start_ns,
                    end_ns=end_ns,
                    bins=checkpoint.time_bin_count,
                    mark_count=mark_count,
                    origin="C_missing_clean",
                    created_step=step_index,
                    seed=seed,
                    rng=rng,
                    occupied=occupied,
                    work=work,
                    limits=self.config.limits,
                )
                d_count = _poisson(
                    d_coefficient
                    * checkpoint.noise_mean_event_count
                    * duration_scale
                    * history_scale,
                    rng,
                    work=work,
                    work_limit=self.config.limits.max_poisson_draw_work,
                )
                d_points, d_collisions = _sample_hpp_points(
                    d_count,
                    start_ns=start_ns,
                    end_ns=end_ns,
                    mark_count=mark_count,
                    origin="D_reverse_noise",
                    created_step=step_index,
                    seed=seed,
                    rng=rng,
                    occupied=occupied,
                )
                points = b_points + c_points + d_points + e_points
                if len(points) > self.config.limits.max_generation_points:
                    raise _AddThinRefusal("generation_point_limit_exceeded")
                step_collisions = c_collisions + d_collisions
                collisions += step_collisions
                step_evidence.append(
                    AddThinStepEvidenceV1(
                        step_index=step_index + 1,
                        input_count=input_count,
                        b_count=len(b_points),
                        c_count=len(c_points),
                        d_count=len(d_points),
                        e_count=len(e_points),
                        thinned_count=thinned,
                        collision_count=step_collisions,
                        output_count=len(points),
                    )
                )
            input_count = len(points)
            classifier = checkpoint.classifier_probabilities[0]
            b_points = []
            thinned = 0
            for point in points:
                cell = _point_cell(
                    point,
                    start_ns=start_ns,
                    end_ns=end_ns,
                    bins=checkpoint.time_bin_count,
                    mark_count=mark_count,
                )
                if rng.random() < classifier[cell]:
                    b_points.append(
                        _Point(
                            point_id=point.point_id,
                            event_time_ns=point.event_time_ns,
                            mark_index=point.mark_index,
                            origin=point.origin,
                            created_step=point.created_step,
                            parent_point_id=point.parent_point_id,
                            survival_steps=point.survival_steps + 1,
                        )
                    )
                else:
                    thinned += 1
            occupied = {item.event_time_ns for item in ordered}
            occupied.update(item.event_time_ns for item in b_points)
            final_means = tuple(
                (1.0 - cumulative[0]) * rate * duration_scale * history_scale
                for rate in rates
            )
            c_points, final_collisions = _sample_cell_points(
                final_means,
                start_ns=start_ns,
                end_ns=end_ns,
                bins=checkpoint.time_bin_count,
                mark_count=mark_count,
                origin="C_final_clean",
                created_step=0,
                seed=seed,
                rng=rng,
                occupied=occupied,
                work=work,
                limits=self.config.limits,
            )
            points = b_points + c_points
            final_point_count = len(points)
            collisions += final_collisions
            step_evidence.append(
                AddThinStepEvidenceV1(
                    step_index=1,
                    input_count=input_count,
                    b_count=len(b_points),
                    c_count=len(c_points),
                    d_count=0,
                    e_count=0,
                    thinned_count=thinned,
                    collision_count=final_collisions,
                    output_count=len(points),
                )
            )
            anchors: dict[str, list[BenchmarkEventV1]] = defaultdict(list)
            for event in ordered:
                anchors[event.symbol].append(event)
            generated: list[BenchmarkEventV1] = []
            lineages: list[AddThinGenerationLineageV1] = []
            interval_counts: Counter[str] = Counter()
            vocabulary = checkpoint.mark_vocabulary
            for ordinal, point in enumerate(
                sorted(
                    points, key=lambda item: (item.event_time_ns, item.point_id)
                ),
                start=1,
            ):
                if time.perf_counter() > deadline:
                    raise _AddThinRefusal("generation_wall_time_exceeded")
                joint_mark = vocabulary[point.mark_index]
                destination_symbol, mark = joint_mark.split(":", 1)
                pair = _enclosing_anchor_pair(
                    anchors, destination_symbol, point.event_time_ns
                )
                if pair is None or not window.owns_event_time(
                    point.event_time_ns
                ):
                    skipped += 1
                    continue
                left, right = pair
                interval_id = derive_anchor_interval_id(
                    left.benchmark_event_id, right.benchmark_event_id
                )
                interval_counts[interval_id] += 1
                if (
                    interval_counts[interval_id]
                    > self.config.limits.max_events_per_interval
                ):
                    raise _AddThinRefusal("events_per_interval_limit_exceeded")
                if len(generated) >= self.config.limits.max_generation_points:
                    raise _AddThinRefusal("generated_event_limit_exceeded")
                if len(generated) + 1 > (
                    len(ordered)
                    * self.config.limits.max_candidate_amplification
                ):
                    raise _AddThinRefusal(
                        "candidate_amplification_limit_exceeded"
                    )
                bid, ask = _project_quote(
                    left, right, point.event_time_ns, mark
                )
                bin_index = (
                    _point_cell(
                        point,
                        start_ns=start_ns,
                        end_ns=end_ns,
                        bins=checkpoint.time_bin_count,
                        mark_count=mark_count,
                    )
                    // mark_count
                )
                intensity = checkpoint.clean_intensity[bin_index][
                    point.mark_index
                ]
                bin_total = sum(checkpoint.clean_intensity[bin_index])
                mark_probability = intensity / bin_total
                source_id = _stable_id(
                    "add-thin-event",
                    {
                        "semantic_seed": seed,
                        "point_id": point.point_id,
                        "event_time_ns": point.event_time_ns,
                        "joint_mark": joint_mark,
                        "anchor_interval_id": interval_id,
                        "checkpoint_id": checkpoint.checkpoint_id,
                    },
                )
                generated.append(
                    BenchmarkEventV1(
                        source_event_id=source_id,
                        symbol=destination_symbol,
                        event_time_ns=point.event_time_ns,
                        event_sequence=ordinal,
                        bid=bid,
                        ask=ask,
                        epoch_id=left.epoch_id,
                        session=context.session,
                        event_state=mark,
                        sparsity="add-thin-histogram-marked-cpu-v1",
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
                    AddThinGenerationLineageV1(
                        source_event_id=source_id,
                        origin=point.origin,
                        created_step=point.created_step,
                        survival_steps=point.survival_steps,
                        final_survival=True,
                        time_bin=bin_index,
                        destination_symbol=destination_symbol,
                        transition_mark=mark,
                        mark_probability=mark_probability,
                        bin_intensity=intensity,
                        parent_point_id=point.parent_point_id,
                        left_anchor_event_id=left.benchmark_event_id,
                        right_anchor_event_id=right.benchmark_event_id,
                        anchor_interval_id=interval_id,
                    )
                )
            lineage_hash = _lineage_digest(lineages)
            output_events = tuple(
                sorted(ordered + tuple(generated), key=_benchmark_event_key)
            )
            output_bytes = len(
                canonical_contract_json(
                    {
                        "events": [item.to_dict() for item in output_events],
                        "lineage": [item.to_dict() for item in lineages],
                        "steps": [item.to_dict() for item in step_evidence],
                    }
                ).encode()
            )
            if output_bytes > self.config.limits.max_output_bytes:
                raise _AddThinRefusal("generation_output_limit_exceeded")
            if time.perf_counter() > deadline:
                raise _AddThinRefusal("generation_wall_time_exceeded")
            measured_peak = max(0, peak_rss_bytes() - before_peak)
            if measured_peak > self.config.limits.max_generation_memory_bytes:
                raise _AddThinRefusal("generation_measured_memory_exceeded")
            status = (
                AddThinGenerationStatus.GENERATED
                if generated
                else AddThinGenerationStatus.EMPTY
            )
            evidence = AddThinGenerationEvidenceV1(
                config_id=self.config.config_id,
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
                history_conditioning_scale=history_scale,
                initial_noise_count=initial_noise_count,
                final_point_count=final_point_count,
                generated_event_count=len(generated),
                skipped_unsupported_count=skipped,
                collision_count=collisions,
                poisson_draw_work=work[0],
                semantic_seed=seed,
                input_anchor_sha256=anchor_hash,
                input_event_content_sha256=input_hash,
                history_content_sha256=history_hash,
                window_context_sha256=context_hash,
                lineage_content_sha256=lineage_hash,
                step_evidence=tuple(step_evidence),
                parameter_bytes=checkpoint.parameter_bytes,
                wall_time_ms=round((time.perf_counter() - started) * 1000),
                peak_memory_bytes=measured_peak,
            )
            return AddThinGenerationResultV1(
                output_events, tuple(lineages), evidence
            )
        except (
            AddThinFitError,
            AddThinGenerationError,
            ArithmeticError,
            KeyError,
            TypeError,
            ValueError,
        ) as err:
            status = (
                AddThinGenerationStatus.REFUSED
                if isinstance(err, (_AddThinRefusal, AddThinFitError))
                else AddThinGenerationStatus.FAILED
            )
            evidence = AddThinGenerationEvidenceV1(
                config_id=self.config.config_id,
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
                history_conditioning_scale=history_scale,
                initial_noise_count=initial_noise_count,
                final_point_count=final_point_count,
                generated_event_count=0,
                skipped_unsupported_count=skipped,
                collision_count=collisions,
                poisson_draw_work=work[0],
                semantic_seed=seed,
                input_anchor_sha256=anchor_hash,
                input_event_content_sha256=input_hash,
                history_content_sha256=history_hash,
                window_context_sha256=context_hash,
                lineage_content_sha256=None,
                step_evidence=tuple(step_evidence),
                parameter_bytes=checkpoint.parameter_bytes,
                wall_time_ms=round((time.perf_counter() - started) * 1000),
                peak_memory_bytes=max(0, peak_rss_bytes() - before_peak),
                failure_reason=(
                    f"generation_{status.value}:{type(err).__name__}:{err}"
                ),
            )
            return AddThinGenerationResultV1((), (), evidence)


def build_add_thin_benchmark_candidate(
    config: AddThinConfigV1,
    fit_result: AddThinFitResultV1,
    *,
    ensemble_member_ids: Sequence[str],
) -> BenchmarkCandidateV1:
    """Describe the fitted challenger without promoting it."""
    if fit_result.config_id != config.config_id:
        raise ValueError("Add-Thin fit and config differ")
    return BenchmarkCandidateV1(
        kind=BenchmarkCandidateKind.CANDIDATE,
        method_id=ADD_THIN_GENERATOR_ID,
        implementation_version=ADD_THIN_IMPLEMENTATION_VERSION,
        parameters={
            "config_id": config.config_id,
            "fit_id": fit_result.fit_id,
            "architecture": config.architecture,
            "automatic_winner": False,
        },
        ensemble_member_ids=tuple(ensemble_member_ids),
    )


def build_fitted_add_thin_generator(
    config: AddThinConfigV1,
    fit_result: AddThinFitResultV1,
    *,
    ensemble_member_ids: Sequence[str],
    window_contexts: Mapping[str, AddThinWindowContextV1],
) -> FittedAddThinBenchmarkGeneratorV1:
    """Bind one immutable Add-Thin checkpoint to the benchmark adapter."""
    return FittedAddThinBenchmarkGeneratorV1(
        candidate=build_add_thin_benchmark_candidate(
            config,
            fit_result,
            ensemble_member_ids=ensemble_member_ids,
        ),
        config=config,
        fit_result=fit_result,
        window_contexts=window_contexts,
    )


@dataclass(frozen=True, slots=True)
class AddThinCandidateLineageV1:
    """Carving pointer retaining the sampled Add-Thin lineage identity."""

    event_id: str
    transformation_id: str
    generation_lineage_id: str
    schema_version: str = ADD_THIN_CANDIDATE_LINEAGE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ADD_THIN_CANDIDATE_LINEAGE_SCHEMA_VERSION:
            raise ValueError("unsupported Add-Thin candidate-lineage schema")
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
class AddThinCandidateBatchV1:
    """Process-local Add-Thin proposals for one immutable anchor interval."""

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
    event_lineage: tuple[AddThinCandidateLineageV1, ...]
    fit_id: str
    dataset_id: str
    checkpoint_id: str
    generation_evidence_id: str
    window_context_id: str
    batch_id: str = ""
    schema_version: str = ADD_THIN_CANDIDATE_BATCH_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ADD_THIN_CANDIDATE_BATCH_SCHEMA_VERSION:
            raise ValueError("unsupported Add-Thin candidate-batch schema")
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
            raise ValueError("generated Add-Thin batch requires events")
        if status is not MotifGenerationStatus.GENERATED and events:
            raise ValueError("closed Add-Thin batch contains events")
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
                "Add-Thin candidate event differs from batch scope"
            )
        event_ids = {item.event_id for item in events}
        if len(event_ids) != len(events) or event_ids != {
            item.event_id for item in lineages
        }:
            raise ValueError("Add-Thin candidate lineage does not reconcile")
        if len({item.generation_lineage_id for item in lineages}) != len(
            lineages
        ):
            raise ValueError("Add-Thin generation lineage is reused")
        object.__setattr__(self, "events", events)
        object.__setattr__(self, "event_lineage", lineages)
        expected = _stable_id(
            "add-thin-candidate-batch", self.identity_payload()
        )
        if self.batch_id and self.batch_id != expected:
            raise ValueError("Add-Thin candidate batch_id differs")
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

    def lineage_for(self, event_id: str) -> AddThinCandidateLineageV1:
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


def build_add_thin_candidate_batches(
    *,
    run: ReconstructionRunV1,
    window: ReconstructionWindowV1,
    config: AddThinConfigV1,
    fit_result: AddThinFitResultV1,
    generation_result: AddThinGenerationResultV1,
    context: AddThinWindowContextV1,
    observed_events: Sequence[SyntheticEventV1],
    session_state: str,
    special_tags: Sequence[str] = (),
    event_tags: Sequence[str] = (),
) -> tuple[AddThinCandidateBatchV1, ...]:
    """Project Add-Thin proposals into the generator-neutral carving seam."""
    if window.run_id != run.run_id:
        raise ValueError("Add-Thin candidate window does not belong to run")
    if window.ensemble_member_id not in run.ensemble_member_ids:
        raise ValueError("Add-Thin candidate member is outside run")
    if config.config_id not in run.configuration_ids:
        raise ValueError("Add-Thin config is absent from reconstruction run")
    _validate_fit(config, fit_result)
    dataset = fit_result.dataset_manifest
    checkpoint = fit_result.checkpoint
    if dataset is None or checkpoint is None:
        raise ValueError("Add-Thin candidate requires fitted artifacts")
    evidence = generation_result.evidence
    if (
        evidence.config_id != config.config_id
        or evidence.fit_id != fit_result.fit_id
        or evidence.dataset_id != dataset.dataset_id
        or evidence.checkpoint_id != checkpoint.checkpoint_id
        or evidence.window_id != window.window_id
        or evidence.window_context_id != context.context_id
        or context.window_id != window.window_id
    ):
        raise ValueError("Add-Thin fit/generation/context identities differ")
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
            "Add-Thin carving projection requires observed anchors"
        )
    if evidence.input_anchor_sha256 is not None and (
        _synthetic_anchor_digest(observed) != evidence.input_anchor_sha256
    ):
        raise ValueError(
            "Add-Thin carving anchors differ from generation input"
        )
    upstream_closed = evidence.status in {
        AddThinGenerationStatus.REFUSED,
        AddThinGenerationStatus.FAILED,
    }
    proposals = tuple(
        item
        for item in generation_result.events
        if item.sparsity.startswith("add-thin-")
    )
    generation_lineage = {
        item.source_event_id: item for item in generation_result.event_lineage
    }
    if set(generation_lineage) != {item.source_event_id for item in proposals}:
        raise ValueError("Add-Thin proposal and generation lineage differ")
    batches: list[AddThinCandidateBatchV1] = []
    assigned: set[str] = set()
    by_symbol: dict[str, list[SyntheticEventV1]] = defaultdict(list)
    for event in observed:
        by_symbol[event.symbol].append(event)
    for symbol in sorted(by_symbol):
        anchors = by_symbol[symbol]
        if len(anchors) < 2:
            raise ValueError(
                "each Add-Thin carving symbol requires two anchors"
            )
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
                "add-thin-interval-transformation",
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
                    source_version_id=fit_result.fit_id,
                    left_anchor_event_id=left.event_id,
                    right_anchor_event_id=right.event_id,
                    anchor_interval_id=interval_id,
                    generator_id=ADD_THIN_GENERATOR_ID,
                    generator_version=ADD_THIN_IMPLEMENTATION_VERSION,
                    generator_config_id=config.config_id,
                    reference_id=item.source_event_id,
                    motif_id=ADD_THIN_GENERATOR_ID,
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
                AddThinCandidateBatchV1(
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
                            AddThinCandidateLineageV1(
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
                    checkpoint_id=checkpoint.checkpoint_id,
                    generation_evidence_id=evidence.evidence_id,
                    window_context_id=context.context_id,
                )
            )
    if assigned != {item.source_event_id for item in proposals}:
        raise ValueError("Add-Thin proposal lies outside observed anchors")
    return tuple(batches)


__all__ = [
    "ADD_THIN_ARCHITECTURE",
    "ADD_THIN_CANDIDATE_BATCH_SCHEMA_VERSION",
    "ADD_THIN_CANDIDATE_LINEAGE_SCHEMA_VERSION",
    "ADD_THIN_CHECKPOINT_SCHEMA_VERSION",
    "ADD_THIN_CONFIG_SCHEMA_VERSION",
    "ADD_THIN_DATASET_MANIFEST_SCHEMA_VERSION",
    "ADD_THIN_DATASET_WINDOW_SCHEMA_VERSION",
    "ADD_THIN_FIT_RESULT_SCHEMA_VERSION",
    "ADD_THIN_GENERATION_EVIDENCE_SCHEMA_VERSION",
    "ADD_THIN_GENERATION_LINEAGE_SCHEMA_VERSION",
    "ADD_THIN_GENERATOR_ID",
    "ADD_THIN_IMPLEMENTATION_VERSION",
    "ADD_THIN_PROTECTED_WINDOW_SCHEMA_VERSION",
    "ADD_THIN_RESOURCE_LIMITS_SCHEMA_VERSION",
    "ADD_THIN_STEP_EVIDENCE_SCHEMA_VERSION",
    "ADD_THIN_WINDOW_CONTEXT_SCHEMA_VERSION",
    "AddThinCandidateBatchV1",
    "AddThinCandidateLineageV1",
    "AddThinCheckpointV1",
    "AddThinConfigV1",
    "AddThinDatasetManifestV1",
    "AddThinDatasetWindowV1",
    "AddThinFitError",
    "AddThinFitResultV1",
    "AddThinFitStatus",
    "AddThinGenerationError",
    "AddThinGenerationEvidenceV1",
    "AddThinGenerationLineageV1",
    "AddThinGenerationResultV1",
    "AddThinGenerationStatus",
    "AddThinProtectedWindowV1",
    "AddThinResourceLimitsV1",
    "AddThinStepEvidenceV1",
    "AddThinWindowContextV1",
    "FittedAddThinBenchmarkGeneratorV1",
    "build_add_thin_benchmark_candidate",
    "build_add_thin_candidate_batches",
    "build_add_thin_protected_window",
    "build_fitted_add_thin_generator",
    "default_add_thin_config",
    "fit_add_thin_challenger",
]
