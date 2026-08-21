"""Exact raw-proposal diagnostics for fitted marked Hawkes challengers.

The analytic compensator in this module applies only to the unconstrained
fitted Hawkes proposal law.  Carving, anchor rejection, and reconciliation
change that law, so final products retain their separate simulation-predictive
diagnostics in :mod:`histdatacom.synthetic.qualification`.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import random
import statistics
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from enum import Enum
from itertools import groupby, pairwise
from pathlib import Path
from typing import Any, cast

from histdatacom.runtime_contracts import ArtifactRef, JSONValue
from histdatacom.synthetic.benchmark import BenchmarkEventV1
from histdatacom.synthetic.contracts import canonical_contract_json
from histdatacom.synthetic.marked_hawkes import (
    MARK_STATES,
    NANOSECONDS_PER_SECOND,
    TRANSITION_CONDITIONED_MARK_POLICY,
    HawkesExcitationStructure,
    MarkedHawkesConfigV1,
    MarkedHawkesFitResultV1,
    MarkedHawkesFitStatus,
)

HAWKES_RESIDUAL_POLICY_SCHEMA_VERSION = "histdatacom.hawkes-residual-policy.v1"
HAWKES_RESIDUAL_WINDOW_SCHEMA_VERSION = "histdatacom.hawkes-residual-window.v1"
HAWKES_MARK_CALIBRATION_BIN_SCHEMA_VERSION = (
    "histdatacom.hawkes-mark-calibration-bin.v1"
)
HAWKES_RESIDUAL_STRATUM_SCHEMA_VERSION = (
    "histdatacom.hawkes-residual-stratum.v1"
)
HAWKES_RESIDUAL_POWER_RESULT_SCHEMA_VERSION = (
    "histdatacom.hawkes-residual-power-result.v1"
)
HAWKES_RESIDUAL_REPORT_SCHEMA_VERSION = "histdatacom.hawkes-residual-report.v1"

EXACT_COMPENSATOR_METHOD = (
    "multivariate-exponential-integrated-mass-piecewise-exact-v1"
)
MARK_PIT_METHOD = "ordered-randomized-discrete-pit-semantic-sha256-v1"
MULTIPLICITY_METHOD = "benjamini-hochberg-within-split-family-v1"
POWER_STUDY_METHOD = "hawkes-residual-power-study-v1"
PRACTICAL_RESIDUAL_TOLERANCE = 0.20
MAX_RESIDUAL_WINDOWS = 512
MAX_RESIDUAL_EVENTS = 1_000_000
MAX_RESIDUAL_STRATA = 512
MAX_REPORT_BYTES = 16 * 1024 * 1024
DEFAULT_OBSERVATION_SCENARIO_ID = "observed-reference-operator"


class HawkesResidualStatus(str, Enum):
    """Fail-closed state for one analytic diagnostic surface."""

    PASSED = "passed"
    FAILED = "failed"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"
    REFUSED = "refused"


class HawkesResidualStage(str, Enum):
    """Scientific law to which a residual report applies."""

    RAW_PROPOSAL = "raw_proposal"
    FINAL_CONSTRAINED_PRODUCT = "final_constrained_product"


@dataclass(frozen=True, slots=True)
class HawkesResidualPolicyV1:
    """Frozen support, multiplicity, mark, and power policy."""

    alpha: float = 0.05
    minimum_residual_count: int = 64
    minimum_stratum_count: int = 16
    minimum_window_count: int = 6
    maximum_absolute_lag1: float = 0.20
    mark_calibration_bin_count: int = 10
    semantic_mark_seed: int = 511_000
    power_replications: int = 128
    power_sample_sizes: tuple[int, ...] = (32, 64, 128, 256)
    minimum_power: float = 0.80
    maximum_false_positive_rate: float = 0.08
    policy_id: str = ""
    schema_version: str = HAWKES_RESIDUAL_POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, HAWKES_RESIDUAL_POLICY_SCHEMA_VERSION
        )
        for name in ("alpha", "minimum_power", "maximum_false_positive_rate"):
            value = _probability(getattr(self, name), name, open_interval=True)
            object.__setattr__(self, name, value)
        if self.minimum_power <= 0.5:
            raise ValueError("Hawkes residual minimum power must exceed 0.5")
        if self.maximum_false_positive_rate < self.alpha:
            raise ValueError(
                "Hawkes residual false-positive limit is below alpha"
            )
        for name, lower, upper in (
            ("minimum_residual_count", 8, MAX_RESIDUAL_EVENTS),
            ("minimum_stratum_count", 4, MAX_RESIDUAL_EVENTS),
            ("minimum_window_count", 2, MAX_RESIDUAL_WINDOWS),
            ("mark_calibration_bin_count", 2, 32),
            ("power_replications", 64, 2048),
        ):
            object.__setattr__(
                self,
                name,
                _bounded_int(getattr(self, name), name, lower, upper),
            )
        lag = _probability(
            self.maximum_absolute_lag1,
            "maximum_absolute_lag1",
            open_interval=True,
        )
        object.__setattr__(self, "maximum_absolute_lag1", lag)
        seed = _bounded_int(
            self.semantic_mark_seed, "semantic_mark_seed", 0, 2**63 - 1
        )
        object.__setattr__(self, "semantic_mark_seed", seed)
        sample_sizes = tuple(
            sorted(
                {
                    _bounded_int(
                        item, "power sample size", 8, MAX_RESIDUAL_EVENTS
                    )
                    for item in self.power_sample_sizes
                }
            )
        )
        if not sample_sizes or len(sample_sizes) > 16:
            raise ValueError("Hawkes residual power grid is invalid")
        object.__setattr__(self, "power_sample_sizes", sample_sizes)
        expected = _stable_id("hawkes-residual-policy", self.payload())
        if self.policy_id and self.policy_id != expected:
            raise ValueError("Hawkes residual policy identity differs")
        object.__setattr__(self, "policy_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "alpha": self.alpha,
            "minimum_residual_count": self.minimum_residual_count,
            "minimum_stratum_count": self.minimum_stratum_count,
            "minimum_window_count": self.minimum_window_count,
            "maximum_absolute_lag1": self.maximum_absolute_lag1,
            "mark_calibration_bin_count": self.mark_calibration_bin_count,
            "semantic_mark_seed": self.semantic_mark_seed,
            "power_replications": self.power_replications,
            "power_sample_sizes": list(self.power_sample_sizes),
            "minimum_power": self.minimum_power,
            "maximum_false_positive_rate": self.maximum_false_positive_rate,
            "multiplicity_method": MULTIPLICITY_METHOD,
            "mark_pit_method": MARK_PIT_METHOD,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "policy_id": self.policy_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> HawkesResidualPolicyV1:
        if data.get("multiplicity_method") != MULTIPLICITY_METHOD:
            raise ValueError("Hawkes residual multiplicity method differs")
        if data.get("mark_pit_method") != MARK_PIT_METHOD:
            raise ValueError("Hawkes residual mark PIT method differs")
        return cls(
            alpha=_finite_float(data.get("alpha"), "alpha"),
            minimum_residual_count=_strict_int(
                data.get("minimum_residual_count"), "minimum_residual_count"
            ),
            minimum_stratum_count=_strict_int(
                data.get("minimum_stratum_count"), "minimum_stratum_count"
            ),
            minimum_window_count=_strict_int(
                data.get("minimum_window_count"), "minimum_window_count"
            ),
            maximum_absolute_lag1=_finite_float(
                data.get("maximum_absolute_lag1"), "maximum_absolute_lag1"
            ),
            mark_calibration_bin_count=_strict_int(
                data.get("mark_calibration_bin_count"),
                "mark_calibration_bin_count",
            ),
            semantic_mark_seed=_strict_int(
                data.get("semantic_mark_seed"), "semantic_mark_seed"
            ),
            power_replications=_strict_int(
                data.get("power_replications"), "power_replications"
            ),
            power_sample_sizes=tuple(
                _strict_int(item, "power sample size")
                for item in _sequence(data.get("power_sample_sizes"))
            ),
            minimum_power=_finite_float(
                data.get("minimum_power"), "minimum_power"
            ),
            maximum_false_positive_rate=_finite_float(
                data.get("maximum_false_positive_rate"),
                "maximum_false_positive_rate",
            ),
            policy_id=str(data.get("policy_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class HawkesResidualWindowV1:
    """Process-local protected window; event rows are never serialized."""

    window_id: str
    split_kind: str
    start_ns: int
    end_ns: int
    epoch_id: str
    session: str
    observation_scenario_id: str
    events: tuple[BenchmarkEventV1, ...]
    protected_anchor_truncation_count: int = 0
    support_boundary_truncation_count: int = 0
    window_identity: str = ""
    schema_version: str = HAWKES_RESIDUAL_WINDOW_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, HAWKES_RESIDUAL_WINDOW_SCHEMA_VERSION
        )
        for name in (
            "window_id",
            "epoch_id",
            "session",
            "observation_scenario_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        if self.split_kind not in {"validation", "final_holdout"}:
            raise ValueError("Hawkes residual window split is not protected")
        start = _bounded_int(self.start_ns, "start_ns", 0, 2**63 - 1)
        end = _bounded_int(self.end_ns, "end_ns", start + 1, 2**63 - 1)
        object.__setattr__(self, "start_ns", start)
        object.__setattr__(self, "end_ns", end)
        events = tuple(
            sorted(
                self.events,
                key=lambda item: (
                    item.event_time_ns,
                    item.event_sequence,
                    item.symbol,
                    item.benchmark_event_id,
                ),
            )
        )
        if not events or len(events) > MAX_RESIDUAL_EVENTS:
            raise ValueError("Hawkes residual window event count is invalid")
        if any(
            not isinstance(item, BenchmarkEventV1)
            or item.event_time_ns < start
            or item.event_time_ns >= end
            for item in events
        ):
            raise ValueError("Hawkes residual window contains invalid events")
        object.__setattr__(self, "events", events)
        anchor_count = sum(item.anchor_id is not None for item in events)
        declared_anchor_count = _bounded_int(
            self.protected_anchor_truncation_count,
            "protected_anchor_truncation_count",
            0,
            len(events),
        )
        if declared_anchor_count not in {0, anchor_count}:
            raise ValueError(
                "Hawkes residual protected-anchor count differs from events"
            )
        object.__setattr__(
            self, "protected_anchor_truncation_count", anchor_count
        )
        object.__setattr__(
            self,
            "support_boundary_truncation_count",
            _bounded_int(
                self.support_boundary_truncation_count,
                "support_boundary_truncation_count",
                0,
                len(events),
            ),
        )
        expected = _stable_id("hawkes-residual-window", self.identity_payload())
        if self.window_identity and self.window_identity != expected:
            raise ValueError("Hawkes residual window identity differs")
        object.__setattr__(self, "window_identity", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "window_id": self.window_id,
            "split_kind": self.split_kind,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "epoch_id": self.epoch_id,
            "session": self.session,
            "observation_scenario_id": self.observation_scenario_id,
            "event_count": len(self.events),
            "event_content_sha256": hashlib.sha256(
                canonical_contract_json(
                    [item.to_dict() for item in self.events]
                ).encode("utf-8")
            ).hexdigest(),
            "protected_anchor_truncation_count": (
                self.protected_anchor_truncation_count
            ),
            "support_boundary_truncation_count": (
                self.support_boundary_truncation_count
            ),
            "event_rows_persisted": False,
        }


@dataclass(frozen=True, slots=True)
class HawkesMarkCalibrationBinV1:
    """One bounded one-vs-rest mark-calibration bin."""

    mark: str
    bin_index: int
    lower_probability: float
    upper_probability: float
    sample_count: int
    mean_predicted_probability: float
    observed_frequency: float
    bin_id: str = ""
    schema_version: str = HAWKES_MARK_CALIBRATION_BIN_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, HAWKES_MARK_CALIBRATION_BIN_SCHEMA_VERSION
        )
        if self.mark not in MARK_STATES:
            raise ValueError("Hawkes calibration mark is unsupported")
        object.__setattr__(
            self, "bin_index", _bounded_int(self.bin_index, "bin_index", 0, 31)
        )
        lower = _probability(
            self.lower_probability, "lower_probability", open_interval=False
        )
        upper = _probability(
            self.upper_probability, "upper_probability", open_interval=False
        )
        if upper <= lower:
            raise ValueError("Hawkes calibration bin bounds are invalid")
        object.__setattr__(self, "lower_probability", lower)
        object.__setattr__(self, "upper_probability", upper)
        object.__setattr__(
            self,
            "sample_count",
            _bounded_int(
                self.sample_count, "sample_count", 1, MAX_RESIDUAL_EVENTS
            ),
        )
        for name in ("mean_predicted_probability", "observed_frequency"):
            object.__setattr__(
                self,
                name,
                _probability(getattr(self, name), name, open_interval=False),
            )
        expected = _stable_id("hawkes-mark-calibration-bin", self.payload())
        if self.bin_id and self.bin_id != expected:
            raise ValueError("Hawkes mark calibration bin identity differs")
        object.__setattr__(self, "bin_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "mark": self.mark,
            "bin_index": self.bin_index,
            "lower_probability": self.lower_probability,
            "upper_probability": self.upper_probability,
            "sample_count": self.sample_count,
            "mean_predicted_probability": self.mean_predicted_probability,
            "observed_frequency": self.observed_frequency,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "bin_id": self.bin_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> HawkesMarkCalibrationBinV1:
        return cls(
            mark=str(data.get("mark", "")),
            bin_index=_strict_int(data.get("bin_index"), "bin_index"),
            lower_probability=_finite_float(
                data.get("lower_probability"), "lower_probability"
            ),
            upper_probability=_finite_float(
                data.get("upper_probability"), "upper_probability"
            ),
            sample_count=_strict_int(data.get("sample_count"), "sample_count"),
            mean_predicted_probability=_finite_float(
                data.get("mean_predicted_probability"),
                "mean_predicted_probability",
            ),
            observed_frequency=_finite_float(
                data.get("observed_frequency"), "observed_frequency"
            ),
            bin_id=str(data.get("bin_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class HawkesResidualStratumV1:
    """Bounded time, mark, censoring, and support summary for one slice."""

    dimension: str
    key: str
    window_count: int
    sample_count: int
    mark_sample_count: int
    time_uniform_ks: float | None
    time_uniform_p_value: float | None
    time_uniform_adjusted_p_value: float | None
    time_lag1_autocorrelation: float | None
    time_lag1_p_value: float | None
    time_lag1_adjusted_p_value: float | None
    mark_uniform_ks: float | None
    mark_uniform_p_value: float | None
    mark_uniform_adjusted_p_value: float | None
    integrated_hazard_quantiles: Mapping[str, float]
    pit_tail_rates: Mapping[str, float]
    mark_log_score: float | None
    mark_brier_score: float | None
    mark_calibration_bins: tuple[HawkesMarkCalibrationBinV1, ...]
    transition_confusion_counts: Mapping[str, int]
    conditional_pit_means: Mapping[str, float]
    missing_mark_states: tuple[str, ...]
    reset_count: int
    right_censoring_count: int
    right_censoring_hazard_mean: float
    right_censoring_hazard_max: float
    protected_anchor_truncation_count: int
    support_boundary_truncation_count: int
    tied_event_count: int
    skipped_event_count: int
    status: HawkesResidualStatus
    reason_codes: tuple[str, ...]
    stratum_id: str = ""
    schema_version: str = HAWKES_RESIDUAL_STRATUM_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, HAWKES_RESIDUAL_STRATUM_SCHEMA_VERSION
        )
        object.__setattr__(self, "dimension", _required_text(self.dimension))
        object.__setattr__(self, "key", _required_text(self.key))
        for name in (
            "window_count",
            "sample_count",
            "mark_sample_count",
            "reset_count",
            "right_censoring_count",
            "protected_anchor_truncation_count",
            "support_boundary_truncation_count",
            "tied_event_count",
            "skipped_event_count",
        ):
            object.__setattr__(
                self,
                name,
                _bounded_int(getattr(self, name), name, 0, MAX_RESIDUAL_EVENTS),
            )
        if self.mark_sample_count > self.sample_count:
            raise ValueError(
                "Hawkes mark residual support exceeds time support"
            )
        for name in (
            "time_uniform_ks",
            "time_uniform_p_value",
            "time_uniform_adjusted_p_value",
            "time_lag1_p_value",
            "time_lag1_adjusted_p_value",
            "mark_uniform_ks",
            "mark_uniform_p_value",
            "mark_uniform_adjusted_p_value",
        ):
            value = getattr(self, name)
            if value is not None:
                value = _probability(value, name, open_interval=False)
            object.__setattr__(self, name, value)
        lag = self.time_lag1_autocorrelation
        if lag is not None:
            lag = _finite_float(lag, "time_lag1_autocorrelation")
            if not -1.0 <= lag <= 1.0:
                raise ValueError("Hawkes residual lag correlation is invalid")
        object.__setattr__(self, "time_lag1_autocorrelation", lag)
        for name in (
            "mark_log_score",
            "mark_brier_score",
            "right_censoring_hazard_mean",
            "right_censoring_hazard_max",
        ):
            value = getattr(self, name)
            if value is not None:
                value = _finite_float(value, name)
                if value < 0.0:
                    raise ValueError(f"{name} must be nonnegative")
            object.__setattr__(self, name, value)
        hazards = _bounded_float_mapping(
            self.integrated_hazard_quantiles, "integrated hazard quantile", 16
        )
        tails = _bounded_probability_mapping(
            self.pit_tail_rates, "PIT tail rate", 8
        )
        conditional = _bounded_float_mapping(
            self.conditional_pit_means, "conditional PIT mean", 64
        )
        if any(value < 0.0 or value > 1.0 for value in conditional.values()):
            raise ValueError("conditional PIT means are outside [0, 1]")
        confusion = {
            _required_text(key): _bounded_int(
                value, f"confusion count {key}", 0, MAX_RESIDUAL_EVENTS
            )
            for key, value in sorted(self.transition_confusion_counts.items())
        }
        if len(confusion) > len(MARK_STATES) ** 2:
            raise ValueError("Hawkes mark confusion matrix exceeds bound")
        bins = tuple(
            sorted(self.mark_calibration_bins, key=lambda item: item.bin_id)
        )
        if len(bins) > len(MARK_STATES) * 32:
            raise ValueError("Hawkes mark calibration bins exceed bound")
        missing = tuple(
            sorted({_required_text(item) for item in self.missing_mark_states})
        )
        if not set(missing).issubset(MARK_STATES):
            raise ValueError("Hawkes missing mark states are unsupported")
        object.__setattr__(self, "integrated_hazard_quantiles", hazards)
        object.__setattr__(self, "pit_tail_rates", tails)
        object.__setattr__(self, "conditional_pit_means", conditional)
        object.__setattr__(self, "transition_confusion_counts", confusion)
        object.__setattr__(self, "mark_calibration_bins", bins)
        object.__setattr__(self, "missing_mark_states", missing)
        object.__setattr__(self, "status", HawkesResidualStatus(self.status))
        reasons = _text_tuple(self.reason_codes, allow_empty=False)
        object.__setattr__(self, "reason_codes", reasons)
        expected = _stable_id("hawkes-residual-stratum", self.payload())
        if self.stratum_id and self.stratum_id != expected:
            raise ValueError("Hawkes residual stratum identity differs")
        object.__setattr__(self, "stratum_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "dimension": self.dimension,
            "key": self.key,
            "window_count": self.window_count,
            "sample_count": self.sample_count,
            "mark_sample_count": self.mark_sample_count,
            "time_uniform_ks": self.time_uniform_ks,
            "time_uniform_p_value": self.time_uniform_p_value,
            "time_uniform_adjusted_p_value": self.time_uniform_adjusted_p_value,
            "time_lag1_autocorrelation": self.time_lag1_autocorrelation,
            "time_lag1_p_value": self.time_lag1_p_value,
            "time_lag1_adjusted_p_value": self.time_lag1_adjusted_p_value,
            "mark_uniform_ks": self.mark_uniform_ks,
            "mark_uniform_p_value": self.mark_uniform_p_value,
            "mark_uniform_adjusted_p_value": self.mark_uniform_adjusted_p_value,
            "integrated_hazard_quantiles": dict(
                self.integrated_hazard_quantiles
            ),
            "pit_tail_rates": dict(self.pit_tail_rates),
            "mark_log_score": self.mark_log_score,
            "mark_brier_score": self.mark_brier_score,
            "mark_calibration_bins": [
                item.to_dict() for item in self.mark_calibration_bins
            ],
            "transition_confusion_counts": dict(
                self.transition_confusion_counts
            ),
            "conditional_pit_means": dict(self.conditional_pit_means),
            "missing_mark_states": list(self.missing_mark_states),
            "reset_count": self.reset_count,
            "right_censoring_count": self.right_censoring_count,
            "right_censoring_hazard_mean": self.right_censoring_hazard_mean,
            "right_censoring_hazard_max": self.right_censoring_hazard_max,
            "protected_anchor_truncation_count": self.protected_anchor_truncation_count,
            "support_boundary_truncation_count": self.support_boundary_truncation_count,
            "tied_event_count": self.tied_event_count,
            "skipped_event_count": self.skipped_event_count,
            "status": self.status.value,
            "reason_codes": list(self.reason_codes),
            "residual_rows_embedded": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "stratum_id": self.stratum_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> HawkesResidualStratumV1:
        if data.get("residual_rows_embedded") is not False:
            raise ValueError("Hawkes residual stratum embeds rows")
        return cls(
            dimension=str(data.get("dimension", "")),
            key=str(data.get("key", "")),
            window_count=_strict_int(data.get("window_count"), "window_count"),
            sample_count=_strict_int(data.get("sample_count"), "sample_count"),
            mark_sample_count=_strict_int(
                data.get("mark_sample_count"), "mark_sample_count"
            ),
            time_uniform_ks=_optional_float(data.get("time_uniform_ks")),
            time_uniform_p_value=_optional_float(
                data.get("time_uniform_p_value")
            ),
            time_uniform_adjusted_p_value=_optional_float(
                data.get("time_uniform_adjusted_p_value")
            ),
            time_lag1_autocorrelation=_optional_float(
                data.get("time_lag1_autocorrelation")
            ),
            time_lag1_p_value=_optional_float(data.get("time_lag1_p_value")),
            time_lag1_adjusted_p_value=_optional_float(
                data.get("time_lag1_adjusted_p_value")
            ),
            mark_uniform_ks=_optional_float(data.get("mark_uniform_ks")),
            mark_uniform_p_value=_optional_float(
                data.get("mark_uniform_p_value")
            ),
            mark_uniform_adjusted_p_value=_optional_float(
                data.get("mark_uniform_adjusted_p_value")
            ),
            integrated_hazard_quantiles={
                str(key): _finite_float(value, str(key))
                for key, value in _mapping(
                    data.get("integrated_hazard_quantiles")
                ).items()
            },
            pit_tail_rates={
                str(key): _finite_float(value, str(key))
                for key, value in _mapping(data.get("pit_tail_rates")).items()
            },
            mark_log_score=_optional_float(data.get("mark_log_score")),
            mark_brier_score=_optional_float(data.get("mark_brier_score")),
            mark_calibration_bins=tuple(
                HawkesMarkCalibrationBinV1.from_dict(_mapping(item))
                for item in _sequence(data.get("mark_calibration_bins"))
            ),
            transition_confusion_counts={
                str(key): _strict_int(value, str(key))
                for key, value in _mapping(
                    data.get("transition_confusion_counts")
                ).items()
            },
            conditional_pit_means={
                str(key): _finite_float(value, str(key))
                for key, value in _mapping(
                    data.get("conditional_pit_means")
                ).items()
            },
            missing_mark_states=_string_tuple(data.get("missing_mark_states")),
            reset_count=_strict_int(data.get("reset_count"), "reset_count"),
            right_censoring_count=_strict_int(
                data.get("right_censoring_count"), "right_censoring_count"
            ),
            right_censoring_hazard_mean=_finite_float(
                data.get("right_censoring_hazard_mean"),
                "right_censoring_hazard_mean",
            ),
            right_censoring_hazard_max=_finite_float(
                data.get("right_censoring_hazard_max"),
                "right_censoring_hazard_max",
            ),
            protected_anchor_truncation_count=_strict_int(
                data.get("protected_anchor_truncation_count"),
                "protected_anchor_truncation_count",
            ),
            support_boundary_truncation_count=_strict_int(
                data.get("support_boundary_truncation_count"),
                "support_boundary_truncation_count",
            ),
            tied_event_count=_strict_int(
                data.get("tied_event_count"), "tied_event_count"
            ),
            skipped_event_count=_strict_int(
                data.get("skipped_event_count"), "skipped_event_count"
            ),
            status=HawkesResidualStatus(str(data.get("status", ""))),
            reason_codes=_string_tuple(data.get("reason_codes")),
            stratum_id=str(data.get("stratum_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class HawkesResidualPowerResultV1:
    """Deterministic false-positive/power evidence for one misspecification."""

    family: str
    test_method: str
    alternative_parameters: Mapping[str, float]
    sample_sizes: tuple[int, ...]
    false_positive_by_sample_size: Mapping[str, float]
    power_by_sample_size: Mapping[str, float]
    observed_support: int
    observed_power: float
    status: HawkesResidualStatus
    seed: int
    result_id: str = ""
    schema_version: str = HAWKES_RESIDUAL_POWER_RESULT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, HAWKES_RESIDUAL_POWER_RESULT_SCHEMA_VERSION
        )
        object.__setattr__(self, "family", _required_text(self.family))
        object.__setattr__(
            self, "test_method", _required_text(self.test_method)
        )
        parameters = _bounded_float_mapping(
            self.alternative_parameters, "alternative parameter", 16
        )
        if not parameters:
            raise ValueError("Hawkes residual power result lacks alternative")
        sizes = tuple(
            sorted(
                {
                    _bounded_int(item, "sample size", 8, MAX_RESIDUAL_EVENTS)
                    for item in self.sample_sizes
                }
            )
        )
        false_positive = _bounded_probability_mapping(
            self.false_positive_by_sample_size, "false-positive rate", 16
        )
        power = _bounded_probability_mapping(
            self.power_by_sample_size, "power", 16
        )
        if set(false_positive) != {str(item) for item in sizes} or set(
            power
        ) != set(false_positive):
            raise ValueError("Hawkes residual power regions differ")
        object.__setattr__(self, "alternative_parameters", parameters)
        object.__setattr__(self, "sample_sizes", sizes)
        object.__setattr__(
            self, "false_positive_by_sample_size", false_positive
        )
        object.__setattr__(self, "power_by_sample_size", power)
        object.__setattr__(
            self,
            "observed_support",
            _bounded_int(
                self.observed_support,
                "observed_support",
                0,
                MAX_RESIDUAL_EVENTS,
            ),
        )
        object.__setattr__(
            self,
            "observed_power",
            _probability(
                self.observed_power, "observed_power", open_interval=False
            ),
        )
        object.__setattr__(self, "status", HawkesResidualStatus(self.status))
        object.__setattr__(
            self, "seed", _bounded_int(self.seed, "seed", 0, 2**63 - 1)
        )
        expected = _stable_id("hawkes-residual-power-result", self.payload())
        if self.result_id and self.result_id != expected:
            raise ValueError("Hawkes residual power identity differs")
        object.__setattr__(self, "result_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "family": self.family,
            "test_method": self.test_method,
            "alternative_parameters": dict(self.alternative_parameters),
            "sample_sizes": list(self.sample_sizes),
            "false_positive_by_sample_size": dict(
                self.false_positive_by_sample_size
            ),
            "power_by_sample_size": dict(self.power_by_sample_size),
            "observed_support": self.observed_support,
            "observed_power": self.observed_power,
            "status": self.status.value,
            "seed": self.seed,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "result_id": self.result_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> HawkesResidualPowerResultV1:
        return cls(
            family=str(data.get("family", "")),
            test_method=str(data.get("test_method", "")),
            alternative_parameters={
                str(key): _finite_float(value, str(key))
                for key, value in _mapping(
                    data.get("alternative_parameters")
                ).items()
            },
            sample_sizes=tuple(
                _strict_int(item, "sample size")
                for item in _sequence(data.get("sample_sizes"))
            ),
            false_positive_by_sample_size={
                str(key): _finite_float(value, str(key))
                for key, value in _mapping(
                    data.get("false_positive_by_sample_size")
                ).items()
            },
            power_by_sample_size={
                str(key): _finite_float(value, str(key))
                for key, value in _mapping(
                    data.get("power_by_sample_size")
                ).items()
            },
            observed_support=_strict_int(
                data.get("observed_support"), "observed_support"
            ),
            observed_power=_finite_float(
                data.get("observed_power"), "observed_power"
            ),
            status=HawkesResidualStatus(str(data.get("status", ""))),
            seed=_strict_int(data.get("seed"), "seed"),
            result_id=str(data.get("result_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class HawkesResidualReportV1:
    """Content-addressed raw-proposal report with no residual/event rows."""

    engine_id: str
    config_id: str
    fit_id: str
    excitation_structure: HawkesExcitationStructure
    split_kind: str
    policy: HawkesResidualPolicyV1
    compensator_method: str
    applicability_reason: str
    conditioning_model_keys: tuple[str, ...]
    observation_scenario_ids: tuple[str, ...]
    window_identities: tuple[str, ...]
    strata: tuple[HawkesResidualStratumV1, ...]
    power_results: tuple[HawkesResidualPowerResultV1, ...]
    family_statuses: Mapping[str, HawkesResidualStatus]
    window_count: int
    event_count: int
    residual_count: int
    mark_residual_count: int
    reset_count: int
    right_censoring_count: int
    protected_anchor_truncation_count: int
    support_boundary_truncation_count: int
    tied_event_count: int
    skipped_event_count: int
    status: HawkesResidualStatus
    reason_codes: tuple[str, ...]
    implementation_sha256: str
    report_id: str = ""
    schema_version: str = HAWKES_RESIDUAL_REPORT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, HAWKES_RESIDUAL_REPORT_SCHEMA_VERSION
        )
        for name in (
            "engine_id",
            "config_id",
            "fit_id",
            "compensator_method",
            "applicability_reason",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self,
            "excitation_structure",
            HawkesExcitationStructure(self.excitation_structure),
        )
        if self.split_kind not in {"validation", "final_holdout"}:
            raise ValueError("Hawkes residual report split is not protected")
        if not isinstance(self.policy, HawkesResidualPolicyV1):
            raise TypeError("Hawkes residual report policy must use v1")
        models = tuple(
            sorted(
                {_required_text(item) for item in self.conditioning_model_keys}
            )
        )
        scenarios = tuple(
            sorted(
                {_required_text(item) for item in self.observation_scenario_ids}
            )
        )
        window_identities = tuple(
            sorted({_required_text(item) for item in self.window_identities})
        )
        strata = tuple(sorted(self.strata, key=lambda item: item.stratum_id))
        powers = tuple(sorted(self.power_results, key=lambda item: item.family))
        if len(strata) > MAX_RESIDUAL_STRATA:
            raise ValueError("Hawkes residual report strata exceed bound")
        if len({item.stratum_id for item in strata}) != len(strata):
            raise ValueError("Hawkes residual report strata duplicate")
        if len({item.family for item in powers}) != len(powers):
            raise ValueError("Hawkes residual power families duplicate")
        object.__setattr__(self, "conditioning_model_keys", models)
        object.__setattr__(self, "observation_scenario_ids", scenarios)
        object.__setattr__(self, "window_identities", window_identities)
        object.__setattr__(self, "strata", strata)
        object.__setattr__(self, "power_results", powers)
        expected_families = {
            "time_uniformity",
            "time_serial_dependence",
            "mark_calibration",
        }
        statuses = {
            _required_text(key): HawkesResidualStatus(value)
            for key, value in sorted(self.family_statuses.items())
        }
        if set(statuses) != expected_families:
            raise ValueError("Hawkes residual family status coverage differs")
        object.__setattr__(self, "family_statuses", statuses)
        for name in (
            "window_count",
            "event_count",
            "residual_count",
            "mark_residual_count",
            "reset_count",
            "right_censoring_count",
            "protected_anchor_truncation_count",
            "support_boundary_truncation_count",
            "tied_event_count",
            "skipped_event_count",
        ):
            object.__setattr__(
                self,
                name,
                _bounded_int(getattr(self, name), name, 0, MAX_RESIDUAL_EVENTS),
            )
        if self.mark_residual_count > self.residual_count:
            raise ValueError("Hawkes report mark support exceeds time support")
        object.__setattr__(self, "status", HawkesResidualStatus(self.status))
        reasons = _text_tuple(self.reason_codes, allow_empty=False)
        object.__setattr__(self, "reason_codes", reasons)
        object.__setattr__(
            self,
            "implementation_sha256",
            _sha256(self.implementation_sha256, "implementation_sha256"),
        )
        if self.status is HawkesResidualStatus.REFUSED:
            if strata or powers or self.residual_count or models:
                raise ValueError(
                    "refused Hawkes residual report contains usable evidence"
                )
        elif (
            not strata
            or not models
            or not scenarios
            or len(window_identities) != self.window_count
        ):
            raise ValueError("Hawkes residual report lacks analytic evidence")
        expected = _stable_id("hawkes-residual-report", self.payload())
        if self.report_id and self.report_id != expected:
            raise ValueError("Hawkes residual report identity differs")
        object.__setattr__(self, "report_id", expected)
        if len(self.to_json().encode("utf-8")) > MAX_REPORT_BYTES:
            raise ValueError("Hawkes residual report exceeds artifact bound")

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "engine_id": self.engine_id,
            "config_id": self.config_id,
            "fit_id": self.fit_id,
            "excitation_structure": self.excitation_structure.value,
            "split_kind": self.split_kind,
            "diagnostic_stage": HawkesResidualStage.RAW_PROPOSAL.value,
            "policy": self.policy.to_dict(),
            "compensator_method": self.compensator_method,
            "applicability_reason": self.applicability_reason,
            "conditioning_model_keys": list(self.conditioning_model_keys),
            "observation_scenario_ids": list(self.observation_scenario_ids),
            "window_identities": list(self.window_identities),
            "strata": [item.to_dict() for item in self.strata],
            "power_results": [item.to_dict() for item in self.power_results],
            "family_statuses": {
                key: value.value for key, value in self.family_statuses.items()
            },
            "window_count": self.window_count,
            "event_count": self.event_count,
            "residual_count": self.residual_count,
            "mark_residual_count": self.mark_residual_count,
            "reset_count": self.reset_count,
            "right_censoring_count": self.right_censoring_count,
            "protected_anchor_truncation_count": (
                self.protected_anchor_truncation_count
            ),
            "support_boundary_truncation_count": (
                self.support_boundary_truncation_count
            ),
            "tied_event_count": self.tied_event_count,
            "skipped_event_count": self.skipped_event_count,
            "status": self.status.value,
            "reason_codes": list(self.reason_codes),
            "implementation_sha256": self.implementation_sha256,
            "analytic_compensator_applies_before_carving": True,
            "analytic_compensator_applies_to_final_product": False,
            "final_product_requires_simulation_predictive_report": True,
            "failed_or_skipped_events_silently_discarded": False,
            "right_censoring_retained": True,
            "residual_rows_embedded": False,
            "event_rows_embedded": False,
            "historical_truth_claim": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "report_id": self.report_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> HawkesResidualReportV1:
        fixed = {
            "diagnostic_stage": HawkesResidualStage.RAW_PROPOSAL.value,
            "analytic_compensator_applies_before_carving": True,
            "analytic_compensator_applies_to_final_product": False,
            "final_product_requires_simulation_predictive_report": True,
            "failed_or_skipped_events_silently_discarded": False,
            "right_censoring_retained": True,
            "residual_rows_embedded": False,
            "event_rows_embedded": False,
            "historical_truth_claim": False,
        }
        if any(data.get(key) != value for key, value in fixed.items()):
            raise ValueError("Hawkes residual report scope or nonclaim differs")
        return cls(
            engine_id=str(data.get("engine_id", "")),
            config_id=str(data.get("config_id", "")),
            fit_id=str(data.get("fit_id", "")),
            excitation_structure=HawkesExcitationStructure(
                str(data.get("excitation_structure", ""))
            ),
            split_kind=str(data.get("split_kind", "")),
            policy=HawkesResidualPolicyV1.from_dict(
                _mapping(data.get("policy"))
            ),
            compensator_method=str(data.get("compensator_method", "")),
            applicability_reason=str(data.get("applicability_reason", "")),
            conditioning_model_keys=_string_tuple(
                data.get("conditioning_model_keys")
            ),
            observation_scenario_ids=_string_tuple(
                data.get("observation_scenario_ids")
            ),
            window_identities=_string_tuple(data.get("window_identities")),
            strata=tuple(
                HawkesResidualStratumV1.from_dict(_mapping(item))
                for item in _sequence(data.get("strata"))
            ),
            power_results=tuple(
                HawkesResidualPowerResultV1.from_dict(_mapping(item))
                for item in _sequence(data.get("power_results"))
            ),
            family_statuses={
                str(key): HawkesResidualStatus(str(value))
                for key, value in _mapping(data.get("family_statuses")).items()
            },
            window_count=_strict_int(data.get("window_count"), "window_count"),
            event_count=_strict_int(data.get("event_count"), "event_count"),
            residual_count=_strict_int(
                data.get("residual_count"), "residual_count"
            ),
            mark_residual_count=_strict_int(
                data.get("mark_residual_count"), "mark_residual_count"
            ),
            reset_count=_strict_int(data.get("reset_count"), "reset_count"),
            right_censoring_count=_strict_int(
                data.get("right_censoring_count"), "right_censoring_count"
            ),
            protected_anchor_truncation_count=_strict_int(
                data.get("protected_anchor_truncation_count"),
                "protected_anchor_truncation_count",
            ),
            support_boundary_truncation_count=_strict_int(
                data.get("support_boundary_truncation_count"),
                "support_boundary_truncation_count",
            ),
            tied_event_count=_strict_int(
                data.get("tied_event_count"), "tied_event_count"
            ),
            skipped_event_count=_strict_int(
                data.get("skipped_event_count"), "skipped_event_count"
            ),
            status=HawkesResidualStatus(str(data.get("status", ""))),
            reason_codes=_string_tuple(data.get("reason_codes")),
            implementation_sha256=str(data.get("implementation_sha256", "")),
            report_id=str(data.get("report_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class _ResidualSample:
    window_id: str
    symbol: str
    epoch_id: str
    session: str
    event_state: str
    observation_scenario_id: str
    integrated_hazard: float
    time_pit: float
    duration_seconds: float
    previous_mark: str | None
    predicted_mark_probabilities: Mapping[str, float]
    observed_mark: str | None
    mark_pit: float | None


@dataclass(frozen=True, slots=True)
class _WindowEvidence:
    window_id: str
    symbols: tuple[str, ...]
    right_censoring_hazards: tuple[float, ...]
    reset_count: int
    protected_anchor_truncation_count: int
    support_boundary_truncation_count: int
    tied_event_count: int
    skipped_event_count: int
    conditioning_model_key: str


def evaluate_marked_hawkes_residuals(
    config: MarkedHawkesConfigV1,
    fit: MarkedHawkesFitResultV1,
    windows: Sequence[HawkesResidualWindowV1],
    *,
    engine_id: str,
    policy: HawkesResidualPolicyV1 | None = None,
) -> tuple[HawkesResidualReportV1, ...]:
    """Evaluate exact raw-proposal residuals on protected windows by split."""
    if not isinstance(config, MarkedHawkesConfigV1):
        raise TypeError("Hawkes residual adapter requires marked Hawkes config")
    if not isinstance(fit, MarkedHawkesFitResultV1):
        raise TypeError("Hawkes residual adapter requires marked Hawkes fit")
    if fit.config_id != config.config_id:
        raise ValueError("Hawkes residual config and fit differ")
    selected_policy = policy or HawkesResidualPolicyV1()
    selected_windows = tuple(windows)
    if len(selected_windows) > MAX_RESIDUAL_WINDOWS:
        raise ValueError("Hawkes residual windows exceed bound")
    return tuple(
        _evaluate_split(
            config,
            fit,
            tuple(
                item
                for item in selected_windows
                if item.split_kind == split_kind
            ),
            engine_id=_required_text(engine_id),
            split_kind=split_kind,
            policy=selected_policy,
        )
        for split_kind in ("validation", "final_holdout")
    )


def _evaluate_split(
    config: MarkedHawkesConfigV1,
    fit: MarkedHawkesFitResultV1,
    windows: Sequence[HawkesResidualWindowV1],
    *,
    engine_id: str,
    split_kind: str,
    policy: HawkesResidualPolicyV1,
) -> HawkesResidualReportV1:
    if fit.status is not MarkedHawkesFitStatus.FITTED:
        return _refused_report(
            config,
            fit,
            engine_id=engine_id,
            split_kind=split_kind,
            policy=policy,
            reason="fitted_hawkes_parameters_unavailable",
        )
    if not windows:
        return _refused_report(
            config,
            fit,
            engine_id=engine_id,
            split_kind=split_kind,
            policy=policy,
            reason="protected_split_windows_unavailable",
        )
    samples: list[_ResidualSample] = []
    evidence: list[_WindowEvidence] = []
    for window in windows:
        window_samples, window_evidence = _window_residual_samples(
            config, fit, window, policy
        )
        samples.extend(window_samples)
        evidence.append(window_evidence)
    if len(samples) > MAX_RESIDUAL_EVENTS:
        raise ValueError("Hawkes residual sample count exceeds bound")
    grouped: dict[tuple[str, str], list[_ResidualSample]] = defaultdict(list)
    grouped[("overall", "all")].extend(samples)
    for sample in samples:
        for dimension, key in (
            ("symbol", sample.symbol),
            ("epoch", sample.epoch_id),
            ("session", sample.session),
            ("event_state", sample.event_state),
            ("observation_scenario", sample.observation_scenario_id),
        ):
            grouped[(dimension, key)].append(sample)
    preliminary = tuple(
        _summarize_stratum(
            dimension,
            key,
            values,
            evidence,
            policy=policy,
        )
        for (dimension, key), values in sorted(grouped.items())
    )
    strata = _apply_multiplicity(preliminary, policy)
    overall = next(
        item
        for item in strata
        if item.dimension == "overall" and item.key == "all"
    )
    powers = run_hawkes_residual_power_study(
        policy,
        observed_time_support=overall.sample_count,
        observed_mark_support=overall.mark_sample_count,
    )
    family_statuses = _family_statuses(overall, powers, policy)
    if HawkesResidualStatus.FAILED in family_statuses.values():
        status = HawkesResidualStatus.FAILED
        reasons = ("one_or_more_analytic_residual_families_failed",)
    elif HawkesResidualStatus.INSUFFICIENT_EVIDENCE in family_statuses.values():
        status = HawkesResidualStatus.INSUFFICIENT_EVIDENCE
        reasons = ("one_or_more_analytic_residual_families_underpowered",)
    else:
        status = HawkesResidualStatus.PASSED
        reasons = ("analytic_raw_proposal_residual_families_passed",)
    return HawkesResidualReportV1(
        engine_id=engine_id,
        config_id=config.config_id,
        fit_id=fit.fit_id,
        excitation_structure=config.excitation_structure,
        split_kind=split_kind,
        policy=policy,
        compensator_method=EXACT_COMPENSATOR_METHOD,
        applicability_reason=(
            "fitted_exponential_kernel_hawkes_before_carving_and_reconciliation"
        ),
        conditioning_model_keys=tuple(
            item.conditioning_model_key for item in evidence
        ),
        observation_scenario_ids=tuple(
            item.observation_scenario_id for item in windows
        ),
        window_identities=tuple(item.window_identity for item in windows),
        strata=strata,
        power_results=powers,
        family_statuses=family_statuses,
        window_count=len(windows),
        event_count=sum(len(item.events) for item in windows),
        residual_count=len(samples),
        mark_residual_count=sum(item.mark_pit is not None for item in samples),
        reset_count=sum(item.reset_count for item in evidence),
        right_censoring_count=sum(
            len(item.right_censoring_hazards) for item in evidence
        ),
        protected_anchor_truncation_count=sum(
            item.protected_anchor_truncation_count for item in evidence
        ),
        support_boundary_truncation_count=sum(
            item.support_boundary_truncation_count for item in evidence
        ),
        tied_event_count=sum(item.tied_event_count for item in evidence),
        skipped_event_count=sum(item.skipped_event_count for item in evidence),
        status=status,
        reason_codes=reasons,
        implementation_sha256=_implementation_sha256(),
    )


def _refused_report(
    config: MarkedHawkesConfigV1,
    fit: MarkedHawkesFitResultV1,
    *,
    engine_id: str,
    split_kind: str,
    policy: HawkesResidualPolicyV1,
    reason: str,
) -> HawkesResidualReportV1:
    return HawkesResidualReportV1(
        engine_id=engine_id,
        config_id=config.config_id,
        fit_id=fit.fit_id,
        excitation_structure=config.excitation_structure,
        split_kind=split_kind,
        policy=policy,
        compensator_method=EXACT_COMPENSATOR_METHOD,
        applicability_reason=reason,
        conditioning_model_keys=(),
        observation_scenario_ids=(),
        window_identities=(),
        strata=(),
        power_results=(),
        family_statuses={
            "time_uniformity": HawkesResidualStatus.REFUSED,
            "time_serial_dependence": HawkesResidualStatus.REFUSED,
            "mark_calibration": HawkesResidualStatus.REFUSED,
        },
        window_count=0,
        event_count=0,
        residual_count=0,
        mark_residual_count=0,
        reset_count=0,
        right_censoring_count=0,
        protected_anchor_truncation_count=0,
        support_boundary_truncation_count=0,
        tied_event_count=0,
        skipped_event_count=0,
        status=HawkesResidualStatus.REFUSED,
        reason_codes=(reason,),
        implementation_sha256=_implementation_sha256(),
    )


def _window_residual_samples(
    config: MarkedHawkesConfigV1,
    fit: MarkedHawkesFitResultV1,
    window: HawkesResidualWindowV1,
    policy: HawkesResidualPolicyV1,
) -> tuple[tuple[_ResidualSample, ...], _WindowEvidence]:
    model_key, model = _conditioning_model(fit, window)
    symbols = tuple(str(item) for item in _sequence(model.get("symbols")))
    if symbols != fit.symbols:
        raise ValueError("Hawkes residual conditioning symbols differ")
    symbol_index = {symbol: index for index, symbol in enumerate(symbols)}
    dimension = len(symbols)
    decay = _positive_float(model.get("decay_per_second"), "decay_per_second")
    baseline = _float_vector(
        model.get("baseline_rates_per_second"), dimension, "baseline rates"
    )
    excitation = _float_matrix(
        model.get("excitation_matrix"), dimension, "excitation matrix"
    )
    immigrant_marks = _mapping(
        model.get("immigrant_mark_probabilities"), "immigrant marks"
    )
    excitation_marks = _mapping(
        model.get("excitation_mark_probabilities"), "excitation marks"
    )
    transition_counts = (
        _mapping(model.get("mark_transition_counts"), "mark transition counts")
        if config.mark_policy == TRANSITION_CONDITIONED_MARK_POLICY
        else None
    )
    recursion = [0.0] * dimension
    accumulated = [0.0] * dimension
    previous_mark: dict[str, str] = {}
    previous_event_ns = dict.fromkeys(symbols, window.start_ns)
    previous_global_ns = window.start_ns
    event_marks = _window_marks(window.events)
    samples: list[_ResidualSample] = []
    tied_count = 0
    skipped_count = 0
    for event_time_ns, grouped_values in groupby(
        window.events, key=lambda item: item.event_time_ns
    ):
        grouped = tuple(grouped_values)
        elapsed = (event_time_ns - previous_global_ns) / NANOSECONDS_PER_SECOND
        _accumulate_hazard(
            accumulated,
            recursion,
            baseline,
            excitation,
            decay,
            elapsed,
        )
        factor = math.exp(-decay * max(0.0, elapsed))
        recursion = [value * factor for value in recursion]
        destination_counts: Counter[int] = Counter()
        for event in grouped:
            try:
                destination = symbol_index[event.symbol]
            except KeyError as err:
                raise ValueError(
                    "Hawkes residual event symbol is not fitted"
                ) from err
            destination_counts[destination] += 1
            hazard = accumulated[destination]
            if destination_counts[destination] > 1:
                tied_count += 1
            mark = event_marks.get(event.benchmark_event_id, "")
            if (
                event.anchor_id is None
                and math.isfinite(hazard)
                and hazard > 0.0
            ):
                mark_probabilities = _predictive_mark_probabilities(
                    config,
                    model,
                    destination=destination,
                    symbols=symbols,
                    baseline=baseline,
                    excitation=excitation,
                    recursion=recursion,
                    immigrant_marks=immigrant_marks,
                    excitation_marks=excitation_marks,
                    transition_counts=transition_counts,
                    previous_mark=previous_mark.get(event.symbol),
                )
                mark_pit = (
                    _randomized_mark_pit(
                        mark_probabilities,
                        mark,
                        semantic_key=(
                            f"{policy.semantic_mark_seed}|{fit.fit_id}|"
                            f"{window.window_id}|{event.benchmark_event_id}"
                        ),
                    )
                    if mark
                    else None
                )
                duration = (
                    event.event_time_ns - previous_event_ns[event.symbol]
                ) / NANOSECONDS_PER_SECOND
                samples.append(
                    _ResidualSample(
                        window_id=window.window_id,
                        symbol=event.symbol,
                        epoch_id=window.epoch_id,
                        session=window.session,
                        event_state=event.event_state,
                        observation_scenario_id=window.observation_scenario_id,
                        integrated_hazard=hazard,
                        time_pit=1.0 - math.exp(-hazard),
                        duration_seconds=max(0.0, duration),
                        previous_mark=previous_mark.get(event.symbol),
                        predicted_mark_probabilities=mark_probabilities,
                        observed_mark=mark or None,
                        mark_pit=mark_pit,
                    )
                )
            elif event.anchor_id is None:
                skipped_count += 1
            accumulated[destination] = 0.0
            previous_event_ns[event.symbol] = event.event_time_ns
            if mark:
                previous_mark[event.symbol] = mark
        for event in grouped:
            recursion[symbol_index[event.symbol]] += 1.0
        previous_global_ns = event_time_ns
    elapsed = (window.end_ns - previous_global_ns) / NANOSECONDS_PER_SECOND
    _accumulate_hazard(
        accumulated,
        recursion,
        baseline,
        excitation,
        decay,
        elapsed,
    )
    return tuple(samples), _WindowEvidence(
        window_id=window.window_id,
        symbols=symbols,
        right_censoring_hazards=tuple(accumulated),
        reset_count=len(symbols),
        protected_anchor_truncation_count=window.protected_anchor_truncation_count,
        support_boundary_truncation_count=window.support_boundary_truncation_count,
        tied_event_count=tied_count,
        skipped_event_count=skipped_count,
        conditioning_model_key=model_key,
    )


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


def _conditioning_model(
    fit: MarkedHawkesFitResultV1,
    window: HawkesResidualWindowV1,
) -> tuple[str, Mapping[str, Any]]:
    models = _mapping(
        fit.parameters.get("conditioning_models"), "conditioning models"
    )
    for key in (
        f"exact|{window.epoch_id}|{window.session}",
        f"session|{window.session}",
    ):
        if key in models:
            return key, _mapping(models[key], f"conditioning model {key}")
    raise ValueError("Hawkes residual window lacks a fitted conditioning model")


def _accumulate_hazard(
    accumulated: list[float],
    recursion: Sequence[float],
    baseline: Sequence[float],
    excitation: Sequence[Sequence[float]],
    decay: float,
    elapsed_seconds: float,
) -> None:
    elapsed = max(0.0, _finite_float(elapsed_seconds, "elapsed_seconds"))
    kernel_mass = 1.0 - math.exp(-decay * elapsed)
    for destination in range(len(baseline)):
        increment = baseline[destination] * elapsed + sum(
            excitation[destination][source] * recursion[source] * kernel_mass
            for source in range(len(recursion))
        )
        if not math.isfinite(increment) or increment < 0.0:
            raise ValueError("Hawkes compensator increment is invalid")
        accumulated[destination] += increment


def _predictive_mark_probabilities(
    config: MarkedHawkesConfigV1,
    model: Mapping[str, Any],
    *,
    destination: int,
    symbols: Sequence[str],
    baseline: Sequence[float],
    excitation: Sequence[Sequence[float]],
    recursion: Sequence[float],
    immigrant_marks: Mapping[str, Any],
    excitation_marks: Mapping[str, Any],
    transition_counts: Mapping[str, Any] | None,
    previous_mark: str | None,
) -> dict[str, float]:
    destination_symbol = symbols[destination]
    weights = [
        baseline[destination],
        *(
            _positive_or_zero(
                model,
                excitation[destination][source] * recursion[source],
                "excitation mark mixture weight",
            )
            for source in range(len(symbols))
        ),
    ]
    # The beta factor is common to every excited component but not the
    # immigrant component, so restore it before normalizing the mixture.
    decay = _positive_float(model.get("decay_per_second"), "decay_per_second")
    weights = [weights[0], *(decay * item for item in weights[1:])]
    component_probabilities: list[Mapping[str, Any]] = [
        _mapping(immigrant_marks[destination_symbol], "immigrant mark cell")
    ]
    by_source = _mapping(
        excitation_marks[destination_symbol], "excitation destination marks"
    )
    component_probabilities.extend(
        _mapping(by_source[source], f"excitation mark source {source}")
        for source in symbols
    )
    total = sum(weights)
    if total <= 0.0:
        raise ValueError("Hawkes mark predictive intensity is nonpositive")
    result = dict.fromkeys(MARK_STATES, 0.0)
    for weight, component in zip(weights, component_probabilities):
        probabilities = _normalized_mark_probabilities(component)
        if transition_counts is not None:
            probabilities = _transition_conditioned_probabilities(
                transition_counts,
                destination_symbol=destination_symbol,
                previous_mark=previous_mark,
                component_probabilities=probabilities,
                smoothing_count=config.mark_smoothing_count,
            )
        for mark in MARK_STATES:
            result[mark] += weight / total * probabilities[mark]
    return _normalized_mark_probabilities(result)


def _positive_or_zero(
    _context: Mapping[str, Any], value: float, name: str
) -> float:
    selected = _finite_float(value, name)
    if selected < 0.0:
        raise ValueError(f"{name} must be nonnegative")
    return selected


def _normalized_mark_probabilities(
    values: Mapping[str, Any],
) -> dict[str, float]:
    if set(values) != set(MARK_STATES):
        raise ValueError("Hawkes predictive mark states differ")
    selected = {
        mark: _finite_float(values[mark], f"mark probability {mark}")
        for mark in MARK_STATES
    }
    if any(value < 0.0 for value in selected.values()):
        raise ValueError("Hawkes predictive mark probability is negative")
    total = sum(selected.values())
    if total <= 0.0:
        raise ValueError(
            "Hawkes predictive mark probability mass is nonpositive"
        )
    return {mark: selected[mark] / total for mark in MARK_STATES}


def _transition_conditioned_probabilities(
    transition_counts: Mapping[str, Any],
    *,
    destination_symbol: str,
    previous_mark: str | None,
    component_probabilities: Mapping[str, float],
    smoothing_count: float,
) -> dict[str, float]:
    if previous_mark not in MARK_STATES:
        return dict(component_probabilities)
    assert previous_mark is not None
    by_previous = _mapping(
        transition_counts[destination_symbol], "mark transition destination"
    )
    row = _mapping(by_previous[previous_mark], "mark transition row")
    smoothing = _positive_float(smoothing_count, "mark_smoothing_count")
    return _normalized_mark_probabilities(
        {
            mark: _finite_float(row[mark], f"mark transition count {mark}")
            + smoothing * component_probabilities[mark]
            for mark in MARK_STATES
        }
    )


def _randomized_mark_pit(
    probabilities: Mapping[str, float],
    mark: str,
    *,
    semantic_key: str,
) -> float:
    if mark not in MARK_STATES:
        raise ValueError("randomized mark PIT received unsupported mark")
    normalized = _normalized_mark_probabilities(probabilities)
    lower = sum(
        normalized[item] for item in MARK_STATES[: MARK_STATES.index(mark)]
    )
    digest = hashlib.sha256(semantic_key.encode("utf-8")).digest()
    randomizer = int.from_bytes(digest[:8], "big") / 2**64
    return min(1.0, max(0.0, lower + normalized[mark] * randomizer))


def _summarize_stratum(
    dimension: str,
    key: str,
    samples: Sequence[_ResidualSample],
    evidence: Sequence[_WindowEvidence],
    *,
    policy: HawkesResidualPolicyV1,
) -> HawkesResidualStratumV1:
    hazards = tuple(item.integrated_hazard for item in samples)
    pits = tuple(item.time_pit for item in samples)
    mark_samples = tuple(item for item in samples if item.mark_pit is not None)
    mark_pits = tuple(cast(float, item.mark_pit) for item in mark_samples)
    time_ks = _uniform_ks(pits) if pits else None
    time_p = (
        _ks_uniform_p_value(time_ks, len(pits)) if time_ks is not None else None
    )
    lag = _lag_one_correlation(pits) if len(pits) >= 3 else None
    lag_p = (
        _lag_correlation_p_value(lag, len(pits)) if lag is not None else None
    )
    mark_ks = _uniform_ks(mark_pits) if mark_pits else None
    mark_p = (
        _ks_uniform_p_value(mark_ks, len(mark_pits))
        if mark_ks is not None
        else None
    )
    log_score = (
        statistics.fmean(
            -math.log(
                max(
                    1e-15,
                    item.predicted_mark_probabilities[
                        cast(str, item.observed_mark)
                    ],
                )
            )
            for item in mark_samples
        )
        if mark_samples
        else None
    )
    brier = (
        statistics.fmean(
            sum(
                (
                    item.predicted_mark_probabilities[mark]
                    - float(item.observed_mark == mark)
                )
                ** 2
                for mark in MARK_STATES
            )
            for item in mark_samples
        )
        if mark_samples
        else None
    )
    windows = {item.window_id for item in samples}
    selected_evidence = tuple(
        item for item in evidence if item.window_id in windows
    )
    censoring = tuple(
        value
        for item in selected_evidence
        for value in item.right_censoring_hazards
    )
    observed_marks = {cast(str, item.observed_mark) for item in mark_samples}
    minimum = (
        policy.minimum_residual_count
        if dimension == "overall"
        else policy.minimum_stratum_count
    )
    reasons: list[str] = []
    if len(pits) < minimum or len(windows) < min(
        policy.minimum_window_count, minimum
    ):
        status = HawkesResidualStatus.INSUFFICIENT_EVIDENCE
        reasons.append("residual_cell_support_below_policy_minimum")
    elif (
        time_ks is None
        or time_ks > PRACTICAL_RESIDUAL_TOLERANCE
        or lag is None
        or abs(lag) > policy.maximum_absolute_lag1
        or (mark_ks is not None and mark_ks > PRACTICAL_RESIDUAL_TOLERANCE)
    ):
        status = HawkesResidualStatus.FAILED
        reasons.append("raw_proposal_residual_cell_practical_gate_failed")
    else:
        status = HawkesResidualStatus.PASSED
        reasons.append("raw_proposal_residual_cell_practical_gate_passed")
    if any(
        value is not None and value < policy.alpha
        for value in (time_p, lag_p, mark_p)
    ):
        reasons.append("raw_proposal_exact_null_rejected_descriptively")
    missing = tuple(mark for mark in MARK_STATES if mark not in observed_marks)
    if missing:
        reasons.append("one_or_more_mark_states_unsupported_in_cell")
    conditional_groups: dict[str, list[float]] = defaultdict(list)
    for item in samples:
        conditional_groups[
            f"previous_mark:{item.previous_mark or 'window_reset'}"
        ].append(item.time_pit)
        conditional_groups[
            f"duration:{_duration_bin(item.duration_seconds)}"
        ].append(item.time_pit)
    confusion = Counter(
        f"predicted:{max(MARK_STATES, key=item.predicted_mark_probabilities.__getitem__)}"
        f"|observed:{item.observed_mark}"
        for item in mark_samples
    )
    return HawkesResidualStratumV1(
        dimension=dimension,
        key=key,
        window_count=len(windows),
        sample_count=len(pits),
        mark_sample_count=len(mark_pits),
        time_uniform_ks=time_ks,
        time_uniform_p_value=time_p,
        time_uniform_adjusted_p_value=None,
        time_lag1_autocorrelation=lag,
        time_lag1_p_value=lag_p,
        time_lag1_adjusted_p_value=None,
        mark_uniform_ks=mark_ks,
        mark_uniform_p_value=mark_p,
        mark_uniform_adjusted_p_value=None,
        integrated_hazard_quantiles={
            f"q{round(level * 100):02d}": _quantile(hazards, level)
            for level in (0.01, 0.10, 0.25, 0.50, 0.75, 0.90, 0.99)
        },
        pit_tail_rates={
            "below_0_01": _rate(value < 0.01 for value in pits),
            "above_0_99": _rate(value > 0.99 for value in pits),
        },
        mark_log_score=log_score,
        mark_brier_score=brier,
        mark_calibration_bins=_mark_calibration_bins(mark_samples, policy),
        transition_confusion_counts=dict(confusion),
        conditional_pit_means={
            name: statistics.fmean(values)
            for name, values in sorted(conditional_groups.items())
        },
        missing_mark_states=missing,
        reset_count=sum(item.reset_count for item in selected_evidence),
        right_censoring_count=len(censoring),
        right_censoring_hazard_mean=(
            statistics.fmean(censoring) if censoring else 0.0
        ),
        right_censoring_hazard_max=(max(censoring) if censoring else 0.0),
        protected_anchor_truncation_count=sum(
            item.protected_anchor_truncation_count for item in selected_evidence
        ),
        support_boundary_truncation_count=sum(
            item.support_boundary_truncation_count for item in selected_evidence
        ),
        tied_event_count=sum(
            item.tied_event_count for item in selected_evidence
        ),
        skipped_event_count=sum(
            item.skipped_event_count for item in selected_evidence
        ),
        status=status,
        reason_codes=tuple(reasons),
    )


def _mark_calibration_bins(
    samples: Sequence[_ResidualSample], policy: HawkesResidualPolicyV1
) -> tuple[HawkesMarkCalibrationBinV1, ...]:
    grouped: dict[tuple[str, int], list[tuple[float, float]]] = defaultdict(
        list
    )
    count = policy.mark_calibration_bin_count
    for sample in samples:
        for mark in MARK_STATES:
            probability = sample.predicted_mark_probabilities[mark]
            index = min(count - 1, math.floor(probability * count))
            grouped[(mark, index)].append(
                (probability, float(sample.observed_mark == mark))
            )
    return tuple(
        HawkesMarkCalibrationBinV1(
            mark=mark,
            bin_index=index,
            lower_probability=index / count,
            upper_probability=(index + 1) / count,
            sample_count=len(values),
            mean_predicted_probability=statistics.fmean(
                item[0] for item in values
            ),
            observed_frequency=statistics.fmean(item[1] for item in values),
        )
        for (mark, index), values in sorted(grouped.items())
    )


def _duration_bin(value: float) -> str:
    if value < 0.01:
        return "lt_10ms"
    if value < 0.1:
        return "10_to_100ms"
    if value < 1.0:
        return "100ms_to_1s"
    return "gte_1s"


def _apply_multiplicity(
    strata: Sequence[HawkesResidualStratumV1],
    policy: HawkesResidualPolicyV1,
) -> tuple[HawkesResidualStratumV1, ...]:
    time_adjusted = _benjamini_hochberg(
        {
            item.stratum_id: item.time_uniform_p_value
            for item in strata
            if item.time_uniform_p_value is not None
        }
    )
    lag_adjusted = _benjamini_hochberg(
        {
            item.stratum_id: item.time_lag1_p_value
            for item in strata
            if item.time_lag1_p_value is not None
        }
    )
    mark_adjusted = _benjamini_hochberg(
        {
            item.stratum_id: item.mark_uniform_p_value
            for item in strata
            if item.mark_uniform_p_value is not None
        }
    )
    result: list[HawkesResidualStratumV1] = []
    for item in strata:
        reasons = list(item.reason_codes)
        adjusted_values = tuple(
            value
            for value in (
                time_adjusted.get(item.stratum_id),
                lag_adjusted.get(item.stratum_id),
                mark_adjusted.get(item.stratum_id),
            )
            if value is not None
        )
        if (
            item.status is not HawkesResidualStatus.INSUFFICIENT_EVIDENCE
            and any(value < policy.alpha for value in adjusted_values)
        ):
            reasons.append(
                "multiplicity_adjusted_exact_null_rejected_descriptively"
            )
        result.append(
            replace(
                item,
                time_uniform_adjusted_p_value=time_adjusted.get(
                    item.stratum_id
                ),
                time_lag1_adjusted_p_value=lag_adjusted.get(item.stratum_id),
                mark_uniform_adjusted_p_value=mark_adjusted.get(
                    item.stratum_id
                ),
                reason_codes=tuple(dict.fromkeys(reasons)),
                stratum_id="",
            )
        )
    return tuple(result)


def _family_statuses(
    overall: HawkesResidualStratumV1,
    powers: Sequence[HawkesResidualPowerResultV1],
    policy: HawkesResidualPolicyV1,
) -> dict[str, HawkesResidualStatus]:
    power_by_family = {item.family: item.status for item in powers}

    def selected_status(
        *,
        support: int,
        practical_effect: float | None,
        maximum_effect: float,
        observed_ok: bool,
        power_families: Sequence[str],
    ) -> HawkesResidualStatus:
        if support < policy.minimum_residual_count:
            return HawkesResidualStatus.INSUFFICIENT_EVIDENCE
        power_statuses = tuple(
            power_by_family.get(
                family, HawkesResidualStatus.INSUFFICIENT_EVIDENCE
            )
            for family in power_families
        )
        if HawkesResidualStatus.FAILED in power_statuses:
            return HawkesResidualStatus.FAILED
        if HawkesResidualStatus.REFUSED in power_statuses:
            return HawkesResidualStatus.REFUSED
        if any(
            status is not HawkesResidualStatus.PASSED
            for status in power_statuses
        ):
            return HawkesResidualStatus.INSUFFICIENT_EVIDENCE
        if practical_effect is None:
            return HawkesResidualStatus.INSUFFICIENT_EVIDENCE
        return (
            HawkesResidualStatus.PASSED
            if practical_effect <= maximum_effect and observed_ok
            else HawkesResidualStatus.FAILED
        )

    return {
        "time_uniformity": selected_status(
            support=overall.sample_count,
            practical_effect=overall.time_uniform_ks,
            maximum_effect=PRACTICAL_RESIDUAL_TOLERANCE,
            observed_ok=True,
            power_families=("wrong_baseline", "wrong_decay"),
        ),
        "time_serial_dependence": selected_status(
            support=overall.sample_count,
            practical_effect=(
                abs(overall.time_lag1_autocorrelation)
                if overall.time_lag1_autocorrelation is not None
                else None
            ),
            maximum_effect=policy.maximum_absolute_lag1,
            observed_ok=(overall.time_lag1_autocorrelation is not None),
            power_families=("wrong_excitation",),
        ),
        "mark_calibration": selected_status(
            support=overall.mark_sample_count,
            practical_effect=overall.mark_uniform_ks,
            maximum_effect=PRACTICAL_RESIDUAL_TOLERANCE,
            observed_ok=not overall.missing_mark_states,
            power_families=("wrong_mark_probabilities",),
        ),
    }


def run_hawkes_residual_power_study(
    policy: HawkesResidualPolicyV1,
    *,
    observed_time_support: int,
    observed_mark_support: int,
) -> tuple[HawkesResidualPowerResultV1, ...]:
    """Simulate wrong baseline, decay, excitation, and mark alternatives."""
    specifications = (
        (
            "wrong_baseline",
            "two_sided_uniform_ks",
            {"exponential_rate": 1.35},
            observed_time_support,
        ),
        (
            "wrong_decay",
            "two_sided_uniform_ks",
            {
                "fast_rate": 4.00,
                "slow_rate": 0.25,
                "mixture_weight": 0.50,
            },
            observed_time_support,
        ),
        (
            "wrong_excitation",
            "two_sided_lag1_correlation_test",
            {"lag_mixture_weight": 0.65},
            observed_time_support,
        ),
        (
            "wrong_mark_probabilities",
            "ordered_randomized_mark_pit_uniform_ks",
            {"pit_power": 1.60},
            observed_mark_support,
        ),
    )
    results: list[HawkesResidualPowerResultV1] = []
    for family, method, parameters, observed_support in specifications:
        seed = _semantic_seed(
            policy.semantic_mark_seed,
            POWER_STUDY_METHOD,
            family,
            policy.policy_id,
        )
        false_positive: dict[str, float] = {}
        power: dict[str, float] = {}
        for sample_size in policy.power_sample_sizes:
            null_rejections = 0
            alternative_rejections = 0
            for replication in range(policy.power_replications):
                null_rng = random.Random(
                    _semantic_seed(
                        seed, str(sample_size), str(replication), "null"
                    )
                )
                alternative_rng = random.Random(
                    _semantic_seed(
                        seed, str(sample_size), str(replication), "alternative"
                    )
                )
                null_values = tuple(
                    null_rng.random() for _ in range(sample_size)
                )
                alternative = _misspecified_values(
                    family, sample_size, alternative_rng
                )
                null_rejections += int(
                    _power_test_rejects(family, null_values, policy)
                )
                alternative_rejections += int(
                    _power_test_rejects(family, alternative, policy)
                )
            false_positive[str(sample_size)] = (
                null_rejections / policy.power_replications
            )
            power[str(sample_size)] = (
                alternative_rejections / policy.power_replications
            )
        observed_power = _interpolated_power(
            observed_support, policy.power_sample_sizes, power
        )
        if max(false_positive.values()) > policy.maximum_false_positive_rate:
            status = HawkesResidualStatus.FAILED
        elif observed_power < policy.minimum_power:
            status = HawkesResidualStatus.INSUFFICIENT_EVIDENCE
        else:
            status = HawkesResidualStatus.PASSED
        results.append(
            HawkesResidualPowerResultV1(
                family=family,
                test_method=method,
                alternative_parameters=parameters,
                sample_sizes=policy.power_sample_sizes,
                false_positive_by_sample_size=false_positive,
                power_by_sample_size=power,
                observed_support=observed_support,
                observed_power=observed_power,
                status=status,
                seed=seed,
            )
        )
    return tuple(results)


def _misspecified_values(
    family: str, sample_size: int, rng: random.Random
) -> tuple[float, ...]:
    if family == "wrong_baseline":
        return tuple(
            1.0 - math.exp(-rng.expovariate(1.35)) for _ in range(sample_size)
        )
    if family == "wrong_decay":
        return tuple(
            1.0
            - math.exp(-rng.expovariate(0.25 if rng.random() < 0.50 else 4.00))
            for _ in range(sample_size)
        )
    if family == "wrong_mark_probabilities":
        return tuple(rng.random() ** 1.60 for _ in range(sample_size))
    if family == "wrong_excitation":
        values = [rng.random()]
        for _ in range(1, sample_size):
            values.append(0.65 * values[-1] + 0.35 * rng.random())
        return tuple(values)
    raise ValueError(f"unsupported Hawkes residual misspecification: {family}")


def _power_test_rejects(
    family: str, values: Sequence[float], policy: HawkesResidualPolicyV1
) -> bool:
    if family == "wrong_excitation":
        lag = _lag_one_correlation(values)
        return (
            lag is None
            or _lag_correlation_p_value(lag, len(values)) < policy.alpha
        )
    statistic = _uniform_ks(values)
    return _ks_uniform_p_value(statistic, len(values)) < policy.alpha


def _interpolated_power(
    support: int,
    sample_sizes: Sequence[int],
    power: Mapping[str, float],
) -> float:
    if support <= 0:
        return 0.0
    ordered = tuple(sorted(sample_sizes))
    if support <= ordered[0]:
        return power[str(ordered[0])]
    if support >= ordered[-1]:
        return power[str(ordered[-1])]
    for left, right in pairwise(ordered):
        if left <= support <= right:
            fraction = (support - left) / (right - left)
            return power[str(left)] + fraction * (
                power[str(right)] - power[str(left)]
            )
    raise RuntimeError("Hawkes residual power interpolation failed")


def write_hawkes_residual_report(
    report: HawkesResidualReportV1, output_directory: str | Path
) -> ArtifactRef:
    """Atomically persist one row-free content-addressed analytic report."""
    if not isinstance(report, HawkesResidualReportV1):
        raise TypeError("Hawkes residual writer requires v1 report")
    encoded = report.to_json().encode("utf-8") + b"\n"
    root = Path(output_directory).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(encoded).hexdigest()
    target = root / f"hawkes-residual-report-{digest}.json"
    if target.exists() and target.read_bytes() != encoded:
        raise ValueError("Hawkes residual artifact content-address collision")
    if not target.exists():
        temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
        temporary.write_bytes(encoded)
        os.replace(temporary, target)
    ref = ArtifactRef(
        kind="hawkes_residual_report_v1",
        path=str(target),
        size_bytes=len(encoded),
        sha256=digest,
        metadata={
            "artifact_role": "raw_proposal_residual",
            "engine_id": report.engine_id,
            "config_id": report.config_id,
            "fit_id": report.fit_id,
            "split_kind": report.split_kind,
            "diagnostic_stage": HawkesResidualStage.RAW_PROPOSAL.value,
            "report_id": report.report_id,
            "status": report.status.value,
            "implementation_sha256": report.implementation_sha256,
        },
    )
    if read_hawkes_residual_report(ref.path) != report:
        raise ValueError("published Hawkes residual report differs on readback")
    return ref


def read_hawkes_residual_report(path: str | Path) -> HawkesResidualReportV1:
    """Read and hash-verify one content-addressed analytic report."""
    selected = Path(path).expanduser().resolve()
    content = selected.read_bytes()
    if len(content) > MAX_REPORT_BYTES:
        raise ValueError("Hawkes residual artifact exceeds size bound")
    digest = hashlib.sha256(content).hexdigest()
    if selected.name != f"hawkes-residual-report-{digest}.json":
        raise ValueError("Hawkes residual artifact filename/hash differ")
    payload = json.loads(content)
    if not isinstance(payload, dict):
        raise TypeError("Hawkes residual artifact payload is not an object")
    return HawkesResidualReportV1.from_dict(payload)


def verify_hawkes_residual_artifact_ref(
    ref: ArtifactRef,
) -> HawkesResidualReportV1:
    """Verify strong reference metadata and return the restored report."""
    if not isinstance(ref, ArtifactRef):
        raise TypeError("Hawkes residual reference must be an ArtifactRef")
    report = read_hawkes_residual_report(ref.path)
    content = Path(ref.path).expanduser().resolve().read_bytes()
    if (
        ref.kind != "hawkes_residual_report_v1"
        or ref.size_bytes != len(content)
        or ref.sha256 != hashlib.sha256(content).hexdigest()
        or ref.metadata.get("artifact_role") != "raw_proposal_residual"
        or ref.metadata.get("engine_id") != report.engine_id
        or ref.metadata.get("config_id") != report.config_id
        or ref.metadata.get("fit_id") != report.fit_id
        or ref.metadata.get("split_kind") != report.split_kind
        or ref.metadata.get("diagnostic_stage")
        != HawkesResidualStage.RAW_PROPOSAL.value
        or ref.metadata.get("report_id") != report.report_id
        or ref.metadata.get("status") != report.status.value
        or ref.metadata.get("implementation_sha256")
        != report.implementation_sha256
        or report.implementation_sha256 != _implementation_sha256()
    ):
        raise ValueError("Hawkes residual strong reference differs from report")
    return report


def _uniform_ks(values: Sequence[float]) -> float:
    ordered = tuple(
        sorted(
            _probability(item, "PIT", open_interval=False) for item in values
        )
    )
    if not ordered:
        raise ValueError("uniform KS requires samples")
    count = len(ordered)
    return max(
        max((index + 1) / count - value, value - index / count)
        for index, value in enumerate(ordered)
    )


def _ks_uniform_p_value(statistic: float, sample_count: int) -> float:
    if sample_count <= 0:
        return 0.0
    selected = _probability(statistic, "KS statistic", open_interval=False)
    scaled = (
        math.sqrt(sample_count) + 0.12 + 0.11 / math.sqrt(sample_count)
    ) * selected
    total = 0.0
    for index in range(1, 101):
        term = (
            2.0
            * (-1.0) ** (index - 1)
            * math.exp(-2.0 * index * index * scaled * scaled)
        )
        total += term
        if abs(term) < 1e-12:
            break
    return min(1.0, max(0.0, total))


def _lag_one_correlation(values: Sequence[float]) -> float | None:
    if len(values) < 3:
        return None
    left = tuple(values[:-1])
    right = tuple(values[1:])
    left_mean = statistics.fmean(left)
    right_mean = statistics.fmean(right)
    numerator = sum(
        (first - left_mean) * (second - right_mean)
        for first, second in zip(left, right)
    )
    left_scale = sum((value - left_mean) ** 2 for value in left)
    right_scale = sum((value - right_mean) ** 2 for value in right)
    denominator = math.sqrt(left_scale * right_scale)
    return 0.0 if denominator == 0.0 else numerator / denominator


def _lag_correlation_p_value(correlation: float, sample_count: int) -> float:
    if sample_count < 3:
        return 0.0
    selected = min(
        0.999999999999, abs(_finite_float(correlation, "correlation"))
    )
    statistic = selected * math.sqrt(
        (sample_count - 2) / max(1e-15, 1.0 - selected**2)
    )
    # Conservative two-sided normal approximation; the practical lag bound is
    # the decision gate and this probability participates in multiplicity.
    return min(1.0, math.erfc(statistic / math.sqrt(2.0)))


def _benjamini_hochberg(values: Mapping[str, float | None]) -> dict[str, float]:
    ordered = sorted(
        (
            (_probability(value, key, open_interval=False), key)
            for key, value in values.items()
            if value is not None
        ),
        key=lambda item: (item[0], item[1]),
    )
    count = len(ordered)
    adjusted: dict[str, float] = {}
    running = 1.0
    for rank, (value, key) in reversed(tuple(enumerate(ordered, start=1))):
        running = min(running, value * count / rank)
        adjusted[key] = min(1.0, running)
    return adjusted


def _quantile(values: Sequence[float], level: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(_finite_float(value, "quantile value") for value in values)
    position = (len(ordered) - 1) * level
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] + fraction * (ordered[upper] - ordered[lower])


def _rate(values: Sequence[bool] | Any) -> float:
    selected = tuple(bool(item) for item in values)
    return sum(selected) / len(selected) if selected else 0.0


def _semantic_seed(base_seed: int, *values: str) -> int:
    digest = hashlib.sha256(
        canonical_contract_json(
            {"base_seed": base_seed, "values": list(values)}
        ).encode("utf-8")
    ).digest()
    return int.from_bytes(digest[:8], "big") & (2**63 - 1)


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _implementation_sha256() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def _sha256(value: Any, name: str) -> str:
    selected = _required_text(value).lower()
    if len(selected) != 64 or any(
        character not in "0123456789abcdef" for character in selected
    ):
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return selected


def _require_schema(value: str, expected: str) -> None:
    if value != expected:
        raise ValueError(f"unsupported schema version: {value!r}")


def _required_text(value: Any) -> str:
    selected = str(value).strip() if value is not None else ""
    if not selected:
        raise ValueError("required text is empty")
    return selected


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    return value


def _bounded_int(value: Any, name: str, lower: int, upper: int) -> int:
    selected = _strict_int(value, name)
    if not lower <= selected <= upper:
        raise ValueError(f"{name} is outside [{lower}, {upper}]")
    return selected


def _finite_float(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be numeric")
    selected = float(value)
    if not math.isfinite(selected):
        raise ValueError(f"{name} must be finite")
    return selected


def _positive_float(value: Any, name: str) -> float:
    selected = _finite_float(value, name)
    if selected <= 0.0:
        raise ValueError(f"{name} must be positive")
    return selected


def _probability(value: Any, name: str, *, open_interval: bool) -> float:
    selected = _finite_float(value, name)
    valid = 0.0 < selected < 1.0 if open_interval else 0.0 <= selected <= 1.0
    if not valid:
        raise ValueError(f"{name} must be a probability")
    return selected


def _optional_float(value: Any) -> float | None:
    return None if value is None else _finite_float(value, "optional float")


def _mapping(value: Any, name: str = "mapping") -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    return cast(Mapping[str, Any], value)


def _sequence(value: Any) -> tuple[Any, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise TypeError("value must be a sequence")
    return tuple(value)


def _string_tuple(value: Any) -> tuple[str, ...]:
    return tuple(_required_text(item) for item in _sequence(value))


def _text_tuple(values: Sequence[str], *, allow_empty: bool) -> tuple[str, ...]:
    selected = tuple(_required_text(item) for item in values)
    if not allow_empty and not selected:
        raise ValueError("text tuple is empty")
    return selected


def _bounded_float_mapping(
    values: Mapping[str, Any], name: str, maximum: int
) -> dict[str, float]:
    selected = {
        _required_text(key): _finite_float(value, f"{name} {key}")
        for key, value in sorted(values.items())
    }
    if len(selected) > maximum:
        raise ValueError(f"{name} mapping exceeds bound")
    return selected


def _bounded_probability_mapping(
    values: Mapping[str, Any], name: str, maximum: int
) -> dict[str, float]:
    selected = _bounded_float_mapping(values, name, maximum)
    if any(value < 0.0 or value > 1.0 for value in selected.values()):
        raise ValueError(f"{name} mapping contains a non-probability")
    return selected


def _float_vector(value: Any, size: int, name: str) -> list[float]:
    values = [_finite_float(item, name) for item in _sequence(value)]
    if len(values) != size:
        raise ValueError(f"{name} dimension differs")
    return values


def _float_matrix(value: Any, size: int, name: str) -> list[list[float]]:
    rows = [
        [_finite_float(item, name) for item in _sequence(row)]
        for row in _sequence(value)
    ]
    if len(rows) != size or any(len(row) != size for row in rows):
        raise ValueError(f"{name} dimensions differ")
    return rows


__all__ = [
    "DEFAULT_OBSERVATION_SCENARIO_ID",
    "EXACT_COMPENSATOR_METHOD",
    "HAWKES_MARK_CALIBRATION_BIN_SCHEMA_VERSION",
    "HAWKES_RESIDUAL_POLICY_SCHEMA_VERSION",
    "HAWKES_RESIDUAL_POWER_RESULT_SCHEMA_VERSION",
    "HAWKES_RESIDUAL_REPORT_SCHEMA_VERSION",
    "HAWKES_RESIDUAL_STRATUM_SCHEMA_VERSION",
    "HAWKES_RESIDUAL_WINDOW_SCHEMA_VERSION",
    "HawkesMarkCalibrationBinV1",
    "HawkesResidualPolicyV1",
    "HawkesResidualPowerResultV1",
    "HawkesResidualReportV1",
    "HawkesResidualStage",
    "HawkesResidualStatus",
    "HawkesResidualStratumV1",
    "HawkesResidualWindowV1",
    "evaluate_marked_hawkes_residuals",
    "read_hawkes_residual_report",
    "run_hawkes_residual_power_study",
    "verify_hawkes_residual_artifact_ref",
    "write_hawkes_residual_report",
]
