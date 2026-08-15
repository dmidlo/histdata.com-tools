"""Powered, process-aware qualification of HistData proposal portfolios.

The contracts in this module consume the exact row-free metric trace emitted by
the installed proposal evaluator.  Tick rows remain process-local.  The v2.4
executable boundary is HistData.com ASCII/T only; provider-neutral identity
seams do not admit OANDA, broker feeds, or alternate historical providers.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import random
import statistics
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from histdatacom.orchestration.reconstruction import (
    artifact_ref_for_file,
    verify_artifact_ref,
)
from histdatacom.reconstruction_experiment import (
    CURRENT_EXPERIMENT_PROVIDER_ID,
    ReconstructionExperimentManifestV1,
    ReconstructionExperimentRole,
    read_reconstruction_experiment,
    verify_reconstruction_experiment,
)
from histdatacom.runtime_contracts import ArtifactRef, JSONValue
from histdatacom.synthetic.benchmark_corpus import (
    BenchmarkWindowMetricObservationV1,
    BenchmarkWindowMetricTraceV1,
    ReverseDegradationBenchmarkCorpusV1,
    read_benchmark_window_metric_trace,
    read_reverse_degradation_benchmark_campaign,
    read_reverse_degradation_benchmark_corpus,
)
from histdatacom.synthetic.contracts import canonical_contract_json
from histdatacom.synthetic.proposal_engines import (
    ProposalEngineEvidenceV1,
    ProposalPortfolioEvaluationV1,
    proposal_engine_registry,
    read_proposal_portfolio_evaluation,
)

POWERED_QUALIFICATION_POLICY_SCHEMA_VERSION = (
    "histdatacom.powered-qualification-policy.v1"
)
POINT_PROCESS_RESIDUAL_INPUT_SCHEMA_VERSION = (
    "histdatacom.point-process-residual-input.v1"
)
POINT_PROCESS_RESIDUAL_REPORT_SCHEMA_VERSION = (
    "histdatacom.point-process-residual-report.v1"
)
PREDICTIVE_SCORE_REPORT_SCHEMA_VERSION = (
    "histdatacom.predictive-score-report.v1"
)
QUALIFICATION_GATE_POWER_RESULT_SCHEMA_VERSION = (
    "histdatacom.qualification-gate-power-result.v1"
)
QUALIFICATION_POWER_STUDY_SCHEMA_VERSION = (
    "histdatacom.qualification-power-study.v1"
)
PROPOSAL_PORTFOLIO_CALIBRATION_SCHEMA_VERSION = (
    "histdatacom.proposal-portfolio-calibration.v1"
)
ENGINE_QUALIFICATION_DECISION_SCHEMA_VERSION = (
    "histdatacom.engine-qualification-decision.v1"
)
POWERED_QUALIFICATION_DOSSIER_SCHEMA_VERSION = (
    "histdatacom.powered-qualification-dossier.v1"
)

CURRENT_QUALIFICATION_PROVIDER_ID = "histdata.com"
CURRENT_QUALIFICATION_SOURCE_FORMAT = "ascii"
CURRENT_QUALIFICATION_TIMEFRAME = "T"
MAX_RESIDUAL_SAMPLES = 1_000_000
MAX_QUALIFICATION_ENGINES = 32
MAX_QUALIFICATION_FEATURES = 96
MAX_QUALIFICATION_ARTIFACT_BYTES = 64 * 1024 * 1024
MAX_POWER_REPLICATIONS = 4096
MAX_POWER_REGIONS = 16
DEFAULT_POWER_SAMPLE_SIZES = (3, 6, 12, 24, 48)
DEFAULT_SCORE_FEATURE_EXCLUSIONS = frozenset({"window_duration_seconds"})

_VERIFIED_QUALIFICATION_DOSSIERS: ContextVar[dict[str, Any] | None] = (
    ContextVar("verified_qualification_dossiers", default=None)
)
QUALIFICATION_CONTROL_METHODS = (
    "dense_identity",
    "linear_interpolation",
    "negative_anchor_drop",
)

DEFAULT_HARD_GATE_FAMILIES = {
    "time_uniformity": "wrong_intensity",
    "time_serial_dependence": "clustering",
    "mark_calibration": "mark_mix",
    "multivariate_energy": "timestamp_precision",
    "multivariate_variogram": "cross_currency_dependence",
    "calibration_sharpness": "spread_tail",
    "path_tail": "path_dependence",
    "regime_transition": "regime_transition",
    "event_response": "event_response",
    "refusal_calibration": "stale_burst_behavior",
}

DEFAULT_GATE_EFFECT_SIZES = {
    "time_uniformity": 1.30,
    "time_serial_dependence": 1.25,
    "mark_calibration": 1.30,
    "multivariate_energy": 1.20,
    "multivariate_variogram": 1.25,
    "calibration_sharpness": 1.20,
    "path_tail": 1.25,
    "regime_transition": 1.30,
    "event_response": 1.35,
    "refusal_calibration": 1.25,
}

DEFAULT_GATE_TEST_METHODS = {
    "time_uniformity": "clustered_window_ks_practical_equivalence",
    "time_serial_dependence": "clustered_window_lag_practical_equivalence",
    "mark_calibration": "clustered_window_mark_ks_practical_equivalence",
    "multivariate_energy": "paired_energy_score_difference",
    "multivariate_variogram": "paired_variogram_score_difference",
    "calibration_sharpness": "finite_rank_envelope_binomial_coverage",
    "path_tail": "paired_tail_loss_difference",
    "regime_transition": "paired_transition_loss_difference",
    "event_response": "paired_event_response_loss_difference",
    "refusal_calibration": "paired_brier_loss_difference",
}

RESIDUAL_SUPPORT_GATES = frozenset(
    {"time_uniformity", "time_serial_dependence", "mark_calibration"}
)
PRACTICAL_RESIDUAL_TOLERANCE = 0.20
CONFORMAL_TARGET_COVERAGE = 0.90
RAW_RANK_ENVELOPE_METHOD = "raw-finite-rank-envelope-v1"
VALIDATION_SPLIT_CONFORMAL_METHOD = "validation-split-conformal-envelope-v1"


class QualificationStatus(str, Enum):
    """Explicit evidence result; absence and low power never mean pass."""

    PASSED = "passed"
    FAILED = "failed"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"
    NOT_APPLICABLE = "not_applicable"
    REFUSED = "refused"


class PointProcessResidualMethod(str, Enum):
    """Supported analytic or predictive point-process residual route."""

    ANALYTIC_TIME_RESCALING = "analytic_time_rescaling"
    SIMULATION_PREDICTIVE = "simulation_predictive"


@dataclass(frozen=True, slots=True)
class PoweredQualificationPolicyV1:
    """Predeclared thresholds, power targets, and misspecification mapping."""

    alpha: float = 0.05
    minimum_power: float = 0.80
    maximum_false_positive_rate: float = 0.08
    minimum_window_count: int = 6
    minimum_residual_count: int = 64
    power_replications: int = 512
    power_sample_sizes: tuple[int, ...] = DEFAULT_POWER_SAMPLE_SIZES
    hard_gate_families: Mapping[str, str] = field(
        default_factory=lambda: dict(DEFAULT_HARD_GATE_FAMILIES)
    )
    gate_effect_sizes: Mapping[str, float] = field(
        default_factory=lambda: dict(DEFAULT_GATE_EFFECT_SIZES)
    )
    policy_id: str = ""
    schema_version: str = POWERED_QUALIFICATION_POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, POWERED_QUALIFICATION_POLICY_SCHEMA_VERSION
        )
        for name in (
            "alpha",
            "minimum_power",
            "maximum_false_positive_rate",
        ):
            value = _probability(getattr(self, name), name, open_interval=True)
            object.__setattr__(self, name, value)
        if self.minimum_power <= 0.5:
            raise ValueError("qualification minimum power must exceed 0.5")
        if self.maximum_false_positive_rate < self.alpha:
            raise ValueError("false-positive tolerance cannot be below alpha")
        object.__setattr__(
            self,
            "minimum_window_count",
            _bounded_int(
                self.minimum_window_count, "minimum_window_count", 2, 512
            ),
        )
        object.__setattr__(
            self,
            "minimum_residual_count",
            _bounded_int(
                self.minimum_residual_count,
                "minimum_residual_count",
                8,
                MAX_RESIDUAL_SAMPLES,
            ),
        )
        object.__setattr__(
            self,
            "power_replications",
            _bounded_int(
                self.power_replications,
                "power_replications",
                128,
                MAX_POWER_REPLICATIONS,
            ),
        )
        sample_sizes = tuple(
            sorted(
                {
                    _positive_int(item, "power sample size")
                    for item in self.power_sample_sizes
                }
            )
        )
        if not sample_sizes or len(sample_sizes) > MAX_POWER_REGIONS:
            raise ValueError("power sample-size grid is invalid")
        object.__setattr__(self, "power_sample_sizes", sample_sizes)
        families = dict(self.hard_gate_families)
        effects = dict(self.gate_effect_sizes)
        if set(families) != set(DEFAULT_HARD_GATE_FAMILIES):
            raise ValueError("qualification hard-gate set differs")
        if set(effects) != set(families):
            raise ValueError("qualification gate effects do not cover gates")
        normalized_families = {
            _identifier(gate, "gate_id"): _identifier(
                family, "misspecification"
            )
            for gate, family in families.items()
        }
        normalized_effects = {
            gate: _positive_float(effects[gate], f"effect size {gate}")
            for gate in normalized_families
        }
        object.__setattr__(
            self,
            "hard_gate_families",
            dict(sorted(normalized_families.items())),
        )
        object.__setattr__(
            self, "gate_effect_sizes", dict(sorted(normalized_effects.items()))
        )
        expected = _stable_id("powered-qualification-policy", self.payload())
        if self.policy_id and self.policy_id != expected:
            raise ValueError("powered qualification policy identity differs")
        object.__setattr__(self, "policy_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "alpha": self.alpha,
            "minimum_power": self.minimum_power,
            "maximum_false_positive_rate": self.maximum_false_positive_rate,
            "minimum_window_count": self.minimum_window_count,
            "minimum_residual_count": self.minimum_residual_count,
            "power_replications": self.power_replications,
            "power_sample_sizes": list(self.power_sample_sizes),
            "hard_gate_families": dict(self.hard_gate_families),
            "gate_effect_sizes": dict(self.gate_effect_sizes),
            "multiplicity_policy": "benjamini-hochberg-within-engine-final-holdout-v1",
            "portfolio_fit_split": "validation",
            "portfolio_evaluation_split": "final_holdout",
            "automatic_winner": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "policy_id": self.policy_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> PoweredQualificationPolicyV1:
        if data.get("automatic_winner") is not False:
            raise ValueError("qualification policy cannot select a winner")
        return cls(
            alpha=_finite_float(data.get("alpha"), "alpha"),
            minimum_power=_finite_float(
                data.get("minimum_power"), "minimum_power"
            ),
            maximum_false_positive_rate=_finite_float(
                data.get("maximum_false_positive_rate"),
                "maximum_false_positive_rate",
            ),
            minimum_window_count=_strict_int(
                data.get("minimum_window_count"), "minimum_window_count"
            ),
            minimum_residual_count=_strict_int(
                data.get("minimum_residual_count"), "minimum_residual_count"
            ),
            power_replications=_strict_int(
                data.get("power_replications"), "power_replications"
            ),
            power_sample_sizes=_int_tuple(data.get("power_sample_sizes")),
            hard_gate_families={
                str(key): str(value)
                for key, value in _mapping(
                    data.get("hard_gate_families")
                ).items()
            },
            gate_effect_sizes={
                str(key): _finite_float(value, str(key))
                for key, value in _mapping(
                    data.get("gate_effect_sizes")
                ).items()
            },
            policy_id=str(data.get("policy_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class PointProcessResidualInputV1:
    """Bounded process-local input to analytic or simulation residual checks."""

    engine_id: str
    config_id: str
    split_kind: str
    stratum_id: str
    method: PointProcessResidualMethod
    integrated_hazards: tuple[float, ...] = ()
    simulation_time_pits: tuple[float, ...] = ()
    mark_pits: tuple[float, ...] = ()
    fit_id: str | None = None
    residual_input_id: str = ""
    schema_version: str = POINT_PROCESS_RESIDUAL_INPUT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, POINT_PROCESS_RESIDUAL_INPUT_SCHEMA_VERSION
        )
        for name in ("engine_id", "config_id", "stratum_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        if self.split_kind not in {"validation", "final_holdout"}:
            raise ValueError("point-process residual split is not protected")
        object.__setattr__(
            self, "method", PointProcessResidualMethod(self.method)
        )
        hazards = _bounded_floats(
            self.integrated_hazards,
            "integrated_hazards",
            lower=0.0,
            upper=None,
        )
        time_pits = _bounded_floats(
            self.simulation_time_pits,
            "simulation_time_pits",
            lower=0.0,
            upper=1.0,
        )
        marks = _bounded_floats(
            self.mark_pits, "mark_pits", lower=0.0, upper=1.0
        )
        if self.method is PointProcessResidualMethod.ANALYTIC_TIME_RESCALING:
            if not hazards or time_pits:
                raise ValueError("analytic residuals require hazards only")
        elif not time_pits or hazards:
            raise ValueError("simulation residuals require PIT values only")
        object.__setattr__(self, "integrated_hazards", hazards)
        object.__setattr__(self, "simulation_time_pits", time_pits)
        object.__setattr__(self, "mark_pits", marks)
        object.__setattr__(self, "fit_id", _optional_text(self.fit_id))
        expected = _stable_id("point-process-residual-input", self.payload())
        if self.residual_input_id and self.residual_input_id != expected:
            raise ValueError("point-process residual input identity differs")
        object.__setattr__(self, "residual_input_id", expected)

    @property
    def time_pits(self) -> tuple[float, ...]:
        if self.method is PointProcessResidualMethod.ANALYTIC_TIME_RESCALING:
            return tuple(
                1.0 - math.exp(-value) for value in self.integrated_hazards
            )
        return self.simulation_time_pits

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "engine_id": self.engine_id,
            "config_id": self.config_id,
            "fit_id": self.fit_id,
            "split_kind": self.split_kind,
            "stratum_id": self.stratum_id,
            "method": self.method.value,
            "integrated_hazards": list(self.integrated_hazards),
            "simulation_time_pits": list(self.simulation_time_pits),
            "mark_pits": list(self.mark_pits),
            "process_local_not_persisted": True,
        }


@dataclass(frozen=True, slots=True)
class PointProcessResidualReportV1:
    """Bounded residual summary without event or residual rows."""

    engine_id: str
    config_id: str
    split_kind: str
    stratum_id: str
    method: PointProcessResidualMethod
    sample_count: int
    mark_sample_count: int
    time_uniform_ks: float | None
    time_uniform_p_value: float | None
    time_lag1_autocorrelation: float | None
    mark_uniform_ks: float | None
    mark_uniform_p_value: float | None
    quantiles: Mapping[str, float]
    status: QualificationStatus
    reason_codes: tuple[str, ...]
    residual_input_id: str
    report_id: str = ""
    schema_version: str = POINT_PROCESS_RESIDUAL_REPORT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, POINT_PROCESS_RESIDUAL_REPORT_SCHEMA_VERSION
        )
        for name in (
            "engine_id",
            "config_id",
            "stratum_id",
            "residual_input_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        if self.split_kind not in {"validation", "final_holdout"}:
            raise ValueError("residual report split is not protected")
        object.__setattr__(
            self, "method", PointProcessResidualMethod(self.method)
        )
        for name in ("sample_count", "mark_sample_count"):
            object.__setattr__(
                self,
                name,
                _bounded_int(
                    getattr(self, name), name, 0, MAX_RESIDUAL_SAMPLES
                ),
            )
        for name in (
            "time_uniform_ks",
            "time_uniform_p_value",
            "mark_uniform_ks",
            "mark_uniform_p_value",
        ):
            value = getattr(self, name)
            if value is not None:
                value = _probability(value, name, open_interval=False)
            object.__setattr__(self, name, value)
        lag = self.time_lag1_autocorrelation
        if lag is not None:
            lag = _finite_float(lag, "time_lag1_autocorrelation")
            if not -1.0 <= lag <= 1.0:
                raise ValueError("residual lag correlation is outside [-1,1]")
        object.__setattr__(self, "time_lag1_autocorrelation", lag)
        quantiles = _float_mapping(self.quantiles, "residual quantile", 16)
        object.__setattr__(self, "quantiles", quantiles)
        object.__setattr__(self, "status", QualificationStatus(self.status))
        reasons = _text_tuple(self.reason_codes, allow_empty=False)
        object.__setattr__(self, "reason_codes", reasons)
        expected = _stable_id("point-process-residual-report", self.payload())
        if self.report_id and self.report_id != expected:
            raise ValueError("point-process residual report identity differs")
        object.__setattr__(self, "report_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "engine_id": self.engine_id,
            "config_id": self.config_id,
            "split_kind": self.split_kind,
            "stratum_id": self.stratum_id,
            "method": self.method.value,
            "sample_count": self.sample_count,
            "mark_sample_count": self.mark_sample_count,
            "time_uniform_ks": self.time_uniform_ks,
            "time_uniform_p_value": self.time_uniform_p_value,
            "time_lag1_autocorrelation": self.time_lag1_autocorrelation,
            "mark_uniform_ks": self.mark_uniform_ks,
            "mark_uniform_p_value": self.mark_uniform_p_value,
            "quantiles": dict(self.quantiles),
            "status": self.status.value,
            "reason_codes": list(self.reason_codes),
            "residual_input_id": self.residual_input_id,
            "residual_rows_embedded": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "report_id": self.report_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> PointProcessResidualReportV1:
        if data.get("residual_rows_embedded") is not False:
            raise ValueError("residual report embeds residual rows")
        return cls(
            engine_id=str(data.get("engine_id", "")),
            config_id=str(data.get("config_id", "")),
            split_kind=str(data.get("split_kind", "")),
            stratum_id=str(data.get("stratum_id", "")),
            method=PointProcessResidualMethod(str(data.get("method", ""))),
            sample_count=_strict_int(data.get("sample_count"), "sample_count"),
            mark_sample_count=_strict_int(
                data.get("mark_sample_count"), "mark_sample_count"
            ),
            time_uniform_ks=_optional_float(data.get("time_uniform_ks")),
            time_uniform_p_value=_optional_float(
                data.get("time_uniform_p_value")
            ),
            time_lag1_autocorrelation=_optional_float(
                data.get("time_lag1_autocorrelation")
            ),
            mark_uniform_ks=_optional_float(data.get("mark_uniform_ks")),
            mark_uniform_p_value=_optional_float(
                data.get("mark_uniform_p_value")
            ),
            quantiles={
                str(key): _finite_float(value, str(key))
                for key, value in _mapping(data.get("quantiles")).items()
            },
            status=QualificationStatus(str(data.get("status", ""))),
            reason_codes=_string_tuple(data.get("reason_codes")),
            residual_input_id=str(data.get("residual_input_id", "")),
            report_id=str(data.get("report_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def evaluate_point_process_residuals(
    inputs: PointProcessResidualInputV1,
    policy: PoweredQualificationPolicyV1 | None = None,
) -> PointProcessResidualReportV1:
    """Evaluate analytic compensators or simulation PITs deterministically."""
    selected_policy = policy or PoweredQualificationPolicyV1()
    pits = inputs.time_pits
    marks = inputs.mark_pits
    time_ks = _uniform_ks(pits) if pits else None
    time_p = (
        _ks_uniform_p_value(time_ks, len(pits)) if time_ks is not None else None
    )
    lag = _lag_one_correlation(pits) if len(pits) >= 3 else None
    mark_ks = _uniform_ks(marks) if marks else None
    mark_p = (
        _ks_uniform_p_value(mark_ks, len(marks))
        if mark_ks is not None
        else None
    )
    reasons: list[str] = []
    if len(pits) < selected_policy.minimum_residual_count:
        status = QualificationStatus.INSUFFICIENT_EVIDENCE
        reasons.append("residual_support_below_policy_minimum")
    else:
        failed = bool(
            time_p is None
            or time_p < selected_policy.alpha
            or lag is None
            or abs(lag) > 0.20
            or (marks and (mark_p is None or mark_p < selected_policy.alpha))
        )
        status = (
            QualificationStatus.FAILED if failed else QualificationStatus.PASSED
        )
        reasons.append(
            "residual_diagnostics_reject_model"
            if failed
            else "residual_diagnostics_do_not_reject_model"
        )
    quantiles = {
        f"time_q{int(level * 100):02d}": _quantile(pits, level)
        for level in (0.01, 0.10, 0.25, 0.50, 0.75, 0.90, 0.99)
    }
    return PointProcessResidualReportV1(
        engine_id=inputs.engine_id,
        config_id=inputs.config_id,
        split_kind=inputs.split_kind,
        stratum_id=inputs.stratum_id,
        method=inputs.method,
        sample_count=len(pits),
        mark_sample_count=len(marks),
        time_uniform_ks=time_ks,
        time_uniform_p_value=time_p,
        time_lag1_autocorrelation=lag,
        mark_uniform_ks=mark_ks,
        mark_uniform_p_value=mark_p,
        quantiles=quantiles,
        status=status,
        reason_codes=tuple(reasons),
        residual_input_id=inputs.residual_input_id,
    )


@dataclass(frozen=True, slots=True)
class PredictiveScoreReportV1:
    """Proper-score, calibration, sharpness, and process-error summary."""

    engine_id: str
    split_kind: str
    feature_names: tuple[str, ...]
    window_count: int
    member_observation_count: int
    energy_score: float | None
    variogram_score_p05: float | None
    variogram_score_p1: float | None
    marginal_crps: float | None
    nominal_coverage: float
    empirical_coverage: float | None
    calibration_error: float | None
    sharpness: float | None
    tail_error: float | None
    path_error: float | None
    cross_series_error: float | None
    status: QualificationStatus
    reason_codes: tuple[str, ...]
    trace_id: str
    regime_transition_error: float | None = None
    event_response_error: float | None = None
    stratum_support_counts: Mapping[str, int] = field(default_factory=dict)
    interval_calibration_method: str = RAW_RANK_ENVELOPE_METHOD
    interval_calibration_window_count: int = 0
    mean_interval_adjustment: float | None = None
    report_id: str = ""
    schema_version: str = PREDICTIVE_SCORE_REPORT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, PREDICTIVE_SCORE_REPORT_SCHEMA_VERSION
        )
        object.__setattr__(self, "engine_id", _required_text(self.engine_id))
        object.__setattr__(self, "trace_id", _required_text(self.trace_id))
        object.__setattr__(
            self,
            "interval_calibration_method",
            _identifier(
                self.interval_calibration_method,
                "interval_calibration_method",
            ),
        )
        if self.split_kind not in {"validation", "final_holdout"}:
            raise ValueError("predictive score split is not protected")
        features = _text_tuple(self.feature_names, allow_empty=False)
        if len(features) > MAX_QUALIFICATION_FEATURES:
            raise ValueError("predictive score feature count exceeds bound")
        object.__setattr__(self, "feature_names", features)
        for name in ("window_count", "member_observation_count"):
            object.__setattr__(
                self,
                name,
                _bounded_int(getattr(self, name), name, 0, 1_000_000),
            )
        object.__setattr__(
            self,
            "interval_calibration_window_count",
            _bounded_int(
                self.interval_calibration_window_count,
                "interval_calibration_window_count",
                0,
                1_000_000,
            ),
        )
        for name in (
            "energy_score",
            "variogram_score_p05",
            "variogram_score_p1",
            "marginal_crps",
            "calibration_error",
            "sharpness",
            "tail_error",
            "path_error",
            "cross_series_error",
            "regime_transition_error",
            "event_response_error",
            "mean_interval_adjustment",
        ):
            value = getattr(self, name)
            if value is not None:
                value = _nonnegative_float(value, name)
            object.__setattr__(self, name, value)
        object.__setattr__(
            self,
            "nominal_coverage",
            _probability(
                self.nominal_coverage, "nominal_coverage", open_interval=False
            ),
        )
        if self.empirical_coverage is not None:
            object.__setattr__(
                self,
                "empirical_coverage",
                _probability(
                    self.empirical_coverage,
                    "empirical_coverage",
                    open_interval=False,
                ),
            )
        object.__setattr__(self, "status", QualificationStatus(self.status))
        object.__setattr__(
            self,
            "reason_codes",
            _text_tuple(self.reason_codes, allow_empty=False),
        )
        support = {
            _identifier(key, "stratum support"): _bounded_int(
                value, f"stratum support {key}", 0, 1_000_000
            )
            for key, value in self.stratum_support_counts.items()
        }
        object.__setattr__(
            self, "stratum_support_counts", dict(sorted(support.items()))
        )
        expected = _stable_id("predictive-score-report", self.payload())
        if self.report_id and self.report_id != expected:
            raise ValueError("predictive score report identity differs")
        object.__setattr__(self, "report_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        payload: dict[str, JSONValue] = {
            "schema_version": self.schema_version,
            "engine_id": self.engine_id,
            "split_kind": self.split_kind,
            "feature_names": list(self.feature_names),
            "window_count": self.window_count,
            "member_observation_count": self.member_observation_count,
            "energy_score": self.energy_score,
            "variogram_score_p05": self.variogram_score_p05,
            "variogram_score_p1": self.variogram_score_p1,
            "marginal_crps": self.marginal_crps,
            "nominal_coverage": self.nominal_coverage,
            "empirical_coverage": self.empirical_coverage,
            "calibration_error": self.calibration_error,
            "sharpness": self.sharpness,
            "tail_error": self.tail_error,
            "path_error": self.path_error,
            "cross_series_error": self.cross_series_error,
            "status": self.status.value,
            "reason_codes": list(self.reason_codes),
            "trace_id": self.trace_id,
            "event_rows_embedded": False,
        }
        if self.regime_transition_error is not None:
            payload["regime_transition_error"] = self.regime_transition_error
        if self.event_response_error is not None:
            payload["event_response_error"] = self.event_response_error
        if self.stratum_support_counts:
            payload["stratum_support_counts"] = dict(
                self.stratum_support_counts
            )
        if (
            self.interval_calibration_method != RAW_RANK_ENVELOPE_METHOD
            or self.interval_calibration_window_count
            or self.mean_interval_adjustment is not None
        ):
            payload["interval_calibration_method"] = (
                self.interval_calibration_method
            )
            payload["interval_calibration_window_count"] = (
                self.interval_calibration_window_count
            )
            payload["mean_interval_adjustment"] = self.mean_interval_adjustment
        return payload

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "report_id": self.report_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> PredictiveScoreReportV1:
        if data.get("event_rows_embedded") is not False:
            raise ValueError("predictive score report embeds event rows")
        return cls(
            engine_id=str(data.get("engine_id", "")),
            split_kind=str(data.get("split_kind", "")),
            feature_names=_string_tuple(data.get("feature_names")),
            window_count=_strict_int(data.get("window_count"), "window_count"),
            member_observation_count=_strict_int(
                data.get("member_observation_count"), "member_observation_count"
            ),
            energy_score=_optional_float(data.get("energy_score")),
            variogram_score_p05=_optional_float(
                data.get("variogram_score_p05")
            ),
            variogram_score_p1=_optional_float(data.get("variogram_score_p1")),
            marginal_crps=_optional_float(data.get("marginal_crps")),
            nominal_coverage=_finite_float(
                data.get("nominal_coverage"), "nominal_coverage"
            ),
            empirical_coverage=_optional_float(data.get("empirical_coverage")),
            calibration_error=_optional_float(data.get("calibration_error")),
            sharpness=_optional_float(data.get("sharpness")),
            tail_error=_optional_float(data.get("tail_error")),
            path_error=_optional_float(data.get("path_error")),
            cross_series_error=_optional_float(data.get("cross_series_error")),
            status=QualificationStatus(str(data.get("status", ""))),
            reason_codes=_string_tuple(data.get("reason_codes")),
            trace_id=str(data.get("trace_id", "")),
            regime_transition_error=_optional_float(
                data.get("regime_transition_error")
            ),
            event_response_error=_optional_float(
                data.get("event_response_error")
            ),
            stratum_support_counts={
                str(key): _strict_int(value, str(key))
                for key, value in _mapping(
                    data.get("stratum_support_counts", {})
                ).items()
            },
            interval_calibration_method=str(
                data.get(
                    "interval_calibration_method",
                    RAW_RANK_ENVELOPE_METHOD,
                )
            ),
            interval_calibration_window_count=_strict_int(
                data.get("interval_calibration_window_count", 0),
                "interval_calibration_window_count",
            ),
            mean_interval_adjustment=_optional_float(
                data.get("mean_interval_adjustment")
            ),
            report_id=str(data.get("report_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class QualificationGatePowerResultV1:
    """Finite-sample false-positive and power region for one hard gate."""

    gate_id: str
    misspecification_family: str
    test_method: str
    effect_size: float
    alternative_parameters: Mapping[str, float]
    observed_support_count: int
    replications: int
    false_positive_by_sample_size: Mapping[str, float]
    power_by_sample_size: Mapping[str, float]
    observed_false_positive_rate: float
    observed_power: float
    status: QualificationStatus
    seed: int
    result_id: str = ""
    schema_version: str = QUALIFICATION_GATE_POWER_RESULT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, QUALIFICATION_GATE_POWER_RESULT_SCHEMA_VERSION
        )
        object.__setattr__(
            self, "gate_id", _identifier(self.gate_id, "gate_id")
        )
        object.__setattr__(
            self,
            "misspecification_family",
            _identifier(
                self.misspecification_family, "misspecification_family"
            ),
        )
        object.__setattr__(
            self, "test_method", _identifier(self.test_method, "test_method")
        )
        object.__setattr__(
            self,
            "effect_size",
            _positive_float(self.effect_size, "effect_size"),
        )
        parameters = _float_mapping(
            self.alternative_parameters, "alternative parameter", 16
        )
        if not parameters:
            raise ValueError("power result requires alternative parameters")
        object.__setattr__(self, "alternative_parameters", parameters)
        object.__setattr__(
            self,
            "observed_support_count",
            _bounded_int(
                self.observed_support_count,
                "observed_support_count",
                0,
                1_000_000,
            ),
        )
        object.__setattr__(
            self,
            "replications",
            _bounded_int(
                self.replications, "replications", 128, MAX_POWER_REPLICATIONS
            ),
        )
        false_positive = _probability_mapping(
            self.false_positive_by_sample_size, "false_positive_by_sample_size"
        )
        power = _probability_mapping(
            self.power_by_sample_size, "power_by_sample_size"
        )
        if set(false_positive) != set(power):
            raise ValueError("power/FPR reliability regions differ")
        object.__setattr__(
            self, "false_positive_by_sample_size", false_positive
        )
        object.__setattr__(self, "power_by_sample_size", power)
        object.__setattr__(
            self,
            "observed_false_positive_rate",
            _probability(
                self.observed_false_positive_rate,
                "observed_false_positive_rate",
                open_interval=False,
            ),
        )
        object.__setattr__(
            self,
            "observed_power",
            _probability(
                self.observed_power, "observed_power", open_interval=False
            ),
        )
        object.__setattr__(self, "status", QualificationStatus(self.status))
        if self.status not in {
            QualificationStatus.PASSED,
            QualificationStatus.INSUFFICIENT_EVIDENCE,
            QualificationStatus.FAILED,
        }:
            raise ValueError("power result has an invalid status")
        if not 0 <= self.seed < 2**64:
            raise ValueError("power-study seed is outside uint64")
        expected = _stable_id("qualification-gate-power", self.payload())
        if self.result_id and self.result_id != expected:
            raise ValueError("qualification gate power identity differs")
        object.__setattr__(self, "result_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "gate_id": self.gate_id,
            "misspecification_family": self.misspecification_family,
            "test_method": self.test_method,
            "effect_size": self.effect_size,
            "alternative_parameters": dict(self.alternative_parameters),
            "observed_support_count": self.observed_support_count,
            "replications": self.replications,
            "false_positive_by_sample_size": dict(
                self.false_positive_by_sample_size
            ),
            "power_by_sample_size": dict(self.power_by_sample_size),
            "observed_false_positive_rate": self.observed_false_positive_rate,
            "observed_power": self.observed_power,
            "status": self.status.value,
            "seed": self.seed,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "result_id": self.result_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> QualificationGatePowerResultV1:
        return cls(
            gate_id=str(data.get("gate_id", "")),
            misspecification_family=str(
                data.get("misspecification_family", "")
            ),
            test_method=str(data.get("test_method", "")),
            effect_size=_finite_float(data.get("effect_size"), "effect_size"),
            alternative_parameters={
                str(key): _finite_float(value, str(key))
                for key, value in _mapping(
                    data.get("alternative_parameters")
                ).items()
            },
            observed_support_count=_strict_int(
                data.get("observed_support_count"), "observed_support_count"
            ),
            replications=_strict_int(data.get("replications"), "replications"),
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
            observed_false_positive_rate=_finite_float(
                data.get("observed_false_positive_rate"),
                "observed_false_positive_rate",
            ),
            observed_power=_finite_float(
                data.get("observed_power"), "observed_power"
            ),
            status=QualificationStatus(str(data.get("status", ""))),
            seed=_strict_int(data.get("seed"), "seed"),
            result_id=str(data.get("result_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class QualificationPowerStudyV1:
    """Complete power evidence covering every predeclared hard gate."""

    policy_id: str
    trace_id: str
    results: tuple[QualificationGatePowerResultV1, ...]
    study_id: str = ""
    schema_version: str = QUALIFICATION_POWER_STUDY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, QUALIFICATION_POWER_STUDY_SCHEMA_VERSION
        )
        object.__setattr__(self, "policy_id", _required_text(self.policy_id))
        object.__setattr__(self, "trace_id", _required_text(self.trace_id))
        results = tuple(sorted(self.results, key=lambda item: item.gate_id))
        if not results or len({item.gate_id for item in results}) != len(
            results
        ):
            raise ValueError(
                "qualification power results are empty or duplicated"
            )
        object.__setattr__(self, "results", results)
        expected = _stable_id("qualification-power-study", self.payload())
        if self.study_id and self.study_id != expected:
            raise ValueError("qualification power study identity differs")
        object.__setattr__(self, "study_id", expected)

    @property
    def reliable(self) -> bool:
        return all(
            item.status is QualificationStatus.PASSED for item in self.results
        )

    def result(self, gate_id: str) -> QualificationGatePowerResultV1:
        for item in self.results:
            if item.gate_id == gate_id:
                return item
        raise ValueError(f"unknown qualification power gate: {gate_id}")

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "policy_id": self.policy_id,
            "trace_id": self.trace_id,
            "results": [item.to_dict() for item in self.results],
            "reliable": self.reliable,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "study_id": self.study_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> QualificationPowerStudyV1:
        study = cls(
            policy_id=str(data.get("policy_id", "")),
            trace_id=str(data.get("trace_id", "")),
            results=tuple(
                QualificationGatePowerResultV1.from_dict(_mapping(item))
                for item in _sequence(data.get("results"))
            ),
            study_id=str(data.get("study_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        if data.get("reliable") is not study.reliable:
            raise ValueError(
                "qualification power reliability derivation differs"
            )
        return study


def run_qualification_power_study(
    policy: PoweredQualificationPolicyV1,
    *,
    trace_id: str,
    observed_support_count: int | None = None,
    observed_support_by_gate: Mapping[str, int] | None = None,
) -> QualificationPowerStudyV1:
    """Exercise each real gate statistic against its named misspecification.

    The study deliberately uses the same KS, serial-correlation, proper-score,
    coverage, tail, transition, event-response, and refusal-loss functionals
    consumed by qualification.  It is therefore a finite-sample gate study,
    not a generic Gaussian mean-test proxy.
    """
    if (observed_support_count is None) == (observed_support_by_gate is None):
        raise ValueError(
            "power study requires exactly one observed support specification"
        )
    if observed_support_by_gate is None:
        shared_support = _bounded_int(
            observed_support_count,
            "observed_support_count",
            0,
            1_000_000,
        )
        support_by_gate = dict.fromkeys(
            policy.hard_gate_families, shared_support
        )
    else:
        if set(observed_support_by_gate) != set(policy.hard_gate_families):
            raise ValueError("power-study support does not cover every gate")
        support_by_gate = {
            gate_id: _bounded_int(
                observed_support_by_gate[gate_id],
                f"observed support {gate_id}",
                0,
                1_000_000,
            )
            for gate_id in policy.hard_gate_families
        }
    results: list[QualificationGatePowerResultV1] = []
    for gate_id, family in policy.hard_gate_families.items():
        support = support_by_gate[gate_id]
        sizes = tuple(
            sorted(set(policy.power_sample_sizes) | {max(1, support)})
        )
        seed = _semantic_seed(
            {
                "policy_id": policy.policy_id,
                "trace_id": _required_text(trace_id),
                "gate_id": gate_id,
                "family": family,
            }
        )
        rng = random.Random(seed)
        false_positive: dict[str, float] = {}
        power: dict[str, float] = {}
        effect = policy.gate_effect_sizes[gate_id]
        alternative_parameters = _gate_alternative_parameters(gate_id, effect)
        for sample_size in sizes:
            null_rejections = 0
            alternative_rejections = 0
            for _ in range(policy.power_replications):
                null_rejected, alternative_rejected = _simulate_gate_rejections(
                    gate_id,
                    sample_size=sample_size,
                    effect_size=effect,
                    alpha=policy.alpha,
                    rng=rng,
                )
                null_rejections += int(null_rejected)
                alternative_rejections += int(alternative_rejected)
            false_positive[str(sample_size)] = (
                null_rejections / policy.power_replications
            )
            power[str(sample_size)] = (
                alternative_rejections / policy.power_replications
            )
        observed_key = str(max(1, support))
        observed_fpr = false_positive[observed_key]
        observed_power = power[observed_key]
        minimum_support = policy.minimum_window_count
        if support < minimum_support:
            status = QualificationStatus.INSUFFICIENT_EVIDENCE
        elif observed_fpr > policy.maximum_false_positive_rate:
            status = QualificationStatus.FAILED
        elif observed_power < policy.minimum_power:
            status = QualificationStatus.INSUFFICIENT_EVIDENCE
        else:
            status = QualificationStatus.PASSED
        results.append(
            QualificationGatePowerResultV1(
                gate_id=gate_id,
                misspecification_family=family,
                test_method=DEFAULT_GATE_TEST_METHODS[gate_id],
                effect_size=effect,
                alternative_parameters=alternative_parameters,
                observed_support_count=support,
                replications=policy.power_replications,
                false_positive_by_sample_size=false_positive,
                power_by_sample_size=power,
                observed_false_positive_rate=observed_fpr,
                observed_power=observed_power,
                status=status,
                seed=seed,
            )
        )
    return QualificationPowerStudyV1(
        policy_id=policy.policy_id,
        trace_id=trace_id,
        results=tuple(results),
    )


def _gate_alternative_parameters(
    gate_id: str, effect_size: float
) -> dict[str, float]:
    effect = _positive_float(effect_size, "effect_size")
    parameters = {
        "time_uniformity": {
            "practical_tolerance": PRACTICAL_RESIDUAL_TOLERANCE,
            "alternative_window_ks_mean": (
                PRACTICAL_RESIDUAL_TOLERANCE + 0.06 * effect
            ),
        },
        "time_serial_dependence": {
            "practical_tolerance": PRACTICAL_RESIDUAL_TOLERANCE,
            "alternative_window_lag_mean": (
                PRACTICAL_RESIDUAL_TOLERANCE + 0.05 * effect
            ),
        },
        "mark_calibration": {
            "practical_tolerance": PRACTICAL_RESIDUAL_TOLERANCE,
            "alternative_window_ks_mean": (
                PRACTICAL_RESIDUAL_TOLERANCE + 0.05 * effect
            ),
        },
        "multivariate_energy": {"location_shift_sd": 0.90 * effect},
        "multivariate_variogram": {
            "forecast_common_factor": max(0.0, 0.95 - 0.80 * effect)
        },
        "calibration_sharpness": {
            "coverage_probability": max(0.10, 0.90 - 0.25 * effect)
        },
        "path_tail": {"tail_scale_multiplier": 1.0 + 1.20 * effect},
        "regime_transition": {
            "transition_probability_shift": min(0.45, 0.30 * effect)
        },
        "event_response": {
            "response_attenuation": max(0.0, 1.0 - 0.80 * effect)
        },
        "refusal_calibration": {
            "refusal_probability_shift": min(0.65, 0.30 * effect)
        },
    }
    try:
        return parameters[gate_id]
    except KeyError as err:  # pragma: no cover - policy validates coverage
        raise ValueError(f"unsupported power-study gate {gate_id}") from err


def _simulate_gate_rejections(
    gate_id: str,
    *,
    sample_size: int,
    effect_size: float,
    alpha: float,
    rng: random.Random,
) -> tuple[bool, bool]:
    """Return paired null/alternative decisions for one registered gate."""
    count = _positive_int(sample_size, "sample_size")
    parameters = _gate_alternative_parameters(gate_id, effect_size)
    if gate_id in RESIDUAL_SUPPORT_GATES:
        if gate_id == "time_serial_dependence":
            null_mean, null_sd = 0.10, 0.07
            alternative_mean = parameters["alternative_window_lag_mean"]
            alternative_sd = 0.06
        else:
            null_mean, null_sd = 0.05, 0.03
            alternative_mean = parameters["alternative_window_ks_mean"]
            alternative_sd = 0.05
        null = tuple(
            min(1.0, max(0.0, rng.gauss(null_mean, null_sd)))
            for _ in range(count)
        )
        alternative = tuple(
            min(1.0, max(0.0, rng.gauss(alternative_mean, alternative_sd)))
            for _ in range(count)
        )
        return (
            _lower_mean_confidence_bound(null, alpha)
            > PRACTICAL_RESIDUAL_TOLERANCE,
            _lower_mean_confidence_bound(alternative, alpha)
            > PRACTICAL_RESIDUAL_TOLERANCE,
        )
    if gate_id == "multivariate_energy":
        shift = parameters["location_shift_sd"]
        null_losses: list[float] = []
        alternative_losses: list[float] = []
        for _ in range(count):
            truth = tuple(rng.gauss(0.0, 1.0) for _ in range(4))
            oracle = tuple(
                tuple(rng.gauss(0.0, 1.0) for _ in range(4)) for _ in range(4)
            )
            null_samples = tuple(
                tuple(rng.gauss(0.0, 1.0) for _ in range(4)) for _ in range(4)
            )
            alternative_samples = tuple(
                tuple(rng.gauss(shift, 1.0) for _ in range(4)) for _ in range(4)
            )
            baseline = _energy_score(truth, oracle)
            null_losses.append(_energy_score(truth, null_samples) - baseline)
            alternative_losses.append(
                _energy_score(truth, alternative_samples) - baseline
            )
        return (
            _paired_loss_rejects(null_losses, alpha),
            _paired_loss_rejects(alternative_losses, alpha),
        )
    if gate_id == "multivariate_variogram":
        alternative_common = parameters["forecast_common_factor"]
        null_losses = []
        alternative_losses = []
        for _ in range(count):
            common = rng.gauss(0.0, 1.0)
            truth = tuple(
                0.95 * common + math.sqrt(1.0 - 0.95**2) * rng.gauss(0.0, 1.0)
                for _ in range(4)
            )

            def samples(common_weight: float) -> tuple[tuple[float, ...], ...]:
                residual_weight = math.sqrt(max(0.0, 1.0 - common_weight**2))
                result = []
                for _member in range(4):
                    factor = rng.gauss(0.0, 1.0)
                    result.append(
                        tuple(
                            common_weight * factor
                            + residual_weight * rng.gauss(0.0, 1.0)
                            for _dimension in range(4)
                        )
                    )
                return tuple(result)

            oracle = samples(0.95)
            baseline = _variogram_score(truth, oracle, 0.5)
            null_losses.append(
                _variogram_score(truth, samples(0.95), 0.5) - baseline
            )
            alternative_losses.append(
                _variogram_score(truth, samples(alternative_common), 0.5)
                - baseline
            )
        return (
            _paired_loss_rejects(null_losses, alpha),
            _paired_loss_rejects(alternative_losses, alpha),
        )
    if gate_id == "calibration_sharpness":
        nominal = 0.90
        alternative_probability = parameters["coverage_probability"]
        null_coverage = _mean(
            [float(rng.random() < nominal) for _ in range(count)]
        )
        alternative_coverage = _mean(
            [
                float(rng.random() < alternative_probability)
                for _ in range(count)
            ]
        )
        tolerance = _binomial_coverage_tolerance(nominal, count, alpha)
        return (
            abs(null_coverage - nominal) > tolerance,
            abs(alternative_coverage - nominal) > tolerance,
        )
    if gate_id == "path_tail":
        scale = parameters["tail_scale_multiplier"]
        path_null_losses: list[float] = []
        path_alternative_losses: list[float] = []
        for _ in range(count):
            path_truth = abs(rng.gauss(0.0, 1.0))
            path_oracle_loss = abs(abs(rng.gauss(0.0, 1.0)) - path_truth)
            path_null_losses.append(
                abs(abs(rng.gauss(0.0, 1.0)) - path_truth) - path_oracle_loss
            )
            path_alternative_losses.append(
                abs(scale * abs(rng.gauss(0.0, 1.0)) - path_truth)
                - path_oracle_loss
            )
        return (
            _paired_loss_rejects(path_null_losses, alpha),
            _paired_loss_rejects(path_alternative_losses, alpha),
        )
    if gate_id == "regime_transition":
        shift = parameters["transition_probability_shift"]
        regime_truth = [float(rng.random() < 0.50) for _ in range(count)]
        regime_baseline = [
            min(0.95, max(0.05, 0.50 + rng.gauss(0.0, 0.04)))
            for _ in range(count)
        ]
        regime_null = [
            min(0.95, max(0.05, 0.50 + rng.gauss(0.0, 0.04)))
            for _ in range(count)
        ]
        regime_alternative = [
            min(0.95, max(0.05, 0.50 + shift + rng.gauss(0.0, 0.04)))
            for _ in range(count)
        ]
        return (
            _paired_loss_rejects(
                [
                    (left - observed) ** 2 - (base - observed) ** 2
                    for left, base, observed in zip(
                        regime_null, regime_baseline, regime_truth
                    )
                ],
                alpha,
            ),
            _paired_loss_rejects(
                [
                    (left - observed) ** 2 - (base - observed) ** 2
                    for left, base, observed in zip(
                        regime_alternative, regime_baseline, regime_truth
                    )
                ],
                alpha,
            ),
        )
    if gate_id == "event_response":
        attenuation = parameters["response_attenuation"]
        event_reference = [rng.gauss(1.0, 0.5) for _ in range(count)]
        event_oracle = [rng.gauss(1.0, 0.5) for _ in range(count)]
        event_null_losses = [
            abs(rng.gauss(1.0, 0.5) - truth) - abs(base - truth)
            for truth, base in zip(event_reference, event_oracle)
        ]
        event_alternative_losses = [
            abs(rng.gauss(attenuation, 0.5) - truth) - abs(base - truth)
            for truth, base in zip(event_reference, event_oracle)
        ]
        return (
            _paired_loss_rejects(event_null_losses, alpha),
            _paired_loss_rejects(event_alternative_losses, alpha),
        )
    if gate_id == "refusal_calibration":
        shift = parameters["refusal_probability_shift"]
        predicted = 0.10
        refusal_oracle = [
            (float(rng.random() < predicted) - predicted) ** 2
            for _ in range(count)
        ]
        refusal_null_losses = [
            (float(rng.random() < predicted) - predicted) ** 2 - baseline_loss
            for baseline_loss in refusal_oracle
        ]
        refusal_alternative_losses = [
            (float(rng.random() < min(1.0, predicted + shift)) - predicted) ** 2
            - baseline_loss
            for baseline_loss in refusal_oracle
        ]
        return (
            _paired_loss_rejects(refusal_null_losses, alpha),
            _paired_loss_rejects(refusal_alternative_losses, alpha),
        )
    raise ValueError(f"unsupported power-study gate {gate_id}")


def _paired_loss_rejects(
    values: Sequence[float], alpha: float, *, center: float = 0.0
) -> bool:
    """One-sided paired test without a small-sample normal approximation."""
    if len(values) < 2:
        return False
    differences = tuple(
        _finite_float(value, "paired loss") - center for value in values
    )
    selected_alpha = _probability(alpha, "paired alpha", open_interval=True)
    if len(differences) <= 8:
        observed = sum(differences)
        magnitudes = tuple(abs(value) for value in differences)
        at_least_observed = 0
        for assignment in range(1 << len(magnitudes)):
            signed_sum = sum(
                value if assignment & (1 << index) else -value
                for index, value in enumerate(magnitudes)
            )
            if signed_sum >= observed - 1e-15:
                at_least_observed += 1
        exact_p_value = at_least_observed / (1 << len(magnitudes))
        return observed > 0.0 and exact_p_value <= selected_alpha
    mean = _mean(differences)
    variance = sum((value - mean) ** 2 for value in differences) / (
        len(differences) - 1
    )
    if variance <= 0.0:
        return mean > 0.0 and len(differences) >= 5
    statistic = mean / math.sqrt(variance / len(differences))
    critical = _student_t_one_sided_critical(
        selected_alpha, len(differences) - 1
    )
    return statistic > critical


def _student_t_one_sided_critical(alpha: float, degrees_freedom: int) -> float:
    """Accurate bounded Cornish-Fisher t quantile without a SciPy dependency."""
    selected_alpha = _probability(alpha, "t alpha", open_interval=True)
    degrees = _positive_int(degrees_freedom, "degrees_freedom")
    z_value = statistics.NormalDist().inv_cdf(1.0 - selected_alpha)
    inverse = 1.0 / degrees
    first = (z_value**3 + z_value) * inverse / 4.0
    second = (
        (5.0 * z_value**5 + 16.0 * z_value**3 + 3.0 * z_value)
        * inverse**2
        / 96.0
    )
    third = (
        (
            3.0 * z_value**7
            + 19.0 * z_value**5
            + 17.0 * z_value**3
            - 15.0 * z_value
        )
        * inverse**3
        / 384.0
    )
    return z_value + first + second + third


def _binomial_coverage_tolerance(
    nominal: float, sample_count: int, alpha: float
) -> float:
    """Two-sided finite-sample tolerance for a declared coverage target."""
    target = _probability(nominal, "nominal coverage", open_interval=False)
    count = _positive_int(sample_count, "coverage sample count")
    selected_alpha = _probability(alpha, "coverage alpha", open_interval=True)
    critical = statistics.NormalDist().inv_cdf(1.0 - selected_alpha / 2.0)
    standard_error = math.sqrt(target * (1.0 - target) / count)
    return min(1.0, critical * standard_error + 1.0 / count)


@dataclass(frozen=True, slots=True)
class ProposalPortfolioCalibrationV1:
    """Frozen cross-engine predictive weights and untouched holdout scores."""

    evaluation_id: str
    trace_id: str
    engine_ids: tuple[str, ...]
    weights: Mapping[str, float]
    fit_window_ids: tuple[str, ...]
    final_holdout_window_ids: tuple[str, ...]
    validation_energy_score: float | None
    final_holdout_energy_score: float | None
    final_holdout_variogram_score_p05: float | None
    status: QualificationStatus
    reason_codes: tuple[str, ...]
    calibration_id: str = ""
    schema_version: str = PROPOSAL_PORTFOLIO_CALIBRATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, PROPOSAL_PORTFOLIO_CALIBRATION_SCHEMA_VERSION
        )
        object.__setattr__(
            self, "evaluation_id", _required_text(self.evaluation_id)
        )
        object.__setattr__(self, "trace_id", _required_text(self.trace_id))
        selected_status = QualificationStatus(self.status)
        engines = _text_tuple(self.engine_ids, allow_empty=True)
        if len(engines) > MAX_QUALIFICATION_ENGINES:
            raise ValueError("portfolio calibration engine count exceeds bound")
        weights = {
            _required_text(key): _nonnegative_float(value, f"weight {key}")
            for key, value in self.weights.items()
        }
        if set(weights) != set(engines):
            raise ValueError(
                "portfolio calibration weights differ from engines"
            )
        if engines and not math.isclose(
            sum(weights.values()), 1.0, abs_tol=1e-9
        ):
            raise ValueError("portfolio calibration weights do not sum to one")
        if not engines and (
            weights or selected_status is QualificationStatus.PASSED
        ):
            raise ValueError(
                "empty portfolio calibration cannot carry weights or pass"
            )
        object.__setattr__(self, "engine_ids", engines)
        object.__setattr__(self, "weights", dict(sorted(weights.items())))
        fit_windows = _text_tuple(self.fit_window_ids, allow_empty=False)
        holdout_windows = _text_tuple(
            self.final_holdout_window_ids, allow_empty=False
        )
        if set(fit_windows) & set(holdout_windows):
            raise ValueError("portfolio fit and final holdout windows overlap")
        object.__setattr__(self, "fit_window_ids", fit_windows)
        object.__setattr__(self, "final_holdout_window_ids", holdout_windows)
        for name in (
            "validation_energy_score",
            "final_holdout_energy_score",
            "final_holdout_variogram_score_p05",
        ):
            value = getattr(self, name)
            if value is not None:
                value = _nonnegative_float(value, name)
            object.__setattr__(self, name, value)
        object.__setattr__(self, "status", selected_status)
        object.__setattr__(
            self,
            "reason_codes",
            _text_tuple(self.reason_codes, allow_empty=False),
        )
        expected = _stable_id("proposal-portfolio-calibration", self.payload())
        if self.calibration_id and self.calibration_id != expected:
            raise ValueError("proposal portfolio calibration identity differs")
        object.__setattr__(self, "calibration_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "evaluation_id": self.evaluation_id,
            "trace_id": self.trace_id,
            "engine_ids": list(self.engine_ids),
            "weights": dict(self.weights),
            "fit_split": "validation",
            "fit_window_ids": list(self.fit_window_ids),
            "evaluation_split": "final_holdout",
            "final_holdout_window_ids": list(self.final_holdout_window_ids),
            "weights_frozen_before_final_holdout": True,
            "final_holdout_evaluated_once": True,
            "validation_energy_score": self.validation_energy_score,
            "final_holdout_energy_score": self.final_holdout_energy_score,
            "final_holdout_variogram_score_p05": (
                self.final_holdout_variogram_score_p05
            ),
            "status": self.status.value,
            "reason_codes": list(self.reason_codes),
            "automatic_winner": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "calibration_id": self.calibration_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ProposalPortfolioCalibrationV1:
        for name, expected in (
            ("fit_split", "validation"),
            ("evaluation_split", "final_holdout"),
            ("weights_frozen_before_final_holdout", True),
            ("final_holdout_evaluated_once", True),
            ("automatic_winner", False),
        ):
            if data.get(name) != expected:
                raise ValueError(f"portfolio calibration {name} differs")
        return cls(
            evaluation_id=str(data.get("evaluation_id", "")),
            trace_id=str(data.get("trace_id", "")),
            engine_ids=_string_tuple(data.get("engine_ids")),
            weights={
                str(key): _finite_float(value, str(key))
                for key, value in _mapping(data.get("weights")).items()
            },
            fit_window_ids=_string_tuple(data.get("fit_window_ids")),
            final_holdout_window_ids=_string_tuple(
                data.get("final_holdout_window_ids")
            ),
            validation_energy_score=_optional_float(
                data.get("validation_energy_score")
            ),
            final_holdout_energy_score=_optional_float(
                data.get("final_holdout_energy_score")
            ),
            final_holdout_variogram_score_p05=_optional_float(
                data.get("final_holdout_variogram_score_p05")
            ),
            status=QualificationStatus(str(data.get("status", ""))),
            reason_codes=_string_tuple(data.get("reason_codes")),
            calibration_id=str(data.get("calibration_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EngineQualificationDecisionV1:
    """Separate benchmark, reconstruction, and ensemble eligibility."""

    engine_id: str
    evidence_ids: tuple[str, ...]
    residual_report_ids: tuple[str, ...]
    score_report_ids: tuple[str, ...]
    power_study_id: str
    portfolio_calibration_id: str
    gate_statuses: Mapping[str, QualificationStatus]
    adjusted_p_values: Mapping[str, float]
    benchmark_eligible: bool
    reconstruction_eligible: bool
    ensemble_eligible: bool
    status: QualificationStatus
    reason_codes: tuple[str, ...]
    decision_id: str = ""
    schema_version: str = ENGINE_QUALIFICATION_DECISION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, ENGINE_QUALIFICATION_DECISION_SCHEMA_VERSION
        )
        object.__setattr__(self, "engine_id", _required_text(self.engine_id))
        for name in (
            "power_study_id",
            "portfolio_calibration_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        for name in ("evidence_ids", "residual_report_ids", "score_report_ids"):
            object.__setattr__(
                self, name, _text_tuple(getattr(self, name), allow_empty=True)
            )
        statuses = {
            _identifier(key, "gate_id"): QualificationStatus(value)
            for key, value in self.gate_statuses.items()
        }
        if set(statuses) != set(DEFAULT_HARD_GATE_FAMILIES):
            raise ValueError("engine qualification gates differ from policy")
        object.__setattr__(
            self, "gate_statuses", dict(sorted(statuses.items()))
        )
        object.__setattr__(
            self,
            "adjusted_p_values",
            _probability_mapping(self.adjusted_p_values, "adjusted_p_values"),
        )
        for name in (
            "benchmark_eligible",
            "reconstruction_eligible",
            "ensemble_eligible",
        ):
            if type(getattr(self, name)) is not bool:
                raise TypeError(f"{name} must be boolean")
        if self.ensemble_eligible and not self.reconstruction_eligible:
            raise ValueError(
                "ensemble eligibility requires reconstruction eligibility"
            )
        if self.reconstruction_eligible and not self.benchmark_eligible:
            raise ValueError(
                "reconstruction eligibility requires benchmark eligibility"
            )
        object.__setattr__(self, "status", QualificationStatus(self.status))
        object.__setattr__(
            self,
            "reason_codes",
            _text_tuple(self.reason_codes, allow_empty=False),
        )
        expected = _stable_id("engine-qualification-decision", self.payload())
        if self.decision_id and self.decision_id != expected:
            raise ValueError("engine qualification decision identity differs")
        object.__setattr__(self, "decision_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "engine_id": self.engine_id,
            "evidence_ids": list(self.evidence_ids),
            "residual_report_ids": list(self.residual_report_ids),
            "score_report_ids": list(self.score_report_ids),
            "power_study_id": self.power_study_id,
            "portfolio_calibration_id": self.portfolio_calibration_id,
            "gate_statuses": {
                key: value.value for key, value in self.gate_statuses.items()
            },
            "adjusted_p_values": dict(self.adjusted_p_values),
            "benchmark_eligible": self.benchmark_eligible,
            "reconstruction_eligible": self.reconstruction_eligible,
            "ensemble_eligible": self.ensemble_eligible,
            "status": self.status.value,
            "reason_codes": list(self.reason_codes),
            "automatic_winner": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "decision_id": self.decision_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EngineQualificationDecisionV1:
        if data.get("automatic_winner") is not False:
            raise ValueError("engine qualification cannot select a winner")
        return cls(
            engine_id=str(data.get("engine_id", "")),
            evidence_ids=_string_tuple(data.get("evidence_ids")),
            residual_report_ids=_string_tuple(data.get("residual_report_ids")),
            score_report_ids=_string_tuple(data.get("score_report_ids")),
            power_study_id=str(data.get("power_study_id", "")),
            portfolio_calibration_id=str(
                data.get("portfolio_calibration_id", "")
            ),
            gate_statuses={
                str(key): QualificationStatus(str(value))
                for key, value in _mapping(data.get("gate_statuses")).items()
            },
            adjusted_p_values={
                str(key): _finite_float(value, str(key))
                for key, value in _mapping(
                    data.get("adjusted_p_values")
                ).items()
            },
            benchmark_eligible=_strict_bool(
                data.get("benchmark_eligible"), "benchmark_eligible"
            ),
            reconstruction_eligible=_strict_bool(
                data.get("reconstruction_eligible"), "reconstruction_eligible"
            ),
            ensemble_eligible=_strict_bool(
                data.get("ensemble_eligible"), "ensemble_eligible"
            ),
            status=QualificationStatus(str(data.get("status", ""))),
            reason_codes=_string_tuple(data.get("reason_codes")),
            decision_id=str(data.get("decision_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class PoweredQualificationDossierV1:
    """Complete machine-readable #490 evidence and eligibility boundary."""

    evaluation_id: str
    registry_id: str
    corpus_id: str
    campaign_id: str
    experiment_id: str
    trace_id: str
    policy: PoweredQualificationPolicyV1
    power_study: QualificationPowerStudyV1
    residual_reports: tuple[PointProcessResidualReportV1, ...]
    score_reports: tuple[PredictiveScoreReportV1, ...]
    portfolio_calibration: ProposalPortfolioCalibrationV1
    engine_decisions: tuple[EngineQualificationDecisionV1, ...]
    control_checks: Mapping[str, bool]
    input_artifacts: Mapping[str, ArtifactRef]
    implementation_sha256: str
    dossier_id: str = ""
    schema_version: str = POWERED_QUALIFICATION_DOSSIER_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, POWERED_QUALIFICATION_DOSSIER_SCHEMA_VERSION
        )
        for name in (
            "evaluation_id",
            "registry_id",
            "corpus_id",
            "campaign_id",
            "experiment_id",
            "trace_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        if not isinstance(self.policy, PoweredQualificationPolicyV1):
            raise TypeError("qualification dossier policy must use v1")
        if not isinstance(self.power_study, QualificationPowerStudyV1):
            raise TypeError("qualification dossier power study must use v1")
        if (
            self.power_study.policy_id != self.policy.policy_id
            or self.power_study.trace_id != self.trace_id
        ):
            raise ValueError("qualification power study identity differs")
        residuals = tuple(
            sorted(self.residual_reports, key=lambda item: item.report_id)
        )
        scores = tuple(
            sorted(self.score_reports, key=lambda item: item.report_id)
        )
        decisions = tuple(
            sorted(self.engine_decisions, key=lambda item: item.engine_id)
        )
        if not residuals or not scores or not decisions:
            raise ValueError("qualification dossier evidence is incomplete")
        if len({item.engine_id for item in decisions}) != len(decisions):
            raise ValueError("qualification dossier engine decisions duplicate")
        if not isinstance(
            self.portfolio_calibration, ProposalPortfolioCalibrationV1
        ):
            raise TypeError("qualification portfolio calibration must use v1")
        if self.portfolio_calibration.evaluation_id != self.evaluation_id:
            raise ValueError("qualification portfolio evaluation differs")
        object.__setattr__(self, "residual_reports", residuals)
        object.__setattr__(self, "score_reports", scores)
        object.__setattr__(self, "engine_decisions", decisions)
        checks = {
            _identifier(key, "control_check"): _strict_bool(value, key)
            for key, value in self.control_checks.items()
        }
        if set(checks) != {
            "dense_identity_behaves_as_reference",
            "negative_control_fails_for_anchor_loss",
            "protected_splits_disjoint",
            "histdata_only",
        }:
            raise ValueError("qualification control checks differ")
        object.__setattr__(self, "control_checks", dict(sorted(checks.items())))
        artifacts = {
            _identifier(key, "input_artifact"): value
            for key, value in sorted(self.input_artifacts.items())
        }
        if not artifacts or any(
            not isinstance(value, ArtifactRef) for value in artifacts.values()
        ):
            raise TypeError("qualification dossier input artifacts are invalid")
        object.__setattr__(self, "input_artifacts", artifacts)
        object.__setattr__(
            self,
            "implementation_sha256",
            _sha256(self.implementation_sha256, "implementation_sha256"),
        )
        expected = _stable_id("powered-qualification-dossier", self.payload())
        if self.dossier_id and self.dossier_id != expected:
            raise ValueError("powered qualification dossier identity differs")
        object.__setattr__(self, "dossier_id", expected)
        if (
            len(self.to_json().encode("utf-8"))
            > MAX_QUALIFICATION_ARTIFACT_BYTES
        ):
            raise ValueError("qualification dossier exceeds artifact bound")

    @property
    def reconstruction_eligible_engine_ids(self) -> tuple[str, ...]:
        return tuple(
            item.engine_id
            for item in self.engine_decisions
            if item.reconstruction_eligible
        )

    @property
    def ensemble_eligible_engine_ids(self) -> tuple[str, ...]:
        return tuple(
            item.engine_id
            for item in self.engine_decisions
            if item.ensemble_eligible
        )

    def decision(self, engine_id: str) -> EngineQualificationDecisionV1:
        for item in self.engine_decisions:
            if item.engine_id == engine_id:
                return item
        raise ValueError(f"qualification decision is absent: {engine_id}")

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "evaluation_id": self.evaluation_id,
            "registry_id": self.registry_id,
            "corpus_id": self.corpus_id,
            "campaign_id": self.campaign_id,
            "experiment_id": self.experiment_id,
            "trace_id": self.trace_id,
            "policy": self.policy.to_dict(),
            "power_study": self.power_study.to_dict(),
            "residual_reports": [
                item.to_dict() for item in self.residual_reports
            ],
            "score_reports": [item.to_dict() for item in self.score_reports],
            "portfolio_calibration": self.portfolio_calibration.to_dict(),
            "engine_decisions": [
                item.to_dict() for item in self.engine_decisions
            ],
            "control_checks": dict(self.control_checks),
            "input_artifact_identities": {
                key: _artifact_identity(value)
                for key, value in self.input_artifacts.items()
            },
            "implementation_sha256": self.implementation_sha256,
            "provider_id": CURRENT_QUALIFICATION_PROVIDER_ID,
            "source_format": CURRENT_QUALIFICATION_SOURCE_FORMAT,
            "timeframe": CURRENT_QUALIFICATION_TIMEFRAME,
            "reconstruction_eligible_engine_ids": list(
                self.reconstruction_eligible_engine_ids
            ),
            "ensemble_eligible_engine_ids": list(
                self.ensemble_eligible_engine_ids
            ),
            "automatic_winner": False,
            "historical_truth_claim": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.payload(),
            "input_artifacts": {
                key: value.to_dict()
                for key, value in self.input_artifacts.items()
            },
            "dossier_id": self.dossier_id,
        }

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> PoweredQualificationDossierV1:
        if (
            data.get("provider_id") != CURRENT_QUALIFICATION_PROVIDER_ID
            or data.get("source_format") != CURRENT_QUALIFICATION_SOURCE_FORMAT
            or data.get("timeframe") != CURRENT_QUALIFICATION_TIMEFRAME
            or data.get("automatic_winner") is not False
            or data.get("historical_truth_claim") is not False
        ):
            raise ValueError("qualification dossier scope or nonclaim differs")
        dossier = cls(
            evaluation_id=str(data.get("evaluation_id", "")),
            registry_id=str(data.get("registry_id", "")),
            corpus_id=str(data.get("corpus_id", "")),
            campaign_id=str(data.get("campaign_id", "")),
            experiment_id=str(data.get("experiment_id", "")),
            trace_id=str(data.get("trace_id", "")),
            policy=PoweredQualificationPolicyV1.from_dict(
                _mapping(data.get("policy"))
            ),
            power_study=QualificationPowerStudyV1.from_dict(
                _mapping(data.get("power_study"))
            ),
            residual_reports=tuple(
                PointProcessResidualReportV1.from_dict(_mapping(item))
                for item in _sequence(data.get("residual_reports"))
            ),
            score_reports=tuple(
                PredictiveScoreReportV1.from_dict(_mapping(item))
                for item in _sequence(data.get("score_reports"))
            ),
            portfolio_calibration=ProposalPortfolioCalibrationV1.from_dict(
                _mapping(data.get("portfolio_calibration"))
            ),
            engine_decisions=tuple(
                EngineQualificationDecisionV1.from_dict(_mapping(item))
                for item in _sequence(data.get("engine_decisions"))
            ),
            control_checks={
                str(key): _strict_bool(value, str(key))
                for key, value in _mapping(data.get("control_checks")).items()
            },
            input_artifacts={
                str(key): ArtifactRef.from_dict(_mapping(value))
                for key, value in _mapping(data.get("input_artifacts")).items()
            },
            implementation_sha256=str(data.get("implementation_sha256", "")),
            dossier_id=str(data.get("dossier_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        expected_identities = {
            key: _artifact_identity(value)
            for key, value in dossier.input_artifacts.items()
        }
        if data.get("input_artifact_identities") != expected_identities:
            raise ValueError("qualification input artifact identities differ")
        if data.get("reconstruction_eligible_engine_ids") != list(
            dossier.reconstruction_eligible_engine_ids
        ):
            raise ValueError("qualification reconstruction eligibility differs")
        if data.get("ensemble_eligible_engine_ids") != list(
            dossier.ensemble_eligible_engine_ids
        ):
            raise ValueError("qualification ensemble eligibility differs")
        return dossier

    @classmethod
    def from_json(cls, text: str) -> PoweredQualificationDossierV1:
        if len(text.encode("utf-8")) > MAX_QUALIFICATION_ARTIFACT_BYTES:
            raise ValueError("qualification dossier JSON exceeds bound")
        payload = json.loads(text)
        return cls.from_dict(_mapping(payload))


def _build_powered_qualification_dossier(
    evaluation_path: str | Path,
    experiment_path: str | Path,
    *,
    policy: PoweredQualificationPolicyV1,
) -> PoweredQualificationDossierV1:
    """Derive one dossier without writing or trusting retained decisions."""
    selected_policy = policy
    evaluation = read_proposal_portfolio_evaluation(evaluation_path)
    registry = proposal_engine_registry()
    if evaluation.registry_id != registry.registry_id:
        raise ValueError("proposal evaluation registry is stale")
    trace_ref = evaluation.artifact_refs.get("window_metric_trace")
    scorecard_ref = evaluation.artifact_refs.get("scorecard")
    manifest_ref = evaluation.artifact_refs.get("manifest")
    if trace_ref is None or scorecard_ref is None or manifest_ref is None:
        raise ValueError("proposal evaluation lacks #490 metric evidence")
    verify_artifact_ref(trace_ref)
    verify_artifact_ref(scorecard_ref)
    verify_artifact_ref(manifest_ref)
    trace = read_benchmark_window_metric_trace(trace_ref.path)
    campaign = read_reverse_degradation_benchmark_campaign(scorecard_ref.path)
    corpus = read_reverse_degradation_benchmark_corpus(manifest_ref.path)
    if (
        trace.corpus_id != evaluation.corpus_id
        or trace.campaign_id != evaluation.campaign_id
        or campaign.corpus_id != evaluation.corpus_id
        or campaign.campaign_id != evaluation.campaign_id
    ):
        raise ValueError("qualification campaign/trace identity differs")
    experiment = read_reconstruction_experiment(experiment_path)
    verification = verify_reconstruction_experiment(experiment)
    if not verification.verified:
        raise ValueError("qualification experiment verification failed")
    _verify_experiment_scope(experiment, evaluation, corpus)

    evidence_by_engine = {
        item.engine_id: item for item in evaluation.engine_evidence
    }
    candidate_to_engine = {
        item.candidate_id: item.engine_id for item in evaluation.engine_evidence
    }
    observations_by_engine: dict[
        str, list[BenchmarkWindowMetricObservationV1]
    ] = defaultdict(list)
    for observation in trace.observations:
        engine_id = candidate_to_engine.get(observation.candidate_id)
        if engine_id is not None:
            observations_by_engine[engine_id].append(observation)

    scales = _validation_feature_scales(
        tuple(
            item
            for engine_id in evaluation.executed_engine_ids
            for item in observations_by_engine.get(engine_id, ())
        )
    )
    residual_reports = tuple(
        _trace_residual_report(
            engine_id,
            evidence_by_engine[engine_id],
            tuple(observations_by_engine.get(engine_id, ())),
            split_kind,
            trace.trace_id,
            selected_policy,
        )
        for engine_id in evaluation.executed_engine_ids
        for split_kind in ("validation", "final_holdout")
    )
    engine_score_reports = tuple(
        _predictive_score_report(
            engine_id,
            tuple(observations_by_engine.get(engine_id, ())),
            split_kind,
            trace.trace_id,
            scales,
            selected_policy,
        )
        for engine_id in evaluation.executed_engine_ids
        for split_kind in ("validation", "final_holdout")
    )
    control_score_reports = tuple(
        _predictive_score_report(
            f"control:{method_name}",
            tuple(
                item
                for item in trace.observations
                if item.method_name == method_name
            ),
            split_kind,
            trace.trace_id,
            scales,
            selected_policy,
        )
        for method_name in QUALIFICATION_CONTROL_METHODS
        for split_kind in ("validation", "final_holdout")
    )
    score_reports = (*engine_score_reports, *control_score_reports)
    power_engine_ids = {
        engine_id
        for engine_id in evaluation.executed_engine_ids
        if evidence_by_engine[engine_id].promotion_eligible
        and not evidence_by_engine[engine_id].provisional
    }
    final_residuals = tuple(
        item
        for item in residual_reports
        if item.split_kind == "final_holdout"
        and item.engine_id in power_engine_ids
    )
    final_scores = tuple(
        item
        for item in engine_score_reports
        if item.split_kind == "final_holdout"
        and item.engine_id in power_engine_ids
    )
    residual_support = min(
        (item.sample_count for item in final_residuals), default=0
    )
    window_support = min(
        (item.window_count for item in final_scores), default=0
    )
    member_support = min(
        (item.member_observation_count for item in final_scores), default=0
    )
    coverage_support = min(
        (item.window_count for item in final_scores), default=0
    )
    event_support = min(
        (
            item.stratum_support_counts.get("event_windows", 0)
            for item in final_scores
        ),
        default=0,
    )
    support_by_gate = {
        "time_uniformity": residual_support,
        "time_serial_dependence": residual_support,
        "mark_calibration": residual_support,
        "multivariate_energy": window_support,
        "multivariate_variogram": window_support,
        "calibration_sharpness": coverage_support,
        "path_tail": window_support,
        "regime_transition": member_support,
        "event_response": event_support,
        "refusal_calibration": member_support,
    }
    power_study = run_qualification_power_study(
        selected_policy,
        trace_id=trace.trace_id,
        observed_support_by_gate=support_by_gate,
    )
    controls = _control_checks(trace, experiment)
    benchmark_calibration_engines = tuple(
        engine_id
        for engine_id in evaluation.executed_engine_ids
        if observations_by_engine.get(engine_id)
        and evidence_by_engine[engine_id].promotion_eligible
        and not evidence_by_engine[engine_id].provisional
    )
    calibration = _calibrate_portfolio(
        evaluation,
        trace,
        observations_by_engine,
        engine_ids=benchmark_calibration_engines,
        scales=scales,
        minimum_window_count=selected_policy.minimum_window_count,
    )
    decisions = _engine_decisions(
        evaluation,
        residual_reports,
        score_reports,
        power_study,
        calibration,
        controls,
        selected_policy,
    )
    input_artifacts = {
        "evaluation": artifact_ref_for_file(
            evaluation_path, kind="proposal_portfolio_evaluation_v1"
        ),
        "experiment": artifact_ref_for_file(
            experiment_path, kind="reconstruction_experiment_manifest_v1"
        ),
        "scorecard": scorecard_ref,
        "benchmark_manifest": manifest_ref,
        "window_metric_trace": trace_ref,
    }
    return PoweredQualificationDossierV1(
        evaluation_id=evaluation.evaluation_id,
        registry_id=evaluation.registry_id,
        corpus_id=evaluation.corpus_id,
        campaign_id=evaluation.campaign_id,
        experiment_id=experiment.experiment_id,
        trace_id=trace.trace_id,
        policy=selected_policy,
        power_study=power_study,
        residual_reports=residual_reports,
        score_reports=score_reports,
        portfolio_calibration=calibration,
        engine_decisions=decisions,
        control_checks=controls,
        input_artifacts=input_artifacts,
        implementation_sha256=_implementation_sha256(),
    )


def qualify_histdata_proposal_portfolio(
    evaluation_path: str | Path,
    experiment_path: str | Path,
    *,
    output_directory: str | Path,
    policy: PoweredQualificationPolicyV1 | None = None,
) -> PoweredQualificationDossierV1:
    """Qualify one exact #489 evaluation through the installed #490 path."""
    dossier = _build_powered_qualification_dossier(
        evaluation_path,
        experiment_path,
        policy=policy or PoweredQualificationPolicyV1(),
    )
    write_powered_qualification_dossier(dossier, output_directory)
    return dossier


def verify_powered_qualification_dossier(
    dossier: PoweredQualificationDossierV1,
) -> None:
    """Fail closed when retained qualification evidence is stale or mismatched."""
    cache = _VERIFIED_QUALIFICATION_DOSSIERS.get()
    dossier_id = str(getattr(dossier, "dossier_id", ""))
    if cache is not None and dossier_id in cache:
        if cache[dossier_id] != dossier:
            raise ValueError("qualification dossier identity collision")
        return
    _verify_powered_qualification_dossier_uncached(dossier)
    if cache is not None:
        cache[dossier_id] = dossier


def _verify_powered_qualification_dossier_uncached(
    dossier: PoweredQualificationDossierV1,
) -> None:
    """Perform the complete deterministic powered-evidence recomputation."""
    if not isinstance(dossier, PoweredQualificationDossierV1):
        raise TypeError("qualification dossier must use v1")
    if dossier.implementation_sha256 != _implementation_sha256():
        raise ValueError("qualification implementation identity is stale")
    registry = proposal_engine_registry()
    if dossier.registry_id != registry.registry_id:
        raise ValueError("qualification registry identity is stale")
    if set(dossier.input_artifacts) != {
        "evaluation",
        "experiment",
        "benchmark_manifest",
        "scorecard",
        "window_metric_trace",
    }:
        raise ValueError("qualification input artifact set differs")
    for ref in dossier.input_artifacts.values():
        verify_artifact_ref(ref)
    evaluation = read_proposal_portfolio_evaluation(
        dossier.input_artifacts["evaluation"].path
    )
    experiment = read_reconstruction_experiment(
        dossier.input_artifacts["experiment"].path
    )
    trace = read_benchmark_window_metric_trace(
        dossier.input_artifacts["window_metric_trace"].path
    )
    campaign = read_reverse_degradation_benchmark_campaign(
        dossier.input_artifacts["scorecard"].path
    )
    corpus = read_reverse_degradation_benchmark_corpus(
        dossier.input_artifacts["benchmark_manifest"].path
    )
    if (
        evaluation.evaluation_id != dossier.evaluation_id
        or evaluation.registry_id != dossier.registry_id
        or evaluation.corpus_id != dossier.corpus_id
        or evaluation.campaign_id != dossier.campaign_id
        or experiment.experiment_id != dossier.experiment_id
        or trace.trace_id != dossier.trace_id
        or trace.corpus_id != dossier.corpus_id
        or trace.campaign_id != dossier.campaign_id
        or campaign.corpus_id != dossier.corpus_id
        or campaign.campaign_id != dossier.campaign_id
        or corpus.corpus_id != dossier.corpus_id
    ):
        raise ValueError("qualification retained input identity differs")
    if (
        evaluation.artifact_refs.get("scorecard")
        != dossier.input_artifacts["scorecard"]
        or evaluation.artifact_refs.get("manifest")
        != dossier.input_artifacts["benchmark_manifest"]
        or evaluation.artifact_refs.get("window_metric_trace")
        != dossier.input_artifacts["window_metric_trace"]
    ):
        raise ValueError("qualification evaluation component binding differs")
    verification = verify_reconstruction_experiment(experiment)
    if not verification.verified:
        raise ValueError("qualification experiment verification failed")
    _verify_experiment_scope(experiment, evaluation, corpus)
    if {item.engine_id for item in dossier.engine_decisions} != set(
        evaluation.requested_engine_ids
    ):
        raise ValueError("qualification engine-decision coverage differs")
    expected = _build_powered_qualification_dossier(
        dossier.input_artifacts["evaluation"].path,
        dossier.input_artifacts["experiment"].path,
        policy=dossier.policy,
    )
    if dossier != expected:
        raise ValueError(
            "qualification dossier differs from deterministic recomputation"
        )


@contextmanager
def powered_qualification_verification_scope() -> Iterator[None]:
    """Reuse one verified dossier only within a bounded top-level operation."""
    existing = _VERIFIED_QUALIFICATION_DOSSIERS.get()
    if existing is not None:
        yield
        return
    token = _VERIFIED_QUALIFICATION_DOSSIERS.set({})
    try:
        yield
    finally:
        _VERIFIED_QUALIFICATION_DOSSIERS.reset(token)


def write_powered_qualification_dossier(
    dossier: PoweredQualificationDossierV1,
    output_directory: str | Path,
) -> ArtifactRef:
    """Atomically persist one content-addressed powered dossier."""
    if not isinstance(dossier, PoweredQualificationDossierV1):
        raise TypeError("qualification dossier must use v1")
    root = Path(output_directory).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    encoded = dossier.to_json().encode("utf-8") + b"\n"
    digest = hashlib.sha256(encoded).hexdigest()
    target = root / f"powered-qualification-dossier-{digest}.json"
    _write_once(target, encoded)
    return ArtifactRef(
        kind="powered_qualification_dossier_v1",
        path=str(target),
        size_bytes=len(encoded),
        sha256=digest,
        metadata={
            "dossier_id": dossier.dossier_id,
            "evaluation_id": dossier.evaluation_id,
            "experiment_id": dossier.experiment_id,
        },
    )


def read_powered_qualification_dossier(
    path: str | Path,
) -> PoweredQualificationDossierV1:
    """Read and hash-verify one content-addressed powered dossier."""
    payload = _read_content_addressed_json(
        path,
        prefix="powered-qualification-dossier",
    )
    return PoweredQualificationDossierV1.from_dict(payload)


def _trace_residual_report(
    engine_id: str,
    evidence: ProposalEngineEvidenceV1,
    observations: Sequence[BenchmarkWindowMetricObservationV1],
    split_kind: str,
    trace_id: str,
    policy: PoweredQualificationPolicyV1,
) -> PointProcessResidualReportV1:
    selected = tuple(
        item for item in observations if item.split_kind == split_kind
    )
    grouped = _group_by_window(selected)
    sample_count = len(grouped)
    time_ks_values = tuple(
        _mean(
            [
                item.comparison_metrics.get("simulation_time_pit_ks", 1.0)
                for item in cells
            ]
        )
        for cells in grouped.values()
    )
    time_lag_values = tuple(
        _mean(
            [
                item.comparison_metrics.get("simulation_time_pit_lag1_abs", 1.0)
                for item in cells
            ]
        )
        for cells in grouped.values()
    )
    mark_ks_values = tuple(
        _mean(
            [
                item.comparison_metrics.get("simulation_mark_pit_ks", 1.0)
                for item in cells
            ]
        )
        for cells in grouped.values()
    )
    mean_time = _mean(time_ks_values) if time_ks_values else 1.0
    mean_mark = _mean(mark_ks_values) if mark_ks_values else 1.0
    mean_lag = _mean(time_lag_values) if time_lag_values else 1.0
    time_upper = _upper_mean_confidence_bound(time_ks_values, policy.alpha)
    mark_upper = _upper_mean_confidence_bound(mark_ks_values, policy.alpha)
    lag_upper = _upper_mean_confidence_bound(time_lag_values, policy.alpha)
    time_p = _familywise_residual_p_value(grouped, "simulation_time_pit_ks")
    mark_p = _familywise_residual_p_value(grouped, "simulation_mark_pit_ks")
    if sample_count < policy.minimum_window_count:
        status = QualificationStatus.INSUFFICIENT_EVIDENCE
        reasons = ("protected_window_support_below_policy_minimum",)
    elif (
        time_upper > PRACTICAL_RESIDUAL_TOLERANCE
        or mark_upper > PRACTICAL_RESIDUAL_TOLERANCE
        or lag_upper > PRACTICAL_RESIDUAL_TOLERANCE
    ):
        status = QualificationStatus.FAILED
        reasons = ("clustered_simulation_predictive_practical_gate_failed",)
    else:
        status = QualificationStatus.PASSED
        reasons = ("clustered_simulation_predictive_practical_gate_passed",)
    config_id = (
        evidence.config_ids[0] if evidence.config_ids else "unbound-config"
    )
    input_id = _stable_id(
        "trace-residual-input",
        {
            "engine_id": engine_id,
            "config_id": config_id,
            "split_kind": split_kind,
            "trace_id": trace_id,
            "observation_ids": [item.observation_id for item in selected],
        },
    )
    return PointProcessResidualReportV1(
        engine_id=engine_id,
        config_id=config_id,
        split_kind=split_kind,
        stratum_id="all-histdata-protected-windows",
        method=PointProcessResidualMethod.SIMULATION_PREDICTIVE,
        sample_count=sample_count,
        mark_sample_count=sample_count,
        time_uniform_ks=mean_time,
        time_uniform_p_value=time_p,
        time_lag1_autocorrelation=mean_lag,
        mark_uniform_ks=mean_mark,
        mark_uniform_p_value=mark_p,
        quantiles={
            "window_time_ks_q50": _quantile(time_ks_values, 0.50),
            "window_time_ks_q95": _quantile(time_ks_values, 0.95),
            "window_time_ks_upper95": time_upper,
            "window_time_lag1_abs_q50": _quantile(time_lag_values, 0.50),
            "window_time_lag1_abs_q95": _quantile(time_lag_values, 0.95),
            "window_time_lag1_abs_upper95": lag_upper,
            "window_mark_ks_q50": _quantile(mark_ks_values, 0.50),
            "window_mark_ks_q95": _quantile(mark_ks_values, 0.95),
            "window_mark_ks_upper95": mark_upper,
            "practical_tolerance": PRACTICAL_RESIDUAL_TOLERANCE,
        },
        status=status,
        reason_codes=reasons,
        residual_input_id=input_id,
    )


def _upper_mean_confidence_bound(
    values: Sequence[float], alpha: float
) -> float:
    """Cluster-level one-sided bound; windows, not ticks, are independent."""
    selected = tuple(_finite_float(value, "cluster metric") for value in values)
    if not selected:
        return 1.0
    mean = _mean(selected)
    if len(selected) < 2:
        return mean
    critical = _student_t_one_sided_critical(alpha, len(selected) - 1)
    return mean + critical * statistics.stdev(selected) / math.sqrt(
        len(selected)
    )


def _lower_mean_confidence_bound(
    values: Sequence[float], alpha: float
) -> float:
    """Cluster-level one-sided lower bound for failure-detection power."""
    selected = tuple(_finite_float(value, "cluster metric") for value in values)
    if not selected:
        return 0.0
    mean = _mean(selected)
    if len(selected) < 2:
        return mean
    critical = _student_t_one_sided_critical(alpha, len(selected) - 1)
    return mean - critical * statistics.stdev(selected) / math.sqrt(
        len(selected)
    )


def _familywise_residual_p_value(
    grouped: Mapping[str, Sequence[BenchmarkWindowMetricObservationV1]],
    metric_name: str,
) -> float:
    """Conservative descriptive KS p-value; it is not the cluster gate."""
    p_values: list[float] = []
    for cells in grouped.values():
        event_count = max(
            1, round(cells[0].reference_metrics.get("event_count", 0.0))
        )
        p_values.extend(
            _ks_uniform_p_value(
                item.comparison_metrics.get(metric_name, 1.0), event_count
            )
            for item in cells
        )
    if not p_values:
        return 0.0
    return min(1.0, min(p_values) * len(p_values))


def _validation_feature_scales(
    observations: Sequence[BenchmarkWindowMetricObservationV1],
) -> dict[str, float]:
    validation = tuple(
        item for item in observations if item.split_kind == "validation"
    )
    if not validation:
        raise ValueError("qualification trace has no validation observations")
    names = (
        set(validation[0].reference_metrics) - DEFAULT_SCORE_FEATURE_EXCLUSIONS
    )
    if any(
        (set(item.reference_metrics) - DEFAULT_SCORE_FEATURE_EXCLUSIONS)
        != names
        for item in validation
    ):
        raise ValueError("qualification reference feature registry differs")
    scales: dict[str, float] = {}
    for name in sorted(names):
        values = [item.reference_metrics[name] for item in validation]
        center = statistics.median(values)
        deviations = [abs(value - center) for value in values]
        mad = statistics.median(deviations)
        scales[name] = max(abs(center), 1.4826 * mad, 1e-9)
    return scales


def _predictive_score_report(
    engine_id: str,
    observations: Sequence[BenchmarkWindowMetricObservationV1],
    split_kind: str,
    trace_id: str,
    scales: Mapping[str, float],
    policy: PoweredQualificationPolicyV1,
) -> PredictiveScoreReportV1:
    selected = tuple(
        item for item in observations if item.split_kind == split_kind
    )
    grouped = _group_by_window(selected)
    feature_names = tuple(sorted(scales))
    stratum_support = _score_stratum_support(grouped)
    event_observations = tuple(
        item
        for item in selected
        if item.context_state is not None
        and not item.context_state.startswith("market_context:none:")
    )
    regime_transition_error = (
        _mean(
            [
                item.comparison_metrics.get("update_transition_l1", 1.0)
                for item in selected
            ]
        )
        if selected
        else None
    )
    event_response_error = (
        _mean(
            [
                _event_response_observation_error(item.comparison_metrics)
                for item in event_observations
            ]
        )
        if event_observations
        else None
    )
    if split_kind == "final_holdout":
        validation_grouped = _group_by_window(
            tuple(
                item for item in observations if item.split_kind == "validation"
            )
        )
        interval_adjustments, nominal_coverage = (
            _split_conformal_interval_adjustments(
                validation_grouped,
                feature_names,
                scales,
                target_coverage=CONFORMAL_TARGET_COVERAGE,
            )
        )
        interval_method = VALIDATION_SPLIT_CONFORMAL_METHOD
        interval_calibration_count = len(validation_grouped)
    else:
        interval_adjustments = tuple(0.0 for _ in feature_names)
        nominal_coverage = _mean(
            [_rank_envelope_coverage(len(cells)) for cells in grouped.values()]
        )
        interval_method = RAW_RANK_ENVELOPE_METHOD
        interval_calibration_count = 0
    if len(grouped) < policy.minimum_window_count:
        return PredictiveScoreReportV1(
            engine_id=engine_id,
            split_kind=split_kind,
            feature_names=feature_names,
            window_count=len(grouped),
            member_observation_count=len(selected),
            energy_score=None,
            variogram_score_p05=None,
            variogram_score_p1=None,
            marginal_crps=None,
            nominal_coverage=nominal_coverage,
            empirical_coverage=None,
            calibration_error=None,
            sharpness=None,
            tail_error=None,
            path_error=None,
            cross_series_error=None,
            status=QualificationStatus.INSUFFICIENT_EVIDENCE,
            reason_codes=("protected_window_support_below_policy_minimum",),
            trace_id=trace_id,
            regime_transition_error=regime_transition_error,
            event_response_error=event_response_error,
            stratum_support_counts=stratum_support,
            interval_calibration_method=interval_method,
            interval_calibration_window_count=interval_calibration_count,
            mean_interval_adjustment=(
                _mean(interval_adjustments) if interval_adjustments else None
            ),
        )
    energies: list[float] = []
    variograms05: list[float] = []
    variograms1: list[float] = []
    crps_values: list[float] = []
    covered = 0
    coverage_total = 0
    widths: list[float] = []
    for cells in grouped.values():
        reference = _normalized_vector(
            cells[0].reference_metrics, feature_names, scales
        )
        members = tuple(
            _normalized_vector(item.candidate_metrics, feature_names, scales)
            for item in cells
        )
        energies.append(_energy_score(reference, members))
        variograms05.append(_variogram_score(reference, members, 0.5))
        variograms1.append(_variogram_score(reference, members, 1.0))
        crps_values.append(_marginal_crps(reference, members))
        for dimension, observed in enumerate(reference):
            values = sorted(member[dimension] for member in members)
            if not values:
                continue
            adjustment = interval_adjustments[dimension]
            lower, upper = values[0] - adjustment, values[-1] + adjustment
            covered += int(lower <= observed <= upper)
            coverage_total += 1
            widths.append(upper - lower)
    coverage = covered / coverage_total if coverage_total else None
    comparisons = [item.comparison_metrics for item in selected]
    return PredictiveScoreReportV1(
        engine_id=engine_id,
        split_kind=split_kind,
        feature_names=feature_names,
        window_count=len(grouped),
        member_observation_count=len(selected),
        energy_score=_mean(energies),
        variogram_score_p05=_mean(variograms05),
        variogram_score_p1=_mean(variograms1),
        marginal_crps=_mean(crps_values),
        nominal_coverage=nominal_coverage,
        empirical_coverage=coverage,
        calibration_error=(
            abs(coverage - nominal_coverage) if coverage is not None else None
        ),
        sharpness=_mean(widths) if widths else None,
        tail_error=_mean(
            [
                values.get("spread_tail_relative_error", 1.0)
                + values.get("path_jump_relative_error", 1.0)
                for values in comparisons
            ]
        )
        / 2.0,
        path_error=_mean(
            [
                values.get("path_realized_variation_relative_error", 1.0)
                + values.get("path_excursion_relative_error", 1.0)
                for values in comparisons
            ]
        )
        / 2.0,
        cross_series_error=_mean(
            [
                values.get("triangle_synchronization_error", 1.0)
                for values in comparisons
            ]
        ),
        status=QualificationStatus.PASSED,
        reason_codes=(
            (
                "predictive_scores_and_validation_split_conformal_intervals_computed"
                if split_kind == "final_holdout"
                else "predictive_scores_and_raw_rank_intervals_computed"
            ),
        ),
        trace_id=trace_id,
        regime_transition_error=regime_transition_error,
        event_response_error=event_response_error,
        stratum_support_counts=stratum_support,
        interval_calibration_method=interval_method,
        interval_calibration_window_count=interval_calibration_count,
        mean_interval_adjustment=(
            _mean(interval_adjustments) if interval_adjustments else None
        ),
    )


def _split_conformal_interval_adjustments(
    grouped: Mapping[str, Sequence[BenchmarkWindowMetricObservationV1]],
    feature_names: Sequence[str],
    scales: Mapping[str, float],
    *,
    target_coverage: float,
) -> tuple[tuple[float, ...], float]:
    """Fit marginal finite-sample envelope corrections on validation only."""
    selected_target = _probability(
        target_coverage, "target_coverage", open_interval=True
    )
    count = len(grouped)
    if count < 2:
        raise ValueError("split conformal calibration requires two windows")
    rank = math.ceil((count + 1) * selected_target)
    if rank > count:
        raise ValueError(
            "split conformal target is unattainable at calibration support"
        )
    scores_by_dimension: list[list[float]] = [[] for _ in feature_names]
    for cells in grouped.values():
        reference = _normalized_vector(
            cells[0].reference_metrics, feature_names, scales
        )
        members = tuple(
            _normalized_vector(item.candidate_metrics, feature_names, scales)
            for item in cells
        )
        for dimension, observed in enumerate(reference):
            values = [member[dimension] for member in members]
            nonconformity = max(
                min(values) - observed,
                observed - max(values),
                0.0,
            )
            scores_by_dimension[dimension].append(nonconformity)
    adjustments = tuple(
        sorted(values)[rank - 1] for values in scores_by_dimension
    )
    return adjustments, rank / (count + 1)


def _rank_envelope_coverage(member_count: int) -> float:
    """Attainable min/max rank-envelope coverage for exchangeable draws."""
    count = _bounded_int(member_count, "ensemble member count", 0, 1_000_000)
    if count == 0:
        return 0.0
    return (count - 1.0) / (count + 1.0)


def _score_stratum_support(
    grouped: Mapping[str, Sequence[BenchmarkWindowMetricObservationV1]],
) -> dict[str, int]:
    representatives = tuple(cells[0] for cells in grouped.values() if cells)
    result = {
        "event_windows": sum(
            item.context_state is not None
            and not item.context_state.startswith("market_context:none:")
            for item in representatives
        ),
        "missing_context_windows": sum(
            item.context_state is None
            or item.context_state.startswith("market_context:none:")
            for item in representatives
        ),
    }
    for session in ("asia", "london", "new_york"):
        result[f"session_{session}"] = sum(
            item.session == session for item in representatives
        )
    return result


def _event_response_observation_error(
    metrics: Mapping[str, float],
) -> float:
    names = (
        "event_count_relative_error",
        "path_realized_variation_relative_error",
        "spread_tail_relative_error",
        "update_transition_l1",
        "burst_quiet_rate_error",
    )
    return _mean([metrics.get(name, 1.0) for name in names])


def _calibrate_portfolio(
    evaluation: ProposalPortfolioEvaluationV1,
    trace: BenchmarkWindowMetricTraceV1,
    observations_by_engine: Mapping[
        str, Sequence[BenchmarkWindowMetricObservationV1]
    ],
    *,
    engine_ids: Sequence[str],
    scales: Mapping[str, float],
    minimum_window_count: int,
) -> ProposalPortfolioCalibrationV1:
    engines = tuple(sorted(set(engine_ids)))
    validation_windows = _common_windows(
        observations_by_engine, engines, "validation"
    )
    holdout_windows = _common_windows(
        observations_by_engine, engines, "final_holdout"
    )
    if not engines:
        return ProposalPortfolioCalibrationV1(
            evaluation_id=evaluation.evaluation_id,
            trace_id=trace.trace_id,
            engine_ids=(),
            weights={},
            fit_window_ids=("no-prequalified-engine",),
            final_holdout_window_ids=("no-eligible-engine",),
            validation_energy_score=None,
            final_holdout_energy_score=None,
            final_holdout_variogram_score_p05=None,
            status=QualificationStatus.INSUFFICIENT_EVIDENCE,
            reason_codes=("no_prequalified_engine_for_portfolio",),
        )
    if (
        len(validation_windows) < minimum_window_count
        or len(holdout_windows) < minimum_window_count
    ):
        weights = {engine_id: 1.0 / len(engines) for engine_id in engines}
        return ProposalPortfolioCalibrationV1(
            evaluation_id=evaluation.evaluation_id,
            trace_id=trace.trace_id,
            engine_ids=engines,
            weights=weights,
            fit_window_ids=validation_windows or ("missing-validation",),
            final_holdout_window_ids=holdout_windows
            or ("missing-final-holdout",),
            validation_energy_score=None,
            final_holdout_energy_score=None,
            final_holdout_variogram_score_p05=None,
            status=QualificationStatus.INSUFFICIENT_EVIDENCE,
            reason_codes=("portfolio_window_support_below_policy_minimum",),
        )
    weights = _fit_energy_weights(
        engines,
        observations_by_engine,
        validation_windows,
        scales,
    )
    validation_energy, _ = _portfolio_scores(
        weights,
        observations_by_engine,
        validation_windows,
        "validation",
        scales,
    )
    holdout_energy, holdout_variogram = _portfolio_scores(
        weights,
        observations_by_engine,
        holdout_windows,
        "final_holdout",
        scales,
    )
    return ProposalPortfolioCalibrationV1(
        evaluation_id=evaluation.evaluation_id,
        trace_id=trace.trace_id,
        engine_ids=engines,
        weights=weights,
        fit_window_ids=validation_windows,
        final_holdout_window_ids=holdout_windows,
        validation_energy_score=validation_energy,
        final_holdout_energy_score=holdout_energy,
        final_holdout_variogram_score_p05=holdout_variogram,
        status=QualificationStatus.PASSED,
        reason_codes=(
            (
                "single_benchmark_engine"
                if len(engines) == 1
                else "weights_fitted_on_validation_and_frozen_before_holdout"
            ),
        ),
    )


def _engine_decisions(
    evaluation: ProposalPortfolioEvaluationV1,
    residual_reports: Sequence[PointProcessResidualReportV1],
    score_reports: Sequence[PredictiveScoreReportV1],
    power_study: QualificationPowerStudyV1,
    calibration: ProposalPortfolioCalibrationV1,
    controls: Mapping[str, bool],
    policy: PoweredQualificationPolicyV1,
) -> tuple[EngineQualificationDecisionV1, ...]:
    residual_by_engine: dict[str, list[PointProcessResidualReportV1]] = (
        defaultdict(list)
    )
    score_by_engine: dict[str, list[PredictiveScoreReportV1]] = defaultdict(
        list
    )
    for residual_report in residual_reports:
        residual_by_engine[residual_report.engine_id].append(residual_report)
    for score_report in score_reports:
        score_by_engine[score_report.engine_id].append(score_report)
    linear_control = next(
        (
            item
            for item in score_by_engine.get("control:linear_interpolation", ())
            if item.split_kind == "final_holdout"
        ),
        None,
    )
    evidence_by_engine = {
        item.engine_id: item for item in evaluation.engine_evidence
    }
    decisions: list[EngineQualificationDecisionV1] = []
    for engine_id in evaluation.requested_engine_ids:
        if engine_id in evaluation.refused_engine_ids:
            decisions.append(
                EngineQualificationDecisionV1(
                    engine_id=engine_id,
                    evidence_ids=(),
                    residual_report_ids=(),
                    score_report_ids=(),
                    power_study_id=power_study.study_id,
                    portfolio_calibration_id=calibration.calibration_id,
                    gate_statuses={
                        gate: QualificationStatus.REFUSED
                        for gate in policy.hard_gate_families
                    },
                    adjusted_p_values={},
                    benchmark_eligible=False,
                    reconstruction_eligible=False,
                    ensemble_eligible=False,
                    status=QualificationStatus.REFUSED,
                    reason_codes=(
                        "broker_target_deferred_from_histdata_milestone",
                    ),
                )
            )
            continue
        evidence = evidence_by_engine[engine_id]
        residuals = tuple(residual_by_engine.get(engine_id, ()))
        scores = tuple(score_by_engine.get(engine_id, ()))
        final_residual = next(
            (item for item in residuals if item.split_kind == "final_holdout"),
            None,
        )
        final_score = next(
            (item for item in scores if item.split_kind == "final_holdout"),
            None,
        )
        raw_p_values = {
            "time_uniformity": (
                final_residual.time_uniform_p_value
                if final_residual
                and final_residual.time_uniform_p_value is not None
                else 0.0
            ),
            "mark_calibration": (
                final_residual.mark_uniform_p_value
                if final_residual
                and final_residual.mark_uniform_p_value is not None
                else 0.0
            ),
        }
        adjusted = _benjamini_hochberg(raw_p_values)
        gates = _observed_gate_statuses(
            final_residual,
            final_score,
            evidence,
            power_study,
            controls,
            calibration,
            linear_control,
        )
        any_insufficient = any(
            status is QualificationStatus.INSUFFICIENT_EVIDENCE
            for status in gates.values()
        )
        all_passed = all(
            status is QualificationStatus.PASSED for status in gates.values()
        )
        benchmark_eligible = True
        reconstruction_eligible = bool(
            evidence.promotion_eligible
            and not evidence.provisional
            and all_passed
        )
        ensemble_eligible = bool(
            reconstruction_eligible
            and calibration.status is QualificationStatus.PASSED
            and calibration.weights.get(engine_id, 0.0) > 0.0
        )
        if reconstruction_eligible:
            status = QualificationStatus.PASSED
            reasons = (
                "all_powered_gates_and_retained_promotion_evidence_pass",
            )
        elif any_insufficient:
            status = QualificationStatus.INSUFFICIENT_EVIDENCE
            reasons = ("one_or_more_powered_gates_are_underpowered",)
        else:
            status = QualificationStatus.FAILED
            reasons = (
                (
                    "retained_campaign_not_promotion_eligible"
                    if not evidence.promotion_eligible or evidence.provisional
                    else "one_or_more_powered_gates_failed"
                ),
            )
        decisions.append(
            EngineQualificationDecisionV1(
                engine_id=engine_id,
                evidence_ids=(evidence.evidence_id,),
                residual_report_ids=tuple(item.report_id for item in residuals),
                score_report_ids=tuple(item.report_id for item in scores),
                power_study_id=power_study.study_id,
                portfolio_calibration_id=calibration.calibration_id,
                gate_statuses=gates,
                adjusted_p_values=adjusted,
                benchmark_eligible=benchmark_eligible,
                reconstruction_eligible=reconstruction_eligible,
                ensemble_eligible=ensemble_eligible,
                status=status,
                reason_codes=reasons,
            )
        )
    return tuple(decisions)


def _observed_gate_statuses(
    residual: PointProcessResidualReportV1 | None,
    score: PredictiveScoreReportV1 | None,
    evidence: ProposalEngineEvidenceV1,
    power_study: QualificationPowerStudyV1,
    controls: Mapping[str, bool],
    calibration: ProposalPortfolioCalibrationV1,
    linear_control: PredictiveScoreReportV1 | None,
) -> dict[str, QualificationStatus]:
    values: dict[str, bool | None] = {
        "time_uniformity": (
            residual.quantiles.get("window_time_ks_upper95", 1.0)
            <= PRACTICAL_RESIDUAL_TOLERANCE
            if residual is not None
            else None
        ),
        "time_serial_dependence": (
            residual.quantiles.get("window_time_lag1_abs_upper95", 1.0)
            <= PRACTICAL_RESIDUAL_TOLERANCE
            if residual is not None
            else None
        ),
        "mark_calibration": (
            residual.quantiles.get("window_mark_ks_upper95", 1.0)
            <= PRACTICAL_RESIDUAL_TOLERANCE
            if residual is not None
            else None
        ),
        "multivariate_energy": (
            score.energy_score is not None
            and linear_control is not None
            and linear_control.energy_score is not None
            and score.energy_score <= linear_control.energy_score
            if score is not None
            else None
        ),
        "multivariate_variogram": (
            score.variogram_score_p05 is not None
            and score.cross_series_error is not None
            and linear_control is not None
            and linear_control.variogram_score_p05 is not None
            and linear_control.cross_series_error is not None
            and score.variogram_score_p05 <= linear_control.variogram_score_p05
            and score.cross_series_error <= linear_control.cross_series_error
            if score is not None
            else None
        ),
        "calibration_sharpness": (
            score.calibration_error is not None
            and score.sharpness is not None
            and linear_control is not None
            and linear_control.sharpness is not None
            and score.calibration_error
            <= _binomial_coverage_tolerance(
                score.nominal_coverage,
                max(1, score.window_count),
                0.05,
            )
            and score.sharpness <= linear_control.sharpness
            if score is not None
            else None
        ),
        "path_tail": (
            score.path_error is not None
            and score.tail_error is not None
            and linear_control is not None
            and linear_control.path_error is not None
            and linear_control.tail_error is not None
            and score.path_error <= linear_control.path_error
            and score.tail_error <= linear_control.tail_error
            if score is not None
            else None
        ),
        "regime_transition": (
            "candidate-update-transition" not in evidence.failed_gate_ids
            and score is not None
            and score.regime_transition_error is not None
            and linear_control is not None
            and linear_control.regime_transition_error is not None
            and score.regime_transition_error
            <= linear_control.regime_transition_error
        ),
        "event_response": (
            score is not None
            and score.event_response_error is not None
            and linear_control is not None
            and linear_control.event_response_error is not None
            and score.event_response_error
            <= linear_control.event_response_error
            and bool(controls.get("dense_identity_behaves_as_reference"))
            and bool(controls.get("negative_control_fails_for_anchor_loss"))
        ),
        "refusal_calibration": evidence.refusal_count == 0,
    }
    statuses: dict[str, QualificationStatus] = {}
    for gate_id, observed in values.items():
        reliability = power_study.result(gate_id).status
        if (
            reliability is not QualificationStatus.PASSED
            or (
                gate_id
                in {
                    "time_uniformity",
                    "time_serial_dependence",
                    "mark_calibration",
                }
                and residual is not None
                and residual.status is QualificationStatus.INSUFFICIENT_EVIDENCE
            )
            or (
                calibration.status is QualificationStatus.INSUFFICIENT_EVIDENCE
                and gate_id
                in {
                    "multivariate_energy",
                    "multivariate_variogram",
                    "calibration_sharpness",
                }
            )
            or observed is None
        ):
            statuses[gate_id] = QualificationStatus.INSUFFICIENT_EVIDENCE
        else:
            statuses[gate_id] = (
                QualificationStatus.PASSED
                if observed
                else QualificationStatus.FAILED
            )
    return statuses


def _control_checks(
    trace: BenchmarkWindowMetricTraceV1,
    experiment: ReconstructionExperimentManifestV1,
) -> dict[str, bool]:
    dense = tuple(
        item
        for item in trace.observations
        if item.method_name == "dense_identity"
    )
    negative = tuple(
        item
        for item in trace.observations
        if item.method_name == "negative_anchor_drop"
    )
    validation_windows = {
        item.window_id
        for item in trace.observations
        if item.split_kind == "validation"
    }
    holdout_windows = {
        item.window_id
        for item in trace.observations
        if item.split_kind == "final_holdout"
    }
    return {
        "dense_identity_behaves_as_reference": bool(
            dense
            and all(
                item.comparison_metrics.get("immutable_anchor_violation_count")
                == 0.0
                and item.comparison_metrics.get("event_count_relative_error")
                == 0.0
                for item in dense
            )
        ),
        "negative_control_fails_for_anchor_loss": bool(
            negative
            and all(
                item.comparison_metrics.get(
                    "immutable_anchor_violation_count", 0.0
                )
                > 0.0
                for item in negative
            )
        ),
        "protected_splits_disjoint": not bool(
            validation_windows & holdout_windows
        ),
        "histdata_only": bool(
            experiment.leakage_audit.accepted
            and all(
                selection.source_provider_ids
                == (CURRENT_EXPERIMENT_PROVIDER_ID,)
                for selection in experiment.selections
            )
        ),
    }


def _verify_experiment_scope(
    experiment: ReconstructionExperimentManifestV1,
    evaluation: ProposalPortfolioEvaluationV1,
    corpus: ReverseDegradationBenchmarkCorpusV1,
) -> None:
    if not experiment.leakage_audit.accepted:
        raise ValueError("qualification experiment leakage audit failed")
    if any(
        selection.source_provider_ids != (CURRENT_EXPERIMENT_PROVIDER_ID,)
        or selection.source_format != CURRENT_QUALIFICATION_SOURCE_FORMAT
        or selection.timeframe != CURRENT_QUALIFICATION_TIMEFRAME
        for selection in experiment.selections
    ):
        raise ValueError("qualification experiment is not HistData ASCII/T")
    for role in (
        ReconstructionExperimentRole.MODERN_REFERENCE_TRAINING,
        ReconstructionExperimentRole.CALIBRATION,
        ReconstructionExperimentRole.PROTECTED_HOLDOUT,
    ):
        try:
            experiment.selection_for_role(role)
        except ValueError as err:
            raise ValueError(
                f"qualification experiment lacks frozen {role.value} selection"
            ) from err
    benchmark_ids = {
        binding.artifact_id
        for binding in experiment.artifact_bindings
        if binding.domain == "benchmark"
    }
    if evaluation.corpus_id not in benchmark_ids:
        raise ValueError(
            "qualification experiment does not bind the evaluation corpus"
        )
    periods_by_role: dict[ReconstructionExperimentRole, set[str]] = defaultdict(
        set
    )
    for unit in experiment.split_units:
        for role in unit.roles:
            periods_by_role[
                ReconstructionExperimentRole.from_value(role)
            ].update(unit.periods)
    expected_periods = {
        ReconstructionExperimentRole.MODERN_REFERENCE_TRAINING: set(
            corpus.profile.split_periods["calibration"]
        ),
        ReconstructionExperimentRole.CALIBRATION: set(
            corpus.profile.split_periods["validation"]
        ),
        ReconstructionExperimentRole.PROTECTED_HOLDOUT: set(
            corpus.profile.split_periods["final_holdout"]
        ),
    }
    if any(
        periods_by_role.get(role, set()) != periods
        for role, periods in expected_periods.items()
    ):
        raise ValueError(
            "qualification experiment role periods differ from the modern "
            "reference benchmark splits"
        )


def _group_by_window(
    observations: Sequence[BenchmarkWindowMetricObservationV1],
) -> dict[str, tuple[BenchmarkWindowMetricObservationV1, ...]]:
    grouped: dict[str, list[BenchmarkWindowMetricObservationV1]] = defaultdict(
        list
    )
    for item in observations:
        grouped[item.window_id].append(item)
    return {
        key: tuple(sorted(values, key=lambda item: item.ensemble_member_id))
        for key, values in sorted(grouped.items())
    }


def _common_windows(
    observations_by_engine: Mapping[
        str, Sequence[BenchmarkWindowMetricObservationV1]
    ],
    engine_ids: Sequence[str],
    split_kind: str,
) -> tuple[str, ...]:
    sets = [
        {
            item.window_id
            for item in observations_by_engine.get(engine_id, ())
            if item.split_kind == split_kind
        }
        for engine_id in engine_ids
    ]
    if not sets:
        return ()
    return tuple(sorted(set.intersection(*sets)))


def _fit_energy_weights(
    engine_ids: Sequence[str],
    observations_by_engine: Mapping[
        str, Sequence[BenchmarkWindowMetricObservationV1]
    ],
    window_ids: Sequence[str],
    scales: Mapping[str, float],
) -> dict[str, float]:
    engines = tuple(engine_ids)
    if len(engines) == 1:
        return {engines[0]: 1.0}
    feature_names = tuple(sorted(scales))
    a = [0.0] * len(engines)
    distances = [[0.0] * len(engines) for _ in engines]
    for window_id in window_ids:
        references = []
        samples_by_engine: list[tuple[tuple[float, ...], ...]] = []
        for engine_id in engines:
            cells = tuple(
                item
                for item in observations_by_engine[engine_id]
                if item.split_kind == "validation"
                and item.window_id == window_id
            )
            if not cells:
                raise ValueError(
                    "portfolio calibration engine/window cell is absent"
                )
            references.append(
                _normalized_vector(
                    cells[0].reference_metrics, feature_names, scales
                )
            )
            samples_by_engine.append(
                tuple(
                    _normalized_vector(
                        item.candidate_metrics, feature_names, scales
                    )
                    for item in cells
                )
            )
        reference = references[0]
        if any(value != reference for value in references[1:]):
            raise ValueError("portfolio calibration reference vectors differ")
        for left_index, left_samples in enumerate(samples_by_engine):
            a[left_index] += _mean(
                [_euclidean(sample, reference) for sample in left_samples]
            )
            for right_index, right_samples in enumerate(samples_by_engine):
                distances[left_index][right_index] += _mean(
                    [
                        _euclidean(left, right)
                        for left in left_samples
                        for right in right_samples
                    ]
                )
    divisor = len(window_ids)
    a = [value / divisor for value in a]
    distances = [[value / divisor for value in row] for row in distances]
    weights = [1.0 / len(engines)] * len(engines)
    for iteration in range(1, 1025):
        gradient = [
            a[index]
            - sum(
                distances[index][other] * weights[other]
                for other in range(len(engines))
            )
            for index in range(len(engines))
        ]
        learning_rate = 0.20 / math.sqrt(iteration)
        updated = [
            weights[index] * math.exp(-learning_rate * gradient[index])
            for index in range(len(engines))
        ]
        total = sum(updated)
        weights = [value / total for value in updated]
    rounded = [round(value, 12) for value in weights]
    correction = 1.0 - sum(rounded)
    rounded[max(range(len(rounded)), key=rounded.__getitem__)] += correction
    return dict(zip(engines, rounded))


def _portfolio_scores(
    weights: Mapping[str, float],
    observations_by_engine: Mapping[
        str, Sequence[BenchmarkWindowMetricObservationV1]
    ],
    window_ids: Sequence[str],
    split_kind: str,
    scales: Mapping[str, float],
) -> tuple[float, float]:
    feature_names = tuple(sorted(scales))
    energy_values: list[float] = []
    variogram_values: list[float] = []
    for window_id in window_ids:
        weighted_samples: list[tuple[tuple[float, ...], float]] = []
        reference: tuple[float, ...] | None = None
        for engine_id, weight in weights.items():
            cells = tuple(
                item
                for item in observations_by_engine[engine_id]
                if item.split_kind == split_kind and item.window_id == window_id
            )
            if not cells:
                raise ValueError(
                    "portfolio evaluation engine/window cell is absent"
                )
            current_reference = _normalized_vector(
                cells[0].reference_metrics, feature_names, scales
            )
            if reference is None:
                reference = current_reference
            elif current_reference != reference:
                raise ValueError(
                    "portfolio evaluation reference vectors differ"
                )
            member_weight = weight / len(cells)
            weighted_samples.extend(
                (
                    _normalized_vector(
                        item.candidate_metrics, feature_names, scales
                    ),
                    member_weight,
                )
                for item in cells
            )
        if reference is None:
            raise ValueError("portfolio evaluation window has no reference")
        energy_values.append(
            _weighted_energy_score(reference, weighted_samples)
        )
        variogram_values.append(
            _weighted_variogram_score(reference, weighted_samples, 0.5)
        )
    return _mean(energy_values), _mean(variogram_values)


def _normalized_vector(
    values: Mapping[str, float],
    feature_names: Sequence[str],
    scales: Mapping[str, float],
) -> tuple[float, ...]:
    return tuple(
        _finite_float(values[name], name) / scales[name]
        for name in feature_names
    )


def _energy_score(
    reference: Sequence[float], samples: Sequence[Sequence[float]]
) -> float:
    if not samples:
        raise ValueError("energy score requires predictive samples")
    first = _mean([_euclidean(sample, reference) for sample in samples])
    second = _mean(
        [_euclidean(left, right) for left in samples for right in samples]
    )
    return max(0.0, first - 0.5 * second)


def _weighted_energy_score(
    reference: Sequence[float],
    samples: Sequence[tuple[Sequence[float], float]],
) -> float:
    first = sum(
        weight * _euclidean(sample, reference) for sample, weight in samples
    )
    second = sum(
        left_weight * right_weight * _euclidean(left, right)
        for left, left_weight in samples
        for right, right_weight in samples
    )
    return max(0.0, first - 0.5 * second)


def _variogram_score(
    reference: Sequence[float],
    samples: Sequence[Sequence[float]],
    order: float,
) -> float:
    weighted = tuple((sample, 1.0 / len(samples)) for sample in samples)
    return _weighted_variogram_score(reference, weighted, order)


def _weighted_variogram_score(
    reference: Sequence[float],
    samples: Sequence[tuple[Sequence[float], float]],
    order: float,
) -> float:
    dimension = len(reference)
    if dimension < 2:
        return 0.0
    total = 0.0
    pairs = 0
    for left in range(dimension):
        for right in range(left + 1, dimension):
            observed = abs(reference[left] - reference[right]) ** order
            forecast = sum(
                weight * abs(sample[left] - sample[right]) ** order
                for sample, weight in samples
            )
            total += (observed - forecast) ** 2
            pairs += 1
    return total / pairs


def _marginal_crps(
    reference: Sequence[float], samples: Sequence[Sequence[float]]
) -> float:
    dimensions: list[float] = []
    for index, observed in enumerate(reference):
        values = [sample[index] for sample in samples]
        first = _mean([abs(value - observed) for value in values])
        second = _mean(
            [abs(left - right) for left in values for right in values]
        )
        dimensions.append(max(0.0, first - 0.5 * second))
    return _mean(dimensions)


def _euclidean(left: Sequence[float], right: Sequence[float]) -> float:
    if len(left) != len(right):
        raise ValueError("score vectors have different dimensions")
    return math.sqrt(sum((a - b) ** 2 for a, b in zip(left, right)))


def _benjamini_hochberg(values: Mapping[str, float]) -> dict[str, float]:
    ordered = sorted(
        (
            (name, _probability(value, name, open_interval=False))
            for name, value in values.items()
        ),
        key=lambda item: (item[1], item[0]),
    )
    count = len(ordered)
    adjusted: dict[str, float] = {}
    running = 1.0
    for reverse_index in range(count - 1, -1, -1):
        name, value = ordered[reverse_index]
        rank = reverse_index + 1
        running = min(running, value * count / rank)
        adjusted[name] = min(1.0, running)
    return dict(sorted(adjusted.items()))


def _uniform_ks(values: Sequence[float]) -> float:
    if not values:
        raise ValueError("uniform KS requires samples")
    ordered = sorted(
        _probability(item, "PIT", open_interval=False) for item in values
    )
    count = len(ordered)
    return max(
        max((index + 1) / count - value, value - index / count)
        for index, value in enumerate(ordered)
    )


def _ks_uniform_p_value(statistic: float, sample_count: int) -> float:
    if sample_count <= 0:
        return 0.0
    selected = _probability(statistic, "KS statistic", open_interval=False)
    root = math.sqrt(sample_count)
    transformed = (root + 0.12 + 0.11 / root) * selected
    total = 0.0
    for index in range(1, 101):
        term = math.exp(-2.0 * index * index * transformed * transformed)
        total += term if index % 2 else -term
        if term < 1e-15:
            break
    return min(1.0, max(0.0, 2.0 * total))


def _combine_p_values_fisher(values: Sequence[float]) -> float:
    """Combine independent-window p-values using Fisher's exact even-df tail."""
    if not values:
        return 0.0
    probabilities = tuple(
        max(1e-300, _probability(value, "p-value", open_interval=False))
        for value in values
    )
    half_statistic = -sum(math.log(value) for value in probabilities)
    survival = math.exp(-half_statistic) * sum(
        half_statistic**index / math.factorial(index)
        for index in range(len(probabilities))
    )
    return min(1.0, max(0.0, survival))


def _lag_one_correlation(values: Sequence[float]) -> float:
    if len(values) < 3:
        return 0.0
    left, right = values[:-1], values[1:]
    left_mean, right_mean = _mean(left), _mean(right)
    numerator = sum(
        (a - left_mean) * (b - right_mean) for a, b in zip(left, right)
    )
    denominator = math.sqrt(
        sum((value - left_mean) ** 2 for value in left)
        * sum((value - right_mean) ** 2 for value in right)
    )
    return numerator / denominator if denominator else 0.0


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
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def _mean(values: Sequence[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _artifact_identity(ref: ArtifactRef) -> dict[str, JSONValue]:
    return {
        "kind": ref.kind,
        "size_bytes": ref.size_bytes,
        "sha256": ref.sha256,
        "metadata": dict(ref.metadata),
    }


def _implementation_sha256() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def _semantic_seed(payload: Mapping[str, JSONValue]) -> int:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).digest()
    return int.from_bytes(digest[:8], "big")


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _write_once(path: Path, content: bytes) -> None:
    if path.exists():
        if path.read_bytes() != content:
            raise ValueError("qualification artifact collision")
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(content)
    os.replace(temporary, path)


def _read_content_addressed_json(
    path: str | Path, *, prefix: str
) -> Mapping[str, Any]:
    selected = Path(path).expanduser().resolve()
    if not selected.name.startswith(f"{prefix}-") or not selected.name.endswith(
        ".json"
    ):
        raise ValueError("qualification artifact name is not content addressed")
    digest = selected.name[len(prefix) + 1 : -5]
    _sha256(digest, "artifact filename digest")
    content = selected.read_bytes()
    if len(content) > MAX_QUALIFICATION_ARTIFACT_BYTES:
        raise ValueError("qualification artifact exceeds size bound")
    if hashlib.sha256(content).hexdigest() != digest:
        raise ValueError("qualification artifact content hash differs")
    return _mapping(json.loads(content.decode("utf-8")))


def _require_schema(value: str, expected: str) -> None:
    if value != expected:
        raise ValueError("unsupported qualification contract schema")


def _required_text(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("required qualification text is empty")
    return value.strip()


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    return _required_text(value)


def _identifier(value: Any, name: str) -> str:
    selected = _required_text(value)
    if len(selected) > 256 or any(
        character not in "abcdefghijklmnopqrstuvwxyz0123456789._:-"
        for character in selected
    ):
        raise ValueError(f"{name} is not a bounded lowercase identifier")
    return selected


def _sha256(value: Any, name: str) -> str:
    selected = _required_text(value)
    if len(selected) != 64 or any(
        character not in "0123456789abcdef" for character in selected
    ):
        raise ValueError(f"{name} is not lowercase SHA-256")
    return selected


def _strict_int(value: Any, name: str) -> int:
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    return value


def _strict_bool(value: Any, name: str) -> bool:
    if type(value) is not bool:
        raise TypeError(f"{name} must be boolean")
    return value


def _bounded_int(value: Any, name: str, minimum: int, maximum: int) -> int:
    selected = _strict_int(value, name)
    if not minimum <= selected <= maximum:
        raise ValueError(f"{name} is outside [{minimum},{maximum}]")
    return selected


def _positive_int(value: Any, name: str) -> int:
    selected = _strict_int(value, name)
    if selected <= 0:
        raise ValueError(f"{name} must be positive")
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


def _nonnegative_float(value: Any, name: str) -> float:
    selected = _finite_float(value, name)
    if selected < 0.0:
        raise ValueError(f"{name} must be nonnegative")
    return selected


def _optional_float(value: Any) -> float | None:
    return None if value is None else _finite_float(value, "optional float")


def _probability(value: Any, name: str, *, open_interval: bool) -> float:
    selected = _finite_float(value, name)
    valid = 0.0 < selected < 1.0 if open_interval else 0.0 <= selected <= 1.0
    if not valid:
        raise ValueError(f"{name} is outside its probability interval")
    return selected


def _bounded_floats(
    values: Iterable[Any],
    name: str,
    *,
    lower: float | None,
    upper: float | None,
) -> tuple[float, ...]:
    selected = tuple(_finite_float(value, name) for value in values)
    if len(selected) > MAX_RESIDUAL_SAMPLES:
        raise ValueError(f"{name} exceeds sample bound")
    if lower is not None and any(value < lower for value in selected):
        raise ValueError(f"{name} falls below support")
    if upper is not None and any(value > upper for value in selected):
        raise ValueError(f"{name} exceeds support")
    return selected


def _text_tuple(values: Iterable[Any], *, allow_empty: bool) -> tuple[str, ...]:
    selected = tuple(sorted({_required_text(value) for value in values}))
    if not selected and not allow_empty:
        raise ValueError("qualification text collection is empty")
    return selected


def _string_tuple(value: Any) -> tuple[str, ...]:
    return tuple(_required_text(item) for item in _sequence(value))


def _int_tuple(value: Any) -> tuple[int, ...]:
    return tuple(
        _strict_int(item, "integer tuple item") for item in _sequence(value)
    )


def _float_mapping(
    values: Mapping[str, float], name: str, maximum: int
) -> dict[str, float]:
    selected = {
        _required_text(key): _finite_float(value, f"{name} {key}")
        for key, value in values.items()
    }
    if len(selected) > maximum:
        raise ValueError(f"{name} mapping exceeds bound")
    return dict(sorted(selected.items()))


def _probability_mapping(
    values: Mapping[str, float], name: str
) -> dict[str, float]:
    selected = {
        _required_text(key): _probability(
            value, f"{name} {key}", open_interval=False
        )
        for key, value in values.items()
    }
    if len(selected) > MAX_POWER_REGIONS:
        raise ValueError(f"{name} exceeds reliability-region bound")
    return dict(
        sorted(
            selected.items(),
            key=lambda item: (
                (0, int(item[0])) if item[0].isdigit() else (1, item[0])
            ),
        )
    )


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("qualification value must be a mapping")
    return value


def _sequence(value: Any) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError("qualification value must be a sequence")
    return value


__all__ = [
    "CURRENT_QUALIFICATION_PROVIDER_ID",
    "ENGINE_QUALIFICATION_DECISION_SCHEMA_VERSION",
    "POINT_PROCESS_RESIDUAL_INPUT_SCHEMA_VERSION",
    "POINT_PROCESS_RESIDUAL_REPORT_SCHEMA_VERSION",
    "POWERED_QUALIFICATION_DOSSIER_SCHEMA_VERSION",
    "POWERED_QUALIFICATION_POLICY_SCHEMA_VERSION",
    "PREDICTIVE_SCORE_REPORT_SCHEMA_VERSION",
    "PROPOSAL_PORTFOLIO_CALIBRATION_SCHEMA_VERSION",
    "QUALIFICATION_GATE_POWER_RESULT_SCHEMA_VERSION",
    "QUALIFICATION_POWER_STUDY_SCHEMA_VERSION",
    "EngineQualificationDecisionV1",
    "PointProcessResidualInputV1",
    "PointProcessResidualMethod",
    "PointProcessResidualReportV1",
    "PoweredQualificationDossierV1",
    "PoweredQualificationPolicyV1",
    "PredictiveScoreReportV1",
    "ProposalPortfolioCalibrationV1",
    "QualificationGatePowerResultV1",
    "QualificationPowerStudyV1",
    "QualificationStatus",
    "evaluate_point_process_residuals",
    "powered_qualification_verification_scope",
    "qualify_histdata_proposal_portfolio",
    "read_powered_qualification_dossier",
    "run_qualification_power_study",
    "verify_powered_qualification_dossier",
    "write_powered_qualification_dossier",
]
