"""Observation-process uncertainty for historical reconstruction ensembles.

The v2.4 ensemble contract varies path seeds conditional on one fitted
observation operator.  This module adds a separate v2.5 semantic axis for the
qualified retention interval.  It deliberately leaves v2.4 member identities
unchanged: an uncertainty plan maps each existing member and seed to one of
three evidence-derived operator scenarios and records the decomposition between
operator and path dispersion.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from histdatacom.reconstruction_math import (
    negative_binomial_failure_moments,
)
from histdatacom.runtime_contracts import ArtifactRef, JSONValue
from histdatacom.synthetic.contracts import canonical_contract_json

OBSERVATION_UNCERTAINTY_POLICY_SCHEMA_VERSION = (
    "histdatacom.observation-uncertainty-policy.v1"
)
OBSERVATION_UNCERTAINTY_SCENARIO_SCHEMA_VERSION = (
    "histdatacom.observation-uncertainty-scenario.v1"
)
OBSERVATION_CARDINALITY_EVIDENCE_SCHEMA_VERSION = (
    "histdatacom.observation-cardinality-evidence.v1"
)
OBSERVATION_UNCERTAINTY_MEMBER_SCHEMA_VERSION = (
    "histdatacom.observation-uncertainty-member.v1"
)
OBSERVATION_UNCERTAINTY_ENSEMBLE_SCHEMA_VERSION = (
    "histdatacom.observation-uncertainty-ensemble.v1"
)
OBSERVATION_UNCERTAINTY_DIAGNOSTIC_SCHEMA_VERSION = (
    "histdatacom.observation-uncertainty-diagnostic.v1"
)
OBSERVATION_UNCERTAINTY_DECOMPOSITION_SCHEMA_VERSION = (
    "histdatacom.observation-uncertainty-decomposition.v1"
)
OBSERVATION_UNCERTAINTY_REPORT_SCHEMA_VERSION = (
    "histdatacom.observation-uncertainty-report.v1"
)

OBSERVATION_UNCERTAINTY_POLICY_ARTIFACT_KIND = (
    "observation_uncertainty_policy_v1"
)
OBSERVATION_UNCERTAINTY_ENSEMBLE_ARTIFACT_KIND = (
    "observation_uncertainty_ensemble_v1"
)
OBSERVATION_UNCERTAINTY_REPORT_ARTIFACT_KIND = (
    "observation_uncertainty_report_v1"
)

MAX_OBSERVATION_SCENARIO_MEMBERS = 256
MAX_OBSERVATION_SCENARIO_CELLS = 16_384
MAX_OBSERVATION_QUANTILE_STEPS = 2_000_000
MAX_OBSERVATION_ARTIFACT_BYTES = 64 * 1024 * 1024


class ObservationUncertaintyScenarioKind(str, Enum):
    """The frozen high, central, and low retention scenarios."""

    HIGH_RETENTION_LOW_INFILL = "high_retention_low_infill"
    CENTRAL_FITTED_RETENTION = "central_fitted_retention"
    LOW_RETENTION_HIGH_INFILL = "low_retention_high_infill"


class ObservationUncertaintyAvailability(str, Enum):
    """Availability of qualified retention-interval endpoints."""

    TWO_SIDED = "two_sided"
    LOWER_ONLY = "lower_only"
    UPPER_ONLY = "upper_only"
    UNAVAILABLE = "unavailable"


class ObservationScenarioRetentionMode(str, Enum):
    """Whether event rows or only complete aggregates are retained."""

    FULLY_RETAINED = "fully_retained"
    AGGREGATE_ONLY = "aggregate_only"


class ObservationUncertaintySplit(str, Enum):
    """Permitted calibration roles for the uncertainty report."""

    VALIDATION = "validation"
    FINAL_HOLDOUT = "final_holdout"


class ObservationGenerationStatus(str, Enum):
    """Outcome of one scenario/path diagnostic attempt."""

    COMPLETED = "completed"
    REFUSED = "refused"
    FAILED = "failed"


SCENARIO_ORDER = (
    ObservationUncertaintyScenarioKind.HIGH_RETENTION_LOW_INFILL,
    ObservationUncertaintyScenarioKind.CENTRAL_FITTED_RETENTION,
    ObservationUncertaintyScenarioKind.LOW_RETENTION_HIGH_INFILL,
)
OBSERVATION_UNCERTAINTY_METRIC_NAMES = (
    "event_count",
    "mean_interarrival",
    "path",
    "spread",
    "triangle",
    "strategy_sensitivity",
)


@dataclass(frozen=True, slots=True)
class ObservationUncertaintyPolicyV1:
    """Predeclared scenario, admission, retention, and reporting policy."""

    report_quantiles: tuple[float, ...] = (0.05, 0.50, 0.95)
    admission_quantile: float = 0.99
    minimum_path_realizations_per_scenario: int = 1
    scenario_order: tuple[ObservationUncertaintyScenarioKind, ...] = (
        SCENARIO_ORDER
    )
    fully_retained_scenarios: tuple[ObservationUncertaintyScenarioKind, ...] = (
        SCENARIO_ORDER
    )
    aggregate_only_scenarios: tuple[
        ObservationUncertaintyScenarioKind, ...
    ] = ()
    max_quantile_steps: int = MAX_OBSERVATION_QUANTILE_STEPS
    policy_id: str = ""
    schema_version: str = OBSERVATION_UNCERTAINTY_POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, OBSERVATION_UNCERTAINTY_POLICY_SCHEMA_VERSION
        )
        quantiles = tuple(
            sorted(
                {
                    _open_unit_float(value, "report_quantile")
                    for value in self.report_quantiles
                }
            )
        )
        if len(quantiles) < 3 or 0.5 not in quantiles:
            raise ValueError(
                "observation uncertainty requires lower, median, and upper quantiles"
            )
        object.__setattr__(self, "report_quantiles", quantiles)
        admission = _open_unit_float(
            self.admission_quantile, "admission_quantile"
        )
        if admission <= max(quantiles):
            raise ValueError(
                "admission quantile must exceed every reporting quantile"
            )
        object.__setattr__(self, "admission_quantile", admission)
        minimum = _positive_int(
            self.minimum_path_realizations_per_scenario,
            "minimum_path_realizations_per_scenario",
        )
        if minimum > 16:
            raise ValueError("minimum scenario realizations exceed v1 bound")
        object.__setattr__(
            self, "minimum_path_realizations_per_scenario", minimum
        )
        order = tuple(
            ObservationUncertaintyScenarioKind(value)
            for value in self.scenario_order
        )
        if order != SCENARIO_ORDER:
            raise ValueError("observation uncertainty scenario order is frozen")
        object.__setattr__(self, "scenario_order", order)
        fully = _scenario_kind_tuple(self.fully_retained_scenarios)
        aggregate = _scenario_kind_tuple(self.aggregate_only_scenarios)
        if fully != SCENARIO_ORDER or aggregate:
            raise ValueError(
                "v1 release retention requires every observation scenario"
            )
        object.__setattr__(self, "fully_retained_scenarios", fully)
        object.__setattr__(self, "aggregate_only_scenarios", aggregate)
        steps = _positive_int(self.max_quantile_steps, "max_quantile_steps")
        if steps > MAX_OBSERVATION_QUANTILE_STEPS:
            raise ValueError("observation quantile step bound is exceeded")
        object.__setattr__(self, "max_quantile_steps", steps)
        expected = _stable_id(
            "observation-uncertainty-policy", self.identity_payload()
        )
        supplied = _optional_text(self.policy_id)
        if supplied is not None and supplied != expected:
            raise ValueError("observation uncertainty policy_id differs")
        object.__setattr__(self, "policy_id", expected)

    def retention_mode(
        self, kind: ObservationUncertaintyScenarioKind
    ) -> ObservationScenarioRetentionMode:
        """Return the explicit storage mode for one scenario."""
        selected = ObservationUncertaintyScenarioKind(kind)
        if selected in self.fully_retained_scenarios:
            return ObservationScenarioRetentionMode.FULLY_RETAINED
        return ObservationScenarioRetentionMode.AGGREGATE_ONLY

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "scenario_order": [item.value for item in self.scenario_order],
            "endpoint_policy": {
                ObservationUncertaintyScenarioKind.HIGH_RETENTION_LOW_INFILL.value: "qualified_upper_endpoint",
                ObservationUncertaintyScenarioKind.CENTRAL_FITTED_RETENTION.value: "qualified_point_estimate",
                ObservationUncertaintyScenarioKind.LOW_RETENTION_HIGH_INFILL.value: "qualified_lower_endpoint",
            },
            "scenario_derivation_policy": (
                "qualified-interval-endpoints-no-arbitrary-multipliers-v1"
            ),
            "report_quantiles": list(self.report_quantiles),
            "quantile_parameterization": (
                "negative-binomial-failures-before-retained-successes-v1"
            ),
            "admission_quantile": self.admission_quantile,
            "admission_bound": "one-sided-cantelli-upper-bound-v1",
            "admission_policy": "worst-case-low-retention-scenario-v1",
            "member_assignment_policy": "balanced-semantic-round-robin-v1",
            "minimum_path_realizations_per_scenario": (
                self.minimum_path_realizations_per_scenario
            ),
            "fully_retained_scenarios": [
                item.value for item in self.fully_retained_scenarios
            ],
            "aggregate_only_scenarios": [
                item.value for item in self.aggregate_only_scenarios
            ],
            "aggregate_publication_policy": (
                "complete-scenario-aggregates-even-when-event-rows-omitted-v1"
            ),
            "calibration_splits": [
                item.value for item in ObservationUncertaintySplit
            ],
            "diagnostic_metrics": list(OBSERVATION_UNCERTAINTY_METRIC_NAMES),
            "uncertainty_decomposition": (
                "operator-between-scenario-plus-path-within-scenario-v1"
            ),
            "seed_only_dispersion_is_total_uncertainty": False,
            "max_quantile_steps": self.max_quantile_steps,
            "legacy_v2_4_policy": (
                "replayable-point-estimate-products-not-relabeled-v1"
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "policy_id": self.policy_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ObservationUncertaintyPolicyV1:
        _require_schema(
            str(data.get("schema_version", "")),
            OBSERVATION_UNCERTAINTY_POLICY_SCHEMA_VERSION,
        )
        _require_derived(
            data,
            "scenario_derivation_policy",
            "qualified-interval-endpoints-no-arbitrary-multipliers-v1",
        )
        _require_derived(
            data,
            "endpoint_policy",
            {
                ObservationUncertaintyScenarioKind.HIGH_RETENTION_LOW_INFILL.value: "qualified_upper_endpoint",
                ObservationUncertaintyScenarioKind.CENTRAL_FITTED_RETENTION.value: "qualified_point_estimate",
                ObservationUncertaintyScenarioKind.LOW_RETENTION_HIGH_INFILL.value: "qualified_lower_endpoint",
            },
        )
        _require_derived(
            data,
            "quantile_parameterization",
            "negative-binomial-failures-before-retained-successes-v1",
        )
        _require_derived(
            data, "admission_bound", "one-sided-cantelli-upper-bound-v1"
        )
        _require_derived(
            data, "admission_policy", "worst-case-low-retention-scenario-v1"
        )
        _require_derived(
            data, "member_assignment_policy", "balanced-semantic-round-robin-v1"
        )
        _require_derived(
            data,
            "aggregate_publication_policy",
            "complete-scenario-aggregates-even-when-event-rows-omitted-v1",
        )
        _require_derived(
            data,
            "calibration_splits",
            [item.value for item in ObservationUncertaintySplit],
        )
        _require_derived(
            data,
            "diagnostic_metrics",
            list(OBSERVATION_UNCERTAINTY_METRIC_NAMES),
        )
        _require_derived(
            data,
            "uncertainty_decomposition",
            "operator-between-scenario-plus-path-within-scenario-v1",
        )
        _require_derived(
            data, "seed_only_dispersion_is_total_uncertainty", False
        )
        _require_derived(
            data,
            "legacy_v2_4_policy",
            "replayable-point-estimate-products-not-relabeled-v1",
        )
        return cls(
            report_quantiles=tuple(
                _finite_float(value, "report_quantile")
                for value in _sequence(
                    data.get("report_quantiles"), "report_quantiles"
                )
            ),
            admission_quantile=_finite_float(
                data.get("admission_quantile"), "admission_quantile"
            ),
            minimum_path_realizations_per_scenario=_strict_int(
                data.get("minimum_path_realizations_per_scenario"),
                "minimum_path_realizations_per_scenario",
            ),
            scenario_order=tuple(
                ObservationUncertaintyScenarioKind(str(value))
                for value in _sequence(
                    data.get("scenario_order"), "scenario_order"
                )
            ),
            fully_retained_scenarios=tuple(
                ObservationUncertaintyScenarioKind(str(value))
                for value in _sequence(
                    data.get("fully_retained_scenarios"),
                    "fully_retained_scenarios",
                )
            ),
            aggregate_only_scenarios=tuple(
                ObservationUncertaintyScenarioKind(str(value))
                for value in _sequence(
                    data.get("aggregate_only_scenarios"),
                    "aggregate_only_scenarios",
                )
            ),
            max_quantile_steps=_strict_int(
                data.get("max_quantile_steps"), "max_quantile_steps"
            ),
            policy_id=str(data.get("policy_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> ObservationUncertaintyPolicyV1:
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class ObservationUncertaintyScenarioV1:
    """One evidence-bound retention endpoint with semantic identity."""

    kind: ObservationUncertaintyScenarioKind
    policy_id: str
    report_quantiles: tuple[float, ...]
    admission_quantile: float
    observation_operator_id: str
    conditioning_id: str
    feed_epoch_id: str
    stratum_id: str
    stratum_key: str
    stratum_level: str
    central_retention_probability: float
    lower_retention_probability: float
    upper_retention_probability: float
    retention_probability: float
    support_count: int
    evidence_ids: tuple[str, ...]
    estimation_bases: tuple[str, ...]
    provenance: tuple[str, ...]
    endpoint_policy: str
    scenario_id: str = ""
    schema_version: str = OBSERVATION_UNCERTAINTY_SCENARIO_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, OBSERVATION_UNCERTAINTY_SCENARIO_SCHEMA_VERSION
        )
        kind = ObservationUncertaintyScenarioKind(self.kind)
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "policy_id", _required_text(self.policy_id))
        quantiles = tuple(
            sorted(
                _open_unit_float(value, "report_quantile")
                for value in self.report_quantiles
            )
        )
        if len(quantiles) < 3 or 0.5 not in quantiles:
            raise ValueError("scenario quantile policy is incomplete")
        admission_quantile = _open_unit_float(
            self.admission_quantile, "admission_quantile"
        )
        if admission_quantile <= max(quantiles):
            raise ValueError("scenario admission quantile is not conservative")
        object.__setattr__(self, "report_quantiles", quantiles)
        object.__setattr__(self, "admission_quantile", admission_quantile)
        for name in (
            "observation_operator_id",
            "conditioning_id",
            "feed_epoch_id",
            "stratum_id",
            "stratum_key",
            "stratum_level",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        central = _retention_probability(
            self.central_retention_probability,
            "central_retention_probability",
        )
        lower = _retention_probability(
            self.lower_retention_probability,
            "lower_retention_probability",
        )
        upper = _retention_probability(
            self.upper_retention_probability,
            "upper_retention_probability",
        )
        if not lower <= central <= upper:
            raise ValueError("qualified retention interval is not ordered")
        selected = {
            ObservationUncertaintyScenarioKind.HIGH_RETENTION_LOW_INFILL: upper,
            ObservationUncertaintyScenarioKind.CENTRAL_FITTED_RETENTION: central,
            ObservationUncertaintyScenarioKind.LOW_RETENTION_HIGH_INFILL: lower,
        }[kind]
        endpoint = {
            ObservationUncertaintyScenarioKind.HIGH_RETENTION_LOW_INFILL: "qualified_upper_endpoint",
            ObservationUncertaintyScenarioKind.CENTRAL_FITTED_RETENTION: "qualified_point_estimate",
            ObservationUncertaintyScenarioKind.LOW_RETENTION_HIGH_INFILL: "qualified_lower_endpoint",
        }[kind]
        if (
            _retention_probability(
                self.retention_probability, "retention_probability"
            )
            != selected
        ):
            raise ValueError("scenario retention differs from endpoint policy")
        if self.endpoint_policy != endpoint:
            raise ValueError("scenario endpoint policy differs")
        object.__setattr__(self, "central_retention_probability", central)
        object.__setattr__(self, "lower_retention_probability", lower)
        object.__setattr__(self, "upper_retention_probability", upper)
        object.__setattr__(self, "retention_probability", selected)
        object.__setattr__(
            self,
            "support_count",
            _positive_int(self.support_count, "support_count"),
        )
        for name in ("evidence_ids", "estimation_bases", "provenance"):
            values = _text_tuple(getattr(self, name), name)
            if not values:
                raise ValueError(f"scenario {name} is empty")
            object.__setattr__(self, name, values)
        expected = _stable_id("observation-scenario", self.identity_payload())
        supplied = _optional_text(self.scenario_id)
        if supplied is not None and supplied != expected:
            raise ValueError("observation scenario_id differs")
        object.__setattr__(self, "scenario_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "kind": self.kind.value,
            "observation_uncertainty_policy_id": self.policy_id,
            "report_quantiles": list(self.report_quantiles),
            "admission_quantile": self.admission_quantile,
            "quantile_parameterization": (
                "negative-binomial-failures-before-retained-successes-v1"
            ),
            "admission_bound_method": "one-sided-cantelli-upper-bound-v1",
            "observation_operator_id": self.observation_operator_id,
            "conditioning_id": self.conditioning_id,
            "feed_epoch_id": self.feed_epoch_id,
            "stratum_id": self.stratum_id,
            "stratum_key": self.stratum_key,
            "stratum_level": self.stratum_level,
            "central_retention_probability": self.central_retention_probability,
            "lower_retention_probability": self.lower_retention_probability,
            "upper_retention_probability": self.upper_retention_probability,
            "retention_probability": self.retention_probability,
            "endpoint_policy": self.endpoint_policy,
            "support_count": self.support_count,
            "evidence_ids": list(self.evidence_ids),
            "estimation_bases": list(self.estimation_bases),
            "provenance": list(self.provenance),
            "uncertainty_availability": ObservationUncertaintyAvailability.TWO_SIDED.value,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "scenario_id": self.scenario_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ObservationUncertaintyScenarioV1:
        _require_schema(
            str(data.get("schema_version", "")),
            OBSERVATION_UNCERTAINTY_SCENARIO_SCHEMA_VERSION,
        )
        _require_derived(
            data,
            "uncertainty_availability",
            ObservationUncertaintyAvailability.TWO_SIDED.value,
        )
        _require_derived(
            data,
            "quantile_parameterization",
            "negative-binomial-failures-before-retained-successes-v1",
        )
        _require_derived(
            data,
            "admission_bound_method",
            "one-sided-cantelli-upper-bound-v1",
        )
        return cls(
            kind=ObservationUncertaintyScenarioKind(str(data.get("kind", ""))),
            policy_id=str(data.get("observation_uncertainty_policy_id", "")),
            report_quantiles=tuple(
                _finite_float(value, "report_quantile")
                for value in _sequence(
                    data.get("report_quantiles"), "report_quantiles"
                )
            ),
            admission_quantile=_finite_float(
                data.get("admission_quantile"), "admission_quantile"
            ),
            observation_operator_id=str(
                data.get("observation_operator_id", "")
            ),
            conditioning_id=str(data.get("conditioning_id", "")),
            feed_epoch_id=str(data.get("feed_epoch_id", "")),
            stratum_id=str(data.get("stratum_id", "")),
            stratum_key=str(data.get("stratum_key", "")),
            stratum_level=str(data.get("stratum_level", "")),
            central_retention_probability=_finite_float(
                data.get("central_retention_probability"),
                "central_retention_probability",
            ),
            lower_retention_probability=_finite_float(
                data.get("lower_retention_probability"),
                "lower_retention_probability",
            ),
            upper_retention_probability=_finite_float(
                data.get("upper_retention_probability"),
                "upper_retention_probability",
            ),
            retention_probability=_finite_float(
                data.get("retention_probability"), "retention_probability"
            ),
            support_count=_strict_int(
                data.get("support_count"), "support_count"
            ),
            evidence_ids=_text_tuple(
                _sequence(data.get("evidence_ids"), "evidence_ids"),
                "evidence_ids",
            ),
            estimation_bases=_text_tuple(
                _sequence(data.get("estimation_bases"), "estimation_bases"),
                "estimation_bases",
            ),
            provenance=_text_tuple(
                _sequence(data.get("provenance"), "provenance"), "provenance"
            ),
            endpoint_policy=str(data.get("endpoint_policy", "")),
            scenario_id=str(data.get("scenario_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ObservationCardinalityEvidenceV1:
    """Bounded missing-cardinality and resource evidence for one cell."""

    scenario_id: str
    symbol: str
    feed_epoch_id: str
    session: str
    observed_retained_count: int
    retention_probability: float
    missing_count_mean: float
    missing_count_variance: float
    missing_count_quantiles: Mapping[str, int]
    total_event_count_quantiles: Mapping[str, int]
    admission_missing_count_bound: int
    maximum_missing_event_count: int
    candidate_amplification_bound: float
    maximum_candidate_amplification: float
    limit_exceedance_probability_bound: float
    admitted: bool
    refusal_risk: str
    evidence_id: str = ""
    schema_version: str = OBSERVATION_CARDINALITY_EVIDENCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, OBSERVATION_CARDINALITY_EVIDENCE_SCHEMA_VERSION
        )
        for name in ("scenario_id", "symbol", "feed_epoch_id", "session"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        observed = _nonnegative_int(
            self.observed_retained_count, "observed_retained_count"
        )
        probability = _retention_probability(
            self.retention_probability, "retention_probability"
        )
        mean, variance = negative_binomial_failure_moments(
            observed, probability
        )
        if not math.isclose(
            self.missing_count_mean, mean, rel_tol=1e-12, abs_tol=1e-12
        ):
            raise ValueError("missing-count mean differs from verified formula")
        if not math.isclose(
            self.missing_count_variance,
            variance,
            rel_tol=1e-12,
            abs_tol=1e-12,
        ):
            raise ValueError(
                "missing-count variance differs from verified formula"
            )
        missing = _count_mapping(
            self.missing_count_quantiles, "missing quantile"
        )
        totals = _count_mapping(
            self.total_event_count_quantiles, "total quantile"
        )
        if not missing or set(missing) != set(totals):
            raise ValueError("cardinality quantile keys differ")
        if list(missing.values()) != sorted(missing.values()):
            raise ValueError("missing-count quantiles are not monotone")
        if any(
            totals[key] != observed + value for key, value in missing.items()
        ):
            raise ValueError("total-event quantiles do not reconcile")
        admission = _nonnegative_int(
            self.admission_missing_count_bound,
            "admission_missing_count_bound",
        )
        maximum = _nonnegative_int(
            self.maximum_missing_event_count, "maximum_missing_event_count"
        )
        amplification = _nonnegative_float(
            self.candidate_amplification_bound,
            "candidate_amplification_bound",
        )
        max_amplification = _positive_float(
            self.maximum_candidate_amplification,
            "maximum_candidate_amplification",
        )
        expected_amplification = 0.0 if observed == 0 else admission / observed
        if not math.isclose(
            amplification, expected_amplification, rel_tol=1e-12, abs_tol=1e-12
        ):
            raise ValueError("candidate amplification bound differs")
        risk = _unit_float(
            self.limit_exceedance_probability_bound,
            "limit_exceedance_probability_bound",
        )
        expected_admitted = (
            admission <= maximum and amplification <= max_amplification
        )
        if _strict_bool(self.admitted, "admitted") != expected_admitted:
            raise ValueError("scenario admission decision differs")
        expected_risk = _refusal_risk(risk, expected_admitted)
        if self.refusal_risk != expected_risk:
            raise ValueError("scenario refusal risk differs")
        object.__setattr__(self, "observed_retained_count", observed)
        object.__setattr__(self, "retention_probability", probability)
        object.__setattr__(self, "missing_count_mean", mean)
        object.__setattr__(self, "missing_count_variance", variance)
        object.__setattr__(self, "missing_count_quantiles", missing)
        object.__setattr__(self, "total_event_count_quantiles", totals)
        object.__setattr__(self, "admission_missing_count_bound", admission)
        object.__setattr__(self, "maximum_missing_event_count", maximum)
        object.__setattr__(self, "candidate_amplification_bound", amplification)
        object.__setattr__(
            self, "maximum_candidate_amplification", max_amplification
        )
        object.__setattr__(self, "limit_exceedance_probability_bound", risk)
        expected = _stable_id("observation-cardinality", self.payload())
        supplied = _optional_text(self.evidence_id)
        if supplied is not None and supplied != expected:
            raise ValueError("observation cardinality evidence_id differs")
        object.__setattr__(self, "evidence_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "scenario_id": self.scenario_id,
            "symbol": self.symbol,
            "feed_epoch_id": self.feed_epoch_id,
            "session": self.session,
            "observed_retained_count": self.observed_retained_count,
            "retention_probability": self.retention_probability,
            "negative_binomial_parameterization": "failures-before-retained-successes-v1",
            "missing_count_mean": self.missing_count_mean,
            "missing_count_variance": self.missing_count_variance,
            "missing_count_quantiles": dict(self.missing_count_quantiles),
            "total_event_count_quantiles": dict(
                self.total_event_count_quantiles
            ),
            "admission_missing_count_bound": self.admission_missing_count_bound,
            "admission_bound_method": "one-sided-cantelli-upper-bound-v1",
            "maximum_missing_event_count": self.maximum_missing_event_count,
            "candidate_amplification_bound": self.candidate_amplification_bound,
            "maximum_candidate_amplification": self.maximum_candidate_amplification,
            "limit_exceedance_probability_bound": self.limit_exceedance_probability_bound,
            "admitted": self.admitted,
            "refusal_risk": self.refusal_risk,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "evidence_id": self.evidence_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ObservationCardinalityEvidenceV1:
        _require_schema(
            str(data.get("schema_version", "")),
            OBSERVATION_CARDINALITY_EVIDENCE_SCHEMA_VERSION,
        )
        _require_derived(
            data,
            "negative_binomial_parameterization",
            "failures-before-retained-successes-v1",
        )
        _require_derived(
            data,
            "admission_bound_method",
            "one-sided-cantelli-upper-bound-v1",
        )
        return cls(
            scenario_id=str(data.get("scenario_id", "")),
            symbol=str(data.get("symbol", "")),
            feed_epoch_id=str(data.get("feed_epoch_id", "")),
            session=str(data.get("session", "")),
            observed_retained_count=_strict_int(
                data.get("observed_retained_count"), "observed_retained_count"
            ),
            retention_probability=_finite_float(
                data.get("retention_probability"), "retention_probability"
            ),
            missing_count_mean=_finite_float(
                data.get("missing_count_mean"), "missing_count_mean"
            ),
            missing_count_variance=_finite_float(
                data.get("missing_count_variance"), "missing_count_variance"
            ),
            missing_count_quantiles=_int_mapping(
                data.get("missing_count_quantiles"), "missing_count_quantiles"
            ),
            total_event_count_quantiles=_int_mapping(
                data.get("total_event_count_quantiles"),
                "total_event_count_quantiles",
            ),
            admission_missing_count_bound=_strict_int(
                data.get("admission_missing_count_bound"),
                "admission_missing_count_bound",
            ),
            maximum_missing_event_count=_strict_int(
                data.get("maximum_missing_event_count"),
                "maximum_missing_event_count",
            ),
            candidate_amplification_bound=_finite_float(
                data.get("candidate_amplification_bound"),
                "candidate_amplification_bound",
            ),
            maximum_candidate_amplification=_finite_float(
                data.get("maximum_candidate_amplification"),
                "maximum_candidate_amplification",
            ),
            limit_exceedance_probability_bound=_finite_float(
                data.get("limit_exceedance_probability_bound"),
                "limit_exceedance_probability_bound",
            ),
            admitted=_strict_bool(data.get("admitted"), "admitted"),
            refusal_risk=str(data.get("refusal_risk", "")),
            evidence_id=str(data.get("evidence_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ObservationUncertaintyMemberV1:
    """One path seed assigned to one separate observation scenario."""

    ordinal: int
    ensemble_member_id: str
    path_seed: int
    scenario_id: str
    scenario_kind: ObservationUncertaintyScenarioKind
    retention_mode: ObservationScenarioRetentionMode
    member_id: str = ""
    schema_version: str = OBSERVATION_UNCERTAINTY_MEMBER_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, OBSERVATION_UNCERTAINTY_MEMBER_SCHEMA_VERSION
        )
        object.__setattr__(
            self, "ordinal", _positive_int(self.ordinal, "ordinal")
        )
        object.__setattr__(
            self, "ensemble_member_id", _required_text(self.ensemble_member_id)
        )
        seed = _nonnegative_int(self.path_seed, "path_seed")
        if seed > (1 << 64) - 1:
            raise ValueError("observation path seed exceeds uint64")
        object.__setattr__(self, "path_seed", seed)
        object.__setattr__(
            self, "scenario_id", _required_text(self.scenario_id)
        )
        object.__setattr__(
            self,
            "scenario_kind",
            ObservationUncertaintyScenarioKind(self.scenario_kind),
        )
        object.__setattr__(
            self,
            "retention_mode",
            ObservationScenarioRetentionMode(self.retention_mode),
        )
        expected = _stable_id(
            "observation-uncertainty-member", self.identity_payload()
        )
        supplied = _optional_text(self.member_id)
        if supplied is not None and supplied != expected:
            raise ValueError("observation uncertainty member_id differs")
        object.__setattr__(self, "member_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "ordinal": self.ordinal,
            "ensemble_member_id": self.ensemble_member_id,
            "path_seed": self.path_seed,
            "scenario_id": self.scenario_id,
            "scenario_kind": self.scenario_kind.value,
            "retention_mode": self.retention_mode.value,
            "semantic_axes": ["observation_scenario", "path_seed"],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "member_id": self.member_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ObservationUncertaintyMemberV1:
        _require_schema(
            str(data.get("schema_version", "")),
            OBSERVATION_UNCERTAINTY_MEMBER_SCHEMA_VERSION,
        )
        _require_derived(
            data, "semantic_axes", ["observation_scenario", "path_seed"]
        )
        return cls(
            ordinal=_strict_int(data.get("ordinal"), "ordinal"),
            ensemble_member_id=str(data.get("ensemble_member_id", "")),
            path_seed=_strict_int(data.get("path_seed"), "path_seed"),
            scenario_id=str(data.get("scenario_id", "")),
            scenario_kind=ObservationUncertaintyScenarioKind(
                str(data.get("scenario_kind", ""))
            ),
            retention_mode=ObservationScenarioRetentionMode(
                str(data.get("retention_mode", ""))
            ),
            member_id=str(data.get("member_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ObservationUncertaintyEnsembleV1:
    """One window/stratum scenario × path plan with bounded evidence."""

    policy: ObservationUncertaintyPolicyV1
    conditioning_id: str
    observation_operator_id: str
    feed_epoch_id: str
    session: str
    scenarios: tuple[ObservationUncertaintyScenarioV1, ...]
    members: tuple[ObservationUncertaintyMemberV1, ...]
    cardinality_evidence: tuple[ObservationCardinalityEvidenceV1, ...]
    admitted: bool
    refusal_reasons: tuple[str, ...]
    ensemble_id: str = ""
    schema_version: str = OBSERVATION_UNCERTAINTY_ENSEMBLE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, OBSERVATION_UNCERTAINTY_ENSEMBLE_SCHEMA_VERSION
        )
        if not isinstance(self.policy, ObservationUncertaintyPolicyV1):
            raise TypeError("observation ensemble requires a v1 policy")
        for name in (
            "conditioning_id",
            "observation_operator_id",
            "feed_epoch_id",
            "session",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        scenarios = tuple(
            sorted(
                self.scenarios, key=lambda item: SCENARIO_ORDER.index(item.kind)
            )
        )
        if tuple(item.kind for item in scenarios) != SCENARIO_ORDER:
            raise ValueError(
                "observation ensemble scenario coverage is incomplete"
            )
        if len({item.scenario_id for item in scenarios}) != len(scenarios):
            raise ValueError("observation ensemble scenarios are duplicated")
        if any(
            item.policy_id != self.policy.policy_id
            or item.report_quantiles != self.policy.report_quantiles
            or item.admission_quantile != self.policy.admission_quantile
            or item.conditioning_id != self.conditioning_id
            or item.observation_operator_id != self.observation_operator_id
            or item.feed_epoch_id != self.feed_epoch_id
            for item in scenarios
        ):
            raise ValueError("observation ensemble scenario scope differs")
        members = tuple(sorted(self.members, key=lambda item: item.ordinal))
        if not 3 <= len(members) <= MAX_OBSERVATION_SCENARIO_MEMBERS:
            raise ValueError("observation ensemble member count is invalid")
        if tuple(item.ordinal for item in members) != tuple(
            range(1, len(members) + 1)
        ):
            raise ValueError(
                "observation ensemble member ordinals are incomplete"
            )
        if len({item.ensemble_member_id for item in members}) != len(members):
            raise ValueError("observation ensemble members are duplicated")
        scenario_by_id = {item.scenario_id: item for item in scenarios}
        counts = {kind: 0 for kind in SCENARIO_ORDER}
        for member in members:
            scenario = scenario_by_id.get(member.scenario_id)
            if scenario is None or scenario.kind is not member.scenario_kind:
                raise ValueError("observation member scenario binding differs")
            if member.retention_mode is not self.policy.retention_mode(
                scenario.kind
            ):
                raise ValueError("observation member retention mode differs")
            counts[scenario.kind] += 1
        if any(
            count < self.policy.minimum_path_realizations_per_scenario
            for count in counts.values()
        ):
            raise ValueError(
                "observation scenario lacks required path realizations"
            )
        evidence = tuple(
            sorted(
                self.cardinality_evidence,
                key=lambda item: (item.symbol, item.scenario_id),
            )
        )
        if not evidence or len(evidence) > MAX_OBSERVATION_SCENARIO_CELLS:
            raise ValueError(
                "observation cardinality evidence is empty or oversized"
            )
        symbols = {item.symbol for item in evidence}
        expected_cells = {
            (symbol, scenario.scenario_id)
            for symbol in symbols
            for scenario in scenarios
        }
        if {
            (item.symbol, item.scenario_id) for item in evidence
        } != expected_cells:
            raise ValueError(
                "observation cardinality evidence cells are incomplete"
            )
        if any(
            item.feed_epoch_id != self.feed_epoch_id
            or item.session != self.session
            for item in evidence
        ):
            raise ValueError("observation cardinality evidence scope differs")
        expected_admitted = all(item.admitted for item in evidence)
        if _strict_bool(self.admitted, "admitted") != expected_admitted:
            raise ValueError("observation ensemble admission decision differs")
        reasons = _text_tuple(
            self.refusal_reasons, "refusal_reasons", allow_empty=True
        )
        expected_reasons = tuple(
            sorted(
                f"{item.symbol}:{item.scenario_id}:{item.refusal_risk}"
                for item in evidence
                if not item.admitted
            )
        )
        if reasons != expected_reasons:
            raise ValueError("observation ensemble refusal reasons differ")
        object.__setattr__(self, "scenarios", scenarios)
        object.__setattr__(self, "members", members)
        object.__setattr__(self, "cardinality_evidence", evidence)
        object.__setattr__(self, "refusal_reasons", reasons)
        expected = _stable_id(
            "observation-uncertainty-ensemble", self.identity_payload()
        )
        supplied = _optional_text(self.ensemble_id)
        if supplied is not None and supplied != expected:
            raise ValueError("observation uncertainty ensemble_id differs")
        object.__setattr__(self, "ensemble_id", expected)

    def member_for(
        self, ensemble_member_id: str
    ) -> ObservationUncertaintyMemberV1:
        """Resolve one existing path member to its operator scenario."""
        wanted = _required_text(ensemble_member_id)
        for item in self.members:
            if item.ensemble_member_id == wanted:
                return item
        raise KeyError(wanted)

    def scenario_for(
        self, ensemble_member_id: str
    ) -> ObservationUncertaintyScenarioV1:
        """Resolve the selected scenario while keeping path identity separate."""
        member = self.member_for(ensemble_member_id)
        return next(
            item
            for item in self.scenarios
            if item.scenario_id == member.scenario_id
        )

    @property
    def worst_case_scenario(self) -> ObservationUncertaintyScenarioV1:
        return next(
            item
            for item in self.scenarios
            if item.kind
            is ObservationUncertaintyScenarioKind.LOW_RETENTION_HIGH_INFILL
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "policy": self.policy.to_dict(),
            "conditioning_id": self.conditioning_id,
            "observation_operator_id": self.observation_operator_id,
            "feed_epoch_id": self.feed_epoch_id,
            "session": self.session,
            "uncertainty_availability": ObservationUncertaintyAvailability.TWO_SIDED.value,
            "scenarios": [item.to_dict() for item in self.scenarios],
            "members": [item.to_dict() for item in self.members],
            "cardinality_evidence": [
                item.to_dict() for item in self.cardinality_evidence
            ],
            "admission_scenario_kind": ObservationUncertaintyScenarioKind.LOW_RETENTION_HIGH_INFILL.value,
            "admitted": self.admitted,
            "refusal_reasons": list(self.refusal_reasons),
            "operator_scenario_and_path_seed_are_separate": True,
            "seed_only_dispersion_is_total_uncertainty": False,
            "event_rows_inline": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "ensemble_id": self.ensemble_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ObservationUncertaintyEnsembleV1:
        _require_schema(
            str(data.get("schema_version", "")),
            OBSERVATION_UNCERTAINTY_ENSEMBLE_SCHEMA_VERSION,
        )
        _require_derived(
            data, "operator_scenario_and_path_seed_are_separate", True
        )
        _require_derived(
            data, "seed_only_dispersion_is_total_uncertainty", False
        )
        _require_derived(data, "event_rows_inline", False)
        return cls(
            policy=ObservationUncertaintyPolicyV1.from_dict(
                _mapping(data.get("policy"), "policy")
            ),
            conditioning_id=str(data.get("conditioning_id", "")),
            observation_operator_id=str(
                data.get("observation_operator_id", "")
            ),
            feed_epoch_id=str(data.get("feed_epoch_id", "")),
            session=str(data.get("session", "")),
            scenarios=tuple(
                ObservationUncertaintyScenarioV1.from_dict(item)
                for item in _mapping_sequence(
                    data.get("scenarios"), "scenarios"
                )
            ),
            members=tuple(
                ObservationUncertaintyMemberV1.from_dict(item)
                for item in _mapping_sequence(data.get("members"), "members")
            ),
            cardinality_evidence=tuple(
                ObservationCardinalityEvidenceV1.from_dict(item)
                for item in _mapping_sequence(
                    data.get("cardinality_evidence"), "cardinality_evidence"
                )
            ),
            admitted=_strict_bool(data.get("admitted"), "admitted"),
            refusal_reasons=_text_tuple(
                _sequence(data.get("refusal_reasons"), "refusal_reasons"),
                "refusal_reasons",
                allow_empty=True,
            ),
            ensemble_id=str(data.get("ensemble_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> ObservationUncertaintyEnsembleV1:
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class ObservationUncertaintyDiagnosticV1:
    """One row-free scenario/path diagnostic cell."""

    split: ObservationUncertaintySplit
    scenario_id: str
    ensemble_member_id: str
    path_seed: int
    status: ObservationGenerationStatus
    metrics: Mapping[str, float] = field(default_factory=dict)
    refusal_reason: str | None = None
    diagnostic_id: str = ""
    schema_version: str = OBSERVATION_UNCERTAINTY_DIAGNOSTIC_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            OBSERVATION_UNCERTAINTY_DIAGNOSTIC_SCHEMA_VERSION,
        )
        object.__setattr__(
            self, "split", ObservationUncertaintySplit(self.split)
        )
        object.__setattr__(
            self, "scenario_id", _required_text(self.scenario_id)
        )
        object.__setattr__(
            self, "ensemble_member_id", _required_text(self.ensemble_member_id)
        )
        object.__setattr__(
            self, "path_seed", _nonnegative_int(self.path_seed, "path_seed")
        )
        status = ObservationGenerationStatus(self.status)
        object.__setattr__(self, "status", status)
        metrics = {
            _required_text(name): _finite_float(value, str(name))
            for name, value in self.metrics.items()
        }
        reason = _optional_text(self.refusal_reason)
        if status is ObservationGenerationStatus.COMPLETED:
            if set(metrics) != set(OBSERVATION_UNCERTAINTY_METRIC_NAMES):
                raise ValueError(
                    "completed observation diagnostic metrics are incomplete"
                )
            if reason is not None:
                raise ValueError(
                    "completed observation diagnostic has refusal reason"
                )
        else:
            if metrics:
                raise ValueError(
                    "non-completed observation diagnostic has metrics"
                )
            if reason is None:
                raise ValueError(
                    "non-completed observation diagnostic lacks reason"
                )
        object.__setattr__(self, "metrics", dict(sorted(metrics.items())))
        object.__setattr__(self, "refusal_reason", reason)
        expected = _stable_id(
            "observation-uncertainty-diagnostic", self.payload()
        )
        supplied = _optional_text(self.diagnostic_id)
        if supplied is not None and supplied != expected:
            raise ValueError("observation uncertainty diagnostic_id differs")
        object.__setattr__(self, "diagnostic_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "split": self.split.value,
            "scenario_id": self.scenario_id,
            "ensemble_member_id": self.ensemble_member_id,
            "path_seed": self.path_seed,
            "status": self.status.value,
            "metrics": dict(self.metrics),
            "refusal_reason": self.refusal_reason,
            "event_rows_inline": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "diagnostic_id": self.diagnostic_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ObservationUncertaintyDiagnosticV1:
        _require_schema(
            str(data.get("schema_version", "")),
            OBSERVATION_UNCERTAINTY_DIAGNOSTIC_SCHEMA_VERSION,
        )
        _require_derived(data, "event_rows_inline", False)
        return cls(
            split=ObservationUncertaintySplit(str(data.get("split", ""))),
            scenario_id=str(data.get("scenario_id", "")),
            ensemble_member_id=str(data.get("ensemble_member_id", "")),
            path_seed=_strict_int(data.get("path_seed"), "path_seed"),
            status=ObservationGenerationStatus(str(data.get("status", ""))),
            metrics=_float_mapping(data.get("metrics"), "metrics"),
            refusal_reason=(
                str(data["refusal_reason"])
                if data.get("refusal_reason") is not None
                else None
            ),
            diagnostic_id=str(data.get("diagnostic_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ObservationUncertaintyDecompositionV1:
    """Operator-versus-path dispersion for one metric and split."""

    split: ObservationUncertaintySplit
    metric_name: str
    completed_cell_count: int
    operator_between_scenario_variance: float
    path_within_scenario_variance: float
    total_variance: float
    operator_variance_fraction: float
    decomposition_id: str = ""
    schema_version: str = OBSERVATION_UNCERTAINTY_DECOMPOSITION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            OBSERVATION_UNCERTAINTY_DECOMPOSITION_SCHEMA_VERSION,
        )
        object.__setattr__(
            self, "split", ObservationUncertaintySplit(self.split)
        )
        metric = _required_text(self.metric_name)
        if metric not in OBSERVATION_UNCERTAINTY_METRIC_NAMES:
            raise ValueError("unknown observation uncertainty metric")
        object.__setattr__(self, "metric_name", metric)
        object.__setattr__(
            self,
            "completed_cell_count",
            _positive_int(self.completed_cell_count, "completed_cell_count"),
        )
        between = _nonnegative_float(
            self.operator_between_scenario_variance,
            "operator_between_scenario_variance",
        )
        within = _nonnegative_float(
            self.path_within_scenario_variance,
            "path_within_scenario_variance",
        )
        total = _nonnegative_float(self.total_variance, "total_variance")
        if not math.isclose(
            total, between + within, rel_tol=1e-12, abs_tol=1e-12
        ):
            raise ValueError(
                "observation uncertainty variance does not decompose"
            )
        fraction = _unit_float(
            self.operator_variance_fraction, "operator_variance_fraction"
        )
        expected_fraction = 0.0 if total == 0.0 else between / total
        if not math.isclose(
            fraction, expected_fraction, rel_tol=1e-12, abs_tol=1e-12
        ):
            raise ValueError("observation operator variance fraction differs")
        object.__setattr__(self, "operator_between_scenario_variance", between)
        object.__setattr__(self, "path_within_scenario_variance", within)
        object.__setattr__(self, "total_variance", total)
        object.__setattr__(self, "operator_variance_fraction", fraction)
        expected = _stable_id(
            "observation-uncertainty-decomposition", self.payload()
        )
        supplied = _optional_text(self.decomposition_id)
        if supplied is not None and supplied != expected:
            raise ValueError("observation uncertainty decomposition_id differs")
        object.__setattr__(self, "decomposition_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "split": self.split.value,
            "metric_name": self.metric_name,
            "completed_cell_count": self.completed_cell_count,
            "operator_between_scenario_variance": self.operator_between_scenario_variance,
            "path_within_scenario_variance": self.path_within_scenario_variance,
            "total_variance": self.total_variance,
            "operator_variance_fraction": self.operator_variance_fraction,
            "seed_only_dispersion_is_total_uncertainty": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "decomposition_id": self.decomposition_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ObservationUncertaintyDecompositionV1:
        _require_schema(
            str(data.get("schema_version", "")),
            OBSERVATION_UNCERTAINTY_DECOMPOSITION_SCHEMA_VERSION,
        )
        _require_derived(
            data, "seed_only_dispersion_is_total_uncertainty", False
        )
        return cls(
            split=ObservationUncertaintySplit(str(data.get("split", ""))),
            metric_name=str(data.get("metric_name", "")),
            completed_cell_count=_strict_int(
                data.get("completed_cell_count"), "completed_cell_count"
            ),
            operator_between_scenario_variance=_finite_float(
                data.get("operator_between_scenario_variance"),
                "operator_between_scenario_variance",
            ),
            path_within_scenario_variance=_finite_float(
                data.get("path_within_scenario_variance"),
                "path_within_scenario_variance",
            ),
            total_variance=_finite_float(
                data.get("total_variance"), "total_variance"
            ),
            operator_variance_fraction=_finite_float(
                data.get("operator_variance_fraction"),
                "operator_variance_fraction",
            ),
            decomposition_id=str(data.get("decomposition_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ObservationUncertaintyReportV1:
    """Validation and untouched-holdout uncertainty propagation evidence."""

    ensemble_id: str
    policy_id: str
    scenario_ids: tuple[str, ...]
    diagnostics: tuple[ObservationUncertaintyDiagnosticV1, ...]
    decompositions: tuple[ObservationUncertaintyDecompositionV1, ...]
    untouched_release_holdout: bool = True
    holdout_selection_role: bool = False
    report_id: str = ""
    schema_version: str = OBSERVATION_UNCERTAINTY_REPORT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, OBSERVATION_UNCERTAINTY_REPORT_SCHEMA_VERSION
        )
        object.__setattr__(
            self, "ensemble_id", _required_text(self.ensemble_id)
        )
        object.__setattr__(self, "policy_id", _required_text(self.policy_id))
        scenario_ids = _text_tuple(self.scenario_ids, "scenario_ids")
        if len(scenario_ids) != len(SCENARIO_ORDER):
            raise ValueError("uncertainty report scenario count differs")
        object.__setattr__(self, "scenario_ids", scenario_ids)
        diagnostics = tuple(
            sorted(
                self.diagnostics,
                key=lambda item: (
                    item.split.value,
                    item.scenario_id,
                    item.ensemble_member_id,
                ),
            )
        )
        if not diagnostics or len(diagnostics) > MAX_OBSERVATION_SCENARIO_CELLS:
            raise ValueError("uncertainty diagnostics are empty or oversized")
        if any(item.scenario_id not in scenario_ids for item in diagnostics):
            raise ValueError("uncertainty diagnostic scenario is unknown")
        completed_pairs = {
            (item.split, item.scenario_id)
            for item in diagnostics
            if item.status is ObservationGenerationStatus.COMPLETED
        }
        required_pairs = {
            (split, scenario_id)
            for split in ObservationUncertaintySplit
            for scenario_id in scenario_ids
        }
        if completed_pairs != required_pairs:
            raise ValueError(
                "every observation scenario needs validation and holdout evidence"
            )
        if not _strict_bool(
            self.untouched_release_holdout, "untouched_release_holdout"
        ):
            raise ValueError(
                "observation uncertainty requires an untouched holdout"
            )
        if _strict_bool(self.holdout_selection_role, "holdout_selection_role"):
            raise ValueError(
                "release holdout cannot select observation scenarios"
            )
        decompositions = tuple(
            sorted(
                self.decompositions,
                key=lambda item: (item.split.value, item.metric_name),
            )
        )
        expected_keys = {
            (split, metric)
            for split in ObservationUncertaintySplit
            for metric in OBSERVATION_UNCERTAINTY_METRIC_NAMES
        }
        if {
            (item.split, item.metric_name) for item in decompositions
        } != expected_keys:
            raise ValueError(
                "observation uncertainty decompositions are incomplete"
            )
        object.__setattr__(self, "diagnostics", diagnostics)
        object.__setattr__(self, "decompositions", decompositions)
        expected = _stable_id("observation-uncertainty-report", self.payload())
        supplied = _optional_text(self.report_id)
        if supplied is not None and supplied != expected:
            raise ValueError("observation uncertainty report_id differs")
        object.__setattr__(self, "report_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "ensemble_id": self.ensemble_id,
            "policy_id": self.policy_id,
            "scenario_ids": list(self.scenario_ids),
            "diagnostics": [item.to_dict() for item in self.diagnostics],
            "decompositions": [item.to_dict() for item in self.decompositions],
            "untouched_release_holdout": self.untouched_release_holdout,
            "holdout_selection_role": self.holdout_selection_role,
            "uncertainty_statement": (
                "total uncertainty includes operator scenarios and path seeds; "
                "seed-only dispersion is not total reconstruction uncertainty"
            ),
            "event_rows_inline": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "report_id": self.report_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ObservationUncertaintyReportV1:
        _require_schema(
            str(data.get("schema_version", "")),
            OBSERVATION_UNCERTAINTY_REPORT_SCHEMA_VERSION,
        )
        _require_derived(data, "event_rows_inline", False)
        return cls(
            ensemble_id=str(data.get("ensemble_id", "")),
            policy_id=str(data.get("policy_id", "")),
            scenario_ids=_text_tuple(
                _sequence(data.get("scenario_ids"), "scenario_ids"),
                "scenario_ids",
            ),
            diagnostics=tuple(
                ObservationUncertaintyDiagnosticV1.from_dict(item)
                for item in _mapping_sequence(
                    data.get("diagnostics"), "diagnostics"
                )
            ),
            decompositions=tuple(
                ObservationUncertaintyDecompositionV1.from_dict(item)
                for item in _mapping_sequence(
                    data.get("decompositions"), "decompositions"
                )
            ),
            untouched_release_holdout=_strict_bool(
                data.get("untouched_release_holdout"),
                "untouched_release_holdout",
            ),
            holdout_selection_role=_strict_bool(
                data.get("holdout_selection_role"), "holdout_selection_role"
            ),
            report_id=str(data.get("report_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> ObservationUncertaintyReportV1:
        return cls.from_dict(_json_mapping(text))


def observation_uncertainty_availability(
    conditioning: Mapping[str, Any],
) -> ObservationUncertaintyAvailability:
    """Classify endpoint availability without silently inventing a side."""
    joint = _mapping(conditioning.get("joint_retention"), "joint_retention")
    has_lower = joint.get("retention_lower_bound") is not None
    has_upper = joint.get("retention_upper_bound") is not None
    if has_lower and has_upper:
        return ObservationUncertaintyAvailability.TWO_SIDED
    if has_lower:
        return ObservationUncertaintyAvailability.LOWER_ONLY
    if has_upper:
        return ObservationUncertaintyAvailability.UPPER_ONLY
    return ObservationUncertaintyAvailability.UNAVAILABLE


def observation_admission_missing_count_bound(
    retained_count: int,
    retention_probability: float,
    admission_quantile: float,
) -> int:
    """Return the frozen conservative missing-count admission bound."""
    retained = _nonnegative_int(retained_count, "retained_count")
    probability = _retention_probability(
        retention_probability, "retention_probability"
    )
    quantile = _open_unit_float(admission_quantile, "admission_quantile")
    mean, variance = negative_binomial_failure_moments(retained, probability)
    return _cantelli_upper_bound(mean, variance, quantile)


def derive_observation_uncertainty_scenarios(
    policy: ObservationUncertaintyPolicyV1,
    conditioning: Mapping[str, Any],
) -> tuple[ObservationUncertaintyScenarioV1, ...]:
    """Derive the frozen three scenarios from qualified interval evidence."""
    if not isinstance(policy, ObservationUncertaintyPolicyV1):
        raise TypeError("observation uncertainty policy must use v1")
    availability = observation_uncertainty_availability(conditioning)
    if availability is not ObservationUncertaintyAvailability.TWO_SIDED:
        raise ValueError(
            "observation uncertainty is not two-sided: " + availability.value
        )
    operator_id = _required_text(conditioning.get("observation_operator_id"))
    conditioning_id = _required_text(conditioning.get("conditioning_id"))
    epoch_id = _required_text(conditioning.get("feed_epoch_id"))
    joint = _mapping(conditioning.get("joint_retention"), "joint_retention")
    stratum_id = _required_text(
        joint.get("stratum_id")
        or (
            _required_text(joint.get("left_stratum_id"))
            + "::"
            + _required_text(joint.get("right_stratum_id"))
        )
    )
    stratum_key = str(joint.get("stratum_key") or stratum_id)
    stratum_level = _required_text(joint.get("stratum_level"))
    central = _retention_probability(
        joint.get("retention_probability"), "retention_probability"
    )
    lower = _retention_probability(
        joint.get("retention_lower_bound"), "retention_lower_bound"
    )
    upper = _retention_probability(
        joint.get("retention_upper_bound"), "retention_upper_bound"
    )
    values = {
        ObservationUncertaintyScenarioKind.HIGH_RETENTION_LOW_INFILL: upper,
        ObservationUncertaintyScenarioKind.CENTRAL_FITTED_RETENTION: central,
        ObservationUncertaintyScenarioKind.LOW_RETENTION_HIGH_INFILL: lower,
    }
    policies = {
        ObservationUncertaintyScenarioKind.HIGH_RETENTION_LOW_INFILL: "qualified_upper_endpoint",
        ObservationUncertaintyScenarioKind.CENTRAL_FITTED_RETENTION: "qualified_point_estimate",
        ObservationUncertaintyScenarioKind.LOW_RETENTION_HIGH_INFILL: "qualified_lower_endpoint",
    }
    return tuple(
        ObservationUncertaintyScenarioV1(
            kind=kind,
            policy_id=policy.policy_id,
            report_quantiles=policy.report_quantiles,
            admission_quantile=policy.admission_quantile,
            observation_operator_id=operator_id,
            conditioning_id=conditioning_id,
            feed_epoch_id=epoch_id,
            stratum_id=stratum_id,
            stratum_key=stratum_key,
            stratum_level=stratum_level,
            central_retention_probability=central,
            lower_retention_probability=lower,
            upper_retention_probability=upper,
            retention_probability=values[kind],
            support_count=_positive_int(
                joint.get("support_count"), "support_count"
            ),
            evidence_ids=_text_tuple(
                _sequence(joint.get("evidence_ids"), "evidence_ids"),
                "evidence_ids",
            ),
            estimation_bases=_text_tuple(
                _sequence(joint.get("estimation_bases"), "estimation_bases"),
                "estimation_bases",
            ),
            provenance=_text_tuple(
                _sequence(joint.get("provenance"), "provenance"),
                "provenance",
            ),
            endpoint_policy=policies[kind],
        )
        for kind in SCENARIO_ORDER
    )


def build_observation_uncertainty_ensemble(
    policy: ObservationUncertaintyPolicyV1,
    conditioning: Mapping[str, Any],
    *,
    ensemble_members: Sequence[tuple[str, int]],
    observed_counts: Mapping[str, int],
    session: str,
    maximum_missing_event_count: int,
    maximum_candidate_amplification: float,
) -> ObservationUncertaintyEnsembleV1:
    """Build a deterministic scenario assignment and bounded count evidence."""
    if not isinstance(policy, ObservationUncertaintyPolicyV1):
        raise TypeError("observation uncertainty policy must use v1")
    scenarios = derive_observation_uncertainty_scenarios(policy, conditioning)
    members_input = tuple(ensemble_members)
    minimum_members = (
        len(SCENARIO_ORDER) * policy.minimum_path_realizations_per_scenario
    )
    if (
        not minimum_members
        <= len(members_input)
        <= MAX_OBSERVATION_SCENARIO_MEMBERS
    ):
        raise ValueError(
            "ensemble member count cannot cover observation scenarios"
        )
    if len({item[0] for item in members_input}) != len(members_input):
        raise ValueError(
            "observation ensemble member identities are duplicated"
        )
    scenario_by_kind = {item.kind: item for item in scenarios}
    members = tuple(
        ObservationUncertaintyMemberV1(
            ordinal=ordinal,
            ensemble_member_id=_required_text(member_id),
            path_seed=_nonnegative_int(seed, "path_seed"),
            scenario_id=scenario_by_kind[
                policy.scenario_order[
                    (ordinal - 1) % len(policy.scenario_order)
                ]
            ].scenario_id,
            scenario_kind=policy.scenario_order[
                (ordinal - 1) % len(policy.scenario_order)
            ],
            retention_mode=policy.retention_mode(
                policy.scenario_order[
                    (ordinal - 1) % len(policy.scenario_order)
                ]
            ),
        )
        for ordinal, (member_id, seed) in enumerate(members_input, start=1)
    )
    counts = _count_mapping(observed_counts, "observed count")
    if not counts:
        raise ValueError("observation uncertainty requires observed counts")
    cardinality = tuple(
        _cardinality_evidence(
            policy,
            scenario,
            symbol=symbol,
            observed_retained_count=count,
            session=session,
            maximum_missing_event_count=maximum_missing_event_count,
            maximum_candidate_amplification=maximum_candidate_amplification,
        )
        for symbol, count in counts.items()
        for scenario in scenarios
    )
    admitted = all(item.admitted for item in cardinality)
    reasons = tuple(
        sorted(
            f"{item.symbol}:{item.scenario_id}:{item.refusal_risk}"
            for item in cardinality
            if not item.admitted
        )
    )
    return ObservationUncertaintyEnsembleV1(
        policy=policy,
        conditioning_id=_required_text(conditioning.get("conditioning_id")),
        observation_operator_id=_required_text(
            conditioning.get("observation_operator_id")
        ),
        feed_epoch_id=_required_text(conditioning.get("feed_epoch_id")),
        session=_required_text(session),
        scenarios=scenarios,
        members=members,
        cardinality_evidence=cardinality,
        admitted=admitted,
        refusal_reasons=reasons,
    )


def calibrate_observation_uncertainty(
    ensemble: ObservationUncertaintyEnsembleV1,
    diagnostics: Sequence[ObservationUncertaintyDiagnosticV1],
    *,
    untouched_release_holdout: bool = True,
) -> ObservationUncertaintyReportV1:
    """Decompose operator and path dispersion on validation and holdout."""
    if not isinstance(ensemble, ObservationUncertaintyEnsembleV1):
        raise TypeError("observation uncertainty ensemble must use v1")
    cells = tuple(diagnostics)
    member_by_id = {item.ensemble_member_id: item for item in ensemble.members}
    for item in cells:
        member = member_by_id.get(item.ensemble_member_id)
        if (
            member is None
            or member.scenario_id != item.scenario_id
            or member.path_seed != item.path_seed
        ):
            raise ValueError("observation diagnostic member lineage differs")
    completed_pairs = {
        (item.split, item.scenario_id)
        for item in cells
        if item.status is ObservationGenerationStatus.COMPLETED
    }
    required_pairs = {
        (split, scenario.scenario_id)
        for split in ObservationUncertaintySplit
        for scenario in ensemble.scenarios
    }
    if completed_pairs != required_pairs:
        raise ValueError(
            "observation uncertainty requires validation and holdout support"
        )
    decompositions = tuple(
        _decomposition(cells, split=split, metric_name=metric)
        for split in ObservationUncertaintySplit
        for metric in OBSERVATION_UNCERTAINTY_METRIC_NAMES
    )
    return ObservationUncertaintyReportV1(
        ensemble_id=ensemble.ensemble_id,
        policy_id=ensemble.policy.policy_id,
        scenario_ids=tuple(item.scenario_id for item in ensemble.scenarios),
        diagnostics=cells,
        decompositions=decompositions,
        untouched_release_holdout=untouched_release_holdout,
        holdout_selection_role=False,
    )


def write_observation_uncertainty_policy(
    policy: ObservationUncertaintyPolicyV1, output_directory: str | Path
) -> ArtifactRef:
    ref = _write_contract(
        policy.to_json(),
        output_directory,
        prefix="observation-uncertainty-policy",
        kind=OBSERVATION_UNCERTAINTY_POLICY_ARTIFACT_KIND,
        metadata={"policy_id": policy.policy_id},
    )
    if read_observation_uncertainty_policy(ref.path) != policy:
        raise ValueError("published observation uncertainty policy differs")
    return ref


def read_observation_uncertainty_policy(
    path: str | Path,
) -> ObservationUncertaintyPolicyV1:
    return ObservationUncertaintyPolicyV1.from_dict(
        _read_content_addressed_json(path, "observation-uncertainty-policy")
    )


def write_observation_uncertainty_ensemble(
    ensemble: ObservationUncertaintyEnsembleV1, output_directory: str | Path
) -> ArtifactRef:
    ref = _write_contract(
        ensemble.to_json(),
        output_directory,
        prefix="observation-uncertainty-ensemble",
        kind=OBSERVATION_UNCERTAINTY_ENSEMBLE_ARTIFACT_KIND,
        metadata={
            "ensemble_id": ensemble.ensemble_id,
            "policy_id": ensemble.policy.policy_id,
            "admitted": ensemble.admitted,
        },
    )
    if read_observation_uncertainty_ensemble(ref.path) != ensemble:
        raise ValueError("published observation uncertainty ensemble differs")
    return ref


def read_observation_uncertainty_ensemble(
    path: str | Path,
) -> ObservationUncertaintyEnsembleV1:
    return ObservationUncertaintyEnsembleV1.from_dict(
        _read_content_addressed_json(path, "observation-uncertainty-ensemble")
    )


def write_observation_uncertainty_report(
    report: ObservationUncertaintyReportV1, output_directory: str | Path
) -> ArtifactRef:
    ref = _write_contract(
        report.to_json(),
        output_directory,
        prefix="observation-uncertainty-report",
        kind=OBSERVATION_UNCERTAINTY_REPORT_ARTIFACT_KIND,
        metadata={
            "report_id": report.report_id,
            "ensemble_id": report.ensemble_id,
        },
    )
    if read_observation_uncertainty_report(ref.path) != report:
        raise ValueError("published observation uncertainty report differs")
    return ref


def read_observation_uncertainty_report(
    path: str | Path,
) -> ObservationUncertaintyReportV1:
    return ObservationUncertaintyReportV1.from_dict(
        _read_content_addressed_json(path, "observation-uncertainty-report")
    )


def _cardinality_evidence(
    policy: ObservationUncertaintyPolicyV1,
    scenario: ObservationUncertaintyScenarioV1,
    *,
    symbol: str,
    observed_retained_count: int,
    session: str,
    maximum_missing_event_count: int,
    maximum_candidate_amplification: float,
) -> ObservationCardinalityEvidenceV1:
    observed = _nonnegative_int(
        observed_retained_count, "observed_retained_count"
    )
    probability = scenario.retention_probability
    mean, variance = negative_binomial_failure_moments(observed, probability)
    quantile_values = _negative_binomial_failure_quantiles(
        observed,
        probability,
        policy.report_quantiles,
        max_steps=policy.max_quantile_steps,
    )
    missing_quantiles = {
        _quantile_key(quantile): value
        for quantile, value in zip(policy.report_quantiles, quantile_values)
    }
    total_quantiles = {
        key: observed + value for key, value in missing_quantiles.items()
    }
    admission = _cantelli_upper_bound(mean, variance, policy.admission_quantile)
    maximum = _nonnegative_int(
        maximum_missing_event_count, "maximum_missing_event_count"
    )
    amplification = 0.0 if observed == 0 else admission / observed
    max_amplification = _positive_float(
        maximum_candidate_amplification,
        "maximum_candidate_amplification",
    )
    risk = _cantelli_exceedance_bound(mean, variance, maximum)
    admitted = admission <= maximum and amplification <= max_amplification
    return ObservationCardinalityEvidenceV1(
        scenario_id=scenario.scenario_id,
        symbol=_required_text(symbol).upper(),
        feed_epoch_id=scenario.feed_epoch_id,
        session=_required_text(session),
        observed_retained_count=observed,
        retention_probability=probability,
        missing_count_mean=mean,
        missing_count_variance=variance,
        missing_count_quantiles=missing_quantiles,
        total_event_count_quantiles=total_quantiles,
        admission_missing_count_bound=admission,
        maximum_missing_event_count=maximum,
        candidate_amplification_bound=amplification,
        maximum_candidate_amplification=max_amplification,
        limit_exceedance_probability_bound=risk,
        admitted=admitted,
        refusal_risk=_refusal_risk(risk, admitted),
    )


def _decomposition(
    diagnostics: Sequence[ObservationUncertaintyDiagnosticV1],
    *,
    split: ObservationUncertaintySplit,
    metric_name: str,
) -> ObservationUncertaintyDecompositionV1:
    completed = tuple(
        item
        for item in diagnostics
        if item.split is split
        and item.status is ObservationGenerationStatus.COMPLETED
    )
    grouped: dict[str, list[float]] = {}
    for item in completed:
        grouped.setdefault(item.scenario_id, []).append(
            item.metrics[metric_name]
        )
    if len(grouped) != len(SCENARIO_ORDER):
        raise ValueError("uncertainty decomposition lacks scenario support")
    scenario_means = [statistics.fmean(values) for values in grouped.values()]
    between = statistics.pvariance(scenario_means)
    within = statistics.fmean(
        statistics.pvariance(values) if len(values) > 1 else 0.0
        for values in grouped.values()
    )
    total = between + within
    return ObservationUncertaintyDecompositionV1(
        split=split,
        metric_name=metric_name,
        completed_cell_count=len(completed),
        operator_between_scenario_variance=between,
        path_within_scenario_variance=within,
        total_variance=total,
        operator_variance_fraction=0.0 if total == 0.0 else between / total,
    )


def _negative_binomial_failure_quantiles(
    retained_count: int,
    probability: float,
    quantiles: Sequence[float],
    *,
    max_steps: int,
) -> tuple[int, ...]:
    """Return exact bounded quantiles using a stable log-PMF recurrence."""
    retained = _nonnegative_int(retained_count, "retained_count")
    selected_probability = _retention_probability(probability, "probability")
    selected_quantiles = tuple(
        _open_unit_float(value, "quantile") for value in quantiles
    )
    if retained == 0 or selected_probability == 1.0:
        return tuple(0 for _ in selected_quantiles)
    indexed = sorted(enumerate(selected_quantiles), key=lambda item: item[1])
    results = [0] * len(selected_quantiles)
    log_probability = retained * math.log(selected_probability)
    log_cdf = -math.inf
    log_failure = math.log1p(-selected_probability)
    next_index = 0
    for missing in range(max_steps + 1):
        log_cdf = _logaddexp(log_cdf, log_probability)
        while next_index < len(indexed) and log_cdf >= math.log(
            indexed[next_index][1]
        ):
            original_index, _ = indexed[next_index]
            results[original_index] = missing
            next_index += 1
        if next_index == len(indexed):
            return tuple(results)
        log_probability += (
            math.log(missing + retained) - math.log(missing + 1) + log_failure
        )
    raise ValueError("negative-binomial quantile exceeds bounded support")


def _cantelli_upper_bound(mean: float, variance: float, quantile: float) -> int:
    if variance == 0.0:
        return math.ceil(mean)
    offset = math.sqrt(variance * quantile / (1.0 - quantile))
    return math.ceil(mean + offset)


def _cantelli_exceedance_bound(
    mean: float, variance: float, maximum: int
) -> float:
    if variance == 0.0:
        return float(mean > maximum)
    offset = maximum - mean
    if offset <= 0.0:
        return 1.0
    return min(1.0, variance / (variance + offset * offset))


def _refusal_risk(probability: float, admitted: bool) -> str:
    if not admitted:
        return "certain_or_policy_refused"
    if probability <= 0.01:
        return "low"
    if probability <= 0.10:
        return "moderate"
    return "elevated"


def _logaddexp(left: float, right: float) -> float:
    if left == -math.inf:
        return right
    if right == -math.inf:
        return left
    maximum = max(left, right)
    return maximum + math.log(
        math.exp(left - maximum) + math.exp(right - maximum)
    )


def _quantile_key(value: float) -> str:
    return format(value, ".12g")


def _write_contract(
    text: str,
    output_directory: str | Path,
    *,
    prefix: str,
    kind: str,
    metadata: Mapping[str, JSONValue],
) -> ArtifactRef:
    from histdatacom.orchestration.reconstruction import artifact_ref_for_file

    payload = text.encode("utf-8")
    if len(payload) > MAX_OBSERVATION_ARTIFACT_BYTES:
        raise ValueError("observation uncertainty artifact exceeds size limit")
    digest = hashlib.sha256(payload).hexdigest()
    directory = Path(output_directory).expanduser().resolve()
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"{prefix}-{digest}.json"
    temporary = directory / f".{prefix}-{os.getpid()}-{digest}.tmp"
    try:
        with temporary.open("wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        if target.exists():
            if target.read_bytes() != payload:
                raise ValueError("observation uncertainty artifact collision")
            temporary.unlink()
        else:
            os.replace(temporary, target)
    finally:
        if temporary.exists():
            temporary.unlink()
    return artifact_ref_for_file(target, kind=kind, metadata=metadata)


def _read_content_addressed_json(
    path: str | Path, prefix: str
) -> Mapping[str, Any]:
    from histdatacom.orchestration.reconstruction import (
        artifact_ref_for_file,
        verify_artifact_ref,
    )

    selected = Path(path).expanduser().resolve()
    payload = selected.read_bytes()
    if len(payload) > MAX_OBSERVATION_ARTIFACT_BYTES:
        raise ValueError("observation uncertainty artifact exceeds size limit")
    digest = hashlib.sha256(payload).hexdigest()
    if selected.name != f"{prefix}-{digest}.json":
        raise ValueError("observation uncertainty artifact filename differs")
    ref = artifact_ref_for_file(selected, kind=prefix.replace("-", "_") + "_v1")
    verify_artifact_ref(ref)
    loaded = json.loads(payload)
    if not isinstance(loaded, Mapping):
        raise TypeError(
            "observation uncertainty artifact must contain an object"
        )
    return loaded


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _scenario_kind_tuple(
    values: Sequence[ObservationUncertaintyScenarioKind],
) -> tuple[ObservationUncertaintyScenarioKind, ...]:
    selected = tuple(
        ObservationUncertaintyScenarioKind(value) for value in values
    )
    if len(set(selected)) != len(selected):
        raise ValueError("observation scenario kinds are duplicated")
    return tuple(item for item in SCENARIO_ORDER if item in selected)


def _count_mapping(values: Mapping[str, int], label: str) -> dict[str, int]:
    selected = {
        _required_text(key): _nonnegative_int(value, label)
        for key, value in values.items()
    }
    return dict(sorted(selected.items()))


def _int_mapping(value: Any, name: str) -> dict[str, int]:
    selected = _mapping(value, name)
    return {
        _required_text(key): _strict_int(item, name)
        for key, item in selected.items()
    }


def _float_mapping(value: Any, name: str) -> dict[str, float]:
    selected = _mapping(value, name)
    return {
        _required_text(key): _finite_float(item, name)
        for key, item in selected.items()
    }


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be an object")
    return value


def _mapping_sequence(value: Any, name: str) -> tuple[Mapping[str, Any], ...]:
    values = _sequence(value, name)
    if not all(isinstance(item, Mapping) for item in values):
        raise TypeError(f"{name} must contain objects")
    return tuple(item for item in values if isinstance(item, Mapping))


def _sequence(value: Any, name: str) -> tuple[Any, ...]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(
        value, Sequence
    ):
        raise TypeError(f"{name} must be a sequence")
    return tuple(value)


def _text_tuple(
    values: Sequence[Any], name: str, *, allow_empty: bool = False
) -> tuple[str, ...]:
    selected = tuple(sorted({_required_text(item) for item in values}))
    if not selected and not allow_empty:
        raise ValueError(f"{name} is empty")
    return selected


def _json_mapping(text: str) -> Mapping[str, Any]:
    loaded = json.loads(text)
    if not isinstance(loaded, Mapping):
        raise TypeError("contract JSON must contain an object")
    return loaded


def _require_derived(data: Mapping[str, Any], name: str, expected: Any) -> None:
    if data.get(name) != expected:
        raise ValueError(f"derived {name} differs")


def _required_text(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TypeError("value must be non-empty text")
    return value.strip()


def _optional_text(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return _required_text(value)


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    return value


def _nonnegative_int(value: Any, name: str) -> int:
    selected = _strict_int(value, name)
    if selected < 0:
        raise ValueError(f"{name} must be nonnegative")
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


def _nonnegative_float(value: Any, name: str) -> float:
    selected = _finite_float(value, name)
    if selected < 0.0:
        raise ValueError(f"{name} must be nonnegative")
    return selected


def _positive_float(value: Any, name: str) -> float:
    selected = _finite_float(value, name)
    if selected <= 0.0:
        raise ValueError(f"{name} must be positive")
    return selected


def _unit_float(value: Any, name: str) -> float:
    selected = _finite_float(value, name)
    if not 0.0 <= selected <= 1.0:
        raise ValueError(f"{name} must be in [0, 1]")
    return selected


def _open_unit_float(value: Any, name: str) -> float:
    selected = _finite_float(value, name)
    if not 0.0 < selected < 1.0:
        raise ValueError(f"{name} must be in (0, 1)")
    return selected


def _retention_probability(value: Any, name: str) -> float:
    selected = _finite_float(value, name)
    if not 0.0 < selected <= 1.0:
        raise ValueError(f"{name} must be in (0, 1]")
    return selected


def _strict_bool(value: Any, name: str) -> bool:
    if type(value) is not bool:
        raise TypeError(f"{name} must be boolean")
    return value


def _require_schema(value: str, expected: str) -> None:
    if value != expected:
        raise ValueError(f"unsupported schema version: {value}")


__all__ = [
    "OBSERVATION_CARDINALITY_EVIDENCE_SCHEMA_VERSION",
    "OBSERVATION_UNCERTAINTY_DECOMPOSITION_SCHEMA_VERSION",
    "OBSERVATION_UNCERTAINTY_DIAGNOSTIC_SCHEMA_VERSION",
    "OBSERVATION_UNCERTAINTY_ENSEMBLE_ARTIFACT_KIND",
    "OBSERVATION_UNCERTAINTY_ENSEMBLE_SCHEMA_VERSION",
    "OBSERVATION_UNCERTAINTY_MEMBER_SCHEMA_VERSION",
    "OBSERVATION_UNCERTAINTY_METRIC_NAMES",
    "OBSERVATION_UNCERTAINTY_POLICY_ARTIFACT_KIND",
    "OBSERVATION_UNCERTAINTY_POLICY_SCHEMA_VERSION",
    "OBSERVATION_UNCERTAINTY_REPORT_ARTIFACT_KIND",
    "OBSERVATION_UNCERTAINTY_REPORT_SCHEMA_VERSION",
    "OBSERVATION_UNCERTAINTY_SCENARIO_SCHEMA_VERSION",
    "ObservationCardinalityEvidenceV1",
    "ObservationGenerationStatus",
    "ObservationScenarioRetentionMode",
    "ObservationUncertaintyAvailability",
    "ObservationUncertaintyDecompositionV1",
    "ObservationUncertaintyDiagnosticV1",
    "ObservationUncertaintyEnsembleV1",
    "ObservationUncertaintyMemberV1",
    "ObservationUncertaintyPolicyV1",
    "ObservationUncertaintyReportV1",
    "ObservationUncertaintyScenarioKind",
    "ObservationUncertaintyScenarioV1",
    "ObservationUncertaintySplit",
    "build_observation_uncertainty_ensemble",
    "calibrate_observation_uncertainty",
    "derive_observation_uncertainty_scenarios",
    "observation_admission_missing_count_bound",
    "observation_uncertainty_availability",
    "read_observation_uncertainty_ensemble",
    "read_observation_uncertainty_policy",
    "read_observation_uncertainty_report",
    "write_observation_uncertainty_ensemble",
    "write_observation_uncertainty_policy",
    "write_observation_uncertainty_report",
]
