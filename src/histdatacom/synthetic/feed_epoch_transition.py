"""Qualified feed-epoch transition scenarios for historical reconstruction.

Transition labels identify uncertainty about *when* the feed changed.  They do
not identify a third fitted observation stratum.  This module therefore freezes
three adjacent-epoch scenarios without fitting protected transition products:
left persistence, the historical linear bridge, and early right adoption.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

from histdatacom.runtime_contracts import ArtifactRef, JSONValue
from histdatacom.synthetic.contracts import canonical_contract_json

FEED_EPOCH_TRANSITION_POLICY_SCHEMA_VERSION = (
    "histdatacom.feed-epoch-transition-policy.v1"
)
FEED_EPOCH_TRANSITION_SCENARIO_SCHEMA_VERSION = (
    "histdatacom.feed-epoch-transition-scenario.v1"
)
FEED_EPOCH_TRANSITION_DIAGNOSTIC_SCHEMA_VERSION = (
    "histdatacom.feed-epoch-transition-diagnostic.v1"
)
FEED_EPOCH_TRANSITION_REPORT_SCHEMA_VERSION = (
    "histdatacom.feed-epoch-transition-report.v1"
)
FEED_EPOCH_TRANSITION_POLICY_ARTIFACT_KIND = "feed_epoch_transition_policy_v1"
FEED_EPOCH_TRANSITION_REPORT_ARTIFACT_KIND = "feed_epoch_transition_report_v1"

MAX_TRANSITION_ARTIFACT_BYTES = 64 * 1024 * 1024


class FeedEpochTransitionScenarioKind(str, Enum):
    """Frozen alternatives to the unverified single linear bridge."""

    LEFT_PERSISTENCE = "left_persistence"
    LINEAR_BRIDGE = "linear_bridge"
    EARLY_RIGHT_ADOPTION = "early_right_adoption"


class FeedEpochTransitionSplit(str, Enum):
    """Evaluation roles that may support a transition decision."""

    VALIDATION = "validation"
    FINAL_HOLDOUT = "final_holdout"


class FeedEpochTransitionDiagnosticStatus(str, Enum):
    """Outcome of one scenario/metric evaluation cell."""

    COMPLETED = "completed"
    REFUSED = "refused"
    FAILED = "failed"


class FeedEpochTransitionDecision(str, Enum):
    """Permitted certification outcomes for a transition window."""

    LINEAR_RETAINED = "linear_retained_with_negligible_sensitivity"
    MULTIPLE_SCENARIOS_REQUIRED = "multiple_scenarios_required"
    LIMITED_OR_REFUSED = "limited_or_refused"


TRANSITION_SCENARIO_ORDER = (
    FeedEpochTransitionScenarioKind.LEFT_PERSISTENCE,
    FeedEpochTransitionScenarioKind.LINEAR_BRIDGE,
    FeedEpochTransitionScenarioKind.EARLY_RIGHT_ADOPTION,
)

FEED_EPOCH_TRANSITION_METRIC_NAMES = (
    "missing_count",
    "missing_count_uncertainty",
    "adaptive_boundary_count",
    "refusal_rate",
    "resource_work",
    "interarrival_timing",
    "mark_transition",
    "path_variation",
    "spread",
    "synchronization_age",
    "triangle_residual",
    "projection_burden",
    "strategy_dispersion",
)


@dataclass(frozen=True, slots=True)
class FeedEpochTransitionPolicyV1:
    """Predeclared scenarios, evaluation metrics, and decision tolerances."""

    scenario_order: tuple[FeedEpochTransitionScenarioKind, ...] = (
        TRANSITION_SCENARIO_ORDER
    )
    relative_materiality_tolerance: float = 0.10
    absolute_materiality_tolerance: float = 1e-12
    minimum_path_realizations_per_crossed_cell: int = 1
    ex_ante_prior_artifact_id: str | None = None
    policy_id: str = ""
    schema_version: str = FEED_EPOCH_TRANSITION_POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, FEED_EPOCH_TRANSITION_POLICY_SCHEMA_VERSION
        )
        order = tuple(
            FeedEpochTransitionScenarioKind(item)
            for item in self.scenario_order
        )
        if order != TRANSITION_SCENARIO_ORDER:
            raise ValueError("feed-epoch transition scenario order is frozen")
        object.__setattr__(self, "scenario_order", order)
        relative = _nonnegative_float(
            self.relative_materiality_tolerance,
            "relative_materiality_tolerance",
        )
        absolute = _nonnegative_float(
            self.absolute_materiality_tolerance,
            "absolute_materiality_tolerance",
        )
        if relative > 1.0:
            raise ValueError("transition relative tolerance exceeds one")
        object.__setattr__(self, "relative_materiality_tolerance", relative)
        object.__setattr__(self, "absolute_materiality_tolerance", absolute)
        minimum = _positive_int(
            self.minimum_path_realizations_per_crossed_cell,
            "minimum_path_realizations_per_crossed_cell",
        )
        if minimum > 16:
            raise ValueError(
                "transition crossed-cell realization bound exceeded"
            )
        object.__setattr__(
            self, "minimum_path_realizations_per_crossed_cell", minimum
        )
        prior = _optional_text(self.ex_ante_prior_artifact_id)
        object.__setattr__(self, "ex_ante_prior_artifact_id", prior)
        expected = _stable_id("feed-epoch-transition-policy", self.payload())
        supplied = _optional_text(self.policy_id)
        if supplied is not None and supplied != expected:
            raise ValueError("feed-epoch transition policy_id differs")
        object.__setattr__(self, "policy_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "scenario_order": [item.value for item in self.scenario_order],
            "scenario_definition": {
                FeedEpochTransitionScenarioKind.LEFT_PERSISTENCE.value: (
                    "adjacent-left-fitted-epoch-retention-v1"
                ),
                FeedEpochTransitionScenarioKind.LINEAR_BRIDGE.value: (
                    "elapsed-uncertainty-interval-linear-weight-v1"
                ),
                FeedEpochTransitionScenarioKind.EARLY_RIGHT_ADOPTION.value: (
                    "adjacent-right-fitted-epoch-retention-v1"
                ),
            },
            "protected_transition_product_fit": False,
            "information_policy": (
                "ex-post-only-unless-point-in-time-valid-prior-is-bound-v1"
            ),
            "ex_ante_prior_artifact_id": self.ex_ante_prior_artifact_id,
            "symbol_scope_policy": "exact-synchronized-product-symbols-v1",
            "operator_evidence_policy": (
                "adjacent-qualified-fitted-strata-and-evidence-identities-v1"
            ),
            "member_assignment_policy": (
                "complete-transition-x-observation-cross-product-v1"
            ),
            "minimum_path_realizations_per_crossed_cell": (
                self.minimum_path_realizations_per_crossed_cell
            ),
            "diagnostic_metrics": list(FEED_EPOCH_TRANSITION_METRIC_NAMES),
            "relative_materiality_tolerance": (
                self.relative_materiality_tolerance
            ),
            "absolute_materiality_tolerance": (
                self.absolute_materiality_tolerance
            ),
            "decision_policy": (
                "linear-if-negligible-else-multiple-scenarios-or-limit-v1"
            ),
            "final_holdout_selection_role": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "policy_id": self.policy_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> FeedEpochTransitionPolicyV1:
        _require_schema(
            str(data.get("schema_version", "")),
            FEED_EPOCH_TRANSITION_POLICY_SCHEMA_VERSION,
        )
        _require_derived(data, "protected_transition_product_fit", False)
        _require_derived(
            data,
            "information_policy",
            "ex-post-only-unless-point-in-time-valid-prior-is-bound-v1",
        )
        _require_derived(
            data,
            "symbol_scope_policy",
            "exact-synchronized-product-symbols-v1",
        )
        _require_derived(data, "final_holdout_selection_role", False)
        return cls(
            scenario_order=tuple(
                FeedEpochTransitionScenarioKind(str(item))
                for item in _sequence(
                    data.get("scenario_order"), "scenario_order"
                )
            ),
            relative_materiality_tolerance=_finite_float(
                data.get("relative_materiality_tolerance"),
                "relative_materiality_tolerance",
            ),
            absolute_materiality_tolerance=_finite_float(
                data.get("absolute_materiality_tolerance"),
                "absolute_materiality_tolerance",
            ),
            minimum_path_realizations_per_crossed_cell=_strict_int(
                data.get("minimum_path_realizations_per_crossed_cell"),
                "minimum_path_realizations_per_crossed_cell",
            ),
            ex_ante_prior_artifact_id=_optional_text(
                data.get("ex_ante_prior_artifact_id")
            ),
            policy_id=str(data.get("policy_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> FeedEpochTransitionPolicyV1:
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class FeedEpochTransitionScenarioV1:
    """One boundary-specific transition scenario and complete lineage."""

    kind: FeedEpochTransitionScenarioKind
    policy_id: str
    observation_operator_id: str
    feed_epoch_definition_id: str
    feed_epoch_id: str
    transition_boundary_id: str
    transition_start_ns: int
    transition_end_ns: int
    transition_left_epoch_id: str
    transition_right_epoch_id: str
    symbol_scope: tuple[str, ...]
    information_mode: str
    left_stratum_ids: tuple[str, ...]
    right_stratum_ids: tuple[str, ...]
    operator_evidence_ids: tuple[str, ...]
    linear_right_weight: float
    left_weight: float
    right_weight: float
    scenario_id: str = ""
    schema_version: str = FEED_EPOCH_TRANSITION_SCENARIO_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, FEED_EPOCH_TRANSITION_SCENARIO_SCHEMA_VERSION
        )
        kind = FeedEpochTransitionScenarioKind(self.kind)
        object.__setattr__(self, "kind", kind)
        for name in (
            "policy_id",
            "observation_operator_id",
            "feed_epoch_definition_id",
            "feed_epoch_id",
            "transition_boundary_id",
            "transition_left_epoch_id",
            "transition_right_epoch_id",
            "information_mode",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        start = _strict_int(self.transition_start_ns, "transition_start_ns")
        end = _strict_int(self.transition_end_ns, "transition_end_ns")
        if start >= end:
            raise ValueError("transition interval is empty")
        object.__setattr__(self, "transition_start_ns", start)
        object.__setattr__(self, "transition_end_ns", end)
        for name in (
            "symbol_scope",
            "left_stratum_ids",
            "right_stratum_ids",
            "operator_evidence_ids",
        ):
            values = _text_tuple(getattr(self, name), name)
            object.__setattr__(self, name, values)
        linear = _unit_float(self.linear_right_weight, "linear_right_weight")
        expected_weights = {
            FeedEpochTransitionScenarioKind.LEFT_PERSISTENCE: (1.0, 0.0),
            FeedEpochTransitionScenarioKind.LINEAR_BRIDGE: (
                1.0 - linear,
                linear,
            ),
            FeedEpochTransitionScenarioKind.EARLY_RIGHT_ADOPTION: (0.0, 1.0),
        }[kind]
        left = _unit_float(self.left_weight, "left_weight")
        right = _unit_float(self.right_weight, "right_weight")
        if not (
            math.isclose(left, expected_weights[0], abs_tol=1e-15)
            and math.isclose(right, expected_weights[1], abs_tol=1e-15)
            and math.isclose(left + right, 1.0, abs_tol=1e-15)
        ):
            raise ValueError("transition scenario weights differ from policy")
        object.__setattr__(self, "linear_right_weight", linear)
        object.__setattr__(self, "left_weight", left)
        object.__setattr__(self, "right_weight", right)
        expected = _stable_id("feed-epoch-transition-scenario", self.payload())
        supplied = _optional_text(self.scenario_id)
        if supplied is not None and supplied != expected:
            raise ValueError("feed-epoch transition scenario_id differs")
        object.__setattr__(self, "scenario_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "kind": self.kind.value,
            "feed_epoch_transition_policy_id": self.policy_id,
            "observation_operator_id": self.observation_operator_id,
            "feed_epoch_definition_id": self.feed_epoch_definition_id,
            "feed_epoch_id": self.feed_epoch_id,
            "transition_boundary_id": self.transition_boundary_id,
            "transition_start_ns": self.transition_start_ns,
            "transition_end_ns": self.transition_end_ns,
            "transition_left_epoch_id": self.transition_left_epoch_id,
            "transition_right_epoch_id": self.transition_right_epoch_id,
            "symbol_scope": list(self.symbol_scope),
            "information_mode": self.information_mode,
            "left_stratum_ids": list(self.left_stratum_ids),
            "right_stratum_ids": list(self.right_stratum_ids),
            "operator_evidence_ids": list(self.operator_evidence_ids),
            "linear_right_weight": self.linear_right_weight,
            "left_weight": self.left_weight,
            "right_weight": self.right_weight,
            "protected_transition_product_fit": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "scenario_id": self.scenario_id}


@dataclass(frozen=True, slots=True)
class FeedEpochTransitionDiagnosticV1:
    """One row-free transition scenario diagnostic cell."""

    split: FeedEpochTransitionSplit
    scenario_id: str
    observation_scenario_id: str
    path_seed: int
    metric_values: Mapping[str, float]
    status: FeedEpochTransitionDiagnosticStatus = (
        FeedEpochTransitionDiagnosticStatus.COMPLETED
    )
    limitation: str | None = None
    diagnostic_id: str = ""
    schema_version: str = FEED_EPOCH_TRANSITION_DIAGNOSTIC_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, FEED_EPOCH_TRANSITION_DIAGNOSTIC_SCHEMA_VERSION
        )
        object.__setattr__(self, "split", FeedEpochTransitionSplit(self.split))
        object.__setattr__(
            self, "scenario_id", _required_text(self.scenario_id)
        )
        object.__setattr__(
            self,
            "observation_scenario_id",
            _required_text(self.observation_scenario_id),
        )
        seed = _nonnegative_int(self.path_seed, "path_seed")
        if seed > (1 << 64) - 1:
            raise ValueError("transition path seed exceeds uint64")
        object.__setattr__(self, "path_seed", seed)
        status = FeedEpochTransitionDiagnosticStatus(self.status)
        object.__setattr__(self, "status", status)
        values = {
            _required_text(name): _finite_float(value, str(name))
            for name, value in self.metric_values.items()
        }
        if status is FeedEpochTransitionDiagnosticStatus.COMPLETED:
            if set(values) != set(FEED_EPOCH_TRANSITION_METRIC_NAMES):
                raise ValueError(
                    "completed transition diagnostic metrics differ"
                )
            if self.limitation is not None:
                raise ValueError(
                    "completed transition diagnostic has limitation"
                )
        elif not _optional_text(self.limitation):
            raise ValueError(
                "incomplete transition diagnostic lacks limitation"
            )
        object.__setattr__(self, "metric_values", dict(sorted(values.items())))
        object.__setattr__(self, "limitation", _optional_text(self.limitation))
        expected = _stable_id(
            "feed-epoch-transition-diagnostic", self.payload()
        )
        supplied = _optional_text(self.diagnostic_id)
        if supplied is not None and supplied != expected:
            raise ValueError("feed-epoch transition diagnostic_id differs")
        object.__setattr__(self, "diagnostic_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "split": self.split.value,
            "scenario_id": self.scenario_id,
            "observation_scenario_id": self.observation_scenario_id,
            "path_seed": self.path_seed,
            "metric_values": dict(self.metric_values),
            "status": self.status.value,
            "limitation": self.limitation,
            "rows_exposed": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "diagnostic_id": self.diagnostic_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> FeedEpochTransitionDiagnosticV1:
        _require_derived(data, "rows_exposed", False)
        return cls(
            split=FeedEpochTransitionSplit(str(data.get("split", ""))),
            scenario_id=str(data.get("scenario_id", "")),
            observation_scenario_id=str(
                data.get("observation_scenario_id", "")
            ),
            path_seed=_strict_int(data.get("path_seed"), "path_seed"),
            metric_values={
                str(name): _finite_float(value, str(name))
                for name, value in _mapping(
                    data.get("metric_values"), "metric_values"
                ).items()
            },
            status=FeedEpochTransitionDiagnosticStatus(
                str(data.get("status", ""))
            ),
            limitation=_optional_text(data.get("limitation")),
            diagnostic_id=str(data.get("diagnostic_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class FeedEpochTransitionReportV1:
    """Validation/holdout sensitivity and certification decision."""

    policy_id: str
    scenario_ids: tuple[str, ...]
    observation_scenario_ids: tuple[str, ...]
    diagnostics: tuple[FeedEpochTransitionDiagnosticV1, ...]
    metric_max_absolute_differences: Mapping[str, float]
    metric_max_relative_differences: Mapping[str, float]
    decision: FeedEpochTransitionDecision
    material_metrics: tuple[str, ...]
    certification_state: str
    limitations: tuple[str, ...]
    report_id: str = ""
    schema_version: str = FEED_EPOCH_TRANSITION_REPORT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, FEED_EPOCH_TRANSITION_REPORT_SCHEMA_VERSION
        )
        object.__setattr__(self, "policy_id", _required_text(self.policy_id))
        scenarios = _text_tuple(self.scenario_ids, "scenario_ids")
        if len(scenarios) != len(TRANSITION_SCENARIO_ORDER):
            raise ValueError("transition report scenario coverage differs")
        object.__setattr__(self, "scenario_ids", scenarios)
        observation_scenarios = _text_tuple(
            self.observation_scenario_ids, "observation_scenario_ids"
        )
        if len(observation_scenarios) != 3:
            raise ValueError(
                "transition report requires three observation scenarios"
            )
        object.__setattr__(
            self, "observation_scenario_ids", observation_scenarios
        )
        diagnostics = tuple(self.diagnostics)
        if not diagnostics:
            raise ValueError("transition report diagnostics are empty")
        object.__setattr__(self, "diagnostics", diagnostics)
        absolute = _metric_mapping(
            self.metric_max_absolute_differences,
            "metric_max_absolute_differences",
        )
        relative = _metric_mapping(
            self.metric_max_relative_differences,
            "metric_max_relative_differences",
        )
        object.__setattr__(self, "metric_max_absolute_differences", absolute)
        object.__setattr__(self, "metric_max_relative_differences", relative)
        decision = FeedEpochTransitionDecision(self.decision)
        object.__setattr__(self, "decision", decision)
        material = _text_tuple(
            self.material_metrics, "material_metrics", allow_empty=True
        )
        if not set(material).issubset(FEED_EPOCH_TRANSITION_METRIC_NAMES):
            raise ValueError("transition report material metric is unknown")
        object.__setattr__(self, "material_metrics", material)
        certification = _required_text(self.certification_state)
        expected_certification = {
            FeedEpochTransitionDecision.LINEAR_RETAINED: (
                "qualified_linear_sensitivity_negligible"
            ),
            FeedEpochTransitionDecision.MULTIPLE_SCENARIOS_REQUIRED: (
                "qualified_multiple_transition_scenarios_required"
            ),
            FeedEpochTransitionDecision.LIMITED_OR_REFUSED: (
                "transition_support_limited_or_refused"
            ),
        }[decision]
        if certification != expected_certification:
            raise ValueError("transition certification state differs")
        object.__setattr__(self, "certification_state", certification)
        limitations = _text_tuple(
            self.limitations, "limitations", allow_empty=True
        )
        if decision is FeedEpochTransitionDecision.LIMITED_OR_REFUSED:
            if not limitations:
                raise ValueError("limited transition report lacks limitation")
        elif limitations:
            raise ValueError("qualified transition report has limitations")
        object.__setattr__(self, "limitations", limitations)
        expected = _stable_id("feed-epoch-transition-report", self.payload())
        supplied = _optional_text(self.report_id)
        if supplied is not None and supplied != expected:
            raise ValueError("feed-epoch transition report_id differs")
        object.__setattr__(self, "report_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "feed_epoch_transition_policy_id": self.policy_id,
            "scenario_ids": list(self.scenario_ids),
            "observation_scenario_ids": list(self.observation_scenario_ids),
            "diagnostics": [item.to_dict() for item in self.diagnostics],
            "metric_max_absolute_differences": dict(
                self.metric_max_absolute_differences
            ),
            "metric_max_relative_differences": dict(
                self.metric_max_relative_differences
            ),
            "decision": self.decision.value,
            "material_metrics": list(self.material_metrics),
            "certification_state": self.certification_state,
            "limitations": list(self.limitations),
            "final_holdout_selection_role": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "report_id": self.report_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> FeedEpochTransitionReportV1:
        _require_derived(data, "final_holdout_selection_role", False)
        return cls(
            policy_id=str(data.get("feed_epoch_transition_policy_id", "")),
            scenario_ids=tuple(
                str(item)
                for item in _sequence(data.get("scenario_ids"), "scenario_ids")
            ),
            observation_scenario_ids=tuple(
                str(item)
                for item in _sequence(
                    data.get("observation_scenario_ids"),
                    "observation_scenario_ids",
                )
            ),
            diagnostics=tuple(
                FeedEpochTransitionDiagnosticV1.from_dict(item)
                for item in _mapping_sequence(
                    data.get("diagnostics"), "diagnostics"
                )
            ),
            metric_max_absolute_differences={
                str(name): _finite_float(value, str(name))
                for name, value in _mapping(
                    data.get("metric_max_absolute_differences"),
                    "metric_max_absolute_differences",
                ).items()
            },
            metric_max_relative_differences={
                str(name): _finite_float(value, str(name))
                for name, value in _mapping(
                    data.get("metric_max_relative_differences"),
                    "metric_max_relative_differences",
                ).items()
            },
            decision=FeedEpochTransitionDecision(str(data.get("decision", ""))),
            material_metrics=tuple(
                str(item)
                for item in _sequence(
                    data.get("material_metrics"), "material_metrics"
                )
            ),
            certification_state=str(data.get("certification_state", "")),
            limitations=tuple(
                str(item)
                for item in _sequence(data.get("limitations"), "limitations")
            ),
            report_id=str(data.get("report_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> FeedEpochTransitionReportV1:
        return cls.from_dict(_json_mapping(text))


def build_feed_epoch_transition_scenario(
    policy: FeedEpochTransitionPolicyV1,
    kind: FeedEpochTransitionScenarioKind,
    *,
    observation_operator_id: str,
    feed_epoch_definition_id: str,
    feed_epoch_id: str,
    transition_boundary_id: str,
    transition_start_ns: int,
    transition_end_ns: int,
    transition_left_epoch_id: str,
    transition_right_epoch_id: str,
    symbol_scope: Sequence[str],
    information_mode: str,
    left_stratum_ids: Sequence[str],
    right_stratum_ids: Sequence[str],
    operator_evidence_ids: Sequence[str],
    linear_right_weight: float,
) -> FeedEpochTransitionScenarioV1:
    """Create one deterministic scenario from adjacent fitted evidence."""
    if not isinstance(policy, FeedEpochTransitionPolicyV1):
        raise TypeError("transition policy must use v1")
    selected = FeedEpochTransitionScenarioKind(kind)
    linear = _unit_float(linear_right_weight, "linear_right_weight")
    left, right = {
        FeedEpochTransitionScenarioKind.LEFT_PERSISTENCE: (1.0, 0.0),
        FeedEpochTransitionScenarioKind.LINEAR_BRIDGE: (1.0 - linear, linear),
        FeedEpochTransitionScenarioKind.EARLY_RIGHT_ADOPTION: (0.0, 1.0),
    }[selected]
    return FeedEpochTransitionScenarioV1(
        kind=selected,
        policy_id=policy.policy_id,
        observation_operator_id=observation_operator_id,
        feed_epoch_definition_id=feed_epoch_definition_id,
        feed_epoch_id=feed_epoch_id,
        transition_boundary_id=transition_boundary_id,
        transition_start_ns=transition_start_ns,
        transition_end_ns=transition_end_ns,
        transition_left_epoch_id=transition_left_epoch_id,
        transition_right_epoch_id=transition_right_epoch_id,
        symbol_scope=tuple(symbol_scope),
        information_mode=information_mode,
        left_stratum_ids=tuple(left_stratum_ids),
        right_stratum_ids=tuple(right_stratum_ids),
        operator_evidence_ids=tuple(operator_evidence_ids),
        linear_right_weight=linear,
        left_weight=left,
        right_weight=right,
    )


def transition_crossed_member_count(
    policy: FeedEpochTransitionPolicyV1,
    *,
    observation_scenario_count: int,
) -> int:
    """Return required members for independent transition/observation axes."""
    if not isinstance(policy, FeedEpochTransitionPolicyV1):
        raise TypeError("transition policy must use v1")
    observed = _positive_int(
        observation_scenario_count, "observation_scenario_count"
    )
    return (
        len(policy.scenario_order)
        * observed
        * policy.minimum_path_realizations_per_crossed_cell
    )


def transition_scenario_kind_for_member(
    policy: FeedEpochTransitionPolicyV1,
    *,
    member_ordinal: int,
    observation_scenario_count: int,
) -> FeedEpochTransitionScenarioKind:
    """Assign transition blocks while observation scenarios cycle within them."""
    if not isinstance(policy, FeedEpochTransitionPolicyV1):
        raise TypeError("transition policy must use v1")
    ordinal = _positive_int(member_ordinal, "member_ordinal")
    observed = _positive_int(
        observation_scenario_count, "observation_scenario_count"
    )
    block = ((ordinal - 1) // observed) % len(policy.scenario_order)
    return policy.scenario_order[block]


def evaluate_feed_epoch_transition(
    policy: FeedEpochTransitionPolicyV1,
    scenarios: Sequence[FeedEpochTransitionScenarioV1],
    diagnostics: Sequence[FeedEpochTransitionDiagnosticV1],
) -> FeedEpochTransitionReportV1:
    """Compare endpoint scenarios with linear on validation and holdout."""
    if not isinstance(policy, FeedEpochTransitionPolicyV1):
        raise TypeError("transition policy must use v1")
    scenario_values = tuple(scenarios)
    if tuple(item.kind for item in scenario_values) != policy.scenario_order:
        raise ValueError("transition scenario registry differs from policy")
    if any(item.policy_id != policy.policy_id for item in scenario_values):
        raise ValueError("transition scenario policy lineage differs")
    cells = tuple(diagnostics)
    scenario_by_id = {item.scenario_id: item for item in scenario_values}
    completed = tuple(
        item
        for item in cells
        if item.status is FeedEpochTransitionDiagnosticStatus.COMPLETED
    )
    observation_scenario_ids = tuple(
        sorted({item.observation_scenario_id for item in cells})
    )
    if len(observation_scenario_ids) != 3:
        raise ValueError(
            "transition evaluation requires three observation scenarios"
        )
    required_cells = {
        (split, scenario.scenario_id, observation_scenario_id)
        for split in FeedEpochTransitionSplit
        for scenario in scenario_values
        for observation_scenario_id in observation_scenario_ids
    }
    completed_cells = {
        (item.split, item.scenario_id, item.observation_scenario_id)
        for item in completed
    }
    limitations = tuple(
        sorted(
            {
                item.limitation or "transition diagnostic incomplete"
                for item in cells
                if item.status
                is not FeedEpochTransitionDiagnosticStatus.COMPLETED
            }
        )
    )
    if not {item.scenario_id for item in cells}.issubset(scenario_by_id):
        raise ValueError("transition diagnostic scenario lineage differs")
    complete_path_support = all(
        len(
            {
                item.path_seed
                for item in completed
                if (
                    item.split,
                    item.scenario_id,
                    item.observation_scenario_id,
                )
                == cell
            }
        )
        >= policy.minimum_path_realizations_per_crossed_cell
        for cell in required_cells
    )
    if completed_cells != required_cells or not complete_path_support:
        empty = {name: 0.0 for name in FEED_EPOCH_TRANSITION_METRIC_NAMES}
        return FeedEpochTransitionReportV1(
            policy_id=policy.policy_id,
            scenario_ids=tuple(item.scenario_id for item in scenario_values),
            observation_scenario_ids=observation_scenario_ids,
            diagnostics=cells,
            metric_max_absolute_differences=empty,
            metric_max_relative_differences=empty,
            decision=FeedEpochTransitionDecision.LIMITED_OR_REFUSED,
            material_metrics=(),
            certification_state="transition_support_limited_or_refused",
            limitations=limitations
            or ("incomplete transition scenario support",),
        )
    linear_id = next(
        item.scenario_id
        for item in scenario_values
        if item.kind is FeedEpochTransitionScenarioKind.LINEAR_BRIDGE
    )
    absolute: dict[str, float] = {}
    relative: dict[str, float] = {}
    for metric in FEED_EPOCH_TRANSITION_METRIC_NAMES:
        max_absolute = 0.0
        max_relative = 0.0
        for split in FeedEpochTransitionSplit:
            for observation_scenario_id in observation_scenario_ids:
                linear_values = [
                    item.metric_values[metric]
                    for item in completed
                    if item.split is split
                    and item.scenario_id == linear_id
                    and item.observation_scenario_id == observation_scenario_id
                ]
                if not linear_values:
                    raise ValueError(
                        "transition linear diagnostic support is absent"
                    )
                baseline = sum(linear_values) / len(linear_values)
                for scenario in scenario_values:
                    compared = [
                        item.metric_values[metric]
                        for item in completed
                        if item.split is split
                        and item.scenario_id == scenario.scenario_id
                        and item.observation_scenario_id
                        == observation_scenario_id
                    ]
                    if not compared:
                        raise ValueError(
                            "transition scenario diagnostic support is absent"
                        )
                    difference = abs(sum(compared) / len(compared) - baseline)
                    max_absolute = max(max_absolute, difference)
                    denominator = max(
                        abs(baseline), policy.absolute_materiality_tolerance
                    )
                    max_relative = max(max_relative, difference / denominator)
        absolute[metric] = max_absolute
        relative[metric] = max_relative
    material = tuple(
        metric
        for metric in FEED_EPOCH_TRANSITION_METRIC_NAMES
        if absolute[metric] > policy.absolute_materiality_tolerance
        and relative[metric] > policy.relative_materiality_tolerance
    )
    decision = (
        FeedEpochTransitionDecision.MULTIPLE_SCENARIOS_REQUIRED
        if material
        else FeedEpochTransitionDecision.LINEAR_RETAINED
    )
    certification = (
        "qualified_multiple_transition_scenarios_required"
        if material
        else "qualified_linear_sensitivity_negligible"
    )
    return FeedEpochTransitionReportV1(
        policy_id=policy.policy_id,
        scenario_ids=tuple(item.scenario_id for item in scenario_values),
        observation_scenario_ids=observation_scenario_ids,
        diagnostics=cells,
        metric_max_absolute_differences=absolute,
        metric_max_relative_differences=relative,
        decision=decision,
        material_metrics=material,
        certification_state=certification,
        limitations=(),
    )


def write_feed_epoch_transition_policy(
    policy: FeedEpochTransitionPolicyV1, output_directory: str | Path
) -> ArtifactRef:
    ref = _write_contract(
        policy.to_json(),
        output_directory,
        prefix="feed-epoch-transition-policy",
        kind=FEED_EPOCH_TRANSITION_POLICY_ARTIFACT_KIND,
        metadata={"policy_id": policy.policy_id},
    )
    if read_feed_epoch_transition_policy(ref.path) != policy:
        raise ValueError("published feed-epoch transition policy differs")
    return ref


def read_feed_epoch_transition_policy(
    path: str | Path,
) -> FeedEpochTransitionPolicyV1:
    return FeedEpochTransitionPolicyV1.from_dict(
        _read_content_addressed_json(path, "feed-epoch-transition-policy")
    )


def write_feed_epoch_transition_report(
    report: FeedEpochTransitionReportV1, output_directory: str | Path
) -> ArtifactRef:
    ref = _write_contract(
        report.to_json(),
        output_directory,
        prefix="feed-epoch-transition-report",
        kind=FEED_EPOCH_TRANSITION_REPORT_ARTIFACT_KIND,
        metadata={"report_id": report.report_id, "policy_id": report.policy_id},
    )
    if read_feed_epoch_transition_report(ref.path) != report:
        raise ValueError("published feed-epoch transition report differs")
    return ref


def read_feed_epoch_transition_report(
    path: str | Path,
) -> FeedEpochTransitionReportV1:
    return FeedEpochTransitionReportV1.from_dict(
        _read_content_addressed_json(path, "feed-epoch-transition-report")
    )


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
    if len(payload) > MAX_TRANSITION_ARTIFACT_BYTES:
        raise ValueError("feed-epoch transition artifact exceeds size limit")
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
                raise ValueError("feed-epoch transition artifact collision")
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
    if len(payload) > MAX_TRANSITION_ARTIFACT_BYTES:
        raise ValueError("feed-epoch transition artifact exceeds size limit")
    digest = hashlib.sha256(payload).hexdigest()
    if selected.name != f"{prefix}-{digest}.json":
        raise ValueError("feed-epoch transition artifact filename differs")
    ref = artifact_ref_for_file(selected, kind=prefix.replace("-", "_") + "_v1")
    verify_artifact_ref(ref)
    loaded = json.loads(payload)
    if not isinstance(loaded, Mapping):
        raise TypeError("feed-epoch transition artifact must contain an object")
    return loaded


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _metric_mapping(value: Mapping[str, float], name: str) -> dict[str, float]:
    selected = {
        _required_text(key): _nonnegative_float(item, name)
        for key, item in value.items()
    }
    if set(selected) != set(FEED_EPOCH_TRANSITION_METRIC_NAMES):
        raise ValueError(f"{name} metrics differ")
    return dict(sorted(selected.items()))


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


def _unit_float(value: Any, name: str) -> float:
    selected = _finite_float(value, name)
    if not 0.0 <= selected <= 1.0:
        raise ValueError(f"{name} must be in [0, 1]")
    return selected


def _require_schema(value: str, expected: str) -> None:
    if value != expected:
        raise ValueError(f"unsupported schema version: {value}")


__all__ = [
    "FEED_EPOCH_TRANSITION_DIAGNOSTIC_SCHEMA_VERSION",
    "FEED_EPOCH_TRANSITION_METRIC_NAMES",
    "FEED_EPOCH_TRANSITION_POLICY_ARTIFACT_KIND",
    "FEED_EPOCH_TRANSITION_POLICY_SCHEMA_VERSION",
    "FEED_EPOCH_TRANSITION_REPORT_ARTIFACT_KIND",
    "FEED_EPOCH_TRANSITION_REPORT_SCHEMA_VERSION",
    "FEED_EPOCH_TRANSITION_SCENARIO_SCHEMA_VERSION",
    "TRANSITION_SCENARIO_ORDER",
    "FeedEpochTransitionDecision",
    "FeedEpochTransitionDiagnosticStatus",
    "FeedEpochTransitionDiagnosticV1",
    "FeedEpochTransitionPolicyV1",
    "FeedEpochTransitionReportV1",
    "FeedEpochTransitionScenarioKind",
    "FeedEpochTransitionScenarioV1",
    "FeedEpochTransitionSplit",
    "build_feed_epoch_transition_scenario",
    "evaluate_feed_epoch_transition",
    "read_feed_epoch_transition_policy",
    "read_feed_epoch_transition_report",
    "transition_crossed_member_count",
    "transition_scenario_kind_for_member",
    "write_feed_epoch_transition_policy",
    "write_feed_epoch_transition_report",
]
