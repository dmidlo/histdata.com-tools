"""Release-critical reconciliation projection-burden diagnostics.

Final cross-currency residuals are measured after reconciliation and therefore
cannot, by themselves, show whether a proposal law produced coherent joint
quotes.  This module retains bounded aggregate evidence for the movement from
each synthetic proposal vector to its reconciled vector.  Raw event rows are
accepted only while deriving the report and are never serialized in it.

The primary scale is frozen before results as the sum of the three proposal
spreads, replacing only a zero spread term with a strictly positive epsilon.
No numerator or burden clipping is permitted.  The resulting event burden is
the L1 bid/ask movement divided by that scale.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from itertools import pairwise
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from histdatacom.runtime_contracts import ArtifactRef, JSONValue
from histdatacom.synthetic.contracts import canonical_contract_json

if TYPE_CHECKING:
    from histdatacom.synthetic.hawkes_selection import (
        HawkesProductSelectionDossierV1,
        HawkesValidationComparisonV1,
    )

PROJECTION_BURDEN_POLICY_SCHEMA_VERSION = (
    "histdatacom.projection-burden-policy.v1"
)
PROJECTION_BURDEN_SCENARIO_SCHEMA_VERSION = (
    "histdatacom.projection-burden-scenario.v1"
)
PROJECTION_BURDEN_EVENT_SCHEMA_VERSION = (
    "histdatacom.projection-burden-event.v1"
)
PROJECTION_BURDEN_DISTRIBUTION_SCHEMA_VERSION = (
    "histdatacom.projection-burden-distribution.v1"
)
PROJECTION_BURDEN_SLICE_SCHEMA_VERSION = (
    "histdatacom.projection-burden-slice.v1"
)
PROJECTION_BURDEN_MODEL_DECISION_SCHEMA_VERSION = (
    "histdatacom.projection-burden-model-decision.v1"
)
PROJECTION_BURDEN_MODEL_COMPARISON_SCHEMA_VERSION = (
    "histdatacom.projection-burden-model-comparison.v1"
)
PROJECTION_BURDEN_REPORT_SCHEMA_VERSION = (
    "histdatacom.projection-burden-report.v1"
)
PROJECTION_BURDEN_HAWKES_BINDING_SCHEMA_VERSION = (
    "histdatacom.projection-burden-hawkes-binding.v1"
)
PROJECTION_BURDEN_CONSUMPTION_RECEIPT_SCHEMA_VERSION = (
    "histdatacom.projection-burden-consumption-receipt.v1"
)
PROJECTION_BURDEN_RELEASE_COVERAGE_SCHEMA_VERSION = (
    "histdatacom.projection-burden-release-coverage.v1"
)
PROJECTION_BURDEN_REPORT_ARTIFACT_KIND = "projection-burden-report"
PROJECTION_BURDEN_RELEASE_COVERAGE_ARTIFACT_KIND = (
    "projection-burden-consumption-receipts"
)

TRIANGLE_SYMBOLS = ("eurgbp", "eurusd", "gbpusd")
PRIMARY_SCALE_ID = "combined-triangle-proposal-spread-epsilon.v1"
MIDPOINT_SPREAD_DECOMPOSITION_ID = "shapley-midpoint-spread-l1.v1"
MAX_PROJECTION_EVENTS = 262_144
MAX_PROJECTION_SLICES = 32_768
MAX_PROJECTION_SCENARIOS = 64
MAX_PROJECTION_ARTIFACT_BYTES = 64 * 1024 * 1024
MAX_PROJECTION_RECEIPTS = 16_384


class ProjectionScenarioKind(str, Enum):
    """Whether a scenario estimates product behavior or detection power."""

    BASELINE = "baseline"
    MISSPECIFICATION = "misspecification"


class ProjectionBurdenSliceKind(str, Enum):
    """Predeclared aggregate views required for publication."""

    GLOBAL_MODEL = "global_model"
    WINDOW_MEMBER_MODEL = "window_member_model"
    VALIDATION_COORDINATE = "validation_coordinate"
    ERA = "era"
    SESSION = "session"
    EVENT_STATE = "event_state"
    ALIGNMENT = "alignment"
    SCENARIO = "scenario"
    QUOTE_AGE = "quote_age"


class ProjectionBurdenStatus(str, Enum):
    """Release consequence of projection evidence."""

    PASS = "pass"
    LIMITED = "limited"
    FAIL = "fail"


class ProjectionComparisonConclusion(str, Enum):
    """Comparator-relative conclusion frozen before release evidence."""

    EQUIVALENT = "within_predeclared_ratio"
    LEFT_EXCESSIVE = "left_excessive"
    RIGHT_EXCESSIVE = "right_excessive"


class ProjectionBurdenConsumerKind(str, Enum):
    """Release surfaces that must consume projection evidence."""

    PRODUCT_MANIFEST = "product_manifest"
    CAMPAIGN_SHARD_SUMMARY = "campaign_shard_summary"
    ERA_AUDIT = "era_audit"
    CERTIFICATION = "certification"
    HAWKES_SELECTION = "hawkes_selection"


REQUIRED_RELEASE_CONSUMERS = frozenset(ProjectionBurdenConsumerKind)


def _hawkes_selection_engine_ids() -> tuple[str, str]:
    """Load #508 engine identities without creating a package import cycle."""
    from histdatacom.synthetic.hawkes_selection import (
        HAWKES_SELECTION_ENGINE_IDS,
    )

    return (
        str(HAWKES_SELECTION_ENGINE_IDS[0]),
        str(HAWKES_SELECTION_ENGINE_IDS[1]),
    )


@dataclass(frozen=True, slots=True)
class ProjectionBurdenPolicyV1:
    """Predeclared primary scale, thresholds, and negative-control surface."""

    reconciliation_config_id: str
    alignment_policy_id: str
    spread_epsilon: float = 1e-9
    synthetic_post_residual_tolerance: float = 1e-8
    advisory_mean_burden: float = 0.20
    advisory_p90_burden: float = 0.50
    advisory_p99_burden: float = 1.00
    advisory_max_burden: float = 2.00
    advisory_projected_rate: float = 0.50
    hard_mean_burden: float = 0.50
    hard_p90_burden: float = 1.00
    hard_p99_burden: float = 2.00
    hard_max_burden: float = 5.00
    hard_projected_rate: float = 0.90
    maximum_comparator_burden_ratio: float = 1.25
    comparison_scale_floor: float = 1e-12
    misspecification_detection_minimum_mean_burden: float = 0.50
    minimum_proposals_per_model: int = 1
    quote_age_bin_edges_ns: tuple[int, ...] = (
        0,
        1_000_000,
        10_000_000,
        100_000_000,
    )
    required_misspecification_scenario_ids: tuple[str, ...] = ()
    policy_id: str = ""
    schema_version: str = PROJECTION_BURDEN_POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, PROJECTION_BURDEN_POLICY_SCHEMA_VERSION
        )
        for name in ("reconciliation_config_id", "alignment_policy_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        for name in (
            "spread_epsilon",
            "comparison_scale_floor",
            "maximum_comparator_burden_ratio",
            "misspecification_detection_minimum_mean_burden",
        ):
            object.__setattr__(
                self, name, _positive_float(getattr(self, name), name)
            )
        object.__setattr__(
            self,
            "synthetic_post_residual_tolerance",
            _nonnegative_float(
                self.synthetic_post_residual_tolerance,
                "synthetic_post_residual_tolerance",
            ),
        )
        for name in (
            "advisory_mean_burden",
            "advisory_p90_burden",
            "advisory_p99_burden",
            "advisory_max_burden",
            "hard_mean_burden",
            "hard_p90_burden",
            "hard_p99_burden",
            "hard_max_burden",
        ):
            object.__setattr__(
                self, name, _nonnegative_float(getattr(self, name), name)
            )
        for advisory, hard in (
            (self.advisory_mean_burden, self.hard_mean_burden),
            (self.advisory_p90_burden, self.hard_p90_burden),
            (self.advisory_p99_burden, self.hard_p99_burden),
            (self.advisory_max_burden, self.hard_max_burden),
        ):
            if advisory > hard:
                raise ValueError(
                    "projection advisory burden exceeds hard burden"
                )
        for name in ("advisory_projected_rate", "hard_projected_rate"):
            value = _finite_float(getattr(self, name), name)
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be in [0, 1]")
            object.__setattr__(self, name, value)
        if self.advisory_projected_rate > self.hard_projected_rate:
            raise ValueError("projection advisory rate exceeds hard rate")
        if self.maximum_comparator_burden_ratio <= 1.0:
            raise ValueError("projection comparator ratio must exceed one")
        if (
            isinstance(self.minimum_proposals_per_model, bool)
            or not isinstance(self.minimum_proposals_per_model, int)
            or not 1
            <= self.minimum_proposals_per_model
            <= MAX_PROJECTION_EVENTS
        ):
            raise ValueError("projection proposal minimum is invalid")
        edges = tuple(
            _strict_int(item, "quote_age_bin_edge")
            for item in self.quote_age_bin_edges_ns
        )
        if not edges or edges[0] != 0 or any(item < 0 for item in edges):
            raise ValueError("projection quote-age bins must begin at zero")
        if any(left >= right for left, right in pairwise(edges)):
            raise ValueError(
                "projection quote-age bin edges are not increasing"
            )
        object.__setattr__(self, "quote_age_bin_edges_ns", edges)
        required = _normalized_text_tuple(
            self.required_misspecification_scenario_ids
        )
        if not required or len(required) > MAX_PROJECTION_SCENARIOS:
            raise ValueError(
                "projection policy requires misspecification scenarios"
            )
        object.__setattr__(
            self, "required_misspecification_scenario_ids", required
        )
        expected = _stable_id("projection-burden-policy", self.payload())
        if self.policy_id and self.policy_id != expected:
            raise ValueError("projection burden policy identity differs")
        object.__setattr__(self, "policy_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reconciliation_config_id": self.reconciliation_config_id,
            "alignment_policy_id": self.alignment_policy_id,
            "primary_scale": {
                "scale_id": PRIMARY_SCALE_ID,
                "definition": "sum_symbol_max_proposal_ask_minus_bid_epsilon",
                "spread_epsilon": self.spread_epsilon,
                "strictly_positive": True,
                "clipping_permitted": False,
                "zero_spread_treatment": "replace_only_zero_symbol_spread_with_epsilon",
            },
            "midpoint_spread_decomposition_id": MIDPOINT_SPREAD_DECOMPOSITION_ID,
            "synthetic_post_residual_tolerance": self.synthetic_post_residual_tolerance,
            "advisory_thresholds": {
                "mean": self.advisory_mean_burden,
                "p90": self.advisory_p90_burden,
                "p99": self.advisory_p99_burden,
                "maximum": self.advisory_max_burden,
                "projected_rate": self.advisory_projected_rate,
            },
            "hard_thresholds": {
                "mean": self.hard_mean_burden,
                "p90": self.hard_p90_burden,
                "p99": self.hard_p99_burden,
                "maximum": self.hard_max_burden,
                "projected_rate": self.hard_projected_rate,
            },
            "maximum_comparator_burden_ratio": self.maximum_comparator_burden_ratio,
            "comparison_scale_floor": self.comparison_scale_floor,
            "misspecification_detection_minimum_mean_burden": (
                self.misspecification_detection_minimum_mean_burden
            ),
            "minimum_proposals_per_model": self.minimum_proposals_per_model,
            "quote_age_bin_edges_ns": list(self.quote_age_bin_edges_ns),
            "required_misspecification_scenario_ids": list(
                self.required_misspecification_scenario_ids
            ),
            "validation_only_thresholds": True,
            "final_residual_alone_sufficient": False,
            "observed_only_residual_enters_burden": False,
            "synthetic_post_residual_failure_blocking": True,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "policy_id": self.policy_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ProjectionBurdenPolicyV1:
        _require_policy_constants(data)
        advisory = _mapping(
            data.get("advisory_thresholds"), "advisory_thresholds"
        )
        hard = _mapping(data.get("hard_thresholds"), "hard_thresholds")
        scale = _mapping(data.get("primary_scale"), "primary_scale")
        return cls(
            reconciliation_config_id=str(
                data.get("reconciliation_config_id", "")
            ),
            alignment_policy_id=str(data.get("alignment_policy_id", "")),
            spread_epsilon=_finite_float(
                scale.get("spread_epsilon"), "spread_epsilon"
            ),
            synthetic_post_residual_tolerance=_finite_float(
                data.get("synthetic_post_residual_tolerance"),
                "synthetic_post_residual_tolerance",
            ),
            advisory_mean_burden=_finite_float(
                advisory.get("mean"), "advisory.mean"
            ),
            advisory_p90_burden=_finite_float(
                advisory.get("p90"), "advisory.p90"
            ),
            advisory_p99_burden=_finite_float(
                advisory.get("p99"), "advisory.p99"
            ),
            advisory_max_burden=_finite_float(
                advisory.get("maximum"), "advisory.maximum"
            ),
            advisory_projected_rate=_finite_float(
                advisory.get("projected_rate"), "advisory.projected_rate"
            ),
            hard_mean_burden=_finite_float(hard.get("mean"), "hard.mean"),
            hard_p90_burden=_finite_float(hard.get("p90"), "hard.p90"),
            hard_p99_burden=_finite_float(hard.get("p99"), "hard.p99"),
            hard_max_burden=_finite_float(hard.get("maximum"), "hard.maximum"),
            hard_projected_rate=_finite_float(
                hard.get("projected_rate"), "hard.projected_rate"
            ),
            maximum_comparator_burden_ratio=_finite_float(
                data.get("maximum_comparator_burden_ratio"),
                "maximum_comparator_burden_ratio",
            ),
            comparison_scale_floor=_finite_float(
                data.get("comparison_scale_floor"), "comparison_scale_floor"
            ),
            misspecification_detection_minimum_mean_burden=_finite_float(
                data.get("misspecification_detection_minimum_mean_burden"),
                "misspecification_detection_minimum_mean_burden",
            ),
            minimum_proposals_per_model=_strict_int(
                data.get("minimum_proposals_per_model"),
                "minimum_proposals_per_model",
            ),
            quote_age_bin_edges_ns=tuple(
                _strict_int(item, "quote_age_bin_edge")
                for item in _sequence(
                    data.get("quote_age_bin_edges_ns"),
                    "quote_age_bin_edges_ns",
                )
            ),
            required_misspecification_scenario_ids=_string_tuple(
                data.get("required_misspecification_scenario_ids"),
                "required_misspecification_scenario_ids",
            ),
            policy_id=str(data.get("policy_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ProjectionBurdenScenarioV1:
    """One baseline or intentionally incoherent validation scenario."""

    scenario_id: str
    scenario_kind: ProjectionScenarioKind
    description: str
    intentionally_cross_series_incoherent: bool
    incoherence_strength: float
    definition_content_sha256: str
    schema_version: str = PROJECTION_BURDEN_SCENARIO_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, PROJECTION_BURDEN_SCENARIO_SCHEMA_VERSION
        )
        object.__setattr__(
            self, "scenario_id", _required_text(self.scenario_id)
        )
        object.__setattr__(
            self, "scenario_kind", ProjectionScenarioKind(self.scenario_kind)
        )
        object.__setattr__(
            self, "description", _required_text(self.description)
        )
        if not isinstance(self.intentionally_cross_series_incoherent, bool):
            raise TypeError(
                "projection scenario incoherence flag must be boolean"
            )
        strength = _nonnegative_float(
            self.incoherence_strength, "incoherence_strength"
        )
        object.__setattr__(self, "incoherence_strength", strength)
        object.__setattr__(
            self,
            "definition_content_sha256",
            _sha256(
                self.definition_content_sha256, "definition_content_sha256"
            ),
        )
        expected = self.scenario_kind is ProjectionScenarioKind.MISSPECIFICATION
        if self.intentionally_cross_series_incoherent is not expected:
            raise ValueError(
                "projection scenario kind and incoherence flag differ"
            )
        if expected != (strength > 0.0):
            raise ValueError("projection scenario incoherence strength differs")

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "scenario_id": self.scenario_id,
            "scenario_kind": self.scenario_kind.value,
            "description": self.description,
            "intentionally_cross_series_incoherent": (
                self.intentionally_cross_series_incoherent
            ),
            "incoherence_strength": self.incoherence_strength,
            "definition_content_sha256": self.definition_content_sha256,
            "expected_detection": (
                "projection_burden_or_hard_refusal"
                if self.scenario_kind is ProjectionScenarioKind.MISSPECIFICATION
                else "not_applicable"
            ),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ProjectionBurdenScenarioV1:
        kind = ProjectionScenarioKind(str(data.get("scenario_kind", "")))
        expected_detection = (
            "projection_burden_or_hard_refusal"
            if kind is ProjectionScenarioKind.MISSPECIFICATION
            else "not_applicable"
        )
        if data.get("expected_detection") != expected_detection:
            raise ValueError("projection scenario expected detection differs")
        return cls(
            scenario_id=str(data.get("scenario_id", "")),
            scenario_kind=kind,
            description=str(data.get("description", "")),
            intentionally_cross_series_incoherent=_strict_bool(
                data.get("intentionally_cross_series_incoherent"),
                "intentionally_cross_series_incoherent",
            ),
            incoherence_strength=_finite_float(
                data.get("incoherence_strength"), "incoherence_strength"
            ),
            definition_content_sha256=str(
                data.get("definition_content_sha256", "")
            ),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ProjectionBurdenEventV1:
    """Ephemeral pre/post lineage for one proposal or observed-only residual."""

    event_id: str
    window_id: str
    ensemble_member_id: str
    model_id: str
    model_family: str
    validation_coordinate_id: str
    event_time_ns: int
    era: str
    session: str
    event_state: str
    alignment: str
    scenario_id: str
    quote_age_ns: int
    pre_projection_quotes: Mapping[str, tuple[float, float]]
    post_projection_quotes: Mapping[str, tuple[float, float]]
    pre_projection_triangle_residual: float
    post_projection_triangle_residual: float
    projection_priority_leg: str
    refused_by_hard_limit: bool
    refusal_reason: str | None
    observed_only: bool
    path_metric_pre: float
    path_metric_post: float
    spread_metric_pre: float
    spread_metric_post: float
    source_content_sha256: str
    reconciliation_config_id: str
    alignment_policy_id: str
    event_content_sha256: str = ""
    schema_version: str = PROJECTION_BURDEN_EVENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, PROJECTION_BURDEN_EVENT_SCHEMA_VERSION
        )
        for name in (
            "event_id",
            "window_id",
            "ensemble_member_id",
            "model_id",
            "model_family",
            "validation_coordinate_id",
            "era",
            "session",
            "event_state",
            "alignment",
            "scenario_id",
            "reconciliation_config_id",
            "alignment_policy_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self,
            "event_time_ns",
            _strict_int(self.event_time_ns, "event_time_ns"),
        )
        age = _strict_int(self.quote_age_ns, "quote_age_ns")
        if age < 0:
            raise ValueError("projection quote age must be nonnegative")
        object.__setattr__(self, "quote_age_ns", age)
        pre = _quotes(self.pre_projection_quotes, "pre_projection_quotes")
        post = _quotes(self.post_projection_quotes, "post_projection_quotes")
        object.__setattr__(self, "pre_projection_quotes", pre)
        object.__setattr__(self, "post_projection_quotes", post)
        for name in (
            "pre_projection_triangle_residual",
            "post_projection_triangle_residual",
            "path_metric_pre",
            "path_metric_post",
            "spread_metric_pre",
            "spread_metric_post",
        ):
            value = _finite_float(getattr(self, name), name)
            if "residual" in name and value < 0.0:
                raise ValueError(
                    "projection triangle residual must be nonnegative"
                )
            object.__setattr__(self, name, value)
        priority = _required_text(self.projection_priority_leg)
        if priority not in {*TRIANGLE_SYMBOLS, "none"}:
            raise ValueError("projection priority leg is unsupported")
        object.__setattr__(self, "projection_priority_leg", priority)
        if not isinstance(self.refused_by_hard_limit, bool) or not isinstance(
            self.observed_only, bool
        ):
            raise TypeError("projection event flags must be boolean")
        reason = _optional_text(self.refusal_reason)
        if self.refused_by_hard_limit != (reason is not None):
            raise ValueError("projection refusal reason and flag differ")
        object.__setattr__(self, "refusal_reason", reason)
        object.__setattr__(
            self,
            "source_content_sha256",
            _sha256(self.source_content_sha256, "source_content_sha256"),
        )
        if self.observed_only:
            if self.refused_by_hard_limit or priority != "none" or pre != post:
                raise ValueError(
                    "observed-only residual cannot enter projection"
                )
            if not math.isclose(
                self.pre_projection_triangle_residual,
                self.post_projection_triangle_residual,
                rel_tol=0.0,
                abs_tol=0.0,
            ):
                raise ValueError("observed-only residual changed")
        payload = self.payload()
        expected = hashlib.sha256(
            canonical_contract_json(payload).encode("utf-8")
        ).hexdigest()
        if self.event_content_sha256 and self.event_content_sha256 != expected:
            raise ValueError("projection event content hash differs")
        object.__setattr__(self, "event_content_sha256", expected)

    @property
    def projected(self) -> bool:
        return (
            not self.observed_only
            and not self.refused_by_hard_limit
            and any(
                self.pre_projection_quotes[symbol]
                != self.post_projection_quotes[symbol]
                for symbol in TRIANGLE_SYMBOLS
            )
        )

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "window_id": self.window_id,
            "ensemble_member_id": self.ensemble_member_id,
            "model_id": self.model_id,
            "model_family": self.model_family,
            "validation_coordinate_id": self.validation_coordinate_id,
            "event_time_ns": self.event_time_ns,
            "era": self.era,
            "session": self.session,
            "event_state": self.event_state,
            "alignment": self.alignment,
            "scenario_id": self.scenario_id,
            "quote_age_ns": self.quote_age_ns,
            "pre_projection_quotes": {
                key: list(value)
                for key, value in self.pre_projection_quotes.items()
            },
            "post_projection_quotes": {
                key: list(value)
                for key, value in self.post_projection_quotes.items()
            },
            "pre_projection_triangle_residual": self.pre_projection_triangle_residual,
            "post_projection_triangle_residual": self.post_projection_triangle_residual,
            "projection_priority_leg": self.projection_priority_leg,
            "refused_by_hard_limit": self.refused_by_hard_limit,
            "refusal_reason": self.refusal_reason,
            "observed_only": self.observed_only,
            "path_metric_pre": self.path_metric_pre,
            "path_metric_post": self.path_metric_post,
            "spread_metric_pre": self.spread_metric_pre,
            "spread_metric_post": self.spread_metric_post,
            "source_content_sha256": self.source_content_sha256,
            "reconciliation_config_id": self.reconciliation_config_id,
            "alignment_policy_id": self.alignment_policy_id,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.payload(),
            "event_content_sha256": self.event_content_sha256,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ProjectionBurdenEventV1:
        return cls(
            event_id=str(data.get("event_id", "")),
            window_id=str(data.get("window_id", "")),
            ensemble_member_id=str(data.get("ensemble_member_id", "")),
            model_id=str(data.get("model_id", "")),
            model_family=str(data.get("model_family", "")),
            validation_coordinate_id=str(
                data.get("validation_coordinate_id", "")
            ),
            event_time_ns=_strict_int(
                data.get("event_time_ns"), "event_time_ns"
            ),
            era=str(data.get("era", "")),
            session=str(data.get("session", "")),
            event_state=str(data.get("event_state", "")),
            alignment=str(data.get("alignment", "")),
            scenario_id=str(data.get("scenario_id", "")),
            quote_age_ns=_strict_int(data.get("quote_age_ns"), "quote_age_ns"),
            pre_projection_quotes=_quote_mapping(
                data.get("pre_projection_quotes"), "pre_projection_quotes"
            ),
            post_projection_quotes=_quote_mapping(
                data.get("post_projection_quotes"), "post_projection_quotes"
            ),
            pre_projection_triangle_residual=_finite_float(
                data.get("pre_projection_triangle_residual"),
                "pre_projection_triangle_residual",
            ),
            post_projection_triangle_residual=_finite_float(
                data.get("post_projection_triangle_residual"),
                "post_projection_triangle_residual",
            ),
            projection_priority_leg=str(
                data.get("projection_priority_leg", "")
            ),
            refused_by_hard_limit=_strict_bool(
                data.get("refused_by_hard_limit"), "refused_by_hard_limit"
            ),
            refusal_reason=_optional_text(data.get("refusal_reason")),
            observed_only=_strict_bool(
                data.get("observed_only"), "observed_only"
            ),
            path_metric_pre=_finite_float(
                data.get("path_metric_pre"), "path_metric_pre"
            ),
            path_metric_post=_finite_float(
                data.get("path_metric_post"), "path_metric_post"
            ),
            spread_metric_pre=_finite_float(
                data.get("spread_metric_pre"), "spread_metric_pre"
            ),
            spread_metric_post=_finite_float(
                data.get("spread_metric_post"), "spread_metric_post"
            ),
            source_content_sha256=str(data.get("source_content_sha256", "")),
            reconciliation_config_id=str(
                data.get("reconciliation_config_id", "")
            ),
            alignment_policy_id=str(data.get("alignment_policy_id", "")),
            event_content_sha256=str(data.get("event_content_sha256", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ProjectionBurdenDistributionV1:
    """Deterministic nearest-rank distribution summary."""

    count: int
    mean: float
    total: float
    p50: float
    p90: float
    p99: float
    maximum: float
    schema_version: str = PROJECTION_BURDEN_DISTRIBUTION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, PROJECTION_BURDEN_DISTRIBUTION_SCHEMA_VERSION
        )
        count = _strict_int(self.count, "distribution.count")
        if count < 0:
            raise ValueError("projection distribution count is negative")
        object.__setattr__(self, "count", count)
        values = []
        for name in ("mean", "total", "p50", "p90", "p99", "maximum"):
            value = _nonnegative_float(
                getattr(self, name), f"distribution.{name}"
            )
            object.__setattr__(self, name, value)
            values.append(value)
        if not count and any(values):
            raise ValueError("empty projection distribution is nonzero")
        if count and not math.isclose(
            self.mean * count, self.total, rel_tol=1e-10, abs_tol=1e-12
        ):
            raise ValueError("projection distribution mean and total differ")
        if not self.p50 <= self.p90 <= self.p99 <= self.maximum:
            raise ValueError("projection distribution quantiles are unordered")

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "count": self.count,
            "mean": self.mean,
            "total": self.total,
            "p50": self.p50,
            "p90": self.p90,
            "p99": self.p99,
            "maximum": self.maximum,
            "quantile_method": "nearest-rank.v1",
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ProjectionBurdenDistributionV1:
        if data.get("quantile_method") != "nearest-rank.v1":
            raise ValueError("projection quantile method differs")
        return cls(
            count=_strict_int(data.get("count"), "distribution.count"),
            mean=_finite_float(data.get("mean"), "distribution.mean"),
            total=_finite_float(data.get("total"), "distribution.total"),
            p50=_finite_float(data.get("p50"), "distribution.p50"),
            p90=_finite_float(data.get("p90"), "distribution.p90"),
            p99=_finite_float(data.get("p99"), "distribution.p99"),
            maximum=_finite_float(data.get("maximum"), "distribution.maximum"),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ProjectionBurdenSliceV1:
    """Row-free burden, residual, movement, age, and lineage aggregates."""

    slice_kind: ProjectionBurdenSliceKind
    dimensions: Mapping[str, str]
    proposal_count: int
    projected_event_count: int
    projected_event_rate: float
    hard_refusal_count: int
    hard_refusal_rate: float
    observed_only_residuals: ProjectionBurdenDistributionV1
    synthetic_pre_residuals: ProjectionBurdenDistributionV1
    synthetic_post_residuals: ProjectionBurdenDistributionV1
    burdens: ProjectionBurdenDistributionV1
    projection_l1_total: float
    scale_total: float
    scale_weighted_mean_burden: float
    midpoint_movement_total: float
    spread_movement_total: float
    projection_priority_leg_counts: Mapping[str, int]
    quote_ages_ns: ProjectionBurdenDistributionV1
    burden_quote_age_pearson: float
    path_metric_signed_change_mean: float
    path_metric_absolute_change_mean: float
    spread_metric_signed_change_mean: float
    spread_metric_absolute_change_mean: float
    masked_by_final_residual_count: int
    synthetic_post_residual_failure_count: int
    event_ids_sha256: str
    event_content_sha256: str
    slice_id: str = ""
    schema_version: str = PROJECTION_BURDEN_SLICE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, PROJECTION_BURDEN_SLICE_SCHEMA_VERSION
        )
        object.__setattr__(
            self, "slice_kind", ProjectionBurdenSliceKind(self.slice_kind)
        )
        dimensions = {
            _required_text(str(key)): _required_text(str(value))
            for key, value in sorted(self.dimensions.items())
        }
        _validate_slice_dimensions(self.slice_kind, dimensions)
        object.__setattr__(self, "dimensions", dimensions)
        for name in (
            "proposal_count",
            "projected_event_count",
            "hard_refusal_count",
            "masked_by_final_residual_count",
            "synthetic_post_residual_failure_count",
        ):
            integer_value = _strict_int(getattr(self, name), name)
            if integer_value < 0:
                raise ValueError(f"{name} must be nonnegative")
            object.__setattr__(self, name, integer_value)
        if (
            self.projected_event_count + self.hard_refusal_count
            > self.proposal_count
        ):
            raise ValueError("projection event counts exceed proposal count")
        for name, numerator in (
            ("projected_event_rate", self.projected_event_count),
            ("hard_refusal_rate", self.hard_refusal_count),
        ):
            rate_value = _finite_float(getattr(self, name), name)
            expected = (
                numerator / self.proposal_count if self.proposal_count else 0.0
            )
            if not math.isclose(
                rate_value, expected, rel_tol=1e-12, abs_tol=1e-12
            ):
                raise ValueError(f"{name} differs from event counts")
            object.__setattr__(self, name, rate_value)
        for name in (
            "observed_only_residuals",
            "synthetic_pre_residuals",
            "synthetic_post_residuals",
            "burdens",
            "quote_ages_ns",
        ):
            if not isinstance(
                getattr(self, name), ProjectionBurdenDistributionV1
            ):
                raise TypeError(f"{name} must use projection distribution v1")
        if self.burdens.count != self.proposal_count:
            raise ValueError("projection burden count differs from proposals")
        if self.synthetic_pre_residuals.count != self.proposal_count or (
            self.synthetic_post_residuals.count != self.proposal_count
        ):
            raise ValueError("synthetic residual count differs from proposals")
        if self.quote_ages_ns.count != self.proposal_count:
            raise ValueError(
                "projection quote-age count differs from proposals"
            )
        for name in (
            "projection_l1_total",
            "scale_total",
            "scale_weighted_mean_burden",
            "midpoint_movement_total",
            "spread_movement_total",
            "path_metric_absolute_change_mean",
            "spread_metric_absolute_change_mean",
        ):
            object.__setattr__(
                self, name, _nonnegative_float(getattr(self, name), name)
            )
        expected_weighted = (
            self.projection_l1_total / self.scale_total
            if self.proposal_count
            else 0.0
        )
        if not math.isclose(
            self.scale_weighted_mean_burden,
            expected_weighted,
            rel_tol=1e-12,
            abs_tol=1e-12,
        ):
            raise ValueError("scale-weighted projection burden differs")
        if not math.isclose(
            self.midpoint_movement_total + self.spread_movement_total,
            self.projection_l1_total,
            rel_tol=1e-12,
            abs_tol=1e-12,
        ):
            raise ValueError("midpoint/spread movement does not decompose L1")
        correlation = _finite_float(
            self.burden_quote_age_pearson, "burden_quote_age_pearson"
        )
        if not -1.0 <= correlation <= 1.0:
            raise ValueError("burden/age correlation is outside [-1, 1]")
        object.__setattr__(self, "burden_quote_age_pearson", correlation)
        for name in (
            "path_metric_signed_change_mean",
            "spread_metric_signed_change_mean",
        ):
            object.__setattr__(
                self, name, _finite_float(getattr(self, name), name)
            )
        priority = {
            _required_text(str(key)): _strict_int(value, f"priority.{key}")
            for key, value in sorted(
                self.projection_priority_leg_counts.items()
            )
        }
        if set(priority).difference(TRIANGLE_SYMBOLS) or any(
            value < 0 for value in priority.values()
        ):
            raise ValueError("projection priority-leg counts are invalid")
        if sum(priority.values()) > self.proposal_count:
            raise ValueError("projection priority-leg counts exceed proposals")
        object.__setattr__(self, "projection_priority_leg_counts", priority)
        for name in ("event_ids_sha256", "event_content_sha256"):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))
        expected_slice_id = _stable_id(
            "projection-burden-slice", self.payload()
        )
        if self.slice_id and self.slice_id != expected_slice_id:
            raise ValueError("projection burden slice identity differs")
        object.__setattr__(self, "slice_id", expected_slice_id)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "slice_kind": self.slice_kind.value,
            "dimensions": dict(self.dimensions),
            "proposal_count": self.proposal_count,
            "projected_event_count": self.projected_event_count,
            "projected_event_rate": self.projected_event_rate,
            "hard_refusal_count": self.hard_refusal_count,
            "hard_refusal_rate": self.hard_refusal_rate,
            "observed_only_residuals": self.observed_only_residuals.to_dict(),
            "synthetic_pre_residuals": self.synthetic_pre_residuals.to_dict(),
            "synthetic_post_residuals": self.synthetic_post_residuals.to_dict(),
            "burdens": self.burdens.to_dict(),
            "projection_l1_total": self.projection_l1_total,
            "scale_total": self.scale_total,
            "scale_weighted_mean_burden": self.scale_weighted_mean_burden,
            "midpoint_movement_total": self.midpoint_movement_total,
            "spread_movement_total": self.spread_movement_total,
            "midpoint_spread_decomposition_id": MIDPOINT_SPREAD_DECOMPOSITION_ID,
            "projection_priority_leg_counts": dict(
                self.projection_priority_leg_counts
            ),
            "quote_ages_ns": self.quote_ages_ns.to_dict(),
            "burden_quote_age_pearson": self.burden_quote_age_pearson,
            "path_metric_signed_change_mean": self.path_metric_signed_change_mean,
            "path_metric_absolute_change_mean": self.path_metric_absolute_change_mean,
            "spread_metric_signed_change_mean": self.spread_metric_signed_change_mean,
            "spread_metric_absolute_change_mean": self.spread_metric_absolute_change_mean,
            "masked_by_final_residual_count": self.masked_by_final_residual_count,
            "synthetic_post_residual_failure_count": (
                self.synthetic_post_residual_failure_count
            ),
            "event_ids_sha256": self.event_ids_sha256,
            "event_content_sha256": self.event_content_sha256,
            "event_rows_embedded": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "slice_id": self.slice_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ProjectionBurdenSliceV1:
        if (
            data.get("midpoint_spread_decomposition_id")
            != MIDPOINT_SPREAD_DECOMPOSITION_ID
        ):
            raise ValueError("projection midpoint/spread decomposition differs")
        if data.get("event_rows_embedded") is not False:
            raise ValueError("projection slice embeds event rows")
        return cls(
            slice_kind=ProjectionBurdenSliceKind(
                str(data.get("slice_kind", ""))
            ),
            dimensions={
                str(key): str(value)
                for key, value in _mapping(
                    data.get("dimensions"), "dimensions"
                ).items()
            },
            proposal_count=_strict_int(
                data.get("proposal_count"), "proposal_count"
            ),
            projected_event_count=_strict_int(
                data.get("projected_event_count"), "projected_event_count"
            ),
            projected_event_rate=_finite_float(
                data.get("projected_event_rate"), "projected_event_rate"
            ),
            hard_refusal_count=_strict_int(
                data.get("hard_refusal_count"), "hard_refusal_count"
            ),
            hard_refusal_rate=_finite_float(
                data.get("hard_refusal_rate"), "hard_refusal_rate"
            ),
            observed_only_residuals=ProjectionBurdenDistributionV1.from_dict(
                _mapping(
                    data.get("observed_only_residuals"),
                    "observed_only_residuals",
                )
            ),
            synthetic_pre_residuals=ProjectionBurdenDistributionV1.from_dict(
                _mapping(
                    data.get("synthetic_pre_residuals"),
                    "synthetic_pre_residuals",
                )
            ),
            synthetic_post_residuals=ProjectionBurdenDistributionV1.from_dict(
                _mapping(
                    data.get("synthetic_post_residuals"),
                    "synthetic_post_residuals",
                )
            ),
            burdens=ProjectionBurdenDistributionV1.from_dict(
                _mapping(data.get("burdens"), "burdens")
            ),
            projection_l1_total=_finite_float(
                data.get("projection_l1_total"), "projection_l1_total"
            ),
            scale_total=_finite_float(data.get("scale_total"), "scale_total"),
            scale_weighted_mean_burden=_finite_float(
                data.get("scale_weighted_mean_burden"),
                "scale_weighted_mean_burden",
            ),
            midpoint_movement_total=_finite_float(
                data.get("midpoint_movement_total"), "midpoint_movement_total"
            ),
            spread_movement_total=_finite_float(
                data.get("spread_movement_total"), "spread_movement_total"
            ),
            projection_priority_leg_counts={
                str(key): _strict_int(value, str(key))
                for key, value in _mapping(
                    data.get("projection_priority_leg_counts"),
                    "projection_priority_leg_counts",
                ).items()
            },
            quote_ages_ns=ProjectionBurdenDistributionV1.from_dict(
                _mapping(data.get("quote_ages_ns"), "quote_ages_ns")
            ),
            burden_quote_age_pearson=_finite_float(
                data.get("burden_quote_age_pearson"),
                "burden_quote_age_pearson",
            ),
            path_metric_signed_change_mean=_finite_float(
                data.get("path_metric_signed_change_mean"),
                "path_metric_signed_change_mean",
            ),
            path_metric_absolute_change_mean=_finite_float(
                data.get("path_metric_absolute_change_mean"),
                "path_metric_absolute_change_mean",
            ),
            spread_metric_signed_change_mean=_finite_float(
                data.get("spread_metric_signed_change_mean"),
                "spread_metric_signed_change_mean",
            ),
            spread_metric_absolute_change_mean=_finite_float(
                data.get("spread_metric_absolute_change_mean"),
                "spread_metric_absolute_change_mean",
            ),
            masked_by_final_residual_count=_strict_int(
                data.get("masked_by_final_residual_count"),
                "masked_by_final_residual_count",
            ),
            synthetic_post_residual_failure_count=_strict_int(
                data.get("synthetic_post_residual_failure_count"),
                "synthetic_post_residual_failure_count",
            ),
            event_ids_sha256=str(data.get("event_ids_sha256", "")),
            event_content_sha256=str(data.get("event_content_sha256", "")),
            slice_id=str(data.get("slice_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ProjectionBurdenModelDecisionV1:
    """Release decision for one proposal model from baseline and controls."""

    model_id: str
    model_family: str
    global_slice_id: str
    proposal_count: int
    hard_failure_codes: tuple[str, ...]
    limitation_codes: tuple[str, ...]
    missed_misspecification_scenario_ids: tuple[str, ...]
    masked_by_final_residual_count: int
    synthetic_post_residual_failure_count: int
    status: ProjectionBurdenStatus
    decision_id: str = ""
    schema_version: str = PROJECTION_BURDEN_MODEL_DECISION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            PROJECTION_BURDEN_MODEL_DECISION_SCHEMA_VERSION,
        )
        for name in ("model_id", "model_family", "global_slice_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        for name in (
            "proposal_count",
            "masked_by_final_residual_count",
            "synthetic_post_residual_failure_count",
        ):
            value = _strict_int(getattr(self, name), name)
            if value < 0:
                raise ValueError(f"{name} is negative")
            object.__setattr__(self, name, value)
        hard = _normalized_text_tuple(self.hard_failure_codes, allow_empty=True)
        limitations = _normalized_text_tuple(
            self.limitation_codes, allow_empty=True
        )
        missed = _normalized_text_tuple(
            self.missed_misspecification_scenario_ids, allow_empty=True
        )
        object.__setattr__(self, "hard_failure_codes", hard)
        object.__setattr__(self, "limitation_codes", limitations)
        object.__setattr__(self, "missed_misspecification_scenario_ids", missed)
        expected = (
            ProjectionBurdenStatus.FAIL
            if hard or missed or self.synthetic_post_residual_failure_count
            else (
                ProjectionBurdenStatus.LIMITED
                if limitations
                else ProjectionBurdenStatus.PASS
            )
        )
        supplied = ProjectionBurdenStatus(self.status)
        if supplied is not expected:
            raise ValueError("projection burden model status differs")
        object.__setattr__(self, "status", expected)
        identity = _stable_id(
            "projection-burden-model-decision", self.payload()
        )
        if self.decision_id and self.decision_id != identity:
            raise ValueError(
                "projection burden model decision identity differs"
            )
        object.__setattr__(self, "decision_id", identity)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "model_id": self.model_id,
            "model_family": self.model_family,
            "global_slice_id": self.global_slice_id,
            "proposal_count": self.proposal_count,
            "hard_failure_codes": list(self.hard_failure_codes),
            "limitation_codes": list(self.limitation_codes),
            "missed_misspecification_scenario_ids": list(
                self.missed_misspecification_scenario_ids
            ),
            "masked_by_final_residual_count": self.masked_by_final_residual_count,
            "synthetic_post_residual_failure_count": (
                self.synthetic_post_residual_failure_count
            ),
            "status": self.status.value,
            "final_residual_alone_sufficient": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "decision_id": self.decision_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ProjectionBurdenModelDecisionV1:
        if data.get("final_residual_alone_sufficient") is not False:
            raise ValueError(
                "projection decision permits final-residual-only pass"
            )
        return cls(
            model_id=str(data.get("model_id", "")),
            model_family=str(data.get("model_family", "")),
            global_slice_id=str(data.get("global_slice_id", "")),
            proposal_count=_strict_int(
                data.get("proposal_count"), "proposal_count"
            ),
            hard_failure_codes=_string_tuple(
                data.get("hard_failure_codes"),
                "hard_failure_codes",
                allow_empty=True,
            ),
            limitation_codes=_string_tuple(
                data.get("limitation_codes"),
                "limitation_codes",
                allow_empty=True,
            ),
            missed_misspecification_scenario_ids=_string_tuple(
                data.get("missed_misspecification_scenario_ids"),
                "missed_misspecification_scenario_ids",
                allow_empty=True,
            ),
            masked_by_final_residual_count=_strict_int(
                data.get("masked_by_final_residual_count"),
                "masked_by_final_residual_count",
            ),
            synthetic_post_residual_failure_count=_strict_int(
                data.get("synthetic_post_residual_failure_count"),
                "synthetic_post_residual_failure_count",
            ),
            status=ProjectionBurdenStatus(str(data.get("status", ""))),
            decision_id=str(data.get("decision_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ProjectionBurdenModelComparisonV1:
    """Comparator-relative burden gate over exactly matched baseline cells."""

    left_model_id: str
    right_model_id: str
    matched_cell_count: int
    left_mean_burden: float
    right_mean_burden: float
    left_to_right_ratio: float
    right_to_left_ratio: float
    maximum_permitted_ratio: float
    conclusion: ProjectionComparisonConclusion
    comparison_id: str = ""
    schema_version: str = PROJECTION_BURDEN_MODEL_COMPARISON_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            PROJECTION_BURDEN_MODEL_COMPARISON_SCHEMA_VERSION,
        )
        for name in ("left_model_id", "right_model_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        if self.left_model_id >= self.right_model_id:
            raise ValueError("projection comparison model order differs")
        count = _strict_int(self.matched_cell_count, "matched_cell_count")
        if count <= 0:
            raise ValueError("projection comparison has no matched cells")
        object.__setattr__(self, "matched_cell_count", count)
        for name in (
            "left_mean_burden",
            "right_mean_burden",
            "left_to_right_ratio",
            "right_to_left_ratio",
            "maximum_permitted_ratio",
        ):
            object.__setattr__(
                self, name, _nonnegative_float(getattr(self, name), name)
            )
        if self.maximum_permitted_ratio <= 1.0:
            raise ValueError(
                "projection comparison permitted ratio must exceed one"
            )
        expected = (
            ProjectionComparisonConclusion.LEFT_EXCESSIVE
            if self.left_to_right_ratio > self.maximum_permitted_ratio
            else (
                ProjectionComparisonConclusion.RIGHT_EXCESSIVE
                if self.right_to_left_ratio > self.maximum_permitted_ratio
                else ProjectionComparisonConclusion.EQUIVALENT
            )
        )
        supplied = ProjectionComparisonConclusion(self.conclusion)
        if supplied is not expected:
            raise ValueError("projection comparison conclusion differs")
        object.__setattr__(self, "conclusion", expected)
        identity = _stable_id(
            "projection-burden-model-comparison", self.payload()
        )
        if self.comparison_id and self.comparison_id != identity:
            raise ValueError("projection model comparison identity differs")
        object.__setattr__(self, "comparison_id", identity)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "left_model_id": self.left_model_id,
            "right_model_id": self.right_model_id,
            "matched_cell_count": self.matched_cell_count,
            "left_mean_burden": self.left_mean_burden,
            "right_mean_burden": self.right_mean_burden,
            "left_to_right_ratio": self.left_to_right_ratio,
            "right_to_left_ratio": self.right_to_left_ratio,
            "maximum_permitted_ratio": self.maximum_permitted_ratio,
            "conclusion": self.conclusion.value,
            "comparison_surface": "matched-window-member-baseline-cells.v1",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "comparison_id": self.comparison_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ProjectionBurdenModelComparisonV1:
        if (
            data.get("comparison_surface")
            != "matched-window-member-baseline-cells.v1"
        ):
            raise ValueError("projection comparison surface differs")
        return cls(
            left_model_id=str(data.get("left_model_id", "")),
            right_model_id=str(data.get("right_model_id", "")),
            matched_cell_count=_strict_int(
                data.get("matched_cell_count"), "matched_cell_count"
            ),
            left_mean_burden=_finite_float(
                data.get("left_mean_burden"), "left_mean_burden"
            ),
            right_mean_burden=_finite_float(
                data.get("right_mean_burden"), "right_mean_burden"
            ),
            left_to_right_ratio=_finite_float(
                data.get("left_to_right_ratio"), "left_to_right_ratio"
            ),
            right_to_left_ratio=_finite_float(
                data.get("right_to_left_ratio"), "right_to_left_ratio"
            ),
            maximum_permitted_ratio=_finite_float(
                data.get("maximum_permitted_ratio"), "maximum_permitted_ratio"
            ),
            conclusion=ProjectionComparisonConclusion(
                str(data.get("conclusion", ""))
            ),
            comparison_id=str(data.get("comparison_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ProjectionBurdenReportV1:
    """Complete row-free release diagnostic and qualification decision."""

    policy: ProjectionBurdenPolicyV1
    scenarios: tuple[ProjectionBurdenScenarioV1, ...]
    slices: tuple[ProjectionBurdenSliceV1, ...]
    model_decisions: tuple[ProjectionBurdenModelDecisionV1, ...]
    model_comparisons: tuple[ProjectionBurdenModelComparisonV1, ...]
    input_artifact_ids: Mapping[str, str]
    source_event_count: int
    source_event_ids_sha256: str
    source_event_content_sha256: str
    release_status: ProjectionBurdenStatus
    finding_codes: tuple[str, ...]
    report_id: str = ""
    schema_version: str = PROJECTION_BURDEN_REPORT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, PROJECTION_BURDEN_REPORT_SCHEMA_VERSION
        )
        if not isinstance(self.policy, ProjectionBurdenPolicyV1):
            raise TypeError("projection report policy must use v1")
        scenarios = tuple(
            sorted(self.scenarios, key=lambda item: item.scenario_id)
        )
        if not scenarios or len(scenarios) > MAX_PROJECTION_SCENARIOS:
            raise ValueError("projection report scenario count is invalid")
        if len({item.scenario_id for item in scenarios}) != len(scenarios):
            raise ValueError("projection report scenarios duplicate")
        object.__setattr__(self, "scenarios", scenarios)
        slices = tuple(sorted(self.slices, key=lambda item: item.slice_id))
        if not slices or len(slices) > MAX_PROJECTION_SLICES:
            raise ValueError("projection report slice count is invalid")
        if len({item.slice_id for item in slices}) != len(slices):
            raise ValueError("projection report slices duplicate")
        object.__setattr__(self, "slices", slices)
        decisions = tuple(
            sorted(self.model_decisions, key=lambda item: item.model_id)
        )
        object.__setattr__(self, "model_decisions", decisions)
        comparisons = tuple(
            sorted(self.model_comparisons, key=lambda item: item.comparison_id)
        )
        object.__setattr__(self, "model_comparisons", comparisons)
        artifacts = {
            _required_text(str(key)): _required_text(str(value))
            for key, value in sorted(self.input_artifact_ids.items())
        }
        if set(artifacts) != {
            "alignment_qualification",
            "proposal_lineage",
            "reconciliation_config",
        }:
            raise ValueError(
                "projection report input-artifact coverage differs"
            )
        if (
            artifacts["reconciliation_config"]
            != self.policy.reconciliation_config_id
        ):
            raise ValueError(
                "projection report reconciliation identity differs"
            )
        if (
            artifacts["alignment_qualification"]
            != self.policy.alignment_policy_id
        ):
            raise ValueError("projection report alignment identity differs")
        object.__setattr__(self, "input_artifact_ids", artifacts)
        count = _strict_int(self.source_event_count, "source_event_count")
        if not 1 <= count <= MAX_PROJECTION_EVENTS:
            raise ValueError("projection source event count is invalid")
        object.__setattr__(self, "source_event_count", count)
        for name in ("source_event_ids_sha256", "source_event_content_sha256"):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))
        _validate_projection_report_topology(
            self.policy,
            scenarios,
            slices,
            decisions,
            comparisons,
            count,
        )
        findings = _normalized_text_tuple(self.finding_codes, allow_empty=True)
        object.__setattr__(self, "finding_codes", findings)
        expected_status = (
            ProjectionBurdenStatus.FAIL
            if any(
                item.status is ProjectionBurdenStatus.FAIL for item in decisions
            )
            else (
                ProjectionBurdenStatus.LIMITED
                if any(
                    item.status is ProjectionBurdenStatus.LIMITED
                    for item in decisions
                )
                else ProjectionBurdenStatus.PASS
            )
        )
        supplied = ProjectionBurdenStatus(self.release_status)
        if supplied is not expected_status:
            raise ValueError("projection report release status differs")
        object.__setattr__(self, "release_status", expected_status)
        expected_findings = _report_findings(decisions, comparisons)
        if findings != expected_findings:
            raise ValueError("projection report findings differ")
        identity = _stable_id("projection-burden-report", self.payload())
        if self.report_id and self.report_id != identity:
            raise ValueError("projection burden report identity differs")
        object.__setattr__(self, "report_id", identity)
        _bounded_json(self.to_json(), "projection burden report")

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "policy": self.policy.to_dict(),
            "scenarios": [item.to_dict() for item in self.scenarios],
            "slices": [item.to_dict() for item in self.slices],
            "model_decisions": [
                item.to_dict() for item in self.model_decisions
            ],
            "model_comparisons": [
                item.to_dict() for item in self.model_comparisons
            ],
            "input_artifact_ids": dict(self.input_artifact_ids),
            "source_event_count": self.source_event_count,
            "source_event_ids_sha256": self.source_event_ids_sha256,
            "source_event_content_sha256": self.source_event_content_sha256,
            "release_status": self.release_status.value,
            "finding_codes": list(self.finding_codes),
            "primary_scale_id": PRIMARY_SCALE_ID,
            "event_rows_embedded": False,
            "rejected_rows_retained": False,
            "observed_only_residual_enters_burden": False,
            "final_residual_alone_sufficient": False,
            "cross_currency_coherence_claim_permitted": (
                self.release_status is ProjectionBurdenStatus.PASS
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "report_id": self.report_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    def decision(self, model_id: str) -> ProjectionBurdenModelDecisionV1:
        for item in self.model_decisions:
            if item.model_id == model_id:
                return item
        raise KeyError(model_id)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ProjectionBurdenReportV1:
        constants = {
            "primary_scale_id": PRIMARY_SCALE_ID,
            "event_rows_embedded": False,
            "rejected_rows_retained": False,
            "observed_only_residual_enters_burden": False,
            "final_residual_alone_sufficient": False,
        }
        if any(data.get(key) != value for key, value in constants.items()):
            raise ValueError("projection report fixed semantics differ")
        status = ProjectionBurdenStatus(str(data.get("release_status", "")))
        if data.get("cross_currency_coherence_claim_permitted") is not (
            status is ProjectionBurdenStatus.PASS
        ):
            raise ValueError("projection report coherence claim differs")
        return cls(
            policy=ProjectionBurdenPolicyV1.from_dict(
                _mapping(data.get("policy"), "policy")
            ),
            scenarios=tuple(
                ProjectionBurdenScenarioV1.from_dict(_mapping(item, "scenario"))
                for item in _sequence(data.get("scenarios"), "scenarios")
            ),
            slices=tuple(
                ProjectionBurdenSliceV1.from_dict(_mapping(item, "slice"))
                for item in _sequence(data.get("slices"), "slices")
            ),
            model_decisions=tuple(
                ProjectionBurdenModelDecisionV1.from_dict(
                    _mapping(item, "decision")
                )
                for item in _sequence(
                    data.get("model_decisions"), "model_decisions"
                )
            ),
            model_comparisons=tuple(
                ProjectionBurdenModelComparisonV1.from_dict(
                    _mapping(item, "comparison")
                )
                for item in _sequence(
                    data.get("model_comparisons"), "model_comparisons"
                )
            ),
            input_artifact_ids={
                str(key): str(value)
                for key, value in _mapping(
                    data.get("input_artifact_ids"), "input_artifact_ids"
                ).items()
            },
            source_event_count=_strict_int(
                data.get("source_event_count"), "source_event_count"
            ),
            source_event_ids_sha256=str(
                data.get("source_event_ids_sha256", "")
            ),
            source_event_content_sha256=str(
                data.get("source_event_content_sha256", "")
            ),
            release_status=status,
            finding_codes=_string_tuple(
                data.get("finding_codes"), "finding_codes", allow_empty=True
            ),
            report_id=str(data.get("report_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> ProjectionBurdenReportV1:
        _bounded_json(text, "projection burden report")
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class ProjectionBurdenHawkesBindingV1:
    """Exact #508 validation-coordinate and selected-model burden binding."""

    report_id: str
    report_policy_id: str
    hawkes_dossier_id: str
    hawkes_policy_id: str
    hawkes_comparison_id: str
    selected_engine_id: str
    excluded_engine_id: str
    coordinate_count: int
    coordinate_bindings_sha256: str
    selected_model_status: ProjectionBurdenStatus
    comparator_conclusion: ProjectionComparisonConclusion
    binding_status: ProjectionBurdenStatus
    limitation_codes: tuple[str, ...]
    binding_id: str = ""
    schema_version: str = PROJECTION_BURDEN_HAWKES_BINDING_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            PROJECTION_BURDEN_HAWKES_BINDING_SCHEMA_VERSION,
        )
        for name in (
            "report_id",
            "report_policy_id",
            "hawkes_dossier_id",
            "hawkes_policy_id",
            "hawkes_comparison_id",
            "selected_engine_id",
            "excluded_engine_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        if {self.selected_engine_id, self.excluded_engine_id} != set(
            _hawkes_selection_engine_ids()
        ):
            raise ValueError(
                "projection Hawkes binding engine coverage differs"
            )
        count = _strict_int(self.coordinate_count, "coordinate_count")
        if count <= 0:
            raise ValueError("projection Hawkes binding has no coordinates")
        object.__setattr__(self, "coordinate_count", count)
        object.__setattr__(
            self,
            "coordinate_bindings_sha256",
            _sha256(
                self.coordinate_bindings_sha256, "coordinate_bindings_sha256"
            ),
        )
        selected_status = ProjectionBurdenStatus(self.selected_model_status)
        conclusion = ProjectionComparisonConclusion(self.comparator_conclusion)
        limitations = _normalized_text_tuple(
            self.limitation_codes, allow_empty=True
        )
        expected_status = (
            ProjectionBurdenStatus.FAIL
            if selected_status is ProjectionBurdenStatus.FAIL
            or _comparison_rejects_selected(
                conclusion, self.selected_engine_id, self.excluded_engine_id
            )
            else (
                ProjectionBurdenStatus.LIMITED
                if selected_status is ProjectionBurdenStatus.LIMITED
                else ProjectionBurdenStatus.PASS
            )
        )
        supplied_status = ProjectionBurdenStatus(self.binding_status)
        if supplied_status is not expected_status:
            raise ValueError("projection Hawkes binding status differs")
        expected_limitations = (
            ("selected_model_projection_burden_limited",)
            if expected_status is ProjectionBurdenStatus.LIMITED
            else ()
        )
        if limitations != expected_limitations:
            raise ValueError("projection Hawkes binding limitations differ")
        object.__setattr__(self, "selected_model_status", selected_status)
        object.__setattr__(self, "comparator_conclusion", conclusion)
        object.__setattr__(self, "binding_status", expected_status)
        object.__setattr__(self, "limitation_codes", limitations)
        identity = _stable_id(
            "projection-burden-hawkes-binding", self.payload()
        )
        if self.binding_id and self.binding_id != identity:
            raise ValueError("projection Hawkes binding identity differs")
        object.__setattr__(self, "binding_id", identity)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "report_id": self.report_id,
            "report_policy_id": self.report_policy_id,
            "hawkes_dossier_id": self.hawkes_dossier_id,
            "hawkes_policy_id": self.hawkes_policy_id,
            "hawkes_comparison_id": self.hawkes_comparison_id,
            "selected_engine_id": self.selected_engine_id,
            "excluded_engine_id": self.excluded_engine_id,
            "coordinate_count": self.coordinate_count,
            "coordinate_bindings_sha256": self.coordinate_bindings_sha256,
            "selected_model_status": self.selected_model_status.value,
            "comparator_conclusion": self.comparator_conclusion.value,
            "binding_status": self.binding_status.value,
            "limitation_codes": list(self.limitation_codes),
            "coordinate_numerators_denominators_counts_exact": True,
            "final_residual_alone_sufficient": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "binding_id": self.binding_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ProjectionBurdenHawkesBindingV1:
        if (
            data.get("coordinate_numerators_denominators_counts_exact")
            is not True
            or data.get("final_residual_alone_sufficient") is not False
        ):
            raise ValueError("projection Hawkes binding semantics differ")
        return cls(
            report_id=str(data.get("report_id", "")),
            report_policy_id=str(data.get("report_policy_id", "")),
            hawkes_dossier_id=str(data.get("hawkes_dossier_id", "")),
            hawkes_policy_id=str(data.get("hawkes_policy_id", "")),
            hawkes_comparison_id=str(data.get("hawkes_comparison_id", "")),
            selected_engine_id=str(data.get("selected_engine_id", "")),
            excluded_engine_id=str(data.get("excluded_engine_id", "")),
            coordinate_count=_strict_int(
                data.get("coordinate_count"), "coordinate_count"
            ),
            coordinate_bindings_sha256=str(
                data.get("coordinate_bindings_sha256", "")
            ),
            selected_model_status=ProjectionBurdenStatus(
                str(data.get("selected_model_status", ""))
            ),
            comparator_conclusion=ProjectionComparisonConclusion(
                str(data.get("comparator_conclusion", ""))
            ),
            binding_status=ProjectionBurdenStatus(
                str(data.get("binding_status", ""))
            ),
            limitation_codes=_string_tuple(
                data.get("limitation_codes"),
                "limitation_codes",
                allow_empty=True,
            ),
            binding_id=str(data.get("binding_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ProjectionBurdenConsumptionReceiptV1:
    """Exact release-surface receipt preventing evidence substitution."""

    report_id: str
    policy_id: str
    consumer_kind: ProjectionBurdenConsumerKind
    consumer_id: str
    model_id: str
    model_decision_id: str
    consumed_slice_ids: tuple[str, ...]
    hawkes_binding_id: str | None
    status: ProjectionBurdenStatus
    limitation_codes: tuple[str, ...]
    receipt_id: str = ""
    schema_version: str = PROJECTION_BURDEN_CONSUMPTION_RECEIPT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            PROJECTION_BURDEN_CONSUMPTION_RECEIPT_SCHEMA_VERSION,
        )
        for name in (
            "report_id",
            "policy_id",
            "consumer_id",
            "model_id",
            "model_decision_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        kind = ProjectionBurdenConsumerKind(self.consumer_kind)
        slices = _normalized_text_tuple(self.consumed_slice_ids)
        binding = _optional_text(self.hawkes_binding_id)
        if (kind is ProjectionBurdenConsumerKind.HAWKES_SELECTION) != (
            binding is not None
        ):
            raise ValueError(
                "projection receipt Hawkes binding coverage differs"
            )
        status = ProjectionBurdenStatus(self.status)
        limitations = _normalized_text_tuple(
            self.limitation_codes, allow_empty=True
        )
        if status is ProjectionBurdenStatus.PASS and limitations:
            raise ValueError("passing projection receipt contains limitations")
        if status is ProjectionBurdenStatus.LIMITED and not limitations:
            raise ValueError("limited projection receipt lacks limitations")
        object.__setattr__(self, "consumer_kind", kind)
        object.__setattr__(self, "consumed_slice_ids", slices)
        object.__setattr__(self, "hawkes_binding_id", binding)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "limitation_codes", limitations)
        identity = _stable_id(
            "projection-burden-consumption-receipt", self.payload()
        )
        if self.receipt_id and self.receipt_id != identity:
            raise ValueError(
                "projection burden consumption receipt identity differs"
            )
        object.__setattr__(self, "receipt_id", identity)

    @property
    def cross_currency_coherence_claim_permitted(self) -> bool:
        return self.status is ProjectionBurdenStatus.PASS

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "report_id": self.report_id,
            "policy_id": self.policy_id,
            "consumer_kind": self.consumer_kind.value,
            "consumer_id": self.consumer_id,
            "model_id": self.model_id,
            "model_decision_id": self.model_decision_id,
            "consumed_slice_ids": list(self.consumed_slice_ids),
            "hawkes_binding_id": self.hawkes_binding_id,
            "status": self.status.value,
            "limitation_codes": list(self.limitation_codes),
            "cross_currency_coherence_claim_permitted": (
                self.cross_currency_coherence_claim_permitted
            ),
            "report_identity_exact": True,
            "policy_identity_exact": True,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "receipt_id": self.receipt_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ProjectionBurdenConsumptionReceiptV1:
        if (
            data.get("report_identity_exact") is not True
            or data.get("policy_identity_exact") is not True
        ):
            raise ValueError(
                "projection receipt exact identity semantics differ"
            )
        status = ProjectionBurdenStatus(str(data.get("status", "")))
        if data.get("cross_currency_coherence_claim_permitted") is not (
            status is ProjectionBurdenStatus.PASS
        ):
            raise ValueError("projection receipt coherence claim differs")
        return cls(
            report_id=str(data.get("report_id", "")),
            policy_id=str(data.get("policy_id", "")),
            consumer_kind=ProjectionBurdenConsumerKind(
                str(data.get("consumer_kind", ""))
            ),
            consumer_id=str(data.get("consumer_id", "")),
            model_id=str(data.get("model_id", "")),
            model_decision_id=str(data.get("model_decision_id", "")),
            consumed_slice_ids=_string_tuple(
                data.get("consumed_slice_ids"), "consumed_slice_ids"
            ),
            hawkes_binding_id=_optional_text(data.get("hawkes_binding_id")),
            status=status,
            limitation_codes=_string_tuple(
                data.get("limitation_codes"),
                "limitation_codes",
                allow_empty=True,
            ),
            receipt_id=str(data.get("receipt_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ProjectionBurdenReleaseCoverageV1:
    """Certification-ready aggregate over every required release consumer."""

    report_id: str
    policy_id: str
    required_consumer_ids: Mapping[str, tuple[str, ...]]
    receipts: tuple[ProjectionBurdenConsumptionReceiptV1, ...]
    release_coverage_valid: bool
    excessive_projection_burden_product_count: int
    synthetic_post_projection_residual_failure_count: int
    final_residual_only_projection_pass_count: int
    coverage_id: str = ""
    schema_version: str = PROJECTION_BURDEN_RELEASE_COVERAGE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            PROJECTION_BURDEN_RELEASE_COVERAGE_SCHEMA_VERSION,
        )
        object.__setattr__(self, "report_id", _required_text(self.report_id))
        object.__setattr__(self, "policy_id", _required_text(self.policy_id))
        required_consumer_ids = {
            _required_text(str(key)): _normalized_text_tuple(value)
            for key, value in sorted(self.required_consumer_ids.items())
        }
        expected_consumer_kinds = {
            item.value for item in ProjectionBurdenConsumerKind
        }
        if set(required_consumer_ids) != expected_consumer_kinds:
            raise ValueError(
                "projection required consumer-kind coverage differs"
            )
        if (
            sum(len(value) for value in required_consumer_ids.values())
            > MAX_PROJECTION_RECEIPTS
        ):
            raise ValueError("projection required consumer IDs exceed bound")
        object.__setattr__(self, "required_consumer_ids", required_consumer_ids)
        receipts = tuple(
            sorted(self.receipts, key=lambda item: item.receipt_id)
        )
        if not receipts or len(receipts) > MAX_PROJECTION_RECEIPTS:
            raise ValueError("projection release coverage receipts are invalid")
        if len({item.receipt_id for item in receipts}) != len(receipts):
            raise ValueError("projection release coverage receipts duplicate")
        if len(
            {(item.consumer_kind, item.consumer_id) for item in receipts}
        ) != len(receipts):
            raise ValueError(
                "projection release coverage consumer receipts duplicate"
            )
        if any(
            item.report_id != self.report_id or item.policy_id != self.policy_id
            for item in receipts
        ):
            raise ValueError(
                "projection release coverage contains stale receipts"
            )
        object.__setattr__(self, "receipts", receipts)
        for name in (
            "excessive_projection_burden_product_count",
            "synthetic_post_projection_residual_failure_count",
            "final_residual_only_projection_pass_count",
        ):
            value = _strict_int(getattr(self, name), name)
            if value < 0:
                raise ValueError(f"{name} must be nonnegative")
            object.__setattr__(self, name, value)
        if not isinstance(self.release_coverage_valid, bool):
            raise TypeError(
                "projection release coverage validity must be boolean"
            )
        actual_consumer_ids = {
            kind.value: tuple(
                sorted(
                    item.consumer_id
                    for item in receipts
                    if item.consumer_kind is kind
                )
            )
            for kind in ProjectionBurdenConsumerKind
        }
        expected_valid = (
            actual_consumer_ids == required_consumer_ids
            and all(
                item.status is not ProjectionBurdenStatus.FAIL
                for item in receipts
            )
            and not any(
                (
                    self.excessive_projection_burden_product_count,
                    self.synthetic_post_projection_residual_failure_count,
                    self.final_residual_only_projection_pass_count,
                )
            )
        )
        if self.release_coverage_valid is not expected_valid:
            raise ValueError("projection release coverage validity differs")
        identity = _stable_id(
            "projection-burden-release-coverage", self.payload()
        )
        if self.coverage_id and self.coverage_id != identity:
            raise ValueError(
                "projection burden release coverage identity differs"
            )
        object.__setattr__(self, "coverage_id", identity)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "report_id": self.report_id,
            "policy_id": self.policy_id,
            "required_consumer_ids": {
                key: list(value)
                for key, value in self.required_consumer_ids.items()
            },
            "receipts": [item.to_dict() for item in self.receipts],
            "receipt_count": len(self.receipts),
            "consumer_kinds": cast(
                JSONValue,
                sorted({item.consumer_kind.value for item in self.receipts}),
            ),
            "projection_burden_release_coverage_valid": (
                self.release_coverage_valid
            ),
            "excessive_projection_burden_product_count": (
                self.excessive_projection_burden_product_count
            ),
            "synthetic_post_projection_residual_failure_count": (
                self.synthetic_post_projection_residual_failure_count
            ),
            "final_residual_only_projection_pass_count": (
                self.final_residual_only_projection_pass_count
            ),
            "event_rows_embedded": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "coverage_id": self.coverage_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ProjectionBurdenReleaseCoverageV1:
        receipt_values = tuple(
            ProjectionBurdenConsumptionReceiptV1.from_dict(
                _mapping(item, "receipt")
            )
            for item in _sequence(data.get("receipts"), "receipts")
        )
        if data.get("receipt_count") != len(receipt_values):
            raise ValueError(
                "projection release coverage receipt count differs"
            )
        if data.get("consumer_kinds") != sorted(
            {item.consumer_kind.value for item in receipt_values}
        ):
            raise ValueError(
                "projection release coverage consumer kinds differ"
            )
        if data.get("event_rows_embedded") is not False:
            raise ValueError("projection release coverage embeds event rows")
        return cls(
            report_id=str(data.get("report_id", "")),
            policy_id=str(data.get("policy_id", "")),
            required_consumer_ids={
                str(key): _string_tuple(value, f"required_consumer_ids.{key}")
                for key, value in _mapping(
                    data.get("required_consumer_ids"),
                    "required_consumer_ids",
                ).items()
            },
            receipts=receipt_values,
            release_coverage_valid=_strict_bool(
                data.get("projection_burden_release_coverage_valid"),
                "projection_burden_release_coverage_valid",
            ),
            excessive_projection_burden_product_count=_strict_int(
                data.get("excessive_projection_burden_product_count"),
                "excessive_projection_burden_product_count",
            ),
            synthetic_post_projection_residual_failure_count=_strict_int(
                data.get("synthetic_post_projection_residual_failure_count"),
                "synthetic_post_projection_residual_failure_count",
            ),
            final_residual_only_projection_pass_count=_strict_int(
                data.get("final_residual_only_projection_pass_count"),
                "final_residual_only_projection_pass_count",
            ),
            coverage_id=str(data.get("coverage_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> ProjectionBurdenReleaseCoverageV1:
        _bounded_json(text, "projection burden release coverage")
        return cls.from_dict(_json_mapping(text))


def derive_projection_burden_report(
    policy: ProjectionBurdenPolicyV1,
    scenarios: Sequence[ProjectionBurdenScenarioV1],
    events: Sequence[ProjectionBurdenEventV1],
    *,
    input_artifact_ids: Mapping[str, str],
) -> ProjectionBurdenReportV1:
    """Derive all required aggregate slices and fail-closed model decisions."""
    rows = tuple(events)
    if not rows or len(rows) > MAX_PROJECTION_EVENTS:
        raise ValueError("projection event count is empty or unbounded")
    if len({item.event_id for item in rows}) != len(rows):
        raise ValueError("projection events duplicate exact event IDs")
    scenario_items = tuple(scenarios)
    by_scenario = {item.scenario_id: item for item in scenario_items}
    if len(by_scenario) != len(scenario_items):
        raise ValueError("projection scenarios duplicate")
    if set(policy.required_misspecification_scenario_ids) != {
        item.scenario_id
        for item in scenario_items
        if item.scenario_kind is ProjectionScenarioKind.MISSPECIFICATION
    }:
        raise ValueError(
            "projection misspecification scenario coverage differs"
        )
    if not any(
        item.scenario_kind is ProjectionScenarioKind.BASELINE
        for item in scenario_items
    ):
        raise ValueError("projection report lacks a baseline scenario")
    if any(item.scenario_id not in by_scenario for item in rows):
        raise ValueError("projection event refers to an unknown scenario")
    if any(
        item.reconciliation_config_id != policy.reconciliation_config_id
        for item in rows
    ):
        raise ValueError("projection event reconciliation config is stale")
    if any(
        item.alignment_policy_id != policy.alignment_policy_id for item in rows
    ):
        raise ValueError("projection event alignment policy is stale")
    model_families: dict[str, str] = {}
    for item in rows:
        previous = model_families.setdefault(item.model_id, item.model_family)
        if previous != item.model_family:
            raise ValueError("projection model family differs within one model")
    baseline = tuple(
        item
        for item in rows
        if by_scenario[item.scenario_id].scenario_kind
        is ProjectionScenarioKind.BASELINE
    )
    if {item.model_id for item in baseline} != set(model_families):
        raise ValueError("projection baseline model coverage differs")
    groups = _projection_slice_groups(policy, by_scenario, rows, baseline)
    slices = tuple(
        _summarize_projection_slice(policy, kind, dimensions, selected)
        for kind, dimensions, selected in groups
    )
    global_by_model = {
        item.dimensions["model_id"]: item
        for item in slices
        if item.slice_kind is ProjectionBurdenSliceKind.GLOBAL_MODEL
    }
    scenario_slices = {
        (item.dimensions["model_id"], item.dimensions["scenario_id"]): item
        for item in slices
        if item.slice_kind is ProjectionBurdenSliceKind.SCENARIO
    }
    decisions = tuple(
        _model_decision(
            policy,
            model_id,
            model_families[model_id],
            global_by_model[model_id],
            scenario_slices,
        )
        for model_id in sorted(model_families)
    )
    comparisons = _model_comparisons(
        policy, slices, tuple(sorted(model_families))
    )
    status = (
        ProjectionBurdenStatus.FAIL
        if any(item.status is ProjectionBurdenStatus.FAIL for item in decisions)
        else (
            ProjectionBurdenStatus.LIMITED
            if any(
                item.status is ProjectionBurdenStatus.LIMITED
                for item in decisions
            )
            else ProjectionBurdenStatus.PASS
        )
    )
    return ProjectionBurdenReportV1(
        policy=policy,
        scenarios=scenario_items,
        slices=slices,
        model_decisions=decisions,
        model_comparisons=comparisons,
        input_artifact_ids=input_artifact_ids,
        source_event_count=len(rows),
        source_event_ids_sha256=_text_digest(item.event_id for item in rows),
        source_event_content_sha256=_text_digest(
            item.event_content_sha256 for item in rows
        ),
        release_status=status,
        finding_codes=_report_findings(decisions, comparisons),
    )


def bind_projection_burden_to_hawkes_selection(
    report: ProjectionBurdenReportV1,
    dossier: HawkesProductSelectionDossierV1,
    validation: HawkesValidationComparisonV1,
) -> ProjectionBurdenHawkesBindingV1:
    """Bind exact #508 coordinate numerators, denominators, and counts."""
    if dossier.comparison_id != validation.comparison_id:
        raise ValueError("projection Hawkes validation comparison is stale")
    if dossier.policy.policy_id != validation.policy_id:
        raise ValueError("projection Hawkes selection policy is stale")
    if {item.model_id for item in report.model_decisions} != set(
        _hawkes_selection_engine_ids()
    ):
        raise ValueError(
            "projection report does not cover both Hawkes candidates"
        )
    coordinate_slices = {
        (
            item.dimensions["model_id"],
            item.dimensions["validation_coordinate_id"],
        ): item
        for item in report.slices
        if item.slice_kind is ProjectionBurdenSliceKind.VALIDATION_COORDINATE
    }
    bindings: list[str] = []
    for observation in validation.observations:
        key = (observation.engine_id, observation.coordinate.coordinate_id)
        item = coordinate_slices.get(key)
        if item is None:
            raise ValueError(
                "projection report lacks a Hawkes validation coordinate"
            )
        for actual, expected, name in (
            (
                item.projection_l1_total,
                observation.projection_l1_numerator,
                "numerator",
            ),
            (
                item.scale_total,
                observation.projection_spread_denominator,
                "denominator",
            ),
            (
                float(item.projected_event_count),
                float(observation.projection_event_count),
                "projected event count",
            ),
        ):
            if not math.isclose(actual, expected, rel_tol=1e-10, abs_tol=1e-12):
                raise ValueError(f"projection Hawkes coordinate {name} differs")
        bindings.append(
            canonical_contract_json(
                {
                    "model_id": key[0],
                    "coordinate_id": key[1],
                    "slice_id": item.slice_id,
                    "observation_id": observation.observation_id,
                    "projection_l1_total": item.projection_l1_total,
                    "scale_total": item.scale_total,
                    "projected_event_count": item.projected_event_count,
                }
            )
        )
    if len(coordinate_slices) != len(validation.observations):
        raise ValueError(
            "projection report has foreign Hawkes validation coordinates"
        )
    comparison = _find_model_comparison(
        report, dossier.selected_engine_id, dossier.excluded_engine_id
    )
    selected = report.decision(dossier.selected_engine_id)
    status = (
        ProjectionBurdenStatus.FAIL
        if selected.status is ProjectionBurdenStatus.FAIL
        or _comparison_rejects_selected(
            comparison.conclusion,
            dossier.selected_engine_id,
            dossier.excluded_engine_id,
        )
        else (
            ProjectionBurdenStatus.LIMITED
            if selected.status is ProjectionBurdenStatus.LIMITED
            else ProjectionBurdenStatus.PASS
        )
    )
    return ProjectionBurdenHawkesBindingV1(
        report_id=report.report_id,
        report_policy_id=report.policy.policy_id,
        hawkes_dossier_id=dossier.dossier_id,
        hawkes_policy_id=dossier.policy.policy_id,
        hawkes_comparison_id=validation.comparison_id,
        selected_engine_id=dossier.selected_engine_id,
        excluded_engine_id=dossier.excluded_engine_id,
        coordinate_count=validation.coordinate_count,
        coordinate_bindings_sha256=_text_digest(bindings),
        selected_model_status=selected.status,
        comparator_conclusion=comparison.conclusion,
        binding_status=status,
        limitation_codes=(
            ("selected_model_projection_burden_limited",)
            if status is ProjectionBurdenStatus.LIMITED
            else ()
        ),
    )


def build_projection_burden_consumption_receipt(
    report: ProjectionBurdenReportV1,
    *,
    consumer_kind: ProjectionBurdenConsumerKind,
    consumer_id: str,
    model_id: str,
    consumed_slice_ids: Sequence[str],
    hawkes_binding: ProjectionBurdenHawkesBindingV1 | None = None,
) -> ProjectionBurdenConsumptionReceiptV1:
    """Build one exact receipt for a release consumer."""
    decision = report.decision(model_id)
    slice_ids = tuple(consumed_slice_ids)
    available = {item.slice_id for item in report.slices}
    if not slice_ids or not set(slice_ids).issubset(available):
        raise ValueError("projection receipt consumes missing report slices")
    kind = ProjectionBurdenConsumerKind(consumer_kind)
    if kind is ProjectionBurdenConsumerKind.ERA_AUDIT and not any(
        item.slice_id in slice_ids
        and item.slice_kind is ProjectionBurdenSliceKind.ERA
        for item in report.slices
    ):
        raise ValueError("projection era-audit receipt lacks an era slice")
    if kind is ProjectionBurdenConsumerKind.HAWKES_SELECTION:
        if (
            hawkes_binding is None
            or hawkes_binding.report_id != report.report_id
        ):
            raise ValueError(
                "projection Hawkes receipt lacks its exact binding"
            )
        if hawkes_binding.selected_engine_id != model_id:
            raise ValueError("projection Hawkes receipt model differs")
        status = hawkes_binding.binding_status
        limitations = hawkes_binding.limitation_codes
        binding_id: str | None = hawkes_binding.binding_id
    else:
        if hawkes_binding is not None:
            raise ValueError(
                "non-Hawkes projection receipt includes a Hawkes binding"
            )
        status = decision.status
        limitations = decision.limitation_codes
        binding_id = None
    return ProjectionBurdenConsumptionReceiptV1(
        report_id=report.report_id,
        policy_id=report.policy.policy_id,
        consumer_kind=kind,
        consumer_id=consumer_id,
        model_id=model_id,
        model_decision_id=decision.decision_id,
        consumed_slice_ids=slice_ids,
        hawkes_binding_id=binding_id,
        status=status,
        limitation_codes=limitations,
    )


def verify_projection_burden_release_coverage(
    report: ProjectionBurdenReportV1,
    receipts: Sequence[ProjectionBurdenConsumptionReceiptV1],
    *,
    required_consumer_ids: Mapping[str, tuple[str, ...]],
) -> None:
    """Require exact receipts for every publication and certification surface."""
    coverage = build_projection_burden_release_coverage(
        report,
        receipts,
        required_consumer_ids=required_consumer_ids,
    )
    if not coverage.release_coverage_valid:
        raise ValueError("projection release consumer coverage differs")
    if any(
        (
            coverage.excessive_projection_burden_product_count,
            coverage.synthetic_post_projection_residual_failure_count,
            coverage.final_residual_only_projection_pass_count,
        )
    ):
        raise ValueError(
            "projection release evidence contains blocking findings"
        )


def build_projection_burden_release_coverage(
    report: ProjectionBurdenReportV1,
    receipts: Sequence[ProjectionBurdenConsumptionReceiptV1],
    *,
    required_consumer_ids: Mapping[str, tuple[str, ...]],
) -> ProjectionBurdenReleaseCoverageV1:
    """Build the typed scalar handoff consumed by certification."""
    items = tuple(receipts)
    if not items or len(items) > MAX_PROJECTION_RECEIPTS:
        raise ValueError("projection release receipts are empty or unbounded")
    if len({item.receipt_id for item in items}) != len(items):
        raise ValueError("projection release receipts duplicate")
    decisions = {item.model_id: item for item in report.model_decisions}
    slice_ids = {item.slice_id for item in report.slices}
    for item in items:
        if (
            item.report_id != report.report_id
            or item.policy_id != report.policy.policy_id
        ):
            raise ValueError("projection release receipt is stale")
        decision = decisions.get(item.model_id)
        if decision is None or item.model_decision_id != decision.decision_id:
            raise ValueError(
                "projection release receipt model decision is stale"
            )
        if not set(item.consumed_slice_ids).issubset(slice_ids):
            raise ValueError("projection release receipt slice is stale")
    excessive = sum(
        any(
            "projection_burden_exceeded" in code
            for code in item.hard_failure_codes
        )
        for item in report.model_decisions
    )
    post_failures = sum(
        item.synthetic_post_residual_failure_count
        for item in report.model_decisions
    )
    residual_only_passes = sum(
        item.status is ProjectionBurdenStatus.PASS
        and item.masked_by_final_residual_count > 0
        for item in report.model_decisions
    )
    actual_consumer_ids = {
        kind.value: tuple(
            sorted(
                item.consumer_id for item in items if item.consumer_kind is kind
            )
        )
        for kind in ProjectionBurdenConsumerKind
    }
    normalized_required_consumer_ids = {
        _required_text(str(key)): _normalized_text_tuple(value)
        for key, value in sorted(required_consumer_ids.items())
    }
    return ProjectionBurdenReleaseCoverageV1(
        report_id=report.report_id,
        policy_id=report.policy.policy_id,
        required_consumer_ids=normalized_required_consumer_ids,
        receipts=items,
        release_coverage_valid=(
            actual_consumer_ids == normalized_required_consumer_ids
            and all(
                item.status is not ProjectionBurdenStatus.FAIL for item in items
            )
            and not any((excessive, post_failures, residual_only_passes))
        ),
        excessive_projection_burden_product_count=excessive,
        synthetic_post_projection_residual_failure_count=post_failures,
        final_residual_only_projection_pass_count=residual_only_passes,
    )


def write_projection_burden_report(
    report: ProjectionBurdenReportV1, output_directory: str | Path
) -> ArtifactRef:
    """Write one bounded content-addressed report and verify readback."""
    root = Path(output_directory).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    encoded = report.to_json().encode("utf-8") + b"\n"
    if len(encoded) > MAX_PROJECTION_ARTIFACT_BYTES:
        raise ValueError("projection burden report exceeds byte bound")
    digest = hashlib.sha256(encoded).hexdigest()
    target = root / f"projection-burden-report-{digest}.json"
    _write_once(target, encoded)
    if read_projection_burden_report(target) != report:
        raise ValueError(
            "published projection burden report differs on readback"
        )
    return ArtifactRef(
        kind=PROJECTION_BURDEN_REPORT_ARTIFACT_KIND,
        path=str(target),
        size_bytes=len(encoded),
        sha256=digest,
        metadata={
            "report_id": report.report_id,
            "policy_id": report.policy.policy_id,
            "release_status": report.release_status.value,
        },
    )


def read_projection_burden_report(
    path: str | Path,
) -> ProjectionBurdenReportV1:
    """Read and verify one content-addressed projection report."""
    source = Path(path).expanduser().resolve()
    if source.stat().st_size > MAX_PROJECTION_ARTIFACT_BYTES:
        raise ValueError("projection burden report exceeds byte bound")
    digest = source.name.removeprefix("projection-burden-report-").removesuffix(
        ".json"
    )
    encoded = source.read_bytes()
    if len(digest) != 64 or hashlib.sha256(encoded).hexdigest() != digest:
        raise ValueError("projection burden report content address differs")
    try:
        text = encoded.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError("projection burden report is not UTF-8") from error
    return ProjectionBurdenReportV1.from_json(text)


def write_projection_burden_release_coverage(
    coverage: ProjectionBurdenReleaseCoverageV1, output_directory: str | Path
) -> ArtifactRef:
    """Write the content-addressed certification handoff and verify readback."""
    root = Path(output_directory).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    encoded = coverage.to_json().encode("utf-8") + b"\n"
    if len(encoded) > MAX_PROJECTION_ARTIFACT_BYTES:
        raise ValueError("projection release coverage exceeds byte bound")
    digest = hashlib.sha256(encoded).hexdigest()
    target = root / f"projection-burden-release-coverage-{digest}.json"
    _write_once(target, encoded)
    if read_projection_burden_release_coverage(target) != coverage:
        raise ValueError(
            "published projection release coverage differs on readback"
        )
    return ArtifactRef(
        kind=PROJECTION_BURDEN_RELEASE_COVERAGE_ARTIFACT_KIND,
        path=str(target),
        size_bytes=len(encoded),
        sha256=digest,
        metadata={
            "coverage_id": coverage.coverage_id,
            "report_id": coverage.report_id,
            "release_coverage_valid": coverage.release_coverage_valid,
        },
    )


def read_projection_burden_release_coverage(
    path: str | Path,
) -> ProjectionBurdenReleaseCoverageV1:
    """Read one exact content-addressed certification handoff."""
    source = Path(path).expanduser().resolve()
    if source.stat().st_size > MAX_PROJECTION_ARTIFACT_BYTES:
        raise ValueError("projection release coverage exceeds byte bound")
    digest = source.name.removeprefix(
        "projection-burden-release-coverage-"
    ).removesuffix(".json")
    encoded = source.read_bytes()
    if len(digest) != 64 or hashlib.sha256(encoded).hexdigest() != digest:
        raise ValueError("projection release coverage content address differs")
    try:
        text = encoded.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError("projection release coverage is not UTF-8") from error
    return ProjectionBurdenReleaseCoverageV1.from_json(text)


def _projection_slice_groups(
    policy: ProjectionBurdenPolicyV1,
    scenarios: Mapping[str, ProjectionBurdenScenarioV1],
    rows: tuple[ProjectionBurdenEventV1, ...],
    baseline: tuple[ProjectionBurdenEventV1, ...],
) -> tuple[
    tuple[
        ProjectionBurdenSliceKind,
        Mapping[str, str],
        tuple[ProjectionBurdenEventV1, ...],
    ],
    ...,
]:
    groups: list[
        tuple[
            ProjectionBurdenSliceKind,
            Mapping[str, str],
            tuple[ProjectionBurdenEventV1, ...],
        ]
    ] = []

    def add(
        kind: ProjectionBurdenSliceKind,
        source: tuple[ProjectionBurdenEventV1, ...],
        axes: tuple[str, ...],
        *,
        transform: Any | None = None,
    ) -> None:
        grouped: dict[tuple[str, ...], list[ProjectionBurdenEventV1]] = {}
        for item in source:
            values = tuple(
                str(
                    transform(item, axis)
                    if transform is not None
                    else getattr(item, axis)
                )
                for axis in axes
            )
            grouped.setdefault(values, []).append(item)
        for key in sorted(grouped):
            dimensions = dict(zip(axes, key, strict=True))
            selected = tuple(grouped[key])
            if any(not item.observed_only for item in selected):
                groups.append((kind, dimensions, selected))

    add(
        ProjectionBurdenSliceKind.GLOBAL_MODEL,
        baseline,
        ("model_id", "model_family"),
    )
    add(
        ProjectionBurdenSliceKind.WINDOW_MEMBER_MODEL,
        baseline,
        ("window_id", "ensemble_member_id", "model_id", "model_family"),
    )
    add(
        ProjectionBurdenSliceKind.VALIDATION_COORDINATE,
        baseline,
        ("model_id", "model_family", "validation_coordinate_id"),
    )
    for kind, axis in (
        (ProjectionBurdenSliceKind.ERA, "era"),
        (ProjectionBurdenSliceKind.SESSION, "session"),
        (ProjectionBurdenSliceKind.EVENT_STATE, "event_state"),
        (ProjectionBurdenSliceKind.ALIGNMENT, "alignment"),
    ):
        add(kind, baseline, ("model_id", "model_family", axis))
    add(
        ProjectionBurdenSliceKind.SCENARIO,
        rows,
        ("model_id", "model_family", "scenario_id"),
    )

    def quote_age(item: ProjectionBurdenEventV1, axis: str) -> str:
        if axis == "model_id":
            return item.model_id
        if axis == "model_family":
            return item.model_family
        return _quote_age_label(
            policy.quote_age_bin_edges_ns, item.quote_age_ns
        )

    add(
        ProjectionBurdenSliceKind.QUOTE_AGE,
        baseline,
        ("model_id", "model_family", "quote_age_bin"),
        transform=quote_age,
    )
    if len(groups) > MAX_PROJECTION_SLICES:
        raise ValueError("projection slice count exceeds bound")
    return tuple(groups)


def _summarize_projection_slice(
    policy: ProjectionBurdenPolicyV1,
    kind: ProjectionBurdenSliceKind,
    dimensions: Mapping[str, str],
    rows: tuple[ProjectionBurdenEventV1, ...],
) -> ProjectionBurdenSliceV1:
    synthetic = tuple(item for item in rows if not item.observed_only)
    observed = tuple(item for item in rows if item.observed_only)
    if not synthetic:
        raise ValueError("projection slice lacks synthetic proposals")
    burdens: list[float] = []
    numerators: list[float] = []
    scales: list[float] = []
    midpoint: list[float] = []
    spread: list[float] = []
    for item in synthetic:
        movement, scale, midpoint_component, spread_component = _event_burden(
            policy, item
        )
        numerators.append(movement)
        scales.append(scale)
        burdens.append(movement / scale)
        midpoint.append(midpoint_component)
        spread.append(spread_component)
    projected_count = sum(item.projected for item in synthetic)
    refusal_count = sum(item.refused_by_hard_limit for item in synthetic)
    priority: dict[str, int] = {}
    for item in synthetic:
        if item.projection_priority_leg != "none":
            priority[item.projection_priority_leg] = (
                priority.get(item.projection_priority_leg, 0) + 1
            )
    post_failures = sum(
        item.post_projection_triangle_residual
        > policy.synthetic_post_residual_tolerance
        for item in synthetic
        if not item.refused_by_hard_limit
    )
    masked = sum(
        item.post_projection_triangle_residual
        <= policy.synthetic_post_residual_tolerance
        and burden > policy.advisory_max_burden
        for item, burden in zip(synthetic, burdens, strict=True)
    )
    path_signed = [
        item.path_metric_post - item.path_metric_pre for item in synthetic
    ]
    spread_signed = [
        item.spread_metric_post - item.spread_metric_pre for item in synthetic
    ]
    return ProjectionBurdenSliceV1(
        slice_kind=kind,
        dimensions=dimensions,
        proposal_count=len(synthetic),
        projected_event_count=projected_count,
        projected_event_rate=projected_count / len(synthetic),
        hard_refusal_count=refusal_count,
        hard_refusal_rate=refusal_count / len(synthetic),
        observed_only_residuals=_distribution(
            [item.pre_projection_triangle_residual for item in observed]
        ),
        synthetic_pre_residuals=_distribution(
            [item.pre_projection_triangle_residual for item in synthetic]
        ),
        synthetic_post_residuals=_distribution(
            [item.post_projection_triangle_residual for item in synthetic]
        ),
        burdens=_distribution(burdens),
        projection_l1_total=sum(numerators),
        scale_total=sum(scales),
        scale_weighted_mean_burden=sum(numerators) / sum(scales),
        midpoint_movement_total=sum(midpoint),
        spread_movement_total=sum(spread),
        projection_priority_leg_counts=priority,
        quote_ages_ns=_distribution(
            [float(item.quote_age_ns) for item in synthetic]
        ),
        burden_quote_age_pearson=_pearson(
            [float(item.quote_age_ns) for item in synthetic], burdens
        ),
        path_metric_signed_change_mean=statistics.fmean(path_signed),
        path_metric_absolute_change_mean=statistics.fmean(
            map(abs, path_signed)
        ),
        spread_metric_signed_change_mean=statistics.fmean(spread_signed),
        spread_metric_absolute_change_mean=statistics.fmean(
            map(abs, spread_signed)
        ),
        masked_by_final_residual_count=masked,
        synthetic_post_residual_failure_count=post_failures,
        event_ids_sha256=_text_digest(item.event_id for item in rows),
        event_content_sha256=_text_digest(
            item.event_content_sha256 for item in rows
        ),
    )


def _model_decision(
    policy: ProjectionBurdenPolicyV1,
    model_id: str,
    model_family: str,
    global_slice: ProjectionBurdenSliceV1,
    scenario_slices: Mapping[tuple[str, str], ProjectionBurdenSliceV1],
) -> ProjectionBurdenModelDecisionV1:
    hard: list[str] = []
    limitations: list[str] = []
    model_scenarios = tuple(
        item
        for (candidate_model_id, _), item in scenario_slices.items()
        if candidate_model_id == model_id
    )
    post_failure_count = sum(
        item.synthetic_post_residual_failure_count for item in model_scenarios
    )
    checks = (
        (
            "mean",
            global_slice.burdens.mean,
            policy.advisory_mean_burden,
            policy.hard_mean_burden,
        ),
        (
            "p90",
            global_slice.burdens.p90,
            policy.advisory_p90_burden,
            policy.hard_p90_burden,
        ),
        (
            "p99",
            global_slice.burdens.p99,
            policy.advisory_p99_burden,
            policy.hard_p99_burden,
        ),
        (
            "maximum",
            global_slice.burdens.maximum,
            policy.advisory_max_burden,
            policy.hard_max_burden,
        ),
        (
            "projected_rate",
            global_slice.projected_event_rate,
            policy.advisory_projected_rate,
            policy.hard_projected_rate,
        ),
    )
    for name, value, advisory, maximum in checks:
        if value > maximum:
            hard.append(f"hard_{name}_projection_burden_exceeded")
        elif value > advisory:
            limitations.append(f"advisory_{name}_projection_burden_exceeded")
    if global_slice.proposal_count < policy.minimum_proposals_per_model:
        hard.append("minimum_projection_proposal_support_not_met")
    if post_failure_count:
        hard.append("synthetic_post_projection_residual_blocking")
    missed: list[str] = []
    for scenario_id in policy.required_misspecification_scenario_ids:
        item = scenario_slices.get((model_id, scenario_id))
        if item is None or (
            item.burdens.mean
            < policy.misspecification_detection_minimum_mean_burden
            and not item.hard_refusal_count
        ):
            missed.append(scenario_id)
    return ProjectionBurdenModelDecisionV1(
        model_id=model_id,
        model_family=model_family,
        global_slice_id=global_slice.slice_id,
        proposal_count=global_slice.proposal_count,
        hard_failure_codes=tuple(hard),
        limitation_codes=tuple(limitations),
        missed_misspecification_scenario_ids=tuple(missed),
        masked_by_final_residual_count=global_slice.masked_by_final_residual_count,
        synthetic_post_residual_failure_count=(post_failure_count),
        status=(
            ProjectionBurdenStatus.FAIL
            if hard or missed
            else (
                ProjectionBurdenStatus.LIMITED
                if limitations
                else ProjectionBurdenStatus.PASS
            )
        ),
    )


def _model_comparisons(
    policy: ProjectionBurdenPolicyV1,
    slices: Sequence[ProjectionBurdenSliceV1],
    models: tuple[str, ...],
) -> tuple[ProjectionBurdenModelComparisonV1, ...]:
    cells: dict[str, dict[tuple[str, str], ProjectionBurdenSliceV1]] = {
        model: {} for model in models
    }
    for item in slices:
        if item.slice_kind is not ProjectionBurdenSliceKind.WINDOW_MEMBER_MODEL:
            continue
        key = (
            item.dimensions["window_id"],
            item.dimensions["ensemble_member_id"],
        )
        cells[item.dimensions["model_id"]][key] = item
    comparisons: list[ProjectionBurdenModelComparisonV1] = []
    for left_index, left in enumerate(models):
        for right in models[left_index + 1 :]:
            matched = sorted(set(cells[left]).intersection(cells[right]))
            if not matched:
                raise ValueError(
                    "projection model comparison lacks matched cells"
                )
            left_mean = statistics.fmean(
                cells[left][key].burdens.mean for key in matched
            )
            right_mean = statistics.fmean(
                cells[right][key].burdens.mean for key in matched
            )
            left_ratio = (left_mean + policy.comparison_scale_floor) / (
                right_mean + policy.comparison_scale_floor
            )
            right_ratio = (right_mean + policy.comparison_scale_floor) / (
                left_mean + policy.comparison_scale_floor
            )
            conclusion = (
                ProjectionComparisonConclusion.LEFT_EXCESSIVE
                if left_ratio > policy.maximum_comparator_burden_ratio
                else (
                    ProjectionComparisonConclusion.RIGHT_EXCESSIVE
                    if right_ratio > policy.maximum_comparator_burden_ratio
                    else ProjectionComparisonConclusion.EQUIVALENT
                )
            )
            comparisons.append(
                ProjectionBurdenModelComparisonV1(
                    left_model_id=left,
                    right_model_id=right,
                    matched_cell_count=len(matched),
                    left_mean_burden=left_mean,
                    right_mean_burden=right_mean,
                    left_to_right_ratio=left_ratio,
                    right_to_left_ratio=right_ratio,
                    maximum_permitted_ratio=policy.maximum_comparator_burden_ratio,
                    conclusion=conclusion,
                )
            )
    return tuple(comparisons)


def _event_burden(
    policy: ProjectionBurdenPolicyV1, item: ProjectionBurdenEventV1
) -> tuple[float, float, float, float]:
    movement = 0.0
    scale = 0.0
    midpoint_total = 0.0
    spread_total = 0.0
    for symbol in TRIANGLE_SYMBOLS:
        pre_bid, pre_ask = item.pre_projection_quotes[symbol]
        post_bid, post_ask = item.post_projection_quotes[symbol]
        bid_delta = post_bid - pre_bid
        ask_delta = post_ask - pre_ask
        leg_l1 = abs(bid_delta) + abs(ask_delta)
        midpoint_axis = abs(bid_delta + ask_delta)
        spread_axis = abs(ask_delta - bid_delta)
        shared = min(midpoint_axis, spread_axis)
        midpoint_component = (
            max(midpoint_axis - spread_axis, 0.0) + shared / 2.0
        )
        spread_component = max(spread_axis - midpoint_axis, 0.0) + shared / 2.0
        if not math.isclose(
            midpoint_component + spread_component,
            leg_l1,
            rel_tol=1e-12,
            abs_tol=1e-12,
        ):
            raise ValueError("projection movement decomposition failed")
        movement += leg_l1
        midpoint_total += midpoint_component
        spread_total += spread_component
        scale += max(pre_ask - pre_bid, policy.spread_epsilon)
    if scale <= 0.0:
        raise ValueError("projection primary scale is not strictly positive")
    return movement, scale, midpoint_total, spread_total


def _distribution(values: Sequence[float]) -> ProjectionBurdenDistributionV1:
    ordered = tuple(
        sorted(
            _nonnegative_float(item, "distribution value") for item in values
        )
    )
    if not ordered:
        return ProjectionBurdenDistributionV1(
            count=0,
            mean=0.0,
            total=0.0,
            p50=0.0,
            p90=0.0,
            p99=0.0,
            maximum=0.0,
        )
    total = sum(ordered)
    return ProjectionBurdenDistributionV1(
        count=len(ordered),
        mean=total / len(ordered),
        total=total,
        p50=_nearest_rank(ordered, 0.50),
        p90=_nearest_rank(ordered, 0.90),
        p99=_nearest_rank(ordered, 0.99),
        maximum=ordered[-1],
    )


def _nearest_rank(values: Sequence[float], quantile: float) -> float:
    index = max(0, math.ceil(quantile * len(values)) - 1)
    return values[index]


def _pearson(left: Sequence[float], right: Sequence[float]) -> float:
    if len(left) != len(right) or len(left) < 2:
        return 0.0
    left_mean = statistics.fmean(left)
    right_mean = statistics.fmean(right)
    numerator = sum(
        (x - left_mean) * (y - right_mean)
        for x, y in zip(left, right, strict=True)
    )
    left_scale = math.sqrt(sum((x - left_mean) ** 2 for x in left))
    right_scale = math.sqrt(sum((y - right_mean) ** 2 for y in right))
    if left_scale <= 1e-30 or right_scale <= 1e-30:
        return 0.0
    return max(-1.0, min(1.0, numerator / (left_scale * right_scale)))


def _quote_age_label(edges: tuple[int, ...], age: int) -> str:
    for lower, upper in pairwise(edges):
        if lower <= age < upper:
            return f"[{lower},{upper})"
    return f"[{edges[-1]},inf)"


def _validate_slice_dimensions(
    kind: ProjectionBurdenSliceKind, dimensions: Mapping[str, str]
) -> None:
    expected = {
        ProjectionBurdenSliceKind.GLOBAL_MODEL: {"model_id", "model_family"},
        ProjectionBurdenSliceKind.WINDOW_MEMBER_MODEL: {
            "window_id",
            "ensemble_member_id",
            "model_id",
            "model_family",
        },
        ProjectionBurdenSliceKind.VALIDATION_COORDINATE: {
            "model_id",
            "model_family",
            "validation_coordinate_id",
        },
        ProjectionBurdenSliceKind.ERA: {"model_id", "model_family", "era"},
        ProjectionBurdenSliceKind.SESSION: {
            "model_id",
            "model_family",
            "session",
        },
        ProjectionBurdenSliceKind.EVENT_STATE: {
            "model_id",
            "model_family",
            "event_state",
        },
        ProjectionBurdenSliceKind.ALIGNMENT: {
            "model_id",
            "model_family",
            "alignment",
        },
        ProjectionBurdenSliceKind.SCENARIO: {
            "model_id",
            "model_family",
            "scenario_id",
        },
        ProjectionBurdenSliceKind.QUOTE_AGE: {
            "model_id",
            "model_family",
            "quote_age_bin",
        },
    }[kind]
    if set(dimensions) != expected:
        raise ValueError("projection slice dimensions differ from kind")


def _validate_projection_report_topology(
    policy: ProjectionBurdenPolicyV1,
    scenarios: tuple[ProjectionBurdenScenarioV1, ...],
    slices: tuple[ProjectionBurdenSliceV1, ...],
    decisions: tuple[ProjectionBurdenModelDecisionV1, ...],
    comparisons: tuple[ProjectionBurdenModelComparisonV1, ...],
    source_event_count: int,
) -> None:
    """Reject internally incomplete or cross-model report assemblies."""
    global_slices = tuple(
        item
        for item in slices
        if item.slice_kind is ProjectionBurdenSliceKind.GLOBAL_MODEL
    )
    model_ids = tuple(
        sorted(item.dimensions["model_id"] for item in global_slices)
    )
    if not model_ids or len(set(model_ids)) != len(model_ids):
        raise ValueError("projection report global-model topology differs")
    model_families = {
        item.dimensions["model_id"]: item.dimensions["model_family"]
        for item in global_slices
    }
    slice_coordinates = {
        (
            item.slice_kind,
            tuple(sorted(item.dimensions.items())),
        )
        for item in slices
    }
    if len(slice_coordinates) != len(slices):
        raise ValueError("projection report slice coordinates duplicate")
    if any(
        item.dimensions["model_id"] not in model_families
        or item.dimensions["model_family"]
        != model_families[item.dimensions["model_id"]]
        for item in slices
    ):
        raise ValueError("projection report model-family topology differs")
    required_slice_kinds = set(ProjectionBurdenSliceKind)
    for model_id in model_ids:
        if {
            item.slice_kind
            for item in slices
            if item.dimensions["model_id"] == model_id
        } != required_slice_kinds:
            raise ValueError(
                "projection report required slice-kind coverage differs"
            )

    scenario_ids = {item.scenario_id for item in scenarios}
    misspecification_ids = {
        item.scenario_id
        for item in scenarios
        if item.scenario_kind is ProjectionScenarioKind.MISSPECIFICATION
    }
    if misspecification_ids != set(
        policy.required_misspecification_scenario_ids
    ) or not any(
        item.scenario_kind is ProjectionScenarioKind.BASELINE
        for item in scenarios
    ):
        raise ValueError("projection report scenario topology differs")
    scenario_slices = tuple(
        item
        for item in slices
        if item.slice_kind is ProjectionBurdenSliceKind.SCENARIO
    )
    expected_scenario_coordinates = {
        (model_id, scenario_id)
        for model_id in model_ids
        for scenario_id in scenario_ids
    }
    actual_scenario_coordinates = {
        (item.dimensions["model_id"], item.dimensions["scenario_id"])
        for item in scenario_slices
    }
    if actual_scenario_coordinates != expected_scenario_coordinates or len(
        scenario_slices
    ) != len(expected_scenario_coordinates):
        raise ValueError("projection report scenario-slice topology differs")
    if (
        sum(
            item.proposal_count + item.observed_only_residuals.count
            for item in scenario_slices
        )
        != source_event_count
    ):
        raise ValueError("projection report source-event coverage differs")

    if len(decisions) != len(model_ids) or {
        item.model_id for item in decisions
    } != set(model_ids):
        raise ValueError("projection report model-decision coverage differs")
    global_by_model = {
        item.dimensions["model_id"]: item for item in global_slices
    }
    if any(
        item.model_family != model_families[item.model_id]
        or item.global_slice_id != global_by_model[item.model_id].slice_id
        or item.proposal_count != global_by_model[item.model_id].proposal_count
        for item in decisions
    ):
        raise ValueError("projection report model-decision topology differs")

    expected_comparison_pairs = {
        (left, right)
        for index, left in enumerate(model_ids)
        for right in model_ids[index + 1 :]
    }
    actual_comparison_pairs = {
        tuple(sorted((item.left_model_id, item.right_model_id)))
        for item in comparisons
    }
    if actual_comparison_pairs != expected_comparison_pairs or len(
        comparisons
    ) != len(expected_comparison_pairs):
        raise ValueError("projection report model-comparison topology differs")


def _find_model_comparison(
    report: ProjectionBurdenReportV1, left: str, right: str
) -> ProjectionBurdenModelComparisonV1:
    pair = {left, right}
    for item in report.model_comparisons:
        if {item.left_model_id, item.right_model_id} == pair:
            return item
    raise ValueError("projection report lacks the selected-model comparison")


def _comparison_rejects_selected(
    conclusion: ProjectionComparisonConclusion,
    selected: str,
    excluded: str,
) -> bool:
    ordered = tuple(sorted((selected, excluded)))
    excessive = (
        ordered[0]
        if conclusion is ProjectionComparisonConclusion.LEFT_EXCESSIVE
        else (
            ordered[1]
            if conclusion is ProjectionComparisonConclusion.RIGHT_EXCESSIVE
            else None
        )
    )
    return excessive == selected


def _report_findings(
    decisions: Sequence[ProjectionBurdenModelDecisionV1],
    comparisons: Sequence[ProjectionBurdenModelComparisonV1],
) -> tuple[str, ...]:
    findings: set[str] = set()
    for decision in decisions:
        findings.update(
            f"model={decision.model_id}:{code}"
            for code in decision.hard_failure_codes
        )
        findings.update(
            f"model={decision.model_id}:{code}"
            for code in decision.limitation_codes
        )
        findings.update(
            f"model={decision.model_id}:misspecification_not_detected={scenario_id}"
            for scenario_id in decision.missed_misspecification_scenario_ids
        )
    for comparison in comparisons:
        if (
            comparison.conclusion
            is not ProjectionComparisonConclusion.EQUIVALENT
        ):
            findings.add(
                f"models={comparison.left_model_id},{comparison.right_model_id}:"
                f"{comparison.conclusion.value}"
            )
    return tuple(sorted(findings))


def _quotes(
    values: Mapping[str, tuple[float, float]], name: str
) -> dict[str, tuple[float, float]]:
    if set(values) != set(TRIANGLE_SYMBOLS):
        raise ValueError(f"{name} must contain the exact triangle")
    result: dict[str, tuple[float, float]] = {}
    for symbol in TRIANGLE_SYMBOLS:
        value = values[symbol]
        if (
            not isinstance(value, Sequence)
            or isinstance(value, (str, bytes))
            or len(value) != 2
        ):
            raise TypeError(f"{name}.{symbol} must be one bid/ask pair")
        bid = _positive_float(value[0], f"{name}.{symbol}.bid")
        ask = _positive_float(value[1], f"{name}.{symbol}.ask")
        if ask < bid:
            raise ValueError(f"{name}.{symbol} is crossed")
        result[symbol] = (bid, ask)
    return result


def _quote_mapping(value: Any, name: str) -> dict[str, tuple[float, float]]:
    mapping = _mapping(value, name)
    result: dict[str, tuple[float, float]] = {}
    for key, pair in mapping.items():
        sequence = _sequence(pair, f"{name}.{key}")
        if len(sequence) != 2:
            raise ValueError(f"{name}.{key} must contain bid and ask")
        result[str(key)] = (
            _finite_float(sequence[0], f"{name}.{key}.bid"),
            _finite_float(sequence[1], f"{name}.{key}.ask"),
        )
    return result


def _require_policy_constants(data: Mapping[str, Any]) -> None:
    expected = {
        "midpoint_spread_decomposition_id": MIDPOINT_SPREAD_DECOMPOSITION_ID,
        "validation_only_thresholds": True,
        "final_residual_alone_sufficient": False,
        "observed_only_residual_enters_burden": False,
        "synthetic_post_residual_failure_blocking": True,
    }
    if any(data.get(key) != value for key, value in expected.items()):
        raise ValueError("projection burden policy fixed semantics differ")
    scale = _mapping(data.get("primary_scale"), "primary_scale")
    scale_expected = {
        "scale_id": PRIMARY_SCALE_ID,
        "definition": "sum_symbol_max_proposal_ask_minus_bid_epsilon",
        "strictly_positive": True,
        "clipping_permitted": False,
        "zero_spread_treatment": "replace_only_zero_symbol_spread_with_epsilon",
    }
    if any(scale.get(key) != value for key, value in scale_expected.items()):
        raise ValueError("projection burden primary scale differs")


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _text_digest(values: Any) -> str:
    digest = hashlib.sha256()
    for value in sorted(str(item) for item in values):
        encoded = value.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def _write_once(path: Path, encoded: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    try:
        descriptor = os.open(path, flags, 0o644)
    except FileExistsError:
        if path.read_bytes() != encoded:
            raise ValueError(
                "projection burden artifact path collides"
            ) from None
        return
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
    except Exception:
        path.unlink(missing_ok=True)
        raise


def _bounded_json(text: str, name: str) -> None:
    if len(text.encode("utf-8")) > MAX_PROJECTION_ARTIFACT_BYTES:
        raise ValueError(f"{name} exceeds byte bound")


def _json_mapping(text: str) -> Mapping[str, Any]:
    try:
        value = json.loads(text)
    except json.JSONDecodeError as error:
        raise ValueError("projection burden JSON is invalid") from error
    return _mapping(value, "JSON root")


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    return value


def _sequence(value: Any, name: str) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError(f"{name} must be a sequence")
    return value


def _string_tuple(
    value: Any, name: str, *, allow_empty: bool = False
) -> tuple[str, ...]:
    return _normalized_text_tuple(
        tuple(str(item) for item in _sequence(value, name)),
        allow_empty=allow_empty,
    )


def _normalized_text_tuple(
    values: Sequence[str], *, allow_empty: bool = False
) -> tuple[str, ...]:
    normalized = tuple(sorted({_required_text(item) for item in values}))
    if not normalized and not allow_empty:
        raise ValueError("projection text collection is empty")
    return normalized


def _required_text(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("projection value must be nonempty text")
    return value.strip()


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


def _sha256(value: Any, name: str) -> str:
    text = _required_text(value)
    if len(text) != 64 or any(
        character not in "0123456789abcdef" for character in text
    ):
        raise ValueError(f"{name} must be lowercase SHA-256")
    return text


def _require_schema(value: str, expected: str) -> None:
    if value != expected:
        raise ValueError("unsupported projection burden schema version")


__all__ = [
    "MIDPOINT_SPREAD_DECOMPOSITION_ID",
    "PRIMARY_SCALE_ID",
    "PROJECTION_BURDEN_CONSUMPTION_RECEIPT_SCHEMA_VERSION",
    "PROJECTION_BURDEN_DISTRIBUTION_SCHEMA_VERSION",
    "PROJECTION_BURDEN_EVENT_SCHEMA_VERSION",
    "PROJECTION_BURDEN_HAWKES_BINDING_SCHEMA_VERSION",
    "PROJECTION_BURDEN_MODEL_COMPARISON_SCHEMA_VERSION",
    "PROJECTION_BURDEN_MODEL_DECISION_SCHEMA_VERSION",
    "PROJECTION_BURDEN_POLICY_SCHEMA_VERSION",
    "PROJECTION_BURDEN_RELEASE_COVERAGE_ARTIFACT_KIND",
    "PROJECTION_BURDEN_RELEASE_COVERAGE_SCHEMA_VERSION",
    "PROJECTION_BURDEN_REPORT_ARTIFACT_KIND",
    "PROJECTION_BURDEN_REPORT_SCHEMA_VERSION",
    "PROJECTION_BURDEN_SCENARIO_SCHEMA_VERSION",
    "PROJECTION_BURDEN_SLICE_SCHEMA_VERSION",
    "REQUIRED_RELEASE_CONSUMERS",
    "ProjectionBurdenConsumerKind",
    "ProjectionBurdenConsumptionReceiptV1",
    "ProjectionBurdenDistributionV1",
    "ProjectionBurdenEventV1",
    "ProjectionBurdenHawkesBindingV1",
    "ProjectionBurdenModelComparisonV1",
    "ProjectionBurdenModelDecisionV1",
    "ProjectionBurdenPolicyV1",
    "ProjectionBurdenReleaseCoverageV1",
    "ProjectionBurdenReportV1",
    "ProjectionBurdenScenarioV1",
    "ProjectionBurdenSliceKind",
    "ProjectionBurdenSliceV1",
    "ProjectionBurdenStatus",
    "ProjectionComparisonConclusion",
    "ProjectionScenarioKind",
    "bind_projection_burden_to_hawkes_selection",
    "build_projection_burden_consumption_receipt",
    "build_projection_burden_release_coverage",
    "derive_projection_burden_report",
    "read_projection_burden_release_coverage",
    "read_projection_burden_report",
    "verify_projection_burden_release_coverage",
    "write_projection_burden_release_coverage",
    "write_projection_burden_report",
]
