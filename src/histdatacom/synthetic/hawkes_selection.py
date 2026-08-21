"""Validation-only product selection for diagonal versus full marked Hawkes.

The powered qualification dossier answers whether an engine may reconstruct.
This module answers the separate product question: which of the two qualified
marked-Hawkes excitation structures is selected before a fresh release
holdout is opened.  Selection is replayed from paired, row-free validation
evidence and a content-addressed predeclared policy.
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

from histdatacom.orchestration.reconstruction import (
    artifact_ref_for_file,
    verify_artifact_ref,
)
from histdatacom.runtime_contracts import ArtifactRef, JSONValue
from histdatacom.synthetic.contracts import canonical_contract_json
from histdatacom.synthetic.qualification import (
    PoweredQualificationDossierV1,
    read_powered_qualification_dossier,
    verify_powered_qualification_dossier,
)

HAWKES_PRODUCT_SELECTION_POLICY_SCHEMA_VERSION = (
    "histdatacom.hawkes-product-selection-policy.v1"
)
HAWKES_VALIDATION_COORDINATE_SCHEMA_VERSION = (
    "histdatacom.hawkes-validation-coordinate.v1"
)
HAWKES_VALIDATION_OBSERVATION_SCHEMA_VERSION = (
    "histdatacom.hawkes-validation-observation.v1"
)
HAWKES_VALIDATION_COMPARISON_SCHEMA_VERSION = (
    "histdatacom.hawkes-validation-comparison.v1"
)
HAWKES_FINAL_PRODUCT_RESIDUAL_REPORT_SCHEMA_VERSION = (
    "histdatacom.hawkes-final-product-residual-report.v1"
)
HAWKES_METRIC_COMPARISON_SCHEMA_VERSION = (
    "histdatacom.hawkes-metric-comparison.v1"
)
HAWKES_PRODUCT_SELECTION_DOSSIER_SCHEMA_VERSION = (
    "histdatacom.hawkes-product-selection-dossier.v1"
)
HAWKES_PRODUCT_SELECTION_DOSSIER_ARTIFACT_KIND = (
    "hawkes_product_selection_dossier_v1"
)

DIAGONAL_HAWKES_ENGINE_ID = "histdatacom.marked-hawkes.diagonal_self_excitation"
FULL_HAWKES_ENGINE_ID = "histdatacom.marked-hawkes.full_self_cross_excitation"
HAWKES_SELECTION_ENGINE_IDS = (
    DIAGONAL_HAWKES_ENGINE_ID,
    FULL_HAWKES_ENGINE_ID,
)

MAX_SELECTION_CELLS = 4096
MAX_SELECTION_ARTIFACTS = 64
MAX_SELECTION_ARTIFACT_BYTES = 64 * 1024 * 1024


class HawkesMetricDirection(str, Enum):
    """Whether smaller or larger values are scientifically preferable."""

    LOWER = "lower"
    HIGHER = "higher"


class HawkesComparisonConclusion(str, Enum):
    """Predeclared conclusion for one powered paired comparison."""

    DIAGONAL = "favors_diagonal"
    FULL = "favors_full"
    EQUIVALENT = "practically_equivalent"
    INCONCLUSIVE = "inconclusive"


class HawkesValidationEra(str, Enum):
    """Required transport strata for the product decision."""

    EARLY = "early"
    TRANSITION = "transition"
    MODERN = "modern"


METRIC_DIRECTIONS: Mapping[str, HawkesMetricDirection] = {
    "raw_triangle_residual": HawkesMetricDirection.LOWER,
    "projection_count": HawkesMetricDirection.LOWER,
    "projection_burden": HawkesMetricDirection.LOWER,
    "post_triangle_residual": HawkesMetricDirection.LOWER,
    "event_count_error": HawkesMetricDirection.LOWER,
    "interarrival_error": HawkesMetricDirection.LOWER,
    "time_rescaling_error": HawkesMetricDirection.LOWER,
    "mark_error": HawkesMetricDirection.LOWER,
    "spread_error": HawkesMetricDirection.LOWER,
    "path_error": HawkesMetricDirection.LOWER,
    "tail_error": HawkesMetricDirection.LOWER,
    "maximum_spectral_radius": HawkesMetricDirection.LOWER,
    "stability_margin": HawkesMetricDirection.HIGHER,
    "fit_sensitivity": HawkesMetricDirection.LOWER,
    "era_transport_error": HawkesMetricDirection.LOWER,
    "adaptive_partition_sensitivity": HawkesMetricDirection.LOWER,
    "generation_failure_rate": HawkesMetricDirection.LOWER,
    "generation_refusal_rate": HawkesMetricDirection.LOWER,
    "runtime_seconds": HawkesMetricDirection.LOWER,
    "peak_memory_bytes": HawkesMetricDirection.LOWER,
    "poisson_work": HawkesMetricDirection.LOWER,
    "output_bytes": HawkesMetricDirection.LOWER,
    "amplification": HawkesMetricDirection.LOWER,
    "ensemble_diversity": HawkesMetricDirection.HIGHER,
}
PRIMARY_METRIC_IDS = (
    "raw_triangle_residual",
    "projection_burden",
)
RESOURCE_METRIC_IDS = (
    "runtime_seconds",
    "peak_memory_bytes",
    "poisson_work",
    "output_bytes",
    "amplification",
)
SCIENTIFIC_METRIC_IDS = tuple(
    name for name in METRIC_DIRECTIONS if name not in RESOURCE_METRIC_IDS
)
FINAL_PRODUCT_DIAGNOSTIC_METRICS = (
    "event_count_error",
    "interarrival_error",
    "time_rescaling_error",
    "mark_error",
    "post_triangle_residual",
    "spread_error",
    "path_error",
    "tail_error",
    "generation_failure_rate",
    "generation_refusal_rate",
    "ensemble_diversity",
)


@dataclass(frozen=True, slots=True)
class HawkesProductSelectionPolicyV1:
    """Predeclared paired-comparison and tie-break policy."""

    alpha: float = 0.05
    minimum_power: float = 0.80
    minimum_paired_cells: int = 6
    maximum_spectral_radius: float = 0.98
    relative_scale_floor: float = 1e-12
    projection_spread_epsilon: float = 1e-9
    materiality_margins: Mapping[str, float] = field(
        default_factory=lambda: dict.fromkeys(METRIC_DIRECTIONS, 0.10)
    )
    policy_id: str = ""
    schema_version: str = HAWKES_PRODUCT_SELECTION_POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            HAWKES_PRODUCT_SELECTION_POLICY_SCHEMA_VERSION,
        )
        for name in ("alpha", "minimum_power"):
            value = _finite_float(getattr(self, name), name)
            if not 0.0 < value < 1.0:
                raise ValueError(f"{name} must be in (0, 1)")
            object.__setattr__(self, name, value)
        if self.minimum_power <= 0.5:
            raise ValueError("selection minimum power must exceed 0.5")
        if (
            isinstance(self.minimum_paired_cells, bool)
            or not isinstance(self.minimum_paired_cells, int)
            or not 3 <= self.minimum_paired_cells <= MAX_SELECTION_CELLS
        ):
            raise ValueError("selection paired-cell minimum is invalid")
        radius = _finite_float(
            self.maximum_spectral_radius, "maximum_spectral_radius"
        )
        if not 0.0 < radius < 1.0:
            raise ValueError("maximum spectral radius must be in (0, 1)")
        object.__setattr__(self, "maximum_spectral_radius", radius)
        object.__setattr__(
            self,
            "relative_scale_floor",
            _positive_float(self.relative_scale_floor, "relative_scale_floor"),
        )
        object.__setattr__(
            self,
            "projection_spread_epsilon",
            _positive_float(
                self.projection_spread_epsilon,
                "projection_spread_epsilon",
            ),
        )
        margins = {
            _required_text(key): _positive_float(value, f"margin {key}")
            for key, value in self.materiality_margins.items()
        }
        if set(margins) != set(METRIC_DIRECTIONS):
            raise ValueError(
                "selection materiality margins differ from metrics"
            )
        if any(value >= 1.0 for value in margins.values()):
            raise ValueError("selection materiality margins must be below one")
        object.__setattr__(
            self, "materiality_margins", dict(sorted(margins.items()))
        )
        expected = _stable_id("hawkes-product-selection-policy", self.payload())
        if self.policy_id and self.policy_id != expected:
            raise ValueError("Hawkes selection policy identity differs")
        object.__setattr__(self, "policy_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "candidate_engine_ids": list(HAWKES_SELECTION_ENGINE_IDS),
            "validation_split_only": True,
            "final_holdout_permitted": False,
            "alpha": self.alpha,
            "minimum_power": self.minimum_power,
            "minimum_paired_cells": self.minimum_paired_cells,
            "maximum_spectral_radius": self.maximum_spectral_radius,
            "relative_scale_floor": self.relative_scale_floor,
            "metric_directions": {
                key: value.value for key, value in METRIC_DIRECTIONS.items()
            },
            "materiality_margins": dict(self.materiality_margins),
            "primary_metric_ids": list(PRIMARY_METRIC_IDS),
            "resource_metric_ids": list(RESOURCE_METRIC_IDS),
            "paired_axes": [
                "window_id",
                "degradation_scenario_id",
                "seed",
                "anchor_set_id",
                "adaptive_partition_id",
                "final_constraint_set_id",
                "era",
            ],
            "projection_metric": {
                "metric_id": "dimensionless-projection-burden.v1",
                "numerator": "sum_event_l1_reconciled_minus_proposal",
                "denominator": "sum_event_max_proposal_spread_epsilon",
                "event_set": "all_proposal_quote_vectors_before_projection",
                "projection_spread_epsilon": self.projection_spread_epsilon,
                "clipping_permitted": False,
                "zero_spread_treatment": "replace_only_denominator_term_with_epsilon",
            },
            "uncertainty_method": "paired-student-t-relative-effect.v1",
            "power_method": "two-sided-normal-approximation-at-materiality.v1",
            "selection_rule": "hard-gates-pareto-equivalence-resource-complexity.v1",
            "complexity_order": list(HAWKES_SELECTION_ENGINE_IDS),
            "automatic_winner_from_repository_order": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "policy_id": self.policy_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> HawkesProductSelectionPolicyV1:
        _require_constant_policy_fields(data)
        return cls(
            alpha=_finite_float(data.get("alpha"), "alpha"),
            minimum_power=_finite_float(
                data.get("minimum_power"), "minimum_power"
            ),
            minimum_paired_cells=_strict_int(
                data.get("minimum_paired_cells"), "minimum_paired_cells"
            ),
            maximum_spectral_radius=_finite_float(
                data.get("maximum_spectral_radius"),
                "maximum_spectral_radius",
            ),
            relative_scale_floor=_finite_float(
                data.get("relative_scale_floor"), "relative_scale_floor"
            ),
            projection_spread_epsilon=_finite_float(
                _mapping(data.get("projection_metric")).get(
                    "projection_spread_epsilon"
                ),
                "projection_spread_epsilon",
            ),
            materiality_margins={
                str(key): _finite_float(value, str(key))
                for key, value in _mapping(
                    data.get("materiality_margins")
                ).items()
            },
            policy_id=str(data.get("policy_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> HawkesProductSelectionPolicyV1:
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class HawkesValidationCoordinateV1:
    """One exactly paired validation coordinate."""

    window_id: str
    degradation_scenario_id: str
    seed: int
    anchor_set_id: str
    adaptive_partition_id: str
    final_constraint_set_id: str
    era: HawkesValidationEra
    coordinate_id: str = ""
    schema_version: str = HAWKES_VALIDATION_COORDINATE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, HAWKES_VALIDATION_COORDINATE_SCHEMA_VERSION
        )
        for name in (
            "window_id",
            "degradation_scenario_id",
            "anchor_set_id",
            "adaptive_partition_id",
            "final_constraint_set_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        if isinstance(self.seed, bool) or not isinstance(self.seed, int):
            raise TypeError("Hawkes validation seed must be an integer")
        object.__setattr__(self, "era", HawkesValidationEra(self.era))
        expected = _stable_id("hawkes-validation-coordinate", self.payload())
        if self.coordinate_id and self.coordinate_id != expected:
            raise ValueError("Hawkes validation coordinate identity differs")
        object.__setattr__(self, "coordinate_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "window_id": self.window_id,
            "degradation_scenario_id": self.degradation_scenario_id,
            "seed": self.seed,
            "anchor_set_id": self.anchor_set_id,
            "adaptive_partition_id": self.adaptive_partition_id,
            "final_constraint_set_id": self.final_constraint_set_id,
            "era": self.era.value,
            "split_kind": "validation",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "coordinate_id": self.coordinate_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> HawkesValidationCoordinateV1:
        if data.get("split_kind") != "validation":
            raise ValueError("Hawkes product comparison is not validation-only")
        return cls(
            window_id=str(data.get("window_id", "")),
            degradation_scenario_id=str(
                data.get("degradation_scenario_id", "")
            ),
            seed=_strict_int(data.get("seed"), "seed"),
            anchor_set_id=str(data.get("anchor_set_id", "")),
            adaptive_partition_id=str(data.get("adaptive_partition_id", "")),
            final_constraint_set_id=str(
                data.get("final_constraint_set_id", "")
            ),
            era=HawkesValidationEra(str(data.get("era", ""))),
            coordinate_id=str(data.get("coordinate_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class HawkesValidationObservationV1:
    """Row-free aggregate metrics for one engine and paired coordinate."""

    engine_id: str
    coordinate: HawkesValidationCoordinateV1
    metrics: Mapping[str, float]
    projection_l1_numerator: float
    projection_spread_denominator: float
    projection_event_count: int
    observation_id: str = ""
    schema_version: str = HAWKES_VALIDATION_OBSERVATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            HAWKES_VALIDATION_OBSERVATION_SCHEMA_VERSION,
        )
        if self.engine_id not in HAWKES_SELECTION_ENGINE_IDS:
            raise ValueError("unsupported Hawkes selection engine")
        if not isinstance(self.coordinate, HawkesValidationCoordinateV1):
            raise TypeError("Hawkes observation coordinate must use v1")
        metrics = {
            _required_text(key): _finite_float(value, f"metric {key}")
            for key, value in self.metrics.items()
        }
        if set(metrics) != set(METRIC_DIRECTIONS):
            raise ValueError("Hawkes validation metric coverage differs")
        nonnegative = set(METRIC_DIRECTIONS) - {
            "stability_margin",
            "ensemble_diversity",
        }
        if any(metrics[name] < 0.0 for name in nonnegative):
            raise ValueError("Hawkes loss/resource metrics must be nonnegative")
        if not 0.0 <= metrics["maximum_spectral_radius"] < 1.0:
            raise ValueError("Hawkes spectral radius must be in [0, 1)")
        if metrics["stability_margin"] < 0.0:
            raise ValueError("Hawkes stability margin must be nonnegative")
        if not 0.0 <= metrics["ensemble_diversity"] <= 1.0:
            raise ValueError("Hawkes ensemble diversity must be in [0, 1]")
        for name in ("generation_failure_rate", "generation_refusal_rate"):
            if not 0.0 <= metrics[name] <= 1.0:
                raise ValueError(f"{name} must be in [0, 1]")
        numerator = _nonnegative_float(
            self.projection_l1_numerator, "projection_l1_numerator"
        )
        denominator = _positive_float(
            self.projection_spread_denominator,
            "projection_spread_denominator",
        )
        if (
            isinstance(self.projection_event_count, bool)
            or not isinstance(self.projection_event_count, int)
            or self.projection_event_count < 0
        ):
            raise ValueError("projection event count must be nonnegative")
        if not math.isclose(
            metrics["projection_burden"],
            numerator / denominator,
            rel_tol=1e-12,
            abs_tol=1e-12,
        ):
            raise ValueError("projection burden differs from frozen aggregates")
        if not math.isclose(
            metrics["projection_count"],
            float(self.projection_event_count),
            abs_tol=0.0,
        ):
            raise ValueError("projection count differs from retained event set")
        object.__setattr__(self, "metrics", dict(sorted(metrics.items())))
        object.__setattr__(self, "projection_l1_numerator", numerator)
        object.__setattr__(self, "projection_spread_denominator", denominator)
        expected = _stable_id("hawkes-validation-observation", self.payload())
        if self.observation_id and self.observation_id != expected:
            raise ValueError("Hawkes validation observation identity differs")
        object.__setattr__(self, "observation_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "engine_id": self.engine_id,
            "coordinate": self.coordinate.to_dict(),
            "metrics": dict(self.metrics),
            "projection_l1_numerator": self.projection_l1_numerator,
            "projection_spread_denominator": self.projection_spread_denominator,
            "projection_event_count": self.projection_event_count,
            "complete_constrained_generator_metrics": True,
            "carving_applied": True,
            "reconciliation_applied": True,
            "identical_policy_ensemble": True,
            "event_rows_embedded": False,
            "final_holdout_metrics_embedded": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "observation_id": self.observation_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> HawkesValidationObservationV1:
        if (
            data.get("event_rows_embedded") is not False
            or data.get("final_holdout_metrics_embedded") is not False
            or data.get("complete_constrained_generator_metrics") is not True
            or data.get("carving_applied") is not True
            or data.get("reconciliation_applied") is not True
            or data.get("identical_policy_ensemble") is not True
        ):
            raise ValueError(
                "Hawkes comparison contains prohibited rows or holdout"
            )
        return cls(
            engine_id=str(data.get("engine_id", "")),
            coordinate=HawkesValidationCoordinateV1.from_dict(
                _mapping(data.get("coordinate"))
            ),
            metrics={
                str(key): _finite_float(value, str(key))
                for key, value in _mapping(data.get("metrics")).items()
            },
            projection_l1_numerator=_finite_float(
                data.get("projection_l1_numerator"),
                "projection_l1_numerator",
            ),
            projection_spread_denominator=_finite_float(
                data.get("projection_spread_denominator"),
                "projection_spread_denominator",
            ),
            projection_event_count=_strict_int(
                data.get("projection_event_count"), "projection_event_count"
            ),
            observation_id=str(data.get("observation_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class HawkesValidationComparisonV1:
    """Exact paired validation surface consumed by product selection."""

    policy_id: str
    qualification_dossier_id: str
    observations: tuple[HawkesValidationObservationV1, ...]
    evidence_artifacts: Mapping[str, ArtifactRef]
    comparison_id: str = ""
    schema_version: str = HAWKES_VALIDATION_COMPARISON_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, HAWKES_VALIDATION_COMPARISON_SCHEMA_VERSION
        )
        object.__setattr__(self, "policy_id", _required_text(self.policy_id))
        object.__setattr__(
            self,
            "qualification_dossier_id",
            _required_text(self.qualification_dossier_id),
        )
        observations = tuple(
            sorted(self.observations, key=lambda item: item.observation_id)
        )
        if not observations or len(observations) > MAX_SELECTION_CELLS * 2:
            raise ValueError("Hawkes validation observation count is invalid")
        if len({item.observation_id for item in observations}) != len(
            observations
        ):
            raise ValueError("Hawkes validation observations duplicate")
        by_coordinate: dict[str, set[str]] = {}
        coordinate_by_id: dict[str, HawkesValidationCoordinateV1] = {}
        for item in observations:
            coordinate_by_id.setdefault(
                item.coordinate.coordinate_id, item.coordinate
            )
            if (
                coordinate_by_id[item.coordinate.coordinate_id]
                != item.coordinate
            ):
                raise ValueError("Hawkes coordinate identity collides")
            by_coordinate.setdefault(item.coordinate.coordinate_id, set()).add(
                item.engine_id
            )
        if any(
            ids != set(HAWKES_SELECTION_ENGINE_IDS)
            for ids in by_coordinate.values()
        ):
            raise ValueError("Hawkes comparison cells are not exactly paired")
        if len(observations) != len(by_coordinate) * 2:
            raise ValueError(
                "Hawkes comparison contains duplicate engine cells"
            )
        if {item.era for item in coordinate_by_id.values()} != set(
            HawkesValidationEra
        ):
            raise ValueError(
                "Hawkes comparison lacks early/transition/modern coverage"
            )
        artifacts = {
            _required_text(key): value
            for key, value in sorted(self.evidence_artifacts.items())
        }
        if (
            not artifacts
            or len(artifacts) > MAX_SELECTION_ARTIFACTS
            or any(
                not isinstance(value, ArtifactRef)
                for value in artifacts.values()
            )
        ):
            raise TypeError("Hawkes validation evidence artifacts are invalid")
        object.__setattr__(self, "observations", observations)
        object.__setattr__(self, "evidence_artifacts", artifacts)
        expected = _stable_id("hawkes-validation-comparison", self.payload())
        if self.comparison_id and self.comparison_id != expected:
            raise ValueError("Hawkes validation comparison identity differs")
        object.__setattr__(self, "comparison_id", expected)
        _bounded_json(self.to_json(), "Hawkes validation comparison")

    @property
    def coordinate_count(self) -> int:
        return len(
            {item.coordinate.coordinate_id for item in self.observations}
        )

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "policy_id": self.policy_id,
            "qualification_dossier_id": self.qualification_dossier_id,
            "candidate_engine_ids": list(HAWKES_SELECTION_ENGINE_IDS),
            "observations": [item.to_dict() for item in self.observations],
            "evidence_artifacts": {
                key: value.to_dict()
                for key, value in self.evidence_artifacts.items()
            },
            "coordinate_count": self.coordinate_count,
            "validation_only": True,
            "policy_frozen_before_results": True,
            "final_holdout_opened": False,
            "manual_preference": False,
            "event_rows_embedded": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "comparison_id": self.comparison_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> HawkesValidationComparisonV1:
        for name, expected in (
            ("candidate_engine_ids", list(HAWKES_SELECTION_ENGINE_IDS)),
            ("validation_only", True),
            ("policy_frozen_before_results", True),
            ("final_holdout_opened", False),
            ("manual_preference", False),
            ("event_rows_embedded", False),
        ):
            if data.get(name) != expected:
                raise ValueError(f"Hawkes validation comparison {name} differs")
        comparison = cls(
            policy_id=str(data.get("policy_id", "")),
            qualification_dossier_id=str(
                data.get("qualification_dossier_id", "")
            ),
            observations=tuple(
                HawkesValidationObservationV1.from_dict(_mapping(item))
                for item in _sequence(data.get("observations"))
            ),
            evidence_artifacts={
                str(key): ArtifactRef.from_dict(_mapping(value))
                for key, value in _mapping(
                    data.get("evidence_artifacts")
                ).items()
            },
            comparison_id=str(data.get("comparison_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        if data.get("coordinate_count") != comparison.coordinate_count:
            raise ValueError("Hawkes comparison coordinate count differs")
        return comparison

    @classmethod
    def from_json(cls, text: str) -> HawkesValidationComparisonV1:
        _bounded_json(text, "Hawkes validation comparison")
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class HawkesFinalProductResidualReportV1:
    """Row-free simulation-predictive check of the constrained product law."""

    engine_id: str
    policy_id: str
    comparison_id: str
    qualification_dossier_id: str
    coordinate_ids: tuple[str, ...]
    final_constraint_set_ids: tuple[str, ...]
    metric_summaries: Mapping[str, float]
    observation_count: int
    status: str
    reason_codes: tuple[str, ...]
    report_id: str = ""
    schema_version: str = HAWKES_FINAL_PRODUCT_RESIDUAL_REPORT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            HAWKES_FINAL_PRODUCT_RESIDUAL_REPORT_SCHEMA_VERSION,
        )
        if self.engine_id not in HAWKES_SELECTION_ENGINE_IDS:
            raise ValueError("unsupported final-product Hawkes engine")
        for name in (
            "policy_id",
            "comparison_id",
            "qualification_dossier_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        coordinates = tuple(
            sorted({_required_text(item) for item in self.coordinate_ids})
        )
        constraints = tuple(
            sorted(
                {_required_text(item) for item in self.final_constraint_set_ids}
            )
        )
        if not coordinates or not constraints:
            raise ValueError("final-product residual support is empty")
        observation_count = _strict_int(
            self.observation_count, "observation_count"
        )
        if (
            not 1 <= observation_count <= MAX_SELECTION_CELLS
            or observation_count != len(coordinates)
        ):
            raise ValueError("final-product residual observation count differs")
        summaries = {
            _required_text(key): _nonnegative_float(value, key)
            for key, value in self.metric_summaries.items()
        }
        expected_summary_keys = {
            f"{metric}_{suffix}"
            for metric in FINAL_PRODUCT_DIAGNOSTIC_METRICS
            for suffix in ("mean", "q95")
        }
        if set(summaries) != expected_summary_keys:
            raise ValueError("final-product residual metric summaries differ")
        if self.status not in {"available", "insufficient_evidence"}:
            raise ValueError("final-product residual status differs")
        reasons = _text_tuple(self.reason_codes)
        expected_reason = (
            "constrained_product_simulation_predictive_metrics_available"
            if self.status == "available"
            else "constrained_product_coordinate_support_below_policy_minimum"
        )
        if reasons != (expected_reason,):
            raise ValueError("final-product residual reason differs")
        object.__setattr__(self, "coordinate_ids", coordinates)
        object.__setattr__(self, "observation_count", observation_count)
        object.__setattr__(self, "final_constraint_set_ids", constraints)
        object.__setattr__(
            self, "metric_summaries", dict(sorted(summaries.items()))
        )
        object.__setattr__(self, "reason_codes", reasons)
        expected = _stable_id(
            "hawkes-final-product-residual-report", self.payload()
        )
        if self.report_id and self.report_id != expected:
            raise ValueError(
                "Hawkes final-product residual report identity differs"
            )
        object.__setattr__(self, "report_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "engine_id": self.engine_id,
            "policy_id": self.policy_id,
            "comparison_id": self.comparison_id,
            "qualification_dossier_id": self.qualification_dossier_id,
            "coordinate_ids": list(self.coordinate_ids),
            "final_constraint_set_ids": list(self.final_constraint_set_ids),
            "metric_summaries": dict(self.metric_summaries),
            "observation_count": self.observation_count,
            "status": self.status,
            "reason_codes": list(self.reason_codes),
            "diagnostic_stage": "final_constrained_product",
            "method": "simulation_predictive_metric_ensemble.v1",
            "comparison_basis": (
                "realized_final_metrics_vs_identical_policy_ensemble"
            ),
            "carving_applied": True,
            "reconciliation_applied": True,
            "identical_final_constraint_policy_per_coordinate": True,
            "analytic_compensator_applied": False,
            "event_rows_embedded": False,
            "residual_rows_embedded": False,
            "historical_truth_claim": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "report_id": self.report_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> HawkesFinalProductResidualReportV1:
        fixed = {
            "diagnostic_stage": "final_constrained_product",
            "method": "simulation_predictive_metric_ensemble.v1",
            "comparison_basis": (
                "realized_final_metrics_vs_identical_policy_ensemble"
            ),
            "carving_applied": True,
            "reconciliation_applied": True,
            "identical_final_constraint_policy_per_coordinate": True,
            "analytic_compensator_applied": False,
            "event_rows_embedded": False,
            "residual_rows_embedded": False,
            "historical_truth_claim": False,
        }
        if any(data.get(key) != value for key, value in fixed.items()):
            raise ValueError("final-product residual scope or nonclaim differs")
        return cls(
            engine_id=str(data.get("engine_id", "")),
            policy_id=str(data.get("policy_id", "")),
            comparison_id=str(data.get("comparison_id", "")),
            qualification_dossier_id=str(
                data.get("qualification_dossier_id", "")
            ),
            coordinate_ids=_string_tuple(data.get("coordinate_ids")),
            final_constraint_set_ids=_string_tuple(
                data.get("final_constraint_set_ids")
            ),
            metric_summaries={
                str(key): _finite_float(value, str(key))
                for key, value in _mapping(data.get("metric_summaries")).items()
            },
            observation_count=_strict_int(
                data.get("observation_count"), "observation_count"
            ),
            status=str(data.get("status", "")),
            reason_codes=_string_tuple(data.get("reason_codes")),
            report_id=str(data.get("report_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class HawkesMetricComparisonV1:
    """Paired estimate, uncertainty, power, and frozen conclusion."""

    metric_id: str
    direction: HawkesMetricDirection
    diagonal_mean: float
    full_mean: float
    oriented_relative_effect: float
    confidence_low: float
    confidence_high: float
    standard_error: float
    sample_count: int
    materiality_margin: float
    achieved_power: float
    power_sufficient: bool
    conclusion: HawkesComparisonConclusion
    result_id: str = ""
    schema_version: str = HAWKES_METRIC_COMPARISON_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, HAWKES_METRIC_COMPARISON_SCHEMA_VERSION
        )
        object.__setattr__(self, "metric_id", _required_text(self.metric_id))
        if self.metric_id not in METRIC_DIRECTIONS:
            raise ValueError("unknown Hawkes selection metric")
        direction = HawkesMetricDirection(self.direction)
        if direction is not METRIC_DIRECTIONS[self.metric_id]:
            raise ValueError("Hawkes selection metric direction differs")
        object.__setattr__(self, "direction", direction)
        for name in (
            "diagonal_mean",
            "full_mean",
            "oriented_relative_effect",
            "confidence_low",
            "confidence_high",
            "standard_error",
            "materiality_margin",
            "achieved_power",
        ):
            object.__setattr__(
                self, name, _finite_float(getattr(self, name), name)
            )
        if self.confidence_low > self.confidence_high:
            raise ValueError(
                "Hawkes comparison confidence interval is reversed"
            )
        if self.standard_error < 0.0 or self.materiality_margin <= 0.0:
            raise ValueError("Hawkes comparison uncertainty inputs are invalid")
        if not 0.0 <= self.achieved_power <= 1.0:
            raise ValueError("Hawkes comparison power must be in [0, 1]")
        if isinstance(self.sample_count, bool) or not isinstance(
            self.sample_count, int
        ):
            raise TypeError("Hawkes comparison sample count must be integer")
        if self.sample_count < 1:
            raise ValueError("Hawkes comparison sample count must be positive")
        if type(self.power_sufficient) is not bool:
            raise TypeError("Hawkes comparison power status must be boolean")
        object.__setattr__(
            self, "conclusion", HawkesComparisonConclusion(self.conclusion)
        )
        expected = _stable_id("hawkes-metric-comparison", self.payload())
        if self.result_id and self.result_id != expected:
            raise ValueError("Hawkes metric comparison identity differs")
        object.__setattr__(self, "result_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "metric_id": self.metric_id,
            "direction": self.direction.value,
            "diagonal_mean": self.diagonal_mean,
            "full_mean": self.full_mean,
            "oriented_relative_effect": self.oriented_relative_effect,
            "oriented_effect_interpretation": "positive_means_full_is_worse",
            "confidence_low": self.confidence_low,
            "confidence_high": self.confidence_high,
            "standard_error": self.standard_error,
            "sample_count": self.sample_count,
            "materiality_margin": self.materiality_margin,
            "achieved_power": self.achieved_power,
            "power_sufficient": self.power_sufficient,
            "conclusion": self.conclusion.value,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "result_id": self.result_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> HawkesMetricComparisonV1:
        if (
            data.get("oriented_effect_interpretation")
            != "positive_means_full_is_worse"
        ):
            raise ValueError("Hawkes comparison effect interpretation differs")
        return cls(
            metric_id=str(data.get("metric_id", "")),
            direction=HawkesMetricDirection(str(data.get("direction", ""))),
            diagonal_mean=_finite_float(
                data.get("diagonal_mean"), "diagonal_mean"
            ),
            full_mean=_finite_float(data.get("full_mean"), "full_mean"),
            oriented_relative_effect=_finite_float(
                data.get("oriented_relative_effect"), "oriented_relative_effect"
            ),
            confidence_low=_finite_float(
                data.get("confidence_low"), "confidence_low"
            ),
            confidence_high=_finite_float(
                data.get("confidence_high"), "confidence_high"
            ),
            standard_error=_finite_float(
                data.get("standard_error"), "standard_error"
            ),
            sample_count=_strict_int(data.get("sample_count"), "sample_count"),
            materiality_margin=_finite_float(
                data.get("materiality_margin"), "materiality_margin"
            ),
            achieved_power=_finite_float(
                data.get("achieved_power"), "achieved_power"
            ),
            power_sufficient=_strict_bool(
                data.get("power_sufficient"), "power_sufficient"
            ),
            conclusion=HawkesComparisonConclusion(
                str(data.get("conclusion", ""))
            ),
            result_id=str(data.get("result_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class HawkesProductSelectionDossierV1:
    """Retained deterministic product choice and exclusion rationale."""

    policy: HawkesProductSelectionPolicyV1
    comparison_id: str | None
    qualification_dossier_id: str
    qualification_decision_ids: Mapping[str, str]
    qualification_residual_report_ids: Mapping[str, tuple[str, ...]]
    metric_comparisons: tuple[HawkesMetricComparisonV1, ...]
    final_product_residual_reports: tuple[
        HawkesFinalProductResidualReportV1, ...
    ]
    selected_engine_id: str
    excluded_engine_id: str
    selection_reason_codes: tuple[str, ...]
    exclusion_reason_codes: tuple[str, ...]
    input_artifacts: Mapping[str, ArtifactRef]
    implementation_sha256: str
    dossier_id: str = ""
    schema_version: str = HAWKES_PRODUCT_SELECTION_DOSSIER_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            HAWKES_PRODUCT_SELECTION_DOSSIER_SCHEMA_VERSION,
        )
        if not isinstance(self.policy, HawkesProductSelectionPolicyV1):
            raise TypeError("Hawkes selection dossier policy must use v1")
        gate_selection = self.comparison_id is None
        if not gate_selection:
            object.__setattr__(
                self, "comparison_id", _required_text(self.comparison_id)
            )
        object.__setattr__(
            self,
            "qualification_dossier_id",
            _required_text(self.qualification_dossier_id),
        )
        if {self.selected_engine_id, self.excluded_engine_id} != set(
            HAWKES_SELECTION_ENGINE_IDS
        ):
            raise ValueError("Hawkes dossier selection/exclusion differs")
        if self.selected_engine_id == self.excluded_engine_id:
            raise ValueError(
                "Hawkes dossier cannot select and exclude one engine"
            )
        decisions = {
            _required_text(key): _required_text(value)
            for key, value in self.qualification_decision_ids.items()
        }
        if set(decisions) != set(HAWKES_SELECTION_ENGINE_IDS):
            raise ValueError("Hawkes dossier qualification decisions differ")
        residuals = {
            _required_text(key): _text_tuple(value)
            for key, value in self.qualification_residual_report_ids.items()
        }
        if set(residuals) != set(HAWKES_SELECTION_ENGINE_IDS) or any(
            not any(
                report_id.startswith("hawkes-residual-report:")
                for report_id in report_ids
            )
            or not any(
                report_id.startswith("point-process-residual-report:")
                for report_id in report_ids
            )
            for report_ids in residuals.values()
        ):
            raise ValueError(
                "Hawkes dossier must bind raw and benchmark residual reports"
            )
        comparisons = tuple(
            sorted(self.metric_comparisons, key=lambda item: item.metric_id)
        )
        final_reports = tuple(
            sorted(
                self.final_product_residual_reports,
                key=lambda item: item.engine_id,
            )
        )
        selection_reasons = _text_tuple(self.selection_reason_codes)
        exclusion_reasons = _text_tuple(self.exclusion_reason_codes)
        if gate_selection:
            if comparisons or final_reports:
                raise ValueError(
                    "hard-gate Hawkes selection cannot contain pairwise results"
                )
            if (
                "only_reconstruction_eligible_candidate"
                not in selection_reasons
                or "reconstruction_ineligible_candidate"
                not in exclusion_reasons
            ):
                raise ValueError("hard-gate Hawkes selection reasons differ")
        else:
            if {item.engine_id for item in final_reports} != set(
                HAWKES_SELECTION_ENGINE_IDS
            ) or any(
                item.comparison_id != self.comparison_id
                or item.policy_id != self.policy.policy_id
                or item.qualification_dossier_id
                != self.qualification_dossier_id
                or item.status != "available"
                for item in final_reports
            ):
                raise ValueError(
                    "Hawkes dossier final-product residual reports differ"
                )
            if {item.metric_id for item in comparisons} != set(
                METRIC_DIRECTIONS
            ):
                raise ValueError(
                    "Hawkes dossier metric comparison coverage differs"
                )
            if any(
                not item.power_sufficient
                or item.conclusion is HawkesComparisonConclusion.INCONCLUSIVE
                for item in comparisons
            ):
                raise ValueError(
                    "Hawkes dossier contains unresolved comparisons"
                )
            if "only_reconstruction_eligible_candidate" in selection_reasons:
                raise ValueError("paired Hawkes selection reasons differ")
        artifacts = {
            _required_text(key): value
            for key, value in sorted(self.input_artifacts.items())
        }
        expected_artifacts = {"policy", "qualification"}
        if not gate_selection:
            expected_artifacts.add("validation_comparison")
        if set(artifacts) != expected_artifacts:
            raise ValueError(
                "Hawkes selection dossier input artifact set differs"
            )
        if any(
            not isinstance(value, ArtifactRef) for value in artifacts.values()
        ):
            raise TypeError("Hawkes selection dossier artifacts are invalid")
        object.__setattr__(
            self, "qualification_decision_ids", dict(sorted(decisions.items()))
        )
        object.__setattr__(
            self,
            "qualification_residual_report_ids",
            dict(sorted(residuals.items())),
        )
        object.__setattr__(self, "metric_comparisons", comparisons)
        object.__setattr__(
            self, "final_product_residual_reports", final_reports
        )
        object.__setattr__(self, "selection_reason_codes", selection_reasons)
        object.__setattr__(self, "exclusion_reason_codes", exclusion_reasons)
        object.__setattr__(self, "input_artifacts", artifacts)
        object.__setattr__(
            self,
            "implementation_sha256",
            _sha256(self.implementation_sha256, "implementation_sha256"),
        )
        expected = _stable_id(
            "hawkes-product-selection-dossier", self.payload()
        )
        if self.dossier_id and self.dossier_id != expected:
            raise ValueError(
                "Hawkes product selection dossier identity differs"
            )
        object.__setattr__(self, "dossier_id", expected)
        _bounded_json(self.to_json(), "Hawkes product selection dossier")

    def payload(self) -> dict[str, JSONValue]:
        gate_selection = self.comparison_id is None
        return {
            "schema_version": self.schema_version,
            "policy": self.policy.to_dict(),
            "comparison_id": self.comparison_id,
            "selection_mode": (
                "qualification_hard_gate"
                if gate_selection
                else "paired_validation"
            ),
            "qualification_dossier_id": self.qualification_dossier_id,
            "qualification_decision_ids": dict(self.qualification_decision_ids),
            "qualification_residual_report_ids": {
                key: list(value)
                for key, value in self.qualification_residual_report_ids.items()
            },
            "metric_comparisons": [
                item.to_dict() for item in self.metric_comparisons
            ],
            "final_product_residual_reports": [
                item.to_dict() for item in self.final_product_residual_reports
            ],
            "selected_engine_id": self.selected_engine_id,
            "excluded_engine_id": self.excluded_engine_id,
            "selection_reason_codes": list(self.selection_reason_codes),
            "exclusion_reason_codes": list(self.exclusion_reason_codes),
            "input_artifacts": {
                key: value.to_dict()
                for key, value in self.input_artifacts.items()
            },
            "implementation_sha256": self.implementation_sha256,
            "validation_only": True,
            "final_holdout_used_for_selection": False,
            "paired_validation_comparison_reached": not gate_selection,
            "eligible_but_unselected_engine_excluded": not gate_selection,
            "reconstruction_ineligible_engine_excluded": gate_selection,
            "manual_preference_used": False,
            "repository_order_used": False,
            "historical_truth_claim": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "dossier_id": self.dossier_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> HawkesProductSelectionDossierV1:
        for name, expected in (
            ("validation_only", True),
            ("final_holdout_used_for_selection", False),
            ("manual_preference_used", False),
            ("repository_order_used", False),
            ("historical_truth_claim", False),
        ):
            if data.get(name) != expected:
                raise ValueError(f"Hawkes selection dossier {name} differs")
        selection_mode = str(data.get("selection_mode", ""))
        gate_selection = selection_mode == "qualification_hard_gate"
        if selection_mode not in {
            "qualification_hard_gate",
            "paired_validation",
        }:
            raise ValueError("Hawkes selection dossier mode differs")
        for name, expected in (
            ("paired_validation_comparison_reached", not gate_selection),
            ("eligible_but_unselected_engine_excluded", not gate_selection),
            ("reconstruction_ineligible_engine_excluded", gate_selection),
        ):
            if data.get(name) != expected:
                raise ValueError(f"Hawkes selection dossier {name} differs")
        comparison_value = data.get("comparison_id")
        if gate_selection != (comparison_value is None):
            raise ValueError("Hawkes selection dossier comparison mode differs")
        return cls(
            policy=HawkesProductSelectionPolicyV1.from_dict(
                _mapping(data.get("policy"))
            ),
            comparison_id=(
                None if comparison_value is None else str(comparison_value)
            ),
            qualification_dossier_id=str(
                data.get("qualification_dossier_id", "")
            ),
            qualification_decision_ids={
                str(key): str(value)
                for key, value in _mapping(
                    data.get("qualification_decision_ids")
                ).items()
            },
            qualification_residual_report_ids={
                str(key): _string_tuple(value)
                for key, value in _mapping(
                    data.get("qualification_residual_report_ids")
                ).items()
            },
            metric_comparisons=tuple(
                HawkesMetricComparisonV1.from_dict(_mapping(item))
                for item in _sequence(data.get("metric_comparisons"))
            ),
            final_product_residual_reports=tuple(
                HawkesFinalProductResidualReportV1.from_dict(_mapping(item))
                for item in _sequence(
                    data.get("final_product_residual_reports")
                )
            ),
            selected_engine_id=str(data.get("selected_engine_id", "")),
            excluded_engine_id=str(data.get("excluded_engine_id", "")),
            selection_reason_codes=_string_tuple(
                data.get("selection_reason_codes")
            ),
            exclusion_reason_codes=_string_tuple(
                data.get("exclusion_reason_codes")
            ),
            input_artifacts={
                str(key): ArtifactRef.from_dict(_mapping(value))
                for key, value in _mapping(data.get("input_artifacts")).items()
            },
            implementation_sha256=str(data.get("implementation_sha256", "")),
            dossier_id=str(data.get("dossier_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> HawkesProductSelectionDossierV1:
        _bounded_json(text, "Hawkes product selection dossier")
        return cls.from_dict(_json_mapping(text))


def derive_hawkes_product_selection_dossier(
    policy: HawkesProductSelectionPolicyV1,
    comparison: HawkesValidationComparisonV1 | None,
    qualification: PoweredQualificationDossierV1,
    *,
    input_artifacts: Mapping[str, ArtifactRef],
) -> HawkesProductSelectionDossierV1:
    """Replay the product choice without holdout, issue-order, or manual input."""
    decisions = {
        engine_id: qualification.decision(engine_id)
        for engine_id in HAWKES_SELECTION_ENGINE_IDS
    }
    eligible = tuple(
        engine_id
        for engine_id in HAWKES_SELECTION_ENGINE_IDS
        if decisions[engine_id].reconstruction_eligible
    )
    if not eligible:
        raise ValueError("no Hawkes candidate is reconstruction eligible")
    if len(eligible) == 1:
        if comparison is not None:
            raise ValueError(
                "Hawkes pairwise comparison must be omitted after hard-gate "
                "exclusion"
            )
        selected = eligible[0]
        excluded = (
            FULL_HAWKES_ENGINE_ID
            if selected == DIAGONAL_HAWKES_ENGINE_ID
            else DIAGONAL_HAWKES_ENGINE_ID
        )
        failed_gates = tuple(
            sorted(
                gate_id
                for gate_id, status in decisions[excluded].gate_statuses.items()
                if getattr(status, "value", status) == "failed"
            )
        )
        exclusion_reasons = (
            "reconstruction_ineligible_candidate",
            "one_or_more_powered_gates_failed",
            *(f"hard_gate_failed:{gate_id}" for gate_id in failed_gates),
        )
        return HawkesProductSelectionDossierV1(
            policy=policy,
            comparison_id=None,
            qualification_dossier_id=qualification.dossier_id,
            qualification_decision_ids={
                key: value.decision_id for key, value in decisions.items()
            },
            qualification_residual_report_ids={
                key: tuple(value.residual_report_ids)
                for key, value in decisions.items()
            },
            metric_comparisons=(),
            final_product_residual_reports=(),
            selected_engine_id=selected,
            excluded_engine_id=excluded,
            selection_reason_codes=(
                "only_reconstruction_eligible_candidate",
                "predeclared_hard_gate_precedes_pairwise_comparison",
                "pairwise_validation_comparison_not_reached",
                "final_holdout_not_used",
            ),
            exclusion_reason_codes=exclusion_reasons,
            input_artifacts=input_artifacts,
            implementation_sha256=_implementation_sha256(),
        )
    if comparison is None:
        raise ValueError(
            "both eligible Hawkes candidates require a paired comparison"
        )
    if comparison.policy_id != policy.policy_id:
        raise ValueError("Hawkes comparison policy is stale")
    if comparison.qualification_dossier_id != qualification.dossier_id:
        raise ValueError("Hawkes comparison qualification is stale")
    if comparison.coordinate_count < policy.minimum_paired_cells:
        raise ValueError("Hawkes comparison is below paired-cell minimum")
    for ref in comparison.evidence_artifacts.values():
        verify_artifact_ref(ref)
    final_product_residual_reports = tuple(
        _final_product_residual_report(policy, comparison, engine_id)
        for engine_id in HAWKES_SELECTION_ENGINE_IDS
    )
    if any(
        item.metrics["maximum_spectral_radius"]
        >= policy.maximum_spectral_radius
        for item in comparison.observations
    ):
        raise ValueError(
            "Hawkes validation evidence violates spectral-radius gate"
        )

    metric_comparisons = tuple(
        _compare_metric(policy, comparison.observations, metric_id)
        for metric_id in METRIC_DIRECTIONS
    )
    unresolved = tuple(
        item.metric_id
        for item in metric_comparisons
        if not item.power_sufficient
        or item.conclusion is HawkesComparisonConclusion.INCONCLUSIVE
    )
    if unresolved:
        raise ValueError(
            "Hawkes selection comparisons are unresolved: "
            + ", ".join(unresolved)
        )
    selected, selection_reasons = _select_engine(metric_comparisons)
    excluded = (
        FULL_HAWKES_ENGINE_ID
        if selected == DIAGONAL_HAWKES_ENGINE_ID
        else DIAGONAL_HAWKES_ENGINE_ID
    )
    exclusion_reasons = (
        "reconstruction_eligible_but_not_product_selected",
        "validation_only_predeclared_rule_selected_other_candidate",
        *selection_reasons,
    )
    return HawkesProductSelectionDossierV1(
        policy=policy,
        comparison_id=comparison.comparison_id,
        qualification_dossier_id=qualification.dossier_id,
        qualification_decision_ids={
            key: value.decision_id for key, value in decisions.items()
        },
        qualification_residual_report_ids={
            key: tuple(value.residual_report_ids)
            for key, value in decisions.items()
        },
        metric_comparisons=metric_comparisons,
        final_product_residual_reports=final_product_residual_reports,
        selected_engine_id=selected,
        excluded_engine_id=excluded,
        selection_reason_codes=selection_reasons,
        exclusion_reason_codes=exclusion_reasons,
        input_artifacts=input_artifacts,
        implementation_sha256=_implementation_sha256(),
    )


def _final_product_residual_report(
    policy: HawkesProductSelectionPolicyV1,
    comparison: HawkesValidationComparisonV1,
    engine_id: str,
) -> HawkesFinalProductResidualReportV1:
    observations = tuple(
        item for item in comparison.observations if item.engine_id == engine_id
    )
    status = (
        "available"
        if len(observations) >= policy.minimum_paired_cells
        else "insufficient_evidence"
    )
    reason = (
        "constrained_product_simulation_predictive_metrics_available"
        if status == "available"
        else "constrained_product_coordinate_support_below_policy_minimum"
    )
    return HawkesFinalProductResidualReportV1(
        engine_id=engine_id,
        policy_id=policy.policy_id,
        comparison_id=comparison.comparison_id,
        qualification_dossier_id=comparison.qualification_dossier_id,
        coordinate_ids=tuple(
            item.coordinate.coordinate_id for item in observations
        ),
        final_constraint_set_ids=tuple(
            item.coordinate.final_constraint_set_id for item in observations
        ),
        metric_summaries={
            f"{metric}_{suffix}": (
                statistics.fmean(item.metrics[metric] for item in observations)
                if suffix == "mean"
                else _quantile(
                    tuple(item.metrics[metric] for item in observations),
                    0.95,
                )
            )
            for metric in FINAL_PRODUCT_DIAGNOSTIC_METRICS
            for suffix in ("mean", "q95")
        },
        observation_count=len(observations),
        status=status,
        reason_codes=(reason,),
    )


def build_hawkes_product_selection_dossier(
    policy_path: str | Path,
    comparison_path: str | Path | None,
    qualification_path: str | Path,
    *,
    output_directory: str | Path,
) -> HawkesProductSelectionDossierV1:
    """Read exact retained inputs, derive, verify, and publish one dossier."""
    policy = read_hawkes_product_selection_policy(policy_path)
    comparison = (
        None
        if comparison_path is None
        else read_hawkes_validation_comparison(comparison_path)
    )
    qualification = read_powered_qualification_dossier(qualification_path)
    verify_powered_qualification_dossier(qualification)
    refs = {
        "policy": artifact_ref_for_file(
            policy_path, kind="hawkes_product_selection_policy_v1"
        ),
        "qualification": artifact_ref_for_file(
            qualification_path, kind="powered_qualification_dossier_v1"
        ),
    }
    if comparison_path is not None:
        refs["validation_comparison"] = artifact_ref_for_file(
            comparison_path, kind="hawkes_validation_comparison_v1"
        )
    dossier = derive_hawkes_product_selection_dossier(
        policy, comparison, qualification, input_artifacts=refs
    )
    verify_hawkes_product_selection_dossier(dossier)
    write_hawkes_product_selection_dossier(dossier, output_directory)
    return dossier


def verify_hawkes_product_selection_dossier(
    dossier: HawkesProductSelectionDossierV1,
) -> None:
    """Fail closed on stale code, inputs, qualification, or derived choice."""
    if not isinstance(dossier, HawkesProductSelectionDossierV1):
        raise TypeError("Hawkes product selection dossier must use v1")
    if dossier.implementation_sha256 != _implementation_sha256():
        raise ValueError("Hawkes selection implementation identity is stale")
    for ref in dossier.input_artifacts.values():
        verify_artifact_ref(ref)
    policy = read_hawkes_product_selection_policy(
        dossier.input_artifacts["policy"].path
    )
    comparison_ref = dossier.input_artifacts.get("validation_comparison")
    comparison = (
        None
        if comparison_ref is None
        else read_hawkes_validation_comparison(comparison_ref.path)
    )
    qualification = read_powered_qualification_dossier(
        dossier.input_artifacts["qualification"].path
    )
    verify_powered_qualification_dossier(qualification)
    expected = derive_hawkes_product_selection_dossier(
        policy,
        comparison,
        qualification,
        input_artifacts=dossier.input_artifacts,
    )
    if dossier != expected:
        raise ValueError("Hawkes product selection differs from replay")


def write_hawkes_product_selection_policy(
    policy: HawkesProductSelectionPolicyV1, output_directory: str | Path
) -> ArtifactRef:
    ref = _write_contract(
        policy.to_json(),
        output_directory,
        prefix="hawkes-product-selection-policy",
        kind="hawkes_product_selection_policy_v1",
        metadata={"policy_id": policy.policy_id},
    )
    if read_hawkes_product_selection_policy(ref.path) != policy:
        raise ValueError(
            "published Hawkes selection policy differs on readback"
        )
    return ref


def read_hawkes_product_selection_policy(
    path: str | Path,
) -> HawkesProductSelectionPolicyV1:
    return HawkesProductSelectionPolicyV1.from_dict(
        _read_content_addressed_json(path, "hawkes-product-selection-policy")
    )


def write_hawkes_validation_comparison(
    comparison: HawkesValidationComparisonV1, output_directory: str | Path
) -> ArtifactRef:
    ref = _write_contract(
        comparison.to_json(),
        output_directory,
        prefix="hawkes-validation-comparison",
        kind="hawkes_validation_comparison_v1",
        metadata={
            "comparison_id": comparison.comparison_id,
            "policy_id": comparison.policy_id,
        },
    )
    if read_hawkes_validation_comparison(ref.path) != comparison:
        raise ValueError("published Hawkes comparison differs on readback")
    return ref


def read_hawkes_validation_comparison(
    path: str | Path,
) -> HawkesValidationComparisonV1:
    return HawkesValidationComparisonV1.from_dict(
        _read_content_addressed_json(path, "hawkes-validation-comparison")
    )


def write_hawkes_product_selection_dossier(
    dossier: HawkesProductSelectionDossierV1, output_directory: str | Path
) -> ArtifactRef:
    ref = _write_contract(
        dossier.to_json(),
        output_directory,
        prefix="hawkes-product-selection-dossier",
        kind=HAWKES_PRODUCT_SELECTION_DOSSIER_ARTIFACT_KIND,
        metadata={
            "dossier_id": dossier.dossier_id,
            "selected_engine_id": dossier.selected_engine_id,
        },
    )
    if read_hawkes_product_selection_dossier(ref.path) != dossier:
        raise ValueError("published Hawkes selection differs on readback")
    return ref


def read_hawkes_product_selection_dossier(
    path: str | Path,
) -> HawkesProductSelectionDossierV1:
    return HawkesProductSelectionDossierV1.from_dict(
        _read_content_addressed_json(path, "hawkes-product-selection-dossier")
    )


def _compare_metric(
    policy: HawkesProductSelectionPolicyV1,
    observations: Sequence[HawkesValidationObservationV1],
    metric_id: str,
) -> HawkesMetricComparisonV1:
    paired: dict[str, dict[str, float]] = {}
    raw: dict[str, list[float]] = {
        DIAGONAL_HAWKES_ENGINE_ID: [],
        FULL_HAWKES_ENGINE_ID: [],
    }
    for item in observations:
        value = item.metrics[metric_id]
        paired.setdefault(item.coordinate.coordinate_id, {})[
            item.engine_id
        ] = value
        raw[item.engine_id].append(value)
    relative_effects: list[float] = []
    direction = METRIC_DIRECTIONS[metric_id]
    for coordinate_id in sorted(paired):
        values = paired[coordinate_id]
        diagonal = values[DIAGONAL_HAWKES_ENGINE_ID]
        full = values[FULL_HAWKES_ENGINE_ID]
        scale = max(abs(diagonal), abs(full), policy.relative_scale_floor)
        effect = (full - diagonal) / scale
        if direction is HawkesMetricDirection.HIGHER:
            effect = -effect
        relative_effects.append(effect)
    count = len(relative_effects)
    mean_effect = statistics.fmean(relative_effects)
    standard_error = (
        statistics.stdev(relative_effects) / math.sqrt(count)
        if count > 1
        else 0.0
    )
    critical = _student_t_two_sided_critical(policy.alpha, count - 1)
    confidence_low = mean_effect - critical * standard_error
    confidence_high = mean_effect + critical * standard_error
    margin = policy.materiality_margins[metric_id]
    power = _power_at_margin(relative_effects, margin, policy.alpha)
    power_sufficient = bool(
        count >= policy.minimum_paired_cells and power >= policy.minimum_power
    )
    if not power_sufficient:
        conclusion = HawkesComparisonConclusion.INCONCLUSIVE
    elif confidence_low > margin:
        conclusion = HawkesComparisonConclusion.DIAGONAL
    elif confidence_high < -margin:
        conclusion = HawkesComparisonConclusion.FULL
    elif confidence_low >= -margin and confidence_high <= margin:
        conclusion = HawkesComparisonConclusion.EQUIVALENT
    else:
        conclusion = HawkesComparisonConclusion.INCONCLUSIVE
    return HawkesMetricComparisonV1(
        metric_id=metric_id,
        direction=direction,
        diagonal_mean=statistics.fmean(raw[DIAGONAL_HAWKES_ENGINE_ID]),
        full_mean=statistics.fmean(raw[FULL_HAWKES_ENGINE_ID]),
        oriented_relative_effect=mean_effect,
        confidence_low=confidence_low,
        confidence_high=confidence_high,
        standard_error=standard_error,
        sample_count=count,
        materiality_margin=margin,
        achieved_power=power,
        power_sufficient=power_sufficient,
        conclusion=conclusion,
    )


def _select_engine(
    comparisons: Sequence[HawkesMetricComparisonV1],
) -> tuple[str, tuple[str, ...]]:
    by_id = {item.metric_id: item for item in comparisons}
    primary = _favored_candidates(by_id[name] for name in PRIMARY_METRIC_IDS)
    if len(primary) > 1:
        raise ValueError(
            "Hawkes primary decision surface contains a Pareto tradeoff"
        )
    scientific = _favored_candidates(
        by_id[name] for name in SCIENTIFIC_METRIC_IDS
    )
    if len(scientific) > 1:
        raise ValueError(
            "Hawkes scientific decision surface contains a Pareto tradeoff"
        )
    if primary and scientific and primary != scientific:
        raise ValueError("Hawkes primary and scientific comparisons conflict")
    favored = primary or scientific
    if favored:
        selected = next(iter(favored))
        return selected, (
            "all_hard_gates_passed",
            "powered_validation_scientific_pareto_rule",
            "final_holdout_not_used",
        )
    resources = _favored_candidates(by_id[name] for name in RESOURCE_METRIC_IDS)
    if len(resources) > 1:
        raise ValueError(
            "Hawkes resource decision surface contains a Pareto tradeoff"
        )
    if resources:
        return next(iter(resources)), (
            "all_hard_gates_passed",
            "scientific_metrics_practically_equivalent",
            "powered_validation_resource_rule",
            "final_holdout_not_used",
        )
    return DIAGONAL_HAWKES_ENGINE_ID, (
        "all_hard_gates_passed",
        "scientific_metrics_practically_equivalent",
        "resource_metrics_practically_equivalent",
        "lower_predeclared_model_complexity",
        "final_holdout_not_used",
    )


def _favored_candidates(
    comparisons: Sequence[HawkesMetricComparisonV1] | Any,
) -> set[str]:
    favored: set[str] = set()
    for item in comparisons:
        if item.conclusion is HawkesComparisonConclusion.DIAGONAL:
            favored.add(DIAGONAL_HAWKES_ENGINE_ID)
        elif item.conclusion is HawkesComparisonConclusion.FULL:
            favored.add(FULL_HAWKES_ENGINE_ID)
    return favored


def _power_at_margin(
    values: Sequence[float], margin: float, alpha: float
) -> float:
    if len(values) < 2:
        return 0.0
    standard_deviation = statistics.stdev(values)
    if standard_deviation <= 1e-15:
        return 1.0
    z_critical = statistics.NormalDist().inv_cdf(1.0 - alpha / 2.0)
    noncentrality = margin * math.sqrt(len(values)) / standard_deviation
    normal = statistics.NormalDist()
    return min(
        1.0,
        normal.cdf(-z_critical - noncentrality)
        + 1.0
        - normal.cdf(z_critical - noncentrality),
    )


def _student_t_two_sided_critical(alpha: float, degrees_freedom: int) -> float:
    if degrees_freedom <= 0:
        return statistics.NormalDist().inv_cdf(1.0 - alpha / 2.0)
    z_value = statistics.NormalDist().inv_cdf(1.0 - alpha / 2.0)
    inverse = 1.0 / degrees_freedom
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


def _require_constant_policy_fields(data: Mapping[str, Any]) -> None:
    expected: Mapping[str, Any] = {
        "candidate_engine_ids": list(HAWKES_SELECTION_ENGINE_IDS),
        "validation_split_only": True,
        "final_holdout_permitted": False,
        "metric_directions": {
            key: value.value for key, value in METRIC_DIRECTIONS.items()
        },
        "primary_metric_ids": list(PRIMARY_METRIC_IDS),
        "resource_metric_ids": list(RESOURCE_METRIC_IDS),
        "paired_axes": [
            "window_id",
            "degradation_scenario_id",
            "seed",
            "anchor_set_id",
            "adaptive_partition_id",
            "final_constraint_set_id",
            "era",
        ],
        "uncertainty_method": "paired-student-t-relative-effect.v1",
        "power_method": "two-sided-normal-approximation-at-materiality.v1",
        "selection_rule": "hard-gates-pareto-equivalence-resource-complexity.v1",
        "complexity_order": list(HAWKES_SELECTION_ENGINE_IDS),
        "automatic_winner_from_repository_order": False,
    }
    if any(data.get(key) != value for key, value in expected.items()):
        raise ValueError(
            "Hawkes selection policy frozen decision surface differs"
        )
    projection = _mapping(data.get("projection_metric"))
    expected_projection = {
        "metric_id": "dimensionless-projection-burden.v1",
        "numerator": "sum_event_l1_reconciled_minus_proposal",
        "denominator": "sum_event_max_proposal_spread_epsilon",
        "event_set": "all_proposal_quote_vectors_before_projection",
        "clipping_permitted": False,
        "zero_spread_treatment": "replace_only_denominator_term_with_epsilon",
    }
    if any(
        projection.get(key) != value
        for key, value in expected_projection.items()
    ):
        raise ValueError("Hawkes projection-burden policy differs")


def _write_contract(
    text: str,
    output_directory: str | Path,
    *,
    prefix: str,
    kind: str,
    metadata: Mapping[str, JSONValue],
) -> ArtifactRef:
    root = Path(output_directory).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    encoded = text.encode("utf-8") + b"\n"
    if len(encoded) > MAX_SELECTION_ARTIFACT_BYTES:
        raise ValueError("Hawkes selection artifact exceeds byte bound")
    digest = hashlib.sha256(encoded).hexdigest()
    target = root / f"{prefix}-{digest}.json"
    _write_once(target, encoded)
    return ArtifactRef(
        kind=kind,
        path=str(target),
        size_bytes=len(encoded),
        sha256=digest,
        metadata=dict(metadata),
    )


def _read_content_addressed_json(
    path: str | Path, prefix: str
) -> Mapping[str, Any]:
    source = Path(path).expanduser().resolve()
    if source.stat().st_size > MAX_SELECTION_ARTIFACT_BYTES:
        raise ValueError("Hawkes selection artifact exceeds byte bound")
    encoded = source.read_bytes()
    digest = hashlib.sha256(encoded).hexdigest()
    if source.name != f"{prefix}-{digest}.json":
        raise ValueError("Hawkes selection artifact content hash differs")
    return _mapping(json.loads(encoded))


def _write_once(path: Path, payload: bytes) -> None:
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    except FileExistsError:
        if path.read_bytes() != payload:
            raise ValueError(
                "content-addressed Hawkes artifact already differs"
            )
        return
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())


def _implementation_sha256() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        str(canonical_contract_json(payload)).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _bounded_json(text: str, name: str) -> None:
    if len(text.encode("utf-8")) > MAX_SELECTION_ARTIFACT_BYTES:
        raise ValueError(f"{name} exceeds artifact byte bound")


def _json_mapping(text: str) -> Mapping[str, Any]:
    _bounded_json(text, "Hawkes selection JSON")
    return _mapping(json.loads(text))


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("expected a JSON object")
    return value


def _sequence(value: Any) -> Sequence[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise TypeError("expected a JSON array")
    return value


def _string_tuple(value: Any) -> tuple[str, ...]:
    return tuple(str(item) for item in _sequence(value))


def _text_tuple(value: Sequence[str]) -> tuple[str, ...]:
    selected = tuple(_required_text(item) for item in value)
    if not selected or len(set(selected)) != len(selected):
        raise ValueError(
            "Hawkes selection reason codes are empty or duplicated"
        )
    return selected


def _quantile(values: Sequence[float], level: float) -> float:
    selected = tuple(
        sorted(_finite_float(item, "quantile value") for item in values)
    )
    if not selected:
        raise ValueError("quantile values are empty")
    selected_level = _finite_float(level, "quantile level")
    if not 0.0 <= selected_level <= 1.0:
        raise ValueError("quantile level is outside [0, 1]")
    position = (len(selected) - 1) * selected_level
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return selected[lower]
    weight = position - lower
    return selected[lower] * (1.0 - weight) + selected[upper] * weight


def _required_text(value: Any) -> str:
    selected = str(value).strip()
    if not selected:
        raise ValueError("required text is empty")
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


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    return value


def _strict_bool(value: Any, name: str) -> bool:
    if type(value) is not bool:
        raise TypeError(f"{name} must be boolean")
    return value


def _sha256(value: Any, name: str) -> str:
    selected = _required_text(value)
    if len(selected) != 64 or any(
        character not in "0123456789abcdef" for character in selected
    ):
        raise ValueError(f"{name} must be lowercase SHA-256")
    return selected


def _require_schema(value: str, expected: str) -> None:
    if value != expected:
        raise ValueError(f"unsupported schema version: {value}")


__all__ = [
    "DIAGONAL_HAWKES_ENGINE_ID",
    "FULL_HAWKES_ENGINE_ID",
    "HAWKES_FINAL_PRODUCT_RESIDUAL_REPORT_SCHEMA_VERSION",
    "HAWKES_PRODUCT_SELECTION_DOSSIER_ARTIFACT_KIND",
    "HAWKES_SELECTION_ENGINE_IDS",
    "HawkesComparisonConclusion",
    "HawkesFinalProductResidualReportV1",
    "HawkesMetricComparisonV1",
    "HawkesMetricDirection",
    "HawkesProductSelectionDossierV1",
    "HawkesProductSelectionPolicyV1",
    "HawkesValidationComparisonV1",
    "HawkesValidationCoordinateV1",
    "HawkesValidationEra",
    "HawkesValidationObservationV1",
    "build_hawkes_product_selection_dossier",
    "derive_hawkes_product_selection_dossier",
    "read_hawkes_product_selection_dossier",
    "read_hawkes_product_selection_policy",
    "read_hawkes_validation_comparison",
    "verify_hawkes_product_selection_dossier",
    "write_hawkes_product_selection_dossier",
    "write_hawkes_product_selection_policy",
    "write_hawkes_validation_comparison",
]
