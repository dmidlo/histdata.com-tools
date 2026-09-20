"""Immutable public explanation contracts, not private model qualification."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import re
from typing import ClassVar

from ._wire import Artifact, Record, digest, ordered, text

CAUSAL_NONCLAIM = "model attribution != causal effect in the historical market"
INDEPENDENCE_NONCLAIM = (
    "concentration is not causal independence or historical sample size"
)
PLANES = (
    "broker_style",
    "calendar",
    "forecast",
    "market.activity",
    "market.bar",
    "market.indicator",
    "market.tick",
    "missingness",
    "positioning",
    "regime",
    "strategy",
    "synthetic_flow",
    "triangle",
    "uncertainty",
)
TIMEFRAMES = ("1m", "5m", "15m", "30m", "1h", "4h", "1d")


def _semver(value: str) -> None:
    if (
        re.fullmatch(
            r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)",
            value,
        )
        is None
    ):
        raise ValueError("version must be canonical release SemVer")


class AttributionEvidenceKind(str, Enum):
    FIXTURE = "executed_synthetic_reference_not_scientific_qualification"
    DECLARED = "declared_external_unverified"


class AttributionState(str, Enum):
    IDENTIFIED = "reference_accounting_identified"
    NONIDENTIFIABLE = "attribution_nonidentifiable"
    UNSUPPORTED = "unsupported_or_out_of_domain"
    DECLARED = "declared_unverified"


class FeatureSpace(str, Enum):
    RAW = "raw_logical"
    TRANSFORMED = "normalized_transformed"
    LATENT = "latent_component"
    GROUPED = "grouped_source"


class GroupMethod(str, Enum):
    SUM = "sum_feature_shapley"
    DIRECT = "direct_group_coalition_shapley"


@dataclass(frozen=True, slots=True)
class AttributionReferenceV1(Record):
    schema: str
    native_id: str
    sha256: str
    byte_length: int
    evidence_kind: AttributionEvidenceKind

    def _validate(self) -> None:
        text(self.schema)
        text(self.native_id)
        digest(self.sha256)
        if not 0 < self.byte_length < 2**63:
            raise ValueError("external reference size must be positive int64")


def reference(
    artifact: Artifact,
    *,
    kind: AttributionEvidenceKind = AttributionEvidenceKind.FIXTURE,
) -> AttributionReferenceV1:
    from ._wire import sha256

    encoded = artifact.to_json()
    return AttributionReferenceV1(
        artifact.schema_version(),
        artifact.artifact_id,
        sha256(encoded),
        len(encoded),
        kind,
    )


@dataclass(frozen=True, slots=True)
class AttributionFeatureV1(Record):
    name: str
    plane: str
    semantic_id: str
    unit: str
    timeframe: str | None = None
    strategy_id: str | None = None
    strategy_family: str | None = None
    dependency_class: str | None = None

    def _validate(self) -> None:
        for value in (self.name, self.semantic_id, self.unit):
            text(value)
        if self.plane not in PLANES:
            raise ValueError("unknown feature plane")
        if self.timeframe is not None and self.timeframe not in TIMEFRAMES:
            raise ValueError("unknown timeframe")
        if (
            self.plane in ("market.bar", "market.indicator")
            and self.timeframe is None
        ):
            raise ValueError("multiresolution feature needs timeframe")
        strategy = (
            self.strategy_id,
            self.strategy_family,
            self.dependency_class,
        )
        if self.plane == "strategy":
            if any(value is None for value in strategy):
                raise ValueError("strategy needs ID/family/dependency class")
            assert self.strategy_id is not None
            if (
                re.fullmatch(
                    r"FXES-(?:00[1-9]|0[1-9][0-9]|[1-9][0-9]{2}|1000)",
                    self.strategy_id,
                )
                is None
            ):
                raise ValueError(
                    "strategy ID is outside canonical1..1000 namespace"
                )
            for strategy_value in strategy:
                assert strategy_value is not None
                text(strategy_value)
        elif any(value is not None for value in strategy):
            raise ValueError("non-strategy feature cannot claim FXES ownership")


@dataclass(frozen=True, slots=True)
class AttributionFeatureShardV1(Artifact):
    KIND: ClassVar[str] = "feature-shard"
    catalog: AttributionReferenceV1
    features: tuple[AttributionFeatureV1, ...]

    def _validate(self) -> None:
        if not 1 <= len(self.features) <= 256:
            raise ValueError("feature shard requires1..256 entries")
        ordered(tuple(f.name for f in self.features), nonempty=True)


@dataclass(frozen=True, slots=True)
class AttributionTaxonomyV1(Artifact):
    """Logical inventories may be wide; exact bounded shards remain separate."""

    KIND: ClassVar[str] = "taxonomy"
    version: str
    catalog: AttributionReferenceV1
    shards: tuple[AttributionReferenceV1, ...]
    feature_count: int

    def _validate(self) -> None:
        _semver(self.version)
        if (
            not 1 <= len(self.shards) <= 64
            or not 1 <= self.feature_count <= 16384
        ):
            raise ValueError("taxonomy inventory exceeds64shards/16384features")
        ordered(tuple(s.native_id for s in self.shards), nonempty=True)


@dataclass(frozen=True, slots=True)
class AttributionValueV1(Record):
    feature: AttributionFeatureV1
    value: float | None
    source_id: str
    available_at_ns: int | None
    imputed: bool = False
    mask_name: str | None = None

    def _validate(self) -> None:
        text(self.source_id)
        if self.available_at_ns is not None and self.available_at_ns < 0:
            raise ValueError("negative availability")
        if self.imputed and (self.mask_name is None or self.value is None):
            raise ValueError("imputed value requires explicit retained mask")


@dataclass(frozen=True, slots=True)
class AttributionSnapshotV1(Artifact):
    KIND: ClassVar[str] = "feature-snapshot"
    values: tuple[AttributionValueV1, ...]
    cutoff_at_ns: int
    space: FeatureSpace
    preprocessing: AttributionReferenceV1
    projection: AttributionReferenceV1
    lineage: AttributionReferenceV1
    universe: tuple[str, ...]
    session: str
    domain: str

    def _validate(self) -> None:
        if not 1 <= len(self.values) <= 256 or self.cutoff_at_ns < 0:
            raise ValueError("selected snapshot bounds")
        ordered(tuple(v.feature.name for v in self.values), nonempty=True)
        ordered(self.universe, nonempty=True)
        text(self.session)
        text(self.domain)
        by_name = {v.feature.name: v for v in self.values}
        for value in self.values:
            if value.mask_name is not None:
                mask = by_name.get(value.mask_name)
                if (
                    mask is None
                    or mask.feature.plane != "missingness"
                    or mask.value != float(value.imputed)
                ):
                    raise ValueError("imputation mask absent or inconsistent")


@dataclass(frozen=True, slots=True)
class AttributionBackgroundV1(Artifact):
    KIND: ClassVar[str] = "background"
    snapshots: tuple[AttributionSnapshotV1, ...]
    declared_at_ns: int
    role: str

    def _validate(self) -> None:
        if not 1 <= len(self.snapshots) <= 32 or self.declared_at_ns < 0:
            raise ValueError("background bound")
        if self.role not in (
            "synthetic_fixture",
            "declared_train",
            "protected",
            "unknown",
        ):
            raise ValueError("unknown background role")
        ordered(tuple(s.artifact_id for s in self.snapshots), nonempty=True)


@dataclass(frozen=True, slots=True)
class AttributionGroupV1(Record):
    name: str
    parent: str | None
    members: tuple[str, ...]
    level: str

    def _validate(self) -> None:
        text(self.name)
        ordered(self.members, nonempty=True)
        if self.parent is not None:
            text(self.parent)
        if self.level not in (
            "plane",
            "timeframe",
            "strategy",
            "family",
            "dependency_class",
            "population",
            "correlated_cluster",
        ):
            raise ValueError("unknown group semantic level")


@dataclass(frozen=True, slots=True)
class ExplanationPolicyV1(Artifact):
    KIND: ClassVar[str] = "policy"
    version: str
    method: str
    group_method: GroupMethod
    groups: tuple[AttributionGroupV1, ...]
    reporting_cut: tuple[str, ...]
    absolute_tolerance: float
    relative_tolerance: float
    instability_threshold: float
    maximum_evaluations: int
    correlated_policy: str
    declared_at_ns: int
    causal_nonclaim: str = CAUSAL_NONCLAIM

    def _validate(self) -> None:
        _semver(self.version)
        if self.method not in (
            "exact-polynomial-coalitions.v1",
            "external-declared",
        ):
            raise ValueError("unsupported explainer method")
        if self.correlated_policy != "background-range-and-sibling-ablation.v1":
            raise ValueError("unknown correlated-feature policy")
        if (
            any(
                not 0 <= v <= 1e-6
                for v in (self.absolute_tolerance, self.relative_tolerance)
            )
            or self.instability_threshold < 0
        ):
            raise ValueError("invalid frozen numerical/ambiguity policy")
        if (
            not 1 <= self.maximum_evaluations <= 65536
            or self.declared_at_ns < 0
        ):
            raise ValueError("explanation work/clock bound")
        if self.causal_nonclaim != CAUSAL_NONCLAIM:
            raise ValueError(
                "predictive attribution cannot claim market causality"
            )
        ordered(tuple(g.name for g in self.groups), nonempty=True)
        ordered(self.reporting_cut, nonempty=True)
        from .reference import validate_groups

        validate_groups(self.groups, self.reporting_cut)


@dataclass(frozen=True, slots=True)
class ExplanationPolicyRegistryV1(Artifact):
    KIND: ClassVar[str] = "policy-registry"
    version: str
    policies: tuple[ExplanationPolicyV1, ...]

    def _validate(self) -> None:
        _semver(self.version)
        ordered(tuple(p.artifact_id for p in self.policies), nonempty=True)
        signatures = [(p.method, p.version) for p in self.policies]
        if len(set(signatures)) != len(signatures):
            raise ValueError("method/version reused for different policy")


@dataclass(frozen=True, slots=True)
class PolynomialTermV1(Record):
    coefficient: float
    features: tuple[str, ...]

    def _validate(self) -> None:
        ordered(self.features)


@dataclass(frozen=True, slots=True)
class ReferenceModelV1(Artifact):
    """Actually executable bounded multilinear synthetic model, never weights."""

    KIND: ClassVar[str] = "reference-model"
    features: tuple[str, ...]
    terms: tuple[PolynomialTermV1, ...]
    output_dimension: str
    output_unit: str
    output_class: str | None = None
    horizon_ns: int | None = None

    def _validate(self) -> None:
        ordered(self.features, nonempty=True)
        if len(self.features) > 8 or not 1 <= len(self.terms) <= 128:
            raise ValueError("reference model work bound")
        if len({term.features for term in self.terms}) != len(self.terms):
            raise ValueError("duplicate polynomial term")
        if any(not set(t.features) <= set(self.features) for t in self.terms):
            raise ValueError("unknown polynomial coordinate")
        text(self.output_dimension)
        text(self.output_unit)
        if self.output_class is not None:
            text(self.output_class)
        if self.horizon_ns is not None and self.horizon_ns < 0:
            raise ValueError("negative explained horizon")


@dataclass(frozen=True, slots=True)
class ExplainedOutputV1(Record):
    dimension: str
    unit: str
    class_label: str | None
    horizon_ns: int | None

    def _validate(self) -> None:
        text(self.dimension)
        text(self.unit)
        if self.class_label is not None:
            text(self.class_label)
        if self.horizon_ns is not None and self.horizon_ns < 0:
            raise ValueError("negative explained horizon")


@dataclass(frozen=True, slots=True)
class ContributionV1(Record):
    name: str
    value: float

    def _validate(self) -> None:
        text(self.name)


@dataclass(frozen=True, slots=True)
class InteractionContributionV1(Record):
    features: tuple[str, ...]
    value: float

    def _validate(self) -> None:
        ordered(self.features, nonempty=True)
        if len(self.features) != 2:
            raise ValueError("pair dividend needs exactly two features")

    @property
    def name(self) -> str:
        from ._wire import canonical_json

        return canonical_json(self.features)


@dataclass(frozen=True, slots=True)
class AttributionAmbiguityV1(Record):
    name: str
    background_range: float
    sibling_sensitivity: float

    def _validate(self) -> None:
        text(self.name)
        if min(self.background_range, self.sibling_sensitivity) < 0:
            raise ValueError("negative ambiguity diagnostic")


@dataclass(frozen=True, slots=True)
class DecisionAttributionV1(Artifact):
    KIND: ClassVar[str] = "decision"
    evidence_kind: AttributionEvidenceKind
    policy: ExplanationPolicyV1
    model_reference: AttributionReferenceV1
    model: ReferenceModelV1 | None
    snapshot: AttributionSnapshotV1
    background: AttributionBackgroundV1
    generated_at_ns: int
    state: AttributionState
    raw_output: float | None
    baseline_output: float | None
    contributions: tuple[ContributionV1, ...]
    groups: tuple[ContributionV1, ...]
    interactions: tuple[InteractionContributionV1, ...]
    ambiguity: tuple[AttributionAmbiguityV1, ...]
    additive_residual: float | None
    group_additive_residual: float | None
    effective_feature_dimension: float | None
    evaluations: int
    reason: str
    explained_output: ExplainedOutputV1
    causal_nonclaim: str = CAUSAL_NONCLAIM

    def _validate(self) -> None:
        if self.causal_nonclaim != CAUSAL_NONCLAIM:
            raise ValueError("causal nonclaim is mandatory")
        if (
            self.generated_at_ns < self.snapshot.cutoff_at_ns
            or self.evaluations < 0
        ):
            raise ValueError("explanation generation/diagnostic clock")
        text(self.reason)
        for rows in (
            self.contributions,
            self.groups,
            self.interactions,
            self.ambiguity,
        ):
            ordered(tuple(row.name for row in rows))
        if self.evidence_kind is AttributionEvidenceKind.DECLARED:
            if (
                self.state is not AttributionState.DECLARED
                or self.model is not None
                or self.evaluations
            ):
                raise ValueError(
                    "external attribution cannot claim executed reference evidence"
                )
            if (
                self.model_reference.evidence_kind
                is not AttributionEvidenceKind.DECLARED
            ):
                raise ValueError("external model must remain unverified")
        else:
            from .reference import verify_attribution

            verify_attribution(self)


@dataclass(frozen=True, slots=True)
class ExplanationPlanV1(Artifact):
    """Noncircular scientific method configuration: no experiment/output ID."""

    KIND: ClassVar[str] = "plan"
    model: AttributionReferenceV1
    policy: AttributionReferenceV1
    background: AttributionReferenceV1
    preprocessing: AttributionReferenceV1
    projection: AttributionReferenceV1
    declared_at_ns: int

    def _validate(self) -> None:
        if self.declared_at_ns < 0:
            raise ValueError("negative plan declaration clock")
