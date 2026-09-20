"""Additive temporal training contracts (SemVer 1.0.0).

These envelopes describe research operations, not historical clock attestations.
Only the source-replaying materializer/reader establishes their executable claims.
The frozen v1 training row contracts are intentionally not modified.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum
from typing import ClassVar

from .training_contracts import (
    TrainingContract,
    TrainingBatchV1,
    TrainingOwnershipV1,
    TrainingSourceV1,
    _clock,
    _text,
)

TEMPORAL_VERSION = "1.0.0"
MAX_TEMPORAL_EXAMPLES = 256
TEMPORAL_NONCLAIMS = (
    "normalized_clocks_are_not_historical_source_authenticity",
    "unknown_legacy_availability_is_not_causal_admission",
    "synthetic_siblings_are_not_independent_history",
    "no_empirical_skill_or_holdout_qualification",
)


class TemporalInformationMode(str, Enum):
    NORMALIZED_AS_OF = "normalized_vintage_clock_research"
    EX_POST = "explicit_ex_post_reconstruction_research"


class TemporalPartition(str, Enum):
    TRAIN = "train"
    VALIDATION = "validation"
    TEST = "test"


class TemporalFeatureKind(str, Enum):
    QUOTE_AT_DECISION = "quote_at_exact_decision"
    QUOTE_MEAN = "quote_rolling_mean"
    QUOTE_ZSCORE = "quote_rolling_zscore"
    VINTAGE = "replayed_vintage_cell"


class TemporalStatePolicy(str, Enum):
    RESET = "refuse_cross_partition_state"
    WARMUP_ONLY = "prior_observations_only_unscored_warmup"


class TemporalLabelKind(str, Enum):
    LOG_RETURN = "log_return_t_open_h_closed.v1"
    REALIZED_VARIANCE = "sum_squared_log_increments_t_open_h_closed.v1"
    DIRECTION = "positive_deadband_direction_t_open_h_closed.v1"
    EVENT_RESPONSE = "release_owned_log_return_t_open_h_closed.v1"


class TemporalTargetKind(str, Enum):
    QUOTES = "verified_quote_midpoints"
    VINTAGE_MARKET = "initial_vintage_market_levels"


def _unique(values: tuple[str, ...], *, nonempty: bool = True) -> None:
    if (nonempty and not values) or len(set(values)) != len(values):
        raise ValueError("temporal inventory is empty or contains duplicates")
    for value in values:
        _text(value)


@dataclass(frozen=True, slots=True)
class TrainingTemporalAssignmentV1(TrainingContract):
    KIND: ClassVar[str] = "temporal-assignment"
    evidence_unit_id: str
    partition: TemporalPartition

    def _validate(self) -> None:
        _text(self.evidence_unit_id)


@dataclass(frozen=True, slots=True)
class TrainingTemporalSplitV1(TrainingContract):
    KIND: ClassVar[str] = "temporal-split"
    ownership_id: str
    assignments: tuple[TrainingTemporalAssignmentV1, ...]
    policy_version: str = "complete-unit-chronological-purge-embargo.1.0.0"

    def _validate(self) -> None:
        _text(self.ownership_id)
        _unique(tuple(item.evidence_unit_id for item in self.assignments))
        if self.policy_version != (
            "complete-unit-chronological-purge-embargo.1.0.0"
        ):
            raise ValueError("unsupported temporal split policy")


@dataclass(frozen=True, slots=True)
class TrainingTemporalFeatureV1(TrainingContract):
    KIND: ClassVar[str] = "temporal-feature"
    name: str
    kind: TemporalFeatureKind
    lookback_ns: int = 0
    matrix_id: str | None = None
    column: str | None = None
    period_label: str | None = None
    expected_observation_ids: tuple[str, ...] = ()
    state_policy: TemporalStatePolicy = TemporalStatePolicy.RESET

    def _validate(self) -> None:
        _text(self.name)
        _clock(self.lookback_ns)
        _unique(self.expected_observation_ids, nonempty=False)
        if self.kind is TemporalFeatureKind.VINTAGE:
            if (
                any(
                    v is None
                    for v in (self.matrix_id, self.column, self.period_label)
                )
                or self.lookback_ns
            ):
                raise ValueError("vintage feature requires exact matrix/cell")
            for value in (self.matrix_id, self.column, self.period_label):
                assert value is not None
                _text(value)
        elif (
            any(
                v is not None
                for v in (self.matrix_id, self.column, self.period_label)
            )
            or self.expected_observation_ids
        ):
            raise ValueError("quote features cannot inject vintage/label data")
        elif (
            self.kind is not TemporalFeatureKind.QUOTE_AT_DECISION
            and not self.lookback_ns
        ):
            raise ValueError("rolling feature requires explicit history span")
        elif (
            self.kind is TemporalFeatureKind.QUOTE_AT_DECISION
            and self.lookback_ns
        ):
            raise ValueError("exact-decision quote has no lookback parameter")


@dataclass(frozen=True, slots=True)
class TrainingTemporalLabelV1(TrainingContract):
    KIND: ClassVar[str] = "temporal-label-definition"
    kind: TemporalLabelKind
    horizon_ns: int
    target: TemporalTargetKind = TemporalTargetKind.QUOTES
    deadband: float = 0.0
    matrix_id: str | None = None
    feature_key: str | None = None
    event_observation_id: str | None = None

    def _validate(self) -> None:
        _clock(self.horizon_ns)
        if (
            self.horizon_ns == 0
            or not math.isfinite(self.deadband)
            or self.deadband < 0
        ):
            raise ValueError("label requires positive horizon/finite deadband")
        if self.kind is not TemporalLabelKind.DIRECTION and self.deadband != 0:
            raise ValueError("deadband is meaningful only for direction")
        if self.target is TemporalTargetKind.VINTAGE_MARKET:
            if self.matrix_id is None or self.feature_key is None:
                raise ValueError("market label requires exact source series")
            _text(self.matrix_id)
            _text(self.feature_key)
        elif self.matrix_id is not None or self.feature_key is not None:
            raise ValueError("quote label cannot carry a matrix override")
        if (self.kind is TemporalLabelKind.EVENT_RESPONSE) != (
            self.event_observation_id is not None
        ):
            raise ValueError("event response requires exact release ownership")
        if self.event_observation_id is not None:
            _text(self.event_observation_id)


@dataclass(frozen=True, slots=True)
class TrainingTemporalExampleV1(TrainingContract):
    KIND: ClassVar[str] = "temporal-example"
    key: str
    decision_time_ns: int
    label_cutoff_ns: int
    symbol: str
    evidence_unit_id: str
    partition: TemporalPartition
    features: tuple[TrainingTemporalFeatureV1, ...]
    label: TrainingTemporalLabelV1
    product_manifest_id: str | None = None
    ensemble_member_id: str | None = None

    def _validate(self) -> None:
        for text in (self.key, self.symbol, self.evidence_unit_id):
            _text(text)
        _clock(self.decision_time_ns)
        _clock(self.label_cutoff_ns)
        _clock(self.decision_time_ns + self.label.horizon_ns)
        if self.label_cutoff_ns < self.decision_time_ns + self.label.horizon_ns:
            raise ValueError("label cutoff precedes complete target horizon")
        if len(self.features) > 32:
            raise ValueError("temporal feature count exceeds bound")
        _unique(tuple(f.name for f in self.features))
        if self.product_manifest_id is not None:
            _text(self.product_manifest_id)
        if (self.product_manifest_id is None) != (
            self.ensemble_member_id is None
        ):
            raise ValueError(
                "native temporal selection requires exact product and member"
            )
        if self.ensemble_member_id is not None:
            _text(self.ensemble_member_id)


@dataclass(frozen=True, slots=True)
class TrainingTemporalPlanV1(TrainingContract):
    KIND: ClassVar[str] = "temporal-plan"
    source: TrainingSourceV1
    ownership: TrainingOwnershipV1
    split: TrainingTemporalSplitV1
    information_mode: TemporalInformationMode
    examples: tuple[TrainingTemporalExampleV1, ...]
    legacy_batch_json: str | None = None
    contract_version: str = TEMPORAL_VERSION

    def _validate(self) -> None:
        if self.contract_version != TEMPORAL_VERSION:
            raise ValueError("unsupported temporal contract version")
        if not 0 < len(self.examples) <= MAX_TEMPORAL_EXAMPLES:
            raise ValueError("temporal examples must be nonempty and bounded")
        _unique(tuple(item.key for item in self.examples))
        if self.source.dataset_version_id != self.ownership.dataset_version_id:
            raise ValueError("temporal source and ownership differ")
        if self.split.ownership_id != self.ownership.artifact_id:
            raise ValueError("split does not bind complete ownership")
        expected = tuple(unit.artifact_id for unit in self.ownership.units)
        if (
            tuple(a.evidence_unit_id for a in self.split.assignments)
            != expected
        ):
            raise ValueError("split must assign every complete unit in order")
        order = list(TemporalPartition)
        ranks = [order.index(a.partition) for a in self.split.assignments]
        if ranks != sorted(ranks):
            raise ValueError("split partitions must be chronological")
        lookup = {
            a.evidence_unit_id: a.partition for a in self.split.assignments
        }
        for example in self.examples:
            if lookup.get(example.evidence_unit_id) != example.partition:
                raise ValueError(
                    "example/sibling crosses its historical unit split"
                )
        if self.legacy_batch_json is not None:
            legacy = TrainingBatchV1.from_json(self.legacy_batch_json)
            if legacy.to_json() != self.legacy_batch_json:
                raise ValueError("legacy batch binding must be canonical")
            if (
                legacy.source != self.source
                or legacy.ownership != self.ownership
            ):
                raise ValueError(
                    "legacy batch and temporal source/ownership differ"
                )
            if self.information_mode is not TemporalInformationMode.EX_POST:
                raise ValueError(
                    "legacy batches cannot be promoted to normalized causal clocks"
                )


@dataclass(frozen=True, slots=True)
class TrainingTemporalValueV1(TrainingContract):
    KIND: ClassVar[str] = "temporal-feature-value"
    name: str
    value: float
    available_at_ns: int | None
    dependency_start_ns: int
    dependency_end_ns: int
    record_ids: tuple[str, ...]
    verification: str
    warmup_unit_ids: tuple[str, ...] = ()

    def _validate(self) -> None:
        _text(self.name)
        _text(self.verification)
        if not math.isfinite(self.value):
            raise ValueError("temporal feature must be finite")
        for clock in (self.dependency_start_ns, self.dependency_end_ns):
            _clock(clock)
        if self.dependency_start_ns >= self.dependency_end_ns:
            raise ValueError("feature dependency interval must be nonempty")
        if self.available_at_ns is not None:
            _clock(self.available_at_ns)
        _unique(self.record_ids)
        _unique(self.warmup_unit_ids, nonempty=False)


@dataclass(frozen=True, slots=True)
class TrainingTemporalOutcomeV1(TrainingContract):
    KIND: ClassVar[str] = "temporal-outcome"
    value: float
    target_record_ids: tuple[str, ...]
    baseline_record_id: str
    target_start_exclusive_ns: int
    target_end_inclusive_ns: int
    available_at_ns: int | None
    event_record_id: str | None
    target_unit_ids: tuple[str, ...]
    dependency_start_ns: int
    dependency_end_ns: int

    def _validate(self) -> None:
        if not math.isfinite(self.value):
            raise ValueError("temporal label must be finite")
        _unique(self.target_record_ids)
        _unique(self.target_unit_ids)
        _text(self.baseline_record_id)
        _clock(self.target_start_exclusive_ns)
        _clock(self.target_end_inclusive_ns)
        if self.target_start_exclusive_ns >= self.target_end_inclusive_ns:
            raise ValueError("label interval is empty")
        if self.available_at_ns is not None:
            _clock(self.available_at_ns)
        if self.event_record_id is not None:
            _text(self.event_record_id)
        _clock(self.dependency_start_ns)
        _clock(self.dependency_end_ns)
        if self.dependency_start_ns >= self.dependency_end_ns:
            raise ValueError("label dependency interval must be nonempty")


@dataclass(frozen=True, slots=True)
class TrainingTemporalRowV1(TrainingContract):
    KIND: ClassVar[str] = "temporal-row"
    example_key: str
    evidence_unit_id: str
    partition: TemporalPartition
    features: tuple[TrainingTemporalValueV1, ...]
    outcome: TrainingTemporalOutcomeV1
    status: str
    reasons: tuple[str, ...]

    def _validate(self) -> None:
        _text(self.example_key)
        _text(self.evidence_unit_id)
        _unique(tuple(value.name for value in self.features))
        if self.status not in ("admitted", "excluded"):
            raise ValueError("unknown temporal row status")
        allowed = {
            "target_crosses_partition",
            "purged_dependency_overlap",
            "embargo_dependency_span",
        }
        if (
            tuple(sorted(set(self.reasons))) != self.reasons
            or not set(self.reasons) <= allowed
        ):
            raise ValueError("unknown or duplicate temporal exclusion reason")
        if (self.status == "admitted") != (not self.reasons):
            raise ValueError("status/exclusion reasons disagree")


@dataclass(frozen=True, slots=True)
class TrainingTemporalBatchV1(TrainingContract):
    KIND: ClassVar[str] = "temporal-batch"
    plan: TrainingTemporalPlanV1
    maximum_dependency_span_ns: int
    split_boundaries_ns: tuple[int, ...]
    rows: tuple[TrainingTemporalRowV1, ...]
    nonclaims: tuple[str, ...] = TEMPORAL_NONCLAIMS

    def _validate(self) -> None:
        _clock(self.maximum_dependency_span_ns)
        if (
            self.maximum_dependency_span_ns == 0
            or self.nonclaims != TEMPORAL_NONCLAIMS
        ):
            raise ValueError("invalid temporal span/claim boundary")
        if tuple(row.example_key for row in self.rows) != tuple(
            e.key for e in self.plan.examples
        ):
            raise ValueError("temporal batch must retain every planned result")
        if self.split_boundaries_ns != tuple(
            sorted(set(self.split_boundaries_ns))
        ):
            raise ValueError("split boundaries must be unique and ordered")
        for clock in self.split_boundaries_ns:
            _clock(clock)
