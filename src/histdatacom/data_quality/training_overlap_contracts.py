"""Additive overlap contracts, SemVer 1.0.0; frozen training v1 is unchanged.

Envelope construction proves structure only. Public materialization/persistence
replays complete sources, splits, arithmetic, native consumers and prefix state.
"""

from __future__ import annotations

import math
import hashlib
from dataclasses import dataclass
from fractions import Fraction
from typing import ClassVar

from .training_contracts import (
    TrainingContract,
    TrainingOwnershipV1,
    TrainingSourceV1,
    _clock,
    _text,
    training_json,
    training_load,
)
from .training_overlap_math import overlap_intervals
from .training_temporal_contracts import (
    TemporalPartition,
    TemporalStatePolicy,
    TrainingTemporalPlanV1,
    TrainingTemporalSplitV1,
)

OVERLAP_VERSION = "1.0.0"
OVERLAP_POLICY = "complete-inventory-purge-then-components-unit-mass.1.0.0"
OVERLAP_NONCLAIMS = (
    "expost_source_replay_not_historical_causal_availability",
    "per_original_unit_mass_cap_not_independent_history_or_confidence",
    "native_full_window_features_not_available_to_interior_event_rows",
    "no_empirical_weighting_skill_or_holdout_qualification",
)


def _unique(values: tuple[str, ...]) -> None:
    if len(values) != len(set(values)):
        raise ValueError("overlap inventory contains duplicate identities")
    for value in values:
        _text(value)


def _canonical(value: str) -> None:
    if training_json(training_load(value)) != value:
        raise ValueError("overlap retained payload must be canonical JSON")


@dataclass(frozen=True, slots=True)
class TrainingOverlapGeometryV1(TrainingContract):
    KIND: ClassVar[str] = "overlap-geometry"
    start_ns: int
    end_ns: int
    width_ns: int
    stride_ns: int
    tail_policy: str = "full-windows-first-start-exact-explicit-uncovered.1.0.0"

    def _validate(self) -> None:
        overlap_intervals(
            self.start_ns, self.end_ns, self.width_ns, self.stride_ns
        )
        if (
            self.tail_policy
            != "full-windows-first-start-exact-explicit-uncovered.1.0.0"
        ):
            raise ValueError("unsupported overlap tail/grid policy")

    @property
    def intervals(self) -> tuple[tuple[int, int], ...]:
        return overlap_intervals(
            self.start_ns, self.end_ns, self.width_ns, self.stride_ns
        )

    @property
    def maximum_multiplicity(self) -> int:
        return min(
            len(self.intervals),
            (self.width_ns + self.stride_ns - 1) // self.stride_ns,
        )


@dataclass(frozen=True, slots=True)
class TrainingOverlapTargetV1(TrainingContract):
    KIND: ClassVar[str] = "overlap-target"
    window_index: int
    plan: TrainingTemporalPlanV1

    def _validate(self) -> None:
        _clock(self.window_index)


@dataclass(frozen=True, slots=True)
class TrainingOverlapPlanV1(TrainingContract):
    KIND: ClassVar[str] = "overlap-plan"
    source: TrainingSourceV1
    ownership: TrainingOwnershipV1
    split: TrainingTemporalSplitV1
    geometry: TrainingOverlapGeometryV1
    carry_lookback_ns: int = 0
    carry_policy: TemporalStatePolicy = TemporalStatePolicy.RESET
    targets: tuple[TrainingOverlapTargetV1, ...] = ()
    native_intervals: tuple[str, ...] = (
        "1m",
        "5m",
        "15m",
        "30m",
        "1h",
        "4h",
        "1d",
    )
    triangle_policy_json: str | None = None
    policy_version: str = OVERLAP_POLICY
    contract_version: str = OVERLAP_VERSION

    def _validate(self) -> None:
        _clock(self.carry_lookback_ns)
        if self.carry_lookback_ns > self.geometry.start_ns:
            raise ValueError(
                "overlap lookback precedes normalized clock origin"
            )
        if (
            self.policy_version != OVERLAP_POLICY
            or self.contract_version != OVERLAP_VERSION
        ):
            raise ValueError("unsupported overlap policy/version")
        if self.source.dataset_version_id != self.ownership.dataset_version_id:
            raise ValueError("overlap ownership uses a different source")
        units = self.ownership.units
        if self.split.ownership_id != self.ownership.artifact_id or tuple(
            a.evidence_unit_id for a in self.split.assignments
        ) != tuple(u.artifact_id for u in units):
            raise ValueError(
                "overlap requires the exact complete ownership split"
            )
        ranks = {
            TemporalPartition.TRAIN: 0,
            TemporalPartition.VALIDATION: 1,
            TemporalPartition.TEST: 2,
        }
        sequence = tuple(ranks[a.partition] for a in self.split.assignments)
        if sequence != tuple(sorted(sequence)):
            raise ValueError("overlap split must be chronological")
        if (
            self.geometry.start_ns < units[0].start_ns
            or self.geometry.end_ns > units[-1].end_ns
        ):
            raise ValueError("overlap geometry is outside complete ownership")
        if len(self.targets) > len(self.geometry.intervals) or len(
            {t.window_index for t in self.targets}
        ) != len(self.targets):
            raise ValueError("duplicate/oversized overlap target inventory")
        if tuple(t.window_index for t in self.targets) != tuple(
            sorted(t.window_index for t in self.targets)
        ):
            raise ValueError("overlap target bindings must be ordered")
        for target in self.targets:
            if target.window_index >= len(self.geometry.intervals):
                raise ValueError("overlap target names an unscheduled window")
            if (
                target.plan.source,
                target.plan.ownership,
                target.plan.split,
            ) != (self.source, self.ownership, self.split):
                raise ValueError(
                    "overlap target must replay identical source/ownership/split"
                )
            end = self.geometry.intervals[target.window_index][1]
            if any(e.decision_time_ns != end for e in target.plan.examples):
                raise ValueError(
                    "overlap targets begin exactly at the child end"
                )
        from histdatacom.synthetic.bars import STANDARD_DERIVED_BAR_INTERVALS

        _unique(self.native_intervals)
        if not set(self.native_intervals) <= set(
            STANDARD_DERIVED_BAR_INTERVALS
        ):
            raise ValueError("unsupported native timeframe")
        if self.triangle_policy_json is not None:
            _canonical(self.triangle_policy_json)


@dataclass(frozen=True, slots=True)
class TrainingOverlapSelectionV1(TrainingContract):
    KIND: ClassVar[str] = "overlap-selection"
    window_ids: tuple[str, ...] | None = None
    coordinate_ids: tuple[str, ...] | None = None
    member_ids: tuple[str, ...] | None = None

    def _validate(self) -> None:
        for values in (self.window_ids, self.coordinate_ids, self.member_ids):
            if values is not None:
                _unique(values)
                if values != tuple(sorted(values)):
                    raise ValueError("overlap selection must be ordered")


@dataclass(frozen=True, slots=True)
class TrainingOverlapCoordinateV1(TrainingContract):
    KIND: ClassVar[str] = "overlap-coordinate"
    source_row_key: str
    evidence_unit_id: str
    symbol: str
    event_time_ns: int
    value_json: str
    window_indices: tuple[int, ...]

    def _validate(self) -> None:
        for value in (self.source_row_key, self.evidence_unit_id, self.symbol):
            _text(value)
        _clock(self.event_time_ns)
        _canonical(self.value_json)
        if self.window_indices != tuple(sorted(set(self.window_indices))):
            raise ValueError(
                "coordinate memberships must be ordered and unique"
            )
        for index in self.window_indices:
            _clock(index)

    @property
    def coordinate_id(self) -> str:
        """Immutable source identity, independent of analytical memberships."""
        payload = self.payload()
        payload.pop("window_indices")
        payload.pop("evidence_unit_id")
        return (
            "overlap-coordinate:sha256:"
            + hashlib.sha256(training_json(payload).encode()).hexdigest()
        )


@dataclass(frozen=True, slots=True)
class TrainingOverlapOccurrenceV1(TrainingContract):
    KIND: ClassVar[str] = "overlap-occurrence"
    coordinate_id: str
    member_id: str
    scenario_id: str
    product_manifest_id: str | None
    native_event_json: str | None
    dependency_start_ns: int
    dependency_end_ns: int

    def _validate(self) -> None:
        for value in (self.coordinate_id, self.member_id, self.scenario_id):
            _text(value)
        _clock(self.dependency_start_ns)
        _clock(self.dependency_end_ns)
        if self.dependency_start_ns >= self.dependency_end_ns:
            raise ValueError("occurrence requires nonempty actual dependency")
        if (self.product_manifest_id is None) != (
            self.native_event_json is None
        ):
            raise ValueError(
                "native occurrence requires product and exact event"
            )
        if self.native_event_json is not None:
            _canonical(self.native_event_json)


@dataclass(frozen=True, slots=True)
class TrainingOverlapCarryV1(TrainingContract):
    KIND: ClassVar[str] = "overlap-carry"
    window_id: str
    symbol: str
    member_id: str
    scenario_id: str
    coordinate_ids: tuple[str, ...]
    occurrence_ids: tuple[str, ...]
    last_midpoint: float | None
    mean_midpoint: float | None
    dependency_start_ns: int
    dependency_end_ns: int
    state: str

    def _validate(self) -> None:
        for value in (
            self.window_id,
            self.symbol,
            self.member_id,
            self.scenario_id,
        ):
            _text(value)
        _unique(self.coordinate_ids)
        _unique(self.occurrence_ids)
        _clock(self.dependency_start_ns)
        _clock(self.dependency_end_ns)
        if self.dependency_start_ns > self.dependency_end_ns:
            raise ValueError("carry support is backwards")
        if self.state not in (
            "available_strict_prior",
            "empty_prior_support",
            "unavailable_future_conditioning",
            "unavailable_cross_partition",
        ):
            raise ValueError("unknown prefix state")
        available = self.state == "available_strict_prior"
        if available != bool(self.coordinate_ids) or available != (
            self.last_midpoint is not None and self.mean_midpoint is not None
        ):
            raise ValueError("carry values/support disagree")
        for number in (self.last_midpoint, self.mean_midpoint):
            if number is not None and (
                not math.isfinite(number) or number <= 0
            ):
                raise ValueError("carry midpoint must be finite and positive")


@dataclass(frozen=True, slots=True)
class TrainingOverlapWindowV1(TrainingContract):
    KIND: ClassVar[str] = "overlap-window"
    window_id: str
    index: int
    start_ns: int
    end_ns: int
    parent_unit_ids: tuple[str, ...]
    dependency_unit_ids: tuple[str, ...]
    partition: TemporalPartition | None
    group_id: str | None
    dependency_start_ns: int
    dependency_end_ns: int
    reasons: tuple[str, ...]

    def _validate(self) -> None:
        _text(self.window_id)
        _unique(self.parent_unit_ids)
        _unique(self.dependency_unit_ids)
        if not set(self.parent_unit_ids) <= set(self.dependency_unit_ids):
            raise ValueError(
                "window dependency owners must cover original core owners"
            )
        for value in (
            self.index,
            self.start_ns,
            self.end_ns,
            self.dependency_start_ns,
            self.dependency_end_ns,
        ):
            _clock(value)
        if (
            self.start_ns >= self.end_ns
            or self.dependency_start_ns > self.start_ns
            or self.dependency_end_ns < self.end_ns
        ):
            raise ValueError("window dependency does not cover core")
        if self.reasons != tuple(sorted(set(self.reasons))):
            raise ValueError("window reasons must be ordered and unique")
        allowed = {
            "core_crosses_partition",
            "conditioning_crosses_partition",
            "target_crosses_partition",
            "purged_dependency_overlap",
            "embargo_dependency_span",
            "unavailable_prefix_state",
            "temporal_target_excluded",
        }
        if not set(self.reasons) <= allowed:
            raise ValueError("unknown window exclusion")
        if bool(self.reasons) == (self.group_id is not None):
            raise ValueError("only admitted windows have an overlap group")


@dataclass(frozen=True, slots=True)
class TrainingOverlapMassV1(TrainingContract):
    KIND: ClassVar[str] = "overlap-mass"
    evidence_unit_id: str
    window_id: str
    member_id: str
    scenario_id: str
    complete_row_count: int
    numerator: str
    denominator: str
    reserved_numerator: str
    reserved_denominator: str

    def _validate(self) -> None:
        for value in (
            self.evidence_unit_id,
            self.window_id,
            self.member_id,
            self.scenario_id,
        ):
            _text(value)
        _clock(self.complete_row_count)
        for value in (
            self.numerator,
            self.denominator,
            self.reserved_numerator,
            self.reserved_denominator,
        ):
            if (
                not value.isascii()
                or not value.isdigit()
                or len(value) > 128
                or str(int(value)) != value
            ):
                raise ValueError(
                    "mass requires bounded canonical nonnegative integers"
                )
        if not int(self.denominator) or not int(self.reserved_denominator):
            raise ValueError("mass denominator must be positive")
        if self.per_row * self.complete_row_count > self.reserved_mass:
            raise ValueError("row allocation exceeds reserved branch budget")

    @property
    def per_row(self) -> Fraction:
        return Fraction(int(self.numerator), int(self.denominator))

    @property
    def reserved_mass(self) -> Fraction:
        return Fraction(
            int(self.reserved_numerator), int(self.reserved_denominator)
        )


@dataclass(frozen=True, slots=True)
class TrainingOverlapNativeV1(TrainingContract):
    KIND: ClassVar[str] = "overlap-native"
    window_id: str
    product_manifest_id: str
    feature_cutoff_ns: int
    dependency_end_ns: int
    event_coordinate_ids: tuple[str, ...]
    bars_json: tuple[str, ...]
    triangle_tuples_json: tuple[str, ...]

    def _validate(self) -> None:
        _text(self.window_id)
        _text(self.product_manifest_id)
        _clock(self.feature_cutoff_ns)
        _clock(self.dependency_end_ns)
        _unique(self.event_coordinate_ids)
        for text in (*self.bars_json, *self.triangle_tuples_json):
            _canonical(text)


@dataclass(frozen=True, slots=True)
class TrainingOverlapRowV1(TrainingContract):
    KIND: ClassVar[str] = "overlap-row"
    window_id: str
    coordinate_id: str
    occurrence_id: str
    allocation_id: str
    event_time_ns: int
    analytical_cutoff_ns: int
    historical_available_at_ns: int | None = None

    def _validate(self) -> None:
        for value in (
            self.window_id,
            self.coordinate_id,
            self.occurrence_id,
            self.allocation_id,
        ):
            _text(value)
        _clock(self.event_time_ns)
        _clock(self.analytical_cutoff_ns)
        if (
            self.analytical_cutoff_ns <= self.event_time_ns
            or self.historical_available_at_ns is not None
        ):
            raise ValueError("overlap descendants are explicitly ex-post")


@dataclass(frozen=True, slots=True)
class TrainingOverlapBatchV1(TrainingContract):
    KIND: ClassVar[str] = "overlap-batch"
    plan: TrainingOverlapPlanV1
    selection: TrainingOverlapSelectionV1
    coordinates: tuple[TrainingOverlapCoordinateV1, ...]
    occurrences: tuple[TrainingOverlapOccurrenceV1, ...]
    windows: tuple[TrainingOverlapWindowV1, ...]
    carries: tuple[TrainingOverlapCarryV1, ...]
    allocations: tuple[TrainingOverlapMassV1, ...]
    native: tuple[TrainingOverlapNativeV1, ...]
    targets_json: tuple[str, ...]
    maximum_dependency_span_ns: int
    rows: tuple[TrainingOverlapRowV1, ...]
    nonclaims: tuple[str, ...] = OVERLAP_NONCLAIMS

    def _validate(self) -> None:
        if self.nonclaims != OVERLAP_NONCLAIMS:
            raise ValueError("overlap claim boundary differs")
        _clock(self.maximum_dependency_span_ns)
        for values in (
            self.coordinates,
            self.occurrences,
            self.windows,
            self.carries,
            self.allocations,
            self.native,
            self.rows,
        ):
            _unique(tuple(v.artifact_id for v in values))
        if len(self.windows) != len(self.plan.geometry.intervals):
            raise ValueError("overlap report omits scheduled windows")
        for text in self.targets_json:
            _canonical(text)
