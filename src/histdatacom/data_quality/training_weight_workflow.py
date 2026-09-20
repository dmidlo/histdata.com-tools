"""Source-replayed weighting research; no empirical claim follows from a hash.

Candidate replay is mandatory at every public consuming boundary. Calibration
truth is opened once per distinct original month, never per generated member.
Application error computation additionally requires the exact no-clobber
allocation file: all policy allocations precede outcome inspection. Rows retain
original-parent day ownership, native values and explicitly ex-post semantics.
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass, replace
from fractions import Fraction
from pathlib import Path
from typing import ClassVar, Iterator, Sequence

from histdatacom.synthetic.contracts import (
    SyntheticEventOrigin,
    SyntheticEventV1,
)
from histdatacom.synthetic.generation import EmpiricalMotifGeneratorConfigV1

from .training_contracts import (
    MAX_TRAINING_BYTES,
    NO_LABEL_SCHEMA,
    TrainingConsumerMode,
    TrainingContract,
    TrainingInformationMode,
    TrainingOrigin,
    TrainingRootV1,
    TrainingRowV1,
    TrainingVerificationLevel,
    admitted_modes,
    training_json,
    training_load,
)
from .training_lineage import (
    _ObservedRow,
    _ownership,
    read_training_regular,
    verify_training_source,
)
from .training_weight_approval import (
    TrainingWeightExecutionApprovalV1,
    require_training_weight_approval,
)
from .training_weight_artifacts import _publish, _sync
from .training_weight_calibration import (
    WEIGHT_MEMBERS,
    WEIGHT_PIP,
    WEIGHT_PROBES,
    WEIGHT_SYMBOLS,
    TrainingMemberScaleV1,
    TrainingWeightApplicabilityV1,
    TrainingWeightCalibrationDayV1,
    TrainingWeightCalibrationV1,
    TrainingWeightRadiiV1,
    TrainingWeightScaleSetV1,
    WeightSupport,
    _label,
    apply_training_weight_calibration_math,
    available_path_scale,
    fit_training_weight_calibration_math,
    maximum_path_error,
)
from .training_weight_candidates import (
    TrainingWeightCandidateDayV1,
    TrainingWeightCandidateShard,
    TrainingWeightModelV1,
    replay_training_weight_candidate_shard,
)
from .training_weight_contracts import (
    _object,
    _text,
    read_training_weight_preregistration,
)
from .training_weight_evaluation import (
    TrainingPairedInferenceV1,
    TrainingPairedUnitLossV1,
    _report_view,
    compare_historical_unit_losses,
    fixed_family_holm,
    weighted_member_loss_math,
)
from .training_weight_lineage import (
    CORE_END_NS,
    CORE_START_NS,
    MAX_AGE_NS,
    SECOND_NS,
    TrainingWeightDayBridgeV1,
    WeightEvidenceKind,
    _day_ns,
    _probe_rows,
)
from .training_weights import (
    MemberMass,
    TrainingMemberPolicy,
    UnitMassAllocation,
    _finite_view,
    allocate_unit_mass,
    member_correlation_dimension,
)

MAX_WORKFLOW_ROWS = 4096
MAX_WORKFLOW_NODES = 131_072
MAX_RATIONAL_DIGITS = 4096


class _RowBudgetExceeded(ValueError):
    """A valid request needs streaming chunks, not a relaxed artifact bound."""


def _expanded_nodes(value: object) -> int:
    remaining = MAX_WORKFLOW_NODES

    def visit(item: object, depth: int, name: str = "") -> None:
        nonlocal remaining
        remaining -= 1
        if remaining < 0 or depth > 16:
            raise _RowBudgetExceeded(
                "weighted artifact exceeds frozen expanded-node/depth bound"
            )
        if type(item) is dict:
            for key, child in item.items():
                visit(key, depth + 1)
                visit(child, depth + 1, key)
        elif type(item) is list:
            for child in item:
                visit(child, depth + 1, name)
        elif type(item) is str and name in (
            "value_json",
            "uncertainty_json",
            "refusal_records",
        ):
            visit(training_load(item), depth + 1)

    visit(value, 0)
    return MAX_WORKFLOW_NODES - remaining


_POLICIES = (
    (TrainingMemberPolicy.OBSERVED, 0),
    (TrainingMemberPolicy.EQUAL, 0),
    *((TrainingMemberPolicy.ONE, e) for e in range(4)),
    (TrainingMemberPolicy.MARGINALIZED, 0),
    (TrainingMemberPolicy.UNCERTAINTY, 0),
    (TrainingMemberPolicy.NAIVE, 0),
)


def _shard_ids(role: str) -> tuple[str, ...]:
    periods = ("201001", "201101") if role == "calibration" else ("201102",)
    return tuple(
        f"{period}-{index}" for period in periods for index in range(1, 4)
    )


def _policy_key(policy: TrainingMemberPolicy, epoch: int) -> str:
    return policy.value + (
        f":epoch={epoch}" if policy is TrainingMemberPolicy.ONE else ""
    )


def _fraction(numerator: str, denominator: str) -> Fraction:
    for value in (numerator, denominator):
        if (
            type(value) is not str
            or not 1 <= len(value) <= MAX_RATIONAL_DIGITS
            or not value.isascii()
            or not value.isdecimal()
            or (len(value) > 1 and value[0] == "0")
        ):
            raise ValueError("weight mass requires bounded canonical integers")
    if denominator == "0":
        raise ValueError("weight mass denominator must be positive")
    result = Fraction(int(numerator), int(denominator))
    if (
        str(result.numerator) != numerator
        or str(result.denominator) != denominator
    ):
        raise ValueError("weight mass fraction is not reduced")
    return result


@dataclass(frozen=True, slots=True)
class TrainingWeightMemberMassV1(TrainingContract):
    KIND: ClassVar[str] = "weight-member-mass"
    member_id: str
    numerator: str
    denominator: str
    complete_core_rows: int
    complete_support_rows: int

    def _validate(self) -> None:
        if self.member_id not in (*WEIGHT_MEMBERS, "observed"):
            raise ValueError("weight member is outside fixed scope")
        _fraction(self.numerator, self.denominator)
        if not 0 < self.complete_core_rows <= MAX_WORKFLOW_ROWS:
            raise ValueError("complete core row count exceeds bound")
        if (
            not self.complete_core_rows
            <= self.complete_support_rows
            <= MAX_WORKFLOW_ROWS
        ):
            raise ValueError("complete support row count exceeds native bounds")

    @property
    def mass(self) -> Fraction:
        return _fraction(self.numerator, self.denominator)


@dataclass(frozen=True, slots=True)
class TrainingWeightPolicyAllocationV1(TrainingContract):
    KIND: ClassVar[str] = "weight-policy-allocation"
    policy: TrainingMemberPolicy
    training_epoch: int
    historical_unit_id: str
    status: str
    members: tuple[TrainingWeightMemberMassV1, ...]
    row_kish_ess: float | None

    def _validate(self) -> None:
        if (self.policy, self.training_epoch) not in _POLICIES:
            raise ValueError("policy/epoch is outside the frozen comparison")
        _label(self.historical_unit_id)
        if self.status == "confidence_unavailable":
            if (
                self.policy is not TrainingMemberPolicy.UNCERTAINTY
                or self.members
                or self.row_kish_ess is not None
            ):
                raise ValueError(
                    "only unsupported confidence may omit allocation"
                )
            return
        names = (
            ("observed",)
            if self.policy is TrainingMemberPolicy.OBSERVED
            else WEIGHT_MEMBERS
        )
        if tuple(m.member_id for m in self.members) != names:
            raise ValueError("allocation must retain complete member support")
        expected = (
            "failed_negative_control"
            if self.policy is TrainingMemberPolicy.NAIVE
            else "allocated"
        )
        if self.status != expected:
            raise ValueError("policy allocation status differs")
        allocation = self.allocation
        expected_total = Fraction(4 if allocation.negative_control else 1)
        if allocation.total_mass != expected_total:
            raise ValueError("allocation does not conserve frozen unit mass")
        squared = sum(
            (m.mass * m.mass / m.complete_core_rows for m in self.members),
            Fraction(),
        )
        expected_ess = _finite_view(expected_total * expected_total / squared)
        if self.row_kish_ess != expected_ess:
            raise ValueError(
                "row Kish ESS differs from complete retained masses"
            )

    @property
    def key(self) -> str:
        return _policy_key(self.policy, self.training_epoch)

    @property
    def allocation(self) -> UnitMassAllocation:
        if not self.members:
            raise ValueError("unsupported confidence has no member allocation")
        return UnitMassAllocation(
            self.policy,
            self.historical_unit_id,
            Fraction(1),
            tuple(MemberMass(m.member_id, m.mass) for m in self.members),
            self.policy is TrainingMemberPolicy.NAIVE,
        )


@dataclass(frozen=True, slots=True)
class TrainingWeightInventoryDayV1(TrainingContract):
    KIND: ClassVar[str] = "weight-inventory-day"
    utc_date: str
    status: str
    historical_unit_id: str | None
    candidate_day_id: str | None

    def _validate(self) -> None:
        _day_ns(self.utc_date)
        _label(self.status)
        if (self.historical_unit_id is None) != (self.candidate_day_id is None):
            raise ValueError("inventory day lineage is incomplete")
        for value in (self.historical_unit_id, self.candidate_day_id):
            if value is not None:
                _label(value)


@dataclass(frozen=True, slots=True)
class TrainingWeightShardReceiptV1(TrainingContract):
    KIND: ClassVar[str] = "weight-shard-scope"
    shard_id: str
    source_plan_id: str
    parent_ownership_id: str
    model_id: str
    candidate_day_ids: tuple[str, ...]
    refusal_records: tuple[str, ...]

    def _validate(self) -> None:
        for value in (
            self.shard_id,
            self.source_plan_id,
            self.parent_ownership_id,
            self.model_id,
        ):
            _label(value)
        if len(self.candidate_day_ids) + len(self.refusal_records) > 8:
            raise ValueError("shard scope exceeds frozen eight-day bound")


@dataclass(frozen=True, slots=True)
class TrainingWeightFitV1(TrainingContract):
    KIND: ClassVar[str] = "weight-source-fit"
    evidence_kind: WeightEvidenceKind
    model_id: str
    shards: tuple[TrainingWeightShardReceiptV1, ...]
    inventory: tuple[TrainingWeightInventoryDayV1, ...]
    calibration: TrainingWeightCalibrationV1 | None

    def _validate(self) -> None:
        _inventory_check(
            self.shards, self.inventory, "calibration", self.model_id
        )
        actual = tuple(
            d.historical_unit_id
            for d in self.inventory
            if d.status == "admitted"
        )
        retained = (
            ()
            if self.calibration is None
            else tuple(
                d.scales.historical_unit_id for d in self.calibration.days
            )
        )
        if actual != retained:
            raise ValueError(
                "fit support differs from exact admitted inventory"
            )


def _inventory_check(
    shards: tuple[TrainingWeightShardReceiptV1, ...],
    inventory: tuple[TrainingWeightInventoryDayV1, ...],
    role: str,
    model_id: str,
) -> None:
    _label(model_id)
    keys = tuple(s.shard_id for s in shards)
    if keys != _shard_ids(role):
        raise ValueError(
            "workflow requires the exact complete frozen shard inventory"
        )
    if any(s.model_id != model_id for s in shards):
        raise ValueError("workflow mixes immutable models")
    if tuple(
        d.utc_date for d in inventory
    ) != read_training_weight_preregistration().scheduled_dates(role):
        raise ValueError("workflow must retain every scheduled day in order")
    ids = tuple(
        d.historical_unit_id
        for d in inventory
        if d.historical_unit_id is not None
    )
    if len(set(ids)) != len(ids):
        raise ValueError("workflow repeats original historical-unit support")
    candidates = tuple(
        d.candidate_day_id for d in inventory if d.candidate_day_id is not None
    )
    scope_candidates = tuple(c for s in shards for c in s.candidate_day_ids)
    if (
        len(set(scope_candidates)) != len(scope_candidates)
        or set(candidates) != set(scope_candidates)
        or len(candidates) != len(scope_candidates)
    ):
        raise ValueError(
            "inventory differs from complete candidate-shard day identities"
        )


@dataclass(frozen=True, slots=True)
class TrainingWeightAllocationDayV1(TrainingContract):
    KIND: ClassVar[str] = "weight-allocation-day"
    utc_date: str
    historical_unit_id: str
    bridge_id: str
    candidate_day_id: str
    scales: TrainingWeightScaleSetV1
    radii: TrainingWeightRadiiV1 | None
    policies: tuple[TrainingWeightPolicyAllocationV1, ...]
    member_feature_dimension: float | None

    def _validate(self) -> None:
        if (
            tuple((p.policy, p.training_epoch) for p in self.policies)
            != _POLICIES
        ):
            raise ValueError(
                "allocation must retain all nine policy/epoch coordinates"
            )
        if (
            any(
                p.historical_unit_id != self.historical_unit_id
                for p in self.policies
            )
            or self.scales.historical_unit_id != self.historical_unit_id
            or self.scales.utc_date != self.utc_date
            or self.scales.role != "application"
        ):
            raise ValueError("allocation day mixes historical support")
        if self.radii is not None and self.radii.scales != self.scales:
            raise ValueError("radii differ from available input scales")
        supported = (
            self.radii is not None
            and self.radii.status is WeightSupport.SUPPORTED
        )
        if bool(self.policies[-2].members) != supported:
            raise ValueError("confidence allocation differs from admission")
        reference = (*self.policies[0].members, *self.policies[1].members)
        counts = {m.member_id: m.complete_core_rows for m in reference}
        support = {m.member_id: m.complete_support_rows for m in reference}
        if self.policies != tuple(
            _policy_allocation(
                p, e, self.historical_unit_id, counts, support, self.radii
            )
            for p, e in _POLICIES
        ):
            raise ValueError(
                "policy masses differ from frozen complete allocation recipes"
            )
        if (
            self.member_feature_dimension is not None
            and not 1.0 <= self.member_feature_dimension <= 4.0
        ):
            raise ValueError(
                "member feature dimension is outside fixed support"
            )


@dataclass(frozen=True, slots=True)
class TrainingWeightAllocationV1(TrainingContract):
    KIND: ClassVar[str] = "weight-source-allocation"
    fit: TrainingWeightFitV1
    shards: tuple[TrainingWeightShardReceiptV1, ...]
    inventory: tuple[TrainingWeightInventoryDayV1, ...]
    days: tuple[TrainingWeightAllocationDayV1, ...]

    def _validate(self) -> None:
        _inventory_check(
            self.shards, self.inventory, "application", self.fit.model_id
        )
        if tuple(
            d.historical_unit_id
            for d in self.inventory
            if d.status == "admitted"
        ) != tuple(d.historical_unit_id for d in self.days):
            raise ValueError("allocation support differs from exact inventory")
        fit_ids = {d.historical_unit_id for d in self.fit.inventory}
        if any(d.historical_unit_id in fit_ids for d in self.days):
            raise ValueError(
                "application and calibration historical units overlap"
            )
        for day in self.days:
            expected = (
                None
                if self.fit.calibration is None
                else apply_training_weight_calibration_math(
                    self.fit.calibration, day.scales
                )
            )
            if day.radii != expected:
                raise ValueError(
                    "allocation radii differ from retained calibration"
                )


def _scope(shard: TrainingWeightCandidateShard) -> TrainingWeightShardReceiptV1:
    return TrainingWeightShardReceiptV1(
        shard.shard_id,
        shard.degradation.source_plan.artifact_id,
        shard.degradation.parent_ownership.artifact_id,
        shard.model.artifact_id,
        tuple(d.artifact_id for d in shard.days),
        tuple(r.to_json() for r in shard.refusals),
    )


def _campaign(
    shards: Sequence[TrainingWeightCandidateShard],
    role: str,
    approval: TrainingWeightExecutionApprovalV1 | None,
) -> tuple[
    tuple[TrainingWeightCandidateShard, ...],
    dict[
        str,
        tuple[
            TrainingWeightCandidateShard,
            TrainingWeightCandidateDayV1,
            TrainingWeightDayBridgeV1,
        ],
    ],
    dict[str, TrainingWeightInventoryDayV1],
]:
    if len(shards) != len(_shard_ids(role)):
        raise ValueError("campaign requires exact complete frozen shard scope")
    ordered = tuple(shards[i] for i in range(len(shards)))
    if any(type(s) is not TrainingWeightCandidateShard for s in ordered):
        raise ValueError("workflow requires typed research candidate shards")
    ordered = tuple(sorted(ordered, key=lambda s: s.shard_id))
    if tuple(s.shard_id for s in ordered) != _shard_ids(role):
        raise ValueError("campaign omits or repeats a scheduled shard")
    if any(
        s.model != ordered[0].model or s.degradation.source_plan.role != role
        for s in ordered
    ):
        raise ValueError("campaign model/evidence/role differs")
    # Disallow alternate republishing of the same month under another identity.
    months: dict[str, str] = {}
    for shard in ordered:
        plan = shard.degradation.source_plan
        previous = months.setdefault(plan.period, plan.artifact_id)
        if previous != plan.artifact_id:
            raise ValueError("campaign mixes original sources for one month")
    days: dict[
        str,
        tuple[
            TrainingWeightCandidateShard,
            TrainingWeightCandidateDayV1,
            TrainingWeightDayBridgeV1,
        ],
    ] = {}
    inventory = {
        d: TrainingWeightInventoryDayV1(
            d, "unavailable_missing_shard", None, None
        )
        for d in read_training_weight_preregistration().scheduled_dates(role)
    }
    for shard in ordered:
        replay_training_weight_candidate_shard(shard, approval=approval)
        bridges = {b.artifact_id: b for b in shard.degradation.bridges}
        for refusal in shard.refusals:
            inventory[refusal.utc_date] = TrainingWeightInventoryDayV1(
                refusal.utc_date, refusal.reason, None, None
            )
        for day in shard.days:
            bridge = bridges[day.bridge_id]
            if bridge.utc_date in days:
                raise ValueError("campaign repeats original day support")
            days[bridge.utc_date] = shard, day, bridge
            inventory[bridge.utc_date] = TrainingWeightInventoryDayV1(
                bridge.utc_date,
                "admitted",
                bridge.parent_unit.artifact_id,
                day.artifact_id,
            )
    if any(d.status == "unavailable_missing_shard" for d in inventory.values()):
        raise ValueError(
            "complete replay did not account for every scheduled day"
        )
    return ordered, days, inventory


def _gate(
    model: TrainingWeightModelV1,
    operation: str,
    approval: TrainingWeightExecutionApprovalV1 | None,
) -> None:
    if model.evidence_kind is WeightEvidenceKind.PREREGISTERED:
        require_training_weight_approval(approval, operation)


def _applicability(
    model: TrainingWeightModelV1,
) -> TrainingWeightApplicabilityV1:
    policy = read_training_weight_preregistration()
    raw = policy.to_dict()
    config = EmpiricalMotifGeneratorConfigV1.from_dict(
        _object(_object(raw["generator"])["config"])
    )
    epoch = _text(_object(raw["epoch_mapping"])["historical_epoch_id"])
    if model.evidence_kind is WeightEvidenceKind.FIXTURE:
        epoch = "fixture-not-empirical:" + epoch
    return TrainingWeightApplicabilityV1(
        policy.artifact_id,
        model.index.index_id,
        model.index_file.sha256,
        config.config_id,
        model.adapter_source_fingerprint,
        epoch,
    )


def _mid(bid: float, ask: float) -> float:
    if not (math.isfinite(bid) and math.isfinite(ask) and 0 < bid <= ask):
        raise ValueError("workflow encountered invalid native quotes")
    return _finite_view((Fraction(bid) + Fraction(ask)) / 2)


def _native_probes(
    events: tuple[SyntheticEventV1, ...], start: int
) -> tuple[tuple[float, ...], tuple[float, ...]]:
    cursor = 0
    mids: list[float] = []
    spreads: list[float] = []
    for offset in range(WEIGHT_PROBES):
        probe = start + offset * SECOND_NS
        while cursor < len(events) and events[cursor].event_time_ns <= probe:
            cursor += 1
        if not cursor or probe - events[cursor - 1].event_time_ns > MAX_AGE_NS:
            raise ValueError("missing_or_stale_fixed_probe")
        event = events[cursor - 1]
        mids.append(_mid(event.bid, event.ask))
        spreads.append(_finite_view(Fraction(event.ask) - Fraction(event.bid)))
    return tuple(mids), tuple(spreads)


def _anchor_bridge(
    events: tuple[SyntheticEventV1, ...], start: int
) -> tuple[float, ...]:
    anchors = tuple(
        e for e in events if e.origin is SyntheticEventOrigin.OBSERVED
    )
    cursor = 0
    result: list[float] = []
    for offset in range(WEIGHT_PROBES):
        probe = start + offset * SECOND_NS
        while cursor < len(anchors) and anchors[cursor].event_time_ns <= probe:
            cursor += 1
        if not cursor:
            raise ValueError("anchor bridge lacks left support")
        left = anchors[cursor - 1]
        left_mid = Fraction(_mid(left.bid, left.ask))
        if probe == left.event_time_ns:
            result.append(float(left_mid))
            continue
        if cursor == len(anchors):
            raise ValueError("anchor bridge lacks right support")
        right = anchors[cursor]
        fraction = Fraction(
            probe - left.event_time_ns, right.event_time_ns - left.event_time_ns
        )
        result.append(
            _finite_view(
                left_mid
                + fraction * (Fraction(_mid(right.bid, right.ask)) - left_mid)
            )
        )
    return tuple(result)


def _paths(
    day: TrainingWeightCandidateDayV1, bridge: TrainingWeightDayBridgeV1
) -> dict[tuple[str, str], tuple[float, ...]]:
    start = _day_ns(bridge.utc_date) + CORE_START_NS
    return {
        (c.member_id, c.symbol): _native_probes(c.stream.events, start)[0]
        for c in day.candidates
    }


def _scales(
    shard: TrainingWeightCandidateShard,
    day: TrainingWeightCandidateDayV1,
    bridge: TrainingWeightDayBridgeV1,
) -> TrainingWeightScaleSetV1:
    start = _day_ns(bridge.utc_date) + CORE_START_NS
    values: dict[tuple[str, str], float] = {}
    for candidate in day.candidates:
        events = candidate.stream.events
        mids, spreads = _native_probes(events, start)
        values[candidate.member_id, candidate.symbol] = available_path_scale(
            mids, spreads, _anchor_bridge(events, start)
        )
    return TrainingWeightScaleSetV1(
        bridge.parent_unit.artifact_id,
        bridge.utc_date,
        shard.degradation.source_plan.role,
        _applicability(shard.model),
        day.artifact_id,
        tuple(
            TrainingMemberScaleV1(
                m, tuple(values[m, s] for s in WEIGHT_SYMBOLS)
            )
            for m in WEIGHT_MEMBERS
        ),
    )


def source_bound_training_weight_scales(
    shard: TrainingWeightCandidateShard,
    utc_date: str,
    *,
    approval: TrainingWeightExecutionApprovalV1 | None = None,
) -> TrainingWeightScaleSetV1:
    replay_training_weight_candidate_shard(shard, approval=approval)
    bridges = {b.artifact_id: b for b in shard.degradation.bridges}
    matches = [
        (d, bridges[d.bridge_id])
        for d in shard.days
        if bridges[d.bridge_id].utc_date == utc_date
    ]
    if len(matches) != 1:
        raise ValueError("scales require one fully admitted candidate day")
    return _scales(shard, *matches[0])


def _truth(
    shards: tuple[TrainingWeightCandidateShard, ...],
) -> dict[tuple[str, str], tuple[float, ...]]:
    """One source verification/pass per distinct month; never synthetic counts."""
    result: dict[tuple[str, str], tuple[float, ...]] = {}
    seen: set[str] = set()
    for shard in shards:
        source = shard.degradation.source_plan.parent_source
        if source.artifact_id in seen:
            continue
        seen.add(source.artifact_id)
        verified = verify_training_source(source)
        if _ownership(verified) != shard.degradation.parent_ownership:
            raise ValueError("original truth ownership changed")
        days = {
            b.utc_date
            for s in shards
            if s.degradation.source_plan.parent_source == source
            for b in s.degradation.bridges
        }
        by_start = {_day_ns(d): d for d in days}
        grouped: dict[tuple[str, str], list[_ObservedRow]] = {
            (d, s): [] for d in days for s in WEIGHT_SYMBOLS
        }
        for row in verified.observed:
            day_start = (
                row.event_time_ns // 86_400_000_000_000 * 86_400_000_000_000
            )
            date = by_start.get(day_start)
            if (
                date is not None
                and day_start + CORE_START_NS - MAX_AGE_NS
                <= row.event_time_ns
                <= day_start + CORE_END_NS
            ):
                target = grouped[date, row.symbol]
                if len(target) >= MAX_WORKFLOW_ROWS:
                    raise ValueError("truth support exceeds frozen row bound")
                target.append(row)
        for key, rows in grouped.items():
            probes = _probe_rows(tuple(rows), _day_ns(key[0]) + CORE_START_NS)
            result[key] = tuple(_mid(r.bid, r.ask) for r in probes)
    return result


def fit_training_weight_campaign(
    calibration_shards: Sequence[TrainingWeightCandidateShard],
    *,
    approval: TrainingWeightExecutionApprovalV1 | None = None,
) -> TrainingWeightFitV1:
    ordered, candidates, inventory = _campaign(
        calibration_shards, "calibration", approval
    )
    _gate(ordered[0].model, "calibration-fit", approval)
    truth = _truth(ordered)
    days: list[TrainingWeightCalibrationDayV1] = []
    for date, (shard, day, bridge) in sorted(candidates.items()):
        scales = _scales(shard, day, bridge)
        paths = _paths(day, bridge)
        errors = tuple(
            tuple(
                maximum_path_error(paths[m, s], truth[date, s])
                for s in WEIGHT_SYMBOLS
            )
            for m in WEIGHT_MEMBERS
        )
        days.append(
            TrainingWeightCalibrationDayV1(
                scales,
                errors,
                shard.degradation.source_plan.parent_source.artifact_id,
            )
        )
    calibration = fit_training_weight_calibration_math(days) if days else None
    return TrainingWeightFitV1(
        ordered[0].model.evidence_kind,
        ordered[0].model.artifact_id,
        tuple(_scope(s) for s in ordered),
        tuple(inventory.values()),
        calibration,
    )


def _counts(
    day: TrainingWeightCandidateDayV1, bridge: TrainingWeightDayBridgeV1
) -> dict[str, int]:
    start = _day_ns(bridge.utc_date) + CORE_START_NS
    end = _day_ns(bridge.utc_date) + CORE_END_NS
    counts = {
        m: sum(
            start <= e.event_time_ns < end
            for c in day.candidates
            if c.member_id == m
            for e in c.stream.events
        )
        for m in WEIGHT_MEMBERS
    }
    counts["observed"] = sum(
        start <= o.event_time_ns < end
        for s in bridge.symbols
        for o in s.ordinals
    )
    if any(not 0 < n <= MAX_WORKFLOW_ROWS for n in counts.values()):
        raise ValueError("complete core rows exceed frozen member bound")
    return counts


def _policy_allocation(
    policy: TrainingMemberPolicy,
    epoch: int,
    unit: str,
    counts: dict[str, int],
    support_counts: dict[str, int],
    radii: TrainingWeightRadiiV1 | None,
) -> TrainingWeightPolicyAllocationV1:
    if policy is TrainingMemberPolicy.UNCERTAINTY and (
        radii is None or radii.status is not WeightSupport.SUPPORTED
    ):
        return TrainingWeightPolicyAllocationV1(
            policy, epoch, unit, "confidence_unavailable", (), None
        )
    names = (
        ("observed",)
        if policy is TrainingMemberPolicy.OBSERVED
        else WEIGHT_MEMBERS
    )
    maximum = (
        radii.maximum_pip_radii
        if policy is TrainingMemberPolicy.UNCERTAINTY and radii is not None
        else None
    )
    allocation = allocate_unit_mass(
        policy, unit, names, training_epoch=epoch, maximum_pip_radii=maximum
    )
    members = tuple(
        TrainingWeightMemberMassV1(
            m.member_id,
            str(m.mass.numerator),
            str(m.mass.denominator),
            counts[m.member_id],
            support_counts[m.member_id],
        )
        for m in allocation.members
    )
    squared = sum(
        (m.mass * m.mass / counts[m.member_id] for m in allocation.members),
        Fraction(),
    )
    ess = _finite_view(allocation.total_mass**2 / squared)
    return TrainingWeightPolicyAllocationV1(
        policy,
        epoch,
        unit,
        (
            "failed_negative_control"
            if allocation.negative_control
            else "allocated"
        ),
        members,
        ess,
    )


def _allocate(
    ordered: tuple[TrainingWeightCandidateShard, ...],
    candidates: dict[
        str,
        tuple[
            TrainingWeightCandidateShard,
            TrainingWeightCandidateDayV1,
            TrainingWeightDayBridgeV1,
        ],
    ],
    inventory: dict[str, TrainingWeightInventoryDayV1],
    fit: TrainingWeightFitV1,
) -> TrainingWeightAllocationV1:
    if (
        ordered[0].model.artifact_id != fit.model_id
        or ordered[0].model.evidence_kind is not fit.evidence_kind
    ):
        raise ValueError(
            "application model/evidence differs from fitted calibration"
        )
    days: list[TrainingWeightAllocationDayV1] = []
    fit_dates = {d.utc_date for d in fit.inventory}
    for date, (shard, day, bridge) in sorted(candidates.items()):
        if date in fit_dates:
            raise ValueError("fit/application source intervals overlap")
        scales = _scales(shard, day, bridge)
        radii = (
            None
            if fit.calibration is None
            else apply_training_weight_calibration_math(fit.calibration, scales)
        )
        paths = _paths(day, bridge)
        features = tuple(
            tuple(
                _finite_view((Fraction(b) - Fraction(a)) / Fraction(WEIGHT_PIP))
                for s in WEIGHT_SYMBOLS
                for a, b in zip(paths[m, s], paths[m, s][1:])
            )
            for m in WEIGHT_MEMBERS
        )
        counts = _counts(day, bridge)
        support_counts = {
            m: sum(
                len(c.stream.events) for c in day.candidates if c.member_id == m
            )
            for m in WEIGHT_MEMBERS
        }
        support_counts["observed"] = sum(
            len(s.ordinals) for s in bridge.symbols
        )
        days.append(
            TrainingWeightAllocationDayV1(
                date,
                bridge.parent_unit.artifact_id,
                bridge.artifact_id,
                day.artifact_id,
                scales,
                radii,
                tuple(
                    _policy_allocation(
                        p,
                        e,
                        bridge.parent_unit.artifact_id,
                        counts,
                        support_counts,
                        radii,
                    )
                    for p, e in _POLICIES
                ),
                member_correlation_dimension(features),
            )
        )
    return TrainingWeightAllocationV1(
        fit,
        tuple(_scope(s) for s in ordered),
        tuple(inventory.values()),
        tuple(days),
    )


def allocate_training_weight_campaign(
    application_shards: Sequence[TrainingWeightCandidateShard],
    fit: TrainingWeightFitV1,
    calibration_shards: Sequence[TrainingWeightCandidateShard],
    *,
    approval: TrainingWeightExecutionApprovalV1 | None = None,
) -> TrainingWeightAllocationV1:
    """Replay fit/application evidence; never compute application truth errors."""
    if (
        fit_training_weight_campaign(calibration_shards, approval=approval)
        != fit
    ):
        raise ValueError("fit differs from fresh source-bound calibration")
    ordered, candidates, inventory = _campaign(
        application_shards, "application", approval
    )
    return _allocate(ordered, candidates, inventory, fit)


def write_training_weight_allocation(
    allocation: TrainingWeightAllocationV1,
    directory: str | Path,
    application_shards: Sequence[TrainingWeightCandidateShard],
    calibration_shards: Sequence[TrainingWeightCandidateShard],
    *,
    approval: TrainingWeightExecutionApprovalV1 | None = None,
) -> Path:
    """Fresh replay then atomic no-clobber publication, before outcome access."""
    expected = allocate_training_weight_campaign(
        application_shards,
        allocation.fit,
        calibration_shards,
        approval=approval,
    )
    if expected != allocation:
        raise ValueError(
            "allocation differs from source/model/calibration replay"
        )
    root = Path(directory)
    if root.is_symlink():
        raise ValueError("allocation directory cannot be a symlink")
    existed = root.exists()
    root.mkdir(parents=True, exist_ok=True)
    if not root.is_dir() or root.is_symlink():
        raise ValueError("allocation directory must be a real directory")
    if not existed:
        _sync(root.parent)
    payload = allocation.to_json().encode("ascii")
    path = root / (
        "weight-allocation-" + hashlib.sha256(payload).hexdigest() + ".json"
    )
    _publish(path, payload)
    return path


def _read_allocation(path: str | Path) -> TrainingWeightAllocationV1:
    source = Path(path)
    if source.parent.is_symlink():
        raise ValueError("allocation directory cannot be a symlink")
    payload = read_training_regular(source, MAX_TRAINING_BYTES)
    if (
        source.name
        != "weight-allocation-" + hashlib.sha256(payload).hexdigest() + ".json"
    ):
        raise ValueError("allocation filename differs from canonical bytes")
    allocation = TrainingWeightAllocationV1.from_json(payload.decode("ascii"))
    if allocation.to_json().encode("ascii") != payload:
        raise ValueError("allocation bytes are not canonical")
    return allocation


def read_training_weight_allocation(
    path: str | Path,
    application_shards: Sequence[TrainingWeightCandidateShard],
    calibration_shards: Sequence[TrainingWeightCandidateShard],
    *,
    approval: TrainingWeightExecutionApprovalV1 | None = None,
) -> TrainingWeightAllocationV1:
    allocation = _read_allocation(path)
    expected = allocate_training_weight_campaign(
        application_shards,
        allocation.fit,
        calibration_shards,
        approval=approval,
    )
    if allocation != expected or _read_allocation(path) != allocation:
        raise ValueError("persisted allocation differs from full source replay")
    return allocation


@dataclass(frozen=True, slots=True)
class TrainingWeightedRowV1(TrainingContract):
    KIND: ClassVar[str] = "weight-training-row"
    member_id: str
    row: TrainingRowV1
    mass_numerator: str
    mass_denominator: str

    def _validate(self) -> None:
        _expanded_nodes(self.to_dict())
        if self.member_id not in (*WEIGHT_MEMBERS, "observed"):
            raise ValueError("weighted row member is outside frozen scope")
        _fraction(self.mass_numerator, self.mass_denominator)
        if (
            self.row.information_mode is not TrainingInformationMode.EX_POST
            or self.row.available_at_ns is not None
            or self.row.origin
            not in (
                TrainingOrigin.OBSERVED,
                TrainingOrigin.SYNTHETIC_RECONSTRUCTION,
            )
        ):
            raise ValueError(
                "research weighting cannot admit causal or other origins"
            )

    @property
    def mass(self) -> Fraction:
        return _fraction(self.mass_numerator, self.mass_denominator)


@dataclass(frozen=True, slots=True)
class TrainingWeightedRowsV1(TrainingContract):
    KIND: ClassVar[str] = "weight-training-rows"
    evidence_kind: WeightEvidenceKind
    allocation_id: str
    policy: TrainingWeightPolicyAllocationV1
    rows: tuple[TrainingWeightedRowV1, ...]

    def _validate(self) -> None:
        _expanded_nodes(self.to_dict())
        _label(self.allocation_id)
        if len(self.rows) > MAX_WORKFLOW_ROWS or len(
            {r.row.source_row_key for r in self.rows}
        ) != len(self.rows):
            raise ValueError(
                "weighted row selection duplicates or exceeds support"
            )
        members = {m.member_id: m for m in self.policy.members}
        counts = {m: 0 for m in members}
        for weighted in self.rows:
            member = members.get(weighted.member_id)
            if (
                member is None
                or weighted.row.evidence_unit_id
                != self.policy.historical_unit_id
                or weighted.mass != member.mass / member.complete_core_rows
            ):
                raise ValueError(
                    "weighted row differs from original complete allocation"
                )
            counts[weighted.member_id] += 1
            if counts[weighted.member_id] > member.complete_core_rows:
                raise ValueError(
                    "weighted rows exceed original complete member"
                )


@dataclass(frozen=True, slots=True)
class TrainingWeightRowRequestV1(TrainingContract):
    KIND: ClassVar[str] = "weight-row-request"
    utc_date: str
    policy: TrainingMemberPolicy
    training_epoch: int = 0
    members: tuple[str, ...] | None = None
    row_keys: tuple[str, ...] | None = None
    consumer_mode: TrainingConsumerMode = TrainingConsumerMode.RECONSTRUCTION

    def _validate(self) -> None:
        if (
            self.utc_date
            not in read_training_weight_preregistration().scheduled_dates(
                "application"
            )
        ):
            raise ValueError("row date is outside frozen application schedule")
        if (
            self.policy,
            self.training_epoch,
        ) not in _POLICIES or self.consumer_mode not in admitted_modes(
            TrainingOrigin.SYNTHETIC_RECONSTRUCTION,
            TrainingInformationMode.EX_POST,
        ):
            raise ValueError(
                "consumer or policy is not admitted for ex-post research"
            )
        for values, limit in (
            (self.members, 4),
            (self.row_keys, MAX_WORKFLOW_ROWS),
        ):
            if values is not None and (
                len(values) > limit
                or any(len(v) > 1024 for v in values)
                or len(set(values)) != len(values)
            ):
                raise ValueError(
                    "weighted row filter is duplicated or exceeds bound"
                )


def iter_weighted_training_weight_rows(
    allocation_path: str | Path,
    application_shards: Sequence[TrainingWeightCandidateShard],
    calibration_shards: Sequence[TrainingWeightCandidateShard],
    requests: Sequence[TrainingWeightRowRequestV1],
    *,
    approval: TrainingWeightExecutionApprovalV1 | None = None,
) -> Iterator[TrainingWeightedRowsV1]:
    """One closed replay, then bounded batches; no persistent verified cache.

    The iterator captures immutable, freshly replayed evidence for this export.
    Each output remains independently bounded to 4096 rows / 8 MiB. The caller
    may stream batches without retaining a campaign-sized in-memory row table.
    Reopening the public operation repeats all source verification.
    """
    if not 0 < len(requests) <= 1024:
        raise ValueError("row export request count exceeds fixed work bound")
    selected = tuple(requests[i] for i in range(len(requests)))
    if any(type(r) is not TrainingWeightRowRequestV1 for r in selected):
        raise ValueError("row export requires typed bounded requests")
    if sum(len(r.to_json()) for r in selected) > MAX_TRAINING_BYTES:
        raise ValueError("row export request envelope exceeds byte bound")
    # Retain a fixed immutable snapshot of inputs rather than a caller-mutable
    # sequence that could be replaced between generator yields.
    if len(application_shards) != 3:
        raise ValueError("row export requires the complete application scope")
    applications = tuple(application_shards[i] for i in range(3))
    allocation = read_training_weight_allocation(
        allocation_path, applications, calibration_shards, approval=approval
    )
    for request in selected:
        try:
            batch = _weighted_rows(allocation, applications, request)
        except _RowBudgetExceeded:
            pass
        else:
            yield batch
            continue
        pending = [_selected_row_keys(allocation, applications, request)]
        while pending:
            keys = pending.pop()
            if len(keys) > MAX_WORKFLOW_ROWS:
                middle = len(keys) // 2
                pending.extend((keys[middle:], keys[:middle]))
                continue
            try:
                batch = _weighted_rows(
                    allocation, applications, replace(request, row_keys=keys)
                )
            except _RowBudgetExceeded:
                if len(keys) < 2:
                    raise
                middle = len(keys) // 2
                pending.extend((keys[middle:], keys[:middle]))
            else:
                yield batch


def _selected_row_keys(
    allocation: TrainingWeightAllocationV1,
    applications: tuple[TrainingWeightCandidateShard, ...],
    request: TrainingWeightRowRequestV1,
) -> tuple[str, ...]:
    allocated = next(
        d for d in allocation.days if d.utc_date == request.utc_date
    )
    shard, day = next(
        (s, d)
        for s in applications
        for d in s.days
        if d.artifact_id == allocated.candidate_day_id
    )
    bridge = next(
        b for b in shard.degradation.bridges if b.artifact_id == day.bridge_id
    )
    start = _day_ns(request.utc_date) + CORE_START_NS
    end = _day_ns(request.utc_date) + CORE_END_NS
    members = request.members
    keys: list[str] = []
    if request.policy is TrainingMemberPolicy.OBSERVED:
        if members is None or "observed" in members:
            keys = [
                f"{s.subset_series_id}:{s.period}:{o.subset_row_id}"
                for s in bridge.symbols
                for o in s.ordinals
                if start <= o.event_time_ns < end
            ]
    else:
        keys = [
            c.member_id + ":" + e.event_id
            for c in day.candidates
            if members is None or c.member_id in members
            for e in c.stream.events
            if start <= e.event_time_ns < end
        ]
    if request.row_keys is not None:
        requested = set(request.row_keys)
        if not requested <= set(keys):
            raise ValueError(
                "requested row key is outside selected complete core"
            )
        keys = [k for k in keys if k in requested]
    return tuple(keys)


def weighted_training_weight_rows(
    allocation_path: str | Path,
    application_shards: Sequence[TrainingWeightCandidateShard],
    calibration_shards: Sequence[TrainingWeightCandidateShard],
    utc_date: str,
    policy: TrainingMemberPolicy,
    *,
    training_epoch: int = 0,
    members: tuple[str, ...] | None = None,
    row_keys: tuple[str, ...] | None = None,
    consumer_mode: TrainingConsumerMode = TrainingConsumerMode.RECONSTRUCTION,
    approval: TrainingWeightExecutionApprovalV1 | None = None,
) -> TrainingWeightedRowsV1:
    request = TrainingWeightRowRequestV1(
        utc_date, policy, training_epoch, members, row_keys, consumer_mode
    )
    allocation = read_training_weight_allocation(
        allocation_path,
        application_shards,
        calibration_shards,
        approval=approval,
    )
    return _weighted_rows(allocation, tuple(application_shards), request)


def _weighted_rows(
    allocation: TrainingWeightAllocationV1,
    application_shards: tuple[TrainingWeightCandidateShard, ...],
    request: TrainingWeightRowRequestV1,
) -> TrainingWeightedRowsV1:
    """Research-native #606 rows; filtering retains mass, never renormalizes.

    Halo anchors support reconstruction/probes but receive no core row mass.
    Native event JSON is unchanged. Its subset source identity coexists with
    original-parent ownership; neither a legacy product nor historical clock
    is invented. Raw volume is absent from both observed and native features.
    """
    utc_date, policy, training_epoch = (
        request.utc_date,
        request.policy,
        request.training_epoch,
    )
    members, row_keys = request.members, request.row_keys
    matches = [d for d in allocation.days if d.utc_date == utc_date]
    if len(matches) != 1:
        raise ValueError("row selection requires one fully admitted day")
    allocated_day = matches[0]
    selected_policy = next(
        p
        for p in allocated_day.policies
        if (p.policy, p.training_epoch) == (policy, training_epoch)
    )
    if not selected_policy.members:
        raise ValueError("unsupported confidence policy has no training rows")
    choices = {m.member_id: m for m in selected_policy.members}
    selected = tuple(choices) if members is None else members
    if not set(selected) <= set(choices):
        raise ValueError("selected member is outside complete allocation")
    found = [
        (s, d)
        for s in application_shards
        for d in s.days
        if d.artifact_id == allocated_day.candidate_day_id
    ]
    if len(found) != 1:
        raise ValueError("allocation source day is missing or duplicated")
    shard, day = found[0]
    bridge = next(
        b for b in shard.degradation.bridges if b.artifact_id == day.bridge_id
    )
    start = _day_ns(utc_date) + CORE_START_NS
    end = _day_ns(utc_date) + CORE_END_NS
    decision = max(
        end,
        max(o.event_time_ns + 1 for s in bridge.symbols for o in s.ordinals),
    )
    # Preflight selected complete counts before building any output row.
    maximum = sum(choices[m].complete_core_rows for m in selected)
    if row_keys is None and maximum > MAX_WORKFLOW_ROWS:
        raise _RowBudgetExceeded(
            "use streaming export for a bounded member/row slice"
        )
    root = TrainingRootV1(
        "research-native-generated-replay",
        day.artifact_id,
        hashlib.sha256(day.to_json().encode("ascii")).hexdigest(),
        TrainingVerificationLevel.DERIVED_REPLAY,
    )
    roots = tuple(
        sorted(
            (*shard.degradation.parent_ownership.dependency_roots, root),
            key=lambda r: r.artifact_id,
        )
    )
    empty = TrainingWeightedRowsV1(
        allocation.fit.evidence_kind,
        allocation.artifact_id,
        selected_policy,
        (),
    )
    retained = len(empty.to_json())
    nodes = _expanded_nodes(empty.to_dict())
    result: list[TrainingWeightedRowV1] = []
    available_keys: set[str] = set()
    requested = None if row_keys is None else set(row_keys)

    def append(
        member: str,
        key: str,
        time: int,
        symbol: str,
        value: dict[str, object],
        origin: TrainingOrigin,
        run: str | None,
        native_member: str | None,
    ) -> None:
        nonlocal retained, nodes
        available_keys.add(key)
        if requested is not None and key not in requested:
            return
        mass = choices[member].mass / choices[member].complete_core_rows
        row = TrainingRowV1(
            bridge.parent_unit.artifact_id,
            shard.degradation.parent_ownership.artifact_id,
            origin,
            TrainingInformationMode.EX_POST,
            time,
            decision,
            None,
            symbol,
            bridge.source_plan.parent_source.dataset_version_id,
            key,
            run,
            native_member,
            (),
            training_json(
                {
                    "confidence": None,
                    "status": "research_radius_is_separate_not_native_event_confidence",
                    "allocation_id": allocation.artifact_id,
                }
            ),
            "histdatacom.training-weight-research-native.v1",
            NO_LABEL_SCHEMA,
            training_json(value),
            roots,
            admitted_modes(origin, TrainingInformationMode.EX_POST),
        )
        weighted = TrainingWeightedRowV1(
            member, row, str(mass.numerator), str(mass.denominator)
        )
        retained += len(weighted.to_json()) + 1
        nodes += _expanded_nodes(weighted.to_dict())
        if (
            len(result) >= MAX_WORKFLOW_ROWS
            or retained > MAX_TRAINING_BYTES - 1024
            or nodes > MAX_WORKFLOW_NODES
        ):
            raise _RowBudgetExceeded(
                "weighted training output exceeds aggregate budget; use streaming export"
            )
        result.append(weighted)

    if policy is TrainingMemberPolicy.OBSERVED:
        if "observed" in selected:
            for symbol in bridge.symbols:
                for ordinal in symbol.ordinals:
                    if start <= ordinal.event_time_ns < end:
                        key = f"{symbol.subset_series_id}:{symbol.period}:{ordinal.subset_row_id}"
                        value: dict[str, object] = {
                            "symbol": symbol.symbol,
                            "event_time_ns": ordinal.event_time_ns,
                            "bid": ordinal.bid,
                            "ask": ordinal.ask,
                            "source_version_id": bridge.subset_source.dataset_version_id,
                            "source_series_id": symbol.subset_series_id,
                            "source_period": symbol.period,
                            "source_row_id": ordinal.subset_row_id,
                            "parent_source_series_id": symbol.parent_series_id,
                            "parent_source_row_id": ordinal.parent_row_id,
                        }
                        append(
                            "observed",
                            key,
                            ordinal.event_time_ns,
                            symbol.symbol,
                            value,
                            TrainingOrigin.OBSERVED,
                            None,
                            None,
                        )
    else:
        for candidate in day.candidates:
            if candidate.member_id not in selected:
                continue
            for event in candidate.stream.events:
                if start <= event.event_time_ns < end:
                    origin = (
                        TrainingOrigin.OBSERVED
                        if event.origin is SyntheticEventOrigin.OBSERVED
                        else TrainingOrigin.SYNTHETIC_RECONSTRUCTION
                    )
                    append(
                        candidate.member_id,
                        candidate.member_id + ":" + event.event_id,
                        event.event_time_ns,
                        candidate.symbol,
                        dict(event.to_dict()),
                        origin,
                        event.run_id,
                        event.ensemble_member_id,
                    )
    if requested is not None and not requested <= available_keys:
        raise ValueError("requested row key is outside selected complete core")
    return TrainingWeightedRowsV1(
        allocation.fit.evidence_kind,
        allocation.artifact_id,
        selected_policy,
        tuple(result),
    )


@dataclass(frozen=True, slots=True)
class TrainingWeightOutcomeDayV1(TrainingContract):
    KIND: ClassVar[str] = "weight-outcome-day"
    utc_date: str
    historical_unit_id: str
    allocation_day_id: str
    truth_snapshot_id: str
    member_losses: tuple[float, ...]
    observed_loss: float
    policy_losses: tuple[float | None, ...]
    diagnostic_radius_covered: bool | None
    admitted_radius_covered: bool | None

    def _validate(self) -> None:
        _day_ns(self.utc_date)
        for value in (
            self.historical_unit_id,
            self.allocation_day_id,
            self.truth_snapshot_id,
        ):
            _label(value)
        if len(self.member_losses) != 4 or len(self.policy_losses) != len(
            _POLICIES
        ):
            raise ValueError("outcome loss support differs from frozen scope")
        for loss in (
            *self.member_losses,
            self.observed_loss,
            *self.policy_losses,
        ):
            if loss is not None and (not math.isfinite(loss) or loss < 0):
                raise ValueError(
                    "outcome losses must be finite and nonnegative"
                )
        if (
            self.admitted_radius_covered is not None
            and self.admitted_radius_covered != self.diagnostic_radius_covered
        ):
            raise ValueError(
                "admitted coverage must preserve pre-abstention result"
            )


@dataclass(frozen=True, slots=True)
class TrainingWeightCoverageV1(TrainingContract):
    KIND: ClassVar[str] = "weight-coverage"
    evaluated_unit_ids: tuple[str, ...]
    covered_unit_ids: tuple[str, ...]
    unavailable_unit_ids: tuple[str, ...]
    refused_or_missing_dates: tuple[str, ...]

    def _validate(self) -> None:
        for values in (
            self.evaluated_unit_ids,
            self.covered_unit_ids,
            self.unavailable_unit_ids,
            self.refused_or_missing_dates,
        ):
            if values != tuple(sorted(set(values))):
                raise ValueError(
                    "coverage support must be exact unique ordered identities"
                )
        if not set(self.covered_unit_ids) <= set(
            self.evaluated_unit_ids
        ) or set(self.evaluated_unit_ids) & set(self.unavailable_unit_ids):
            raise ValueError(
                "coverage support overlaps or invents a covered unit"
            )

    @property
    def numerator(self) -> int:
        return len(self.covered_unit_ids)

    @property
    def denominator(self) -> int:
        return len(self.evaluated_unit_ids)


def _coverage(
    days: tuple[TrainingWeightOutcomeDayV1, ...],
    inventory: tuple[TrainingWeightInventoryDayV1, ...],
    *,
    admitted: bool,
) -> TrainingWeightCoverageV1:
    values = {
        d.historical_unit_id: (
            d.admitted_radius_covered
            if admitted
            else d.diagnostic_radius_covered
        )
        for d in days
    }
    return TrainingWeightCoverageV1(
        tuple(sorted(k for k, v in values.items() if v is not None)),
        tuple(sorted(k for k, v in values.items() if v is True)),
        tuple(sorted(k for k, v in values.items() if v is None)),
        tuple(d.utc_date for d in inventory if d.status != "admitted"),
    )


def _comparisons(
    days: tuple[TrainingWeightOutcomeDayV1, ...],
) -> tuple[TrainingPairedInferenceV1, ...]:
    coordinates = read_training_weight_preregistration().holm_coordinates()
    by_key = {
        _policy_key(p, e) + "-minus-equal-unit-mass.v1": i
        for i, (p, e) in enumerate(_POLICIES)
    }
    result = []
    for coordinate in coordinates:
        index = by_key[coordinate]
        units = []
        for day in days:
            loss, equal = day.policy_losses[index], day.policy_losses[1]
            if loss is not None and equal is not None:
                units.append(
                    TrainingPairedUnitLossV1(
                        day.historical_unit_id, coordinate, loss, equal
                    )
                )
        result.append(compare_historical_unit_losses(coordinate, units))
    return tuple(result)


@dataclass(frozen=True, slots=True)
class TrainingWeightPolicyReportV1(TrainingContract):
    KIND: ClassVar[str] = "weight-policy-report"
    policy_key: str
    status: str
    historical_unit_ids: tuple[str, ...]
    unavailable_unit_ids: tuple[str, ...]
    complete_native_support_rows: int
    complete_core_rows: int
    positive_weight_core_rows: int
    member_unit_count: int
    positive_weight_member_unit_count: int
    total_mass_numerator: str
    total_mass_denominator: str
    aggregate_row_kish_ess: float | None

    def _validate(self) -> None:
        if self.policy_key not in tuple(
            _policy_key(p, e) for p, e in _POLICIES
        ):
            raise ValueError("reported policy is outside frozen coordinates")
        if (
            self.historical_unit_ids
            != tuple(sorted(set(self.historical_unit_ids)))
            or len(self.historical_unit_ids) > 20
        ):
            raise ValueError(
                "policy report must count original unique day units"
            )
        if (
            self.unavailable_unit_ids
            != tuple(sorted(set(self.unavailable_unit_ids)))
            or set(self.historical_unit_ids) & set(self.unavailable_unit_ids)
            or len(self.historical_unit_ids) + len(self.unavailable_unit_ids)
            > 20
        ):
            raise ValueError(
                "policy unavailable units overlap or exceed fixed scope"
            )
        if (
            not 0
            <= self.positive_weight_core_rows
            <= self.complete_core_rows
            <= self.complete_native_support_rows
            <= 20 * 4 * MAX_WORKFLOW_ROWS
        ):
            raise ValueError("reported row counts exceed complete native scope")
        if (
            not 0
            <= self.positive_weight_member_unit_count
            <= self.member_unit_count
            <= 80
        ):
            raise ValueError("reported member-unit counts exceed scope")
        _fraction(self.total_mass_numerator, self.total_mass_denominator)
        if (
            self.aggregate_row_kish_ess is not None
            and not 0
            < self.aggregate_row_kish_ess
            <= self.positive_weight_core_rows
        ):
            raise ValueError("row Kish must be a concentration diagnostic")
        expected = (
            "failed_negative_control_not_default"
            if self.policy_key == TrainingMemberPolicy.NAIVE.value
            else "descriptive_weight_concentration_not_independent_history"
        )
        if self.status != expected:
            raise ValueError("policy report scientific status differs")

    @property
    def historical_unit_count(self) -> int:
        return len(self.historical_unit_ids)


def _policy_reports(
    allocation: TrainingWeightAllocationV1,
) -> tuple[TrainingWeightPolicyReportV1, ...]:
    result = []
    for index, (policy, epoch) in enumerate(_POLICIES):
        active = tuple(
            d.policies[index]
            for d in allocation.days
            if d.policies[index].members
        )
        members = tuple(m for p in active for m in p.members)
        mass = sum((m.mass for m in members), Fraction())
        squared = sum(
            (m.mass * m.mass / m.complete_core_rows for m in members),
            Fraction(),
        )
        result.append(
            TrainingWeightPolicyReportV1(
                _policy_key(policy, epoch),
                (
                    "failed_negative_control_not_default"
                    if policy is TrainingMemberPolicy.NAIVE
                    else "descriptive_weight_concentration_not_independent_history"
                ),
                tuple(sorted(p.historical_unit_id for p in active)),
                tuple(
                    sorted(
                        d.historical_unit_id
                        for d in allocation.days
                        if not d.policies[index].members
                    )
                ),
                sum(m.complete_support_rows for m in members),
                sum(m.complete_core_rows for m in members),
                sum(m.complete_core_rows for m in members if m.mass > 0),
                len(members),
                sum(m.mass > 0 for m in members),
                str(mass.numerator),
                str(mass.denominator),
                _finite_view(mass * mass / squared) if squared else None,
            )
        )
    return tuple(result)


@dataclass(frozen=True, slots=True)
class TrainingWeightDimensionV1(TrainingContract):
    KIND: ClassVar[str] = "weight-member-dimension"
    historical_unit_id: str
    dimension: float | None

    def _validate(self) -> None:
        _label(self.historical_unit_id)
        if self.dimension is not None and not 1 <= self.dimension <= 4:
            raise ValueError("member feature dimension exceeds scope")


@dataclass(frozen=True, slots=True)
class TrainingWeightEvaluationV1(TrainingContract):
    KIND: ClassVar[str] = "weight-campaign-evaluation"
    allocation: TrainingWeightAllocationV1
    outcomes: tuple[TrainingWeightOutcomeDayV1, ...]
    comparisons: tuple[TrainingPairedInferenceV1, ...]
    holm_pvalues: tuple[float | None, ...]
    pre_abstention_coverage: TrainingWeightCoverageV1
    admitted_coverage: TrainingWeightCoverageV1
    policy_reports: tuple[TrainingWeightPolicyReportV1, ...]
    member_feature_dimensions: tuple[TrainingWeightDimensionV1, ...]

    def _validate(self) -> None:
        if tuple(
            (d.utc_date, d.historical_unit_id, d.allocation_day_id)
            for d in self.outcomes
        ) != tuple(
            (d.utc_date, d.historical_unit_id, d.artifact_id)
            for d in self.allocation.days
        ):
            raise ValueError("evaluation cannot drop or add admitted outcomes")
        for day, outcome in zip(self.allocation.days, self.outcomes):
            losses = dict(zip(WEIGHT_MEMBERS, outcome.member_losses))
            expected_losses = tuple(
                (
                    None
                    if not p.members
                    else weighted_member_loss_math(
                        p.allocation,
                        (
                            {"observed": outcome.observed_loss}
                            if p.policy is TrainingMemberPolicy.OBSERVED
                            else losses
                        ),
                    )
                )
                for p in day.policies
            )
            if outcome.policy_losses != expected_losses:
                raise ValueError(
                    "policy contributions differ from retained member losses"
                )
            has_diagnostic = day.radii is not None and bool(
                day.radii.diagnostic_symbol_radii
            )
            supported = (
                day.radii is not None
                and day.radii.status is WeightSupport.SUPPORTED
            )
            if (
                outcome.diagnostic_radius_covered is not None
            ) != has_diagnostic or (
                outcome.admitted_radius_covered is not None
            ) != supported:
                raise ValueError(
                    "coverage availability differs from pre-outcome radii"
                )
        expected = _comparisons(self.outcomes)
        adjusted = fixed_family_holm(
            {c.coordinate: c.sign_pvalue for c in expected}
        )
        if self.comparisons != expected or self.holm_pvalues != tuple(
            adjusted.values()
        ):
            raise ValueError(
                "campaign inference differs from exact retained losses"
            )
        if self.pre_abstention_coverage != _coverage(
            self.outcomes, self.allocation.inventory, admitted=False
        ) or self.admitted_coverage != _coverage(
            self.outcomes, self.allocation.inventory, admitted=True
        ):
            raise ValueError("coverage differs from complete outcome inventory")
        if self.policy_reports != _policy_reports(
            self.allocation
        ) or self.member_feature_dimensions != tuple(
            TrainingWeightDimensionV1(
                d.historical_unit_id, d.member_feature_dimension
            )
            for d in self.allocation.days
        ):
            raise ValueError(
                "report counts/masses/dimensions differ from complete allocations"
            )


def _point_loss(path: tuple[float, ...], truth: tuple[float, ...]) -> Fraction:
    if len(path) != WEIGHT_PROBES or len(truth) != WEIGHT_PROBES:
        raise ValueError("point loss must cover exact fixed probes")
    return sum(
        (
            abs(Fraction(a) - Fraction(b)) / Fraction(WEIGHT_PIP)
            for a, b in zip(path, truth)
        ),
        Fraction(),
    )


def evaluate_training_weight_campaign(
    allocation_path: str | Path,
    application_shards: Sequence[TrainingWeightCandidateShard],
    calibration_shards: Sequence[TrainingWeightCandidateShard],
    *,
    approval: TrainingWeightExecutionApprovalV1 | None = None,
) -> TrainingWeightEvaluationV1:
    """Exact persisted allocations are verified BEFORE application truth errors.

    Full source/model replay still occurs; there is no opaque-ID authorization.
    Historical outcomes are evaluation-only and cannot alter fitted scales,
    admission, allocations or member selection. This computes no winner.
    """
    allocation = _read_allocation(allocation_path)
    fit = fit_training_weight_campaign(calibration_shards, approval=approval)
    if fit != allocation.fit:
        raise ValueError("persisted fit differs from source replay")
    ordered, candidates, inventory = _campaign(
        application_shards, "application", approval
    )
    if (
        _allocate(ordered, candidates, inventory, fit) != allocation
        or _read_allocation(allocation_path) != allocation
    ):
        raise ValueError(
            "persisted allocation differs before outcome inspection"
        )
    _gate(ordered[0].model, "application-outcome-inspection", approval)
    _gate(ordered[0].model, "policy-comparison", approval)
    truth = _truth(ordered)
    outcomes: list[TrainingWeightOutcomeDayV1] = []
    for allocated in allocation.days:
        date = allocated.utc_date
        shard, day, bridge = candidates[date]
        paths = _paths(day, bridge)
        losses = {
            m: _report_view(
                sum(
                    (
                        _point_loss(paths[m, s], truth[date, s])
                        for s in WEIGHT_SYMBOLS
                    ),
                    Fraction(),
                )
                / (WEIGHT_PROBES * 3)
            )
            for m in WEIGHT_MEMBERS
        }
        start = _day_ns(date) + CORE_START_NS
        observed = {
            c.symbol: _native_probes(
                tuple(
                    e
                    for e in c.stream.events
                    if e.origin is SyntheticEventOrigin.OBSERVED
                ),
                start,
            )[0]
            for c in day.candidates
            if c.member_id == WEIGHT_MEMBERS[0]
        }
        observed_loss = _report_view(
            sum(
                (
                    _point_loss(observed[s], truth[date, s])
                    for s in WEIGHT_SYMBOLS
                ),
                Fraction(),
            )
            / (WEIGHT_PROBES * 3)
        )
        policy_losses = tuple(
            (
                None
                if not p.members
                else weighted_member_loss_math(
                    p.allocation,
                    (
                        {"observed": observed_loss}
                        if p.policy is TrainingMemberPolicy.OBSERVED
                        else losses
                    ),
                )
            )
            for p in allocated.policies
        )
        diagnostic: bool | None = None
        admitted: bool | None = None
        radii = allocated.radii
        if radii is not None and radii.diagnostic_symbol_radii:
            diagnostic = all(
                maximum_path_error(paths[m, s], truth[date, s]) <= radius
                for m, row in zip(WEIGHT_MEMBERS, radii.diagnostic_symbol_radii)
                for s, radius in zip(WEIGHT_SYMBOLS, row)
            )
            if radii.status is WeightSupport.SUPPORTED:
                admitted = diagnostic
        outcomes.append(
            TrainingWeightOutcomeDayV1(
                date,
                allocated.historical_unit_id,
                allocated.artifact_id,
                shard.degradation.source_plan.parent_source.artifact_id,
                tuple(losses[m] for m in WEIGHT_MEMBERS),
                observed_loss,
                policy_losses,
                diagnostic,
                admitted,
            )
        )
    retained = tuple(outcomes)
    comparisons = _comparisons(retained)
    holm = fixed_family_holm({c.coordinate: c.sign_pvalue for c in comparisons})
    return TrainingWeightEvaluationV1(
        allocation,
        retained,
        comparisons,
        tuple(holm.values()),
        _coverage(retained, allocation.inventory, admitted=False),
        _coverage(retained, allocation.inventory, admitted=True),
        _policy_reports(allocation),
        tuple(
            TrainingWeightDimensionV1(
                d.historical_unit_id, d.member_feature_dimension
            )
            for d in allocation.days
        ),
    )


def replay_training_weight_evaluation(
    evaluation: TrainingWeightEvaluationV1,
    allocation_path: str | Path,
    application_shards: Sequence[TrainingWeightCandidateShard],
    calibration_shards: Sequence[TrainingWeightCandidateShard],
    *,
    approval: TrainingWeightExecutionApprovalV1 | None = None,
) -> TrainingWeightEvaluationV1:
    if (
        evaluate_training_weight_campaign(
            allocation_path,
            application_shards,
            calibration_shards,
            approval=approval,
        )
        != evaluation
    ):
        raise ValueError(
            "evaluation differs from original source/model/outcome replay"
        )
    return evaluation
