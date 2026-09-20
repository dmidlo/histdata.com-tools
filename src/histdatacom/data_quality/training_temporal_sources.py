"""Clean source/value replay for temporal training, without clock promotion.

The only sources are the existing #606 full catalog/product inventory and #561
vintage matrices declared in that inventory. Normalized availability is checked
against retained records; it is never called official historical attestation.
"""

from __future__ import annotations

import bisect
import hashlib
import math
import statistics
from dataclasses import dataclass, field, replace

from histdatacom.forecasting.feature_contracts import FeatureKind
from histdatacom.forecasting.feature_store import (
    FeatureMatrixSnapshotV1,
    VintageFeatureStoreV1,
)
from histdatacom.synthetic.contracts import SyntheticEventV1

from .training_contracts import TrainingEvidenceUnitV1, training_json
from .training_lineage import (
    _ObservedRow,
    _VerifiedSource,
    verify_training_ownership,
)
from .training_temporal_contracts import (
    TemporalFeatureKind,
    TemporalInformationMode,
    TemporalStatePolicy,
    TemporalTargetKind,
    TrainingTemporalExampleV1,
    TrainingTemporalFeatureV1,
    TrainingTemporalPlanV1,
    TrainingTemporalValueV1,
)


@dataclass(frozen=True, slots=True)
class _TemporalPoint:
    """Process-local result of actual source replay, never a public receipt."""

    time_ns: int
    value: float
    record_id: str
    available_at_ns: int | None
    dependency_start_ns: int
    dependency_end_ns: int


@dataclass(frozen=True, slots=True)
class _TemporalVerifiedSource:
    """Process-local only; public boundaries always rebuild this value."""

    plan: TrainingTemporalPlanV1
    legacy: _VerifiedSource
    matrices: dict[str, FeatureMatrixSnapshotV1]
    quote_cache: dict[
        tuple[str, str | None, str | None], tuple[_TemporalPoint, ...]
    ] = field(default_factory=dict)

    def unit(self, clock: int) -> TrainingEvidenceUnitV1:
        units = self.plan.ownership.units
        index = (
            bisect.bisect_right([unit.start_ns for unit in units], clock) - 1
        )
        if index < 0 or clock >= units[index].end_ns:
            raise ValueError(
                "temporal dependency is outside complete ownership"
            )
        return units[index]

    def matrix(self, identity: str | None) -> FeatureMatrixSnapshotV1:
        if identity is None or identity not in self.matrices:
            raise ValueError(
                "matrix is not in replayed complete source inventory"
            )
        return self.matrices[identity]


def verify_training_temporal_source(
    plan: TrainingTemporalPlanV1,
) -> _TemporalVerifiedSource:
    """Verify full catalog/ownership before any row, member or feature selection."""
    if type(plan) is not TrainingTemporalPlanV1:
        raise TypeError("temporal source verification requires a complete plan")
    legacy = verify_training_ownership(plan.source, plan.ownership)
    matrices = {
        feature.artifact.snapshot_id: feature.artifact
        for feature in legacy.features
        if isinstance(feature.artifact, FeatureMatrixSnapshotV1)
    }
    result = _TemporalVerifiedSource(plan, legacy, matrices)
    for example in plan.examples:
        if example.symbol not in legacy.graph_symbols:
            raise ValueError("temporal symbol is outside the verified graph")
        if (
            result.unit(example.decision_time_ns).artifact_id
            != example.evidence_unit_id
        ):
            raise ValueError("decision has a different actual historical owner")
        result.unit(example.decision_time_ns + example.label.horizon_ns)
    return result


def _midpoint(bid: float, ask: float) -> float:
    if not (0 < bid <= ask and math.isfinite(bid) and math.isfinite(ask)):
        raise ValueError(
            "temporal prices require finite positive uncrossed quotes"
        )
    return bid + (ask - bid) / 2


def _quote_points(
    source: _TemporalVerifiedSource, example: TrainingTemporalExampleV1
) -> tuple[_TemporalPoint, ...]:
    """Last physical ordinal/event sequence owns a duplicate clock, explicitly."""
    if source.plan.information_mode is TemporalInformationMode.NORMALIZED_AS_OF:
        raise ValueError(
            "unknown historical quote availability forbids normalized-as-of admission"
        )
    cache_key = (
        example.symbol,
        example.product_manifest_id,
        example.ensemble_member_id,
    )
    if cache_key in source.quote_cache:
        return source.quote_cache[cache_key]
    related = [
        e
        for e in source.plan.examples
        if (e.symbol, e.product_manifest_id, e.ensemble_member_id) == cache_key
    ]
    intervals = []
    for item in related:
        for feature in item.features:
            if feature.kind is TemporalFeatureKind.VINTAGE:
                continue
            lo = item.decision_time_ns
            if feature.kind is not TemporalFeatureKind.QUOTE_AT_DECISION:
                lo = max(0, lo - feature.lookback_ns + 1)
            intervals.append((lo, item.decision_time_ns))
        if item.label.target is TemporalTargetKind.QUOTES:
            intervals.append(
                (
                    item.decision_time_ns,
                    item.decision_time_ns + item.label.horizon_ns,
                )
            )
    merged: list[tuple[int, int]] = []
    for lo, hi in sorted(intervals):
        if merged and lo <= merged[-1][1] + 1:
            merged[-1] = (merged[-1][0], max(hi, merged[-1][1]))
        else:
            merged.append((lo, hi))
    starts = [lo for lo, _ in merged]

    def selected_clock(clock: int) -> bool:
        index = bisect.bisect_right(starts, clock) - 1
        return index >= 0 and clock <= merged[index][1]

    points: dict[int, _TemporalPoint] = {}
    if example.product_manifest_id is None:
        winners: dict[int, _ObservedRow] = {}
        for row in source.legacy.observed:
            if row.symbol == example.symbol and selected_clock(
                row.event_time_ns
            ):
                if len(winners) >= 4096 and row.event_time_ns not in winners:
                    raise ValueError(
                        "temporal selected quote support exceeds 4096 points"
                    )
                winners[row.event_time_ns] = row
        for row in winners.values():
            key = (
                "observed-row:sha256:"
                + hashlib.sha256(
                    training_json(row.payload()).encode()
                ).hexdigest()
            )
            points[row.event_time_ns] = _TemporalPoint(
                row.event_time_ns,
                _midpoint(row.bid, row.ask),
                key,
                None,
                row.event_time_ns,
                row.event_time_ns + 1,
            )
    else:
        products = [
            p
            for p in source.legacy.products
            if p.manifest.manifest_id == example.product_manifest_id
        ]
        if len(products) != 1:
            raise ValueError(
                "temporal product is not in the complete inventory"
            )
        product = products[0]
        members = {e.ensemble_member_id for e in product.events}
        if example.ensemble_member_id not in members:
            raise ValueError(
                "temporal native member is not in the replayed product"
            )
        selected: list[SyntheticEventV1] = []
        for event in product.events:
            if (
                event.symbol.upper() == example.symbol
                and event.ensemble_member_id == example.ensemble_member_id
                and selected_clock(event.event_time_ns)
            ):
                if len(selected) >= 4096:
                    raise ValueError(
                        "temporal selected native support exceeds 4096 events"
                    )
                selected.append(event)
        for event in sorted(
            selected, key=lambda e: (e.event_time_ns, e.event_sequence)
        ):
            points[event.event_time_ns] = _TemporalPoint(
                event.event_time_ns,
                _midpoint(event.bid, event.ask),
                event.event_id,
                None,
                product.dependency_start_ns,
                product.dependency_end_ns,
            )
    result = tuple(points[t] for t in sorted(points))
    if (
        sum(len(value) for value in source.quote_cache.values()) + len(result)
        > 4096
    ):
        raise ValueError(
            "temporal aggregate selected quote support exceeds 4096 points"
        )
    source.quote_cache[cache_key] = result
    return result


def _state_units(
    source: _TemporalVerifiedSource,
    example: TrainingTemporalExampleV1,
    feature: TrainingTemporalFeatureV1,
    lo: int,
    hi: int,
) -> tuple[str, ...]:
    source.unit(lo)
    source.unit(hi - 1)
    assignments = {
        a.evidence_unit_id: a.partition for a in source.plan.split.assignments
    }
    touched = tuple(
        unit.artifact_id
        for unit in source.plan.ownership.units
        if unit.start_ns < hi
        and lo < unit.end_ns
        and assignments[unit.artifact_id] != example.partition
    )
    if touched:
        if feature.state_policy is not TemporalStatePolicy.WARMUP_ONLY:
            raise ValueError(
                "stateful feature crosses a forbidden split boundary"
            )
        if any(
            unit.end_ns > example.decision_time_ns
            for unit in source.plan.ownership.units
            if unit.artifact_id in touched
        ):
            raise ValueError(
                "warmup permits only earlier observed support, never future state"
            )
    return touched


def _vintage_feature(
    source: _TemporalVerifiedSource,
    example: TrainingTemporalExampleV1,
    feature: TrainingTemporalFeatureV1,
) -> TrainingTemporalValueV1:
    matrix = source.matrix(feature.matrix_id)
    if example.decision_time_ns > matrix.request.cutoff_at_ns:
        raise ValueError(
            "feature cutoff exceeds retained vintage inventory cutoff"
        )
    # Rebuild the entire selected grid with only records and metadata known at t.
    # A saved final cell is never used as an input or backdated to this cutoff.
    historical = VintageFeatureStoreV1(
        matrix.observations, matrix.schedules
    ).snapshot(replace(matrix.request, cutoff_at_ns=example.decision_time_ns))
    cells = [
        cell
        for cell in historical.cells
        if cell.column == feature.column
        and cell.period.label == feature.period_label
    ]
    if len(cells) != 1 or cells[0].value is None:
        raise ValueError(
            "requested vintage feature is unavailable at decision cutoff"
        )
    cell = cells[0]
    if (
        feature.expected_observation_ids
        and cell.observation_ids != feature.expected_observation_ids
    ):
        raise ValueError(
            "requested vintage/revision is not the cutoff information set"
        )
    ids = set(cell.observation_ids)
    observations = [
        o for o in historical.observations if o.observation_id in ids
    ]
    if len(observations) != len(ids) or not observations:
        raise ValueError(
            "derived feature lacks exact retained observation support"
        )
    lo = min(o.period.start_ns for o in observations)
    hi = max(o.period.end_ns for o in observations)
    warmup = _state_units(source, example, feature, lo, hi)
    available = max(
        max(o.available_at_ns, o.definition.known_at_ns) for o in observations
    )
    lineage = list(cell.observation_ids)
    lineage.extend(str(o.definition.to_dict()["id"]) for o in observations)
    if cell.schedule_id is not None:
        schedule = next(
            s
            for s in historical.schedules
            if s.to_dict()["id"] == cell.schedule_id
        )
        available = max(available, schedule.known_at_ns)
        lineage.append(cell.schedule_id)
    if available > example.decision_time_ns:
        raise ValueError(
            "feature metadata/schedule availability follows decision"
        )
    assert cell.value is not None
    return TrainingTemporalValueV1(
        feature.name,
        cell.value,
        available,
        lo,
        hi,
        tuple(dict.fromkeys(lineage)),
        "normalized_source_value_and_clock_replay_not_official_authenticity",
        warmup,
    )


def temporal_feature_value(
    source: _TemporalVerifiedSource,
    example: TrainingTemporalExampleV1,
    feature: TrainingTemporalFeatureV1,
    points: tuple[_TemporalPoint, ...],
) -> TrainingTemporalValueV1:
    """Internal computation; no caller-supplied fitted state or labels accepted."""
    if feature.kind is TemporalFeatureKind.VINTAGE:
        return _vintage_feature(source, example, feature)
    history = [p for p in points if p.time_ns <= example.decision_time_ns]
    if feature.kind is TemporalFeatureKind.QUOTE_AT_DECISION:
        history = history[-1:]
        if history and history[0].time_ns != example.decision_time_ns:
            raise ValueError(
                "last quote feature requires an exact decision-time quote"
            )
    else:
        history = [
            p
            for p in history
            if example.decision_time_ns - feature.lookback_ns < p.time_ns
        ]
    if not history:
        raise ValueError("quote feature has no prior observed support")
    lo = min(p.dependency_start_ns for p in history)
    hi = max(p.dependency_end_ns for p in history)
    if feature.kind is not TemporalFeatureKind.QUOTE_AT_DECISION:
        lo = min(lo, max(0, example.decision_time_ns - feature.lookback_ns + 1))
    if (
        feature.state_policy is TemporalStatePolicy.WARMUP_ONLY
        and hi > example.decision_time_ns + 1
    ):
        raise ValueError(
            "warmup cannot import future conditioning or fitted state"
        )
    warmup = _state_units(source, example, feature, lo, hi)
    values = [p.value for p in history]
    mean = statistics.mean(values)
    if feature.kind is TemporalFeatureKind.QUOTE_AT_DECISION:
        value = values[-1]
    elif feature.kind is TemporalFeatureKind.QUOTE_MEAN:
        value = mean
    else:
        if len(values) < 2:
            raise ValueError(
                "normalization requires at least two prior observations"
            )
        try:
            variance = math.fsum(
                (v - mean) ** 2 / (len(values) - 1) for v in values
            )
        except OverflowError as exc:
            raise ValueError(
                "rolling normalization arithmetic is unrepresentable"
            ) from exc
        if not math.isfinite(variance) or variance <= 0:
            raise ValueError("rolling normalization has zero/nonfinite scale")
        value = (values[-1] - mean) / math.sqrt(variance)
    return TrainingTemporalValueV1(
        feature.name,
        value,
        None,
        lo,
        hi,
        tuple(p.record_id for p in history),
        "source_values_replayed_historical_availability_unknown",
        warmup,
    )


def _market_points(
    source: _TemporalVerifiedSource, example: TrainingTemporalExampleV1
) -> tuple[_TemporalPoint, ...]:
    matrix = source.matrix(example.label.matrix_id)
    if example.label_cutoff_ns > matrix.request.cutoff_at_ns:
        raise ValueError(
            "label cutoff exceeds retained source inventory cutoff"
        )
    observations = [
        o
        for o in matrix.observations
        if o.definition.feature_key == example.label.feature_key
        and o.vintage_sequence == 0
        and example.decision_time_ns
        <= o.period.end_ns
        <= example.decision_time_ns + example.label.horizon_ns
    ]
    if not observations or any(
        o.definition.kind is not FeatureKind.MARKET for o in observations
    ):
        raise ValueError(
            "market target requires initial observed market levels"
        )
    if len({o.definition.semantic_id for o in observations}) != 1:
        raise ValueError("market target units/scale/base semantics change")
    points = []
    for item in observations:
        if item.available_at_ns > example.label_cutoff_ns:
            raise ValueError(
                "market target support is unavailable at label cutoff"
            )
        if item.value is None or item.value <= 0:
            raise ValueError(
                "market target has missing/nonpositive price support"
            )
        points.append(
            _TemporalPoint(
                item.period.end_ns,
                item.value,
                item.observation_id,
                max(item.available_at_ns, item.definition.known_at_ns),
                item.period.start_ns,
                item.period.end_ns,
            )
        )
    if len({p.time_ns for p in points}) != len(points):
        raise ValueError("ambiguous market target clocks")
    return tuple(sorted(points, key=lambda point: point.time_ns))
