"""Complete-inventory overlap analysis and row-preserving exact mass projection."""

from __future__ import annotations

import math
from fractions import Fraction
from statistics import mean

from .training_contracts import training_load
from .training_overlap_contracts import (
    TrainingOverlapBatchV1,
    TrainingOverlapCarryV1,
    TrainingOverlapCoordinateV1,
    TrainingOverlapMassV1,
    TrainingOverlapOccurrenceV1,
    TrainingOverlapPlanV1,
    TrainingOverlapRowV1,
    TrainingOverlapSelectionV1,
    TrainingOverlapWindowV1,
)
from .training_overlap_math import overlap_mass
from .training_overlap_sources import (
    _OverlapSource,
    native_overlap_features,
    overlap_id,
    verify_overlap_source,
    window_ids,
)
from .training_temporal_contracts import TemporalStatePolicy
from .training_temporal_views import materialize_training_temporal


def _midpoint(coordinate: TrainingOverlapCoordinateV1) -> float:
    data = training_load(coordinate.value_json)
    bid, ask = data.get("bid"), data.get("ask")
    if (
        type(bid) is not float
        or type(ask) is not float
        or not 0 < bid <= ask
        or not math.isfinite(ask)
    ):
        raise ValueError(
            "prefix state requires finite positive uncrossed raw quotes"
        )
    return bid + (ask - bid) / 2


def _order(
    pair: tuple[TrainingOverlapCoordinateV1, TrainingOverlapOccurrenceV1],
) -> tuple[int, int, str]:
    coordinate, occurrence = pair
    data = training_load(occurrence.native_event_json or coordinate.value_json)
    ordinal = data.get("event_sequence", data.get("source_row_id"))
    if type(ordinal) is not int:
        raise ValueError(
            "prefix state requires exact native/physical ordering evidence"
        )
    return coordinate.event_time_ns, ordinal, coordinate.source_row_key


def _carry(
    plan: TrainingOverlapPlanV1, source: _OverlapSource
) -> tuple[TrainingOverlapCarryV1, ...]:
    count = (
        len(plan.geometry.intervals)
        * len(source.variants)
        * len(source.verified.graph_symbols)
    )
    if count > 4096:
        raise ValueError("complete carry state inventory exceeds4096")
    partitions = {
        a.evidence_unit_id: a.partition for a in plan.split.assignments
    }
    by_id = {c.coordinate_id: c for c in source.coordinates}
    pairs = tuple((by_id[o.coordinate_id], o) for o in source.occurrences)
    ids = window_ids(plan)
    result = []
    for index, (start, _) in enumerate(plan.geometry.intervals):
        lower = start - plan.carry_lookback_ns
        partition = next(
            a.partition
            for u, a in zip(plan.ownership.units, plan.split.assignments)
            if u.start_ns <= start < u.end_ns
        )
        for member, scenario in source.variants:
            for symbol in source.verified.graph_symbols:
                support = sorted(
                    (
                        (c, o)
                        for c, o in pairs
                        if c.symbol == symbol
                        and (o.member_id, o.scenario_id) == (member, scenario)
                        and lower <= c.event_time_ns < start
                    ),
                    key=_order,
                )
                positions = tuple(_order(pair)[:2] for pair in support)
                if len(set(positions)) != len(positions):
                    raise ValueError(
                        "ambiguous native/physical prefix ordering"
                    )
                state = (
                    "available_strict_prior"
                    if support
                    else "empty_prior_support"
                )
                if any(o.dependency_end_ns > start for _, o in support):
                    state = "unavailable_future_conditioning"
                if any(
                    partitions[c.evidence_unit_id] is not partition
                    and (
                        plan.carry_policy is TemporalStatePolicy.RESET
                        or o.product_manifest_id is not None
                        and training_load(o.native_event_json or "{}").get(
                            "origin"
                        )
                        != "observed"
                    )
                    for c, o in support
                ):
                    state = "unavailable_cross_partition"
                # Recompute actual native quote state from the dependency prefix.
                # Never consume a prior overlapping child's terminal state.
                admitted = support if state == "available_strict_prior" else []
                values = tuple(_midpoint(c) for c, _ in admitted)
                result.append(
                    TrainingOverlapCarryV1(
                        ids[index],
                        symbol,
                        member,
                        scenario,
                        tuple(c.coordinate_id for c, _ in admitted),
                        tuple(o.artifact_id for _, o in admitted),
                        values[-1] if values else None,
                        float(mean(values)) if values else None,
                        lower,
                        start,
                        state,
                    )
                )
    return tuple(result)


def _windows(
    plan: TrainingOverlapPlanV1,
    source: _OverlapSource,
    carries: tuple[TrainingOverlapCarryV1, ...],
) -> tuple[tuple[TrainingOverlapWindowV1, ...], tuple[str, ...], int]:
    ids = window_ids(plan)
    partitions = {
        a.evidence_unit_id: a.partition for a in plan.split.assignments
    }
    boundaries = tuple(
        right.start_ns
        for left, right in zip(plan.ownership.units, plan.ownership.units[1:])
        if partitions[left.artifact_id] is not partitions[right.artifact_id]
    )
    targets = {
        target.window_index: materialize_training_temporal(target.plan)
        for target in plan.targets
    }
    records = []
    maximum = 0
    for index, (lo, hi) in enumerate(plan.geometry.intervals):
        core = tuple(
            u.artifact_id
            for u in plan.ownership.units
            if u.start_ns < hi and lo < u.end_ns
        )
        parts = {partitions[u] for u in core}
        partition = next(iter(parts)) if len(parts) == 1 else None
        reasons = set()
        if partition is None:
            reasons.add("core_crosses_partition")
        dependency_lo, dependency_hi = lo - plan.carry_lookback_ns, hi
        conditioning_lo, conditioning_hi = lo, hi
        for product in source.verified.products:
            if any(lo <= e.event_time_ns < hi for e in product.events):
                conditioning_lo = min(
                    conditioning_lo, product.dependency_start_ns
                )
                conditioning_hi = max(
                    conditioning_hi, product.dependency_end_ns
                )
        dependency_lo = min(dependency_lo, conditioning_lo)
        dependency_hi = max(dependency_hi, conditioning_hi)
        conditioning_units = tuple(
            u.artifact_id
            for u in plan.ownership.units
            if u.start_ns < conditioning_hi and conditioning_lo < u.end_ns
        )
        dependent_units = set(conditioning_units)
        if partition is not None and any(
            partitions[u] is not partition for u in conditioning_units
        ):
            reasons.add("conditioning_crosses_partition")
        target = targets.get(index)
        if target is not None:
            for row in target.rows:
                spans = [
                    (
                        row.outcome.dependency_start_ns,
                        row.outcome.dependency_end_ns,
                    )
                ] + [
                    (f.dependency_start_ns, f.dependency_end_ns)
                    for f in row.features
                ]
                dependency_lo = min(dependency_lo, *(s[0] for s in spans))
                dependency_hi = max(dependency_hi, *(s[1] for s in spans))
                dependent_units.update(
                    u.artifact_id
                    for u in plan.ownership.units
                    if any(
                        u.start_ns < stop and start < u.end_ns
                        for start, stop in spans
                    )
                )
                if row.status != "admitted":
                    reasons.add("temporal_target_excluded")
                if partition is None or any(
                    partitions[u] is not partition
                    for u in row.outcome.target_unit_ids
                ):
                    reasons.add("target_crosses_partition")
        if any(
            c.window_id == ids[index] and c.state.startswith("unavailable_")
            for c in carries
        ):
            reasons.add("unavailable_prefix_state")
        maximum = max(maximum, dependency_hi - dependency_lo)
        records.append(
            (
                core,
                partition,
                dependency_lo,
                dependency_hi,
                reasons,
                tuple(sorted(dependent_units)),
            )
        )
    # Derived from the full frozen support inventory, not surviving samples.
    for index, (lo, _) in enumerate(plan.geometry.intervals):
        reasons = records[index][4]
        for boundary in boundaries:
            if boundary - maximum < lo < boundary:
                reasons.add("purged_dependency_overlap")
            if boundary <= lo < boundary + maximum:
                reasons.add("embargo_dependency_span")
    admitted = [i for i, record in enumerate(records) if not record[4]]
    parent = {i: i for i in admitted}

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    # Eligible overlap connectivity is transitive; original siblings also remain
    # connected even where the requested analytical geometry has no shared event.
    for position, left in enumerate(admitted):
        for right in admitted[position + 1 :]:
            lo1, hi1 = plan.geometry.intervals[left]
            lo2, hi2 = plan.geometry.intervals[right]
            if (
                set(records[left][5]) & set(records[right][5])
                or lo1 < hi2
                and lo2 < hi1
            ):
                if records[left][1] is not records[right][1]:
                    raise ValueError(
                        "eligible overlap component crosses a protected split"
                    )
                parent[find(right)] = find(left)
    groups: dict[int, list[int]] = {}
    for i in admitted:
        groups.setdefault(find(i), []).append(i)
    group_ids = {}
    for group in groups.values():
        identity = overlap_id(
            "group",
            {
                "plan": plan.artifact_id,
                "windows": sorted(ids[i] for i in group),
                "original_units": sorted(
                    {u for i in group for u in records[i][5]}
                ),
            },
        )
        for i in group:
            group_ids[i] = identity
    windows = tuple(
        TrainingOverlapWindowV1(
            ids[i],
            i,
            lo,
            hi,
            records[i][0],
            records[i][5],
            records[i][1],
            group_ids.get(i),
            records[i][2],
            records[i][3],
            tuple(sorted(records[i][4])),
        )
        for i, (lo, hi) in enumerate(plan.geometry.intervals)
    )
    return (
        windows,
        tuple(targets[i].to_json() for i in sorted(targets)),
        maximum,
    )


def _allocations(
    plan: TrainingOverlapPlanV1,
    source: _OverlapSource,
    windows: tuple[TrainingOverlapWindowV1, ...],
) -> tuple[TrainingOverlapMassV1, ...]:
    by_id = {c.coordinate_id: c for c in source.coordinates}
    result: list[TrainingOverlapMassV1] = []
    for unit in plan.ownership.units:
        applicable = tuple(
            w for w in windows if unit.artifact_id in w.parent_unit_ids
        )
        if len(result) + len(applicable) * len(source.variants) > 4096:
            raise ValueError(
                "complete original-unit allocation inventory exceeds4096"
            )
        counts = tuple(
            (
                w.window_id,
                member,
                scenario,
                sum(
                    c.evidence_unit_id == unit.artifact_id
                    and w.index in c.window_indices
                    for o in source.occurrences
                    if (o.member_id, o.scenario_id) == (member, scenario)
                    for c in (by_id[o.coordinate_id],)
                ),
            )
            for w in applicable
            for member, scenario in source.variants
        )
        allocations = overlap_mass(
            counts,
            window_ids=tuple(w.window_id for w in applicable),
            variants=source.variants,
        )
        for mass in allocations:
            result.append(
                TrainingOverlapMassV1(
                    unit.artifact_id,
                    mass.window_id,
                    mass.member_id,
                    mass.scenario_id,
                    mass.row_count,
                    str(mass.per_row.numerator),
                    str(mass.per_row.denominator),
                    str(mass.reserved_mass.numerator),
                    str(mass.reserved_mass.denominator),
                )
            )
    return tuple(result)


def materialize_training_overlap(
    plan: TrainingOverlapPlanV1,
    selection: TrainingOverlapSelectionV1 = TrainingOverlapSelectionV1(),
) -> TrainingOverlapBatchV1:
    """Verify every input first; projection never changes membership or mass.

    Window results, native analytics, full event ledger, reserved excluded mass
    and whole-unit split decisions remain inspectable even for an empty view.
    """
    if (
        type(plan) is not TrainingOverlapPlanV1
        or type(selection) is not TrainingOverlapSelectionV1
    ):
        raise TypeError("overlap materialization requires typed plan/selection")
    source = verify_overlap_source(plan)
    carries = _carry(plan, source)
    windows, targets_json, maximum = _windows(plan, source, carries)
    allocations = _allocations(plan, source, windows)
    native = native_overlap_features(plan, source)
    by_id = {c.coordinate_id: c for c in source.coordinates}
    allocation_by_key = {
        (a.evidence_unit_id, a.window_id, a.member_id, a.scenario_id): a
        for a in allocations
    }
    for selected, available in (
        (selection.window_ids, {w.window_id for w in windows}),
        (selection.coordinate_ids, set(by_id)),
        (selection.member_ids, {m for m, _ in source.variants}),
    ):
        if selected is not None and not set(selected) <= available:
            raise ValueError(
                "overlap selection names an unknown complete coordinate"
            )
    rows: list[TrainingOverlapRowV1] = []
    for window in windows:
        if (
            window.reasons
            or selection.window_ids is not None
            and window.window_id not in selection.window_ids
        ):
            continue
        for occurrence in source.occurrences:
            coordinate = by_id[occurrence.coordinate_id]
            if (
                window.index not in coordinate.window_indices
                or selection.coordinate_ids is not None
                and coordinate.coordinate_id not in selection.coordinate_ids
                or selection.member_ids is not None
                and occurrence.member_id not in selection.member_ids
            ):
                continue
            if len(rows) >= 4096:
                raise ValueError(
                    "projected overlap descendant count exceeds4096"
                )
            allocation = allocation_by_key[
                coordinate.evidence_unit_id,
                window.window_id,
                occurrence.member_id,
                occurrence.scenario_id,
            ]
            rows.append(
                TrainingOverlapRowV1(
                    window.window_id,
                    coordinate.coordinate_id,
                    occurrence.artifact_id,
                    allocation.artifact_id,
                    coordinate.event_time_ns,
                    max(window.end_ns, window.dependency_end_ns),
                )
            )
    return TrainingOverlapBatchV1(
        plan,
        selection,
        source.coordinates,
        source.occurrences,
        windows,
        carries,
        allocations,
        native,
        targets_json,
        maximum,
        tuple(rows),
    )


def replay_training_overlap(
    batch: TrainingOverlapBatchV1,
) -> TrainingOverlapBatchV1:
    replayed = materialize_training_overlap(batch.plan, batch.selection)
    if replayed != batch:
        raise ValueError(
            "overlap artifact differs from complete source/native/math replay"
        )
    return replayed


def training_overlap_records(
    batch: TrainingOverlapBatchV1,
) -> tuple[dict[str, object], ...]:
    """Executable public row consumer; no implicit completed-window backfill."""
    replay_training_overlap(batch)
    coordinates = {c.coordinate_id: c for c in batch.coordinates}
    occurrences = {o.artifact_id: o for o in batch.occurrences}
    allocations = {a.artifact_id: a for a in batch.allocations}
    result: list[dict[str, object]] = []
    for row in batch.rows:
        coordinate = coordinates[row.coordinate_id]
        occurrence = occurrences[row.occurrence_id]
        allocation = allocations[row.allocation_id]
        result.append(
            {
                "window_id": row.window_id,
                "source_coordinate_id": row.coordinate_id,
                "original_evidence_unit_id": coordinate.evidence_unit_id,
                "symbol": coordinate.symbol,
                "source_values": training_load(coordinate.value_json),
                "native_values": (
                    training_load(occurrence.native_event_json)
                    if occurrence.native_event_json is not None
                    else None
                ),
                "member_id": occurrence.member_id,
                "scenario_id": occurrence.scenario_id,
                "weight_numerator": allocation.numerator,
                "weight_denominator": allocation.denominator,
                "complete_original_unit_branch_rows": allocation.complete_row_count,
                "event_time_ns": row.event_time_ns,
                "analytical_cutoff_ns": row.analytical_cutoff_ns,
                "historical_available_at_ns": None,
                "information_mode": "explicit_ex_post_reconstruction_research",
            }
        )
    return tuple(result)


def overlap_unit_mass(
    batch: TrainingOverlapBatchV1,
) -> tuple[tuple[str, Fraction], ...]:
    """Actual emitted mass per original unit, not an effective sample size."""
    replay_training_overlap(batch)
    allocations = {a.artifact_id: a for a in batch.allocations}
    totals = {u.artifact_id: Fraction(0) for u in batch.plan.ownership.units}
    for row in batch.rows:
        allocation = allocations[row.allocation_id]
        totals[allocation.evidence_unit_id] += allocation.per_row
    if any(value > 1 for value in totals.values()):
        raise ValueError("original evidence unit descendant mass exceeds one")
    return tuple(sorted(totals.items()))
