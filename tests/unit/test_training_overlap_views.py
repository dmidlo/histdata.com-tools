"""Executed complete-window, split, exact-mass and prefix-state consumers."""

from dataclasses import replace
from fractions import Fraction

import pytest

from histdatacom.data_quality.training_contracts import DAY_NS
from histdatacom.data_quality.training_overlap_contracts import (
    TrainingOverlapBatchV1,
    TrainingOverlapSelectionV1,
)
from histdatacom.data_quality.training_overlap_views import (
    materialize_training_overlap,
    replay_training_overlap,
    training_overlap_records,
)
from tests.fixtures.training_overlap_v1 import BASE, overlap_fixture


def _target_plan(plan, decision, horizon):
    from histdatacom.data_quality.training_overlap_sources import unit_at
    from histdatacom.data_quality.training_temporal_contracts import (
        TemporalFeatureKind,
        TemporalInformationMode,
        TemporalLabelKind,
        TrainingTemporalExampleV1,
        TrainingTemporalFeatureV1,
        TrainingTemporalLabelV1,
        TrainingTemporalPlanV1,
    )

    owner = unit_at(plan.ownership, decision)
    partition = next(
        a.partition
        for a in plan.split.assignments
        if a.evidence_unit_id == owner
    )
    example = TrainingTemporalExampleV1(
        "actual-target",
        decision,
        decision + horizon,
        "EURUSD",
        owner,
        partition,
        (
            TrainingTemporalFeatureV1(
                "quote", TemporalFeatureKind.QUOTE_AT_DECISION
            ),
        ),
        TrainingTemporalLabelV1(TemporalLabelKind.LOG_RETURN, horizon),
    )
    return TrainingTemporalPlanV1(
        plan.source,
        plan.ownership,
        plan.split,
        TemporalInformationMode.EX_POST,
        (example,),
    )


@pytest.mark.parametrize("crosses", [False, True])
def test_actual_temporal_target_replay_derives_full_support_and_split_exclusion(
    tmp_path, crosses
):
    from histdatacom.data_quality.training_overlap_contracts import (
        TrainingOverlapGeometryV1,
        TrainingOverlapTargetV1,
    )
    from histdatacom.data_quality.training_temporal_contracts import (
        TrainingTemporalBatchV1,
    )

    plan = overlap_fixture(tmp_path, multiple_splits=True)
    decision = BASE + (7 if crosses else 6) * DAY_NS // 10
    horizon = (9 if crosses else 1) * DAY_NS // 10
    geometry = TrainingOverlapGeometryV1(
        decision - DAY_NS // 10, decision, DAY_NS // 10, DAY_NS // 10
    )
    plan = replace(
        plan,
        geometry=geometry,
        targets=(
            TrainingOverlapTargetV1(0, _target_plan(plan, decision, horizon)),
        ),
    )
    batch = materialize_training_overlap(plan)
    temporal = TrainingTemporalBatchV1.from_json(batch.targets_json[0])
    assert temporal.rows[0].outcome.target_start_exclusive_ns == decision
    assert (
        temporal.rows[0].outcome.target_end_inclusive_ns == decision + horizon
    )
    assert batch.windows[0].dependency_end_ns == decision + horizon + 1
    assert bool(batch.windows[0].reasons) is crosses
    if crosses:
        assert "target_crosses_partition" in batch.windows[0].reasons
        assert not batch.rows
    else:
        assert temporal.rows[0].outcome.value == pytest.approx(0.0)


def test_exact_source_unit_mass_and_filtered_rows_do_not_renormalize(tmp_path):
    plan = overlap_fixture(tmp_path)
    batch = materialize_training_overlap(plan)
    allocations = {a.artifact_id: a for a in batch.allocations}
    totals = {u.artifact_id: Fraction(0) for u in plan.ownership.units}
    for row in batch.rows:
        allocation = allocations[row.allocation_id]
        totals[allocation.evidence_unit_id] += allocation.per_row
    assert all(0 <= mass <= 1 for mass in totals.values())
    assert sum(mass > 0 for mass in totals.values()) == 3
    selected = materialize_training_overlap(
        plan,
        TrainingOverlapSelectionV1(window_ids=(batch.windows[2].window_id,)),
    )
    assert selected.allocations == batch.allocations
    assert selected.coordinates == batch.coordinates
    assert all(row in batch.rows for row in selected.rows)
    assert TrainingOverlapBatchV1.from_json(batch.to_json()) == batch
    assert len(training_overlap_records(selected)) == len(selected.rows)


def test_boundary_windows_excluded_without_destroying_multiple_splits(tmp_path):
    plan = overlap_fixture(tmp_path, multiple_splits=True)
    batch = materialize_training_overlap(plan)
    assert {w.partition.value for w in batch.windows if not w.reasons} == {
        "train",
        "validation",
        "test",
    }
    assert {
        w.partition.value
        for w in batch.windows
        if any(r.window_id == w.window_id for r in batch.rows)
    } == {"train", "validation", "test"}
    assert any("core_crosses_partition" in w.reasons for w in batch.windows)
    assert any("embargo_dependency_span" in w.reasons for w in batch.windows)
    assert all(
        not any(r.window_id == w.window_id for r in batch.rows)
        for w in batch.windows
        if w.reasons
    )
    assert len({w.group_id for w in batch.windows if not w.reasons}) == 3


def test_actual_prior_state_and_resealed_future_state_refusal(tmp_path):
    plan = overlap_fixture(tmp_path)
    plan = replace(plan, carry_lookback_ns=DAY_NS // 4)
    batch = materialize_training_overlap(plan)
    coordinate_by_id = {c.coordinate_id: c for c in batch.coordinates}
    available = [
        c for c in batch.carries if c.state == "available_strict_prior"
    ]
    assert available
    for carry in available:
        assert all(
            coordinate_by_id[c].event_time_ns < carry.dependency_end_ns
            for c in carry.coordinate_ids
        )
        assert carry.mean_midpoint > 0
    chosen = available[0]
    wrong = replace(chosen, last_midpoint=chosen.last_midpoint + 1.0)
    forged = replace(
        batch, carries=tuple(wrong if c == chosen else c for c in batch.carries)
    )
    assert TrainingOverlapBatchV1.from_json(forged.to_json()) == forged
    with pytest.raises(ValueError, match="complete source/native/math replay"):
        replay_training_overlap(forged)


def test_short_geometry_retains_complete_outside_window_ledger(tmp_path):
    plan = overlap_fixture(tmp_path)
    plan = replace(
        plan,
        geometry=replace(
            plan.geometry, start_ns=BASE + DAY_NS, end_ns=BASE + DAY_NS + 1
        ),
    )
    result = materialize_training_overlap(plan)
    assert result.windows == result.rows == result.allocations == ()
    assert len(result.coordinates) == 18
    assert all(not c.window_indices for c in result.coordinates)


def test_shared_actual_target_units_connect_disjoint_cores_but_prior_warmup_does_not(
    tmp_path,
):
    from histdatacom.data_quality.training_overlap_contracts import (
        TrainingOverlapGeometryV1,
        TrainingOverlapPlanV1,
        TrainingOverlapTargetV1,
    )
    from histdatacom.data_quality.training_temporal_contracts import (
        TemporalPartition,
        TemporalStatePolicy,
        TrainingTemporalAssignmentV1,
        TrainingTemporalSplitV1,
    )
    from tests.fixtures.training_join_v1 import controlled_spine

    spine = controlled_spine(
        tmp_path, tuple(BASE + i * DAY_NS for i in range(4))
    )
    split = TrainingTemporalSplitV1(
        spine.ownership.artifact_id,
        tuple(
            TrainingTemporalAssignmentV1(u.artifact_id, TemporalPartition.TRAIN)
            for u in spine.ownership.units
        ),
    )
    plan = TrainingOverlapPlanV1(
        spine.source,
        spine.ownership,
        split,
        TrainingOverlapGeometryV1(BASE, BASE + 2 * DAY_NS, DAY_NS, DAY_NS),
    )
    warmup = materialize_training_overlap(
        replace(
            plan,
            carry_lookback_ns=DAY_NS,
            carry_policy=TemporalStatePolicy.WARMUP_ONLY,
        )
    )
    assert not set(warmup.windows[0].parent_unit_ids) & set(
        warmup.windows[1].parent_unit_ids
    )
    assert warmup.windows[0].group_id != warmup.windows[1].group_id
    assert all(
        w.dependency_unit_ids == w.parent_unit_ids for w in warmup.windows
    )
    assert any(c.state == "available_strict_prior" for c in warmup.carries)
    targets = tuple(
        TrainingOverlapTargetV1(
            index,
            _target_plan(
                plan, BASE + (index + 1) * DAY_NS, (2 - index) * DAY_NS
            ),
        )
        for index in range(2)
    )
    shared = materialize_training_overlap(replace(plan, targets=targets))
    assert shared.windows[0].group_id == shared.windows[1].group_id
    assert set(shared.windows[0].dependency_unit_ids) & set(
        shared.windows[1].dependency_unit_ids
    )
    assert not set(shared.windows[0].parent_unit_ids) & set(
        shared.windows[1].parent_unit_ids
    )
