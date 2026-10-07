"""Actual generated IPC/native-wide admission and source-derived fit mass."""

from dataclasses import replace
from fractions import Fraction

import pytest

from histdatacom.data_quality.training_contracts import (
    DAY_NS,
    TrainingConsumerMode,
    TrainingRequestV1,
    training_json,
)
from histdatacom.data_quality.training_lineage import build_training_ownership
from histdatacom.data_quality.training_preprocessing_native import (
    MASS_POLICY,
    ROLLING_MASS_POLICY,
    assert_training_preprocessing_native_current,
    inspect_training_preprocessing_native,
    select_training_preprocessing_fit,
)
from histdatacom.data_quality.training_temporal_contracts import (
    TemporalPartition,
    TrainingTemporalAssignmentV1,
    TrainingTemporalSplitV1,
)
from histdatacom.data_quality.training_views import materialize_training_rows
from histdatacom.data_quality.training_wide_contracts import (
    TrainingWideGrainV1,
    WideRowGrain,
)
from histdatacom.data_quality.training_wide_views import (
    build_training_wide_plan,
)
from tests.fixtures.training_join_v1 import join_fixture, vintage_join
from tests.fixtures.training_substrate_v1 import BASE
from tests.fixtures.training_temporal_v1 import SECOND


def _split(join):
    ownership = join.spine.ownership
    return TrainingTemporalSplitV1(
        ownership.artifact_id,
        tuple(
            TrainingTemporalAssignmentV1(
                u.artifact_id,
                (
                    TemporalPartition.TRAIN
                    if u.start_ns <= BASE
                    else (
                        TemporalPartition.VALIDATION
                        if u.start_ns == BASE + DAY_NS
                        else TemporalPartition.TEST
                    )
                ),
            )
            for u in ownership.units
        ),
    )


def _wide(join, *, tiled=False):
    columns = (
        join.columns[0],
        replace(join.columns[0], name="market.tick.EURUSD.second_bid"),
    )
    plans = (
        tuple(replace(join, columns=(c,)) for c in columns)
        if tiled
        else (replace(join, columns=columns),)
    )
    return build_training_wide_plan(
        plans, grain=TrainingWideGrainV1(WideRowGrain.EVENT)
    )


@pytest.fixture(scope="module")
def native(tmp_path_factory):
    return join_fixture(tmp_path_factory.mktemp("preprocessing-native"))


@pytest.fixture(scope="module")
def evidence(native):
    return inspect_training_preprocessing_native(_wide(native), _split(native))


def _train(evidence):
    return tuple(
        sorted(
            a.evidence_unit_id
            for a in evidence.split.assignments
            if a.partition is TemporalPartition.TRAIN
            and a.evidence_unit_id
            in {r.evidence_unit_id for r in evidence.rows}
        )
    )


def _fit(evidence, **kwargs):
    arguments = {
        "fit_mode": "train_fit",
        "fit_unit_ids": _train(evidence),
        "fold_id": "fold-a",
    }
    arguments.update(kwargs)
    return select_training_preprocessing_fit(evidence, **arguments)


def test_source_denominator_is_full_graph_not_selected_symbol_window(evidence):
    assert len(evidence.rows) == 3
    assert all(r.weight == Fraction(1, 12) for r in evidence.rows)
    assert len(evidence.denominators) == len(evidence.split.assignments)
    first = next(
        d for d in evidence.denominators if d.selected_unique_row_count
    )
    assert first.complete_row_count == 12  # 3 symbols, four quotes each
    assert first.selected_unique_row_count == 3
    assert first.selected_mass == Fraction(1, 4)
    assert first.withheld_mass == Fraction(3, 4)
    assert evidence.mass_policy == MASS_POLICY
    assert len(evidence.source_anchor_ids) == 36
    assert all(c.provenance is not None for r in evidence.rows for c in r.cells)
    assert _fit(evidence).selected_mass == Fraction(1, 4)
    assert _fit(evidence).withheld_mass == Fraction(3, 4)


def test_column_tiles_do_not_multiply_mass(native, evidence):
    tiled = inspect_training_preprocessing_native(
        _wide(native, tiled=True), _split(native)
    )
    assert tiled.rows == evidence.rows
    assert tiled.denominators == evidence.denominators
    assert (
        _fit(tiled).parameter_fit_identity
        == _fit(evidence).parameter_fit_identity
    )


@pytest.mark.parametrize("mutation", ("weight", "value", "denominator", "root"))
def test_resealed_process_local_evidence_is_not_authority(evidence, mutation):
    if mutation == "weight":
        bad = replace(
            evidence,
            rows=(replace(evidence.rows[0], weight=Fraction(99)),)
            + evidence.rows[1:],
        )
    elif mutation == "value":
        row = evidence.rows[0]
        bad = replace(
            evidence,
            rows=(
                replace(
                    row,
                    cells=(replace(row.cells[0], original_value=99),)
                    + row.cells[1:],
                ),
            )
            + evidence.rows[1:],
        )
    elif mutation == "denominator":
        bad = replace(evidence, denominators=evidence.denominators[:-1])
    else:
        bad = replace(evidence, roots=())
    with pytest.raises(ValueError, match="fresh native replay"):
        assert_training_preprocessing_native_current(bad)


@pytest.mark.parametrize("mutation", ("subset", "reorder", "wrong_owner"))
def test_split_requires_complete_chronological_ownership(native, mutation):
    split = _split(native)
    if mutation == "subset":
        split = replace(split, assignments=split.assignments[:-1])
    elif mutation == "reorder":
        split = replace(
            split,
            assignments=tuple(
                replace(
                    a,
                    partition=(
                        TemporalPartition.TEST
                        if i == 0
                        else TemporalPartition.TRAIN
                    ),
                )
                for i, a in enumerate(split.assignments)
            ),
        )
    else:
        split = replace(split, ownership_id="different-owner")
    with pytest.raises(ValueError, match="ownership|chronological"):
        inspect_training_preprocessing_native(_wide(native), split)


@pytest.mark.parametrize(
    "partition", (TemporalPartition.VALIDATION, TemporalPartition.TEST)
)
def test_protected_units_cannot_enter_fit(evidence, partition):
    units = tuple(
        sorted(
            a.evidence_unit_id
            for a in evidence.split.assignments
            if a.partition is partition
        )
    )
    with pytest.raises(ValueError, match="whole TRAIN"):
        _fit(evidence, fit_unit_ids=units)


def test_fold_identity_changes_membership_not_actual_native_weights(evidence):
    a = _fit(evidence)
    b = _fit(evidence, fold_id="fold-b")
    assert a.weights == b.weights
    assert a.parameter_fit_identity != b.parameter_fit_identity


def test_mutated_native_spine_resealed_plan_still_refuses(native):
    row = replace(native.spine.rows[0], value_json=training_json({"bid": 999}))
    bad = replace(
        native, spine=replace(native.spine, rows=(row,) + native.spine.rows[1:])
    )
    with pytest.raises(ValueError, match="replay"):
        _wide(bad)


def test_legacy_event_spine_cannot_claim_rolling_availability(evidence):
    with pytest.raises(ValueError, match="GRID/BAR"):
        _fit(evidence, fit_mode="rolling_pti", cutoff_ns=BASE + 4 * SECOND)


def _grid(join, start, end, age=0):
    grain = TrainingWideGrainV1(WideRowGrain.GRID, start, end, SECOND, age)
    plans = []
    for clock in grain.cutoffs:
        spine = materialize_training_rows(
            join.spine.source,
            join.spine.ownership,
            replace(
                join.spine.request,
                start_ns=max(
                    join.spine.ownership.units[0].start_ns, clock - age
                ),
                end_ns=clock + 1,
                decision_time_ns=clock,
            ),
        )
        plans.append(replace(join, spine=spine))
    return build_training_wide_plan(tuple(plans), grain=grain)


def test_missing_grid_anchor_has_no_synthetic_ownership(native):
    evidence = inspect_training_preprocessing_native(
        _grid(native, BASE, BASE + 3 * SECOND), _split(native)
    )
    missing = evidence.rows[0]
    assert (
        missing.native_row
        is missing.evidence_unit_id
        is missing.partition
        is None
    )
    assert missing.weight == 0
    assert all(
        c.state is None and c.provenance is None and c.original_value is None
        for c in missing.cells
    )
    assert (missing.wide_row_id, "missing_spine_anchor") in _fit(
        evidence
    ).excluded_rows


def test_same_original_anchor_reused_across_grid_does_not_inflate_mass(native):
    evidence = inspect_training_preprocessing_native(
        _grid(native, BASE + 4 * SECOND, BASE + 7 * SECOND, age=3 * SECOND),
        _split(native),
    )
    assert len({r.anchor_id for r in evidence.rows}) == 1
    assert all(r.weight == Fraction(1, 36) for r in evidence.rows)
    assert _fit(evidence).selected_mass == Fraction(1, 12)


def test_normalized_macro_grid_is_positive_without_historical_spine_claim(
    tmp_path,
):
    join = vintage_join(join_fixture(tmp_path, matrix=True))
    evidence = inspect_training_preprocessing_native(
        _grid(join, BASE + SECOND, BASE + 4 * SECOND), _split(join)
    )
    selected = _fit(
        evidence, fit_mode="rolling_pti", cutoff_ns=BASE + 2 * SECOND
    )
    assert len(selected.rows) == 2
    assert selected.weights == (Fraction(1, 2), Fraction(1, 2))
    assert selected.mass_policy == ROLLING_MASS_POLICY
    assert (
        selected.availability_scope
        == "normalized-added-feature-clocks-only-expost-spine"
    )
    assert not selected.historical_availability_verified
    assert all(r.native_row.available_at_ns is None for r in selected.rows)
    assert selected.rows[0].cells[0].original_value == 1.0
    assert selected.rows[1].cells[0].original_value == 1.0


def test_normalized_prefix_is_independent_of_later_grid_coordinates(tmp_path):
    join = vintage_join(join_fixture(tmp_path, matrix=True))
    short = inspect_training_preprocessing_native(
        _grid(join, BASE + SECOND, BASE + 3 * SECOND), _split(join)
    )
    long = inspect_training_preprocessing_native(
        _grid(join, BASE + SECOND, BASE + 5 * SECOND), _split(join)
    )
    a = _fit(short, fit_mode="rolling_pti", cutoff_ns=BASE + 2 * SECOND)
    b = _fit(long, fit_mode="rolling_pti", cutoff_ns=BASE + 2 * SECOND)
    assert a.weights == b.weights
    assert a.parameter_fit_identity == b.parameter_fit_identity


def test_raw_quote_grid_is_not_normalized_feature_authority(native):
    evidence = inspect_training_preprocessing_native(
        _grid(native, BASE + SECOND, BASE + 3 * SECOND), _split(native)
    )
    with pytest.raises(ValueError, match="clocked added features"):
        _fit(evidence, fit_mode="rolling_pti", cutoff_ns=BASE + 2 * SECOND)


@pytest.fixture(scope="module")
def normalized_boundary(tmp_path_factory):
    join = vintage_join(
        join_fixture(
            tmp_path_factory.mktemp("preprocessing-normalized-boundary"),
            matrix=True,
        )
    )
    # At midnight on the next ownership day, the last known native quote is
    # still on the TRAIN day. No fabricated quote is added at the grid cutoff.
    clock = BASE + DAY_NS
    plan = _grid(join, clock, clock + SECOND, age=DAY_NS)
    return plan, _split(join)


@pytest.mark.parametrize("fit_mode", ["rolling_pti", "fixed_external"])
@pytest.mark.parametrize(
    "next_partition", [TemporalPartition.TRAIN, TemporalPartition.VALIDATION]
)
def test_normalized_grid_cannot_cross_native_evidence_ownership(
    normalized_boundary, fit_mode, next_partition
):
    plan, split = normalized_boundary
    ownership = plan.tiles[0].join_plan.spine.ownership
    clock = BASE + DAY_NS
    next_unit = next(u for u in ownership.units if u.start_ns == clock)
    split = replace(
        split,
        assignments=tuple(
            (
                replace(a, partition=next_partition)
                if a.evidence_unit_id == next_unit.artifact_id
                else a
            )
            for a in split.assignments
        ),
    )
    evidence = inspect_training_preprocessing_native(plan, split)
    assert len(evidence.rows) == 1
    row = evidence.rows[0]
    assert row.native_row is not None
    assert row.native_row.decision_time_ns == clock
    assert row.native_row.event_time_ns < clock
    assert row.partition is TemporalPartition.TRAIN
    assert row.evidence_unit_id != next_unit.artifact_id
    assert row.cells[0].provenance is not None
    with pytest.raises(ValueError, match="crosses native evidence ownership"):
        _fit(evidence, fit_mode=fit_mode, cutoff_ns=clock)


def test_future_cross_unit_grid_is_excluded_before_fit_admission(
    normalized_boundary,
):
    plan, split = normalized_boundary
    evidence = inspect_training_preprocessing_native(plan, split)
    selected = _fit(
        evidence, fit_mode="rolling_pti", cutoff_ns=BASE + 2 * SECOND
    )
    assert selected.rows == ()
    assert selected.weights == ()
    assert selected.excluded_rows == (
        (evidence.rows[0].wide_row_id, "after_fit_cutoff"),
    )


def test_equal_member_mass_distinguishes_same_label_in_two_actual_runs(
    tmp_path,
):
    from histdatacom.data_quality.training_join_contracts import (
        JoinFamily,
        TrainingJoinSourceV1,
    )
    from tests.fixtures.training_substrate_v1 import (
        observed_source,
        published_product,
        with_products,
    )

    source, version = observed_source(tmp_path / "source")
    a, _ = published_product(
        tmp_path / "a", version, member="same-label", seed=10
    )
    b, _ = published_product(
        tmp_path / "b", version, member="same-label", seed=20
    )
    assert a.manifest.run_id != b.manifest.run_id
    source = with_products(source, a, b)
    ownership = build_training_ownership(source)
    prototype = join_fixture(tmp_path / "prototype")
    spines = []
    for product in (a, b):
        spine = materialize_training_rows(
            source,
            ownership,
            TrainingRequestV1(
                TrainingConsumerMode.DESCRIPTIVE,
                BASE,
                BASE + 4 * SECOND,
                ("EURUSD",),
                product_manifest_id=product.manifest.manifest_id,
            ),
        )
        quotes = TrainingJoinSourceV1(
            "quotes",
            JoinFamily.TICK,
            configuration_json=training_json(
                {
                    "product_manifest_id": product.manifest.manifest_id,
                    "ensemble_member_id": "same-label",
                }
            ),
        )
        spines.append(replace(prototype, spine=spine, sources=(quotes,)))
    plan = build_training_wide_plan(
        tuple(spines), grain=TrainingWideGrainV1(WideRowGrain.PANEL)
    )
    evidence = inspect_training_preprocessing_native(plan, _split(spines[0]))
    assert len({row.member_key for row in evidence.rows}) == 2
    assert len({row.stratum_id for row in evidence.rows}) == 1
    native_denominators = [
        d for d in evidence.denominators if d.selected_logical_row_count
    ]
    assert len(native_denominators) == 2
    assert all(d.member_mass == Fraction(1, 2) for d in native_denominators)
    assert all(d.complete_row_count == 9 for d in native_denominators)
    assert _fit(evidence).selected_mass == Fraction(1, 3)
