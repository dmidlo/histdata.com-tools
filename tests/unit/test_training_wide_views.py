"""Synthetic source-replayed wide geometry, not a historical corpus claim."""

from dataclasses import replace

import pytest

from histdatacom.data_quality.training_contracts import (
    training_load,
)
from histdatacom.data_quality.training_views import materialize_training_rows
from histdatacom.data_quality.training_wide_contracts import (
    TrainingWideGrainV1,
    TrainingWidePlanV1,
    WideRowGrain,
)
from histdatacom.data_quality.training_wide_views import (
    build_training_wide_plan,
    diagnose_training_wide_view,
    materialize_training_wide_view,
    project_training_wide_view,
    replay_training_wide_view,
)
from tests.fixtures.training_join_v1 import join_fixture
from tests.fixtures.training_substrate_v1 import BASE
from tests.fixtures.training_temporal_v1 import SECOND


def wide_plan(native, *, width=4, tile_width=2, grain=None):
    columns = tuple(
        replace(native.columns[0], name=f"market.tick.EURUSD.c{i:04d}")
        for i in range(width)
    )
    plans = tuple(
        replace(native, columns=columns[i : i + tile_width])
        for i in range(0, width, tile_width)
    )
    return build_training_wide_plan(
        plans, grain=grain or TrainingWideGrainV1(WideRowGrain.EVENT)
    )


@pytest.fixture(scope="module")
def native(tmp_path_factory):
    return join_fixture(tmp_path_factory.mktemp("wide-synthetic-source"))


@pytest.fixture(scope="module")
def view(native):
    return materialize_training_wide_view(wide_plan(native))


def test_real_event_rows_and_origin_are_not_multiplied(native, view):
    assert TrainingWidePlanV1.from_json(
        wide_plan(native).to_json()
    ) == wide_plan(native)
    result = project_training_wide_view(view)
    assert len(result) == len(native.spine.rows) == 3
    for row, original in zip(result, native.spine.rows):
        assert row["spine"] == original.to_dict()
        assert row["grid_cutoff_ns"] is None
        assert len(row["values"]) == 4
        assert set(row["values"].values()) == {
            training_load(original.value_json)["bid"]
        }
        assert all(c["source_ids"] for c in row["cell_provenance"].values())
    assert replay_training_wide_view(view) == view


def test_schema_and_logical_content_are_tile_partition_invariant(native, view):
    one = materialize_training_wide_view(wide_plan(native, tile_width=4))
    assert one.manifest.plan_id != view.manifest.plan_id
    assert one.manifest.schema_sha256 == view.manifest.schema_sha256
    assert one.manifest.content_sha256 == view.manifest.content_sha256
    assert project_training_wide_view(one) == project_training_wide_view(view)


def test_projection_preserves_rows_and_canonical_column_order(view):
    names = tuple(c.column.name for c in view.manifest.columns)
    result = project_training_wide_view(view, columns=(names[-1], names[0]))
    assert tuple(result[0]["values"]) == (names[0], names[-1])
    empty = project_training_wide_view(view, columns=())
    assert len(empty) == len(result)
    assert all(r["values"] == {} and r["spine"] for r in empty)
    for invalid in ([], (names[0], names[0]), ("invented",)):
        with pytest.raises(ValueError, match="projection"):
            project_training_wide_view(view, columns=invalid)


def test_all_diagnostic_blocks_are_source_bound_not_global_rank(view):
    reports = diagnose_training_wide_view(view)
    assert len(reports) == 1
    report = reports[0]
    assert report["scope"] == "this_declared_block_only_not_global_rank"
    assert report["content_sha256"] == view.manifest.content_sha256
    assert report["report"]["row_count"] == len(view.manifest.rows)
    assert report["report"]["participation_ratio_rank"] == pytest.approx(1.0)


@pytest.mark.parametrize(
    "mutation", ("row", "value", "schema", "missing_group")
)
def test_resealed_native_or_layout_mutation_refuses(view, mutation):
    if mutation == "row":
        bad = replace(
            view, manifest=replace(view.manifest, rows=view.manifest.rows[:-1])
        )
    elif mutation == "schema":
        bad = replace(
            view, manifest=replace(view.manifest, content_sha256="a" * 64)
        )
    elif mutation == "missing_group":
        bad = replace(view, groups=view.groups[:-1])
    else:
        group = view.groups[0]
        row = group.rows[0]
        value = replace(row.values[0], value_json='{"value":42.0}')
        group = replace(
            group,
            rows=(replace(row, values=(value,) + row.values[1:]),)
            + group.rows[1:],
        )
        bad = replace(view, groups=(group,) + view.groups[1:])
    with pytest.raises(ValueError, match="differs"):
        project_training_wide_view(bad)


def test_sliced_native_spine_is_not_a_wide_row_partition(native):
    sliced = replace(
        native, spine=replace(native.spine, rows=native.spine.rows[:1])
    )
    with pytest.raises(ValueError, match="complete canonical source replay"):
        wide_plan(sliced)


def test_grid_anchors_retain_actual_events_and_explicit_vacant_coordinates(
    native,
):
    grain = TrainingWideGrainV1(
        WideRowGrain.GRID, BASE, BASE + 4 * SECOND, SECOND, 0
    )
    plans = []
    for cutoff in grain.cutoffs:
        request = replace(
            native.spine.request,
            start_ns=cutoff,
            end_ns=cutoff + 1,
            decision_time_ns=cutoff,
        )
        spine = materialize_training_rows(
            native.spine.source, native.spine.ownership, request
        )
        plans.append(replace(native, spine=spine))
    view = materialize_training_wide_view(
        build_training_wide_plan(tuple(plans), grain=grain)
    )
    records = project_training_wide_view(view)
    assert len(records) == 4
    assert records[0]["row_state"] == "missing_spine_anchor"
    assert records[0]["spine"] is None
    assert records[0]["values"] == {native.columns[0].name: None}
    for row in records[1:]:
        assert row["spine"]["event_time_ns"] == row["grid_cutoff_ns"]
        assert row["spine"]["decision_time_ns"] == row["grid_cutoff_ns"]


def test_grid_lookback_is_full_request_not_sliced_evidence(native):
    cutoff = BASE + 3 * SECOND
    grain = TrainingWideGrainV1(
        WideRowGrain.GRID, cutoff, cutoff + SECOND, SECOND, 2 * SECOND
    )
    spine = materialize_training_rows(
        native.spine.source,
        native.spine.ownership,
        replace(
            native.spine.request, end_ns=cutoff + 1, decision_time_ns=cutoff
        ),
    )
    result = materialize_training_wide_view(
        build_training_wide_plan((replace(native, spine=spine),), grain=grain)
    )
    row = project_training_wide_view(result)[0]
    assert len(result.controls[0].plan.spine.rows) == 3
    assert row["spine"]["event_time_ns"] == cutoff
    wrong = replace(spine.request, start_ns=cutoff)
    narrowed = materialize_training_rows(spine.source, spine.ownership, wrong)
    plan = build_training_wide_plan(
        (replace(native, spine=narrowed),), grain=grain
    )
    with pytest.raises(ValueError, match="complete bounded lookup"):
        materialize_training_wide_view(plan)


@pytest.mark.parametrize(
    "changes", ({"anchor_timeframe": ""}, {"grid_step_ns": SECOND})
)
def test_event_grain_rejects_dormant_grid_metadata(changes):
    with pytest.raises(ValueError, match="invent grid"):
        TrainingWideGrainV1(WideRowGrain.EVENT, **changes)


def test_two_thousand_logical_columns_keep_native_128_limit_and_honest_aliases(
    native,
):
    plan = wide_plan(native, width=2048, tile_width=128)
    assert len(plan.columns) == 2048
    assert all(len(tile.join_plan.columns) == 128 for tile in plan.tiles)
    view = materialize_training_wide_view(plan)
    records = project_training_wide_view(
        view, columns=(plan.columns[-1].column.name,)
    )
    assert len(records) == 3 and len(records[0]["values"]) == 1
    assert (
        "logical_width_is_not_independent_information"
        in view.manifest.nonclaims
    )
    with pytest.raises(ValueError, match="budget"):
        wide_plan(native, width=129, tile_width=129)


def test_duplicate_rectangle_and_schema_relabeling_refuse(native):
    plan = wide_plan(native)
    with pytest.raises(ValueError, match="unique coordinates"):
        replace(plan, tiles=(plan.tiles[0],) + plan.tiles)
    with pytest.raises(ValueError, match="missing/duplicate"):
        replace(plan, tiles=plan.tiles[:-1])
    definitions = (
        replace(plan.columns[0], units="invented-units"),
    ) + plan.columns[1:]
    forged = replace(plan, columns=definitions)
    with pytest.raises(ValueError, match="source-derived"):
        materialize_training_wide_view(forged)


def test_empty_source_still_has_registry_and_native_controls(tmp_path):
    view = materialize_training_wide_view(
        wide_plan(join_fixture(tmp_path, empty=True))
    )
    assert not view.manifest.rows
    assert len(view.controls) == 1 and not view.controls[0].rows
    assert len(view.manifest.columns) == 4
    assert project_training_wide_view(view) == ()
