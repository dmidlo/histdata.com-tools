"""Real synthetic member/event identities at explicit wide row grains."""

from dataclasses import replace

import pytest

from histdatacom.data_quality.training_contracts import (
    DAY_NS,
    TrainingConsumerMode,
    TrainingRequestV1,
    training_json,
)
from histdatacom.data_quality.training_join_contracts import (
    JoinFamily,
    JoinInformationMode,
    TrainingJoinEntityV1,
    TrainingJoinPlanV1,
    TrainingJoinSourceV1,
)
from histdatacom.data_quality.training_lineage import build_training_ownership
from histdatacom.data_quality.training_views import materialize_training_rows
from histdatacom.data_quality.training_wide_contracts import (
    TrainingWideGrainV1,
    WideRowGrain,
)
from histdatacom.data_quality.training_wide_views import (
    build_training_wide_plan,
    materialize_training_wide_view,
    project_training_wide_view,
)
from tests.fixtures.training_join_v1 import bound_column, join_fixture
from tests.fixtures.training_substrate_v1 import (
    BASE,
    observed_source,
    published_product,
    with_products,
)
from tests.fixtures.training_temporal_v1 import SECOND


def test_panel_keeps_two_real_members_and_shared_evidence_unit_without_average(
    tmp_path,
):
    source, version = observed_source(tmp_path / "source")
    a, _ = published_product(tmp_path / "member-a", version)
    b, _ = published_product(
        tmp_path / "member-b", version, member="member-b", seed=653
    )
    source = with_products(source, a, b)
    ownership = build_training_ownership(source)
    binding = TrainingJoinSourceV1(
        "quotes",
        JoinFamily.TICK,
        configuration_json=training_json(
            {"product_manifest_id": None, "ensemble_member_id": None}
        ),
    )
    column = bound_column(
        binding, "market.tick.EURUSD.bid", TrainingJoinEntityV1("EURUSD"), "bid"
    )
    plans = []
    for product in (a, b):
        spine = materialize_training_rows(
            source,
            ownership,
            TrainingRequestV1(
                TrainingConsumerMode.DESCRIPTIVE,
                BASE,
                BASE + DAY_NS,
                ("EURUSD",),
                product_manifest_id=product.manifest.manifest_id,
            ),
        )
        plans.append(
            TrainingJoinPlanV1(
                spine, JoinInformationMode.EX_POST, (binding,), (column,)
            )
        )
    panel = materialize_training_wide_view(
        build_training_wide_plan(
            tuple(plans), grain=TrainingWideGrainV1(WideRowGrain.PANEL)
        )
    )
    records = project_training_wide_view(panel)
    assert len(records) == sum(len(p.spine.rows) for p in plans) == 6
    assert {r["spine"]["ensemble_member_id"] for r in records} == {
        "member-a",
        "member-b",
    }
    assert len({r["spine"]["evidence_unit_id"] for r in records}) == 1
    actual = {r["spine"]["artifact_id"] for r in records}
    assert actual == {r.artifact_id for p in plans for r in p.spine.rows}
    originals = {
        row.artifact_id: row.to_dict()
        for native_plan in plans
        for row in native_plan.spine.rows
    }
    assert all(
        record["spine"] == originals[record["spine"]["artifact_id"]]
        for record in records
    )
    assert any(
        r["spine"]["origin"] == "synthetic_reconstruction" for r in records
    )
    with pytest.raises(ValueError, match="explicit panel"):
        build_training_wide_plan(
            tuple(plans), grain=TrainingWideGrainV1(WideRowGrain.EVENT)
        )


def test_anchor_bar_clock_retains_prior_real_tick_without_making_bar_event(
    tmp_path,
):
    native = join_fixture(tmp_path)
    minute = 60 * SECOND
    cutoff = BASE + minute
    grain = TrainingWideGrainV1(
        WideRowGrain.BAR, cutoff, cutoff + minute, minute, minute, "1m"
    )
    request = replace(
        native.spine.request,
        start_ns=BASE,
        end_ns=cutoff + 1,
        decision_time_ns=cutoff,
    )
    spine = materialize_training_rows(
        native.spine.source, native.spine.ownership, request
    )
    view = materialize_training_wide_view(
        build_training_wide_plan((replace(native, spine=spine),), grain=grain)
    )
    result = project_training_wide_view(view)
    assert len(result) == 1 and len(view.controls[0].plan.spine.rows) == 4
    assert result[0]["grid_cutoff_ns"] == cutoff
    assert result[0]["spine"]["event_time_ns"] == BASE + 4 * SECOND
    assert result[0]["spine"]["decision_time_ns"] == cutoff
    assert result[0]["row_grain"] == "anchor_bar_clock"
    assert (
        result[0]["values"][native.columns[0].name] is None
    )  # EXACT cutoff is not a hidden forward fill.
    with pytest.raises(ValueError, match="standard UTC width"):
        replace(grain, anchor_timeframe="5m")


@pytest.mark.parametrize(
    "changes",
    (
        {"grid_step_ns": 0},
        {"grid_start_ns": BASE + 1},
        {"grid_end_ns": BASE + 17 * SECOND},
        {"max_anchor_age_ns": -1},
    ),
)
def test_grid_geometry_resource_and_alignment_refusals(changes):
    values = {
        "grid_start_ns": BASE,
        "grid_end_ns": BASE + SECOND,
        "grid_step_ns": SECOND,
        "max_anchor_age_ns": 0,
    }
    values.update(changes)
    with pytest.raises(ValueError):
        TrainingWideGrainV1(WideRowGrain.GRID, **values)
