"""Actual606 spine replay, numerical selection and durable null evidence."""

from dataclasses import replace

import pytest

from histdatacom.data_quality.training_contracts import (
    training_json,
    training_load,
)
from histdatacom.data_quality.training_join_contracts import (
    JoinDirection,
    JoinFamily,
    JoinInformationMode,
    JoinMeaning,
    JoinState,
    TrainingJoinBatchV1,
    TrainingJoinColumnV1,
    TrainingJoinEntityV1,
    TrainingJoinSourceV1,
)
from histdatacom.data_quality.training_join_views import (
    materialize_training_joins,
    replay_training_joins,
    training_join_records,
)
from tests.fixtures.training_join_v1 import join_fixture, vintage_join
from tests.fixtures.training_substrate_v1 import BASE
from tests.fixtures.training_temporal_v1 import SECOND


def test_source_bound_quote_join_preserves_exact_spine_rows(tmp_path):
    plan = join_fixture(tmp_path)
    result = materialize_training_joins(plan)
    assert len(result.rows) == len(plan.spine.rows) == 3
    assert tuple(r.spine_row_id for r in result.rows) == tuple(
        r.artifact_id for r in plan.spine.rows
    )
    assert [
        training_load(r.values[0].value_json)["value"] for r in result.rows
    ] == pytest.approx([0.9999, 1.0099, 1.0199])
    assert all(r.values[0].available_at_ns is None for r in result.rows)
    assert TrainingJoinBatchV1.from_json(result.to_json()) == result
    assert replay_training_joins(result) == result
    assert len(training_join_records(result)) == 3


def test_unknown_legacy_clock_never_promotes_entire_spine(tmp_path):
    plan = replace(
        join_fixture(tmp_path),
        information_mode=JoinInformationMode.NORMALIZED_AS_OF,
    )
    result = materialize_training_joins(plan)
    for row in result.rows:
        value = row.values[0]
        assert value.state is JoinState.UNKNOWN_AVAILABILITY
        assert value.value_json == '{"value":null}'
        assert len(value.refusal_evidence) == 1
        native = value.refusal_evidence[0]
        assert native.source_time_ns == row.decision_time_ns
        assert native.parent_source_ids
        assert native.available_at_ns is None
        assert "value" not in native.payload()
    assert all(r.available_at_ns is None for r in result.plan.spine.rows)


def test_prior_and_age_boundary_never_select_future_quote(tmp_path):
    plan = join_fixture(tmp_path, decision=BASE + 2500_000_000)
    column = replace(
        plan.columns[0],
        direction=JoinDirection.BOUNDED_PRIOR,
        max_age_ns=500_000_000,
    )
    result = materialize_training_joins(replace(plan, columns=(column,)))
    assert all(
        training_load(r.values[0].value_json)["value"] == pytest.approx(1.0099)
        for r in result.rows
    )
    stale = materialize_training_joins(
        replace(plan, columns=(replace(column, max_age_ns=499_999_999),))
    )
    assert all(r.values[0].state is JoinState.STALE for r in stale.rows)
    assert (
        stale.rows[0].values[0].refusal_evidence[0].source_time_ns
        == BASE + 2 * SECOND
    )


def test_duplicate_clock_uses_actual_last_physical_ordinal(tmp_path):
    plan = join_fixture(tmp_path, duplicate=True)
    column = replace(
        plan.columns[0], name="market.tick.EURUSD.midpoint", field="midpoint"
    )
    result = materialize_training_joins(replace(plan, columns=(column,)))
    assert len(result.rows) == 4  # Source rows remain distinct, no collapse.
    assert training_load(result.rows[0].values[0].value_json)["value"] == 1.0
    assert (
        result.rows[0].values[0].source_ids
        == result.rows[1].values[0].source_ids
    )


def test_future_revision_is_recomputed_per_cutoff(tmp_path):
    plan = vintage_join(join_fixture(tmp_path, matrix=True))
    result = materialize_training_joins(plan)
    assert [
        training_load(r.values[0].value_json)["value"] for r in result.rows
    ] == [1.0, 1.0, 9.0]
    assert (
        result.rows[0].values[0].source_ids
        != result.rows[2].values[0].source_ids
    )
    assert all(
        v.available_at_ns <= r.decision_time_ns
        for r in result.rows
        for v in r.values
    )
    assert all(
        not v.historical_availability_verified
        for r in result.rows
        for v in r.values
    )
    assert replay_training_joins(result) == result


@pytest.mark.parametrize(
    "meaning", [JoinMeaning.EVENT, JoinMeaning.SNAPSHOT, JoinMeaning.CLOSED_BAR]
)
def test_no_forward_fill_for_nonpersistent_family(meaning):
    with pytest.raises(ValueError, match="fill|closed-bar"):
        TrainingJoinColumnV1(
            "calendar.event",
            "source",
            TrainingJoinEntityV1("EURUSD"),
            "occurrence",
            JoinDirection.PRIOR,
            meaning,
            SECOND,
        )


def test_future_reserved_family_is_explicit_null_not_invented_flow(tmp_path):
    plan = join_fixture(tmp_path)
    source = TrainingJoinSourceV1("flow", JoinFamily.UNSUPPORTED)
    column = TrainingJoinColumnV1(
        "synthetic_flow.arrivals",
        "flow",
        TrainingJoinEntityV1("EURUSD"),
        "arrivals",
        JoinDirection.EXACT,
        JoinMeaning.EVENT,
        0,
    )
    result = materialize_training_joins(
        replace(plan, sources=(source,), columns=(column,))
    )
    assert all(r.values[0].state is JoinState.UNSUPPORTED for r in result.rows)
    assert all(r.values[0].value_json == '{"value":null}' for r in result.rows)


def test_missing_projection_still_replays_entire_spine(tmp_path):
    plan = join_fixture(tmp_path, empty=True)
    assert not plan.spine.rows
    assert not materialize_training_joins(replace(plan, columns=())).rows
    forged = replace(
        plan,
        spine=replace(
            plan.spine,
            ownership=replace(
                plan.spine.ownership,
                units=(
                    replace(
                        plan.spine.ownership.units[0],
                        anchor_content_sha256="b" * 64,
                    ),
                )
                + plan.spine.ownership.units[1:],
            ),
        ),
    )
    with pytest.raises(ValueError, match="ownership"):
        materialize_training_joins(replace(forged, columns=()))


def test_unknown_field_refuses_even_empty_rows(tmp_path):
    plan = join_fixture(tmp_path, empty=True)
    with pytest.raises(ValueError, match="unknown native quote"):
        materialize_training_joins(
            replace(
                plan, columns=(replace(plan.columns[0], field="fake_volume"),)
            )
        )


def test_duplicate_names_and_sources_refuse_before_replay(tmp_path):
    plan = join_fixture(tmp_path)
    with pytest.raises(ValueError, match="duplicate"):
        replace(plan, columns=plan.columns * 2)
    with pytest.raises(ValueError, match="duplicate"):
        replace(plan, sources=plan.sources * 2)


def test_correctly_resealed_false_value_or_null_fails_reexecution(tmp_path):
    batch = materialize_training_joins(join_fixture(tmp_path))
    first = batch.rows[0]
    forged = replace(
        batch,
        rows=(
            replace(
                first,
                values=(
                    replace(
                        first.values[0],
                        value_json=training_json({"value": 42.0}),
                    ),
                ),
            ),
        )
        + batch.rows[1:],
    )
    assert TrainingJoinBatchV1.from_json(forged.to_json()) == forged
    assert forged.artifact_id != batch.artifact_id
    with pytest.raises(ValueError, match="differ from source replay"):
        replay_training_joins(forged)
