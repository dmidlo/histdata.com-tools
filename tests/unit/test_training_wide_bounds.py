"""Bounded new layout structure, including a valid native maximal lag."""

from dataclasses import replace

import pytest

from histdatacom.data_quality import training_wide_contracts as contracts
from histdatacom.data_quality.training_join_contracts import (
    JoinFamily,
    JoinInformationMode,
    JoinMeaning,
    TrainingJoinEntityV1,
    TrainingJoinPlanV1,
    TrainingJoinSourceV1,
)
from histdatacom.data_quality.training_wide_contracts import (
    TrainingWideGrainV1,
    WideRowGrain,
)
from histdatacom.data_quality.training_wide_views import (
    build_training_wide_plan,
    materialize_training_wide_view,
    project_training_wide_view,
)
from histdatacom.forecasting.feature_artifacts import write_feature_artifact
from histdatacom.forecasting.feature_contracts import (
    FeatureColumnV1,
    FeatureRequestV1,
    FeatureTransformKind,
    FeatureTransformV1,
)
from tests.fixtures.forecast_feature_store_v1 import (
    CUTOFF,
    PERIODS,
    store_fixture,
)
from tests.fixtures.training_join_v1 import (
    bound_column,
    controlled_spine,
    join_fixture,
)


def test_maximal_native_lag_can_remain_explicitly_warmup(tmp_path):
    transform = FeatureTransformV1(FeatureTransformKind.LAG, window=10000)
    matrix = store_fixture().snapshot(
        FeatureRequestV1(
            CUTOFF, PERIODS, (FeatureColumnV1("macro", "macro.cpi", transform),)
        )
    )
    path = write_feature_artifact(matrix, tmp_path / "macro")
    source = TrainingJoinSourceV1(
        "macro", JoinFamily.VINTAGE, paths=(str(path),)
    )
    column = bound_column(
        source,
        "calendar.macro.lag",
        TrainingJoinEntityV1("EURUSD", series="macro.cpi"),
        "macro",
        coordinate=PERIODS[-1].label,
        meaning=JoinMeaning.SNAPSHOT,
    )
    spine = controlled_spine(tmp_path / "spine", (CUTOFF,), derived=(path,))
    plan = TrainingJoinPlanV1(
        spine, JoinInformationMode.NORMALIZED_AS_OF, (source,), (column,)
    )
    wide = build_training_wide_plan(
        (plan,), grain=TrainingWideGrainV1(WideRowGrain.EVENT)
    )
    assert wide.columns[0].lag_steps == 10000
    assert wide.columns[0].warmup_steps == 10001
    result = project_training_wide_view(materialize_training_wide_view(wide))[0]
    assert result["values"][column.name] is None
    assert result["states"][column.name] == "insufficient_warmup"
    with pytest.raises(ValueError, match="unbounded"):
        replace(wide.columns[0], warmup_steps=10002)


@pytest.fixture(scope="module")
def plan(tmp_path_factory):
    native = join_fixture(tmp_path_factory.mktemp("wide-budget-source"))
    return build_training_wide_plan(
        (native,), grain=TrainingWideGrainV1(WideRowGrain.EVENT)
    )


@pytest.mark.parametrize(
    "name,value",
    (
        ("MAX_WIDE_COLUMNS", 0),
        ("MAX_WIDE_SPINES", 0),
        ("MAX_WIDE_TILES", 0),
        ("MAX_WIDE_ROWS", 2),
        ("MAX_WIDE_CELLS", 2),
    ),
)
def test_wide_dimension_budgets_refuse_without_source_execution(
    plan, monkeypatch, name, value
):
    from histdatacom.data_quality import training_wide_views as views

    def forbidden(*args, **kwargs):
        pytest.fail("invalid wide dimensions reached native execution")

    monkeypatch.setattr(views, "materialize_training_joins", forbidden)
    monkeypatch.setattr(contracts, name, value)
    with pytest.raises(ValueError, match="bound|budget"):
        replace(plan)


@pytest.mark.parametrize(
    "change",
    (
        {"byte_count": True},
        {"byte_count": 0},
        {"byte_count": 8 * 1024 * 1024 + 1},
        {"file_sha256": "../not-a-digest"},
    ),
)
def test_child_exact_scalar_and_native_envelope_bounds(change):
    fields = {
        "tile_id": "native-plan",
        "batch_id": "native-batch",
        "file_sha256": "a" * 64,
        "byte_count": 1,
    }
    fields.update(change)
    with pytest.raises(ValueError):
        contracts.TrainingWideChildV1(**fields)


def test_wire_unknown_duplicate_and_nonfinite_fields_refuse():
    grain = TrainingWideGrainV1(WideRowGrain.EVENT)
    payload = grain.to_dict()
    payload["future_permission"] = "allow"
    with pytest.raises(ValueError, match="unknown"):
        TrainingWideGrainV1.from_dict(payload)
    with pytest.raises(ValueError):
        TrainingWideGrainV1.from_json(
            '{"kind":"event_clock","kind":"member_scenario_panel"}'
        )
    with pytest.raises(ValueError):
        TrainingWideGrainV1.from_json('{"grid_step_ns":NaN}')


@pytest.mark.parametrize(
    "mutation",
    (
        "duplicate",
        "empty",
        "sources",
        "columns",
        "spines",
        "rows",
        "cells",
        "envelope",
    ),
)
def test_public_builder_preflights_geometry_before_any_source_execution(
    plan, monkeypatch, mutation
):
    from histdatacom.data_quality import training_wide_views as views

    native = plan.tiles[0].join_plan
    inputs = (native,)
    if mutation == "duplicate":
        inputs = (native, native)
    elif mutation == "empty":
        inputs = (replace(native, columns=()),)
    elif mutation == "sources":
        other = replace(native.columns[0], name="market.tick.EURUSD.other")
        extra = TrainingJoinSourceV1("unused", JoinFamily.UNSUPPORTED)
        inputs = (
            native,
            replace(
                native, columns=(other,), sources=native.sources + (extra,)
            ),
        )
    else:
        attribute = {
            "columns": "MAX_WIDE_COLUMNS",
            "spines": "MAX_WIDE_SPINES",
            "rows": "MAX_WIDE_ROWS",
            "cells": "MAX_WIDE_CELLS",
            "envelope": "MAX_TRAINING_BYTES",
        }[mutation]
        monkeypatch.setattr(contracts, attribute, 0)

    def forbidden(*args, **kwargs):
        pytest.fail(
            "structurally inadmissible builder reached native execution"
        )

    monkeypatch.setattr(views, "materialize_training_joins", forbidden)
    with pytest.raises(ValueError):
        views.build_training_wide_plan(inputs, grain=plan.grain)


@pytest.mark.parametrize("field", ("bid", "ask", "midpoint", "spread"))
def test_native_tick_descriptor_uses_exact_quote_field_units(plan, field):
    from histdatacom.data_quality.training_wide_schema import (
        describe_training_wide_column,
    )

    native = plan.tiles[0].join_plan
    definition = describe_training_wide_column(
        native.sources[0],
        replace(native.columns[0], field=field),
        information_mode=native.information_mode,
    )
    assert definition.units == "quote_currency_per_base_currency"
    assert definition.dtype.value == "finite_number"


@pytest.mark.parametrize(
    "field,coordinate",
    (
        pytest.param("not_native", "", id="unsupported_field"),
        pytest.param("bid", "opaque", id="unexpected_coordinate"),
    ),
)
def test_tick_declaration_and_empty_native_spine_refuse_unknown_coordinates(
    tmp_path, field, coordinate
):
    from histdatacom.data_quality.training_join_views import (
        materialize_training_joins,
    )
    from histdatacom.data_quality.training_wide_schema import (
        describe_training_wide_column,
    )

    native = join_fixture(tmp_path, empty=True)
    assert not native.spine.rows
    column = replace(native.columns[0], field=field, coordinate=coordinate)
    # Native source preparation already rejects this on an empty spine. The
    # additive descriptor must not invent units even outside materialization.
    with pytest.raises(ValueError, match="unknown native quote"):
        materialize_training_joins(replace(native, columns=(column,)))
    with pytest.raises(ValueError, match="unknown native quote"):
        describe_training_wide_column(
            native.sources[0],
            column,
            information_mode=native.information_mode,
        )
