"""Independent synthetic native-unit and explicit-null descriptor regressions."""

import json
from dataclasses import replace

import pytest

from histdatacom.data_quality.training_join_contracts import (
    JoinDirection,
    JoinFamily,
    JoinInformationMode,
    JoinMeaning,
    TrainingJoinEntityV1,
    TrainingJoinPlanV1,
    TrainingJoinSourceV1,
)
from histdatacom.data_quality.training_join_views import (
    materialize_training_joins,
)
from histdatacom.data_quality.training_wide_contracts import (
    TrainingWideGrainV1,
    WideRowGrain,
    WideValueType,
)
from histdatacom.data_quality.training_wide_schema import (
    describe_training_wide_column,
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
from histdatacom.market_context.economic_calendar import (
    write_economic_calendar_corpus,
)
from tests.fixtures.forecast_contracts_v1 import RELEASE_TIME, calendar_fixture
from tests.fixtures.forecast_feature_store_v1 import (
    CUTOFF,
    PERIODS,
    store_fixture,
)
from tests.fixtures.training_join_v1 import bound_column, controlled_spine
from tests.fixtures.training_temporal_v1 import SECOND


def _describe(source, column):
    return describe_training_wide_column(
        source, column, information_mode=JoinInformationMode.NORMALIZED_AS_OF
    )


def _has_exact_object(value, expected):
    if value == expected:
        return True
    if isinstance(value, dict):
        return any(_has_exact_object(v, expected) for v in value.values())
    if isinstance(value, list):
        return any(_has_exact_object(v, expected) for v in value)
    return False


@pytest.mark.parametrize(
    "timeframe", ["1m", "5m", "15m", "30m", "1h", "4h", "1d"]
)
def test_bar_descriptor_preserves_actual_native_units_and_warmup(timeframe):
    from histdatacom.synthetic.bar_features import bar_feature_definitions

    source = TrainingJoinSourceV1("bars", JoinFamily.BAR)
    for native in bar_feature_definitions():
        column = bound_column(
            source,
            f"market.bar.EURUSD.observed.{timeframe}.{native.name}",
            TrainingJoinEntityV1("EURUSD"),
            native.name,
            coordinate=f"observed:{timeframe}",
            meaning=JoinMeaning.CLOSED_BAR,
        )
        definition = _describe(source, column)
        assert definition.units == native.units
        assert definition.lag_steps == native.lag_bars
        assert definition.warmup_steps == native.required_closed_bars
        assert definition.step_unit == "closed_bars"
        assert definition.timeframe == timeframe
        assert native.artifact_id in definition.native_semantics_json


@pytest.mark.parametrize(
    "kind,unit",
    [
        (FeatureTransformKind.LEVEL, "percent_mom"),
        (FeatureTransformKind.LAG, "percent_mom"),
        (FeatureTransformKind.ROLLING_ZSCORE, "standard-deviations"),
    ],
)
def test_vintage_descriptor_binds_transformed_not_raw_observation_semantics(
    tmp_path, kind, unit
):
    transform = (
        FeatureTransformV1(kind, window=2, min_support=2)
        if kind is FeatureTransformKind.ROLLING_ZSCORE
        else FeatureTransformV1(kind)
    )
    native = FeatureColumnV1("macro", "macro.cpi", transform)
    matrix = store_fixture().snapshot(
        FeatureRequestV1(CUTOFF, PERIODS, (native,))
    )
    path = write_feature_artifact(matrix, tmp_path)
    source = TrainingJoinSourceV1(
        "macro", JoinFamily.VINTAGE, paths=(str(path),)
    )
    column = bound_column(
        source,
        "calendar.macro.transformed",
        TrainingJoinEntityV1("EURUSD", series="macro.cpi"),
        "macro",
        coordinate=PERIODS[-1].label,
        meaning=JoinMeaning.SNAPSHOT,
    )
    definition = _describe(source, column)
    assert definition.units == unit == matrix.cells[-1].unit
    assert definition.step_unit == "reference_periods"
    assert definition.lag_steps == (
        transform.window if kind is FeatureTransformKind.LAG else 0
    )
    assert _has_exact_object(
        json.loads(definition.native_semantics_json), transform.to_dict()
    )
    assert matrix.cells[-1].value is not None
    spine = controlled_spine(tmp_path / "spine", (CUTOFF,), derived=(path,))
    plan = TrainingJoinPlanV1(
        spine, JoinInformationMode.NORMALIZED_AS_OF, (source,), (column,)
    )
    native_value = materialize_training_joins(plan).rows[0].values[0]
    view = materialize_training_wide_view(
        build_training_wide_plan(
            (plan,), grain=TrainingWideGrainV1(WideRowGrain.EVENT)
        )
    )
    records = project_training_wide_view(view)
    assert len(records) == 1
    record = records[0]
    assert record["spine"] == spine.rows[0].to_dict()
    assert record["values"][column.name] == matrix.cells[-1].value
    assert (
        record["states"][column.name] == native_value.state.value == "available"
    )
    assert record["cell_provenance"][column.name] == native_value.to_dict()
    assert view.manifest.columns[0].units == definition.units


@pytest.mark.parametrize(
    "field,unit",
    [
        ("open_interest_all", "reported_futures_contracts_not_spot_volume"),
        ("pct_of_oi_noncomm_long_all", "percent_of_reported_open_interest"),
        ("traders_tot_all", "reported_traders"),
    ],
)
def test_positioning_descriptor_distinguishes_declared_numeric_unit_families(
    field, unit
):
    source = TrainingJoinSourceV1("positions", JoinFamily.POSITIONING)
    column = bound_column(
        source, "positioning.value", TrainingJoinEntityV1("EURUSD"), field
    )
    assert _describe(source, column).units == unit


@pytest.mark.parametrize(
    "field",
    [
        "conc_gross_le_4_tdr_long_all",
        "change_in_pct_of_oi_noncomm_long_all",
        "unmapped_pct_of_oi_field",
    ],
)
def test_positioning_does_not_default_unknown_or_concentration_fields_to_contracts(
    field,
):
    source = TrainingJoinSourceV1("positions", JoinFamily.POSITIONING)
    column = bound_column(
        source, "positioning.value", TrainingJoinEntityV1("EURUSD"), field
    )
    assert (
        _describe(source, column).units
        != "reported_futures_contracts_not_spot_volume"
    )


@pytest.mark.parametrize("cutoff", [RELEASE_TIME, RELEASE_TIME + 20 * SECOND])
def test_unavailable_or_stale_calendar_preserves_refusal_parent_basis(
    tmp_path, cutoff
):
    corpus = calendar_fixture(availability_delay_ns=SECOND)
    path = write_economic_calendar_corpus(corpus, tmp_path / "calendar").path
    source = TrainingJoinSourceV1(
        "calendar", JoinFamily.CALENDAR, paths=(str(path),)
    )
    column = bound_column(
        source,
        "calendar.cpi.actual",
        TrainingJoinEntityV1("EURUSD", "USD", "US", "fixture.cpi.mom"),
        "actual_value",
        coordinate="period-0",
        direction=JoinDirection.BOUNDED_PRIOR,
        max_age_ns=10 * SECOND,
    )
    spine = controlled_spine(tmp_path / "spine", (cutoff,))
    native_plan = TrainingJoinPlanV1(
        spine, JoinInformationMode.NORMALIZED_AS_OF, (source,), (column,)
    )
    batch = materialize_training_joins(native_plan)
    value = batch.rows[0].values[0]
    assert value.value_json == '{"value":null}'
    assert corpus.corpus_id not in value.source_ids + value.parent_source_ids
    assert any(
        corpus.corpus_id in e.parent_source_ids for e in value.refusal_evidence
    )
    wide = build_training_wide_plan(
        (native_plan,), grain=TrainingWideGrainV1(WideRowGrain.EVENT)
    )
    assert len(wide.columns) == 1
    assert wide.columns[0].units == next(
        r.unit for r in corpus.releases if r.reference_period == "period-0"
    )


def _calendar_with_unit_variant(tmp_path, *, currency, economy, released=True):
    corpus = calendar_fixture()
    changed = {
        "logical_event_key": "synthetic.other-version",
        "source_release_id": "synthetic.other-version",
        "series_version": "v2",
        "economy": "Synthetic alternative economy",
        "economy_code": economy,
        "currency": currency,
        "affected_currencies": (currency,),
        "unit": "synthetic_index",
        "release_id": "",
    }
    alternative = replace(corpus.releases[0], **changed)
    additions = (alternative,)
    if released:
        additions += (
            replace(
                corpus.releases[1],
                supersedes_release_id=alternative.release_id,
                **changed,
            ),
        )
    corpus = replace(
        corpus, releases=(*corpus.releases, *additions), corpus_id=""
    )
    path = write_economic_calendar_corpus(corpus, tmp_path / "calendar").path
    source = TrainingJoinSourceV1(
        "calendar", JoinFamily.CALENDAR, paths=(str(path),)
    )
    column = bound_column(
        source,
        "calendar.cpi.actual",
        TrainingJoinEntityV1("EURUSD", "USD", "US", "fixture.cpi.mom"),
        "actual_value",
        coordinate="period-0",
    )
    return source, column


@pytest.mark.parametrize("currency,economy", [("JPY", "US"), ("USD", "JP")])
def test_calendar_units_follow_native_entity_selection_not_unrelated_versions(
    tmp_path, currency, economy
):
    source, column = _calendar_with_unit_variant(
        tmp_path, currency=currency, economy=economy
    )
    spine = controlled_spine(tmp_path / "spine", (RELEASE_TIME,))
    plan = TrainingJoinPlanV1(
        spine, JoinInformationMode.NORMALIZED_AS_OF, (source,), (column,)
    )
    native = materialize_training_joins(plan)
    assert native.rows[0].values[0].value_json == '{"value":3.0}'
    assert _describe(source, column).units == "percent_mom"
    wide = build_training_wide_plan(
        (plan,), grain=TrainingWideGrainV1(WideRowGrain.EVENT)
    )
    assert wide.columns[0].units == "percent_mom"
    records = project_training_wide_view(materialize_training_wide_view(wide))
    assert len(records) == 1
    record = records[0]
    value = native.rows[0].values[0]
    assert record["spine"] == spine.rows[0].to_dict()
    assert record["values"][column.name] == 3.0
    assert record["states"][column.name] == value.state.value == "available"
    assert record["cell_provenance"][column.name] == value.to_dict()


@pytest.mark.parametrize("currency,economy", [("JPY", "US"), ("USD", "JP")])
def test_calendar_unmatched_entity_does_not_borrow_another_entity_units(
    tmp_path, currency, economy
):
    corpus = calendar_fixture()
    path = write_economic_calendar_corpus(corpus, tmp_path / "calendar").path
    source = TrainingJoinSourceV1(
        "calendar", JoinFamily.CALENDAR, paths=(str(path),)
    )
    column = bound_column(
        source,
        "calendar.cpi.actual",
        TrainingJoinEntityV1(
            "EUR" + currency, currency, economy, "fixture.cpi.mom"
        ),
        "actual_value",
        coordinate="period-0",
    )
    assert _describe(source, column).units is None


def test_calendar_selected_incompatible_unit_versions_still_refuse(tmp_path):
    source, column = _calendar_with_unit_variant(
        tmp_path, currency="USD", economy="US"
    )
    with pytest.raises(ValueError, match="incompatible native unit"):
        _describe(source, column)


def test_calendar_schedule_without_actual_does_not_supply_actual_value_units(
    tmp_path,
):
    source, column = _calendar_with_unit_variant(
        tmp_path, currency="USD", economy="US", released=False
    )
    assert _describe(source, column).units == "percent_mom"


def test_unavailable_vintage_warmup_retains_verified_native_basis(tmp_path):
    transform = FeatureTransformV1(FeatureTransformKind.LAG, window=1)
    native = FeatureColumnV1("macro", "macro.cpi", transform)
    matrix = store_fixture().snapshot(
        FeatureRequestV1(CUTOFF, PERIODS, (native,))
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
        coordinate=PERIODS[0].label,
        meaning=JoinMeaning.SNAPSHOT,
    )
    spine = controlled_spine(tmp_path / "spine", (CUTOFF,), derived=(path,))
    plan = TrainingJoinPlanV1(
        spine, JoinInformationMode.NORMALIZED_AS_OF, (source,), (column,)
    )
    assert (
        materialize_training_joins(plan).rows[0].values[0].value_json
        == '{"value":null}'
    )
    assert (
        build_training_wide_plan(
            (plan,), grain=TrainingWideGrainV1(WideRowGrain.EVENT)
        )
        .columns[0]
        .column
        == column
    )


def test_reserved_unsupported_source_does_not_invent_units():
    source = TrainingJoinSourceV1("unsupported", JoinFamily.UNSUPPORTED)
    column = bound_column(
        source, "synthetic_flow.value", TrainingJoinEntityV1("EURUSD"), "value"
    )
    definition = _describe(source, column)
    assert definition.units is None
    assert definition.dtype is WideValueType.UNAVAILABLE
