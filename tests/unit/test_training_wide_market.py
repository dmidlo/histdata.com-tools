"""Wide market acceptance from generated native publications, never real data."""

from dataclasses import replace

import pytest

from histdatacom.data_quality.training_contracts import (
    DAY_NS,
    TrainingConsumerMode,
    TrainingRequestV1,
    training_json,
    training_load,
)
from histdatacom.data_quality.training_join_contracts import (
    JoinFamily,
    JoinInformationMode,
    JoinMeaning,
    JoinState,
    TrainingJoinEntityV1,
    TrainingJoinPlanV1,
    TrainingJoinSourceV1,
)
from histdatacom.data_quality.training_join_views import (
    materialize_training_joins,
)
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
from histdatacom.synthetic.activity import (
    ActivitySliceScope,
    summarize_committed_reconstruction_activity,
)
from histdatacom.synthetic.bar_features import (
    BarAbsenceDeclarationV1,
    BarAvailabilityBasis,
    BarAvailabilityDeclarationV1,
    BarFeaturePolicyV1,
    BarFeatureState,
)
from histdatacom.synthetic.bars import STANDARD_DERIVED_BAR_INTERVALS
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.triangle_bar_features import (
    TriangleBarPolicyV1,
    TriangleBarSourceV1,
)
from histdatacom.synthetic.triangle_projection_features import (
    TriangleProjectionEvidenceV1,
    derive_triangle_projection_features,
)
from tests.fixtures import training_substrate_v1 as fixture
from tests.fixtures.training_join_v1 import bound_column
from tests.unit import test_training_join_market as native_market

BASE = fixture.BASE
SECOND = 1_000_000_000
OHLC = ("bid_open", "bid_high", "bid_low", "bid_close")
native = native_market.native


def _bar_name(family, scope, interval, field):
    if family is JoinFamily.BAR:
        return f"market.bar.EURUSD.{scope}.{interval}.{field}"
    return f"market.indicator.EURUSD.{interval}.{field}"


def _wide(plan, tile_width=128):
    columns = tuple(sorted(plan.columns, key=lambda c: c.name))
    tiles = tuple(
        replace(plan, columns=columns[start : start + tile_width])
        for start in range(0, len(columns), tile_width)
    )
    return materialize_training_wide_view(
        build_training_wide_plan(
            tiles, grain=TrainingWideGrainV1(WideRowGrain.EVENT)
        )
    )


def _declarations(bars, policy, symbols=("EURUSD",)):
    return tuple(
        BarAvailabilityDeclarationV1(
            bar.bar_id,
            bar.bar_end_ns,
            bar.bar_end_ns,
            BarAvailabilityBasis.DECLARED_SOURCE_CLOCK,
            "synthetic test clock, not historical evidence",
            (
                bar.bar_end_ns
                if bar.scope is not ActivitySliceScope.OBSERVED
                else None
            ),
        )
        for symbol in symbols
        for bar in bars.verified_bars(symbol, policy)
    )


def _assert_native_parity(plan, records):
    native_result = materialize_training_joins(plan)
    assert len(records) == len(plan.spine.rows) == len(native_result.rows)
    originals = {r.artifact_id: r for r in plan.spine.rows}
    for record, joined in zip(records, native_result.rows):
        assert record["spine"] == originals[joined.spine_row_id].to_dict()
        assert record["values"] == {
            value.column: training_load(value.value_json)["value"]
            for value in joined.values
        }
        assert record["cell_provenance"] == {
            value.column: value.to_dict() for value in joined.values
        }
        assert record["states"] == {
            value.column: value.state.value for value in joined.values
        }


@pytest.fixture(scope="module")
def ohlc_view(native):
    _, _, _, _, bars, _, snapshots, spine, _, _ = native
    policy = BarFeaturePolicyV1(
        InformationMode.EX_POST_RECONSTRUCTION,
        scopes=(ActivitySliceScope.MERGED,),
        feature_names=OHLC + ("mid_log_open_close_return",),
        allow_prior_generated_state=True,
    )
    declarations = _declarations(bars, policy)
    evidence = tuple(
        training_json(
            bars.snapshot(
                symbol="EURUSD",
                decision_time_ns=snapshot.decision_time_ns,
                policy=policy,
                availability=declarations,
            ).to_dict()
        )
        for snapshot in snapshots
    )
    bindings = tuple(
        TrainingJoinSourceV1(
            key,
            family,
            paths=(bars.reconstruction_manifest_path, bars.bar_manifest_path),
            evidence_json=evidence,
        )
        for key, family in (
            ("bar", JoinFamily.BAR),
            ("indicator", JoinFamily.INDICATOR),
        )
    )
    columns = tuple(
        bound_column(
            binding,
            _bar_name(binding.family, "merged", interval, field),
            TrainingJoinEntityV1("EURUSD"),
            field,
            coordinate=f"merged:{interval}",
            meaning=JoinMeaning.CLOSED_BAR,
        )
        for binding in bindings
        for interval in STANDARD_DERIVED_BAR_INTERVALS
        for field in (
            OHLC
            if binding.family is JoinFamily.BAR
            else ("mid_log_open_close_return",)
        )
    )
    plan = TrainingJoinPlanV1(
        spine, JoinInformationMode.EX_POST, bindings, columns
    )
    view = _wide(plan, tile_width=10)
    return plan, view, project_training_wide_view(view)


def test_seven_timeframe_ohlc_and_indicator_values_keep_exact_native_rows(
    ohlc_view, native
):
    plan, view, records = ohlc_view
    _assert_native_parity(plan, records)
    assert len(view.manifest.columns) == 35
    assert len(records) == 7
    middle = (BASE + native[-1]) // 2
    for interval, width in STANDARD_DERIVED_BAR_INTERVALS.items():
        cutoff = (middle // width + 1) * width
        record = next(
            r for r in records if r["spine"]["decision_time_ns"] == cutoff
        )
        for field in OHLC:
            name = f"market.bar.EURUSD.merged.{interval}.{field}"
            assert record["states"][name] == JoinState.AVAILABLE.value
            assert record["values"][name] == pytest.approx(
                fixture.QUOTES["EURUSD"][0]
            )
            assert (
                native[2].manifest.manifest_id
                in record["cell_provenance"][name]["parent_source_ids"]
            )
        indicator = (
            f"market.indicator.EURUSD.{interval}.mid_log_open_close_return"
        )
        assert record["states"][indicator] == JoinState.AVAILABLE.value
        assert record["values"][indicator] == pytest.approx(0)
    assert all(
        c.timeframe in STANDARD_DERIVED_BAR_INTERVALS
        for c in view.manifest.columns
    )
    assert all(c.step_unit == "closed_bars" for c in view.manifest.columns)


def test_market_column_projection_and_repartition_keep_content_and_provenance(
    ohlc_view,
):
    plan, view, records = ohlc_view
    single = _wide(plan)
    assert single.manifest.schema_sha256 == view.manifest.schema_sha256
    assert single.manifest.content_sha256 == view.manifest.content_sha256
    names = (
        view.manifest.columns[-1].column.name,
        view.manifest.columns[0].column.name,
    )
    projected = project_training_wide_view(single, columns=names)
    for actual, full in zip(projected, records):
        assert actual["spine"] == full["spine"]
        for key in ("values", "states", "cell_provenance"):
            assert actual[key] == {n: full[key][n] for n in sorted(names)}


@pytest.fixture(scope="module")
def triangle_activity_view(native):
    source, ownership, product, _, bars, _, _, _, captured, right = native
    cutoff = right + SECOND
    bar_policy = BarFeaturePolicyV1(
        InformationMode.EX_POST_RECONSTRUCTION,
        scopes=(ActivitySliceScope.MERGED, ActivitySliceScope.OBSERVED),
        allow_prior_generated_state=True,
    )
    triangle_source = TriangleBarSourceV1(bars)
    snapshot = triangle_source.snapshot(
        decision_time_ns=cutoff,
        policy=TriangleBarPolicyV1(bar_policy, BASE),
        availability=_declarations(bars, bar_policy, fixture.SYMBOLS),
    )
    evidence = TriangleProjectionEvidenceV1(
        training_json(captured["run"].to_dict()),
        training_json(captured["window"].to_dict()),
        training_json(captured["config"].to_dict()),
        tuple(
            training_json(s.to_dict())
            for _, s in sorted(captured["streams"].items())
        ),
        (),
        "modern-reference:training-fixture",
        cutoff,
        cutoff,
    )
    projection = derive_triangle_projection_features(
        triangle_source,
        snapshot,
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
        evidence=evidence,
    )
    triangle = TrainingJoinSourceV1(
        "triangle",
        JoinFamily.TRIANGLE,
        paths=(bars.reconstruction_manifest_path, bars.bar_manifest_path),
        evidence_json=(training_json(projection.to_dict()),),
    )
    activity = summarize_committed_reconstruction_activity(
        product.manifest_path,
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
        information_manifest_id="controlled-expost-fixture",
    )
    activity_binding = TrainingJoinSourceV1(
        "activity",
        JoinFamily.ACTIVITY,
        paths=(str(product.manifest_path),),
        evidence_json=(training_json(activity.to_dict()),),
    )
    metric = next(
        m
        for item in activity.slices
        if item.symbol.upper() == "EURUSD"
        and item.scope is ActivitySliceScope.MERGED
        for m in item.metrics
        if m.value is not None
    )
    columns = tuple(
        bound_column(
            triangle,
            f"triangle.{interval}.{scope}.{field}",
            TrainingJoinEntityV1("EURUSD"),
            field,
            coordinate=f"{scope}:{interval}",
            meaning=JoinMeaning.CLOSED_BAR,
        )
        for interval in STANDARD_DERIVED_BAR_INTERVALS
        for scope in ("merged", "observed")
        for field in ("residual_close", "projection.projected_rate")
    ) + (
        bound_column(
            activity_binding,
            f"activity.EURUSD.merged.{metric.name}",
            TrainingJoinEntityV1("EURUSD"),
            metric.name,
            coordinate="merged",
            meaning=JoinMeaning.SNAPSHOT,
        ),
    )
    spine = materialize_training_rows(
        source,
        ownership,
        TrainingRequestV1(
            TrainingConsumerMode.DESCRIPTIVE,
            BASE,
            BASE + 1,
            ("EURUSD",),
            decision_time_ns=cutoff,
        ),
    )
    plan = TrainingJoinPlanV1(
        spine,
        JoinInformationMode.EX_POST,
        (triangle, activity_binding),
        columns,
    )
    view = _wide(plan, tile_width=10)
    return plan, view, project_training_wide_view(view), evidence, metric


def test_all_triangle_timeframes_and_activity_retain_native_values_and_nulls(
    triangle_activity_view,
):
    plan, view, records, evidence, metric = triangle_activity_view
    _assert_native_parity(plan, records)
    assert len(records) == 1 and len(records[0]["values"]) == 29
    record = records[0]
    projected = "triangle.1d.merged.projection.projected_rate"
    assert record["states"][projected] == JoinState.AVAILABLE.value
    assert (
        evidence.artifact_id
        in record["cell_provenance"][projected]["parent_source_ids"]
    )
    for interval in STANDARD_DERIVED_BAR_INTERVALS:
        name = f"triangle.{interval}.observed.projection.projected_rate"
        assert record["states"][name] == JoinState.NOT_APPLICABLE.value
        assert record["values"][name] is None
    activity_name = f"activity.EURUSD.merged.{metric.name}"
    # Activity is an exact snapshot at the committed window's final event;
    # the later projection cutoff must not silently forward-fill that value.
    assert record["states"][activity_name] == JoinState.UNAVAILABLE.value
    assert record["values"][activity_name] is None
    assert record["cell_provenance"][activity_name]["reason"] == (
        "no_source_coordinate_at_declared_join"
    )
    descriptor = next(
        c for c in view.manifest.columns if c.column.name == activity_name
    )
    assert descriptor.units == metric.unit
    semantics = training_load(descriptor.native_semantics_json)
    assert semantics["native_semantics"] == metric.semantics.value


def test_activity_is_available_only_at_its_exact_native_snapshot(
    triangle_activity_view, native
):
    original, _, _, _, metric = triangle_activity_view
    source, ownership, _, _, _, _, _, _, _, right = native
    spine = materialize_training_rows(
        source,
        ownership,
        TrainingRequestV1(
            TrainingConsumerMode.DESCRIPTIVE,
            BASE,
            BASE + 1,
            ("EURUSD",),
            decision_time_ns=right,
        ),
    )
    binding = next(
        s for s in original.sources if s.family is JoinFamily.ACTIVITY
    )
    column = next(c for c in original.columns if c.source_key == binding.key)
    plan = TrainingJoinPlanV1(
        spine, JoinInformationMode.EX_POST, (binding,), (column,)
    )
    records = project_training_wide_view(_wide(plan))
    _assert_native_parity(plan, records)
    assert len(records) == 1
    record = records[0]
    assert record["states"][column.name] == JoinState.AVAILABLE.value
    assert record["values"][column.name] == metric.value
    provenance = record["cell_provenance"][column.name]
    assert provenance["source_time_ns"] == right
    assert provenance["origin"].endswith("not_traded_volume")


@pytest.mark.parametrize(
    "absence,state",
    (
        ("expected_closure", JoinState.EXPECTED_CLOSURE),
        ("source_outage", JoinState.SOURCE_OUTAGE),
        ("missing_bar", JoinState.EMPTY_MARKET_INTERVAL),
        (None, JoinState.INSUFFICIENT_WARMUP),
    ),
)
def test_wide_native_absence_and_indicator_warmup_remain_null(
    native, absence, state
):
    source, ownership, _, _, bars, _, _, _, _, _ = native
    cutoff = BASE if absence else BASE + DAY_NS
    field = "bid_close" if absence else "mid_log_close_return"
    policy = BarFeaturePolicyV1(
        InformationMode.EX_ANTE_SIMULATION,
        intervals=("1d",),
        scopes=(ActivitySliceScope.OBSERVED,),
        feature_names=(field,),
    )
    absences = (
        (
            BarAbsenceDeclarationV1(
                "EURUSD",
                ActivitySliceScope.OBSERVED,
                "1d",
                BASE - DAY_NS,
                BASE,
                BarFeatureState(absence),
                "synthetic native absence declaration",
            ),
        )
        if absence
        else ()
    )
    snapshot = bars.snapshot(
        symbol="EURUSD",
        decision_time_ns=cutoff,
        policy=policy,
        availability=_declarations(bars, policy),
        absences=absences,
    )
    binding = TrainingJoinSourceV1(
        "native",
        JoinFamily.BAR if absence else JoinFamily.INDICATOR,
        paths=(bars.reconstruction_manifest_path, bars.bar_manifest_path),
        evidence_json=(training_json(snapshot.to_dict()),),
    )
    column = bound_column(
        binding,
        _bar_name(binding.family, "observed", "1d", field),
        TrainingJoinEntityV1("EURUSD"),
        field,
        coordinate="observed:1d",
        meaning=JoinMeaning.CLOSED_BAR,
    )
    spine = materialize_training_rows(
        source,
        ownership,
        TrainingRequestV1(
            TrainingConsumerMode.DESCRIPTIVE,
            BASE,
            BASE + 1,
            ("EURUSD",),
            decision_time_ns=cutoff,
        ),
    )
    plan = TrainingJoinPlanV1(
        spine, JoinInformationMode.NORMALIZED_AS_OF, (binding,), (column,)
    )
    records = project_training_wide_view(_wide(plan))
    _assert_native_parity(plan, records)
    assert records[0]["states"][column.name] == state.value
    assert records[0]["values"][column.name] is None
    assert (
        records[0]["cell_provenance"][column.name][
            "historical_availability_verified"
        ]
        is False
    )
