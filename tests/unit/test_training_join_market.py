"""Real owned native publications supply every market join family."""

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
from histdatacom.data_quality.training_lineage import build_training_ownership
from histdatacom.data_quality.training_views import materialize_training_rows
from histdatacom.synthetic.activity import (
    ActivitySliceScope,
    summarize_committed_reconstruction_activity,
)
from histdatacom.synthetic.bar_features import (
    BarFeaturePolicyV1,
    BarFeatureSourceV1,
    BarAvailabilityDeclarationV1,
    BarAvailabilityBasis,
)
from histdatacom.synthetic.bars import (
    STANDARD_DERIVED_BAR_INTERVALS,
    DerivedBarPolicyV1,
    publish_derived_bars,
)
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.triangle_bar_features import (
    TriangleBarPolicyV1,
    TriangleBarSourceV1,
)
from histdatacom.synthetic.triangle_projection_features import (
    TriangleProjectionEvidenceV1,
    derive_triangle_projection_features,
)
from tests.fixtures.training_join_v1 import bound_column, native_observed_source
from tests.fixtures import training_substrate_v1 as fixture

BASE = fixture.BASE
SECOND = 1_000_000_000


@pytest.fixture(scope="module")
def native(tmp_path_factory):
    root = tmp_path_factory.mktemp("join-owned-native")
    right = BASE + DAY_NS + SECOND
    middle = (BASE + right) // 2
    cutoffs = tuple(
        sorted(
            {
                (middle // width + 1) * width
                for width in STANDARD_DERIVED_BAR_INTERVALS.values()
            }
        )
    )
    times = (BASE, *cutoffs, right, BASE + 2 * DAY_NS)
    captured = {}
    original = fixture.reconcile_cross_currency_window

    def capture(**kwargs):
        captured.update(kwargs)
        return original(**kwargs)

    # Temporary fixture-only clock geometry; production/source contracts are
    # executed unchanged, including exact physical source ordinals and delivery.
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(fixture, "TIMES", times)
        patch.setattr(fixture, "reconcile_cross_currency_window", capture)
        source, version = native_observed_source(root / "observed", times)
        product, delivered = fixture.published_product(
            root / "product", version, indices=(0, len(times) - 2)
        )
    source = fixture.with_products(source, product)
    ownership = build_training_ownership(source)
    bars = publish_derived_bars(
        root / "bars",
        product.manifest_path,
        policy=DerivedBarPolicyV1(
            intervals=tuple(STANDARD_DERIVED_BAR_INTERVALS),
            scopes=tuple(ActivitySliceScope),
        ),
    )
    bar_source = BarFeatureSourceV1(
        str(product.manifest_path), str(bars.manifest_path)
    )
    policy = BarFeaturePolicyV1(
        InformationMode.EX_POST_RECONSTRUCTION,
        scopes=(ActivitySliceScope.MERGED,),
        feature_names=("bid_close", "mid_log_open_close_return"),
        allow_prior_generated_state=True,
    )
    records = bar_source.verified_bars("EURUSD", policy)
    declarations = tuple(
        BarAvailabilityDeclarationV1(
            b.bar_id,
            b.bar_end_ns,
            b.bar_end_ns,
            BarAvailabilityBasis.DECLARED_SOURCE_CLOCK,
            "controlled synthetic clock, not historical evidence",
            b.bar_end_ns,
        )
        for b in records
    )
    snapshots = tuple(
        bar_source.snapshot(
            symbol="EURUSD",
            decision_time_ns=cutoff,
            policy=policy,
            availability=declarations,
        )
        for cutoff in cutoffs
    )
    spine = materialize_training_rows(
        source,
        ownership,
        TrainingRequestV1(
            TrainingConsumerMode.DESCRIPTIVE,
            min(cutoffs),
            max(cutoffs) + 1,
            ("EURUSD",),
        ),
    )
    return (
        source,
        ownership,
        product,
        delivered,
        bar_source,
        policy,
        snapshots,
        spine,
        captured,
        right,
    )


def test_all_seven_timeframes_append_without_cartesian_rows(native):
    (
        source,
        ownership,
        product,
        delivered,
        bar_source,
        policy,
        snapshots,
        spine,
        _,
        _,
    ) = native
    binding = TrainingJoinSourceV1(
        "bars",
        JoinFamily.BAR,
        paths=(
            bar_source.reconstruction_manifest_path,
            bar_source.bar_manifest_path,
        ),
        evidence_json=tuple(training_json(s.to_dict()) for s in snapshots),
    )
    columns = tuple(
        bound_column(
            binding,
            f"market.bar.EURUSD.merged.{interval}.bid_close",
            TrainingJoinEntityV1("EURUSD"),
            "bid_close",
            coordinate=f"merged:{interval}",
            meaning=JoinMeaning.CLOSED_BAR,
        )
        for interval in STANDARD_DERIVED_BAR_INTERVALS
    )
    plan = TrainingJoinPlanV1(
        spine, JoinInformationMode.EX_POST, (binding,), columns
    )
    result = materialize_training_joins(plan)
    assert len(result.rows) == len(spine.rows) == 7
    assert all(len(row.values) == 7 for row in result.rows)
    middle = (BASE + native[-1]) // 2
    for index, interval in enumerate(STANDARD_DERIVED_BAR_INTERVALS):
        width = STANDARD_DERIVED_BAR_INTERVALS[interval]
        cutoff = (middle // width + 1) * width
        row = next(r for r in result.rows if r.decision_time_ns == cutoff)
        assert row.values[index].state is JoinState.AVAILABLE
        assert training_load(row.values[index].value_json)[
            "value"
        ] == pytest.approx(fixture.QUOTES["EURUSD"][0])
        assert (
            product.manifest.manifest_id in row.values[index].parent_source_ids
        )


def test_observed_closed_bar_normalized_clock_and_partial_refusal(native):
    source, ownership, product, _, bar_source, _, _, _, _, right = native
    policy = BarFeaturePolicyV1(
        InformationMode.EX_ANTE_SIMULATION,
        intervals=("1d",),
        scopes=(ActivitySliceScope.OBSERVED,),
        feature_names=("bid_close",),
    )
    records = bar_source.verified_bars("EURUSD", policy)
    declarations = tuple(
        BarAvailabilityDeclarationV1(
            b.bar_id,
            b.bar_end_ns,
            b.bar_end_ns,
            BarAvailabilityBasis.DECLARED_SOURCE_CLOCK,
            "controlled availability declaration",
        )
        for b in records
        if b.scope is ActivitySliceScope.OBSERVED
    )
    cutoff = BASE + DAY_NS
    snapshot = bar_source.snapshot(
        symbol="EURUSD",
        decision_time_ns=cutoff,
        policy=policy,
        availability=declarations,
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
    binding = TrainingJoinSourceV1(
        "bar",
        JoinFamily.BAR,
        paths=(
            bar_source.reconstruction_manifest_path,
            bar_source.bar_manifest_path,
        ),
        evidence_json=(training_json(snapshot.to_dict()),),
    )
    column = bound_column(
        binding,
        "market.bar.EURUSD.observed.1d.bid_close",
        TrainingJoinEntityV1("EURUSD"),
        "bid_close",
        coordinate="observed:1d",
        meaning=JoinMeaning.CLOSED_BAR,
    )
    result = materialize_training_joins(
        TrainingJoinPlanV1(
            spine, JoinInformationMode.NORMALIZED_AS_OF, (binding,), (column,)
        )
    )
    assert result.rows[0].values[0].state is JoinState.AVAILABLE
    assert result.rows[0].values[0].source_time_ns == cutoff
    assert not result.rows[0].values[0].historical_availability_verified
    with pytest.raises(ValueError, match="partial"):
        replace(snapshot.cells[0], partial=True)


def test_triangle_and_projection_replay_actual_delivery_and_null_observed_state(
    native,
):
    source, ownership, product, delivered, bars, _, _, _, captured, right = (
        native
    )
    cutoff = right + SECOND
    policy = TriangleBarPolicyV1(
        BarFeaturePolicyV1(
            InformationMode.EX_POST_RECONSTRUCTION,
            intervals=("1d",),
            scopes=(ActivitySliceScope.MERGED, ActivitySliceScope.OBSERVED),
            allow_prior_generated_state=True,
        ),
        BASE,
    )
    declarations = tuple(
        BarAvailabilityDeclarationV1(
            b.bar_id,
            b.bar_end_ns,
            b.bar_end_ns,
            BarAvailabilityBasis.DECLARED_SOURCE_CLOCK,
            "controlled quote clocks",
            (
                b.bar_end_ns
                if b.scope is not ActivitySliceScope.OBSERVED
                else None
            ),
        )
        for symbol in fixture.SYMBOLS
        for b in bars.verified_bars(symbol, policy.bar_policy)
    )
    native_source = TriangleBarSourceV1(bars)
    snapshot = native_source.snapshot(
        decision_time_ns=cutoff, policy=policy, availability=declarations
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
        native_source,
        snapshot,
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
        evidence=evidence,
    )
    binding = TrainingJoinSourceV1(
        "triangle",
        JoinFamily.TRIANGLE,
        paths=(bars.reconstruction_manifest_path, bars.bar_manifest_path),
        evidence_json=(training_json(projection.to_dict()),),
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
    ordinary_field = next(
        v.name
        for c in snapshot.cells
        if c.scope is ActivitySliceScope.MERGED
        for v in c.values
        if v.value is not None
    )
    columns = (
        bound_column(
            binding,
            f"triangle.1d.merged.{ordinary_field}",
            TrainingJoinEntityV1("EURUSD"),
            ordinary_field,
            coordinate="merged:1d",
            meaning=JoinMeaning.CLOSED_BAR,
        ),
        bound_column(
            binding,
            "triangle.1d.merged.projection.projected_rate",
            TrainingJoinEntityV1("EURUSD"),
            "projection.projected_rate",
            coordinate="merged:1d",
            meaning=JoinMeaning.CLOSED_BAR,
        ),
        bound_column(
            binding,
            "triangle.1d.observed.projection.projected_rate",
            TrainingJoinEntityV1("EURUSD"),
            "projection.projected_rate",
            coordinate="observed:1d",
            meaning=JoinMeaning.CLOSED_BAR,
        ),
    )
    result = materialize_training_joins(
        TrainingJoinPlanV1(
            spine, JoinInformationMode.EX_POST, (binding,), columns
        )
    )
    assert result.rows[0].values[0].state is JoinState.AVAILABLE
    assert result.rows[0].values[1].state is JoinState.AVAILABLE
    assert result.rows[0].values[2].state is JoinState.NOT_APPLICABLE
    assert evidence.artifact_id in result.rows[0].values[1].parent_source_ids
    for native_artifact in (snapshot, projection):
        wire = native_artifact.to_dict()
        wire["unknown_future_metadata"] = "not in the native writer"
        altered = replace(binding, evidence_json=(training_json(wire),))
        with pytest.raises(ValueError):
            materialize_training_joins(
                TrainingJoinPlanV1(
                    spine,
                    JoinInformationMode.EX_POST,
                    (altered,),
                    (columns[0],),
                )
            )


def test_committed_activity_uses_full_window_and_never_claims_spot_volume(
    native,
):
    source, ownership, product, _, bars, _, _, _, _, right = native
    activity = summarize_committed_reconstruction_activity(
        product.manifest_path,
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
        information_manifest_id="controlled-expost-fixture",
    )
    binding = TrainingJoinSourceV1(
        "activity",
        JoinFamily.ACTIVITY,
        paths=(str(product.manifest_path),),
        evidence_json=(training_json(activity.to_dict()),),
    )
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
    field = next(
        m.name
        for s in activity.slices
        if s.symbol.upper() == "EURUSD" and s.scope is ActivitySliceScope.MERGED
        for m in s.metrics
        if m.value is not None
    )
    column = bound_column(
        binding,
        f"activity.EURUSD.merged.{field}",
        TrainingJoinEntityV1("EURUSD"),
        field,
        coordinate="merged",
        meaning=JoinMeaning.SNAPSHOT,
    )
    plan = TrainingJoinPlanV1(
        spine, JoinInformationMode.EX_POST, (binding,), (column,)
    )
    result = materialize_training_joins(plan)
    assert result.rows[0].values[0].state is JoinState.AVAILABLE
    assert result.rows[0].values[0].origin.endswith("not_traded_volume")
    assert (
        materialize_training_joins(
            replace(plan, information_mode=JoinInformationMode.NORMALIZED_AS_OF)
        )
        .rows[0]
        .values[0]
        .state
        is JoinState.UNKNOWN_AVAILABILITY
    )


@pytest.mark.parametrize(
    "state,expected",
    [
        ("expected_closure", JoinState.EXPECTED_CLOSURE),
        ("source_outage", JoinState.SOURCE_OUTAGE),
        ("missing_bar", JoinState.EMPTY_MARKET_INTERVAL),
    ],
)
def test_native_absence_states_survive_normalized_nulls_without_forward_fill(
    native, state, expected
):
    from histdatacom.synthetic.bar_features import (
        BarAbsenceDeclarationV1,
        BarFeatureState,
    )

    source, ownership, _, _, bars, _, _, _, _, _ = native
    policy = BarFeaturePolicyV1(
        InformationMode.EX_ANTE_SIMULATION,
        intervals=("1d",),
        scopes=(ActivitySliceScope.OBSERVED,),
        feature_names=("bid_close",),
    )
    absence = BarAbsenceDeclarationV1(
        "EURUSD",
        ActivitySliceScope.OBSERVED,
        "1d",
        BASE - DAY_NS,
        BASE,
        BarFeatureState(state),
        "controlled native absence declaration",
    )
    snapshot = bars.snapshot(
        symbol="EURUSD",
        decision_time_ns=BASE,
        policy=policy,
        absences=(absence,),
    )
    spine = materialize_training_rows(
        source,
        ownership,
        TrainingRequestV1(
            TrainingConsumerMode.DESCRIPTIVE, BASE, BASE + 1, ("EURUSD",)
        ),
    )
    binding = TrainingJoinSourceV1(
        "bars",
        JoinFamily.BAR,
        paths=(bars.reconstruction_manifest_path, bars.bar_manifest_path),
        evidence_json=(training_json(snapshot.to_dict()),),
    )
    column = bound_column(
        binding,
        "market.bar.EURUSD.observed.1d.bid_close",
        TrainingJoinEntityV1("EURUSD"),
        "bid_close",
        coordinate="observed:1d",
        meaning=JoinMeaning.CLOSED_BAR,
    )
    value = (
        materialize_training_joins(
            TrainingJoinPlanV1(
                spine,
                JoinInformationMode.NORMALIZED_AS_OF,
                (binding,),
                (column,),
            )
        )
        .rows[0]
        .values[0]
    )
    assert value.state is expected
    assert value.value_json == '{"value":null}'
    assert value.reason == state
    assert value.parent_source_ids


def test_native_indicator_warmup_is_not_zero_or_filled_bar(native):
    source, ownership, _, _, bars, _, _, _, _, _ = native
    policy = BarFeaturePolicyV1(
        InformationMode.EX_ANTE_SIMULATION,
        intervals=("1d",),
        scopes=(ActivitySliceScope.OBSERVED,),
        feature_names=("mid_log_close_return",),
    )
    records = bars.verified_bars("EURUSD", policy)
    declarations = tuple(
        BarAvailabilityDeclarationV1(
            b.bar_id,
            b.bar_end_ns,
            b.bar_end_ns,
            BarAvailabilityBasis.DECLARED_SOURCE_CLOCK,
            "controlled clock",
        )
        for b in records
    )
    snapshot = bars.snapshot(
        symbol="EURUSD",
        decision_time_ns=BASE + DAY_NS,
        policy=policy,
        availability=declarations,
    )
    spine = materialize_training_rows(
        source,
        ownership,
        TrainingRequestV1(
            TrainingConsumerMode.DESCRIPTIVE,
            BASE,
            BASE + 1,
            ("EURUSD",),
            decision_time_ns=BASE + DAY_NS,
        ),
    )
    binding = TrainingJoinSourceV1(
        "indicator",
        JoinFamily.INDICATOR,
        paths=(bars.reconstruction_manifest_path, bars.bar_manifest_path),
        evidence_json=(training_json(snapshot.to_dict()),),
    )
    column = bound_column(
        binding,
        "market.indicator.EURUSD.1d.mid_log_close_return",
        TrainingJoinEntityV1("EURUSD"),
        "mid_log_close_return",
        coordinate="observed:1d",
        meaning=JoinMeaning.CLOSED_BAR,
    )
    value = (
        materialize_training_joins(
            TrainingJoinPlanV1(
                spine,
                JoinInformationMode.NORMALIZED_AS_OF,
                (binding,),
                (column,),
            )
        )
        .rows[0]
        .values[0]
    )
    assert value.state is JoinState.INSUFFICIENT_WARMUP
    assert value.value_json == '{"value":null}'
