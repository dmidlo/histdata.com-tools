"""Actual IPC/vintage adapters, exact labels, and all five leakage canaries."""

import math
from dataclasses import replace

import pytest

from histdatacom.data_quality.training_contracts import (
    DAY_NS,
    TrainingConsumerMode,
    TrainingRequestV1,
)
from histdatacom.data_quality.training_temporal_contracts import (
    TemporalFeatureKind,
    TemporalInformationMode,
    TemporalLabelKind,
    TemporalPartition,
    TemporalStatePolicy,
    TemporalTargetKind,
    TrainingTemporalBatchV1,
    TrainingTemporalFeatureV1,
    TrainingTemporalLabelV1,
)
from histdatacom.data_quality.training_temporal_views import (
    materialize_training_temporal,
    replay_training_temporal,
    temporal_plan_from_training_batch,
    training_temporal_rows,
)
from histdatacom.data_quality.training_views import materialize_training_rows
from tests.fixtures.training_substrate_v1 import BASE
from tests.fixtures.training_temporal_v1 import (
    SECOND,
    temporal_plan,
    temporal_source,
)


@pytest.fixture
def source(tmp_path):
    return temporal_source(tmp_path)


@pytest.fixture
def vintage(tmp_path):
    return temporal_source(tmp_path, matrix=True)


def test_real_ipc_source_label_and_replay(source):
    source, ownership, _ = source
    plan = temporal_plan(source, ownership)
    batch = materialize_training_temporal(plan)
    assert batch.rows[0].features[0].value == pytest.approx(1.0)
    assert batch.rows[0].features[0].available_at_ns is None
    assert batch.rows[0].outcome.available_at_ns is None
    assert batch.rows[0].outcome.value == pytest.approx(math.log(1.02))
    assert len(batch.rows[0].outcome.target_record_ids) == 2
    assert (
        batch.rows[0].outcome.baseline_record_id
        not in batch.rows[0].outcome.target_record_ids
    )
    assert batch.rows[0].status == "admitted"
    assert (
        "unknown_legacy_availability_is_not_causal_admission" in batch.nonclaims
    )
    assert TrainingTemporalBatchV1.from_json(batch.to_json()) == batch
    assert replay_training_temporal(batch) == batch


@pytest.mark.parametrize("kind", list(TemporalLabelKind)[:3])
def test_independent_exact_interval_label_math(source, kind):
    source, ownership, _ = source
    plan = temporal_plan(source, ownership)
    label = replace(
        plan.examples[0].label,
        kind=kind,
        deadband=0.03 if kind is TemporalLabelKind.DIRECTION else 0.0,
    )
    plan = replace(plan, examples=(replace(plan.examples[0], label=label),))
    result = materialize_training_temporal(plan).rows[0].outcome
    expected = {
        TemporalLabelKind.LOG_RETURN: math.log(1.02),
        TemporalLabelKind.REALIZED_VARIANCE: math.log(1.01) ** 2
        + math.log(1.02 / 1.01) ** 2,
        TemporalLabelKind.DIRECTION: 0.0,
    }[kind]
    assert result.value == pytest.approx(expected)
    assert result.target_start_exclusive_ns == BASE + SECOND
    assert result.target_end_inclusive_ns == BASE + 3 * SECOND


def test_existing_606_batch_executes_same_temporal_path(source):
    source, ownership, _ = source
    legacy = materialize_training_rows(
        source,
        ownership,
        TrainingRequestV1(
            TrainingConsumerMode.DESCRIPTIVE, BASE, BASE + DAY_NS, ("EURUSD",)
        ),
    )
    plain = temporal_plan(source, ownership)
    plan = temporal_plan_from_training_batch(
        legacy, plain.split, plain.examples
    )
    result = materialize_training_temporal(plan)
    assert result.plan.legacy_batch_json == legacy.to_json()
    assert result.rows == materialize_training_temporal(plain).rows
    with pytest.raises(ValueError, match="cannot be promoted"):
        replace(plan, information_mode=TemporalInformationMode.NORMALIZED_AS_OF)


def vintage_plan(vintage, *, key="price", expected=()):
    source, ownership, matrix = vintage
    plan = temporal_plan(source, ownership)
    feature = TrainingTemporalFeatureV1(
        key,
        TemporalFeatureKind.VINTAGE,
        matrix_id=matrix.snapshot_id,
        column=key,
        period_label="1",
        expected_observation_ids=expected,
    )
    label = TrainingTemporalLabelV1(
        TemporalLabelKind.LOG_RETURN,
        2 * SECOND,
        TemporalTargetKind.VINTAGE_MARKET,
        matrix_id=matrix.snapshot_id,
        feature_key="price",
    )
    return replace(
        plan,
        information_mode=TemporalInformationMode.NORMALIZED_AS_OF,
        examples=(replace(plan.examples[0], features=(feature,), label=label),),
    )


def test_canonical_matrix_positive_normalized_clock_path(vintage):
    batch = materialize_training_temporal(vintage_plan(vintage))
    row = batch.rows[0]
    assert row.features[0].value == 10.0
    assert row.features[0].available_at_ns == BASE + SECOND
    assert row.outcome.value == pytest.approx(math.log(12) - math.log(10))
    assert row.outcome.available_at_ns == BASE + 3 * SECOND
    assert "not_official_authenticity" in row.features[0].verification
    assert replay_training_temporal(batch) == batch


@pytest.mark.parametrize("key", ["macro", "classification"])
def test_revision_and_final_classification_cannot_be_backdated(vintage, key):
    matrix = vintage[2]
    initial = next(
        o
        for o in matrix.observations
        if o.definition.feature_key == key and o.vintage_sequence == 0
    )
    final = next(
        o
        for o in matrix.observations
        if o.definition.feature_key == key and o.vintage_sequence == 1
    )
    positive = materialize_training_temporal(
        vintage_plan(vintage, key=key, expected=(initial.observation_id,))
    )
    assert positive.rows[0].features[0].value == initial.value
    with pytest.raises(ValueError, match="revision is not"):
        materialize_training_temporal(
            vintage_plan(vintage, key=key, expected=(final.observation_id,))
        )


def test_forward_target_cannot_enter_feature_normalization(vintage):
    plan = vintage_plan(vintage)
    example = plan.examples[0]
    future = replace(example.features[0], period_label="3")
    with pytest.raises(ValueError, match="unavailable at decision"):
        materialize_training_temporal(
            replace(plan, examples=(replace(example, features=(future,)),))
        )
    payload = example.features[0].to_dict()
    payload["kind"] = "label_rolling_normalization"
    with pytest.raises(ValueError):
        TrainingTemporalFeatureV1.from_dict(payload)


def test_event_label_requires_exact_release_ownership(vintage):
    plan = vintage_plan(vintage)
    event = next(
        o
        for o in vintage[2].observations
        if o.definition.feature_key == "macro" and o.vintage_sequence == 0
    )
    label = replace(
        plan.examples[0].label,
        kind=TemporalLabelKind.EVENT_RESPONSE,
        event_observation_id=event.observation_id,
    )
    plan = replace(plan, examples=(replace(plan.examples[0], label=label),))
    assert (
        materialize_training_temporal(plan).rows[0].outcome.event_record_id
        == event.observation_id
    )
    forged = replace(label, event_observation_id="arbitrary-release-id")
    with pytest.raises(ValueError, match="exact retained release"):
        materialize_training_temporal(
            replace(plan, examples=(replace(plan.examples[0], label=forged),))
        )


def test_legacy_quotes_never_gain_causal_clock_from_mode(source):
    plan = temporal_plan(
        source[0], source[1], mode=TemporalInformationMode.NORMALIZED_AS_OF
    )
    with pytest.raises(
        ValueError, match="unknown historical quote availability"
    ):
        materialize_training_temporal(plan)


def test_complete_map_and_member_sibling_split_canary(source):
    plan = temporal_plan(source[0], source[1])
    with pytest.raises(ValueError, match="every complete unit"):
        replace(
            plan,
            split=replace(plan.split, assignments=plan.split.assignments[:-1]),
        )
    sibling = replace(
        plan.examples[0], key="sibling", partition=TemporalPartition.TEST
    )
    with pytest.raises(ValueError, match="sibling crosses"):
        replace(plan, examples=(*plan.examples, sibling))
    with pytest.raises(ValueError, match="nonempty"):
        replace(plan, examples=())


def test_dependency_derived_purge_embargo_and_target_unit_boundary(source):
    source, ownership, _ = source
    parts = (
        TemporalPartition.TRAIN,
        TemporalPartition.VALIDATION,
        TemporalPartition.TEST,
    )
    plan = temporal_plan(source, ownership, partitions=parts)
    first = replace(
        plan.examples[0],
        label=replace(plan.examples[0].label, horizon_ns=DAY_NS),
        label_cutoff_ns=BASE + DAY_NS + 4 * SECOND,
    )
    second = replace(
        plan.examples[0],
        key="second",
        decision_time_ns=BASE + DAY_NS + SECOND,
        label_cutoff_ns=BASE + DAY_NS + 4 * SECOND,
        evidence_unit_id=next(
            u.artifact_id
            for u in ownership.units
            if u.start_ns == BASE + DAY_NS
        ),
        partition=parts[1],
    )
    batch = materialize_training_temporal(
        replace(plan, examples=(first, second))
    )
    assert batch.maximum_dependency_span_ns == DAY_NS + 1
    assert "target_crosses_partition" in batch.rows[0].reasons
    assert "purged_dependency_overlap" in batch.rows[0].reasons
    assert "embargo_dependency_span" in batch.rows[1].reasons
    assert (
        training_temporal_rows(batch, information_mode=plan.information_mode)
        == ()
    )


def test_stateful_boundary_requires_explicit_unscored_warmup(source):
    source, ownership, _ = source
    parts = (
        TemporalPartition.TRAIN,
        TemporalPartition.VALIDATION,
        TemporalPartition.TEST,
    )
    plan = temporal_plan(source, ownership, partitions=parts)
    feature = TrainingTemporalFeatureV1(
        "mean", TemporalFeatureKind.QUOTE_MEAN, DAY_NS + SECOND
    )
    example = replace(
        plan.examples[0],
        decision_time_ns=BASE + DAY_NS + SECOND,
        label_cutoff_ns=BASE + DAY_NS + 4 * SECOND,
        evidence_unit_id=next(
            u.artifact_id
            for u in ownership.units
            if u.start_ns == BASE + DAY_NS
        ),
        partition=parts[1],
        features=(feature,),
    )
    with pytest.raises(ValueError, match="stateful feature crosses"):
        materialize_training_temporal(replace(plan, examples=(example,)))
    example = replace(
        example,
        features=(
            replace(feature, state_policy=TemporalStatePolicy.WARMUP_ONLY),
        ),
    )
    row = materialize_training_temporal(
        replace(plan, examples=(example,))
    ).rows[0]
    assert row.features[0].warmup_unit_ids == (
        next(u.artifact_id for u in ownership.units if u.start_ns == BASE),
    )
    assert row.status == "excluded"


def test_empty_projection_still_replays_source_and_unknown_ids_refuse(
    source, monkeypatch
):
    plan = temporal_plan(source[0], source[1])
    batch = materialize_training_temporal(plan)
    assert (
        training_temporal_rows(
            batch, information_mode=plan.information_mode, example_keys=()
        )
        == ()
    )
    import histdatacom.data_quality.training_temporal_sources as module

    def broken(*args):
        raise ValueError("source changed")

    monkeypatch.setattr(module, "verify_training_ownership", broken)
    with pytest.raises(ValueError, match="source changed"):
        training_temporal_rows(
            batch, information_mode=plan.information_mode, example_keys=()
        )


def test_real_rolling_normalizer_excludes_forward_label_prices(source):
    import statistics

    source, ownership, _ = source
    plan = temporal_plan(source, ownership)
    feature = TrainingTemporalFeatureV1(
        "zscore", TemporalFeatureKind.QUOTE_ZSCORE, 3 * SECOND
    )
    example = replace(
        plan.examples[0],
        decision_time_ns=BASE + 3 * SECOND,
        features=(feature,),
        label=replace(plan.examples[0].label, horizon_ns=SECOND),
    )
    batch = materialize_training_temporal(replace(plan, examples=(example,)))
    history = [1.0, 1.01, 1.02]
    expected = (history[-1] - statistics.mean(history)) / statistics.stdev(
        history
    )
    assert batch.rows[0].features[0].value == pytest.approx(expected)
    # A real rolling calculation contaminated with the next target quote.
    contaminated = [*history, 1.03]
    false_value = (
        contaminated[-1] - statistics.mean(contaminated)
    ) / statistics.stdev(contaminated)
    assert false_value != pytest.approx(expected)
    wrong = replace(batch.rows[0].features[0], value=false_value)
    forged = replace(batch, rows=(replace(batch.rows[0], features=(wrong,)),))
    assert TrainingTemporalBatchV1.from_json(forged.to_json()) == forged
    with pytest.raises(ValueError, match="differ from replay"):
        replay_training_temporal(forged)


def test_native_label_only_conditioning_and_real_sibling_products(tmp_path):
    from histdatacom.data_quality.training_lineage import (
        build_training_ownership,
    )
    from histdatacom.forecasting.feature_artifacts import write_feature_artifact
    from tests.fixtures.training_substrate_v1 import (
        observed_source,
        published_product,
        with_products,
        macro_matrix,
    )

    source, version = observed_source(tmp_path / "native")
    a, _ = published_product(tmp_path / "a", version, member="a")
    b, _ = published_product(tmp_path / "b", version, member="b")
    matrix = macro_matrix(end=BASE + SECOND - 2)
    path = write_feature_artifact(matrix, tmp_path / "matrix")
    source = replace(
        with_products(source, a, b), derived_artifact_paths=(str(path),)
    )
    ownership = build_training_ownership(source)
    plan = temporal_plan(source, ownership)
    feature = TrainingTemporalFeatureV1(
        "macro",
        TemporalFeatureKind.VINTAGE,
        matrix_id=matrix.snapshot_id,
        column="macro_value",
        period_label="fixture-period",
    )
    first = replace(
        plan.examples[0],
        features=(feature,),
        product_manifest_id=a.manifest.manifest_id,
        ensemble_member_id="a",
        label=replace(plan.examples[0].label, horizon_ns=SECOND),
    )
    second = replace(
        first,
        key="sibling",
        product_manifest_id=b.manifest.manifest_id,
        ensemble_member_id="b",
    )
    batch = materialize_training_temporal(
        replace(plan, examples=(first, second))
    )
    for row in batch.rows:
        assert row.outcome.target_end_inclusive_ns == BASE + 2 * SECOND
        assert row.outcome.dependency_end_ns == BASE + 3 * SECOND + 1
        assert row.outcome.dependency_start_ns == BASE + SECOND
        assert row.outcome.target_unit_ids == (first.evidence_unit_id,)
        assert row.outcome.available_at_ns is None
    assert batch.maximum_dependency_span_ns >= 3 * SECOND + 1
    with pytest.raises(ValueError, match="sibling crosses"):
        replace(
            plan,
            examples=(first, replace(second, partition=TemporalPartition.TEST)),
        )
    premature = replace(first, label_cutoff_ns=BASE + 2 * SECOND)
    with pytest.raises(ValueError, match="conditioning support"):
        materialize_training_temporal(replace(plan, examples=(premature,)))
    future_warmup = TrainingTemporalFeatureV1(
        "future",
        TemporalFeatureKind.QUOTE_AT_DECISION,
        state_policy=TemporalStatePolicy.WARMUP_ONLY,
    )
    with pytest.raises(ValueError, match="future conditioning"):
        materialize_training_temporal(
            replace(plan, examples=(replace(first, features=(future_warmup,)),))
        )
    with pytest.raises(ValueError, match="member"):
        materialize_training_temporal(
            replace(
                plan, examples=(replace(first, ensemble_member_id="forged"),)
            )
        )


def test_future_unavailable_market_observation_does_not_change_earlier_label(
    vintage, tmp_path
):
    from histdatacom.data_quality.training_lineage import (
        build_training_ownership,
    )
    from histdatacom.forecasting.feature_artifacts import write_feature_artifact

    source, ownership, matrix = vintage
    original = materialize_training_temporal(vintage_plan(vintage))
    later = next(
        o
        for o in matrix.observations
        if o.definition.feature_key == "price" and o.period.label == "4"
    )
    changed = replace(
        later,
        value=None,
        available_at_ns=BASE + 9 * SECOND,
        published_at_ns=BASE + 9 * SECOND,
        definition=replace(later.definition, scale=100.0),
    )
    matrix = replace(
        matrix,
        observations=tuple(
            changed if o == later else o for o in matrix.observations
        ),
    )
    path = write_feature_artifact(matrix, tmp_path / "future")
    source = replace(source, derived_artifact_paths=(str(path),))
    ownership = build_training_ownership(source)
    current = materialize_training_temporal(
        vintage_plan((source, ownership, matrix))
    )
    assert current.rows[0].features == original.rows[0].features
    assert current.rows[0].outcome.value == original.rows[0].outcome.value
    assert (
        current.rows[0].outcome.target_record_ids
        == original.rows[0].outcome.target_record_ids
    )


@pytest.mark.parametrize(
    "value",
    [
        float.fromhex("0x0.0000000000001p-1022"),
        float.fromhex("0x1.fffffffffffffp+1023"),
    ],
)
def test_positive_extreme_midpoint_and_mean_do_not_underflow_or_overflow(
    source, value
):
    from histdatacom.data_quality.training_temporal_sources import (
        _midpoint,
        _TemporalPoint,
        temporal_feature_value,
        verify_training_temporal_source,
    )

    assert _midpoint(value, value) == value
    plan = temporal_plan(source[0], source[1])
    example = replace(plan.examples[0], decision_time_ns=BASE + 2 * SECOND)
    feature = TrainingTemporalFeatureV1(
        "mean", TemporalFeatureKind.QUOTE_MEAN, 2 * SECOND
    )
    # Pure arithmetic probe; these private synthetic points are not public evidence.
    points = tuple(
        _TemporalPoint(
            BASE + i * SECOND,
            value,
            str(i),
            None,
            BASE + i * SECOND,
            BASE + i * SECOND + 1,
        )
        for i in (1, 2)
    )
    result = temporal_feature_value(
        verify_training_temporal_source(plan), example, feature, points
    )
    assert result.value == value


def test_exact_horizon_and_endpoint_availability_are_required(vintage):
    plan = vintage_plan(vintage)
    example = plan.examples[0]
    bad = replace(example, label=replace(example.label, horizon_ns=SECOND + 1))
    with pytest.raises(ValueError, match="exact decision/horizon"):
        materialize_training_temporal(replace(plan, examples=(bad,)))


def test_empty_selection_does_not_accept_resealed_false_ownership(source):
    plan = temporal_plan(source[0], source[1])
    batch = materialize_training_temporal(plan)
    unit = next(
        u
        for u in plan.ownership.units
        if u.artifact_id == plan.examples[0].evidence_unit_id
    )
    false_unit = replace(unit, anchor_content_sha256="f" * 64)
    ownership = replace(
        plan.ownership,
        units=tuple(
            false_unit if u == unit else u for u in plan.ownership.units
        ),
    )
    split = replace(
        plan.split,
        ownership_id=ownership.artifact_id,
        assignments=tuple(
            (
                replace(a, evidence_unit_id=false_unit.artifact_id)
                if a.evidence_unit_id == unit.artifact_id
                else a
            )
            for a in plan.split.assignments
        ),
    )
    example = replace(plan.examples[0], evidence_unit_id=false_unit.artifact_id)
    wrong = replace(
        batch,
        plan=replace(
            plan, ownership=ownership, split=split, examples=(example,)
        ),
        rows=(replace(batch.rows[0], evidence_unit_id=false_unit.artifact_id),),
    )
    with pytest.raises(ValueError, match="frozen ownership"):
        training_temporal_rows(
            wrong, information_mode=plan.information_mode, example_keys=()
        )


def test_unused_quote_between_examples_and_matrix_target_does_not_leak(
    tmp_path,
):
    source, ownership, matrix = temporal_source(
        tmp_path, matrix=True, crossed_seconds=(2,)
    )
    plan = vintage_plan((source, ownership, matrix))
    first = replace(
        plan.examples[0],
        features=(
            TrainingTemporalFeatureV1(
                "quote", TemporalFeatureKind.QUOTE_AT_DECISION
            ),
        ),
    )
    second = replace(
        first,
        key="later",
        decision_time_ns=BASE + 3 * SECOND,
        label=replace(first.label, horizon_ns=SECOND),
    )
    plan = replace(
        plan,
        information_mode=TemporalInformationMode.EX_POST,
        examples=(first, second),
    )
    batch = materialize_training_temporal(plan)
    assert [r.features[0].value for r in batch.rows] == pytest.approx(
        [1.0, 1.02]
    )
    selected_crossed = replace(first, decision_time_ns=BASE + 2 * SECOND)
    with pytest.raises(ValueError, match="uncrossed"):
        materialize_training_temporal(
            replace(plan, examples=(selected_crossed,))
        )


def test_exact_quote_sampler_does_not_implicitly_carry_sparse_previous_quote(
    vintage,
):
    plan = vintage_plan(vintage)
    feature = TrainingTemporalFeatureV1(
        "quote", TemporalFeatureKind.QUOTE_AT_DECISION
    )
    with pytest.raises(ValueError, match="no lookback"):
        replace(feature, lookback_ns=SECOND)
    # Raw quote support ends at4s, but the normalized grid can still be requested
    # at an intermediate clock; fail in the feature before any endpoint label.
    example = replace(
        plan.examples[0],
        decision_time_ns=BASE + SECOND + 1,
        features=(feature,),
        label=replace(plan.examples[0].label, horizon_ns=SECOND),
    )
    with pytest.raises(
        ValueError, match="prior observed support|exact decision-time"
    ):
        materialize_training_temporal(
            replace(
                plan,
                information_mode=TemporalInformationMode.EX_POST,
                examples=(example,),
            )
        )


def test_quote_support_reused_once_within_closed_materialization(
    source, monkeypatch
):
    import histdatacom.data_quality.training_temporal_sources as module

    original = module._midpoint
    calls = []

    def counted(bid, ask):
        calls.append((bid, ask))
        return original(bid, ask)

    monkeypatch.setattr(module, "_midpoint", counted)
    plan = temporal_plan(source[0], source[1])
    plan = replace(
        plan,
        examples=tuple(replace(plan.examples[0], key=str(i)) for i in range(8)),
    )
    batch = materialize_training_temporal(plan)
    assert len(batch.rows) == 8
    assert len(calls) == 3


def test_duplicate_clock_uses_last_physical_ordinal_before_numerical_admission(
    tmp_path,
):
    source, ownership, _ = temporal_source(tmp_path, duplicate_crossed=True)
    batch = materialize_training_temporal(temporal_plan(source, ownership))
    assert batch.rows[0].features[0].value == pytest.approx(1.0)
    assert batch.rows[0].outcome.value == pytest.approx(math.log(1.02))


def test_selected_support_budget_refuses_before_value_calculation(
    tmp_path, monkeypatch
):
    import histdatacom.data_quality.training_temporal_sources as module

    source, ownership, _ = temporal_source(tmp_path, dense_points=4097)
    plan = temporal_plan(source, ownership)
    example = replace(
        plan.examples[0],
        label=replace(plan.examples[0].label, horizon_ns=5 * SECOND),
        label_cutoff_ns=BASE + 6 * SECOND,
    )
    calls = []

    def forbidden(*args):
        calls.append(args)
        raise AssertionError(
            "point math must follow complete bounded winner selection"
        )

    monkeypatch.setattr(module, "_midpoint", forbidden)
    with pytest.raises(ValueError, match="4096 points"):
        materialize_training_temporal(replace(plan, examples=(example,)))
    assert calls == []
