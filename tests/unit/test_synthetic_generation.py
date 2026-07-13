"""Tests for variable-cardinality empirical-motif candidate generation."""

from __future__ import annotations

from dataclasses import replace
import hashlib

import pytest

from histdatacom.runtime_contracts import ArtifactRef
from histdatacom.synthetic import (
    CANDIDATE_ONLY_CONSTRAINT_SET_ID,
    EMPIRICAL_MOTIF_GENERATOR_ID,
    BenchmarkCandidateKind,
    BenchmarkCandidateV1,
    BenchmarkControlKind,
    BenchmarkEventV1,
    BenchmarkExecutionEvidenceV1,
    BenchmarkProfileV1,
    BenchmarkScenarioV1,
    BenchmarkSplitKind,
    BenchmarkSplitV1,
    EmpiricalMotifBenchmarkGeneratorV1,
    EmpiricalMotifGeneratorConfigV1,
    InformationMode,
    MotifGenerationDecision,
    MotifGenerationStatus,
    ReferenceMotifConditionV1,
    ReferenceMotifIndexConfigV1,
    ReferenceMotifQueryV1,
    ReferenceMotifSourceEventV1,
    ReferenceMotifSourceWindowV1,
    ReferenceMotifSplitKind,
    ReferenceMotifSplitV1,
    ReferenceMotifTransformPolicyV1,
    ReconstructionRunV1,
    ReconstructionStoragePolicyV1,
    ReconstructionWindowV1,
    ReverseDegradationBenchmarkManifestV1,
    ReverseDegradationBenchmarkV1,
    SyntheticEventOrigin,
    SyntheticEventV1,
    build_benchmark_control_windows,
    build_reference_motif_index,
    generate_benchmark_candidate_window,
    generate_empirical_motif_candidates,
    query_reference_motifs,
)

BASE_NS = 1_700_000_000_000_000_000
SECOND = 1_000_000_000
SOURCE_VERSION_ID = "source-version:fixture-v1"
MEMBER_ID = "member-a"


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _condition(
    *,
    feed_epoch_id: str = "feed-epoch:modern-v1",
    intensity: float = 8.0,
    interarrival_ns: float | None = None,
    timestamp_precision_ns: float = 1_000_000.0,
    session_state: str = "active",
    special_tags: tuple[str, ...] = ("ordinary",),
    event_tags: tuple[str, ...] = (),
) -> ReferenceMotifConditionV1:
    cadence = interarrival_ns or SECOND / intensity if intensity else SECOND
    return ReferenceMotifConditionV1(
        symbol="EURUSD",
        feed_epoch_id=feed_epoch_id,
        session_state=session_state,
        currencies=("EUR", "USD"),
        active_sessions=("london",),
        special_tags=special_tags,
        event_tags=event_tags,
        return_regime="small-positive",
        range_regime="normal",
        volatility_regime="normal",
        spread_regime="tight",
        activity_regime="active" if intensity else "inactive",
        interarrival_regime="dense" if intensity >= 8 else "sparse",
        timestamp_precision="millisecond",
        price_precision="five-decimal",
        source_quality_state="eligible",
        metrics={
            "return_value": 0.0004,
            "range_value": 0.0015,
            "volatility": 0.001,
            "spread": 0.0002,
            "tick_intensity": intensity,
            "interarrival_ns": cadence,
            "timestamp_precision_ns": timestamp_precision_ns,
            "price_precision_digits": 5.0,
            "source_quality_score": 1.0,
        },
    )


def _splits() -> tuple[ReferenceMotifSplitV1, ...]:
    return (
        ReferenceMotifSplitV1(
            ReferenceMotifSplitKind.TRAIN,
            BASE_NS - 100 * SECOND,
            BASE_NS - 50 * SECOND,
        ),
        ReferenceMotifSplitV1(
            ReferenceMotifSplitKind.CALIBRATION,
            BASE_NS + 10 * SECOND,
            BASE_NS + 20 * SECOND,
        ),
        ReferenceMotifSplitV1(
            ReferenceMotifSplitKind.VALIDATION,
            BASE_NS + 30 * SECOND,
            BASE_NS + 40 * SECOND,
        ),
        ReferenceMotifSplitV1(
            ReferenceMotifSplitKind.FINAL_HOLDOUT,
            BASE_NS + 50 * SECOND,
            BASE_NS + 60 * SECOND,
        ),
    )


def _source_window(
    condition: ReferenceMotifConditionV1,
    *,
    event_offsets_ns: tuple[int, ...] = (
        0,
        125_000_000,
        250_000_000,
    ),
    quotes: tuple[tuple[float, float], ...] = (
        (1.1000, 1.1002),
        (1.1001, 1.1003),
        (1.10005, 1.10025),
    ),
    allow_spread_scaling: bool = False,
    name: str = "ordinary",
) -> ReferenceMotifSourceWindowV1:
    start = BASE_NS - 90 * SECOND
    events = tuple(
        ReferenceMotifSourceEventV1(
            event_time_ns=start + offset,
            event_sequence=index,
            bid=bid,
            ask=ask,
            source_row_id=index,
        )
        for index, (offset, (bid, ask)) in enumerate(
            zip(event_offsets_ns, quotes), start=1
        )
    )
    artifact = ArtifactRef(
        kind="augmented-tick-partition",
        path=f"artifacts/{name}.data",
        size_bytes=4_096,
        sha256=_digest(name),
        metadata={"projection": "motif-source-v1"},
    )
    return ReferenceMotifSourceWindowV1(
        source_series_id=f"ascii:T:EURUSD:histdata.com:{name}",
        period="202001",
        source_artifact=artifact,
        split_kind=ReferenceMotifSplitKind.TRAIN,
        condition=condition,
        events=events,
        first_known_at_ns=events[-1].event_time_ns,
        available_at_ns=events[-1].event_time_ns,
        transform_policy=ReferenceMotifTransformPolicyV1(
            min_time_scale=0.5,
            max_time_scale=2.0,
            min_price_scale=0.5,
            max_price_scale=2.0,
            max_time_warp_ratio=1.25,
            allow_spread_scaling=allow_spread_scaling,
        ),
    )


def _index(
    condition: ReferenceMotifConditionV1,
    **window_kwargs: object,
):
    return build_reference_motif_index(
        (_source_window(condition, **window_kwargs),),
        splits=_splits(),
        config=ReferenceMotifIndexConfigV1(min_cell_support=1),
    )


def _result(
    index,
    condition: ReferenceMotifConditionV1,
    *,
    minimum_support: int = 1,
    used_at_ns: int = BASE_NS + SECOND,
):
    return query_reference_motifs(
        index,
        ReferenceMotifQueryV1(
            condition=condition,
            information_mode=InformationMode.EX_POST_RECONSTRUCTION,
            used_at_ns=used_at_ns,
            min_cell_support=minimum_support,
        ),
    )


def _run(
    config: EmpiricalMotifGeneratorConfigV1,
    *,
    members: tuple[str, ...] = (MEMBER_ID,),
    storage_policy: ReconstructionStoragePolicyV1 | None = None,
) -> ReconstructionRunV1:
    return ReconstructionRunV1(
        symbols=("EURUSD",),
        source_version_ids=(SOURCE_VERSION_ID,),
        configuration_ids=(config.config_id,),
        ensemble_member_ids=members,
        base_seed=7,
        storage_policy=storage_policy or ReconstructionStoragePolicyV1(),
    )


def _anchor(
    run: ReconstructionRunV1,
    event_time_ns: int,
    *,
    sequence: int,
    row_id: int,
    member: str = MEMBER_ID,
    bid: float = 1.1000,
    ask: float = 1.1002,
) -> SyntheticEventV1:
    return SyntheticEventV1.observed(
        symbol="EURUSD",
        event_time_ns=event_time_ns,
        event_sequence=sequence,
        bid=bid,
        ask=ask,
        run_id=run.run_id,
        ensemble_member_id=member,
        source_version_id=SOURCE_VERSION_ID,
        source_series_id="ascii:T:EURUSD:histdata.com",
        source_period="202001",
        source_row_id=row_id,
    )


def _window(
    run: ReconstructionRunV1,
    start_ns: int,
    end_ns: int,
    *,
    member: str = MEMBER_ID,
    left_halo_ns: int = 0,
    right_lookahead_ns: int = 0,
) -> ReconstructionWindowV1:
    return ReconstructionWindowV1(
        run_id=run.run_id,
        ensemble_member_id=member,
        symbols=("EURUSD",),
        core_start_ns=start_ns,
        core_end_ns=end_ns,
        left_halo_ns=left_halo_ns,
        right_lookahead_ns=right_lookahead_ns,
    )


@pytest.mark.parametrize(
    ("intensity", "expected_count"),
    ((2.0, 1), (4.0, 3), (8.0, 7), (16.0, 15)),
)
def test_cardinality_tracks_conditioned_tick_intensity(
    intensity: float,
    expected_count: int,
) -> None:
    source_condition = _condition()
    target_condition = _condition(intensity=intensity)
    index = _index(source_condition)
    config = EmpiricalMotifGeneratorConfigV1()
    run = _run(config)
    left = _anchor(run, BASE_NS, sequence=0, row_id=1)
    right = _anchor(
        run,
        BASE_NS + SECOND,
        sequence=99,
        row_id=2,
        bid=1.1004,
        ask=1.1006,
    )

    batch = generate_empirical_motif_candidates(
        run=run,
        window=_window(run, BASE_NS, BASE_NS + SECOND + 1),
        left_anchor=left,
        right_anchor=right,
        query_result=_result(index, target_condition),
        config=config,
    )

    assert batch.status is MotifGenerationStatus.GENERATED
    assert batch.target_event_count == expected_count
    assert len(batch.events) == expected_count
    assert all(
        event.constraint_set_id == CANDIDATE_ONLY_CONSTRAINT_SET_ID
        for event in batch.events
    )


def test_cardinality_falls_back_to_conditioned_interarrival_cadence() -> None:
    source_condition = _condition()
    target_condition = _condition(intensity=1.0, interarrival_ns=200_000_000.0)
    target_condition = replace(
        target_condition,
        metrics={
            key: value
            for key, value in target_condition.metrics.items()
            if key != "tick_intensity"
        },
    )
    index = _index(source_condition)
    config = EmpiricalMotifGeneratorConfigV1()
    run = _run(config)
    left = _anchor(run, BASE_NS, sequence=0, row_id=1)
    right = _anchor(run, BASE_NS + SECOND, sequence=99, row_id=2)

    batch = generate_empirical_motif_candidates(
        run=run,
        window=_window(run, BASE_NS, BASE_NS + SECOND + 1),
        left_anchor=left,
        right_anchor=right,
        query_result=_result(index, target_condition),
        config=config,
    )

    assert batch.status is MotifGenerationStatus.GENERATED
    assert batch.target_event_count == 4
    assert [item.event_time_ns - BASE_NS for item in batch.events] == [
        200_000_000,
        400_000_000,
        600_000_000,
        800_000_000,
    ]


def test_lineage_anchor_seams_and_merged_observations_are_preserved() -> None:
    condition = _condition()
    index = _index(condition)
    config = EmpiricalMotifGeneratorConfigV1()
    run = _run(config)
    left = _anchor(run, BASE_NS, sequence=0, row_id=1)
    right = _anchor(
        run,
        BASE_NS + SECOND,
        sequence=99,
        row_id=2,
        bid=1.1004,
        ask=1.1006,
    )
    batch = generate_empirical_motif_candidates(
        run=run,
        window=_window(run, BASE_NS, BASE_NS + SECOND + 1),
        left_anchor=left,
        right_anchor=right,
        query_result=_result(index, condition),
        config=config,
    )
    stream = batch.merged_stream((left, right))

    observed = tuple(
        event
        for event in stream.events
        if event.origin is SyntheticEventOrigin.OBSERVED
    )
    assert observed == (left, right)
    assert [event.to_dict() for event in observed] == [
        left.to_dict(),
        right.to_dict(),
    ]
    assert stream.observed_event_count == 2
    assert stream.synthetic_event_count == 7
    assert all(
        left.event_time_ns < item.event_time_ns < right.event_time_ns
        for item in batch.events
    )
    assert all(item.bid > 0.0 and item.ask >= item.bid for item in batch.events)
    assert len(batch.event_lineage) == len(batch.events)
    assert all(
        batch.lineage_for(item.event_id).global_event_ordinal > 0
        for item in batch.events
    )
    assert all(item.time_warp_ratio <= 1.25 for item in batch.transformations)
    event_by_ordinal = {item.event_sequence: item for item in batch.events}
    left_mid = (left.bid + left.ask) / 2.0
    right_mid = (right.bid + right.ask) / 2.0
    left_spread = left.ask - left.bid
    right_spread = right.ask - right.bid
    for transform in batch.transformations:
        seam = event_by_ordinal[transform.output_end_ordinal]
        progress = (seam.event_time_ns - left.event_time_ns) / (
            right.event_time_ns - left.event_time_ns
        )
        expected_mid = left_mid + progress * (right_mid - left_mid)
        expected_spread = left_spread + progress * (right_spread - left_spread)
        assert seam.bid == round(expected_mid - expected_spread / 2.0, 5)
        assert seam.ask == round(expected_mid + expected_spread / 2.0, 5)
    assert EmpiricalMotifGeneratorConfigV1.from_json(config.to_json()) == config
    assert all(
        type(item).from_json(item.to_json()) == item
        for item in batch.transformations
    )
    assert all(
        type(item).from_json(item.to_json()) == item
        for item in batch.event_lineage
    )
    metadata = batch.metadata()
    assert metadata["candidate_only"] is True
    assert metadata["hard_carving_status"] == "not_evaluated"
    assert metadata["broker_conditioning_status"] == "not_applied"
    assert metadata["final_storage_status"] == "not_persisted"
    assert metadata["generator_config"] == config.to_dict()
    assert metadata["transformations_inline"] is False
    assert "transformations" not in metadata
    assert len(metadata["event_content_sha256"]) == 64

    changed_event = replace(
        batch.events[0],
        bid=batch.events[0].bid + 0.00001,
        ask=batch.events[0].ask + 0.00001,
    )
    changed_batch = replace(
        batch,
        events=(changed_event, *batch.events[1:]),
        batch_id="",
    )
    assert changed_batch.batch_id != batch.batch_id
    with pytest.raises(ValueError, match="event-to-transform lineage"):
        replace(
            batch,
            event_lineage=(
                replace(
                    batch.event_lineage[0],
                    requested_event_time_ns=(
                        batch.event_lineage[0].requested_event_time_ns + 1
                    ),
                ),
                *batch.event_lineage[1:],
            ),
            batch_id="",
        )


def test_retry_and_partitioning_do_not_change_candidate_identity_or_values() -> (
    None
):
    condition = _condition()
    index = _index(condition)
    config = EmpiricalMotifGeneratorConfigV1()
    run = _run(config, members=(MEMBER_ID, "member-b"))
    left = _anchor(run, BASE_NS, sequence=0, row_id=1)
    right = _anchor(
        run,
        BASE_NS + SECOND,
        sequence=99,
        row_id=2,
        bid=1.1004,
        ask=1.1006,
    )
    result = _result(index, condition)
    whole_window = _window(run, BASE_NS, BASE_NS + SECOND + 1)
    whole = generate_empirical_motif_candidates(
        run=run,
        window=whole_window,
        left_anchor=left,
        right_anchor=right,
        query_result=result,
        config=config,
    )
    retry = generate_empirical_motif_candidates(
        run=run,
        window=whole_window,
        left_anchor=left,
        right_anchor=right,
        query_result=result,
        config=config,
    )
    edge = BASE_NS + SECOND // 2
    first = generate_empirical_motif_candidates(
        run=run,
        window=_window(
            run,
            BASE_NS,
            edge,
            right_lookahead_ns=BASE_NS + SECOND + 1 - edge,
        ),
        left_anchor=left,
        right_anchor=right,
        query_result=result,
        config=config,
    )
    second = generate_empirical_motif_candidates(
        run=run,
        window=_window(
            run,
            edge,
            BASE_NS + SECOND + 1,
            left_halo_ns=edge - BASE_NS,
        ),
        left_anchor=left,
        right_anchor=right,
        query_result=result,
        config=config,
    )

    assert whole.events == retry.events
    partitioned = tuple(
        sorted(
            (*first.events, *second.events),
            key=lambda item: (item.event_time_ns, item.event_sequence),
        )
    )
    assert partitioned == whole.events
    assert len({item.event_id for item in partitioned}) == len(partitioned)


def test_ensemble_members_have_isolated_deterministic_candidate_identity() -> (
    None
):
    condition = _condition()
    index = _index(condition)
    config = EmpiricalMotifGeneratorConfigV1()
    run = _run(config, members=(MEMBER_ID, "member-b"))

    def generate(member: str):
        left = _anchor(run, BASE_NS, sequence=0, row_id=1, member=member)
        right = _anchor(
            run,
            BASE_NS + SECOND,
            sequence=99,
            row_id=2,
            member=member,
        )
        return generate_empirical_motif_candidates(
            run=run,
            window=_window(
                run,
                BASE_NS,
                BASE_NS + SECOND + 1,
                member=member,
            ),
            left_anchor=left,
            right_anchor=right,
            query_result=_result(index, condition),
            config=config,
        )

    first = generate(MEMBER_ID)
    first_retry = generate(MEMBER_ID)
    second = generate("member-b")

    assert first.events == first_retry.events
    assert [item.event_id for item in first.events] != [
        item.event_id for item in second.events
    ]
    assert all(item.ensemble_member_id == MEMBER_ID for item in first.events)
    assert all(item.ensemble_member_id == "member-b" for item in second.events)


def test_resource_estimation_tuning_does_not_change_semantic_event_identity() -> (
    None
):
    condition = _condition()
    index = _index(condition)
    first_config = EmpiricalMotifGeneratorConfigV1(
        estimated_bytes_per_event=512
    )
    second_config = EmpiricalMotifGeneratorConfigV1(
        estimated_bytes_per_event=1_024
    )
    assert first_config.config_id == second_config.config_id
    run = _run(first_config)
    left = _anchor(run, BASE_NS, sequence=0, row_id=1)
    right = _anchor(run, BASE_NS + SECOND, sequence=99, row_id=2)
    result = _result(index, condition)
    window = _window(run, BASE_NS, BASE_NS + SECOND + 1)

    first = generate_empirical_motif_candidates(
        run=run,
        window=window,
        left_anchor=left,
        right_anchor=right,
        query_result=result,
        config=first_config,
    )
    second = generate_empirical_motif_candidates(
        run=run,
        window=window,
        left_anchor=left,
        right_anchor=right,
        query_result=result,
        config=second_config,
    )

    assert first.events == second.events
    assert (
        second.resource_estimate.estimated_memory_bytes
        == 2 * first.resource_estimate.estimated_memory_bytes
    )


def test_same_timestamp_and_reversed_anchors_refuse_explicitly() -> None:
    condition = _condition()
    index = _index(condition)
    result = _result(index, condition, used_at_ns=BASE_NS)
    config = EmpiricalMotifGeneratorConfigV1()
    run = _run(config)
    window = _window(run, BASE_NS, BASE_NS + SECOND + 1)
    same_left = _anchor(run, BASE_NS, sequence=0, row_id=1)
    same_right = _anchor(run, BASE_NS, sequence=1, row_id=2)
    zero = generate_empirical_motif_candidates(
        run=run,
        window=window,
        left_anchor=same_left,
        right_anchor=same_right,
        query_result=result,
        config=config,
    )
    reversed_batch = generate_empirical_motif_candidates(
        run=run,
        window=window,
        left_anchor=_anchor(run, BASE_NS + SECOND, sequence=2, row_id=3),
        right_anchor=same_left,
        query_result=result,
        config=config,
    )

    assert zero.status is MotifGenerationStatus.REFUSED
    assert zero.decision is MotifGenerationDecision.ZERO_WIDTH_ANCHOR
    assert reversed_batch.decision is MotifGenerationDecision.REVERSED_ANCHOR
    assert not zero.events and not reversed_batch.events
    with pytest.raises(ValueError, match="right anchor boundary"):
        generate_empirical_motif_candidates(
            run=run,
            window=window,
            left_anchor=same_left,
            right_anchor=same_right,
            query_result=_result(index, condition),
            config=config,
        )


def test_duplicate_precision_timestamps_remain_stably_sequenced() -> None:
    condition = _condition(
        intensity=2_000.0,
        timestamp_precision_ns=1_000_000.0,
    )
    index = _index(
        condition,
        event_offsets_ns=(0, 500_000, 1_000_000),
    )
    config = EmpiricalMotifGeneratorConfigV1()
    run = _run(
        config,
        storage_policy=ReconstructionStoragePolicyV1(
            max_candidate_amplification=100.0
        ),
    )
    left = _anchor(run, BASE_NS, sequence=0, row_id=1)
    right = _anchor(
        run,
        BASE_NS + 5_000_000,
        sequence=99,
        row_id=2,
    )
    batch = generate_empirical_motif_candidates(
        run=run,
        window=_window(run, BASE_NS, BASE_NS + 5_000_001),
        left_anchor=left,
        right_anchor=right,
        query_result=_result(
            index,
            condition,
            used_at_ns=BASE_NS + 5_000_000,
        ),
        config=config,
    )

    positions = [
        (item.event_time_ns, item.event_sequence) for item in batch.events
    ]
    assert batch.status is MotifGenerationStatus.GENERATED
    assert len({item.event_time_ns for item in batch.events}) < len(
        batch.events
    )
    assert positions == sorted(positions)
    assert len(set(positions)) == len(positions)


def test_sparse_closed_and_large_gap_decisions_are_observable() -> None:
    source_condition = _condition()
    index = _index(source_condition)
    config = EmpiricalMotifGeneratorConfigV1()
    run = _run(config)
    left = _anchor(run, BASE_NS, sequence=0, row_id=1)
    right = _anchor(run, BASE_NS + SECOND, sequence=99, row_id=2)
    window = _window(run, BASE_NS, BASE_NS + SECOND + 1)

    sparse = generate_empirical_motif_candidates(
        run=run,
        window=window,
        left_anchor=left,
        right_anchor=right,
        query_result=_result(index, source_condition, minimum_support=2),
        config=config,
    )
    closed_condition = _condition(
        session_state="weekend_closed",
        intensity=0.0,
    )
    closed = generate_empirical_motif_candidates(
        run=run,
        window=window,
        left_anchor=left,
        right_anchor=right,
        query_result=_result(index, closed_condition, minimum_support=2),
        config=config,
    )
    large_right = _anchor(
        run,
        BASE_NS + 100 * SECOND,
        sequence=100,
        row_id=3,
    )
    large = generate_empirical_motif_candidates(
        run=run,
        window=_window(run, BASE_NS, BASE_NS + 100 * SECOND + 1),
        left_anchor=left,
        right_anchor=large_right,
        query_result=_result(
            index,
            source_condition,
            used_at_ns=BASE_NS + 100 * SECOND,
        ),
        config=config,
    )

    assert sparse.decision is MotifGenerationDecision.NO_SUPPORTED_CELL
    assert any(
        item["outcome"] == "sparse"
        for item in sparse.metadata()["backoff_attempts"]
    )
    assert closed.status is MotifGenerationStatus.EMPTY
    assert closed.decision is MotifGenerationDecision.CLOSED_SESSION
    assert large.status is MotifGenerationStatus.REFUSED
    assert large.decision is MotifGenerationDecision.RESOURCE_LIMIT
    assert large.resource_estimate.candidate_amplification > 25.0


@pytest.mark.parametrize(
    ("special_tags", "event_tags"),
    (("daily_rollover", ()), ("ordinary", ("us_cpi",))),
)
def test_rollover_and_event_window_conditioning_remains_in_lineage(
    special_tags: str,
    event_tags: tuple[str, ...],
) -> None:
    condition = _condition(
        special_tags=(special_tags,),
        event_tags=event_tags,
    )
    index = _index(condition, name=f"{special_tags}-{'-'.join(event_tags)}")
    config = EmpiricalMotifGeneratorConfigV1()
    run = _run(config)
    left = _anchor(run, BASE_NS, sequence=0, row_id=1)
    right = _anchor(run, BASE_NS + SECOND, sequence=99, row_id=2)
    batch = generate_empirical_motif_candidates(
        run=run,
        window=_window(run, BASE_NS, BASE_NS + SECOND + 1),
        left_anchor=left,
        right_anchor=right,
        query_result=_result(index, condition),
        config=config,
    )

    assert batch.status is MotifGenerationStatus.GENERATED
    assert batch.metadata()["condition"]["special_tags"] == [special_tags]
    assert batch.metadata()["condition"]["event_tags"] == list(event_tags)
    assert all(
        item.feed_epoch_id == condition.feed_epoch_id for item in batch.events
    )


def test_unsafe_spread_shape_refuses_the_entire_anchor_interval() -> None:
    condition = _condition()
    index = _index(
        condition,
        quotes=(
            (1.1000, 1.1200),
            (1.1095, 1.1105),
            (1.1000, 1.1200),
        ),
        allow_spread_scaling=True,
        name="unsafe-spread-shape",
    )
    config = EmpiricalMotifGeneratorConfigV1()
    run = _run(config)
    left = _anchor(run, BASE_NS, sequence=0, row_id=1)
    right = _anchor(run, BASE_NS + SECOND, sequence=99, row_id=2)
    batch = generate_empirical_motif_candidates(
        run=run,
        window=_window(run, BASE_NS, BASE_NS + SECOND + 1),
        left_anchor=left,
        right_anchor=right,
        query_result=_result(index, condition),
        config=config,
    )

    assert batch.status is MotifGenerationStatus.REFUSED
    assert batch.decision is MotifGenerationDecision.INVALID_TRANSFORMED_QUOTE
    assert not batch.events


def test_benchmark_adapter_uses_the_shared_reverse_degradation_interface() -> (
    None
):
    condition = _condition()
    index = _index(condition)
    config = EmpiricalMotifGeneratorConfigV1()
    run = _run(config)
    candidate = BenchmarkCandidateV1(
        kind=BenchmarkCandidateKind.CANDIDATE,
        method_id=EMPIRICAL_MOTIF_GENERATOR_ID,
        implementation_version="1.0.0",
        parameters={"motif_generator_config_id": config.config_id},
        ensemble_member_ids=(MEMBER_ID,),
    )
    scenario = BenchmarkScenarioV1(
        split_kind=BenchmarkSplitKind.VALIDATION,
        epoch_id=condition.feed_epoch_id,
        severity_id="sparse",
        observation_operator_id="observation-operator:fixture",
        degradation_parameters={"retention_probability": 0.25},
    )
    window = _window(run, BASE_NS, BASE_NS + SECOND + 1)
    degraded = (
        BenchmarkEventV1(
            source_event_id="left",
            symbol="EURUSD",
            event_time_ns=BASE_NS,
            event_sequence=0,
            bid=1.1000,
            ask=1.1002,
            epoch_id=condition.feed_epoch_id,
            session="london",
            event_state="ordinary",
            sparsity="sparse",
            anchor_id="left",
        ),
        BenchmarkEventV1(
            source_event_id="right",
            symbol="EURUSD",
            event_time_ns=BASE_NS + SECOND,
            event_sequence=99,
            bid=1.1004,
            ask=1.1006,
            epoch_id=condition.feed_epoch_id,
            session="london",
            event_state="ordinary",
            sparsity="sparse",
            anchor_id="right",
        ),
    )
    adapter = EmpiricalMotifBenchmarkGeneratorV1(
        candidate=candidate,
        run=run,
        motif_index=index,
        condition=condition,
        config=config,
    )

    candidate_window = generate_benchmark_candidate_window(
        adapter,
        candidate,
        degraded,
        scenario=scenario,
        window=window,
        ensemble_member_id=MEMBER_ID,
        execution=BenchmarkExecutionEvidenceV1(
            attempted=True,
            converged=True,
            peak_memory_bytes=7 * config.estimated_bytes_per_event,
        ),
    )

    assert candidate_window.candidate_id == candidate.candidate_id
    assert len(candidate_window.events) == 9
    assert {item.source_event_id for item in candidate_window.events} >= {
        "left",
        "right",
    }
    assert (
        sum(
            item.sparsity == "empirical-motif-candidate"
            for item in candidate_window.events
        )
        == 7
    )


def test_reverse_degradation_scorecard_compares_motif_generator_to_controls() -> (
    None
):
    config = EmpiricalMotifGeneratorConfigV1()
    run = _run(config)
    candidate = BenchmarkCandidateV1(
        kind=BenchmarkCandidateKind.CANDIDATE,
        method_id=EMPIRICAL_MOTIF_GENERATOR_ID,
        implementation_version="1.0.0",
        parameters={"motif_generator_config_id": config.config_id},
        ensemble_member_ids=(MEMBER_ID,),
    )

    def control(
        kind: BenchmarkControlKind,
        method_id: str,
        parameters: dict[str, object] | None = None,
    ) -> BenchmarkCandidateV1:
        return BenchmarkCandidateV1(
            kind=BenchmarkCandidateKind.CONTROL,
            method_id=method_id,
            implementation_version="fixture-v1",
            parameters=parameters or {},
            ensemble_member_ids=("control",),
            control_kind=kind,
        )

    calibration_start = BASE_NS - 20 * SECOND
    validation_start = BASE_NS
    validation_end = BASE_NS + SECOND + 1
    holdout_start = BASE_NS + 20 * SECOND
    holdout_end = holdout_start + SECOND + 1
    splits = (
        BenchmarkSplitV1(
            BenchmarkSplitKind.CALIBRATION,
            calibration_start,
            BASE_NS - 10 * SECOND,
        ),
        BenchmarkSplitV1(
            BenchmarkSplitKind.VALIDATION,
            validation_start,
            validation_end,
        ),
        BenchmarkSplitV1(
            BenchmarkSplitKind.FINAL_HOLDOUT,
            holdout_start,
            holdout_end,
        ),
    )
    scenarios = tuple(
        BenchmarkScenarioV1(
            split_kind=split_kind,
            epoch_id=epoch_id,
            severity_id=severity_id,
            observation_operator_id=f"operator:{epoch_id}:{severity_id}",
            degradation_parameters={"retention_probability": retention},
        )
        for split_kind, epoch_id, severity_id, retention in (
            (
                BenchmarkSplitKind.VALIDATION,
                "feed-epoch:modern-v1",
                "mild",
                0.5,
            ),
            (
                BenchmarkSplitKind.VALIDATION,
                "feed-epoch:legacy-v1",
                "severe",
                0.25,
            ),
            (
                BenchmarkSplitKind.FINAL_HOLDOUT,
                "feed-epoch:modern-v1",
                "severe",
                0.25,
            ),
            (
                BenchmarkSplitKind.FINAL_HOLDOUT,
                "feed-epoch:legacy-v1",
                "mild",
                0.5,
            ),
        )
    )
    manifest = ReverseDegradationBenchmarkManifestV1(
        run_id=run.run_id,
        information_manifest_id="information-manifest:fixture",
        profile=BenchmarkProfileV1(),
        splits=splits,
        scenarios=scenarios,
        candidates=(
            control(BenchmarkControlKind.NO_FILL, "no-fill"),
            control(
                BenchmarkControlKind.LINEAR_INTERPOLATION,
                "linear",
                {"interval_ns": 125_000_000},
            ),
            control(
                BenchmarkControlKind.RESAMPLE_LAST,
                "resample-last",
                {"interval_ns": 125_000_000},
            ),
            control(
                BenchmarkControlKind.EMPIRICAL_OVERLAY,
                "empirical-overlay",
                {"source_schema": "synthetic-tick-generation.v1"},
            ),
            candidate,
        ),
    )
    engine = ReverseDegradationBenchmarkV1(manifest)
    for scenario in manifest.scenarios:
        start_ns = (
            validation_start
            if scenario.split_kind is BenchmarkSplitKind.VALIDATION
            else holdout_start
        )
        end_ns = (
            validation_end
            if scenario.split_kind is BenchmarkSplitKind.VALIDATION
            else holdout_end
        )
        condition = _condition(feed_epoch_id=scenario.epoch_id)
        index = _index(condition, name=f"benchmark-{scenario.scenario_id}")
        window = _window(run, start_ns, end_ns)
        reference = tuple(
            BenchmarkEventV1(
                source_event_id=f"{scenario.scenario_id}:source:{ordinal}",
                symbol="EURUSD",
                event_time_ns=start_ns + ordinal * 125_000_000,
                event_sequence=ordinal,
                bid=1.1000 + ordinal * 0.00005,
                ask=1.1002 + ordinal * 0.00005,
                epoch_id=scenario.epoch_id,
                session=condition.session_state,
                event_state=condition.activity_regime,
                sparsity=scenario.severity_id,
                anchor_id=(f"anchor:{ordinal}" if ordinal in {0, 8} else None),
            )
            for ordinal in range(9)
        )
        degraded = (reference[0], reference[-1])
        adapter = EmpiricalMotifBenchmarkGeneratorV1(
            candidate=candidate,
            run=run,
            motif_index=index,
            condition=condition,
            config=config,
        )
        motif_window = generate_benchmark_candidate_window(
            adapter,
            candidate,
            degraded,
            scenario=scenario,
            window=window,
            ensemble_member_id=MEMBER_ID,
            execution=BenchmarkExecutionEvidenceV1(
                attempted=True,
                converged=True,
            ),
        )
        controls = build_benchmark_control_windows(
            manifest,
            scenario,
            window,
            degraded,
            empirical_overlay_events=degraded,
        )
        engine.consume_window(
            scenario_id=scenario.scenario_id,
            window=window,
            reference_events=reference,
            degraded_events=degraded,
            candidate_windows=(*controls, motif_window),
        )

    scorecard = engine.finalize()
    motif_scores = tuple(
        item
        for item in scorecard.candidate_scores
        if item.candidate_id == candidate.candidate_id
    )
    assert scorecard.automatic_winner is False
    assert len(motif_scores) == len(scenarios)
    assert all(
        "mean_soft_loss_delta" in item.relative_to_no_fill
        for item in motif_scores
    )
    assert all(
        item.execution_summary["converged_count"] == 1 for item in motif_scores
    )
