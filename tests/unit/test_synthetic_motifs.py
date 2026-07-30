"""Tests for deterministic, split-safe empirical reference motifs."""

from __future__ import annotations

from dataclasses import replace
import hashlib
import json
from pathlib import Path

import polars as pl
import pytest

from histdatacom.runtime_contracts import ArtifactRef
from histdatacom.synthetic import (
    REFERENCE_MOTIF_ARTIFACT_KIND,
    REFERENCE_MOTIF_FRAGMENT_SCHEMA_VERSION,
    REFERENCE_MOTIF_INDEX_SCHEMA_VERSION,
    InformationMode,
    InformationScope,
    InformationSplitKind,
    InformationStage,
    ReferenceMotifConditionV1,
    ReferenceMotifFragmentV1,
    ReferenceMotifIndexConfigV1,
    ReferenceMotifIndexV1,
    ReferenceMotifLeakageError,
    ReferenceMotifQueryResultV1,
    ReferenceMotifQueryStatus,
    ReferenceMotifQueryV1,
    ReferenceMotifResourceLimitError,
    ReferenceMotifSourceEventV1,
    ReferenceMotifSourceWindowV1,
    ReferenceMotifSplitKind,
    ReferenceMotifSplitV1,
    ReferenceMotifTransformPolicyV1,
    ReferenceMotifTransition,
    ReconstructionInformationManifestV1,
    ReconstructionInformationPolicyV1,
    ReconstructionInformationSplitV1,
    ReconstructionRunV1,
    ReconstructionWindowV1,
    audit_reconstruction_information,
    build_reference_motif_index,
    extract_reference_motif_fragment,
    query_reference_motifs,
    read_reference_motif_index,
    reference_motif_condition_from_quotes,
    reconstruction_information_window_plan_id,
    reference_motif_information_inputs,
    reference_motif_source_window_from_training_frame,
    write_reference_motif_index,
)

BASE_NS = 1_700_000_000_000_000_000
STEP = 1_000_000_000


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _artifact(name: str = "eurusd-modern") -> ArtifactRef:
    return ArtifactRef(
        kind="augmented-tick-partition",
        path=f"artifacts/{name}.data",
        size_bytes=4096,
        sha256=_digest(name),
        metadata={"training_columns": 521, "projection": "motif-source-v1"},
    )


def _splits() -> tuple[ReferenceMotifSplitV1, ...]:
    return (
        ReferenceMotifSplitV1(
            ReferenceMotifSplitKind.TRAIN,
            BASE_NS,
            BASE_NS + 1500 * STEP,
        ),
        ReferenceMotifSplitV1(
            ReferenceMotifSplitKind.CALIBRATION,
            BASE_NS + 2000 * STEP,
            BASE_NS + 3000 * STEP,
        ),
        ReferenceMotifSplitV1(
            ReferenceMotifSplitKind.VALIDATION,
            BASE_NS + 4000 * STEP,
            BASE_NS + 5000 * STEP,
        ),
        ReferenceMotifSplitV1(
            ReferenceMotifSplitKind.FINAL_HOLDOUT,
            BASE_NS + 6000 * STEP,
            BASE_NS + 7000 * STEP,
        ),
    )


def _condition(
    *,
    symbol: str = "EURUSD",
    session: str = "active",
    active_sessions: tuple[str, ...] = ("london",),
    event_tags: tuple[str, ...] = (),
    spread: float = 0.0002,
    volatility: float = 0.001,
) -> ReferenceMotifConditionV1:
    return ReferenceMotifConditionV1(
        symbol=symbol,
        feed_epoch_id="feed-epoch:modern-v1",
        session_state=session,
        active_sessions=active_sessions,
        overlap_tags=("london_new_york",) if len(active_sessions) > 1 else (),
        special_tags=("ordinary",),
        holiday_tags=(),
        event_tags=event_tags,
        return_regime="small-positive",
        range_regime="normal",
        volatility_regime="normal",
        spread_regime="tight",
        activity_regime="active",
        interarrival_regime="dense",
        timestamp_precision="millisecond",
        price_precision="five-decimal",
        source_quality_state="eligible",
        metrics={
            "return_value": 0.0004,
            "range_value": 0.0015,
            "volatility": volatility,
            "spread": spread,
            "tick_intensity": 8.0,
            "interarrival_ns": 125_000_000.0,
            "timestamp_precision_ns": 1_000_000.0,
            "price_precision_digits": 5.0,
            "source_quality_score": 1.0,
        },
    )


def _split_start(kind: ReferenceMotifSplitKind) -> int:
    return {
        ReferenceMotifSplitKind.TRAIN: BASE_NS,
        ReferenceMotifSplitKind.CALIBRATION: BASE_NS + 2000 * STEP,
        ReferenceMotifSplitKind.VALIDATION: BASE_NS + 4000 * STEP,
        ReferenceMotifSplitKind.FINAL_HOLDOUT: BASE_NS + 6000 * STEP,
    }[kind]


def _window(
    offset: int,
    *,
    split: ReferenceMotifSplitKind = ReferenceMotifSplitKind.TRAIN,
    condition: ReferenceMotifConditionV1 | None = None,
    shape: tuple[float, ...] = (0.0, 0.0001, -0.00005),
    time_shape: tuple[int, ...] = (0, 10, 30),
    source_name: str = "eurusd-modern",
    eligible: bool = True,
    available_at_ns: int | None = None,
) -> ReferenceMotifSourceWindowV1:
    start = _split_start(split) + offset * STEP
    events = tuple(
        ReferenceMotifSourceEventV1(
            event_time_ns=start + time_offset * 1_000_000,
            event_sequence=index,
            bid=round(1.1 + movement, 8),
            ask=round(1.1002 + movement, 8),
            source_row_id=offset * 10 + index,
        )
        for index, (time_offset, movement) in enumerate(
            zip(time_shape, shape), start=1
        )
    )
    availability = (
        available_at_ns
        if available_at_ns is not None
        else events[-1].event_time_ns
    )
    return ReferenceMotifSourceWindowV1(
        source_series_id=f"ascii:T:EURUSD:histdata.com:{source_name}",
        period="202001",
        source_artifact=_artifact(source_name),
        split_kind=split,
        condition=condition or _condition(),
        events=events,
        first_known_at_ns=availability,
        available_at_ns=availability,
        data_quality_eligible=eligible,
        data_quality_reasons=() if eligible else ("DQ_NEGATIVE_SPREAD",),
        transform_policy=ReferenceMotifTransformPolicyV1(
            min_time_scale=0.75,
            max_time_scale=1.5,
            min_price_scale=0.8,
            max_price_scale=1.25,
            max_time_warp_ratio=1.1,
        ),
    )


def _index(
    windows: tuple[ReferenceMotifSourceWindowV1, ...] | None = None,
    *,
    config: ReferenceMotifIndexConfigV1 | None = None,
) -> ReferenceMotifIndexV1:
    values = windows or (
        _window(10, condition=_condition(spread=0.00018)),
        _window(
            20,
            condition=_condition(spread=0.00022),
            shape=(0.0, 0.0002, 0.0001),
        ),
        _window(
            30,
            condition=_condition(
                active_sessions=("new_york",),
                event_tags=("us_cpi",),
                spread=0.0004,
            ),
            shape=(0.0, -0.0002, 0.0003),
        ),
    )
    return build_reference_motif_index(
        values,
        splits=_splits(),
        config=config or ReferenceMotifIndexConfigV1(min_cell_support=1),
    )


def test_augmented_training_frame_projects_to_compact_fragment() -> None:
    rows = {
        "series_id": ["ascii:T:EURUSD:histdata.com"] * 3,
        "period": ["202001"] * 3,
        "row_id": [101, 102, 103],
        "event_seq": [1, 2, 3],
        "symbol": ["EURUSD"] * 3,
        "timestamp_utc_ms": [
            1_700_000_000_000,
            1_700_000_000_010,
            1_700_000_000_030,
        ],
        "bid": [1.1, 1.1001, 1.1001],
        "ask": [1.1002, 1.1003, 1.1004],
        "training_usable": [True, True, True],
        "training_exclusion_reason_code": [0, 0, 0],
    }
    rows.update(
        {f"augmented_{index}": [float(index)] * 3 for index in range(521)}
    )
    frame = pl.DataFrame(rows)

    window = reference_motif_source_window_from_training_frame(
        frame,
        source_artifact=_artifact(),
        split_kind=ReferenceMotifSplitKind.TRAIN,
        condition=_condition(),
        first_known_at_ns=1_700_000_000_030_000_000,
        available_at_ns=1_700_000_000_030_000_000,
    )
    fragment = extract_reference_motif_fragment(window)
    payload = fragment.to_dict()

    assert fragment.schema_version == REFERENCE_MOTIF_FRAGMENT_SCHEMA_VERSION
    assert fragment.event_offsets_ns == (0, 10_000_000, 30_000_000)
    assert fragment.transitions == (
        ReferenceMotifTransition.START,
        ReferenceMotifTransition.BOTH,
        ReferenceMotifTransition.ASK,
    )
    assert fragment.source_row_ids == (101, 102, 103)
    assert fragment.end_bid == pytest.approx(1.1001)
    assert fragment.end_ask == pytest.approx(1.1004)
    assert not any("augmented_" in key for key in payload)
    assert "events" not in payload
    assert len(json.dumps(payload)) < 16_384
    assert ReferenceMotifSourceWindowV1.from_dict(window.to_dict()) == window
    assert ReferenceMotifFragmentV1.from_dict(payload) == fragment

    inconsistent = frame.with_columns(pl.lit(False).alias("training_usable"))
    with pytest.raises(ValueError, match="usability and exclusion reason"):
        reference_motif_source_window_from_training_frame(
            inconsistent,
            source_artifact=_artifact(),
            split_kind=ReferenceMotifSplitKind.TRAIN,
            condition=_condition(),
            first_known_at_ns=1_700_000_000_030_000_000,
            available_at_ns=1_700_000_000_030_000_000,
        )


def test_quote_feature_schema_is_fixed_and_encodes_weekday() -> None:
    condition = reference_motif_condition_from_quotes(
        symbol="EURUSD",
        feed_epoch_id="technology_epoch_04",
        session_state="london",
        event_times_ns=(
            1_704_067_200_000_000_000,
            1_704_067_200_100_000_000,
            1_704_067_200_400_000_000,
        ),
        bids=(1.10000, 1.10002, 1.10008),
        asks=(1.10010, 1.10012, 1.10018),
        event_tags=("market_context:scheduled_macro",),
    )

    assert condition.special_tags == ("weekday:monday",)
    assert condition.event_tags == ("market_context:scheduled_macro",)
    assert condition.return_regime == "up_large"
    assert condition.volatility_regime in {"medium", "high"}
    assert condition.activity_regime == "high"
    assert condition.timestamp_precision == "millisecond"
    assert condition.metrics["timestamp_precision_ns"] == 100_000_000.0

    with pytest.raises(ValueError, match="aligned quote rows"):
        reference_motif_condition_from_quotes(
            symbol="EURUSD",
            feed_epoch_id="technology_epoch_04",
            session_state="london",
            event_times_ns=(1, 2),
            bids=(1.0,),
            asks=(1.1, 1.2),
        )


def test_compact_fragment_revalidates_quote_and_availability_invariants() -> (
    None
):
    window = _window(10)
    fragment = extract_reference_motif_fragment(window)

    with pytest.raises(ValueError, match="negative spread"):
        replace(
            fragment,
            ask_deltas=(0.0, -0.0002, 0.0002),
            transitions=(
                ReferenceMotifTransition.START,
                ReferenceMotifTransition.BOTH,
                ReferenceMotifTransition.BOTH,
            ),
            fragment_id="",
        )
    with pytest.raises(ValueError, match="transition marks disagree"):
        replace(
            fragment,
            transitions=(
                ReferenceMotifTransition.START,
                ReferenceMotifTransition.BID,
                ReferenceMotifTransition.ASK,
            ),
            fragment_id="",
        )
    with pytest.raises(ValueError, match="before its last observation"):
        replace(
            window,
            first_known_at_ns=window.end_ns - 1,
            available_at_ns=window.end_ns,
            source_window_id="",
        )


def test_build_is_order_independent_train_only_and_fully_accounted() -> None:
    windows = (
        _window(10),
        _window(20, shape=(0.0, 0.0002, 0.0001)),
        _window(30, shape=(0.0, -0.0002, 0.0003)),
        _window(
            10,
            split=ReferenceMotifSplitKind.CALIBRATION,
            shape=(0.0, 0.0003, 0.0005, 0.0004),
            time_shape=(0, 3, 17, 51),
            source_name="calibration",
        ),
        _window(
            10,
            split=ReferenceMotifSplitKind.VALIDATION,
            shape=(0.0, -0.0004, 0.0002, 0.0007, 0.0001),
            time_shape=(0, 2, 9, 28, 90),
            source_name="validation",
        ),
        _window(
            10,
            split=ReferenceMotifSplitKind.FINAL_HOLDOUT,
            shape=(0.0, 0.0002, -0.0006, 0.0009, 0.0004, -0.0001),
            time_shape=(0, 5, 12, 25, 61, 130),
            source_name="holdout",
        ),
        _window(40, eligible=False, source_name="ineligible"),
    )
    config = ReferenceMotifIndexConfigV1(
        max_fragments=2,
        min_cell_support=1,
    )

    first = build_reference_motif_index(
        windows, splits=_splits(), config=config
    )
    second = build_reference_motif_index(
        reversed(windows), splits=_splits(), config=config
    )

    assert first == second
    assert first.schema_version == REFERENCE_MOTIF_INDEX_SCHEMA_VERSION
    assert first.source_window_count == 7
    assert len(first.fragments) == 2
    assert {item.split_kind for item in first.fragments} == {
        ReferenceMotifSplitKind.TRAIN
    }
    assert first.excluded_split_counts == {
        "calibration": 1,
        "final_holdout": 1,
        "validation": 1,
    }
    assert first.ineligible_window_count == 1
    assert first.selection_omitted_count == 1
    assert first.leakage_comparison_count >= 0
    assert first.index_id == second.index_id


def test_cross_split_normalized_shape_near_duplicate_fails_closed() -> None:
    train = _window(10, shape=(0.0, 0.0001, -0.00005))
    holdout = _window(
        10,
        split=ReferenceMotifSplitKind.FINAL_HOLDOUT,
        shape=(0.0, 0.0002, -0.0001),
        time_shape=(0, 20, 60),
        source_name="holdout-near-duplicate",
    )

    with pytest.raises(ReferenceMotifLeakageError) as raised:
        build_reference_motif_index(
            (train, holdout),
            splits=_splits(),
            config=ReferenceMotifIndexConfigV1(min_cell_support=1),
        )

    assert (
        raised.value.findings[0]["rule"] == "cross_split_near_duplicate_shape"
    )
    assert raised.value.findings[0]["split_kinds"] == ["final_holdout", "train"]


def test_cross_split_source_guard_catches_adjacent_window_leakage() -> None:
    splits = (
        ReferenceMotifSplitV1(
            ReferenceMotifSplitKind.TRAIN, BASE_NS, BASE_NS + 1000
        ),
        ReferenceMotifSplitV1(
            ReferenceMotifSplitKind.CALIBRATION, BASE_NS + 1000, BASE_NS + 2000
        ),
        ReferenceMotifSplitV1(
            ReferenceMotifSplitKind.VALIDATION, BASE_NS + 2000, BASE_NS + 3000
        ),
        ReferenceMotifSplitV1(
            ReferenceMotifSplitKind.FINAL_HOLDOUT,
            BASE_NS + 3000,
            BASE_NS + 4000,
        ),
    )
    train = replace(
        _window(1),
        events=(
            ReferenceMotifSourceEventV1(BASE_NS + 900, 1, 1.1, 1.1002, 1),
            ReferenceMotifSourceEventV1(BASE_NS + 990, 2, 1.1001, 1.1003, 2),
        ),
        source_window_id="",
    )
    calibration = replace(
        _window(1, split=ReferenceMotifSplitKind.CALIBRATION),
        source_artifact=train.source_artifact,
        events=(
            ReferenceMotifSourceEventV1(BASE_NS + 1001, 1, 1.2, 1.2003, 3),
            ReferenceMotifSourceEventV1(BASE_NS + 1100, 2, 1.2002, 1.2005, 4),
        ),
        source_window_id="",
    )

    with pytest.raises(ReferenceMotifLeakageError) as raised:
        build_reference_motif_index(
            (train, calibration),
            splits=splits,
            config=ReferenceMotifIndexConfigV1(
                min_events_per_fragment=2,
                min_cell_support=1,
                source_overlap_guard_ns=20,
            ),
        )

    assert any(
        item["rule"] == "cross_split_source_window_overlap"
        for item in raised.value.findings
    )


def test_artifact_round_trip_verifies_content_identity(tmp_path: Path) -> None:
    index = _index()
    path = tmp_path / "reference-motifs.json"

    reference = write_reference_motif_index(index, path)
    restored = read_reference_motif_index(path, artifact_ref=reference)

    assert reference.kind == REFERENCE_MOTIF_ARTIFACT_KIND
    assert reference.metadata["index_id"] == index.index_id
    assert restored == index
    path.write_text(f"{index.to_json()} \n", encoding="utf-8")
    with pytest.raises(ValueError, match="artifact (size|sha256) differs"):
        read_reference_motif_index(path, artifact_ref=reference)


def test_query_uses_distance_deterministic_ties_and_explicit_backoff() -> None:
    index = _index()
    exact_query = ReferenceMotifQueryV1(
        condition=_condition(spread=0.00019),
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
        used_at_ns=BASE_NS + 8000 * STEP,
        max_results=2,
        min_cell_support=2,
    )

    first = query_reference_motifs(index, exact_query)
    second = query_reference_motifs(
        index, ReferenceMotifQueryV1.from_dict(exact_query.to_dict())
    )

    assert first == second
    assert first.status is ReferenceMotifQueryStatus.MATCHED
    assert [item.rank for item in first.matches] == [1, 2]
    assert [item.distance for item in first.matches] == sorted(
        item.distance for item in first.matches
    )
    assert first.matches[0].fragment.source_artifact.sha256
    assert first.matches[0].fragment.source_row_ids
    assert first.matches[0].cell_support == 2
    assert first.backoff_attempts[-1].outcome == "selected"
    assert ReferenceMotifQueryResultV1.from_json(first.to_json()) == first

    fallback = query_reference_motifs(
        index,
        ReferenceMotifQueryV1(
            condition=_condition(
                active_sessions=("london",),
                event_tags=("ecb_press_conference",),
            ),
            information_mode=InformationMode.EX_POST_RECONSTRUCTION,
            used_at_ns=BASE_NS + 8000 * STEP,
            min_cell_support=2,
        ),
    )
    assert fallback.status is ReferenceMotifQueryStatus.MATCHED
    assert fallback.backoff_attempts[0].level == "exact"
    assert fallback.backoff_attempts[0].outcome == "sparse"
    assert fallback.matches[0].backoff_level == "symbol_epoch_session"


def test_point_in_time_query_filters_future_motifs_and_binds_information() -> (
    None
):
    available = BASE_NS + 500 * STEP
    index = _index(
        (
            _window(10, available_at_ns=available),
            _window(
                20,
                available_at_ns=BASE_NS + 900 * STEP,
                shape=(0.0, 0.0002, 0.0001),
            ),
        ),
        config=ReferenceMotifIndexConfigV1(min_cell_support=1),
    )
    query = ReferenceMotifQueryV1(
        condition=_condition(),
        information_mode=InformationMode.EX_ANTE_SIMULATION,
        used_at_ns=BASE_NS + 700 * STEP,
        as_of_ns=BASE_NS + 700 * STEP,
        min_cell_support=1,
    )

    result = query_reference_motifs(index, query)
    inputs = reference_motif_information_inputs(
        result, run_id="run:point-in-time"
    )

    assert result.status is ReferenceMotifQueryStatus.MATCHED
    assert result.hidden_by_availability_count == 1
    assert len(result.matches) == 1
    assert len(inputs) == 1
    assert inputs[0].information_mode is InformationMode.EX_ANTE_SIMULATION
    assert inputs[0].stage is InformationStage.MOTIF_SELECTION
    assert inputs[0].scope is InformationScope.EMPIRICAL_MOTIF
    assert inputs[0].split_kind is InformationSplitKind.TRAIN
    assert inputs[0].allowed_lookahead_ns == 0
    assert inputs[0].observation_end_ns <= inputs[0].used_at_ns

    policy = ReconstructionInformationPolicyV1(
        InformationMode.EX_ANTE_SIMULATION
    )
    run = ReconstructionRunV1(
        symbols=("EURUSD",),
        source_version_ids=(index.index_id,),
        configuration_ids=(policy.policy_id, index.config.config_id),
        ensemble_member_ids=("member-1",),
        base_seed=438,
    )
    target_window = ReconstructionWindowV1(
        run_id=run.run_id,
        ensemble_member_id="member-1",
        symbols=run.symbols,
        core_start_ns=query.used_at_ns,
        core_end_ns=query.used_at_ns + STEP,
    )
    manifest = ReconstructionInformationManifestV1(
        run_id=run.run_id,
        policy_id=policy.policy_id,
        information_mode=policy.information_mode,
        window_plan_id=reconstruction_information_window_plan_id(
            (target_window,)
        ),
        inputs=reference_motif_information_inputs(result, run_id=run.run_id),
        splits=(
            ReconstructionInformationSplitV1(
                InformationSplitKind.TRAIN,
                BASE_NS,
                BASE_NS + 1500 * STEP,
            ),
            ReconstructionInformationSplitV1(
                InformationSplitKind.CALIBRATION,
                BASE_NS + 2000 * STEP,
                BASE_NS + 3000 * STEP,
            ),
            ReconstructionInformationSplitV1(
                InformationSplitKind.VALIDATION,
                BASE_NS + 4000 * STEP,
                BASE_NS + 7000 * STEP,
            ),
        ),
    )
    audit = audit_reconstruction_information(
        manifest,
        policy,
        run=run,
        windows=(target_window,),
    )
    assert audit.accepted is True

    with pytest.raises(ValueError, match="requires as_of_ns"):
        ReferenceMotifQueryV1(
            condition=_condition(),
            information_mode=InformationMode.EX_ANTE_SIMULATION,
            used_at_ns=BASE_NS,
        )


def test_point_in_time_query_refuses_when_every_match_is_future() -> None:
    index = _index(
        (_window(10, available_at_ns=BASE_NS + 900 * STEP),),
        config=ReferenceMotifIndexConfigV1(min_cell_support=1),
    )
    result = query_reference_motifs(
        index,
        ReferenceMotifQueryV1(
            condition=_condition(),
            information_mode=InformationMode.EX_ANTE_SIMULATION,
            used_at_ns=BASE_NS + 100 * STEP,
            as_of_ns=BASE_NS + 100 * STEP,
            min_cell_support=1,
        ),
    )

    assert result.status is ReferenceMotifQueryStatus.NOT_AVAILABLE_AS_OF
    assert not result.matches
    assert result.hidden_by_availability_count == 1


def test_resource_limits_and_split_order_fail_before_indexing() -> None:
    config = ReferenceMotifIndexConfigV1(
        max_source_windows=2,
        min_cell_support=1,
    )
    with pytest.raises(ReferenceMotifResourceLimitError):
        build_reference_motif_index(
            (_window(10), _window(20), _window(30)),
            splits=_splits(),
            config=config,
        )
    with pytest.raises(ValueError, match="must be train"):
        build_reference_motif_index(
            (_window(10),),
            splits=tuple(reversed(_splits())),
            config=ReferenceMotifIndexConfigV1(min_cell_support=1),
        )


def test_period_scale_build_and_retrieval_remain_config_bounded() -> None:
    count = 1000
    windows = tuple(
        _window(
            index + 1,
            shape=(
                0.0,
                ((index % 19) + 1) * 0.000001,
                ((index % 23) - 11) * 0.000001,
            ),
            time_shape=(0, 10 + index % 7, 40 + index % 13),
            source_name=f"period-{index % 17}",
            condition=_condition(
                spread=0.0001 + (index % 9) * 0.00001,
                volatility=0.0005 + (index % 11) * 0.0001,
            ),
        )
        for index in range(count)
    )
    config = ReferenceMotifIndexConfigV1(
        max_source_windows=count,
        max_fragments=64,
        min_cell_support=1,
        max_matches=8,
    )

    index = build_reference_motif_index(
        windows, splits=_splits(), config=config
    )
    result = query_reference_motifs(
        index,
        ReferenceMotifQueryV1(
            condition=_condition(),
            information_mode=InformationMode.EX_POST_RECONSTRUCTION,
            used_at_ns=BASE_NS + 8000 * STEP,
            max_results=8,
            min_cell_support=1,
        ),
    )

    assert index.source_window_count == count
    assert len(index.fragments) == 64
    assert index.selection_omitted_count == count - 64
    assert len(result.matches) <= 8
    assert result.scanned_fragment_count <= 64 * len(config.backoff_levels)
    assert len(index.to_json().encode("utf-8")) < config.max_artifact_bytes
