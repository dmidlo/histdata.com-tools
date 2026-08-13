"""Synchronized point-in-time cross-series constraint contracts."""

from __future__ import annotations

import json

import pytest

from histdatacom.cross_series_constraints import (
    CrossSeriesAlignmentPolicy,
    CrossSeriesConstraintBundleV1,
    CrossSeriesConstraintPolicyV1,
    CrossSeriesConstraintStatus,
    CrossSeriesConstraintUseStatus,
    CrossSeriesConstraintWindowV1,
    CrossSeriesRelationKind,
    CrossSeriesSourceBindingV1,
    compile_histdata_cross_series_constraints,
    cross_series_constraint_use,
    require_constraint_support_for_synchronization_time,
    select_constraint_synchronization_time,
)
from histdatacom.synthetic.contracts import SyntheticEventV1

_START = 1_325_376_000_000_000_000
_PERIOD = "201201"
_SYMBOLS = ("eurgbp", "eurusd", "gbpusd")


def test_complete_triangle_preserves_duplicate_identity_and_round_trips() -> (
    None
):
    events = _triangle_events(
        {
            "eurgbp": ((0, 0.8), (1, 0.8), (2, 0.8)),
            "eurusd": ((0, 1.2), (1, 1.2), (1, 1.2), (2, 1.2)),
            "gbpusd": ((0, 1.5), (1, 1.5), (2, 1.5)),
        }
    )

    bundle = _compile(events)
    restored = CrossSeriesConstraintBundleV1.from_json(bundle.to_json())
    triangle = _window(bundle, CrossSeriesRelationKind.TRIANGLE)
    grid = _window(bundle, CrossSeriesRelationKind.TIMESTAMP_GRID)
    eurusd = next(item for item in bundle.members if item.symbol == "eurusd")

    assert restored == bundle
    assert bundle.status is CrossSeriesConstraintStatus.READY
    assert triangle.status is CrossSeriesConstraintStatus.READY
    assert (
        triangle.alignment.policy
        is CrossSeriesAlignmentPolicy.EXACT_EVENT_SEQUENCE
    )
    assert triangle.alignment.support_count == 3
    assert triangle.alignment.support_start_ns == _START
    assert triangle.alignment.support_end_ns == _START + 2_000_001
    assert triangle.alignment.unmatched_event_count_by_symbol["eurusd"] == 1
    assert eurusd.event_count == 4
    assert eurusd.unique_timestamp_count == 3
    assert eurusd.duplicate_timestamp_event_count == 1
    assert grid.alignment.sample_alignment_ids
    assert len(set(grid.alignment.sample_alignment_ids)) == 3
    assert triangle.residual_summary is not None
    assert triangle.residual_summary.compared_count == 3
    assert triangle.residual_summary.maximum == pytest.approx(0.0, abs=1e-12)
    assert '"full_tick_rows_embedded":false' in bundle.to_json()
    assert '"timestamp_only_join":false' in bundle.to_json()
    selected_time, selected_window_id = select_constraint_synchronization_time(
        (bundle,), start_ns=_START, end_ns=_START + 3_000_000_000
    )
    assert selected_time == triangle.alignment.recommended_event_time_ns
    assert selected_window_id == triangle.constraint_window_id
    require_constraint_support_for_synchronization_time(
        (bundle,), selected_time
    )
    with pytest.raises(ValueError, match="lacks constraint support"):
        require_constraint_support_for_synchronization_time(
            (bundle,), selected_time + 1
        )
    before_availability = cross_series_constraint_use(
        (bundle,),
        stage="proposal",
        used_at_ns=bundle.available_at_ns - 1,
    )
    assert before_availability.status is (
        CrossSeriesConstraintUseStatus.REFUSED
    )

    reordered = {
        symbol: tuple(reversed(events[symbol]))
        for symbol in reversed(tuple(events))
    }
    assert _compile(reordered, reverse_bindings=True) == bundle
    tampered = json.loads(bundle.to_json())
    tampered["windows"][0]["support_start_ns"] += 1
    with pytest.raises(ValueError, match="support bounds differ"):
        CrossSeriesConstraintBundleV1.from_dict(tampered)

    for stage in (
        "source_enrichment",
        "proposal",
        "carving",
        "cross_series_reconciliation",
        "validation",
    ):
        use = cross_series_constraint_use(
            (bundle,), stage=stage, used_at_ns=bundle.as_of_ns
        )
        assert use.status is CrossSeriesConstraintUseStatus.APPLIED
        assert use.consumed_window_ids


def test_asynchronous_support_is_bounded_and_never_forward_filled() -> None:
    events = _triangle_events(
        {
            "eurgbp": ((2_000, 0.8), (12_000, 0.8)),
            "eurusd": ((1_000, 1.2), (11_000, 1.2)),
            "gbpusd": ((0, 1.5), (10_000, 1.5)),
        }
    )

    bundle = _compile(events)
    triangle = _window(bundle, CrossSeriesRelationKind.TRIANGLE)
    grid = _window(bundle, CrossSeriesRelationKind.TIMESTAMP_GRID)
    stale = _window(bundle, CrossSeriesRelationKind.STALE_ALIGNMENT)
    overlap = _window(bundle, CrossSeriesRelationKind.RANGE_OVERLAP)
    inverse = _window(bundle, CrossSeriesRelationKind.INVERSE)

    assert bundle.status is CrossSeriesConstraintStatus.LIMITED
    assert triangle.status is CrossSeriesConstraintStatus.LIMITED
    assert (
        triangle.alignment.policy
        is CrossSeriesAlignmentPolicy.NEAREST_PRIOR_BOUNDED
    )
    assert triangle.alignment.support_count == 2
    assert triangle.limiting_symbols == ("eurgbp", "gbpusd")
    assert triangle.residual_summary is None
    assert grid.status is CrossSeriesConstraintStatus.INSUFFICIENT
    assert grid.alignment.support_count == 0
    assert stale.status is CrossSeriesConstraintStatus.LIMITED
    assert stale.alignment.maximum_observed_age_ns == 2_000_000_000
    assert stale.alignment.stale_support_count == 2
    assert stale.alignment.configured_max_age_ns == 5_000_000_000
    assert stale.alignment.to_dict()["forward_fill"] is False
    assert overlap.status is CrossSeriesConstraintStatus.LIMITED
    assert "unequal_member_coverage_ranges" in overlap.limitations
    assert inverse.status is CrossSeriesConstraintStatus.UNAVAILABLE
    reordered = {
        symbol: tuple(reversed(events[symbol]))
        for symbol in reversed(tuple(events))
    }
    assert _compile(reordered, reverse_bindings=True) == bundle


def test_partial_group_is_anomaly_eligible_but_refuses_production_stages() -> (
    None
):
    events = _triangle_events(
        {
            "eurusd": ((0, 1.2), (1, 1.2)),
            "gbpusd": ((0, 1.5), (1, 1.5)),
        }
    )
    bundle = _compile(events)

    assert bundle.status is CrossSeriesConstraintStatus.UNAVAILABLE
    assert _window(bundle, CrossSeriesRelationKind.TRIANGLE).status is (
        CrossSeriesConstraintStatus.INSUFFICIENT
    )
    source = cross_series_constraint_use(
        (bundle,), stage="source_enrichment", used_at_ns=bundle.as_of_ns
    )
    anomaly = cross_series_constraint_use(
        (bundle,), stage="anomaly_label", used_at_ns=bundle.as_of_ns
    )
    proposal = cross_series_constraint_use(
        (bundle,), stage="proposal", used_at_ns=bundle.as_of_ns
    )

    assert source.status is CrossSeriesConstraintUseStatus.APPLIED
    assert source.effects["normal_training_eligible"] is False
    assert anomaly.status is CrossSeriesConstraintUseStatus.APPLIED
    assert anomaly.effects["anomaly_label_eligible"] is True
    assert proposal.status is CrossSeriesConstraintUseStatus.REFUSED

    unavailable = compile_histdata_cross_series_constraints(
        {},
        source_bindings=(),
        synchronization_unit_id="triangle-unit",
        evidence_window_id="empty-window",
        dataset_version_ids=("histdata-fixture-v1",),
        support_start_ns=_START,
        support_end_ns=_START + 1,
        available_at_ns=_START,
        as_of_ns=_START,
        information_mode="ex_post_reconstruction",
    )
    assert unavailable.status is CrossSeriesConstraintStatus.UNAVAILABLE
    assert unavailable.windows == ()
    assert "required_cross_series_evidence_unavailable" in (
        unavailable.limitations
    )


def test_contradictory_anchors_are_labeled_and_cannot_be_silently_projected() -> (
    None
):
    events = _triangle_events(
        {
            "eurgbp": ((0, 2.0), (1, 2.0), (2, 2.0)),
            "eurusd": ((0, 1.2), (1, 1.2), (2, 1.2)),
            "gbpusd": ((0, 1.5), (1, 1.5), (2, 1.5)),
        }
    )
    bundle = _compile(events)
    triangle = _window(bundle, CrossSeriesRelationKind.TRIANGLE)

    assert bundle.status is CrossSeriesConstraintStatus.CONTRADICTORY
    assert triangle.status is CrossSeriesConstraintStatus.CONTRADICTORY
    assert triangle.residual_summary is not None
    assert triangle.residual_summary.error_count == 3
    assert (
        cross_series_constraint_use(
            (bundle,), stage="anomaly_label", used_at_ns=bundle.as_of_ns
        ).status
        is CrossSeriesConstraintUseStatus.APPLIED
    )
    for stage in ("proposal", "carving", "reconciliation", "validation"):
        assert (
            cross_series_constraint_use(
                (bundle,), stage=stage, used_at_ns=bundle.as_of_ns
            ).status
            is CrossSeriesConstraintUseStatus.REFUSED
        )
    assert all(
        event.bid == event.ask
        for symbol_events in events.values()
        for event in symbol_events
    )


def test_ex_ante_compilation_withholds_future_relation_support() -> None:
    events = _triangle_events(
        {
            symbol: (
                (
                    0,
                    (
                        0.8
                        if symbol == "eurgbp"
                        else (1.2 if symbol == "eurusd" else 1.5)
                    ),
                ),
                (
                    10_000,
                    (
                        0.8
                        if symbol == "eurgbp"
                        else (1.2 if symbol == "eurusd" else 1.5)
                    ),
                ),
            )
            for symbol in _SYMBOLS
        }
    )
    as_of = _START

    bundle = _compile(
        events,
        information_mode="ex_ante_simulation",
        as_of_ns=as_of,
        available_at_ns=_START + 20_000_000_000,
    )

    assert bundle.available_at_ns == _START + 20_000_000_000
    assert bundle.as_of_ns == as_of
    assert all(item.event_count == 1 for item in bundle.members)
    assert "future_cross_series_events_withheld_in_ex_ante_mode" in (
        bundle.limitations
    )
    assert _window(bundle, CrossSeriesRelationKind.TRIANGLE).status is (
        CrossSeriesConstraintStatus.INSUFFICIENT
    )
    assert (
        cross_series_constraint_use(
            (bundle,), stage="proposal", used_at_ns=as_of
        ).status
        is CrossSeriesConstraintUseStatus.REFUSED
    )


def test_nearest_prior_age_boundary_and_bundle_window_bound_are_exact() -> None:
    base = {
        "eurgbp": ((5_000, 0.8),),
        "eurusd": ((0, 1.2),),
        "gbpusd": ((0, 1.5),),
    }
    policy = CrossSeriesConstraintPolicyV1(
        minimum_alignment_support=1,
        nearest_prior_max_age_ns=5_000_000_000,
        max_windows=5,
    )
    at_boundary = _compile(_triangle_events(base), policy=policy)
    triangle = _window(at_boundary, CrossSeriesRelationKind.TRIANGLE)

    assert triangle.alignment.support_count == 1
    assert at_boundary.omitted_window_count == 0
    assert len(at_boundary.windows) == 5

    outside_events = _triangle_events(base)
    outside_events["eurgbp"] = (
        SyntheticEventV1.observed(
            symbol="eurgbp",
            event_time_ns=_START + 5_000_000_001,
            event_sequence=0,
            bid=0.8,
            ask=0.8,
            run_id="run-1",
            ensemble_member_id="member-1",
            source_version_id="histdata-fixture-v1",
            source_series_id=_series_id("eurgbp"),
            source_period=_PERIOD,
            source_row_id=1,
        ),
    )
    outside_bundle = _compile(outside_events, policy=policy)
    outside_triangle = _window(outside_bundle, CrossSeriesRelationKind.TRIANGLE)
    assert outside_triangle.alignment.support_count == 0
    assert outside_triangle.status is CrossSeriesConstraintStatus.INSUFFICIENT
    with pytest.raises(ValueError, match="cannot hold one relation group"):
        CrossSeriesConstraintPolicyV1(max_windows=4)
    with pytest.raises(ValueError, match="must remain enabled"):
        CrossSeriesConstraintPolicyV1(fail_closed_on_contradiction=False)


def test_histdata_compiler_rejects_generated_rows_and_provider_expansion() -> (
    None
):
    events = _triangle_events(
        {
            "eurgbp": ((0, 0.8), (2, 0.8)),
            "eurusd": ((0, 1.2), (2, 1.2)),
            "gbpusd": ((0, 1.5), (2, 1.5)),
        }
    )
    left, right = events["eurusd"]
    generated = SyntheticEventV1.generated(
        symbol="eurusd",
        event_time_ns=_START + 1_000_000,
        event_sequence=0,
        bid=1.2,
        ask=1.2,
        run_id="run-1",
        ensemble_member_id="member-1",
        source_version_id="histdata-fixture-v1",
        left_anchor_event_id=left.event_id,
        right_anchor_event_id=right.event_id,
        generator_id="negative-control",
        generator_version="1.0.0",
        generator_config_id="negative-control-config",
        constraint_set_id="negative-control-constraints",
    )
    events["eurusd"] = (left, generated, right)

    with pytest.raises(ValueError, match="observed events only"):
        _compile(events)
    with pytest.raises(ValueError, match="only HistData.com"):
        _compile(
            _triangle_events(
                {
                    "eurgbp": ((0, 0.8), (1, 0.8)),
                    "eurusd": ((0, 1.2), (1, 1.2)),
                    "gbpusd": ((0, 1.5), (1, 1.5)),
                }
            ),
            policy=CrossSeriesConstraintPolicyV1(
                supported_provider_ids=("histdata.com", "oanda")
            ),
        )


def _compile(
    events: dict[str, tuple[SyntheticEventV1, ...]],
    *,
    policy: CrossSeriesConstraintPolicyV1 | None = None,
    information_mode: str = "ex_post_reconstruction",
    as_of_ns: int | None = None,
    available_at_ns: int | None = None,
    reverse_bindings: bool = False,
) -> CrossSeriesConstraintBundleV1:
    bindings = tuple(
        _binding(symbol, events_for_symbol[0].source_series_id or "")
        for symbol, events_for_symbol in sorted(events.items())
    )
    if reverse_bindings:
        bindings = tuple(reversed(bindings))
    selected_as_of = (
        as_of_ns if as_of_ns is not None else _START + 60_000_000_000
    )
    return compile_histdata_cross_series_constraints(
        events,
        source_bindings=bindings,
        synchronization_unit_id="triangle-unit",
        evidence_window_id="window-1",
        dataset_version_ids=("histdata-fixture-v1",),
        support_start_ns=_START,
        support_end_ns=_START + 60_000_000_000,
        available_at_ns=(
            available_at_ns if available_at_ns is not None else selected_as_of
        ),
        as_of_ns=selected_as_of,
        information_mode=information_mode,
        policy=policy,
    )


def _triangle_events(
    values: dict[str, tuple[tuple[int, float], ...]],
) -> dict[str, tuple[SyntheticEventV1, ...]]:
    result: dict[str, tuple[SyntheticEventV1, ...]] = {}
    for symbol, rows in sorted(values.items()):
        counts: dict[int, int] = {}
        events: list[SyntheticEventV1] = []
        for row_id, (offset_ms, price) in enumerate(rows, start=1):
            timestamp = _START + offset_ms * 1_000_000
            sequence = counts.get(timestamp, 0)
            counts[timestamp] = sequence + 1
            events.append(
                SyntheticEventV1.observed(
                    symbol=symbol,
                    event_time_ns=timestamp,
                    event_sequence=sequence,
                    bid=price,
                    ask=price,
                    run_id="run-1",
                    ensemble_member_id="member-1",
                    source_version_id="histdata-fixture-v1",
                    source_series_id=_series_id(symbol),
                    source_period=_PERIOD,
                    source_row_id=row_id,
                )
            )
        result[symbol] = tuple(events)
    return result


def _series_id(symbol: str) -> str:
    return f"ascii-tick:{symbol}:{_PERIOD}:sha256:{_digest(symbol)}"


def _binding(symbol: str, series_id: str) -> CrossSeriesSourceBindingV1:
    digest = _digest(symbol)
    return CrossSeriesSourceBindingV1(
        provider_id="histdata.com",
        dataset_version_id="histdata-fixture-v1",
        symbol=symbol,
        period=_PERIOD,
        series_id=series_id,
        source_partition_id=f"partition:{symbol}",
        source_artifact_id=f"histdata_ascii_tick_arrow:sha256:{digest}",
        source_artifact_sha256=digest,
    )


def _digest(symbol: str) -> str:
    return {"eurgbp": "a", "eurusd": "b", "gbpusd": "c"}[symbol] * 64


def _window(
    bundle: CrossSeriesConstraintBundleV1, kind: CrossSeriesRelationKind
) -> CrossSeriesConstraintWindowV1:
    return next(item for item in bundle.windows if item.relation_kind is kind)
