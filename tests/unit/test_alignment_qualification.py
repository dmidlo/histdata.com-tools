"""Tests for exact/bounded-prior triangle alignment qualification."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest

from histdatacom.orchestration.reconstruction import artifact_ref_for_file
from histdatacom.runtime_contracts import ArtifactRef
from histdatacom.synthetic.alignment_qualification import (
    REQUIRED_AGE_SLICE_DIMENSIONS,
    REQUIRED_ALIGNMENT_METRICS,
    TriangleAgeRuleAction,
    TriangleAlignmentAgeRuleV1,
    TriangleAlignmentConsumptionReceiptV1,
    TriangleAlignmentMetricToleranceV1,
    TriangleAlignmentOutcomeV1,
    TriangleAlignmentPolicy,
    TriangleAlignmentQualificationPolicyV1,
    TriangleAlignmentQualificationV1,
    TriangleAlignmentResidualBinV1,
    TriangleAlignmentSourceEventV1,
    TriangleAlignmentSourceWindowV1,
    TriangleAlignmentTupleV1,
    TriangleQualificationStatus,
    TriangleSourceWindowState,
    TriangleSupportClass,
    TriangleToleranceSeverity,
    analyze_triangle_alignment_window,
    build_triangle_quote_age_slices,
    build_triangle_support_census,
    qualify_triangle_alignment,
    read_triangle_alignment_qualification,
    write_triangle_alignment_qualification,
)

_CANDIDATE_ID = "reconstruction-release-candidate:alignment-test"


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _candidate_ref(tmp_path: Path) -> ArtifactRef:
    path = tmp_path / "candidate.json"
    path.write_text(
        json.dumps({"candidate_id": _CANDIDATE_ID}), encoding="utf-8"
    )
    return artifact_ref_for_file(
        path,
        kind="reconstruction_release_candidate_v1",
        metadata={"candidate_id": _CANDIDATE_ID},
    )


def _event(
    symbol: str, time_ns: int, sequence: int = 0
) -> TriangleAlignmentSourceEventV1:
    return TriangleAlignmentSourceEventV1(
        source_event_id=f"source:{symbol}:{time_ns}:{sequence}",
        symbol=symbol,
        event_time_ns=time_ns,
        event_sequence=sequence,
        bid=1.0 + time_ns / 100_000,
        ask=1.0002 + time_ns / 100_000,
        source_partition_id=f"partition:{symbol}:202001",
        source_row_content_sha256=_sha(f"row:{symbol}:{time_ns}:{sequence}"),
    )


def _window(
    start: int,
    end: int,
    state: TriangleSourceWindowState,
    events: tuple[TriangleAlignmentSourceEventV1, ...],
    *,
    label: str,
) -> TriangleAlignmentSourceWindowV1:
    return TriangleAlignmentSourceWindowV1(
        candidate_id=_CANDIDATE_ID,
        start_ns=start,
        end_ns=end,
        year=2020,
        feed_epoch=f"epoch:{label}",
        session="london_new_york_overlap",
        event_state="scheduled_event" if label == "bounded" else "ordinary",
        activity_stratum="high" if label == "bounded" else "quiet",
        source_state=state,
        events=events,
    )


def _source_windows() -> tuple[TriangleAlignmentSourceWindowV1, ...]:
    exact = tuple(
        _event(symbol, time_ns)
        for time_ns in (10, 50)
        for symbol in ("eurgbp", "eurusd", "gbpusd")
    )
    bounded = (
        _event("eurgbp", 110),
        _event("eurgbp", 160),
        _event("eurusd", 109),
        _event("eurusd", 159),
        _event("gbpusd", 108),
        _event("gbpusd", 158),
    )
    incomplete = (_event("eurgbp", 210), _event("eurusd", 210))
    return (
        _window(
            0, 100, TriangleSourceWindowState.AVAILABLE, exact, label="exact"
        ),
        _window(
            100,
            200,
            TriangleSourceWindowState.AVAILABLE,
            bounded,
            label="bounded",
        ),
        _window(
            200,
            300,
            TriangleSourceWindowState.INCOMPLETE,
            incomplete,
            label="incomplete",
        ),
        _window(
            300,
            400,
            TriangleSourceWindowState.EXPECTED_CLOSURE,
            (),
            label="closure",
        ),
    )


def _policy(
    *,
    residual_limit: float = 1.0,
    burden_limit: float = 1.0,
    action: TriangleAgeRuleAction = TriangleAgeRuleAction.ADMIT,
) -> TriangleAlignmentQualificationPolicyV1:
    return TriangleAlignmentQualificationPolicyV1(
        maximum_age_ns=5,
        sensitivity_age_ceilings_ns=(2, 4, 5),
        minimum_alignment_support=2,
        minimum_sensitivity_pairs_per_window=1,
        metric_tolerances=tuple(
            TriangleAlignmentMetricToleranceV1(
                metric_name=metric,
                absolute_tolerance=0.2,
                relative_tolerance=0.2,
                severity=(
                    TriangleToleranceSeverity.ADVISORY
                    if metric == "downstream_sensitivity"
                    else TriangleToleranceSeverity.HARD
                ),
            )
            for metric in sorted(REQUIRED_ALIGNMENT_METRICS)
        ),
        age_rules=(
            TriangleAlignmentAgeRuleV1(
                lower_age_ns=0,
                upper_age_ns=6,
                action=action,
                maximum_synthetic_residual=residual_limit,
                maximum_projection_burden=burden_limit,
                maximum_relative_sensitivity=0.5,
            ),
        ),
    )


def _metrics(offset: float = 0.0) -> dict[str, float]:
    return {metric: 1.0 + offset for metric in REQUIRED_ALIGNMENT_METRICS}


def _outcome(
    evidence_id: str,
    window_id: str,
    source_hash: str,
    *,
    experiment: str,
    policy: TriangleAlignmentPolicy,
    maximum_age_ns: int,
    offset: float = 0.0,
    validation_only: bool = False,
) -> TriangleAlignmentOutcomeV1:
    return TriangleAlignmentOutcomeV1(
        candidate_id=_CANDIDATE_ID,
        source_window_id=window_id,
        experiment_identity_id=experiment,
        semantic_member_id="member:0",
        observation_scenario_id="scenario:central",
        policy=policy,
        configured_max_age_ns=maximum_age_ns,
        alignment_evidence_id=evidence_id,
        source_content_sha256=source_hash,
        output_content_sha256=_sha(
            f"output:{window_id}:{experiment}:{policy.value}:{maximum_age_ns}"
        ),
        metrics=_metrics(offset),
        validation_only=validation_only,
        observed_only_residual_immutable=True,
        synthetic_involved_residual_passed=True,
    )


def _receipt(
    evidence_id: str,
    window_id: str,
    tuple_hash: str,
    policy_id: str,
    *,
    policy: TriangleAlignmentPolicy,
    maximum_age_ns: int,
    probe_leg: str,
    recommended_time_ns: int,
) -> TriangleAlignmentConsumptionReceiptV1:
    return TriangleAlignmentConsumptionReceiptV1(
        source_window_id=window_id,
        planner_alignment_evidence_id=evidence_id,
        planner_policy=policy,
        planner_max_age_ns=maximum_age_ns,
        planner_probe_leg=probe_leg,
        planner_recommended_event_time_ns=recommended_time_ns,
        planner_tuple_content_sha256=tuple_hash,
        runtime_alignment_evidence_id=evidence_id,
        runtime_policy=policy,
        runtime_max_age_ns=maximum_age_ns,
        runtime_probe_leg=probe_leg,
        runtime_recommended_event_time_ns=recommended_time_ns,
        runtime_tuple_content_sha256=tuple_hash,
        validation_policy=policy,
        validation_max_age_ns=maximum_age_ns,
        publication_policy_id=policy_id,
        expected_policy_id=policy_id,
        publication_alignment_evidence_id=evidence_id,
        atomic_publication=True,
    )


def _residual_bins() -> tuple[TriangleAlignmentResidualBinV1, ...]:
    return (
        TriangleAlignmentResidualBinV1(
            lower_age_ns=0,
            upper_age_ns=1,
            sample_count=2,
            observed_only_residual_mean=0.01,
            observed_only_residual_maximum=0.02,
            synthetic_post_projection_residual_mean=0.01,
            synthetic_post_projection_residual_maximum=0.02,
            projection_burden_mean=0.01,
            projection_burden_maximum=0.02,
            observed_evidence_content_sha256=_sha("observed:exact"),
            observed_only_residual_immutable=True,
            synthetic_involved_residual_passed=True,
        ),
        TriangleAlignmentResidualBinV1(
            lower_age_ns=1,
            upper_age_ns=6,
            sample_count=2,
            observed_only_residual_mean=0.02,
            observed_only_residual_maximum=0.03,
            synthetic_post_projection_residual_mean=0.02,
            synthetic_post_projection_residual_maximum=0.03,
            projection_burden_mean=0.02,
            projection_burden_maximum=0.03,
            observed_evidence_content_sha256=_sha("observed:bounded"),
            observed_only_residual_immutable=True,
            synthetic_involved_residual_passed=True,
        ),
    )


def _campaign(
    tmp_path: Path,
    policy: TriangleAlignmentQualificationPolicyV1 | None = None,
) -> tuple[
    TriangleAlignmentQualificationPolicyV1,
    tuple[TriangleAlignmentSourceWindowV1, ...],
    tuple[TriangleAlignmentOutcomeV1, ...],
    tuple[TriangleAlignmentResidualBinV1, ...],
    tuple[TriangleAlignmentConsumptionReceiptV1, ...],
    ArtifactRef,
]:
    selected_policy = policy or _policy()
    windows = _source_windows()
    evidence = tuple(
        analyze_triangle_alignment_window(item, selected_policy)
        for item in windows
    )
    exact, bounded = evidence[:2]
    outcomes = (
        _outcome(
            exact.evidence_id,
            exact.source_window_id,
            exact.source_content_sha256,
            experiment="exact-comparison",
            policy=TriangleAlignmentPolicy.EXACT_EVENT_SEQUENCE,
            maximum_age_ns=0,
        ),
        _outcome(
            exact.evidence_id,
            exact.source_window_id,
            exact.source_content_sha256,
            experiment="exact-comparison",
            policy=TriangleAlignmentPolicy.BOUNDED_PRIOR,
            maximum_age_ns=5,
            offset=0.01,
        ),
        *(
            _outcome(
                bounded.evidence_id,
                bounded.source_window_id,
                bounded.source_content_sha256,
                experiment="bounded-ceilings",
                policy=TriangleAlignmentPolicy.BOUNDED_PRIOR,
                maximum_age_ns=ceiling,
                offset=ceiling / 1_000,
                validation_only=True,
            )
            for ceiling in selected_policy.sensitivity_age_ceilings_ns
        ),
    )
    receipts = tuple(
        _receipt(
            item.evidence_id,
            item.source_window_id,
            item.selected_tuple_content_sha256,
            selected_policy.policy_id,
            policy=item.selected_policy,
            maximum_age_ns=item.configured_max_age_ns,
            probe_leg=item.selected_probe_leg,
            recommended_time_ns=item.recommended_event_time_ns,
        )
        for item in evidence[:2]
        if item.selected_policy is not None
        and item.selected_probe_leg is not None
        and item.recommended_event_time_ns is not None
    )
    return (
        selected_policy,
        windows,
        outcomes,
        _residual_bins(),
        receipts,
        _candidate_ref(tmp_path),
    )


def _qualify(tmp_path: Path) -> TriangleAlignmentQualificationV1:
    policy, windows, outcomes, bins, receipts, candidate_ref = _campaign(
        tmp_path
    )
    return qualify_triangle_alignment(
        candidate_id=_CANDIDATE_ID,
        release_candidate_ref=candidate_ref,
        policy=policy,
        source_windows=windows,
        outcomes=outcomes,
        residual_bins=bins,
        consumption_receipts=receipts,
        created_at="2026-08-21T00:00:00Z",
    )


def test_event_and_window_identity_bind_nanoseconds_and_content() -> None:
    event = _event("EUR/USD", 10)
    assert event.symbol == "eurusd"
    assert event.event_id.startswith("triangle-alignment-source-event:sha256:")
    with pytest.raises(ValueError, match="identity differs"):
        replace(event, event_time_ns=11)

    window = _source_windows()[0]
    assert window.start_ns <= min(item.event_time_ns for item in window.events)
    with pytest.raises(ValueError, match="content hash differs"):
        replace(window, events=window.events[:-1])


def test_exact_and_bounded_alignment_reports_every_probe_leg() -> None:
    policy = _policy()
    exact = analyze_triangle_alignment_window(_source_windows()[0], policy)
    bounded = analyze_triangle_alignment_window(_source_windows()[1], policy)

    assert exact.support_class is TriangleSupportClass.EXACT
    assert exact.exact_event_sequence_support == 2
    assert exact.selected_policy is TriangleAlignmentPolicy.EXACT_EVENT_SEQUENCE
    assert all(not item.maximum_age_ns for item in exact.selected_tuples)

    assert bounded.support_class is TriangleSupportClass.BOUNDED_PRIOR_ONLY
    assert bounded.selected_probe_leg == "eurgbp"
    assert set(bounded.bounded_support_by_probe_leg) == {
        "eurgbp",
        "eurusd",
        "gbpusd",
    }
    assert bounded.bounded_support_by_probe_leg["eurgbp"] == 2
    assert all(
        selected_time <= item.probe_time_ns
        for item in bounded.selected_tuples
        for selected_time in item.selected_event_times_ns.values()
    )


def test_future_event_and_silent_widening_fail_closed() -> None:
    source = _source_windows()[1]
    evidence = analyze_triangle_alignment_window(source, _policy())
    aligned = evidence.selected_tuples[0]
    future_times = dict(aligned.selected_event_times_ns)
    future_times["eurusd"] = aligned.probe_time_ns + 1
    with pytest.raises(ValueError, match="future or retimestamped"):
        replace(aligned, selected_event_times_ns=future_times)
    with pytest.raises(ValueError, match="silently widens"):
        replace(aligned, configured_max_age_ns=1)


def test_support_census_decomposes_complete_candidate_range() -> None:
    policy = _policy()
    evidence = tuple(
        analyze_triangle_alignment_window(item, policy)
        for item in _source_windows()
    )
    census = build_triangle_support_census(evidence)
    assert census.start_ns == 0
    assert census.end_ns == 400
    assert census.window_counts == {
        "bounded_prior_only": 1,
        "empty": 0,
        "exact": 1,
        "expected_closure": 1,
        "incomplete_source": 1,
        "unsupported_complete": 0,
    }
    assert census.bounded_created_window_fraction == 0.5
    assert census.bounded_created_duration_fraction == 0.5
    assert all(
        symbol in census.alternative_probe_support_counts
        for symbol in ("eurgbp", "eurusd", "gbpusd")
    )


def test_quote_age_slices_cover_required_dimensions_and_support_kinds() -> None:
    policy = _policy()
    evidence = tuple(
        analyze_triangle_alignment_window(item, policy)
        for item in _source_windows()
    )
    slices = build_triangle_quote_age_slices(evidence)
    assert {item.dimension for item in slices} == set(
        REQUIRED_AGE_SLICE_DIMENSIONS
    )
    assert {item.support_class for item in slices} == {
        TriangleSupportClass.EXACT,
        TriangleSupportClass.BOUNDED_PRIOR_ONLY,
    }
    assert max(item.maximum_age_ns for item in slices) == 2


def test_complete_qualification_round_trips_content_addressed(
    tmp_path: Path,
) -> None:
    qualification = _qualify(tmp_path)
    assert qualification.status is TriangleQualificationStatus.PASS
    assert qualification.census.bounded_created_window_fraction == 0.5
    assert qualification.comparisons
    assert all(item.matched for item in qualification.consumption_receipts)

    ref = write_triangle_alignment_qualification(
        qualification, tmp_path / "audit"
    )
    restored = read_triangle_alignment_qualification(ref.path)
    assert restored == qualification
    assert (
        TriangleAlignmentQualificationV1.from_dict(qualification.to_dict())
        == qualification
    )


def test_missing_exact_or_ceiling_sensitivity_fails(tmp_path: Path) -> None:
    policy, windows, outcomes, bins, receipts, candidate_ref = _campaign(
        tmp_path
    )
    qualification = qualify_triangle_alignment(
        candidate_id=_CANDIDATE_ID,
        release_candidate_ref=candidate_ref,
        policy=policy,
        source_windows=windows,
        outcomes=outcomes[1:-1],
        residual_bins=bins,
        consumption_receipts=receipts,
        created_at="2026-08-21T00:00:00Z",
    )
    assert qualification.status is TriangleQualificationStatus.FAIL
    assert any(
        "exact_bounded_sensitivity_missing" in item
        for item in qualification.failure_reasons
    )
    assert any(
        "bounded_ceiling_sensitivity_missing" in item
        for item in qualification.failure_reasons
    )


def test_metric_breach_cannot_hide_behind_final_residual(
    tmp_path: Path,
) -> None:
    policy, windows, outcomes, bins, receipts, candidate_ref = _campaign(
        tmp_path
    )
    metrics = dict(outcomes[1].metrics)
    metrics["projection_burden"] = 10.0
    outcomes = (
        outcomes[0],
        replace(outcomes[1], metrics=metrics, outcome_id=""),
        *outcomes[2:],
    )
    qualification = qualify_triangle_alignment(
        candidate_id=_CANDIDATE_ID,
        release_candidate_ref=candidate_ref,
        policy=policy,
        source_windows=windows,
        outcomes=outcomes,
        residual_bins=bins,
        consumption_receipts=receipts,
        created_at="2026-08-21T00:00:00Z",
    )
    assert qualification.status is TriangleQualificationStatus.FAIL
    assert any(
        "projection_burden" in item for item in qualification.failure_reasons
    )


def test_age_rule_refusal_and_residual_burden_are_blocking(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="no admitted age region"):
        _policy(action=TriangleAgeRuleAction.REFUSE)

    policy = _policy(residual_limit=0.01, burden_limit=0.01)
    policy, windows, outcomes, bins, receipts, candidate_ref = _campaign(
        tmp_path, policy
    )
    qualification = qualify_triangle_alignment(
        candidate_id=_CANDIDATE_ID,
        release_candidate_ref=candidate_ref,
        policy=policy,
        source_windows=windows,
        outcomes=outcomes,
        residual_bins=bins,
        consumption_receipts=receipts,
        created_at="2026-08-21T00:00:00Z",
    )
    assert qualification.status is TriangleQualificationStatus.FAIL
    assert any(
        "synthetic_residual_tolerance" in item
        for item in qualification.failure_reasons
    )
    assert any(
        "projection_burden_tolerance" in item
        for item in qualification.failure_reasons
    )


def test_runtime_consumption_and_atomic_publication_must_match(
    tmp_path: Path,
) -> None:
    policy, windows, outcomes, bins, receipts, candidate_ref = _campaign(
        tmp_path
    )
    receipts = (
        replace(
            receipts[0],
            runtime_recommended_event_time_ns=10,
            receipt_id="",
        ),
        receipts[1],
    )
    qualification = qualify_triangle_alignment(
        candidate_id=_CANDIDATE_ID,
        release_candidate_ref=candidate_ref,
        policy=policy,
        source_windows=windows,
        outcomes=outcomes,
        residual_bins=bins,
        consumption_receipts=receipts,
        created_at="2026-08-21T00:00:00Z",
    )
    assert qualification.status is TriangleQualificationStatus.FAIL
    assert any(
        "runtime_consumption_differs" in item
        for item in qualification.failure_reasons
    )


def test_incomplete_and_closure_windows_never_receive_alignment() -> None:
    policy = _policy()
    incomplete = analyze_triangle_alignment_window(_source_windows()[2], policy)
    closure = analyze_triangle_alignment_window(_source_windows()[3], policy)
    assert incomplete.support_class is TriangleSupportClass.INCOMPLETE_SOURCE
    assert closure.support_class is TriangleSupportClass.EXPECTED_CLOSURE
    assert incomplete.selected_tuples == closure.selected_tuples == ()
    assert incomplete.selected_policy is closure.selected_policy is None


def test_duplicate_timestamps_retain_event_sequence_identity() -> None:
    events = tuple(
        _event(symbol, 10, sequence)
        for symbol in ("eurgbp", "eurusd", "gbpusd")
        for sequence in (0, 1)
    )
    window = _window(
        0,
        100,
        TriangleSourceWindowState.AVAILABLE,
        events,
        label="duplicates",
    )
    evidence = analyze_triangle_alignment_window(window, _policy())
    assert evidence.exact_event_sequence_support == 2
    assert len({item.tuple_id for item in evidence.selected_tuples}) == 2
    assert {
        item.selected_event_ids["eurgbp"] for item in evidence.selected_tuples
    } == {item.event_id for item in events if item.symbol == "eurgbp"}


def test_tuple_deserialization_rejects_claimed_future_policy() -> None:
    evidence = analyze_triangle_alignment_window(
        _source_windows()[1], _policy()
    )
    payload = evidence.selected_tuples[0].to_dict()
    payload["future_event_allowed"] = True
    with pytest.raises(ValueError, match="safety policy differs"):
        TriangleAlignmentTupleV1.from_dict(payload)
