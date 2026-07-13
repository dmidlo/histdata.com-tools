"""Tests for the streaming reverse-degradation benchmark."""

from __future__ import annotations

from dataclasses import replace
import hashlib

import polars as pl
import pytest

from histdatacom.synthetic import (
    BENCHMARK_EVENT_SCHEMA_VERSION,
    BenchmarkCandidateKind,
    BenchmarkCandidateV1,
    BenchmarkCandidateWindowV1,
    BenchmarkControlKind,
    BenchmarkEventV1,
    BenchmarkExecutionEvidenceV1,
    BenchmarkProfileV1,
    BenchmarkScenarioV1,
    BenchmarkSplitKind,
    BenchmarkSplitV1,
    ReverseDegradationBenchmarkManifestV1,
    ReverseDegradationBenchmarkV1,
    ReverseDegradationScorecardV1,
    benchmark_events_from_empirical_overlay,
    build_benchmark_control_events,
    build_benchmark_control_windows,
    degrade_benchmark_window,
    generate_benchmark_candidate_window,
    validate_benchmark_information_boundary,
)
from histdatacom.synthetic.information import (
    InformationMode,
    InformationSplitKind,
    ReconstructionInformationManifestV1,
    ReconstructionInformationSplitV1,
)
from histdatacom.synthetic.observation import (
    ObservationApplicationResultV1,
    ObservationCarryStateV1,
    ObservationInputEventV1,
    ObservationOutputEventV1,
)
from histdatacom.synthetic.streaming import ReconstructionWindowV1

BASE_NS = 1_700_000_000_000_000_000
RUN_ID = "reconstruction-run:sha256:benchmark-fixture"
INFORMATION_POLICY_ID = "information-policy:sha256:benchmark-fixture"
WINDOW_PLAN_ID = "window-plan:sha256:benchmark-fixture"


def _information_manifest() -> ReconstructionInformationManifestV1:
    return ReconstructionInformationManifestV1(
        run_id=RUN_ID,
        policy_id=INFORMATION_POLICY_ID,
        information_mode=InformationMode.EX_ANTE_SIMULATION,
        window_plan_id=WINDOW_PLAN_ID,
        inputs=(),
        splits=(
            ReconstructionInformationSplitV1(
                InformationSplitKind.TRAIN,
                BASE_NS,
                BASE_NS + 1_000,
            ),
            ReconstructionInformationSplitV1(
                InformationSplitKind.CALIBRATION,
                BASE_NS + 1_000,
                BASE_NS + 2_000,
            ),
            ReconstructionInformationSplitV1(
                InformationSplitKind.VALIDATION,
                BASE_NS + 2_000,
                BASE_NS + 6_000,
            ),
        ),
    )


def _candidate(
    kind: BenchmarkCandidateKind,
    method: str,
    *,
    control: BenchmarkControlKind | None = None,
    members: tuple[str, ...] = ("control",),
    parameters: dict[str, object] | None = None,
) -> BenchmarkCandidateV1:
    return BenchmarkCandidateV1(
        kind=kind,
        method_id=method,
        implementation_version="fixture-v1",
        parameters=parameters or {},
        ensemble_member_ids=members,
        control_kind=control,
    )


def _manifest(
    *, profile: BenchmarkProfileV1 | None = None
) -> ReverseDegradationBenchmarkManifestV1:
    information = _information_manifest()
    splits = (
        BenchmarkSplitV1(
            BenchmarkSplitKind.CALIBRATION,
            BASE_NS + 1_000,
            BASE_NS + 2_000,
        ),
        BenchmarkSplitV1(
            BenchmarkSplitKind.VALIDATION,
            BASE_NS + 2_000,
            BASE_NS + 4_000,
        ),
        BenchmarkSplitV1(
            BenchmarkSplitKind.FINAL_HOLDOUT,
            BASE_NS + 4_000,
            BASE_NS + 6_000,
        ),
    )
    scenarios = tuple(
        BenchmarkScenarioV1(
            split_kind=split,
            epoch_id=epoch,
            severity_id=severity,
            observation_operator_id=f"operator:{epoch}:{severity}",
            degradation_parameters={
                "retention_probability": retention,
                "severity": severity,
            },
        )
        for split, epoch, severity, retention in (
            (
                BenchmarkSplitKind.VALIDATION,
                "epoch-modern",
                "mild",
                0.8,
            ),
            (
                BenchmarkSplitKind.FINAL_HOLDOUT,
                "epoch-modern",
                "severe",
                0.4,
            ),
            (
                BenchmarkSplitKind.VALIDATION,
                "epoch-legacy",
                "severe",
                0.4,
            ),
            (
                BenchmarkSplitKind.FINAL_HOLDOUT,
                "epoch-legacy",
                "mild",
                0.8,
            ),
        )
    )
    candidates = (
        _candidate(
            BenchmarkCandidateKind.CONTROL,
            "no-fill",
            control=BenchmarkControlKind.NO_FILL,
        ),
        _candidate(
            BenchmarkCandidateKind.CONTROL,
            "linear",
            control=BenchmarkControlKind.LINEAR_INTERPOLATION,
            parameters={"interval_ns": 100},
        ),
        _candidate(
            BenchmarkCandidateKind.CONTROL,
            "resample",
            control=BenchmarkControlKind.RESAMPLE_LAST,
            parameters={"interval_ns": 250},
        ),
        _candidate(
            BenchmarkCandidateKind.CONTROL,
            "empirical-overlay",
            control=BenchmarkControlKind.EMPIRICAL_OVERLAY,
            parameters={"source_schema": "synthetic-tick-generation.v1"},
        ),
        _candidate(
            BenchmarkCandidateKind.CANDIDATE,
            "fixture-generator",
            members=("member-a", "member-b"),
            parameters={"temperature": 0.0},
        ),
    )
    return ReverseDegradationBenchmarkManifestV1(
        run_id=RUN_ID,
        information_manifest_id=information.manifest_id,
        profile=profile or BenchmarkProfileV1(),
        splits=splits,
        scenarios=scenarios,
        candidates=candidates,
    )


def _window(scenario: BenchmarkScenarioV1) -> ReconstructionWindowV1:
    start = (
        BASE_NS + 2_000
        if scenario.split_kind is BenchmarkSplitKind.VALIDATION
        else BASE_NS + 4_000
    )
    return ReconstructionWindowV1(
        run_id=RUN_ID,
        ensemble_member_id="member-a",
        symbols=("EURUSD",),
        core_start_ns=start,
        core_end_ns=start + 2_000,
    )


def _event(
    scenario: BenchmarkScenarioV1,
    window: ReconstructionWindowV1,
    ordinal: int,
    *,
    member: str | None = None,
    sparsity: str | None = None,
    shift: float = 0.0,
    support: bool = False,
    anchor: bool = False,
) -> BenchmarkEventV1:
    bid = 1.1000 + ordinal * 0.0001 + shift
    ask = bid + 0.0001
    mid = (bid + ask) / 2
    return BenchmarkEventV1(
        source_event_id=f"source-{ordinal}",
        symbol="EURUSD",
        event_time_ns=window.core_start_ns + 100 + ordinal * 100,
        event_sequence=ordinal,
        bid=bid,
        ask=ask,
        epoch_id=scenario.epoch_id,
        session="london",
        event_state="scheduled-event" if ordinal >= 3 else "normal",
        sparsity=sparsity or scenario.severity_id,
        ensemble_member_id=member,
        anchor_id=f"anchor-{ordinal}" if anchor else None,
        support_lower_mid=mid - 0.0002 if support else None,
        support_upper_mid=mid + 0.0002 if support else None,
    )


def _events(
    scenario: BenchmarkScenarioV1,
    window: ReconstructionWindowV1,
    ordinals: tuple[int, ...],
    **kwargs: object,
) -> tuple[BenchmarkEventV1, ...]:
    return tuple(
        _event(scenario, window, ordinal, **kwargs) for ordinal in ordinals
    )


def _candidate_windows(
    manifest: ReverseDegradationBenchmarkManifestV1,
    scenario: BenchmarkScenarioV1,
    window: ReconstructionWindowV1,
    reference: tuple[BenchmarkEventV1, ...],
    degraded: tuple[BenchmarkEventV1, ...],
    *,
    hard_violation: bool = False,
    drop_candidate_anchors: bool = False,
) -> tuple[BenchmarkCandidateWindowV1, ...]:
    overlay = tuple(
        replace(
            event,
            ensemble_member_id="control",
            benchmark_event_id="",
        )
        for event in degraded
    )
    windows = list(
        build_benchmark_control_windows(
            manifest,
            scenario,
            window,
            degraded,
            empirical_overlay_events=overlay,
        )
    )
    candidate = next(
        item
        for item in manifest.candidates
        if item.kind is BenchmarkCandidateKind.CANDIDATE
    )
    for member, shift in (("member-a", 0.00001), ("member-b", -0.00001)):
        generated = tuple(
            replace(
                item,
                ensemble_member_id=member,
                bid=item.bid + shift,
                ask=item.ask + shift,
                anchor_id=None if drop_candidate_anchors else item.anchor_id,
                support_lower_mid=item.mid - 0.0002,
                support_upper_mid=item.mid + 0.0002,
                benchmark_event_id="",
            )
            for item in reference
        )
        windows.append(
            BenchmarkCandidateWindowV1(
                scenario_id=scenario.scenario_id,
                candidate_id=candidate.candidate_id,
                window_id=window.window_id,
                ensemble_member_id=member,
                events=generated,
                execution=BenchmarkExecutionEvidenceV1(
                    attempted=True,
                    converged=True,
                    wall_time_ms=5,
                    peak_memory_bytes=1_024,
                    scratch_bytes=2_048,
                    durable_bytes=256,
                ),
                hard_constraint_violations=(
                    {"historical_anchor_violation": 1} if hard_violation else {}
                ),
                cross_series_hooks={"triangle_residual": 0.00001},
                strategy_hooks={"spread_cost_delta": 0.00002},
            )
        )
    return tuple(windows)


def _run_scorecard(
    *,
    hard_violation: bool = False,
    protected_anchors: bool = False,
    drop_candidate_anchors: bool = False,
) -> tuple[
    ReverseDegradationBenchmarkManifestV1, ReverseDegradationScorecardV1
]:
    manifest = _manifest()
    engine = ReverseDegradationBenchmarkV1(manifest)
    for scenario in manifest.scenarios:
        window = _window(scenario)
        reference = _events(
            scenario,
            window,
            (0, 1, 2, 3, 4, 5),
            anchor=protected_anchors,
        )
        degraded = _events(
            scenario,
            window,
            (0, 2, 5),
            anchor=protected_anchors,
        )
        engine.consume_window(
            scenario_id=scenario.scenario_id,
            window=window,
            reference_events=reference,
            degraded_events=degraded,
            candidate_windows=_candidate_windows(
                manifest,
                scenario,
                window,
                reference,
                degraded,
                hard_violation=hard_violation,
                drop_candidate_anchors=drop_candidate_anchors,
            ),
        )
    return manifest, engine.finalize()


def test_manifest_is_immutable_versioned_and_information_bound() -> None:
    information = _information_manifest()
    manifest = _manifest()

    validate_benchmark_information_boundary(manifest, information)
    assert (
        ReverseDegradationBenchmarkManifestV1.from_json(manifest.to_json())
        == manifest
    )
    assert tuple(item.kind for item in manifest.splits) == (
        BenchmarkSplitKind.CALIBRATION,
        BenchmarkSplitKind.VALIDATION,
        BenchmarkSplitKind.FINAL_HOLDOUT,
    )
    assert len({item.epoch_id for item in manifest.scenarios}) == 2
    assert len({item.severity_id for item in manifest.scenarios}) == 2
    assert all(
        candidate.generator_config_id != scenario.degradation_config_id
        for candidate in manifest.candidates
        for scenario in manifest.scenarios
    )
    with pytest.raises(ValueError, match="calibration differs"):
        validate_benchmark_information_boundary(
            replace(
                manifest,
                splits=(
                    BenchmarkSplitV1(
                        BenchmarkSplitKind.CALIBRATION,
                        BASE_NS + 1_001,
                        BASE_NS + 2_000,
                    ),
                    *manifest.splits[1:],
                ),
                manifest_id="",
            ),
            information,
        )


def test_manifest_requires_full_controls_multiple_cells_and_no_winner() -> None:
    manifest = _manifest()
    controls = tuple(
        item for item in manifest.candidates if item.control_kind is not None
    )

    with pytest.raises(ValueError, match="control set differs"):
        replace(manifest, candidates=controls[:-1], manifest_id="")
    duplicate_no_fill = _candidate(
        BenchmarkCandidateKind.CONTROL,
        "second-no-fill",
        control=BenchmarkControlKind.NO_FILL,
    )
    with pytest.raises(ValueError, match="exactly one"):
        replace(
            manifest,
            candidates=manifest.candidates + (duplicate_no_fill,),
            manifest_id="",
        )
    with pytest.raises(ValueError, match="multiple feed epochs"):
        replace(
            manifest,
            scenarios=tuple(
                item
                for item in manifest.scenarios
                if item.epoch_id == "epoch-modern"
            ),
            manifest_id="",
        )
    with pytest.raises(ValueError, match="automatic winner"):
        replace(manifest, automatic_winner=True, manifest_id="")


def test_controls_are_transparent_bounded_and_overlay_is_same_cardinality() -> (
    None
):
    manifest = _manifest()
    scenario = manifest.scenarios[0]
    window = _window(scenario)
    degraded = _events(scenario, window, (0, 2, 5))
    by_control = {
        item.control_kind: item
        for item in manifest.candidates
        if item.control_kind is not None
    }

    no_fill = build_benchmark_control_events(
        by_control[BenchmarkControlKind.NO_FILL],
        degraded,
        ensemble_member_id="control",
    )
    interpolated = build_benchmark_control_events(
        by_control[BenchmarkControlKind.LINEAR_INTERPOLATION],
        degraded,
        ensemble_member_id="control",
    )
    resampled = build_benchmark_control_events(
        by_control[BenchmarkControlKind.RESAMPLE_LAST],
        degraded,
        ensemble_member_id="control",
    )

    assert len(no_fill) == len(degraded)
    assert len(interpolated) > len(degraded)
    assert len(resampled) <= len(degraded)
    with pytest.raises(ValueError, match="preserve degraded-row cardinality"):
        build_benchmark_control_events(
            by_control[BenchmarkControlKind.EMPIRICAL_OVERLAY],
            degraded,
            ensemble_member_id="control",
            empirical_overlay_events=degraded[:-1],
        )


def test_empirical_overlay_adapter_consumes_augmented_columns() -> None:
    frame = pl.DataFrame(
        {
            "timestamp_utc_ms": [1_700_000_000_000, 1_700_000_000_100],
            "synth_bid": [1.1, 1.1001],
            "synth_ask": [1.1002, 1.1003],
        }
    )

    events = benchmark_events_from_empirical_overlay(
        frame,
        symbol="eurusd",
        epoch_id="epoch-modern",
        session="london",
        event_state="normal",
    )

    assert len(events) == frame.height
    assert events[0].event_time_ns == 1_700_000_000_000_000_000
    assert events[0].symbol == "EURUSD"
    assert events[0].sparsity == "empirical_overlay"


class _FixtureGenerator:
    """Deterministic generator implementing the public benchmark protocol."""

    event_schema_version = BENCHMARK_EVENT_SCHEMA_VERSION

    def __init__(self, candidate_id: str) -> None:
        self.candidate_id = candidate_id

    def generate(
        self,
        degraded_events: tuple[BenchmarkEventV1, ...],
        *,
        scenario: BenchmarkScenarioV1,
        window: ReconstructionWindowV1,
        ensemble_member_id: str,
    ) -> tuple[BenchmarkEventV1, ...]:
        del scenario, window
        return tuple(
            replace(
                item,
                ensemble_member_id=ensemble_member_id,
                benchmark_event_id="",
            )
            for item in degraded_events
        )


def test_generator_uses_shared_interface_and_independent_identity() -> None:
    manifest = _manifest()
    scenario = manifest.scenarios[0]
    window = _window(scenario)
    degraded = _events(scenario, window, (0, 2, 5))
    candidate = next(
        item
        for item in manifest.candidates
        if item.kind is BenchmarkCandidateKind.CANDIDATE
    )

    generated = generate_benchmark_candidate_window(
        _FixtureGenerator(candidate.candidate_id),
        candidate,
        degraded,
        scenario=scenario,
        window=window,
        ensemble_member_id="member-a",
        execution=BenchmarkExecutionEvidenceV1(attempted=True, converged=True),
    )

    assert len(generated.events) == len(degraded)
    assert generated.metadata()["events_inline"] is False
    assert "events" not in generated.metadata()
    assert all(
        item.schema_version == BENCHMARK_EVENT_SCHEMA_VERSION
        for item in generated.events
    )


class _ObservationOperatorStub:
    """Small valid observation result producer for adapter integration."""

    operator_id = (
        "observation-operator:sha256:"
        + hashlib.sha256(b"benchmark-operator").hexdigest()
    )
    stratum_id = (
        "observation-stratum:sha256:"
        + hashlib.sha256(b"benchmark-stratum").hexdigest()
    )

    def degrade(
        self,
        events: list[ObservationInputEventV1],
        *,
        window: ReconstructionWindowV1,
        carry: ObservationCarryStateV1 | None,
        protected_event_ids: tuple[str, ...],
        source_start: bool,
    ) -> ObservationApplicationResultV1:
        del carry, source_start
        outputs = tuple(
            ObservationOutputEventV1(
                source_event_id=event.source_event_id,
                operator_id=self.operator_id,
                stratum_id=self.stratum_id,
                symbol=event.symbol,
                source_time_ns=event.event_time_ns,
                observed_time_ns=event.event_time_ns,
                observed_sequence=0,
                bid=event.bid,
                ask=event.ask,
                duplicate_ordinal=0,
                transformations=(),
                protected_anchor=event.source_event_id in protected_event_ids,
            )
            for event in events
        )
        last = outputs[-1]
        return ObservationApplicationResultV1(
            operator_id=self.operator_id,
            window_id=window.window_id,
            symbol=last.symbol,
            application_mode="degrade",
            input_count=len(events),
            output_events=outputs,
            reason_counts={"retained": len(events)},
            fallback_counts={"global": len(events)},
            diagnostic_samples=(),
            samples_truncated=False,
            carry_state=ObservationCarryStateV1(
                operator_id=self.operator_id,
                symbol=last.symbol,
                last_source_time_ns=last.source_time_ns,
                last_observed_time_ns=last.observed_time_ns,
                last_bid=last.bid,
                last_ask=last.ask,
            ),
        )


def test_degradation_adapter_binds_operator_and_preserves_protected_anchor() -> (
    None
):
    manifest = _manifest()
    original = manifest.scenarios[0]
    operator = _ObservationOperatorStub()
    scenario = replace(
        original,
        observation_operator_id=operator.operator_id,
        scenario_id="",
        degradation_config_id="",
    )
    window = _window(scenario)
    reference = _events(scenario, window, (0,), anchor=True)

    degraded, result = degrade_benchmark_window(
        operator,  # type: ignore[arg-type]
        reference,
        scenario=scenario,
        window=window,
        protected_event_ids=(reference[0].source_event_id,),
        source_start=True,
    )

    assert result.application_mode == "degrade"
    assert degraded[0].anchor_id == reference[0].anchor_id
    assert degraded[0].schema_version == BENCHMARK_EVENT_SCHEMA_VERSION
    with pytest.raises(ValueError, match="operator identity differs"):
        degrade_benchmark_window(
            operator,  # type: ignore[arg-type]
            reference,
            scenario=original,
            window=window,
            source_start=True,
        )


def test_scorecard_is_stratified_reproducible_and_never_selects_winner() -> (
    None
):
    manifest, scorecard = _run_scorecard()
    _, repeated = _run_scorecard()

    assert scorecard.scorecard_id == repeated.scorecard_id
    assert (
        ReverseDegradationScorecardV1.from_json(scorecard.to_json())
        == scorecard
    )
    assert scorecard.automatic_winner is False
    assert scorecard.to_dict()["winner_candidate_id"] is None
    assert len(scorecard.candidate_scores) == (
        len(manifest.scenarios) * len(manifest.candidates)
    )
    candidate_scores = [
        item
        for item in scorecard.candidate_scores
        if manifest.candidate_by_id(item.candidate_id).kind
        is BenchmarkCandidateKind.CANDIDATE
    ]
    assert all(item.promotion_eligible for item in candidate_scores)
    assert all(len(item.slice_scores) >= 2 for item in candidate_scores)
    assert all(
        item.uncertainty_metrics["support_interval_count"] > 0
        for item in candidate_scores
    )
    assert all(
        item.uncertainty_metrics["ensemble_common_event_comparison_count"] > 0
        for item in candidate_scores
    )
    assert all(item.cross_series_hooks for item in candidate_scores)
    assert all(item.strategy_hooks for item in candidate_scores)
    assert all(
        item.execution_summary["attempted_count"] == 2
        and item.execution_summary["converged_count"] == 2
        and item.execution_summary["failure_count"] == 0
        and item.execution_summary["peak_memory_bytes"] == 1_024
        and item.execution_summary["scratch_bytes"] == 4_096
        and item.execution_summary["durable_bytes"] == 512
        for item in candidate_scores
    )
    assert all(
        "worst_slice_soft_loss" in item.aggregate_metrics
        and "mean_soft_loss_delta" in item.relative_to_no_fill
        for item in scorecard.candidate_scores
    )
    metric_names = set(candidate_scores[0].slice_scores[0].metrics)
    assert {
        "event_count_relative_error",
        "interarrival_hist_l1",
        "burst_duration_relative_error",
        "quiet_duration_relative_error",
        "bid_mean_relative_error",
        "ask_mean_relative_error",
        "spread_hist_l1",
        "bid_transition_relative_error",
        "ask_transition_relative_error",
        "mid_transition_relative_error",
        "spread_transition_relative_error",
        "mid_range_relative_error",
        "endpoint_relative_error",
    } <= metric_names


def test_hard_constraint_violation_blocks_soft_fit_promotion() -> None:
    manifest, scorecard = _run_scorecard(hard_violation=True)
    candidate_scores = [
        item
        for item in scorecard.candidate_scores
        if manifest.candidate_by_id(item.candidate_id).kind
        is BenchmarkCandidateKind.CANDIDATE
    ]

    assert all(not item.promotion_eligible for item in candidate_scores)
    assert all(
        item.hard_constraint_violations["historical_anchor_violation"] == 2
        for item in candidate_scores
    )
    assert all(
        item.aggregate_metrics["mean_soft_loss"] < 1
        for item in candidate_scores
    )


def test_missing_protected_anchor_is_an_automatic_hard_gate() -> None:
    manifest, scorecard = _run_scorecard(
        protected_anchors=True,
        drop_candidate_anchors=True,
    )
    candidate_scores = [
        item
        for item in scorecard.candidate_scores
        if manifest.candidate_by_id(item.candidate_id).kind
        is BenchmarkCandidateKind.CANDIDATE
    ]

    assert all(not item.promotion_eligible for item in candidate_scores)
    assert all(
        item.hard_constraint_violations["historical_anchor_missing"] > 0
        for item in candidate_scores
    )
    with pytest.raises(ValueError, match="always block promotion"):
        replace(
            candidate_scores[0],
            promotion_eligible=True,
            candidate_score_id="",
        )


def test_finalize_rejects_partial_immutable_split_coverage() -> None:
    manifest = _manifest()
    engine = ReverseDegradationBenchmarkV1(manifest)
    for index, scenario in enumerate(manifest.scenarios):
        full_window = _window(scenario)
        window = (
            replace(
                full_window,
                core_end_ns=full_window.core_start_ns + 1_000,
                window_id="",
                synchronization_unit_id="",
            )
            if index == 0
            else full_window
        )
        reference = _events(scenario, window, (0, 1, 2))
        degraded = _events(scenario, window, (0, 2))
        engine.consume_window(
            scenario_id=scenario.scenario_id,
            window=window,
            reference_events=reference,
            degraded_events=degraded,
            candidate_windows=_candidate_windows(
                manifest, scenario, window, reference, degraded
            ),
        )

    with pytest.raises(ValueError, match="complete split"):
        engine.finalize()


def test_engine_fails_closed_on_missing_members_bounds_and_reuse() -> None:
    manifest = _manifest()
    scenario = manifest.scenarios[0]
    window = _window(scenario)
    reference = _events(scenario, window, (0, 1, 2))
    degraded = _events(scenario, window, (0, 2))
    windows = _candidate_windows(
        manifest, scenario, window, reference, degraded
    )
    engine = ReverseDegradationBenchmarkV1(manifest)

    with pytest.raises(ValueError, match="cover configured ensemble members"):
        engine.consume_window(
            scenario_id=scenario.scenario_id,
            window=window,
            reference_events=reference,
            degraded_events=degraded,
            candidate_windows=windows[:-1],
        )
    outside = replace(
        reference[0],
        event_time_ns=window.core_end_ns,
        benchmark_event_id="",
    )
    with pytest.raises(ValueError, match="outside window ownership"):
        engine.consume_window(
            scenario_id=scenario.scenario_id,
            window=window,
            reference_events=(outside,),
            degraded_events=degraded,
            candidate_windows=windows,
        )
    with pytest.raises(ValueError, match="unprocessed scenario cells"):
        engine.finalize()


def test_profile_bounds_refuse_unbounded_online_state() -> None:
    profile = replace(BenchmarkProfileV1(), max_slices=1, profile_id="")
    manifest = _manifest(profile=profile)
    scenario = manifest.scenarios[0]
    window = _window(scenario)
    reference = _events(scenario, window, (0, 1, 2))
    degraded = _events(scenario, window, (0, 2))

    with pytest.raises(ValueError, match="slice limit"):
        ReverseDegradationBenchmarkV1(manifest).consume_window(
            scenario_id=scenario.scenario_id,
            window=window,
            reference_events=reference,
            degraded_events=degraded,
            candidate_windows=_candidate_windows(
                manifest, scenario, window, reference, degraded
            ),
        )
