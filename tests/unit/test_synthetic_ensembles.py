"""Tests for calibrated reconstruction-ensemble contracts."""

from __future__ import annotations

from dataclasses import replace
import hashlib

import pytest

from histdatacom.synthetic import (
    ENSEMBLE_CALIBRATION_METRIC_NAMES,
    ENSEMBLE_CONFIDENCE_QUANTITY,
    BenchmarkCandidateWindowV1,
    BenchmarkEventV1,
    BenchmarkExecutionEvidenceV1,
    BenchmarkScenarioV1,
    BenchmarkSplitKind,
    EnsembleCalibrationConfigV1,
    EnsembleCalibrationReportV1,
    EnsembleCalibrationSampleV1,
    EnsembleCalibrationStratumV1,
    EnsembleDiversityStatus,
    EnsembleMemberCalibrationV1,
    EnsembleMemberStatus,
    EnsembleReportStatus,
    EnsembleStorageEstimateV1,
    ReconstructionEnsemblePlanV1,
    ReconstructionResourceLimitError,
    benchmark_ensemble_calibration_sample,
    benchmark_logical_content_sha256,
    build_ensemble_regeneration_request,
    calibrate_reconstruction_ensemble,
    estimate_reconstruction_ensemble_resources,
    plan_reconstruction_ensemble,
    verify_ensemble_regeneration,
)

HORIZON_NS = 60_000_000_000
BASE_METRICS = {
    "event_count": 100.0,
    "observed_duration_ns": 60_000_000_000.0,
    "mean_interarrival_ns": 600_000_000.0,
    "mean_spread": 0.0002,
    "mid_path_range": 0.002,
    "endpoint_mid": 1.1,
    "downstream_sensitivity": 0.5,
}


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _config(**changes: object) -> EnsembleCalibrationConfigV1:
    values: dict[str, object] = {
        "member_count": 3,
        "retained_member_count": 2,
        "horizons_ns": (HORIZON_NS,),
        "minimum_fit_samples": 2,
        "nominal_coverage": 0.8,
        "minimum_achieved_coverage": 0.5,
    }
    values.update(changes)
    return EnsembleCalibrationConfigV1(**values)  # type: ignore[arg-type]


def _plan(
    *,
    config: EnsembleCalibrationConfigV1 | None = None,
) -> ReconstructionEnsemblePlanV1:
    return plan_reconstruction_ensemble(
        symbols=("eurusd",),
        source_artifact_hashes={"source-eurusd": _sha("source-eurusd")},
        configuration_artifact_hashes={"generator-v1": _sha("generator-v1")},
        base_seed=20260713,
        config=config or _config(),
    )


def _metrics(scale: float) -> dict[str, float]:
    return {name: value * scale for name, value in BASE_METRICS.items()}


def _stratum() -> EnsembleCalibrationStratumV1:
    return EnsembleCalibrationStratumV1(
        epoch_id="modern-dense",
        session="london",
        event_state="ordinary",
        symbol="eurusd",
        horizon_ns=HORIZON_NS,
        sparsity="sparse-20pct",
    )


def _sample(
    plan: ReconstructionEnsemblePlanV1,
    *,
    split: BenchmarkSplitKind,
    ordinal: int,
    collapsed: bool = False,
    false_diversity: bool = False,
) -> EnsembleCalibrationSampleV1:
    members = []
    for index, member in enumerate(plan.members):
        scale = 1.0 if false_diversity else (0.9 + index * 0.1)
        digest_label = (
            f"shared-{split.value}-{ordinal}"
            if collapsed
            else f"{member.member_id}-{split.value}-{ordinal}"
        )
        members.append(
            EnsembleMemberCalibrationV1(
                member_id=member.member_id,
                status=EnsembleMemberStatus.COMPLETED,
                metrics=_metrics(scale),
                logical_content_sha256=_sha(digest_label),
            )
        )
    return EnsembleCalibrationSampleV1(
        benchmark_manifest_id="benchmark-manifest-v1",
        scenario_id=f"scenario-{split.value}-{ordinal}",
        candidate_id="empirical-motif-v1",
        window_id=f"window-{split.value}-{ordinal}",
        split_kind=split,
        stratum=_stratum(),
        reference_metrics=_metrics(1.0),
        reference_content_sha256=_sha(f"reference-{split.value}-{ordinal}"),
        members=tuple(members),
    )


def _calibrated_report(
    plan: ReconstructionEnsemblePlanV1,
) -> tuple[EnsembleCalibrationReportV1, EnsembleStorageEstimateV1]:
    counts = {member.member_id: 100 for member in plan.members}
    estimate = estimate_reconstruction_ensemble_resources(
        plan,
        input_event_count=100,
        member_event_counts=counts,
    )
    samples = (
        _sample(plan, split=BenchmarkSplitKind.VALIDATION, ordinal=1),
        _sample(plan, split=BenchmarkSplitKind.VALIDATION, ordinal=2),
        _sample(plan, split=BenchmarkSplitKind.FINAL_HOLDOUT, ordinal=1),
    )
    return (
        calibrate_reconstruction_ensemble(
            plan,
            samples=samples,
            storage_estimate=estimate,
        ),
        estimate,
    )


def _event(
    source_id: str,
    *,
    time_ns: int,
    sequence: int,
    mid: float,
    member_id: str | None = None,
) -> BenchmarkEventV1:
    return BenchmarkEventV1(
        source_event_id=source_id,
        symbol="eurusd",
        event_time_ns=time_ns,
        event_sequence=sequence,
        bid=mid - 0.0001,
        ask=mid + 0.0001,
        epoch_id="modern-dense",
        session="london",
        event_state="ordinary",
        sparsity="sparse-20pct",
        ensemble_member_id=member_id,
    )


def test_plan_is_deterministic_hash_bound_and_round_trips() -> None:
    first = _plan()
    second = _plan()

    assert first == second
    assert first.plan_id == second.plan_id
    assert len({item.member_id for item in first.members}) == 3
    assert len({item.seed for item in first.members}) == 3
    assert ReconstructionEnsemblePlanV1.from_json(first.to_json()) == first

    with pytest.raises(ValueError, match="config hash differs"):
        plan_reconstruction_ensemble(
            symbols=("eurusd",),
            source_artifact_hashes={"source-eurusd": _sha("source-eurusd")},
            configuration_artifact_hashes={
                first.config.config_id: _sha("tampered-config")
            },
            base_seed=20260713,
            config=first.config,
        )


def test_resource_preflight_accounts_for_all_members_and_retained_quota() -> (
    None
):
    plan = _plan()
    counts = {
        plan.members[0].member_id: 10,
        plan.members[1].member_id: 20,
        plan.members[2].member_id: 30,
    }
    estimate = estimate_reconstruction_ensemble_resources(
        plan,
        input_event_count=10,
        member_event_counts=counts,
    )

    assert estimate.conservative_retained_event_count == 50
    assert estimate.resource_estimate.candidate_event_count == 60
    assert estimate.resource_estimate.estimated_output_bytes == (
        50 * plan.config.estimated_bytes_per_event
    )
    assert EnsembleStorageEstimateV1.from_json(estimate.to_json()) == estimate
    with pytest.raises(ValueError, match="resource estimate arithmetic"):
        replace(
            estimate,
            resource_estimate=replace(
                estimate.resource_estimate,
                estimated_output_bytes=1,
                estimate_id="",
            ),
            estimate_id="",
        )

    with pytest.raises(ReconstructionResourceLimitError, match="amplification"):
        estimate_reconstruction_ensemble_resources(
            plan,
            input_event_count=1,
            member_event_counts=counts,
        )


def test_reverse_degradation_adapter_is_compact_and_records_failures() -> None:
    plan = _plan()
    scenario = BenchmarkScenarioV1(
        split_kind=BenchmarkSplitKind.VALIDATION,
        epoch_id="modern-dense",
        severity_id="sparse-20pct",
        observation_operator_id="operator-v1",
        degradation_parameters={"retention_rate": 0.2},
    )
    reference = tuple(
        _event(
            f"reference-{index}", time_ns=index * 1_000, sequence=index, mid=1.1
        )
        for index in range(1, 4)
    )
    windows = []
    for index, member in enumerate(plan.members):
        execution = BenchmarkExecutionEvidenceV1(
            attempted=True,
            converged=index != 2,
            failure_reason="worker_failed" if index == 2 else None,
        )
        events = tuple(
            _event(
                f"candidate-{index}-{event_index}",
                time_ns=event_index * 1_000,
                sequence=event_index,
                mid=1.1 + index * 0.0001,
                member_id=member.member_id,
            )
            for event_index in range(1, 4)
        )
        windows.append(
            BenchmarkCandidateWindowV1(
                scenario_id=scenario.scenario_id,
                candidate_id="empirical-motif-v1",
                window_id="window-1",
                ensemble_member_id=member.member_id,
                events=events,
                execution=execution,
                hard_constraint_violations=(
                    {"negative_spread": 1} if index == 1 else {}
                ),
                strategy_hooks={"downstream_sensitivity": 0.5},
            )
        )

    sample = benchmark_ensemble_calibration_sample(
        plan,
        benchmark_manifest_id="benchmark-manifest-v1",
        scenario=scenario,
        candidate_id="empirical-motif-v1",
        window_id="window-1",
        horizon_ns=HORIZON_NS,
        reference_events=reference,
        member_windows=windows,
        reference_downstream_sensitivity=0.5,
    )

    assert sorted(item.status.value for item in sample.members) == [
        "completed",
        "failed",
        "refused",
    ]
    assert sample.stratum.to_dict().keys() >= {
        "epoch_id",
        "session",
        "event_state",
        "symbol",
        "horizon_ns",
        "sparsity",
    }
    assert sample.to_dict()["event_rows_inline"] is False
    assert EnsembleCalibrationSampleV1.from_json(sample.to_json()) == sample
    assert benchmark_logical_content_sha256(reference) == (
        benchmark_logical_content_sha256(reversed(reference))
    )
    reidentified = tuple(
        replace(
            event,
            source_event_id=f"retry-or-seed-{index}",
            ensemble_member_id="different-member",
            benchmark_event_id="",
        )
        for index, event in enumerate(reference)
    )
    assert benchmark_logical_content_sha256(reference) == (
        benchmark_logical_content_sha256(reidentified)
    )


def test_calibration_reports_defined_coverage_and_no_automatic_winner() -> None:
    plan = _plan()
    report, _ = _calibrated_report(plan)

    assert report.status is EnsembleReportStatus.CALIBRATED
    assert report.candidate_id == "empirical-motif-v1"
    assert report.primary_member_id in report.retained_member_ids
    assert len(report.retained_member_ids) == 2
    assert len(report.regenerable_member_ids) == 1
    assert report.automatic_winner is False
    assert report.default_generator_id is None
    assert len(report.metric_calibrations) == len(
        ENSEMBLE_CALIBRATION_METRIC_NAMES
    )
    assert all(
        item.calibrated_coverage_rate == 1.0
        for item in report.metric_calibrations
    )
    payload = report.to_dict()
    assert payload["confidence_quantity"] == ENSEMBLE_CONFIDENCE_QUANTITY
    assert payload["confidence_scope"] == (
        "stratum_metric_horizon_summary_not_per_event"
    )
    assert payload["primary_interpretation"] == (
        "representative_member_not_historical_truth"
    )
    assert payload["winner_member_id"] is None
    assert payload["event_rows_inline"] is False
    assert EnsembleCalibrationReportV1.from_json(report.to_json()) == report
    with pytest.raises(ValueError, match="status differs from evidence"):
        replace(
            report,
            status=EnsembleReportStatus.MISCALIBRATED,
            report_id="",
        )


def test_every_configured_horizon_requires_calibration_evidence() -> None:
    plan = _plan(config=_config(horizons_ns=(HORIZON_NS, HORIZON_NS * 2)))
    estimate = estimate_reconstruction_ensemble_resources(
        plan,
        input_event_count=100,
        member_event_counts={item.member_id: 100 for item in plan.members},
    )

    with pytest.raises(ValueError, match="configured horizons"):
        calibrate_reconstruction_ensemble(
            plan,
            samples=(
                _sample(
                    plan,
                    split=BenchmarkSplitKind.VALIDATION,
                    ordinal=1,
                ),
                _sample(
                    plan,
                    split=BenchmarkSplitKind.VALIDATION,
                    ordinal=2,
                ),
                _sample(
                    plan,
                    split=BenchmarkSplitKind.FINAL_HOLDOUT,
                    ordinal=1,
                ),
            ),
            storage_estimate=estimate,
        )


@pytest.mark.parametrize(  # type: ignore[untyped-decorator]
    ("collapsed", "false_diversity", "expected"),
    (
        (True, False, EnsembleDiversityStatus.COLLAPSED),
        (False, True, EnsembleDiversityStatus.FALSE_DIVERSITY),
    ),
)
def test_non_substantive_member_diversity_blocks_calibrated_status(
    collapsed: bool,
    false_diversity: bool,
    expected: EnsembleDiversityStatus,
) -> None:
    plan = _plan()
    estimate = estimate_reconstruction_ensemble_resources(
        plan,
        input_event_count=100,
        member_event_counts={item.member_id: 100 for item in plan.members},
    )
    samples = tuple(
        _sample(
            plan,
            split=split,
            ordinal=ordinal,
            collapsed=collapsed,
            false_diversity=false_diversity,
        )
        for split, ordinal in (
            (BenchmarkSplitKind.VALIDATION, 1),
            (BenchmarkSplitKind.VALIDATION, 2),
            (BenchmarkSplitKind.FINAL_HOLDOUT, 1),
        )
    )

    report = calibrate_reconstruction_ensemble(
        plan,
        samples=samples,
        storage_estimate=estimate,
    )

    assert report.status is EnsembleReportStatus.MISCALIBRATED
    assert {item.status for item in report.diversity_summaries} == {expected}


def test_regeneration_requires_calibration_omission_and_exact_hashes() -> None:
    plan = _plan()
    report, _ = _calibrated_report(plan)
    omitted = report.regenerable_member_ids
    request = build_ensemble_regeneration_request(
        plan,
        report,
        member_ids=omitted,
    )
    assert (
        tuple(
            item.member_id
            for item in verify_ensemble_regeneration(
                plan,
                report,
                request,
                available_source_artifact_hashes=plan.source_hashes,
                available_configuration_artifact_hashes=(
                    plan.configuration_hashes
                ),
            )
        )
        == omitted
    )
    with pytest.raises(ValueError, match="retained/unknown"):
        verify_ensemble_regeneration(
            plan,
            report,
            build_ensemble_regeneration_request(
                plan,
                report,
                member_ids=(report.retained_member_ids[0],),
            ),
            available_source_artifact_hashes=plan.source_hashes,
            available_configuration_artifact_hashes=(plan.configuration_hashes),
        )
    tampered = replace(
        request,
        source_artifact_hashes={"source-eurusd": _sha("different-source")},
        request_id="",
    )
    with pytest.raises(ValueError, match="source hashes differ"):
        verify_ensemble_regeneration(
            plan,
            report,
            tampered,
            available_source_artifact_hashes=plan.source_hashes,
            available_configuration_artifact_hashes=(plan.configuration_hashes),
        )
    with pytest.raises(ValueError, match="available regeneration source"):
        verify_ensemble_regeneration(
            plan,
            report,
            request,
            available_source_artifact_hashes={
                "source-eurusd": _sha("different-source")
            },
            available_configuration_artifact_hashes=(plan.configuration_hashes),
        )
