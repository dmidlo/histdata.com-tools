"""Powered process-aware qualification contracts and finite-sample behavior."""

from __future__ import annotations

import hashlib
import math
import random
from pathlib import Path
from types import SimpleNamespace

import pytest

import histdatacom.synthetic.qualification as qualification_module
from histdatacom.runtime_contracts import ArtifactRef
from histdatacom.synthetic.benchmark_corpus import (
    BenchmarkWindowMetricObservationV1,
)
from histdatacom.synthetic.qualification import (
    EngineQualificationDecisionV1,
    PointProcessResidualInputV1,
    PointProcessResidualMethod,
    PoweredQualificationDossierV1,
    PoweredQualificationPolicyV1,
    PredictiveScoreReportV1,
    ProposalPortfolioCalibrationV1,
    QualificationStatus,
    evaluate_point_process_residuals,
    powered_qualification_verification_scope,
    read_powered_qualification_dossier,
    run_qualification_power_study,
    write_powered_qualification_dossier,
)


def _artifact(tmp_path: Path, label: str) -> ArtifactRef:
    content = f"{label}\n".encode()
    digest = hashlib.sha256(content).hexdigest()
    path = tmp_path / f"{label}-{digest}.json"
    path.write_bytes(content)
    return ArtifactRef(
        kind=f"{label}_v1",
        path=str(path),
        size_bytes=len(content),
        sha256=digest,
    )


def _residual_input(sample_count: int) -> PointProcessResidualInputV1:
    rng = random.Random(490)
    pits = tuple(rng.random() for _ in range(sample_count))
    return PointProcessResidualInputV1(
        engine_id="histdatacom.event-clock.nhpp",
        config_id="event-clock-config:test",
        fit_id="event-clock-fit:test",
        split_kind="final_holdout",
        stratum_id="all",
        method=PointProcessResidualMethod.ANALYTIC_TIME_RESCALING,
        integrated_hazards=tuple(-math.log1p(-value) for value in pits),
        mark_pits=pits,
    )


def test_analytic_time_rescaling_is_deterministic_and_power_guarded() -> None:
    policy = PoweredQualificationPolicyV1(minimum_residual_count=64)

    adequate = evaluate_point_process_residuals(_residual_input(256), policy)
    underpowered = evaluate_point_process_residuals(_residual_input(16), policy)

    assert adequate.status is QualificationStatus.PASSED
    assert adequate.method is PointProcessResidualMethod.ANALYTIC_TIME_RESCALING
    assert adequate.diagnostic_stage == "raw_proposal"
    assert (
        adequate.payload()["analytic_compensator_applies_to_final_product"]
        is False
    )
    assert adequate == evaluate_point_process_residuals(
        _residual_input(256), policy
    )
    assert underpowered.status is QualificationStatus.INSUFFICIENT_EVIDENCE
    assert "residual_support_below_policy_minimum" in underpowered.reason_codes
    assert adequate.payload()["residual_rows_embedded"] is False


def test_power_study_covers_every_named_gate_and_publishes_regions() -> None:
    policy = PoweredQualificationPolicyV1(power_replications=512)

    study = run_qualification_power_study(
        policy,
        trace_id="window-metric-trace:test",
        observed_support_count=3,
    )

    assert study == run_qualification_power_study(
        policy,
        trace_id="window-metric-trace:test",
        observed_support_count=3,
    )
    assert {item.gate_id for item in study.results} == set(
        policy.hard_gate_families
    )
    assert all(
        item.misspecification_family == policy.hard_gate_families[item.gate_id]
        for item in study.results
    )
    assert {item.test_method for item in study.results} == set(
        qualification_module.DEFAULT_GATE_TEST_METHODS.values()
    )
    assert all(item.alternative_parameters for item in study.results)
    assert all(
        item.status is QualificationStatus.INSUFFICIENT_EVIDENCE
        for item in study.results
    )
    assert all(
        set(item.false_positive_by_sample_size)
        == set(item.power_by_sample_size)
        for item in study.results
    )


def test_proper_scores_reward_identity_and_detect_dependence_error() -> None:
    reference = (0.0, 2.0)
    identity = ((0.0, 2.0), (0.0, 2.0))
    misspecified = ((1.0, 1.0), (1.0, 1.0))

    assert qualification_module._energy_score(reference, identity) == 0.0
    assert qualification_module._energy_score(
        reference, misspecified
    ) > qualification_module._energy_score(reference, identity)
    assert (
        qualification_module._variogram_score(reference, identity, 0.5) == 0.0
    )
    assert qualification_module._variogram_score(
        reference, misspecified, 0.5
    ) > qualification_module._variogram_score(reference, identity, 0.5)
    assert qualification_module._marginal_crps(reference, identity) == 0.0


def test_portfolio_weights_use_validation_and_ignore_final_holdout() -> None:
    engine_ids = ("engine.good", "engine.bad")
    observations: dict[str, list[BenchmarkWindowMetricObservationV1]] = {
        engine_id: [] for engine_id in engine_ids
    }
    for split in ("validation", "final_holdout"):
        for index in range(6):
            reference = {"count": float(index + 1), "spread": 1.0}
            for engine_id in engine_ids:
                for member in ("member-01", "member-02"):
                    if split == "validation":
                        offset = 0.0 if engine_id == "engine.good" else 5.0
                    else:
                        # Deliberately reverse holdout performance. Frozen weights
                        # must remain a function of validation observations only.
                        offset = 5.0 if engine_id == "engine.good" else 0.0
                    observations[engine_id].append(
                        BenchmarkWindowMetricObservationV1(
                            candidate_id=f"candidate:{engine_id}",
                            method_name=engine_id,
                            role="candidate",
                            split_kind=split,
                            window_id=f"{split}-window-{index}",
                            ensemble_member_id=member,
                            reference_metrics=reference,
                            candidate_metrics={
                                "count": reference["count"] + offset,
                                "spread": reference["spread"] + offset,
                            },
                            comparison_metrics={"error": offset},
                        )
                    )
    scales = {"count": 1.0, "spread": 1.0}
    validation_windows = tuple(
        f"validation-window-{index}" for index in range(6)
    )

    weights = qualification_module._fit_energy_weights(
        engine_ids,
        observations,
        validation_windows,
        scales,
    )
    without_holdout = {
        key: tuple(item for item in values if item.split_kind == "validation")
        for key, values in observations.items()
    }

    assert weights == qualification_module._fit_energy_weights(
        engine_ids,
        without_holdout,
        validation_windows,
        scales,
    )
    assert weights["engine.good"] > weights["engine.bad"]


def test_dossier_round_trip_is_content_addressed_and_row_free(
    tmp_path: Path,
) -> None:
    policy = PoweredQualificationPolicyV1(power_replications=512)
    power = run_qualification_power_study(
        policy,
        trace_id="window-metric-trace:test",
        observed_support_count=6,
    )
    residual = evaluate_point_process_residuals(_residual_input(256), policy)
    score = PredictiveScoreReportV1(
        engine_id=residual.engine_id,
        split_kind="final_holdout",
        feature_names=("count", "spread"),
        window_count=6,
        member_observation_count=12,
        energy_score=0.1,
        variogram_score_p05=0.1,
        variogram_score_p1=0.1,
        marginal_crps=0.1,
        nominal_coverage=0.90,
        empirical_coverage=0.90,
        calibration_error=0.0,
        sharpness=0.2,
        tail_error=0.1,
        path_error=0.1,
        cross_series_error=0.1,
        status=QualificationStatus.PASSED,
        reason_codes=("predictive_scores_computed",),
        trace_id=power.trace_id,
    )
    calibration = ProposalPortfolioCalibrationV1(
        evaluation_id="proposal-evaluation:test",
        trace_id=power.trace_id,
        engine_ids=(residual.engine_id,),
        weights={residual.engine_id: 1.0},
        fit_window_ids=tuple(f"validation-{index}" for index in range(6)),
        final_holdout_window_ids=tuple(
            f"holdout-{index}" for index in range(6)
        ),
        validation_energy_score=0.1,
        final_holdout_energy_score=0.1,
        final_holdout_variogram_score_p05=0.1,
        status=QualificationStatus.PASSED,
        reason_codes=("single_legacy_qualified_engine",),
    )
    decision = EngineQualificationDecisionV1(
        engine_id=residual.engine_id,
        evidence_ids=("evidence:test",),
        residual_report_ids=(residual.report_id,),
        score_report_ids=(score.report_id,),
        power_study_id=power.study_id,
        portfolio_calibration_id=calibration.calibration_id,
        gate_statuses=dict.fromkeys(
            policy.hard_gate_families, QualificationStatus.PASSED
        ),
        adjusted_p_values={"time_uniformity": 0.5, "mark_calibration": 0.5},
        benchmark_eligible=True,
        reconstruction_eligible=True,
        ensemble_eligible=True,
        status=QualificationStatus.PASSED,
        reason_codes=("all_powered_gates_pass",),
    )
    dossier = PoweredQualificationDossierV1(
        evaluation_id=calibration.evaluation_id,
        registry_id="proposal-registry:test",
        corpus_id="benchmark-corpus:test",
        campaign_id="benchmark-campaign:test",
        experiment_id="reconstruction-experiment:test",
        trace_id=power.trace_id,
        policy=policy,
        power_study=power,
        residual_reports=(residual,),
        score_reports=(score,),
        portfolio_calibration=calibration,
        engine_decisions=(decision,),
        control_checks={
            "dense_identity_behaves_as_reference": True,
            "negative_control_fails_for_anchor_loss": True,
            "protected_splits_disjoint": True,
            "histdata_only": True,
        },
        input_artifacts={
            "evaluation": _artifact(tmp_path, "evaluation"),
            "experiment": _artifact(tmp_path, "experiment"),
            "scorecard": _artifact(tmp_path, "scorecard"),
            "window_metric_trace": _artifact(tmp_path, "trace"),
        },
        implementation_sha256="1" * 64,
    )

    artifact = write_powered_qualification_dossier(dossier, tmp_path)

    assert read_powered_qualification_dossier(artifact.path) == dossier
    assert dossier.to_dict()["historical_truth_claim"] is False
    assert all(
        item.to_dict()["event_rows_embedded"] is False
        for item in dossier.score_reports
    )

    tampered = tmp_path / f"powered-qualification-dossier-{'0' * 64}.json"
    tampered.write_bytes(Path(artifact.path).read_bytes())
    with pytest.raises(ValueError, match="content hash differs"):
        read_powered_qualification_dossier(tampered)


def test_qualification_verification_cache_is_bounded_to_one_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[object] = []
    dossier = SimpleNamespace(dossier_id="powered-qualification-dossier:test")
    monkeypatch.setattr(
        qualification_module,
        "_verify_powered_qualification_dossier_uncached",
        lambda supplied: calls.append(supplied),
    )

    qualification_module.verify_powered_qualification_dossier(dossier)
    with powered_qualification_verification_scope():
        qualification_module.verify_powered_qualification_dossier(dossier)
        qualification_module.verify_powered_qualification_dossier(dossier)
    qualification_module.verify_powered_qualification_dossier(dossier)

    assert calls == [dossier, dossier, dossier]
