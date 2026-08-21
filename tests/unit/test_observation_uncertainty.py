"""Tests for observation-process uncertainty propagation contracts."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from histdatacom.synthetic.observation_uncertainty import (
    OBSERVATION_UNCERTAINTY_METRIC_NAMES,
    ObservationGenerationStatus,
    ObservationScenarioRetentionMode,
    ObservationUncertaintyAvailability,
    ObservationUncertaintyDiagnosticV1,
    ObservationUncertaintyEnsembleV1,
    ObservationUncertaintyPolicyV1,
    ObservationUncertaintyReportV1,
    ObservationUncertaintyScenarioKind,
    ObservationUncertaintySplit,
    build_observation_uncertainty_ensemble,
    calibrate_observation_uncertainty,
    derive_observation_uncertainty_scenarios,
    observation_uncertainty_availability,
    read_observation_uncertainty_ensemble,
    read_observation_uncertainty_policy,
    read_observation_uncertainty_report,
    write_observation_uncertainty_ensemble,
    write_observation_uncertainty_policy,
    write_observation_uncertainty_report,
)


def _conditioning(
    *, lower: float = 0.25, central: float = 0.50, upper: float = 0.75
) -> dict[str, object]:
    return {
        "schema_version": (
            "histdatacom.historical-product-observation-conditioning.v2"
        ),
        "conditioning_id": "conditioning-v1",
        "observation_operator_id": "operator-v1",
        "feed_epoch_definition_id": "epochs-v2",
        "feed_epoch_id": "modern-dense",
        "information_mode": "ex_post_reconstruction",
        "joint_retention": {
            "stratum_id": "stratum-modern-global",
            "stratum_key": "epoch:modern-dense",
            "stratum_level": "epoch",
            "retention_probability": central,
            "retention_lower_bound": lower,
            "retention_upper_bound": upper,
            "support_count": 100,
            "evidence_ids": ["retention-fit-v1"],
            "estimation_bases": ["relative-active-time-dense-denominator"],
            "provenance": ["benchmark-validation-v1"],
        },
        "symbols": {},
    }


def _members(count: int = 6) -> tuple[tuple[str, int], ...]:
    return tuple(
        (f"path-{ordinal}", 10_000 + ordinal) for ordinal in range(1, count + 1)
    )


def _ensemble(
    *,
    conditioning: dict[str, object] | None = None,
    maximum_missing_event_count: int = 10_000,
    maximum_candidate_amplification: float = 100.0,
) -> ObservationUncertaintyEnsembleV1:
    return build_observation_uncertainty_ensemble(
        ObservationUncertaintyPolicyV1(),
        conditioning or _conditioning(),
        ensemble_members=_members(),
        observed_counts={"EURUSD": 10, "GBPUSD": 12, "GLOBAL": 22},
        session="london",
        maximum_missing_event_count=maximum_missing_event_count,
        maximum_candidate_amplification=maximum_candidate_amplification,
    )


def _diagnostics(
    ensemble: ObservationUncertaintyEnsembleV1,
) -> tuple[ObservationUncertaintyDiagnosticV1, ...]:
    scenario_rank = {
        scenario.scenario_id: rank
        for rank, scenario in enumerate(ensemble.scenarios, start=1)
    }
    return tuple(
        ObservationUncertaintyDiagnosticV1(
            split=split,
            scenario_id=member.scenario_id,
            ensemble_member_id=member.ensemble_member_id,
            path_seed=member.path_seed,
            status=ObservationGenerationStatus.COMPLETED,
            metrics={
                metric: (
                    scenario_rank[member.scenario_id]
                    + member.ordinal / 100.0
                    + split_index / 10.0
                    + metric_index / 1_000.0
                )
                for metric_index, metric in enumerate(
                    OBSERVATION_UNCERTAINTY_METRIC_NAMES
                )
            },
        )
        for split_index, split in enumerate(ObservationUncertaintySplit)
        for member in ensemble.members
    )


def test_scenarios_are_endpoint_derived_and_axes_are_separate() -> None:
    ensemble = _ensemble()

    assert ensemble.admitted is True
    assert [scenario.kind for scenario in ensemble.scenarios] == [
        ObservationUncertaintyScenarioKind.HIGH_RETENTION_LOW_INFILL,
        ObservationUncertaintyScenarioKind.CENTRAL_FITTED_RETENTION,
        ObservationUncertaintyScenarioKind.LOW_RETENTION_HIGH_INFILL,
    ]
    assert [
        scenario.retention_probability for scenario in ensemble.scenarios
    ] == [
        0.75,
        0.50,
        0.25,
    ]
    assert all(
        scenario.policy_id == ensemble.policy.policy_id
        and scenario.report_quantiles == ensemble.policy.report_quantiles
        and scenario.admission_quantile == ensemble.policy.admission_quantile
        for scenario in ensemble.scenarios
    )
    assert {member.scenario_kind for member in ensemble.members} == set(
        ObservationUncertaintyScenarioKind
    )
    assert all(
        member.retention_mode is ObservationScenarioRetentionMode.FULLY_RETAINED
        for member in ensemble.members
    )
    assert len({member.path_seed for member in ensemble.members}) == len(
        ensemble.members
    )
    assert ensemble.worst_case_scenario.retention_probability == 0.25
    assert (
        ObservationUncertaintyEnsembleV1.from_json(ensemble.to_json())
        == ensemble
    )

    with pytest.raises(ValueError, match="every observation scenario"):
        ObservationUncertaintyPolicyV1(
            fully_retained_scenarios=(
                ObservationUncertaintyScenarioKind.CENTRAL_FITTED_RETENTION,
            ),
            aggregate_only_scenarios=(
                ObservationUncertaintyScenarioKind.HIGH_RETENTION_LOW_INFILL,
                ObservationUncertaintyScenarioKind.LOW_RETENTION_HIGH_INFILL,
            ),
        )


def test_cardinality_evidence_uses_verified_moments_and_changes_with_interval() -> (
    None
):
    first = _ensemble()
    narrower = _ensemble(conditioning=_conditioning(lower=0.40, upper=0.60))
    low = first.worst_case_scenario
    low_evidence = next(
        item
        for item in first.cardinality_evidence
        if item.scenario_id == low.scenario_id and item.symbol == "GLOBAL"
    )

    assert low_evidence.missing_count_mean == 66.0
    assert low_evidence.missing_count_variance == 264.0
    assert list(low_evidence.missing_count_quantiles.values()) == sorted(
        low_evidence.missing_count_quantiles.values()
    )
    assert all(
        low_evidence.total_event_count_quantiles[key]
        == 22 + low_evidence.missing_count_quantiles[key]
        for key in low_evidence.missing_count_quantiles
    )
    assert first.ensemble_id != narrower.ensemble_id
    assert (
        first.worst_case_scenario.retention_probability
        != narrower.worst_case_scenario.retention_probability
    )


def test_availability_is_explicit_and_arbitrary_multiplier_is_impossible() -> (
    None
):
    two_sided = _conditioning()
    assert (
        observation_uncertainty_availability(two_sided)
        is ObservationUncertaintyAvailability.TWO_SIDED
    )

    lower_only = _conditioning()
    joint = lower_only["joint_retention"]
    assert isinstance(joint, dict)
    joint.pop("retention_upper_bound")
    assert (
        observation_uncertainty_availability(lower_only)
        is ObservationUncertaintyAvailability.LOWER_ONLY
    )
    with pytest.raises(ValueError, match="not two-sided: lower_only"):
        derive_observation_uncertainty_scenarios(
            ObservationUncertaintyPolicyV1(), lower_only
        )

    unavailable = _conditioning()
    unavailable_joint = unavailable["joint_retention"]
    assert isinstance(unavailable_joint, dict)
    unavailable_joint.pop("retention_lower_bound")
    unavailable_joint.pop("retention_upper_bound")
    assert (
        observation_uncertainty_availability(unavailable)
        is ObservationUncertaintyAvailability.UNAVAILABLE
    )


def test_worst_case_admission_refuses_resource_unsafe_scenarios() -> None:
    refused = _ensemble(
        maximum_missing_event_count=1,
        maximum_candidate_amplification=1.01,
    )

    assert refused.admitted is False
    assert refused.refusal_reasons
    assert any(
        item.refusal_risk == "certain_or_policy_refused"
        and item.admitted is False
        for item in refused.cardinality_evidence
    )
    with pytest.raises(ValueError, match="admission decision differs"):
        replace(refused, admitted=True, ensemble_id="")


def test_report_separates_operator_and_path_dispersion() -> None:
    ensemble = _ensemble()
    report = calibrate_observation_uncertainty(ensemble, _diagnostics(ensemble))

    assert report.untouched_release_holdout is True
    assert report.holdout_selection_role is False
    assert len(report.decompositions) == (
        len(ObservationUncertaintySplit)
        * len(OBSERVATION_UNCERTAINTY_METRIC_NAMES)
    )
    assert all(
        item.operator_between_scenario_variance > 0.0
        and item.path_within_scenario_variance > 0.0
        and item.total_variance
        == item.operator_between_scenario_variance
        + item.path_within_scenario_variance
        for item in report.decompositions
    )
    assert ObservationUncertaintyReportV1.from_json(report.to_json()) == report

    without_holdout = tuple(
        item
        for item in _diagnostics(ensemble)
        if item.split is ObservationUncertaintySplit.VALIDATION
    )
    with pytest.raises(ValueError, match="validation and holdout"):
        calibrate_observation_uncertainty(ensemble, without_holdout)
    with pytest.raises(ValueError, match="untouched holdout"):
        calibrate_observation_uncertainty(
            ensemble,
            _diagnostics(ensemble),
            untouched_release_holdout=False,
        )


def test_content_addressed_artifacts_round_trip(tmp_path) -> None:
    policy = ObservationUncertaintyPolicyV1()
    ensemble = _ensemble()
    report = calibrate_observation_uncertainty(ensemble, _diagnostics(ensemble))

    policy_ref = write_observation_uncertainty_policy(policy, tmp_path)
    ensemble_ref = write_observation_uncertainty_ensemble(ensemble, tmp_path)
    report_ref = write_observation_uncertainty_report(report, tmp_path)

    assert read_observation_uncertainty_policy(policy_ref.path) == policy
    assert read_observation_uncertainty_ensemble(ensemble_ref.path) == ensemble
    assert read_observation_uncertainty_report(report_ref.path) == report
    Path(policy_ref.path).rename(tmp_path / "renamed.json")
    with pytest.raises(ValueError, match="filename differs"):
        read_observation_uncertainty_policy(tmp_path / "renamed.json")

    tampered = policy.to_dict()
    tampered["uncertainty_decomposition"] = "seed-only-v0"
    with pytest.raises(ValueError, match="uncertainty_decomposition differs"):
        ObservationUncertaintyPolicyV1.from_dict(tampered)
