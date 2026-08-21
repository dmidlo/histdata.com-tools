"""Release-critical projection-burden diagnostic regression tests."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from histdatacom.reconstruction import (
    ReconstructionCampaignProductEntryV1,
    ReconstructionCampaignProductShardV1,
    ReconstructionPlanError,
)
from histdatacom.runtime_contracts import ArtifactRef
from histdatacom.synthetic.certification import (
    modern_reference_triangle_certification_policy,
)
from histdatacom.synthetic.delivery import ReconstructionDeliveryMode
from histdatacom.synthetic.hawkes_selection import (
    DIAGONAL_HAWKES_ENGINE_ID,
    FULL_HAWKES_ENGINE_ID,
    METRIC_DIRECTIONS,
    HawkesProductSelectionPolicyV1,
    HawkesValidationComparisonV1,
    HawkesValidationCoordinateV1,
    HawkesValidationEra,
    HawkesValidationObservationV1,
    derive_hawkes_product_selection_dossier,
)
from histdatacom.synthetic.persistence import (
    ReconstructionDeliveryQualityManifestV1,
)
from histdatacom.synthetic.projection_burden import (
    MIDPOINT_SPREAD_DECOMPOSITION_ID,
    PRIMARY_SCALE_ID,
    ProjectionBurdenConsumerKind,
    ProjectionBurdenEventV1,
    ProjectionBurdenHawkesBindingV1,
    ProjectionBurdenPolicyV1,
    ProjectionBurdenReleaseCoverageV1,
    ProjectionBurdenReportV1,
    ProjectionBurdenScenarioV1,
    ProjectionBurdenSliceKind,
    ProjectionBurdenStatus,
    ProjectionComparisonConclusion,
    ProjectionScenarioKind,
    bind_projection_burden_to_hawkes_selection,
    build_projection_burden_consumption_receipt,
    build_projection_burden_release_coverage,
    derive_projection_burden_report,
    read_projection_burden_release_coverage,
    read_projection_burden_report,
    verify_projection_burden_release_coverage,
    write_projection_burden_release_coverage,
    write_projection_burden_report,
)


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _scenarios() -> tuple[ProjectionBurdenScenarioV1, ...]:
    return (
        ProjectionBurdenScenarioV1(
            scenario_id="baseline",
            scenario_kind=ProjectionScenarioKind.BASELINE,
            description="unmodified validation proposal law",
            intentionally_cross_series_incoherent=False,
            incoherence_strength=0.0,
            definition_content_sha256=_sha("baseline-definition"),
        ),
        ProjectionBurdenScenarioV1(
            scenario_id="misspecified-cross-series",
            scenario_kind=ProjectionScenarioKind.MISSPECIFICATION,
            description="intentionally displaced direct leg",
            intentionally_cross_series_incoherent=True,
            incoherence_strength=1.0,
            definition_content_sha256=_sha("misspecified-definition"),
        ),
    )


def _policy(**changes: Any) -> ProjectionBurdenPolicyV1:
    values: dict[str, Any] = {
        "reconciliation_config_id": "reconciliation-config:frozen",
        "alignment_policy_id": "triangle-alignment-policy:frozen",
        "advisory_mean_burden": 10.0,
        "advisory_p90_burden": 10.0,
        "advisory_p99_burden": 10.0,
        "advisory_max_burden": 10.0,
        "advisory_projected_rate": 1.0,
        "hard_mean_burden": 20.0,
        "hard_p90_burden": 20.0,
        "hard_p99_burden": 20.0,
        "hard_max_burden": 20.0,
        "hard_projected_rate": 1.0,
        "misspecification_detection_minimum_mean_burden": 0.5,
        "required_misspecification_scenario_ids": (
            "misspecified-cross-series",
        ),
    }
    values.update(changes)
    return ProjectionBurdenPolicyV1(**values)


def _quotes(*, zero_spread: bool = False) -> dict[str, tuple[float, float]]:
    if zero_spread:
        return {
            "eurgbp": (10.0, 10.0),
            "eurusd": (20.0, 20.0),
            "gbpusd": (30.0, 30.0),
        }
    return {
        "eurgbp": (10.0, 11.0),
        "eurusd": (20.0, 22.0),
        "gbpusd": (30.0, 32.0),
    }


def _event(
    index: int,
    *,
    model_id: str = DIAGONAL_HAWKES_ENGINE_ID,
    scenario_id: str = "baseline",
    movement: float = 0.5,
    coordinate_id: str = "coordinate-0",
    observed_only: bool = False,
    observed_residual: float = 0.10,
    post_residual: float = 0.0,
    refused: bool = False,
    quote_age_ns: int | None = None,
    era: str = "modern",
    session: str = "london-new-york-overlap",
    event_state: str = "ordinary",
    alignment: str = "exact",
    zero_spread: bool = False,
) -> ProjectionBurdenEventV1:
    pre = _quotes(zero_spread=zero_spread)
    post = dict(pre)
    if not observed_only and movement:
        shift = movement / 2.0
        bid, ask = pre["eurgbp"]
        post["eurgbp"] = (bid + shift, ask + shift)
    return ProjectionBurdenEventV1(
        event_id=f"event-{model_id}-{scenario_id}-{coordinate_id}-{index}",
        window_id=f"window-{coordinate_id}",
        ensemble_member_id="member-0",
        model_id=model_id,
        model_family="marked-hawkes",
        validation_coordinate_id=coordinate_id,
        event_time_ns=1_700_000_000_000_000_000 + index,
        era=era,
        session=session,
        event_state=event_state,
        alignment=alignment,
        scenario_id=scenario_id,
        quote_age_ns=index * 1_000 if quote_age_ns is None else quote_age_ns,
        pre_projection_quotes=pre,
        post_projection_quotes=post,
        pre_projection_triangle_residual=observed_residual,
        post_projection_triangle_residual=(
            observed_residual if observed_only else post_residual
        ),
        projection_priority_leg=(
            "none" if observed_only or not movement else "eurgbp"
        ),
        refused_by_hard_limit=refused,
        refusal_reason="hard-projection-limit" if refused else None,
        observed_only=observed_only,
        path_metric_pre=1.0,
        path_metric_post=1.0 if observed_only else 1.0 + movement,
        spread_metric_pre=2.0,
        spread_metric_post=2.0 if observed_only else 2.0 + movement / 2.0,
        source_content_sha256=_sha(f"source-{index}-{model_id}-{scenario_id}"),
        reconciliation_config_id="reconciliation-config:frozen",
        alignment_policy_id="triangle-alignment-policy:frozen",
    )


def _report(
    *,
    policy: ProjectionBurdenPolicyV1 | None = None,
    baseline_movement: Mapping[str, float] | None = None,
    misspecification_movement: float = 5.0,
    observed_residual: float = 10.0,
    post_residual: float = 0.0,
    refused_misspecification: bool = False,
) -> ProjectionBurdenReportV1:
    active_policy = policy or _policy()
    movement_by_model = baseline_movement or {
        DIAGONAL_HAWKES_ENGINE_ID: 0.5,
        FULL_HAWKES_ENGINE_ID: 0.51,
    }
    events: list[ProjectionBurdenEventV1] = []
    for model_index, (model_id, movement) in enumerate(
        sorted(movement_by_model.items())
    ):
        for item_index in range(2):
            events.append(
                _event(
                    model_index * 100 + item_index,
                    model_id=model_id,
                    movement=movement,
                    quote_age_ns=(item_index + 1) * 1_000_000,
                    era="early" if item_index == 0 else "modern",
                    session="london" if item_index == 0 else "overlap",
                    event_state="ordinary" if item_index == 0 else "news",
                    alignment="exact" if item_index == 0 else "bounded-prior",
                )
            )
        events.append(
            _event(
                model_index * 100 + 20,
                model_id=model_id,
                movement=0.0,
                observed_only=True,
                observed_residual=observed_residual,
            )
        )
        events.append(
            _event(
                model_index * 100 + 30,
                model_id=model_id,
                scenario_id="misspecified-cross-series",
                movement=misspecification_movement,
                post_residual=post_residual,
                refused=refused_misspecification,
            )
        )
    return derive_projection_burden_report(
        active_policy,
        _scenarios(),
        events,
        input_artifact_ids={
            "alignment_qualification": "triangle-alignment-policy:frozen",
            "proposal_lineage": "proposal-lineage:frozen",
            "reconciliation_config": "reconciliation-config:frozen",
        },
    )


def _artifact(tmp_path: Path, label: str) -> ArtifactRef:
    encoded = f"{label}\n".encode()
    digest = hashlib.sha256(encoded).hexdigest()
    path = tmp_path / f"{label}-{digest}.json"
    path.write_bytes(encoded)
    return ArtifactRef(
        kind=f"{label}_v1",
        path=str(path),
        size_bytes=len(encoded),
        sha256=digest,
    )


def _hawkes_evidence(
    tmp_path: Path,
) -> tuple[
    ProjectionBurdenReportV1,
    Any,
    HawkesValidationComparisonV1,
]:
    policy = HawkesProductSelectionPolicyV1()
    observations: list[HawkesValidationObservationV1] = []
    events: list[ProjectionBurdenEventV1] = []
    eras = tuple(HawkesValidationEra)
    for coordinate_index in range(6):
        coordinate = HawkesValidationCoordinateV1(
            window_id=f"validation-window-{coordinate_index}",
            degradation_scenario_id=f"degradation-{coordinate_index % 2}",
            seed=516 + coordinate_index,
            anchor_set_id=f"anchors-{coordinate_index}",
            adaptive_partition_id=f"partition-{coordinate_index}",
            final_constraint_set_id="constraints:frozen",
            era=eras[coordinate_index % len(eras)],
        )
        for model_index, (model_id, burden, resource_multiplier) in enumerate(
            (
                (DIAGONAL_HAWKES_ENGINE_ID, 0.10, 1.0),
                (FULL_HAWKES_ENGINE_ID, 0.102, 1.5),
            )
        ):
            metrics = dict.fromkeys(METRIC_DIRECTIONS, 1.0)
            metrics.update(
                {
                    "projection_count": 2.0,
                    "projection_burden": burden,
                    "maximum_spectral_radius": 0.50 + model_index * 0.01,
                    "stability_margin": 0.50 - model_index * 0.01,
                    "generation_failure_rate": 0.01,
                    "generation_refusal_rate": 0.01,
                    "ensemble_diversity": 0.80 + model_index * 0.01,
                }
            )
            for metric_id in (
                "runtime_seconds",
                "peak_memory_bytes",
                "poisson_work",
                "output_bytes",
                "amplification",
            ):
                metrics[metric_id] = 100.0 * resource_multiplier
            observations.append(
                HawkesValidationObservationV1(
                    engine_id=model_id,
                    coordinate=coordinate,
                    metrics=metrics,
                    projection_l1_numerator=burden * 10.0,
                    projection_spread_denominator=10.0,
                    projection_event_count=2,
                )
            )
            for event_index in range(2):
                events.append(
                    _event(
                        coordinate_index * 1000
                        + model_index * 100
                        + event_index,
                        model_id=model_id,
                        movement=burden * 5.0,
                        coordinate_id=coordinate.coordinate_id,
                        era=coordinate.era.value,
                    )
                )
    for model_index, model_id in enumerate(
        (DIAGONAL_HAWKES_ENGINE_ID, FULL_HAWKES_ENGINE_ID)
    ):
        events.append(
            _event(
                90_000 + model_index,
                model_id=model_id,
                scenario_id="misspecified-cross-series",
                movement=5.0,
            )
        )
    comparison = HawkesValidationComparisonV1(
        policy_id=policy.policy_id,
        qualification_dossier_id="powered-qualification-dossier:test",
        observations=tuple(observations),
        evidence_artifacts={"validation": _artifact(tmp_path, "validation")},
    )
    decisions = {
        model_id: SimpleNamespace(
            decision_id=f"qualification:{model_id}",
            reconstruction_eligible=True,
            residual_report_ids=(
                f"hawkes-residual-report:{model_id}",
                f"point-process-residual-report:{model_id}",
            ),
        )
        for model_id in (DIAGONAL_HAWKES_ENGINE_ID, FULL_HAWKES_ENGINE_ID)
    }
    qualification = SimpleNamespace(
        dossier_id="powered-qualification-dossier:test",
        decision=lambda model_id: decisions[model_id],
    )
    dossier = derive_hawkes_product_selection_dossier(
        policy,
        comparison,
        cast(Any, qualification),
        input_artifacts={
            "policy": _artifact(tmp_path, "policy"),
            "qualification": _artifact(tmp_path, "qualification"),
            "validation_comparison": _artifact(tmp_path, "comparison"),
        },
    )
    report = derive_projection_burden_report(
        _policy(),
        _scenarios(),
        events,
        input_artifact_ids={
            "alignment_qualification": "triangle-alignment-policy:frozen",
            "proposal_lineage": "proposal-lineage:frozen",
            "reconciliation_config": "reconciliation-config:frozen",
        },
    )
    return report, dossier, comparison


def test_primary_scale_decomposition_zero_spread_and_no_clipping() -> None:
    report = derive_projection_burden_report(
        _policy(spread_epsilon=0.5),
        _scenarios(),
        (
            _event(1, movement=3.0, zero_spread=True),
            _event(
                2,
                scenario_id="misspecified-cross-series",
                movement=30.0,
                zero_spread=True,
            ),
        ),
        input_artifact_ids={
            "alignment_qualification": "triangle-alignment-policy:frozen",
            "proposal_lineage": "proposal-lineage:frozen",
            "reconciliation_config": "reconciliation-config:frozen",
        },
    )
    item = next(
        value
        for value in report.slices
        if value.slice_kind is ProjectionBurdenSliceKind.GLOBAL_MODEL
    )

    assert (
        report.policy.to_dict()["primary_scale"]["scale_id"] == PRIMARY_SCALE_ID
    )
    assert item.projection_l1_total == pytest.approx(3.0)
    assert item.scale_total == pytest.approx(1.5)
    assert item.burdens.maximum == pytest.approx(2.0)
    assert (
        item.midpoint_movement_total + item.spread_movement_total
        == pytest.approx(item.projection_l1_total)
    )
    assert (
        item.to_dict()["midpoint_spread_decomposition_id"]
        == MIDPOINT_SPREAD_DECOMPOSITION_ID
    )


def test_report_publishes_complete_slices_and_excludes_observed_burden() -> (
    None
):
    report = _report()
    required = set(ProjectionBurdenSliceKind)

    assert {item.slice_kind for item in report.slices} == required
    assert report.release_status is ProjectionBurdenStatus.PASS
    assert all(
        item.status is ProjectionBurdenStatus.PASS
        for item in report.model_decisions
    )
    global_slice = next(
        item
        for item in report.slices
        if item.slice_kind is ProjectionBurdenSliceKind.GLOBAL_MODEL
        and item.dimensions["model_id"] == DIAGONAL_HAWKES_ENGINE_ID
    )
    assert global_slice.proposal_count == 2
    assert global_slice.dimensions["model_family"] == "marked-hawkes"
    assert global_slice.observed_only_residuals.maximum == 10.0
    assert global_slice.burdens.maximum == pytest.approx(0.1)
    assert global_slice.projected_event_count == 2
    assert global_slice.projection_priority_leg_counts == {"eurgbp": 2}
    assert global_slice.path_metric_absolute_change_mean == pytest.approx(0.5)
    assert global_slice.spread_metric_absolute_change_mean == pytest.approx(
        0.25
    )
    assert global_slice.quote_ages_ns.p90 == 2_000_000
    assert report.to_dict()["event_rows_embedded"] is False
    assert report.to_dict()["rejected_rows_retained"] is False


def test_final_residual_alone_cannot_hide_excessive_burden() -> None:
    policy = _policy(
        advisory_max_burden=0.04,
        hard_max_burden=0.05,
        advisory_mean_burden=0.04,
        hard_mean_burden=0.05,
        advisory_p90_burden=0.04,
        hard_p90_burden=0.05,
        advisory_p99_burden=0.04,
        hard_p99_burden=0.05,
    )
    report = _report(policy=policy, post_residual=0.0)

    assert report.release_status is ProjectionBurdenStatus.FAIL
    assert all(
        item.masked_by_final_residual_count == 2
        and "hard_maximum_projection_burden_exceeded" in item.hard_failure_codes
        for item in report.model_decisions
    )
    assert report.to_dict()["cross_currency_coherence_claim_permitted"] is False


def test_synthetic_post_projection_residual_is_blocking() -> None:
    report = _report(
        policy=_policy(synthetic_post_residual_tolerance=0.01),
        post_residual=0.02,
    )

    assert report.release_status is ProjectionBurdenStatus.FAIL
    assert all(
        "synthetic_post_projection_residual_blocking" in item.hard_failure_codes
        for item in report.model_decisions
    )


def test_misspecification_controls_and_hard_refusals_are_retained() -> None:
    missed = _report(misspecification_movement=0.1)
    detected = _report(
        misspecification_movement=5.0,
        refused_misspecification=True,
    )

    assert missed.release_status is ProjectionBurdenStatus.FAIL
    assert all(
        item.missed_misspecification_scenario_ids
        == ("misspecified-cross-series",)
        for item in missed.model_decisions
    )
    scenario_slices = [
        item
        for item in detected.slices
        if item.slice_kind is ProjectionBurdenSliceKind.SCENARIO
        and item.dimensions["scenario_id"] == "misspecified-cross-series"
    ]
    assert detected.release_status is ProjectionBurdenStatus.PASS
    assert all(item.hard_refusal_count == 1 for item in scenario_slices)


def test_observed_only_residual_is_immutable() -> None:
    observed = _event(1, movement=0.0, observed_only=True)
    payload = observed.to_dict()
    payload["post_projection_triangle_residual"] = 0.0
    payload["event_content_sha256"] = ""

    with pytest.raises(ValueError, match="observed-only residual changed"):
        ProjectionBurdenEventV1.from_dict(payload)


def test_comparator_relative_gate_identifies_excessive_model() -> None:
    report = _report(
        policy=_policy(maximum_comparator_burden_ratio=1.25),
        baseline_movement={
            DIAGONAL_HAWKES_ENGINE_ID: 0.5,
            FULL_HAWKES_ENGINE_ID: 2.0,
        },
    )
    comparison = report.model_comparisons[0]

    assert (
        comparison.conclusion is ProjectionComparisonConclusion.RIGHT_EXCESSIVE
    )
    assert comparison.right_to_left_ratio > 1.25
    assert "right_excessive" in report.finding_codes[0]


def test_report_roundtrip_content_address_and_tamper_refusal(
    tmp_path: Path,
) -> None:
    report = _report()
    ref = write_projection_burden_report(report, tmp_path)

    assert read_projection_burden_report(ref.path) == report
    assert ProjectionBurdenReportV1.from_json(report.to_json()) == report
    payload = report.to_dict()
    payload["final_residual_alone_sufficient"] = True
    with pytest.raises(ValueError, match="fixed semantics"):
        ProjectionBurdenReportV1.from_dict(payload)
    removed_scenario = next(
        item
        for item in report.slices
        if item.slice_kind is ProjectionBurdenSliceKind.SCENARIO
        and item.dimensions["model_id"] == FULL_HAWKES_ENGINE_ID
        and item.dimensions["scenario_id"] == "misspecified-cross-series"
    )
    with pytest.raises(ValueError, match="scenario-slice topology"):
        replace(
            report,
            slices=tuple(
                item for item in report.slices if item != removed_scenario
            ),
            report_id="",
        )
    Path(ref.path).write_text("{}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="content address"):
        read_projection_burden_report(ref.path)


def test_hawkes_selection_binds_every_exact_coordinate(tmp_path: Path) -> None:
    report, dossier, comparison = _hawkes_evidence(tmp_path)
    binding = bind_projection_burden_to_hawkes_selection(
        report, dossier, comparison
    )

    assert binding.binding_status is ProjectionBurdenStatus.PASS
    assert binding.coordinate_count == 6
    assert binding.selected_engine_id == DIAGONAL_HAWKES_ENGINE_ID
    assert (
        ProjectionBurdenHawkesBindingV1.from_dict(binding.to_dict()) == binding
    )
    altered_observations = list(comparison.observations)
    original = altered_observations[0]
    altered_observations[0] = HawkesValidationObservationV1(
        engine_id=original.engine_id,
        coordinate=original.coordinate,
        metrics={
            **original.metrics,
            "projection_burden": (original.projection_l1_numerator + 0.1)
            / original.projection_spread_denominator,
        },
        projection_l1_numerator=original.projection_l1_numerator + 0.1,
        projection_spread_denominator=original.projection_spread_denominator,
        projection_event_count=original.projection_event_count,
    )
    stale = HawkesValidationComparisonV1(
        policy_id=comparison.policy_id,
        qualification_dossier_id=comparison.qualification_dossier_id,
        observations=tuple(altered_observations),
        evidence_artifacts=comparison.evidence_artifacts,
    )
    with pytest.raises(ValueError, match="comparison is stale"):
        bind_projection_burden_to_hawkes_selection(report, dossier, stale)


def test_release_consumers_require_complete_exact_nonfailed_coverage(
    tmp_path: Path,
) -> None:
    report, dossier, comparison = _hawkes_evidence(tmp_path)
    binding = bind_projection_burden_to_hawkes_selection(
        report, dossier, comparison
    )
    model_id = binding.selected_engine_id
    global_slice = next(
        item
        for item in report.slices
        if item.slice_kind is ProjectionBurdenSliceKind.GLOBAL_MODEL
        and item.dimensions["model_id"] == model_id
    )
    era_slice = next(
        item
        for item in report.slices
        if item.slice_kind is ProjectionBurdenSliceKind.ERA
        and item.dimensions["model_id"] == model_id
    )
    receipts = tuple(
        build_projection_burden_consumption_receipt(
            report,
            consumer_kind=kind,
            consumer_id=f"consumer:{kind.value}",
            model_id=model_id,
            consumed_slice_ids=(
                (
                    era_slice.slice_id
                    if kind is ProjectionBurdenConsumerKind.ERA_AUDIT
                    else global_slice.slice_id
                ),
            ),
            hawkes_binding=(
                binding
                if kind is ProjectionBurdenConsumerKind.HAWKES_SELECTION
                else None
            ),
        )
        for kind in ProjectionBurdenConsumerKind
    )
    required_consumer_ids = {
        kind.value: (f"consumer:{kind.value}",)
        for kind in ProjectionBurdenConsumerKind
    }

    verify_projection_burden_release_coverage(
        report,
        receipts,
        required_consumer_ids=required_consumer_ids,
    )
    coverage = build_projection_burden_release_coverage(
        report,
        receipts,
        required_consumer_ids=required_consumer_ids,
    )
    coverage_ref = write_projection_burden_release_coverage(coverage, tmp_path)
    assert all(
        item.cross_currency_coherence_claim_permitted for item in receipts
    )
    assert coverage.release_coverage_valid is True
    assert coverage.excessive_projection_burden_product_count == 0
    assert coverage.synthetic_post_projection_residual_failure_count == 0
    assert coverage.final_residual_only_projection_pass_count == 0
    assert (
        ProjectionBurdenReleaseCoverageV1.from_json(coverage.to_json())
        == coverage
    )
    assert (
        read_projection_burden_release_coverage(coverage_ref.path) == coverage
    )
    with pytest.raises(ValueError, match="consumer coverage"):
        verify_projection_burden_release_coverage(
            report,
            receipts[:-1],
            required_consumer_ids=required_consumer_ids,
        )
    missing_product = {
        **required_consumer_ids,
        ProjectionBurdenConsumerKind.PRODUCT_MANIFEST.value: (
            "consumer:product_manifest",
            "product-manifest:missing",
        ),
    }
    with pytest.raises(ValueError, match="consumer coverage"):
        verify_projection_burden_release_coverage(
            report,
            receipts,
            required_consumer_ids=missing_product,
        )
    stale = replace(
        receipts[0], report_id="projection-burden-report:stale", receipt_id=""
    )
    with pytest.raises(ValueError, match="stale"):
        verify_projection_burden_release_coverage(
            report,
            (stale, *receipts[1:]),
            required_consumer_ids=required_consumer_ids,
        )


def test_policy_event_and_scenario_reject_stale_or_invalid_inputs() -> None:
    policy = _policy()
    with pytest.raises(ValueError, match="identity differs"):
        replace(policy, policy_id="projection-burden-policy:stale")
    with pytest.raises(ValueError, match="config is stale"):
        derive_projection_burden_report(
            policy,
            _scenarios(),
            (
                replace(
                    _event(1),
                    reconciliation_config_id="reconciliation-config:other",
                    event_content_sha256="",
                ),
            ),
            input_artifact_ids={
                "alignment_qualification": "triangle-alignment-policy:frozen",
                "proposal_lineage": "proposal-lineage:frozen",
                "reconciliation_config": "reconciliation-config:frozen",
            },
        )
    with pytest.raises(ValueError, match="scenario kind"):
        ProjectionBurdenScenarioV1(
            scenario_id="invalid",
            scenario_kind=ProjectionScenarioKind.BASELINE,
            description="invalid incoherent baseline",
            intentionally_cross_series_incoherent=True,
            incoherence_strength=1.0,
            definition_content_sha256=_sha("invalid"),
        )


def test_product_quality_manifest_binds_report_receipt_and_claim() -> None:
    quality = ReconstructionDeliveryQualityManifestV1(
        delivery_manifest_id="delivery:one",
        delivery_profile_id="delivery-profile:one",
        delivery_mode=ReconstructionDeliveryMode.MODERN_REFERENCE,
        delivery_output_content_sha256=_sha("delivery-output"),
        final_validation_id="validation:one",
        final_validation_status="passed",
        cross_instrument_quality_status="passed",
        cross_instrument_quality_sha256=_sha("cross-quality"),
        observed_event_count=1,
        synthetic_event_count=1,
        identity_event_count=1,
        identity_lineage_sha256=_sha("identity-lineage"),
        delivery_action_counts={"identity": 1},
        benchmark_artifact_ids=("benchmark:one",),
        projection_burden_report_ids=("projection-burden-report:one",),
        projection_burden_receipt_ids=("projection-burden-receipt:one",),
        projection_burden_status="qualified",
    )
    payload = quality.to_dict()

    assert payload["projection_burden_status"] == "qualified"
    assert payload["cross_currency_coherence_claim_permitted"] is True
    assert ReconstructionDeliveryQualityManifestV1.from_dict(payload) == quality
    payload["cross_currency_coherence_claim_permitted"] = False
    with pytest.raises(ValueError, match="derived field"):
        ReconstructionDeliveryQualityManifestV1.from_dict(payload)
    with pytest.raises(ValueError, match="coherence claim"):
        replace(
            quality,
            cross_instrument_quality_status="cross_currency_coherent",
            projection_burden_status="limited",
            quality_manifest_id="",
        )
    with pytest.raises(ValueError, match="status and evidence differ"):
        replace(
            quality,
            projection_burden_status="not_claimed",
            quality_manifest_id="",
        )


def test_campaign_shard_summary_retains_projection_receipts() -> None:
    entry = ReconstructionCampaignProductEntryV1(
        support_id="support:one",
        plan_id="plan:one",
        shard_id="shard:one",
        start_ns=1,
        end_ns=2,
        status="empty",
        reason_code="expected-market-closure",
    )
    shard = ReconstructionCampaignProductShardV1(
        plan_set_id="plan-set:one",
        support_artifact_id="support-map:one",
        plan_id="plan:one",
        shard_id="shard:one",
        requested_start_ns=1,
        requested_end_ns=2,
        entries=(entry,),
        status="complete",
        projection_burden_receipt_ids=("projection-burden-receipt:one",),
    )
    payload = shard.to_dict()

    assert payload["projection_burden_receipt_count"] == 1
    assert ReconstructionCampaignProductShardV1.from_dict(payload) == shard
    payload["projection_burden_receipt_count"] = 2
    with pytest.raises(ReconstructionPlanError, match="receipt count differs"):
        ReconstructionCampaignProductShardV1.from_dict(payload)


def test_v25_certification_requires_projection_burden_evidence() -> None:
    policy = modern_reference_triangle_certification_policy(
        common_end_period="202001",
        peak_memory_budget_bytes=1,
        scratch_budget_bytes=1,
        runtime_budget_seconds=1.0,
        storage_budget_bytes=1,
        candidate_amplification_budget=1.0,
    )
    requirements = {item.check_id: item for item in policy.requirements}

    assert {
        "projection_burden_release_coverage_valid",
        "excessive_projection_burden_product_count",
        "synthetic_post_projection_residual_failure_count",
        "final_residual_only_projection_pass_count",
    }.issubset(requirements)
    assert set(
        requirements[
            "projection_burden_release_coverage_valid"
        ].required_artifact_kinds
    ) == {
        "projection-burden-report",
        "projection-burden-consumption-receipts",
    }
