"""Validation-only diagonal-versus-full Hawkes product selection."""

from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

import histdatacom.synthetic.hawkes_selection as selection_module
from histdatacom.runtime_contracts import ArtifactRef
from histdatacom.synthetic.hawkes_selection import (
    DIAGONAL_HAWKES_ENGINE_ID,
    FULL_HAWKES_ENGINE_ID,
    METRIC_DIRECTIONS,
    HawkesComparisonConclusion,
    HawkesFinalProductResidualReportV1,
    HawkesProductSelectionDossierV1,
    HawkesProductSelectionPolicyV1,
    HawkesValidationComparisonV1,
    HawkesValidationCoordinateV1,
    HawkesValidationEra,
    HawkesValidationObservationV1,
    build_hawkes_product_selection_dossier,
    derive_hawkes_product_selection_dossier,
    read_hawkes_product_selection_dossier,
    read_hawkes_product_selection_policy,
    read_hawkes_validation_comparison,
    verify_hawkes_product_selection_dossier,
    write_hawkes_product_selection_dossier,
    write_hawkes_product_selection_policy,
    write_hawkes_validation_comparison,
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


def _metrics(
    *, full: bool, resource_multiplier: float = 1.0
) -> dict[str, float]:
    metrics = dict.fromkeys(METRIC_DIRECTIONS, 1.0)
    metrics.update(
        {
            "projection_count": 2.0,
            "projection_burden": 0.10 if not full else 0.102,
            "maximum_spectral_radius": 0.50 if not full else 0.51,
            "stability_margin": 0.50 if not full else 0.49,
            "generation_failure_rate": 0.01,
            "generation_refusal_rate": 0.01,
            "ensemble_diversity": 0.80 if not full else 0.81,
        }
    )
    for name in (
        "runtime_seconds",
        "peak_memory_bytes",
        "poisson_work",
        "output_bytes",
        "amplification",
    ):
        metrics[name] = 100.0 * resource_multiplier
    return metrics


def _observations(
    *, resource_multiplier: float = 1.5
) -> tuple[HawkesValidationObservationV1, ...]:
    observations: list[HawkesValidationObservationV1] = []
    eras = tuple(HawkesValidationEra)
    for index in range(6):
        coordinate = HawkesValidationCoordinateV1(
            window_id=f"validation-window-{index}",
            degradation_scenario_id=f"degradation-{index % 2}",
            seed=508 + index,
            anchor_set_id=f"anchors-{index}",
            adaptive_partition_id=f"partition-{index}",
            final_constraint_set_id="constraints:frozen",
            era=eras[index % len(eras)],
        )
        for engine_id, full in (
            (DIAGONAL_HAWKES_ENGINE_ID, False),
            (FULL_HAWKES_ENGINE_ID, True),
        ):
            metrics = _metrics(
                full=full,
                resource_multiplier=(resource_multiplier if full else 1.0),
            )
            burden = metrics["projection_burden"]
            observations.append(
                HawkesValidationObservationV1(
                    engine_id=engine_id,
                    coordinate=coordinate,
                    metrics=metrics,
                    projection_l1_numerator=burden * 10.0,
                    projection_spread_denominator=10.0,
                    projection_event_count=2,
                )
            )
    return tuple(observations)


def _qualification(
    *, full_eligible: bool = True, residual_complete: bool = True
) -> Any:
    decisions = {
        engine_id: SimpleNamespace(
            engine_id=engine_id,
            decision_id=f"engine-qualification-decision:{engine_id}",
            reconstruction_eligible=(
                full_eligible if engine_id == FULL_HAWKES_ENGINE_ID else True
            ),
            residual_report_ids=(
                *(
                    (f"hawkes-residual-report:{engine_id}",)
                    if residual_complete
                    else ()
                ),
                f"point-process-residual-report:{engine_id}",
            ),
        )
        for engine_id in (DIAGONAL_HAWKES_ENGINE_ID, FULL_HAWKES_ENGINE_ID)
    }
    return SimpleNamespace(
        dossier_id="powered-qualification-dossier:test",
        decision=lambda engine_id: decisions[engine_id],
    )


def _comparison(
    tmp_path: Path,
    policy: HawkesProductSelectionPolicyV1,
    *,
    observations: tuple[HawkesValidationObservationV1, ...] | None = None,
) -> HawkesValidationComparisonV1:
    return HawkesValidationComparisonV1(
        policy_id=policy.policy_id,
        qualification_dossier_id="powered-qualification-dossier:test",
        observations=observations or _observations(),
        evidence_artifacts={
            "validation-run": _artifact(tmp_path, "validation")
        },
    )


def _input_artifacts(tmp_path: Path) -> dict[str, ArtifactRef]:
    return {
        "policy": _artifact(tmp_path, "policy-input"),
        "qualification": _artifact(tmp_path, "qualification-input"),
        "validation_comparison": _artifact(tmp_path, "comparison-input"),
    }


def test_validation_only_selection_prefers_lower_resource_diagonal(
    tmp_path: Path,
) -> None:
    policy = HawkesProductSelectionPolicyV1()
    comparison = _comparison(tmp_path, policy)

    dossier = derive_hawkes_product_selection_dossier(
        policy,
        comparison,
        cast(Any, _qualification()),
        input_artifacts=_input_artifacts(tmp_path),
    )

    assert dossier.selected_engine_id == DIAGONAL_HAWKES_ENGINE_ID
    assert dossier.excluded_engine_id == FULL_HAWKES_ENGINE_ID
    assert "powered_validation_resource_rule" in dossier.selection_reason_codes
    assert "reconstruction_eligible_but_not_product_selected" in (
        dossier.exclusion_reason_codes
    )
    assert all(item.power_sufficient for item in dossier.metric_comparisons)
    assert all(
        item.conclusion is not HawkesComparisonConclusion.INCONCLUSIVE
        for item in dossier.metric_comparisons
    )
    assert {
        item.engine_id for item in dossier.final_product_residual_reports
    } == {DIAGONAL_HAWKES_ENGINE_ID, FULL_HAWKES_ENGINE_ID}
    assert all(
        item.to_dict()["diagnostic_stage"] == "final_constrained_product"
        and item.to_dict()["method"]
        == "simulation_predictive_metric_ensemble.v1"
        and item.to_dict()["event_rows_embedded"] is False
        and item.status == "available"
        for item in dossier.final_product_residual_reports
    )
    payload = dossier.to_dict()
    assert payload["validation_only"] is True
    assert payload["final_holdout_used_for_selection"] is False
    assert payload["manual_preference_used"] is False


def test_policy_comparison_and_dossier_are_content_addressed(
    tmp_path: Path,
) -> None:
    policy = HawkesProductSelectionPolicyV1()
    policy_ref = write_hawkes_product_selection_policy(policy, tmp_path)
    comparison = _comparison(tmp_path, policy)
    comparison_ref = write_hawkes_validation_comparison(comparison, tmp_path)
    dossier = derive_hawkes_product_selection_dossier(
        policy,
        comparison,
        cast(Any, _qualification()),
        input_artifacts=_input_artifacts(tmp_path),
    )
    dossier_ref = write_hawkes_product_selection_dossier(dossier, tmp_path)

    assert read_hawkes_product_selection_policy(policy_ref.path) == policy
    assert read_hawkes_validation_comparison(comparison_ref.path) == comparison
    assert read_hawkes_product_selection_dossier(dossier_ref.path) == dossier
    assert (
        HawkesProductSelectionDossierV1.from_json(dossier.to_json()) == dossier
    )
    final_payload = dossier.final_product_residual_reports[0].to_dict()
    final_payload["analytic_compensator_applied"] = True
    with pytest.raises(ValueError, match="scope or nonclaim"):
        HawkesFinalProductResidualReportV1.from_dict(final_payload)


def test_comparison_rejects_holdout_and_unpaired_or_incomplete_era_evidence(
    tmp_path: Path,
) -> None:
    policy = HawkesProductSelectionPolicyV1()
    comparison = _comparison(tmp_path, policy)
    holdout_payload = comparison.to_dict()
    holdout_payload["final_holdout_opened"] = True
    with pytest.raises(ValueError, match="final_holdout_opened"):
        HawkesValidationComparisonV1.from_dict(holdout_payload)

    with pytest.raises(ValueError, match="exactly paired"):
        HawkesValidationComparisonV1(
            policy_id=policy.policy_id,
            qualification_dossier_id=comparison.qualification_dossier_id,
            observations=comparison.observations[:-1],
            evidence_artifacts=comparison.evidence_artifacts,
        )

    modern_only = tuple(
        HawkesValidationObservationV1(
            engine_id=item.engine_id,
            coordinate=HawkesValidationCoordinateV1(
                window_id=item.coordinate.window_id,
                degradation_scenario_id=item.coordinate.degradation_scenario_id,
                seed=item.coordinate.seed,
                anchor_set_id=item.coordinate.anchor_set_id,
                adaptive_partition_id=item.coordinate.adaptive_partition_id,
                final_constraint_set_id=item.coordinate.final_constraint_set_id,
                era=HawkesValidationEra.MODERN,
            ),
            metrics=item.metrics,
            projection_l1_numerator=item.projection_l1_numerator,
            projection_spread_denominator=item.projection_spread_denominator,
            projection_event_count=item.projection_event_count,
        )
        for item in comparison.observations
    )
    with pytest.raises(ValueError, match="early/transition/modern"):
        _comparison(tmp_path, policy, observations=modern_only)


def test_selection_refuses_stale_underpowered_and_conflicting_evidence(
    tmp_path: Path,
) -> None:
    policy = HawkesProductSelectionPolicyV1()
    comparison = _comparison(tmp_path, policy)
    artifacts = _input_artifacts(tmp_path)
    stale = HawkesValidationComparisonV1(
        policy_id="hawkes-product-selection-policy:stale",
        qualification_dossier_id=comparison.qualification_dossier_id,
        observations=comparison.observations,
        evidence_artifacts=comparison.evidence_artifacts,
    )
    with pytest.raises(ValueError, match="policy is stale"):
        derive_hawkes_product_selection_dossier(
            policy,
            stale,
            cast(Any, _qualification()),
            input_artifacts=artifacts,
        )

    underpowered_policy = HawkesProductSelectionPolicyV1(minimum_paired_cells=7)
    underpowered = HawkesValidationComparisonV1(
        policy_id=underpowered_policy.policy_id,
        qualification_dossier_id=comparison.qualification_dossier_id,
        observations=comparison.observations,
        evidence_artifacts=comparison.evidence_artifacts,
    )
    with pytest.raises(ValueError, match="paired-cell minimum"):
        derive_hawkes_product_selection_dossier(
            underpowered_policy,
            underpowered,
            cast(Any, _qualification()),
            input_artifacts=artifacts,
        )

    with pytest.raises(ValueError, match="both Hawkes candidates"):
        derive_hawkes_product_selection_dossier(
            policy,
            comparison,
            cast(Any, _qualification(full_eligible=False)),
            input_artifacts=artifacts,
        )

    with pytest.raises(ValueError, match="raw and benchmark residual reports"):
        derive_hawkes_product_selection_dossier(
            policy,
            comparison,
            cast(Any, _qualification(residual_complete=False)),
            input_artifacts=artifacts,
        )

    unstable_observations: list[HawkesValidationObservationV1] = []
    for item in comparison.observations:
        metrics = dict(item.metrics)
        if item.engine_id == FULL_HAWKES_ENGINE_ID:
            metrics["maximum_spectral_radius"] = 0.99
        unstable_observations.append(
            HawkesValidationObservationV1(
                engine_id=item.engine_id,
                coordinate=item.coordinate,
                metrics=metrics,
                projection_l1_numerator=item.projection_l1_numerator,
                projection_spread_denominator=item.projection_spread_denominator,
                projection_event_count=item.projection_event_count,
            )
        )
    unstable = _comparison(
        tmp_path, policy, observations=tuple(unstable_observations)
    )
    with pytest.raises(ValueError, match="spectral-radius gate"):
        derive_hawkes_product_selection_dossier(
            policy,
            unstable,
            cast(Any, _qualification()),
            input_artifacts=artifacts,
        )

    conflicting_observations: list[HawkesValidationObservationV1] = []
    for item in comparison.observations:
        metrics = dict(item.metrics)
        if item.engine_id == DIAGONAL_HAWKES_ENGINE_ID:
            metrics["raw_triangle_residual"] = 1.0
            metrics["projection_burden"] = 0.50
        else:
            metrics["raw_triangle_residual"] = 2.0
            metrics["projection_burden"] = 0.10
        conflicting_observations.append(
            HawkesValidationObservationV1(
                engine_id=item.engine_id,
                coordinate=item.coordinate,
                metrics=metrics,
                projection_l1_numerator=metrics["projection_burden"] * 10.0,
                projection_spread_denominator=10.0,
                projection_event_count=item.projection_event_count,
            )
        )
    conflict = _comparison(
        tmp_path, policy, observations=tuple(conflicting_observations)
    )
    with pytest.raises(ValueError, match="primary.*Pareto tradeoff"):
        derive_hawkes_product_selection_dossier(
            policy,
            conflict,
            cast(Any, _qualification()),
            input_artifacts=artifacts,
        )


def test_dossier_verification_refuses_stale_implementation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    policy = HawkesProductSelectionPolicyV1()
    dossier = derive_hawkes_product_selection_dossier(
        policy,
        _comparison(tmp_path, policy),
        cast(Any, _qualification()),
        input_artifacts=_input_artifacts(tmp_path),
    )
    monkeypatch.setattr(
        selection_module, "_implementation_sha256", lambda: "0" * 64
    )

    with pytest.raises(ValueError, match="implementation identity is stale"):
        verify_hawkes_product_selection_dossier(dossier)


def test_public_builder_replays_and_publishes_exact_inputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    policy = HawkesProductSelectionPolicyV1()
    comparison = _comparison(tmp_path, policy)
    policy_ref = write_hawkes_product_selection_policy(policy, tmp_path)
    comparison_ref = write_hawkes_validation_comparison(comparison, tmp_path)
    qualification_path = tmp_path / "qualification.json"
    qualification_path.write_text("{}\n", encoding="utf-8")
    qualification = _qualification()
    monkeypatch.setattr(
        selection_module,
        "read_powered_qualification_dossier",
        lambda _: qualification,
    )
    monkeypatch.setattr(
        selection_module,
        "verify_powered_qualification_dossier",
        lambda _: None,
    )

    dossier = build_hawkes_product_selection_dossier(
        policy_ref.path,
        comparison_ref.path,
        qualification_path,
        output_directory=tmp_path / "selection",
    )

    assert dossier.selected_engine_id == DIAGONAL_HAWKES_ENGINE_ID
    published = tuple(
        (tmp_path / "selection").glob("hawkes-product-selection-dossier-*.json")
    )
    assert len(published) == 1
    assert read_hawkes_product_selection_dossier(published[0]) == dossier
