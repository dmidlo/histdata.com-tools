"""Modern-reference reconstruction certification regression tests."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest

from histdatacom.synthetic import (
    MODERN_REFERENCE_DELIVERY_CLAIM,
    MODERN_REFERENCE_DELIVERY_MODE,
    PROMOTION_ONLY_CHECK_IDS,
    CertificationArtifactV1,
    CertificationCheckStatus,
    CertificationComparator,
    CertificationObservationV1,
    CertificationRequirementV1,
    CertificationState,
    ReconstructionCertificationDossierV2,
    ReconstructionCertificationPolicyV2,
    evaluate_modern_reference_reconstruction_certification,
    load_modern_reference_reconstruction_certification_dossier,
    modern_reference_triangle_certification_policy,
    write_modern_reference_reconstruction_certification_dossier,
)

METHODOLOGY = (
    "The fixture binds independently produced readiness, scientific, product, "
    "operational, repository, and publication reports to a frozen modern-reference "
    "policy. It exercises contract semantics only and contains no event rows."
)


def _policy() -> ReconstructionCertificationPolicyV2:
    return modern_reference_triangle_certification_policy(
        common_end_period="202606",
        peak_memory_budget_bytes=4_000_000_000,
        scratch_budget_bytes=80_000_000_000,
        runtime_budget_seconds=86_400.0,
        storage_budget_bytes=80_000_000_000,
        candidate_amplification_budget=10.0,
    )


def _passing_value(
    requirement: CertificationRequirementV1,
) -> bool | int | float | str | None:
    if requirement.comparator is CertificationComparator.LESS_OR_EQUAL:
        assert isinstance(requirement.expected, (int, float))
        return cast(int | float, requirement.expected) / 2
    if requirement.comparator is CertificationComparator.GREATER_OR_EQUAL:
        return requirement.expected
    return cast(bool | int | float | str | None, requirement.expected)


def _artifacts(
    policy: ReconstructionCertificationPolicyV2,
) -> tuple[CertificationArtifactV1, ...]:
    kinds = sorted(
        {
            kind
            for requirement in policy.requirements
            for kind in requirement.required_artifact_kinds
        }
    )
    return tuple(
        CertificationArtifactV1.from_payload(
            policy_id=policy.policy_id,
            kind=kind,
            subject_id=f"{kind}:fixture",
            subject_schema_version=f"histdatacom.{kind}.v1",
            payload={
                "schema_version": f"histdatacom.{kind}.v1",
                "subject_id": f"{kind}:fixture",
                "fixture": True,
            },
            relative_path=f"certification/{kind}.json",
            metadata={"event_rows_inline": False},
        )
        for kind in kinds
    )


def _observations(
    policy: ReconstructionCertificationPolicyV2,
    artifacts: tuple[CertificationArtifactV1, ...],
    *,
    omit: frozenset[str] = frozenset(),
) -> tuple[CertificationObservationV1, ...]:
    by_kind = {item.kind: item for item in artifacts}
    return tuple(
        CertificationObservationV1(
            check_id=requirement.check_id,
            actual=_passing_value(requirement),
            artifact_evidence_ids=tuple(
                by_kind[kind].evidence_id
                for kind in requirement.required_artifact_kinds
            ),
            note="deterministic contract fixture",
        )
        for requirement in policy.requirements
        if requirement.check_id not in omit
    )


def _dossier(
    *, omit: frozenset[str] = frozenset()
) -> ReconstructionCertificationDossierV2:
    policy = _policy()
    artifacts = _artifacts(policy)
    return evaluate_modern_reference_reconstruction_certification(
        policy,
        artifacts=artifacts,
        observations=_observations(policy, artifacts, omit=omit),
        methodology=METHODOLOGY,
        accepted_limitations=(
            "This fixture proves certification mechanics, not product evidence.",
        ),
    )


def test_policy_covers_all_live_issue_seams_without_broker_evidence() -> None:
    """V2 binds the complete #491 scope and leaves old identities readable."""
    policy = _policy()
    checks = {item.check_id for item in policy.requirements}
    kinds = {
        kind
        for requirement in policy.requirements
        for kind in requirement.required_artifact_kinds
    }

    assert policy.delivery_mode == MODERN_REFERENCE_DELIVERY_MODE
    assert policy.delivery_claim == MODERN_REFERENCE_DELIVERY_CLAIM
    assert policy.product_version == "2.5.0"
    assert all("broker" not in value for value in checks | kinds)
    assert {
        "source_inventory_reconciled",
        "support_map_gap_or_overlap_count",
        "valid_common_data_refusal_count",
        "unclassified_terminal_outcome_count",
        "scientific_ledger_valid",
        "math_verification_report_valid",
        "scientific_lineage_binding_valid",
        "conditioning_input_missing_state_count",
        "generated_origin_misclassification_count",
        "ex_post_invalid_for_backtest_missing_count",
        "market_context_corpus_valid",
        "cftc_positioning_corpus_valid",
        "feed_epoch_artifact_valid",
        "observation_operator_valid",
        "benchmark_corpus_valid",
        "qualified_portfolio_artifact_valid",
        "information_modes_audited_separately",
        "diagnostic_publication_valid",
        "representative_window_class_missing_count",
        "substantial_multi_period_run_passed",
        "full_campaign_execution_passed",
        "executable_retained_product_missing_count",
        "fabricated_liquidity_terminal_outcome_count",
        "campaign_product_index_valid",
        "campaign_dataset_publication_valid",
        "storage_disconnect_resume_passed",
        "mounted_storage_integrity_passed",
        "cancellation_publishable_partial_count",
        "invalid_information_mode_refused",
        "quota_overflow_refused",
        "public_cli_api_evidence_chain_passed",
        "declared_test_dependencies_installed",
    }.issubset(checks)
    assert "motif-qualification" not in kinds
    assert "powered-qualification-dossier" in kinds
    assert "hawkes-product-selection-dossier" in kinds
    assert "diagnostic-publication-manifest" in kinds
    assert "reconstruction-plan-support-map" in kinds
    assert "reconstruction-scientific-ledger" in kinds
    assert "reconstruction-math-verification-report" in kinds
    assert "reconstruction-campaign-product-index" in kinds
    assert "reconstruction-campaign-dataset-publication" in kinds
    assert "storage-qualification-report" in kinds
    assert (
        ReconstructionCertificationPolicyV2.from_dict(policy.to_dict())
        == policy
    )


def test_embedded_v2_1_policy_remains_replayable() -> None:
    """The derived broker boundary follows the embedded product version."""
    current = _policy()
    legacy = replace(current, product_version="2.1.0", policy_id="")

    assert legacy.to_dict()["broker_adaptation"] == (
        "excluded-from-v2.1.0-certification"
    )
    assert ReconstructionCertificationPolicyV2.from_dict(legacy.to_dict()) == (
        legacy
    )


def test_complete_v2_fixture_certifies_and_round_trips() -> None:
    """Complete bounded V2 evidence produces a broker-neutral dossier."""
    dossier = _dossier()
    payload = dossier.to_dict()

    assert dossier.state is CertificationState.CERTIFIED
    assert dossier.summary["passed_gate_count"] == 15
    assert payload["delivery_mode"] == MODERN_REFERENCE_DELIVERY_MODE
    assert payload["delivery_claim"] == MODERN_REFERENCE_DELIVERY_CLAIM
    assert payload["broker_specific_claim"] is False
    assert (
        ReconstructionCertificationDossierV2.from_json(dossier.to_json())
        == dossier
    )


def test_only_promotion_coverage_missing_is_ready_for_promotion() -> None:
    """The modern-reference policy retains the exactly-once coverage boundary."""
    dossier = _dossier(omit=PROMOTION_ONLY_CHECK_IDS)

    assert dossier.state is CertificationState.READY_FOR_PROMOTION
    assert {
        result.check_id
        for gate in dossier.gate_results
        for result in gate.check_results
        if result.status is CertificationCheckStatus.MISSING
    } == PROMOTION_ONLY_CHECK_IDS


def test_v2_rejects_broker_artifacts_and_nonexact_evidence_bindings() -> None:
    """Broker evidence and unrelated extra artifacts cannot satisfy V2 checks."""
    policy = _policy()
    artifacts = _artifacts(policy)
    broker = CertificationArtifactV1.from_payload(
        policy_id=policy.policy_id,
        kind="broker-delivery-fingerprint",
        subject_id="broker:fixture",
        subject_schema_version="histdatacom.broker.v1",
        payload={"schema_version": "histdatacom.broker.v1"},
        relative_path="certification/broker.json",
    )
    with pytest.raises(ValueError, match="broker-specific evidence"):
        evaluate_modern_reference_reconstruction_certification(
            policy,
            artifacts=(*artifacts, broker),
            observations=_observations(policy, artifacts),
            methodology=METHODOLOGY,
        )

    first = _observations(policy, artifacts)[0]
    altered = replace(
        first,
        artifact_evidence_ids=(
            *first.artifact_evidence_ids,
            artifacts[-1].evidence_id,
        ),
        observation_id="",
    )
    observations = tuple(
        altered if item.check_id == first.check_id else item
        for item in _observations(policy, artifacts)
    )
    dossier = evaluate_modern_reference_reconstruction_certification(
        policy,
        artifacts=artifacts,
        observations=observations,
        methodology=METHODOLOGY,
    )
    result = next(
        item
        for gate in dossier.gate_results
        for item in gate.check_results
        if item.check_id == first.check_id
    )
    assert result.status is CertificationCheckStatus.MISSING
    assert "evidence kinds differ" in result.reason


def test_v2_publication_is_atomic_replayable_and_explicitly_unconditioned(
    tmp_path: Path,
) -> None:
    """The published report names its delivery boundary without broker claims."""
    dossier = _dossier()
    json_path = tmp_path / "dossier.json"
    markdown_path = tmp_path / "dossier.md"

    refs = write_modern_reference_reconstruction_certification_dossier(
        dossier, json_path=json_path, markdown_path=markdown_path
    )

    assert (
        load_modern_reference_reconstruction_certification_dossier(json_path)
        == dossier
    )
    assert all(ref.sha256 and ref.size_bytes for ref in refs)
    markdown = markdown_path.read_text(encoding="utf-8")
    assert "modern_reference" in markdown
    assert "unconditioned_reference" in markdown
    assert "Broker-specific claim: `false`" in markdown
