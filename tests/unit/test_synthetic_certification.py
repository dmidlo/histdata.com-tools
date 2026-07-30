"""Regression tests for release-grade reconstruction certification."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest

from histdatacom.synthetic import (
    CERTIFICATION_ARTIFACT_SCHEMA_VERSION,
    EURUSD_TRIANGLE_COMMON_START_PERIOD,
    PROMOTION_ONLY_CHECK_IDS,
    CertificationArtifactV1,
    CertificationCheckStatus,
    CertificationComparator,
    CertificationGate,
    CertificationObservationV1,
    CertificationRequirementV1,
    CertificationState,
    ReconstructionCertificationDossierV1,
    ReconstructionCertificationPolicyV1,
    eurusd_triangle_certification_policy,
    evaluate_reconstruction_certification,
    load_reconstruction_certification_dossier,
    write_reconstruction_certification_dossier,
)
from histdatacom.synthetic.certification import EURUSD_TRIANGLE_SYMBOLS

BROKER_ID = "broker-delivery-fingerprint:sha256:qualified-fixture"
METHODOLOGY = (
    "The fixture binds immutable source identities, final holdout scorecards, "
    "post-render cross-currency validation, restart/replay evidence, resource "
    "measurements, negative tests, and downstream strategy sensitivity to one "
    "predeclared policy. Event rows remain outside the dossier."
)


def _policy() -> ReconstructionCertificationPolicyV1:
    return eurusd_triangle_certification_policy(
        broker_fingerprint_id=BROKER_ID,
        common_end_period="202606",
        peak_memory_budget_bytes=4_000_000_000,
        scratch_budget_bytes=80_000_000_000,
        runtime_budget_seconds=86_400.0,
        storage_budget_bytes=80_000_000_000,
    )


def _artifact(
    kind: str,
    *,
    policy: ReconstructionCertificationPolicyV1 | None = None,
    verified: bool = True,
) -> CertificationArtifactV1:
    selected_policy = policy or _policy()
    subject_id = (
        BROKER_ID
        if kind == "broker-delivery-fingerprint"
        else f"{kind}:fixture"
    )
    return CertificationArtifactV1.from_payload(
        policy_id=selected_policy.policy_id,
        kind=kind,
        subject_id=subject_id,
        subject_schema_version=f"histdatacom.{kind}.v1",
        payload={
            "kind": kind,
            "subject_id": subject_id,
            "fixture": True,
        },
        relative_path=f"certification/{kind}.json",
        verified=verified,
        metadata={"event_rows_inline": False},
    )


def _artifacts(
    policy: ReconstructionCertificationPolicyV1,
) -> tuple[CertificationArtifactV1, ...]:
    kinds = sorted(
        {
            kind
            for requirement in policy.requirements
            for kind in requirement.required_artifact_kinds
        }
    )
    return tuple(_artifact(kind, policy=policy) for kind in kinds)


def _passing_value(
    requirement: CertificationRequirementV1,
) -> bool | int | float | str | None:
    if requirement.comparator is CertificationComparator.LESS_OR_EQUAL:
        assert isinstance(requirement.expected, (int, float))
        return cast(int | float, requirement.expected) / 2
    if requirement.comparator is CertificationComparator.GREATER_OR_EQUAL:
        return requirement.expected
    return cast(bool | int | float | str | None, requirement.expected)


def _observations(
    policy: ReconstructionCertificationPolicyV1,
    artifacts: tuple[CertificationArtifactV1, ...],
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
            note="measured by deterministic certification fixture",
        )
        for requirement in policy.requirements
    )


def _dossier(
    *,
    omit_checks: frozenset[str] = frozenset(),
    actual_overrides: dict[str, bool | int | float | str | None] | None = None,
    artifacts: tuple[CertificationArtifactV1, ...] | None = None,
    blocking_limitations: tuple[str, ...] = (),
) -> ReconstructionCertificationDossierV1:
    policy = _policy()
    selected_artifacts = artifacts or _artifacts(policy)
    observations = []
    overrides = actual_overrides or {}
    for item in _observations(policy, selected_artifacts):
        if item.check_id in omit_checks:
            continue
        observations.append(
            replace(
                item,
                actual=overrides.get(item.check_id, item.actual),
                observation_id="",
            )
        )
    return evaluate_reconstruction_certification(
        policy,
        artifacts=selected_artifacts,
        observations=observations,
        methodology=METHODOLOGY,
        accepted_limitations=(
            "The deterministic fixture demonstrates contract semantics, not historical truth.",
        ),
        blocking_limitations=blocking_limitations,
    )


def test_default_policy_predeclares_every_issue_gate_and_round_trips() -> None:
    """The policy is complete, deterministic, and tied to the selected broker."""
    policy = _policy()

    assert set(item.gate for item in policy.requirements) == set(
        CertificationGate
    )
    assert policy.symbols == tuple(sorted(EURUSD_TRIANGLE_SYMBOLS))
    assert policy.common_start_period == EURUSD_TRIANGLE_COMMON_START_PERIOD
    assert policy.broker_fingerprint_id == BROKER_ID
    assert len({item.check_id for item in policy.requirements}) == len(
        policy.requirements
    )
    assert (
        ReconstructionCertificationPolicyV1.from_dict(policy.to_dict())
        == policy
    )


def test_complete_content_bound_evidence_certifies_without_product_claims() -> (
    None
):
    """All measured gates certify while retaining only bounded metadata."""
    dossier = _dossier()
    payload = dossier.to_dict()

    assert dossier.state is CertificationState.CERTIFIED
    assert dossier.certified
    assert payload["release_authorized"] is True
    assert payload["event_rows_inline"] is False
    assert payload["analytical_frame_columns_inline"] is False
    assert payload["automatic_winner"] is False
    assert payload["historical_truth_claim"] is False
    assert payload["investment_recommendation"] is False
    assert dossier.summary["passed_gate_count"] == 15
    assert (
        ReconstructionCertificationDossierV1.from_json(dossier.to_json())
        == dossier
    )


def test_only_promotion_coverage_missing_is_ready_for_promotion() -> None:
    """Coverage stays deferred until dev-to-main without hiding other gates."""
    dossier = _dossier(omit_checks=PROMOTION_ONLY_CHECK_IDS)

    assert dossier.state is CertificationState.READY_FOR_PROMOTION
    assert dossier.ready_for_promotion
    assert not dossier.certified
    missing = {
        result.check_id
        for gate in dossier.gate_results
        for result in gate.check_results
        if result.status is CertificationCheckStatus.MISSING
    }
    assert missing == PROMOTION_ONLY_CHECK_IDS


def test_missing_broker_artifact_remains_incomplete() -> None:
    """A caller cannot replace a qualified broker fingerprint with a boolean."""
    policy = _policy()
    artifacts = tuple(
        item
        for item in _artifacts(policy)
        if item.kind != "broker-delivery-fingerprint"
    )
    by_kind = {item.kind: item for item in artifacts}
    observations = []
    for requirement in policy.requirements:
        available = tuple(
            by_kind[kind].evidence_id
            for kind in requirement.required_artifact_kinds
            if kind in by_kind
        )
        if not available:
            available = (artifacts[0].evidence_id,)
        observations.append(
            CertificationObservationV1(
                check_id=requirement.check_id,
                actual=_passing_value(requirement),
                artifact_evidence_ids=available,
            )
        )

    dossier = evaluate_reconstruction_certification(
        policy,
        artifacts=artifacts,
        observations=observations,
        methodology=METHODOLOGY,
        blocking_limitations=(
            "No qualified live broker fingerprint is available.",
        ),
    )

    assert dossier.state is CertificationState.INCOMPLETE
    cross = next(
        item
        for item in dossier.gate_results
        if item.gate is CertificationGate.CROSS_CURRENCY
    )
    assert cross.status is CertificationCheckStatus.MISSING


def test_wrong_broker_identity_fails_certification() -> None:
    """Verified content from another broker profile cannot satisfy the policy."""
    policy = _policy()
    artifacts = tuple(
        (
            replace(
                item,
                subject_id="broker-delivery-fingerprint:sha256:other",
                evidence_id="",
            )
            if item.kind == "broker-delivery-fingerprint"
            else item
        )
        for item in _artifacts(policy)
    )
    dossier = _dossier(artifacts=artifacts)

    assert dossier.state is CertificationState.FAILED
    cross = next(
        item
        for item in dossier.gate_results
        if item.gate is CertificationGate.CROSS_CURRENCY
    )
    assert cross.status is CertificationCheckStatus.FAILED
    assert "fingerprint differs" in cross.check_results[0].reason


@pytest.mark.parametrize(  # type: ignore[untyped-decorator]
    "check_id,actual",
    (
        ("raw_source_hash_mismatch_count", 1),
        ("reverse_holdout_failure_count", 1),
        ("actual_peak_memory_bytes", 4_000_000_001),
        ("corruption_refused", False),
        ("strategy_automatic_winner", True),
        ("local_simple_registry_preflight_passed", False),
    ),
)
def test_measured_failures_fail_closed(
    check_id: str, actual: bool | int
) -> None:
    """Scientific, operational, resource, and release failures are terminal."""
    dossier = _dossier(actual_overrides={check_id: actual})

    assert dossier.state is CertificationState.FAILED
    result = next(
        result
        for gate in dossier.gate_results
        for result in gate.check_results
        if result.check_id == check_id
    )
    assert result.status is CertificationCheckStatus.FAILED


def test_unverified_artifact_cannot_support_a_pass() -> None:
    """Content identity without verification is missing evidence, not a pass."""
    policy = _policy()
    artifacts = tuple(
        (
            _artifact(item.kind, policy=policy, verified=False)
            if item.kind == "information-audit-report"
            else item
        )
        for item in _artifacts(policy)
    )
    dossier = _dossier(artifacts=artifacts)

    assert dossier.state is CertificationState.INCOMPLETE
    information = next(
        item
        for item in dossier.gate_results
        if item.gate is CertificationGate.INFORMATION_SAFETY
    )
    assert information.status is CertificationCheckStatus.MISSING


def test_blocking_limitation_prevents_certification() -> None:
    """A contradictory limitation cannot be accepted away."""
    dossier = _dossier(
        blocking_limitations=(
            "Final historical reconstruction has not been executed.",
        )
    )

    assert dossier.state is CertificationState.INCOMPLETE
    assert dossier.summary["blocking_limitation_count"] == 1
    assert not dossier.to_dict()["release_authorized"]


def test_measured_failure_outranks_a_blocking_limitation() -> None:
    """Known threshold violations remain failures even when work is missing."""
    dossier = _dossier(
        actual_overrides={"reverse_holdout_failure_count": 1},
        blocking_limitations=("A separate required artifact is unavailable.",),
    )

    assert dossier.state is CertificationState.FAILED


def test_artifacts_are_bound_to_the_predeclared_policy() -> None:
    """Evidence produced under different thresholds cannot be repurposed."""
    policy = _policy()
    other_policy = eurusd_triangle_certification_policy(
        broker_fingerprint_id=BROKER_ID,
        common_end_period="202606",
        peak_memory_budget_bytes=4_000_000_001,
        scratch_budget_bytes=80_000_000_000,
        runtime_budget_seconds=86_400.0,
        storage_budget_bytes=80_000_000_000,
    )
    artifacts = _artifacts(other_policy)

    with pytest.raises(ValueError, match="differ from policy"):
        evaluate_reconstruction_certification(
            policy,
            artifacts=artifacts,
            observations=_observations(policy, artifacts),
            methodology=METHODOLOGY,
        )


def test_dossier_publication_is_atomic_replayable_and_human_readable(
    tmp_path: Path,
) -> None:
    """Machine and human reports publish with strong content references."""
    dossier = _dossier()
    json_path = tmp_path / "evidence" / "certification.json"
    markdown_path = tmp_path / "evidence" / "certification.md"

    json_ref, markdown_ref = write_reconstruction_certification_dossier(
        dossier,
        json_path=json_path,
        markdown_path=markdown_path,
    )

    assert load_reconstruction_certification_dossier(json_path) == dossier
    assert json_ref.sha256
    assert markdown_ref.sha256
    assert json_ref.size_bytes == json_path.stat().st_size
    markdown = markdown_path.read_text(encoding="utf-8")
    assert "## Gate results" in markdown
    assert "## Methodology" in markdown
    assert "## Accepted limitations" in markdown
    assert "## Blocking limitations" in markdown
    assert "event rows" in markdown.lower()


def test_tampering_and_scope_drift_are_rejected() -> None:
    """Deterministic identities and the three-symbol coverage scope fail closed."""
    dossier = _dossier()
    payload = dossier.to_dict()
    payload["dossier_id"] = (
        "reconstruction-certification-dossier:sha256:tampered"
    )
    with pytest.raises(ValueError, match="dossier_id differs"):
        ReconstructionCertificationDossierV1.from_dict(payload)

    policy = _policy()
    with pytest.raises(ValueError, match="EURUSD triangle"):
        replace(policy, symbols=("EURUSD",), policy_id="")
    with pytest.raises(ValueError, match="common coverage"):
        replace(policy, common_start_period="200005", policy_id="")


def test_unknown_or_duplicate_observations_are_rejected() -> None:
    """The evaluator consumes exactly the checks declared by policy."""
    policy = _policy()
    artifacts = _artifacts(policy)
    observations = _observations(policy, artifacts)
    unknown = CertificationObservationV1(
        check_id="undeclared-check",
        actual=True,
        artifact_evidence_ids=(artifacts[0].evidence_id,),
    )
    with pytest.raises(ValueError, match="outside policy"):
        evaluate_reconstruction_certification(
            policy,
            artifacts=artifacts,
            observations=(*observations, unknown),
            methodology=METHODOLOGY,
        )
    with pytest.raises(ValueError, match="duplicate check"):
        evaluate_reconstruction_certification(
            policy,
            artifacts=artifacts,
            observations=(*observations, observations[0]),
            methodology=METHODOLOGY,
        )


def test_requirement_and_artifact_contract_bounds() -> None:
    """Paths, hashes, scalar comparisons, and policy coverage remain bounded."""
    with pytest.raises(ValueError, match="relative and safe"):
        CertificationArtifactV1(
            policy_id=_policy().policy_id,
            kind="report",
            subject_id="report:one",
            subject_schema_version="report.v1",
            content_sha256="a" * 64,
            relative_path="../secret.json",
            size_bytes=1,
            verified=True,
            metadata={},
        )
    with pytest.raises(ValueError, match="SHA-256"):
        replace(_artifact("report"), content_sha256="bad", evidence_id="")
    with pytest.raises(ValueError, match="true comparator"):
        CertificationRequirementV1(
            gate=CertificationGate.REPOSITORY_GATES,
            check_id="invalid-true",
            comparator=CertificationComparator.TRUE,
            expected=False,
            required_artifact_kinds=("report",),
            description="invalid comparator fixture",
        )

    artifact = _artifact("report")
    assert artifact.schema_version == CERTIFICATION_ARTIFACT_SCHEMA_VERSION
    assert CertificationArtifactV1.from_dict(artifact.to_dict()) == artifact
