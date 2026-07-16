"""Executable modern-reference certification campaign tests."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import cast

import pytest

from histdatacom.runtime_contracts import JSONScalar, JSONValue
from histdatacom.synthetic import CertificationComparator, CertificationState
from histdatacom.synthetic.certification import (
    CertificationRequirementV1,
    ReconstructionCertificationPolicyV2,
    modern_reference_triangle_certification_policy,
)
from histdatacom.synthetic.certification_campaign import (
    METHODOLOGY_REPORT_EVIDENCE_KEY,
    CertificationCampaignArtifactV1,
    CertificationCampaignObservationV1,
    ModernReferenceCertificationCampaignSpecV1,
    read_modern_reference_certification_campaign_spec,
    run_modern_reference_certification_campaign,
)
from histdatacom.synthetic.contracts import canonical_contract_json


def _policy() -> ReconstructionCertificationPolicyV2:
    return modern_reference_triangle_certification_policy(
        common_end_period="202606",
        peak_memory_budget_bytes=4_000_000_000,
        scratch_budget_bytes=80_000_000_000,
        runtime_budget_seconds=86_400.0,
        storage_budget_bytes=80_000_000_000,
        candidate_amplification_budget=10.0,
    )


def _passing_value(requirement: CertificationRequirementV1) -> JSONScalar:
    if requirement.comparator is CertificationComparator.LESS_OR_EQUAL:
        return cast(float, requirement.expected) / 2
    if requirement.comparator is CertificationComparator.GREATER_OR_EQUAL:
        return requirement.expected
    return requirement.expected


def _write_json(path: Path, payload: dict[str, JSONValue]) -> str:
    encoded = canonical_contract_json(payload).encode("utf-8") + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(encoded)
    return hashlib.sha256(encoded).hexdigest()


def _complete_spec(
    tmp_path: Path,
) -> ModernReferenceCertificationCampaignSpecV1:
    policy = _policy()
    requirements = {
        item.check_id: item
        for item in policy.requirements
        if item.check_id
        not in {
            "coverage_promotion_run_count",
            "methodology_and_limitations_published",
            "machine_evidence_manifest_published",
        }
    }
    checks_by_measurement_kind: dict[str, dict[str, JSONScalar]] = {}
    for requirement in requirements.values():
        kind = next(
            item
            for item in requirement.required_artifact_kinds
            if item != "methodology-report"
        )
        checks_by_measurement_kind.setdefault(kind, {})[
            requirement.check_id
        ] = _passing_value(requirement)
    kinds = sorted(
        {
            kind
            for requirement in requirements.values()
            for kind in requirement.required_artifact_kinds
            if kind != "methodology-report"
        }
    )
    artifacts = []
    for kind in kinds:
        key = kind.replace("-", "_")
        schema = f"histdatacom.{kind}.test.v1"
        report_id = f"{kind}:fixture"
        payload: dict[str, JSONValue] = {
            "schema_version": schema,
            "report_id": report_id,
            "measurements": checks_by_measurement_kind.get(kind, {}),
            "event_rows_inline": False,
        }
        path = tmp_path / "reports" / f"{key}.json"
        digest = _write_json(path, payload)
        artifacts.append(
            CertificationCampaignArtifactV1(
                evidence_key=key,
                kind=kind,
                path=str(path.relative_to(tmp_path)),
                content_sha256=digest,
                subject_id=report_id,
                subject_id_pointer="/report_id",
                subject_schema_version=schema,
                relative_path=f"reports/{key}.json",
                metadata={"event_rows_inline": False},
            )
        )
    keys_by_kind = {item.kind: item.evidence_key for item in artifacts}
    observations = []
    for requirement in requirements.values():
        measurement_kind = next(
            item
            for item in requirement.required_artifact_kinds
            if item != "methodology-report"
        )
        evidence_keys = tuple(
            (
                METHODOLOGY_REPORT_EVIDENCE_KEY
                if kind == "methodology-report"
                else keys_by_kind[kind]
            )
            for kind in requirement.required_artifact_kinds
        )
        observations.append(
            CertificationCampaignObservationV1(
                check_id=requirement.check_id,
                measurement_evidence_key=keys_by_kind[measurement_kind],
                measurement_pointer=f"/measurements/{requirement.check_id}",
                artifact_evidence_keys=evidence_keys,
                note="measured by a content-bound campaign fixture",
            )
        )
    return ModernReferenceCertificationCampaignSpecV1(
        common_end_period="202606",
        peak_memory_budget_bytes=4_000_000_000,
        scratch_budget_bytes=80_000_000_000,
        runtime_budget_seconds=86_400.0,
        storage_budget_bytes=80_000_000_000,
        candidate_amplification_budget=10.0,
        artifacts=tuple(artifacts),
        observations=tuple(observations),
        methodology=(
            "All observations are extracted from hash-verified JSON reports; "
            "the fixture proves campaign mechanics rather than product quality."
        ),
        accepted_limitations=("Fixture evidence is not release evidence.",),
        blocking_limitations=(),
    )


def _write_spec(
    tmp_path: Path, spec: ModernReferenceCertificationCampaignSpecV1
) -> Path:
    path = tmp_path / "campaign.json"
    path.write_text(spec.to_json() + "\n", encoding="utf-8")
    return path


def test_campaign_extracts_every_value_and_reaches_ready_for_promotion(
    tmp_path: Path,
) -> None:
    """A campaign can pass only with exact hash-bound scalar extractions."""
    spec = _complete_spec(tmp_path)
    spec_path = _write_spec(tmp_path, spec)

    dossier, result = run_modern_reference_certification_campaign(
        spec_path, output_directory=tmp_path / "output"
    )

    assert dossier.state is CertificationState.READY_FOR_PROMOTION
    assert result.state is CertificationState.READY_FOR_PROMOTION
    assert result.verified_input_count == len(spec.artifacts)
    assert result.observation_count == len(spec.observations) + 2
    assert read_modern_reference_certification_campaign_spec(spec_path) == spec
    assert Path(result.dossier_json.path).is_file()
    assert Path(result.dossier_markdown.path).is_file()
    assert (tmp_path / "output" / "campaign-result.json").is_file()


def test_campaign_rejects_changed_evidence_bytes(tmp_path: Path) -> None:
    """A report changed after campaign freeze cannot be consumed."""
    spec = _complete_spec(tmp_path)
    spec_path = _write_spec(tmp_path, spec)
    changed = tmp_path / spec.artifacts[0].path
    changed.write_text("{}\n", encoding="utf-8")

    with pytest.raises(ValueError, match="hash differs"):
        run_modern_reference_certification_campaign(
            spec_path, output_directory=tmp_path / "output"
        )


def test_campaign_rejects_inline_values_and_premature_coverage(
    tmp_path: Path,
) -> None:
    """Ordinary dev campaigns cannot smuggle values or promotion coverage."""
    spec = _complete_spec(tmp_path)
    payload = spec.to_dict()
    payload["observation_values_inline"] = True
    with pytest.raises(ValueError, match="inline observation"):
        ModernReferenceCertificationCampaignSpecV1.from_dict(payload)

    first = spec.observations[0]
    coverage = CertificationCampaignObservationV1(
        check_id="coverage_promotion_run_count",
        measurement_evidence_key=first.measurement_evidence_key,
        measurement_pointer=first.measurement_pointer,
        artifact_evidence_keys=first.artifact_evidence_keys,
    )
    with pytest.raises(ValueError, match="forbidden outside promotion"):
        ModernReferenceCertificationCampaignSpecV1(
            common_end_period=spec.common_end_period,
            peak_memory_budget_bytes=spec.peak_memory_budget_bytes,
            scratch_budget_bytes=spec.scratch_budget_bytes,
            runtime_budget_seconds=spec.runtime_budget_seconds,
            storage_budget_bytes=spec.storage_budget_bytes,
            candidate_amplification_budget=spec.candidate_amplification_budget,
            artifacts=spec.artifacts,
            observations=(*spec.observations, coverage),
            methodology=spec.methodology,
            accepted_limitations=spec.accepted_limitations,
            blocking_limitations=spec.blocking_limitations,
        )
