"""Fresh matrix enforcement using generated inputs and real native checks only."""

from dataclasses import replace
import hashlib
import json
import os

import pytest

from histdatacom.reconstruction import (
    ReconstructionClient,
    ReconstructionExitCode,
)
from histdatacom.reconstruction_cli import main
from histdatacom.reconstruction_cli import build_parser
from histdatacom.cli_config import configured_reconstruction_argv
from histdatacom.reconstruction_math import (
    current_reconstruction_math_verification_report,
)
from histdatacom.synthetic.capability_certification import (
    CapabilityCertificationDossierV1,
    CapabilityCertificationSpecV1,
    CapabilityCertificationStateV1,
    evaluate_capability_certification,
    read_capability_certification_spec,
    render_capability_certification_markdown,
    run_capability_certification,
    verify_capability_certification_dossier,
)
from histdatacom.synthetic.capability_matrix import (
    CAPABILITY_REQUIREMENT_IDS,
    V2_5_CRITICAL_REQUIREMENT_IDS,
    CapabilityClaimKindV1 as Claim,
    CapabilityEvidenceScopeV1 as Scope,
    CapabilityMatrixPolicyV1 as Policy,
    CapabilityMatrixRequirementV1 as Requirement,
    CapabilityMatrixRowV1 as Row,
    CapabilityMatrixV1 as Matrix,
    CapabilityMatrixWaiverV1 as Waiver,
    CapabilityStateV1 as State,
)
from histdatacom.synthetic.capability_verification import (
    CapabilityEvidenceDeclarationV1,
    CapabilityEvidenceRefV1,
    verify_capability_execution,
)

NOW = "2026-10-03T12:00:00Z"
KERNELS = "computational_reproducibility.reference_kernels"


def _matrix(policy, passed=()):
    outcomes = {item.requirement_id: item for item in passed}
    return Matrix(
        policy_id=policy.policy_id,
        release_id=policy.release_id,
        dataset_id=policy.dataset_id,
        as_of_utc=NOW,
        rows=tuple(
            Row(
                requirement_id=name,
                policy_id=policy.policy_id,
                implementation_commit="a" * 40 if name in outcomes else None,
                execution_artifact_id=(
                    outcomes[name].execution_artifact_id
                    if name in outcomes
                    else None
                ),
                independent_verification_artifact_id=(
                    outcomes[name].independent_verification_artifact_id
                    if name in outcomes
                    else None
                ),
                state=(
                    State.EXECUTED_PASSED
                    if name in outcomes
                    else State.NOT_IMPLEMENTED
                ),
                evidence_scope=(
                    outcomes[name].evidence_scope
                    if name in outcomes
                    else Scope.NONE
                ),
                scope="Generated test of an exactly scoped native mathematical requirement",
                limitations=(
                    "No historical campaign or provider qualification",
                ),
                blocking_issues=(),
                last_verified_at_utc=NOW if name in outcomes else None,
                release_id=policy.release_id,
                dataset_id=policy.dataset_id,
            )
            for name in CAPABILITY_REQUIREMENT_IDS
        ),
    )


def _spec(tmp_path, *, passed=True):
    """Actually execute the bounded reference kernel, not a fixture scalar."""
    report = current_reconstruction_math_verification_report.__wrapped__()
    encoded = (report.to_json() + "\n").encode()
    (tmp_path / "math.json").write_bytes(encoded)
    declaration = CapabilityEvidenceDeclarationV1(
        KERNELS,
        "reference-kernels.v1",
        CapabilityEvidenceRefV1(
            "math.json",
            report.schema_version,
            report.report_id,
            hashlib.sha256(encoded).hexdigest(),
            len(encoded),
        ),
    )
    prepared = verify_capability_execution(
        (declaration,), evidence_root=tmp_path, dataset_id=None
    )
    assert not prepared.blockers
    policy = Policy(
        release_id=prepared.release_id,
        dataset_id=None,
        claim_kind=Claim.NARROWER_PREDECLARED,
        claim_label="Actual generated reference-kernel check only",
        scope="One native software requirement, not a complete dataset",
        limitations=(
            "No full campaign, empirical quality, or publication claim",
        ),
        requirements=(Requirement(KERNELS, Scope.SOFTWARE),),
        permitted_waivers=(),
    )
    return CapabilityCertificationSpecV1(
        policy,
        _matrix(policy, prepared.outcomes if passed else ()),
        (declaration,),
    )


def _write_spec(tmp_path, spec):
    path = tmp_path / "spec.json"
    path.write_text(spec.to_json() + "\n")
    return path


def test_real_native_software_verification_is_explicitly_limited(tmp_path):
    spec = _spec(tmp_path)
    dossier = evaluate_capability_certification(spec, evidence_root=tmp_path)
    assert (
        dossier.state is CapabilityCertificationStateV1.VERIFIED_LIMITED_CLAIM
    )
    assert dossier.spec.matrix.matrix_id == spec.matrix.matrix_id
    assert dossier.spec.policy.policy_id == spec.policy.policy_id
    assert dossier.verification.outcomes[0].passed
    assert dossier.verification.to_dict()["full_certification"] is False
    assert (
        verify_capability_certification_dossier(dossier, evidence_root=tmp_path)
        == dossier
    )
    restored = CapabilityCertificationDossierV1.from_json(dossier.to_json())
    assert restored == dossier
    markdown = render_capability_certification_markdown(dossier)
    assert "verified_limited_claim" in markdown
    assert "never certifies the complete" in markdown


def test_real_native_pass_does_not_override_unexecuted_matrix(tmp_path):
    dossier = evaluate_capability_certification(
        _spec(tmp_path, passed=False), evidence_root=tmp_path
    )
    assert dossier.state is CapabilityCertificationStateV1.BLOCKED
    assert dossier.verification.outcomes[0].passed
    assert any(item[0] == KERNELS for item in dossier.verification.blockers)


def test_stored_receipt_and_rehashed_report_cannot_replace_fresh_native_check(
    tmp_path,
):
    spec = _spec(tmp_path)
    dossier = evaluate_capability_certification(spec, evidence_root=tmp_path)
    payload = json.loads((tmp_path / "math.json").read_text())
    payload["report_id"] = "forged-report:sha256:" + "b" * 64
    encoded = json.dumps(payload, sort_keys=True).encode()
    (tmp_path / "math.json").write_bytes(encoded)
    # A retained structure can still be inspected, but actual revalidation fails.
    assert (
        CapabilityCertificationDossierV1.from_json(dossier.to_json()) == dossier
    )
    with pytest.raises(ValueError):
        verify_capability_certification_dossier(dossier, evidence_root=tmp_path)
    reference = replace(
        spec.declarations[0].execution,
        sha256=hashlib.sha256(encoded).hexdigest(),
        size_bytes=len(encoded),
    )
    forged = replace(
        spec,
        declarations=(replace(spec.declarations[0], execution=reference),),
        spec_id="",
    )
    with pytest.raises(ValueError, match="subject"):
        evaluate_capability_certification(forged, evidence_root=tmp_path)
    # Rebinding the claimed subject gets past graph association, not the real
    # native mathematical report's canonical identity and recomputation.
    reference = replace(reference, subject_id=payload["report_id"])
    forged = replace(
        forged,
        declarations=(replace(spec.declarations[0], execution=reference),),
        spec_id="",
    )
    result = evaluate_capability_certification(forged, evidence_root=tmp_path)
    assert result.state is CapabilityCertificationStateV1.BLOCKED


def test_missing_bytes_cannot_be_replaced_by_persisted_pass(tmp_path):
    dossier = evaluate_capability_certification(
        _spec(tmp_path), evidence_root=tmp_path
    )
    (tmp_path / "math.json").unlink()
    with pytest.raises((ValueError, OSError)):
        verify_capability_certification_dossier(dossier, evidence_root=tmp_path)


def test_full_catalog_without_actual_campaign_is_blocked(tmp_path):
    policy = Policy(
        release_id="synthetic-full-claim-with-no-campaign",
        dataset_id=None,
        claim_kind=Claim.FULL_V2_5_CAMPAIGN,
        claim_label="Missing full campaign",
        scope="No real data are read by this test",
        limitations=("Unexecuted synthetic negative",),
        requirements=tuple(
            Requirement(name, Scope.COMPLETE)
            for name in V2_5_CRITICAL_REQUIREMENT_IDS
        ),
        permitted_waivers=(),
    )
    dossier = evaluate_capability_certification(
        CapabilityCertificationSpecV1(policy, _matrix(policy), ()),
        evidence_root=tmp_path,
    )
    assert dossier.state is CapabilityCertificationStateV1.BLOCKED
    assert {name for name, _ in dossier.verification.blockers} >= set(
        V2_5_CRITICAL_REQUIREMENT_IDS
    )
    assert (
        "__matrix__",
        "full_claim_requires_materialized_dataset",
    ) in dossier.verification.blockers


def test_public_client_and_cli_repeat_checks_and_retain_refusals(
    tmp_path, capsys
):
    spec = _spec(tmp_path)
    path = _write_spec(tmp_path, spec)
    output = tmp_path / "out"
    dossier = ReconstructionClient().certify_capabilities(
        path, evidence_root=tmp_path, output_directory=output
    )
    assert read_capability_certification_spec(path) == spec
    assert (
        run_capability_certification(
            path, evidence_root=tmp_path, output_directory=output
        )
        == dossier
    )
    assert (
        CapabilityCertificationDossierV1.from_json(
            (output / "capability-certification.json").read_text()
        )
        == dossier
    )
    code = main(
        [
            "--json",
            "certify-capabilities",
            "--spec",
            str(path),
            "--evidence-root",
            str(tmp_path),
            "--output-directory",
            str(output),
        ]
    )
    assert code == ReconstructionExitCode.SUCCESS
    assert (
        json.loads(capsys.readouterr().out)["state"] == "verified_limited_claim"
    )
    negative = replace(spec, matrix=_matrix(spec.policy), spec_id="")
    path = _write_spec(tmp_path, negative)
    code = main(
        [
            "--json",
            "certify-capabilities",
            "--spec",
            str(path),
            "--evidence-root",
            str(tmp_path),
            "--output-directory",
            str(tmp_path / "refused"),
        ]
    )
    assert code == ReconstructionExitCode.REFUSED
    assert json.loads(capsys.readouterr().out)["state"] == "blocked"


def test_existing_output_is_preserved_not_overwritten(tmp_path):
    path = _write_spec(tmp_path, _spec(tmp_path))
    output = tmp_path / "out"
    output.mkdir()
    destination = output / "capability-certification.json"
    destination.write_text("unrelated retained artifact")
    with pytest.raises(ValueError, match="refusing to replace"):
        run_capability_certification(
            path, evidence_root=tmp_path, output_directory=output
        )
    assert destination.read_text() == "unrelated retained artifact"
    assert not list(output.glob(".capability-*"))


@pytest.mark.parametrize("field", ["spec_id", "unknown", "matrix"])
def test_spec_rejects_foreign_or_extra_structural_fields(tmp_path, field):
    payload = _spec(tmp_path).to_dict()
    payload[field] = "forged"
    with pytest.raises((ValueError, TypeError)):
        CapabilityCertificationSpecV1.from_dict(payload)


def test_frozen_object_tamper_is_readmitted(tmp_path):
    spec = _spec(tmp_path)
    object.__setattr__(spec, "spec_id", "forged")
    with pytest.raises(ValueError):
        evaluate_capability_certification(spec, evidence_root=tmp_path)


@pytest.mark.parametrize(
    "text",
    ['{"schema_version":"a","schema_version":"b"}', '{"value":NaN}', "[]"],
)
def test_strict_json_refuses_duplicate_nonfinite_or_nonobject(text):
    with pytest.raises(ValueError):
        CapabilityCertificationSpecV1.from_json(text)


def test_reader_is_bounded_before_json_parsing(tmp_path):
    path = tmp_path / "too-large.json"
    path.write_bytes(b" " * (8 * 1024 * 1024 + 1))
    with pytest.raises(ValueError, match="bounded"):
        read_capability_certification_spec(path)


@pytest.mark.parametrize("kind", ["symlink", "fifo"])
def test_spec_reader_refuses_nonregular_inputs_without_blocking(tmp_path, kind):
    path = tmp_path / "not-regular.json"
    if kind == "symlink":
        target = tmp_path / "target.json"
        target.write_text("{}")
        path.symlink_to(target)
    else:
        os.mkfifo(path)
    with pytest.raises((OSError, ValueError)):
        read_capability_certification_spec(path)


def test_explicit_full_policy_waivers_never_produce_full_campaign_label(
    tmp_path,
):
    dataset = "generated-dataset:sha256:" + "f" * 64
    prepared = verify_capability_execution(
        (), evidence_root=tmp_path, dataset_id=dataset
    )
    policy = Policy(
        release_id=prepared.release_id,
        dataset_id=dataset,
        claim_kind=Claim.FULL_V2_5_CAMPAIGN,
        claim_label="Explicitly waived synthetic policy",
        scope="No actual campaign exists",
        limitations=("Synthetic waiver mechanics only",),
        requirements=tuple(
            Requirement(name, Scope.COMPLETE)
            for name in V2_5_CRITICAL_REQUIREMENT_IDS
        ),
        permitted_waivers=tuple(
            Waiver(name, "synthetic policy section 1", "No execution claimed")
            for name in V2_5_CRITICAL_REQUIREMENT_IDS
        ),
    )
    initial = _matrix(policy)
    rows = tuple(
        (
            replace(
                row,
                state=State.WAIVED_WITH_LIMITATION,
                evidence_scope=Scope.COMPLETE,
                limitations=("No execution claimed",),
                waiver_policy_clause="synthetic policy section 1",
                row_id="",
            )
            if row.requirement_id in V2_5_CRITICAL_REQUIREMENT_IDS
            else row
        )
        for row in initial.rows
    )
    matrix = replace(initial, rows=rows, matrix_id="")
    dossier = evaluate_capability_certification(
        CapabilityCertificationSpecV1(policy, matrix, ()),
        evidence_root=tmp_path,
    )
    assert (
        dossier.state is CapabilityCertificationStateV1.VERIFIED_LIMITED_CLAIM
    )
    assert not dossier.verification.outcomes
    assert "Waiver" in render_capability_certification_markdown(dossier)


def test_no_callback_or_verified_boolean_can_enter_public_evaluation(tmp_path):
    spec = _spec(tmp_path)
    with pytest.raises(TypeError):
        evaluate_capability_certification(
            spec, evidence_root=tmp_path, verified=True
        )
    with pytest.raises(TypeError):
        evaluate_capability_certification(
            spec, evidence_root=tmp_path, verifier=lambda *_: True
        )


def test_capability_cli_yaml_defaults_and_explicit_overrides(tmp_path):
    config = tmp_path / "config.yaml"
    config.write_text(
        "histdatacom:\n  reconstruction:\n"
        "    command: certify-capabilities\n"
        "    spec: configured.json\n"
        "    evidence_root: configured-evidence\n"
        "    output_directory: configured-output\n"
        "    json: true\n"
    )
    parser = build_parser()
    args = parser.parse_args(
        configured_reconstruction_argv(["--config", str(config)])
    )
    assert (
        args.reconstruction_command,
        args.spec,
        args.evidence_root,
        args.output_directory,
        args.json,
    ) == (
        "certify-capabilities",
        "configured.json",
        "configured-evidence",
        "configured-output",
        True,
    )
    # A configured older command must not hide an explicitly selected successor.
    config.write_text(
        config.read_text().replace(
            "command: certify-capabilities", "command: preview"
        )
    )
    args = parser.parse_args(
        configured_reconstruction_argv(
            [
                "--config",
                str(config),
                "certify-capabilities",
                "--spec",
                "explicit.json",
                "--evidence-root",
                "explicit-evidence",
                "--output-directory",
                "explicit-output",
            ]
        )
    )
    assert (
        args.reconstruction_command,
        args.spec,
        args.evidence_root,
        args.output_directory,
    ) == (
        "certify-capabilities",
        "explicit.json",
        "explicit-evidence",
        "explicit-output",
    )


def test_bounded_reader_without_optional_posix_flags(tmp_path, monkeypatch):
    from histdatacom.synthetic.capability_certification import (
        _read_bounded_regular,
    )

    path = tmp_path / "regular"
    path.write_bytes(b"read once")
    monkeypatch.delattr(os, "O_NOFOLLOW", raising=False)
    monkeypatch.delattr(os, "O_NONBLOCK", raising=False)
    assert _read_bounded_regular(path, 10) == b"read once"
    with pytest.raises(ValueError):
        _read_bounded_regular(path, 1)


def test_bounded_reader_refuses_replaced_inode_before_read(
    tmp_path, monkeypatch
):
    from histdatacom.synthetic.capability_certification import (
        _read_bounded_regular,
    )

    path = tmp_path / "regular"
    substitute = tmp_path / "other"
    path.write_bytes(b"first")
    substitute.write_bytes(b"other")
    original_open = os.open

    def swapped_open(target, flags):
        substitute.replace(path)
        return original_open(target, flags)

    monkeypatch.setattr(os, "open", swapped_open)
    with pytest.raises(ValueError, match="bounded regular"):
        _read_bounded_regular(path, 5)
