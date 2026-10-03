"""Closed fresh proofs from generated inputs, never empirical certification."""

from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path

import pytest

from histdatacom.synthetic import capability_verification as api
from histdatacom.reconstruction_math import (
    current_reconstruction_math_verification_report,
)
from histdatacom.synthetic.capability_matrix import (
    CapabilityEvidenceScopeV1 as Scope,
)
from histdatacom.synthetic.capability_matrix import CapabilityStateV1 as State
from histdatacom.synthetic.proposal_engines import proposal_engine_registry
from tests.unit.test_capability_matrix import matrix, policy, row


def reference(root, path, *, subject=None, schema=None):
    raw = path.read_bytes()
    return api.CapabilityEvidenceRefV1(
        path.relative_to(root).as_posix(),
        schema,
        subject,
        hashlib.sha256(raw).hexdigest(),
        len(raw),
    )


def declaration(root, profile="reference-kernels.v1"):
    if profile == "reference-kernels.v1":
        value = current_reconstruction_math_verification_report.__wrapped__()
        subject = value.report_id
    else:
        value = proposal_engine_registry()
        subject = value.registry_id
    path = root / (profile + ".json")
    path.write_text(value.to_json())
    return api.CapabilityEvidenceDeclarationV1(
        api._PROFILES[profile],
        profile,
        reference(root, path, subject=subject, schema=value.schema_version),
    )


@pytest.fixture
def math_evidence(tmp_path):
    root = tmp_path.resolve()
    item = declaration(root)
    result = api.verify_capability_execution(
        (item,), evidence_root=root, dataset_id=None
    )
    assert result.outcomes[0].passed
    return root, item, result


def bound_matrix(receipt, *, scope=Scope.SOFTWARE):
    proof = receipt.outcomes[0]
    frozen = policy(
        ids=(proof.requirement_id,),
        scope=scope,
        release_id=receipt.release_id,
        dataset_id=receipt.dataset_id,
    )
    claim = row(
        frozen,
        proof.requirement_id,
        State.EXECUTED_PASSED,
        execution_artifact_id=proof.execution_artifact_id,
        independent_verification_artifact_id=proof.independent_verification_artifact_id,
        evidence_scope=proof.evidence_scope,
    )
    return matrix(frozen, (claim,)), frozen


@pytest.mark.parametrize(
    "profile", ("reference-kernels.v1", "model-registry.v1")
)
def test_real_current_software_checks_are_recomputed_and_matrix_bound(
    tmp_path, profile
):
    root = tmp_path.resolve()
    item = declaration(root, profile)
    prepared = api.verify_capability_execution(
        (item,), evidence_root=root, dataset_id=None
    )
    assert prepared.outcomes[0].passed and not prepared.blockers
    value, frozen = bound_matrix(prepared)
    fresh = api.verify_capability_evidence(
        value, frozen, evidence_root=root, declarations=(item,)
    )
    assert fresh == prepared
    assert fresh.to_dict()["full_certification"] is False
    assert fresh.to_dict()["external_execution_attested"] is False
    assert (
        api.CapabilityEvidenceVerificationV1.from_json(fresh.to_json()) == fresh
    )
    assert api.CapabilityEvidenceDeclarationV1.from_dict(item.to_dict()) == item


def test_actual_generated_experiment_reads_catalog_partitions_and_exact_dataset(
    tmp_path,
):
    from tests.unit.test_reconstruction_experiment import _freeze

    root = tmp_path.resolve()
    native, ref, _, _ = _freeze(root)
    execution = reference(
        root,
        Path(ref.path),
        subject=native.experiment_id,
        schema=native.schema_version,
    )
    inputs = tuple(
        reference(root, path)
        for path in sorted(root.rglob("*"))
        if path.is_file() and path != Path(ref.path)
    )
    item = api.CapabilityEvidenceDeclarationV1(
        "source_experiment_identity", "experiment.v1", execution, inputs
    )
    (dataset,) = native.dataset_version_ids
    actual = api.verify_capability_execution(
        (item,), evidence_root=root, dataset_id=dataset
    )
    assert (
        actual.outcomes[0].passed
        and actual.outcomes[0].evidence_scope is Scope.BOUNDED
    )
    foreign = api.verify_capability_execution(
        (item,), evidence_root=root, dataset_id="dataset:sha256:" + "f" * 64
    )
    assert not foreign.outcomes[0].passed
    # A new declaration with current file hashes does not repair the parent
    # catalog's immutable partition hashes or make tampered inputs valid.
    partition = next(
        path
        for path in root.rglob("*")
        if path.is_file() and path.name == ".data"
    )
    raw = partition.read_bytes()
    partition.write_bytes(raw[:-1] + bytes((raw[-1] ^ 1,)))
    changed = replace(
        item,
        inputs=tuple(reference(root, root / ref.path) for ref in item.inputs),
    )
    with pytest.raises(ValueError, match="nested artifact"):
        api.verify_capability_execution(
            (changed,), evidence_root=root, dataset_id=dataset
        )


def test_omitted_reachable_native_inputs_fail_before_profile(
    tmp_path, monkeypatch
):
    root = tmp_path.resolve()
    item = declaration(root)
    path = root / item.execution.path
    payload = json.loads(path.read_text())
    payload["foreign_ref"] = {
        "path": str(root / "absent"),
        "sha256": "a" * 64,
        "size_bytes": 1,
    }
    path.write_text(json.dumps(payload))
    item = replace(
        item,
        execution=reference(
            root,
            path,
            subject=item.execution.subject_id,
            schema=item.execution.schema_version,
        ),
    )
    monkeypatch.setattr(
        api,
        "_profile",
        lambda *args: pytest.fail("profile reached before graph admission"),
    )
    with pytest.raises(ValueError, match="nested artifact"):
        api.verify_capability_execution(
            (item,), evidence_root=root, dataset_id=None
        )


@pytest.mark.parametrize("change", ("hash", "size", "subject", "schema"))
def test_declared_file_pins_are_checked_before_native_work(tmp_path, change):
    root = tmp_path.resolve()
    item = declaration(root)
    changes = {
        "hash": {"sha256": "f" * 64},
        "size": {"size_bytes": item.execution.size_bytes + 1},
        "subject": {"subject_id": "foreign:sha256:" + "e" * 64},
        "schema": {"schema_version": "histdatacom.proposal-engine-registry.v1"},
    }
    item = replace(item, execution=replace(item.execution, **changes[change]))
    with pytest.raises(ValueError):
        api.verify_capability_execution(
            (item,), evidence_root=root, dataset_id=None
        )


@pytest.mark.parametrize(
    "field",
    (
        "event_rows_inline",
        "samples_inline",
        "summary.passed",
        "summary.check_count",
    ),
)
def test_resealed_native_scalar_alias_is_not_exact_typed_evidence(
    tmp_path, field
):
    root = tmp_path.resolve()
    item = declaration(root)
    path = root / item.execution.path
    payload = json.loads(path.read_text())
    if field.startswith("summary."):
        key = field.split(".")[1]
        payload["summary"][key] = (
            1 if key == "passed" else float(payload["summary"][key])
        )
    else:
        payload[field] = 0
    # Retain the native content ID while resealing the actual file bytes.
    # A canonical comparison must distinguish scalar types even when Python
    # equality considers their values identical.
    path.write_text(json.dumps(payload))
    changed = replace(
        item,
        execution=reference(
            root,
            path,
            subject=item.execution.subject_id,
            schema=item.execution.schema_version,
        ),
    )
    assert changed.execution.sha256 != item.execution.sha256
    result = api.verify_capability_execution(
        (changed,), evidence_root=root, dataset_id=None
    )
    assert not result.outcomes[0].passed and result.blockers


def test_typed_auxiliary_subject_cannot_be_forged(tmp_path):
    root = tmp_path.resolve()
    item = declaration(root)
    other = declaration(root, "model-registry.v1").execution
    item = replace(
        item, inputs=(replace(other, subject_id="wrong:sha256:" + "a" * 64),)
    )
    with pytest.raises(ValueError, match="subject"):
        api.verify_capability_execution(
            (item,), evidence_root=root, dataset_id=None
        )


def test_scalar_report_and_coherent_altered_math_report_are_not_proof(tmp_path):
    root = tmp_path.resolve()
    item = declaration(root)
    path = root / item.execution.path
    path.write_text(
        json.dumps(
            {
                "schema_version": item.execution.schema_version,
                "report_id": item.execution.subject_id,
                "passed": True,
            }
        )
    )
    changed = replace(
        item,
        execution=reference(
            root,
            path,
            subject=item.execution.subject_id,
            schema=item.execution.schema_version,
        ),
    )
    failed = api.verify_capability_execution(
        (changed,), evidence_root=root, dataset_id=None
    )
    assert not failed.outcomes[0].passed and failed.blockers
    native = current_reconstruction_math_verification_report.__wrapped__()
    # A valid, self-consistent subset is not the current complete harness.
    altered = replace(native, checks=native.checks[:-1], report_id="")
    path.write_text(altered.to_json())
    changed = replace(
        item,
        execution=reference(
            root, path, subject=altered.report_id, schema=altered.schema_version
        ),
    )
    assert (
        not api.verify_capability_execution(
            (changed,), evidence_root=root, dataset_id=None
        )
        .outcomes[0]
        .passed
    )


@pytest.mark.parametrize(
    "field", ("execution_artifact_id", "independent_verification_artifact_id")
)
def test_matrix_caller_identity_is_not_authority(math_evidence, field):
    root, item, receipt = math_evidence
    value, frozen = bound_matrix(receipt)
    changed = tuple(
        (
            replace(entry, **{field: "foreign:sha256:" + "f" * 64}, row_id="")
            if entry.requirement_id == item.requirement_id
            else entry
        )
        for entry in value.rows
    )
    value = replace(value, rows=changed, matrix_id="")
    result = api.verify_capability_evidence(
        value, frozen, evidence_root=root, declarations=(item,)
    )
    assert (
        item.requirement_id,
        "fresh evidence identity or scope differs from row",
    ) in result.blockers


def test_narrow_software_proof_does_not_satisfy_complete_scope(math_evidence):
    root, item, receipt = math_evidence
    value, frozen = bound_matrix(receipt, scope=Scope.COMPLETE)
    result = api.verify_capability_evidence(
        value, frozen, evidence_root=root, declarations=(item,)
    )
    assert result.blockers and not result.to_dict()["full_certification"]


def test_missing_adapter_is_explicit_and_foreign_release_blocked(math_evidence):
    root, item, receipt = math_evidence
    frozen = policy(
        ids=("fresh_release_holdout",), release_id=receipt.release_id
    )
    claim = row(frozen, "fresh_release_holdout", State.EXECUTED_PASSED)
    result = api.verify_capability_evidence(
        matrix(frozen, (claim,)), frozen, evidence_root=root, declarations=()
    )
    assert any(
        reason == "closed verifier or required evidence unavailable"
        for _, reason in result.blockers
    )
    assert any(name == "__context__" for name, _ in result.blockers)


@pytest.mark.parametrize(
    "path", ("../a", "/absolute", "a//b", "a/./b", "a/../b", "a\\b")
)
def test_noncanonical_paths_refused(path):
    with pytest.raises(ValueError):
        api.CapabilityEvidenceRefV1(path, None, None, "a" * 64, 1)


def test_symlink_file_refused(tmp_path):
    root = tmp_path.resolve()
    item = declaration(root)
    source = root / item.execution.path
    other = root / "source.json"
    source.rename(other)
    source.symlink_to(other)
    with pytest.raises(ValueError, match="symlink"):
        api.verify_capability_execution(
            (item,), evidence_root=root, dataset_id=None
        )


@pytest.mark.parametrize(
    "payload",
    (
        b'{"x":1,"x":2}',
        b'{"x":NaN}',
        b'{"x":Infinity}',
        b"[" * 70 + b"0" + b"]" * 70,
    ),
)
def test_bounded_json_parser_rejects_ambiguity(payload):
    with pytest.raises(ValueError):
        api._json(payload)


def test_nested_frozen_mutation_and_subclass_rejected_before_io(
    math_evidence, monkeypatch
):
    root, item, receipt = math_evidence
    value, frozen = bound_matrix(receipt)
    monkeypatch.setattr(api, "_graph", lambda *args: pytest.fail("I/O reached"))
    object.__setattr__(item.execution, "size_bytes", True)
    with pytest.raises(ValueError):
        api.verify_capability_evidence(
            value, frozen, evidence_root=root, declarations=(item,)
        )

    class Hostile:
        @property
        def requirement_id(self):
            pytest.fail("hostile property evaluated")

    with pytest.raises(TypeError):
        api.verify_capability_evidence(
            value, frozen, evidence_root=root, declarations=(Hostile(),)
        )
    with pytest.raises(ValueError):
        api.verify_capability_evidence(
            value, frozen, evidence_root=root, declarations=[item]
        )


@pytest.mark.parametrize(
    "field",
    ("verification_id", "release_id", "implementation_id", "dataset_id"),
)
def test_structural_receipt_checks_its_own_identity(math_evidence, field):
    _, _, receipt = math_evidence
    payload = receipt.to_dict()
    payload[field] = "forged:sha256:" + "f" * 64
    with pytest.raises(ValueError):
        api.CapabilityEvidenceVerificationV1.from_dict(payload)


@pytest.mark.parametrize(
    "mutation", ("row_id", "result", "scope", "flag", "extra")
)
def test_receipt_nested_and_authority_tampering_refused(
    math_evidence, mutation
):
    _, _, receipt = math_evidence
    payload = receipt.to_dict()
    if mutation == "row_id":
        payload["outcomes"][0]["independent_verification_artifact_id"] = (
            "wrong:sha256:" + "0" * 64
        )
    elif mutation == "result":
        payload["outcomes"][0]["result_json"] = "{}"
    elif mutation == "scope":
        payload["outcomes"][0]["evidence_scope"] = "complete"
    elif mutation == "flag":
        payload["external_execution_attested"] = True
    else:
        payload["verified"] = True
    with pytest.raises(ValueError):
        api.CapabilityEvidenceVerificationV1.from_dict(payload)


def test_current_source_profile_identity_is_bound_not_caller_commit(
    math_evidence, monkeypatch
):
    root, item, receipt = math_evidence
    value, frozen = bound_matrix(receipt)
    monkeypatch.setattr(
        api,
        "_implementation",
        lambda: "capability-verifier-implementation:sha256:" + "e" * 64,
    )
    result = api.verify_capability_evidence(
        value, frozen, evidence_root=root, declarations=(item,)
    )
    assert result.release_id != receipt.release_id
    assert (
        "__context__",
        "current evidence release/dataset differs",
    ) in result.blockers


def test_unsupported_schema_profile_and_extra_claims_refuse(tmp_path):
    root = tmp_path.resolve()
    item = declaration(root)
    with pytest.raises(ValueError, match="unsupported typed"):
        replace(item.execution, schema_version="arbitrary.report.v1")
    with pytest.raises(ValueError, match="mapping"):
        replace(item, profile_version="caller-callback.v1")
    with pytest.raises(ValueError, match="fields"):
        api.CapabilityEvidenceDeclarationV1.from_dict(
            {**item.to_dict(), "selected_engine_ids": ["engine"]}
        )
    with pytest.raises(ValueError, match="duplicate"):
        api.verify_capability_execution(
            (item, item), evidence_root=root, dataset_id=None
        )


@pytest.mark.parametrize(
    "profile,requirement,dependencies",
    (
        (
            "powered-qualification.v1",
            "powered_engine_eligibility",
            ("model_bank_registration",),
        ),
        (
            "hawkes-selection.v1",
            "product_engine_selection",
            ("model_bank_registration", "powered_engine_eligibility"),
        ),
        (
            "dataset-publication.v1",
            "provider_neutral_publication",
            (
                "campaign_product_index",
                "complete_product_rectangle",
                "complete_temporal_campaign",
                "full_deep_verification",
                "era_stratified_audit",
                "derived_bars_reconciliation",
            ),
        ),
    ),
)
def test_unqualified_profiles_cannot_admit_even_recorded_pass_rows(
    tmp_path, profile, requirement, dependencies
):
    root = tmp_path.resolve()
    item = declaration(root)
    with pytest.raises(ValueError, match="mapping"):
        replace(item, profile_version=profile, requirement_id=requirement)
    context = api.verify_capability_execution(
        (), evidence_root=root, dataset_id=None
    )
    ids = (requirement, *dependencies)
    frozen = policy(ids=ids, release_id=context.release_id)
    claims = tuple(row(frozen, name, State.EXECUTED_PASSED) for name in ids)
    actual = api.verify_capability_evidence(
        matrix(frozen, claims), frozen, evidence_root=root, declarations=()
    )
    assert (
        requirement,
        "closed verifier or required evidence unavailable",
    ) in actual.blockers
    assert actual.outcomes == ()


def test_named_pipe_refuses_without_open_or_native_work(tmp_path, monkeypatch):
    root = tmp_path.resolve()
    item = declaration(root)
    path = root / item.execution.path
    path.unlink()
    os.mkfifo(path)
    monkeypatch.setattr(
        api.os,
        "open",
        lambda *args, **kwargs: pytest.fail("FIFO must refuse before open"),
    )
    monkeypatch.setattr(
        api, "_profile", lambda *args: pytest.fail("native profile reached")
    )
    with pytest.raises(ValueError, match="regular file"):
        api.verify_capability_execution(
            (item,), evidence_root=root, dataset_id=None
        )


def test_regular_descriptor_uses_nonblocking_nofollow_when_supported(
    tmp_path, monkeypatch
):
    path = tmp_path / "regular"
    path.write_bytes(b"value")
    original = os.open
    flags = []

    def record(path, value):
        flags.append(value)
        return original(path, value)

    monkeypatch.setattr(api.os, "open", record)
    assert api._read(path, 5)[0] == b"value"
    assert len(flags) == 1
    for name in ("O_NONBLOCK", "O_NOFOLLOW"):
        expected = getattr(os, name, 0)
        assert flags[0] & expected == expected


def test_oversized_declaration_collection_refuses_before_iteration(tmp_path):
    class Hostile:
        @property
        def requirement_id(self):
            pytest.fail("oversized collection traversed")

    with pytest.raises(ValueError, match="bounded exact tuple"):
        api.verify_capability_execution(
            (Hostile(),) * (api.MAX_DECLARATIONS + 1),
            evidence_root=tmp_path.resolve(),
            dataset_id=None,
        )


def test_full_missing_dataset_is_a_recorded_blocker_not_an_exception(tmp_path):
    from histdatacom.synthetic.capability_matrix import (
        CapabilityClaimKindV1,
        V2_5_CRITICAL_REQUIREMENT_IDS,
    )

    frozen = policy(
        ids=V2_5_CRITICAL_REQUIREMENT_IDS,
        claim_kind=CapabilityClaimKindV1.FULL_V2_5_CAMPAIGN,
        scope=Scope.COMPLETE,
    )
    checked = api.verify_capability_evidence(
        matrix(frozen),
        frozen,
        evidence_root=tmp_path.resolve(),
        declarations=(),
    )
    assert (
        "__matrix__",
        "full_claim_requires_materialized_dataset",
    ) in checked.blockers


def test_ancestor_directory_swap_is_detected_even_when_original_file_survives(
    tmp_path, monkeypatch
):
    root = tmp_path.resolve()
    folder = root / "declared"
    folder.mkdir()
    item = declaration(folder)
    item = replace(
        item,
        execution=replace(
            item.execution, path="declared/" + item.execution.path
        ),
    )
    original = api._profile

    def swapping(*args):
        result = original(*args)
        folder.rename(root / "aside")
        folder.mkdir()
        folder.rmdir()
        (root / "aside").rename(folder)
        return result

    monkeypatch.setattr(api, "_profile", swapping)
    with pytest.raises(ValueError, match="ancestor directory"):
        api.verify_capability_execution(
            (item,), evidence_root=root, dataset_id=None
        )


def test_native_relative_reference_cannot_verify_different_root_and_cwd_bytes(
    tmp_path, monkeypatch
):
    root = tmp_path.resolve() / "root"
    cwd = tmp_path.resolve() / "cwd"
    root.mkdir()
    cwd.mkdir()
    item = declaration(root)
    (root / "nested.bin").write_bytes(b"A")
    (cwd / "nested.bin").write_bytes(b"B")
    path = root / item.execution.path
    payload = json.loads(path.read_text())
    payload["foreign_ref"] = {
        "path": "nested.bin",
        "sha256": hashlib.sha256(b"A").hexdigest(),
        "size_bytes": 1,
    }
    path.write_text(json.dumps(payload))
    changed = replace(
        item,
        execution=reference(
            root,
            path,
            subject=item.execution.subject_id,
            schema=item.execution.schema_version,
        ),
        inputs=(reference(root, root / "nested.bin"),),
    )
    monkeypatch.chdir(cwd)
    monkeypatch.setattr(
        api, "_profile", lambda *args: pytest.fail("native reader reached")
    )
    with pytest.raises(ValueError, match="canonical absolute"):
        api.verify_capability_execution(
            (changed,), evidence_root=root, dataset_id=None
        )
