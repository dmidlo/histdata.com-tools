"""Generated status is a structural claim view, never scientific execution."""

from __future__ import annotations

import hashlib
import sys
from collections import Counter
from dataclasses import replace
from pathlib import Path

import pytest

from histdatacom.synthetic.capability_matrix import (
    CAPABILITY_CATALOG,
    CAPABILITY_REQUIREMENT_IDS,
    MAX_MATRIX_BYTES,
    V2_5_CRITICAL_REQUIREMENT_IDS,
    CapabilityClaimKindV1,
    CapabilityEvidenceScopeV1,
    CapabilityStateV1,
    structural_matrix_blockers,
)
from scripts import generate_capability_matrix as generator

ROOT = Path(__file__).resolve().parents[2]
BASE = "c5304b37a0ad1898559693055b347144c0b71d6d"


@pytest.fixture
def generated_root(tmp_path: Path) -> Path:
    for relative in (
        generator.MATRIX_PATH,
        generator.POLICY_PATH,
        generator.TRADER_PATH,
    ):
        destination = tmp_path / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes((ROOT / relative).read_bytes())
    (tmp_path / "README.md").write_text(
        "untouched prefix\n"
        + generator.START
        + "\nstale\n"
        + generator.END
        + "\nuntouched suffix\n",
        encoding="utf-8",
    )
    return tmp_path


def test_current_machine_snapshot_has_exact_catalog_and_frozen_claim() -> None:
    matrix, policy = generator.read_inputs(ROOT)
    assert len(matrix.rows) == 93
    assert {row.requirement_id for row in matrix.rows} == set(
        CAPABILITY_REQUIREMENT_IDS
    )
    assert matrix.release_id == "histdatacom-unreleased-dev-" + BASE
    assert matrix.dataset_id is policy.dataset_id is None
    assert policy.claim_kind is CapabilityClaimKindV1.FULL_V2_5_CAMPAIGN
    assert not policy.permitted_waivers
    assert {row.requirement_id for row in policy.requirements} == set(
        V2_5_CRITICAL_REQUIREMENT_IDS
    )
    assert len(policy.requirements) == 24
    assert all(
        row.evidence_scope is CapabilityEvidenceScopeV1.COMPLETE
        for row in policy.requirements
    )
    assert len(structural_matrix_blockers(matrix, policy)) == 25
    assert "actual package promotion/publication evidence" in policy.scope
    assert "cannot establish full certification" in policy.scope
    promotion = next(
        row
        for row in matrix.rows
        if row.requirement_id == "package_release_promotion"
    )
    assert (
        "Requires actual package promotion/publication evidence"
        in promotion.scope
    )
    assert any(
        "readiness do not establish actual promotion/publication" in text
        for text in promotion.limitations
    )


def test_snapshot_distinguishes_code_partial_execution_and_missing_programs() -> (
    None
):
    matrix, _ = generator.read_inputs(ROOT)
    assert Counter(row.state.value for row in matrix.rows) == {
        "implemented_unexecuted": 41,
        "deferred_blocked": 35,
        "executed_insufficient_evidence": 10,
        "not_implemented": 7,
    }
    assert not any(
        row.state is CapabilityStateV1.EXECUTED_PASSED for row in matrix.rows
    )
    for row in matrix.rows:
        assert row.independent_verification_artifact_id is None
        assert row.last_verified_at_utc is None
        if row.execution_artifact_id is not None:
            assert row.state is CapabilityStateV1.EXECUTED_INSUFFICIENT_EVIDENCE
            assert any("not a fresh run" in text for text in row.limitations)
            assert row.evidence_scope in {
                CapabilityEvidenceScopeV1.SOFTWARE,
                CapabilityEvidenceScopeV1.BOUNDED,
            }


@pytest.mark.parametrize(
    ("name", "issue"),
    (
        ("campaign_product_index", 522),
        ("full_deep_verification", 523),
        ("era_stratified_audit", 524),
    ),
)
def test_upstream_campaign_gaps_are_not_waived(name: str, issue: int) -> None:
    matrix, _ = generator.read_inputs(ROOT)
    row = next(item for item in matrix.rows if item.requirement_id == name)
    assert row.state is CapabilityStateV1.NOT_IMPLEMENTED
    assert row.implementation_commit is None
    assert row.execution_artifact_id is None
    assert row.blocking_issues == (issue,)


def test_documented_status_bytes_equal_the_machine_rendering() -> None:
    for path, expected in generator.expected_outputs(ROOT).items():
        assert path.read_text(encoding="utf-8") == expected


def test_readme_has_exact_36_parent_rows_and_full_docs_all_93_claims() -> None:
    matrix, policy = generator.read_inputs(ROOT)
    summary = generator.render_readme(matrix, policy)
    table_rows = [
        line for line in summary.splitlines() if line.startswith("| ")
    ][1:]
    parents = [item for item in CAPABILITY_CATALOG if item.parent_id is None]
    assert len(table_rows) == len(parents) == 36
    assert all(title.title in summary for title in parents)
    docs = generator.render_docs(matrix, policy)
    for name in CAPABILITY_REQUIREMENT_IDS:
        assert docs.count("| " + name + " |") == 1
    assert "not established by this snapshot" in summary
    assert "Held #639" in summary
    assert "24 critical rows at complete scope" in docs
    assert "does not publish a package" in docs
    assert "requires actual promotion and publication evidence" in docs
    assert "narrowed claim and cannot establish full certification" in docs
    assert "promotion-readiness evidence" not in docs
    assert "(reconstruction-certification-contracts.md)" in docs


def _invoke(monkeypatch: pytest.MonkeyPatch, root: Path, flag: str) -> int:
    monkeypatch.setattr(generator, "ROOT", root)
    monkeypatch.setattr(sys, "argv", ["generate_capability_matrix.py", flag])
    return generator.main()


def test_write_is_repeatable_preserves_unrelated_readme_and_inputs(
    generated_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = (
        generator.MATRIX_PATH,
        generator.POLICY_PATH,
        generator.TRADER_PATH,
    )
    before = {
        str(path): hashlib.sha256(
            (generated_root / path).read_bytes()
        ).hexdigest()
        for path in paths
    }
    assert _invoke(monkeypatch, generated_root, "--write") == 0
    first = (generated_root / "README.md").read_bytes()
    assert first.startswith(b"untouched prefix\n")
    assert first.endswith(b"untouched suffix\n")
    assert _invoke(monkeypatch, generated_root, "--write") == 0
    assert (generated_root / "README.md").read_bytes() == first
    assert _invoke(monkeypatch, generated_root, "--check") == 0
    assert {
        str(path): hashlib.sha256(
            (generated_root / path).read_bytes()
        ).hexdigest()
        for path in paths
    } == before


@pytest.mark.parametrize("path", ("README.md", "docs/capability-matrix.md"))
def test_check_detects_generated_drift_without_rewriting(
    generated_root: Path, monkeypatch: pytest.MonkeyPatch, path: str
) -> None:
    assert _invoke(monkeypatch, generated_root, "--write") == 0
    target = generated_root / path
    target.write_text(
        target.read_text(encoding="utf-8").replace(
            "implemented_unexecuted", "forged_pass", 1
        ),
        encoding="utf-8",
    )
    before = target.read_bytes()
    assert _invoke(monkeypatch, generated_root, "--check") == 1
    assert target.read_bytes() == before


@pytest.mark.parametrize(
    "markers",
    (
        "",
        generator.START,
        generator.START + generator.START + generator.END,
        generator.END + generator.START,
    ),
)
def test_missing_duplicate_reversed_markers_refuse_before_any_write(
    generated_root: Path, monkeypatch: pytest.MonkeyPatch, markers: str
) -> None:
    target = generated_root / "README.md"
    target.write_text(markers, encoding="ascii")
    with pytest.raises(ValueError, match="marker"):
        _invoke(monkeypatch, generated_root, "--write")
    assert target.read_text(encoding="ascii") == markers
    assert not (generated_root / "docs/capability-matrix.md").exists()


@pytest.mark.parametrize("path", (generator.MATRIX_PATH, generator.POLICY_PATH))
def test_noncanonical_machine_inputs_refuse(
    generated_root: Path, path: Path
) -> None:
    target = generated_root / path
    target.write_bytes(target.read_bytes() + b"\n")
    with pytest.raises(ValueError, match="canonical"):
        generator.expected_outputs(generated_root)


def test_valid_but_different_policy_identity_refuses_before_write(
    generated_root: Path,
) -> None:
    _, policy = generator.read_inputs(generated_root)
    changed = replace(
        policy, claim_label="Different frozen claim", policy_id=""
    )
    (generated_root / generator.POLICY_PATH).write_text(
        changed.to_json(), encoding="ascii"
    )
    with pytest.raises(ValueError, match="differ"):
        generator.expected_outputs(generated_root)
    assert not (generated_root / "docs/capability-matrix.md").exists()


def test_oversize_input_refuses_before_json_parse(
    generated_root: Path,
) -> None:
    (generated_root / generator.MATRIX_PATH).write_bytes(
        b"x" * (MAX_MATRIX_BYTES + 1)
    )
    with pytest.raises(ValueError, match="byte bound"):
        generator.expected_outputs(generated_root)


def test_malformed_identity_is_not_rendered_as_a_status(
    generated_root: Path,
) -> None:
    target = generated_root / generator.MATRIX_PATH
    text = target.read_text(encoding="ascii")
    assert '"state":"not_implemented"' in text
    target.write_text(
        text.replace(
            '"state":"not_implemented"', '"state":"executed_passed"', 1
        ),
        encoding="ascii",
    )
    with pytest.raises(ValueError):
        generator.expected_outputs(generated_root)
