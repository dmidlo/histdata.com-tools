"""Generated trader claims remain distinct from parent certification evidence."""

from __future__ import annotations

import hashlib
import os
import sys
from collections import Counter
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from histdatacom.synthetic.capability_matrix import CapabilityStateV1
from histdatacom.synthetic.trader_maturity import (
    MAX_WIRE_BYTES,
    TRADER_MATURITY_STAGE_IDS,
    TraderReferenceAvailabilityV1,
)
from scripts import generate_capability_matrix as generator

ROOT = Path(__file__).resolve().parents[2]
INPUTS = (
    generator.MATRIX_PATH,
    generator.POLICY_PATH,
    generator.TRADER_PATH,
)


@pytest.fixture
def generated_root(tmp_path: Path) -> Path:
    for relative in INPUTS:
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((ROOT / relative).read_bytes())
    (tmp_path / "README.md").write_text(
        "untouched prefix\n"
        + generator.START
        + "\nold generated section\n"
        + generator.END
        + "\nuntouched suffix\n",
        encoding="ascii",
    )
    return tmp_path


def test_released_parent_machine_bytes_are_unchanged() -> None:
    expected = {
        generator.MATRIX_PATH: "1a5cd466c95becbea263ed82d694a69b075d0ddec105e0417280211e2c958ad9",
        generator.POLICY_PATH: "e0243113d378858826e0ad1dfe72bd0786671baa9e841b7f3a4d626a0c73da5b",
    }
    for relative, digest in expected.items():
        assert (
            hashlib.sha256((ROOT / relative).read_bytes()).hexdigest() == digest
        )


def test_current_sixteen_claims_retain_exact_scope_and_unavailable_evidence() -> (
    None
):
    parent, _ = generator.read_inputs(ROOT)
    supplement = generator.read_trader_input(ROOT, parent)
    assert (
        tuple(row.stage_id for row in supplement.rows)
        == TRADER_MATURITY_STAGE_IDS
    )
    assert len(supplement.rows) == 16
    assert supplement.parent_matrix_id == parent.matrix_id
    assert supplement.release_id == parent.release_id
    assert supplement.dataset_id is parent.dataset_id is None
    assert supplement.claim_kind == "non_authoritative_supplement"
    assert Counter(row.state.value for row in supplement.rows) == {
        "deferred_blocked": 8,
        "not_implemented": 7,
        "implemented_unexecuted": 1,
    }
    assert (
        supplement.catalog.availability
        is TraderReferenceAvailabilityV1.DECLARED_UNAVAILABLE
    )
    assert supplement.catalog.source_artifact_id is None
    assert (
        supplement.catalog.source_json_sha256
        == "f6a83e7e60cde3e90c271f6342c204995f67b9261b912adb56407e51bbb5a7d2"
    )
    assert (
        supplement.catalog.canonical_catalog_sha256
        == "402deed78368bf5e96d638736abab3c3b145c120a186964a1d3c8bf50f8daa31"
    )
    for row in supplement.rows:
        assert row.evidence_scope.value == "none"
        assert row.last_verified_at_utc is None
        assert row.evidence.execution_artifact_id is None
        assert row.evidence.independent_verification_artifact_id is None
        assert row.evidence.campaign_id is None
        assert row.evidence.product_id is None
    by_id = {row.stage_id: row for row in supplement.rows}
    assert 680 in by_id["archive_integrity"].blocking_issues
    assert 680 in by_id["catalog_identity"].blocking_issues
    branch = by_id["branch_integration"]
    assert branch.state is CapabilityStateV1.IMPLEMENTED_UNEXECUTED
    assert "five provider-neutral" in branch.scope
    assert "not a production trader subsystem" in branch.scope
    assert (
        branch.evidence.implementation_commit
        == "3def2ab9393b7f2e4cad444530def9f91da91a64"
    )
    assert any("closure comment is not" in item for item in branch.limitations)


def test_generated_views_append_sixteen_without_changing_parent_view() -> None:
    parent, policy = generator.read_inputs(ROOT)
    supplement = generator.read_trader_input(ROOT, parent)
    outputs = generator.expected_outputs(ROOT)
    readme = outputs[ROOT / "README.md"]
    docs = outputs[ROOT / "docs/capability-matrix.md"]
    assert docs.startswith(generator.render_docs(parent, policy))
    parent_readme = generator.render_readme(parent, policy).removesuffix(
        generator.END
    )
    assert parent_readme in readme
    summary = generator.render_trader_readme(supplement, parent)
    assert (
        len([line for line in summary.splitlines() if line.startswith("| ")])
        == 17
    )
    for stage in TRADER_MATURITY_STAGE_IDS:
        assert docs.count("| " + stage + ": ") == 1
    assert "unchanged 93-row matrix" in summary
    assert "not 16 additional certification requirements" in summary
    assert "grants no certification or publication authority" in summary
    assert "Authoritative v33 source bytes are unavailable (#680)" in docs
    assert supplement.parent_matrix_id in summary
    assert supplement.release_id in summary
    assert "dataset: `absent`" in summary
    for path, expected in outputs.items():
        assert path.read_text("utf-8") == expected


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("parent_matrix_id", "capability-matrix:sha256:" + "a" * 64),
        ("release_id", "different-release"),
        ("dataset_id", "dataset:sha256:" + "b" * 64),
    ),
)
def test_resealed_parent_binding_mismatch_refuses_before_output(
    generated_root: Path, field: str, value: str
) -> None:
    parent, _ = generator.read_inputs(generated_root)
    supplement = generator.read_trader_input(generated_root, parent)
    changed = replace(supplement, **{field: value, "matrix_id": ""})
    (generated_root / generator.TRADER_PATH).write_text(
        changed.to_json(), encoding="ascii"
    )
    before = (generated_root / "README.md").read_bytes()
    with pytest.raises(ValueError, match="differs"):
        generator.expected_outputs(generated_root)
    assert (generated_root / "README.md").read_bytes() == before
    assert not (generated_root / "docs/capability-matrix.md").exists()


@pytest.mark.parametrize(
    "change", ("missing", "noncanonical", "schema", "oversize")
)
def test_invalid_required_third_input_refuses_before_output(
    generated_root: Path, change: str
) -> None:
    target = generated_root / generator.TRADER_PATH
    if change == "missing":
        target.unlink()
    elif change == "noncanonical":
        target.write_bytes(target.read_bytes() + b"\n")
    elif change == "schema":
        target.write_text(
            target.read_text("ascii").replace(
                "histdatacom.trader-maturity-matrix.v1",
                "histdatacom.trader-maturity-matrix.v99",
            ),
            encoding="ascii",
        )
    else:
        target.write_bytes(b"x" * (MAX_WIRE_BYTES + 1))
    before = (generated_root / "README.md").read_bytes()
    with pytest.raises((ValueError, FileNotFoundError)):
        generator.expected_outputs(generated_root)
    assert (generated_root / "README.md").read_bytes() == before
    assert not (generated_root / "docs/capability-matrix.md").exists()


@pytest.mark.parametrize("relative", ("README.md", "docs/capability-matrix.md"))
def test_trader_only_render_drift_is_detected_without_rewriting(
    generated_root: Path, monkeypatch: pytest.MonkeyPatch, relative: str
) -> None:
    monkeypatch.setattr(generator, "ROOT", generated_root)
    monkeypatch.setattr(
        sys, "argv", ["generate_capability_matrix.py", "--write"]
    )
    assert generator.main() == 0
    target = generated_root / relative
    text = target.read_text("utf-8")
    at = text.index("Trader maturity")
    target.write_text(
        text[:at] + text[at:].replace("deferred_blocked", "executed_passed", 1),
        encoding="utf-8",
    )
    before = target.read_bytes()
    monkeypatch.setattr(
        sys, "argv", ["generate_capability_matrix.py", "--check"]
    )
    assert generator.main() == 1
    assert target.read_bytes() == before


@pytest.mark.parametrize("relative", INPUTS)
@pytest.mark.parametrize("kind", ("symlink", "fifo"))
def test_nonregular_input_refuses_before_descriptor_open(
    generated_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    relative: Path,
    kind: str,
) -> None:
    target = generated_root / relative
    target.unlink()
    if kind == "symlink":
        target.symlink_to(ROOT / relative)
    else:
        if not hasattr(os, "mkfifo"):
            pytest.skip("FIFO creation is unavailable on this platform")
        os.mkfifo(target)

    def poison(*args: object, **kwargs: object) -> int:
        raise AssertionError("nonregular input reached descriptor open")

    monkeypatch.setattr(generator.os, "open", poison)
    with pytest.raises(ValueError, match="regular file"):
        generator._read(target)


@pytest.mark.parametrize("phase", ("before", "after"))
def test_descriptor_identity_changes_refuse(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    target = tmp_path / "input.json"
    target.write_bytes(b"{}")
    original = os.fstat
    calls = 0

    def changed(descriptor: int) -> os.stat_result:
        nonlocal calls
        calls += 1
        value = original(descriptor)
        if calls == (1 if phase == "before" else 2):
            attributes = {
                name: getattr(value, name)
                for name in (
                    "st_dev",
                    "st_ino",
                    "st_mode",
                    "st_size",
                    "st_mtime_ns",
                    "st_ctime_ns",
                )
            }
            attributes["st_ino"] += 1
            return cast(os.stat_result, SimpleNamespace(**attributes))
        return value

    monkeypatch.setattr(generator.os, "fstat", changed)
    with pytest.raises(ValueError, match="changed"):
        generator._read(target)


def test_valid_reader_retains_checks_without_optional_posix_flags(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "input.json"
    target.write_bytes(b"{}")
    monkeypatch.delattr(generator.os, "O_NOFOLLOW", raising=False)
    monkeypatch.delattr(generator.os, "O_NONBLOCK", raising=False)
    assert generator._read(target) == "{}"
