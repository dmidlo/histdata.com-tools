"""Ordinary host tests also enforce the standalone source-build lock."""

import hashlib
import json
from pathlib import Path
from runpy import run_path
import shutil

import pytest

ROOT = Path(__file__).resolve().parents[2]
REFERENCE = ROOT / "broker-plugins/reference"


def _check(root):
    functions = run_path(str(REFERENCE / "tools/generate_metadata.py"))
    functions["check_metadata"](root)


def test_reference_source_build_lock_matches_actual_checkout():
    # This is the actual stdlib-only build checker, not a duplicated verifier.
    _check(REFERENCE)
    lock = json.loads((REFERENCE / "metadata-lock.json").read_text("ascii"))
    workflow = ".github/workflows/ci.yml"
    assert (
        lock["files"][workflow]
        == hashlib.sha256((REFERENCE / workflow).read_bytes()).hexdigest()
    )


def test_reference_source_build_refuses_workflow_drift(tmp_path):
    lock = json.loads((REFERENCE / "metadata-lock.json").read_text("ascii"))
    for name in ("metadata-lock.json", *lock["files"]):
        target = tmp_path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(REFERENCE / name, target)
    _check(tmp_path)
    workflow = tmp_path / ".github/workflows/ci.yml"
    workflow.write_bytes(workflow.read_bytes() + b"\n# changed after locking\n")
    with pytest.raises(RuntimeError, match="stale metadata") as failure:
        _check(tmp_path)
    assert str(failure.value.__cause__) == "source or resource changed"


def test_starter_source_build_lock_is_not_rewritten_as_reference_format():
    root = ROOT / "broker-plugins/starter"
    lock = json.loads((root / "metadata-lock.json").read_text("ascii"))
    assert "files" not in lock
    assert all(
        hashlib.sha256((root / name).read_bytes()).hexdigest() == digest
        for name, digest in lock.items()
    )
