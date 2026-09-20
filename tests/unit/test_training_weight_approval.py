"""Operational-gate tests on synthetic package bytes, never real windows."""

from dataclasses import FrozenInstanceError, replace
import errno
import hashlib
import os
from pathlib import Path
import shutil

import pytest

from histdatacom.data_quality import training_weight_approval as approval
from histdatacom.data_quality.training_contracts import training_json
from histdatacom.data_quality.training_weight_contracts import (
    PREREGISTRATION_FILE_SHA256,
    read_training_weight_preregistration,
)


@pytest.fixture
def package(tmp_path, monkeypatch):
    root = tmp_path / "fixture_package"
    root.mkdir()
    (root / "__init__.py").write_bytes(b'"""Synthetic package."""\n')
    (root / "calculation.py").write_bytes(b"VALUE = 1\n")
    assets = root / "assets"
    assets.mkdir()
    (assets / "values.json").write_bytes(b'{"value":1}')
    (assets / "table.b64").write_bytes(b"c3ludGhldGlj")
    monkeypatch.setattr(approval, "_package_root", lambda: root)
    return root


def receipt(fingerprint="a" * 64, operations=None):
    return approval.TrainingWeightExecutionApprovalV1(
        approval.TRAINING_WEIGHT_POLICY_COMMIT,
        PREREGISTRATION_FILE_SHA256,
        fingerprint,
        operations or approval.TRAINING_WEIGHT_ALLOWED_OPERATIONS,
        "fixture coordinator",
        "2026-09-19T12:34:56Z",
    )


def test_explicit_receipt_roundtrip_and_policy_stays_unapproved(
    package, tmp_path
):
    value = receipt(approval.training_weight_source_fingerprint())
    assert (
        approval.TrainingWeightExecutionApprovalV1.from_json(value.to_json())
        == value
    )
    for operation in value.allowed_operations:
        approval.require_training_weight_approval(value, operation)
    assert not read_training_weight_preregistration().execution_approved
    with pytest.raises(FrozenInstanceError):
        value.coordinator_label = "changed"
    destination = tmp_path / "external-approval.json"
    destination.write_bytes(value.to_json().encode("ascii"))
    assert (
        approval.read_training_weight_execution_approval(destination) == value
    )
    destination.write_bytes(value.to_json().encode("ascii") + b"\n")
    with pytest.raises(ValueError, match="canonical"):
        approval.read_training_weight_execution_approval(destination)


@pytest.mark.parametrize("bad", [None, {}, "approved", True])
def test_missing_or_untyped_approval_refuses_before_fingerprinting(
    monkeypatch, bad
):
    monkeypatch.setattr(
        approval,
        "training_weight_source_fingerprint",
        lambda: pytest.fail("must not inspect package"),
    )
    with pytest.raises(ValueError, match="explicit coordinator approval"):
        approval.require_training_weight_approval(bad, "candidate-generation")


@pytest.mark.parametrize(
    "changes",
    [
        {"protocol_commit": "f" * 40},
        {"protocol_file_sha256": "f" * 64},
        {"source_fingerprint": "f" * 40},
        {"source_fingerprint": "F" * 64},
        {"allowed_operations": ()},
        {
            "allowed_operations": (
                "candidate-generation",
                "candidate-generation",
            )
        },
        {
            "allowed_operations": (
                "source-window-decoding",
                "candidate-generation",
            )
        },
        {"allowed_operations": ("publish-production",)},
        {"coordinator_label": " person "},
        {"coordinator_label": "x" * 129},
        {"approved_at_utc": "2026-09-19T12:34:56+00:00"},
        {"approved_at_utc": "2026-09-19T12:34:56.000Z"},
        {"approved_at_utc": "2026-02-30T12:34:56Z"},
        {"approved_at_utc": 20260919},
    ],
)
def test_policy_operations_time_and_scalar_types_are_strict(changes):
    with pytest.raises((ValueError, TypeError)):
        replace(receipt(), **changes)


def test_operation_subset_is_explicit_and_unknowns_refuse(package):
    value = receipt(
        approval.training_weight_source_fingerprint(), ("candidate-generation",)
    )
    approval.require_training_weight_approval(value, "candidate-generation")
    for operation in ("calibration-fit", "publish-production", True):
        with pytest.raises(ValueError):
            approval.require_training_weight_approval(value, operation)


def test_resealed_policy_drift_unknown_fields_and_coercion_refuse():
    value = receipt().to_dict()
    value["protocol_file_sha256"] = "f" * 64
    value.pop("artifact_id")
    value["artifact_id"] = (
        "training-weight-execution-approval:sha256:"
        + hashlib.sha256(training_json(value).encode()).hexdigest()
    )
    with pytest.raises(ValueError):
        approval.TrainingWeightExecutionApprovalV1.from_dict(value)
    for field, replacement in (
        ("extra", True),
        ("coordinator_label", 42),
        ("allowed_operations", [True]),
    ):
        altered = receipt().to_dict()
        altered[field] = replacement
        with pytest.raises(ValueError):
            approval.TrainingWeightExecutionApprovalV1.from_dict(altered)


@pytest.mark.parametrize(
    "relative", ["calculation.py", "assets/values.json", "assets/table.b64"]
)
def test_same_size_mtime_replacement_revokes_approval(package, relative):
    value = receipt(approval.training_weight_source_fingerprint())
    target = package / relative
    info = target.stat()
    payload = target.read_bytes()
    target.write_bytes(bytes([payload[0] ^ 1]) + payload[1:])
    os.utime(target, ns=(info.st_atime_ns, info.st_mtime_ns))
    with pytest.raises(ValueError, match="stale"):
        approval.require_training_weight_approval(value, "candidate-generation")


@pytest.mark.parametrize("mutation", ["add", "remove", "rename"])
def test_package_membership_changes_revoke_approval(package, mutation):
    value = receipt(approval.training_weight_source_fingerprint())
    target = package / "calculation.py"
    if mutation == "add":
        (package / "new.py").write_bytes(b"x=1\n")
    elif mutation == "remove":
        target.unlink()
    else:
        target.rename(package / "renamed.py")
    with pytest.raises(ValueError, match="stale"):
        approval.require_training_weight_approval(value, "candidate-generation")


def test_relative_inventory_relocates_and_ignores_only_derived_caches(
    package, tmp_path, monkeypatch
):
    original = approval.training_weight_source_fingerprint()
    cache = package / "__pycache__"
    cache.mkdir()
    (cache / "cache.json").write_bytes(b"not computational source")
    (package / "ignored.pyc").write_bytes(b"derived bytecode")
    assert approval.training_weight_source_fingerprint() == original
    relocated = tmp_path / "relocated"
    shutil.copytree(package, relocated)
    monkeypatch.setattr(approval, "_package_root", lambda: relocated)
    assert approval.training_weight_source_fingerprint() == original


@pytest.mark.parametrize("kind", ["file", "directory", "root"])
def test_symlink_paths_refuse(package, tmp_path, monkeypatch, kind):
    link = tmp_path / "alias" if kind == "root" else package / "alias.py"
    target = (
        package
        if kind == "root"
        else (
            package / "assets"
            if kind == "directory"
            else package / "calculation.py"
        )
    )
    try:
        link.symlink_to(target, target_is_directory=kind != "file")
    except NotImplementedError:
        pytest.skip("platform cannot create synthetic symlinks")
    except OSError as exc:
        if os.name == "nt" and getattr(exc, "winerror", None) == 1314:
            pytest.skip("platform cannot create synthetic symlinks")
        raise
    if kind == "root":
        monkeypatch.setattr(approval, "_package_root", lambda: link)
    with pytest.raises(ValueError, match="symlink|real directory"):
        approval.training_weight_source_fingerprint()


@pytest.mark.skipif(
    not hasattr(os, "mkfifo"), reason="FIFO fixture requires POSIX"
)
def test_fifo_refuses_without_opening(package, tmp_path):
    fifo = package / "bad.json"
    os.mkfifo(fifo)
    with pytest.raises(ValueError, match="regular file"):
        approval.training_weight_source_fingerprint()
    with pytest.raises(ValueError, match="regular file"):
        approval.read_training_weight_execution_approval(fifo)


@pytest.mark.parametrize(
    "limit,value",
    [
        ("MAX_FINGERPRINT_FILES", 1),
        ("MAX_FINGERPRINT_ENTRIES", 1),
        ("MAX_FINGERPRINT_FILE_BYTES", 1),
        ("MAX_FINGERPRINT_TOTAL_BYTES", 1),
    ],
)
def test_resource_bounds_refuse_before_file_content_reads(
    package, monkeypatch, limit, value
):
    monkeypatch.setattr(approval, limit, value)
    monkeypatch.setattr(
        approval,
        "_read_digest",
        lambda *_: pytest.fail("bounds must precede read"),
    )
    with pytest.raises(ValueError, match="bound"):
        approval.training_weight_source_fingerprint()


def test_inventory_race_and_content_growth_are_refused(package, monkeypatch):
    original = approval._read_digest
    invoked = False

    def changed(path, stamp):
        nonlocal invoked
        result = original(path, stamp)
        if not invoked:
            (package / "late.py").write_bytes(b"x=1\n")
            invoked = True
        return result

    monkeypatch.setattr(approval, "_read_digest", changed)
    with pytest.raises(ValueError, match="changed"):
        approval.training_weight_source_fingerprint()
    info = (package / "late.py").stat()
    (package / "late.py").write_bytes(b"longer content\n")
    with pytest.raises(ValueError, match="changed"):
        original(package / "late.py", approval._stamp(info))


def test_relative_path_budget_and_empty_package_refuse(
    package, tmp_path, monkeypatch
):
    monkeypatch.setattr(approval, "MAX_FINGERPRINT_PATH_CHARS", 5)
    with pytest.raises(ValueError, match="relative path"):
        approval.training_weight_source_fingerprint()
    empty = tmp_path / "empty"
    empty.mkdir()
    monkeypatch.setattr(approval, "_package_root", lambda: empty)
    with pytest.raises(ValueError, match="empty"):
        approval.training_weight_source_fingerprint()


@pytest.mark.parametrize(
    "change", ["python", "platform", "numeric-version", "absent-dependency"]
)
def test_runtime_or_dependency_drift_revokes_approval(
    package, monkeypatch, change
):
    real_version = approval.metadata.version
    monkeypatch.setattr(
        approval.metadata,
        "version",
        lambda name: "1.0.0" if name == "numpy" else real_version(name),
    )
    value = receipt(approval.training_weight_source_fingerprint())
    if change == "python":
        monkeypatch.setattr(
            approval.sys, "version", "different exact Python build"
        )
    elif change == "platform":
        monkeypatch.setattr(
            approval.platform, "release", lambda: "different-kernel"
        )
    else:
        original = approval.metadata.version

        def version(name):
            if name == "numpy":
                if change == "numeric-version":
                    return "999.0.0"
                raise approval.metadata.PackageNotFoundError(name)
            return original(name)

        monkeypatch.setattr(approval.metadata, "version", version)
    with pytest.raises(ValueError, match="stale"):
        approval.require_training_weight_approval(value, "candidate-generation")


def test_reader_refuses_symlink_directory_and_oversized_external_receipt(
    tmp_path,
):
    value = receipt()
    target = tmp_path / "approval.json"
    target.write_bytes(value.to_json().encode("ascii"))
    with pytest.raises(ValueError, match="regular file"):
        approval.read_training_weight_execution_approval(tmp_path)
    link = tmp_path / "alias.json"
    try:
        link.symlink_to(target)
    except NotImplementedError:
        pass
    except OSError as exc:
        if not (os.name == "nt" and getattr(exc, "winerror", None) == 1314):
            raise
    else:
        with pytest.raises(ValueError, match="regular file"):
            approval.read_training_weight_execution_approval(link)
    target.write_bytes(b" " * (approval.MAX_APPROVAL_BYTES + 1))
    with pytest.raises(ValueError, match="bounded"):
        approval.read_training_weight_execution_approval(target)


@pytest.mark.parametrize("code", [errno.EIO, errno.ENOSPC])
@pytest.mark.parametrize("combined", [False, True])
def test_symlink_fixture_does_not_hide_unrelated_oserror(
    package, tmp_path, monkeypatch, code, combined
):
    error = OSError(code, "synthetic filesystem failure")

    def fail(*args, **kwargs):
        raise error

    monkeypatch.setattr(Path, "symlink_to", fail)
    with pytest.raises(OSError) as raised:
        if combined:
            test_reader_refuses_symlink_directory_and_oversized_external_receipt(
                tmp_path
            )
        else:
            test_symlink_paths_refuse(package, tmp_path, monkeypatch, "file")
    assert raised.value is error


def test_unsupported_symlink_keeps_directory_and_size_refusal_coverage(
    tmp_path, monkeypatch
):
    def unsupported(*args, **kwargs):
        raise NotImplementedError

    monkeypatch.setattr(Path, "symlink_to", unsupported)
    test_reader_refuses_symlink_directory_and_oversized_external_receipt(
        tmp_path
    )
    assert (
        tmp_path / "approval.json"
    ).stat().st_size > approval.MAX_APPROVAL_BYTES
