"""Metadata-only diagnostic protocol tests; no empirical inputs or approvals."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
import hashlib
import os
from pathlib import Path
import subprocess
from typing import Any

import pytest

from histdatacom.data_quality import training_weight_diagnostic_protocol as d
from histdatacom.data_quality.training_weight_approval import (
    TRAINING_WEIGHT_POLICY_COMMIT,
    TrainingWeightExecutionApprovalV1,
)
from histdatacom.data_quality.training_weight_candidates import (
    TrainingWeightFileV1,
)
from histdatacom.data_quality.training_weight_contracts import (
    PREREGISTRATION_FILE_SHA256,
    _integer,
    _object,
    _text,
    read_training_weight_preregistration,
)


def _file(path: Path, label: str) -> TrainingWeightFileV1:
    return TrainingWeightFileV1(
        str(path), 100, hashlib.sha256(label.encode()).hexdigest()
    )


def _fixture_plan(tmp_path: Path) -> d.TrainingWeightDiagnosticPlanV1:
    root = tmp_path / d.DIAGNOSTIC_FIXTURE_COMPONENT
    entries = []
    for period in d.DIAGNOSTIC_PERIODS:
        for symbol in d.DIAGNOSTIC_SYMBOLS:
            relative = f"{symbol.lower()}/{period[:4]}/{int(period[4:])}/.data"
            entries.append(
                d.TrainingWeightDiagnosticInputV1(
                    d.TrainingWeightDiagnosticInputRole.HISTORICAL_SOURCE,
                    relative,
                    _file(root / relative, relative),
                    symbol,
                    period,
                    100,
                )
            )
    for symbol in d.DIAGNOSTIC_SYMBOLS:
        relative = f"model/{symbol.lower()}.arrow"
        entries.append(
            d.TrainingWeightDiagnosticInputV1(
                d.TrainingWeightDiagnosticInputRole.MODEL_TRAINING_SOURCE,
                relative,
                _file(root / relative, relative),
                symbol,
                "202001",
            )
        )
    entries.append(
        d.TrainingWeightDiagnosticInputV1(
            d.TrainingWeightDiagnosticInputRole.MODEL_INDEX,
            "model/index.json",
            _file(root / "model/index.json", "index"),
        )
    )
    return d.TrainingWeightDiagnosticPlanV1(
        d.TrainingWeightDiagnosticEvidenceKind.FIXTURE,
        "synthetic-diagnostic-001",
        tuple(sorted(entries, key=lambda e: (e.role.value, e.relative_path))),
    )


def _declared_plan(tmp_path: Path) -> d.TrainingWeightDiagnosticPlanV1:
    # Declare immutable asset metadata without opening any referenced input.
    return d.TrainingWeightDiagnosticPlanV1(
        d.TrainingWeightDiagnosticEvidenceKind.DIAGNOSTIC,
        "metadata-test-not-an-execution-approval",
        tuple(
            d.TrainingWeightDiagnosticInputV1(
                d.TrainingWeightDiagnosticInputRole(role),
                relative,
                TrainingWeightFileV1(
                    str(tmp_path / relative),
                    _integer(item["size_bytes"]),
                    _text(item["sha256"]),
                ),
                _text(item["symbol"]),
                _text(item["period"]),
                _integer(item["row_count"]),
            )
            for (role, relative), item in sorted(d._known_inputs().items())
        ),
    )


def _structural_receipt(
    plan: d.TrainingWeightDiagnosticPlanV1,
) -> d.TrainingWeightDiagnosticApprovalV1:
    # Synthetic declaration only: no real package fingerprint or existing commit.
    # It is never written outside pytest's disposable temporary directory.
    return d.TrainingWeightDiagnosticApprovalV1(
        protocol_commit="a" * 40,
        protocol_file_sha256=d.DIAGNOSTIC_PROTOCOL_FILE_SHA256,
        protocol_git_blob_sha1=d.DIAGNOSTIC_PROTOCOL_GIT_BLOB_SHA1,
        source_fingerprint="b" * 64,
        plan_id=plan.artifact_id,
        allowed_operations=("diagnostic-input-verification",),
        coordinator_label="synthetic-contract-test-only",
        approved_at_utc="2026-01-01T00:00:00Z",
    )


def test_exact_protocol_schedule_limits_and_old_asset_preserved() -> None:
    protocol = d.read_training_weight_diagnostic_protocol()
    old = read_training_weight_preregistration()
    payload = protocol.to_dict()
    assert protocol.version == "1.0.0"
    assert not protocol.execution_approved and not old.execution_approved
    assert protocol.scheduled_dates() == (
        old.scheduled_dates("calibration") + old.scheduled_dates("application")
    )
    assert len(set(protocol.scheduled_dates())) == 62
    assert len(protocol.scheduled_dates("201001")) == 21
    assert protocol.selected_dates() == (
        "2010-01-04",
        "2010-01-15",
        "2011-01-04",
        "2011-02-02",
    )
    assert _object(payload["source_census"])["scheduled_cells"] == 186
    assert _object(payload["generator_diagnosis"])["scheduled_cells"] == 48
    assert payload["limits"] == {
        "maximum_source_partition_bytes": 134217728,
        "maximum_source_rows_per_month": 2000000,
        "maximum_model_index_bytes": 2097152,
        "maximum_model_source_bytes": 2147483648,
        "maximum_artifact_bytes": 8388608,
        "maximum_collection_items": 4096,
        "maximum_depth": 16,
        "maximum_total_expanded_nodes": 131072,
        "maximum_native_events_per_day_member": 4096,
        "maximum_days_per_shard": 8,
        "source_month_runtime_seconds": 300,
        "generator_day_runtime_seconds": 120,
        "maximum_attempt_runtime_seconds": 10800,
    }
    assert (
        d.TrainingWeightDiagnosticProtocolV1.from_json(protocol.to_json())
        == protocol
    )
    payload["version"] = "1.0.1"
    with pytest.raises(ValueError, match="frozen"):
        d.TrainingWeightDiagnosticProtocolV1.from_dict(payload)
    assert protocol.to_dict()["version"] == "1.0.0"
    with pytest.raises(ValueError, match="period"):
        protocol.scheduled_dates("201201")


def test_fixture_plan_roundtrip_immutable_and_no_input_io(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail(*args: object, **kwargs: object) -> None:
        pytest.fail("metadata construction must not verify input files")

    monkeypatch.setattr(TrainingWeightFileV1, "verify", fail)
    plan = _fixture_plan(tmp_path)
    assert len(plan.inputs) == 13
    assert not tmp_path.joinpath(d.DIAGNOSTIC_FIXTURE_COMPONENT).exists()
    assert d.TrainingWeightDiagnosticPlanV1.from_json(plan.to_json()) == plan
    for operation in d.DIAGNOSTIC_ALLOWED_OPERATIONS:
        d.require_training_weight_diagnostic_approval(None, operation, plan)
    with pytest.raises(FrozenInstanceError):
        plan.attempt_label = "changed"  # type: ignore[misc]
    with pytest.raises(ValueError, match="unknown or missing"):
        d.TrainingWeightDiagnosticPlanV1.from_dict(
            {**plan.to_dict(), "extra": 1}
        )
    with pytest.raises(ValueError, match="cannot claim approval"):
        d.require_training_weight_diagnostic_approval(
            _structural_receipt(plan), "diagnostic-input-verification", plan
        )


def test_exact_real_allowlist_is_metadata_only(tmp_path: Path) -> None:
    plan = _declared_plan(tmp_path)
    assert len(plan.inputs) == 26
    assert sum(x.row_count for x in plan.inputs) == 2_989_245
    assert (
        sum(
            x.file.size_bytes
            for x in plan.inputs_for(
                d.TrainingWeightDiagnosticInputRole.HISTORICAL_SOURCE
            )
        )
        == 83_714_359
    )
    assert not tuple(tmp_path.iterdir())
    assert d.TrainingWeightDiagnosticPlanV1.from_json(plan.to_json()) == plan
    with pytest.raises(ValueError, match="approval is required"):
        d.require_training_weight_diagnostic_approval(
            None, "diagnostic-input-verification", plan
        )
    first = plan.inputs[0]
    altered = replace(first, file=replace(first.file, sha256="d" * 64))
    with pytest.raises(ValueError, match="identity differs"):
        replace(plan, inputs=(altered,) + plan.inputs[1:])


@pytest.mark.parametrize(
    "defect", ["missing", "duplicate", "reversed", "locator", "month-rows"]
)
def test_plan_complete_inventory_and_bounds(
    tmp_path: Path, defect: str
) -> None:
    plan = _fixture_plan(tmp_path)
    entries = list(plan.inputs)
    if defect == "missing":
        entries.pop()
    elif defect == "duplicate":
        entries.append(entries[-1])
    elif defect == "reversed":
        entries.reverse()
    elif defect == "locator":
        entries[0] = replace(
            entries[0],
            file=replace(entries[0].file, path=entries[-1].file.path),
        )
    else:
        entries = [
            replace(e, row_count=1_000_000) if e.period == "201001" else e
            for e in entries
        ]
    with pytest.raises(ValueError):
        replace(plan, inputs=tuple(entries))


@pytest.mark.parametrize("role", list(d.TrainingWeightDiagnosticInputRole))
def test_fixture_rejects_every_known_empirical_hash(
    tmp_path: Path, role: d.TrainingWeightDiagnosticInputRole
) -> None:
    plan = _fixture_plan(tmp_path)
    known = next(
        v["sha256"]
        for (r, _), v in d._known_inputs().items()
        if r == role.value
    )
    first = replace(
        plan.inputs[0], file=replace(plan.inputs[0].file, sha256=_text(known))
    )
    with pytest.raises(ValueError, match="empirical relabel"):
        replace(plan, inputs=(first,) + plan.inputs[1:])


def test_fixture_rejects_real_locator_without_fixture_marker(
    tmp_path: Path,
) -> None:
    plan = _fixture_plan(tmp_path)
    first = plan.inputs[0]
    changed = replace(
        first,
        file=replace(first.file, path=str(tmp_path / first.relative_path)),
    )
    with pytest.raises(ValueError, match="empirical relabel"):
        replace(plan, inputs=(changed,) + plan.inputs[1:])


@pytest.mark.parametrize(
    "defect",
    ["suffix", "relative", "parent", "bool-count", "period", "model-fields"],
)
def test_input_refuses_coercion_and_path_aliases(
    tmp_path: Path, defect: str
) -> None:
    plan = _fixture_plan(tmp_path)
    item = plan.inputs_for(
        d.TrainingWeightDiagnosticInputRole.HISTORICAL_SOURCE
    )[0]
    changes: dict[str, Any] = {}
    if defect == "suffix":
        changes["file"] = replace(
            item.file, path=str(tmp_path / "different.data")
        )
    elif defect == "relative":
        changes["relative_path"] = "../" + item.relative_path
    elif defect == "parent":
        changes["file"] = replace(
            item.file, path=str(tmp_path) + "/../" + item.relative_path
        )
    elif defect == "bool-count":
        changes["row_count"] = True
    elif defect == "period":
        changes["period"] = "201013"
    else:
        changes["role"] = d.TrainingWeightDiagnosticInputRole.MODEL_INDEX
    with pytest.raises(ValueError):
        replace(item, **changes)


def test_approval_exact_attempt_least_privilege_and_fresh_runtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan = _declared_plan(tmp_path)
    receipt = _structural_receipt(plan)
    calls = []

    def fingerprint() -> str:
        calls.append(True)
        return "b" * 64

    monkeypatch.setattr(d, "training_weight_source_fingerprint", fingerprint)
    for _ in range(2):
        d.require_training_weight_diagnostic_approval(
            receipt, "diagnostic-input-verification", plan
        )
    assert len(calls) == 2
    with pytest.raises(ValueError, match="outside coordinator"):
        d.require_training_weight_diagnostic_approval(
            receipt, "diagnostic-generator-trace", plan
        )
    for operation in (
        "candidate-generation",
        "calibration-fit",
        "application-outcome-inspection",
        "policy-comparison",
        "unknown",
    ):
        with pytest.raises(ValueError, match="unsupported"):
            d.require_training_weight_diagnostic_approval(
                receipt, operation, plan
            )
    with pytest.raises(ValueError, match="another attempt"):
        d.require_training_weight_diagnostic_approval(
            receipt,
            "diagnostic-input-verification",
            replace(plan, attempt_label="different-attempt"),
        )
    monkeypatch.setattr(
        d, "training_weight_source_fingerprint", lambda: "c" * 64
    )
    with pytest.raises(ValueError, match="stale"):
        d.require_training_weight_diagnostic_approval(
            receipt, "diagnostic-input-verification", plan
        )
    assert not d.read_training_weight_diagnostic_protocol().execution_approved


@pytest.mark.parametrize(
    "changes",
    [
        {"protocol_commit": "0" * 40},
        {"protocol_commit": "main"},
        {"protocol_file_sha256": "0" * 64},
        {"protocol_git_blob_sha1": "0" * 40},
        {"protocol_id": "old"},
        {"source_fingerprint": "B" * 64},
        {"plan_id": "old"},
        {"allowed_operations": ()},
        {"allowed_operations": ("calibration-fit",)},
        {
            "allowed_operations": (
                "diagnostic-source-census",
                "diagnostic-input-verification",
            )
        },
        {
            "allowed_operations": (
                "diagnostic-source-census",
                "diagnostic-source-census",
            )
        },
        {"coordinator_label": " trailing "},
        {"approved_at_utc": "2026-02-30T00:00:00Z"},
        {"approved_at_utc": "2026-01-01T00:00:00+00:00"},
    ],
)
def test_approval_closed_canonical_fields(
    tmp_path: Path, changes: dict[str, Any]
) -> None:
    receipt = _structural_receipt(_declared_plan(tmp_path))
    with pytest.raises(ValueError):
        replace(receipt, **changes)


def test_old_approval_and_schema_cannot_grant_diagnostics(
    tmp_path: Path,
) -> None:
    plan = _declared_plan(tmp_path)
    old_receipt = TrainingWeightExecutionApprovalV1(
        TRAINING_WEIGHT_POLICY_COMMIT,
        PREREGISTRATION_FILE_SHA256,
        "b" * 64,
        ("source-window-decoding",),
        "synthetic-contract-test-only",
        "2026-01-01T00:00:00Z",
    )
    with pytest.raises(ValueError, match="approval is required"):
        d.require_training_weight_diagnostic_approval(old_receipt, "diagnostic-input-verification", plan)  # type: ignore[arg-type]
    receipt = _structural_receipt(plan)
    payload = receipt.to_dict()
    payload["schema_version"] = (
        "histdatacom.training-weight-execution-approval.v1"
    )
    with pytest.raises(ValueError, match="schema"):
        d.TrainingWeightDiagnosticApprovalV1.from_dict(payload)


def test_diagnostic_json_shared_expansion_and_limits() -> None:
    assert d.diagnostic_load(d.diagnostic_json({"v": [[0] * 4096] * 31}))
    with pytest.raises(ValueError, match="traversal"):
        d.diagnostic_json({"v": [[0] * 4096] * 32})
    with pytest.raises(ValueError, match="collection"):
        d.diagnostic_json({"v": [0] * 4097})
    with pytest.raises(ValueError, match="budget"):
        d.diagnostic_json({"v": "\u0000" * 1_500_000})
    cycle: list[object] = []
    cycle.append(cycle)
    with pytest.raises(ValueError, match="nesting"):
        d.diagnostic_json({"v": cycle})
    with pytest.raises(ValueError):
        d.diagnostic_load('{"duplicate":1,"duplicate":2}')


def test_approval_reader_exact_canonical_regular_file(tmp_path: Path) -> None:
    receipt = _structural_receipt(_declared_plan(tmp_path))
    path = tmp_path / "synthetic-receipt.json"
    path.write_text(receipt.to_json(), encoding="ascii")
    assert d.read_training_weight_diagnostic_approval(path) == receipt
    path.write_text(receipt.to_json() + "\n", encoding="ascii")
    with pytest.raises(ValueError, match="canonical"):
        d.read_training_weight_diagnostic_approval(path)
    path.write_bytes(b"x" * (d.MAX_DIAGNOSTIC_METADATA_BYTES + 1))
    with pytest.raises(ValueError, match="bounded regular"):
        d.read_training_weight_diagnostic_approval(path)
    with pytest.raises(ValueError, match="bounded regular"):
        d.read_training_weight_diagnostic_approval(tmp_path)


def test_approval_reader_refuses_symlink(tmp_path: Path) -> None:
    target = tmp_path / "target.json"
    target.write_text("{}", encoding="ascii")
    link = tmp_path / "link.json"
    try:
        link.symlink_to(target)
    except NotImplementedError:
        pytest.skip("symlinks unavailable")
    except OSError as exc:
        if os.name == "nt" and getattr(exc, "winerror", None) == 1314:
            pytest.skip("Windows symlink privilege unavailable")
        raise
    with pytest.raises(ValueError, match="bounded regular"):
        d.read_training_weight_diagnostic_approval(link)


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="FIFO unavailable")
def test_approval_reader_refuses_fifo_before_open(tmp_path: Path) -> None:
    fifo = tmp_path / "receipt.fifo"
    os.mkfifo(fifo)
    with pytest.raises(ValueError, match="bounded regular"):
        d.read_training_weight_diagnostic_approval(fifo)


def test_local_git_verifier_is_exact_read_only_and_not_approval(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data = (
        Path(d.__file__)
        .with_name("assets")
        .joinpath(d.DIAGNOSTIC_PROTOCOL_FILENAME)
        .read_bytes()
    )
    calls: list[list[str]] = []

    def git(
        command: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[bytes]:
        calls.append(command)
        assert kwargs["timeout"] == 5
        assert kwargs["env"]["GIT_NO_REPLACE_OBJECTS"] == "1"
        if command[3:5] == ["cat-file", "-t"]:
            result = b"commit\n"
        elif command[3] == "rev-parse":
            result = d.DIAGNOSTIC_PROTOCOL_GIT_BLOB_SHA1.encode() + b"\n"
        elif command[3:5] == ["cat-file", "-s"]:
            result = str(len(data)).encode() + b"\n"
        else:
            assert command[3:5] == ["cat-file", "blob"]
            result = data
        return subprocess.CompletedProcess(command, 0, stdout=result)

    monkeypatch.setattr(d.subprocess, "run", git)
    d.verify_training_weight_diagnostic_protocol_commit(tmp_path, "a" * 40)
    assert len(calls) == 4
    assert not d.read_training_weight_diagnostic_protocol().execution_approved
    monkeypatch.setattr(
        d.subprocess,
        "run",
        lambda *a, **k: subprocess.CompletedProcess(a, 0, stdout=b"different"),
    )
    with pytest.raises(ValueError, match="not a commit"):
        d.verify_training_weight_diagnostic_protocol_commit(tmp_path, "a" * 40)
    with pytest.raises(ValueError, match="full Git"):
        d.verify_training_weight_diagnostic_protocol_commit(
            tmp_path, "HEAD:secret"
        )
