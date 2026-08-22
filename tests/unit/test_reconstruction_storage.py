"""Tests for fail-closed reconstruction storage-root identity guards."""

from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from histdatacom.orchestration.reconstruction import (
    ReconstructionArtifactError,
    ReconstructionStage,
    ReconstructionStageCommandV1,
    execute_reconstruction_stage,
)
from histdatacom.reconstruction_storage import (
    RECONSTRUCTION_PLAN_EXECUTION_MANIFEST_ARTIFACT_KIND,
    RECONSTRUCTION_STORAGE_ROOT_GUARD_MARKER,
    ReconstructionStorageRootError,
    create_reconstruction_storage_root_guard,
    verify_reconstruction_storage_for_execution,
    verify_reconstruction_storage_root_guard,
)
from histdatacom.runtime_contracts import ArtifactRef


def _execution_ref(
    path: Path,
    *,
    output_root: Path,
    scratch_root: Path,
    guard_ref: ArtifactRef,
) -> ArtifactRef:
    payload = {
        "schema_version": "histdatacom.reconstruction-plan-execution-manifest.v1",
        "output_root": str(output_root.resolve()),
        "scratch_root": str(scratch_root.resolve()),
        "artifacts": {"storage_root_guard": guard_ref.to_dict()},
    }
    encoded = (
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
        + b"\n"
    )
    path.write_bytes(encoded)
    return ArtifactRef(
        kind=RECONSTRUCTION_PLAN_EXECUTION_MANIFEST_ARTIFACT_KIND,
        path=str(path.resolve()),
        size_bytes=len(encoded),
        sha256=hashlib.sha256(encoded).hexdigest(),
    )


def test_storage_root_guard_binds_identical_markers_and_device(
    tmp_path: Path,
) -> None:
    output = tmp_path / "mounted" / "output"
    scratch = tmp_path / "mounted" / "scratch"
    guard, ref = create_reconstruction_storage_root_guard(
        output_root=output,
        scratch_root=scratch,
        artifact_root=tmp_path / "artifacts",
    )

    assert verify_reconstruction_storage_root_guard(ref) == guard
    artifact_bytes = Path(ref.path).read_bytes()
    assert (output / RECONSTRUCTION_STORAGE_ROOT_GUARD_MARKER).read_bytes() == (
        artifact_bytes
    )
    assert (
        scratch / RECONSTRUCTION_STORAGE_ROOT_GUARD_MARKER
    ).read_bytes() == (artifact_bytes)
    assert output.stat().st_dev == scratch.stat().st_dev


def test_detached_mount_fails_before_same_named_fallback_is_created(
    tmp_path: Path,
) -> None:
    mount = tmp_path / "mount"
    output = mount / "output"
    scratch = mount / "scratch"
    _, guard_ref = create_reconstruction_storage_root_guard(
        output_root=output,
        scratch_root=scratch,
        artifact_root=tmp_path / "artifacts",
    )
    execution_ref = _execution_ref(
        tmp_path / "execution.json",
        output_root=output,
        scratch_root=scratch,
        guard_ref=guard_ref,
    )
    verify_reconstruction_storage_for_execution(execution_ref)

    mount.rename(tmp_path / "detached-volume")
    mount.mkdir()
    fallback_window = scratch / "run" / "member" / "window"
    command = ReconstructionStageCommandV1(
        stage=ReconstructionStage.SOURCE_ENRICHMENT,
        handler_name="must-not-run",
        receipt_path=str(fallback_window / "receipts" / "source.json"),
        configuration_refs=(execution_ref,),
    )
    invocation = SimpleNamespace(
        command=command,
        prior_outcomes=(),
        task=SimpleNamespace(scratch_directory=str(fallback_window)),
    )

    with pytest.raises(
        ReconstructionArtifactError, match="guarded storage root"
    ):
        asyncio.run(execute_reconstruction_stage(invocation))

    assert not (mount / "output").exists()
    assert not (mount / "scratch").exists()
    assert tuple(mount.iterdir()) == ()


def test_storage_root_guard_rejects_marker_tampering(tmp_path: Path) -> None:
    output = tmp_path / "output"
    scratch = tmp_path / "scratch"
    _, ref = create_reconstruction_storage_root_guard(
        output_root=output,
        scratch_root=scratch,
        artifact_root=tmp_path / "artifacts",
    )
    (scratch / RECONSTRUCTION_STORAGE_ROOT_GUARD_MARKER).write_text(
        "{}\n", encoding="utf-8"
    )

    with pytest.raises(ReconstructionStorageRootError, match="marker differs"):
        verify_reconstruction_storage_root_guard(ref)
