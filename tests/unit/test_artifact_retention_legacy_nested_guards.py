"""Tiny real filesystem regressions for derived legacy mutation targets.

These are cooperative refusal/ordinary-I/O checks, not managed GC authority or
scientific execution. Invalid marker bytes must protect the namespace too.
"""

from __future__ import annotations

import hashlib
import importlib
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from histdatacom import managed_artifact_boundary as boundary
from histdatacom.manifest_store import ManifestStatusStore
from histdatacom.records import Record
from histdatacom.runtime_contracts import ArtifactRef


def _mark(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    (path / boundary.MANAGED_ARTIFACT_MARKER).write_bytes(b"invalid marker")
    (path / "protected.bin").write_bytes(b"generated protected bytes")
    return path


def _snapshot(root: Path) -> dict[str, tuple[str, bytes | str]]:
    return {
        item.relative_to(root).as_posix(): (
            ("symlink", item.readlink().as_posix())
            if item.is_symlink()
            else (
                ("directory", b"")
                if item.is_dir()
                else ("file", item.read_bytes())
            )
        )
        for item in sorted(root.rglob("*"))
    }


@pytest.mark.parametrize("alias", (False, True))
def test_status_constructor_admits_actual_nested_database(
    tmp_path: Path, alias: bool
) -> None:
    ordinary = tmp_path / "ordinary"
    ordinary.mkdir()
    db_parent = ManifestStatusStore.path_for_root(ordinary).parent
    if alias:
        db_parent.symlink_to(
            _mark(tmp_path / "managed"), target_is_directory=True
        )
    else:
        _mark(db_parent)
    before = _snapshot(tmp_path)
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        ManifestStatusStore(ordinary)
    assert _snapshot(tmp_path) == before


def test_existing_status_instance_rechecks_actual_database(
    tmp_path: Path,
) -> None:
    store = ManifestStatusStore(tmp_path / "ordinary")
    _mark(store.db_path.parent)
    before = _snapshot(tmp_path)
    record = Record(data_dir=str(tmp_path / "ordinary" / "data"))
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        store.write_record(record)
    assert _snapshot(tmp_path) == before


@pytest.mark.parametrize("explicit_root", (False, True))
def test_status_pair_refuses_before_meta_unlink(
    tmp_path: Path, explicit_root: bool
) -> None:
    root = tmp_path / "ordinary"
    store = ManifestStatusStore(root)
    data = root / "data" / "child"
    data.mkdir(parents=True)
    meta = data / ".meta"
    meta.write_bytes(b"legacy metadata must remain")
    record = Record(data_dir=str(data))
    store.write_record(record)
    _mark(store.db_path.parent)
    before = _snapshot(tmp_path)
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        record.delete_manifest_status(
            base_dir=str(root) if explicit_root else ""
        )
    assert _snapshot(tmp_path) == before


def test_status_pair_admits_meta_alias_before_database(tmp_path: Path) -> None:
    root = tmp_path / "ordinary"
    root.mkdir()
    protected = _mark(tmp_path / "managed") / "protected.bin"
    (root / ".meta").symlink_to(protected)
    before = _snapshot(tmp_path)
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        Record(data_dir=str(root)).delete_manifest_status()
    assert _snapshot(tmp_path) == before


def test_unmanaged_status_pair_still_deletes_both_members(
    tmp_path: Path,
) -> None:
    root = tmp_path / "ordinary"
    store = ManifestStatusStore(root)
    data = root / "data"
    data.mkdir()
    meta = data / ".meta"
    meta.write_bytes(b"legacy metadata")
    record = Record(data_dir=str(data))
    item = store.write_record(record)
    assert store.get_work_item(item.work_id) is not None
    record.delete_manifest_status(base_dir=str(root))
    assert not meta.exists()
    assert store.get_work_item(item.work_id) is None


@pytest.mark.parametrize(
    "module_name,function_name",
    (
        ("release_holdout", "_write_once"),
        ("release_holdout", "_reserve_once"),
        ("release_holdout_evaluation", "_reserve_once"),
    ),
)
def test_holdout_low_level_writes_refuse_nested_namespace(
    tmp_path: Path, module_name: str, function_name: str
) -> None:
    module = importlib.import_module("histdatacom.synthetic." + module_name)
    managed = _mark(tmp_path / "ordinary" / "nested")
    before = _snapshot(tmp_path)
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        getattr(module, function_name)(managed / "state.json", b"{}\n")
    assert _snapshot(tmp_path) == before


@pytest.mark.parametrize(
    "module_name", ("release_holdout", "release_holdout_evaluation")
)
def test_holdout_contract_refuses_before_mkdir(
    tmp_path: Path, module_name: str
) -> None:
    module = importlib.import_module("histdatacom.synthetic." + module_name)
    managed = _mark(tmp_path / "ordinary" / "nested")
    before = _snapshot(tmp_path)
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        module._write_contract(
            "{}", managed / "new", prefix="test", kind="test", metadata={}
        )
    assert _snapshot(tmp_path) == before


@pytest.mark.parametrize(
    "module_name", ("release_holdout", "release_holdout_evaluation")
)
def test_holdout_ordinary_contract_and_reservation_still_write(
    tmp_path: Path, module_name: str
) -> None:
    module = importlib.import_module("histdatacom.synthetic." + module_name)
    root = tmp_path / module_name
    ref = module._write_contract(
        "{}", root, prefix="test", kind="test", metadata={}
    )
    assert Path(ref.path).read_bytes() == b"{}\n"
    assert ref.sha256 == hashlib.sha256(b"{}\n").hexdigest()
    reservation = root / "reservation.json"
    module._reserve_once(reservation, b"reserved\n")
    assert reservation.read_bytes() == b"reserved\n"
    with pytest.raises(module.ReleaseHoldoutAlreadyConsumedError):
        module._reserve_once(reservation, b"reopened\n")
    assert reservation.read_bytes() == b"reserved\n"


@pytest.mark.parametrize(
    "module_name", ("release_holdout", "release_holdout_evaluation")
)
def test_holdout_atomic_replace_admits_actual_target_alias(
    tmp_path: Path, module_name: str
) -> None:
    module = importlib.import_module("histdatacom.synthetic." + module_name)
    protected = _mark(tmp_path / "managed") / "protected.bin"
    ordinary = tmp_path / "ordinary"
    ordinary.mkdir()
    target = ordinary / "state.json"
    target.symlink_to(protected)
    before = _snapshot(tmp_path)
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        module._atomic_replace(target, b"new\n")
    assert _snapshot(tmp_path) == before


@pytest.mark.parametrize(
    "module_name,function_name",
    (
        ("release_holdout", "execute_release_holdout_once"),
        (
            "release_holdout_evaluation",
            "execute_reconstruction_release_holdout_once",
        ),
    ),
)
def test_public_holdout_admits_state_before_authorization_or_evaluation(
    tmp_path: Path, module_name: str, function_name: str
) -> None:
    module = importlib.import_module("histdatacom.synthetic." + module_name)
    managed = _mark(tmp_path / "ordinary" / "nested")
    before = _snapshot(tmp_path)

    def poison(*args: object) -> None:
        raise AssertionError("managed state must refuse before evaluation")

    # Deliberately invalid authorization is an early-admission poison, never a
    # successful scientific fixture or bypass of the real native verifier.
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        getattr(module, function_name)(
            None,
            managed / "new",
            poison,
            evaluated_at_utc="2026-01-01T00:00:00Z",
        )
    assert _snapshot(tmp_path) == before


@pytest.mark.parametrize("alias", (False, True))
def test_real_stage_json_writer_refuses_nested_stage_before_verification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, alias: bool
) -> None:
    from histdatacom.synthetic import reconstruction_handlers as handlers

    root = tmp_path / "scratch"
    root.mkdir()
    stage = root / "derived"
    if alias:
        stage.symlink_to(_mark(tmp_path / "managed"), target_is_directory=True)
    else:
        _mark(stage)
    invocation = SimpleNamespace(
        task=SimpleNamespace(scratch_directory=str(root))
    )
    before = _snapshot(tmp_path)

    def poison(*args: object) -> None:
        raise AssertionError(
            "derived managed stage must refuse before storage replay"
        )

    monkeypatch.setattr(
        handlers, "verify_reconstruction_storage_for_execution", poison
    )
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        handlers._write_json_artifact(
            invocation, "derived", "test", {"synthetic": True}
        )
    assert _snapshot(tmp_path) == before


def _source(tmp_path: Path) -> ArtifactRef:
    path = tmp_path / "source.arrow"
    payload = (
        b"tiny generated byte-copy fixture; not scientific Arrow evidence\n"
    )
    path.write_bytes(payload)
    return ArtifactRef(
        kind="test-source",
        path=str(path),
        size_bytes=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
    )


@pytest.mark.parametrize("alias", (False, True))
def test_portable_source_admits_real_derived_copy_directory(
    tmp_path: Path, alias: bool
) -> None:
    from histdatacom.synthetic import persistence

    source = _source(tmp_path)
    root = tmp_path / "ordinary"
    product = root / persistence.RECONSTRUCTION_PRODUCT_DIRECTORY
    product.mkdir(parents=True)
    target = product / "source-artifacts"
    if alias:
        target.symlink_to(_mark(tmp_path / "managed"), target_is_directory=True)
    else:
        _mark(target)
    before = _snapshot(tmp_path)
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        persistence._materialize_portable_source_artifacts(
            root, {"series": source}
        )
    assert _snapshot(tmp_path) == before


def test_portable_source_unmanaged_exact_copy_still_works(
    tmp_path: Path,
) -> None:
    from histdatacom.synthetic import persistence

    source = _source(tmp_path)
    root = tmp_path / "ordinary"
    result = persistence._materialize_portable_source_artifacts(
        root, {"series": source}
    )
    retained = result["series"]
    path = root / persistence.RECONSTRUCTION_PRODUCT_DIRECTORY / retained.path
    assert path.read_bytes() == Path(source.path).read_bytes()
    assert retained.sha256 == source.sha256
    assert retained.size_bytes == source.size_bytes
    assert tuple(path.parent.iterdir()) == (path,)


@pytest.mark.parametrize("alias", (False, True))
def test_readiness_writer_guards_actual_derived_directory(
    tmp_path: Path, alias: bool
) -> None:
    from histdatacom.orchestration import readiness
    from histdatacom.orchestration.queues import TaskQueueLane

    state = tmp_path / "ordinary"
    state.mkdir()
    derived = readiness.worker_readiness_dir(state)
    if alias:
        derived.symlink_to(
            _mark(tmp_path / "managed"), target_is_directory=True
        )
    else:
        _mark(derived)
    before = _snapshot(tmp_path)
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        readiness.write_worker_readiness_payload(
            state, next(iter(TaskQueueLane)), {"synthetic": True}
        )
    assert _snapshot(tmp_path) == before


def test_readiness_unmanaged_write_still_works(tmp_path: Path) -> None:
    from histdatacom.orchestration import readiness
    from histdatacom.orchestration.queues import TaskQueueLane

    lane = next(iter(TaskQueueLane))
    result = readiness.write_worker_readiness_payload(
        tmp_path, lane, {"synthetic": True}
    )
    assert result["synthetic"] is True
    assert readiness.worker_readiness_path(tmp_path, lane).is_file()


def test_supervisor_state_writer_refuses_pid_alias(tmp_path: Path) -> None:
    from histdatacom.orchestration.supervisor import OrchestrationSupervisor

    protected = _mark(tmp_path / "managed") / "protected.bin"
    pid = tmp_path / "pid.json"
    pid.symlink_to(protected)
    subject = SimpleNamespace(paths=SimpleNamespace(pid_file=pid))
    before = _snapshot(tmp_path)
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        OrchestrationSupervisor._write_state(subject, {"synthetic": True})
    assert _snapshot(tmp_path) == before


def test_supervisor_log_refuses_before_process_or_mkdir(tmp_path: Path) -> None:
    from histdatacom.orchestration.supervisor import OrchestrationSupervisor

    managed = _mark(tmp_path / "ordinary" / "logs")
    before = _snapshot(tmp_path)

    def poison(*args: object, **kwargs: object) -> None:
        raise AssertionError("managed log must refuse before process launch")

    subject = SimpleNamespace(_process_factory=poison)
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        OrchestrationSupervisor._launch_component(
            subject, ("never-launched",), managed / "new" / "log.txt"
        )
    assert _snapshot(tmp_path) == before


@pytest.mark.parametrize(
    "managed_child", ("state", "logs", "sqlite", "manifests")
)
def test_supervisor_start_preflights_runtime_before_status_repair(
    tmp_path: Path, managed_child: str
) -> None:
    from histdatacom.orchestration.runtime import OrchestrationPaths
    from histdatacom.orchestration.supervisor import OrchestrationSupervisor

    root = tmp_path / "ordinary"
    _mark(root / managed_child)
    before = _snapshot(tmp_path)

    def poison(*args: object, **kwargs: object) -> None:
        raise AssertionError("managed runtime must refuse before status repair")

    subject = SimpleNamespace(
        paths=OrchestrationPaths.from_runtime_dir(root), status=poison
    )
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        OrchestrationSupervisor.start(subject)
    assert _snapshot(tmp_path) == before


@pytest.mark.parametrize(
    "role", ("output_root", "scratch_root", "artifact_root")
)
def test_storage_root_creation_preflights_all_roots(
    tmp_path: Path, role: str
) -> None:
    from histdatacom.reconstruction_storage import (
        create_reconstruction_storage_root_guard,
    )

    managed = _mark(tmp_path / "managed")
    arguments = {
        name: tmp_path / "ordinary" / name
        for name in ("output_root", "scratch_root", "artifact_root")
    }
    arguments[role] = managed / "new"
    before = _snapshot(tmp_path)
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        create_reconstruction_storage_root_guard(**arguments)
    assert _snapshot(tmp_path) == before


@pytest.mark.parametrize("role", ("output_root", "scratch_root"))
def test_storage_root_creation_preflights_marker_alias(
    tmp_path: Path, role: str
) -> None:
    from histdatacom.reconstruction_storage import (
        RECONSTRUCTION_STORAGE_ROOT_GUARD_MARKER,
        create_reconstruction_storage_root_guard,
    )

    protected = _mark(tmp_path / "managed") / "protected.bin"
    arguments = {
        name: tmp_path / "ordinary" / name
        for name in ("output_root", "scratch_root", "artifact_root")
    }
    arguments[role].mkdir(parents=True)
    (arguments[role] / RECONSTRUCTION_STORAGE_ROOT_GUARD_MARKER).symlink_to(
        protected
    )
    before = _snapshot(tmp_path)
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        create_reconstruction_storage_root_guard(**arguments)
    assert _snapshot(tmp_path) == before


def test_lifecycle_append_rechecks_namespace_with_real_open_descriptor(
    tmp_path: Path,
) -> None:
    from histdatacom.broker_plugin_lifecycle.storage import Journal

    root = tmp_path / "ordinary"
    root.mkdir()
    partial = root / "partition-0000.partial"
    descriptor = os.open(partial, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(descriptor, b"retained bytes\n")
        _mark(root)
        subject = object.__new__(Journal)
        subject.directory = root
        subject.partitions = []
        subject.descriptor = descriptor
        before = _snapshot(tmp_path)
        with pytest.raises(boundary.ManagedArtifactBoundaryError):
            subject.append(None)
        assert _snapshot(tmp_path) == before
        assert os.fstat(descriptor).st_size == len(b"retained bytes\n")
    finally:
        os.close(descriptor)


@pytest.mark.parametrize(
    "filename",
    ("manifest.pending", "manifest.json", "manifest.json.provider-policy.json"),
)
def test_lifecycle_publish_preflights_entire_target_set(
    tmp_path: Path, filename: str
) -> None:
    from histdatacom.broker_plugin_lifecycle.storage import Journal

    root = tmp_path / "ordinary"
    root.mkdir()
    protected = _mark(tmp_path / "managed") / "protected.bin"
    (root / filename).symlink_to(protected)
    subject = object.__new__(Journal)
    subject.directory = root
    before = _snapshot(tmp_path)
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        subject.publish(None)
    assert _snapshot(tmp_path) == before


@pytest.mark.parametrize("method", ("append", "seal"))
def test_lifecycle_partition_paths_refuse_managed_alias(
    tmp_path: Path, method: str
) -> None:
    from histdatacom.broker_plugin_lifecycle.storage import Journal

    root = tmp_path / "ordinary"
    root.mkdir()
    protected = _mark(tmp_path / "managed") / "protected.bin"
    (root / "partition-0000.partial").symlink_to(protected)
    subject = object.__new__(Journal)
    subject.directory = root
    subject.partitions = []
    before = _snapshot(tmp_path)
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        subject.append(None) if method == "append" else subject.seal()
    assert _snapshot(tmp_path) == before


@pytest.mark.parametrize("suffix", (".partial", ".provider-policy.json"))
def test_fingerprint_preflights_all_targets_before_policy_write(
    tmp_path: Path, suffix: str
) -> None:
    from histdatacom.broker_capture.fingerprints import (
        write_broker_delivery_fingerprint,
    )

    root = tmp_path / "ordinary"
    root.mkdir()
    protected = _mark(tmp_path / "managed") / "protected.bin"
    target = root / "fingerprint.json"
    target.with_name(target.name + suffix).symlink_to(protected)
    before = _snapshot(tmp_path)
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        write_broker_delivery_fingerprint(target, None)
    assert _snapshot(tmp_path) == before
