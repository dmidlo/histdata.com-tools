"""Generated-only legacy mutation boundaries; no managed deletion authority."""

from __future__ import annotations

import importlib
import inspect
import os
from pathlib import Path
import shlex
import subprocess
import sys
from types import SimpleNamespace

import pytest

from histdatacom import managed_artifact_boundary as boundary


@pytest.fixture
def managed(tmp_path: Path) -> Path:
    root = tmp_path / "managed"
    root.mkdir()
    (root / boundary.MANAGED_ARTIFACT_MARKER).write_bytes(b"invalid on purpose")
    (root / "source.csv").write_bytes(b"synthetic protected source")
    return root


def _namespace_target(arguments: dict, expression: str, target: Path) -> None:
    parts = expression.strip().split(".")
    if len(parts) == 1:
        arguments[parts[0]] = target
        return
    value = arguments.get(parts[0])
    if not isinstance(value, SimpleNamespace):
        value = SimpleNamespace()
        arguments[parts[0]] = value
    for part in parts[1:-1]:
        child = getattr(value, part, None)
        if not isinstance(child, SimpleNamespace):
            child = SimpleNamespace()
            setattr(value, part, child)
        value = child
    setattr(value, parts[-1], target)


# Each tuple invokes the actual writer/mutation boundary, without replacing its
# guard. Invalid scientific arguments must never be inspected before refusal.
# This is admission coverage, not scientific execution or publication evidence.
ENTRYPOINTS = (
    ("activity_stages", "atomic_write_zip_archive", "data_dir"),
    ("activity_stages", "atomic_extract_archive_member", "target_path"),
    ("activity_stages", "write_repository_data_file", "repo_local_path"),
    ("activity_stages", "create_cache_file", "record.data_dir"),
    ("activity_stages", "atomic_write_polars_cache", "target_path"),
    ("activity_stages", "extract_archive_to_record", "record.data_dir"),
    ("attribution.artifacts", "write_attribution_artifact", "directory"),
    (
        "broker_capture.fingerprints",
        "write_broker_delivery_fingerprint",
        "path",
    ),
    (
        "broker_plugin_policy.scientific_storage",
        "write_broker_scientific_lineage",
        "native_path",
    ),
    ("broker_plugin_policy.storage", "write_broker_policy_context", "path"),
    ("broker_plugin_policy.storage", "write_broker_policy_receipt", "path"),
    ("broker_plugin_registry.storage", "write_plugin_inventory", "path"),
    ("broker_plugin_security.storage", "write_security_receipt", "path"),
    (
        "broker_plugin_security.isolation",
        "prepare_kernel_launch",
        "working_directory",
    ),
    ("data_quality.training_artifacts", "write_training_artifact", "directory"),
    (
        "data_quality.training_join_artifacts",
        "write_training_join_artifact",
        "directory",
    ),
    (
        "data_quality.training_overlap_artifacts",
        "write_training_overlap_artifact",
        "directory",
    ),
    (
        "data_quality.training_temporal_artifacts",
        "write_training_temporal_artifact",
        "directory",
    ),
    ("data_quality.training_weight_artifacts", "_publish", "path"),
    (
        "data_quality.training_wide_artifacts",
        "write_training_wide_artifact",
        "directory",
    ),
    ("experiments.artifacts", "write_experiment_artifact", "directory"),
    (
        "forecasting.benchmark_artifacts",
        "write_benchmark_artifact",
        "directory",
    ),
    ("forecasting.engine_artifacts", "write_engine_artifact", "directory"),
    ("forecasting.feature_artifacts", "write_feature_artifact", "directory"),
    ("forecasting.math_artifacts", "write_math_artifact", "directory"),
    ("market_context.corpus", "_write_once", "path"),
    ("market_context.official_sources", "_write_once", "path"),
    ("market_context.positioning", "_write_once", "path"),
    ("reconstruction_storage", "_write_exact_file", "path"),
    ("schema_semantics.storage", "write_semantic_proof", "directory"),
    (
        "synthetic.activity",
        "write_reconstruction_activity_manifest",
        "directory",
    ),
    ("synthetic.alignment_qualification", "_write_content_addressed", "root"),
    ("synthetic.bars", "_atomic_write_bytes", "path"),
    ("synthetic.bars", "_remove_bar_scratch", "path"),
    ("synthetic.benchmark_corpus", "_write_once", "path"),
    (
        "synthetic.benchmark_source_projection",
        "build_benchmark_source_projection_manifest",
        "projection_root",
    ),
    ("synthetic.benchmark_source_projection", "_write_once", "path"),
    ("synthetic.capability_certification", "_publish_exact", "path"),
    ("synthetic.certification", "_atomic_write", "path"),
    ("synthetic.certification_campaign", "_atomic_write", "path"),
    (
        "synthetic.critical_path_quality",
        "write_critical_path_gate_report",
        "path",
    ),
    ("synthetic.diagnostics", "_write_once", "path"),
    ("synthetic.feed_epoch_transition", "_write_contract", "output_directory"),
    ("synthetic.motif_library", "_write_once", "path"),
    ("synthetic.motifs", "write_reference_motif_index", "path"),
    (
        "synthetic.observation_uncertainty",
        "_write_contract",
        "output_directory",
    ),
    ("synthetic.partition_invariance", "_write_contract", "output_directory"),
    ("synthetic.projection_burden", "_write_once", "path"),
    ("synthetic.release_candidate", "_write_contract", "output_directory"),
    ("synthetic.release_holdout", "_atomic_replace", "path"),
    (
        "synthetic.release_holdout_evaluation",
        "_write_contract",
        "output_directory",
    ),
    ("synthetic.support_verification", "_write_json", "path"),
    (
        "orchestration.reconstruction",
        "cleanup_reconstruction_window_scratch",
        "path",
    ),
    ("orchestration.reconstruction", "_atomic_write", "path"),
    ("orchestration.readiness", "remove_worker_readiness", "state_dir"),
    (
        "orchestration.supervisor",
        "OrchestrationSupervisor._acquire_lock",
        "self.paths.lock_file",
    ),
    (
        "orchestration.supervisor",
        "OrchestrationSupervisor._release_lock",
        "self.paths.lock_file",
    ),
    (
        "orchestration.supervisor",
        "OrchestrationSupervisor._remove_state_files",
        "self.paths.pid_file, self.paths.lock_file",
    ),
    ("orchestration.maintenance", "_rotate_log_path", "path"),
    (
        "synthetic.persistence",
        "stage_delivery_reconstruction_publication",
        "root, staging_root",
    ),
    ("synthetic.persistence", "cleanup_reconstruction_scratch", "root"),
    ("synthetic.persistence", "_materialize_portable_source_artifacts", "root"),
    ("synthetic.persistence", "_write_parquet_partition", "target"),
    ("synthetic.persistence", "_atomic_write_bytes", "path"),
    ("synthetic.persistence", "_remove_scratch_entry", "path"),
    (
        "synthetic.reconstruction_handlers",
        "proposal_handler",
        "invocation.task.scratch_directory",
    ),
    (
        "synthetic.reconstruction_handlers",
        "carving_handler",
        "invocation.task.scratch_directory",
    ),
    (
        "synthetic.reconstruction_handlers",
        "validation_handler",
        "invocation.task.scratch_directory",
    ),
    (
        "synthetic.reconstruction_handlers",
        "_cleanup_committed_window_scratch",
        "invocation.task.scratch_directory",
    ),
    (
        "synthetic.reconstruction_handlers",
        "_stage_directory",
        "invocation.task.scratch_directory",
    ),
    ("activity_stages", "_unlink_path", "path"),
    ("manifest_store", "ManifestStatusStore.__init__", "root_dir"),
    (
        "manifest_store",
        "ManifestStatusStore.import_meta_file",
        "meta_path, self.root_dir",
    ),
    ("manifest_store", "ManifestStatusStore.prune_retention", "self.root_dir"),
    ("records", "Record.delete_manifest_status", "self.data_dir"),
    ("broker_plugin_lifecycle.storage", "Journal.__init__", "directory"),
    ("broker_plugin_lifecycle.storage", "Journal.seal", "self.directory"),
)


@pytest.mark.parametrize("module,qualified_name,targets", ENTRYPOINTS)
def test_actual_writer_entrypoint_refuses_managed_namespace(
    managed: Path, module: str, qualified_name: str, targets: str
) -> None:
    function = importlib.import_module("histdatacom." + module)
    for component in qualified_name.split("."):
        function = getattr(function, component)
    arguments = {
        name: None
        for name, parameter in inspect.signature(function).parameters.items()
        if parameter.default is inspect.Parameter.empty
        and parameter.kind
        not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
    }
    for target in targets.split(","):
        _namespace_target(arguments, target, managed / "payload")
    if qualified_name == "_rotate_log_path":
        arguments["max_rotated_logs"] = 1
    before = sorted(path.name for path in managed.iterdir())
    with pytest.raises(boundary.ManagedArtifactBoundaryError, match="managed"):
        function(**arguments)
    assert sorted(path.name for path in managed.iterdir()) == before
    assert (
        managed / "source.csv"
    ).read_bytes() == b"synthetic protected source"


@pytest.mark.parametrize("kind", ["invalid", "empty", "directory", "symlink"])
def test_marker_presence_is_protection(tmp_path: Path, kind: str) -> None:
    marker = tmp_path / boundary.MANAGED_ARTIFACT_MARKER
    if kind == "directory":
        marker.mkdir()
    elif kind == "symlink":
        marker.symlink_to(tmp_path / "missing")
    else:
        marker.write_bytes(b"" if kind == "empty" else b"not JSON")
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        boundary.assert_unmanaged_mutation_paths((tmp_path / "new" / "output",))


def test_lexical_and_symlink_alias_ancestry(
    managed: Path, tmp_path: Path
) -> None:
    alias = tmp_path / "alias"
    alias.symlink_to(managed, target_is_directory=True)
    escape = managed / "outside"
    escape.symlink_to(tmp_path, target_is_directory=True)
    for path in (alias / "new", escape / "ordinary", managed / ".." / "other"):
        with pytest.raises(boundary.ManagedArtifactBoundaryError):
            boundary.assert_unmanaged_mutation_paths((path,))


def test_recursive_ancestor_and_complete_batch_preflight(
    managed: Path, tmp_path: Path
) -> None:
    from histdatacom.cancellation import cleanup_partial_artifacts

    ordinary = tmp_path / "ordinary.partial"
    ordinary.write_bytes(b"keep before complete admission")
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        boundary.assert_unmanaged_mutation_paths((tmp_path,), recursive=True)
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        cleanup_partial_artifacts(iter((ordinary, managed / "source.csv")))
    assert ordinary.read_bytes() == b"keep before complete admission"


@pytest.mark.parametrize(
    "limit", ["MAX_BOUNDARY_ENTRIES", "MAX_BOUNDARY_DEPTH"]
)
def test_incomplete_scan_refuses_before_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, limit: str
) -> None:
    root = tmp_path / "ordinary"
    leaf = root / "nested" / "data"
    leaf.parent.mkdir(parents=True)
    leaf.write_bytes(b"bounded")
    monkeypatch.setattr(boundary, limit, 1)
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        boundary.guarded_campaign_cleanup((root,), mode="working-artifacts")
    assert leaf.read_bytes() == b"bounded"


def test_path_inventory_is_bounded_without_exhausting_generator(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from histdatacom.cancellation import cleanup_partial_artifacts

    def paths():
        for _ in range(boundary.MAX_BOUNDARY_PATHS + 1):
            yield tmp_path / "missing"
        pytest.fail("unbounded iterator traversal")

    with pytest.raises(boundary.ManagedArtifactBoundaryError, match="too many"):
        cleanup_partial_artifacts(paths())


def test_unknown_inspection_refuses(tmp_path: Path, monkeypatch) -> None:
    def inaccessible(_):
        raise PermissionError("synthetic")

    monkeypatch.setattr(os, "scandir", inaccessible)
    with pytest.raises(boundary.ManagedArtifactBoundaryError, match="inspect"):
        boundary.assert_unmanaged_mutation_paths((tmp_path,), recursive=True)


def test_source_cleanup_dry_run_preserved_but_apply_denied(
    managed: Path,
) -> None:
    from histdatacom.source_cleanup import cleanup_transient_source_artifacts

    result = cleanup_transient_source_artifacts(managed)
    assert result.dry_run and result.matched_count == 1
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        cleanup_transient_source_artifacts(managed, apply=True)
    assert (managed / "source.csv").exists()


def test_activity_cleanup_preflights_both_sources(
    managed: Path, tmp_path: Path
):
    from histdatacom.activity_stages import (
        _delete_cache_source_artifacts,
        _delete_zip_after_extraction,
        _unlink_if_present,
    )

    ordinary = tmp_path / "first.zip"
    ordinary.write_bytes(b"zip")
    record = SimpleNamespace(
        data_dir=str(tmp_path),
        zip_filename=ordinary.name,
        csv_filename=str(managed / "source.csv"),
    )
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        _delete_cache_source_artifacts(record, {"delete_after_cache": True})
    assert ordinary.read_bytes() == b"zip"
    record.zip_filename = str(managed / "source.csv")
    assert _delete_zip_after_extraction(record, zip_persist=True) is False
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        _delete_zip_after_extraction(record, zip_persist=False)
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        _unlink_if_present(str(managed), "source.csv")


def test_quality_report_noops_preserved_and_mutation_denied(managed: Path):
    from histdatacom.orchestration.activities import _quality_report_disposition

    artifact = SimpleNamespace(
        path=str(managed / "source.csv"), size_bytes=26, sha256="0" * 64
    )
    request = SimpleNamespace(quality_report_path="explicit")
    assert _quality_report_disposition(request, artifact, success=True)["kept"]
    request.quality_report_path = ""
    assert _quality_report_disposition(request, artifact, success=False)["kept"]
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        _quality_report_disposition(request, artifact, success=True)


def test_runtime_cache_and_lock_actual_boundaries(managed: Path):
    from histdatacom.orchestration.resources import (
        _DirectoryLock,
        _provision_temporal_runtime,
        prune_temporal_runtime_cache,
    )

    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        prune_temporal_runtime_cache(cache_dir=managed, keep_current=False)
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        _provision_temporal_runtime(
            managed,
            None,
            None,
            download_archive=None,
            download_timeout=1,
            lock_timeout=1,
        )
    lock = _DirectoryLock(managed, timeout=1)
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        lock.__enter__()
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        lock.__exit__()


def test_rotation_admits_all_siblings_before_effect(
    managed: Path, tmp_path: Path
):
    from histdatacom.orchestration.maintenance import _rotate_log_path

    log = tmp_path / "worker.log"
    log.write_bytes(b"current")
    (tmp_path / "worker.log.2").symlink_to(managed / "source.csv")
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        _rotate_log_path(log, 2)
    assert log.read_bytes() == b"current"


def test_partial_output_alias_refused_before_arrow_import(
    managed: Path, tmp_path: Path, monkeypatch
):
    from histdatacom.synthetic import persistence

    target = tmp_path / "data.parquet"
    target.with_name(target.name + ".partial").symlink_to(
        managed / "source.csv"
    )
    monkeypatch.setattr(
        persistence,
        "_arrow_modules",
        lambda: pytest.fail("Arrow before full path admission"),
    )
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        persistence._write_parquet_partition(None, target, row_group_size=1)


@pytest.mark.parametrize("mode", ["cache", "working-artifacts"])
def test_campaign_generated_command_rechecks_at_execution(
    managed: Path, tmp_path: Path, mode: str
):
    from histdatacom.data_quality.campaign import _cleanup_command

    ordinary = tmp_path / "first generated"
    ordinary.mkdir()
    (ordinary / ".data").write_bytes(b"cache")
    late = tmp_path / "late generated"
    late.mkdir()
    command = _cleanup_command(mode, [str(ordinary), str(late)])
    (late / boundary.MANAGED_ARTIFACT_MARKER).write_bytes(
        b"late malformed marker"
    )
    argv = shlex.split(command)
    assert argv[:3] == ["python", "-m", "histdatacom.managed_artifact_boundary"]
    argv[0] = (
        sys.executable
    )  # Execute the selected test runtime, not shell PATH.
    result = subprocess.run(
        argv, cwd=tmp_path, text=True, capture_output=True, check=False
    )
    assert result.returncode == 1
    assert "managed artifact namespace" in result.stderr
    assert (ordinary / ".data").read_bytes() == b"cache"
    assert (late / boundary.MANAGED_ARTIFACT_MARKER).exists()


@pytest.mark.parametrize("mode", ["cache", "working-artifacts"])
def test_campaign_ordinary_synthetic_cleanup(tmp_path: Path, mode: str):
    root = tmp_path / "generated"
    root.mkdir()
    (root / ".data").write_bytes(b"cache")
    (root / "source.csv").write_bytes(b"source")
    result = boundary.guarded_campaign_cleanup((root,), mode=mode)
    assert result["removed_files"] == (1 if mode == "cache" else 2)
    assert result["removed_bytes"] == (5 if mode == "cache" else 11)
    if mode == "cache":
        assert (root / "source.csv").read_bytes() == b"source"
    else:
        assert not root.exists()


def test_ordinary_writer_and_cleanup_compatibility(tmp_path: Path):
    from histdatacom.cancellation import cleanup_partial_artifacts
    from histdatacom.reconstruction_storage import _write_exact_file
    from histdatacom.source_cleanup import cleanup_transient_source_artifacts

    output = tmp_path / "output.json"
    _write_exact_file(output, b"{}", replace_existing=False)
    assert output.read_bytes() == b"{}"
    partial = tmp_path / "partial"
    partial.write_bytes(b"partial")
    assert cleanup_partial_artifacts((partial,))[0].removed
    (tmp_path / "synthetic.csv").write_bytes(b"generated source")
    result = cleanup_transient_source_artifacts(tmp_path, apply=True)
    assert result.deleted_count == 1 and result.errors == ()
    assert output.read_bytes() == b"{}"


def test_cleanup_overlap_and_special_objects_preflight(tmp_path: Path):
    root = tmp_path / "ordinary"
    root.mkdir()
    (root / "file").write_bytes(b"keep")
    with pytest.raises(boundary.ManagedArtifactBoundaryError, match="overlap"):
        boundary.guarded_campaign_cleanup(
            (root, root / "file"), mode="working-artifacts"
        )
    if hasattr(os, "mkfifo"):
        os.mkfifo(root / "fifo")
        with pytest.raises(
            boundary.ManagedArtifactBoundaryError, match="unsupported"
        ):
            boundary.guarded_campaign_cleanup((root,), mode="working-artifacts")
    assert (root / "file").read_bytes() == b"keep"


def test_maintenance_checks_status_namespace_before_rotating_logs(tmp_path):
    from histdatacom.orchestration.maintenance import (
        OrchestrationRetentionPolicy,
        _compact_sqlite_store,
        _status_store_result,
        run_orchestration_maintenance,
    )
    from histdatacom.orchestration.runtime import (
        build_orchestration_runtime_policy,
    )

    runtime = build_orchestration_runtime_policy(
        workspace=tmp_path / "workspace", runtime_home=tmp_path / "runtime"
    )
    runtime.paths.logs_dir.mkdir(parents=True)
    runtime.paths.server_log.write_bytes(b"ordinary log needing rotation")
    runtime.paths.manifests_dir.mkdir(parents=True, exist_ok=True)
    (
        runtime.paths.manifests_dir / boundary.MANAGED_ARTIFACT_MARKER
    ).write_bytes(b"protected")
    policy = OrchestrationRetentionPolicy(max_log_bytes=1)
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        run_orchestration_maintenance(runtime, policy)
    assert (
        runtime.paths.server_log.read_bytes()
        == b"ordinary log needing rotation"
    )
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        _status_store_result(runtime, policy)
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        _compact_sqlite_store(runtime.paths.manifests_dir / "db.sqlite")


def test_cancel_and_commit_independent_mutation_guards(managed):
    from histdatacom.synthetic.persistence import (
        StagedReconstructionPublicationV2,
        commit_delivery_reconstruction_publication,
    )
    from histdatacom.synthetic.reconstruction_handlers import (
        _cancel_if_requested,
    )

    invocation = SimpleNamespace(
        cancellation_requested=False,
        task=SimpleNamespace(scratch_directory=str(managed)),
    )
    _cancel_if_requested(invocation)
    invocation.cancellation_requested = True
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        _cancel_if_requested(invocation)
    # Even a corrupted, bypass-constructed carrier cannot bypass the independent
    # commit boundary; no publication/model fixture is needed for admission.
    staged = object.__new__(StagedReconstructionPublicationV2)
    object.__setattr__(staged, "staging_directory", managed)
    object.__setattr__(staged, "committed_directory", managed / "commit")
    with pytest.raises(boundary.ManagedArtifactBoundaryError):
        commit_delivery_reconstruction_publication(staged)


def test_closed_census_has_actual_entrypoint_coverage():
    covered = {row[0] for row in ENTRYPOINTS} | {
        "cancellation",
        "source_cleanup",
        "orchestration.activities",
        "orchestration.resources",
        "data_quality.campaign",
    }
    assert len(covered) == 58


def test_unmanaged_missing_target_is_idempotent(tmp_path):
    boundary.assert_unmanaged_mutation_paths(
        (tmp_path / "missing",), recursive=True
    )
    assert boundary.guarded_campaign_cleanup(
        (tmp_path / "missing",), mode="cache"
    ) == {
        "mode": "cache",
        "removed_files": 0,
        "removed_directories": 0,
        "removed_bytes": 0,
    }
