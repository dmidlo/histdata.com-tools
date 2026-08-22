"""Tests for exact reconstruction release-candidate governance."""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from histdatacom.datasets import DatasetCatalog, histdata_cache_path
from histdatacom.orchestration.reconstruction import artifact_ref_for_file
from histdatacom.reconstruction_experiment import build_legacy_histdata_catalog
from histdatacom.reconstruction_schema import reconstruction_schema_registry
from histdatacom.runtime_contracts import ArtifactRef
from histdatacom.synthetic.reconstruction_plan import (
    FIRST_PARTY_RECONSTRUCTION_HANDLERS,
)
from histdatacom.synthetic.release_candidate import (
    CRITICAL_PATH_GATE_EVIDENCE_KINDS,
    REQUIRED_RELEASE_CANDIDATE_COMMANDS,
    REQUIRED_RELEASE_CANDIDATE_DEPENDENCIES,
    REQUIRED_RELEASE_CANDIDATE_DEPENDENCY_KINDS,
    REQUIRED_RELEASE_CANDIDATE_FORBIDDEN_FALLBACKS,
    REQUIRED_RELEASE_CANDIDATE_GATES,
    REQUIRED_RELEASE_CANDIDATE_RUNTIME_DEPENDENCIES,
    ReconstructionReleaseCandidateV1,
    ReleaseCandidateArtifactBindingV1,
    ReleaseCandidateBranchGovernanceV1,
    ReleaseCandidateBuildSetV1,
    ReleaseCandidateDependencyV1,
    ReleaseCandidateFilesystemRootV1,
    ReleaseCandidateGitIdentityV1,
    ReleaseCandidateRuntimeIdentityV1,
    ReleaseCandidateValidationGateV1,
    bind_release_candidate_artifact,
    capture_release_candidate_runtime_identity,
    freeze_reconstruction_release_candidate,
    inspect_release_candidate_git_identity,
    read_reconstruction_release_candidate,
    read_release_candidate_artifact_binding,
    verify_reconstruction_release_candidate,
    write_reconstruction_release_candidate,
    write_release_candidate_artifact_binding,
)
from histdatacom.synthetic.release_holdout import (
    ProtectedReleaseHoldoutWindowV1,
    ReleaseHoldoutAccessPolicyV1,
    ReleaseHoldoutDevelopmentUnitV1,
    build_protected_release_holdout_manifest,
    freeze_release_candidate,
    write_protected_release_holdout_manifest,
    write_release_candidate_freeze,
)

_DAY_NS = 24 * 60 * 60 * 1_000_000_000
_SOURCE_CUTOFF_NS = int(
    datetime(2002, 4, 1, tzinfo=timezone.utc).timestamp() * 1_000_000_000
)
_SOURCE_START_MS = int(
    datetime(2002, 3, 2, tzinfo=timezone.utc).timestamp() * 1000
)
_SYMBOLS = ("EURGBP", "EURUSD", "GBPUSD")
_COMMIT_SHA = hashlib.sha256(b"commit").hexdigest()
_TREE_SHA = hashlib.sha256(b"tree").hexdigest()
_MACHINE_CLASS = "darwin-arm64-research-runner"
_FROZEN_STAGES = (
    "fit",
    "preprocess",
    "support_tuning",
    "smoothing",
    "engine_selection",
    "scenario_policy",
    "adaptive_policy",
)
_HANDLERS = tuple(FIRST_PARTY_RECONSTRUCTION_HANDLERS.values())


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _artifact(
    tmp_path: Path,
    name: str,
    *,
    metadata: dict[str, object] | None = None,
    kind: str = "release_candidate_test_evidence",
) -> ArtifactRef:
    path = tmp_path / "evidence" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"artifact": name}), encoding="utf-8")
    return artifact_ref_for_file(
        path,
        kind=kind,
        metadata=metadata,
    )


def _holdout_refs(tmp_path: Path) -> tuple[ArtifactRef, ArtifactRef, int]:
    development = ReleaseHoldoutDevelopmentUnitV1(
        split_role="validation",
        period="202501",
        start_ns=1,
        end_ns=_SOURCE_CUTOFF_NS,
        source_partition_ids=("partition:development",),
        source_hashes={"eurusd": _sha("development-source")},
        source_signature_sha256=_sha("development-signature"),
        motif_signature_sha256=_sha("development-motif"),
        context_signature_sha256=_sha("development-context"),
        source_neighbor_sketch=_sha("development-source-sketch")[:16],
        motif_neighbor_sketch=_sha("development-motif-sketch")[:16],
        context_neighbor_sketch=_sha("development-context-sketch")[:16],
        cohesion_group_ids=("cohesion:development",),
        anchor_neighborhood_ids=("anchor:development",),
        context_event_ids=("context:development",),
    )
    epochs = ("early", "qualified_transition", "modern", "modern")
    sessions = ("asia", "london", "new_york", "overlap_closure")
    event_strata = ("ordinary", "event", "ordinary", "event")
    scenarios = (
        "high_retention_low_infill",
        "central_fitted_retention",
        "low_retention_high_infill",
        "central_fitted_retention",
    )
    alignments = ("exact", "bounded_nearest", "exact", "bounded_nearest")
    deficits = ("low", "median", "high", "median")
    windows = []
    for index in range(4):
        label = f"holdout-{index}"
        start_ns = _SOURCE_CUTOFF_NS + (index + 1) * 9 * _DAY_NS
        windows.append(
            ProtectedReleaseHoldoutWindowV1(
                period=f"2026{index + 1:02d}",
                start_ns=start_ns,
                end_ns=start_ns + _DAY_NS,
                source_partition_ids=(f"partition:{label}",),
                source_hashes={"eurusd": _sha(f"source:{label}")},
                source_signature_sha256=_sha(f"signature:{label}"),
                motif_signature_sha256=_sha(f"motif:{label}"),
                context_signature_sha256=_sha(f"context:{label}"),
                source_neighbor_sketch=_sha(f"source-sketch:{label}")[:16],
                motif_neighbor_sketch=_sha(f"motif-sketch:{label}")[:16],
                context_neighbor_sketch=_sha(f"context-sketch:{label}")[:16],
                cohesion_group_ids=(f"cohesion:{label}",),
                anchor_neighborhood_ids=(f"anchor:{label}",),
                context_event_ids=(f"context-event:{label}",),
                symbol_event_counts={"eurusd": 100 + index},
                epoch_stratum=epochs[index],
                session_stratum=sessions[index],
                event_stratum=event_strata[index],
                observation_scenario_id=scenarios[index],
                alignment_kind=alignments[index],
                deficit_stratum=deficits[index],
            )
        )
    selection_ref = _artifact(
        tmp_path,
        "selection.json",
        metadata={"dossier_id": "selection:dossier:v1"},
    )
    manifest = build_protected_release_holdout_manifest(
        ReleaseHoldoutAccessPolicyV1(),
        windows,
        (development,),
        selection_dossier_id="selection:dossier:v1",
        selection_dossier_ref=selection_ref,
        source_cutoff_ns=_SOURCE_CUTOFF_NS,
        claim_scope="v2.5-release-candidate",
        frozen_at_utc="2026-08-20T12:00:00Z",
    )
    stage_artifacts = {
        stage: _artifact(
            tmp_path,
            f"stage-{stage}.json",
            metadata={"input_roles": ["calibration", "validation"]},
        )
        for stage in _FROZEN_STAGES
    }
    graph = freeze_release_candidate(
        manifest,
        candidate_id="release-candidate:v2.5.0",
        stage_artifacts=stage_artifacts,
        frozen_at_utc="2026-08-20T13:00:00Z",
    )
    return (
        write_release_candidate_freeze(graph, tmp_path / "holdout"),
        write_protected_release_holdout_manifest(
            manifest, tmp_path / "holdout"
        ),
        _SOURCE_CUTOFF_NS,
    )


def _git_identity() -> ReleaseCandidateGitIdentityV1:
    return ReleaseCandidateGitIdentityV1(
        repository_url="https://github.com/dmidlo/histdata.com-tools.git",
        ref_name="refs/tags/v2.5.0-rc.1",
        commit_sha=_COMMIT_SHA,
        tree_sha=_TREE_SHA,
        captured_at_utc="2026-08-20T14:00:00Z",
    )


def _build_set(tmp_path: Path) -> ReleaseCandidateBuildSetV1:
    return ReleaseCandidateBuildSetV1(
        package_name="histdatacom",
        package_version="2.5.0",
        git_commit_sha=_COMMIT_SHA,
        artifacts={
            "wheel": _artifact(
                tmp_path,
                "histdatacom-2.5.0-py3-none-any.whl",
                metadata={
                    "git_commit_sha": _COMMIT_SHA,
                    "package_version": "2.5.0",
                    "distribution_format": "wheel",
                },
            ),
            "sdist": _artifact(
                tmp_path,
                "histdatacom-2.5.0.tar.gz",
                metadata={
                    "git_commit_sha": _COMMIT_SHA,
                    "package_version": "2.5.0",
                    "distribution_format": "sdist",
                },
            ),
        },
    )


def _runtime() -> ReleaseCandidateRuntimeIdentityV1:
    return ReleaseCandidateRuntimeIdentityV1(
        python_implementation="CPython",
        python_version="3.13.7",
        python_abi="cpython-313",
        operating_system="Darwin",
        operating_system_release="25.6.0",
        architecture="arm64",
        machine_class=_MACHINE_CLASS,
        dependency_versions={
            name: "1.0.0"
            for name in REQUIRED_RELEASE_CANDIDATE_RUNTIME_DEPENDENCIES
        },
        compression_versions={"zlib": "1.3.1", "gzip": "stdlib"},
    )


def _dataset_catalog_dependency(
    tmp_path: Path,
) -> tuple[ReleaseCandidateDependencyV1, str, dict[str, str]]:
    source_root = tmp_path / "ASCII" / "T"
    for ordinal, symbol in enumerate(_SYMBOLS):
        path = histdata_cache_path(source_root, symbol, "200203")
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                "datetime": [
                    _SOURCE_START_MS + ordinal,
                    _SOURCE_START_MS + 1000 + ordinal,
                ],
                "bid": [1.0 + ordinal / 1000, 1.0001 + ordinal / 1000],
                "ask": [1.0002 + ordinal / 1000, 1.0003 + ordinal / 1000],
                "vol": [0, 0],
            },
            schema={
                "datetime": pl.Int64,
                "bid": pl.Float64,
                "ask": pl.Float64,
                "vol": pl.Int32,
            },
        ).write_ipc(path)
    qualification_ref = _artifact(
        tmp_path,
        "dataset-qualification.json",
        metadata={"status": "qualified"},
    )
    catalog, catalog_path, version = build_legacy_histdata_catalog(
        source_root,
        symbols=_SYMBOLS,
        periods=("200203",),
        qualification_evidence=(qualification_ref,),
        path=tmp_path / "dataset-catalog.json",
    )
    catalog_ref = artifact_ref_for_file(
        catalog_path,
        kind=REQUIRED_RELEASE_CANDIDATE_DEPENDENCY_KINDS["dataset_catalog"],
        metadata={
            "artifact_id": catalog.catalog_id,
            "dataset_revision": version.dataset_version_id,
        },
    )
    source_hashes = {
        f"{partition.symbol.lower()}:{partition.period}": (
            partition.artifact.sha256
        )
        for partition in version.partitions
    }
    return (
        ReleaseCandidateDependencyV1(
            name="dataset_catalog",
            artifact_id=catalog.catalog_id,
            artifact_ref=catalog_ref,
        ),
        version.dataset_version_id,
        source_hashes,
    )


def _dependencies(tmp_path: Path) -> tuple[ReleaseCandidateDependencyV1, ...]:
    graph_ref, holdout_ref, _ = _holdout_refs(tmp_path)
    registry_id = reconstruction_schema_registry().registry_id
    dataset_dependency, _, _ = _dataset_catalog_dependency(tmp_path)
    dependencies = []
    for name in REQUIRED_RELEASE_CANDIDATE_DEPENDENCIES:
        if name == "candidate_graph":
            artifact_id = str(graph_ref.metadata["graph_id"])
            ref = graph_ref
        elif name == "protected_release_holdout":
            artifact_id = str(holdout_ref.metadata["manifest_id"])
            ref = holdout_ref
        elif name == "dataset_catalog":
            dependencies.append(dataset_dependency)
            continue
        elif name == "schema_registry":
            artifact_id = registry_id
            ref = _artifact(
                tmp_path,
                f"dependency-{name}.json",
                metadata={"registry_id": artifact_id},
                kind=REQUIRED_RELEASE_CANDIDATE_DEPENDENCY_KINDS[name],
            )
        elif name == "experiment_manifest":
            artifact_id = "experiment:v2.5.0"
            ref = _artifact(
                tmp_path,
                f"dependency-{name}.json",
                metadata={"experiment_id": artifact_id},
                kind=REQUIRED_RELEASE_CANDIDATE_DEPENDENCY_KINDS[name],
            )
        else:
            artifact_id = f"{name}:v1"
            extra: dict[str, object] = {"artifact_id": artifact_id}
            if name in {"selected_engine_config", "selected_engine_fit"}:
                extra["engine_id"] = "histdatacom.marked-hawkes"
            ref = _artifact(
                tmp_path,
                f"dependency-{name}.json",
                metadata=extra,
                kind=REQUIRED_RELEASE_CANDIDATE_DEPENDENCY_KINDS[name],
            )
        dependencies.append(
            ReleaseCandidateDependencyV1(
                name=name,
                artifact_id=artifact_id,
                artifact_ref=ref,
            )
        )
    return tuple(dependencies)


def _roots(tmp_path: Path) -> tuple[ReleaseCandidateFilesystemRootV1, ...]:
    roots = []
    for role in ("artifact", "output", "checkpoint", "scratch"):
        filesystem_id = f"apfs-volume:{role}"
        evidence_ref = _artifact(
            tmp_path,
            f"filesystem-{role}.json",
            metadata={
                "role": role,
                "filesystem_id": filesystem_id,
                "writable": True,
                "atomic_replace_verified": True,
                "durable_write_verified": True,
                "machine_class": _MACHINE_CLASS,
            },
        )
        roots.append(
            ReleaseCandidateFilesystemRootV1(
                role=role,
                path=str(tmp_path / "qualified-roots" / role),
                filesystem_id=filesystem_id,
                filesystem_type="apfs",
                device_id=f"device:{role}",
                machine_class=_MACHINE_CLASS,
                free_bytes=100_000_000_000,
                qualification_ref=evidence_ref,
                qualified_at_utc="2026-08-20T14:10:00Z",
            )
        )
    return tuple(roots)


def _gates(tmp_path: Path) -> tuple[ReleaseCandidateValidationGateV1, ...]:
    return tuple(
        ReleaseCandidateValidationGateV1(
            gate_name=gate_name,
            git_commit_sha=_COMMIT_SHA,
            platform="github-hosted" if "isolated" in gate_name else "local",
            command=f"validate {gate_name}",
            evidence_ref=_artifact(
                tmp_path,
                f"gate-{gate_name}.json",
                kind=CRITICAL_PATH_GATE_EVIDENCE_KINDS.get(
                    gate_name, "release_candidate_test_evidence"
                ),
                metadata={
                    "gate_name": gate_name,
                    "git_commit_sha": _COMMIT_SHA,
                    "passed": True,
                },
            ),
            completed_at_utc="2026-08-20T14:20:00Z",
        )
        for gate_name in REQUIRED_RELEASE_CANDIDATE_GATES
    )


def _branch_governance(tmp_path: Path) -> ReleaseCandidateBranchGovernanceV1:
    immutable_ref = "refs/tags/v2.5.0-rc.1"
    return ReleaseCandidateBranchGovernanceV1(
        immutable_ref=immutable_ref,
        git_commit_sha=_COMMIT_SHA,
        required_checks=(
            "Build package artifacts",
            "Build documentation",
            "Production coverage",
            "Python 3.13 on macos-latest",
            "Python 3.13 on ubuntu-latest",
            "Python 3.13 on windows-latest",
        ),
        protection_ref=_artifact(
            tmp_path,
            "branch-protection.json",
            metadata={
                "immutable_ref": immutable_ref,
                "git_commit_sha": _COMMIT_SHA,
                "protection_enabled": True,
                "same_checks_as_main": True,
                "signed_merge_or_tag_required": True,
            },
        ),
    )


def _candidate(tmp_path: Path) -> ReconstructionReleaseCandidateV1:
    dependencies = _dependencies(tmp_path)
    catalog_dependency = next(
        item for item in dependencies if item.name == "dataset_catalog"
    )
    catalog = DatasetCatalog.read(catalog_dependency.artifact_ref.path)
    version = catalog.versions[0]
    _, _, source_cutoff_ns = _holdout_refs(tmp_path / "cutoff")
    return freeze_reconstruction_release_candidate(
        git_identity=_git_identity(),
        build_set=_build_set(tmp_path),
        runtime_identity=_runtime(),
        schema_registry_id=reconstruction_schema_registry().registry_id,
        dataset_revision=version.dataset_version_id,
        source_partition_hashes={
            f"{partition.symbol.lower()}:{partition.period}": (
                partition.artifact.sha256
            )
            for partition in version.partitions
        },
        experiment_id="experiment:v2.5.0",
        selected_engine_id="histdatacom.marked-hawkes",
        dependencies=dependencies,
        filesystem_roots=_roots(tmp_path),
        validation_gates=_gates(tmp_path),
        branch_governance=_branch_governance(tmp_path),
        executable_scope=(
            "complete HistData.com EURGBP/EURUSD/GBPUSD ASCII/T campaign",
        ),
        scientific_estimand=(
            "conditional distribution of plausible missing tick events"
        ),
        scientific_nonclaims=(
            "not recovery of the unknowable historical event stream",
            "not valid for pretending synthetic rows were observed",
        ),
        source_cutoff_ns=source_cutoff_ns,
        permitted_commands=tuple(REQUIRED_RELEASE_CANDIDATE_COMMANDS),
        runtime_handlers=_HANDLERS,
        forbidden_fallbacks=tuple(
            REQUIRED_RELEASE_CANDIDATE_FORBIDDEN_FALLBACKS
        ),
        release_blocking_issues=tuple(range(514, 527)),
        frozen_at_utc="2026-08-20T15:00:00Z",
    )


def test_candidate_round_trip_is_content_addressed_and_complete(
    tmp_path: Path,
) -> None:
    candidate = _candidate(tmp_path)

    assert {item.name for item in candidate.dependencies} == (
        REQUIRED_RELEASE_CANDIDATE_DEPENDENCIES
    )
    assert len(candidate.runtime_handlers) == 7
    assert candidate.to_dict()["fresh_release_holdout_sealed"] is True
    assert candidate.to_dict()["qualification_started"] is False
    assert (
        candidate.to_dict()["cross_candidate_certification_permitted"] is False
    )

    ref = write_reconstruction_release_candidate(candidate, tmp_path)
    assert ref.sha256 in Path(ref.path).name
    assert read_reconstruction_release_candidate(ref.path) == candidate


def test_any_dependency_change_creates_a_new_candidate(tmp_path: Path) -> None:
    candidate = _candidate(tmp_path)
    original = candidate.dependency("storage_policy")
    changed_ref = _artifact(
        tmp_path,
        "changed-storage-policy.json",
        metadata={"artifact_id": "storage_policy:v2"},
        kind=REQUIRED_RELEASE_CANDIDATE_DEPENDENCY_KINDS["storage_policy"],
    )
    changed_dependency = ReleaseCandidateDependencyV1(
        name="storage_policy",
        artifact_id="storage_policy:v2",
        artifact_ref=changed_ref,
    )
    changed = replace(
        candidate,
        dependencies=tuple(
            changed_dependency if item == original else item
            for item in candidate.dependencies
        ),
        candidate_id="",
    )

    assert changed.candidate_id != candidate.candidate_id


def test_dependency_kind_must_match_executable_plan_artifact(
    tmp_path: Path,
) -> None:
    candidate = _candidate(tmp_path)
    cftc = candidate.dependency("cftc_positioning")

    with pytest.raises(
        ValueError,
        match="cftc_positioning requires cftc_positioning_corpus_v1",
    ):
        replace(
            cftc,
            artifact_ref=replace(
                cftc.artifact_ref,
                kind="certification_report_v1",
            ),
            dependency_id="",
        )


def test_campaign_catalog_exactly_binds_hashes_and_source_cutoff(
    tmp_path: Path,
) -> None:
    candidate = _candidate(tmp_path)
    source_key = next(iter(candidate.source_partition_hashes))
    changed_hashes = {
        **candidate.source_partition_hashes,
        source_key: "0" * 64,
    }

    with pytest.raises(
        ValueError,
        match="source hashes differ from dataset catalog",
    ):
        verify_reconstruction_release_candidate(
            replace(
                candidate,
                source_partition_hashes=changed_hashes,
                candidate_id="",
            )
        )

    with pytest.raises(ValueError, match="dataset exceeds source cutoff"):
        verify_reconstruction_release_candidate(
            replace(
                candidate,
                source_cutoff_ns=candidate.source_cutoff_ns - 1,
                candidate_id="",
            )
        )

    with pytest.raises(ValueError, match="period coverage is incomplete"):
        verify_reconstruction_release_candidate(
            replace(
                candidate,
                source_cutoff_ns=int(
                    datetime(2002, 5, 1, tzinfo=timezone.utc).timestamp()
                    * 1_000_000_000
                ),
                candidate_id="",
            )
        )


def test_incomplete_gates_and_mismatched_commit_fail_closed(
    tmp_path: Path,
) -> None:
    candidate = _candidate(tmp_path)

    with pytest.raises(ValueError, match="validation gates are incomplete"):
        replace(candidate, validation_gates=candidate.validation_gates[:-1])

    gate = candidate.validation_gates[0]
    with pytest.raises(ValueError, match="commit identity differs"):
        replace(gate, git_commit_sha=_sha("another-commit"), gate_id="")

    critical = next(
        item
        for item in candidate.validation_gates
        if item.gate_name == "critical_mutation_testing"
    )
    with pytest.raises(ValueError, match="critical-path evidence kind differs"):
        replace(
            critical,
            evidence_ref=replace(
                critical.evidence_ref,
                kind="generic_test_evidence",
            ),
            gate_id="",
        )


def test_holdout_graph_and_source_cutoff_must_match(tmp_path: Path) -> None:
    candidate = _candidate(tmp_path)

    with pytest.raises(ValueError, match="source cutoff differs"):
        freeze_reconstruction_release_candidate(
            **{
                **_freeze_kwargs(candidate),
                "source_cutoff_ns": candidate.source_cutoff_ns + 1,
            }
        )


def test_candidate_bound_artifact_rejects_cross_candidate_reuse(
    tmp_path: Path,
) -> None:
    candidate = _candidate(tmp_path)
    evidence_ref = _artifact(
        tmp_path,
        "qualification-output.json",
        metadata={"candidate_id": candidate.candidate_id},
    )
    binding = bind_release_candidate_artifact(
        candidate,
        artifact_role="partition-invariance-qualification",
        artifact_ref=evidence_ref,
        issued_at_utc="2026-08-20T16:00:00Z",
    )
    ref = write_release_candidate_artifact_binding(binding, tmp_path)

    assert read_release_candidate_artifact_binding(ref.path) == binding
    with pytest.raises(ValueError, match="another candidate"):
        replace(binding, candidate_id="candidate:other", binding_id="")


def test_content_addressed_reader_rejects_tampering(tmp_path: Path) -> None:
    ref = write_reconstruction_release_candidate(_candidate(tmp_path), tmp_path)
    path = Path(ref.path)
    path.write_text(path.read_text() + " ", encoding="utf-8")

    with pytest.raises(ValueError, match="not content addressed"):
        read_reconstruction_release_candidate(path)


def test_nested_artifact_hash_tampering_fails_verification(
    tmp_path: Path,
) -> None:
    candidate = _candidate(tmp_path)
    artifact = candidate.build_set.artifacts["wheel"]
    path = Path(artifact.path)
    encoded = path.read_bytes()
    path.write_bytes(bytes([encoded[0] ^ 1]) + encoded[1:])

    with pytest.raises(ValueError, match="artifact hash differs"):
        verify_reconstruction_release_candidate(candidate)


def test_filesystem_roots_must_be_absolute_disjoint_and_qualified(
    tmp_path: Path,
) -> None:
    roots = _roots(tmp_path)
    with pytest.raises(ValueError, match="path is relative"):
        replace(roots[0], path="relative/artifacts", root_id="")

    candidate = _candidate(tmp_path / "candidate")
    overlapping = replace(
        candidate.filesystem_roots[0],
        path=str(Path(candidate.filesystem_roots[1].path) / "artifacts"),
        root_id="",
    )
    with pytest.raises(ValueError, match="roots overlap"):
        replace(
            candidate,
            filesystem_roots=(overlapping, *candidate.filesystem_roots[1:]),
            candidate_id="",
        )


def test_git_inspection_requires_clean_full_identity(tmp_path: Path) -> None:
    repository = tmp_path / "repo"
    repository.mkdir()
    subprocess.run(("git", "init", "-q", str(repository)), check=True)
    subprocess.run(
        (
            "git",
            "-C",
            str(repository),
            "remote",
            "add",
            "origin",
            "example:test",
        ),
        check=True,
    )
    tracked = repository / "tracked.txt"
    tracked.write_text("tracked\n", encoding="utf-8")
    subprocess.run(
        ("git", "-C", str(repository), "add", "tracked.txt"), check=True
    )
    subprocess.run(
        (
            "git",
            "-C",
            str(repository),
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "-qm",
            "initial",
        ),
        check=True,
    )

    identity = inspect_release_candidate_git_identity(
        repository, captured_at_utc="2026-08-20T14:00:00Z"
    )
    assert len(identity.commit_sha) in {40, 64}
    assert identity.clean_tree is True

    subprocess.run(
        ("git", "-C", str(repository), "tag", "v2.5.0-rc.1"), check=True
    )
    subprocess.run(
        ("git", "-C", str(repository), "checkout", "-q", "v2.5.0-rc.1"),
        check=True,
    )
    tagged = inspect_release_candidate_git_identity(
        repository, captured_at_utc="2026-08-20T14:00:00Z"
    )
    assert tagged.ref_name == "refs/tags/v2.5.0-rc.1"

    tracked.write_text("dirty\n", encoding="utf-8")
    with pytest.raises(ValueError, match="clean Git tree"):
        inspect_release_candidate_git_identity(repository)


def test_schema_registry_discovers_candidate_contracts() -> None:
    versions = {
        item.contract_schema_version
        for item in reconstruction_schema_registry().contracts
    }

    assert "histdatacom.reconstruction-release-candidate.v1" in versions
    assert "histdatacom.release-candidate-artifact-binding.v1" in versions


def test_runtime_capture_binds_critical_dependency_versions() -> None:
    runtime = capture_release_candidate_runtime_identity(
        machine_class="local-test-runner"
    )

    assert runtime.machine_class == "local-test-runner"
    assert REQUIRED_RELEASE_CANDIDATE_RUNTIME_DEPENDENCIES.issubset(
        runtime.dependency_versions
    )
    assert runtime.compression_versions["zlib"]


def test_freeze_rejects_stale_registry_and_handler_set(tmp_path: Path) -> None:
    candidate = _candidate(tmp_path)
    kwargs = {
        **_freeze_kwargs(candidate),
        "source_cutoff_ns": candidate.source_cutoff_ns,
    }

    with pytest.raises(ValueError, match="schema registry is stale"):
        freeze_reconstruction_release_candidate(
            **{**kwargs, "schema_registry_id": "registry:stale"}
        )
    with pytest.raises(ValueError, match="handler registry differs"):
        freeze_reconstruction_release_candidate(
            **{
                **kwargs,
                "runtime_handlers": (*candidate.runtime_handlers[:-1], "x"),
            }
        )


def _freeze_kwargs(
    candidate: ReconstructionReleaseCandidateV1,
) -> dict[str, Any]:
    return {
        "git_identity": candidate.git_identity,
        "build_set": candidate.build_set,
        "runtime_identity": candidate.runtime_identity,
        "schema_registry_id": candidate.schema_registry_id,
        "dataset_revision": candidate.dataset_revision,
        "source_partition_hashes": candidate.source_partition_hashes,
        "experiment_id": candidate.experiment_id,
        "selected_engine_id": candidate.selected_engine_id,
        "dependencies": candidate.dependencies,
        "filesystem_roots": candidate.filesystem_roots,
        "validation_gates": candidate.validation_gates,
        "branch_governance": candidate.branch_governance,
        "executable_scope": candidate.executable_scope,
        "scientific_estimand": candidate.scientific_estimand,
        "scientific_nonclaims": candidate.scientific_nonclaims,
        "permitted_commands": candidate.permitted_commands,
        "runtime_handlers": candidate.runtime_handlers,
        "forbidden_fallbacks": candidate.forbidden_fallbacks,
        "release_blocking_issues": candidate.release_blocking_issues,
        "frozen_at_utc": candidate.frozen_at_utc,
    }


def test_binding_contract_requires_candidate_metadata(tmp_path: Path) -> None:
    ref = _artifact(
        tmp_path,
        "wrong-candidate.json",
        metadata={"candidate_id": "candidate:wrong"},
    )

    with pytest.raises(ValueError, match="another candidate"):
        ReleaseCandidateArtifactBindingV1(
            candidate_id="candidate:expected",
            artifact_role="certification",
            artifact_ref=ref,
            issued_at_utc="2026-08-20T16:00:00Z",
        )
