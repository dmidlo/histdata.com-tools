"""Closure gates for catalog-bound HistData reconstruction experiments."""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from histdatacom.datasets import (
    DatasetAliasV1,
    DatasetCatalog,
    DatasetDescriptorV1,
    DatasetOrigin,
    DatasetQueryScopeV1,
    FixtureProviderAdapter,
    build_observed_dataset_version,
    fixture_csv_path,
    histdata_cache_path,
)
from histdatacom.orchestration.reconstruction import artifact_ref_for_file
from histdatacom.reconstruction import ReconstructionClient
from histdatacom.reconstruction_cli import main as reconstruction_cli_main
from histdatacom.reconstruction_experiment import (
    MAX_BOUND_EXPERIMENT_ARTIFACT_BYTES,
    MAX_EXPERIMENT_BYTES,
    ReconstructionExperimentArtifactBindingV1,
    ReconstructionExperimentError,
    ReconstructionExperimentManifestV1,
    ReconstructionExperimentRole,
    ReconstructionExperimentSplitPolicyV1,
    ReconstructionExperimentSplitUnitV1,
    audit_reconstruction_experiment_splits,
    build_legacy_histdata_catalog,
    current_reconstruction_experiment_implementation,
    discover_reconstruction_experiments,
    freeze_histdata_reconstruction_experiment,
    read_reconstruction_experiment,
    verify_reconstruction_experiment,
)
from histdatacom.runtime_contracts import ArtifactRef

_SYMBOLS = ("EURGBP", "EURUSD", "GBPUSD")
_PERIOD = "202001"
_START_MS = int(datetime(2020, 1, 2, tzinfo=timezone.utc).timestamp() * 1000)


def test_implementation_identity_covers_plan_runtime_api_and_science() -> None:
    implementation = current_reconstruction_experiment_implementation()

    assert {
        "histdatacom.datasets.adapters",
        "histdatacom.reconstruction",
        "histdatacom.reconstruction_schema",
        "histdatacom.synthetic.benchmark_corpus",
        "histdatacom.synthetic.marked_hawkes",
        "histdatacom.synthetic.qualification",
        "histdatacom.synthetic.reconstruction_handlers",
        "histdatacom.synthetic.reconstruction_plan",
    }.issubset(implementation.module_sha256)


def _write_triangle(root: Path) -> None:
    for ordinal, symbol in enumerate(_SYMBOLS):
        path = histdata_cache_path(root, symbol, _PERIOD)
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                "datetime": [_START_MS + ordinal, _START_MS + 1000 + ordinal],
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


def _quality_artifact(
    root: Path,
    *,
    nested: bool = False,
    padding_bytes: int = 0,
) -> tuple[Path, ArtifactRef]:
    path = root / "quality-policy.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    identity = {"policy_id": "quality-policy:test"}
    payload = {
        "schema_version": "histdatacom.test-quality-policy.v1",
        **({"domain": identity} if nested else identity),
        "padding": "x" * padding_bytes,
    }
    path.write_text(
        json.dumps(payload, sort_keys=True),
        encoding="utf-8",
    )
    return path, artifact_ref_for_file(path, kind="test_quality_policy_v1")


def _freeze(
    root: Path,
    *,
    source_root: Path | None = None,
    nested_binding: bool = False,
    binding_padding_bytes: int = 0,
) -> tuple[ReconstructionExperimentManifestV1, ArtifactRef, Path, Path]:
    selected_source = source_root or root / "ASCII" / "T"
    _write_triangle(selected_source)
    _, quality_ref = _quality_artifact(
        root,
        nested=nested_binding,
        padding_bytes=binding_padding_bytes,
    )
    catalog, catalog_path, _ = build_legacy_histdata_catalog(
        selected_source,
        symbols=_SYMBOLS,
        periods=(_PERIOD,),
        qualification_evidence=(quality_ref,),
        path=root / "dataset-catalog.json",
    )
    binding = ReconstructionExperimentArtifactBindingV1(
        name="quality-policy",
        domain="evidence",
        artifact=quality_ref,
        artifact_id="quality-policy:test",
        artifact_identity_field=(
            "domain.policy_id" if nested_binding else "policy_id"
        ),
        dataset_roles=(
            ReconstructionExperimentRole.HISTORICAL_ANCHOR,
            ReconstructionExperimentRole.PRODUCT_INPUT,
        ),
        schema_versions=("histdatacom.test-quality-policy.v1",),
    )
    manifest, ref = freeze_histdata_reconstruction_experiment(
        catalog_path=catalog_path,
        dataset_reference="reconstruction-selected",
        query_scope=DatasetQueryScopeV1(
            symbols=_SYMBOLS,
            periods=(_PERIOD,),
            origin=DatasetOrigin.OBSERVED,
        ),
        roles=(
            ReconstructionExperimentRole.HISTORICAL_ANCHOR,
            ReconstructionExperimentRole.PRODUCT_INPUT,
        ),
        output_directory=root / "experiments",
        artifact_bindings=(binding,),
        evidence_policy_ids=("quality-policy:test",),
        preprocessing_ids=("preprocessing:test",),
        feature_schema_versions=("histdatacom.test-features.v1",),
        benchmark_gate_ids=("benchmark-gate:test",),
        limitations=("Test fixture; HistData ASCII/T only.",),
    )
    assert catalog.catalog_id in manifest.catalog_ids
    return manifest, ref, catalog_path, selected_source


def test_freeze_verify_discover_and_publication_summary_are_bounded(
    tmp_path: Path,
) -> None:
    manifest, ref, _, _ = _freeze(tmp_path)
    restored = read_reconstruction_experiment(ref.path)
    verification = verify_reconstruction_experiment(restored)
    client = ReconstructionClient()

    assert restored == manifest
    assert verification.verified
    assert verification.verified_partition_count == 3
    assert verification.verified_binding_count == 1
    assert discover_reconstruction_experiments(tmp_path) == (Path(ref.path),)
    assert client.inspect_experiment(ref.path) == manifest
    assert client.verify_experiment(ref.path).verified
    summaries = client.experiments(tmp_path)
    assert summaries[0]["experiment_id"] == manifest.experiment_id
    serialized = json.dumps(summaries, sort_keys=True)
    assert str(tmp_path) not in serialized
    assert '"rows"' not in serialized


def test_verification_resolves_nested_identity_in_authoritative_manifest(
    tmp_path: Path,
) -> None:
    manifest, _, _, _ = _freeze(tmp_path, nested_binding=True)

    verification = verify_reconstruction_experiment(manifest)

    assert verification.verified
    assert verification.verified_binding_count == 1


def test_verification_bounds_large_authoritative_domain_manifest(
    tmp_path: Path,
) -> None:
    assert MAX_BOUND_EXPERIMENT_ARTIFACT_BYTES > MAX_EXPERIMENT_BYTES
    manifest, _, _, _ = _freeze(
        tmp_path,
        binding_padding_bytes=MAX_EXPERIMENT_BYTES + 1,
    )

    verification = verify_reconstruction_experiment(manifest)

    assert verification.verified
    assert verification.verified_binding_count == 1


def test_installed_cli_lists_inspects_and_verifies_experiments(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    manifest, ref, _, _ = _freeze(tmp_path)

    assert (
        reconstruction_cli_main(
            ["--json", "experiment-list", "--root", str(tmp_path)]
        )
        == 0
    )
    listed = json.loads(capsys.readouterr().out)
    assert listed["experiments"][0]["experiment_id"] == manifest.experiment_id
    assert str(tmp_path) not in json.dumps(listed)

    assert (
        reconstruction_cli_main(
            ["--json", "experiment-inspect", "--manifest", ref.path]
        )
        == 0
    )
    inspected = json.loads(capsys.readouterr().out)
    assert inspected["experiment_id"] == manifest.experiment_id
    assert str(tmp_path) not in json.dumps(inspected)

    assert (
        reconstruction_cli_main(
            ["--json", "experiment-verify", "--manifest", ref.path]
        )
        == 0
    )
    verified = json.loads(capsys.readouterr().out)
    assert verified["status"] == "verified"


def test_scientific_identity_is_stable_across_local_materialization_roots(
    tmp_path: Path,
) -> None:
    first_root = tmp_path / "first"
    first, _, _, first_source = _freeze(first_root)
    second_root = tmp_path / "second"
    second_source = second_root / "ASCII" / "T"
    shutil.copytree(first_source, second_source)
    second, _, _, _ = _freeze(second_root, source_root=second_source)

    assert first.dataset_version_ids == second.dataset_version_ids
    assert first.catalog_ids == second.catalog_ids
    assert first.experiment_id == second.experiment_id
    assert (
        first.selections[0].local_materialization_root
        != second.selections[0].local_materialization_root
    )


def test_split_audit_rejects_partition_cohesion_overlap_and_neighbor_leakage() -> (
    None
):
    policy = ReconstructionExperimentSplitPolicyV1(neighbor_guard_ns=20)
    training = ReconstructionExperimentSplitUnitV1(
        selection_id="selection:training",
        roles=(ReconstructionExperimentRole.MODERN_REFERENCE_TRAINING,),
        partition_ids=("partition:training",),
        symbols=("EURUSD",),
        periods=("202001",),
        start_ns=0,
        end_ns=100,
        cohesion_group_ids=("duplicate-timestamp:1", "event:1"),
        row_identity_policy_ids=("source-row:test",),
        selected_fields=("bid", "ask"),
        timestamp_masking="event_time_masked_row_identity_retained",
    )
    holdout = ReconstructionExperimentSplitUnitV1(
        selection_id="selection:holdout",
        roles=(ReconstructionExperimentRole.PROTECTED_HOLDOUT,),
        partition_ids=("partition:training",),
        symbols=("EURUSD",),
        periods=("202001",),
        start_ns=90,
        end_ns=200,
        cohesion_group_ids=("duplicate-timestamp:1", "anchor:1"),
        row_identity_policy_ids=("source-row:test",),
        selected_fields=("bid", "ask"),
    )

    audit = audit_reconstruction_experiment_splits(policy, (training, holdout))

    assert not audit.accepted
    assert set(audit.finding_codes) == {
        "cohesion_group_cross_split_reuse",
        "partition_cross_split_reuse",
        "temporal_overlap_cross_split",
    }
    assert audit.shared_partition_count == 1
    assert audit.shared_cohesion_group_count == 1
    assert audit.overlap_count == 1


def test_tampered_partition_and_bound_artifact_fail_verification(
    tmp_path: Path,
) -> None:
    manifest, _, _, source = _freeze(tmp_path)
    histdata_cache_path(source, "EURUSD", _PERIOD).write_bytes(b"tampered")

    verification = verify_reconstruction_experiment(manifest)

    assert not verification.verified
    assert "dataset_selection_verification_failed" in verification.finding_codes


def test_fixture_provider_catalog_is_rejected_by_histdata_experiment(
    tmp_path: Path,
) -> None:
    source = tmp_path / "fixture"
    csv_path = fixture_csv_path(source, "EURUSD", _PERIOD)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.write_text(
        "timestamp,bid,ask,vol,native_id\n2020-01-02T00:00:00Z,1.0,1.1,0,native-1\n",
        encoding="utf-8",
    )
    _, quality_ref = _quality_artifact(tmp_path)
    adapter = FixtureProviderAdapter()
    descriptor = DatasetDescriptorV1(
        dataset_id="fixture-ticks",
        display_name="Fixture ticks",
        description="Deliberately unsupported provider fixture.",
        allowed_origins=(DatasetOrigin.OBSERVED,),
    )
    version = build_observed_dataset_version(
        adapter,
        source,
        descriptor,
        symbols=("EURUSD",),
        periods=(_PERIOD,),
        qualification_evidence=(quality_ref,),
    )
    catalog = DatasetCatalog(
        providers=(adapter.provider,),
        adapters=(adapter.descriptor,),
        datasets=(descriptor,),
        versions=(version,),
        aliases=(
            DatasetAliasV1(
                alias="fixture-selected",
                dataset_id=descriptor.dataset_id,
                dataset_version_id=version.dataset_version_id,
                revision=1,
            ),
        ),
    )
    catalog_path = catalog.write(tmp_path / "fixture-catalog.json")

    with pytest.raises(
        ReconstructionExperimentError,
        match="observed HistData.com partitions",
    ):
        freeze_histdata_reconstruction_experiment(
            catalog_path=catalog_path,
            dataset_reference="fixture-selected",
            query_scope=DatasetQueryScopeV1(
                symbols=("EURUSD",),
                periods=(_PERIOD,),
                origin=DatasetOrigin.OBSERVED,
            ),
            roles=(ReconstructionExperimentRole.PRODUCT_INPUT,),
            output_directory=tmp_path / "experiments",
            artifact_bindings=(),
            evidence_policy_ids=("quality-policy:test",),
            preprocessing_ids=("preprocessing:test",),
            feature_schema_versions=("histdatacom.test-features.v1",),
            benchmark_gate_ids=("benchmark-gate:test",),
        )
