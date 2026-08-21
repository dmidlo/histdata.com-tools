"""Tests for measured reconstruction campaign resource envelopes."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from histdatacom.orchestration.reconstruction import artifact_ref_for_file
from histdatacom.runtime_contracts import ArtifactRef
from histdatacom.synthetic import resource_envelopes as resource_module
from histdatacom.synthetic.resource_envelopes import (
    REQUIRED_MEASUREMENT_STRATA,
    CampaignResourceForecastV1,
    ReconstructionResourceAuditError,
    ReconstructionResourceMeasurementCorpusV1,
    ReconstructionResourceMeasurementV1,
    ReconstructionResourceProbeV1,
    ReconstructionResourceRuntimeTelemetryV1,
    ReconstructionStorageQualificationV1,
    admit_campaign_resources,
    build_campaign_resource_audit,
    build_resource_measurement_corpus,
    fit_resource_envelopes,
    forecast_campaign_resources,
    measure_reconstruction_resource_probe,
    read_campaign_resource_audit,
    read_resource_measurement_corpus,
    read_storage_qualification,
    review_physical_packing,
    write_resource_measurement_corpus,
    write_storage_qualification,
)


def _ref(
    *, kind: str = "test", metadata: dict[str, Any] | None = None
) -> ArtifactRef:
    return ArtifactRef(
        kind=kind,
        path="/tmp/resource-evidence.json",
        size_bytes=1,
        sha256="a" * 64,
        metadata=metadata or {},
    )


def _measurement(
    case_id: str,
    outcome: str,
    *,
    era: str,
    missingness: str,
    alignment: str,
    deficit: str,
    split: str,
    logical: int = 0,
    observed: int = 0,
    synthetic: int = 0,
    scale: int = 1,
) -> ReconstructionResourceMeasurementV1:
    success = outcome == "success"
    parquet_bytes = 80 * scale if success else 0
    manifest_bytes = 20 * scale if success else 0
    directory_bytes = 110 * scale if success else 0
    candidate_count = max(1, observed * 2) if success else scale
    return ReconstructionResourceMeasurementV1(
        probe_id=f"probe-{case_id}",
        case_id=case_id,
        terminal_outcome=outcome,
        strata={
            "era": era,
            "missingness": missingness,
            "alignment": alignment,
            "deficit": deficit,
            "split": split,
            "member_scope": "all_retained",
        },
        product_manifest_ref=_ref() if success else None,
        publication_id=f"publication-{case_id}" if success else None,
        logical_event_count=logical if success else 0,
        observed_event_count=observed if success else 0,
        synthetic_event_count=synthetic if success else 0,
        physical_row_count=synthetic if success else 0,
        parquet_bytes=parquet_bytes,
        parquet_uncompressed_bytes=parquet_bytes * 2,
        manifest_bytes=manifest_bytes,
        directory_bytes=directory_bytes,
        inode_count=3 if success else 0,
        row_group_count=1 if success else 0,
        row_group_occupancy=(synthetic / 100 if success else 0.0),
        bytes_per_synthetic_event=(
            parquet_bytes / synthetic if success and synthetic else 0.0
        ),
        bytes_per_logical_event=(
            parquet_bytes / logical if success and logical else 0.0
        ),
        compression_ratio=0.5 if success else 0.0,
        verification_read_bytes=200 * scale if success else 0,
        verify_wall_seconds=float(scale) if success else 0.0,
        verify_throughput_bytes_per_second=200.0 if success else 0.0,
        wall_seconds=float(10 * scale),
        cpu_seconds=float(5 * scale),
        peak_rss_bytes=1_000 * scale,
        peak_scratch_bytes=500 * scale,
        stage_output_bytes=100 * scale if success else 0,
        write_amplification=1.5 if success else 0.0,
        candidate_event_count=candidate_count,
        candidate_amplification=(
            candidate_count / observed if success else 0.0
        ),
        poisson_work_units=30 * scale,
        temporal_history_bytes=40 * scale,
        checkpoint_bytes=50 * scale,
        cleanup_status={
            "success": "committed_scratch_removed",
            "refusal": "refused_scratch_removed",
            "cancellation": "cancelled_scratch_removed",
            "failure": "failed_scratch_removed",
        }[outcome],
    )


def _corpus() -> ReconstructionResourceMeasurementCorpusV1:
    return ReconstructionResourceMeasurementCorpusV1(
        measurements=(
            _measurement(
                "early",
                "success",
                era="early_sparse",
                missingness="low",
                alignment="exact",
                deficit="zero",
                split="unsplit_or_shallow",
                logical=10,
                observed=10,
                synthetic=0,
                scale=1,
            ),
            _measurement(
                "transition",
                "success",
                era="feed_transition",
                missingness="median",
                alignment="bounded_nearest",
                deficit="positive",
                split="deep_recursive",
                logical=20,
                observed=12,
                synthetic=8,
                scale=2,
            ),
            _measurement(
                "crisis",
                "success",
                era="crisis_high_activity",
                missingness="high",
                alignment="exact",
                deficit="positive",
                split="deep_recursive",
                logical=40,
                observed=24,
                synthetic=16,
                scale=3,
            ),
            _measurement(
                "modern",
                "refusal",
                era="modern_dense",
                missingness="low",
                alignment="bounded_nearest",
                deficit="zero",
                split="unsplit_or_shallow",
                scale=4,
            ),
            _measurement(
                "cancel",
                "cancellation",
                era="modern_dense",
                missingness="median",
                alignment="exact",
                deficit="positive",
                split="deep_recursive",
                scale=5,
            ),
            _measurement(
                "fail",
                "failure",
                era="early_sparse",
                missingness="high",
                alignment="bounded_nearest",
                deficit="zero",
                split="unsplit_or_shallow",
                scale=6,
            ),
        )
    )


def _forecast(**changes: Any) -> CampaignResourceForecastV1:
    values: dict[str, Any] = {
        "final_support_map_id": "final-support:test",
        "product_count": 10,
        "logical_event_count": 1_000,
        "observed_event_count": 800,
        "synthetic_event_count": 200,
        "candidate_event_count": 2_000,
        "output_bytes_lower": 5_000,
        "output_bytes_upper": 10_000,
        "peak_scratch_bytes_per_worker": 2_000,
        "peak_rss_bytes_per_worker": 1_000,
        "inode_count_upper": 30,
        "temporal_history_bytes_upper": 400,
        "checkpoint_bytes_upper": 500,
        "poisson_work_units_upper": 600,
        "verification_read_bytes_lower": 6_000,
        "verification_read_bytes_upper": 12_000,
        "verify_seconds_lower": 1.0,
        "verify_seconds_upper": 2.0,
        "campaign_seconds_lower": 10.0,
        "campaign_seconds_upper": 20.0,
        "campaign_cpu_seconds_lower": 5.0,
        "campaign_cpu_seconds_upper": 10.0,
        "write_amplification_upper": 2.0,
        "candidate_amplification_upper": 3.0,
        "extrapolation_factor": 1.0,
    }
    values.update(changes)
    return CampaignResourceForecastV1(**values)


def _storage_qualification(
    tmp_path: Path,
) -> ReconstructionStorageQualificationV1:
    sentinel = "c" * 64
    evidence_path = tmp_path / "disconnect.json"
    evidence_path.write_text('{"result":"failed-closed"}\n', encoding="utf-8")
    disconnect_evidence = artifact_ref_for_file(
        evidence_path,
        kind="storage_disconnect_drill",
        metadata={
            "filesystem_id": "fs-1",
            "device_id": "device-1",
            "failed_closed": True,
            "local_fallback_absent": True,
        },
    )
    qualification_evidence_path = tmp_path / "storage-measurement.json"
    qualification_evidence_path.write_text(
        '{"measurement":"qualified"}\n', encoding="utf-8"
    )
    qualification_evidence = artifact_ref_for_file(
        qualification_evidence_path,
        kind="storage_measurement",
        metadata={
            "filesystem_id": "fs-1",
            "device_id": "device-1",
            "remounted_filesystem_id": "fs-1",
            "remounted_device_id": "device-1",
            "sustained_test_bytes": 10_000,
            "write_throughput_bytes_per_second": 1_000.0,
            "read_throughput_bytes_per_second": 2_000.0,
            "sentinel_sha256_before": sentinel,
            "sentinel_sha256_after": sentinel,
            "same_filesystem": True,
            "non_sparse_write_verified": True,
            "remount_hash_verified": True,
            "all_terminal_cleanup_verified": True,
        },
    )
    return ReconstructionStorageQualificationV1(
        output_root=str(tmp_path / "mount" / "output"),
        scratch_root=str(tmp_path / "mount" / "scratch"),
        filesystem_id="fs-1",
        device_id="device-1",
        remounted_filesystem_id="fs-1",
        remounted_device_id="device-1",
        sustained_test_bytes=10_000,
        write_throughput_bytes_per_second=1_000.0,
        read_throughput_bytes_per_second=2_000.0,
        sentinel_sha256_before=sentinel,
        sentinel_sha256_after=sentinel,
        qualification_evidence_ref=qualification_evidence,
        disconnect_evidence_ref=disconnect_evidence,
        same_filesystem=True,
        non_sparse_write_verified=True,
        disconnect_failed_closed=True,
        local_fallback_absent=True,
        remount_hash_verified=True,
        success_cleanup_verified=True,
        refusal_cleanup_verified=True,
        cancellation_cleanup_verified=True,
        failure_cleanup_verified=True,
        qualified_at_utc="2026-08-21T12:00:00Z",
    )


def test_corpus_enforces_full_stratified_terminal_census_and_round_trips(
    tmp_path: Path,
) -> None:
    corpus = _corpus()

    for axis, values in REQUIRED_MEASUREMENT_STRATA.items():
        assert values == {item.strata[axis] for item in corpus.measurements}
    assert corpus.terminal_counts == {
        "cancellation": 1,
        "failure": 1,
        "refusal": 1,
        "success": 3,
    }
    assert type(corpus).from_dict(corpus.to_dict()) == corpus
    assert corpus.to_dict()["aggregate_workload"] == {
        "measurement_count": 6,
        "successful_product_count": 3,
        "logical_event_count": 70,
        "observed_event_count": 46,
        "synthetic_event_count": 24,
        "physical_row_count": 24,
        "parquet_bytes": 480,
        "manifest_bytes": 120,
        "directory_bytes": 660,
        "inode_count": 9,
        "row_group_count": 3,
        "verification_read_bytes": 1_200,
        "stage_output_bytes": 600,
        "candidate_event_count": 107,
        "poisson_work_units": 630,
        "temporal_history_bytes": 840,
        "checkpoint_bytes": 1_050,
        "wall_seconds": 210.0,
        "cpu_seconds": 105.0,
        "maximum_peak_rss_bytes": 6_000,
        "maximum_peak_scratch_bytes": 3_000,
    }

    ref = write_resource_measurement_corpus(corpus, tmp_path)
    assert read_resource_measurement_corpus(ref.path) == corpus

    incomplete = tuple(
        item
        for item in corpus.measurements
        if item.terminal_outcome != "failure"
    )
    with pytest.raises(
        ReconstructionResourceAuditError, match="success/refusal"
    ):
        ReconstructionResourceMeasurementCorpusV1(incomplete)


def test_envelopes_use_nearest_rank_high_tail_and_never_the_mean() -> None:
    corpus = _corpus()

    envelopes = fit_resource_envelopes(corpus, quantile=0.95)
    by_metric = {item.metric: item for item in envelopes}

    scratch_values = [500.0, 1_000.0, 1_500.0, 2_000.0, 2_500.0, 3_000.0]
    assert by_metric[
        "peak_scratch_bytes_per_worker"
    ].high_quantile_value == max(scratch_values)
    assert by_metric["peak_scratch_bytes_per_worker"].high_quantile_value != (
        sum(scratch_values) / len(scratch_values)
    )
    assert by_metric[
        "peak_scratch_bytes_per_worker"
    ].conservative_upper_bound == max(scratch_values)
    assert (
        by_metric["peak_scratch_bytes_per_worker"].maximum_absolute_residual
        == 2_500.0
    )
    assert by_metric["bytes_per_synthetic_event"].sample_count == 2


def test_forecast_uses_all_members_manifest_overhead_and_slowest_verification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    corpus = _corpus()
    envelopes = fit_resource_envelopes(corpus)
    windows = (
        SimpleNamespace(
            status="executable",
            member_count=3,
            core_event_counts={"a": 4, "b": 5, "c": 6},
            modeled_missing_event_count=5,
            candidate_amplification=2.0,
        ),
        SimpleNamespace(
            status="refused",
            member_count=0,
            core_event_counts={},
            modeled_missing_event_count=0,
            candidate_amplification=0.0,
        ),
    )
    monkeypatch.setattr(
        resource_module,
        "read_final_support_verification_shard",
        lambda _: SimpleNamespace(windows=windows),
    )
    support = cast(
        Any,
        SimpleNamespace(
            final_support_map_id="final-support:test",
            verification_shard_refs=(SimpleNamespace(path="shard.json"),),
        ),
    )

    forecast = forecast_campaign_resources(support, corpus, envelopes)
    by_metric = {item.metric: item for item in envelopes}

    assert forecast.product_count == 3
    assert forecast.observed_event_count == 45
    assert forecast.synthetic_event_count == 15
    assert forecast.candidate_event_count == 90
    expected_upper = (
        60 * by_metric["bytes_per_logical_event"].high_quantile_value
        + 3 * by_metric["manifest_bytes_per_product"].high_quantile_value
    )
    assert forecast.output_bytes_upper == int(expected_upper)
    assert forecast.verify_seconds_upper == pytest.approx(
        forecast.verification_read_bytes_upper
        / by_metric["verify_throughput_bytes_per_second"].minimum_value
    )
    assert (
        forecast.campaign_cpu_seconds_upper
        >= forecast.campaign_cpu_seconds_lower
    )
    assert forecast.poisson_work_units_upper > 0
    assert forecast.write_amplification_upper == 1.5


def test_forecast_refuses_extrapolation_beyond_measured_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    corpus = _corpus()
    envelopes = fit_resource_envelopes(corpus, extrapolation_limit_factor=2.0)
    window = SimpleNamespace(
        status="executable",
        member_count=1,
        core_event_counts={"a": 1_000},
        modeled_missing_event_count=0,
        candidate_amplification=1.0,
    )
    monkeypatch.setattr(
        resource_module,
        "read_final_support_verification_shard",
        lambda _: SimpleNamespace(windows=(window,)),
    )
    support = cast(
        Any,
        SimpleNamespace(
            final_support_map_id="final-support:test",
            verification_shard_refs=(SimpleNamespace(path="shard.json"),),
        ),
    )

    with pytest.raises(ReconstructionResourceAuditError, match="extrapolation"):
        forecast_campaign_resources(support, corpus, envelopes)


def test_admission_reserves_capacity_and_freezes_concurrency_and_shards() -> (
    None
):
    admitted = admit_campaign_resources(
        _forecast(),
        available_memory_bytes=8_000,
        available_scratch_bytes=16_000,
        available_output_bytes=20_000,
        available_inodes=100,
        maximum_campaign_seconds=10.0,
        reserve_fraction=0.25,
        maximum_container_bytes=5_000,
        maximum_products_per_container=4,
    )

    assert admitted.status == "admitted"
    assert admitted.concurrency == 6
    assert admitted.products_per_shard == 4
    assert type(admitted).from_dict(admitted.to_dict()) == admitted

    refused = admit_campaign_resources(
        _forecast(output_bytes_upper=15_001),
        available_memory_bytes=8_000,
        available_scratch_bytes=16_000,
        available_output_bytes=20_000,
        available_inodes=100,
        maximum_campaign_seconds=10.0,
        reserve_fraction=0.25,
    )
    assert refused.status == "refused"
    assert (
        "output_forecast_exceeds_reserved_capacity" in refused.refusal_reasons
    )

    with pytest.raises(ReconstructionResourceAuditError, match="reserve"):
        admit_campaign_resources(
            _forecast(),
            available_memory_bytes=8_000,
            available_scratch_bytes=16_000,
            available_output_bytes=20_000,
            available_inodes=100,
            maximum_campaign_seconds=10.0,
            reserve_fraction=0.9,
        )


def test_packing_stays_per_product_without_partition_evidence() -> None:
    corpus = _corpus()
    policy = admit_campaign_resources(
        _forecast(),
        available_memory_bytes=8_000,
        available_scratch_bytes=16_000,
        available_output_bytes=20_000,
        available_inodes=100,
        maximum_campaign_seconds=10.0,
    )

    conservative = review_physical_packing(corpus, policy)
    qualified = review_physical_packing(
        corpus,
        policy,
        partition_sensitivity_evidence_available=True,
    )

    assert conservative.decision == "retain_per_product"
    assert qualified.decision == "bounded_immutable_containers"
    assert qualified.per_window_identity_preserved is True
    assert qualified.independent_replay_preserved is True


def test_probe_measurement_reads_v3_delta_rows_and_physical_parquet(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "product"
    root.mkdir()
    parquet_path = root / "synthetic.parquet"
    pq.write_table(
        pa.table({"value": [1, 2, 3]}), parquet_path, row_group_size=2
    )
    manifest_path = root / "manifest.json"
    manifest_path.write_text("{}\n", encoding="utf-8")
    manifest_ref = artifact_ref_for_file(
        manifest_path,
        kind="reconstruction-product-manifest",
    )
    source_ref = ArtifactRef(
        kind="source",
        path=str(tmp_path / "source.parquet"),
        size_bytes=101,
        sha256="b" * 64,
    )

    class FakeV3:
        publication_id = "publication:test"
        partitions = (
            SimpleNamespace(relative_path="synthetic.parquet", row_count=3),
        )
        replay = SimpleNamespace(row_group_size=2)
        observed_anchor_segments = (
            SimpleNamespace(source_artifact=source_ref),
        )
        source = SimpleNamespace(observed_event_count=7)
        constraints = SimpleNamespace(synthetic_event_count=3)
        event_count = 10

    monkeypatch.setattr(
        resource_module, "ReconstructionProductManifestV3", FakeV3
    )
    monkeypatch.setattr(
        resource_module,
        "verify_reconstruction_publication",
        lambda _: FakeV3(),
    )
    probe = ReconstructionResourceProbeV1(
        case_id="physical-v3",
        terminal_outcome="success",
        strata={
            "era": "modern_dense",
            "missingness": "median",
            "alignment": "exact",
            "deficit": "positive",
            "split": "unsplit_or_shallow",
            "member_scope": "all_retained",
        },
        telemetry=ReconstructionResourceRuntimeTelemetryV1(
            wall_seconds=2.0,
            cpu_seconds=1.5,
            peak_rss_bytes=1_000,
            peak_scratch_bytes=500,
            stage_output_bytes=parquet_path.stat().st_size,
            candidate_event_count=14,
            poisson_work_units=3,
            temporal_history_bytes=50,
            checkpoint_bytes=25,
            cleanup_status="committed_scratch_removed",
        ),
        product_manifest_ref=manifest_ref,
    )

    measured = measure_reconstruction_resource_probe(probe)

    assert measured.logical_event_count == 10
    assert measured.observed_event_count == 7
    assert measured.synthetic_event_count == measured.physical_row_count == 3
    assert measured.parquet_bytes == parquet_path.stat().st_size
    assert measured.row_group_count == 2
    assert measured.verification_read_bytes == (
        measured.manifest_bytes + measured.parquet_bytes + 101
    )
    assert measured.bytes_per_logical_event == pytest.approx(
        measured.parquet_bytes / 10
    )


def test_non_success_probes_capture_cleanup_without_product_bytes() -> None:
    probes = []
    for outcome, cleanup in (
        ("refusal", "refused_scratch_removed"),
        ("cancellation", "cancelled_scratch_removed"),
        ("failure", "failed_scratch_removed"),
    ):
        probes.append(
            ReconstructionResourceProbeV1(
                case_id=outcome,
                terminal_outcome=outcome,
                strata={},
                telemetry=ReconstructionResourceRuntimeTelemetryV1(
                    wall_seconds=1.0,
                    cpu_seconds=0.5,
                    peak_rss_bytes=10,
                    peak_scratch_bytes=20,
                    stage_output_bytes=0,
                    candidate_event_count=1,
                    poisson_work_units=1,
                    temporal_history_bytes=1,
                    checkpoint_bytes=1,
                    cleanup_status=cleanup,
                ),
            )
        )

    measured = tuple(
        measure_reconstruction_resource_probe(item) for item in probes
    )

    assert {item.terminal_outcome for item in measured} == {
        "refusal",
        "cancellation",
        "failure",
    }
    assert all(
        item.directory_bytes == item.inode_count == 0 for item in measured
    )


def test_storage_qualification_verifies_disconnect_evidence_and_round_trips(
    tmp_path: Path,
) -> None:
    qualification = _storage_qualification(tmp_path)
    evidence_path = Path(qualification.disconnect_evidence_ref.path)

    ref = write_storage_qualification(qualification, tmp_path / "qualification")
    assert read_storage_qualification(ref.path) == qualification

    evidence_path.write_text('{"result":"tampered"}\n', encoding="utf-8")
    with pytest.raises(ValueError, match="(size|sha256) differs"):
        read_storage_qualification(ref.path)


def test_campaign_audit_writes_rereads_and_recomputes_all_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = _storage_qualification(tmp_path)
    support_path = tmp_path / "final-support.json"
    support_path.write_text("{}\n", encoding="utf-8")
    candidate_path = tmp_path / "release-candidate.json"
    candidate_path.write_text("{}\n", encoding="utf-8")
    candidate_id = "reconstruction-release-candidate:test"
    support = SimpleNamespace(
        final_support_map_id="final-adaptive-support-map:test",
        release_candidate_id=candidate_id,
        verification_shard_refs=(
            SimpleNamespace(path="verification-shard.json"),
        ),
    )
    candidate = SimpleNamespace(
        candidate_id=candidate_id,
        filesystem_roots=(
            SimpleNamespace(
                role="output",
                path=storage.output_root,
                filesystem_id=storage.filesystem_id,
                device_id=storage.device_id,
            ),
            SimpleNamespace(
                role="scratch",
                path=storage.scratch_root,
                filesystem_id=storage.filesystem_id,
                device_id=storage.device_id,
            ),
        ),
    )
    window = SimpleNamespace(
        status="executable",
        member_count=2,
        core_event_counts={"a": 10, "b": 10, "c": 10},
        modeled_missing_event_count=10,
        candidate_amplification=1.0,
    )
    monkeypatch.setattr(
        resource_module,
        "read_final_adaptive_support_map_index",
        lambda _: support,
    )
    monkeypatch.setattr(
        resource_module,
        "read_reconstruction_release_candidate",
        lambda _: candidate,
    )
    monkeypatch.setattr(
        resource_module,
        "read_final_support_verification_shard",
        lambda _: SimpleNamespace(windows=(window,)),
    )

    ref = build_campaign_resource_audit(
        final_support_map_path=support_path,
        release_candidate_path=candidate_path,
        corpus=_corpus(),
        storage=storage,
        available_memory_bytes=100_000,
        available_scratch_bytes=100_000,
        available_output_bytes=100_000,
        available_inodes=1_000,
        maximum_campaign_seconds=10_000.0,
        output_directory=tmp_path / "audit",
    )
    audit = read_campaign_resource_audit(ref.path)

    assert audit.status == "qualified"
    assert audit.policy.status == "admitted"
    assert audit.forecast.product_count == 2
    assert audit.forecast.logical_event_count == 80
    assert audit.forecast.write_amplification_upper == 1.5
    assert audit.packing_review.decision == "retain_per_product"


def test_build_corpus_measures_every_probe_before_census_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    corpus = _corpus()
    probes = tuple(
        SimpleNamespace(case_id=item.case_id) for item in corpus.measurements
    )
    indexed = {item.case_id: item for item in corpus.measurements}
    monkeypatch.setattr(
        resource_module,
        "measure_reconstruction_resource_probe",
        lambda probe: indexed[probe.case_id],
    )

    rebuilt = build_resource_measurement_corpus(cast(Any, probes))

    assert rebuilt == corpus


def test_measurement_identity_detects_derived_field_tampering() -> None:
    measurement = _corpus().measurements[0]

    with pytest.raises(ReconstructionResourceAuditError, match="identity"):
        replace(
            measurement, peak_scratch_bytes=measurement.peak_scratch_bytes + 1
        )
