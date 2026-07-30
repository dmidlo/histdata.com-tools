"""Tests for honest final-event activity and liquidity-proxy semantics."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from histdatacom.synthetic import (
    ACTIVITY_REVERSE_DEGRADATION_METRICS,
    SYNTHETIC_EVENT_SCHEMA_VERSION,
    ActivityAggregationSemantics,
    ActivitySliceScope,
    ActivityVolumeState,
    InformationMode,
    ReconstructionActivityBenchmarkEvidenceV1,
    ReconstructionActivityManifestV1,
    ReconstructionActivityPolicyV1,
    activity_bar_projection_semantics,
    read_reconstruction_activity_manifest,
    reconstruction_activity_benchmark_evidence,
    summarize_committed_reconstruction_activity,
    summarize_reconstruction_activity,
    summarize_reconstruction_activity_streams,
    write_reconstruction_activity_manifest,
)
from histdatacom.synthetic.contracts import SYNTHETIC_EVENT_ARROW_COLUMNS
from histdatacom.synthetic.persistence import publish_reconstruction_group
from tests.unit.test_synthetic_benchmark import _run_scorecard
from tests.unit.test_synthetic_contracts import _stream
from tests.unit.test_synthetic_persistence import _publication_inputs


def test_activity_contract_round_trip_separates_origins_and_units() -> None:
    """Observed, synthetic, and merged activity remain explicit."""
    stream = _stream(generated_count=2)

    manifest = summarize_reconstruction_activity_streams(
        (stream,),
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
        information_manifest_id="information-manifest:sha256:activity-test",
        calibration_report_id="ensemble-calibration:sha256:test",
    )

    assert ReconstructionActivityManifestV1.from_json(manifest.to_json()) == (
        manifest
    )
    assert manifest.event_count == 4
    assert manifest.symbols == ("eurusd",)
    assert manifest.to_dict()["event_schema_version"] == (
        SYNTHETIC_EVENT_SCHEMA_VERSION
    )
    assert manifest.to_dict()["event_schema_augmented"] is False
    assert manifest.to_dict()["centralized_traded_volume_claim"] is False
    by_scope = {item.scope: item for item in manifest.slices}
    assert by_scope[ActivitySliceScope.OBSERVED].event_count == 2
    assert by_scope[ActivitySliceScope.SYNTHETIC].event_count == 2
    assert by_scope[ActivitySliceScope.MERGED].event_count == 4

    synthetic = by_scope[ActivitySliceScope.SYNTHETIC]
    metrics = synthetic.metric_by_name
    assert metrics["event_count"].value == 2
    assert metrics["quote_update_count"].value == 2
    assert metrics["exposure_duration_ns"].unit == "nanosecond"
    assert metrics["tick_intensity_per_second"].unit == ("event_per_second")
    assert metrics["mean_spread"].semantics.value == ("spread_liquidity_proxy")
    assert metrics["mean_event_confidence"].value == 0.8
    assert synthetic.generator_ids == ("empirical-motif",)
    assert synthetic.generator_versions == ("1.0.0",)
    assert synthetic.reference_ids == ("reference:modern-2026",)
    assert synthetic.motif_ids == ("motif:quiet-london-001",)
    assert synthetic.feed_epoch_ids == ("feed-epoch:modern",)
    assert synthetic.broker_profile_ids == ("broker-profile:demo-v1",)
    assert synthetic.constraint_set_ids == ("constraint:sha256:historical-v1",)
    assert synthetic.stream_ids == (stream.stream_id,)
    assert synthetic.event_content_sha256


def test_activity_manifest_persistence_is_content_addressed(
    tmp_path: Path,
) -> None:
    """Published activity evidence is atomic, compact, and hash verified."""
    manifest = summarize_reconstruction_activity_streams(
        (_stream(generated_count=2),),
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
        information_manifest_id="information-manifest:sha256:activity-test",
    )

    first = write_reconstruction_activity_manifest(manifest, tmp_path)
    second = write_reconstruction_activity_manifest(manifest, tmp_path)

    assert first == second
    assert first.kind == "activity-manifest"
    assert read_reconstruction_activity_manifest(first.path) == manifest
    Path(first.path).write_bytes(Path(first.path).read_bytes() + b"changed")
    with pytest.raises(ValueError, match="hash differs"):
        read_reconstruction_activity_manifest(first.path)


def test_activity_manifest_compacts_high_cardinality_provenance() -> None:
    """Large reference sets retain a bounded prefix plus count and digest."""
    source = _stream(generated_count=3)
    events = tuple(
        replace(
            event,
            reference_id=(
                f"reference:unique-{index}"
                if event.reference_id is not None
                else None
            ),
            motif_id=(
                f"motif:unique-{index}" if event.motif_id is not None else None
            ),
            event_id="",
        )
        for index, event in enumerate(source.events)
    )
    stream = replace(source, events=events, stream_id="")

    manifest = summarize_reconstruction_activity_streams(
        (stream,),
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
        information_manifest_id="information-manifest:sha256:activity-test",
        policy=ReconstructionActivityPolicyV1(max_provenance_values=1),
    )
    synthetic = next(
        item
        for item in manifest.slices
        if item.scope is ActivitySliceScope.SYNTHETIC
    )

    assert len(synthetic.reference_ids) == 1
    assert len(synthetic.motif_ids) == 1
    assert any(
        value.startswith("reference_ids_truncated:occurrence_count=3:sha256=")
        for value in synthetic.limitations
    )
    assert any(
        value.startswith("motif_ids_truncated:occurrence_count=3:sha256=")
        for value in synthetic.limitations
    )


def test_activity_is_deterministic_and_keeps_event_schema_narrow() -> None:
    """Derived evidence never becomes a fabricated final-event column."""
    stream = _stream(generated_count=3)
    kwargs = {
        "information_mode": InformationMode.EX_POST_RECONSTRUCTION,
        "information_manifest_id": "information-manifest:sha256:stable",
    }

    first = summarize_reconstruction_activity_streams((stream,), **kwargs)
    second = summarize_reconstruction_activity_streams((stream,), **kwargs)

    assert first == second
    assert first.manifest_id == second.manifest_id
    assert len(SYNTHETIC_EVENT_ARROW_COLUMNS) == 26
    assert "volume" not in SYNTHETIC_EVENT_ARROW_COLUMNS
    assert "activity" not in SYNTHETIC_EVENT_ARROW_COLUMNS
    assert first.policy.volume_state is ActivityVolumeState.UNAVAILABLE
    assert first.to_dict()["output_mode"] == "derived_metadata"


def test_activity_information_modes_fail_closed() -> None:
    """Ex-ante evidence requires an as-of boundary; ex-post forbids one."""
    stream = _stream()

    with pytest.raises(ValueError, match="requires as_of_ns"):
        summarize_reconstruction_activity_streams(
            (stream,),
            information_mode=InformationMode.EX_ANTE_SIMULATION,
            information_manifest_id="information-manifest:sha256:mode",
        )
    with pytest.raises(ValueError, match="rejects as_of_ns"):
        summarize_reconstruction_activity_streams(
            (stream,),
            information_mode=InformationMode.EX_POST_RECONSTRUCTION,
            information_manifest_id="information-manifest:sha256:mode",
            as_of_ns=1_700_000_000_000_000_000,
        )

    ex_ante = summarize_reconstruction_activity_streams(
        (stream,),
        information_mode=InformationMode.EX_ANTE_SIMULATION,
        information_manifest_id="information-manifest:sha256:mode",
        as_of_ns=1_700_000_000_000_000_000,
    )
    assert ex_ante.information_mode is InformationMode.EX_ANTE_SIMULATION
    assert ex_ante.as_of_ns == 1_700_000_000_000_000_000


def test_activity_rejects_unsupported_size_claims_and_unordered_events() -> (
    None
):
    """Missing source sizes and unstable ordering cannot become evidence."""
    stream = _stream()
    source_size_policy = ReconstructionActivityPolicyV1(
        volume_state=ActivityVolumeState.OBSERVED_SOURCE_SIZE
    )
    with pytest.raises(ValueError, match="no source-supported size fields"):
        summarize_reconstruction_activity_streams(
            (stream,),
            information_mode=InformationMode.EX_POST_RECONSTRUCTION,
            information_manifest_id="information-manifest:sha256:size",
            policy=source_size_policy,
        )

    with pytest.raises(ValueError, match="strictly ordered"):
        summarize_reconstruction_activity(
            reversed(stream.events),
            run_id=stream.run_id,
            ensemble_member_id=stream.ensemble_member_id,
            information_mode=InformationMode.EX_POST_RECONSTRUCTION,
            information_manifest_id="information-manifest:sha256:order",
        )


def test_activity_provenance_limits_publish_count_and_digest() -> None:
    """Bounded metadata compacts excess provenance without losing evidence."""
    stream = _stream(generated_count=2)
    events = tuple(
        replace(
            event,
            source_version_id=f"source-version:{index}",
            event_id="",
        )
        for index, event in enumerate(stream.events, start=1)
    )
    policy = ReconstructionActivityPolicyV1(max_provenance_values=1)

    manifest = summarize_reconstruction_activity(
        events,
        run_id=stream.run_id,
        ensemble_member_id=stream.ensemble_member_id,
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
        information_manifest_id="information-manifest:sha256:bounded",
        policy=policy,
    )

    assert all(len(item.source_version_ids) == 1 for item in manifest.slices)
    assert all(
        any(
            value.startswith("source_version_ids_truncated:occurrence_count=")
            and ":sha256=" in value
            for value in item.limitations
        )
        for item in manifest.slices
    )


def test_bar_projection_semantics_are_explicit_and_non_volume() -> None:
    """#18 receives sum/recompute/weighted-mean rules, not a volume guess."""
    projection = activity_bar_projection_semantics()

    assert projection["tick_count"]["operation"] == "sum"
    assert projection["activity_duration_ns"]["operation"] == (
        "recompute_from_bar_event_bounds"
    )
    assert projection["tick_intensity_per_second"]["operation"] == (
        "recompute_count_divided_by_activity_duration"
    )
    assert projection["mean_spread"]["operation"] == (
        "event_support_weighted_mean"
    )
    assert projection["volume"] == {
        "source_metric": None,
        "operation": "unavailable_unless_separately_sourced",
        "unit": None,
    }


def test_committed_parquet_activity_matches_in_memory_streaming(
    tmp_path: Path,
) -> None:
    """Projected one-row batches reproduce the in-memory final event slices."""
    rendered, anchors, storage_policy, retention = _publication_inputs(tmp_path)
    published = publish_reconstruction_group(
        tmp_path / "archive",
        rendered,
        immutable_source_anchors=anchors,
        symbol_group_id="eurusd-triangle",
        retention_plan=retention,
        storage_policy=storage_policy,
        row_group_size=2,
    )
    information_id = "information-manifest:sha256:committed"

    committed = summarize_committed_reconstruction_activity(
        published.manifest_path,
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
        information_manifest_id=information_id,
        batch_size=1,
    )
    in_memory = summarize_reconstruction_activity_streams(
        rendered.streams,
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
        information_manifest_id=information_id,
        window_id=published.manifest.window_id,
        synchronization_unit_id=(published.manifest.synchronization_unit_id),
        product_manifest_id=published.manifest.manifest_id,
    )

    assert committed == in_memory
    assert committed.product_manifest_id == published.manifest.manifest_id
    assert committed.event_count == published.manifest.event_count
    assert all(item.stream_ids for item in committed.slices)


def test_existing_reverse_degradation_scorecard_supplies_activity_evidence() -> (
    None
):
    """Activity validation reuses the established benchmark and calibration."""
    _, scorecard = _run_scorecard()

    evidence = reconstruction_activity_benchmark_evidence(scorecard)

    assert (
        ReconstructionActivityBenchmarkEvidenceV1.from_dict(evidence.to_dict())
        == evidence
    )
    assert set(evidence.metric_support_counts) == set(
        ACTIVITY_REVERSE_DEGRADATION_METRICS
    )
    assert all(value > 0 for value in evidence.metric_support_counts.values())
    assert evidence.calibration_supported_candidate_count > 0
    assert evidence.execution_failure_count == 0
    assert evidence.to_dict()["automatic_winner"] is False
    assert evidence.to_dict()["winner_candidate_id"] is None


def test_activity_metric_definition_drift_is_rejected() -> None:
    """Units and aggregation semantics are part of the versioned contract."""
    manifest = summarize_reconstruction_activity_streams(
        (_stream(),),
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
        information_manifest_id="information-manifest:sha256:metric-drift",
    )
    metric = manifest.slices[0].metrics[0]

    with pytest.raises(ValueError, match="definition differs"):
        replace(
            metric,
            aggregation=ActivityAggregationSemantics.MAXIMUM,
        )


def test_activity_json_rejects_derived_claim_drift() -> None:
    """Serialized evidence cannot be relabeled as volume or a winner."""
    manifest = summarize_reconstruction_activity_streams(
        (_stream(),),
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
        information_manifest_id="information-manifest:sha256:claim-drift",
    )
    payload = manifest.to_dict()
    payload["centralized_traded_volume_claim"] = True

    with pytest.raises(ValueError, match="derived activity field"):
        ReconstructionActivityManifestV1.from_dict(payload)
