"""Tests for bounded reconstruction stream and checkpoint contracts."""

from __future__ import annotations

import asyncio
from dataclasses import replace
import hashlib
import json
from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st
import pytest
from temporalio.converter import DataConverter

from histdatacom.runtime_contracts import ArtifactRef
from histdatacom.synthetic import (
    CARRY_STATE_SCHEMA_VERSION,
    EVENT_BATCH_SCHEMA_VERSION,
    PARTITION_MANIFEST_SCHEMA_VERSION,
    RECONSTRUCTION_CHECKPOINT_SCHEMA_VERSION,
    RECONSTRUCTION_HEARTBEAT_SCHEMA_VERSION,
    RECONSTRUCTION_RESOURCE_ESTIMATE_SCHEMA_VERSION,
    RECONSTRUCTION_RUN_SCHEMA_VERSION,
    RECONSTRUCTION_STORAGE_POLICY_SCHEMA_VERSION,
    RECONSTRUCTION_WINDOW_SCHEMA_VERSION,
    REJECTION_SUMMARY_SCHEMA_VERSION,
    CarryStateV1,
    EventBatchV1,
    PartitionManifestV1,
    ReconstructionCheckpointV1,
    ReconstructionCommitPhase,
    ReconstructionHeartbeatV1,
    ReconstructionResourceEstimateV1,
    ReconstructionResourceLimitError,
    ReconstructionRunV1,
    ReconstructionStoragePolicyV1,
    ReconstructionWindowV1,
    RejectionSummaryV1,
    artifact_ref_for_json_contract,
    plan_reconstruction_windows,
    validate_reconstruction_window_plan,
)

START_NS = 1_700_000_000_000_000_000
END_NS = START_NS + 12_000
SYMBOLS = ("eurusd", "gbpusd", "eurgbp")


def _ref(
    kind: str,
    path: str,
    content: str,
    *,
    metadata: dict | None = None,
) -> ArtifactRef:
    encoded = content.encode("utf-8")
    return ArtifactRef(
        kind=kind,
        path=path,
        size_bytes=len(encoded),
        sha256=hashlib.sha256(encoded).hexdigest(),
        metadata=dict(metadata or {}),
    )


def _run(
    *,
    policy: ReconstructionStoragePolicyV1 | None = None,
) -> ReconstructionRunV1:
    return ReconstructionRunV1(
        symbols=SYMBOLS,
        source_version_ids=("source:sha256:historical-v1",),
        configuration_ids=("config:sha256:reconstruction-v1",),
        ensemble_member_ids=("member-000", "member-001"),
        base_seed=20260713,
        storage_policy=policy or ReconstructionStoragePolicyV1(),
    )


def _window() -> ReconstructionWindowV1:
    return plan_reconstruction_windows(
        _run(),
        ensemble_member_id="member-000",
        start_ns=START_NS,
        end_ns=END_NS,
        window_size_ns=4_000,
        left_halo_ns=1_000,
        right_lookahead_ns=2_000,
    )[0]


def _batch(
    window: ReconstructionWindowV1,
    symbol: str,
    ordinal: int,
    *,
    event_count: int = 3,
) -> EventBatchV1:
    normalized_symbol = symbol.lower()
    content = f"{window.window_id}:{normalized_symbol}:{ordinal}:{event_count}"
    content_sha256 = hashlib.sha256(content.encode("utf-8")).hexdigest()
    return EventBatchV1(
        run_id=window.run_id,
        window_id=window.window_id,
        synchronization_unit_id=window.synchronization_unit_id,
        ensemble_member_id=window.ensemble_member_id,
        symbol=normalized_symbol,
        batch_ordinal=ordinal,
        event_count=event_count,
        ownership_start_ns=window.core_start_ns,
        ownership_end_ns=window.core_end_ns,
        first_event_time_ns=window.core_start_ns + ordinal,
        last_event_time_ns=window.core_start_ns + ordinal + 100,
        content_sha256=content_sha256,
        artifact=_ref(
            "synthetic-event-batch",
            f"scratch/{window.window_id}/{normalized_symbol}/{ordinal}.parquet",
            content,
            metadata={"event_count": event_count},
        ),
    )


def _carry(window: ReconstructionWindowV1) -> CarryStateV1:
    return CarryStateV1(
        run_id=window.run_id,
        ensemble_member_id=window.ensemble_member_id,
        symbol_watermarks_ns={
            symbol: window.core_end_ns for symbol in window.symbols
        },
        last_event_ids={
            symbol: f"event:sha256:{symbol}" for symbol in window.symbols
        },
        state_artifacts=(
            _ref(
                "reconstruction-carry-detail",
                f"scratch/{window.window_id}/carry.parquet",
                "bounded carry detail",
            ),
        ),
    )


def _rejections(window: ReconstructionWindowV1) -> RejectionSummaryV1:
    return RejectionSummaryV1(
        run_id=window.run_id,
        window_id=window.window_id,
        candidate_count=12,
        accepted_count=9,
        rejected_count=3,
        reason_counts={"negative_spread": 1, "weekend_closure": 2},
    )


def _manifest(
    window: ReconstructionWindowV1,
) -> tuple[PartitionManifestV1, tuple[EventBatchV1, ...]]:
    batches = tuple(_batch(window, symbol, 0) for symbol in window.symbols)
    carry = _carry(window)
    rejections = _rejections(window)
    manifest = PartitionManifestV1(
        run_id=window.run_id,
        window_id=window.window_id,
        synchronization_unit_id=window.synchronization_unit_id,
        ensemble_member_id=window.ensemble_member_id,
        symbols=window.symbols,
        symbol_event_counts={symbol: 3 for symbol in window.symbols},
        event_batches=batches,
        rejection_summary_ref=artifact_ref_for_json_contract(
            rejections,
            kind="rejection-summary",
            path=f"scratch/{window.window_id}/rejections.json",
        ),
        carry_state_ref=artifact_ref_for_json_contract(
            carry,
            kind="carry-state",
            path=f"scratch/{window.window_id}/carry.json",
        ),
    )
    return manifest, batches


def test_schema_versions_and_round_trips_are_explicit() -> None:
    run = _run()
    policy = run.storage_policy
    estimate = ReconstructionResourceEstimateV1(
        input_event_count=100,
        candidate_event_count=200,
        retained_ensemble_members=2,
        inflight_batches=2,
        peak_events_per_batch=50,
        estimated_memory_bytes=1_000,
        estimated_scratch_bytes=2_000,
        estimated_output_bytes=3_000,
        estimated_batch_count=4,
    )
    window = _window()
    batch = _batch(window, "eurusd", 0)
    carry = _carry(window)
    rejections = _rejections(window)
    manifest, _ = _manifest(window)
    checkpoint = ReconstructionCheckpointV1.planned(window)
    heartbeat = ReconstructionHeartbeatV1(
        run_id=window.run_id,
        window_id=window.window_id,
        synchronization_unit_id=window.synchronization_unit_id,
        phase=ReconstructionCommitPhase.RUNNING,
        sequence=1,
        completed_units=1,
        total_units=3,
    )

    assert ReconstructionStoragePolicyV1.from_json(policy.to_json()) == policy
    assert ReconstructionResourceEstimateV1.from_json(estimate.to_json()) == (
        estimate
    )
    assert ReconstructionRunV1.from_json(run.to_json()) == run
    assert ReconstructionWindowV1.from_json(window.to_json()) == window
    assert EventBatchV1.from_json(batch.to_json()) == batch
    assert CarryStateV1.from_json(carry.to_json()) == carry
    assert RejectionSummaryV1.from_json(rejections.to_json()) == rejections
    assert PartitionManifestV1.from_json(manifest.to_json()) == manifest
    assert ReconstructionCheckpointV1.from_json(checkpoint.to_json()) == (
        checkpoint
    )
    assert ReconstructionHeartbeatV1.from_json(heartbeat.to_json()) == (
        heartbeat
    )
    assert all(
        version.endswith(".v1")
        for version in (
            RECONSTRUCTION_STORAGE_POLICY_SCHEMA_VERSION,
            RECONSTRUCTION_RESOURCE_ESTIMATE_SCHEMA_VERSION,
            RECONSTRUCTION_RUN_SCHEMA_VERSION,
            RECONSTRUCTION_WINDOW_SCHEMA_VERSION,
            EVENT_BATCH_SCHEMA_VERSION,
            CARRY_STATE_SCHEMA_VERSION,
            REJECTION_SUMMARY_SCHEMA_VERSION,
            PARTITION_MANIFEST_SCHEMA_VERSION,
            RECONSTRUCTION_CHECKPOINT_SCHEMA_VERSION,
            RECONSTRUCTION_HEARTBEAT_SCHEMA_VERSION,
        )
    )


def test_execution_policy_does_not_change_semantic_run_or_seed() -> None:
    narrow = _run(policy=ReconstructionStoragePolicyV1(max_inflight_batches=1))
    parallel = _run(
        policy=ReconstructionStoragePolicyV1(max_inflight_batches=32)
    )

    assert narrow.run_id == parallel.run_id
    assert narrow.seed_for("member-000", "anchor-interval-42") == (
        parallel.seed_for("member-000", "anchor-interval-42")
    )
    assert narrow.storage_policy.policy_id != parallel.storage_policy.policy_id

    policy_payload = narrow.storage_policy.to_dict()
    policy_payload["atomic_promotion_required"] = "true"
    policy_payload["policy_id"] = ""
    with pytest.raises(ValueError, match="must be a boolean"):
        ReconstructionStoragePolicyV1.from_dict(policy_payload)


@given(
    first_size=st.integers(min_value=1, max_value=4_000),
    second_size=st.integers(min_value=1, max_value=4_000),
    offset=st.integers(min_value=0, max_value=11_999),
)
@settings(max_examples=60, deadline=None)
def test_legal_window_partitioning_has_single_ownership_and_same_seed(
    first_size: int,
    second_size: int,
    offset: int,
) -> None:
    run = _run()
    first = plan_reconstruction_windows(
        run,
        ensemble_member_id="member-000",
        start_ns=START_NS,
        end_ns=END_NS,
        window_size_ns=first_size,
        left_halo_ns=500,
        right_lookahead_ns=750,
    )
    second = plan_reconstruction_windows(
        run,
        ensemble_member_id="member-000",
        start_ns=START_NS,
        end_ns=END_NS,
        window_size_ns=second_size,
        left_halo_ns=500,
        right_lookahead_ns=750,
    )
    event_time = START_NS + offset

    assert sum(window.owns_event_time(event_time) for window in first) == 1
    assert sum(window.owns_event_time(event_time) for window in second) == 1
    assert run.seed_for("member-000", f"anchor:{event_time}") == (
        run.seed_for("member-000", f"anchor:{event_time}")
    )


def test_window_halo_is_readable_but_never_generation_owned() -> None:
    window = _window()

    assert window.reads_event_time(window.input_start_ns)
    assert not window.owns_event_time(window.input_start_ns)
    assert window.owns_event_time(window.core_start_ns)
    assert not window.owns_event_time(window.core_end_ns)
    assert window.reads_event_time(window.core_end_ns)
    assert not window.reads_event_time(window.input_end_ns)


def test_window_plan_rejects_gaps_overlaps_and_scope_drift() -> None:
    first = _window()
    gap = ReconstructionWindowV1(
        run_id=first.run_id,
        ensemble_member_id=first.ensemble_member_id,
        symbols=first.symbols,
        core_start_ns=first.core_end_ns + 1,
        core_end_ns=first.core_end_ns + 100,
    )
    overlap = replace(
        gap,
        core_start_ns=first.core_end_ns - 1,
        window_id="",
        synchronization_unit_id="",
    )
    drift = ReconstructionWindowV1(
        run_id=first.run_id,
        ensemble_member_id="member-001",
        symbols=first.symbols,
        core_start_ns=first.core_end_ns,
        core_end_ns=first.core_end_ns + 100,
    )

    with pytest.raises(ValueError, match="contiguous"):
        validate_reconstruction_window_plan((first, gap))
    with pytest.raises(ValueError, match="contiguous"):
        validate_reconstruction_window_plan((first, overlap))
    with pytest.raises(ValueError, match="scope drifted"):
        validate_reconstruction_window_plan((first, drift))


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("candidate_event_count", 251, "amplification"),
        ("peak_events_per_batch", 100_001, "peak_events_per_batch"),
        ("retained_ensemble_members", 5, "retained_ensemble_members"),
        ("inflight_batches", 9, "inflight_batches"),
        ("estimated_memory_bytes", 2 * 1024**3 + 1, "memory"),
        ("estimated_scratch_bytes", 100 * 1024**3 + 1, "scratch"),
        ("estimated_output_bytes", 100 * 1024**3 + 1, "output"),
    ),
)
def test_resource_preflight_fails_early_with_full_estimate(
    field: str,
    value: int,
    message: str,
) -> None:
    policy = ReconstructionStoragePolicyV1()
    estimate = ReconstructionResourceEstimateV1(
        input_event_count=10,
        candidate_event_count=100,
        retained_ensemble_members=2,
        inflight_batches=2,
        peak_events_per_batch=100,
        estimated_memory_bytes=1_000,
        estimated_scratch_bytes=2_000,
        estimated_output_bytes=3_000,
        estimated_batch_count=4,
    )
    refused = replace(estimate, **{field: value}, estimate_id="")

    with pytest.raises(ReconstructionResourceLimitError, match=message) as err:
        policy.preflight(refused)

    assert err.value.estimate == refused
    assert err.value.violations
    assert policy.preflight(estimate) == estimate


def test_event_batches_are_strong_refs_and_retry_deterministic() -> None:
    window = _window()
    original = _batch(window, "EURUSD", 0)
    retry = _batch(window, "eurusd", 0)
    relocated_retry = replace(
        retry,
        artifact=replace(
            retry.artifact,
            path="scratch/worker-99/retry.parquet",
            metadata={"worker": "99"},
        ),
        batch_id="",
    )
    payload = original.to_dict()

    assert original == retry
    assert original.batch_id == retry.batch_id
    assert relocated_retry != original
    assert relocated_retry.batch_id == original.batch_id
    checkpoint = ReconstructionCheckpointV1.planned(window)
    assert checkpoint.pending_batches((original, relocated_retry)) == (
        original,
    )
    assert "events" not in payload
    assert payload["artifact"]["sha256"]
    assert payload["artifact"]["size_bytes"] > 0
    with pytest.raises(ValueError, match="SHA-256"):
        replace(
            original,
            artifact=replace(original.artifact, sha256="weak"),
            batch_id="",
        )
    with pytest.raises(ValueError, match="inline data"):
        replace(
            original,
            artifact=replace(
                original.artifact,
                metadata={"events": [{"bid": 1.1}]},
            ),
            batch_id="",
        )
    with pytest.raises(ValueError, match="finite numbers"):
        replace(
            original,
            artifact=replace(
                original.artifact,
                metadata={"quality_score": float("nan")},
            ),
            batch_id="",
        )
    with pytest.raises(ValueError, match="outside half-open"):
        replace(
            original,
            last_event_time_ns=original.ownership_end_ns,
            batch_id="",
        )


def test_carry_and_rejection_contracts_are_bounded_aggregates() -> None:
    window = _window()
    carry = _carry(window)
    rejections = _rejections(window)

    assert "events" not in carry.to_dict()
    assert len(carry.to_json().encode("utf-8")) < 262_144
    assert sum(rejections.reason_counts.values()) == rejections.rejected_count
    with pytest.raises(ValueError, match="reconcile"):
        replace(rejections, reason_counts={"one": 1}, summary_id="")
    with pytest.raises(ValueError, match="unknown symbol"):
        replace(
            carry,
            last_event_ids={"usdjpy": "event:sha256:unknown"},
            carry_id="",
        )


def test_partition_manifest_commits_all_symbols_as_one_unit() -> None:
    window = _window()
    manifest, batches = _manifest(window)

    assert manifest.symbols == tuple(sorted(SYMBOLS))
    assert manifest.event_count == 9
    assert {batch.symbol for batch in manifest.event_batches} == set(SYMBOLS)
    assert PartitionManifestV1.from_json(manifest.to_json()) == manifest
    with pytest.raises(ValueError, match="reconcile"):
        replace(
            manifest,
            symbol_event_counts={**manifest.symbol_event_counts, "eurusd": 2},
            manifest_id="",
        )
    foreign = replace(
        batches[0],
        synchronization_unit_id="sync:foreign",
        batch_id="",
    )
    with pytest.raises(ValueError, match="scope"):
        replace(
            manifest,
            event_batches=(foreign, *batches[1:]),
            manifest_id="",
        )


def test_checkpoint_crash_retry_deduplicates_completed_batches() -> None:
    window = _window()
    manifest, batches = _manifest(window)
    planned = ReconstructionCheckpointV1.planned(window)
    running = planned.transition(
        ReconstructionCommitPhase.RUNNING,
        expected_checkpoint_id=planned.checkpoint_id,
    )
    after_first = running.transition(
        ReconstructionCommitPhase.RUNNING,
        expected_checkpoint_id=running.checkpoint_id,
        completed_batches=(batches[0],),
        input_watermark_ns=window.core_end_ns,
    )
    recovered = ReconstructionCheckpointV1.from_json(after_first.to_json())

    assert recovered.pending_batches((batches[0], *batches)) == batches[1:]
    finished = recovered.transition(
        ReconstructionCommitPhase.RUNNING,
        expected_checkpoint_id=recovered.checkpoint_id,
        completed_batches=(batches[0], *batches[1:]),
        output_watermark_ns=window.core_end_ns,
    )
    assert set(finished.completed_batch_ids) == {
        batch.batch_id for batch in batches
    }
    assert not finished.pending_batches(batches)
    assert manifest.event_count == sum(batch.event_count for batch in batches)


def test_two_phase_commit_never_advertises_partial_output() -> None:
    window = _window()
    manifest, batches = _manifest(window)
    carry = _carry(window)
    rejections = _rejections(window)
    carry_ref = artifact_ref_for_json_contract(
        carry,
        kind="carry-state",
        path=f"scratch/{window.window_id}/carry.json",
    )
    rejection_ref = artifact_ref_for_json_contract(
        rejections,
        kind="rejection-summary",
        path=f"scratch/{window.window_id}/rejections.json",
    )
    staged_ref = artifact_ref_for_json_contract(
        manifest,
        kind="partition-manifest-temp",
        path=f"scratch/{window.window_id}/manifest.partial.json",
    )
    committed_ref = artifact_ref_for_json_contract(
        manifest,
        kind="partition-manifest",
        path=f"products/{window.window_id}/manifest.json",
    )
    checkpoint = ReconstructionCheckpointV1.planned(window)
    checkpoint = checkpoint.transition(
        ReconstructionCommitPhase.RUNNING,
        expected_checkpoint_id=checkpoint.checkpoint_id,
        completed_batches=batches,
        carry_state_ref=carry_ref,
        rejection_summary_ref=rejection_ref,
    )
    staged = checkpoint.transition(
        ReconstructionCommitPhase.STAGED,
        expected_checkpoint_id=checkpoint.checkpoint_id,
        staged_manifest_ref=staged_ref,
    )
    validated = staged.transition(
        ReconstructionCommitPhase.VALIDATED,
        expected_checkpoint_id=staged.checkpoint_id,
    )

    assert staged.advertised_manifest_ref is None
    assert validated.advertised_manifest_ref is None
    committed = validated.transition(
        ReconstructionCommitPhase.COMMITTED,
        expected_checkpoint_id=validated.checkpoint_id,
        committed_manifest_ref=committed_ref,
    )
    assert committed.advertised_manifest_ref == committed_ref
    assert committed.staged_manifest_ref is None
    assert (
        committed.transition(
            ReconstructionCommitPhase.COMMITTED,
            expected_checkpoint_id=committed.checkpoint_id,
            committed_manifest_ref=committed_ref,
        )
        is committed
    )
    with pytest.raises(ValueError, match="cannot change manifest"):
        committed.transition(
            ReconstructionCommitPhase.COMMITTED,
            expected_checkpoint_id=committed.checkpoint_id,
            committed_manifest_ref=replace(
                committed_ref,
                path="products/conflicting/manifest.json",
            ),
        )


def test_commit_rejects_promoted_bytes_that_differ_from_validated_stage() -> (
    None
):
    window = _window()
    manifest, _ = _manifest(window)
    staged_ref = artifact_ref_for_json_contract(
        manifest,
        kind="partition-manifest-temp",
        path=f"scratch/{window.window_id}/manifest.partial.json",
    )
    checkpoint = ReconstructionCheckpointV1.planned(window)
    checkpoint = checkpoint.transition(
        ReconstructionCommitPhase.RUNNING,
        expected_checkpoint_id=checkpoint.checkpoint_id,
    )
    checkpoint = checkpoint.transition(
        ReconstructionCommitPhase.STAGED,
        expected_checkpoint_id=checkpoint.checkpoint_id,
        staged_manifest_ref=staged_ref,
    )
    checkpoint = checkpoint.transition(
        ReconstructionCommitPhase.VALIDATED,
        expected_checkpoint_id=checkpoint.checkpoint_id,
    )
    different = _ref(
        "partition-manifest",
        f"products/{window.window_id}/manifest.json",
        "different bytes",
    )

    with pytest.raises(ValueError, match="do not match"):
        checkpoint.transition(
            ReconstructionCommitPhase.COMMITTED,
            expected_checkpoint_id=checkpoint.checkpoint_id,
            committed_manifest_ref=different,
        )


def test_checkpoint_rejects_stale_transition_and_watermark_regression() -> None:
    window = _window()
    checkpoint = ReconstructionCheckpointV1.planned(window)
    running = checkpoint.transition(
        ReconstructionCommitPhase.RUNNING,
        expected_checkpoint_id=checkpoint.checkpoint_id,
        input_watermark_ns=window.core_end_ns,
    )

    with pytest.raises(ValueError, match="stale checkpoint"):
        running.transition(
            ReconstructionCommitPhase.RUNNING,
            expected_checkpoint_id=checkpoint.checkpoint_id,
        )
    with pytest.raises(ValueError, match="cannot move backwards"):
        running.transition(
            ReconstructionCommitPhase.RUNNING,
            expected_checkpoint_id=running.checkpoint_id,
            input_watermark_ns=window.core_start_ns,
        )


def test_cancellation_stops_advertising_and_resume_clears_partial_ref() -> None:
    window = _window()
    manifest, _ = _manifest(window)
    staged_ref = artifact_ref_for_json_contract(
        manifest,
        kind="partition-manifest-temp",
        path=f"scratch/{window.window_id}/manifest.partial.json",
    )
    checkpoint = ReconstructionCheckpointV1.planned(window)
    checkpoint = checkpoint.transition(
        ReconstructionCommitPhase.RUNNING,
        expected_checkpoint_id=checkpoint.checkpoint_id,
    )
    staged = checkpoint.transition(
        ReconstructionCommitPhase.STAGED,
        expected_checkpoint_id=checkpoint.checkpoint_id,
        staged_manifest_ref=staged_ref,
    )
    cancelled = staged.transition(
        ReconstructionCommitPhase.CANCELLED,
        expected_checkpoint_id=staged.checkpoint_id,
        interruption_reason="operator requested cancellation",
    )

    assert cancelled.advertised_manifest_ref is None
    assert cancelled.staged_manifest_ref == staged_ref
    resumed = cancelled.transition(
        ReconstructionCommitPhase.RUNNING,
        expected_checkpoint_id=cancelled.checkpoint_id,
    )
    assert resumed.staged_manifest_ref is None
    assert resumed.interruption_reason == ""


def test_checkpoint_policy_enforces_bounded_workflow_payload() -> None:
    window = _window()
    checkpoint = ReconstructionCheckpointV1.planned(window)
    tiny_policy = ReconstructionStoragePolicyV1(max_checkpoint_bytes=100)

    with pytest.raises(ValueError, match="checkpoint payload"):
        checkpoint.assert_within(tiny_policy)
    assert checkpoint.assert_within(_run().storage_policy) == checkpoint


def test_heartbeat_is_bounded_and_explicit_about_cancel_resume() -> None:
    window = _window()
    checkpoint = ReconstructionCheckpointV1.planned(window)
    heartbeat = ReconstructionHeartbeatV1(
        run_id=window.run_id,
        window_id=window.window_id,
        synchronization_unit_id=window.synchronization_unit_id,
        phase=ReconstructionCommitPhase.RUNNING,
        sequence=3,
        completed_units=2,
        total_units=4,
        observed_event_count=100,
        candidate_event_count=200,
        accepted_event_count=150,
        scratch_bytes=10_000,
        output_bytes=5_000,
        checkpoint_id=checkpoint.checkpoint_id,
        cancellation_requested=True,
        message="draining current bounded batch",
    )
    payload = heartbeat.to_dict()

    assert payload["percent_complete"] == 50.0
    assert payload["stops_future_work_on_cancel"] is True
    assert payload["resume_mode"] == "last_valid_checkpoint"
    assert "events" not in json.dumps(payload)
    assert len(heartbeat.to_json().encode("utf-8")) < 65_536


def test_checkpoint_and_heartbeat_survive_temporal_data_conversion() -> None:
    window = _window()
    checkpoint = ReconstructionCheckpointV1.planned(window)
    heartbeat = ReconstructionHeartbeatV1(
        run_id=window.run_id,
        window_id=window.window_id,
        synchronization_unit_id=window.synchronization_unit_id,
        phase=ReconstructionCommitPhase.PLANNED,
        sequence=0,
        completed_units=0,
        total_units=3,
        checkpoint_id=checkpoint.checkpoint_id,
    )
    payload = {
        "checkpoint": checkpoint.to_dict(),
        "heartbeat": heartbeat.to_dict(),
    }

    async def round_trip() -> dict:
        encoded = await DataConverter.default.encode([payload])
        [decoded] = await DataConverter.default.decode(
            encoded,
            type_hints=[dict],
        )
        return decoded

    assert asyncio.run(round_trip()) == payload


def test_derived_fields_and_ids_fail_closed() -> None:
    window = _window()
    manifest, _ = _manifest(window)
    checkpoint = ReconstructionCheckpointV1.planned(window)

    window_payload = window.to_dict()
    window_payload["window_id"] = "reconstruction-window:sha256:" + "0" * 64
    with pytest.raises(ValueError, match="window_id"):
        ReconstructionWindowV1.from_dict(window_payload)

    manifest_payload = manifest.to_dict()
    manifest_payload["event_count"] = 999
    with pytest.raises(ValueError, match="event_count"):
        PartitionManifestV1.from_dict(manifest_payload)

    checkpoint_payload = checkpoint.to_dict()
    checkpoint_payload["advertised_manifest_ref"] = _ref(
        "partition-manifest",
        "products/uncommitted.json",
        "not committed",
    ).to_dict()
    with pytest.raises(ValueError, match="advertised manifest"):
        ReconstructionCheckpointV1.from_dict(checkpoint_payload)

    duplicate_payload = checkpoint.to_dict()
    duplicate_payload["phase"] = "running"
    duplicate_payload["revision"] = 1
    duplicate_payload["completed_batch_ids"] = ["batch:one", "batch:one"]
    duplicate_payload["checkpoint_id"] = ""
    with pytest.raises(ValueError, match="must be unique"):
        ReconstructionCheckpointV1.from_dict(duplicate_payload)


def test_checkpoint_fixture_is_stable_bounded_and_reconstructable() -> None:
    fixture = (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "reconstruction_checkpoint_v1.json"
    )
    text = fixture.read_text(encoding="utf-8")
    checkpoint = ReconstructionCheckpointV1.from_dict(json.loads(text))

    assert checkpoint == ReconstructionCheckpointV1.planned(_window())
    assert json.dumps(
        checkpoint.to_dict(), indent=2, sort_keys=True
    ) + "\n" == (text)
    assert len(text.encode("utf-8")) < 2_048
    assert not {"events", "rows", "records"}.intersection(checkpoint.to_dict())
