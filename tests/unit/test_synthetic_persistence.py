"""Atomic final-product persistence for reconstructed event streams."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import duckdb
import polars as pl
import pyarrow.parquet as pq
import pytest

from histdatacom.broker_capture import fit_broker_delivery_fingerprint
from histdatacom.synthetic import (
    RECONSTRUCTION_PRODUCT_DIRECTORY,
    BrokerTransferConfigV1,
    ReconstructionCheckpointV1,
    ReconstructionCommitPhase,
    ReconstructionPersistenceError,
    ReconstructionProductManifestV1,
    ReconstructionProductManifestV2,
    ReconstructionStoragePolicyV1,
    ReconstructionStoragePreflightError,
    SyntheticEventOrigin,
    cleanup_reconstruction_scratch,
    commit_delivery_reconstruction_publication,
    commit_reconstruction_publication,
    discover_reconstruction_manifests,
    estimate_reconstruction_retention,
    iter_reconstruction_event_batches,
    project_modern_reference_delivery,
    publish_reconstruction_group,
    read_reconstruction_streams,
    reconstruction_parquet_paths,
    render_broker_delivery,
    scan_reconstruction_events_polars,
    stage_delivery_reconstruction_publication,
    stage_reconstruction_publication,
    validate_cross_currency_output,
    verify_reconstruction_publication,
)
from histdatacom.synthetic.contracts import SYNTHETIC_EVENT_ARROW_COLUMNS
from histdatacom.synthetic.cross_currency import CrossCurrencyValidationStage
from tests.unit.test_broker_delivery_fingerprints import (
    BASE_WALL_NS,
    _capture,
)
from tests.unit.test_synthetic_broker_transfer import _group_with_constraints


def test_publish_exact_narrow_schema_reconciles_anchors_and_replays(
    tmp_path: Path,
) -> None:
    """The committed product contains exact #431 rows and compact evidence."""
    rendered, anchors, policy, retention = _publication_inputs(tmp_path)

    published = publish_reconstruction_group(
        tmp_path / "archive",
        rendered,
        immutable_source_anchors=anchors,
        symbol_group_id="eurusd-triangle",
        retention_plan=retention,
        storage_policy=policy,
        row_group_size=2,
    )

    assert not published.idempotent_retry
    manifest = verify_reconstruction_publication(published.manifest_path)
    assert manifest == published.manifest
    assert ReconstructionProductManifestV1.from_json(manifest.to_json()) == (
        manifest
    )
    assert manifest.event_count == sum(
        len(stream.events) for stream in rendered.streams
    )
    assert manifest.observed_event_count == len(anchors)
    assert manifest.synthetic_event_count == (
        manifest.event_count - len(anchors)
    )
    assert manifest.quality.broker_transfer_manifest_id == (
        rendered.manifest.manifest_id
    )
    assert manifest.source.observed_content_sha256
    assert manifest.constraints.constraint_set_ids
    assert manifest.replay.logical_content_sha256
    assert manifest.ensemble.materialized_member_id == (
        rendered.manifest.ensemble_member_id
    )
    assert manifest.ensemble.retention_plan_id == retention.plan_id
    assert manifest.ensemble.to_dict()["automatic_winner"] is False
    assert manifest.retention.plan_id == retention.plan_id
    payload = manifest.to_dict()
    assert payload["event_rows_inline"] is False
    assert payload["analytical_frame_columns_inline"] is False
    assert "events" not in json.dumps(payload)

    for partition in manifest.partitions:
        table = pq.ParquetFile(
            published.manifest_path.parent / partition.relative_path
        ).read()
        assert table.schema.names == list(SYNTHETIC_EVENT_ARROW_COLUMNS)
        assert table.num_columns == 26
        assert table.num_columns < 521
        assert table.num_rows == partition.row_count
        assert partition.row_group_count >= 1
    assert any(item.row_group_count > 1 for item in manifest.partitions)
    assert read_reconstruction_streams(published.manifest_path) == (
        rendered.streams
    )


def test_staging_is_invisible_and_commit_is_atomic_and_idempotent(
    tmp_path: Path,
) -> None:
    """Only directory promotion makes a synchronized unit discoverable."""
    rendered, anchors, policy, retention = _publication_inputs(tmp_path)
    root = tmp_path / "archive"

    staged = stage_reconstruction_publication(
        root,
        rendered,
        immutable_source_anchors=anchors,
        symbol_group_id="eurusd-triangle",
        retention_plan=retention,
        storage_policy=policy,
    )

    assert staged.staging_directory.exists()
    assert staged.manifest_ref.path == str(staged.manifest_path)
    assert discover_reconstruction_manifests(root) == ()

    committed = commit_reconstruction_publication(staged)
    assert not committed.idempotent_retry
    assert not staged.staging_directory.exists()
    assert discover_reconstruction_manifests(root) == (committed.manifest_path,)

    retry = commit_reconstruction_publication(staged)
    assert retry.idempotent_retry
    assert retry.manifest == committed.manifest
    assert retry.manifest_ref.sha256 == committed.manifest_ref.sha256

    publish_retry = publish_reconstruction_group(
        root,
        rendered,
        immutable_source_anchors=anchors,
        symbol_group_id="eurusd-triangle",
        retention_plan=retention,
        storage_policy=policy,
    )
    assert publish_retry.idempotent_retry
    assert publish_retry.manifest == committed.manifest


def test_generic_delivery_commit_recovers_after_atomic_rename(
    tmp_path: Path,
) -> None:
    """A retry after rename does not depend on the vanished staging path."""
    run, window, group, _constraints = _group_with_constraints()
    anchors = tuple(
        event
        for stream in group.streams
        for event in stream.events
        if event.origin is SyntheticEventOrigin.OBSERVED
    )
    validation = validate_cross_currency_output(
        run=run,
        window=window,
        streams={stream.symbol: stream for stream in group.streams},
        config=group.config,
        stage=CrossCurrencyValidationStage.POST_BROKER,
        observed_anchors=anchors,
    )
    delivered = project_modern_reference_delivery(
        group, delivery_profile_id="modern-reference:fixture"
    )
    retention = estimate_reconstruction_retention(
        run_id=run.run_id,
        primary_member_id=window.ensemble_member_id,
        retained_member_event_counts={
            window.ensemble_member_id: sum(
                len(stream.events) for stream in delivered.streams
            )
        },
        estimated_partition_count=len(delivered.streams),
        storage_policy=run.storage_policy,
    )
    root = tmp_path / "archive"
    staged = stage_delivery_reconstruction_publication(
        root,
        delivered,
        final_validation=validation,
        benchmark_artifact_ids=("benchmark:fixture",),
        benchmark_evidence={"gate": "passed"},
        point_in_time_evidence_projection_ids=("projection:fixture",),
        point_in_time_evidence_decision_ids=("decision:fixture",),
        cross_series_constraint_bundle_ids=("bundle:fixture",),
        cross_series_constraint_window_ids=("window:fixture",),
        cross_series_constraint_decision_ids=("cross-decision:fixture",),
        immutable_source_anchors=anchors,
        symbol_group_id=window.synchronization_unit_id,
        retention_plan=retention,
        storage_policy=run.storage_policy,
        staging_root=tmp_path / "window-scratch" / "publication",
        row_group_size=2,
    )

    assert discover_reconstruction_manifests(root) == ()
    committed = commit_delivery_reconstruction_publication(staged)
    assert not committed.idempotent_retry
    assert not staged.staging_directory.exists()
    assert isinstance(committed.manifest, ReconstructionProductManifestV2)
    assert committed.manifest.quality.point_in_time_evidence_projection_ids == (
        "projection:fixture",
    )
    assert committed.manifest.quality.point_in_time_evidence_decision_ids == (
        "decision:fixture",
    )
    assert committed.manifest.quality.cross_series_constraint_bundle_ids == (
        "bundle:fixture",
    )
    assert committed.manifest.quality.cross_series_constraint_window_ids == (
        "window:fixture",
    )
    assert committed.manifest.quality.cross_series_constraint_decision_ids == (
        "cross-decision:fixture",
    )
    legacy_quality = replace(
        committed.manifest.quality,
        point_in_time_evidence_projection_ids=(),
        point_in_time_evidence_decision_ids=(),
        cross_series_constraint_bundle_ids=(),
        cross_series_constraint_window_ids=(),
        cross_series_constraint_decision_ids=(),
        quality_manifest_id="",
    )
    legacy_payload = legacy_quality.to_dict()
    assert "point_in_time_evidence_projection_ids" not in legacy_payload
    assert "point_in_time_evidence_decision_ids" not in legacy_payload
    assert "cross_series_constraint_bundle_ids" not in legacy_payload
    assert "cross_series_constraint_window_ids" not in legacy_payload
    assert "cross_series_constraint_decision_ids" not in legacy_payload
    assert type(legacy_quality).from_dict(legacy_payload) == legacy_quality
    assert discover_reconstruction_manifests(
        root,
        delivery_profile_id="modern-reference:fixture",
    ) == (committed.manifest_path,)

    recovered = commit_delivery_reconstruction_publication(staged)
    assert recovered.idempotent_retry
    assert recovered.manifest == committed.manifest
    assert read_reconstruction_streams(committed.manifest_path) == (
        delivered.streams
    )


def test_idempotent_publish_retry_revalidates_committed_partitions(
    tmp_path: Path,
) -> None:
    """A matching manifest cannot hide corrupt committed Parquet on retry."""
    rendered, anchors, policy, retention = _publication_inputs(tmp_path)
    root = tmp_path / "archive"
    published = publish_reconstruction_group(
        root,
        rendered,
        immutable_source_anchors=anchors,
        symbol_group_id="eurusd-triangle",
        retention_plan=retention,
        storage_policy=policy,
    )
    partition = published.manifest.partitions[0]
    partition_path = published.manifest_path.parent / partition.relative_path
    partition_path.write_bytes(partition_path.read_bytes()[:32])

    with pytest.raises(
        ReconstructionPersistenceError,
        match="byte size differs",
    ):
        publish_reconstruction_group(
            root,
            rendered,
            immutable_source_anchors=anchors,
            symbol_group_id="eurusd-triangle",
            retention_plan=retention,
            storage_policy=policy,
        )


def test_manifest_refs_complete_the_checkpoint_two_phase_commit(
    tmp_path: Path,
) -> None:
    """Staged and promoted byte identities plug into the #432 state machine."""
    rendered, anchors, policy, retention = _publication_inputs(tmp_path)
    staged = stage_reconstruction_publication(
        tmp_path / "archive",
        rendered,
        immutable_source_anchors=anchors,
        symbol_group_id="eurusd-triangle",
        retention_plan=retention,
        storage_policy=policy,
    )
    checkpoint = ReconstructionCheckpointV1(
        run_id=rendered.manifest.run_id,
        window_id=rendered.manifest.window_id,
        synchronization_unit_id=rendered.manifest.synchronization_unit_id,
        revision=1,
        phase=ReconstructionCommitPhase.RUNNING,
    )
    staged_checkpoint = checkpoint.transition(
        ReconstructionCommitPhase.STAGED,
        expected_checkpoint_id=checkpoint.checkpoint_id,
        staged_manifest_ref=staged.manifest_ref,
    )
    validated_checkpoint = staged_checkpoint.transition(
        ReconstructionCommitPhase.VALIDATED,
        expected_checkpoint_id=staged_checkpoint.checkpoint_id,
    )

    published = commit_reconstruction_publication(staged)
    committed_checkpoint = validated_checkpoint.transition(
        ReconstructionCommitPhase.COMMITTED,
        expected_checkpoint_id=validated_checkpoint.checkpoint_id,
        committed_manifest_ref=published.manifest_ref,
    )

    assert committed_checkpoint.advertised_manifest_ref == (
        published.manifest_ref
    )
    assert committed_checkpoint.staged_manifest_ref is None
    assert staged_checkpoint.advertised_manifest_ref is None


def test_truncated_staging_fails_closed_before_publication(
    tmp_path: Path,
) -> None:
    """A damaged Parquet footer can never cross the commit boundary."""
    rendered, anchors, policy, retention = _publication_inputs(tmp_path)
    root = tmp_path / "archive"
    staged = stage_reconstruction_publication(
        root,
        rendered,
        immutable_source_anchors=anchors,
        symbol_group_id="eurusd-triangle",
        retention_plan=retention,
        storage_policy=policy,
    )
    partition = staged.manifest.partitions[0]
    partition_path = staged.staging_directory / partition.relative_path
    partition_path.write_bytes(partition_path.read_bytes()[:32])

    with pytest.raises(
        ReconstructionPersistenceError,
        match="byte size differs",
    ):
        commit_reconstruction_publication(staged)

    assert discover_reconstruction_manifests(root) == ()
    assert cleanup_reconstruction_scratch(root) == (staged.staging_directory,)
    assert not staged.staging_directory.exists()


def test_anchor_value_drift_refuses_even_when_observed_id_is_unchanged(
    tmp_path: Path,
) -> None:
    """Publication compares immutable values as well as stable source IDs."""
    rendered, anchors, policy, retention = _publication_inputs(tmp_path)
    first = anchors[0]
    changed = replace(first, bid=first.bid + 0.0001)

    with pytest.raises(
        ReconstructionPersistenceError,
        match="values or IDs differ",
    ):
        publish_reconstruction_group(
            tmp_path / "archive",
            rendered,
            immutable_source_anchors=(changed, *anchors[1:]),
            symbol_group_id="eurusd-triangle",
            retention_plan=retention,
            storage_policy=policy,
        )

    assert discover_reconstruction_manifests(tmp_path / "archive") == ()


def test_retention_preflight_estimates_primary_and_all_retained_members() -> (
    None
):
    """Primary, retained-member, manifest, and policy bytes reconcile early."""
    policy = ReconstructionStoragePolicyV1(
        max_output_bytes=10_000_000,
        max_retained_ensemble_members=3,
    )
    plan = estimate_reconstruction_retention(
        run_id="run:fixture",
        primary_member_id="member-000",
        retained_member_event_counts={
            "member-000": 1_000,
            "member-001": 1_500,
            "member-002": 2_000,
        },
        estimated_partition_count=9,
        storage_policy=policy,
        estimated_bytes_per_event=200,
        estimated_compression_ratio=0.5,
    )

    assert plan.estimated_primary_bytes == 100_000
    assert plan.estimated_retained_bytes == 450_000
    assert plan.estimated_manifest_bytes == 9 * 4_096
    assert plan.estimated_total_output_bytes == (
        plan.estimated_retained_bytes + plan.estimated_manifest_bytes
    )
    assert plan.storage_policy_id == policy.policy_id

    limited = ReconstructionStoragePolicyV1(
        max_output_bytes=100,
        max_retained_ensemble_members=1,
    )
    with pytest.raises(ReconstructionStoragePreflightError) as raised:
        estimate_reconstruction_retention(
            run_id="run:fixture",
            primary_member_id="member-000",
            retained_member_event_counts={
                "member-000": 1_000,
                "member-001": 1_500,
            },
            estimated_partition_count=2,
            storage_policy=limited,
        )
    assert len(raised.value.violations) == 2


def test_cleanup_removes_only_transaction_scratch(
    tmp_path: Path,
) -> None:
    """Cleanup cannot touch committed products or immutable source evidence."""
    rendered, anchors, policy, retention = _publication_inputs(tmp_path)
    root = tmp_path / "archive"
    committed = publish_reconstruction_group(
        root,
        rendered,
        immutable_source_anchors=anchors,
        symbol_group_id="eurusd-triangle",
        retention_plan=retention,
        storage_policy=policy,
    )
    staged = stage_reconstruction_publication(
        root,
        rendered,
        immutable_source_anchors=anchors,
        symbol_group_id="eurusd-triangle",
        retention_plan=retention,
        storage_policy=policy,
    )
    source = root / "immutable-source.data"
    source.write_text("evidence", encoding="utf-8")
    unrelated = staged.staging_directory.parent / "operator-note.txt"
    unrelated.write_text("keep", encoding="utf-8")

    removed = cleanup_reconstruction_scratch(root)

    assert removed == (staged.staging_directory,)
    assert committed.manifest_path.exists()
    assert source.read_text(encoding="utf-8") == "evidence"
    assert unrelated.read_text(encoding="utf-8") == "keep"
    assert discover_reconstruction_manifests(root) == (committed.manifest_path,)


def test_arrow_and_polars_scans_prune_files_columns_and_rows(
    tmp_path: Path,
) -> None:
    """Manifest pruning and lazy readers avoid unrelated product columns."""
    rendered, anchors, policy, retention = _publication_inputs(tmp_path)
    published = publish_reconstruction_group(
        tmp_path / "archive",
        rendered,
        immutable_source_anchors=anchors,
        symbol_group_id="eurusd-triangle",
        retention_plan=retention,
        storage_policy=policy,
    )
    eurusd = next(
        stream for stream in rendered.streams if stream.symbol == "eurusd"
    )
    start = eurusd.events[0].event_time_ns
    end = eurusd.events[-1].event_time_ns + 1
    paths = reconstruction_parquet_paths(
        published.manifest_path,
        symbols=("eurusd",),
        start_ns=start,
        end_ns=end,
    )

    assert paths
    assert all("symbol=eurusd" in str(path) for path in paths)
    assert all("symbol=gbpusd" not in str(path) for path in paths)
    batches = tuple(
        iter_reconstruction_event_batches(
            published.manifest_path,
            columns=("symbol", "event_time_ns", "bid"),
            symbols=("eurusd",),
            start_ns=start,
            end_ns=end,
            batch_size=1,
        )
    )
    assert batches
    assert all(
        batch.schema.names == ["symbol", "event_time_ns", "bid"]
        for batch in batches
    )
    lazy = scan_reconstruction_events_polars(
        published.manifest_path,
        columns=("symbol", "event_time_ns", "bid"),
        symbols=("eurusd",),
        start_ns=start,
        end_ns=end,
    )
    plan = lazy.explain(optimized=True)
    frame = lazy.collect()
    assert frame.columns == ["symbol", "event_time_ns", "bid"]
    assert frame["symbol"].unique().to_list() == ["eurusd"]
    assert frame.height == len(eurusd.events)
    assert "PROJECT 3/26 COLUMNS" in plan
    assert "SELECTION" in plan


def test_duckdb_smoke_proves_parquet_projection_and_filter_pushdown(
    tmp_path: Path,
) -> None:
    """DuckDB reads the committed paths with projected columns and filters."""
    rendered, anchors, policy, retention = _publication_inputs(tmp_path)
    published = publish_reconstruction_group(
        tmp_path / "archive",
        rendered,
        immutable_source_anchors=anchors,
        symbol_group_id="eurusd-triangle",
        retention_plan=retention,
        storage_policy=policy,
    )
    eurusd = next(
        stream for stream in rendered.streams if stream.symbol == "eurusd"
    )
    start = eurusd.events[1].event_time_ns
    end = eurusd.events[-1].event_time_ns + 1
    paths = reconstruction_parquet_paths(
        published.manifest_path,
        symbols=("eurusd",),
        start_ns=start,
        end_ns=end,
    )
    literals = ",".join(
        "'" + str(path).replace("'", "''") + "'" for path in paths
    )
    query = (
        "SELECT symbol, event_time_ns, bid "
        f"FROM read_parquet([{literals}]) "
        f"WHERE event_time_ns >= {start} AND event_time_ns < {end}"
    )

    connection = duckdb.connect()
    try:
        plan_rows = connection.execute("EXPLAIN " + query).fetchall()
        rows = connection.execute(query).fetchall()
    finally:
        connection.close()
    plan = "\n".join(str(value) for row in plan_rows for value in row)

    expected_rows = sum(event.event_time_ns >= start for event in eurusd.events)
    assert len(rows) == expected_rows
    assert {row[0] for row in rows} == {"eurusd"}
    assert "Projections:" in plan
    assert "symbol" in plan and "event_time_ns" in plan and "bid" in plan
    assert "Filters:" in plan


def test_same_writer_runtime_produces_stable_partition_byte_hashes(
    tmp_path: Path,
) -> None:
    """Physical hashes are stable across roots under the pinned writer."""
    rendered, anchors, policy, retention = _publication_inputs(tmp_path)
    first = publish_reconstruction_group(
        tmp_path / "archive-a",
        rendered,
        immutable_source_anchors=anchors,
        symbol_group_id="eurusd-triangle",
        retention_plan=retention,
        storage_policy=policy,
        row_group_size=2,
    )
    second = publish_reconstruction_group(
        tmp_path / "archive-b",
        rendered,
        immutable_source_anchors=anchors,
        symbol_group_id="eurusd-triangle",
        retention_plan=retention,
        storage_policy=policy,
        row_group_size=2,
    )

    assert first.manifest.publication_id == second.manifest.publication_id
    assert first.manifest.manifest_id == second.manifest.manifest_id
    assert first.manifest.replay.partition_byte_sha256 == (
        second.manifest.replay.partition_byte_sha256
    )
    assert [item.byte_sha256 for item in first.manifest.partitions] == [
        item.byte_sha256 for item in second.manifest.partitions
    ]


def test_manifest_tampering_fails_closed(
    tmp_path: Path,
) -> None:
    """Derived counts and deterministic manifest identity reject mutation."""
    rendered, anchors, policy, retention = _publication_inputs(tmp_path)
    published = publish_reconstruction_group(
        tmp_path / "archive",
        rendered,
        immutable_source_anchors=anchors,
        symbol_group_id="eurusd-triangle",
        retention_plan=retention,
        storage_policy=policy,
    )
    payload = json.loads(published.manifest_path.read_text(encoding="utf-8"))
    payload["event_count"] += 1
    published.manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="event_count"):
        verify_reconstruction_publication(published.manifest_path)


def test_discovery_filters_member_run_broker_and_group_axes(
    tmp_path: Path,
) -> None:
    """Committed lookup uses manifest axes without scanning unrelated rows."""
    rendered, anchors, policy, retention = _publication_inputs(tmp_path)
    root = tmp_path / "archive"
    published = publish_reconstruction_group(
        root,
        rendered,
        immutable_source_anchors=anchors,
        symbol_group_id="eurusd-triangle",
        retention_plan=retention,
        storage_policy=policy,
    )

    assert discover_reconstruction_manifests(
        root,
        run_id=rendered.manifest.run_id,
        broker_profile_id=rendered.manifest.fingerprint_id,
        ensemble_member_id=rendered.manifest.ensemble_member_id,
        symbol_group_id="eurusd-triangle",
    ) == (published.manifest_path,)
    assert (
        discover_reconstruction_manifests(
            root, ensemble_member_id="other-member"
        )
        == ()
    )
    assert RECONSTRUCTION_PRODUCT_DIRECTORY in str(published.manifest_path)


def _publication_inputs(tmp_path: Path):
    run, window, group, constraints = _group_with_constraints()
    capture = _capture(
        tmp_path / "broker-capture",
        seed=446,
        wall_start_ns=BASE_WALL_NS,
    )
    fingerprint = fit_broker_delivery_fingerprint(
        tmp_path / "broker-capture", (capture,)
    )
    rendered = render_broker_delivery(
        run=run,
        window=window,
        group=group,
        fingerprint=fingerprint,
        constraints=constraints,
        selected_at_utc_ns=fingerprint.effective_start_utc_ns,
        config=BrokerTransferConfigV1(
            strength=0.25,
            max_events_per_group=100,
        ),
        quality_period="202001",
    )
    anchors = tuple(
        event
        for stream in group.streams
        for event in stream.events
        if event.origin is SyntheticEventOrigin.OBSERVED
    )
    policy = run.storage_policy
    event_count = sum(len(stream.events) for stream in rendered.streams)
    retention = estimate_reconstruction_retention(
        run_id=run.run_id,
        primary_member_id=rendered.manifest.ensemble_member_id,
        retained_member_event_counts={
            rendered.manifest.ensemble_member_id: event_count
        },
        estimated_partition_count=len(rendered.streams),
        storage_policy=policy,
    )
    return rendered, anchors, policy, retention


def test_polars_dependency_is_the_supported_runtime() -> None:
    """Keep the smoke-test import visible to dependency and packaging audits."""
    assert tuple(int(part) for part in pl.__version__.split(".")[:2]) >= (1, 41)
