"""Native generated-stream commitments, not scientific campaign qualification."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace

import pytest

from histdatacom import campaign_verification
from histdatacom.synthetic import persistence
from histdatacom.synthetic.contracts import SyntheticEventOrigin
from histdatacom.synthetic.cross_currency import (
    CrossCurrencyValidationStage,
    validate_cross_currency_output,
)
from histdatacom.synthetic.delivery import project_modern_reference_delivery
from tests.unit.test_synthetic_broker_transfer import _group_with_constraints

COLLECTIONS = (
    "benchmark_artifact_ids",
    "point_in_time_evidence_projection_ids",
    "point_in_time_evidence_decision_ids",
    "cross_series_constraint_bundle_ids",
    "cross_series_constraint_window_ids",
    "cross_series_constraint_decision_ids",
    "projection_burden_report_ids",
    "projection_burden_receipt_ids",
)


def _native_inputs():
    run, window, group, _ = _group_with_constraints()
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
    assert validation.passed
    delivered = project_modern_reference_delivery(
        group, delivery_profile_id="modern-reference:commitment-regression"
    )
    arguments = {name: (f"{name}:a", f"{name}:z") for name in COLLECTIONS}
    arguments.update(
        final_validation=validation,
        benchmark_evidence={"scientific_promotion_evaluated": False},
        projection_burden_status="limited",
    )
    return run, window, delivered, anchors, arguments


@pytest.mark.parametrize("field", COLLECTIONS)
def test_quality_commitment_uses_retained_normalized_collection(field):
    _, _, delivered, _, arguments = _native_inputs()
    canonical = persistence._delivery_quality_manifest(delivered, **arguments)
    first, last = arguments[field]
    arguments[field] = (f" {last} ", first, last, first)
    reordered = persistence._delivery_quality_manifest(delivered, **arguments)
    assert reordered.to_dict() == canonical.to_dict()
    evidence = {name: list(getattr(reordered, name)) for name in COLLECTIONS}
    evidence.update(
        final_validation=arguments["final_validation"].to_dict(),
        benchmark_evidence=reordered.benchmark_evidence,
        projection_burden_status=reordered.projection_burden_status,
    )
    expected = hashlib.sha256(
        json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    assert reordered.cross_instrument_quality_sha256 == expected
    assert type(reordered).from_dict(reordered.to_dict()) == reordered
    campaign_verification._verify_quality_evidence_hash(
        reordered, arguments["final_validation"]
    )


def test_legacy_raw_order_commitment_remains_readable_but_requires_republication():
    _, _, delivered, _, arguments = _native_inputs()
    first, last = arguments["benchmark_artifact_ids"]
    arguments["benchmark_artifact_ids"] = (last, first, last)
    current = persistence._delivery_quality_manifest(delivered, **arguments)
    raw_evidence = {name: list(arguments[name]) for name in COLLECTIONS}
    raw_evidence.update(
        final_validation=arguments["final_validation"].to_dict(),
        benchmark_evidence=arguments["benchmark_evidence"],
        projection_burden_status=arguments["projection_burden_status"],
    )
    legacy_hash = hashlib.sha256(
        json.dumps(raw_evidence, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    assert legacy_hash != current.cross_instrument_quality_sha256
    legacy = replace(
        current,
        cross_instrument_quality_sha256=legacy_hash,
        quality_manifest_id="",
    )
    assert type(legacy).from_dict(legacy.to_dict()) == legacy
    with pytest.raises(ValueError, match="require explicit republication"):
        campaign_verification._verify_quality_evidence_hash(
            legacy, arguments["final_validation"]
        )


def test_public_native_writer_commits_identical_normalized_evidence(tmp_path):
    run, window, delivered, anchors, arguments = _native_inputs()
    retention = persistence.estimate_reconstruction_retention(
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
    publications = []
    for label in ("canonical", "reordered"):
        if label == "reordered":
            for name in COLLECTIONS:
                first, last = arguments[name]
                arguments[name] = (last, first, last)
        staged = persistence.stage_delivery_reconstruction_publication(
            tmp_path / label,
            delivered,
            **arguments,
            immutable_source_anchors=anchors,
            symbol_group_id=window.synchronization_unit_id,
            retention_plan=retention,
            storage_policy=run.storage_policy,
            staging_root=tmp_path / f"{label}-scratch",
            experiment_id="reconstruction-experiment:commitment-regression",
        )
        committed = persistence.commit_delivery_reconstruction_publication(
            staged
        )
        verified = persistence.verify_reconstruction_publication(
            committed.manifest_path
        )
        assert verified == committed.manifest
        assert (
            persistence.read_reconstruction_streams(committed.manifest_path)
            == delivered.streams
        )
        publications.append(verified)
    assert publications[0].to_json() == publications[1].to_json()
