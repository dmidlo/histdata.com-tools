"""Trust, replay, support, and drift tests for broker fingerprints."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path

import pytest

from histdatacom.broker_capture import (
    AppendOnlyBrokerCaptureWriterV1,
    BrokerAdapterMessageV1,
    BrokerCaptureEligibilityStatus,
    BrokerCaptureEventKind,
    BrokerCaptureEventV1,
    BrokerCapturePriceTextSemantics,
    BrokerCaptureSessionManifestV1,
    BrokerCaptureSessionV1,
    BrokerCaptureStoragePolicyV1,
    BrokerCaptureSourceTimestampSemantics,
    BrokerDeliveryDriftConfigV1,
    BrokerDeliveryDriftStatus,
    BrokerDeliveryCellV1,
    BrokerDeliveryFingerprintArtifactError,
    BrokerDeliveryFingerprintComparisonV1,
    BrokerDeliveryFingerprintIdentityError,
    BrokerDeliveryFingerprintV1,
    BrokerDeliveryFitConfigV1,
    BrokerDeliveryIneligibleCaptureError,
    BrokerDeliveryResourceLimitError,
    BrokerDeliverySupportStatus,
    assess_broker_capture_eligibility,
    compare_broker_delivery_fingerprints,
    fit_broker_delivery_fingerprint,
    load_broker_delivery_fingerprint,
    write_broker_delivery_fingerprint,
)
from histdatacom.market_context import (
    MarketContextEventV1,
    MarketContextKind,
    MarketContextPrecision,
    MarketContextSourceV1,
    MarketContextTimelineV1,
)
from histdatacom.synthetic import SyntheticEventStreamV1, SyntheticEventV1

SECOND_NS = 1_000_000_000
BASE_WALL_NS = int(
    datetime(2023, 12, 25, 12, tzinfo=timezone.utc).timestamp() * SECOND_NS
)
BASE_MONOTONIC_NS = 10_000_000_000
CONFIG_SHA256 = hashlib.sha256(b"stable-public-adapter-config").hexdigest()
ACCOUNT_SHA256 = hashlib.sha256(b"stable-account-identity").hexdigest()


def test_fit_is_deterministic_bounded_and_recovers_conditioned_characteristics(
    tmp_path: Path,
) -> None:
    first = _capture(tmp_path, seed=1, wall_start_ns=BASE_WALL_NS)
    second = _capture(
        tmp_path,
        seed=2,
        wall_start_ns=BASE_WALL_NS + 24 * 60 * 60 * SECOND_NS,
    )
    config = BrokerDeliveryFitConfigV1(
        min_cell_support=4,
        max_samples_per_metric=5,
    )

    forward = fit_broker_delivery_fingerprint(
        tmp_path,
        (first, second),
        config=config,
        market_context_timeline=_timeline(),
    )
    reversed_input = fit_broker_delivery_fingerprint(
        tmp_path,
        (second, first),
        config=config,
        market_context_timeline=_timeline(),
    )

    assert forward == reversed_input
    assert BrokerDeliveryFingerprintV1.from_json(forward.to_json()) == forward
    assert len(forward.capture_evidence) == 2
    assert all(item.logical_content_sha256 for item in forward.capture_evidence)
    assert all(
        item.partition_hashes_sha256 for item in forward.capture_evidence
    )
    assert all(
        item.status is BrokerCaptureEligibilityStatus.ELIGIBLE
        for item in forward.eligibility_decisions
    )
    assert any(
        cell.condition.key == "holiday=major_holiday:christmas_day"
        for cell in forward.cells
    )
    assert any(
        cell.condition.key == "event=market_macro_release"
        for cell in forward.cells
    )
    assert any(
        cell.condition.key.startswith("session=") for cell in forward.cells
    )
    global_cell = _cell(forward, "global")
    metrics = {item.name: item for item in global_cell.metrics}
    assert metrics["quote_interarrival_ns"].support_count > 5
    assert metrics["quote_interarrival_ns"].sample_count == 5
    assert metrics["quote_interarrival_ns"].limitations == (
        "deterministic_bottom_hash_sample",
    )
    assert metrics["spread"].estimate == pytest.approx(0.0002)
    assert metrics["source_timestamp_precision_ns"].estimate == 100_000
    assert metrics["price_decimal_places"].estimate == 4
    assert metrics["exact_duplicate_rate"].lower is not None
    assert metrics["exact_duplicate_rate"].upper is not None
    assert metrics["burst_interval_run_length"].support_count > 0
    assert metrics["quiet_interval_run_length"].estimate == 1
    assert metrics["stale_quote_run_length"].estimate == 1
    assert metrics["event_intensity_hz"].support_count == 2
    assert metrics["quote_intensity_hz"].support_count == 2
    assert metrics["event_kind.reconnect_rate"].estimate is not None
    assert metrics["outage_or_gap_duration_ns"].estimate == 2 * SECOND_NS
    assert "global_similarity_score" not in forward.to_dict()


def test_sparse_cells_back_off_and_unqualified_capture_fails_closed(
    tmp_path: Path,
) -> None:
    manifest = _capture(tmp_path, seed=3, wall_start_ns=BASE_WALL_NS)
    config = BrokerDeliveryFitConfigV1(
        min_cell_support=6,
        post_lifecycle_quote_count=2,
    )
    fingerprint = fit_broker_delivery_fingerprint(
        tmp_path, (manifest,), config=config
    )
    lifecycle = next(
        cell
        for cell in fingerprint.cells
        if cell.condition.key == "lifecycle=post_reconnect"
    )

    assert lifecycle.support_count == 2
    assert lifecycle.support_status is BrokerDeliverySupportStatus.BACKED_OFF
    assert (
        lifecycle.effective_condition_id
        == _cell(fingerprint, "global").condition.condition_id
    )
    assert lifecycle.backoff_condition_ids == (
        _cell(fingerprint, "global").condition.condition_id,
    )

    failed_root = tmp_path / "failed"
    failed = _capture(
        failed_root,
        seed=4,
        wall_start_ns=BASE_WALL_NS,
        completed=False,
    )
    eligibility = assess_broker_capture_eligibility(
        failed_root, failed, config=config
    )
    assert eligibility.status is BrokerCaptureEligibilityStatus.INELIGIBLE
    assert "capture_not_completed" in eligibility.reason_codes
    with pytest.raises(BrokerDeliveryIneligibleCaptureError) as error:
        fit_broker_delivery_fingerprint(failed_root, (failed,), config=config)
    assert error.value.eligibility == eligibility


def test_integrity_clock_and_resource_health_gates_are_explicit(
    tmp_path: Path,
) -> None:
    corrupt_root = tmp_path / "corrupt"
    corrupt = _capture(corrupt_root, seed=5, wall_start_ns=BASE_WALL_NS)
    data_path = corrupt_root / corrupt.partitions[0].data_artifact.path
    data_path.write_bytes(data_path.read_bytes() + b"{}\n")
    eligibility = assess_broker_capture_eligibility(corrupt_root, corrupt)
    assert eligibility.status is BrokerCaptureEligibilityStatus.INELIGIBLE
    assert "integrity_verification_failed" in eligibility.reason_codes

    correction_root = tmp_path / "correction"
    correction = _capture(
        correction_root,
        seed=6,
        wall_start_ns=BASE_WALL_NS,
        clock_correction_ns=25_000_000,
    )
    limited = assess_broker_capture_eligibility(correction_root, correction)
    assert limited.status is BrokerCaptureEligibilityStatus.LIMITED
    assert limited.max_abs_clock_correction_ns == 25_000_000
    strict = assess_broker_capture_eligibility(
        correction_root,
        correction,
        config=BrokerDeliveryFitConfigV1(max_abs_clock_correction_ns=1),
    )
    assert strict.status is BrokerCaptureEligibilityStatus.INELIGIBLE
    assert "excessive_clock_correction_magnitude" in strict.reason_codes

    with pytest.raises(
        BrokerDeliveryResourceLimitError, match="max_input_events"
    ):
        fit_broker_delivery_fingerprint(
            correction_root,
            (correction,),
            config=BrokerDeliveryFitConfigV1(max_input_events=8),
        )
    with pytest.raises(BrokerDeliveryResourceLimitError, match="max_cells"):
        fit_broker_delivery_fingerprint(
            correction_root,
            (correction,),
            config=BrokerDeliveryFitConfigV1(max_cells=1),
        )
    context_root = tmp_path / "context-bound"
    context_manifest = _capture(
        context_root, seed=16, wall_start_ns=BASE_WALL_NS
    )
    with pytest.raises(
        BrokerDeliveryResourceLimitError,
        match="max_market_matches_per_quote",
    ):
        fit_broker_delivery_fingerprint(
            context_root,
            (context_manifest,),
            config=BrokerDeliveryFitConfigV1(max_market_matches_per_quote=1),
            market_context_timeline=_timeline(),
        )


def test_drift_is_stratified_support_aware_and_has_no_winner_score(
    tmp_path: Path,
) -> None:
    reference_manifest = _capture(
        tmp_path / "reference", seed=7, wall_start_ns=BASE_WALL_NS
    )
    stable_manifest = _capture(
        tmp_path / "stable",
        seed=8,
        wall_start_ns=BASE_WALL_NS + 24 * 60 * 60 * SECOND_NS,
    )
    drift_manifest = _capture(
        tmp_path / "drift",
        seed=9,
        wall_start_ns=BASE_WALL_NS + 48 * 60 * 60 * SECOND_NS,
        cadence_ns=SECOND_NS,
        spread=0.001,
        precision_ns=1,
        decimal_places=5,
    )
    reference = fit_broker_delivery_fingerprint(
        tmp_path / "reference", (reference_manifest,)
    )
    stable = fit_broker_delivery_fingerprint(
        tmp_path / "stable", (stable_manifest,)
    )
    drift = fit_broker_delivery_fingerprint(
        tmp_path / "drift", (drift_manifest,)
    )

    stable_comparison = compare_broker_delivery_fingerprints(reference, stable)
    assert any(
        item.status is BrokerDeliveryDriftStatus.STABLE
        for item in stable_comparison.comparisons
    )
    comparison = compare_broker_delivery_fingerprints(reference, drift)
    material_names = {
        item.metric_name
        for item in comparison.comparisons
        if item.status is BrokerDeliveryDriftStatus.MATERIAL_DRIFT
        and item.condition_key == "global"
    }
    assert {
        "active_quote_interarrival_ns",
        "spread",
        "source_timestamp_precision_ns",
        "price_decimal_places",
    }.issubset(material_names)
    assert comparison.material_drift_count > 0
    assert comparison.to_dict()["global_similarity_score"] is None
    assert (
        BrokerDeliveryFingerprintComparisonV1.from_json(comparison.to_json())
        == comparison
    )

    bounded = compare_broker_delivery_fingerprints(
        reference,
        drift,
        config=BrokerDeliveryDriftConfigV1(max_comparisons=3),
    )
    assert len(bounded.comparisons) == 3
    assert bounded.truncated
    assert bounded.comparison_candidate_count > 3


def test_successor_is_versioned_without_mutating_prior_synthetic_lineage(
    tmp_path: Path,
) -> None:
    first_manifest = _capture(
        tmp_path / "first", seed=10, wall_start_ns=BASE_WALL_NS
    )
    second_manifest = _capture(
        tmp_path / "second",
        seed=11,
        wall_start_ns=BASE_WALL_NS + 24 * 60 * 60 * SECOND_NS,
    )
    first = fit_broker_delivery_fingerprint(
        tmp_path / "first", (first_manifest,)
    )
    stream = _synthetic_stream(first.fingerprint_id)
    prior_bytes = stream.to_json()

    successor = fit_broker_delivery_fingerprint(
        tmp_path / "second",
        (second_manifest,),
        supersedes=first,
        effective_start_utc_ns=first.effective_start_utc_ns + SECOND_NS,
    )

    assert successor.supersedes_fingerprint_id == first.fingerprint_id
    assert successor.fingerprint_id != first.fingerprint_id
    assert stream.to_json() == prior_bytes
    generated = next(
        event for event in stream.events if event.broker_profile_id
    )
    assert generated.broker_profile_id == first.fingerprint_id
    with pytest.raises(BrokerDeliveryFingerprintIdentityError):
        fit_broker_delivery_fingerprint(
            tmp_path / "second",
            (second_manifest,),
            supersedes=first,
            effective_start_utc_ns=first.effective_start_utc_ns,
        )


def test_fingerprint_artifacts_are_atomic_immutable_and_verified(
    tmp_path: Path,
) -> None:
    first_manifest = _capture(
        tmp_path / "first", seed=12, wall_start_ns=BASE_WALL_NS
    )
    second_manifest = _capture(
        tmp_path / "second",
        seed=13,
        wall_start_ns=BASE_WALL_NS + 24 * 60 * 60 * SECOND_NS,
        spread=0.0004,
    )
    first = fit_broker_delivery_fingerprint(
        tmp_path / "first", (first_manifest,)
    )
    second = fit_broker_delivery_fingerprint(
        tmp_path / "second", (second_manifest,)
    )
    target = tmp_path / "profiles" / "broker-fingerprint.json"

    artifact = write_broker_delivery_fingerprint(target, first)
    assert artifact.sha256 == hashlib.sha256(target.read_bytes()).hexdigest()
    assert artifact.metadata["fingerprint_id"] == first.fingerprint_id
    assert load_broker_delivery_fingerprint(target) == first
    assert write_broker_delivery_fingerprint(target, first) == artifact
    with pytest.raises(
        BrokerDeliveryFingerprintArtifactError, match="other content"
    ):
        write_broker_delivery_fingerprint(target, second)
    target.write_text("{}\n", encoding="utf-8")
    with pytest.raises(BrokerDeliveryFingerprintArtifactError, match="invalid"):
        load_broker_delivery_fingerprint(target)


def test_capture_identity_mixing_is_refused(tmp_path: Path) -> None:
    first = _capture(tmp_path / "first", seed=14, wall_start_ns=BASE_WALL_NS)
    second = _capture(
        tmp_path / "second",
        seed=15,
        wall_start_ns=BASE_WALL_NS + SECOND_NS,
        adapter_config_sha256=hashlib.sha256(b"other-config").hexdigest(),
    )
    with pytest.raises(BrokerDeliveryFingerprintIdentityError):
        fit_broker_delivery_fingerprint(tmp_path, (first, second))


def _capture(
    root: Path,
    *,
    seed: int,
    wall_start_ns: int,
    cadence_ns: int = 100_000_000,
    spread: float = 0.0002,
    precision_ns: int = 100_000,
    decimal_places: int = 4,
    completed: bool = True,
    clock_correction_ns: int | None = None,
    adapter_config_sha256: str = CONFIG_SHA256,
) -> BrokerCaptureSessionManifestV1:
    session = BrokerCaptureSessionV1(
        adapter_id="fixture.broker",
        adapter_version="1.0.0",
        adapter_config_sha256=adapter_config_sha256,
        protocol="fixture-stream",
        environment_id="paper",
        server_id="fixture-server",
        account_id_sha256=ACCOUNT_SHA256,
        host_id_sha256=hashlib.sha256(f"host-{seed}".encode()).hexdigest(),
        started_at_utc_ns=wall_start_ns + seed,
        started_at_monotonic_ns=BASE_MONOTONIC_NS + seed,
        public_metadata={"fixture_seed": seed},
    )
    writer = AppendOnlyBrokerCaptureWriterV1(
        root,
        session=session,
        storage_policy=_storage_policy(),
    )
    messages: list[BrokerAdapterMessageV1] = [
        BrokerAdapterMessageV1(
            kind=BrokerCaptureEventKind.PROCESS_START,
            reason_code="collector_started",
        ),
        BrokerAdapterMessageV1(
            kind=BrokerCaptureEventKind.CONNECTION_OPEN,
            connection_id="connection-1",
        ),
        BrokerAdapterMessageV1(
            kind=BrokerCaptureEventKind.SUBSCRIPTION_ADD,
            connection_id="connection-1",
            subscription_id="subscription-eurusd",
            symbol="EURUSD",
        ),
    ]
    first_quote = _quote(
        index=0,
        wall_ns=wall_start_ns,
        spread=spread,
        precision_ns=precision_ns,
        decimal_places=decimal_places,
        source_batch_id="batch-1",
    )
    messages.extend(
        (first_quote, BrokerAdapterMessageV1.from_json(first_quote.to_json()))
    )
    messages.extend(
        _quote(
            index=index,
            wall_ns=wall_start_ns + index * cadence_ns,
            spread=spread,
            precision_ns=precision_ns,
            decimal_places=decimal_places,
            source_batch_id=f"batch-{1 + index // 4}",
        )
        for index in range(2, 12)
    )
    messages.extend(
        (
            BrokerAdapterMessageV1(
                kind=BrokerCaptureEventKind.OUTAGE_START,
                reason_code="fixture_outage",
            ),
            BrokerAdapterMessageV1(
                kind=BrokerCaptureEventKind.OUTAGE_END,
                gap_duration_ns=2 * SECOND_NS,
                reason_code="fixture_outage_recovered",
            ),
            BrokerAdapterMessageV1(
                kind=BrokerCaptureEventKind.RECONNECT,
                connection_id="connection-2",
                reason_code="fixture_reconnect",
            ),
            _quote(
                index=12,
                wall_ns=wall_start_ns + 12 * cadence_ns,
                spread=spread,
                precision_ns=precision_ns,
                decimal_places=decimal_places,
                source_batch_id="batch-4",
            ),
            _quote(
                index=13,
                wall_ns=wall_start_ns + 13 * cadence_ns,
                spread=spread,
                precision_ns=precision_ns,
                decimal_places=decimal_places,
                source_batch_id="batch-4",
            ),
            BrokerAdapterMessageV1(
                kind=BrokerCaptureEventKind.PROCESS_STOP,
                reason_code="collector_stopped",
            ),
        )
    )
    if clock_correction_ns is not None:
        messages.insert(
            3,
            BrokerAdapterMessageV1(
                kind=BrokerCaptureEventKind.CLOCK_CORRECTION,
                reason_code="wall_monotonic_divergence",
            ),
        )
    monotonic_ns = BASE_MONOTONIC_NS
    receive_wall_ns = wall_start_ns
    for sequence, message in enumerate(messages):
        gap = (
            6 * SECOND_NS
            if message.kind is BrokerCaptureEventKind.OUTAGE_END
            else cadence_ns
        )
        monotonic_ns += gap
        receive_wall_ns += gap
        writer.append(
            BrokerCaptureEventV1(
                session_id=session.session_id,
                capture_sequence=sequence,
                receive_time_utc_ns=receive_wall_ns,
                receive_time_monotonic_ns=monotonic_ns,
                message=message,
                clock_offset_change_ns=(
                    clock_correction_ns
                    if message.kind is BrokerCaptureEventKind.CLOCK_CORRECTION
                    else None
                ),
            )
        )
    return writer.close(
        completed=completed,
        limitations=(() if completed else ("collector_failure:fixture",)),
    )


def _quote(
    *,
    index: int,
    wall_ns: int,
    spread: float,
    precision_ns: int,
    decimal_places: int,
    source_batch_id: str,
) -> BrokerAdapterMessageV1:
    bid_text = f"{1.1 + index * 0.0001:.{decimal_places}f}"
    bid = float(bid_text)
    ask_text = f"{bid + spread:.{decimal_places}f}"
    ask = float(ask_text)
    return BrokerAdapterMessageV1(
        kind=BrokerCaptureEventKind.QUOTE,
        source_event_time_ns=wall_ns,
        source_timestamp_semantics=BrokerCaptureSourceTimestampSemantics.BROKER_EVENT,
        source_timestamp_precision_ns=precision_ns,
        source_sequence=index,
        source_message_id=f"quote-{index}",
        source_batch_id=source_batch_id,
        symbol="EURUSD",
        bid=bid,
        ask=ask,
        bid_text=bid_text,
        ask_text=ask_text,
        price_text_semantics=BrokerCapturePriceTextSemantics.SOURCE_LEXEME,
    )


def _storage_policy() -> BrokerCaptureStoragePolicyV1:
    return BrokerCaptureStoragePolicyV1(
        max_partition_events=7,
        max_partition_bytes=2 * 1024**2,
        max_partition_duration_ns=60 * SECOND_NS,
        max_session_bytes=32 * 1024**2,
        high_watermark_bytes=24 * 1024**2,
        max_retained_partitions=100,
        manifest_reserve_bytes=64 * 1024,
        fsync_each_event=False,
    )


def _timeline() -> MarketContextTimelineV1:
    source = MarketContextSourceV1(
        name="Fixture macro schedule",
        source_version="2023-12-25-v1",
        retrieved_at_ns=BASE_WALL_NS - SECOND_NS,
        content_sha256="a" * 64,
        adapter_name="fixture-macro",
        adapter_version="1.0",
        license_name="Fixture-only",
        redistribution_allowed=False,
        redistribution_constraints=("Fixture only.",),
        limitations=("Synthetic test event.",),
    )
    event = MarketContextEventV1(
        canonical_key="us.fixture.release.2023-12-25",
        kind=MarketContextKind.MACRO_RELEASE,
        title="Fixture release",
        source=source,
        source_event_time="2023-12-25T12:00:00+00:00",
        source_timezone="UTC",
        event_time_ns=BASE_WALL_NS,
        first_known_at_ns=BASE_WALL_NS - 2 * SECOND_NS,
        available_at_ns=BASE_WALL_NS - 2 * SECOND_NS,
        pre_event_ns=SECOND_NS,
        post_event_ns=60 * SECOND_NS,
        affected_currencies=("USD",),
        affected_symbols=("EURUSD",),
        confidence=1.0,
        precision=MarketContextPrecision.EXACT,
        limitations=("Synthetic test event.",),
        vintage_id="fixture-v1",
        tags=("scheduled",),
    )
    return MarketContextTimelineV1(
        timeline_version="fixture-2023-12-25-v1",
        coverage_start_ns=BASE_WALL_NS - 60 * SECOND_NS,
        coverage_end_ns=BASE_WALL_NS + 4 * 24 * 60 * 60 * SECOND_NS,
        complete=True,
        events=(event,),
        limitations=("Fixture timeline only.",),
    )


def _synthetic_stream(profile_id: str) -> SyntheticEventStreamV1:
    left = SyntheticEventV1.observed(
        symbol="EURUSD",
        event_time_ns=BASE_WALL_NS,
        event_sequence=0,
        bid=1.1,
        ask=1.1002,
        run_id="run-1",
        ensemble_member_id="member-1",
        source_version_id="source-v1",
        source_series_id="series-1",
        source_period="202312",
        source_row_id=1,
    )
    right = SyntheticEventV1.observed(
        symbol="EURUSD",
        event_time_ns=BASE_WALL_NS + 2 * SECOND_NS,
        event_sequence=0,
        bid=1.1001,
        ask=1.1003,
        run_id="run-1",
        ensemble_member_id="member-1",
        source_version_id="source-v1",
        source_series_id="series-1",
        source_period="202312",
        source_row_id=2,
    )
    generated = SyntheticEventV1.generated(
        symbol="EURUSD",
        event_time_ns=BASE_WALL_NS + SECOND_NS,
        event_sequence=0,
        bid=1.10005,
        ask=1.10025,
        run_id="run-1",
        ensemble_member_id="member-1",
        source_version_id="source-v1",
        left_anchor_event_id=left.event_id,
        right_anchor_event_id=right.event_id,
        generator_id="fixture-generator",
        generator_version="1.0.0",
        generator_config_id="generator-config-v1",
        broker_profile_id=profile_id,
        constraint_set_id="constraint-set-v1",
    )
    return SyntheticEventStreamV1(
        run_id="run-1",
        ensemble_member_id="member-1",
        symbol="EURUSD",
        events=(left, generated, right),
    )


def _cell(
    fingerprint: BrokerDeliveryFingerprintV1, key: str
) -> BrokerDeliveryCellV1:
    return next(item for item in fingerprint.cells if item.condition.key == key)
