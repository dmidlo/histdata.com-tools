"""Trust and failure tests for append-only broker delivery capture."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from histdatacom.broker_capture import (
    AppendOnlyBrokerCaptureWriterV1,
    BrokerAdapterMessageV1,
    BrokerCaptureActivitySemantics,
    BrokerCaptureAdapterV1,
    BrokerCaptureBackpressureError,
    BrokerCaptureEventKind,
    BrokerCaptureEventV1,
    BrokerCaptureExistingSessionError,
    BrokerCaptureIntegrityError,
    BrokerCapturePriceTextSemantics,
    BrokerCaptureQuotaError,
    BrokerCaptureReplaySummaryV1,
    BrokerCaptureRetentionError,
    BrokerCaptureSessionManifestV1,
    BrokerCaptureSessionState,
    BrokerCaptureSessionV1,
    BrokerCaptureSizeSemantics,
    BrokerCaptureStoragePolicyV1,
    BrokerCaptureSourceTimestampSemantics,
    LiveBrokerCaptureSourceV1,
    SequenceBrokerCaptureAdapterV1,
    SequenceBrokerCaptureClockV1,
    consume_broker_capture_source,
    discover_broker_capture_session_manifests,
    inspect_broker_capture_session,
    load_broker_capture_session_manifest,
    logical_capture_content_sha256,
    replay_broker_capture_session,
    verify_broker_capture_partition_manifests,
)

_WALL_BASE = 1_700_000_000_000_000_000
_MONOTONIC_BASE = 2_000_000_000


@dataclass(slots=True)
class _CollectingConsumer:
    events: list[BrokerCaptureEventV1] = field(default_factory=list)

    def on_event(self, event: BrokerCaptureEventV1) -> None:
        self.events.append(event)


class _PrivateConfigurationAdapter:
    adapter_id = "synthetic.fixture"
    adapter_version = "1.0.0"

    def __init__(
        self, credential: str, messages: tuple[BrokerAdapterMessageV1, ...]
    ) -> None:
        self._credential = credential
        self._messages = messages

    def iter_messages(self) -> tuple[BrokerAdapterMessageV1, ...]:
        return self._messages


def test_capture_contracts_are_versioned_roundtrippable_and_secret_free() -> (
    None
):
    session = _session(1)
    message = BrokerAdapterMessageV1(
        kind=BrokerCaptureEventKind.QUOTE,
        source_event_time_ns=_WALL_BASE,
        source_timestamp_semantics=(
            BrokerCaptureSourceTimestampSemantics.BROKER_EVENT
        ),
        source_timestamp_precision_ns=1_000_000,
        source_sequence=7,
        source_message_id="quote-7",
        source_batch_id="batch-1",
        symbol="eurusd",
        bid=1.1,
        ask=1.1002,
        bid_text="1.1000",
        ask_text="1.1002",
        price_text_semantics=BrokerCapturePriceTextSemantics.SOURCE_LEXEME,
        bid_size=2.0,
        ask_size=3.0,
        size_semantics=BrokerCaptureSizeSemantics.QUOTED_SIZE,
        activity_value=5.0,
        activity_semantics=BrokerCaptureActivitySemantics.BROKER_ACTIVITY,
        raw_message_sha256=hashlib.sha256(b"public-message").hexdigest(),
        public_metadata={"channel": "prices", "venue_class": "retail"},
    )
    event = BrokerCaptureEventV1(
        session_id=session.session_id,
        capture_sequence=0,
        receive_time_utc_ns=_WALL_BASE + 1,
        receive_time_monotonic_ns=_MONOTONIC_BASE + 1,
        message=message,
    )
    policy = _policy()

    assert BrokerCaptureSessionV1.from_json(session.to_json()) == session
    assert BrokerAdapterMessageV1.from_json(message.to_json()) == message
    assert BrokerCaptureEventV1.from_json(event.to_json()) == event
    assert BrokerCaptureStoragePolicyV1.from_json(policy.to_json()) == policy
    assert message.symbol == "EURUSD"
    assert message.bid_text == "1.1000"
    assert message.source_timestamp_precision_ns == 1_000_000
    assert message.message_id.startswith("broker-message-")
    assert event.event_id.startswith("broker-capture-event-")

    secret = "do-not-persist-this-token"
    with pytest.raises(ValueError, match="sensitive metadata key") as key_error:
        BrokerAdapterMessageV1(
            kind=BrokerCaptureEventKind.HEARTBEAT,
            public_metadata={"access_token": secret},
        )
    assert secret not in str(key_error.value)
    with pytest.raises(
        ValueError, match="sensitive metadata value"
    ) as value_error:
        BrokerCaptureSessionV1(
            **{
                **_session_kwargs(2),
                "public_metadata": {"header": f"Bearer {secret}"},
            }
        )
    assert secret not in str(value_error.value)
    with pytest.raises(ValueError, match="explicit size semantics"):
        BrokerAdapterMessageV1(
            kind=BrokerCaptureEventKind.QUOTE,
            symbol="EURUSD",
            bid=1.0,
            ask=1.1,
            bid_size=1.0,
        )


def test_live_source_surfaces_fixture_health_ordering_and_clock_drift() -> None:
    session = _session(3)
    source, fixture_messages = _fixture_source(session)
    events = tuple(source.iter_events())

    assert [event.capture_sequence for event in events] == list(
        range(len(events))
    )
    assert all(event.receive_time_utc_ns > 0 for event in events)
    assert all(event.receive_time_monotonic_ns > 0 for event in events)
    assert all(
        current.receive_time_monotonic_ns >= previous.receive_time_monotonic_ns
        for previous, current in zip(events, events[1:])
    )
    assert len(events) == len(fixture_messages) + 1

    kinds = [event.kind for event in events]
    for required in (
        BrokerCaptureEventKind.PROCESS_START,
        BrokerCaptureEventKind.PROCESS_STOP,
        BrokerCaptureEventKind.PROCESS_RESTART,
        BrokerCaptureEventKind.CONNECTION_OPEN,
        BrokerCaptureEventKind.CONNECTION_CLOSE,
        BrokerCaptureEventKind.RECONNECT,
        BrokerCaptureEventKind.SUBSCRIPTION_ADD,
        BrokerCaptureEventKind.SUBSCRIPTION_REMOVE,
        BrokerCaptureEventKind.HEARTBEAT,
        BrokerCaptureEventKind.GAP,
        BrokerCaptureEventKind.OUTAGE_START,
        BrokerCaptureEventKind.OUTAGE_END,
        BrokerCaptureEventKind.CLOCK_CORRECTION,
    ):
        assert required in kinds

    correction = next(
        event
        for event in events
        if event.kind is BrokerCaptureEventKind.CLOCK_CORRECTION
    )
    assert correction.clock_offset_change_ns == 100_000_000
    assert correction.message.reason_code == "wall_monotonic_divergence"

    quotes = [
        event for event in events if event.kind is BrokerCaptureEventKind.QUOTE
    ]
    assert quotes[0].message.message_id == quotes[1].message.message_id
    assert quotes[0].event_id != quotes[1].event_id
    assert quotes[0].message.bid == quotes[2].message.bid
    assert quotes[0].message.ask == quotes[2].message.ask
    assert (
        quotes[1].receive_time_monotonic_ns
        - quotes[0].receive_time_monotonic_ns
        < 1_000_000
    )
    gap = next(
        event for event in events if event.kind is BrokerCaptureEventKind.GAP
    )
    gap_index = events.index(gap)
    assert gap.message.gap_duration_ns == 5_000_000_000
    assert (
        gap.receive_time_monotonic_ns
        - events[gap_index - 1].receive_time_monotonic_ns
        > 4_000_000_000
    )


def test_append_only_rotation_replay_and_live_consumer_parity(
    tmp_path: Path,
) -> None:
    session = _session(4)
    source, _messages = _fixture_source(session)
    policy = _policy(max_partition_events=4)
    writer = AppendOnlyBrokerCaptureWriterV1(
        tmp_path, session=session, storage_policy=policy
    )
    live_consumer = _CollectingConsumer()

    live_result = consume_broker_capture_source(
        source,
        sink=writer,
        consumers=(live_consumer,),
    )
    manifest = writer.close()

    assert manifest.state is BrokerCaptureSessionState.COMPLETED
    assert manifest.complete
    assert manifest.event_count == live_result.event_count == 17
    assert len(manifest.partitions) == 5
    assert all(partition.completed for partition in manifest.partitions)
    assert manifest.partial_artifact_count == 0
    assert sum(manifest.event_kind_counts.values()) == 17
    assert inspect_broker_capture_session(tmp_path, session.session_id).clean

    manifest_path = tmp_path / session.session_id / "session.manifest.json"
    restored = load_broker_capture_session_manifest(manifest_path)
    assert restored == manifest
    assert (
        BrokerCaptureSessionManifestV1.from_json(manifest.to_json()) == manifest
    )
    assert discover_broker_capture_session_manifests(tmp_path) == (manifest,)
    assert (
        verify_broker_capture_partition_manifests(tmp_path, manifest)
        == manifest
    )

    replay_consumer = _CollectingConsumer()
    replay_summary = replay_broker_capture_session(
        tmp_path, manifest, consumers=(replay_consumer,)
    )
    assert replay_consumer.events == live_consumer.events
    assert replay_summary.event_kind_counts == live_result.event_kind_counts
    assert replay_summary.logical_content_sha256 == (
        logical_capture_content_sha256(live_consumer.events)
    )
    assert (
        BrokerCaptureReplaySummaryV1.from_json(replay_summary.to_json())
        == replay_summary
    )


def test_private_adapter_configuration_never_enters_capture_artifacts(
    tmp_path: Path,
) -> None:
    credential = "super-secret-broker-credential"
    session = _session(5)
    _source, messages = _fixture_source(session)
    adapter = _PrivateConfigurationAdapter(credential, messages[:3])
    assert isinstance(adapter, BrokerCaptureAdapterV1)
    source = LiveBrokerCaptureSourceV1(
        session=session,
        adapter=adapter,
        clock=SequenceBrokerCaptureClockV1(_clock_samples(3)),
    )
    writer = AppendOnlyBrokerCaptureWriterV1(
        tmp_path, session=session, storage_policy=_policy()
    )
    consume_broker_capture_source(source, sink=writer)
    writer.close()

    artifact_bytes = b"".join(
        path.read_bytes()
        for path in sorted(tmp_path.rglob("*"))
        if path.is_file()
    )
    assert credential.encode("utf-8") not in artifact_bytes


def test_partial_and_orphan_artifacts_are_detected_but_not_advertised(
    tmp_path: Path,
) -> None:
    session = _session(6)
    source, _messages = _fixture_source(session)
    writer = AppendOnlyBrokerCaptureWriterV1(
        tmp_path, session=session, storage_policy=_policy()
    )
    consume_broker_capture_source(source, sink=writer)
    manifest = writer.close()
    session_dir = tmp_path / session.session_id

    partial = session_dir / "partition-999998.jsonl.partial"
    orphan = session_dir / "partition-999999.jsonl"
    partial.write_text('{"partial":', encoding="utf-8")
    orphan.write_text("{}\n", encoding="utf-8")

    inspection = inspect_broker_capture_session(tmp_path, session.session_id)
    assert inspection.partial_artifacts == (partial.name,)
    assert inspection.orphan_data_artifacts == (orphan.name,)
    assert not inspection.clean
    discovered = discover_broker_capture_session_manifests(tmp_path)
    assert discovered == (manifest,)
    assert all(
        partition.data_artifact.path.endswith(".jsonl")
        for partition in discovered[0].partitions
    )


def test_quota_backpressure_and_retention_refuse_predictably(
    tmp_path: Path,
) -> None:
    retention_session = _session(7)
    retention_source, _messages = _fixture_source(retention_session)
    retention_events = tuple(retention_source.iter_events())
    retention_writer = AppendOnlyBrokerCaptureWriterV1(
        tmp_path / "retention",
        session=retention_session,
        storage_policy=_policy(
            max_partition_events=1,
            max_retained_partitions=1,
        ),
    )
    retention_writer.append(retention_events[0])
    with pytest.raises(BrokerCaptureRetentionError, match="retention ceiling"):
        retention_writer.append(retention_events[1])
    failed_manifest = retention_writer.close(
        completed=False, limitations=("retention_refusal",)
    )
    assert failed_manifest.state is BrokerCaptureSessionState.FAILED
    assert failed_manifest.limitations == ("retention_refusal",)
    assert failed_manifest.event_kind_counts == {"process_start": 1}

    quota_session = _session(8)
    quota_event = _large_event(quota_session)
    line_size = len((quota_event.to_json() + "\n").encode("utf-8"))
    reserve = 64 * 1024
    quota_writer = AppendOnlyBrokerCaptureWriterV1(
        tmp_path / "quota",
        session=quota_session,
        storage_policy=_policy(
            max_partition_bytes=line_size + 100,
            manifest_reserve_bytes=reserve,
            max_session_bytes=line_size + reserve + 100,
            high_watermark_bytes=line_size + reserve + 100,
        ),
    )
    with pytest.raises(BrokerCaptureQuotaError, match="disk quota"):
        quota_writer.append(quota_event)
    quota_writer.close(completed=False, limitations=("quota_refusal",))

    pressure_session = _session(9)
    pressure_event = _large_event(pressure_session)
    pressure_line_size = len((pressure_event.to_json() + "\n").encode("utf-8"))
    pressure_writer = AppendOnlyBrokerCaptureWriterV1(
        tmp_path / "pressure",
        session=pressure_session,
        storage_policy=_policy(
            max_partition_bytes=pressure_line_size + 100,
            manifest_reserve_bytes=reserve,
            max_session_bytes=pressure_line_size + reserve + 100_000,
            high_watermark_bytes=pressure_line_size + reserve,
        ),
    )
    with pytest.raises(BrokerCaptureBackpressureError, match="high watermark"):
        pressure_writer.append(pressure_event)
    pressure_writer.close(
        completed=False, limitations=("backpressure_refusal",)
    )


def test_existing_session_and_corrupt_replay_fail_closed(
    tmp_path: Path,
) -> None:
    session = _session(10)
    source, _messages = _fixture_source(session)
    policy = _policy()
    writer = AppendOnlyBrokerCaptureWriterV1(
        tmp_path, session=session, storage_policy=policy
    )
    consume_broker_capture_source(source, sink=writer)
    manifest = writer.close()

    with pytest.raises(BrokerCaptureExistingSessionError):
        AppendOnlyBrokerCaptureWriterV1(
            tmp_path, session=session, storage_policy=policy
        )

    first_sidecar = (
        tmp_path / session.session_id / "partition-000000.manifest.json"
    )
    original_sidecar = first_sidecar.read_text(encoding="utf-8")
    first_sidecar.write_text("{}\n", encoding="utf-8")
    with pytest.raises(
        BrokerCaptureIntegrityError, match="manifest is invalid"
    ):
        replay_broker_capture_session(tmp_path, manifest)
    first_sidecar.write_text(original_sidecar, encoding="utf-8")

    first_path = tmp_path / manifest.partitions[0].data_artifact.path
    first_path.write_bytes(first_path.read_bytes() + b"{}\n")
    with pytest.raises(BrokerCaptureIntegrityError, match="size"):
        replay_broker_capture_session(tmp_path, manifest)


def _session(seed: int) -> BrokerCaptureSessionV1:
    return BrokerCaptureSessionV1(**_session_kwargs(seed))


def _session_kwargs(seed: int) -> dict[str, object]:
    return {
        "adapter_id": "synthetic.fixture",
        "adapter_version": "1.0.0",
        "adapter_config_sha256": hashlib.sha256(
            f"public-config-{seed}".encode("utf-8")
        ).hexdigest(),
        "protocol": "fixture-stream",
        "environment_id": "paper",
        "server_id": "fixture-server",
        "started_at_utc_ns": _WALL_BASE + seed,
        "started_at_monotonic_ns": _MONOTONIC_BASE + seed,
        "account_id_sha256": hashlib.sha256(
            f"account-{seed}".encode("utf-8")
        ).hexdigest(),
        "host_id_sha256": hashlib.sha256(
            f"host-{seed}".encode("utf-8")
        ).hexdigest(),
        "public_metadata": {"fixture": True, "seed": seed},
    }


def _fixture_source(
    session: BrokerCaptureSessionV1,
) -> tuple[LiveBrokerCaptureSourceV1, tuple[BrokerAdapterMessageV1, ...]]:
    quote = BrokerAdapterMessageV1(
        kind=BrokerCaptureEventKind.QUOTE,
        source_event_time_ns=_WALL_BASE + 3_000_000,
        source_timestamp_semantics=(
            BrokerCaptureSourceTimestampSemantics.BROKER_EVENT
        ),
        source_timestamp_precision_ns=100_000,
        source_batch_id="batch-burst-1",
        symbol="EURUSD",
        bid=1.1000,
        ask=1.1002,
        bid_text="1.1000",
        ask_text="1.1002",
        price_text_semantics=BrokerCapturePriceTextSemantics.SOURCE_LEXEME,
        bid_size=2.0,
        ask_size=3.0,
        size_semantics=BrokerCaptureSizeSemantics.QUOTED_SIZE,
        activity_value=1.0,
        activity_semantics=BrokerCaptureActivitySemantics.MESSAGE_COUNT,
        raw_message_sha256=hashlib.sha256(b"quote-a").hexdigest(),
    )
    messages = (
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
        quote,
        BrokerAdapterMessageV1.from_json(quote.to_json()),
        BrokerAdapterMessageV1(
            kind=BrokerCaptureEventKind.QUOTE,
            source_event_time_ns=_WALL_BASE + 3_100_000,
            source_timestamp_semantics=(
                BrokerCaptureSourceTimestampSemantics.BROKER_EVENT
            ),
            source_timestamp_precision_ns=100_000,
            source_batch_id="batch-burst-1",
            symbol="EURUSD",
            bid=1.1000,
            ask=1.1002,
            bid_text="1.1000",
            ask_text="1.1002",
            price_text_semantics=(
                BrokerCapturePriceTextSemantics.SOURCE_LEXEME
            ),
        ),
        BrokerAdapterMessageV1(
            kind=BrokerCaptureEventKind.QUOTE,
            source_event_time_ns=_WALL_BASE + 3_200_000,
            source_timestamp_semantics=(
                BrokerCaptureSourceTimestampSemantics.BROKER_EVENT
            ),
            source_timestamp_precision_ns=100_000,
            source_batch_id="batch-burst-1",
            symbol="EURUSD",
            bid=1.1001,
            ask=1.1003,
            bid_text="1.1001",
            ask_text="1.1003",
            price_text_semantics=(
                BrokerCapturePriceTextSemantics.SOURCE_LEXEME
            ),
        ),
        BrokerAdapterMessageV1(
            kind=BrokerCaptureEventKind.HEARTBEAT,
            connection_id="connection-1",
        ),
        BrokerAdapterMessageV1(
            kind=BrokerCaptureEventKind.GAP,
            gap_duration_ns=5_000_000_000,
            reason_code="known_quiet_gap",
        ),
        BrokerAdapterMessageV1(
            kind=BrokerCaptureEventKind.OUTAGE_START,
            reason_code="fixture_outage",
        ),
        BrokerAdapterMessageV1(
            kind=BrokerCaptureEventKind.OUTAGE_END,
            gap_duration_ns=1_000_000_000,
            reason_code="fixture_outage_recovered",
        ),
        BrokerAdapterMessageV1(
            kind=BrokerCaptureEventKind.CONNECTION_CLOSE,
            connection_id="connection-1",
            reason_code="fixture_disconnect",
        ),
        BrokerAdapterMessageV1(
            kind=BrokerCaptureEventKind.RECONNECT,
            connection_id="connection-2",
            reason_code="fixture_reconnect",
        ),
        BrokerAdapterMessageV1(
            kind=BrokerCaptureEventKind.PROCESS_RESTART,
            reason_code="fixture_process_restart",
            public_metadata={"previous_process": "fixture-process-1"},
        ),
        BrokerAdapterMessageV1(
            kind=BrokerCaptureEventKind.SUBSCRIPTION_REMOVE,
            connection_id="connection-2",
            subscription_id="subscription-eurusd",
            symbol="EURUSD",
        ),
        BrokerAdapterMessageV1(
            kind=BrokerCaptureEventKind.PROCESS_STOP,
            reason_code="collector_stopped",
        ),
    )
    adapter = SequenceBrokerCaptureAdapterV1(
        adapter_id=session.adapter_id,
        adapter_version=session.adapter_version,
        messages=messages,
    )
    source = LiveBrokerCaptureSourceV1(
        session=session,
        adapter=adapter,
        clock=SequenceBrokerCaptureClockV1(_clock_samples(len(messages))),
        clock_correction_threshold_ns=5_000_000,
    )
    return source, messages


def _clock_samples(count: int) -> tuple[tuple[int, int], ...]:
    increments = (
        0,
        1_000_000,
        2_000_000,
        3_000_000,
        3_100_000,
        3_200_000,
        3_300_000,
        4_000_000,
        5_004_000_000,
        5_005_000_000,
        5_006_000_000,
        5_007_000_000,
        5_008_000_000,
        5_009_000_000,
        5_010_000_000,
        5_011_000_000,
    )
    if count > len(increments):
        raise ValueError("fixture requests too many clock samples")
    return tuple(
        (
            _WALL_BASE + delta + (100_000_000 if index >= 10 else 0),
            _MONOTONIC_BASE + delta,
        )
        for index, delta in enumerate(increments[:count])
    )


def _policy(**overrides: object) -> BrokerCaptureStoragePolicyV1:
    values: dict[str, object] = {
        "max_partition_events": 100,
        "max_partition_bytes": 2 * 1024**2,
        "max_partition_duration_ns": 60 * 1_000_000_000,
        "max_session_bytes": 32 * 1024**2,
        "high_watermark_bytes": 24 * 1024**2,
        "max_retained_partitions": 100,
        "manifest_reserve_bytes": 64 * 1024,
        "fsync_each_event": True,
    }
    values.update(overrides)
    return BrokerCaptureStoragePolicyV1(**values)


def _large_event(session: BrokerCaptureSessionV1) -> BrokerCaptureEventV1:
    message = BrokerAdapterMessageV1(
        kind=BrokerCaptureEventKind.HEARTBEAT,
        public_metadata={"bounded_fixture_padding": "x" * 18_000},
    )
    return BrokerCaptureEventV1(
        session_id=session.session_id,
        capture_sequence=0,
        receive_time_utc_ns=_WALL_BASE,
        receive_time_monotonic_ns=_MONOTONIC_BASE,
        message=message,
    )
