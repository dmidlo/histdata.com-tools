"""Generated adapters and actual local durable writes; no provider campaign."""

import os
from dataclasses import replace

import pytest

from histdatacom.broker_capture import (
    AppendOnlyBrokerCaptureWriterV1,
    BrokerAdapterMessageV1,
    BrokerCaptureEventKind,
    BrokerCaptureReplaySourceV1,
    SequenceBrokerCaptureAdapterV1,
)
from histdatacom.broker_plugin_health import (
    BrokerHostHealthFailure as Failure,
)
from histdatacom.broker_plugin_health import (
    BrokerHostHealthObservationKind as Kind,
)
from histdatacom.broker_plugin_health import (
    BrokerHostHealthPolicyV1,
)
from histdatacom.broker_plugin_health import (
    BrokerHostHealthState as State,
)
from histdatacom.broker_plugin_health.runtime_legacy import (
    BrokerHostHealthAdmissionError,
    capture_legacy_with_host_health,
    read_legacy_host_health,
    require_legacy_capture_health,
)
from histdatacom.broker_plugin_health.storage import _observations
from tests.fixtures.broker_provider_policy import (
    generated_legacy_request,
    generated_provider_scope,
    legacy_policy_inputs,
)


class TickClock:
    def __init__(self, session):
        self.wall = session.started_at_utc_ns
        self.mono = session.started_at_monotonic_ns

    def sample(self):
        self.wall += 1_000_000
        self.mono += 1_000_000
        return self.wall, self.mono


def capture(
    tmp_path, *, fsync_each_event=False, messages=None, clock=None, policy=None
):
    inputs = legacy_policy_inputs()
    request = generated_legacy_request(inputs.session)
    adapter = SequenceBrokerCaptureAdapterV1(
        inputs.session.adapter_id,
        inputs.session.adapter_version,
        (
            inputs.messages[:4] + inputs.messages[-1:]
            if messages is None
            else messages
        ),
    )
    with generated_provider_scope(request):
        result = capture_legacy_with_host_health(
            tmp_path,
            provider_request=request,
            adapter=adapter,
            storage_policy=replace(
                inputs.storage_policy,
                fsync_each_event=fsync_each_event,
                policy_id=None,
            ),
            symbols=("EURUSD",),
            clock=clock or TickClock(inputs.session),
            policy=policy or BrokerHostHealthPolicyV1(),
        )
    return request, result


@pytest.mark.parametrize("fsync_each_event", [False, True])
def test_native_ingress_actual_fsync_and_independent_reader(
    tmp_path, fsync_each_event
):
    request, result = capture(tmp_path, fsync_each_event=fsync_each_event)
    assert result.audit.state is State.QUALIFIED
    with generated_provider_scope(request):
        audit = require_legacy_capture_health(
            tmp_path, result.manifest, provider_request=request
        )
        events = tuple(
            BrokerCaptureReplaySourceV1(
                tmp_path, result.manifest, provider_request=request
            ).iter_events()
        )
    assert audit.to_json() == result.audit.to_json()
    observations = tuple(_observations(result.health_directory, 1000))
    ingress = {
        item.event_id: item
        for item in observations
        if item.kind is Kind.INGRESS
    }
    persisted = {
        item.native_record_id: item
        for item in observations
        if item.kind is Kind.PERSISTED
    }
    assert len(events) == len(ingress) == len(persisted)
    for event in events:
        received = ingress[event.message.message_id]
        assert (received.utc_ns, received.monotonic_ns) == (
            event.receive_time_utc_ns,
            event.receive_time_monotonic_ns,
        )
        assert persisted[event.event_id].monotonic_ns > received.monotonic_ns
    # The native directory stays a native V1 inventory; audit is a sibling.
    assert result.health_directory.parent == tmp_path
    assert not (
        tmp_path / result.manifest.session.session_id / "audit.json"
    ).exists()


def test_buffered_append_does_not_prematurely_claim_persistence(
    tmp_path, monkeypatch
):
    from histdatacom.broker_capture import storage

    actual = storage.os.fsync
    acknowledged = []
    from histdatacom.broker_plugin_health.runtime_legacy import (
        LegacyCaptureHealthObserver,
    )

    original_persisted = LegacyCaptureHealthObserver.persisted
    native_syncs = []

    def sync(fd):
        # Actual native descriptor is only a .jsonl.partial at this boundary.
        # fstat identity permits the test to recognize it without OS path APIs.
        for path in tmp_path.glob("*/partition-*.jsonl.partial"):
            if os.fstat(fd).st_ino == path.stat().st_ino:
                native_syncs.append(path.name)
        return actual(fd)

    def persisted(observer, event):
        assert native_syncs, "native fsync must precede durable observation"
        acknowledged.append(event.capture_sequence)
        return original_persisted(observer, event)

    monkeypatch.setattr(storage.os, "fsync", sync)
    monkeypatch.setattr(LegacyCaptureHealthObserver, "persisted", persisted)
    _request, result = capture(
        tmp_path, messages=legacy_policy_inputs().messages
    )
    assert acknowledged == list(range(result.manifest.event_count))
    assert len(native_syncs) == len(result.manifest.partitions)
    observations = tuple(_observations(result.health_directory, 1000))
    first_persisted = next(
        item.sequence for item in observations if item.kind is Kind.PERSISTED
    )
    assert (
        sum(
            item.kind is Kind.INGRESS for item in observations[:first_persisted]
        )
        == 8
    )


def test_missing_historical_observations_are_unavailable_not_reconstructed(
    tmp_path,
):
    inputs = legacy_policy_inputs()
    request = generated_legacy_request(inputs.session)
    with generated_provider_scope(request):
        with AppendOnlyBrokerCaptureWriterV1(
            tmp_path,
            session=inputs.session,
            storage_policy=inputs.storage_policy,
            provider_request=request,
        ) as writer:
            for event in inputs.events:
                writer.append(event)
        result = read_legacy_host_health(
            tmp_path, writer.manifest, provider_request=request
        )
        assert result.state is State.UNAVAILABLE
        with pytest.raises(
            BrokerHostHealthAdmissionError,
            match="historical_observations_unavailable",
        ):
            require_legacy_capture_health(
                tmp_path, writer.manifest, provider_request=request
            )


@pytest.mark.parametrize(
    "mutation",
    ["audit", "observation", "invocation", "missing", "foreign", "symlink"],
)
def test_reader_refuses_rewritten_or_incomplete_host_evidence(
    tmp_path, mutation
):
    request, result = capture(tmp_path)
    directory = result.health_directory
    if mutation == "audit":
        path = directory / "audit.json"
        text = path.read_text()
        path.write_text(
            text.replace("qualified_host_boundary_only", "degraded")
        )
    elif mutation == "observation":
        path = directory / "observations.jsonl"
        lines = path.read_text().splitlines(keepends=True)
        path.write_text("".join(lines[:-1]))
    elif mutation == "invocation":
        (directory / "invocation.json").write_text("{}")
    elif mutation == "missing":
        (directory / "audit.json").unlink()
    elif mutation == "foreign":
        (directory / "foreign.json").write_text("{}")
    else:
        path = directory / "audit.json"
        target = directory.parent / "other-audit.json"
        path.rename(target)
        path.symlink_to(target)
    with generated_provider_scope(request), pytest.raises(ValueError):
        require_legacy_capture_health(
            tmp_path, result.manifest, provider_request=request
        )


def test_actual_clock_correction_control_is_persisted_without_invented_ingress(
    tmp_path,
):
    inputs = legacy_policy_inputs()
    clock = TickClock(inputs.session)
    original = clock.sample
    calls = 0

    def jump():
        nonlocal calls
        calls += 1
        if calls == 3:
            clock.wall += 2_000_000_000
        return original()

    clock.sample = jump
    request, result = capture(tmp_path, clock=clock, messages=inputs.messages)
    assert result.audit.state is State.DEGRADED
    assert Failure.CLOCK_JUMP in result.audit.failures
    observations = tuple(_observations(result.health_directory, 1000))
    controls = [
        item
        for item in observations
        if item.kind is Kind.PERSISTED and item.event_id is None
    ]
    assert len(controls) == 1
    assert controls[0].ingress_sequence is None
    assert sum(item.kind is Kind.INGRESS for item in observations) == len(
        inputs.messages
    )
    with (
        generated_provider_scope(request),
        pytest.raises(BrokerHostHealthAdmissionError),
    ):
        require_legacy_capture_health(
            tmp_path, result.manifest, provider_request=request
        )


def test_reconnect_epoch_is_bound_from_native_messages(tmp_path):
    inputs = legacy_policy_inputs()
    messages = (
        inputs.messages[:4]
        + (
            BrokerAdapterMessageV1(
                BrokerCaptureEventKind.RECONNECT, connection_id="new"
            ),
        )
        + inputs.messages[4:]
    )
    request, result = capture(tmp_path, messages=messages)
    assert {bucket.epoch for bucket in result.audit.buckets} == {0, 1}
    queue = [
        item
        for item in _observations(result.health_directory, 1000)
        if item.kind is Kind.QUEUE
    ]
    assert [(item.epoch, item.queue_items) for item in queue] == [
        (0, 0),
        (1, 0),
    ]
    for epoch in (0, 1):
        assert (
            sum(
                bucket.queue_covered_ns
                for bucket in result.audit.buckets
                if bucket.epoch == epoch
            )
            > 0
        )
    assert result.audit.state is State.DEGRADED
    with generated_provider_scope(request):
        assert (
            read_legacy_host_health(
                tmp_path, result.manifest, provider_request=request
            )
            == result.audit
        )


def test_malformed_actual_ingress_is_retained_without_raw_payload(tmp_path):
    canary = "synthetic-malformed-raw-body-never-retained"
    with pytest.raises(ValueError, match="malformed"):
        capture(tmp_path, messages=(canary,))
    directories = tuple(tmp_path.glob("*-host-health"))
    assert len(directories) == 1
    observations = tuple(_observations(directories[0], 100))
    assert [item.kind for item in observations] == [
        Kind.QUEUE,
        Kind.INGRESS,
        Kind.REFUSED,
    ]
    assert observations[1].event_id is None
    assert not (directories[0] / "audit.json").exists()
    assert all(
        canary not in path.read_text() for path in directories[0].iterdir()
    )


def test_persistence_slo_measures_actual_host_residence(tmp_path):
    request, result = capture(
        tmp_path, policy=BrokerHostHealthPolicyV1(max_persistence_p95_ns=1)
    )
    assert Failure.PERSISTENCE_LAG in result.audit.failures
    with (
        generated_provider_scope(request),
        pytest.raises(BrokerHostHealthAdmissionError),
    ):
        require_legacy_capture_health(
            tmp_path, result.manifest, provider_request=request
        )
