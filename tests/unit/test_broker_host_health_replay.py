"""Generated native capture fixtures and independent observation canaries."""

import hashlib
from collections import Counter
from dataclasses import replace

import pytest

from histdatacom.broker_capture.contracts import (
    BROKER_CAPTURE_DATA_ARTIFACT_KIND,
    BrokerCaptureEventV1,
    BrokerCapturePartitionManifestV1,
    BrokerCaptureSessionManifestV1,
    BrokerCaptureSessionState,
)
from histdatacom.broker_plugin_health import (
    BrokerHostHealthAuditV1,
    BrokerHostHealthPolicyV1,
    make_legacy_health_header,
    replay_legacy_host_health,
)
from histdatacom.broker_plugin_health import (
    BrokerHostHealthFailure as Failure,
)
from histdatacom.broker_plugin_health import (
    BrokerHostHealthObservationKind as Kind,
)
from histdatacom.broker_plugin_health import (
    BrokerHostHealthObservationV1 as Observation,
)
from histdatacom.broker_plugin_health import (
    BrokerHostHealthReason as Reason,
)
from histdatacom.broker_plugin_health import (
    BrokerHostHealthState as State,
)
from histdatacom.broker_plugin_policy.bindings import resolve_provider_subject
from histdatacom.broker_plugin_policy.contracts import (
    BrokerPolicyOperation,
    BrokerPolicyRequestV1,
)
from histdatacom.broker_plugin_policy.decisions import decide_provider_operation
from histdatacom.runtime_contracts import ArtifactRef
from tests.fixtures.broker_provider_policy import (
    generated_legacy_request,
    legacy_policy_inputs,
    policy_context,
)


def legacy_fixture(
    *, messages=None, policy=None, delay=100, clock_correction_ns=None
):
    inputs = legacy_policy_inputs()
    request = generated_legacy_request(inputs.session)
    subject = resolve_provider_subject(request)
    decision = decide_provider_operation(
        policy_context(subject.bindings[0]),
        BrokerPolicyRequestV1(subject, BrokerPolicyOperation.CAPTURE, 10),
    )
    if messages is None:
        messages = inputs.messages
    events = tuple(
        BrokerCaptureEventV1(
            inputs.session.session_id,
            index,
            inputs.session.started_at_utc_ns + (index + 1) * 100_000_000,
            inputs.session.started_at_monotonic_ns + (index + 1) * 100_000_000,
            message,
            clock_offset_change_ns=(
                clock_correction_ns
                if message.kind.value == "clock_correction"
                else None
            ),
        )
        for index, message in enumerate(messages)
    )
    payload = b"".join(event.to_json().encode() + b"\n" for event in events)
    kinds = dict(Counter(event.kind.value for event in events))
    partition = BrokerCapturePartitionManifestV1(
        inputs.session.session_id,
        inputs.storage_policy.policy_id,
        0,
        ArtifactRef(
            BROKER_CAPTURE_DATA_ARTIFACT_KIND,
            "partition-000000.jsonl",
            len(payload),
            hashlib.sha256(payload).hexdigest(),
        ),
        len(events),
        0,
        len(events) - 1,
        events[0].receive_time_utc_ns,
        events[-1].receive_time_utc_ns,
        events[0].receive_time_monotonic_ns,
        events[-1].receive_time_monotonic_ns,
        kinds,
    )
    manifest = BrokerCaptureSessionManifestV1(
        inputs.session,
        inputs.storage_policy,
        BrokerCaptureSessionState.COMPLETED,
        (partition,),
        len(events),
        kinds,
        0,
        len(events) - 1,
    )
    header = make_legacy_health_header(
        inputs.session,
        decision,
        provider_request=request,
        symbols=("EURUSD",),
        queue_capacity=32,
        policy=policy or BrokerHostHealthPolicyV1(),
    )
    observations = [
        Observation(
            0,
            Kind.QUEUE,
            0,
            header.started_at_utc_ns,
            header.started_at_monotonic_ns,
            queue_items=0,
        )
    ]
    ingress = 0
    epoch = 0
    for event in events:
        epoch += event.kind.value in ("reconnect", "process_restart")
        control = event.kind.value == "clock_correction"
        if not control:
            observations.append(
                Observation(
                    len(observations),
                    Kind.INGRESS,
                    epoch,
                    event.receive_time_utc_ns,
                    event.receive_time_monotonic_ns,
                    ingress_sequence=ingress,
                    event_id=event.message.message_id,
                )
            )
        observations.append(
            Observation(
                len(observations),
                Kind.PERSISTED,
                epoch,
                event.receive_time_utc_ns + delay,
                event.receive_time_monotonic_ns + delay,
                ingress_sequence=None if control else ingress,
                event_id=None if control else event.message.message_id,
                native_record_id=event.event_id,
            )
        )
        ingress += not control
    observations.append(
        Observation(
            len(observations),
            Kind.CLOSE,
            epoch,
            events[-1].receive_time_utc_ns + delay + 1,
            events[-1].receive_time_monotonic_ns + delay + 1,
        )
    )
    return header, observations, manifest, events, decision, request


def replay(fixture):
    header, observations, manifest, events, decision, request = fixture
    return replay_legacy_host_health(
        header,
        observations,
        manifest,
        events,
        decision,
        provider_request=request,
    )


def test_exact_complete_native_inventory_and_deterministic_audit():
    fixture = legacy_fixture()
    result = replay(fixture)
    assert result.state is State.QUALIFIED
    assert result.complete_observations
    assert sum(bucket.persisted for bucket in result.buckets) == len(fixture[3])
    assert all(bucket.upstream_loss_unknown for bucket in result.buckets)
    assert all(
        bucket.receive_to_persist.mean_ns in (None, 100.0)
        for bucket in result.buckets
    )
    assert BrokerHostHealthAuditV1.from_json(result.to_json()) == result
    assert result == replay(fixture)
    expected = hashlib.sha256(
        b"".join(item.to_json().encode() + b"\n" for item in fixture[1])
    ).hexdigest()
    assert result.observations_sha256 == expected


def test_hidden_upstream_loss_cannot_be_certified_known_zero():
    fixture = legacy_fixture(
        policy=BrokerHostHealthPolicyV1(require_known_upstream_loss=True)
    )
    result = replay(fixture)
    assert result.state is State.INSUFFICIENT
    assert Failure.UPSTREAM_UNKNOWN in result.failures
    assert all(bucket.upstream_loss_unknown for bucket in result.buckets)
    assert sum(bucket.known_host_dropped for bucket in result.buckets) == 0


def test_stationary_window_arithmetic_does_not_invent_full_last_bucket():
    fixture = legacy_fixture(
        policy=BrokerHostHealthPolicyV1(stationary_window_declared=True)
    )
    result = replay(fixture)
    for bucket in result.buckets:
        if bucket.bucket == 2 or not bucket.received:
            assert bucket.little_law_mean_in_system is None
        else:
            # Independently declared one-second window and 100 ns residence.
            assert bucket.little_law_mean_in_system == pytest.approx(
                bucket.received * 100 / 1_000_000_000
            )


def test_native_byte_budget_refuses_without_truncating_inventory(monkeypatch):
    import histdatacom.broker_plugin_health.replay as health_replay

    fixture = legacy_fixture()
    monkeypatch.setattr(health_replay, "MAX_NATIVE_HEALTH_BYTES", 1)
    with pytest.raises(ValueError, match="byte bound"):
        replay(fixture)


@pytest.mark.parametrize(
    ("kind", "ceiling", "failure"),
    [
        ("outage_start", "max_reported_source_gap_events", Failure.SOURCE_GAP),
        ("reconnect", "max_reconnect_events", Failure.SOURCE_GAP),
        ("clock_correction", "max_clock_correction_events", Failure.CLOCK_JUMP),
    ],
)
def test_explicit_source_condition_ceilings_default_zero_and_refuse_excess(
    kind, ceiling, failure
):
    from histdatacom.broker_capture import (
        BrokerAdapterMessageV1,
        BrokerCaptureEventKind,
    )

    message = BrokerAdapterMessageV1(
        BrokerCaptureEventKind(kind),
        reason_code=(
            "wall_monotonic_divergence"
            if kind == "clock_correction"
            else "generated_scenario"
        ),
        connection_id="new" if kind == "reconnect" else None,
    )
    quote = legacy_policy_inputs().messages[3]
    fixture = legacy_fixture(messages=(message, quote), clock_correction_ns=25)
    result = replay(fixture)
    assert failure in result.failures
    allowed = list(fixture)
    allowed[0] = replace(
        allowed[0], policy=BrokerHostHealthPolicyV1(**{ceiling: 1})
    )
    admitted = replay(allowed)
    assert failure not in admitted.failures
    assert all(bucket.upstream_loss_unknown for bucket in admitted.buckets)
    assert admitted.state is State.QUALIFIED
    excess = legacy_fixture(
        messages=(message, message, quote),
        clock_correction_ns=25,
        policy=BrokerHostHealthPolicyV1(**{ceiling: 1}),
    )
    # Reconnects define separate epochs, so each epoch's one event respects its
    # per-bucket ceiling; zero remains the explicit refusing threshold.
    if kind == "reconnect":
        excess = (
            replace(excess[0], policy=BrokerHostHealthPolicyV1()),
            *excess[1:],
        )
    assert failure in replay(excess).failures


def test_source_gap_ceiling_does_not_qualify_native_sdk_partial_or_lying_claim():
    from histdatacom.broker_plugin_health import replay_lifecycle_host_health

    values = list(sdk_fixture(source_order=(1, 2, 3), gap=True))
    values[0] = replace(
        values[0],
        policy=BrokerHostHealthPolicyV1(max_reported_source_gap_events=1),
    )
    result = replay_lifecycle_host_health(*values)
    assert result.state is State.INSUFFICIENT
    assert {Failure.INCOMPLETE, Failure.PLUGIN_CLAIM} <= set(result.failures)


def test_same_values_different_source_instant_are_unchanged_not_duplicate():
    message = legacy_policy_inputs().messages[3]
    newer = replace(
        message,
        source_event_time_ns=message.source_event_time_ns + 1,
        source_sequence=1,
        source_message_id="new",
        message_id="",
    )
    result = replay(legacy_fixture(messages=(message, newer)))
    assert sum(bucket.unchanged_quotes for bucket in result.buckets) == 1
    assert sum(bucket.exact_quote_duplicates for bucket in result.buckets) == 0


def test_source_time_reorder_detected_under_monotonic_host_order():
    messages = legacy_policy_inputs().messages[3:6]
    result = replay(
        legacy_fixture(messages=(messages[2], messages[0], messages[1]))
    )
    assert result.state is State.DEGRADED
    assert Failure.REORDER in result.failures
    assert sum(bucket.reordered_source_times for bucket in result.buckets) == 1


def test_repeated_source_identity_detected_and_stale_age_uses_host_clock():
    message = legacy_policy_inputs().messages[3]
    result = replay(
        legacy_fixture(
            messages=(message, message),
            policy=BrokerHostHealthPolicyV1(
                stale_after_ns=50_000_000, max_stale_rate=0.0
            ),
        )
    )
    assert sum(bucket.exact_quote_duplicates for bucket in result.buckets) == 1
    assert sum(bucket.stale_quotes for bucket in result.buckets) == 1
    assert {Failure.DUPLICATE, Failure.STALE} <= set(result.failures)


def test_real_host_persistence_delay_not_provider_sleep_controls_lag():
    result = replay(
        legacy_fixture(
            delay=10000,
            policy=BrokerHostHealthPolicyV1(max_persistence_p95_ns=9999),
        )
    )
    assert Failure.PERSISTENCE_LAG in result.failures
    assert all(
        bucket.receive_to_persist.p95_ns in (None, 10000)
        for bucket in result.buckets
    )


@pytest.mark.parametrize(
    "mutation",
    [
        "observation_drop",
        "observation_reorder",
        "record_drop",
        "record_reorder",
        "record_mutate",
        "duplicate_completion",
        "wrong_epoch",
        "wrong_event",
        "wrong_header",
        "wrong_policy",
        "after_close",
    ],
)
def test_native_observation_substitutions_refuse(mutation):
    header, observations, manifest, events, decision, request = legacy_fixture()
    if mutation == "observation_drop":
        observations.pop(2)
    elif mutation == "observation_reorder":
        observations[1], observations[2] = observations[2], observations[1]
    elif mutation == "record_drop":
        events = events[:-1]
    elif mutation == "record_reorder":
        events = (events[1], events[0], *events[2:])
    elif mutation == "record_mutate":
        events = (
            replace(
                events[0],
                receive_time_utc_ns=events[0].receive_time_utc_ns + 1,
                event_id="",
            ),
            *events[1:],
        )
    elif mutation == "duplicate_completion":
        observations[4] = replace(
            observations[2],
            sequence=4,
            utc_ns=observations[4].utc_ns,
            monotonic_ns=observations[4].monotonic_ns,
        )
    elif mutation == "wrong_epoch":
        observations[1] = replace(observations[1], epoch=1)
    elif mutation == "wrong_event":
        observations[2] = replace(
            observations[2], event_id="different-native-event"
        )
    elif mutation == "wrong_header":
        header = replace(header, provider_id="different-provider")
    elif mutation == "wrong_policy":
        decision = decide_provider_operation(
            decision.context,
            replace(
                decision.request, operation=BrokerPolicyOperation.MATERIAL_USE
            ),
        )
    else:
        observations.append(
            replace(observations[-1], sequence=len(observations))
        )
    with pytest.raises(ValueError):
        replay((header, observations, manifest, events, decision, request))


def test_missing_queue_and_terminal_evidence_remains_insufficient():
    header, observations, *rest = legacy_fixture()
    observations = [
        replace(item, sequence=index)
        for index, item in enumerate(observations[1:-1])
    ]
    result = replay((header, observations, *rest))
    assert result.state is State.INSUFFICIENT
    assert {Failure.INCOMPLETE, Failure.QUEUE_UNOBSERVED} <= set(
        result.failures
    )
    assert all(bucket.queue_maximum is None for bucket in result.buckets)


@pytest.mark.parametrize("allowance", [0, 10_000_000_000])
def test_explicit_queue_overflow_cannot_be_erased_by_empty_snapshot(allowance):
    fixture = list(
        legacy_fixture(
            policy=BrokerHostHealthPolicyV1(max_queue_saturation_ns=allowance)
        )
    )
    fixture[1][0] = replace(fixture[1][0], reason=Reason.QUEUE_OVERFLOW)
    result = replay(fixture)
    assert result.state is State.DEGRADED
    assert Failure.QUEUE_SATURATION in result.failures
    assert sum(bucket.known_host_dropped for bucket in result.buckets) == 0
    assert sum(bucket.queue_saturation_ns for bucket in result.buckets) == 0


@pytest.mark.parametrize(
    "mode",
    ["zero_default", "one_ns_under", "exact_allowance", "explicit_overflow"],
)
def test_full_queue_duration_uses_declared_slo_without_waiving_overflow(mode):
    fixture = list(
        legacy_fixture(messages=(legacy_policy_inputs().messages[3],))
    )
    duration = fixture[1][-1].monotonic_ns - fixture[1][0].monotonic_ns
    allowance = {"zero_default": 0, "one_ns_under": duration - 1}.get(
        mode, duration
    )
    fixture[0] = replace(
        fixture[0],
        policy=BrokerHostHealthPolicyV1(max_queue_saturation_ns=allowance),
    )
    fixture[1][0] = replace(
        fixture[1][0],
        queue_items=fixture[0].queue_capacity,
        reason=(
            Reason.QUEUE_OVERFLOW
            if mode == "explicit_overflow"
            else Reason.NONE
        ),
    )
    result = replay(fixture)
    assert (
        sum(bucket.queue_saturation_ns for bucket in result.buckets) == duration
    )
    if mode == "exact_allowance":
        assert result.state is State.QUALIFIED
        assert Failure.QUEUE_SATURATION not in result.failures
    else:
        assert result.state is State.DEGRADED
        assert Failure.QUEUE_SATURATION in result.failures


def test_empty_queue_overflow_disagrees_with_sdk_healthy_claim():
    from histdatacom.broker_plugin_health import replay_lifecycle_host_health

    values = list(sdk_fixture(source_order=(1, 2, 3)))
    values[0] = replace(
        values[0],
        policy=BrokerHostHealthPolicyV1(max_queue_saturation_ns=10_000_000_000),
    )
    values[1][0] = replace(values[1][0], reason=Reason.QUEUE_OVERFLOW)
    result = replay_lifecycle_host_health(*values)
    assert {Failure.QUEUE_SATURATION, Failure.PLUGIN_CLAIM} <= set(
        result.failures
    )
    assert (
        sum(bucket.healthy_claim_discrepancies for bucket in result.buckets)
        == 1
    )


def test_identified_overflow_refusal_remains_failure_with_duration_allowance():
    fixture = list(
        legacy_fixture(
            policy=BrokerHostHealthPolicyV1(
                max_queue_saturation_ns=10_000_000_000,
                max_known_drop_rate=1.0,
            )
        )
    )
    observations = fixture[1]
    close = observations.pop()
    ingress = len(fixture[3])
    observations.append(
        Observation(
            len(observations),
            Kind.INGRESS,
            0,
            close.utc_ns,
            close.monotonic_ns,
            ingress_sequence=ingress,
            event_id="generated-unpersisted-event",
        )
    )
    observations.append(
        Observation(
            len(observations),
            Kind.REFUSED,
            0,
            close.utc_ns + 1,
            close.monotonic_ns + 1,
            ingress_sequence=ingress,
            event_id="generated-unpersisted-event",
            reason=Reason.QUEUE_OVERFLOW,
        )
    )
    observations.append(
        replace(
            close,
            sequence=len(observations),
            utc_ns=close.utc_ns + 2,
            monotonic_ns=close.monotonic_ns + 2,
        )
    )
    result = replay(fixture)
    assert result.state is State.DEGRADED
    assert Failure.QUEUE_SATURATION in result.failures
    assert sum(bucket.known_host_dropped for bucket in result.buckets) == 1


def test_no_retrospective_manifest_only_observation_invention():
    header, _observations, *rest = legacy_fixture()
    with pytest.raises(ValueError, match="missing host observations"):
        replay((header, [], *rest))


def test_bounded_timing_samples_and_bucket_inventories_refuse_not_truncate():
    with pytest.raises(ValueError, match="sample inventory"):
        replay(
            legacy_fixture(
                policy=BrokerHostHealthPolicyV1(max_samples_per_bucket=1)
            )
        )
    with pytest.raises(ValueError, match="bucket inventory"):
        replay(legacy_fixture(policy=BrokerHostHealthPolicyV1(max_buckets=1)))


def test_historical_status_is_explicitly_unavailable_not_an_audit():
    from histdatacom.broker_plugin_health import historical_host_health_status

    fixture = legacy_fixture()
    status = historical_host_health_status(fixture[2])
    assert status.state is State.UNAVAILABLE
    assert status.native_manifest_id == fixture[2].manifest_id
    assert not hasattr(status, "buckets")
    with pytest.raises(ValueError):
        replace(status, state=State.QUALIFIED)


def test_host_and_source_clock_discontinuities_are_separate():
    message = legacy_policy_inputs().messages[3]
    shifted = replace(
        message,
        source_event_time_ns=message.source_event_time_ns + 10_000_000_000,
        message_id="",
    )
    result = replay(legacy_fixture(messages=(message, shifted)))
    assert sum(bucket.source_clock_jump_count for bucket in result.buckets) == 1
    assert sum(bucket.host_clock_jump_count for bucket in result.buckets) == 0
    header, observations, *rest = legacy_fixture()
    observations[-1] = replace(
        observations[-1], utc_ns=observations[-1].utc_ns + 2_000_000_000
    )
    result = replay((header, observations, *rest))
    assert sum(bucket.host_clock_jump_count for bucket in result.buckets) == 1
    assert Failure.CLOCK_JUMP in result.failures


def test_saturation_duration_spans_bucket_edges_without_losing_coverage():
    header, observations, *rest = legacy_fixture()
    observations[0] = replace(
        observations[0], queue_items=header.queue_capacity
    )
    result = replay((header, observations, *rest))
    expected = observations[-1].monotonic_ns - observations[0].monotonic_ns
    assert (
        sum(bucket.queue_saturation_ns for bucket in result.buckets) == expected
    )
    assert sum(bucket.queue_covered_ns for bucket in result.buckets) == expected
    assert sum(bucket.queue_saturation_events for bucket in result.buckets) == 1
    assert Failure.QUEUE_SATURATION in result.failures


@pytest.mark.parametrize("malformed", [False, True])
def test_unpersisted_refusal_keeps_unknown_symbol_and_honest_denominator(
    malformed,
):
    header, observations, *rest = legacy_fixture()
    close = observations.pop()
    ingress = len(rest[1])
    event_id = None if malformed else "generated-refused-native-event"
    observations.extend(
        (
            Observation(
                len(observations),
                Kind.INGRESS,
                0,
                close.utc_ns,
                close.monotonic_ns,
                ingress_sequence=ingress,
                event_id=event_id,
                reason=Reason.MALFORMED if malformed else Reason.NONE,
            ),
            Observation(
                len(observations) + 1,
                Kind.REFUSED,
                0,
                close.utc_ns + 1,
                close.monotonic_ns + 1,
                ingress_sequence=ingress,
                event_id=event_id,
                reason=Reason.MALFORMED if malformed else Reason.QUEUE_OVERFLOW,
            ),
        )
    )
    observations.append(
        replace(
            close,
            sequence=len(observations),
            utc_ns=close.utc_ns + 2,
            monotonic_ns=close.monotonic_ns + 2,
        )
    )
    result = replay((header, observations, *rest))
    assert sum(b.known_host_dropped for b in result.buckets) == (
        0 if malformed else 1
    )
    assert sum(b.malformed for b in result.buckets) == int(malformed)
    assert sum(b.refused for b in result.buckets) == 1
    assert all(b.symbol is None for b in result.buckets if b.refused)


def test_retry_duplicate_requires_prior_persisted_exact_event():
    header, observations, manifest, events, decision, request = legacy_fixture()
    close = observations.pop()
    retry = len(events)
    observations.append(
        Observation(
            len(observations),
            Kind.INGRESS,
            0,
            close.utc_ns,
            close.monotonic_ns,
            ingress_sequence=retry,
            event_id=events[0].message.message_id,
        )
    )
    observations.append(
        Observation(
            len(observations),
            Kind.REFUSED,
            0,
            close.utc_ns + 1,
            close.monotonic_ns + 1,
            ingress_sequence=retry,
            event_id=events[0].message.message_id,
            reason=Reason.DUPLICATE_DELIVERY,
        )
    )
    observations.append(
        replace(
            close,
            sequence=len(observations),
            utc_ns=close.utc_ns + 2,
            monotonic_ns=close.monotonic_ns + 2,
        )
    )
    result = replay((header, observations, manifest, events, decision, request))
    assert sum(b.delivery_retries for b in result.buckets) == 1
    assert sum(b.known_host_dropped for b in result.buckets) == 0
    observations[-3] = replace(observations[-3], event_id="nonexistent-event")
    observations[-2] = replace(observations[-2], event_id="nonexistent-event")
    with pytest.raises(ValueError, match="prior persisted"):
        replay((header, observations, manifest, events, decision, request))


def sdk_fixture(*, source_order=(3, 1, 2), gap=False, heartbeat=False):
    """Native canonical objects only; no provider or subprocess is invoked."""
    from histdatacom.broker_plugin_capabilities import (
        BrokerCapabilityWorkflowV1,
        BrokerInvocationAssociation,
        BrokerInvocationBindingV1,
        broker_capability_catalog,
        negotiate_broker_capabilities,
        validate_broker_capability_event,
        validate_broker_instrument,
        validate_broker_metadata,
    )
    from histdatacom.broker_plugin_health import make_lifecycle_health_header
    from histdatacom.broker_plugin_lifecycle.contracts import (
        BrokerLifecycleCompletion,
        BrokerLifecycleHeaderV1,
        BrokerLifecycleIdentityV1,
        BrokerLifecycleManifestV1,
        BrokerLifecyclePartitionV1,
        BrokerLifecyclePolicyV1,
        BrokerLifecycleRecordV1,
        BrokerLifecycleSessionV1,
        BrokerLifecycleTransitionV1,
    )
    from histdatacom.broker_plugin_lifecycle.contracts import (
        BrokerLifecycleReason as LifecycleReason,
    )
    from histdatacom.broker_plugin_lifecycle.contracts import (
        BrokerLifecycleState as LifecycleState,
    )
    from histdatacom.broker_plugin_permissions import (
        BrokerPermissionBindingV1,
        BrokerPermissionContextV1,
        BrokerPermissionGrantV1,
        BrokerPermissionManifestV1,
        decide_broker_permissions,
    )
    from histdatacom.broker_plugin_registry import (
        BrokerPluginCandidateV1,
        BrokerPluginInventoryV1,
    )
    from histdatacom.broker_plugins import (
        BrokerConfigurationSchemaV1,
        BrokerDiagnosticSeverity,
        BrokerDiagnosticV1,
        BrokerEventKind,
        BrokerEventV1,
        BrokerGapScope,
        BrokerGapV1,
        BrokerInstrumentV1,
        BrokerPluginMetadataV1,
        BrokerQuoteV1,
        BrokerReasonCode,
        BrokerSessionV1,
        BrokerSourceTimeSemantics,
        BrokerSourceTimeV1,
    )
    from tests.fixtures.broker_capability_wheel import capability_registration
    from tests.fixtures.broker_provider_policy import generated_sdk_request

    caps = tuple(
        sorted(
            item.capability_id
            for item in broker_capability_catalog().definitions
        )
    )
    registration = capability_registration(caps)
    candidate = BrokerPluginCandidateV1(registration, "a" * 64, "b" * 64)
    inventory = BrokerPluginInventoryV1((candidate,))
    operations = tuple(
        sorted(
            (
                "configuration_schema",
                "metadata",
                "open_session",
                "instruments",
                "subscribe",
                "iter_events",
                "unsubscribe",
                "close_session",
            )
        )
    )
    plan = negotiate_broker_capabilities(
        inventory,
        BrokerCapabilityWorkflowV1(operations, (), caps),
        plugin_id=registration.plugin_id,
    )
    invocation = generated_sdk_request(plan, provider_id="offline")
    subject = resolve_provider_subject(invocation)
    provider = decide_provider_operation(
        policy_context(subject.bindings[0]),
        BrokerPolicyRequestV1(subject, BrokerPolicyOperation.CAPTURE, 10),
    )
    pm = BrokerPermissionManifestV1(
        candidate.artifact_id,
        registration.distribution_name,
        registration.distribution_version,
        inventory.sdk_version,
        ("offline",),
        ("emit:health", "emit:quotes"),
        resource_abi="none",
    )
    binding = BrokerPermissionBindingV1(
        candidate.artifact_id,
        pm.artifact_id,
        inventory.sdk_version,
        "offline",
        invocation.configuration_profile.artifact_id,
    )
    grant = BrokerPermissionGrantV1(
        binding, pm.required_atoms, "generated-operator", 1, 1000000, "a" * 32
    )
    pc = BrokerPermissionContextV1((grant,), (), 0)
    pd = decide_broker_permissions(pm, binding, grant.artifact_id, pc, 10)
    native_header = BrokerLifecycleHeaderV1(
        inventory,
        plan,
        BrokerLifecyclePolicyV1(),
        ("EURUSD",),
        "a" * 32,
        "3.10.19",
    )
    header = make_lifecycle_health_header(
        native_header,
        invocation,
        pm,
        pc,
        pd,
        provider,
        started_at_utc_ns=10000,
        started_at_monotonic_ns=100,
    )
    metadata = BrokerPluginMetadataV1(
        registration.plugin_id,
        registration.plugin_version,
        registration.display_name,
    )
    identity = BrokerLifecycleIdentityV1(
        BrokerInvocationBindingV1(
            plan.artifact_id,
            candidate.artifact_id,
            metadata.artifact_id,
            BrokerInvocationAssociation.INSTALLED_ENTRYPOINT,
            candidate.implementation_sha256,
        ),
        validate_broker_metadata(plan, metadata),
        BrokerConfigurationSchemaV1(()),
    )
    session = BrokerSessionV1(
        metadata.artifact_id, "b" * 32, 10000, "generated-clock"
    )
    instrument = validate_broker_instrument(
        plan, BrokerInstrumentV1("EURUSD", "EUR/USD", "EUR", "USD", "0.00001")
    )
    records = []

    def add(kind, payload, delivery=None):
        records.append(
            BrokerLifecycleRecordV1(
                native_header.artifact_id,
                len(records),
                0,
                10000 + (len(records) + 1) * 10,
                100 + (len(records) + 1) * 10,
                kind,
                payload.to_json(),
                delivery,
            )
        )

    def transition(previous, current, reason):
        add(
            "transition",
            BrokerLifecycleTransitionV1(previous, current, reason, 0),
        )

    transition(
        LifecycleState.DISCOVERED,
        LifecycleState.CONFIGURED,
        LifecycleReason.CONFIGURED,
    )
    transition(
        LifecycleState.CONFIGURED,
        LifecycleState.STARTING,
        LifecycleReason.STARTING,
    )
    add("identity", identity)
    add("session", BrokerLifecycleSessionV1(session, (instrument,)))
    transition(
        LifecycleState.STARTING, LifecycleState.ACTIVE, LifecycleReason.ACTIVE
    )
    healthy = BrokerEventV1(
        session.artifact_id,
        0,
        BrokerEventKind.HEALTH,
        "connection",
        diagnostic=BrokerDiagnosticV1(
            BrokerReasonCode.HEALTHY,
            BrokerDiagnosticSeverity.INFO,
            "Generated healthy claim",
        ),
    )
    add("event", validate_broker_capability_event(plan, healthy), 0)
    for index, source in enumerate(source_order, 1):
        event = BrokerEventV1(
            session.artifact_id,
            index,
            BrokerEventKind.QUOTE,
            "connection",
            source_time=BrokerSourceTimeV1(
                10000 + source * 10, 1, BrokerSourceTimeSemantics.BROKER_EVENT
            ),
            instrument="EURUSD",
            quote=BrokerQuoteV1("EURUSD", "1.1", "1.2"),
        )
        add("event", validate_broker_capability_event(plan, event), index)
    count = 1 + len(source_order)
    if heartbeat:
        event = BrokerEventV1(
            session.artifact_id,
            count,
            BrokerEventKind.HEARTBEAT,
            "connection",
        )
        add("event", validate_broker_capability_event(plan, event), count)
        count += 1
    if gap:
        event = BrokerEventV1(
            session.artifact_id,
            count,
            BrokerEventKind.GAP,
            "connection",
            gap=BrokerGapV1(BrokerGapScope.SOURCE, 10000, 10001, 5),
            diagnostic=BrokerDiagnosticV1(
                BrokerReasonCode.SOURCE_GAP,
                BrokerDiagnosticSeverity.WARNING,
                "Generated source gap",
            ),
        )
        add("event", validate_broker_capability_event(plan, event), count)
        count += 1
    transition(
        LifecycleState.ACTIVE, LifecycleState.STOPPING, LifecycleReason.EOF
    )
    transition(
        LifecycleState.STOPPING, LifecycleState.STOPPED, LifecycleReason.CLOSED
    )
    payload = b"".join(record.to_json().encode() + b"\n" for record in records)
    partition = BrokerLifecyclePartitionV1(
        0,
        hashlib.sha256(payload).hexdigest(),
        len(payload),
        0,
        len(records),
        count,
    )
    manifest = BrokerLifecycleManifestV1(
        native_header,
        LifecycleState.STOPPED,
        (
            BrokerLifecycleCompletion.PARTIAL
            if gap
            else BrokerLifecycleCompletion.COMPLETE
        ),
        (partition,),
        appended_records=len(records),
        appended_events=count,
        received_events=count,
        unknown_loss=gap,
        worker_reaped=True,
    )
    observations = [Observation(0, Kind.QUEUE, 0, 10000, 100, queue_items=0)]
    ingress = 0
    for record in records:
        event_id = None
        if record.kind == "event":
            from histdatacom.broker_plugin_capabilities import (
                BrokerAdmittedEventV1,
            )

            event_id = BrokerAdmittedEventV1.from_json(
                record.payload_json
            ).artifact_id
            observations.append(
                Observation(
                    len(observations),
                    Kind.INGRESS,
                    0,
                    record.receive_utc_ns,
                    record.receive_monotonic_ns,
                    ingress_sequence=ingress,
                    event_id=event_id,
                )
            )
        observations.append(
            Observation(
                len(observations),
                Kind.PERSISTED,
                0,
                record.receive_utc_ns + 1,
                record.receive_monotonic_ns + 1,
                ingress_sequence=None if event_id is None else ingress,
                event_id=event_id,
                native_record_id=record.artifact_id,
            )
        )
        ingress += int(event_id is not None)
    observations.append(
        Observation(
            len(observations),
            Kind.CLOSE,
            0,
            records[-1].receive_utc_ns + 2,
            records[-1].receive_monotonic_ns + 2,
        )
    )
    return (
        header,
        observations,
        manifest,
        records,
        invocation,
        pm,
        pc,
        pd,
        provider,
    )


def test_sdk_healthy_claim_does_not_hide_independent_source_reorder():
    from histdatacom.broker_plugin_health import replay_lifecycle_host_health

    result = replay_lifecycle_host_health(*sdk_fixture())
    assert result.state is State.DEGRADED
    assert {Failure.REORDER, Failure.PLUGIN_CLAIM} <= set(result.failures)
    assert sum(b.healthy_claim_discrepancies for b in result.buckets) == 1
    assert sum(b.persisted for b in result.buckets) == 4
    assert result.native_record_count == 11


def test_sdk_clean_control_and_event_inventory_qualifies_host_boundary_only():
    from histdatacom.broker_plugin_health import replay_lifecycle_host_health

    result = replay_lifecycle_host_health(*sdk_fixture(source_order=(1, 2, 3)))
    assert result.state is State.QUALIFIED
    assert all(b.upstream_loss_unknown for b in result.buckets)


@pytest.mark.parametrize(
    "failure", [Failure.HEARTBEAT_GAP, Failure.QUEUE_SATURATION]
)
def test_sdk_healthy_claim_disagrees_with_observed_host_slo_breaches(failure):
    from histdatacom.broker_plugin_health import replay_lifecycle_host_health

    values = list(sdk_fixture(source_order=(1, 2, 3), heartbeat=True))
    if failure is Failure.HEARTBEAT_GAP:
        values[0] = replace(
            values[0], policy=BrokerHostHealthPolicyV1(max_heartbeat_gap_ns=25)
        )
    else:
        values[1][0] = replace(
            values[1][0], queue_items=values[0].queue_capacity
        )
    result = replay_lifecycle_host_health(*values)
    assert {failure, Failure.PLUGIN_CLAIM} <= set(result.failures)
    assert (
        sum(bucket.healthy_claim_discrepancies for bucket in result.buckets)
        == 1
    )


def test_plugin_source_gap_count_is_not_authoritative_host_dropped_count():
    from histdatacom.broker_plugin_health import replay_lifecycle_host_health

    result = replay_lifecycle_host_health(*sdk_fixture(gap=True))
    assert Failure.SOURCE_GAP in result.failures
    assert sum(b.known_host_dropped for b in result.buckets) == 0
    assert sum(b.gap_count for b in result.buckets) == 1
    assert sum(b.healthy_claim_discrepancies for b in result.buckets) == 1


@pytest.mark.parametrize(
    "mutation",
    [
        "config",
        "permission_decision",
        "permission_context",
        "permission_manifest",
        "provider",
        "control_omission",
        "claim_flag",
    ],
)
def test_sdk_authority_and_control_record_substitution_fail_closed(mutation):
    from histdatacom.broker_plugin_health import replay_lifecycle_host_health
    from histdatacom.broker_plugin_permissions import BrokerPermissionReason

    values = list(sdk_fixture())
    if mutation == "config":
        values[4] = replace(
            values[4],
            configuration_profile=replace(
                values[4].configuration_profile, profile_id="different"
            ),
        )
    elif mutation == "permission_decision":
        values[7] = replace(
            values[7],
            admitted=False,
            reason=BrokerPermissionReason.REQUIRED_DENIED,
            effective_atoms=(),
        )
    elif mutation == "permission_context":
        values[6] = replace(values[6], revision=1)
    elif mutation == "permission_manifest":
        values[5] = replace(values[5], distribution_version="9.0.0")
    elif mutation == "provider":
        values[8] = decide_provider_operation(
            values[8].context,
            replace(values[8].request, operation=BrokerPolicyOperation.INVOKE),
        )
    elif mutation == "control_omission":
        values[1] = [
            replace(x, sequence=i)
            for i, x in enumerate(values[1][:1] + values[1][2:])
        ]
    else:
        values[0] = replace(values[0], plugin_id="invented-plugin")
    with pytest.raises(ValueError):
        replay_lifecycle_host_health(*values)
