"""Pure generated terminal predicates; no worker, provider, or capture IO.

The public-verifier wiring tests stub only the expensive native IO readers.
Exact evidence, authority, native, health, and permission contracts still cross
the real verifier boundary. These stubs are not native execution qualification.
"""

import hashlib
from dataclasses import replace
from zipfile import ZipFile

import pytest

from histdatacom.broker_plugin_capabilities import (
    BrokerCapabilityWorkflowV1,
    negotiate_broker_capabilities,
    validate_broker_capability_event,
)
from histdatacom.broker_plugin_conformance import (
    BrokerConformancePlanV1,
    BrokerConformanceProfile,
    BrokerConformanceReason,
    BrokerConformanceStatus,
    broker_conformance_catalog,
    build_broker_conformance_fixture,
    verification,
)
from histdatacom.broker_plugin_conformance.authority import generated_authority
from histdatacom.broker_plugin_conformance.evidence import (
    BrokerConformanceEvidenceV1,
)
from histdatacom.broker_plugin_conformance.fixture_plugin import (
    ConformanceFixture,
)
from histdatacom.broker_plugin_health import (
    BrokerHostHealthAuditV1,
    BrokerHostHealthHeaderV1,
    BrokerHostHealthNativeFamily,
    BrokerHostHealthPolicyV1,
    BrokerHostHealthState,
)
from histdatacom.broker_plugin_lifecycle import (
    BrokerLifecycleCompletion as Completion,
)
from histdatacom.broker_plugin_lifecycle import (
    BrokerLifecycleHeaderV1,
    BrokerLifecycleManifestV1,
    BrokerLifecyclePartitionV1,
    BrokerLifecyclePolicyV1,
    BrokerLifecycleRecordV1,
    BrokerLifecycleSessionV1,
)
from histdatacom.broker_plugin_lifecycle import (
    BrokerLifecycleReason as Reason,
)
from histdatacom.broker_plugin_lifecycle import (
    BrokerLifecycleState as State,
)
from histdatacom.broker_plugin_lifecycle import (
    BrokerLifecycleTransitionV1 as Transition,
)
from histdatacom.broker_plugin_permissions import (
    BrokerPermissionManifestV1,
    build_permission_execution,
    permission_resource_path,
)
from histdatacom.broker_plugin_policy import (
    BrokerProviderConfigurationV1,
    BrokerProviderOutputContractV1,
    BrokerSDKInvocationV1,
)
from histdatacom.broker_plugin_provenance import (
    BrokerProvenanceNativeFamily,
    BrokerProvenanceVerificationReason,
    verify_provenance_chain,
)
from histdatacom.broker_plugin_registry import BrokerPluginInventoryV1
from histdatacom.broker_plugins import BrokerEventKind as Kind
from tests.fixtures.broker_provider_policy import sdk_policy_invocation
from tests.unit.test_broker_provenance_core import (
    generated_chain,
    generated_header,
    verified,
)


@pytest.fixture(scope="module")
def generated_native_header():
    plan = sdk_policy_invocation().plan
    return BrokerLifecycleHeaderV1(
        BrokerPluginInventoryV1((plan.candidate,)),
        plan,
        BrokerLifecyclePolicyV1(),
        ("EURUSD",),
        "a" * 32,
        "3.10.19",
    )


def _outcome(header, case="quotes.symbols"):
    transitions = [
        Transition(State.DISCOVERED, State.CONFIGURED, Reason.CONFIGURED, 0),
        Transition(State.CONFIGURED, State.STARTING, Reason.STARTING, 0),
        Transition(State.STARTING, State.ACTIVE, Reason.ACTIVE, 0),
    ]
    events = ((0, Kind.QUOTE),) * 3
    sessions, forced = 1, 0
    previous, stop, terminal, end = (
        State.ACTIVE,
        Reason.EOF,
        Reason.CLOSED,
        State.STOPPED,
    )
    partial = case in {
        "gaps.delivery",
        "health.false-gap",
        "lifecycle.cancellation",
        "lifecycle.forced-shutdown",
        "lifecycle.reconnect",
        "queue.overflow",
    }
    if case in {"gaps.delivery", "health.false-gap"}:
        transitions.append(
            Transition(State.ACTIVE, State.DEGRADED, Reason.GAP, 0)
        )
        previous = State.DEGRADED
        events = ((0, Kind.GAP),) + events
    elif case == "lifecycle.reconnect":
        header = replace(
            header, policy=replace(header.policy, retry_delays_ms=(0,))
        )
        transitions.extend(
            (
                Transition(State.ACTIVE, State.DEGRADED, Reason.RECONNECT, 0),
                Transition(State.DEGRADED, State.RECONNECTING, Reason.RETRY, 0),
                Transition(
                    State.RECONNECTING, State.STARTING, Reason.STARTING, 1
                ),
                Transition(State.STARTING, State.ACTIVE, Reason.ACTIVE, 1),
                Transition(State.ACTIVE, State.DEGRADED, Reason.RECONNECT, 1),
            )
        )
        previous, stop, terminal, end = (
            State.DEGRADED,
            Reason.RETRY_EXHAUSTED,
            Reason.FORCED,
            State.FAILED,
        )
        sessions, forced = 2, 1
        events = tuple(
            (epoch, kind)
            for epoch in (0, 1)
            for kind in (Kind.QUOTE,) * 3 + (Kind.DISCONNECTED,)
        )
    elif case in {
        "lifecycle.cancellation",
        "lifecycle.forced-shutdown",
        "queue.overflow",
    }:
        stop = {
            "lifecycle.cancellation": Reason.CANCELLED,
            "lifecycle.forced-shutdown": Reason.SHUTDOWN_TIMEOUT,
            "queue.overflow": Reason.QUEUE_SATURATED,
        }[case]
        terminal, end, forced = Reason.FORCED, State.FAILED, 1
        if case == "lifecycle.cancellation":
            events = ()
        elif case == "queue.overflow":
            events, terminal, forced = events[:1], Reason.QUEUE_SATURATED, 0
    epoch = sessions - 1
    transitions.extend(
        (
            Transition(previous, State.STOPPING, stop, epoch),
            Transition(State.STOPPING, end, terminal, epoch),
        )
    )
    count = len(transitions) + len(events) + sessions
    partition = BrokerLifecyclePartitionV1(
        0, "0" * 64, 1, 0, count, len(events)
    )
    native = BrokerLifecycleManifestV1(
        header,
        end,
        Completion.PARTIAL if partial else Completion.COMPLETE,
        partitions=() if partial else (partition,),
        partial_partition=partition if partial else None,
        appended_records=count,
        appended_events=len(events),
        received_events=len(events),
        unknown_loss=partial,
        worker_reaped=True,
        forced_terminations=forced,
    )
    return native, tuple(transitions), events, sessions


def _accept(case, outcome, **kwargs):
    return verification._native_case_outcome(
        case,
        *outcome,
        pinned=kwargs.get("pinned", True),
        complete_chain=kwargs.get("complete_chain", True),
        refused=kwargs.get("refused", False),
    )


@pytest.mark.parametrize(
    "case",
    (
        "quotes.symbols",
        "quotes.decimals",
        "timestamps.source",
        "timestamps.receive",
        "heartbeat.delivery",
        "sizes.quoted",
        "raw.hashes",
        "health.honest",
    ),
)
def test_ordinary_positive_needs_complete_terminal(
    generated_native_header, case
):
    outcome = _outcome(generated_native_header, case)
    assert _accept(case, outcome)
    native, transitions, events, sessions = outcome
    for stop in (
        Reason.PLUGIN_FAILURE,
        Reason.RUN_TIMEOUT,
        Reason.STARTUP_TIMEOUT,
    ):
        changed = replace(
            native,
            state=State.FAILED,
            completion=Completion.PARTIAL,
            unknown_loss=True,
        )
        tail = (
            Transition(State.ACTIVE, State.STOPPING, stop, 0),
            Transition(State.STOPPING, State.FAILED, stop, 0),
        )
        assert not _accept(
            case, (changed, transitions[:-2] + tail, events, sessions)
        )
    assert not _accept(case, outcome, complete_chain=False)


@pytest.mark.parametrize(
    "case",
    (
        "gaps.delivery",
        "health.false-gap",
        "lifecycle.cancellation",
        "lifecycle.forced-shutdown",
        "lifecycle.reconnect",
        "queue.overflow",
    ),
)
def test_intended_partial_requires_pinned_reaped_expected_terminal(
    generated_native_header, case
):
    native, transitions, events, sessions = outcome = _outcome(
        generated_native_header, case
    )
    assert native.completion is Completion.PARTIAL
    assert _accept(case, outcome, complete_chain=False)
    assert not _accept(case, outcome, pinned=False)
    assert not _accept(case, outcome, refused=True)
    assert not _accept(
        case,
        (replace(native, worker_reaped=False), transitions, events, sessions),
    )
    for stop in (
        Reason.PLUGIN_FAILURE,
        Reason.RUN_TIMEOUT,
        Reason.STARTUP_TIMEOUT,
    ):
        tail = (
            replace(transitions[-2], reason=stop),
            replace(transitions[-1], current=State.FAILED, reason=stop),
        )
        assert not _accept(
            case,
            (
                replace(native, state=State.FAILED),
                transitions[:-2] + tail,
                events,
                sessions,
            ),
        )


def test_partial_cleanup_and_retry_budget_are_not_interchangeable(
    generated_native_header,
):
    native, transitions, events, sessions = _outcome(
        generated_native_header, "lifecycle.cancellation"
    )
    graceful = replace(native, state=State.STOPPED, forced_terminations=0)
    closed = replace(
        transitions[-1], current=State.STOPPED, reason=Reason.CLOSED
    )
    assert _accept(
        "lifecycle.cancellation",
        (graceful, transitions[:-1] + (closed,), events, sessions),
        complete_chain=False,
    )
    native, transitions, events, sessions = _outcome(
        generated_native_header, "lifecycle.forced-shutdown"
    )
    assert not _accept(
        "lifecycle.forced-shutdown",
        (replace(native, forced_terminations=0), transitions, events, sessions),
    )
    native, transitions, events, sessions = _outcome(
        generated_native_header, "lifecycle.reconnect"
    )
    assert not _accept(
        "lifecycle.reconnect", (native, transitions, events[:-1], sessions)
    )
    assert not _accept("lifecycle.reconnect", (native, transitions, events, 1))
    assert not _accept(
        "quotes.symbols", (native, transitions, events, sessions)
    )


@pytest.mark.parametrize("partial", (False, True))
def test_pinned_chain_accepts_only_closed_exact_replay(partial):
    chain = generated_chain(
        terminal_changes={
            "native_complete": not partial,
            "observations_complete": not partial,
        }
    )
    header, links, checkpoints, seal = chain
    result = verified(chain)
    assert verification._pinned_native_replay(result, seal)
    assert result.complete is (not partial)
    for changed in (
        replace(result, root_sha256="0" * 64),
        replace(result, entry_count=result.entry_count - 1),
        replace(result, checkpoint_count=result.checkpoint_count - 1),
        replace(result, reason=BrokerProvenanceVerificationReason.UNANCHORED),
        replace(result, header=replace(header, plugin_id="substituted")),
    ):
        assert not verification._pinned_native_replay(changed, seal)
    truncated = verify_provenance_chain(
        header,
        links[:-1],
        checkpoints[:-1],
        None,
        expected_header_id=header.artifact_id,
        expected_seal_id=seal.artifact_id,
    )
    assert not verification._pinned_native_replay(truncated, seal)


@pytest.fixture(scope="module")
def verifier_fixture(tmp_path_factory):
    fixture = build_broker_conformance_fixture(
        tmp_path_factory.mktemp("terminal-fixture")
    )
    inventory = BrokerPluginInventoryV1((fixture.candidate,))
    registration = fixture.candidate.registration
    with ZipFile(fixture.wheel) as archive:
        manifest = BrokerPermissionManifestV1.from_json(
            archive.read(
                permission_resource_path(
                    registration.plugin_id, registration.entry_point
                )
            ).decode("ascii")
        )
    catalog = broker_conformance_catalog()
    plan = BrokerConformancePlanV1(
        fixture.driver,
        catalog.artifact_id,
        BrokerConformanceProfile.ISOLATED,
        tuple(
            sorted(
                {case.capability for case in catalog.cases}
                - {"common", "host-adversarial.v1"}
            )
        ),
        tuple(
            sorted(
                case.case_id
                for case in catalog.cases
                if "isolated_contract_v1" in case.profiles
                and case.execution_backend != "reference_host_fault"
            )
        ),
    )
    negotiated = negotiate_broker_capabilities(
        inventory,
        BrokerCapabilityWorkflowV1(
            tuple(
                sorted(
                    (
                        "configuration_schema",
                        "open_session",
                        "instruments",
                        "subscribe",
                        "iter_events",
                        "unsubscribe",
                        "close_session",
                    )
                )
            ),
            optional=tuple(
                sorted(
                    set(registration.capabilities)
                    - {"session.v1", "events.v1", "instruments.v1"}
                )
            ),
        ),
        plugin_id=registration.plugin_id,
    )
    return plan, inventory, manifest, negotiated


def _verifier_case(verifier_fixture, tmp_path, monkeypatch, case, failure):
    plan, inventory, permissions, negotiated = verifier_fixture
    definition = next(
        item
        for item in broker_conformance_catalog().cases
        if item.case_id == case
    )
    scenario = next(
        item
        for item in plan.driver.scenarios
        if item.scenario_id == definition.scenario_id
    )
    request = BrokerSDKInvocationV1(
        negotiated,
        BrokerProviderConfigurationV1(
            plan.driver.provider_id,
            "generated-conformance",
            plan.driver.configuration_schema_json,
            scenario.configuration_json,
        ),
        BrokerProviderOutputContractV1(
            "sdk-v1",
            tuple(sorted(kind.value for kind in Kind)),
            allow_raw_hashes=True,
            allow_opaque_metadata=True,
        ),
    )
    authority = generated_authority(request, permissions)
    header = BrokerLifecycleHeaderV1(
        inventory,
        negotiated,
        BrokerLifecyclePolicyV1(),
        ("EURUSD",),
        "a" * 32,
        "3.10.19",
    )
    native, transitions, _, sessions = _outcome(header, case)
    if failure == "plugin_failure":
        native = replace(
            native,
            state=State.FAILED,
            completion=Completion.PARTIAL,
            unknown_loss=True,
        )
        transitions = transitions[:-2] + (
            Transition(State.ACTIVE, State.STOPPING, Reason.PLUGIN_FAILURE, 0),
            Transition(State.STOPPING, State.FAILED, Reason.PLUGIN_FAILURE, 0),
        )
    elif failure == "unreaped":
        native = replace(native, worker_reaped=False)
    plugin = ConformanceFixture(authority.resources)
    session = plugin.open_session({"mode": definition.scenario_id})
    # Pure generated event vectors: no close() of close-block and no sleeping modes.
    events = tuple(
        validate_broker_capability_event(negotiated, event)
        for event in plugin.iter_events(session)
    )
    if failure == "plugin_failure":
        events = tuple(
            event for event in events if event.event.kind is Kind.QUOTE
        )[:1]
    records = []

    def record(kind, payload, epoch=0, delivery=None):
        sequence = len(records)
        records.append(
            BrokerLifecycleRecordV1(
                native.header.artifact_id,
                sequence,
                epoch,
                100 + sequence,
                200 + sequence,
                kind,
                payload.to_json(),
                delivery,
            )
        )

    for transition in transitions:
        record("transition", transition, transition.epoch)
    for epoch in range(sessions):
        # Generated reader stubs still model distinct physical openings and
        # bind their events correctly. Only these named negative controls
        # deliberately reuse the public nonce, even with different timestamps.
        epoch_session = replace(
            session,
            instance_nonce=(
                "a" * 32
                if failure.startswith("reused_session")
                else f"{epoch + 1:032x}"
            ),
            opened_at_utc_ns=(
                100 + epoch if failure == "reused_session_changed_time" else 100
            ),
        )
        record("session", BrokerLifecycleSessionV1(epoch_session, ()), epoch)
        for index, event in enumerate(events):
            rebound = validate_broker_capability_event(
                negotiated,
                replace(event.event, session_id=epoch_session.artifact_id),
            )
            record("event", rebound, epoch, index)
    partition_bytes = b"".join(
        (record.to_json() + "\n").encode("ascii") for record in records
    )
    partition = BrokerLifecyclePartitionV1(
        0,
        hashlib.sha256(partition_bytes).hexdigest(),
        len(partition_bytes),
        0,
        len(records),
        len(events) * sessions,
    )
    native = replace(
        native,
        partitions=(
            (partition,) if native.completion is Completion.COMPLETE else ()
        ),
        partial_partition=(
            None if native.completion is Completion.COMPLETE else partition
        ),
        appended_records=len(records),
        appended_events=len(events) * sessions,
        received_events=len(events) * sessions,
    )
    proof = build_permission_execution(request, native, authority.authority)
    audit = BrokerHostHealthAuditV1(
        BrokerHostHealthHeaderV1(
            BrokerHostHealthNativeFamily.LIFECYCLE_V1,
            native.header.artifact_id,
            plan.driver.candidate_id,
            plan.driver.provider_id,
            request.configuration_profile.artifact_id,
            permissions.artifact_id,
            proof.context.artifact_id,
            proof.decision.artifact_id,
            "generated-provider-decision",
            ("EURUSD",),
            100,
            200,
            2,
            BrokerHostHealthPolicyV1(),
        ),
        native.artifact_id,
        "1" * 64,
        "2" * 64,
        0,
        len(records),
        True,
        BrokerHostHealthState.QUALIFIED,
        (),
        (),
    )
    registration = negotiated.candidate.registration

    def digest(value):
        return hashlib.sha256(value.to_json().encode("ascii")).hexdigest()

    provenance_header = generated_header(
        family=BrokerProvenanceNativeFamily.LIFECYCLE_V1,
        capture_id=native.header.artifact_id,
        native_header_id=native.header.artifact_id,
        native_header_sha256=digest(native.header),
        plugin_id=registration.plugin_id,
        distribution_name=registration.distribution_name,
        distribution_version=registration.distribution_version,
        implementation_sha256=negotiated.candidate.implementation_sha256,
        registration_sha256=negotiated.candidate.registration_sha256,
        sdk_version=inventory.sdk_version,
        permission_manifest_id=permissions.artifact_id,
        permission_context_id=proof.context.artifact_id,
        permission_decision_id=proof.decision.artifact_id,
        permission_grant_id=proof.decision.grant_id,
    )
    complete = native.completion is Completion.COMPLETE
    chain = generated_chain(
        provenance_header,
        {
            "native_manifest_id": native.artifact_id,
            "native_manifest_sha256": digest(native),
            "health_audit_id": audit.artifact_id,
            "health_audit_sha256": digest(audit),
            "permission_execution_id": proof.artifact_id,
            "permission_execution_sha256": digest(proof),
            "native_complete": complete,
            "observations_complete": complete,
        },
    )
    seal = chain[-1]

    def read_native(directory, manifest, *, provider_request, expected_root):
        assert directory == tmp_path / "native"
        assert (
            manifest == native
            and provider_request == request
            and expected_root == seal
        )
        result = verified(chain)
        if failure == "unanchored":
            return replace(
                result, reason=BrokerProvenanceVerificationReason.UNANCHORED
            )
        if failure == "truncated":
            return replace(
                result,
                seal=None,
                reason=BrokerProvenanceVerificationReason.PARTIAL,
                entry_count=result.entry_count - 1,
            )
        return result

    monkeypatch.setattr(
        verification,
        "replay_broker_lifecycle",
        lambda *args, **kwargs: iter(records),
    )
    monkeypatch.setattr(
        verification, "read_lifecycle_capture_provenance", read_native
    )
    evidence = BrokerConformanceEvidenceV1(
        plan.artifact_id,
        case,
        plan.driver.candidate_id,
        1,
        2,
        inventory.to_json(),
        request.to_json(),
        permissions.to_json(),
        proof.context.to_json(),
        authority.policy.context.to_json(),
        "isolated",
        native.to_json(),
        proof.to_json(),
        audit.to_json(),
        seal.to_json(),
        capability_plan_json=negotiated.to_json(),
    )
    return plan, evidence, tuple(records), native, seal, read_native


@pytest.mark.parametrize(
    "case,failure,expected",
    (
        ("quotes.symbols", "none", True),
        ("quotes.symbols", "plugin_failure", False),
        ("quotes.symbols", "unanchored", False),
        ("quotes.symbols", "truncated", False),
        ("lifecycle.forced-shutdown", "none", True),
        ("lifecycle.forced-shutdown", "unreaped", False),
        ("gaps.delivery", "none", True),
        ("lifecycle.reconnect", "none", True),
        ("lifecycle.reconnect", "reused_session", False),
        ("lifecycle.reconnect", "reused_session_changed_time", False),
    ),
)
def test_public_verifier_wires_native_terminal_gate(
    verifier_fixture, tmp_path, monkeypatch, case, failure, expected
):
    plan, evidence, *_ = _verifier_case(
        verifier_fixture, tmp_path, monkeypatch, case, failure
    )
    result = verification.verify_conformance_evidence(plan, evidence, tmp_path)
    assert (result.status is BrokerConformanceStatus.PASS) is expected
    if failure in {"reused_session", "reused_session_changed_time"}:
        assert result.status is BrokerConformanceStatus.FAIL
        assert result.reason is BrokerConformanceReason.CONTRACT
        assert result.execution_backend == "isolated"
