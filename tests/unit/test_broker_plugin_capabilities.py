"""Whole-workflow capability preflight and native SDK evidence semantics."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
import itertools
import json
from pathlib import Path
import runpy
import threading

import pytest

from histdatacom.broker_plugin_capabilities import (
    PUBLIC_CONTEXT_NAMESPACE,
    BrokerAdmittedEventV1,
    BrokerCapabilityError,
    BrokerCapabilityPlanV1,
    BrokerCapabilityWorkflowV1,
    BrokerFieldSupport,
    BrokerInvocationAssociation,
    GatedBrokerPluginV1,
    broker_capability_catalog,
    canonical_capability_json,
    invoke_authorized_broker_factory,
    negotiate_broker_capabilities,
    validate_broker_capability_event,
    validate_broker_instrument,
    validate_broker_metadata,
    verify_broker_admitted_event,
    verify_broker_capability_plan,
)
from histdatacom.broker_plugin_registry import (
    BrokerPluginCandidateV1,
    BrokerPluginInventoryV1,
)
from histdatacom.broker_plugins import (
    BrokerActivitySemantics,
    BrokerActivityV1,
    BrokerDiagnosticSeverity,
    BrokerDiagnosticV1,
    BrokerEventKind,
    BrokerEventV1,
    BrokerExtensionV1,
    BrokerGapScope,
    BrokerGapV1,
    BrokerInstrumentV1,
    BrokerPluginV1,
    BrokerQuoteV1,
    BrokerRawProvenanceV1,
    BrokerReasonCode,
    BrokerReceiveTimeV1,
    BrokerSessionV1,
    BrokerSizeSemantics,
    BrokerSizeV1,
    BrokerSourceTimeSemantics,
    BrokerSourceTimeV1,
)

ROOT = Path(__file__).resolve().parents[2]
WHEEL = runpy.run_path(str(ROOT / "tests/fixtures/broker_capability_wheel.py"))
EXTERNAL = runpy.run_path(
    str(ROOT / "tests/fixtures/broker_capability_external.py")
)
ALL = tuple(
    item.capability_id for item in broker_capability_catalog().definitions
)
OPERATIONS = tuple(
    sorted(
        (
            "metadata",
            "configuration_schema",
            "open_session",
            "instruments",
            "subscribe",
            "iter_events",
            "unsubscribe",
            "close_session",
        )
    )
)


def _inventory(
    caps: tuple[str, ...] = ALL, **changes: object
) -> BrokerPluginInventoryV1:
    registration = replace(
        WHEEL["capability_registration"](tuple(sorted(caps))), **changes
    )
    return BrokerPluginInventoryV1(
        (BrokerPluginCandidateV1(registration, "a" * 64, "b" * 64),)
    )


def _plan(
    caps: tuple[str, ...] = ALL,
    *,
    operations: tuple[str, ...] = OPERATIONS,
    required: tuple[str, ...] = (),
    optional: tuple[str, ...] = (),
) -> BrokerCapabilityPlanV1:
    return negotiate_broker_capabilities(
        _inventory(caps),
        BrokerCapabilityWorkflowV1(
            tuple(sorted(operations)),
            tuple(sorted(required)),
            tuple(sorted(optional)),
        ),
        plugin_id="org.example.capabilities",
    )


def _session() -> BrokerSessionV1:
    return BrokerSessionV1(
        EXTERNAL["factory"]().metadata.artifact_id,
        "a" * 32,
        100,
        "fixture-clock",
    )


def _event(**changes: object) -> BrokerEventV1:
    return replace(
        BrokerEventV1(
            _session().artifact_id,
            0,
            BrokerEventKind.QUOTE,
            "c-1",
            instrument="EURUSD",
            quote=BrokerQuoteV1("EURUSD", "1.1", "1.2"),
        ),
        **changes,
    )


def _invocation(
    plugin: object | None = None, plan: BrokerCapabilityPlanV1 | None = None
) -> GatedBrokerPluginV1:
    return invoke_authorized_broker_factory(
        _inventory(),
        plan or _plan(),
        authorize=lambda _: True,
        factory=lambda: plugin or EXTERNAL["factory"](),
    )


def test_catalog_exhaustively_covers_sdk_methods_and_event_kinds() -> None:
    catalog = broker_capability_catalog()
    protocol = {
        name
        for name, value in vars(BrokerPluginV1).items()
        if not name.startswith("_")
        and (callable(value) or isinstance(value, property))
    }
    assert {
        item.sdk_method for item in catalog.operations if item.executable
    } == protocol
    assert all(
        item.capability_id.endswith(".v1") for item in catalog.definitions
    )
    assert len({item.capability_id for item in catalog.definitions}) == len(
        catalog.definitions
    )
    assert type(catalog).from_json(catalog.to_json()) == catalog


def test_generated_optional_capability_monotonicity_and_required_removal() -> (
    None
):
    required = ("session.v1", "events.v1", "instruments.v1", "quotes.v1")
    optional = (
        "heartbeat.v1",
        "timestamps.receive.v1",
        "sizes.quoted.v1",
        "raw-hashes.v1",
    )
    for count in range(len(optional) + 1):
        for extras in itertools.combinations(optional, count):
            caps = tuple(sorted(required + extras))
            assert _plan(caps).admitted
            for removed in required:
                plan = _plan(tuple(item for item in caps if item != removed))
                assert not plan.admitted and removed in plan.missing_required
                with pytest.raises(
                    BrokerCapabilityError, match="^unsupported_capability$"
                ):
                    plan.require_admitted()


def test_provider_name_is_not_a_semantic_branch_and_unknown_optional_is_safe() -> (
    None
):
    workflow = BrokerCapabilityWorkflowV1(
        ("metadata",), optional=("vendor.future.v2",)
    )
    first = negotiate_broker_capabilities(
        _inventory(provider_ids=("one",)), workflow, provider_id="one"
    )
    other = negotiate_broker_capabilities(
        _inventory(provider_ids=("two",)), workflow, provider_id="two"
    )
    assert (
        first.admitted
        and other.admitted
        and first.enabled_optional == other.enabled_optional
    )
    assert "vendor.future.v2" not in first.enabled_optional
    assert not _plan(required=("vendor.future.v2",)).admitted


@pytest.mark.parametrize(
    "operation",
    [
        "history_replay",
        "history_backfill",
        "offline_capture_replay",
        "legacy_live_capture",
        "unknown",
    ],
)
def test_non_executable_operations_refuse_without_authorization_or_factory(
    operation: str,
) -> None:
    plan = _plan(operations=(operation,))
    calls: list[str] = []
    with pytest.raises(BrokerCapabilityError, match="unsupported_operation"):
        invoke_authorized_broker_factory(
            _inventory(),
            plan,
            authorize=lambda _: calls.append("authorize"),
            factory=lambda: calls.append("factory"),
        )
    assert calls == []


def test_missing_required_and_authorization_refusal_make_zero_provider_calls() -> (
    None
):
    calls: list[str] = []
    plan = _plan(caps=())
    with pytest.raises(BrokerCapabilityError, match="unsupported_capability"):
        invoke_authorized_broker_factory(
            _inventory(()),
            plan,
            authorize=lambda _: calls.append("authorize"),
            factory=lambda: calls.append("factory"),
        )
    assert calls == []
    for denied in (False, None, 1, "yes"):
        with pytest.raises(
            BrokerCapabilityError, match="authorization_required"
        ):
            invoke_authorized_broker_factory(
                _inventory(),
                _plan(),
                authorize=lambda _: denied,
                factory=lambda: calls.append("factory"),
            )
    assert calls == []


def test_public_construction_and_plan_binding_substitution_are_unavailable() -> (
    None
):
    with pytest.raises(BrokerCapabilityError, match="authorization_required"):
        GatedBrokerPluginV1(
            EXTERNAL["factory"](),
            _plan(),
            association=BrokerInvocationAssociation.INSTALLED_ENTRYPOINT,
            module_sha256="b" * 64,
        )
    invocation = _invocation()
    with pytest.raises(AttributeError):
        invocation.plan = _plan()
    with pytest.raises(AttributeError):
        invocation.binding = invocation.binding
    assert (
        invocation.binding.association
        is BrokerInvocationAssociation.CALLER_FACTORY
    )
    assert invocation.binding.module_sha256 is None


def test_frozen_wire_roundtrip_tamper_and_bounded_malformed_inputs() -> None:
    plan = _plan()
    assert BrokerCapabilityPlanV1.from_json(plan.to_json()) == plan
    verify_broker_capability_plan(plan, _inventory())
    with pytest.raises(FrozenInstanceError):
        plan.required = ()
    with pytest.raises(BrokerCapabilityError):
        replace(plan, missing_required=("quotes.v1",))
    with pytest.raises(BrokerCapabilityError):
        verify_broker_capability_plan(plan, _inventory(provider_ids=("other",)))
    for field, value in (
        ("artifact_id", "changed"),
        ("required", []),
        ("catalog_id", "broker-capability-catalog:sha256:" + "0" * 64),
    ):
        payload = plan.to_dict()
        payload[field] = value
        with pytest.raises(BrokerCapabilityError):
            BrokerCapabilityPlanV1.from_dict(payload)
    cycle: list[object] = []
    cycle.append(cycle)
    for value in (
        cycle,
        [object()] * 129,
        10**500,
        float("nan"),
        {"bad": object()},
    ):
        with pytest.raises(BrokerCapabilityError, match="invalid_artifact"):
            canonical_capability_json(value)
    for text in ('{"a":1,"a":2}', "[" * 2000, "x" * 524_289):
        with pytest.raises(BrokerCapabilityError, match="invalid_artifact"):
            BrokerCapabilityPlanV1.from_json(text)


def test_full_factory_workflow_is_executable_and_preserves_null_provenance() -> (
    None
):
    invocation = _invocation()
    assert invocation.configuration_schema.fields == ()
    session = invocation.open_session({})
    assert session.metadata_id == invocation.metadata.metadata.artifact_id
    assert invocation.instruments()[0].instrument.symbol == "EURUSD"
    invocation.subscribe(("EURUSD",))
    events = list(invocation.iter_events())
    assert len(events) == 1
    event = events[0]
    assert (
        event.event.source_time is None and event.event.quote.bid_size is None
    )
    assert {item.field: item.support for item in event.fields}[
        "source_time"
    ] is BrokerFieldSupport.NOT_REPORTED
    assert BrokerAdmittedEventV1.from_json(event.to_json()) == event
    verify_broker_admitted_event(event, invocation.plan)
    invocation.unsubscribe(("EURUSD",))
    invocation.close_session()
    invocation.close_session()
    with pytest.raises(BrokerCapabilityError, match="invalid_state"):
        invocation.open_session({})


def test_unsupported_and_not_requested_fields_remain_null_not_synthesized() -> (
    None
):
    caps = tuple(
        item
        for item in WHEEL["CAPABILITIES"]
        if item != "timestamps.receive.v1"
    )
    receipt = validate_broker_capability_event(_plan(caps), _event())
    assert (
        next(
            item for item in receipt.fields if item.field == "source_time"
        ).support
        is BrokerFieldSupport.UNSUPPORTED
    )
    metadata_plan = _plan(operations=("metadata",))
    metadata = EXTERNAL["factory"]().metadata
    assert (
        validate_broker_metadata(metadata_plan, metadata).metadata == metadata
    )
    instrument = BrokerInstrumentV1("EURUSD", "EUR/USD", "EUR", "USD")
    assert (
        validate_broker_instrument(
            _plan(), instrument
        ).instrument.price_increment
        is None
    )
    with pytest.raises(BrokerCapabilityError, match="capability_violation"):
        validate_broker_instrument(
            _plan(required=("instruments.price-increment.v1",)), instrument
        )


@pytest.mark.parametrize(
    "semantic,atom",
    [
        (BrokerSourceTimeSemantics.BROKER_EVENT, "timestamps.broker-event.v1"),
        (
            BrokerSourceTimeSemantics.EXCHANGE_EVENT,
            "timestamps.exchange-event.v1",
        ),
        (
            BrokerSourceTimeSemantics.ADAPTER_RECEIVE,
            "timestamps.adapter-receive.v1",
        ),
    ],
)
def test_source_timestamp_exact_semantics(
    semantic: BrokerSourceTimeSemantics, atom: str
) -> None:
    event = _event(source_time=BrokerSourceTimeV1(100, 10, semantic))
    assert (
        validate_broker_capability_event(_plan(required=(atom,)), event).event
        == event
    )
    for other in (
        "timestamps.broker-event.v1",
        "timestamps.exchange-event.v1",
        "timestamps.adapter-receive.v1",
    ):
        if other != atom:
            with pytest.raises(
                BrokerCapabilityError, match="capability_violation"
            ):
                validate_broker_capability_event(
                    _plan(required=(other,)), event
                )
    caps = tuple(item for item in ALL if item != atom)
    with pytest.raises(BrokerCapabilityError, match="capability_violation"):
        validate_broker_capability_event(_plan(caps), event)


@pytest.mark.parametrize(
    "first,second",
    [
        ("timestamps.broker-event.v1", "timestamps.adapter-receive.v1"),
        ("sizes.quoted.v1", "sizes.broker-specific.v1"),
        ("activity.message-count.v1", "activity.broker.v1"),
    ],
)
def test_conflicting_required_alternatives_refuse_at_preflight(
    first: str, second: str
) -> None:
    plan = _plan(required=(first, second))
    assert not plan.admitted and set(plan.unsupported_required) == {
        first,
        second,
    }


@pytest.mark.parametrize(
    "semantic,atom,other",
    [
        (
            BrokerSizeSemantics.QUOTED_SIZE,
            "sizes.quoted.v1",
            "sizes.broker-specific.v1",
        ),
        (
            BrokerSizeSemantics.BROKER_SPECIFIC,
            "sizes.broker-specific.v1",
            "sizes.quoted.v1",
        ),
    ],
)
def test_bid_and_ask_size_required_meanings_cannot_be_replaced(
    semantic: BrokerSizeSemantics, atom: str, other: str
) -> None:
    size = BrokerSizeV1("10", semantic, "EUR")
    event = _event(quote=BrokerQuoteV1("EURUSD", "1.1", "1.2", size, size))
    assert (
        validate_broker_capability_event(_plan(required=(atom,)), event).event
        == event
    )
    with pytest.raises(BrokerCapabilityError):
        validate_broker_capability_event(_plan(required=(other,)), event)
    with pytest.raises(BrokerCapabilityError):
        validate_broker_capability_event(
            _plan(required=(atom,)),
            replace(event, quote=replace(event.quote, ask_size=None)),
        )


@pytest.mark.parametrize(
    "semantic,atom",
    [
        (BrokerActivitySemantics.MESSAGE_COUNT, "activity.message-count.v1"),
        (BrokerActivitySemantics.BROKER_ACTIVITY, "activity.broker.v1"),
        (
            BrokerActivitySemantics.LIQUIDITY_PROXY,
            "activity.liquidity-proxy.v1",
        ),
    ],
)
def test_activity_meaning_never_becomes_volume(
    semantic: BrokerActivitySemantics, atom: str
) -> None:
    event = _event(
        quote=BrokerQuoteV1(
            "EURUSD",
            "1.1",
            "1.2",
            activity=BrokerActivityV1("2", semantic, "message"),
        )
    )
    admitted = validate_broker_capability_event(_plan(required=(atom,)), event)
    assert admitted.event.quote.activity.semantics is semantic
    assert "volume" not in admitted.to_dict()["event"]["quote"]
    wrong = (
        "activity.broker.v1"
        if atom != "activity.broker.v1"
        else "activity.message-count.v1"
    )
    with pytest.raises(BrokerCapabilityError):
        validate_broker_capability_event(_plan(required=(wrong,)), event)


@pytest.mark.parametrize("kind", list(BrokerEventKind))
def test_every_event_kind_is_gated_without_legacy_relabeling(
    kind: BrokerEventKind,
) -> None:
    codes = {
        BrokerEventKind.QUOTE: "quotes.v1",
        BrokerEventKind.CONNECTED: "connection.v1",
        BrokerEventKind.DISCONNECTED: "connection.v1",
        BrokerEventKind.RECONNECTING: "reconnect.v1",
        BrokerEventKind.SUBSCRIBED: "subscriptions.v1",
        BrokerEventKind.UNSUBSCRIBED: "subscriptions.v1",
        BrokerEventKind.HEARTBEAT: "heartbeat.v1",
        BrokerEventKind.HEALTH: "health.v1",
        BrokerEventKind.GAP: "gaps.v1",
        BrokerEventKind.CLOCK_CORRECTION: "clock-corrections.v1",
    }
    event = _event(
        kind=kind,
        quote=_event().quote if kind is BrokerEventKind.QUOTE else None,
        gap=(
            BrokerGapV1(BrokerGapScope.TRANSPORT, 10)
            if kind is BrokerEventKind.GAP
            else None
        ),
        diagnostic=BrokerDiagnosticV1(
            BrokerReasonCode.CLOCK_DISCONTINUITY,
            BrokerDiagnosticSeverity.WARNING,
            "Public fixture diagnostic",
        ),
        receive_time=BrokerReceiveTimeV1(200, 10, "fixture-clock"),
    )
    assert (
        validate_broker_capability_event(_plan(), event).event.to_json()
        == event.to_json()
    )
    # A metadata/events-only operation does not itself require quote capability.
    operations = (
        ("iter_events", "open_session")
        if kind is BrokerEventKind.QUOTE
        else OPERATIONS
    )
    plan = _plan(
        tuple(atom for atom in ALL if atom != codes[kind]),
        operations=operations,
    )
    with pytest.raises(BrokerCapabilityError, match="capability_violation"):
        validate_broker_capability_event(plan, event)


def test_receive_time_raw_hash_and_public_context_have_exact_provenance() -> (
    None
):
    event = _event(
        receive_time=BrokerReceiveTimeV1(200, 10, "fixture-clock"),
        raw_provenance=BrokerRawProvenanceV1(
            "c" * 64, 12, "application/json", "external-rights-policy"
        ),
    )
    admitted = validate_broker_capability_event(
        _plan(required=("timestamps.receive.v1", "raw-hashes.v1")), event
    )
    assert admitted.event.raw_provenance.policy_id == "external-rights-policy"
    assert admitted.event.source_time is None
    metadata = replace(
        EXTERNAL["factory"]().metadata,
        extensions=(
            BrokerExtensionV1(
                PUBLIC_CONTEXT_NAMESPACE,
                json.dumps(
                    {
                        "account_class": "demo",
                        "feed_class": "indicative",
                        "server_label": "test",
                    }
                ),
            ),
        ),
    )
    receipt = validate_broker_metadata(_plan(), metadata)
    assert all(
        item.support is BrokerFieldSupport.PRESENT for item in receipt.fields
    )
    with pytest.raises(BrokerCapabilityError):
        validate_broker_metadata(_plan(caps=()), metadata)
    with pytest.raises(BrokerCapabilityError):
        validate_broker_metadata(
            _plan(),
            replace(
                metadata,
                extensions=(
                    BrokerExtensionV1(
                        PUBLIC_CONTEXT_NAMESPACE,
                        '{"server_label":"private-host"}',
                    ),
                ),
            ),
        )


def test_plugin_failures_never_echo_credentials_and_runtime_identity_is_checked() -> (
    None
):
    def bad() -> BrokerPluginV1:
        raise RuntimeError("password=PRIVATE_CANARY")

    with pytest.raises(BrokerCapabilityError) as error:
        invoke_authorized_broker_factory(
            _inventory(), _plan(), authorize=lambda _: True, factory=bad
        )
    assert (
        str(error.value) == "plugin_failure"
        and error.value.to_dict()["reason"] == "plugin_failure"
    )

    class Wrong(EXTERNAL["OfflineCapabilityPlugin"]):
        @property
        def metadata(self):
            return replace(super().metadata, plugin_id="org.example.other")

    with pytest.raises(
        BrokerCapabilityError, match="runtime_identity_mismatch"
    ):
        _invocation(Wrong())


def test_method_state_invalid_symbols_and_cross_thread_calls_refuse() -> None:
    invocation = _invocation()
    with pytest.raises(BrokerCapabilityError):
        invocation.instruments()
    invocation.open_session({})
    with pytest.raises(BrokerCapabilityError):
        invocation.subscribe(("EURUSD",))
    invocation.instruments()
    for symbols in (("GBPUSD",), ("EURUSD", "EURUSD"), (), ["EURUSD"]):
        with pytest.raises(BrokerCapabilityError):
            invocation.subscribe(symbols)
    errors: list[str] = []

    def cross_thread() -> None:
        try:
            invocation.instruments()
        except BrokerCapabilityError as exc:
            errors.append(str(exc))

    thread = threading.Thread(target=cross_thread)
    thread.start()
    thread.join()
    assert errors == ["invalid_state"]
    invocation.close_session()


def test_stream_creation_is_lazy_single_owner_and_budget_does_not_probe_extra() -> (
    None
):
    EXTERNAL["CALLS"].clear()
    invocation = _invocation()
    invocation.open_session({})
    invocation.instruments()
    invocation.subscribe(("EURUSD",))
    first = invocation.iter_events(max_events=1)
    second = invocation.iter_events(max_events=1)
    assert EXTERNAL["CALLS"].count("iter_events") == 0
    assert next(first).event.sequence == 0
    with pytest.raises(BrokerCapabilityError, match="invalid_state"):
        next(second)
    assert EXTERNAL["CALLS"].count("iter_events") == 1
    with pytest.raises(BrokerCapabilityError, match="resource_limit"):
        next(first)
    assert EXTERNAL["CALLS"].count("iter_events") == 1
    assert [item.event.sequence for item in invocation.iter_events()] == [1]
    for bad in (True, 0, 1_000_001):
        with pytest.raises(BrokerCapabilityError, match="resource_limit"):
            invocation.iter_events(max_events=bad)
    invocation.close_session()


def test_stream_rejects_sequence_clock_session_and_unsubscribed_quotes() -> (
    None
):
    class Sequence(EXTERNAL["OfflineCapabilityPlugin"]):
        events: list[BrokerEventV1] = []

        def iter_events(self, session):
            yield from self.events

    for invalid in (
        replace(_event(), sequence=1),
        replace(
            _event(), session_id="broker-plugin-session:sha256:" + "b" * 64
        ),
    ):
        plugin = Sequence()
        plugin.events = [invalid]
        invocation = _invocation(plugin)
        invocation.open_session({})
        invocation.instruments()
        invocation.subscribe(("EURUSD",))
        with pytest.raises(BrokerCapabilityError, match="capability_violation"):
            list(invocation.iter_events())
        invocation.close_session()
    plugin = Sequence()
    plugin.events = [
        _event(receive_time=BrokerReceiveTimeV1(200, 20, "fixture-clock"))
    ]
    invocation = _invocation(plugin)
    invocation.open_session({})
    invocation.instruments()
    invocation.subscribe(("EURUSD",))
    assert len(list(invocation.iter_events())) == 1
    plugin.events = [
        _event(
            sequence=1,
            receive_time=BrokerReceiveTimeV1(201, 19, "fixture-clock"),
        )
    ]
    with pytest.raises(BrokerCapabilityError, match="capability_violation"):
        list(invocation.iter_events())
    invocation.close_session()


@pytest.mark.parametrize(
    "operations,missing",
    [
        (("instruments",), "open_session"),
        (("subscribe",), "instruments"),
        (("iter_events",), "open_session"),
        (("unsubscribe", "open_session"), "subscribe"),
    ],
)
def test_impossible_operation_sets_refuse_before_factory(
    operations: tuple[str, ...], missing: str
) -> None:
    plan = _plan(operations=operations)
    assert not plan.admitted and missing in plan.unsupported_operations
    calls: list[str] = []
    with pytest.raises(BrokerCapabilityError, match="unsupported_operation"):
        invoke_authorized_broker_factory(
            _inventory(),
            plan,
            authorize=lambda _: calls.append("authorize"),
            factory=lambda: calls.append("factory"),
        )
    assert not calls


def test_health_only_stream_needs_session_not_subscription_and_optional_monotonicity() -> (
    None
):
    for caps in (
        ("events.v1", "session.v1"),
        ("events.v1", "session.v1", "quotes.v1"),
    ):
        assert _plan(caps, operations=("iter_events", "open_session")).admitted
    assert not _plan(
        operations=("iter_events", "open_session"), required=("quotes.v1",)
    ).admitted
    for operation in ("metadata", "configuration_schema", "close_session"):
        assert _plan(caps=(), operations=(operation,)).admitted


def test_undeclared_method_invalid_config_and_secret_system_exit_are_closed() -> (
    None
):
    calls = EXTERNAL["CALLS"]
    calls.clear()
    invocation = _invocation(plan=_plan(operations=("metadata",)))
    for operation in (
        lambda: invocation.configuration_schema,
        lambda: invocation.open_session({}),
        invocation.instruments,
        lambda: invocation.subscribe(("EURUSD",)),
        lambda: invocation.unsubscribe(("EURUSD",)),
        invocation.iter_events,
    ):
        with pytest.raises(
            BrokerCapabilityError, match="unsupported_operation"
        ):
            operation()
    assert calls == ["factory", "metadata"]
    invocation.close_session()
    invocation = _invocation()
    calls.clear()
    with pytest.raises(BrokerCapabilityError, match="capability_violation"):
        invocation.open_session({"password": "SECRET_CONFIG_CANARY"})
    assert "open_session" not in calls
    invocation.close_session()

    def stop():
        raise SystemExit("SECRET_EXCEPTION_CANARY")

    with pytest.raises(BrokerCapabilityError, match="^plugin_failure$"):
        invoke_authorized_broker_factory(
            _inventory(), _plan(), authorize=lambda _: True, factory=stop
        )


def test_unsubscribed_quotes_are_not_delivered_and_explicit_clock_correction_is_preserved() -> (
    None
):
    class Sequence(EXTERNAL["OfflineCapabilityPlugin"]):
        events: list[BrokerEventV1] = []

        def iter_events(self, session):
            yield from self.events

    plugin = Sequence()
    invocation = _invocation(plugin)
    invocation.open_session({})
    invocation.instruments()
    plugin.events = [_event()]
    with pytest.raises(BrokerCapabilityError, match="capability_violation"):
        list(invocation.iter_events())
    invocation.subscribe(("EURUSD",))
    plugin.events = [
        _event(receive_time=BrokerReceiveTimeV1(200, 20, "fixture-clock"))
    ]
    assert len(list(invocation.iter_events())) == 1
    correction = _event(
        sequence=1,
        kind=BrokerEventKind.CLOCK_CORRECTION,
        quote=None,
        receive_time=BrokerReceiveTimeV1(100, 21, "fixture-clock"),
        diagnostic=BrokerDiagnosticV1(
            BrokerReasonCode.CLOCK_DISCONTINUITY,
            BrokerDiagnosticSeverity.WARNING,
            "Declared correction",
        ),
    )
    plugin.events = [correction]
    assert (
        list(invocation.iter_events())[0].event.to_json()
        == correction.to_json()
    )
    invocation.close_session()


def test_event_receipt_replay_refuses_forged_support_and_nested_extensions_roundtrip() -> (
    None
):
    event = _event(
        extensions=(
            BrokerExtensionV1(
                "org.example.evidence",
                json.dumps({"nested": [True, None, {"x": "y"}]}),
            ),
        )
    )
    admitted = validate_broker_capability_event(_plan(), event)
    restored = BrokerAdmittedEventV1.from_json(admitted.to_json())
    verify_broker_admitted_event(restored, _plan())
    forged = replace(
        admitted,
        fields=tuple(
            (
                replace(item, support=BrokerFieldSupport.PRESENT)
                if item.field == "source_time"
                else item
            )
            for item in admitted.fields
        ),
    )
    with pytest.raises(BrokerCapabilityError, match="capability_violation"):
        verify_broker_admitted_event(forged, _plan())
    assert restored.event.extensions == event.extensions


def test_expanded_alias_graph_refuses_before_whole_container_serialization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from histdatacom.broker_plugin_capabilities import contracts

    original = contracts.json.dumps
    serialized_containers: list[object] = []

    def guarded(value, *args, **kwargs):
        if type(value) in (list, dict):
            serialized_containers.append(value)
            raise AssertionError("oversized expansion reached final serializer")
        return original(value, *args, **kwargs)

    monkeypatch.setattr(contracts.json, "dumps", guarded)
    # Tiny retained graph, approximately 16GiB if naively expanded.
    branch = ["x" * 8192] * 128
    repeated = [[branch] * 128] * 128
    with pytest.raises(BrokerCapabilityError, match="invalid_artifact"):
        canonical_capability_json(repeated)
    assert serialized_containers == []


def test_traversal_budget_matches_exact_canonical_escaped_byte_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from histdatacom.broker_plugin_capabilities import contracts

    payload = {'escaped"key': ["\u00e9", True, False, None, 123, {"x": "\\\n"}]}
    expected = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    monkeypatch.setattr(contracts, "MAX_CAPABILITY_BYTES", len(expected))
    assert canonical_capability_json(payload) == expected
    monkeypatch.setattr(contracts, "MAX_CAPABILITY_BYTES", len(expected) - 1)
    with pytest.raises(BrokerCapabilityError, match="invalid_artifact"):
        canonical_capability_json(payload)
