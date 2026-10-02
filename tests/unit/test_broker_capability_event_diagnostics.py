"""Exact host rejection gates cannot be impersonated by producer exceptions."""

from dataclasses import replace

import pytest

from histdatacom.broker_plugin_capabilities import (
    BrokerCapabilityError,
    BrokerCapabilityEventFailure as Failure,
    BrokerCapabilityReason as Reason,
)
from histdatacom.broker_plugin_capabilities.event_diagnostics import (
    next_host_validated_event,
)
from histdatacom.broker_plugins import BrokerReceiveTimeV1
from tests.unit.test_broker_plugin_capabilities import _event, _session


def checked(event, **changes):
    return next_host_validated_event(
        iter((event,)),
        _session(),
        sequence=0,
        previous_monotonic=10,
        previous_utc=100,
        **changes,
    )


@pytest.mark.parametrize(
    "kind,expected",
    [
        ("type", Failure.EVENT_TYPE),
        ("contract", Failure.EVENT_CONTRACT),
        ("spread", Failure.INVALID_SPREAD),
        ("session", Failure.SESSION),
        ("sequence", Failure.SEQUENCE),
        ("clock", Failure.CLOCK_IDENTITY),
        ("monotonic", Failure.MONOTONIC_REGRESSION),
        ("utc", Failure.UTC_REGRESSION),
    ],
)
def test_exact_returned_input_fails_its_actual_host_gate(kind, expected):
    event = _event()
    if kind == "type":
        event = {"private": "generated-do-not-retain"}
    elif kind == "contract":
        object.__setattr__(event.quote, "bid", "not-a-decimal")
    elif kind == "spread":
        object.__setattr__(event.quote, "bid", "1.3")
    elif kind == "session":
        event = replace(
            event,
            session_id=replace(_session(), instance_nonce="b" * 32).artifact_id,
        )
    elif kind == "sequence":
        event = replace(event, sequence=1)
    else:
        event = replace(
            event,
            receive_time=BrokerReceiveTimeV1(
                90 if kind == "utc" else 100,
                9 if kind == "monotonic" else 10,
                (
                    "other-clock"
                    if kind == "clock"
                    else _session().receive_clock_id
                ),
            ),
        )
    with pytest.raises(BrokerCapabilityError) as caught:
        checked(event)
    assert caught.value.reason is Reason.CAPABILITY_VIOLATION
    assert caught.value.event_failure is expected
    assert str(caught.value) == "capability_violation"
    assert caught.value.to_dict() == {
        "status": "refused",
        "reason": "capability_violation",
    }


@pytest.mark.parametrize("failure", list(Failure))
def test_plugin_exception_cannot_impersonate_any_host_gate(failure):
    def malicious():
        raise BrokerCapabilityError(
            Reason.CAPABILITY_VIOLATION, event_failure=failure
        )
        yield

    with pytest.raises(BrokerCapabilityError) as caught:
        next_host_validated_event(
            malicious(),
            _session(),
            sequence=0,
            previous_monotonic=None,
            previous_utc=None,
        )
    assert caught.value.reason is Reason.PLUGIN_FAILURE
    assert caught.value.event_failure is None


def test_valid_input_is_detached_and_eof_is_not_a_failure():
    event = _event()
    assert checked(event) == event
    assert checked(event) is not event
    with pytest.raises(StopIteration):
        next_host_validated_event(
            iter(()),
            _session(),
            sequence=0,
            previous_monotonic=None,
            previous_utc=None,
        )


@pytest.mark.parametrize(
    "reason,field",
    [
        (Reason.PLUGIN_FAILURE, Failure.EVENT_TYPE),
        (Reason.CAPABILITY_VIOLATION, "event_type"),
    ],
)
def test_closed_failure_constructor_refuses_invalid_associations(reason, field):
    with pytest.raises(ValueError):
        BrokerCapabilityError(reason, event_failure=field)


@pytest.mark.parametrize("spoof", [False, True])
def test_actual_gated_plugin_keeps_host_failure_distinct_from_plugin_claim(
    spoof,
):
    from tests.fixtures.broker_provider_policy import (
        generated_provider_scope,
        generated_sdk_request,
    )
    from tests.unit.test_broker_plugin_capabilities import (
        EXTERNAL,
        _invocation,
        _plan,
    )

    class Broken(EXTERNAL["OfflineCapabilityPlugin"]):
        def iter_events(self, session):
            if spoof:
                raise BrokerCapabilityError(
                    Reason.CAPABILITY_VIOLATION,
                    event_failure=Failure.INVALID_SPREAD,
                )
            event = _event(session_id=session.artifact_id)
            object.__setattr__(event.quote, "bid", "1.3")
            yield event

    with generated_provider_scope(generated_sdk_request(_plan())):
        invocation = _invocation(Broken())
        invocation.open_session({})
        invocation.instruments()
        invocation.subscribe(("EURUSD",))
        with pytest.raises(BrokerCapabilityError) as caught:
            tuple(invocation.iter_events())
        invocation.close_session()
    if spoof:
        assert caught.value.reason is Reason.PLUGIN_FAILURE
        assert caught.value.event_failure is None
    else:
        assert caught.value.reason is Reason.CAPABILITY_VIOLATION
        assert caught.value.event_failure is Failure.INVALID_SPREAD
