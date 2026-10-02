"""Closed, payload-free diagnostics at actual host event-validation gates."""

from __future__ import annotations

from collections.abc import Iterator
from typing import NoReturn

from histdatacom.broker_plugins import (
    BrokerEventKind,
    BrokerEventV1,
    BrokerQuoteV1,
    BrokerSessionV1,
)
from histdatacom.broker_plugins.contracts import _decimal

from .contracts import (
    BrokerCapabilityError,
)
from .contracts import (
    BrokerCapabilityEventFailure as Failure,
)
from .contracts import (
    BrokerCapabilityReason as Reason,
)


def _refuse(failure: Failure) -> NoReturn:
    raise BrokerCapabilityError(
        Reason.CAPABILITY_VIOLATION, event_failure=failure
    ) from None


def next_host_validated_event(
    events: Iterator[BrokerEventV1],
    session: BrokerSessionV1,
    *,
    sequence: int,
    previous_monotonic: int | None,
    previous_utc: int | None,
) -> BrokerEventV1:
    """Advance once, then distinguish host validation from producer exceptions.

    Only a closed gate identifier escapes; no input bytes, derived input hash,
    configuration, raw exception or unreviewed diagnostic callback is retained.
    A plugin raising our own error type is still an untrusted producer failure.
    """
    try:
        event = next(events)
    except StopIteration:
        raise
    except (Exception, SystemExit):
        raise BrokerCapabilityError(Reason.PLUGIN_FAILURE) from None
    if type(event) is not BrokerEventV1:
        _refuse(Failure.EVENT_TYPE)
    try:
        # Full bounded native restoration precedes use. Frozen objects are not
        # assumed intact merely because a plugin returned an exact SDK class.
        restored = BrokerEventV1.from_json(event.to_json())
    except (Exception, SystemExit):
        # Attribute the spread gate only when actual exact bounded quote
        # scalars independently demonstrate the predicate. Other malformed
        # content remains a generic contract failure, never a guessed cause.
        quote = getattr(event, "quote", None)
        if type(quote) is BrokerQuoteV1:
            try:
                bid = _decimal(quote.bid, "bid", positive=True)
                ask = _decimal(quote.ask, "ask", positive=True)
            except (AttributeError, ValueError, TypeError):
                pass
            else:
                if bid > ask:
                    _refuse(Failure.INVALID_SPREAD)
        _refuse(Failure.EVENT_CONTRACT)
    if restored.session_id != session.artifact_id:
        _refuse(Failure.SESSION)
    if restored.sequence != sequence:
        _refuse(Failure.SEQUENCE)
    timing = restored.receive_time
    if timing is not None:
        if timing.clock_id != session.receive_clock_id:
            _refuse(Failure.CLOCK_IDENTITY)
        if previous_monotonic is not None and (
            timing.monotonic_ns < previous_monotonic
        ):
            _refuse(Failure.MONOTONIC_REGRESSION)
        if (
            previous_utc is not None
            and timing.utc_ns < previous_utc
            and restored.kind is not BrokerEventKind.CLOCK_CORRECTION
        ):
            _refuse(Failure.UTC_REGRESSION)
    return restored
