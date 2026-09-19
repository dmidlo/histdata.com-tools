"""Structural plugin interfaces; SDK imports never open host machinery."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from typing import Protocol, runtime_checkable

from .contracts import (
    MAX_SDK_COLLECTION_ITEMS,
    BrokerConfigurationSchemaV1,
    BrokerDiagnosticSeverity,
    BrokerDiagnosticV1,
    BrokerEventKind,
    BrokerEventV1,
    BrokerInstrumentV1,
    BrokerPluginMetadataV1,
    BrokerReasonCode,
    BrokerSessionV1,
)


class BrokerPluginError(RuntimeError):
    """A bounded public diagnostic, never a raw transport exception dump."""

    def __init__(self, diagnostic: BrokerDiagnosticV1) -> None:
        self.diagnostic = diagnostic
        super().__init__(diagnostic.code.value + ": " + diagnostic.summary)


@runtime_checkable
class BrokerPluginV1(Protocol):
    """One synchronous, session-scoped producer of broker-neutral evidence.

    The host owns persistence, capture clocks, retry policy and cancellation.
    A plugin controls only its connection state and ephemeral configuration.
    """

    @property
    def metadata(self) -> BrokerPluginMetadataV1: ...

    @property
    def configuration_schema(self) -> BrokerConfigurationSchemaV1: ...

    def open_session(
        self, configuration: Mapping[str, object]
    ) -> BrokerSessionV1:
        """Validate configuration and establish a new independently ordered session."""
        ...

    def instruments(
        self, session: BrokerSessionV1
    ) -> tuple[BrokerInstrumentV1, ...]:
        """Return a complete bounded discovery snapshot or refuse explicitly."""
        ...

    def subscribe(
        self, session: BrokerSessionV1, symbols: tuple[str, ...]
    ) -> None:
        """Subscribe using the canonical symbols declared by discovery."""
        ...

    def unsubscribe(
        self, session: BrokerSessionV1, symbols: tuple[str, ...]
    ) -> None:
        """Remove subscriptions; emit evidence of the change in event order."""
        ...

    def iter_events(self, session: BrokerSessionV1) -> Iterator[BrokerEventV1]:
        """Yield bounded events in contiguous local sequence order from zero."""
        ...

    def close_session(self, session: BrokerSessionV1) -> None:
        """Release connection resources; repeated close of this session is harmless."""
        ...


def normalize_broker_instrument(
    provider_symbol: str, instruments: tuple[BrokerInstrumentV1, ...]
) -> BrokerInstrumentV1:
    """Resolve exact discovery evidence without guessing suffixes or currencies."""
    if not isinstance(instruments, tuple) or not all(
        isinstance(item, BrokerInstrumentV1) for item in instruments
    ):
        raise TypeError("instrument discovery requires immutable SDK artifacts")
    if len(instruments) > MAX_SDK_COLLECTION_ITEMS:
        raise ValueError("instrument discovery exceeds SDK bounds")
    provider_names = [item.provider_symbol for item in instruments]
    symbols = [item.symbol for item in instruments]
    if len(set(provider_names)) != len(provider_names) or len(
        set(symbols)
    ) != len(symbols):
        raise ValueError("instrument discovery is ambiguous")
    for item in instruments:
        if item.provider_symbol == provider_symbol:
            return item
    raise BrokerPluginError(
        BrokerDiagnosticV1(
            code=BrokerReasonCode.UNSUPPORTED_INSTRUMENT,
            severity=BrokerDiagnosticSeverity.ERROR,
            summary="Provider instrument was not declared by discovery",
        )
    )


def validate_broker_event_stream(
    events: Iterable[BrokerEventV1],
    session: BrokerSessionV1,
    *,
    starting_sequence: int = 0,
) -> Iterator[BrokerEventV1]:
    """Check ordering/session/clock evidence without reading or retaining rows.

    A caller admitting a resumed fragment must supply its verified next sequence.
    SDK validation is not source capture certification or a reconnect policy.
    """
    if type(starting_sequence) is not int or not 0 <= starting_sequence < 2**63:
        raise ValueError("invalid starting sequence")
    next_sequence = starting_sequence
    previous_monotonic: int | None = None
    previous_utc: int | None = None
    for event in events:
        if not isinstance(event, BrokerEventV1):
            raise TypeError("event stream requires BrokerEventV1")
        if (
            event.session_id != session.artifact_id
            or event.sequence != next_sequence
        ):
            raise ValueError("event session or contiguous sequence differs")
        timing = event.receive_time
        if timing is not None:
            if timing.clock_id != session.receive_clock_id:
                raise ValueError("receive clock differs from session")
            if (
                previous_monotonic is not None
                and timing.monotonic_ns < previous_monotonic
            ):
                raise ValueError("monotonic receive time moves backward")
            if (
                previous_utc is not None
                and timing.utc_ns < previous_utc
                and event.kind is not BrokerEventKind.CLOCK_CORRECTION
            ):
                raise ValueError(
                    "backward wall-clock steps require correction evidence"
                )
            previous_monotonic = timing.monotonic_ns
            previous_utc = timing.utc_ns
        next_sequence += 1
        yield event
