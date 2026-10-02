"""Small public SDK implementation with explicit generated test scenarios.

Replace the generated transport in iter_events when implementing a provider.
Do not retain configuration or put provider responses/exceptions in diagnostics.
Negative modes deliberately violate contracts to exercise host refusals; never
expose these modes as an actual broker feed. There is no network or credential IO.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import replace
from typing import NoReturn, cast

from histdatacom.broker_plugins import (
    BROKER_PLUGIN_SDK_VERSION,
    BrokerConfigurationFieldV1,
    BrokerConfigurationSchemaV1,
    BrokerConfigurationType,
    BrokerDiagnosticSeverity,
    BrokerDiagnosticV1,
    BrokerEventKind,
    BrokerEventV1,
    BrokerGapScope,
    BrokerGapV1,
    BrokerInstrumentV1,
    BrokerPluginError,
    BrokerPluginMetadataV1,
    BrokerPluginV1,
    BrokerQuoteV1,
    BrokerReasonCode,
    BrokerReceiveTimeV1,
    BrokerSessionV1,
    BrokerSourceTimeSemantics,
    BrokerSourceTimeV1,
)

PLUGIN_ID = "org.example.starter"
PLUGIN_VERSION = "0.1.0"
PROVIDER_ID = "starter"
DISPLAY_NAME = "Generated broker starter (not a provider feed)"
SYMBOL = "EURUSD"
SCENARIOS = (
    "bad-clock",
    "bad-sequence",
    "burst",
    "close-block",
    "duplicate",
    "exception",
    "finite",
    "gap",
    "healthy-delay",
    "healthy-duplicate",
    "healthy-gap",
    "healthy-reorder",
    "heartbeat",
    "honest-health",
    "invalid-spread",
    "malformed",
    "next-block",
    "reconnect",
    "secret-leak",
    "stale",
    "unchanged",
)
CAPABILITIES = tuple(
    sorted(
        (
            "connection.v1",
            "events.v1",
            "gaps.v1",
            "health.v1",
            "heartbeat.v1",
            "instruments.v1",
            "quotes.v1",
            "session.v1",
            "timestamps.broker-event.v1",
            "timestamps.receive.v1",
        )
    )
)
METADATA = BrokerPluginMetadataV1(PLUGIN_ID, PLUGIN_VERSION, DISPLAY_NAME)
CONFIGURATION_SCHEMA = BrokerConfigurationSchemaV1(
    (
        BrokerConfigurationFieldV1(
            "mode",
            BrokerConfigurationType.STRING,
            "Generated test scenario; never a live endpoint or credential.",
            choices=SCENARIOS,
        ),
    )
)
INSTRUMENT = BrokerInstrumentV1(SYMBOL, "EUR/USD", "EUR", "USD")
FIXTURE_UTC_NS = 1_700_000_000_000_000_000


def _refuse(code: BrokerReasonCode) -> NoReturn:
    raise BrokerPluginError(
        BrokerDiagnosticV1(
            code,
            BrokerDiagnosticSeverity.ERROR,
            code.value,
        )
    )


class StarterPlugin:
    """Single-owner, one-session synchronous lifecycle skeleton.

    Provider implementations should replace generated observations, not host
    clocks, persistence, grants, scientific state, or private host internals.
    """

    metadata = METADATA
    configuration_schema = CONFIGURATION_SCHEMA

    def __init__(self) -> None:
        if BROKER_PLUGIN_SDK_VERSION != "1.0.0":
            raise RuntimeError("unsupported public broker SDK version")
        self._session: BrokerSessionV1 | None = None
        self._mode = "finite"
        self._symbols: set[str] = set()
        self._sequence = 0
        self._emitted = False
        self._streaming = False

    def _active(self, session: BrokerSessionV1) -> None:
        if self._session is None or session != self._session:
            _refuse(BrokerReasonCode.UNSUPPORTED_OPERATION)

    def open_session(self, configuration: Mapping[str, object]) -> BrokerSessionV1:
        if self._session is not None:
            _refuse(BrokerReasonCode.UNSUPPORTED_OPERATION)
        try:
            self.configuration_schema.validate_configuration(configuration)
        except (TypeError, ValueError):
            _refuse(BrokerReasonCode.INVALID_CONFIGURATION)
        mode = configuration["mode"]
        assert type(mode) is str
        self._mode = mode
        if mode == "exception":
            raise RuntimeError("generated open failure")
        if mode == "secret-leak":
            raise RuntimeError("generated-conformance-secret-not-for-retention")
        self._sequence = 0
        self._emitted = False
        self._symbols.clear()
        # These are explicitly generated source/receive clocks, not observations
        # of a real provider. A fresh public nonce still distinguishes sessions.
        self._session = BrokerSessionV1(
            self.metadata.artifact_id,
            uuid.uuid4().hex,
            FIXTURE_UTC_NS,
            "starter-generated-receive-clock",
        )
        return self._session

    def instruments(self, session: BrokerSessionV1) -> tuple[BrokerInstrumentV1, ...]:
        self._active(session)
        return (INSTRUMENT,)

    def subscribe(self, session: BrokerSessionV1, symbols: tuple[str, ...]) -> None:
        self._active(session)
        if not symbols or len(set(symbols)) != len(symbols) or set(symbols) - {SYMBOL}:
            _refuse(BrokerReasonCode.UNSUPPORTED_INSTRUMENT)
        self._symbols.update(symbols)

    def unsubscribe(self, session: BrokerSessionV1, symbols: tuple[str, ...]) -> None:
        self._active(session)
        if not symbols or set(symbols) - self._symbols:
            _refuse(BrokerReasonCode.UNSUPPORTED_INSTRUMENT)
        self._symbols.difference_update(symbols)

    def _event(
        self,
        kind: BrokerEventKind,
        *,
        instrument: str | None = None,
        quote: BrokerQuoteV1 | None = None,
        source_offset: int = 1,
        diagnostic: BrokerDiagnosticV1 | None = None,
        gap: BrokerGapV1 | None = None,
    ) -> BrokerEventV1:
        assert self._session is not None
        sequence = self._sequence
        self._sequence += 1
        return BrokerEventV1(
            self._session.artifact_id,
            sequence,
            kind,
            "starter-generated-connection",
            source_time=(
                BrokerSourceTimeV1(
                    source_offset * 1_000_000_000,
                    1,
                    BrokerSourceTimeSemantics.BROKER_EVENT,
                )
                if quote is not None and self._mode != "honest-health"
                else None
            ),
            receive_time=BrokerReceiveTimeV1(
                (
                    time.time_ns()
                    if self._mode == "honest-health"
                    else (10 + sequence) * 1_000_000_000
                ),
                time.monotonic_ns() if self._mode == "honest-health" else 10 + sequence,
                self._session.receive_clock_id,
            ),
            instrument=instrument,
            quote=quote,
            diagnostic=diagnostic,
            gap=gap,
        )

    def iter_events(self, session: BrokerSessionV1) -> Iterator[BrokerEventV1]:
        self._active(session)
        if self._streaming:
            _refuse(BrokerReasonCode.UNSUPPORTED_OPERATION)
        self._streaming = True
        try:
            if self._emitted:
                return
            self._emitted = True
            if self._mode == "next-block":
                time.sleep(300)
                return
            if SYMBOL not in self._symbols:
                _refuse(BrokerReasonCode.UNSUPPORTED_OPERATION)
            if self._mode == "malformed":
                yield cast(BrokerEventV1, {"generated": "not-an-SDK-event"})
                return
            if self._mode in ("finite", "honest-health"):
                yield self._event(
                    BrokerEventKind.HEALTH,
                    diagnostic=BrokerDiagnosticV1(
                        BrokerReasonCode.HEALTHY,
                        BrokerDiagnosticSeverity.INFO,
                        BrokerReasonCode.HEALTHY.value,
                    ),
                )
            if self._mode in ("gap", "healthy-gap"):
                yield self._event(
                    BrokerEventKind.GAP,
                    gap=BrokerGapV1(BrokerGapScope.SOURCE, 100, 200, 5),
                    diagnostic=BrokerDiagnosticV1(
                        BrokerReasonCode.SOURCE_GAP,
                        BrokerDiagnosticSeverity.WARNING,
                        BrokerReasonCode.SOURCE_GAP.value,
                    ),
                )
            offsets = (
                (1, 1, 2)
                if self._mode in ("duplicate", "healthy-duplicate", "stale")
                else ((3, 1, 2) if self._mode == "healthy-reorder" else (1, 2, 3))
            )
            for index in range(4 if self._mode == "burst" else 3):
                if self._mode in ("stale", "healthy-delay") and index == 1:
                    time.sleep(0.15)
                if self._mode in ("heartbeat", "healthy-delay"):
                    yield self._event(BrokerEventKind.HEARTBEAT)
                quote = BrokerQuoteV1(
                    SYMBOL,
                    f"1.1000{index}" if self._mode == "stale" else "1.10000",
                    "1.20000",
                )
                if self._mode == "invalid-spread":
                    # Deliberately corrupt only generated fixtures. A real
                    # implementation must NEVER circumvent frozen validation.
                    object.__setattr__(quote, "bid", "2.00000")
                event = self._event(
                    BrokerEventKind.QUOTE,
                    instrument=SYMBOL,
                    quote=(
                        BrokerQuoteV1(SYMBOL, "1.10000", "1.20000")
                        if self._mode == "invalid-spread"
                        else quote
                    ),
                    source_offset=offsets[index % 3],
                )
                if self._mode == "invalid-spread":
                    object.__setattr__(event, "quote", quote)
                elif self._mode == "bad-sequence" and index == 1:
                    event = replace(event, sequence=event.sequence + 1)
                elif self._mode == "bad-clock" and index:
                    assert event.receive_time is not None
                    event = replace(
                        event,
                        receive_time=replace(
                            event.receive_time,
                            monotonic_ns=1,
                        ),
                    )
                yield event
            if self._mode == "reconnect":
                yield self._event(BrokerEventKind.DISCONNECTED)
        finally:
            self._streaming = False

    def close_session(self, session: BrokerSessionV1) -> None:
        if self._session is None:
            return
        self._active(session)
        if self._mode == "close-block":
            time.sleep(300)
        self._symbols.clear()
        self._session = None
        # No terminal event is fabricated after a caller stops consuming.


def factory() -> BrokerPluginV1:
    """Emission-only resource ABI: no injected host object or secret parameter."""
    return StarterPlugin()
