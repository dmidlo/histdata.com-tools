"""External plugin example: its only application import is the public SDK.

This deterministic fixture performs no network I/O and owns no host objects.
It is also a strict-mypy structural conformance fixture, not a provider plugin.
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping

from histdatacom.broker_plugins import (
    BrokerConfigurationFieldV1,
    BrokerConfigurationSchemaV1,
    BrokerConfigurationType,
    BrokerDiagnosticSeverity,
    BrokerDiagnosticV1,
    BrokerEventKind,
    BrokerEventV1,
    BrokerInstrumentV1,
    BrokerPluginError,
    BrokerPluginMetadataV1,
    BrokerPluginV1,
    BrokerQuoteV1,
    BrokerReasonCode,
    BrokerReceiveTimeV1,
    BrokerSessionV1,
    normalize_broker_instrument,
    validate_broker_event_stream,
)


class ExternalFixturePlugin:
    """A structural implementation with private connection state only."""

    def __init__(self) -> None:
        self._session: BrokerSessionV1 | None = None
        self._closed = False
        self._sequence = 0
        self._pending: list[BrokerEventV1] = []

    @property
    def metadata(self) -> BrokerPluginMetadataV1:
        return BrokerPluginMetadataV1(
            plugin_id="org.example.fixture",
            plugin_version="7.2.1",
            display_name="External fixture",
        )

    @property
    def configuration_schema(self) -> BrokerConfigurationSchemaV1:
        return BrokerConfigurationSchemaV1(
            (
                BrokerConfigurationFieldV1(
                    name="credential",
                    value_type=BrokerConfigurationType.STRING,
                    description="Ephemeral provider credential",
                    secret=True,
                ),
            )
        )

    def open_session(
        self, configuration: Mapping[str, object]
    ) -> BrokerSessionV1:
        try:
            self.configuration_schema.validate_configuration(configuration)
        except (TypeError, ValueError):
            raise BrokerPluginError(
                BrokerDiagnosticV1(
                    BrokerReasonCode.INVALID_CONFIGURATION,
                    BrokerDiagnosticSeverity.ERROR,
                    "Fixture configuration failed public schema validation",
                )
            ) from None
        if self._session is not None:
            raise self._error("This fixture supports one session")
        self._session = BrokerSessionV1(
            metadata_id=self.metadata.artifact_id,
            instance_nonce="1" * 32,  # Deliberately stable fixture identity.
            opened_at_utc_ns=1_000,
            receive_clock_id="fixture-process-clock",
        )
        self._emit(BrokerEventKind.CONNECTED)
        return self._session

    def instruments(
        self, session: BrokerSessionV1
    ) -> tuple[BrokerInstrumentV1, ...]:
        self._require_session(session)
        return (
            BrokerInstrumentV1(
                symbol="EURUSD",
                provider_symbol="EUR/USD.fixture",
                base_currency="EUR",
                quote_currency="USD",
                price_increment="0.00001",
            ),
        )

    def subscribe(
        self, session: BrokerSessionV1, symbols: tuple[str, ...]
    ) -> None:
        self._require_session(session)
        if symbols != ("EURUSD",):
            raise self._error("Unsupported fixture subscription")
        self._emit(BrokerEventKind.SUBSCRIBED, instrument="EURUSD")
        self._emit(
            BrokerEventKind.QUOTE,
            instrument="EURUSD",
            quote=BrokerQuoteV1("EURUSD", "1.10000", "1.10012"),
        )
        self._emit(BrokerEventKind.HEARTBEAT)

    def unsubscribe(
        self, session: BrokerSessionV1, symbols: tuple[str, ...]
    ) -> None:
        self._require_session(session)
        if symbols != ("EURUSD",):
            raise self._error("Unsupported fixture subscription")
        self._emit(BrokerEventKind.UNSUBSCRIBED, instrument="EURUSD")

    def iter_events(self, session: BrokerSessionV1) -> Iterator[BrokerEventV1]:
        self._require_session(session, allow_closed=True)
        pending = tuple(self._pending)
        self._pending.clear()
        return iter(pending)

    def close_session(self, session: BrokerSessionV1) -> None:
        self._require_session(session, allow_closed=True)
        if not self._closed:
            self._emit(BrokerEventKind.DISCONNECTED)
            self._closed = True

    def _require_session(
        self, session: BrokerSessionV1, *, allow_closed: bool = False
    ) -> None:
        if self._session != session or (self._closed and not allow_closed):
            raise self._error("Session is not active")

    def _emit(
        self,
        kind: BrokerEventKind,
        *,
        instrument: str | None = None,
        quote: BrokerQuoteV1 | None = None,
    ) -> None:
        if self._session is None:
            raise self._error("Session has not been opened")
        self._pending.append(
            BrokerEventV1(
                session_id=self._session.artifact_id,
                sequence=self._sequence,
                kind=kind,
                connection_id="fixture-connection",
                receive_time=BrokerReceiveTimeV1(
                    utc_ns=1_000 + self._sequence,
                    monotonic_ns=500 + self._sequence,
                    clock_id=self._session.receive_clock_id,
                ),
                instrument=instrument,
                quote=quote,
            )
        )
        self._sequence += 1

    @staticmethod
    def _error(summary: str) -> BrokerPluginError:
        return BrokerPluginError(
            BrokerDiagnosticV1(
                BrokerReasonCode.UNSUPPORTED_OPERATION,
                BrokerDiagnosticSeverity.ERROR,
                summary,
            )
        )


def exercise_external_plugin() -> tuple[BrokerEventV1, ...]:
    """Execute every mandatory protocol method using only public types."""
    plugin: BrokerPluginV1 = ExternalFixturePlugin()
    assert isinstance(plugin, BrokerPluginV1)
    session = plugin.open_session({"credential": "fixture-only-secret"})
    instrument = normalize_broker_instrument(
        "EUR/USD.fixture",
        plugin.instruments(session),
    )
    plugin.subscribe(session, (instrument.symbol,))
    plugin.unsubscribe(session, (instrument.symbol,))
    plugin.close_session(session)
    plugin.close_session(session)
    events = tuple(
        validate_broker_event_stream(plugin.iter_events(session), session)
    )
    assert len(events) == 6
    assert events[-1].kind is BrokerEventKind.DISCONNECTED
    for event in events:
        assert BrokerEventV1.from_json(event.to_json()) == event
        assert "fixture-only-secret" not in event.to_json()
    return events


if __name__ == "__main__":
    print(json.dumps({"event_count": len(exercise_external_plugin())}))
