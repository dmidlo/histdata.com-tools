"""Executable external plugin: imports only the public, stdlib-only SDK."""

from __future__ import annotations

from collections.abc import Iterator, Mapping

from histdatacom.broker_plugins import (
    BrokerConfigurationSchemaV1,
    BrokerEventKind,
    BrokerEventV1,
    BrokerInstrumentV1,
    BrokerPluginMetadataV1,
    BrokerPluginV1,
    BrokerQuoteV1,
    BrokerReceiveTimeV1,
    BrokerSessionV1,
)

CALLS: list[str] = ["module_import"]


class OfflineCapabilityPlugin:
    def __init__(self) -> None:
        CALLS.append("factory")
        self.sequence = 0

    @property
    def metadata(self) -> BrokerPluginMetadataV1:
        CALLS.append("metadata")
        return BrokerPluginMetadataV1(
            "org.example.capabilities", "1.0.0", "Offline capability fixture"
        )

    @property
    def configuration_schema(self) -> BrokerConfigurationSchemaV1:
        CALLS.append("configuration_schema")
        return BrokerConfigurationSchemaV1(())

    def open_session(
        self, configuration: Mapping[str, object]
    ) -> BrokerSessionV1:
        CALLS.append("open_session")
        self.configuration_schema.validate_configuration(configuration)
        return BrokerSessionV1(
            self.metadata.artifact_id, "a" * 32, 100, "fixture-clock"
        )

    def instruments(
        self, session: BrokerSessionV1
    ) -> tuple[BrokerInstrumentV1, ...]:
        CALLS.append("instruments")
        return (
            BrokerInstrumentV1("EURUSD", "EUR/USD", "EUR", "USD", "0.00001"),
        )

    def subscribe(
        self, session: BrokerSessionV1, symbols: tuple[str, ...]
    ) -> None:
        CALLS.append("subscribe")

    def unsubscribe(
        self, session: BrokerSessionV1, symbols: tuple[str, ...]
    ) -> None:
        CALLS.append("unsubscribe")

    def iter_events(self, session: BrokerSessionV1) -> Iterator[BrokerEventV1]:
        CALLS.append("iter_events")
        value = self.sequence
        self.sequence += 1
        yield BrokerEventV1(
            session.artifact_id,
            value,
            BrokerEventKind.QUOTE,
            "connection-1",
            receive_time=BrokerReceiveTimeV1(
                200 + value, 10 + value, "fixture-clock"
            ),
            instrument="EURUSD",
            quote=BrokerQuoteV1("EURUSD", "1.1", "1.2"),
        )

    def close_session(self, session: BrokerSessionV1) -> None:
        CALLS.append("close_session")


def factory() -> BrokerPluginV1:
    return OfflineCapabilityPlugin()
