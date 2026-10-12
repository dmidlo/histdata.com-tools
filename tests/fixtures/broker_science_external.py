"""Small generated transport; actual clocks, no provider or market truth."""

import time

from histdatacom.broker_plugins import (
    BrokerConfigurationFieldV1,
    BrokerConfigurationSchemaV1,
    BrokerConfigurationType,
    BrokerDiagnosticSeverity,
    BrokerDiagnosticV1,
    BrokerEventKind,
    BrokerEventV1,
    BrokerInstrumentV1,
    BrokerPluginMetadataV1,
    BrokerQuoteV1,
    BrokerReasonCode,
    BrokerReceiveTimeV1,
    BrokerSessionV1,
    BrokerSourceTimeSemantics,
    BrokerSourceTimeV1,
)


class ScienceFixture:
    metadata = BrokerPluginMetadataV1(
        "org.example.lifecycle", "1.0.0", "Generated scientific transport"
    )
    configuration_schema = BrokerConfigurationSchemaV1(
        (
            BrokerConfigurationFieldV1(
                "mode",
                BrokerConfigurationType.STRING,
                "Offline fixture scenario",
            ),
            BrokerConfigurationFieldV1(
                "credential",
                BrokerConfigurationType.STRING,
                "Ephemeral fixture value",
                required=False,
                secret=True,
            ),
        )
    )

    def open_session(self, configuration):
        self.mode = configuration["mode"]
        return BrokerSessionV1(
            self.metadata.artifact_id,
            "b" * 32,
            time.time_ns(),
            "generated-clock",
        )

    def instruments(self, session):
        return (
            BrokerInstrumentV1("EURUSD", "EUR/USD", "EUR", "USD", "0.00001"),
        )

    def subscribe(self, session, symbols):
        pass

    def unsubscribe(self, session, symbols):
        pass

    def close_session(self, session):
        pass

    def iter_events(self, session):
        yield BrokerEventV1(
            session.artifact_id,
            0,
            BrokerEventKind.HEALTH,
            "generated-connection",
            receive_time=BrokerReceiveTimeV1(
                time.time_ns(), time.monotonic_ns(), "generated-clock"
            ),
            diagnostic=BrokerDiagnosticV1(
                BrokerReasonCode.HEALTHY,
                BrokerDiagnosticSeverity.INFO,
                "Generated transport is producing its declared finite sequence",
            ),
        )
        for sequence in range(1, 4):
            receive_time = BrokerReceiveTimeV1(
                time.time_ns(), time.monotonic_ns(), "generated-clock"
            )
            source = None
            if self.mode in {"source", "mixed"}:
                semantics = (
                    BrokerSourceTimeSemantics.BROKER_EVENT
                    if sequence < 3 or self.mode == "source"
                    else BrokerSourceTimeSemantics.ADAPTER_RECEIVE
                )
                source = BrokerSourceTimeV1(receive_time.utc_ns, 1, semantics)
            yield BrokerEventV1(
                session.artifact_id,
                sequence,
                BrokerEventKind.QUOTE,
                "generated-connection",
                source_time=source,
                receive_time=receive_time,
                instrument="EURUSD",
                quote=BrokerQuoteV1(
                    "EURUSD",
                    "1.10000",
                    ("1.10010", "1.10020", "1.10030")[sequence - 1],
                ),
            )


def factory():
    return ScienceFixture()
