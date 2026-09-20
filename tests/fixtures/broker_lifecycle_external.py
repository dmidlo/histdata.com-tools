"""Offline lifecycle conformance plugin; imports only the public SDK."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
import os
import time
from typing import cast

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
    BrokerPluginV1,
    BrokerQuoteV1,
    BrokerReceiveTimeV1,
    BrokerReasonCode,
    BrokerSessionV1,
)


class LifecycleFixture:
    def __init__(self) -> None:
        self.mode = "finite"

    @property
    def metadata(self) -> BrokerPluginMetadataV1:
        return BrokerPluginMetadataV1(
            "org.example.lifecycle", "1.0.0", "Offline lifecycle fixture"
        )

    @property
    def configuration_schema(self) -> BrokerConfigurationSchemaV1:
        return BrokerConfigurationSchemaV1(
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

    def open_session(
        self, configuration: Mapping[str, object]
    ) -> BrokerSessionV1:
        self.mode = cast(str, configuration["mode"])
        if self.mode == "open_block":
            time.sleep(60)
        if self.mode == "exception":
            raise RuntimeError("credential=do-not-echo-provider-secret")
        if self.mode in ("stdout", "stderr"):
            os.write(
                1 if self.mode == "stdout" else 2,
                b"do-not-echo-output-secret" * 5000,
            )
        return BrokerSessionV1(
            self.metadata.artifact_id, "a" * 32, 100, "fixture-clock"
        )

    def instruments(
        self, session: BrokerSessionV1
    ) -> tuple[BrokerInstrumentV1, ...]:
        return (
            BrokerInstrumentV1("EURUSD", "EUR/USD", "EUR", "USD", "0.00001"),
        )

    def subscribe(
        self, session: BrokerSessionV1, symbols: tuple[str, ...]
    ) -> None:
        pass

    def unsubscribe(
        self, session: BrokerSessionV1, symbols: tuple[str, ...]
    ) -> None:
        pass

    def iter_events(self, session: BrokerSessionV1) -> Iterator[BrokerEventV1]:
        if self.mode == "next_block":
            time.sleep(60)
        if self.mode == "malformed":
            yield cast(BrokerEventV1, object())
            return
        for sequence in range(2):
            yield BrokerEventV1(
                session.artifact_id,
                sequence,
                BrokerEventKind.QUOTE,
                "connection-1",
                receive_time=BrokerReceiveTimeV1(
                    200 + sequence, 10 + sequence, "fixture-clock"
                ),
                instrument="EURUSD",
                quote=BrokerQuoteV1("EURUSD", "1.1", "1.2"),
            )
        if self.mode == "reconnect":
            yield BrokerEventV1(
                session.artifact_id,
                2,
                BrokerEventKind.DISCONNECTED,
                "connection-1",
            )
        if self.mode in (
            "health_gap",
            "health_error",
            "health_warning",
            "health_info",
        ):
            yield BrokerEventV1(
                session.artifact_id,
                2,
                BrokerEventKind.HEALTH,
                "connection-1",
                diagnostic=BrokerDiagnosticV1(
                    (
                        BrokerReasonCode.SOURCE_GAP
                        if self.mode == "health_gap"
                        else BrokerReasonCode.INTERNAL_FAILURE
                    ),
                    (
                        BrokerDiagnosticSeverity.ERROR
                        if self.mode == "health_error"
                        else (
                            BrokerDiagnosticSeverity.INFO
                            if self.mode == "health_info"
                            else BrokerDiagnosticSeverity.WARNING
                        )
                    ),
                    "Offline diagnostic",
                ),
            )
        if self.mode == "after_block":
            time.sleep(60)

    def close_session(self, session: BrokerSessionV1) -> None:
        if self.mode == "close_block":
            time.sleep(60)
        if self.mode == "inherited_pipes":
            child = os.fork()
            if child == 0:
                time.sleep(60)
                os._exit(0)


def factory() -> BrokerPluginV1:
    return LifecycleFixture()
