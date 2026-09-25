"""Generated public-SDK source with deliberately misleading healthy claims."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
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
    BrokerGapScope,
    BrokerGapV1,
    BrokerHostResourcesV1,
    BrokerInstrumentV1,
    BrokerPluginMetadataV1,
    BrokerPluginV1,
    BrokerQuoteV1,
    BrokerReasonCode,
    BrokerReceiveTimeV1,
    BrokerSessionV1,
    BrokerSourceTimeSemantics,
    BrokerSourceTimeV1,
)


class HealthFixture:
    def __init__(self, resources: BrokerHostResourcesV1) -> None:
        self.mode = "honest"

    @property
    def metadata(self) -> BrokerPluginMetadataV1:
        return BrokerPluginMetadataV1(
            "org.example.permissions", "1.0.0", "Generated permission fixture"
        )

    @property
    def configuration_schema(self) -> BrokerConfigurationSchemaV1:
        return BrokerConfigurationSchemaV1(
            (
                BrokerConfigurationFieldV1(
                    "mode", BrokerConfigurationType.STRING, "Generated scenario"
                ),
                BrokerConfigurationFieldV1(
                    "target",
                    BrokerConfigurationType.STRING,
                    "Generated protected target",
                    required=False,
                ),
                BrokerConfigurationFieldV1(
                    "port",
                    BrokerConfigurationType.INTEGER,
                    "Generated localhost port",
                    required=False,
                ),
            )
        )

    def open_session(
        self, configuration: Mapping[str, object]
    ) -> BrokerSessionV1:
        self.mode = cast(str, configuration["mode"])
        if self.mode not in {
            "honest",
            "duplicate",
            "reorder",
            "gap",
            "delay",
            "unchanged",
        }:
            raise ValueError("unknown generated health scenario")
        return BrokerSessionV1(
            self.metadata.artifact_id, "b" * 32, 100, "generated-clock"
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

    def close_session(self, session: BrokerSessionV1) -> None:
        pass

    def iter_events(self, session: BrokerSessionV1) -> Iterator[BrokerEventV1]:
        sequence = 0
        if self.mode != "honest":
            yield BrokerEventV1(
                session.artifact_id,
                sequence,
                BrokerEventKind.HEALTH,
                "generated-connection",
                diagnostic=BrokerDiagnosticV1(
                    BrokerReasonCode.HEALTHY,
                    BrokerDiagnosticSeverity.INFO,
                    "Generated healthy claim, not host evidence",
                ),
            )
            sequence += 1
        if self.mode == "gap":
            yield BrokerEventV1(
                session.artifact_id,
                sequence,
                BrokerEventKind.GAP,
                "generated-connection",
                gap=BrokerGapV1(BrokerGapScope.SOURCE, 100, 200, 987654),
                diagnostic=BrokerDiagnosticV1(
                    BrokerReasonCode.SOURCE_GAP,
                    BrokerDiagnosticSeverity.WARNING,
                    "Generated source gap claim; count is not observed host loss",
                ),
            )
            sequence += 1
        offsets = (3, 1, 2) if self.mode == "reorder" else (1, 2, 3)
        if self.mode == "duplicate":
            offsets = (1, 1, 2)
        for index, offset in enumerate(offsets):
            if self.mode == "delay" and index == 1:
                time.sleep(0.15)
            if self.mode == "delay":
                yield BrokerEventV1(
                    session.artifact_id,
                    sequence,
                    BrokerEventKind.HEARTBEAT,
                    "generated-connection",
                )
                sequence += 1
            if self.mode in {"duplicate", "unchanged"}:
                bid, ask = "1.1000", "1.2000"
            else:
                bid, ask = f"1.100{index}", f"1.200{index}"
            yield BrokerEventV1(
                session.artifact_id,
                sequence,
                BrokerEventKind.QUOTE,
                "generated-connection",
                source_time=BrokerSourceTimeV1(
                    offset * 1_000_000_000,
                    1,
                    BrokerSourceTimeSemantics.BROKER_EVENT,
                ),
                receive_time=BrokerReceiveTimeV1(
                    (index + 10) * 1_000_000_000, index + 10, "generated-clock"
                ),
                instrument="EURUSD",
                quote=BrokerQuoteV1("EURUSD", bid, ask),
            )
            sequence += 1


def factory(resources: BrokerHostResourcesV1) -> BrokerPluginV1:
    return HealthFixture(resources)
