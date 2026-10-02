"""Bundled generated plugin source, copied into an independent fixture wheel.

This module imports only the public plugin SDK and the standard library. The
wheel builder appends a frozen FAULT constant: a different fault is different
installed code, never a verdict supplied by the declarative driver.
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Iterator, Mapping
from typing import cast
from uuid import uuid4

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
    BrokerRawProvenanceV1,
    BrokerReasonCode,
    BrokerReceiveTimeV1,
    BrokerSessionV1,
    BrokerSizeSemantics,
    BrokerSizeV1,
    BrokerSourceTimeSemantics,
    BrokerSourceTimeV1,
)

FAULT = "none"
SECRET_CANARY = "generated-conformance-secret-not-for-retention"


class ConformanceFixture:
    def __init__(self, resources: BrokerHostResourcesV1) -> None:
        self.resources = resources
        self.mode = "finite"
        self.closed = False

    @property
    def metadata(self) -> BrokerPluginMetadataV1:
        return BrokerPluginMetadataV1(
            "org.histdatacom.conformance-fixture",
            "1.0.0",
            "Generated conformance fixture",
        )

    @property
    def configuration_schema(self) -> BrokerConfigurationSchemaV1:
        return BrokerConfigurationSchemaV1(
            (
                BrokerConfigurationFieldV1(
                    "mode",
                    BrokerConfigurationType.STRING,
                    "Canonical generated scenario",
                ),
            )
        )

    def open_session(
        self, configuration: Mapping[str, object]
    ) -> BrokerSessionV1:
        self.mode = cast(str, configuration["mode"])
        if self.mode == "exception":
            raise RuntimeError("generated-plugin-exception")
        if FAULT == "secret-leak" or self.mode == "secret-leak":
            raise RuntimeError(SECRET_CANARY)
        if self.mode == "open-block":
            time.sleep(300)
        reused = FAULT in {
            "reused-session-nonce",
            "reused-session-nonce-changing-time",
        }
        opened_at = (
            time.time_ns()
            if FAULT == "reused-session-nonce-changing-time"
            else 100
        )
        return BrokerSessionV1(
            self.metadata.artifact_id,
            "a" * 32 if reused else uuid4().hex,
            opened_at,
            "generated-clock",
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
        if symbols != ("EURUSD",):
            raise ValueError("unknown generated instrument")

    def unsubscribe(
        self, session: BrokerSessionV1, symbols: tuple[str, ...]
    ) -> None:
        if symbols != ("EURUSD",):
            raise ValueError("unknown generated instrument")

    def close_session(self, session: BrokerSessionV1) -> None:
        if self.mode == "close-block":
            time.sleep(300)
        self.closed = True

    def iter_events(self, session: BrokerSessionV1) -> Iterator[BrokerEventV1]:
        if self.mode == "next-block":
            time.sleep(300)
        if self.mode == "malformed" or FAULT == "malformed-event":
            yield cast(BrokerEventV1, {"untyped": "generated"})
            return
        sequence = 0
        if FAULT == "false-health" or self.mode in {"finite", "honest-health"}:
            yield BrokerEventV1(
                session.artifact_id,
                sequence,
                BrokerEventKind.HEALTH,
                "generated-connection",
                diagnostic=BrokerDiagnosticV1(
                    BrokerReasonCode.HEALTHY,
                    BrokerDiagnosticSeverity.INFO,
                    "Generated advisory health, never host authority",
                ),
            )
            sequence += 1
        if self.mode in {"gap", "healthy-gap"}:
            yield BrokerEventV1(
                session.artifact_id,
                sequence,
                BrokerEventKind.GAP,
                "generated-connection",
                gap=BrokerGapV1(BrokerGapScope.SOURCE, 100, 200, 5),
                diagnostic=BrokerDiagnosticV1(
                    BrokerReasonCode.SOURCE_GAP,
                    BrokerDiagnosticSeverity.WARNING,
                    "Generated source-gap evidence",
                ),
            )
            sequence += 1
        count = (
            4 if self.mode == "burst" else 96 if self.mode == "overflow" else 3
        )
        offsets = (
            (1, 1, 2)
            if self.mode in {"duplicate", "stale", "healthy-duplicate"}
            or FAULT == "reconnect-duplication"
            else (3, 1, 2) if self.mode == "healthy-reorder" else (1, 2, 3)
        )
        for index in range(count):
            if self.mode in {"healthy-delay", "stale"} and index == 1:
                time.sleep(0.15)
            if self.mode in {"heartbeat", "healthy-delay"}:
                yield BrokerEventV1(
                    session.artifact_id,
                    sequence,
                    BrokerEventKind.HEARTBEAT,
                    "generated-connection",
                )
                sequence += 1
            offset = offsets[index % 3]
            bid, ask = "1.10000", "1.20000"
            if self.mode == "stale":
                bid = f"1.1000{index}"
            if FAULT == "nondeterministic-replay":
                bid = "1.1" + str(time.time_ns())
            size = (
                BrokerSizeV1("2", BrokerSizeSemantics.QUOTED_SIZE, "base-units")
                if self.mode == "sizes" or FAULT == "fabricated-optional"
                else None
            )
            raw = (
                BrokerRawProvenanceV1(
                    hashlib.sha256(b"generated").hexdigest(),
                    9,
                    "application/json",
                    "generated-no-provider-authority",
                )
                if self.mode == "raw"
                else None
            )
            # This receive-side health scenario makes no source-clock claim.
            # Fixed source vectors belong to the separate timestamp/replay
            # scenarios: their one-second spacing is not the physical pacing
            # of fresh authorization and durable native persistence.
            receive = BrokerReceiveTimeV1(
                (index + 10) * 1_000_000_000,
                1 if self.mode == "bad-clock" and index == 1 else index + 10,
                "generated-clock",
            )
            source: BrokerSourceTimeV1 | None = BrokerSourceTimeV1(
                offset * 1_000_000_000,
                1,
                BrokerSourceTimeSemantics.BROKER_EVENT,
            )
            if self.mode == "honest-health":
                receive = BrokerReceiveTimeV1(
                    time.time_ns(), time.monotonic_ns(), "generated-clock"
                )
                source = None
            event = BrokerEventV1(
                session.artifact_id,
                sequence
                + (1 if self.mode == "bad-sequence" and index == 1 else 0),
                BrokerEventKind.QUOTE,
                "generated-connection",
                source_time=source,
                receive_time=receive,
                instrument="EURUSD",
                quote=BrokerQuoteV1(
                    "EURUSD", bid, ask, bid_size=size, ask_size=size
                ),
                raw_provenance=raw,
            )
            if self.mode == "invalid-spread":
                # A deliberately malformed returned object, not an exception
                # raised by our own SDK constructor before the host sees it.
                object.__setattr__(event.quote, "bid", "1.3")
            yield event
            sequence += 1
        if self.mode == "reconnect" or FAULT == "reconnect-duplication":
            yield BrokerEventV1(
                session.artifact_id,
                sequence,
                BrokerEventKind.DISCONNECTED,
                "generated-connection",
            )


def factory(resources: BrokerHostResourcesV1) -> BrokerPluginV1:
    return ConformanceFixture(resources)
