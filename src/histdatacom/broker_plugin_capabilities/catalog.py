"""Frozen capability meanings and exhaustive SDK-v1 operation requirements."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from .contracts import (
    BrokerCapabilityDefinitionV1,
    BrokerOperationV1,
    _Artifact,
)

_DECLARATIONS = (
    (
        "session.v1",
        "Open an independently identified SDK session.",
        "open_session",
    ),
    (
        "events.v1",
        "Iterate ordered SDK events, not necessarily quote events.",
        "iter_events",
    ),
    (
        "quotes.v1",
        "Streaming canonical FX bid/ask quotes with decimal lexemes.",
        "event.quote",
    ),
    (
        "instruments.v1",
        "Bounded exact canonical/provider symbol discovery.",
        "instruments",
    ),
    (
        "instruments.price-increment.v1",
        "Declared positive decimal price increment, not inferred precision.",
        "instrument.price_increment",
    ),
    (
        "timestamps.broker-event.v1",
        "Source broker event timestamp with declared precision.",
        "event.source_time.broker_event",
    ),
    (
        "timestamps.exchange-event.v1",
        "Source exchange event timestamp with declared precision.",
        "event.source_time.exchange_event",
    ),
    (
        "timestamps.adapter-receive.v1",
        "Adapter receive observation, not broker/exchange event time.",
        "event.source_time.adapter_receive",
    ),
    (
        "timestamps.receive.v1",
        "Plugin UTC/monotonic receive observation, never host capture time.",
        "event.receive_time",
    ),
    (
        "heartbeat.v1",
        "Explicit provider/transport heartbeat events, not inferred liveness.",
        "event.heartbeat",
    ),
    (
        "health.v1",
        "Advisory plugin health diagnostics, not host completeness certification.",
        "event.health",
    ),
    (
        "connection.v1",
        "Explicit connected/disconnected events and connection identities.",
        "event.connected.disconnected",
    ),
    (
        "subscriptions.v1",
        "Explicit subscribed/unsubscribed control events.",
        "event.subscribed.unsubscribed",
    ),
    (
        "reconnect.v1",
        "Explicit reconnect diagnostic events, not host retry authority.",
        "event.reconnecting",
    ),
    (
        "gaps.v1",
        "Explicit source/transport/collector gap evidence with optional counts.",
        "event.gap",
    ),
    (
        "clock-corrections.v1",
        "Explicit plugin receive-clock correction evidence, not collector clock authority.",
        "event.clock_correction",
    ),
    (
        "sizes.quoted.v1",
        "Quoted bid/ask size with exact declared unit; not centralized traded volume.",
        "event.quote.bid_size.ask_size.quoted_size",
    ),
    (
        "sizes.broker-specific.v1",
        "Broker-specific bid/ask quantity with explicit unit.",
        "event.quote.bid_size.ask_size.broker_specific",
    ),
    (
        "activity.message-count.v1",
        "Integer message activity count, never traded volume.",
        "event.quote.activity.message_count",
    ),
    (
        "activity.broker.v1",
        "Provider-specific activity measurement with explicit unit.",
        "event.quote.activity.broker_activity",
    ),
    (
        "activity.liquidity-proxy.v1",
        "Explicit liquidity proxy, not observed centralized volume.",
        "event.quote.activity.liquidity_proxy",
    ),
    (
        "raw-hashes.v1",
        "Raw-message SHA-256, size and policy pointer; no retention/right grant.",
        "event.raw_provenance",
    ),
    (
        "metadata.server.v1",
        "Public server class/label, never credential or private account identity.",
        "metadata.public_context.server_label",
    ),
    (
        "metadata.account-class.v1",
        "Public account class, never account number or customer identity.",
        "metadata.public_context.account_class",
    ),
    (
        "metadata.feed-class.v1",
        "Public feed class, never an inferred provider semantic.",
        "metadata.public_context.feed_class",
    ),
    (
        "history.replay.v1",
        "Provider historical replay declaration; SDK v1 has no execution method.",
        "unsupported.history_replay",
    ),
    (
        "history.backfill.v1",
        "Provider historical backfill declaration; SDK v1 has no execution method.",
        "unsupported.history_backfill",
    ),
)
PUBLIC_CONTEXT_NAMESPACE = "org.histdatacom.broker-public-context"
SOURCE_TIME_CAPABILITIES = (
    "timestamps.adapter-receive.v1",
    "timestamps.broker-event.v1",
    "timestamps.exchange-event.v1",
)
SIZE_CAPABILITIES = ("sizes.broker-specific.v1", "sizes.quoted.v1")
ACTIVITY_CAPABILITIES = (
    "activity.broker.v1",
    "activity.liquidity-proxy.v1",
    "activity.message-count.v1",
)
METADATA_CAPABILITIES = (
    "metadata.account-class.v1",
    "metadata.feed-class.v1",
    "metadata.server.v1",
)
EVENT_CAPABILITIES = tuple(
    sorted(item[0] for item in _DECLARATIONS if item[2].startswith("event."))
)


@dataclass(frozen=True, slots=True)
class BrokerCapabilityCatalogV1(_Artifact):
    definitions: tuple[BrokerCapabilityDefinitionV1, ...]
    operations: tuple[BrokerOperationV1, ...]
    version: str = "1.0.0"
    KIND: ClassVar[str] = "catalog"

    def _validate(self) -> None:
        if self.version != "1.0.0":
            raise ValueError("unsupported catalog version")
        names = tuple(item.capability_id for item in self.definitions)
        operations = tuple(item.operation_id for item in self.operations)
        if names != tuple(sorted(set(names))) or operations != tuple(
            sorted(set(operations))
        ):
            raise ValueError("duplicate or unordered catalog")
        if any(
            set(item.required + item.optional) - set(names)
            for item in self.operations
        ):
            raise ValueError("operation references unknown catalog atom")


_OPERATIONS = (
    BrokerOperationV1("metadata", "metadata", optional=METADATA_CAPABILITIES),
    BrokerOperationV1("configuration_schema", "configuration_schema"),
    BrokerOperationV1("open_session", "open_session", ("session.v1",)),
    BrokerOperationV1(
        "instruments",
        "instruments",
        ("instruments.v1",),
        ("instruments.price-increment.v1",),
        prerequisites=("open_session",),
    ),
    BrokerOperationV1(
        "subscribe",
        "subscribe",
        ("instruments.v1", "quotes.v1"),
        prerequisites=("instruments", "open_session"),
    ),
    BrokerOperationV1(
        "unsubscribe",
        "unsubscribe",
        ("quotes.v1",),
        prerequisites=("instruments", "open_session", "subscribe"),
    ),
    BrokerOperationV1(
        "iter_events",
        "iter_events",
        ("events.v1",),
        EVENT_CAPABILITIES,
        prerequisites=("open_session",),
    ),
    BrokerOperationV1("close_session", "close_session"),
    BrokerOperationV1(
        "history_replay",
        "unsupported",
        ("history.replay.v1",),
        executable=False,
    ),
    BrokerOperationV1(
        "history_backfill",
        "unsupported",
        ("history.backfill.v1",),
        executable=False,
    ),
    BrokerOperationV1("offline_capture_replay", "offline", executable=False),
    BrokerOperationV1("legacy_live_capture", "legacy", executable=False),
)
_CATALOG = BrokerCapabilityCatalogV1(
    tuple(
        BrokerCapabilityDefinitionV1(*item) for item in sorted(_DECLARATIONS)
    ),
    tuple(sorted(_OPERATIONS, key=lambda item: item.operation_id)),
)


def broker_capability_catalog() -> BrokerCapabilityCatalogV1:
    """Immutable SDK-v1 policy. New meanings require a new catalog identity."""
    return _CATALOG
