"""Admit native SDK evidence without fabricating fields or legacy mappings."""

from __future__ import annotations

from histdatacom.broker_plugins import (
    BrokerEventKind,
    BrokerEventV1,
    BrokerInstrumentV1,
    BrokerPluginMetadataV1,
)

from .catalog import (
    ACTIVITY_CAPABILITIES,
    PUBLIC_CONTEXT_NAMESPACE,
    SIZE_CAPABILITIES,
    SOURCE_TIME_CAPABILITIES,
)
from .contracts import (
    BrokerAdmittedEventV1,
    BrokerAdmittedInstrumentV1,
    BrokerAdmittedMetadataV1,
    BrokerCapabilityError,
    BrokerCapabilityPlanV1,
    BrokerCapabilityReason,
    BrokerFieldReceiptV1,
    BrokerFieldSupport,
)

_EVENT_CAPABILITIES = {
    BrokerEventKind.CONNECTED: "connection.v1",
    BrokerEventKind.DISCONNECTED: "connection.v1",
    BrokerEventKind.RECONNECTING: "reconnect.v1",
    BrokerEventKind.SUBSCRIBED: "subscriptions.v1",
    BrokerEventKind.UNSUBSCRIBED: "subscriptions.v1",
    BrokerEventKind.QUOTE: "quotes.v1",
    BrokerEventKind.HEARTBEAT: "heartbeat.v1",
    BrokerEventKind.HEALTH: "health.v1",
    BrokerEventKind.GAP: "gaps.v1",
    BrokerEventKind.CLOCK_CORRECTION: "clock-corrections.v1",
}
_SOURCE = {
    "broker_event": "timestamps.broker-event.v1",
    "exchange_event": "timestamps.exchange-event.v1",
    "adapter_receive": "timestamps.adapter-receive.v1",
}
_SIZES = {
    "quoted_size": "sizes.quoted.v1",
    "broker_specific": "sizes.broker-specific.v1",
}
_ACTIVITY = {
    "message_count": "activity.message-count.v1",
    "broker_activity": "activity.broker.v1",
    "liquidity_proxy": "activity.liquidity-proxy.v1",
}
_CONTEXT = {
    "account_class": (
        "metadata.account-class.v1",
        {"demo", "retail", "professional", "institutional", "unknown"},
    ),
    "feed_class": (
        "metadata.feed-class.v1",
        {
            "indicative",
            "executable",
            "consolidated",
            "single_dealer",
            "unknown",
        },
    ),
    "server_label": (
        "metadata.server.v1",
        {"demo", "test", "production", "unknown"},
    ),
}


def _operation(plan: BrokerCapabilityPlanV1, operation: str) -> None:
    if type(plan) is not BrokerCapabilityPlanV1:
        raise BrokerCapabilityError(BrokerCapabilityReason.INVALID_PLAN)
    plan.require_admitted()
    if operation not in plan.workflow.operations and operation not in {
        "metadata",
        "close_session",
    }:
        raise BrokerCapabilityError(
            BrokerCapabilityReason.UNSUPPORTED_OPERATION
        )


def _enabled(plan: BrokerCapabilityPlanV1) -> set[str]:
    return set(plan.required + plan.enabled_optional)


def _field(
    plan: BrokerCapabilityPlanV1,
    field: str,
    actual: str | None,
    alternatives: tuple[str, ...],
    *,
    applicable: bool = True,
) -> BrokerFieldReceiptV1:
    enabled = _enabled(plan)
    available = set(alternatives) & enabled
    required = set(alternatives) & set(plan.required)
    advertised = set(alternatives) & set(
        plan.candidate.registration.capabilities
    )
    if actual is not None:
        if actual not in available or (required and actual not in required):
            raise BrokerCapabilityError(
                BrokerCapabilityReason.CAPABILITY_VIOLATION
            )
        state = BrokerFieldSupport.PRESENT
    elif not applicable:
        state = BrokerFieldSupport.NOT_APPLICABLE
    elif required:
        raise BrokerCapabilityError(BrokerCapabilityReason.CAPABILITY_VIOLATION)
    elif available:
        state = BrokerFieldSupport.NOT_REPORTED
    elif advertised:
        state = BrokerFieldSupport.NOT_REQUESTED
    else:
        state = BrokerFieldSupport.UNSUPPORTED
    # A group receipt identifies the exact observed capability when present;
    # the generic group token otherwise cannot claim a fabricated semantic.
    capability = actual if actual is not None else field + ".v1"
    return BrokerFieldReceiptV1(field, capability, state)


def validate_broker_metadata(
    plan: BrokerCapabilityPlanV1, metadata: BrokerPluginMetadataV1
) -> BrokerAdmittedMetadataV1:
    _operation(plan, "metadata")
    try:
        if type(metadata) is not BrokerPluginMetadataV1:
            raise ValueError
        metadata = BrokerPluginMetadataV1.from_json(metadata.to_json())
        registration = plan.candidate.registration
        if (
            metadata.plugin_id != registration.plugin_id
            or metadata.plugin_version != registration.plugin_version
            or not registration.supports_sdk(metadata.sdk_version)
        ):
            raise BrokerCapabilityError(BrokerCapabilityReason.RUNTIME_IDENTITY)
        context: dict[str, object] = {}
        for extension in metadata.extensions:
            if extension.namespace == PUBLIC_CONTEXT_NAMESPACE:
                context = extension.payload()
        if set(context) - set(_CONTEXT):
            raise ValueError
        receipts = []
        for field, (capability, permitted) in sorted(_CONTEXT.items()):
            value = context.get(field)
            if field in context and (
                type(value) is not str or value not in permitted
            ):
                raise ValueError
            receipts.append(
                _field(
                    plan,
                    field,
                    capability if field in context else None,
                    (capability,),
                )
            )
        return BrokerAdmittedMetadataV1(
            plan.artifact_id, metadata, tuple(receipts)
        )
    except BrokerCapabilityError:
        raise
    except Exception:
        raise BrokerCapabilityError(
            BrokerCapabilityReason.CAPABILITY_VIOLATION
        ) from None


def validate_broker_instrument(
    plan: BrokerCapabilityPlanV1, instrument: BrokerInstrumentV1
) -> BrokerAdmittedInstrumentV1:
    _operation(plan, "instruments")
    try:
        if type(instrument) is not BrokerInstrumentV1:
            raise ValueError
        instrument = BrokerInstrumentV1.from_json(instrument.to_json())
        atom = "instruments.price-increment.v1"
        receipt = _field(
            plan,
            "price_increment",
            atom if instrument.price_increment is not None else None,
            (atom,),
        )
        return BrokerAdmittedInstrumentV1(
            plan.artifact_id, instrument, (receipt,)
        )
    except BrokerCapabilityError:
        raise
    except Exception:
        raise BrokerCapabilityError(
            BrokerCapabilityReason.CAPABILITY_VIOLATION
        ) from None


def validate_broker_capability_event(
    plan: BrokerCapabilityPlanV1, event: BrokerEventV1
) -> BrokerAdmittedEventV1:
    _operation(plan, "iter_events")
    try:
        if type(event) is not BrokerEventV1:
            raise ValueError
        event = BrokerEventV1.from_json(event.to_json())
        if _EVENT_CAPABILITIES[event.kind] not in _enabled(plan):
            raise BrokerCapabilityError(
                BrokerCapabilityReason.CAPABILITY_VIOLATION
            )
        quote = event.quote
        fields = [
            _field(
                plan,
                "source_time",
                (
                    None
                    if event.source_time is None
                    else _SOURCE[event.source_time.semantics.value]
                ),
                SOURCE_TIME_CAPABILITIES,
                applicable=quote is not None,
            ),
            _field(
                plan,
                "receive_time",
                (
                    "timestamps.receive.v1"
                    if event.receive_time is not None
                    else None
                ),
                ("timestamps.receive.v1",),
            ),
            _field(
                plan,
                "raw_provenance",
                "raw-hashes.v1" if event.raw_provenance is not None else None,
                ("raw-hashes.v1",),
                applicable=quote is not None,
            ),
        ]
        for side in ("bid_size", "ask_size"):
            size = None if quote is None else getattr(quote, side)
            fields.append(
                _field(
                    plan,
                    side,
                    None if size is None else _SIZES[size.semantics.value],
                    SIZE_CAPABILITIES,
                    applicable=quote is not None,
                )
            )
        activity = None if quote is None else quote.activity
        fields.append(
            _field(
                plan,
                "activity",
                (
                    None
                    if activity is None
                    else _ACTIVITY[activity.semantics.value]
                ),
                ACTIVITY_CAPABILITIES,
                applicable=quote is not None,
            )
        )
        return BrokerAdmittedEventV1(
            plan.artifact_id,
            event,
            tuple(sorted(fields, key=lambda item: item.field)),
        )
    except BrokerCapabilityError:
        raise
    except Exception:
        raise BrokerCapabilityError(
            BrokerCapabilityReason.CAPABILITY_VIOLATION
        ) from None


def verify_broker_admitted_event(
    artifact: BrokerAdmittedEventV1, plan: BrokerCapabilityPlanV1
) -> None:
    """Restored receipt bytes alone are not a proof of capability admission."""
    if (
        type(artifact) is not BrokerAdmittedEventV1
        or validate_broker_capability_event(plan, artifact.event).to_json()
        != artifact.to_json()
    ):
        raise BrokerCapabilityError(BrokerCapabilityReason.CAPABILITY_VIOLATION)
