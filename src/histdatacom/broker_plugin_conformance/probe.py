"""Public installed SDK negative-gate probe with bounded host-owned progress."""

from __future__ import annotations

from dataclasses import dataclass, field

from histdatacom.broker_plugin_capabilities import (
    invoke_authorized_installed_broker_plugin,
)
from histdatacom.broker_plugin_policy import BrokerSDKInvocationV1
from histdatacom.broker_plugin_registry import BrokerPluginInventoryV1
from histdatacom.broker_plugin_security import BrokerPrivateMaterialGuard

from ._wire import parse_conformance_json
from .evidence import BrokerConformanceProbeV1
from .fixture_plugin import SECRET_CANARY

NEGATIVE_PROBES = frozenset(
    (
        "lifecycle.exception",
        "lifecycle.malformed",
        "secrets.redaction",
        "quotes.invalid-spread",
        "quotes.sequence",
        "timestamps.monotonic",
        "sizes.permission-denial",
        "raw.permission-denial",
    )
)


@dataclass
class ProbeProgress:
    stage: str = "factory"
    metadata: str = ""
    binding: str = ""
    session: str = ""
    events: list[str] = field(default_factory=list)

    def artifact(self, candidate_id: str) -> BrokerConformanceProbeV1:
        return BrokerConformanceProbeV1(
            candidate_id,
            self.stage,
            self.metadata,
            self.binding,
            self.session,
            tuple(self.events),
        )


def probe_installed_candidate(
    inventory: BrokerPluginInventoryV1,
    request: BrokerSDKInvocationV1,
    symbols: tuple[str, ...],
    progress: ProbeProgress,
) -> None:
    """No supplied factory, requester, raw plugin object or expected result."""
    plugin = invoke_authorized_installed_broker_plugin(
        inventory,
        request.plan,
        authorize=lambda _: True,
        provider_request=request,
    )
    guard = BrokerPrivateMaterialGuard((SECRET_CANARY,))
    try:
        progress.stage = "metadata"
        text = plugin.metadata.to_json()
        guard.check(text)
        progress.metadata = text
        progress.binding = plugin.binding.to_json()
        progress.stage = "configuration"
        schema = plugin.configuration_schema
        if schema.to_json() != request.configuration_profile.schema_json:
            raise ValueError("configuration schema substituted")
        progress.stage = "open"
        session = plugin.open_session(
            parse_conformance_json(
                request.configuration_profile.public_configuration_json
            )
        )
        guard.check(session.to_json())
        progress.session = session.to_json()
        progress.stage = "instruments"
        plugin.instruments()
        progress.stage = "subscribe"
        plugin.subscribe(symbols)
        progress.stage = "events"
        for event in plugin.iter_events(max_events=128):
            text = event.to_json()
            guard.check(text)
            progress.events.append(text)
        progress.stage = "unsubscribe"
        plugin.unsubscribe(symbols)
        progress.stage = "close"
        plugin.close_session()
        progress.stage = "complete"
    finally:
        try:
            plugin.close_session()
        except Exception:  # noqa: BLE001, S110
            # Preserve the first closed refusal; do not log private cleanup.
            pass
