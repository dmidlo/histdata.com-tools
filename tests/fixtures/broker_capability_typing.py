"""Strict installed public host-side typing; no source path or private imports."""

from collections.abc import Iterator

from histdatacom.broker_plugin_capabilities import (
    BrokerAdmittedEventV1,
    BrokerCapabilityPlanV1,
    BrokerCapabilityWorkflowV1,
    GatedBrokerPluginV1,
    invoke_authorized_installed_broker_plugin,
    negotiate_broker_capabilities,
)
from histdatacom.broker_plugin_registry import BrokerPluginInventoryV1


def preflight(inventory: BrokerPluginInventoryV1) -> BrokerCapabilityPlanV1:
    return negotiate_broker_capabilities(
        inventory,
        BrokerCapabilityWorkflowV1(("metadata",)),
        plugin_id="org.example.capabilities",
    )


def authorized(
    inventory: BrokerPluginInventoryV1, plan: BrokerCapabilityPlanV1
) -> GatedBrokerPluginV1:
    return invoke_authorized_installed_broker_plugin(
        inventory, plan, authorize=lambda _: True
    )


def events(invocation: GatedBrokerPluginV1) -> Iterator[BrokerAdmittedEventV1]:
    return invocation.iter_events(max_events=10)
