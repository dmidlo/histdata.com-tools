"""Strict installed public host-side typing; no source path or private imports."""

from collections.abc import Iterator
from contextlib import contextmanager

from histdatacom.broker_plugin_capabilities import (
    BrokerAdmittedEventV1,
    BrokerCapabilityPlanV1,
    BrokerCapabilityWorkflowV1,
    GatedBrokerPluginV1,
    invoke_authorized_installed_broker_plugin,
    negotiate_broker_capabilities,
)
from histdatacom.broker_plugin_permissions import (
    BrokerPermissionAuthorityV1,
    BrokerPermissionResourcesV1,
    broker_permission_scope,
)
from histdatacom.broker_plugin_policy import (
    BrokerPolicySource,
    BrokerSDKInvocationV1,
    provider_policy_scope,
)
from histdatacom.broker_plugin_registry import BrokerPluginInventoryV1


def preflight(inventory: BrokerPluginInventoryV1) -> BrokerCapabilityPlanV1:
    return negotiate_broker_capabilities(
        inventory,
        BrokerCapabilityWorkflowV1(("metadata",)),
        plugin_id="org.example.capabilities",
    )


@contextmanager
def authorized(
    inventory: BrokerPluginInventoryV1,
    plan: BrokerCapabilityPlanV1,
    provider_request: BrokerSDKInvocationV1,
    policy_source: BrokerPolicySource,
    permission_authority: BrokerPermissionAuthorityV1,
    resources: BrokerPermissionResourcesV1 | None = None,
) -> Iterator[GatedBrokerPluginV1]:
    """The caller supplies declarations and uses the plugin inside this scope."""
    with (
        provider_policy_scope(policy_source),
        broker_permission_scope(permission_authority, resources=resources),
    ):
        yield invoke_authorized_installed_broker_plugin(
            inventory,
            plan,
            authorize=lambda _: True,
            provider_request=provider_request,
        )


def events(invocation: GatedBrokerPluginV1) -> Iterator[BrokerAdmittedEventV1]:
    return invocation.iter_events(max_events=10)
