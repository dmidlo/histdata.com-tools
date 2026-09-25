"""Strict external public API typing fixture, checked against installed wheel."""

from pathlib import Path

from histdatacom.broker_plugin_capabilities import BrokerCapabilityPlanV1
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
from histdatacom.broker_plugin_security import (
    BrokerSecureLifecycleResultV1,
    BrokerSecurityPolicyV1,
    run_secure_broker_plugin,
)


def invoke(
    inventory: BrokerPluginInventoryV1,
    plan: BrokerCapabilityPlanV1,
    policy: BrokerSecurityPolicyV1,
    destination: Path,
    provider_request: BrokerSDKInvocationV1,
    policy_source: BrokerPolicySource,
    permission_authority: BrokerPermissionAuthorityV1,
    resources: BrokerPermissionResourcesV1 | None = None,
) -> BrokerSecureLifecycleResultV1:
    with (
        provider_policy_scope(policy_source),
        broker_permission_scope(permission_authority, resources=resources),
    ):
        return run_secure_broker_plugin(
            inventory,
            plan,
            policy,
            {},
            (),
            destination,
            authorize=lambda _: True,
            provider_request=provider_request,
        )
