"""Strict external public API typing fixture, checked against installed wheel."""

from pathlib import Path
from histdatacom.broker_plugin_capabilities import BrokerCapabilityPlanV1
from histdatacom.broker_plugin_registry import BrokerPluginInventoryV1
from histdatacom.broker_plugin_policy import (
    BrokerPolicySource,
    BrokerSDKInvocationV1,
    provider_policy_scope,
)
from histdatacom.broker_plugin_security import (
    BrokerSecretProvider,
    BrokerSecurityPolicyV1,
    BrokerSecureLifecycleResultV1,
    run_secure_broker_plugin,
)


class ExternalSecrets:
    def resolve(self, handle: str) -> str:
        return "synthetic-fixture-only"


provider: BrokerSecretProvider = ExternalSecrets()


def invoke(
    inventory: BrokerPluginInventoryV1,
    plan: BrokerCapabilityPlanV1,
    policy: BrokerSecurityPolicyV1,
    destination: Path,
    provider_request: BrokerSDKInvocationV1,
    policy_source: BrokerPolicySource,
) -> BrokerSecureLifecycleResultV1:
    with provider_policy_scope(policy_source):
        return run_secure_broker_plugin(
            inventory,
            plan,
            policy,
            {},
            (),
            destination,
            authorize=lambda _: True,
            secret_provider=provider,
            provider_request=provider_request,
        )
