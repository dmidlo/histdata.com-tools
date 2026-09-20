"""Strict external public API typing fixture, checked against installed wheel."""

from pathlib import Path
from histdatacom.broker_plugin_capabilities import BrokerCapabilityPlanV1
from histdatacom.broker_plugin_registry import BrokerPluginInventoryV1
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
) -> BrokerSecureLifecycleResultV1:
    return run_secure_broker_plugin(
        inventory,
        plan,
        policy,
        {},
        (),
        destination,
        authorize=lambda _: True,
        secret_provider=provider,
    )
