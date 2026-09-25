"""External callers need only installed public lifecycle/registry APIs."""

from pathlib import Path

from histdatacom.broker_plugin_capabilities import (
    BrokerCapabilityPlanV1,
    BrokerCapabilityWorkflowV1,
    negotiate_broker_capabilities,
)
from histdatacom.broker_plugin_lifecycle import (
    BrokerLifecyclePolicyV1,
    BrokerLifecycleResultV1,
    run_broker_plugin_lifecycle,
    replay_broker_lifecycle,
)
from histdatacom.broker_plugin_registry import discover_broker_plugins
from histdatacom.broker_plugin_policy import (
    BrokerPolicySource,
    BrokerSDKInvocationV1,
    provider_policy_scope,
)


def authorized(plan: BrokerCapabilityPlanV1) -> bool:
    return plan.admitted


def capture(
    directory: Path,
    provider_request: BrokerSDKInvocationV1,
    policy_source: BrokerPolicySource,
) -> BrokerLifecycleResultV1:
    inventory = discover_broker_plugins()
    workflow = BrokerCapabilityWorkflowV1(
        ("configuration_schema", "iter_events", "open_session")
    )
    plan = negotiate_broker_capabilities(
        inventory, workflow, plugin_id="org.example.lifecycle"
    )
    with provider_policy_scope(policy_source):
        result = run_broker_plugin_lifecycle(
            inventory,
            plan,
            {"mode": "finite"},
            (),
            directory,
            authorize=authorized,
            policy=BrokerLifecyclePolicyV1(),
            provider_request=provider_request,
        )
        for record in replay_broker_lifecycle(
            directory, provider_request=provider_request
        ):
            assert record.capture_sequence >= 0
    return result
