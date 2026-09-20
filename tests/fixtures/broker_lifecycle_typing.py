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


def authorized(plan: BrokerCapabilityPlanV1) -> bool:
    return plan.admitted


def capture(directory: Path) -> BrokerLifecycleResultV1:
    inventory = discover_broker_plugins()
    workflow = BrokerCapabilityWorkflowV1(
        ("configuration_schema", "iter_events", "open_session")
    )
    plan = negotiate_broker_capabilities(
        inventory, workflow, plugin_id="org.example.lifecycle"
    )
    result = run_broker_plugin_lifecycle(
        inventory,
        plan,
        {"mode": "finite"},
        (),
        directory,
        authorize=authorized,
        policy=BrokerLifecyclePolicyV1(),
    )
    for record in replay_broker_lifecycle(directory):
        assert record.capture_sequence >= 0
    return result
