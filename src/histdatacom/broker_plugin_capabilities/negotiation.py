"""Pure deterministic preflight, distinct from authorization and execution."""

from __future__ import annotations

from histdatacom.broker_plugin_registry import (
    BrokerPluginInventoryV1,
    select_broker_plugin,
)

from .catalog import (
    ACTIVITY_CAPABILITIES,
    SIZE_CAPABILITIES,
    SOURCE_TIME_CAPABILITIES,
    broker_capability_catalog,
)
from .contracts import (
    BrokerCapabilityError,
    BrokerCapabilityPlanV1,
    BrokerCapabilityReason,
    BrokerCapabilityWorkflowV1,
)


def _derived(
    workflow: BrokerCapabilityWorkflowV1, advertised: tuple[str, ...]
) -> dict[str, tuple[str, ...]]:
    catalog = broker_capability_catalog()
    operations = {item.operation_id: item for item in catalog.operations}
    known = {item.capability_id for item in catalog.definitions}
    required = set(workflow.required)
    optional = set(workflow.optional)
    unsupported = []
    # Runtime identity always examines metadata; supported public context is
    # validated even if callers did not explicitly request its property again.
    optional.update(operations["metadata"].optional)
    for name in workflow.operations:
        operation = operations.get(name)
        if operation is None or not operation.executable:
            unsupported.append(name)
        if operation is not None:
            required.update(operation.required)
            optional.update(operation.optional)
            unsupported.extend(
                set(operation.prerequisites) - set(workflow.operations)
            )
    # Quote acquisition requires an explicit instrument/subscription workflow.
    # Optional provider enrichments never add preflight dependencies; a health-
    # only event workflow remains valid and rejects unsolicited quote outputs.
    if "iter_events" in workflow.operations and "quotes.v1" in required:
        unsupported.extend(
            {"instruments", "subscribe"} - set(workflow.operations)
        )
    optional -= required
    incompatible = set()
    for alternatives in (
        SOURCE_TIME_CAPABILITIES,
        SIZE_CAPABILITIES,
        ACTIVITY_CAPABILITIES,
    ):
        conflict = required & set(alternatives)
        if len(conflict) > 1:
            incompatible.update(conflict)
    return {
        "required": tuple(sorted(required)),
        "optional": tuple(sorted(optional)),
        "missing_required": tuple(sorted(required - set(advertised))),
        "unsupported_required": tuple(
            sorted((required - known) | incompatible)
        ),
        "enabled_optional": tuple(sorted(optional & set(advertised) & known)),
        "unsupported_operations": tuple(sorted(set(unsupported))),
    }


def _verify_plan_fields(plan: BrokerCapabilityPlanV1) -> None:
    if plan.catalog_id != broker_capability_catalog().artifact_id:
        raise ValueError("unknown capability policy")
    expected = _derived(plan.workflow, plan.candidate.registration.capabilities)
    if any(getattr(plan, name) != values for name, values in expected.items()):
        raise ValueError("plan decisions differ from catalog")


def negotiate_broker_capabilities(
    inventory: BrokerPluginInventoryV1,
    workflow: BrokerCapabilityWorkflowV1,
    *,
    plugin_id: str | None = None,
    provider_id: str | None = None,
    version_constraint: str = "",
) -> BrokerCapabilityPlanV1:
    """Return inspectable admission/refusal without loading plugin code."""
    try:
        if type(workflow) is not BrokerCapabilityWorkflowV1:
            raise ValueError
        workflow = BrokerCapabilityWorkflowV1.from_json(workflow.to_json())
        inventory = BrokerPluginInventoryV1.from_json(inventory.to_json())
        candidate = select_broker_plugin(
            inventory,
            plugin_id=plugin_id,
            provider_id=provider_id,
            version_constraint=version_constraint,
        )
        return BrokerCapabilityPlanV1(
            inventory.artifact_id,
            candidate,
            broker_capability_catalog().artifact_id,
            workflow,
            **_derived(workflow, candidate.registration.capabilities),
        )
    except Exception:
        raise BrokerCapabilityError(
            BrokerCapabilityReason.INVALID_PLAN
        ) from None


def verify_broker_capability_plan(
    plan: BrokerCapabilityPlanV1, inventory: BrokerPluginInventoryV1
) -> None:
    """Recompute from the full authoritative inventory, including ambiguity."""
    try:
        if type(plan) is not BrokerCapabilityPlanV1:
            raise ValueError
        expected = negotiate_broker_capabilities(
            inventory,
            plan.workflow,
            plugin_id=plan.candidate.registration.plugin_id,
            version_constraint="=="
            + plan.candidate.registration.plugin_version,
        )
        if expected.to_json() != plan.to_json():
            raise ValueError
    except Exception:
        raise BrokerCapabilityError(
            BrokerCapabilityReason.INVALID_PLAN
        ) from None
