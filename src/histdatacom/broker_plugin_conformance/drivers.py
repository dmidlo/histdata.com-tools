"""Metadata-only exact candidate/permission binding for declarative fixtures."""

from __future__ import annotations

from dataclasses import replace
from urllib.parse import urlsplit

from histdatacom.broker_plugin_permissions import (
    BrokerPermissionError,
    read_installed_broker_permissions,
)
from histdatacom.broker_plugin_registry import (
    BrokerPluginInventoryV1,
    discover_broker_plugins,
)
from histdatacom.broker_plugins import BROKER_PLUGIN_SUPPORTED_SDK_VERSIONS

from .catalog import broker_conformance_catalog, required_cases, verify_plan
from .contracts import (
    BrokerConformanceDriverV1,
    BrokerConformancePlanV1,
    BrokerConformanceProfile,
    BrokerConformanceSubject,
)


def inspect_broker_conformance_driver(
    inventory: BrokerPluginInventoryV1,
    driver: BrokerConformanceDriverV1,
) -> BrokerConformanceDriverV1:
    """No plugin import, factory, transport effect or automatic fallback."""
    if (
        type(inventory) is not BrokerPluginInventoryV1
        or type(driver) is not BrokerConformanceDriverV1
    ):
        raise ValueError("exact conformance inputs required")
    matches = [
        item
        for item in inventory.candidates
        if item.artifact_id == driver.candidate_id
    ]
    if len(matches) != 1:
        raise ValueError("conformance candidate missing or ambiguous")
    candidate = matches[0]
    permissions = read_installed_broker_permissions(
        candidate, sdk_version=inventory.sdk_version
    )
    if (
        permissions.artifact_id != driver.permission_manifest_id
        or driver.provider_id not in candidate.registration.provider_ids
    ):
        raise ValueError("conformance driver binding mismatch")
    # No override of a provider origin and no arbitrary private transport hook.
    # Optional production endpoints also refuse: a synthetic runner must not
    # generate authority over them simply because a particular case is quiet.
    for endpoint in permissions.endpoints:
        parsed = urlsplit(endpoint.origin)
        if parsed.hostname not in {"127.0.0.1", "::1"}:
            raise ValueError("conformance requires declared loopback resources")
    known = {item.scenario_id for item in broker_conformance_catalog().cases}
    if {item.scenario_id for item in driver.scenarios} - known:
        raise ValueError("unknown conformance fixture scenario")
    return BrokerConformanceDriverV1.from_json(driver.to_json())


def resolve_conformance_inventory(
    driver: BrokerConformanceDriverV1,
) -> BrokerPluginInventoryV1:
    """Recover the chosen SDK from the exact retained permission declaration.

    A V1 driver already content-addresses its permission manifest, whose SDK
    field is exact. Discovery is metadata-only; no alternate plugin is run.
    """
    inventory = discover_broker_plugins()
    candidates = tuple(
        item
        for item in inventory.candidates
        if item.artifact_id == driver.candidate_id
    )
    if inventory.diagnostics or len(candidates) != 1:
        raise ValueError("conformance candidate missing or ambiguous")
    selected: list[str] = []
    for sdk_version in BROKER_PLUGIN_SUPPORTED_SDK_VERSIONS:
        if not candidates[0].registration.supports_sdk(sdk_version):
            continue
        try:
            manifest = read_installed_broker_permissions(
                candidates[0], sdk_version=sdk_version
            )
        except BrokerPermissionError:
            continue
        if manifest.artifact_id == driver.permission_manifest_id:
            selected.append(sdk_version)
    if len(selected) != 1:
        raise ValueError(
            "conformance SDK lacks an exact installed permission match"
        )
    result = replace(inventory, sdk_version=selected[0])
    inspect_broker_conformance_driver(result, driver)
    return result


def plan_broker_conformance(
    inventory: BrokerPluginInventoryV1,
    driver: BrokerConformanceDriverV1,
    *,
    profile: BrokerConformanceProfile,
    capabilities: tuple[str, ...],
    subject: BrokerConformanceSubject = BrokerConformanceSubject.CANDIDATE,
    case_timeout_ms: int = 300_000,
) -> BrokerConformancePlanV1:
    driver = inspect_broker_conformance_driver(inventory, driver)
    catalog = broker_conformance_catalog()
    cases = {
        case.case_id
        for case in catalog.cases
        if case.capability == "common" and profile.value in case.profiles
    }
    for capability in capabilities:
        cases.update(required_cases(profile, capability))
    plan = BrokerConformancePlanV1(
        driver,
        catalog.artifact_id,
        profile,
        capabilities,
        tuple(sorted(cases)),
        subject,
        case_timeout_ms,
    )
    verify_plan(plan)
    return plan
