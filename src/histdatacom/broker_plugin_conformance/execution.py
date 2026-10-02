"""One exact installed candidate execution in a caller-owned fresh interpreter."""

from __future__ import annotations

import time
from dataclasses import replace
from pathlib import Path

from histdatacom.broker_plugin_capabilities import (
    BrokerCapabilityError,
    BrokerCapabilityWorkflowV1,
    negotiate_broker_capabilities,
)
from histdatacom.broker_plugin_health import BrokerHostHealthPolicyV1
from histdatacom.broker_plugin_lifecycle import BrokerLifecyclePolicyV1
from histdatacom.broker_plugin_permissions import (
    BrokerPermissionContextV1,
    BrokerPermissionError,
    BrokerPermissionRevocationV1,
    broker_permission_scope,
    read_installed_broker_permissions,
)
from histdatacom.broker_plugin_policy import (
    BrokerPolicyError,
    BrokerPolicyOperation,
    BrokerProviderConfigurationV1,
    BrokerProviderOutputContractV1,
    BrokerSDKInvocationV1,
    provider_policy_scope,
    require_provider_operation,
)
from histdatacom.broker_plugin_registry import discover_broker_plugins
from histdatacom.broker_plugin_security import (
    BrokerSecurityError,
    BrokerSecurityMode,
    BrokerSecurityPolicyV1,
    BrokerTrustTier,
    run_secure_broker_plugin,
    run_trusted_broker_plugin,
)
from histdatacom.broker_plugins import BrokerEventKind

from ._wire import parse_conformance_json
from .authority import generated_authority
from .catalog import (
    HOST_ADMISSION_CASES,
    broker_conformance_catalog,
    verify_plan,
)
from .contracts import BrokerConformancePlanV1, BrokerConformanceProfile
from .drivers import inspect_broker_conformance_driver
from .evidence import BrokerConformanceEvidenceV1
from .fixture_plugin import SECRET_CANARY
from .probe import NEGATIVE_PROBES, ProbeProgress, probe_installed_candidate
from .reference import reference_host_fault


def execute_conformance_case(
    plan: BrokerConformancePlanV1,
    case_id: str,
    directory: Path,
    *,
    trusted_equivalence: bool = False,
) -> BrokerConformanceEvidenceV1:
    """Internal worker entry. Never accepts a factory, verdict or requester."""
    verify_plan(plan)
    if case_id not in plan.case_ids:
        raise ValueError("unplanned conformance case")
    if trusted_equivalence and case_id != "execution.equivalence":
        raise ValueError("backend substitution outside equivalence case")
    case = next(
        item
        for item in broker_conformance_catalog().cases
        if item.case_id == case_id
    )
    scenario = next(
        item
        for item in plan.driver.scenarios
        if item.scenario_id == case.scenario_id
    )
    started = time.time_ns()
    inventory = discover_broker_plugins()
    inspect_broker_conformance_driver(inventory, plan.driver)
    candidate = next(
        item
        for item in inventory.candidates
        if item.artifact_id == plan.driver.candidate_id
    )
    workflow = BrokerCapabilityWorkflowV1(
        tuple(
            sorted(
                (
                    "configuration_schema",
                    "open_session",
                    "instruments",
                    "subscribe",
                    "iter_events",
                    "unsubscribe",
                    "close_session",
                )
            )
        ),
        optional=tuple(
            sorted(
                set(candidate.registration.capabilities)
                - {"session.v1", "events.v1", "instruments.v1"}
            )
        ),
    )
    negotiated = negotiate_broker_capabilities(
        inventory, workflow, plugin_id=candidate.registration.plugin_id
    )
    manifest = read_installed_broker_permissions(candidate)
    if not negotiated.admitted:
        return BrokerConformanceEvidenceV1(
            plan.artifact_id,
            case_id,
            candidate.artifact_id,
            started,
            time.time_ns(),
            inventory.to_json(),
            "",
            manifest.to_json(),
            "",
            "",
            "none",
            refusal_family="capability",
            refusal_reason="unsupported_capability",
            capability_plan_json=negotiated.to_json(),
        )
    profile = BrokerProviderConfigurationV1(
        plan.driver.provider_id,
        "generated-conformance",
        plan.driver.configuration_schema_json,
        scenario.configuration_json,
    )
    request = BrokerSDKInvocationV1(
        negotiated,
        profile,
        BrokerProviderOutputContractV1(
            "sdk-v1",
            tuple(sorted(item.value for item in BrokerEventKind)),
            allow_raw_hashes="raw-hashes.v1" in negotiated.enabled_optional,
            allow_opaque_metadata=True,
        ),
    )
    manifest = read_installed_broker_permissions(candidate)
    omitted: tuple[str, ...] = ()
    if case_id == "permissions.required-denial":
        omitted = manifest.required_atoms
    elif case_id == "sizes.permission-denial":
        omitted = ("emit:sizes",)
    elif case_id == "raw.permission-denial":
        omitted = ("raw_payload:emit",)
    denied = {
        "policy.invoke-denial": BrokerPolicyOperation.INVOKE,
        "policy.retention-denial": BrokerPolicyOperation.RETAIN_LOCAL,
        "policy.publication-denial": BrokerPolicyOperation.PUBLISH,
    }.get(case_id)
    authority = generated_authority(
        request, manifest, omitted=omitted, denied=denied
    )
    isolated = (
        plan.profile is not BrokerConformanceProfile.TRUSTED
        and not trusted_equivalence
    )
    native_family = "isolated" if isolated else "trusted"
    if case_id in HOST_ADMISSION_CASES:
        native_family = "host_admission"
    security = BrokerSecurityPolicyV1(
        candidate.artifact_id,
        BrokerTrustTier.REVIEWED,
        (
            BrokerSecurityMode.KERNEL_ISOLATED
            if isolated
            else BrokerSecurityMode.TRUSTED_IN_PROCESS
        ),
    )
    native_json = permission_json = health_json = provenance_json = ""
    refusal_family = refusal_reason = "none"
    refusal_decision = ""
    progress = ProbeProgress()
    probed = False
    event_failure = "none"
    try:
        with (
            reference_host_fault(plan, case_id),
            provider_policy_scope(authority.policy),
            broker_permission_scope(
                authority.authority, resources=authority.resources
            ),
        ):
            if case_id == "permissions.revocation":
                previous = authority.permissions.context
                grant = previous.grants[0]
                authority.permissions.context = BrokerPermissionContextV1(
                    previous.grants,
                    (
                        BrokerPermissionRevocationV1(
                            grant.artifact_id,
                            time.time_ns(),
                            "generated-conformance-revocation",
                        ),
                    ),
                    1,
                )
            if case_id == "policy.publication-denial":
                require_provider_operation(
                    request, BrokerPolicyOperation.PUBLISH
                )
            if case_id in NEGATIVE_PROBES:
                native_family = "trusted_probe"
                probed = True
                probe_installed_candidate(
                    inventory, request, plan.driver.symbols, progress
                )
            elif isolated:
                cancellation_started = time.monotonic()
                lifecycle = BrokerLifecyclePolicyV1(
                    queue_items=2,
                    max_events=128,
                    startup_timeout_ms=30_000,
                    run_timeout_ms=(
                        240_000 if case_id == "lifecycle.reconnect" else 120_000
                    ),
                    acknowledgement_timeout_ms=10_000,
                    shutdown_timeout_ms=100,
                    delivery_retries=0,
                    retry_delays_ms=(
                        (0,) if case_id == "lifecycle.reconnect" else ()
                    ),
                )
                if case_id == "queue.overflow":
                    lifecycle = replace(
                        lifecycle,
                        queue_items=1,
                        acknowledgement_timeout_ms=10,
                        delivery_retries=8,
                    )
                result = run_secure_broker_plugin(
                    inventory,
                    negotiated,
                    security,
                    parse_conformance_json(scenario.configuration_json),
                    plan.driver.symbols,
                    directory / "native",
                    authorize=lambda _: True,
                    provider_request=request,
                    private_identifiers=(SECRET_CANARY,),
                    lifecycle_policy=lifecycle,
                    cancellation=lambda: case_id == "lifecycle.cancellation"
                    and time.monotonic() - cancellation_started > 45,
                    health_policy=BrokerHostHealthPolicyV1(
                        stale_after_ns=(
                            50_000_000
                            if case_id == "quotes.stale"
                            else 1_000_000_000
                        ),
                        max_persistence_p95_ns=120_000_000_000,
                        max_heartbeat_gap_ns=(
                            50_000_000
                            if case_id == "health.false-delay"
                            else None
                        ),
                        bucket_width_ns=180_000_000_000,
                    ),
                )
                native_json = result.native.manifest.to_json()
                permission_json = result.native.permissions.to_json()
                health_json = result.native.health.to_json()
                provenance_json = result.native.provenance.to_json()
            else:
                trusted = run_trusted_broker_plugin(
                    inventory,
                    negotiated,
                    security,
                    parse_conformance_json(scenario.configuration_json),
                    plan.driver.symbols,
                    authorize=lambda _: True,
                    provider_request=request,
                    private_identifiers=(SECRET_CANARY,),
                    max_events=128,
                )
                native_json = trusted.receipt.to_json()
                permission_json = trusted.permissions.to_json()
    except BrokerPermissionError as error:
        refusal_family, refusal_reason = "permission", error.reason
        if error.decision is not None:
            refusal_decision = error.decision.to_json()
    except BrokerPolicyError as error:
        refusal_family, refusal_reason = "policy", "provider_refused"
        if error.decision is not None:
            refusal_decision = error.decision.to_json()
    except BrokerSecurityError as error:
        refusal_family, refusal_reason = "security", error.reason.value
    except BrokerCapabilityError as error:
        refusal_family, refusal_reason = "capability", error.reason.value
        if error.event_failure is not None:
            event_failure = error.event_failure.value
    except Exception:  # noqa: BLE001 - never retain private plugin exceptions.
        # No exception repr, configuration, plugin print or traceback becomes a
        # report. Unexpected execution is ERROR, never a successful refusal.
        refusal_family, refusal_reason = "execution", "unexpected_failure"
    return BrokerConformanceEvidenceV1(
        plan.artifact_id,
        case_id,
        candidate.artifact_id,
        started,
        time.time_ns(),
        inventory.to_json(),
        request.to_json(),
        manifest.to_json(),
        authority.permissions.context.to_json(),
        authority.policy.context.to_json(),
        native_family,
        native_json,
        permission_json,
        health_json,
        provenance_json,
        refusal_family,
        refusal_reason,
        refusal_decision,
        progress.artifact(candidate.artifact_id).to_json() if probed else "",
        event_failure,
        negotiated.to_json(),
    )
