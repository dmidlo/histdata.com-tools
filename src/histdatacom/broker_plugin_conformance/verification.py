"""Native-evidence replay and fixed conformance predicates, not driver verdicts."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass, replace
from decimal import Decimal
from pathlib import Path

from histdatacom.broker_plugin_capabilities import BrokerAdmittedEventV1
from histdatacom.broker_plugin_health import BrokerHostHealthAuditV1
from histdatacom.broker_plugin_lifecycle import (
    BrokerLifecycleCompletion,
    BrokerLifecycleManifestV1,
    BrokerLifecycleReason,
    BrokerLifecycleRecordV1,
    BrokerLifecycleSessionV1,
    BrokerLifecycleState,
    BrokerLifecycleTransitionV1,
    replay_broker_lifecycle,
)
from histdatacom.broker_plugin_permissions import (
    BrokerPermissionBindingV1,
    BrokerPermissionContextV1,
    BrokerPermissionDecisionV1,
    BrokerPermissionExecutionV1,
    BrokerPermissionManifestV1,
    decide_broker_permissions,
    verify_permission_execution,
)
from histdatacom.broker_plugin_policy import (
    BrokerPolicyContextV1,
    BrokerPolicyOperation,
    BrokerSDKInvocationV1,
    provider_policy_scope,
    sdk_invocation_binding,
)
from histdatacom.broker_plugin_provenance import (
    BrokerProvenanceCheckpointV1,
    BrokerProvenanceHeaderV1,
    BrokerProvenanceLinkV1,
    BrokerProvenanceSealV1,
    BrokerProvenanceVerificationReason,
    BrokerProvenanceVerificationV1,
    verify_provenance_chain,
)
from histdatacom.broker_plugin_provenance.native import (
    read_lifecycle_capture_provenance,
)
from histdatacom.broker_plugin_registry import BrokerPluginInventoryV1
from histdatacom.broker_plugin_security import BrokerTrustedSecurityReceiptV1
from histdatacom.broker_plugins import (
    BrokerEventKind,
    BrokerEventV1,
    BrokerSessionV1,
)

from .authority import PolicySource, generated_policy_context
from .catalog import broker_conformance_catalog, verify_plan
from .contracts import (
    BrokerConformanceCaseResultV1,
    BrokerConformancePlanV1,
    BrokerConformanceProfile,
    BrokerConformanceSubject,
)
from .contracts import (
    BrokerConformanceReason as Reason,
)
from .contracts import (
    BrokerConformanceStatus as Status,
)
from .evidence import BrokerConformanceEvidenceV1, BrokerConformanceProbeV1
from .fixture_plugin import SECRET_CANARY
from .sessions import session_nonces_are_fresh


@dataclass(frozen=True)
class _EvidenceAssessment:
    result: BrokerConformanceCaseResultV1
    sessions: tuple[BrokerSessionV1, ...] = ()
    events: tuple[BrokerEventV1, ...] = ()


def _bind_native_records(
    manifest: BrokerLifecycleManifestV1,
    records: Iterable[BrokerLifecycleRecordV1],
) -> tuple[BrokerLifecycleRecordV1, ...]:
    """Bind this exact observed snapshot, not a later path read, to the pin.

    Native replay checks its current files and authority independently. The
    expected manifest here comes from the permission-bound conformance evidence;
    hashing the actual returned records prevents an A/B/A directory swap from
    borrowing a different opening inventory. The existing pinned provenance
    replay remains mandatory, and this function grants no authority.
    """
    if type(manifest) is not BrokerLifecycleManifestV1:
        raise ValueError("exact native record binding inputs required")
    manifest = BrokerLifecycleManifestV1.from_json(manifest.to_json())
    receipts = manifest.partitions + (
        ()
        if manifest.partial_partition is None
        else (manifest.partial_partition,)
    )
    policy = manifest.header.policy
    if (
        manifest.completion is BrokerLifecycleCompletion.OPEN
        or len(receipts) > policy.max_partitions
        or sum(item.record_count for item in receipts)
        != manifest.appended_records
        or sum(item.byte_count for item in receipts) > policy.max_capture_bytes
    ):
        raise ValueError("native observations differ from expected manifest")
    iterator = iter(records)
    bound: list[BrokerLifecycleRecordV1] = []
    offset = total_events = 0
    run_id = manifest.header.artifact_id
    for receipt in receipts:
        if (
            receipt.first_sequence != offset
            or receipt.record_count > policy.partition_events
            or receipt.byte_count > policy.partition_bytes
        ):
            raise ValueError(
                "native observations exceed expected partition policy"
            )
        digest = hashlib.sha256()
        byte_count = event_count = 0
        for _ in range(receipt.record_count):
            try:
                record = next(iterator)
            except StopIteration:
                raise ValueError(
                    "native observations ended before expected partition"
                ) from None
            if (
                type(record) is not BrokerLifecycleRecordV1
                or record.run_id != run_id
                or record.capture_sequence != offset
            ):
                raise ValueError("native observation run or sequence changed")
            canonical = record.to_json()
            encoded = (canonical + "\n").encode("ascii")
            byte_count += len(encoded)
            if byte_count > receipt.byte_count:
                raise ValueError("native observation partition bytes changed")
            digest.update(encoded)
            event_count += record.kind == "event"
            # Detach from generator-owned objects before any later iteration.
            bound.append(BrokerLifecycleRecordV1.from_json(canonical))
            offset += 1
        if (
            byte_count != receipt.byte_count
            or digest.hexdigest() != receipt.sha256
            or event_count != receipt.event_count
        ):
            raise ValueError(
                "native observations differ from expected partition"
            )
        total_events += event_count
    sentinel = object()
    if next(iterator, sentinel) is not sentinel:
        raise ValueError("native observations exceed expected record inventory")
    if (
        offset != manifest.appended_records
        or total_events != manifest.appended_events
    ):
        raise ValueError("native observation totals changed")
    return tuple(bound)


def _pinned_native_replay(
    verified: BrokerProvenanceVerificationV1,
    seal: BrokerProvenanceSealV1,
) -> bool:
    """Check the closed result of native replay with this explicit expected seal.

    PARTIAL does not mean a complete capture. An intentionally partial capture
    can nevertheless have a fully replayed, pinned terminal chain. The native
    reader above this predicate must have checked the external expected root;
    a constructed verification artifact is not evidence of that IO operation.
    """
    return (
        verified.reason
        in {
            BrokerProvenanceVerificationReason.VERIFIED,
            BrokerProvenanceVerificationReason.PARTIAL,
        }
        and verified.seal == seal
        and verified.header.artifact_id == seal.header_id
        and verified.root_sha256 == seal.root_sha256
        and verified.entry_count == seal.entry_count
        and verified.checkpoint_count == seal.checkpoint_count
    )


def _native_case_outcome(
    case: str,
    native: BrokerLifecycleManifestV1 | None,
    transitions: tuple[BrokerLifecycleTransitionV1, ...],
    epoch_events: tuple[tuple[int, BrokerEventKind], ...],
    sessions: int,
    *,
    pinned: bool,
    complete_chain: bool,
    refused: bool,
) -> bool:
    """Require the scenario's successful host outcome, not just one good event."""
    reason = BrokerLifecycleReason
    state = BrokerLifecycleState
    if (
        native is None
        or refused
        or not pinned
        or not native.worker_reaped
        or len(transitions) < 2
        or native.error_diagnostics
        or (case != "queue.overflow" and native.discarded_known)
    ):
        return False
    stopping, terminal = transitions[-2:]
    if (
        stopping.current is not state.STOPPING
        or terminal.previous is not state.STOPPING
        or terminal.current is not native.state
        or stopping.epoch != terminal.epoch
    ):
        return False
    reasons = {item.reason for item in transitions}
    allowed = {reason.CONFIGURED, reason.STARTING, reason.ACTIVE}
    kinds = tuple(kind for _, kind in epoch_events)
    quotes = (BrokerEventKind.QUOTE,) * 3
    finite_end = (
        stopping.reason is reason.EOF
        and terminal.reason is reason.CLOSED
        and native.state is state.STOPPED
    )
    if case == "lifecycle.reconnect":
        epochs = 1 + len(native.header.policy.retry_delays_ms)
        allowed |= {
            reason.RECONNECT,
            reason.RETRY,
            reason.RETRY_EXHAUSTED,
            reason.FORCED,
        }
        return (
            native.completion is BrokerLifecycleCompletion.PARTIAL
            and native.state is state.FAILED
            and not native.discarded_known
            and stopping.reason is reason.RETRY_EXHAUSTED
            and terminal.reason in {reason.RETRY_EXHAUSTED, reason.FORCED}
            and (
                terminal.reason is not reason.FORCED
                or native.forced_terminations > 0
            )
            and reasons <= allowed
            and epochs >= 2
            and sessions == epochs
            and terminal.epoch == epochs - 1
            and sum(item.reason is reason.RETRY for item in transitions)
            == epochs - 1
            and epoch_events
            == tuple(
                (epoch, kind)
                for epoch in range(epochs)
                for kind in quotes + (BrokerEventKind.DISCONNECTED,)
            )
        )
    if case in {
        "lifecycle.cancellation",
        "lifecycle.forced-shutdown",
        "queue.overflow",
    }:
        expected_stop = {
            "lifecycle.cancellation": reason.CANCELLED,
            "lifecycle.forced-shutdown": reason.SHUTDOWN_TIMEOUT,
            "queue.overflow": reason.QUEUE_SATURATED,
        }[case]
        allowed |= {expected_stop, reason.FORCED, reason.CLOSED}
        if case == "lifecycle.cancellation":
            expected_events = not kinds
            terminal_ok = (
                native.state is state.STOPPED
                and terminal.reason in {reason.CLOSED, reason.CANCELLED}
                and not native.forced_terminations
            ) or (
                native.state is state.FAILED
                and terminal.reason is reason.FORCED
                and native.forced_terminations > 0
            )
        elif case == "lifecycle.forced-shutdown":
            expected_events = kinds == quotes
            terminal_ok = (
                native.state is state.FAILED
                and terminal.reason is reason.FORCED
                and native.forced_terminations > 0
            )
        else:
            expected_events = (
                True  # The controlled overflow has no fixed prefix length.
            )
            terminal_ok = (
                native.state is state.FAILED
                and terminal.reason in {reason.QUEUE_SATURATED, reason.FORCED}
                and (
                    terminal.reason is not reason.FORCED
                    or native.forced_terminations > 0
                )
            )
        return (
            native.completion is BrokerLifecycleCompletion.PARTIAL
            and sessions == 1
            and stopping.reason is expected_stop
            and terminal.epoch == 0
            and reasons <= allowed
            and expected_events
            and terminal_ok
        )
    allowed |= {reason.EOF, reason.CLOSED}
    if case in {"gaps.delivery", "health.false-gap"}:
        return (
            native.completion is BrokerLifecycleCompletion.PARTIAL
            and finite_end
            and sessions == 1
            and not native.forced_terminations
            and not native.discarded_known
            and reasons <= allowed | {reason.GAP}
            and epoch_events
            == tuple((0, kind) for kind in (BrokerEventKind.GAP,) + quotes)
        )
    return (
        native.completion is BrokerLifecycleCompletion.COMPLETE
        and complete_chain
        and finite_end
        and sessions == 1
        and reasons <= allowed
    )


def _mutations_refused(directory: Path, seal: BrokerProvenanceSealV1) -> bool:
    """Independent file parsing and adversarial pure replay after native replay."""
    from .storage import read_text

    if directory.is_symlink():
        raise ValueError("symlink provenance mutation input")
    header = BrokerProvenanceHeaderV1.from_json(
        read_text(directory / "header.json")
    )
    links = tuple(
        BrokerProvenanceLinkV1.from_json(line)
        for line in read_text(directory / "chain.jsonl").splitlines()
    )
    checkpoints = tuple(
        BrokerProvenanceCheckpointV1.from_json(line)
        for line in read_text(directory / "checkpoints.jsonl").splitlines()
    )
    if not 3 <= len(links) <= 4096 or len(checkpoints) > 4096:
        return False
    changed = replace(links[1], sha256="0" * 64)
    variants = (
        links[:1] + (changed,) + links[2:],
        links[:1] + links[2:],
        links[:1] + (links[1],) + links[1:],
        (links[1], links[0]) + links[2:],
        links[:-1],
    )
    for variant in variants:
        try:
            result = verify_provenance_chain(
                header,
                variant,
                checkpoints,
                seal,
                expected_header_id=header.artifact_id,
                expected_seal_id=seal.artifact_id,
            )
        except ValueError:
            continue
        if result.complete:
            return False
    for changed_header in (
        replace(header, plugin_id="org.histdatacom.substituted"),
        replace(header, configuration_sha256="0" * 64),
        replace(
            header,
            permission_manifest_id="broker-permission-manifest:sha256:"
            + "0" * 64,
        ),
        replace(
            header,
            permission_grant_id="broker-permission-grant:sha256:" + "0" * 64,
        ),
        replace(
            header,
            provider_decision_id="broker-provider-policy-decision:sha256:"
            + "0" * 64,
        ),
    ):
        if verify_provenance_chain(
            changed_header,
            links,
            checkpoints,
            seal,
            expected_header_id=header.artifact_id,
            expected_seal_id=seal.artifact_id,
        ).complete:
            return False
    return not verify_provenance_chain(
        header,
        links,
        checkpoints,
        seal,
        expected_header_id=header.artifact_id,
        expected_seal_id="broker-provenance-seal:sha256:" + "0" * 64,
    ).anchored


def verify_conformance_evidence(
    plan: BrokerConformancePlanV1,
    evidence: BrokerConformanceEvidenceV1,
    directory: Path,
    *,
    _paired_second: bool = False,
) -> BrokerConformanceCaseResultV1:
    """Recompute the case result from exact retained native evidence.

    This is a reproducible software-contract report, not authentication of an
    arbitrary third-party report producer or current provider authorization.
    Isolated capture evidence must independently replay against its pinned seal.
    """
    return _assess_conformance_evidence(
        plan, evidence, directory, _paired_second=_paired_second
    ).result


def _assess_conformance_evidence(
    plan: BrokerConformancePlanV1,
    evidence: BrokerConformanceEvidenceV1,
    directory: Path,
    *,
    _paired_second: bool = False,
) -> _EvidenceAssessment:
    """Keep observations local to the assessment that actually bound them."""
    verify_plan(plan)
    evidence = BrokerConformanceEvidenceV1.from_json(evidence.to_json())
    if (
        evidence.plan_id != plan.artifact_id
        or evidence.case_id not in plan.case_ids
        or evidence.candidate_id != plan.driver.candidate_id
    ):
        raise ValueError("substituted conformance execution")
    definition = next(
        item
        for item in broker_conformance_catalog().cases
        if item.case_id == evidence.case_id
    )
    expected_family = (
        "trusted"
        if plan.profile is BrokerConformanceProfile.TRUSTED
        else "isolated"
    )
    if definition.execution_backend == "trusted_probe":
        expected_family = "trusted_probe"
    if definition.execution_backend == "host_admission":
        expected_family = "host_admission"
    if _paired_second:
        if evidence.case_id != "execution.equivalence":
            raise ValueError("unexpected second-backend exemption")
        expected_family = "trusted"
    if (
        definition.execution_backend == "reference_host_fault"
        and plan.subject is not BrokerConformanceSubject.REFERENCE
    ):
        raise ValueError("candidate evidence cannot be a host selftest")
    if (
        evidence.native_family != "none"
        and evidence.native_family != expected_family
    ):
        raise ValueError(
            "actual execution backend does not satisfy planned gate"
        )
    inventory = BrokerPluginInventoryV1.from_json(evidence.inventory_json)
    if not evidence.invocation_json:
        from histdatacom.broker_plugin_capabilities import (
            BrokerCapabilityPlanV1,
            verify_broker_capability_plan,
        )

        refused_plan = BrokerCapabilityPlanV1.from_json(
            evidence.capability_plan_json
        )
        verify_broker_capability_plan(refused_plan, inventory)
        manifest = BrokerPermissionManifestV1.from_json(
            evidence.permission_manifest_json
        )
        if (
            refused_plan.admitted
            or refused_plan.candidate.artifact_id != plan.driver.candidate_id
            or manifest.artifact_id != plan.driver.permission_manifest_id
        ):
            raise ValueError("invalid capability refusal evidence")
        return _EvidenceAssessment(
            BrokerConformanceCaseResultV1(
                evidence.case_id,
                Status.UNSUPPORTED,
                Reason.CAPABILITY,
                plan.subject,
                (evidence.artifact_id,),
                "metadata_only",
            )
        )
    request = BrokerSDKInvocationV1.from_json(evidence.invocation_json)
    if evidence.capability_plan_json != request.plan.to_json():
        raise ValueError("native capability plan substituted")
    permissions = BrokerPermissionManifestV1.from_json(
        evidence.permission_manifest_json
    )
    context = BrokerPermissionContextV1.from_json(
        evidence.permission_context_json
    )
    provider = BrokerPolicyContextV1.from_json(evidence.provider_context_json)
    if (
        request.plan.candidate.artifact_id != plan.driver.candidate_id
        or request.plan.inventory_id != inventory.artifact_id
        or permissions.artifact_id != plan.driver.permission_manifest_id
    ):
        raise ValueError("native conformance identities changed")
    if (
        request.configuration_profile.schema_json
        != plan.driver.configuration_schema_json
    ):
        raise ValueError("native driver schema changed")
    case = evidence.case_id
    definition = next(
        item
        for item in broker_conformance_catalog().cases
        if item.case_id == case
    )
    scenario = next(
        item
        for item in plan.driver.scenarios
        if item.scenario_id == definition.scenario_id
    )
    if (
        request.configuration_profile.public_configuration_json
        != scenario.configuration_json
        or request.configuration_profile.provider_id != plan.driver.provider_id
    ):
        raise ValueError("native scenario or provider changed")
    expected_binding = BrokerPermissionBindingV1(
        plan.driver.candidate_id,
        permissions.artifact_id,
        permissions.sdk_version,
        plan.driver.provider_id,
        request.configuration_profile.artifact_id,
    )
    if (
        not context.grants
        or any(grant.binding != expected_binding for grant in context.grants)
        or len(provider.policies) != 1
        or provider.policies[0].binding != sdk_invocation_binding(request)
    ):
        raise ValueError("native authority substituted")
    status, reason = Status.FAIL, Reason.CONTRACT
    events: tuple[BrokerEventV1, ...] = ()
    audit = None
    native = None
    chain_verified = False
    chain_pinned = False
    native_transitions: tuple[BrokerLifecycleTransitionV1, ...] = ()
    epoch_events: tuple[tuple[int, BrokerEventKind], ...] = ()
    native_reasons: set[str] = set()
    native_sessions = 0
    observed_sessions: tuple[BrokerSessionV1, ...] = ()
    probe = None
    if evidence.probe_json:
        from histdatacom.broker_plugin_capabilities import (
            BrokerAdmittedMetadataV1,
            BrokerInvocationBindingV1,
            validate_broker_metadata,
            verify_broker_admitted_event,
        )

        probe = BrokerConformanceProbeV1.from_json(evidence.probe_json)
        if probe.candidate_id != plan.driver.candidate_id:
            raise ValueError("probe candidate changed")
        if probe.metadata_json:
            metadata = BrokerAdmittedMetadataV1.from_json(probe.metadata_json)
            if (
                validate_broker_metadata(request.plan, metadata.metadata)
                != metadata
            ):
                raise ValueError("probe metadata changed")
            binding = BrokerInvocationBindingV1.from_json(probe.binding_json)
            if (
                binding.candidate_id != plan.driver.candidate_id
                or binding.plan_id != request.plan.artifact_id
                or binding.metadata_id != metadata.metadata.artifact_id
            ):
                raise ValueError("probe binding changed")
        if probe.session_json:
            session = BrokerSessionV1.from_json(probe.session_json)
            if (
                not probe.metadata_json
                or session.metadata_id != metadata.metadata.artifact_id
            ):
                raise ValueError("probe session changed")
            for text in probe.events_json:
                event = BrokerAdmittedEventV1.from_json(text)
                verify_broker_admitted_event(event, request.plan)
                if event.event.session_id != session.artifact_id:
                    raise ValueError("probe event session changed")
    if evidence.native_json:
        proof = BrokerPermissionExecutionV1.from_json(
            evidence.permission_execution_json
        )
        if evidence.native_family == "trusted":
            receipt = BrokerTrustedSecurityReceiptV1.from_json(
                evidence.native_json
            )
            verify_permission_execution(proof, request, receipt)
            observed_sessions = (
                BrokerSessionV1.from_json(receipt.session_json),
            )
            events = tuple(
                BrokerAdmittedEventV1.from_json(text).event
                for text in receipt.events_json
            )
        elif evidence.native_family == "isolated":
            native = BrokerLifecycleManifestV1.from_json(evidence.native_json)
            verify_permission_execution(proof, request, native)
            seal = BrokerProvenanceSealV1.from_json(evidence.provenance_json)
            # A new explicit generated policy is current authority for this
            # generated-only replay, not the retained historical snapshot.
            source = PolicySource(
                generated_policy_context(provider.policies[0].binding)
            )
            with provider_policy_scope(source):
                records = _bind_native_records(
                    native,
                    replay_broker_lifecycle(
                        directory / "native", provider_request=request
                    ),
                )
                verified = read_lifecycle_capture_provenance(
                    directory / "native",
                    native,
                    provider_request=request,
                    expected_root=seal,
                )
            chain_verified = verified.anchored and verified.complete
            chain_pinned = _pinned_native_replay(verified, seal)
            events = tuple(
                BrokerAdmittedEventV1.from_json(record.payload_json).event
                for record in records
                if record.kind == "event"
            )
            native_transitions = tuple(
                BrokerLifecycleTransitionV1.from_json(record.payload_json)
                for record in records
                if record.kind == "transition"
            )
            native_reasons = {item.reason.value for item in native_transitions}
            epoch_events = tuple(
                (record.epoch, event.kind)
                for record, event in zip(
                    (record for record in records if record.kind == "event"),
                    events,
                )
            )
            observed_sessions = tuple(
                BrokerLifecycleSessionV1.from_json(record.payload_json).session
                for record in records
                if record.kind == "session"
            )
            native_sessions = len(observed_sessions)
            audit = BrokerHostHealthAuditV1.from_json(evidence.health_json)
            if audit.artifact_id != seal.terminal.health_audit_id:
                raise ValueError("substituted native host-health audit")
        else:
            raise ValueError("missing native execution family")
    quotes = tuple(
        event for event in events if event.kind is BrokerEventKind.QUOTE
    )
    refusal = evidence.refusal_family != "none"
    passed = False
    if case in {"permissions.required-denial", "permissions.revocation"}:
        if (
            evidence.refusal_family == "permission"
            and evidence.refusal_decision_json
        ):
            decision = BrokerPermissionDecisionV1.from_json(
                evidence.refusal_decision_json
            )
            replay = decide_broker_permissions(
                permissions,
                decision.binding,
                decision.grant_id,
                context,
                decision.at_ns,
            )
            passed = (
                replay == decision
                and not decision.admitted
                and decision.binding == expected_binding
                and evidence.started_at_ns
                <= decision.at_ns
                <= evidence.stopped_at_ns
            )
    elif case.startswith("policy."):
        # Actual native operation must have returned its closed decision. An
        # arbitrary exception or absent ledger is not a successful denial.
        if (
            evidence.refusal_family == "policy"
            and evidence.refusal_decision_json
        ):
            from histdatacom.broker_plugin_policy import BrokerPolicyDecisionV1

            policy_decision = BrokerPolicyDecisionV1.from_json(
                evidence.refusal_decision_json
            )
            passed = (
                policy_decision.status.value == "denied"
                and policy_decision.context == provider
                and policy_decision.request.subject.bindings
                == (sdk_invocation_binding(request),)
                and policy_decision.request.operation
                is {
                    "policy.invoke-denial": BrokerPolicyOperation.INVOKE,
                    "policy.retention-denial": BrokerPolicyOperation.RETAIN_LOCAL,
                    "policy.publication-denial": BrokerPolicyOperation.PUBLISH,
                }[case]
            )
    elif case in {
        "lifecycle.exception",
        "lifecycle.malformed",
        "secrets.redaction",
        "quotes.invalid-spread",
        "quotes.sequence",
        "timestamps.monotonic",
        "sizes.permission-denial",
        "raw.permission-denial",
    }:
        status, reason = Status.NOT_RUN, Reason.EVIDENCE
        if probe is not None and probe.metadata_json and probe.binding_json:
            if case in {"lifecycle.exception", "secrets.redaction"}:
                passed = (
                    probe.stage == "open"
                    and evidence.refusal_family == "capability"
                    and evidence.refusal_reason == "plugin_failure"
                )
            elif (
                case in {"sizes.permission-denial", "raw.permission-denial"}
                and probe.stage == "events"
                and probe.session_json
            ):
                if (
                    evidence.refusal_family == "permission"
                    and evidence.refusal_reason == "resource_permission_denied"
                    and evidence.refusal_decision_json
                ):
                    denied = BrokerPermissionDecisionV1.from_json(
                        evidence.refusal_decision_json
                    )
                    replayed = decide_broker_permissions(
                        permissions,
                        denied.binding,
                        denied.grant_id,
                        context,
                        denied.at_ns,
                    )
                    atom = (
                        "emit:sizes"
                        if case == "sizes.permission-denial"
                        else "raw_payload:emit"
                    )
                    passed = (
                        denied == replayed
                        and denied.binding == expected_binding
                        and denied.admitted
                        and atom not in denied.effective_atoms
                    )
            elif (
                case
                in {
                    "lifecycle.malformed",
                    "quotes.invalid-spread",
                    "quotes.sequence",
                    "timestamps.monotonic",
                }
                and probe.stage == "events"
                and probe.session_json
            ):
                expected_failure = {
                    "lifecycle.malformed": "event_type",
                    "quotes.invalid-spread": "invalid_spread",
                    "quotes.sequence": "sequence",
                    "timestamps.monotonic": "monotonic_regression",
                }[case]
                passed = (
                    evidence.refusal_family == "capability"
                    and evidence.refusal_reason == "capability_violation"
                    and evidence.event_failure == expected_failure
                )
        if case == "secrets.redaction":
            passed = passed and SECRET_CANARY not in evidence.to_json()
    elif case == "lifecycle.cancellation":
        passed = (
            "cancel_requested" in native_reasons
            and native_sessions > 0
            and native is not None
            and native.worker_reaped
        )
    elif case == "lifecycle.forced-shutdown":
        passed = bool(
            native_reasons & {"shutdown_deadline", "forced_termination"}
        )
    elif case == "lifecycle.reconnect":
        passed = (
            native_sessions >= 2
            and audit is not None
            and not any(
                bucket.exact_quote_duplicates for bucket in audit.buckets
            )
        )
    elif case == "queue.overflow":
        passed = "queue_saturated" in native_reasons
    elif case == "queue.backpressure":
        passed = (
            len(quotes) == 4
            and "finite_stream_ended" in native_reasons
            and native is not None
            and native.completion is BrokerLifecycleCompletion.COMPLETE
        )
    elif case.startswith("health."):
        if audit is not None:
            if case == "health.unknown-upstream":
                passed = bool(audit.buckets) and all(
                    bucket.upstream_loss_unknown for bucket in audit.buckets
                )
            elif case == "health.honest":
                passed = audit.state.value == "qualified_host_boundary_only"
            else:
                stimulated = {
                    "health.false-duplicate": any(
                        bucket.exact_quote_duplicates
                        for bucket in audit.buckets
                    ),
                    "health.false-gap": any(
                        bucket.gap_count for bucket in audit.buckets
                    ),
                    "health.false-reorder": any(
                        bucket.reordered_source_times
                        for bucket in audit.buckets
                    ),
                    "health.false-delay": any(
                        bucket.max_heartbeat_gap_ns is not None
                        and bucket.max_heartbeat_gap_ns > 50_000_000
                        for bucket in audit.buckets
                    ),
                }[case]
                passed = (
                    stimulated
                    and bool(events)
                    and not any(
                        bucket.healthy_claim_discrepancies
                        for bucket in audit.buckets
                    )
                )
    elif case == "provenance.replay":
        passed = chain_verified
    elif case == "provenance.mutations":
        passed = chain_verified and _mutations_refused(
            directory / "native-provenance",
            BrokerProvenanceSealV1.from_json(evidence.provenance_json),
        )
    elif case == "quotes.symbols":
        passed = bool(quotes) and all(
            event.instrument == "EURUSD" for event in quotes
        )
    elif case == "quotes.decimals":
        passed = bool(quotes) and all(
            event.quote is not None
            and event.quote.bid == "1.10000"
            and event.quote.ask == "1.20000"
            and Decimal(event.quote.bid) <= Decimal(event.quote.ask)
            for event in quotes
        )
    elif case in {"quotes.duplicate", "quotes.stale"}:
        stamps = [
            event.source_time.timestamp_ns
            for event in quotes
            if event.source_time
        ]
        if len(stamps) == 3 and stamps[0] == stamps[1] and len(quotes) == 3:
            if case == "quotes.duplicate":
                passed = (
                    quotes[0].quote == quotes[1].quote
                    and quotes[0].instrument == quotes[1].instrument
                    and quotes[0].connection_id == quotes[1].connection_id
                    and quotes[0].artifact_id != quotes[1].artifact_id
                )
            else:
                passed = (
                    quotes[0].quote != quotes[1].quote
                    and audit is not None
                    and any(bucket.stale_quotes for bucket in audit.buckets)
                    and not any(
                        bucket.exact_quote_duplicates
                        for bucket in audit.buckets
                    )
                )
    elif case == "quotes.unchanged":
        passed = (
            len(quotes) == 3
            and len(
                {
                    event.source_time.timestamp_ns
                    for event in quotes
                    if event.source_time
                }
            )
            == 3
            and len({event.quote.to_json() for event in quotes if event.quote})
            == 1
        )
    elif case == "timestamps.source":
        passed = bool(quotes) and all(
            event.source_time is not None
            and event.source_time.semantics.value == "broker_event"
            for event in quotes
        )
    elif case == "timestamps.receive":
        passed = bool(quotes) and all(
            event.receive_time is not None for event in quotes
        )
    elif case == "heartbeat.delivery":
        passed = any(
            event.kind is BrokerEventKind.HEARTBEAT for event in events
        )
    elif case == "gaps.delivery":
        passed = any(
            event.gap is not None and event.gap.missing_message_count == 5
            for event in events
        )
    elif case == "sizes.quoted":
        passed = bool(quotes) and all(
            event.quote is not None
            and event.quote.bid_size is not None
            and event.quote.bid_size.value == "2"
            for event in quotes
        )
    elif case == "raw.hashes":
        passed = bool(quotes) and all(
            event.raw_provenance is not None for event in quotes
        )
    elif case in {
        "discovery.metadata",
        "discovery.sdk",
        "permissions.declaration",
        "lifecycle.finite",
    }:
        passed = (
            tuple(event.kind for event in events)
            == (
                BrokerEventKind.HEALTH,
                BrokerEventKind.QUOTE,
                BrokerEventKind.QUOTE,
                BrokerEventKind.QUOTE,
            )
            and not refusal
            and (
                evidence.native_family == "trusted"
                or (
                    native is not None
                    and native.completion is BrokerLifecycleCompletion.COMPLETE
                )
            )
        )
    elif case in {
        "replay.determinism",
        "execution.equivalence",
        "session.freshness",
    }:
        # Only a prerequisite: storage and runner also require a second run.
        passed = (
            tuple(event.kind for event in events)
            == (
                BrokerEventKind.HEALTH,
                BrokerEventKind.QUOTE,
                BrokerEventKind.QUOTE,
                BrokerEventKind.QUOTE,
            )
            and not refusal
            and (
                evidence.native_family == "trusted"
                or (
                    chain_verified
                    and native is not None
                    and native.completion is BrokerLifecycleCompletion.COMPLETE
                )
            )
        )
    if passed and evidence.native_json:
        passed = session_nonces_are_fresh(observed_sessions)
    if passed and evidence.native_family == "isolated":
        passed = _native_case_outcome(
            case,
            native,
            native_transitions,
            epoch_events,
            native_sessions,
            pinned=chain_pinned,
            complete_chain=chain_verified,
            refused=refusal,
        )
    if passed:
        status, reason = Status.PASS, Reason.VERIFIED
    elif evidence.refusal_family == "execution":
        status, reason = Status.ERROR, Reason.EXECUTION
    elif evidence.refusal_reason == "security_backend_unsupported":
        status, reason = Status.UNSUPPORTED, Reason.PLATFORM
    return _EvidenceAssessment(
        BrokerConformanceCaseResultV1(
            case,
            status,
            reason,
            plan.subject,
            (evidence.artifact_id,),
            (
                "reference_host_fault"
                if case in {"queue.overflow", "provenance.mutations"}
                else evidence.native_family
            ),
        ),
        observed_sessions,
        events,
    )


def verify_conformance_equivalence(
    plan: BrokerConformancePlanV1,
    first: BrokerConformanceEvidenceV1,
    second: BrokerConformanceEvidenceV1,
    directory: Path,
) -> BrokerConformanceCaseResultV1:
    """Compare SDK semantics, retaining physical run clocks and roots separately."""
    paired = first.case_id == "execution.equivalence"
    if (
        first.case_id
        not in {
            "execution.equivalence",
            "replay.determinism",
            "session.freshness",
        }
        or first.case_id != second.case_id
    ):
        raise ValueError("invalid paired conformance case")
    second_directory = directory / ("trusted" if paired else "repeat")
    left_assessment = _assess_conformance_evidence(plan, first, directory)
    right_assessment = _assess_conformance_evidence(
        plan, second, second_directory, _paired_second=paired
    )
    left, right = left_assessment.result, right_assessment.result
    if left.status is Status.UNSUPPORTED or right.status is Status.UNSUPPORTED:
        status, reason = Status.UNSUPPORTED, Reason.PLATFORM
    else:
        if paired and (
            first.native_family != "isolated"
            or second.native_family != "trusted"
        ):
            raise ValueError(
                "equivalence requires both actual execution backends"
            )
        if not paired and first.native_family != second.native_family:
            raise ValueError("repeat execution backend changed")
        status, reason = Status.FAIL, Reason.CONTRACT
        if (
            left.status is Status.PASS
            and right.status is Status.PASS
            and first.stopped_at_ns <= second.started_at_ns
        ):

            first_sessions, first_events = (
                left_assessment.sessions,
                left_assessment.events,
            )
            second_sessions, second_events = (
                right_assessment.sessions,
                right_assessment.events,
            )

            def semantics(event: BrokerEventV1) -> tuple[object, ...]:
                return (
                    event.kind,
                    event.instrument,
                    event.quote,
                    event.source_time,
                    event.gap,
                    event.diagnostic,
                    event.raw_provenance,
                )

            fresh = session_nonces_are_fresh(
                second_sessions, earlier=first_sessions
            )
            same_semantics = bool(first_events) and tuple(
                map(semantics, first_events)
            ) == tuple(map(semantics, second_events))
            if fresh and (
                first.case_id == "session.freshness" or same_semantics
            ):
                status, reason = Status.PASS, Reason.VERIFIED
    return BrokerConformanceCaseResultV1(
        first.case_id,
        status,
        reason,
        plan.subject,
        tuple(sorted((first.artifact_id, second.artifact_id))),
        "paired_runtime",
    )
