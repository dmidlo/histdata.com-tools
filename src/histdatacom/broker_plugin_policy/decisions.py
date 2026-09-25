"""Recomputed, declared-evidence admission; no legal interpretation."""

from __future__ import annotations

from .contracts import (
    BrokerPolicyCellDecisionV1,
    BrokerPolicyConstraintMode,
    BrokerPolicyContextV1,
    BrokerPolicyDecisionV1,
    BrokerPolicyEvidenceKind,
    BrokerPolicyOperation,
    BrokerPolicyRequestV1,
    BrokerPolicyRetention,
    BrokerPolicyStatus,
)


def validate_review_context(context: BrokerPolicyContextV1) -> None:
    policies = {p.artifact_id: p for p in context.policies}
    evidence = {e.artifact_id: e for e in context.evidence}
    acks = {a.artifact_id: a for a in context.acknowledgements}
    if not set(context.selected_policy_ids) <= policies.keys():
        raise ValueError(
            "selected policy is absent from exact retained inventory"
        )
    bindings = [
        policies[i].binding.artifact_id for i in context.selected_policy_ids
    ]
    if len(bindings) != len(set(bindings)):
        raise ValueError("ambiguous selected policies for one native binding")
    successors: dict[str, str] = {}
    reference_contents: dict[tuple[str, str], tuple[str, int]] = {}
    for item in context.evidence:
        key = (item.reference.kind, item.reference.native_id)
        value = (item.reference.sha256, item.reference.byte_length)
        if key in reference_contents and reference_contents[key] != value:
            raise ValueError(
                "conflicting content for one terms evidence identity"
            )
        reference_contents[key] = value
    for identity, policy in policies.items():
        if not set(policy.evidence_ids) <= evidence.keys():
            raise ValueError("missing retained policy terms evidence")
        if any(
            evidence[i].provider_id != policy.binding.provider_id
            for i in policy.evidence_ids
        ):
            raise ValueError("terms evidence belongs to a different provider")
        previous = policy.predecessor_id
        if previous is not None:
            if previous not in policies or previous == identity:
                raise ValueError("missing or cyclic policy predecessor")
            parent = policies[previous]
            if (
                parent.binding != policy.binding
                or parent.declared_at_ns > policy.declared_at_ns
            ):
                raise ValueError(
                    "policy successor changes native binding or reverses declaration"
                )
            if parent.version == policy.version:
                raise ValueError("policy successor must have a new SemVer")
            if previous in successors:
                raise ValueError("ambiguous policy successor fork")
            successors[previous] = identity
        seen = {identity}
        while previous is not None:
            if previous in seen or previous not in policies:
                raise ValueError("cyclic or missing policy predecessor")
            seen.add(previous)
            previous = policies[previous].predecessor_id
    for ack in context.acknowledgements:
        if ack.policy_id not in policies:
            raise ValueError("acknowledgement lacks retained policy")
        policy = policies[ack.policy_id]
        if ack.evidence_ids != policy.evidence_ids:
            raise ValueError(
                "acknowledgement must bind the complete exact terms inventory"
            )
        if ack.acknowledged_at_ns < max(
            policy.declared_at_ns,
            *(evidence[i].reviewed_at_ns for i in policy.evidence_ids),
        ):
            raise ValueError(
                "acknowledgement predates policy or evidence review"
            )
    for revocation in context.revocations:
        if (
            revocation.target_id not in policies
            and revocation.target_id not in acks
        ):
            raise ValueError(
                "revocation lacks exact retained policy/acknowledgement"
            )
        if not set(revocation.evidence_ids) <= evidence.keys():
            raise ValueError("revocation lacks retained evidence")
        policy_id = (
            revocation.target_id
            if revocation.target_id in policies
            else acks[revocation.target_id].policy_id
        )
        if any(
            evidence[i].provider_id != policies[policy_id].binding.provider_id
            for i in revocation.evidence_ids
        ):
            raise ValueError(
                "revocation evidence belongs to a different provider"
            )
        if any(
            evidence[i].reviewed_at_ns > revocation.recorded_at_ns
            for i in revocation.evidence_ids
        ):
            raise ValueError("revocation predates retained evidence review")


def _decision_values(
    context: BrokerPolicyContextV1,
    request: BrokerPolicyRequestV1,
) -> tuple[
    tuple[BrokerPolicyCellDecisionV1, ...], BrokerPolicyStatus, int | None
]:
    validate_review_context(context)
    now = request.decision_at_ns
    policies = {p.artifact_id: p for p in context.policies}
    evidence = {e.artifact_id: e for e in context.evidence}
    selected = {
        policies[i].binding.artifact_id: policies[i]
        for i in context.selected_policy_ids
    }
    revoked = {
        r.target_id
        for r in context.revocations
        if r.recorded_at_ns <= now and r.effective_at_ns <= now
    }
    output: list[BrokerPolicyCellDecisionV1] = []
    for binding in request.subject.bindings:
        policy = selected.get(binding.artifact_id)
        if policy is None:
            output.extend(
                BrokerPolicyCellDecisionV1(
                    binding.artifact_id,
                    None,
                    cls,
                    BrokerPolicyStatus.UNKNOWN,
                    ("policy_not_selected_for_exact_binding",),
                    (),
                    (),
                    (),
                    None,
                )
                for cls in request.subject.data_classes
            )
            continue
        policy_id = policy.artifact_id
        common: set[str] = set()
        denied = False
        deadlines = (
            [policy.expires_at_ns] if policy.expires_at_ns is not None else []
        )
        if (
            request.operation is BrokerPolicyOperation.INVOKE
            and policy.plugin_software_license is None
        ):
            common.add("plugin_software_license_unknown")
        if request.subject.evidence_kind is BrokerPolicyEvidenceKind.SYNTHETIC:
            common.add("synthetic_subject_is_not_operational_authority")
        if now < max(policy.declared_at_ns, policy.effective_from_ns):
            common.add("policy_not_yet_known_or_effective")
        if policy.expires_at_ns is not None and now >= policy.expires_at_ns:
            common.add("policy_expired")
        if policy_id in revoked:
            common.add("policy_revoked")
            denied = True
        for successor in context.policies:
            if successor.declared_at_ns > now:
                continue
            parent = successor.predecessor_id
            while parent is not None:
                if parent == policy_id:
                    if successor.effective_from_ns <= now:
                        common.add("selected_policy_superseded")
                    else:
                        deadlines.append(successor.effective_from_ns)
                    break
                parent = policies[parent].predecessor_id
        for identity in policy.evidence_ids:
            item = evidence[identity]
            if item.reviewed_at_ns > now:
                common.add("terms_evidence_not_reviewed_at_operation")
            if item.evidence_kind is not BrokerPolicyEvidenceKind.DECLARED:
                common.add("synthetic_evidence_cannot_authorize_operations")
        active_acks = []
        for ack in context.acknowledgements:
            if ack.policy_id != policy_id or ack.acknowledged_at_ns > now:
                continue
            if ack.expires_at_ns is not None and ack.expires_at_ns <= now:
                continue
            if ack.artifact_id in revoked:
                continue
            active_acks.append(ack)
        if not active_acks:
            common.add("no_current_exact_operator_acknowledgement")
        else:
            # All retained current acknowledgements bind the same policy and
            # full evidence. Conservative earliest expiry, never longest cherry-pick.
            deadlines.extend(
                a.expires_at_ns
                for a in active_acks
                if a.expires_at_ns is not None
            )
        relevant_targets = {policy_id} | {a.artifact_id for a in active_acks}
        deadlines.extend(
            r.effective_at_ns
            for r in context.revocations
            if r.target_id in relevant_targets
            and r.recorded_at_ns <= now < r.effective_at_ns
        )
        execution = context.execution
        constraint_values = {
            "commercial_use": (
                "commercial" if execution.commercial_use else "noncommercial"
            ),
            "geography": execution.geography,
            "account_class": execution.account_class,
            "feed_type": execution.feed_type,
        }
        for constraint in policy.constraints:
            if constraint.mode is BrokerPolicyConstraintMode.UNKNOWN:
                common.add("unknown_constraint:" + constraint.name)
            elif constraint.mode is BrokerPolicyConstraintMode.ONLY:
                if constraint.name == "eligibility":
                    matches = set(constraint.values) <= set(
                        execution.eligibility_assertions
                    )
                    known = bool(execution.eligibility_assertions)
                else:
                    value = constraint_values[constraint.name]
                    known = value is not None
                    matches = value in constraint.values
                if not known:
                    common.add("unknown_constraint_value:" + constraint.name)
                elif not matches:
                    common.add("constraint_denied:" + constraint.name)
                    denied = True
        until = min(deadlines) if deadlines else None
        for cls in request.subject.data_classes:
            rule = next(
                r
                for r in policy.rules
                if r.operation is request.operation and r.data_class is cls
            )
            reasons = set(common)
            cell_denied = denied or rule.status is BrokerPolicyStatus.DENIED
            if rule.status is not BrokerPolicyStatus.ALLOWED:
                reasons.add("declared_" + rule.status.value)
            if not set(rule.required_attribution_ids) <= set(
                execution.attribution_ids
            ):
                reasons.add("required_attribution_not_acknowledged")
            if request.operation is BrokerPolicyOperation.RETAIN_LOCAL:
                if rule.retention is BrokerPolicyRetention.UNKNOWN:
                    reasons.add("unknown_retention_duration")
                elif rule.retention is BrokerPolicyRetention.FINITE:
                    reasons.add("finite_retention_enforcement_unsupported")
                if request.intended_retention_deadline_ns is not None:
                    if (
                        until is not None
                        and request.intended_retention_deadline_ns > until
                    ):
                        reasons.add(
                            "requested_retention_outlives_current_admission"
                        )
            if (
                request.operation
                in (
                    BrokerPolicyOperation.REDISTRIBUTE,
                    BrokerPolicyOperation.PUBLISH,
                )
                and request.recipient_scope is None
            ):
                reasons.add("recipient_scope_unknown")
            status = (
                BrokerPolicyStatus.DENIED
                if cell_denied
                else (
                    BrokerPolicyStatus.UNKNOWN
                    if reasons
                    else BrokerPolicyStatus.ALLOWED
                )
            )
            output.append(
                BrokerPolicyCellDecisionV1(
                    binding.artifact_id,
                    policy_id,
                    cls,
                    status,
                    tuple(sorted(reasons)),
                    rule.evidence_ids,
                    tuple(sorted(a.artifact_id for a in active_acks)),
                    rule.required_attribution_ids,
                    until,
                )
            )
    cells = tuple(output)
    status = (
        BrokerPolicyStatus.DENIED
        if any(c.status is BrokerPolicyStatus.DENIED for c in cells)
        else (
            BrokerPolicyStatus.UNKNOWN
            if any(c.status is BrokerPolicyStatus.UNKNOWN for c in cells)
            else BrokerPolicyStatus.ALLOWED
        )
    )
    deadlines = [
        c.valid_until_ns for c in cells if c.valid_until_ns is not None
    ]
    return cells, status, min(deadlines) if deadlines else None


def decide_provider_operation(
    context: BrokerPolicyContextV1,
    request: BrokerPolicyRequestV1,
) -> BrokerPolicyDecisionV1:
    """Pure offline decision, not a native subject verification or scope grant."""
    if (
        type(context) is not BrokerPolicyContextV1
        or type(request) is not BrokerPolicyRequestV1
    ):
        raise TypeError(
            "provider decision requires exact typed context/request"
        )
    context = BrokerPolicyContextV1.from_json(context.to_json())
    request = BrokerPolicyRequestV1.from_json(request.to_json())
    cells, status, until = _decision_values(context, request)
    return BrokerPolicyDecisionV1(context, request, cells, status, until)
