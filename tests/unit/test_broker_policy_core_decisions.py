"""Eligibility and evidence-graph canaries using generated declarations only."""

from dataclasses import replace

import pytest

from histdatacom.broker_plugin_policy.contracts import (
    BrokerPolicyAttributionV1,
    BrokerPolicyConstraintMode,
    BrokerPolicyRequestV1,
    BrokerPolicyRevocationV1,
    BrokerPolicyOperation as Operation,
    BrokerPolicyStatus as Status,
)
from histdatacom.broker_plugin_policy.decisions import decide_provider_operation
from tests.fixtures.broker_policy_core import (
    core_context,
    core_subject,
    with_policy,
)


def decision(context, *, now=10, operation=Operation.MATERIAL_USE, **kwargs):
    return decide_provider_operation(
        context,
        BrokerPolicyRequestV1(core_subject(context), operation, now, **kwargs),
    )


def reasons(result):
    return {reason for cell in result.cells for reason in cell.reasons}


@pytest.mark.parametrize(
    "name,matching,missing,mismatching",
    [
        ("account_class", "retail", None, "institutional"),
        ("geography", "region-a", None, "region-b"),
        ("feed_type", "quotes", None, "trades"),
        ("commercial_use", "noncommercial", False, True),
        ("eligibility", "eligible", (), ("different",)),
    ],
)
def test_all_five_constraints_distinguish_match_unknown_and_denied(
    name, matching, missing, mismatching
):
    context = core_context()
    policy = context.policies[0]
    policy = replace(
        policy,
        constraints=tuple(
            (
                replace(
                    c, mode=BrokerPolicyConstraintMode.ONLY, values=(matching,)
                )
                if c.name == name
                else c
            )
            for c in policy.constraints
        ),
    )
    context = with_policy(context, policy)
    assert decision(context).allowed
    field = "eligibility_assertions" if name == "eligibility" else name
    changed = replace(
        context, execution=replace(context.execution, **{field: mismatching})
    )
    assert decision(changed).status is Status.DENIED
    assert "constraint_denied:" + name in reasons(decision(changed))
    if name != "commercial_use":
        unknown = replace(
            context, execution=replace(context.execution, **{field: missing})
        )
        assert decision(unknown).status is Status.UNKNOWN
        assert "unknown_constraint_value:" + name in reasons(decision(unknown))
    unknown_policy = replace(
        policy,
        constraints=tuple(
            (
                replace(c, mode=BrokerPolicyConstraintMode.UNKNOWN, values=())
                if c.name == name
                else c
            )
            for c in policy.constraints
        ),
    )
    assert (
        decision(with_policy(context, unknown_policy)).status is Status.UNKNOWN
    )


def test_attribution_text_and_exact_acknowledgement_are_distinct():
    context = core_context()
    policy = context.policies[0]
    attribution = BrokerPolicyAttributionV1(
        "generated-credit", "Generated test attribution"
    )
    policy = replace(
        policy,
        attributions=(attribution,),
        rules=tuple(
            replace(r, required_attribution_ids=(attribution.attribution_id,))
            for r in policy.rules
        ),
    )
    context = with_policy(context, policy)
    assert "required_attribution_not_acknowledged" in reasons(decision(context))
    wrong = replace(
        context,
        execution=replace(
            context.execution, attribution_ids=("another-credit",)
        ),
    )
    assert not decision(wrong).allowed
    correct = replace(
        context,
        execution=replace(
            context.execution, attribution_ids=(attribution.attribution_id,)
        ),
    )
    assert decision(correct).allowed


def successor_context(context, *, declared=20, effective=30, selected=False):
    parent = context.policies[0]
    child = replace(
        parent,
        version="1.1.0",
        predecessor_id=parent.artifact_id,
        declared_at_ns=declared,
        effective_from_ns=effective,
    )
    ack = replace(
        context.acknowledgements[0],
        policy_id=child.artifact_id,
        acknowledged_at_ns=declared,
    )
    return replace(
        context,
        policies=tuple(sorted((parent, child), key=lambda p: p.artifact_id)),
        acknowledgements=tuple(
            sorted(
                context.acknowledgements + (ack,), key=lambda a: a.artifact_id
            )
        ),
        selected_policy_ids=(
            child.artifact_id if selected else parent.artifact_id,
        ),
    )


def test_successor_is_known_as_of_declared_clock_and_requires_explicit_selection():
    base = core_context()
    context = successor_context(base)
    before = decision(context, now=19)
    assert before.allowed and before.valid_until_ns == 100
    announced = decision(context, now=20)
    assert announced.allowed and announced.valid_until_ns == 30
    assert decision(context, now=29).allowed
    refused = decision(context, now=30)
    assert not refused.allowed
    assert "selected_policy_superseded" in reasons(refused)
    selected = successor_context(base, selected=True)
    assert not decision(selected, now=29).allowed
    assert decision(selected, now=30).allowed
    # Adding the later-declared successor does not change old as-of cell facts.
    historical = decision(base, now=19)
    assert before.cells == historical.cells
    assert before.request == historical.request


@pytest.mark.parametrize(
    "mutation",
    ["same_version", "fork", "missing_parent", "binding", "declaration"],
)
def test_successor_graph_rejects_ambiguous_or_rewritten_ancestry(mutation):
    context = successor_context(core_context())
    parent = next(p for p in context.policies if p.predecessor_id is None)
    child = next(p for p in context.policies if p.predecessor_id is not None)
    if mutation == "same_version":
        child = replace(child, version=parent.version)
    elif mutation == "missing_parent":
        child = replace(child, predecessor_id="missing-generated-policy")
    elif mutation == "binding":
        child = replace(
            child,
            binding=replace(
                child.binding, native_binding_kind="changed-binding"
            ),
        )
    elif mutation == "declaration":
        child = replace(child, declared_at_ns=1)
    children = (child,)
    if mutation == "fork":
        children += (replace(child, version="1.2.0"),)
    with pytest.raises(ValueError):
        replace(
            context,
            policies=tuple(
                sorted((parent,) + children, key=lambda p: p.artifact_id)
            ),
            acknowledgements=(),
            selected_policy_ids=(parent.artifact_id,),
        )


@pytest.mark.parametrize("target", ["policy", "ack"])
def test_revocation_records_preserve_history_and_stop_current_permission(
    target,
):
    context = core_context()
    identity = (
        context.policies[0].artifact_id
        if target == "policy"
        else context.acknowledgements[0].artifact_id
    )
    revocation = BrokerPolicyRevocationV1(
        identity, 30, 20, context.policies[0].evidence_ids, "withdrawn"
    )
    changed = replace(context, revocations=(revocation,))
    assert decision(changed, now=29).cells == decision(context, now=29).cells
    result = decision(changed, now=30)
    assert not result.allowed
    assert result.status is (
        Status.DENIED if target == "policy" else Status.UNKNOWN
    )


def test_exact_ack_evidence_and_provider_identity_are_mandatory():
    context = core_context()
    evidence = context.evidence[0]
    second = replace(
        evidence,
        reference=replace(evidence.reference, native_id="second-terms"),
    )
    policy = replace(
        context.policies[0],
        evidence_ids=tuple(sorted((evidence.artifact_id, second.artifact_id))),
    )
    ack = replace(context.acknowledgements[0], policy_id=policy.artifact_id)
    with pytest.raises(ValueError, match="complete exact terms"):
        replace(
            context,
            evidence=tuple(
                sorted((evidence, second), key=lambda e: e.artifact_id)
            ),
            policies=(policy,),
            acknowledgements=(ack,),
            selected_policy_ids=(policy.artifact_id,),
        )
    foreign = replace(evidence, provider_id="different-provider")
    foreign_policy = replace(
        context.policies[0],
        evidence_ids=(foreign.artifact_id,),
        rules=tuple(
            replace(r, evidence_ids=(foreign.artifact_id,))
            for r in context.policies[0].rules
        ),
    )
    with pytest.raises(ValueError, match="different provider"):
        replace(
            context,
            evidence=(foreign,),
            policies=(foreign_policy,),
            acknowledgements=(),
            selected_policy_ids=(foreign_policy.artifact_id,),
        )


def test_same_evidence_native_identity_cannot_mean_two_contents():
    context = core_context()
    second = replace(
        context.evidence[0],
        reference=replace(context.evidence[0].reference, sha256="f" * 64),
    )
    with pytest.raises(ValueError, match="conflicting content"):
        replace(
            context,
            evidence=tuple(
                sorted(
                    context.evidence + (second,), key=lambda e: e.artifact_id
                )
            ),
        )


@pytest.mark.parametrize(
    "now,allowed", [(0, False), (2, False), (3, True), (99, True), (100, False)]
)
def test_exact_effective_ack_and_expiry_boundaries(now, allowed):
    assert decision(core_context(), now=now).allowed is allowed


def test_software_license_does_not_infer_any_provider_data_rights():
    context = core_context(Status.UNKNOWN)
    assert context.policies[0].host_software_license == "MIT"
    assert not decision(context, operation=Operation.INVOKE).allowed
    context = core_context()
    context = with_policy(
        context, replace(context.policies[0], plugin_software_license=None)
    )
    assert not decision(context, operation=Operation.INVOKE).allowed
    # Reusing already-captured material is not software invocation authority.
    assert decision(context, operation=Operation.MATERIAL_USE).allowed


@pytest.mark.parametrize(
    "operation", [Operation.PUBLISH, Operation.REDISTRIBUTE]
)
def test_dissemination_requires_separate_class_right_and_recipient_scope(
    operation,
):
    context = core_context()
    assert not decision(context, operation=operation).allowed
    assert decision(
        context,
        operation=operation,
        recipient_scope="generated-public-recipient",
    ).allowed
    policy = replace(
        context.policies[0],
        rules=tuple(
            replace(r, status=Status.DENIED) if r.operation is operation else r
            for r in context.policies[0].rules
        ),
    )
    assert not decision(
        with_policy(context, policy),
        operation=operation,
        recipient_scope="generated-public-recipient",
    ).allowed
