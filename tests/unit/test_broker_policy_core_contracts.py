"""Strict bounded canonical review contracts, without provider inputs."""

from dataclasses import FrozenInstanceError, replace
import json

import pytest

from histdatacom.broker_plugin_policy._wire import canonical_json, plain
from histdatacom.broker_plugin_policy.contracts import (
    BrokerPolicyConstraintMode,
    BrokerPolicyContextV1,
    BrokerPolicyEvidenceKind,
    BrokerPolicyOperation,
    BrokerPolicyRetention,
    BrokerPolicyStatus,
)
from tests.fixtures.broker_policy_core import core_context, with_policy


def test_complete_immutable_canonical_context_roundtrip():
    context = core_context()
    assert len(context.policies[0].rules) == 49
    assert BrokerPolicyContextV1.from_json(context.to_json()) == context
    with pytest.raises(FrozenInstanceError):
        context.policies = ()
    detached = context.to_dict()
    detached["payload"]["policies"].clear()
    assert len(context.policies) == 1
    assert "not legal advice" in context.policies[0].nonclaim


@pytest.mark.parametrize(
    "mutation",
    [
        "unknown",
        "missing",
        "duplicate",
        "pretty",
        "wrong_id",
        "bool_clock",
        "string_clock",
    ],
)
def test_canonical_envelope_rejects_ambiguous_coercive_payloads(mutation):
    context = core_context()
    data = context.to_dict()
    if mutation == "unknown":
        data["payload"]["policies"][0]["extra"] = "later"
    elif mutation == "missing":
        del data["payload"]["execution"]
    elif mutation == "wrong_id":
        data["artifact_id"] = data["artifact_id"][:-1] + (
            "1" if data["artifact_id"][-1] != "1" else "2"
        )
    elif mutation in ("bool_clock", "string_clock"):
        data["payload"]["policies"][0]["declared_at_ns"] = (
            True if mutation == "bool_clock" else "2"
        )
    text = json.dumps(data, sort_keys=True, separators=(",", ":"))
    if mutation == "duplicate":
        text = text[:-1] + ',"payload":{}}'
    if mutation == "pretty":
        text = json.dumps(data, indent=2)
    with pytest.raises(ValueError):
        BrokerPolicyContextV1.from_json(text)


@pytest.mark.parametrize("value", [True, -1, 2**63, 1.0, "2"])
def test_clocks_are_nonnegative_exact_int64(value):
    with pytest.raises(ValueError):
        replace(core_context().policies[0], declared_at_ns=value)


@pytest.mark.parametrize(
    "mutation",
    [
        "missing",
        "duplicate",
        "unsorted",
        "unknown_retention",
        "duration_without_finite",
        "allow_without_evidence",
        "missing_attribution",
        "semver",
        "host_license",
        "causal_claim",
    ],
)
def test_complete_matrix_and_semantic_constraints(mutation):
    context = core_context()
    policy = context.policies[0]
    kwargs = {}
    if mutation == "missing":
        kwargs["rules"] = policy.rules[:-1]
    elif mutation == "duplicate":
        kwargs["rules"] = policy.rules[:-1] + (policy.rules[0],)
    elif mutation == "unsorted":
        kwargs["rules"] = tuple(reversed(policy.rules))
    elif mutation == "unknown_retention":
        with pytest.raises(ValueError):
            replace(policy.rules[0], retention=BrokerPolicyRetention.UNBOUNDED)
        return
    elif mutation == "duration_without_finite":
        with pytest.raises(ValueError):
            replace(policy.rules[0], maximum_retention_ns=1)
        return
    elif mutation == "allow_without_evidence":
        with pytest.raises(ValueError):
            replace(policy.rules[0], evidence_ids=())
        return
    elif mutation == "missing_attribution":
        kwargs["rules"] = (
            replace(policy.rules[0], required_attribution_ids=("missing",)),
        ) + policy.rules[1:]
    else:
        key = {
            "semver": "version",
            "host_license": "host_software_license",
            "causal_claim": "nonclaim",
        }[mutation]
        kwargs[key] = "changed"
    with pytest.raises(ValueError):
        replace(policy, **kwargs)


def test_large_external_reference_does_not_allocate_referenced_bytes():
    reference = core_context().evidence[0].reference
    assert replace(reference, byte_length=2**62).byte_length == 2**62
    assert len(canonical_json(reference)) < 300


def test_secret_text_binding_and_evidence_never_retained():
    context = core_context()
    for text in (
        "Bearer ABCDEFGH123",
        "https://user:password@example.org/terms",
    ):
        with pytest.raises(ValueError):
            replace(context.evidence[0], locator=text)
    with pytest.raises(ValueError):
        replace(
            context.policies[0].binding,
            native_binding_json='{"password":"plaintext"}',
        )


def test_aggregate_alias_cardinality_depth_and_string_work_preflight():
    shared = ["x" * 65536] * 128
    with pytest.raises(ValueError, match="byte"):
        plain([shared, shared])
    cyclic = []
    cyclic.append(cyclic)
    with pytest.raises(ValueError, match="depth"):
        plain(cyclic)
    with pytest.raises(ValueError, match="collection"):
        plain([None] * 4097)
    with pytest.raises(ValueError, match="string"):
        plain("x" * 65537)


@pytest.mark.parametrize(
    "name",
    [
        "geography",
        "commercial_use",
        "account_class",
        "feed_type",
        "eligibility",
    ],
)
def test_unknown_constraints_are_explicit_not_missing(name):
    context = core_context()
    policy = context.policies[0]
    changed = replace(
        policy,
        constraints=tuple(
            (
                replace(c, mode=BrokerPolicyConstraintMode.UNKNOWN)
                if c.name == name
                else c
            )
            for c in policy.constraints
        ),
    )
    assert with_policy(context, changed).policies[0] == changed


def test_synthetic_evidence_is_a_distinct_nonqualifying_declaration():
    context = core_context()
    evidence = replace(
        context.evidence[0], evidence_kind=BrokerPolicyEvidenceKind.SYNTHETIC
    )
    assert evidence.artifact_id != context.evidence[0].artifact_id
    assert all(
        r.status is BrokerPolicyStatus.ALLOWED
        for r in context.policies[0].rules
    )
    assert {r.operation for r in context.policies[0].rules} == set(
        BrokerPolicyOperation
    )
