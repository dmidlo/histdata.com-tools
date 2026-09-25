"""Generated declarations for pure policy arithmetic; never native authority."""

from dataclasses import replace

from histdatacom.broker_plugin_policy.contracts import (
    BrokerPolicyAcknowledgementV1,
    BrokerPolicyBindingV1,
    BrokerPolicyConstraintMode,
    BrokerPolicyConstraintV1,
    BrokerPolicyContextV1,
    BrokerPolicyDataClass,
    BrokerPolicyEvidenceKind,
    BrokerPolicyEvidenceV1,
    BrokerPolicyExecutionV1,
    BrokerPolicyOperation,
    BrokerPolicyReferenceV1,
    BrokerPolicyRetention,
    BrokerPolicyRuleV1,
    BrokerPolicyStatus,
    BrokerPolicySubjectV1,
    BrokerProviderPolicyV1,
    canonical_policy_json,
)


def core_context(status=BrokerPolicyStatus.ALLOWED):
    binding = BrokerPolicyBindingV1(
        "generated-provider",
        "generated-metadata-not-native",
        canonical_policy_json({"configuration": "generated-v1"}),
    )
    evidence = BrokerPolicyEvidenceV1(
        BrokerPolicyReferenceV1(
            "generated-terms", "generated-terms-v1", "a" * 64, 100
        ),
        binding.provider_id,
        "generated terms for tests only",
        "1",
        "2020-01-01",
        "generated issuer",
        1,
        BrokerPolicyEvidenceKind.DECLARED,
        "fixture:generated-not-real-permission",
    )
    policy = BrokerProviderPolicyV1(
        "1.0.0",
        binding,
        "MIT",
        "MIT",
        (evidence.artifact_id,),
        tuple(
            BrokerPolicyRuleV1(
                op,
                cls,
                status,
                (evidence.artifact_id,),
                (
                    BrokerPolicyRetention.UNBOUNDED
                    if op is BrokerPolicyOperation.RETAIN_LOCAL
                    else BrokerPolicyRetention.NOT_APPLICABLE
                ),
            )
            for op in sorted(BrokerPolicyOperation)
            for cls in sorted(BrokerPolicyDataClass)
        ),
        tuple(
            BrokerPolicyConstraintV1(name, BrokerPolicyConstraintMode.ANY)
            for name in (
                "account_class",
                "commercial_use",
                "eligibility",
                "feed_type",
                "geography",
            )
        ),
        (),
        2,
        2,
        100,
    )
    ack = BrokerPolicyAcknowledgementV1(
        policy.artifact_id,
        policy.evidence_ids,
        BrokerPolicyReferenceV1("operator", "generated operator", "b" * 64, 1),
        3,
        100,
    )
    return BrokerPolicyContextV1(
        (policy,),
        (evidence,),
        (ack,),
        (),
        (policy.artifact_id,),
        BrokerPolicyExecutionV1(
            False, "region-a", "retail", "quotes", ("eligible",)
        ),
    )


def core_subject(context, classes=(BrokerPolicyDataClass.NORMALIZED_QUOTES,)):
    return BrokerPolicySubjectV1(
        BrokerPolicyReferenceV1(
            "generated-subject", "not-native-proof", "c" * 64, 1
        ),
        (context.policies[0].binding,),
        tuple(sorted(classes)),
    )


def with_policy(context, policy):
    ack = replace(
        context.acknowledgements[0],
        policy_id=policy.artifact_id,
        evidence_ids=policy.evidence_ids,
        acknowledged_at_ns=max(
            3,
            policy.declared_at_ns,
            *(e.reviewed_at_ns for e in context.evidence),
        ),
    )
    return replace(
        context,
        policies=(policy,),
        acknowledgements=(ack,),
        selected_policy_ids=(policy.artifact_id,),
    )


class CoreMutableSource:
    def __init__(self, context):
        self.context = context
        self.reads = 0

    def read_policy_context(self):
        self.reads += 1
        return self.context
