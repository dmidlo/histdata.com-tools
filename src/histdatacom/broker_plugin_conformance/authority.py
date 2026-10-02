"""Explicit generated-only authority, never a provider-name rights exemption."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from histdatacom.broker_plugin_permissions import (
    BrokerPermissionAuthorityV1,
    BrokerPermissionBindingV1,
    BrokerPermissionContextV1,
    BrokerPermissionGrantV1,
    BrokerPermissionManifestV1,
    BrokerPermissionResourcesV1,
)
from histdatacom.broker_plugin_policy import (
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
    BrokerProviderPolicyV1,
    BrokerSDKInvocationV1,
    sdk_invocation_binding,
)

EMISSION_ATOMS = frozenset(
    ("emit:health", "emit:quotes", "emit:sizes", "raw_payload:emit")
)


def generated_policy_context(
    binding: BrokerPolicyBindingV1,
    denied: BrokerPolicyOperation | None = None,
) -> BrokerPolicyContextV1:
    """An explicit synthetic operator declaration bound to exact fixture inputs."""
    terms = b"Generated conformance terms; no real provider authorization."
    reference = BrokerPolicyReferenceV1(
        "synthetic-conformance-terms",
        "generated-conformance-v1",
        hashlib.sha256(terms).hexdigest(),
        len(terms),
    )
    evidence = BrokerPolicyEvidenceV1(
        reference,
        binding.provider_id,
        "generated-conformance",
        "1",
        "2020-01-01",
        "synthetic-conformance-issuer",
        1,
        # An explicit operator declaration over this generated-only fixture.
        # SYNTHETIC evidence intentionally cannot authorize native operations.
        BrokerPolicyEvidenceKind.DECLARED,
        "fixture:generated-conformance",
    )
    rules = tuple(
        BrokerPolicyRuleV1(
            operation,
            data_class,
            (
                BrokerPolicyStatus.DENIED
                if operation is denied
                else BrokerPolicyStatus.ALLOWED
            ),
            (evidence.artifact_id,),
            (
                BrokerPolicyRetention.UNBOUNDED
                if operation is BrokerPolicyOperation.RETAIN_LOCAL
                else BrokerPolicyRetention.NOT_APPLICABLE
            ),
        )
        for operation in sorted(BrokerPolicyOperation)
        for data_class in sorted(BrokerPolicyDataClass)
    )
    policy = BrokerProviderPolicyV1(
        "1.0.0",
        binding,
        "MIT",
        "MIT",
        (evidence.artifact_id,),
        rules,
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
        2**63 - 1,
    )
    operator = b"generated-conformance-operator"
    acknowledgement = BrokerPolicyAcknowledgementV1(
        policy.artifact_id,
        (evidence.artifact_id,),
        BrokerPolicyReferenceV1(
            "synthetic-operator-reference",
            "generated-operator",
            hashlib.sha256(operator).hexdigest(),
            len(operator),
        ),
        3,
        2**63 - 1,
    )
    return BrokerPolicyContextV1(
        (policy,),
        (evidence,),
        (acknowledgement,),
        (),
        (policy.artifact_id,),
        BrokerPolicyExecutionV1(False, "synthetic", "fixture", "fixture"),
    )


@dataclass
class PermissionSource:
    context: BrokerPermissionContextV1

    def read_context(self) -> BrokerPermissionContextV1:
        return self.context


@dataclass
class PolicySource:
    context: BrokerPolicyContextV1

    def read_policy_context(self) -> BrokerPolicyContextV1:
        return self.context


@dataclass(frozen=True)
class GeneratedAuthority:
    authority: BrokerPermissionAuthorityV1
    request: BrokerSDKInvocationV1
    permissions: PermissionSource
    policy: PolicySource

    @property
    def resources(self) -> BrokerPermissionResourcesV1 | None:
        if self.authority.manifest.resource_abi == "none":
            self.authority.require_admission()
            return None
        return BrokerPermissionResourcesV1(
            self.authority, provider_request=self.request
        )


def generated_authority(
    request: BrokerSDKInvocationV1,
    manifest: BrokerPermissionManifestV1,
    *,
    omitted: tuple[str, ...] = (),
    denied: BrokerPolicyOperation | None = None,
) -> GeneratedAuthority:
    # A driver cannot turn diagnostic execution into ambient network, filesystem,
    # secrets or subprocess authorization. These require a separate explicit
    # resource driver/profile, and remain unqualified in this emission ABI.
    binding = BrokerPermissionBindingV1(
        request.plan.candidate.artifact_id,
        manifest.artifact_id,
        manifest.sdk_version,
        request.configuration_profile.provider_id,
        request.configuration_profile.artifact_id,
    )
    grant = BrokerPermissionGrantV1(
        binding,
        tuple(
            sorted(
                (
                    set(manifest.required_atoms + manifest.optional_atoms)
                    & EMISSION_ATOMS
                )
                - set(omitted)
            )
        ),
        "generated-conformance-operator",
        0,
        2**63 - 1,
        "a" * 32,
    )
    permissions = PermissionSource(BrokerPermissionContextV1((grant,)))
    authority = BrokerPermissionAuthorityV1(
        manifest, binding, grant.artifact_id, permissions
    )
    policy = PolicySource(
        generated_policy_context(sdk_invocation_binding(request), denied)
    )
    return GeneratedAuthority(
        authority,
        request,
        permissions,
        policy,
    )
