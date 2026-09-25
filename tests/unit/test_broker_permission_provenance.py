"""Synthetic native bindings and historical permission-snapshot replay."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from types import SimpleNamespace

import pytest

from histdatacom.broker_plugin_capabilities import (
    BrokerInvocationAssociation,
    BrokerInvocationBindingV1,
    negotiate_broker_capabilities,
    validate_broker_capability_event,
    validate_broker_metadata,
)
from histdatacom.broker_plugin_lifecycle.contracts import (
    BrokerLifecycleCompletion,
    BrokerLifecycleHeaderV1,
    BrokerLifecycleManifestV1,
    BrokerLifecyclePolicyV1,
    BrokerLifecycleState,
)
from histdatacom.broker_plugin_permissions import (
    BrokerPermissionAuthorityV1,
    BrokerPermissionBindingV1,
    BrokerPermissionContextV1,
    BrokerPermissionError,
    BrokerPermissionExecutionV1,
    BrokerPermissionGrantV1,
    BrokerPermissionManifestV1,
    BrokerPermissionRevocationV1,
    build_permission_execution,
    verify_permission_execution,
)
from histdatacom.broker_plugin_permissions.provenance import _chunks
from histdatacom.broker_plugin_registry import (
    BrokerPluginCandidateV1,
    BrokerPluginInventoryV1,
)
from histdatacom.broker_plugin_security.contracts import (
    BrokerSecurityMode,
    BrokerSecurityPolicyV1,
    BrokerSoftwareProvenanceV1,
    BrokerTrustedSecurityReceiptV1,
    BrokerTrustTier,
)
from histdatacom.broker_plugins import (
    BrokerActivitySemantics,
    BrokerActivityV1,
    BrokerEventKind,
    BrokerEventV1,
    BrokerExtensionV1,
    BrokerPluginMetadataV1,
    BrokerQuoteV1,
    BrokerReceiveTimeV1,
    BrokerSessionV1,
)
from tests.fixtures.broker_provider_policy import sdk_policy_invocation


class _Source:
    def __init__(self, context):
        self.context = context
        self.reads = 0

    def read_context(self):
        self.reads += 1
        return self.context


def _inputs(extra_candidates=0, *, extra_capabilities=()):
    original = sdk_policy_invocation(extra_capabilities=extra_capabilities)
    candidate = original.plan.candidate
    candidates = (candidate,) + tuple(
        BrokerPluginCandidateV1(
            replace(
                candidate.registration,
                plugin_id=f"org.example.extra{index}",
                distribution_name=f"other-fixture-{index}",
            ),
            hashlib.sha256(f"registration-{index}".encode()).hexdigest(),
            hashlib.sha256(f"implementation-{index}".encode()).hexdigest(),
        )
        for index in range(extra_candidates)
    )
    inventory = BrokerPluginInventoryV1(candidates)
    plan = negotiate_broker_capabilities(
        inventory,
        original.plan.workflow,
        plugin_id=candidate.registration.plugin_id,
    )
    invocation = replace(
        original,
        plan=plan,
        configuration_profile=replace(
            original.configuration_profile,
            provider_id=candidate.registration.provider_ids[0],
        ),
    )
    header = BrokerLifecycleHeaderV1(
        inventory,
        plan,
        BrokerLifecyclePolicyV1(),
        (),
        "a" * 32,
        "3.10.19",
    )
    manifest = BrokerPermissionManifestV1(
        candidate.artifact_id,
        candidate.registration.distribution_name,
        candidate.registration.distribution_version,
        "1.0.0",
        candidate.registration.provider_ids,
        ("emit:health", "emit:quotes"),
        resource_abi="none",
    )
    binding = BrokerPermissionBindingV1(
        candidate.artifact_id,
        manifest.artifact_id,
        "1.0.0",
        invocation.configuration_profile.provider_id,
        invocation.configuration_profile.artifact_id,
    )
    grant = BrokerPermissionGrantV1(
        binding, manifest.declared_atoms, "operator", 10, 30, "b" * 32
    )
    source = _Source(BrokerPermissionContextV1((grant,)))
    now = [15]
    authority = BrokerPermissionAuthorityV1(
        manifest, binding, grant.artifact_id, source, lambda: now[0]
    )
    return invocation, header, authority, source, now


def _trusted(invocation, *, quote=None, metadata_extensions=()):
    candidate = invocation.plan.candidate
    registration = candidate.registration
    metadata = BrokerPluginMetadataV1(
        registration.plugin_id,
        registration.plugin_version,
        registration.display_name,
        extensions=metadata_extensions,
    )
    session = BrokerSessionV1(
        metadata.artifact_id, "c" * 32, 100, "fixture-clock"
    )
    event = BrokerEventV1(
        session.artifact_id,
        0,
        BrokerEventKind.QUOTE,
        "fixture-connection",
        receive_time=BrokerReceiveTimeV1(200, 10, "fixture-clock"),
        instrument="EURUSD",
        quote=quote or BrokerQuoteV1("EURUSD", "1.1000", "1.1002"),
    )
    native_binding = BrokerInvocationBindingV1(
        invocation.plan.artifact_id,
        candidate.artifact_id,
        metadata.artifact_id,
        BrokerInvocationAssociation.INSTALLED_ENTRYPOINT,
        candidate.implementation_sha256,
    )
    return BrokerTrustedSecurityReceiptV1(
        BrokerSecurityPolicyV1(
            candidate.artifact_id,
            BrokerTrustTier.REVIEWED,
            BrokerSecurityMode.TRUSTED_IN_PROCESS,
        ),
        BrokerSoftwareProvenanceV1(
            candidate.artifact_id,
            registration.distribution_name,
            registration.distribution_version,
            "1.0.0",
            candidate.registration_sha256,
            candidate.implementation_sha256,
        ),
        invocation.plan.to_json(),
        validate_broker_metadata(invocation.plan, metadata).to_json(),
        native_binding.to_json(),
        session.to_json(),
        (validate_broker_capability_event(invocation.plan, event).to_json(),),
        invocation.configuration_profile.public_configuration_json,
    )


@pytest.mark.parametrize("family", ["header", "manifest", "trusted"])
def test_complete_native_families_roundtrip_and_single_atomic_source_snapshot(
    family: str,
) -> None:
    invocation, header, authority, source, _ = _inputs()
    native = (
        header
        if family == "header"
        else (
            BrokerLifecycleManifestV1(
                header,
                BrokerLifecycleState.DISCOVERED,
                BrokerLifecycleCompletion.OPEN,
            )
            if family == "manifest"
            else _trusted(invocation)
        )
    )
    expected_native_bytes = native.to_json()
    execution = build_permission_execution(invocation, native, authority)
    assert source.reads == 1
    assert execution.native_artifact_id == native.artifact_id
    assert (
        execution.native_artifact_sha256
        == hashlib.sha256(expected_native_bytes.encode("ascii")).hexdigest()
    )
    assert execution.native_artifact_json == expected_native_bytes
    assert execution.context.artifact_id == execution.decision.context_id
    restored = BrokerPermissionExecutionV1.from_json(execution.to_json())
    assert restored == execution
    verify_permission_execution(restored, invocation, native)
    assert (
        source.reads == 1
    )  # Offline verification never contacts an authority.
    assert native.to_json() == expected_native_bytes


@pytest.mark.parametrize("field", ["activity", "metadata_extensions"])
def test_trusted_native_quantities_and_opaque_metadata_require_exact_atoms(
    field,
):
    invocation, _, authority, _, _ = _inputs(
        extra_capabilities=("activity.message-count.v1",)
    )
    native = _trusted(
        invocation,
        quote=(
            BrokerQuoteV1(
                "EURUSD",
                "1.1000",
                "1.1002",
                activity=BrokerActivityV1(
                    "1", BrokerActivitySemantics.MESSAGE_COUNT, "message"
                ),
            )
            if field == "activity"
            else None
        ),
        metadata_extensions=(
            (BrokerExtensionV1("org.example.capability", '{"synthetic":true}'),)
            if field == "metadata_extensions"
            else ()
        ),
    )
    with pytest.raises(ValueError, match="exceeds retained resource grants"):
        build_permission_execution(invocation, native, authority)


def test_admission_snapshot_cannot_pair_decision_with_second_source_revision() -> (
    None
):
    invocation, header, authority, source, _ = _inputs()
    original = source.context

    def changing():
        source.reads += 1
        if source.reads > 1:
            grant = original.grants[0]
            return BrokerPermissionContextV1(
                original.grants,
                (
                    BrokerPermissionRevocationV1(
                        grant.artifact_id, 12, "operator-denied"
                    ),
                ),
                1,
            )
        return original

    source.read_context = changing
    execution = build_permission_execution(invocation, header, authority)
    assert source.reads == 1
    assert execution.context == original
    with pytest.raises(BrokerPermissionError, match="permission_grant_revoked"):
        authority.require_admission()
    # Retained evidence remains historically verifiable, never a current grant.
    verify_permission_execution(execution, invocation, header)


@pytest.mark.parametrize(
    "field,value",
    [
        ("native_artifact_id", "broker-lifecycle-header:sha256:" + "f" * 64),
        ("native_artifact_sha256", "f" * 64),
        ("native_kind", "caller_verified_object"),
    ],
)
def test_hash_shaped_labels_or_unknown_native_families_cannot_substitute(
    field, value
) -> None:
    invocation, header, authority, _, _ = _inputs()
    execution = build_permission_execution(invocation, header, authority)
    with pytest.raises(ValueError):
        replace(execution, **{field: value})
    with pytest.raises(ValueError, match="exact native"):
        build_permission_execution(
            invocation,
            SimpleNamespace(artifact_id=header.artifact_id),
            authority,
        )


@pytest.mark.parametrize(
    "mutation",
    [
        "candidate",
        "profile",
        "provider",
        "sdk",
        "manifest",
        "decision",
        "context",
    ],
)
def test_resealed_cross_identity_substitutions_refuse(mutation: str) -> None:
    invocation, header, authority, source, _ = _inputs()
    execution = build_permission_execution(invocation, header, authority)
    if mutation in ("profile", "provider"):
        changed = replace(
            invocation,
            configuration_profile=replace(
                invocation.configuration_profile,
                **(
                    {"profile_id": "different-profile"}
                    if mutation == "profile"
                    else {"provider_id": "other"}
                ),
            ),
        )
        changes = {"invocation_chunks": _chunks(changed.to_json())}
    elif mutation == "candidate":
        changes = {
            "binding": replace(
                execution.binding,
                candidate_id="broker-plugin-candidate:sha256:" + "d" * 64,
            )
        }
    elif mutation == "sdk":
        changes = {"binding": replace(execution.binding, sdk_version="2.0.0")}
    elif mutation == "manifest":
        changes = {
            "manifest": replace(
                execution.manifest, distribution_version="1.0.1"
            )
        }
    elif mutation == "decision":
        changes = {
            "decision": replace(
                execution.decision, effective_atoms=("emit:quotes",)
            )
        }
    else:
        grant = source.context.grants[0]
        changes = {
            "context": replace(
                source.context,
                revocations=(
                    BrokerPermissionRevocationV1(
                        grant.artifact_id, 12, "operator-denied"
                    ),
                ),
                revision=1,
            )
        }
    with pytest.raises(ValueError):
        replace(execution, **changes)


def test_exact_native_artifact_substitution_detected_even_when_resealed_and_valid() -> (
    None
):
    invocation, header, authority, _, _ = _inputs()
    execution = build_permission_execution(invocation, header, authority)
    changed = replace(header, run_nonce="d" * 32)
    with pytest.raises(ValueError, match="supplied native artifacts differ"):
        verify_permission_execution(execution, invocation, changed)
    changed_execution = build_permission_execution(
        invocation, changed, authority
    )
    assert changed_execution.artifact_id != execution.artifact_id
    assert changed_execution.native_artifact_id != execution.native_artifact_id


def test_native_trusted_public_configuration_must_match_reviewed_profile() -> (
    None
):
    invocation, _, authority, _, _ = _inputs()
    receipt = replace(
        _trusted(invocation), public_configuration_json='{"unreviewed":true}'
    )
    with pytest.raises(ValueError, match="trusted native receipt"):
        build_permission_execution(invocation, receipt, authority)


def test_historical_expiration_does_not_rewrite_evidence_or_create_a_live_permit() -> (
    None
):
    invocation, header, authority, _, now = _inputs()
    execution = build_permission_execution(invocation, header, authority)
    now[0] = 30
    verify_permission_execution(execution, invocation, header)
    with pytest.raises(BrokerPermissionError, match="permission_grant_expired"):
        build_permission_execution(invocation, header, authority)


def test_large_exact_native_header_uses_canonical_bounded_chunks() -> None:
    invocation, header, authority, _, _ = _inputs(extra_candidates=80)
    assert len(header.to_json()) > 65_536
    execution = build_permission_execution(invocation, header, authority)
    assert len(execution.native_artifact_chunks) > 2
    assert all(
        len(chunk) == 32_768 for chunk in execution.native_artifact_chunks[:-1]
    )
    verify_permission_execution(execution, invocation, header)
    text = execution.native_artifact_json
    with pytest.raises(ValueError, match="chunks are not canonical"):
        replace(
            execution,
            native_artifact_chunks=(
                text[:100],
                text[100:200],
                *execution.native_artifact_chunks[1:],
            ),
        )


def test_private_configuration_names_cannot_become_retained_plaintext_permission_mode() -> (
    None
):
    from histdatacom.broker_plugins import (
        BrokerConfigurationFieldV1,
        BrokerConfigurationSchemaV1,
        BrokerConfigurationType,
    )

    invocation, header, authority, _, _ = _inputs()
    profile = replace(
        invocation.configuration_profile,
        schema_json=BrokerConfigurationSchemaV1(
            (
                BrokerConfigurationFieldV1(
                    "secret",
                    BrokerConfigurationType.STRING,
                    "Generated only",
                    secret=True,
                ),
            )
        ).to_json(),
        private_field_names=("secret",),
    )
    changed = replace(invocation, configuration_profile=profile)
    # Even a correctly selected grant for that profile must not authorize the
    # deprecated raw-secret configuration route through this new permission ABI.
    binding = replace(authority.binding, configuration_id=profile.artifact_id)
    grant = BrokerPermissionGrantV1(
        binding, authority.manifest.declared_atoms, "operator", 10, 30, "e" * 32
    )
    selected = BrokerPermissionAuthorityV1(
        authority.manifest,
        binding,
        grant.artifact_id,
        _Source(BrokerPermissionContextV1((grant,))),
        lambda: 15,
    )
    with pytest.raises(ValueError, match="exact native invocation"):
        build_permission_execution(changed, header, selected)
