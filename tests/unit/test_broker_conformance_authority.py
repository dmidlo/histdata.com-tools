"""Generated authority ABI admission, using only inert synthetic contracts."""

import hashlib
from dataclasses import replace

import pytest

from histdatacom.broker_plugin_capabilities import (
    BrokerCapabilityWorkflowV1,
    negotiate_broker_capabilities,
)
from histdatacom.broker_plugin_conformance import authority as authority_module
from histdatacom.broker_plugin_conformance.authority import generated_authority
from histdatacom.broker_plugin_permissions import (
    BrokerPermissionContextV1,
    BrokerPermissionError,
    BrokerPermissionManifestV1,
    BrokerPermissionResourcesV1,
    BrokerPermissionRevocationV1,
    broker_permission_scope,
)
from histdatacom.broker_plugin_permissions.scope import (
    current_host_resources,
    current_permission_authority,
    require_native_permissions,
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
from histdatacom.broker_plugin_registry import (
    BrokerPluginCandidateV1,
    BrokerPluginInventoryV1,
    BrokerPluginRegistrationV1,
)
from histdatacom.broker_plugins import BrokerConfigurationSchemaV1


def _generated(abi, *, omitted=(), denied=None):
    registration = BrokerPluginRegistrationV1(
        "org.histdatacom.generated-resource-abi",
        "1.0.0",
        "Generated resource ABI",
        "histdatacom-generated-resource-abi",
        "1.0.0",
        "generated_resource_abi.plugin:factory",
        "1.0.0",
        "2.0.0",
        ("generated-resource-abi",),
        tuple(
            sorted(
                (
                    "session.v1",
                    "events.v1",
                    "instruments.v1",
                    "quotes.v1",
                    "health.v1",
                )
            )
        ),
    )
    candidate = BrokerPluginCandidateV1(
        registration,
        hashlib.sha256(registration.to_json().encode("ascii")).hexdigest(),
        hashlib.sha256(b"Synthetic implementation; never loaded").hexdigest(),
    )
    workflow = BrokerCapabilityWorkflowV1(
        tuple(
            sorted(
                (
                    "metadata",
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
        required=("quotes.v1",),
    )
    plan = negotiate_broker_capabilities(
        BrokerPluginInventoryV1((candidate,)),
        workflow,
        plugin_id=registration.plugin_id,
    )
    request = BrokerSDKInvocationV1(
        plan,
        BrokerProviderConfigurationV1(
            "generated-resource-abi",
            "synthetic-profile",
            BrokerConfigurationSchemaV1(()).to_json(),
            "{}",
        ),
        BrokerProviderOutputContractV1("sdk-v1", ("quote",)),
    )
    manifest = BrokerPermissionManifestV1(
        candidate.artifact_id,
        registration.distribution_name,
        registration.distribution_version,
        "1.0.0",
        registration.provider_ids,
        ("emit:health", "emit:quotes"),
        resource_abi=abi,
    )
    return generated_authority(
        request, manifest, omitted=omitted, denied=denied
    )


def _revoke(generated):
    previous = generated.permissions.context
    generated.permissions.context = BrokerPermissionContextV1(
        previous.grants,
        (
            BrokerPermissionRevocationV1(
                generated.authority.grant_id, 0, "test"
            ),
        ),
        previous.revision + 1,
    )


def test_resource_free_manifest_is_admitted_without_resources():
    generated = _generated("none")
    assert generated.resources is None
    assert generated.authority.require_admission().effective_atoms == (
        "emit:health",
        "emit:quotes",
    )


def test_resource_free_access_admits_fresh_without_resource_constructor(
    monkeypatch,
):
    generated = _generated("none")
    reads = []

    def read_context():
        reads.append(generated.permissions.context.artifact_id)
        return generated.permissions.context

    def unexpected_resources(*args, **kwargs):
        pytest.fail("resource-less declaration reached resource constructor")

    monkeypatch.setattr(generated.permissions, "read_context", read_context)
    monkeypatch.setattr(
        authority_module, "BrokerPermissionResourcesV1", unexpected_resources
    )
    assert generated.resources is None
    assert len(reads) == 1
    assert generated.resources is None
    assert len(reads) == 2


def test_host_resources_preserve_exact_binding_and_single_admission(
    monkeypatch,
):
    generated = _generated("host_resources_v1")
    reads, constructions = [], []

    def read_context():
        reads.append(generated.permissions.context.artifact_id)
        return generated.permissions.context

    def resources(authority, *, provider_request):
        constructions.append((authority, provider_request))
        return BrokerPermissionResourcesV1(
            authority, provider_request=provider_request
        )

    monkeypatch.setattr(generated.permissions, "read_context", read_context)
    monkeypatch.setattr(
        authority_module, "BrokerPermissionResourcesV1", resources
    )
    result = generated.resources
    assert type(result) is BrokerPermissionResourcesV1
    assert result.authority is generated.authority
    assert constructions == [(generated.authority, generated.request)]
    assert len(reads) == 1


@pytest.mark.parametrize("abi", ("none", "host_resources_v1"))
@pytest.mark.parametrize(
    "denial,reason",
    (
        ("required", "required_permission_denied"),
        ("missing", "permission_grant_missing"),
        ("revoked", "permission_grant_revoked"),
    ),
)
def test_resource_access_preserves_typed_permission_denials(
    abi, denial, reason
):
    generated = _generated(
        abi, omitted=("emit:quotes",) if denial == "required" else ()
    )
    if denial == "missing":
        generated.permissions.context = BrokerPermissionContextV1(())
    elif denial == "revoked":
        _revoke(generated)
    with pytest.raises(BrokerPermissionError) as failure:
        _ = generated.resources
    assert failure.value.reason == reason
    assert failure.value.decision is not None
    assert not failure.value.decision.admitted


@pytest.mark.parametrize("abi", ("none", "host_resources_v1"))
def test_scope_exposes_only_declared_resources(abi):
    generated = _generated(abi)
    resources = generated.resources
    with broker_permission_scope(generated.authority, resources=resources):
        assert current_permission_authority() is generated.authority
        assert (
            require_native_permissions(generated.request) is generated.authority
        )
        if abi == "none":
            with pytest.raises(BrokerPermissionError) as failure:
                current_host_resources()
            assert failure.value.reason == "host_resources_not_declared"
        else:
            assert current_host_resources() is resources


@pytest.mark.parametrize("abi", ("none", "host_resources_v1"))
def test_scope_checks_fresh_revocation_before_resource_availability(abi):
    generated = _generated(abi)
    with broker_permission_scope(
        generated.authority, resources=generated.resources
    ):
        _revoke(generated)
        with pytest.raises(BrokerPermissionError) as failure:
            current_host_resources()
        assert failure.value.reason == "permission_grant_revoked"
        with pytest.raises(BrokerPermissionError) as failure:
            _ = generated.resources
        assert failure.value.reason == "permission_grant_revoked"


def test_host_resources_reject_mismatched_provider_configuration():
    generated = _generated("host_resources_v1")
    request = replace(
        generated.request,
        configuration_profile=replace(
            generated.request.configuration_profile,
            profile_id="different-synthetic-profile",
        ),
    )
    generated = replace(generated, request=request)
    with pytest.raises(
        ValueError, match="provider resource invocation binding mismatch"
    ):
        _ = generated.resources


def test_resource_free_scope_rejects_mismatched_native_invocation():
    generated = _generated("none")
    request = replace(
        generated.request,
        configuration_profile=replace(
            generated.request.configuration_profile,
            profile_id="different-synthetic-profile",
        ),
    )
    with broker_permission_scope(
        generated.authority, resources=generated.resources
    ):
        with pytest.raises(BrokerPermissionError) as failure:
            require_native_permissions(request)
        assert failure.value.reason == "native_permission_binding_mismatch"


@pytest.mark.parametrize("abi", ("none", "host_resources_v1"))
def test_resource_admission_never_overrides_provider_denial(abi):
    generated = _generated(abi, denied=BrokerPolicyOperation.INVOKE)
    with (
        provider_policy_scope(generated.policy),
        broker_permission_scope(
            generated.authority, resources=generated.resources
        ),
        pytest.raises(BrokerPolicyError) as failure,
    ):
        require_provider_operation(
            generated.request, BrokerPolicyOperation.INVOKE
        )
    assert failure.value.reason == "operation_not_allowed"
    assert failure.value.decision is not None
    assert not failure.value.decision.allowed
