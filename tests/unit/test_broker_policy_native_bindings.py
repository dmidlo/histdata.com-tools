"""Closed native root/parent/classification canaries, all generated in memory."""

from dataclasses import replace
import hashlib
from pathlib import Path
import subprocess
import sys

import pytest

from histdatacom.broker_plugin_capabilities import (
    BrokerCapabilityWorkflowV1,
    BrokerInvocationAssociation,
    BrokerInvocationBindingV1,
    negotiate_broker_capabilities,
    validate_broker_capability_event,
    validate_broker_instrument,
    validate_broker_metadata,
)
from histdatacom.broker_plugin_lifecycle.contracts import (
    BrokerLifecycleCompletion,
    BrokerLifecycleHeaderV1,
    BrokerLifecycleIdentityV1,
    BrokerLifecycleManifestV1,
    BrokerLifecyclePolicyV1,
    BrokerLifecycleReason,
    BrokerLifecycleRecordV1,
    BrokerLifecycleSessionV1,
    BrokerLifecycleState,
    BrokerLifecycleTransitionV1,
)
from histdatacom.broker_plugin_policy.bindings import (
    BrokerFingerprintFitV1,
    BrokerLegacyCaptureV1,
    BrokerLegacyRecordV1,
    BrokerProviderConfigurationV1,
    BrokerSDKInvocationV1,
    BrokerSDKLifecycleV1,
    BrokerSDKRecordV1,
    BrokerSDKSecurityV1,
    legacy_capture_binding,
    native_provider_artifact_json,
    resolve_provider_subject,
    sdk_invocation_binding,
)
from histdatacom.broker_plugin_policy.contracts import (
    BrokerPolicyDataClass as DataClass,
)
from histdatacom.broker_plugin_registry import BrokerPluginInventoryV1
from histdatacom.broker_plugin_security.contracts import (
    BrokerSecurityMode,
    BrokerSecurityPolicyV1,
    BrokerSecurityReceiptV1,
    BrokerSoftwareProvenanceV1,
    BrokerTrustedSecurityReceiptV1,
    BrokerTrustTier,
)
from histdatacom.broker_plugins import (
    BrokerConfigurationFieldV1,
    BrokerConfigurationSchemaV1,
    BrokerConfigurationType,
    BrokerEventKind,
    BrokerEventV1,
    BrokerExtensionV1,
    BrokerInstrumentV1,
    BrokerPluginMetadataV1,
    BrokerQuoteV1,
    BrokerReceiveTimeV1,
    BrokerSessionV1,
)
from tests.fixtures.broker_provider_policy import (
    legacy_output_contract,
    legacy_policy_inputs,
    sdk_policy_invocation,
)


def sdk_roots(*, opaque=False):
    invocation = sdk_policy_invocation()
    if opaque:
        invocation = replace(
            invocation,
            output_contract=replace(
                invocation.output_contract, allow_opaque_metadata=True
            ),
        )
    registration = invocation.plan.candidate.registration
    metadata = BrokerPluginMetadataV1(
        registration.plugin_id,
        registration.plugin_version,
        registration.display_name,
        extensions=(
            (
                BrokerExtensionV1(
                    "org.example.context", '{"note":"generated opaque input"}'
                ),
            )
            if opaque
            else ()
        ),
    )
    session = BrokerSessionV1(
        metadata.artifact_id, "a" * 32, 100, "generated-clock"
    )
    event = BrokerEventV1(
        session.artifact_id,
        0,
        BrokerEventKind.QUOTE,
        "generated-connection",
        receive_time=BrokerReceiveTimeV1(200, 10, "generated-clock"),
        instrument="EURUSD",
        quote=BrokerQuoteV1("EURUSD", "1.1000", "1.1002"),
    )
    binding = BrokerInvocationBindingV1(
        invocation.plan.artifact_id,
        invocation.plan.candidate.artifact_id,
        metadata.artifact_id,
        BrokerInvocationAssociation.INSTALLED_ENTRYPOINT,
        invocation.plan.candidate.implementation_sha256,
    )
    return invocation, metadata, session, event, binding


def lifecycle_roots(*, opaque=False):
    invocation, metadata, session, event, binding = sdk_roots(opaque=opaque)
    header = BrokerLifecycleHeaderV1(
        BrokerPluginInventoryV1((invocation.plan.candidate,)),
        invocation.plan,
        BrokerLifecyclePolicyV1(),
        (),
        "a" * 32,
        "3.10.19",
    )
    identity = BrokerLifecycleIdentityV1(
        binding,
        validate_broker_metadata(invocation.plan, metadata),
        invocation.configuration_profile.schema,
    )
    native_session = BrokerLifecycleSessionV1(session, ())
    admitted = validate_broker_capability_event(invocation.plan, event)
    record = BrokerLifecycleRecordV1(
        header.artifact_id, 0, 0, 200, 10, "event", admitted.to_json(), 0
    )
    return invocation, header, identity, native_session, record


def security_roots(*, isolated=False, opaque=False):
    invocation, metadata, session, event, binding = sdk_roots(opaque=opaque)
    candidate = invocation.plan.candidate
    software = BrokerSoftwareProvenanceV1(
        candidate.artifact_id,
        candidate.registration.distribution_name,
        candidate.registration.distribution_version,
        "1.0.0",
        candidate.registration_sha256,
        candidate.implementation_sha256,
    )
    policy = BrokerSecurityPolicyV1(
        candidate.artifact_id,
        BrokerTrustTier.REVIEWED,
        (
            BrokerSecurityMode.KERNEL_ISOLATED
            if isolated
            else BrokerSecurityMode.TRUSTED_IN_PROCESS
        ),
    )
    if isolated:
        _, header, _, _, _ = lifecycle_roots(opaque=opaque)
        manifest = BrokerLifecycleManifestV1(
            header,
            BrokerLifecycleState.DISCOVERED,
            BrokerLifecycleCompletion.OPEN,
        )
        receipt = BrokerSecurityReceiptV1(
            policy, software, manifest.artifact_id, "{}", "macos-seatbelt-v1"
        )
        return BrokerSDKSecurityV1(invocation, receipt, manifest)
    receipt = BrokerTrustedSecurityReceiptV1(
        policy,
        software,
        invocation.plan.to_json(),
        validate_broker_metadata(invocation.plan, metadata).to_json(),
        binding.to_json(),
        session.to_json(),
        (validate_broker_capability_event(invocation.plan, event).to_json(),),
        "{}",
    )
    return BrokerSDKSecurityV1(invocation, receipt)


def test_secret_schema_descriptors_are_allowed_but_private_values_never_bound():
    invocation = sdk_policy_invocation()
    schema = BrokerConfigurationSchemaV1(
        (
            BrokerConfigurationFieldV1(
                "password",
                BrokerConfigurationType.STRING,
                "Private account credential",
                secret=True,
            ),
            BrokerConfigurationFieldV1(
                "endpoint", BrokerConfigurationType.STRING, "Public endpoint"
            ),
        )
    )
    profile = BrokerProviderConfigurationV1(
        "generated-sdk-provider",
        "reviewed-opaque-profile",
        schema.to_json(),
        '{"endpoint":"https://example.invalid"}',
        ("password",),
    )
    secret = "generated-secret-value-not-to-be-retained"
    profile.verify_configuration(
        {"password": secret, "endpoint": "https://example.invalid"}
    )
    invocation = replace(invocation, configuration_profile=profile)
    binding = sdk_invocation_binding(invocation)
    assert secret not in binding.to_json()
    assert hashlib.sha256(secret.encode()).hexdigest() not in binding.to_json()
    assert "password" in binding.to_json()  # Field name, never field value.
    assert (
        DataClass.PRIVATE_ACCOUNT
        in resolve_provider_subject(invocation).data_classes
    )
    assert BrokerSDKInvocationV1.from_json(invocation.to_json()) == invocation
    with pytest.raises(ValueError):
        profile.verify_configuration(
            {"password": secret, "endpoint": "changed"}
        )


@pytest.mark.parametrize("name", ["password", "api_key", "token"])
def test_nonsecret_schema_label_cannot_whitelist_credential_key(name):
    schema = BrokerConfigurationSchemaV1(
        (
            BrokerConfigurationFieldV1(
                name,
                BrokerConfigurationType.STRING,
                "Incorrect public declaration",
            ),
        )
    )
    with pytest.raises(ValueError, match="credential"):
        BrokerProviderConfigurationV1(
            "provider",
            "profile",
            schema.to_json(),
            '{"' + name + '":"must-not-retain"}',
        )


@pytest.mark.parametrize(
    "mutation", ["missing", "foreign_metadata", "foreign_module"]
)
def test_sdk_binding_requires_exact_metadata_and_implementation(mutation):
    invocation, metadata, _, _, binding = sdk_roots()
    if mutation == "foreign_module":
        binding = replace(binding, module_sha256="f" * 64)
    elif mutation == "foreign_metadata":
        binding = replace(
            binding,
            metadata_id=replace(
                metadata, display_name="other metadata"
            ).artifact_id,
        )
    parent = None if mutation == "missing" else metadata
    with pytest.raises(ValueError, match="metadata|implementation"):
        resolve_provider_subject(
            BrokerSDKRecordV1(invocation, binding, metadata=parent)
        )


def test_sdk_binding_positive_parent_and_raw_instrument_capability_replay():
    invocation, metadata, _, _, binding = sdk_roots()
    wrapper = BrokerSDKRecordV1(invocation, binding, metadata=metadata)
    assert native_provider_artifact_json(wrapper) == binding.to_json()
    instrument = BrokerInstrumentV1("EURUSD", "EURUSD", "EUR", "USD", "0.0001")
    # An instrument query must be present in the selected workflow, not merely
    # supported by its registration or structurally valid in the SDK.
    no_instruments_plan = negotiate_broker_capabilities(
        BrokerPluginInventoryV1((invocation.plan.candidate,)),
        BrokerCapabilityWorkflowV1(("metadata",)),
        plugin_id=invocation.plan.candidate.registration.plugin_id,
    )
    with pytest.raises(ValueError):
        resolve_provider_subject(
            BrokerSDKRecordV1(
                replace(invocation, plan=no_instruments_plan),
                instrument,
                metadata=metadata,
            )
        )
    instrument = replace(instrument, price_increment=None)
    admitted = validate_broker_instrument(invocation.plan, instrument)
    assert (
        native_provider_artifact_json(
            BrokerSDKRecordV1(invocation, admitted, metadata=metadata)
        )
        == admitted.to_json()
    )


def test_supplied_sdk_metadata_must_be_the_record_not_unrelated_parent():
    invocation, metadata, _, _, _ = sdk_roots()
    with pytest.raises(ValueError, match="differs from supplied parent"):
        resolve_provider_subject(
            BrokerSDKRecordV1(
                invocation,
                metadata,
                metadata=replace(metadata, display_name="other metadata"),
            )
        )


@pytest.mark.parametrize("kind", ["event", "transition", "identity", "session"])
def test_lifecycle_preserves_full_parent_class_closure_and_exact_payload(kind):
    invocation, header, identity, session, event_record = lifecycle_roots(
        opaque=True
    )
    records = {
        "event": event_record,
        "identity": identity,
        "session": session,
        "transition": BrokerLifecycleTransitionV1(
            BrokerLifecycleState.CONFIGURED,
            BrokerLifecycleState.STARTING,
            BrokerLifecycleReason.STARTING,
            0,
        ),
    }
    record = records[kind]
    wrapper = BrokerSDKLifecycleV1(
        invocation, header, record, session, identity
    )
    subject = resolve_provider_subject(wrapper)
    assert DataClass.RAW_PAYLOAD in subject.data_classes
    assert (DataClass.NORMALIZED_QUOTES in subject.data_classes) is (
        kind == "event"
    )
    assert native_provider_artifact_json(wrapper) == record.to_json()


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_identity",
        "missing_session",
        "foreign_session",
        "foreign_plan",
        "foreign_run",
        "foreign_identity_payload",
    ],
)
def test_lifecycle_links_event_session_metadata_plan_and_actual_run(mutation):
    invocation, header, identity, session, record = lifecycle_roots()
    if mutation == "missing_identity":
        identity = None
    elif mutation == "missing_session":
        session = None
    elif mutation == "foreign_session":
        session = replace(
            session, session=replace(session.session, instance_nonce="b" * 32)
        )
    elif mutation == "foreign_plan":
        invocation = replace(
            invocation,
            plan=replace(
                invocation.plan,
                candidate=replace(
                    invocation.plan.candidate, implementation_sha256="f" * 64
                ),
            ),
        )
    elif mutation == "foreign_run":
        record = replace(
            record, run_id=replace(header, run_nonce="b" * 32).artifact_id
        )
    else:
        different = replace(
            identity,
            configuration_schema=BrokerConfigurationSchemaV1(
                (
                    BrokerConfigurationFieldV1(
                        "different",
                        BrokerConfigurationType.STRING,
                        "Other schema",
                    ),
                )
            ),
        )
        record = different
    with pytest.raises(ValueError):
        resolve_provider_subject(
            BrokerSDKLifecycleV1(invocation, header, record, session, identity)
        )


@pytest.mark.parametrize("isolated", [False, True])
def test_native_security_payload_is_exact_and_parent_linked(isolated):
    wrapper = security_roots(isolated=isolated)
    subject = resolve_provider_subject(wrapper)
    assert DataClass.CONTENT_HASHES in subject.data_classes
    assert native_provider_artifact_json(wrapper) == wrapper.receipt.to_json()
    if isolated:
        with pytest.raises(ValueError, match="exact lifecycle manifest"):
            resolve_provider_subject(replace(wrapper, manifest=None))
        with pytest.raises(ValueError, match="manifest differs"):
            resolve_provider_subject(
                replace(
                    wrapper,
                    manifest=replace(
                        wrapper.manifest, stdout_discarded_bytes=1
                    ),
                )
            )


@pytest.mark.parametrize(
    "field,value",
    [
        ("registration_sha256", "f" * 64),
        ("implementation_sha256", "f" * 64),
        ("distribution_name", "other-distribution"),
        ("distribution_version", "9.0.0"),
        ("sdk_version", "9.0.0"),
    ],
)
def test_isolated_security_refuses_resealed_software_provenance_mismatch(
    field, value
):
    wrapper = security_roots(isolated=True)
    receipt = replace(
        wrapper.receipt,
        software=replace(wrapper.receipt.software, **{field: value}),
    )
    with pytest.raises(ValueError, match="software provenance"):
        resolve_provider_subject(replace(wrapper, receipt=receipt))


def test_trusted_security_embedded_metadata_keeps_opaque_class():
    assert (
        DataClass.RAW_PAYLOAD
        in resolve_provider_subject(security_roots(opaque=True)).data_classes
    )


@pytest.mark.parametrize("expected_only", ["legacy", "sdk"])
def test_expected_only_request_is_not_persistable_native_output(expected_only):
    request = (
        sdk_policy_invocation()
        if expected_only == "sdk"
        else BrokerLegacyCaptureV1(
            legacy_policy_inputs().session, legacy_output_contract()
        )
    )
    assert resolve_provider_subject(request).native_ref.byte_length > 0
    with pytest.raises(ValueError, match="no storable native artifact"):
        native_provider_artifact_json(request)


def test_legacy_native_bytes_and_configuration_projection_stay_unchanged():
    inputs = legacy_policy_inputs()
    for record in (inputs.session, inputs.events[3], inputs.messages[3]):
        wrapper = BrokerLegacyRecordV1(
            inputs.session, record, legacy_output_contract()
        )
        assert native_provider_artifact_json(wrapper) == record.to_json()
    other_start = replace(
        inputs.session,
        started_at_utc_ns=inputs.session.started_at_utc_ns + 1,
        session_id="",
    )
    assert other_start.session_id != inputs.session.session_id
    assert legacy_capture_binding(other_start) == legacy_capture_binding(
        inputs.session
    )
    other_config = replace(
        inputs.session, adapter_config_sha256="d" * 64, session_id=""
    )
    assert legacy_capture_binding(other_config) != legacy_capture_binding(
        inputs.session
    )


def test_unknown_native_object_never_executes_serializer():
    class Unknown:
        def to_json(self):
            pytest.fail("unknown native serializer must not execute")

    for function in (resolve_provider_subject, native_provider_artifact_json):
        with pytest.raises(ValueError, match="unsupported"):
            function(Unknown())


def test_sdk_output_and_invocation_roundtrip_need_no_site_packages():
    source_root = str(Path(__file__).resolve().parents[2] / "src")
    script = """
import sys
sys.path.insert(0, sys.argv[1])
from histdatacom.broker_plugin_policy.bindings import (
    BrokerProviderOutputContractV1, BrokerSDKInvocationV1,
    resolve_provider_subject,
)
contract = BrokerProviderOutputContractV1('sdk-v1', ('quote',))
assert BrokerProviderOutputContractV1.from_json(contract.to_json()) == contract
try:
    BrokerProviderOutputContractV1('unknown-v1', ('quote',))
except ValueError:
    pass
else:
    raise AssertionError('unknown family was admitted')
text = sys.stdin.read()
invocation = BrokerSDKInvocationV1.from_json(text)
assert invocation.to_json() == text
assert resolve_provider_subject(invocation).bindings
assert not {'certifi', 'numpy', 'polars', 'histdatacom.broker_capture'} & set(sys.modules)
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-S", "-c", script, source_root],
        input=sdk_policy_invocation().to_json(),
        text=True,
        capture_output=True,
        timeout=15,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr


def test_fingerprint_expected_shape_binds_config_and_all_unique_providers():
    from histdatacom.broker_capture.fingerprint_contracts import (
        BrokerDeliveryFitConfigV1,
    )

    first = legacy_policy_inputs().session
    second = replace(
        first,
        adapter_config_sha256="b" * 64,
        account_id_sha256="c" * 64,
        session_id="",
    )
    contract = replace(
        legacy_output_contract(), allow_private_account_metadata=True
    )
    requests = tuple(
        BrokerLegacyCaptureV1(session, contract) for session in (first, second)
    )
    config = BrokerDeliveryFitConfigV1()
    shape = BrokerFingerprintFitV1(requests, config)
    subject = resolve_provider_subject(shape)
    assert len(subject.bindings) == 2
    assert set(subject.data_classes) == {
        DataClass.FINGERPRINTS,
        DataClass.CONTENT_HASHES,
        DataClass.HEALTH,
        DataClass.PRIVATE_ACCOUNT,
    }
    assert DataClass.NORMALIZED_QUOTES not in subject.data_classes
    with pytest.raises(ValueError, match="no storable native artifact"):
        native_provider_artifact_json(shape)
    changed = replace(
        shape, fit_config=replace(config, min_cell_support=5, config_id="")
    )
    assert resolve_provider_subject(changed).native_ref != subject.native_ref
    assert resolve_provider_subject(changed).bindings == subject.bindings


@pytest.mark.parametrize(
    "mutation",
    [
        "empty",
        "duplicate",
        "same_session_new_shape",
        "list",
        "too_many",
        "unknown_config",
    ],
)
def test_fingerprint_shape_refuses_ambiguous_or_unbounded_requests(mutation):
    from histdatacom.broker_capture.fingerprint_contracts import (
        BrokerDeliveryFitConfigV1,
    )

    request = BrokerLegacyCaptureV1(
        legacy_policy_inputs().session, legacy_output_contract()
    )
    shape = BrokerFingerprintFitV1((request,), BrokerDeliveryFitConfigV1())
    if mutation == "empty":
        shape = replace(shape, capture_requests=())
    elif mutation == "duplicate":
        shape = replace(shape, capture_requests=(request, request))
    elif mutation == "same_session_new_shape":
        shape = replace(
            shape,
            capture_requests=(
                request,
                replace(
                    request,
                    output_contract=replace(
                        request.output_contract, allow_opaque_metadata=True
                    ),
                ),
            ),
        )
    elif mutation == "list":
        shape = replace(shape, capture_requests=[request])
    elif mutation == "too_many":
        shape = replace(shape, capture_requests=(request,) * 65)
    else:
        shape = replace(shape, fit_config=object())
    with pytest.raises(ValueError):
        resolve_provider_subject(shape)


def test_legacy_free_text_manifest_is_raw_not_bounded_health():
    from histdatacom.broker_capture.contracts import (
        BrokerCaptureSessionManifestV1,
        BrokerCaptureSessionState,
    )

    inputs = legacy_policy_inputs()
    manifest = BrokerCaptureSessionManifestV1(
        inputs.session,
        inputs.storage_policy,
        BrokerCaptureSessionState.OPEN,
        (),
        0,
        {},
        None,
        None,
        limitations=("opaque provider response cannot be normalized health",),
    )
    plain = BrokerLegacyRecordV1(inputs.session, manifest)
    assert DataClass.RAW_PAYLOAD in resolve_provider_subject(plain).data_classes
    with pytest.raises(ValueError, match="outside the reviewed output"):
        resolve_provider_subject(
            replace(plain, output_contract=legacy_output_contract())
        )
    permitted_shape = replace(
        legacy_output_contract(), allow_opaque_metadata=True
    )
    assert (
        native_provider_artifact_json(
            replace(plain, output_contract=permitted_shape)
        )
        == manifest.to_json()
    )


@pytest.mark.parametrize("mutation", ["text", "cyclic", "wide", "expanded"])
def test_native_resource_preflight_runs_before_legacy_serializer(
    monkeypatch, mutation
):
    from histdatacom.broker_capture.contracts import BrokerCaptureSessionV1

    session = legacy_policy_inputs().session
    if mutation == "text":
        value = {"note": "x" * (8 * 1024 * 1024 + 1)}
    elif mutation == "cyclic":
        value = {}
        value["cycle"] = value
    elif mutation == "wide":
        value = {"items": [None] * 4097}
    else:
        value = {"items": [[None] * 4096] * 64}
    # Valid native instances are frozen; simulated hostile deep mutation must
    # be refused before calling their older, allocation-heavy serializer.
    object.__setattr__(session, "public_metadata", value)
    monkeypatch.setattr(
        BrokerCaptureSessionV1,
        "to_json",
        lambda _: pytest.fail("preflight must precede serializer"),
    )
    with pytest.raises(ValueError, match="bound"):
        resolve_provider_subject(BrokerLegacyRecordV1(session, session))
