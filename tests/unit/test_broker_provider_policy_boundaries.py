"""Independent declared-rights boundaries; generated inputs, never real terms."""

from dataclasses import replace
import hashlib
import json
import traceback

import pytest

from histdatacom.broker_plugin_policy.contracts import (
    BrokerPolicyBindingV1,
    BrokerPolicyContextV1,
    BrokerPolicyDataClass as DataClass,
    BrokerPolicyDecisionV1,
    BrokerPolicyEvidenceKind as EvidenceKind,
    BrokerPolicyOperation as Operation,
    BrokerPolicyReferenceV1,
    BrokerPolicyRequestV1,
    BrokerPolicyRevocationV1,
    BrokerPolicyStatus as Status,
    BrokerPolicySubjectV1,
)
from histdatacom.broker_plugin_policy.decisions import decide_provider_operation
from histdatacom.broker_plugin_policy.scope import (
    BrokerPolicyError,
    provider_policy_scope,
    require_provider_operation,
)
from tests.fixtures.broker_provider_policy import (
    POLICY_NOW,
    RAW_CANARY,
    CountingLegacyAdapter,
    MutablePolicySource,
    legacy_output_contract,
    legacy_policy_inputs,
    policy_context,
    sdk_policy_invocation,
)

from histdatacom.broker_plugin_policy.bindings import (
    BrokerLegacyCaptureV1,
    BrokerLegacyRecordV1,
    BrokerProviderConfigurationV1,
    legacy_capture_binding,
    resolve_provider_subject,
)


def _binding(name="one"):
    return BrokerPolicyBindingV1(
        "generated-provider-" + name,
        "generated-contract-test-binding",
        json.dumps({"fixture": name}, sort_keys=True, separators=(",", ":")),
    )


@pytest.fixture
def fixed_policy_clock(monkeypatch):
    import histdatacom.broker_plugin_policy.scope as scope

    clock = [POLICY_NOW]
    monkeypatch.setattr(scope, "_now_ns", lambda: clock[0])
    return clock


@pytest.fixture(scope="module")
def native_legacy():
    return legacy_policy_inputs(), legacy_output_contract()


def test_native_legacy_binding_and_reference_preserve_exact_bytes(
    native_legacy,
):
    inputs, output = native_legacy
    before = inputs.session.to_json(), tuple(e.to_json() for e in inputs.events)
    subject = resolve_provider_subject(
        BrokerLegacyCaptureV1(inputs.session, output)
    )
    assert subject.bindings == (legacy_capture_binding(inputs.session),)
    assert subject.evidence_kind is EvidenceKind.DECLARED
    assert set(subject.data_classes) == {
        DataClass.CONTENT_HASHES,
        DataClass.HEALTH,
        DataClass.NORMALIZED_QUOTES,
    }
    for event in inputs.events:
        record_subject = resolve_provider_subject(
            BrokerLegacyRecordV1(inputs.session, event, output)
        )
        assert record_subject.bindings == subject.bindings
        assert DataClass.RAW_PAYLOAD not in record_subject.data_classes
        assert DataClass.PRIVATE_ACCOUNT not in record_subject.data_classes
    assert before == (
        inputs.session.to_json(),
        tuple(e.to_json() for e in inputs.events),
    )


@pytest.mark.parametrize("wrapper", ("capture", "event"))
def test_native_subject_requires_live_scope_not_standalone_allowance(
    native_legacy, wrapper
):
    inputs, output = native_legacy
    native = (
        BrokerLegacyCaptureV1(inputs.session, output)
        if wrapper == "capture"
        else BrokerLegacyRecordV1(inputs.session, inputs.events[3], output)
    )
    subject = resolve_provider_subject(native)
    assert decide_provider_operation(
        policy_context(subject.bindings[0]), _request(subject)
    ).allowed
    with pytest.raises(
        BrokerPolicyError, match="current_process_policy_scope_required"
    ):
        require_provider_operation(native, Operation.CAPTURE)


@pytest.mark.parametrize("changed", ("session", "configuration", "provider"))
def test_native_event_cannot_borrow_other_session(native_legacy, changed):
    inputs, output = native_legacy
    changes = {
        "session": {"started_at_utc_ns": inputs.session.started_at_utc_ns + 1},
        "configuration": {"adapter_config_sha256": "c" * 64},
        "provider": {"adapter_id": "generated-other-provider"},
    }[changed]
    other = replace(inputs.session, session_id="", **changes)
    with pytest.raises(ValueError, match="another session"):
        resolve_provider_subject(
            BrokerLegacyRecordV1(other, inputs.events[3], output)
        )


@pytest.mark.parametrize(
    "field,value",
    (
        ("adapter_config_sha256", "c" * 64),
        ("server_id", "other-generated-server"),
        ("account_id_sha256", "d" * 64),
        ("adapter_id", "other-generated-adapter"),
    ),
)
def test_changed_native_configuration_cannot_borrow_policy(
    native_legacy, fixed_policy_clock, field, value
):
    inputs, output = native_legacy
    other = replace(inputs.session, session_id="", **{field: value})
    # Allowing the *shape* does not add this provider/configuration to the ledger.
    output = replace(output, allow_private_account_metadata=True)
    native = BrokerLegacyCaptureV1(other, output)
    context = policy_context(legacy_capture_binding(inputs.session))
    with provider_policy_scope(MutablePolicySource(context)):
        with pytest.raises(BrokerPolicyError) as error:
            require_provider_operation(native, Operation.CAPTURE)
    assert error.value.decision.status is Status.UNKNOWN


@pytest.mark.parametrize("surface", ("session", "message"))
def test_opaque_native_metadata_requires_explicit_output_and_raw_allowance(
    native_legacy, fixed_policy_clock, surface
):
    inputs, output = native_legacy
    if surface == "session":
        session = replace(
            inputs.session,
            public_metadata={"generated": RAW_CANARY},
            session_id="",
        )
        native = BrokerLegacyCaptureV1(session, output)
    else:
        session = inputs.session
        message = replace(
            inputs.messages[3],
            public_metadata={"generated": RAW_CANARY},
            message_id="",
        )
        native = BrokerLegacyRecordV1(session, message, output)
    with pytest.raises(ValueError, match="outside the reviewed output"):
        resolve_provider_subject(native)
    permitted_shape = replace(
        native, output_contract=replace(output, allow_opaque_metadata=True)
    )
    assert (
        DataClass.RAW_PAYLOAD
        in resolve_provider_subject(permitted_shape).data_classes
    )
    context = policy_context(
        legacy_capture_binding(session),
        changes=((Operation.CAPTURE, DataClass.RAW_PAYLOAD, Status.DENIED),),
    )
    with provider_policy_scope(MutablePolicySource(context)):
        with pytest.raises(BrokerPolicyError) as error:
            require_provider_operation(permitted_shape, Operation.CAPTURE)
    assert error.value.decision.status is Status.DENIED


def test_raw_hash_is_not_raw_payload_and_is_still_output_gated(
    fixed_policy_clock,
):
    inputs, output = (
        legacy_policy_inputs(raw_hash=True),
        legacy_output_contract(),
    )
    native = BrokerLegacyRecordV1(inputs.session, inputs.events[3], output)
    with pytest.raises(ValueError, match="disabled raw hash"):
        resolve_provider_subject(native)
    native = replace(
        native, output_contract=replace(output, allow_raw_hashes=True)
    )
    subject = resolve_provider_subject(native)
    assert set(subject.data_classes) == {
        DataClass.CONTENT_HASHES,
        DataClass.HEALTH,
        DataClass.NORMALIZED_QUOTES,
    }
    context = policy_context(
        subject.bindings[0],
        changes=((Operation.CAPTURE, DataClass.RAW_PAYLOAD, Status.DENIED),),
    )
    before = inputs.events[3].to_json()
    with provider_policy_scope(MutablePolicySource(context)):
        decision = require_provider_operation(native, Operation.CAPTURE)
    assert decision.allowed
    assert RAW_CANARY not in before and RAW_CANARY not in decision.to_json()
    assert hashlib.sha256(RAW_CANARY.encode("ascii")).hexdigest() in before
    assert inputs.events[3].to_json() == before


@pytest.mark.parametrize("field", ("account_id_sha256", "host_id_sha256"))
def test_private_native_identifier_cannot_be_downgraded_to_content_hash(
    native_legacy, fixed_policy_clock, field
):
    inputs, output = native_legacy
    session = replace(inputs.session, session_id="", **{field: "e" * 64})
    native = BrokerLegacyCaptureV1(session, output)
    with pytest.raises(ValueError, match="outside the reviewed output"):
        resolve_provider_subject(native)
    native = replace(
        native,
        output_contract=replace(output, allow_private_account_metadata=True),
    )
    subject = resolve_provider_subject(native)
    assert DataClass.PRIVATE_ACCOUNT in subject.data_classes
    context = policy_context(
        subject.bindings[0],
        changes=(
            (Operation.CAPTURE, DataClass.PRIVATE_ACCOUNT, Status.DENIED),
        ),
    )
    with provider_policy_scope(MutablePolicySource(context)):
        with pytest.raises(BrokerPolicyError) as error:
            require_provider_operation(native, Operation.CAPTURE)
    assert error.value.decision.status is Status.DENIED


def test_undeclared_native_quote_kind_fails_before_policy_decision(
    native_legacy,
):
    inputs, output = native_legacy
    output = replace(output, event_kinds=("process_start",))
    with pytest.raises(ValueError, match="undeclared event kind"):
        resolve_provider_subject(
            BrokerLegacyRecordV1(inputs.session, inputs.events[3], output)
        )


@pytest.mark.parametrize("surface", ("session", "message", "event"))
def test_mutated_native_values_with_stale_identity_refuse(surface):
    inputs, output = legacy_policy_inputs(), legacy_output_contract()
    if surface == "session":
        inputs.session.public_metadata["generated"] = "mutated"
    elif surface == "message":
        inputs.events[3].message.public_metadata["generated"] = "mutated"
    else:
        object.__setattr__(
            inputs.events[3],
            "receive_time_utc_ns",
            inputs.events[3].receive_time_utc_ns + 1,
        )
    with pytest.raises(ValueError, match="identity"):
        resolve_provider_subject(
            BrokerLegacyRecordV1(inputs.session, inputs.events[3], output)
        )


def test_duck_typed_native_descriptor_never_executes_its_serialization(
    native_legacy,
):
    calls = []

    class Spoof:
        def to_json(self):
            calls.append("to_json")
            raise AssertionError("not a native value")

    inputs, output = native_legacy
    with pytest.raises(ValueError, match="unsupported native"):
        resolve_provider_subject(
            BrokerLegacyRecordV1(inputs.session, Spoof(), output)
        )
    with pytest.raises(ValueError, match="unsupported or incomplete"):
        resolve_provider_subject(Spoof())
    assert calls == []


def test_configuration_profile_verifies_exact_public_fields_without_secret_hashing():
    from histdatacom.broker_plugins import (
        BrokerConfigurationFieldV1,
        BrokerConfigurationSchemaV1,
        BrokerConfigurationType,
    )

    schema = BrokerConfigurationSchemaV1(
        (
            BrokerConfigurationFieldV1(
                "mode", BrokerConfigurationType.STRING, "Generated mode"
            ),
            BrokerConfigurationFieldV1(
                "credential",
                BrokerConfigurationType.STRING,
                "Generated credential",
                secret=True,
            ),
        )
    )
    profile = BrokerProviderConfigurationV1(
        "generated-sdk-provider",
        "generated-account-profile",
        schema.to_json(),
        '{"mode":"finite"}',
        ("credential",),
    )
    before = profile.to_json()
    for secret in (RAW_CANARY, RAW_CANARY + "-rotated"):
        profile.verify_configuration({"mode": "finite", "credential": secret})
        assert secret not in profile.to_json()
        assert (
            hashlib.sha256(secret.encode()).hexdigest() not in profile.to_json()
        )
        assert profile.to_json() == before
    for configuration in (
        {"mode": "different", "credential": RAW_CANARY},
        {"mode": "finite"},
        {"mode": "finite", "credential": RAW_CANARY, "extra": "generated"},
    ):
        with pytest.raises(ValueError):
            profile.verify_configuration(configuration)
    # Both early credential-key screening and exact schema classification must
    # refuse this; their evaluation order is not part of the public contract.
    with pytest.raises(ValueError):
        replace(
            profile,
            public_configuration_json='{"credential":"generated","mode":"finite"}',
            private_field_names=(),
        )


def _subject(bindings, classes=(DataClass.NORMALIZED_QUOTES,)):
    payload = b"Generated descriptor; not a host-native admission proof."
    return BrokerPolicySubjectV1(
        BrokerPolicyReferenceV1(
            "generated-contract-test-subject",
            "generated-native-descriptor",
            hashlib.sha256(payload).hexdigest(),
            len(payload),
        ),
        tuple(sorted(bindings, key=lambda item: item.artifact_id)),
        tuple(sorted(classes)),
    )


def _request(subject, operation=Operation.CAPTURE, at=POLICY_NOW, **kwargs):
    if operation in (Operation.REDISTRIBUTE, Operation.PUBLISH):
        kwargs.setdefault("recipient_scope", "generated-local-test-recipient")
    return BrokerPolicyRequestV1(subject, operation, at, **kwargs)


@pytest.fixture(scope="module")
def native_sdk():
    from histdatacom.broker_plugins import (
        BrokerEventKind,
        BrokerEventV1,
        BrokerPluginMetadataV1,
        BrokerQuoteV1,
        BrokerReceiveTimeV1,
        BrokerSessionV1,
    )

    invocation = sdk_policy_invocation()
    registration = invocation.plan.candidate.registration
    metadata = BrokerPluginMetadataV1(
        registration.plugin_id,
        registration.plugin_version,
        registration.display_name,
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
    return invocation, metadata, session, event


def test_sdk_canonical_session_and_event_are_bound_to_actual_metadata(
    native_sdk, fixed_policy_clock
):
    from histdatacom.broker_plugin_policy.bindings import (
        BrokerSDKRecordV1,
        sdk_invocation_binding,
    )

    invocation, metadata, session, event = native_sdk
    before = metadata.to_json(), session.to_json(), event.to_json()
    source = MutablePolicySource(
        policy_context(sdk_invocation_binding(invocation))
    )
    with provider_policy_scope(source):
        for record in (metadata, session, event):
            result = require_provider_operation(
                BrokerSDKRecordV1(
                    invocation, record, session=session, metadata=metadata
                ),
                Operation.CAPTURE,
            )
            assert result.allowed
            assert result.request.subject.bindings == (
                sdk_invocation_binding(invocation),
            )
    assert before == (metadata.to_json(), session.to_json(), event.to_json())


@pytest.mark.parametrize(
    "route", ("factory", "installed", "lifecycle", "trusted", "kernel")
)
@pytest.mark.parametrize("status", (None, Status.UNKNOWN, Status.DENIED))
def test_sdk_execution_routes_refuse_without_current_rights_before_effects(
    tmp_path, native_sdk, fixed_policy_clock, route, status
):
    from contextlib import nullcontext
    from histdatacom.broker_plugin_capabilities import (
        invoke_authorized_broker_factory,
        invoke_authorized_installed_broker_plugin,
    )
    from histdatacom.broker_plugin_lifecycle import run_broker_plugin_lifecycle
    from histdatacom.broker_plugin_policy.bindings import sdk_invocation_binding
    from histdatacom.broker_plugin_registry import BrokerPluginInventoryV1
    from histdatacom.broker_plugin_security import (
        BrokerSecurityMode,
        BrokerSecurityPolicyV1,
        BrokerTrustTier,
        run_secure_broker_plugin,
        run_trusted_broker_plugin,
    )

    request, _, _, _ = native_sdk
    plan = request.plan
    inventory = BrokerPluginInventoryV1((plan.candidate,))
    calls = []

    def authorize(_):
        calls.append("authorize")
        return True

    def factory():
        calls.append("factory")
        raise AssertionError("no provider callback may run")

    destination = tmp_path / "must-not-exist"
    manager = (
        nullcontext()
        if status is None
        else provider_policy_scope(
            MutablePolicySource(
                policy_context(sdk_invocation_binding(request), status=status)
            )
        )
    )
    with manager, pytest.raises(BrokerPolicyError):
        if route == "factory":
            invoke_authorized_broker_factory(
                inventory,
                plan,
                authorize=authorize,
                factory=factory,
                provider_request=request,
            )
        elif route == "installed":
            invoke_authorized_installed_broker_plugin(
                inventory, plan, authorize=authorize, provider_request=request
            )
        elif route == "lifecycle":
            run_broker_plugin_lifecycle(
                inventory,
                plan,
                {},
                ("EURUSD",),
                destination,
                authorize=authorize,
                provider_request=request,
            )
        else:
            policy = BrokerSecurityPolicyV1(
                plan.candidate.artifact_id,
                BrokerTrustTier.REVIEWED,
                (
                    BrokerSecurityMode.TRUSTED_IN_PROCESS
                    if route == "trusted"
                    else BrokerSecurityMode.KERNEL_ISOLATED
                ),
            )
            if route == "trusted":
                run_trusted_broker_plugin(
                    inventory,
                    plan,
                    policy,
                    {},
                    ("EURUSD",),
                    authorize=authorize,
                    provider_request=request,
                )
            else:
                run_secure_broker_plugin(
                    inventory,
                    plan,
                    policy,
                    {},
                    ("EURUSD",),
                    destination,
                    authorize=authorize,
                    provider_request=request,
                )
    assert calls == []
    assert tuple(tmp_path.iterdir()) == ()


@pytest.mark.parametrize("record_kind", ("session", "event"))
@pytest.mark.parametrize("parent_kind", ("missing", "foreign", "mismatched"))
def test_sdk_foreign_or_absent_metadata_cannot_relabel_a_canonical_session(
    native_sdk, record_kind, parent_kind
):
    from histdatacom.broker_plugin_policy.bindings import BrokerSDKRecordV1

    invocation, metadata, session, event = native_sdk
    foreign_metadata = replace(
        metadata, plugin_id="org.example.foreign-provider"
    )
    foreign_session = replace(session, metadata_id=foreign_metadata.artifact_id)
    if parent_kind == "missing":
        parent = None
    else:
        session = foreign_session
        parent = foreign_metadata if parent_kind == "foreign" else metadata
    event = replace(event, session_id=session.artifact_id)
    record = session if record_kind == "session" else event
    # Each object is separately canonical; it is the cross-parent claim that is false.
    assert type(record).from_json(record.to_json()) == record
    with pytest.raises(ValueError):
        resolve_provider_subject(
            BrokerSDKRecordV1(
                invocation, record, session=session, metadata=parent
            )
        )


def test_sdk_schema_mismatch_refuses_before_public_configuration_use(
    native_sdk,
):
    from histdatacom.broker_plugin_policy.bindings import BrokerSDKRecordV1
    from histdatacom.broker_plugins import (
        BrokerConfigurationFieldV1,
        BrokerConfigurationSchemaV1,
        BrokerConfigurationType,
    )

    invocation, _, _, _ = native_sdk
    other_schema = BrokerConfigurationSchemaV1(
        (
            BrokerConfigurationFieldV1(
                "new_field", BrokerConfigurationType.STRING, "Generated field"
            ),
        )
    )
    with pytest.raises(ValueError, match="differs from reviewed"):
        resolve_provider_subject(BrokerSDKRecordV1(invocation, other_schema))


@pytest.mark.parametrize("record_kind", ("session", "event"))
@pytest.mark.parametrize("parent_kind", ("opaque", "private-account"))
def test_sdk_parent_metadata_classes_cannot_disappear_from_child_admission(
    native_sdk, fixed_policy_clock, record_kind, parent_kind
):
    from histdatacom.broker_plugin_capabilities import PUBLIC_CONTEXT_NAMESPACE
    from histdatacom.broker_plugin_policy.bindings import (
        BrokerSDKRecordV1,
        sdk_invocation_binding,
    )
    from histdatacom.broker_plugins import BrokerExtensionV1

    _, metadata, session, event = native_sdk
    invocation = sdk_policy_invocation(
        extra_capabilities=("metadata.account-class.v1",)
    )
    output = replace(
        invocation.output_contract,
        allow_opaque_metadata=True,
        allow_private_account_metadata=True,
    )
    invocation = replace(invocation, output_contract=output)
    if parent_kind == "opaque":
        extension = BrokerExtensionV1(
            "org.example.generated-metadata",
            '{"note":"opaque generated field"}',
        )
        data_class = DataClass.RAW_PAYLOAD
    else:
        extension = BrokerExtensionV1(
            PUBLIC_CONTEXT_NAMESPACE, '{"account_class":"demo"}'
        )
        data_class = DataClass.PRIVATE_ACCOUNT
    metadata = replace(metadata, extensions=(extension,))
    session = replace(session, metadata_id=metadata.artifact_id)
    event = replace(event, session_id=session.artifact_id)
    record = session if record_kind == "session" else event
    native = BrokerSDKRecordV1(
        invocation, record, session=session, metadata=metadata
    )
    subject = resolve_provider_subject(native)
    assert data_class in subject.data_classes
    context = policy_context(
        sdk_invocation_binding(invocation),
        changes=((Operation.CAPTURE, data_class, Status.DENIED),),
    )
    with provider_policy_scope(MutablePolicySource(context)):
        with pytest.raises(BrokerPolicyError) as error:
            require_provider_operation(native, Operation.CAPTURE)
    assert error.value.decision.status is Status.DENIED


def _combined(*contexts):
    def records(name):
        return tuple(
            sorted(
                (
                    item
                    for context in contexts
                    for item in getattr(context, name)
                ),
                key=lambda item: item.artifact_id,
            )
        )

    return BrokerPolicyContextV1(
        records("policies"),
        records("evidence"),
        records("acknowledgements"),
        records("revocations"),
        tuple(
            sorted(
                identity for c in contexts for identity in c.selected_policy_ids
            )
        ),
        contexts[0].execution,
    )


def _reseal(envelope):
    def encode(value):
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )

    subject = {
        "schema_version": envelope["schema_version"],
        "payload": envelope["payload"],
    }
    prefix = envelope["artifact_id"].rsplit(":", 1)[0]
    envelope["artifact_id"] = (
        prefix
        + ":"
        + hashlib.sha256(encode(subject).encode("ascii")).hexdigest()
    )
    return encode(envelope)


def _legacy_clock(inputs):
    from histdatacom.broker_capture import SequenceBrokerCaptureClockV1

    return SequenceBrokerCaptureClockV1(
        tuple(
            (event.receive_time_utc_ns, event.receive_time_monotonic_ns)
            for event in inputs.events
        )
    )


@pytest.mark.parametrize("status", (None, Status.UNKNOWN, Status.DENIED))
def test_live_constructor_refuses_before_adapter_properties(
    native_legacy, fixed_policy_clock, status
):
    from histdatacom.broker_capture import LiveBrokerCaptureSourceV1
    from contextlib import nullcontext

    inputs, output = native_legacy
    adapter = CountingLegacyAdapter(inputs)
    request = BrokerLegacyCaptureV1(inputs.session, output)
    manager = (
        nullcontext()
        if status is None
        else provider_policy_scope(
            MutablePolicySource(
                policy_context(
                    legacy_capture_binding(inputs.session), status=status
                )
            )
        )
    )
    with manager, pytest.raises(BrokerPolicyError):
        LiveBrokerCaptureSourceV1(
            inputs.session,
            adapter,
            _legacy_clock(inputs),
            provider_request=request,
        )
    assert adapter.calls == []


@pytest.mark.parametrize("status", (None, Status.UNKNOWN, Status.DENIED))
def test_writer_refuses_before_creating_any_output(
    tmp_path, native_legacy, fixed_policy_clock, status
):
    from histdatacom.broker_capture import AppendOnlyBrokerCaptureWriterV1
    from contextlib import nullcontext

    inputs, output = native_legacy
    request = BrokerLegacyCaptureV1(inputs.session, output)
    destination = tmp_path / "must-not-exist"
    manager = (
        nullcontext()
        if status is None
        else provider_policy_scope(
            MutablePolicySource(
                policy_context(
                    legacy_capture_binding(inputs.session), status=status
                )
            )
        )
    )
    with manager, pytest.raises(BrokerPolicyError):
        AppendOnlyBrokerCaptureWriterV1(
            destination,
            session=inputs.session,
            storage_policy=inputs.storage_policy,
            provider_request=request,
        )
    assert not destination.exists()
    assert tuple(tmp_path.iterdir()) == ()


def test_writer_cannot_promise_finite_retention_it_does_not_enforce(
    tmp_path, native_legacy, fixed_policy_clock
):
    from histdatacom.broker_capture import AppendOnlyBrokerCaptureWriterV1

    inputs, output = native_legacy
    context = policy_context(
        legacy_capture_binding(inputs.session), maximum_retention_ns=100
    )
    with provider_policy_scope(MutablePolicySource(context)):
        with pytest.raises(BrokerPolicyError):
            AppendOnlyBrokerCaptureWriterV1(
                tmp_path / "must-not-exist",
                session=inputs.session,
                storage_policy=inputs.storage_policy,
                provider_request=BrokerLegacyCaptureV1(inputs.session, output),
            )
    assert tuple(tmp_path.iterdir()) == ()


def test_native_decision_detaches_identity_before_caller_mutation(
    fixed_policy_clock,
):
    inputs, output = legacy_policy_inputs(), legacy_output_contract()
    native = BrokerLegacyCaptureV1(inputs.session, output)
    source = MutablePolicySource(
        policy_context(legacy_capture_binding(inputs.session))
    )
    with provider_policy_scope(source):
        decision = require_provider_operation(native, Operation.CAPTURE)
        encoded = decision.to_json()
        inputs.session.public_metadata["generated"] = "later caller mutation"
        with pytest.raises(ValueError, match="identity"):
            require_provider_operation(native, Operation.CAPTURE)
    assert decision.to_json() == encoded
    assert BrokerPolicyDecisionV1.from_json(encoded) == decision
    assert "later caller mutation" not in encoded


def test_native_guard_rechecks_expiry_after_native_resolution(
    native_legacy, monkeypatch
):
    import histdatacom.broker_plugin_policy.scope as scope

    inputs, output = native_legacy
    expiry = POLICY_NOW + 1
    ticks = iter((POLICY_NOW, expiry))
    monkeypatch.setattr(scope, "_now_ns", lambda: next(ticks))
    context = policy_context(
        legacy_capture_binding(inputs.session), expires_at_ns=expiry
    )
    with provider_policy_scope(MutablePolicySource(context)):
        with pytest.raises(BrokerPolicyError) as error:
            require_provider_operation(
                BrokerLegacyCaptureV1(inputs.session, output), Operation.CAPTURE
            )
    assert error.value.decision.request.decision_at_ns == expiry
    assert not error.value.decision.allowed


def test_raw_denied_native_capture_persists_exact_quotes_without_raw_payload(
    tmp_path, native_legacy, fixed_policy_clock
):
    from histdatacom.broker_capture import (
        AppendOnlyBrokerCaptureWriterV1,
        BrokerCaptureReplaySourceV1,
        LiveBrokerCaptureSourceV1,
    )

    inputs, output = native_legacy
    request = BrokerLegacyCaptureV1(inputs.session, output)
    context = policy_context(
        legacy_capture_binding(inputs.session),
        changes=tuple(
            (operation, DataClass.RAW_PAYLOAD, Status.DENIED)
            for operation in Operation
        ),
    )
    adapter = CountingLegacyAdapter(inputs)
    with provider_policy_scope(MutablePolicySource(context)):
        source = LiveBrokerCaptureSourceV1(
            inputs.session,
            adapter,
            _legacy_clock(inputs),
            provider_request=request,
        )
        writer = AppendOnlyBrokerCaptureWriterV1(
            tmp_path / "capture",
            session=inputs.session,
            storage_policy=inputs.storage_policy,
            provider_request=request,
        )
        observed = tuple(source.iter_events())
        for event in observed:
            writer.append(event)
        manifest = writer.close()
    assert tuple(e.to_json() for e in observed) == tuple(
        e.to_json() for e in inputs.events
    )
    # Materializing retained provider events requires its own current allowance.
    with provider_policy_scope(MutablePolicySource(context)):
        replayed = tuple(
            BrokerCaptureReplaySourceV1(
                tmp_path / "capture", manifest, provider_request=request
            ).iter_events()
        )
    assert replayed == inputs.events
    assert adapter.calls.count("next") == len(inputs.events) + 1
    assert adapter.calls[-1] == "close"
    for path in (tmp_path / "capture").rglob("*"):
        if path.is_file():
            data = path.read_bytes()
            assert RAW_CANARY.encode() not in data
            assert (
                hashlib.sha256(RAW_CANARY.encode()).hexdigest().encode()
                not in data
            )


def test_raw_denied_capture_remains_fit_writable_and_identity_readable(
    tmp_path, native_legacy, fixed_policy_clock
):
    from histdatacom.broker_capture import (
        AppendOnlyBrokerCaptureWriterV1,
        fit_broker_delivery_fingerprint,
        load_broker_delivery_fingerprint,
        write_broker_delivery_fingerprint,
    )
    from histdatacom.broker_plugin_policy.native_inputs import (
        provider_native_inputs,
    )
    from histdatacom.broker_plugin_policy.storage import (
        BrokerPolicyArtifactReceiptV1,
        read_broker_policy_receipt,
        verify_broker_policy_receipt,
    )

    inputs, output = native_legacy
    request = BrokerLegacyCaptureV1(inputs.session, output)
    context = policy_context(
        legacy_capture_binding(inputs.session),
        changes=tuple(
            (operation, DataClass.RAW_PAYLOAD, Status.DENIED)
            for operation in Operation
        ),
    )
    capture_root = tmp_path / "capture"
    with (
        provider_policy_scope(MutablePolicySource(context)),
        provider_native_inputs(request),
    ):
        writer = AppendOnlyBrokerCaptureWriterV1(
            capture_root,
            session=inputs.session,
            storage_policy=inputs.storage_policy,
            provider_request=request,
        )
        for event in inputs.events:
            writer.append(event)
        manifest = writer.close()
        fingerprint = fit_broker_delivery_fingerprint(
            capture_root, (manifest,), provider_requests=(request,)
        )
        from_registry = fit_broker_delivery_fingerprint(
            capture_root, (manifest,)
        )
        assert fingerprint.to_json() == from_registry.to_json()
        target = tmp_path / "derived" / "fingerprint.json"
        artifact = write_broker_delivery_fingerprint(target, fingerprint)
    native_bytes = (fingerprint.to_json() + "\n").encode("utf-8")
    assert target.read_bytes() == native_bytes
    assert artifact.sha256 == hashlib.sha256(native_bytes).hexdigest()
    assert artifact.size_bytes == len(native_bytes)
    assert load_broker_delivery_fingerprint(target) == fingerprint
    sidecar = target.with_name(target.name + ".provider-policy.json")
    assert sidecar.is_file()
    receipt = read_broker_policy_receipt(sidecar)
    verify_broker_policy_receipt(receipt, fingerprint, target)
    assert receipt.native_file.sha256 == artifact.sha256
    assert receipt.native_file.byte_length == artifact.size_bytes
    # A correctly re-sealed, internally valid declaration cannot omit the
    # fingerprint class when verified against the actual unchanged native file.
    reduced = replace(
        receipt.operation_admission.request.subject,
        data_classes=tuple(
            item
            for item in receipt.operation_admission.request.subject.data_classes
            if item is not DataClass.FINGERPRINTS
        ),
    )
    forged = replace(
        receipt,
        operation_admission=decide_provider_operation(
            receipt.operation_admission.context,
            replace(receipt.operation_admission.request, subject=reduced),
        ),
        retention_admission=decide_provider_operation(
            receipt.retention_admission.context,
            replace(receipt.retention_admission.request, subject=reduced),
        ),
    )
    forged = BrokerPolicyArtifactReceiptV1.from_json(forged.to_json())
    with pytest.raises(ValueError, match="classification"):
        verify_broker_policy_receipt(forged, fingerprint, target)
    assert RAW_CANARY.encode() not in native_bytes
    assert all(
        item.event_count == len(inputs.events)
        for item in fingerprint.capture_evidence
    )
    assert (
        DataClass.FINGERPRINTS
        in resolve_provider_subject(fingerprint).data_classes
    )
    assert (
        DataClass.RAW_PAYLOAD
        not in resolve_provider_subject(fingerprint).data_classes
    )


def test_revocation_stops_live_pulls_but_closes_bound_iterator(
    native_legacy, fixed_policy_clock
):
    from histdatacom.broker_capture import LiveBrokerCaptureSourceV1

    inputs, output = native_legacy
    adapter = CountingLegacyAdapter(inputs)
    context = policy_context(legacy_capture_binding(inputs.session))
    source = MutablePolicySource(context)
    with provider_policy_scope(source):
        live = LiveBrokerCaptureSourceV1(
            inputs.session,
            adapter,
            _legacy_clock(inputs),
            provider_request=BrokerLegacyCaptureV1(inputs.session, output),
        )
        events = iter(live.iter_events())
        assert next(events) == inputs.events[0]
        pulled = adapter.calls.count("next")
        revocation = BrokerPolicyRevocationV1(
            context.policies[0].artifact_id,
            POLICY_NOW,
            POLICY_NOW,
            context.policies[0].evidence_ids,
            "withdrawn",
        )
        source.current = replace(context, revocations=(revocation,))
        with pytest.raises(BrokerPolicyError):
            next(events)
    assert adapter.calls.count("next") == pulled
    assert adapter.calls[-1] == "close"


@pytest.mark.parametrize("status", (None, Status.UNKNOWN, Status.DENIED))
def test_consume_refuses_before_arbitrary_source_properties_or_iteration(
    native_legacy, fixed_policy_clock, status
):
    from contextlib import nullcontext
    from histdatacom.broker_capture import consume_broker_capture_source

    inputs, output = native_legacy
    calls = []

    class Source:
        @property
        def session_id(self):
            calls.append("session_id")
            return inputs.session.session_id

        def iter_events(self):
            calls.append("iter_events")
            return iter(inputs.events)

    manager = (
        nullcontext()
        if status is None
        else provider_policy_scope(
            MutablePolicySource(
                policy_context(
                    legacy_capture_binding(inputs.session), status=status
                )
            )
        )
    )
    with manager, pytest.raises(BrokerPolicyError):
        consume_broker_capture_source(
            Source(),
            provider_request=BrokerLegacyCaptureV1(inputs.session, output),
        )
    assert calls == []


@pytest.mark.parametrize("callback_kind", ("sink", "consumer"))
def test_consume_dispatch_requires_specific_operation_before_callback(
    native_legacy, fixed_policy_clock, callback_kind
):
    from histdatacom.broker_capture import consume_broker_capture_source

    inputs, output = native_legacy
    calls = []

    class Source:
        session_id = inputs.session.session_id

        def iter_events(self):
            return iter(inputs.events)

    class Sink:
        def append(self, event):
            calls.append(event.event_id)

    class Consumer:
        def on_event(self, event):
            calls.append(event.event_id)

    operation = (
        Operation.RETAIN_LOCAL
        if callback_kind == "sink"
        else Operation.MATERIAL_USE
    )
    context = policy_context(
        legacy_capture_binding(inputs.session),
        changes=((operation, DataClass.HEALTH, Status.DENIED),),
    )
    kwargs = (
        {"sink": Sink()}
        if callback_kind == "sink"
        else {"consumers": (Consumer(),)}
    )
    with provider_policy_scope(MutablePolicySource(context)):
        with pytest.raises(BrokerPolicyError):
            consume_broker_capture_source(
                Source(),
                provider_request=BrokerLegacyCaptureV1(inputs.session, output),
                **kwargs,
            )
    assert calls == []


def test_expiry_refuses_writer_terminal_without_fabricating_completion(
    tmp_path, native_legacy, fixed_policy_clock
):
    from histdatacom.broker_capture import AppendOnlyBrokerCaptureWriterV1

    inputs, output = native_legacy
    context = policy_context(
        legacy_capture_binding(inputs.session), expires_at_ns=POLICY_NOW + 1
    )
    with provider_policy_scope(MutablePolicySource(context)):
        writer = AppendOnlyBrokerCaptureWriterV1(
            tmp_path / "capture",
            session=inputs.session,
            storage_policy=inputs.storage_policy,
            provider_request=BrokerLegacyCaptureV1(inputs.session, output),
        )
        writer.append(inputs.events[0])
        before = {
            path: path.read_bytes()
            for path in (tmp_path / "capture").rglob("*")
            if path.is_file()
        }
        fixed_policy_clock[0] += 1
        with pytest.raises(BrokerPolicyError):
            writer.close()
    assert writer.manifest.state.value == "open"
    after = {
        path: path.read_bytes()
        for path in (tmp_path / "capture").rglob("*")
        if path.is_file()
    }
    assert before == after


@pytest.fixture(scope="module")
def declared_contexts():
    binding = _binding()
    return {status: policy_context(binding, status=status) for status in Status}


@pytest.mark.parametrize("operation", tuple(Operation))
@pytest.mark.parametrize("data_class", tuple(DataClass))
@pytest.mark.parametrize("status", tuple(Status))
def test_complete_declared_matrix_never_inherits_other_operation_or_class(
    declared_contexts, operation, data_class, status
):
    # This exercises pure declaration arithmetic, not host-native permission.
    context = declared_contexts[status]
    request = _request(
        _subject((context.policies[0].binding,), (data_class,)), operation
    )
    decision = decide_provider_operation(context, request)
    assert decision.status is status
    assert len(decision.cells) == 1
    assert decision.cells[0].data_class is data_class
    assert decision.allowed is (status is Status.ALLOWED)
    assert BrokerPolicyDecisionV1.from_json(decision.to_json()) == decision


@pytest.mark.parametrize("second", (Status.DENIED, Status.UNKNOWN))
def test_mixed_provider_and_class_closure_is_conjunctive(second):
    a, b = _binding("a"), _binding("b")
    context = _combined(
        policy_context(a),
        policy_context(
            b, changes=((Operation.CAPTURE, DataClass.CONTENT_HASHES, second),)
        ),
    )
    request = _request(
        _subject(
            (a, b), (DataClass.NORMALIZED_QUOTES, DataClass.CONTENT_HASHES)
        )
    )
    decision = decide_provider_operation(context, request)
    assert decision.status is second and not decision.allowed
    assert len(decision.cells) == 4
    refused = tuple(
        cell for cell in decision.cells if cell.status is not Status.ALLOWED
    )
    assert len(refused) == 1
    assert refused[0].binding_id == b.artifact_id
    assert refused[0].data_class is DataClass.CONTENT_HASHES


def test_missing_second_binding_cannot_borrow_first_provider_allowance():
    a, b = _binding("a"), _binding("b")
    decision = decide_provider_operation(
        policy_context(a), _request(_subject((a, b)))
    )
    assert decision.status is Status.UNKNOWN
    assert (
        next(
            cell for cell in decision.cells if cell.binding_id == b.artifact_id
        ).policy_id
        is None
    )


@pytest.mark.parametrize(
    "mutation", ("status", "cells", "deadline", "remove-cell")
)
def test_correctly_resealed_false_decision_is_recomputed(mutation):
    binding = _binding()
    decision = decide_provider_operation(
        policy_context(binding, status=Status.DENIED),
        _request(_subject((binding,))),
    )
    envelope = decision.to_dict()
    assert _reseal(envelope) == decision.to_json()
    payload = envelope["payload"]
    if mutation == "status":
        payload["status"] = Status.ALLOWED.value
    elif mutation == "cells":
        payload["cells"][0]["status"] = Status.ALLOWED.value
        payload["cells"][0]["reasons"] = []
        payload["status"] = Status.ALLOWED.value
    elif mutation == "deadline":
        payload["valid_until_ns"] = None
    else:
        payload["cells"] = []
    with pytest.raises(ValueError, match="replay"):
        BrokerPolicyDecisionV1.from_json(_reseal(envelope))


@pytest.mark.parametrize(
    "at,allowed", ((2, False), (3, True), (999, True), (1000, False))
)
def test_acknowledgement_and_expiry_are_half_open(at, allowed):
    binding = _binding()
    context = policy_context(binding, expires_at_ns=1000)
    decision = decide_provider_operation(
        context, _request(_subject((binding,)), at=at)
    )
    assert decision.allowed is allowed
    assert decision.valid_until_ns == 1000


@pytest.mark.parametrize("target", ("policy", "acknowledgement"))
def test_known_scheduled_revocation_bounds_allowance_before_effective_time(
    target,
):
    binding = _binding()
    context = policy_context(binding)
    target_id = (
        context.policies[0]
        if target == "policy"
        else context.acknowledgements[0]
    ).artifact_id
    revocation = BrokerPolicyRevocationV1(
        target_id, 900, 1001, (context.evidence[0].artifact_id,), "withdrawn"
    )
    context = replace(context, revocations=(revocation,))
    before = decide_provider_operation(context, _request(_subject((binding,))))
    after = decide_provider_operation(
        context, _request(_subject((binding,)), at=1001)
    )
    assert before.allowed and before.valid_until_ns == 1001
    assert not after.allowed
    # Old evidence is still readable as-of; it is not a new live permission.
    assert BrokerPolicyDecisionV1.from_json(before.to_json()) == before


def test_future_recorded_revocation_does_not_rewrite_historical_decision():
    binding = _binding()
    context = policy_context(binding)
    revocation = BrokerPolicyRevocationV1(
        context.policies[0].artifact_id,
        1001,
        900,
        (context.evidence[0].artifact_id,),
        "terms_changed",
    )
    context = replace(context, revocations=(revocation,))
    assert decide_provider_operation(
        context, _request(_subject((binding,)))
    ).allowed
    assert not decide_provider_operation(
        context, _request(_subject((binding,)), at=1001)
    ).allowed


def test_missing_or_unacknowledged_terms_never_become_allowed():
    binding = _binding()
    context = policy_context(binding)
    unacknowledged = replace(context, acknowledgements=())
    assert not decide_provider_operation(
        unacknowledged, _request(_subject((binding,)))
    ).allowed
    with pytest.raises(ValueError, match="evidence"):
        replace(context, evidence=())
    with pytest.raises(ValueError, match="predates"):
        replace(
            context,
            acknowledgements=(
                replace(context.acknowledgements[0], acknowledged_at_ns=1),
            ),
        )


@pytest.mark.parametrize("deadline", (None, 1001, 1500))
def test_finite_retention_is_not_accepted_as_a_timestamp_only_promise(deadline):
    binding = _binding()
    context = policy_context(binding, maximum_retention_ns=5000)
    request = _request(
        _subject((binding,)),
        Operation.RETAIN_LOCAL,
        intended_retention_deadline_ns=deadline,
    )
    decision = decide_provider_operation(context, request)
    assert not decision.allowed
    assert (
        "finite_retention_enforcement_unsupported" in decision.cells[0].reasons
    )


def test_synthetic_evidence_label_never_grants_operational_permission():
    binding = _binding()
    context = policy_context(binding, evidence_kind=EvidenceKind.SYNTHETIC)
    for subject in (
        _subject((binding,)),
        replace(_subject((binding,)), evidence_kind=EvidenceKind.SYNTHETIC),
    ):
        assert not decide_provider_operation(context, _request(subject)).allowed


@pytest.mark.parametrize("kind", ("subject", "request", "decision"))
def test_caller_declared_metadata_cannot_substitute_for_native_subject(kind):
    binding = _binding()
    context = policy_context(binding)
    subject = _subject((binding,))
    request = _request(subject)
    decision = decide_provider_operation(context, request)
    value = {"subject": subject, "request": request, "decision": decision}[kind]
    with provider_policy_scope(MutablePolicySource(context)):
        with pytest.raises(BrokerPolicyError, match="native_subject_required"):
            require_provider_operation(value, Operation.CAPTURE)


def test_scope_has_no_default_authority_and_is_restored_after_exception():
    binding = _binding()
    context = policy_context(binding)
    with pytest.raises(
        BrokerPolicyError, match="current_process_policy_scope_required"
    ):
        require_provider_operation(object(), Operation.CAPTURE)
    with pytest.raises(RuntimeError, match="fixture-stop"):
        with provider_policy_scope(MutablePolicySource(context)):
            raise RuntimeError("fixture-stop")
    with pytest.raises(
        BrokerPolicyError, match="current_process_policy_scope_required"
    ):
        require_provider_operation(object(), Operation.CAPTURE)


def test_fresh_source_refusal_does_not_echo_its_exception():
    canary = "generated-private-policy-source-canary"

    class BrokenSource:
        def read_policy_context(self):
            raise RuntimeError(canary)

    with pytest.raises(BrokerPolicyError) as caught:
        with provider_policy_scope(BrokenSource()):
            pytest.fail("invalid source entered scope")
    assert canary not in "".join(traceback.format_exception(caught.value))


def test_scope_rechecks_pinned_execution_and_preserves_observed_revocations():
    binding = _binding()
    context = policy_context(binding)
    source = MutablePolicySource(context)
    with provider_policy_scope(source):
        source.current = replace(
            context, execution=replace(context.execution, commercial_use=True)
        )
        with pytest.raises(
            BrokerPolicyError,
            match="selected_policy_or_execution_context_changed",
        ):
            require_provider_operation(object(), Operation.CAPTURE)
    source.current = context
    revocation = BrokerPolicyRevocationV1(
        context.policies[0].artifact_id,
        900,
        900,
        (context.evidence[0].artifact_id,),
        "withdrawn",
    )
    with provider_policy_scope(source):
        source.current = replace(context, revocations=(revocation,))
        # A metadata-only request remains refused, but the scope has now seen
        # this complete ledger. Removing that recorded revocation is rollback.
        with pytest.raises(BrokerPolicyError, match="native_subject_required"):
            require_provider_operation(_subject((binding,)), Operation.CAPTURE)
        source.current = context
        with pytest.raises(
            BrokerPolicyError, match="review_inventory_removed_or_rewritten"
        ):
            require_provider_operation(object(), Operation.CAPTURE)
    assert source.reads == 5
