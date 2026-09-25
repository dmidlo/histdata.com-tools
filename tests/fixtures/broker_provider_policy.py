"""Generated native broker inputs; no provider rights or scope is implicit."""

from __future__ import annotations

from dataclasses import dataclass
from contextlib import ExitStack, contextmanager
import hashlib
import json

from histdatacom.broker_capture import (
    BrokerAdapterMessageV1,
    BrokerCaptureEventKind,
    BrokerCaptureEventV1,
    BrokerCapturePriceTextSemantics,
    BrokerCaptureSessionV1,
    BrokerCaptureSourceTimestampSemantics,
    BrokerCaptureStoragePolicyV1,
)
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
    BrokerProviderPolicyV1,
)

SECOND_NS = 1_000_000_000
WALL_NS = 1_703_505_600 * SECOND_NS
MONOTONIC_NS = 10 * SECOND_NS
RAW_CANARY = "generated-opaque-provider-payload-never-permitted"
POLICY_NOW = 1000
POLICY_EXPIRY = 2**63 - 1


def generated_permission_authority(request):
    """Explicit emission-only grant for one exact synthetic native invocation."""
    from histdatacom.broker_plugin_permissions import (
        BrokerPermissionAuthorityV1,
        BrokerPermissionBindingV1,
        BrokerPermissionContextV1,
        BrokerPermissionGrantV1,
        BrokerPermissionManifestV1,
    )
    from histdatacom.broker_plugins import BROKER_PLUGIN_SDK_VERSION

    candidate = request.plan.candidate
    registration = candidate.registration
    atoms = ("emit:health", "emit:quotes", "emit:sizes", "raw_payload:emit")
    manifest = BrokerPermissionManifestV1(
        candidate.artifact_id,
        registration.distribution_name,
        registration.distribution_version,
        BROKER_PLUGIN_SDK_VERSION,
        registration.provider_ids,
        atoms,
        resource_abi="none",
    )
    binding = BrokerPermissionBindingV1(
        candidate.artifact_id,
        manifest.artifact_id,
        BROKER_PLUGIN_SDK_VERSION,
        request.configuration_profile.provider_id,
        request.configuration_profile.artifact_id,
    )
    grant = BrokerPermissionGrantV1(
        binding,
        atoms,
        "synthetic-fixture-operator",
        0,
        2**63 - 1,
        "a" * 32,
    )

    class Source:
        def read_context(self):
            return BrokerPermissionContextV1((grant,))

    return BrokerPermissionAuthorityV1(
        manifest, binding, grant.artifact_id, Source()
    )


def policy_context(
    binding: BrokerPolicyBindingV1,
    *,
    status: BrokerPolicyStatus = BrokerPolicyStatus.ALLOWED,
    changes: tuple[
        tuple[BrokerPolicyOperation, BrokerPolicyDataClass, BrokerPolicyStatus],
        ...,
    ] = (),
    expires_at_ns: int = POLICY_EXPIRY,
    maximum_retention_ns: int | None = None,
    evidence_kind: BrokerPolicyEvidenceKind = BrokerPolicyEvidenceKind.DECLARED,
) -> BrokerPolicyContextV1:
    """Explicit test declarations, never a magic provider-name exemption."""
    terms = b"Generated test terms, not actual provider permission."
    reference = BrokerPolicyReferenceV1(
        "synthetic-provider-policy-test-terms",
        "generated-test-terms-v1",
        hashlib.sha256(terms).hexdigest(),
        len(terms),
    )
    evidence = BrokerPolicyEvidenceV1(
        reference,
        binding.provider_id,
        "generated-test-terms",
        "1",
        "2020-01-01",
        "synthetic-test-issuer",
        1,
        evidence_kind,
        "fixture:generated-test-terms",
    )
    overrides = {(op, cls): value for op, cls, value in changes}
    rules = []
    for operation in sorted(BrokerPolicyOperation):
        for data_class in sorted(BrokerPolicyDataClass):
            selected = overrides.get((operation, data_class), status)
            retention = BrokerPolicyRetention.NOT_APPLICABLE
            if operation is BrokerPolicyOperation.RETAIN_LOCAL:
                retention = (
                    BrokerPolicyRetention.UNBOUNDED
                    if maximum_retention_ns is None
                    else BrokerPolicyRetention.FINITE
                )
            rules.append(
                BrokerPolicyRuleV1(
                    operation,
                    data_class,
                    selected,
                    (evidence.artifact_id,),
                    retention,
                    (
                        maximum_retention_ns
                        if operation is BrokerPolicyOperation.RETAIN_LOCAL
                        else None
                    ),
                )
            )
    policy = BrokerProviderPolicyV1(
        "1.0.0",
        binding,
        "MIT",
        "MIT",
        (evidence.artifact_id,),
        tuple(rules),
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
        expires_at_ns,
    )
    operator = b"generated-test-operator"
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
        expires_at_ns,
    )
    return BrokerPolicyContextV1(
        (policy,),
        (evidence,),
        (acknowledgement,),
        (),
        (policy.artifact_id,),
        BrokerPolicyExecutionV1(False, "synthetic", "fixture", "fixture"),
    )


class MutablePolicySource:
    """A mutable supplied ledger exposes whether every operation rereads it."""

    def __init__(self, context: BrokerPolicyContextV1) -> None:
        self.current = context
        self.reads = 0

    def read_policy_context(self) -> BrokerPolicyContextV1:
        self.reads += 1
        return self.current


@dataclass(frozen=True)
class LegacyPolicyInputs:
    session: BrokerCaptureSessionV1
    messages: tuple[BrokerAdapterMessageV1, ...]
    events: tuple[BrokerCaptureEventV1, ...]
    storage_policy: BrokerCaptureStoragePolicyV1


def legacy_policy_inputs(*, raw_hash: bool = False) -> LegacyPolicyInputs:
    """Construct native values only, before any scope, filesystem or transport."""
    session = BrokerCaptureSessionV1(
        adapter_id="fixture.provider-policy",
        adapter_version="1.0.0",
        adapter_config_sha256=hashlib.sha256(
            b"generated-provider-policy-configuration"
        ).hexdigest(),
        protocol="fixture-stream",
        environment_id="synthetic",
        server_id="fixture-server",
        started_at_utc_ns=WALL_NS,
        started_at_monotonic_ns=MONOTONIC_NS,
    )
    messages = [
        BrokerAdapterMessageV1(
            kind=BrokerCaptureEventKind.PROCESS_START,
            reason_code="collector_started",
        ),
        BrokerAdapterMessageV1(
            kind=BrokerCaptureEventKind.CONNECTION_OPEN,
            connection_id="connection-1",
        ),
        BrokerAdapterMessageV1(
            kind=BrokerCaptureEventKind.SUBSCRIPTION_ADD,
            connection_id="connection-1",
            subscription_id="eurusd-subscription",
            symbol="EURUSD",
        ),
    ]
    for index in range(16):
        bid = f"{1.1 + index * 0.0001:.4f}"
        ask = f"{float(bid) + 0.0002:.4f}"
        messages.append(
            BrokerAdapterMessageV1(
                kind=BrokerCaptureEventKind.QUOTE,
                source_event_time_ns=WALL_NS + index * 100_000_000,
                source_timestamp_semantics=(
                    BrokerCaptureSourceTimestampSemantics.BROKER_EVENT
                ),
                source_timestamp_precision_ns=100_000,
                source_sequence=index,
                source_message_id=f"quote-{index}",
                source_batch_id=f"batch-{index // 4}",
                symbol="EURUSD",
                bid=float(bid),
                ask=float(ask),
                bid_text=bid,
                ask_text=ask,
                price_text_semantics=(
                    BrokerCapturePriceTextSemantics.SOURCE_LEXEME
                ),
                raw_message_sha256=(
                    hashlib.sha256(RAW_CANARY.encode("ascii")).hexdigest()
                    if raw_hash
                    else None
                ),
            )
        )
    messages.append(
        BrokerAdapterMessageV1(
            kind=BrokerCaptureEventKind.PROCESS_STOP,
            reason_code="collector_stopped",
        )
    )
    events = tuple(
        BrokerCaptureEventV1(
            session_id=session.session_id,
            capture_sequence=index,
            receive_time_utc_ns=WALL_NS + (index + 1) * 100_000_000,
            receive_time_monotonic_ns=MONOTONIC_NS + (index + 1) * 100_000_000,
            message=message,
        )
        for index, message in enumerate(messages)
    )
    return LegacyPolicyInputs(
        session,
        tuple(messages),
        events,
        BrokerCaptureStoragePolicyV1(
            max_partition_events=7,
            max_partition_bytes=2 * 1024**2,
            max_partition_duration_ns=60 * SECOND_NS,
            max_session_bytes=32 * 1024**2,
            high_watermark_bytes=24 * 1024**2,
            max_retained_partitions=100,
            manifest_reserve_bytes=64 * 1024,
            fsync_each_event=False,
        ),
    )


class CountingLegacyAdapter:
    """Property and iterator canaries expose activation before admission."""

    def __init__(self, inputs: LegacyPolicyInputs) -> None:
        self.inputs = inputs
        self.calls: list[str] = []

    @property
    def adapter_id(self) -> str:
        self.calls.append("adapter_id")
        return self.inputs.session.adapter_id

    @property
    def adapter_version(self) -> str:
        self.calls.append("adapter_version")
        return self.inputs.session.adapter_version

    def iter_messages(self):
        self.calls.append("iter_messages")
        return _CountingMessages(iter(self.inputs.messages), self.calls)


class _CountingMessages:
    def __init__(self, iterator, calls):
        self.iterator = iterator
        self.calls = calls

    def __iter__(self):
        return self

    def __next__(self):
        self.calls.append("next")
        return next(self.iterator)

    def close(self):
        self.calls.append("close")


def legacy_output_contract():
    """Declare the exact generated native shape, granting no permission."""
    from histdatacom.broker_plugin_policy.bindings import (
        BrokerProviderOutputContractV1,
    )

    return BrokerProviderOutputContractV1(
        "legacy-capture-v1",
        tuple(
            sorted(
                {
                    message.kind.value
                    for message in legacy_policy_inputs().messages
                }
            )
        ),
    )


def sdk_policy_invocation(*, extra_capabilities=()):
    """Pure native plan/configuration, not an installed-module attestation."""
    from histdatacom.broker_plugin_capabilities import (
        BrokerCapabilityWorkflowV1,
        negotiate_broker_capabilities,
    )
    from histdatacom.broker_plugin_policy.bindings import (
        BrokerProviderConfigurationV1,
        BrokerProviderOutputContractV1,
        BrokerSDKInvocationV1,
    )
    from histdatacom.broker_plugin_registry import (
        BrokerPluginCandidateV1,
        BrokerPluginInventoryV1,
    )
    from histdatacom.broker_plugins import BrokerConfigurationSchemaV1
    from tests.fixtures.broker_capability_wheel import capability_registration

    registration = capability_registration()
    if extra_capabilities:
        from dataclasses import replace

        registration = replace(
            registration,
            capabilities=tuple(
                sorted(set(registration.capabilities) | set(extra_capabilities))
            ),
        )
    candidate = BrokerPluginCandidateV1(
        registration,
        hashlib.sha256(b"generated-test-registration").hexdigest(),
        hashlib.sha256(b"generated-test-entry-module").hexdigest(),
    )
    operations = tuple(
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
    )
    plan = negotiate_broker_capabilities(
        BrokerPluginInventoryV1((candidate,)),
        BrokerCapabilityWorkflowV1(operations, (), ()),
        plugin_id=registration.plugin_id,
    )
    return BrokerSDKInvocationV1(
        plan,
        BrokerProviderConfigurationV1(
            "generated-sdk-provider",
            "generated-config-profile",
            BrokerConfigurationSchemaV1(()).to_json(),
            "{}",
        ),
        BrokerProviderOutputContractV1("sdk-v1", ("quote",)),
    )


def generated_sdk_request(
    plan,
    *,
    schema=None,
    public_configuration=None,
    private_field_names=(),
    output_contract=None,
    provider_id=None,
    profile_id="generated-test-profile",
):
    """Explicit test request; neither creates rights nor invokes a plugin.

    Secrets are represented only by schema field names. Callers must supply the
    exact public configuration they intend to exercise, never ephemeral values.
    """
    from histdatacom.broker_plugin_policy.bindings import (
        BrokerProviderConfigurationV1,
        BrokerProviderOutputContractV1,
        BrokerSDKInvocationV1,
    )
    from histdatacom.broker_plugins import (
        BrokerConfigurationSchemaV1,
        BrokerEventKind,
    )

    if schema is None:
        schema = BrokerConfigurationSchemaV1(())
    if output_contract is None:
        output_contract = BrokerProviderOutputContractV1(
            "sdk-v1",
            tuple(sorted(kind.value for kind in BrokerEventKind)),
            allow_raw_hashes="raw-hashes.v1"
            in plan.required + plan.enabled_optional,
            allow_opaque_metadata=True,
            allow_private_account_metadata=True,
        )
    return BrokerSDKInvocationV1(
        plan,
        BrokerProviderConfigurationV1(
            (
                plan.candidate.registration.provider_ids[0]
                if provider_id is None
                else provider_id
            ),
            profile_id,
            schema.to_json(),
            json.dumps(
                {} if public_configuration is None else public_configuration,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            ),
            tuple(sorted(private_field_names)),
        ),
        output_contract,
    )


def generated_legacy_request(session):
    """Explicit rich legacy fixture shape, not an implicit permission scope."""
    from histdatacom.broker_plugin_policy.bindings import (
        BrokerLegacyCaptureV1,
        BrokerProviderOutputContractV1,
    )

    return BrokerLegacyCaptureV1(
        session,
        BrokerProviderOutputContractV1(
            "legacy-capture-v1",
            tuple(sorted(kind.value for kind in BrokerCaptureEventKind)),
            allow_raw_hashes=True,
            allow_opaque_metadata=True,
            allow_private_account_metadata=True,
        ),
    )


@contextmanager
def generated_provider_scope(*native_subjects):
    """Opt-in generated DECLARED terms for exact resolved test bindings.

    This is never autouse, an adapter-name exception, legal evidence or a rights
    bypass. Unknown native types still fail in the production closed resolver.
    """
    from histdatacom.broker_plugin_policy.bindings import (
        resolve_provider_subject,
    )
    from histdatacom.broker_plugin_policy.scope import provider_policy_scope

    if not native_subjects:
        raise ValueError("explicit native test subjects required")
    bindings = {}
    for native in native_subjects:
        for binding in resolve_provider_subject(native).bindings:
            bindings[binding.artifact_id] = binding
    contexts = tuple(policy_context(bindings[key]) for key in sorted(bindings))

    def union(field):
        entries = {
            item.artifact_id: item
            for context in contexts
            for item in getattr(context, field)
        }
        return tuple(entries[key] for key in sorted(entries))

    combined = BrokerPolicyContextV1(
        union("policies"),
        union("evidence"),
        union("acknowledgements"),
        (),
        tuple(
            sorted(
                identity
                for context in contexts
                for identity in context.selected_policy_ids
            )
        ),
        contexts[0].execution,
    )
    source = MutablePolicySource(combined)
    from histdatacom.broker_plugin_policy.bindings import BrokerSDKInvocationV1
    from histdatacom.broker_plugin_permissions.scope import (
        broker_permission_scope,
    )

    invocations = {
        item.artifact_id: item
        for item in native_subjects
        if type(item) is BrokerSDKInvocationV1
    }
    with ExitStack() as stack:
        stack.enter_context(provider_policy_scope(source))
        if len(invocations) == 1:
            stack.enter_context(
                broker_permission_scope(
                    generated_permission_authority(
                        next(iter(invocations.values()))
                    )
                )
            )
        yield source
