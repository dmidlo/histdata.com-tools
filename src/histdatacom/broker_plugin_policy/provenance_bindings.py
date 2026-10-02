"""Closed source classification for host-chain evidence, never a hash waiver."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .bindings import (
    BrokerLegacyCaptureV1,
    BrokerLegacyRecordV1,
    BrokerSDKInvocationV1,
    BrokerSDKLifecycleV1,
    BrokerSDKRecordV1,
    _composite_ref,
    _native_ref,
    _resolve_native,
    _ResolvedNative,
    _restore_native,
)
from .contracts import BrokerPolicyDataClass as DataClass
from .contracts import BrokerPolicySubjectV1


@dataclass(frozen=True, slots=True)
class BrokerProvenanceEvidenceV1:
    invocation: BrokerSDKInvocationV1 | BrokerLegacyCaptureV1
    native_header: object
    health_header: object
    header: object
    artifact: object
    session: object = None
    identity: object = None


def resolve_provenance_native(
    native: BrokerProvenanceEvidenceV1,
) -> _ResolvedNative:
    from histdatacom.broker_plugin_health.contracts import (
        BrokerHostHealthHeaderV1,
        BrokerHostHealthObservationV1,
    )
    from histdatacom.broker_plugin_provenance.contracts import (
        BrokerProvenanceCheckpointV1,
        BrokerProvenanceEntryV1,
        BrokerProvenanceHeaderV1,
        BrokerProvenanceLinkV1,
        BrokerProvenanceSealV1,
        BrokerProvenanceTerminalV1,
    )
    from histdatacom.broker_plugin_provenance.contracts import (
        BrokerProvenanceEntryKind as Kind,
    )
    from histdatacom.broker_plugin_provenance.contracts import (
        BrokerProvenanceNativeFamily as Family,
    )
    from histdatacom.broker_plugin_provenance.native_common import (
        native_sha256,
        parse_ingress,
    )

    from .health_bindings import (
        BrokerHostHealthEvidenceV1,
        resolve_host_health_native,
    )

    header: Any = _restore_native(native.header, {BrokerProvenanceHeaderV1})
    health: Any = _restore_native(
        native.health_header, {BrokerHostHealthHeaderV1}
    )
    artifact: Any = _restore_native(
        native.artifact,
        {
            BrokerProvenanceHeaderV1,
            BrokerProvenanceEntryV1,
            BrokerProvenanceLinkV1,
            BrokerProvenanceCheckpointV1,
            BrokerProvenanceTerminalV1,
            BrokerProvenanceSealV1,
        },
    )
    source = _resolve_native(native.invocation).subject
    health_source = resolve_host_health_native(
        BrokerHostHealthEvidenceV1(
            native.invocation,
            native.native_header,
            health,
            health,
        ),
    ).subject
    classes = set(source.data_classes) | set(health_source.data_classes)
    classes |= {DataClass.HEALTH, DataClass.CONTENT_HASHES}
    if source.bindings != health_source.bindings:
        raise ValueError("provenance source/health provider substitution")
    parent: Any = native.native_header
    if (
        header.capture_id != health.capture_id
        or header.plugin_id != health.plugin_id
        or header.provider_id != health.provider_id
        or header.configuration_id != health.configuration_id
        or header.provider_decision_id != health.provider_decision_id
        or header.permission_manifest_id != health.permission_manifest_id
        or header.permission_context_id != health.permission_context_id
        or header.permission_decision_id != health.permission_decision_id
        or header.started_at_utc_ns != health.started_at_utc_ns
        or header.started_at_monotonic_ns != health.started_at_monotonic_ns
        or header.native_header_sha256 != native_sha256(parent.to_json())
    ):
        raise ValueError("provenance header differs from exact native parents")
    sdk = type(native.invocation) is BrokerSDKInvocationV1
    if sdk:
        invocation = native.invocation
        assert isinstance(invocation, BrokerSDKInvocationV1)
        candidate = invocation.plan.candidate
        registration = candidate.registration
        if (
            header.family is not Family.LIFECYCLE_V1
            or header.native_header_id != parent.artifact_id
            or header.plugin_version != registration.plugin_version
            or header.distribution_name != registration.distribution_name
            or header.distribution_version != registration.distribution_version
            or header.implementation_sha256 != candidate.implementation_sha256
            or header.registration_sha256 != candidate.registration_sha256
            or header.sdk_version != parent.inventory.sdk_version
            or header.configuration_sha256
            != native_sha256(
                invocation.configuration_profile.public_configuration_json,
            )
        ):
            raise ValueError(
                "provenance SDK software/configuration substitution"
            )
    else:
        if (
            type(native.invocation) is not BrokerLegacyCaptureV1
            or header.family is not Family.LEGACY_CAPTURE_V1
            or header.native_header_id != parent.session_id
            or header.plugin_version != parent.adapter_version
            or header.configuration_sha256 != parent.adapter_config_sha256
        ):
            raise ValueError("provenance legacy identity substitution")
    if type(artifact) is BrokerProvenanceHeaderV1:
        if artifact != header:
            raise ValueError("provenance header substitution")
        entry = None
    elif type(artifact) is BrokerProvenanceLinkV1:
        entry = artifact.entry
    else:
        if artifact.header_id != header.artifact_id:
            raise ValueError("provenance artifact belongs to another header")
        entry = artifact if type(artifact) is BrokerProvenanceEntryV1 else None
    if entry is not None:
        if entry.header_id != header.artifact_id:
            raise ValueError("provenance entry belongs to another header")
        actual: Any = None
        if entry.kind is Kind.HOST_OBSERVATION:
            observation = BrokerHostHealthObservationV1.from_json(
                entry.payload_json
            )
            if (
                observation.epoch != entry.epoch
                or observation.artifact_id != entry.native_record_id
            ):
                raise ValueError("provenance host observation identity differs")
            actual = BrokerHostHealthEvidenceV1(
                native.invocation,
                native.native_header,
                health,
                observation,
            )
        elif entry.kind is Kind.NATIVE_RECORD:
            if sdk:
                from histdatacom.broker_plugin_lifecycle.contracts import (
                    BrokerLifecycleRecordV1,
                )

                assert isinstance(native.invocation, BrokerSDKInvocationV1)
                record: Any = BrokerLifecycleRecordV1.from_json(
                    entry.payload_json
                )
                if (
                    record.artifact_id != entry.native_record_id
                    or record.epoch != entry.epoch
                ):
                    raise ValueError(
                        "provenance lifecycle record identity differs"
                    )
                actual = BrokerSDKLifecycleV1(
                    native.invocation,
                    native.native_header,
                    record,
                    native.session,
                    native.identity,
                )
            else:
                from histdatacom.broker_capture.contracts import (
                    BrokerCaptureEventV1,
                )

                record = BrokerCaptureEventV1.from_json(entry.payload_json)
                if record.event_id != entry.native_record_id:
                    raise ValueError(
                        "provenance legacy record identity differs"
                    )
                assert isinstance(native.invocation, BrokerLegacyCaptureV1)
                actual = BrokerLegacyRecordV1(
                    native.invocation.session,
                    record,
                    native.invocation.output_contract,
                )
        elif entry.kind is Kind.ADMITTED_INGRESS:
            payload, _, _ = parse_ingress(
                entry.payload_json, family=header.family.value
            )
            if entry.native_record_id is not None:
                raise ValueError(
                    "ingress cannot claim a persisted native record"
                )
            if sdk:
                assert isinstance(native.invocation, BrokerSDKInvocationV1)
                session: Any = native.session
                identity: Any = native.identity
                if session is None or identity is None:
                    raise ValueError(
                        "SDK ingress requires actual session/identity"
                    )
                actual = BrokerSDKRecordV1(
                    native.invocation,
                    payload,
                    session.session,
                    identity.metadata.metadata,
                )
            else:
                assert isinstance(native.invocation, BrokerLegacyCaptureV1)
                actual = BrokerLegacyRecordV1(
                    native.invocation.session,
                    payload,
                    native.invocation.output_contract,
                )
        elif entry.kind is Kind.TERMINAL:
            terminal = BrokerProvenanceTerminalV1.from_json(entry.payload_json)
            if terminal.header_id != header.artifact_id:
                raise ValueError("terminal provenance header differs")
        if actual is not None:
            resolved = _resolve_native(actual).subject
            if resolved.bindings != source.bindings:
                raise ValueError("provenance payload source differs")
            classes |= set(resolved.data_classes)
    reference = _composite_ref(
        "capture-provenance",
        {
            "source": source.native_ref,
            "header": _native_ref(header),
            "artifact": _native_ref(artifact),
        },
    )
    return _ResolvedNative(
        BrokerPolicySubjectV1(
            reference, source.bindings, tuple(sorted(classes))
        ),
        artifact.to_json(),
    )
