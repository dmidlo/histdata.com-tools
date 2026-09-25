"""Closed provider-data classification for host-produced health evidence."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .bindings import (
    BrokerLegacyCaptureV1,
    BrokerSDKInvocationV1,
    _ResolvedNative,
    _composite_ref,
    _native_ref,
    _restore_native,
    legacy_capture_binding,
    sdk_invocation_binding,
)
from .contracts import BrokerPolicyDataClass as DataClass, BrokerPolicySubjectV1


@dataclass(frozen=True, slots=True)
class BrokerHostHealthEvidenceV1:
    invocation: BrokerSDKInvocationV1 | BrokerLegacyCaptureV1
    native_header: object
    health_header: object
    artifact: object


def resolve_host_health_native(
    native: BrokerHostHealthEvidenceV1,
) -> _ResolvedNative:
    from histdatacom.broker_plugin_health.contracts import (
        BrokerHostHealthAuditV1,
        BrokerHostHealthHeaderV1,
        BrokerHostHealthNativeFamily as Family,
        BrokerHostHealthObservationV1,
    )

    header: Any = _restore_native(
        native.health_header, {BrokerHostHealthHeaderV1}
    )
    artifact: Any = _restore_native(
        native.artifact,
        {
            BrokerHostHealthHeaderV1,
            BrokerHostHealthObservationV1,
            BrokerHostHealthAuditV1,
        },
    )
    classes = {DataClass.HEALTH, DataClass.CONTENT_HASHES}
    if type(native.invocation) is BrokerSDKInvocationV1:
        from histdatacom.broker_plugin_lifecycle.contracts import (
            BrokerLifecycleHeaderV1,
        )

        invocation = BrokerSDKInvocationV1.from_json(
            native.invocation.to_json()
        )
        parent = _restore_native(
            native.native_header, {BrokerLifecycleHeaderV1}
        )
        binding = sdk_invocation_binding(invocation)
        if (
            header.family is not Family.LIFECYCLE_V1
            or parent.plan.to_json() != invocation.plan.to_json()
            or header.capture_id != parent.artifact_id
            or header.plugin_id != parent.plan.candidate.registration.plugin_id
            or header.provider_id
            != invocation.configuration_profile.provider_id
            or header.configuration_id
            != invocation.configuration_profile.artifact_id
            or header.symbols != parent.symbols
            or header.queue_capacity != parent.policy.queue_items
        ):
            raise ValueError("host health native SDK binding mismatch")
        if invocation.configuration_profile.private_field_names:
            classes.add(DataClass.PRIVATE_ACCOUNT)
    elif type(native.invocation) is BrokerLegacyCaptureV1:
        from histdatacom.broker_capture.contracts import BrokerCaptureSessionV1

        parent = _restore_native(native.native_header, {BrokerCaptureSessionV1})
        if native.invocation.session.to_json() != parent.to_json():
            raise ValueError("host health native legacy session substitution")
        binding = legacy_capture_binding(parent)
        if (
            header.family is not Family.LEGACY_CAPTURE_V1
            or header.capture_id != parent.session_id
            or header.plugin_id != parent.adapter_id
            or header.provider_id != parent.adapter_id
            or header.configuration_id != parent.adapter_config_sha256
        ):
            raise ValueError("host health native legacy binding mismatch")
        if (
            parent.account_id_sha256 is not None
            or parent.host_id_sha256 is not None
        ):
            classes.add(DataClass.PRIVATE_ACCOUNT)
    else:
        raise ValueError("unsupported host health native invocation")
    if type(artifact) is BrokerHostHealthHeaderV1 and artifact != header:
        raise ValueError("host health header substitution")
    if type(artifact) is BrokerHostHealthAuditV1 and artifact.header != header:
        raise ValueError("host health audit header substitution")
    reference = _composite_ref(
        "host-health",
        {
            "binding": binding,
            "header": _native_ref(header),
            "artifact": _native_ref(artifact),
        },
    )
    return _ResolvedNative(
        BrokerPolicySubjectV1(reference, (binding,), tuple(sorted(classes))),
        artifact.to_json(),
    )
