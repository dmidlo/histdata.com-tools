"""Native SDK scientific subjects retain actual SDK provider bindings."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .bindings import (
    BrokerSDKInvocationV1,
    _composite_ref,
    _native_ref,
    _ref,
    _ResolvedNative,
    _sdk_subject,
)
from .contracts import BrokerPolicyDataClass as DataClass
from .contracts import BrokerPolicySubjectV1

if TYPE_CHECKING:
    from histdatacom.broker_capture.fingerprint_v3 import (
        BrokerDeliveryFingerprintV3,
        BrokerLifecycleFingerprintFitConfigV1,
    )


@dataclass(frozen=True, slots=True)
class BrokerLifecycleFingerprintFitV1:
    capture_requests: tuple[BrokerSDKInvocationV1, ...]
    fit_config: BrokerLifecycleFingerprintFitConfigV1


def resolve_lifecycle_fingerprint(
    native: BrokerLifecycleFingerprintFitV1 | BrokerDeliveryFingerprintV3,
) -> _ResolvedNative:
    from histdatacom.broker_capture.fingerprint_v3 import (
        BrokerDeliveryFingerprintV3,
        BrokerLifecycleFingerprintFitConfigV1,
    )
    from histdatacom.broker_plugin_compatibility import (
        BrokerCaptureCompatibilityV1,
    )
    from histdatacom.broker_plugins.contracts import BrokerPluginMetadataV1

    from .bindings import _sdk_metadata_classes
    from .provenance_bindings import (
        BrokerProvenanceEvidenceV1,
        resolve_provenance_native,
    )

    classes = {
        DataClass.FINGERPRINTS,
        DataClass.HEALTH,
        DataClass.CONTENT_HASHES,
    }
    if type(native) is BrokerLifecycleFingerprintFitV1:
        if (
            type(native.fit_config) is not BrokerLifecycleFingerprintFitConfigV1
            or type(native.capture_requests) is not tuple
            or not 1
            <= len(native.capture_requests)
            <= native.fit_config.max_capture_manifests
            or any(
                type(x) is not BrokerSDKInvocationV1
                for x in native.capture_requests
            )
        ):
            raise ValueError(
                "native fingerprint fitting requires bounded exact SDK requests"
            )
        policy = BrokerLifecycleFingerprintFitConfigV1.from_dict(
            native.fit_config.to_dict()
        )
        sources = tuple(
            _sdk_subject(x).subject for x in native.capture_requests
        )
        reference = _composite_ref(
            "lifecycle-fingerprint-fit",
            {
                "requests": tuple(x.native_ref for x in sources),
                "config": _ref(
                    "BrokerLifecycleFingerprintFitConfigV1",
                    policy.config_id,
                    policy.to_json(),
                ),
            },
        )
        artifact_json = None
    elif type(native) is BrokerDeliveryFingerprintV3:
        from .bindings import _preflight_native

        _preflight_native(native)
        value = BrokerDeliveryFingerprintV3.from_json(native.to_json())
        sources = tuple(
            resolve_provenance_native(
                BrokerProvenanceEvidenceV1(
                    root.invocation,
                    root.manifest.header,
                    root.health_audit.header,
                    root.header,
                    root.seal,
                )
            ).subject
            for root in value.capture_roots
        )
        for root in value.capture_roots:
            if root.compatibility_json is not None:
                evidence = BrokerCaptureCompatibilityV1.from_json(
                    root.compatibility_json
                )
                for text in evidence.runtime_metadata_jsons:
                    classes |= _sdk_metadata_classes(
                        BrokerPluginMetadataV1.from_json(text)
                    )
        reference = _native_ref(value, "fingerprint_id")
        artifact_json = value.to_json()
    else:
        raise ValueError("unsupported native fingerprint subject")
    bindings = {b.artifact_id: b for s in sources for b in s.bindings}
    if len(bindings) != 1:
        raise ValueError(
            "native fingerprint requires one exact compatible provider configuration"
        )
    for source in sources:
        classes.update(source.data_classes)
    return _ResolvedNative(
        BrokerPolicySubjectV1(
            reference,
            tuple(bindings[k] for k in sorted(bindings)),
            tuple(sorted(classes)),
        ),
        artifact_json,
    )
