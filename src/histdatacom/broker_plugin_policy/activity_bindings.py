"""Exact retained activity metadata and its complete native broker parents.

This reconciles declared native lineage, not original quote authenticity or
unretained execution. No new activity wire fields or scientific claims exist.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, cast

from .bindings import (
    _ResolvedNative,
    _composite_ref,
    _preflight_native,
    _ref,
    _resolve_native,
    _restore_native,
)
from .contracts import BrokerPolicyDataClass, BrokerPolicySubjectV1

if TYPE_CHECKING:
    from histdatacom.synthetic.activity import ReconstructionActivityManifestV1

_LIMITATIONS = frozenset(
    {
        "centralized_traded_volume_unavailable",
        "zero_exposure_duration",
        "event_confidence_partial_or_unavailable",
        "stream_identity_unavailable",
        "no_quote_transition",
        "spread_is_a_liquidity_proxy_not_traded_volume",
    }
)
_TRUNCATION = re.compile(
    r"(?:source_version_ids|generator_ids|generator_versions|"
    r"generator_config_ids|reference_ids|motif_ids|feed_epoch_ids|"
    r"constraint_set_ids|stream_ids)_truncated:occurrence_count="
    r"[1-9][0-9]*:sha256=[0-9a-f]{64}"
)


def _activity_profile_ids(
    artifact: ReconstructionActivityManifestV1,
) -> tuple[str, ...]:
    """Refuse lineage whose legacy bounded projection omitted broker parents."""
    if any(
        limitation.startswith("broker_profile_ids_truncated:")
        for item in artifact.slices
        for limitation in item.limitations
    ):
        raise ValueError("activity requires complete broker profile provenance")
    identities = tuple(
        sorted(
            {
                identity
                for item in artifact.slices
                for identity in item.broker_profile_ids
            }
        )
    )
    if len(identities) > 16:
        raise ValueError("activity broker parent inventory exceeds limit")
    return identities


def _resolve_activity_native(
    native: object,
) -> _ResolvedNative | None:
    """Resolve only the exact activity family; other families remain separate."""
    from histdatacom.broker_capture.fingerprint_contracts import (
        BrokerDeliveryFingerprintV1,
    )
    from histdatacom.synthetic.activity import ReconstructionActivityManifestV1

    from .derived import BrokerDerivedArtifactV1

    if (
        type(native) is not BrokerDerivedArtifactV1
        or type(native.artifact) is not ReconstructionActivityManifestV1
    ):
        return None
    if (
        type(native.fingerprints) is not tuple
        or not 1 <= len(native.fingerprints) <= 16
    ):
        raise ValueError("activity requires bounded exact fingerprint roots")
    _preflight_native(native)
    artifact = cast(
        ReconstructionActivityManifestV1,
        _restore_native(native.artifact, {ReconstructionActivityManifestV1}),
    )
    fingerprints = tuple(
        _restore_native(item, {BrokerDeliveryFingerprintV1})
        for item in native.fingerprints
    )
    identities = tuple(sorted(item.fingerprint_id for item in fingerprints))
    if len(set(identities)) != len(
        identities
    ) or identities != _activity_profile_ids(artifact):
        raise ValueError("activity fingerprint inventory differs from lineage")
    parents = tuple(
        _resolve_native(item).subject
        for item in sorted(fingerprints, key=lambda value: value.fingerprint_id)
    )
    bindings = {
        binding.artifact_id: binding
        for parent in parents
        for binding in parent.bindings
    }
    if len(bindings) > 16:
        raise ValueError("activity provider binding inventory exceeds limit")
    classes = {
        BrokerPolicyDataClass.BROKER_SYNTHETIC,
        BrokerPolicyDataClass.CONTENT_HASHES,
    }
    for parent in parents:
        classes.update(parent.data_classes)
    for item in artifact.slices:
        limitations = (
            *item.limitations,
            *(
                reason
                for metric in item.metrics
                for reason in metric.limitations
            ),
        )
        if any(
            reason not in _LIMITATIONS and _TRUNCATION.fullmatch(reason) is None
            for reason in limitations
        ):
            classes.add(BrokerPolicyDataClass.RAW_PAYLOAD)
    text = artifact.to_json()
    return _ResolvedNative(
        BrokerPolicySubjectV1(
            _composite_ref(
                "broker-derived-artifact",
                {
                    "artifact": _ref(
                        type(artifact).__name__, artifact.manifest_id, text
                    ),
                    "fingerprints": [parent.native_ref for parent in parents],
                },
            ),
            tuple(bindings[key] for key in sorted(bindings)),
            tuple(sorted(classes)),
        ),
        text,
    )
