"""Closed derived-statistics classification, never an authorization waiver."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from histdatacom.broker_plugin_policy.bindings import (
    BrokerLegacyCaptureV1,
    BrokerProviderOutputContractV1,
    BrokerSDKInvocationV1,
    _composite_ref,
    _resolve_native,
    _ResolvedNative,
    _restore_native,
)
from histdatacom.broker_plugin_policy.contracts import (
    BrokerPolicyDataClass as DataClass,
    BrokerPolicySubjectV1,
)
from histdatacom.broker_plugin_provenance.contracts import (
    BrokerProvenanceSealV1,
)


@dataclass(frozen=True, slots=True)
class CrossFeedDerivationV1:
    """Exact two-source request; public producer separately replays its roots."""

    left_request: BrokerLegacyCaptureV1 | BrokerSDKInvocationV1
    right_request: BrokerLegacyCaptureV1 | BrokerSDKInvocationV1
    left_root: BrokerProvenanceSealV1
    right_root: BrokerProvenanceSealV1
    configuration: object
    parent_clock_id: str | None = None


def snapshot_request(
    value: object,
) -> BrokerLegacyCaptureV1 | BrokerSDKInvocationV1:
    """Detach the closed invocation, including non-serializable legacy wrapper."""
    if type(value) is BrokerSDKInvocationV1:
        return cast(
            BrokerSDKInvocationV1,
            _restore_native(value, {BrokerSDKInvocationV1}),
        )
    if type(value) is not BrokerLegacyCaptureV1:
        raise ValueError("exact native provider invocation required")
    from histdatacom.broker_capture.contracts import BrokerCaptureSessionV1

    request = BrokerLegacyCaptureV1(
        _restore_native(value.session, {BrokerCaptureSessionV1}),
        _restore_native(
            value.output_contract, {BrokerProviderOutputContractV1}
        ),
    )
    _resolve_native(request)
    return request


def resolve_cross_feed_native(native: CrossFeedDerivationV1) -> _ResolvedNative:
    from .clock_contracts import ClockFitRequestV1
    from .matching_contracts import MatchingPolicyV1

    if type(native) is not CrossFeedDerivationV1:
        raise ValueError("exact cross-feed native derivation required")
    left = snapshot_request(native.left_request)
    right = snapshot_request(native.right_request)
    left_root = _restore_native(native.left_root, {BrokerProvenanceSealV1})
    right_root = _restore_native(native.right_root, {BrokerProvenanceSealV1})
    config = _restore_native(
        native.configuration, {ClockFitRequestV1, MatchingPolicyV1}
    )
    if (type(config) is MatchingPolicyV1) != (
        native.parent_clock_id is not None
    ):
        raise ValueError("matching requires a frozen clock parent")
    if native.parent_clock_id is not None:
        import re

        if (
            re.fullmatch(
                r"cross-feed-clock-model:sha256:[a-f0-9]{64}",
                native.parent_clock_id,
            )
            is None
        ):
            raise ValueError("invalid frozen clock parent identity")
    resolved = tuple(_resolve_native(item).subject for item in (left, right))
    bindings = {
        binding.artifact_id: binding
        for source in resolved
        for binding in source.bindings
    }
    classes = {
        DataClass.FINGERPRINTS,
        DataClass.NORMALIZED_QUOTES,
        DataClass.HEALTH,
        DataClass.CONTENT_HASHES,
    }
    for source in resolved:
        classes.update(source.data_classes)
    reference = _composite_ref(
        "cross-feed-derivation",
        {
            "left": resolved[0].to_dict(),
            "right": resolved[1].to_dict(),
            "left_root": left_root.to_dict(),
            "right_root": right_root.to_dict(),
            "configuration": config.to_dict(),
            "parent_clock_id": native.parent_clock_id,
        },
    )
    return _ResolvedNative(
        BrokerPolicySubjectV1(
            reference,
            tuple(bindings[key] for key in sorted(bindings)),
            tuple(sorted(classes)),
        ),
        config.to_json(),
    )
