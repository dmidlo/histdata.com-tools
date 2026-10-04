"""Native public clock-to-match workflow with fresh roots and current rights."""

from __future__ import annotations

from histdatacom.broker_plugin_policy import (
    BrokerPolicyOperation,
    require_provider_operation,
)

from .clock import fit_clock
from .clock_contracts import ClockFitRequestV1, ClockModelV1
from .matching import match_cross_feed
from .matching_contracts import MatchingPolicyV1, MatchingReportV1
from .native import NativeCaptureRefV1, _snapshot, admit_capture
from .rights import CrossFeedDerivationV1


def _authorize(
    left: NativeCaptureRefV1,
    right: NativeCaptureRefV1,
    configuration: ClockFitRequestV1 | MatchingPolicyV1,
    parent_clock_id: str | None = None,
) -> None:
    require_provider_operation(
        CrossFeedDerivationV1(
            left.provider_request,
            right.provider_request,
            left.expected_root,
            right.expected_root,
            configuration,
            parent_clock_id,
        ),
        BrokerPolicyOperation.DERIVE,
    )


def fit_clock_model(
    left_ref: NativeCaptureRefV1,
    right_ref: NativeCaptureRefV1,
    request: ClockFitRequestV1,
) -> ClockModelV1:
    """Fit declared correspondences from actual independently sealed captures.

    Current provider-policy scope is mandatory. The return is an in-memory
    derived diagnostic, not a permission to retain, publish or redistribute it.
    """
    left_ref, right_ref = _snapshot(left_ref), _snapshot(right_ref)
    if type(request) is not ClockFitRequestV1:
        raise ValueError("exact frozen clock request required")
    request = ClockFitRequestV1.from_json(request.to_json())
    _authorize(left_ref, right_ref, request)
    left, right = admit_capture(left_ref), admit_capture(right_ref)
    model = fit_clock(left, right, request)
    _authorize(left_ref, right_ref, request)
    return model


def replay_clock_model(
    left_ref: NativeCaptureRefV1,
    right_ref: NativeCaptureRefV1,
    expected: ClockModelV1,
    *,
    expected_model_id: str,
) -> ClockModelV1:
    """Recompute from native bytes; a rehashed altered artifact does not pass."""
    if type(expected) is not ClockModelV1:
        raise ValueError("exact frozen clock model required")
    expected = ClockModelV1.from_json(expected.to_json())
    if expected.artifact_id != expected_model_id:
        raise ValueError("independently retained clock identity differs")
    actual = fit_clock_model(left_ref, right_ref, expected.request)
    if actual.to_json() != expected.to_json():
        raise ValueError(
            "frozen clock does not replay from actual native inputs"
        )
    return actual


def match_captures(
    left_ref: NativeCaptureRefV1,
    right_ref: NativeCaptureRefV1,
    model: ClockModelV1,
    policy: MatchingPolicyV1,
    *,
    expected_model_id: str,
    decision_cutoffs: tuple[int, int] | None = None,
) -> MatchingReportV1:
    """Consume the exact frozen calibration; never invent a new fit policy.

    Recalculation here is verification of the frozen artifact, not a second
    matcher-specific clock estimate. Its bytes must match before any assignment.
    """
    left_ref, right_ref = _snapshot(left_ref), _snapshot(right_ref)
    if type(model) is not ClockModelV1 or type(policy) is not MatchingPolicyV1:
        raise ValueError("exact clock model and matching policy required")
    model = ClockModelV1.from_json(model.to_json())
    policy = MatchingPolicyV1.from_json(policy.to_json())
    if model.artifact_id != expected_model_id:
        raise ValueError("independently retained clock identity differs")
    _authorize(left_ref, right_ref, policy, expected_model_id)
    _authorize(left_ref, right_ref, model.request)
    left, right = admit_capture(left_ref), admit_capture(right_ref)
    replayed = fit_clock(left, right, model.request)
    if replayed.to_json() != model.to_json():
        raise ValueError(
            "frozen clock does not replay from actual native inputs"
        )
    result = match_cross_feed(
        left,
        right,
        model,
        policy,
        decision_cutoffs=decision_cutoffs,
    )
    _authorize(left_ref, right_ref, policy, expected_model_id)
    return result
