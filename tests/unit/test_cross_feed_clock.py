"""Exact invented kernel vectors, not authenticated capture/clock claims."""

from __future__ import annotations

from dataclasses import replace
from fractions import Fraction

import pytest

from histdatacom.cross_feed._wire import RationalV1
from histdatacom.cross_feed.capture_contracts import (
    NativeCaptureV1,
    NativeQuoteV1,
)
from histdatacom.cross_feed.clock import correct_clock_quote, fit_clock
from histdatacom.cross_feed.clock_contracts import (
    ClockCandidatePairV1,
    ClockFitPolicyV1,
    ClockFitRequestV1,
    ClockModelV1,
)

SECOND = 1_000_000_000
BASE = 1_000_000_000_000


def quote(side: str, i: int, source: int) -> NativeQuoteV1:
    return NativeQuoteV1(
        capture_id=side,
        event_id=f"{side}-{i:03d}",
        record_id=f"{side}-record-{i:03d}",
        sequence=i,
        epoch_id=f"{side}-epoch",
        symbol="eurusd",
        source_time_ns=source,
        source_precision_ns=1000,
        source_time_semantics="broker_event",
        receive_utc_ns=BASE + i * SECOND + 20_000_000,
        receive_monotonic_ns=i * SECOND + 20_000_000,
        clock_id=f"{side}-host",
        bid=RationalV1("11", "10"),
        ask=RationalV1("1101", "1000"),
        price_basis="bid_ask",
        source_sequence=None,
        source_message_id=None,
        source_batch_id=None,
        health_state="qualified_host_boundary_only",
        health_reasons=(),
    )


def capture(side: str, quotes: tuple[NativeQuoteV1, ...]) -> NativeCaptureV1:
    return NativeCaptureV1(
        side,
        f"{side}-manifest",
        f"{side}-audit",
        "qualified_host_boundary_only",
        (),
        quotes,
        "1" * 64,
        0,
    )


def inputs(
    differences: tuple[int, ...] = (12_000_000,) * 5,
) -> tuple[NativeCaptureV1, NativeCaptureV1, ClockFitRequestV1]:
    left = capture(
        "left",
        tuple(
            quote("left", i, BASE + i * SECOND + difference)
            for i, difference in enumerate(differences)
        ),
    )
    right = capture(
        "right",
        tuple(
            quote("right", i, BASE + i * SECOND)
            for i in range(len(differences))
        ),
    )
    request = ClockFitRequestV1(
        tuple(
            ClockCandidatePairV1(a.event_id, b.event_id)
            for a, b in zip(left.quotes, right.quotes)
        ),
        len(differences) - 1,
        len(differences) - 1,
    )
    return left, right, request


@pytest.mark.parametrize("offset", (12_000_000, -12_000_000, 0))
def test_exact_conditional_offset_and_reference_coordinate(offset: int) -> None:
    left, right, request = inputs((offset,) * 5)
    model = fit_clock(left, right, request)
    assert model.status == "ready"
    assert len(model.segments) == 1
    segment = model.segments[0]
    assert segment.offset_ns.value == offset
    assert segment.drift.value == 0
    assert segment.residual_mad_ns.value == 0
    for a, b in zip(left.quotes, right.quotes):
        corrected_a = correct_clock_quote(model, a)
        corrected_b = correct_clock_quote(model, b)
        assert corrected_a.status == corrected_b.status == "usable"
        assert corrected_a.corrected_time_ns is not None
        assert corrected_b.corrected_time_ns is not None
        assert corrected_a.corrected_time_ns.value == a.source_time_ns
        assert corrected_b.corrected_time_ns.value == a.source_time_ns
        assert corrected_a.uncertainty_radius_ns is not None
        assert corrected_b.uncertainty_radius_ns is not None
        assert corrected_a.uncertainty_radius_ns.value == 1000
        assert corrected_b.uncertainty_radius_ns.value >= 3000
    assert (
        model.interpretation
        == "conditional_source_clock_difference_not_latency"
    )
    assert model.causality_scope == "sealed_capture_offline_prefix_simulation"


@pytest.mark.parametrize("sign", (-1, 1))
def test_exact_affine_drift_is_not_float_timestamp_arithmetic(
    sign: int,
) -> None:
    left, right, request = inputs(
        tuple(12_000_000 + sign * i * 1_000_000 for i in range(5))
    )
    model = fit_clock(left, right, request)
    assert model.status == "ready"
    assert model.segments[0].drift.value == Fraction(sign, 1000)
    assert model.segments[0].offset_ns.value == 12_000_000
    for a, b in zip(left.quotes, right.quotes):
        corrected = correct_clock_quote(model, b)
        assert corrected.corrected_time_ns is not None
        assert corrected.corrected_time_ns.value == a.source_time_ns


def test_raw_difference_mad_is_distinct_from_affine_residual_mad() -> None:
    left, right, request = inputs(
        tuple(x * 1_000_000 for x in (12, 11, 13, 10, 200))
    )
    segment = fit_clock(left, right, request).segments[0]
    assert segment.median_difference_ns.value == 12_000_000
    assert segment.difference_mad_ns.value == 1_000_000
    assert segment.scaled_difference_mad_ns.value == 1_482_600
    assert (
        segment.scaled_mad_ns.value
        == segment.residual_mad_ns.value * Fraction(7413, 5000)
    )


def test_even_support_retains_fractional_nanoseconds() -> None:
    left, right, request = inputs((0, 0, 1, 1))
    segment = fit_clock(left, right, request).segments[0]
    assert segment.median_difference_ns.value == Fraction(1, 2)
    assert segment.difference_mad_ns.value == Fraction(1, 2)
    assert segment.scaled_difference_mad_ns.value == Fraction(7413, 10000)


def test_sustained_jump_splits_but_isolated_outlier_does_not() -> None:
    left, right, request = inputs((12_000_000,) * 4 + (32_000_000,) * 4)
    model = fit_clock(left, right, request)
    assert model.status == "ready"
    assert tuple(s.pair_count for s in model.segments) == (4, 4)
    assert tuple(s.offset_ns.value for s in model.segments) == (
        12_000_000,
        32_000_000,
    )
    assert tuple(s.right_first_sequence for s in model.segments) == (0, 4)
    for a, b in zip(left.quotes, right.quotes):
        actual = correct_clock_quote(model, b)
        assert actual.corrected_time_ns is not None
        assert actual.corrected_time_ns.value == a.source_time_ns
    a, b, req = inputs((12_000_000,) * 3 + (200_000_000,) + (12_000_000,) * 3)
    assert len(fit_clock(a, b, req).segments) == 1


def test_native_epochs_split_even_without_detected_offset_jump() -> None:
    left, right, request = inputs((12_000_000,) * 6)
    right = replace(
        right,
        quotes=tuple(
            replace(q, epoch_id="right-second") if q.sequence >= 3 else q
            for q in right.quotes
        ),
    )
    model = fit_clock(left, right, request)
    assert tuple(s.right_epoch_id for s in model.segments) == (
        "right-epoch",
        "right-second",
    )
    assert all(s.pair_count == 3 for s in model.segments)


@pytest.mark.parametrize("count", (1, 2))
def test_insufficient_segment_is_not_silent_identity_clock(count: int) -> None:
    left, right, request = inputs((12_000_000,) * count)
    model = fit_clock(left, right, request)
    assert model.status == "insufficient"
    assert "insufficient_segment_pairs" in model.segments[0].reasons
    assert correct_clock_quote(model, right.quotes[0]).status == "unavailable"


def test_no_drift_time_support_is_explicit() -> None:
    left, right, request = inputs()
    left = replace(
        left,
        quotes=tuple(
            replace(q, source_time_ns=BASE + 12_000_000) for q in left.quotes
        ),
    )
    right = replace(
        right,
        quotes=tuple(replace(q, source_time_ns=BASE) for q in right.quotes),
    )
    model = fit_clock(left, right, request)
    assert model.status == "insufficient"
    assert "source_time_has_no_drift_support" in model.segments[0].reasons


def ex_ante_inputs() -> (
    tuple[NativeCaptureV1, NativeCaptureV1, ClockFitRequestV1]
):
    left, right, request = inputs()
    return (
        left,
        right,
        replace(
            request,
            calibration_pairs=request.calibration_pairs[:3],
            left_cutoff_sequence=2,
            right_cutoff_sequence=2,
            mode="frozen_ex_ante",
            policy=replace(
                request.policy, maximum_prediction_horizon_ns=2 * SECOND
            ),
        ),
    )


def test_ex_ante_requires_frozen_both_side_cutoffs_and_future_quote() -> None:
    left, right, request = ex_ante_inputs()
    model = fit_clock(left, right, request)
    assert correct_clock_quote(model, right.quotes[3]).reasons == (
        "ex_ante_decision_cutoffs_required",
    )
    assert (
        "model_after_decision_cutoff"
        in correct_clock_quote(
            model, right.quotes[3], decision_cutoffs=(1, 3)
        ).reasons
    )
    assert (
        "quote_after_decision_cutoff"
        in correct_clock_quote(
            model, right.quotes[4], decision_cutoffs=(3, 3)
        ).reasons
    )
    assert (
        "quote_not_after_frozen_calibration"
        in correct_clock_quote(
            model, right.quotes[2], decision_cutoffs=(3, 3)
        ).reasons
    )
    actual = correct_clock_quote(
        model, right.quotes[3], decision_cutoffs=(3, 3)
    )
    assert actual.status == "usable"
    assert actual.corrected_time_ns is not None
    assert actual.corrected_time_ns.value == left.quotes[3].source_time_ns


@pytest.mark.parametrize(
    "cutoffs", ((True, 3), (3,), [3, 3], (-1, 3), (2**63, 3))
)
def test_cutoff_wire_does_not_accept_coercions(cutoffs: object) -> None:
    left, right, request = inputs()
    with pytest.raises(ValueError, match="exact host-sequence"):
        correct_clock_quote(fit_clock(left, right, request), right.quotes[0], decision_cutoffs=cutoffs)  # type: ignore[arg-type]


def test_future_calibration_pair_refuses_before_fitting() -> None:
    left, right, request = inputs()
    with pytest.raises(ValueError, match="declared cutoff"):
        fit_clock(left, right, replace(request, left_cutoff_sequence=1))


def test_horizon_and_epoch_are_not_silently_extended() -> None:
    left, right, request = ex_ante_inputs()
    model = fit_clock(left, right, replace(request, policy=ClockFitPolicyV1()))
    assert correct_clock_quote(
        model, right.quotes[3], decision_cutoffs=(3, 3)
    ).reasons == ("outside_frozen_clock_support",)
    unknown_epoch = replace(right.quotes[2], epoch_id="not-fitted")
    retrospective = fit_clock(
        left, right, replace(request, mode="retrospective")
    )
    assert correct_clock_quote(retrospective, unknown_epoch).reasons == (
        "outside_frozen_clock_support",
    )


@pytest.mark.parametrize(
    ("changes", "reason"),
    (
        (
            {"source_time_ns": None, "source_precision_ns": None},
            "source_clock_unavailable",
        ),
        (
            {"source_time_semantics": "adapter_receive"},
            "source_clock_semantics_unsupported",
        ),
        (
            {"health_state": "historical_observations_unavailable"},
            "host_health_unavailable",
        ),
        ({"health_state": "degraded"}, "host_health_failure_unspecified"),
        (
            {"health_reasons": ("known_host_drop_rate",)},
            "host_health:known_host_drop_rate",
        ),
        (
            {"health_state": "native_prefix_diagnostics"},
            "host_prefix_evidence_unavailable",
        ),
    ),
)
def test_refused_pair_remains_in_complete_denominator(
    changes: dict[str, object], reason: str
) -> None:
    left, right, request = inputs()
    right = replace(
        right, quotes=(replace(right.quotes[0], **changes),) + right.quotes[1:]
    )
    model = fit_clock(left, right, request)
    assert len(model.pair_uses) == 5
    assert model.pair_uses[0].status == "excluded"
    assert reason in model.pair_uses[0].reasons
    assert model.status == "partial"
    assert sum(segment.pair_count for segment in model.segments) == 4


def test_prefix_evidence_and_explicit_upstream_exception_are_not_qualification() -> (
    None
):
    left, right, request = inputs()
    right = replace(
        right,
        health_state="insufficient_evidence",
        health_reasons=("upstream_loss_unidentified",),
        quotes=tuple(
            replace(
                q,
                health_state="native_prefix_diagnostics",
                health_reasons=("upstream_loss_unidentified",),
                prefix_health_observation_id=f"observed-{q.sequence}",
            )
            for q in right.quotes
        ),
    )
    model = fit_clock(left, right, request)
    assert model.status == "ready"
    assert (
        model.delay_diagnostics[-1].health_state == "native_prefix_diagnostics"
    )
    assert model.delay_diagnostics[-1].health_reasons == (
        "upstream_loss_unidentified",
    )
    assert (
        fit_clock(
            left,
            right,
            replace(
                request,
                policy=replace(request.policy, allowed_health_reasons=()),
            ),
        ).status
        == "insufficient"
    )


def test_future_health_changes_provenance_not_prefix_numbers_or_status() -> (
    None
):
    left, right, request = ex_ante_inputs()
    right = replace(
        right,
        quotes=tuple(
            replace(
                q,
                health_state="native_prefix_diagnostics",
                prefix_health_observation_id=f"observed-{q.sequence}",
            )
            for q in right.quotes
        ),
    )
    baseline = fit_clock(left, right, request)
    future = replace(
        right,
        health_state="degraded",
        health_reasons=("clock_discontinuity",),
        health_audit_id="later-audit",
        quotes=right.quotes[:3]
        + tuple(
            replace(q, health_reasons=("clock_discontinuity",))
            for q in right.quotes[3:]
        ),
    )
    altered = fit_clock(left, future, request)
    assert baseline.artifact_id != altered.artifact_id
    assert baseline.right_prefix_sha256 == altered.right_prefix_sha256
    assert baseline.status == altered.status == "ready"
    assert baseline.segments == altered.segments
    assert baseline.delay_diagnostics == altered.delay_diagnostics
    assert (
        "host_health:clock_discontinuity"
        in correct_clock_quote(
            altered, future.quotes[3], decision_cutoffs=(3, 3)
        ).reasons
    )


def test_clock_health_exception_is_explicit_and_identity_bound() -> None:
    left, right, request = inputs()
    right = replace(
        right,
        quotes=tuple(
            replace(q, health_reasons=("clock_discontinuity",))
            for q in right.quotes
        ),
    )
    refused = fit_clock(left, right, request)
    allowed = fit_clock(
        left,
        right,
        replace(
            request,
            policy=replace(
                request.policy,
                allowed_health_reasons=(
                    "clock_discontinuity",
                    "upstream_loss_unidentified",
                ),
            ),
        ),
    )
    assert refused.status == "insufficient"
    assert allowed.status == "ready"
    assert refused.artifact_id != allowed.artifact_id


def test_delay_diagnostics_allow_negative_source_host_difference() -> None:
    left, right, request = inputs((30_000_000,) * 5)
    model = fit_clock(left, right, request)
    diagnostic = model.delay_diagnostics[0]
    assert diagnostic.median_ns is not None
    assert diagnostic.median_ns.value == -10_000_000
    assert diagnostic.sample_count == 5
    assert diagnostic.source_increment_gcd_ns == SECOND
    assert (
        diagnostic.minimum_precision_ns
        == diagnostic.maximum_precision_ns
        == 1000
    )


def test_host_clock_jump_is_retained_in_delay_diagnostics() -> None:
    left, right, request = inputs()
    right = replace(
        right,
        quotes=tuple(
            replace(
                q,
                receive_utc_ns=q.receive_utc_ns
                + (20_000_000 if q.sequence >= 3 else 0),
            )
            for q in right.quotes
        ),
    )
    diagnostic = fit_clock(left, right, request).delay_diagnostics[-1]
    assert diagnostic.host_offset_jump_count == 1
    assert diagnostic.maximum_abs_host_offset_change_ns == 20_000_000


def test_timezone_shaped_offset_is_not_silently_repaired() -> None:
    left, right, request = inputs((3_600 * SECOND,) * 5)
    request = replace(
        request,
        policy=replace(request.policy, maximum_offset_ns=7_200 * SECOND),
    )
    model = fit_clock(left, right, request)
    assert model.status == "insufficient"
    assert "possible_timezone_or_dst_mismatch" in model.segments[0].reasons


@pytest.mark.parametrize(
    "field", ("fit_method", "jump_method", "quantization_rule")
)
def test_unknown_algorithm_cannot_change_frozen_rule(field: str) -> None:
    with pytest.raises(ValueError, match="unsupported clock rule"):
        replace(ClockFitPolicyV1(), **{field: "caller_algorithm"})


@pytest.mark.parametrize("defect", ("missing", "symbol", "same-root"))
def test_pair_membership_scope_is_admitted(defect: str) -> None:
    left, right, request = inputs()
    if defect == "missing":
        right = replace(right, quotes=right.quotes[1:])
    elif defect == "symbol":
        right = replace(
            right,
            quotes=(replace(right.quotes[0], symbol="gbpusd"),)
            + right.quotes[1:],
        )
    else:
        right = left
    with pytest.raises(ValueError):
        fit_clock(left, right, request)


def test_complete_clock_wire_roundtrip_and_resealed_denominator_refusal() -> (
    None
):
    model = fit_clock(*inputs())
    assert ClockModelV1.from_json(model.to_json()) == model
    with pytest.raises(ValueError, match="denominator"):
        replace(model, pair_uses=model.pair_uses[:-1])
    with pytest.raises(ValueError, match="status"):
        replace(model, status="insufficient")
    with pytest.raises(ValueError, match="interpretation"):
        replace(model, interpretation="true_absolute_utc")


def test_pair_work_bound_and_duplicate_candidates_fail_closed() -> None:
    with pytest.raises(ValueError, match="pair count"):
        inputs((0,) * 129)
    left, right, request = inputs()
    with pytest.raises(ValueError, match="sorted unique"):
        replace(request, calibration_pairs=(request.calibration_pairs[0],) * 2)
    with pytest.raises(ValueError, match="foreign capture"):
        correct_clock_quote(
            fit_clock(left, right, request), quote("foreign", 0, BASE)
        )


def test_subclass_serializer_cannot_be_used_as_clock_authority() -> None:
    class HostileRequest(ClockFitRequestV1):
        def to_payload(self) -> dict[str, object]:
            raise AssertionError("untrusted serializer executed")

    left, right, request = inputs()
    hostile = HostileRequest(request.calibration_pairs, 4, 4)
    with pytest.raises(TypeError, match="exact clock/native"):
        fit_clock(left, right, hostile)
