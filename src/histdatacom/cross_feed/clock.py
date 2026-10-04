"""Exact, bounded conditional clock math over detached native projections.

These are computation kernels, not source authentication. The public API must
freshly replay both native captures and the retained model before matching.
No pair is declared a true latent correspondence by this module, no source
timestamp is silently treated as UTC, and no delay is called network latency.
"""

from __future__ import annotations

from dataclasses import dataclass
from fractions import Fraction
from itertools import pairwise
from math import gcd
from typing import TypeVar

from ._wire import RationalV1, Record, canonical_json, sha256
from .capture_contracts import NativeCaptureV1, NativeQuoteV1
from .clock_contracts import (
    ClockCorrectionV1,
    ClockDelayDiagnosticV1,
    ClockFitPolicyV1,
    ClockFitRequestV1,
    ClockModelV1,
    ClockPairUseV1,
    ClockSegmentV1,
)

_T = TypeVar("_T", bound=Record)
_SCALE = Fraction(7413, 5000)
_HEALTH_STATES = frozenset(
    {
        "qualified_host_boundary_only",
        "native_prefix_diagnostics",
        "degraded",
        "insufficient_evidence",
    }
)


def _readmit(value: _T, cls: type[_T]) -> _T:
    if type(value) is not cls:
        raise TypeError("exact clock/native contract required")
    # The base reads dataclass fields, not a caller-overridden nested serializer.
    return cls.from_payload(Record.to_payload(value))


def _rational(value: Fraction | int) -> RationalV1:
    return RationalV1.from_fraction(value)


def _checked(value: Fraction) -> Fraction:
    return _rational(value).value


def _median(values: list[Fraction]) -> Fraction:
    if not values:
        raise ValueError("median has no clock support")
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return _checked((ordered[middle - 1] + ordered[middle]) / 2)


def _mad(values: list[Fraction], center: Fraction) -> Fraction:
    return _median([abs(value - center) for value in values])


def _health(quote: NativeQuoteV1, policy: ClockFitPolicyV1) -> tuple[str, ...]:
    reasons: set[str] = set()
    if quote.health_state not in _HEALTH_STATES:
        reasons.add("host_health_unavailable")
    if (
        quote.health_state == "native_prefix_diagnostics"
        and quote.prefix_health_observation_id is None
    ):
        reasons.add("host_prefix_evidence_unavailable")
    if (
        quote.health_state
        not in ("qualified_host_boundary_only", "native_prefix_diagnostics")
        and not quote.health_reasons
    ):
        reasons.add("host_health_failure_unspecified")
    reasons.update(
        "host_health:" + reason
        for reason in quote.health_reasons
        if reason not in policy.allowed_health_reasons
    )
    return tuple(sorted(reasons))


def _clock_reasons(
    quote: NativeQuoteV1, policy: ClockFitPolicyV1
) -> tuple[str, ...]:
    reasons = set(_health(quote, policy))
    if quote.source_time_ns is None or quote.source_precision_ns is None:
        reasons.add("source_clock_unavailable")
    elif quote.source_time_ns < 0 or quote.source_precision_ns < 1:
        reasons.add("source_clock_invalid")
    if quote.source_time_semantics not in ("broker_event", "exchange_event"):
        reasons.add("source_clock_semantics_unsupported")
    return tuple(sorted(reasons))


@dataclass(frozen=True, slots=True)
class _Pair:
    left: NativeQuoteV1
    right: NativeQuoteV1
    difference: Fraction


def _pair_time(pair: _Pair) -> int:
    assert pair.right.source_time_ns is not None
    return pair.right.source_time_ns


def _split_jumps(
    rows: list[_Pair], policy: ClockFitPolicyV1
) -> list[list[_Pair]]:
    """Split a sustained discontinuity, not one isolated offset outlier.

    A cut needs both an adjacent discontinuity and minimum-support medians on
    either side exceeding the allowed affine drift plus the fixed threshold.
    Native epoch boundaries have already split rows and are never bridged.
    """
    minimum = policy.minimum_segment_pairs
    limit = Fraction(policy.maximum_drift_ppm, 1_000_000)
    start = 0
    result: list[list[_Pair]] = []
    for split in range(minimum, len(rows) - minimum + 1):
        if split - start < minimum:
            continue
        left = rows[split - minimum : split]
        right = rows[split : split + minimum]
        delta = rows[split].difference - rows[split - 1].difference
        adjacent_allowance = policy.jump_threshold_ns + limit * abs(
            _pair_time(rows[split]) - _pair_time(rows[split - 1])
        )
        if abs(delta) <= adjacent_allowance:
            continue
        jump = _median([row.difference for row in right]) - _median(
            [row.difference for row in left]
        )
        elapsed = abs(
            _median([Fraction(_pair_time(row)) for row in right])
            - _median([Fraction(_pair_time(row)) for row in left])
        )
        if (
            abs(jump) > policy.jump_threshold_ns + limit * elapsed
            and delta * jump > 0
        ):
            result.append(rows[start:split])
            start = split
    result.append(rows[start:])
    return result


def _fit_segment(rows: list[_Pair], policy: ClockFitPolicyV1) -> ClockSegmentV1:
    times = [_pair_time(row) for row in rows]
    differences = [row.difference for row in rows]
    origin = min(times)
    slopes = [
        _checked((differences[j] - differences[i]) / (times[j] - times[i]))
        for i in range(len(rows))
        for j in range(i + 1, len(rows))
        if times[j] != times[i]
    ]
    drift = _median(slopes) if slopes else Fraction(0)
    offset = _median(
        [
            _checked(value - drift * (time - origin))
            for time, value in zip(times, differences)
        ]
    )
    residuals = [
        _checked(value - offset - drift * (time - origin))
        for time, value in zip(times, differences)
    ]
    residual_mad = _mad(residuals, _median(residuals))
    raw_median = _median(differences)
    left_precision = max(row.left.source_precision_ns or 0 for row in rows)
    right_precision = max(row.right.source_precision_ns or 0 for row in rows)
    span = max(times) - min(times)
    drift_radius = (
        _mad(slopes, drift) * _SCALE * policy.residual_band_multiplier.value
        + Fraction(2 * (left_precision + right_precision), span)
        if span and slopes
        else Fraction(0)
    )
    reasons = []
    if len(rows) < policy.minimum_segment_pairs:
        reasons.append("insufficient_segment_pairs")
    if not slopes:
        reasons.append("source_time_has_no_drift_support")
    if abs(drift) > Fraction(policy.maximum_drift_ppm, 1_000_000):
        reasons.append("drift_outside_declared_bound")
    if max(abs(offset + drift * (time - origin)) for time in times) > (
        policy.maximum_offset_ns
    ):
        reasons.append("offset_outside_declared_bound")
    # A near-whole-hour displacement is a diagnostic, not a timezone fix.
    hour = 3_600_000_000_000
    nearest_hour = (abs(raw_median) + hour // 2) // hour
    if nearest_hour and abs(abs(raw_median) - nearest_hour * hour) <= (
        policy.jump_threshold_ns + left_precision + right_precision
    ):
        reasons.append("possible_timezone_or_dst_mismatch")
    left_times = [row.left.source_time_ns for row in rows]
    assert all(time is not None for time in left_times)
    return ClockSegmentV1(
        left_epoch_id=rows[0].left.epoch_id,
        right_epoch_id=rows[0].right.epoch_id,
        left_first_sequence=min(row.left.sequence for row in rows),
        left_last_sequence=max(row.left.sequence for row in rows),
        right_first_sequence=min(row.right.sequence for row in rows),
        right_last_sequence=max(row.right.sequence for row in rows),
        left_minimum_time_ns=min(row.left.source_time_ns or 0 for row in rows),
        left_maximum_time_ns=max(row.left.source_time_ns or 0 for row in rows),
        right_minimum_time_ns=min(times),
        right_maximum_time_ns=max(times),
        origin_time_ns=origin,
        pair_count=len(rows),
        pair_digest=sha256(
            canonical_json(
                [(row.left.event_id, row.right.event_id) for row in rows]
            )
        ),
        offset_ns=_rational(offset),
        drift=_rational(drift),
        median_difference_ns=_rational(raw_median),
        difference_mad_ns=_rational(_mad(differences, raw_median)),
        scaled_difference_mad_ns=_rational(
            _mad(differences, raw_median) * _SCALE
        ),
        residual_mad_ns=_rational(residual_mad),
        scaled_mad_ns=_rational(residual_mad * _SCALE),
        drift_radius=_rational(drift_radius),
        left_precision_ns=left_precision,
        right_precision_ns=right_precision,
        status="insufficient" if reasons else "usable",
        reasons=tuple(sorted(reasons)),
    )


def _diagnostics(
    capture: NativeCaptureV1, cutoff: int, policy: ClockFitPolicyV1
) -> tuple[ClockDelayDiagnosticV1, ...]:
    groups: dict[tuple[str, str, tuple[str, ...]], list[NativeQuoteV1]] = {}
    for quote in capture.quotes:
        if quote.sequence <= cutoff:
            key = (quote.epoch_id, quote.health_state, quote.health_reasons)
            groups.setdefault(key, []).append(quote)
    output = []
    for (epoch, state, reasons), quotes in sorted(groups.items()):
        present = [q for q in quotes if q.source_time_ns is not None]
        values = [
            Fraction(q.receive_utc_ns - (q.source_time_ns or 0))
            for q in present
        ]
        increments = [
            (after.source_time_ns or 0) - (before.source_time_ns or 0)
            for before, after in pairwise(present)
        ]
        host_changes = [
            (after.receive_utc_ns - before.receive_utc_ns)
            - (after.receive_monotonic_ns - before.receive_monotonic_ns)
            for before, after in pairwise(quotes)
        ]
        center = _median(values) if values else None
        output.append(
            ClockDelayDiagnosticV1(
                capture.root_id,
                epoch,
                state,
                reasons,
                len(values),
                len(quotes) - len(values),
                _rational(min(values)) if values else None,
                _rational(max(values)) if values else None,
                _rational(center) if center is not None else None,
                (
                    _rational(_mad(values, center))
                    if center is not None
                    else None
                ),
                (
                    min(q.source_precision_ns or 0 for q in present)
                    if present
                    else None
                ),
                (
                    max(q.source_precision_ns or 0 for q in present)
                    if present
                    else None
                ),
                gcd(*(abs(value) for value in increments)),
                sum(value < 0 for value in increments),
                sum(
                    abs(value) > policy.jump_threshold_ns
                    for value in host_changes
                ),
                max((abs(value) for value in host_changes), default=0),
            )
        )
    return tuple(output)


def fit_clock(
    left: NativeCaptureV1,
    right: NativeCaptureV1,
    request: ClockFitRequestV1,
) -> ClockModelV1:
    """Compute conditional diagnostics; public native admission is separate."""
    request = _readmit(request, ClockFitRequestV1)
    left = _readmit(left, NativeCaptureV1)
    right = _readmit(right, NativeCaptureV1)
    if left.root_id == right.root_id:
        raise ValueError("clock calibration requires distinct captures")
    left_by_id = {quote.event_id: quote for quote in left.quotes}
    right_by_id = {quote.event_id: quote for quote in right.quotes}
    groups: dict[tuple[str, str], list[_Pair]] = {}
    uses = []
    for pair in request.calibration_pairs:
        a = left_by_id.get(pair.left_event_id)
        b = right_by_id.get(pair.right_event_id)
        if a is None or b is None:
            raise ValueError(
                "clock calibration pair is absent from native capture"
            )
        if a.symbol != b.symbol:
            raise ValueError("clock calibration pair symbols differ")
        if a.sequence > request.left_cutoff_sequence or b.sequence > (
            request.right_cutoff_sequence
        ):
            raise ValueError("clock calibration pair exceeds declared cutoff")
        reasons = tuple(
            sorted(
                set(_clock_reasons(a, request.policy))
                | set(_clock_reasons(b, request.policy))
            )
        )
        uses.append(
            ClockPairUseV1(pair, "excluded" if reasons else "included", reasons)
        )
        if not reasons:
            assert a.source_time_ns is not None
            assert b.source_time_ns is not None
            groups.setdefault((a.epoch_id, b.epoch_id), []).append(
                _Pair(a, b, Fraction(a.source_time_ns - b.source_time_ns))
            )
    segments: list[ClockSegmentV1] = []
    for rows in groups.values():
        rows.sort(key=lambda row: (row.right.sequence, row.left.sequence))
        segments.extend(
            _fit_segment(part, request.policy)
            for part in _split_jumps(rows, request.policy)
        )
    segments.sort(
        key=lambda segment: (
            segment.right_first_sequence,
            segment.left_first_sequence,
        )
    )
    usable = sum(segment.status == "usable" for segment in segments)
    status = (
        "insufficient"
        if not usable
        else (
            "ready"
            if usable == len(segments)
            and all(use.status == "included" for use in uses)
            else "partial"
        )
    )
    return ClockModelV1(
        left.root_id,
        right.root_id,
        left.artifact_id,
        right.artifact_id,
        left.health_audit_id,
        right.health_audit_id,
        sha256(
            canonical_json(
                tuple(
                    q
                    for q in left.quotes
                    if q.sequence <= request.left_cutoff_sequence
                )
            )
        ),
        sha256(
            canonical_json(
                tuple(
                    q
                    for q in right.quotes
                    if q.sequence <= request.right_cutoff_sequence
                )
            )
        ),
        request,
        tuple(uses),
        tuple(segments),
        _diagnostics(left, request.left_cutoff_sequence, request.policy)
        + _diagnostics(right, request.right_cutoff_sequence, request.policy),
        status,
    )


def correct_clock_quote(
    model: ClockModelV1,
    quote: NativeQuoteV1,
    *,
    decision_cutoffs: tuple[int, int] | None = None,
) -> ClockCorrectionV1:
    """Use the frozen rule without fitting; this kernel grants no authority."""
    model = _readmit(model, ClockModelV1)
    quote = _readmit(quote, NativeQuoteV1)
    if quote.capture_id not in (model.left_capture_id, model.right_capture_id):
        raise ValueError("clock correction quote belongs to a foreign capture")
    side = 0 if quote.capture_id == model.left_capture_id else 1
    cutoffs = (
        model.request.left_cutoff_sequence,
        model.request.right_cutoff_sequence,
    )
    if decision_cutoffs is not None and (
        type(decision_cutoffs) is not tuple
        or len(decision_cutoffs) != 2
        or any(
            type(value) is not int or not 0 <= value < 2**63
            for value in decision_cutoffs
        )
    ):
        raise ValueError(
            "clock decision requires two exact host-sequence cutoffs"
        )
    reasons = set(_clock_reasons(quote, model.request.policy))
    if decision_cutoffs is not None and quote.sequence > decision_cutoffs[side]:
        reasons.add("quote_after_decision_cutoff")
    if model.request.mode == "frozen_ex_ante":
        if decision_cutoffs is None:
            reasons.add("ex_ante_decision_cutoffs_required")
        elif any(a < b for a, b in zip(decision_cutoffs, cutoffs)):
            reasons.add("model_after_decision_cutoff")
        if quote.sequence <= cutoffs[side]:
            reasons.add("quote_not_after_frozen_calibration")
    model_id = model.artifact_id

    def unavailable(extra: str | None = None) -> ClockCorrectionV1:
        return ClockCorrectionV1(
            "unavailable",
            quote.event_id,
            quote.capture_id,
            model_id,
            None,
            None,
            None,
            tuple(sorted(reasons | ({extra} if extra else set()))),
        )

    if reasons:
        return unavailable()
    assert quote.source_time_ns is not None
    assert quote.source_precision_ns is not None
    horizon = model.request.policy.maximum_prediction_horizon_ns
    same_epoch = [
        segment
        for segment in model.segments
        if quote.epoch_id
        == (segment.left_epoch_id if side == 0 else segment.right_epoch_id)
    ]
    candidates = []
    for segment in same_epoch:
        first, last, lo, hi = (
            (
                segment.left_first_sequence,
                segment.left_last_sequence,
                segment.left_minimum_time_ns,
                segment.left_maximum_time_ns,
            )
            if side == 0
            else (
                segment.right_first_sequence,
                segment.right_last_sequence,
                segment.right_minimum_time_ns,
                segment.right_maximum_time_ns,
            )
        )
        # Never interpolate across an unresolved gap between detected jumps.
        within_sequence = first <= quote.sequence <= last
        after_final = quote.sequence > last and last == max(
            item.left_last_sequence if side == 0 else item.right_last_sequence
            for item in same_epoch
        )
        before_first = (
            model.request.mode == "retrospective"
            and quote.sequence < first
            and first
            == min(
                (
                    item.left_first_sequence
                    if side == 0
                    else item.right_first_sequence
                )
                for item in same_epoch
            )
        )
        if not (within_sequence or after_final or before_first):
            continue
        distance = max(lo - quote.source_time_ns, quote.source_time_ns - hi, 0)
        if distance <= horizon:
            candidates.append(segment)
    if not candidates:
        return unavailable("outside_frozen_clock_support")
    if len(candidates) != 1:
        return unavailable("ambiguous_clock_segment")
    segment = candidates[0]
    if segment.status != "usable":
        reasons.update(segment.reasons)
        return unavailable("clock_segment_insufficient")
    if side == 0:
        center = Fraction(quote.source_time_ns)
        radius = Fraction(quote.source_precision_ns)
    else:
        elapsed = quote.source_time_ns - segment.origin_time_ns
        correction = _checked(
            segment.offset_ns.value + segment.drift.value * elapsed
        )
        if abs(correction) > model.request.policy.maximum_offset_ns:
            return unavailable("predicted_offset_outside_declared_bound")
        center = _checked(Fraction(quote.source_time_ns) + correction)
        radius = _checked(
            Fraction(
                quote.source_precision_ns
                + segment.left_precision_ns
                + segment.right_precision_ns
            )
            + model.request.policy.residual_band_multiplier.value
            * segment.scaled_mad_ns.value
            + abs(elapsed) * segment.drift_radius.value
        )
    if not 0 <= center < 2**63:
        return unavailable("corrected_time_outside_native_domain")
    return ClockCorrectionV1(
        "usable",
        quote.event_id,
        quote.capture_id,
        model_id,
        segment.artifact_id,
        _rational(center),
        _rational(radius),
        (),
    )


__all__ = ["correct_clock_quote", "fit_clock"]
