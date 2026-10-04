"""Exact diagnostics against independently supplied invented correspondences."""

from __future__ import annotations

from dataclasses import fields, replace
from fractions import Fraction

from ._wire import RationalV1
from .capture_contracts import NativeCaptureV1
from .clock_contracts import ClockModelV1
from .matching import (
    _preflight_size,
    _work_bound,
    match_cross_feed,
    replay_matching_report,
)
from .matching_contracts import (
    MAX_SENSITIVITY_POINTS,
    MAX_SOLVE_WORK,
    MatchingEvaluationV1,
    MatchingPolicyV1,
    MatchingReportV1,
    MatchingSensitivityPointV1,
    MatchingSensitivityV1,
    MatchingTruthV1,
)


def _truth(value: MatchingTruthV1, expected_id: str) -> MatchingTruthV1:
    if type(value) is not MatchingTruthV1:
        raise ValueError("exact independent synthetic truth required")
    value = MatchingTruthV1(
        **{field.name: getattr(value, field.name) for field in fields(value)}
    )
    if value.artifact_id != expected_id:
        raise ValueError("independent synthetic truth root mismatch")
    return value


def _rate(numerator: int, denominator: int) -> RationalV1 | None:
    return (
        RationalV1.from_fraction(Fraction(numerator, denominator))
        if denominator
        else None
    )


def _evaluate_replayed(
    report: MatchingReportV1, truth: MatchingTruthV1
) -> MatchingEvaluationV1:
    if (
        report.left_capture_id != truth.left_capture_id
        or report.right_capture_id != truth.right_capture_id
        or report.left_event_ids != truth.left_event_ids
        or report.right_event_ids != truth.right_event_ids
    ):
        raise ValueError("known truth must bind the complete event denominator")
    actual = {pair.key for pair in report.matches}
    expected = {pair.key for pair in truth.correspondences}
    true_positive = len(actual & expected)
    false_positive = len(actual - expected)
    false_negative = len(expected - actual)
    ambiguous = sum(
        pair.status in ("ambiguous", "ambiguous_uncertain")
        for pair in report.matches
    )
    edge_map = {edge.key: edge for edge in report.candidates}

    def samples(field: str) -> tuple[RationalV1, ...]:
        values = [getattr(edge_map[pair.key], field) for pair in report.matches]
        return tuple(sorted(values, key=lambda value: value.value))

    return MatchingEvaluationV1(
        report.artifact_id,
        truth.artifact_id,
        true_positive,
        false_positive,
        false_negative,
        _rate(true_positive, len(actual)),
        _rate(true_positive, len(expected)),
        _rate(ambiguous, len(actual)),
        _rate(
            len(report.source_only),
            len(report.left_event_ids) + len(report.right_event_ids),
        ),
        samples("time_residual_ns"),
        samples("bid_residual"),
        samples("ask_residual"),
    )


def evaluate_matching(
    left: NativeCaptureV1,
    right: NativeCaptureV1,
    model: ClockModelV1,
    report: MatchingReportV1,
    truth: MatchingTruthV1,
    *,
    expected_report_id: str,
    expected_truth_id: str,
) -> MatchingEvaluationV1:
    """Replay matching before scoring; caller truth is a declared test oracle.

    Neither a self-consistent truth hash nor synthetic precision/recall proves
    real-feed latent identity, source completeness, latency or retention.
    """
    truth = _truth(truth, expected_truth_id)
    replayed = replay_matching_report(
        left, right, model, report, expected_report_id=expected_report_id
    )
    return _evaluate_replayed(replayed, truth)


def evaluate_matching_sensitivity(
    left: NativeCaptureV1,
    right: NativeCaptureV1,
    model: ClockModelV1,
    policy: MatchingPolicyV1,
    truth: MatchingTruthV1,
    tolerances_ns: tuple[RationalV1, ...],
    *,
    expected_truth_id: str,
    decision_cutoffs: tuple[int, int] | None = None,
) -> MatchingSensitivityV1:
    """Run a supplied finite tolerance grid without fitting/selecting a policy.

    All points share one frozen clock. The complete declared grid and metrics
    are retained, including failed correspondence, ambiguity and source-only
    support. There is no winner, early stopping, or search for passing seeds.
    """
    if (
        type(tolerances_ns) is not tuple
        or not 1 <= len(tolerances_ns) <= MAX_SENSITIVITY_POINTS
    ):
        raise ValueError("bounded immutable sensitivity grid required")
    if any(type(value) is not RationalV1 for value in tolerances_ns):
        raise ValueError("exact rational sensitivity tolerance required")
    tolerances = tuple(
        RationalV1.from_payload(value.to_payload()) for value in tolerances_ns
    )
    values = tuple(value.value for value in tolerances)
    if values != tuple(sorted(set(values))) or min(values) < 0:
        raise ValueError("nonnegative sorted unique sensitivity grid required")
    if type(left) is not NativeCaptureV1 or type(right) is not NativeCaptureV1:
        raise ValueError("exact native sensitivity projections required")
    if type(left.quotes) is not tuple or type(right.quotes) is not tuple:
        raise ValueError("immutable native sensitivity quotes required")
    n_left, n_right = len(left.quotes), len(right.quotes)
    _preflight_size(n_left, n_right)
    work = _work_bound(n_left, n_right, n_left * n_right)
    if work * len(values) > MAX_SOLVE_WORK:
        raise ValueError("aggregate sensitivity solve work bound")
    if type(policy) is not MatchingPolicyV1 or type(model) is not ClockModelV1:
        raise ValueError("exact sensitivity policy and clock required")
    policy = MatchingPolicyV1.from_dict(policy.to_dict())
    model = ClockModelV1.from_dict(model.to_dict())
    truth = _truth(truth, expected_truth_id)
    points = []
    for tolerance in tolerances:
        report = match_cross_feed(
            left,
            right,
            model,
            replace(policy, time_tolerance_ns=tolerance),
            decision_cutoffs=decision_cutoffs,
        )
        score = _evaluate_replayed(report, truth)
        points.append(
            MatchingSensitivityPointV1(tolerance, report.artifact_id, score)
        )
    return MatchingSensitivityV1(
        left.root_id,
        right.root_id,
        model.artifact_id,
        policy.artifact_id,
        truth.artifact_id,
        decision_cutoffs,
        tuple(points),
    )
