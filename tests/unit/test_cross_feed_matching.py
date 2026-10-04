"""Finite invented graphs; no capture, provider data or qualification claims."""

from __future__ import annotations

from dataclasses import replace
from fractions import Fraction
from itertools import permutations

import pytest

from histdatacom.cross_feed import matching
from histdatacom.cross_feed._wire import RationalV1
from histdatacom.cross_feed.capture_contracts import (
    NativeCaptureV1,
    NativeQuoteV1,
)
from histdatacom.cross_feed.clock import fit_clock
from histdatacom.cross_feed.clock_contracts import (
    ClockCandidatePairV1,
    ClockFitPolicyV1,
    ClockFitRequestV1,
)
from histdatacom.cross_feed.evaluation import (
    evaluate_matching,
    evaluate_matching_sensitivity,
)
from histdatacom.cross_feed.matching_contracts import (
    CandidateEdgeV1,
    KnownCorrespondenceV1,
    MatchedPairV1,
    MatchingClockV1,
    MatchingEvaluationV1,
    MatchingPolicyV1,
    MatchingReportV1,
    MatchingTruthV1,
    RejectedPairV1,
    SourceOnlyV1,
)


def r(value: int | Fraction) -> RationalV1:
    return RationalV1.from_fraction(value)


def policy(**changes: object) -> MatchingPolicyV1:
    result = MatchingPolicyV1(
        r(10),
        r(1),
        r(1),
        r(1),
        r(1),
        r(1),
        (r(1), r(0), r(0), r(0), r(0)),
        r(0),
        r(100),
    )
    return replace(result, **changes)


def report() -> MatchingReportV1:
    edge = CandidateEdgeV1("a", "b", r(0), r(0), r(0), r(0), None, r(2), r(0))
    return MatchingReportV1(
        "left-root",
        "right-root",
        "left-projection",
        "right-projection",
        "frozen-clock",
        "retrospective",
        None,
        policy(),
        ("a",),
        ("b",),
        (
            MatchingClockV1("left", "a", "usable", r(1), r(1), "epoch", ()),
            MatchingClockV1("right", "b", "usable", r(1), r(1), "epoch", ()),
        ),
        (edge,),
        (),
        (MatchedPairV1("a", "b", "confident", r(0), r(0), None, None, r(2)),),
        (),
        r(0),
        100,
    )


def test_residual_augmentation_beats_nearest_neighbor() -> None:
    chosen, cost = matching._solve(
        ("a", "b"),
        ("x", "y"),
        (
            ("a", "x", Fraction(0)),
            ("a", "y", Fraction(1)),
            ("b", "x", Fraction(0)),
        ),
    )
    assert chosen == (("a", "y"), ("b", "x"))
    assert cost == 1


def test_global_alternative_is_not_local_second_best() -> None:
    edges = (
        ("a", "x", Fraction(0)),
        ("a", "y", Fraction(1)),
        ("b", "x", Fraction(1)),
        ("b", "y", Fraction(100)),
    )
    chosen, best = matching._solve(("a", "b"), ("x", "y"), edges)
    assert best == 2
    assert chosen == (("a", "y"), ("b", "x"))
    for key in chosen:
        alternate, total = matching._solve(("a", "b"), ("x", "y"), edges, key)
        assert len(alternate) == 2
        assert total - best == 98


def test_cardinality_precedes_any_finite_price_cost() -> None:
    chosen, cost = matching._solve(
        ("a", "b"),
        ("x", "y"),
        (
            ("a", "x", Fraction(0)),
            ("a", "y", Fraction(10**100)),
            ("b", "x", Fraction(0)),
        ),
    )
    assert len(chosen) == 2 and cost == 10**100


def test_solver_is_invariant_to_vertex_and_edge_input_order() -> None:
    edges = tuple((a, b, Fraction(0)) for a in ("a", "b") for b in ("x", "y"))
    expected = matching._solve(("a", "b"), ("x", "y"), edges)
    for edge_order in permutations(edges):
        assert matching._solve(("b", "a"), ("y", "x"), edge_order) == expected
    assert len(expected[0]) == 2
    for key in expected[0]:
        alternative, cost = matching._solve(("a", "b"), ("x", "y"), edges, key)
        assert len(alternative) == 2 and cost == 0


def test_no_same_cardinality_alternative_remains_absent() -> None:
    edges = (("a", "x", Fraction(1, 7)),)
    assert matching._solve(("a",), ("x",), edges) == (
        (("a", "x"),),
        Fraction(1, 7),
    )
    assert matching._solve(("a",), ("x",), edges, ("a", "x")) == ((), 0)


def test_exact_fraction_cost_order_is_not_float_order() -> None:
    tiny = Fraction(1, 10**100)
    edges = (("a", "x", Fraction(1)), ("a", "y", Fraction(1) - tiny))
    assert matching._solve(("a",), ("x", "y"), edges) == (
        (("a", "y"),),
        1 - tiny,
    )


@pytest.mark.parametrize(
    "left,right,edges",
    [
        (("a", "a"), ("x",), ()),
        (("a",), ("x",), (("a", "x", Fraction(-1)),)),
        (("a",), ("x",), (("foreign", "x", Fraction(1)),)),
        (("a",), ("x",), (("a", "x", 1.0),)),
        (("a",), ("x",), (("a", "x", Fraction(1)), ("a", "x", Fraction(1)))),
    ],
)
def test_solver_refuses_invalid_graph(left, right, edges) -> None:
    with pytest.raises(ValueError):
        matching._solve(left, right, edges)


def test_empty_graph_has_exact_empty_assignment() -> None:
    assert matching._solve((), ("x",), ()) == ((), 0)
    assert matching._solve(("a",), ("x",), ()) == ((), 0)


def test_bounded_arithmetic_refuses_unbounded_denominator_growth() -> None:
    with pytest.raises(ValueError, match="arithmetic bound"):
        matching._bounded(Fraction(1, 2**4097))


def test_dense_output_and_alternative_work_have_prospective_caps() -> None:
    with pytest.raises(ValueError, match="output reservation"):
        matching._preflight_size(64, 64)
    with pytest.raises(ValueError, match="work bound"):
        matching._work_bound(64, 64, 4096)
    matching._preflight_size(16, 16)
    assert matching._work_bound(16, 16, 256) < 8_000_000


@pytest.mark.parametrize(
    "changes",
    [
        {"time_tolerance_ns": r(-1)},
        {"time_scale_ns": r(0)},
        {"weights": (r(1),)},
        {"weights": (r(0),) * 5},
        {"weights": (r(-1), r(1), r(0), r(0), r(0))},
        {"ambiguity_margin": r(-1)},
        {"maximum_uncertainty_ns": r(-1)},
        {"algorithm": "nearest_neighbor"},
        {"transition_basis": "sorted_source_time"},
        {"maximum_bid_difference": r(-1)},
        {"maximum_ask_difference": r(-1)},
        {"maximum_spread_difference": r(-1)},
    ],
)
def test_frozen_policy_refuses_invalid_values(changes) -> None:
    with pytest.raises(ValueError):
        policy(**changes)


def test_policy_and_report_round_trip_with_versioned_identity() -> None:
    value = report()
    assert MatchingPolicyV1.from_json(value.policy.to_json()) == value.policy
    assert MatchingReportV1.from_json(value.to_json()) == value
    assert value.artifact_id.startswith("cross-feed-matching-report:sha256:")


@pytest.mark.parametrize(
    "changes",
    [
        {"total_cost": r(1)},
        {"left_event_ids": ("a", "missing")},
        {"right_event_ids": ("b", "b")},
        {
            "source_only": (
                SourceOnlyV1("left", "a", "no_compatible_candidate"),
            )
        },
        {"candidates": ()},
        {"rejected_pairs": (RejectedPairV1("a", "b", "time_gate"),)},
        {"interpretation": "measured_latency"},
        {"decision_cutoffs": (1,)},
    ],
)
def test_report_refuses_denominator_or_claim_forgery(changes) -> None:
    with pytest.raises(ValueError):
        replace(report(), **changes)


def test_report_derives_ambiguity_and_uncertainty_from_policy() -> None:
    base = report()
    for margin, uncertainty, expected in (
        (0, 2, "ambiguous"),
        (1, 0, "uncertain"),
        (0, 0, "ambiguous_uncertain"),
    ):
        changed_policy = replace(
            base.policy, maximum_uncertainty_ns=r(uncertainty)
        )
        item = replace(
            base.matches[0],
            alternate_total_cost=r(margin),
            global_margin=r(margin),
            status=expected,
        )
        assert (
            replace(base, policy=changed_policy, matches=(item,))
            .matches[0]
            .status
            == expected
        )
        with pytest.raises(ValueError, match="status"):
            replace(
                base,
                policy=changed_policy,
                matches=(replace(item, status="confident"),),
            )


def test_alternative_margin_requires_exact_global_total() -> None:
    with pytest.raises(ValueError, match="margin"):
        replace(
            report().matches[0], alternate_total_cost=r(3), global_margin=r(2)
        )


def test_unknown_clock_has_no_fabricated_zero_projection() -> None:
    with pytest.raises(ValueError):
        MatchingClockV1(
            "left", "a", "unavailable", r(0), r(0), None, ("missing",)
        )


def test_truth_requires_full_unique_one_to_one_correspondences() -> None:
    truth = MatchingTruthV1(
        "invented-fixture",
        "left-root",
        "right-root",
        ("a",),
        ("b",),
        (KnownCorrespondenceV1("a", "b"),),
    )
    assert MatchingTruthV1.from_json(truth.to_json()) == truth
    with pytest.raises(ValueError):
        replace(truth, correspondences=(KnownCorrespondenceV1("a", "foreign"),))
    with pytest.raises(ValueError):
        replace(truth, scope="empirical_latent_truth")


def test_evaluation_empty_denominators_are_none_not_success() -> None:
    value = MatchingEvaluationV1(
        "report", "truth", 0, 0, 0, None, None, None, None, (), (), ()
    )
    assert MatchingEvaluationV1.from_json(value.to_json()) == value
    with pytest.raises(ValueError):
        replace(value, precision=r(1))


def test_malformed_expected_root_refuses_before_report_readmission(
    monkeypatch,
) -> None:
    def poison(*args, **kwargs):
        raise AssertionError("report must not be traversed")

    monkeypatch.setattr(matching, "fields", poison)
    with pytest.raises(ValueError, match="expected matching"):
        matching.replay_matching_report(
            None, None, None, None, expected_report_id="wrong"
        )


def native_quote(side: str, index: int) -> NativeQuoteV1:
    time = (index + 1) * 1000 - (100 if side == "right" else 0)
    bid = Fraction(1) + Fraction(index, 1000)
    return NativeQuoteV1(
        capture_id=side + "-root",
        event_id=f"{side}-{index}",
        record_id=f"{side}-record-{index}",
        sequence=index,
        epoch_id="epoch",
        symbol="EURUSD",
        source_time_ns=time,
        source_precision_ns=1,
        source_time_semantics="broker_event",
        receive_utc_ns=time + 5000,
        receive_monotonic_ns=100 + index * 100,
        clock_id="host-" + side,
        bid=r(bid),
        ask=r(bid + Fraction(1, 10000)),
        price_basis="bid_ask_quote",
        source_sequence=str(index),
        source_message_id=None,
        source_batch_id=None,
        health_state="qualified_host_boundary_only",
        health_reasons=(),
    )


def captures(count: int = 4) -> tuple[NativeCaptureV1, NativeCaptureV1]:
    values = []
    for side in ("left", "right"):
        values.append(
            NativeCaptureV1(
                side + "-root",
                side + "-manifest",
                side + "-health",
                "qualified_host_boundary_only",
                (),
                tuple(native_quote(side, index) for index in range(count)),
                "0" * 64,
                0,
            )
        )
    return values[0], values[1]


def fitted(
    left: NativeCaptureV1, right: NativeCaptureV1, mode: str = "retrospective"
):
    request = ClockFitRequestV1(
        tuple(
            ClockCandidatePairV1(f"left-{i}", f"right-{i}") for i in range(3)
        ),
        2,
        2,
        ClockFitPolicyV1(maximum_prediction_horizon_ns=10_000),
        mode,
    )
    return fit_clock(left, right, request)


def truth_for(left: NativeCaptureV1, right: NativeCaptureV1) -> MatchingTruthV1:
    return MatchingTruthV1(
        "fixed-invented-four-point-shift",
        left.root_id,
        right.root_id,
        tuple(sorted(quote.event_id for quote in left.quotes)),
        tuple(sorted(quote.event_id for quote in right.quotes)),
        tuple(
            KnownCorrespondenceV1(f"left-{i}", f"right-{i}")
            for i in range(min(len(left.quotes), len(right.quotes)))
        ),
    )


def test_actual_frozen_clock_is_consumed_without_refitting(monkeypatch) -> None:
    left, right = captures()
    model = fitted(left, right)

    def forbidden(*args, **kwargs):
        raise AssertionError("matcher must not refit a clock")

    from histdatacom.cross_feed import clock

    monkeypatch.setattr(clock, "fit_clock", forbidden)
    actual = matching.match_cross_feed(left, right, model, policy())
    assert len(actual.matches) == 4
    assert not actual.source_only
    assert all(item.status == "confident" for item in actual.matches)
    assert all(edge.time_residual_ns.value == 0 for edge in actual.candidates)
    assert actual.model_id == model.artifact_id
    assert (
        matching.replay_matching_report(
            left, right, model, actual, expected_report_id=actual.artifact_id
        )
        == actual
    )


def test_actual_kernel_rejects_stale_projection_bound_to_same_capture_name() -> (
    None
):
    left, right = captures()
    model = fitted(left, right)
    changed = replace(
        right,
        quotes=right.quotes[:-1]
        + (replace(right.quotes[-1], bid=r(Fraction(1, 2))),),
    )
    with pytest.raises(ValueError, match="root mismatch"):
        matching.match_cross_feed(left, changed, model, policy())


def test_actual_kernel_retains_missing_clock_and_symbol_support() -> None:
    left, right = captures(5)
    right = replace(
        right,
        quotes=right.quotes[:3]
        + (
            replace(
                right.quotes[3], source_time_ns=None, source_precision_ns=None
            ),
            replace(right.quotes[4], symbol="GBPUSD"),
        ),
    )
    actual = matching.match_cross_feed(
        left, right, fitted(left, right), policy()
    )
    assert len(actual.matches) == 3
    assert len(actual.candidates) + len(actual.rejected_pairs) == 25
    reasons = {
        (item.side, item.event_id): item.reason for item in actual.source_only
    }
    assert reasons["right", "right-3"] == "clock_unavailable"
    assert reasons["right", "right-4"] == "no_compatible_candidate"
    assert {item.reason for item in actual.rejected_pairs} >= {
        "clock_unavailable",
        "symbol_mismatch",
    }


def test_actual_clock_uncertainty_is_not_silently_confident() -> None:
    left, right = captures()
    actual = matching.match_cross_feed(
        left, right, fitted(left, right), policy(maximum_uncertainty_ns=r(0))
    )
    assert len(actual.matches) == 4
    assert all(item.status == "uncertain" for item in actual.matches)
    assert all(item.uncertainty_radius_ns.value > 0 for item in actual.matches)


def test_transition_weight_requires_real_prior_support() -> None:
    left, right = captures()
    actual = matching.match_cross_feed(
        left,
        right,
        fitted(left, right),
        policy(weights=(r(1), r(0), r(0), r(0), r(1))),
    )
    assert len(actual.matches) == 3
    assert {item.event_id for item in actual.source_only} == {
        "left-0",
        "right-0",
    }
    assert any(
        item.reason == "transition_support_unavailable"
        for item in actual.rejected_pairs
    )


def test_actual_host_sequence_cutoff_prevents_future_matching() -> None:
    left, right = captures(5)
    model = fitted(left, right, "frozen_ex_ante")
    actual = matching.match_cross_feed(
        left, right, model, policy(), decision_cutoffs=(3, 3)
    )
    assert len(actual.matches) == 1
    assert actual.matches[0].key == ("left-3", "right-3")
    assert {item.event_id for item in actual.source_only} == {
        f"{side}-{index}"
        for side in ("left", "right")
        for index in (0, 1, 2, 4)
    }
    assert all(
        item.reason == "clock_unavailable" for item in actual.source_only
    )


def test_actual_replay_rejects_resealed_assignment_claims() -> None:
    left, right = captures()
    model = fitted(left, right)
    actual = matching.match_cross_feed(left, right, model, policy())
    forged = replace(
        actual,
        matches=tuple(
            replace(
                item,
                status="ambiguous",
                alternate_total_cost=item.best_total_cost,
                global_margin=r(0),
            )
            for item in actual.matches
        ),
    )
    with pytest.raises(ValueError, match="expected matching root"):
        matching.replay_matching_report(
            left, right, model, forged, expected_report_id=actual.artifact_id
        )
    with pytest.raises(ValueError, match="semantic replay"):
        matching.replay_matching_report(
            left, right, model, forged, expected_report_id=forged.artifact_id
        )


def test_actual_evaluation_uses_independent_truth_and_exact_metrics() -> None:
    left, right = captures()
    model = fitted(left, right)
    actual = matching.match_cross_feed(left, right, model, policy())
    truth = truth_for(left, right)
    score = evaluate_matching(
        left,
        right,
        model,
        actual,
        truth,
        expected_report_id=actual.artifact_id,
        expected_truth_id=truth.artifact_id,
    )
    assert score.true_positive == 4
    assert score.false_positive == score.false_negative == 0
    assert score.precision == score.recall == r(1)
    assert score.ambiguity_rate == score.unmatched_rate == r(0)
    assert score.time_residuals_ns == (r(0),) * 4
    assert MatchingEvaluationV1.from_json(score.to_json()) == score
    with pytest.raises(ValueError, match="truth root"):
        evaluate_matching(
            left,
            right,
            model,
            actual,
            truth,
            expected_report_id=actual.artifact_id,
            expected_truth_id="different",
        )
    foreign = replace(
        truth, left_event_ids=truth.left_event_ids + ("z-unknown",)
    )
    with pytest.raises(ValueError, match="complete event denominator"):
        evaluate_matching(
            left,
            right,
            model,
            actual,
            foreign,
            expected_report_id=actual.artifact_id,
            expected_truth_id=foreign.artifact_id,
        )


def test_actual_sensitivity_retains_entire_fixed_grid_and_failures() -> None:
    left, right = captures()
    right = replace(
        right,
        quotes=right.quotes[:-1]
        + (replace(right.quotes[-1], source_time_ns=3920),),
    )
    model = fitted(left, right)
    truth = truth_for(left, right)
    result = evaluate_matching_sensitivity(
        left,
        right,
        model,
        policy(),
        truth,
        (r(0), r(10), r(100)),
        expected_truth_id=truth.artifact_id,
    )
    assert tuple(point.evaluation.true_positive for point in result.points) == (
        3,
        3,
        4,
    )
    assert tuple(
        point.evaluation.false_negative for point in result.points
    ) == (1, 1, 0)
    assert result.model_id == model.artifact_id
    assert result.points[0].evaluation.recall == r(Fraction(3, 4))
    assert len({point.matching_report_id for point in result.points}) == 3


def test_oversized_block_refuses_before_clock_callback(monkeypatch) -> None:
    left, right = captures(64)

    def forbidden(*args, **kwargs):
        raise AssertionError("no clock work before block admission")

    monkeypatch.setattr(matching, "correct_clock_quote", forbidden)
    with pytest.raises(ValueError, match="output reservation"):
        matching.match_cross_feed(left, right, None, policy())


@pytest.mark.parametrize(
    "gate",
    [
        "maximum_bid_difference",
        "maximum_ask_difference",
        "maximum_spread_difference",
    ],
)
def test_actual_frozen_price_gate_keeps_equal_time_quotes_source_only(
    gate,
) -> None:
    left, right = captures()
    right = replace(
        right,
        quotes=right.quotes[:-1]
        + (replace(right.quotes[-1], bid=r(10), ask=r(11)),),
    )
    model = fitted(left, right)
    selected_policy = policy(**{gate: r(Fraction(1, 10))})
    actual = matching.match_cross_feed(left, right, model, selected_policy)
    assert len(actual.matches) == 3
    assert {item.event_id for item in actual.source_only} == {
        "left-3",
        "right-3",
    }
    assert all(
        item.reason == "no_compatible_candidate" for item in actual.source_only
    )
    assert (
        next(
            item
            for item in actual.rejected_pairs
            if item.key == ("left-3", "right-3")
        ).reason
        == "price_gate"
    )
    # Explicit None means no price gate, not an implicit identity guarantee.
    unbounded = matching.match_cross_feed(left, right, model, policy())
    assert len(unbounded.matches) == 4


def test_aggregate_health_text_refuses_before_clock_work(monkeypatch) -> None:
    left, right = captures()
    right = replace(
        right,
        quotes=right.quotes[:-1]
        + (replace(right.quotes[-1], health_reasons=("a" * 150, "b" * 150)),),
    )
    model = fitted(left, right)

    def forbidden(*args, **kwargs):
        raise AssertionError("clock work must follow aggregate admission")

    monkeypatch.setattr(matching, "correct_clock_quote", forbidden)
    with pytest.raises(ValueError, match="health reason output bound"):
        matching.match_cross_feed(left, right, model, policy())
