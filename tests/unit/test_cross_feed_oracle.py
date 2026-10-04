"""Independent exhaustive accounting for small conditional match graphs.

The oracle enumerates assignments directly. It does not share the production
flow algorithm, potentials, reduced-cost arithmetic, or ambiguity helpers.
Pure graph checks are not native capture/rights evidence; the companion native
tests exercise the real public intake and clock-to-matching consumer.
"""

from fractions import Fraction
from itertools import product
from dataclasses import replace

import pytest


def exhaustive_assignments(left, right, edges):
    costs = {(a, b): cost for a, b, cost in edges}
    assignments = []
    for choices in product((None, *right), repeat=len(left)):
        used = tuple(value for value in choices if value is not None)
        if len(used) != len(set(used)):
            continue
        pairs = tuple(
            sorted((a, b) for a, b in zip(left, choices) if b is not None)
        )
        if any(pair not in costs for pair in pairs):
            continue
        assignments.append((pairs, sum((costs[p] for p in pairs), Fraction(0))))
    return tuple(assignments)


def optimum(assignments, *, forbidden=None, cardinality=None):
    allowed = [
        (pairs, cost)
        for pairs, cost in assignments
        if (forbidden is None or forbidden not in pairs)
        and (cardinality is None or len(pairs) == cardinality)
    ]
    if not allowed:
        return ()
    objective = min((-len(pairs), cost) for pairs, cost in allowed)
    return tuple(
        sorted(
            (pairs, cost)
            for pairs, cost in allowed
            if (-len(pairs), cost) == objective
        )
    )


@pytest.mark.parametrize(
    "cost_law", ["zero", "integer", "rational", "asymmetric"]
)
def test_every_two_by_three_graph_matches_exhaustive_global_oracle(cost_law):
    from histdatacom.cross_feed.matching import _solve

    left, right = ("a", "b"), ("x", "y", "z")
    universe = tuple(product(left, right))
    for mask in range(1 << len(universe)):
        costs = {
            "zero": lambda n: Fraction(0),
            "integer": lambda n: Fraction(n % 3),
            "rational": lambda n: Fraction(n + 1, (n % 3) + 2),
            "asymmetric": lambda n: Fraction((n * 7 + 3) % 11, 5),
        }
        edges = tuple(
            (a, b, costs[cost_law](n))
            for n, (a, b) in enumerate(universe)
            if mask & (1 << n)
        )
        truth = exhaustive_assignments(left, right, edges)
        selected, cost = _solve(left, right, edges)
        assert (selected, cost) in optimum(truth), (cost_law, mask)
        assert len({a for a, _ in selected}) == len(selected)
        assert len({b for _, b in selected}) == len(selected)
        # This checks alternate GLOBAL feasibility, including reassignment of
        # other selected edges. A local per-event runner-up is insufficient.
        for forbidden in selected:
            alternate, alternate_cost = _solve(left, right, edges, forbidden)
            assert (alternate, alternate_cost) in optimum(
                truth, forbidden=forbidden
            )
            same_cardinality = optimum(
                truth, forbidden=forbidden, cardinality=len(selected)
            )
            if same_cardinality:
                assert len(alternate) == len(selected)
                assert alternate_cost - cost >= 0
            else:
                assert len(alternate) < len(selected)
        assert _solve(
            tuple(reversed(left)),
            tuple(reversed(right)),
            tuple(reversed(edges)),
        ) == (selected, cost)


@pytest.mark.parametrize(
    "costs",
    [
        (("a", "x", 0), ("a", "y", 1), ("b", "x", 1)),
        (("a", "x", 0), ("a", "y", 1), ("b", "x", 1), ("b", "y", 100)),
        (("a", "x", 1), ("a", "y", 1), ("b", "x", 1), ("b", "y", 1)),
        (
            ("a", "x", 0),
            ("a", "y", 1),
            ("b", "y", 0),
            ("b", "z", 1),
            ("c", "x", 0),
        ),
    ],
    ids=(
        "greedy_loses_cardinality",
        "greedy_loses_cost",
        "equal_cost_ambiguity",
        "burst_reassignment_chain",
    ),
)
def test_adversarial_bursts_require_global_reassignment(costs):
    from histdatacom.cross_feed.matching import _solve

    left = tuple(sorted({a for a, _, _ in costs}))
    right = tuple(sorted({b for _, b, _ in costs}))
    edges = tuple((a, b, Fraction(cost)) for a, b, cost in costs)
    actual = _solve(left, right, edges)
    assert actual in optimum(exhaustive_assignments(left, right, edges))
    if len(costs) == 3:
        assert actual == ((("a", "y"), ("b", "x")), Fraction(2))
    if all(cost == 1 for _, _, cost in costs):
        for edge in actual[0]:
            alternative = _solve(left, right, edges, edge)
            assert alternative[1] == actual[1]
            assert alternative[0] != actual[0]


def test_fractional_objective_is_independent_of_decimal_context():
    from decimal import localcontext
    from histdatacom.cross_feed.matching import _solve

    edges = (
        ("a", "x", Fraction(1, 3)),
        ("a", "y", Fraction(1, 7)),
        ("b", "x", Fraction(2, 7)),
        ("b", "y", Fraction(1, 3)),
    )
    expected = ((("a", "y"), ("b", "x")), Fraction(3, 7))
    for precision in (2, 7, 28):
        with localcontext() as context:
            context.prec = precision
            assert _solve(("a", "b"), ("x", "y"), edges) == expected


@pytest.mark.parametrize("bad_cost", [0, 0.0, True, Fraction(-1)])
def test_solver_never_silently_coerces_untyped_or_negative_cost(bad_cost):
    from histdatacom.cross_feed.matching import _solve

    with pytest.raises(ValueError, match="exact nonnegative costs"):
        _solve(("a",), ("x",), (("a", "x", bad_cost),))


def kernel_capture(name, times, *, prices=None, precision=1):
    """Construct mathematical vectors, NOT independently verified captures."""
    from histdatacom.cross_feed._wire import RationalV1
    from histdatacom.cross_feed.capture_contracts import (
        NativeCaptureV1,
        NativeQuoteV1,
    )

    r = RationalV1.from_fraction
    if prices is None:
        prices = tuple(Fraction(11000 + i, 10000) for i in range(len(times)))
    quotes = tuple(
        NativeQuoteV1(
            capture_id=name,
            event_id=f"{name}-{i:03d}",
            record_id=f"{name}-host-record-{i:03d}",
            sequence=i,
            epoch_id="epoch-1",
            symbol="EURUSD",
            source_time_ns=time,
            source_precision_ns=precision,
            source_time_semantics="broker_event",
            receive_utc_ns=10**15 + i * 10**9,
            receive_monotonic_ns=10**12 + i * 10**9,
            clock_id="invented-host-clock",
            bid=r(price),
            ask=r(price + Fraction(1, 10000)),
            price_basis="exact_decimal",
            source_sequence=str(i),
            source_message_id=f"fixture-message-{i}",
            source_batch_id=f"fixture-batch-{i // 2}",
            health_state="qualified_host_boundary_only",
            health_reasons=(),
        )
        for i, (time, price) in enumerate(zip(times, prices))
    )
    return NativeCaptureV1(
        name,
        f"{name}-manifest",
        f"{name}-health",
        "qualified_host_boundary_only",
        (),
        quotes,
        "a" * 64,
        0,
    )


def fit_kernel(left, right, *, policy=None, mode="retrospective", cutoff=None):
    from histdatacom.cross_feed.clock import fit_clock
    from histdatacom.cross_feed.clock_contracts import (
        ClockCandidatePairV1,
        ClockFitPolicyV1,
        ClockFitRequestV1,
    )

    count = min(len(left.quotes), len(right.quotes))
    if cutoff is None:
        cutoff = count - 1
    request = ClockFitRequestV1(
        tuple(
            ClockCandidatePairV1(a.event_id, b.event_id)
            for a, b in zip(left.quotes[:count], right.quotes[:count])
        ),
        cutoff,
        cutoff,
        policy or ClockFitPolicyV1(),
        mode,
    )
    return fit_clock(left, right, request)


@pytest.mark.parametrize(
    "drift", [Fraction(0), Fraction(1, 1000), Fraction(-1, 1000)]
)
def test_exact_affine_clock_known_correspondences(drift):
    from histdatacom.cross_feed.clock import correct_clock_quote

    right_times = tuple((i + 1) * 10**9 for i in range(5))
    left_times = tuple(
        int(t + 12_000_000 + drift * (t - right_times[0])) for t in right_times
    )
    left = kernel_capture("left", left_times)
    right = kernel_capture("right", right_times)
    model = fit_kernel(left, right)
    assert model.status == "ready"
    assert len(model.segments) == 1
    segment = model.segments[0]
    assert segment.drift.value == drift
    assert segment.offset_ns.value == 12_000_000 + drift * (
        segment.origin_time_ns - right_times[0]
    )
    for expected, quote in zip(left_times, right.quotes):
        correction = correct_clock_quote(model, quote)
        assert correction.status == "usable"
        assert correction.corrected_time_ns.value == expected
        assert correction.uncertainty_radius_ns.value >= 1


def test_independent_median_mad_outlier_hand_vector():
    from histdatacom.cross_feed.clock_contracts import ClockFitPolicyV1

    offsets = (12_000_000, 11_000_000, 13_000_000, 10_000_000, 200_000_000)
    right_times = tuple((i + 1) * 10**9 for i in range(5))
    left = kernel_capture(
        "left", tuple(t + d for t, d in zip(right_times, offsets))
    )
    right = kernel_capture("right", right_times)
    # This analytical outlier vector tests the reported raw robust statistic,
    # not a claim that its affine clock law meets an empirical drift threshold.
    model = fit_kernel(
        left, right, policy=ClockFitPolicyV1(maximum_drift_ppm=100_000)
    )
    assert len(model.segments) == 1
    segment = model.segments[0]
    ordered = sorted(offsets)
    independent_median = ordered[len(ordered) // 2]
    deviations = sorted(abs(value - independent_median) for value in offsets)
    independent_mad = deviations[len(deviations) // 2]
    assert (
        segment.median_difference_ns.value == independent_median == 12_000_000
    )
    assert segment.difference_mad_ns.value == independent_mad == 1_000_000
    assert Fraction(7413, 5000) * independent_mad == 1_482_600
    assert segment.scaled_difference_mad_ns.value == 1_482_600


def test_actual_matching_report_global_margins_equal_independent_assignment_oracle():
    from histdatacom.cross_feed._wire import RationalV1
    from histdatacom.cross_feed.matching import match_cross_feed
    from histdatacom.cross_feed.matching_contracts import MatchingPolicyV1

    r = RationalV1.from_fraction
    # Calibration has distinct timestamps, then the declared matching gate
    # deliberately admits a full ambiguous burst. Price/time weights are zero
    # except spread, which is identical. Every complete assignment costs zero.
    right_times = (1_000_000_000, 2_000_000_000, 3_000_000_000)
    left = kernel_capture("left", tuple(t + 12_000_000 for t in right_times))
    right = kernel_capture("right", right_times)
    model = fit_kernel(left, right)
    policy = MatchingPolicyV1(
        r(3_000_000_000),
        r(1),
        r(1),
        r(1),
        r(1),
        r(1),
        (r(0), r(0), r(0), r(1), r(0)),
        r(0),
        r(100),
    )
    report = match_cross_feed(left, right, model, policy)
    edges = tuple((*edge.key, edge.cost.value) for edge in report.candidates)
    all_assignments = exhaustive_assignments(
        report.left_event_ids, report.right_event_ids, edges
    )
    selected = tuple(pair.key for pair in report.matches)
    assert (selected, report.total_cost.value) in optimum(all_assignments)
    assert len(report.matches) == 3
    for pair in report.matches:
        alternate = optimum(all_assignments, forbidden=pair.key, cardinality=3)
        assert alternate
        assert pair.alternate_total_cost.value == alternate[0][1]
        assert pair.global_margin.value == 0
        assert pair.status == "ambiguous"
    changed = replace(policy, time_tolerance_ns=r(0))
    unique = match_cross_feed(left, right, model, changed)
    assert all(
        pair.status == "confident" and pair.alternate_total_cost is None
        for pair in unique.matches
    )
    from concurrent.futures import ThreadPoolExecutor

    # No worker-count input enters scientific policy. Scheduling independent
    # immutable calls in opposite order must preserve the exact result bytes.
    policies = (policy, changed, changed, policy)
    expected = (
        report.to_json(),
        unique.to_json(),
        unique.to_json(),
        report.to_json(),
    )
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = tuple(
            executor.submit(match_cross_feed, left, right, model, p)
            for p in reversed(policies)
        )
        actual = tuple(
            future.result().to_json() for future in reversed(futures)
        )
    assert actual == expected


def test_batched_reordered_arrivals_keep_held_out_truth_and_source_only():
    """Invented typed vectors, not independently native-admitted evidence."""
    from histdatacom.cross_feed._wire import RationalV1
    from histdatacom.cross_feed.clock import fit_clock
    from histdatacom.cross_feed.clock_contracts import (
        ClockCandidatePairV1,
        ClockFitPolicyV1,
        ClockFitRequestV1,
    )
    from histdatacom.cross_feed.evaluation import evaluate_matching
    from histdatacom.cross_feed.matching import match_cross_feed
    from histdatacom.cross_feed.matching_contracts import (
        KnownCorrespondenceV1,
        MatchingPolicyV1,
        MatchingTruthV1,
    )

    r = RationalV1.from_fraction
    second = 1_000_000_000
    left = kernel_capture(
        "left",
        tuple(n * second + 12_000_000 for n in range(1, 7)),
        precision=1000,
    )
    # The last two arrivals reverse source-time order and share actual host
    # receive coordinates. Prices follow the independently declared latent
    # events, not their arrival positions. The final left event has no peer.
    right = kernel_capture(
        "right",
        tuple(n * second for n in (1, 2, 3, 5, 4)),
        prices=tuple(
            Fraction(11000 + index, 10000) for index in (0, 1, 2, 4, 3)
        ),
        precision=1000,
    )
    right = replace(
        right,
        health_state="degraded",
        health_reasons=("source_timestamp_reordering",),
        quotes=right.quotes[:3]
        + tuple(
            replace(
                quote,
                receive_utc_ns=10**15 + 5 * second,
                receive_monotonic_ns=10**12 + 5 * second,
                source_batch_id="held-out-batch",
                health_state=(
                    "degraded" if quote.sequence == 4 else quote.health_state
                ),
                health_reasons=(
                    ("source_timestamp_reordering",)
                    if quote.sequence == 4
                    else ()
                ),
            )
            for quote in right.quotes[3:]
        ),
    )
    assert tuple(quote.sequence for quote in right.quotes) == (0, 1, 2, 3, 4)
    assert right.quotes[3].source_time_ns > right.quotes[4].source_time_ns
    assert right.quotes[3].receive_utc_ns == right.quotes[4].receive_utc_ns
    assert (
        right.quotes[3].receive_monotonic_ns
        == right.quotes[4].receive_monotonic_ns
    )
    request = ClockFitRequestV1(
        tuple(
            ClockCandidatePairV1(f"left-{i:03d}", f"right-{i:03d}")
            for i in range(3)
        ),
        2,
        2,
        ClockFitPolicyV1(
            maximum_prediction_horizon_ns=4 * second,
            allowed_health_reasons=(
                "source_timestamp_reordering",
                "upstream_loss_unidentified",
            ),
        ),
    )
    # All expectations are fixed before the fit. The two reversed held-out
    # pairs are deliberately absent from the calibration request.
    expected_pairs = (
        ("left-000", "right-000"),
        ("left-001", "right-001"),
        ("left-002", "right-002"),
        ("left-003", "right-004"),
        ("left-004", "right-003"),
    )
    truth = MatchingTruthV1(
        "independent-batched-reordered-arrivals",
        left.root_id,
        right.root_id,
        tuple(quote.event_id for quote in left.quotes),
        tuple(quote.event_id for quote in right.quotes),
        tuple(KnownCorrespondenceV1(*pair) for pair in expected_pairs),
    )
    model = fit_clock(left, right, request)
    assert model.status == "ready"
    assert model.segments[0].offset_ns.value == 12_000_000
    assert model.segments[0].drift.value == 0
    assert model.segments[0].pair_count == 3
    report = match_cross_feed(
        left,
        right,
        model,
        MatchingPolicyV1(
            r(0),
            r(second),
            r(1),
            r(1),
            r(1),
            r(1),
            (r(1), r(1), r(1), r(1), r(0)),
            r(0),
            r(100_000),
        ),
    )
    assert tuple(pair.key for pair in report.matches) == expected_pairs
    assert tuple(pair.key for pair in report.matches[3:]) == (
        ("left-003", "right-004"),
        ("left-004", "right-003"),
    )
    assert tuple(
        (item.side, item.event_id, item.reason) for item in report.source_only
    ) == (
        ("left", "left-005", "no_compatible_candidate"),
    )
    assert len(report.candidates) == 5
    assert len(report.rejected_pairs) == 25
    assert {item.reason for item in report.rejected_pairs} == {"time_gate"}
    assert report.total_cost.value == 0
    assert all(
        pair.status == "confident" and pair.alternate_total_cost is None
        for pair in report.matches
    )
    assert tuple(
        pair.uncertainty_radius_ns.value for pair in report.matches
    ) == (
        4000,
        6000,
        8000,
        10000,
        12000,
    )
    score = evaluate_matching(
        left,
        right,
        model,
        report,
        truth,
        expected_report_id=report.artifact_id,
        expected_truth_id=truth.artifact_id,
    )
    # These aggregate rates include the full fixture denominator; the exact
    # two held-out correspondences are separately asserted above.
    assert (
        score.true_positive,
        score.false_positive,
        score.false_negative,
    ) == (
        5,
        0,
        0,
    )
    assert score.precision.value == score.recall.value == 1
    assert score.ambiguity_rate.value == 0
    assert score.unmatched_rate.value == Fraction(1, 11)
    assert (
        score.time_residuals_ns
        == score.bid_residuals
        == score.ask_residuals
        == (r(0),) * 5
    )
