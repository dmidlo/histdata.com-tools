"""Bounded deterministic matching of already-admitted native projections.

This kernel does not acquire sources, grant rights, or fit a clock. The public
native API is responsible for fresh capture/model replay before invoking it.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, fields
from fractions import Fraction

from ._wire import RationalV1
from .capture_contracts import NativeCaptureV1, NativeQuoteV1
from .clock import correct_clock_quote
from .clock_contracts import ClockModelV1
from .matching_contracts import (
    MAX_ARITHMETIC_BITS,
    MAX_CANDIDATE_PAIRS,
    MAX_SIDE_EVENTS,
    MAX_SOLVE_WORK,
    CandidateEdgeV1,
    MatchedPairV1,
    MatchingClockV1,
    MatchingPolicyV1,
    MatchingReportV1,
    RejectedPairV1,
    SourceOnlyV1,
)


def _bounded(value: Fraction) -> Fraction:
    if (
        max(value.numerator.bit_length(), value.denominator.bit_length())
        > MAX_ARITHMETIC_BITS
    ):
        raise ValueError("matching intermediate rational arithmetic bound")
    return value


def _r(value: Fraction | int) -> RationalV1:
    return RationalV1.from_fraction(value)


def _preflight_size(left_count: int, right_count: int) -> None:
    if max(left_count, right_count) > MAX_SIDE_EVENTS:
        raise ValueError("matching side event bound")
    pairs = left_count * right_count
    if pairs > MAX_CANDIDATE_PAIRS:
        raise ValueError("matching candidate pair bound")
    # Conservative expanded-node/ASCII-byte reservation for every potential
    # edge (including nine bounded rationals), matches and full clock inventory.
    # Shared wire ceiling is 8 MiB / 131072 nodes. Refuse before corrections or
    # any solve, even when later compatibility filters could shrink the graph.
    nodes = 2048 + 160 * (
        pairs + left_count + right_count + min(left_count, right_count)
    )
    size = 16384 + 8192 * (
        pairs + left_count + right_count + min(left_count, right_count)
    )
    if nodes > 120_000 or size > 8 * 1024 * 1024:
        raise ValueError("matching prospective output reservation")


def _work_bound(left_count: int, right_count: int, edge_count: int) -> int:
    vertices = left_count + right_count + 2
    augmentations = min(left_count, right_count) + 1
    solves = min(left_count, right_count) + 1
    work = (
        solves
        * augmentations
        * (vertices * vertices + 2 * edge_count + left_count + right_count)
    )
    if work > MAX_SOLVE_WORK:
        raise ValueError("matching prospective alternative solve work bound")
    return work


@dataclass(slots=True)
class _Arc:
    destination: int
    reverse: int
    capacity: int
    cost: Fraction


def _solve(
    left_ids: tuple[str, ...],
    right_ids: tuple[str, ...],
    edges: tuple[tuple[str, str, Fraction], ...],
    forbidden: tuple[str, str] | None = None,
) -> tuple[tuple[tuple[str, str], ...], Fraction]:
    """Exact successive shortest augmenting paths with canonical tie order.

    The min-cost flow residual network, not greedy nearest-neighbor pairing,
    allows a later augmenting path to undo/reassign earlier selections.
    Dijkstra reduced costs are nonnegative under exact updated potentials.
    """
    left_ids, right_ids = tuple(sorted(left_ids)), tuple(sorted(right_ids))
    if len(set(left_ids)) != len(left_ids) or len(set(right_ids)) != len(
        right_ids
    ):
        raise ValueError("duplicate solver identity")
    _preflight_size(len(left_ids), len(right_ids))
    if len(edges) > MAX_CANDIDATE_PAIRS:
        raise ValueError("solver edge bound")
    ordered = tuple(sorted(edges, key=lambda item: (item[0], item[1])))
    if len({(a, b) for a, b, _ in ordered}) != len(ordered):
        raise ValueError("duplicate solver edge")
    if any(type(cost) is not Fraction or cost < 0 for _, _, cost in ordered):
        raise ValueError("solver requires exact nonnegative costs")
    left_map = {value: i + 1 for i, value in enumerate(left_ids)}
    right_map = {
        value: i + 1 + len(left_ids) for i, value in enumerate(right_ids)
    }
    target = len(left_ids) + len(right_ids) + 1
    graph: list[list[_Arc]] = [[] for _ in range(target + 1)]

    def connect(start: int, end: int, cost: Fraction) -> _Arc:
        forward = _Arc(end, len(graph[end]), 1, _bounded(cost))
        backward = _Arc(start, len(graph[start]), 0, -cost)
        graph[start].append(forward)
        graph[end].append(backward)
        return forward

    for node in left_map.values():
        connect(0, node, Fraction(0))
    for node in right_map.values():
        connect(node, target, Fraction(0))
    forward_edges: dict[tuple[str, str], tuple[_Arc, Fraction]] = {}
    for left, right, cost in ordered:
        if left not in left_map or right not in right_map:
            raise ValueError("solver edge outside event inventory")
        if (left, right) != forbidden:
            arc = connect(left_map[left], right_map[right], cost)
            forward_edges[left, right] = arc, cost
    potential = [Fraction(0) for _ in graph]
    while True:
        distance: list[Fraction | None] = [None for _ in graph]
        previous: list[tuple[int, int] | None] = [None for _ in graph]
        visited: set[int] = set()
        distance[0] = Fraction(0)
        for _ in graph:
            reachable = [
                (value, node)
                for node, value in enumerate(distance)
                if value is not None and node not in visited
            ]
            if not reachable:
                break
            current_distance, node = min(reachable)
            visited.add(node)
            for index, arc in enumerate(graph[node]):
                if not arc.capacity or arc.destination in visited:
                    continue
                reduced = _bounded(
                    arc.cost + potential[node] - potential[arc.destination]
                )
                if reduced < 0:
                    raise ValueError("negative exact residual reduced cost")
                candidate = _bounded(current_distance + reduced)
                old = distance[arc.destination]
                if old is None or candidate < old:
                    distance[arc.destination] = candidate
                    previous[arc.destination] = node, index
        if distance[target] is None:
            break
        for node, value in enumerate(distance):
            if value is not None:
                potential[node] = _bounded(potential[node] + value)
        node = target
        while node:
            predecessor = previous[node]
            if predecessor is None:
                raise ValueError("incomplete augmenting path")
            origin, index = predecessor
            arc = graph[origin][index]
            arc.capacity -= 1
            graph[node][arc.reverse].capacity += 1
            node = origin
    selected = tuple(
        key for key, (arc, _) in forward_edges.items() if not arc.capacity
    )
    cost = Fraction(0)
    for key in selected:
        cost = _bounded(cost + forward_edges[key][1])
    return selected, cost


def _capture(value: NativeCaptureV1) -> NativeCaptureV1:
    if type(value) is not NativeCaptureV1 or type(value.quotes) is not tuple:
        raise ValueError("exact immutable native capture required")
    if len(value.quotes) > MAX_SIDE_EVENTS:
        raise ValueError("matching side event bound")
    if any(type(quote) is not NativeQuoteV1 for quote in value.quotes):
        raise ValueError("exact native quote required")
    return NativeCaptureV1.from_dict(value.to_dict())


def _transitions(capture: NativeCaptureV1) -> dict[str, Fraction | None]:
    previous: dict[tuple[str, str, str], Fraction] = {}
    result: dict[str, Fraction | None] = {}
    for quote in capture.quotes:
        # Never join a transition across a connection epoch, symbol or basis.
        key = quote.epoch_id, quote.symbol, quote.price_basis
        mid = _bounded((quote.bid.value + quote.ask.value) / 2)
        result[quote.event_id] = (
            _bounded(mid - previous[key]) if key in previous else None
        )
        previous[key] = mid
    return result


def _clocks(
    capture: NativeCaptureV1,
    model: ClockModelV1,
    side: str,
    cutoffs: tuple[int, int] | None,
) -> tuple[MatchingClockV1, ...]:
    result = []
    for quote in sorted(capture.quotes, key=lambda quote: quote.event_id):
        correction = correct_clock_quote(model, quote, decision_cutoffs=cutoffs)
        if (
            correction.event_id != quote.event_id
            or correction.capture_id != capture.root_id
            or correction.model_id != model.artifact_id
        ):
            raise ValueError("clock projection changed event/model binding")
        if (
            len(correction.reasons) > 32
            or sum(len(reason) for reason in correction.reasons)
            + len(correction.segment_id or "")
            > 512
        ):
            raise ValueError("matching clock projection output bound")
        result.append(
            MatchingClockV1(
                side,
                quote.event_id,
                correction.status,
                correction.corrected_time_ns,
                correction.uncertainty_radius_ns,
                correction.segment_id,
                tuple(sorted(set(correction.reasons))),
            )
        )
    return tuple(result)


def match_cross_feed(
    left: NativeCaptureV1,
    right: NativeCaptureV1,
    model: ClockModelV1,
    policy: MatchingPolicyV1,
    *,
    decision_cutoffs: tuple[int, int] | None = None,
) -> MatchingReportV1:
    """Compute a conditional match report, not native-root authenticity.

    Public callers needing capture/rights/model evidence must use the native
    API, which replays those independent roots. This pure kernel freshly
    readmits its immutable inputs but cannot turn their IDs into authority.
    """
    if type(left) is not NativeCaptureV1 or type(right) is not NativeCaptureV1:
        raise ValueError("exact native capture required")
    if type(left.quotes) is not tuple or type(right.quotes) is not tuple:
        raise ValueError("immutable native quote inventory required")
    _preflight_size(len(left.quotes), len(right.quotes))
    _work_bound(
        len(left.quotes),
        len(right.quotes),
        len(left.quotes) * len(right.quotes),
    )
    left, right = _capture(left), _capture(right)
    if type(model) is not ClockModelV1 or type(policy) is not MatchingPolicyV1:
        raise ValueError("exact frozen clock and matching policy required")
    model = ClockModelV1.from_dict(model.to_dict())
    policy = MatchingPolicyV1.from_dict(policy.to_dict())
    if (
        model.left_capture_id != left.root_id
        or model.right_capture_id != right.root_id
        or model.left_projection_id != left.artifact_id
        or model.right_projection_id != right.artifact_id
        or left.root_id == right.root_id
    ):
        raise ValueError("clock model capture root mismatch")
    if decision_cutoffs is not None and (
        type(decision_cutoffs) is not tuple
        or len(decision_cutoffs) != 2
        or any(type(x) is not int or x < 0 for x in decision_cutoffs)
    ):
        raise ValueError("two exact host sequence cutoffs required")
    # Widths multiply inside the complete candidate/uncertainty report. Admit
    # all declared event roots before clock execution, not after serialization.
    for capture in (left, right):
        for quote in capture.quotes:
            if len(quote.event_id) > 256:
                raise ValueError("matching identity width bound")
            if (
                len(quote.health_reasons) > 32
                or any(len(reason) > 256 for reason in quote.health_reasons)
                or sum(len(reason) for reason in quote.health_reasons) > 256
            ):
                raise ValueError("matching health reason output bound")
    clocks = _clocks(left, model, "left", decision_cutoffs) + _clocks(
        right, model, "right", decision_cutoffs
    )
    clock_map = {(item.side, item.event_id): item for item in clocks}
    left_quotes = {quote.event_id: quote for quote in left.quotes}
    right_quotes = {quote.event_id: quote for quote in right.quotes}
    left_ids, right_ids = tuple(sorted(left_quotes)), tuple(
        sorted(right_quotes)
    )
    transitions_left, transitions_right = _transitions(left), _transitions(
        right
    )
    candidates: list[CandidateEdgeV1] = []
    rejected: list[RejectedPairV1] = []
    scales = (
        policy.time_scale_ns.value,
        policy.bid_scale.value,
        policy.ask_scale.value,
        policy.spread_scale.value,
        policy.transition_scale.value,
    )
    for left_id in left_ids:
        lq, lc = left_quotes[left_id], clock_map["left", left_id]
        for right_id in right_ids:
            rq, rc = right_quotes[right_id], clock_map["right", right_id]
            reason: str | None = None
            if lc.status != "usable" or rc.status != "usable":
                reason = "clock_unavailable"
            elif lq.symbol != rq.symbol:
                reason = "symbol_mismatch"
            elif lq.price_basis != rq.price_basis:
                reason = "price_basis_mismatch"
            if reason is not None:
                rejected.append(RejectedPairV1(left_id, right_id, reason))
                continue
            assert (
                lc.corrected_time_ns is not None
                and rc.corrected_time_ns is not None
            )
            assert (
                lc.uncertainty_radius_ns is not None
                and rc.uncertainty_radius_ns is not None
            )
            time_difference = _bounded(
                lc.corrected_time_ns.value - rc.corrected_time_ns.value
            )
            if abs(time_difference) > policy.time_tolerance_ns.value:
                rejected.append(RejectedPairV1(left_id, right_id, "time_gate"))
                continue
            lt, rt = transitions_left[left_id], transitions_right[right_id]
            transition = (
                _bounded(lt - rt) if lt is not None and rt is not None else None
            )
            if transition is None and policy.weights[4].value:
                rejected.append(
                    RejectedPairV1(
                        left_id, right_id, "transition_support_unavailable"
                    )
                )
                continue
            bid_difference = _bounded(lq.bid.value - rq.bid.value)
            ask_difference = _bounded(lq.ask.value - rq.ask.value)
            spread_difference = _bounded(
                (lq.ask.value - lq.bid.value) - (rq.ask.value - rq.bid.value)
            )
            if any(
                limit is not None and abs(difference) > limit.value
                for difference, limit in (
                    (bid_difference, policy.maximum_bid_difference),
                    (ask_difference, policy.maximum_ask_difference),
                    (spread_difference, policy.maximum_spread_difference),
                )
            ):
                rejected.append(RejectedPairV1(left_id, right_id, "price_gate"))
                continue
            cost = Fraction(0)
            values = (
                time_difference,
                bid_difference,
                ask_difference,
                spread_difference,
                transition,
            )
            for value, weight, scale in zip(values, policy.weights, scales):
                if value is not None and weight.value:
                    cost = _bounded(
                        cost + _bounded(weight.value * abs(value) / scale)
                    )
            uncertainty = _bounded(
                lc.uncertainty_radius_ns.value + rc.uncertainty_radius_ns.value
            )
            candidates.append(
                CandidateEdgeV1(
                    left_id,
                    right_id,
                    _r(time_difference),
                    _r(bid_difference),
                    _r(ask_difference),
                    _r(spread_difference),
                    _r(transition) if transition is not None else None,
                    _r(uncertainty),
                    _r(cost),
                )
            )
    work = _work_bound(len(left_ids), len(right_ids), len(candidates))
    graph = tuple(
        (edge.left_event_id, edge.right_event_id, edge.cost.value)
        for edge in candidates
    )
    selected, total = _solve(left_ids, right_ids, graph)
    edge_map = {edge.key: edge for edge in candidates}
    matches = []
    for key in selected:
        alternative, alternative_cost = _solve(left_ids, right_ids, graph, key)
        alternative_total = (
            alternative_cost if len(alternative) == len(selected) else None
        )
        margin = (
            _bounded(alternative_total - total)
            if alternative_total is not None
            else None
        )
        if margin is not None and margin < 0:
            raise ValueError("alternate assignment improved optimum")
        ambiguous = (
            margin is not None and margin <= policy.ambiguity_margin.value
        )
        edge = edge_map[key]
        uncertain = (
            edge.uncertainty_radius_ns.value
            > policy.maximum_uncertainty_ns.value
        )
        status = (
            "ambiguous_uncertain"
            if ambiguous and uncertain
            else (
                "ambiguous"
                if ambiguous
                else "uncertain" if uncertain else "confident"
            )
        )
        matches.append(
            MatchedPairV1(
                key[0],
                key[1],
                status,
                edge.cost,
                _r(total),
                (
                    _r(alternative_total)
                    if alternative_total is not None
                    else None
                ),
                _r(margin) if margin is not None else None,
                edge.uncertainty_radius_ns,
            )
        )
    only = []
    for side, ids, index in (("left", left_ids, 0), ("right", right_ids, 1)):
        used = {key[index] for key in selected}
        for event in ids:
            if event in used:
                continue
            reason = (
                "clock_unavailable"
                if clock_map[side, event].status == "unavailable"
                else (
                    "one_to_one_competition"
                    if any(edge.key[index] == event for edge in candidates)
                    else "no_compatible_candidate"
                )
            )
            only.append(SourceOnlyV1(side, event, reason))
    return MatchingReportV1(
        left.root_id,
        right.root_id,
        left.artifact_id,
        right.artifact_id,
        model.artifact_id,
        model.request.mode,
        decision_cutoffs,
        policy,
        left_ids,
        right_ids,
        clocks,
        tuple(candidates),
        tuple(rejected),
        tuple(matches),
        tuple(only),
        _r(total),
        work,
    )


def replay_matching_report(
    left: NativeCaptureV1,
    right: NativeCaptureV1,
    model: ClockModelV1,
    expected: MatchingReportV1,
    *,
    expected_report_id: str,
) -> MatchingReportV1:
    """Exact pure replay against an independently retained expected root."""
    if (
        type(expected_report_id) is not str
        or re.fullmatch(
            r"cross-feed-matching-report:sha256:[0-9a-f]{64}",
            expected_report_id,
        )
        is None
    ):
        raise ValueError("expected matching report identity required")
    if type(expected) is not MatchingReportV1:
        raise ValueError("exact matching report required")
    # Reconstruct raw fields before invoking an instance serializer. The wire
    # constructor bounds nested inventories and refuses subclass substitution.
    admitted = MatchingReportV1(
        **{
            field.name: getattr(expected, field.name)
            for field in fields(expected)
        }
    )
    if admitted.artifact_id != expected_report_id:
        raise ValueError("independent expected matching root mismatch")
    cutoffs = admitted.decision_cutoffs
    if cutoffs is None:
        exact_cutoffs = None
    else:
        exact_cutoffs = (cutoffs[0], cutoffs[1])
    actual = match_cross_feed(
        left, right, model, admitted.policy, decision_cutoffs=exact_cutoffs
    )
    if actual.to_json() != admitted.to_json():
        raise ValueError("matching semantic replay mismatch")
    return actual
