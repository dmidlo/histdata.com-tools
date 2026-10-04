"""Frozen matching and known-truth evaluation contracts; no feed authority."""

from __future__ import annotations

from dataclasses import dataclass
from fractions import Fraction
from typing import ClassVar

from ._wire import Artifact, RationalV1, Record, text

MAX_SIDE_EVENTS = 64
MAX_CANDIDATE_PAIRS = 4096
MAX_SOLVE_WORK = 8_000_000
MAX_ARITHMETIC_BITS = 4096
MAX_SENSITIVITY_POINTS = 8


def _ids(values: tuple[str, ...]) -> None:
    if values != tuple(sorted(set(values))):
        raise ValueError("matching identities must be sorted unique")
    for value in values:
        text(value)
        if len(value) > 256:
            raise ValueError("matching identity width bound")


@dataclass(frozen=True, slots=True)
class MatchingPolicyV1(Artifact):
    """Exact dimensions: ns for time, native quote-price units otherwise.

    Transition is the difference of consecutive per-symbol mid-price changes
    in native host-record order. A positive transition weight requires both
    predecessors. No worker parameter or hidden estimated scale exists.
    """

    time_tolerance_ns: RationalV1
    time_scale_ns: RationalV1
    bid_scale: RationalV1
    ask_scale: RationalV1
    spread_scale: RationalV1
    transition_scale: RationalV1
    weights: tuple[RationalV1, ...]
    ambiguity_margin: RationalV1
    maximum_uncertainty_ns: RationalV1
    maximum_bid_difference: RationalV1 | None = None
    maximum_ask_difference: RationalV1 | None = None
    maximum_spread_difference: RationalV1 | None = None
    algorithm: str = "max_cardinality_min_cost_global_alternatives_v1"
    transition_basis: str = "native_host_order_mid_change_v1"
    KIND: ClassVar[str] = "matching-policy"

    def _validate(self) -> None:
        if self.algorithm != "max_cardinality_min_cost_global_alternatives_v1":
            raise ValueError("unsupported matching algorithm")
        if self.transition_basis != "native_host_order_mid_change_v1":
            raise ValueError("unsupported matching transition basis")
        if len(self.weights) != 5 or any(x.value < 0 for x in self.weights):
            raise ValueError("five nonnegative frozen weights required")
        if not any(x.value for x in self.weights):
            raise ValueError("at least one matching weight must be positive")
        if (
            min(
                self.time_tolerance_ns.value,
                self.ambiguity_margin.value,
                self.maximum_uncertainty_ns.value,
            )
            < 0
        ):
            raise ValueError("matching thresholds must be nonnegative")
        if (
            min(
                self.time_scale_ns.value,
                self.bid_scale.value,
                self.ask_scale.value,
                self.spread_scale.value,
                self.transition_scale.value,
            )
            <= 0
        ):
            raise ValueError("matching normalization scales must be positive")
        if any(
            value is not None and value.value < 0
            for value in (
                self.maximum_bid_difference,
                self.maximum_ask_difference,
                self.maximum_spread_difference,
            )
        ):
            raise ValueError("price compatibility limits must be nonnegative")


@dataclass(frozen=True, slots=True)
class MatchingClockV1(Record):
    side: str
    event_id: str
    status: str
    corrected_time_ns: RationalV1 | None
    uncertainty_radius_ns: RationalV1 | None
    segment_id: str | None
    reasons: tuple[str, ...]

    def _validate(self) -> None:
        if self.side not in ("left", "right"):
            raise ValueError("invalid matching clock side")
        _ids((self.event_id,))
        if self.status not in ("usable", "unavailable"):
            raise ValueError("invalid matching clock status")
        present = (
            self.corrected_time_ns is not None
            and self.uncertainty_radius_ns is not None
        )
        if (self.status == "usable") != present:
            raise ValueError("matching clock status/projection mismatch")
        if self.status == "unavailable" and (
            self.corrected_time_ns is not None
            or self.uncertainty_radius_ns is not None
        ):
            raise ValueError("unavailable clock cannot contain a projection")
        if (
            self.uncertainty_radius_ns is not None
            and self.uncertainty_radius_ns.value < 0
        ):
            raise ValueError("negative clock uncertainty")
        _ids(self.reasons)


@dataclass(frozen=True, slots=True)
class CandidateEdgeV1(Record):
    left_event_id: str
    right_event_id: str
    time_residual_ns: RationalV1
    bid_residual: RationalV1
    ask_residual: RationalV1
    spread_residual: RationalV1
    transition_residual: RationalV1 | None
    uncertainty_radius_ns: RationalV1
    cost: RationalV1

    def _validate(self) -> None:
        _ids((self.left_event_id,))
        _ids((self.right_event_id,))
        if min(self.cost.value, self.uncertainty_radius_ns.value) < 0:
            raise ValueError("negative matching cost/uncertainty")

    @property
    def key(self) -> tuple[str, str]:
        return self.left_event_id, self.right_event_id


@dataclass(frozen=True, slots=True)
class RejectedPairV1(Record):
    left_event_id: str
    right_event_id: str
    reason: str

    def _validate(self) -> None:
        _ids((self.left_event_id,))
        _ids((self.right_event_id,))
        if self.reason not in (
            "clock_unavailable",
            "symbol_mismatch",
            "price_basis_mismatch",
            "price_gate",
            "time_gate",
            "transition_support_unavailable",
        ):
            raise ValueError("unknown candidate rejection reason")

    @property
    def key(self) -> tuple[str, str]:
        return self.left_event_id, self.right_event_id


@dataclass(frozen=True, slots=True)
class MatchedPairV1(Record):
    left_event_id: str
    right_event_id: str
    status: str
    edge_cost: RationalV1
    best_total_cost: RationalV1
    alternate_total_cost: RationalV1 | None
    global_margin: RationalV1 | None
    uncertainty_radius_ns: RationalV1

    def _validate(self) -> None:
        _ids((self.left_event_id,))
        _ids((self.right_event_id,))
        if self.status not in (
            "confident",
            "ambiguous",
            "uncertain",
            "ambiguous_uncertain",
        ):
            raise ValueError("unknown match status")
        if (
            min(
                self.edge_cost.value,
                self.best_total_cost.value,
                self.uncertainty_radius_ns.value,
            )
            < 0
        ):
            raise ValueError("negative match cost/uncertainty")
        if (self.alternate_total_cost is None) != (self.global_margin is None):
            raise ValueError("alternate assignment and margin must coexist")
        if self.alternate_total_cost is not None:
            assert self.global_margin is not None
            if (
                self.global_margin.value < 0
                or self.alternate_total_cost.value - self.best_total_cost.value
                != self.global_margin.value
            ):
                raise ValueError("incorrect global assignment margin")

    @property
    def key(self) -> tuple[str, str]:
        return self.left_event_id, self.right_event_id


@dataclass(frozen=True, slots=True)
class SourceOnlyV1(Record):
    side: str
    event_id: str
    reason: str

    def _validate(self) -> None:
        _ids((self.event_id,))
        if self.side not in ("left", "right") or self.reason not in (
            "clock_unavailable",
            "no_compatible_candidate",
            "one_to_one_competition",
        ):
            raise ValueError("invalid source-only observation")


@dataclass(frozen=True, slots=True)
class MatchingReportV1(Artifact):
    left_capture_id: str
    right_capture_id: str
    left_projection_id: str
    right_projection_id: str
    model_id: str
    clock_mode: str
    decision_cutoffs: tuple[int, ...] | None
    policy: MatchingPolicyV1
    left_event_ids: tuple[str, ...]
    right_event_ids: tuple[str, ...]
    clocks: tuple[MatchingClockV1, ...]
    candidates: tuple[CandidateEdgeV1, ...]
    rejected_pairs: tuple[RejectedPairV1, ...]
    matches: tuple[MatchedPairV1, ...]
    source_only: tuple[SourceOnlyV1, ...]
    total_cost: RationalV1
    prospective_solve_work: int
    interpretation: str = "conditional_correspondence_not_latency_or_retention"
    KIND: ClassVar[str] = "matching-report"

    def _validate(self) -> None:
        for value in (
            self.left_capture_id,
            self.right_capture_id,
            self.left_projection_id,
            self.right_projection_id,
            self.model_id,
        ):
            _ids((value,))
        if self.left_capture_id == self.right_capture_id:
            raise ValueError("cross-feed roots must be distinct")
        if self.clock_mode not in ("retrospective", "frozen_ex_ante"):
            raise ValueError("invalid matching clock mode")
        if self.decision_cutoffs is not None and (
            len(self.decision_cutoffs) != 2 or min(self.decision_cutoffs) < 0
        ):
            raise ValueError("two nonnegative host sequence cutoffs required")
        if (
            self.interpretation
            != "conditional_correspondence_not_latency_or_retention"
        ):
            raise ValueError("unsupported matching interpretation")
        for values in (self.left_event_ids, self.right_event_ids):
            _ids(values)
            if len(values) > MAX_SIDE_EVENTS:
                raise ValueError("matching event bound")
        if not 0 <= self.prospective_solve_work <= MAX_SOLVE_WORK:
            raise ValueError("matching solve work bound")
        universe = {
            (left, right)
            for left in self.left_event_ids
            for right in self.right_event_ids
        }
        candidate_keys = tuple(x.key for x in self.candidates)
        rejected_keys = tuple(x.key for x in self.rejected_pairs)
        if (
            candidate_keys != tuple(sorted(set(candidate_keys)))
            or rejected_keys != tuple(sorted(set(rejected_keys)))
            or set(candidate_keys) & set(rejected_keys)
            or set(candidate_keys) | set(rejected_keys) != universe
        ):
            raise ValueError("candidate/rejection denominator mismatch")
        expected_clocks = tuple(
            (side, event)
            for side, events in (
                ("left", self.left_event_ids),
                ("right", self.right_event_ids),
            )
            for event in events
        )
        if tuple((x.side, x.event_id) for x in self.clocks) != expected_clocks:
            raise ValueError("clock projection denominator mismatch")
        clock_map = {(x.side, x.event_id): x for x in self.clocks}
        edge_map = {edge.key: edge for edge in self.candidates}
        match_keys = tuple(x.key for x in self.matches)
        if match_keys != tuple(sorted(set(match_keys))):
            raise ValueError("matches must be sorted unique")
        for index in (0, 1):
            if len({key[index] for key in match_keys}) != len(match_keys):
                raise ValueError("matching is not one-to-one")
        if self.total_cost.value != sum(
            (match.edge_cost.value for match in self.matches), Fraction(0)
        ):
            raise ValueError("matching total cost mismatch")
        for match in self.matches:
            edge = edge_map.get(match.key)
            if (
                edge is None
                or match.edge_cost != edge.cost
                or match.best_total_cost != self.total_cost
                or match.uncertainty_radius_ns != edge.uncertainty_radius_ns
            ):
                raise ValueError("match does not bind its candidate")
            ambiguous = (
                match.global_margin is not None
                and match.global_margin.value
                <= self.policy.ambiguity_margin.value
            )
            uncertain = (
                edge.uncertainty_radius_ns.value
                > self.policy.maximum_uncertainty_ns.value
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
            if match.status != status:
                raise ValueError("match status is not derived from policy")
        expected_only = tuple(
            (side, event)
            for side, events, index in (
                ("left", self.left_event_ids, 0),
                ("right", self.right_event_ids, 1),
            )
            for event in events
            if event not in {key[index] for key in match_keys}
        )
        if (
            tuple((x.side, x.event_id) for x in self.source_only)
            != expected_only
        ):
            raise ValueError("source-only denominator mismatch")
        for item in self.source_only:
            index = 0 if item.side == "left" else 1
            reason = (
                "clock_unavailable"
                if clock_map[item.side, item.event_id].status == "unavailable"
                else (
                    "one_to_one_competition"
                    if any(
                        key[index] == item.event_id for key in candidate_keys
                    )
                    else "no_compatible_candidate"
                )
            )
            if item.reason != reason:
                raise ValueError("source-only reason mismatch")


@dataclass(frozen=True, slots=True)
class KnownCorrespondenceV1(Record):
    left_event_id: str
    right_event_id: str

    def _validate(self) -> None:
        _ids((self.left_event_id,))
        _ids((self.right_event_id,))

    @property
    def key(self) -> tuple[str, str]:
        return self.left_event_id, self.right_event_id


@dataclass(frozen=True, slots=True)
class MatchingTruthV1(Artifact):
    """Independent fixture assertion, never learned from assigned matches."""

    fixture_id: str
    left_capture_id: str
    right_capture_id: str
    left_event_ids: tuple[str, ...]
    right_event_ids: tuple[str, ...]
    correspondences: tuple[KnownCorrespondenceV1, ...]
    scope: str = "invented_synthetic_known_truth_only"
    KIND: ClassVar[str] = "matching-truth"

    def _validate(self) -> None:
        for value in (
            self.fixture_id,
            self.left_capture_id,
            self.right_capture_id,
        ):
            _ids((value,))
        for values in (self.left_event_ids, self.right_event_ids):
            _ids(values)
            if len(values) > MAX_SIDE_EVENTS:
                raise ValueError("known truth event bound")
        keys = tuple(x.key for x in self.correspondences)
        if keys != tuple(sorted(set(keys))):
            raise ValueError("known truth must be sorted unique")
        if any(
            left not in self.left_event_ids or right not in self.right_event_ids
            for left, right in keys
        ) or any(len({key[i] for key in keys}) != len(keys) for i in (0, 1)):
            raise ValueError("known truth must be one-to-one within inventory")
        if self.scope != "invented_synthetic_known_truth_only":
            raise ValueError("known truth is not a real-feed qualification")


@dataclass(frozen=True, slots=True)
class MatchingEvaluationV1(Artifact):
    report_id: str
    truth_id: str
    true_positive: int
    false_positive: int
    false_negative: int
    precision: RationalV1 | None
    recall: RationalV1 | None
    ambiguity_rate: RationalV1 | None
    unmatched_rate: RationalV1 | None
    time_residuals_ns: tuple[RationalV1, ...]
    bid_residuals: tuple[RationalV1, ...]
    ask_residuals: tuple[RationalV1, ...]
    scope: str = "invented_synthetic_known_truth_only"
    KIND: ClassVar[str] = "matching-evaluation"

    def _validate(self) -> None:
        _ids((self.report_id,))
        _ids((self.truth_id,))
        if (
            min(self.true_positive, self.false_positive, self.false_negative)
            < 0
        ):
            raise ValueError("negative evaluation count")
        if (
            max(
                self.true_positive + self.false_positive,
                self.true_positive + self.false_negative,
            )
            > MAX_SIDE_EVENTS
        ):
            raise ValueError("evaluation count bound")
        for value, denominator in (
            (self.precision, self.true_positive + self.false_positive),
            (self.recall, self.true_positive + self.false_negative),
        ):
            expected = (
                Fraction(self.true_positive, denominator)
                if denominator
                else None
            )
            if (value.value if value is not None else None) != expected:
                raise ValueError("evaluation precision/recall mismatch")
        for value in (self.ambiguity_rate, self.unmatched_rate):
            if value is not None and not 0 <= value.value <= 1:
                raise ValueError("evaluation rate out of range")
        n = self.true_positive + self.false_positive
        for values in (
            self.time_residuals_ns,
            self.bid_residuals,
            self.ask_residuals,
        ):
            if len(values) != n or tuple(x.value for x in values) != tuple(
                sorted(x.value for x in values)
            ):
                raise ValueError(
                    "residual distribution requires sorted full samples"
                )
        if self.scope != "invented_synthetic_known_truth_only":
            raise ValueError("unsupported evaluation claim")


@dataclass(frozen=True, slots=True)
class MatchingSensitivityPointV1(Record):
    time_tolerance_ns: RationalV1
    matching_report_id: str
    evaluation: MatchingEvaluationV1

    def _validate(self) -> None:
        if self.time_tolerance_ns.value < 0:
            raise ValueError("negative sensitivity tolerance")
        if self.matching_report_id != self.evaluation.report_id:
            raise ValueError("sensitivity report identity mismatch")


@dataclass(frozen=True, slots=True)
class MatchingSensitivityV1(Artifact):
    left_capture_id: str
    right_capture_id: str
    model_id: str
    baseline_policy_id: str
    truth_id: str
    decision_cutoffs: tuple[int, ...] | None
    points: tuple[MatchingSensitivityPointV1, ...]
    scope: str = "predeclared_synthetic_sensitivity_not_policy_selection"
    KIND: ClassVar[str] = "matching-sensitivity"

    def _validate(self) -> None:
        for value in (
            self.left_capture_id,
            self.right_capture_id,
            self.model_id,
            self.baseline_policy_id,
            self.truth_id,
        ):
            _ids((value,))
        if not 1 <= len(self.points) <= MAX_SENSITIVITY_POINTS:
            raise ValueError("sensitivity point bound")
        tolerances = tuple(
            point.time_tolerance_ns.value for point in self.points
        )
        if tolerances != tuple(sorted(set(tolerances))):
            raise ValueError("sensitivity tolerances must be sorted unique")
        if any(
            point.evaluation.truth_id != self.truth_id for point in self.points
        ):
            raise ValueError("sensitivity truth identity mismatch")
        if self.decision_cutoffs is not None and (
            len(self.decision_cutoffs) != 2 or min(self.decision_cutoffs) < 0
        ):
            raise ValueError("sensitivity requires exact host cutoffs")
        if (
            self.scope
            != "predeclared_synthetic_sensitivity_not_policy_selection"
        ):
            raise ValueError("unsupported sensitivity claim")
