"""Executable synthetic action decomposition; not a live order authority."""

from __future__ import annotations

from dataclasses import dataclass
from fractions import Fraction
from math import isfinite
from typing import ClassVar

from ._wire import Artifact, Record, ordered, text
from .contracts import AttributionReferenceV1, CAUSAL_NONCLAIM
from .reference import _float

STAGES = (
    "model",
    "calibration",
    "policy",
    "sizing",
    "portfolio",
    "margin",
    "validity",
    "execution",
)


@dataclass(frozen=True, slots=True)
class ReferenceDecisionPolicyV1(Artifact):
    """Frozen simple fixture equations; no fitted/live policy or safety claim."""

    KIND: ClassVar[str] = "reference-decision-policy"
    calibration_scale: float
    calibration_offset: float
    short_threshold: float
    long_threshold: float
    expected_cost: float
    size: float
    maximum_position: float
    previous_position: float
    hysteresis: float
    portfolio_limit: float
    margin_limit: float
    validity_allowed: bool
    execution_allowed: bool
    known_at_ns: int

    def _validate(self) -> None:
        if self.short_threshold >= self.long_threshold or self.known_at_ns < 0:
            raise ValueError("decision threshold/clock ordering")
        if (
            min(
                self.expected_cost,
                self.size,
                self.maximum_position,
                self.hysteresis,
                self.portfolio_limit,
                self.margin_limit,
            )
            < 0
        ):
            raise ValueError("negative expected cost or decision bound")


@dataclass(frozen=True, slots=True)
class DecisionStageV1(Record):
    stage: str
    input_value: float
    output_value: float
    reason: str
    active_constraints: tuple[str, ...]

    def _validate(self) -> None:
        if self.stage not in STAGES:
            raise ValueError("unknown decision decomposition stage")
        text(self.reason)
        ordered(self.active_constraints)


def _stages(
    raw: float, policy: ReferenceDecisionPolicyV1
) -> tuple[DecisionStageV1, ...]:
    if (
        type(raw) is not float
        or not isfinite(raw)
        or type(policy) is not ReferenceDecisionPolicyV1
    ):
        raise ValueError(
            "decision execution requires finite float and exact immutable policy"
        )
    current = Fraction(raw)
    result = [
        DecisionStageV1("model", raw, raw, "raw_model_score_not_action", ())
    ]

    def add(
        stage: str,
        value: Fraction,
        reason: str,
        constraints: tuple[str, ...] = (),
    ) -> None:
        nonlocal current
        result.append(
            DecisionStageV1(
                stage, _float(current), _float(value), reason, constraints
            )
        )
        current = value

    add(
        "calibration",
        current * Fraction(policy.calibration_scale)
        + Fraction(policy.calibration_offset),
        "frozen_affine_calibration",
    )
    signal = (
        1
        if current >= Fraction(policy.long_threshold)
        else (-1 if current <= Fraction(policy.short_threshold) else 0)
    )
    constraints: tuple[str, ...] = ("deadband",)
    reason = "policy_deadband"
    if signal:
        reason, constraints = "threshold_direction", ()
        if current * signal <= Fraction(policy.expected_cost):
            signal, reason, constraints = (
                0,
                "cost_gate_rejected",
                ("expected_cost",),
            )
    add("policy", Fraction(signal), reason, constraints)
    desired = current * Fraction(policy.size)
    reason, constraints = "fixed_reference_size", ()
    if current and abs(desired - Fraction(policy.previous_position)) < Fraction(
        policy.hysteresis
    ):
        desired, reason, constraints = (
            Fraction(policy.previous_position),
            "hysteresis_retained_position",
            ("turnover_hysteresis",),
        )
    cap = Fraction(policy.maximum_position)
    clipped = max(-cap, min(cap, desired))
    if clipped != desired:
        reason, constraints = "size_clipped", tuple(
            sorted(constraints + ("position_cap",))
        )
    add("sizing", clipped, reason, constraints)
    for stage, limit in (
        ("portfolio", policy.portfolio_limit),
        ("margin", policy.margin_limit),
    ):
        projected = max(-Fraction(limit), min(Fraction(limit), current))
        changed = projected != current
        add(
            stage,
            projected,
            stage + "_projection" if changed else stage + "_unchanged",
            (stage + "_cap",) if changed else (),
        )
    add(
        "validity",
        current if policy.validity_allowed else Fraction(),
        "validity_passed" if policy.validity_allowed else "validity_refused",
        () if policy.validity_allowed else ("validity",),
    )
    add(
        "execution",
        current if policy.execution_allowed else Fraction(),
        (
            "synthetic_execution_accepted"
            if policy.execution_allowed
            else "execution_rejected"
        ),
        () if policy.execution_allowed else ("execution",),
    )
    return tuple(result)


@dataclass(frozen=True, slots=True)
class ReferenceDecisionV1(Artifact):
    """Immutable original action. Later explanation binds it, never rewrites it."""

    KIND: ClassVar[str] = "reference-action"
    model: AttributionReferenceV1
    snapshot: AttributionReferenceV1
    deployment: AttributionReferenceV1
    account_state: AttributionReferenceV1
    policy: ReferenceDecisionPolicyV1
    decision_at_ns: int
    raw_score: float
    stages: tuple[DecisionStageV1, ...]
    final_action: float
    causal_nonclaim: str = CAUSAL_NONCLAIM

    def _validate(self) -> None:
        if (
            self.causal_nonclaim != CAUSAL_NONCLAIM
            or self.decision_at_ns < self.policy.known_at_ns
        ):
            raise ValueError("decision nonclaim/availability")
        expected = _stages(self.raw_score, self.policy)
        if (
            self.stages != expected
            or self.final_action != expected[-1].output_value
        ):
            raise ValueError(
                "decision explanation differs from executed stage equations"
            )


def execute_reference_decision(
    model: AttributionReferenceV1,
    snapshot: AttributionReferenceV1,
    deployment: AttributionReferenceV1,
    account_state: AttributionReferenceV1,
    policy: ReferenceDecisionPolicyV1,
    *,
    decision_at_ns: int,
    raw_score: float,
) -> ReferenceDecisionV1:
    stages = _stages(raw_score, policy)
    return ReferenceDecisionV1(
        model,
        snapshot,
        deployment,
        account_state,
        policy,
        decision_at_ns,
        raw_score,
        stages,
        stages[-1].output_value,
    )
