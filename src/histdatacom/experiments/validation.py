"""Declared search, comparisons and immutable lifecycle interoperability."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import ClassVar

from ._wire import (
    Artifact,
    Record,
    canonical_json,
    digest,
    identifier,
    ordered,
    text,
)
from .contracts import ArtifactReferenceV1, ExperimentBundleV1


def scientific_differences(
    left: ExperimentBundleV1, right: ExperimentBundleV1
) -> tuple[str, ...]:
    """Exact top-level scientific coordinates; component references are atomic.

    Environment and RNG subfields are separately named. Descriptive branch and
    display title are deliberately absent. No statistical causal claim follows.
    """

    def coordinates(bundle: ExperimentBundleV1) -> dict[str, object]:
        inputs = bundle.scientific_inputs
        result = inputs.to_payload()
        result.pop("components")
        result.pop("environment")
        result.pop("randomness")
        result.update(
            {
                "components." + c.field.value: c.to_payload()
                for c in inputs.components
            }
        )
        result.update(
            {
                "environment." + key: value
                for key, value in inputs.environment.to_payload().items()
            }
        )
        result.update(
            {
                "randomness." + key: value
                for key, value in inputs.randomness.to_payload().items()
            }
        )
        return result

    a, b = coordinates(left), coordinates(right)
    return tuple(
        sorted(
            key for key in a if canonical_json(a[key]) != canonical_json(b[key])
        )
    )


class ComparisonKind(str, Enum):
    ABLATION = "controlled_component_ablation"
    CHAMPION_CHALLENGER = "champion_challenger"
    DESCRIPTIVE = "descriptive_comparison"


@dataclass(frozen=True, slots=True)
class ExperimentComparisonV1(Artifact):
    KIND: ClassVar[str] = "comparison"
    left_experiment_id: str
    right_experiment_id: str
    kind: ComparisonKind
    declared_differences: tuple[str, ...]
    left_result_id: str | None = None
    right_result_id: str | None = None

    def _validate(self) -> None:
        identifier(self.left_experiment_id, "scientific")
        identifier(self.right_experiment_id, "scientific")
        if self.left_experiment_id == self.right_experiment_id:
            raise ValueError(
                "comparison requires distinct scientific experiments"
            )
        ordered(self.declared_differences, nonempty=True)
        if (self.left_result_id is None) != (self.right_result_id is None):
            raise ValueError("comparison requires both results or neither")
        for result in (self.left_result_id, self.right_result_id):
            if result is not None:
                identifier(result, "metric-result")


@dataclass(frozen=True, slots=True)
class CandidateDeclarationV1(Record):
    candidate_key: str
    experiment_id: str
    parent_experiment_ids: tuple[str, ...]

    def _validate(self) -> None:
        text(self.candidate_key)
        identifier(self.experiment_id, "scientific")
        ordered(self.parent_experiment_ids)
        for parent in self.parent_experiment_ids:
            identifier(parent, "scientific")
            if parent == self.experiment_id:
                raise ValueError("self-parented candidate")


@dataclass(frozen=True, slots=True)
class ExperimentSearchPlanV1(Artifact):
    """Pre-result inventory identity without circular candidate experiment IDs."""

    KIND: ClassVar[str] = "search-plan"
    cycle_id: str
    declared_at_ns: int
    predecessor_plan_ids: tuple[str, ...]
    protected_content_sha256: tuple[str, ...]
    search_budget: int
    candidate_keys: tuple[str, ...]

    def _validate(self) -> None:
        text(self.cycle_id)
        if self.declared_at_ns < 0 or not 0 < self.search_budget <= 4096:
            raise ValueError("invalid frozen search plan budget/clock")
        ordered(self.predecessor_plan_ids)
        ordered(self.protected_content_sha256)
        ordered(self.candidate_keys, nonempty=True)
        if len(self.candidate_keys) > self.search_budget:
            raise ValueError("frozen search plan exceeds budget")
        for value in self.predecessor_plan_ids:
            identifier(value, "search-plan")
        for value in self.protected_content_sha256:
            digest(value)


@dataclass(frozen=True, slots=True)
class ResearchCycleV1(Artifact):
    """Frozen DECLARED inventory; cannot certify unreported external work."""

    KIND: ClassVar[str] = "research-cycle"
    cycle_id: str
    declared_at_ns: int
    predecessor_cycle_ids: tuple[str, ...]
    protected_roots: tuple[str, ...]
    search_budget: int
    candidates: tuple[CandidateDeclarationV1, ...]
    search_plan: ExperimentSearchPlanV1

    def _validate(self) -> None:
        text(self.cycle_id)
        if (
            self.cycle_id != self.search_plan.cycle_id
            or self.declared_at_ns != self.search_plan.declared_at_ns
            or self.search_budget != self.search_plan.search_budget
            or tuple(c.candidate_key for c in self.candidates)
            != self.search_plan.candidate_keys
        ):
            raise ValueError("cycle differs from frozen search plan")
        if self.declared_at_ns < 0 or not 0 < self.search_budget <= 4096:
            raise ValueError("invalid cycle clock/search budget")
        ordered(self.predecessor_cycle_ids)
        ordered(self.protected_roots)
        if self.cycle_id in self.predecessor_cycle_ids:
            raise ValueError("self-parented research cycle")
        ordered(tuple(c.candidate_key for c in self.candidates), nonempty=True)
        if len(self.candidates) > self.search_budget:
            raise ValueError("declared candidate budget exceeded")
        if len({c.experiment_id for c in self.candidates}) != len(
            self.candidates
        ):
            raise ValueError("duplicate candidate scientific identity")


class CandidateOutcome(str, Enum):
    NOT_EXECUTED = "not_executed"
    FAILED = "executed_failed"
    UNFAVORABLE = "executed_unfavorable"
    FAVORABLE = "executed_favorable_declared_not_certified"
    INSUFFICIENT = "executed_insufficient_evidence"


@dataclass(frozen=True, slots=True)
class CandidateDispositionV1(Artifact):
    KIND: ClassVar[str] = "candidate-disposition"
    cycle_id: str
    experiment_id: str
    outcome: CandidateOutcome
    attempt_ids: tuple[str, ...]
    result_ids: tuple[str, ...]
    reason: str

    def _validate(self) -> None:
        text(self.cycle_id)
        identifier(self.experiment_id, "scientific")
        text(self.reason)
        ordered(self.attempt_ids)
        ordered(self.result_ids)
        for value in self.attempt_ids:
            identifier(value, "attempt")
        for value in self.result_ids:
            identifier(value, "metric-result")
        if self.outcome is CandidateOutcome.NOT_EXECUTED:
            if self.attempt_ids or self.result_ids:
                raise ValueError("unexecuted candidate has execution evidence")
        elif not self.attempt_ids:
            raise ValueError("executed candidate lacks retained attempt")
        if (
            self.outcome
            in (CandidateOutcome.FAVORABLE, CandidateOutcome.UNFAVORABLE)
            and not self.result_ids
        ):
            raise ValueError("candidate outcome lacks retained results")


@dataclass(frozen=True, slots=True)
class ProtectedExposureV1(Artifact):
    KIND: ClassVar[str] = "protected-exposure"
    experiment_id: str
    protected_root: str
    inspected_at_ns: int
    inspected_result_ids: tuple[str, ...]
    redesigned_experiment_ids: tuple[str, ...]

    def _validate(self) -> None:
        identifier(self.experiment_id, "scientific")
        text(self.protected_root)
        if self.inspected_at_ns < 0:
            raise ValueError("invalid inspection clock")
        ordered(self.inspected_result_ids, nonempty=True)
        ordered(self.redesigned_experiment_ids)
        for result in self.inspected_result_ids:
            identifier(result, "metric-result")
        for experiment in self.redesigned_experiment_ids:
            identifier(experiment, "scientific")


class ExperimentStatus(str, Enum):
    PLANNED = "planned"
    RUNNING = "running"
    FAILED = "failed"
    COMPLETED = "completed"
    INVALIDATED = "invalidated"
    SUPERSEDED = "superseded"


@dataclass(frozen=True, slots=True)
class ExperimentLifecycleV1(Artifact):
    """Append-only event, never a mutable status inside historical results."""

    KIND: ClassVar[str] = "lifecycle"
    experiment_id: str
    previous_event_id: str | None
    status: ExperimentStatus
    recorded_at_ns: int
    reason: str
    successor_experiment_id: str | None = None

    def _validate(self) -> None:
        identifier(self.experiment_id, "scientific")
        if self.previous_event_id is not None:
            identifier(self.previous_event_id, "lifecycle")
        text(self.reason)
        if self.recorded_at_ns < 0:
            raise ValueError("invalid lifecycle clock")
        if (
            self.status is ExperimentStatus.SUPERSEDED
            and self.successor_experiment_id is None
        ):
            raise ValueError("supersession requires successor")
        if self.successor_experiment_id is not None:
            identifier(self.successor_experiment_id, "scientific")
            if (
                self.status
                not in (
                    ExperimentStatus.SUPERSEDED,
                    ExperimentStatus.INVALIDATED,
                )
                or self.successor_experiment_id == self.experiment_id
            ):
                raise ValueError("invalid lifecycle successor")


@dataclass(frozen=True, slots=True)
class PromotionBindingV1(Artifact):
    """Qualification-context interoperability, NOT an operational deployment."""

    KIND: ClassVar[str] = "promotion-binding"
    deployment: ArtifactReferenceV1
    promotion_policy: ArtifactReferenceV1
    qualifying_experiment_ids: tuple[str, ...]
    qualifying_result_ids: tuple[str, ...]
    comparison_ids: tuple[str, ...]
    recorded_at_ns: int
    limitations: tuple[str, ...]

    def _validate(self) -> None:
        ordered(self.qualifying_experiment_ids, nonempty=True)
        ordered(self.qualifying_result_ids, nonempty=True)
        ordered(self.comparison_ids)
        ordered(self.limitations, nonempty=True)
        if self.recorded_at_ns < 0:
            raise ValueError("invalid promotion clock")
        for value in self.qualifying_experiment_ids:
            identifier(value, "scientific")
        for value in self.qualifying_result_ids:
            identifier(value, "metric-result")
        for value in self.comparison_ids:
            identifier(value, "comparison")


@dataclass(frozen=True, slots=True)
class ExperimentReportV1(Artifact):
    """A report declares references only: displayed values resolve from results."""

    KIND: ClassVar[str] = "report"
    title: str
    result_ids: tuple[str, ...]
    comparison_ids: tuple[str, ...]

    def _validate(self) -> None:
        text(self.title)
        ordered(self.result_ids, nonempty=True)
        ordered(self.comparison_ids)
        for result in self.result_ids:
            identifier(result, "metric-result")
        for comparison in self.comparison_ids:
            identifier(comparison, "comparison")
