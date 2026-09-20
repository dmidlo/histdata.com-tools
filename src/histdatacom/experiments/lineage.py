"""Closed declared registry validation and reverse invalidation discovery.

This graph proves internal reference consistency, not the absence of undisclosed
work, truthful clocks, empirical validity, source rights or historical knowledge.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, TypeVar

from ._wire import Artifact, canonical_json, load_json
from .contracts import (
    AttemptStatus,
    ArtifactReferenceV1,
    ComponentField,
    EvidenceKind,
    ExperimentAttemptV1,
    ExperimentBundleV1,
    ExperimentResultV1,
    MetricRegistryV1,
    ResultReceiptV1,
    ResultState,
    RetainedFixtureV1,
    result_reference,
)
from .validation import (
    CandidateDispositionV1,
    CandidateOutcome,
    ExperimentComparisonV1,
    ExperimentLifecycleV1,
    ExperimentReportV1,
    ExperimentStatus,
    PromotionBindingV1,
    ProtectedExposureV1,
    ResearchCycleV1,
    scientific_differences,
)

_A = TypeVar("_A", bound=Artifact)


def _index(items: tuple[_A, ...]) -> dict[str, _A]:
    result = {item.artifact_id: item for item in items}
    if len(result) != len(items):
        raise ValueError("duplicate registry artifact")
    if tuple(result) != tuple(sorted(result)):
        raise ValueError("registry artifacts must be sorted by artifact ID")
    return result


def _ancestors(start: str, edges: dict[str, set[str]]) -> set[str]:
    result: set[str] = set()
    pending = list(edges.get(start, ()))
    while pending:
        current = pending.pop()
        if current not in result:
            result.add(current)
            pending.extend(edges.get(current, ()))
    return result


def _acyclic(edges: dict[str, set[str]]) -> None:
    remaining = {node: len(parents) for node, parents in edges.items()}
    children: dict[str, set[str]] = {node: set() for node in edges}
    for node, parents in edges.items():
        for parent in parents:
            if parent not in edges:
                raise ValueError("unresolved graph ancestor")
            children[parent].add(node)
    queue = [node for node, count in remaining.items() if not count]
    seen = 0
    while queue:
        parent = queue.pop()
        seen += 1
        for child in children[parent]:
            remaining[child] -= 1
            if not remaining[child]:
                queue.append(child)
    if seen != len(edges):
        raise ValueError("cyclic declared graph")


@dataclass(frozen=True, slots=True)
class ExperimentRegistryV1(Artifact):
    """One bounded immutable snapshot; originals survive subsequent snapshots."""

    KIND: ClassVar[str] = "registry"
    bundles: tuple[ExperimentBundleV1, ...]
    metric_registries: tuple[MetricRegistryV1, ...]
    fixtures: tuple[RetainedFixtureV1, ...]
    attempts: tuple[ExperimentAttemptV1, ...]
    results: tuple[ExperimentResultV1, ...]
    result_receipts: tuple[ResultReceiptV1, ...]
    cycles: tuple[ResearchCycleV1, ...]
    dispositions: tuple[CandidateDispositionV1, ...]
    exposures: tuple[ProtectedExposureV1, ...]
    lifecycle: tuple[ExperimentLifecycleV1, ...]
    comparisons: tuple[ExperimentComparisonV1, ...]
    promotions: tuple[PromotionBindingV1, ...]
    reports: tuple[ExperimentReportV1, ...]

    def _validate(self) -> None:
        payload = self.to_payload()
        payload["fixtures"] = [
            {
                "reference": fixture.reference,
                "payload": load_json(fixture.payload_json),
            }
            for fixture in self.fixtures
        ]
        canonical_json(
            {"schema_version": self.schema_version(), "payload": payload}
        )
        RegistryView(self).validate()


class RegistryView:
    """Process-local indexes, built once per closed operation; no stale cache."""

    def __init__(self, registry: ExperimentRegistryV1) -> None:
        self.registry = registry
        self.bundles = _index(registry.bundles)
        self.metric_registries = _index(registry.metric_registries)
        self.fixtures = _index(registry.fixtures)
        self.attempts = _index(registry.attempts)
        self.result_artifacts = _index(registry.results)
        self.receipts = _index(registry.result_receipts)
        self.cycle_artifacts = _index(registry.cycles)
        self.disposition_artifacts = _index(registry.dispositions)
        self.exposures = _index(registry.exposures)
        self.lifecycle = _index(registry.lifecycle)
        self.comparisons = _index(registry.comparisons)
        self.promotions = _index(registry.promotions)
        self.reports = _index(registry.reports)
        self.experiments = {b.experiment_id: b for b in registry.bundles}
        self.results = {r.result_id: r for r in registry.results}
        self.cycles = {c.cycle_id: c for c in registry.cycles}
        self.dispositions = {d.experiment_id: d for d in registry.dispositions}
        if (
            len(self.results) != len(registry.results)
            or len(self.cycles) != len(registry.cycles)
            or len(self.dispositions) != len(registry.dispositions)
        ):
            raise ValueError("duplicate semantic registry identity")
        self.metrics = {}
        self.allowed_metrics: dict[str, set[str]] = {}
        for registry_id, metrics in self.metric_registries.items():
            metric_index = {
                metric.artifact_id: metric for metric in metrics.metrics
            }
            self.metrics.update(metric_index)
            self.allowed_metrics[registry_id] = set(metric_index)
        self.result_times: dict[str, int] = {}
        self.parents: dict[str, set[str]] = {}
        self.cycle_parents = {
            c.cycle_id: set(c.predecessor_cycle_ids) for c in registry.cycles
        }
        self.histories: dict[str, list[ExperimentLifecycleV1]] = {}

    def validate(self) -> None:
        try:
            self._inputs()
            self._search()
            self._attempts_results()
            self._lifecycle()
            self._exposure()
            self._comparisons_reports()
            for promotion in self.registry.promotions:
                # Old promotions remain readable after later invalidation.
                self.assert_promotion(promotion, at_ns=promotion.recorded_at_ns)
        except KeyError as exc:
            raise ValueError(
                "unresolved experiment registry reference"
            ) from exc

    def _inputs(self) -> None:
        retained = {f.reference.native_id: f for f in self.registry.fixtures}
        if len(retained) != len(self.fixtures):
            raise ValueError("duplicate retained fixture native identity")
        references: dict[str, str] = {}
        used: set[str] = set()
        all_references: list[ArtifactReferenceV1] = []
        for bundle in self.registry.bundles:
            science = bundle.scientific_inputs
            self.metric_registries[science.metric_registry_id]
            lineage = next(
                c
                for c in science.components
                if c.field is ComponentField.DATASET_LINEAGE
            )
            if not set(science.protected_evaluation_roots) <= {
                r.native_id for r in lineage.references
            }:
                raise ValueError(
                    "protected roots must bind exact dataset lineage references"
                )
            all_references.extend(science.references())
        for attempt in self.registry.attempts:
            all_references.extend(
                (
                    attempt.runtime_identity,
                    *attempt.logs,
                    *attempt.checkpoints,
                    *attempt.produced_artifacts,
                )
            )
        for promotion in self.registry.promotions:
            all_references.extend(
                (promotion.deployment, promotion.promotion_policy)
            )
        for ref in all_references:
            value = canonical_json(ref)
            if (
                ref.native_id in references
                and references[ref.native_id] != value
            ):
                raise ValueError(
                    "native reference identity has conflicting content"
                )
            references[ref.native_id] = value
            if ref.native_id in self.results:
                if ref != result_reference(
                    self.results[ref.native_id], ref.evidence_kind
                ):
                    raise ValueError(
                        "produced result reference content mismatch"
                    )
                continue
            if ref.evidence_kind is EvidenceKind.SYNTHETIC_FIXTURE:
                if retained[ref.native_id].reference != ref:
                    raise ValueError(
                        "fixture reference does not match retained bytes"
                    )
                used.add(ref.native_id)
        if used != set(retained):
            raise ValueError("unbound retained fixture evidence")

    def _search(self) -> None:
        _acyclic(self.cycle_parents)
        for cycle in self.registry.cycles:
            expected_roots: set[str] = set()
            expected_hashes: set[str] = set()
            if (
                tuple(
                    sorted(
                        self.cycles[p].search_plan.artifact_id
                        for p in cycle.predecessor_cycle_ids
                    )
                )
                != cycle.search_plan.predecessor_plan_ids
            ):
                raise ValueError("search predecessor plan identity mismatch")
            for parent in cycle.predecessor_cycle_ids:
                if self.cycles[parent].declared_at_ns >= cycle.declared_at_ns:
                    raise ValueError(
                        "successor cycle must follow predecessor declaration"
                    )
            for candidate in cycle.candidates:
                bundle = self.experiments[candidate.experiment_id]
                if (
                    bundle.scientific_inputs.research_cycle_id != cycle.cycle_id
                    or candidate.experiment_id in self.parents
                ):
                    raise ValueError("candidate cycle ownership mismatch")
                if (
                    bundle.scientific_inputs.search_plan_id
                    != cycle.search_plan.artifact_id
                ):
                    raise ValueError(
                        "experiment bound to different frozen search plan"
                    )
                expected_roots.update(
                    bundle.scientific_inputs.protected_evaluation_roots
                )
                expected_hashes.update(
                    ref.sha256
                    for component in bundle.scientific_inputs.components
                    if component.field is ComponentField.DATASET_LINEAGE
                    for ref in component.references
                    if ref.native_id
                    in bundle.scientific_inputs.protected_evaluation_roots
                )
                self.parents[candidate.experiment_id] = set(
                    candidate.parent_experiment_ids
                )
                disposition = self.dispositions[candidate.experiment_id]
                if disposition.cycle_id != cycle.cycle_id:
                    raise ValueError("candidate disposition cycle mismatch")
            if expected_roots != set(cycle.protected_roots):
                raise ValueError("cycle protected root inventory mismatch")
            if expected_hashes != set(
                cycle.search_plan.protected_content_sha256
            ):
                raise ValueError(
                    "protected content roots differ from frozen search plan"
                )
        if set(self.parents) != set(self.experiments) or set(
            self.dispositions
        ) != set(self.experiments):
            raise ValueError(
                "complete declared candidate/disposition inventory required"
            )
        _acyclic(self.parents)
        for experiment, parents in self.parents.items():
            child_cycle = self.experiments[
                experiment
            ].scientific_inputs.research_cycle_id
            for parent in parents:
                previous = self.experiments[
                    parent
                ].scientific_inputs.research_cycle_id
                if previous != child_cycle and previous not in _ancestors(
                    child_cycle, self.cycle_parents
                ):
                    raise ValueError(
                        "candidate ancestor outside declared research cycles"
                    )

    def _attempts_results(self) -> None:
        retry_edges: dict[str, set[str]] = {}
        labels: dict[tuple[str, str], list[ExperimentAttemptV1]] = {}
        state_children: set[str] = set()
        for attempt_id, attempt in self.attempts.items():
            bundle = self.bundles[attempt.bundle_id]
            if (
                bundle.experiment_id != attempt.experiment_id
                or attempt.runtime_identity
                != bundle.scientific_inputs.environment.runtime
            ):
                raise ValueError("attempt changed scientific inputs/runtime")
            cycle = self.cycles[bundle.scientific_inputs.research_cycle_id]
            if attempt.started_at_ns < cycle.declared_at_ns:
                raise ValueError("attempt precedes declared search inventory")
            key = (attempt.experiment_id, attempt.attempt_label)
            labels.setdefault(key, []).append(attempt)
            if attempt.previous_state_id is not None:
                previous_state = self.attempts[attempt.previous_state_id]
                if attempt.previous_state_id in state_children:
                    raise ValueError("forked attempt state")
                state_children.add(attempt.previous_state_id)
                before, after = (
                    previous_state.to_payload(),
                    attempt.to_payload(),
                )
                for name in (
                    "status",
                    "ended_at_ns",
                    "reason",
                    "logs",
                    "checkpoints",
                    "produced_artifacts",
                    "previous_state_id",
                ):
                    before.pop(name)
                    after.pop(name)
                if (
                    previous_state.status is not AttemptStatus.RUNNING
                    or before != after
                    or not set(previous_state.logs) <= set(attempt.logs)
                    or not set(previous_state.checkpoints)
                    <= set(attempt.checkpoints)
                    or not set(previous_state.produced_artifacts)
                    <= set(attempt.produced_artifacts)
                ):
                    raise ValueError(
                        "attempt state changed immutable inputs or lost outputs"
                    )
            retry_edges[attempt_id] = set()
            if attempt.retry_of_attempt_id is not None:
                previous = self.attempts[attempt.retry_of_attempt_id]
                if (
                    previous.experiment_id != attempt.experiment_id
                    or previous.ended_at_ns is None
                    or previous.ended_at_ns > attempt.started_at_ns
                ):
                    raise ValueError(
                        "retry changes experiment or precedes prior termination"
                    )
                retry_edges[attempt_id].add(attempt.retry_of_attempt_id)
            if attempt.previous_state_id is not None:
                retry_edges[attempt_id].add(attempt.previous_state_id)
        for states in labels.values():
            roots = [a for a in states if a.previous_state_id is None]
            if len(roots) != 1 or len(states) > 2:
                raise ValueError(
                    "attempt label reused without exact state chain"
                )
        _acyclic(retry_edges)
        receipted: set[str] = set()
        for result_id, result in self.results.items():
            bundle = self.experiments[result.experiment_id]
            if (
                result.metric_id
                not in self.allowed_metrics[
                    bundle.scientific_inputs.metric_registry_id
                ]
                or result.stratum_id
                not in bundle.scientific_inputs.reporting_strata
            ):
                raise ValueError(
                    "result metric/stratum outside scientific inputs"
                )
            self.metrics[result.metric_id]
        for receipt in self.registry.result_receipts:
            result = self.results[receipt.result_id]
            attempt = self.attempts[receipt.attempt_id]
            if (
                attempt.experiment_id != result.experiment_id
                or attempt.status is not AttemptStatus.COMPLETED
            ):
                raise ValueError(
                    "result requires matching completed producer attempt"
                )
            if (
                receipt.artifact
                != result_reference(result, receipt.artifact.evidence_kind)
                or receipt.artifact not in attempt.produced_artifacts
            ):
                raise ValueError(
                    "result bytes not retained in producer output inventory"
                )
            receipted.add(receipt.result_id)
            assert attempt.ended_at_ns is not None
            self.result_times[receipt.result_id] = min(
                self.result_times.get(receipt.result_id, attempt.ended_at_ns),
                attempt.ended_at_ns,
            )
        if receipted != set(self.results):
            raise ValueError("result lacks producer receipt")
        used_attempts: set[str] = set()
        used_results: set[str] = set()
        for disposition in self.registry.dispositions:
            for attempt_id in disposition.attempt_ids:
                if (
                    self.attempts[attempt_id].experiment_id
                    != disposition.experiment_id
                ):
                    raise ValueError("disposition attempt ownership mismatch")
                used_attempts.add(attempt_id)
            for result_id in disposition.result_ids:
                if (
                    self.results[result_id].experiment_id
                    != disposition.experiment_id
                ):
                    raise ValueError("disposition result ownership mismatch")
                used_results.add(result_id)
            outcome_states = {
                self.attempts[a].status for a in disposition.attempt_ids
            }
            if disposition.outcome is CandidateOutcome.FAILED and (
                AttemptStatus.FAILED not in outcome_states
                or AttemptStatus.COMPLETED in outcome_states
            ):
                raise ValueError(
                    "failed disposition contradicts retained attempts"
                )
            if (
                disposition.outcome
                in (CandidateOutcome.UNFAVORABLE, CandidateOutcome.FAVORABLE)
                and AttemptStatus.COMPLETED not in outcome_states
            ):
                raise ValueError(
                    "evaluated disposition lacks completed attempt"
                )
        if used_attempts != set(self.attempts) or used_results != set(
            self.results
        ):
            raise ValueError(
                "search ledger omitted retained failed/unfavorable work"
            )

    def _lifecycle(self) -> None:
        allowed = {
            ExperimentStatus.PLANNED: {
                ExperimentStatus.RUNNING,
                ExperimentStatus.FAILED,
                ExperimentStatus.INVALIDATED,
                ExperimentStatus.SUPERSEDED,
            },
            ExperimentStatus.RUNNING: {
                ExperimentStatus.COMPLETED,
                ExperimentStatus.FAILED,
                ExperimentStatus.INVALIDATED,
            },
            ExperimentStatus.FAILED: {
                ExperimentStatus.RUNNING,
                ExperimentStatus.INVALIDATED,
                ExperimentStatus.SUPERSEDED,
            },
            ExperimentStatus.COMPLETED: {
                ExperimentStatus.INVALIDATED,
                ExperimentStatus.SUPERSEDED,
            },
            ExperimentStatus.INVALIDATED: set(),
            ExperimentStatus.SUPERSEDED: {ExperimentStatus.INVALIDATED},
        }
        by_previous: set[str] = set()
        replaced_states = {
            a.previous_state_id
            for a in self.registry.attempts
            if a.previous_state_id is not None
        }
        leaves_by_experiment: dict[str, list[ExperimentAttemptV1]] = {}
        for attempt_id, attempt in self.attempts.items():
            if attempt_id not in replaced_states:
                leaves_by_experiment.setdefault(
                    attempt.experiment_id, []
                ).append(attempt)
        first_result_by_experiment: dict[str, int] = {}
        for result_id, result in self.results.items():
            produced_at = self.result_times[result_id]
            first_result_by_experiment[result.experiment_id] = min(
                first_result_by_experiment.get(
                    result.experiment_id, produced_at
                ),
                produced_at,
            )
        for event_id, event in self.lifecycle.items():
            self.experiments[event.experiment_id]
            history = self.histories.setdefault(event.experiment_id, [])
            history.append(event)
            if event.successor_experiment_id is not None:
                self.experiments[event.successor_experiment_id]
                if event.experiment_id not in _ancestors(
                    event.successor_experiment_id, self.parents
                ):
                    raise ValueError(
                        "successor must retain original experiment ancestry"
                    )
            if event.previous_event_id is None:
                if event.status is not ExperimentStatus.PLANNED:
                    raise ValueError("lifecycle must begin planned")
            else:
                if event.previous_event_id in by_previous:
                    raise ValueError("forked lifecycle history")
                by_previous.add(event.previous_event_id)
                previous = self.lifecycle[event.previous_event_id]
                if (
                    previous.experiment_id != event.experiment_id
                    or previous.recorded_at_ns >= event.recorded_at_ns
                    or event.status not in allowed[previous.status]
                ):
                    raise ValueError("invalid lifecycle transition")
            attempts = leaves_by_experiment.get(event.experiment_id, [])
            expected = {
                ExperimentStatus.RUNNING: None,
                ExperimentStatus.FAILED: AttemptStatus.FAILED,
                ExperimentStatus.COMPLETED: AttemptStatus.COMPLETED,
            }
            if event.status in expected:
                matching = [
                    a
                    for a in attempts
                    if a.started_at_ns <= event.recorded_at_ns
                    and (
                        (
                            expected[event.status] is None
                            and (
                                a.ended_at_ns is None
                                or event.recorded_at_ns < a.ended_at_ns
                            )
                        )
                        or (
                            a.status is expected[event.status]
                            and a.ended_at_ns is not None
                            and a.ended_at_ns <= event.recorded_at_ns
                        )
                    )
                ]
                if not matching:
                    raise ValueError(
                        "lifecycle status lacks matching execution evidence"
                    )
            if event.status is ExperimentStatus.COMPLETED and (
                event.experiment_id not in first_result_by_experiment
                or first_result_by_experiment[event.experiment_id]
                > event.recorded_at_ns
            ):
                raise ValueError(
                    "completed experiment has no retained result as of completion"
                )
        if set(self.histories) != set(self.experiments):
            raise ValueError("experiment lifecycle history missing")
        for history in self.histories.values():
            history.sort(key=lambda event: event.recorded_at_ns)
            if sum(event.previous_event_id is None for event in history) != 1:
                raise ValueError("multiple lifecycle roots")
            for before, after in zip(history, history[1:]):
                if after.previous_event_id != before.artifact_id:
                    raise ValueError("disconnected lifecycle history")

    def status(
        self, experiment_id: str, *, at_ns: int | None = None
    ) -> ExperimentStatus | None:
        history = self.histories[experiment_id]
        eligible = [
            event
            for event in history
            if at_ns is None or event.recorded_at_ns <= at_ns
        ]
        return eligible[-1].status if eligible else None

    def result_available_at(self, result_id: str, at_ns: int) -> bool:
        ended = self.result_times.get(result_id)
        return ended is not None and ended <= at_ns

    def _exposure(self) -> None:
        for exposure in self.registry.exposures:
            source = self.experiments[exposure.experiment_id]
            if (
                exposure.protected_root
                not in source.scientific_inputs.protected_evaluation_roots
            ):
                raise ValueError("exposure root not bound to experiment")
            for result_id in exposure.inspected_result_ids:
                if (
                    self.results[result_id].experiment_id
                    != source.experiment_id
                ):
                    raise ValueError("exposure result ownership mismatch")
                producers = [
                    self.attempts[r.attempt_id]
                    for r in self.registry.result_receipts
                    if r.result_id == result_id
                ]
                if not any(
                    a.ended_at_ns is not None
                    and a.ended_at_ns <= exposure.inspected_at_ns
                    for a in producers
                ):
                    raise ValueError("exposure precedes retained result")
            for target_id in exposure.redesigned_experiment_ids:
                target = self.experiments[target_id]
                cycle_id = target.scientific_inputs.research_cycle_id
                old_cycle = source.scientific_inputs.research_cycle_id
                if (
                    old_cycle == cycle_id
                    or old_cycle not in _ancestors(cycle_id, self.cycle_parents)
                    or self.cycles[cycle_id].declared_at_ns
                    <= exposure.inspected_at_ns
                    or source.experiment_id
                    not in _ancestors(target_id, self.parents)
                ):
                    raise ValueError(
                        "protected redesign requires successor research cycle and ancestry"
                    )
                affected = {target_id} | {
                    candidate
                    for candidate in self.parents
                    if target_id in _ancestors(candidate, self.parents)
                }
                exposed_hashes = {
                    ref.sha256
                    for component in source.scientific_inputs.components
                    if component.field
                    in (
                        ComponentField.DATASET_LINEAGE,
                        ComponentField.SPLIT_ARTIFACT,
                    )
                    for ref in component.references
                    if ref.native_id == exposure.protected_root
                    or component.field is ComponentField.SPLIT_ARTIFACT
                }
                if any(
                    exposed_hashes
                    & {
                        ref.sha256
                        for component in self.experiments[
                            candidate
                        ].scientific_inputs.components
                        if component.field
                        in (
                            ComponentField.DATASET_LINEAGE,
                            ComponentField.SPLIT_ARTIFACT,
                            ComponentField.DATASET_PRODUCTS,
                        )
                        for ref in component.references
                    }
                    for candidate in affected
                ):
                    raise ValueError(
                        "inspected protected root reused for successor evaluation"
                    )

    def _comparisons_reports(self) -> None:
        for comparison in self.registry.comparisons:
            left = self.experiments[comparison.left_experiment_id]
            right = self.experiments[comparison.right_experiment_id]
            if (
                scientific_differences(left, right)
                != comparison.declared_differences
            ):
                raise ValueError(
                    "declared ablation differences do not match scientific inputs"
                )
            if (
                comparison.left_result_id is not None
                and comparison.right_result_id is not None
            ):
                a, b = (
                    self.results[comparison.left_result_id],
                    self.results[comparison.right_result_id],
                )
                if (
                    a.experiment_id != left.experiment_id
                    or b.experiment_id != right.experiment_id
                    or a.metric_id != b.metric_id
                    or a.stratum_id != b.stratum_id
                ):
                    raise ValueError(
                        "comparison results lack shared metric/stratum and ownership"
                    )
        for report in self.registry.reports:
            for result in report.result_ids:
                self.results[result]
            for comparison_id in report.comparison_ids:
                comparison = self.comparisons[comparison_id]
                if (
                    comparison.left_result_id not in report.result_ids
                    or comparison.right_result_id not in report.result_ids
                ):
                    raise ValueError(
                        "report comparison results absent from exact citation inventory"
                    )

    def assert_promotion(
        self, promotion: PromotionBindingV1, *, at_ns: int | None = None
    ) -> None:
        try:
            cited = {
                self.results[r].experiment_id
                for r in promotion.qualifying_result_ids
            }
            if cited != set(promotion.qualifying_experiment_ids):
                raise ValueError(
                    "promotion lacks exact qualifying experiment/result context"
                )
            for result_id in promotion.qualifying_result_ids:
                result = self.results[result_id]
                if not self.result_available_at(
                    result_id, promotion.recorded_at_ns
                ):
                    raise ValueError(
                        "promotion precedes exact result producer evidence"
                    )
                if result.payload.state is not ResultState.AVAILABLE:
                    raise ValueError(
                        "unavailable result cannot qualify a promotion binding"
                    )
            for experiment in promotion.qualifying_experiment_ids:
                if (
                    self.status(experiment, at_ns=at_ns)
                    is not ExperimentStatus.COMPLETED
                ):
                    raise ValueError("promotion experiment not completed/valid")
                cycle_id = self.experiments[
                    experiment
                ].scientific_inputs.research_cycle_id
                cycle_scope = {cycle_id} | _ancestors(
                    cycle_id, self.cycle_parents
                )
                candidates = {
                    c.experiment_id
                    for cycle in cycle_scope
                    for c in self.cycles[cycle].candidates
                }
                for candidate in candidates:
                    if (
                        self.dispositions[candidate].outcome
                        is CandidateOutcome.NOT_EXECUTED
                    ):
                        raise ValueError(
                            "promotion search inventory still unexecuted"
                        )
                    if self.status(candidate, at_ns=at_ns) in (
                        None,
                        ExperimentStatus.PLANNED,
                        ExperimentStatus.RUNNING,
                        ExperimentStatus.INVALIDATED,
                    ):
                        raise ValueError(
                            "promotion depends on unfinished/invalidated search lineage"
                        )
                    disposition = self.dispositions[candidate]
                    replaced = {
                        self.attempts[a].previous_state_id
                        for a in disposition.attempt_ids
                    }
                    leaves = [
                        self.attempts[a]
                        for a in disposition.attempt_ids
                        if a not in replaced
                        and self.attempts[a].started_at_ns
                        <= promotion.recorded_at_ns
                    ]
                    if any(
                        a.ended_at_ns is None
                        or a.ended_at_ns > promotion.recorded_at_ns
                        for a in leaves
                    ):
                        raise ValueError(
                            "promotion precedes complete declared search evidence"
                        )
            for comparison_id in promotion.comparison_ids:
                comparison = self.comparisons[comparison_id]
                if not {
                    comparison.left_experiment_id,
                    comparison.right_experiment_id,
                } <= set(promotion.qualifying_experiment_ids):
                    raise ValueError(
                        "promotion comparison outside qualifying experiments"
                    )
                if not {
                    comparison.left_result_id,
                    comparison.right_result_id,
                } <= set(promotion.qualifying_result_ids):
                    raise ValueError(
                        "promotion comparison lacks exact qualifying results"
                    )
        except KeyError as exc:
            raise ValueError(
                "unresolved promotion qualification context"
            ) from exc


def assert_promotion_admissible(
    registry: ExperimentRegistryV1, promotion: PromotionBindingV1
) -> None:
    """Check CURRENT declared lineage; never certify deployment or private merit."""
    view = RegistryView(registry)
    view.validate()
    if (
        promotion.artifact_id not in view.promotions
        or view.promotions[promotion.artifact_id] != promotion
    ):
        raise ValueError("promotion must be retained in the validated registry")
    view.assert_promotion(promotion)


def reverse_dependencies(
    registry: ExperimentRegistryV1, artifact_id: str
) -> tuple[str, ...]:
    """Transitive dependents, including historical reports/deployment bindings."""
    view = RegistryView(registry)
    view.validate()
    edges: dict[str, set[str]] = {}
    for fixture_id, fixture in view.fixtures.items():
        edges[fixture.reference.native_id] = {fixture_id}
    for experiment, bundle in view.experiments.items():
        edges[experiment] = (
            set(view.parents[experiment])
            | {
                bundle.scientific_inputs.metric_registry_id,
                bundle.scientific_inputs.search_plan_id,
            }
            | {r.native_id for r in bundle.scientific_inputs.references()}
        )
    for bundle_id, bundle in view.bundles.items():
        edges[bundle_id] = {bundle.experiment_id}
    for attempt_id, attempt in view.attempts.items():
        edges[attempt_id] = {attempt.experiment_id, attempt.bundle_id}
    for result_id, result in view.results.items():
        edges[result_id] = {result.experiment_id, result.metric_id}
    for receipt_id, receipt in view.receipts.items():
        edges[receipt_id] = {receipt.result_id, receipt.attempt_id}
        edges[receipt.result_id].add(receipt.attempt_id)
    for cycle in registry.cycles:
        edges[cycle.search_plan.artifact_id] = set(
            cycle.search_plan.predecessor_plan_ids
        ) | set(cycle.search_plan.protected_content_sha256)
        edges[cycle.artifact_id] = (
            {c.experiment_id for c in cycle.candidates}
            | {view.cycles[c].artifact_id for c in cycle.predecessor_cycle_ids}
            | {cycle.search_plan.artifact_id}
        )
    for comparison_id, comparison in view.comparisons.items():
        edges[comparison_id] = {
            comparison.left_experiment_id,
            comparison.right_experiment_id,
        } | {
            r
            for r in (comparison.left_result_id, comparison.right_result_id)
            if r is not None
        }
    for report_id, report in view.reports.items():
        edges[report_id] = set(report.result_ids) | set(report.comparison_ids)
    for promotion_id, promotion in view.promotions.items():
        edges[promotion_id] = (
            set(promotion.qualifying_experiment_ids)
            | set(promotion.qualifying_result_ids)
            | set(promotion.comparison_ids)
            | {
                view.cycles[
                    view.experiments[e].scientific_inputs.research_cycle_id
                ].artifact_id
                for e in promotion.qualifying_experiment_ids
            }
        )
    for registry_id, metrics in view.metric_registries.items():
        edges[registry_id] = {m.artifact_id for m in metrics.metrics}
    known = set(edges) | {
        parent for parents in edges.values() for parent in parents
    }
    if artifact_id not in known:
        raise ValueError("unknown reverse-lineage root")
    reverse: dict[str, set[str]] = {}
    for dependent, parents in edges.items():
        for parent in parents:
            reverse.setdefault(parent, set()).add(dependent)
    return tuple(sorted(_ancestors(artifact_id, reverse) - {artifact_id}))
