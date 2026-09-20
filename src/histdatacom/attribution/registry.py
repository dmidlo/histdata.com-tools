"""Closed public explanation graph and explicit existing718 sidecar bridge."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, TYPE_CHECKING

if TYPE_CHECKING:
    from histdatacom.experiments import RetainedFixtureV1

from ._wire import Artifact, Record, canonical_json, ordered, text
from .contracts import (
    AttributionEvidenceKind,
    AttributionReferenceV1,
    AttributionState,
    DecisionAttributionV1,
    ExplanationPlanV1,
    ExplanationPolicyRegistryV1,
    reference,
)
from .decisions import ReferenceDecisionV1


@dataclass(frozen=True, slots=True)
class AttributionExperimentLinkV1(Record):
    attribution_id: str
    plan_id: str
    experiment_id: str
    result_ids: tuple[str, ...]
    action_id: str | None
    linked_at_ns: int
    role: str = "posthoc_explanation"

    def _validate(self) -> None:
        for value in (self.attribution_id, self.plan_id, self.experiment_id):
            text(value)
        ordered(self.result_ids)
        if self.linked_at_ns < 0:
            raise ValueError("negative sidecar clock")
        if self.role not in ("posthoc_explanation", "scientific_result"):
            raise ValueError("unknown attribution result role")
        if self.role == "scientific_result" and not self.result_ids:
            raise ValueError("scientific result link needs produced results")


@dataclass(frozen=True, slots=True)
class AttributionRegistryV1(Artifact):
    """Retained typed references are not authentication of private artifacts."""

    KIND: ClassVar[str] = "registry"
    policy_registry: ExplanationPolicyRegistryV1
    plans: tuple[ExplanationPlanV1, ...]
    attributions: tuple[DecisionAttributionV1, ...]
    actions: tuple[ReferenceDecisionV1, ...]
    links: tuple[AttributionExperimentLinkV1, ...]
    experiment_registry_json: str | None

    def _validate(self) -> None:
        for values in (self.plans, self.attributions, self.actions):
            ordered(tuple(v.artifact_id for v in values))
        plans = {p.artifact_id: p for p in self.plans}
        policies = {p.artifact_id for p in self.policy_registry.policies}
        attrs = {a.artifact_id: a for a in self.attributions}
        actions = {a.artifact_id: a for a in self.actions}
        for attribution in self.attributions:
            if attribution.policy.artifact_id not in policies:
                raise ValueError("unregistered explanation policy")
        for action in self.actions:
            matches = [
                a
                for a in self.attributions
                if a.model_reference == action.model
                and reference(a.snapshot) == action.snapshot
                and a.raw_output == action.raw_score
                and a.snapshot.cutoff_at_ns == action.decision_at_ns
            ]
            if not matches:
                raise ValueError(
                    "action lacks an actually replayed model/snapshot score"
                )
        if len(
            {(link.attribution_id, link.experiment_id) for link in self.links}
        ) != len(self.links):
            raise ValueError("duplicate experiment explanation sidecar")
        for link in self.links:
            if link.attribution_id not in attrs or link.plan_id not in plans:
                raise ValueError("missing attribution/plan ancestor")
            attribution, plan = attrs[link.attribution_id], plans[link.plan_id]
            if (
                plan.model != attribution.model_reference
                or plan.policy != reference(attribution.policy)
                or plan.background != reference(attribution.background)
                or plan.preprocessing != attribution.snapshot.preprocessing
                or plan.projection != attribution.snapshot.projection
            ):
                raise ValueError(
                    "sidecar plan differs from replayed attribution inputs"
                )
            if (
                not max(plan.declared_at_ns, attribution.generated_at_ns)
                <= link.linked_at_ns
            ):
                raise ValueError("sidecar predates retained evidence")
            if plan.declared_at_ns < max(
                attribution.policy.declared_at_ns,
                attribution.background.declared_at_ns,
            ):
                raise ValueError(
                    "plan claims declaration before its policy/background"
                )
            if link.action_id is not None:
                if link.action_id not in actions:
                    raise ValueError("missing exact original action")
                action = actions[link.action_id]
                if (
                    action.model != attribution.model_reference
                    or action.snapshot != reference(attribution.snapshot)
                    or action.raw_score != attribution.raw_output
                    or action.decision_at_ns
                    != attribution.snapshot.cutoff_at_ns
                    or action.decision_at_ns > attribution.generated_at_ns
                ):
                    raise ValueError(
                        "action model/schema/preprocessing/clock differs from explanation"
                    )
        if self.experiment_registry_json is None:
            if self.links:
                raise ValueError(
                    "experiment sidecars require actual retained registry"
                )
        else:
            _validate_experiment_bridge(self)


def explanation_plan_fixture(plan: ExplanationPlanV1) -> RetainedFixtureV1:
    """Exact method configuration, explicitly not a fitted model receipt.

    Include its reference in MODEL_CONFIGURATION before result production.
    The plan contains no experiment/result/decision identity, avoiding cycles.
    """
    from histdatacom.experiments import fixture_specification

    return fixture_specification(
        "public-attribution-method-plan.v1", plan.to_dict()
    )


def _validate_experiment_bridge(registry: AttributionRegistryV1) -> None:
    from histdatacom.experiments import ComponentField, ExperimentRegistryV1
    from histdatacom.experiments.lineage import RegistryView
    from ._wire import load_json

    assert registry.experiment_registry_json is not None
    # Charge parsed composition before the existing reader reconstructs a graph.
    parsed = load_json(registry.experiment_registry_json)
    canonical_json({"registry": registry.to_payload(), "experiment": parsed})
    original = ExperimentRegistryV1.from_json(registry.experiment_registry_json)
    if original.to_json() != registry.experiment_registry_json:
        raise ValueError("noncanonical native experiment registry")
    view = RegistryView(original)
    view.validate()
    plans = {p.artifact_id: p for p in registry.plans}
    attrs = {a.artifact_id: a for a in registry.attributions}
    for link in registry.links:
        try:
            bundle = view.experiments[link.experiment_id]
            plan = plans[link.plan_id]
            attribution = attrs[link.attribution_id]
            expected = explanation_plan_fixture(plan)
            configuration = next(
                c
                for c in bundle.scientific_inputs.components
                if c.field is ComponentField.MODEL_CONFIGURATION
            )
            if (
                expected.reference not in configuration.references
                or expected not in original.fixtures
            ):
                raise ValueError(
                    "experiment lacks exact pre-result explanation plan configuration"
                )
            # The bridge compares exact native model/preprocessing references,
            # not generic native-ID strings or the fixture name alone.
            for field, ref in (
                (ComponentField.PREPROCESSING, plan.preprocessing),
                (ComponentField.FEATURE_PROJECTION, plan.projection),
                (ComponentField.MODEL_WEIGHTS, plan.model),
            ):
                candidates = next(
                    c.references
                    for c in bundle.scientific_inputs.components
                    if c.field is field
                )
                if not any(
                    _same_reference(ref, r.to_payload()) for r in candidates
                ):
                    raise ValueError(
                        "experiment scientific stack differs from attribution plan"
                    )
            for result_id in link.result_ids:
                result = view.results[result_id]
                produced = view.result_times[result_id]
                if (
                    result.experiment_id != link.experiment_id
                    or produced
                    < max(
                        plan.declared_at_ns,
                        attribution.policy.declared_at_ns,
                        attribution.background.declared_at_ns,
                    )
                    or produced > link.linked_at_ns
                ):
                    raise ValueError(
                        "result/plan/explanation as-of ordering differs"
                    )
                if (
                    link.role == "scientific_result"
                    and produced < attribution.generated_at_ns
                ):
                    raise ValueError(
                        "scientific result predates cited attribution"
                    )
            if (
                attribution.evidence_kind is AttributionEvidenceKind.DECLARED
                and attribution.state is not AttributionState.DECLARED
            ):
                raise ValueError(
                    "external result cannot acquire fixture qualification"
                )
        except KeyError as exc:
            raise ValueError("unresolved experiment sidecar evidence") from exc


def _same_reference(
    ref: AttributionReferenceV1, payload: dict[str, object]
) -> bool:
    return all(
        payload.get(name) == getattr(ref, name)
        for name in ("schema", "native_id", "sha256", "byte_length")
    )


def reverse_attribution_dependencies(
    registry: AttributionRegistryV1, identity: str
) -> tuple[str, ...]:
    """Exact graph references, not discovery of undisclosed external work."""
    registry = AttributionRegistryV1.from_json(registry.to_json())
    found = set()
    for attribution in registry.attributions:
        refs = (
            attribution.model_reference.native_id,
            attribution.snapshot.artifact_id,
            attribution.policy.artifact_id,
            attribution.background.artifact_id,
            attribution.snapshot.preprocessing.native_id,
            attribution.snapshot.projection.native_id,
            attribution.snapshot.lineage.native_id,
        )
        if identity == attribution.artifact_id or identity in refs:
            found.add(attribution.artifact_id)
    for link in registry.links:
        if link.attribution_id in found or identity in (
            link.plan_id,
            link.experiment_id,
            link.action_id,
        ):
            found.update(
                (link.attribution_id, link.experiment_id, *link.result_ids)
            )
            if link.action_id is not None:
                found.add(link.action_id)
    return tuple(sorted(found))
