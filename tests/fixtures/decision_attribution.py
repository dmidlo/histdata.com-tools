"""Executed tiny public models; no fitted/private/historical model evidence."""

from dataclasses import replace

from histdatacom.attribution.contracts import (
    AttributionBackgroundV1,
    AttributionEvidenceKind,
    AttributionFeatureV1,
    AttributionGroupV1,
    AttributionReferenceV1,
    AttributionSnapshotV1,
    AttributionValueV1,
    ExplanationPolicyRegistryV1,
    ExplanationPolicyV1,
    FeatureSpace,
    GroupMethod,
    PolynomialTermV1,
    ReferenceModelV1,
)
from histdatacom.attribution.reference import explain_reference
from histdatacom.attribution.registry import AttributionRegistryV1


def declared_reference(name="fixture-metadata"):
    return AttributionReferenceV1(
        "synthetic-fixture-metadata.v1",
        name,
        "a" * 64,
        1,
        AttributionEvidenceKind.DECLARED,
    )


def reference_inputs():
    features = (
        AttributionFeatureV1(
            "x1", "market.tick", "fixture.x1.v1", "dimensionless"
        ),
        AttributionFeatureV1(
            "x2", "market.tick", "fixture.x2.v1", "dimensionless"
        ),
    )
    snapshot = AttributionSnapshotV1(
        tuple(
            AttributionValueV1(feature, 1.0, "synthetic-source", 10)
            for feature in features
        ),
        10,
        FeatureSpace.RAW,
        declared_reference("preprocessing"),
        declared_reference("projection"),
        declared_reference("lineage"),
        ("EURUSD",),
        "synthetic-session",
        "synthetic_fixture",
    )
    baseline = replace(
        snapshot,
        values=tuple(
            replace(value, value=0.0, available_at_ns=1)
            for value in snapshot.values
        ),
        cutoff_at_ns=1,
    )
    background = AttributionBackgroundV1((baseline,), 2, "synthetic_fixture")
    policy = ExplanationPolicyV1(
        "1.0.0",
        "exact-polynomial-coalitions.v1",
        GroupMethod.SUM,
        (
            AttributionGroupV1("all", None, ("x1", "x2"), "plane"),
            AttributionGroupV1("first", "all", ("x1",), "correlated_cluster"),
            AttributionGroupV1("second", "all", ("x2",), "correlated_cluster"),
        ),
        ("first", "second"),
        1e-12,
        1e-12,
        10.0,
        65536,
        "background-range-and-sibling-ablation.v1",
        2,
    )
    model = ReferenceModelV1(
        ("x1", "x2"),
        (
            PolynomialTermV1(3.0, ("x1",)),
            PolynomialTermV1(2.0, ("x2",)),
            PolynomialTermV1(4.0, ("x1", "x2")),
        ),
        "fixture.response",
        "dimensionless",
    )
    return model, policy, snapshot, background


def example_attribution():
    return explain_reference(*reference_inputs(), generated_at_ns=20)


def example_registry():
    attribution = example_attribution()
    return AttributionRegistryV1(
        ExplanationPolicyRegistryV1("1.0.0", (attribution.policy,)),
        (),
        (attribution,),
        (),
        (),
        None,
    )


def linked_example_registry(*, generated_at_ns=20, role="posthoc_explanation"):
    from histdatacom.experiments import (
        ArtifactReferenceV1,
        ComponentField,
        EvidenceKind,
        ScientificComponentV1,
    )
    from histdatacom.attribution.contracts import ExplanationPlanV1, reference
    from histdatacom.attribution.registry import (
        AttributionExperimentLinkV1,
        explanation_plan_fixture,
    )
    from histdatacom.attribution.decisions import (
        ReferenceDecisionPolicyV1,
        execute_reference_decision,
    )
    from tests.fixtures.experiment_bundles import assemble, bundle_fixture

    attribution = explain_reference(
        *reference_inputs(), generated_at_ns=generated_at_ns
    )
    plan = ExplanationPlanV1(
        attribution.model_reference,
        reference(attribution.policy),
        reference(attribution.background),
        attribution.snapshot.preprocessing,
        attribution.snapshot.projection,
        2,
    )
    fixture = explanation_plan_fixture(plan)
    bundle, fixtures = bundle_fixture()
    replacements = {
        ComponentField.PREPROCESSING: plan.preprocessing,
        ComponentField.FEATURE_PROJECTION: plan.projection,
        ComponentField.MODEL_WEIGHTS: plan.model,
    }
    components = []
    for component in bundle.scientific_inputs.components:
        if component.field is ComponentField.MODEL_CONFIGURATION:
            components.append(
                ScientificComponentV1(component.field, (fixture.reference,))
            )
        elif component.field in replacements:
            ref = replacements[component.field]
            # Existing718 intentionally treats new native artifact schemas as
            # declared external. The719 sidecar independently replays its model.
            old = ArtifactReferenceV1(
                ref.schema,
                ref.native_id,
                ref.sha256,
                ref.byte_length,
                EvidenceKind.DECLARED_EXTERNAL,
            )
            components.append(ScientificComponentV1(component.field, (old,)))
        else:
            components.append(component)
    bundle = replace(
        bundle,
        scientific_inputs=replace(
            bundle.scientific_inputs, components=tuple(components)
        ),
    )
    used = {r.native_id for r in bundle.scientific_inputs.references()}
    retained = tuple(
        f for f in fixtures + (fixture,) if f.reference.native_id in used
    )
    old = assemble((bundle,), retained, cycle_times={"cycle-1": 10})
    policy = ReferenceDecisionPolicyV1(
        1.0, 0.0, -0.5, 0.5, 10.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, True, True, 2
    )
    action = execute_reference_decision(
        attribution.model_reference,
        reference(attribution.snapshot),
        declared_reference("deployment"),
        declared_reference("account"),
        policy,
        decision_at_ns=10,
        raw_score=9.0,
    )
    link = AttributionExperimentLinkV1(
        attribution.artifact_id,
        plan.artifact_id,
        old.bundles[0].experiment_id,
        (old.results[0].result_id,),
        action.artifact_id,
        max(40, generated_at_ns + 1),
        role,
    )
    return AttributionRegistryV1(
        ExplanationPolicyRegistryV1("1.0.0", (attribution.policy,)),
        (plan,),
        (attribution,),
        (action,),
        (link,),
        old.to_json(),
    )
