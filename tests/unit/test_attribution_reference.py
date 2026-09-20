"""Independent synthetic equations, stability diagnostics and native interventions."""

from dataclasses import replace
from fractions import Fraction
from itertools import permutations

import pytest

from histdatacom.attribution.contracts import (
    AttributionBackgroundV1,
    AttributionGroupV1,
    AttributionState,
    ContributionV1,
    FeatureSpace,
    GroupMethod,
    PolynomialTermV1,
    ReferenceModelV1,
)
from histdatacom.attribution.reference import explain_reference
from histdatacom.attribution.interventions import (
    CounterfactualAttributionV1,
    NativeTriangleInterventionV1,
    TriangleInterventionScenarioV1,
    make_triangle_intervention,
)
from histdatacom.attribution.mapping import (
    AttributionMappingV1,
    DiagonalFeatureMapV1,
    MappedAttributionV1,
)
from tests.fixtures.decision_attribution import reference_inputs


def test_independent_two_feature_shapley_and_interaction_accounting():
    attribution = explain_reference(*reference_inputs(), generated_at_ns=20)
    # Independent coalition equations from719, not the production evaluator.
    v = {(): 0, ("x1",): 3, ("x2",): 2, ("x1", "x2"): 9}
    expected = (
        Fraction(v[("x1",)] - v[()] + v[("x1", "x2")] - v[("x2",)], 2),
        Fraction(v[("x2",)] - v[()] + v[("x1", "x2")] - v[("x1",)], 2),
    )
    assert (
        tuple(c.value for c in attribution.contributions)
        == tuple(float(x) for x in expected)
        == (5.0, 4.0)
    )
    assert attribution.raw_output == 9 and attribution.baseline_output == 0
    assert (
        attribution.additive_residual
        == attribution.group_additive_residual
        == 0
    )
    assert attribution.interactions[0].value == 4
    assert attribution.evaluations == 12


@pytest.mark.parametrize(
    "x,y", [(0.0, 0.0), (1.0, 1.0), (-2.0, 3.0), (0.25, 0.5), (10.0, -2.0)]
)
def test_permutation_oracle_at_several_exact_points(x, y):
    model, policy, snapshot, background = reference_inputs()
    snapshot = replace(
        snapshot,
        values=(
            replace(snapshot.values[0], value=x),
            replace(snapshot.values[1], value=y),
        ),
    )
    values = {"x1": Fraction(x), "x2": Fraction(y)}
    scores = {name: Fraction() for name in values}

    def f(point):
        return 3 * point["x1"] + 2 * point["x2"] + 4 * point["x1"] * point["x2"]

    for order in permutations(values):
        point = {"x1": Fraction(), "x2": Fraction()}
        for name in order:
            previous = f(point)
            point[name] = values[name]
            scores[name] += (f(point) - previous) / 2
    result = explain_reference(
        model, policy, snapshot, background, generated_at_ns=20
    )
    assert tuple(c.value for c in result.contributions) == tuple(
        float(scores[name]) for name in sorted(scores)
    )


def test_resealed_contributions_and_residuals_are_replayed():
    result = explain_reference(*reference_inputs(), generated_at_ns=20)
    with pytest.raises(ValueError, match="replay"):
        replace(
            result,
            contributions=(
                ContributionV1("x1", 6.0),
                ContributionV1("x2", 3.0),
            ),
        )
    with pytest.raises(ValueError, match="replay"):
        replace(result, evaluations=11)


def test_group_partition_no_double_counting_and_direct_distinct_identity():
    model, policy, snapshot, background = reference_inputs()
    with pytest.raises(ValueError, match="double"):
        replace(policy, reporting_cut=("all", "first", "second"))
    with pytest.raises(ValueError, match="omits"):
        replace(policy, reporting_cut=("first",))
    summed = replace(policy, reporting_cut=("all",))
    direct = replace(summed, group_method=GroupMethod.DIRECT)
    a = explain_reference(
        model, summed, snapshot, background, generated_at_ns=20
    )
    b = explain_reference(
        model, direct, snapshot, background, generated_at_ns=20
    )
    assert a.groups == b.groups == (ContributionV1("all", 9.0),)
    assert a.artifact_id != b.artifact_id


def test_three_player_oracle_and_direct_groups_are_not_singleton_sums():
    _, policy, snapshot, background = reference_inputs()
    third = replace(
        snapshot.values[1],
        feature=replace(snapshot.values[1].feature, name="x3"),
    )
    snapshot = replace(snapshot, values=snapshot.values + (third,))
    baseline = replace(
        snapshot,
        values=tuple(
            replace(v, value=0.0, available_at_ns=1) for v in snapshot.values
        ),
        cutoff_at_ns=1,
    )
    background = replace(background, snapshots=(baseline,))
    model = ReferenceModelV1(
        ("x1", "x2", "x3"),
        (PolynomialTermV1(6.0, ("x1", "x2", "x3")),),
        "fixture.response",
        "dimensionless",
        "class-A",
        100,
    )
    groups = (
        AttributionGroupV1("all", None, model.features, "plane"),
        AttributionGroupV1("pair", "all", ("x1", "x2"), "correlated_cluster"),
        AttributionGroupV1("third", "all", ("x3",), "correlated_cluster"),
    )
    policy = replace(policy, groups=groups, reporting_cut=("pair", "third"))
    summed = explain_reference(
        model, policy, snapshot, background, generated_at_ns=20
    )
    direct = explain_reference(
        model,
        replace(policy, group_method=GroupMethod.DIRECT),
        snapshot,
        background,
        generated_at_ns=20,
    )
    # Independent unanimity game: only the final player changes output0→6
    # in each of all six permutations; every player is final exactly twice.
    oracle = {name: Fraction() for name in model.features}
    for order in permutations(model.features):
        oracle[order[-1]] += Fraction(6, 6)
    assert (
        tuple(c.value for c in summed.contributions)
        == tuple(oracle.values())
        == (2, 2, 2)
    )
    assert tuple(c.value for c in summed.groups) == (4, 2)
    assert tuple(c.value for c in direct.groups) == (3, 3)
    assert summed.explained_output.class_label == "class-A"
    assert summed.explained_output.horizon_ns == 100
    assert type(summed).from_json(summed.to_json()) == summed
    with pytest.raises(ValueError, match="replay"):
        replace(
            summed,
            explained_output=replace(summed.explained_output, horizon_ns=101),
        )


def test_background_sibling_ambiguity_and_correlated_effective_dimension():
    model, policy, snapshot, background = reference_inputs()
    other = replace(
        background.snapshots[0],
        values=tuple(
            replace(v, value=0.5) for v in background.snapshots[0].values
        ),
    )
    background = replace(
        background,
        snapshots=tuple(
            sorted(background.snapshots + (other,), key=lambda s: s.artifact_id)
        ),
    )
    result = explain_reference(
        model,
        replace(policy, instability_threshold=0.1),
        snapshot,
        background,
        generated_at_ns=20,
    )
    assert result.state is AttributionState.NONIDENTIFIABLE
    assert all(
        a.background_range > 0 and a.sibling_sensitivity > 0
        for a in result.ambiguity
    )
    assert result.effective_feature_dimension == 1.0


@pytest.mark.parametrize(
    "kind", ["future", "unknown", "protected", "domain", "budget"]
)
def test_unsupported_states_are_not_fabricated_outputs(kind, monkeypatch):
    model, policy, snapshot, background = reference_inputs()
    if kind in ("future", "unknown"):
        snapshot = replace(
            snapshot,
            values=(
                replace(
                    snapshot.values[0],
                    available_at_ns=11 if kind == "future" else None,
                ),
                snapshot.values[1],
            ),
        )
    elif kind == "protected":
        background = replace(background, role="protected")
    elif kind == "domain":
        background = replace(
            background,
            snapshots=(replace(background.snapshots[0], domain="different"),),
        )
    else:
        policy = replace(policy, maximum_evaluations=11)
    import histdatacom.attribution.reference as ref

    monkeypatch.setattr(
        ref,
        "_evaluate",
        lambda *args: pytest.fail("preflight must run before model"),
    )
    result = explain_reference(
        model, policy, snapshot, background, generated_at_ns=20
    )
    assert (
        result.state is AttributionState.UNSUPPORTED
        and result.raw_output is None
        and result.evaluations == 0
    )


def test_diagonal_mapping_replays_values_and_refuses_latent_relabel():
    from histdatacom.attribution.contracts import reference

    model, policy, raw, bg = reference_inputs()
    mapping = AttributionMappingV1(
        (
            DiagonalFeatureMapV1("x1", "x1", 2.0, 0.0),
            DiagonalFeatureMapV1("x2", "x2", 2.0, 0.0),
        ),
        0,
    )

    def transform(s):
        return replace(
            s,
            space=FeatureSpace.TRANSFORMED,
            preprocessing=reference(mapping),
            values=tuple(replace(v, value=v.value * 2) for v in s.values),
        )

    transformed = transform(raw)
    transformed_bg = replace(bg, snapshots=(transform(bg.snapshots[0]),))
    result = explain_reference(
        model, policy, transformed, transformed_bg, generated_at_ns=20
    )
    mapped = MappedAttributionV1(result, mapping, raw, bg, result.contributions)
    assert type(mapped).from_json(mapped.to_json()) == mapped
    with pytest.raises(ValueError, match="diagonal"):
        replace(
            mapped,
            mapping=replace(
                mapping,
                axes=(replace(mapping.axes[0], scale=3.0), mapping.axes[1]),
            ),
        )
    latent = explain_reference(
        model,
        policy,
        replace(transformed, space=FeatureSpace.LATENT),
        replace(
            transformed_bg,
            snapshots=(
                replace(transformed_bg.snapshots[0], space=FeatureSpace.LATENT),
            ),
        ),
        generated_at_ns=20,
    )
    with pytest.raises(ValueError, match="latent"):
        replace(mapped, source=latent)


def test_native_triangle_intervention_recomputes_all_dependent_features():
    scenario = TriangleInterventionScenarioV1(
        1.0, 2.0, 10, 10, 20, 0, 100, "fixture-session"
    )
    intervention = make_triangle_intervention(scenario, eurusd=2.0, gbpusd=2.0)
    assert [v.value for v in intervention.before_snapshot.values] == [
        0.5,
        1.0,
        2.0,
        0.0,
    ]
    assert [v.value for v in intervention.after_snapshot.values] == [
        1.0,
        2.0,
        2.0,
        0.0,
    ]
    assert (
        NativeTriangleInterventionV1.from_json(intervention.to_json())
        == intervention
    )
    names = tuple(v.feature.name for v in intervention.before_snapshot.values)
    model = ReferenceModelV1(
        names,
        (PolynomialTermV1(1.0, (names[1],)),),
        "fixture.price",
        "USD_per_EUR",
    )
    _, policy, _, _ = reference_inputs()
    policy = replace(
        policy,
        groups=(AttributionGroupV1("all", None, names, "plane"),),
        reporting_cut=("all",),
    )
    background = AttributionBackgroundV1(
        (intervention.before_snapshot,), 20, "synthetic_fixture"
    )
    a = explain_reference(
        model,
        policy,
        intervention.before_snapshot,
        background,
        generated_at_ns=30,
    )
    b = explain_reference(
        model,
        policy,
        intervention.after_snapshot,
        background,
        generated_at_ns=30,
    )
    counterfactual = CounterfactualAttributionV1(intervention, a, b, 1.0)
    assert (
        type(counterfactual).from_json(counterfactual.to_json())
        == counterfactual
    )
    with pytest.raises(ValueError, match="regeneration"):
        replace(intervention, after_snapshot=intervention.before_snapshot)


@pytest.mark.parametrize(
    "change",
    [
        {"available_at_ns": 21},
        {"universe": ("EURUSD",)},
        {"units": ("wrong",)},
        {"session_end_ns": 15},
    ],
)
def test_native_counterfactual_rejects_time_universe_units_session(change):
    scenario = TriangleInterventionScenarioV1(
        1.0, 2.0, 10, 10, 20, 0, 100, "fixture"
    )
    with pytest.raises(ValueError):
        replace(scenario, **change)
