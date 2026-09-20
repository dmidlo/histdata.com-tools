"""Independent synthetic tests against promotion of incompatible evidence."""

from dataclasses import replace

import pytest

from histdatacom.attribution.contracts import (
    AttributionEvidenceKind,
    AttributionState,
)
from histdatacom.attribution.interventions import (
    CounterfactualAttributionV1,
    TriangleInterventionScenarioV1,
    make_triangle_intervention,
)
from histdatacom.attribution.reference import explain_reference
from histdatacom.attribution.reports import (
    AttributionStratumV1,
    StratifiedAttributionV1,
    build_attribution_report,
)
from tests.fixtures.decision_attribution import (
    declared_reference,
    example_attribution,
    reference_inputs,
)


def test_native_intervention_does_not_certify_declared_model_outputs():
    scenario = TriangleInterventionScenarioV1(
        1.0, 2.0, 10, 10, 20, 0, 100, "synthetic-session"
    )
    intervention = make_triangle_intervention(scenario, eurusd=2.0, gbpusd=2.0)
    template = example_attribution()
    before = replace(
        template,
        evidence_kind=AttributionEvidenceKind.DECLARED,
        state=AttributionState.DECLARED,
        model=None,
        model_reference=declared_reference("unexecuted-private-model"),
        snapshot=intervention.before_snapshot,
        generated_at_ns=30,
        raw_output=100.0,
        evaluations=0,
    )
    after = replace(
        before, snapshot=intervention.after_snapshot, raw_output=999.0
    )
    # The primitive-price intervention is real synthetic execution; neither
    # declared score came from an executed model. Their arithmetic difference
    # cannot become a model-replayed counterfactual by embedding that execution.
    with pytest.raises(ValueError, match="executed|reference|ownership"):
        CounterfactualAttributionV1(intervention, before, after, 899.0)


@pytest.mark.parametrize(
    "changed", ("preprocessing_bytes", "projection_bytes", "feature_units")
)
def test_report_stack_uses_exact_semantics_not_reusable_reference_labels(
    changed,
):
    model, policy, snapshot, background = reference_inputs()
    first = explain_reference(
        model, policy, snapshot, background, generated_at_ns=20
    )
    if changed == "feature_units":

        def alter(point):
            value = point.values[0]
            return replace(
                point,
                values=(
                    replace(value, feature=replace(value.feature, unit="pips")),
                    *point.values[1:],
                ),
            )

    else:
        field = changed.removesuffix("_bytes")
        revised = replace(getattr(snapshot, field), sha256="b" * 64)
        assert revised.native_id == getattr(snapshot, field).native_id

        def alter(point):
            return replace(point, **{field: revised})

    second = explain_reference(
        model,
        policy,
        alter(snapshot),
        replace(
            background, snapshots=tuple(alter(p) for p in background.snapshots)
        ),
        generated_at_ns=21,
    )
    # Both models independently execute and reconcile. That does not make their
    # differently defined input coordinates scientifically poolable.
    assert first.raw_output == second.raw_output == 9.0
    assert first.artifact_id != second.artifact_id
    stratum = AttributionStratumV1(
        "EURUSD",
        "fixture-regime",
        "fixture-era",
        "synthetic_fixture",
        "synthetic-support",
        "synthetic-session",
        "no-live-broker",
    )
    entries = (
        StratifiedAttributionV1(first, stratum, "unit-a"),
        StratifiedAttributionV1(second, stratum, "unit-b"),
    )
    with pytest.raises(ValueError, match="semantics|stack|pool"):
        build_attribution_report(entries)


@pytest.mark.parametrize("different_stratum", (False, True))
def test_changed_background_is_refused_within_stratum_and_disclosed_across_it(
    different_stratum,
):
    model, policy, point, background = reference_inputs()
    first = explain_reference(
        model, policy, point, background, generated_at_ns=20
    )
    baseline = background.snapshots[0]
    shifted = replace(
        background,
        snapshots=(
            replace(
                baseline,
                values=tuple(replace(v, value=0.25) for v in baseline.values),
            ),
        ),
    )
    second = explain_reference(
        model, policy, point, shifted, generated_at_ns=21
    )
    assert first.raw_output == second.raw_output
    assert first.baseline_output != second.baseline_output
    stratum = AttributionStratumV1(
        "EURUSD",
        "fixture-regime",
        "fixture-era",
        "synthetic_fixture",
        "synthetic-support",
        "synthetic-session",
        "no-live-broker",
    )
    entries = (
        StratifiedAttributionV1(first, stratum, "unit-a"),
        StratifiedAttributionV1(
            second,
            (
                replace(stratum, era="later-fixture-era")
                if different_stratum
                else stratum
            ),
            "unit-b",
        ),
    )
    if not different_stratum:
        with pytest.raises(
            ValueError, match="pool different reference backgrounds"
        ):
            build_attribution_report(entries)
    else:
        report = build_attribution_report(entries)
        assert len(report.differences) == 2
        assert all(d.background_changed for d in report.differences)
        assert type(report).from_json(report.to_json()) == report
        with pytest.raises(ValueError, match="replay"):
            replace(
                report,
                differences=(
                    replace(report.differences[0], background_changed=False),
                    *report.differences[1:],
                ),
            )
