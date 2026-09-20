"""Native718 sidecars and distinct executed score-to-action stages."""

from dataclasses import replace

import pytest

from histdatacom.attribution.contracts import reference
from histdatacom.attribution.decisions import (
    ReferenceDecisionPolicyV1,
    execute_reference_decision,
)
from histdatacom.attribution.registry import reverse_attribution_dependencies
from tests.fixtures.decision_attribution import (
    declared_reference,
    example_attribution,
    linked_example_registry,
)


def decision(policy=None):
    attribution = example_attribution()
    policy = policy or ReferenceDecisionPolicyV1(
        1.0, 0.0, -0.5, 0.5, 0.0, 4.0, 3.0, 0.0, 0.0, 2.0, 1.0, True, True, 1
    )
    return execute_reference_decision(
        attribution.model_reference,
        reference(attribution.snapshot),
        declared_reference("deployment"),
        declared_reference("account"),
        policy,
        decision_at_ns=10,
        raw_score=attribution.raw_output,
    )


def test_eight_executed_stages_retain_clipping_and_active_constraints():
    result = decision()
    assert tuple(s.stage for s in result.stages) == (
        "model",
        "calibration",
        "policy",
        "sizing",
        "portfolio",
        "margin",
        "validity",
        "execution",
    )
    assert [s.output_value for s in result.stages] == [
        9.0,
        9.0,
        1.0,
        3.0,
        2.0,
        1.0,
        1.0,
        1.0,
    ]
    assert result.stages[3].active_constraints == ("position_cap",)
    assert result.stages[4].active_constraints == ("portfolio_cap",)
    assert result.stages[5].active_constraints == ("margin_cap",)
    with pytest.raises(ValueError, match="equations"):
        replace(
            result,
            stages=(
                replace(result.stages[0], reason="model predicted neutral"),
            )
            + result.stages[1:],
        )


@pytest.mark.parametrize(
    "field,value,stage,reason",
    [
        ("expected_cost", 10.0, 2, "cost_gate_rejected"),
        ("long_threshold", 10.0, 2, "policy_deadband"),
        ("validity_allowed", False, 6, "validity_refused"),
        ("execution_allowed", False, 7, "execution_rejected"),
    ],
)
def test_zero_action_does_not_claim_neutral_positive_model(
    field, value, stage, reason
):
    result = decision(replace(decision().policy, **{field: value}))
    assert result.raw_score == 9.0 and result.final_action == 0.0
    assert result.stages[stage].reason == reason


def test_hysteresis_uses_previous_position_and_calibration_has_own_stage():
    policy = replace(
        decision().policy,
        size=1.2,
        previous_position=1.0,
        hysteresis=0.5,
        calibration_scale=0.5,
    )
    result = decision(policy)
    assert result.stages[1].output_value == 4.5
    assert result.stages[3].reason == "hysteresis_retained_position"
    assert result.final_action == 1.0


def test_existing_experiment_plan_binding_and_posthoc_history():
    registry = linked_example_registry(generated_at_ns=40)
    original_action = registry.actions[0].artifact_id
    assert registry.attributions[0].generated_at_ns == 40
    assert type(registry).from_json(registry.to_json()) == registry
    assert registry.actions[0].artifact_id == original_action
    assert (
        registry.actions[0].raw_score == 9
        and registry.actions[0].final_action == 0
    )
    with pytest.raises(ValueError, match="predates"):
        replace(
            registry,
            links=(replace(registry.links[0], role="scientific_result"),),
        )
    # An action-only posthoc link does not manufacture a new result.
    assert (
        replace(registry, links=(replace(registry.links[0], result_ids=()),))
        .links[0]
        .result_ids
        == ()
    )


def test_exact_plan_refs_and_action_score_cannot_be_resealed():
    registry = linked_example_registry()
    with pytest.raises(ValueError):
        replace(
            registry,
            links=(replace(registry.links[0], plan_id="missing-plan"),),
        )
    bad = replace(
        registry.plans[0], background=declared_reference("different-background")
    )
    with pytest.raises(ValueError, match="differs"):
        replace(
            registry,
            plans=(bad,),
            links=(replace(registry.links[0], plan_id=bad.artifact_id),),
        )
    wrong = execute_reference_decision(
        registry.actions[0].model,
        registry.actions[0].snapshot,
        registry.actions[0].deployment,
        registry.actions[0].account_state,
        registry.actions[0].policy,
        decision_at_ns=10,
        raw_score=0.0,
    )
    with pytest.raises(ValueError, match="replayed"):
        replace(
            registry,
            actions=(wrong,),
            links=(replace(registry.links[0], action_id=wrong.artifact_id),),
        )


def test_reverse_lineage_contains_original_results_and_actions():
    registry = linked_example_registry()
    refs = reverse_attribution_dependencies(
        registry, registry.attributions[0].model_reference.native_id
    )
    assert registry.attributions[0].artifact_id in refs
    assert registry.links[0].experiment_id in refs
    assert registry.links[0].result_ids[0] in refs
    assert registry.actions[0].artifact_id in refs
