"""Independent synthetic timing and identity checks at the frozen718 bridge."""

from dataclasses import replace

import pytest

from histdatacom.attribution.registry import (
    AttributionRegistryV1,
    reverse_attribution_dependencies,
)
from histdatacom.experiments import ExperimentRegistryV1
from tests.fixtures.decision_attribution import linked_example_registry


@pytest.fixture(scope="module")
def linked():
    return linked_example_registry()


def test_later_explanation_preserves_historical_result_and_action_identity(
    linked,
):
    later = linked_example_registry(generated_at_ns=40)
    assert later.experiment_registry_json == linked.experiment_registry_json
    assert later.actions == linked.actions
    assert later.plans == linked.plans
    assert later.links[0].result_ids == linked.links[0].result_ids
    assert (
        later.attributions[0].artifact_id != linked.attributions[0].artifact_id
    )
    assert AttributionRegistryV1.from_json(later.to_json()) == later
    assert linked.experiment_registry_json is not None
    original = ExperimentRegistryV1.from_json(linked.experiment_registry_json)
    assert original.to_json() == later.experiment_registry_json


def test_scientific_result_cannot_cite_explanation_from_its_future():
    with pytest.raises(ValueError, match="scientific result predates"):
        linked_example_registry(generated_at_ns=40, role="scientific_result")
    early = linked_example_registry(role="scientific_result")
    assert AttributionRegistryV1.from_json(early.to_json()) == early


@pytest.mark.parametrize("action_only", (True, False))
def test_posthoc_sidecar_can_explain_action_or_experiment_without_result(
    linked, action_only
):
    link = replace(
        linked.links[0],
        result_ids=(),
        action_id=linked.actions[0].artifact_id if action_only else None,
    )
    registry = replace(linked, links=(link,))
    assert AttributionRegistryV1.from_json(registry.to_json()) == registry
    with pytest.raises(ValueError, match="needs produced results"):
        replace(link, role="scientific_result")


def test_sidecar_cannot_predate_cited_result_even_if_explanation_exists(linked):
    with pytest.raises(ValueError, match="as-of ordering"):
        replace(linked, links=(replace(linked.links[0], linked_at_ns=29),))


def test_sidecar_cannot_predate_retained_explanation(linked):
    with pytest.raises(ValueError, match="predates retained evidence"):
        replace(linked, links=(replace(linked.links[0], linked_at_ns=19),))


@pytest.mark.parametrize("missing", ("plans", "attributions", "actions"))
def test_bridge_requires_exact_retained_ancestors(linked, missing):
    with pytest.raises(ValueError, match="missing|lacks an actually replayed"):
        replace(linked, **{missing: ()})


def test_bridge_does_not_accept_detached_native_registry(linked):
    with pytest.raises(ValueError, match="actual retained registry"):
        replace(linked, experiment_registry_json=None)


def test_positive_raw_score_is_not_relabelled_neutral_after_cost_gate(linked):
    attribution, action = linked.attributions[0], linked.actions[0]
    assert attribution.raw_output == 9.0
    assert action.raw_score == 9.0
    assert action.final_action == 0.0
    assert action.stages[0].output_value == 9.0
    assert action.stages[2].output_value == 0.0
    assert "cost" in action.stages[2].reason
    assert action.model == attribution.model_reference
    found = reverse_attribution_dependencies(
        linked, attribution.model_reference.native_id
    )
    assert attribution.artifact_id in found
    assert action.artifact_id in found
    assert linked.links[0].experiment_id in found
    assert set(linked.links[0].result_ids) <= set(found)


def test_plan_cannot_relabel_a_different_model_as_original_configuration(
    linked,
):
    changed = replace(
        linked.plans[0],
        model=replace(linked.plans[0].model, sha256="b" * 64),
    )
    link = replace(linked.links[0], plan_id=changed.artifact_id)
    with pytest.raises(ValueError, match="differs from replayed"):
        replace(linked, plans=(changed,), links=(link,))
