"""Closed recipe refusal and resealed native-result controls on invented data."""

from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest

from histdatacom.data_quality.training_contracts import (
    TrainingConsumerMode,
    training_json,
    training_load,
)
from histdatacom.data_quality.training_scenario_contracts import (
    ScenarioViewKind,
    TrainingScenarioPlanV1,
    TrainingScenarioPolicyV1,
    TrainingScenarioRequestV1,
    TrainingScenarioResearchBindingV1,
)
from histdatacom.data_quality.training_scenario_views import (
    materialize_training_scenario_view,
    replay_training_scenario_view,
)

pytest_plugins = ("tests.fixtures.training_scenario_v1",)


def plan_and_request(fixture):
    return (
        TrainingScenarioPlanV1(
            fixture["source"],
            fixture["ownership"],
            TrainingScenarioPolicyV1(),
            research_binding=fixture["binding"],
        ),
        TrainingScenarioRequestV1(
            ScenarioViewKind.MEMBER_PANEL,
            TrainingConsumerMode.DESCRIPTIVE,
            fixture["start_ns"],
            fixture["end_ns"],
            fixture["symbols"],
        ),
    )


@pytest.fixture(scope="module")
def actual_research_panel(scenario_research):
    return materialize_training_scenario_view(
        *plan_and_request(scenario_research)
    )


@pytest.mark.parametrize(
    "defect",
    (
        "passing_token",
        "caller_fit",
        "foreign_dataset",
        "float_seed",
        "negative_seed",
        "too_many_paths",
        "too_long_window",
        "unknown_config",
        "coercive_config",
        "duplicate_period",
        "bad_schema",
        "wrong_expected_id",
    ),
)
def test_closed_research_recipe_refuses_before_native_work(
    scenario_research, monkeypatch, defect
):
    from histdatacom.data_quality import training_scenario_research as research

    plan, request = plan_and_request(scenario_research)
    data = training_load(scenario_research["binding"].recipe_json)
    if defect == "passing_token":
        data["verified"] = True
    elif defect == "caller_fit":
        data["fit"] = {"status": "fitted"}
    elif defect == "foreign_dataset":
        data["dataset_version_id"] = "unrelated-source"
    elif defect == "float_seed":
        data["base_seed"] = float(data["base_seed"])
    elif defect == "negative_seed":
        data["base_seed"] = -1
    elif defect == "too_many_paths":
        data["paths_per_cell"] = 1000
    elif defect == "too_long_window":
        data["end_ns"] = data["start_ns"] + 3600 * 10**9 + 1
    elif defect == "unknown_config":
        data["feed_epoch_fit_config"]["caller_approved"] = True
    elif defect == "coercive_config":
        data["feed_epoch_fit_config"]["min_evidence_periods"] = "6"
    elif defect == "duplicate_period":
        data["calibration_periods"].append(data["calibration_periods"][0])
    elif defect == "bad_schema":
        data["schema_version"] = (
            "histdatacom.training-scenario-research-recipe.v999"
        )
    wire = training_json(data)
    identity = (
        "training-scenario-research-recipe:sha256:"
        + hashlib.sha256(wire.encode("ascii")).hexdigest()
    )
    if defect == "wrong_expected_id":
        identity = "training-scenario-research-recipe:sha256:" + "0" * 64
    plan = replace(
        plan, research_binding=TrainingScenarioResearchBindingV1(wire, identity)
    )

    def forbidden_execution(*args, **kwargs):
        pytest.fail("invalid recipe reached native scientific execution")

    # Negative admission guard only; all positive evidence uses actual kernels.
    monkeypatch.setattr(research, "_execute", forbidden_execution)
    with pytest.raises((ValueError, TypeError)):
        materialize_training_scenario_view(plan, request)


def test_resealed_missing_native_rows_are_not_execution_evidence(
    actual_research_panel,
):
    original = actual_research_panel
    assert len(original.rows) > 1
    altered = replace(original, rows=original.rows[:-1])
    assert altered.artifact_id != original.artifact_id
    with pytest.raises(ValueError, match="fresh complete native replay"):
        replay_training_scenario_view(
            altered, expected_view_id=altered.artifact_id
        )


def test_noncentral_native_member_cannot_be_called_central(
    actual_research_panel,
):
    from histdatacom.data_quality.training_scenario_contracts import (
        ScenarioAxis,
    )

    original = actual_research_panel
    noncentral = next(
        member
        for member in original.members
        if any(
            axis.axis is ScenarioAxis.OBSERVATION_RETENTION
            and axis.value_id != "central_fitted_retention"
            for axis in member.axes
        )
    )
    plan = replace(
        original.plan,
        policy=replace(
            original.plan.policy,
            central_member_keys=(noncentral.member_key,),
        ),
    )
    with pytest.raises(ValueError, match="fitted-central"):
        materialize_training_scenario_view(
            plan,
            replace(
                original.request, view=ScenarioViewKind.CENTRAL_COUNTERFACTUAL
            ),
        )


def test_missing_declared_native_product_cannot_shrink_denominator(
    scenario_research,
):
    from histdatacom.data_quality.training_lineage import (
        build_training_ownership,
    )

    plan, request = plan_and_request(scenario_research)
    assert len(plan.source.product_manifest_paths) > 1
    source = replace(
        plan.source,
        product_manifest_paths=plan.source.product_manifest_paths[:-1],
    )
    narrowed = replace(
        plan, source=source, ownership=build_training_ownership(source)
    )
    with pytest.raises(ValueError, match="expected native product is missing"):
        materialize_training_scenario_view(narrowed, request)
