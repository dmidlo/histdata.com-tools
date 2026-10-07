"""Native synthetic exports retain input semantics after real transformations.

One source-replayed fit/application covers independent scalar, projection and
state-category outputs. No fabricated view or verifier replacement grants the
input schema authority; neither these inputs nor their units qualify history.
"""

from dataclasses import replace

import pytest

from histdatacom.data_quality.training_join_contracts import JoinState
from histdatacom.data_quality.training_preprocessing import (
    TrainingPreprocessingStepV1,
    apply_training_preprocessing,
    evidence_records,
    fit_training_preprocessing,
    materialize_training_evidence_view,
)
from tests.fixtures.training_preprocessing_v1 import (
    ASK,
    BID,
    SPREAD,
    preprocessing_plan,
)

pytest_plugins = ("tests.fixtures.training_preprocessing_v1",)

STATE = "state." + BID


@pytest.fixture(scope="module")
def native_export_schema(preprocessing_native):
    plan = preprocessing_plan(
        preprocessing_native, columns=(BID, ASK, SPREAD, STATE)
    )
    plan = replace(
        plan,
        steps=(
            TrainingPreprocessingStepV1("scalar", "zscale", (SPREAD,)),
            TrainingPreprocessingStepV1("projection", "pca", (BID, ASK)),
            TrainingPreprocessingStepV1("category", "categorical", (STATE,)),
        ),
    )
    originals = evidence_records(materialize_training_evidence_view(plan))
    fit = fit_training_preprocessing(plan)
    view = apply_training_preprocessing(fit, expected_fit_id=fit.artifact_id)
    return plan, originals, view.records


def test_all_selected_input_descriptors_survive_native_dense_export(
    native_export_schema,
):
    plan, originals, rows = native_export_schema
    assert len(rows) == len(originals) == 24
    assert {row["partition"] for row in rows} == {"train", "validation", "test"}
    for evidence, row in zip(originals, rows):
        assert row["kind"] == "dense_model"
        assert row["input_schema"] == evidence["schema"]
        assert set(row["input_schema"]) == set(plan.columns)
        assert row["originals"] == evidence["originals"]
        assert set(row["schema"]) == set(row["values"])
        for output in row["schema"].values():
            assert output["native_dependencies"]
            assert set(output["native_dependencies"]) <= set(
                row["input_schema"]
            )
            for parent in output["native_dependencies"]:
                assert (
                    row["input_schema"][parent]["descriptor"]["name"] == parent
                )


def test_scalar_export_keeps_exact_native_units_dtype_and_meaning(
    native_export_schema,
):
    plan, _, rows = native_export_schema
    definition = next(
        column
        for column in plan.wide_plan.columns
        if column.column.name == SPREAD
    )
    for row in rows:
        output = row["schema"][SPREAD]
        assert output["descriptor"]["family"] == "zscale"
        assert output["native_dependencies"] == [SPREAD]
        native = row["input_schema"][SPREAD]["descriptor"]
        assert native["kind"] == "native_value"
        assert native["ontology"] is None
        assert native["native_definition"] == definition.to_dict()
        assert native["native_definition"]["units"] == (
            "quote_currency_per_base_currency"
        )
        assert native["native_definition"]["dtype"] == definition.dtype.value
        assert native["native_definition"]["column"]["meaning"] == (
            definition.column.meaning.value
        )


def test_projection_export_resolves_both_native_input_definitions(
    native_export_schema,
):
    plan, _, rows = native_export_schema
    definitions = {
        column.column.name: column.to_dict()
        for column in plan.wide_plan.columns
        if column.column.name in (BID, ASK)
    }
    for row in rows:
        output = row["schema"]["pca:0"]
        assert output["descriptor"]["family"] == "pca"
        assert output["native_dependencies"] == sorted((BID, ASK))
        assert BID not in row["values"] and ASK not in row["values"]
        assert {
            parent: row["input_schema"][parent]["descriptor"][
                "native_definition"
            ]
            for parent in output["native_dependencies"]
        } == definitions


def test_state_category_export_retains_native_ontology_without_substitution(
    native_export_schema,
):
    _, _, rows = native_export_schema
    for row in rows:
        native = row["input_schema"][STATE]["descriptor"]
        assert native["name"] == STATE
        assert native["kind"] == "native_missingness_category"
        assert native["ontology"] == [state.value for state in JoinState] + [
            "missing_spine_anchor"
        ]
        assert native["native_definition"] == (
            row["input_schema"][BID]["descriptor"]["native_definition"]
        )
        encoded = [
            output
            for output in row["schema"].values()
            if output["descriptor"]["family"] == "categorical"
        ]
        assert len(encoded) == 3  # Known category, unknown, missing.
        assert all(
            output["native_dependencies"] == [STATE] for output in encoded
        )
        assert STATE not in row["values"]
        assert row["originals"][STATE]["value"] == JoinState.AVAILABLE.value
