"""Synthetic request-shape checks, never a preprocessing fit certificate."""

from dataclasses import replace

import pytest

from histdatacom.data_quality.training_preprocessing_contracts import (
    PREPROCESSING_FAMILIES,
    PreprocessingFitMode,
    TrainingPreprocessingPlanV1,
    TrainingPreprocessingStepV1,
    readmit_preprocessing_contract,
)
from histdatacom.data_quality.training_temporal_contracts import (
    TemporalPartition,
    TrainingTemporalAssignmentV1,
    TrainingTemporalSplitV1,
)
from histdatacom.data_quality.training_wide_contracts import (
    TrainingWideGrainV1,
    WideRowGrain,
)
from histdatacom.data_quality.training_wide_views import (
    build_training_wide_plan,
)
from tests.fixtures.training_join_v1 import join_fixture


@pytest.fixture(scope="module")
def plan(tmp_path_factory):
    native = join_fixture(tmp_path_factory.mktemp("preprocessing-contracts"))
    wide = build_training_wide_plan(
        (native,), grain=TrainingWideGrainV1(WideRowGrain.EVENT)
    )
    ownership = native.spine.ownership
    split = TrainingTemporalSplitV1(
        ownership.artifact_id,
        tuple(
            TrainingTemporalAssignmentV1(
                unit.artifact_id, tuple(TemporalPartition)[min(index, 2)]
            )
            for index, unit in enumerate(ownership.units)
        ),
    )
    columns = (native.columns[0].name,)
    return TrainingPreprocessingPlanV1(
        wide,
        split,
        "synthetic-fold-1",
        PreprocessingFitMode.TRAIN_FIT,
        (ownership.units[0].artifact_id,),
        columns,
        (TrainingPreprocessingStepV1("scale", "zscale", columns),),
    )


def test_plan_roundtrip_retains_complete_native_scope(plan):
    assert TrainingPreprocessingPlanV1.from_json(plan.to_json()) == plan
    detached = readmit_preprocessing_contract(plan, TrainingPreprocessingPlanV1)
    assert detached == plan and detached is not plan
    assert detached.wide_plan is not plan.wide_plan
    assert len(detached.split.assignments) == len(
        detached.wide_plan.tiles[0].join_plan.spine.ownership.units
    )


@pytest.mark.parametrize("index", [1, 2])
def test_protected_membership_refuses_at_request_boundary(plan, index):
    with pytest.raises(ValueError, match="protected"):
        replace(
            plan, fit_unit_ids=(plan.split.assignments[index].evidence_unit_id,)
        )


def test_fold_and_mode_are_identity_bearing(plan):
    assert (
        replace(plan, fold_id="synthetic-fold-2").artifact_id
        != plan.artifact_id
    )
    rolling = replace(
        plan, fit_mode=PreprocessingFitMode.ROLLING_PTI, rolling_window_ns=10
    )
    assert rolling.artifact_id != plan.artifact_id
    assert (
        replace(rolling, rolling_window_ns=11).artifact_id
        != rolling.artifact_id
    )


@pytest.mark.parametrize("window", [None, 0, True, -1])
def test_rolling_requires_exact_positive_frozen_window(plan, window):
    with pytest.raises((TypeError, ValueError)):
        replace(
            plan,
            fit_mode=PreprocessingFitMode.ROLLING_PTI,
            rolling_window_ns=window,
        )


def test_none_mode_cannot_conceal_learned_state(plan):
    with pytest.raises(ValueError, match="conceal"):
        replace(plan, fit_mode=PreprocessingFitMode.NONE, fit_unit_ids=())
    raw = replace(
        plan, fit_mode=PreprocessingFitMode.NONE, fit_unit_ids=(), steps=()
    )
    assert raw.fit_mode is PreprocessingFitMode.NONE


def test_categorical_missingness_is_explicit_native_companion(plan):
    assert (
        replace(plan, columns=("state." + plan.columns[0],))
        .columns[0]
        .startswith("state.")
    )
    with pytest.raises(ValueError, match="native feature registry"):
        replace(plan, columns=("labels.future_return",))


@pytest.mark.parametrize("family", PREPROCESSING_FAMILIES)
def test_every_governed_family_has_versioned_request_identity(plan, family):
    step = replace(plan.steps[0], family=family)
    assert step.version == "1.0.0"
    assert TrainingPreprocessingStepV1.from_json(step.to_json()) == step


@pytest.mark.parametrize(
    "changes",
    [
        {"family": "arbitrary_callback"},
        {"version": "2.0.0"},
        {"columns": ()},
        {"columns": ["market.tick.EURUSD.bid"]},
        {"options_json": '{ "value":1}'},
        {"options_json": '{"value":1,"value":2}'},
        {"options_json": '{"value":NaN}'},
    ],
)
def test_closed_bounded_step_shape(plan, changes):
    with pytest.raises((TypeError, ValueError)):
        replace(plan.steps[0], **changes)


def test_stateless_zero_and_prior_do_not_accept_fitted_parameters(plan):
    column = plan.columns
    with pytest.raises(ValueError, match="no fitted options"):
        TrainingPreprocessingStepV1(
            "zero", "confirmed_zero", column, '{"mean":0}'
        )
    with pytest.raises(ValueError, match="staleness"):
        TrainingPreprocessingStepV1("prior", "prior_state", column)
    with pytest.raises(ValueError, match="clock"):
        TrainingPreprocessingStepV1(
            "prior", "prior_state", column, '{"max_age_ns":true}'
        )
    TrainingPreprocessingStepV1(
        "prior", "prior_state", column, '{"max_age_ns":5}'
    )


def test_raw_field_readmission_does_not_trust_cached_wire(plan):
    changed = replace(plan, fold_id="before")
    original_id = changed.artifact_id
    changed.to_json()
    object.__setattr__(changed, "fold_id", "after")
    assert changed.artifact_id == original_id
    fresh = readmit_preprocessing_contract(changed, TrainingPreprocessingPlanV1)
    assert fresh.fold_id == "after" and fresh.artifact_id != original_id


def test_raw_nested_mutation_is_readmitted_before_native_serialization(plan):
    changed = readmit_preprocessing_contract(plan, TrainingPreprocessingPlanV1)
    object.__setattr__(
        changed.steps[0], "options_json", "x" * (8 * 1024 * 1024 + 1)
    )
    with pytest.raises(ValueError, match="budget"):
        readmit_preprocessing_contract(changed, TrainingPreprocessingPlanV1)


def test_native_split_cannot_be_sliced_or_reassigned_out_of_order(plan):
    short = replace(plan.split, assignments=plan.split.assignments[:1])
    with pytest.raises(ValueError, match="cover native"):
        replace(plan, split=short)
    reversed_parts = tuple(
        replace(item, partition=part)
        for item, part in zip(
            plan.split.assignments,
            (TemporalPartition.TEST, TemporalPartition.VALIDATION)
            + (TemporalPartition.TRAIN,) * (len(plan.split.assignments) - 2),
        )
    )
    with pytest.raises(ValueError, match="chronological"):
        replace(plan, split=replace(plan.split, assignments=reversed_parts))
