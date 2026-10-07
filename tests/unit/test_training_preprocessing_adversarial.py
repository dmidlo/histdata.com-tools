"""Independent leakage/admission canaries, with no successful verifier doubles."""

from __future__ import annotations

import json
from dataclasses import replace
from fractions import Fraction

import pytest

pytest_plugins = ("tests.fixtures.training_preprocessing_v1",)

FAMILIES = (
    "zscale",
    "robust_scale",
    "minmax",
    "winsorize",
    "quantile_rank",
    "categorical",
    "median_imputer",
    "variance_selector",
    "correlation_pruner",
    "pca",
    "svd",
    "whitening",
    "population_reduction",
    "linear_embedding",
)


@pytest.mark.parametrize(
    "family",
    ("fixed_unit_conversion", "unit_scale", "backward_fill", "target_encoder"),
)
def test_unsupported_transform_does_not_silently_substitute_raw(family):
    from histdatacom.data_quality.training_preprocessing_contracts import (
        TrainingPreprocessingStepV1,
    )

    with pytest.raises(ValueError, match="family"):
        TrainingPreprocessingStepV1(
            "unsupported-request", family, ("market.tick.EURUSD.bid",)
        )


@pytest.mark.parametrize("family", FAMILIES)
def test_protected_fit_members_refuse_every_stateful_family(
    preprocessing_native, family
):
    from histdatacom.data_quality.training_temporal_contracts import (
        TemporalPartition,
    )
    from tests.fixtures.training_preprocessing_v1 import preprocessing_plan

    protected = next(
        item.evidence_unit_id
        for item in preprocessing_native["split"].assignments
        if item.partition is TemporalPartition.VALIDATION
    )
    with pytest.raises(ValueError, match="protected or unknown"):
        preprocessing_plan(
            preprocessing_native, family=family, fit_unit_ids=(protected,)
        )


def test_partial_or_foreign_fit_unit_refuses(preprocessing_native):
    from tests.fixtures.training_preprocessing_v1 import preprocessing_plan

    with pytest.raises(ValueError, match="protected or unknown"):
        preprocessing_plan(
            preprocessing_native,
            fit_unit_ids=("training-evidence-unit:sha256:" + "a" * 64,),
        )
    # A split cannot discard the rest of the independent source's unit roster.
    native = dict(preprocessing_native)
    native["split"] = replace(
        native["split"], assignments=native["split"].assignments[1:]
    )
    with pytest.raises(ValueError, match="complete|cover native ownership"):
        preprocessing_plan(native)


@pytest.mark.parametrize(
    "column",
    ("label.return", "target.protected_direction", "market.tick.EURUSD.future"),
)
def test_protected_label_feature_canary_refuses(preprocessing_native, column):
    from tests.fixtures.training_preprocessing_v1 import preprocessing_plan

    with pytest.raises(ValueError, match="outside native feature registry"):
        preprocessing_plan(preprocessing_native, columns=(column,))


@pytest.mark.parametrize("value", (True, False, float("nan"), float("inf")))
def test_reference_rejects_non_numeric_or_nonfinite_scalar(value):
    from histdatacom.data_quality.training_preprocessing_math import (
        fit_reference_transform,
    )

    with pytest.raises((TypeError, ValueError)):
        fit_reference_transform("zscale", ("x",), ((value,),), (Fraction(1),))


@pytest.mark.parametrize(
    "weights",
    ((Fraction(-1),), (Fraction(0),), (1.0,), (True,), []),
)
def test_reference_weights_cannot_be_negative_zero_coerced_or_mutable(weights):
    from histdatacom.data_quality.training_preprocessing_math import (
        fit_reference_transform,
    )

    with pytest.raises((TypeError, ValueError)):
        fit_reference_transform("zscale", ("x",), ((1,),), weights)


@pytest.mark.parametrize(
    "family,options",
    (
        ("zscale", '{"include_validation":true}'),
        ("categorical", "{}"),
        ("winsorize", '{"lower":"3/4","upper":"1/4"}'),
        ("robust_scale", '{"mad_scale":"0"}'),
        ("quantile_rank", '{"test_distribution":[1,2,3]}'),
        ("pca", '{"n_components":true}'),
        ("pca", '{"n_components":0}'),
    ),
)
def test_reference_unknown_or_invalid_policy_does_not_select_another_fit(
    family, options
):
    from histdatacom.data_quality.training_preprocessing_math import (
        fit_reference_transform,
    )

    with pytest.raises((TypeError, ValueError)):
        fit_reference_transform(
            family, ("x",), ((1,), (2,)), (Fraction(1),) * 2, options
        )


@pytest.mark.parametrize("family", ("zscale", "robust_scale", "median_imputer"))
def test_all_missing_fit_cannot_invent_zero_or_a_median(family):
    from histdatacom.data_quality.training_preprocessing_math import (
        apply_reference_transform,
        fit_reference_transform,
    )

    fit = fit_reference_transform(
        family, ("missing",), ((None,), (None,)), (Fraction(1),) * 2
    )
    assert apply_reference_transform(fit, ((None,),)) == ((None,),)


@pytest.mark.parametrize(
    "family",
    ("pca", "svd", "whitening", "population_reduction", "linear_embedding"),
)
def test_projection_no_complete_support_does_not_fabricate_a_fit(family):
    from histdatacom.data_quality.training_preprocessing_math import (
        fit_reference_transform,
    )

    with pytest.raises(ValueError, match="complete"):
        fit_reference_transform(
            family,
            ("x", "y"),
            ((1, None), (None, 2)),
            (Fraction(1),) * 2,
        )


def test_raw_quote_rolling_is_not_granted_availability(preprocessing_native):
    from histdatacom.data_quality.training_preprocessing_views import (
        fit_training_preprocessing,
    )
    from tests.fixtures.training_preprocessing_v1 import preprocessing_plan
    from tests.fixtures.training_substrate_v1 import BASE
    from tests.fixtures.training_temporal_v1 import SECOND

    plan = preprocessing_plan(
        preprocessing_native,
        fit_mode="rolling_pti",
        rolling_window_ns=10 * SECOND,
    )
    with pytest.raises(ValueError, match="GRID/BAR|clocked added features"):
        fit_training_preprocessing(plan, cutoff_ns=BASE + 4 * SECOND)


@pytest.fixture(scope="module")
def native_fit(preprocessing_native):
    from histdatacom.data_quality.training_preprocessing_views import (
        fit_training_preprocessing,
    )
    from tests.fixtures.training_preprocessing_v1 import preprocessing_plan

    return fit_training_preprocessing(preprocessing_plan(preprocessing_native))


@pytest.mark.parametrize("mutation", ("parameters", "environment", "weight"))
def test_resealed_parameters_and_environment_refuse(native_fit, mutation):
    from histdatacom.data_quality.training_contracts import training_json
    from histdatacom.data_quality.training_preprocessing_math import (
        ReferenceTransformFitV1,
    )
    from histdatacom.data_quality.training_preprocessing_views import (
        replay_training_preprocessing_fit,
    )

    if mutation == "weight":
        membership = json.loads(native_fit.membership_json)
        membership["rows"][0]["mass"] = "1"
        changed = replace(native_fit, membership_json=training_json(membership))
    else:
        kernel = ReferenceTransformFitV1.from_json(
            native_fit.steps[0].kernel_fit_json
        )
        if mutation == "parameters":
            parameters = json.loads(kernel.parameters_json)
            # Shape-valid within the retained min/max, but not the native
            # TRAIN mean 5/2: authoritative replay, not DTO shape, must refuse.
            parameters["columns"][0]["mean"] = "3"
            kernel = replace(kernel, parameters_json=training_json(parameters))
        elif mutation == "environment":
            environment = json.loads(kernel.environment_json)
            environment["python"] = "different-runtime"
            kernel = replace(
                kernel, environment_json=training_json(environment)
            )
        changed = replace(
            native_fit,
            steps=(
                replace(native_fit.steps[0], kernel_fit_json=kernel.to_json()),
            ),
        )
    with pytest.raises((ValueError, TypeError)):
        replay_training_preprocessing_fit(
            changed, expected_fit_id=changed.artifact_id
        )


@pytest.mark.parametrize(
    "family",
    (
        "zscale",
        "median_imputer",
        "quantile_rank",
        "pca",
        "categorical",
        "variance_selector",
    ),
)
def test_train_plus_protected_fit_canary_refuses(preprocessing_native, family):
    from histdatacom.data_quality.training_temporal_contracts import (
        TemporalPartition,
    )
    from tests.fixtures.training_preprocessing_v1 import preprocessing_plan

    units = tuple(
        sorted(
            assignment.evidence_unit_id
            for assignment in preprocessing_native["split"].assignments
            if assignment.partition
            in (TemporalPartition.TRAIN, TemporalPartition.VALIDATION)
        )
    )
    with pytest.raises(ValueError, match="protected or unknown"):
        preprocessing_plan(
            preprocessing_native, family=family, fit_unit_ids=units
        )


def test_future_backfill_canary_refuses(tmp_path):
    from histdatacom.data_quality.training_preprocessing_contracts import (
        PreprocessingFitMode,
        TrainingPreprocessingStepV1,
    )
    from histdatacom.data_quality.training_preprocessing_views import (
        materialize_training_preprocessing_view,
    )
    from tests.fixtures.training_preprocessing_v1 import (
        MACRO,
        build_preprocessing_vintage_fixture,
        preprocessing_plan,
    )

    native = build_preprocessing_vintage_fixture(
        tmp_path / "prerelease", include_initial_missing=True
    )
    base = preprocessing_plan(native, columns=(MACRO,))
    raw = replace(
        base, fit_mode=PreprocessingFitMode.NONE, fit_unit_ids=(), steps=()
    )
    view = materialize_training_preprocessing_view(raw, require_dense=False)
    assert view.records[0]["values"][MACRO] is None
    assert view.records[0]["originals"][MACRO]["value"] is None
    assert view.records[1]["values"][MACRO] == 1
    # VINTAGE is a cutoff-specific SNAPSHOT, not persistent STATE. Future
    # values cannot be used as backfill under an invented carry instruction.
    attempted = replace(
        raw,
        steps=(
            TrainingPreprocessingStepV1(
                "carry", "prior_state", (MACRO,), '{"max_age_ns":10000000000}'
            ),
        ),
    )
    with pytest.raises(ValueError, match="persistent state"):
        materialize_training_preprocessing_view(attempted, require_dense=False)
    with pytest.raises(ValueError, match="unresolved missing"):
        materialize_training_preprocessing_view(raw)


def test_rolling_cutoff_or_external_unknown_clock_cannot_be_relabelled(
    preprocessing_vintage,
):
    from histdatacom.data_quality.training_preprocessing_views import (
        apply_training_preprocessing,
        fit_training_preprocessing,
    )
    from tests.fixtures.training_preprocessing_v1 import (
        MACRO,
        preprocessing_plan,
    )
    from tests.fixtures.training_substrate_v1 import BASE
    from tests.fixtures.training_temporal_v1 import SECOND

    plan = preprocessing_plan(
        preprocessing_vintage,
        columns=(MACRO,),
        fit_mode="rolling_pti",
        rolling_window_ns=10 * SECOND,
    )
    fit = fit_training_preprocessing(plan, cutoff_ns=BASE + 2 * SECOND)
    with pytest.raises(ValueError, match="exact fitted cutoff"):
        apply_training_preprocessing(
            fit, expected_fit_id=fit.artifact_id, cutoff_ns=BASE + 3 * SECOND
        )
    external = preprocessing_plan(
        preprocessing_vintage, columns=(MACRO,), fit_mode="fixed_external"
    )
    with pytest.raises(ValueError, match="known cutoff"):
        fit_training_preprocessing(external)


def test_frozen_view_snapshot_tampering_refuses(native_fit):
    from histdatacom.data_quality.training_contracts import training_json
    from histdatacom.data_quality.training_preprocessing_views import (
        apply_training_preprocessing,
        preprocessing_records,
    )

    view = apply_training_preprocessing(
        native_fit, expected_fit_id=native_fit.artifact_id
    )
    rows = list(view.records)
    rows[0]["values"]["market.tick.EURUSD.bid"] = 999
    tampered = replace(view, records_json=training_json({"records": rows}))
    with pytest.raises(ValueError, match="complete native replay"):
        preprocessing_records(tampered)


def test_native_source_mutation_and_resealed_wide_values_refuse(tmp_path):
    from histdatacom.data_quality.training_contracts import training_json
    from histdatacom.data_quality.training_preprocessing_views import (
        fit_training_preprocessing,
        replay_training_preprocessing_fit,
    )
    from tests.fixtures.training_preprocessing_v1 import (
        build_preprocessing_fixture,
        preprocessing_plan,
    )

    native = build_preprocessing_fixture(tmp_path / "own-input")
    plan = preprocessing_plan(native)
    fit = fit_training_preprocessing(plan)
    # Alter a retained source not selected as a numerical fit column. Full
    # admission must still reject it; selected-feature hashing is insufficient.
    artifact = native["artifact_path"]
    original = artifact.read_bytes()
    try:
        artifact.write_bytes(original + b"\n")
        with pytest.raises((ValueError, OSError)):
            replay_training_preprocessing_fit(
                fit, expected_fit_id=fit.artifact_id
            )
    finally:
        artifact.write_bytes(original)
    tile = plan.wide_plan.tiles[0]
    spine = tile.join_plan.spine
    changed_value = json.loads(spine.rows[0].value_json)
    changed_value["bid"] = 999.0
    changed_spine = replace(
        spine,
        rows=(replace(spine.rows[0], value_json=training_json(changed_value)),)
        + spine.rows[1:],
    )
    changed_join = replace(tile.join_plan, spine=changed_spine)
    changed_wide = replace(
        plan.wide_plan,
        tiles=(replace(tile, join_plan_json=changed_join.to_json()),),
    )
    changed_plan = replace(plan, wide_plan=changed_wide)
    assert changed_plan.artifact_id != plan.artifact_id
    with pytest.raises(
        ValueError,
        match="join spine differs from complete canonical source replay",
    ):
        fit_training_preprocessing(changed_plan)
