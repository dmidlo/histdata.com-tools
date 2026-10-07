"""Independent synthetic arithmetic and real native fit/apply consumers.

Fixture source admission and numerical reference vectors are separate proofs.
Neither establishes historical availability or empirical learner usefulness.
"""

from __future__ import annotations

import json
import math
from dataclasses import replace
from fractions import Fraction

import pytest

from histdatacom.data_quality.training_join_contracts import JoinState
from histdatacom.data_quality.training_temporal_contracts import (
    TemporalPartition,
)

pytest_plugins = ("tests.fixtures.training_preprocessing_v1",)

BID = "market.tick.EURUSD.bid"
ASK = "market.tick.EURUSD.ask"
SPREAD = "market.tick.EURUSD.spread"
ABSENT = "uncertainty.EURUSD.unavailable"
OTHER_SYMBOL = "market.tick.GBPUSD.bid"
MACRO = "calendar.fixture.level"
DAY_BIDS = (
    (1.0, 2.0, 3.0, 4.0),
    (2.0, 4.0, 6.0, 8.0),
    (100.0, 110.0, 120.0, 130.0),
    (200.0, 210.0, 220.0, 230.0),
    (300.0, 310.0, 320.0, 330.0),
    (400.0, 410.0, 420.0, 430.0),
)
SPREADS = (0.5, 1.0, 1.5, 0.5)


def test_fixture_native_values_splits_and_missingness(preprocessing_native):
    native = preprocessing_native
    records = native["records"]
    assert len(records) == 24
    assignments = {
        item.evidence_unit_id: item.partition
        for item in native["split"].assignments
    }
    selected = []
    for index, record in enumerate(records):
        value = DAY_BIDS[index // 4][index % 4]
        assert record["values"][BID] == value
        assert record["values"][ASK] == value + SPREADS[index % 4]
        assert record["values"][SPREAD] == SPREADS[index % 4]
        assert record["values"][ABSENT] is None
        assert record["values"][OTHER_SYMBOL] is None
        assert record["states"][ABSENT] == JoinState.UNSUPPORTED.value
        assert record["states"][OTHER_SYMBOL] == JoinState.NOT_APPLICABLE.value
        assert record["spine"]["origin"] == "observed"
        assert record["cell_provenance"][BID]["source_ids"]
        selected.append(assignments[record["spine"]["evidence_unit_id"]])
    assert selected == (
        [TemporalPartition.TRAIN] * 8
        + [TemporalPartition.VALIDATION] * 8
        + [TemporalPartition.TEST] * 8
    )
    assert len({r["spine"]["evidence_unit_id"] for r in records}) == 6


def test_fixture_normalized_revision_grid_replays_known_prefix(
    preprocessing_vintage,
):
    native = preprocessing_vintage
    assert tuple(row["values"][MACRO] for row in native["records"]) == (
        1.0,
        2.0,
        3.0,
        4.0,
    )
    for row in native["records"]:
        cell = row["cell_provenance"][MACRO]
        assert cell["available_at_ns"] == row["grid_cutoff_ns"]
        assert cell["dependency_end_ns"] <= row["grid_cutoff_ns"] + 1
        assert cell["historical_availability_verified"] is False
        assert row["spine"]["available_at_ns"] is None
        assert row["spine"]["information_mode"] == "ex_post"


def test_fixture_future_revision_and_quote_count_do_not_change_grid_prefix(
    tmp_path, preprocessing_vintage
):
    from tests.fixtures.training_preprocessing_v1 import (
        build_preprocessing_vintage_fixture,
    )

    changed = build_preprocessing_vintage_fixture(
        tmp_path / "changed", future_value=9999.0, extra_future_quotes=7
    )
    truncated = build_preprocessing_vintage_fixture(
        tmp_path / "truncated", include_future=False
    )
    for candidate in (changed, truncated):
        assert tuple(
            (row["grid_cutoff_ns"], row["values"], row["states"])
            for row in candidate["records"]
        ) == tuple(
            (row["grid_cutoff_ns"], row["values"], row["states"])
            for row in preprocessing_vintage["records"]
        )
        # Different full input roots remain different; this is numerical
        # prefix invariance, not a false claim that the input files match.
        assert candidate["source"].artifact_id != (
            preprocessing_vintage["source"].artifact_id
        )


def test_fixture_calendar_has_real_known_state_donors_and_native_gaps(
    preprocessing_calendar,
):
    native = preprocessing_calendar
    column = native["column"]
    assert tuple(record["values"][column] for record in native["records"]) == (
        3.0,
        None,
        9.0,
        None,
        4.0,
        None,
        12.0,
        None,
        2.0,
        None,
        8.0,
        None,
    )
    for index, record in enumerate(native["records"]):
        cell = record["cell_provenance"][column]
        if index % 2:
            assert record["states"][column] == JoinState.UNAVAILABLE.value
        else:
            assert record["states"][column] == JoinState.AVAILABLE.value
            assert cell["available_at_ns"] == native["clocks"][index]
            assert cell["source_time_ns"] == native["clocks"][index]
        assert cell["historical_availability_verified"] is False


def test_native_siblings_preserve_unit_mass(preprocessing_siblings):
    from histdatacom.data_quality.training_preprocessing_native import (
        inspect_training_preprocessing_native,
    )

    fixture = preprocessing_siblings
    full = inspect_training_preprocessing_native(
        fixture["wide_plan"], fixture["split"]
    )
    selected = inspect_training_preprocessing_native(
        fixture["selected_plan"], fixture["split"]
    )
    assert len(full.rows) == 6
    assert len(selected.rows) == 3
    assert {row.weight for row in full.rows} == {Fraction(1, 18)}
    assert {row.weight for row in selected.rows} == {Fraction(1, 18)}
    assert sum((row.weight for row in full.rows), Fraction()) == Fraction(1, 3)
    assert sum((row.weight for row in selected.rows), Fraction()) == Fraction(
        1, 6
    )
    strata = {row.stratum_id for row in full.rows}
    assert len(strata) == 1
    denominator = tuple(d for d in full.denominators if d.stratum_id in strata)
    assert len(denominator) == 2
    assert {d.complete_row_count for d in denominator} == {9}
    assert {d.member_mass for d in denominator} == {Fraction(1, 2)}
    assert {d.selected_mass for d in denominator} == {Fraction(1, 6)}
    origins = [row.native_row.origin.value for row in full.rows]
    assert origins.count("observed") == 4
    assert origins.count("synthetic_reconstruction") == 2


def test_reference_independent_exact_weighted_moments_and_quantiles():
    from histdatacom.data_quality.training_preprocessing_math import (
        fit_reference_transform,
    )
    from tests.fixtures.training_preprocessing_v1 import (
        weighted_moments,
        weighted_quantile,
    )

    values = (Fraction(1), Fraction(2), Fraction(4), Fraction(100))
    weights = (Fraction(1, 2), Fraction(1, 4), Fraction(1, 4), Fraction(0))
    mean, variance = weighted_moments(values, weights)
    assert (mean, variance) == (Fraction(2), Fraction(3, 2))
    fit = fit_reference_transform(
        "robust_scale", ("x",), tuple((x,) for x in values), weights
    )
    stats = json.loads(fit.parameters_json)["columns"][0]
    assert Fraction(stats["mean"]) == mean
    assert Fraction(stats["variance"]) == variance
    assert (
        Fraction(stats["median"])
        == weighted_quantile(values, weights, Fraction(1, 2))
        == 1
    )
    assert Fraction(stats["mad"]) == 0
    support = json.loads(fit.support_json)["columns"][0]
    assert support["zero_weight_rows"] == 1
    assert Fraction(support["supported_mass"]) == 1


@pytest.mark.parametrize(
    "family,options,expected",
    (
        ("robust_scale", "{}", (Fraction(2), None)),
        ("minmax", "{}", (Fraction(1), None)),
        ("winsorize", '{"lower":"1/4","upper":"3/4"}', (Fraction(3), None)),
        ("quantile_rank", "{}", (Fraction(1), None)),
        ("median_imputer", "{}", (Fraction(4), Fraction(2))),
    ),
)
def test_reference_exact_scalar_families_independent_oracle(
    family, options, expected
):
    from histdatacom.data_quality.training_preprocessing_math import (
        apply_reference_transform,
        fit_reference_transform,
    )

    fit = fit_reference_transform(
        family,
        ("x",),
        ((1,), (2,), (3,), (4,)),
        (Fraction(1, 4),) * 4,
        options,
    )
    before = fit.to_json()
    result = apply_reference_transform(fit, ((4,), (None,)))
    assert result == tuple((value,) for value in expected)
    assert fit.to_json() == before


def test_reference_train_only_zscore_matches_literal_leakage_example():
    from histdatacom.data_quality.training_preprocessing_math import (
        apply_reference_transform,
        fit_reference_transform,
    )

    fitted = fit_reference_transform(
        "zscale", ("x",), ((1,), (2,), (3,), (4,)), (Fraction(1),) * 4
    )
    leaked = fit_reference_transform(
        "zscale",
        ("x",),
        ((1,), (2,), (3,), (4,), (100,)),
        (Fraction(1),) * 5,
    )
    assert json.loads(fitted.parameters_json)["columns"][0]["mean"] == "5/2"
    assert json.loads(leaked.parameters_json)["columns"][0]["mean"] == "22"
    assert apply_reference_transform(fitted, ((4,),))[0][0] == pytest.approx(
        3 / math.sqrt(5)
    )
    assert apply_reference_transform(leaked, ((4,),))[0][0] == pytest.approx(
        -18 / math.sqrt(1522)
    )
    # This exposes why membership is governed by the public native layer;
    # a low-level mathematical fit does not itself attest a TRAIN partition.
    assert fitted.fit_id != leaked.fit_id


def test_reference_categorical_unknown_and_missing_do_not_expand_vocabulary():
    from histdatacom.data_quality.training_preprocessing_math import (
        apply_reference_transform,
        fit_reference_transform,
    )

    fit = fit_reference_transform(
        "categorical",
        ("state.x",),
        (("available",), ("unsupported",), (None,)),
        (Fraction(1),) * 3,
        '{"ontology_id":"literal-native-state-test-ontology"}',
    )
    before = fit.to_json()
    assert json.loads(fit.parameters_json)["columns"][0]["categories"] == [
        "available",
        "unsupported",
    ]
    assert apply_reference_transform(
        fit, (("available",), ("not_applicable",), (None,))
    ) == ((1, 0, 0, 0), (0, 0, 1, 0), (0, 0, 0, 1))
    assert fit.to_json() == before


@pytest.mark.parametrize("family", ("variance_selector", "correlation_pruner"))
def test_reference_selection_and_pruning_keep_declared_priority(family):
    from histdatacom.data_quality.training_preprocessing_math import (
        apply_reference_transform,
        fit_reference_transform,
    )

    rows = ((1, 2, 9), (2, 4, 9), (3, 6, 9), (4, 8, 9))
    fit = fit_reference_transform(
        family, ("x", "twice_x", "constant"), rows, (Fraction(1),) * 4
    )
    expected = ("x", "twice_x") if family == "variance_selector" else ("x",)
    assert fit.output_columns == expected
    assert apply_reference_transform(fit, ((5, 10, 9),)) == (
        (
            (Fraction(5), Fraction(10))
            if family == "variance_selector"
            else (Fraction(5),)
        ),
    )


@pytest.mark.parametrize(
    "family",
    ("pca", "svd", "whitening", "population_reduction", "linear_embedding"),
)
def test_reference_projection_families_have_independent_axis_oracle(family):
    from histdatacom.data_quality.training_preprocessing_math import (
        apply_reference_transform,
        fit_reference_transform,
    )

    # Mean zero, diagonal covariance diag(5,1), distinct eigenvalues.
    rows = ((-3, 1), (-1, -1), (1, -1), (3, 1))
    fit = fit_reference_transform(
        family, ("x", "y"), rows, (Fraction(1, 4),) * 4
    )
    result = apply_reference_transform(fit, ((2, 7), (None, 7)))
    assert result[0][0] == pytest.approx(
        2 / math.sqrt(5) if family == "whitening" else 2
    )
    assert result[1] == (None,)
    projection = json.loads(fit.parameters_json)["projection"]
    assert tuple(Fraction(value) for value in projection["center"]) == (0, 0)
    assert float.fromhex(projection["eigenvalues"][0]) == pytest.approx(5)
    vector = tuple(float.fromhex(value) for value in projection["basis"][0])
    assert vector == pytest.approx((1, 0), abs=1e-12)


def _fit_apply(plan, *, cutoff_ns=None, require_dense=True):
    from histdatacom.data_quality.training_preprocessing_views import (
        apply_training_preprocessing,
        fit_training_preprocessing,
    )

    fit = fit_training_preprocessing(plan, cutoff_ns=cutoff_ns)
    view = apply_training_preprocessing(
        fit,
        expected_fit_id=fit.artifact_id,
        cutoff_ns=cutoff_ns,
        require_dense=require_dense,
    )
    return fit, view


def _kernel(fit, index=0):
    from histdatacom.data_quality.training_preprocessing_math import (
        ReferenceTransformFitV1,
    )

    return ReferenceTransformFitV1.from_json(fit.steps[index].kernel_fit_json)


@pytest.mark.parametrize(
    "family",
    (
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
    ),
)
def test_native_all_stateful_families_fit_apply_with_exact_lineage(
    preprocessing_native, family
):
    from tests.fixtures.training_preprocessing_v1 import preprocessing_plan

    columns = ("state." + BID,) if family == "categorical" else (BID, ASK)
    plan = preprocessing_plan(
        preprocessing_native, family=family, columns=columns
    )
    fit, view = _fit_apply(plan)
    kernel = _kernel(fit)
    assert kernel.family == family
    membership = json.loads(fit.membership_json)
    assert membership["fit_unit_ids"] == list(plan.fit_unit_ids)
    assert len(membership["rows"]) == 4
    assert {Fraction(row["mass"]) for row in membership["rows"]} == {
        Fraction(1, 12)
    }
    assert {row["unit_id"] for row in membership["rows"]} == set(
        plan.fit_unit_ids
    )
    assert len(view.records) == 24
    assert {row["partition"] for row in view.records} == {
        "train",
        "validation",
        "test",
    }
    assert all(row["fit_id"] == fit.artifact_id for row in view.records)
    for row in view.records:
        assert row["kind"] == "dense_model"
        assert row["parameter_identity"] == fit.parameter_identity
        assert row["step_ids"] == [plan.steps[0].artifact_id]
        assert set(row["schema"]) == set(row["values"])
        for name, descriptor in row["schema"].items():
            assert descriptor["descriptor"]["kernel"] == kernel.fit_id
            assert descriptor["descriptor"]["family"] == family
            expected_dependencies = (
                sorted(columns)
                if family
                in (
                    "pca",
                    "svd",
                    "whitening",
                    "population_reduction",
                    "linear_embedding",
                )
                else [columns[0]] if family == "categorical" else [name]
            )
            assert descriptor["native_dependencies"] == expected_dependencies
            assert descriptor["lineage_id"].startswith(
                "preprocessing-view-lineage:sha256:"
            )
            assert descriptor["pipeline_lineage_id"].startswith(
                "preprocessing-output-lineage:sha256:"
            )
            assert math.isfinite(row["values"][name])
    assert json.loads(kernel.environment_json)
    if family == "zscale":
        assert all(
            row["originals"][BID]["missingness"] == "observed"
            for row in view.records
        )
        stats = json.loads(kernel.parameters_json)["columns"]
        assert Fraction(stats[0]["mean"]) == Fraction(5, 2)
        assert Fraction(stats[0]["variance"]) == Fraction(5, 4)
        assert view.records[3]["values"][BID] == pytest.approx(3 / math.sqrt(5))
        assert view.records[8]["values"][BID] == pytest.approx(
            195 / math.sqrt(5)
        )
    if family == "categorical":
        assert json.loads(kernel.parameters_json)["columns"][0][
            "categories"
        ] == ["available"]
        assert all(
            tuple(row["values"].values()) == (1, 0, 0) for row in view.records
        )


def test_native_train_membership_and_parameter_oracle(preprocessing_native):
    from tests.fixtures.training_preprocessing_v1 import (
        preprocessing_plan,
        weighted_moments,
    )

    units = tuple(
        sorted(
            {
                row["spine"]["evidence_unit_id"]
                for row in preprocessing_native["records"][:8]
            }
        )
    )
    fit, view = _fit_apply(
        preprocessing_plan(preprocessing_native, fit_unit_ids=units)
    )
    values = tuple(Fraction(value) for day in DAY_BIDS[:2] for value in day)
    mean, variance = weighted_moments(values, (Fraction(1, 12),) * 8)
    stats = json.loads(_kernel(fit).parameters_json)["columns"][0]
    assert Fraction(stats["mean"]) == mean == Fraction(15, 4)
    assert Fraction(stats["variance"]) == variance == Fraction(75, 16)
    assert len(json.loads(fit.membership_json)["rows"]) == 8
    assert json.loads(fit.support_json)["selected_mass"] == "2/3"
    assert view.records[8]["values"][BID] == pytest.approx(
        (100 - float(mean)) / math.sqrt(float(variance))
    )


def test_native_evidence_and_dense_layers_keep_originals(
    preprocessing_calendar,
):
    from histdatacom.data_quality.training_preprocessing import (
        evidence_records,
        materialize_training_evidence_view,
    )
    from tests.fixtures.training_preprocessing_v1 import preprocessing_plan

    name = preprocessing_calendar["column"]
    plan = preprocessing_plan(
        preprocessing_calendar, family="median_imputer", columns=(name,)
    )
    evidence = evidence_records(materialize_training_evidence_view(plan))
    native_definition = next(
        column
        for column in plan.wide_plan.columns
        if column.column.name == name
    )
    assert (
        evidence[0]["schema"][name]["descriptor"]["native_definition"]
        == native_definition.to_dict()
    )
    assert all(
        row["originals"][name]["missingness"] == "derived"
        for row in evidence[::2]
    )
    # The last exact-join gap has only past candidates, so it is refused,
    # not an upcoming observation, observed zero, or available state.
    assert evidence[-1]["originals"][name]["missingness"] == "refused"
    fit, view = _fit_apply(plan)
    assert (
        tuple(row["values"][name] for row in evidence)
        == preprocessing_calendar["values"]
    )
    assert tuple(row["values"][name] for row in view.records) == (
        3,
        3,
        9,
        3,
        4,
        3,
        12,
        3,
        2,
        3,
        8,
        3,
    )
    assert (
        json.loads(_kernel(fit).parameters_json)["columns"][0]["median"] == "3"
    )
    for original, dense in zip(evidence, view.records):
        assert original["originals"] == dense["originals"]
        assert original["kind"] == "evidence"
        assert dense["kind"] == "dense_model"
        assert dense["masks"][name] is (original["values"][name] is None)
        assert dense["output_status"][name] == "derived"
        assert dense["fit_support"]["historical_availability_verified"] is False
        support = dense["output_support"][name]
        assert support["is_observed_or_supported"] is (
            original["values"][name] is not None
        )
        assert support["has_missing_native_input"] is (
            original["values"][name] is None
        )
        assert support["methods"] == ["median_imputer"]
        assert support["policy_id"] == plan.artifact_id
        assert support["confidence"] == "not_calibrated"
        if original["values"][name] is None:
            assert (
                dense["imputation_evidence"][name]["method"] == "median_imputer"
            )
        assert (
            dense["schema"][name]["lineage_id"]
            != original["schema"][name]["lineage_id"]
        )


def test_prior_state_fill_is_bounded_and_aged(preprocessing_calendar):
    from histdatacom.data_quality.training_preprocessing_contracts import (
        TrainingPreprocessingStepV1,
    )
    from tests.fixtures.training_preprocessing_v1 import preprocessing_plan
    from tests.fixtures.training_temporal_v1 import SECOND

    name = preprocessing_calendar["column"]
    plan = preprocessing_plan(
        preprocessing_calendar, family="median_imputer", columns=(name,)
    )
    prior = TrainingPreprocessingStepV1(
        "prior", "prior_state", (name,), '{"max_age_ns":1000000000}'
    )
    plan = replace(plan, steps=(prior,) + plan.steps)
    _, view = _fit_apply(plan)
    assert tuple(row["values"][name] for row in view.records) == (
        3,
        3,
        9,
        9,
        4,
        4,
        12,
        12,
        2,
        2,
        8,
        8,
    )
    for row in view.records[1::2]:
        evidence = row["imputation_evidence"][name]
        assert evidence["method"] == "prior_state"
        assert evidence["age_ns"] == SECOND
        assert evidence["policy_id"] == prior.artifact_id
        assert row["originals"][name]["value"] is None
        assert row["originals"][name]["native_state"] == "unavailable"
        assert row["masks"][name] is True
        for donor in evidence["donors"]:
            assert (
                donor["provenance"]["available_at_ns"]
                < row["originals"][name]["decision_time_ns"]
            )
    shorter = replace(
        plan,
        steps=(replace(prior, options_json='{"max_age_ns":0}'),)
        + plan.steps[1:],
    )
    _, short_view = _fit_apply(shorter)
    assert (
        tuple(row["values"][name] for row in short_view.records)[1::2]
        == (3,) * 6
    )
    for index, row in enumerate(short_view.records):
        if index % 2:
            evidence = row["imputation_evidence"][name]
            assert evidence["method"] == "median_imputer"
            assert evidence["support"]["supported_rows"] == 2
            assert all(
                event["method"] != "prior_state" for event in evidence["events"]
            )
            assert "donors" not in evidence
        else:
            assert not row["imputation_evidence"]


def test_frozen_synthetic_imputation_and_mask_comparison(
    preprocessing_calendar,
    record_property,
):
    import numpy as np

    from histdatacom.data_quality.training_preprocessing_contracts import (
        TrainingPreprocessingStepV1,
    )
    from tests.fixtures.training_preprocessing_v1 import preprocessing_plan

    # Policies, target, learner, threshold, and ridge penalty are fixed before
    # any TEST prediction. This is a software comparison, not policy selection.
    name = preprocessing_calendar["column"]
    simple = preprocessing_plan(
        preprocessing_calendar, family="median_imputer", columns=(name,)
    )
    richer = replace(
        simple,
        steps=(
            TrainingPreprocessingStepV1(
                "prior", "prior_state", (name,), '{"max_age_ns":1000000000}'
            ),
        )
        + simple.steps,
    )
    labels = np.asarray(preprocessing_calendar["labels"], dtype=float)
    expected_predictions = {
        ("simple", False): (13 / 56, 19 / 56, 49 / 56, 19 / 56),
        ("simple", True): (4 / 47, 19 / 47, 40 / 47, 19 / 47),
        ("richer", False): (-11 / 74, -11 / 74, 61 / 74, 61 / 74),
        ("richer", True): (-11 / 74, -11 / 74, 61 / 74, 61 / 74),
    }
    results = {}
    for policy_name, plan in (("simple", simple), ("richer", richer)):
        fit, view = _fit_apply(plan)
        records = view.records
        for masked in (False, True):
            x = np.asarray(
                [
                    [
                        1.0,
                        row["values"][name],
                        *([float(row["masks"][name])] if masked else []),
                    ]
                    for row in records
                ]
            )
            train = np.asarray([row["partition"] == "train" for row in records])
            test = np.asarray([row["partition"] == "test" for row in records])
            penalty = np.eye(x.shape[1])
            penalty[0, 0] = 0.0
            coefficients = np.linalg.solve(
                x[train].T @ x[train] + penalty, x[train].T @ labels[train]
            )
            prediction = x[test] @ coefficients
            results[(policy_name, masked)] = (
                coefficients,
                prediction,
                float(np.mean((prediction - labels[test]) ** 2)),
            )
            assert np.isfinite(prediction).all()
            assert len(prediction) == 4
            assert prediction == pytest.approx(
                expected_predictions[(policy_name, masked)], abs=1e-12
            )
            assert len(json.loads(fit.membership_json)["rows"]) == 4
            record_property(
                f"{policy_name}_{'masked' if masked else 'unmasked'}",
                json.dumps(
                    {
                        "scope": "synthetic_functionality_not_empirical_utility",
                        "policy_selection": False,
                        "fit_id": fit.artifact_id,
                        "training_rows": 4,
                        "coefficients": coefficients.tolist(),
                        "test_predictions": prediction.tolist(),
                        "test_mse": results[(policy_name, masked)][2],
                    },
                    sort_keys=True,
                ),
            )
    assert set(results) == {
        ("simple", False),
        ("simple", True),
        ("richer", False),
        ("richer", True),
    }
    assert not np.allclose(
        results[("simple", False)][1], results[("simple", True)][1]
    )
    assert not np.allclose(
        results[("simple", True)][1], results[("richer", True)][1]
    )
    assert all(math.isfinite(result[2]) for result in results.values())


def test_native_rolling_future_truncation(tmp_path, preprocessing_vintage):
    from tests.fixtures.training_preprocessing_v1 import (
        build_preprocessing_vintage_fixture,
        preprocessing_plan,
    )
    from tests.fixtures.training_substrate_v1 import BASE
    from tests.fixtures.training_temporal_v1 import SECOND

    candidates = (
        preprocessing_vintage,
        build_preprocessing_vintage_fixture(
            tmp_path / "revised", future_value=99999.0, extra_future_quotes=9
        ),
        build_preprocessing_vintage_fixture(
            tmp_path / "truncated", include_future=False
        ),
    )
    fits, outputs = [], []
    for native in candidates:
        plan = preprocessing_plan(
            native,
            columns=(MACRO,),
            fit_mode="rolling_pti",
            rolling_window_ns=10 * SECOND,
        )
        fit, view = _fit_apply(plan, cutoff_ns=BASE + 3 * SECOND)
        assert len(json.loads(fit.membership_json)["rows"]) == 3
        assert len(view.records) == 1
        assert view.records[0]["originals"][MACRO]["value"] == 3.0
        assert (
            view.records[0]["originals"][MACRO]["provenance"]["available_at_ns"]
            <= BASE + 3 * SECOND
        )
        assert view.records[0]["values"][MACRO] == pytest.approx(math.sqrt(1.5))
        fits.append(fit)
        outputs.append(view.records[0]["values"])
    assert outputs[0] == outputs[1] == outputs[2]
    assert len({_kernel(fit).to_json() for fit in fits}) == 1
    # Full source identities legitimately differ; no false same-file claim.
    assert len({fit.artifact_id for fit in fits}) == 3


def test_actual_external_calibration_fit_and_application(
    tmp_path, preprocessing_vintage
):
    from histdatacom.data_quality.training_contracts import DAY_NS
    from histdatacom.data_quality.training_preprocessing_views import (
        apply_training_preprocessing,
        fit_training_preprocessing,
    )
    from tests.fixtures.training_preprocessing_v1 import (
        build_preprocessing_vintage_fixture,
        preprocessing_plan,
    )
    from tests.fixtures.training_substrate_v1 import BASE
    from tests.fixtures.training_temporal_v1 import SECOND

    calibration = preprocessing_plan(
        preprocessing_vintage, columns=(MACRO,), fit_mode="fixed_external"
    )
    fit = fit_training_preprocessing(calibration, cutoff_ns=BASE + 4 * SECOND)
    independent = build_preprocessing_vintage_fixture(
        tmp_path / "independent", base_ns=BASE + DAY_NS
    )
    application = preprocessing_plan(
        independent, columns=(MACRO,), fit_mode="fixed_external"
    )
    view = apply_training_preprocessing(
        fit,
        expected_fit_id=fit.artifact_id,
        plan=application,
        cutoff_ns=BASE + DAY_NS + 4 * SECOND,
    )
    assert len(view.records) == 1
    assert view.records[0]["values"][MACRO] == pytest.approx(3 / math.sqrt(5))
    assert view.records[0]["fit_id"] == fit.artifact_id
    assert (
        application.wide_plan.artifact_id != calibration.wide_plan.artifact_id
    )
    with pytest.raises(ValueError, match="overlaps"):
        apply_training_preprocessing(
            fit,
            expected_fit_id=fit.artifact_id,
            plan=calibration,
            cutoff_ns=BASE + 4 * SECOND,
        )


def test_native_state_ontology_unknown_is_not_expanded(tmp_path):
    from tests.fixtures.training_preprocessing_v1 import (
        build_preprocessing_calendar_fixture,
        preprocessing_plan,
    )

    native = build_preprocessing_calendar_fixture(
        tmp_path / "new-state", train_gaps=False
    )
    name = "state." + native["column"]
    fit, view = _fit_apply(
        preprocessing_plan(native, columns=(name,), family="categorical")
    )
    kernel = _kernel(fit)
    assert json.loads(kernel.parameters_json)["columns"][0]["categories"] == [
        "available"
    ]
    columns = kernel.output_columns
    assert len(columns) == 3
    assert json.loads(kernel.options_json)["ontology_id"].startswith(
        "preprocessing-ontology:sha256:"
    )
    for row in view.records:
        expected = (
            (1, 0, 0)
            if row["originals"][name]["value"] == "available"
            else (0, 1, 0)
        )
        assert tuple(row["values"][c] for c in columns) == expected
    assert any(
        row["partition"] == "validation"
        and row["originals"][name]["value"] == "unavailable"
        for row in view.records
    )


def test_no_fit_mode_is_explicit_identity(preprocessing_native):
    from histdatacom.data_quality.training_preprocessing_contracts import (
        PreprocessingFitMode,
    )
    from histdatacom.data_quality.training_preprocessing_views import (
        materialize_training_preprocessing_view,
    )
    from tests.fixtures.training_preprocessing_v1 import preprocessing_plan

    base = preprocessing_plan(preprocessing_native)
    plan = replace(
        base, fit_mode=PreprocessingFitMode.NONE, fit_unit_ids=(), steps=()
    )
    view = materialize_training_preprocessing_view(plan)
    assert plan.artifact_id != base.artifact_id
    assert view.fit is None
    assert tuple(row["values"][BID] for row in view.records) == tuple(
        v for day in DAY_BIDS for v in day
    )
    assert all(
        row["fit_id"] is None and row["parameter_identity"] is None
        for row in view.records
    )
    assert all(row["output_status"][BID] == "native" for row in view.records)


def test_fold_local_fit_cannot_be_reused_globally(preprocessing_native):
    from histdatacom.data_quality.training_preprocessing_views import (
        apply_training_preprocessing,
        fit_training_preprocessing,
    )
    from tests.fixtures.training_preprocessing_v1 import preprocessing_plan

    first = preprocessing_plan(preprocessing_native, fold_id="outer-0/inner-0")
    other = replace(first, fold_id="outer-0/inner-1")
    left, right = fit_training_preprocessing(first), fit_training_preprocessing(
        other
    )
    assert left.artifact_id != right.artifact_id
    assert left.parameter_identity != right.parameter_identity
    assert _kernel(left).parameters_json == _kernel(right).parameters_json
    left_view = apply_training_preprocessing(
        left, expected_fit_id=left.artifact_id
    )
    right_view = apply_training_preprocessing(
        right, expected_fit_id=right.artifact_id
    )
    assert left_view.records[0]["values"] == right_view.records[0]["values"]
    assert (
        left_view.records[0]["schema"][BID]["lineage_id"]
        != right_view.records[0]["schema"][BID]["lineage_id"]
    )
    with pytest.raises(ValueError, match="cross fold"):
        apply_training_preprocessing(
            left, expected_fit_id=left.artifact_id, plan=other
        )


def test_nested_folds_retain_distinct_fit_membership(preprocessing_native):
    from histdatacom.data_quality.training_preprocessing_views import (
        fit_training_preprocessing,
    )
    from tests.fixtures.training_preprocessing_v1 import preprocessing_plan

    units = tuple(
        sorted(
            {
                row["spine"]["evidence_unit_id"]
                for row in preprocessing_native["records"][:8]
            }
        )
    )
    fits = [
        fit_training_preprocessing(
            preprocessing_plan(
                preprocessing_native,
                fold_id=f"outer-0/inner-{index}",
                fit_unit_ids=(unit,),
            )
        )
        for index, unit in enumerate(units)
    ]
    assert {
        json.loads(_kernel(fit).parameters_json)["columns"][0]["mean"]
        for fit in fits
    } == {"5/2", "5"}
    assert len({fit.parameter_identity for fit in fits}) == 2
    assert {
        tuple(json.loads(fit.membership_json)["fit_unit_ids"]) for fit in fits
    } == {(unit,) for unit in units}


def test_native_sibling_fit_uses_exact_complete_roster_weights(
    preprocessing_siblings,
):
    from tests.fixtures.training_preprocessing_v1 import (
        preprocessing_plan,
        weighted_moments,
    )

    fit, view = _fit_apply(preprocessing_plan(preprocessing_siblings))
    rows = json.loads(fit.membership_json)["rows"]
    assert {Fraction(row["mass"]) for row in rows} == {Fraction(1, 18)}
    assert len(rows) == 6
    assert sum(Fraction(row["mass"]) for row in rows) == Fraction(1, 3)
    values = tuple(
        Fraction(record["values"][BID])
        for record in preprocessing_siblings["records"]
    )
    mean, variance = weighted_moments(values, (Fraction(1, 18),) * 6)
    stats = json.loads(_kernel(fit).parameters_json)["columns"][0]
    assert Fraction(stats["mean"]) == mean
    assert Fraction(stats["variance"]) == variance
    assert len(view.records) == 6


def test_native_train_imputer_then_scaler_replays_each_stage(
    preprocessing_calendar,
):
    from histdatacom.data_quality.training_preprocessing_contracts import (
        TrainingPreprocessingStepV1,
    )
    from tests.fixtures.training_preprocessing_v1 import preprocessing_plan

    name = preprocessing_calendar["column"]
    plan = preprocessing_plan(
        preprocessing_calendar, family="median_imputer", columns=(name,)
    )
    plan = replace(
        plan,
        steps=plan.steps
        + (TrainingPreprocessingStepV1("scale-imputed", "zscale", (name,)),),
    )
    fit, view = _fit_apply(plan)
    stats = json.loads(_kernel(fit, 1).parameters_json)["columns"][0]
    assert stats["mean"] == "9/2"
    assert stats["variance"] == "27/4"
    assert view.records[1]["values"][name] == pytest.approx(-1 / math.sqrt(3))
    assert view.records[1]["originals"][name]["value"] is None
    assert view.records[1]["masks"][name] is True
    assert view.records[1]["output_support"][name]["methods"] == [
        "median_imputer",
        "zscale",
    ]
    events = view.records[1]["imputation_evidence"][name]["events"]
    assert any(event["method"] == "median_imputer" for event in events)


def test_unsupported_facts_never_become_quiet_zero(preprocessing_native):
    from histdatacom.data_quality.training_preprocessing_contracts import (
        PreprocessingFitMode,
        TrainingPreprocessingStepV1,
    )
    from histdatacom.data_quality.training_preprocessing_views import (
        materialize_training_preprocessing_view,
    )
    from tests.fixtures.training_preprocessing_v1 import preprocessing_plan

    plan = preprocessing_plan(
        preprocessing_native,
        family="median_imputer",
        columns=(ABSENT, OTHER_SYMBOL),
    )
    plan = replace(
        plan,
        fit_mode=PreprocessingFitMode.NONE,
        fit_unit_ids=(),
        steps=(
            TrainingPreprocessingStepV1(
                "confirmed", "confirmed_zero", (ABSENT, OTHER_SYMBOL)
            ),
        ),
    )
    view = materialize_training_preprocessing_view(plan, require_dense=False)
    for row in view.records:
        assert row["values"] == {ABSENT: None, OTHER_SYMBOL: None}
        assert row["originals"][ABSENT]["missingness"] == "unsupported"
        assert row["originals"][OTHER_SYMBOL]["missingness"] == "not_applicable"
        assert row["masks"] == {ABSENT: True, OTHER_SYMBOL: True}
        assert not row["imputation_evidence"]
    with pytest.raises(ValueError, match="unresolved missing"):
        materialize_training_preprocessing_view(plan)


def test_requirement_asset_maps_full_public_scope_without_global_claims():
    import ast
    from importlib.resources import files
    from pathlib import Path

    from histdatacom.data_quality import training_preprocessing as public
    from histdatacom.data_quality import (
        training_preprocessing_views as implementation,
    )

    asset = json.loads(
        files("histdatacom.data_quality")
        .joinpath("assets/training_preprocessing_requirements_v1.json")
        .read_text()
    )
    assert asset["issues"] == [655, 697]
    assert asset["version"] == "1.0.0"
    assert asset["global_525_complete"] is False
    assert asset["global_691_complete"] is False
    assert asset["scientific_qualification"] == "not_claimed"
    assert asset["historical_availability_verification"] == "not_claimed"
    assert (
        asset["qualification_scope"]
        == "synthetic_native_replay_and_reference_math_only"
    )
    ids = [atom["id"] for atom in asset["atoms"]]
    assert len(ids) == len(set(ids)) >= 50
    root = Path(__file__).parent
    for atom in asset["atoms"]:
        assert atom["id"].startswith(str(atom["issue"]) + ".")
        for reference in (atom["test"], *atom.get("supporting_tests", [])):
            filename, function = reference.split("::")
            definitions = {
                node.name
                for node in ast.walk(ast.parse((root / filename).read_text()))
                if isinstance(node, ast.FunctionDef)
            }
            assert function in definitions, reference
    for name in (
        "fit_training_preprocessing",
        "apply_training_preprocessing",
        "materialize_training_evidence_view",
        "evidence_records",
        "preprocessing_records",
    ):
        assert name in public.__all__
        assert getattr(public, name) is getattr(implementation, name)


def test_rolling_application_does_not_numerically_touch_later_finite_extreme(
    tmp_path,
):
    from tests.fixtures.training_preprocessing_v1 import (
        build_preprocessing_vintage_fixture,
        preprocessing_plan,
    )
    from tests.fixtures.training_substrate_v1 import BASE
    from tests.fixtures.training_temporal_v1 import SECOND

    native = build_preprocessing_vintage_fixture(
        tmp_path / "extreme-future", future_value=1.7e308, grid_stop_second=6
    )
    assert native["records"][-1]["grid_cutoff_ns"] == BASE + 5 * SECOND
    assert native["records"][-1]["values"][MACRO] == 1.7e308
    plan = preprocessing_plan(
        native,
        columns=(MACRO,),
        fit_mode="rolling_pti",
        rolling_window_ns=10 * SECOND,
    )
    fit, view = _fit_apply(plan, cutoff_ns=BASE + 3 * SECOND)
    stats = json.loads(_kernel(fit).parameters_json)["columns"][0]
    assert stats["mean"] == "2"
    assert stats["variance"] == "2/3"
    assert len(view.records) == 1
    assert view.records[0]["values"][MACRO] == pytest.approx(math.sqrt(1.5))


def test_native_unequal_unit_counts_change_weighted_fit_not_unit_mass(tmp_path):
    from tests.fixtures.training_preprocessing_v1 import (
        build_preprocessing_fixture,
        preprocessing_plan,
        weighted_moments,
    )

    native = build_preprocessing_fixture(
        tmp_path / "unequal-units", sparse_second_train_day=True
    )
    units = tuple(
        sorted(
            {
                record["spine"]["evidence_unit_id"]
                for record in native["records"][:6]
            }
        )
    )
    fit, view = _fit_apply(preprocessing_plan(native, fit_unit_ids=units))
    membership = json.loads(fit.membership_json)["rows"]
    assert len(membership) == 6
    weights_by_row = {
        record["row_id"]: Fraction(record["mass"]) for record in membership
    }
    ordered_weights = tuple(
        weights_by_row[row["row_id"]] for row in view.records[:6]
    )
    assert ordered_weights == (Fraction(1, 12),) * 4 + (Fraction(1, 6),) * 2
    values = tuple(Fraction(value) for value in (1, 2, 3, 4, 2, 8))
    mean, variance = weighted_moments(values, ordered_weights)
    assert mean == Fraction(15, 4)
    assert variance == Fraction(107, 16)
    assert sum(values) / len(values) == Fraction(10, 3) != mean
    stats = json.loads(_kernel(fit).parameters_json)["columns"][0]
    assert Fraction(stats["mean"]) == mean
    assert Fraction(stats["variance"]) == variance
    assert (
        sum(ordered_weights[:4]) == sum(ordered_weights[4:]) == Fraction(1, 3)
    )


def test_native_scaling_to_projection_retains_both_input_paths(
    preprocessing_native,
):
    from histdatacom.data_quality.training_preprocessing_contracts import (
        TrainingPreprocessingStepV1,
    )
    from tests.fixtures.training_preprocessing_v1 import preprocessing_plan

    plan = preprocessing_plan(preprocessing_native, columns=(BID, ASK))
    plan = replace(
        plan,
        steps=plan.steps
        + (TrainingPreprocessingStepV1("project-scaled", "pca", (BID, ASK)),),
    )
    fit, view = _fit_apply(plan)
    for row in view.records:
        descriptor = row["schema"]["pca:0"]
        assert descriptor["native_dependencies"] == sorted((BID, ASK))
        paths = descriptor["transforms"]
        assert len(paths) == 3
        scales = [event for event in paths if event["family"] == "zscale"]
        assert {tuple(event["input_columns"]) for event in scales} == {
            (BID,),
            (ASK,),
        }
        assert {event["step_id"] for event in scales} == {
            plan.steps[0].artifact_id
        }
        assert paths[-1]["family"] == "pca"
        assert paths[-1]["kernel_id"] == _kernel(fit, 1).fit_id
        assert paths[-1]["input_columns"] == [BID, ASK]
