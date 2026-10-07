"""Hand-calculated pure references: no source/split/qualification authority."""

import json
import math
from dataclasses import FrozenInstanceError, replace
from decimal import Decimal, localcontext
from fractions import Fraction as F
from itertools import permutations

import pytest

from histdatacom.data_quality import training_preprocessing_math as kernel
from histdatacom.data_quality.training_preprocessing_math import (
    FAMILIES,
    PROJECTIONS,
    ReferenceTransformFitV1,
    apply_reference_transform as apply,
    fit_reference_transform as fit,
)


def numeric(family, values=(1, 2, 3, 4), weights=None, options="{}"):
    return fit(
        family,
        ("x",),
        tuple((value,) for value in values),
        tuple(F(1) for _ in values) if weights is None else weights,
        options,
    )


def parameters(result):
    return json.loads(result.parameters_json)


def test_train_only_zscale_literal_future_leak_canary():
    trained = numeric("zscale")
    leaked = numeric("zscale", (1, 2, 3, 4, 100))
    assert parameters(trained)["columns"][0]["mean"] == "5/2"
    assert parameters(trained)["columns"][0]["variance"] == "5/4"
    assert parameters(leaked)["columns"][0]["mean"] == "22"
    assert apply(trained, ((4,),))[0][0] == pytest.approx(1.3416407865)
    assert apply(leaked, ((4,),))[0][0] == pytest.approx(-0.4613868143)
    before = trained.to_json()
    apply(trained, ((100,), (10**9,), (None,)))
    assert trained.to_json() == before
    assert trained.fit_id != leaked.fit_id


def test_weighted_sibling_mass_and_exact_support():
    actual = numeric("zscale", (0, 10, 10), (F(1), F(1, 2), F(1, 2)))
    wrong = numeric("zscale", (0, 10, 10))
    assert parameters(actual)["columns"][0]["mean"] == "5"
    assert parameters(actual)["columns"][0]["variance"] == "25"
    assert parameters(wrong)["columns"][0]["mean"] == "20/3"
    assert apply(actual, ((10,),)) == ((1.0,),)
    support = json.loads(actual.support_json)["columns"][0]
    assert support == {
        "rows": 3,
        "positive_weight_rows": 3,
        "supported_rows": 3,
        "missing_rows": 0,
        "zero_weight_rows": 0,
        "total_mass": "2",
        "supported_mass": "2",
        "missing_mass": "0",
    }


def test_binary64_convention_and_decimal_context_independence():
    with localcontext() as context:
        context.prec = 1
        actual = numeric("median_imputer", (0.1,))
    expected = F.from_float(0.1)
    assert parameters(actual)["columns"][0]["median"] == str(expected)
    assert expected != F(1, 10)
    assert apply(actual, ((None,),)) == ((expected,),)


def test_weighted_robust_median_mad_literal():
    actual = numeric("robust_scale", (0, 2, 8), (F(1), F(2), F(1)))
    assert parameters(actual)["columns"][0]["median"] == "2"
    # First cumulative half-mass is at zero deviation (two observations).
    assert parameters(actual)["columns"][0]["mad"] == "0"
    assert apply(actual, ((100,), (None,))) == ((F(0),), (None,))
    ordinary = numeric("robust_scale", (1, 2, 3, 4))
    assert parameters(ordinary)["columns"][0]["median"] == "2"
    assert parameters(ordinary)["columns"][0]["mad"] == "1"
    assert apply(ordinary, ((4,),)) == ((F(2),),)
    calibrated = numeric("robust_scale", options='{"mad_scale":"3/2"}')
    assert apply(calibrated, ((4,),)) == ((F(4, 3),),)


def test_minmax_is_not_implicit_clipping():
    actual = numeric("minmax")
    assert apply(actual, ((0,), (4,), (7,), (None,))) == (
        (F(-1, 3),),
        (F(1),),
        (F(2),),
        (None,),
    )


def test_winsorize_weighted_quantile_endpoints():
    actual = numeric(
        "winsorize",
        (0, 2, 8, 99),
        (F(1), F(2), F(1), F(0)),
        '{"lower":"1/4","upper":"3/4"}',
    )
    assert parameters(actual)["columns"][0]["lower"] == "0"
    assert parameters(actual)["columns"][0]["upper"] == "2"
    assert apply(actual, ((-99,), (1,), (99,))) == ((F(0),), (F(1),), (F(2),))


def test_quantile_rank_ties_zero_mass_and_extrapolation():
    actual = numeric(
        "quantile_rank", (0, 2, 2, 8, 99), (F(1), F(1), F(1), F(1), F(0))
    )
    assert parameters(actual)["columns"][0]["cdf"] == [
        ["0", "1"],
        ["2", "2"],
        ["8", "1"],
    ]
    assert apply(actual, ((-1,), (0,), (1,), (2,), (8,), (999,))) == (
        (F(0),),
        (F(1, 4),),
        (F(1, 4),),
        (F(3, 4),),
        (F(1),),
        (F(1),),
    )


def test_categorical_ontology_frozen_vocabulary_unknown_missing():
    actual = fit(
        "categorical",
        ("reason",),
        (("b",), ("a",), ("future",), (None,)),
        (F(1), F(1), F(0), F(1)),
        '{"ontology_id":"reason.v1"}',
    )
    assert actual.output_columns == (
        "0:category:0",
        "0:category:1",
        "0:category:2",
        "0:category:3",
    )
    assert parameters(actual)["columns"] == [{"categories": ["a", "b"]}]
    before = actual.to_json()
    assert apply(actual, (("a",), ("future",), (None,))) == (
        (1, 0, 0, 0),
        (0, 0, 1, 0),
        (0, 0, 0, 1),
    )
    assert actual.to_json() == before
    other = fit(
        "categorical",
        ("reason",),
        (("b",), ("a",), ("future",), (None,)),
        (F(1), F(1), F(0), F(1)),
        '{"ontology_id":"reason.v2"}',
    )
    assert other.fit_id != actual.fit_id


def test_all_missing_category_retains_unknown_and_missing():
    actual = fit(
        "categorical",
        ("category",),
        ((None,),),
        (F(1),),
        '{"ontology_id":"v1"}',
    )
    assert apply(actual, (("future",), (None,))) == ((1, 0), (0, 1))


def test_median_imputer_preserves_existing_values_and_unavailable():
    actual = numeric(
        "median_imputer", (1, 5, None, 99), (F(1), F(2), F(3), F(0))
    )
    assert apply(actual, ((None,), (77,))) == ((F(5),), (F(77),))
    support = json.loads(actual.support_json)["columns"][0]
    assert support["supported_mass"] == "3"
    assert support["missing_mass"] == "3"
    assert support["zero_weight_rows"] == 1
    assert apply(numeric("median_imputer", (None,)), ((None,), (8,))) == (
        (None,),
        (F(8),),
    )


@pytest.mark.parametrize(
    "family", ["zscale", "robust_scale", "minmax", "winsorize", "quantile_rank"]
)
def test_all_missing_numeric_is_not_invented_zero(family):
    actual = numeric(family, (None, None))
    assert apply(actual, ((100,), (None,))) == ((None,), (None,))


@pytest.mark.parametrize("family", ["zscale", "robust_scale", "minmax"])
def test_constant_numeric_scale_is_declared_zero(family):
    actual = numeric(family, (7, 7))
    assert parameters(actual)["columns"][0]["variance"] == "0"
    assert apply(actual, ((7,), (100,), (None,))) == ((0,), (0,), (None,))


def test_variance_selector_exact_threshold_and_empty_output():
    actual = fit(
        "variance_selector",
        ("vary", "constant", "missing"),
        ((0, 5, None), (2, 5, None)),
        (F(1), F(1)),
    )
    assert actual.output_columns == ("vary",)
    assert apply(actual, ((9, 99, None),)) == ((F(9),),)
    dropped = numeric("variance_selector", (0, 2), options='{"threshold":"1"}')
    assert dropped.output_columns == ()
    assert apply(dropped, ((3,), (None,))) == ((), ())


def test_correlation_pruning_exact_sign_priority_and_pair_support():
    rows = ((-2, 4, 0), (-1, 2, 1), (1, -2, 1), (2, -4, 0))
    actual = fit(
        "correlation_pruner",
        ("first", "negative_copy", "orthogonal"),
        rows,
        (F(1),) * 4,
    )
    assert actual.output_columns == ("first", "orthogonal")
    pair = parameters(actual)["pair_support"][0]
    assert pair == {
        "left": 0,
        "right": 1,
        "rows": 4,
        "mass": "4",
        "correlation_squared": "1",
    }
    assert apply(actual, ((9, -18, 3),)) == ((F(9), F(3)),)


def test_correlation_unsupported_pair_does_not_invent_dependence():
    actual = fit(
        "correlation_pruner",
        ("a", "b"),
        ((1, None), (2, None), (None, 1), (None, 2)),
        (F(1),) * 4,
    )
    assert actual.output_columns == ("a", "b")
    assert parameters(actual)["pair_support"][0]["correlation_squared"] is None
    assert parameters(actual)["pair_support"][0]["rows"] == 0


@pytest.mark.parametrize("family", PROJECTIONS)
def test_all_linear_families_actual_numerical_fit_and_frozen_apply(family):
    # Exact second moments diag(5/2,1/4), zero means and zero covariance.
    rows = ((-2, 0), (-1, 1), (1, 1), (2, 0))
    if family == "svd":
        # Uncentered y second moment is 1/2; principal x remains exact.
        pass
    actual = fit(family, ("x", "y"), rows, (F(1),) * 4)
    info = parameters(actual)["projection"]
    assert info["complete_rows"] == 4
    assert info["complete_mass"] == "4"
    assert float.fromhex(info["eigenvalues"][0]) == pytest.approx(2.5)
    assert [float.fromhex(v) for v in info["basis"][0]] == [1.0, 0.0]
    expected = 4 / math.sqrt(2.5) if family == "whitening" else 4.0
    assert apply(actual, ((4, 999),))[0][0] == pytest.approx(expected)
    assert apply(actual, ((None, 999),)) == ((None,),)
    assert json.loads(actual.environment_json)["numpy"] != "unused"
    before = actual.to_json()
    apply(actual, ((10000, -10000),))
    assert actual.to_json() == before


def test_weighted_whitening_population_covariance_identity():
    rows = ((-2, 0), (-1, 1), (1, 1), (2, 0))
    actual = fit(
        "whitening", ("x", "y"), rows, (F(1),) * 4, '{"n_components":2}'
    )
    projected = apply(actual, rows)
    for j in range(2):
        assert sum(row[j] for row in projected) / 4 == pytest.approx(
            0, abs=1e-15
        )
        assert sum(row[j] ** 2 for row in projected) / 4 == pytest.approx(1)
    assert sum(row[0] * row[1] for row in projected) == pytest.approx(0)


def test_projection_complete_case_support_excludes_missing_and_zero_mass():
    actual = fit(
        "pca",
        ("x", "y"),
        ((-2, 0), (2, 0), (999, None), (999, 999)),
        (F(1), F(1), F(3), F(0)),
    )
    info = parameters(actual)["projection"]
    assert info["center"] == ["0", "0"]
    assert info["complete_rows"] == 2
    assert info["complete_mass"] == "2"


@pytest.mark.parametrize("family", PROJECTIONS)
def test_projection_tied_or_missing_rank_refuses(family):
    with pytest.raises(ValueError, match="tied"):
        fit(
            family,
            ("x", "y"),
            ((-1, -1), (-1, 1), (1, -1), (1, 1)),
            (F(1),) * 4,
        )
    with pytest.raises(ValueError, match="rank"):
        fit(family, ("x", "y"), ((0, 0), (0, 0)), (F(1),) * 2)
    with pytest.raises(ValueError, match="complete"):
        fit(family, ("x", "y"), ((None, 1), (1, None)), (F(1),) * 2)


@pytest.mark.parametrize(
    "family",
    [
        "zscale",
        "robust_scale",
        "minmax",
        "winsorize",
        "quantile_rank",
        "median_imputer",
        "variance_selector",
        "correlation_pruner",
        "pca",
    ],
)
def test_parameter_permutation_invariance(family):
    rows = ((1,), (2,), (4,))
    weights = (F(1), F(2), F(1))
    baseline = fit(family, ("x",), rows, weights)
    for order in permutations(range(3)):
        actual = fit(
            family,
            ("x",),
            tuple(rows[i] for i in order),
            tuple(weights[i] for i in order),
        )
        assert actual.parameters_json == baseline.parameters_json
        assert actual.support_json == baseline.support_json


@pytest.mark.parametrize("family", FAMILIES)
def test_every_family_strict_roundtrip_and_no_mutation(family):
    if family == "categorical":
        actual = fit(
            family,
            ("x",),
            (("a",), ("b",)),
            (F(1), F(1)),
            '{"ontology_id":"v1"}',
        )
    else:
        actual = numeric(family)
    assert actual.input_columns == actual.columns
    assert ReferenceTransformFitV1.from_dict(actual.to_dict()) == actual
    assert ReferenceTransformFitV1.from_json(actual.to_json()) == actual
    assert actual.fit_id.startswith("reference-transform-fit:sha256:")
    with pytest.raises(FrozenInstanceError):
        actual.family = "other"
    with pytest.raises(ValueError):
        ReferenceTransformFitV1.from_json(actual.to_json() + "\n")


@pytest.mark.parametrize(
    "value",
    [
        True,
        False,
        float("nan"),
        float("inf"),
        float("-inf"),
        "1",
        Decimal("1"),
        object(),
    ],
)
def test_numeric_strict_types_and_nonfinite_refusal(value):
    with pytest.raises(ValueError):
        numeric("zscale", (value,))
    with pytest.raises(ValueError):
        apply(numeric("zscale"), ((value,),))


@pytest.mark.parametrize(
    "weights", [(1,), (True,), (F(-1),), (F(0),), [F(1)], ()]
)
def test_invalid_exact_masses_refuse(weights):
    with pytest.raises(ValueError):
        numeric("zscale", (1,), weights)


@pytest.mark.parametrize(
    "family,options",
    [
        ("zscale", '{"axis":0}'),
        ("zscale", '{"x":1,"x":2}'),
        ("categorical", "{}"),
        ("categorical", '{"ontology_id":true}'),
        ("robust_scale", '{"mad_scale":"0"}'),
        ("robust_scale", '{"mad_scale":"2/2"}'),
        ("winsorize", '{"lower":"1","upper":"0"}'),
        ("variance_selector", '{"threshold":"-1"}'),
        ("correlation_pruner", '{"threshold":"0"}'),
        ("pca", '{"n_components":true}'),
        ("pca", '{"n_components":17}'),
        ("pca", '{"eigen_tolerance":"0"}'),
        ("pca", '{"n_components":1.0}'),
    ],
)
def test_closed_options_refuse(family, options):
    with pytest.raises(ValueError):
        numeric(family, options=options)


def test_poison_subclasses_refuse_without_callbacks():
    class Poison(int):
        def __float__(self):
            pytest.fail("poison conversion invoked")

        def bit_length(self):
            pytest.fail("poison arithmetic invoked")

    with pytest.raises(ValueError):
        numeric("zscale", (Poison(1),))

    class PoisonFit(ReferenceTransformFitV1):
        def to_json(self):
            pytest.fail("overridden serializer invoked")

    with pytest.raises(ValueError):
        apply(object.__new__(PoisonFit), ((1,),))


def test_closed_schema_parameter_and_support_forgeries():
    actual = numeric("zscale")
    raw = actual.to_dict()
    raw["parameters"]["columns"][0]["variance"] = "-1"
    with pytest.raises(ValueError, match="statistics"):
        ReferenceTransformFitV1.from_dict(raw)
    raw = actual.to_dict()
    raw["support"]["columns"][0]["supported_mass"] = "3"
    with pytest.raises(ValueError, match="accounting"):
        ReferenceTransformFitV1.from_dict(raw)
    raw = actual.to_dict()
    raw["extra"] = "ignored?"
    with pytest.raises(ValueError, match="fields"):
        ReferenceTransformFitV1.from_dict(raw)
    # Mathematical shape is not authority: a plausible forged center needs
    # upstream fresh native refitting, which this deliberately pure layer lacks.
    raw = actual.to_dict()
    raw["parameters"]["columns"][0]["mean"] = "3"
    resealed = ReferenceTransformFitV1.from_dict(raw)
    assert resealed.fit_id != actual.fit_id


def test_bounds_refuse_before_arithmetic_or_numpy(monkeypatch):
    def poisoned(*args, **kwargs):
        pytest.fail("expensive fit reached after invalid preflight")

    monkeypatch.setattr(kernel, "_fit_parameters", poisoned)
    for rows, columns in [
        (((1,),) * 513, ("x",)),
        (((1,) * 65,), tuple(f"x{i}" for i in range(65))),
    ]:
        with pytest.raises(ValueError):
            fit("zscale", columns, rows, (F(1),) * len(rows))
    with pytest.raises(ValueError, match="projection work"):
        fit("pca", tuple(f"x{i}" for i in range(17)), ((1,) * 17,), (F(1),))
    with pytest.raises(ValueError):
        numeric("zscale", (F(1 << 8192),))


def test_json_decoder_depth_and_unicode_budget_before_allocation(monkeypatch):
    with pytest.raises(ValueError, match="depth"):
        ReferenceTransformFitV1.from_json(
            '{"x":' + "[" * 21 + "0" + "]" * 21 + "}"
        )
    for value in ("abc", "\x7f", "😀", "\ud800", '"\\\n\x01'):
        encoded = json.dumps(
            {"x": value},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        monkeypatch.setattr(kernel, "MAX_JSON_BYTES", len(encoded))
        assert kernel._wire({"x": value}) == encoded
        monkeypatch.setattr(kernel, "MAX_JSON_BYTES", len(encoded) - 1)
        with pytest.raises(ValueError, match="byte bound"):
            kernel._wire({"x": value})


def test_numeric_scale_underflow_and_output_overflow_are_explicit():
    tiny = F(1, 10**200)
    actual = numeric("zscale", (0, tiny))
    with pytest.raises(ValueError, match="underflows"):
        apply(actual, ((tiny,),))
    actual = numeric("zscale")
    with pytest.raises(ValueError, match="binary64"):
        apply(actual, ((F(10**500),),))


def test_categorical_cardinality_is_bounded_and_no_dynamic_expansion():
    with pytest.raises(ValueError, match="vocabulary"):
        fit(
            "categorical",
            ("x",),
            tuple((str(i),) for i in range(129)),
            (F(1),) * 129,
            '{"ontology_id":"v1"}',
        )
    with pytest.raises(ValueError):
        fit("categorical", ("x",), ((True,),), (F(1),), '{"ontology_id":"v1"}')


def test_fit_unknown_families_columns_and_shape_refuse():
    with pytest.raises(ValueError):
        numeric("target_encoder")
    with pytest.raises(ValueError):
        fit("zscale", ("x", "x"), ((1, 2),), (F(1),))
    with pytest.raises(ValueError):
        fit("zscale", ("x",), ((1, 2),), (F(1),))
    with pytest.raises(ValueError):
        fit("zscale", ("x",), (), ())
    with pytest.raises(ValueError):
        replace(numeric("zscale"), output_columns=("not-x",))
