"""Synthetic arithmetic/shape canaries; no data or scientific certification."""

import json
import math
from dataclasses import FrozenInstanceError
from decimal import ROUND_UP, Inexact, localcontext
from fractions import Fraction
from itertools import product

import pytest

from histdatacom.data_quality import training_wide_diagnostics as module
from histdatacom.data_quality.training_wide_diagnostics import (
    WIDE_DIAGNOSTIC_NONCLAIMS,
)
from histdatacom.data_quality.training_wide_diagnostics import (
    diagnose_wide_feature_values as diagnose,
)


def test_affine_duplicate_columns_have_one_effective_dimension():
    result = diagnose(
        ("market.a", "market.b", "market.c"),
        tuple((i, 2 * i + 11, -i) for i in range(5)),
    )
    assert result.participation_ratio_rank == 1.0
    assert result.rank_status == "available"
    assert result.complete_case_count == 5
    assert [p.correlation for p in result.pairwise_correlations] == [1, -1, -1]
    assert all(p.support_count == 5 for p in result.pairwise_correlations)
    assert all(c.status == "numeric" for c in result.column_summaries)


def test_orthogonal_columns_and_known_nontrivial_rank():
    rows = ((1, 1), (1, -1), (-1, 1), (-1, -1))
    result = diagnose(("x", "y"), rows)
    assert result.participation_ratio_rank == 2.0
    assert result.pairwise_correlations[0].correlation == 0
    # Third column duplicates x: C has eigenvalues 2, 1, 0, ratio 9/5.
    result = diagnose(("x", "y", "z"), tuple((x, y, x) for x, y in rows))
    assert result.participation_ratio_rank == float(Fraction(9, 5))


def test_pairwise_missing_support_is_not_a_rank_matrix():
    result = diagnose(
        ("x", "y", "z"),
        (
            (0, 0, None),
            (1, 1, None),
            (0, None, 0),
            (1, None, 1),
            (None, 0, 1),
            (None, 1, 0),
        ),
    )
    # This pairwise-deletion correlation matrix would not be PSD.
    assert [p.correlation for p in result.pairwise_correlations] == [1, 1, -1]
    assert all(p.support_count == 2 for p in result.pairwise_correlations)
    assert result.complete_case_count == 0
    assert result.rank_status == "insufficient_common_support"
    assert result.participation_ratio_rank is None


def test_rank_recomputes_correlations_on_common_support_only():
    result = diagnose(
        ("x", "y", "z"),
        ((0, 0, 0), (1, 1, 1), (2, -100, None)),
    )
    assert result.pairwise_correlations[0].correlation < 0
    assert result.pairwise_correlations[0].support_count == 3
    assert result.complete_case_count == 2
    assert result.participation_ratio_rank == 1


def test_constant_common_support_is_not_silently_dropped():
    result = diagnose(("x", "y"), ((1, 0), (1, 1), (2, None)))
    assert result.column_summaries[0].status == "numeric"
    assert result.pairwise_correlations[0].status == "constant"
    assert result.rank_status == "constant_on_common_support"
    assert result.participation_ratio_rank is None
    result = diagnose(("x", "y"), ((1, 0), (1, 1)))
    assert result.column_summaries[0].status == "constant"
    assert result.rank_columns == ("x", "y")
    assert result.participation_ratio_rank is None


def test_bool_categorical_and_mixed_columns_are_not_coerced_or_cherry_picked():
    result = diagnose(
        ("n", "bool", "category", "mixed", "null"),
        (
            (0, False, "a", 0, None),
            (1, True, "b", 1, None),
            (2, True, "a", "bad", None),
        ),
    )
    assert result.rank_columns == ("n",)
    assert result.participation_ratio_rank == 1
    assert [c.status for c in result.column_summaries] == [
        "numeric",
        "unsupported_type",
        "unsupported_type",
        "unsupported_type",
        "insufficient_support",
    ]
    mixed = next(p for p in result.pairwise_correlations if p.right == "mixed")
    assert mixed.support_count == 2
    assert mixed.status == "unsupported_type"
    assert mixed.correlation is mixed.squared_correlation is None


@pytest.mark.parametrize("rows", [(), ((None,),), ((None,), (None,))])
def test_absent_numeric_evidence_is_unavailable_not_zero(rows):
    result = diagnose(("x",), rows)
    assert result.rank_status == "no_numeric_columns"
    assert result.participation_ratio_rank is None
    assert result.complete_case_count == 0
    assert result.column_summaries[0].missing_count == len(rows)


def test_single_numeric_observation_has_insufficient_common_support():
    result = diagnose(("x",), ((1,), (None,)))
    assert result.rank_columns == ("x",)
    assert result.complete_case_count == 1
    assert result.rank_status == "insufficient_common_support"


@pytest.mark.parametrize("scale", [1e308, 1e-308, math.ulp(0.0)])
def test_exact_arithmetic_avoids_overflow_underflow_and_ambient_decimal(scale):
    rows = ((-scale, scale), (0.0, 0.0), (scale, -scale))
    baseline = diagnose(("x", "y"), rows)
    with localcontext() as context:
        context.prec = 2
        context.rounding = ROUND_UP
        context.Emax = 2
        context.Emin = -2
        context.traps[Inexact] = True
        assert diagnose(("x", "y"), rows) == baseline
    assert baseline.participation_ratio_rank == 1
    assert baseline.pairwise_correlations[0].correlation == -1


def test_exact_big_integers_do_not_collapse_into_constant_floats():
    base = 2**1000
    result = diagnose(("x", "y"), tuple((base + i, i) for i in range(3)))
    assert result.column_summaries[0].status == "numeric"
    assert result.participation_ratio_rank == 1
    assert result.pairwise_correlations[0].correlation == 1


@pytest.mark.parametrize("tiny", [1e-300, -1e-300])
def test_tiny_correlation_is_not_lost_when_its_square_underflows(tiny):
    result = diagnose(("x", "y"), ((1, tiny), (0, 1), (-1, 0)))
    pair = result.pairwise_correlations[0]
    assert pair.correlation == pytest.approx(
        math.sqrt(3) * tiny / 2, rel=1e-12, abs=0
    )
    assert pair.correlation != 0
    assert pair.squared_correlation == 0  # Explicitly rounded float view.


def test_maximum_column_and_cell_budget_is_admitted_without_truncation():
    columns = tuple(f"x{i}" for i in range(32))
    result = diagnose(columns, tuple((i,) * 32 for i in range(128)))
    assert len(result.column_summaries) == 32
    assert len(result.pairwise_correlations) == 32 * 31 // 2
    assert result.complete_case_count == result.row_count == 128
    assert result.rank_columns == columns
    assert result.participation_ratio_rank == 1


def test_nonperfect_correlation_is_independent_of_decimal_context():
    rows = ((0, 0), (1, 1), (2, 1), (3, 4))
    result = diagnose(("x", "y"), rows)
    with localcontext() as context:
        context.prec = 2
        context.rounding = ROUND_UP
        context.traps[Inexact] = True
        assert diagnose(("x", "y"), rows) == result
    # Hand-centered covariance numerator 6, squared norms 5 and 9.
    pair = result.pairwise_correlations[0]
    assert pair.squared_correlation == 0.8
    assert pair.correlation == pytest.approx(math.sqrt(0.8))
    assert result.participation_ratio_rank == float(Fraction(10, 9))


@pytest.mark.parametrize(
    "columns,rows",
    [
        ([], ()),
        (("x",), []),
        ((), ()),
        (("x", "x"), ()),
        (("x",), ([1],)),
        (("x",), ((1, 2),)),
        (("",), ()),
        (("x" * 257,), ()),
        (("not a name",), ()),
        ((1,), ()),
        (("x",), ((float("inf"),),)),
        (("x",), ((float("nan"),),)),
        (("x",), ((2**1024,),)),
        (("x",), ((Fraction(1, 2),),)),
        (("x",), (([],),)),
        (("x",), (("a" * 4097,),)),
    ],
)
def test_noncanonical_or_oversized_scalars_and_shapes_refuse(columns, rows):
    with pytest.raises(ValueError):
        diagnose(columns, rows)


def test_exact_type_boundaries_refuse_subclasses():
    class Integer(int):
        pass

    class Text(str):
        pass

    class Tuple(tuple):
        pass

    for columns, rows in [
        (Tuple(("x",)), ()),
        (("x",), Tuple(())),
        (("x",), (Tuple((1,)),)),
        ((Text("x"),), ()),
        (("x",), ((Integer(1),),)),
        (("x",), ((Text("a"),),)),
    ]:
        with pytest.raises(ValueError):
            diagnose(columns, rows)


def test_cardinality_budgets_precede_scalar_reads_and_arithmetic(monkeypatch):
    def forbidden(*args):
        raise AssertionError("oversized requests must not perform arithmetic")

    monkeypatch.setattr(module, "_center", forbidden)
    for columns, rows in [
        (tuple(f"x{i}" for i in range(33)), ()),
        (("x",), ((object(),),) * 257),
        (tuple(f"x{i}" for i in range(32)), ((object(),) * 32,) * 129),
    ]:
        with pytest.raises(ValueError, match="column/row/cell budget"):
            diagnose(columns, rows)


def test_aggregate_text_and_rational_budgets_precede_covariance(monkeypatch):
    def forbidden(*args):
        raise AssertionError("aggregate budgets must precede covariance")

    monkeypatch.setattr(module, "_center", forbidden)
    columns = tuple(f"x{i}" for i in range(16))
    with pytest.raises(ValueError, match="aggregate text"):
        diagnose(columns, (("a" * 4096,) * 16,) * 17)
    with pytest.raises(ValueError, match="aggregate rational bit"):
        diagnose(columns, ((math.ulp(0.0),) * 16,) * 256)


def test_report_is_immutable_detached_and_canonical_json_ready():
    result = diagnose(("x", "y"), ((1, 2), (2, 4)))
    with pytest.raises(FrozenInstanceError):
        result.row_count = 999
    first = result.to_dict()
    json.dumps(first, allow_nan=False)
    first["column_summaries"][0]["status"] = "invented"
    first["rank_columns"].append("invented")
    assert result.to_dict() != first
    assert result.nonclaims == WIDE_DIAGNOSTIC_NONCLAIMS
    assert len(result.pairwise_correlations) == 1


def test_row_permutation_and_column_reordering_preserve_mathematical_results():
    rows = ((0, 4), (2, 1), (1, 7))
    result = diagnose(("x", "y"), rows)
    assert diagnose(("x", "y"), tuple(reversed(rows))) == result
    reordered = diagnose(("y", "x"), tuple((y, x) for x, y in rows))
    assert reordered.rank_columns == ("y", "x")
    assert reordered.participation_ratio_rank == result.participation_ratio_rank
    assert reordered.pairwise_correlations[0].correlation == (
        result.pairwise_correlations[0].correlation
    )


def test_exhaustive_nullable_matrices_match_an_independent_integer_oracle():
    # 4**6 small matrices, using n*sum(xy)-sum(x)*sum(y), not the
    # implementation's rational centered-vector operations.
    for flat in product((-1, 0, 1, None), repeat=6):
        rows = tuple(zip(flat[:3], flat[3:]))
        result = diagnose(("x", "y"), rows)
        pair = result.pairwise_correlations[0]
        complete = tuple(
            (x, y) for x, y in rows if x is not None and y is not None
        )
        count = len(complete)
        assert pair.support_count == count
        if count < 2:
            assert pair.status == "insufficient_support"
            assert pair.correlation is None
            continue
        xs = tuple(x for x, _ in complete)
        ys = tuple(y for _, y in complete)
        xx = count * sum(x * x for x in xs) - sum(xs) ** 2
        yy = count * sum(y * y for y in ys) - sum(ys) ** 2
        xy = count * sum(x * y for x, y in complete) - sum(xs) * sum(ys)
        if not xx or not yy:
            assert pair.status == "constant"
            assert pair.correlation is None
            assert result.participation_ratio_rank is None
        else:
            squared = Fraction(xy * xy, xx * yy)
            assert pair.status == "available"
            assert pair.squared_correlation == float(squared)
            assert pair.correlation == pytest.approx(
                math.copysign(math.sqrt(float(squared)), xy)
            )
            assert result.participation_ratio_rank == float(2 / (1 + squared))
