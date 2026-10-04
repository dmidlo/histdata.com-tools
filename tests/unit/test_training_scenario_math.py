"""Independent exact arithmetic and semantic-sampling vectors; no authority."""

from decimal import Decimal, localcontext
from fractions import Fraction as F
from itertools import permutations

import pytest

from histdatacom.data_quality.training_scenario_math import (
    MAX_SCENARIO_RATIONAL_BITS,
    normalized_weights,
    sample_scenario_member,
    scenario_scalar_moments,
)


def test_exact_hand_computed_moments_and_quantile_boundaries():
    result = scenario_scalar_moments(
        (F(0), F(2), F(8)),
        (F(1), F(2), F(1)),
        (F(0), F(1, 4), F(1, 2), F(3, 4), F(1)),
    )
    assert result.normalized_weights == (F(1, 4), F(1, 2), F(1, 4))
    assert result.mean == 3
    assert result.population_variance == 9
    assert result.quantile_values == (0, 0, 2, 2, 8)


def test_zero_mass_extremes_do_not_change_quantiles():
    result = scenario_scalar_moments(
        (F(-999), F(2), F(2), F(9), F(999)),
        (F(0), F(1), F(1), F(2), F(0)),
        (F(0), F(1, 2), F(1)),
    )
    assert result.quantile_values == (2, 2, 9)
    assert result.mean == F(11, 2)
    assert result.population_variance == F(49, 4)


def test_permutation_and_mass_scale_invariance():
    values, weights = (F(1, 3), F(7, 5), F(-2)), (F(1), F(2), F(3))
    baseline = scenario_scalar_moments(values, weights)
    for order in permutations(range(3)):
        actual = scenario_scalar_moments(
            tuple(values[i] for i in order),
            tuple(19 * weights[i] for i in order),
        )
        assert (
            actual.mean,
            actual.population_variance,
            actual.quantile_values,
        ) == (
            baseline.mean,
            baseline.population_variance,
            baseline.quantile_values,
        )


def test_singleton_and_exact_binary64_subnormal():
    value = F.from_float(float.fromhex("0x0.0000000000001p-1022"))
    result = scenario_scalar_moments((value,), (F(1),))
    assert result.mean == value
    assert result.population_variance == 0
    assert result.quantile_values == (value, value, value)


def test_binary64_extreme_difference_is_still_exact():
    high = F.from_float(float.fromhex("0x1.fffffffffffffp+1023"))
    low = F.from_float(float.fromhex("0x0.0000000000001p-1022"))
    result = scenario_scalar_moments((low, high), (F(1), F(1)))
    assert result.mean == (low + high) / 2
    assert result.population_variance == (high - low) ** 2 / 4


def test_decimal_context_does_not_change_exact_arithmetic():
    with localcontext() as context:
        context.prec = 1
        context.Emax = 1
        context.Emin = -1
        result = scenario_scalar_moments((F(1, 7), F(2, 7)), (F(1), F(1)))
    assert result.mean == F(3, 14)
    assert result.population_variance == F(1, 196)


@pytest.mark.parametrize(
    "weights", [(), (F(0),), (F(-1),), (1,), (True,), [F(1)]]
)
def test_invalid_weights_refuse(weights):
    with pytest.raises(ValueError):
        normalized_weights(weights)


@pytest.mark.parametrize("value", [1, True, 0.1, float("nan"), Decimal("0.1")])
def test_nonfraction_scalar_refuses(value):
    with pytest.raises(ValueError):
        scenario_scalar_moments((value,), (F(1),))


@pytest.mark.parametrize(
    "probabilities", [(F(-1),), (F(2),), (F(1), F(0)), (F(1), F(1)), (0,)]
)
def test_invalid_quantiles_refuse(probabilities):
    with pytest.raises(ValueError):
        scenario_scalar_moments((F(1),), (F(1),), probabilities)


def test_fraction_bounds_checked_before_and_during_arithmetic():
    with pytest.raises(ValueError, match="bounded"):
        normalized_weights((F(1 << MAX_SCENARIO_RATIONAL_BITS),))
    with pytest.raises(ValueError, match="bounded"):
        scenario_scalar_moments(
            (F(0), F(1 << (MAX_SCENARIO_RATIONAL_BITS - 2))),
            (F(1), F(1)),
        )


def test_inventory_bound_and_length_mismatch():
    with pytest.raises(ValueError):
        scenario_scalar_moments((F(1),) * 129, (F(1),) * 129)
    with pytest.raises(ValueError):
        scenario_scalar_moments((F(1),), (F(1), F(2)))


def test_sampling_is_order_invariant_and_epoch_keyed():
    choices = ("native-a", "native-b", "native-c", "native-refused")
    baseline = tuple(
        sample_scenario_member(
            choices, evidence_unit_id="unit", epoch=e, seed="frozen"
        )
        for e in range(32)
    )
    assert len(set(baseline)) > 1
    assert "native-refused" in baseline  # It is never filtered/retried here.
    for order in permutations(choices):
        assert (
            tuple(
                sample_scenario_member(
                    order, evidence_unit_id="unit", epoch=e, seed="frozen"
                )
                for e in range(32)
            )
            == baseline
        )


def test_frozen_sampling_v1_literal_domain_vector():
    # SHA256(domain + b'["frozen","unit","all",0,"b"]') begins 1fc6e875;
    # a begins 89122ce1 and c begins 588e2442. This is the new v1 priority
    # algorithm, not the older #607 modulo selector.
    assert (
        sample_scenario_member(
            ("a", "b", "c"), evidence_unit_id="unit", epoch=0, seed="frozen"
        )
        == "b"
    )


def test_unrelated_unit_sampling_does_not_mutate_selection():
    keys = ("a", "b")
    before = sample_scenario_member(
        keys, evidence_unit_id="unit", epoch=3, seed="frozen"
    )
    sample_scenario_member(
        ("x",), evidence_unit_id="other", epoch=4, seed="other"
    )
    assert (
        sample_scenario_member(
            keys, evidence_unit_id="unit", epoch=3, seed="frozen"
        )
        == before
    )


@pytest.mark.parametrize(
    "keys,epoch",
    [
        ((), 0),
        (("a", "a"), 0),
        (("a",), -1),
        (("a",), True),
        (("",), 0),
        (("a",) * 129, 0),
    ],
)
def test_sampling_invalid_inventory_refuses(keys, epoch):
    with pytest.raises(ValueError):
        sample_scenario_member(
            keys, evidence_unit_id="unit", epoch=epoch, seed="frozen"
        )
