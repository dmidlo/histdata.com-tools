"""Independent synthetic inference checks, never historical qualification."""

from dataclasses import replace
from fractions import Fraction
import hashlib
import math
import random

import numpy as np
import pytest
from scipy.stats import binomtest
from statsmodels.stats.multitest import multipletests

from histdatacom.data_quality.training_contracts import training_json
from histdatacom.data_quality.training_weight_contracts import (
    read_training_weight_preregistration,
)
from histdatacom.data_quality import training_weight_evaluation as evaluation
from histdatacom.data_quality.training_weight_evaluation import (
    TrainingPairedInferenceV1,
    TrainingPairedUnitLossV1,
    compare_historical_unit_losses,
    exact_paired_sign_pvalue,
    fixed_family_holm,
    paired_day_bootstrap,
    weighted_member_loss_math,
)
from histdatacom.data_quality.training_weights import (
    MemberMass,
    TrainingMemberPolicy as Policy,
    UnitMassAllocation,
    allocate_unit_mass,
)


def coordinates():
    return read_training_weight_preregistration().holm_coordinates()


def units(count=20, *, coordinate=None):
    name = coordinates()[0] if coordinate is None else coordinate
    return tuple(
        TrainingPairedUnitLossV1(
            f"synthetic-unit-{i:02d}", name, float(i + 1), 10.0
        )
        for i in range(count)
    )


def reseal(value):
    body = {key: item for key, item in value.items() if key != "artifact_id"}
    digest = hashlib.sha256(training_json(body).encode()).hexdigest()
    return {**body, "artifact_id": "training-paired-inference:sha256:" + digest}


@pytest.mark.parametrize("policy", list(Policy))
def test_all_six_policies_contribute_their_exact_retained_mass(policy):
    members = ("observed",) if policy is Policy.OBSERVED else ("a", "b")
    kwargs = (
        {"maximum_pip_radii": (0.0, 3.0)}
        if policy is Policy.UNCERTAINTY
        else {}
    )
    allocation = allocate_unit_mass(policy, "unit", members, **kwargs)
    losses = dict(zip(members, (1.0, 9.0)))
    expected = sum(
        (
            member.mass * Fraction(losses[member.member_id])
            for member in allocation.members
        ),
        Fraction(),
    )
    assert weighted_member_loss_math(allocation, losses) == float(expected)
    assert allocation.total_mass == (2 if policy is Policy.NAIVE else 1)
    if policy is Policy.NAIVE:
        assert not allocation.mass_gate_passes
        assert all(policy.value not in name for name in coordinates())


def test_marginalization_really_executes_callback_and_filter_never_renormalizes(
    monkeypatch,
):
    calls = []
    real = evaluation.member_marginalized_loss

    def tracked(names, callback):
        def record(name):
            calls.append(name)
            return callback(name)

        return real(names, record)

    monkeypatch.setattr(evaluation, "member_marginalized_loss", tracked)
    allocation = allocate_unit_mass(Policy.MARGINALIZED, "unit", ("a", "b"))
    # Predictions -1/+1 have mean zero, but their squared losses both equal1.
    assert weighted_member_loss_math(allocation, {"a": 1.0, "b": 1.0}) == 1.0
    assert calls == ["a", "b"]
    calls.clear()
    retained = allocation.filtered(("a",))
    assert weighted_member_loss_math(retained, {"a": 9.0}) == 4.5
    assert calls == ["a"] and retained.total_mass == Fraction(1, 2)
    monkeypatch.setattr(
        evaluation, "member_marginalized_loss", lambda *args: 123.0
    )
    with pytest.raises(ValueError, match="callback"):
        weighted_member_loss_math(allocation, {"a": 1.0, "b": 1.0})


def test_weighted_loss_extremes_and_invalid_support_refuse():
    allocation = allocate_unit_mass(
        Policy.MARGINALIZED, "unit", ("a", "b"), unit_mass=Fraction(2)
    )
    # The unweighted mean rounds to zero, but the final contribution does not.
    assert (
        weighted_member_loss_math(allocation, {"a": 5e-324, "b": 0.0}) == 5e-324
    )
    with pytest.raises(ValueError, match="representable"):
        weighted_member_loss_math(allocation, {"a": 1e308, "b": 1e308})
    with pytest.raises(ValueError, match="underflows"):
        weighted_member_loss_math(
            allocate_unit_mass(Policy.EQUAL, "unit", ("a", "b")),
            {"a": 5e-324, "b": 0.0},
        )
    for losses in (
        {"a": 1.0},
        {"a": 1.0, "b": 2.0, "extra": 3.0},
        {"a": -1.0, "b": 0.0},
        {"a": True, "b": 0.0},
        {"a": float("inf"), "b": 0.0},
    ):
        with pytest.raises(ValueError):
            weighted_member_loss_math(allocation, losses)
    unequal = UnitMassAllocation(
        Policy.MARGINALIZED,
        "unit",
        Fraction(1),
        (MemberMass("a", Fraction(1, 4)), MemberMass("b", Fraction(3, 4))),
        False,
    )
    with pytest.raises(ValueError, match="equal retained masses"):
        weighted_member_loss_math(unequal, {"a": 1.0, "b": 1.0})


@pytest.mark.parametrize("size", [1, 2, 3, 10, 20])
def test_exact_two_sided_sign_test_matches_independent_binomial_oracle(size):
    for positive in range(size + 1):
        values = [1.0] * positive + [-1.0] * (size - positive)
        actual, n = exact_paired_sign_pvalue(values)
        assert n == size
        assert actual == pytest.approx(
            binomtest(positive, size, 0.5, alternative="two-sided").pvalue,
            abs=1e-15,
        )
    if size < 20:
        with_tie, n = exact_paired_sign_pvalue(values + [0.0])
        assert with_tie == actual and n == size
    assert exact_paired_sign_pvalue([0.0] * size) == (None, 0)


def test_bootstrap_matches_numpy_linear_quantiles_with_identical_day_draws():
    values = [float(i - 9) for i in range(20)]
    rng = random.Random(60795)
    indices = np.array(
        [[rng.randrange(20) for _ in range(20)] for _ in range(10_000)]
    )
    expected = np.quantile(
        np.array(values)[indices].mean(axis=1), (0.025, 0.975), method="linear"
    )
    assert paired_day_bootstrap(values) == pytest.approx(expected, abs=1e-14)
    assert paired_day_bootstrap(values[:19]) is None
    assert paired_day_bootstrap([1e308] * 20) == (1e308, 1e308)
    assert paired_day_bootstrap([5e-324] * 20) == (5e-324, 5e-324)


def test_exact_loss_differences_survive_cancellation_before_final_rounding():
    coordinate = coordinates()[0]
    retained = tuple(
        TrainingPairedUnitLossV1(
            f"synthetic-unit-{i:02d}",
            coordinate,
            1e308 if i < 10 else 0.0,
            1.0 if i < 10 else 1e308,
        )
        for i in range(20)
    )
    assert sum(u.difference for u in retained[:1]) == 1e308
    result = compare_historical_unit_losses(coordinate, retained)
    # Averaging rounded +/-1e308 differences would erase this exact net loss.
    assert result.mean_difference == -0.5
    assert result.nonzero_unit_count == 20 and result.sign_pvalue == 1.0
    assert all(math.isfinite(value) for value in result.interval)
    assert TrainingPairedInferenceV1.from_json(result.to_json()) == result


def test_nonzero_mean_underflow_refuses_and_interval_rounding_is_outward():
    coordinate = coordinates()[0]
    with pytest.raises(ValueError, match="underflows"):
        compare_historical_unit_losses(
            coordinate,
            (
                TrainingPairedUnitLossV1("a", coordinate, 5e-324, 0.0),
                TrainingPairedUnitLossV1("b", coordinate, 0.0, 0.0),
            ),
        )
    exact = Fraction(1, 10)
    lower, upper = paired_day_bootstrap([exact] * 20)
    assert Fraction(lower) <= exact <= Fraction(upper)
    assert Fraction(math.nextafter(lower, math.inf)) > exact
    assert Fraction(math.nextafter(upper, -math.inf)) < exact
    tiny = Fraction(1, 2**1075)
    lower, upper = paired_day_bootstrap([tiny] * 20)
    assert lower == 0.0 and upper == 5e-324


def test_minimum_support_empty_partial_and_tied_states_remain_explicit():
    coordinate = coordinates()[0]
    empty = compare_historical_unit_losses(coordinate, ())
    assert empty.status == "unavailable_no_common_units"
    assert empty.mean_difference is empty.sign_pvalue is None
    assert empty.interval == () and empty.nonzero_unit_count == 0
    partial = compare_historical_unit_losses(coordinate, units(19))
    assert partial.status == "insufficient_common_historical_units"
    assert partial.mean_difference is not None
    assert partial.interval == () and partial.sign_pvalue is None
    full = compare_historical_unit_losses(coordinate, units())
    assert full.status == "conditional_descriptive_cluster_inference"
    assert full.mean_difference == 0.5
    assert full.nonzero_unit_count == 19
    assert len(full.interval) == 2 and full.sign_pvalue is not None
    tied = compare_historical_unit_losses(
        coordinate,
        tuple(replace(u, candidate_loss=u.equal_loss) for u in units()),
    )
    assert tied.interval == (0.0, 0.0)
    assert tied.sign_pvalue is None and tied.nonzero_unit_count == 0


def test_duplicate_or_mixed_units_refuse_before_bootstrap(monkeypatch):
    monkeypatch.setattr(
        evaluation,
        "paired_day_bootstrap",
        lambda *args: pytest.fail("must reject scope before bootstrap"),
    )
    retained = units()
    for wrong in (
        retained[:-1] + (retained[0],),
        retained[:-1] + (replace(retained[-1], coordinate=coordinates()[1]),),
        retained + (retained[0],),
        ("not-a-unit",),
    ):
        with pytest.raises(ValueError):
            compare_historical_unit_losses(coordinates()[0], wrong)
    with pytest.raises(ValueError, match="fixed family"):
        compare_historical_unit_losses("unregistered-policy", retained)


@pytest.mark.parametrize("bad", [True, -1.0, float("inf"), float("nan")])
def test_retained_losses_require_finite_nonnegative_exact_scalars(bad):
    with pytest.raises(ValueError):
        replace(units(1)[0], candidate_loss=bad)


def test_frozen_holm_family_matches_statsmodels_and_retains_unavailable():
    names = coordinates()
    assert len(names) == 7
    raw = dict(zip(names, (0.001, 0.02, None, 0.2, 0.01, None, 1.0)))
    actual = fixed_family_holm(raw)
    oracle = multipletests(
        [1.0 if raw[name] is None else raw[name] for name in names],
        method="holm",
    )[1]
    assert tuple(actual) == names
    for name, expected in zip(names, oracle):
        if raw[name] is None:
            assert actual[name] is None
        else:
            assert actual[name] == pytest.approx(expected)
    assert fixed_family_holm(dict.fromkeys(names)) == dict.fromkeys(names)
    assert fixed_family_holm(dict.fromkeys(names, 0.01)) == dict.fromkeys(
        names, 0.07
    )
    for invalid in (
        {key: value for key, value in raw.items() if key != names[0]},
        {**raw, "extra": 0.01},
        {**raw, names[0]: -0.1},
        {**raw, names[0]: 1.1},
        {**raw, names[0]: True},
        {**raw, names[0]: float("nan")},
    ):
        with pytest.raises(ValueError):
            fixed_family_holm(invalid)


@pytest.mark.parametrize(
    "field,wrong",
    [
        ("mean_difference", 999.0),
        ("interval", [0.0, 0.0]),
        ("status", "passed"),
        ("sign_pvalue", 0.0),
        ("nonzero_unit_count", 100),
    ],
)
def test_correctly_resealed_summary_tampering_fails_retained_loss_replay(
    field, wrong
):
    result = compare_historical_unit_losses(coordinates()[0], units())
    value = result.to_dict()
    value[field] = wrong
    forged = reseal(value)
    assert forged["artifact_id"] != result.artifact_id
    assert reseal(forged) == forged
    with pytest.raises(ValueError, match="summaries"):
        TrainingPairedInferenceV1.from_dict(forged)


def test_wire_roundtrip_is_stable_and_unknown_nested_content_refuses():
    result = compare_historical_unit_losses(
        coordinates()[0], tuple(reversed(units()))
    )
    restored = TrainingPairedInferenceV1.from_json(result.to_json())
    assert restored == result and restored.to_json() == result.to_json()
    wire = result.to_dict()
    wire["units"][0]["extra"] = "not ignored"
    with pytest.raises(ValueError, match="unknown"):
        TrainingPairedInferenceV1.from_dict(wire)


def test_bootstrap_cache_is_content_bound_and_does_not_skip_resource_guards(
    monkeypatch,
):
    evaluation._bootstrap_interval.cache_clear()
    real_random = random.Random
    counts = []

    class CountingRandom(real_random):
        def randrange(self, *args, **kwargs):
            counts.append(1)
            return super().randrange(*args, **kwargs)

    monkeypatch.setattr(evaluation.random, "Random", CountingRandom)
    first = compare_historical_unit_losses(coordinates()[0], units())
    assert len(counts) == 200_000
    assert TrainingPairedInferenceV1.from_json(first.to_json()) == first
    assert len(counts) == 200_000
    changed = units()[:-1] + (replace(units()[-1], candidate_loss=99.0),)
    second = compare_historical_unit_losses(coordinates()[0], changed)
    assert first.artifact_id != second.artifact_id
    assert len(counts) == 400_000
    assert evaluation._bootstrap_interval.cache_info().maxsize == 16
    monkeypatch.setattr(evaluation, "MAX_BOOTSTRAP_WORK_BYTES", 1)
    with pytest.raises(ValueError, match="workspace"):
        paired_day_bootstrap(tuple(u.difference for u in units()))


def test_numeric_and_cardinality_bounds_refuse_before_resampling(monkeypatch):
    class Oversized:
        def __len__(self):
            return 21

        def __getitem__(self, index):
            pytest.fail("must not traverse oversized support")

    for operation in (paired_day_bootstrap, exact_paired_sign_pvalue):
        with pytest.raises(ValueError, match="bounded"):
            operation(Oversized())
        for invalid in ([], [True], [float("inf")], [float("nan")]):
            with pytest.raises(ValueError):
                operation(invalid)
    monkeypatch.setattr(
        evaluation.random, "Random", lambda *args: pytest.fail("must preflight")
    )
    with pytest.raises(ValueError, match="workspace"):
        paired_day_bootstrap([Fraction(2**65500)] * 20)
    with pytest.raises(ValueError, match="common denominator"):
        paired_day_bootstrap(
            [Fraction(1, 2**40000), Fraction(1, 3**20000)] + [0.0] * 18
        )
