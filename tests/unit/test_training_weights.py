"""Independent exact arithmetic checks, not empirical qualification."""

from fractions import Fraction

import pytest

from histdatacom.data_quality.training_weights import (
    MemberMass,
    TrainingMemberPolicy as Policy,
    UnitMassAllocation,
    allocate_unit_mass,
    kish_effective_sample_size,
    member_correlation_dimension,
    member_marginalized_loss,
)


def test_direct_mass_constructors_store_exact_immutable_rationals():
    member = MemberMass("a", 0.1)
    assert type(member.mass) is Fraction
    assert member.mass == Fraction(0.1)
    allocation = UnitMassAllocation(Policy.EQUAL, "unit", 1.0, (member,), False)
    assert type(allocation.unit_mass) is Fraction
    assert type(allocation.total_mass) is Fraction
    with pytest.raises(ValueError, match="common denominator"):
        UnitMassAllocation(
            Policy.EQUAL,
            "unit",
            Fraction(1),
            (
                MemberMass("a", Fraction(1, 2**40000)),
                MemberMass("b", Fraction(1, 3**20000)),
            ),
            False,
        )


def test_collection_cardinality_is_checked_before_iteration():
    class Oversized:
        def __len__(self):
            return 100

        def __iter__(self):
            raise AssertionError("must not copy oversized input")

    with pytest.raises(ValueError):
        allocate_unit_mass(Policy.EQUAL, "unit", Oversized())
    with pytest.raises(ValueError):
        member_marginalized_loss(Oversized(), lambda _: 1.0)
    allocation = allocate_unit_mass(Policy.EQUAL, "unit", ("a",))
    with pytest.raises(ValueError):
        allocation.filtered(Oversized())


def test_equal_default_mass_survives_member_duplication_and_filtering():
    four = allocate_unit_mass(Policy.EQUAL, "unit", ("a", "b", "c", "d"))
    eight = allocate_unit_mass(Policy.EQUAL, "unit", tuple("abcdefgh"))
    assert four.total_mass == eight.total_mass == Fraction(1)
    assert four.mass_gate_passes and eight.mass_gate_passes
    assert eight.filtered(("a", "d")).total_mass == Fraction(1, 4)
    assert eight.row_mass("a", 3) * 3 == Fraction(1, 8)
    assert eight.row_mass("a", 3) * 2 < Fraction(1, 8)
    # Kish measures weight concentration; it intentionally can grow while the
    # exact same total historical evidence mass remains conserved.
    assert kish_effective_sample_size([m.mass for m in four.members]) == 4
    assert kish_effective_sample_size([m.mass for m in eight.members]) == 8


def test_naive_concatenation_is_never_a_successful_default():
    naive = allocate_unit_mass(Policy.NAIVE, "unit", ("a", "b", "c", "d"))
    assert naive.total_mass == 4
    assert naive.negative_control
    assert not naive.mass_gate_passes
    assert not naive.filtered(("a",)).mass_gate_passes


def test_deterministic_epoch_selection_does_not_depend_on_order():
    for epoch in range(4):
        first = allocate_unit_mass(
            Policy.ONE, "unit", ("c", "b", "a"), training_epoch=epoch
        )
        second = allocate_unit_mass(
            Policy.ONE, "unit", ("a", "b", "c"), training_epoch=epoch
        )
        assert first == second
        assert first.total_mass == 1
        assert sorted(m.mass for m in first.members) == [0, 0, 1]


def test_confidence_radius_formula_cap_and_row_normalization_are_exact():
    result = allocate_unit_mass(
        Policy.UNCERTAINTY,
        "unit",
        ("a", "b", "c", "d"),
        maximum_pip_radii=(0.0, 1.0, 3.0, 1000.0),
    )
    assert [m.mass for m in result.members] == [
        Fraction(1, 2),
        Fraction(1, 4),
        Fraction(1, 8),
        Fraction(1, 8),
    ]
    assert result.total_mass == 1
    assert (
        max(m.mass for m in result.members)
        / min(m.mass for m in result.members)
        == 4
    )
    assert result.row_mass("c", 100) * 100 == Fraction(1, 8)


def test_kish_reference_extreme_values_zero_and_invalid_support():
    assert kish_effective_sample_size([1.0] * 4) == 4
    assert kish_effective_sample_size([1e308] * 4) == 4
    assert kish_effective_sample_size([1e-308] * 4) == 4
    assert kish_effective_sample_size([0.0] * 4) is None
    assert kish_effective_sample_size([1.0, 2.0]) == pytest.approx(9 / 5)
    for bad in ([-1.0], [float("inf")], [True], []):
        with pytest.raises(ValueError):
            kish_effective_sample_size(bad)


def test_correlation_independent_duplicate_near_duplicate_and_constant():
    independent = [
        [1.0, -1.0, 1.0, -1.0, 1.0, -1.0, 1.0, -1.0],
        [1.0, 1.0, -1.0, -1.0, 1.0, 1.0, -1.0, -1.0],
        [1.0, 1.0, 1.0, 1.0, -1.0, -1.0, -1.0, -1.0],
        [1.0, -1.0, -1.0, 1.0, 1.0, -1.0, -1.0, 1.0],
    ]
    assert member_correlation_dimension(independent) == 4
    assert member_correlation_dimension([independent[0]] * 8) == 1
    near = [
        independent[0],
        [x + i * 0.001 for i, x in enumerate(independent[0])],
    ]
    assert 1 < member_correlation_dimension(near) < 1.0001
    assert member_correlation_dimension([[1.0] * 8, independent[0]]) is None
    assert member_correlation_dimension([[1e308, -1e308], [1e308, -1e308]]) == 1


def test_correlation_equicorrelation_half_has_independent_reference():
    # Three orthogonal centered rows; adding the common row to each private
    # row creates two members with rho=1/2. PR=4/(2+2*(1/2)^2)=8/5.
    common = [1.0, 1.0, -1.0, -1.0]
    first = [1.0, -1.0, 1.0, -1.0]
    second = [1.0, -1.0, -1.0, 1.0]
    vectors = [
        [a + b for a, b in zip(common, private)] for private in (first, second)
    ]
    assert member_correlation_dimension(vectors) == float(Fraction(8, 5))


def test_member_marginalized_loss_really_invokes_each_member():
    calls = []

    def loss(member):
        calls.append(member)
        prediction = {"a": -1.0, "b": 1.0}[member]
        return prediction**2

    assert member_marginalized_loss(("a", "b"), loss) == 1.0
    assert calls == ["a", "b"]
    assert member_marginalized_loss(("a", "b"), lambda _: 1e308) == 1e308


def test_rational_collection_work_is_bounded_before_summing():
    with pytest.raises(ValueError, match="aggregate bit budget"):
        kish_effective_sample_size([Fraction(2**65530)] * 65)
    with pytest.raises(ValueError, match="common denominator"):
        kish_effective_sample_size(
            [Fraction(1, 2**40000), Fraction(1, 3**20000)]
        )
    called = []
    with pytest.raises(ValueError):
        member_marginalized_loss((True,), lambda m: called.append(m))
    assert called == []


@pytest.mark.parametrize("policy", list(Policy))
def test_all_six_policies_are_explicit_and_bounded(policy):
    kwargs = {}
    members = ("a", "b")
    if policy is Policy.OBSERVED:
        members = ("observed",)
    if policy is Policy.UNCERTAINTY:
        kwargs["maximum_pip_radii"] = (1.0, 2.0)
    allocation = allocate_unit_mass(policy, "unit", members, **kwargs)
    assert allocation.total_mass == (2 if policy is Policy.NAIVE else 1)
    for count in (0, True, 65537):
        with pytest.raises(ValueError):
            allocation.row_mass(members[0], count)
