"""Exact bounded evidence-mass arithmetic and dependence diagnostics.

These mathematical functions do not qualify caller-provided source identities.
The replaying lineage/artifact workflow supplies verified members and units.
Fractions preserve unit mass exactly; floating views are diagnostics only.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import Enum
from fractions import Fraction

from .training_contracts import training_json

MAX_WEIGHT_MEMBERS = 16
MAX_WEIGHT_FEATURES = 4096
MAX_WEIGHT_ROWS = 65_536
MAX_RATIONAL_BITS = 65_536
MAX_TOTAL_RATIONAL_BITS = 4_194_304


class TrainingMemberPolicy(str, Enum):
    OBSERVED = "observed-only.v1"
    EQUAL = "equal-unit-mass.v1"
    ONE = "one-member-per-epoch.v1"
    MARGINALIZED = "member-marginalized-loss.v1"
    UNCERTAINTY = "calibrated-capped-radius.v1"
    NAIVE = "naive-concatenation-negative-control.v1"


def _rational(value: float | Fraction) -> Fraction:
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("weight arithmetic requires finite values")
        result = Fraction(value)
    elif type(value) is Fraction:
        result = value
    else:
        raise ValueError(
            "weight arithmetic requires exact float/Fraction types"
        )
    if (
        max(abs(result.numerator).bit_length(), result.denominator.bit_length())
        > MAX_RATIONAL_BITS
    ):
        raise ValueError("weight rational exceeds work bound")
    return result


def _bounded_values(
    values: Sequence[float | Fraction], limit: int
) -> tuple[Fraction, ...]:
    if not 0 < len(values) <= limit:
        raise ValueError("weight collection is empty or exceeds bound")
    result: list[Fraction] = []
    remaining = MAX_TOTAL_RATIONAL_BITS
    denominator = 1
    for value in values:
        rational = _rational(value)
        remaining -= (
            abs(rational.numerator).bit_length()
            + rational.denominator.bit_length()
        )
        if remaining < 0:
            raise ValueError("weight collection exceeds aggregate bit budget")
        # Arbitrary Fraction denominators otherwise turn a bounded list into
        # unbounded intermediate integer growth. Float denominators are powers
        # of two; their common denominator remains small even at extreme scales.
        factor = rational.denominator // math.gcd(
            denominator, rational.denominator
        )
        if denominator.bit_length() + factor.bit_length() > MAX_RATIONAL_BITS:
            raise ValueError("weight common denominator exceeds work bound")
        denominator *= factor
        result.append(rational)
    return tuple(result)


def _finite_view(value: Fraction) -> float:
    try:
        result = float(value)
    except OverflowError as exc:
        raise ValueError("weight diagnostic is not representable") from exc
    if not math.isfinite(result):
        raise ValueError("weight diagnostic is not finite")
    return result


@dataclass(frozen=True, slots=True)
class MemberMass:
    member_id: str
    mass: Fraction

    def __post_init__(self) -> None:
        if (
            type(self.member_id) is not str
            or not self.member_id
            or len(self.member_id) > 1024
        ):
            raise ValueError("invalid member identity")
        mass = _rational(self.mass)
        if mass < 0:
            raise ValueError("member mass is negative")
        object.__setattr__(self, "mass", mass)


@dataclass(frozen=True, slots=True)
class UnitMassAllocation:
    policy: TrainingMemberPolicy
    historical_unit_id: str
    unit_mass: Fraction
    members: tuple[MemberMass, ...]
    negative_control: bool

    def __post_init__(self) -> None:
        if type(self.policy) is not TrainingMemberPolicy:
            raise ValueError("invalid member policy")
        if (
            type(self.historical_unit_id) is not str
            or not self.historical_unit_id
            or len(self.historical_unit_id) > 1024
        ):
            raise ValueError("invalid historical unit identity")
        mass = _rational(self.unit_mass)
        if mass <= 0:
            raise ValueError("unit mass must be positive")
        object.__setattr__(self, "unit_mass", mass)
        if (
            type(self.members) is not tuple
            or not 0 < len(self.members) <= MAX_WEIGHT_MEMBERS
            or any(type(item) is not MemberMass for item in self.members)
        ):
            raise ValueError("invalid immutable member mass collection")
        names = tuple(item.member_id for item in self.members)
        if names != tuple(sorted(set(names))):
            raise ValueError("member identities must be sorted and unique")
        _bounded_values(
            tuple(item.mass for item in self.members), MAX_WEIGHT_MEMBERS
        )
        if type(self.negative_control) is not bool or self.negative_control != (
            self.policy is TrainingMemberPolicy.NAIVE
        ):
            raise ValueError("negative-control state differs from policy")
        if not self.negative_control and self.total_mass > self.unit_mass:
            raise ValueError("allocation exceeds historical evidence mass")

    @property
    def total_mass(self) -> Fraction:
        return sum((item.mass for item in self.members), Fraction())

    @property
    def mass_gate_passes(self) -> bool:
        return not self.negative_control and self.total_mass <= self.unit_mass

    def filtered(self, retained: Sequence[str]) -> UnitMassAllocation:
        """Delete members without redistributing their historical mass."""
        if not 0 < len(retained) <= MAX_WEIGHT_MEMBERS:
            raise ValueError("filtered member selection exceeds bound")
        names = tuple(retained)
        if (
            not names
            or len(names) > MAX_WEIGHT_MEMBERS
            or any(type(name) is not str for name in names)
            or len(names) != len(set(names))
        ):
            raise ValueError("filtered member selection is empty or duplicated")
        if not set(names) <= {item.member_id for item in self.members}:
            raise ValueError("filtered member is outside original allocation")
        return UnitMassAllocation(
            self.policy,
            self.historical_unit_id,
            self.unit_mass,
            tuple(item for item in self.members if item.member_id in names),
            self.negative_control,
        )

    def row_mass(self, member_id: str, complete_row_count: int) -> Fraction:
        """Normalize over the complete member, never the filtered row count."""
        if (
            type(complete_row_count) is not int
            or not 0 < complete_row_count <= MAX_WEIGHT_ROWS
        ):
            raise ValueError("complete row count is invalid")
        for item in self.members:
            if item.member_id == member_id:
                return item.mass / complete_row_count
        raise ValueError("member is outside allocation")


def allocate_unit_mass(
    policy: TrainingMemberPolicy,
    historical_unit_id: str,
    member_ids: Sequence[str],
    *,
    training_epoch: int = 0,
    unit_mass: Fraction = Fraction(1),
    maximum_pip_radii: Sequence[float] | None = None,
) -> UnitMassAllocation:
    """Allocate exact unit mass; supported radii come from verified calibration.

    ``maximum_pip_radii`` align with the caller's member order. Sorting does not
    change the mapping. This function itself makes no calibration claim.
    """
    if type(policy) is not TrainingMemberPolicy:
        raise ValueError("invalid policy")
    if not 0 < len(member_ids) <= MAX_WEIGHT_MEMBERS:
        raise ValueError("member count exceeds bound")
    names = tuple(member_ids)
    if (
        not 0 < len(names) <= MAX_WEIGHT_MEMBERS
        or any(
            type(name) is not str or not name or len(name) > 1024
            for name in names
        )
        or len(set(names)) != len(names)
    ):
        raise ValueError("member set must be nonempty, bounded and unique")
    if type(training_epoch) is not int or not 0 <= training_epoch < 2**63:
        raise ValueError("training epoch must be a nonnegative int64")
    mass = _rational(unit_mass)
    if mass <= 0:
        raise ValueError("unit mass must be positive")
    if policy is not TrainingMemberPolicy.UNCERTAINTY:
        if maximum_pip_radii is not None:
            raise ValueError("this policy cannot consume confidence radii")
        shares = {name: Fraction(1, len(names)) for name in names}
        if policy is TrainingMemberPolicy.OBSERVED:
            if names != ("observed",):
                raise ValueError("observed policy requires observed-only input")
        elif policy is TrainingMemberPolicy.ONE:
            ordered = sorted(names)
            digest = hashlib.sha256(
                training_json(
                    [policy.value, historical_unit_id, training_epoch]
                ).encode("ascii")
            ).hexdigest()
            selected = ordered[int(digest, 16) % len(ordered)]
            shares = {name: Fraction(name == selected) for name in names}
        elif policy is TrainingMemberPolicy.NAIVE:
            shares = {name: Fraction(1) for name in names}
    else:
        if maximum_pip_radii is None or len(maximum_pip_radii) != len(names):
            raise ValueError("calibrated radii must cover every member")
        radii = _bounded_values(maximum_pip_radii, MAX_WEIGHT_MEMBERS)
        if any(value < 0 for value in radii):
            raise ValueError("calibrated radius is negative")
        raw = tuple(1 / (1 + value) for value in radii)
        peak = max(raw)
        capped = tuple(max(value / peak, Fraction(1, 4)) for value in raw)
        total = sum(capped, Fraction())
        shares = dict(zip(names, (value / total for value in capped)))
    return UnitMassAllocation(
        policy,
        historical_unit_id,
        mass,
        tuple(MemberMass(name, mass * shares[name]) for name in sorted(names)),
        policy is TrainingMemberPolicy.NAIVE,
    )


def kish_effective_sample_size(
    weights: Sequence[float | Fraction],
) -> float | None:
    """Weight concentration, not a correlation-adjusted sample count."""
    values = _bounded_values(weights, MAX_WEIGHT_ROWS)
    if any(value < 0 for value in values):
        raise ValueError("Kish weights must be nonnegative")
    total = sum(values, Fraction())
    if total == 0:
        return None
    squares = sum((value * value for value in values), Fraction())
    return _finite_view(total * total / squares)


def member_correlation_dimension(
    features: Sequence[Sequence[float]],
) -> float | None:
    """Participation ratio of member Pearson correlations, without eigenfits.

    Exact rational centered cross-products avoid overflow and PSD tolerances.
    For symmetric correlation C, trace(C@C) is sum_ij C_ij**2. Squared
    correlations need no rounded square roots or fabricated event aggregates.
    A constant member vector makes the diagnostic unavailable.
    """
    if not 0 < len(features) <= MAX_WEIGHT_MEMBERS:
        raise ValueError("member feature count exceeds bound")
    widths = {len(row) for row in features}
    if len(widths) != 1 or not 2 <= next(iter(widths)) <= MAX_WEIGHT_FEATURES:
        raise ValueError("member feature support is empty, ragged or oversized")
    centered: list[tuple[Fraction, ...]] = []
    squares: list[Fraction] = []
    for row in features:
        values = _bounded_values(row, MAX_WEIGHT_FEATURES)
        mean = sum(values, Fraction()) / len(values)
        deviations = tuple(value - mean for value in values)
        squared = sum((value * value for value in deviations), Fraction())
        if squared == 0:
            return None
        centered.append(deviations)
        squares.append(squared)
    denominator = Fraction(len(features))
    for i in range(len(features)):
        for j in range(i):
            cross = sum(
                (a * b for a, b in zip(centered[i], centered[j])), Fraction()
            )
            denominator += 2 * cross * cross / (squares[i] * squares[j])
    return _finite_view(Fraction(len(features) ** 2) / denominator)


def member_marginalized_loss(
    member_ids: Sequence[str], loss: Callable[[str], float]
) -> float:
    """Execute each member loss then average, never average predictions first."""
    if not 0 < len(member_ids) <= MAX_WEIGHT_MEMBERS:
        raise ValueError("marginalized member count exceeds bound")
    names = tuple(member_ids)
    if (
        not 0 < len(names) <= MAX_WEIGHT_MEMBERS
        or any(
            type(name) is not str or not name or len(name) > 1024
            for name in names
        )
        or len(set(names)) != len(names)
    ):
        raise ValueError("marginalized loss requires bounded unique members")
    values = tuple(_rational(loss(name)) for name in names)
    return _finite_view(sum(values, Fraction()) / len(values))
