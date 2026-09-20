"""Historical-unit paired inference for the frozen weighting comparison.

These bounded calculations do not create independent observations or certify
the source of caller-provided losses. The source-bound campaign reader replays
the corresponding allocations, paths, truth and unit ownership separately.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from fractions import Fraction
from functools import lru_cache
from typing import ClassVar, Sequence

from .training_contracts import TrainingContract
from .training_weight_calibration import _label
from .training_weight_contracts import read_training_weight_preregistration
from .training_weights import (
    TrainingMemberPolicy,
    UnitMassAllocation,
    _bounded_values,
    _finite_view,
    _rational,
    member_marginalized_loss,
)

BOOTSTRAP_REPLICATES = 10_000
BOOTSTRAP_SEED = 60795
MIN_APPLICATION_UNITS = 20
MAX_APPLICATION_UNITS = 20
MAX_BOOTSTRAP_WORK_BYTES = 8 * 1024 * 1024


def _report_view(value: Fraction) -> float:
    result = _finite_view(value)
    if value != 0 and result == 0.0:
        raise ValueError("nonzero loss report underflows floating precision")
    return result


def weighted_member_loss_math(
    allocation: UnitMassAllocation, member_losses: dict[str, float]
) -> float:
    """Execute the actual marginalization seam; filtering never rescales mass.

    The negative control deliberately contributes M*W mass, so its returned
    weighted contribution is not advertised as a normalized per-unit risk.
    It is excluded from the fixed inferential family.
    """
    if type(allocation) is not UnitMassAllocation:
        raise ValueError("loss evaluation requires a typed mass allocation")
    names = tuple(m.member_id for m in allocation.members)
    if (
        type(member_losses) is not dict
        or len(member_losses) != len(names)
        or set(member_losses) != set(names)
    ):
        raise ValueError("loss support must match the complete allocation")
    values = dict(
        zip(
            names,
            _bounded_values(tuple(member_losses[name] for name in names), 16),
        )
    )
    if any(value < 0 for value in values.values()):
        raise ValueError("point losses must be nonnegative")
    if allocation.policy is TrainingMemberPolicy.MARGINALIZED:
        # Preserve exact recorded mass even when the caller intentionally
        # filters members. Calling the public callback API is observable and
        # prevents substituting loss(mean_prediction) for mean(member_loss).
        if len({member.mass for member in allocation.members}) != 1:
            raise ValueError("marginalization requires equal retained masses")
        mean = member_marginalized_loss(names, member_losses.__getitem__)
        exact_mean = sum(values.values(), Fraction()) / len(names)
        if mean != _finite_view(exact_mean):
            raise ValueError("marginalized callback differs from exact loss")
        # Do not round the mean before scaling: a subnormal mean may round to
        # zero although its weighted contribution is representable. Filtering
        # retains original mass, rather than redistributing a full W_h.
        return _report_view(exact_mean * allocation.total_mass)
    return _report_view(
        sum(
            (
                member.mass * values[member.member_id]
                for member in allocation.members
            ),
            Fraction(),
        )
    )


def _difference_values(
    differences: Sequence[float | Fraction],
) -> tuple[Fraction, ...]:
    count = len(differences)
    if not 0 < count <= MAX_APPLICATION_UNITS:
        raise ValueError("paired comparison requires bounded day-unit support")
    # Sequence indexing bounds traversal even for a custom iterator whose
    # behavior disagrees with its advertised length.
    return _bounded_values(
        tuple(differences[i] for i in range(count)), MAX_APPLICATION_UNITS
    )


def exact_paired_sign_pvalue(
    differences: Sequence[float | Fraction],
) -> tuple[float | None, int]:
    values = _difference_values(differences)
    n = sum(value != 0 for value in values)
    if not n:
        return None, 0
    positive = sum(value > 0 for value in values)
    tail = min(positive, n - positive)
    numerator = 2 * sum(math.comb(n, k) for k in range(tail + 1))
    return float(min(Fraction(1), Fraction(numerator, 2**n))), n


def _linear_quantile(
    sorted_sums: Sequence[int],
    denominator: int,
    probability: Fraction,
    *,
    upper: bool,
) -> float:
    position = (len(sorted_sums) - 1) * probability
    low = position.numerator // position.denominator
    fraction = position - low
    value = Fraction(sorted_sums[low], denominator)
    if fraction:
        value += fraction * (
            Fraction(sorted_sums[low + 1], denominator) - value
        )
    result = _finite_view(value)
    if (upper and Fraction(result) < value) or (
        not upper and Fraction(result) > value
    ):
        result = math.nextafter(result, math.inf if upper else -math.inf)
    if not math.isfinite(result):
        raise ValueError("bootstrap bound is not representable")
    return result


@lru_cache(maxsize=16)
def _bootstrap_interval(
    integers: tuple[int, ...], denominator: int, replicates: int, seed: int
) -> tuple[float, float]:
    """Content-only bounded cache; no artifact identity or pass flag is a key.

    Exact common-denominator integer sums avoid constructing 200,000 Fraction
    objects. Only two final scalar bounds are cached; all retained losses and
    reported summaries are still independently validated on every restoration.
    """
    rng = random.Random(seed)
    samples = sorted(
        sum(integers[rng.randrange(len(integers))] for _ in integers)
        for _ in range(replicates)
    )
    return (
        _linear_quantile(
            samples, denominator * len(integers), Fraction(1, 40), upper=False
        ),
        _linear_quantile(
            samples, denominator * len(integers), Fraction(39, 40), upper=True
        ),
    )


def paired_day_bootstrap(
    differences: Sequence[float | Fraction],
) -> tuple[float, float] | None:
    """Fixed-seed percentile interval, resampling entire historical day units."""
    values = _difference_values(differences)
    if len(values) < MIN_APPLICATION_UNITS:
        return None
    denominator = math.lcm(*(value.denominator for value in values))
    integers = tuple(
        value.numerator * (denominator // value.denominator) for value in values
    )
    bits = max(abs(value).bit_length() for value in integers)
    bits += len(values).bit_length()
    if BOOTSTRAP_REPLICATES * (48 + (bits + 7) // 8) > MAX_BOOTSTRAP_WORK_BYTES:
        raise ValueError("bootstrap integer workspace exceeds bound")
    return _bootstrap_interval(
        integers, denominator, BOOTSTRAP_REPLICATES, BOOTSTRAP_SEED
    )


def fixed_family_holm(
    pvalues: dict[str, float | None],
) -> dict[str, float | None]:
    """Retain all seven coordinates; missing p=1 only inside Holm arithmetic."""
    coordinates = read_training_weight_preregistration().holm_coordinates()
    if (
        type(pvalues) is not dict
        or len(pvalues) != len(coordinates)
        or set(pvalues) != set(coordinates)
    ):
        raise ValueError(
            "Holm requires the exact preregistered seven-coordinate family"
        )
    values: dict[str, Fraction] = {}
    for coordinate in coordinates:
        value = pvalues[coordinate]
        rational = Fraction(1) if value is None else _rational(value)
        if not 0 <= rational <= 1:
            raise ValueError("Holm p-value is outside [0,1]")
        values[coordinate] = rational
    previous = Fraction()
    result: dict[str, float | None] = {}
    for rank, coordinate in enumerate(
        sorted(coordinates, key=lambda c: (values[c], c))
    ):
        previous = min(
            Fraction(1),
            max(previous, (len(coordinates) - rank) * values[coordinate]),
        )
        result[coordinate] = (
            None if pvalues[coordinate] is None else _finite_view(previous)
        )
    return {coordinate: result[coordinate] for coordinate in coordinates}


@dataclass(frozen=True, slots=True)
class TrainingPairedUnitLossV1(TrainingContract):
    KIND: ClassVar[str] = "paired-unit-loss"
    historical_unit_id: str
    coordinate: str
    candidate_loss: float
    equal_loss: float

    def _validate(self) -> None:
        _label(self.historical_unit_id)
        if (
            self.coordinate
            not in read_training_weight_preregistration().holm_coordinates()
        ):
            raise ValueError("paired comparison is outside fixed family")
        if _rational(self.candidate_loss) < 0 or _rational(self.equal_loss) < 0:
            raise ValueError("paired point losses must be nonnegative")

    @property
    def difference(self) -> float:
        return _finite_view(
            Fraction(self.candidate_loss) - Fraction(self.equal_loss)
        )


@dataclass(frozen=True, slots=True)
class TrainingPairedInferenceV1(TrainingContract):
    KIND: ClassVar[str] = "paired-inference"
    coordinate: str
    units: tuple[TrainingPairedUnitLossV1, ...]
    status: str
    mean_difference: float | None
    interval: tuple[float, ...]
    sign_pvalue: float | None
    nonzero_unit_count: int

    def _validate(self) -> None:
        if (
            self.coordinate
            not in read_training_weight_preregistration().holm_coordinates()
        ):
            raise ValueError(
                "inference coordinate differs from preregistered family"
            )
        if len(self.units) > MAX_APPLICATION_UNITS or any(
            u.coordinate != self.coordinate for u in self.units
        ):
            raise ValueError("inference unit scope differs from coordinate")
        ids = tuple(u.historical_unit_id for u in self.units)
        if ids != tuple(sorted(set(ids))):
            raise ValueError(
                "inference must contain unique ordered historical units"
            )
        expected = _inference_values(self.units)
        if (
            self.status,
            self.mean_difference,
            self.interval,
            self.sign_pvalue,
            self.nonzero_unit_count,
        ) != expected:
            raise ValueError(
                "inference summaries differ from retained unit losses"
            )


def _inference_values(
    units: tuple[TrainingPairedUnitLossV1, ...],
) -> tuple[str, float | None, tuple[float, ...], float | None, int]:
    if not units:
        return "unavailable_no_common_units", None, (), None, 0
    # Retain exact differences until the final exposed scalar. Averaging the
    # rounded .difference views can erase small net effects after cancellation.
    differences = tuple(
        Fraction(u.candidate_loss) - Fraction(u.equal_loss) for u in units
    )
    mean = _report_view(sum(differences, Fraction()) / len(differences))
    nonzero = sum(value != 0.0 for value in differences)
    if len(units) < MIN_APPLICATION_UNITS:
        return "insufficient_common_historical_units", mean, (), None, nonzero
    interval = paired_day_bootstrap(differences)
    if interval is None:
        raise ValueError("sufficient support unexpectedly lacks interval")
    pvalue, effective_n = exact_paired_sign_pvalue(differences)
    return (
        "conditional_descriptive_cluster_inference",
        mean,
        interval,
        pvalue,
        effective_n,
    )


def compare_historical_unit_losses(
    coordinate: str, units: Sequence[TrainingPairedUnitLossV1]
) -> TrainingPairedInferenceV1:
    """No synthetic row/member count is used to increase inferential support."""
    if len(units) > MAX_APPLICATION_UNITS:
        raise ValueError("paired inference exceeds application-day bound")
    if (
        coordinate
        not in read_training_weight_preregistration().holm_coordinates()
    ):
        raise ValueError("paired comparison is outside fixed family")
    supplied = tuple(units[i] for i in range(len(units)))
    if any(type(unit) is not TrainingPairedUnitLossV1 for unit in supplied):
        raise ValueError("paired comparison requires typed historical units")
    retained = tuple(sorted(supplied, key=lambda u: u.historical_unit_id))
    if len({u.historical_unit_id for u in retained}) != len(retained) or any(
        u.coordinate != coordinate for u in retained
    ):
        raise ValueError("paired comparison has duplicate or mixed units")
    values = _inference_values(retained)
    return TrainingPairedInferenceV1(coordinate, retained, *values)
