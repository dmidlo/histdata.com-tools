"""Exact bounded scenario arithmetic, never native-source authority.

Weights are nonnegative rational masses. Quantiles use the left-continuous
inverse weighted CDF (the first value whose cumulative mass reaches p); p=0
selects the smallest positive-mass value. No timestamp, category or path
alignment is inferred by these functions.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from fractions import Fraction

MAX_SCENARIO_MEMBERS = 128
MAX_SCENARIO_COORDINATES = 1024
MAX_SCENARIO_CELLS = 4096
# Includes exact binary64 subnormals and their squared differences. Every
# normalized intermediate is checked; unrestricted caller integers refuse.
MAX_SCENARIO_RATIONAL_BITS = 8192
MAX_SCENARIO_QUANTILES = 16


def checked_fraction(value: Fraction) -> Fraction:
    if (
        type(value) is not Fraction
        or max(value.numerator.bit_length(), value.denominator.bit_length())
        > MAX_SCENARIO_RATIONAL_BITS
    ):
        raise ValueError(
            "scenario arithmetic requires a bounded exact Fraction"
        )
    return value


def _sum(values: tuple[Fraction, ...]) -> Fraction:
    result = Fraction(0)
    for value in values:
        result = checked_fraction(result + value)
    return result


def normalized_weights(weights: tuple[Fraction, ...]) -> tuple[Fraction, ...]:
    """Normalize explicit masses; this does not prove a complete inventory."""
    if (
        type(weights) is not tuple
        or not 0 < len(weights) <= MAX_SCENARIO_MEMBERS
    ):
        raise ValueError(
            "scenario weight inventory must be bounded and nonempty"
        )
    for weight in weights:
        checked_fraction(weight)
        if weight < 0:
            raise ValueError("scenario weights cannot be negative")
    total = _sum(weights)
    if not total:
        raise ValueError("scenario weights have no positive mass")
    return tuple(checked_fraction(weight / total) for weight in weights)


@dataclass(frozen=True, slots=True)
class ScenarioScalarMoments:
    """Process-local arithmetic result, not a retained verification receipt."""

    normalized_weights: tuple[Fraction, ...]
    mean: Fraction
    population_variance: Fraction
    quantile_values: tuple[Fraction, ...]


def scenario_scalar_moments(
    values: tuple[Fraction, ...],
    weights: tuple[Fraction, ...],
    probabilities: tuple[Fraction, ...] = (
        Fraction(1, 4),
        Fraction(1, 2),
        Fraction(3, 4),
    ),
) -> ScenarioScalarMoments:
    """Exact mean, population variance and deterministic weighted quantiles."""
    if (
        type(values) is not tuple
        or not 0 < len(values) <= MAX_SCENARIO_MEMBERS
        or type(weights) is not tuple
        or len(values) != len(weights)
        or type(probabilities) is not tuple
        or len(probabilities) > MAX_SCENARIO_QUANTILES
    ):
        raise ValueError("invalid bounded scenario scalar inventory")
    for value in values:
        checked_fraction(value)
    for probability in probabilities:
        checked_fraction(probability)
        if not 0 <= probability <= 1:
            raise ValueError("scenario quantile probability is outside [0,1]")
    if probabilities != tuple(sorted(set(probabilities))):
        raise ValueError(
            "scenario quantile probabilities must be ordered unique"
        )
    masses = normalized_weights(weights)
    mean = _sum(
        tuple(
            checked_fraction(value * mass)
            for value, mass in zip(values, masses)
        )
    )
    terms = []
    for value, mass in zip(values, masses):
        delta = checked_fraction(value - mean)
        square = checked_fraction(delta * delta)
        terms.append(checked_fraction(mass * square))
    variance = _sum(tuple(terms))
    ordered = sorted(
        (value, mass) for value, mass in zip(values, masses) if mass > 0
    )
    quantiles = []
    for probability in probabilities:
        cumulative = Fraction(0)
        for value, mass in ordered:
            cumulative = checked_fraction(cumulative + mass)
            if cumulative >= probability:
                quantiles.append(value)
                break
    return ScenarioScalarMoments(masses, mean, variance, tuple(quantiles))


def sample_scenario_member(
    member_keys: tuple[str, ...],
    *,
    evidence_unit_id: str,
    epoch: int,
    seed: str,
    group_key: str = "all",
) -> str:
    """Domain-separated equal-priority selection over the complete roster.

    Callers retain a selected refusal instead of resampling another member.
    Other evidence units and input order cannot alter a unit's selection.
    This is deterministic semantic sampling, not a calibrated probability law.
    """
    if (
        type(member_keys) is not tuple
        or not 0 < len(member_keys) <= MAX_SCENARIO_MEMBERS
        or type(epoch) is not int
        or not 0 <= epoch <= 2**63 - 1
    ):
        raise ValueError("invalid scenario sampling inventory or epoch")
    for value in (*member_keys, evidence_unit_id, seed, group_key):
        if (
            type(value) is not str
            or not value
            or len(value) > 1024
            or value != value.strip()
            or any(ord(char) < 32 for char in value)
        ):
            raise ValueError("invalid bounded scenario sampling identity")
    if len(set(member_keys)) != len(member_keys):
        raise ValueError("duplicate scenario member key")

    def priority(member: str) -> tuple[bytes, str]:
        payload = [seed, evidence_unit_id, group_key, epoch, member]
        wire = json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
        digest = hashlib.sha256(
            b"histdatacom.training-scenario-sample.v1\n" + wire.encode("ascii")
        ).digest()
        return digest, member

    return min(member_keys, key=priority)
