"""Bounded exact overlap geometry and mass (pure math, not qualification).

The first grid start is exactly ``start_ns``. Full half-open windows only;
there is no epoch snapping, extra right-anchored window, or tail redistribution.
"""

from __future__ import annotations

from dataclasses import dataclass
from fractions import Fraction

from .training_contracts import _clock, _text

MAX_OVERLAP_WINDOWS = 256
MAX_OVERLAP_EVENTS = 4096
MAX_OVERLAP_BRANCHES = 4096
MAX_OVERLAP_VARIANTS = 32


def overlap_intervals(
    start_ns: int, end_ns: int, width_ns: int, stride_ns: int
) -> tuple[tuple[int, int], ...]:
    for value in (start_ns, end_ns, width_ns, stride_ns):
        _clock(value)
    if end_ns <= start_ns or not width_ns or not stride_ns:
        raise ValueError("overlap bounds, width and stride must be positive")
    if stride_ns > width_ns:
        raise ValueError("overlap stride cannot exceed width")
    count = max(0, 1 + (end_ns - start_ns - width_ns) // stride_ns)
    if count > MAX_OVERLAP_WINDOWS:
        raise ValueError("overlap window bound exceeded")
    return tuple(
        (start_ns + i * stride_ns, start_ns + i * stride_ns + width_ns)
        for i in range(count)
    )


def overlap_memberships(
    times: tuple[int, ...], intervals: tuple[tuple[int, int], ...]
) -> tuple[tuple[int, ...], ...]:
    if type(times) is not tuple or len(times) > MAX_OVERLAP_EVENTS:
        raise ValueError("overlap event inventory exceeds bound")
    if type(intervals) is not tuple or len(intervals) > MAX_OVERLAP_WINDOWS:
        raise ValueError("overlap interval inventory exceeds bound")
    for interval in intervals:
        if type(interval) is not tuple or len(interval) != 2:
            raise ValueError("overlap interval requires exact two coordinates")
        lo, hi = interval
        _clock(lo)
        _clock(hi)
        if lo >= hi:
            raise ValueError("overlap interval is empty")
    if intervals != tuple(sorted(set(intervals))):
        raise ValueError("overlap intervals must be ordered and unique")
    result = []
    memberships = 0
    for time in times:
        _clock(time)
        row = tuple(
            i for i, (lo, hi) in enumerate(intervals) if lo <= time < hi
        )
        memberships += len(row)
        if memberships > MAX_OVERLAP_BRANCHES:
            raise ValueError("expanded overlap membership bound exceeded")
        result.append(row)
    return tuple(result)


@dataclass(frozen=True, slots=True)
class OverlapMass:
    """Process-local exact arithmetic; source adapters prove complete counts."""

    window_id: str
    member_id: str
    scenario_id: str
    per_row: Fraction
    row_count: int
    reserved_mass: Fraction


def overlap_mass(
    complete_counts: tuple[tuple[str, str, str, int], ...],
    parent_mass: Fraction = Fraction(1),
    *,
    window_ids: tuple[str, ...],
    variants: tuple[tuple[str, str], ...],
) -> tuple[OverlapMass, ...]:
    """Parent → windows → members → scenarios → all-symbol original rows.

    Every declared window/variant pair is required, including zero-count
    branches. Exclusion/selection happens *after* allocation. This function is
    called independently for each original historical unit by the verifier.
    """
    if (
        type(window_ids) is not tuple
        or len(window_ids) > MAX_OVERLAP_WINDOWS
        or type(variants) is not tuple
        or not 0 < len(variants) <= MAX_OVERLAP_VARIANTS
        or type(complete_counts) is not tuple
        or len(complete_counts) > MAX_OVERLAP_BRANCHES
        or len(window_ids) * len(variants) > MAX_OVERLAP_BRANCHES
    ):
        raise ValueError("complete overlap mass inventory exceeds bound")
    if type(parent_mass) is not Fraction or parent_mass < 0:
        raise ValueError("parent mass requires a nonnegative exact Fraction")
    if (
        max(
            parent_mass.numerator.bit_length(),
            parent_mass.denominator.bit_length(),
        )
        > 256
    ):
        raise ValueError("parent rational exceeds bit bound")
    if len(set(window_ids)) != len(window_ids):
        raise ValueError("duplicate scheduled window")
    for window in window_ids:
        _text(window)
    for pair in variants:
        if type(pair) is not tuple or len(pair) != 2:
            raise ValueError("variant requires member/scenario pair")
        for text in pair:
            _text(text)
    if len(set(variants)) != len(variants):
        raise ValueError("duplicate member/scenario variant")
    counts = {}
    for row in complete_counts:
        if type(row) is not tuple or len(row) != 4:
            raise ValueError("mass row requires exact four fields")
        window, member, scenario, count = row
        if type(count) is not int or not 0 <= count <= MAX_OVERLAP_EVENTS:
            raise ValueError("complete row count exceeds bound")
        key = (window, member, scenario)
        if key in counts:
            raise ValueError("duplicate complete branch")
        counts[key] = count
    expected = {(w, m, s) for w in window_ids for m, s in variants}
    if set(counts) != expected:
        raise ValueError("mass requires exact complete branch inventory")
    members = {m for m, _ in variants}
    scenario_counts = {m: sum(x == m for x, _ in variants) for m in members}
    result = []
    for window, member, scenario in sorted(expected):
        reserved = parent_mass / len(window_ids) / len(members)
        reserved /= scenario_counts[member]
        count = counts[window, member, scenario]
        result.append(
            OverlapMass(
                window,
                member,
                scenario,
                reserved / count if count else Fraction(0),
                count,
                reserved,
            )
        )
    return tuple(result)
