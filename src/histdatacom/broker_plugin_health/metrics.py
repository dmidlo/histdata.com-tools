"""Declared numerical summaries, with no fitted thresholds or provider claims."""

from __future__ import annotations

import math
from collections.abc import Iterable

from .contracts import BrokerHostHealthDistributionV1, BrokerHostHealthRateV1


def wilson_interval(
    numerator: int, denominator: int
) -> tuple[float, float] | None:
    """Two-sided 95% Wilson; empty support is unavailable, never [0,0]."""
    if (
        type(numerator) is not int
        or type(denominator) is not int
        or not 0 <= numerator <= denominator <= 1_000_000
    ):
        raise ValueError("invalid bounded Bernoulli counts")
    if denominator == 0:
        return None
    z = 1.959963984540054
    p = numerator / denominator
    z2 = z * z
    divisor = 1 + z2 / denominator
    center = (p + z2 / (2 * denominator)) / divisor
    radius = (
        z
        * math.sqrt(
            p * (1 - p) / denominator + z2 / (4 * denominator * denominator)
        )
        / divisor
    )
    return max(0.0, center - radius), min(1.0, center + radius)


def health_rate(numerator: int, denominator: int) -> BrokerHostHealthRateV1:
    interval = wilson_interval(numerator, denominator)
    return BrokerHostHealthRateV1(
        numerator,
        denominator,
        None if interval is None else numerator / denominator,
        None if interval is None else interval[0],
        None if interval is None else interval[1],
    )


def summarize_health_times(
    values: Iterable[int], *, maximum_samples: int = 8192
) -> BrokerHostHealthDistributionV1:
    """Exact bounded samples; nearest-rank quantiles, signed clock deltas allowed."""
    if type(maximum_samples) is not int or not 1 <= maximum_samples <= 100_000:
        raise ValueError("invalid sample bound")
    samples: list[int] = []
    for value in values:
        if (
            type(value) is not int
            or abs(value) >= 2**63
            or len(samples) >= maximum_samples
        ):
            raise ValueError(
                "health sample invalid or bounded inventory exceeded"
            )
        samples.append(value)
    if not samples:
        return BrokerHostHealthDistributionV1(0, None, None, None, None, None)
    samples.sort()
    count = len(samples)
    return BrokerHostHealthDistributionV1(
        count,
        samples[0],
        samples[-1],
        samples[(count + 1) // 2 - 1],
        samples[(95 * count + 99) // 100 - 1],
        sum(samples) / count,
    )


def little_law_diagnostic(
    arrivals: int,
    window_ns: int,
    mean_residence_ns: float,
    *,
    stationary_window_declared: bool,
) -> float | None:
    """L=lambda*W conditional on an explicit stationary-window assumption.

    Not an M/M/1 model, a stability test, or an independently measured queue
    length. Callers must retain the assumption separately from actual occupancy.
    """
    if (
        type(arrivals) is not int
        or not 0 <= arrivals <= 1_000_000
        or type(window_ns) is not int
        or not 1 <= window_ns < 2**63
        or type(mean_residence_ns) is not float
        or not math.isfinite(mean_residence_ns)
        or mean_residence_ns < 0
        or type(stationary_window_declared) is not bool
    ):
        raise ValueError("invalid Little law inputs")
    if not stationary_window_declared:
        return None
    return arrivals * mean_residence_ns / window_ns
