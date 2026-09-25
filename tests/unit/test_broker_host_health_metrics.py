"""Independent decimal/reference math for host-observed health diagnostics."""

import math
from decimal import Decimal, localcontext

import pytest

from histdatacom.broker_plugin_health import (
    health_rate,
    little_law_diagnostic,
    summarize_health_times,
    wilson_interval,
)


@pytest.mark.parametrize(
    "count,total",
    [(0, 1), (1, 1), (25, 10000), (1, 1000000), (999999, 1000000), (19, 43)],
)
def test_wilson_matches_independent_high_precision_quadratic(count, total):
    # Solve n(p_hat-p)^2=z^2*p*(1-p), independently of production's
    # center/radius implementation. The bound is numerical, counts stay exact.
    with localcontext() as context:
        context.prec = 60
        n, k = Decimal(total), Decimal(count)
        z2 = Decimal("1.959963984540054") ** 2
        a, b, c = n + z2, -(2 * k + z2), k * k / n
        root = (b * b - 4 * a * c).sqrt()
        expected = (float((-b - root) / (2 * a)), float((-b + root) / (2 * a)))
    actual = wilson_interval(count, total)
    assert actual == pytest.approx(expected, rel=0, abs=3e-16)
    assert health_rate(count, total).value == count / total


def test_documented_rate_and_conditional_little_law_reference():
    lower, upper = wilson_interval(25, 10000)
    assert lower == pytest.approx(0.0016939976111583195)
    assert upper == pytest.approx(0.0036880807676648924)
    assert (
        little_law_diagnostic(
            5000, 1_000_000_000, 12_000_000.0, stationary_window_declared=True
        )
        == 60.0
    )
    assert (
        little_law_diagnostic(
            5000, 1_000_000_000, 12_000_000.0, stationary_window_declared=False
        )
        is None
    )


def test_no_support_is_not_zero_rate_or_zero_latency():
    assert wilson_interval(0, 0) is None
    rate = health_rate(0, 0)
    assert (rate.value, rate.wilson_lower, rate.wilson_upper) == (
        None,
        None,
        None,
    )
    summary = summarize_health_times(())
    assert summary.count == 0
    assert summary.mean_ns is None


def test_exact_nearest_rank_signed_deltas_and_large_integer_mean():
    samples = [-10, -2, 0, 17, 23, 25, 10**12]
    result = summarize_health_times(reversed(samples))
    assert result.minimum_ns == -10
    assert result.maximum_ns == 10**12
    assert result.p50_ns == 17
    assert result.p95_ns == 10**12
    assert result.mean_ns == float(
        sum(Decimal(x) for x in samples) / len(samples)
    )


@pytest.mark.parametrize(
    "numerator,denominator",
    [(-1, 2), (3, 2), (1, 0), (True, 2), (1, False), (1.0, 2), (0, 1000001)],
)
def test_invalid_rate_inputs_refuse(numerator, denominator):
    with pytest.raises(ValueError):
        wilson_interval(numerator, denominator)


@pytest.mark.parametrize(
    "values", [[True], [1.0], [2**63], [-(2**63)], [math.inf], [math.nan]]
)
def test_noninteger_and_unbounded_time_samples_refuse(values):
    with pytest.raises(ValueError):
        summarize_health_times(values)


def test_sample_budget_never_silently_truncates_quantiles():
    with pytest.raises(ValueError, match="bound"):
        summarize_health_times(range(4), maximum_samples=3)


@pytest.mark.parametrize(
    "arguments",
    [
        (True, 1, 1.0, True),
        (1, 0, 1.0, True),
        (1, 1, -1.0, True),
        (1, 1, math.nan, True),
        (1, 1, 1.0, 1),
    ],
)
def test_invalid_little_law_inputs_refuse(arguments):
    n, window, residence, stationary = arguments
    with pytest.raises(ValueError):
        little_law_diagnostic(
            n, window, residence, stationary_window_declared=stationary
        )
