"""Independent numerical oracles; no expected value from the tested helper."""

from __future__ import annotations

import json
import math
from dataclasses import replace

import numpy as np
import pytest
import statsmodels.api as sm
from statsmodels.stats.sandwich_covariance import cov_hac

from histdatacom.forecasting import math_reference as ref
from histdatacom.forecasting import math_verification as verification


@pytest.mark.parametrize("size", (1, 2, 4, 8, 16))
def test_covariance_solve_matches_independent_numpy(size):
    rng = np.random.default_rng(580 + size)
    raw = rng.normal(size=(size, size))
    covariance = raw @ raw.T + 0.05 * np.eye(size)
    matrix = tuple(tuple(float(x) for x in row) for row in covariance)
    inverse_times_one = np.linalg.solve(covariance, np.ones(size))
    expected = inverse_times_one / inverse_times_one.sum()
    weights, variance = ref.covariance_weights(matrix)
    assert weights == pytest.approx(expected, rel=1e-11, abs=1e-12)
    assert sum(weights) == pytest.approx(1)
    assert variance == pytest.approx(float(expected @ covariance @ expected))
    assert variance == pytest.approx(1 / float(inverse_times_one.sum()))


@pytest.mark.parametrize(
    "matrix",
    (
        ((1.0, 1.0), (1.0, 1.0)),
        ((1.0, 1.0 - 1e-14), (1.0 - 1e-14, 1.0)),
        ((1.0, 2.0), (2.0, 1.0)),
        ((1.0, 0.2), (0.3, 1.0)),
        ((5e-324, 0.0), (0.0, 5e-324)),
        ((1.0,), (2.0,)),
    ),
)
def test_invalid_singular_near_singular_and_underflow_covariance_refuse(matrix):
    with pytest.raises(ValueError):
        ref.covariance_weights(matrix)


def test_negative_weights_not_clipped_and_constant_ensemble_not_independence():
    weights, variance = ref.covariance_weights(((1.0, 2.0), (2.0, 9.0)))
    assert weights == pytest.approx((7 / 6, -1 / 6))
    assert variance == pytest.approx(5 / 6)
    assert ref.equicorrelation_diversity(
        10, 0.8
    ).effective_count == pytest.approx(1.2195121951219512)
    assert ref.equicorrelation_diversity(10, 1.0).effective_count == 1
    assert ref.equicorrelation_diversity(3, -0.25).effective_count == 6
    assert ref.equicorrelation_diversity(2, -1.0).effective_count is None


@pytest.mark.parametrize(
    "n,rho", ((1, 0.1), (0, 0.0), (True, 0.0), (3, -0.51), (3, 1.01))
)
def test_effective_count_domain(n, rho):
    with pytest.raises(ValueError):
        ref.equicorrelation_diversity(n, rho)


@pytest.mark.parametrize("size", (1, 2, 7, 31))
def test_crps_two_constructions_and_numpy_pairwise(size):
    rng = np.random.default_rng(580 + size)
    draws = rng.normal(size=size)
    outcome = 0.4
    expected = float(
        np.abs(draws - outcome).mean()
        - 0.5 * np.abs(draws[:, None] - draws[None, :]).mean()
    )
    support = tuple((float(x), 1 / size) for x in sorted(draws))
    actual = ref.empirical_crps(tuple(float(x) for x in draws), outcome)
    assert actual == pytest.approx(expected)
    assert ref.weighted_crps(support, outcome) == pytest.approx(expected)
    assert (
        ref.empirical_crps(tuple(float(x) for x in reversed(draws)), outcome)
        == actual
    )
    assert actual >= 0


def test_crps_degenerate_weighted_and_documented_arithmetic_refusal():
    assert ref.empirical_crps((2.0, 2.0), 2.0) == 0
    assert ref.empirical_crps((2.0,), 3.0) == 1
    assert ref.weighted_crps(((0.0, 0.25), (2.0, 0.75)), 2.0) == 0.125
    with pytest.raises(ValueError, match="finite"):
        ref.empirical_crps((-1e308, 1e308), 0.0)
    assert ref.weighted_crps(((-1e308, 0.5), (0.0, 0.5)), 0.0) == 2.5e307


def test_pinball_quantiles_and_tied_minimizers():
    support = ((0.0, 0.25), (2.0, 0.5), (4.0, 0.25))
    for tau in (0.1, 0.25, 0.5, 0.75, 0.9):
        q = ref.discrete_quantile(support, tau)
        losses = [
            (
                value,
                sum(
                    mass * ((tau - int(y < value)) * (y - value))
                    for y, mass in support
                ),
            )
            for value in np.linspace(-1, 5, 61)
        ]
        best = min(loss for _, loss in losses)
        assert sum(
            mass * ref.pinball_loss(q, y, tau) for y, mass in support
        ) == pytest.approx(best)
    assert ref.distribution_point(((2.0, 0.5), (4.0, 0.5)), "mean") == 3
    assert (
        ref.distribution_point(
            ((2.0, 0.5), (4.0, 0.5)), "lower-weighted-median"
        )
        == 2
    )


@pytest.mark.parametrize(
    "p", (0.0, 5e-324, 1e-12, 0.1, 0.5, 0.9, 1 - 1e-12, 1.0)
)
@pytest.mark.parametrize("outcome", (False, True))
def test_binary_probability_extremes_without_clipping(p, outcome):
    assert 0 <= ref.brier_score(p, outcome) <= 1
    result = ref.binary_log_score(p, outcome)
    impossible = (p == 0 and outcome) or (p == 1 and not outcome)
    assert result.impossible_event is impossible
    if impossible:
        assert result.loss is None
    else:
        expected = -math.log(p) if outcome else -math.log1p(-p)
        assert result.loss == pytest.approx(expected)


def test_fixed_bin_calibration_fixture_and_empty_bin():
    assert ref.reliability_bins(
        (0.0, 0.25, 0.75, 1.0), (False, False, True, True), (0.0, 0.5, 1.0)
    ) == ((2, 0.125, 0.0), (2, 0.875, 1.0))
    assert ref.reliability_bins(
        (0.0, 1.0), (False, True), (0.0, 0.2, 0.8, 1.0)
    )[1] == (0, None, None)
    with pytest.raises(ValueError):
        ref.reliability_bins((0.5,), (), (0.0, 1.0))


@pytest.mark.parametrize("size,lags", ((20, 0), (20, 1), (40, 3), (100, 8)))
def test_paired_hac_matches_independent_statsmodels(size, lags):
    rng = np.random.default_rng(580)
    differences = rng.normal(0.15, 0.3, size)
    fitted = sm.OLS(differences, np.ones((size, 1))).fit()
    variance_of_mean = float(
        cov_hac(fitted, nlags=lags, use_correction=True)[0, 0]
    )
    result = ref.paired_hac(tuple(float(x) for x in differences), lags=lags)
    assert result.mean_difference == pytest.approx(float(fitted.params[0]))
    assert result.standard_error**2 == pytest.approx(variance_of_mean)
    assert result.lower_95 == pytest.approx(
        float(fitted.params[0])
        - 1.959963984540054 * math.sqrt(variance_of_mean)
    )
    assert result.upper_95 == pytest.approx(
        float(fitted.params[0])
        + 1.959963984540054 * math.sqrt(variance_of_mean)
    )


def test_hac_tiny_missing_constant_and_horizon_refusals():
    for values, kwargs in (
        ((1.0, 2.0), {"lags": 0}),
        ((1.0,) * 20, {"lags": 0, "horizon_steps": 2}),
        ((1.0,) * 19 + (math.nan,), {"lags": 1}),
        ((1.0,) * 20, {"lags": True}),
    ):
        with pytest.raises(ValueError):
            ref.paired_hac(values, **kwargs)
    result = ref.paired_hac((2.0,) * 20, lags=1)
    assert result.mean_difference == 2
    assert result.statistic is result.p_value is result.lower_95 is None


@pytest.mark.parametrize("bad", (True, None, "1", math.nan, math.inf, 10**1000))
def test_public_numeric_refusal_is_bounded_value_error(bad):
    with pytest.raises(ValueError):
        ref.point_losses(bad, 0.0)


def test_formula_receipt_reexecutes_and_has_exact_complete_coverage(
    monkeypatch,
):
    report = verification.ForecastMathVerificationV1.current()
    assert report.passed
    assert {check.formula for check in report.checks} == {
        key for key, _ in verification.FORMULAS
    }
    assert (
        verification.ForecastMathVerificationV1.from_json(report.to_json())
        == report
    )
    with pytest.raises(ValueError):
        replace(report, checks=report.checks[:-1])
    for field in ("formulas", "checks", "code_bindings"):
        data = report.to_dict()
        data[field] = data[field][:-1]
        with pytest.raises(ValueError):
            verification.ForecastMathVerificationV1.from_dict(data)
    original = ref.brier_score
    monkeypatch.setattr(ref, "brier_score", lambda p, y: original(p, y) + 0.1)
    assert not verification.ForecastMathVerificationV1.current().passed
    with pytest.raises(ValueError):
        verification.ForecastMathVerificationV1.from_dict(report.to_dict())


def test_expanded_wire_depth_bytes_and_alias_bounds(monkeypatch):
    value = {"x": "é😀\\\n"}
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    monkeypatch.setattr(verification, "MAX_MATH_ARTIFACT_BYTES", len(encoded))
    assert verification.math_json(value) == encoded
    monkeypatch.setattr(
        verification, "MAX_MATH_ARTIFACT_BYTES", len(encoded) - 1
    )
    with pytest.raises(ValueError):
        verification.math_json(value)
    monkeypatch.setattr(
        verification, "MAX_MATH_ARTIFACT_BYTES", 8 * 1024 * 1024
    )
    branch = ["x" * 1024] * 100
    with pytest.raises(ValueError, match="byte"):
        verification.math_json({"alias": [branch] * 10_000})
    cyclic = []
    cyclic.append(cyclic)
    with pytest.raises(ValueError, match="nesting"):
        verification.math_json({"cycle": cyclic})
