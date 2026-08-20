"""Deterministic scientific-math verification and report contracts."""

from __future__ import annotations

import json
import math
from dataclasses import replace

import pytest

import histdatacom
from histdatacom.reconstruction_math import (
    RECONSTRUCTION_MATH_FORMULAS,
    RECONSTRUCTION_MATH_FORMULA_VERSION,
    ReconstructionMathVerificationReportV1,
    bounded_negative_binomial_moments,
    current_reconstruction_math_verification_report,
    dimensionless_projection_burden,
    energy_score,
    exponential_hawkes_integrated_intensity,
    integrated_hawkes_kernel,
    inverse_time_rescaling_pit,
    invert_exponential_hawkes_integrated_intensity,
    nearest_prior_quote_age_ns,
    negative_binomial_failure_moments,
    negative_binomial_failure_probability,
    read_reconstruction_math_verification_report,
    time_rescaling_pit,
    triangle_bid_ask_envelope,
    two_by_two_spectral_radius,
    variogram_score,
)


def test_current_report_passes_is_deterministic_and_round_trips(
    tmp_path,
) -> None:
    """Every frozen formula passes and the exact report remains replayable."""
    current_reconstruction_math_verification_report.cache_clear()
    report = current_reconstruction_math_verification_report()

    assert report.formula_version == RECONSTRUCTION_MATH_FORMULA_VERSION
    assert report.passed is True
    assert report.summary == {
        "check_count": 23,
        "passed_check_count": 23,
        "failed_check_count": 0,
        "passed": True,
    }
    assert {item.formula_key for item in report.checks} == set(
        RECONSTRUCTION_MATH_FORMULAS
    )
    assert report == current_reconstruction_math_verification_report()
    assert histdatacom.current_reconstruction_math_verification_report() == (
        report
    )
    assert (
        ReconstructionMathVerificationReportV1.from_json(report.to_json())
        == report
    )

    path = tmp_path / "math-verification.json"
    path.write_text(report.to_json(), encoding="utf-8")
    assert read_reconstruction_math_verification_report(path) == report


def test_report_rejects_changed_check_and_derived_summary() -> None:
    """A stale ID or handwritten passing summary cannot replace evidence."""
    report = current_reconstruction_math_verification_report()
    with pytest.raises(ValueError, match="identity differs"):
        replace(
            report.checks[0],
            actual={"tampered": True},
        )

    payload = json.loads(report.to_json())
    payload["summary"]["passed"] = False
    with pytest.raises(ValueError, match="summary"):
        ReconstructionMathVerificationReportV1.from_dict(payload)


@pytest.mark.parametrize("probability", (0.12, 0.55, 0.93))
def test_negative_binomial_tail_reproduces_closed_form_moments(
    probability: float,
) -> None:
    """Bounded numerical sums verify the failures-before-successes formula."""
    expected_mean, expected_variance = negative_binomial_failure_moments(
        8, probability
    )
    mass, mean, variance, bound = bounded_negative_binomial_moments(
        8, probability
    )

    assert mass == pytest.approx(1.0, abs=2e-14)
    assert mean == pytest.approx(expected_mean, rel=2e-12)
    assert variance == pytest.approx(expected_variance, rel=3e-12)
    assert bound > 0
    assert negative_binomial_failure_probability(0, 8, probability) == (
        pytest.approx(probability**8)
    )


def test_negative_binomial_identity_retention_has_zero_failures() -> None:
    """Full retention is the valid degenerate zero-missing distribution."""
    assert negative_binomial_failure_probability(0, 8, 1.0) == 1.0
    assert negative_binomial_failure_probability(1, 8, 1.0) == 0.0
    assert negative_binomial_failure_moments(8, 1.0) == (0.0, 0.0)
    assert bounded_negative_binomial_moments(8, 1.0) == (
        1.0,
        0.0,
        0.0,
        0,
    )


@pytest.mark.parametrize(
    ("retained", "probability"),
    ((0, 0.5), (8, 0.0), (8, 1.1)),
)
def test_negative_binomial_contract_rejects_unidentified_parameters(
    retained: int, probability: float
) -> None:
    """The verification formula has the same identifiable parameter domain."""
    with pytest.raises(ValueError):
        negative_binomial_failure_moments(retained, probability)


def test_hawkes_integrated_kernel_and_exact_spectral_radius() -> None:
    """Component decay scales integrate away and the Perron root is exact."""
    matrix = integrated_hawkes_kernel(
        (
            ((0.30, 0.20), (0.20, 0.24)),
            ((0.10, 0.34), (0.25, 0.25)),
        )
    )

    assert matrix[0] == pytest.approx((0.50, 0.44))
    assert matrix[1] == pytest.approx((0.44, 0.50))
    assert two_by_two_spectral_radius(matrix) == pytest.approx(0.94)
    assert two_by_two_spectral_radius(((0.50, 0.45), (0.45, 0.50))) == (
        pytest.approx(0.95)
    )


def test_exponential_compensator_inverse_censor_reset_and_gradient() -> None:
    """Analytic hazards obey inverse, censor, reset, and derivative identities."""
    parameters = {
        "baseline_rate": 0.7,
        "decay_rate": 1.3,
        "excitation_masses": (0.4, 0.2),
        "history_event_times": (-0.5, 0.0),
    }
    start, end = 0.0, 1.25
    hazard = exponential_hawkes_integrated_intensity(start, end, **parameters)
    recovered = invert_exponential_hawkes_integrated_intensity(
        hazard, start, **parameters
    )

    assert recovered == pytest.approx(end, abs=1e-12)
    assert inverse_time_rescaling_pit(time_rescaling_pit(hazard)) == (
        pytest.approx(hazard, abs=1e-15)
    )
    assert exponential_hawkes_integrated_intensity(
        start, end, censor_time=0.8, **parameters
    ) == pytest.approx(
        exponential_hawkes_integrated_intensity(start, 0.8, **parameters)
    )
    assert exponential_hawkes_integrated_intensity(
        start,
        end,
        baseline_rate=0.7,
        decay_rate=1.3,
        excitation_masses=(0.4, 0.2),
        history_event_times=(-0.5, -0.1),
        reset_time=0.0,
    ) == pytest.approx(0.7 * (end - start))

    step = 1e-6
    upper = exponential_hawkes_integrated_intensity(
        start, end + step, **parameters
    )
    lower = exponential_hawkes_integrated_intensity(
        start, end - step, **parameters
    )
    gradient = (upper - lower) / (2.0 * step)
    intensity = 0.7 + sum(
        mass * 1.3 * math.exp(-1.3 * (end - event_time))
        for mass, event_time in zip((0.4, 0.2), (-0.5, 0.0), strict=True)
    )
    assert gradient == pytest.approx(intensity, abs=2e-10)


def test_compensator_rejects_future_history_and_unbounded_inverse() -> None:
    """No future event or insufficient inversion interval is accepted."""
    with pytest.raises(ValueError, match="future event"):
        exponential_hawkes_integrated_intensity(
            0.0,
            1.0,
            baseline_rate=1.0,
            decay_rate=1.0,
            excitation_masses=(0.2,),
            history_event_times=(0.1,),
        )
    with pytest.raises(ValueError, match="exceeds inversion"):
        invert_exponential_hawkes_integrated_intensity(
            10.0,
            0.0,
            baseline_rate=0.1,
            decay_rate=1.0,
            excitation_masses=(),
            history_event_times=(),
            maximum_interval=1.0,
        )


def test_proper_scores_cover_goldens_permutation_scaling_and_missing() -> None:
    """Finite estimators obey core invariants and reject missing cells."""
    reference = (0.0, 2.0)
    samples = ((0.0, 0.0), (2.0, 2.0))
    identity = (reference, reference)

    assert energy_score(reference, samples) == pytest.approx(
        2.0 - math.sqrt(2.0) / 2.0
    )
    assert variogram_score(reference, samples, 1.0) == pytest.approx(4.0)
    assert energy_score(reference, tuple(reversed(samples))) == (
        energy_score(reference, samples)
    )
    assert variogram_score(reference, tuple(reversed(samples)), 0.5) == (
        variogram_score(reference, samples, 0.5)
    )
    assert energy_score(reference, identity) == 0.0
    assert variogram_score(reference, identity, 0.5) == 0.0

    scaled_reference = tuple(3.0 * value for value in reference)
    scaled_samples = tuple(
        tuple(3.0 * value for value in sample) for sample in samples
    )
    assert energy_score(scaled_reference, scaled_samples) == pytest.approx(
        3.0 * energy_score(reference, samples)
    )
    assert variogram_score(
        scaled_reference, scaled_samples, 0.5
    ) == pytest.approx(3.0 * variogram_score(reference, samples, 0.5))

    with pytest.raises(TypeError, match="predictive cell"):
        energy_score(reference, ((0.0, None),))  # type: ignore[list-item]


def test_projection_burden_triangle_envelope_and_no_future_selection() -> None:
    """Projection and synchronization references preserve dimensional semantics."""
    burden = dimensionless_projection_burden(
        ((1.0000, 1.0002), (1.2000, 1.2000)),
        ((1.0001, 1.0003), (1.2001, 1.2002)),
        spread_epsilon=0.0001,
    )
    bid, ask = triangle_bid_ask_envelope(
        numerator_bid=1.1999,
        numerator_ask=1.2001,
        denominator_bid=1.4999,
        denominator_ask=1.5001,
    )

    assert burden == pytest.approx(5.0 / 3.0)
    assert bid == pytest.approx(1.1999 / 1.5001)
    assert ask == pytest.approx(1.2001 / 1.4999)
    assert nearest_prior_quote_age_ns(
        (1_000, 1_500, 2_500), 2_000, maximum_age_ns=600
    ) == (1, 500)
    assert nearest_prior_quote_age_ns(
        (1_000, 2_000, 2_500), 2_000, maximum_age_ns=0
    ) == (1, 0)

    with pytest.raises(ValueError, match="no prior"):
        nearest_prior_quote_age_ns((2_500,), 2_000, maximum_age_ns=1_000)
    with pytest.raises(ValueError, match="maximum age"):
        nearest_prior_quote_age_ns((1_000,), 2_000, maximum_age_ns=999)
