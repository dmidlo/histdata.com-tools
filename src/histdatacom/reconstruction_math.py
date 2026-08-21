"""Independent deterministic verification of reconstruction mathematics.

This module freezes transparent reference formulas for release-critical
reconstruction mathematics and compares them with the production paths used by
planning, generation, qualification, and cross-currency reconciliation.  The
result is a bounded content-addressed report that certification can bind
without persisting samples, event rows, or numerical work arrays.
"""

from __future__ import annotations

import hashlib
import json
import math
import random
from bisect import bisect_right
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, cast

from histdatacom.runtime_contracts import JSONValue
from histdatacom.synthetic.contracts import canonical_contract_json

RECONSTRUCTION_MATH_CHECK_SCHEMA_VERSION = (
    "histdatacom.reconstruction-math-check.v1"
)
RECONSTRUCTION_MATH_VERIFICATION_REPORT_SCHEMA_VERSION = (
    "histdatacom.reconstruction-math-verification-report.v1"
)
RECONSTRUCTION_MATH_VERIFICATION_ARTIFACT_KIND = (
    "reconstruction-math-verification-report"
)
RECONSTRUCTION_MATH_FORMULA_VERSION = "1.1.0"

MAX_RECONSTRUCTION_MATH_CHECKS = 64
MAX_RECONSTRUCTION_MATH_MAPPING_ITEMS = 64
MAX_RECONSTRUCTION_MATH_REPORT_BYTES = 2 * 1024 * 1024
MAX_RECONSTRUCTION_MATH_TEXT_LENGTH = 4_096

RECONSTRUCTION_MATH_FORMULAS: Mapping[str, str] = {
    "adaptive-cardinality-safety-v1": "floor(max_events * safety_fraction)",
    "energy-score-finite-ensemble-v1": ("mean(||X-y||) - 0.5*mean(||X-X'||)"),
    "fx-triangle-bid-ask-envelope-v1": (
        "direct_bid=numerator_bid/denominator_ask; "
        "direct_ask=numerator_ask/denominator_bid"
    ),
    "hawkes-integrated-kernel-v1": "K_ij=sum_q(alpha_ijq)",
    "negative-binomial-failures-v1": (
        "P(M=k)=Gamma(k+r)/(Gamma(r)k!)*p^r*(1-p)^k; "
        "E[M]=r(1-p)/p; Var[M]=r(1-p)/p^2"
    ),
    "ordered-discrete-mark-pit-v1": (
        "u=sum_{m'<m}(pi_m')+pi_m*semantic_uniform_sha256"
    ),
    "projection-burden-dimensionless-v1": (
        "sum_e(||q'_e-q_e||_1)/sum_e(max(spread(q_e),epsilon))"
    ),
    "quote-age-nearest-prior-v1": (
        "age_ns=probe_time_ns-selected_time_ns; " "0<=age_ns<=maximum_age_ns"
    ),
    "time-rescaling-pit-v1": "z=int(lambda(s|H_s)ds); u=1-exp(-z)",
    "variogram-score-finite-ensemble-v1": (
        "sum_ij(w_ij*(|y_i-y_j|^p-E|X_i-X_j|^p)^2)"
    ),
}


@dataclass(frozen=True, slots=True)
class ReconstructionMathCheckV1:
    """One bounded deterministic formula or production-parity check."""

    check_key: str
    formula_key: str
    passed: bool
    expected: Mapping[str, JSONValue]
    actual: Mapping[str, JSONValue]
    tolerance: float
    production_paths: tuple[str, ...]
    note: str
    check_id: str = ""
    schema_version: str = RECONSTRUCTION_MATH_CHECK_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, RECONSTRUCTION_MATH_CHECK_SCHEMA_VERSION
        )
        object.__setattr__(self, "check_key", _identifier(self.check_key))
        formula_key = _identifier(self.formula_key)
        if formula_key not in RECONSTRUCTION_MATH_FORMULAS:
            raise ValueError("math check references an unknown formula")
        object.__setattr__(self, "formula_key", formula_key)
        if not isinstance(self.passed, bool):
            raise TypeError("math check passed must be boolean")
        expected = _bounded_json_mapping(self.expected, "expected")
        actual = _bounded_json_mapping(self.actual, "actual")
        object.__setattr__(self, "expected", expected)
        object.__setattr__(self, "actual", actual)
        tolerance = _nonnegative_finite(self.tolerance, "tolerance")
        object.__setattr__(self, "tolerance", tolerance)
        paths = _normalized_text_tuple(self.production_paths)
        if not paths:
            raise ValueError("math check requires production paths")
        object.__setattr__(self, "production_paths", paths)
        object.__setattr__(self, "note", _required_text(self.note))
        expected_id = _stable_id(
            "reconstruction-math-check", self.identity_payload()
        )
        supplied = str(self.check_id or "").strip()
        if supplied and supplied != expected_id:
            raise ValueError("math check identity differs from content")
        object.__setattr__(self, "check_id", expected_id)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return complete stable check content."""
        return {
            "schema_version": self.schema_version,
            "check_key": self.check_key,
            "formula_key": self.formula_key,
            "passed": self.passed,
            "expected": dict(self.expected),
            "actual": dict(self.actual),
            "tolerance": self.tolerance,
            "production_paths": list(self.production_paths),
            "note": self.note,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "check_id": self.check_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ReconstructionMathCheckV1:
        return cls(
            check_key=str(data.get("check_key", "")),
            formula_key=str(data.get("formula_key", "")),
            passed=_strict_bool(data.get("passed"), "passed"),
            expected=_mapping(data.get("expected"), "expected"),
            actual=_mapping(data.get("actual"), "actual"),
            tolerance=_finite_float(data.get("tolerance"), "tolerance"),
            production_paths=_string_tuple(data.get("production_paths")),
            note=str(data.get("note", "")),
            check_id=str(data.get("check_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionMathVerificationReportV1:
    """Content-addressed result of every frozen reconstruction math check."""

    checks: tuple[ReconstructionMathCheckV1, ...]
    formula_version: str = RECONSTRUCTION_MATH_FORMULA_VERSION
    report_id: str = ""
    schema_version: str = RECONSTRUCTION_MATH_VERIFICATION_REPORT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            RECONSTRUCTION_MATH_VERIFICATION_REPORT_SCHEMA_VERSION,
        )
        if self.formula_version != RECONSTRUCTION_MATH_FORMULA_VERSION:
            raise ValueError("unsupported reconstruction math formula version")
        checks = tuple(sorted(self.checks, key=lambda item: item.check_key))
        if not checks or len(checks) > MAX_RECONSTRUCTION_MATH_CHECKS:
            raise ValueError("reconstruction math check collection is invalid")
        if len({item.check_key for item in checks}) != len(checks):
            raise ValueError("reconstruction math report duplicates check keys")
        object.__setattr__(self, "checks", checks)
        expected_id = _stable_id(
            "reconstruction-math-verification", self.identity_payload()
        )
        supplied = str(self.report_id or "").strip()
        if supplied and supplied != expected_id:
            raise ValueError("math verification report identity differs")
        object.__setattr__(self, "report_id", expected_id)
        if (
            len(self.to_json().encode("utf-8"))
            > MAX_RECONSTRUCTION_MATH_REPORT_BYTES
        ):
            raise ValueError("math verification report exceeds payload limit")

    @property
    def passed(self) -> bool:
        """Return whether every frozen formula check passed."""
        return all(item.passed for item in self.checks)

    @property
    def summary(self) -> dict[str, JSONValue]:
        """Return bounded derived counts for certification extraction."""
        return {
            "check_count": len(self.checks),
            "passed_check_count": sum(item.passed for item in self.checks),
            "failed_check_count": sum(not item.passed for item in self.checks),
            "passed": self.passed,
        }

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "formula_version": self.formula_version,
            "formulas": dict(sorted(RECONSTRUCTION_MATH_FORMULAS.items())),
            "checks": [item.to_dict() for item in self.checks],
            "summary": self.summary,
            "event_rows_inline": False,
            "samples_inline": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "report_id": self.report_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionMathVerificationReportV1:
        _require_derived(
            data,
            "formulas",
            dict(sorted(RECONSTRUCTION_MATH_FORMULAS.items())),
        )
        _require_derived(data, "event_rows_inline", False)
        _require_derived(data, "samples_inline", False)
        report = cls(
            checks=tuple(
                ReconstructionMathCheckV1.from_dict(item)
                for item in _mapping_sequence(data.get("checks"), "checks")
            ),
            formula_version=str(data.get("formula_version", "")),
            report_id=str(data.get("report_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        _require_derived(data, "summary", report.summary)
        return report

    @classmethod
    def from_json(cls, text: str) -> ReconstructionMathVerificationReportV1:
        value = json.loads(text)
        return cls.from_dict(_mapping(value, "report"))


def negative_binomial_failure_probability(
    failures: int, retained_count: int, retention_probability: float
) -> float:
    """Return P(M=k) for failures before ``retained_count`` successes."""
    k = _nonnegative_int(failures, "failures")
    retained = _positive_int(retained_count, "retained_count")
    probability = _probability(retention_probability)
    if probability == 1.0:
        return 1.0 if k == 0 else 0.0
    log_probability = (
        math.lgamma(k + retained)
        - math.lgamma(retained)
        - math.lgamma(k + 1)
        + retained * math.log(probability)
        + k * math.log1p(-probability)
    )
    return math.exp(log_probability)


def negative_binomial_failure_moments(
    retained_count: int, retention_probability: float
) -> tuple[float, float]:
    """Return the closed-form mean and variance for missing failures."""
    retained = _positive_int(retained_count, "retained_count")
    probability = _probability(retention_probability)
    if probability == 1.0:
        return 0.0, 0.0
    mean = retained * (1.0 - probability) / probability
    variance = retained * (1.0 - probability) / (probability**2)
    return mean, variance


def bounded_negative_binomial_moments(
    retained_count: int,
    retention_probability: float,
    *,
    tail_probability: float = 1e-14,
    maximum_failures: int = 1_000_000,
) -> tuple[float, float, float, int]:
    """Numerically sum a bounded tail and return mass, mean, variance, bound."""
    retained = _positive_int(retained_count, "retained_count")
    probability = _probability(retention_probability)
    if probability == 1.0:
        return 1.0, 0.0, 0.0, 0
    tail = _positive_finite(tail_probability, "tail_probability")
    if tail >= 1.0:
        raise ValueError("tail probability must be below one")
    maximum = _positive_int(maximum_failures, "maximum_failures")
    value = probability**retained
    mass = value
    first = 0.0
    second = 0.0
    bound = 0
    mode = max(
        0, math.floor((retained - 1) * (1.0 - probability) / probability)
    )
    for failures in range(maximum):
        next_failures = failures + 1
        value *= (failures + retained) / next_failures * (1.0 - probability)
        mass += value
        first += next_failures * value
        second += next_failures * next_failures * value
        bound = next_failures
        if next_failures > mode and value < tail and 1.0 - mass <= tail:
            break
    else:
        raise ValueError("negative-binomial tail exceeds numerical bound")
    variance = second - first * first
    return mass, first, variance, bound


def integrated_hawkes_kernel(
    component_masses: Sequence[Sequence[Sequence[float]]],
) -> tuple[tuple[float, ...], ...]:
    """Integrate multi-exponential kernels by summing component masses."""
    if not component_masses:
        raise ValueError("Hawkes component tensor cannot be empty")
    dimension = len(component_masses)
    rows: list[tuple[float, ...]] = []
    component_count: int | None = None
    for row in component_masses:
        if len(row) != dimension:
            raise ValueError("Hawkes component tensor must be square")
        values: list[float] = []
        for components in row:
            if not components:
                raise ValueError(
                    "Hawkes kernel requires exponential components"
                )
            if component_count is None:
                component_count = len(components)
            if len(components) != component_count:
                raise ValueError("Hawkes component counts differ")
            masses = tuple(
                _nonnegative_finite(item, "alpha") for item in components
            )
            values.append(sum(masses))
        rows.append(tuple(values))
    return tuple(rows)


def two_by_two_spectral_radius(
    matrix: Sequence[Sequence[float]],
) -> float:
    """Return the exact Perron root for a nonnegative 2x2 matrix."""
    if len(matrix) != 2 or any(len(row) != 2 for row in matrix):
        raise ValueError("reference spectral radius requires a 2x2 matrix")
    a, b = (_nonnegative_finite(value, "matrix value") for value in matrix[0])
    c, d = (_nonnegative_finite(value, "matrix value") for value in matrix[1])
    return 0.5 * (a + d + math.sqrt((a - d) ** 2 + 4.0 * b * c))


def exponential_hawkes_integrated_intensity(
    start_time: float,
    end_time: float,
    *,
    baseline_rate: float,
    decay_rate: float,
    excitation_masses: Sequence[float],
    history_event_times: Sequence[float],
    reset_time: float | None = None,
    censor_time: float | None = None,
) -> float:
    """Integrate a univariate exponential Hawkes intensity over one interval."""
    start = _finite_float(start_time, "start_time")
    end = _finite_float(end_time, "end_time")
    if censor_time is not None:
        end = min(end, _finite_float(censor_time, "censor_time"))
    if end < start:
        raise ValueError("integrated-intensity interval is reversed")
    baseline = _nonnegative_finite(baseline_rate, "baseline_rate")
    decay = _positive_finite(decay_rate, "decay_rate")
    masses = tuple(
        _nonnegative_finite(value, "excitation mass")
        for value in excitation_masses
    )
    history = tuple(
        _finite_float(value, "history event time")
        for value in history_event_times
    )
    if len(masses) != len(history):
        raise ValueError("Hawkes history and excitation masses differ")
    if any(event_time > start for event_time in history):
        raise ValueError("time-rescaling history contains a future event")
    reset = (
        None if reset_time is None else _finite_float(reset_time, "reset_time")
    )
    if reset is not None and reset > start:
        raise ValueError("reset time follows integration start")
    result = baseline * (end - start)
    for mass, event_time in zip(masses, history, strict=True):
        if reset is not None and event_time < reset:
            continue
        result += mass * (
            math.exp(-decay * (start - event_time))
            - math.exp(-decay * (end - event_time))
        )
    return result


def invert_exponential_hawkes_integrated_intensity(
    target_hazard: float,
    start_time: float,
    *,
    baseline_rate: float,
    decay_rate: float,
    excitation_masses: Sequence[float],
    history_event_times: Sequence[float],
    maximum_interval: float = 1_000_000.0,
) -> float:
    """Invert the monotone compensator with deterministic bounded bisection."""
    target = _nonnegative_finite(target_hazard, "target_hazard")
    start = _finite_float(start_time, "start_time")
    maximum = _positive_finite(maximum_interval, "maximum_interval")
    if target == 0.0:
        return start
    upper = min(1.0, maximum)
    while (
        exponential_hawkes_integrated_intensity(
            start,
            start + upper,
            baseline_rate=baseline_rate,
            decay_rate=decay_rate,
            excitation_masses=excitation_masses,
            history_event_times=history_event_times,
        )
        < target
    ):
        upper *= 2.0
        if upper > maximum:
            upper = maximum
            break
    upper_hazard = exponential_hawkes_integrated_intensity(
        start,
        start + upper,
        baseline_rate=baseline_rate,
        decay_rate=decay_rate,
        excitation_masses=excitation_masses,
        history_event_times=history_event_times,
    )
    if upper_hazard < target:
        raise ValueError("target hazard exceeds inversion interval")
    lower = 0.0
    for _iteration in range(96):
        midpoint = 0.5 * (lower + upper)
        value = exponential_hawkes_integrated_intensity(
            start,
            start + midpoint,
            baseline_rate=baseline_rate,
            decay_rate=decay_rate,
            excitation_masses=excitation_masses,
            history_event_times=history_event_times,
        )
        if value < target:
            lower = midpoint
        else:
            upper = midpoint
    return start + 0.5 * (lower + upper)


def time_rescaling_pit(integrated_hazard: float) -> float:
    """Map one nonnegative compensator increment to its uniform PIT."""
    hazard = _nonnegative_finite(integrated_hazard, "integrated_hazard")
    return -math.expm1(-hazard)


def inverse_time_rescaling_pit(value: float) -> float:
    """Recover one compensator increment from a PIT in [0,1)."""
    pit = _nonnegative_finite(value, "pit")
    if pit >= 1.0:
        raise ValueError("time-rescaling PIT must be below one")
    return -math.log1p(-pit)


def energy_score(
    reference: Sequence[float], samples: Sequence[Sequence[float]]
) -> float:
    """Return the equal-weight finite-ensemble energy score."""
    observed, members = _score_inputs(reference, samples)
    first = sum(_euclidean(member, observed) for member in members) / len(
        members
    )
    second = sum(
        _euclidean(left, right) for left in members for right in members
    ) / (len(members) ** 2)
    return max(0.0, first - 0.5 * second)


def variogram_score(
    reference: Sequence[float],
    samples: Sequence[Sequence[float]],
    order: float,
    *,
    weights: Sequence[Sequence[float]] | None = None,
) -> float:
    """Return a weighted finite-ensemble variogram score."""
    observed, members = _score_inputs(reference, samples)
    exponent = _positive_finite(order, "order")
    dimension = len(observed)
    if dimension < 2:
        return 0.0
    if weights is None:
        pair_weight = 1.0 / (dimension * (dimension - 1))
        matrix = tuple(
            tuple(
                0.0 if left == right else pair_weight
                for right in range(dimension)
            )
            for left in range(dimension)
        )
    else:
        matrix = tuple(
            tuple(
                _nonnegative_finite(value, "variogram weight") for value in row
            )
            for row in weights
        )
        if len(matrix) != dimension or any(
            len(row) != dimension for row in matrix
        ):
            raise ValueError("variogram weight dimensions differ")
    total = 0.0
    for left in range(dimension):
        for right in range(dimension):
            if left == right:
                continue
            observed_difference = (
                abs(observed[left] - observed[right]) ** exponent
            )
            forecast_difference = sum(
                abs(member[left] - member[right]) ** exponent
                for member in members
            ) / len(members)
            total += (
                matrix[left][right]
                * (observed_difference - forecast_difference) ** 2
            )
    return total


def dimensionless_projection_burden(
    original_quotes: Sequence[tuple[float, float]],
    projected_quotes: Sequence[tuple[float, float]],
    *,
    spread_epsilon: float,
) -> float:
    """Return L1 quote movement normalized by original quoted spread."""
    if not original_quotes or len(original_quotes) != len(projected_quotes):
        raise ValueError("projection quote collections differ or are empty")
    epsilon = _positive_finite(spread_epsilon, "spread_epsilon")
    numerator = 0.0
    denominator = 0.0
    for original, projected in zip(
        original_quotes, projected_quotes, strict=True
    ):
        original_bid, original_ask = _quote(original, "original quote")
        projected_bid, projected_ask = _quote(projected, "projected quote")
        numerator += abs(projected_bid - original_bid) + abs(
            projected_ask - original_ask
        )
        denominator += max(original_ask - original_bid, epsilon)
    return numerator / denominator


def triangle_bid_ask_envelope(
    *,
    numerator_bid: float,
    numerator_ask: float,
    denominator_bid: float,
    denominator_ask: float,
) -> tuple[float, float]:
    """Return the exact no-arbitrage bid/ask envelope for a direct FX cross."""
    numerator = _quote((numerator_bid, numerator_ask), "numerator quote")
    denominator = _quote(
        (denominator_bid, denominator_ask), "denominator quote"
    )
    return (
        numerator[0] / denominator[1],
        numerator[1] / denominator[0],
    )


def nearest_prior_quote_age_ns(
    event_times_ns: Sequence[int],
    probe_time_ns: int,
    *,
    maximum_age_ns: int,
) -> tuple[int, int]:
    """Select the latest nonfuture quote and return its index and age."""
    if not event_times_ns:
        raise ValueError("quote-age selection requires event times")
    times = tuple(
        _nonnegative_int(value, "event time") for value in event_times_ns
    )
    if tuple(sorted(times)) != times:
        raise ValueError("quote-age event times must be ordered")
    probe = _nonnegative_int(probe_time_ns, "probe_time_ns")
    maximum = _nonnegative_int(maximum_age_ns, "maximum_age_ns")
    index = bisect_right(times, probe) - 1
    if index < 0:
        raise ValueError("no prior quote exists")
    age = probe - times[index]
    if age > maximum:
        raise ValueError("nearest prior quote exceeds maximum age")
    return index, age


@lru_cache(maxsize=1)
def current_reconstruction_math_verification_report() -> (
    ReconstructionMathVerificationReportV1
):
    """Run and return the frozen deterministic production-parity harness."""
    checks: list[ReconstructionMathCheckV1] = []
    checks.extend(_negative_binomial_checks())
    checks.extend(_hawkes_stability_checks())
    checks.extend(_time_rescaling_checks())
    checks.extend(_proper_score_checks())
    checks.extend(_projection_and_synchronization_checks())
    return ReconstructionMathVerificationReportV1(tuple(checks))


def read_reconstruction_math_verification_report(
    path: str | Path,
) -> ReconstructionMathVerificationReportV1:
    """Read and identity-check one bounded verification report."""
    target = Path(path).expanduser().resolve()
    if not target.is_file():
        raise ValueError("math verification report does not exist")
    if target.stat().st_size > MAX_RECONSTRUCTION_MATH_REPORT_BYTES:
        raise ValueError("math verification report exceeds payload limit")
    return ReconstructionMathVerificationReportV1.from_json(
        target.read_text(encoding="utf-8")
    )


def _negative_binomial_checks() -> list[ReconstructionMathCheckV1]:
    from histdatacom.synthetic import marked_hawkes as production
    from histdatacom.synthetic import reconstruction_plan

    checks: list[ReconstructionMathCheckV1] = []
    tolerance = 2e-9
    for label, probability in (
        ("sparse", 0.12),
        ("transition", 0.55),
        ("modern", 0.93),
    ):
        expected_mean, expected_variance = negative_binomial_failure_moments(
            8, probability
        )
        mass, actual_mean, actual_variance, bound = (
            bounded_negative_binomial_moments(8, probability)
        )
        passed = (
            abs(1.0 - mass) <= tolerance
            and abs(actual_mean - expected_mean)
            <= tolerance * max(1.0, expected_mean)
            and abs(actual_variance - expected_variance)
            <= tolerance * max(1.0, expected_variance)
        )
        checks.append(
            _check(
                f"negative-binomial-{label}-moments",
                "negative-binomial-failures-v1",
                passed,
                expected={
                    "mass": 1.0,
                    "mean": _stable_float(expected_mean),
                    "variance": _stable_float(expected_variance),
                },
                actual={
                    "mass": _stable_float(mass),
                    "mean": _stable_float(actual_mean),
                    "variance": _stable_float(actual_variance),
                    "tail_bound": bound,
                },
                tolerance=tolerance,
                production_paths=(
                    "histdatacom.synthetic.marked_hawkes._sample_negative_binomial_failures",
                ),
                note="Bounded PMF summation independently reproduces closed-form moments.",
            )
        )

    rng = random.Random(507_001)
    draws = tuple(
        production._sample_negative_binomial_failures(8, 0.35, rng)
        for _ in range(20_000)
    )
    expected_mean, expected_variance = negative_binomial_failure_moments(
        8, 0.35
    )
    actual_mean = sum(draws) / len(draws)
    actual_variance = sum((value - actual_mean) ** 2 for value in draws) / len(
        draws
    )
    sampler_passed = (
        abs(actual_mean - expected_mean) <= 0.20
        and abs(actual_variance - expected_variance) <= 2.5
    )
    checks.append(
        _check(
            "negative-binomial-production-sampler",
            "negative-binomial-failures-v1",
            sampler_passed,
            expected={
                "mean": _stable_float(expected_mean),
                "variance": _stable_float(expected_variance),
                "draw_count": len(draws),
            },
            actual={
                "mean": _stable_float(actual_mean),
                "variance": _stable_float(actual_variance),
                "minimum": min(draws),
                "maximum": max(draws),
            },
            tolerance=2.5,
            production_paths=(
                "histdatacom.synthetic.marked_hawkes._sample_negative_binomial_failures",
            ),
            note="A fixed-seed production sample agrees with the independent parameterization.",
        )
    )
    safety_fraction = reconstruction_plan._ADAPTIVE_CARDINALITY_SAFETY_FRACTION
    actual_limit = math.floor(8_192 * safety_fraction)
    checks.append(
        _check(
            "adaptive-planning-cardinality-identity",
            "adaptive-cardinality-safety-v1",
            actual_limit == 6_963 and safety_fraction == 0.85,
            expected={
                "max_events": 8_192,
                "safety_fraction": 0.85,
                "limit": 6_963,
            },
            actual={
                "max_events": 8_192,
                "safety_fraction": safety_fraction,
                "limit": actual_limit,
            },
            tolerance=0.0,
            production_paths=(
                "histdatacom.synthetic.reconstruction_plan._ADAPTIVE_CARDINALITY_SAFETY_FRACTION",
            ),
            note="The current planning identity remains floor(8192 * 0.85) = 6963.",
        )
    )
    return checks


def _hawkes_stability_checks() -> list[ReconstructionMathCheckV1]:
    from histdatacom.synthetic import marked_hawkes as production

    components = (
        ((0.30, 0.20), (0.20, 0.24)),
        ((0.10, 0.34), (0.25, 0.25)),
    )
    matrix = integrated_hawkes_kernel(components)
    expected_matrix = ((0.50, 0.44), (0.44, 0.50))
    integrated_passed = all(
        math.isclose(actual, expected, rel_tol=0.0, abs_tol=1e-15)
        for actual_row, expected_row in zip(
            matrix, expected_matrix, strict=True
        )
        for actual, expected in zip(actual_row, expected_row, strict=True)
    )
    checks = [
        _check(
            "hawkes-integrated-kernel",
            "hawkes-integrated-kernel-v1",
            integrated_passed,
            expected={"matrix": [list(row) for row in expected_matrix]},
            actual={"matrix": [list(row) for row in matrix]},
            tolerance=1e-15,
            production_paths=(
                "histdatacom.synthetic.marked_hawkes._fit_fixed_decay",
                "histdatacom.synthetic.marked_hawkes._validate_model_stability",
            ),
            note="Integrated multi-exponential mass is independent of decay scales.",
        )
    ]
    limit = production.MarkedHawkesConfigV1(
        production.HawkesExcitationStructure.FULL
    ).maximum_branching_ratio
    for label, cross_mass, expected_admitted in (
        ("below", 0.44, True),
        ("at", 0.45, False),
        ("above", 0.46, False),
    ):
        candidate = ((0.50, cross_mass), (cross_mass, 0.50))
        reference_radius = two_by_two_spectral_radius(candidate)
        production_radius = production._spectral_radius(candidate)
        model = {
            "symbols": ["eurusd", "gbpusd"],
            "baseline_rates_per_second": [1.0, 1.0],
            "excitation_matrix": [list(row) for row in candidate],
            "spectral_radius": production_radius,
            "stability_margin": 1.0 - production_radius,
        }
        admitted = True
        try:
            production._validate_model_stability(
                model,
                excitation_structure=production.HawkesExcitationStructure.FULL,
                maximum_branching_ratio=limit,
            )
        except ValueError:
            admitted = False
        checks.append(
            _check(
                f"hawkes-stability-{label}-bound",
                "hawkes-integrated-kernel-v1",
                math.isclose(
                    production_radius,
                    reference_radius,
                    rel_tol=1e-12,
                    abs_tol=1e-12,
                )
                and admitted is expected_admitted,
                expected={
                    "spectral_radius": _stable_float(reference_radius),
                    "admitted": expected_admitted,
                    "configured_bound": limit,
                },
                actual={
                    "spectral_radius": _stable_float(production_radius),
                    "admitted": admitted,
                },
                tolerance=1e-12,
                production_paths=(
                    "histdatacom.synthetic.marked_hawkes._spectral_radius",
                    "histdatacom.synthetic.marked_hawkes._validate_model_stability",
                ),
                note="Stationary admission is strict at the configured branching bound.",
            )
        )

    tampered = {
        "symbols": ["eurusd", "gbpusd"],
        "baseline_rates_per_second": [1.0, 1.0],
        "excitation_matrix": [[0.50, 0.01], [0.00, 0.50]],
        "spectral_radius": 0.50,
        "stability_margin": 0.50,
    }
    serialized = json.loads(json.dumps(tampered, sort_keys=True))
    rejected = False
    try:
        production._validate_model_stability(
            serialized,
            excitation_structure=production.HawkesExcitationStructure.DIAGONAL,
            maximum_branching_ratio=limit,
        )
    except ValueError:
        rejected = True
    checks.append(
        _check(
            "hawkes-serialized-structure-tampering",
            "hawkes-integrated-kernel-v1",
            rejected,
            expected={"tampering_rejected": True},
            actual={"tampering_rejected": rejected},
            tolerance=0.0,
            production_paths=(
                "histdatacom.synthetic.marked_hawkes._validate_model_stability",
            ),
            note="A serialized diagonal model cannot acquire undeclared cross-excitation.",
        )
    )
    return checks


def _time_rescaling_checks() -> list[ReconstructionMathCheckV1]:
    from histdatacom.synthetic.hawkes_residuals import (
        _accumulate_hazard,
        _randomized_mark_pit,
    )
    from histdatacom.synthetic.qualification import (
        PointProcessResidualInputV1,
        PointProcessResidualMethod,
    )

    baseline_rate = 0.7
    decay_rate = 1.3
    excitation_masses = (0.4, 0.2)
    history_event_times = (-0.5, 0.0)
    start = 0.0
    end = 1.25
    hazard = exponential_hawkes_integrated_intensity(
        start,
        end,
        baseline_rate=baseline_rate,
        decay_rate=decay_rate,
        excitation_masses=excitation_masses,
        history_event_times=history_event_times,
    )
    midpoint_count = 200_000
    step = (end - start) / midpoint_count
    numeric = 0.0
    for index in range(midpoint_count):
        time_value = start + (index + 0.5) * step
        intensity = baseline_rate + sum(
            mass
            * decay_rate
            * math.exp(-decay_rate * (time_value - event_time))
            for mass, event_time in zip(
                excitation_masses,
                history_event_times,
                strict=True,
            )
        )
        numeric += intensity * step
    checks = [
        _check(
            "time-rescaling-analytic-compensator",
            "time-rescaling-pit-v1",
            math.isclose(hazard, numeric, rel_tol=0.0, abs_tol=2e-11),
            expected={"midpoint_integral": _stable_float(numeric)},
            actual={"analytic_integral": _stable_float(hazard)},
            tolerance=2e-11,
            production_paths=(
                "histdatacom.synthetic.qualification.PointProcessResidualInputV1.time_pits",
                "histdatacom.synthetic.neural_tpp._time_integral",
            ),
            note="The exponential analytic compensator matches a dense midpoint integral.",
        )
    ]
    recovered_end = invert_exponential_hawkes_integrated_intensity(
        hazard,
        start,
        baseline_rate=baseline_rate,
        decay_rate=decay_rate,
        excitation_masses=excitation_masses,
        history_event_times=history_event_times,
    )
    checks.append(
        _check(
            "time-rescaling-inverse-compensator",
            "time-rescaling-pit-v1",
            math.isclose(recovered_end, end, rel_tol=0.0, abs_tol=1e-12),
            expected={"end_time": end},
            actual={"recovered_end_time": _stable_float(recovered_end)},
            tolerance=1e-12,
            production_paths=(
                "histdatacom.synthetic.neural_tpp._inverse_elapsed_seconds",
            ),
            note="Bounded bisection recovers the interval from its analytic hazard.",
        )
    )
    elapsed = 1.25
    recursion = (2.0, 1.5)
    baseline = (0.7, 0.4)
    excitation = ((0.3, 0.2), (0.1, 0.25))
    accumulated = [0.0, 0.0]
    _accumulate_hazard(
        accumulated,
        recursion,
        baseline,
        excitation,
        decay_rate,
        elapsed,
    )
    kernel_mass = 1.0 - math.exp(-decay_rate * elapsed)
    expected_multivariate = tuple(
        baseline[destination] * elapsed
        + sum(
            excitation[destination][source] * recursion[source] * kernel_mass
            for source in range(2)
        )
        for destination in range(2)
    )
    checks.append(
        _check(
            "hawkes-raw-proposal-multivariate-compensator",
            "time-rescaling-pit-v1",
            all(
                math.isclose(actual, expected, rel_tol=0.0, abs_tol=1e-15)
                for actual, expected in zip(
                    accumulated, expected_multivariate, strict=True
                )
            ),
            expected={
                "destination_0": _stable_float(expected_multivariate[0]),
                "destination_1": _stable_float(expected_multivariate[1]),
            },
            actual={
                "destination_0": _stable_float(accumulated[0]),
                "destination_1": _stable_float(accumulated[1]),
            },
            tolerance=1e-15,
            production_paths=(
                "histdatacom.synthetic.hawkes_residuals._accumulate_hazard",
            ),
            note=(
                "The installed raw-proposal adapter exactly integrates every "
                "destination/source exponential-kernel contribution."
            ),
        )
    )
    mark_probabilities = {
        "ask_only": 0.10,
        "bid_only": 0.20,
        "joint": 0.30,
        "unchanged": 0.40,
    }
    semantic_key = "math-verification|ordered-mark-pit"
    randomizer = (
        int.from_bytes(
            hashlib.sha256(semantic_key.encode("utf-8")).digest()[:8], "big"
        )
        / 2**64
    )
    expected_mark_pit = 0.30 + 0.30 * randomizer
    production_mark_pit = _randomized_mark_pit(
        mark_probabilities, "joint", semantic_key=semantic_key
    )
    checks.append(
        _check(
            "hawkes-conditioned-mark-semantic-pit",
            "ordered-discrete-mark-pit-v1",
            math.isclose(
                production_mark_pit,
                expected_mark_pit,
                rel_tol=0.0,
                abs_tol=1e-15,
            ),
            expected={"ordered_mark_pit": _stable_float(expected_mark_pit)},
            actual={"production_mark_pit": _stable_float(production_mark_pit)},
            tolerance=1e-15,
            production_paths=(
                "histdatacom.synthetic.hawkes_residuals._randomized_mark_pit",
            ),
            note=(
                "Ordered categorical PIT randomization is fixed by the "
                "semantic SHA-256 key."
            ),
        )
    )
    pit = time_rescaling_pit(hazard)
    production_input = PointProcessResidualInputV1(
        engine_id="math-verification",
        config_id="math-verification-config",
        fit_id="math-verification-fit",
        split_kind="validation",
        stratum_id="fixture",
        method=PointProcessResidualMethod.ANALYTIC_TIME_RESCALING,
        integrated_hazards=(hazard,),
    )
    production_pit = production_input.time_pits[0]
    checks.append(
        _check(
            "time-rescaling-pit-roundtrip",
            "time-rescaling-pit-v1",
            math.isclose(pit, production_pit, rel_tol=0.0, abs_tol=1e-15)
            and math.isclose(
                inverse_time_rescaling_pit(pit),
                hazard,
                rel_tol=0.0,
                abs_tol=1e-15,
            ),
            expected={"integrated_hazard": _stable_float(hazard)},
            actual={
                "production_pit": _stable_float(production_pit),
                "reference_pit": _stable_float(pit),
                "roundtrip_hazard": _stable_float(
                    inverse_time_rescaling_pit(pit)
                ),
            },
            tolerance=1e-15,
            production_paths=(
                "histdatacom.synthetic.qualification.PointProcessResidualInputV1.time_pits",
            ),
            note="The production PIT and stable inverse use the declared transform.",
        )
    )
    censored = exponential_hawkes_integrated_intensity(
        start,
        end,
        baseline_rate=baseline_rate,
        decay_rate=decay_rate,
        excitation_masses=excitation_masses,
        history_event_times=history_event_times,
        censor_time=0.8,
    )
    explicit = exponential_hawkes_integrated_intensity(
        start,
        0.8,
        baseline_rate=baseline_rate,
        decay_rate=decay_rate,
        excitation_masses=excitation_masses,
        history_event_times=history_event_times,
    )
    checks.append(
        _check(
            "time-rescaling-right-censoring",
            "time-rescaling-pit-v1",
            math.isclose(censored, explicit, rel_tol=0.0, abs_tol=1e-15),
            expected={"explicit_short_interval": _stable_float(explicit)},
            actual={"censored_interval": _stable_float(censored)},
            tolerance=1e-15,
            production_paths=(
                "histdatacom.synthetic.qualification.PointProcessResidualInputV1",
            ),
            note="A censored interval accumulates hazard only through the censor boundary.",
        )
    )
    reset = exponential_hawkes_integrated_intensity(
        start,
        end,
        baseline_rate=baseline_rate,
        decay_rate=decay_rate,
        excitation_masses=excitation_masses,
        history_event_times=(-0.5, -0.1),
        reset_time=0.0,
    )
    baseline_only = baseline_rate * (end - start)
    checks.append(
        _check(
            "time-rescaling-reset-boundary",
            "time-rescaling-pit-v1",
            math.isclose(reset, baseline_only, rel_tol=0.0, abs_tol=1e-15),
            expected={"baseline_only_hazard": baseline_only},
            actual={"reset_hazard": _stable_float(reset)},
            tolerance=1e-15,
            production_paths=(
                "histdatacom.synthetic.marked_hawkes.MarkedHawkesConfigV1.fit_boundary_policy",
            ),
            note="Calibration-window reset excludes every pre-boundary excitation event.",
        )
    )
    step_size = 1e-6
    upper = exponential_hawkes_integrated_intensity(
        start,
        end + step_size,
        baseline_rate=baseline_rate,
        decay_rate=decay_rate,
        excitation_masses=excitation_masses,
        history_event_times=history_event_times,
    )
    lower = exponential_hawkes_integrated_intensity(
        start,
        end - step_size,
        baseline_rate=baseline_rate,
        decay_rate=decay_rate,
        excitation_masses=excitation_masses,
        history_event_times=history_event_times,
    )
    finite_difference = (upper - lower) / (2.0 * step_size)
    terminal_intensity = baseline_rate + sum(
        mass * decay_rate * math.exp(-decay_rate * (end - event_time))
        for mass, event_time in zip(
            excitation_masses,
            history_event_times,
            strict=True,
        )
    )
    checks.append(
        _check(
            "time-rescaling-compensator-gradient",
            "time-rescaling-pit-v1",
            math.isclose(
                finite_difference,
                terminal_intensity,
                rel_tol=0.0,
                abs_tol=2e-10,
            ),
            expected={"terminal_intensity": _stable_float(terminal_intensity)},
            actual={"finite_difference": _stable_float(finite_difference)},
            tolerance=2e-10,
            production_paths=(
                "histdatacom.synthetic.neural_tpp._time_integral",
            ),
            note="The finite-difference compensator gradient equals terminal intensity.",
        )
    )
    return checks


def _proper_score_checks() -> list[ReconstructionMathCheckV1]:
    from histdatacom.synthetic import qualification as production

    reference = (0.0, 2.0)
    samples = ((0.0, 0.0), (2.0, 2.0))
    energy = energy_score(reference, samples)
    production_energy = production._energy_score(reference, samples)
    variogram = variogram_score(reference, samples, 1.0)
    production_variogram = production._variogram_score(reference, samples, 1.0)
    checks = [
        _check(
            "proper-score-fixed-goldens",
            "energy-score-finite-ensemble-v1",
            math.isclose(energy, 2.0 - math.sqrt(2.0) / 2.0, abs_tol=1e-15)
            and math.isclose(energy, production_energy, abs_tol=1e-15)
            and math.isclose(variogram, 4.0, abs_tol=1e-15)
            and math.isclose(variogram, production_variogram, abs_tol=1e-15),
            expected={
                "energy": _stable_float(2.0 - math.sqrt(2.0) / 2.0),
                "variogram_p1": 4.0,
            },
            actual={
                "reference_energy": _stable_float(energy),
                "production_energy": _stable_float(production_energy),
                "reference_variogram_p1": _stable_float(variogram),
                "production_variogram_p1": _stable_float(production_variogram),
            },
            tolerance=1e-15,
            production_paths=(
                "histdatacom.synthetic.qualification._energy_score",
                "histdatacom.synthetic.qualification._variogram_score",
            ),
            note="Transparent finite-ensemble goldens match both production proper scores.",
        )
    ]
    permuted = tuple(reversed(samples))
    checks.append(
        _check(
            "proper-score-permutation-invariance",
            "energy-score-finite-ensemble-v1",
            energy_score(reference, permuted) == energy
            and variogram_score(reference, permuted, 0.5)
            == variogram_score(reference, samples, 0.5),
            expected={"permutation_invariant": True},
            actual={
                "energy_invariant": energy_score(reference, permuted) == energy,
                "variogram_invariant": variogram_score(reference, permuted, 0.5)
                == variogram_score(reference, samples, 0.5),
            },
            tolerance=0.0,
            production_paths=(
                "histdatacom.synthetic.qualification._energy_score",
                "histdatacom.synthetic.qualification._variogram_score",
            ),
            note="Forecast-member ordering cannot change either score.",
        )
    )
    identity = (reference, reference)
    scaled_reference = tuple(3.0 * value for value in reference)
    scaled_samples = tuple(
        tuple(3.0 * value for value in sample) for sample in samples
    )
    checks.append(
        _check(
            "proper-score-degenerate-nonnegative-scaling",
            "variogram-score-finite-ensemble-v1",
            energy_score(reference, identity) == 0.0
            and variogram_score(reference, identity, 0.5) == 0.0
            and energy >= 0.0
            and variogram >= 0.0
            and math.isclose(
                energy_score(scaled_reference, scaled_samples),
                3.0 * energy,
                abs_tol=1e-14,
            )
            and math.isclose(
                variogram_score(scaled_reference, scaled_samples, 0.5),
                3.0 * variogram_score(reference, samples, 0.5),
                abs_tol=1e-14,
            ),
            expected={
                "degenerate_energy": 0.0,
                "degenerate_variogram": 0.0,
                "energy_scale": 3.0,
                "variogram_p05_scale": 3.0,
            },
            actual={
                "degenerate_energy": energy_score(reference, identity),
                "degenerate_variogram": variogram_score(
                    reference, identity, 0.5
                ),
                "energy_scale": _stable_float(
                    energy_score(scaled_reference, scaled_samples) / energy
                ),
                "variogram_p05_scale": _stable_float(
                    variogram_score(scaled_reference, scaled_samples, 0.5)
                    / variogram_score(reference, samples, 0.5)
                ),
            },
            tolerance=1e-14,
            production_paths=(
                "histdatacom.synthetic.qualification._energy_score",
                "histdatacom.synthetic.qualification._variogram_score",
            ),
            note="Scores are nonnegative, vanish for identity, and obey scale laws.",
        )
    )
    missing_rejected = False
    try:
        energy_score(reference, ((0.0, cast(float, None)),))
    except (TypeError, ValueError):
        missing_rejected = True
    checks.append(
        _check(
            "proper-score-missing-cell-policy",
            "energy-score-finite-ensemble-v1",
            missing_rejected,
            expected={"missing_cell_policy": "reject", "rejected": True},
            actual={
                "missing_cell_policy": "reject",
                "rejected": missing_rejected,
            },
            tolerance=0.0,
            production_paths=(
                "histdatacom.synthetic.qualification._energy_score",
                "histdatacom.synthetic.qualification._variogram_score",
            ),
            note="Missing predictive cells are rejected rather than silently imputed.",
        )
    )
    return checks


def _projection_and_synchronization_checks() -> list[ReconstructionMathCheckV1]:
    from histdatacom.synthetic import cross_currency as production
    from histdatacom.synthetic.contracts import SyntheticEventV1

    original = ((1.0000, 1.0002), (1.2000, 1.2000))
    projected = ((1.0001, 1.0003), (1.2001, 1.2002))
    burden = dimensionless_projection_burden(
        original, projected, spread_epsilon=0.0001
    )
    checks = [
        _check(
            "dimensionless-projection-burden",
            "projection-burden-dimensionless-v1",
            math.isclose(burden, 5.0 / 3.0, rel_tol=0.0, abs_tol=1e-12),
            expected={"burden": _stable_float(5.0 / 3.0)},
            actual={"burden": _stable_float(burden)},
            tolerance=1e-12,
            production_paths=(
                "histdatacom.synthetic.cross_currency.CrossCurrencyProjectionLineageV1",
                "histdatacom.synthetic.cross_currency._required_projection_quote",
            ),
            note="Projection movement is dimensionless and zero spreads use only epsilon.",
        )
    ]
    expected_envelope = triangle_bid_ask_envelope(
        numerator_bid=1.1999,
        numerator_ask=1.2001,
        denominator_bid=1.4999,
        denominator_ask=1.5001,
    )

    def event(symbol: str, bid: float, ask: float) -> SyntheticEventV1:
        return SyntheticEventV1.generated(
            symbol=symbol,
            event_time_ns=1_000,
            event_sequence=0,
            bid=bid,
            ask=ask,
            run_id="math-verification-run",
            ensemble_member_id="member-01",
            source_version_id="source-version",
            left_anchor_event_id=f"{symbol}-left",
            right_anchor_event_id=f"{symbol}-right",
            generator_id="math-verification-generator",
            generator_version="1.0.0",
            generator_config_id="math-verification-config",
            constraint_set_id="math-verification-constraints",
        )

    relationship = (
        production.eurusd_triangle_reconciliation_config().relationships[0]
    )
    events = (
        event("EURGBP", 0.79, 0.81),
        event("EURUSD", 1.1999, 1.2001),
        event("GBPUSD", 1.4999, 1.5001),
    )
    production_envelope = production._required_projection_quote(
        relationship, events, "eurgbp"
    )
    envelope_passed = all(
        math.isclose(left, right, rel_tol=0.0, abs_tol=1e-15)
        for left, right in zip(
            expected_envelope, production_envelope, strict=True
        )
    )
    checks.append(
        _check(
            "triangle-exact-bid-ask-envelope",
            "fx-triangle-bid-ask-envelope-v1",
            envelope_passed,
            expected={"bid": expected_envelope[0], "ask": expected_envelope[1]},
            actual={
                "bid": production_envelope[0],
                "ask": production_envelope[1],
            },
            tolerance=1e-15,
            production_paths=(
                "histdatacom.synthetic.cross_currency._required_projection_quote",
            ),
            note="The production triangle projection crosses the correct bid/ask sides.",
        )
    )
    index, age = nearest_prior_quote_age_ns(
        (1_000, 1_500, 2_500), 2_000, maximum_age_ns=600
    )
    future_refused = False
    stale_refused = False
    try:
        nearest_prior_quote_age_ns((2_500,), 2_000, maximum_age_ns=1_000)
    except ValueError:
        future_refused = True
    try:
        nearest_prior_quote_age_ns((1_000,), 2_000, maximum_age_ns=999)
    except ValueError:
        stale_refused = True
    exact_index, exact_age = nearest_prior_quote_age_ns(
        (1_000, 2_000, 2_500), 2_000, maximum_age_ns=0
    )
    checks.append(
        _check(
            "quote-age-and-no-future-use",
            "quote-age-nearest-prior-v1",
            (index, age) == (1, 500)
            and (exact_index, exact_age) == (1, 0)
            and future_refused
            and stale_refused,
            expected={
                "selected_index": 1,
                "age_ns": 500,
                "exact_age_ns": 0,
                "future_refused": True,
                "stale_refused": True,
            },
            actual={
                "selected_index": index,
                "age_ns": age,
                "exact_age_ns": exact_age,
                "future_refused": future_refused,
                "stale_refused": stale_refused,
            },
            tolerance=0.0,
            production_paths=(
                "histdatacom.synthetic.cross_currency._relationship_matches",
                "histdatacom.synthetic.cross_currency.CrossCurrencyJoinPolicy",
            ),
            note="Nearest-prior support is bounded, exact when requested, and never future-filled.",
        )
    )
    return checks


def _check(
    check_key: str,
    formula_key: str,
    passed: bool,
    *,
    expected: Mapping[str, JSONValue],
    actual: Mapping[str, JSONValue],
    tolerance: float,
    production_paths: tuple[str, ...],
    note: str,
) -> ReconstructionMathCheckV1:
    return ReconstructionMathCheckV1(
        check_key=check_key,
        formula_key=formula_key,
        passed=passed,
        expected=expected,
        actual=actual,
        tolerance=tolerance,
        production_paths=production_paths,
        note=note,
    )


def _score_inputs(
    reference: Sequence[float], samples: Sequence[Sequence[float]]
) -> tuple[tuple[float, ...], tuple[tuple[float, ...], ...]]:
    observed = tuple(
        _finite_float(value, "reference cell") for value in reference
    )
    if not observed:
        raise ValueError("proper score requires a reference vector")
    members = tuple(
        tuple(_finite_float(value, "predictive cell") for value in sample)
        for sample in samples
    )
    if not members:
        raise ValueError("proper score requires predictive samples")
    if any(len(member) != len(observed) for member in members):
        raise ValueError("proper-score vector dimensions differ")
    return observed, members


def _euclidean(left: Sequence[float], right: Sequence[float]) -> float:
    if len(left) != len(right):
        raise ValueError("Euclidean vector dimensions differ")
    return math.sqrt(
        sum(
            (left_value - right_value) ** 2
            for left_value, right_value in zip(left, right, strict=True)
        )
    )


def _quote(value: Sequence[float], name: str) -> tuple[float, float]:
    if len(value) != 2:
        raise ValueError(f"{name} must contain bid and ask")
    bid = _positive_finite(value[0], f"{name} bid")
    ask = _positive_finite(value[1], f"{name} ask")
    if ask < bid:
        raise ValueError(f"{name} ask is below bid")
    return bid, ask


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _stable_float(value: float) -> float:
    return float(f"{_finite_float(value, 'stable float'):.12g}")


def _identifier(value: Any) -> str:
    text = str(value).strip().lower().replace("_", "-")
    if (
        not text
        or len(text) > 256
        or text.startswith("-")
        or text.endswith("-")
        or "--" in text
        or any(
            character not in "abcdefghijklmnopqrstuvwxyz0123456789-"
            for character in text
        )
    ):
        raise ValueError("invalid reconstruction math identifier")
    return text


def _required_text(value: Any) -> str:
    text = str(value).strip()
    if not text or len(text) > MAX_RECONSTRUCTION_MATH_TEXT_LENGTH:
        raise ValueError("required reconstruction math text is invalid")
    return text


def _normalized_text_tuple(values: Sequence[Any]) -> tuple[str, ...]:
    selected = tuple(sorted({_required_text(item) for item in values}))
    if len(selected) > MAX_RECONSTRUCTION_MATH_MAPPING_ITEMS:
        raise ValueError("reconstruction math text collection exceeds limit")
    return selected


def _bounded_json_mapping(
    value: Mapping[str, JSONValue], name: str
) -> Mapping[str, JSONValue]:
    mapping = dict(value)
    if not mapping or len(mapping) > MAX_RECONSTRUCTION_MATH_MAPPING_ITEMS:
        raise ValueError(f"{name} mapping is empty or unbounded")
    canonical_contract_json(mapping)
    return mapping


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    return value


def _mapping_sequence(value: Any, name: str) -> tuple[Mapping[str, Any], ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise TypeError(f"{name} must be a sequence")
    if not value or len(value) > MAX_RECONSTRUCTION_MATH_CHECKS:
        raise ValueError(f"{name} sequence is empty or unbounded")
    return tuple(_mapping(item, name) for item in value)


def _string_tuple(value: Any) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise TypeError("expected a sequence of strings")
    return tuple(str(item) for item in value)


def _strict_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be boolean")
    return value


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    return value


def _nonnegative_int(value: Any, name: str) -> int:
    result = _strict_int(value, name)
    if result < 0:
        raise ValueError(f"{name} must be nonnegative")
    return result


def _positive_int(value: Any, name: str) -> int:
    result = _strict_int(value, name)
    if result <= 0:
        raise ValueError(f"{name} must be positive")
    return result


def _finite_float(value: Any, name: str) -> float:
    if isinstance(value, bool):
        raise TypeError(f"{name} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as err:
        raise TypeError(f"{name} must be numeric") from err
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _nonnegative_finite(value: Any, name: str) -> float:
    result = _finite_float(value, name)
    if result < 0.0:
        raise ValueError(f"{name} must be nonnegative")
    return result


def _positive_finite(value: Any, name: str) -> float:
    result = _finite_float(value, name)
    if result <= 0.0:
        raise ValueError(f"{name} must be positive")
    return result


def _probability(value: Any) -> float:
    result = _finite_float(value, "retention_probability")
    if not 0.0 < result <= 1.0:
        raise ValueError("retention probability must be inside (0,1]")
    return result


def _require_schema(actual: str, expected: str) -> None:
    if actual != expected:
        raise ValueError(
            f"unsupported reconstruction math schema; expected {expected}"
        )


def _require_derived(data: Mapping[str, Any], name: str, expected: Any) -> None:
    if data.get(name) != expected:
        raise ValueError(f"derived reconstruction math field {name} differs")


__all__ = [
    "RECONSTRUCTION_MATH_CHECK_SCHEMA_VERSION",
    "RECONSTRUCTION_MATH_FORMULAS",
    "RECONSTRUCTION_MATH_FORMULA_VERSION",
    "RECONSTRUCTION_MATH_VERIFICATION_ARTIFACT_KIND",
    "RECONSTRUCTION_MATH_VERIFICATION_REPORT_SCHEMA_VERSION",
    "ReconstructionMathCheckV1",
    "ReconstructionMathVerificationReportV1",
    "bounded_negative_binomial_moments",
    "current_reconstruction_math_verification_report",
    "dimensionless_projection_burden",
    "energy_score",
    "exponential_hawkes_integrated_intensity",
    "integrated_hawkes_kernel",
    "inverse_time_rescaling_pit",
    "invert_exponential_hawkes_integrated_intensity",
    "nearest_prior_quote_age_ns",
    "negative_binomial_failure_moments",
    "negative_binomial_failure_probability",
    "read_reconstruction_math_verification_report",
    "time_rescaling_pit",
    "triangle_bid_ask_envelope",
    "two_by_two_spectral_radius",
    "variogram_score",
]
