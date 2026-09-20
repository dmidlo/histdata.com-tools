"""Executed versioned formula checks; a receipt alone is not certification."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from histdatacom.runtime_contracts import JSONValue

from . import math_reference as reference
from .contracts import _load, _seal, _verify

FORECAST_MATH_FORMULA_VERSION = "1.0.0"
MAX_MATH_ARTIFACT_BYTES = 8 * 1024 * 1024
MAX_MATH_DEPTH = 64
FORMULAS = (
    (
        "covariance-combination-v1",
        "w=solve(Sigma,1)/sum(solve(Sigma,1)); variance=1/sum(solve(Sigma,1)); positive-definite covariance, unbiased errors assumed",
    ),
    (
        "equicorrelation-diversity-v1",
        "variance_ratio=(1+(n-1)*rho)/n; n_eff=1/variance_ratio; equal weights and variances only",
    ),
    ("empirical-crps-v1", "mean(abs(x-y))-sum(abs(x_i-x_j))/(2*M*M)"),
    (
        "weighted-crps-v1",
        "integral((F(z)-1{y<=z})**2,dz); finite discrete support",
    ),
    (
        "pinball-quantile-v1",
        "(tau-1{y<q})*(y-q); lower inverse CDF; all minimizers allowed at mass-boundary ties",
    ),
    ("binary-brier-v1", "(p-y)**2; y boolean, p in [0,1]"),
    (
        "binary-log-score-v1",
        "-y*log(p)-(1-y)*log(1-p); impossible realized event gives explicit positive infinity, not clipped probability",
    ),
    (
        "fixed-bin-reliability-v1",
        "fixed [left,right) bins with final right endpoint included; retain count/mean probability/frequency; empty bins unavailable",
    ),
    (
        "surprise-value-added-v1",
        "D=Ahat-Chat; S=A-F; VA=abs(A-F)-abs(A-Ahat); identical units or explicit precommitted scale",
    ),
    (
        "point-scores-v1",
        "error=point-actual; absolute/squared error; MAE, RMSE, median absolute error; positive ex-ante scale divides error before scoring",
    ),
    (
        "distribution-point-v1",
        "mean=sum(value*mass); lower weighted median=min(value: cumulative mass>=0.5); exact retained finite support",
    ),
    (
        "paired-bartlett-hac-v1",
        "d=baseline_loss-candidate_loss; gamma(k)=sum(centered[t]*centered[t-k])/n; LRV=n/(n-1)*(gamma(0)+2*sum((1-k/(L+1))*gamma(k))); SE=sqrt(LRV/n); normal-approximation 95% CI; explicit event-index lags/horizon/minimum count",
    ),
)
_SOURCE_FILES = (
    "math_reference.py",
    "math_verification.py",
    "math_reports.py",
    "math_artifacts.py",
    "contracts.py",
    "scoring.py",
    "feature_forecasts.py",
    "engine_runner.py",
)


def _guard(value: object) -> None:
    """Bound expanded bytes with O(depth) traversal space, before copying."""
    remaining = MAX_MATH_ARTIFACT_BYTES

    def charge(cost: int) -> None:
        nonlocal remaining
        remaining -= cost
        if remaining < 0:
            raise ValueError("forecast math artifact exceeds byte bounds")

    def string_cost(text: str) -> int:
        if len(text) > MAX_MATH_ARTIFACT_BYTES:
            raise ValueError("forecast math text exceeds byte bounds")
        # Match ensure_ascii without allocating a potentially 12x escaped copy.
        return 2 + sum(
            (
                12
                if ord(char) > 0xFFFF
                else (
                    2
                    if char in '\\"\b\f\n\r\t'
                    else 6 if ord(char) < 32 or ord(char) > 126 else 1
                )
            )
            for char in text
        )

    def visit(item: object, depth: int) -> None:
        if depth > MAX_MATH_DEPTH:
            raise ValueError("forecast math nesting exceeds bounds")
        if type(item) is dict:
            if len(item) > 4096:
                raise ValueError("forecast math mapping exceeds bounds")
            charge(2 + max(0, len(item) - 1))
            for key, child in item.items():
                if type(key) is not str or len(key) > 4096:
                    raise ValueError("forecast math key is invalid")
                charge(string_cost(key) + 1)
                visit(child, depth + 1)
        elif isinstance(item, (list, tuple)):
            if len(item) > 10_000:
                raise ValueError("forecast math collection exceeds bounds")
            charge(2 + max(0, len(item) - 1))
            for child in item:
                visit(child, depth + 1)
        elif item is None or type(item) in (str, int, float, bool):
            if type(item) is int and not -(2**63) <= item < 2**63:
                raise ValueError("forecast math integer exceeds int64")
            if type(item) is float and not math.isfinite(item):
                raise ValueError("forecast math wire numbers must be finite")
            charge(
                string_cost(item)
                if type(item) is str
                else len(json.dumps(item, ensure_ascii=True, allow_nan=False))
            )
        else:
            raise ValueError("forecast math requires JSON values")

    visit(value, 0)


def math_json(value: Mapping[str, Any]) -> str:
    body = dict(value)
    _guard(body)
    return json.dumps(
        body,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def math_load(text: str) -> Mapping[str, Any]:
    if (
        type(text) is not str
        or len(text) > MAX_MATH_ARTIFACT_BYTES
        or len(text.encode()) > MAX_MATH_ARTIFACT_BYTES
    ):
        raise ValueError("forecast math artifact exceeds byte bounds")
    data = _load(text)
    _guard(data)
    return data


def math_seal(kind: str, body: Mapping[str, Any]) -> dict[str, JSONValue]:
    _guard(dict(body))
    result = _seal(kind, body)
    _guard(result)
    return result


def _bindings() -> tuple[tuple[str, str], ...]:
    return tuple(
        (
            name,
            hashlib.sha256(
                Path(__file__).with_name(name).read_bytes()
            ).hexdigest(),
        )
        for name in _SOURCE_FILES
    )


@dataclass(frozen=True, slots=True)
class ForecastMathCheckV1:
    key: str
    formula: str
    expected: tuple[float, ...]
    actual: tuple[float, ...]
    tolerance: float = 1e-12

    def __post_init__(self) -> None:
        if (
            type(self.key) is not str
            or not 1 <= len(self.key) <= 128
            or self.formula not in dict(FORMULAS)
        ):
            raise ValueError("unknown mathematical check/formula")
        object.__setattr__(
            self, "expected", reference.vector(self.expected, 64)
        )
        object.__setattr__(self, "actual", reference.vector(self.actual, 64))
        if len(self.expected) != len(self.actual) or self.tolerance != 1e-12:
            raise ValueError("math check comparison policy differs")

    @property
    def passed(self) -> bool:
        return all(
            math.isclose(a, b, rel_tol=self.tolerance, abs_tol=self.tolerance)
            for a, b in zip(self.expected, self.actual)
        )

    def to_dict(self) -> dict[str, JSONValue]:
        return math_seal(
            "forecast-math-check",
            {
                "key": self.key,
                "formula": self.formula,
                "expected": list(self.expected),
                "actual": list(self.actual),
                "tolerance": self.tolerance,
                "passed": self.passed,
            },
        )


def _run_checks() -> tuple[ForecastMathCheckV1, ...]:
    """Frozen goldens/alternative equations; never read the check's expected data."""
    checks = []

    def check(
        key: str,
        formula: str,
        expected: tuple[float, ...],
        actual: tuple[float, ...],
    ) -> None:
        checks.append(ForecastMathCheckV1(key, formula, expected, actual))

    for key, matrix, expected in (
        ("diagonal", ((1.0, 0.0), (0.0, 4.0)), (0.8, 0.2, 0.8, 0.8, 1.0)),
        (
            "negative-unconstrained-weight",
            ((1.0, 2.0), (2.0, 9.0)),
            (7 / 6, -1 / 6, 5 / 6, 5 / 6, 1.0),
        ),
        (
            "dense-four",
            (
                (1.0, 0.5, 0.25, 0.125),
                (0.5, 1.0, 0.5, 0.25),
                (0.25, 0.5, 1.0, 0.5),
                (0.125, 0.25, 0.5, 1.0),
            ),
            (1 / 3, 1 / 6, 1 / 6, 1 / 3, 0.5, 0.5, 1.0),
        ),
    ):
        weights, variance = reference.covariance_weights(matrix)
        check(
            key,
            "covariance-combination-v1",
            expected,
            (
                *weights,
                variance,
                reference.quadratic_variance(matrix, weights),
                sum(weights),
            ),
        )
    for n, rho, ratio, effective in (
        (10, 0.8, 0.82, 10 / 8.2),
        (3, -0.25, 1 / 6, 6.0),
        (1, 0.0, 1.0, 1.0),
    ):
        result = reference.equicorrelation_diversity(n, rho)
        check(
            f"equicorrelation-{n}",
            "equicorrelation-diversity-v1",
            (ratio, effective),
            (result.relative_variance, result.effective_count or 0.0),
        )
    diversity = reference.equicorrelation_diversity(2, -1.0)
    check(
        "perfect-negative-boundary",
        "equicorrelation-diversity-v1",
        (0.0, 1.0),
        (diversity.relative_variance, float(diversity.effective_count is None)),
    )
    for key, draws, outcome, crps_expected in (
        ("correct", (2.0, 2.0), 2.0, 0.0),
        ("unit-miss", (2.0,), 3.0, 1.0),
        ("nondegenerate", (0.0, 1.0, 3.0), 2.0, 2 / 3),
        ("permuted", (3.0, 0.0, 1.0), 2.0, 2 / 3),
    ):
        check(
            "crps-" + key,
            "empirical-crps-v1",
            (crps_expected,),
            (reference.empirical_crps(draws, outcome),),
        )
    check(
        "cdf-integral",
        "weighted-crps-v1",
        (2 / 3, 0.125),
        (
            reference.weighted_crps(
                ((0.0, 1 / 3), (1.0, 1 / 3), (3.0, 1 / 3)), 2.0
            ),
            reference.weighted_crps(((0.0, 0.25), (2.0, 0.75)), 2.0),
        ),
    )
    support = ((0.0, 0.25), (2.0, 0.5), (4.0, 0.25))

    def loss(q: float, tau: float) -> float:
        return sum(
            mass * reference.pinball_loss(q, y, tau) for y, mass in support
        )

    check(
        "pinball-and-minimizers",
        "pinball-quantile-v1",
        (0.5, 1.5, 2.0, 0.5, 0.75, 0.5, 0.5),
        (
            reference.pinball_loss(1.0, 3.0, 0.25),
            reference.pinball_loss(3.0, 1.0, 0.25),
            reference.discrete_quantile(support, 0.5),
            loss(2.0, 0.5),
            loss(1.0, 0.5),
            loss(0.0, 0.25),
            loss(1.0, 0.25),
        ),
    )
    check(
        "binary-brier-bounds",
        "binary-brier-v1",
        (0.0, 1.0, 0.5625),
        (
            reference.brier_score(0.0, False),
            reference.brier_score(0.0, True),
            reference.brier_score(0.25, True),
        ),
    )
    correct = reference.binary_log_score(1.0, True)
    impossible = reference.binary_log_score(0.0, True)
    moderate = reference.binary_log_score(0.5, False)
    extreme = reference.binary_log_score(5e-324, True)
    check(
        "binary-log-endpoints",
        "binary-log-score-v1",
        (0.0, 1.0, math.log(2), 744.4400719213812),
        (
            correct.loss or 0.0,
            float(impossible.impossible_event and impossible.loss is None),
            moderate.loss or 0.0,
            extreme.loss or 0.0,
        ),
    )
    bins = reference.reliability_bins(
        (0.0, 0.25, 0.75, 1.0), (False, False, True, True), (0.0, 0.5, 1.0)
    )
    empty = reference.reliability_bins(
        (0.0, 1.0), (False, True), (0.0, 0.2, 0.8, 1.0)
    )[1]
    check(
        "fixed-bin-calibration-fixture",
        "fixed-bin-reliability-v1",
        (2.0, 0.125, 0.0, 2.0, 0.875, 1.0, 1.0),
        tuple(float(x) for row in bins for x in row if x is not None)
        + (float(empty == (0, None, None)),),
    )
    check(
        "signed-divergence-surprise-va",
        "surprise-value-added-v1",
        (1.0, 1.0, -1.0),
        reference.surprise_values(4.0, 3.0, 2.0, 1.0),
    )
    check(
        "explicit-direction-polarity",
        "surprise-value-added-v1",
        (1.0, -1.0, -1.0, 1.0, 0.0, 0.0),
        tuple(
            float(value)
            for x in (2.0, -2.0, 0.0)
            for value in reference.directional_mapping(x, -1)
        ),
    )
    losses = reference.point_losses(4.0, 2.0, consensus=1.0, scale=2.0)
    check(
        "point-scale-and-errors",
        "point-scores-v1",
        (2.0, 2.0, 4.0, 1.0, 1.0, -1.0),
        tuple(float(value) for value in losses.values() if value is not None),
    )
    check(
        "retained-distribution-point",
        "distribution-point-v1",
        (3.0, 2.0),
        (
            reference.distribution_point(((2.0, 0.5), (4.0, 0.5)), "mean"),
            reference.distribution_point(
                ((2.0, 0.5), (4.0, 0.5)), "lower-weighted-median"
            ),
        ),
    )
    hac = reference.paired_hac((0.0, 2.0) * 10, lags=1)
    constant = reference.paired_hac((2.0,) * 20, lags=1)
    check(
        "hac-bartlett-alternating",
        "paired-bartlett-hac-v1",
        (1.0, 1 / 19, math.sqrt(1 / 380), 1.0),
        (
            hac.mean_difference,
            hac.long_run_variance,
            hac.standard_error,
            float(constant.statistic is None and constant.p_value is None),
        ),
    )
    refusals: tuple[tuple[str, str, Callable[[], object]], ...] = (
        (
            "singular",
            "covariance-combination-v1",
            lambda: reference.covariance_weights(((1.0, 1.0), (1.0, 1.0))),
        ),
        (
            "near-singular",
            "covariance-combination-v1",
            lambda: reference.covariance_weights(
                ((1.0, 1.0 - 1e-14), (1.0 - 1e-14, 1.0))
            ),
        ),
        (
            "asymmetric",
            "covariance-combination-v1",
            lambda: reference.covariance_weights(((1.0, 0.1), (0.2, 1.0))),
        ),
        (
            "indefinite",
            "covariance-combination-v1",
            lambda: reference.covariance_weights(((1.0, 2.0), (2.0, 1.0))),
        ),
        (
            "invalid-correlation",
            "equicorrelation-diversity-v1",
            lambda: reference.equicorrelation_diversity(3, -0.6),
        ),
        (
            "missing-draw",
            "empirical-crps-v1",
            lambda: reference.empirical_crps((1.0, math.nan), 0.0),
        ),
        (
            "tiny-hac",
            "paired-bartlett-hac-v1",
            lambda: reference.paired_hac((1.0, 2.0), lags=0),
        ),
        (
            "wrong-horizon-hac",
            "paired-bartlett-hac-v1",
            lambda: reference.paired_hac(
                (0.0, 2.0) * 10, lags=0, horizon_steps=2
            ),
        ),
        (
            "extreme-overflow",
            "point-scores-v1",
            lambda: reference.point_losses(1e308, -1e308),
        ),
        (
            "invalid-probability",
            "binary-log-score-v1",
            lambda: reference.binary_log_score(1.1, True),
        ),
    )
    for key, formula, execute in refusals:
        refused = False
        try:
            execute()
        except ValueError:
            refused = True
        check(key, formula, (1.0,), (float(refused),))
    return tuple(checks)


@dataclass(frozen=True, slots=True)
class ForecastMathVerificationV1:
    """Current executable evidence, not arbitrary caller-supplied pass flags."""

    checks: tuple[ForecastMathCheckV1, ...]
    code_bindings: tuple[tuple[str, str], ...]
    formula_version: str = FORECAST_MATH_FORMULA_VERSION

    def __post_init__(self) -> None:
        if (
            type(self.checks) is not tuple
            or not 1 <= len(self.checks) <= 64
            or any(
                type(check) is not ForecastMathCheckV1 for check in self.checks
            )
        ):
            raise ValueError("verification requires bounded typed checks")
        if (
            self.formula_version != FORECAST_MATH_FORMULA_VERSION
            or self.code_bindings != _bindings()
            or self.checks != _run_checks()
        ):
            raise ValueError(
                "math verification differs from current executed formulas/code"
            )
        if {check.formula for check in self.checks} != {
            key for key, _ in FORMULAS
        }:
            raise ValueError("verification formula coverage is incomplete")
        self.to_json()

    @property
    def passed(self) -> bool:
        return all(check.passed for check in self.checks)

    @property
    def verification_id(self) -> str:
        return str(self.to_dict()["id"])

    def to_dict(self) -> dict[str, JSONValue]:
        return math_seal(
            "forecast-math-verification",
            {
                "formula_version": self.formula_version,
                "formulas": [list(pair) for pair in FORMULAS],
                "code_bindings": [list(pair) for pair in self.code_bindings],
                "checks": [check.to_dict() for check in self.checks],
                "passed": self.passed,
                "claim": "deterministic-formula-verification-not-empirical-qualification",
            },
        )

    def to_json(self) -> str:
        return math_json(self.to_dict())

    @classmethod
    def current(cls) -> ForecastMathVerificationV1:
        return cls(_run_checks(), _bindings())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ForecastMathVerificationV1:
        _guard(dict(data))
        result = cls.current()
        _verify(data, result.to_dict())
        return result

    @classmethod
    def from_json(cls, text: str) -> ForecastMathVerificationV1:
        return cls.from_dict(math_load(text))
