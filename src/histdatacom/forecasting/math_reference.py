"""Bounded transparent mathematical references, not fitted forecasting models.

No production scoring helper is imported here. Missing numerical cells are
rejected; probabilities are never clipped and covariance is never regularized
implicitly. A different estimator or regularization policy needs a new formula.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass

MAX_MATH_ROWS = 4096
MAX_MATH_DRAWS = 512
MAX_COVARIANCE_DIMENSION = 16
MAX_HAC_LAGS = 128


def number(value: object) -> float:
    if type(value) not in (int, float):
        raise ValueError("reference requires finite numeric cells")
    try:
        if not isinstance(value, (int, float)):
            raise ValueError("reference requires finite numeric cells")
        result = float(value)
    except OverflowError as exc:
        raise ValueError("reference number is unrepresentable") from exc
    if not math.isfinite(result):
        raise ValueError("reference requires finite numeric cells")
    return result


def vector(
    values: Sequence[float], maximum: int = MAX_MATH_ROWS
) -> tuple[float, ...]:
    if type(values) not in (tuple, list) or not 1 <= len(values) <= maximum:
        raise ValueError("reference requires a bounded nonempty sequence")
    return tuple(number(value) for value in values)


def finite_sum(values: Sequence[float]) -> float:
    try:
        return number(math.fsum(values))
    except OverflowError as exc:
        raise ValueError("reference arithmetic is unrepresentable") from exc


def average(values: Sequence[float]) -> float:
    data = vector(values)
    return finite_sum(tuple(value / len(data) for value in data))


def median_value(values: Sequence[float]) -> float:
    data = sorted(vector(values))
    middle = len(data) // 2
    return (
        data[middle]
        if len(data) % 2
        else data[middle - 1] / 2 + data[middle] / 2
    )


def covariance_weights(
    covariance: Sequence[Sequence[float]],
) -> tuple[tuple[float, ...], float]:
    """Solve Sigma*x=1 by scaled Cholesky; return x/sum(x), 1/sum(x).

    Strictly symmetric positive-definite input only. A normalized Cholesky
    pivot <= 1e-12 refuses numerical near-singularity; it does not select ridge
    strength, clip negative weights, or claim empirically unbiased errors.
    """
    if (
        type(covariance) not in (tuple, list)
        or not 1 <= len(covariance) <= MAX_COVARIANCE_DIMENSION
    ):
        raise ValueError("covariance dimension is outside bounds")
    size = len(covariance)
    matrix = tuple(vector(row, MAX_COVARIANCE_DIMENSION) for row in covariance)
    if any(len(row) != size for row in matrix):
        raise ValueError("covariance must be square")
    if any(
        matrix[i][j] != matrix[j][i] for i in range(size) for j in range(size)
    ):
        raise ValueError("covariance must be exactly symmetric")
    scale = max(abs(value) for row in matrix for value in row)
    if scale == 0:
        raise ValueError("singular covariance")
    lower = [[0.0] * size for _ in range(size)]
    for i in range(size):
        for j in range(i + 1):
            residual = matrix[i][j] / scale - finite_sum(
                tuple(lower[i][k] * lower[j][k] for k in range(j))
            )
            if i == j:
                if residual <= 1e-12:
                    raise ValueError(
                        "covariance is indefinite or numerically near-singular"
                    )
                lower[i][j] = math.sqrt(residual)
            else:
                lower[i][j] = number(residual / lower[j][j])
    forward = [0.0] * size
    for i in range(size):
        forward[i] = number(
            (
                1.0
                - finite_sum(tuple(lower[i][j] * forward[j] for j in range(i)))
            )
            / lower[i][i]
        )
    solution = [0.0] * size
    for i in range(size - 1, -1, -1):
        solution[i] = number(
            (
                forward[i]
                - finite_sum(
                    tuple(lower[j][i] * solution[j] for j in range(i + 1, size))
                )
            )
            / lower[i][i]
        )
    total = finite_sum(solution)
    if total <= 0:
        raise ValueError("covariance solution has no positive normalization")
    variance = number(scale / total)
    if variance <= 0:
        raise ValueError("positive covariance variance underflowed")
    return tuple(number(value / total) for value in solution), variance


def quadratic_variance(
    covariance: Sequence[Sequence[float]], weights: Sequence[float]
) -> float:
    w = vector(weights, MAX_COVARIANCE_DIMENSION)
    if type(covariance) not in (tuple, list) or len(covariance) != len(w):
        raise ValueError("quadratic variance dimensions differ")
    matrix = tuple(vector(row, MAX_COVARIANCE_DIMENSION) for row in covariance)
    if any(len(row) != len(w) for row in matrix):
        raise ValueError("quadratic variance dimensions differ")
    return finite_sum(
        tuple(
            number(w[i] * matrix[i][j] * w[j])
            for i in range(len(w))
            for j in range(len(w))
        )
    )


@dataclass(frozen=True, slots=True)
class EffectiveDiversityV1:
    relative_variance: float
    effective_count: float | None


def equicorrelation_diversity(
    count: int, correlation: float
) -> EffectiveDiversityV1:
    """None effective count denotes exact zero variance/infinite diversity.

    This identity assumes equal variance/equal weights/equicorrelation. Negative
    admissible correlation may give n_eff > n; no empirical independence claim.
    """
    if type(count) is not int or not 1 <= count <= 1_000_000:
        raise ValueError("equicorrelation count is outside bounds")
    rho = number(correlation)
    if count == 1:
        if rho != 0:
            raise ValueError("singleton has no pairwise correlation")
        return EffectiveDiversityV1(1.0, 1.0)
    if not -1 / (count - 1) <= rho <= 1:
        raise ValueError("equicorrelation matrix is not positive semidefinite")
    numerator = number(1 + (count - 1) * rho)
    return EffectiveDiversityV1(
        numerator / count, None if numerator == 0 else number(count / numerator)
    )


def empirical_crps(draws: Sequence[float], observed: float) -> float:
    """Transparent O(M²) empirical CRPS, with a hard 512-draw work bound."""
    data = vector(draws, MAX_MATH_DRAWS)
    actual = number(observed)
    first = average(tuple(number(abs(value - actual)) for value in data))
    second = finite_sum(
        tuple(
            number(abs(left - right)) / (2 * len(data) ** 2)
            for left in data
            for right in data
        )
    )
    result = number(first - second)
    if result < -1e-12 * max(1.0, first, second):
        raise ValueError("CRPS is numerically negative")
    return max(0.0, result)


def probability_support(
    support: Sequence[tuple[float, float]],
) -> tuple[tuple[float, float], ...]:
    if (
        type(support) not in (tuple, list)
        or not 1 <= len(support) <= MAX_MATH_DRAWS
    ):
        raise ValueError("probability support exceeds work bounds")
    if any(
        type(pair) not in (tuple, list) or len(pair) != 2 for pair in support
    ):
        raise ValueError("probability support needs exact pairs")
    points = tuple((number(value), number(mass)) for value, mass in support)
    if any(not 0 < mass <= 1 for _, mass in points) or not math.isclose(
        finite_sum(tuple(mass for _, mass in points)),
        1.0,
        rel_tol=0,
        abs_tol=1e-12,
    ):
        raise ValueError("probability masses must be positive and sum to one")
    if any(left[0] >= right[0] for left, right in zip(points, points[1:])):
        raise ValueError("probability support must increase strictly")
    return points


def weighted_crps(
    support: Sequence[tuple[float, float]], observed: float
) -> float:
    """Independent CDF-integral construction, including tails to the outcome."""
    points = probability_support(support)
    actual = number(observed)
    knots = sorted({actual, *(value for value, _ in points)})
    pieces = []
    for left, right in zip(knots, knots[1:]):
        cdf = finite_sum(tuple(mass for value, mass in points if value <= left))
        error = cdf - float(actual <= left)
        pieces.append(number(number(right - left) * error * error))
    return finite_sum(pieces)


def pinball_loss(quantile: float, observed: float, probability: float) -> float:
    tau = number(probability)
    if not 0 < tau < 1:
        raise ValueError("quantile probability must be strictly inside (0,1)")
    q, y = number(quantile), number(observed)
    return number((tau - float(y < q)) * number(y - q))


def discrete_quantile(
    support: Sequence[tuple[float, float]], probability: float
) -> float:
    points = probability_support(support)
    tau = number(probability)
    if not 0 < tau < 1:
        raise ValueError("quantile probability must be strictly inside (0,1)")
    cumulative = 0.0
    for value, mass in points:
        cumulative += mass
        if cumulative >= tau:
            return value
    # Input sum tolerance cannot silently invent a tail quantile.
    raise ValueError("quantile falls beyond retained probability mass")


def distribution_point(
    support: Sequence[tuple[float, float]], statistic: str
) -> float:
    """Derive from masses, never a serialized/caller-supplied point value."""
    points = probability_support(support)
    if statistic == "mean":
        return finite_sum(tuple(number(value * mass) for value, mass in points))
    if statistic == "lower-weighted-median":
        for index, (value, _) in enumerate(points):
            if (
                finite_sum(tuple(mass for _, mass in points[: index + 1]))
                >= 0.5
            ):
                return value
        raise ValueError("distribution has no lower median")
    raise ValueError("unknown point statistic")


def _binary(probability: float, observed: bool) -> float:
    p = number(probability)
    if type(observed) is not bool or not 0 <= p <= 1:
        raise ValueError(
            "binary scores require probability and boolean outcome"
        )
    return p


def brier_score(probability: float, observed: bool) -> float:
    return (_binary(probability, observed) - int(observed)) ** 2


@dataclass(frozen=True, slots=True)
class BinaryLogScoreV1:
    loss: float | None
    impossible_event: bool


def binary_log_score(probability: float, observed: bool) -> BinaryLogScoreV1:
    """Represent +infinity explicitly; never clip an impossible prediction."""
    p = _binary(probability, observed)
    if (observed and p == 0) or (not observed and p == 1):
        return BinaryLogScoreV1(None, True)
    return BinaryLogScoreV1(
        number(-math.log(p) if observed else -math.log1p(-p)), False
    )


def reliability_bins(
    probabilities: Sequence[float],
    outcomes: Sequence[bool],
    edges: Sequence[float],
) -> tuple[tuple[int, float | None, float | None], ...]:
    """Fixed [left,right) bins (last includes 1); empty cells are unavailable."""
    values = vector(probabilities)
    if type(outcomes) not in (tuple, list) or len(outcomes) != len(values):
        raise ValueError("reliability cells must be paired")
    bins = vector(edges, 65)
    if (
        bins[0] != 0
        or bins[-1] != 1
        or any(a >= b for a, b in zip(bins, bins[1:]))
    ):
        raise ValueError("reliability edges must partition [0,1]")
    for p, y in zip(values, outcomes):
        _binary(p, y)
    result = []
    for index, (left, right) in enumerate(zip(bins, bins[1:])):
        selected = tuple(
            i
            for i, p in enumerate(values)
            if left <= p < right or (index == len(bins) - 2 and p == 1)
        )
        result.append(
            (
                len(selected),
                (
                    average(tuple(values[i] for i in selected))
                    if selected
                    else None
                ),
                (
                    average(tuple(float(outcomes[i]) for i in selected))
                    if selected
                    else None
                ),
            )
        )
    return tuple(result)


def point_losses(
    prediction: float,
    observed: float,
    *,
    consensus: float | None = None,
    scale: float | None = None,
) -> dict[str, float | None]:
    point, actual = number(prediction), number(observed)
    error = number(point - actual)
    result: dict[str, float | None] = {
        "forecast_minus_observed": error,
        "absolute_error": abs(error),
        "squared_error": number(error * error),
        "normalized_absolute_error": None,
        "normalized_squared_error": None,
        "independent_value_added": None,
    }
    if consensus is not None:
        result["independent_value_added"] = number(
            abs(actual - number(consensus)) - abs(error)
        )
    if scale is not None:
        divisor = number(scale)
        if divisor <= 0:
            raise ValueError("explicit normalization must be positive")
        normalized = number(abs(error) / divisor)
        result["normalized_absolute_error"] = normalized
        result["normalized_squared_error"] = number(normalized * normalized)
    return result


def surprise_values(
    actual_prediction: float,
    consensus_prediction: float,
    actual: float,
    observed_consensus: float,
) -> tuple[float, float, float]:
    a, c, y, f = map(
        number,
        (actual_prediction, consensus_prediction, actual, observed_consensus),
    )
    return number(a - c), number(y - f), number(abs(y - f) - abs(y - a))


def directional_mapping(value: float, polarity: int = 1) -> tuple[int, int]:
    """Raw sign and declared orientation, not economic desirability/profit."""
    number_value = number(value)
    if type(polarity) is not int or polarity not in (-1, 1):
        raise ValueError("direction polarity must be exactly -1 or 1")
    raw = int(number_value > 0) - int(number_value < 0)
    return raw, raw * polarity


@dataclass(frozen=True, slots=True)
class HacResultV1:
    count: int
    mean_difference: float
    long_run_variance: float
    standard_error: float
    statistic: float | None
    p_value: float | None
    lower_95: float | None
    upper_95: float | None
    status: str


def paired_hac(
    differences: Sequence[float],
    *,
    lags: int,
    horizon_steps: int = 1,
    minimum_count: int = 20,
) -> HacResultV1:
    """Bartlett/Newey-West variance of one homogeneous ordered loss sequence.

    gamma(k)=sum_t((d_t-dbar)(d_(t-k)-dbar))/n. Apply n/(n-1)
    finite-sample correction, then SE=sqrt(LRV/n). Normal 95% interval/p-value
    are asymptotic approximations, NOT a universal DM finite-sample test.
    Explicit event-index lags assume weak stationarity; no automatic lag fit.
    """
    data = vector(differences)
    n = len(data)
    if (
        type(minimum_count) is not int
        or not 8 <= minimum_count <= MAX_MATH_ROWS
        or n < minimum_count
    ):
        raise ValueError("insufficient declared support for HAC inference")
    if (
        type(horizon_steps) is not int
        or not 1 <= horizon_steps <= MAX_HAC_LAGS + 1
    ):
        raise ValueError("HAC horizon steps are outside bounds")
    if type(lags) is not int or not horizon_steps - 1 <= lags <= min(
        MAX_HAC_LAGS, n - 2
    ):
        raise ValueError("HAC lags cannot support the declared horizon")
    center = average(data)
    residuals = tuple(number(value - center) for value in data)
    gamma = tuple(
        finite_sum(
            tuple(
                number(residuals[t] * residuals[t - k]) / n for t in range(k, n)
            )
        )
        for k in range(lags + 1)
    )
    variance = number(
        finite_sum(
            (
                gamma[0],
                *(
                    2 * (1 - k / (lags + 1)) * gamma[k]
                    for k in range(1, lags + 1)
                ),
            )
        )
        * n
        / (n - 1)
    )
    if variance < 0:
        raise ValueError("HAC long-run variance is numerically negative")
    se = math.sqrt(variance / n)
    if se == 0:
        return HacResultV1(
            n,
            center,
            variance,
            se,
            None,
            None,
            None,
            None,
            "zero_long_run_variance_no_inference",
        )
    statistic = number(center / se)
    half = number(1.959963984540054 * se)
    return HacResultV1(
        n,
        center,
        variance,
        se,
        statistic,
        math.erfc(abs(statistic) / math.sqrt(2)),
        number(center - half),
        number(center + half),
        "normal_approximation_not_empirical_qualification",
    )
