"""Bounded reference preprocessing, NOT evidence or fit-membership authority.

All families fit only the supplied rows/masses. Application never refits. Numeric
inputs use exact binary64 ratios (not decimal display text); moments are weighted
population moments. Zero-mass rows do not fit anything. Missing numeric values
stay missing except for median imputation; an all-missing imputer stays missing.
Zero fitted scales map supported values to zero, explicitly recorded as degenerate.

Closed options (omitted keys use these defaults; unknown keys always refuse):
* zscale, minmax, median_imputer, quantile_rank: {}. Rank is the fitted weighted
  CDF P(X <= x), including ties; outside support gives 0/1. Quantiles elsewhere
  use the first positive-mass value reaching p, including p=0 at the minimum.
* robust_scale: mad_scale="1" (no implied Gaussian calibration).
* winsorize: lower="1/20", upper="19/20".
* variance_selector: threshold="0"; retain strictly greater population variance.
* correlation_pruner: threshold="19/20", variance_threshold="0". Preserve input
  column priority, prune abs(pairwise-complete weighted correlation) >= threshold;
  an unsupported/constant pair does not establish correlation.
* categorical: ontology_id REQUIRED. Positive-mass string vocabulary is sorted;
  each input has separate unknown and missing one-hot slots. Bools are not labels.
* pca, svd, whitening, population_reduction, linear_embedding: n_components=1,
  eigen_tolerance="1/1000000000000". Fit complete positive-mass rows. SVD uses
  uncentered second moments; the others use centered covariance. Population and
  embedding variants are transparent PCA projections, not special latent models.
  Whitening divides by sqrt(eigenvalue). Retained zero/tied eigenvalues refuse;
  eigenvector sign uses the first largest-magnitude loading. Numerical runtime
  identity is retained, not an assertion of cross-environment identical results.

Rational options are canonical integer or n/d strings. Masks, causal selection,
ontology authority, actual evidence weights, split membership and replay belong
to the caller. A reconstructed fit is a mathematical object, never a proof.
"""

from __future__ import annotations

import hashlib
import json
import math
import platform
import re
import sys
from dataclasses import dataclass
from fractions import Fraction
from typing import Any, TypeAlias

Scalar: TypeAlias = int | float | Fraction | str | None
MAX_ROWS = 512
MAX_COLUMNS = 64
MAX_CELLS = 32_768
MAX_OUTPUT_COLUMNS = 256
MAX_RATIONAL_BITS = 8192
MAX_JSON_BYTES = 8 * 1024 * 1024
MAX_PROJECTION_COLUMNS = 16
MAX_PROJECTION_CELLS = 8192
MAX_CATEGORIES = 128
VERSION = "1.0.0"
PROJECTIONS = (
    "pca",
    "svd",
    "whitening",
    "population_reduction",
    "linear_embedding",
)
FAMILIES = (
    "zscale",
    "robust_scale",
    "minmax",
    "winsorize",
    "quantile_rank",
    "categorical",
    "median_imputer",
    "variance_selector",
    "correlation_pruner",
    *PROJECTIONS,
)
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_RATIONAL = re.compile(r"(?:0|-?[1-9][0-9]*)(?:/[1-9][0-9]*)?\Z")


def _fraction(value: Fraction) -> Fraction:
    if (
        type(value) is not Fraction
        or max(value.numerator.bit_length(), value.denominator.bit_length())
        > MAX_RATIONAL_BITS
    ):
        raise ValueError("bounded exact rational required")
    return value


def _number(value: Scalar) -> Fraction:
    if type(value) is Fraction:
        return _fraction(value)
    if type(value) is int and value.bit_length() <= MAX_RATIONAL_BITS:
        return Fraction(value)
    if type(value) is float and math.isfinite(value):
        return _fraction(Fraction.from_float(value))
    raise ValueError(
        "finite exact numeric scalar required; bool is not numeric"
    )


def _sum(values: Any) -> Fraction:
    result = Fraction(0)
    for value in values:
        result = _fraction(result + _fraction(value))
    return result


def _ratio_text(value: Fraction) -> str:
    _fraction(value)
    return str(value)


def _ratio(value: Any) -> Fraction:
    if (
        type(value) is not str
        or len(value) > 5000
        or _RATIONAL.fullmatch(value) is None
    ):
        raise ValueError("canonical bounded rational string required")
    result = _fraction(Fraction(value))
    if str(result) != value:
        raise ValueError("rational string is not reduced/canonical")
    return result


def _text(value: Any) -> str:
    if (
        type(value) is not str
        or not 0 < len(value) <= 256
        or value.strip() != value
        or any(
            ord(char) < 32 or 0xD800 <= ord(char) <= 0xDFFF for char in value
        )
    ):
        raise ValueError("bounded nonempty label required")
    return value


def _json_bound(value: Any) -> None:
    """Charge exact ASCII JSON string escapes before constructing wire bytes."""
    pending = [(value, 0)]
    size = nodes = 0
    while pending:
        item, depth = pending.pop()
        nodes += 1
        if depth > 20 or nodes > 262_144:
            raise ValueError("reference JSON tree bound exceeded")
        if type(item) is str:
            size += 2
            for char in item:
                code = ord(char)
                size += (
                    12
                    if code > 0xFFFF
                    else (
                        6
                        if code >= 127 or code < 32 and char not in "\b\f\n\r\t"
                        else 2 if char in '\\"\b\f\n\r\t' else 1
                    )
                )
                if size > MAX_JSON_BYTES:
                    raise ValueError("reference JSON byte bound exceeded")
        elif type(item) is dict:
            if len(item) > MAX_CELLS or any(type(k) is not str for k in item):
                raise ValueError("reference JSON object bound/type")
            size += 2 + max(0, len(item) - 1) + len(item)
            pending.extend(
                (v, depth + 1) for pair in item.items() for v in pair
            )
        elif type(item) is list:
            if len(item) > MAX_CELLS:
                raise ValueError("reference JSON array bound")
            size += 2 + max(0, len(item) - 1)
            pending.extend((v, depth + 1) for v in item)
        elif type(item) is int and abs(item) <= 2**63 - 1:
            size += len(str(item))
        elif item is None:
            size += 4
        else:
            raise ValueError(
                "reference JSON permits only closed primitive types"
            )
        if size > MAX_JSON_BYTES:
            raise ValueError("reference JSON byte bound exceeded")


def _wire(value: Any) -> str:
    _json_bound(value)
    result = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    if len(result) > MAX_JSON_BYTES:
        raise ValueError("reference JSON byte bound exceeded")
    return result


def _pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in values:
        if key in result:
            raise ValueError("duplicate reference JSON key")
        result[key] = value
    return result


def _load(value: Any, *, canonical: bool = True) -> dict[str, Any]:
    if type(value) is not str or len(value) > MAX_JSON_BYTES:
        raise ValueError("bounded reference JSON string required")
    # Refuse oversized/deep syntax before the recursive JSON decoder allocates.
    depth = 0
    quoted = escaped = False
    for char in value:
        if quoted:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                quoted = False
        elif char == '"':
            quoted = True
        elif char in "[{":
            depth += 1
            if depth > 20:
                raise ValueError("reference JSON depth bound exceeded")
        elif char in "]}":
            depth -= 1
    try:
        result = json.loads(value, object_pairs_hook=_pairs)
    except (ValueError, RecursionError) as error:
        raise ValueError("invalid reference JSON") from error
    if type(result) is not dict:
        raise ValueError("reference JSON object required")
    rendered = _wire(result)
    if canonical and rendered != value:
        raise ValueError("noncanonical reference JSON")
    return result


def _keys(value: Any, expected: set[str]) -> None:
    if type(value) is not dict or set(value) != expected:
        raise ValueError("reference fields differ from closed schema")


def _columns(columns: Any, maximum: int = MAX_COLUMNS) -> tuple[str, ...]:
    if type(columns) is not tuple or not 0 < len(columns) <= maximum:
        raise ValueError("bounded nonempty column tuple required")
    for column in columns:
        _text(column)
    if len(set(columns)) != len(columns):
        raise ValueError("duplicate columns")
    return columns


def _rows(
    rows: Any, width: int, *, categorical: bool
) -> tuple[tuple[Scalar, ...], ...]:
    if (
        type(rows) is not tuple
        or len(rows) > MAX_ROWS
        or len(rows) * width > MAX_CELLS
    ):
        raise ValueError("bounded row tuple required")
    for row in rows:
        if type(row) is not tuple or len(row) != width:
            raise ValueError("row width/type differs from columns")
    wire_size = 2
    for row in rows:
        wire_size += 3
        for value in row:
            if value is not None:
                admitted = (
                    _text(value) if categorical else _ratio_text(_number(value))
                )
                # A single bounded scalar is serialized, never the whole table.
                wire_size += len(json.dumps(admitted, ensure_ascii=True)) + 1
            else:
                wire_size += 5
            if wire_size > MAX_JSON_BYTES:
                raise ValueError("reference input wire bound exceeded")
    return rows


def _options(family: str, value: str) -> dict[str, Any]:
    if type(value) is not str or len(value) > 16_384:
        raise ValueError("transform options bound exceeded")
    options = _load(value, canonical=False)
    defaults: dict[str, Any] = {}
    if family == "robust_scale":
        defaults = {"mad_scale": "1"}
    elif family == "winsorize":
        defaults = {"lower": "1/20", "upper": "19/20"}
    elif family == "variance_selector":
        defaults = {"threshold": "0"}
    elif family == "correlation_pruner":
        defaults = {"threshold": "19/20", "variance_threshold": "0"}
    elif family == "categorical":
        _keys(options, {"ontology_id"})
        _text(options["ontology_id"])
        return options
    elif family in PROJECTIONS:
        defaults = {"n_components": 1, "eigen_tolerance": "1/1000000000000"}
    if not set(options) <= set(defaults):
        raise ValueError("unknown reference transform option")
    options = defaults | options
    for key, item in options.items():
        if key == "n_components":
            if type(item) is not int or not 1 <= item <= MAX_PROJECTION_COLUMNS:
                raise ValueError("invalid component count")
        else:
            number = _ratio(item)
            if number < 0:
                raise ValueError("negative transform option")
    if (
        family == "winsorize"
        and not 0 <= _ratio(options["lower"]) < _ratio(options["upper"]) <= 1
    ):
        raise ValueError("invalid winsorization probabilities")
    if (
        family == "correlation_pruner"
        and not 0 < _ratio(options["threshold"]) <= 1
    ):
        raise ValueError("invalid correlation threshold")
    if family == "robust_scale" and _ratio(options["mad_scale"]) <= 0:
        raise ValueError("MAD scale must be positive")
    if family in PROJECTIONS and not Fraction(1, 10**15) <= _ratio(
        options["eigen_tolerance"]
    ) <= Fraction(1, 100):
        raise ValueError("unsupported eigen tolerance")
    return options


def _quantile(
    values: list[tuple[Fraction, Fraction]], probability: Fraction
) -> Fraction:
    ordered = sorted((x, w) for x, w in values if w > 0)
    target = _fraction(probability * _sum(w for _, w in ordered))
    cumulative = Fraction(0)
    for value, mass in ordered:
        cumulative = _fraction(cumulative + mass)
        if cumulative >= target:
            return value
    raise ValueError("quantile has no positive support")


def _moments(
    values: list[tuple[Fraction, Fraction]],
) -> tuple[Fraction, Fraction]:
    total = _sum(w for _, w in values)
    mean = _fraction(_sum(_fraction(x * w) for x, w in values) / total)
    variance = _fraction(
        _sum(_fraction(w * _fraction((x - mean) ** 2)) for x, w in values)
        / total
    )
    return mean, variance


def _environment(numerical: bool) -> dict[str, Any]:
    result = {
        "algorithm": "reference-preprocessing-v1",
        "python": platform.python_version(),
        "implementation": sys.implementation.name,
        "machine": platform.machine(),
        "system": platform.system(),
        "numpy": "unused",
        "numpy_build_sha256": "unused",
    }
    if numerical:
        import numpy as np

        from io import StringIO
        from contextlib import redirect_stdout

        stream = StringIO()
        with redirect_stdout(stream):
            np.show_config()
        result["numpy"] = np.__version__
        result["numpy_build_sha256"] = hashlib.sha256(
            stream.getvalue().encode()
        ).hexdigest()
    return result


def _finite(value: Fraction | float) -> float:
    try:
        result = float(value)
    except (OverflowError, ValueError) as error:
        raise ValueError("numeric projection outside binary64 range") from error
    if not math.isfinite(result):
        raise ValueError("nonfinite numeric projection")
    return result


def _sqrt_positive(value: Fraction | float) -> float:
    projected = _finite(value)
    if projected <= 0:
        raise ValueError("positive numeric scale underflows binary64")
    return math.sqrt(projected)


def _float(value: Any) -> float:
    if type(value) is not str or len(value) > 64:
        raise ValueError("canonical finite hexadecimal float required")
    try:
        result = _finite(float.fromhex(value))
    except (ValueError, OverflowError) as error:
        raise ValueError(
            "canonical finite hexadecimal float required"
        ) from error
    if result.hex() != value:
        raise ValueError("noncanonical hexadecimal float")
    return result


def _statistics(
    rows: tuple[tuple[Scalar, ...], ...],
    weights: tuple[Fraction, ...],
    width: int,
    categorical: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    parameters: list[dict[str, Any]] = []
    support: list[dict[str, Any]] = []
    total = _sum(weights)
    for column in range(width):
        present = [
            (row[column], weight)
            for row, weight in zip(rows, weights)
            if weight > 0 and row[column] is not None
        ]
        mass = _sum(weight for _, weight in present)
        support.append(
            {
                "rows": len(rows),
                "positive_weight_rows": sum(w > 0 for w in weights),
                "supported_rows": len(present),
                "missing_rows": sum(
                    w > 0 and row[column] is None
                    for row, w in zip(rows, weights)
                ),
                "zero_weight_rows": sum(w == 0 for w in weights),
                "total_mass": _ratio_text(total),
                "supported_mass": _ratio_text(mass),
                "missing_mass": _ratio_text(_fraction(total - mass)),
            }
        )
        if categorical:
            categories = sorted({_text(value) for value, _ in present})
            if len(categories) > MAX_CATEGORIES:
                raise ValueError("category vocabulary bound exceeded")
            parameters.append({"categories": categories})
        elif not present:
            parameters.append(
                {
                    key: None
                    for key in (
                        "mean",
                        "variance",
                        "median",
                        "mad",
                        "minimum",
                        "maximum",
                    )
                }
            )
        else:
            numeric = [(_number(x), w) for x, w in present]
            mean, variance = _moments(numeric)
            median = _quantile(numeric, Fraction(1, 2))
            mad = _quantile(
                [(abs(x - median), w) for x, w in numeric], Fraction(1, 2)
            )
            parameters.append(
                dict(
                    zip(
                        (
                            "mean",
                            "variance",
                            "median",
                            "mad",
                            "minimum",
                            "maximum",
                        ),
                        map(
                            _ratio_text,
                            (
                                mean,
                                variance,
                                median,
                                mad,
                                min(x for x, _ in numeric),
                                max(x for x, _ in numeric),
                            ),
                        ),
                    )
                )
            )
    return parameters, support


def _projection(
    family: str,
    rows: tuple[tuple[Scalar, ...], ...],
    weights: tuple[Fraction, ...],
    width: int,
    options: dict[str, Any],
) -> dict[str, Any]:
    if (
        width > MAX_PROJECTION_COLUMNS
        or len(rows) * width > MAX_PROJECTION_CELLS
    ):
        raise ValueError("linear projection work bound exceeded")
    complete = [
        (tuple(_number(v) for v in row), w)
        for row, w in zip(rows, weights)
        if w > 0 and all(v is not None for v in row)
    ]
    if not complete:
        raise ValueError(
            "linear projection has no complete positive-mass support"
        )
    total = _sum(w for _, w in complete)
    means = tuple(
        (
            Fraction(0)
            if family == "svd"
            else _fraction(
                _sum(_fraction(row[j] * w) for row, w in complete) / total
            )
        )
        for j in range(width)
    )
    covariance = [
        [
            _fraction(
                _sum(
                    _fraction(
                        w * _fraction((row[i] - means[i]) * (row[j] - means[j]))
                    )
                    for row, w in complete
                )
                / total
            )
            for j in range(width)
        ]
        for i in range(width)
    ]
    import numpy as np

    array = np.array(
        [[_finite(value) for value in row] for row in covariance],
        dtype=np.float64,
    )
    eigenvalues, eigenvectors = np.linalg.eigh(array)
    order = np.argsort(eigenvalues)[::-1]
    eigenvalues, eigenvectors = eigenvalues[order], eigenvectors[:, order]
    if (
        not np.isfinite(eigenvalues).all()
        or not np.isfinite(eigenvectors).all()
    ):
        raise ValueError("nonfinite eigensystem")
    count = options["n_components"]
    tolerance = _finite(_ratio(options["eigen_tolerance"])) * max(
        abs(float(eigenvalues[0])), sys.float_info.min
    )
    if count > width or float(eigenvalues[count - 1]) <= tolerance:
        raise ValueError("insufficient nondegenerate projection rank")
    for j in range(count):
        if any(
            i != j and abs(float(eigenvalues[j] - eigenvalues[i])) <= tolerance
            for i in range(width)
        ):
            raise ValueError("tied eigenbasis is not uniquely identified")
    basis = []
    for j in range(count):
        vector = eigenvectors[:, j]
        pivot = int(np.argmax(np.abs(vector)))
        if vector[pivot] < 0:
            vector = -vector
        basis.append([float(value).hex() for value in vector])
    return {
        "center": [_ratio_text(value) for value in means],
        "basis": basis,
        "eigenvalues": [float(value).hex() for value in eigenvalues[:count]],
        "complete_rows": len(complete),
        "complete_mass": _ratio_text(total),
    }


def _fit_parameters(
    family: str,
    columns: tuple[str, ...],
    rows: tuple[tuple[Scalar, ...], ...],
    weights: tuple[Fraction, ...],
    options: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], tuple[str, ...]]:
    width = len(columns)
    stats, support = _statistics(rows, weights, width, family == "categorical")
    parameters: dict[str, Any] = {"columns": stats}
    outputs = columns
    if family in PROJECTIONS:
        parameters["projection"] = _projection(
            family, rows, weights, width, options
        )
        outputs = tuple(f"{family}:{i}" for i in range(options["n_components"]))
    elif family == "categorical":
        outputs = tuple(
            f"{j}:category:{i}"
            for j, item in enumerate(stats)
            for i in range(len(item["categories"]) + 2)
        )
        if len(outputs) > MAX_OUTPUT_COLUMNS:
            raise ValueError("encoded output column bound exceeded")
    elif family in ("winsorize", "quantile_rank"):
        for j, item in enumerate(stats):
            values = [
                (_number(row[j]), w)
                for row, w in zip(rows, weights)
                if w > 0 and row[j] is not None
            ]
            if family == "winsorize":
                item["lower"] = (
                    _ratio_text(_quantile(values, _ratio(options["lower"])))
                    if values
                    else None
                )
                item["upper"] = (
                    _ratio_text(_quantile(values, _ratio(options["upper"])))
                    if values
                    else None
                )
            else:
                masses: dict[Fraction, Fraction] = {}
                for value, mass in values:
                    masses[value] = _fraction(
                        masses.get(value, Fraction(0)) + mass
                    )
                item["cdf"] = [
                    [_ratio_text(x), _ratio_text(w)]
                    for x, w in sorted(masses.items())
                ]
    elif family in ("variance_selector", "correlation_pruner"):
        threshold = _ratio(
            options["variance_threshold"]
            if family == "correlation_pruner"
            else options["threshold"]
        )
        selected: list[int] = []
        pair_support = []
        for j, item in enumerate(stats):
            if (
                item["variance"] is None
                or _ratio(item["variance"]) <= threshold
            ):
                continue
            redundant = False
            if family == "correlation_pruner":
                for i in selected:
                    pairs = [
                        (_number(row[i]), _number(row[j]), w)
                        for row, w in zip(rows, weights)
                        if w > 0 and row[i] is not None and row[j] is not None
                    ]
                    correlation_squared = None
                    mass = _sum(w for _, _, w in pairs)
                    if pairs:
                        mi, vi = _moments([(x, w) for x, _, w in pairs])
                        mj, vj = _moments([(y, w) for _, y, w in pairs])
                        if vi > 0 and vj > 0:
                            cov = _fraction(
                                _sum(
                                    _fraction(
                                        w * _fraction((x - mi) * (y - mj))
                                    )
                                    for x, y, w in pairs
                                )
                                / mass
                            )
                            correlation_squared = _fraction(
                                _fraction(cov * cov) / _fraction(vi * vj)
                            )
                            if (
                                correlation_squared
                                >= _ratio(options["threshold"]) ** 2
                            ):
                                redundant = True
                    pair_support.append(
                        {
                            "left": i,
                            "right": j,
                            "rows": len(pairs),
                            "mass": _ratio_text(mass),
                            "correlation_squared": (
                                None
                                if correlation_squared is None
                                else _ratio_text(correlation_squared)
                            ),
                        }
                    )
            if not redundant:
                selected.append(j)
        parameters["selected"] = selected
        if family == "correlation_pruner":
            parameters["pair_support"] = pair_support
        outputs = tuple(columns[i] for i in selected)
    return parameters, {"columns": support}, outputs


@dataclass(frozen=True, slots=True)
class ReferenceTransformFitV1:
    """Frozen pure fit; reconstruction validates shape, NOT training authority."""

    family: str
    columns: tuple[str, ...]
    output_columns: tuple[str, ...]
    options_json: str
    parameters_json: str
    support_json: str
    environment_json: str
    fit_input_sha256: str

    def __post_init__(self) -> None:
        _validate_fit(self)

    @property
    def input_columns(self) -> tuple[str, ...]:
        return self.columns

    def to_dict(self) -> dict[str, Any]:
        _validate_fit(self)
        return {
            "kind": "reference-transform-fit",
            "version": VERSION,
            "family": self.family,
            "columns": list(self.columns),
            "output_columns": list(self.output_columns),
            "options": _load(self.options_json),
            "parameters": _load(self.parameters_json),
            "support": _load(self.support_json),
            "environment": _load(self.environment_json),
            "fit_input_sha256": self.fit_input_sha256,
        }

    def to_json(self) -> str:
        return _wire(self.to_dict())

    @property
    def fit_id(self) -> str:
        return (
            "reference-transform-fit:sha256:"
            + hashlib.sha256(self.to_json().encode("ascii")).hexdigest()
        )

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ReferenceTransformFitV1:
        _json_bound(value)
        _keys(
            value,
            {
                "kind",
                "version",
                "family",
                "columns",
                "output_columns",
                "options",
                "parameters",
                "support",
                "environment",
                "fit_input_sha256",
            },
        )
        if (
            value["kind"] != "reference-transform-fit"
            or value["version"] != VERSION
            or type(value["columns"]) is not list
            or type(value["output_columns"]) is not list
        ):
            raise ValueError("unsupported reference fit envelope")
        return cls(
            value["family"],
            tuple(value["columns"]),
            tuple(value["output_columns"]),
            _wire(value["options"]),
            _wire(value["parameters"]),
            _wire(value["support"]),
            _wire(value["environment"]),
            value["fit_input_sha256"],
        )

    @classmethod
    def from_json(cls, value: str) -> ReferenceTransformFitV1:
        return cls.from_dict(_load(value))


def _validate_fit(fit: ReferenceTransformFitV1) -> None:
    if (
        type(fit) is not ReferenceTransformFitV1
        or type(fit.family) is not str
        or fit.family not in FAMILIES
    ):
        raise ValueError("exact supported reference fit required")
    wires = (
        fit.options_json,
        fit.parameters_json,
        fit.support_json,
        fit.environment_json,
    )
    if (
        any(type(value) is not str for value in wires)
        or sum(len(value) for value in wires) > MAX_JSON_BYTES
    ):
        raise ValueError("reference fit aggregate wire bound exceeded")
    _columns(fit.columns)
    if (
        type(fit.output_columns) is not tuple
        or len(fit.output_columns) > MAX_OUTPUT_COLUMNS
    ):
        raise ValueError("bounded output columns required")
    if fit.output_columns:
        _columns(fit.output_columns, MAX_OUTPUT_COLUMNS)
    if (
        type(fit.fit_input_sha256) is not str
        or _DIGEST.fullmatch(fit.fit_input_sha256) is None
    ):
        raise ValueError("invalid fit-input digest")
    options = _load(fit.options_json)
    if _options(fit.family, fit.options_json) != options:
        raise ValueError("fit must retain expanded options")
    parameters = _load(fit.parameters_json)
    extra = (
        {"projection"}
        if fit.family in PROJECTIONS
        else (
            {"selected", "pair_support"}
            if fit.family == "correlation_pruner"
            else {"selected"} if fit.family == "variance_selector" else set()
        )
    )
    _keys(parameters, {"columns"} | extra)
    stats = parameters["columns"]
    support = _load(fit.support_json)
    _keys(support, {"columns"})
    if (
        type(stats) is not list
        or type(support["columns"]) is not list
        or len(stats) != len(fit.columns)
        or len(support["columns"]) != len(stats)
    ):
        raise ValueError("fit column/support shape differs")
    for item, counts in zip(stats, support["columns"]):
        _keys(
            counts,
            {
                "rows",
                "positive_weight_rows",
                "supported_rows",
                "missing_rows",
                "zero_weight_rows",
                "total_mass",
                "supported_mass",
                "missing_mass",
            },
        )
        for key in (
            "rows",
            "positive_weight_rows",
            "supported_rows",
            "missing_rows",
            "zero_weight_rows",
        ):
            if type(counts[key]) is not int or not 0 <= counts[key] <= MAX_ROWS:
                raise ValueError("invalid fit-support count")
        total, present, missing = (
            _ratio(counts[key])
            for key in ("total_mass", "supported_mass", "missing_mass")
        )
        if (
            total <= 0
            or min(present, missing) < 0
            or present + missing != total
            or counts["rows"]
            != counts["positive_weight_rows"] + counts["zero_weight_rows"]
            or counts["positive_weight_rows"]
            != counts["supported_rows"] + counts["missing_rows"]
            or (present == 0) != (counts["supported_rows"] == 0)
            or (missing == 0) != (counts["missing_rows"] == 0)
        ):
            raise ValueError("fit-support accounting differs")
        for key in (
            "rows",
            "positive_weight_rows",
            "zero_weight_rows",
            "total_mass",
        ):
            if counts[key] != support["columns"][0][key]:
                raise ValueError("fit-support global totals differ by column")
        if fit.family == "categorical":
            _keys(item, {"categories"})
            categories = item["categories"]
            if type(categories) is not list or len(categories) > MAX_CATEGORIES:
                raise ValueError("invalid category inventory")
            for category in categories:
                _text(category)
            if categories != sorted(set(categories)) or bool(
                categories
            ) != bool(present):
                raise ValueError("category inventory/support differs")
            continue
        keys = {"mean", "variance", "median", "mad", "minimum", "maximum"}
        if fit.family == "winsorize":
            keys |= {"lower", "upper"}
        if fit.family == "quantile_rank":
            keys |= {"cdf"}
        _keys(item, keys)
        for key in keys - {"cdf"}:
            if present == 0:
                if item[key] is not None:
                    raise ValueError("unsupported statistic is not missing")
            else:
                _ratio(item[key])
        if present > 0 and (
            _ratio(item["variance"]) < 0
            or _ratio(item["mad"]) < 0
            or not _ratio(item["minimum"])
            <= _ratio(item["mean"])
            <= _ratio(item["maximum"])
            or not _ratio(item["minimum"])
            <= _ratio(item["median"])
            <= _ratio(item["maximum"])
        ):
            raise ValueError("invalid scalar statistics")
        if (
            fit.family == "winsorize"
            and present > 0
            and not _ratio(item["minimum"])
            <= _ratio(item["lower"])
            <= _ratio(item["upper"])
            <= _ratio(item["maximum"])
        ):
            raise ValueError("invalid learned clipping thresholds")
        if fit.family == "quantile_rank":
            cdf = item["cdf"]
            if type(cdf) is not list or len(cdf) > MAX_ROWS:
                raise ValueError("invalid CDF support")
            previous = None
            mass = Fraction(0)
            for pair in cdf:
                if type(pair) is not list or len(pair) != 2:
                    raise ValueError("invalid CDF row")
                x, w = map(_ratio, pair)
                if w <= 0 or previous is not None and x <= previous:
                    raise ValueError("invalid CDF ordering/mass")
                previous, mass = x, _fraction(mass + w)
            if mass != present:
                raise ValueError("CDF mass/support differs")
    environment = _load(fit.environment_json)
    _keys(
        environment,
        {
            "algorithm",
            "python",
            "implementation",
            "machine",
            "system",
            "numpy",
            "numpy_build_sha256",
        },
    )
    for value in environment.values():
        _text(value)
    if environment["algorithm"] != "reference-preprocessing-v1":
        raise ValueError("unknown numerical algorithm")
    expected = fit.columns
    if fit.family in PROJECTIONS:
        projection = parameters["projection"]
        _keys(
            projection,
            {
                "center",
                "basis",
                "eigenvalues",
                "complete_rows",
                "complete_mass",
            },
        )
        count = options["n_components"]
        if (
            len(fit.columns) > MAX_PROJECTION_COLUMNS
            or type(projection["complete_rows"]) is not int
            or not 0 < projection["complete_rows"] <= MAX_ROWS
            or _ratio(projection["complete_mass"]) <= 0
        ):
            raise ValueError("invalid complete-case projection support")
        for key, length in (
            ("center", len(fit.columns)),
            ("basis", count),
            ("eigenvalues", count),
        ):
            if (
                type(projection[key]) is not list
                or len(projection[key]) != length
            ):
                raise ValueError("invalid projection dimensions")
        for value in projection["center"]:
            _ratio(value)
        for row in projection["basis"]:
            if type(row) is not list or len(row) != len(fit.columns):
                raise ValueError("invalid basis row")
            values = [_float(value) for value in row]
            if any(abs(value) > 1 for value in values):
                raise ValueError("invalid eigenvector loading")
        if any(_float(value) <= 0 for value in projection["eigenvalues"]):
            raise ValueError("invalid eigenvalue")
        if (
            environment["numpy"] == "unused"
            or _DIGEST.fullmatch(environment["numpy_build_sha256"]) is None
        ):
            raise ValueError("projection numerical environment absent")
        expected = tuple(f"{fit.family}:{i}" for i in range(count))
    elif fit.family == "categorical":
        expected = tuple(
            f"{j}:category:{i}"
            for j, item in enumerate(stats)
            for i in range(len(item["categories"]) + 2)
        )
    elif "selected" in parameters:
        selected = parameters["selected"]
        if (
            type(selected) is not list
            or any(
                type(i) is not int or not 0 <= i < len(stats) for i in selected
            )
            or selected != sorted(set(selected))
        ):
            raise ValueError("invalid selected columns")
        expected = tuple(fit.columns[i] for i in selected)
        if "pair_support" in parameters:
            pairs = parameters["pair_support"]
            if type(pairs) is not list or len(pairs) > MAX_COLUMNS**2:
                raise ValueError("invalid pairwise support")
            for pair in pairs:
                _keys(
                    pair,
                    {"left", "right", "rows", "mass", "correlation_squared"},
                )
                if (
                    any(
                        type(pair[k]) is not int
                        for k in ("left", "right", "rows")
                    )
                    or not 0 <= pair["left"] < pair["right"] < len(stats)
                    or not 0 <= pair["rows"] <= MAX_ROWS
                    or _ratio(pair["mass"]) < 0
                ):
                    raise ValueError("invalid pairwise support entry")
                if (
                    pair["correlation_squared"] is not None
                    and not 0 <= _ratio(pair["correlation_squared"]) <= 1
                ):
                    raise ValueError("invalid squared correlation")
    if fit.output_columns != expected:
        raise ValueError("output columns differ from fitted operation")


def fit_reference_transform(
    family: str,
    columns: tuple[str, ...],
    rows: tuple[tuple[Scalar, ...], ...],
    weights: tuple[Fraction, ...],
    options_json: str = "{}",
) -> ReferenceTransformFitV1:
    """Fit passed data only. Selection/weights are not independently proven."""
    if type(family) is not str or family not in FAMILIES:
        raise ValueError("unsupported reference transform family")
    _columns(columns)
    options = _options(family, options_json)
    _rows(rows, len(columns), categorical=family == "categorical")
    if type(weights) is not tuple or len(weights) != len(rows) or not rows:
        raise ValueError("nonempty row-aligned exact weights required")
    for weight in weights:
        if _fraction(weight) < 0:
            raise ValueError("negative fit mass")
    if _sum(weights) <= 0:
        raise ValueError("fit has no positive mass")
    if family in PROJECTIONS and (
        len(columns) > MAX_PROJECTION_COLUMNS
        or len(rows) * len(columns) > MAX_PROJECTION_CELLS
    ):
        raise ValueError("linear projection work bound exceeded")
    inputs = {
        "columns": list(columns),
        "rows": [
            [
                (
                    value
                    if value is None or family == "categorical"
                    else _ratio_text(_number(value))
                )
                for value in row
            ]
            for row in rows
        ],
        "weights": [_ratio_text(w) for w in weights],
    }
    input_digest = hashlib.sha256(_wire(inputs).encode("ascii")).hexdigest()
    parameters, support, outputs = _fit_parameters(
        family, columns, rows, weights, options
    )
    return ReferenceTransformFitV1(
        family,
        columns,
        outputs,
        _wire(options),
        _wire(parameters),
        _wire(support),
        _wire(_environment(family in PROJECTIONS)),
        input_digest,
    )


def apply_reference_transform(
    fit: ReferenceTransformFitV1, rows: tuple[tuple[Scalar, ...], ...]
) -> tuple[tuple[Scalar, ...], ...]:
    """Use frozen parameters only; unknowns/missingness cannot update the fit."""
    _validate_fit(fit)
    _rows(rows, len(fit.columns), categorical=fit.family == "categorical")
    if len(rows) * len(fit.output_columns) > MAX_CELLS:
        raise ValueError("transformed output cell bound exceeded")
    parameters = _load(fit.parameters_json)
    options = _load(fit.options_json)
    result: list[tuple[Scalar, ...]] = []
    for row in rows:
        if fit.family in PROJECTIONS:
            projection = parameters["projection"]
            if any(value is None for value in row):
                result.append(tuple(None for _ in fit.output_columns))
                continue
            centered = tuple(
                _fraction(_number(value) - _ratio(center))
                for value, center in zip(row, projection["center"])
            )
            projected = []
            for vector, eigenvalue in zip(
                projection["basis"], projection["eigenvalues"]
            ):
                value = _finite(
                    _sum(
                        _fraction(x * Fraction.from_float(_float(coefficient)))
                        for x, coefficient in zip(centered, vector)
                    )
                )
                if fit.family == "whitening":
                    value = _finite(value / _sqrt_positive(_float(eigenvalue)))
                projected.append(value)
            result.append(tuple(projected))
            continue
        if "selected" in parameters:
            result.append(
                tuple(
                    None if row[j] is None else _number(row[j])
                    for j in parameters["selected"]
                )
            )
            continue
        values: list[Scalar] = []
        for original, item in zip(row, parameters["columns"]):
            if fit.family == "categorical":
                categories = item["categories"]
                index = (
                    len(categories) + 1
                    if original is None
                    else (
                        categories.index(original)
                        if original in categories
                        else len(categories)
                    )
                )
                values.extend(
                    1 if i == index else 0 for i in range(len(categories) + 2)
                )
                continue
            if fit.family == "median_imputer":
                values.append(
                    _number(original)
                    if original is not None
                    else (
                        None
                        if item["median"] is None
                        else _ratio(item["median"])
                    )
                )
                continue
            if original is None or item["mean"] is None:
                values.append(None)
                continue
            x = _number(original)
            if fit.family == "zscale":
                variance = _ratio(item["variance"])
                values.append(
                    0.0
                    if variance == 0
                    else _finite(
                        _finite(_fraction(x - _ratio(item["mean"])))
                        / _sqrt_positive(variance)
                    )
                )
            elif fit.family == "robust_scale":
                scale = _fraction(
                    _ratio(item["mad"]) * _ratio(options["mad_scale"])
                )
                values.append(
                    Fraction(0)
                    if scale == 0
                    else _fraction((x - _ratio(item["median"])) / scale)
                )
            elif fit.family == "minmax":
                low, high = _ratio(item["minimum"]), _ratio(item["maximum"])
                values.append(
                    Fraction(0)
                    if high == low
                    else _fraction((x - low) / (high - low))
                )
            elif fit.family == "winsorize":
                values.append(
                    max(_ratio(item["lower"]), min(x, _ratio(item["upper"])))
                )
            elif fit.family == "quantile_rank":
                cdf = [(_ratio(a), _ratio(b)) for a, b in item["cdf"]]
                values.append(
                    _fraction(
                        _sum(w for value, w in cdf if value <= x)
                        / _sum(w for _, w in cdf)
                    )
                )
            else:
                raise ValueError("unsupported reference application")
        result.append(tuple(values))
    return tuple(result)
