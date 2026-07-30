"""Explicit ARCH/GARCH volatility diagnostics and training projections."""

from __future__ import annotations

import hashlib
import importlib
import importlib.metadata
import json
import math
import re
import time
import warnings
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from statistics import median
from typing import Any, cast

from histdatacom.data_quality.autoregressive import (
    _first_available_row_id,
    _int,
    _mapping,
    _mapping_rows,
    _optional_float,
    _optional_int,
    _rate,
    _rounded,
    _target_sort_key,
    _text,
    _warning_codes,
)
from histdatacom.data_quality.classical_model_contracts import (
    CLASSICAL_MODEL_INPUT_SCHEMA_VERSION,
    ClassicalModelInputProfile,
    ClassicalModelInputResult,
    build_classical_model_input,
)
from histdatacom.data_quality.contracts import QualityFinding
from histdatacom.data_quality.limits import bounded_report_limit
from histdatacom.data_quality.seasonal_exogenous import _model_references
from histdatacom.data_quality.training_features import (
    VOLATILITY_COLUMNS,
    ensure_tick_training_features,
)
from histdatacom.runtime_contracts import JSONValue

VOLATILITY_SCHEMA_VERSION = "histdatacom.volatility.v1"
VOLATILITY_CONFIGURATION_SCHEMA_VERSION = (
    "histdatacom.volatility-configuration.v1"
)
VOLATILITY_FIT_SCHEMA_VERSION = "histdatacom.volatility-fit-result.v1"
VOLATILITY_FORECAST_SCHEMA_VERSION = "histdatacom.volatility-forecast.v1"
VOLATILITY_EVALUATION_SCHEMA_VERSION = "histdatacom.volatility-evaluation.v1"
VOLATILITY_TRAINING_PROJECTION_SCHEMA_VERSION = (
    "histdatacom.volatility-training-projection.v1"
)
VOLATILITY_SUMMARY_SCHEMA_VERSION = "histdatacom.volatility-summary.v1"
VOLATILITY_SUMMARY_METADATA_KEY = "time_series_fingerprint_volatility_summary"
VOLATILITY_BOUNDED_PAYLOAD_KEY = "fingerprint_volatility"

DEFAULT_VOLATILITY_SUMMARY_TARGET_LIMIT = 16
DEFAULT_VOLATILITY_ROUNDING_DIGITS = 12
MAX_VOLATILITY_SPECIFICATIONS = 32
MAX_VOLATILITY_ORDER = 64
MAX_PARAMETER_BOUNDS = 64

VOLATILITY_FAMILIES = ("arch", "garch")
VOLATILITY_FAMILY_CODES = {"arch": 1, "garch": 2}
VOLATILITY_INPUT_DEFINITIONS = (
    "raw_return",
    "log_return",
    "demeaned_return",
    "mean_model_residual",
)
VOLATILITY_INPUT_DEFINITION_CODES = {
    name: index
    for index, name in enumerate(VOLATILITY_INPUT_DEFINITIONS, start=1)
}
VOLATILITY_MEAN_MODELS = ("zero", "constant")
VOLATILITY_MEAN_MODEL_CODES = {"zero": 1, "constant": 2}
VOLATILITY_DISTRIBUTIONS = ("normal", "students_t")
VOLATILITY_DISTRIBUTION_CODES = {"normal": 1, "students_t": 2}
VOLATILITY_INITIALIZATIONS = ("backend_default", "sample_variance", "fixed")
VOLATILITY_REALIZED_VARIANCE_PROXIES = ("squared_return",)
VOLATILITY_FIT_STATUS_CODES = {
    "unavailable": 1,
    "skipped": 2,
    "failed": 3,
    "limited": 4,
    "fitted": 5,
    "converged": 6,
}
VOLATILITY_REASON_CODES = {
    "": 0,
    "dependency_unavailable": 1,
    "input_contract_unavailable": 2,
    "insufficient_folds": 3,
    "insufficient_history": 4,
    "invalid_configuration": 5,
    "invalid_transform": 6,
    "residual_series_unavailable": 7,
    "residual_reference_mismatch": 8,
    "non_finite_input": 9,
    "zero_variance": 10,
    "scaling_failure": 11,
    "invalid_order": 12,
    "non_positive_variance": 13,
    "non_stationary_variance": 14,
    "parameter_bound_violation": 15,
    "optimizer_failure": 16,
    "singular_covariance": 17,
    "numerical_instability": 18,
    "resource_limit": 19,
    "timeout": 20,
    "backend_failure": 21,
    "target_unavailable": 22,
}

ASYMMETRIC_VOLATILITY_EXTENSION_REGISTRY: tuple[
    Mapping[str, JSONValue], ...
] = (
    {
        "family": "gjr_garch",
        "status": "registered_not_enabled",
        "backend_volatility": "GARCH",
        "asymmetric_order_parameter": "o",
    },
    {
        "family": "egarch",
        "status": "registered_not_enabled",
        "backend_volatility": "EGARCH",
    },
)

_SPECIFICATION_ID = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,63}$")
_PARAMETER_NAME = re.compile(r"^[A-Za-z0-9_.()\[\]-]{1,96}$")


@dataclass(frozen=True, slots=True)
class VolatilitySpecification:
    """One explicit symmetric ARCH(q) or GARCH(p,q) configuration."""

    specification_id: str
    family: str
    input_definition: str = "raw_return"
    mean_model: str = "zero"
    mean_model_reference_id: str = ""
    distribution: str = "normal"
    innovation_order: int = 1
    variance_order: int = 0
    scale_factor: float = 100.0
    variance_initialization: str = "backend_default"
    initial_variance: float | None = None
    covariance_type: str = "robust"
    optimizer_tolerance: float | None = None
    parameter_bounds: tuple[tuple[str, float | None, float | None], ...] = ()
    max_iterations: int = 200

    def __post_init__(self) -> None:
        if not _SPECIFICATION_ID.fullmatch(self.specification_id):
            raise ValueError("invalid volatility specification_id")
        if self.family not in VOLATILITY_FAMILIES:
            raise ValueError("unsupported volatility family")
        if self.input_definition not in VOLATILITY_INPUT_DEFINITIONS:
            raise ValueError("unsupported volatility input_definition")
        if self.mean_model not in VOLATILITY_MEAN_MODELS:
            raise ValueError("unsupported volatility mean_model")
        if self.input_definition == "mean_model_residual":
            if not self.mean_model_reference_id:
                raise ValueError("mean-model residuals require a reference ID")
            if self.mean_model != "zero":
                raise ValueError("residual inputs require a zero mean model")
        elif self.mean_model_reference_id:
            raise ValueError("mean_model_reference_id requires residual input")
        if (
            self.input_definition == "demeaned_return"
            and self.mean_model != "zero"
        ):
            raise ValueError("demeaned inputs require a zero mean model")
        if self.distribution not in VOLATILITY_DISTRIBUTIONS:
            raise ValueError("unsupported volatility distribution")
        if not 1 <= self.innovation_order <= MAX_VOLATILITY_ORDER:
            raise ValueError("innovation_order must be between 1 and 64")
        if self.family == "arch" and self.variance_order != 0:
            raise ValueError("ARCH requires variance_order=0")
        if (
            self.family == "garch"
            and not 1 <= self.variance_order <= MAX_VOLATILITY_ORDER
        ):
            raise ValueError("GARCH variance_order must be between 1 and 64")
        if not math.isfinite(self.scale_factor) or self.scale_factor <= 0:
            raise ValueError("scale_factor must be positive and finite")
        if self.variance_initialization not in VOLATILITY_INITIALIZATIONS:
            raise ValueError("unsupported variance_initialization")
        if self.variance_initialization == "fixed":
            if (
                self.initial_variance is None
                or not math.isfinite(self.initial_variance)
                or self.initial_variance <= 0
            ):
                raise ValueError(
                    "fixed initialization requires positive initial_variance"
                )
        elif self.initial_variance is not None:
            raise ValueError("initial_variance requires fixed initialization")
        if self.covariance_type not in {"robust", "classic"}:
            raise ValueError("unsupported covariance_type")
        if self.optimizer_tolerance is not None and (
            not math.isfinite(self.optimizer_tolerance)
            or self.optimizer_tolerance <= 0
        ):
            raise ValueError("optimizer_tolerance must be positive")
        if len(self.parameter_bounds) > MAX_PARAMETER_BOUNDS:
            raise ValueError("too many parameter bounds")
        seen: set[str] = set()
        for name, lower, upper in self.parameter_bounds:
            if not _PARAMETER_NAME.fullmatch(name) or name in seen:
                raise ValueError("parameter bounds require unique valid names")
            if lower is not None and not math.isfinite(lower):
                raise ValueError("parameter lower bounds must be finite")
            if upper is not None and not math.isfinite(upper):
                raise ValueError("parameter upper bounds must be finite")
            if lower is not None and upper is not None and lower > upper:
                raise ValueError("parameter lower bound exceeds upper bound")
            seen.add(name)
        if self.max_iterations < 1:
            raise ValueError("max_iterations must be positive")

    def to_metadata(self) -> dict[str, JSONValue]:
        return {
            "schema_version": VOLATILITY_CONFIGURATION_SCHEMA_VERSION,
            "specification_id": self.specification_id,
            "family": self.family,
            "family_code": VOLATILITY_FAMILY_CODES[self.family],
            "input_definition": self.input_definition,
            "input_definition_code": VOLATILITY_INPUT_DEFINITION_CODES[
                self.input_definition
            ],
            "mean_model": self.mean_model,
            "mean_model_code": VOLATILITY_MEAN_MODEL_CODES[self.mean_model],
            "mean_model_reference_id": self.mean_model_reference_id or None,
            "distribution": self.distribution,
            "distribution_code": VOLATILITY_DISTRIBUTION_CODES[
                self.distribution
            ],
            "innovation_order": self.innovation_order,
            "variance_order": self.variance_order,
            "scale_factor": self.scale_factor,
            "variance_initialization": self.variance_initialization,
            "initial_variance": self.initial_variance,
            "covariance_type": self.covariance_type,
            "optimizer_tolerance": self.optimizer_tolerance,
            "parameter_bounds": cast(
                JSONValue,
                [
                    {"parameter": name, "lower": lower, "upper": upper}
                    for name, lower, upper in self.parameter_bounds
                ],
            ),
            "max_iterations": self.max_iterations,
            "symmetric": True,
            "asymmetric_order": 0,
            "power": 2.0,
            "automatic_order_selection": False,
        }


def _default_specifications() -> tuple[VolatilitySpecification, ...]:
    return (
        VolatilitySpecification("arch-5", "arch", innovation_order=5),
        VolatilitySpecification(
            "garch-1-1", "garch", mean_model="constant", variance_order=1
        ),
    )


@dataclass(frozen=True, slots=True)
class VolatilityProfile:
    """Explicit volatility controls; disabled by default."""

    enabled: bool = False
    specifications: tuple[VolatilitySpecification, ...] = field(
        default_factory=_default_specifications
    )
    projection_specification_ids: tuple[str, ...] = ("arch-5", "garch-1-1")
    projection_horizon: int = 1
    realized_variance_proxy: str = "squared_return"
    annualization_periods: int = 0
    baseline_rolling_windows: tuple[int, ...] = (5, 20)
    ewma_decay: float = 0.94
    maximum_persistence: float = 0.999999
    maximum_covariance_condition_number: float = 1e30
    boundary_tolerance: float = 1e-6
    compare_exponential_smoothing: bool = True
    compare_autoregressive: bool = True
    compare_seasonal_exogenous: bool = True
    compare_state_space: bool = True
    rounding_digits: int = DEFAULT_VOLATILITY_ROUNDING_DIGITS

    def __post_init__(self) -> None:
        if (
            not self.specifications
            or len(self.specifications) > MAX_VOLATILITY_SPECIFICATIONS
        ):
            raise ValueError(
                "volatility specifications must contain 1 to 32 items"
            )
        identifiers = tuple(
            item.specification_id for item in self.specifications
        )
        if len(set(identifiers)) != len(identifiers):
            raise ValueError("volatility specification IDs must be unique")
        if not self.projection_specification_ids or not set(
            self.projection_specification_ids
        ).issubset(identifiers):
            raise ValueError("projection IDs must select configured models")
        projected_families = {
            item.family
            for item in self.specifications
            if item.specification_id in self.projection_specification_ids
        }
        if len(projected_families) != len(self.projection_specification_ids):
            raise ValueError(
                "only one projection specification per family is supported"
            )
        if self.projection_horizon < 1:
            raise ValueError("projection_horizon must be positive")
        if (
            self.realized_variance_proxy
            not in VOLATILITY_REALIZED_VARIANCE_PROXIES
        ):
            raise ValueError("unsupported realized_variance_proxy")
        if self.annualization_periods < 0:
            raise ValueError("annualization_periods must be non-negative")
        if (
            not self.baseline_rolling_windows
            or any(value < 2 for value in self.baseline_rolling_windows)
            or tuple(sorted(set(self.baseline_rolling_windows)))
            != self.baseline_rolling_windows
        ):
            raise ValueError(
                "baseline windows must be sorted unique values >=2"
            )
        if not 0 < self.ewma_decay < 1:
            raise ValueError("ewma_decay must be between zero and one")
        if not 0 < self.maximum_persistence <= 1:
            raise ValueError("maximum_persistence must be in (0,1]")
        if (
            not math.isfinite(self.maximum_covariance_condition_number)
            or self.maximum_covariance_condition_number <= 1
        ):
            raise ValueError(
                "maximum_covariance_condition_number must exceed one"
            )
        if not 0 < self.boundary_tolerance < 1:
            raise ValueError("boundary_tolerance must be in (0,1)")
        if not 0 <= self.rounding_digits <= 16:
            raise ValueError("rounding_digits must be between 0 and 16")

    def to_metadata(self) -> dict[str, JSONValue]:
        return {
            "enabled": self.enabled,
            "specifications": cast(
                JSONValue, [item.to_metadata() for item in self.specifications]
            ),
            "projection_specification_ids": list(
                self.projection_specification_ids
            ),
            "projection_horizon": self.projection_horizon,
            "realized_variance_proxy": self.realized_variance_proxy,
            "annualization_periods": self.annualization_periods,
            "baseline_rolling_windows": list(self.baseline_rolling_windows),
            "ewma_decay": self.ewma_decay,
            "maximum_persistence": self.maximum_persistence,
            "maximum_covariance_condition_number": (
                self.maximum_covariance_condition_number
            ),
            "boundary_tolerance": self.boundary_tolerance,
            "compare_exponential_smoothing": self.compare_exponential_smoothing,
            "compare_autoregressive": self.compare_autoregressive,
            "compare_seasonal_exogenous": self.compare_seasonal_exogenous,
            "compare_state_space": self.compare_state_space,
            "rounding_digits": self.rounding_digits,
            "automatic_order_selection": False,
            "automatic_winner": False,
            "asymmetric_extensions": cast(
                JSONValue, list(ASYMMETRIC_VOLATILITY_EXTENSION_REGISTRY)
            ),
        }


@dataclass(frozen=True, slots=True)
class VolatilityResult:
    diagnostics: Mapping[str, JSONValue]
    annotations: tuple[Mapping[str, Any], ...]
    input_result: ClassicalModelInputResult


@dataclass(frozen=True, slots=True)
class _Backend:
    version: str
    arch_model: Any


@dataclass(frozen=True, slots=True)
class _FitOutcome:
    status: str
    reason: str
    mean_forecasts: tuple[float, ...]
    variance_forecasts: tuple[float, ...]
    parameters: Mapping[str, float]
    warning_codes: tuple[str, ...]
    converged: bool
    effective_observation_count: int
    missing_reset_count: int
    persistence: float | None
    unconditional_variance: float | None
    covariance_condition_number: float | None
    boundary_parameter: bool
    standardized_residual_summary: Mapping[str, JSONValue]
    log_likelihood: float | None
    aic: float | None
    bic: float | None


def volatility_from_training_frame(
    frame: Any | None,
    fingerprint: Mapping[str, JSONValue],
    *,
    input_profile: ClassicalModelInputProfile | None = None,
    profile: VolatilityProfile | None = None,
    residual_series: Sequence[float | None] | None = None,
    residual_series_reference_id: str = "",
    exponential_smoothing: Mapping[str, JSONValue] | None = None,
    autoregressive: Mapping[str, JSONValue] | None = None,
    seasonal_exogenous: Mapping[str, JSONValue] | None = None,
    state_space: Mapping[str, JSONValue] | None = None,
    target: Any | None = None,
) -> VolatilityResult:
    selected_input = input_profile or ClassicalModelInputProfile(enabled=True)
    input_result = build_classical_model_input(
        frame, fingerprint, profile=selected_input, target=target
    )
    return volatility_from_model_input(
        frame,
        input_result,
        fingerprint,
        input_profile=selected_input,
        profile=profile,
        residual_series=residual_series,
        residual_series_reference_id=residual_series_reference_id,
        exponential_smoothing=exponential_smoothing,
        autoregressive=autoregressive,
        seasonal_exogenous=seasonal_exogenous,
        state_space=state_space,
        target=target,
    )


def volatility_from_model_input(
    frame: Any | None,
    input_result: ClassicalModelInputResult,
    fingerprint: Mapping[str, JSONValue],
    *,
    input_profile: ClassicalModelInputProfile,
    profile: VolatilityProfile | None = None,
    residual_series: Sequence[float | None] | None = None,
    residual_series_reference_id: str = "",
    exponential_smoothing: Mapping[str, JSONValue] | None = None,
    autoregressive: Mapping[str, JSONValue] | None = None,
    seasonal_exogenous: Mapping[str, JSONValue] | None = None,
    state_space: Mapping[str, JSONValue] | None = None,
    target: Any | None = None,
) -> VolatilityResult:
    selected = profile or VolatilityProfile(enabled=True)
    base = _base_payload(input_result, fingerprint, selected)
    if selected.projection_horizon not in input_profile.horizons:
        return _unavailable_result(input_result, base, "invalid_configuration")
    if input_result.contract.get("status") == "unavailable":
        return _unavailable_result(
            input_result, base, "input_contract_unavailable"
        )
    if not input_result.folds:
        return _unavailable_result(
            input_result, base, "insufficient_folds", status="limited"
        )
    backend = _load_backend()
    if backend is None:
        return _unavailable_result(input_result, base, "dependency_unavailable")
    return _evaluate_models(
        frame,
        input_result,
        fingerprint,
        input_profile,
        selected,
        backend,
        residual_series=residual_series,
        residual_series_reference_id=residual_series_reference_id,
        exponential_smoothing=exponential_smoothing,
        autoregressive=autoregressive,
        seasonal_exogenous=seasonal_exogenous,
        state_space=state_space,
        target=target,
    )


def volatility_diagnostics_from_training_frame(
    frame: Any | None,
    fingerprint: Mapping[str, JSONValue],
    *,
    input_profile: ClassicalModelInputProfile | None = None,
    profile: VolatilityProfile | None = None,
    target: Any | None = None,
) -> dict[str, JSONValue]:
    return dict(
        volatility_from_training_frame(
            frame,
            fingerprint,
            input_profile=input_profile,
            profile=profile,
            target=target,
        ).diagnostics
    )


def project_volatility_onto_training_frame(
    frame: Any, result: VolatilityResult, *, target: Any | None = None
) -> Any:
    import polars as pl

    columns = set(getattr(frame, "columns", ()))
    enriched = (
        frame
        if {"series_id", "period", "row_id"}.issubset(columns)
        else ensure_tick_training_features(frame, target=target)
    )
    left = enriched.drop(
        [name for name in VOLATILITY_COLUMNS if name in enriched.columns]
    ).with_row_index("__cm_volatility_original_order")
    if result.annotations:
        right = pl.DataFrame(
            [dict(row) for row in result.annotations], infer_schema_length=None
        )
        projected = left.join(
            right,
            on=["series_id", "period", "row_id"],
            how="left",
            validate="m:1",
        )
    else:
        projected = left
    projected = _ensure_projection_columns(projected)
    return projected.sort("__cm_volatility_original_order").drop(
        "__cm_volatility_original_order"
    )


def volatility_summary(
    findings: Iterable[QualityFinding],
    *,
    target_limit: int | None = DEFAULT_VOLATILITY_SUMMARY_TARGET_LIMIT,
) -> dict[str, JSONValue] | None:
    targets: list[dict[str, JSONValue]] = []
    statuses: Counter[str] = Counter()
    for finding in findings:
        fingerprint = _mapping(finding.metadata.get("time_series_fingerprint"))
        payload = _mapping(fingerprint.get("volatility"))
        if not payload:
            continue
        status = _text(payload.get("status")) or "unavailable"
        evaluation = _mapping(payload.get("evaluation"))
        fit_summary = _mapping(payload.get("fit_summary"))
        statuses[status] += 1
        targets.append(
            {
                "target_axis": dict(_mapping(payload.get("target_axis"))),
                "status": status,
                "reason": payload.get("reason"),
                "model_count": _int(evaluation.get("model_count")),
                "fit_attempt_count": _int(fit_summary.get("fit_attempt_count")),
                "evaluated_fold_count": _int(
                    evaluation.get("evaluated_fold_count")
                ),
                "failed_fit_count": _int(fit_summary.get("failed_fit_count")),
            }
        )
    if not targets:
        return None
    targets.sort(key=_target_sort_key)
    limit = bounded_report_limit(
        target_limit,
        default_limit=DEFAULT_VOLATILITY_SUMMARY_TARGET_LIMIT,
        allow_unbounded=True,
    )
    included = limit.slice(targets)
    omitted = len(targets) - len(included)
    return {
        "schema_version": VOLATILITY_SUMMARY_SCHEMA_VERSION,
        "advisory": True,
        "target_count": len(targets),
        "included_target_count": len(included),
        "omitted_target_count": omitted,
        "truncated": omitted > 0,
        "status_counts": dict(sorted(statuses.items())),
        "target_summaries": cast(JSONValue, included),
        "limit_metadata": {"targets": limit.limit_payload()},
    }


def format_volatility_summary_lines(
    summary: Mapping[str, JSONValue] | None,
) -> tuple[str, ...]:
    if not summary:
        return ()
    statuses = _mapping(summary.get("status_counts"))
    lines = [
        "",
        "ARCH and GARCH volatility models",
        f"targets: {_int(summary.get('target_count'))} ready: {_int(statuses.get('ready'))} limited: {_int(statuses.get('limited'))} unavailable: {_int(statuses.get('unavailable'))}",
    ]
    for item in _mapping_rows(summary.get("target_summaries")):
        axis = _mapping(item.get("target_axis"))
        label = "/".join(
            _text(axis.get(key))
            for key in ("data_format", "timeframe", "symbol", "period")
        )
        lines.append(
            f"- {label}: {_text(item.get('status'))} models={_int(item.get('model_count'))} fits={_int(item.get('fit_attempt_count'))} folds={_int(item.get('evaluated_fold_count'))}"
        )
    if summary.get("truncated") is True:
        lines.append(
            f"- {_int(summary.get('omitted_target_count'))} targets omitted"
        )
    return tuple(lines)


def _evaluate_models(
    frame: Any | None,
    input_result: ClassicalModelInputResult,
    fingerprint: Mapping[str, JSONValue],
    input_profile: ClassicalModelInputProfile,
    profile: VolatilityProfile,
    backend: _Backend,
    *,
    residual_series: Sequence[float | None] | None,
    residual_series_reference_id: str,
    exponential_smoothing: Mapping[str, JSONValue] | None,
    autoregressive: Mapping[str, JSONValue] | None,
    seasonal_exogenous: Mapping[str, JSONValue] | None,
    state_space: Mapping[str, JSONValue] | None,
    target: Any | None,
) -> VolatilityResult:
    rows = cast(list[dict[str, Any]], input_result.regularized_frame.to_dicts())
    folds = [dict(fold) for fold in input_result.folds]
    origins = _folds_by_origin(folds)
    resources = input_profile.resources
    specifications = profile.specifications[: resources.max_candidate_orders]
    limitations: list[str] = []
    estimated_memory = len(rows) * max(1, len(specifications)) * 8 * 10
    if (
        len(specifications) < len(profile.specifications)
        or estimated_memory > resources.max_memory_bytes
    ):
        limitations.append("resource_limit")
    if estimated_memory > resources.max_memory_bytes:
        specifications = ()
    if residual_series is not None and len(residual_series) != len(rows):
        return _unavailable_result(
            input_result,
            _base_payload(input_result, fingerprint, profile),
            "invalid_configuration",
        )
    started = time.monotonic()
    fit_attempt_count = 0
    all_evaluations: list[dict[str, Any]] = []
    all_fit_samples: list[dict[str, JSONValue]] = []
    model_payloads: list[dict[str, JSONValue]] = []
    fit_statuses: Counter[str] = Counter()
    fit_reasons: Counter[str] = Counter()
    warning_counts: Counter[str] = Counter()
    for specification_code, specification in enumerate(specifications, start=1):
        model_id = _model_id(
            specification,
            _text(input_result.contract.get("derivation_id")),
            backend.version,
        )
        model_evaluations: list[dict[str, Any]] = []
        model_fit_samples: list[dict[str, JSONValue]] = []
        configuration_reason = _configuration_reason(
            specification,
            input_profile,
            residual_series,
            residual_series_reference_id,
        )
        for _, origin_folds in origins:
            if fit_attempt_count >= resources.max_fit_attempts:
                limitations.append("resource_limit")
                break
            if time.monotonic() - started >= resources.max_wall_time_seconds:
                limitations.append("timeout")
                break
            fit_attempt_count += 1
            origin = origin_folds[0]
            start = _int(origin.get("training_start_index"))
            end = _int(origin.get("training_end_index"))
            source = (
                residual_series
                if specification.input_definition == "mean_model_residual"
                else tuple(
                    _optional_float(row.get("cm_input_value")) for row in rows
                )
            )
            values = (
                tuple(source[index] for index in range(start, end + 1))
                if source is not None
                else ()
            )
            if specification.input_definition == "demeaned_return":
                finite = [float(value) for value in values if value is not None]
                center = sum(finite) / len(finite) if finite else 0.0
                values = tuple(
                    None if value is None else float(value) - center
                    for value in values
                )
            max_horizon = max(
                _int(fold.get("horizon")) for fold in origin_folds
            )
            outcome = (
                _empty_fit("skipped", configuration_reason)
                if configuration_reason
                else _fit_specification(
                    specification, values, max_horizon, profile, backend
                )
            )
            fit_statuses[outcome.status] += 1
            if outcome.reason:
                fit_reasons[outcome.reason] += 1
            warning_counts.update(outcome.warning_codes)
            fit_sample = _fit_sample(
                outcome,
                specification,
                model_id,
                origin,
                profile.rounding_digits,
            )
            model_fit_samples.append(fit_sample)
            all_fit_samples.append(fit_sample)
            for fold in origin_folds:
                evaluation = _fold_evaluation(
                    rows,
                    residual_series,
                    fold,
                    specification,
                    specification_code,
                    model_id,
                    outcome,
                    profile,
                )
                model_evaluations.append(evaluation)
                all_evaluations.append(evaluation)
        model_payloads.append(
            _model_evaluation_payload(
                specification,
                specification_code,
                model_id,
                model_evaluations,
                model_fit_samples,
                resources.max_retained_diagnostics,
                profile.rounding_digits,
            )
        )
        if limitations and limitations[-1] in {"resource_limit", "timeout"}:
            break
    baselines = _variance_baselines(rows, folds, profile)
    references = {
        "exponential_smoothing": _model_references(
            exponential_smoothing,
            enabled=profile.compare_exponential_smoothing,
            unavailable_reason="exponential_smoothing_not_enabled",
        ),
        "autoregressive": _model_references(
            autoregressive,
            enabled=profile.compare_autoregressive,
            unavailable_reason="autoregressive_not_enabled",
        ),
        "seasonal_exogenous": _model_references(
            seasonal_exogenous,
            enabled=profile.compare_seasonal_exogenous,
            unavailable_reason="seasonal_exogenous_not_enabled",
        ),
        "state_space": _model_references(
            state_space,
            enabled=profile.compare_state_space,
            unavailable_reason="state_space_not_enabled",
        ),
    }
    annotations, collisions = _build_annotations(
        frame, all_evaluations, profile, input_result, target=target
    )
    evaluated_count = sum(
        item.get("status") == "evaluated" for item in all_evaluations
    )
    failed_count = sum(
        status in {"failed", "unavailable"}
        for status in fit_statuses.elements()
    )
    limited_count = fit_statuses["limited"] + fit_statuses["skipped"]
    if not evaluated_count:
        limitations.append("insufficient_history")
    limitations = list(dict.fromkeys(limitations))
    status = (
        "ready"
        if not limitations and not failed_count and not limited_count
        else "limited"
    )
    reason = (
        limitations[0]
        if limitations
        else sorted(fit_reasons)[0] if fit_reasons else None
    )
    diagnostics: dict[str, JSONValue] = {
        **_base_payload(input_result, fingerprint, profile),
        "status": status,
        "reason": reason,
        "limitations": cast(JSONValue, limitations),
        "backend": {
            "provider": "arch",
            "version": backend.version,
            "available": True,
            "model_factory": "arch.arch_model",
            "import_basis": "optional_models_extra",
        },
        "fit_summary": {
            "schema_version": VOLATILITY_FIT_SCHEMA_VERSION,
            "fit_attempt_count": fit_attempt_count,
            "status_counts": dict(sorted(fit_statuses.items())),
            "reason_counts": dict(sorted(fit_reasons.items())),
            "warning_counts": dict(sorted(warning_counts.items())),
            "failed_fit_count": failed_count,
            "limited_fit_count": limited_count,
            "convergence_rate": _rate(
                fit_statuses["converged"],
                fit_attempt_count,
                profile.rounding_digits,
            ),
            "failure_rate": _rate(
                failed_count, fit_attempt_count, profile.rounding_digits
            ),
            "fit_samples": cast(
                JSONValue, all_fit_samples[: resources.max_retained_diagnostics]
            ),
            "fit_samples_truncated": len(all_fit_samples)
            > resources.max_retained_diagnostics,
        },
        "resource_usage": {
            "limits": resources.to_metadata(),
            "estimated_working_memory_bytes": estimated_memory,
            "memory_limit_exceeded": estimated_memory
            > resources.max_memory_bytes,
            "fit_attempt_count": fit_attempt_count,
            "wall_time_limit_enforced": True,
            "wall_time_observed_in_payload": False,
        },
        "evaluation": {
            "schema_version": VOLATILITY_EVALUATION_SCHEMA_VERSION,
            "calculation_basis": "regular_grid_rolling_origin_conditional_variance",
            "variance_scale": "unscaled_return_squared",
            "mean_scale": "unscaled_return",
            "realized_variance_proxy": profile.realized_variance_proxy,
            "model_count": len(model_payloads),
            "fold_count": len(folds),
            "evaluated_fold_count": evaluated_count,
            "skipped_evaluation_count": len(all_evaluations) - evaluated_count,
            "forecast_coverage_rate": _rate(
                evaluated_count, len(all_evaluations), profile.rounding_digits
            ),
            "models": cast(JSONValue, model_payloads),
            "reference_variance_baselines": cast(JSONValue, baselines),
            "baseline_relative_skill": cast(
                JSONValue,
                _baseline_relative_skill(
                    model_payloads, baselines, profile.rounding_digits
                ),
            ),
            "preceding_mean_model_references": cast(JSONValue, references),
            "comparison_semantics": "descriptive_shared_folds_separate_mean_and_variance_metrics",
            "automatic_winner": False,
        },
        "training_projection": _training_projection_metadata(
            profile, input_result, len(annotations), collisions
        ),
        "fit_duration_included": False,
    }
    return VolatilityResult(diagnostics, annotations, input_result)


def _configuration_reason(
    specification: VolatilitySpecification,
    input_profile: ClassicalModelInputProfile,
    residual_series: Sequence[float | None] | None,
    residual_reference: str,
) -> str:
    expected = (
        "return"
        if specification.input_definition in {"raw_return", "demeaned_return"}
        else (
            "log_return"
            if specification.input_definition == "log_return"
            else input_profile.transform
        )
    )
    if (
        specification.input_definition != "mean_model_residual"
        and input_profile.transform != expected
    ):
        return "invalid_transform"
    if specification.input_definition == "mean_model_residual":
        if residual_series is None:
            return "residual_series_unavailable"
        if residual_reference != specification.mean_model_reference_id:
            return "residual_reference_mismatch"
    if (
        specification.innovation_order + specification.variance_order
        > input_profile.resources.max_candidate_orders
    ):
        return "invalid_order"
    return ""


def _fit_specification(
    specification: VolatilitySpecification,
    values: Sequence[float | None],
    horizon: int,
    profile: VolatilityProfile,
    backend: _Backend,
) -> _FitOutcome:
    trailing, resets = _trailing_contiguous(values)
    minimum = max(
        12,
        4 * (specification.innovation_order + specification.variance_order + 1),
    )
    if len(trailing) < minimum:
        return _empty_fit(
            "skipped",
            "insufficient_history",
            observed_count=len(trailing),
            missing_reset_count=resets,
        )
    if any(not math.isfinite(value) for value in trailing):
        return _empty_fit(
            "failed",
            "non_finite_input",
            observed_count=len(trailing),
            missing_reset_count=resets,
        )
    center = sum(trailing) / len(trailing)
    sample_variance = sum((value - center) ** 2 for value in trailing) / len(
        trailing
    )
    if not math.isfinite(sample_variance) or sample_variance <= 1e-24:
        return _empty_fit(
            "skipped",
            "zero_variance",
            observed_count=len(trailing),
            missing_reset_count=resets,
        )
    scaled = [value * specification.scale_factor for value in trailing]
    if any(not math.isfinite(value) for value in scaled):
        return _empty_fit(
            "failed",
            "scaling_failure",
            observed_count=len(trailing),
            missing_reset_count=resets,
        )
    backcast = None
    if specification.variance_initialization == "sample_variance":
        backcast = sample_variance * specification.scale_factor**2
    elif specification.variance_initialization == "fixed":
        backcast = (
            cast(float, specification.initial_variance)
            * specification.scale_factor**2
        )
    try:
        numpy = importlib.import_module("numpy")
        model = backend.arch_model(
            numpy.asarray(scaled, dtype=float),
            mean="Zero" if specification.mean_model == "zero" else "Constant",
            vol="ARCH" if specification.family == "arch" else "GARCH",
            p=specification.innovation_order,
            o=0,
            q=specification.variance_order,
            power=2.0,
            dist="normal" if specification.distribution == "normal" else "t",
            rescale=False,
        )
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = model.fit(
                update_freq=0,
                disp="off",
                show_warning=False,
                cov_type=specification.covariance_type,
                tol=specification.optimizer_tolerance,
                options={"maxiter": specification.max_iterations},
                backcast=backcast,
            )
            forecast = result.forecast(
                horizon=horizon, method="analytic", reindex=False
            )
        scale2 = specification.scale_factor**2
        means = tuple(
            float(value) / specification.scale_factor
            for value in forecast.mean.values[-1]
        )
        variances = tuple(
            float(value) / scale2 for value in forecast.variance.values[-1]
        )
        if (
            not means
            or not variances
            or any(not math.isfinite(value) for value in (*means, *variances))
        ):
            return _empty_fit(
                "failed",
                "numerical_instability",
                observed_count=len(trailing),
                missing_reset_count=resets,
            )
        if any(value <= 0 for value in variances):
            return _empty_fit(
                "failed",
                "non_positive_variance",
                observed_count=len(trailing),
                missing_reset_count=resets,
            )
        parameters = {
            str(name): float(value)
            for name, value in result.params.items()
            if math.isfinite(float(value))
        }
        covariance = numpy.asarray(result.param_cov, dtype=float)
        if covariance.size and not numpy.isfinite(covariance).all():
            return _empty_fit(
                "failed",
                "singular_covariance",
                observed_count=len(trailing),
                missing_reset_count=resets,
            )
        covariance_condition = (
            float(numpy.linalg.cond(covariance)) if covariance.size else None
        )
        if covariance_condition is not None and (
            not math.isfinite(covariance_condition)
            or covariance_condition
            > profile.maximum_covariance_condition_number
        ):
            return _empty_fit(
                "failed",
                "singular_covariance",
                observed_count=len(trailing),
                missing_reset_count=resets,
            )
        persistence = sum(
            value
            for name, value in parameters.items()
            if name.startswith("alpha[") or name.startswith("beta[")
        )
        omega = parameters.get("omega")
        unconditional = (
            omega / scale2 / (1.0 - persistence)
            if omega is not None and 0 <= persistence < 1
            else None
        )
        boundary = _boundary_parameter(
            parameters, specification, profile.boundary_tolerance
        )
        reason = ""
        status = "converged"
        converged = int(getattr(result, "convergence_flag", 1)) == 0 and bool(
            getattr(result.optimization_result, "success", False)
        )
        if not converged:
            status, reason = "limited", "optimizer_failure"
        elif persistence >= profile.maximum_persistence:
            status, reason = "limited", "non_stationary_variance"
        elif _violates_parameter_bounds(
            parameters, specification.parameter_bounds
        ):
            status, reason = "limited", "parameter_bound_violation"
        residuals = [
            float(value)
            for value in result.std_resid
            if math.isfinite(float(value))
        ]
        return _FitOutcome(
            status,
            reason,
            means,
            variances,
            parameters,
            _warning_codes(caught),
            converged,
            int(getattr(result, "nobs", len(trailing))),
            resets,
            persistence,
            unconditional,
            covariance_condition,
            boundary,
            _numeric_summary(residuals, profile.rounding_digits),
            _finite_float(getattr(result, "loglikelihood", None)),
            _finite_float(getattr(result, "aic", None)),
            _finite_float(getattr(result, "bic", None)),
        )
    except (
        ArithmeticError,
        ImportError,
        RuntimeError,
        TypeError,
        ValueError,
        ZeroDivisionError,
    ):
        return _empty_fit(
            "failed",
            "backend_failure",
            observed_count=len(trailing),
            missing_reset_count=resets,
        )


def _fold_evaluation(
    rows: Sequence[Mapping[str, Any]],
    residual_series: Sequence[float | None] | None,
    fold: Mapping[str, Any],
    specification: VolatilitySpecification,
    specification_code: int,
    model_id: str,
    outcome: _FitOutcome,
    profile: VolatilityProfile,
) -> dict[str, Any]:
    horizon = _int(fold.get("horizon"))
    target_index = _int(fold.get("target_index"))
    actual = None
    if 0 <= target_index < len(rows):
        actual = (
            _optional_float(residual_series[target_index])
            if specification.input_definition == "mean_model_residual"
            and residual_series is not None
            else _optional_float(rows[target_index].get("cm_input_value"))
        )
    mean_forecast = (
        outcome.mean_forecasts[horizon - 1]
        if 0 < horizon <= len(outcome.mean_forecasts)
        else None
    )
    variance_forecast = (
        outcome.variance_forecasts[horizon - 1]
        if 0 < horizon <= len(outcome.variance_forecasts)
        else None
    )
    if outcome.status in {"failed", "skipped", "unavailable"}:
        status, reason = "not_evaluated", outcome.reason
    elif fold.get("status") != "valid" or actual is None:
        status, reason = "skipped", "target_unavailable"
    elif variance_forecast is None or mean_forecast is None:
        status, reason = "skipped", "numerical_instability"
    else:
        status, reason = "evaluated", ""
    realized = (
        actual**2 if status == "evaluated" and actual is not None else None
    )
    volatility = (
        math.sqrt(variance_forecast)
        if variance_forecast is not None and variance_forecast > 0
        else None
    )
    annualized_variance = (
        variance_forecast * profile.annualization_periods
        if variance_forecast is not None and profile.annualization_periods
        else None
    )
    annualized_volatility = (
        math.sqrt(annualized_variance)
        if annualized_variance is not None
        else None
    )
    return {
        "schema_version": VOLATILITY_FORECAST_SCHEMA_VERSION,
        "status": status,
        "reason": reason or None,
        "series_id": _text(fold.get("series_id")),
        "period": _text(fold.get("period")),
        "model_id": model_id,
        "specification_id": specification.specification_id,
        "specification_code": specification_code,
        "family": specification.family,
        "family_code": VOLATILITY_FAMILY_CODES[specification.family],
        "input_definition": specification.input_definition,
        "input_definition_code": VOLATILITY_INPUT_DEFINITION_CODES[
            specification.input_definition
        ],
        "mean_model": specification.mean_model,
        "mean_model_code": VOLATILITY_MEAN_MODEL_CODES[
            specification.mean_model
        ],
        "distribution": specification.distribution,
        "distribution_code": VOLATILITY_DISTRIBUTION_CODES[
            specification.distribution
        ],
        "innovation_order": specification.innovation_order,
        "variance_order": specification.variance_order,
        "scale_factor": specification.scale_factor,
        "fit_status": outcome.status,
        "fit_reason": outcome.reason or None,
        "converged": outcome.converged,
        "effective_observation_count": outcome.effective_observation_count,
        "missing_reset_count": outcome.missing_reset_count,
        "persistence": _rounded(outcome.persistence, profile.rounding_digits),
        "unconditional_variance": _rounded(
            outcome.unconditional_variance, profile.rounding_digits
        ),
        "boundary_parameter": outcome.boundary_parameter,
        "fold_id": _int(fold.get("fold_id")),
        "origin_row_id": fold.get("origin_row_id"),
        "target_row_id": fold.get("target_row_id"),
        "origin_bin_end_utc_ms": _int(fold.get("origin_bin_end_utc_ms")),
        "target_bin_end_utc_ms": _int(fold.get("target_bin_end_utc_ms")),
        "horizon": horizon,
        "mean_forecast": _rounded(mean_forecast, profile.rounding_digits),
        "variance_forecast": _rounded(
            variance_forecast, profile.rounding_digits
        ),
        "volatility_forecast": _rounded(volatility, profile.rounding_digits),
        "annualized_variance_forecast": _rounded(
            annualized_variance, profile.rounding_digits
        ),
        "annualized_volatility_forecast": _rounded(
            annualized_volatility, profile.rounding_digits
        ),
        "actual_return": _rounded(actual, profile.rounding_digits),
        "realized_variance_proxy": _rounded(realized, profile.rounding_digits),
        "mean_error": _rounded(
            (
                mean_forecast - actual
                if status == "evaluated"
                and mean_forecast is not None
                and actual is not None
                else None
            ),
            profile.rounding_digits,
        ),
        "variance_error": _rounded(
            (
                variance_forecast - realized
                if status == "evaluated"
                and variance_forecast is not None
                and realized is not None
                else None
            ),
            profile.rounding_digits,
        ),
        "volatility_error": _rounded(
            (
                volatility - abs(actual)
                if status == "evaluated"
                and volatility is not None
                and actual is not None
                else None
            ),
            profile.rounding_digits,
        ),
        "qlike_loss": _rounded(
            (
                math.log(variance_forecast) + realized / variance_forecast
                if status == "evaluated"
                and variance_forecast is not None
                and variance_forecast > 0
                and realized is not None
                else None
            ),
            profile.rounding_digits,
        ),
    }


def _fit_sample(
    outcome: _FitOutcome,
    specification: VolatilitySpecification,
    model_id: str,
    origin: Mapping[str, Any],
    digits: int,
) -> dict[str, JSONValue]:
    return {
        "schema_version": VOLATILITY_FIT_SCHEMA_VERSION,
        "model_id": model_id,
        "specification_id": specification.specification_id,
        "family": specification.family,
        "fold_id": _int(origin.get("fold_id")),
        "origin_row_id": origin.get("origin_row_id"),
        "status": outcome.status,
        "reason": outcome.reason or None,
        "converged": outcome.converged,
        "effective_observation_count": outcome.effective_observation_count,
        "missing_reset_count": outcome.missing_reset_count,
        "parameters": cast(
            JSONValue,
            {
                name: _rounded(value, digits)
                for name, value in sorted(outcome.parameters.items())
            },
        ),
        "persistence": _rounded(outcome.persistence, digits),
        "unconditional_variance": _rounded(
            outcome.unconditional_variance, digits
        ),
        "covariance_condition_number": _rounded(
            outcome.covariance_condition_number, digits
        ),
        "boundary_parameter": outcome.boundary_parameter,
        "standardized_residual_summary": dict(
            outcome.standardized_residual_summary
        ),
        "log_likelihood": _rounded(outcome.log_likelihood, digits),
        "aic": _rounded(outcome.aic, digits),
        "bic": _rounded(outcome.bic, digits),
        "warning_codes": list(outcome.warning_codes),
        "backend_exception_text_included": False,
    }


def _model_evaluation_payload(
    specification: VolatilitySpecification,
    specification_code: int,
    model_id: str,
    evaluations: Sequence[Mapping[str, Any]],
    fit_samples: Sequence[Mapping[str, JSONValue]],
    diagnostic_limit: int,
    digits: int,
) -> dict[str, JSONValue]:
    evaluated = [row for row in evaluations if row.get("status") == "evaluated"]
    horizons: dict[str, JSONValue] = {}
    for horizon in sorted({_int(row.get("horizon")) for row in evaluated}):
        rows = [row for row in evaluated if _int(row.get("horizon")) == horizon]
        horizons[str(horizon)] = _metric_summary(rows, digits)
    return {
        "schema_version": VOLATILITY_EVALUATION_SCHEMA_VERSION,
        "model_id": model_id,
        "specification_id": specification.specification_id,
        "specification_code": specification_code,
        "family": specification.family,
        "configuration": specification.to_metadata(),
        "status": "evaluated" if evaluated else "not_evaluated",
        "evaluated_count": len(evaluated),
        "horizon_metrics": horizons,
        "rolling_window_stability": _rolling_stability(evaluated, digits),
        "parameter_stability": _parameter_stability(fit_samples, digits),
        "fit_samples": cast(JSONValue, list(fit_samples[:diagnostic_limit])),
        "fit_samples_truncated": len(fit_samples) > diagnostic_limit,
        "automatic_winner": False,
    }


def _metric_summary(
    rows: Sequence[Mapping[str, Any]], digits: int
) -> dict[str, JSONValue]:
    def values(name: str) -> list[float]:
        return [
            value
            for row in rows
            if (value := _optional_float(row.get(name))) is not None
        ]

    mean_errors, variance_errors, volatility_errors, qlike = (
        values("mean_error"),
        values("variance_error"),
        values("volatility_error"),
        values("qlike_loss"),
    )
    return {
        "count": len(rows),
        "mean_metrics": _errors(mean_errors, digits),
        "variance_metrics": {
            **_errors(variance_errors, digits),
            "mean_qlike": _rounded(
                sum(qlike) / len(qlike) if qlike else None, digits
            ),
        },
        "volatility_metrics": _errors(volatility_errors, digits),
        "metrics_are_not_interchangeable": True,
    }


def _errors(errors: Sequence[float], digits: int) -> dict[str, JSONValue]:
    absolute = [abs(value) for value in errors]
    return {
        "count": len(errors),
        "mae": _rounded(
            sum(absolute) / len(absolute) if absolute else None, digits
        ),
        "rmse": _rounded(
            (
                math.sqrt(sum(value * value for value in errors) / len(errors))
                if errors
                else None
            ),
            digits,
        ),
        "bias": _rounded(sum(errors) / len(errors) if errors else None, digits),
    }


def _rolling_stability(
    rows: Sequence[Mapping[str, Any]], digits: int
) -> dict[str, JSONValue]:
    width = max(1, math.ceil(len(rows) / 3))
    chunks = [
        rows[index : index + width] for index in range(0, len(rows), width)
    ]
    return {
        "segments": cast(
            JSONValue,
            [_metric_summary(chunk, digits) for chunk in chunks if chunk],
        ),
        "automatic_winner": False,
    }


def _parameter_stability(
    samples: Sequence[Mapping[str, JSONValue]], digits: int
) -> dict[str, JSONValue]:
    grouped: dict[str, list[float]] = {}
    for sample in samples:
        for name, raw in _mapping(sample.get("parameters")).items():
            value = _optional_float(raw)
            if value is not None:
                grouped.setdefault(name, []).append(value)
    return {
        "parameters": cast(
            JSONValue,
            {
                name: _numeric_summary(values, digits)
                for name, values in sorted(grouped.items())
            },
        ),
        "bounded": True,
    }


def _variance_baselines(
    rows: Sequence[Mapping[str, Any]],
    folds: Sequence[Mapping[str, Any]],
    profile: VolatilityProfile,
) -> list[dict[str, JSONValue]]:
    predictions: dict[str, list[dict[str, Any]]] = {
        f"rolling_variance_{window}": []
        for window in profile.baseline_rolling_windows
    }
    predictions[f"ewma_variance_{profile.ewma_decay}"] = []
    for fold in folds:
        if fold.get("status") != "valid":
            continue
        start, end, target = (
            _int(fold.get("training_start_index")),
            _int(fold.get("training_end_index")),
            _int(fold.get("target_index")),
        )
        if not 0 <= target < len(rows):
            continue
        actual = _optional_float(rows[target].get("cm_input_value"))
        training = [
            _optional_float(rows[index].get("cm_input_value"))
            for index in range(start, end + 1)
        ]
        trailing, _ = _trailing_contiguous(training)
        if actual is None:
            continue
        realized = actual**2
        for window in profile.baseline_rolling_windows:
            if len(trailing) >= window:
                sample = trailing[-window:]
                center = sum(sample) / len(sample)
                prediction = sum(
                    (value - center) ** 2 for value in sample
                ) / len(sample)
                predictions[f"rolling_variance_{window}"].append(
                    {
                        "horizon": _int(fold.get("horizon")),
                        "error": prediction - realized,
                        "qlike": (
                            math.log(prediction) + realized / prediction
                            if prediction > 0
                            else None
                        ),
                    }
                )
        if trailing:
            variance = trailing[0] ** 2
            for value in trailing[1:]:
                variance = (
                    profile.ewma_decay * variance
                    + (1 - profile.ewma_decay) * value**2
                )
            predictions[f"ewma_variance_{profile.ewma_decay}"].append(
                {
                    "horizon": _int(fold.get("horizon")),
                    "error": variance - realized,
                    "qlike": (
                        math.log(variance) + realized / variance
                        if variance > 0
                        else None
                    ),
                }
            )
    output: list[dict[str, JSONValue]] = []
    for name, evaluations in predictions.items():
        errors = [cast(float, row["error"]) for row in evaluations]
        qlike = [
            cast(float, row["qlike"])
            for row in evaluations
            if row["qlike"] is not None
        ]
        horizon_metrics: dict[str, JSONValue] = {}
        for horizon in sorted(
            {_int(row.get("horizon")) for row in evaluations}
        ):
            horizon_rows = [
                row
                for row in evaluations
                if _int(row.get("horizon")) == horizon
            ]
            horizon_errors = [cast(float, row["error"]) for row in horizon_rows]
            horizon_qlike = [
                cast(float, row["qlike"])
                for row in horizon_rows
                if row["qlike"] is not None
            ]
            horizon_metrics[str(horizon)] = {
                **_errors(horizon_errors, profile.rounding_digits),
                "mean_qlike": _rounded(
                    (
                        sum(horizon_qlike) / len(horizon_qlike)
                        if horizon_qlike
                        else None
                    ),
                    profile.rounding_digits,
                ),
            }
        output.append(
            {
                "name": name,
                "realized_variance_proxy": profile.realized_variance_proxy,
                "metrics": {
                    **_errors(errors, profile.rounding_digits),
                    "mean_qlike": _rounded(
                        sum(qlike) / len(qlike) if qlike else None,
                        profile.rounding_digits,
                    ),
                },
                "horizon_metrics": horizon_metrics,
                "automatic_winner": False,
            }
        )
    return output


def _baseline_relative_skill(
    models: Sequence[Mapping[str, JSONValue]],
    baselines: Sequence[Mapping[str, JSONValue]],
    digits: int,
) -> list[dict[str, JSONValue]]:
    output: list[dict[str, JSONValue]] = []
    for model in models:
        model_horizons = _mapping(model.get("horizon_metrics"))
        comparisons: list[dict[str, JSONValue]] = []
        for baseline in baselines:
            baseline_horizons = _mapping(baseline.get("horizon_metrics"))
            horizon_skill: dict[str, JSONValue] = {}
            for horizon, raw_model_metrics in sorted(model_horizons.items()):
                model_variance = _mapping(
                    _mapping(raw_model_metrics).get("variance_metrics")
                )
                baseline_metrics = _mapping(baseline_horizons.get(horizon))
                model_mae = _optional_float(model_variance.get("mae"))
                baseline_mae = _optional_float(baseline_metrics.get("mae"))
                model_qlike = _optional_float(model_variance.get("mean_qlike"))
                baseline_qlike = _optional_float(
                    baseline_metrics.get("mean_qlike")
                )
                horizon_skill[horizon] = {
                    "variance_mae_skill": _rounded(
                        (
                            1.0 - model_mae / baseline_mae
                            if model_mae is not None
                            and baseline_mae is not None
                            and baseline_mae > 0
                            else None
                        ),
                        digits,
                    ),
                    "qlike_difference": _rounded(
                        (
                            model_qlike - baseline_qlike
                            if model_qlike is not None
                            and baseline_qlike is not None
                            else None
                        ),
                        digits,
                    ),
                    "positive_mae_skill_means_model_improvement": True,
                    "negative_qlike_difference_means_model_improvement": True,
                }
            comparisons.append(
                {
                    "baseline": baseline.get("name"),
                    "horizon_skill": horizon_skill,
                }
            )
        output.append(
            {
                "model_id": model.get("model_id"),
                "specification_id": model.get("specification_id"),
                "comparisons": cast(JSONValue, comparisons),
                "descriptive_only": True,
                "automatic_winner": False,
            }
        )
    return output


def _build_annotations(
    frame: Any | None,
    evaluations: Sequence[Mapping[str, Any]],
    profile: VolatilityProfile,
    input_result: ClassicalModelInputResult,
    *,
    target: Any | None,
) -> tuple[tuple[Mapping[str, Any], ...], int]:
    if frame is None:
        return (), 0
    try:
        enriched = ensure_tick_training_features(frame, target=target)
    except (AttributeError, TypeError, ValueError):
        return (), 0
    availability: dict[tuple[str, str], list[tuple[int, int]]] = {}
    for row in cast(list[dict[str, Any]], enriched.to_dicts()):
        timestamp, row_id = _optional_int(
            row.get("timestamp_utc_ms")
        ), _optional_int(row.get("row_id"))
        if timestamp is not None and row_id is not None:
            availability.setdefault(
                (_text(row.get("series_id")), _text(row.get("period"))), []
            ).append((timestamp, row_id))
    for values in availability.values():
        values.sort()
    selected = [
        row
        for row in evaluations
        if _text(row.get("specification_id"))
        in profile.projection_specification_ids
        and _int(row.get("horizon")) == profile.projection_horizon
    ]
    selected.sort(
        key=lambda row: (
            _text(row.get("series_id")),
            _text(row.get("period")),
            _int(row.get("origin_bin_end_utc_ms")),
            _int(row.get("fold_id")),
        )
    )
    merged: dict[tuple[str, str, int], dict[str, Any]] = {}
    collisions = 0
    for evaluation in selected:
        group = (
            _text(evaluation.get("series_id")),
            _text(evaluation.get("period")),
        )
        for diagnostic, key_name in (
            (False, "origin_bin_end_utc_ms"),
            (True, "target_bin_end_utc_ms"),
        ):
            if (
                diagnostic
                and _optional_float(evaluation.get("variance_error")) is None
            ):
                continue
            row_id = _first_available_row_id(
                availability.get(group, ()), _int(evaluation.get(key_name))
            )
            if row_id is None:
                continue
            key = (*group, row_id)
            annotation = _annotation_row(
                evaluation, input_result, row_id, diagnostic=diagnostic
            )
            if key in merged:
                collisions += 1
                merged[key] = _merge_annotation_rows(merged[key], annotation)
            else:
                merged[key] = annotation
    return tuple(merged[key] for key in sorted(merged)), collisions


def _annotation_row(
    evaluation: Mapping[str, Any],
    input_result: ClassicalModelInputResult,
    row_id: int,
    *,
    diagnostic: bool,
) -> dict[str, Any]:
    family = _text(evaluation.get("family"))
    prefix = f"cm_{family}_"
    fit_status = _text(evaluation.get("fit_status")) or "unavailable"
    forecast_available = (
        not diagnostic
        and _optional_float(evaluation.get("variance_forecast")) is not None
    )
    origin_time, target_time = _int(
        evaluation.get("origin_bin_end_utc_ms")
    ), _int(evaluation.get("target_bin_end_utc_ms"))
    row: dict[str, Any] = {
        "series_id": _text(evaluation.get("series_id")),
        "period": _text(evaluation.get("period")),
        "row_id": row_id,
    }
    values: dict[str, Any] = {
        "schema_version": VOLATILITY_TRAINING_PROJECTION_SCHEMA_VERSION,
        "input_derivation_id": _text(
            input_result.contract.get("derivation_id")
        ),
        "model_id": _text(evaluation.get("model_id")),
        "specification_code": _int(evaluation.get("specification_code")),
        "input_definition_code": _int(evaluation.get("input_definition_code")),
        "mean_model_code": _int(evaluation.get("mean_model_code")),
        "distribution_code": _int(evaluation.get("distribution_code")),
        "innovation_order": _int(evaluation.get("innovation_order")),
        "variance_order": _int(evaluation.get("variance_order")),
        "scale_factor": _optional_float(evaluation.get("scale_factor")),
        "fit_status_code": VOLATILITY_FIT_STATUS_CODES.get(fit_status, 1),
        "failure_reason_code": VOLATILITY_REASON_CODES.get(
            _text(evaluation.get("fit_reason")), 0
        ),
        "converged": bool(evaluation.get("converged", False)),
        "effective_observation_count": _int(
            evaluation.get("effective_observation_count")
        ),
        "missing_reset_count": _int(evaluation.get("missing_reset_count")),
        "fold_id": _int(evaluation.get("fold_id")),
        "origin_row_id": evaluation.get("origin_row_id"),
        "target_row_id": evaluation.get("target_row_id"),
        "horizon": _int(evaluation.get("horizon")),
        "mean_forecast": (
            None
            if diagnostic
            else _optional_float(evaluation.get("mean_forecast"))
        ),
        "variance_forecast": (
            None
            if diagnostic
            else _optional_float(evaluation.get("variance_forecast"))
        ),
        "volatility_forecast": (
            None
            if diagnostic
            else _optional_float(evaluation.get("volatility_forecast"))
        ),
        "annualized_variance_forecast": (
            None
            if diagnostic
            else _optional_float(evaluation.get("annualized_variance_forecast"))
        ),
        "annualized_volatility_forecast": (
            None
            if diagnostic
            else _optional_float(
                evaluation.get("annualized_volatility_forecast")
            )
        ),
        "forecast_available": forecast_available,
        "forecast_available_at_utc_ms": (
            origin_time if forecast_available else None
        ),
        "actual_return": (
            _optional_float(evaluation.get("actual_return"))
            if diagnostic
            else None
        ),
        "realized_variance_proxy": (
            _optional_float(evaluation.get("realized_variance_proxy"))
            if diagnostic
            else None
        ),
        "mean_error": (
            _optional_float(evaluation.get("mean_error"))
            if diagnostic
            else None
        ),
        "variance_error": (
            _optional_float(evaluation.get("variance_error"))
            if diagnostic
            else None
        ),
        "volatility_error": (
            _optional_float(evaluation.get("volatility_error"))
            if diagnostic
            else None
        ),
        "qlike_loss": (
            _optional_float(evaluation.get("qlike_loss"))
            if diagnostic
            else None
        ),
        "diagnostic_available": diagnostic,
        "diagnostic_available_at_utc_ms": target_time if diagnostic else None,
        "diagnostic_only": diagnostic,
        "persistence": _optional_float(evaluation.get("persistence")),
        "unconditional_variance": _optional_float(
            evaluation.get("unconditional_variance")
        ),
        "boundary_parameter": bool(evaluation.get("boundary_parameter", False)),
        "training_eligible": forecast_available,
    }
    row.update({prefix + suffix: value for suffix, value in values.items()})
    return row


def _merge_annotation_rows(
    current: Mapping[str, Any], incoming: Mapping[str, Any]
) -> dict[str, Any]:
    merged = dict(current)
    flags = {
        name
        for name in VOLATILITY_COLUMNS
        if name.endswith(
            (
                "forecast_available",
                "diagnostic_available",
                "diagnostic_only",
                "training_eligible",
            )
        )
    }
    for name, value in incoming.items():
        if name in flags:
            merged[name] = bool(merged.get(name)) or bool(value)
        elif value is not None:
            merged[name] = value
    return merged


def _training_projection_metadata(
    profile: VolatilityProfile,
    input_result: ClassicalModelInputResult,
    annotation_count: int,
    collision_count: int,
) -> dict[str, JSONValue]:
    return {
        "schema_version": VOLATILITY_TRAINING_PROJECTION_SCHEMA_VERSION,
        "grain": "row",
        "identity_fields": ["series_id", "period", "row_id"],
        "timestamp_is_sole_identity": False,
        "mapping_policy": "first_source_row_at_or_after_availability",
        "collision_policy": "merge_forecast_and_diagnostic_latest_origin_wins",
        "collision_count": collision_count,
        "annotation_count": annotation_count,
        "projection_specification_ids": list(
            profile.projection_specification_ids
        ),
        "projection_horizon": profile.projection_horizon,
        "input_derivation_id": input_result.contract.get("derivation_id"),
        "column_names": list(VOLATILITY_COLUMNS),
        "column_prefixes": ["cm_arch_", "cm_garch_"],
        "forecast_rows_point_in_time_safe": True,
        "diagnostic_rows_retrospective": True,
        "observed_columns_overwritten": False,
    }


def _base_payload(
    input_result: ClassicalModelInputResult,
    fingerprint: Mapping[str, JSONValue],
    profile: VolatilityProfile,
) -> dict[str, JSONValue]:
    contract = input_result.contract
    regularization = _mapping(contract.get("regularization"))
    return {
        "schema_version": VOLATILITY_SCHEMA_VERSION,
        "advisory": True,
        "target_axis": dict(_mapping(contract.get("target_axis"))),
        "reference_fingerprint_id": contract.get("reference_fingerprint_id")
        or fingerprint.get("fingerprint_id"),
        "input_schema_version": CLASSICAL_MODEL_INPUT_SCHEMA_VERSION,
        "input_derivation_id": contract.get("derivation_id"),
        "input_status": contract.get("status"),
        "calculation_basis": "regular_grid_rolling_origin_conditional_variance",
        "configuration": profile.to_metadata(),
        "input_transform_policy": dict(
            _mapping(contract.get("transform_policy"))
        ),
        "input_missingness_policy": {
            "expected_closure_policy": regularization.get(
                "expected_closure_policy"
            ),
            "unexpected_missing_policy": regularization.get(
                "unexpected_missing_policy"
            ),
            "fit_policy": "trailing_contiguous_observations_after_last_missing_bin",
            "missing_values_filled": False,
        },
        "variance_contract": {
            "conditional_variance": True,
            "realized_proxy": profile.realized_variance_proxy,
            "variance_scale": "unscaled_input_squared",
            "volatility_scale": "unscaled_input",
            "mean_metrics_separate": True,
        },
        "asymmetric_extension_registry": cast(
            JSONValue, list(ASYMMETRIC_VOLATILITY_EXTENSION_REGISTRY)
        ),
    }


def _unavailable_result(
    input_result: ClassicalModelInputResult,
    base: Mapping[str, JSONValue],
    reason: str,
    *,
    status: str = "unavailable",
) -> VolatilityResult:
    diagnostics: dict[str, JSONValue] = {
        **dict(base),
        "status": status,
        "reason": reason,
        "limitations": [reason],
        "backend": {
            "provider": "arch",
            "available": False,
            "import_basis": "optional_models_extra",
        },
        "fit_summary": {
            "schema_version": VOLATILITY_FIT_SCHEMA_VERSION,
            "fit_attempt_count": 0,
            "status_counts": {},
            "reason_counts": {reason: 1},
            "failed_fit_count": 0,
            "limited_fit_count": 0,
            "fit_samples": [],
            "backend_exception_text_included": False,
        },
        "evaluation": {
            "schema_version": VOLATILITY_EVALUATION_SCHEMA_VERSION,
            "model_count": 0,
            "fold_count": len(input_result.folds),
            "evaluated_fold_count": 0,
            "models": [],
            "reference_variance_baselines": [],
            "comparison_semantics": "descriptive_shared_folds_separate_mean_and_variance_metrics",
            "automatic_winner": False,
        },
        "training_projection": {
            "schema_version": VOLATILITY_TRAINING_PROJECTION_SCHEMA_VERSION,
            "column_names": list(VOLATILITY_COLUMNS),
            "column_prefixes": ["cm_arch_", "cm_garch_"],
            "annotation_count": 0,
            "observed_columns_overwritten": False,
        },
        "fit_duration_included": False,
    }
    return VolatilityResult(diagnostics, (), input_result)


def _empty_fit(
    status: str,
    reason: str,
    *,
    observed_count: int = 0,
    missing_reset_count: int = 0,
) -> _FitOutcome:
    return _FitOutcome(
        status,
        reason,
        (),
        (),
        {},
        (),
        False,
        observed_count,
        missing_reset_count,
        None,
        None,
        None,
        False,
        {"count": 0, "mean": None, "std": None, "max_abs": None},
        None,
        None,
        None,
    )


def _trailing_contiguous(
    values: Sequence[float | None],
) -> tuple[list[float], int]:
    last_missing = max(
        (index for index, value in enumerate(values) if value is None),
        default=-1,
    )
    trailing = [
        float(value)
        for value in values[last_missing + 1 :]
        if value is not None
    ]
    return trailing, sum(value is None for value in values)


def _boundary_parameter(
    parameters: Mapping[str, float],
    specification: VolatilitySpecification,
    tolerance: float,
) -> bool:
    if any(
        name == "omega" and value <= tolerance
        for name, value in parameters.items()
    ):
        return True
    if any(
        (name.startswith("alpha[") or name.startswith("beta["))
        and value <= tolerance
        for name, value in parameters.items()
    ):
        return True
    return any(
        (
            lower is not None
            and abs(parameters.get(name, math.inf) - lower) <= tolerance
        )
        or (
            upper is not None
            and abs(parameters.get(name, -math.inf) - upper) <= tolerance
        )
        for name, lower, upper in specification.parameter_bounds
    )


def _violates_parameter_bounds(
    parameters: Mapping[str, float],
    bounds: Sequence[tuple[str, float | None, float | None]],
) -> bool:
    return any(
        name not in parameters
        or (lower is not None and parameters[name] < lower)
        or (upper is not None and parameters[name] > upper)
        for name, lower, upper in bounds
    )


def _numeric_summary(
    values: Sequence[float], digits: int
) -> dict[str, JSONValue]:
    if not values:
        return {
            "count": 0,
            "mean": None,
            "std": None,
            "median": None,
            "mad": None,
            "max_abs": None,
        }
    center = sum(values) / len(values)
    med = median(values)
    return {
        "count": len(values),
        "mean": _rounded(center, digits),
        "std": _rounded(
            math.sqrt(
                sum((value - center) ** 2 for value in values) / len(values)
            ),
            digits,
        ),
        "median": _rounded(med, digits),
        "mad": _rounded(median(abs(value - med) for value in values), digits),
        "max_abs": _rounded(max(abs(value) for value in values), digits),
    }


def _finite_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _model_id(
    specification: VolatilitySpecification,
    derivation_id: str,
    backend_version: str,
) -> str:
    payload = {
        "schema_version": VOLATILITY_SCHEMA_VERSION,
        "input_derivation_id": derivation_id,
        "backend": {"provider": "arch", "version": backend_version},
        "configuration": specification.to_metadata(),
    }
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _folds_by_origin(
    folds: Sequence[Mapping[str, Any]],
) -> list[tuple[tuple[Any, ...], list[dict[str, Any]]]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for fold in folds:
        key = (
            _text(fold.get("series_id")),
            _text(fold.get("period")),
            _int(fold.get("training_start_index")),
            _int(fold.get("training_end_index")),
            _int(fold.get("origin_bin_end_utc_ms")),
        )
        grouped.setdefault(key, []).append(dict(fold))
    return [
        (key, sorted(values, key=lambda item: _int(item.get("horizon"))))
        for key, values in sorted(grouped.items())
    ]


def _load_backend() -> _Backend | None:
    try:
        module = importlib.import_module("arch")
        version = importlib.metadata.version("arch")
    except (ImportError, importlib.metadata.PackageNotFoundError):
        return None
    return _Backend(version, module.arch_model)


def _ensure_projection_columns(frame: Any) -> Any:
    import polars as pl

    strings = {"schema_version", "input_derivation_id", "model_id"}
    booleans = {
        "converged",
        "forecast_available",
        "diagnostic_available",
        "diagnostic_only",
        "boundary_parameter",
        "training_eligible",
    }
    floats = {
        "scale_factor",
        "mean_forecast",
        "variance_forecast",
        "volatility_forecast",
        "annualized_variance_forecast",
        "annualized_volatility_forecast",
        "actual_return",
        "realized_variance_proxy",
        "mean_error",
        "variance_error",
        "volatility_error",
        "qlike_loss",
        "persistence",
        "unconditional_variance",
    }
    expressions = []
    for name in VOLATILITY_COLUMNS:
        if name in frame.columns:
            continue
        suffix = name.removeprefix("cm_arch_").removeprefix("cm_garch_")
        dtype = (
            pl.Utf8
            if suffix in strings
            else (
                pl.Boolean
                if suffix in booleans
                else pl.Float64 if suffix in floats else pl.Int64
            )
        )
        expressions.append(pl.lit(None, dtype=dtype).alias(name))
    return frame.with_columns(expressions) if expressions else frame
