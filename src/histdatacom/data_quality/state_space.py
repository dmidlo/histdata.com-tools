"""Explicit structural state-space and Kalman fingerprint diagnostics."""

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
from contextlib import nullcontext
from dataclasses import dataclass, field
from statistics import median
from typing import Any, cast

from histdatacom.data_quality.autoregressive import (
    _covariance_condition_number,
    _first_available_row_id,
    _fit_converged,
    _fitted_parameters,
    _float,
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
from histdatacom.data_quality.exponential_smoothing import (
    _inverse_forecasts,
    _reference_baseline_payloads,
)
from histdatacom.data_quality.limits import bounded_report_limit
from histdatacom.data_quality.seasonal_exogenous import _model_references
from histdatacom.data_quality.training_features import (
    KALMAN_COLUMNS,
    STATE_SPACE_COLUMNS,
    ensure_tick_training_features,
)
from histdatacom.runtime_contracts import JSONValue

STATE_SPACE_SCHEMA_VERSION = "histdatacom.state-space.v1"
STATE_SPACE_CONFIGURATION_SCHEMA_VERSION = (
    "histdatacom.state-space-configuration.v1"
)
STATE_SPACE_FIT_SCHEMA_VERSION = "histdatacom.state-space-fit-result.v1"
STATE_SPACE_STATE_RESULT_SCHEMA_VERSION = "histdatacom.kalman-state-result.v1"
STATE_SPACE_FORECAST_SCHEMA_VERSION = "histdatacom.state-space-forecast.v1"
STATE_SPACE_EVALUATION_SCHEMA_VERSION = "histdatacom.state-space-evaluation.v1"
STATE_SPACE_TRAINING_PROJECTION_SCHEMA_VERSION = (
    "histdatacom.state-space-training-projection.v1"
)
STATE_SPACE_SUMMARY_SCHEMA_VERSION = "histdatacom.state-space-summary.v1"
STATE_SPACE_SUMMARY_METADATA_KEY = "time_series_fingerprint_state_space_summary"
STATE_SPACE_BOUNDED_PAYLOAD_KEY = "fingerprint_state_space"

DEFAULT_STATE_SPACE_SUMMARY_TARGET_LIMIT = 16
DEFAULT_STATE_SPACE_ROLLING_WINDOWS = (5, 20)
DEFAULT_STATE_SPACE_ROUNDING_DIGITS = 12
MAX_STATE_SPACE_SPECIFICATIONS = 32
MAX_STATE_DIMENSION = 256
MAX_COMPONENT_COUNT = 16
MAX_RETAINED_STATES = 64
MAX_FIXED_PARAMETERS = 32

STATE_SPACE_FAMILIES = ("local_level", "local_linear_trend", "structural")
STATE_SPACE_FAMILY_CODES = {
    "local_level": 1,
    "local_linear_trend": 2,
    "structural": 3,
}
STATE_SPACE_INITIALIZATION_CODES = {
    "default": 1,
    "approximate_diffuse": 2,
    "exact_diffuse": 3,
}
STATE_SPACE_FIT_STATUS_CODES = {
    "unavailable": 1,
    "skipped": 2,
    "failed": 3,
    "limited": 4,
    "fitted": 5,
    "converged": 6,
}
STATE_SPACE_REASON_CODES = {
    "": 0,
    "dependency_unavailable": 1,
    "input_contract_unavailable": 2,
    "insufficient_folds": 3,
    "insufficient_history": 4,
    "invalid_configuration": 5,
    "invalid_time_basis": 6,
    "unidentifiable_model": 7,
    "singular_covariance": 8,
    "non_positive_covariance": 9,
    "diffuse_initialization_failure": 10,
    "numerical_instability": 11,
    "missingness_limitation": 12,
    "long_missing_gap": 13,
    "optimizer_failure": 14,
    "resource_limit": 15,
    "timeout": 16,
    "backend_failure": 17,
    "target_unavailable": 18,
    "inverse_transform_unavailable": 19,
    "zero_variance": 20,
}
KALMAN_FILTERED_CALCULATION_BASIS_CODE = 1
KALMAN_SMOOTHED_CALCULATION_BASIS_CODE = 2

_SPECIFICATION_ID = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,63}$")
_PARAMETER_NAME = re.compile(r"^[A-Za-z0-9_.()\[\]-]{1,96}$")
_OPTIMIZERS = {"lbfgs", "bfgs", "powell", "nm", "cg", "ncg"}


@dataclass(frozen=True, slots=True)
class StateSpaceSpecification:
    """One explicit local-level, local-trend, or structural configuration."""

    specification_id: str
    family: str
    irregular: bool = True
    stochastic_level: bool = True
    stochastic_trend: bool = False
    seasonal_period: int = 0
    seasonal_cycle_ms: int = 0
    stochastic_seasonal: bool = True
    cycle: bool = False
    stochastic_cycle: bool = False
    damped_cycle: bool = False
    autoregressive_order: int = 0
    initialization_method: str = "default"
    approximate_diffuse_variance: float = 1_000_000.0
    optimizer: str = "lbfgs"
    fixed_parameters: tuple[tuple[str, float], ...] = ()
    max_iterations: int = 200

    def __post_init__(self) -> None:
        if not _SPECIFICATION_ID.fullmatch(self.specification_id):
            raise ValueError("invalid state-space specification_id")
        if self.family not in STATE_SPACE_FAMILIES:
            raise ValueError("unsupported state-space family")
        if self.family == "local_level":
            if self.stochastic_trend or self.seasonal_period or self.cycle:
                raise ValueError(
                    "local-level models cannot add trend/seasonal/cycle"
                )
            if self.autoregressive_order:
                raise ValueError("local-level models cannot add autoregression")
        if self.family == "local_linear_trend":
            if not self.stochastic_trend:
                raise ValueError("local-linear-trend requires stochastic_trend")
            if self.seasonal_period or self.cycle or self.autoregressive_order:
                raise ValueError(
                    "local-linear-trend is a dedicated trend model"
                )
        if self.family == "structural" and not any(
            (
                self.stochastic_level,
                self.stochastic_trend,
                self.seasonal_period,
                self.cycle,
                self.autoregressive_order,
            )
        ):
            raise ValueError("structural model requires an explicit component")
        if self.seasonal_period:
            if self.seasonal_period < 2 or self.seasonal_period > 100_000:
                raise ValueError("seasonal_period must be between 2 and 100000")
            if self.seasonal_cycle_ms < 1:
                raise ValueError("seasonal_cycle_ms must be explicit")
        elif self.seasonal_cycle_ms:
            raise ValueError("seasonal_cycle_ms requires seasonal_period")
        if self.autoregressive_order < 0 or self.autoregressive_order > 64:
            raise ValueError("autoregressive_order must be between 0 and 64")
        if self.initialization_method not in STATE_SPACE_INITIALIZATION_CODES:
            raise ValueError("unsupported state-space initialization_method")
        if (
            not math.isfinite(self.approximate_diffuse_variance)
            or self.approximate_diffuse_variance <= 0
        ):
            raise ValueError("approximate_diffuse_variance must be positive")
        if self.optimizer not in _OPTIMIZERS:
            raise ValueError("unsupported state-space optimizer")
        if len(self.fixed_parameters) > MAX_FIXED_PARAMETERS:
            raise ValueError("too many fixed state-space parameters")
        seen: set[str] = set()
        for name, value in self.fixed_parameters:
            if (
                not _PARAMETER_NAME.fullmatch(name)
                or name in seen
                or not math.isfinite(value)
            ):
                raise ValueError(
                    "fixed parameters must be unique finite scalars"
                )
            seen.add(name)
        if self.max_iterations < 1:
            raise ValueError("max_iterations must be positive")

    @property
    def level(self) -> bool:
        """Return whether the model includes a level state."""
        return True

    @property
    def trend(self) -> bool:
        """Return whether the model includes a trend state."""
        return self.family == "local_linear_trend" or self.stochastic_trend

    @property
    def component_count(self) -> int:
        """Return the explicitly configured structural component count."""
        return sum(
            bool(value)
            for value in (
                self.level,
                self.trend,
                self.irregular,
                self.seasonal_period,
                self.cycle,
                self.autoregressive_order,
            )
        )

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return stable JSON-compatible configuration metadata."""
        latent_components = ["level"]
        if self.trend:
            latent_components.append("trend")
        if self.seasonal_period:
            latent_components.append("seasonal")
        if self.cycle:
            latent_components.append("cycle")
        if self.autoregressive_order:
            latent_components.append("autoregressive")
        metadata: dict[str, JSONValue] = {
            "schema_version": STATE_SPACE_CONFIGURATION_SCHEMA_VERSION,
            "specification_id": self.specification_id,
            "family": self.family,
            "family_code": STATE_SPACE_FAMILY_CODES[self.family],
            "observable": "cm_input_value",
            "observable_contract": {
                "name": "cm_input_value",
                "scalar": True,
                "missing_supported": True,
                "time_basis": "regular_grid",
            },
            "latent_state_contract": {
                "components": cast(JSONValue, latent_components),
                "bounded_by_profile_state_dimension": True,
            },
            "observation_equation": "y_t = Z_t alpha_t + epsilon_t",
            "transition_equation": "alpha_t = T_t alpha_(t-1) + eta_t",
            "observation_noise": {
                "enabled": self.irregular,
                "estimated_variance": self.irregular,
            },
            "process_noise": {
                "stochastic_level": self.stochastic_level,
                "stochastic_trend": self.stochastic_trend,
                "stochastic_seasonal": (
                    self.stochastic_seasonal and bool(self.seasonal_period)
                ),
                "stochastic_cycle": self.stochastic_cycle and self.cycle,
            },
            "level": self.level,
            "trend": self.trend,
            "irregular": self.irregular,
            "stochastic_level": self.stochastic_level,
            "stochastic_trend": self.stochastic_trend,
            "seasonal_period": self.seasonal_period,
            "seasonal_cycle_ms": self.seasonal_cycle_ms,
            "stochastic_seasonal": self.stochastic_seasonal,
            "cycle": self.cycle,
            "stochastic_cycle": self.stochastic_cycle,
            "damped_cycle": self.damped_cycle,
            "autoregressive_order": self.autoregressive_order,
            "component_count": self.component_count,
            "initialization_method": self.initialization_method,
            "initialization_code": STATE_SPACE_INITIALIZATION_CODES[
                self.initialization_method
            ],
            "approximate_diffuse_variance": self.approximate_diffuse_variance,
            "optimizer": self.optimizer,
            "fixed_parameters": cast(
                JSONValue,
                [
                    {"parameter": name, "value": value}
                    for name, value in self.fixed_parameters
                ],
            ),
            "max_iterations": self.max_iterations,
            "automatic_component_selection": False,
        }
        return metadata


def _default_specifications() -> tuple[StateSpaceSpecification, ...]:
    return (
        StateSpaceSpecification("local-level", "local_level"),
        StateSpaceSpecification(
            "local-linear-trend",
            "local_linear_trend",
            stochastic_trend=True,
        ),
        StateSpaceSpecification(
            "structural-hourly",
            "structural",
            seasonal_period=60,
            seasonal_cycle_ms=3_600_000,
        ),
    )


@dataclass(frozen=True, slots=True)
class StateSpaceProfile:
    """Explicit state-space controls; disabled by default."""

    enabled: bool = False
    specifications: tuple[StateSpaceSpecification, ...] = field(
        default_factory=_default_specifications
    )
    projection_specification_id: str = "local-level"
    projection_horizon: int = 1
    max_state_dimension: int = 64
    max_component_count: int = 8
    max_prediction_only_gap: int = 240
    max_retained_states: int = 16
    baseline_rolling_windows: tuple[int, ...] = (
        DEFAULT_STATE_SPACE_ROLLING_WINDOWS
    )
    compare_exponential_smoothing: bool = True
    compare_autoregressive: bool = True
    compare_seasonal_exogenous: bool = True
    rounding_digits: int = DEFAULT_STATE_SPACE_ROUNDING_DIGITS

    def __post_init__(self) -> None:
        if not self.specifications:
            raise ValueError(
                "at least one state-space specification is required"
            )
        if len(self.specifications) > MAX_STATE_SPACE_SPECIFICATIONS:
            raise ValueError("too many state-space specifications")
        identifiers = tuple(
            item.specification_id for item in self.specifications
        )
        if len(set(identifiers)) != len(identifiers):
            raise ValueError("state-space specification IDs must be unique")
        if self.projection_specification_id not in identifiers:
            raise ValueError("projection ID must select a configured model")
        if self.projection_horizon < 1:
            raise ValueError("projection_horizon must be positive")
        if not 1 <= self.max_state_dimension <= MAX_STATE_DIMENSION:
            raise ValueError("max_state_dimension must be between 1 and 256")
        if not 1 <= self.max_component_count <= MAX_COMPONENT_COUNT:
            raise ValueError("max_component_count must be between 1 and 16")
        if self.max_prediction_only_gap < 0:
            raise ValueError("max_prediction_only_gap must be non-negative")
        if not 1 <= self.max_retained_states <= MAX_RETAINED_STATES:
            raise ValueError("max_retained_states must be between 1 and 64")
        if (
            not self.baseline_rolling_windows
            or any(value < 1 for value in self.baseline_rolling_windows)
            or tuple(sorted(set(self.baseline_rolling_windows)))
            != self.baseline_rolling_windows
        ):
            raise ValueError("baseline windows must be sorted unique positives")
        if not 0 <= self.rounding_digits <= 16:
            raise ValueError("rounding_digits must be between 0 and 16")

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return stable profile metadata."""
        return {
            "enabled": self.enabled,
            "specifications": cast(
                JSONValue, [item.to_metadata() for item in self.specifications]
            ),
            "projection_specification_id": self.projection_specification_id,
            "projection_horizon": self.projection_horizon,
            "max_state_dimension": self.max_state_dimension,
            "max_component_count": self.max_component_count,
            "max_prediction_only_gap": self.max_prediction_only_gap,
            "max_retained_states": self.max_retained_states,
            "baseline_rolling_windows": list(self.baseline_rolling_windows),
            "compare_exponential_smoothing": self.compare_exponential_smoothing,
            "compare_autoregressive": self.compare_autoregressive,
            "compare_seasonal_exogenous": self.compare_seasonal_exogenous,
            "rounding_digits": self.rounding_digits,
            "automatic_component_selection": False,
            "automatic_winner": False,
            "full_series_smoothing_projection": False,
        }


@dataclass(frozen=True, slots=True)
class StateSpaceResult:
    """Bounded diagnostics plus durable row-key annotations."""

    diagnostics: Mapping[str, JSONValue]
    annotations: tuple[Mapping[str, Any], ...]
    input_result: ClassicalModelInputResult


@dataclass(frozen=True, slots=True)
class _Backend:
    version: str
    unobserved_components: Any


@dataclass(frozen=True, slots=True)
class _FitOutcome:
    status: str
    reason: str
    forecasts: tuple[float, ...]
    standard_errors: tuple[float, ...]
    lower_bounds: tuple[float, ...]
    upper_bounds: tuple[float, ...]
    parameters: Mapping[str, float]
    warning_codes: tuple[str, ...]
    converged: bool
    state_dimension: int
    state_names: tuple[str, ...]
    filtered_state: tuple[float | None, ...]
    filtered_variance: tuple[float | None, ...]
    smoothed_state: tuple[float | None, ...]
    smoothed_variance: tuple[float | None, ...]
    effective_observation_count: int
    missing_observation_count: int
    prediction_only_transition_count: int
    max_prediction_only_gap: int
    log_likelihood: float | None
    aic: float | None
    bic: float | None
    covariance_condition_number: float | None
    innovation_summary: Mapping[str, JSONValue]


def state_space_from_training_frame(
    frame: Any | None,
    fingerprint: Mapping[str, JSONValue],
    *,
    input_profile: ClassicalModelInputProfile | None = None,
    profile: StateSpaceProfile | None = None,
    exponential_smoothing: Mapping[str, JSONValue] | None = None,
    autoregressive: Mapping[str, JSONValue] | None = None,
    seasonal_exogenous: Mapping[str, JSONValue] | None = None,
    target: Any | None = None,
) -> StateSpaceResult:
    """Regularize an enriched tick frame and evaluate configured models."""
    selected_input = input_profile or ClassicalModelInputProfile(enabled=True)
    input_result = build_classical_model_input(
        frame, fingerprint, profile=selected_input, target=target
    )
    return state_space_from_model_input(
        frame,
        input_result,
        fingerprint,
        input_profile=selected_input,
        profile=profile,
        exponential_smoothing=exponential_smoothing,
        autoregressive=autoregressive,
        seasonal_exogenous=seasonal_exogenous,
        target=target,
    )


def state_space_from_model_input(
    frame: Any | None,
    input_result: ClassicalModelInputResult,
    fingerprint: Mapping[str, JSONValue],
    *,
    input_profile: ClassicalModelInputProfile,
    profile: StateSpaceProfile | None = None,
    exponential_smoothing: Mapping[str, JSONValue] | None = None,
    autoregressive: Mapping[str, JSONValue] | None = None,
    seasonal_exogenous: Mapping[str, JSONValue] | None = None,
    target: Any | None = None,
) -> StateSpaceResult:
    """Fit explicit structural models on shared rolling-origin folds."""
    selected = profile or StateSpaceProfile(enabled=True)
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
        exponential_smoothing=exponential_smoothing,
        autoregressive=autoregressive,
        seasonal_exogenous=seasonal_exogenous,
        target=target,
    )


def state_space_diagnostics_from_training_frame(
    frame: Any | None,
    fingerprint: Mapping[str, JSONValue],
    *,
    input_profile: ClassicalModelInputProfile | None = None,
    profile: StateSpaceProfile | None = None,
    target: Any | None = None,
) -> dict[str, JSONValue]:
    """Return diagnostics without exposing the annotation wrapper."""
    return dict(
        state_space_from_training_frame(
            frame,
            fingerprint,
            input_profile=input_profile,
            profile=profile,
            target=target,
        ).diagnostics
    )


def project_state_space_onto_training_frame(
    frame: Any,
    result: StateSpaceResult,
    *,
    target: Any | None = None,
) -> Any:
    """Join state-space/Kalman annotations by durable row identity."""
    import polars as pl

    columns = set(getattr(frame, "columns", ()))
    if not {"series_id", "period", "row_id"}.issubset(columns):
        enriched = ensure_tick_training_features(frame, target=target)
    else:
        enriched = frame
    projection_columns = (*STATE_SPACE_COLUMNS, *KALMAN_COLUMNS)
    left = enriched.drop(
        [name for name in projection_columns if name in enriched.columns]
    ).with_row_index("__cm_state_space_original_order")
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
    return projected.sort("__cm_state_space_original_order").drop(
        "__cm_state_space_original_order"
    )


def state_space_summary(
    findings: Iterable[QualityFinding],
    *,
    target_limit: int | None = DEFAULT_STATE_SPACE_SUMMARY_TARGET_LIMIT,
) -> dict[str, JSONValue] | None:
    """Return bounded report metadata for structural-model results."""
    targets: list[dict[str, JSONValue]] = []
    statuses: Counter[str] = Counter()
    for finding in findings:
        fingerprint = _mapping(finding.metadata.get("time_series_fingerprint"))
        payload = _mapping(fingerprint.get("state_space"))
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
        default_limit=DEFAULT_STATE_SPACE_SUMMARY_TARGET_LIMIT,
        allow_unbounded=True,
    )
    included = limit.slice(targets)
    omitted = len(targets) - len(included)
    return {
        "schema_version": STATE_SPACE_SUMMARY_SCHEMA_VERSION,
        "advisory": True,
        "target_count": len(targets),
        "included_target_count": len(included),
        "omitted_target_count": omitted,
        "truncated": omitted > 0,
        "status_counts": dict(sorted(statuses.items())),
        "target_summaries": cast(JSONValue, included),
        "limit_metadata": {"targets": limit.limit_payload()},
    }


def format_state_space_summary_lines(
    summary: Mapping[str, JSONValue] | None,
) -> tuple[str, ...]:
    """Return concise human-readable structural-model lines."""
    if not summary:
        return ()
    statuses = _mapping(summary.get("status_counts"))
    lines = [
        "",
        "State-space and Kalman models",
        (
            f"targets: {_int(summary.get('target_count'))} "
            f"ready: {_int(statuses.get('ready'))} "
            f"limited: {_int(statuses.get('limited'))} "
            f"unavailable: {_int(statuses.get('unavailable'))}"
        ),
    ]
    for item in _mapping_rows(summary.get("target_summaries")):
        axis = _mapping(item.get("target_axis"))
        label = "/".join(
            _text(axis.get(key))
            for key in ("data_format", "timeframe", "symbol", "period")
        )
        lines.append(
            f"- {label}: {_text(item.get('status'))} "
            f"models={_int(item.get('model_count'))} "
            f"fits={_int(item.get('fit_attempt_count'))} "
            f"folds={_int(item.get('evaluated_fold_count'))}"
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
    profile: StateSpaceProfile,
    backend: _Backend,
    *,
    exponential_smoothing: Mapping[str, JSONValue] | None,
    autoregressive: Mapping[str, JSONValue] | None,
    seasonal_exogenous: Mapping[str, JSONValue] | None,
    target: Any | None,
) -> StateSpaceResult:
    rows = cast(list[dict[str, Any]], input_result.regularized_frame.to_dicts())
    folds = [dict(fold) for fold in input_result.folds]
    origins = _folds_by_origin(folds)
    resources = input_profile.resources
    specifications = profile.specifications[: resources.max_candidate_orders]
    limitations: list[str] = []
    estimated_memory = _estimated_memory(len(rows), len(folds), specifications)
    if len(specifications) < len(profile.specifications):
        limitations.append("resource_limit")
    if estimated_memory > resources.max_memory_bytes:
        limitations.append("resource_limit")
        specifications = ()
    started = time.monotonic()
    fit_attempt_count = 0
    all_evaluations: list[dict[str, Any]] = []
    model_payloads: list[dict[str, JSONValue]] = []
    all_fit_samples: list[dict[str, JSONValue]] = []
    fit_statuses: Counter[str] = Counter()
    fit_reasons: Counter[str] = Counter()
    warning_counts: Counter[str] = Counter()
    for specification_code, specification in enumerate(specifications, start=1):
        configuration_reason = _configuration_reason(
            specification, input_profile, profile
        )
        model_id = _model_id(
            specification,
            _text(input_result.contract.get("derivation_id")),
            backend.version,
        )
        model_evaluations: list[dict[str, Any]] = []
        model_fit_samples: list[dict[str, JSONValue]] = []
        for _, origin_folds in origins:
            if fit_attempt_count >= resources.max_fit_attempts:
                limitations.append("resource_limit")
                break
            if time.monotonic() - started >= resources.max_wall_time_seconds:
                limitations.append("timeout")
                break
            fit_attempt_count += 1
            origin = origin_folds[0]
            start_index = _int(origin.get("training_start_index"))
            end_index = _int(origin.get("training_end_index"))
            indexes = tuple(range(start_index, end_index + 1))
            values = tuple(
                _optional_float(rows[index].get("cm_input_value"))
                for index in indexes
            )
            max_horizon = max(
                _int(fold.get("horizon")) for fold in origin_folds
            )
            if configuration_reason:
                outcome = _empty_fit(
                    "skipped",
                    configuration_reason,
                    observed_count=sum(value is not None for value in values),
                    missing_count=sum(value is None for value in values),
                    max_gap=_max_missing_run(values),
                )
            else:
                outcome = _fit_specification(
                    specification,
                    values,
                    max_horizon,
                    profile,
                    backend,
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
                indexes,
                len(values),
                profile,
            )
            model_fit_samples.append(fit_sample)
            all_fit_samples.append(fit_sample)
            original_forecasts = _inverse_forecasts(
                rows,
                end_index,
                outcome.forecasts,
                input_profile,
                profile.rounding_digits,
            )
            for fold in origin_folds:
                evaluation = _fold_evaluation(
                    rows,
                    fold,
                    specification,
                    specification_code,
                    model_id,
                    outcome,
                    original_forecasts,
                    input_profile,
                    profile.rounding_digits,
                    profile.max_retained_states,
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
    baselines = _reference_baseline_payloads(
        rows,
        folds,
        profile.baseline_rolling_windows,
        profile.rounding_digits,
    )
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
            "provider": "statsmodels",
            "version": backend.version,
            "available": True,
            "model_class": "statsmodels.tsa.statespace.UnobservedComponents",
            "import_basis": "optional_models_extra",
        },
        "fit_summary": {
            "schema_version": STATE_SPACE_FIT_SCHEMA_VERSION,
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
                JSONValue,
                all_fit_samples[: resources.max_retained_diagnostics],
            ),
            "fit_samples_truncated": (
                len(all_fit_samples) > resources.max_retained_diagnostics
            ),
        },
        "resource_usage": {
            "limits": resources.to_metadata(),
            "state_space_limits": {
                "max_state_dimension": profile.max_state_dimension,
                "max_component_count": profile.max_component_count,
                "max_prediction_only_gap": profile.max_prediction_only_gap,
                "max_retained_states": profile.max_retained_states,
            },
            "estimated_working_memory_bytes": estimated_memory,
            "memory_limit_exceeded": (
                estimated_memory > resources.max_memory_bytes
            ),
            "fit_attempt_count": fit_attempt_count,
            "wall_time_limit_enforced": True,
            "wall_time_observed_in_payload": False,
        },
        "evaluation": {
            "schema_version": STATE_SPACE_EVALUATION_SCHEMA_VERSION,
            "calculation_basis": "regular_grid_rolling_origin_kalman_filter",
            "original_scale": True,
            "model_count": len(model_payloads),
            "fold_count": len(folds),
            "evaluated_fold_count": evaluated_count,
            "skipped_evaluation_count": (
                len(all_evaluations) - evaluated_count
            ),
            "forecast_coverage_rate": _rate(
                evaluated_count,
                len(all_evaluations),
                profile.rounding_digits,
            ),
            "models": cast(JSONValue, model_payloads),
            "reference_baselines": cast(JSONValue, baselines),
            "reference_models": cast(JSONValue, references),
            "comparison_semantics": "descriptive_shared_folds_only",
            "automatic_winner": False,
        },
        "training_projection": _training_projection_metadata(
            profile, input_result, len(annotations), collisions
        ),
        "fit_duration_included": False,
    }
    return StateSpaceResult(diagnostics, annotations, input_result)


def _configuration_reason(
    specification: StateSpaceSpecification,
    input_profile: ClassicalModelInputProfile,
    profile: StateSpaceProfile,
) -> str:
    if specification.seasonal_period:
        expected = specification.seasonal_period * input_profile.frequency_ms
        if expected != specification.seasonal_cycle_ms:
            return "invalid_time_basis"
    if specification.component_count > profile.max_component_count:
        return "resource_limit"
    return ""


def _fit_specification(
    specification: StateSpaceSpecification,
    values: Sequence[float | None],
    horizon: int,
    profile: StateSpaceProfile,
    backend: _Backend,
) -> _FitOutcome:
    observed_count = sum(value is not None for value in values)
    missing_count = len(values) - observed_count
    max_gap = _max_missing_run(values)
    finite = [float(value) for value in values if value is not None]
    minimum = max(8, specification.component_count * 3)
    if observed_count < minimum:
        return _empty_fit(
            "skipped",
            "insufficient_history",
            observed_count=observed_count,
            missing_count=missing_count,
            max_gap=max_gap,
        )
    if max(finite) - min(finite) <= 1e-15:
        return _empty_fit(
            "skipped",
            "zero_variance",
            observed_count=observed_count,
            missing_count=missing_count,
            max_gap=max_gap,
        )
    if max_gap > profile.max_prediction_only_gap:
        return _empty_fit(
            "skipped",
            "long_missing_gap",
            observed_count=observed_count,
            missing_count=missing_count,
            max_gap=max_gap,
        )
    try:
        numpy = importlib.import_module("numpy")
        endog = numpy.asarray(
            [numpy.nan if value is None else float(value) for value in values],
            dtype=float,
        )
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            model = backend.unobserved_components(
                endog,
                level=True,
                trend=specification.trend,
                seasonal=specification.seasonal_period or None,
                cycle=specification.cycle,
                autoregressive=specification.autoregressive_order or None,
                irregular=specification.irregular,
                stochastic_level=specification.stochastic_level,
                stochastic_trend=specification.stochastic_trend,
                stochastic_seasonal=specification.stochastic_seasonal,
                stochastic_cycle=specification.stochastic_cycle,
                damped_cycle=specification.damped_cycle,
                use_exact_diffuse=(
                    specification.initialization_method == "exact_diffuse"
                ),
            )
            if specification.initialization_method == "approximate_diffuse":
                model.initialize_approximate_diffuse(
                    specification.approximate_diffuse_variance
                )
            if model.k_states > profile.max_state_dimension:
                return _empty_fit(
                    "skipped",
                    "resource_limit",
                    observed_count=observed_count,
                    missing_count=missing_count,
                    max_gap=max_gap,
                    state_dimension=int(model.k_states),
                    state_names=tuple(str(name) for name in model.state_names),
                )
            fixed = dict(specification.fixed_parameters)
            unknown = sorted(set(fixed) - set(model.param_names))
            if unknown:
                return _empty_fit(
                    "skipped",
                    "invalid_configuration",
                    observed_count=observed_count,
                    missing_count=missing_count,
                    max_gap=max_gap,
                    state_dimension=int(model.k_states),
                    state_names=tuple(str(name) for name in model.state_names),
                )
            context = model.fix_params(fixed) if fixed else nullcontext()
            with context:
                result = model.fit(
                    method=specification.optimizer,
                    maxiter=specification.max_iterations,
                    disp=False,
                    low_memory=False,
                )
            forecast = result.get_forecast(steps=horizon)
        predicted = tuple(
            _finite_or_none(item) for item in forecast.predicted_mean
        )
        standard_errors = tuple(
            _finite_or_none(item) for item in forecast.se_mean
        )
        intervals = forecast.conf_int(alpha=0.05)
        lower = tuple(_finite_or_none(row[0]) for row in intervals)
        upper = tuple(_finite_or_none(row[1]) for row in intervals)
        filtered_state = _last_vector(result.filtered_state)
        smoothed_state = _last_vector(result.smoothed_state)
        filtered_variance = _last_covariance_diagonal(result.filtered_state_cov)
        smoothed_variance = _last_covariance_diagonal(result.smoothed_state_cov)
        covariance_condition = _covariance_condition_number(result)
        if (
            any(value is None for value in predicted)
            or any(value is None for value in standard_errors)
            or any(value is None for value in lower)
            or any(value is None for value in upper)
            or not filtered_state
            or all(value is None for value in filtered_state)
            or not _states_are_valid(
                (
                    *filtered_state,
                    *smoothed_state,
                    *filtered_variance,
                    *smoothed_variance,
                )
            )
        ):
            return _empty_fit(
                "failed",
                "numerical_instability",
                observed_count=observed_count,
                missing_count=missing_count,
                max_gap=max_gap,
                state_dimension=int(model.k_states),
                state_names=tuple(str(name) for name in model.state_names),
            )
        if any(
            value is not None and value < -1e-10 for value in filtered_variance
        ):
            return _empty_fit(
                "failed",
                "non_positive_covariance",
                observed_count=observed_count,
                missing_count=missing_count,
                max_gap=max_gap,
                state_dimension=int(model.k_states),
                state_names=tuple(str(name) for name in model.state_names),
            )
        warning_codes = _warning_codes(caught)
        converged = _fit_converged(result, warning_codes)
        status = "converged" if converged else "limited"
        reason = "" if converged else "optimizer_failure"
        return _FitOutcome(
            status=status,
            reason=reason,
            forecasts=tuple(float(cast(float, value)) for value in predicted),
            standard_errors=tuple(
                float(cast(float, value)) for value in standard_errors
            ),
            lower_bounds=tuple(float(cast(float, value)) for value in lower),
            upper_bounds=tuple(float(cast(float, value)) for value in upper),
            parameters=_fitted_parameters(result),
            warning_codes=warning_codes,
            converged=converged,
            state_dimension=int(model.k_states),
            state_names=tuple(str(name) for name in model.state_names),
            filtered_state=filtered_state,
            filtered_variance=filtered_variance,
            smoothed_state=smoothed_state,
            smoothed_variance=smoothed_variance,
            effective_observation_count=int(
                getattr(result, "nobs_effective", observed_count)
            ),
            missing_observation_count=missing_count,
            prediction_only_transition_count=missing_count,
            max_prediction_only_gap=max_gap,
            log_likelihood=_optional_float(getattr(result, "llf", None)),
            aic=_optional_float(getattr(result, "aic", None)),
            bic=_optional_float(getattr(result, "bic", None)),
            covariance_condition_number=covariance_condition,
            innovation_summary=_innovation_summary(
                result, profile.rounding_digits
            ),
        )
    except Exception as exc:  # backend failures are isolated and normalized
        return _empty_fit(
            "failed",
            _backend_failure_reason(exc),
            observed_count=observed_count,
            missing_count=missing_count,
            max_gap=max_gap,
        )


def _empty_fit(
    status: str,
    reason: str,
    *,
    observed_count: int,
    missing_count: int,
    max_gap: int,
    state_dimension: int = 0,
    state_names: tuple[str, ...] = (),
) -> _FitOutcome:
    return _FitOutcome(
        status=status,
        reason=reason,
        forecasts=(),
        standard_errors=(),
        lower_bounds=(),
        upper_bounds=(),
        parameters={},
        warning_codes=(),
        converged=False,
        state_dimension=state_dimension,
        state_names=state_names,
        filtered_state=(),
        filtered_variance=(),
        smoothed_state=(),
        smoothed_variance=(),
        effective_observation_count=observed_count,
        missing_observation_count=missing_count,
        prediction_only_transition_count=missing_count,
        max_prediction_only_gap=max_gap,
        log_likelihood=None,
        aic=None,
        bic=None,
        covariance_condition_number=None,
        innovation_summary={},
    )


def _retained_state_count(outcome: _FitOutcome, limit: int) -> int:
    """Return the common bounded prefix with complete state diagnostics."""
    return min(
        limit,
        len(outcome.state_names),
        len(outcome.filtered_state),
        len(outcome.filtered_variance),
        len(outcome.smoothed_state),
        len(outcome.smoothed_variance),
    )


def _fit_sample(
    outcome: _FitOutcome,
    specification: StateSpaceSpecification,
    model_id: str,
    fold: Mapping[str, Any],
    indexes: Sequence[int],
    grid_observation_count: int,
    profile: StateSpaceProfile,
) -> dict[str, JSONValue]:
    retained = _retained_state_count(outcome, profile.max_retained_states)
    states = [
        {
            "name": outcome.state_names[index],
            "filtered": outcome.filtered_state[index],
            "filtered_variance": outcome.filtered_variance[index],
            "smoothed": outcome.smoothed_state[index],
            "smoothed_variance": outcome.smoothed_variance[index],
        }
        for index in range(retained)
    ]
    return {
        "schema_version": STATE_SPACE_FIT_SCHEMA_VERSION,
        "state_schema_version": STATE_SPACE_STATE_RESULT_SCHEMA_VERSION,
        "model_id": model_id,
        "specification_id": specification.specification_id,
        "family": specification.family,
        "status": outcome.status,
        "reason": outcome.reason or None,
        "converged": outcome.converged,
        "state_dimension": outcome.state_dimension,
        "state_names": list(outcome.state_names[:retained]),
        "states": cast(JSONValue, states),
        "states_truncated": retained > 0
        and len(outcome.state_names) > retained,
        "filtered_calculation_basis": "kalman_filter_origin_training_segment",
        "smoothed_calculation_basis": (
            "kalman_smoother_origin_training_segment_retrospective"
        ),
        "smoothing_used_for_forecast": False,
        "full_series_smoothing_used": False,
        "series_id": _text(fold.get("series_id")),
        "period": _text(fold.get("period")),
        "origin_bin_end_utc_ms": _int(fold.get("origin_bin_end_utc_ms")),
        "grid_observation_count": grid_observation_count,
        "effective_observation_count": outcome.effective_observation_count,
        "missing_observation_count": outcome.missing_observation_count,
        "prediction_only_transition_count": (
            outcome.prediction_only_transition_count
        ),
        "max_prediction_only_gap": outcome.max_prediction_only_gap,
        "transition_elapsed_time_basis": "one_regular_grid_step",
        "segment_start_index": indexes[0] if indexes else None,
        "segment_end_index": indexes[-1] if indexes else None,
        "parameters": dict(outcome.parameters),
        "parameter_count": len(outcome.parameters),
        "log_likelihood": _rounded(
            outcome.log_likelihood, profile.rounding_digits
        ),
        "aic": _rounded(outcome.aic, profile.rounding_digits),
        "bic": _rounded(outcome.bic, profile.rounding_digits),
        "covariance_condition_number": _rounded(
            outcome.covariance_condition_number, profile.rounding_digits
        ),
        "innovation_summary": dict(outcome.innovation_summary),
        "warning_codes": list(outcome.warning_codes),
        "fit_duration_included": False,
        "backend_exception_text_included": False,
    }


def _fold_evaluation(
    rows: Sequence[Mapping[str, Any]],
    fold: Mapping[str, Any],
    specification: StateSpaceSpecification,
    specification_code: int,
    model_id: str,
    outcome: _FitOutcome,
    original_forecasts: Sequence[float | None],
    input_profile: ClassicalModelInputProfile,
    rounding_digits: int,
    retained_state_limit: int,
) -> dict[str, Any]:
    horizon = _int(fold.get("horizon"))
    forecast = (
        original_forecasts[horizon - 1]
        if 0 < horizon <= len(original_forecasts)
        else None
    )
    transformed = (
        outcome.forecasts[horizon - 1]
        if 0 < horizon <= len(outcome.forecasts)
        else None
    )
    target_index = _int(fold.get("target_index"))
    actual = (
        _optional_float(rows[target_index].get("cm_input_observed_value"))
        if 0 <= target_index < len(rows)
        else None
    )
    if outcome.status in {"failed", "skipped", "unavailable"}:
        status, reason = "not_evaluated", outcome.reason
    elif fold.get("status") != "valid" or actual is None:
        status, reason = "skipped", "target_unavailable"
    elif forecast is None:
        status, reason = "skipped", "inverse_transform_unavailable"
    else:
        status, reason = "evaluated", ""
    error = (
        forecast - actual
        if status == "evaluated" and forecast is not None and actual is not None
        else None
    )
    retained = _retained_state_count(outcome, retained_state_limit)
    return {
        "schema_version": STATE_SPACE_FORECAST_SCHEMA_VERSION,
        "status": status,
        "reason": reason or None,
        "series_id": _text(fold.get("series_id")),
        "period": _text(fold.get("period")),
        "model_id": model_id,
        "specification_id": specification.specification_id,
        "specification_code": specification_code,
        "family": specification.family,
        "family_code": STATE_SPACE_FAMILY_CODES[specification.family],
        "state_dimension": outcome.state_dimension,
        "component_count": specification.component_count,
        "initialization_code": STATE_SPACE_INITIALIZATION_CODES[
            specification.initialization_method
        ],
        "fit_status": outcome.status,
        "fit_reason": outcome.reason or None,
        "converged": outcome.converged,
        "effective_observation_count": outcome.effective_observation_count,
        "missing_observation_count": outcome.missing_observation_count,
        "prediction_only_transition_count": (
            outcome.prediction_only_transition_count
        ),
        "max_prediction_only_gap": outcome.max_prediction_only_gap,
        "fold_id": _int(fold.get("fold_id")),
        "origin_row_id": fold.get("origin_row_id"),
        "target_row_id": fold.get("target_row_id"),
        "origin_bin_end_utc_ms": _int(fold.get("origin_bin_end_utc_ms")),
        "target_bin_end_utc_ms": _int(fold.get("target_bin_end_utc_ms")),
        "horizon": horizon,
        "transformed_forecast": _rounded(transformed, rounding_digits),
        "forecast": _rounded(forecast, rounding_digits),
        "forecast_standard_error": _sequence_value(
            outcome.standard_errors, horizon, rounding_digits
        ),
        "forecast_lower": _sequence_value(
            outcome.lower_bounds, horizon, rounding_digits
        ),
        "forecast_upper": _sequence_value(
            outcome.upper_bounds, horizon, rounding_digits
        ),
        "uncertainty_scale": (
            "original" if input_profile.transform == "level" else "transformed"
        ),
        "actual": _rounded(actual, rounding_digits),
        "error": _rounded(error, rounding_digits),
        "absolute_error": _rounded(
            abs(error) if error is not None else None, rounding_digits
        ),
        "squared_error": _rounded(
            error * error if error is not None else None, rounding_digits
        ),
        "state_names": list(outcome.state_names[:retained]),
        "filtered_state": list(outcome.filtered_state[:retained]),
        "filtered_variance": list(outcome.filtered_variance[:retained]),
        "smoothed_state": list(outcome.smoothed_state[:retained]),
        "smoothed_variance": list(outcome.smoothed_variance[:retained]),
        "states_truncated": retained > 0
        and len(outcome.state_names) > retained,
        "filtered_state_available_at_origin": retained > 0,
        "smoothed_state_retrospective": retained > 0,
        "smoothed_state_used_for_forecast": False,
        "full_series_smoothing_used": False,
        "original_scale": True,
        "future_values_visible": False,
        "automatic_winner": False,
    }


def _model_evaluation_payload(
    specification: StateSpaceSpecification,
    specification_code: int,
    model_id: str,
    evaluations: Sequence[Mapping[str, Any]],
    fit_samples: Sequence[Mapping[str, JSONValue]],
    retained_limit: int,
    rounding_digits: int,
) -> dict[str, JSONValue]:
    statuses = Counter(_text(row.get("status")) for row in evaluations)
    fit_statuses = Counter(_text(row.get("status")) for row in fit_samples)
    reasons = Counter(
        _text(row.get("reason"))
        for row in evaluations
        if _text(row.get("reason"))
    )
    horizons = sorted({_int(row.get("horizon")) for row in evaluations})
    return {
        "model_id": model_id,
        "specification_id": specification.specification_id,
        "specification_code": specification_code,
        "family": specification.family,
        "configuration": specification.to_metadata(),
        "status": "ready" if statuses["evaluated"] else "limited",
        "fit_status_counts": dict(sorted(fit_statuses.items())),
        "evaluation_status_counts": dict(sorted(statuses.items())),
        "reason_counts": dict(sorted(reasons.items())),
        "forecast_coverage_rate": _rate(
            statuses["evaluated"], len(evaluations), rounding_digits
        ),
        "convergence_rate": _rate(
            fit_statuses["converged"], len(fit_samples), rounding_digits
        ),
        "horizon_metrics": cast(
            JSONValue,
            [
                _metrics_for_horizon(evaluations, horizon, rounding_digits)
                for horizon in horizons
            ],
        ),
        "parameter_stability": _parameter_stability(
            fit_samples, rounding_digits
        ),
        "uncertainty_summary": _uncertainty_summary(
            evaluations, rounding_digits
        ),
        "fold_results": cast(
            JSONValue, [dict(row) for row in evaluations[:retained_limit]]
        ),
        "fold_results_truncated": len(evaluations) > retained_limit,
        "fit_samples": cast(
            JSONValue, [dict(row) for row in fit_samples[:retained_limit]]
        ),
        "fit_samples_truncated": len(fit_samples) > retained_limit,
        "automatic_winner": False,
    }


def _metrics_for_horizon(
    evaluations: Sequence[Mapping[str, Any]],
    horizon: int,
    rounding_digits: int,
) -> dict[str, JSONValue]:
    selected = [
        row
        for row in evaluations
        if _int(row.get("horizon")) == horizon
        and row.get("status") == "evaluated"
    ]
    errors = [_float(row.get("error")) for row in selected]
    absolute = [abs(value) for value in errors]
    squared = [value * value for value in errors]
    return {
        "horizon": horizon,
        "evaluation_count": len(selected),
        "mae": _rounded(
            sum(absolute) / len(absolute) if absolute else None,
            rounding_digits,
        ),
        "rmse": _rounded(
            math.sqrt(sum(squared) / len(squared)) if squared else None,
            rounding_digits,
        ),
        "bias": _rounded(
            sum(errors) / len(errors) if errors else None,
            rounding_digits,
        ),
        "original_scale": True,
    }


def _parameter_stability(
    fit_samples: Sequence[Mapping[str, JSONValue]], rounding_digits: int
) -> dict[str, JSONValue]:
    parameters: dict[str, list[float]] = {}
    for sample in fit_samples:
        for name, raw in _mapping(sample.get("parameters")).items():
            value = _optional_float(raw)
            if value is not None:
                parameters.setdefault(name, []).append(value)
    summaries: dict[str, JSONValue] = {}
    for name, values in sorted(parameters.items()):
        center = median(values)
        summaries[name] = {
            "count": len(values),
            "min": _rounded(min(values), rounding_digits),
            "max": _rounded(max(values), rounding_digits),
            "median": _rounded(center, rounding_digits),
            "mad": _rounded(
                median(abs(value - center) for value in values),
                rounding_digits,
            ),
        }
    return {
        "parameter_count": len(summaries),
        "parameters": summaries,
        "bounded": True,
    }


def _uncertainty_summary(
    evaluations: Sequence[Mapping[str, Any]], rounding_digits: int
) -> dict[str, JSONValue]:
    values = [
        value
        for row in evaluations
        if (value := _optional_float(row.get("forecast_standard_error")))
        is not None
    ]
    return _numeric_summary(values, rounding_digits)


def _innovation_summary(result: Any, digits: int) -> dict[str, JSONValue]:
    try:
        raw = result.filter_results.forecasts_error[0]
    except (AttributeError, IndexError, TypeError):
        return _numeric_summary((), digits)
    values = [
        value for item in raw if (value := _optional_float(item)) is not None
    ]
    return _numeric_summary(values, digits)


def _numeric_summary(
    values: Sequence[float], digits: int
) -> dict[str, JSONValue]:
    if not values:
        return {"count": 0, "mean": None, "std": None, "max_abs": None}
    center = sum(values) / len(values)
    variance = sum((value - center) ** 2 for value in values) / len(values)
    return {
        "count": len(values),
        "mean": _rounded(center, digits),
        "std": _rounded(math.sqrt(variance), digits),
        "max_abs": _rounded(max(abs(value) for value in values), digits),
    }


def _build_annotations(
    frame: Any | None,
    evaluations: Sequence[Mapping[str, Any]],
    profile: StateSpaceProfile,
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
    source_rows = cast(list[dict[str, Any]], enriched.to_dicts())
    availability: dict[tuple[str, str], list[tuple[int, int]]] = {}
    for row in source_rows:
        timestamp = _optional_int(row.get("timestamp_utc_ms"))
        row_id = _optional_int(row.get("row_id"))
        if timestamp is not None and row_id is not None:
            availability.setdefault(
                (_text(row.get("series_id")), _text(row.get("period"))), []
            ).append((timestamp, row_id))
    for values in availability.values():
        values.sort()
    merged: dict[tuple[str, str, int], dict[str, Any]] = {}
    selected = [
        row
        for row in evaluations
        if _text(row.get("specification_id"))
        == profile.projection_specification_id
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
    collisions = 0
    for evaluation in selected:
        group = (
            _text(evaluation.get("series_id")),
            _text(evaluation.get("period")),
        )
        for diagnostic, time_key in (
            (False, "origin_bin_end_utc_ms"),
            (True, "target_bin_end_utc_ms"),
        ):
            if diagnostic and _optional_float(evaluation.get("error")) is None:
                continue
            row_id = _first_available_row_id(
                availability.get(group, ()), _int(evaluation.get(time_key))
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


def _merge_annotation_rows(
    current: Mapping[str, Any], incoming: Mapping[str, Any]
) -> dict[str, Any]:
    """Preserve forecast and retrospective fields sharing one source row."""
    merged = dict(current)
    availability_flags = {
        "cm_state_space_forecast_available",
        "cm_state_space_diagnostic_available",
        "cm_state_space_diagnostic_only",
        "cm_state_space_training_eligible",
        "cm_kalman_filtered_available",
        "cm_kalman_filtered_training_eligible",
        "cm_kalman_smoothed_available",
        "cm_kalman_smoothed_retrospective",
        "cm_kalman_smoothed_diagnostic_only",
    }
    for name, value in incoming.items():
        if name in availability_flags:
            merged[name] = bool(merged.get(name)) or bool(value)
        elif value is not None:
            merged[name] = value
    merged["cm_kalman_smoothed_training_eligible"] = False
    return merged


def _annotation_row(
    evaluation: Mapping[str, Any],
    input_result: ClassicalModelInputResult,
    row_id: int,
    *,
    diagnostic: bool,
) -> dict[str, Any]:
    fit_status = _text(evaluation.get("fit_status")) or "unavailable"
    forecast_available = (
        not diagnostic
        and _optional_float(evaluation.get("forecast")) is not None
    )
    state_names = cast(list[str], evaluation.get("state_names", []))
    filtered = cast(list[Any], evaluation.get("filtered_state", []))
    filtered_variance = cast(list[Any], evaluation.get("filtered_variance", []))
    smoothed = cast(list[Any], evaluation.get("smoothed_state", []))
    smoothed_variance = cast(list[Any], evaluation.get("smoothed_variance", []))
    level_index = _state_index(state_names, "level")
    trend_index = _state_index(state_names, "trend")
    origin_time = _int(evaluation.get("origin_bin_end_utc_ms"))
    target_time = _int(evaluation.get("target_bin_end_utc_ms"))
    return {
        "series_id": _text(evaluation.get("series_id")),
        "period": _text(evaluation.get("period")),
        "row_id": row_id,
        "cm_state_space_schema_version": (
            STATE_SPACE_TRAINING_PROJECTION_SCHEMA_VERSION
        ),
        "cm_state_space_input_derivation_id": _text(
            input_result.contract.get("derivation_id")
        ),
        "cm_state_space_model_id": _text(evaluation.get("model_id")),
        "cm_state_space_family_code": _int(evaluation.get("family_code")),
        "cm_state_space_specification_code": _int(
            evaluation.get("specification_code")
        ),
        "cm_state_space_state_dimension": _int(
            evaluation.get("state_dimension")
        ),
        "cm_state_space_component_count": _int(
            evaluation.get("component_count")
        ),
        "cm_state_space_initialization_code": _int(
            evaluation.get("initialization_code")
        ),
        "cm_state_space_fit_status_code": STATE_SPACE_FIT_STATUS_CODES.get(
            fit_status, 1
        ),
        "cm_state_space_failure_reason_code": STATE_SPACE_REASON_CODES.get(
            _text(evaluation.get("fit_reason")), 0
        ),
        "cm_state_space_converged": bool(evaluation.get("converged", False)),
        "cm_state_space_effective_observation_count": _int(
            evaluation.get("effective_observation_count")
        ),
        "cm_state_space_missing_observation_count": _int(
            evaluation.get("missing_observation_count")
        ),
        "cm_state_space_prediction_only_transition_count": _int(
            evaluation.get("prediction_only_transition_count")
        ),
        "cm_state_space_max_prediction_only_gap": _int(
            evaluation.get("max_prediction_only_gap")
        ),
        "cm_state_space_fold_id": _int(evaluation.get("fold_id")),
        "cm_state_space_origin_row_id": evaluation.get("origin_row_id"),
        "cm_state_space_target_row_id": evaluation.get("target_row_id"),
        "cm_state_space_horizon": _int(evaluation.get("horizon")),
        "cm_state_space_forecast": (
            _optional_float(evaluation.get("forecast"))
            if not diagnostic
            else None
        ),
        "cm_state_space_forecast_standard_error": (
            _optional_float(evaluation.get("forecast_standard_error"))
            if not diagnostic
            else None
        ),
        "cm_state_space_forecast_lower": (
            _optional_float(evaluation.get("forecast_lower"))
            if not diagnostic
            else None
        ),
        "cm_state_space_forecast_upper": (
            _optional_float(evaluation.get("forecast_upper"))
            if not diagnostic
            else None
        ),
        "cm_state_space_forecast_available": forecast_available,
        "cm_state_space_forecast_available_at_utc_ms": (
            origin_time if not diagnostic else None
        ),
        "cm_state_space_actual": (
            _optional_float(evaluation.get("actual")) if diagnostic else None
        ),
        "cm_state_space_error": (
            _optional_float(evaluation.get("error")) if diagnostic else None
        ),
        "cm_state_space_diagnostic_available": diagnostic,
        "cm_state_space_diagnostic_available_at_utc_ms": (
            target_time if diagnostic else None
        ),
        "cm_state_space_diagnostic_only": diagnostic,
        "cm_state_space_original_scale": True,
        "cm_state_space_training_eligible": forecast_available,
        "cm_kalman_schema_version": STATE_SPACE_STATE_RESULT_SCHEMA_VERSION,
        "cm_kalman_model_id": _text(evaluation.get("model_id")),
        "cm_kalman_filtered_calculation_basis_code": (
            KALMAN_FILTERED_CALCULATION_BASIS_CODE
        ),
        "cm_kalman_filtered_level": (
            _vector_value(filtered, level_index) if not diagnostic else None
        ),
        "cm_kalman_filtered_trend": (
            _vector_value(filtered, trend_index) if not diagnostic else None
        ),
        "cm_kalman_filtered_level_variance": (
            _vector_value(filtered_variance, level_index)
            if not diagnostic
            else None
        ),
        "cm_kalman_filtered_trend_variance": (
            _vector_value(filtered_variance, trend_index)
            if not diagnostic
            else None
        ),
        "cm_kalman_filtered_available": forecast_available,
        "cm_kalman_filtered_available_at_utc_ms": (
            origin_time if forecast_available else None
        ),
        "cm_kalman_filtered_training_eligible": forecast_available,
        "cm_kalman_smoothed_calculation_basis_code": (
            KALMAN_SMOOTHED_CALCULATION_BASIS_CODE
        ),
        "cm_kalman_smoothed_level": (
            _vector_value(smoothed, level_index) if diagnostic else None
        ),
        "cm_kalman_smoothed_trend": (
            _vector_value(smoothed, trend_index) if diagnostic else None
        ),
        "cm_kalman_smoothed_level_variance": (
            _vector_value(smoothed_variance, level_index)
            if diagnostic
            else None
        ),
        "cm_kalman_smoothed_trend_variance": (
            _vector_value(smoothed_variance, trend_index)
            if diagnostic
            else None
        ),
        "cm_kalman_smoothed_available": diagnostic,
        "cm_kalman_smoothed_available_at_utc_ms": (
            target_time if diagnostic else None
        ),
        "cm_kalman_smoothed_retrospective": diagnostic,
        "cm_kalman_smoothed_diagnostic_only": diagnostic,
        "cm_kalman_smoothed_training_eligible": False,
    }


def _training_projection_metadata(
    profile: StateSpaceProfile,
    input_result: ClassicalModelInputResult,
    annotation_count: int,
    collision_count: int,
) -> dict[str, JSONValue]:
    return {
        "schema_version": STATE_SPACE_TRAINING_PROJECTION_SCHEMA_VERSION,
        "grain": "row",
        "identity_fields": ["series_id", "period", "row_id"],
        "timestamp_is_sole_identity": False,
        "mapping_policy": "first_source_row_at_or_after_availability",
        "collision_policy": "merge_forecast_and_diagnostic_latest_origin_wins",
        "collision_count": collision_count,
        "annotation_count": annotation_count,
        "projection_specification_id": profile.projection_specification_id,
        "projection_horizon": profile.projection_horizon,
        "input_derivation_id": input_result.contract.get("derivation_id"),
        "column_names": list((*STATE_SPACE_COLUMNS, *KALMAN_COLUMNS)),
        "column_prefixes": ["cm_state_space_", "cm_kalman_"],
        "filtered_state_forecast_safe": True,
        "smoothed_state_retrospective_diagnostic_only": True,
        "smoothed_state_training_eligible": False,
        "full_series_smoothing_projected": False,
        "observed_columns_overwritten": False,
    }


def _base_payload(
    input_result: ClassicalModelInputResult,
    fingerprint: Mapping[str, JSONValue],
    profile: StateSpaceProfile,
) -> dict[str, JSONValue]:
    contract = input_result.contract
    regularization = _mapping(contract.get("regularization"))
    return {
        "schema_version": STATE_SPACE_SCHEMA_VERSION,
        "advisory": True,
        "target_axis": dict(_mapping(contract.get("target_axis"))),
        "reference_fingerprint_id": contract.get("reference_fingerprint_id")
        or fingerprint.get("fingerprint_id"),
        "input_schema_version": CLASSICAL_MODEL_INPUT_SCHEMA_VERSION,
        "input_derivation_id": contract.get("derivation_id"),
        "input_status": contract.get("status"),
        "calculation_basis": "regular_grid_rolling_origin_kalman_filter",
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
            "expected_closure_count": regularization.get(
                "expected_closure_count"
            ),
            "unexpected_missing_count": regularization.get(
                "unexpected_missing_count"
            ),
            "forward_fill_policy": regularization.get("forward_fill_policy"),
            "backend_missing_observation_behavior": (
                "prediction_only_kalman_transition"
            ),
        },
        "missing_observation_policy": {
            "regular_time_basis": True,
            "fill_policy": "none",
            "transition_policy": "prediction_only",
            "expected_closures_remain_missing": True,
            "unexpected_missing_bins_remain_missing": True,
        },
        "transition_time_basis": {
            "grid": "regular",
            "frequency_ms": regularization.get("frequency_ms"),
            "irregular_elapsed_scaling_supported": False,
            "prediction_only_transition_elapsed_ms": regularization.get(
                "frequency_ms"
            ),
        },
        "forward_fill_policy": "never",
        "original_scale_forecasts": True,
        "filtering_basis": "origin_training_segment_only",
        "smoothing_basis": (
            "origin_training_segment_retrospective_diagnostic_only"
        ),
        "smoothed_state_used_for_forecast": False,
        "full_series_smoothing_used": False,
        "automatic_component_selection": False,
        "automatic_winner": False,
        "hard_fail_quality_gate": False,
        "fitted_objects_included": False,
        "backend_exception_text_included": False,
    }


def _unavailable_result(
    input_result: ClassicalModelInputResult,
    base: Mapping[str, JSONValue],
    reason: str,
    *,
    status: str = "unavailable",
) -> StateSpaceResult:
    configuration = _mapping(base.get("configuration"))
    diagnostics: dict[str, JSONValue] = {
        **dict(base),
        "status": status,
        "reason": reason,
        "limitations": [reason],
        "backend": {
            "provider": "statsmodels",
            "version": None,
            "available": reason != "dependency_unavailable",
            "model_class": "statsmodels.tsa.statespace.UnobservedComponents",
        },
        "fit_summary": {
            "schema_version": STATE_SPACE_FIT_SCHEMA_VERSION,
            "fit_attempt_count": 0,
            "status_counts": {},
            "reason_counts": {reason: 1},
            "warning_counts": {},
            "failed_fit_count": 0,
            "limited_fit_count": 0,
            "convergence_rate": None,
            "failure_rate": None,
            "fit_samples": [],
            "fit_samples_truncated": False,
        },
        "evaluation": {
            "schema_version": STATE_SPACE_EVALUATION_SCHEMA_VERSION,
            "calculation_basis": "regular_grid_rolling_origin_kalman_filter",
            "original_scale": True,
            "model_count": 0,
            "fold_count": len(input_result.folds),
            "evaluated_fold_count": 0,
            "skipped_evaluation_count": len(input_result.folds),
            "forecast_coverage_rate": None,
            "models": [],
            "reference_baselines": [],
            "reference_models": {},
            "automatic_winner": False,
        },
        "resource_usage": {
            "estimated_working_memory_bytes": 0,
            "memory_limit_exceeded": False,
            "fit_attempt_count": 0,
            "wall_time_limit_enforced": True,
            "wall_time_observed_in_payload": False,
        },
        "training_projection": {
            "schema_version": STATE_SPACE_TRAINING_PROJECTION_SCHEMA_VERSION,
            "column_names": list((*STATE_SPACE_COLUMNS, *KALMAN_COLUMNS)),
            "projection_specification_id": configuration.get(
                "projection_specification_id"
            ),
            "annotation_count": 0,
        },
        "fit_duration_included": False,
    }
    return StateSpaceResult(diagnostics, (), input_result)


def _ensure_projection_columns(frame: Any) -> Any:
    import polars as pl

    definitions = {
        "schema_version": pl.Utf8,
        "input_derivation_id": pl.Utf8,
        "model_id": pl.Utf8,
        "converged": pl.Boolean,
        "forecast_available": pl.Boolean,
        "diagnostic_available": pl.Boolean,
        "diagnostic_only": pl.Boolean,
        "original_scale": pl.Boolean,
        "training_eligible": pl.Boolean,
        "filtered_available": pl.Boolean,
        "filtered_training_eligible": pl.Boolean,
        "smoothed_available": pl.Boolean,
        "smoothed_retrospective": pl.Boolean,
        "smoothed_diagnostic_only": pl.Boolean,
        "smoothed_training_eligible": pl.Boolean,
    }
    float_suffixes = {
        "forecast",
        "forecast_standard_error",
        "forecast_lower",
        "forecast_upper",
        "actual",
        "error",
        "filtered_level",
        "filtered_trend",
        "filtered_level_variance",
        "filtered_trend_variance",
        "smoothed_level",
        "smoothed_trend",
        "smoothed_level_variance",
        "smoothed_trend_variance",
    }
    result = frame
    for name in (*STATE_SPACE_COLUMNS, *KALMAN_COLUMNS):
        if name in result.columns:
            continue
        prefix = (
            "cm_state_space_"
            if name.startswith("cm_state_space_")
            else "cm_kalman_"
        )
        suffix = name.removeprefix(prefix)
        dtype = definitions.get(
            suffix, pl.Float64 if suffix in float_suffixes else pl.Int64
        )
        result = result.with_columns(pl.lit(None).cast(dtype).alias(name))
    return result


def _model_id(
    specification: StateSpaceSpecification,
    derivation_id: str,
    backend_version: str,
) -> str:
    payload = {
        "schema_version": STATE_SPACE_CONFIGURATION_SCHEMA_VERSION,
        "derivation_id": derivation_id,
        "backend_version": backend_version,
        "configuration": specification.to_metadata(),
    }
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":")
    ).encode()
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _estimated_memory(
    row_count: int,
    fold_count: int,
    specifications: Sequence[StateSpaceSpecification],
) -> int:
    component_weight = sum(
        max(1, specification.component_count)
        for specification in specifications
    )
    return (
        max(1, row_count) * max(1, fold_count) * max(1, component_weight) * 64
    )


def _load_backend() -> _Backend | None:
    try:
        module = importlib.import_module(
            "statsmodels.tsa.statespace.structural"
        )
        version = importlib.metadata.version("statsmodels")
    except (ImportError, importlib.metadata.PackageNotFoundError):
        return None
    return _Backend(
        version=version, unobserved_components=module.UnobservedComponents
    )


def _backend_failure_reason(exc: Exception) -> str:
    message = str(exc).lower()
    if "initial" in message or "diffuse" in message:
        return "diffuse_initialization_failure"
    if "positive" in message and "cov" in message:
        return "non_positive_covariance"
    if "singular" in message or "linalg" in message:
        return "singular_covariance"
    if "identif" in message:
        return "unidentifiable_model"
    if "seasonal" in message or "frequency" in message:
        return "invalid_time_basis"
    if "overflow" in message or "finite" in message or "nan" in message:
        return "numerical_instability"
    if "parameter" in message or "component" in message:
        return "invalid_configuration"
    return "backend_failure"


def _folds_by_origin(
    folds: Sequence[Mapping[str, Any]],
) -> list[tuple[tuple[str, str, int], list[dict[str, Any]]]]:
    grouped: dict[tuple[str, str, int], list[dict[str, Any]]] = {}
    for fold in folds:
        key = (
            _text(fold.get("series_id")),
            _text(fold.get("period")),
            _int(fold.get("origin_bin_end_utc_ms")),
        )
        grouped.setdefault(key, []).append(dict(fold))
    return [
        (key, sorted(values, key=lambda item: _int(item.get("horizon"))))
        for key, values in sorted(grouped.items())
    ]


def _max_missing_run(values: Sequence[float | None]) -> int:
    longest = 0
    current = 0
    for value in values:
        if value is None:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def _finite_or_none(value: Any) -> float | None:
    parsed = _optional_float(value)
    return parsed if parsed is not None and math.isfinite(parsed) else None


def _last_vector(raw: Any) -> tuple[float | None, ...]:
    try:
        return tuple(_finite_or_none(value) for value in raw[:, -1])
    except (IndexError, TypeError):
        return ()


def _last_covariance_diagonal(raw: Any) -> tuple[float | None, ...]:
    try:
        matrix = raw[:, :, -1]
        return tuple(
            _finite_or_none(matrix[index, index])
            for index in range(len(matrix))
        )
    except (IndexError, TypeError):
        return ()


def _states_are_valid(values: Sequence[float | None]) -> bool:
    return all(value is None or math.isfinite(value) for value in values)


def _sequence_value(
    values: Sequence[float], horizon: int, digits: int
) -> float | None:
    if 0 < horizon <= len(values):
        value = _rounded(values[horizon - 1], digits)
        return float(value) if value is not None else None
    return None


def _state_index(names: Sequence[str], requested: str) -> int | None:
    for index, name in enumerate(names):
        if name == requested or name.startswith(f"{requested}."):
            return index
    return None


def _vector_value(values: Sequence[Any], index: int | None) -> float | None:
    if index is None or index < 0 or index >= len(values):
        return None
    value = _optional_float(values[index])
    return float(value) if value is not None else None
