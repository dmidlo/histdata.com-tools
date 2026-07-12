"""Optional exponential-smoothing models over classical model contracts.

The implementation is deliberately family-scoped.  It consumes the regular
grid and rolling-origin folds from :mod:`classical_model_contracts`, fits only
explicitly configured specifications, and emits bounded advisory diagnostics.
The rich numerical backend is imported lazily so core fingerprinting remains a
low-dependency path.
"""

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

from histdatacom.data_quality.calendar import classify_histdata_timestamp
from histdatacom.data_quality.classical_model_contracts import (
    CLASSICAL_MODEL_INPUT_SCHEMA_VERSION,
    ClassicalModelInputProfile,
    ClassicalModelInputResult,
    build_classical_model_input,
)
from histdatacom.data_quality.contracts import QualityFinding
from histdatacom.data_quality.limits import bounded_report_limit
from histdatacom.data_quality.training_features import (
    EXPONENTIAL_SMOOTHING_COLUMNS,
    ensure_tick_training_features,
)
from histdatacom.runtime_contracts import JSONValue

EXPONENTIAL_SMOOTHING_SCHEMA_VERSION = "histdatacom.exponential-smoothing.v1"
EXPONENTIAL_SMOOTHING_CONFIGURATION_SCHEMA_VERSION = (
    "histdatacom.exponential-smoothing-configuration.v1"
)
EXPONENTIAL_SMOOTHING_FIT_SCHEMA_VERSION = (
    "histdatacom.exponential-smoothing-fit-result.v1"
)
EXPONENTIAL_SMOOTHING_FORECAST_SCHEMA_VERSION = (
    "histdatacom.exponential-smoothing-forecast.v1"
)
EXPONENTIAL_SMOOTHING_EVALUATION_SCHEMA_VERSION = (
    "histdatacom.exponential-smoothing-evaluation.v1"
)
EXPONENTIAL_SMOOTHING_TRAINING_PROJECTION_SCHEMA_VERSION = (
    "histdatacom.exponential-smoothing-training-projection.v1"
)
EXPONENTIAL_SMOOTHING_SUMMARY_SCHEMA_VERSION = (
    "histdatacom.exponential-smoothing-summary.v1"
)
EXPONENTIAL_SMOOTHING_SUMMARY_METADATA_KEY = (
    "time_series_fingerprint_exponential_smoothing_summary"
)
EXPONENTIAL_SMOOTHING_BOUNDED_PAYLOAD_KEY = "fingerprint_exponential_smoothing"

DEFAULT_EXPONENTIAL_SMOOTHING_SUMMARY_TARGET_LIMIT = 16
DEFAULT_EXPONENTIAL_SMOOTHING_ROLLING_WINDOWS = (5, 20)
DEFAULT_EXPONENTIAL_SMOOTHING_ROUNDING_DIGITS = 12
MAX_EXPONENTIAL_SMOOTHING_SPECIFICATIONS = 16
MAX_EXPONENTIAL_SMOOTHING_PARAMETER_BOUNDS = 32

EXPONENTIAL_SMOOTHING_FAMILIES = (
    "ses",
    "holt",
    "holt_winters",
    "ets",
)
EXPONENTIAL_SMOOTHING_FAMILY_CODES = {
    "ses": 1,
    "holt": 2,
    "holt_winters": 3,
    "ets": 4,
}
EXPONENTIAL_SMOOTHING_COMPONENT_CODES = {"none": 0, "add": 1, "mul": 2}
EXPONENTIAL_SMOOTHING_INITIALIZATION_CODES = {
    "estimated": 1,
    "heuristic": 2,
    "legacy-heuristic": 3,
    "known": 4,
}
EXPONENTIAL_SMOOTHING_FIT_STATUS_CODES = {
    "unavailable": 1,
    "skipped": 2,
    "failed": 3,
    "limited": 4,
    "fitted": 5,
    "converged": 6,
}
EXPONENTIAL_SMOOTHING_REASON_CODES = {
    "": 0,
    "dependency_unavailable": 1,
    "input_contract_unavailable": 2,
    "insufficient_folds": 3,
    "insufficient_data": 4,
    "insufficient_seasonal_cycles": 5,
    "invalid_multiplicative_domain": 6,
    "invalid_configuration": 7,
    "optimizer_failure": 8,
    "numerical_failure": 9,
    "resource_limit": 10,
    "timeout": 11,
    "backend_failure": 12,
    "target_unavailable": 13,
    "inverse_transform_unavailable": 14,
}
EXPONENTIAL_SMOOTHING_WARNING_CODES = {
    "ConvergenceWarning": "convergence_warning",
    "RuntimeWarning": "runtime_warning",
    "ValueWarning": "value_warning",
    "UserWarning": "user_warning",
}
EXPONENTIAL_SMOOTHING_BASELINE_CODES = {
    "naive_random_walk": 1,
    "rolling_mean": 2,
    "rolling_median": 3,
    "session_seasonal_naive": 4,
}

_SPECIFICATION_ID = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,63}$")
_COMPONENTS = {"none", "add", "mul"}
_INITIALIZATION_METHODS = {
    "estimated",
    "heuristic",
    "legacy-heuristic",
    "known",
}
_BOUND_PARAMETER_NAMES = {
    "smoothing_level",
    "smoothing_trend",
    "smoothing_seasonal",
    "damping_trend",
    "initial_level",
    "initial_trend",
}


@dataclass(frozen=True, slots=True)
class ExponentialSmoothingSpecification:
    """One explicit exponential-smoothing specification."""

    specification_id: str = "ses"
    family: str = "ses"
    level: bool = True
    error: str = "add"
    trend: str = "none"
    damped_trend: bool = False
    seasonal: str = "none"
    seasonal_periods: int = 0
    initialization_method: str = "estimated"
    initial_level: float | None = None
    initial_trend: float | None = None
    initial_seasonal: tuple[float, ...] = ()
    optimized: bool = True
    method: str = ""
    use_brute: bool = False
    remove_bias: bool = False
    smoothing_level: float | None = None
    smoothing_trend: float | None = None
    smoothing_seasonal: float | None = None
    damping_trend: float | None = None
    parameter_bounds: tuple[tuple[str, float, float], ...] = ()
    max_iterations: int = 200

    def __post_init__(self) -> None:
        if not _SPECIFICATION_ID.fullmatch(self.specification_id):
            raise ValueError("invalid exponential-smoothing specification_id")
        if self.family not in EXPONENTIAL_SMOOTHING_FAMILIES:
            raise ValueError("unsupported exponential-smoothing family")
        if self.level is not True:
            raise ValueError(
                "exponential-smoothing level component is required"
            )
        if self.error not in {"add", "mul"}:
            raise ValueError("error must be add or mul")
        if self.trend not in _COMPONENTS or self.seasonal not in _COMPONENTS:
            raise ValueError("trend and seasonal must be none, add, or mul")
        if self.initialization_method not in _INITIALIZATION_METHODS:
            raise ValueError("unsupported initialization_method")
        if self.family == "ses" and (
            self.trend != "none" or self.seasonal != "none" or self.damped_trend
        ):
            raise ValueError(
                "SES cannot configure trend, damping, or seasonality"
            )
        if self.family == "holt" and (
            self.trend == "none" or self.seasonal != "none"
        ):
            raise ValueError(
                "Holt requires trend and cannot configure seasonality"
            )
        if self.family == "holt_winters" and self.seasonal == "none":
            raise ValueError(
                "Holt-Winters requires an explicit seasonal component"
            )
        if self.damped_trend and self.trend == "none":
            raise ValueError("damped_trend requires a trend component")
        if self.seasonal == "none" and self.seasonal_periods != 0:
            raise ValueError("seasonal_periods requires a seasonal component")
        if self.seasonal != "none" and self.seasonal_periods < 2:
            raise ValueError("seasonal_periods must be at least 2")
        if self.family == "ets" and not self.optimized:
            raise ValueError("ETSModel requires optimized fitting")
        if self.initialization_method == "known":
            if self.initial_level is None:
                raise ValueError("known initialization requires initial_level")
            if self.trend != "none" and self.initial_trend is None:
                raise ValueError(
                    "known trend initialization requires initial_trend"
                )
            if (
                self.seasonal != "none"
                and len(self.initial_seasonal) != self.seasonal_periods
            ):
                raise ValueError(
                    "known seasonal initialization must match seasonal_periods"
                )
        for name in (
            "smoothing_level",
            "smoothing_trend",
            "smoothing_seasonal",
            "damping_trend",
        ):
            value = getattr(self, name)
            if value is not None and not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be between 0 and 1")
        if self.max_iterations < 1:
            raise ValueError("max_iterations must be positive")
        if (
            len(self.parameter_bounds)
            > MAX_EXPONENTIAL_SMOOTHING_PARAMETER_BOUNDS
        ):
            raise ValueError("too many exponential-smoothing parameter bounds")
        names: set[str] = set()
        for name, lower, upper in self.parameter_bounds:
            if name not in _BOUND_PARAMETER_NAMES and not name.startswith(
                "initial_seasonal."
            ):
                raise ValueError(f"unsupported parameter bound: {name}")
            if (
                name in names
                or not math.isfinite(lower)
                or not math.isfinite(upper)
            ):
                raise ValueError("parameter bounds must be unique and finite")
            if lower >= upper:
                raise ValueError(
                    "parameter bound lower value must be below upper"
                )
            names.add(name)

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return a stable JSON-compatible specification."""
        return {
            "schema_version": EXPONENTIAL_SMOOTHING_CONFIGURATION_SCHEMA_VERSION,
            "specification_id": self.specification_id,
            "family": self.family,
            "family_code": EXPONENTIAL_SMOOTHING_FAMILY_CODES[self.family],
            "level": self.level,
            "error": self.error,
            "trend": self.trend,
            "damped_trend": self.damped_trend,
            "seasonal": self.seasonal,
            "seasonal_periods": self.seasonal_periods,
            "initialization_method": self.initialization_method,
            "initial_level": self.initial_level,
            "initial_trend": self.initial_trend,
            "initial_seasonal": cast(JSONValue, list(self.initial_seasonal)),
            "optimized": self.optimized,
            "method": self.method or None,
            "use_brute": self.use_brute,
            "remove_bias": self.remove_bias,
            "smoothing_level": self.smoothing_level,
            "smoothing_trend": self.smoothing_trend,
            "smoothing_seasonal": self.smoothing_seasonal,
            "damping_trend": self.damping_trend,
            "parameter_bounds": cast(
                JSONValue,
                [
                    {"parameter": name, "lower": lower, "upper": upper}
                    for name, lower, upper in self.parameter_bounds
                ],
            ),
            "max_iterations": self.max_iterations,
            "automatic_search": False,
        }


def _default_specifications() -> tuple[ExponentialSmoothingSpecification, ...]:
    return (ExponentialSmoothingSpecification(),)


@dataclass(frozen=True, slots=True)
class ExponentialSmoothingProfile:
    """Explicit fitted-family controls; disabled by default."""

    enabled: bool = False
    specifications: tuple[ExponentialSmoothingSpecification, ...] = field(
        default_factory=_default_specifications
    )
    projection_specification_id: str = "ses"
    projection_horizon: int = 1
    baseline_rolling_windows: tuple[int, ...] = (
        DEFAULT_EXPONENTIAL_SMOOTHING_ROLLING_WINDOWS
    )
    rounding_digits: int = DEFAULT_EXPONENTIAL_SMOOTHING_ROUNDING_DIGITS

    def __post_init__(self) -> None:
        if not self.specifications:
            raise ValueError(
                "at least one exponential-smoothing specification is required"
            )
        if len(self.specifications) > MAX_EXPONENTIAL_SMOOTHING_SPECIFICATIONS:
            raise ValueError("too many exponential-smoothing specifications")
        identifiers = tuple(
            spec.specification_id for spec in self.specifications
        )
        if len(set(identifiers)) != len(identifiers):
            raise ValueError(
                "exponential-smoothing specification IDs must be unique"
            )
        if self.projection_specification_id not in identifiers:
            raise ValueError(
                "projection_specification_id must select a specification"
            )
        if self.projection_horizon < 1:
            raise ValueError("projection_horizon must be positive")
        if (
            not self.baseline_rolling_windows
            or any(value < 1 for value in self.baseline_rolling_windows)
            or tuple(sorted(set(self.baseline_rolling_windows)))
            != self.baseline_rolling_windows
        ):
            raise ValueError(
                "baseline_rolling_windows must be sorted unique positives"
            )
        if not 0 <= self.rounding_digits <= 16:
            raise ValueError("rounding_digits must be between 0 and 16")

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return stable profile metadata."""
        return {
            "enabled": self.enabled,
            "specifications": cast(
                JSONValue, [spec.to_metadata() for spec in self.specifications]
            ),
            "projection_specification_id": self.projection_specification_id,
            "projection_horizon": self.projection_horizon,
            "baseline_rolling_windows": list(self.baseline_rolling_windows),
            "rounding_digits": self.rounding_digits,
            "automatic_search": False,
            "automatic_winner": False,
        }


@dataclass(frozen=True, slots=True)
class ExponentialSmoothingResult:
    """Bounded diagnostics plus durable row-key annotations."""

    diagnostics: Mapping[str, JSONValue]
    annotations: tuple[Mapping[str, Any], ...]
    input_result: ClassicalModelInputResult


@dataclass(frozen=True, slots=True)
class _Backend:
    version: str
    holt_winters: Any
    ets_model: Any


@dataclass(frozen=True, slots=True)
class _FitOutcome:
    status: str
    reason: str
    forecasts: tuple[float, ...]
    parameters: Mapping[str, float]
    warning_codes: tuple[str, ...]
    converged: bool
    fit_seconds: float


def exponential_smoothing_from_training_frame(
    frame: Any | None,
    fingerprint: Mapping[str, JSONValue],
    *,
    input_profile: ClassicalModelInputProfile | None = None,
    profile: ExponentialSmoothingProfile | None = None,
    target: Any | None = None,
) -> ExponentialSmoothingResult:
    """Regularize the enriched tick frame and evaluate configured models."""
    selected_input = input_profile or ClassicalModelInputProfile(enabled=True)
    input_result = build_classical_model_input(
        frame,
        fingerprint,
        profile=selected_input,
        target=target,
    )
    return exponential_smoothing_from_model_input(
        frame,
        input_result,
        fingerprint,
        input_profile=selected_input,
        profile=profile,
        target=target,
    )


def exponential_smoothing_from_model_input(
    frame: Any | None,
    input_result: ClassicalModelInputResult,
    fingerprint: Mapping[str, JSONValue],
    *,
    input_profile: ClassicalModelInputProfile,
    profile: ExponentialSmoothingProfile | None = None,
    target: Any | None = None,
) -> ExponentialSmoothingResult:
    """Fit explicit exponential-smoothing specifications over #421 folds."""
    selected = profile or ExponentialSmoothingProfile(enabled=True)
    base = _base_payload(input_result, fingerprint, selected)
    if selected.projection_horizon not in input_profile.horizons:
        return _unavailable_result(
            input_result,
            base,
            "invalid_configuration",
        )
    if input_result.contract.get("status") == "unavailable":
        return _unavailable_result(
            input_result,
            base,
            "input_contract_unavailable",
        )
    if not input_result.folds:
        return _unavailable_result(
            input_result,
            base,
            "insufficient_folds",
            status="limited",
        )
    backend = _load_backend()
    if backend is None:
        return _unavailable_result(
            input_result,
            base,
            "dependency_unavailable",
        )
    return _evaluate_models(
        frame,
        input_result,
        fingerprint,
        input_profile,
        selected,
        backend,
        target=target,
    )


def exponential_smoothing_diagnostics_from_training_frame(
    frame: Any | None,
    fingerprint: Mapping[str, JSONValue],
    *,
    input_profile: ClassicalModelInputProfile | None = None,
    profile: ExponentialSmoothingProfile | None = None,
    target: Any | None = None,
) -> dict[str, JSONValue]:
    """Return only the serializable fitted-family diagnostics."""
    return dict(
        exponential_smoothing_from_training_frame(
            frame,
            fingerprint,
            input_profile=input_profile,
            profile=profile,
            target=target,
        ).diagnostics
    )


def project_exponential_smoothing_onto_training_frame(
    frame: Any,
    result: ExponentialSmoothingResult,
    *,
    target: Any | None = None,
) -> Any:
    """Join bounded ETS annotations by durable row identity only."""
    import polars as pl

    columns = set(getattr(frame, "columns", ()))
    if not {"series_id", "period", "row_id"}.issubset(columns):
        enriched = ensure_tick_training_features(frame, target=target)
    else:
        enriched = frame
    left = enriched.drop(
        [
            name
            for name in EXPONENTIAL_SMOOTHING_COLUMNS
            if name in enriched.columns
        ]
    ).with_row_index("__cm_ets_original_order")
    if result.annotations:
        right = pl.DataFrame(
            [dict(row) for row in result.annotations],
            infer_schema_length=None,
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
    return projected.sort("__cm_ets_original_order").drop(
        "__cm_ets_original_order"
    )


def exponential_smoothing_summary(
    findings: Iterable[QualityFinding],
    *,
    target_limit: (
        int | None
    ) = DEFAULT_EXPONENTIAL_SMOOTHING_SUMMARY_TARGET_LIMIT,
) -> dict[str, JSONValue] | None:
    """Return bounded report metadata for exponential-smoothing results."""
    targets: list[dict[str, JSONValue]] = []
    statuses: Counter[str] = Counter()
    for finding in findings:
        fingerprint = _mapping(finding.metadata.get("time_series_fingerprint"))
        payload = _mapping(fingerprint.get("exponential_smoothing"))
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
        default_limit=DEFAULT_EXPONENTIAL_SMOOTHING_SUMMARY_TARGET_LIMIT,
        allow_unbounded=True,
    )
    included = limit.slice(targets)
    omitted = len(targets) - len(included)
    return {
        "schema_version": EXPONENTIAL_SMOOTHING_SUMMARY_SCHEMA_VERSION,
        "advisory": True,
        "target_count": len(targets),
        "included_target_count": len(included),
        "omitted_target_count": omitted,
        "truncated": omitted > 0,
        "status_counts": dict(sorted(statuses.items())),
        "target_summaries": cast(JSONValue, included),
        "limit_metadata": {"targets": limit.limit_payload()},
    }


def format_exponential_smoothing_summary_lines(
    summary: Mapping[str, JSONValue] | None,
) -> tuple[str, ...]:
    """Return concise human-readable fitted-family lines."""
    if not summary:
        return ()
    statuses = _mapping(summary.get("status_counts"))
    lines = [
        "",
        "Exponential-smoothing models",
        (
            f"targets: {_int(summary.get('target_count'))} "
            f"ready: {_int(statuses.get('ready'))} "
            f"limited: {_int(statuses.get('limited'))} "
            f"unavailable: {_int(statuses.get('unavailable'))}"
        ),
    ]
    for target in _mapping_rows(summary.get("target_summaries")):
        axis = _mapping(target.get("target_axis"))
        label = "/".join(
            _text(axis.get(key))
            for key in ("data_format", "timeframe", "symbol", "period")
        )
        lines.append(
            f"- {label}: {_text(target.get('status'))} "
            f"models={_int(target.get('model_count'))} "
            f"fits={_int(target.get('fit_attempt_count'))} "
            f"folds={_int(target.get('evaluated_fold_count'))}"
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
    profile: ExponentialSmoothingProfile,
    backend: _Backend,
    *,
    target: Any | None,
) -> ExponentialSmoothingResult:
    rows = cast(list[dict[str, Any]], input_result.regularized_frame.to_dicts())
    folds = [dict(fold) for fold in input_result.folds]
    origins = _folds_by_origin(folds)
    resource_policy = input_profile.resources
    specifications = profile.specifications[
        : resource_policy.max_candidate_orders
    ]
    limitations: list[str] = []
    estimated_working_memory_bytes = _estimated_working_memory_bytes(
        len(rows), len(folds), len(specifications)
    )
    if len(specifications) < len(profile.specifications):
        limitations.append("resource_limit")
    if estimated_working_memory_bytes > resource_policy.max_memory_bytes:
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
        model_id = _model_id(
            specification,
            _text(input_result.contract.get("derivation_id")),
            backend.version,
        )
        model_evaluations: list[dict[str, Any]] = []
        model_fit_samples: list[dict[str, JSONValue]] = []
        for origin_key, origin_folds in origins:
            if fit_attempt_count >= resource_policy.max_fit_attempts:
                limitations.append("resource_limit")
                break
            if (
                time.monotonic() - started
                >= resource_policy.max_wall_time_seconds
            ):
                limitations.append("timeout")
                break
            fit_attempt_count += 1
            origin = origin_folds[0]
            training_start = _int(origin.get("training_start_index"))
            training_end = _int(origin.get("training_end_index"))
            segment_indexes = _trailing_contiguous_indexes(
                rows,
                training_start,
                training_end,
                _text(origin.get("series_id")),
                _text(origin.get("period")),
            )
            training_values = tuple(
                _float(rows[index].get("cm_input_value"))
                for index in segment_indexes
            )
            max_horizon = max(
                _int(fold.get("horizon")) for fold in origin_folds
            )
            fit_outcome = _fit_specification(
                specification,
                training_values,
                max_horizon,
                backend,
                minimum_required=_minimum_required_observations(specification),
            )
            fit_statuses[fit_outcome.status] += 1
            if fit_outcome.reason:
                fit_reasons[fit_outcome.reason] += 1
            warning_counts.update(fit_outcome.warning_codes)
            fit_sample = _fit_sample(
                fit_outcome,
                specification,
                model_id,
                origin,
                segment_indexes,
                len(training_values),
            )
            model_fit_samples.append(fit_sample)
            all_fit_samples.append(fit_sample)
            original_forecasts = _inverse_forecasts(
                rows,
                training_end,
                fit_outcome.forecasts,
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
                    fit_outcome,
                    original_forecasts,
                    profile.rounding_digits,
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
                resource_policy.max_retained_diagnostics,
                profile.rounding_digits,
            )
        )
        if limitations and limitations[-1] in {"resource_limit", "timeout"}:
            break

    references = _reference_baseline_payloads(
        rows,
        folds,
        profile.baseline_rolling_windows,
        profile.rounding_digits,
    )
    annotations, projection_collisions = _build_annotations(
        frame,
        all_evaluations,
        profile,
        input_result,
        target=target,
    )
    evaluated_count = sum(
        evaluation.get("status") == "evaluated"
        for evaluation in all_evaluations
    )
    skipped_evaluation_count = len(all_evaluations) - evaluated_count
    failed_count = sum(
        status in {"failed", "unavailable"}
        for status in fit_statuses.elements()
    )
    limited_fit_count = fit_statuses["limited"] + fit_statuses["skipped"]
    if not evaluated_count:
        limitations.append("insufficient_data")
    limitations = list(dict.fromkeys(limitations))
    status = (
        "ready"
        if not limitations and failed_count == 0 and limited_fit_count == 0
        else "limited"
    )
    reason = (
        limitations[0]
        if limitations
        else (
            sorted(fit_reasons)[0]
            if fit_reasons
            else ("backend_failure" if failed_count else None)
        )
    )
    if reason and status == "ready":
        status = "limited"
    diagnostics: dict[str, JSONValue] = {
        **_base_payload(input_result, fingerprint, profile),
        "status": status,
        "reason": reason,
        "limitations": cast(JSONValue, limitations),
        "backend": {
            "provider": "statsmodels",
            "version": backend.version,
            "available": True,
            "import_basis": "optional_models_extra",
        },
        "fit_summary": {
            "schema_version": EXPONENTIAL_SMOOTHING_FIT_SCHEMA_VERSION,
            "fit_attempt_count": fit_attempt_count,
            "status_counts": dict(sorted(fit_statuses.items())),
            "reason_counts": dict(sorted(fit_reasons.items())),
            "warning_counts": dict(sorted(warning_counts.items())),
            "failed_fit_count": failed_count,
            "limited_fit_count": limited_fit_count,
            "fit_samples": cast(
                JSONValue,
                all_fit_samples[: resource_policy.max_retained_diagnostics],
            ),
            "fit_samples_truncated": (
                len(all_fit_samples) > resource_policy.max_retained_diagnostics
            ),
        },
        "resource_usage": {
            "limits": resource_policy.to_metadata(),
            "estimated_working_memory_bytes": estimated_working_memory_bytes,
            "memory_limit_exceeded": (
                estimated_working_memory_bytes
                > resource_policy.max_memory_bytes
            ),
            "fit_attempt_count": fit_attempt_count,
            "wall_time_limit_enforced": True,
            "wall_time_observed_in_payload": False,
        },
        "evaluation": {
            "schema_version": EXPONENTIAL_SMOOTHING_EVALUATION_SCHEMA_VERSION,
            "calculation_basis": "regular_grid_rolling_origin",
            "original_scale": True,
            "model_count": len(model_payloads),
            "fold_count": len(folds),
            "evaluated_fold_count": evaluated_count,
            "skipped_evaluation_count": skipped_evaluation_count,
            "models": cast(JSONValue, model_payloads),
            "reference_baselines": cast(JSONValue, references),
            "comparison_semantics": "descriptive_shared_folds_only",
            "automatic_winner": False,
        },
        "training_projection": _training_projection_metadata(
            profile,
            input_result,
            len(annotations),
            projection_collisions,
        ),
        "fit_duration_included": False,
    }
    return ExponentialSmoothingResult(
        diagnostics=diagnostics,
        annotations=annotations,
        input_result=input_result,
    )


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


def _estimated_working_memory_bytes(
    row_count: int,
    fold_count: int,
    specification_count: int,
) -> int:
    """Return a conservative deterministic bound for fitted-family work arrays."""
    row_storage = row_count * 16 * 8
    fold_storage = fold_count * 64 * 8
    model_storage = specification_count * 512 * 8
    return row_storage + fold_storage + model_storage


def _trailing_contiguous_indexes(
    rows: Sequence[Mapping[str, Any]],
    start: int,
    end: int,
    series_id: str,
    period: str,
) -> tuple[int, ...]:
    selected: list[int] = []
    for index in range(end, start - 1, -1):
        row = rows[index]
        if (
            _text(row.get("series_id")) != series_id
            or _text(row.get("period")) != period
        ):
            break
        value = _optional_float(row.get("cm_input_value"))
        if value is None or row.get("cm_input_transform_valid") is not True:
            if selected:
                break
            continue
        selected.append(index)
    return tuple(reversed(selected))


def _fit_specification(
    specification: ExponentialSmoothingSpecification,
    values: Sequence[float],
    horizon: int,
    backend: _Backend,
    *,
    minimum_required: int,
) -> _FitOutcome:
    if len(values) < minimum_required:
        reason = (
            "insufficient_seasonal_cycles"
            if specification.seasonal != "none"
            else "insufficient_data"
        )
        return _FitOutcome("skipped", reason, (), {}, (), False, 0.0)
    if _uses_multiplicative_component(specification) and any(
        value <= 0 for value in values
    ):
        return _FitOutcome(
            "skipped",
            "invalid_multiplicative_domain",
            (),
            {},
            (),
            False,
            0.0,
        )
    started = time.monotonic()
    captured: list[warnings.WarningMessage]
    try:
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always")
            fitted = _statsmodels_fit(specification, values, backend)
            raw_forecasts = fitted.forecast(horizon)
        forecasts = tuple(float(value) for value in raw_forecasts)
        if not forecasts or any(
            not math.isfinite(value) for value in forecasts
        ):
            return _FitOutcome(
                "failed",
                "numerical_failure",
                (),
                {},
                _warning_codes(captured),
                False,
                time.monotonic() - started,
            )
        warning_codes = _warning_codes(captured)
        converged = _fit_converged(
            fitted, warning_codes, specification.optimized
        )
        status = (
            "limited"
            if "convergence_warning" in warning_codes
            else "converged" if converged else "fitted"
        )
        reason = "optimizer_failure" if status == "limited" else ""
        return _FitOutcome(
            status,
            reason,
            forecasts,
            _fitted_parameters(fitted),
            warning_codes,
            converged,
            time.monotonic() - started,
        )
    except (ArithmeticError, FloatingPointError, OverflowError):
        return _FitOutcome(
            "failed",
            "numerical_failure",
            (),
            {},
            (),
            False,
            time.monotonic() - started,
        )
    except (TypeError, ValueError) as exc:
        return _FitOutcome(
            "failed",
            _configuration_failure_reason(exc),
            (),
            {},
            (),
            False,
            time.monotonic() - started,
        )
    except Exception:  # pragma: no cover - backend-specific safety boundary
        return _FitOutcome(
            "failed",
            "backend_failure",
            (),
            {},
            (),
            False,
            time.monotonic() - started,
        )


def _statsmodels_fit(
    specification: ExponentialSmoothingSpecification,
    values: Sequence[float],
    backend: _Backend,
) -> Any:
    trend = None if specification.trend == "none" else specification.trend
    seasonal = (
        None if specification.seasonal == "none" else specification.seasonal
    )
    bounds = {
        name: (lower, upper)
        for name, lower, upper in specification.parameter_bounds
    }
    initialization = {
        "initialization_method": specification.initialization_method,
        "initial_level": specification.initial_level,
        "initial_trend": specification.initial_trend,
        "initial_seasonal": (
            list(specification.initial_seasonal)
            if specification.initial_seasonal
            else None
        ),
    }
    if specification.family == "ets":
        model = backend.ets_model(
            values,
            error=specification.error,
            trend=trend,
            damped_trend=specification.damped_trend,
            seasonal=seasonal,
            seasonal_periods=(specification.seasonal_periods or None),
            bounds=(bounds or None),
            missing="raise",
            **initialization,
        )
        return model.fit(
            maxiter=specification.max_iterations,
            disp=False,
        )
    model = backend.holt_winters(
        values,
        trend=trend,
        damped_trend=specification.damped_trend,
        seasonal=seasonal,
        seasonal_periods=(specification.seasonal_periods or None),
        bounds=(bounds or None),
        missing="raise",
        use_boxcox=False,
        **initialization,
    )
    minimize_kwargs: dict[str, Any] | None = None
    if specification.optimized:
        minimize_kwargs = {"options": {"maxiter": specification.max_iterations}}
    return model.fit(
        smoothing_level=specification.smoothing_level,
        smoothing_trend=specification.smoothing_trend,
        smoothing_seasonal=specification.smoothing_seasonal,
        damping_trend=specification.damping_trend,
        optimized=specification.optimized,
        remove_bias=specification.remove_bias,
        method=(specification.method or None),
        minimize_kwargs=minimize_kwargs,
        use_brute=specification.use_brute,
    )


def _minimum_required_observations(
    specification: ExponentialSmoothingSpecification,
) -> int:
    if specification.seasonal != "none":
        return max(4, 2 * specification.seasonal_periods)
    if specification.trend != "none":
        return 3
    return 2


def _uses_multiplicative_component(
    specification: ExponentialSmoothingSpecification,
) -> bool:
    return "mul" in {
        specification.error,
        specification.trend,
        specification.seasonal,
    }


def _warning_codes(
    captured: Sequence[warnings.WarningMessage],
) -> tuple[str, ...]:
    values = {
        EXPONENTIAL_SMOOTHING_WARNING_CODES.get(
            item.category.__name__, "backend_warning"
        )
        for item in captured
    }
    return tuple(sorted(values))


def _fit_converged(
    fitted: Any,
    warning_codes: Sequence[str],
    optimized: bool,
) -> bool:
    if not optimized or "convergence_warning" in warning_codes:
        return False
    result = getattr(fitted, "mle_retvals", None)
    if result is None:
        return True
    if isinstance(result, Mapping):
        value = result.get("converged", result.get("success"))
    else:
        value = getattr(result, "success", getattr(result, "converged", None))
    return True if value is None else bool(value)


def _fitted_parameters(fitted: Any) -> dict[str, float]:
    raw = getattr(fitted, "params", {})
    values: dict[str, float] = {}
    items: Iterable[tuple[Any, Any]]
    if isinstance(raw, Mapping):
        items = raw.items()
    else:
        names = list(getattr(getattr(fitted, "model", None), "param_names", ()))
        items = zip(names, raw, strict=False)
    for name, value in items:
        scalar = _optional_float(value)
        if scalar is not None:
            values[str(name)] = scalar
    return dict(sorted(values.items()))


def _configuration_failure_reason(exc: Exception) -> str:
    message = str(exc).lower()
    if "seasonal" in message and ("cycle" in message or "period" in message):
        return "insufficient_seasonal_cycles"
    if "positive" in message or "strictly > 0" in message:
        return "invalid_multiplicative_domain"
    if "bound" in message or "initial" in message:
        return "invalid_configuration"
    return "backend_failure"


def _fit_sample(
    outcome: _FitOutcome,
    specification: ExponentialSmoothingSpecification,
    model_id: str,
    fold: Mapping[str, Any],
    segment_indexes: Sequence[int],
    observation_count: int,
) -> dict[str, JSONValue]:
    return {
        "schema_version": EXPONENTIAL_SMOOTHING_FIT_SCHEMA_VERSION,
        "model_id": model_id,
        "specification_id": specification.specification_id,
        "status": outcome.status,
        "reason": outcome.reason or None,
        "converged": outcome.converged,
        "series_id": _text(fold.get("series_id")),
        "period": _text(fold.get("period")),
        "origin_bin_end_utc_ms": _int(fold.get("origin_bin_end_utc_ms")),
        "observation_count": observation_count,
        "segment_start_index": segment_indexes[0] if segment_indexes else None,
        "segment_end_index": segment_indexes[-1] if segment_indexes else None,
        "training_segment_policy": "trailing_contiguous_after_missing",
        "parameters": dict(outcome.parameters),
        "parameter_count": len(outcome.parameters),
        "warning_codes": cast(JSONValue, list(outcome.warning_codes)),
        "fit_duration_included": False,
        "backend_exception_text_included": False,
    }


def _fold_evaluation(
    rows: Sequence[Mapping[str, Any]],
    fold: Mapping[str, Any],
    specification: ExponentialSmoothingSpecification,
    specification_code: int,
    model_id: str,
    outcome: _FitOutcome,
    original_forecasts: Sequence[float | None],
    rounding_digits: int,
) -> dict[str, Any]:
    horizon = _int(fold.get("horizon"))
    forecast = (
        original_forecasts[horizon - 1]
        if 0 < horizon <= len(original_forecasts)
        else None
    )
    transformed_forecast = (
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
    fold_valid = fold.get("status") == "valid"
    if outcome.status in {"failed", "skipped", "unavailable"}:
        status = "not_evaluated"
        reason = outcome.reason
    elif not fold_valid or actual is None:
        status = "skipped"
        reason = "target_unavailable"
    elif forecast is None:
        status = "skipped"
        reason = "inverse_transform_unavailable"
    else:
        status = "evaluated"
        reason = ""
    error = (
        forecast - actual
        if status == "evaluated" and forecast is not None and actual is not None
        else None
    )
    return {
        "schema_version": EXPONENTIAL_SMOOTHING_FORECAST_SCHEMA_VERSION,
        "status": status,
        "reason": reason or None,
        "series_id": _text(fold.get("series_id")),
        "period": _text(fold.get("period")),
        "model_id": model_id,
        "specification_id": specification.specification_id,
        "specification_code": specification_code,
        "family": specification.family,
        "family_code": EXPONENTIAL_SMOOTHING_FAMILY_CODES[specification.family],
        "error_component": specification.error,
        "trend_component": specification.trend,
        "seasonal_component": specification.seasonal,
        "damped_trend": specification.damped_trend,
        "initialization_method": specification.initialization_method,
        "fit_status": outcome.status,
        "fit_reason": outcome.reason or None,
        "converged": outcome.converged,
        "fold_id": _int(fold.get("fold_id")),
        "origin_row_id": fold.get("origin_row_id"),
        "target_row_id": fold.get("target_row_id"),
        "origin_bin_end_utc_ms": _int(fold.get("origin_bin_end_utc_ms")),
        "target_bin_end_utc_ms": _int(fold.get("target_bin_end_utc_ms")),
        "horizon": horizon,
        "transformed_forecast": _rounded(transformed_forecast, rounding_digits),
        "forecast": _rounded(forecast, rounding_digits),
        "actual": _rounded(actual, rounding_digits),
        "error": _rounded(error, rounding_digits),
        "absolute_error": _rounded(
            abs(error) if error is not None else None, rounding_digits
        ),
        "squared_error": _rounded(
            error * error if error is not None else None, rounding_digits
        ),
        "original_scale": True,
        "future_values_visible": False,
        "automatic_winner": False,
    }


def _model_evaluation_payload(
    specification: ExponentialSmoothingSpecification,
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
    metrics = [
        _metrics_for_horizon(evaluations, horizon, rounding_digits)
        for horizon in horizons
    ]
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
        "horizon_metrics": cast(JSONValue, metrics),
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
        "mae": (
            _rounded(sum(absolute) / len(absolute), rounding_digits)
            if absolute
            else None
        ),
        "rmse": (
            _rounded(math.sqrt(sum(squared) / len(squared)), rounding_digits)
            if squared
            else None
        ),
        "bias": (
            _rounded(sum(errors) / len(errors), rounding_digits)
            if errors
            else None
        ),
        "original_scale": True,
    }


def _inverse_forecasts(
    rows: Sequence[Mapping[str, Any]],
    origin_index: int,
    forecasts: Sequence[float],
    profile: ClassicalModelInputProfile,
    rounding_digits: int,
) -> tuple[float | None, ...]:
    if not forecasts or not 0 <= origin_index < len(rows):
        return ()
    origin = rows[origin_index]
    group_indexes = [
        index
        for index, row in enumerate(rows)
        if _text(row.get("series_id")) == _text(origin.get("series_id"))
        and _text(row.get("period")) == _text(origin.get("period"))
    ]
    try:
        local_origin = group_indexes.index(origin_index)
    except ValueError:
        return tuple(None for _ in forecasts)
    observed = [
        _optional_float(rows[index].get("cm_input_observed_value"))
        for index in group_indexes
    ]
    base = _base_transform(observed, profile.transform)
    stages: list[list[float | None]] = [base]
    lags: list[int] = []
    for _ in range(profile.differencing_order):
        stages.append(_difference(stages[-1], 1))
        lags.append(1)
    for _ in range(profile.seasonal_differencing_order):
        stages.append(_difference(stages[-1], profile.seasonal_period))
        lags.append(profile.seasonal_period)
    current: list[float | None] = [float(value) for value in forecasts]
    for stage_index in range(len(stages) - 1, 0, -1):
        parent = stages[stage_index - 1]
        lag = lags[stage_index - 1]
        restored: list[float | None] = []
        for offset, value in enumerate(current, start=1):
            reference_position = local_origin + offset - lag
            if reference_position <= local_origin:
                reference = (
                    parent[reference_position]
                    if 0 <= reference_position < len(parent)
                    else None
                )
            else:
                prediction_position = reference_position - local_origin - 1
                reference = (
                    restored[prediction_position]
                    if 0 <= prediction_position < len(restored)
                    else None
                )
            restored.append(
                value + reference
                if value is not None and reference is not None
                else None
            )
        current = restored
    if profile.transform == "level":
        levels = current
    elif profile.transform == "log_level":
        levels = [
            (
                math.exp(value)
                if value is not None and math.isfinite(value)
                else None
            )
            for value in current
        ]
    else:
        previous = next(
            (
                observed[index]
                for index in range(local_origin, -1, -1)
                if observed[index] is not None
            ),
            None,
        )
        levels = []
        for value in current:
            if value is None or previous is None:
                levels.append(None)
                previous = None
                continue
            candidate_level = (
                previous * (1.0 + value)
                if profile.transform == "return"
                else previous * math.exp(value)
            )
            next_level: float | None = (
                candidate_level if math.isfinite(candidate_level) else None
            )
            levels.append(next_level)
            previous = next_level
    return tuple(_rounded(value, rounding_digits) for value in levels)


def _base_transform(
    observed: Sequence[float | None], transform: str
) -> list[float | None]:
    values: list[float | None] = []
    previous: float | None = None
    for value in observed:
        if value is None:
            transformed = None
            previous = None
        elif transform == "level":
            transformed = value
        elif transform == "log_level":
            transformed = math.log(value) if value > 0 else None
        elif previous is None:
            transformed = None
        elif transform == "return":
            transformed = value / previous - 1.0 if previous != 0 else None
        else:
            transformed = (
                math.log(value / previous)
                if value > 0 and previous > 0
                else None
            )
        values.append(transformed)
        if value is not None:
            previous = value
    return values


def _difference(values: Sequence[float | None], lag: int) -> list[float | None]:
    output: list[float | None] = []
    for index, value in enumerate(values):
        previous = values[index - lag] if index >= lag else None
        output.append(
            value - previous
            if value is not None and previous is not None
            else None
        )
    return output


def _reference_baseline_payloads(
    rows: Sequence[Mapping[str, Any]],
    folds: Sequence[Mapping[str, Any]],
    rolling_windows: Sequence[int],
    rounding_digits: int,
) -> list[dict[str, JSONValue]]:
    predictions: dict[str, list[dict[str, Any]]] = {
        "naive_random_walk": [],
        **{f"rolling_mean_{window}": [] for window in rolling_windows},
        **{f"rolling_median_{window}": [] for window in rolling_windows},
        "session_seasonal_naive": [],
    }
    for fold in folds:
        if fold.get("status") != "valid":
            continue
        start = _int(fold.get("training_start_index"))
        end = _int(fold.get("training_end_index"))
        target_index = _int(fold.get("target_index"))
        if not 0 <= target_index < len(rows):
            continue
        actual = _optional_float(
            rows[target_index].get("cm_input_observed_value")
        )
        if actual is None:
            continue
        training = [
            _optional_float(rows[index].get("cm_input_observed_value"))
            for index in range(start, end + 1)
        ]
        valid = [value for value in training if value is not None]
        if valid:
            predictions["naive_random_walk"].append(
                _baseline_evaluation(fold, valid[-1], actual)
            )
        for window in rolling_windows:
            if len(valid) >= window:
                sample = valid[-window:]
                predictions[f"rolling_mean_{window}"].append(
                    _baseline_evaluation(
                        fold, sum(sample) / len(sample), actual
                    )
                )
                predictions[f"rolling_median_{window}"].append(
                    _baseline_evaluation(fold, float(median(sample)), actual)
                )
        target_timestamp = _int(
            rows[target_index].get("cm_input_bin_start_utc_ms")
        )
        target_session = classify_histdata_timestamp(
            target_timestamp
        ).session_state
        session_value: float | None = None
        for index in range(end, start - 1, -1):
            value = _optional_float(rows[index].get("cm_input_observed_value"))
            timestamp = _int(rows[index].get("cm_input_bin_start_utc_ms"))
            if (
                value is not None
                and classify_histdata_timestamp(timestamp).session_state
                == target_session
            ):
                session_value = value
                break
        if session_value is not None:
            predictions["session_seasonal_naive"].append(
                _baseline_evaluation(fold, session_value, actual)
            )
    payloads: list[dict[str, JSONValue]] = []
    for name, evaluations in predictions.items():
        base_name = (
            "rolling_mean"
            if name.startswith("rolling_mean_")
            else (
                "rolling_median" if name.startswith("rolling_median_") else name
            )
        )
        baseline_window: int | None = (
            int(name.rsplit("_", 1)[1]) if name[-1:].isdigit() else None
        )
        horizons = sorted({_int(row.get("horizon")) for row in evaluations})
        payloads.append(
            {
                "model": base_name,
                "model_code": EXPONENTIAL_SMOOTHING_BASELINE_CODES[base_name],
                "window": baseline_window,
                "calculation_basis": "shared_regular_grid_folds",
                "horizon_metrics": cast(
                    JSONValue,
                    [
                        _metrics_for_horizon(
                            evaluations, horizon, rounding_digits
                        )
                        for horizon in horizons
                    ],
                ),
                "evaluation_count": len(evaluations),
                "automatic_winner": False,
            }
        )
    return payloads


def _baseline_evaluation(
    fold: Mapping[str, Any], forecast: float, actual: float
) -> dict[str, Any]:
    error = forecast - actual
    return {
        "status": "evaluated",
        "horizon": _int(fold.get("horizon")),
        "forecast": forecast,
        "actual": actual,
        "error": error,
    }


def _build_annotations(
    frame: Any | None,
    evaluations: Sequence[Mapping[str, Any]],
    profile: ExponentialSmoothingProfile,
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
        if timestamp is None or row_id is None:
            continue
        availability.setdefault(
            (_text(row.get("series_id")), _text(row.get("period"))), []
        ).append((timestamp, row_id))
    for values in availability.values():
        values.sort()

    forecast_rows: dict[tuple[str, str, int], dict[str, Any]] = {}
    diagnostic_rows: dict[tuple[str, str, int], dict[str, Any]] = {}
    collisions = 0
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
    for evaluation in selected:
        group = (
            _text(evaluation.get("series_id")),
            _text(evaluation.get("period")),
        )
        forecast = _optional_float(evaluation.get("forecast"))
        if forecast is not None:
            row_id = _first_available_row_id(
                availability.get(group, ()),
                _int(evaluation.get("origin_bin_end_utc_ms")),
            )
            if row_id is not None:
                key = (*group, row_id)
                if key in forecast_rows:
                    collisions += 1
                forecast_rows[key] = _annotation_row(
                    evaluation,
                    input_result,
                    row_id,
                    diagnostic=False,
                )
        error = _optional_float(evaluation.get("error"))
        if error is not None:
            row_id = _first_available_row_id(
                availability.get(group, ()),
                _int(evaluation.get("target_bin_end_utc_ms")),
            )
            if row_id is not None:
                key = (*group, row_id)
                if key in diagnostic_rows:
                    collisions += 1
                diagnostic_rows[key] = _annotation_row(
                    evaluation,
                    input_result,
                    row_id,
                    diagnostic=True,
                )
    merged: dict[tuple[str, str, int], dict[str, Any]] = dict(forecast_rows)
    for key, diagnostic in diagnostic_rows.items():
        if key not in merged:
            merged[key] = diagnostic
            continue
        forecast_annotation = merged[key]
        if forecast_annotation.get("cm_ets_fold_id") == diagnostic.get(
            "cm_ets_fold_id"
        ):
            for name in (
                "cm_ets_actual",
                "cm_ets_error",
                "cm_ets_diagnostic_available",
                "cm_ets_diagnostic_available_at_utc_ms",
            ):
                forecast_annotation[name] = diagnostic.get(name)
        else:
            collisions += 1
    return (
        tuple(merged[key] for key in sorted(merged)),
        collisions,
    )


def _first_available_row_id(
    values: Sequence[tuple[int, int]], threshold: int
) -> int | None:
    return next(
        (row_id for timestamp, row_id in values if timestamp >= threshold), None
    )


def _annotation_row(
    evaluation: Mapping[str, Any],
    input_result: ClassicalModelInputResult,
    projection_row_id: int,
    *,
    diagnostic: bool,
) -> dict[str, Any]:
    fit_status = _text(evaluation.get("fit_status")) or "unavailable"
    trend = _text(evaluation.get("trend_component")) or "none"
    seasonal = _text(evaluation.get("seasonal_component")) or "none"
    error = _text(evaluation.get("error_component")) or "add"
    initialization = (
        _text(evaluation.get("initialization_method")) or "estimated"
    )
    row = {
        "series_id": _text(evaluation.get("series_id")),
        "period": _text(evaluation.get("period")),
        "row_id": projection_row_id,
        "cm_ets_schema_version": EXPONENTIAL_SMOOTHING_TRAINING_PROJECTION_SCHEMA_VERSION,
        "cm_ets_input_derivation_id": _text(
            input_result.contract.get("derivation_id")
        ),
        "cm_ets_model_id": _text(evaluation.get("model_id")),
        "cm_ets_family_code": _int(evaluation.get("family_code")),
        "cm_ets_specification_code": _int(evaluation.get("specification_code")),
        "cm_ets_error_code": EXPONENTIAL_SMOOTHING_COMPONENT_CODES[error],
        "cm_ets_trend_code": EXPONENTIAL_SMOOTHING_COMPONENT_CODES[trend],
        "cm_ets_seasonal_code": EXPONENTIAL_SMOOTHING_COMPONENT_CODES[seasonal],
        "cm_ets_damped_trend": bool(evaluation.get("damped_trend", False)),
        "cm_ets_initialization_code": EXPONENTIAL_SMOOTHING_INITIALIZATION_CODES[
            initialization
        ],
        "cm_ets_fit_status_code": EXPONENTIAL_SMOOTHING_FIT_STATUS_CODES.get(
            fit_status, 1
        ),
        "cm_ets_converged": bool(evaluation.get("converged", False)),
        "cm_ets_fold_id": _int(evaluation.get("fold_id")),
        "cm_ets_origin_row_id": evaluation.get("origin_row_id"),
        "cm_ets_target_row_id": evaluation.get("target_row_id"),
        "cm_ets_horizon": _int(evaluation.get("horizon")),
        "cm_ets_forecast": _optional_float(evaluation.get("forecast")),
        "cm_ets_forecast_available": not diagnostic,
        "cm_ets_forecast_available_at_utc_ms": _int(
            evaluation.get("origin_bin_end_utc_ms")
        ),
        "cm_ets_actual": (
            _optional_float(evaluation.get("actual")) if diagnostic else None
        ),
        "cm_ets_error": (
            _optional_float(evaluation.get("error")) if diagnostic else None
        ),
        "cm_ets_diagnostic_available": diagnostic,
        "cm_ets_diagnostic_available_at_utc_ms": (
            _int(evaluation.get("target_bin_end_utc_ms"))
            if diagnostic
            else None
        ),
        "cm_ets_diagnostic_only": diagnostic,
        "cm_ets_original_scale": True,
        "cm_ets_training_eligible": not diagnostic,
    }
    return row


def _training_projection_metadata(
    profile: ExponentialSmoothingProfile,
    input_result: ClassicalModelInputResult,
    annotation_count: int,
    collision_count: int,
) -> dict[str, JSONValue]:
    return {
        "schema_version": EXPONENTIAL_SMOOTHING_TRAINING_PROJECTION_SCHEMA_VERSION,
        "grain": "row",
        "identity_fields": ["series_id", "period", "row_id"],
        "timestamp_is_sole_identity": False,
        "mapping_policy": "first_source_row_at_or_after_availability",
        "collision_policy": "latest_origin_wins",
        "collision_count": collision_count,
        "annotation_count": annotation_count,
        "projection_specification_id": profile.projection_specification_id,
        "projection_horizon": profile.projection_horizon,
        "input_derivation_id": input_result.contract.get("derivation_id"),
        "column_names": list(EXPONENTIAL_SMOOTHING_COLUMNS),
        "forecast_time_values_use_future": False,
        "diagnostics_marked_post_observation": True,
        "observed_columns_overwritten": False,
    }


def _base_payload(
    input_result: ClassicalModelInputResult,
    fingerprint: Mapping[str, JSONValue],
    profile: ExponentialSmoothingProfile,
) -> dict[str, JSONValue]:
    input_contract = input_result.contract
    input_regularization = _mapping(input_contract.get("regularization"))
    return {
        "schema_version": EXPONENTIAL_SMOOTHING_SCHEMA_VERSION,
        "advisory": True,
        "target_axis": dict(_mapping(input_contract.get("target_axis"))),
        "reference_fingerprint_id": input_contract.get(
            "reference_fingerprint_id"
        )
        or fingerprint.get("fingerprint_id"),
        "input_schema_version": CLASSICAL_MODEL_INPUT_SCHEMA_VERSION,
        "input_derivation_id": input_contract.get("derivation_id"),
        "input_status": input_contract.get("status"),
        "calculation_basis": "regular_grid_rolling_origin",
        "configuration": profile.to_metadata(),
        "input_transform_policy": dict(
            _mapping(input_contract.get("transform_policy"))
        ),
        "input_missingness_policy": {
            "expected_closure_policy": input_regularization.get(
                "expected_closure_policy"
            ),
            "unexpected_missing_policy": input_regularization.get(
                "unexpected_missing_policy"
            ),
            "expected_closure_count": input_regularization.get(
                "expected_closure_count"
            ),
            "unexpected_missing_count": input_regularization.get(
                "unexpected_missing_count"
            ),
            "expected_closure_grid_rows_retained": input_regularization.get(
                "expected_closure_grid_rows_retained"
            ),
            "expected_closure_model_observations_omitted": (
                input_regularization.get(
                    "expected_closure_model_observations_omitted"
                )
            ),
        },
        "missing_observation_policy": "reset_to_trailing_contiguous_segment",
        "forward_fill_policy": "never",
        "original_scale_forecasts": True,
        "automatic_search": False,
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
) -> ExponentialSmoothingResult:
    configuration = _mapping(base.get("configuration"))
    diagnostics: dict[str, JSONValue] = {
        **dict(base),
        "status": status,
        "reason": reason,
        "limitations": [reason],
        "backend": {
            "provider": "statsmodels",
            "version": None,
            "available": False,
            "import_basis": "optional_models_extra",
        },
        "fit_summary": {
            "schema_version": EXPONENTIAL_SMOOTHING_FIT_SCHEMA_VERSION,
            "fit_attempt_count": 0,
            "status_counts": {},
            "reason_counts": {reason: 1},
            "warning_counts": {},
            "failed_fit_count": 0,
            "limited_fit_count": 0,
            "fit_samples": [],
            "fit_samples_truncated": False,
        },
        "evaluation": {
            "schema_version": EXPONENTIAL_SMOOTHING_EVALUATION_SCHEMA_VERSION,
            "calculation_basis": "regular_grid_rolling_origin",
            "original_scale": True,
            "model_count": len(
                _mapping_rows(configuration.get("specifications"))
            ),
            "fold_count": len(input_result.folds),
            "evaluated_fold_count": 0,
            "skipped_evaluation_count": 0,
            "models": [],
            "reference_baselines": [],
            "comparison_semantics": "descriptive_shared_folds_only",
            "automatic_winner": False,
        },
        "resource_usage": {
            "limits": dict(
                _mapping(
                    _mapping(input_result.contract.get("configuration")).get(
                        "resources"
                    )
                )
            ),
            "estimated_working_memory_bytes": 0,
            "memory_limit_exceeded": False,
            "fit_attempt_count": 0,
            "wall_time_limit_enforced": True,
            "wall_time_observed_in_payload": False,
        },
        "training_projection": {
            "schema_version": EXPONENTIAL_SMOOTHING_TRAINING_PROJECTION_SCHEMA_VERSION,
            "grain": "row",
            "identity_fields": ["series_id", "period", "row_id"],
            "timestamp_is_sole_identity": False,
            "mapping_policy": "first_source_row_at_or_after_availability",
            "collision_policy": "latest_origin_wins",
            "collision_count": 0,
            "annotation_count": 0,
            "projection_specification_id": configuration.get(
                "projection_specification_id"
            ),
            "projection_horizon": configuration.get("projection_horizon"),
            "input_derivation_id": input_result.contract.get("derivation_id"),
            "column_names": list(EXPONENTIAL_SMOOTHING_COLUMNS),
            "forecast_time_values_use_future": False,
            "diagnostics_marked_post_observation": True,
            "observed_columns_overwritten": False,
        },
        "fit_duration_included": False,
    }
    return ExponentialSmoothingResult(diagnostics, (), input_result)


def _ensure_projection_columns(frame: Any) -> Any:
    import polars as pl

    expressions = []
    for name, dtype in _projection_dtypes().items():
        if name in frame.columns:
            expressions.append(pl.col(name).cast(dtype).alias(name))
        else:
            expressions.append(pl.lit(None).cast(dtype).alias(name))
    return frame.with_columns(expressions)


def _projection_dtypes() -> dict[str, Any]:
    import polars as pl

    strings = {
        "cm_ets_schema_version",
        "cm_ets_input_derivation_id",
        "cm_ets_model_id",
    }
    booleans = {
        "cm_ets_damped_trend",
        "cm_ets_converged",
        "cm_ets_forecast_available",
        "cm_ets_diagnostic_available",
        "cm_ets_diagnostic_only",
        "cm_ets_original_scale",
        "cm_ets_training_eligible",
    }
    floats = {"cm_ets_forecast", "cm_ets_actual", "cm_ets_error"}
    return {
        name: (
            pl.Utf8
            if name in strings
            else (
                pl.Boolean
                if name in booleans
                else pl.Float64 if name in floats else pl.Int64
            )
        )
        for name in EXPONENTIAL_SMOOTHING_COLUMNS
    }


def _load_backend() -> _Backend | None:
    try:
        statsmodels = importlib.import_module("statsmodels")
        holtwinters = importlib.import_module("statsmodels.tsa.holtwinters")
        ets = importlib.import_module(
            "statsmodels.tsa.exponential_smoothing.ets"
        )
        version = getattr(statsmodels, "__version__", "") or (
            importlib.metadata.version("statsmodels")
        )
        return _Backend(
            version=str(version),
            holt_winters=holtwinters.ExponentialSmoothing,
            ets_model=ets.ETSModel,
        )
    except (
        ImportError,
        ModuleNotFoundError,
        importlib.metadata.PackageNotFoundError,
    ):
        return None


def _model_id(
    specification: ExponentialSmoothingSpecification,
    input_derivation_id: str,
    backend_version: str,
) -> str:
    payload = {
        "schema_version": EXPONENTIAL_SMOOTHING_CONFIGURATION_SCHEMA_VERSION,
        "input_derivation_id": input_derivation_id,
        "backend": "statsmodels",
        "backend_version": backend_version,
        "specification": specification.to_metadata(),
    }
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _target_sort_key(value: Mapping[str, JSONValue]) -> tuple[str, ...]:
    axis = _mapping(value.get("target_axis"))
    return tuple(
        _text(axis.get(key))
        for key in ("data_format", "timeframe", "symbol", "period")
    )


def _mapping(value: object) -> Mapping[str, JSONValue]:
    return (
        cast(Mapping[str, JSONValue], value)
        if isinstance(value, Mapping)
        else {}
    )


def _mapping_rows(value: object) -> list[Mapping[str, JSONValue]]:
    if not isinstance(value, list):
        return []
    return [
        cast(Mapping[str, JSONValue], row)
        for row in value
        if isinstance(row, Mapping)
    ]


def _text(value: object) -> str:
    return str(value or "")


def _int(value: object) -> int:
    try:
        return int(cast(Any, value) or 0)
    except (TypeError, ValueError):
        return 0


def _optional_int(value: object) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(cast(Any, value))
    except (TypeError, ValueError):
        return None


def _float(value: object) -> float:
    result = _optional_float(value)
    return result if result is not None else 0.0


def _optional_float(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(cast(Any, value))
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _rounded(value: float | None, digits: int) -> float | None:
    return (
        round(value, digits)
        if value is not None and math.isfinite(value)
        else None
    )
