"""Optional explicit-order AR, ARMA, and ARIMA model diagnostics.

The family consumes the regular grid and rolling-origin folds established by
the classical-model input contract.  Statsmodels is imported lazily, all model
specifications are explicit, and every failure remains bounded and advisory.
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
from contextlib import nullcontext
from dataclasses import dataclass, field
from statistics import median
from typing import Any, cast

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
    _trailing_contiguous_indexes,
)
from histdatacom.data_quality.limits import bounded_report_limit
from histdatacom.data_quality.training_features import (
    AUTOREGRESSIVE_COLUMNS,
    AUTOREGRESSIVE_FAMILY_COLUMN_SUFFIXES,
    ensure_tick_training_features,
)
from histdatacom.runtime_contracts import JSONValue

AUTOREGRESSIVE_SCHEMA_VERSION = "histdatacom.autoregressive.v1"
AUTOREGRESSIVE_CONFIGURATION_SCHEMA_VERSION = (
    "histdatacom.autoregressive-configuration.v1"
)
AUTOREGRESSIVE_FIT_SCHEMA_VERSION = "histdatacom.autoregressive-fit-result.v1"
AUTOREGRESSIVE_FORECAST_SCHEMA_VERSION = (
    "histdatacom.autoregressive-forecast.v1"
)
AUTOREGRESSIVE_EVALUATION_SCHEMA_VERSION = (
    "histdatacom.autoregressive-evaluation.v1"
)
AUTOREGRESSIVE_TRAINING_PROJECTION_SCHEMA_VERSION = (
    "histdatacom.autoregressive-training-projection.v1"
)
AUTOREGRESSIVE_SUMMARY_SCHEMA_VERSION = "histdatacom.autoregressive-summary.v1"
AUTOREGRESSIVE_SUMMARY_METADATA_KEY = (
    "time_series_fingerprint_autoregressive_summary"
)
AUTOREGRESSIVE_BOUNDED_PAYLOAD_KEY = "fingerprint_autoregressive"

DEFAULT_AUTOREGRESSIVE_SUMMARY_TARGET_LIMIT = 16
DEFAULT_AUTOREGRESSIVE_ROLLING_WINDOWS = (5, 20)
DEFAULT_AUTOREGRESSIVE_ROUNDING_DIGITS = 12
MAX_AUTOREGRESSIVE_SPECIFICATIONS = 32
MAX_AUTOREGRESSIVE_ORDER = 64
MAX_AUTOREGRESSIVE_FIXED_PARAMETERS = 32

AUTOREGRESSIVE_FAMILIES = ("ar", "arma", "arima")
AUTOREGRESSIVE_FAMILY_CODES = {"ar": 1, "arma": 2, "arima": 3}
AUTOREGRESSIVE_TREND_CODES = {"n": 0, "c": 1, "t": 2, "ct": 3}
AUTOREGRESSIVE_INITIALIZATION_CODES = {
    "default": 1,
    "stationary": 2,
    "approximate_diffuse": 3,
}
AUTOREGRESSIVE_FIT_STATUS_CODES = {
    "unavailable": 1,
    "skipped": 2,
    "failed": 3,
    "limited": 4,
    "fitted": 5,
    "converged": 6,
}
AUTOREGRESSIVE_REASON_CODES = {
    "": 0,
    "dependency_unavailable": 1,
    "input_contract_unavailable": 2,
    "insufficient_folds": 3,
    "insufficient_lags": 4,
    "invalid_order": 5,
    "invalid_configuration": 6,
    "nonstationary_configuration": 7,
    "noninvertible_configuration": 8,
    "zero_variance": 9,
    "optimizer_failure": 10,
    "singularity": 11,
    "numerical_overflow": 12,
    "numerical_failure": 13,
    "resource_limit": 14,
    "timeout": 15,
    "backend_failure": 16,
    "target_unavailable": 17,
    "inverse_transform_unavailable": 18,
}
AUTOREGRESSIVE_WARNING_CODES = {
    "ConvergenceWarning": "convergence_warning",
    "RuntimeWarning": "runtime_warning",
    "ValueWarning": "value_warning",
    "UserWarning": "user_warning",
}

_SPECIFICATION_ID = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,63}$")
_PARAMETER_NAME = re.compile(r"^[A-Za-z0-9_.()\[\]-]{1,96}$")
_ESTIMATION_METHODS = {
    "statespace",
    "innovations_mle",
    "hannan_rissanen",
    "burg",
    "yule_walker",
}


@dataclass(frozen=True, slots=True)
class AutoregressiveSpecification:
    """One explicit AR, ARMA, or ARIMA specification."""

    specification_id: str
    family: str
    p: int
    d: int = 0
    q: int = 0
    trend: str = "n"
    initialization_method: str = "default"
    estimation_method: str = "statespace"
    enforce_stationarity: bool = True
    enforce_invertibility: bool = True
    concentrate_scale: bool = False
    fixed_parameters: tuple[tuple[str, float], ...] = ()
    max_iterations: int = 200

    def __post_init__(self) -> None:
        if not _SPECIFICATION_ID.fullmatch(self.specification_id):
            raise ValueError("invalid autoregressive specification_id")
        if self.family not in AUTOREGRESSIVE_FAMILIES:
            raise ValueError("unsupported autoregressive family")
        for name, value in (("p", self.p), ("d", self.d), ("q", self.q)):
            if value < 0 or value > MAX_AUTOREGRESSIVE_ORDER:
                raise ValueError(f"{name} must be between 0 and 64")
        if self.family == "ar" and not (
            self.p >= 1 and self.d == 0 and self.q == 0
        ):
            raise ValueError("AR requires p >= 1 with d = q = 0")
        if self.family == "arma" and not (
            self.p >= 1 and self.d == 0 and self.q >= 1
        ):
            raise ValueError("ARMA requires p >= 1, q >= 1, and d = 0")
        if self.family == "arima" and self.d < 1:
            raise ValueError("ARIMA requires d >= 1; use AR or ARMA when d = 0")
        if self.trend not in AUTOREGRESSIVE_TREND_CODES:
            raise ValueError("trend must be n, c, t, or ct")
        if self.d > 0 and "c" in self.trend:
            raise ValueError(
                "integrated ARIMA cannot include a constant trend term"
            )
        if (
            self.initialization_method
            not in AUTOREGRESSIVE_INITIALIZATION_CODES
        ):
            raise ValueError("unsupported autoregressive initialization_method")
        if self.initialization_method == "stationary" and self.d > 0:
            raise ValueError("stationary initialization requires d = 0")
        if self.estimation_method not in _ESTIMATION_METHODS:
            raise ValueError("unsupported autoregressive estimation_method")
        if self.family == "arma" and self.estimation_method in {
            "burg",
            "yule_walker",
        }:
            raise ValueError("AR-only estimation method cannot fit ARMA")
        if self.family == "arima" and self.estimation_method not in {
            "statespace",
            "innovations_mle",
        }:
            raise ValueError(
                "integrated ARIMA requires statespace or innovations_mle"
            )
        if self.fixed_parameters and self.estimation_method != "statespace":
            raise ValueError("fixed parameters require statespace estimation")
        if len(self.fixed_parameters) > MAX_AUTOREGRESSIVE_FIXED_PARAMETERS:
            raise ValueError("too many fixed autoregressive parameters")
        seen: set[str] = set()
        for name, fixed_value in self.fixed_parameters:
            if (
                not _PARAMETER_NAME.fullmatch(name)
                or name in seen
                or not math.isfinite(fixed_value)
            ):
                raise ValueError(
                    "fixed parameters must be unique finite scalars"
                )
            seen.add(name)
        if self.max_iterations < 1:
            raise ValueError("max_iterations must be positive")

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return a stable serializable model specification."""
        return {
            "schema_version": AUTOREGRESSIVE_CONFIGURATION_SCHEMA_VERSION,
            "specification_id": self.specification_id,
            "family": self.family,
            "family_code": AUTOREGRESSIVE_FAMILY_CODES[self.family],
            "order": [self.p, self.d, self.q],
            "p": self.p,
            "d": self.d,
            "q": self.q,
            "trend": self.trend,
            "trend_code": AUTOREGRESSIVE_TREND_CODES[self.trend],
            "initialization_method": self.initialization_method,
            "estimation_method": self.estimation_method,
            "enforce_stationarity": self.enforce_stationarity,
            "enforce_invertibility": self.enforce_invertibility,
            "concentrate_scale": self.concentrate_scale,
            "fixed_parameters": cast(
                JSONValue,
                [
                    {"parameter": name, "value": value}
                    for name, value in self.fixed_parameters
                ],
            ),
            "max_iterations": self.max_iterations,
            "automatic_order_selection": False,
        }


def _default_specifications() -> tuple[AutoregressiveSpecification, ...]:
    return (
        AutoregressiveSpecification("ar-1", "ar", 1, trend="c"),
        AutoregressiveSpecification("arma-1-1", "arma", 1, q=1, trend="c"),
        AutoregressiveSpecification("arima-1-1-1", "arima", 1, d=1, q=1),
    )


@dataclass(frozen=True, slots=True)
class AutoregressiveProfile:
    """Explicit fitted autoregressive controls; disabled by default."""

    enabled: bool = False
    specifications: tuple[AutoregressiveSpecification, ...] = field(
        default_factory=_default_specifications
    )
    projection_specification_ids: tuple[str, ...] = (
        "ar-1",
        "arma-1-1",
        "arima-1-1-1",
    )
    projection_horizon: int = 1
    baseline_rolling_windows: tuple[int, ...] = (
        DEFAULT_AUTOREGRESSIVE_ROLLING_WINDOWS
    )
    compare_exponential_smoothing: bool = True
    rounding_digits: int = DEFAULT_AUTOREGRESSIVE_ROUNDING_DIGITS

    def __post_init__(self) -> None:
        if not self.specifications:
            raise ValueError(
                "at least one autoregressive specification is required"
            )
        if len(self.specifications) > MAX_AUTOREGRESSIVE_SPECIFICATIONS:
            raise ValueError("too many autoregressive specifications")
        identifiers = tuple(
            item.specification_id for item in self.specifications
        )
        if len(set(identifiers)) != len(identifiers):
            raise ValueError("autoregressive specification IDs must be unique")
        selected = set(self.projection_specification_ids)
        if not selected or not selected.issubset(identifiers):
            raise ValueError(
                "projection specification IDs must select configured models"
            )
        selected_families = {
            item.family
            for item in self.specifications
            if item.specification_id in selected
        }
        if len(selected_families) != len(selected):
            raise ValueError(
                "select at most one projection per autoregressive family"
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
                JSONValue, [item.to_metadata() for item in self.specifications]
            ),
            "projection_specification_ids": list(
                self.projection_specification_ids
            ),
            "projection_horizon": self.projection_horizon,
            "baseline_rolling_windows": list(self.baseline_rolling_windows),
            "compare_exponential_smoothing": self.compare_exponential_smoothing,
            "rounding_digits": self.rounding_digits,
            "automatic_order_selection": False,
            "automatic_winner": False,
        }


@dataclass(frozen=True, slots=True)
class AutoregressiveResult:
    """Bounded diagnostics plus durable row-key annotations."""

    diagnostics: Mapping[str, JSONValue]
    annotations: tuple[Mapping[str, Any], ...]
    input_result: ClassicalModelInputResult


@dataclass(frozen=True, slots=True)
class _Backend:
    version: str
    arima: Any


@dataclass(frozen=True, slots=True)
class _FitOutcome:
    status: str
    reason: str
    forecasts: tuple[float, ...]
    parameters: Mapping[str, float]
    warning_codes: tuple[str, ...]
    converged: bool
    stationary: bool | None
    invertible: bool | None
    ar_root_min_modulus: float | None
    ma_root_min_modulus: float | None
    covariance_condition_number: float | None
    effective_observation_count: int


def autoregressive_from_training_frame(
    frame: Any | None,
    fingerprint: Mapping[str, JSONValue],
    *,
    input_profile: ClassicalModelInputProfile | None = None,
    profile: AutoregressiveProfile | None = None,
    exponential_smoothing: Mapping[str, JSONValue] | None = None,
    target: Any | None = None,
) -> AutoregressiveResult:
    """Regularize an enriched tick frame and evaluate configured models."""
    selected_input = input_profile or ClassicalModelInputProfile(enabled=True)
    input_result = build_classical_model_input(
        frame,
        fingerprint,
        profile=selected_input,
        target=target,
    )
    return autoregressive_from_model_input(
        frame,
        input_result,
        fingerprint,
        input_profile=selected_input,
        profile=profile,
        exponential_smoothing=exponential_smoothing,
        target=target,
    )


def autoregressive_from_model_input(
    frame: Any | None,
    input_result: ClassicalModelInputResult,
    fingerprint: Mapping[str, JSONValue],
    *,
    input_profile: ClassicalModelInputProfile,
    profile: AutoregressiveProfile | None = None,
    exponential_smoothing: Mapping[str, JSONValue] | None = None,
    target: Any | None = None,
) -> AutoregressiveResult:
    """Fit explicit AR, ARMA, and ARIMA specifications over #421 folds."""
    selected = profile or AutoregressiveProfile(enabled=True)
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
        target=target,
    )


def autoregressive_diagnostics_from_training_frame(
    frame: Any | None,
    fingerprint: Mapping[str, JSONValue],
    *,
    input_profile: ClassicalModelInputProfile | None = None,
    profile: AutoregressiveProfile | None = None,
    exponential_smoothing: Mapping[str, JSONValue] | None = None,
    target: Any | None = None,
) -> dict[str, JSONValue]:
    """Return only serializable fitted-family diagnostics."""
    return dict(
        autoregressive_from_training_frame(
            frame,
            fingerprint,
            input_profile=input_profile,
            profile=profile,
            exponential_smoothing=exponential_smoothing,
            target=target,
        ).diagnostics
    )


def project_autoregressive_onto_training_frame(
    frame: Any,
    result: AutoregressiveResult,
    *,
    target: Any | None = None,
) -> Any:
    """Join bounded autoregressive annotations by durable row identity."""
    import polars as pl

    columns = set(getattr(frame, "columns", ()))
    if not {"series_id", "period", "row_id"}.issubset(columns):
        enriched = ensure_tick_training_features(frame, target=target)
    else:
        enriched = frame
    left = enriched.drop(
        [name for name in AUTOREGRESSIVE_COLUMNS if name in enriched.columns]
    ).with_row_index("__cm_autoregressive_original_order")
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
    return projected.sort("__cm_autoregressive_original_order").drop(
        "__cm_autoregressive_original_order"
    )


def autoregressive_summary(
    findings: Iterable[QualityFinding],
    *,
    target_limit: int | None = DEFAULT_AUTOREGRESSIVE_SUMMARY_TARGET_LIMIT,
) -> dict[str, JSONValue] | None:
    """Return bounded report metadata for autoregressive results."""
    targets: list[dict[str, JSONValue]] = []
    statuses: Counter[str] = Counter()
    for finding in findings:
        fingerprint = _mapping(finding.metadata.get("time_series_fingerprint"))
        payload = _mapping(fingerprint.get("autoregressive"))
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
        default_limit=DEFAULT_AUTOREGRESSIVE_SUMMARY_TARGET_LIMIT,
        allow_unbounded=True,
    )
    included = limit.slice(targets)
    omitted = len(targets) - len(included)
    return {
        "schema_version": AUTOREGRESSIVE_SUMMARY_SCHEMA_VERSION,
        "advisory": True,
        "target_count": len(targets),
        "included_target_count": len(included),
        "omitted_target_count": omitted,
        "truncated": omitted > 0,
        "status_counts": dict(sorted(statuses.items())),
        "target_summaries": cast(JSONValue, included),
        "limit_metadata": {"targets": limit.limit_payload()},
    }


def format_autoregressive_summary_lines(
    summary: Mapping[str, JSONValue] | None,
) -> tuple[str, ...]:
    """Return concise human-readable fitted-family lines."""
    if not summary:
        return ()
    statuses = _mapping(summary.get("status_counts"))
    lines = [
        "",
        "Autoregressive models",
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
    profile: AutoregressiveProfile,
    backend: _Backend,
    *,
    exponential_smoothing: Mapping[str, JSONValue] | None,
    target: Any | None,
) -> AutoregressiveResult:
    rows = cast(list[dict[str, Any]], input_result.regularized_frame.to_dicts())
    folds = [dict(fold) for fold in input_result.folds]
    origins = _folds_by_origin(folds)
    resources = input_profile.resources
    specifications = profile.specifications[: resources.max_candidate_orders]
    limitations: list[str] = []
    estimated_memory = _estimated_working_memory_bytes(
        len(rows), len(folds), len(specifications)
    )
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
            training_start = _int(origin.get("training_start_index"))
            training_end = _int(origin.get("training_end_index"))
            indexes = _trailing_contiguous_indexes(
                rows,
                training_start,
                training_end,
                _text(origin.get("series_id")),
                _text(origin.get("period")),
            )
            values = tuple(
                _float(rows[index].get("cm_input_value")) for index in indexes
            )
            max_horizon = max(
                _int(fold.get("horizon")) for fold in origin_folds
            )
            outcome = _fit_specification(
                specification,
                values,
                max_horizon,
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
            )
            model_fit_samples.append(fit_sample)
            all_fit_samples.append(fit_sample)
            original_forecasts = _inverse_forecasts(
                rows,
                training_end,
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
    ets_references = _exponential_smoothing_references(
        (
            exponential_smoothing
            if profile.compare_exponential_smoothing
            else None
        ),
        enabled=profile.compare_exponential_smoothing,
    )
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
        limitations.append("insufficient_lags")
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
            "import_basis": "optional_models_extra",
        },
        "fit_summary": {
            "schema_version": AUTOREGRESSIVE_FIT_SCHEMA_VERSION,
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
            "fit_samples_truncated": (
                len(all_fit_samples) > resources.max_retained_diagnostics
            ),
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
            "schema_version": AUTOREGRESSIVE_EVALUATION_SCHEMA_VERSION,
            "calculation_basis": "regular_grid_rolling_origin",
            "original_scale": True,
            "model_count": len(model_payloads),
            "fold_count": len(folds),
            "evaluated_fold_count": evaluated_count,
            "skipped_evaluation_count": len(all_evaluations) - evaluated_count,
            "forecast_coverage_rate": _rate(
                evaluated_count, len(all_evaluations), profile.rounding_digits
            ),
            "models": cast(JSONValue, model_payloads),
            "reference_baselines": cast(JSONValue, baselines),
            "reference_exponential_smoothing": cast(JSONValue, ets_references),
            "comparison_semantics": "descriptive_shared_folds_only",
            "automatic_winner": False,
        },
        "training_projection": _training_projection_metadata(
            profile, input_result, len(annotations), collisions
        ),
        "fit_duration_included": False,
    }
    return AutoregressiveResult(diagnostics, annotations, input_result)


def _fit_specification(
    specification: AutoregressiveSpecification,
    values: Sequence[float],
    horizon: int,
    backend: _Backend,
) -> _FitOutcome:
    effective_count = max(
        0, len(values) - specification.d - max(specification.p, specification.q)
    )
    minimum = max(8, specification.p + specification.q + specification.d + 3)
    if len(values) < minimum or effective_count < 3:
        return _empty_fit("skipped", "insufficient_lags", effective_count)
    if max(values) - min(values) <= 1e-15:
        return _empty_fit("skipped", "zero_variance", effective_count)
    try:
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always")
            fitted = _statsmodels_fit(specification, values, backend)
            raw_forecasts = fitted.forecast(horizon)
            stationary, ar_min = _root_status(getattr(fitted, "arroots", ()))
            invertible, ma_min = _root_status(getattr(fitted, "maroots", ()))
            condition_number = _covariance_condition_number(fitted)
        forecasts = tuple(float(value) for value in raw_forecasts)
        warning_codes = _warning_codes(captured)
        if not forecasts or any(
            not math.isfinite(value) for value in forecasts
        ):
            return _empty_fit(
                "failed",
                "numerical_failure",
                effective_count,
                warning_codes=warning_codes,
            )
        converged = _fit_converged(fitted, warning_codes)
        status = (
            "limited"
            if "convergence_warning" in warning_codes or not converged
            else "converged"
        )
        reason = "optimizer_failure" if status == "limited" else ""
        return _FitOutcome(
            status=status,
            reason=reason,
            forecasts=forecasts,
            parameters=_fitted_parameters(fitted),
            warning_codes=warning_codes,
            converged=converged,
            stationary=stationary,
            invertible=invertible,
            ar_root_min_modulus=ar_min,
            ma_root_min_modulus=ma_min,
            covariance_condition_number=condition_number,
            effective_observation_count=effective_count,
        )
    except (ArithmeticError, FloatingPointError, OverflowError):
        return _empty_fit("failed", "numerical_overflow", effective_count)
    except Exception as exc:  # backend-specific safety boundary
        return _empty_fit(
            "failed", _backend_failure_reason(exc), effective_count
        )


def _statsmodels_fit(
    specification: AutoregressiveSpecification,
    values: Sequence[float],
    backend: _Backend,
) -> Any:
    model = backend.arima(
        values,
        order=(specification.p, specification.d, specification.q),
        seasonal_order=(0, 0, 0, 0),
        trend=specification.trend,
        enforce_stationarity=specification.enforce_stationarity,
        enforce_invertibility=specification.enforce_invertibility,
        concentrate_scale=specification.concentrate_scale,
        missing="raise",
        validate_specification=True,
    )
    if specification.initialization_method == "stationary":
        model.initialize_stationary()
    elif specification.initialization_method == "approximate_diffuse":
        model.initialize_approximate_diffuse()
    fixed = dict(specification.fixed_parameters)
    unknown = sorted(set(fixed) - set(model.param_names))
    if unknown:
        raise ValueError(
            "fixed parameter is not present in model specification"
        )
    method_kwargs: dict[str, Any] | None = None
    if specification.estimation_method == "statespace":
        method_kwargs = {"maxiter": specification.max_iterations, "disp": 0}
    elif specification.estimation_method == "innovations_mle":
        method_kwargs = {
            "minimize_kwargs": {
                "options": {"maxiter": specification.max_iterations}
            }
        }
    context = model.fix_params(fixed) if fixed else nullcontext()
    with context:
        return model.fit(
            method=specification.estimation_method,
            method_kwargs=method_kwargs,
            low_memory=False,
        )


def _empty_fit(
    status: str,
    reason: str,
    effective_count: int,
    *,
    warning_codes: tuple[str, ...] = (),
) -> _FitOutcome:
    return _FitOutcome(
        status,
        reason,
        (),
        {},
        warning_codes,
        False,
        None,
        None,
        None,
        None,
        None,
        effective_count,
    )


def _fit_sample(
    outcome: _FitOutcome,
    specification: AutoregressiveSpecification,
    model_id: str,
    fold: Mapping[str, Any],
    indexes: Sequence[int],
    observation_count: int,
) -> dict[str, JSONValue]:
    return {
        "schema_version": AUTOREGRESSIVE_FIT_SCHEMA_VERSION,
        "model_id": model_id,
        "specification_id": specification.specification_id,
        "family": specification.family,
        "order": [specification.p, specification.d, specification.q],
        "status": outcome.status,
        "reason": outcome.reason or None,
        "converged": outcome.converged,
        "stationary": outcome.stationary,
        "invertible": outcome.invertible,
        "ar_root_min_modulus": _rounded(outcome.ar_root_min_modulus, 12),
        "ma_root_min_modulus": _rounded(outcome.ma_root_min_modulus, 12),
        "covariance_condition_number": _rounded(
            outcome.covariance_condition_number, 12
        ),
        "series_id": _text(fold.get("series_id")),
        "period": _text(fold.get("period")),
        "origin_bin_end_utc_ms": _int(fold.get("origin_bin_end_utc_ms")),
        "observation_count": observation_count,
        "effective_observation_count": outcome.effective_observation_count,
        "segment_start_index": indexes[0] if indexes else None,
        "segment_end_index": indexes[-1] if indexes else None,
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
    specification: AutoregressiveSpecification,
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
    return {
        "schema_version": AUTOREGRESSIVE_FORECAST_SCHEMA_VERSION,
        "status": status,
        "reason": reason or None,
        "series_id": _text(fold.get("series_id")),
        "period": _text(fold.get("period")),
        "model_id": model_id,
        "specification_id": specification.specification_id,
        "specification_code": specification_code,
        "family": specification.family,
        "family_code": AUTOREGRESSIVE_FAMILY_CODES[specification.family],
        "p": specification.p,
        "d": specification.d,
        "q": specification.q,
        "trend": specification.trend,
        "trend_code": AUTOREGRESSIVE_TREND_CODES[specification.trend],
        "initialization_method": specification.initialization_method,
        "estimation_method": specification.estimation_method,
        "fit_status": outcome.status,
        "fit_reason": outcome.reason or None,
        "converged": outcome.converged,
        "stationary": outcome.stationary,
        "invertible": outcome.invertible,
        "ar_root_min_modulus": _rounded(
            outcome.ar_root_min_modulus, rounding_digits
        ),
        "ma_root_min_modulus": _rounded(
            outcome.ma_root_min_modulus, rounding_digits
        ),
        "covariance_condition_number": _rounded(
            outcome.covariance_condition_number, rounding_digits
        ),
        "effective_observation_count": outcome.effective_observation_count,
        "fold_id": _int(fold.get("fold_id")),
        "origin_row_id": fold.get("origin_row_id"),
        "target_row_id": fold.get("target_row_id"),
        "origin_bin_end_utc_ms": _int(fold.get("origin_bin_end_utc_ms")),
        "target_bin_end_utc_ms": _int(fold.get("target_bin_end_utc_ms")),
        "horizon": horizon,
        "transformed_forecast": _rounded(transformed, rounding_digits),
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
        "residual_state_reused_across_origins": False,
        "automatic_winner": False,
    }


def _model_evaluation_payload(
    specification: AutoregressiveSpecification,
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
            sum(absolute) / len(absolute) if absolute else None, rounding_digits
        ),
        "rmse": _rounded(
            math.sqrt(sum(squared) / len(squared)) if squared else None,
            rounding_digits,
        ),
        "bias": _rounded(
            sum(errors) / len(errors) if errors else None, rounding_digits
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
                median(abs(value - center) for value in values), rounding_digits
            ),
        }
    return {
        "parameter_count": len(summaries),
        "parameters": summaries,
        "bounded": True,
    }


def _exponential_smoothing_references(
    payload: Mapping[str, JSONValue] | None,
    *,
    enabled: bool,
) -> list[dict[str, JSONValue]]:
    if not enabled:
        return [{"status": "not_requested", "automatic_winner": False}]
    selected = _mapping(payload)
    models = _mapping_rows(_mapping(selected.get("evaluation")).get("models"))
    if not models:
        return [
            {
                "status": "unavailable",
                "reason": "exponential_smoothing_not_enabled",
                "automatic_winner": False,
            }
        ]
    return [
        {
            "status": model.get("status"),
            "family": model.get("family"),
            "specification_id": model.get("specification_id"),
            "model_id": model.get("model_id"),
            "horizon_metrics": model.get("horizon_metrics"),
            "calculation_basis": "shared_regular_grid_folds",
            "automatic_winner": False,
        }
        for model in models[:MAX_AUTOREGRESSIVE_SPECIFICATIONS]
    ]


def _build_annotations(
    frame: Any | None,
    evaluations: Sequence[Mapping[str, Any]],
    profile: AutoregressiveProfile,
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
    family_folds: dict[tuple[str, str, int, str], int] = {}
    collisions = 0
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
            _text(row.get("family")),
        )
    )
    for evaluation in selected:
        group = (
            _text(evaluation.get("series_id")),
            _text(evaluation.get("period")),
        )
        family = _text(evaluation.get("family"))
        for diagnostic, value_key, time_key in (
            (False, "forecast", "origin_bin_end_utc_ms"),
            (True, "error", "target_bin_end_utc_ms"),
        ):
            if (
                diagnostic
                and _optional_float(evaluation.get(value_key)) is None
            ):
                continue
            row_id = _first_available_row_id(
                availability.get(group, ()), _int(evaluation.get(time_key))
            )
            if row_id is None:
                continue
            key = (*group, row_id)
            family_key = (*key, family)
            annotation = _annotation_row(
                evaluation, input_result, row_id, diagnostic=diagnostic
            )
            existing = merged.setdefault(
                key,
                {"series_id": group[0], "period": group[1], "row_id": row_id},
            )
            existing_fold = family_folds.get(family_key)
            current_fold = _int(evaluation.get("fold_id"))
            if existing_fold is not None and existing_fold != current_fold:
                collisions += 1
            if diagnostic and existing_fold == current_fold:
                prefix = f"cm_{family}_"
                for suffix in (
                    "actual",
                    "error",
                    "diagnostic_available",
                    "diagnostic_available_at_utc_ms",
                ):
                    existing[prefix + suffix] = annotation.get(prefix + suffix)
            else:
                existing.update(annotation)
                family_folds[family_key] = current_fold
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
        and _optional_float(evaluation.get("forecast")) is not None
    )
    return {
        "series_id": _text(evaluation.get("series_id")),
        "period": _text(evaluation.get("period")),
        "row_id": row_id,
        prefix
        + "schema_version": AUTOREGRESSIVE_TRAINING_PROJECTION_SCHEMA_VERSION,
        prefix
        + "input_derivation_id": _text(
            input_result.contract.get("derivation_id")
        ),
        prefix + "model_id": _text(evaluation.get("model_id")),
        prefix + "family_code": _int(evaluation.get("family_code")),
        prefix
        + "specification_code": _int(evaluation.get("specification_code")),
        prefix + "p": _int(evaluation.get("p")),
        prefix + "d": _int(evaluation.get("d")),
        prefix + "q": _int(evaluation.get("q")),
        prefix + "trend_code": _int(evaluation.get("trend_code")),
        prefix + "calculation_basis_code": 1,
        prefix
        + "fit_status_code": AUTOREGRESSIVE_FIT_STATUS_CODES.get(fit_status, 1),
        prefix
        + "failure_reason_code": AUTOREGRESSIVE_REASON_CODES.get(
            _text(evaluation.get("fit_reason")), 0
        ),
        prefix + "converged": bool(evaluation.get("converged", False)),
        prefix + "stationary": evaluation.get("stationary"),
        prefix + "invertible": evaluation.get("invertible"),
        prefix
        + "ar_root_min_modulus": _optional_float(
            evaluation.get("ar_root_min_modulus")
        ),
        prefix
        + "ma_root_min_modulus": _optional_float(
            evaluation.get("ma_root_min_modulus")
        ),
        prefix
        + "covariance_condition_number": _optional_float(
            evaluation.get("covariance_condition_number")
        ),
        prefix
        + "effective_observation_count": _int(
            evaluation.get("effective_observation_count")
        ),
        prefix + "fold_id": _int(evaluation.get("fold_id")),
        prefix + "origin_row_id": evaluation.get("origin_row_id"),
        prefix + "target_row_id": evaluation.get("target_row_id"),
        prefix + "horizon": _int(evaluation.get("horizon")),
        prefix + "forecast": _optional_float(evaluation.get("forecast")),
        prefix + "forecast_available": forecast_available,
        prefix
        + "forecast_available_at_utc_ms": _int(
            evaluation.get("origin_bin_end_utc_ms")
        ),
        prefix
        + "actual": (
            _optional_float(evaluation.get("actual")) if diagnostic else None
        ),
        prefix
        + "error": (
            _optional_float(evaluation.get("error")) if diagnostic else None
        ),
        prefix + "diagnostic_available": diagnostic,
        prefix
        + "diagnostic_available_at_utc_ms": (
            _int(evaluation.get("target_bin_end_utc_ms"))
            if diagnostic
            else None
        ),
        prefix + "diagnostic_only": diagnostic,
        prefix + "original_scale": True,
        prefix + "training_eligible": forecast_available,
    }


def _training_projection_metadata(
    profile: AutoregressiveProfile,
    input_result: ClassicalModelInputResult,
    annotation_count: int,
    collision_count: int,
) -> dict[str, JSONValue]:
    return {
        "schema_version": AUTOREGRESSIVE_TRAINING_PROJECTION_SCHEMA_VERSION,
        "grain": "row",
        "identity_fields": ["series_id", "period", "row_id"],
        "timestamp_is_sole_identity": False,
        "mapping_policy": "first_source_row_at_or_after_availability",
        "collision_policy": "latest_origin_wins_per_family",
        "collision_count": collision_count,
        "annotation_count": annotation_count,
        "projection_specification_ids": list(
            profile.projection_specification_ids
        ),
        "projection_horizon": profile.projection_horizon,
        "input_derivation_id": input_result.contract.get("derivation_id"),
        "column_names": list(AUTOREGRESSIVE_COLUMNS),
        "family_column_prefixes": ["cm_ar_", "cm_arma_", "cm_arima_"],
        "forecast_time_values_use_future": False,
        "diagnostics_marked_post_observation": True,
        "observed_columns_overwritten": False,
    }


def _base_payload(
    input_result: ClassicalModelInputResult,
    fingerprint: Mapping[str, JSONValue],
    profile: AutoregressiveProfile,
) -> dict[str, JSONValue]:
    contract = input_result.contract
    regularization = _mapping(contract.get("regularization"))
    audit = _mapping(fingerprint.get("fingerprint_audit"))
    statuses = _mapping(audit.get("section_statuses"))
    stationarity = _mapping(fingerprint.get("stationarity_diagnostics"))
    return {
        "schema_version": AUTOREGRESSIVE_SCHEMA_VERSION,
        "advisory": True,
        "target_axis": dict(_mapping(contract.get("target_axis"))),
        "reference_fingerprint_id": contract.get("reference_fingerprint_id")
        or fingerprint.get("fingerprint_id"),
        "input_schema_version": CLASSICAL_MODEL_INPUT_SCHEMA_VERSION,
        "input_derivation_id": contract.get("derivation_id"),
        "input_status": contract.get("status"),
        "calculation_basis": "regular_grid_rolling_origin",
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
        },
        "prerequisite_readiness": {
            "dependence_status": statuses.get("dependence") or "unknown",
            "stationarity_status": statuses.get("stationarity_diagnostics")
            or "unknown",
            "stationarity_recommendations": stationarity.get(
                "recommended_transforms"
            )
            or [],
            "recommendations_applied_automatically": False,
            "advisory_only": True,
        },
        "model_differencing_is_separate_from_input_differencing": True,
        "missing_observation_policy": "reset_to_trailing_contiguous_segment",
        "forward_fill_policy": "never",
        "original_scale_forecasts": True,
        "automatic_order_selection": False,
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
) -> AutoregressiveResult:
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
            "schema_version": AUTOREGRESSIVE_FIT_SCHEMA_VERSION,
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
            "schema_version": AUTOREGRESSIVE_EVALUATION_SCHEMA_VERSION,
            "calculation_basis": "regular_grid_rolling_origin",
            "original_scale": True,
            "model_count": len(
                _mapping_rows(configuration.get("specifications"))
            ),
            "fold_count": len(input_result.folds),
            "evaluated_fold_count": 0,
            "skipped_evaluation_count": 0,
            "forecast_coverage_rate": None,
            "models": [],
            "reference_baselines": [],
            "reference_exponential_smoothing": [],
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
            "schema_version": AUTOREGRESSIVE_TRAINING_PROJECTION_SCHEMA_VERSION,
            "grain": "row",
            "identity_fields": ["series_id", "period", "row_id"],
            "timestamp_is_sole_identity": False,
            "mapping_policy": "first_source_row_at_or_after_availability",
            "collision_policy": "latest_origin_wins_per_family",
            "collision_count": 0,
            "annotation_count": 0,
            "projection_specification_ids": configuration.get(
                "projection_specification_ids"
            ),
            "projection_horizon": configuration.get("projection_horizon"),
            "input_derivation_id": input_result.contract.get("derivation_id"),
            "column_names": list(AUTOREGRESSIVE_COLUMNS),
            "family_column_prefixes": ["cm_ar_", "cm_arma_", "cm_arima_"],
            "forecast_time_values_use_future": False,
            "diagnostics_marked_post_observation": True,
            "observed_columns_overwritten": False,
        },
        "fit_duration_included": False,
    }
    return AutoregressiveResult(diagnostics, (), input_result)


def _load_backend() -> _Backend | None:
    try:
        statsmodels = importlib.import_module("statsmodels")
        module = importlib.import_module("statsmodels.tsa.arima.model")
        version = getattr(statsmodels, "__version__", "") or (
            importlib.metadata.version("statsmodels")
        )
        return _Backend(version=str(version), arima=module.ARIMA)
    except (
        ImportError,
        ModuleNotFoundError,
        importlib.metadata.PackageNotFoundError,
    ):
        return None


def _warning_codes(
    captured: Sequence[warnings.WarningMessage],
) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                AUTOREGRESSIVE_WARNING_CODES.get(
                    item.category.__name__, "backend_warning"
                )
                for item in captured
            }
        )
    )


def _fit_converged(fitted: Any, warning_codes: Sequence[str]) -> bool:
    if "convergence_warning" in warning_codes:
        return False
    result = getattr(fitted, "mle_retvals", None)
    if result is None:
        return True
    if isinstance(result, Mapping):
        value = result.get("converged", result.get("success"))
    else:
        value = getattr(result, "success", getattr(result, "converged", None))
    return True if value is None else bool(value)


def _root_status(raw: Any) -> tuple[bool | None, float | None]:
    try:
        moduli = [abs(complex(value)) for value in raw]
    except (TypeError, ValueError):
        return None, None
    if not moduli:
        return None, None
    minimum = min(moduli)
    return all(value > 1.0 for value in moduli), float(minimum)


def _fitted_parameters(fitted: Any) -> dict[str, float]:
    names = [str(name) for name in getattr(fitted, "param_names", ())]
    raw = getattr(fitted, "params", ())
    values: dict[str, float] = {}
    for name, value in zip(names, raw, strict=False):
        scalar = _optional_float(value)
        if scalar is not None:
            values[name.removesuffix(" (fixed)")] = scalar
    return dict(sorted(values.items()))


def _covariance_condition_number(fitted: Any) -> float | None:
    try:
        numpy = importlib.import_module("numpy")
        value = float(numpy.linalg.cond(fitted.cov_params()))
        return value if math.isfinite(value) else None
    except Exception:
        return None


def _backend_failure_reason(exc: Exception) -> str:
    message = str(exc).lower()
    if "not stationary" in message or "non-stationary" in message:
        return "nonstationary_configuration"
    if "not invertible" in message or "non-invertible" in message:
        return "noninvertible_configuration"
    if "singular" in message or "linalg" in message:
        return "singularity"
    if "order" in message or "lag" in message:
        return "invalid_order"
    if "trend" in message or "parameter" in message or "initial" in message:
        return "invalid_configuration"
    if "overflow" in message or "finite" in message:
        return "numerical_overflow"
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


def _estimated_working_memory_bytes(
    row_count: int, fold_count: int, specification_count: int
) -> int:
    return (
        row_count * 24 * 8
        + fold_count * 96 * 8
        + specification_count * 1024 * 8
    )


def _model_id(
    specification: AutoregressiveSpecification,
    input_derivation_id: str,
    backend_version: str,
) -> str:
    payload = {
        "schema_version": AUTOREGRESSIVE_CONFIGURATION_SCHEMA_VERSION,
        "input_derivation_id": input_derivation_id,
        "backend": "statsmodels",
        "backend_version": backend_version,
        "specification": specification.to_metadata(),
    }
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _first_available_row_id(
    values: Sequence[tuple[int, int]], threshold: int
) -> int | None:
    return next(
        (row_id for timestamp, row_id in values if timestamp >= threshold), None
    )


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

    strings = {"schema_version", "input_derivation_id", "model_id"}
    booleans = {
        "converged",
        "stationary",
        "invertible",
        "forecast_available",
        "diagnostic_available",
        "diagnostic_only",
        "original_scale",
        "training_eligible",
    }
    floats = {
        "forecast",
        "actual",
        "error",
        "ar_root_min_modulus",
        "ma_root_min_modulus",
        "covariance_condition_number",
    }
    return {
        f"cm_{family}_{suffix}": (
            pl.Utf8
            if suffix in strings
            else (
                pl.Boolean
                if suffix in booleans
                else pl.Float64 if suffix in floats else pl.Int64
            )
        )
        for family in AUTOREGRESSIVE_FAMILIES
        for suffix in AUTOREGRESSIVE_FAMILY_COLUMN_SUFFIXES
    }


def _rate(numerator: int, denominator: int, digits: int) -> float | None:
    return _rounded(numerator / denominator if denominator else None, digits)


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
        return int(cast(Any, value))
    except (TypeError, ValueError, OverflowError):
        return 0


def _optional_int(value: object) -> int | None:
    try:
        return int(cast(Any, value)) if value is not None else None
    except (TypeError, ValueError, OverflowError):
        return None


def _float(value: object) -> float:
    return float(cast(Any, value))


def _optional_float(value: object) -> float | None:
    try:
        candidate = float(cast(Any, value))
    except (TypeError, ValueError, OverflowError):
        return None
    return candidate if math.isfinite(candidate) else None


def _rounded(value: float | None, digits: int) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(value, digits)
