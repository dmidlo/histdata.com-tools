"""Explicit SARIMA, ARIMAX, and SARIMAX fingerprint diagnostics.

The family consumes the #421 regular-grid and rolling-origin contracts.  All
exogenous values are deterministic calendar features known independently of
future market observations, and Statsmodels is imported lazily.
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
    _root_status,
    _rounded,
    _target_sort_key,
    _text,
    _warning_codes,
)
from histdatacom.data_quality.calendar import (
    SESSION_ASIA,
    SESSION_LONDON,
    SESSION_NEW_YORK,
    SESSION_STATE_FRIDAY_CLOSE,
    SESSION_STATE_MARKET_OPEN,
    SESSION_STATE_SUNDAY_OPEN,
    SESSION_STATE_WEEKEND_CLOSURE,
    classify_histdata_timestamp,
)
from histdatacom.data_quality.calendar_profiles import (
    HistDataCalendarProfile,
    default_calendar_profile,
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
    _trailing_contiguous_indexes,
)
from histdatacom.data_quality.limits import bounded_report_limit
from histdatacom.data_quality.training_features import (
    SEASONAL_EXOGENOUS_COLUMNS,
    SEASONAL_EXOGENOUS_FAMILY_COLUMN_SUFFIXES,
    ensure_tick_training_features,
)
from histdatacom.runtime_contracts import JSONValue

SEASONAL_EXOGENOUS_SCHEMA_VERSION = "histdatacom.seasonal-exogenous.v1"
SEASONAL_EXOGENOUS_CONFIGURATION_SCHEMA_VERSION = (
    "histdatacom.seasonal-exogenous-configuration.v1"
)
SEASONAL_EXOGENOUS_REGRESSOR_SCHEMA_VERSION = (
    "histdatacom.seasonal-exogenous-regressors.v1"
)
SEASONAL_EXOGENOUS_FIT_SCHEMA_VERSION = (
    "histdatacom.seasonal-exogenous-fit-result.v1"
)
SEASONAL_EXOGENOUS_FORECAST_SCHEMA_VERSION = (
    "histdatacom.seasonal-exogenous-forecast.v1"
)
SEASONAL_EXOGENOUS_EVALUATION_SCHEMA_VERSION = (
    "histdatacom.seasonal-exogenous-evaluation.v1"
)
SEASONAL_EXOGENOUS_TRAINING_PROJECTION_SCHEMA_VERSION = (
    "histdatacom.seasonal-exogenous-training-projection.v1"
)
SEASONAL_EXOGENOUS_SUMMARY_SCHEMA_VERSION = (
    "histdatacom.seasonal-exogenous-summary.v1"
)
SEASONAL_EXOGENOUS_SUMMARY_METADATA_KEY = (
    "time_series_fingerprint_seasonal_exogenous_summary"
)
SEASONAL_EXOGENOUS_BOUNDED_PAYLOAD_KEY = "fingerprint_seasonal_exogenous"

DEFAULT_SEASONAL_EXOGENOUS_SUMMARY_TARGET_LIMIT = 16
DEFAULT_SEASONAL_EXOGENOUS_ROLLING_WINDOWS = (5, 20)
DEFAULT_SEASONAL_EXOGENOUS_ROUNDING_DIGITS = 12
MAX_SEASONAL_EXOGENOUS_SPECIFICATIONS = 32
MAX_SEASONAL_EXOGENOUS_ORDER = 64
MAX_SEASONAL_PERIOD = 100_000
MAX_SEASONAL_EXOGENOUS_REGRESSORS = 32
MAX_SEASONAL_EXOGENOUS_FIXED_PARAMETERS = 32

SEASONAL_EXOGENOUS_FAMILIES = ("sarima", "arimax", "sarimax")
SEASONAL_EXOGENOUS_FAMILY_CODES = {
    "sarima": 1,
    "arimax": 2,
    "sarimax": 3,
}
SEASONAL_EXOGENOUS_TREND_CODES = {"n": 0, "c": 1, "t": 2, "ct": 3}
SEASONAL_EXOGENOUS_INITIALIZATION_CODES = {
    "default": 1,
    "stationary": 2,
    "approximate_diffuse": 3,
}
SEASONAL_EXOGENOUS_FIT_STATUS_CODES = {
    "unavailable": 1,
    "skipped": 2,
    "failed": 3,
    "limited": 4,
    "fitted": 5,
    "converged": 6,
}
SEASONAL_EXOGENOUS_REASON_CODES = {
    "": 0,
    "dependency_unavailable": 1,
    "input_contract_unavailable": 2,
    "insufficient_folds": 3,
    "insufficient_history": 4,
    "invalid_order": 5,
    "invalid_configuration": 6,
    "invalid_seasonality": 7,
    "unknown_regressor": 8,
    "future_regressor_unavailable": 9,
    "partial_calendar_unavailable": 10,
    "rank_deficient_regressors": 11,
    "collinearity": 12,
    "optimizer_failure": 13,
    "singularity": 14,
    "numerical_overflow": 15,
    "numerical_failure": 16,
    "resource_limit": 17,
    "timeout": 18,
    "backend_failure": 19,
    "target_unavailable": 20,
    "inverse_transform_unavailable": 21,
    "zero_variance": 22,
}

CALENDAR_REGRESSOR_NAMES = (
    "source_hour_sin",
    "source_hour_cos",
    "source_weekday_sin",
    "source_weekday_cos",
    "market_open",
    "weekend_closure",
    "sunday_open",
    "friday_close",
    "session_asia",
    "session_london",
    "session_new_york",
    "overlap_asia_london",
    "overlap_london_new_york",
    "daily_rollover",
    "london_fix",
    "month_end",
    "quarter_end",
    "year_end",
    "holiday_any",
    "event_any",
)
PARTIAL_CALENDAR_REGRESSORS = {"holiday_any", "event_any"}

_SPECIFICATION_ID = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,63}$")
_PARAMETER_NAME = re.compile(r"^[A-Za-z0-9_.()\[\]-]{1,96}$")
_TAG_REGRESSOR = re.compile(r"^tag:[a-z0-9][a-z0-9_.:-]{0,127}$")
_OPTIMIZERS = {"lbfgs", "bfgs", "powell", "nm", "cg", "ncg"}


@dataclass(frozen=True, slots=True)
class CalendarRegressorProfile:
    """Bounded forecast-safe calendar-regressor controls."""

    allow_partial_calendar: bool = True
    require_complete_calendar_for: tuple[str, ...] = ()
    max_regressors: int = 16

    def __post_init__(self) -> None:
        if self.max_regressors < 1 or self.max_regressors > 64:
            raise ValueError("max_regressors must be between 1 and 64")
        unknown = set(self.require_complete_calendar_for) - (
            PARTIAL_CALENDAR_REGRESSORS
        )
        if unknown:
            raise ValueError("complete-calendar controls support holiday/event")

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return stable JSON-compatible controls."""
        return {
            "allow_partial_calendar": self.allow_partial_calendar,
            "require_complete_calendar_for": list(
                self.require_complete_calendar_for
            ),
            "max_regressors": self.max_regressors,
            "market_observation_regressors_allowed": False,
            "future_derived_regressors_allowed": False,
        }


@dataclass(frozen=True, slots=True)
class SeasonalExogenousSpecification:
    """One explicit SARIMA, ARIMAX, or SARIMAX configuration."""

    specification_id: str
    family: str
    p: int
    d: int = 0
    q: int = 0
    seasonal_p: int = 0
    seasonal_d: int = 0
    seasonal_q: int = 0
    seasonal_period: int = 0
    seasonal_cycle_ms: int = 0
    trend: str = "n"
    initialization_method: str = "default"
    optimizer: str = "lbfgs"
    enforce_stationarity: bool = True
    enforce_invertibility: bool = True
    concentrate_scale: bool = False
    use_exact_diffuse: bool = False
    regressor_names: tuple[str, ...] = ()
    fixed_parameters: tuple[tuple[str, float], ...] = ()
    max_iterations: int = 200

    def __post_init__(self) -> None:
        if not _SPECIFICATION_ID.fullmatch(self.specification_id):
            raise ValueError("invalid seasonal/exogenous specification_id")
        if self.family not in SEASONAL_EXOGENOUS_FAMILIES:
            raise ValueError("unsupported seasonal/exogenous family")
        orders = {
            "p": self.p,
            "d": self.d,
            "q": self.q,
            "seasonal_p": self.seasonal_p,
            "seasonal_d": self.seasonal_d,
            "seasonal_q": self.seasonal_q,
        }
        for name, value in orders.items():
            if value < 0 or value > MAX_SEASONAL_EXOGENOUS_ORDER:
                raise ValueError(f"{name} must be between 0 and 64")
        has_seasonal = any((self.seasonal_p, self.seasonal_d, self.seasonal_q))
        has_exog = bool(self.regressor_names)
        if self.family == "sarima" and (not has_seasonal or has_exog):
            raise ValueError("SARIMA requires seasonality and no regressors")
        if self.family == "arimax" and (has_seasonal or not has_exog):
            raise ValueError("ARIMAX requires regressors and no seasonality")
        if self.family == "sarimax" and (not has_seasonal or not has_exog):
            raise ValueError("SARIMAX requires seasonality and regressors")
        if has_seasonal:
            if not 2 <= self.seasonal_period <= MAX_SEASONAL_PERIOD:
                raise ValueError("seasonal_period must be between 2 and 100000")
            if self.seasonal_cycle_ms < 1:
                raise ValueError("seasonal_cycle_ms must be explicit")
        elif self.seasonal_period or self.seasonal_cycle_ms:
            raise ValueError("nonseasonal ARIMAX cannot set a seasonal cycle")
        if self.trend not in SEASONAL_EXOGENOUS_TREND_CODES:
            raise ValueError("trend must be n, c, t, or ct")
        if self.d + self.seasonal_d > 0 and "c" in self.trend:
            raise ValueError("integrated seasonal models cannot use constant")
        if self.initialization_method not in (
            SEASONAL_EXOGENOUS_INITIALIZATION_CODES
        ):
            raise ValueError("unsupported initialization_method")
        if (
            self.initialization_method == "stationary"
            and self.d + self.seasonal_d > 0
        ):
            raise ValueError(
                "stationary initialization requires no differencing"
            )
        if self.optimizer not in _OPTIMIZERS:
            raise ValueError("unsupported seasonal/exogenous optimizer")
        if not self.regressor_names and self.family in {"arimax", "sarimax"}:
            raise ValueError("exogenous family requires regressors")
        if len(self.regressor_names) > MAX_SEASONAL_EXOGENOUS_REGRESSORS:
            raise ValueError("too many seasonal/exogenous regressors")
        if len(set(self.regressor_names)) != len(self.regressor_names):
            raise ValueError("regressor names must be unique")
        for name in self.regressor_names:
            if name not in CALENDAR_REGRESSOR_NAMES and not (
                _TAG_REGRESSOR.fullmatch(name)
            ):
                raise ValueError("unknown calendar regressor")
        if len(self.fixed_parameters) > (
            MAX_SEASONAL_EXOGENOUS_FIXED_PARAMETERS
        ):
            raise ValueError("too many fixed seasonal/exogenous parameters")
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
        """Return a stable serializable specification."""
        return {
            "schema_version": SEASONAL_EXOGENOUS_CONFIGURATION_SCHEMA_VERSION,
            "specification_id": self.specification_id,
            "family": self.family,
            "family_code": SEASONAL_EXOGENOUS_FAMILY_CODES[self.family],
            "order": [self.p, self.d, self.q],
            "seasonal_order": [
                self.seasonal_p,
                self.seasonal_d,
                self.seasonal_q,
                self.seasonal_period,
            ],
            "seasonal_cycle_ms": self.seasonal_cycle_ms,
            "trend": self.trend,
            "trend_code": SEASONAL_EXOGENOUS_TREND_CODES[self.trend],
            "initialization_method": self.initialization_method,
            "optimizer": self.optimizer,
            "enforce_stationarity": self.enforce_stationarity,
            "enforce_invertibility": self.enforce_invertibility,
            "concentrate_scale": self.concentrate_scale,
            "use_exact_diffuse": self.use_exact_diffuse,
            "regressor_names": list(self.regressor_names),
            "fixed_parameters": cast(
                JSONValue,
                [
                    {"parameter": name, "value": value}
                    for name, value in self.fixed_parameters
                ],
            ),
            "max_iterations": self.max_iterations,
            "automatic_order_selection": False,
            "automatic_regressor_selection": False,
        }


def _default_specifications() -> tuple[SeasonalExogenousSpecification, ...]:
    return (
        SeasonalExogenousSpecification(
            "sarima-1-0-0x1-0-0-60",
            "sarima",
            1,
            seasonal_p=1,
            seasonal_period=60,
            seasonal_cycle_ms=3_600_000,
            trend="c",
        ),
        SeasonalExogenousSpecification(
            "arimax-1-hour",
            "arimax",
            1,
            trend="c",
            regressor_names=("source_hour_sin", "source_hour_cos"),
        ),
        SeasonalExogenousSpecification(
            "sarimax-1-hour",
            "sarimax",
            1,
            seasonal_p=1,
            seasonal_period=60,
            seasonal_cycle_ms=3_600_000,
            trend="c",
            regressor_names=("source_hour_sin", "source_hour_cos"),
        ),
    )


@dataclass(frozen=True, slots=True)
class SeasonalExogenousProfile:
    """Explicit seasonal/exogenous controls; disabled by default."""

    enabled: bool = False
    specifications: tuple[SeasonalExogenousSpecification, ...] = field(
        default_factory=_default_specifications
    )
    projection_specification_ids: tuple[str, ...] = (
        "sarima-1-0-0x1-0-0-60",
        "arimax-1-hour",
        "sarimax-1-hour",
    )
    projection_horizon: int = 1
    regressor_profile: CalendarRegressorProfile = field(
        default_factory=CalendarRegressorProfile
    )
    baseline_rolling_windows: tuple[int, ...] = (
        DEFAULT_SEASONAL_EXOGENOUS_ROLLING_WINDOWS
    )
    compare_exponential_smoothing: bool = True
    compare_autoregressive: bool = True
    rounding_digits: int = DEFAULT_SEASONAL_EXOGENOUS_ROUNDING_DIGITS

    def __post_init__(self) -> None:
        if not self.specifications:
            raise ValueError(
                "at least one seasonal/exogenous model is required"
            )
        if len(self.specifications) > MAX_SEASONAL_EXOGENOUS_SPECIFICATIONS:
            raise ValueError("too many seasonal/exogenous specifications")
        identifiers = tuple(
            item.specification_id for item in self.specifications
        )
        if len(set(identifiers)) != len(identifiers):
            raise ValueError(
                "seasonal/exogenous specification IDs must be unique"
            )
        selected = set(self.projection_specification_ids)
        if not selected or not selected.issubset(identifiers):
            raise ValueError("projection IDs must select configured models")
        families = {
            item.family
            for item in self.specifications
            if item.specification_id in selected
        }
        if len(families) != len(selected):
            raise ValueError("select at most one projection per model family")
        if self.projection_horizon < 1:
            raise ValueError("projection_horizon must be positive")
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
            "projection_specification_ids": list(
                self.projection_specification_ids
            ),
            "projection_horizon": self.projection_horizon,
            "regressor_profile": self.regressor_profile.to_metadata(),
            "baseline_rolling_windows": list(self.baseline_rolling_windows),
            "compare_exponential_smoothing": self.compare_exponential_smoothing,
            "compare_autoregressive": self.compare_autoregressive,
            "rounding_digits": self.rounding_digits,
            "automatic_order_selection": False,
            "automatic_regressor_selection": False,
            "automatic_winner": False,
        }


@dataclass(frozen=True, slots=True)
class SeasonalExogenousResult:
    """Bounded diagnostics plus durable row-key annotations."""

    diagnostics: Mapping[str, JSONValue]
    annotations: tuple[Mapping[str, Any], ...]
    input_result: ClassicalModelInputResult


@dataclass(frozen=True, slots=True)
class _Backend:
    version: str
    sarimax: Any


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
    residual_summary: Mapping[str, JSONValue]


@dataclass(frozen=True, slots=True)
class _RegressorRow:
    values: Mapping[str, float]
    available: bool
    availability: str
    reason: str
    active_sessions: tuple[str, ...]
    special_tags: tuple[str, ...]
    holiday_tags: tuple[str, ...]
    event_tags: tuple[str, ...]


def seasonal_exogenous_from_training_frame(
    frame: Any | None,
    fingerprint: Mapping[str, JSONValue],
    *,
    input_profile: ClassicalModelInputProfile | None = None,
    profile: SeasonalExogenousProfile | None = None,
    calendar_profile: HistDataCalendarProfile | None = None,
    exponential_smoothing: Mapping[str, JSONValue] | None = None,
    autoregressive: Mapping[str, JSONValue] | None = None,
    target: Any | None = None,
) -> SeasonalExogenousResult:
    """Regularize an enriched tick frame and evaluate configured models."""
    selected_input = input_profile or ClassicalModelInputProfile(enabled=True)
    input_result = build_classical_model_input(
        frame, fingerprint, profile=selected_input, target=target
    )
    return seasonal_exogenous_from_model_input(
        frame,
        input_result,
        fingerprint,
        input_profile=selected_input,
        profile=profile,
        calendar_profile=calendar_profile,
        exponential_smoothing=exponential_smoothing,
        autoregressive=autoregressive,
        target=target,
    )


def seasonal_exogenous_from_model_input(
    frame: Any | None,
    input_result: ClassicalModelInputResult,
    fingerprint: Mapping[str, JSONValue],
    *,
    input_profile: ClassicalModelInputProfile,
    profile: SeasonalExogenousProfile | None = None,
    calendar_profile: HistDataCalendarProfile | None = None,
    exponential_smoothing: Mapping[str, JSONValue] | None = None,
    autoregressive: Mapping[str, JSONValue] | None = None,
    target: Any | None = None,
) -> SeasonalExogenousResult:
    """Fit explicit SARIMA, ARIMAX, and SARIMAX specifications."""
    selected = profile or SeasonalExogenousProfile(enabled=True)
    selected_calendar = calendar_profile or default_calendar_profile()
    rows = cast(list[dict[str, Any]], input_result.regularized_frame.to_dicts())
    regressors, regressor_contract = _calendar_regressors(
        rows, selected, selected_calendar, target=target
    )
    base = _base_payload(
        input_result,
        fingerprint,
        selected,
        regressor_contract,
        selected_calendar,
    )
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
        selected_calendar,
        regressors,
        regressor_contract,
        backend,
        exponential_smoothing=exponential_smoothing,
        autoregressive=autoregressive,
        target=target,
    )


def seasonal_exogenous_diagnostics_from_training_frame(
    frame: Any | None,
    fingerprint: Mapping[str, JSONValue],
    *,
    input_profile: ClassicalModelInputProfile | None = None,
    profile: SeasonalExogenousProfile | None = None,
    calendar_profile: HistDataCalendarProfile | None = None,
    target: Any | None = None,
) -> dict[str, JSONValue]:
    """Return diagnostics without exposing the annotation wrapper."""
    return dict(
        seasonal_exogenous_from_training_frame(
            frame,
            fingerprint,
            input_profile=input_profile,
            profile=profile,
            calendar_profile=calendar_profile,
            target=target,
        ).diagnostics
    )


def project_seasonal_exogenous_onto_training_frame(
    frame: Any,
    result: SeasonalExogenousResult,
    *,
    target: Any | None = None,
) -> Any:
    """Join bounded seasonal/exogenous annotations by durable row identity."""
    import polars as pl

    columns = set(getattr(frame, "columns", ()))
    if not {"series_id", "period", "row_id"}.issubset(columns):
        enriched = ensure_tick_training_features(frame, target=target)
    else:
        enriched = frame
    left = enriched.drop(
        [
            name
            for name in SEASONAL_EXOGENOUS_COLUMNS
            if name in enriched.columns
        ]
    ).with_row_index("__cm_seasonal_exogenous_original_order")
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
    return projected.sort("__cm_seasonal_exogenous_original_order").drop(
        "__cm_seasonal_exogenous_original_order"
    )


def seasonal_exogenous_summary(
    findings: Iterable[QualityFinding],
    *,
    target_limit: int | None = DEFAULT_SEASONAL_EXOGENOUS_SUMMARY_TARGET_LIMIT,
) -> dict[str, JSONValue] | None:
    """Return bounded report metadata for seasonal/exogenous results."""
    targets: list[dict[str, JSONValue]] = []
    statuses: Counter[str] = Counter()
    for finding in findings:
        fingerprint = _mapping(finding.metadata.get("time_series_fingerprint"))
        payload = _mapping(fingerprint.get("seasonal_exogenous"))
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
        default_limit=DEFAULT_SEASONAL_EXOGENOUS_SUMMARY_TARGET_LIMIT,
        allow_unbounded=True,
    )
    included = limit.slice(targets)
    omitted = len(targets) - len(included)
    return {
        "schema_version": SEASONAL_EXOGENOUS_SUMMARY_SCHEMA_VERSION,
        "advisory": True,
        "target_count": len(targets),
        "included_target_count": len(included),
        "omitted_target_count": omitted,
        "truncated": omitted > 0,
        "status_counts": dict(sorted(statuses.items())),
        "target_summaries": cast(JSONValue, included),
        "limit_metadata": {"targets": limit.limit_payload()},
    }


def format_seasonal_exogenous_summary_lines(
    summary: Mapping[str, JSONValue] | None,
) -> tuple[str, ...]:
    """Return concise human-readable seasonal/exogenous lines."""
    if not summary:
        return ()
    statuses = _mapping(summary.get("status_counts"))
    lines = [
        "",
        "Seasonal and exogenous models",
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


def _calendar_regressors(
    rows: Sequence[Mapping[str, Any]],
    profile: SeasonalExogenousProfile,
    calendar_profile: HistDataCalendarProfile,
    *,
    target: Any | None,
) -> tuple[tuple[_RegressorRow, ...], dict[str, JSONValue]]:
    requested = tuple(
        dict.fromkeys(
            name
            for specification in profile.specifications
            for name in specification.regressor_names
        )
    )
    tag_vocabulary = {item.tag for item in calendar_profile.date_tags} | {
        item.tag for item in calendar_profile.window_tags
    }
    unknown = sorted(
        name
        for name in requested
        if name.startswith("tag:") and name[4:] not in tag_vocabulary
    )
    result: list[_RegressorRow] = []
    availability_counts: Counter[str] = Counter()
    for row in rows:
        timestamp = _int(row.get("cm_input_bin_start_utc_ms"))
        classification = classify_histdata_timestamp(
            timestamp,
            calendar_profile=calendar_profile,
            asset_class=(
                _text(getattr(target, "metadata", {}).get("asset_class"))
                if target is not None
                else ""
            ),
        )
        source = classification.source_datetime
        hour = source.hour + source.minute / 60.0
        weekday = float(source.weekday())
        values: dict[str, float] = {
            "source_hour_sin": math.sin(2.0 * math.pi * hour / 24.0),
            "source_hour_cos": math.cos(2.0 * math.pi * hour / 24.0),
            "source_weekday_sin": math.sin(2.0 * math.pi * weekday / 7.0),
            "source_weekday_cos": math.cos(2.0 * math.pi * weekday / 7.0),
            "market_open": float(
                classification.session_state == SESSION_STATE_MARKET_OPEN
            ),
            "weekend_closure": float(
                classification.session_state == SESSION_STATE_WEEKEND_CLOSURE
            ),
            "sunday_open": float(
                classification.session_state == SESSION_STATE_SUNDAY_OPEN
            ),
            "friday_close": float(
                classification.session_state == SESSION_STATE_FRIDAY_CLOSE
            ),
            "session_asia": float(
                SESSION_ASIA in classification.active_sessions
            ),
            "session_london": float(
                SESSION_LONDON in classification.active_sessions
            ),
            "session_new_york": float(
                SESSION_NEW_YORK in classification.active_sessions
            ),
            "overlap_asia_london": float(
                "asia_london_overlap" in classification.overlaps
            ),
            "overlap_london_new_york": float(
                "london_new_york_overlap" in classification.overlaps
            ),
            "daily_rollover": float(
                "daily_rollover" in classification.special_tags
            ),
            "london_fix": float(
                "london_4pm_fix_window" in classification.special_tags
            ),
            "month_end": float("month_end" in classification.special_tags),
            "quarter_end": float("quarter_end" in classification.special_tags),
            "year_end": float("year_end" in classification.special_tags),
            "holiday_any": float(bool(classification.holiday_tags)),
            "event_any": float(bool(classification.event_tags)),
        }
        for tag in tag_vocabulary:
            values[f"tag:{tag}"] = float(tag in classification.calendar_tags)
        reason = ""
        availability = "complete"
        if unknown:
            availability, reason = "unavailable", "unknown_regressor"
        elif (
            any(name in PARTIAL_CALENDAR_REGRESSORS for name in requested)
            and not calendar_profile.complete
        ):
            availability = "partial"
            required = set(
                profile.regressor_profile.require_complete_calendar_for
            )
            selected_partial = set(requested) & PARTIAL_CALENDAR_REGRESSORS
            if required & selected_partial:
                reason = "partial_calendar_unavailable"
            elif not profile.regressor_profile.allow_partial_calendar:
                reason = "partial_calendar_unavailable"
        available = not reason
        availability_counts[availability] += 1
        result.append(
            _RegressorRow(
                values=values,
                available=available,
                availability=availability,
                reason=reason,
                active_sessions=classification.active_sessions,
                special_tags=classification.special_tags,
                holiday_tags=classification.holiday_tags,
                event_tags=classification.event_tags,
            )
        )
    definitions = [
        {
            "name": name,
            "known_in_advance": True,
            "market_observation": False,
            "calculation_basis": (
                "configured_calendar_tag"
                if name.startswith("tag:")
                else "deterministic_calendar_classifier"
            ),
        }
        for name in requested
    ]
    contract: dict[str, JSONValue] = {
        "schema_version": SEASONAL_EXOGENOUS_REGRESSOR_SCHEMA_VERSION,
        "status": "unavailable" if unknown else "ready",
        "column_order": list(requested),
        "regressor_count": len(requested),
        "definitions": cast(JSONValue, definitions),
        "supported_builtin_vocabulary": list(CALENDAR_REGRESSOR_NAMES),
        "configured_tag_vocabulary": sorted(tag_vocabulary),
        "unknown_regressors": unknown,
        "availability_counts": dict(sorted(availability_counts.items())),
        "calendar_profile": calendar_profile.to_metadata(),
        "calendar_profile_complete": calendar_profile.complete,
        "future_values_derived_without_market_observations": True,
        "future_observed_market_values_allowed": False,
        "full_series_smoothed_state_allowed": False,
        "provenance": {
            "classifier": "histdatacom.data_quality.calendar",
            "timestamp_basis": "regular_grid_bin_start_utc_ms",
            "profile_source": calendar_profile.source,
            "profile_version": calendar_profile.version,
        },
    }
    return tuple(result), contract


def _evaluate_models(
    frame: Any | None,
    input_result: ClassicalModelInputResult,
    fingerprint: Mapping[str, JSONValue],
    input_profile: ClassicalModelInputProfile,
    profile: SeasonalExogenousProfile,
    calendar_profile: HistDataCalendarProfile,
    regressors: Sequence[_RegressorRow],
    regressor_contract: Mapping[str, JSONValue],
    backend: _Backend,
    *,
    exponential_smoothing: Mapping[str, JSONValue] | None,
    autoregressive: Mapping[str, JSONValue] | None,
    target: Any | None,
) -> SeasonalExogenousResult:
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
            specification, input_profile, profile, regressor_contract
        )
        model_id = _model_id(
            specification,
            _text(input_result.contract.get("derivation_id")),
            backend.version,
            calendar_profile,
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
            future_indexes = tuple(
                index
                for index in range(
                    training_end + 1, training_end + max_horizon + 1
                )
                if index < len(rows)
            )
            train_exog, exog_reason = _matrix_for_indexes(
                regressors, indexes, specification.regressor_names
            )
            future_exog, future_reason = _matrix_for_indexes(
                regressors, future_indexes, specification.regressor_names
            )
            reason = configuration_reason or exog_reason or future_reason
            if (
                len(future_indexes) < max_horizon
                and specification.regressor_names
            ):
                reason = reason or "future_regressor_unavailable"
            if reason:
                outcome = _empty_fit(
                    "skipped",
                    reason,
                    _effective_count(specification, len(values)),
                )
            else:
                outcome = _fit_specification(
                    specification,
                    values,
                    train_exog,
                    future_exog,
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
                    regressors,
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
    final_reason = (
        limitations[0]
        if limitations
        else sorted(fit_reasons)[0] if fit_reasons else None
    )
    diagnostics: dict[str, JSONValue] = {
        **_base_payload(
            input_result,
            fingerprint,
            profile,
            regressor_contract,
            calendar_profile,
        ),
        "status": status,
        "reason": final_reason,
        "limitations": cast(JSONValue, limitations),
        "backend": {
            "provider": "statsmodels",
            "version": backend.version,
            "available": True,
            "model_class": "statsmodels.tsa.statespace.SARIMAX",
            "import_basis": "optional_models_extra",
        },
        "fit_summary": {
            "schema_version": SEASONAL_EXOGENOUS_FIT_SCHEMA_VERSION,
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
            "schema_version": SEASONAL_EXOGENOUS_EVALUATION_SCHEMA_VERSION,
            "calculation_basis": "regular_grid_rolling_origin_calendar_exog",
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
            "reference_models": cast(JSONValue, references),
            "regime_error_summary": _regime_error_summary(
                all_evaluations, profile.rounding_digits
            ),
            "comparison_semantics": "descriptive_shared_folds_only",
            "automatic_winner": False,
        },
        "training_projection": _training_projection_metadata(
            profile, input_result, len(annotations), collisions
        ),
        "fit_duration_included": False,
    }
    return SeasonalExogenousResult(diagnostics, annotations, input_result)


def _configuration_reason(
    specification: SeasonalExogenousSpecification,
    input_profile: ClassicalModelInputProfile,
    profile: SeasonalExogenousProfile,
    regressor_contract: Mapping[str, JSONValue],
) -> str:
    if specification.seasonal_period:
        expected = specification.seasonal_period * input_profile.frequency_ms
        if expected != specification.seasonal_cycle_ms:
            return "invalid_seasonality"
    if (
        len(specification.regressor_names)
        > profile.regressor_profile.max_regressors
    ):
        return "resource_limit"
    unknown = set(
        cast(list[str], regressor_contract.get("unknown_regressors", []))
    )
    if unknown & set(specification.regressor_names):
        return "unknown_regressor"
    return ""


def _matrix_for_indexes(
    rows: Sequence[_RegressorRow],
    indexes: Sequence[int],
    names: Sequence[str],
) -> tuple[tuple[tuple[float, ...], ...] | None, str]:
    if not names:
        return None, ""
    matrix: list[tuple[float, ...]] = []
    for index in indexes:
        if index < 0 or index >= len(rows):
            return None, "future_regressor_unavailable"
        row = rows[index]
        if not row.available:
            return None, row.reason or "future_regressor_unavailable"
        try:
            matrix.append(tuple(float(row.values[name]) for name in names))
        except KeyError:
            return None, "unknown_regressor"
    return tuple(matrix), ""


def _fit_specification(
    specification: SeasonalExogenousSpecification,
    values: Sequence[float],
    train_exog: Sequence[Sequence[float]] | None,
    future_exog: Sequence[Sequence[float]] | None,
    horizon: int,
    backend: _Backend,
) -> _FitOutcome:
    effective_count = _effective_count(specification, len(values))
    minimum = max(
        8,
        specification.p
        + specification.q
        + specification.d
        + (
            specification.seasonal_p
            + specification.seasonal_q
            + specification.seasonal_d
        )
        * max(1, specification.seasonal_period)
        + 3,
    )
    if len(values) < minimum or effective_count < 3:
        return _empty_fit("skipped", "insufficient_history", effective_count)
    if max(values) - min(values) <= 1e-15:
        return _empty_fit("skipped", "zero_variance", effective_count)
    if train_exog is not None:
        rank_reason = _regressor_rank_reason(train_exog)
        if rank_reason:
            return _empty_fit("skipped", rank_reason, effective_count)
    try:
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always")
            fitted = _statsmodels_fit(
                specification, values, train_exog, backend
            )
            raw_forecasts = fitted.forecast(steps=horizon, exog=future_exog)
            stationary, ar_min = _root_status(getattr(fitted, "arroots", ()))
            invertible, ma_min = _root_status(getattr(fitted, "maroots", ()))
            condition = _covariance_condition_number(fitted)
            residual_summary = _residual_summary(
                getattr(fitted, "resid", ()), 12
            )
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
        return _FitOutcome(
            status=status,
            reason="optimizer_failure" if status == "limited" else "",
            forecasts=forecasts,
            parameters=_fitted_parameters(fitted),
            warning_codes=warning_codes,
            converged=converged,
            stationary=stationary,
            invertible=invertible,
            ar_root_min_modulus=ar_min,
            ma_root_min_modulus=ma_min,
            covariance_condition_number=condition,
            effective_observation_count=effective_count,
            residual_summary=residual_summary,
        )
    except (ArithmeticError, FloatingPointError, OverflowError):
        return _empty_fit("failed", "numerical_overflow", effective_count)
    except Exception as exc:
        return _empty_fit(
            "failed", _backend_failure_reason(exc), effective_count
        )


def _statsmodels_fit(
    specification: SeasonalExogenousSpecification,
    values: Sequence[float],
    exog: Sequence[Sequence[float]] | None,
    backend: _Backend,
) -> Any:
    model = backend.sarimax(
        values,
        exog=exog,
        order=(specification.p, specification.d, specification.q),
        seasonal_order=(
            specification.seasonal_p,
            specification.seasonal_d,
            specification.seasonal_q,
            specification.seasonal_period,
        ),
        trend=specification.trend,
        enforce_stationarity=specification.enforce_stationarity,
        enforce_invertibility=specification.enforce_invertibility,
        concentrate_scale=specification.concentrate_scale,
        use_exact_diffuse=specification.use_exact_diffuse,
        simple_differencing=False,
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
        raise ValueError("fixed parameter is not present in model")
    context = model.fix_params(fixed) if fixed else nullcontext()
    with context:
        return model.fit(
            method=specification.optimizer,
            maxiter=specification.max_iterations,
            disp=False,
            low_memory=False,
        )


def _regressor_rank_reason(matrix: Sequence[Sequence[float]]) -> str:
    if not matrix or not matrix[0]:
        return ""
    try:
        numpy = importlib.import_module("numpy")
        array = numpy.asarray(matrix, dtype=float)
        if not numpy.isfinite(array).all():
            return "future_regressor_unavailable"
        rank = int(numpy.linalg.matrix_rank(array))
        if rank < array.shape[1]:
            return "rank_deficient_regressors"
        condition = float(numpy.linalg.cond(array))
        if not math.isfinite(condition) or condition > 1e12:
            return "collinearity"
    except (TypeError, ValueError):
        return "future_regressor_unavailable"
    return ""


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
        {},
    )


def _effective_count(
    specification: SeasonalExogenousSpecification, count: int
) -> int:
    loss = (
        specification.d
        + max(specification.p, specification.q)
        + specification.seasonal_d * specification.seasonal_period
        + max(specification.seasonal_p, specification.seasonal_q)
        * specification.seasonal_period
    )
    return max(0, count - loss)


def _fit_sample(
    outcome: _FitOutcome,
    specification: SeasonalExogenousSpecification,
    model_id: str,
    fold: Mapping[str, Any],
    indexes: Sequence[int],
    observation_count: int,
) -> dict[str, JSONValue]:
    return {
        "schema_version": SEASONAL_EXOGENOUS_FIT_SCHEMA_VERSION,
        "model_id": model_id,
        "specification_id": specification.specification_id,
        "family": specification.family,
        "order": [specification.p, specification.d, specification.q],
        "seasonal_order": [
            specification.seasonal_p,
            specification.seasonal_d,
            specification.seasonal_q,
            specification.seasonal_period,
        ],
        "seasonal_cycle_ms": specification.seasonal_cycle_ms,
        "regressor_names": list(specification.regressor_names),
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
        "nonseasonal_differencing_loss": specification.d,
        "seasonal_differencing_loss": (
            specification.seasonal_d * specification.seasonal_period
        ),
        "segment_start_index": indexes[0] if indexes else None,
        "segment_end_index": indexes[-1] if indexes else None,
        "training_segment_policy": "trailing_contiguous_after_missing",
        "parameters": dict(outcome.parameters),
        "parameter_count": len(outcome.parameters),
        "residual_summary": dict(outcome.residual_summary),
        "warning_codes": cast(JSONValue, list(outcome.warning_codes)),
        "fit_duration_included": False,
        "backend_exception_text_included": False,
    }


def _fold_evaluation(
    rows: Sequence[Mapping[str, Any]],
    regressors: Sequence[_RegressorRow],
    fold: Mapping[str, Any],
    specification: SeasonalExogenousSpecification,
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
    target_regressor = (
        regressors[target_index]
        if 0 <= target_index < len(regressors)
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
        "schema_version": SEASONAL_EXOGENOUS_FORECAST_SCHEMA_VERSION,
        "status": status,
        "reason": reason or None,
        "series_id": _text(fold.get("series_id")),
        "period": _text(fold.get("period")),
        "model_id": model_id,
        "specification_id": specification.specification_id,
        "specification_code": specification_code,
        "family": specification.family,
        "family_code": SEASONAL_EXOGENOUS_FAMILY_CODES[specification.family],
        "p": specification.p,
        "d": specification.d,
        "q": specification.q,
        "seasonal_p": specification.seasonal_p,
        "seasonal_d": specification.seasonal_d,
        "seasonal_q": specification.seasonal_q,
        "seasonal_period": specification.seasonal_period,
        "seasonal_cycle_ms": specification.seasonal_cycle_ms,
        "trend": specification.trend,
        "trend_code": SEASONAL_EXOGENOUS_TREND_CODES[specification.trend],
        "regressor_names": list(specification.regressor_names),
        "regressor_set_code": _regressor_set_code(
            specification.regressor_names
        ),
        "regressor_count": len(specification.regressor_names),
        "regressor_available": (
            target_regressor.available if target_regressor else False
        ),
        "regressor_availability": (
            target_regressor.availability if target_regressor else "unavailable"
        ),
        "target_active_sessions": list(
            target_regressor.active_sessions if target_regressor else ()
        ),
        "target_special_tags": list(
            target_regressor.special_tags if target_regressor else ()
        ),
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
        "future_market_values_used_as_regressors": False,
        "full_series_smoothed_state_used": False,
        "residual_state_reused_across_origins": False,
        "automatic_winner": False,
    }


def _model_evaluation_payload(
    specification: SeasonalExogenousSpecification,
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


def _residual_summary(raw: Any, digits: int) -> dict[str, JSONValue]:
    values = [
        value for item in raw if (value := _optional_float(item)) is not None
    ]
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


def _model_references(
    payload: Mapping[str, JSONValue] | None,
    *,
    enabled: bool,
    unavailable_reason: str,
) -> list[dict[str, JSONValue]]:
    if not enabled:
        return [{"status": "not_requested", "automatic_winner": False}]
    selected = _mapping(payload)
    models = _mapping_rows(_mapping(selected.get("evaluation")).get("models"))
    if not models:
        return [
            {
                "status": "unavailable",
                "reason": unavailable_reason,
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
        for model in models[:MAX_SEASONAL_EXOGENOUS_SPECIFICATIONS]
    ]


def _regime_error_summary(
    evaluations: Sequence[Mapping[str, Any]], digits: int
) -> dict[str, JSONValue]:
    sessions: dict[str, list[float]] = {}
    tags: dict[str, list[float]] = {}
    for row in evaluations:
        error = _optional_float(row.get("error"))
        if error is None or row.get("status") != "evaluated":
            continue
        for session in cast(list[str], row.get("target_active_sessions", [])):
            sessions.setdefault(session, []).append(abs(error))
        for tag in cast(list[str], row.get("target_special_tags", [])):
            tags.setdefault(tag, []).append(abs(error))
    return {
        "by_active_session": _bounded_error_groups(sessions, digits),
        "by_special_tag": _bounded_error_groups(tags, digits),
        "causal_interpretation": False,
    }


def _bounded_error_groups(
    groups: Mapping[str, Sequence[float]], digits: int
) -> list[dict[str, JSONValue]]:
    return [
        {
            "name": name,
            "count": len(values),
            "mae": _rounded(sum(values) / len(values), digits),
        }
        for name, values in sorted(groups.items())[:16]
        if values
    ]


def _build_annotations(
    frame: Any | None,
    evaluations: Sequence[Mapping[str, Any]],
    profile: SeasonalExogenousProfile,
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
            current_fold = _int(evaluation.get("fold_id"))
            annotation = _annotation_row(
                evaluation, input_result, row_id, diagnostic=diagnostic
            )
            existing = merged.setdefault(
                key,
                {"series_id": group[0], "period": group[1], "row_id": row_id},
            )
            existing_fold = family_folds.get(family_key)
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
        + "schema_version": (
            SEASONAL_EXOGENOUS_TRAINING_PROJECTION_SCHEMA_VERSION
        ),
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
        prefix + "seasonal_p": _int(evaluation.get("seasonal_p")),
        prefix + "seasonal_d": _int(evaluation.get("seasonal_d")),
        prefix + "seasonal_q": _int(evaluation.get("seasonal_q")),
        prefix + "seasonal_period": _int(evaluation.get("seasonal_period")),
        prefix + "seasonal_cycle_ms": _int(evaluation.get("seasonal_cycle_ms")),
        prefix + "trend_code": _int(evaluation.get("trend_code")),
        prefix
        + "regressor_set_code": _int(evaluation.get("regressor_set_code")),
        prefix + "regressor_count": _int(evaluation.get("regressor_count")),
        prefix
        + "regressor_available": bool(
            evaluation.get("regressor_available", False)
        ),
        prefix + "calculation_basis_code": 1,
        prefix
        + "fit_status_code": SEASONAL_EXOGENOUS_FIT_STATUS_CODES.get(
            fit_status, 1
        ),
        prefix
        + "failure_reason_code": SEASONAL_EXOGENOUS_REASON_CODES.get(
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
    profile: SeasonalExogenousProfile,
    input_result: ClassicalModelInputResult,
    annotation_count: int,
    collision_count: int,
) -> dict[str, JSONValue]:
    return {
        "schema_version": SEASONAL_EXOGENOUS_TRAINING_PROJECTION_SCHEMA_VERSION,
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
        "column_names": list(SEASONAL_EXOGENOUS_COLUMNS),
        "family_column_prefixes": [
            "cm_sarima_",
            "cm_arimax_",
            "cm_sarimax_",
        ],
        "forecast_time_values_use_future_market_observations": False,
        "diagnostics_marked_post_observation": True,
        "observed_columns_overwritten": False,
    }


def _base_payload(
    input_result: ClassicalModelInputResult,
    fingerprint: Mapping[str, JSONValue],
    profile: SeasonalExogenousProfile,
    regressor_contract: Mapping[str, JSONValue],
    calendar_profile: HistDataCalendarProfile,
) -> dict[str, JSONValue]:
    contract = input_result.contract
    regularization = _mapping(contract.get("regularization"))
    return {
        "schema_version": SEASONAL_EXOGENOUS_SCHEMA_VERSION,
        "advisory": True,
        "target_axis": dict(_mapping(contract.get("target_axis"))),
        "reference_fingerprint_id": contract.get("reference_fingerprint_id")
        or fingerprint.get("fingerprint_id"),
        "input_schema_version": CLASSICAL_MODEL_INPUT_SCHEMA_VERSION,
        "input_derivation_id": contract.get("derivation_id"),
        "input_status": contract.get("status"),
        "calculation_basis": "regular_grid_rolling_origin_calendar_exog",
        "configuration": profile.to_metadata(),
        "regressors": dict(regressor_contract),
        "calendar_profile": calendar_profile.to_metadata(),
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
        "input_and_model_differencing_are_distinct": True,
        "nonseasonal_and_seasonal_differencing_are_distinct": True,
        "missing_observation_policy": "reset_to_trailing_contiguous_segment",
        "forward_fill_policy": "never",
        "original_scale_forecasts": True,
        "future_calendar_values_known_independently": True,
        "future_market_observation_regressors_allowed": False,
        "full_series_smoothed_state_allowed": False,
        "automatic_order_selection": False,
        "automatic_regressor_selection": False,
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
) -> SeasonalExogenousResult:
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
            "model_class": "statsmodels.tsa.statespace.SARIMAX",
        },
        "fit_summary": {
            "schema_version": SEASONAL_EXOGENOUS_FIT_SCHEMA_VERSION,
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
            "schema_version": SEASONAL_EXOGENOUS_EVALUATION_SCHEMA_VERSION,
            "calculation_basis": "regular_grid_rolling_origin_calendar_exog",
            "original_scale": True,
            "model_count": 0,
            "fold_count": len(input_result.folds),
            "evaluated_fold_count": 0,
            "skipped_evaluation_count": len(input_result.folds),
            "forecast_coverage_rate": None,
            "models": [],
            "reference_baselines": [],
            "reference_models": {},
            "regime_error_summary": {},
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
            "schema_version": (
                SEASONAL_EXOGENOUS_TRAINING_PROJECTION_SCHEMA_VERSION
            ),
            "column_names": list(SEASONAL_EXOGENOUS_COLUMNS),
            "projection_specification_ids": configuration.get(
                "projection_specification_ids"
            )
            or [],
            "annotation_count": 0,
        },
        "fit_duration_included": False,
    }
    return SeasonalExogenousResult(diagnostics, (), input_result)


def _load_backend() -> _Backend | None:
    try:
        module = importlib.import_module("statsmodels.tsa.statespace.sarimax")
        version = importlib.metadata.version("statsmodels")
    except (ImportError, importlib.metadata.PackageNotFoundError):
        return None
    return _Backend(version=version, sarimax=module.SARIMAX)


def _backend_failure_reason(exc: Exception) -> str:
    message = str(exc).lower()
    if "seasonal periodicity" in message or "seasonal_order" in message:
        return "invalid_seasonality"
    if "rank" in message:
        return "rank_deficient_regressors"
    if "collinear" in message:
        return "collinearity"
    if "exog" in message or "regressor" in message:
        return "future_regressor_unavailable"
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


def _estimated_memory(
    row_count: int,
    fold_count: int,
    specifications: Sequence[SeasonalExogenousSpecification],
) -> int:
    regressor_count = sum(len(item.regressor_names) for item in specifications)
    return max(
        1,
        row_count * max(1, regressor_count) * 8
        + fold_count * max(1, len(specifications)) * 4_096,
    )


def _model_id(
    specification: SeasonalExogenousSpecification,
    derivation_id: str,
    backend_version: str,
    calendar_profile: HistDataCalendarProfile,
) -> str:
    payload = {
        "backend": "statsmodels",
        "backend_version": backend_version,
        "configuration": specification.to_metadata(),
        "input_derivation_id": derivation_id,
        "calendar_profile": {
            "name": calendar_profile.name,
            "version": calendar_profile.version,
            "source": calendar_profile.source,
        },
    }
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _regressor_set_code(names: Sequence[str]) -> int:
    encoded = "\n".join(names).encode("utf-8")
    return int.from_bytes(hashlib.sha256(encoded).digest()[:4], "big")


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
        "regressor_available",
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
        for family in SEASONAL_EXOGENOUS_FAMILIES
        for suffix in SEASONAL_EXOGENOUS_FAMILY_COLUMN_SUFFIXES
    }
