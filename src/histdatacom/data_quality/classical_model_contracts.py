"""Classical-model input, fold, fit, and evaluation contracts.

This module deliberately stops before fitting a statistical model.  It turns
the enriched ASCII tick training frame into a deterministic regular-grid view,
defines leakage-safe evaluation folds, and projects only point-in-time-safe
scalars back onto the canonical tick rows.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import math
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, TypedDict, cast

from histdatacom.data_quality.calendar import (
    SESSION_STATE_WEEKEND_CLOSURE,
    classify_histdata_timestamp,
)
from histdatacom.data_quality.contracts import QualityFinding
from histdatacom.data_quality.limits import bounded_report_limit
from histdatacom.data_quality.training_features import (
    CLASSICAL_MODEL_CONTRACT_COLUMNS,
    TRAINING_SCHEMA_VERSION,
    ensure_tick_training_features,
)
from histdatacom.runtime_contracts import JSONValue

CLASSICAL_MODEL_INPUT_SCHEMA_VERSION = "histdatacom.classical-model-input.v1"
CLASSICAL_MODEL_FOLD_SCHEMA_VERSION = "histdatacom.classical-model-fold.v1"
CLASSICAL_MODEL_FIT_RESULT_SCHEMA_VERSION = (
    "histdatacom.classical-model-fit-result.v1"
)
CLASSICAL_MODEL_EVALUATION_RESULT_SCHEMA_VERSION = (
    "histdatacom.classical-model-evaluation-result.v1"
)
CLASSICAL_MODEL_TRAINING_PROJECTION_SCHEMA_VERSION = (
    "histdatacom.classical-model-training-projection.v1"
)
CLASSICAL_MODEL_INPUT_SUMMARY_SCHEMA_VERSION = (
    "histdatacom.classical-model-input-summary.v1"
)
CLASSICAL_MODEL_INPUT_SUMMARY_METADATA_KEY = (
    "time_series_fingerprint_classical_model_input_summary"
)
CLASSICAL_MODEL_INPUT_BOUNDED_PAYLOAD_KEY = "fingerprint_classical_model_input"

DEFAULT_MODEL_FREQUENCY_MS = 60_000
DEFAULT_MODEL_MINIMUM_TRAINING_OBSERVATIONS = 20
DEFAULT_MODEL_MINIMUM_EVALUATION_OBSERVATIONS = 5
DEFAULT_MODEL_STEP_SIZE = 5
DEFAULT_MODEL_HORIZONS = (1,)
DEFAULT_MODEL_ROUNDING_DIGITS = 12
DEFAULT_MODEL_INPUT_SUMMARY_TARGET_LIMIT = 16
MAX_MODEL_HORIZONS = 16

MODEL_INPUT_REQUIRED_COLUMNS = (
    "series_id",
    "period",
    "row_id",
    "timestamp_utc_ms",
    "mid",
    "spread",
    "training_usable",
)
MODEL_INPUT_STATUS_CODES = {
    "unavailable": 1,
    "limited": 2,
    "ready": 3,
}
MODEL_INPUT_REASON_CODES = {
    "": 0,
    "training_frame_unavailable": 1,
    "missing_required_columns": 2,
    "no_usable_rows": 3,
    "source_row_limit": 4,
    "regularized_observation_limit": 5,
    "insufficient_regularized_observations": 6,
    "insufficient_folds": 7,
    "invalid_transform_domain": 8,
}
MODEL_BIN_STATUS_CODES = {
    "observed": 1,
    "expected_closure": 2,
    "unexpected_missing": 3,
    "insufficient_observations": 4,
}
MODEL_TRANSFORM_CODES = {
    "level": 1,
    "log_level": 2,
    "return": 3,
    "log_return": 4,
}
MODEL_FOLD_KIND_CODES = {"expanding": 1, "rolling": 2}
MODEL_EVALUATION_STATUS_CODES = {
    "unavailable": 1,
    "contract_ready": 2,
    "evaluated": 3,
}
MODEL_FIT_STATUSES = (
    "fitted",
    "converged",
    "limited",
    "skipped",
    "timed_out",
    "numerically_invalid",
    "dependency_unavailable",
    "failed",
)
MODEL_FAILURE_REASON_CODES = (
    "",
    "insufficient_data",
    "invalid_configuration",
    "optimizer_failure",
    "numerical_failure",
    "resource_limit",
    "timeout",
    "dependency_unavailable",
    "backend_failure",
)
MODEL_OPTIONAL_DEPENDENCIES = (
    ("statsmodels", "ETS, ARIMA, SARIMAX, and state-space families"),
    ("arch", "ARCH and GARCH volatility families"),
)


class _GridRow(TypedDict):
    series_id: str
    period: str
    cm_input_bin_start_utc_ms: int
    cm_input_bin_end_utc_ms: int
    cm_input_available_at_utc_ms: int
    cm_input_observation_count: int
    cm_input_source_first_row_id: int | None
    cm_input_source_last_row_id: int | None
    cm_input_status_code: int
    cm_input_expected_closure: bool
    cm_input_unexpected_missing: bool
    cm_input_mid_open: float | None
    cm_input_mid_high: float | None
    cm_input_mid_low: float | None
    cm_input_mid_close: float | None
    cm_input_mid_mean: float | None
    cm_input_mid_median: float | None
    cm_input_spread: float | None
    cm_input_observed_value: float | None
    cm_input_value: float | None
    cm_input_transform_valid: bool


@dataclass(frozen=True, slots=True)
class ClassicalModelResourcePolicy:
    """Bounded resource limits shared by future classical model families."""

    max_source_rows: int = 1_000_000
    max_regularized_observations: int = 100_000
    max_folds: int = 64
    max_horizons: int = MAX_MODEL_HORIZONS
    max_candidate_orders: int = 32
    max_fit_attempts: int = 64
    max_wall_time_seconds: int = 300
    max_memory_bytes: int = 536_870_912
    max_retained_diagnostics: int = 64

    def __post_init__(self) -> None:
        for name, value in self.to_metadata().items():
            if not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return stable JSON-compatible limit metadata."""
        return {
            "max_source_rows": self.max_source_rows,
            "max_regularized_observations": (self.max_regularized_observations),
            "max_folds": self.max_folds,
            "max_horizons": self.max_horizons,
            "max_candidate_orders": self.max_candidate_orders,
            "max_fit_attempts": self.max_fit_attempts,
            "max_wall_time_seconds": self.max_wall_time_seconds,
            "max_memory_bytes": self.max_memory_bytes,
            "max_retained_diagnostics": self.max_retained_diagnostics,
        }


@dataclass(frozen=True, slots=True)
class ClassicalModelInputProfile:
    """Explicit regularization, transform, fold, and limit controls."""

    enabled: bool = False
    frequency_ms: int = DEFAULT_MODEL_FREQUENCY_MS
    alignment_epoch_ms: int = 0
    closed_side: str = "left"
    label_side: str = "left"
    midpoint_aggregation: str = "last"
    spread_aggregation: str = "last"
    minimum_observations_per_bin: int = 1
    expected_closure_policy: str = "mark"
    unexpected_missing_policy: str = "mark"
    transform: str = "level"
    differencing_order: int = 0
    seasonal_differencing_order: int = 0
    seasonal_period: int = 0
    horizons: tuple[int, ...] = DEFAULT_MODEL_HORIZONS
    fold_kind: str = "expanding"
    minimum_training_observations: int = (
        DEFAULT_MODEL_MINIMUM_TRAINING_OBSERVATIONS
    )
    minimum_evaluation_observations: int = (
        DEFAULT_MODEL_MINIMUM_EVALUATION_OBSERVATIONS
    )
    step_size: int = DEFAULT_MODEL_STEP_SIZE
    rolling_window: int = 0
    embargo_observations: int = 0
    rounding_digits: int = DEFAULT_MODEL_ROUNDING_DIGITS
    resources: ClassicalModelResourcePolicy = field(
        default_factory=ClassicalModelResourcePolicy
    )

    def __post_init__(self) -> None:
        if self.frequency_ms < 1:
            raise ValueError("frequency_ms must be positive")
        if self.closed_side != "left" or self.label_side != "left":
            raise ValueError(
                "only left-closed, left-labeled UTC bins are supported"
            )
        allowed_aggregations = {"first", "last", "mean", "median"}
        if self.midpoint_aggregation not in allowed_aggregations:
            raise ValueError("unsupported midpoint_aggregation")
        if self.spread_aggregation not in allowed_aggregations:
            raise ValueError("unsupported spread_aggregation")
        if self.minimum_observations_per_bin < 1:
            raise ValueError("minimum_observations_per_bin must be positive")
        if self.expected_closure_policy not in {"mark", "omit"}:
            raise ValueError("unsupported expected_closure_policy")
        if self.unexpected_missing_policy != "mark":
            raise ValueError("unexpected missing bins must be marked")
        if self.transform not in MODEL_TRANSFORM_CODES:
            raise ValueError("unsupported transform")
        if self.differencing_order not in {0, 1, 2}:
            raise ValueError("differencing_order must be 0, 1, or 2")
        if self.seasonal_differencing_order not in {0, 1}:
            raise ValueError("seasonal_differencing_order must be 0 or 1")
        if self.seasonal_differencing_order and self.seasonal_period < 2:
            raise ValueError("seasonal_period must be at least 2")
        if not self.horizons or any(value < 1 for value in self.horizons):
            raise ValueError("horizons must contain positive integers")
        if len(self.horizons) > self.resources.max_horizons:
            raise ValueError("horizons exceed the resource policy")
        if tuple(sorted(set(self.horizons))) != self.horizons:
            raise ValueError("horizons must be sorted and unique")
        if self.fold_kind not in MODEL_FOLD_KIND_CODES:
            raise ValueError("fold_kind must be expanding or rolling")
        for name in (
            "minimum_training_observations",
            "minimum_evaluation_observations",
            "step_size",
        ):
            if int(getattr(self, name)) < 1:
                raise ValueError(f"{name} must be positive")
        if self.fold_kind == "rolling" and (
            self.rolling_window < self.minimum_training_observations
        ):
            raise ValueError(
                "rolling_window must cover minimum training observations"
            )
        if self.embargo_observations < 0:
            raise ValueError("embargo_observations must be non-negative")
        if not 0 <= self.rounding_digits <= 16:
            raise ValueError("rounding_digits must be between 0 and 16")

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return a stable profile payload."""
        return {
            "enabled": self.enabled,
            "frequency_ms": self.frequency_ms,
            "alignment_epoch_ms": self.alignment_epoch_ms,
            "timezone": "UTC",
            "closed_side": self.closed_side,
            "label_side": self.label_side,
            "midpoint_aggregation": self.midpoint_aggregation,
            "spread_aggregation": self.spread_aggregation,
            "minimum_observations_per_bin": (self.minimum_observations_per_bin),
            "expected_closure_policy": self.expected_closure_policy,
            "unexpected_missing_policy": self.unexpected_missing_policy,
            "transform": self.transform,
            "differencing_order": self.differencing_order,
            "seasonal_differencing_order": (self.seasonal_differencing_order),
            "seasonal_period": self.seasonal_period,
            "horizons": list(self.horizons),
            "fold_kind": self.fold_kind,
            "minimum_training_observations": (
                self.minimum_training_observations
            ),
            "minimum_evaluation_observations": (
                self.minimum_evaluation_observations
            ),
            "step_size": self.step_size,
            "rolling_window": self.rolling_window,
            "embargo_observations": self.embargo_observations,
            "rounding_digits": self.rounding_digits,
            "resources": self.resources.to_metadata(),
        }


@dataclass(frozen=True, slots=True)
class ClassicalModelInputResult:
    """A bounded regularized frame plus serializable contract metadata."""

    regularized_frame: Any
    contract: Mapping[str, JSONValue]
    folds: tuple[Mapping[str, JSONValue], ...]


def build_classical_model_input(
    frame: Any | None,
    fingerprint: Mapping[str, JSONValue],
    *,
    profile: ClassicalModelInputProfile | None = None,
    target: Any | None = None,
) -> ClassicalModelInputResult:
    """Build a deterministic regular-grid view and leakage-safe folds."""
    import polars as pl

    selected = profile or ClassicalModelInputProfile(enabled=True)
    base = _base_contract(fingerprint, selected)
    if frame is None:
        return _unavailable_result(base, "training_frame_unavailable")
    input_was_enriched = "training_schema_version" in getattr(
        frame, "columns", ()
    )
    try:
        enriched = ensure_tick_training_features(frame, target=target)
    except (AttributeError, TypeError, ValueError):
        return _unavailable_result(base, "training_frame_unavailable")
    missing = sorted(
        set(MODEL_INPUT_REQUIRED_COLUMNS)
        - set(getattr(enriched, "columns", ()))
    )
    if missing:
        base["missing_required_columns"] = cast(JSONValue, missing)
        return _unavailable_result(base, "missing_required_columns")

    source_row_count = int(enriched.height)
    source_truncated = source_row_count > selected.resources.max_source_rows
    bounded = enriched.head(selected.resources.max_source_rows)
    usable = bounded.filter(
        pl.col("training_usable").fill_null(False)
        & pl.col("timestamp_utc_ms").is_not_null()
        & pl.col("mid").is_not_null()
        & pl.col("mid").is_finite()
        & pl.col("spread").is_not_null()
        & pl.col("spread").is_finite()
    )
    if usable.height < 1:
        base["source"] = {
            **cast(dict[str, JSONValue], base["source"]),
            "row_count": source_row_count,
            "bounded_row_count": int(bounded.height),
            "usable_row_count": 0,
            "structurally_unusable_row_count": int(bounded.height),
            "source_rows_truncated": source_truncated,
            "legacy_cache_enriched_on_read": not input_was_enriched,
        }
        return _unavailable_result(base, "no_usable_rows")

    grid, grid_metadata = _regularize(usable, selected)
    transformed, transform_metadata = _apply_transforms(grid, selected)
    folds = _build_folds(transformed, selected)
    annotated = _annotate_primary_folds(transformed, folds)
    limitations: list[str] = []
    if source_truncated:
        limitations.append("source_row_limit")
    if grid_metadata["truncated"] is True:
        limitations.append("regularized_observation_limit")
    valid_count = _valid_observation_count(annotated)
    if valid_count < selected.minimum_training_observations:
        limitations.append("insufficient_regularized_observations")
    if not folds:
        limitations.append("insufficient_folds")
    if _int(transform_metadata["invalid_domain_count"]) > 0:
        limitations.append("invalid_transform_domain")
    status = "ready" if not limitations else "limited"
    reason = limitations[0] if limitations else None
    contract: dict[str, JSONValue] = {
        **base,
        "status": status,
        "reason": reason,
        "limitations": cast(JSONValue, limitations),
        "derivation_id": _derivation_id(annotated, selected, fingerprint),
        "source": {
            **cast(dict[str, JSONValue], base["source"]),
            "row_count": source_row_count,
            "bounded_row_count": int(bounded.height),
            "usable_row_count": int(usable.height),
            "structurally_unusable_row_count": int(
                bounded.height - usable.height
            ),
            "source_rows_truncated": source_truncated,
            "legacy_cache_enriched_on_read": not input_was_enriched,
        },
        "regularization": grid_metadata,
        "transform_policy": transform_metadata,
        "fold_policy": _fold_policy(selected, folds),
        "dependency_policy": classical_model_dependency_status(),
        "training_projection": _training_projection_metadata(
            status, reason, selected, folds
        ),
    }
    return ClassicalModelInputResult(
        regularized_frame=annotated,
        contract=contract,
        folds=tuple(folds),
    )


def classical_model_input_contract_from_training_frame(
    frame: Any | None,
    fingerprint: Mapping[str, JSONValue],
    *,
    profile: ClassicalModelInputProfile | None = None,
    target: Any | None = None,
) -> dict[str, JSONValue]:
    """Return only the serializable contract for fingerprint/report use."""
    return dict(
        build_classical_model_input(
            frame,
            fingerprint,
            profile=profile,
            target=target,
        ).contract
    )


def project_classical_model_input_onto_training_frame(
    frame: Any,
    result: ClassicalModelInputResult,
    *,
    target: Any | None = None,
) -> Any:
    """Project completed-bin scalars without leaking before bin close."""
    import polars as pl

    columns = set(getattr(frame, "columns", ()))
    if not {"series_id", "period", "row_id"}.issubset(columns):
        enriched = ensure_tick_training_features(frame, target=target)
    else:
        enriched = frame
    missing = sorted({"series_id", "period", "row_id"} - set(enriched.columns))
    if missing:
        raise ValueError(
            "classical model projection requires identity columns: "
            + ", ".join(missing)
        )
    if "timestamp_utc_ms" not in enriched.columns:
        return _with_empty_projection(enriched)
    grid = result.regularized_frame
    if getattr(grid, "height", 0) < 1:
        return _with_empty_projection(enriched)

    right = _projection_frame(grid, result.contract)
    left = enriched.drop(
        [
            column
            for column in CLASSICAL_MODEL_CONTRACT_COLUMNS
            if column in enriched.columns
        ]
    ).with_row_index("__cm_original_order")
    with_timestamp = left.filter(pl.col("timestamp_utc_ms").is_not_null()).sort(
        "series_id", "period", "timestamp_utc_ms", "row_id"
    )
    without_timestamp = left.filter(pl.col("timestamp_utc_ms").is_null())
    if with_timestamp.height:
        projected = with_timestamp.join_asof(
            right.sort(
                "series_id",
                "period",
                "cm_input_available_at_utc_ms",
            ),
            left_on="timestamp_utc_ms",
            right_on="cm_input_available_at_utc_ms",
            by=["series_id", "period"],
            strategy="backward",
            check_sortedness=False,
        )
    else:
        projected = _with_empty_projection(with_timestamp)
    if without_timestamp.height:
        without_timestamp = _with_empty_projection(without_timestamp)
        projected = pl.concat(
            [projected, without_timestamp], how="diagonal_relaxed"
        )
    projected = projected.sort("__cm_original_order").drop(
        "__cm_original_order"
    )
    return _ensure_projection_dtypes(projected)


def classical_model_fit_result(
    *,
    model_id: str,
    family: str,
    status: str,
    reason: str = "",
    warning_codes: Sequence[str] = (),
    parameter_count: int = 0,
    fitted_observation_count: int = 0,
) -> dict[str, JSONValue]:
    """Create a bounded family-neutral fit-result contract."""
    if status not in MODEL_FIT_STATUSES:
        raise ValueError("unsupported fit status")
    if reason not in MODEL_FAILURE_REASON_CODES:
        raise ValueError("unsupported fit failure reason")
    warnings = sorted(set(str(value) for value in warning_codes))[:64]
    return {
        "schema_version": CLASSICAL_MODEL_FIT_RESULT_SCHEMA_VERSION,
        "advisory": True,
        "model_id": model_id,
        "family": family,
        "status": status,
        "reason": reason or None,
        "parameter_count": max(0, int(parameter_count)),
        "fitted_observation_count": max(0, int(fitted_observation_count)),
        "warning_codes": cast(JSONValue, warnings),
        "warning_count": len(warnings),
        "backend_exception_text_included": False,
    }


def classical_model_evaluation_result(
    *,
    model_id: str,
    status: str,
    fold_count: int,
    horizon_count: int,
    metric_scale: str,
    reason: str = "",
) -> dict[str, JSONValue]:
    """Create a bounded evaluation-result contract without model fitting."""
    if status not in {"unavailable", "contract_ready", "evaluated"}:
        raise ValueError("unsupported evaluation status")
    return {
        "schema_version": CLASSICAL_MODEL_EVALUATION_RESULT_SCHEMA_VERSION,
        "advisory": True,
        "model_id": model_id,
        "status": status,
        "reason": reason or None,
        "fold_count": max(0, int(fold_count)),
        "horizon_count": max(0, int(horizon_count)),
        "metric_scale": metric_scale,
        "automatic_winner": False,
        "full_forecasts_included": False,
        "full_residuals_included": False,
    }


def classical_model_dependency_status(
    *, probe: bool = False
) -> dict[str, JSONValue]:
    """Describe optional providers without making contracts environment-specific."""
    dependencies: list[dict[str, JSONValue]] = []
    for package, purpose in MODEL_OPTIONAL_DEPENDENCIES:
        dependencies.append(
            {
                "package": package,
                "available": (
                    importlib.util.find_spec(package) is not None
                    if probe
                    else None
                ),
                "purpose": purpose,
            }
        )
    return {
        "core_dependency_added": False,
        "future_install_extra": "models",
        "contract_available_without_optional_dependencies": True,
        "rich_model_fitting_available": False,
        "availability_basis": "runtime_probe" if probe else "not_probed",
        "dependencies": cast(JSONValue, dependencies),
    }


def classical_model_input_summary(
    findings: Iterable[QualityFinding],
    *,
    target_limit: int | None = DEFAULT_MODEL_INPUT_SUMMARY_TARGET_LIMIT,
) -> dict[str, JSONValue] | None:
    """Return a bounded report summary for opt-in model input contracts."""
    targets: list[dict[str, JSONValue]] = []
    statuses: Counter[str] = Counter()
    for finding in findings:
        fingerprint = _mapping(finding.metadata.get("time_series_fingerprint"))
        contract = _mapping(fingerprint.get("classical_model_input"))
        if not contract:
            continue
        status = _text(contract.get("status")) or "unavailable"
        regularization = _mapping(contract.get("regularization"))
        fold_policy = _mapping(contract.get("fold_policy"))
        statuses[status] += 1
        targets.append(
            {
                "target_axis": dict(_mapping(contract.get("target_axis"))),
                "status": status,
                "reason": contract.get("reason"),
                "regularized_observation_count": _int(
                    regularization.get("regularized_observation_count")
                ),
                "observed_bin_count": _int(
                    regularization.get("observed_bin_count")
                ),
                "expected_closure_count": _int(
                    regularization.get("expected_closure_count")
                ),
                "unexpected_missing_count": _int(
                    regularization.get("unexpected_missing_count")
                ),
                "fold_count": _int(fold_policy.get("fold_count")),
            }
        )
    if not targets:
        return None
    targets.sort(key=_target_sort_key)
    limit = bounded_report_limit(
        target_limit,
        default_limit=DEFAULT_MODEL_INPUT_SUMMARY_TARGET_LIMIT,
        allow_unbounded=True,
    )
    included = limit.slice(targets)
    omitted = len(targets) - len(included)
    return {
        "schema_version": CLASSICAL_MODEL_INPUT_SUMMARY_SCHEMA_VERSION,
        "advisory": True,
        "target_count": len(targets),
        "included_target_count": len(included),
        "omitted_target_count": omitted,
        "truncated": omitted > 0,
        "status_counts": dict(sorted(statuses.items())),
        "target_summaries": cast(JSONValue, included),
        "limit_metadata": {"targets": limit.limit_payload()},
    }


def format_classical_model_input_summary_lines(
    summary: Mapping[str, JSONValue] | None,
) -> tuple[str, ...]:
    """Return concise human-readable model-input contract lines."""
    if not summary:
        return ()
    statuses = _mapping(summary.get("status_counts"))
    lines = [
        "",
        "Classical model input contracts",
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
            f"- {label}: {target.get('status', 'unavailable')} "
            f"bins={_int(target.get('regularized_observation_count'))} "
            f"folds={_int(target.get('fold_count'))}"
        )
    if summary.get("truncated") is True:
        lines.append(
            f"- {_int(summary.get('omitted_target_count'))} targets omitted"
        )
    return tuple(lines)


def _base_contract(
    fingerprint: Mapping[str, JSONValue],
    profile: ClassicalModelInputProfile,
) -> dict[str, JSONValue]:
    reference_fingerprint_id = _reference_fingerprint_id(fingerprint)
    return {
        "schema_version": CLASSICAL_MODEL_INPUT_SCHEMA_VERSION,
        "advisory": True,
        "base_grain": {"data_format": "ascii", "timeframe": "T"},
        "target_axis": dict(_mapping(fingerprint.get("target_axis"))),
        "reference_fingerprint_id": reference_fingerprint_id,
        "reference_fingerprint_basis": (
            "provided_fingerprint_id"
            if _text(fingerprint.get("fingerprint_id"))
            else "canonical_pre_contract_snapshot"
        ),
        "source": {
            "kind": "enriched_training_frame",
            "row_count": 0,
            "usable_row_count": 0,
            "legacy_cache_enriched_on_read": False,
        },
        "training_schema_version": TRAINING_SCHEMA_VERSION,
        "row_selection_policy": "training_usable_and_finite_mid_spread",
        "configuration": profile.to_metadata(),
        "calculation_basis": "regular_grid_from_enriched_ascii_ticks",
        "canonical_row_identity": ["series_id", "period", "row_id"],
        "timestamp_is_sole_identity": False,
        "regularized_view_replaces_source_cache": False,
        "derived_ohlc_is_canonical_source": False,
        "hard_fail_quality_gate": False,
        "model_fitting_in_scope": False,
    }


def _unavailable_result(
    base: Mapping[str, JSONValue], reason: str
) -> ClassicalModelInputResult:
    import polars as pl

    contract: dict[str, JSONValue] = {
        **dict(base),
        "status": "unavailable",
        "reason": reason,
        "limitations": cast(JSONValue, [reason]),
        "regularization": _empty_regularization(),
        "transform_policy": _empty_transform_policy(base),
        "fold_policy": _empty_fold_policy(base),
        "dependency_policy": classical_model_dependency_status(),
        "training_projection": _training_projection_metadata(
            "unavailable",
            reason,
            _profile_from_base(base),
            (),
        ),
    }
    return ClassicalModelInputResult(pl.DataFrame(), contract, ())


def _regularize(
    frame: Any, profile: ClassicalModelInputProfile
) -> tuple[Any, dict[str, JSONValue]]:
    import polars as pl

    frequency = profile.frequency_ms
    alignment = profile.alignment_epoch_ms
    working = frame.sort(
        "series_id", "period", "timestamp_utc_ms", "row_id"
    ).with_columns(
        (
            ((pl.col("timestamp_utc_ms") - alignment) // frequency) * frequency
            + alignment
        )
        .cast(pl.Int64)
        .alias("__cm_bin_start")
    )
    grouped = (
        working.group_by("series_id", "period", "__cm_bin_start")
        .agg(
            pl.len().alias("observation_count"),
            pl.col("row_id").first().alias("source_first_row_id"),
            pl.col("row_id").last().alias("source_last_row_id"),
            pl.col("mid").first().alias("mid_first"),
            pl.col("mid").last().alias("mid_last"),
            pl.col("mid").mean().alias("mid_mean"),
            pl.col("mid").median().alias("mid_median"),
            pl.col("mid").min().alias("mid_min"),
            pl.col("mid").max().alias("mid_max"),
            pl.col("spread").first().alias("spread_first"),
            pl.col("spread").last().alias("spread_last"),
            pl.col("spread").mean().alias("spread_mean"),
            pl.col("spread").median().alias("spread_median"),
        )
        .sort("series_id", "period", "__cm_bin_start")
    )
    observed = {
        (
            str(row["series_id"]),
            str(row["period"]),
            int(row["__cm_bin_start"]),
        ): row
        for row in grouped.to_dicts()
    }
    rows: list[_GridRow] = []
    truncated = False
    group_bounds = (
        grouped.group_by("series_id", "period")
        .agg(
            pl.col("__cm_bin_start").min().alias("first_bin"),
            pl.col("__cm_bin_start").max().alias("last_bin"),
        )
        .sort("series_id", "period")
        .to_dicts()
    )
    for bounds in group_bounds:
        series_id = str(bounds["series_id"])
        period = str(bounds["period"])
        first_bin = int(bounds["first_bin"])
        last_bin = int(bounds["last_bin"])
        for bin_start in range(first_bin, last_bin + frequency, frequency):
            if len(rows) >= profile.resources.max_regularized_observations:
                truncated = True
                break
            item = observed.get((series_id, period, bin_start))
            rows.append(
                _grid_row(
                    series_id,
                    period,
                    bin_start,
                    item,
                    profile,
                )
            )
        if truncated:
            break
    result = pl.DataFrame(rows) if rows else pl.DataFrame()
    statuses = Counter(
        _bin_status_name(row["cm_input_status_code"]) for row in rows
    )
    metadata: dict[str, JSONValue] = {
        "schema_version": CLASSICAL_MODEL_INPUT_SCHEMA_VERSION,
        "basis": "regular_grid",
        "source_basis": "enriched_ascii_tick_rows",
        "timezone": "UTC",
        "frequency_ms": profile.frequency_ms,
        "alignment_epoch_ms": profile.alignment_epoch_ms,
        "closed_side": profile.closed_side,
        "label_side": profile.label_side,
        "bin_interval": "[start,end)",
        "midpoint_aggregation": profile.midpoint_aggregation,
        "spread_aggregation": profile.spread_aggregation,
        "derived_ohlc_fields": [
            "cm_input_mid_open",
            "cm_input_mid_high",
            "cm_input_mid_low",
            "cm_input_mid_close",
        ],
        "minimum_observations_per_bin": (profile.minimum_observations_per_bin),
        "duplicate_timestamp_policy": (
            "preserve_and_aggregate_in_row_id_order"
        ),
        "partial_or_unusable_row_policy": (
            "exclude_via_training_usable_before_regularization"
        ),
        "empty_bin_value_policy": "explicit_null",
        "insufficient_bin_value_policy": "explicit_null",
        "expected_closure_policy": profile.expected_closure_policy,
        "expected_closure_grid_rows_retained": True,
        "expected_closure_model_observations_omitted": (
            profile.expected_closure_policy == "omit"
        ),
        "unexpected_missing_policy": profile.unexpected_missing_policy,
        "forward_fill_policy": "never",
        "period_boundary_crossing": False,
        "period_partitioned": True,
        "rounding_digits": profile.rounding_digits,
        "rounding_applies_to": ["cm_input_value"],
        "regularized_observation_count": len(rows),
        "observed_bin_count": statuses["observed"],
        "expected_closure_count": statuses["expected_closure"],
        "unexpected_missing_count": statuses["unexpected_missing"],
        "insufficient_observation_bin_count": statuses[
            "insufficient_observations"
        ],
        "structurally_unavailable_count": statuses["insufficient_observations"],
        "truncated": truncated,
        "row_mapping_policy": "availability_safe_repetition_after_bin_close",
    }
    return result, metadata


def _grid_row(
    series_id: str,
    period: str,
    bin_start: int,
    item: Mapping[str, Any] | None,
    profile: ClassicalModelInputProfile,
) -> _GridRow:
    bin_end = bin_start + profile.frequency_ms
    if item is None:
        expected = (
            classify_histdata_timestamp(bin_start).session_state
            == SESSION_STATE_WEEKEND_CLOSURE
        )
        status = "expected_closure" if expected else "unexpected_missing"
        count = 0
    else:
        count = int(item.get("observation_count", 0) or 0)
        expected = False
        status = (
            "observed"
            if count >= profile.minimum_observations_per_bin
            else "insufficient_observations"
        )
    observed_value = (
        _aggregate_value(item, "mid", profile.midpoint_aggregation)
        if status == "observed"
        else None
    )
    spread_value = (
        _aggregate_value(item, "spread", profile.spread_aggregation)
        if status == "observed"
        else None
    )
    return {
        "series_id": series_id,
        "period": period,
        "cm_input_bin_start_utc_ms": bin_start,
        "cm_input_bin_end_utc_ms": bin_end,
        "cm_input_available_at_utc_ms": bin_end,
        "cm_input_observation_count": count,
        "cm_input_source_first_row_id": _optional_int(
            item.get("source_first_row_id") if item else None
        ),
        "cm_input_source_last_row_id": _optional_int(
            item.get("source_last_row_id") if item else None
        ),
        "cm_input_status_code": MODEL_BIN_STATUS_CODES[status],
        "cm_input_expected_closure": expected,
        "cm_input_unexpected_missing": status == "unexpected_missing",
        "cm_input_mid_open": _optional_float(
            item.get("mid_first") if item else None
        ),
        "cm_input_mid_high": _optional_float(
            item.get("mid_max") if item else None
        ),
        "cm_input_mid_low": _optional_float(
            item.get("mid_min") if item else None
        ),
        "cm_input_mid_close": _optional_float(
            item.get("mid_last") if item else None
        ),
        "cm_input_mid_mean": _optional_float(
            item.get("mid_mean") if item else None
        ),
        "cm_input_mid_median": _optional_float(
            item.get("mid_median") if item else None
        ),
        "cm_input_spread": spread_value,
        "cm_input_observed_value": observed_value,
        "cm_input_value": observed_value,
        "cm_input_transform_valid": observed_value is not None,
    }


def _aggregate_value(
    item: Mapping[str, Any] | None, prefix: str, aggregation: str
) -> float | None:
    if item is None:
        return None
    return _optional_float(item.get(f"{prefix}_{aggregation}"))


def _apply_transforms(
    frame: Any, profile: ClassicalModelInputProfile
) -> tuple[Any, dict[str, JSONValue]]:
    import polars as pl

    if getattr(frame, "height", 0) < 1:
        return frame, _empty_transform_policy_from_profile(profile)
    rows = cast(list[dict[str, Any]], frame.to_dicts())
    invalid_domain_count = 0
    warmup_loss = 0
    groups: dict[tuple[str, str], list[int]] = {}
    for index, row in enumerate(rows):
        groups.setdefault(
            (str(row["series_id"]), str(row["period"])), []
        ).append(index)
    for indexes in groups.values():
        base: list[float | None] = []
        previous: float | None = None
        for index in indexes:
            value = _optional_float(rows[index].get("cm_input_observed_value"))
            transformed: float | None
            if value is None:
                transformed = None
                previous = None
            elif profile.transform == "level":
                transformed = value
            elif profile.transform == "log_level":
                transformed = math.log(value) if value > 0 else None
            elif previous is None:
                transformed = None
            elif profile.transform == "return":
                transformed = value / previous - 1 if previous != 0 else None
            else:
                transformed = (
                    math.log(value / previous)
                    if value > 0 and previous > 0
                    else None
                )
            if value is not None and transformed is None:
                if (
                    profile.transform in {"log_level", "log_return"}
                    and value <= 0
                ):
                    invalid_domain_count += 1
                else:
                    warmup_loss += 1
            base.append(transformed)
            if value is not None:
                previous = value
        values = base
        for _ in range(profile.differencing_order):
            values = _difference(values, 1)
        for _ in range(profile.seasonal_differencing_order):
            values = _difference(values, profile.seasonal_period)
        for offset, index in enumerate(indexes):
            original = base[offset]
            value = values[offset]
            if original is not None and value is None:
                warmup_loss += 1
            rows[index]["cm_input_value"] = _rounded(
                value, profile.rounding_digits
            )
            rows[index]["cm_input_transform_valid"] = value is not None
    result = pl.DataFrame(rows, infer_schema_length=None)
    metadata = _empty_transform_policy_from_profile(profile)
    metadata.update(
        {
            "invalid_domain_count": invalid_domain_count,
            "warmup_loss": warmup_loss,
            "transformed_observation_count": _valid_observation_count(result),
        }
    )
    return result, metadata


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


def _build_folds(
    frame: Any, profile: ClassicalModelInputProfile
) -> list[dict[str, JSONValue]]:
    if getattr(frame, "height", 0) < 1:
        return []
    rows = cast(list[dict[str, Any]], frame.to_dicts())
    folds: list[dict[str, JSONValue]] = []
    groups: dict[tuple[str, str], list[int]] = {}
    for index, row in enumerate(rows):
        groups.setdefault(
            (str(row["series_id"]), str(row["period"])), []
        ).append(index)
    fold_id = 1
    for (series_id, period), indexes in sorted(groups.items()):
        valid = [
            index
            for index in indexes
            if rows[index].get("cm_input_transform_valid") is True
        ]
        if len(valid) < (
            profile.minimum_training_observations
            + profile.minimum_evaluation_observations
        ):
            continue
        origins = valid[
            profile.minimum_training_observations - 1 :: profile.step_size
        ]
        for origin in origins:
            future_valid = [index for index in valid if index > origin]
            if len(future_valid) < profile.minimum_evaluation_observations:
                continue
            if profile.fold_kind == "rolling":
                train_valid = [index for index in valid if index <= origin][
                    -profile.rolling_window :
                ]
            else:
                train_valid = [index for index in valid if index <= origin]
            evaluation_start = origin + 1 + profile.embargo_observations
            evaluation_end = min(
                indexes[-1],
                evaluation_start
                + max(
                    profile.minimum_evaluation_observations,
                    max(profile.horizons),
                )
                - 1,
            )
            evaluation_valid = [
                index
                for index in valid
                if evaluation_start <= index <= evaluation_end
            ]
            if len(evaluation_valid) < profile.minimum_evaluation_observations:
                continue
            for horizon in profile.horizons:
                target_index = origin + profile.embargo_observations + horizon
                if target_index > indexes[-1]:
                    continue
                target = rows[target_index]
                if (
                    profile.expected_closure_policy == "omit"
                    and target.get("cm_input_expected_closure") is True
                ):
                    continue
                status = (
                    "valid"
                    if target.get("cm_input_transform_valid") is True
                    else "skipped"
                )
                reason = "" if status == "valid" else "target_unavailable"
                folds.append(
                    {
                        "schema_version": CLASSICAL_MODEL_FOLD_SCHEMA_VERSION,
                        "fold_id": fold_id,
                        "series_id": series_id,
                        "period": period,
                        "kind": profile.fold_kind,
                        "kind_code": MODEL_FOLD_KIND_CODES[profile.fold_kind],
                        "status": status,
                        "reason": reason or None,
                        "shuffle": False,
                        "future_values_visible": False,
                        "timestamp_required_as_identity": False,
                        "embargo_observations": profile.embargo_observations,
                        "horizon": horizon,
                        "training_observation_count": len(train_valid),
                        "training_start_index": train_valid[0],
                        "training_end_index": train_valid[-1],
                        "evaluation_start_index": evaluation_start,
                        "evaluation_end_index": evaluation_end,
                        "target_index": target_index,
                        "origin_bin_end_utc_ms": rows[origin][
                            "cm_input_bin_end_utc_ms"
                        ],
                        "target_bin_end_utc_ms": target[
                            "cm_input_bin_end_utc_ms"
                        ],
                        "origin_row_id": rows[origin].get(
                            "cm_input_source_last_row_id"
                        ),
                        "target_row_id": target.get(
                            "cm_input_source_last_row_id"
                        ),
                        "training_start_row_id": rows[train_valid[0]].get(
                            "cm_input_source_first_row_id"
                        ),
                        "training_end_row_id": rows[train_valid[-1]].get(
                            "cm_input_source_last_row_id"
                        ),
                        "evaluation_start_row_id": rows[evaluation_start].get(
                            "cm_input_source_first_row_id"
                        ),
                        "evaluation_end_row_id": rows[evaluation_end].get(
                            "cm_input_source_last_row_id"
                        ),
                    }
                )
                fold_id += 1
                if len(folds) >= profile.resources.max_folds:
                    return folds
    return folds


def _annotate_primary_folds(
    frame: Any, folds: Sequence[Mapping[str, JSONValue]]
) -> Any:
    import polars as pl

    if getattr(frame, "height", 0) < 1:
        return frame
    rows = cast(list[dict[str, Any]], frame.to_dicts())
    primary: dict[tuple[str, str, int], Mapping[str, JSONValue]] = {}
    for fold in folds:
        key = (
            _text(fold.get("series_id")),
            _text(fold.get("period")),
            _int(fold.get("target_bin_end_utc_ms")),
        )
        primary.setdefault(key, fold)
    for row in rows:
        group = (str(row["series_id"]), str(row["period"]))
        fold = primary.get(
            (
                group[0],
                group[1],
                int(row["cm_input_bin_end_utc_ms"]),
            ),
            {},
        )
        row.update(_fold_projection_values(fold, row))
    return pl.DataFrame(rows, infer_schema_length=None)


def _fold_projection_values(
    fold: Mapping[str, JSONValue], row: Mapping[str, Any]
) -> dict[str, Any]:
    available = bool(fold) and fold.get("status") == "valid"
    return {
        "cm_fold_id": fold.get("fold_id"),
        "cm_fold_kind_code": fold.get("kind_code"),
        "cm_fold_origin_row_id": fold.get("origin_row_id"),
        "cm_fold_target_row_id": fold.get("target_row_id"),
        "cm_fold_horizon": fold.get("horizon"),
        "cm_fold_training_start_row_id": fold.get("training_start_row_id"),
        "cm_fold_training_end_row_id": fold.get("training_end_row_id"),
        "cm_fold_evaluation_start_row_id": fold.get("evaluation_start_row_id"),
        "cm_fold_evaluation_end_row_id": fold.get("evaluation_end_row_id"),
        "cm_evaluation_status_code": (
            MODEL_EVALUATION_STATUS_CODES["contract_ready"]
            if available
            else MODEL_EVALUATION_STATUS_CODES["unavailable"]
        ),
        "cm_evaluation_target_available": available,
        "cm_evaluation_forecast": None,
        "cm_evaluation_actual": (
            row.get("cm_input_value") if available else None
        ),
        "cm_evaluation_error": None,
        "cm_evaluation_diagnostic_only": available,
    }


def _projection_frame(frame: Any, contract: Mapping[str, JSONValue]) -> Any:
    import polars as pl

    status = _text(contract.get("status"))
    reason = _text(contract.get("reason"))
    derivation_id = _text(contract.get("derivation_id"))
    configuration = _mapping(contract.get("configuration"))
    selected = frame.with_columns(
        [
            pl.lit(CLASSICAL_MODEL_INPUT_SCHEMA_VERSION).alias(
                "cm_input_schema_version"
            ),
            pl.lit(derivation_id).alias("cm_input_derivation_id"),
            pl.lit(MODEL_INPUT_STATUS_CODES.get(status, 0))
            .cast(pl.Int32)
            .alias("cm_input_status_code"),
            pl.lit(status == "ready").alias("cm_input_ready"),
            pl.lit(MODEL_INPUT_REASON_CODES.get(reason, 99 if reason else 0))
            .cast(pl.Int32)
            .alias("cm_input_exclusion_reason_code"),
            pl.lit(_int(configuration.get("frequency_ms")))
            .cast(pl.Int64)
            .alias("cm_input_frequency_ms"),
            pl.lit(
                MODEL_TRANSFORM_CODES.get(
                    _text(configuration.get("transform")), 0
                )
            )
            .cast(pl.Int32)
            .alias("cm_input_transform_code"),
            pl.lit(1).cast(pl.Int32).alias("cm_input_calculation_basis_code"),
            pl.lit(CLASSICAL_MODEL_FOLD_SCHEMA_VERSION).alias(
                "cm_fold_schema_version"
            ),
            pl.lit(CLASSICAL_MODEL_EVALUATION_RESULT_SCHEMA_VERSION).alias(
                "cm_evaluation_schema_version"
            ),
            pl.col("cm_input_transform_valid").alias("cm_input_available"),
        ]
    )
    keep = [
        "series_id",
        "period",
        "cm_input_available_at_utc_ms",
        *[
            column
            for column in CLASSICAL_MODEL_CONTRACT_COLUMNS
            if column in selected.columns
        ],
    ]
    return selected.select(list(dict.fromkeys(keep)))


def _with_empty_projection(frame: Any) -> Any:
    import polars as pl

    expressions = []
    for name, dtype in _projection_dtypes().items():
        if name not in getattr(frame, "columns", ()):
            expressions.append(pl.lit(None).cast(dtype).alias(name))
    return frame.with_columns(expressions) if expressions else frame


def _ensure_projection_dtypes(frame: Any) -> Any:
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
        "cm_input_schema_version",
        "cm_input_derivation_id",
        "cm_fold_schema_version",
        "cm_evaluation_schema_version",
    }
    booleans = {
        "cm_input_ready",
        "cm_input_available",
        "cm_input_expected_closure",
        "cm_input_unexpected_missing",
        "cm_evaluation_target_available",
        "cm_evaluation_diagnostic_only",
    }
    floats = {
        "cm_input_observed_value",
        "cm_input_value",
        "cm_input_spread",
        "cm_evaluation_forecast",
        "cm_evaluation_actual",
        "cm_evaluation_error",
    }
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
        for name in CLASSICAL_MODEL_CONTRACT_COLUMNS
    }


def _fold_policy(
    profile: ClassicalModelInputProfile,
    folds: Sequence[Mapping[str, JSONValue]],
) -> dict[str, JSONValue]:
    statuses = Counter(_text(fold.get("status")) for fold in folds)
    retained = folds[: profile.resources.max_retained_diagnostics]
    return {
        "schema_version": CLASSICAL_MODEL_FOLD_SCHEMA_VERSION,
        "kind": profile.fold_kind,
        "shuffle": False,
        "future_values_visible": False,
        "timestamp_required_as_identity": False,
        "minimum_training_observations": (
            profile.minimum_training_observations
        ),
        "minimum_evaluation_observations": (
            profile.minimum_evaluation_observations
        ),
        "step_size": profile.step_size,
        "rolling_window": profile.rolling_window,
        "embargo_observations": profile.embargo_observations,
        "horizons": list(profile.horizons),
        "horizon_unit": "regularized_grid_steps",
        "incomplete_horizon_policy": "omit_unavailable_targets",
        "fold_count": len(folds),
        "valid_fold_count": statuses["valid"],
        "skipped_fold_count": statuses["skipped"],
        "folds_truncated": len(retained) < len(folds),
        "fold_samples": [dict(fold) for fold in retained],
    }


def _training_projection_metadata(
    status: str,
    reason: str | None,
    profile: ClassicalModelInputProfile,
    folds: Sequence[Mapping[str, JSONValue]],
) -> dict[str, JSONValue]:
    return {
        "schema_version": CLASSICAL_MODEL_TRAINING_PROJECTION_SCHEMA_VERSION,
        "grain": "row",
        "identity_fields": ["series_id", "period", "row_id"],
        "timestamp_required_as_identity": False,
        "mapping_policy": "availability_safe_repetition_after_bin_close",
        "column_names": list(CLASSICAL_MODEL_CONTRACT_COLUMNS),
        "status_code": MODEL_INPUT_STATUS_CODES.get(status, 0),
        "ready": status == "ready",
        "reason_code": MODEL_INPUT_REASON_CODES.get(
            reason or "", 99 if reason else 0
        ),
        "frequency_ms": profile.frequency_ms,
        "transform_code": MODEL_TRANSFORM_CODES[profile.transform],
        "fold_count": len(folds),
        "forecast_columns_populated": False,
        "observed_columns_overwritten": False,
    }


def _empty_regularization() -> dict[str, JSONValue]:
    return {
        "schema_version": CLASSICAL_MODEL_INPUT_SCHEMA_VERSION,
        "basis": "regular_grid",
        "regularized_observation_count": 0,
        "observed_bin_count": 0,
        "expected_closure_count": 0,
        "unexpected_missing_count": 0,
        "insufficient_observation_bin_count": 0,
        "structurally_unavailable_count": 0,
        "truncated": False,
        "forward_fill_policy": "never",
    }


def _empty_transform_policy(
    base: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    return _empty_transform_policy_from_profile(_profile_from_base(base))


def _empty_transform_policy_from_profile(
    profile: ClassicalModelInputProfile,
) -> dict[str, JSONValue]:
    inverse = {
        "level": "identity",
        "log_level": "exp",
        "return": "compound_from_last_level",
        "log_return": "exp_and_compound_from_last_level",
    }[profile.transform]
    return {
        "transform": profile.transform,
        "transform_code": MODEL_TRANSFORM_CODES[profile.transform],
        "differencing_order": profile.differencing_order,
        "seasonal_differencing_order": (profile.seasonal_differencing_order),
        "seasonal_period": profile.seasonal_period,
        "inverse_transform": inverse,
        "original_scale_metrics_required": True,
        "applied_explicitly": True,
        "invalid_domain_count": 0,
        "warmup_loss": 0,
        "transformed_observation_count": 0,
        "cross_period_state": False,
    }


def _empty_fold_policy(base: Mapping[str, JSONValue]) -> dict[str, JSONValue]:
    return _fold_policy(_profile_from_base(base), ())


def _profile_from_base(
    base: Mapping[str, JSONValue],
) -> ClassicalModelInputProfile:
    configuration = _mapping(base.get("configuration"))
    return ClassicalModelInputProfile(
        enabled=bool(configuration.get("enabled", False)),
        frequency_ms=max(
            1,
            _int(configuration.get("frequency_ms"))
            or DEFAULT_MODEL_FREQUENCY_MS,
        ),
        transform=_text(configuration.get("transform")) or "level",
        horizons=tuple(_ints(configuration.get("horizons")))
        or DEFAULT_MODEL_HORIZONS,
    )


def _derivation_id(
    frame: Any,
    profile: ClassicalModelInputProfile,
    fingerprint: Mapping[str, JSONValue],
) -> str:
    samples: list[dict[str, JSONValue]] = []
    if getattr(frame, "height", 0):
        for row in (
            frame.select(
                "series_id",
                "period",
                "cm_input_bin_start_utc_ms",
                "cm_input_observation_count",
                "cm_input_value",
            )
            .head(32)
            .to_dicts()
        ):
            samples.append(cast(dict[str, JSONValue], row))
    payload = {
        "schema_version": CLASSICAL_MODEL_INPUT_SCHEMA_VERSION,
        "fingerprint_id": _reference_fingerprint_id(fingerprint),
        "configuration": profile.to_metadata(),
        "row_count": int(getattr(frame, "height", 0) or 0),
        "samples": samples,
    }
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _reference_fingerprint_id(
    fingerprint: Mapping[str, JSONValue],
) -> str:
    provided = _text(fingerprint.get("fingerprint_id"))
    if provided:
        return provided
    snapshot = {
        key: value
        for key, value in fingerprint.items()
        if key not in {"classical_model_input", "fingerprint_id"}
    }
    encoded = json.dumps(
        snapshot, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _valid_observation_count(frame: Any) -> int:
    if "cm_input_transform_valid" not in getattr(frame, "columns", ()):
        return 0
    return int(frame.get_column("cm_input_transform_valid").sum() or 0)


def _bin_status_name(code: int) -> str:
    for name, value in MODEL_BIN_STATUS_CODES.items():
        if value == code:
            return name
    return "unknown"


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


def _ints(value: object) -> list[int]:
    if not isinstance(value, list):
        return []
    return [_int(item) for item in value if _int(item) > 0]


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(cast(Any, value))
    except (TypeError, ValueError):
        return None


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(cast(Any, value))
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _rounded(value: float | None, digits: int) -> float | None:
    return round(value, digits) if value is not None else None
