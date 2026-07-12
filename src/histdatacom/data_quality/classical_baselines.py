"""Optional deterministic classical baselines over enriched ASCII tick rows."""

from __future__ import annotations

import hashlib
import json
import math
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from statistics import median
from typing import Any, TypedDict, cast

from histdatacom.data_quality.contracts import QualityFinding
from histdatacom.data_quality.limits import bounded_report_limit
from histdatacom.data_quality.training_features import (
    TRAINING_SCHEMA_VERSION,
    ensure_tick_training_features,
)
from histdatacom.runtime_contracts import JSONValue

CLASSICAL_BASELINE_SCHEMA_VERSION = "histdatacom.classical-baselines.v1"
CLASSICAL_BASELINE_TRAINING_PROJECTION_SCHEMA_VERSION = (
    "histdatacom.classical-baseline-training-projection.v1"
)
CLASSICAL_BASELINE_SUMMARY_SCHEMA_VERSION = (
    "histdatacom.classical-baseline-summary.v1"
)
CLASSICAL_BASELINE_SUMMARY_METADATA_KEY = (
    "time_series_fingerprint_classical_baseline_summary"
)
CLASSICAL_BASELINE_BOUNDED_PAYLOAD_KEY = "fingerprint_classical_baselines"

DEFAULT_BASELINE_EVALUATION_FRACTION = 0.2
DEFAULT_BASELINE_MINIMUM_TRAINING_ROWS = 20
DEFAULT_BASELINE_MINIMUM_EVALUATION_ROWS = 5
DEFAULT_BASELINE_ROLLING_WINDOWS = (5, 20)
DEFAULT_BASELINE_ROUNDING_DIGITS = 12
DEFAULT_BASELINE_SUMMARY_TARGET_LIMIT = 16
MAX_BASELINE_ROLLING_WINDOWS = 16

BASELINE_REQUIRED_IDENTITY_COLUMNS = ("series_id", "period", "row_id")
BASELINE_REQUIRED_INPUT_COLUMNS = (
    *BASELINE_REQUIRED_IDENTITY_COLUMNS,
    "mid",
    "training_usable",
)
BASELINE_DEFERRED_MODEL_FAMILIES = (
    "ets",
    "arima",
    "sarima",
    "state_space",
    "garch",
)
BASELINE_MODEL_CODES = {
    "naive_random_walk": 1,
    "rolling_mean": 2,
    "rolling_median": 3,
    "session_seasonal_naive": 4,
}
BASELINE_STATUS_CODES = {"unavailable": 1, "limited": 2, "ready": 3}
STATIONARITY_STATUS_CODES = {
    "unavailable": 1,
    "limited": 2,
    "valid": 3,
}
BASELINE_REASON_CODES = {
    "": 0,
    "training_frame_unavailable": 1,
    "missing_required_columns": 2,
    "insufficient_training_rows": 3,
    "insufficient_evaluation_rows": 4,
    "no_usable_mid_values": 5,
    "stationarity_unavailable": 6,
    "stationarity_limited": 7,
    "model_evaluation_unavailable": 8,
}
TRANSFORM_ADVISORY_BITS = {
    "log_return": 1,
    "differencing": 2,
    "session_conditioning": 4,
}


class _BaselineRow(TypedDict):
    series_id: str
    period: str
    row_id: int
    mid: float
    session_state: int


@dataclass(frozen=True, slots=True)
class ClassicalBaselineProfile:
    """Opt-in controls for deterministic, advisory baseline evaluation."""

    enabled: bool = False
    evaluation_fraction: float = DEFAULT_BASELINE_EVALUATION_FRACTION
    minimum_training_rows: int = DEFAULT_BASELINE_MINIMUM_TRAINING_ROWS
    minimum_evaluation_rows: int = DEFAULT_BASELINE_MINIMUM_EVALUATION_ROWS
    rolling_windows: tuple[int, ...] = DEFAULT_BASELINE_ROLLING_WINDOWS
    session_seasonal_enabled: bool = True
    rounding_digits: int = DEFAULT_BASELINE_ROUNDING_DIGITS

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return stable JSON-compatible profile metadata."""
        return {
            "enabled": self.enabled,
            "evaluation_fraction": self.evaluation_fraction,
            "minimum_training_rows": self.minimum_training_rows,
            "minimum_evaluation_rows": self.minimum_evaluation_rows,
            "rolling_windows": list(self.rolling_windows),
            "session_seasonal_enabled": self.session_seasonal_enabled,
            "rounding_digits": self.rounding_digits,
        }


def classical_baseline_diagnostics_from_training_frame(
    frame: Any | None,
    fingerprint: Mapping[str, JSONValue],
    *,
    profile: ClassicalBaselineProfile | None = None,
    target: Any | None = None,
) -> dict[str, JSONValue]:
    """Evaluate deterministic classical baselines over enriched tick rows.

    Metrics use a chronological holdout in durable ``row_id`` order.  Actual
    prior values are available to later walk-forward predictions; no shuffle,
    fitted statistical model, or future value participates in a forecast.
    """
    selected = profile or ClassicalBaselineProfile(enabled=True)
    base = _base_payload(fingerprint, selected)
    if frame is None:
        return _unavailable_payload(base, "training_frame_unavailable")

    input_was_enriched = "training_schema_version" in getattr(
        frame, "columns", ()
    )
    try:
        enriched = ensure_tick_training_features(frame, target=target)
    except (AttributeError, TypeError, ValueError):
        return _unavailable_payload(base, "training_frame_unavailable")

    columns = set(getattr(enriched, "columns", ()))
    training_substrate = dict(_mapping(base.get("training_substrate")))
    training_substrate["legacy_cache_enriched_on_read"] = not input_was_enriched
    base["training_substrate"] = training_substrate
    missing = sorted(set(BASELINE_REQUIRED_INPUT_COLUMNS) - columns)
    if missing:
        missing_payload = _unavailable_payload(base, "missing_required_columns")
        missing_payload["training_substrate"] = {
            **cast(dict[str, JSONValue], missing_payload["training_substrate"]),
            "missing_required_columns": cast(JSONValue, missing),
        }
        return missing_payload

    rows = _usable_rows(enriched)
    if not rows:
        return _unavailable_payload(base, "no_usable_mid_values")

    split = _chronological_split(len(rows), selected)
    base["split_policy"] = split
    training_count = _int(split.get("training_row_count"))
    evaluation_count = _int(split.get("evaluation_row_count"))
    split["split_row_id"] = (
        rows[training_count]["row_id"] if evaluation_count else None
    )
    if training_count < selected.minimum_training_rows:
        return _unavailable_payload(
            base,
            "insufficient_training_rows",
            split_policy=split,
        )
    if evaluation_count < selected.minimum_evaluation_rows:
        return _unavailable_payload(
            base,
            "insufficient_evaluation_rows",
            split_policy=split,
        )

    values = [row["mid"] for row in rows]
    session_states = [row["session_state"] for row in rows]
    models = _evaluate_models(
        values,
        session_states,
        split_index=training_count,
        profile=selected,
        fingerprint=fingerprint,
    )
    evaluated = [
        model for model in models if model.get("status") == "evaluated"
    ]
    if not evaluated:
        return _unavailable_payload(
            base,
            "model_evaluation_unavailable",
            split_policy=split,
            models=models,
        )

    best = min(
        evaluated,
        key=lambda model: (
            _float(model.get("mae"), default=math.inf),
            _int(model.get("model_code")),
            _int(model.get("window")),
        ),
    )
    prerequisite = _prerequisite_readiness(fingerprint)
    guard_codes = _evaluation_guard_codes(fingerprint, prerequisite)
    limitations = list(guard_codes)
    if any(
        model.get("status") != "evaluated"
        and model.get("model") != "session_seasonal_naive"
        for model in models
    ):
        limitations.append("some_models_skipped")
    status = "ready" if not limitations else "limited"
    reason = limitations[0] if limitations else None
    payload: dict[str, JSONValue] = {
        **base,
        "status": status,
        "reason": reason,
        "prerequisite_readiness": prerequisite,
        "split_policy": split,
        "evaluation": {
            "status": "evaluated",
            "metric": "mid",
            "calculation_basis": "observed_sequence_walk_forward",
            "transforms_applied": [],
            "recommended_transforms": _recommended_transforms(fingerprint),
            "guard_codes": guard_codes,
            "model_count": len(models),
            "evaluated_model_count": len(evaluated),
            "skipped_model_count": len(models) - len(evaluated),
            "models": cast(JSONValue, models),
            "best_model": dict(best),
        },
        "limitations": limitations,
    }
    payload["training_projection"] = classical_baseline_training_projection(
        payload
    )
    payload["baseline_id"] = _baseline_id(payload)
    return payload


def classical_baseline_training_projection(
    diagnostics: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    """Return flat period-grain baseline scalars for enriched rows."""
    status = _text(diagnostics.get("status"))
    reason = _text(diagnostics.get("reason"))
    split = _mapping(diagnostics.get("split_policy"))
    prerequisite = _mapping(diagnostics.get("prerequisite_readiness"))
    evaluation = _mapping(diagnostics.get("evaluation"))
    best = _mapping(evaluation.get("best_model"))
    transforms = _strings(evaluation.get("recommended_transforms"))
    return {
        "schema_version": CLASSICAL_BASELINE_TRAINING_PROJECTION_SCHEMA_VERSION,
        "grain": "period",
        "identity_fields": list(BASELINE_REQUIRED_IDENTITY_COLUMNS),
        "timestamp_required": False,
        "values": {
            "baseline_status_code": BASELINE_STATUS_CODES.get(status, 0),
            "baseline_training_ready": status in {"limited", "ready"},
            "baseline_exclusion_reason_code": BASELINE_REASON_CODES.get(
                reason, 99 if reason else 0
            ),
            "baseline_training_row_count": _int(
                split.get("training_row_count")
            ),
            "baseline_evaluation_row_count": _int(
                split.get("evaluation_row_count")
            ),
            "baseline_split_row_id": split.get("split_row_id"),
            "baseline_best_model_code": _int(best.get("model_code")),
            "baseline_best_model_window": best.get("window"),
            "baseline_best_mae": best.get("mae"),
            "baseline_best_rmse": best.get("rmse"),
            "baseline_stationarity_status_code": STATIONARITY_STATUS_CODES.get(
                _text(prerequisite.get("stationarity_status")), 0
            ),
            "baseline_transform_advisory_code": _transform_advisory_code(
                transforms
            ),
        },
    }


def project_classical_baseline_onto_training_frame(
    frame: Any,
    diagnostics: Mapping[str, JSONValue],
) -> Any:
    """Project period baseline scalars without using or joining on timestamp."""
    missing = sorted(
        set(BASELINE_REQUIRED_IDENTITY_COLUMNS)
        - set(getattr(frame, "columns", ()))
    )
    if missing:
        raise ValueError(
            "classical baseline projection requires enriched ASCII tick "
            f"identity columns: {', '.join(missing)}"
        )
    import polars as pl

    projection = classical_baseline_training_projection(diagnostics)
    values = _mapping(projection.get("values"))
    integer_columns = {
        "baseline_status_code",
        "baseline_exclusion_reason_code",
        "baseline_training_row_count",
        "baseline_evaluation_row_count",
        "baseline_split_row_id",
        "baseline_best_model_code",
        "baseline_best_model_window",
        "baseline_stationarity_status_code",
        "baseline_transform_advisory_code",
    }
    boolean_columns = {"baseline_training_ready"}
    expressions = []
    for name, value in values.items():
        expression = pl.lit(value)
        if name in integer_columns:
            expression = expression.cast(pl.Int64)
        elif name in boolean_columns:
            expression = expression.cast(pl.Boolean)
        else:
            expression = expression.cast(pl.Float64)
        expressions.append(expression.alias(name))
    return frame.with_columns(expressions)


def classical_baseline_summary(
    findings: Iterable[QualityFinding],
    *,
    target_limit: int | None = DEFAULT_BASELINE_SUMMARY_TARGET_LIMIT,
) -> dict[str, JSONValue] | None:
    """Return a bounded run summary for opt-in baseline diagnostics."""
    targets: list[dict[str, JSONValue]] = []
    status_counts: Counter[str] = Counter()
    best_model_counts: Counter[str] = Counter()
    for finding in findings:
        fingerprint = _mapping(finding.metadata.get("time_series_fingerprint"))
        diagnostics = _mapping(fingerprint.get("classical_baselines"))
        if not diagnostics:
            continue
        status = _text(diagnostics.get("status")) or "unavailable"
        evaluation = _mapping(diagnostics.get("evaluation"))
        best = _mapping(evaluation.get("best_model"))
        model = _text(best.get("model"))
        status_counts[status] += 1
        if model:
            best_model_counts[model] += 1
        split = _mapping(diagnostics.get("split_policy"))
        targets.append(
            {
                "target_axis": dict(_mapping(diagnostics.get("target_axis"))),
                "status": status,
                "reason": diagnostics.get("reason"),
                "training_row_count": _int(split.get("training_row_count")),
                "evaluation_row_count": _int(split.get("evaluation_row_count")),
                "best_model": model or None,
                "best_model_code": _int(best.get("model_code")),
                "best_mae": best.get("mae"),
                "guard_codes": cast(
                    JSONValue, _strings(evaluation.get("guard_codes"))
                ),
            }
        )
    if not targets:
        return None
    targets.sort(key=_target_sort_key)
    limit = bounded_report_limit(
        target_limit,
        default_limit=DEFAULT_BASELINE_SUMMARY_TARGET_LIMIT,
        allow_unbounded=True,
    )
    included = limit.slice(targets)
    omitted = max(0, len(targets) - len(included))
    return {
        "schema_version": CLASSICAL_BASELINE_SUMMARY_SCHEMA_VERSION,
        "advisory": True,
        "target_count": len(targets),
        "included_target_count": len(included),
        "omitted_target_count": omitted,
        "truncated": omitted > 0,
        "status_counts": dict(sorted(status_counts.items())),
        "best_model_counts": dict(sorted(best_model_counts.items())),
        "target_summaries": cast(JSONValue, included),
        "limit_metadata": {"targets": limit.limit_payload()},
    }


def format_classical_baseline_summary_lines(
    summary: Mapping[str, JSONValue] | None,
) -> tuple[str, ...]:
    """Return concise console lines for baseline diagnostics."""
    if not summary:
        return ()
    status = _mapping(summary.get("status_counts"))
    lines = [
        "",
        "Classical fingerprint baselines",
        (
            "targets: "
            f"{_int(summary.get('target_count'))} "
            f"ready: {_int(status.get('ready'))} "
            f"limited: {_int(status.get('limited'))} "
            f"unavailable: {_int(status.get('unavailable'))}"
        ),
    ]
    for target in _mapping_rows(summary.get("target_summaries")):
        axis = _mapping(target.get("target_axis"))
        label = "/".join(
            _text(axis.get(key))
            for key in ("data_format", "timeframe", "symbol", "period")
        )
        best = _text(target.get("best_model")) or "none"
        lines.append(
            f"- {label}: {target.get('status', 'unavailable')} "
            f"train={_int(target.get('training_row_count'))} "
            f"eval={_int(target.get('evaluation_row_count'))} "
            f"best={best}"
        )
    if summary.get("truncated") is True:
        lines.append(
            f"- {_int(summary.get('omitted_target_count'))} targets omitted"
        )
    return tuple(lines)


def _base_payload(
    fingerprint: Mapping[str, JSONValue],
    profile: ClassicalBaselineProfile,
) -> dict[str, JSONValue]:
    return {
        "schema_version": CLASSICAL_BASELINE_SCHEMA_VERSION,
        "advisory": True,
        "base_grain": {"data_format": "ascii", "timeframe": "T"},
        "target_axis": dict(_mapping(fingerprint.get("target_axis"))),
        "reference_fingerprint_id": _reference_fingerprint_id(fingerprint),
        "configuration": profile.to_metadata(),
        "training_substrate": {
            "schema_version": TRAINING_SCHEMA_VERSION,
            "status": "available",
            "required_columns": list(BASELINE_REQUIRED_INPUT_COLUMNS),
            "identity_fields": list(BASELINE_REQUIRED_IDENTITY_COLUMNS),
            "ordering_fields": list(BASELINE_REQUIRED_IDENTITY_COLUMNS),
            "timestamp_required": False,
            "metric": "mid",
            "observed_bid_ask_preserved": True,
            "legacy_cache_enriched_on_read": False,
        },
        "split_policy": _empty_split_policy(profile),
        "prerequisite_readiness": _prerequisite_readiness(fingerprint),
        "evaluation": _empty_evaluation(fingerprint),
        "limitations": [],
        "deferred_model_families": list(BASELINE_DEFERRED_MODEL_FAMILIES),
        "non_goals": [
            "forecasting_leaderboard",
            "automatic_model_selection",
            "synthetic_generation",
            "hard_fail_quality_gate",
        ],
    }


def _unavailable_payload(
    base: Mapping[str, JSONValue],
    reason: str,
    *,
    split_policy: Mapping[str, JSONValue] | None = None,
    models: Sequence[Mapping[str, JSONValue]] = (),
) -> dict[str, JSONValue]:
    payload = dict(base)
    payload.update(
        {
            "status": "unavailable",
            "reason": reason,
            "limitations": [reason],
        }
    )
    if split_policy is not None:
        payload["split_policy"] = dict(split_policy)
    evaluation = dict(_mapping(payload.get("evaluation")))
    if models:
        evaluation.update(
            {
                "models": [dict(model) for model in models],
                "model_count": len(models),
                "evaluated_model_count": 0,
                "skipped_model_count": len(models),
            }
        )
    payload["evaluation"] = evaluation
    training = dict(_mapping(payload.get("training_substrate")))
    if reason in {"training_frame_unavailable", "missing_required_columns"}:
        training["status"] = "unavailable"
    payload["training_substrate"] = training
    payload["training_projection"] = classical_baseline_training_projection(
        payload
    )
    payload["baseline_id"] = _baseline_id(payload)
    return payload


def _usable_rows(frame: Any) -> list[_BaselineRow]:
    selected = [
        *BASELINE_REQUIRED_IDENTITY_COLUMNS,
        "mid",
        "training_usable",
    ]
    has_session = "class_session_state_code" in frame.columns
    if has_session:
        selected.append("class_session_state_code")
    rows: list[_BaselineRow] = []
    ordered = frame.select(selected).sort(
        list(BASELINE_REQUIRED_IDENTITY_COLUMNS)
    )
    for row in ordered.iter_rows(named=True):
        value = row.get("mid")
        if row.get("training_usable") is not True:
            continue
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            continue
        mid = float(value)
        if not math.isfinite(mid):
            continue
        rows.append(
            {
                "series_id": str(row.get("series_id") or ""),
                "period": str(row.get("period") or ""),
                "row_id": int(row.get("row_id") or 0),
                "mid": mid,
                "session_state": int(row.get("class_session_state_code") or 0),
            }
        )
    return rows


def _chronological_split(
    row_count: int,
    profile: ClassicalBaselineProfile,
) -> dict[str, JSONValue]:
    requested_evaluation = max(
        profile.minimum_evaluation_rows,
        int(math.ceil(row_count * profile.evaluation_fraction)),
    )
    evaluation_count = min(row_count, requested_evaluation)
    training_count = max(0, row_count - evaluation_count)
    return {
        "kind": "chronological_holdout",
        "order_by": list(BASELINE_REQUIRED_IDENTITY_COLUMNS),
        "timestamp_required": False,
        "shuffle": False,
        "walk_forward": True,
        "future_values_visible": False,
        "evaluation_fraction": profile.evaluation_fraction,
        "row_count": row_count,
        "training_row_count": training_count,
        "evaluation_row_count": evaluation_count,
        "split_index": training_count,
        "split_row_id": training_count + 1 if evaluation_count else None,
        "metrics_emitted": (
            training_count >= profile.minimum_training_rows
            and evaluation_count >= profile.minimum_evaluation_rows
        ),
    }


def _empty_split_policy(
    profile: ClassicalBaselineProfile,
) -> dict[str, JSONValue]:
    return {
        "kind": "chronological_holdout",
        "order_by": list(BASELINE_REQUIRED_IDENTITY_COLUMNS),
        "timestamp_required": False,
        "shuffle": False,
        "walk_forward": True,
        "future_values_visible": False,
        "evaluation_fraction": profile.evaluation_fraction,
        "row_count": 0,
        "training_row_count": 0,
        "evaluation_row_count": 0,
        "split_index": 0,
        "split_row_id": None,
        "metrics_emitted": False,
    }


def _evaluate_models(
    values: Sequence[float],
    session_states: Sequence[int],
    *,
    split_index: int,
    profile: ClassicalBaselineProfile,
    fingerprint: Mapping[str, JSONValue],
) -> list[dict[str, JSONValue]]:
    models = [
        _evaluate_prediction_series(
            "naive_random_walk",
            [values[index - 1] for index in range(split_index, len(values))],
            values[split_index:],
            profile=profile,
        )
    ]
    for window in profile.rolling_windows[:MAX_BASELINE_ROLLING_WINDOWS]:
        mean_predictions = [
            (
                _mean(values[max(0, index - window) : index])
                if index >= window
                else None
            )
            for index in range(split_index, len(values))
        ]
        median_predictions = [
            median(values[index - window : index]) if index >= window else None
            for index in range(split_index, len(values))
        ]
        models.extend(
            (
                _evaluate_prediction_series(
                    "rolling_mean",
                    mean_predictions,
                    values[split_index:],
                    profile=profile,
                    window=window,
                ),
                _evaluate_prediction_series(
                    "rolling_median",
                    median_predictions,
                    values[split_index:],
                    profile=profile,
                    window=window,
                ),
            )
        )
    if profile.session_seasonal_enabled:
        if _session_seasonal_eligible(session_states, fingerprint):
            predictions = _session_seasonal_predictions(
                values,
                session_states,
                split_index=split_index,
            )
            models.append(
                _evaluate_prediction_series(
                    "session_seasonal_naive",
                    predictions,
                    values[split_index:],
                    profile=profile,
                )
            )
        else:
            models.append(
                {
                    "model": "session_seasonal_naive",
                    "model_code": BASELINE_MODEL_CODES[
                        "session_seasonal_naive"
                    ],
                    "status": "skipped",
                    "reason": "session_topology_unavailable",
                    "forecast_count": 0,
                    "evaluation_row_count": len(values) - split_index,
                    "coverage_rate": 0.0,
                    "mae": None,
                    "rmse": None,
                    "mean_error": None,
                }
            )
    return models


def _evaluate_prediction_series(
    model: str,
    predictions: Sequence[float | None],
    actuals: Sequence[float],
    *,
    profile: ClassicalBaselineProfile,
    window: int | None = None,
) -> dict[str, JSONValue]:
    pairs = [
        (float(prediction), float(actual))
        for prediction, actual in zip(predictions, actuals, strict=True)
        if prediction is not None and math.isfinite(float(prediction))
    ]
    errors = [prediction - actual for prediction, actual in pairs]
    result: dict[str, JSONValue] = {
        "model": model,
        "model_code": BASELINE_MODEL_CODES[model],
        "status": "evaluated" if errors else "skipped",
        "reason": None if errors else "insufficient_model_history",
        "forecast_count": len(errors),
        "evaluation_row_count": len(actuals),
        "coverage_rate": _rounded(
            len(errors) / len(actuals) if actuals else 0.0,
            profile.rounding_digits,
        ),
        "mae": (
            _rounded(
                _mean([abs(error) for error in errors]),
                profile.rounding_digits,
            )
            if errors
            else None
        ),
        "rmse": (
            _rounded(
                math.sqrt(_mean([error * error for error in errors])),
                profile.rounding_digits,
            )
            if errors
            else None
        ),
        "mean_error": (
            _rounded(_mean(errors), profile.rounding_digits) if errors else None
        ),
    }
    if window is not None:
        result["window"] = window
    return result


def _session_seasonal_eligible(
    session_states: Sequence[int],
    fingerprint: Mapping[str, JSONValue],
) -> bool:
    calendar = _mapping(fingerprint.get("calendar_regimes"))
    calendar_status = _text(calendar.get("status"))
    return calendar_status in {"ok", "limited"} and len(set(session_states)) > 1


def _session_seasonal_predictions(
    values: Sequence[float],
    session_states: Sequence[int],
    *,
    split_index: int,
) -> list[float | None]:
    last_by_session: dict[int, float] = {}
    predictions: list[float | None] = []
    for index, (value, session) in enumerate(
        zip(values, session_states, strict=True)
    ):
        if index >= split_index:
            predictions.append(last_by_session.get(session))
        last_by_session[session] = value
    return predictions


def _prerequisite_readiness(
    fingerprint: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    audit = _mapping(fingerprint.get("fingerprint_audit"))
    statuses = _mapping(audit.get("section_statuses"))
    required = (
        "coverage",
        "temporal_topology",
        "calendar_regimes",
        "tick_distribution",
        "microstructure_dynamics",
        "dependence",
        "stationarity_diagnostics",
        "decomposition",
    )
    missing = [section for section in required if section not in statuses]
    limited = [
        section
        for section in required
        if _text(statuses.get(section)) in {"limited", "skipped"}
    ]
    unavailable = [
        section
        for section in required
        if _text(statuses.get(section)) == "unavailable"
    ]
    stationarity = _mapping(fingerprint.get("stationarity_diagnostics"))
    raw_status = _text(stationarity.get("stationarity_status"))
    stationarity_status = {
        "ok": "valid",
        "limited": "limited",
        "unavailable": "unavailable",
    }.get(raw_status, "unavailable")
    if missing or unavailable or stationarity_status == "unavailable":
        status = "unavailable"
    elif limited or stationarity_status == "limited":
        status = "limited"
    else:
        status = "valid"
    return {
        "status": status,
        "required_sections": cast(JSONValue, list(required)),
        "missing_sections": cast(JSONValue, missing),
        "limited_sections": cast(JSONValue, limited),
        "unavailable_sections": cast(JSONValue, unavailable),
        "stationarity_status": stationarity_status,
        "stationarity_reason": stationarity.get("reason"),
        "rolling_drift_status": (
            "available"
            if _mapping(stationarity.get("rolling_windows"))
            else "unavailable"
        ),
        "distribution_shift_status": _text(
            _mapping(
                stationarity.get("first_middle_last_distribution_shift")
            ).get("status")
        )
        or "unavailable",
        "computed_window_count": _int(
            stationarity.get("computed_window_count")
        ),
        "skipped_window_count": _int(stationarity.get("skipped_window_count")),
        "zero_variance_metrics": cast(
            JSONValue,
            _strings(stationarity.get("zero_variance_metrics")),
        ),
        "recommended_transforms": _recommended_transforms(fingerprint),
    }


def _evaluation_guard_codes(
    fingerprint: Mapping[str, JSONValue],
    prerequisite: Mapping[str, JSONValue],
) -> list[JSONValue]:
    guards: list[str] = []
    status = _text(prerequisite.get("stationarity_status"))
    if status == "unavailable":
        guards.append("stationarity_unavailable")
    elif status == "limited":
        guards.append("stationarity_limited")
    if _int(prerequisite.get("skipped_window_count")) > 0:
        guards.append("skipped_rolling_windows")
    if _strings(prerequisite.get("zero_variance_metrics")):
        guards.append("zero_variance")
    distribution = _mapping(
        _mapping(fingerprint.get("stationarity_diagnostics")).get(
            "first_middle_last_distribution_shift"
        )
    )
    if _text(distribution.get("status")) != "computed":
        guards.append("distribution_shift_unavailable")
    for transform in _recommended_transforms(fingerprint):
        guards.append(f"transform_recommended:{transform}")
    return list(dict.fromkeys(guards))


def _recommended_transforms(
    fingerprint: Mapping[str, JSONValue],
) -> list[JSONValue]:
    stationarity = _mapping(fingerprint.get("stationarity_diagnostics"))
    return cast(
        list[JSONValue],
        _strings(stationarity.get("recommended_transforms")),
    )


def _empty_evaluation(
    fingerprint: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    return {
        "status": "not_evaluated",
        "metric": "mid",
        "calculation_basis": "observed_sequence_walk_forward",
        "transforms_applied": [],
        "recommended_transforms": _recommended_transforms(fingerprint),
        "guard_codes": [],
        "model_count": 0,
        "evaluated_model_count": 0,
        "skipped_model_count": 0,
        "models": [],
        "best_model": {},
    }


def _baseline_id(payload: Mapping[str, JSONValue]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _reference_fingerprint_id(
    fingerprint: Mapping[str, JSONValue],
) -> str:
    existing = _text(fingerprint.get("fingerprint_id"))
    if existing:
        return existing
    basis = {
        key: value
        for key, value in fingerprint.items()
        if key not in {"classical_baselines", "fingerprint_id"}
    }
    encoded = json.dumps(basis, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _transform_advisory_code(transforms: Sequence[str]) -> int:
    return sum(TRANSFORM_ADVISORY_BITS.get(value, 0) for value in transforms)


def _target_sort_key(item: Mapping[str, JSONValue]) -> tuple[str, ...]:
    axis = _mapping(item.get("target_axis"))
    return tuple(
        _text(axis.get(key))
        for key in ("data_format", "timeframe", "symbol", "period", "kind")
    )


def _mapping(value: Any) -> Mapping[str, JSONValue]:
    return value if isinstance(value, Mapping) else {}


def _mapping_rows(value: Any) -> list[Mapping[str, JSONValue]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _strings(value: Any) -> list[str]:
    if not isinstance(value, (list, tuple)):
        return []
    return [str(item) for item in value if item not in (None, "")]


def _text(value: Any) -> str:
    return str(value or "")


def _int(value: Any) -> int:
    if isinstance(value, bool) or value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _float(value: Any, *, default: float = 0.0) -> float:
    if isinstance(value, bool) or value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _mean(values: Sequence[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _rounded(value: float, digits: int) -> float:
    return round(float(value), digits)
