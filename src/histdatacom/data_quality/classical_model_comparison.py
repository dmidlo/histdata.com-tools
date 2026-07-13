"""Family-neutral comparison of saved classical-model evaluation artifacts.

The comparison layer never fits a model.  It normalizes the bounded evaluation
artifacts emitted by the classical baseline, exponential-smoothing,
autoregressive, seasonal/exogenous, state-space, and volatility families.  A
comparison is eligible only when its target, scale, regularization, transform,
missingness, fold, period, and horizon identities agree with an explicitly
configured reference baseline.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from statistics import median
from typing import Any, cast

from histdatacom.data_quality.contracts import QualityFinding
from histdatacom.data_quality.limits import bounded_report_limit
from histdatacom.data_quality.training_features import (
    CLASSICAL_MODEL_COMPARISON_COLUMNS,
    ensure_tick_training_features,
)
from histdatacom.runtime_contracts import JSONValue

CLASSICAL_MODEL_COMPARISON_SCHEMA_VERSION = (
    "histdatacom.classical-model-comparison.v1"
)
CLASSICAL_MODEL_COMPARISON_ELIGIBILITY_SCHEMA_VERSION = (
    "histdatacom.classical-model-comparison-eligibility.v1"
)
CLASSICAL_MODEL_SKILL_SCHEMA_VERSION = "histdatacom.classical-model-skill.v1"
CLASSICAL_MODEL_STABILITY_SCHEMA_VERSION = (
    "histdatacom.classical-model-stability.v1"
)
CLASSICAL_MODEL_FIT_ACCOUNTING_SCHEMA_VERSION = (
    "histdatacom.classical-model-fit-accounting.v1"
)
CLASSICAL_MODEL_COMPARISON_TRAINING_PROJECTION_SCHEMA_VERSION = (
    "histdatacom.classical-model-comparison-training-projection.v1"
)
CLASSICAL_MODEL_COMPARISON_SUMMARY_SCHEMA_VERSION = (
    "histdatacom.classical-model-comparison-summary.v1"
)
CLASSICAL_MODEL_COMPARISON_SUMMARY_METADATA_KEY = (
    "time_series_fingerprint_classical_model_comparison_summary"
)
CLASSICAL_MODEL_COMPARISON_BOUNDED_PAYLOAD_KEY = (
    "fingerprint_classical_model_comparison"
)

DEFAULT_CLASSICAL_MODEL_COMPARISON_SUMMARY_TARGET_LIMIT = 25
DEFAULT_CLASSICAL_MODEL_COMPARISON_MAX_MODELS = 64
DEFAULT_CLASSICAL_MODEL_COMPARISON_MAX_HORIZONS = 16
DEFAULT_CLASSICAL_MODEL_COMPARISON_MAX_COMPARISONS = 256
DEFAULT_CLASSICAL_MODEL_COMPARISON_MAX_REASON_CODES = 24
DEFAULT_CLASSICAL_MODEL_COMPARISON_MAX_SAMPLES = 12
DEFAULT_CLASSICAL_MODEL_COMPARISON_ROUNDING_DIGITS = 12
MAX_CLASSICAL_MODEL_COMPARISON_MODELS = 256
MAX_CLASSICAL_MODEL_COMPARISON_HORIZONS = 64
MAX_CLASSICAL_MODEL_COMPARISONS = 2048
MAX_CLASSICAL_MODEL_COMPARISON_REASON_CODES = 128
MAX_CLASSICAL_MODEL_COMPARISON_SAMPLES = 64

COMPARISON_FAMILY_CODES = {
    "baseline": 1,
    "ses": 10,
    "holt": 11,
    "damped_trend": 12,
    "holt_winters": 13,
    "ets": 14,
    "ar": 20,
    "arma": 21,
    "arima": 22,
    "sarima": 30,
    "arimax": 31,
    "sarimax": 32,
    "local_level": 40,
    "local_linear_trend": 41,
    "structural": 42,
    "arch": 50,
    "garch": 51,
}
COMPARISON_TARGET_METRIC_CODES = {
    "mid_level": 1,
    "return_mean": 2,
    "conditional_variance": 3,
    "absolute_return_volatility": 4,
}
COMPARISON_SCALE_CODES = {
    "original_mid": 1,
    "unscaled_return": 2,
    "unscaled_return_squared": 3,
    "absolute_unscaled_return": 4,
}
COMPARISON_METRIC_CODES = {
    "mae": 1,
    "rmse": 2,
    "bias": 3,
    "mean_qlike": 4,
}
COMPARISON_STATUS_CODES = {
    "unavailable": 1,
    "limited": 2,
    "ready": 3,
}
COMPARISON_ELIGIBILITY_CODES = {
    "ineligible": 1,
    "eligible": 2,
    "reference": 3,
}
COMPARISON_REASON_CODES = {
    "none": 0,
    "no_model_results": 1,
    "reference_baseline_unavailable": 2,
    "reference_metric_unavailable": 3,
    "baseline_near_zero": 4,
    "frequency_mismatch": 5,
    "transform_mismatch": 6,
    "missingness_policy_mismatch": 7,
    "fold_set_mismatch": 8,
    "period_mismatch": 9,
    "horizon_mismatch": 10,
    "target_metric_mismatch": 11,
    "scale_mismatch": 12,
    "incomplete_fold_overlap": 13,
    "fold_evidence_truncated": 14,
    "raw_metric_unavailable": 15,
    "reference_record": 16,
    "regularization_contract_mismatch": 17,
}
COMPARISON_SKILL_STATUS_CODES = {
    "unavailable": 1,
    "reference": 2,
    "available": 3,
    "negative": 4,
}
COMPARISON_STABILITY_STATUS_CODES = {
    "unavailable": 1,
    "insufficient_folds": 2,
    "stable": 3,
    "structural_shift": 4,
    "isolated_fit_failures": 5,
    "persistent_degradation": 6,
}
COMPARISON_BASELINE_CODES = {
    "": 0,
    "naive_random_walk": 1,
    "rolling_mean": 2,
    "rolling_median": 3,
    "session_seasonal_naive": 4,
    "rolling_variance_5": 10,
    "rolling_variance_20": 11,
    "ewma_variance_0.94": 12,
}

_MEAN_FAMILY_SECTIONS = (
    "exponential_smoothing",
    "autoregressive",
    "seasonal_exogenous",
    "state_space",
)
_FIT_ACCOUNTING_BUCKETS = (
    "attempted",
    "fitted",
    "converged",
    "limited",
    "skipped",
    "timed_out",
    "numerically_invalid",
    "dependency_unavailable",
    "failed",
    "resource_limited",
)
_RESOURCE_REASON_MARKERS = ("budget", "memory", "resource", "timeout")
_NUMERICAL_REASON_MARKERS = (
    "numerical",
    "non_finite",
    "singular",
    "covariance",
)


@dataclass(frozen=True, slots=True)
class ClassicalModelComparisonProfile:
    """Operator controls for bounded saved-artifact comparisons."""

    enabled: bool = False
    mean_reference_baseline: str = "naive_random_walk"
    variance_reference_baseline: str = "ewma_variance_0.94"
    near_zero_tolerance: float = 1e-12
    minimum_stability_folds: int = 3
    drift_tolerance: float = 0.25
    max_models: int = DEFAULT_CLASSICAL_MODEL_COMPARISON_MAX_MODELS
    max_horizons: int = DEFAULT_CLASSICAL_MODEL_COMPARISON_MAX_HORIZONS
    max_comparisons: int = DEFAULT_CLASSICAL_MODEL_COMPARISON_MAX_COMPARISONS
    max_reason_codes: int = DEFAULT_CLASSICAL_MODEL_COMPARISON_MAX_REASON_CODES
    max_samples: int = DEFAULT_CLASSICAL_MODEL_COMPARISON_MAX_SAMPLES
    rounding_digits: int = DEFAULT_CLASSICAL_MODEL_COMPARISON_ROUNDING_DIGITS

    def __post_init__(self) -> None:
        if not self.mean_reference_baseline:
            raise ValueError("mean_reference_baseline must not be empty")
        if not self.variance_reference_baseline:
            raise ValueError("variance_reference_baseline must not be empty")
        if self.near_zero_tolerance <= 0:
            raise ValueError("near_zero_tolerance must be positive")
        if self.minimum_stability_folds < 2:
            raise ValueError("minimum_stability_folds must be at least 2")
        if self.drift_tolerance < 0:
            raise ValueError("drift_tolerance must be non-negative")
        _bounded_positive(
            self.max_models,
            MAX_CLASSICAL_MODEL_COMPARISON_MODELS,
            "max_models",
        )
        _bounded_positive(
            self.max_horizons,
            MAX_CLASSICAL_MODEL_COMPARISON_HORIZONS,
            "max_horizons",
        )
        _bounded_positive(
            self.max_comparisons,
            MAX_CLASSICAL_MODEL_COMPARISONS,
            "max_comparisons",
        )
        _bounded_positive(
            self.max_reason_codes,
            MAX_CLASSICAL_MODEL_COMPARISON_REASON_CODES,
            "max_reason_codes",
        )
        _bounded_positive(
            self.max_samples,
            MAX_CLASSICAL_MODEL_COMPARISON_SAMPLES,
            "max_samples",
        )
        if not 0 <= self.rounding_digits <= 16:
            raise ValueError("rounding_digits must be between 0 and 16")

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible profile metadata."""
        return {
            "enabled": self.enabled,
            "mean_reference_baseline": self.mean_reference_baseline,
            "variance_reference_baseline": self.variance_reference_baseline,
            "near_zero_tolerance": self.near_zero_tolerance,
            "minimum_stability_folds": self.minimum_stability_folds,
            "drift_tolerance": self.drift_tolerance,
            "max_models": self.max_models,
            "max_horizons": self.max_horizons,
            "max_comparisons": self.max_comparisons,
            "max_reason_codes": self.max_reason_codes,
            "max_samples": self.max_samples,
            "rounding_digits": self.rounding_digits,
            "selection_policy": "none",
            "automatic_search": False,
        }


@dataclass(frozen=True, slots=True)
class ClassicalModelComparisonResult:
    """Bounded diagnostics plus durable row-key annotations."""

    diagnostics: Mapping[str, JSONValue]
    annotations: tuple[Mapping[str, Any], ...]


def classical_model_comparison_from_saved_results(
    frame: Any | None,
    fingerprint: Mapping[str, JSONValue],
    *,
    model_input: Mapping[str, JSONValue] | None = None,
    classical_baselines: Mapping[str, JSONValue] | None = None,
    exponential_smoothing: Mapping[str, JSONValue] | None = None,
    autoregressive: Mapping[str, JSONValue] | None = None,
    seasonal_exogenous: Mapping[str, JSONValue] | None = None,
    state_space: Mapping[str, JSONValue] | None = None,
    volatility: Mapping[str, JSONValue] | None = None,
    profile: ClassicalModelComparisonProfile | None = None,
    target: Any | None = None,
) -> ClassicalModelComparisonResult:
    """Compare bounded saved family results without triggering model fits."""
    selected = profile or ClassicalModelComparisonProfile(enabled=True)
    input_contract = dict(model_input or {})
    family_payloads = {
        "classical_baselines": dict(classical_baselines or {}),
        "exponential_smoothing": dict(exponential_smoothing or {}),
        "autoregressive": dict(autoregressive or {}),
        "seasonal_exogenous": dict(seasonal_exogenous or {}),
        "state_space": dict(state_space or {}),
        "volatility": dict(volatility or {}),
    }
    identity = _comparison_identity(fingerprint, input_contract)
    candidates = _normalized_candidates(
        family_payloads,
        identity=identity,
        profile=selected,
    )
    candidates.sort(key=_candidate_sort_key)
    model_count = len({str(row.get("model_id") or "") for row in candidates})
    model_truncated = model_count > selected.max_models
    allowed_models = {str(row.get("model_id") or "") for row in candidates}
    if model_truncated:
        allowed_models = set(sorted(allowed_models)[: selected.max_models])
        candidates = [
            row
            for row in candidates
            if str(row.get("model_id") or "") in allowed_models
        ]
    horizon_values = sorted(
        {_int(row.get("horizon")) for row in candidates if row.get("horizon")}
    )
    horizon_truncated = len(horizon_values) > selected.max_horizons
    allowed_horizons = set(horizon_values[: selected.max_horizons])
    candidates = [
        row
        for row in candidates
        if _int(row.get("horizon")) in allowed_horizons
    ]
    comparison_rows = _comparison_rows(candidates, selected)
    comparison_truncated = len(comparison_rows) > selected.max_comparisons
    comparison_rows = comparison_rows[: selected.max_comparisons]
    accounting = _fit_accounting(
        family_payloads,
        candidates,
        identity=identity,
        profile=selected,
    )
    status = "ready" if comparison_rows else "unavailable"
    reason = None if comparison_rows else "no_model_results"
    eligible_count = sum(row.get("eligible") is True for row in comparison_rows)
    if comparison_rows and not eligible_count:
        status, reason = "limited", "reference_baseline_unavailable"
    reason_counts = Counter(
        reason_code
        for row in comparison_rows
        for reason_code in _string_rows(row.get("eligibility_reasons"))
    )
    bounded_reasons = dict(
        sorted(reason_counts.items())[: selected.max_reason_codes]
    )
    diagnostics: dict[str, JSONValue] = {
        "schema_version": CLASSICAL_MODEL_COMPARISON_SCHEMA_VERSION,
        "advisory": True,
        "status": status,
        "reason": reason,
        "target_axis": cast(
            JSONValue, dict(_mapping(fingerprint.get("target_axis")))
        ),
        "comparison_identity": cast(JSONValue, identity),
        "configuration": selected.to_metadata(),
        "source_contracts": {
            "model_input_schema_version": input_contract.get("schema_version"),
            "family_schema_versions": {
                name: payload.get("schema_version")
                for name, payload in family_payloads.items()
                if payload
            },
            "saved_artifacts_only": True,
            "model_fits_triggered": False,
        },
        "metric_contract": {
            "mean_level_target": "mid_level",
            "volatility_mean_target": "return_mean",
            "variance_target": "conditional_variance",
            "volatility_target": "absolute_return_volatility",
            "metrics_are_not_interchangeable": True,
            "original_and_transformed_scales_distinct": True,
        },
        "comparison_count": len(comparison_rows),
        "eligible_comparison_count": eligible_count,
        "ineligible_comparison_count": len(comparison_rows) - eligible_count,
        "model_count": min(model_count, selected.max_models),
        "horizons": cast(JSONValue, horizon_values[: selected.max_horizons]),
        "comparison_records": cast(JSONValue, comparison_rows),
        "fit_accounting": accounting,
        "reason_counts": cast(JSONValue, bounded_reasons),
        "stability_context": _stability_context(fingerprint),
        "bounds": {
            "models": _bound_payload(
                model_count, selected.max_models, model_truncated
            ),
            "horizons": _bound_payload(
                len(horizon_values), selected.max_horizons, horizon_truncated
            ),
            "comparisons": _bound_payload(
                len(_comparison_rows(candidates, selected)),
                selected.max_comparisons,
                comparison_truncated,
            ),
            "reason_codes": _bound_payload(
                len(reason_counts),
                selected.max_reason_codes,
                len(reason_counts) > selected.max_reason_codes,
            ),
        },
        "selection_policy": "none",
        "deterministic_ordering_purpose": "stable_serialization_and_display_only",
        "descriptive_only": True,
        "hard_fail_quality_gate": False,
        "non_goals": [
            "model_selection",
            "production_recommendation",
            "automatic_order_search",
            "hyperparameter_search",
            "champion_challenger_promotion",
        ],
    }
    annotations, collisions = _build_annotations(
        frame,
        comparison_rows,
        input_contract,
        selected,
        target=target,
    )
    diagnostics["training_projection"] = {
        "schema_version": (
            CLASSICAL_MODEL_COMPARISON_TRAINING_PROJECTION_SCHEMA_VERSION
        ),
        "columns": list(CLASSICAL_MODEL_COMPARISON_COLUMNS),
        "column_prefixes": ["cm_comparison_", "cm_skill_", "cm_stability_"],
        "annotation_count": len(annotations),
        "projection_collision_count": collisions,
        "join_keys": ["series_id", "period", "row_id"],
        "timestamp_is_sole_join_key": False,
        "projection_record_policy": "stable_serialization_first",
        "projection_record_is_model_selection": False,
        "diagnostics_are_retrospective": True,
        "training_eligible": False,
        "legacy_cache_enriched_on_read": True,
        "observed_columns_preserved": True,
        "synthetic_columns_preserved": True,
    }
    diagnostics["comparison_id"] = _stable_id(
        "classical-model-comparison", diagnostics
    )
    return ClassicalModelComparisonResult(diagnostics, annotations)


def project_classical_model_comparison_onto_training_frame(
    frame: Any,
    result: ClassicalModelComparisonResult,
    *,
    target: Any | None = None,
) -> Any:
    """Join retrospective comparison scalars by durable row identity."""
    import polars as pl

    columns = set(getattr(frame, "columns", ()))
    enriched = (
        frame
        if {"series_id", "period", "row_id"}.issubset(columns)
        else ensure_tick_training_features(frame, target=target)
    )
    left = enriched.drop(
        [
            name
            for name in CLASSICAL_MODEL_COMPARISON_COLUMNS
            if name in enriched.columns
        ]
    ).with_row_index("__cm_comparison_original_order")
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
    return projected.sort("__cm_comparison_original_order").drop(
        "__cm_comparison_original_order"
    )


def classical_model_comparison_summary(
    findings: Iterable[QualityFinding],
    *,
    target_limit: int | None = (
        DEFAULT_CLASSICAL_MODEL_COMPARISON_SUMMARY_TARGET_LIMIT
    ),
) -> dict[str, JSONValue] | None:
    """Return a bounded report summary for comparison findings."""
    targets: list[dict[str, JSONValue]] = []
    statuses: Counter[str] = Counter()
    eligible_count = 0
    for finding in findings:
        fingerprint = _mapping(finding.metadata.get("time_series_fingerprint"))
        payload = _mapping(fingerprint.get("classical_model_comparison"))
        if not payload:
            continue
        status = _text(payload.get("status")) or "unavailable"
        statuses[status] += 1
        eligible = _int(payload.get("eligible_comparison_count"))
        eligible_count += eligible
        targets.append(
            {
                "target_axis": cast(
                    JSONValue, dict(_mapping(payload.get("target_axis")))
                ),
                "status": status,
                "reason": payload.get("reason"),
                "comparison_id": payload.get("comparison_id"),
                "comparison_count": _int(payload.get("comparison_count")),
                "eligible_comparison_count": eligible,
                "ineligible_comparison_count": _int(
                    payload.get("ineligible_comparison_count")
                ),
                "model_count": _int(payload.get("model_count")),
                "horizons": cast(JSONValue, _int_rows(payload.get("horizons"))),
                "selection_policy": "none",
            }
        )
    if not targets:
        return None
    targets.sort(
        key=lambda row: _axis_sort_key(_mapping(row.get("target_axis")))
    )
    limit = bounded_report_limit(
        target_limit,
        default_limit=DEFAULT_CLASSICAL_MODEL_COMPARISON_SUMMARY_TARGET_LIMIT,
    )
    included = limit.slice(targets)
    return {
        "schema_version": CLASSICAL_MODEL_COMPARISON_SUMMARY_SCHEMA_VERSION,
        "advisory": True,
        "target_count": len(targets),
        "included_target_count": len(included),
        "omitted_target_count": max(0, len(targets) - len(included)),
        "truncated": len(included) < len(targets),
        "status_counts": dict(sorted(statuses.items())),
        "eligible_comparison_count": eligible_count,
        "target_summaries": cast(JSONValue, included),
        "limit_metadata": limit.count_payload(len(targets)),
        "selection_policy": "none",
    }


def format_classical_model_comparison_summary_lines(
    summary: Mapping[str, JSONValue] | None,
) -> list[str]:
    """Return concise human-readable report lines."""
    if not summary:
        return []
    statuses = _mapping(summary.get("status_counts"))
    status_text = ", ".join(
        f"{name}={value}" for name, value in sorted(statuses.items())
    )
    lines = [
        "Classical model comparison",
        (
            f"targets: {summary.get('target_count', 0)}; "
            f"eligible comparisons: {summary.get('eligible_comparison_count', 0)}"
        ),
        f"statuses: {status_text or 'none'}",
        "selection policy: none (descriptive comparisons only)",
    ]
    omitted = _int(summary.get("omitted_target_count"))
    if omitted:
        lines.append(f"additional comparison targets omitted: {omitted}")
    return lines


def _normalized_candidates(
    payloads: Mapping[str, Mapping[str, JSONValue]],
    *,
    identity: Mapping[str, JSONValue],
    profile: ClassicalModelComparisonProfile,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    shared_baselines: list[Mapping[str, JSONValue]] = []
    for section in _MEAN_FAMILY_SECTIONS:
        payload = payloads.get(section, {})
        family_identity = _family_identity(identity, payload)
        evaluation = _mapping(payload.get("evaluation"))
        for baseline in _mapping_rows(evaluation.get("reference_baselines")):
            shared_baselines.append(
                {
                    **baseline,
                    "__comparison_identity": cast(JSONValue, family_identity),
                }
            )
        for model in _mapping_rows(evaluation.get("models")):
            candidates.extend(
                _mean_model_candidates(
                    model,
                    section=section,
                    identity=family_identity,
                    profile=profile,
                )
            )
    candidates.extend(
        _mean_baseline_candidates(
            shared_baselines,
            identity=identity,
            profile=profile,
        )
    )
    volatility = payloads.get("volatility", {})
    volatility_identity = _family_identity(identity, volatility)
    evaluation = _mapping(volatility.get("evaluation"))
    for model in _mapping_rows(evaluation.get("models")):
        candidates.extend(
            _volatility_model_candidates(
                model,
                identity=volatility_identity,
                profile=profile,
            )
        )
    candidates.extend(
        _variance_baseline_candidates(
            _mapping_rows(evaluation.get("reference_variance_baselines")),
            identity=volatility_identity,
            profile=profile,
        )
    )
    if not shared_baselines:
        candidates.extend(
            _legacy_baseline_candidates(
                payloads.get("classical_baselines", {}),
                identity=identity,
                profile=profile,
            )
        )
    return _deduplicated_candidates(candidates)


def _mean_model_candidates(
    model: Mapping[str, JSONValue],
    *,
    section: str,
    identity: Mapping[str, JSONValue],
    profile: ClassicalModelComparisonProfile,
) -> list[dict[str, Any]]:
    family = _text(model.get("family")) or section
    output: list[dict[str, Any]] = []
    horizons = _bounded_horizon_rows(model.get("horizon_metrics"))
    for horizon_row in horizons:
        horizon = _int(horizon_row.get("horizon"))
        output.append(
            _candidate(
                model,
                identity=identity,
                family=family,
                target_metric="mid_level",
                scale="original_mid",
                horizon=horizon,
                metrics=_simple_metrics(horizon_row),
                fold_results=_fold_results(model, horizon, profile),
                is_reference=False,
                reference_name=profile.mean_reference_baseline,
                profile=profile,
            )
        )
    return output


def _mean_baseline_candidates(
    baselines: Sequence[Mapping[str, JSONValue]],
    *,
    identity: Mapping[str, JSONValue],
    profile: ClassicalModelComparisonProfile,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    seen: set[tuple[str, int | None, int]] = set()
    for baseline in baselines:
        baseline_identity = _mapping(baseline.get("__comparison_identity"))
        name = _baseline_name(baseline)
        window = _optional_int(baseline.get("window"))
        for horizon_row in _bounded_horizon_rows(
            baseline.get("horizon_metrics")
        ):
            horizon = _int(horizon_row.get("horizon"))
            key = (name, window, horizon)
            if key in seen:
                continue
            seen.add(key)
            model_id = f"baseline:{name}" + (
                f":{window}" if window is not None else ""
            )
            model = {
                "model_id": model_id,
                "specification_id": name,
                "specification_code": COMPARISON_BASELINE_CODES.get(name, 0),
                "family": "baseline",
                "status": "ready",
            }
            output.append(
                _candidate(
                    model,
                    identity=baseline_identity or identity,
                    family="baseline",
                    target_metric="mid_level",
                    scale="original_mid",
                    horizon=horizon,
                    metrics=_simple_metrics(horizon_row),
                    fold_results=(),
                    is_reference=name == profile.mean_reference_baseline,
                    reference_name=profile.mean_reference_baseline,
                    profile=profile,
                    baseline_name=name,
                )
            )
    return output


def _legacy_baseline_candidates(
    payload: Mapping[str, JSONValue],
    *,
    identity: Mapping[str, JSONValue],
    profile: ClassicalModelComparisonProfile,
) -> list[dict[str, Any]]:
    evaluation = _mapping(payload.get("evaluation"))
    output: list[dict[str, Any]] = []
    for model in _mapping_rows(evaluation.get("models")):
        name = _text(model.get("model"))
        if not name:
            continue
        candidate = _candidate(
            {
                "model_id": f"legacy-baseline:{name}",
                "specification_id": name,
                "specification_code": model.get("model_code"),
                "family": "baseline",
                "status": model.get("status"),
            },
            identity={
                **identity,
                "fold_set_id": "legacy_chronological_holdout",
            },
            family="baseline",
            target_metric="mid_level",
            scale="original_mid",
            horizon=1,
            metrics={
                "mae": model.get("mae"),
                "rmse": model.get("rmse"),
                "bias": model.get("mean_error"),
            },
            fold_results=(),
            is_reference=False,
            reference_name=profile.mean_reference_baseline,
            profile=profile,
            baseline_name=name,
        )
        candidate["eligibility_seed_reasons"] = ["fold_set_mismatch"]
        output.append(candidate)
    return output


def _volatility_model_candidates(
    model: Mapping[str, JSONValue],
    *,
    identity: Mapping[str, JSONValue],
    profile: ClassicalModelComparisonProfile,
) -> list[dict[str, Any]]:
    family = _text(model.get("family")) or "volatility"
    output: list[dict[str, Any]] = []
    for horizon, raw in _bounded_mapping_items(
        model.get("horizon_metrics"), MAX_CLASSICAL_MODEL_COMPARISON_HORIZONS
    ):
        horizon_value = _int(horizon)
        metrics = _mapping(raw)
        groups = (
            (
                "return_mean",
                "unscaled_return",
                _mapping(metrics.get("mean_metrics")),
                "",
            ),
            (
                "conditional_variance",
                "unscaled_return_squared",
                _mapping(metrics.get("variance_metrics")),
                profile.variance_reference_baseline,
            ),
            (
                "absolute_return_volatility",
                "absolute_unscaled_return",
                _mapping(metrics.get("volatility_metrics")),
                "",
            ),
        )
        for target_metric, scale, metric_group, reference_name in groups:
            output.append(
                _candidate(
                    model,
                    identity=identity,
                    family=family,
                    target_metric=target_metric,
                    scale=scale,
                    horizon=horizon_value,
                    metrics=_simple_metrics(metric_group),
                    fold_results=(),
                    is_reference=False,
                    reference_name=reference_name,
                    profile=profile,
                )
            )
    return output


def _variance_baseline_candidates(
    baselines: Sequence[Mapping[str, JSONValue]],
    *,
    identity: Mapping[str, JSONValue],
    profile: ClassicalModelComparisonProfile,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for baseline in baselines:
        name = _text(baseline.get("name"))
        if not name:
            continue
        for horizon, raw in _bounded_mapping_items(
            baseline.get("horizon_metrics"),
            MAX_CLASSICAL_MODEL_COMPARISON_HORIZONS,
        ):
            model = {
                "model_id": f"baseline:{name}",
                "specification_id": name,
                "specification_code": COMPARISON_BASELINE_CODES.get(name, 0),
                "family": "baseline",
                "status": "ready",
            }
            output.append(
                _candidate(
                    model,
                    identity=identity,
                    family="baseline",
                    target_metric="conditional_variance",
                    scale="unscaled_return_squared",
                    horizon=_int(horizon),
                    metrics=_simple_metrics(_mapping(raw)),
                    fold_results=(),
                    is_reference=name == profile.variance_reference_baseline,
                    reference_name=profile.variance_reference_baseline,
                    profile=profile,
                    baseline_name=name,
                )
            )
    return output


def _candidate(
    model: Mapping[str, Any],
    *,
    identity: Mapping[str, JSONValue],
    family: str,
    target_metric: str,
    scale: str,
    horizon: int,
    metrics: Mapping[str, Any],
    fold_results: Sequence[Mapping[str, Any]],
    is_reference: bool,
    reference_name: str,
    profile: ClassicalModelComparisonProfile,
    baseline_name: str = "",
) -> dict[str, Any]:
    fit_samples = _mapping_rows(model.get("fit_samples"))
    fit_status_counts = _count_mapping(model.get("fit_status_counts"))
    if not fit_status_counts and fit_samples:
        fit_status_counts = dict(
            Counter(
                status
                for row in fit_samples
                if (status := _text(row.get("status")))
            )
        )
    evaluation_status_counts = _count_mapping(
        model.get("evaluation_status_counts")
    )
    reason_counts = _count_mapping(model.get("reason_counts"))
    if fit_samples:
        sampled_reasons = Counter(
            reason
            for row in fit_samples
            if (reason := _text(row.get("reason")))
        )
        if not reason_counts:
            reason_counts = dict(sampled_reasons)
    candidate: dict[str, Any] = {
        **identity,
        "family": family,
        "family_code": COMPARISON_FAMILY_CODES.get(family, 0),
        "model_id": _text(model.get("model_id"))
        or f"{family}:{_text(model.get('specification_id'))}",
        "specification_id": _text(model.get("specification_id")),
        "specification_code": _int(model.get("specification_code")),
        "model_status": _text(model.get("status")) or "unavailable",
        "target_metric": target_metric,
        "scale": scale,
        "horizon": horizon,
        "metrics": dict(metrics),
        "is_reference_baseline": is_reference,
        "baseline_name": baseline_name,
        "reference_baseline_name": reference_name,
        "fit_status_counts": fit_status_counts,
        "evaluation_status_counts": evaluation_status_counts,
        "reason_counts": reason_counts,
        "fold_results": [dict(row) for row in fold_results],
        "fold_results_truncated": model.get("fold_results_truncated") is True,
        "parameter_stability": dict(_mapping(model.get("parameter_stability"))),
        "rolling_window_stability": dict(
            _mapping(model.get("rolling_window_stability"))
        ),
        "regime_error_summary": dict(
            _mapping(model.get("regime_error_summary"))
        ),
    }
    candidate["stability"] = _candidate_stability(candidate, profile)
    return candidate


def _comparison_rows(
    candidates: Sequence[Mapping[str, Any]],
    profile: ClassicalModelComparisonProfile,
) -> list[dict[str, JSONValue]]:
    references = {
        (
            _text(row.get("target_metric")),
            _text(row.get("scale")),
            _int(row.get("horizon")),
            _text(row.get("baseline_name")),
        ): row
        for row in candidates
        if row.get("is_reference_baseline") is True
    }
    output: list[dict[str, JSONValue]] = []
    for candidate in candidates:
        reference_name = _text(candidate.get("reference_baseline_name"))
        reference = references.get(
            (
                _text(candidate.get("target_metric")),
                _text(candidate.get("scale")),
                _int(candidate.get("horizon")),
                reference_name,
            )
        )
        for metric_name, metric_value in sorted(
            _mapping(candidate.get("metrics")).items()
        ):
            if metric_name not in COMPARISON_METRIC_CODES:
                continue
            output.append(
                _comparison_record(
                    candidate,
                    reference,
                    metric_name=metric_name,
                    metric_value=_optional_float(metric_value),
                    profile=profile,
                )
            )
    output.sort(key=_comparison_sort_key)
    return output


def _comparison_record(
    candidate: Mapping[str, Any],
    reference: Mapping[str, Any] | None,
    *,
    metric_name: str,
    metric_value: float | None,
    profile: ClassicalModelComparisonProfile,
) -> dict[str, JSONValue]:
    reasons = list(_string_rows(candidate.get("eligibility_seed_reasons")))
    is_reference = candidate.get("is_reference_baseline") is True
    baseline_value = (
        _optional_float(_mapping(reference.get("metrics")).get(metric_name))
        if reference is not None
        else None
    )
    if metric_value is None:
        reasons.append("raw_metric_unavailable")
    if is_reference:
        reasons.append("reference_record")
    elif reference is None:
        reasons.append("reference_baseline_unavailable")
    else:
        reasons.extend(_identity_mismatch_reasons(candidate, reference))
        if baseline_value is None:
            reasons.append("reference_metric_unavailable")
    fold_overlap = _fold_overlap(candidate, reference)
    if fold_overlap.get("complete") is False:
        reasons.append("incomplete_fold_overlap")
    if candidate.get("fold_results_truncated") is True:
        reasons.append("fold_evidence_truncated")
    reasons = list(dict.fromkeys(reasons))
    eligible = not reasons
    if is_reference:
        eligible = True
    skill = _skill_payload(
        metric_name,
        metric_value,
        baseline_value,
        is_reference=is_reference,
        reasons=reasons,
        profile=profile,
        support_count=_metric_support(candidate),
    )
    record: dict[str, JSONValue] = {
        "schema_version": CLASSICAL_MODEL_COMPARISON_ELIGIBILITY_SCHEMA_VERSION,
        "comparison_id": "",
        "dataset_id": candidate.get("dataset_id"),
        "fingerprint_id": candidate.get("fingerprint_id"),
        "regularization_contract_id": candidate.get(
            "regularization_contract_id"
        ),
        "fold_set_id": candidate.get("fold_set_id"),
        "family": candidate.get("family"),
        "family_code": candidate.get("family_code"),
        "model_id": candidate.get("model_id"),
        "specification_id": candidate.get("specification_id"),
        "specification_code": candidate.get("specification_code"),
        "target_metric": candidate.get("target_metric"),
        "target_metric_code": COMPARISON_TARGET_METRIC_CODES.get(
            _text(candidate.get("target_metric")), 0
        ),
        "scale": candidate.get("scale"),
        "scale_code": COMPARISON_SCALE_CODES.get(
            _text(candidate.get("scale")), 0
        ),
        "frequency_ms": candidate.get("frequency_ms"),
        "transform": candidate.get("transform"),
        "missingness_policy": candidate.get("missingness_policy"),
        "period": candidate.get("period"),
        "horizon": candidate.get("horizon"),
        "metric": metric_name,
        "metric_code": COMPARISON_METRIC_CODES[metric_name],
        "metric_value": _rounded(metric_value, profile.rounding_digits),
        "reference_baseline": candidate.get("reference_baseline_name"),
        "reference_baseline_code": COMPARISON_BASELINE_CODES.get(
            _text(candidate.get("reference_baseline_name")), 0
        ),
        "reference_metric_value": _rounded(
            baseline_value, profile.rounding_digits
        ),
        "eligible": eligible,
        "eligibility_status": (
            "reference"
            if is_reference
            else "eligible" if eligible else "ineligible"
        ),
        "eligibility_reasons": cast(JSONValue, reasons),
        "fold_overlap": fold_overlap,
        "skill": skill,
        "stability": cast(JSONValue, candidate.get("stability")),
        "fit_accounting": _candidate_accounting(candidate, profile),
        "descriptive_only": True,
    }
    record["comparison_id"] = _stable_id("comparison", record)
    return record


def _skill_payload(
    metric_name: str,
    metric_value: float | None,
    baseline_value: float | None,
    *,
    is_reference: bool,
    reasons: Sequence[str],
    profile: ClassicalModelComparisonProfile,
    support_count: int,
) -> dict[str, JSONValue]:
    status = "unavailable"
    reason: str | None = reasons[0] if reasons else None
    value: float | None = None
    mode = "ratio_reduction"
    if is_reference:
        status, reason = "reference", "reference_record"
    elif (
        metric_value is not None and baseline_value is not None and not reasons
    ):
        if metric_name == "mean_qlike":
            mode = "baseline_minus_model"
            value = baseline_value - metric_value
        elif abs(baseline_value) <= profile.near_zero_tolerance:
            reason = "baseline_near_zero"
        else:
            value = 1.0 - metric_value / baseline_value
        if value is not None:
            status = "negative" if value < 0 else "available"
            reason = None
    return {
        "schema_version": CLASSICAL_MODEL_SKILL_SCHEMA_VERSION,
        "status": status,
        "reason": reason,
        "mode": mode,
        "value": _rounded(value, profile.rounding_digits),
        "negative": value is not None and value < 0,
        "raw_metric_value": _rounded(metric_value, profile.rounding_digits),
        "reference_metric_value": _rounded(
            baseline_value, profile.rounding_digits
        ),
        "support_count": support_count,
        "dispersion": {
            "available": False,
            "reason": "aggregate_reference_baseline",
            "fold_level_skill_values_included": False,
        },
    }


def _candidate_stability(
    candidate: Mapping[str, Any],
    profile: ClassicalModelComparisonProfile,
) -> dict[str, JSONValue]:
    rows = [
        row
        for row in _mapping_rows(candidate.get("fold_results"))
        if row.get("status") == "evaluated"
    ]
    errors = [
        value
        for row in rows
        if (value := _optional_float(row.get("error"))) is not None
    ]
    absolute = [abs(value) for value in errors]
    drift = _segment_drift(absolute, profile)
    parameter = _parameter_drift(
        _mapping(candidate.get("parameter_stability")), profile
    )
    fit_counts = _count_mapping(candidate.get("fit_status_counts"))
    attempted = sum(fit_counts.values())
    failures = sum(
        count
        for name, count in fit_counts.items()
        if name in {"failed", "unavailable", "timed_out"}
    )
    convergence_rate = (
        fit_counts.get("converged", 0) / attempted if attempted else None
    )
    failure_rate = failures / attempted if attempted else None
    status = "insufficient_folds"
    if len(absolute) >= profile.minimum_stability_folds:
        if parameter is not None and parameter > profile.drift_tolerance * 2:
            status = "structural_shift"
        elif drift is not None and drift > profile.drift_tolerance:
            status = "persistent_degradation"
        elif failures == 1:
            status = "isolated_fit_failures"
        else:
            status = "stable"
    rolling = _mapping(candidate.get("rolling_window_stability"))
    if not absolute and _mapping_rows(rolling.get("segments")):
        status = "stable"
    return {
        "schema_version": CLASSICAL_MODEL_STABILITY_SCHEMA_VERSION,
        "status": status,
        "fold_count": len(rows),
        "error_drift": _rounded(drift, profile.rounding_digits),
        "skill_drift": None,
        "skill_drift_reason": "aggregate_reference_baseline",
        "parameter_drift": _rounded(parameter, profile.rounding_digits),
        "fit_duration_drift": None,
        "fit_duration_reason": "fit_duration_unavailable_in_source_contract",
        "convergence_rate": _rounded(convergence_rate, profile.rounding_digits),
        "failure_rate": _rounded(failure_rate, profile.rounding_digits),
        "regime_session_sensitivity": cast(
            JSONValue,
            _bounded_mapping(
                _mapping(candidate.get("regime_error_summary")),
                profile.max_samples,
            ),
        ),
        "robust_bounded_summary": True,
    }


def _fit_accounting(
    payloads: Mapping[str, Mapping[str, JSONValue]],
    candidates: Sequence[Mapping[str, Any]],
    *,
    identity: Mapping[str, JSONValue],
    profile: ClassicalModelComparisonProfile,
) -> dict[str, JSONValue]:
    by_family: list[dict[str, JSONValue]] = []
    totals = Counter({name: 0 for name in _FIT_ACCOUNTING_BUCKETS})
    for section in (*_MEAN_FAMILY_SECTIONS, "volatility"):
        payload = payloads.get(section, {})
        if not payload:
            continue
        fit = _mapping(payload.get("fit_summary"))
        counts = _normalized_fit_counts(fit)
        totals.update(counts)
        by_family.append(
            {
                "family_group": section,
                "period": identity.get("period"),
                "counts": cast(JSONValue, counts),
                "rates": _fit_rates(counts, profile),
                "reason_counts": cast(
                    JSONValue,
                    _bounded_count_mapping(
                        _mapping(fit.get("reason_counts")),
                        profile.max_reason_codes,
                    ),
                ),
                "warning_counts": cast(
                    JSONValue,
                    _bounded_count_mapping(
                        _mapping(fit.get("warning_counts")),
                        profile.max_reason_codes,
                    ),
                ),
            }
        )
    by_specification: list[dict[str, JSONValue]] = []
    seen: set[tuple[str, str, int]] = set()
    for candidate in candidates:
        key = (
            _text(candidate.get("family")),
            _text(candidate.get("specification_id")),
            _int(candidate.get("horizon")),
        )
        if key in seen or key[0] == "baseline":
            continue
        seen.add(key)
        accounting = _candidate_accounting(candidate, profile)
        by_specification.append(
            {
                "family": key[0],
                "specification_id": key[1],
                "horizon": key[2],
                "period": identity.get("period"),
                **accounting,
            }
        )
    by_specification.sort(
        key=lambda row: (
            _text(row.get("family")),
            _text(row.get("specification_id")),
            _int(row.get("horizon")),
        )
    )
    by_specification = by_specification[: profile.max_comparisons]
    return {
        "schema_version": CLASSICAL_MODEL_FIT_ACCOUNTING_SCHEMA_VERSION,
        "totals": dict(totals),
        "rates": _fit_rates(totals, profile),
        "by_family": cast(JSONValue, by_family[: profile.max_models]),
        "by_specification_horizon_period": cast(JSONValue, by_specification),
        "failed_models_preserved_in_denominator": True,
        "resource_terminations_separate": True,
        "survivorship_filtering": False,
    }


def _candidate_accounting(
    candidate: Mapping[str, Any],
    profile: ClassicalModelComparisonProfile,
) -> dict[str, JSONValue]:
    counts = _normalized_model_counts(candidate)
    return {
        "counts": cast(JSONValue, counts),
        "rates": _fit_rates(counts, profile),
        "reason_counts": cast(
            JSONValue,
            _bounded_count_mapping(
                _mapping(candidate.get("reason_counts")),
                profile.max_reason_codes,
            ),
        ),
    }


def _normalized_fit_counts(raw: Mapping[str, JSONValue]) -> dict[str, int]:
    statuses = _count_mapping(raw.get("status_counts"))
    reasons = _count_mapping(raw.get("reason_counts"))
    attempted = _int(raw.get("fit_attempt_count")) or sum(statuses.values())
    counts = Counter({name: 0 for name in _FIT_ACCOUNTING_BUCKETS})
    counts["attempted"] = attempted
    counts["converged"] = statuses.get("converged", 0)
    counts["limited"] = statuses.get("limited", 0)
    counts["skipped"] = statuses.get("skipped", 0)
    counts["timed_out"] = statuses.get("timed_out", 0) + sum(
        value for key, value in reasons.items() if "timeout" in key
    )
    counts["dependency_unavailable"] = statuses.get("unavailable", 0) + sum(
        value for key, value in reasons.items() if "dependency" in key
    )
    counts["numerically_invalid"] = sum(
        value
        for key, value in reasons.items()
        if any(marker in key for marker in _NUMERICAL_REASON_MARKERS)
    )
    counts["resource_limited"] = sum(
        value
        for key, value in reasons.items()
        if any(marker in key for marker in _RESOURCE_REASON_MARKERS)
    )
    counts["failed"] = _int(raw.get("failed_fit_count")) or statuses.get(
        "failed", 0
    )
    counts["fitted"] = max(0, attempted - counts["skipped"] - counts["failed"])
    return dict(counts)


def _normalized_model_counts(candidate: Mapping[str, Any]) -> dict[str, int]:
    statuses = _count_mapping(candidate.get("fit_status_counts"))
    reasons = _count_mapping(candidate.get("reason_counts"))
    attempted = sum(statuses.values())
    return _normalized_fit_counts(
        {
            "fit_attempt_count": attempted,
            "status_counts": cast(JSONValue, statuses),
            "reason_counts": cast(JSONValue, reasons),
            "failed_fit_count": sum(
                value
                for key, value in statuses.items()
                if key in {"failed", "unavailable"}
            ),
        }
    )


def _fit_rates(
    counts: Mapping[str, int], profile: ClassicalModelComparisonProfile
) -> dict[str, JSONValue]:
    attempted = counts.get("attempted", 0)
    return {
        f"{name}_rate": _rounded(
            counts.get(name, 0) / attempted if attempted else None,
            profile.rounding_digits,
        )
        for name in _FIT_ACCOUNTING_BUCKETS
        if name != "attempted"
    }


def _comparison_identity(
    fingerprint: Mapping[str, JSONValue],
    model_input: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    target_axis = _mapping(fingerprint.get("target_axis"))
    regularization = _mapping(model_input.get("regularization"))
    transform = _mapping(model_input.get("transform_policy"))
    fold_policy = _mapping(model_input.get("fold_policy"))
    missingness = {
        "expected_closure_policy": regularization.get(
            "expected_closure_policy"
        ),
        "unexpected_missing_policy": regularization.get(
            "unexpected_missing_policy"
        ),
        "empty_bin_value_policy": regularization.get("empty_bin_value_policy"),
        "forward_fill_policy": regularization.get("forward_fill_policy"),
    }
    dataset_id = _stable_id(
        "dataset",
        {
            "target_axis": dict(target_axis),
            "fingerprint_id": fingerprint.get("fingerprint_id"),
        },
    )
    regularization_id = _text(model_input.get("derivation_id")) or _stable_id(
        "regularization", regularization
    )
    fold_set_id = _stable_id(
        "fold-set",
        {
            "kind": fold_policy.get("kind"),
            "fold_count": fold_policy.get("fold_count"),
            "horizons": fold_policy.get("horizons"),
            "minimum_training_observations": fold_policy.get(
                "minimum_training_observations"
            ),
            "rolling_window_observations": fold_policy.get(
                "rolling_window_observations"
            ),
            "embargo_observations": fold_policy.get("embargo_observations"),
        },
    )
    return {
        "dataset_id": dataset_id,
        "fingerprint_id": fingerprint.get("fingerprint_id"),
        "regularization_contract_id": regularization_id,
        "fold_set_id": fold_set_id,
        "frequency_ms": regularization.get("frequency_ms"),
        "transform": transform.get("transform"),
        "missingness_policy": _stable_id("missingness", missingness),
        "period": target_axis.get("period"),
    }


def _family_identity(
    identity: Mapping[str, JSONValue],
    payload: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    """Overlay identity evidence persisted by one saved family artifact."""
    if not payload:
        return dict(identity)
    output = dict(identity)
    derivation_id = _text(payload.get("input_derivation_id"))
    if derivation_id:
        output["regularization_contract_id"] = derivation_id
    transform = _mapping(payload.get("input_transform_policy"))
    if transform.get("transform") is not None:
        output["transform"] = transform.get("transform")
    target_axis = _mapping(payload.get("target_axis"))
    if target_axis.get("period") is not None:
        output["period"] = target_axis.get("period")
    configuration = _mapping(payload.get("configuration"))
    if configuration.get("frequency_ms") is not None:
        output["frequency_ms"] = configuration.get("frequency_ms")
    return output


def _build_annotations(
    frame: Any | None,
    comparisons: Sequence[Mapping[str, JSONValue]],
    model_input: Mapping[str, JSONValue],
    profile: ClassicalModelComparisonProfile,
    *,
    target: Any | None,
) -> tuple[tuple[Mapping[str, Any], ...], int]:
    if frame is None or not comparisons:
        return (), 0
    try:
        enriched = ensure_tick_training_features(frame, target=target)
    except (AttributeError, TypeError, ValueError):
        return (), 0
    availability: dict[tuple[str, str], list[tuple[int, int]]] = {}
    for row in cast(list[dict[str, Any]], enriched.to_dicts()):
        timestamp = _optional_int(row.get("timestamp_utc_ms"))
        row_id = _optional_int(row.get("row_id"))
        if timestamp is None or row_id is None:
            continue
        key = (_text(row.get("series_id")), _text(row.get("period")))
        availability.setdefault(key, []).append((timestamp, row_id))
    for values in availability.values():
        values.sort()
    folds = _mapping_rows(
        _mapping(model_input.get("fold_policy")).get("fold_samples")
    )
    comparisons_by_horizon: dict[int, list[Mapping[str, JSONValue]]] = {}
    for comparison in comparisons:
        comparisons_by_horizon.setdefault(
            _int(comparison.get("horizon")), []
        ).append(comparison)
    for rows in comparisons_by_horizon.values():
        rows.sort(key=_projection_sort_key)
    annotations: dict[tuple[str, str, int], dict[str, Any]] = {}
    collisions = 0
    for fold in folds:
        horizon = _int(fold.get("horizon"))
        options = comparisons_by_horizon.get(horizon, ())
        if not options:
            continue
        comparison = options[0]
        group = (_text(fold.get("series_id")), _text(fold.get("period")))
        target_time = _int(fold.get("target_bin_end_utc_ms"))
        row_id = _first_available_row_id(
            availability.get(group, ()), target_time
        )
        if row_id is None:
            continue
        annotation_key = (*group, row_id)
        annotation = _annotation_row(comparison, fold, row_id, profile)
        if annotation_key in annotations:
            collisions += 1
            continue
        annotations[annotation_key] = annotation
    return (
        tuple(annotations[item] for item in sorted(annotations)),
        collisions,
    )


def _annotation_row(
    comparison: Mapping[str, JSONValue],
    fold: Mapping[str, JSONValue],
    row_id: int,
    profile: ClassicalModelComparisonProfile,
) -> dict[str, Any]:
    skill = _mapping(comparison.get("skill"))
    stability = _mapping(comparison.get("stability"))
    accounting = _mapping(comparison.get("fit_accounting"))
    rates = _mapping(accounting.get("rates"))
    eligible = comparison.get("eligible") is True
    reasons = _string_rows(comparison.get("eligibility_reasons"))
    reason = reasons[0] if reasons else "none"
    target_time = _int(fold.get("target_bin_end_utc_ms"))
    return {
        "series_id": _text(fold.get("series_id")),
        "period": _text(fold.get("period")),
        "row_id": row_id,
        "cm_comparison_schema_version": (
            CLASSICAL_MODEL_COMPARISON_TRAINING_PROJECTION_SCHEMA_VERSION
        ),
        "cm_comparison_id": comparison.get("comparison_id"),
        "cm_comparison_dataset_id": comparison.get("dataset_id"),
        "cm_comparison_regularization_contract_id": comparison.get(
            "regularization_contract_id"
        ),
        "cm_comparison_fold_set_id": comparison.get("fold_set_id"),
        "cm_comparison_family_code": comparison.get("family_code"),
        "cm_comparison_model_id": comparison.get("model_id"),
        "cm_comparison_specification_code": comparison.get(
            "specification_code"
        ),
        "cm_comparison_target_metric_code": comparison.get(
            "target_metric_code"
        ),
        "cm_comparison_scale_code": comparison.get("scale_code"),
        "cm_comparison_metric_code": comparison.get("metric_code"),
        "cm_comparison_horizon": comparison.get("horizon"),
        "cm_comparison_fold_id": fold.get("fold_id"),
        "cm_comparison_origin_row_id": fold.get("origin_row_id"),
        "cm_comparison_target_row_id": fold.get("target_row_id"),
        "cm_comparison_eligible": eligible,
        "cm_comparison_eligibility_code": COMPARISON_ELIGIBILITY_CODES[
            _text(comparison.get("eligibility_status")) or "ineligible"
        ],
        "cm_comparison_reason_code": COMPARISON_REASON_CODES.get(reason, 0),
        "cm_comparison_metric_value": comparison.get("metric_value"),
        "cm_comparison_reference_metric_value": comparison.get(
            "reference_metric_value"
        ),
        "cm_comparison_diagnostic_available": True,
        "cm_comparison_diagnostic_available_at_utc_ms": target_time,
        "cm_comparison_diagnostic_only": True,
        "cm_comparison_training_eligible": False,
        "cm_skill_schema_version": CLASSICAL_MODEL_SKILL_SCHEMA_VERSION,
        "cm_skill_reference_baseline_code": comparison.get(
            "reference_baseline_code"
        ),
        "cm_skill_status_code": COMPARISON_SKILL_STATUS_CODES.get(
            _text(skill.get("status")), 1
        ),
        "cm_skill_value": skill.get("value"),
        "cm_skill_negative": skill.get("negative"),
        "cm_skill_support_count": skill.get("support_count"),
        "cm_skill_available": skill.get("value") is not None,
        "cm_skill_diagnostic_only": True,
        "cm_stability_schema_version": CLASSICAL_MODEL_STABILITY_SCHEMA_VERSION,
        "cm_stability_status_code": COMPARISON_STABILITY_STATUS_CODES.get(
            _text(stability.get("status")), 1
        ),
        "cm_stability_fold_count": stability.get("fold_count"),
        "cm_stability_error_drift": stability.get("error_drift"),
        "cm_stability_skill_drift": stability.get("skill_drift"),
        "cm_stability_parameter_drift": stability.get("parameter_drift"),
        "cm_stability_fit_duration_drift": stability.get("fit_duration_drift"),
        "cm_stability_convergence_rate": rates.get("converged_rate"),
        "cm_stability_failure_rate": rates.get("failed_rate"),
        "cm_stability_available": stability.get("status")
        not in {None, "unavailable"},
        "cm_stability_diagnostic_only": True,
    }


def _ensure_projection_columns(frame: Any) -> Any:
    import polars as pl

    string_suffixes = {
        "schema_version",
        "id",
        "dataset_id",
        "regularization_contract_id",
        "fold_set_id",
        "model_id",
    }
    boolean_suffixes = {
        "eligible",
        "diagnostic_available",
        "diagnostic_only",
        "training_eligible",
        "negative",
        "available",
    }
    float_suffixes = {
        "metric_value",
        "reference_metric_value",
        "value",
        "error_drift",
        "skill_drift",
        "parameter_drift",
        "fit_duration_drift",
        "convergence_rate",
        "failure_rate",
    }
    expressions = []
    for name in CLASSICAL_MODEL_COMPARISON_COLUMNS:
        if name in frame.columns:
            continue
        suffix = _comparison_column_suffix(name)
        dtype = (
            pl.Utf8
            if suffix in string_suffixes
            else (
                pl.Boolean
                if suffix in boolean_suffixes
                else pl.Float64 if suffix in float_suffixes else pl.Int64
            )
        )
        expressions.append(pl.lit(None, dtype=dtype).alias(name))
    return frame.with_columns(expressions) if expressions else frame


def _identity_mismatch_reasons(
    candidate: Mapping[str, Any], reference: Mapping[str, Any]
) -> list[str]:
    fields = (
        (
            "regularization_contract_id",
            "regularization_contract_mismatch",
        ),
        ("frequency_ms", "frequency_mismatch"),
        ("transform", "transform_mismatch"),
        ("missingness_policy", "missingness_policy_mismatch"),
        ("fold_set_id", "fold_set_mismatch"),
        ("period", "period_mismatch"),
        ("horizon", "horizon_mismatch"),
        ("target_metric", "target_metric_mismatch"),
        ("scale", "scale_mismatch"),
    )
    return [
        reason
        for field, reason in fields
        if candidate.get(field) != reference.get(field)
    ]


def _fold_overlap(
    candidate: Mapping[str, Any], reference: Mapping[str, Any] | None
) -> dict[str, JSONValue]:
    candidate_ids = {
        _int(row.get("fold_id"))
        for row in _mapping_rows(candidate.get("fold_results"))
        if row.get("fold_id") is not None
    }
    reference_ids = (
        {
            _int(row.get("fold_id"))
            for row in _mapping_rows(reference.get("fold_results"))
            if row.get("fold_id") is not None
        }
        if reference is not None
        else set()
    )
    if not candidate_ids or not reference_ids:
        return {
            "available": False,
            "complete": None,
            "candidate_fold_count": len(candidate_ids),
            "reference_fold_count": len(reference_ids),
            "overlap_count": 0,
            "reason": "aggregate_fold_evidence",
        }
    overlap = candidate_ids & reference_ids
    return {
        "available": True,
        "complete": overlap == candidate_ids == reference_ids,
        "candidate_fold_count": len(candidate_ids),
        "reference_fold_count": len(reference_ids),
        "overlap_count": len(overlap),
        "reason": None,
    }


def _stability_context(
    fingerprint: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    stationarity = _mapping(fingerprint.get("stationarity_diagnostics"))
    decomposition = _mapping(fingerprint.get("decomposition"))
    calendar = _mapping(fingerprint.get("calendar_regimes"))
    return {
        "stationarity_status": stationarity.get("status"),
        "decomposition_status": decomposition.get("status"),
        "calendar_regime_status": calendar.get("status"),
        "relationship": "contextual_association_only",
        "causal_conclusion": False,
    }


def _segment_drift(
    values: Sequence[float], profile: ClassicalModelComparisonProfile
) -> float | None:
    if len(values) < profile.minimum_stability_folds:
        return None
    width = max(1, len(values) // 3)
    first = median(values[:width])
    last = median(values[-width:])
    denominator = max(abs(first), profile.near_zero_tolerance)
    return (last - first) / denominator


def _parameter_drift(
    payload: Mapping[str, JSONValue], profile: ClassicalModelComparisonProfile
) -> float | None:
    parameters = _mapping(payload.get("parameters"))
    drifts: list[float] = []
    for raw in parameters.values():
        summary = _mapping(raw)
        minimum = _optional_float(summary.get("min"))
        maximum = _optional_float(summary.get("max"))
        center = _optional_float(summary.get("median"))
        if minimum is None or maximum is None or center is None:
            continue
        drifts.append(
            (maximum - minimum) / max(abs(center), profile.near_zero_tolerance)
        )
    return max(drifts) if drifts else None


def _fold_results(
    model: Mapping[str, JSONValue],
    horizon: int,
    profile: ClassicalModelComparisonProfile,
) -> list[Mapping[str, Any]]:
    rows = [
        row
        for row in _mapping_rows(model.get("fold_results"))
        if _int(row.get("horizon")) == horizon
    ]
    rows.sort(
        key=lambda row: (
            _int(row.get("fold_id")),
            _int(row.get("target_row_id")),
        )
    )
    return rows[: profile.max_samples]


def _simple_metrics(raw: Mapping[str, Any]) -> dict[str, JSONValue]:
    metrics = {
        name: cast(JSONValue, raw.get(name))
        for name in COMPARISON_METRIC_CODES
        if name in raw
    }
    if "evaluation_count" in raw:
        metrics["evaluation_count"] = cast(
            JSONValue, raw.get("evaluation_count")
        )
    elif "count" in raw:
        metrics["evaluation_count"] = cast(JSONValue, raw.get("count"))
    return metrics


def _bounded_horizon_rows(raw: Any) -> list[Mapping[str, Any]]:
    rows = _mapping_rows(raw)
    rows.sort(key=lambda row: _int(row.get("horizon")))
    return rows[:MAX_CLASSICAL_MODEL_COMPARISON_HORIZONS]


def _bounded_mapping_items(raw: Any, limit: int) -> list[tuple[str, Any]]:
    return sorted(_mapping(raw).items(), key=lambda item: _int(item[0]))[:limit]


def _baseline_name(payload: Mapping[str, JSONValue]) -> str:
    return _text(payload.get("model")) or _text(payload.get("name"))


def _metric_support(candidate: Mapping[str, Any]) -> int:
    metrics = _mapping(candidate.get("metrics"))
    count = _optional_int(metrics.get("evaluation_count"))
    if count is not None:
        return count
    fold_rows = _mapping_rows(candidate.get("fold_results"))
    return sum(row.get("status") == "evaluated" for row in fold_rows)


def _deduplicated_candidates(
    candidates: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    output: dict[tuple[str, str, int, str], dict[str, Any]] = {}
    for candidate in candidates:
        key = (
            _text(candidate.get("model_id")),
            _text(candidate.get("target_metric")),
            _int(candidate.get("horizon")),
            _text(candidate.get("scale")),
        )
        output.setdefault(key, candidate)
    return list(output.values())


def _candidate_sort_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        _text(row.get("target_metric")),
        _text(row.get("scale")),
        _int(row.get("horizon")),
        0 if row.get("is_reference_baseline") is True else 1,
        _text(row.get("family")),
        _text(row.get("specification_id")),
        _text(row.get("model_id")),
    )


def _comparison_sort_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        _text(row.get("target_metric")),
        _text(row.get("scale")),
        _int(row.get("horizon")),
        _text(row.get("metric")),
        0 if row.get("eligibility_status") == "reference" else 1,
        _text(row.get("family")),
        _text(row.get("specification_id")),
        _text(row.get("model_id")),
    )


def _projection_sort_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
    """Prefer a usable non-reference diagnostic for the bounded row view."""
    skill = _mapping(row.get("skill"))
    return (
        0 if row.get("eligibility_status") != "reference" else 1,
        0 if row.get("eligible") is True else 1,
        0 if skill.get("value") is not None else 1,
        *_comparison_sort_key(row),
    )


def _comparison_column_suffix(name: str) -> str:
    for prefix in ("cm_comparison_", "cm_skill_", "cm_stability_"):
        if name.startswith(prefix):
            return name.removeprefix(prefix)
    return name


def _first_available_row_id(
    rows: Sequence[tuple[int, int]], available_at: int
) -> int | None:
    return next(
        (row_id for timestamp, row_id in rows if timestamp >= available_at),
        None,
    )


def _stable_id(prefix: str, payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=str,
    ).encode("utf-8")
    return f"{prefix}:sha256:{hashlib.sha256(encoded).hexdigest()}"


def _bound_payload(
    total: int, limit: int, truncated: bool
) -> dict[str, JSONValue]:
    return {
        "total_count": total,
        "included_count": min(total, limit),
        "omitted_count": max(0, total - limit),
        "limit": limit,
        "truncated": truncated,
    }


def _bounded_mapping(
    payload: Mapping[str, JSONValue], limit: int
) -> dict[str, JSONValue]:
    return dict(sorted(payload.items())[:limit])


def _bounded_count_mapping(raw: Any, limit: int) -> dict[str, int]:
    return dict(sorted(_count_mapping(raw).items())[:limit])


def _count_mapping(raw: Any) -> dict[str, int]:
    return {
        str(key): _int(value)
        for key, value in _mapping(raw).items()
        if _int(value) >= 0
    }


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _mapping_rows(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [row for row in value if isinstance(row, Mapping)]


def _string_rows(value: Any) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [str(item) for item in value if str(item)]


def _int_rows(value: Any) -> list[int]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [_int(item) for item in value]


def _text(value: Any) -> str:
    return str(value) if value is not None else ""


def _int(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _optional_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _rounded(value: float | None, digits: int) -> float | None:
    return (
        round(value, digits)
        if value is not None and math.isfinite(value)
        else None
    )


def _axis_sort_key(axis: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(
        _text(axis.get(name))
        for name in ("data_format", "timeframe", "symbol", "period", "kind")
    )


def _bounded_positive(value: int, maximum: int, name: str) -> None:
    if not 1 <= value <= maximum:
        raise ValueError(f"{name} must be between 1 and {maximum}")
