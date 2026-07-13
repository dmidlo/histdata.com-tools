"""Row-aligned ASCII tick training feature substrate.

The canonical training surface is a single flat Polars frame: observed tick
market data plus deterministic row identity, issue flags, classification codes,
training controls, and synthetic placeholders.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from histdatacom.data_quality.contracts import (
    QualityFinding,
    QualityLocation,
    QualityReport,
    QualityRuleResult,
    QualitySeverity,
    QualityTarget,
    QualityTargetKind,
)
from histdatacom.histdata_ascii import TICK
from histdatacom.runtime_contracts import JSONValue

TRAINING_SCHEMA_VERSION = "histdatacom.ascii-tick-training-features.v1"
TRAINING_FEATURE_REPORT_SCHEMA_VERSION = (
    "histdatacom.training-feature-report.v1"
)
TRAINING_FEATURE_REPORT_RULE_ID = "training_features.row_issues"

DEFAULT_SUSPICIOUS_TICK_GAP_MS = 172_800_000

IDENTITY_COLUMNS = (
    "training_schema_version",
    "series_id",
    "period",
    "row_id",
    "source_row_number",
    "event_seq",
    "symbol",
    "format",
    "timeframe",
    "source",
    "timestamp_utc_ms",
)

QUALITY_ISSUE_COLUMNS = (
    "dq_issue_duplicate_timestamp",
    "dq_issue_non_monotonic_timestamp",
    "dq_issue_gap_after_previous",
    "dq_issue_suspicious_gap",
    "dq_issue_weekend_activity",
    "dq_issue_session_closed",
    "dq_issue_negative_spread",
    "dq_issue_zero_spread",
    "dq_issue_wide_spread",
    "dq_issue_invalid_row",
    "dq_issue_partial_row",
    "dq_issue_source_unavailable",
    "dq_issue_topology_unavailable",
    "dq_issue_distribution_missing",
    "dq_issue_precision_warning",
    "dq_issue_cache_float_precision",
    "dq_issue_fingerprint_unready",
)

HARD_ISSUE_COLUMNS = (
    "dq_issue_non_monotonic_timestamp",
    "dq_issue_negative_spread",
    "dq_issue_invalid_row",
    "dq_issue_source_unavailable",
    "dq_issue_topology_unavailable",
)

WARNING_ISSUE_COLUMNS = tuple(
    column
    for column in QUALITY_ISSUE_COLUMNS
    if column not in HARD_ISSUE_COLUMNS
)

SYNTHETIC_PLACEHOLDER_COLUMNS = (
    "synth_bid",
    "synth_ask",
    "synth_spread",
    "synth_mid",
    "synth_method_code",
    "synth_confidence",
    "synth_usable",
)

CLASSICAL_MODEL_CONTRACT_COLUMNS = (
    "cm_input_schema_version",
    "cm_input_derivation_id",
    "cm_input_status_code",
    "cm_input_ready",
    "cm_input_exclusion_reason_code",
    "cm_input_frequency_ms",
    "cm_input_bin_start_utc_ms",
    "cm_input_bin_end_utc_ms",
    "cm_input_available_at_utc_ms",
    "cm_input_observation_count",
    "cm_input_source_first_row_id",
    "cm_input_source_last_row_id",
    "cm_input_observed_value",
    "cm_input_value",
    "cm_input_spread",
    "cm_input_transform_code",
    "cm_input_calculation_basis_code",
    "cm_input_available",
    "cm_input_expected_closure",
    "cm_input_unexpected_missing",
    "cm_fold_schema_version",
    "cm_fold_id",
    "cm_fold_kind_code",
    "cm_fold_origin_row_id",
    "cm_fold_target_row_id",
    "cm_fold_horizon",
    "cm_fold_training_start_row_id",
    "cm_fold_training_end_row_id",
    "cm_fold_evaluation_start_row_id",
    "cm_fold_evaluation_end_row_id",
    "cm_evaluation_schema_version",
    "cm_evaluation_status_code",
    "cm_evaluation_target_available",
    "cm_evaluation_forecast",
    "cm_evaluation_actual",
    "cm_evaluation_error",
    "cm_evaluation_diagnostic_only",
)

EXPONENTIAL_SMOOTHING_COLUMNS = (
    "cm_ets_schema_version",
    "cm_ets_input_derivation_id",
    "cm_ets_model_id",
    "cm_ets_family_code",
    "cm_ets_specification_code",
    "cm_ets_error_code",
    "cm_ets_trend_code",
    "cm_ets_seasonal_code",
    "cm_ets_damped_trend",
    "cm_ets_initialization_code",
    "cm_ets_fit_status_code",
    "cm_ets_converged",
    "cm_ets_fold_id",
    "cm_ets_origin_row_id",
    "cm_ets_target_row_id",
    "cm_ets_horizon",
    "cm_ets_forecast",
    "cm_ets_forecast_available",
    "cm_ets_forecast_available_at_utc_ms",
    "cm_ets_actual",
    "cm_ets_error",
    "cm_ets_diagnostic_available",
    "cm_ets_diagnostic_available_at_utc_ms",
    "cm_ets_diagnostic_only",
    "cm_ets_original_scale",
    "cm_ets_training_eligible",
)

AUTOREGRESSIVE_FAMILY_COLUMN_SUFFIXES = (
    "schema_version",
    "input_derivation_id",
    "model_id",
    "family_code",
    "specification_code",
    "p",
    "d",
    "q",
    "trend_code",
    "calculation_basis_code",
    "fit_status_code",
    "failure_reason_code",
    "converged",
    "stationary",
    "invertible",
    "ar_root_min_modulus",
    "ma_root_min_modulus",
    "covariance_condition_number",
    "effective_observation_count",
    "fold_id",
    "origin_row_id",
    "target_row_id",
    "horizon",
    "forecast",
    "forecast_available",
    "forecast_available_at_utc_ms",
    "actual",
    "error",
    "diagnostic_available",
    "diagnostic_available_at_utc_ms",
    "diagnostic_only",
    "original_scale",
    "training_eligible",
)
AUTOREGRESSIVE_COLUMNS = tuple(
    f"cm_{family}_{suffix}"
    for family in ("ar", "arma", "arima")
    for suffix in AUTOREGRESSIVE_FAMILY_COLUMN_SUFFIXES
)

SEASONAL_EXOGENOUS_FAMILY_COLUMN_SUFFIXES = (
    "schema_version",
    "input_derivation_id",
    "model_id",
    "family_code",
    "specification_code",
    "p",
    "d",
    "q",
    "seasonal_p",
    "seasonal_d",
    "seasonal_q",
    "seasonal_period",
    "seasonal_cycle_ms",
    "trend_code",
    "regressor_set_code",
    "regressor_count",
    "regressor_available",
    "calculation_basis_code",
    "fit_status_code",
    "failure_reason_code",
    "converged",
    "stationary",
    "invertible",
    "ar_root_min_modulus",
    "ma_root_min_modulus",
    "covariance_condition_number",
    "effective_observation_count",
    "fold_id",
    "origin_row_id",
    "target_row_id",
    "horizon",
    "forecast",
    "forecast_available",
    "forecast_available_at_utc_ms",
    "actual",
    "error",
    "diagnostic_available",
    "diagnostic_available_at_utc_ms",
    "diagnostic_only",
    "original_scale",
    "training_eligible",
)
SEASONAL_EXOGENOUS_COLUMNS = tuple(
    f"cm_{family}_{suffix}"
    for family in ("sarima", "arimax", "sarimax")
    for suffix in SEASONAL_EXOGENOUS_FAMILY_COLUMN_SUFFIXES
)

STATE_SPACE_COLUMN_SUFFIXES = (
    "schema_version",
    "input_derivation_id",
    "model_id",
    "family_code",
    "specification_code",
    "state_dimension",
    "component_count",
    "initialization_code",
    "fit_status_code",
    "failure_reason_code",
    "converged",
    "effective_observation_count",
    "missing_observation_count",
    "prediction_only_transition_count",
    "max_prediction_only_gap",
    "fold_id",
    "origin_row_id",
    "target_row_id",
    "horizon",
    "forecast",
    "forecast_standard_error",
    "forecast_lower",
    "forecast_upper",
    "forecast_available",
    "forecast_available_at_utc_ms",
    "actual",
    "error",
    "diagnostic_available",
    "diagnostic_available_at_utc_ms",
    "diagnostic_only",
    "original_scale",
    "training_eligible",
)
STATE_SPACE_COLUMNS = tuple(
    f"cm_state_space_{suffix}" for suffix in STATE_SPACE_COLUMN_SUFFIXES
)

KALMAN_COLUMN_SUFFIXES = (
    "schema_version",
    "model_id",
    "filtered_calculation_basis_code",
    "filtered_level",
    "filtered_trend",
    "filtered_level_variance",
    "filtered_trend_variance",
    "filtered_available",
    "filtered_available_at_utc_ms",
    "filtered_training_eligible",
    "smoothed_calculation_basis_code",
    "smoothed_level",
    "smoothed_trend",
    "smoothed_level_variance",
    "smoothed_trend_variance",
    "smoothed_available",
    "smoothed_available_at_utc_ms",
    "smoothed_retrospective",
    "smoothed_diagnostic_only",
    "smoothed_training_eligible",
)
KALMAN_COLUMNS = tuple(
    f"cm_kalman_{suffix}" for suffix in KALMAN_COLUMN_SUFFIXES
)

VOLATILITY_FAMILY_COLUMN_SUFFIXES = (
    "schema_version",
    "input_derivation_id",
    "model_id",
    "specification_code",
    "input_definition_code",
    "mean_model_code",
    "distribution_code",
    "innovation_order",
    "variance_order",
    "scale_factor",
    "fit_status_code",
    "failure_reason_code",
    "converged",
    "effective_observation_count",
    "missing_reset_count",
    "fold_id",
    "origin_row_id",
    "target_row_id",
    "horizon",
    "mean_forecast",
    "variance_forecast",
    "volatility_forecast",
    "annualized_variance_forecast",
    "annualized_volatility_forecast",
    "forecast_available",
    "forecast_available_at_utc_ms",
    "actual_return",
    "realized_variance_proxy",
    "mean_error",
    "variance_error",
    "volatility_error",
    "qlike_loss",
    "diagnostic_available",
    "diagnostic_available_at_utc_ms",
    "diagnostic_only",
    "persistence",
    "unconditional_variance",
    "boundary_parameter",
    "training_eligible",
)
VOLATILITY_COLUMNS = tuple(
    f"cm_{family}_{suffix}"
    for family in ("arch", "garch")
    for suffix in VOLATILITY_FAMILY_COLUMN_SUFFIXES
)

TRAINING_REQUIRED_COLUMNS = (
    *IDENTITY_COLUMNS,
    "spread",
    "mid",
    "quality_status_code",
    "quality_severity_code",
    "quality_finding_count",
    "quality_warning_count",
    "quality_error_count",
    *QUALITY_ISSUE_COLUMNS,
    "gap_from_previous_ms",
    "expected_gap_ms",
    "period_invalid_row_rate",
    "period_zero_spread_rate",
    "period_negative_spread_rate",
    "period_duplicate_timestamp_count",
    "period_suspicious_gap_count",
    "period_quality_issue_count",
    "training_usable",
    "training_weight",
    "training_exclusion_reason_code",
    "class_quality_state_code",
    "class_session_state_code",
    "class_spread_regime_code",
    "class_gap_state_code",
    "class_volatility_regime_code",
    "class_training_action_code",
    *SYNTHETIC_PLACEHOLDER_COLUMNS,
    *CLASSICAL_MODEL_CONTRACT_COLUMNS,
    *EXPONENTIAL_SMOOTHING_COLUMNS,
    *AUTOREGRESSIVE_COLUMNS,
    *SEASONAL_EXOGENOUS_COLUMNS,
    *STATE_SPACE_COLUMNS,
    *KALMAN_COLUMNS,
    *VOLATILITY_COLUMNS,
)

ISSUE_CODE_BY_COLUMN = {
    "dq_issue_duplicate_timestamp": "DQ_ISSUE_DUPLICATE_TIMESTAMP",
    "dq_issue_non_monotonic_timestamp": "DQ_ISSUE_NON_MONOTONIC_TIMESTAMP",
    "dq_issue_gap_after_previous": "DQ_ISSUE_GAP_AFTER_PREVIOUS",
    "dq_issue_suspicious_gap": "DQ_ISSUE_SUSPICIOUS_GAP",
    "dq_issue_weekend_activity": "DQ_ISSUE_WEEKEND_ACTIVITY",
    "dq_issue_session_closed": "DQ_ISSUE_SESSION_CLOSED",
    "dq_issue_negative_spread": "DQ_ISSUE_NEGATIVE_SPREAD",
    "dq_issue_zero_spread": "DQ_ISSUE_ZERO_SPREAD",
    "dq_issue_wide_spread": "DQ_ISSUE_WIDE_SPREAD",
    "dq_issue_invalid_row": "DQ_ISSUE_INVALID_ROW",
    "dq_issue_partial_row": "DQ_ISSUE_PARTIAL_ROW",
    "dq_issue_source_unavailable": "DQ_ISSUE_SOURCE_UNAVAILABLE",
    "dq_issue_topology_unavailable": "DQ_ISSUE_TOPOLOGY_UNAVAILABLE",
    "dq_issue_distribution_missing": "DQ_ISSUE_DISTRIBUTION_MISSING",
    "dq_issue_precision_warning": "DQ_ISSUE_PRECISION_WARNING",
    "dq_issue_cache_float_precision": "DQ_ISSUE_CACHE_FLOAT_PRECISION",
    "dq_issue_fingerprint_unready": "DQ_ISSUE_FINGERPRINT_UNREADY",
}


@dataclass(frozen=True, slots=True)
class TrainingFeatureDefinition:
    """A scalar column in the row-aligned training feature schema."""

    name: str
    dtype: str
    default: JSONValue | None
    description: str
    source: str
    grain: str
    nullable: bool = True


def training_feature_definitions() -> tuple[TrainingFeatureDefinition, ...]:
    """Return the canonical ASCII tick training feature catalog."""
    definitions = [
        TrainingFeatureDefinition(
            "training_schema_version",
            "Utf8",
            TRAINING_SCHEMA_VERSION,
            "Version marker for the enriched ASCII tick training schema.",
            "training_features",
            "row",
            False,
        ),
        TrainingFeatureDefinition(
            "series_id",
            "Utf8",
            "",
            "Deterministic series identity derived from stable dimensions.",
            "training_features",
            "series",
            False,
        ),
        TrainingFeatureDefinition(
            "period",
            "Utf8",
            "",
            "Source archive/cache period for the row.",
            "source",
            "period",
            False,
        ),
        TrainingFeatureDefinition(
            "row_id",
            "Int64",
            None,
            "Deterministic row identity within the series and period.",
            "training_features",
            "row",
            False,
        ),
        TrainingFeatureDefinition(
            "source_row_number",
            "Int64",
            None,
            "One-based row number from the source order.",
            "source",
            "row",
            False,
        ),
        TrainingFeatureDefinition(
            "event_seq",
            "Int64",
            None,
            "Deterministic event sequence preserving duplicate timestamps.",
            "training_features",
            "row",
            False,
        ),
        TrainingFeatureDefinition(
            "symbol",
            "Utf8",
            "",
            "FX symbol or pair for the observed tick series.",
            "source",
            "series",
            False,
        ),
        TrainingFeatureDefinition(
            "format",
            "Utf8",
            "ascii",
            "HistData source format. Initial training support is ASCII only.",
            "source",
            "series",
            False,
        ),
        TrainingFeatureDefinition(
            "timeframe",
            "Utf8",
            TICK,
            "HistData source timeframe. Initial training support is tick only.",
            "source",
            "series",
            False,
        ),
        TrainingFeatureDefinition(
            "source",
            "Utf8",
            "histdata.com",
            "Source system for the observed row.",
            "source",
            "series",
            False,
        ),
        TrainingFeatureDefinition(
            "timestamp_utc_ms",
            "Int64",
            None,
            "Observed UTC epoch millisecond timestamp as a feature.",
            "source",
            "row",
            True,
        ),
        TrainingFeatureDefinition(
            "spread",
            "Float64",
            None,
            "Observed ask minus bid.",
            "market",
            "row",
            True,
        ),
        TrainingFeatureDefinition(
            "mid",
            "Float64",
            None,
            "Observed midpoint between bid and ask.",
            "market",
            "row",
            True,
        ),
        TrainingFeatureDefinition(
            "gap_from_previous_ms",
            "Int64",
            None,
            "Observed timestamp gap from the prior source row.",
            "time",
            "row",
            True,
        ),
        TrainingFeatureDefinition(
            "expected_gap_ms",
            "Int64",
            None,
            "Expected tick gap when a deterministic expectation is available.",
            "time",
            "row",
            True,
        ),
    ]
    definitions.extend(_quality_definition_rows())
    definitions.extend(_period_metric_definition_rows())
    definitions.extend(_classification_definition_rows())
    definitions.extend(_synthetic_definition_rows())
    definitions.extend(_classical_model_contract_definition_rows())
    definitions.extend(_exponential_smoothing_definition_rows())
    definitions.extend(_autoregressive_definition_rows())
    definitions.extend(_seasonal_exogenous_definition_rows())
    definitions.extend(_state_space_definition_rows())
    definitions.extend(_volatility_definition_rows())
    return tuple(definitions)


def required_training_feature_columns() -> tuple[str, ...]:
    """Return the required row-aligned training feature column names."""
    return TRAINING_REQUIRED_COLUMNS


def enrich_tick_cache_with_training_features(
    frame: Any,
    *,
    target: Any | None = None,
    symbol: str = "",
    data_format: str = "",
    timeframe: str = "",
    period: str = "",
    source: str = "histdata.com",
    quality_report: QualityReport | None = None,
    quality_payload: Mapping[str, JSONValue] | None = None,
    fingerprint_payload: Mapping[str, JSONValue] | None = None,
    classification_profile: Mapping[str, JSONValue] | None = None,
) -> Any:
    """Return a flat enriched ASCII tick training feature frame.

    The output keeps observed market columns intact and adds deterministic
    identity, quality issue, classification, training-control, period-metric,
    and synthetic placeholder columns at the same row grain.
    """
    import polars as pl

    _ = (
        quality_report,
        quality_payload,
        fingerprint_payload,
        classification_profile,
    )
    context = _training_context(
        target,
        symbol=symbol,
        data_format=data_format,
        timeframe=timeframe,
        period=period,
        source=source,
    )
    _require_ascii_tick_context(context)
    if "training_schema_version" in getattr(frame, "columns", ()):
        return _ensure_registered_columns(frame)

    _require_observed_tick_columns(frame)
    observed_columns = set(frame.columns)
    series_id = _series_id(context)

    enriched = frame.with_row_index("__training_row_index", offset=1)
    partial_exprs = [
        pl.col(column).is_null() if column in observed_columns else pl.lit(True)
        for column in ("datetime", "bid", "ask", "vol")
    ]
    suspicious_gap_threshold = _suspicious_gap_threshold(context)
    enriched = enriched.with_columns(
        [
            pl.lit(TRAINING_SCHEMA_VERSION).alias("training_schema_version"),
            pl.lit(series_id).alias("series_id"),
            pl.lit(context["period"]).alias("period"),
            pl.col("__training_row_index").cast(pl.Int64).alias("row_id"),
            pl.col("__training_row_index")
            .cast(pl.Int64)
            .alias("source_row_number"),
            pl.col("__training_row_index").cast(pl.Int64).alias("event_seq"),
            pl.lit(context["symbol"]).alias("symbol"),
            pl.lit(context["format"]).alias("format"),
            pl.lit(context["timeframe"]).alias("timeframe"),
            pl.lit(context["source"]).alias("source"),
            pl.col("datetime").cast(pl.Int64).alias("timestamp_utc_ms"),
            (pl.col("ask") - pl.col("bid")).alias("spread"),
            ((pl.col("ask") + pl.col("bid")) / 2).alias("mid"),
            pl.col("datetime")
            .cast(pl.Int64)
            .diff()
            .alias("gap_from_previous_ms"),
            pl.lit(None).cast(pl.Int64).alias("expected_gap_ms"),
            pl.sum_horizontal(partial_exprs)
            .gt(0)
            .alias("dq_issue_partial_row"),
        ]
    )
    enriched = enriched.with_columns(
        [
            pl.col("datetime")
            .is_duplicated()
            .fill_null(False)
            .alias("dq_issue_duplicate_timestamp"),
            pl.col("gap_from_previous_ms")
            .lt(0)
            .fill_null(False)
            .alias("dq_issue_non_monotonic_timestamp"),
            pl.col("gap_from_previous_ms")
            .gt(suspicious_gap_threshold)
            .fill_null(False)
            .alias("dq_issue_suspicious_gap"),
            pl.lit(False).alias("dq_issue_weekend_activity"),
            pl.lit(False).alias("dq_issue_session_closed"),
            pl.col("spread")
            .lt(0)
            .fill_null(False)
            .alias("dq_issue_negative_spread"),
            pl.col("spread")
            .eq(0)
            .fill_null(False)
            .alias("dq_issue_zero_spread"),
            pl.col("spread")
            .gt(0.01)
            .fill_null(False)
            .alias("dq_issue_wide_spread"),
            _invalid_row_expr().alias("dq_issue_invalid_row"),
            pl.lit(False).alias("dq_issue_source_unavailable"),
            pl.lit(False).alias("dq_issue_topology_unavailable"),
            pl.lit(False).alias("dq_issue_distribution_missing"),
            pl.lit(False).alias("dq_issue_precision_warning"),
            pl.lit(False).alias("dq_issue_cache_float_precision"),
            pl.lit(False).alias("dq_issue_fingerprint_unready"),
        ]
    )
    enriched = enriched.with_columns(
        pl.col("dq_issue_suspicious_gap").alias("dq_issue_gap_after_previous")
    )
    enriched = _with_period_metrics(enriched)
    enriched = _with_quality_counts(enriched)
    enriched = _with_classification(enriched)
    enriched = _with_synthetic_placeholders(enriched)
    enriched = enriched.drop("__training_row_index")
    return _ensure_registered_columns(enriched)


def ensure_tick_training_features(
    frame: Any,
    *,
    target: Any | None = None,
    symbol: str = "",
    data_format: str = "",
    timeframe: str = "",
    period: str = "",
    source: str = "histdata.com",
) -> Any:
    """Return an enriched ASCII tick frame, preserving already-enriched caches."""
    return enrich_tick_cache_with_training_features(
        frame,
        target=target,
        symbol=symbol,
        data_format=data_format,
        timeframe=timeframe,
        period=period,
        source=source,
    )


def quality_report_from_training_features(
    frame: Any,
    *,
    target: Any | None = None,
) -> QualityReport:
    """Derive an audit QualityReport from row-aligned training issue columns."""
    _require_ascii_tick_context(
        _training_context(
            target,
            symbol="",
            data_format="",
            timeframe="",
            period="",
            source="histdata.com",
        )
    )
    quality_target = _quality_target_from_context(target)
    issue_counts = _issue_counts(frame)
    findings = tuple(
        _finding_for_issue_column(quality_target, column, count)
        for column, count in issue_counts.items()
        if count > 0
    )
    rule_result = QualityRuleResult(
        rule_id=TRAINING_FEATURE_REPORT_RULE_ID,
        target=quality_target,
        findings=findings,
    )
    return QualityReport(
        targets=(quality_target,),
        rule_results=(rule_result,),
        metadata={
            "schema_version": TRAINING_FEATURE_REPORT_SCHEMA_VERSION,
            "training_schema_version": TRAINING_SCHEMA_VERSION,
            "row_count": int(getattr(frame, "height", 0) or 0),
            "issue_counts": _json_int_counts(issue_counts),
            "training_action_counts": _json_int_counts(
                _value_counts(frame, "class_training_action_code")
            ),
            "quality_state_counts": _json_int_counts(
                _value_counts(frame, "class_quality_state_code")
            ),
        },
    )


def _quality_definition_rows() -> list[TrainingFeatureDefinition]:
    definitions = [
        TrainingFeatureDefinition(
            "quality_status_code",
            "Int32",
            0,
            "Row quality status code: 0 clean, 1 warning, 2 failed.",
            "training_features",
            "row",
            False,
        ),
        TrainingFeatureDefinition(
            "quality_severity_code",
            "Int32",
            0,
            "Compact row severity code aligned to quality status.",
            "training_features",
            "row",
            False,
        ),
        TrainingFeatureDefinition(
            "quality_finding_count",
            "Int32",
            0,
            "Count of row-aligned quality issues.",
            "training_features",
            "row",
            False,
        ),
        TrainingFeatureDefinition(
            "quality_warning_count",
            "Int32",
            0,
            "Count of warning issue flags on this row.",
            "training_features",
            "row",
            False,
        ),
        TrainingFeatureDefinition(
            "quality_error_count",
            "Int32",
            0,
            "Count of hard error issue flags on this row.",
            "training_features",
            "row",
            False,
        ),
    ]
    for column in QUALITY_ISSUE_COLUMNS:
        definitions.append(
            TrainingFeatureDefinition(
                column,
                "Boolean",
                False,
                f"Row-aligned data-quality issue indicator: {column}.",
                "training_features",
                "row",
                False,
            )
        )
    return definitions


def _period_metric_definition_rows() -> list[TrainingFeatureDefinition]:
    return [
        TrainingFeatureDefinition(
            "period_invalid_row_rate",
            "Float64",
            0.0,
            "Period-level invalid row rate projected onto each row.",
            "training_features",
            "period",
            False,
        ),
        TrainingFeatureDefinition(
            "period_zero_spread_rate",
            "Float64",
            0.0,
            "Period-level zero-spread row rate projected onto each row.",
            "training_features",
            "period",
            False,
        ),
        TrainingFeatureDefinition(
            "period_negative_spread_rate",
            "Float64",
            0.0,
            "Period-level negative-spread row rate projected onto each row.",
            "training_features",
            "period",
            False,
        ),
        TrainingFeatureDefinition(
            "period_duplicate_timestamp_count",
            "Int64",
            0,
            "Rows in the period flagged with duplicate timestamps.",
            "training_features",
            "period",
            False,
        ),
        TrainingFeatureDefinition(
            "period_suspicious_gap_count",
            "Int64",
            0,
            "Rows in the period flagged with suspicious timestamp gaps.",
            "training_features",
            "period",
            False,
        ),
        TrainingFeatureDefinition(
            "period_quality_issue_count",
            "Int64",
            0,
            "Total row issue flags in the period projected onto each row.",
            "training_features",
            "period",
            False,
        ),
    ]


def _classification_definition_rows() -> list[TrainingFeatureDefinition]:
    return [
        TrainingFeatureDefinition(
            "training_usable",
            "Boolean",
            True,
            "Whether the observed row is usable for training as-is.",
            "classification",
            "row",
            False,
        ),
        TrainingFeatureDefinition(
            "training_weight",
            "Float64",
            1.0,
            "Default deterministic training weight for the row.",
            "classification",
            "row",
            False,
        ),
        TrainingFeatureDefinition(
            "training_exclusion_reason_code",
            "Int32",
            0,
            "Compact exclusion reason code; 0 means not excluded.",
            "classification",
            "row",
            False,
        ),
        TrainingFeatureDefinition(
            "class_quality_state_code",
            "Int32",
            0,
            "Quality class: 0 clean, 1 warning, 2 failed, 3 unusable.",
            "classification",
            "row",
            False,
        ),
        TrainingFeatureDefinition(
            "class_session_state_code",
            "Int32",
            0,
            "Session class placeholder for later calendar conditioning.",
            "classification",
            "row",
            False,
        ),
        TrainingFeatureDefinition(
            "class_spread_regime_code",
            "Int32",
            0,
            "Spread class: 0 normal, 1 zero, 2 negative, 3 wide, 4 unknown.",
            "classification",
            "row",
            False,
        ),
        TrainingFeatureDefinition(
            "class_gap_state_code",
            "Int32",
            0,
            "Gap class: 0 continuous/unknown, 2 suspicious, 3 nonmonotonic, 4 duplicate.",
            "classification",
            "row",
            False,
        ),
        TrainingFeatureDefinition(
            "class_volatility_regime_code",
            "Int32",
            0,
            "Volatility regime placeholder for later tick-return features.",
            "classification",
            "row",
            False,
        ),
        TrainingFeatureDefinition(
            "class_training_action_code",
            "Int32",
            0,
            "Training action: 0 use, 1 augment, 2 repair, 3 replace, 4 exclude.",
            "classification",
            "row",
            False,
        ),
    ]


def _synthetic_definition_rows() -> list[TrainingFeatureDefinition]:
    return [
        TrainingFeatureDefinition(
            "synth_bid",
            "Float64",
            None,
            "Synthetic bid placeholder; observed bid is never overwritten.",
            "synthetic",
            "row",
            True,
        ),
        TrainingFeatureDefinition(
            "synth_ask",
            "Float64",
            None,
            "Synthetic ask placeholder; observed ask is never overwritten.",
            "synthetic",
            "row",
            True,
        ),
        TrainingFeatureDefinition(
            "synth_spread",
            "Float64",
            None,
            "Synthetic spread placeholder.",
            "synthetic",
            "row",
            True,
        ),
        TrainingFeatureDefinition(
            "synth_mid",
            "Float64",
            None,
            "Synthetic midpoint placeholder.",
            "synthetic",
            "row",
            True,
        ),
        TrainingFeatureDefinition(
            "synth_method_code",
            "Int32",
            None,
            "Synthetic method code placeholder.",
            "synthetic",
            "row",
            True,
        ),
        TrainingFeatureDefinition(
            "synth_confidence",
            "Float64",
            None,
            "Synthetic confidence placeholder.",
            "synthetic",
            "row",
            True,
        ),
        TrainingFeatureDefinition(
            "synth_usable",
            "Boolean",
            None,
            "Synthetic usability placeholder.",
            "synthetic",
            "row",
            True,
        ),
    ]


def _classical_model_contract_definition_rows() -> (
    list[TrainingFeatureDefinition]
):
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
    definitions: list[TrainingFeatureDefinition] = []
    for name in CLASSICAL_MODEL_CONTRACT_COLUMNS:
        dtype = (
            "Utf8"
            if name in strings
            else (
                "Boolean"
                if name in booleans
                else "Float64" if name in floats else "Int64"
            )
        )
        definitions.append(
            TrainingFeatureDefinition(
                name,
                dtype,
                None,
                (
                    "Point-in-time-safe classical model contract scalar: "
                    f"{name}."
                ),
                "classical_model_contracts",
                "row",
                True,
            )
        )
    return definitions


def _exponential_smoothing_definition_rows() -> list[TrainingFeatureDefinition]:
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
    definitions: list[TrainingFeatureDefinition] = []
    for name in EXPONENTIAL_SMOOTHING_COLUMNS:
        dtype = (
            "Utf8"
            if name in strings
            else (
                "Boolean"
                if name in booleans
                else "Float64" if name in floats else "Int64"
            )
        )
        definitions.append(
            TrainingFeatureDefinition(
                name,
                dtype,
                None,
                f"Point-in-time-safe exponential-smoothing scalar: {name}.",
                "exponential_smoothing",
                "row",
                True,
            )
        )
    return definitions


def _autoregressive_definition_rows() -> list[TrainingFeatureDefinition]:
    strings = {
        "schema_version",
        "input_derivation_id",
        "model_id",
    }
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
    definitions: list[TrainingFeatureDefinition] = []
    for name in AUTOREGRESSIVE_COLUMNS:
        suffix = name.split("_", maxsplit=2)[-1]
        dtype = (
            "Utf8"
            if suffix in strings
            else (
                "Boolean"
                if suffix in booleans
                else "Float64" if suffix in floats else "Int64"
            )
        )
        definitions.append(
            TrainingFeatureDefinition(
                name,
                dtype,
                None,
                f"Point-in-time-safe autoregressive scalar: {name}.",
                "autoregressive",
                "row",
                True,
            )
        )
    return definitions


def _seasonal_exogenous_definition_rows() -> list[TrainingFeatureDefinition]:
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
    definitions: list[TrainingFeatureDefinition] = []
    for name in SEASONAL_EXOGENOUS_COLUMNS:
        suffix = name.split("_", maxsplit=2)[-1]
        dtype = (
            "Utf8"
            if suffix in strings
            else (
                "Boolean"
                if suffix in booleans
                else "Float64" if suffix in floats else "Int64"
            )
        )
        definitions.append(
            TrainingFeatureDefinition(
                name,
                dtype,
                None,
                f"Point-in-time-safe seasonal/exogenous scalar: {name}.",
                "seasonal_exogenous",
                "row",
                True,
            )
        )
    return definitions


def _state_space_definition_rows() -> list[TrainingFeatureDefinition]:
    strings = {"schema_version", "input_derivation_id", "model_id"}
    booleans = {
        "converged",
        "forecast_available",
        "diagnostic_available",
        "diagnostic_only",
        "original_scale",
        "training_eligible",
        "filtered_available",
        "filtered_training_eligible",
        "smoothed_available",
        "smoothed_retrospective",
        "smoothed_diagnostic_only",
        "smoothed_training_eligible",
    }
    floats = {
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
    definitions: list[TrainingFeatureDefinition] = []
    for name in (*STATE_SPACE_COLUMNS, *KALMAN_COLUMNS):
        if name.startswith("cm_state_space_"):
            suffix = name.removeprefix("cm_state_space_")
        else:
            suffix = name.removeprefix("cm_kalman_")
        dtype = (
            "Utf8"
            if suffix in strings
            else (
                "Boolean"
                if suffix in booleans
                else "Float64" if suffix in floats else "Int64"
            )
        )
        definitions.append(
            TrainingFeatureDefinition(
                name,
                dtype,
                None,
                f"Point-in-time state-space/Kalman scalar: {name}.",
                "state_space",
                "row",
                True,
            )
        )
    return definitions


def _volatility_definition_rows() -> list[TrainingFeatureDefinition]:
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
    definitions: list[TrainingFeatureDefinition] = []
    for name in VOLATILITY_COLUMNS:
        suffix = name.removeprefix("cm_arch_").removeprefix("cm_garch_")
        dtype = (
            "Utf8"
            if suffix in strings
            else (
                "Boolean"
                if suffix in booleans
                else "Float64" if suffix in floats else "Int64"
            )
        )
        definitions.append(
            TrainingFeatureDefinition(
                name,
                dtype,
                None,
                f"Point-in-time ARCH/GARCH scalar: {name}.",
                "volatility",
                "row",
                True,
            )
        )
    return definitions


def _training_context(
    target: Any | None,
    *,
    symbol: str,
    data_format: str,
    timeframe: str,
    period: str,
    source: str,
) -> dict[str, str]:
    context = {
        "symbol": _context_value(target, "symbol")
        or _context_value(target, "data_fxpair")
        or symbol,
        "format": _context_value(target, "data_format")
        or _context_value(target, "format")
        or data_format
        or "ascii",
        "timeframe": _context_value(target, "timeframe")
        or _context_value(target, "data_timeframe")
        or timeframe
        or TICK,
        "period": _context_value(target, "period")
        or _context_value(target, "data_datemonth")
        or period,
        "source": _context_value(target, "source") or source or "histdata.com",
    }
    context["symbol"] = context["symbol"].upper()
    context["format"] = context["format"].lower()
    context["timeframe"] = _normalize_timeframe(context["timeframe"])
    context["source"] = context["source"] or "histdata.com"
    return context


def _context_value(target: Any | None, name: str) -> str:
    if target is None:
        return ""
    if isinstance(target, Mapping):
        return str(target.get(name, "") or "")
    return str(getattr(target, name, "") or "")


def _normalize_timeframe(value: str) -> str:
    normalized = value.strip()
    aliases = {
        "tick": TICK,
        "ticks": TICK,
        "tick-data-quotes": TICK,
        "tick_data_quotes": TICK,
        "t": TICK,
    }
    return str(aliases.get(normalized.lower(), normalized))


def _require_ascii_tick_context(context: Mapping[str, str]) -> None:
    if context["format"] != "ascii" or context["timeframe"] != TICK:
        raise ValueError("training features support ASCII tick inputs only")


def _require_observed_tick_columns(frame: Any) -> None:
    missing = sorted({"datetime", "bid", "ask"} - set(frame.columns))
    if missing:
        raise ValueError(
            "ASCII tick training enrichment requires columns: "
            + ", ".join(missing)
        )


def _series_id(context: Mapping[str, str]) -> str:
    return ":".join(
        (
            context["format"],
            context["timeframe"],
            context["symbol"],
            context["source"].lower(),
        )
    )


def _suspicious_gap_threshold(context: Mapping[str, str]) -> int:
    value = context.get("suspicious_gap_ms", "")
    try:
        return int(value)
    except (TypeError, ValueError):
        return DEFAULT_SUSPICIOUS_TICK_GAP_MS


def _invalid_row_expr() -> Any:
    import polars as pl

    return (
        pl.col("datetime").is_null()
        | pl.col("bid").is_null()
        | pl.col("ask").is_null()
        | pl.col("bid").le(0)
        | pl.col("ask").le(0)
    ).fill_null(True)


def _with_period_metrics(frame: Any) -> Any:
    import polars as pl

    issue_sum_expr = _issue_sum_expr(QUALITY_ISSUE_COLUMNS)
    return frame.with_columns(
        [
            pl.col("dq_issue_invalid_row")
            .cast(pl.Float64)
            .mean()
            .fill_null(0.0)
            .alias("period_invalid_row_rate"),
            pl.col("dq_issue_zero_spread")
            .cast(pl.Float64)
            .mean()
            .fill_null(0.0)
            .alias("period_zero_spread_rate"),
            pl.col("dq_issue_negative_spread")
            .cast(pl.Float64)
            .mean()
            .fill_null(0.0)
            .alias("period_negative_spread_rate"),
            pl.col("dq_issue_duplicate_timestamp")
            .cast(pl.Int64)
            .sum()
            .alias("period_duplicate_timestamp_count"),
            pl.col("dq_issue_suspicious_gap")
            .cast(pl.Int64)
            .sum()
            .alias("period_suspicious_gap_count"),
            issue_sum_expr.sum()
            .cast(pl.Int64)
            .alias("period_quality_issue_count"),
        ]
    )


def _with_quality_counts(frame: Any) -> Any:
    import polars as pl

    hard_count = _issue_sum_expr(HARD_ISSUE_COLUMNS)
    warning_count = _issue_sum_expr(WARNING_ISSUE_COLUMNS)
    return frame.with_columns(
        [
            hard_count.cast(pl.Int32).alias("quality_error_count"),
            warning_count.cast(pl.Int32).alias("quality_warning_count"),
        ]
    ).with_columns(
        [
            (pl.col("quality_error_count") + pl.col("quality_warning_count"))
            .cast(pl.Int32)
            .alias("quality_finding_count"),
            pl.when(pl.col("quality_error_count") > 0)
            .then(2)
            .when(pl.col("quality_warning_count") > 0)
            .then(1)
            .otherwise(0)
            .cast(pl.Int32)
            .alias("quality_status_code"),
            pl.when(pl.col("quality_error_count") > 0)
            .then(2)
            .when(pl.col("quality_warning_count") > 0)
            .then(1)
            .otherwise(0)
            .cast(pl.Int32)
            .alias("quality_severity_code"),
        ]
    )


def _with_classification(frame: Any) -> Any:
    import polars as pl

    has_error = pl.col("quality_error_count") > 0
    has_warning = pl.col("quality_warning_count") > 0
    return frame.with_columns(
        [
            has_error.not_().alias("training_usable"),
            pl.when(has_error)
            .then(0.0)
            .when(has_warning)
            .then(0.5)
            .otherwise(1.0)
            .alias("training_weight"),
            pl.when(pl.col("dq_issue_invalid_row"))
            .then(1)
            .when(pl.col("dq_issue_negative_spread"))
            .then(2)
            .when(pl.col("dq_issue_non_monotonic_timestamp"))
            .then(3)
            .when(pl.col("dq_issue_source_unavailable"))
            .then(4)
            .when(pl.col("dq_issue_topology_unavailable"))
            .then(5)
            .otherwise(0)
            .cast(pl.Int32)
            .alias("training_exclusion_reason_code"),
            pl.when(
                pl.col("dq_issue_source_unavailable")
                | pl.col("dq_issue_topology_unavailable")
                | pl.col("dq_issue_invalid_row")
            )
            .then(3)
            .when(has_error)
            .then(2)
            .when(has_warning)
            .then(1)
            .otherwise(0)
            .cast(pl.Int32)
            .alias("class_quality_state_code"),
            pl.lit(0).cast(pl.Int32).alias("class_session_state_code"),
            pl.when(pl.col("dq_issue_negative_spread"))
            .then(2)
            .when(pl.col("dq_issue_zero_spread"))
            .then(1)
            .when(pl.col("dq_issue_wide_spread"))
            .then(3)
            .when(pl.col("dq_issue_distribution_missing"))
            .then(4)
            .otherwise(0)
            .cast(pl.Int32)
            .alias("class_spread_regime_code"),
            pl.when(pl.col("dq_issue_non_monotonic_timestamp"))
            .then(3)
            .when(pl.col("dq_issue_suspicious_gap"))
            .then(2)
            .when(pl.col("dq_issue_duplicate_timestamp"))
            .then(4)
            .otherwise(0)
            .cast(pl.Int32)
            .alias("class_gap_state_code"),
            pl.lit(0).cast(pl.Int32).alias("class_volatility_regime_code"),
            pl.when(has_error)
            .then(4)
            .when(has_warning)
            .then(1)
            .otherwise(0)
            .cast(pl.Int32)
            .alias("class_training_action_code"),
        ]
    )


def _with_synthetic_placeholders(frame: Any) -> Any:
    import polars as pl

    return frame.with_columns(
        [
            pl.lit(None).cast(pl.Float64).alias("synth_bid"),
            pl.lit(None).cast(pl.Float64).alias("synth_ask"),
            pl.lit(None).cast(pl.Float64).alias("synth_spread"),
            pl.lit(None).cast(pl.Float64).alias("synth_mid"),
            pl.lit(None).cast(pl.Int32).alias("synth_method_code"),
            pl.lit(None).cast(pl.Float64).alias("synth_confidence"),
            pl.lit(None).cast(pl.Boolean).alias("synth_usable"),
        ]
    )


def _issue_sum_expr(columns: tuple[str, ...]) -> Any:
    import polars as pl

    return pl.sum_horizontal(
        [pl.col(column).cast(pl.Int32) for column in columns]
    )


def _ensure_registered_columns(frame: Any) -> Any:
    missing = [
        definition
        for definition in training_feature_definitions()
        if definition.name not in frame.columns
    ]
    if not missing:
        return frame

    return frame.with_columns(
        [_default_expr(definition) for definition in missing]
    )


def _default_expr(definition: TrainingFeatureDefinition) -> Any:
    import polars as pl

    return (
        pl.lit(definition.default)
        .cast(_polars_dtype(definition.dtype))
        .alias(definition.name)
    )


def _polars_dtype(dtype: str) -> Any:
    import polars as pl

    return {
        "Boolean": pl.Boolean,
        "Float64": pl.Float64,
        "Int32": pl.Int32,
        "Int64": pl.Int64,
        "Utf8": pl.Utf8,
    }[dtype]


def _quality_target_from_context(target: Any | None) -> QualityTarget:
    if isinstance(target, QualityTarget):
        return target
    kind = QualityTargetKind.UNKNOWN
    path = _context_value(target, "path")
    if path and Path(path).name == ".data":
        kind = QualityTargetKind.CACHE
    return QualityTarget(
        path=path,
        kind=kind,
        data_format=_context_value(target, "data_format")
        or _context_value(target, "format"),
        timeframe=_context_value(target, "timeframe")
        or _context_value(target, "data_timeframe"),
        symbol=_context_value(target, "symbol")
        or _context_value(target, "data_fxpair"),
        period=_context_value(target, "period")
        or _context_value(target, "data_datemonth"),
    )


def _issue_counts(frame: Any) -> dict[str, int]:
    import polars as pl

    if getattr(frame, "height", 0) < 1:
        return {column: 0 for column in QUALITY_ISSUE_COLUMNS}

    expressions = [
        pl.col(column).cast(pl.Int64).sum().alias(column)
        for column in QUALITY_ISSUE_COLUMNS
        if column in frame.columns
    ]
    if not expressions:
        return {column: 0 for column in QUALITY_ISSUE_COLUMNS}

    values = frame.select(expressions).to_dicts()[0]
    return {
        column: int(values.get(column, 0) or 0)
        for column in QUALITY_ISSUE_COLUMNS
    }


def _finding_for_issue_column(
    target: QualityTarget,
    column: str,
    count: int,
) -> QualityFinding:
    severity = (
        QualitySeverity.ERROR
        if column in HARD_ISSUE_COLUMNS
        else QualitySeverity.WARNING
    )
    code = ISSUE_CODE_BY_COLUMN[column]
    return QualityFinding(
        severity=severity,
        code=code,
        message=f"{count} row(s) flagged {column}.",
        rule_id=TRAINING_FEATURE_REPORT_RULE_ID,
        target=target,
        location=QualityLocation(path=target.path, column=column),
        metadata={
            "schema_version": TRAINING_FEATURE_REPORT_SCHEMA_VERSION,
            "training_schema_version": TRAINING_SCHEMA_VERSION,
            "issue_column": column,
            "row_count": count,
        },
    )


def _value_counts(frame: Any, column: str) -> dict[str, int]:
    if column not in getattr(frame, "columns", ()):
        return {}
    rows = frame.get_column(column).value_counts().to_dicts()
    counts: dict[str, int] = {}
    for row in rows:
        value = row[column]
        count = row["count"]
        counts[str(value)] = int(count)
    return counts


def _json_int_counts(values: Mapping[str, int]) -> dict[str, JSONValue]:
    return {str(key): int(value) for key, value in values.items()}
