"""Shared deterministic contract registry for fingerprint discovery surfaces."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from histdatacom.data_quality.autoregressive import (
    AUTOREGRESSIVE_BOUNDED_PAYLOAD_KEY,
    AUTOREGRESSIVE_CONFIGURATION_SCHEMA_VERSION,
    AUTOREGRESSIVE_EVALUATION_SCHEMA_VERSION,
    AUTOREGRESSIVE_FAMILIES,
    AUTOREGRESSIVE_FIT_SCHEMA_VERSION,
    AUTOREGRESSIVE_FIT_STATUS_CODES,
    AUTOREGRESSIVE_FORECAST_SCHEMA_VERSION,
    AUTOREGRESSIVE_REASON_CODES,
    AUTOREGRESSIVE_SCHEMA_VERSION,
    AUTOREGRESSIVE_SUMMARY_METADATA_KEY,
    AUTOREGRESSIVE_SUMMARY_SCHEMA_VERSION,
    AUTOREGRESSIVE_TRAINING_PROJECTION_SCHEMA_VERSION,
    DEFAULT_AUTOREGRESSIVE_SUMMARY_TARGET_LIMIT,
)
from histdatacom.data_quality.calendar import (
    TIME_SERIES_FINGERPRINT_CALENDAR_REGIMES_SCHEMA_VERSION,
)
from histdatacom.data_quality.classical_baselines import (
    CLASSICAL_BASELINE_BOUNDED_PAYLOAD_KEY,
    CLASSICAL_BASELINE_SCHEMA_VERSION,
    CLASSICAL_BASELINE_SUMMARY_METADATA_KEY,
    CLASSICAL_BASELINE_SUMMARY_SCHEMA_VERSION,
    CLASSICAL_BASELINE_TRAINING_PROJECTION_SCHEMA_VERSION,
    DEFAULT_BASELINE_SUMMARY_TARGET_LIMIT,
)
from histdatacom.data_quality.classical_model_contracts import (
    CLASSICAL_MODEL_EVALUATION_RESULT_SCHEMA_VERSION,
    CLASSICAL_MODEL_FIT_RESULT_SCHEMA_VERSION,
    CLASSICAL_MODEL_FOLD_SCHEMA_VERSION,
    CLASSICAL_MODEL_INPUT_BOUNDED_PAYLOAD_KEY,
    CLASSICAL_MODEL_INPUT_SCHEMA_VERSION,
    CLASSICAL_MODEL_INPUT_SUMMARY_METADATA_KEY,
    CLASSICAL_MODEL_INPUT_SUMMARY_SCHEMA_VERSION,
    CLASSICAL_MODEL_TRAINING_PROJECTION_SCHEMA_VERSION,
    DEFAULT_MODEL_INPUT_SUMMARY_TARGET_LIMIT,
    MODEL_FAILURE_REASON_CODES,
    MODEL_FIT_STATUSES,
    MODEL_TRANSFORM_CODES,
)
from histdatacom.data_quality.fingerprints import (
    CROSS_SERIES_FINGERPRINT_METADATA_KEY,
    CROSS_SERIES_FINGERPRINT_RULE_ID,
    CROSS_SERIES_FINGERPRINT_SCHEMA_VERSION,
    DEFAULT_FINGERPRINT_DISTRIBUTION_ATTENTION_LIMIT,
    DEFAULT_FINGERPRINT_DISTRIBUTION_FLAG_CACHE_FLOAT_PRECISION,
    DEFAULT_FINGERPRINT_DISTRIBUTION_FLAG_TRUNCATED,
    DEFAULT_FINGERPRINT_DISTRIBUTION_INVALID_ROW_MIN_COUNT,
    DEFAULT_FINGERPRINT_DISTRIBUTION_INVALID_ROW_MIN_RATE,
    DEFAULT_FINGERPRINT_DISTRIBUTION_NEGATIVE_SPREAD_MIN_COUNT,
    DEFAULT_FINGERPRINT_DISTRIBUTION_NEGATIVE_SPREAD_MIN_RATE,
    DEFAULT_FINGERPRINT_DISTRIBUTION_SUMMARY_LIMIT,
    DEFAULT_FINGERPRINT_DISTRIBUTION_ZERO_SPREAD_MIN_COUNT,
    DEFAULT_FINGERPRINT_DISTRIBUTION_ZERO_SPREAD_MIN_RATE,
    DEFAULT_FINGERPRINT_READINESS_LAG_LIMIT,
    DEFAULT_FINGERPRINT_READINESS_RISK_REASON_LIMIT,
    DEFAULT_FINGERPRINT_READINESS_RISK_SECTION_LIMIT,
    DEFAULT_FINGERPRINT_READINESS_RISK_TARGET_LIMIT,
    DEFAULT_FINGERPRINT_PARITY_SUMMARY_LIMIT,
    DEFAULT_FINGERPRINT_READINESS_SUMMARY_LIMIT,
    DEFAULT_FINGERPRINT_REGIME_COUNT_LIMIT,
    DEFAULT_FINGERPRINT_REGIME_SUMMARY_LIMIT,
    DEFAULT_FINGERPRINT_TOPOLOGY_ATTENTION_LIMIT,
    DEFAULT_FINGERPRINT_TOPOLOGY_INSPECTION_SAMPLE_LIMIT,
    DEFAULT_FINGERPRINT_TOPOLOGY_SUMMARY_LIMIT,
    SERIES_FINGERPRINT_RULE_ID,
    TIME_SERIES_FINGERPRINT_AUDIT_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_CONDITIONAL_DISTRIBUTIONS_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_COVERAGE_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_COVERAGE_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_DECOMPOSITION_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_DECOMPOSITION_TRAINING_PROJECTION_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_DEPENDENCE_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_DYNAMICS_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_PARITY_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_PARITY_SUMMARY_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_PARITY_SUMMARY_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_READINESS_RISK_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_READINESS_RISK_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_REGIME_SUMMARY_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_REGIME_SUMMARY_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_STATIONARITY_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_SCHEMA_VERSION,
)
from histdatacom.data_quality.exponential_smoothing import (
    DEFAULT_EXPONENTIAL_SMOOTHING_SUMMARY_TARGET_LIMIT,
    EXPONENTIAL_SMOOTHING_BOUNDED_PAYLOAD_KEY,
    EXPONENTIAL_SMOOTHING_CONFIGURATION_SCHEMA_VERSION,
    EXPONENTIAL_SMOOTHING_EVALUATION_SCHEMA_VERSION,
    EXPONENTIAL_SMOOTHING_FAMILIES,
    EXPONENTIAL_SMOOTHING_FIT_SCHEMA_VERSION,
    EXPONENTIAL_SMOOTHING_FORECAST_SCHEMA_VERSION,
    EXPONENTIAL_SMOOTHING_REASON_CODES,
    EXPONENTIAL_SMOOTHING_SCHEMA_VERSION,
    EXPONENTIAL_SMOOTHING_SUMMARY_METADATA_KEY,
    EXPONENTIAL_SMOOTHING_SUMMARY_SCHEMA_VERSION,
    EXPONENTIAL_SMOOTHING_TRAINING_PROJECTION_SCHEMA_VERSION,
)
from histdatacom.data_quality.time import (
    TIMESTAMP_TOPOLOGY_INSPECTION_SCHEMA_VERSION,
)
from histdatacom.data_quality.synthetic_constraints import (
    DEFAULT_SYNTHETIC_CONSTRAINT_SUMMARY_LIMIT,
    SYNTHETIC_CONSTRAINT_BOUNDED_PAYLOAD_KEY,
    SYNTHETIC_CONSTRAINT_SUMMARY_METADATA_KEY,
    SYNTHETIC_CONSTRAINT_SUMMARY_SCHEMA_VERSION,
    SYNTHETIC_CONSTRAINTS_SCHEMA_VERSION,
    SYNTHETIC_VALIDATION_SCHEMA_VERSION,
)
from histdatacom.data_quality.seasonal_exogenous import (
    DEFAULT_SEASONAL_EXOGENOUS_SUMMARY_TARGET_LIMIT,
    SEASONAL_EXOGENOUS_BOUNDED_PAYLOAD_KEY,
    SEASONAL_EXOGENOUS_CONFIGURATION_SCHEMA_VERSION,
    SEASONAL_EXOGENOUS_EVALUATION_SCHEMA_VERSION,
    SEASONAL_EXOGENOUS_FAMILIES,
    SEASONAL_EXOGENOUS_FIT_SCHEMA_VERSION,
    SEASONAL_EXOGENOUS_FIT_STATUS_CODES,
    SEASONAL_EXOGENOUS_FORECAST_SCHEMA_VERSION,
    SEASONAL_EXOGENOUS_REASON_CODES,
    SEASONAL_EXOGENOUS_REGRESSOR_SCHEMA_VERSION,
    SEASONAL_EXOGENOUS_SCHEMA_VERSION,
    SEASONAL_EXOGENOUS_SUMMARY_METADATA_KEY,
    SEASONAL_EXOGENOUS_SUMMARY_SCHEMA_VERSION,
    SEASONAL_EXOGENOUS_TRAINING_PROJECTION_SCHEMA_VERSION,
)
from histdatacom.data_quality.state_space import (
    DEFAULT_STATE_SPACE_SUMMARY_TARGET_LIMIT,
    STATE_SPACE_BOUNDED_PAYLOAD_KEY,
    STATE_SPACE_CONFIGURATION_SCHEMA_VERSION,
    STATE_SPACE_EVALUATION_SCHEMA_VERSION,
    STATE_SPACE_FAMILIES,
    STATE_SPACE_FIT_SCHEMA_VERSION,
    STATE_SPACE_FIT_STATUS_CODES,
    STATE_SPACE_FORECAST_SCHEMA_VERSION,
    STATE_SPACE_REASON_CODES,
    STATE_SPACE_SCHEMA_VERSION,
    STATE_SPACE_STATE_RESULT_SCHEMA_VERSION,
    STATE_SPACE_SUMMARY_METADATA_KEY,
    STATE_SPACE_SUMMARY_SCHEMA_VERSION,
    STATE_SPACE_TRAINING_PROJECTION_SCHEMA_VERSION,
)
from histdatacom.data_quality.training_features import (
    AUTOREGRESSIVE_COLUMNS,
    EXPONENTIAL_SMOOTHING_COLUMNS,
    SEASONAL_EXOGENOUS_COLUMNS,
    KALMAN_COLUMNS,
    STATE_SPACE_COLUMNS,
    training_feature_definitions,
)
from histdatacom.histdata_ascii import TICK
from histdatacom.runtime_contracts import JSONValue

FINGERPRINT_SERIES_CONFIG_KEYS = (
    "quantiles",
    "lags",
    "rolling_windows",
    "histogram_bins",
    "max_rows",
    "rounding_digits",
    "topology_inspection_sample_limit",
    "distribution_attention",
    "cache_source_parity",
    "classical_baselines",
    "classical_model_input",
    "exponential_smoothing",
    "autoregressive",
    "seasonal_exogenous",
    "state_space",
)
FINGERPRINT_DISTRIBUTION_ATTENTION_CONFIG_KEYS = (
    "invalid_row_min_count",
    "invalid_row_min_rate",
    "zero_spread_min_count",
    "zero_spread_min_rate",
    "negative_spread_min_count",
    "negative_spread_min_rate",
    "flag_truncated_distribution",
    "flag_cache_float_precision",
)

FINGERPRINT_COVERAGE_BOUNDED_PAYLOAD_KEY = "fingerprint_coverage"
FINGERPRINT_TOPOLOGY_BOUNDED_PAYLOAD_KEY = "fingerprint_topology"
FINGERPRINT_TOPOLOGY_ATTENTION_BOUNDED_PAYLOAD_KEY = (
    "fingerprint_topology_attention"
)
FINGERPRINT_DISTRIBUTION_BOUNDED_PAYLOAD_KEY = "fingerprint_distribution"
FINGERPRINT_DISTRIBUTION_ATTENTION_BOUNDED_PAYLOAD_KEY = (
    "fingerprint_distribution_attention"
)
FINGERPRINT_REGIME_BOUNDED_PAYLOAD_KEY = "fingerprint_regime"
FINGERPRINT_READINESS_BOUNDED_PAYLOAD_KEY = "fingerprint_readiness"
FINGERPRINT_READINESS_RISK_BOUNDED_PAYLOAD_KEY = "fingerprint_readiness_risk"
FINGERPRINT_CROSS_SERIES_BOUNDED_PAYLOAD_KEY = "fingerprint_cross_series"
FINGERPRINT_PARITY_BOUNDED_PAYLOAD_KEY = "fingerprint_parity"
FINGERPRINT_SYNTHETIC_CONSTRAINT_BOUNDED_PAYLOAD_KEY = (
    SYNTHETIC_CONSTRAINT_BOUNDED_PAYLOAD_KEY
)

FINGERPRINT_SECTION_STATUSES = ("valid", "limited", "skipped", "unavailable")
FINGERPRINT_DYNAMICS_STATUSES = ("ok", "limited", "unavailable")
FINGERPRINT_READINESS_STATUSES = (
    "computed",
    "valid",
    "limited",
    "skipped",
    "unavailable",
    "not_applicable",
)
FINGERPRINT_ELIGIBILITY_STATUSES = ("eligible", "ineligible")
FINGERPRINT_SKIP_REASON_CODES = (
    "unsupported_timeframe",
    "unsupported_target_kind",
    "source_unreadable",
    "cache_unavailable",
    "missing_required_columns",
    "metric_not_available",
    "insufficient_rows",
    "insufficient_sequence_rows",
    "insufficient_sample_count",
    "zero_variance",
    "no_computable_lags",
    "skipped_lags",
    "skipped_rolling_windows",
    "stationarity_limited",
    "stationarity_unavailable",
    "not_emitted",
)
FINGERPRINT_TOPOLOGY_LIMITATIONS = (
    "timestamp_topology_unavailable",
    "no_parsed_timestamps",
    "invalid_timestamps_skipped",
    "non_monotonic_timestamp_order",
    "duplicate_timestamps",
    "suspicious_gaps",
    "expected_session_closures",
    "weekend_activity",
)
FINGERPRINT_CONDITIONAL_DISTRIBUTION_GROUPS = (
    "active_session",
    "special_tag",
)

FINGERPRINT_BASIS_DESCRIPTIONS = (
    (
        "observed_sequence",
        "statistics computed over parsed row order without regular-grid imputation",
    ),
    (
        "regular_grid",
        "deterministic UTC grid derived from enriched ASCII tick rows",
    ),
    ("limited", "section emitted with advisory limitations"),
    ("unavailable", "section could not compute enough contract data"),
)
FINGERPRINT_ROW_ORDER_DESCRIPTIONS = (
    (
        "source_text_order",
        "rows were scanned from source CSV or ZIP member text order",
    ),
    ("cache_order", "rows were scanned from the selected Polars cache order"),
    ("none", "no row sequence was available"),
    ("unknown", "older or incomplete payload did not state row order"),
)
FINGERPRINT_COMPUTED_FROM_DESCRIPTIONS = (
    ("text_scan", "source text was read directly"),
    ("direct_cache", "target itself was a cache"),
    (
        "fresh_sibling_cache",
        "fresh sibling cache was used for the source target",
    ),
    ("unavailable", "source and cache projection were not usable"),
    ("unknown", "older or incomplete payload did not state source basis"),
)
FINGERPRINT_CACHE_SOURCE_DESCRIPTIONS = (
    ("direct", "cache target was evaluated directly"),
    ("sibling", "fresh sibling cache was selected for a source target"),
    ("none", "no cache source participated"),
)


@dataclass(frozen=True)
class FingerprintSchemaContract:
    """One schema or payload surface exposed by fingerprint discovery."""

    key: str
    schema_version: str | None
    rule_id: str
    status: str
    metadata_key: str = ""
    payload_path: str = ""
    bounded_payload_key: str = ""
    issue: str = ""

    def to_discovery_payload(self) -> dict[str, JSONValue]:
        payload: dict[str, JSONValue] = {
            "schema_version": self.schema_version,
            "rule_id": self.rule_id,
            "status": self.status,
        }
        if self.metadata_key:
            payload["metadata_key"] = self.metadata_key
        if self.payload_path:
            payload["payload_path"] = self.payload_path
        if self.bounded_payload_key:
            payload["bounded_payload_key"] = self.bounded_payload_key
        if self.issue:
            payload["issue"] = self.issue
        return payload


@dataclass(frozen=True)
class FingerprintTargetSectionContract:
    """One implemented target-scoped fingerprint section."""

    name: str
    description: str
    target_timeframes: tuple[str, ...]
    schema_key: str
    key_fields: tuple[str, ...] = ()
    basis_values: tuple[str, ...] = ()
    row_order_values: tuple[str, ...] = ()
    extra: Mapping[str, JSONValue] = field(default_factory=dict)

    def to_discovery_payload(self) -> dict[str, JSONValue]:
        payload: dict[str, JSONValue] = {
            "name": self.name,
            "status": "implemented",
            "description": self.description,
            "target_timeframes": _json_strings(self.target_timeframes),
            "schema_key": self.schema_key,
        }
        if self.key_fields:
            payload["key_fields"] = _json_strings(self.key_fields)
        if self.basis_values:
            payload["basis_values"] = _json_strings(self.basis_values)
        if self.row_order_values:
            payload["row_order_values"] = _json_strings(self.row_order_values)
        if self.extra:
            payload.update(dict(self.extra))
        return payload


@dataclass(frozen=True)
class FingerprintPlannedSectionContract:
    """One planned fingerprint roadmap section kept visible to consumers."""

    name: str
    issue: str

    def to_discovery_payload(self) -> dict[str, JSONValue]:
        return {
            "name": self.name,
            "status": "planned",
            "schema_version": None,
            "issue": self.issue,
        }


@dataclass(frozen=True)
class FingerprintRunSectionContract:
    """One run-scoped fingerprint section."""

    name: str
    status: str
    rule_id: str
    issue: str

    def to_discovery_payload(self) -> dict[str, JSONValue]:
        return {
            "name": self.name,
            "status": self.status,
            "rule_id": self.rule_id,
            "issue": self.issue,
        }


@dataclass(frozen=True)
class FingerprintReportSurfaceContract:
    """One report/bounded/CLI surface derived from fingerprint findings."""

    key: str
    summary_schema_key: str
    report_metadata_key: str
    bounded_payload_key: str
    cli_summary_section: str
    cli_summary_heading: str
    intentional_absence_reason: str = ""

    def to_discovery_payload(self) -> dict[str, JSONValue]:
        payload: dict[str, JSONValue] = {
            "key": self.key,
            "summary_schema_key": self.summary_schema_key,
            "report_metadata_key": self.report_metadata_key,
            "bounded_payload_key": self.bounded_payload_key,
            "cli_summary_section": self.cli_summary_section,
            "cli_summary_state": (
                "intentionally_absent"
                if self.intentional_absence_reason
                else "present"
            ),
        }
        if self.cli_summary_heading:
            payload["cli_summary_heading"] = self.cli_summary_heading
        if self.intentional_absence_reason:
            payload["intentional_absence_reason"] = (
                self.intentional_absence_reason
            )
        return payload


FINGERPRINT_SCHEMA_CONTRACTS = (
    FingerprintSchemaContract(
        "series_fingerprint",
        TIME_SERIES_FINGERPRINT_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        metadata_key=TIME_SERIES_FINGERPRINT_METADATA_KEY,
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_coverage_summary",
        TIME_SERIES_FINGERPRINT_COVERAGE_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        metadata_key=TIME_SERIES_FINGERPRINT_COVERAGE_METADATA_KEY,
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_topology_summary",
        TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        metadata_key=TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_METADATA_KEY,
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_topology_attention",
        TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        metadata_key=TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_METADATA_KEY,
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_topology_inspection",
        TIMESTAMP_TOPOLOGY_INSPECTION_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path="temporal_topology.inspection_context",
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_distribution_summary",
        TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        metadata_key=TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_METADATA_KEY,
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_distribution_attention",
        TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        metadata_key=TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_METADATA_KEY,
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_calendar_regimes",
        TIME_SERIES_FINGERPRINT_CALENDAR_REGIMES_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path="time_series_fingerprint.calendar_regimes",
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_conditional_distributions",
        TIME_SERIES_FINGERPRINT_CONDITIONAL_DISTRIBUTIONS_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path="time_series_fingerprint.conditional_distributions",
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_regime_summary",
        TIME_SERIES_FINGERPRINT_REGIME_SUMMARY_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        metadata_key=TIME_SERIES_FINGERPRINT_REGIME_SUMMARY_METADATA_KEY,
        bounded_payload_key=FINGERPRINT_REGIME_BOUNDED_PAYLOAD_KEY,
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_dynamics",
        TIME_SERIES_FINGERPRINT_DYNAMICS_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path="time_series_fingerprint.microstructure_dynamics",
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_dependence",
        TIME_SERIES_FINGERPRINT_DEPENDENCE_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path="time_series_fingerprint.dependence",
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_stationarity_diagnostics",
        TIME_SERIES_FINGERPRINT_STATIONARITY_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path="time_series_fingerprint.stationarity_diagnostics",
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_decomposition",
        TIME_SERIES_FINGERPRINT_DECOMPOSITION_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path="time_series_fingerprint.decomposition",
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_decomposition_training_projection",
        TIME_SERIES_FINGERPRINT_DECOMPOSITION_TRAINING_PROJECTION_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path=(
            "time_series_fingerprint.decomposition.training_projection"
        ),
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_classical_baselines",
        CLASSICAL_BASELINE_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path="time_series_fingerprint.classical_baselines",
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_classical_baseline_training_projection",
        CLASSICAL_BASELINE_TRAINING_PROJECTION_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path=(
            "time_series_fingerprint.classical_baselines.training_projection"
        ),
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_classical_baseline_summary",
        CLASSICAL_BASELINE_SUMMARY_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        metadata_key=CLASSICAL_BASELINE_SUMMARY_METADATA_KEY,
        bounded_payload_key=CLASSICAL_BASELINE_BOUNDED_PAYLOAD_KEY,
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_classical_model_input",
        CLASSICAL_MODEL_INPUT_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path="time_series_fingerprint.classical_model_input",
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_classical_model_fold",
        CLASSICAL_MODEL_FOLD_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path="time_series_fingerprint.classical_model_input.fold_policy",
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_classical_model_fit_result",
        CLASSICAL_MODEL_FIT_RESULT_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_classical_model_evaluation_result",
        CLASSICAL_MODEL_EVALUATION_RESULT_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_classical_model_training_projection",
        CLASSICAL_MODEL_TRAINING_PROJECTION_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path=(
            "time_series_fingerprint.classical_model_input.training_projection"
        ),
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_classical_model_input_summary",
        CLASSICAL_MODEL_INPUT_SUMMARY_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        metadata_key=CLASSICAL_MODEL_INPUT_SUMMARY_METADATA_KEY,
        bounded_payload_key=CLASSICAL_MODEL_INPUT_BOUNDED_PAYLOAD_KEY,
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_exponential_smoothing",
        EXPONENTIAL_SMOOTHING_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path="time_series_fingerprint.exponential_smoothing",
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_exponential_smoothing_configuration",
        EXPONENTIAL_SMOOTHING_CONFIGURATION_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path="time_series_fingerprint.exponential_smoothing.configuration",
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_exponential_smoothing_fit",
        EXPONENTIAL_SMOOTHING_FIT_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path="time_series_fingerprint.exponential_smoothing.fit_summary",
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_exponential_smoothing_forecast",
        EXPONENTIAL_SMOOTHING_FORECAST_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_exponential_smoothing_evaluation",
        EXPONENTIAL_SMOOTHING_EVALUATION_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path="time_series_fingerprint.exponential_smoothing.evaluation",
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_exponential_smoothing_training_projection",
        EXPONENTIAL_SMOOTHING_TRAINING_PROJECTION_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path=(
            "time_series_fingerprint.exponential_smoothing.training_projection"
        ),
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_exponential_smoothing_summary",
        EXPONENTIAL_SMOOTHING_SUMMARY_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        metadata_key=EXPONENTIAL_SMOOTHING_SUMMARY_METADATA_KEY,
        bounded_payload_key=EXPONENTIAL_SMOOTHING_BOUNDED_PAYLOAD_KEY,
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_autoregressive",
        AUTOREGRESSIVE_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path="time_series_fingerprint.autoregressive",
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_autoregressive_configuration",
        AUTOREGRESSIVE_CONFIGURATION_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path="time_series_fingerprint.autoregressive.configuration",
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_autoregressive_fit",
        AUTOREGRESSIVE_FIT_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path="time_series_fingerprint.autoregressive.fit_summary",
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_autoregressive_forecast",
        AUTOREGRESSIVE_FORECAST_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_autoregressive_evaluation",
        AUTOREGRESSIVE_EVALUATION_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path="time_series_fingerprint.autoregressive.evaluation",
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_autoregressive_training_projection",
        AUTOREGRESSIVE_TRAINING_PROJECTION_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path=(
            "time_series_fingerprint.autoregressive.training_projection"
        ),
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_autoregressive_summary",
        AUTOREGRESSIVE_SUMMARY_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        metadata_key=AUTOREGRESSIVE_SUMMARY_METADATA_KEY,
        bounded_payload_key=AUTOREGRESSIVE_BOUNDED_PAYLOAD_KEY,
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_seasonal_exogenous",
        SEASONAL_EXOGENOUS_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path="time_series_fingerprint.seasonal_exogenous",
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_seasonal_exogenous_configuration",
        SEASONAL_EXOGENOUS_CONFIGURATION_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path="time_series_fingerprint.seasonal_exogenous.configuration",
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_seasonal_exogenous_regressors",
        SEASONAL_EXOGENOUS_REGRESSOR_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path="time_series_fingerprint.seasonal_exogenous.regressors",
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_seasonal_exogenous_fit",
        SEASONAL_EXOGENOUS_FIT_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path="time_series_fingerprint.seasonal_exogenous.fit_summary",
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_seasonal_exogenous_forecast",
        SEASONAL_EXOGENOUS_FORECAST_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_seasonal_exogenous_evaluation",
        SEASONAL_EXOGENOUS_EVALUATION_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path="time_series_fingerprint.seasonal_exogenous.evaluation",
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_seasonal_exogenous_training_projection",
        SEASONAL_EXOGENOUS_TRAINING_PROJECTION_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path=(
            "time_series_fingerprint.seasonal_exogenous.training_projection"
        ),
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_seasonal_exogenous_summary",
        SEASONAL_EXOGENOUS_SUMMARY_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        metadata_key=SEASONAL_EXOGENOUS_SUMMARY_METADATA_KEY,
        bounded_payload_key=SEASONAL_EXOGENOUS_BOUNDED_PAYLOAD_KEY,
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_state_space",
        STATE_SPACE_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path="time_series_fingerprint.state_space",
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_state_space_configuration",
        STATE_SPACE_CONFIGURATION_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path="time_series_fingerprint.state_space.configuration",
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_state_space_fit",
        STATE_SPACE_FIT_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path="time_series_fingerprint.state_space.fit_summary",
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_kalman_state_result",
        STATE_SPACE_STATE_RESULT_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_state_space_forecast",
        STATE_SPACE_FORECAST_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_state_space_evaluation",
        STATE_SPACE_EVALUATION_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path="time_series_fingerprint.state_space.evaluation",
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_state_space_training_projection",
        STATE_SPACE_TRAINING_PROJECTION_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path="time_series_fingerprint.state_space.training_projection",
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_state_space_summary",
        STATE_SPACE_SUMMARY_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        metadata_key=STATE_SPACE_SUMMARY_METADATA_KEY,
        bounded_payload_key=STATE_SPACE_BOUNDED_PAYLOAD_KEY,
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_audit",
        TIME_SERIES_FINGERPRINT_AUDIT_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path="time_series_fingerprint.fingerprint_audit",
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_cache_source_parity",
        TIME_SERIES_FINGERPRINT_PARITY_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path="time_series_fingerprint.cache_source_parity",
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_cache_source_parity_summary",
        TIME_SERIES_FINGERPRINT_PARITY_SUMMARY_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        metadata_key=TIME_SERIES_FINGERPRINT_PARITY_SUMMARY_METADATA_KEY,
        bounded_payload_key=FINGERPRINT_PARITY_BOUNDED_PAYLOAD_KEY,
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_synthetic_constraints",
        SYNTHETIC_CONSTRAINTS_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        payload_path="time_series_fingerprint.synthetic_constraints",
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_synthetic_constraint_summary",
        SYNTHETIC_CONSTRAINT_SUMMARY_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        metadata_key=SYNTHETIC_CONSTRAINT_SUMMARY_METADATA_KEY,
        bounded_payload_key=FINGERPRINT_SYNTHETIC_CONSTRAINT_BOUNDED_PAYLOAD_KEY,
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_synthetic_validation",
        SYNTHETIC_VALIDATION_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_readiness_summary",
        TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        metadata_key=TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_METADATA_KEY,
        bounded_payload_key=FINGERPRINT_READINESS_BOUNDED_PAYLOAD_KEY,
        status="implemented",
    ),
    FingerprintSchemaContract(
        "fingerprint_readiness_risk",
        TIME_SERIES_FINGERPRINT_READINESS_RISK_SCHEMA_VERSION,
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        metadata_key=TIME_SERIES_FINGERPRINT_READINESS_RISK_METADATA_KEY,
        bounded_payload_key=FINGERPRINT_READINESS_RISK_BOUNDED_PAYLOAD_KEY,
        status="implemented",
    ),
    FingerprintSchemaContract(
        "cross_series_fingerprint",
        CROSS_SERIES_FINGERPRINT_SCHEMA_VERSION,
        rule_id=CROSS_SERIES_FINGERPRINT_RULE_ID,
        metadata_key=CROSS_SERIES_FINGERPRINT_METADATA_KEY,
        bounded_payload_key=FINGERPRINT_CROSS_SERIES_BOUNDED_PAYLOAD_KEY,
        status="implemented",
    ),
)

FINGERPRINT_REPORT_SURFACE_CONTRACTS = (
    FingerprintReportSurfaceContract(
        "coverage_summary",
        "fingerprint_coverage_summary",
        TIME_SERIES_FINGERPRINT_COVERAGE_METADATA_KEY,
        FINGERPRINT_COVERAGE_BOUNDED_PAYLOAD_KEY,
        "coverage",
        "Fingerprint coverage",
    ),
    FingerprintReportSurfaceContract(
        "topology_summary",
        "fingerprint_topology_summary",
        TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_METADATA_KEY,
        FINGERPRINT_TOPOLOGY_BOUNDED_PAYLOAD_KEY,
        "topology_summary",
        "Fingerprint topology",
    ),
    FingerprintReportSurfaceContract(
        "topology_attention",
        "fingerprint_topology_attention",
        TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_METADATA_KEY,
        FINGERPRINT_TOPOLOGY_ATTENTION_BOUNDED_PAYLOAD_KEY,
        "topology_attention",
        "Fingerprint topology attention",
    ),
    FingerprintReportSurfaceContract(
        "distribution_summary",
        "fingerprint_distribution_summary",
        TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_METADATA_KEY,
        FINGERPRINT_DISTRIBUTION_BOUNDED_PAYLOAD_KEY,
        "distribution_summary",
        "Fingerprint distributions",
    ),
    FingerprintReportSurfaceContract(
        "distribution_attention",
        "fingerprint_distribution_attention",
        TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_METADATA_KEY,
        FINGERPRINT_DISTRIBUTION_ATTENTION_BOUNDED_PAYLOAD_KEY,
        "distribution_attention",
        "Fingerprint distribution attention",
    ),
    FingerprintReportSurfaceContract(
        "regime_summary",
        "fingerprint_regime_summary",
        TIME_SERIES_FINGERPRINT_REGIME_SUMMARY_METADATA_KEY,
        FINGERPRINT_REGIME_BOUNDED_PAYLOAD_KEY,
        "regime_summary",
        "Fingerprint regimes",
    ),
    FingerprintReportSurfaceContract(
        "cache_source_parity",
        "fingerprint_cache_source_parity_summary",
        TIME_SERIES_FINGERPRINT_PARITY_SUMMARY_METADATA_KEY,
        FINGERPRINT_PARITY_BOUNDED_PAYLOAD_KEY,
        "cache_source_parity",
        "Fingerprint cache/source parity",
    ),
    FingerprintReportSurfaceContract(
        "synthetic_constraints",
        "fingerprint_synthetic_constraint_summary",
        SYNTHETIC_CONSTRAINT_SUMMARY_METADATA_KEY,
        FINGERPRINT_SYNTHETIC_CONSTRAINT_BOUNDED_PAYLOAD_KEY,
        "synthetic_constraints",
        "Synthetic fingerprint constraints",
    ),
    FingerprintReportSurfaceContract(
        "classical_baselines",
        "fingerprint_classical_baseline_summary",
        CLASSICAL_BASELINE_SUMMARY_METADATA_KEY,
        CLASSICAL_BASELINE_BOUNDED_PAYLOAD_KEY,
        "classical_baselines",
        "Classical fingerprint baselines",
    ),
    FingerprintReportSurfaceContract(
        "classical_model_input",
        "fingerprint_classical_model_input_summary",
        CLASSICAL_MODEL_INPUT_SUMMARY_METADATA_KEY,
        CLASSICAL_MODEL_INPUT_BOUNDED_PAYLOAD_KEY,
        "classical_model_input",
        "Classical model input contracts",
    ),
    FingerprintReportSurfaceContract(
        "exponential_smoothing",
        "fingerprint_exponential_smoothing_summary",
        EXPONENTIAL_SMOOTHING_SUMMARY_METADATA_KEY,
        EXPONENTIAL_SMOOTHING_BOUNDED_PAYLOAD_KEY,
        "exponential_smoothing",
        "Exponential-smoothing models",
    ),
    FingerprintReportSurfaceContract(
        "autoregressive",
        "fingerprint_autoregressive_summary",
        AUTOREGRESSIVE_SUMMARY_METADATA_KEY,
        AUTOREGRESSIVE_BOUNDED_PAYLOAD_KEY,
        "autoregressive",
        "Autoregressive models",
    ),
    FingerprintReportSurfaceContract(
        "seasonal_exogenous",
        "fingerprint_seasonal_exogenous_summary",
        SEASONAL_EXOGENOUS_SUMMARY_METADATA_KEY,
        SEASONAL_EXOGENOUS_BOUNDED_PAYLOAD_KEY,
        "seasonal_exogenous",
        "Seasonal and exogenous models",
    ),
    FingerprintReportSurfaceContract(
        "state_space",
        "fingerprint_state_space_summary",
        STATE_SPACE_SUMMARY_METADATA_KEY,
        STATE_SPACE_BOUNDED_PAYLOAD_KEY,
        "state_space",
        "State-space and Kalman models",
    ),
    FingerprintReportSurfaceContract(
        "readiness_summary",
        "fingerprint_readiness_summary",
        TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_METADATA_KEY,
        FINGERPRINT_READINESS_BOUNDED_PAYLOAD_KEY,
        "readiness_summary",
        "Fingerprint readiness",
    ),
    FingerprintReportSurfaceContract(
        "readiness_risk",
        "fingerprint_readiness_risk",
        TIME_SERIES_FINGERPRINT_READINESS_RISK_METADATA_KEY,
        FINGERPRINT_READINESS_RISK_BOUNDED_PAYLOAD_KEY,
        "readiness_risk",
        "Fingerprint readiness risk",
    ),
    FingerprintReportSurfaceContract(
        "cross_series",
        "cross_series_fingerprint",
        CROSS_SERIES_FINGERPRINT_METADATA_KEY,
        FINGERPRINT_CROSS_SERIES_BOUNDED_PAYLOAD_KEY,
        "cross_series",
        "Cross-series fingerprint",
    ),
)

IMPLEMENTED_FINGERPRINT_TARGET_SECTION_CONTRACTS = (
    FingerprintTargetSectionContract(
        "coverage",
        "all supported targets",
        target_timeframes=(TICK,),
        schema_key="series_fingerprint",
        key_fields=(
            "row_count",
            "parsed_row_count",
            "start_timestamp_utc_ms",
            "end_timestamp_utc_ms",
        ),
    ),
    FingerprintTargetSectionContract(
        "temporal_topology",
        "timestamp continuity, ordering, gaps, duplicates, and sampling basis",
        target_timeframes=(TICK,),
        schema_key="series_fingerprint",
        basis_values=("observed_sequence",),
        extra={
            "optional_nested_schema_key": "fingerprint_topology_inspection",
            "profile_controlled_by": ["topology_inspection_sample_limit"],
        },
    ),
    FingerprintTargetSectionContract(
        "calendar_regimes",
        "session, special-window, holiday, event, hour, and weekday counts",
        target_timeframes=(TICK,),
        schema_key="fingerprint_calendar_regimes",
        basis_values=("text_scan", "direct_cache", "fresh_sibling_cache"),
    ),
    FingerprintTargetSectionContract(
        "tick_distribution",
        "bid, ask, spread, precision, zero/negative spread, and invalid-row summaries",
        target_timeframes=(TICK,),
        schema_key="series_fingerprint",
    ),
    FingerprintTargetSectionContract(
        "conditional_distributions",
        "bounded tick-spread summaries by active session and special tag",
        target_timeframes=(TICK,),
        schema_key="fingerprint_conditional_distributions",
        basis_values=("text", "cache"),
        extra={
            "metric": "tick_spread",
            "grouped_by": ["active_session", "special_tag"],
        },
    ),
    FingerprintTargetSectionContract(
        "microstructure_dynamics",
        "tick interarrival, spread changes, stale quotes, bursts, and one-sided movement",
        target_timeframes=(TICK,),
        schema_key="fingerprint_dynamics",
        basis_values=("observed_sequence",),
        row_order_values=("source_text_order", "cache_order"),
    ),
    FingerprintTargetSectionContract(
        "dependence",
        "observed-sequence lag autocorrelation for returns, ranges, spreads, and spread changes",
        target_timeframes=(TICK,),
        schema_key="fingerprint_dependence",
        basis_values=("observed_sequence",),
        row_order_values=("source_text_order", "cache_order"),
        extra={
            "acf_basis": "observed_sequence",
            "profile_controlled_by": ["lags", "rounding_digits"],
        },
    ),
    FingerprintTargetSectionContract(
        "stationarity_diagnostics",
        "advisory rolling drift, distribution shift, and transform recommendations",
        target_timeframes=(TICK,),
        schema_key="fingerprint_stationarity_diagnostics",
        basis_values=("observed_sequence",),
        row_order_values=("source_text_order", "cache_order"),
        extra={
            "profile_controlled_by": [
                "rolling_windows",
                "rounding_digits",
            ],
        },
    ),
    FingerprintTargetSectionContract(
        "decomposition",
        (
            "advisory trend, seasonality, residual, smoothing-window, and "
            "structural-break proxies"
        ),
        target_timeframes=(TICK,),
        schema_key="fingerprint_decomposition",
        basis_values=("observed_sequence",),
        row_order_values=("source_text_order", "cache_order"),
        extra={
            "profile_controlled_by": [
                "rolling_windows",
                "histogram_bins",
                "rounding_digits",
            ],
            "stationarity_basis": "stationarity_diagnostics",
            "training_projection_grain": "period",
            "training_projection_identity": [
                "series_id",
                "period",
                "row_id",
            ],
        },
    ),
    FingerprintTargetSectionContract(
        "cache_source_parity",
        "opt-in source/cache and enriched training projection comparison",
        target_timeframes=(TICK,),
        schema_key="fingerprint_cache_source_parity",
        basis_values=("text_scan", "direct_cache", "fresh_sibling_cache"),
        extra={
            "profile_controlled_by": ["cache_source_parity"],
            "default_enabled": False,
            "advisory": True,
        },
    ),
    FingerprintTargetSectionContract(
        "classical_baselines",
        "opt-in deterministic advisory baselines over enriched tick rows",
        target_timeframes=(TICK,),
        schema_key="fingerprint_classical_baselines",
        basis_values=("observed_sequence_walk_forward",),
        row_order_values=("series_id_period_row_id",),
        extra={
            "issue": "#332",
            "profile_controlled_by": ["classical_baselines"],
            "default_enabled": False,
            "advisory": True,
            "base_grain": "ascii/T",
            "metric": "mid",
            "timestamp_required": False,
            "model_families": [
                "naive_random_walk",
                "rolling_mean",
                "rolling_median",
                "session_seasonal_naive",
            ],
        },
    ),
    FingerprintTargetSectionContract(
        "classical_model_input",
        "opt-in regularized input and evaluation contract over enriched tick rows",
        target_timeframes=(TICK,),
        schema_key="fingerprint_classical_model_input",
        basis_values=("regular_grid_from_enriched_ascii_ticks",),
        row_order_values=("series_id_period_row_id",),
        extra={
            "issue": "#421",
            "profile_controlled_by": ["classical_model_input"],
            "default_enabled": False,
            "advisory": True,
            "base_grain": "ascii/T",
            "derived_grain": "regular_grid",
            "timestamp_required_as_identity": False,
            "augmented_column_prefixes": [
                "cm_input_",
                "cm_fold_",
                "cm_evaluation_",
            ],
            "model_fitting_in_scope": False,
            "aggregations": ["first", "last", "mean", "median"],
            "transforms": list(MODEL_TRANSFORM_CODES),
            "fold_kinds": ["expanding", "rolling"],
            "fit_statuses": list(MODEL_FIT_STATUSES),
            "failure_reason_codes": list(MODEL_FAILURE_REASON_CODES),
            "row_mapping_policy": (
                "availability_safe_repetition_after_bin_close"
            ),
        },
    ),
    FingerprintTargetSectionContract(
        "exponential_smoothing",
        "opt-in fitted SES, Holt, Holt-Winters, and ETS diagnostics",
        target_timeframes=(TICK,),
        schema_key="fingerprint_exponential_smoothing",
        basis_values=("regular_grid_rolling_origin",),
        row_order_values=("series_id_period_row_id",),
        extra={
            "issue": "#422",
            "profile_controlled_by": [
                "classical_model_input",
                "exponential_smoothing",
            ],
            "default_enabled": False,
            "advisory": True,
            "base_grain": "ascii/T",
            "derived_grain": "regular_grid",
            "optional_dependency_extra": "models",
            "backend": "statsmodels",
            "model_families": list(EXPONENTIAL_SMOOTHING_FAMILIES),
            "failure_reason_codes": list(EXPONENTIAL_SMOOTHING_REASON_CODES),
            "augmented_column_prefixes": ["cm_ets_"],
            "augmented_columns": [
                {
                    "name": definition.name,
                    "dtype": definition.dtype,
                    "nullable": definition.nullable,
                    "grain": definition.grain,
                    "source": definition.source,
                }
                for definition in training_feature_definitions()
                if definition.name in EXPONENTIAL_SMOOTHING_COLUMNS
            ],
            "automatic_search": False,
            "automatic_winner": False,
            "row_mapping_policy": ("first_source_row_at_or_after_availability"),
        },
    ),
    FingerprintTargetSectionContract(
        "autoregressive",
        "opt-in fitted explicit-order AR, ARMA, and ARIMA diagnostics",
        target_timeframes=(TICK,),
        schema_key="fingerprint_autoregressive",
        basis_values=("regular_grid_rolling_origin",),
        row_order_values=("series_id_period_row_id",),
        extra={
            "issue": "#423",
            "profile_controlled_by": [
                "classical_model_input",
                "autoregressive",
            ],
            "default_enabled": False,
            "advisory": True,
            "base_grain": "ascii/T",
            "derived_grain": "regular_grid",
            "optional_dependency_extra": "models",
            "backend": "statsmodels",
            "model_families": list(AUTOREGRESSIVE_FAMILIES),
            "fit_statuses": list(AUTOREGRESSIVE_FIT_STATUS_CODES),
            "failure_reason_codes": list(AUTOREGRESSIVE_REASON_CODES),
            "augmented_column_prefixes": [
                "cm_ar_",
                "cm_arma_",
                "cm_arima_",
            ],
            "augmented_columns": [
                {
                    "name": definition.name,
                    "dtype": definition.dtype,
                    "nullable": definition.nullable,
                    "grain": definition.grain,
                    "source": definition.source,
                }
                for definition in training_feature_definitions()
                if definition.name in AUTOREGRESSIVE_COLUMNS
            ],
            "explicit_order_configuration": True,
            "automatic_order_selection": False,
            "automatic_winner": False,
            "row_mapping_policy": ("first_source_row_at_or_after_availability"),
        },
    ),
    FingerprintTargetSectionContract(
        "seasonal_exogenous",
        "opt-in fitted explicit-order SARIMA, ARIMAX, and SARIMAX diagnostics",
        target_timeframes=(TICK,),
        schema_key="fingerprint_seasonal_exogenous",
        basis_values=("regular_grid_rolling_origin",),
        row_order_values=("series_id_period_row_id",),
        extra={
            "issue": "#424",
            "profile_controlled_by": [
                "classical_model_input",
                "seasonal_exogenous",
            ],
            "default_enabled": False,
            "advisory": True,
            "base_grain": "ascii/T",
            "derived_grain": "regular_grid",
            "optional_dependency_extra": "models",
            "backend": "statsmodels",
            "model_families": list(SEASONAL_EXOGENOUS_FAMILIES),
            "fit_statuses": list(SEASONAL_EXOGENOUS_FIT_STATUS_CODES),
            "failure_reason_codes": list(SEASONAL_EXOGENOUS_REASON_CODES),
            "augmented_column_prefixes": [
                "cm_sarima_",
                "cm_arimax_",
                "cm_sarimax_",
            ],
            "augmented_columns": [
                {
                    "name": definition.name,
                    "dtype": definition.dtype,
                    "nullable": definition.nullable,
                    "grain": definition.grain,
                    "source": definition.source,
                }
                for definition in training_feature_definitions()
                if definition.name in SEASONAL_EXOGENOUS_COLUMNS
            ],
            "explicit_order_configuration": True,
            "deterministic_regressor_contract": True,
            "future_regressor_policy": "calendar_known_in_advance_only",
            "automatic_order_selection": False,
            "automatic_winner": False,
            "row_mapping_policy": ("first_source_row_at_or_after_availability"),
        },
    ),
    FingerprintTargetSectionContract(
        "state_space",
        "opt-in structural state-space and leakage-safe Kalman diagnostics",
        target_timeframes=(TICK,),
        schema_key="fingerprint_state_space",
        basis_values=("regular_grid_rolling_origin",),
        row_order_values=("series_id_period_row_id",),
        extra={
            "issue": "#425",
            "profile_controlled_by": [
                "classical_model_input",
                "state_space",
            ],
            "default_enabled": False,
            "advisory": True,
            "base_grain": "ascii/T",
            "derived_grain": "regular_grid",
            "optional_dependency_extra": "models",
            "backend": "statsmodels",
            "model_families": list(STATE_SPACE_FAMILIES),
            "fit_statuses": list(STATE_SPACE_FIT_STATUS_CODES),
            "failure_reason_codes": list(STATE_SPACE_REASON_CODES),
            "augmented_column_prefixes": [
                "cm_state_space_",
                "cm_kalman_",
            ],
            "augmented_columns": [
                {
                    "name": definition.name,
                    "dtype": definition.dtype,
                    "nullable": definition.nullable,
                    "grain": definition.grain,
                    "source": definition.source,
                }
                for definition in training_feature_definitions()
                if definition.name in (*STATE_SPACE_COLUMNS, *KALMAN_COLUMNS)
            ],
            "explicit_component_configuration": True,
            "filtered_state_policy": "forecast_origin_information_only",
            "smoothed_state_policy": "retrospective_diagnostic_only",
            "missing_observation_policy": "prediction_only_transition",
            "automatic_component_selection": False,
            "automatic_winner": False,
            "row_mapping_policy": "first_source_row_at_or_after_availability",
        },
    ),
    FingerprintTargetSectionContract(
        "synthetic_constraints",
        "generator-facing defects, stylized facts, artifacts, and validation contract",
        target_timeframes=(TICK,),
        schema_key="fingerprint_synthetic_constraints",
        basis_values=("enriched_training_frame", "fingerprint_fallback"),
        extra={
            "issue": "#333",
            "advisory": True,
            "base_grain": "ascii/T",
            "generation_in_scope": False,
            "non_tick_input_constraints_supported": False,
        },
    ),
    FingerprintTargetSectionContract(
        "fingerprint_audit",
        "machine-readable expected/emitted/skipped section accounting and readiness",
        target_timeframes=(TICK,),
        schema_key="fingerprint_audit",
    ),
)

PLANNED_FINGERPRINT_TARGET_SECTION_CONTRACTS: tuple[
    FingerprintPlannedSectionContract, ...
] = ()
IMPLEMENTED_FINGERPRINT_RUN_SECTION_CONTRACTS = (
    FingerprintRunSectionContract(
        "cross_series_fingerprint",
        "implemented",
        CROSS_SERIES_FINGERPRINT_RULE_ID,
        "#331",
    ),
)
PLANNED_FINGERPRINT_RUN_SECTION_CONTRACTS: tuple[
    FingerprintRunSectionContract, ...
] = ()

FINGERPRINT_SECTION_LIMIT_DEFAULTS = {
    "topology_summary_target_limit": DEFAULT_FINGERPRINT_TOPOLOGY_SUMMARY_LIMIT,
    "topology_attention_target_limit": DEFAULT_FINGERPRINT_TOPOLOGY_ATTENTION_LIMIT,
    "topology_inspection_sample_limit": (
        DEFAULT_FINGERPRINT_TOPOLOGY_INSPECTION_SAMPLE_LIMIT
    ),
    "distribution_summary_target_limit": DEFAULT_FINGERPRINT_DISTRIBUTION_SUMMARY_LIMIT,
    "distribution_attention_target_limit": (
        DEFAULT_FINGERPRINT_DISTRIBUTION_ATTENTION_LIMIT
    ),
    "regime_summary_target_limit": DEFAULT_FINGERPRINT_REGIME_SUMMARY_LIMIT,
    "regime_summary_count_limit": DEFAULT_FINGERPRINT_REGIME_COUNT_LIMIT,
    "readiness_summary_target_limit": DEFAULT_FINGERPRINT_READINESS_SUMMARY_LIMIT,
    "readiness_summary_lag_limit": DEFAULT_FINGERPRINT_READINESS_LAG_LIMIT,
    "readiness_risk_target_limit": (
        DEFAULT_FINGERPRINT_READINESS_RISK_TARGET_LIMIT
    ),
    "readiness_risk_section_limit": (
        DEFAULT_FINGERPRINT_READINESS_RISK_SECTION_LIMIT
    ),
    "readiness_risk_reason_limit": (
        DEFAULT_FINGERPRINT_READINESS_RISK_REASON_LIMIT
    ),
    "parity_summary_target_limit": DEFAULT_FINGERPRINT_PARITY_SUMMARY_LIMIT,
    "synthetic_constraint_summary_target_limit": (
        DEFAULT_SYNTHETIC_CONSTRAINT_SUMMARY_LIMIT
    ),
    "classical_baseline_summary_target_limit": (
        DEFAULT_BASELINE_SUMMARY_TARGET_LIMIT
    ),
    "classical_model_input_summary_target_limit": (
        DEFAULT_MODEL_INPUT_SUMMARY_TARGET_LIMIT
    ),
    "exponential_smoothing_summary_target_limit": (
        DEFAULT_EXPONENTIAL_SMOOTHING_SUMMARY_TARGET_LIMIT
    ),
    "autoregressive_summary_target_limit": (
        DEFAULT_AUTOREGRESSIVE_SUMMARY_TARGET_LIMIT
    ),
    "seasonal_exogenous_summary_target_limit": (
        DEFAULT_SEASONAL_EXOGENOUS_SUMMARY_TARGET_LIMIT
    ),
    "state_space_summary_target_limit": DEFAULT_STATE_SPACE_SUMMARY_TARGET_LIMIT,
}
FINGERPRINT_DISTRIBUTION_ATTENTION_DEFAULTS = {
    "invalid_row_min_count": DEFAULT_FINGERPRINT_DISTRIBUTION_INVALID_ROW_MIN_COUNT,
    "invalid_row_min_rate": DEFAULT_FINGERPRINT_DISTRIBUTION_INVALID_ROW_MIN_RATE,
    "zero_spread_min_count": DEFAULT_FINGERPRINT_DISTRIBUTION_ZERO_SPREAD_MIN_COUNT,
    "zero_spread_min_rate": DEFAULT_FINGERPRINT_DISTRIBUTION_ZERO_SPREAD_MIN_RATE,
    "negative_spread_min_count": DEFAULT_FINGERPRINT_DISTRIBUTION_NEGATIVE_SPREAD_MIN_COUNT,
    "negative_spread_min_rate": DEFAULT_FINGERPRINT_DISTRIBUTION_NEGATIVE_SPREAD_MIN_RATE,
    "flag_truncated_distribution": DEFAULT_FINGERPRINT_DISTRIBUTION_FLAG_TRUNCATED,
    "flag_cache_float_precision": DEFAULT_FINGERPRINT_DISTRIBUTION_FLAG_CACHE_FLOAT_PRECISION,
}


def implemented_fingerprint_target_section_names() -> tuple[str, ...]:
    """Return implemented target section names in discovery order."""
    return tuple(
        section.name
        for section in IMPLEMENTED_FINGERPRINT_TARGET_SECTION_CONTRACTS
    )


def planned_fingerprint_target_section_names() -> tuple[str, ...]:
    """Return planned target section names in discovery order."""
    return tuple(
        section.name for section in PLANNED_FINGERPRINT_TARGET_SECTION_CONTRACTS
    )


def _json_strings(values: tuple[object, ...]) -> list[JSONValue]:
    return [str(value) for value in values]
